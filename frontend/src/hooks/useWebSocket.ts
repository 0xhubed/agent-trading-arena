import { useEffect, useRef, useCallback } from 'react';
import { useCompetitionStore } from '../stores/competition';
import { useLearningStore } from '../stores/learning';
import type { LearningEvent } from '../types/learning';

// Fetch initial data from REST API
async function fetchInitialData() {
  const baseUrl = '/api';

  try {
    const [leaderboardHistoryRes, decisionsRes, fundingRes, liquidationsRes, extendedLeaderboardRes] = await Promise.all([
      fetch(`${baseUrl}/history/leaderboard?limit=200`),
      fetch(`${baseUrl}/history/decisions?limit=50`),
      fetch(`${baseUrl}/funding?limit=100`),
      fetch(`${baseUrl}/liquidations?limit=50`),
      fetch(`${baseUrl}/leaderboard?extended=true`),
    ]);

    const results = {
      leaderboardHistory: leaderboardHistoryRes.ok ? await leaderboardHistoryRes.json() : [],
      decisions: decisionsRes.ok ? await decisionsRes.json() : [],
      funding: fundingRes.ok ? await fundingRes.json() : [],
      liquidations: liquidationsRes.ok ? await liquidationsRes.json() : [],
      extendedLeaderboard: extendedLeaderboardRes.ok ? await extendedLeaderboardRes.json() : [],
    };

    return results;
  } catch (error) {
    console.error('Failed to fetch initial data:', error);
    return null;
  }
}

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const initialDataFetched = useRef(false);

  const {
    setConnected,
    setStatus,
    setTick,
    setAgents,
    setLeaderboard,
    setMarket,
    handleTickEvent,
    addDecision,
    addFundingPayments,
    addLiquidations,
    setEquityHistory,
    setRecentDecisions,
  } = useCompetitionStore();

  const { addLearningEvent } = useLearningStore();

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = async () => {
      console.log('WebSocket connected');
      reconnectAttemptsRef.current = 0;
      setConnected(true);

      // Fetch historical data on first connection
      if (!initialDataFetched.current) {
        initialDataFetched.current = true;
        const data = await fetchInitialData();
        if (data) {
          // Transform leaderboard history to equity history format
          if (data.leaderboardHistory.length > 0) {
            const equityHistory = data.leaderboardHistory.map((entry: { tick: number; timestamp: string; leaderboard: unknown[] }) => ({
              tick: entry.tick,
              timestamp: entry.timestamp,
              leaderboard: entry.leaderboard,
            }));
            setEquityHistory(equityHistory);
          }

          // Transform decisions to the expected format
          if (data.decisions.length > 0) {
            const decisions = data.decisions.map((d: { agent_id: string; timestamp: string; action: string; confidence: number; reasoning: string }) => ({
              agent_id: d.agent_id,
              decision: {
                action: d.action,
                confidence: d.confidence,
                reasoning: d.reasoning,
              },
              timestamp: d.timestamp,
            }));
            setRecentDecisions(decisions);
          }

          // Load funding payments
          if (data.funding.length > 0) {
            addFundingPayments(data.funding);
          }

          // Load liquidations
          if (data.liquidations.length > 0) {
            addLiquidations(data.liquidations);
          }

          // Set extended leaderboard data
          if (data.extendedLeaderboard.length > 0) {
            setLeaderboard(data.extendedLeaderboard);
          }
        }
      }
    };

    ws.onclose = () => {
      console.log('WebSocket disconnected');
      setConnected(false);

      // Reconnect with exponential backoff (1s, 2s, 4s, 8s, ... max 30s)
      const attempt = reconnectAttemptsRef.current;
      const delay = Math.min(1000 * Math.pow(2, attempt), 30000);
      reconnectAttemptsRef.current = attempt + 1;
      console.log(`WebSocket reconnecting in ${delay}ms (attempt ${attempt + 1})`);
      reconnectTimeoutRef.current = window.setTimeout(() => {
        connect();
      }, delay);
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        handleMessage(message);
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    };
  }, [setConnected, setLeaderboard, setEquityHistory, setRecentDecisions, addFundingPayments, addLiquidations]);

  const handleMessage = useCallback(
    (message: { type: string; data: unknown }) => {
      switch (message.type) {
        case 'init': {
          const data = message.data as {
            status: 'running' | 'stopped';
            tick: number;
            leaderboard: [];
            market: Record<string, number>;
            agents: { id: string; name: string; model: string; agent_type?: string; agent_type_description?: string }[];
          };
          setStatus(data.status);
          setTick(data.tick);
          // Add default values for agent_type fields if not present
          setAgents(data.agents.map(a => ({
            ...a,
            agent_type: a.agent_type || 'LLM',
            agent_type_description: a.agent_type_description || '',
          })));
          setLeaderboard(data.leaderboard);
          setMarket(
            Object.fromEntries(
              Object.entries(data.market).map(([k, v]) => [k, { price: v, change_24h: 0 }])
            )
          );
          break;
        }

        case 'tick': {
          const data = message.data as {
            tick: number;
            timestamp: string;
            leaderboard: [];
            market: Record<string, { price: number; change_24h: number }>;
            decisions: Record<string, { action: string; reasoning: string; confidence: number }>;
          };
          handleTickEvent(data);
          break;
        }

        case 'decision': {
          const data = message.data as {
            agent_id: string;
            decision: {
              action: string;
              symbol?: string;
              size?: string;
              leverage?: number;
              confidence: number;
              reasoning: string;
            };
          };
          addDecision({
            agent_id: data.agent_id,
            decision: data.decision,
            timestamp: new Date().toISOString(),
          });
          break;
        }

        case 'competition_started': {
          // Reset store data when competition starts (clears old cached data)
          const { reset } = useCompetitionStore.getState();
          reset();
          setStatus('running');
          // Re-fetch initial data
          initialDataFetched.current = false;
          break;
        }

        case 'reset': {
          // Full reset triggered from admin panel - clear all cached state
          const { reset } = useCompetitionStore.getState();
          reset();
          // Allow re-fetching initial data on next connection
          initialDataFetched.current = false;
          break;
        }

        case 'competition_stopped': {
          setStatus('stopped');
          break;
        }

        case 'ping': {
          // Send pong
          wsRef.current?.send('pong');
          break;
        }

        case 'funding': {
          const data = message.data as {
            tick: number;
            timestamp: string;
            payments: Array<{
              agent_id: string;
              symbol: string;
              side: string;
              funding_rate: number;
              notional: number;
              amount: number;
              direction: 'paid' | 'received';
            }>;
          };
          const payments = data.payments.map((p) => ({
            ...p,
            tick: data.tick,
            timestamp: data.timestamp,
          }));
          addFundingPayments(payments);
          break;
        }

        case 'liquidation': {
          const data = message.data as {
            tick: number;
            timestamp: string;
            liquidations: Array<{
              agent_id: string;
              symbol: string;
              side: string;
              size: number;
              entry_price: number;
              liquidation_price: number;
              mark_price: number;
              margin_lost: number;
              fee: number;
              total_loss: number;
            }>;
          };
          const liquidations = data.liquidations.map((l) => ({
            ...l,
            tick: data.tick,
            timestamp: data.timestamp,
          }));
          addLiquidations(liquidations);
          break;
        }

        case 'learning_event': {
          const data = message.data as LearningEvent;
          addLearningEvent({
            ...data,
            id: data.id || `${data.agent_id}-${Date.now()}`,
            timestamp: data.timestamp || new Date().toISOString(),
          });
          break;
        }
      }
    },
    [setStatus, setTick, setAgents, setLeaderboard, setMarket, handleTickEvent, addDecision, addFundingPayments, addLiquidations, addLearningEvent]
  );

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
    }
    wsRef.current?.close();
  }, []);

  useEffect(() => {
    connect();
    return () => {
      disconnect();
    };
  }, [connect, disconnect]);

  return { connect, disconnect };
}
