import { create } from 'zustand';
import type { Agent, LeaderboardEntry, MarketData, Decision, HistoryPoint } from '../types';

interface DecisionWithAgent {
  agent_id: string;
  agent_name?: string;
  decision: Decision;
  timestamp: string;
}

export interface FundingPayment {
  agent_id: string;
  symbol: string;
  side: string;
  funding_rate: number;
  notional: number;
  amount: number;
  direction: 'paid' | 'received';
  tick: number;
  timestamp: string;
}

export interface Liquidation {
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
  tick: number;
  timestamp: string;
}

interface CompetitionState {
  // Connection state
  connected: boolean;
  status: 'not_started' | 'running' | 'stopped';

  // Current state
  tick: number;
  agents: Agent[];
  leaderboard: LeaderboardEntry[];
  market: MarketData;
  recentDecisions: DecisionWithAgent[];

  // History for charts
  equityHistory: HistoryPoint[];

  // Funding and liquidation events
  fundingPayments: FundingPayment[];
  liquidations: Liquidation[];

  // Selected agent for detail view
  selectedAgentId: string | null;

  // Thinking state - track which agents are processing
  thinkingAgents: Set<string>;
  lastTickTime: number;

  // Actions
  setConnected: (connected: boolean) => void;
  setStatus: (status: 'not_started' | 'running' | 'stopped') => void;
  setTick: (tick: number) => void;
  setAgents: (agents: Agent[]) => void;
  setLeaderboard: (leaderboard: LeaderboardEntry[]) => void;
  setMarket: (market: MarketData) => void;
  addDecision: (decision: DecisionWithAgent) => void;
  addHistoryPoint: (point: HistoryPoint) => void;
  setEquityHistory: (history: HistoryPoint[]) => void;
  setRecentDecisions: (decisions: DecisionWithAgent[]) => void;
  setSelectedAgentId: (agentId: string | null) => void;
  addFundingPayments: (payments: FundingPayment[]) => void;
  addLiquidations: (liquidations: Liquidation[]) => void;
  handleTickEvent: (data: {
    tick: number;
    timestamp: string;
    leaderboard: LeaderboardEntry[];
    market: MarketData;
    decisions: { [agentId: string]: { action: string; reasoning: string; confidence: number } };
  }) => void;
  setThinkingAgents: (agentIds: string[]) => void;
  clearThinkingAgents: () => void;
  startThinking: () => void;
  reset: () => void;
}

export const useCompetitionStore = create<CompetitionState>((set, get) => ({
  // Initial state
  connected: false,
  status: 'not_started',
  tick: 0,
  agents: [],
  leaderboard: [],
  market: {},
  recentDecisions: [],
  equityHistory: [],
  fundingPayments: [],
  liquidations: [],
  selectedAgentId: null,
  thinkingAgents: new Set<string>(),
  lastTickTime: 0,

  // Actions
  setConnected: (connected) => set({ connected }),
  setStatus: (status) => set({ status }),
  setTick: (tick) => set({ tick }),
  setAgents: (agents) => set({ agents }),
  setLeaderboard: (leaderboard) => set({ leaderboard }),
  setMarket: (market) => set({ market }),

  addDecision: (decision) =>
    set((state) => ({
      recentDecisions: [decision, ...state.recentDecisions].slice(0, 50),
    })),

  addHistoryPoint: (point) =>
    set((state) => ({
      equityHistory: [...state.equityHistory, point].slice(-200),
    })),

  setEquityHistory: (history) => set({ equityHistory: history.slice(-200) }),

  setRecentDecisions: (decisions) => set({ recentDecisions: decisions.slice(0, 50) }),

  setSelectedAgentId: (agentId) => set({ selectedAgentId: agentId }),

  addFundingPayments: (payments) =>
    set((state) => ({
      fundingPayments: [...payments, ...state.fundingPayments].slice(0, 100),
    })),

  addLiquidations: (newLiquidations) =>
    set((state) => ({
      liquidations: [...newLiquidations, ...state.liquidations].slice(0, 50),
    })),

  handleTickEvent: (data) => {
    const { agents } = get();

    // Add decisions from tick
    const newDecisions: DecisionWithAgent[] = Object.entries(data.decisions).map(
      ([agentId, dec]) => ({
        agent_id: agentId,
        agent_name: agents.find((a) => a.id === agentId)?.name,
        decision: {
          action: dec.action,
          confidence: dec.confidence,
          reasoning: dec.reasoning,
        },
        timestamp: data.timestamp,
      })
    );

    set((state) => ({
      tick: data.tick,
      leaderboard: data.leaderboard,
      market: data.market,
      recentDecisions: [...newDecisions, ...state.recentDecisions].slice(0, 50),
      equityHistory: [
        ...state.equityHistory,
        { tick: data.tick, timestamp: data.timestamp, leaderboard: data.leaderboard },
      ].slice(-200),
      // Clear thinking state when tick completes
      thinkingAgents: new Set<string>(),
      lastTickTime: Date.now(),
    }));
  },

  setThinkingAgents: (agentIds) =>
    set({ thinkingAgents: new Set(agentIds) }),

  clearThinkingAgents: () =>
    set({ thinkingAgents: new Set<string>() }),

  startThinking: () => {
    const { agents } = get();
    // Set all agents as thinking
    set({ thinkingAgents: new Set(agents.map(a => a.id)) });
  },

  reset: () =>
    set({
      connected: false,
      status: 'not_started',
      tick: 0,
      agents: [],
      leaderboard: [],
      market: {},
      recentDecisions: [],
      equityHistory: [],
      fundingPayments: [],
      liquidations: [],
      selectedAgentId: null,
      thinkingAgents: new Set<string>(),
      lastTickTime: 0,
    }),
}));
