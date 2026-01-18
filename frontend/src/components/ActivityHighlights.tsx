import { useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useCompetitionStore, Liquidation } from '../stores/competition';
import clsx from 'clsx';

interface ActivityEvent {
  id: string;
  type: 'liquidation' | 'high_confidence' | 'big_trade' | 'rank_change';
  timestamp: string;
  agent_id: string;
  agent_name?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any;
  priority: number;
}

function formatTimeAgo(timestamp: string): string {
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now.getTime() - then.getTime();
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);

  if (diffMins < 1) return 'just now';
  if (diffMins === 1) return '1m ago';
  if (diffMins < 60) return `${diffMins}m ago`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours === 1) return '1h ago';
  return `${diffHours}h ago`;
}

// Only show events from the last 30 minutes
function isRecent(timestamp: string, maxAgeMinutes = 30): boolean {
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now.getTime() - then.getTime();
  const diffMins = diffMs / 1000 / 60;
  return diffMins <= maxAgeMinutes;
}

function EventIcon({ type }: { type: ActivityEvent['type'] }) {
  switch (type) {
    case 'liquidation':
      return (
        <div className="w-8 h-8 rounded-full bg-loss/20 flex items-center justify-center">
          <svg className="w-4 h-4 text-loss" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
      );
    case 'high_confidence':
      return (
        <div className="w-8 h-8 rounded-full bg-accent/20 flex items-center justify-center">
          <svg className="w-4 h-4 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
        </div>
      );
    case 'big_trade':
      return (
        <div className="w-8 h-8 rounded-full bg-highlight/20 flex items-center justify-center">
          <svg className="w-4 h-4 text-highlight" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
          </svg>
        </div>
      );
    case 'rank_change':
      return (
        <div className="w-8 h-8 rounded-full bg-profit/20 flex items-center justify-center">
          <svg className="w-4 h-4 text-profit" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 10l7-7m0 0l7 7m-7-7v18" />
          </svg>
        </div>
      );
  }
}

function EventDescription({ event }: { event: ActivityEvent }) {
  switch (event.type) {
    case 'liquidation': {
      const data = event.data as unknown as Liquidation;
      return (
        <div>
          <span className="text-loss font-medium">{event.agent_name || event.agent_id}</span>
          <span className="text-neutral"> liquidated on </span>
          <span className="text-white font-mono-numbers">{data.symbol}</span>
          <span className="text-loss font-mono-numbers ml-1">
            -${(data.total_loss as number).toFixed(2)}
          </span>
        </div>
      );
    }
    case 'high_confidence': {
      const dec = event.data as { action: string; symbol?: string; confidence: number };
      return (
        <div>
          <span className="text-accent font-medium">{event.agent_name || event.agent_id}</span>
          <span className="text-neutral"> high conviction </span>
          <span className={clsx(
            'font-medium',
            dec.action.includes('long') ? 'text-profit' :
            dec.action.includes('short') ? 'text-loss' : 'text-white'
          )}>
            {dec.action.replace('_', ' ')}
          </span>
          {dec.symbol && <span className="text-white font-mono-numbers ml-1">{dec.symbol}</span>}
          <span className="text-accent font-mono-numbers ml-1">
            {Math.round((dec.confidence as number) * 100)}%
          </span>
        </div>
      );
    }
    case 'big_trade': {
      const trade = event.data as { action: string; symbol: string; size?: string };
      return (
        <div>
          <span className="text-highlight font-medium">{event.agent_name || event.agent_id}</span>
          <span className="text-neutral"> opened position </span>
          <span className="text-white font-mono-numbers">{trade.symbol}</span>
        </div>
      );
    }
    case 'rank_change': {
      const change = event.data as { from: number; to: number };
      return (
        <div>
          <span className="text-profit font-medium">{event.agent_name || event.agent_id}</span>
          <span className="text-neutral"> moved from </span>
          <span className="text-neutral">#{change.from}</span>
          <span className="text-neutral"> to </span>
          <span className="text-profit font-medium">#{change.to}</span>
        </div>
      );
    }
  }
}

export default function ActivityHighlights() {
  const { recentDecisions, liquidations, agents, equityHistory } = useCompetitionStore();

  const events = useMemo(() => {
    const allEvents: ActivityEvent[] = [];

    // Add liquidations (high priority) - only recent ones
    for (const liq of liquidations.slice(0, 5)) {
      if (!isRecent(liq.timestamp)) continue;
      const agent = agents.find(a => a.id === liq.agent_id);
      allEvents.push({
        id: `liq-${liq.agent_id}-${liq.timestamp}`,
        type: 'liquidation',
        timestamp: liq.timestamp,
        agent_id: liq.agent_id,
        agent_name: agent?.name,
        data: liq,
        priority: 100,
      });
    }

    // Add high confidence trades (confidence >= 0.8) - only recent ones
    for (const dec of recentDecisions.slice(0, 20)) {
      if (!isRecent(dec.timestamp)) continue;
      if (dec.decision.confidence >= 0.8 && dec.decision.action !== 'hold') {
        const agent = agents.find(a => a.id === dec.agent_id);
        allEvents.push({
          id: `conf-${dec.agent_id}-${dec.timestamp}`,
          type: 'high_confidence',
          timestamp: dec.timestamp,
          agent_id: dec.agent_id,
          agent_name: agent?.name || dec.agent_name,
          data: {
            action: dec.decision.action,
            symbol: dec.decision.symbol,
            confidence: dec.decision.confidence,
          },
          priority: 50,
        });
      }
    }

    // Add big trades (open_long or open_short) - only recent ones
    for (const dec of recentDecisions.slice(0, 15)) {
      if (!isRecent(dec.timestamp)) continue;
      if (dec.decision.action === 'open_long' || dec.decision.action === 'open_short') {
        // Skip if already added as high confidence
        if (allEvents.some(e => e.id === `conf-${dec.agent_id}-${dec.timestamp}`)) continue;

        const agent = agents.find(a => a.id === dec.agent_id);
        allEvents.push({
          id: `trade-${dec.agent_id}-${dec.timestamp}`,
          type: 'big_trade',
          timestamp: dec.timestamp,
          agent_id: dec.agent_id,
          agent_name: agent?.name || dec.agent_name,
          data: {
            action: dec.decision.action,
            symbol: dec.decision.symbol,
            size: dec.decision.size,
          },
          priority: 30,
        });
      }
    }

    // Detect rank changes from equity history - only recent ones
    if (equityHistory.length >= 2) {
      const current = equityHistory[equityHistory.length - 1];
      const previous = equityHistory[equityHistory.length - 2];

      // Only show rank changes if they're recent
      if (isRecent(current.timestamp)) {
        const currentRanks = new Map(
          current.leaderboard.map((e, i) => [e.agent_id, i + 1])
        );
        const previousRanks = new Map(
          previous.leaderboard.map((e, i) => [e.agent_id, i + 1])
        );

        for (const [agentId, currentRank] of currentRanks) {
          const previousRank = previousRanks.get(agentId);
          if (previousRank && previousRank > currentRank && currentRank <= 3) {
            const agent = agents.find(a => a.id === agentId);
            allEvents.push({
              id: `rank-${agentId}-${current.timestamp}`,
              type: 'rank_change',
              timestamp: current.timestamp,
              agent_id: agentId,
              agent_name: agent?.name,
              data: { from: previousRank, to: currentRank },
              priority: 40,
            });
          }
        }
      }
    }

    // Sort by priority and timestamp, take top 5
    return allEvents
      .sort((a, b) => {
        if (b.priority !== a.priority) return b.priority - a.priority;
        return new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
      })
      .slice(0, 5);
  }, [recentDecisions, liquidations, agents, equityHistory]);

  if (events.length === 0) {
    return null;
  }

  return (
    <div className="glass-strong rounded-xl p-4">
      <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-highlight animate-pulse"></span>
        Activity Highlights
      </h3>

      <div className="space-y-2">
        <AnimatePresence mode="popLayout">
          {events.map((event) => (
            <motion.div
              key={event.id}
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: 10 }}
              className="flex items-center gap-3 p-2 rounded-lg bg-surface/30 hover:bg-surface/50 transition-colors"
            >
              <EventIcon type={event.type} />
              <div className="flex-1 min-w-0 text-sm">
                <EventDescription event={event} />
              </div>
              <div className="text-xs text-neutral flex-shrink-0">
                {formatTimeAgo(event.timestamp)}
              </div>
            </motion.div>
          ))}
        </AnimatePresence>
      </div>
    </div>
  );
}
