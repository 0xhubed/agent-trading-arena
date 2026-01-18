/**
 * LearningEventsFeed component - real-time learning events across all agents.
 */

import { useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import clsx from 'clsx';
import {
  useLearningStore,
  fetchLearningEvents,
} from '../../stores/learning';
import type { LearningEvent, LearningEventType } from '../../types/learning';

interface LearningEventsFeedProps {
  className?: string;
  limit?: number;
  agentFilter?: string;
}

const eventTypeConfig: Record<
  LearningEventType,
  { icon: string; color: string; bg: string; label: string }
> = {
  pattern_learned: {
    icon: '📊',
    color: 'text-accent',
    bg: 'bg-accent/20',
    label: 'Pattern Learned',
  },
  reflection: {
    icon: '💭',
    color: 'text-highlight',
    bg: 'bg-highlight/20',
    label: 'Reflection',
  },
  strategy_shift: {
    icon: '🔄',
    color: 'text-amber-400',
    bg: 'bg-amber-500/20',
    label: 'Strategy Shift',
  },
  rag_retrieval: {
    icon: '🔍',
    color: 'text-profit',
    bg: 'bg-profit/20',
    label: 'Memory Retrieved',
  },
  meta_insight: {
    icon: '🧠',
    color: 'text-purple-400',
    bg: 'bg-purple-500/20',
    label: 'Meta Insight',
  },
};

function formatTimeAgo(timestamp: string): string {
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now.getTime() - then.getTime();
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);
  const diffHours = Math.floor(diffMins / 60);

  if (diffSecs < 60) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  return then.toLocaleDateString();
}

function EventCard({ event }: { event: LearningEvent }) {
  const config = eventTypeConfig[event.event_type] || eventTypeConfig.reflection;

  return (
    <motion.div
      initial={{ opacity: 0, y: -10, scale: 0.95 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
      className={clsx(
        'p-3 rounded-lg border transition-all',
        'bg-surface/50 border-white/5 hover:bg-surface/80'
      )}
    >
      {/* Header */}
      <div className="flex items-center gap-2 mb-2">
        <span className={clsx('text-lg', config.color)}>{config.icon}</span>
        <span className={clsx('text-[10px] font-bold px-1.5 py-0.5 rounded uppercase', config.bg, config.color)}>
          {config.label}
        </span>
        <span className="text-xs text-neutral ml-auto">
          {formatTimeAgo(event.timestamp)}
        </span>
      </div>

      {/* Agent */}
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs text-neutral">Agent:</span>
        <span className="text-xs font-medium text-white">
          {event.agent_name || event.agent_id}
        </span>
      </div>

      {/* Summary */}
      <p className="text-sm text-neutral/90">{event.summary}</p>

      {/* Details (if any) */}
      {event.details && Object.keys(event.details).length > 0 && (
        <div className="mt-2 pt-2 border-t border-white/5 flex flex-wrap gap-1.5">
          {Object.entries(event.details).slice(0, 3).map(([key, value]) => (
            <span
              key={key}
              className="text-[10px] px-1.5 py-0.5 rounded bg-surface text-neutral font-mono-numbers"
            >
              {key}: {typeof value === 'object' ? JSON.stringify(value) : String(value)}
            </span>
          ))}
        </div>
      )}
    </motion.div>
  );
}

export default function LearningEventsFeed({
  className,
  limit = 10,
  agentFilter,
}: LearningEventsFeedProps) {
  const { learningEvents, setLearningEvents } = useLearningStore();

  useEffect(() => {
    async function loadEvents() {
      const events = await fetchLearningEvents(limit * 2); // Fetch extra for filtering
      setLearningEvents(events);
    }
    loadEvents();

    // Refresh every 30 seconds
    const interval = setInterval(loadEvents, 30000);
    return () => clearInterval(interval);
  }, [limit, setLearningEvents]);

  // Filter events if agentFilter is provided
  const filteredEvents = agentFilter
    ? learningEvents.filter((e) => e.agent_id === agentFilter)
    : learningEvents;

  const displayEvents = filteredEvents.slice(0, limit);

  // Count by type
  const eventCounts = displayEvents.reduce((acc, e) => {
    acc[e.event_type] = (acc[e.event_type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  return (
    <div className={clsx('glass-strong rounded-xl p-4', className)}>
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-highlight animate-pulse"></span>
          Learning Events
        </h3>
        {!agentFilter && displayEvents.length > 0 && (
          <span className="text-xs text-neutral">
            {displayEvents.length} recent
          </span>
        )}
      </div>

      {/* Event type summary */}
      {Object.keys(eventCounts).length > 0 && (
        <div className="flex flex-wrap gap-1.5 mb-3">
          {Object.entries(eventCounts).map(([type, count]) => {
            const config = eventTypeConfig[type as LearningEventType];
            if (!config) return null;
            return (
              <span
                key={type}
                className={clsx('text-[10px] px-1.5 py-0.5 rounded', config.bg, config.color)}
              >
                {config.icon} {count}
              </span>
            );
          })}
        </div>
      )}

      {/* Events list */}
      {displayEvents.length === 0 ? (
        <div className="text-center text-neutral py-6 text-sm">
          <div className="w-10 h-10 mx-auto mb-2 rounded-full bg-surface flex items-center justify-center">
            <span className="text-xl">📡</span>
          </div>
          No learning events yet
          <p className="text-xs mt-1 text-neutral/60">
            Events appear as agents learn from their experiences
          </p>
        </div>
      ) : (
        <div className="space-y-2 max-h-[400px] overflow-y-auto pr-1">
          <AnimatePresence mode="popLayout">
            {displayEvents.map((event) => (
              <EventCard key={event.id} event={event} />
            ))}
          </AnimatePresence>
        </div>
      )}
    </div>
  );
}
