import { useEffect, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import clsx from 'clsx';
import type { EvolutionEvent } from '../../types/evolution';

interface Props {
  runId: string | null;
}

const EVENT_COLORS: Record<string, string> = {
  crossover: 'text-blue-400 bg-blue-400/10',
  llm_crossover: 'text-blue-300 bg-blue-300/10',
  mutation: 'text-yellow-400 bg-yellow-400/10',
  llm_mutation: 'text-yellow-300 bg-yellow-300/10',
  elimination: 'text-red-400 bg-red-400/10',
  migration: 'text-purple-400 bg-purple-400/10',
  elite_selection: 'text-green-400 bg-green-400/10',
};

const EVENT_ICONS: Record<string, string> = {
  crossover: 'X',
  llm_crossover: 'AI',
  mutation: 'M',
  llm_mutation: 'AM',
  elimination: 'E',
  migration: 'T',
  elite_selection: 'S',
};

const MAX_EVENTS = 50;
const WS_PATH = '/ws';
const MAX_RECONNECT_ATTEMPTS = 5;

function formatEvent(event: EvolutionEvent): string {
  switch (event.type) {
    case 'crossover':
    case 'llm_crossover':
      return `${event.parent_ids?.[0]?.slice(0, 8)} + ${event.parent_ids?.[1]?.slice(0, 8)} → ${event.child_id?.slice(0, 8)}`;
    case 'mutation':
    case 'llm_mutation':
      return `${event.genome_id?.slice(0, 8)}: ${event.mutations?.join(', ') || 'mutated'}`;
    case 'elite_selection':
      return `Elites: ${event.elite_ids?.map((id) => id.slice(0, 8)).join(', ')}`;
    case 'migration':
      return 'Island migration occurred';
    default:
      return JSON.stringify(event);
  }
}

export default function EvolutionFeed({ runId }: Props) {
  const [events, setEvents] = useState<(EvolutionEvent & { _id: number })[]>([]);
  const [paused, setPaused] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const idRef = useRef(0);

  useEffect(() => {
    if (!runId) return;

    let ws: WebSocket | null = null;
    let reconnectTimeout: ReturnType<typeof setTimeout>;
    let reconnectAttempts = 0;

    const connect = () => {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(`${protocol}//${window.location.host}${WS_PATH}`);
      wsRef.current = ws;

      ws.onopen = () => {
        reconnectAttempts = 0;
      };

      ws.onmessage = (msg) => {
        try {
          const data = JSON.parse(msg.data);
          if (data.type !== 'evolution_event') return;
          if (data.data?.run_id !== runId) return;

          const event: EvolutionEvent & { _id: number } = {
            ...data.data,
            _id: ++idRef.current,
            timestamp: new Date().toISOString(),
          };

          setEvents((prev) => {
            const next = [...prev, event];
            return next.slice(-MAX_EVENTS);
          });
        } catch {
          // ignore parse errors
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          const delay = Math.min(1000 * 2 ** reconnectAttempts, 30000);
          reconnectAttempts++;
          reconnectTimeout = setTimeout(connect, delay);
        }
      };
    };

    connect();

    return () => {
      clearTimeout(reconnectTimeout);
      if (ws) {
        ws.close();
        wsRef.current = null;
      }
    };
  }, [runId]);

  useEffect(() => {
    if (!paused && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [events, paused]);

  if (!runId) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        Select a run to view the event feed
      </div>
    );
  }

  return (
    <div className="glass-strong rounded-xl p-4 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-white">Evolution Feed</h2>
        <div className="flex items-center gap-3">
          <span className="text-xs text-neutral">{events.length} events</span>
          <button
            onClick={() => setPaused(!paused)}
            className={clsx(
              'px-3 py-1 rounded text-xs font-medium transition-colors',
              paused
                ? 'bg-yellow-500/20 text-yellow-400'
                : 'bg-green-500/20 text-green-400'
            )}
          >
            {paused ? 'Paused' : 'Live'}
          </button>
          <button
            onClick={() => setEvents([])}
            className="px-3 py-1 rounded text-xs font-medium bg-red-500/20 text-red-400"
          >
            Clear
          </button>
        </div>
      </div>

      <div
        ref={scrollRef}
        className="h-96 overflow-y-auto space-y-1 font-mono text-xs"
      >
        <AnimatePresence initial={false}>
          {events.map((event) => (
            <motion.div
              key={event._id}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, height: 0 }}
              transition={{ duration: 0.2 }}
              className={clsx(
                'flex items-center gap-2 px-3 py-2 rounded-lg',
                EVENT_COLORS[event.type] || 'text-neutral bg-white/5'
              )}
            >
              <span className="flex-shrink-0 w-6 text-center font-bold text-[10px]">
                {EVENT_ICONS[event.type] || '?'}
              </span>
              <span className="flex-shrink-0 text-neutral text-[10px] w-12">
                {event.generation !== undefined ? `G${event.generation}` : ''}
              </span>
              <span className="flex-1 truncate">{formatEvent(event)}</span>
              <span className="flex-shrink-0 text-neutral text-[10px]">
                {event.timestamp
                  ? new Date(event.timestamp).toLocaleTimeString()
                  : ''}
              </span>
            </motion.div>
          ))}
        </AnimatePresence>

        {events.length === 0 && (
          <div className="text-center text-neutral py-16">
            Waiting for events... Start an evolution run to see real-time updates.
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-3 text-[10px]">
        {Object.entries(EVENT_COLORS).map(([type, color]) => (
          <span key={type} className={clsx('px-2 py-0.5 rounded', color)}>
            {type.replace('_', ' ')}
          </span>
        ))}
      </div>
    </div>
  );
}
