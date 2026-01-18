import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import clsx from 'clsx';
import AgentAvatar from './AgentAvatar';
import { useCompetitionStore } from '../stores/competition';
import { listItem, staggerContainer } from '../utils/animations';

interface HistoricalDecision {
  id: number;
  agent_id: string;
  tick: number;
  timestamp: string;
  action: string;
  symbol?: string;
  confidence: number;
  reasoning: string;
}

function getActionStyles(action: string): { text: string; bg: string } {
  switch (action) {
    case 'open_long':
      return { text: 'text-profit', bg: 'bg-profit/10' };
    case 'open_short':
      return { text: 'text-loss', bg: 'bg-loss/10' };
    case 'close':
      return { text: 'text-highlight', bg: 'bg-highlight/10' };
    default:
      return { text: 'text-neutral', bg: 'bg-neutral/10' };
  }
}

function getActionLabel(action: string): string {
  switch (action) {
    case 'open_long':
      return 'LONG';
    case 'open_short':
      return 'SHORT';
    case 'close':
      return 'CLOSE';
    default:
      return 'HOLD';
  }
}

export default function HistoryView() {
  const { agents } = useCompetitionStore();
  const [decisions, setDecisions] = useState<HistoricalDecision[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<{
    agentId: string;
    action: string;
  }>({
    agentId: '',
    action: '',
  });

  const getAgentName = (agentId: string) => {
    return agents.find((a) => a.id === agentId)?.name || agentId;
  };

  useEffect(() => {
    async function fetchHistory() {
      setLoading(true);
      try {
        const params = new URLSearchParams();
        if (filter.agentId) params.set('agent_id', filter.agentId);
        params.set('limit', '100');

        const response = await fetch(`/api/history/decisions?${params}`);
        if (response.ok) {
          const data = await response.json();
          setDecisions(data);
        }
      } catch (error) {
        console.error('Failed to fetch history:', error);
      } finally {
        setLoading(false);
      }
    }
    fetchHistory();
  }, [filter.agentId]);

  // Filter decisions by action type on the client side
  const filteredDecisions = filter.action
    ? decisions.filter((d) => d.action === filter.action)
    : decisions;

  return (
    <div className="glass-strong rounded-xl p-4 sm:p-6">
      <h2 className="text-lg font-semibold mb-4 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
        Decision History
      </h2>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-4">
        {/* Agent filter */}
        <select
          value={filter.agentId}
          onChange={(e) => setFilter((f) => ({ ...f, agentId: e.target.value }))}
          className="bg-surface border border-white/10 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-accent"
        >
          <option value="">All Agents</option>
          {agents.map((agent) => (
            <option key={agent.id} value={agent.id}>
              {agent.name}
            </option>
          ))}
        </select>

        {/* Action filter */}
        <select
          value={filter.action}
          onChange={(e) => setFilter((f) => ({ ...f, action: e.target.value }))}
          className="bg-surface border border-white/10 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-accent"
        >
          <option value="">All Actions</option>
          <option value="open_long">Long</option>
          <option value="open_short">Short</option>
          <option value="close">Close</option>
          <option value="hold">Hold</option>
        </select>

        {/* Clear filters */}
        {(filter.agentId || filter.action) && (
          <button
            onClick={() => setFilter({ agentId: '', action: '' })}
            className="px-3 py-2 text-sm text-neutral hover:text-white transition-colors"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Results count */}
      <div className="text-sm text-neutral mb-4">
        {filteredDecisions.length} decision{filteredDecisions.length !== 1 ? 's' : ''} found
      </div>

      {/* Decision list */}
      {loading ? (
        <div className="text-center text-neutral py-8 animate-pulse-slow">
          <div className="flex items-center justify-center gap-3">
            <div className="w-2 h-2 bg-accent rounded-full animate-ping" />
            Loading history...
          </div>
        </div>
      ) : filteredDecisions.length === 0 ? (
        <div className="text-center text-neutral py-8">
          <div className="w-12 h-12 mx-auto mb-3 rounded-full bg-surface flex items-center justify-center">
            <span className="text-2xl">📜</span>
          </div>
          No decisions found
        </div>
      ) : (
        <motion.div
          className="space-y-2 max-h-96 overflow-y-auto pr-2"
          variants={staggerContainer}
          initial="initial"
          animate="animate"
        >
          <AnimatePresence>
            {filteredDecisions.map((dec) => {
              const actionStyles = getActionStyles(dec.action);

              return (
                <motion.div
                  key={dec.id}
                  variants={listItem}
                  className="flex items-start gap-3 p-3 bg-surface/50 rounded-lg border border-white/5 hover:border-white/10 transition-colors"
                >
                  <AgentAvatar agentId={dec.agent_id} size={32} />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center flex-wrap gap-2 text-sm mb-1">
                      <span className="font-medium text-white">
                        {getAgentName(dec.agent_id)}
                      </span>
                      <span className="text-neutral text-xs">
                        Tick {dec.tick}
                      </span>
                      <span
                        className={clsx(
                          'text-xs font-bold px-2 py-0.5 rounded-full',
                          actionStyles.text,
                          actionStyles.bg
                        )}
                      >
                        {getActionLabel(dec.action)}
                      </span>
                      {dec.symbol && (
                        <span className="text-xs text-neutral font-mono-numbers">
                          {dec.symbol}
                        </span>
                      )}
                      <span
                        className={clsx(
                          'text-xs font-mono-numbers px-2 py-0.5 rounded-full ml-auto',
                          dec.confidence >= 0.7
                            ? 'bg-profit/20 text-profit'
                            : dec.confidence >= 0.4
                            ? 'bg-neutral/20 text-neutral'
                            : 'bg-loss/20 text-loss'
                        )}
                      >
                        {Math.round(dec.confidence * 100)}%
                      </span>
                    </div>
                    <p className="text-sm text-neutral/80 italic truncate">
                      "{dec.reasoning || 'No reasoning provided'}"
                    </p>
                    <div className="text-xs text-neutral/60 mt-1">
                      {new Date(dec.timestamp).toLocaleString()}
                    </div>
                  </div>
                </motion.div>
              );
            })}
          </AnimatePresence>
        </motion.div>
      )}
    </div>
  );
}
