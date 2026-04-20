import { useMemo, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { useCompetitionStore } from '../stores/competition';
import clsx from 'clsx';
import AgentAvatar from './AgentAvatar';
import Sparkline from './Sparkline';
import ThinkingIndicator from './ThinkingIndicator';
import { staggerContainer, listItem } from '../utils/animations';

function formatNumber(value: number | undefined | null, decimals = 1): string {
  if (value === undefined || value === null) return '-';
  return value.toFixed(decimals);
}

function formatAgentName(name: string): string {
  // Remove common prefixes/suffixes to make names more readable
  // "The Analyst (Agentic)" -> "Analyst (Agentic)"
  // "The Disciplined Trader" -> "Disciplined Trader"
  // "GPT Momentum (Pure)" -> "GPT Momentum (Pure)"
  let formatted = name;

  // Remove "The " prefix
  if (formatted.startsWith('The ')) {
    formatted = formatted.slice(4);
  }

  return formatted;
}

function MetricBadge({
  label,
  value,
  suffix = '',
  positive = true,
  highlight = false
}: {
  label: string;
  value: string | number | undefined | null;
  suffix?: string;
  positive?: boolean;
  highlight?: boolean;
}) {
  const displayValue = value === undefined || value === null ? '-' : `${value}${suffix}`;

  return (
    <div className={clsx(
      'flex flex-col items-center px-2 py-1 rounded',
      highlight ? 'bg-accent/10' : 'bg-surface/30'
    )}>
      <span className="text-[10px] text-neutral uppercase tracking-wide">{label}</span>
      <span className={clsx(
        'text-xs font-mono-numbers font-medium',
        highlight ? 'text-accent' :
        typeof value === 'number' && !positive ? 'text-loss' :
        typeof value === 'number' && positive && value > 0 ? 'text-profit' : 'text-white'
      )}>
        {displayValue}
      </span>
    </div>
  );
}

export default function Leaderboard() {
  const { leaderboard, agents, equityHistory, status, lastTickTime } = useCompetitionStore();
  const [isThinking, setIsThinking] = useState(false);

  // Simulate thinking state based on time since last tick
  // Competition runs every 60s, agents think for ~5-30s after tick starts
  useEffect(() => {
    if (status !== 'running') {
      setIsThinking(false);
      return;
    }

    const checkThinking = () => {
      const now = Date.now();
      const timeSinceLastTick = now - lastTickTime;
      // Show thinking if we're between 0-45 seconds after last tick completed
      // (agents are processing next tick)
      const tickInterval = 60000; // 60 seconds
      const timeInCycle = timeSinceLastTick % tickInterval;

      // Show thinking during the middle of the cycle (agents processing)
      // Don't show thinking right after tick completes (results just came in)
      setIsThinking(timeInCycle > 5000 && timeInCycle < 55000);
    };

    checkThinking();
    const interval = setInterval(checkThinking, 1000);
    return () => clearInterval(interval);
  }, [status, lastTickTime]);

  // Build sparkline data for each agent from equity history
  const agentSparklines = useMemo(() => {
    const sparklines: Record<string, number[]> = {};

    // Take last 20 history points for sparklines
    const recentHistory = equityHistory.slice(-20);

    for (const point of recentHistory) {
      for (const entry of point.leaderboard) {
        if (!sparklines[entry.agent_id]) {
          sparklines[entry.agent_id] = [];
        }
        sparklines[entry.agent_id].push(entry.equity);
      }
    }

    return sparklines;
  }, [equityHistory]);

  // Merge agent info with leaderboard (memoized to avoid re-renders)
  const entries = useMemo(() =>
    leaderboard.map((entry) => ({
      ...entry,
      agent: agents.find((a) => a.id === entry.agent_id),
      sparklineData: agentSparklines[entry.agent_id] || [],
      isThinking,
    })),
    [leaderboard, agents, agentSparklines, isThinking]
  );

  return (
    <div className="glass-strong rounded-xl p-4 sm:p-6">
      <h2 className="text-lg font-semibold mb-4 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
        Leaderboard
      </h2>

      {entries.length === 0 ? (
        <div className="text-center text-neutral py-8">
          <div className="w-12 h-12 mx-auto mb-3 rounded-full bg-surface flex items-center justify-center">
            <span className="text-2xl">🏆</span>
          </div>
          No agents registered yet
        </div>
      ) : (
        <motion.div
          className="space-y-2 sm:space-y-3"
          variants={staggerContainer}
          initial="initial"
          animate="animate"
        >
          <AnimatePresence mode="popLayout">
            {entries.map((entry, index) => (
              <motion.div
                key={entry.agent_id}
                variants={listItem}
                layout
                layoutId={entry.agent_id}
              >
                <Link
                  to={`/agent/${entry.agent_id}`}
                  className={clsx(
                    'block p-3 sm:p-4 rounded-lg transition-all duration-200',
                    'hover:scale-[1.01] sm:hover:scale-[1.02]',
                    index === 0 && entry.pnl_percent > 0
                      ? 'bg-profit/5 border border-profit/20 glow-profit'
                      : index === 0 && entry.pnl_percent < 0
                      ? 'bg-loss/5 border border-loss/20 glow-loss'
                      : 'bg-surface/50 hover:bg-surface/80 border border-white/5'
                  )}
                >
                  {/* Main row */}
                  <div className="flex items-center gap-3 sm:gap-4">
                    {/* Agent Avatar */}
                    <div className="relative flex-shrink-0">
                      <AgentAvatar agentId={entry.agent_id} size={40} />
                      {/* Rank badge overlay */}
                      <div
                        className={clsx(
                          'absolute -bottom-1 -right-1 w-5 h-5 rounded-full flex items-center justify-center text-xs font-bold',
                          index === 0
                            ? 'bg-accent text-white shadow-glow-sm'
                            : index === 1
                            ? 'bg-neutral/50 text-white'
                            : index === 2
                            ? 'bg-amber-600/50 text-amber-200'
                            : 'bg-surface text-neutral border border-border'
                        )}
                      >
                        {index + 1}
                      </div>
                    </div>

                    {/* Agent info */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-0.5">
                        <span className="font-medium text-white text-sm sm:text-base" title={entry.agent?.name || entry.agent_id}>
                          {formatAgentName(entry.agent?.name || entry.agent_id)}
                        </span>
                        {entry.isThinking && (
                          <ThinkingIndicator size="sm" className="flex-shrink-0" />
                        )}
                        {entry.agent?.agent_type && (
                          <span
                            className={clsx(
                              'text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wide hidden sm:inline-block',
                              entry.agent.agent_type === 'Journal-Aware'
                                ? 'bg-indigo-500/20 text-indigo-400'
                                : entry.agent.agent_type === 'Agentic'
                                ? 'bg-accent/20 text-accent'
                                : entry.agent.agent_type === 'LLM'
                                ? 'bg-highlight/20 text-highlight'
                                : entry.agent.agent_type === 'TA'
                                ? 'bg-amber-500/20 text-amber-400'
                                : entry.agent.agent_type === 'Passive'
                                ? 'bg-neutral/20 text-neutral'
                                : 'bg-purple-500/20 text-purple-400'
                            )}
                          >
                            {entry.agent.agent_type}
                          </span>
                        )}
                        {entry.agent?.is_learning_agent && (
                          <span className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wide bg-gradient-to-r from-accent/20 to-highlight/20 text-white hidden sm:inline-flex items-center gap-0.5">
                            <span className="w-1 h-1 rounded-full bg-accent animate-pulse"></span>
                            Learning
                          </span>
                        )}
                      </div>
                      <div className="text-xs text-neutral truncate">
                        <span className="font-mono">{entry.agent?.model || 'Unknown'}</span>
                      </div>
                    </div>

                    {/* Sparkline */}
                    <div className="hidden sm:block flex-shrink-0">
                      <Sparkline
                        data={entry.sparklineData}
                        width={50}
                        height={24}
                        color="auto"
                      />
                    </div>

                    {/* Equity & P&L */}
                    <div className="text-right flex-shrink-0">
                      <div className="font-mono-numbers font-medium text-white text-sm sm:text-base">
                        ${entry.equity.toLocaleString(undefined, {
                          minimumFractionDigits: 0,
                          maximumFractionDigits: 0,
                        })}
                      </div>
                      <div
                        className={clsx(
                          'font-mono-numbers text-xs sm:text-sm font-medium',
                          entry.pnl_percent >= 0 ? 'text-profit' : 'text-loss'
                        )}
                      >
                        {entry.pnl_percent >= 0 ? '+' : ''}
                        {entry.pnl_percent.toFixed(2)}%
                      </div>
                    </div>
                  </div>

                  {/* Extended metrics row */}
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <MetricBadge
                      label="Win Rate"
                      value={entry.win_rate !== undefined ? formatNumber(entry.win_rate, 0) : undefined}
                      suffix="%"
                      highlight={entry.win_rate !== undefined && entry.win_rate >= 50}
                    />
                    <MetricBadge
                      label="Sharpe"
                      value={entry.sharpe_ratio !== undefined ? formatNumber(entry.sharpe_ratio, 2) : undefined}
                      highlight={entry.sharpe_ratio !== undefined && entry.sharpe_ratio > 1}
                    />
                    <MetricBadge
                      label="Max DD"
                      value={entry.max_drawdown !== undefined ? formatNumber(entry.max_drawdown, 1) : undefined}
                      suffix="%"
                      positive={false}
                    />
                    <MetricBadge
                      label="Trades"
                      value={entry.total_trades ?? entry.trades}
                    />
                    {entry.profit_factor !== undefined && entry.profit_factor !== null && (
                      <MetricBadge
                        label="Profit Factor"
                        value={formatNumber(entry.profit_factor, 2)}
                        highlight={entry.profit_factor > 1.5}
                      />
                    )}
                    <div className="flex items-center gap-2 ml-auto text-xs text-neutral">
                      <span className="flex items-center gap-1">
                        <span className="w-1 h-1 rounded-full bg-highlight"></span>
                        {entry.positions} pos
                      </span>
                    </div>
                  </div>
                </Link>
              </motion.div>
            ))}
          </AnimatePresence>
        </motion.div>
      )}
    </div>
  );
}
