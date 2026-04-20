import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { useCompetitionStore } from '../stores/competition';
import clsx from 'clsx';
import AgentAvatar from './AgentAvatar';
import { InfoTooltip, GLOSSARY } from './InfoTooltip';
import {
  LearningProgress,
  SimilarSituationsPanel,
  LearnedPatternsPanel,
  MetaLearningPanel,
  LearningEventsFeed,
} from './learning';

interface Analytics {
  total_trades: number;
  closed_trades: number;  // Only trades with realized P&L
  winning_trades: number;
  losing_trades: number;
  win_rate: number;
  total_pnl: number;
  total_fees_paid: number;
  net_funding: number;
  max_drawdown: number;
  max_drawdown_amount: number;
  current_drawdown: number;
  sharpe_ratio: number;
  profit_factor: number;
  average_win: number;
  average_loss: number;
  largest_win: number;
  largest_loss: number;
  expectancy: number;
  average_leverage_used: number;
  equity_high: number;
  equity_low: number;
}

interface Behavioral {
  action_distribution: Record<string, number>;
  confidence: {
    average: number;
    min: number;
    max: number;
    total_decisions: number;
  };
  symbol_distribution: Record<string, number>;
  long_short_ratio: number;
  long_count: number;
  short_count: number;
  long_pct: number;
  short_pct: number;
  average_leverage: number;
}

interface AgentDetailData {
  id: string;
  name: string;
  model: string;
  agent_type: string;
  agent_type_description: string;
  character: string;
  is_learning_agent?: boolean;
  portfolio: {
    equity: number;
    available_margin: number;
    used_margin: number;
    margin_utilization: number;
    positions: {
      symbol: string;
      side: string;
      size: number;
      entry_price: number;
      mark_price: number;
      liquidation_price: number;
      margin: number;
      unrealized_pnl: number;
      roe_percent: number;
      leverage: number;
      stop_loss?: number | null;
      take_profit?: number | null;
    }[];
    realized_pnl: number;
    total_pnl: number;
    pnl_percent: number;
  };
  analytics: Analytics | null;
  behavioral: Behavioral | null;
  funding: {
    paid: number;
    received: number;
    net: number;
  };
  liquidations: number;
  recent_decisions: {
    tick: number;
    timestamp: string;
    action: string;
    symbol?: string;
    confidence: number;
    reasoning: string;
  }[];
  trades: {
    id: string;
    symbol: string;
    side: string;
    size: string;
    price: string;
    realized_pnl?: string;
    timestamp: string;
  }[];
}

function StatCard({
  label,
  value,
  suffix = '',
  positive,
  highlight = false,
  small = false,
  tooltip,
}: {
  label: string;
  value: string | number | null | undefined;
  suffix?: string;
  positive?: boolean;
  highlight?: boolean;
  small?: boolean;
  tooltip?: React.ReactNode;
}) {
  const displayValue = value === null || value === undefined ? '-' : `${value}${suffix}`;

  return (
    <div className={clsx(
      'glass rounded-xl p-3 text-center',
      highlight && 'ring-1 ring-accent/30'
    )}>
      <div className="text-xs text-neutral mb-1 flex items-center justify-center">
        {label}
        {tooltip && <InfoTooltip content={tooltip} />}
      </div>
      <div className={clsx(
        'font-mono-numbers font-bold',
        small ? 'text-base' : 'text-lg',
        positive === true ? 'text-profit' :
        positive === false ? 'text-loss' :
        'text-white'
      )}>
        {displayValue}
      </div>
    </div>
  );
}

function formatCharacter(character: string): JSX.Element {
  if (!character) return <></>;

  // Split by newlines and filter empty lines
  const lines = character.split('\n').filter(line => line.trim());

  // Check if it has structured content (rules, numbered items)
  const hasStructure = lines.some(line =>
    /^(\d+\.|[-•*]|[A-Z]+:)/.test(line.trim())
  );

  if (hasStructure) {
    return (
      <div className="space-y-1.5">
        {lines.map((line, i) => {
          const trimmed = line.trim();
          // Headers (ALL CAPS followed by colon)
          if (/^[A-Z][A-Z\s]+:/.test(trimmed)) {
            return (
              <div key={i} className="text-accent font-medium text-xs uppercase tracking-wide mt-2 first:mt-0">
                {trimmed}
              </div>
            );
          }
          // Numbered rules or bullet points
          if (/^(\d+\.|[-•*])/.test(trimmed)) {
            return (
              <div key={i} className="flex gap-2 text-neutral/80">
                <span className="text-accent/60 flex-shrink-0">
                  {trimmed.match(/^(\d+\.|[-•*])/)?.[0]}
                </span>
                <span>{trimmed.replace(/^(\d+\.|[-•*])\s*/, '')}</span>
              </div>
            );
          }
          // Regular text
          return <div key={i} className="text-neutral/80">{trimmed}</div>;
        })}
      </div>
    );
  }

  // Simple character description
  return <span className="text-neutral/70 italic">"{character}"</span>;
}

function formatReasoning(reasoning: string): JSX.Element {
  if (!reasoning) {
    return <span className="text-neutral/50">No reasoning provided</span>;
  }

  // Split into sentences
  const sentences = reasoning.split(/(?<=[.!?])\s+/).filter(s => s.trim());

  if (sentences.length > 2) {
    // Show ALL sentences in the detail view (no truncation)
    return (
      <ul className="list-none space-y-1">
        {sentences.map((sentence, i) => (
          <li key={i} className="flex gap-2">
            <span className="text-accent/60 flex-shrink-0">•</span>
            <span>{sentence.trim()}</span>
          </li>
        ))}
      </ul>
    );
  }

  return <span>"{reasoning}"</span>;
}

function ProgressBar({ label, value, max, color = 'accent' }: {
  label: string;
  value: number;
  max: number;
  color?: 'accent' | 'profit' | 'loss';
}) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div className="flex items-center gap-3">
      <span className="text-xs text-neutral w-20 truncate">{label}</span>
      <div className="flex-1 h-2 bg-surface rounded-full overflow-hidden">
        <div
          className={clsx(
            'h-full rounded-full transition-all',
            color === 'profit' ? 'bg-profit' :
            color === 'loss' ? 'bg-loss' :
            'bg-accent'
          )}
          style={{ width: `${Math.min(pct, 100)}%` }}
        />
      </div>
      <span className="text-xs font-mono-numbers text-white w-8 text-right">{value}</span>
    </div>
  );
}

export default function AgentDetail() {
  const { agentId } = useParams<{ agentId: string }>();
  const { leaderboard } = useCompetitionStore();
  const [agent, setAgent] = useState<AgentDetailData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'decisions' | 'trades' | 'learning' | 'description'>('overview');

  useEffect(() => {
    const controller = new AbortController();

    async function fetchAgent() {
      if (!agentId) return;

      try {
        setLoading(true);
        const response = await fetch(`/api/agents/${agentId}/full`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error('Agent not found');
        }
        const data = await response.json();
        setAgent(data);
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return;
        setError(err instanceof Error ? err.message : 'Failed to load agent');
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    fetchAgent();
    // Refresh every 30 seconds
    const interval = setInterval(fetchAgent, 30000);
    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [agentId]);

  const rank = leaderboard.findIndex((e) => e.agent_id === agentId) + 1;

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-radial-subtle flex items-center justify-center">
        <div className="text-neutral animate-pulse-slow flex items-center gap-3">
          <div className="w-2 h-2 bg-accent rounded-full animate-ping" />
          Loading agent data...
        </div>
      </div>
    );
  }

  if (error || !agent) {
    return (
      <div className="min-h-screen bg-gradient-radial-subtle flex flex-col items-center justify-center gap-4">
        <div className="glass-strong rounded-xl p-8 text-center">
          <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-loss/10 flex items-center justify-center">
            <span className="text-3xl">!</span>
          </div>
          <div className="text-loss mb-4">{error || 'Agent not found'}</div>
          <Link
            to="/"
            className="inline-flex items-center gap-2 px-4 py-2 bg-accent hover:bg-accent/80 rounded-lg transition-all hover:scale-105"
          >
            Back to Dashboard
          </Link>
        </div>
      </div>
    );
  }

  const portfolio = agent.portfolio;
  const analytics = agent.analytics;
  const behavioral = agent.behavioral;

  return (
    <div className="min-h-screen bg-gradient-radial-subtle p-4 sm:p-6">
      <div className="max-w-5xl mx-auto">
        {/* Back link */}
        <Link
          to="/"
          className="inline-flex items-center gap-2 text-neutral hover:text-white mb-4 sm:mb-6 transition-all hover:gap-3"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
          </svg>
          Back to Dashboard
        </Link>

        {/* Header */}
        <div className={clsx(
          'glass-strong rounded-xl p-4 sm:p-6 mb-4 sm:mb-6',
          (portfolio?.pnl_percent || 0) > 0 ? 'glow-profit' :
          (portfolio?.pnl_percent || 0) < 0 ? 'glow-loss' : ''
        )}>
          <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-4">
            <div className="flex items-start gap-4">
              <AgentAvatar agentId={agent.id} size={64} className="hidden sm:block" />
              <div>
                <div className="flex items-center gap-2 mb-1">
                  <h1 className="text-2xl sm:text-3xl font-bold text-white">{agent.name}</h1>
                  {agent.agent_type && (
                    <span
                      className={clsx(
                        'text-xs font-bold px-2 py-1 rounded uppercase tracking-wide',
                        agent.agent_type === 'Journal-Aware'
                          ? 'bg-indigo-500/20 text-indigo-400'
                          : agent.agent_type === 'Agentic'
                          ? 'bg-accent/20 text-accent'
                          : agent.agent_type === 'LLM'
                          ? 'bg-highlight/20 text-highlight'
                          : agent.agent_type === 'TA'
                          ? 'bg-amber-500/20 text-amber-400'
                          : agent.agent_type === 'Passive'
                          ? 'bg-neutral/20 text-neutral'
                          : 'bg-purple-500/20 text-purple-400'
                      )}
                    >
                      {agent.agent_type}
                    </span>
                  )}
                  {agent.is_learning_agent && (
                    <span className="text-xs font-bold px-2 py-1 rounded uppercase tracking-wide bg-gradient-to-r from-accent/20 to-highlight/20 text-white flex items-center gap-1">
                      <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse"></span>
                      Learning
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-2 text-neutral text-sm sm:text-base">
                  <span className="font-mono">{agent.model}</span>
                  {agent.agent_type_description && (
                    <>
                      <span className="text-white/20">|</span>
                      <span>{agent.agent_type_description}</span>
                    </>
                  )}
                </div>
                {agent.character && (
                  <div className="text-sm mt-3 max-w-lg">
                    {formatCharacter(agent.character)}
                  </div>
                )}
              </div>
            </div>
            <div className="flex items-center gap-4">
              {rank > 0 && (
                <div
                  className={clsx(
                    'rank-badge w-12 h-12 text-xl',
                    rank === 1 ? 'rank-badge-1' :
                    rank === 2 ? 'rank-badge-2' :
                    rank === 3 ? 'rank-badge-3' :
                    'bg-surface text-neutral border border-border'
                  )}
                >
                  {rank}
                </div>
              )}
              <div className="text-right">
                <div className="font-mono-numbers text-2xl font-bold text-white">
                  ${portfolio?.equity.toLocaleString(undefined, {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 0,
                  }) || '10,000'}
                </div>
                <div
                  className={clsx(
                    'font-mono-numbers text-lg font-medium',
                    (portfolio?.pnl_percent || 0) >= 0 ? 'text-profit' : 'text-loss'
                  )}
                >
                  {(portfolio?.pnl_percent || 0) >= 0 ? '+' : ''}
                  {(portfolio?.pnl_percent || 0).toFixed(2)}%
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Tab navigation */}
        <div className="flex gap-2 mb-4 flex-wrap">
          {(['overview', 'decisions', 'trades', 'description'] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={clsx(
                'px-4 py-2 rounded-lg text-sm font-medium transition-all',
                activeTab === tab
                  ? 'bg-accent text-white'
                  : 'bg-surface/50 text-neutral hover:bg-surface hover:text-white'
              )}
            >
              {tab.charAt(0).toUpperCase() + tab.slice(1)}
            </button>
          ))}
          {/* Learning tab - only for learning agents */}
          {agent.is_learning_agent && (
            <button
              onClick={() => setActiveTab('learning')}
              className={clsx(
                'px-4 py-2 rounded-lg text-sm font-medium transition-all flex items-center gap-2',
                activeTab === 'learning'
                  ? 'bg-accent text-white'
                  : 'bg-surface/50 text-neutral hover:bg-surface hover:text-white'
              )}
            >
              <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse"></span>
              Learning
            </button>
          )}
        </div>

        {/* Overview Tab */}
        {activeTab === 'overview' && (
          <>
            {/* Performance metrics */}
            {/* Note: Win rate, profit factor, avg win/loss, expectancy require closed trades */}
            {(() => {
              const hasClosedTrades = analytics && analytics.closed_trades > 0;
              const hasSharpeData = analytics && analytics.sharpe_ratio !== 0;
              return (
                <>
                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
                    <StatCard
                      label="Win Rate"
                      value={hasClosedTrades ? (analytics.win_rate * 100).toFixed(0) : 'N/A'}
                      suffix={hasClosedTrades ? '%' : ''}
                      positive={hasClosedTrades ? (analytics.win_rate >= 0.5 || analytics.profit_factor > 1) : undefined}
                      highlight={hasClosedTrades ? analytics.win_rate >= 0.5 : false}
                      tooltip={GLOSSARY.winRate}
                    />
                    <StatCard
                      label="Sharpe Ratio"
                      value={hasSharpeData ? analytics.sharpe_ratio.toFixed(2) : 'N/A'}
                      positive={hasSharpeData ? analytics.sharpe_ratio > 0 : undefined}
                      highlight={hasSharpeData ? analytics.sharpe_ratio > 1 : false}
                      tooltip={GLOSSARY.sharpeRatio}
                    />
                    <StatCard
                      label="Max Drawdown"
                      value={analytics ? (analytics.max_drawdown * 100).toFixed(1) : undefined}
                      suffix="%"
                      positive={analytics ? (analytics.max_drawdown <= 0.05 ? true : analytics.max_drawdown > 0.2 ? false : undefined) : undefined}
                      tooltip={GLOSSARY.maxDrawdown}
                    />
                    <StatCard
                      label="Profit Factor"
                      value={hasClosedTrades
                        ? (analytics.profit_factor === null ? 'N/A' : analytics.profit_factor === Infinity ? '∞' : analytics.profit_factor.toFixed(2))
                        : 'N/A'}
                      positive={hasClosedTrades && analytics.profit_factor !== null ? analytics.profit_factor > 1 : undefined}
                      tooltip={GLOSSARY.profitFactor}
                    />
                  </div>

                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
                    <StatCard
                      label="Round-trips"
                      value={analytics ? `${analytics.closed_trades}` : undefined}
                      small
                      tooltip={GLOSSARY.totalTrades}
                    />
                    <StatCard
                      label="Avg Win"
                      value={hasClosedTrades ? `$${analytics.average_win.toFixed(2)}` : 'N/A'}
                      positive={hasClosedTrades ? true : undefined}
                      small
                      tooltip={GLOSSARY.avgWin}
                    />
                    <StatCard
                      label="Avg Loss"
                      value={hasClosedTrades ? `$${analytics.average_loss.toFixed(2)}` : 'N/A'}
                      positive={hasClosedTrades ? (analytics.average_loss < analytics.average_win ? true : undefined) : undefined}
                      small
                      tooltip={GLOSSARY.avgLoss}
                    />
                    <StatCard
                      label="Expectancy"
                      value={hasClosedTrades ? `$${analytics.expectancy.toFixed(2)}` : 'N/A'}
                      positive={hasClosedTrades ? analytics.expectancy > 0 : undefined}
                      small
                      tooltip={GLOSSARY.expectancy}
                    />
                  </div>
                </>
              );
            })()}

            {/* Behavioral stats */}
            {behavioral && behavioral.confidence && (
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
                {/* Long/Short Distribution */}
                <div className="glass-strong rounded-xl p-4">
                  <h3 className="text-sm font-semibold mb-3 text-white flex items-center">
                    Position Bias
                    <InfoTooltip content={GLOSSARY.positionBias} />
                  </h3>
                  <div className="flex items-center gap-4 mb-3">
                    <div className="flex-1">
                      <div className="flex justify-between text-xs mb-1">
                        <span className="text-profit">Long {behavioral.long_pct ?? 0}%</span>
                        <span className="text-loss">Short {behavioral.short_pct ?? 0}%</span>
                      </div>
                      <div className="h-3 bg-loss rounded-full overflow-hidden">
                        <div
                          className="h-full bg-profit rounded-l-full transition-all"
                          style={{ width: `${behavioral.long_pct ?? 0}%` }}
                        />
                      </div>
                    </div>
                  </div>
                  <div className="text-xs text-neutral flex items-center">
                    Avg Leverage: <span className="text-white font-mono-numbers ml-1">{Number(behavioral.average_leverage ?? 0).toFixed(1)}x</span>
                    <InfoTooltip content={GLOSSARY.avgLeverage} />
                  </div>
                </div>

                {/* Confidence Distribution */}
                <div className="glass-strong rounded-xl p-4">
                  <h3 className="text-sm font-semibold mb-3 text-white flex items-center">
                    Confidence Stats
                    <InfoTooltip content={GLOSSARY.confidence} />
                  </h3>
                  <div className="grid grid-cols-3 gap-2 text-center">
                    <div>
                      <div className="text-xs text-neutral">Avg</div>
                      <div className="font-mono-numbers text-white">{(Number(behavioral.confidence.average ?? 0) * 100).toFixed(0)}%</div>
                    </div>
                    <div>
                      <div className="text-xs text-neutral">Min</div>
                      <div className="font-mono-numbers text-white">{(Number(behavioral.confidence.min ?? 0) * 100).toFixed(0)}%</div>
                    </div>
                    <div>
                      <div className="text-xs text-neutral">Max</div>
                      <div className="font-mono-numbers text-white">{(Number(behavioral.confidence.max ?? 0) * 100).toFixed(0)}%</div>
                    </div>
                  </div>
                  <div className="text-xs text-neutral mt-2 text-center">
                    {behavioral.confidence.total_decisions ?? 0} total decisions
                  </div>
                </div>
              </div>
            )}

            {/* Symbol distribution */}
            {behavioral?.symbol_distribution && Object.keys(behavioral.symbol_distribution).length > 0 && (
              <div className="glass-strong rounded-xl p-4 mb-4">
                <h3 className="text-sm font-semibold mb-3 text-white">Symbol Distribution</h3>
                <div className="space-y-2">
                  {Object.entries(behavioral.symbol_distribution)
                    .sort(([, a], [, b]) => b - a)
                    .map(([symbol, count]) => (
                      <ProgressBar
                        key={symbol}
                        label={symbol}
                        value={count}
                        max={Math.max(...Object.values(behavioral.symbol_distribution))}
                      />
                    ))}
                </div>
              </div>
            )}

            {/* Margin Summary */}
            {portfolio && (
              <div className="glass-strong rounded-xl p-4 mb-4">
                <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
                  Capital Allocation
                </h3>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  <div className="text-center">
                    <div className="text-xs text-neutral mb-1">Total Equity</div>
                    <div className="font-mono-numbers font-bold text-white">
                      ${portfolio.equity.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </div>
                  </div>
                  <div className="text-center">
                    <div className="text-xs text-neutral mb-1">In Positions</div>
                    <div className="font-mono-numbers font-bold text-highlight">
                      ${(portfolio.used_margin || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </div>
                  </div>
                  <div className="text-center">
                    <div className="text-xs text-neutral mb-1">Available</div>
                    <div className="font-mono-numbers font-bold text-profit">
                      ${portfolio.available_margin.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </div>
                  </div>
                  <div className="text-center">
                    <div className="text-xs text-neutral mb-1">Margin Used</div>
                    <div className={clsx(
                      'font-mono-numbers font-bold',
                      (portfolio.margin_utilization || 0) > 80 ? 'text-loss' :
                      (portfolio.margin_utilization || 0) > 50 ? 'text-amber-400' :
                      'text-profit'
                    )}>
                      {(portfolio.margin_utilization || 0).toFixed(1)}%
                    </div>
                  </div>
                </div>
                {/* Margin bar */}
                <div className="mt-3">
                  <div className="h-2 bg-surface rounded-full overflow-hidden">
                    <div
                      className={clsx(
                        'h-full rounded-full transition-all',
                        (portfolio.margin_utilization || 0) > 80 ? 'bg-loss' :
                        (portfolio.margin_utilization || 0) > 50 ? 'bg-amber-500' :
                        'bg-profit'
                      )}
                      style={{ width: `${Math.min(portfolio.margin_utilization || 0, 100)}%` }}
                    />
                  </div>
                </div>
              </div>
            )}

            {/* Current positions */}
            {portfolio?.positions && portfolio.positions.length > 0 && (
              <div className="glass-strong rounded-xl p-4 sm:p-6 mb-4">
                <h2 className="text-lg font-semibold mb-4 text-white flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
                  Current Positions
                </h2>
                <div className="space-y-3">
                  {portfolio.positions.map((pos) => {
                    // Calculate distance to liquidation
                    const distToLiq = pos.side === 'long'
                      ? ((pos.mark_price - pos.liquidation_price) / pos.mark_price) * 100
                      : ((pos.liquidation_price - pos.mark_price) / pos.mark_price) * 100;
                    const isLiquidationClose = distToLiq < 10;

                    return (
                      <div
                        key={pos.symbol}
                        className={clsx(
                          'p-3 sm:p-4 rounded-lg border transition-all',
                          pos.side === 'long'
                            ? 'bg-profit/5 border-profit/20'
                            : 'bg-loss/5 border-loss/20'
                        )}
                      >
                        <div className="flex items-center justify-between mb-2">
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-white">{pos.symbol}</span>
                            <span
                              className={clsx(
                                'text-xs font-bold px-2 py-0.5 rounded-full',
                                pos.side === 'long'
                                  ? 'bg-profit/20 text-profit'
                                  : 'bg-loss/20 text-loss'
                              )}
                            >
                              {pos.side.toUpperCase()} {pos.leverage}x
                            </span>
                          </div>
                          <div
                            className={clsx(
                              'font-mono-numbers font-medium',
                              pos.unrealized_pnl >= 0 ? 'text-profit' : 'text-loss'
                            )}
                          >
                            {pos.unrealized_pnl >= 0 ? '+' : ''}$
                            {pos.unrealized_pnl.toFixed(2)} ({pos.roe_percent.toFixed(2)}%)
                          </div>
                        </div>
                        {/* Position details grid */}
                        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs mb-2">
                          <div>
                            <span className="text-neutral">Margin:</span>
                            <span className="text-white font-mono-numbers ml-1">${pos.margin.toFixed(2)}</span>
                          </div>
                          <div>
                            <span className="text-neutral">Entry:</span>
                            <span className="text-white font-mono-numbers ml-1">${pos.entry_price.toLocaleString()}</span>
                          </div>
                          <div>
                            <span className="text-neutral">Mark:</span>
                            <span className="text-white font-mono-numbers ml-1">${pos.mark_price.toLocaleString()}</span>
                          </div>
                          <div>
                            <span className="text-neutral">Size:</span>
                            <span className="text-white font-mono-numbers ml-1">{pos.size.toFixed(6)}</span>
                          </div>
                        </div>
                        {/* Liquidation info */}
                        <div className={clsx(
                          'p-2 rounded border flex items-center justify-between',
                          isLiquidationClose
                            ? 'bg-loss/10 border-loss/30'
                            : 'bg-surface/50 border-white/5'
                        )}>
                          <div className="flex items-center gap-3 text-xs">
                            <div>
                              <span className="text-neutral">Liq. Price:</span>
                              <span className={clsx(
                                'font-mono-numbers ml-1',
                                isLiquidationClose ? 'text-loss font-bold' : 'text-white'
                              )}>
                                ${pos.liquidation_price.toLocaleString()}
                              </span>
                            </div>
                            <div>
                              <span className="text-neutral">Distance:</span>
                              <span className={clsx(
                                'font-mono-numbers ml-1',
                                distToLiq < 5 ? 'text-loss font-bold' :
                                distToLiq < 15 ? 'text-amber-400' :
                                'text-profit'
                              )}>
                                {distToLiq.toFixed(1)}%
                              </span>
                            </div>
                          </div>
                          {isLiquidationClose && (
                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-loss/20 text-loss font-bold uppercase">
                              Risk
                            </span>
                          )}
                        </div>
                        {/* SL/TP if set */}
                        {(pos.stop_loss || pos.take_profit) && (
                          <div className="flex gap-3 mt-2 text-xs">
                            {pos.stop_loss && (
                              <div className="flex items-center gap-1">
                                <span className="text-loss">SL:</span>
                                <span className="font-mono-numbers text-white">${pos.stop_loss.toLocaleString()}</span>
                              </div>
                            )}
                            {pos.take_profit && (
                              <div className="flex items-center gap-1">
                                <span className="text-profit">TP:</span>
                                <span className="font-mono-numbers text-white">${pos.take_profit.toLocaleString()}</span>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Fees, Funding & Liquidations */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <div className="glass rounded-xl p-4 text-center">
                <div className="text-xs text-neutral mb-1 flex items-center justify-center">
                  Total Fees Paid
                  <InfoTooltip content={GLOSSARY.totalFees} />
                </div>
                <div className="font-mono-numbers font-bold text-lg text-loss">
                  -${analytics?.total_fees_paid?.toFixed(2) || '0.00'}
                </div>
              </div>
              <div className="glass rounded-xl p-4 text-center">
                <div className="text-xs text-neutral mb-1 flex items-center justify-center">
                  Net Funding
                  <InfoTooltip content={GLOSSARY.netFunding} />
                </div>
                <div className={clsx(
                  'font-mono-numbers font-bold text-lg',
                  agent.funding.net >= 0 ? 'text-profit' : 'text-loss'
                )}>
                  {agent.funding.net >= 0 ? '+' : ''}${agent.funding.net.toFixed(2)}
                </div>
              </div>
              <div className="glass rounded-xl p-4 text-center">
                <div className="text-xs text-neutral mb-1 flex items-center justify-center">
                  Total Costs
                  <InfoTooltip content={GLOSSARY.totalCosts} />
                </div>
                <div className="font-mono-numbers font-bold text-lg text-loss">
                  -${((analytics?.total_fees_paid || 0) + Math.abs(Math.min(agent.funding.net, 0))).toFixed(2)}
                </div>
              </div>
              <div className="glass rounded-xl p-4 text-center">
                <div className="text-xs text-neutral mb-1 flex items-center justify-center">
                  Liquidations
                  <InfoTooltip content={GLOSSARY.liquidations} />
                </div>
                <div className={clsx(
                  'font-mono-numbers font-bold text-lg',
                  agent.liquidations > 0 ? 'text-loss' : 'text-white'
                )}>
                  {agent.liquidations}
                </div>
              </div>
            </div>
          </>
        )}

        {/* Decisions Tab */}
        {activeTab === 'decisions' && (
          <div className="glass-strong rounded-xl p-4 sm:p-6">
            <h2 className="text-lg font-semibold mb-4 text-white flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
              Decision History
            </h2>
            {agent.recent_decisions.length === 0 ? (
              <div className="text-center text-neutral py-8">No decisions yet</div>
            ) : (
              <div className="space-y-3 max-h-[600px] overflow-y-auto pr-2">
                {[...agent.recent_decisions]
                  .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
                  .map((dec, index) => (
                  <div key={index} className="p-4 bg-surface/50 rounded-lg border border-white/5">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm text-neutral">
                        Tick {dec.tick} • {new Date(dec.timestamp).toLocaleString()}
                      </span>
                      <span
                        className={clsx(
                          'text-xs font-mono-numbers font-medium px-2 py-1 rounded-full',
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
                    <div className="text-sm text-neutral/90 mb-3 leading-relaxed">
                      {formatReasoning(dec.reasoning)}
                    </div>
                    <div className="flex items-center gap-2">
                      <span
                        className={clsx(
                          'text-xs font-bold px-2.5 py-1 rounded-full',
                          dec.action.includes('long')
                            ? 'bg-profit/10 text-profit'
                            : dec.action.includes('short')
                            ? 'bg-loss/10 text-loss'
                            : dec.action === 'close'
                            ? 'bg-highlight/10 text-highlight'
                            : 'bg-neutral/10 text-neutral'
                        )}
                      >
                        {dec.action.toUpperCase().replace('_', ' ')}
                      </span>
                      {dec.symbol && (
                        <span className="text-xs text-neutral font-mono-numbers">{dec.symbol}</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Trades Tab */}
        {activeTab === 'trades' && (
          <div className="glass-strong rounded-xl p-4 sm:p-6">
            <h2 className="text-lg font-semibold mb-4 text-white flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
              Trade History
            </h2>
            {agent.trades.length === 0 ? (
              <div className="text-center text-neutral py-8">No trades yet</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-neutral text-xs uppercase tracking-wide border-b border-white/10">
                      <th className="text-left pb-3">Symbol</th>
                      <th className="text-left pb-3">Side</th>
                      <th className="text-right pb-3">Size</th>
                      <th className="text-right pb-3">Price</th>
                      <th className="text-right pb-3">P&L</th>
                      <th className="text-right pb-3">Time</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/5">
                    {agent.trades.map((trade) => (
                      <tr key={trade.id} className="hover:bg-surface/50">
                        <td className="py-3 font-mono-numbers">{trade.symbol}</td>
                        <td className="py-3">
                          <span className={clsx(
                            'text-xs font-bold px-2 py-0.5 rounded',
                            trade.side === 'long' ? 'bg-profit/20 text-profit' : 'bg-loss/20 text-loss'
                          )}>
                            {trade.side.toUpperCase()}
                          </span>
                        </td>
                        <td className="py-3 text-right font-mono-numbers">{parseFloat(trade.size).toFixed(6)}</td>
                        <td className="py-3 text-right font-mono-numbers">${parseFloat(trade.price).toLocaleString()}</td>
                        <td className={clsx(
                          'py-3 text-right font-mono-numbers',
                          trade.realized_pnl && parseFloat(trade.realized_pnl) >= 0 ? 'text-profit' :
                          trade.realized_pnl && parseFloat(trade.realized_pnl) < 0 ? 'text-loss' : 'text-neutral'
                        )}>
                          {trade.realized_pnl ? (
                            `${parseFloat(trade.realized_pnl) >= 0 ? '+' : ''}$${parseFloat(trade.realized_pnl).toFixed(2)}`
                          ) : '-'}
                        </td>
                        <td className="py-3 text-right text-neutral text-xs">
                          {new Date(trade.timestamp).toLocaleTimeString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {/* Learning Tab */}
        {activeTab === 'learning' && agent.is_learning_agent && (
          <div className="space-y-4">
            {/* Learning Progress */}
            <LearningProgress agentId={agent.id} />

            {/* Two column layout for panels */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              {/* Similar Situations */}
              <SimilarSituationsPanel agentId={agent.id} />

              {/* Learned Patterns */}
              <LearnedPatternsPanel agentId={agent.id} />
            </div>

            {/* Meta Learning */}
            <MetaLearningPanel agentId={agent.id} />

            {/* Learning Events Feed */}
            <LearningEventsFeed agentFilter={agent.id} limit={10} />
          </div>
        )}

        {/* Description Tab */}
        {activeTab === 'description' && (
          <AgentDescription agent={agent} />
        )}
      </div>
    </div>
  );
}

// Model metadata for personalized descriptions
const MODEL_INFO: Record<string, { displayName: string; description: string; pricing: string; provider: string }> = {
  // Full paths (as resolved by model_registry.py)
  'openai/gpt-oss-20b': {
    displayName: 'GPT-OSS-20B',
    description: 'OpenAI compact open-weight reasoning model (20B)',
    pricing: '$0.05/$0.20 per M tokens',
    provider: 'Together AI',
  },
  'openai/gpt-oss-120b': {
    displayName: 'GPT-OSS-120B',
    description: 'OpenAI open-weight reasoning model (120B)',
    pricing: '$0.15/$0.60 per M tokens',
    provider: 'Together AI',
  },
  // Shorthand aliases (in case config uses shorthand directly)
  'gpt-oss-20b': {
    displayName: 'GPT-OSS-20B',
    description: 'OpenAI compact open-weight reasoning model (20B)',
    pricing: '$0.05/$0.20 per M tokens',
    provider: 'Together AI',
  },
  'gpt-oss-120b': {
    displayName: 'GPT-OSS-120B',
    description: 'OpenAI open-weight reasoning model (120B)',
    pricing: '$0.15/$0.60 per M tokens',
    provider: 'Together AI',
  },
  // GPT-OSS on Ollama Cloud (uses Ollama model naming: model:tag)
  'gpt-oss:120b-cloud': {
    displayName: 'GPT-OSS-120B',
    description: 'OpenAI open-weight reasoning model (120B)',
    pricing: '$20/mo subscription',
    provider: 'Ollama Cloud',
  },
  // Qwen3.5 models — full paths (as resolved by model_registry.py)
  'qwen3.5:397b-cloud': {
    displayName: 'Qwen3.5-397B',
    description: 'Alibaba flagship MoE model (397B total, 17B active)',
    pricing: '$20/mo subscription',
    provider: 'Ollama Cloud',
  },
  'qwen/qwen3.5-122b-a10b': {
    displayName: 'Qwen3.5-122B',
    description: 'Alibaba MoE reasoning model (122B total, 10B active)',
    pricing: '$0.10/$0.40 per M tokens',
    provider: 'OpenRouter',
  },
  // Qwen3.5 shorthand aliases
  'qwen3.5-397b': {
    displayName: 'Qwen3.5-397B',
    description: 'Alibaba flagship MoE model (397B total, 17B active)',
    pricing: '$20/mo subscription',
    provider: 'Ollama Cloud',
  },
  'qwen3.5-122b': {
    displayName: 'Qwen3.5-122B',
    description: 'Alibaba MoE reasoning model (122B total, 10B active)',
    pricing: '$0.10/$0.40 per M tokens',
    provider: 'OpenRouter',
  },
};

function getModelInfo(model: string) {
  return MODEL_INFO[model] || {
    displayName: model,
    description: 'LLM model',
    pricing: 'unknown',
    provider: 'unknown',
  };
}

// Agent descriptions by type - personalized with agent model
function getAgentDescription(agent: AgentDetailData): {
  overview: string;
  mechanics: string[];
  strengths: string[];
  weaknesses: string[];
  config?: Record<string, string | number>;
} {
  const m = getModelInfo(agent.model);

  const descriptions: Record<string, ReturnType<typeof getAgentDescription>> = {
  'Learning': {
    overview: 'Learning agents use a RAG (Retrieval-Augmented Generation) system to learn from past decisions and outcomes. They build a database of situations and can recall similar historical contexts to inform current decisions.',
    mechanics: [
      'Embeds market context using OpenAI embeddings for semantic similarity search',
      'Stores decision outcomes with full context for later retrieval',
      'Before each decision, retrieves similar past situations using vector similarity',
      'Learns patterns from successful and failed trades',
      'Uses meta-learning to identify which strategies work best in different market regimes',
    ],
    strengths: [
      'Improves over time by learning from mistakes',
      'Can identify patterns humans might miss',
      'Adapts to changing market conditions',
      'Remembers successful strategies in similar situations',
    ],
    weaknesses: [
      'Requires time to build up useful memory',
      'Cold start problem - no learning data initially',
      'May overfit to recent patterns',
      'Higher computational cost for embedding generation',
    ],
  },
  'Journal-Aware': {
    overview: `Journal-Aware agents represent the full Observer intelligence stack: skills, forum witness, AND daily journal briefing. Powered by ${m.displayName} via ${m.provider}. Before each decision, the agent reads a personalized report card from the Observer's daily journal — including performance critique, market recap, and specific recommendations. This is the A/B test variant against Forum-Aware (does the Observer's daily report card improve decision-making?).`,
    mechanics: [
      'Inherits full Forum-Aware capabilities (skills + forum witness + ReAct tool loop)',
      'Loads latest Observer journal entry with personalized agent briefing',
      'Journal sections: Market Overview, Personal Report Card, Forum Quality, Recommendations',
      'Report card includes: trade count, win rate, PnL, overtrading score, specific critique',
      'Priority order: position advisories > journal report card > skills > forum witness',
      'If Observer flags overtrading, agent reduces activity; if missed opportunities flagged, widens criteria',
      'Posts trade rationale back to the forum strategy channel',
    ],
    strengths: [
      'Receives personalized performance critique from the Observer',
      'Can self-correct based on data-driven feedback (overtrading, missed opportunities)',
      'Combines all intelligence layers: skills + forum + journal',
      'Journal provides strategic adjustments, skills provide patterns, forum provides timing',
    ],
    weaknesses: [
      'Depends on journal generation running (Observer + Anthropic API)',
      'Journal may be stale if generation fails or is delayed',
      'Largest context window of all agent types (skills + witness + journal)',
      'Experimental — measuring whether self-awareness improves trading',
    ],
    config: {
      'Model': `${m.displayName} (${m.provider})`,
      'Journal Lookback': '1 day',
      'Witness Lookback': '6h',
      'Min Confidence': '60%',
      'Post to Forum': 'Yes',
      'Max Iterations': 2,
    },
  },
  'Forum-Aware': {
    overview: `Forum-Aware agents extend the Skill-Aware approach with access to forum witness summaries. Powered by ${m.displayName} via ${m.provider}. They combine learned skills, tool-based analysis, and Observer-analyzed insights from forum discussions between MarketAnalyst and Contrarian agents. This is the control variant in an A/B test against the Journal-Aware experimental.`,
    mechanics: [
      'Inherits full Skill-Aware capabilities (skills + ReAct tool loop)',
      'Loads witness summaries from Observer forum analysis (6h lookback, 60% min confidence)',
      'Witness types: exit_timing, entry_signal, risk_warning, regime_insight',
      'Injects witness context alongside skills before each decision',
      'Posts trade rationale back to the forum strategy channel',
      'When witness conflicts with skills, prefers skills (more statistical data)',
    ],
    strengths: [
      'Access to real-time discussion insights not available to other agents',
      'Witness summaries are pre-analyzed by the Observer (high signal)',
      'Can pick up on qualitative signals from AI-to-AI debate',
      'Posts rationale to forum, closing the feedback loop',
    ],
    weaknesses: [
      'Depends on discussion agents posting useful analysis',
      'Witness summaries may be stale if Observer analysis is delayed',
      'Extra context increases prompt size and inference time',
      'No self-awareness — cannot adjust based on own performance review',
    ],
    config: {
      'Model': `${m.displayName} (${m.provider})`,
      'Witness Lookback': '6h',
      'Min Confidence': '60%',
      'Post to Forum': 'Yes',
      'Max Iterations': 2,
    },
  },
  'Skill-Aware': {
    overview: `Skill-Aware agents combine learned skills from the Observer Agent with the full agentic ReAct tool loop. Powered by ${m.displayName} via ${m.provider}. They consult .claude/skills/ for trading wisdom, market regime patterns, and risk management rules before making decisions. This is the control variant in an A/B test against the Forum-Aware experimental.`,
    mechanics: [
      'Loads skills from .claude/skills/: trading-wisdom, market-regimes, risk-management, entry-signals',
      'Uses LangGraph ReAct loop (Think → Act → Observe, max 2 iterations)',
      'Tools: recommend_skill, trading_skills + all 8 agentic trading tools',
      'Skills represent statistical evidence from past competition data, generated by the Observer Agent',
      'Higher confidence skills carry more weight in decisions',
    ],
    strengths: [
      'Benefits from collective learning across all agents via Observer',
      'Access to proven patterns and risk rules',
      'Can validate decisions with technical analysis tools',
      'Adapts as skills are updated by Observer Agent',
    ],
    weaknesses: [
      'Depends on quality and freshness of Observer-generated skills',
      'More tools and context means longer inference time',
      'Skills may lag behind rapidly changing market regimes',
      'May over-rely on historical patterns that no longer hold',
    ],
    config: {
      'Model': `${m.displayName} (${m.provider})`,
      'Skills Dir': '.claude/skills',
      'Always Check': 'Yes',
      'Max Iterations': 2,
    },
  },
  'Skill-Only': {
    overview: 'Skill-Only agents make decisions purely based on learned skills without using additional tools. They are simpler and faster than Skill-Aware agents.',
    mechanics: [
      'Loads trading skills from .claude/skills/ directory',
      'Makes decisions based solely on skill guidance and market data',
      'No tool usage - single LLM call per decision',
      'Relies on Observer Agent to update skills periodically',
    ],
    strengths: [
      'Fast decision-making with single LLM call',
      'Lower cost than tool-using agents',
      'Benefits from Observer Agent learning',
    ],
    weaknesses: [
      'Cannot perform detailed technical analysis',
      'Limited to patterns captured in skills',
      'No real-time validation of decisions',
    ],
  },
  'Agentic': {
    overview: `Agentic traders use a LangGraph ReAct (Reasoning + Acting) loop. They think, use tools, observe results, and iterate before making a final decision. Powered by ${m.displayName} (${m.description}) via ${m.provider}.`,
    mechanics: [
      'ReAct Loop: Think → Act → Observe → Repeat (max 2 iterations)',
      'Must use at least 2 tools before deciding (enforced by graph)',
      'Tools: validate_trade, reflect_on_performance, technical_analysis, multi_timeframe_analysis, portfolio_risk_analysis, risk_calculator, trade_history, market_search',
      `${m.displayName} provides native tool calling via ${m.provider} — no custom parsers needed`,
      'Decision node uses tool_choice=none to force JSON output',
    ],
    strengths: [
      'Thorough analysis before each decision',
      'Can validate trades before execution',
      'Uses multiple data sources for confirmation',
      'Self-reflective - learns from recent performance',
      `${m.displayName} via ${m.provider} at ${m.pricing}`,
    ],
    weaknesses: [
      'Slower due to multiple LLM calls per tick (up to 120s timeout)',
      'Higher cost than simple traders due to multi-step reasoning',
      'May over-analyze in fast-moving markets',
      'Tool calling reliability varies by model',
    ],
    config: {
      'Max Iterations': 2,
      'Timeout': '120s',
      'Model': `${m.displayName} (${m.provider})`,
    },
  },
  'LLM': {
    overview: `Simple LLM agents make single-call decisions using only market data and portfolio context. They rely on the model's inherent reasoning capabilities without additional tools. Powered by ${m.displayName} (${m.description}) via ${m.provider}.`,
    mechanics: [
      `Single LLM call per tick via ${m.provider} (${m.displayName})`,
      'Receives: market prices, 24h changes, funding rates, portfolio state',
      'Pre-computed technical analysis: RSI, SMA, MACD, Bollinger Bands from 1h candles',
      'Includes recent decision history and trade performance summary',
      'Returns: action, symbol, size, leverage, confidence, reasoning as JSON',
      'Character/persona defined in config shapes decision style',
    ],
    strengths: [
      'Fast - single inference call per decision',
      `Low cost via ${m.provider} (${m.pricing})`,
      'Receives pre-computed TA indicators without needing tools',
      'Character-driven strategies create diverse competition',
      'Good baseline for measuring tool/skill value add',
    ],
    weaknesses: [
      'Cannot run custom tool queries or multi-step analysis',
      'No access to Observer-generated skills or forum witness data',
      'Limited to pre-computed indicators — cannot explore different timeframes on demand',
      'No learning or adaptation between ticks',
    ],
    config: {
      'Model': `${m.displayName} (${m.provider})`,
      'Pricing': m.pricing,
    },
  },
  'TA': {
    overview: 'Technical Analysis agents use rule-based trading strategies based on classic indicators. No LLM involved - purely algorithmic. Serves as a zero-cost deterministic benchmark.',
    mechanics: [
      'RSI (Relative Strength Index): Period=14, Oversold=30, Overbought=70',
      'SMA Crossover: Short=20 periods, Long=50 periods',
      'Entry: RSI oversold + price above SMA = long, RSI overbought + price below SMA = short',
      'Fixed position sizing: 15% of equity, 3x leverage',
    ],
    strengths: [
      'Deterministic - same inputs always produce same outputs',
      'Zero cost - no LLM calls, pure computation',
      'Battle-tested indicator logic',
      'Fast execution - instant decisions',
    ],
    weaknesses: [
      'Cannot adapt to unusual market conditions',
      'No reasoning or context awareness',
      'May generate false signals in ranging markets',
      'No learning capability',
    ],
    config: {
      'RSI Period': 14,
      'RSI Oversold': 30,
      'RSI Overbought': 70,
      'SMA Short': 20,
      'SMA Long': 50,
      'Position Size': '15%',
      'Leverage': '3x',
    },
  },
  'Passive': {
    overview: 'Index Fund agents follow a passive buy-and-hold strategy, allocating capital equally across available symbols. Serves as the ultimate benchmark - any active strategy that underperforms this is destroying value.',
    mechanics: [
      'Equal-weight allocation across symbols at competition start',
      'Opens long positions and holds indefinitely',
      'No active trading - only re-enters if liquidated',
      'No stop-losses or take-profits',
    ],
    strengths: [
      'Zero cost - no LLM calls after initial allocation',
      'The benchmark every active strategy must beat',
      'No overtrading, no emotional decisions',
      'Benefits from overall market growth',
    ],
    weaknesses: [
      'Cannot profit in bear markets',
      'No risk management or stop-losses',
      'Fully exposed to market drawdowns',
      'Cannot take advantage of short-term opportunities',
    ],
    config: {
      'Allocation': '$2,000/symbol (20%)',
      'Leverage': '1x',
      'Strategy': 'Buy & Hold',
    },
  },
  };

  return descriptions[agent.agent_type] || {
    overview: `Custom agent implementation: ${agent.agent_type_description || 'No description available.'}`,
    mechanics: ['Custom trading logic defined by the agent implementation.'],
    strengths: ['Specialized for specific use case.'],
    weaknesses: ['May have undocumented behaviors.'],
  };
}

function AgentDescription({ agent }: { agent: AgentDetailData }) {
  const description = getAgentDescription(agent);

  return (
    <div className="space-y-4">
      {/* Overview */}
      <div className="glass-strong rounded-xl p-4 sm:p-6">
        <h2 className="text-lg font-semibold mb-3 text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-purple-400"></span>
          How This Agent Works
        </h2>
        <p className="text-neutral/90 leading-relaxed">
          {description.overview}
        </p>
      </div>

      {/* Mechanics */}
      <div className="glass-strong rounded-xl p-4 sm:p-6">
        <h3 className="text-md font-semibold mb-3 text-white">Mechanics</h3>
        <ul className="space-y-2">
          {description.mechanics.map((item, i) => (
            <li key={i} className="flex gap-3 text-sm">
              <span className="text-accent flex-shrink-0">•</span>
              <span className="text-neutral/90">{item}</span>
            </li>
          ))}
        </ul>
      </div>

      {/* Strengths & Weaknesses */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="glass-strong rounded-xl p-4">
          <h3 className="text-md font-semibold mb-3 text-profit flex items-center gap-2">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
            Strengths
          </h3>
          <ul className="space-y-2">
            {description.strengths.map((item, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <span className="text-profit/60 flex-shrink-0">+</span>
                <span className="text-neutral/80">{item}</span>
              </li>
            ))}
          </ul>
        </div>

        <div className="glass-strong rounded-xl p-4">
          <h3 className="text-md font-semibold mb-3 text-loss flex items-center gap-2">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
            Weaknesses
          </h3>
          <ul className="space-y-2">
            {description.weaknesses.map((item, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <span className="text-loss/60 flex-shrink-0">-</span>
                <span className="text-neutral/80">{item}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>

      {/* Configuration (if available) */}
      {description.config && (
        <div className="glass-strong rounded-xl p-4 sm:p-6">
          <h3 className="text-md font-semibold mb-3 text-white">Configuration</h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
            {Object.entries(description.config).map(([key, value]) => (
              <div key={key} className="p-2 bg-surface/50 rounded-lg">
                <div className="text-xs text-neutral">{key}</div>
                <div className="text-sm font-mono-numbers text-white">{value}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Agent-specific info */}
      <div className="glass-strong rounded-xl p-4 sm:p-6">
        <h3 className="text-md font-semibold mb-3 text-white">This Agent</h3>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-neutral">Type:</span>
            <span className="text-white font-medium">{agent.agent_type}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-neutral">Model:</span>
            <span className="text-white font-mono">{agent.model}</span>
          </div>
          {agent.agent_type_description && (
            <div className="flex justify-between">
              <span className="text-neutral">Framework:</span>
              <span className="text-white">{agent.agent_type_description}</span>
            </div>
          )}
          {agent.character && (
            <div className="mt-3 pt-3 border-t border-white/10">
              <div className="text-neutral mb-2">Personality/Character:</div>
              <div className="text-neutral/80 italic text-xs p-2 bg-surface/30 rounded-lg">
                {agent.character.length > 300
                  ? agent.character.substring(0, 300) + '...'
                  : agent.character}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
