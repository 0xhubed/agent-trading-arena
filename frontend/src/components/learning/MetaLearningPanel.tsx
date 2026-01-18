/**
 * MetaLearningPanel component - displays meta-learning insights and top performers.
 */

import { useEffect } from 'react';
import clsx from 'clsx';
import {
  useLearningStore,
  fetchMetaAnalysis,
} from '../../stores/learning';
import type { RegimePerformance } from '../../types/learning';

interface MetaLearningPanelProps {
  agentId: string;
  className?: string;
}

function RegimeBadge({ regime }: { regime: string }) {
  const regimeConfig: Record<string, { label: string; color: string; bg: string }> = {
    trending_up: { label: 'Trending Up', color: 'text-profit', bg: 'bg-profit/20' },
    trending_down: { label: 'Trending Down', color: 'text-loss', bg: 'bg-loss/20' },
    ranging: { label: 'Ranging', color: 'text-neutral', bg: 'bg-neutral/20' },
    volatile: { label: 'Volatile', color: 'text-amber-400', bg: 'bg-amber-500/20' },
    unknown: { label: 'Unknown', color: 'text-neutral', bg: 'bg-neutral/20' },
  };

  const config = regimeConfig[regime] || regimeConfig.unknown;

  return (
    <span className={clsx('text-xs font-medium px-2 py-1 rounded', config.bg, config.color)}>
      {config.label}
    </span>
  );
}

function PerformerRow({
  performer,
  rank,
  isCurrentAgent,
}: {
  performer: RegimePerformance;
  rank: number;
  isCurrentAgent: boolean;
}) {
  return (
    <div
      className={clsx(
        'flex items-center gap-3 p-2 rounded-lg transition-all',
        isCurrentAgent ? 'bg-accent/10 ring-1 ring-accent/30' : 'bg-surface/50 hover:bg-surface/80'
      )}
    >
      {/* Rank */}
      <div
        className={clsx(
          'w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold',
          rank === 1 ? 'bg-accent text-white' :
          rank === 2 ? 'bg-neutral/50 text-white' :
          rank === 3 ? 'bg-amber-600/50 text-amber-200' :
          'bg-surface text-neutral'
        )}
      >
        {rank}
      </div>

      {/* Agent info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span
            className={clsx(
              'font-medium text-sm truncate',
              isCurrentAgent ? 'text-accent' : 'text-white'
            )}
          >
            {performer.agent_name || performer.agent_id}
          </span>
          {isCurrentAgent && (
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-accent/20 text-accent">
              YOU
            </span>
          )}
        </div>
      </div>

      {/* Stats */}
      <div className="flex items-center gap-3 text-xs">
        <div className="text-center">
          <div className="text-neutral">Win Rate</div>
          <div
            className={clsx(
              'font-mono-numbers font-medium',
              performer.win_rate >= 0.5 ? 'text-profit' : 'text-loss'
            )}
          >
            {Math.round(performer.win_rate * 100)}%
          </div>
        </div>
        <div className="text-center">
          <div className="text-neutral">Sharpe</div>
          <div
            className={clsx(
              'font-mono-numbers font-medium',
              performer.sharpe_ratio > 0 ? 'text-profit' : 'text-loss'
            )}
          >
            {performer.sharpe_ratio.toFixed(2)}
          </div>
        </div>
        <div className="text-center">
          <div className="text-neutral">P&L</div>
          <div
            className={clsx(
              'font-mono-numbers font-medium',
              performer.total_pnl >= 0 ? 'text-profit' : 'text-loss'
            )}
          >
            {performer.total_pnl >= 0 ? '+' : ''}${performer.total_pnl.toFixed(0)}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function MetaLearningPanel({ agentId, className }: MetaLearningPanelProps) {
  const { metaAnalysisCache, loading, setLoading, setMetaAnalysis } = useLearningStore();

  const analysis = metaAnalysisCache[agentId];
  const isLoading = loading.meta[agentId];

  useEffect(() => {
    async function loadData() {
      if (analysis) return; // Already loaded
      setLoading('meta', agentId, true);
      const data = await fetchMetaAnalysis(agentId);
      if (data) {
        setMetaAnalysis(agentId, data);
      }
      setLoading('meta', agentId, false);
    }
    loadData();
  }, [agentId, analysis, setLoading, setMetaAnalysis]);

  if (isLoading) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
          Meta-Learning Insights
        </h3>
        <div className="flex items-center justify-center py-6">
          <div className="w-2 h-2 bg-highlight rounded-full animate-ping mr-2" />
          <span className="text-neutral text-sm">Analyzing performance...</span>
        </div>
      </div>
    );
  }

  if (!analysis) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
          Meta-Learning Insights
        </h3>
        <div className="text-center text-neutral py-6 text-sm">
          <div className="w-10 h-10 mx-auto mb-2 rounded-full bg-surface flex items-center justify-center">
            <span className="text-xl">🧠</span>
          </div>
          No meta-learning data available
        </div>
      </div>
    );
  }

  return (
    <div className={clsx('glass-strong rounded-xl p-4', className)}>
      <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
        Meta-Learning Insights
      </h3>

      {/* Current regime */}
      <div className="flex items-center gap-3 mb-4">
        <span className="text-xs text-neutral">Current Market Regime:</span>
        <RegimeBadge regime={analysis.current_regime} />
      </div>

      {/* Agent's rank in this regime */}
      {analysis.this_agent_rank !== null && (
        <div className="p-3 rounded-lg bg-accent/5 border border-accent/20 mb-4">
          <div className="flex items-center justify-between">
            <span className="text-sm text-white">Your Rank in This Regime</span>
            <span
              className={clsx(
                'font-mono-numbers font-bold text-lg',
                analysis.this_agent_rank <= 3 ? 'text-accent' : 'text-white'
              )}
            >
              #{analysis.this_agent_rank}
            </span>
          </div>
        </div>
      )}

      {/* Top performers */}
      {analysis.top_performers.length > 0 && (
        <>
          <div className="text-xs text-neutral mb-2">
            Top Performers in {analysis.current_regime.replace('_', ' ')} markets:
          </div>
          <div className="space-y-2 mb-4">
            {analysis.top_performers.slice(0, 5).map((performer, index) => (
              <PerformerRow
                key={performer.agent_id}
                performer={performer}
                rank={index + 1}
                isCurrentAgent={performer.agent_id === agentId}
              />
            ))}
          </div>
        </>
      )}

      {/* Insight */}
      {analysis.insight && (
        <div className="p-3 rounded-lg bg-highlight/5 border border-highlight/20">
          <div className="text-xs text-highlight font-medium mb-1">Strategy Insight</div>
          <p className="text-sm text-neutral/90">{analysis.insight}</p>
        </div>
      )}
    </div>
  );
}
