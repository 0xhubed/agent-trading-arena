/**
 * SimilarSituationsPanel component - displays RAG retrieval results.
 */

import { useEffect } from 'react';
import clsx from 'clsx';
import {
  useLearningStore,
  fetchSimilarSituations,
} from '../../stores/learning';
import type { SimilarSituation } from '../../types/learning';

interface SimilarSituationsPanelProps {
  agentId: string;
  decisionId?: number;
  className?: string;
}

function SimilarityBadge({ similarity }: { similarity: number }) {
  const pct = Math.round(similarity * 100);
  return (
    <div
      className={clsx(
        'text-xs font-mono-numbers font-bold px-2 py-0.5 rounded-full',
        pct >= 80 ? 'bg-profit/20 text-profit' :
        pct >= 60 ? 'bg-accent/20 text-accent' :
        'bg-neutral/20 text-neutral'
      )}
    >
      {pct}% similar
    </div>
  );
}

function SituationCard({ situation }: { situation: SimilarSituation }) {
  const { decision, outcome } = situation;
  const isProfitable = outcome.was_profitable;

  return (
    <div
      className={clsx(
        'p-3 rounded-lg border transition-all',
        isProfitable
          ? 'bg-profit/5 border-profit/20'
          : 'bg-loss/5 border-loss/20'
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <SimilarityBadge similarity={situation.similarity} />
        <span className="text-xs text-neutral">
          {new Date(situation.timestamp).toLocaleDateString()}
        </span>
      </div>

      {/* Decision info */}
      <div className="mb-2">
        <div className="flex items-center gap-2 mb-1">
          <span
            className={clsx(
              'text-xs font-bold px-2 py-0.5 rounded',
              decision.action.includes('long') ? 'bg-profit/20 text-profit' :
              decision.action.includes('short') ? 'bg-loss/20 text-loss' :
              'bg-neutral/20 text-neutral'
            )}
          >
            {decision.action.toUpperCase().replace('_', ' ')}
          </span>
          {decision.symbol && (
            <span className="text-xs font-mono-numbers text-white">{decision.symbol}</span>
          )}
          <span className="text-xs text-neutral ml-auto">
            {(decision.confidence * 100).toFixed(0)}% conf
          </span>
        </div>
        {decision.reasoning && (
          <p className="text-xs text-neutral/80 line-clamp-2">
            "{decision.reasoning}"
          </p>
        )}
      </div>

      {/* Outcome */}
      <div className="flex items-center justify-between pt-2 border-t border-white/5">
        <span className="text-xs text-neutral">Outcome:</span>
        <div className="flex items-center gap-2">
          <span
            className={clsx(
              'font-mono-numbers text-sm font-medium',
              isProfitable ? 'text-profit' : 'text-loss'
            )}
          >
            {outcome.realized_pnl >= 0 ? '+' : ''}${outcome.realized_pnl.toFixed(2)}
          </span>
          <span
            className={clsx(
              'text-xs px-1.5 py-0.5 rounded',
              outcome.outcome_score >= 0.5 ? 'bg-profit/10 text-profit' :
              outcome.outcome_score >= 0 ? 'bg-neutral/10 text-neutral' :
              'bg-loss/10 text-loss'
            )}
          >
            Score: {outcome.outcome_score.toFixed(2)}
          </span>
        </div>
      </div>

      {/* Regime badge */}
      <div className="mt-2">
        <span
          className={clsx(
            'text-[10px] uppercase tracking-wide px-1.5 py-0.5 rounded',
            situation.regime === 'trending_up' ? 'bg-profit/10 text-profit' :
            situation.regime === 'trending_down' ? 'bg-loss/10 text-loss' :
            situation.regime === 'volatile' ? 'bg-amber-500/10 text-amber-400' :
            'bg-neutral/10 text-neutral'
          )}
        >
          {situation.regime.replace('_', ' ')}
        </span>
      </div>
    </div>
  );
}

export default function SimilarSituationsPanel({
  agentId,
  decisionId,
  className,
}: SimilarSituationsPanelProps) {
  const { similarSituationsCache, loading, setLoading, setSimilarSituations } = useLearningStore();

  const cacheKey = decisionId ? `${agentId}-${decisionId}` : agentId;
  const situations = similarSituationsCache[cacheKey] || [];
  const isLoading = loading.situations[cacheKey];

  useEffect(() => {
    async function loadData() {
      if (situations.length > 0) return; // Already loaded
      setLoading('situations', cacheKey, true);
      const data = await fetchSimilarSituations(agentId, decisionId, 5);
      setSimilarSituations(cacheKey, data);
      setLoading('situations', cacheKey, false);
    }
    loadData();
  }, [agentId, decisionId, cacheKey, situations.length, setLoading, setSimilarSituations]);

  if (isLoading) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
          Similar Historical Situations
        </h3>
        <div className="flex items-center justify-center py-6">
          <div className="w-2 h-2 bg-highlight rounded-full animate-ping mr-2" />
          <span className="text-neutral text-sm">Searching memories...</span>
        </div>
      </div>
    );
  }

  if (situations.length === 0) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
          Similar Historical Situations
        </h3>
        <div className="text-center text-neutral py-6 text-sm">
          <div className="w-10 h-10 mx-auto mb-2 rounded-full bg-surface flex items-center justify-center">
            <span className="text-xl">🔍</span>
          </div>
          No similar situations found in memory
        </div>
      </div>
    );
  }

  // Calculate stats
  const profitableSituations = situations.filter((s) => s.outcome.was_profitable);
  const avgPnl = situations.reduce((sum, s) => sum + s.outcome.realized_pnl, 0) / situations.length;

  return (
    <div className={clsx('glass-strong rounded-xl p-4', className)}>
      <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
        Similar Historical Situations
        <span className="text-xs text-neutral ml-auto">
          {situations.length} found
        </span>
      </h3>

      {/* Summary stats */}
      <div className="flex items-center gap-4 mb-3 text-xs">
        <div className="flex items-center gap-1">
          <span className="text-neutral">Profitable:</span>
          <span
            className={clsx(
              'font-mono-numbers',
              profitableSituations.length > situations.length / 2 ? 'text-profit' : 'text-loss'
            )}
          >
            {profitableSituations.length}/{situations.length}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <span className="text-neutral">Avg P&L:</span>
          <span
            className={clsx(
              'font-mono-numbers',
              avgPnl >= 0 ? 'text-profit' : 'text-loss'
            )}
          >
            {avgPnl >= 0 ? '+' : ''}${avgPnl.toFixed(2)}
          </span>
        </div>
      </div>

      {/* Situations list */}
      <div className="space-y-3 max-h-[400px] overflow-y-auto pr-1">
        {situations.map((situation, index) => (
          <SituationCard key={situation.id || index} situation={situation} />
        ))}
      </div>

      {/* Learning insight */}
      <div className="mt-3 p-2 rounded bg-accent/5 border border-accent/20">
        <div className="text-xs text-accent font-medium mb-1">Learning Insight</div>
        <p className="text-xs text-neutral/80">
          {profitableSituations.length > situations.length / 2
            ? `${Math.round((profitableSituations.length / situations.length) * 100)}% of similar past situations were profitable. Consider following this pattern.`
            : `Only ${Math.round((profitableSituations.length / situations.length) * 100)}% of similar situations were profitable. Exercise caution.`}
        </p>
      </div>
    </div>
  );
}
