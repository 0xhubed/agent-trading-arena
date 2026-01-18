/**
 * DecisionExplanation component - shows decision with learning influences.
 */

import clsx from 'clsx';
import type { EnhancedDecision } from '../../types/learning';

interface DecisionExplanationProps {
  decision: EnhancedDecision;
  className?: string;
  compact?: boolean;
}

function InfluenceBadge({
  type,
  count,
  weight,
}: {
  type: 'rag' | 'pattern' | 'meta';
  count: number;
  weight?: number;
}) {
  const config = {
    rag: { label: 'Similar Situations', icon: '🔍', color: 'text-highlight', bg: 'bg-highlight/20' },
    pattern: { label: 'Patterns', icon: '📊', color: 'text-accent', bg: 'bg-accent/20' },
    meta: { label: 'Top Agents', icon: '🧠', color: 'text-profit', bg: 'bg-profit/20' },
  };

  const c = config[type];

  return (
    <div
      className={clsx(
        'flex items-center gap-1.5 px-2 py-1 rounded text-xs',
        c.bg
      )}
      title={`${count} ${c.label.toLowerCase()} influenced this decision`}
    >
      <span>{c.icon}</span>
      <span className={c.color}>{count}</span>
      {weight !== undefined && (
        <span className="text-neutral">({Math.round(weight * 100)}%)</span>
      )}
    </div>
  );
}

export default function DecisionExplanation({
  decision,
  className,
  compact = false,
}: DecisionExplanationProps) {
  const { influences, is_learning_agent } = decision;
  const { action, symbol, confidence, reasoning } = decision.decision;

  if (!is_learning_agent || !influences) {
    // Regular decision display for non-learning agents
    return (
      <div className={clsx('p-3 rounded-lg bg-surface/50 border border-white/5', className)}>
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            <span
              className={clsx(
                'text-xs font-bold px-2 py-0.5 rounded',
                action.includes('long') ? 'bg-profit/20 text-profit' :
                action.includes('short') ? 'bg-loss/20 text-loss' :
                action === 'close' ? 'bg-highlight/20 text-highlight' :
                'bg-neutral/20 text-neutral'
              )}
            >
              {action.toUpperCase().replace('_', ' ')}
            </span>
            {symbol && (
              <span className="text-xs font-mono-numbers text-white">{symbol}</span>
            )}
          </div>
          <span
            className={clsx(
              'text-xs font-mono-numbers px-2 py-0.5 rounded-full',
              confidence >= 0.7 ? 'bg-profit/20 text-profit' :
              confidence >= 0.4 ? 'bg-neutral/20 text-neutral' :
              'bg-loss/20 text-loss'
            )}
          >
            {Math.round(confidence * 100)}%
          </span>
        </div>
        {!compact && reasoning && (
          <p className="text-xs text-neutral/80 line-clamp-3">"{reasoning}"</p>
        )}
      </div>
    );
  }

  // Enhanced display for learning agents
  return (
    <div
      className={clsx(
        'p-3 rounded-lg border transition-all',
        'bg-accent/5 border-accent/20',
        className
      )}
    >
      {/* Learning agent badge */}
      <div className="flex items-center gap-2 mb-2">
        <span className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-accent/20 text-accent uppercase tracking-wide">
          Learning Agent
        </span>
        {influences.regime && (
          <span
            className={clsx(
              'text-[10px] px-1.5 py-0.5 rounded uppercase',
              influences.regime === 'trending_up' ? 'bg-profit/10 text-profit' :
              influences.regime === 'trending_down' ? 'bg-loss/10 text-loss' :
              influences.regime === 'volatile' ? 'bg-amber-500/10 text-amber-400' :
              'bg-neutral/10 text-neutral'
            )}
          >
            {influences.regime.replace('_', ' ')}
          </span>
        )}
      </div>

      {/* Decision action */}
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span
            className={clsx(
              'text-xs font-bold px-2 py-0.5 rounded',
              action.includes('long') ? 'bg-profit/20 text-profit' :
              action.includes('short') ? 'bg-loss/20 text-loss' :
              action === 'close' ? 'bg-highlight/20 text-highlight' :
              'bg-neutral/20 text-neutral'
            )}
          >
            {action.toUpperCase().replace('_', ' ')}
          </span>
          {symbol && (
            <span className="text-xs font-mono-numbers text-white">{symbol}</span>
          )}
        </div>
        <span
          className={clsx(
            'text-xs font-mono-numbers px-2 py-0.5 rounded-full',
            confidence >= 0.7 ? 'bg-profit/20 text-profit' :
            confidence >= 0.4 ? 'bg-neutral/20 text-neutral' :
            'bg-loss/20 text-loss'
          )}
        >
          {Math.round(confidence * 100)}%
        </span>
      </div>

      {/* Influences */}
      <div className="flex flex-wrap gap-2 mb-2">
        {influences.similar_situations > 0 && (
          <InfluenceBadge type="rag" count={influences.similar_situations} />
        )}
        {influences.matching_patterns > 0 && (
          <InfluenceBadge type="pattern" count={influences.matching_patterns} />
        )}
        {influences.top_agents_considered > 0 && (
          <InfluenceBadge type="meta" count={influences.top_agents_considered} />
        )}
      </div>

      {/* Reasoning */}
      {!compact && reasoning && (
        <div className="pt-2 border-t border-white/5">
          <div className="text-[10px] text-neutral uppercase tracking-wide mb-1">Reasoning</div>
          <p className="text-xs text-neutral/80 line-clamp-4">"{reasoning}"</p>
        </div>
      )}
    </div>
  );
}
