/**
 * LearnedPatternsPanel component - displays learned trading patterns.
 */

import { useEffect } from 'react';
import clsx from 'clsx';
import {
  useLearningStore,
  fetchAgentPatterns,
} from '../../stores/learning';
import type { LearnedPattern, PatternType } from '../../types/learning';

interface LearnedPatternsPanelProps {
  agentId: string;
  className?: string;
}

const patternTypeConfig: Record<PatternType, { label: string; color: string; bg: string }> = {
  entry_signal: { label: 'ENTRY', color: 'text-profit', bg: 'bg-profit/20' },
  exit_signal: { label: 'EXIT', color: 'text-loss', bg: 'bg-loss/20' },
  risk_rule: { label: 'RISK', color: 'text-amber-400', bg: 'bg-amber-500/20' },
  regime_rule: { label: 'REGIME', color: 'text-accent', bg: 'bg-accent/20' },
};

function PatternTypeBadge({ type }: { type: PatternType }) {
  const config = patternTypeConfig[type] || patternTypeConfig.regime_rule;
  return (
    <span className={clsx('text-[10px] font-bold px-1.5 py-0.5 rounded uppercase', config.bg, config.color)}>
      {config.label}
    </span>
  );
}

function ConfidenceBar({ confidence }: { confidence: number }) {
  const pct = Math.round(confidence * 100);
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-surface rounded-full overflow-hidden">
        <div
          className={clsx(
            'h-full rounded-full transition-all',
            pct >= 80 ? 'bg-profit' : pct >= 60 ? 'bg-accent' : 'bg-neutral'
          )}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs font-mono-numbers text-neutral w-8">{pct}%</span>
    </div>
  );
}

function PatternCard({ pattern }: { pattern: LearnedPattern }) {
  const isHighConfidence = pattern.confidence >= 0.7;
  const isRecentlyValidated =
    pattern.last_validated &&
    new Date(pattern.last_validated) > new Date(Date.now() - 24 * 60 * 60 * 1000);

  return (
    <div
      className={clsx(
        'p-3 rounded-lg border transition-all',
        isHighConfidence
          ? 'bg-accent/5 border-accent/20'
          : 'bg-surface/50 border-white/5'
      )}
    >
      {/* Header */}
      <div className="flex items-center gap-2 mb-2">
        <PatternTypeBadge type={pattern.pattern_type} />
        {isRecentlyValidated && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-profit/10 text-profit">
            Recently validated
          </span>
        )}
        {!pattern.is_active && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-loss/10 text-loss">
            Inactive
          </span>
        )}
      </div>

      {/* Lesson / Description */}
      <p className="text-sm text-white mb-2">{pattern.lesson}</p>

      {/* Recommended action */}
      {pattern.recommended_action && (
        <div className="flex items-center gap-2 mb-2">
          <span className="text-xs text-neutral">Recommended:</span>
          <span
            className={clsx(
              'text-xs font-medium px-2 py-0.5 rounded',
              pattern.recommended_action.includes('long') ? 'bg-profit/20 text-profit' :
              pattern.recommended_action.includes('short') ? 'bg-loss/20 text-loss' :
              'bg-neutral/20 text-white'
            )}
          >
            {pattern.recommended_action.toUpperCase().replace('_', ' ')}
          </span>
        </div>
      )}

      {/* Conditions */}
      {Object.keys(pattern.conditions).length > 0 && (
        <div className="mb-2">
          <div className="text-xs text-neutral mb-1">Conditions:</div>
          <div className="flex flex-wrap gap-1">
            {Object.entries(pattern.conditions).slice(0, 4).map(([key, value]) => (
              <span
                key={key}
                className="text-[10px] px-1.5 py-0.5 rounded bg-surface text-neutral font-mono-numbers"
              >
                {key}: {typeof value === 'object' ? JSON.stringify(value) : String(value)}
              </span>
            ))}
            {Object.keys(pattern.conditions).length > 4 && (
              <span className="text-[10px] px-1.5 py-0.5 rounded bg-surface text-neutral">
                +{Object.keys(pattern.conditions).length - 4} more
              </span>
            )}
          </div>
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-3 gap-2 pt-2 border-t border-white/5">
        <div className="text-center">
          <div className="text-[10px] text-neutral">Confidence</div>
          <ConfidenceBar confidence={pattern.confidence} />
        </div>
        <div className="text-center">
          <div className="text-[10px] text-neutral">Win Rate</div>
          <div
            className={clsx(
              'font-mono-numbers text-sm font-medium',
              pattern.success_rate >= 0.5 ? 'text-profit' : 'text-loss'
            )}
          >
            {Math.round(pattern.success_rate * 100)}%
          </div>
        </div>
        <div className="text-center">
          <div className="text-[10px] text-neutral">Samples</div>
          <div className="font-mono-numbers text-sm text-white">{pattern.sample_size}</div>
        </div>
      </div>
    </div>
  );
}

export default function LearnedPatternsPanel({ agentId, className }: LearnedPatternsPanelProps) {
  const { agentPatterns, loading, setLoading, updateAgentPatterns } = useLearningStore();

  const patterns = agentPatterns[agentId] || [];
  const isLoading = loading.patterns[agentId];

  useEffect(() => {
    async function loadData() {
      if (patterns.length > 0) return; // Already loaded
      setLoading('patterns', agentId, true);
      const data = await fetchAgentPatterns(agentId, 0.3);
      updateAgentPatterns(agentId, data);
      setLoading('patterns', agentId, false);
    }
    loadData();
  }, [agentId, patterns.length, setLoading, updateAgentPatterns]);

  if (isLoading) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
          Learned Patterns
        </h3>
        <div className="flex items-center justify-center py-6">
          <div className="w-2 h-2 bg-accent rounded-full animate-ping mr-2" />
          <span className="text-neutral text-sm">Loading patterns...</span>
        </div>
      </div>
    );
  }

  if (patterns.length === 0) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
          Learned Patterns
        </h3>
        <div className="text-center text-neutral py-6 text-sm">
          <div className="w-10 h-10 mx-auto mb-2 rounded-full bg-surface flex items-center justify-center">
            <span className="text-xl">📊</span>
          </div>
          No patterns learned yet
          <p className="text-xs mt-1 text-neutral/60">
            Patterns will be discovered as the agent trades
          </p>
        </div>
      </div>
    );
  }

  // Group patterns by type
  const patternsByType = patterns.reduce((acc, p) => {
    if (!acc[p.pattern_type]) acc[p.pattern_type] = [];
    acc[p.pattern_type].push(p);
    return acc;
  }, {} as Record<PatternType, LearnedPattern[]>);

  // Count high confidence patterns
  const highConfidenceCount = patterns.filter((p) => p.confidence >= 0.7).length;
  const activeCount = patterns.filter((p) => p.is_active).length;

  return (
    <div className={clsx('glass-strong rounded-xl p-4', className)}>
      <h3 className="text-sm font-semibold mb-3 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
        Learned Patterns
        <span className="text-xs text-neutral ml-auto">
          {activeCount} active / {patterns.length} total
        </span>
      </h3>

      {/* Summary */}
      <div className="flex flex-wrap gap-2 mb-3">
        {highConfidenceCount > 0 && (
          <span className="text-xs px-2 py-1 rounded bg-profit/10 text-profit">
            {highConfidenceCount} high confidence
          </span>
        )}
        {Object.entries(patternsByType).map(([type, typePatterns]) => (
          <span
            key={type}
            className={clsx(
              'text-xs px-2 py-1 rounded',
              patternTypeConfig[type as PatternType]?.bg || 'bg-neutral/20',
              patternTypeConfig[type as PatternType]?.color || 'text-neutral'
            )}
          >
            {typePatterns.length} {type.replace('_', ' ')}
          </span>
        ))}
      </div>

      {/* Patterns list */}
      <div className="space-y-3 max-h-[400px] overflow-y-auto pr-1">
        {patterns
          .sort((a, b) => b.confidence - a.confidence)
          .map((pattern, index) => (
            <PatternCard key={pattern.id || index} pattern={pattern} />
          ))}
      </div>
    </div>
  );
}
