import { useState, useEffect } from 'react';
import clsx from 'clsx';
import type { BestGenome } from '../../types/evolution';

interface Props {
  runId: string | null;
}

// Parameter bounds matching AgentGenome.PARAM_BOUNDS
const PARAM_BOUNDS: Record<string, { min: number; max: number; label: string; format: (v: number) => string }> = {
  temperature: { min: 0.1, max: 1.0, label: 'Temperature', format: (v) => v.toFixed(2) },
  max_tokens: { min: 512, max: 4000, label: 'Max Tokens', format: (v) => v.toFixed(0) },
  confidence_threshold: { min: 0.3, max: 0.9, label: 'Confidence Threshold', format: (v) => v.toFixed(2) },
  position_size_pct: { min: 0.05, max: 0.25, label: 'Position Size %', format: (v) => (v * 100).toFixed(1) + '%' },
  sl_pct: { min: 0.01, max: 0.05, label: 'Stop Loss %', format: (v) => (v * 100).toFixed(1) + '%' },
  tp_pct: { min: 0.02, max: 0.10, label: 'Take Profit %', format: (v) => (v * 100).toFixed(1) + '%' },
  max_leverage: { min: 1, max: 10, label: 'Max Leverage', format: (v) => v.toFixed(0) + 'x' },
};

// Metric display config
const METRIC_CONFIG: Record<string, { label: string; format: (v: number) => string; goodWhen: 'high' | 'low' }> = {
  total_return: { label: 'Return', format: (v) => (v >= 0 ? '+' : '') + (v * 100).toFixed(2) + '%', goodWhen: 'high' },
  sharpe_ratio: { label: 'Sharpe', format: (v) => v.toFixed(2), goodWhen: 'high' },
  win_rate: { label: 'Win Rate', format: (v) => (v * 100).toFixed(1) + '%', goodWhen: 'high' },
  max_drawdown_pct: { label: 'Max Drawdown', format: (v) => (v * 100).toFixed(1) + '%', goodWhen: 'low' },
  total_trades: { label: 'Trades', format: (v) => v.toFixed(0), goodWhen: 'high' },
  profit_factor: { label: 'Profit Factor', format: (v) => v.toFixed(2), goodWhen: 'high' },
};

export default function GenomeInspector({ runId }: Props) {
  const [genome, setGenome] = useState<BestGenome | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!runId) {
      setGenome(null);
      return;
    }

    let cancelled = false;
    const fetchBest = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await fetch(`/api/evolution/${runId}/best`);
        if (!response.ok) {
          if (response.status === 404) {
            if (!cancelled) setGenome(null);
            return;
          }
          const data = await response.json();
          throw new Error(data.detail || 'Failed to fetch best genome');
        }
        const data = await response.json();
        if (!cancelled) setGenome(data);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    fetchBest();
    return () => { cancelled = true; };
  }, [runId]);

  if (!runId) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        Select a run to inspect its best genome
      </div>
    );
  }

  if (loading) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center">
        <div className="animate-spin w-8 h-8 border-2 border-accent border-t-transparent rounded-full mx-auto mb-4" />
        <p className="text-neutral">Loading genome...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="glass-strong rounded-xl p-4 border border-loss/50">
        <p className="text-loss">{error}</p>
      </div>
    );
  }

  if (!genome) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        No genomes with fitness scores yet
      </div>
    );
  }

  const g = genome.genome;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="glass-strong rounded-xl p-5">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-lg font-semibold text-white">Best Genome</h2>
          <div className="flex items-center gap-2">
            {genome.is_elite && (
              <span className="px-2 py-0.5 bg-yellow-400/20 text-yellow-400 text-xs rounded-full font-medium">
                Elite
              </span>
            )}
            <span className="text-xs text-neutral font-mono">{genome.genome_id.slice(0, 12)}</span>
          </div>
        </div>
        <div className="flex items-center gap-4 text-sm">
          <span className="text-neutral">Generation <span className="text-white font-mono-numbers">{genome.generation}</span></span>
          <span className="text-neutral">Fitness <span className="text-accent font-mono-numbers font-bold">{genome.fitness.toFixed(4)}</span></span>
          <span className="px-2 py-0.5 bg-white/5 rounded text-xs text-white">{g.model}</span>
        </div>
      </div>

      {/* Parameter bars */}
      <div className="glass-strong rounded-xl p-5">
        <h3 className="text-sm font-medium text-neutral mb-4">Parameters</h3>
        <div className="space-y-4">
          {Object.entries(PARAM_BOUNDS).map(([param, bounds]) => {
            const value = g[param as keyof typeof g] as number;
            const pct = ((value - bounds.min) / (bounds.max - bounds.min)) * 100;
            return (
              <div key={param}>
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-sm text-neutral">{bounds.label}</span>
                  <span className="text-sm font-mono-numbers text-white">{bounds.format(value)}</span>
                </div>
                <div className="relative h-2 bg-white/5 rounded-full overflow-hidden">
                  <div
                    className="absolute inset-y-0 left-0 bg-accent/60 rounded-full transition-all"
                    style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
                  />
                  {/* Position marker */}
                  <div
                    className="absolute top-1/2 -translate-y-1/2 w-3 h-3 bg-accent rounded-full border-2 border-black/50"
                    style={{ left: `calc(${Math.min(100, Math.max(0, pct))}% - 6px)` }}
                  />
                </div>
                <div className="flex justify-between text-xs text-neutral/50 mt-0.5">
                  <span>{bounds.format(bounds.min)}</span>
                  <span>{bounds.format(bounds.max)}</span>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Performance metrics */}
      {genome.metrics && Object.keys(genome.metrics).length > 0 && (
        <div className="glass-strong rounded-xl p-5">
          <h3 className="text-sm font-medium text-neutral mb-4">Performance Metrics</h3>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {Object.entries(genome.metrics).map(([key, value]) => {
              const config = METRIC_CONFIG[key];
              if (!config) {
                return (
                  <div key={key} className="bg-white/5 rounded-lg p-3">
                    <div className="text-xs text-neutral mb-1">{key.replace(/_/g, ' ')}</div>
                    <div className="font-mono-numbers text-white text-sm">{typeof value === 'number' ? value.toFixed(4) : value}</div>
                  </div>
                );
              }

              const isGood = config.goodWhen === 'high' ? value > 0 : value < 0.1;
              return (
                <div key={key} className="bg-white/5 rounded-lg p-3">
                  <div className="text-xs text-neutral mb-1">{config.label}</div>
                  <div className={clsx(
                    'font-mono-numbers text-sm font-medium',
                    isGood ? 'text-profit' : 'text-loss'
                  )}>
                    {config.format(value)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Lineage */}
      {(g.parent_ids.length > 0 || g.mutations.length > 0) && (
        <div className="glass-strong rounded-xl p-5">
          <h3 className="text-sm font-medium text-neutral mb-3">Lineage</h3>

          {g.parent_ids.length > 0 && (
            <div className="mb-3">
              <div className="text-xs text-neutral mb-1.5">Parents</div>
              <div className="flex flex-wrap gap-2">
                {g.parent_ids.map((pid) => (
                  <span key={pid} className="px-2 py-1 bg-white/5 rounded text-xs font-mono text-white">
                    {pid.slice(0, 12)}
                  </span>
                ))}
              </div>
            </div>
          )}

          {g.mutations.length > 0 && (
            <div>
              <div className="text-xs text-neutral mb-1.5">Mutations</div>
              <div className="space-y-1">
                {g.mutations.map((mut, i) => (
                  <div key={i} className="text-xs font-mono text-neutral bg-white/5 rounded px-2 py-1">
                    {mut}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Character preview */}
      {g.character && (
        <div className="glass-strong rounded-xl p-5">
          <h3 className="text-sm font-medium text-neutral mb-3">Character / Personality</h3>
          <blockquote className="border-l-2 border-accent/50 pl-4 text-sm text-white/80 italic leading-relaxed">
            {g.character}
          </blockquote>
        </div>
      )}
    </div>
  );
}
