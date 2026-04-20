import { useState, useEffect } from 'react';
import type { GenomeEntry } from '../../types/evolution';

interface Props {
  runId: string | null;
  totalGenerations: number;
}

// Numeric parameters to track convergence
const TRACKED_PARAMS: { key: string; label: string; min: number; max: number }[] = [
  { key: 'temperature', label: 'Temperature', min: 0.1, max: 1.0 },
  { key: 'confidence_threshold', label: 'Confidence', min: 0.3, max: 0.9 },
  { key: 'position_size_pct', label: 'Position Size', min: 0.05, max: 0.25 },
  { key: 'sl_pct', label: 'Stop Loss', min: 0.01, max: 0.05 },
  { key: 'tp_pct', label: 'Take Profit', min: 0.02, max: 0.10 },
  { key: 'max_leverage', label: 'Max Leverage', min: 1, max: 10 },
];

interface GenDistribution {
  generation: number;
  params: Record<string, { mean: number; min: number; max: number; spread: number }>;
}

export default function PopulationHeatmap({ runId, totalGenerations }: Props) {
  const [genData, setGenData] = useState<GenDistribution[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!runId || totalGenerations <= 0) {
      setGenData([]);
      return;
    }

    let cancelled = false;
    const fetchAll = async () => {
      try {
        setLoading(true);
        setError(null);

        const distributions: GenDistribution[] = [];

        // Fetch each generation (cap at 50 to avoid too many requests)
        const maxGen = Math.min(totalGenerations, 50);
        for (let gen = 0; gen < maxGen; gen++) {
          const response = await fetch(`/api/evolution/${runId}/generations?generation=${gen}`);
          if (!response.ok) continue;
          const data = await response.json();
          const genomes: GenomeEntry[] = data.genomes || [];

          if (genomes.length === 0) continue;

          const paramDist: Record<string, { mean: number; min: number; max: number; spread: number }> = {};

          for (const param of TRACKED_PARAMS) {
            const values = genomes
              .map((g) => g.genome[param.key as keyof typeof g.genome] as number)
              .filter((v) => typeof v === 'number');

            if (values.length === 0) continue;

            const mean = values.reduce((a, b) => a + b, 0) / values.length;
            const min = Math.min(...values);
            const max = Math.max(...values);
            const range = param.max - param.min;
            const spread = range > 0 ? (max - min) / range : 0;

            paramDist[param.key] = { mean, min, max, spread };
          }

          distributions.push({ generation: gen, params: paramDist });
        }

        if (!cancelled) setGenData(distributions);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    fetchAll();
    return () => { cancelled = true; };
  }, [runId, totalGenerations]);

  if (!runId) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        Select a run to view population convergence
      </div>
    );
  }

  if (loading) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center">
        <div className="animate-spin w-8 h-8 border-2 border-accent border-t-transparent rounded-full mx-auto mb-4" />
        <p className="text-neutral">Loading population data...</p>
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

  if (genData.length === 0) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        No generation data available
      </div>
    );
  }

  return (
    <div className="glass-strong rounded-xl p-5">
      <h3 className="text-lg font-semibold text-white mb-2">Population Convergence</h3>
      <p className="text-xs text-neutral mb-4">
        Shows how parameter distributions narrow as the population converges.
        Wider bars = more diversity, narrower = convergence.
      </p>

      <div className="space-y-6">
        {TRACKED_PARAMS.map((param) => (
          <div key={param.key}>
            <div className="text-sm text-neutral mb-2">{param.label}</div>
            <div className="flex items-center gap-1">
              {genData.map((gen) => {
                const dist = gen.params[param.key];
                if (!dist) {
                  return (
                    <div
                      key={gen.generation}
                      className="flex-1 h-8 bg-white/5 rounded-sm"
                      title={`Gen ${gen.generation}: no data`}
                    />
                  );
                }

                const range = param.max - param.min;
                // Position of mean within the full range (0-100%)
                const meanPct = ((dist.mean - param.min) / range) * 100;
                // Width of the population spread (0-100%)
                const spreadPct = dist.spread * 100;
                // Color intensity: more converged = brighter
                const convergence = 1 - dist.spread;
                const opacity = 0.2 + convergence * 0.8;

                return (
                  <div
                    key={gen.generation}
                    className="flex-1 h-8 relative bg-white/5 rounded-sm overflow-hidden"
                    title={`Gen ${gen.generation}: mean=${dist.mean.toFixed(3)}, spread=${(dist.spread * 100).toFixed(1)}%`}
                  >
                    {/* Population range bar */}
                    <div
                      className="absolute inset-y-1 rounded-sm"
                      style={{
                        left: `${Math.max(0, ((dist.min - param.min) / range) * 100)}%`,
                        width: `${Math.min(100, spreadPct)}%`,
                        backgroundColor: `rgba(0, 255, 136, ${opacity})`,
                      }}
                    />
                    {/* Mean indicator */}
                    <div
                      className="absolute top-0 bottom-0 w-px bg-white"
                      style={{
                        left: `${Math.min(100, Math.max(0, meanPct))}%`,
                        opacity: 0.6,
                      }}
                    />
                  </div>
                );
              })}
            </div>
            <div className="flex justify-between text-xs text-neutral/50 mt-0.5">
              <span>Gen 0</span>
              <span>Gen {genData.length - 1}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Legend */}
      <div className="mt-4 pt-4 border-t border-white/5 flex items-center gap-4 text-xs text-neutral">
        <div className="flex items-center gap-2">
          <div className="w-8 h-3 rounded-sm" style={{ backgroundColor: 'rgba(0, 255, 136, 0.3)' }} />
          <span>Diverse</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: 'rgba(0, 255, 136, 0.9)' }} />
          <span>Converged</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-px h-3 bg-white/60" />
          <span>Mean</span>
        </div>
      </div>
    </div>
  );
}
