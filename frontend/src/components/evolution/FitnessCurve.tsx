import { useState, useEffect } from 'react';
import {
  Line,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  ComposedChart,
} from 'recharts';
import type { EvolutionRunDetail } from '../../types/evolution';

interface Props {
  runId: string | null;
}

export default function FitnessCurve({ runId }: Props) {
  const [detail, setDetail] = useState<EvolutionRunDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!runId) {
      setDetail(null);
      return;
    }

    let cancelled = false;
    const fetchDetail = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await fetch(`/api/evolution/${runId}`);
        if (!response.ok) {
          const data = await response.json();
          throw new Error(data.detail || 'Failed to fetch run');
        }
        const data = await response.json();
        if (!cancelled) setDetail(data);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    fetchDetail();

    // Poll if running
    const interval = setInterval(() => {
      if (detail?.status === 'running') fetchDetail();
    }, 5000);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [runId]);

  if (!runId) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        Select a run to view the fitness curve
      </div>
    );
  }

  if (loading && !detail) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center">
        <div className="animate-spin w-8 h-8 border-2 border-accent border-t-transparent rounded-full mx-auto mb-4" />
        <p className="text-neutral">Loading run data...</p>
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

  if (!detail) return null;

  const generations = detail.generations || [];
  const hasData = generations.length > 0;

  // Compute improvement from gen 0 to best
  const firstGen = generations[0];
  const lastGen = generations[generations.length - 1];
  const improvement = firstGen && lastGen
    ? lastGen.best_fitness - firstGen.best_fitness
    : 0;

  return (
    <div className="space-y-4">
      {/* Header with run info */}
      <div className="glass-strong rounded-xl p-5">
        <div className="flex items-center justify-between mb-1">
          <h2 className="text-lg font-semibold text-white">{detail.name}</h2>
          <span className="text-xs text-neutral font-mono">{detail.run_id.slice(0, 8)}...</span>
        </div>
        <div className="flex flex-wrap gap-4 text-sm text-neutral">
          <span>{detail.symbols.join(', ')}</span>
          <span>{detail.backtest_start} to {detail.backtest_end}</span>
          <span>{detail.tick_interval} ticks</span>
          <span>{detail.agent_class.split('.').pop()}</span>
        </div>
      </div>

      {/* Stat cards row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="glass-strong rounded-xl p-4">
          <div className="text-xs text-neutral mb-1">Best Fitness</div>
          <div className="text-2xl font-bold font-mono-numbers text-white">
            {detail.best_fitness != null ? detail.best_fitness.toFixed(4) : '-'}
          </div>
        </div>
        <div className="glass-strong rounded-xl p-4">
          <div className="text-xs text-neutral mb-1">Improvement</div>
          <div className={`text-2xl font-bold font-mono-numbers ${improvement >= 0 ? 'text-profit' : 'text-loss'}`}>
            {improvement >= 0 ? '+' : ''}{improvement.toFixed(4)}
          </div>
        </div>
        <div className="glass-strong rounded-xl p-4">
          <div className="text-xs text-neutral mb-1">Generations</div>
          <div className="text-2xl font-bold font-mono-numbers text-white">
            {detail.current_generation} / {detail.max_generations}
          </div>
        </div>
        <div className="glass-strong rounded-xl p-4">
          <div className="text-xs text-neutral mb-1">Population</div>
          <div className="text-2xl font-bold font-mono-numbers text-white">
            {detail.population_size}
          </div>
        </div>
      </div>

      {/* Fitness chart */}
      <div className="glass-strong rounded-xl p-6">
        <h3 className="text-lg font-semibold text-white mb-4">Fitness over Generations</h3>
        {!hasData ? (
          <div className="h-64 flex items-center justify-center text-neutral">
            No generation data yet
          </div>
        ) : (
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={generations} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
                <XAxis
                  dataKey="generation"
                  stroke="#8888aa"
                  fontSize={11}
                  tickLine={false}
                  axisLine={false}
                  label={{ value: 'Generation', position: 'insideBottom', offset: -5, fill: '#8888aa', fontSize: 11 }}
                />
                <YAxis
                  stroke="#8888aa"
                  fontSize={11}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(v) => v.toFixed(2)}
                  domain={['auto', 'auto']}
                  width={50}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'rgba(18, 18, 26, 0.95)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    borderRadius: '8px',
                    fontSize: '12px',
                    backdropFilter: 'blur(10px)',
                  }}
                  labelFormatter={(gen) => `Generation ${gen}`}
                  formatter={(value: number, name: string) => {
                    const labels: Record<string, string> = {
                      best_fitness: 'Best',
                      avg_fitness: 'Average',
                      worst_fitness: 'Worst',
                    };
                    return [value.toFixed(4), labels[name] || name];
                  }}
                />
                <Legend
                  wrapperStyle={{ fontSize: '11px', paddingTop: '10px' }}
                  formatter={(value) => {
                    const labels: Record<string, string> = {
                      best_fitness: 'Best',
                      avg_fitness: 'Average',
                      worst_fitness: 'Worst',
                    };
                    return labels[value] || value;
                  }}
                />

                {/* Population band between best and worst */}
                <Area
                  dataKey="best_fitness"
                  stroke="none"
                  fill="url(#populationBand)"
                  fillOpacity={1}
                />
                <Area
                  dataKey="worst_fitness"
                  stroke="none"
                  fill="rgba(18, 18, 26, 1)"
                  fillOpacity={1}
                />

                {/* Reference line at 0.5 */}
                <ReferenceLine y={0.5} stroke="#444" strokeDasharray="3 3" />

                {/* Worst fitness - thin dim line */}
                <Line
                  type="monotone"
                  dataKey="worst_fitness"
                  stroke="#666"
                  strokeWidth={1}
                  dot={false}
                  activeDot={{ r: 3, fill: '#666' }}
                />

                {/* Average fitness - dashed cyan */}
                <Line
                  type="monotone"
                  dataKey="avg_fitness"
                  stroke="#22d3ee"
                  strokeWidth={2}
                  strokeDasharray="5 3"
                  dot={false}
                  activeDot={{ r: 4, fill: '#22d3ee' }}
                />

                {/* Best fitness - bold green */}
                <Line
                  type="monotone"
                  dataKey="best_fitness"
                  stroke="#00ff88"
                  strokeWidth={3}
                  dot={{ r: 3, fill: '#00ff88', strokeWidth: 0 }}
                  activeDot={{ r: 5, fill: '#00ff88', strokeWidth: 0 }}
                />

                <defs>
                  <linearGradient id="populationBand" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#00ff88" stopOpacity={0.1} />
                    <stop offset="100%" stopColor="#00ff88" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      {/* Fitness weights */}
      {detail.fitness_weights && Object.keys(detail.fitness_weights).length > 0 && (
        <div className="glass-strong rounded-xl p-5">
          <h3 className="text-sm font-medium text-neutral mb-3">Fitness Weights</h3>
          <div className="flex flex-wrap gap-3">
            {Object.entries(detail.fitness_weights).map(([key, weight]) => (
              <div key={key} className="flex items-center gap-2 px-3 py-1.5 bg-white/5 rounded-lg text-sm">
                <span className="text-white">{key.replace(/_/g, ' ')}</span>
                <span className="font-mono-numbers text-accent">{(weight * 100).toFixed(0)}%</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
