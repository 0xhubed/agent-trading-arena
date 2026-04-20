import { useEffect, useState } from 'react';
import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';
import type { DiversityEntry } from '../../types/evolution';

interface Props {
  runId: string | null;
}

const LOW_DIVERSITY_THRESHOLD = 0.01;

export default function DiversityMonitor({ runId }: Props) {
  const [data, setData] = useState<DiversityEntry[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!runId) return;
    setLoading(true);
    fetch(`/api/evolution/${runId}/diversity`)
      .then((r) => r.json())
      .then((d) => setData(d.diversity || []))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [runId]);

  if (!runId) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        Select a run to view diversity
      </div>
    );
  }

  const latest = data.length > 0 ? data[data.length - 1] : null;
  const isLowDiversity = latest ? latest.total_variance < LOW_DIVERSITY_THRESHOLD : false;

  return (
    <div className="glass-strong rounded-xl p-4 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-white">Diversity Monitor</h2>
        {isLowDiversity && (
          <span className="px-2 py-1 rounded text-xs font-medium bg-yellow-500/20 text-yellow-400">
            Low Diversity Warning
          </span>
        )}
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3">
        <div className="glass rounded-lg p-3 text-center">
          <div className="text-xs text-neutral mb-1">Total Variance</div>
          <div className="text-lg font-bold text-white">
            {latest?.total_variance.toFixed(4) ?? '—'}
          </div>
        </div>
        <div className="glass rounded-lg p-3 text-center">
          <div className="text-xs text-neutral mb-1">Unique Strategies</div>
          <div className="text-lg font-bold text-white">
            {latest?.unique_strategies ?? '—'}
          </div>
        </div>
        <div className="glass rounded-lg p-3 text-center">
          <div className="text-xs text-neutral mb-1">Health</div>
          <div className={`text-lg font-bold ${isLowDiversity ? 'text-yellow-400' : 'text-green-400'}`}>
            {isLowDiversity ? 'Low' : 'Healthy'}
          </div>
        </div>
      </div>

      {/* Chart */}
      {loading ? (
        <div className="h-64 flex items-center justify-center text-neutral">Loading...</div>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <ComposedChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis
              dataKey="generation"
              stroke="#6b7280"
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              label={{ value: 'Generation', position: 'bottom', fill: '#9ca3af', fontSize: 12 }}
            />
            <YAxis
              yAxisId="variance"
              stroke="#6b7280"
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              label={{ value: 'Variance', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 12 }}
            />
            <YAxis
              yAxisId="strategies"
              orientation="right"
              stroke="#6b7280"
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              label={{ value: 'Strategies', angle: 90, position: 'insideRight', fill: '#9ca3af', fontSize: 12 }}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#111827',
                border: '1px solid #374151',
                borderRadius: '8px',
                color: '#e5e7eb',
              }}
            />
            <ReferenceLine
              yAxisId="variance"
              y={LOW_DIVERSITY_THRESHOLD}
              stroke="#ef4444"
              strokeDasharray="5 5"
              label={{ value: 'Low threshold', fill: '#ef4444', fontSize: 10 }}
            />
            <Area
              yAxisId="variance"
              type="monotone"
              dataKey="total_variance"
              name="Parameter Variance"
              fill="#6366f1"
              fillOpacity={0.3}
              stroke="#6366f1"
            />
            <Line
              yAxisId="strategies"
              type="monotone"
              dataKey="unique_strategies"
              name="Unique Strategies"
              stroke="#22c55e"
              strokeWidth={2}
              dot={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
