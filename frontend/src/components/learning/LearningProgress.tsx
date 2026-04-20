/**
 * LearningProgress component - displays learning stats and learning curve chart.
 */

import { useEffect, useMemo } from 'react';
import clsx from 'clsx';
import {
  useLearningStore,
  fetchAgentLearning,
} from '../../stores/learning';
import type { LearningDataPoint } from '../../types/learning';

interface LearningProgressProps {
  agentId: string;
  className?: string;
}

function StatCard({
  label,
  value,
  trend,
  highlight = false,
}: {
  label: string;
  value: string | number;
  trend?: 'up' | 'down' | 'stable';
  highlight?: boolean;
}) {
  return (
    <div
      className={clsx(
        'glass rounded-xl p-3 text-center',
        highlight && 'ring-1 ring-accent/30'
      )}
    >
      <div className="text-xs text-neutral mb-1">{label}</div>
      <div className="flex items-center justify-center gap-1">
        <span
          className={clsx(
            'font-mono-numbers font-bold text-lg',
            highlight ? 'text-accent' : 'text-white'
          )}
        >
          {value}
        </span>
        {trend && (
          <span
            className={clsx(
              'text-xs',
              trend === 'up' ? 'text-profit' : trend === 'down' ? 'text-loss' : 'text-neutral'
            )}
          >
            {trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→'}
          </span>
        )}
      </div>
    </div>
  );
}

function MiniChart({
  data,
  width = 200,
  height = 60,
  dataKey,
  color = '#8B5CF6',
}: {
  data: LearningDataPoint[];
  width?: number;
  height?: number;
  dataKey: keyof LearningDataPoint;
  color?: string;
}) {
  if (!data || data.length < 2) {
    return (
      <div
        className="flex items-center justify-center text-xs text-neutral"
        style={{ width, height }}
      >
        Not enough data
      </div>
    );
  }

  const values = data.map((d) => d[dataKey] as number);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const points = data.map((d, i) => {
    const x = (i / (data.length - 1)) * width;
    const y = height - ((d[dataKey] as number - min) / range) * height * 0.8 - height * 0.1;
    return `${x},${y}`;
  });

  const pathD = `M ${points.join(' L ')}`;

  // Area fill
  const areaD = `${pathD} L ${width},${height} L 0,${height} Z`;

  return (
    <svg width={width} height={height} className="overflow-visible">
      <defs>
        <linearGradient id={`gradient-${dataKey}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={areaD} fill={`url(#gradient-${dataKey})`} />
      <path d={pathD} fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" />
      {/* Latest point highlight */}
      <circle
        cx={width}
        cy={
          height -
          ((values[values.length - 1] - min) / range) * height * 0.8 -
          height * 0.1
        }
        r="4"
        fill={color}
        className="animate-pulse"
      />
    </svg>
  );
}

export default function LearningProgress({ agentId, className }: LearningProgressProps) {
  const { agentStats, agentLearningCurves, loading, setLoading, setAgentLearningData } =
    useLearningStore();

  const stats = agentStats[agentId];
  const learningCurve = agentLearningCurves[agentId] || [];
  const isLoading = loading.stats[agentId];

  useEffect(() => {
    async function loadData() {
      if (stats) return; // Already loaded
      setLoading('stats', agentId, true);
      const data = await fetchAgentLearning(agentId);
      if (data) {
        setAgentLearningData(data);
      }
      setLoading('stats', agentId, false);
    }
    loadData();
  }, [agentId, stats, setLoading, setAgentLearningData]);

  const trend = useMemo(() => {
    if (learningCurve.length < 5) return 'stable';
    const recent = learningCurve.slice(-5);
    const older = learningCurve.slice(-10, -5);
    if (older.length === 0) return 'stable';

    const recentAvg = recent.reduce((a, b) => a + b.win_rate, 0) / recent.length;
    const olderAvg = older.reduce((a, b) => a + b.win_rate, 0) / older.length;

    if (recentAvg > olderAvg + 0.05) return 'up';
    if (recentAvg < olderAvg - 0.05) return 'down';
    return 'stable';
  }, [learningCurve]);

  if (isLoading) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <div className="flex items-center justify-center py-8">
          <div className="w-2 h-2 bg-accent rounded-full animate-ping mr-2" />
          <span className="text-neutral">Loading learning data...</span>
        </div>
      </div>
    );
  }

  if (!stats) {
    return (
      <div className={clsx('glass-strong rounded-xl p-4', className)}>
        <div className="text-center text-neutral py-8">
          <div className="w-12 h-12 mx-auto mb-3 rounded-full bg-surface flex items-center justify-center">
            <span className="text-2xl">📚</span>
          </div>
          No learning data available yet
        </div>
      </div>
    );
  }

  return (
    <div className={clsx('glass-strong rounded-xl p-4', className)}>
      <h3 className="text-sm font-semibold mb-4 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
        Learning Progress
        {trend !== 'stable' && (
          <span
            className={clsx(
              'text-xs px-2 py-0.5 rounded-full',
              trend === 'up' ? 'bg-profit/20 text-profit' : 'bg-loss/20 text-loss'
            )}
          >
            {trend === 'up' ? 'Improving' : 'Needs Review'}
          </span>
        )}
      </h3>

      {/* Stats grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
        <StatCard
          label="Patterns Learned"
          value={stats.patterns_learned}
          highlight={stats.patterns_learned >= 5}
        />
        <StatCard
          label="Situations Referenced"
          value={stats.situations_referenced}
        />
        <StatCard
          label="Reflections Made"
          value={stats.reflections_made}
        />
        <StatCard
          label="vs Baseline"
          value={`${stats.improvement_vs_baseline >= 0 ? '+' : ''}${stats.improvement_vs_baseline.toFixed(1)}%`}
          trend={stats.improvement_vs_baseline > 0 ? 'up' : stats.improvement_vs_baseline < 0 ? 'down' : 'stable'}
          highlight={stats.improvement_vs_baseline > 0}
        />
      </div>

      {/* Learning curve charts */}
      {learningCurve.length > 1 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div className="glass rounded-lg p-3">
            <div className="text-xs text-neutral mb-2">Win Rate Over Time</div>
            <MiniChart
              data={learningCurve}
              dataKey="win_rate"
              width={180}
              height={50}
              color="#22C55E"
            />
            <div className="text-xs text-white font-mono-numbers mt-1">
              Current: {((learningCurve[learningCurve.length - 1]?.win_rate ?? 0) * 100).toFixed(0)}%
            </div>
          </div>
          <div className="glass rounded-lg p-3">
            <div className="text-xs text-neutral mb-2">Sharpe Ratio Over Time</div>
            <MiniChart
              data={learningCurve}
              dataKey="sharpe_ratio"
              width={180}
              height={50}
              color="#8B5CF6"
            />
            <div className="text-xs text-white font-mono-numbers mt-1">
              Current: {(learningCurve[learningCurve.length - 1]?.sharpe_ratio ?? 0).toFixed(2)}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
