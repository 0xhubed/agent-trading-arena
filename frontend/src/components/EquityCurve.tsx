import { useMemo, useState } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { useCompetitionStore } from '../stores/competition';
import { InfoTooltip, GLOSSARY } from './InfoTooltip';
import clsx from 'clsx';

// Extended color palette for agents - 16 distinct colors
const COLORS = [
  '#00ff88', // profit green
  '#6366f1', // accent indigo
  '#22d3ee', // highlight cyan
  '#ff4466', // loss red
  '#f59e0b', // amber
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#14b8a6', // teal
  '#84cc16', // lime
  '#f97316', // orange
  '#06b6d4', // cyan
  '#a855f7', // purple
  '#eab308', // yellow
  '#ef4444', // red
  '#3b82f6', // blue
  '#10b981', // emerald
];

export default function EquityCurve() {
  const { equityHistory, agents } = useCompetitionStore();
  const [selectedAgents, setSelectedAgents] = useState<Set<string>>(new Set());
  const [isFullscreen, setIsFullscreen] = useState(false);

  // Transform history data for recharts, sorted by timestamp
  const chartData = useMemo(() => {
    // First sort by timestamp to ensure chronological order
    const sortedHistory = [...equityHistory].sort((a, b) => {
      const timeA = a.timestamp ? new Date(a.timestamp).getTime() : 0;
      const timeB = b.timestamp ? new Date(b.timestamp).getTime() : 0;
      return timeA - timeB;
    });

    return sortedHistory.map((point) => {
      const timestamp = point.timestamp ? new Date(point.timestamp) : new Date();
      const dataPoint: Record<string, number | string | Date> = {
        tick: point.tick,
        timestamp: timestamp.getTime(), // Use timestamp for X-axis
        displayTime: timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        displayDate: timestamp.toLocaleDateString([], { month: 'short', day: 'numeric' }),
        fullTimestamp: timestamp.toLocaleString(),
      };

      point.leaderboard.forEach((entry) => {
        dataPoint[entry.agent_id] = entry.equity;
      });

      return dataPoint;
    });
  }, [equityHistory]);

  // Get unique agent IDs from history, sorted by name for consistent ordering
  const agentIds = useMemo(() => {
    if (equityHistory.length === 0) return [];
    const ids = new Set<string>();
    equityHistory.forEach((point) => {
      point.leaderboard.forEach((entry) => {
        ids.add(entry.agent_id);
      });
    });
    // Sort by agent name for consistent color assignment and display order
    return Array.from(ids).sort((a, b) => {
      const nameA = agents.find((agent) => agent.id === a)?.name || a;
      const nameB = agents.find((agent) => agent.id === b)?.name || b;
      return nameA.localeCompare(nameB);
    });
  }, [equityHistory, agents]);

  // Agents to display (all if none selected, otherwise only selected)
  const displayedAgentIds = useMemo(() => {
    if (selectedAgents.size === 0) return agentIds;
    return agentIds.filter((id) => selectedAgents.has(id));
  }, [agentIds, selectedAgents]);

  // Calculate Y-axis domain with padding based on displayed agents
  const yDomain = useMemo(() => {
    if (equityHistory.length === 0) return [9000, 11000];

    let min = Infinity;
    let max = -Infinity;

    equityHistory.forEach((point) => {
      point.leaderboard.forEach((entry) => {
        if (displayedAgentIds.includes(entry.agent_id)) {
          if (entry.equity < min) min = entry.equity;
          if (entry.equity > max) max = entry.equity;
        }
      });
    });

    // If no valid data, return default range
    if (min === Infinity || max === -Infinity) return [9000, 11000];

    // Add 5% padding on each side
    const range = max - min;
    const padding = Math.max(range * 0.05, 100); // At least $100 padding

    return [Math.floor(min - padding), Math.ceil(max + padding)];
  }, [equityHistory, displayedAgentIds]);

  const getAgentName = (agentId: string) => {
    return agents.find((a) => a.id === agentId)?.name || agentId;
  };

  const getAgentColor = (agentId: string) => {
    const index = agentIds.indexOf(agentId);
    return COLORS[index % COLORS.length];
  };

  const toggleAgent = (agentId: string) => {
    setSelectedAgents((prev) => {
      const next = new Set(prev);
      if (next.has(agentId)) {
        next.delete(agentId);
      } else {
        next.add(agentId);
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelectedAgents(new Set());
  };

  const selectNone = () => {
    setSelectedAgents(new Set(agentIds));
  };

  const chartContent = (
    <>
      {chartData.length === 0 ? (
        <div className={clsx(
          'flex items-center justify-center text-neutral',
          isFullscreen ? 'h-[80vh]' : 'h-48 sm:h-64'
        )}>
          <div className="text-center">
            <div className="w-12 h-12 mx-auto mb-3 rounded-full bg-surface flex items-center justify-center">
              <span className="text-2xl">📈</span>
            </div>
            Waiting for data...
          </div>
        </div>
      ) : (
        <div className={clsx(isFullscreen ? 'h-[70vh]' : 'h-48 sm:h-64')}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={chartData}
              margin={{ top: 5, right: 10, left: 0, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis
                dataKey="timestamp"
                stroke="#8888aa"
                fontSize={10}
                tickLine={false}
                axisLine={false}
                type="number"
                domain={['dataMin', 'dataMax']}
                tickFormatter={(value) => {
                  const date = new Date(value);
                  // Check if data spans multiple days
                  const firstTimestamp = chartData[0]?.timestamp as number;
                  const lastTimestamp = chartData[chartData.length - 1]?.timestamp as number;
                  const spansDays = firstTimestamp && lastTimestamp &&
                    (new Date(lastTimestamp).getDate() !== new Date(firstTimestamp).getDate());

                  if (spansDays) {
                    // Show date and time
                    return date.toLocaleDateString([], { month: 'short', day: 'numeric' }) +
                           ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                  }
                  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                }}
              />
              <YAxis
                stroke="#8888aa"
                fontSize={11}
                tickLine={false}
                axisLine={false}
                tickFormatter={(value) => `$${(value / 1000).toFixed(1)}k`}
                domain={yDomain}
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
                labelStyle={{ color: '#8888aa' }}
                formatter={(value: number, name: string) => [
                  `$${value.toLocaleString(undefined, {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                  })}`,
                  getAgentName(name),
                ]}
                labelFormatter={(value) => {
                  const date = new Date(value);
                  return date.toLocaleString([], {
                    month: 'short',
                    day: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit',
                  });
                }}
              />
              {!isFullscreen && (
                <Legend
                  wrapperStyle={{ fontSize: '11px', paddingTop: '10px' }}
                  formatter={(value) => (
                    <span className="text-neutral">{getAgentName(value)}</span>
                  )}
                />
              )}
              {displayedAgentIds.map((agentId) => (
                <Line
                  key={agentId}
                  type="monotone"
                  dataKey={agentId}
                  name={agentId}
                  stroke={getAgentColor(agentId)}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{
                    r: 4,
                    strokeWidth: 0,
                    fill: getAgentColor(agentId),
                  }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </>
  );

  // Fullscreen modal
  if (isFullscreen) {
    return (
      <div className="fixed inset-0 z-50 bg-black/95 flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-white/10">
          <h2 className="text-xl font-semibold text-white flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-profit"></span>
            Equity Curves
          </h2>
          <button
            onClick={() => setIsFullscreen(false)}
            className="p-2 rounded-lg bg-surface/50 hover:bg-surface text-neutral hover:text-white transition-colors"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Agent selector */}
        <div className="p-4 border-b border-white/10">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-sm text-neutral">Select agents to compare:</span>
            <button
              onClick={selectAll}
              className="px-2 py-1 text-xs rounded bg-surface/50 text-neutral hover:text-white transition-colors"
            >
              All
            </button>
            <button
              onClick={selectNone}
              className="px-2 py-1 text-xs rounded bg-surface/50 text-neutral hover:text-white transition-colors"
            >
              None
            </button>
          </div>
          <div className="flex flex-wrap gap-2">
            {agentIds.map((agentId) => {
              const isSelected = selectedAgents.size === 0 || selectedAgents.has(agentId);
              const color = getAgentColor(agentId);
              return (
                <button
                  key={agentId}
                  onClick={() => toggleAgent(agentId)}
                  className={clsx(
                    'px-3 py-1.5 rounded-lg text-xs font-medium transition-all flex items-center gap-2',
                    isSelected
                      ? 'bg-surface text-white'
                      : 'bg-surface/30 text-neutral/50'
                  )}
                  style={{
                    borderLeft: `3px solid ${isSelected ? color : 'transparent'}`,
                  }}
                >
                  <span
                    className="w-2 h-2 rounded-full"
                    style={{ backgroundColor: color }}
                  />
                  {getAgentName(agentId)}
                </button>
              );
            })}
          </div>
        </div>

        {/* Chart */}
        <div className="flex-1 p-4">
          {chartContent}
        </div>
      </div>
    );
  }

  return (
    <div className="glass-strong rounded-xl p-4 sm:p-6">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-white flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-profit"></span>
          Equity Curves
          <InfoTooltip content={GLOSSARY.equityCurve} position="right" />
        </h2>
        <div className="flex items-center gap-2">
          <button
            onClick={selectAll}
            className="px-2 py-1 text-xs rounded bg-surface/50 text-neutral hover:text-white transition-colors"
            title="Show all agents"
          >
            All
          </button>
          <button
            onClick={selectNone}
            className="px-2 py-1 text-xs rounded bg-surface/50 text-neutral hover:text-white transition-colors"
            title="Hide all agents"
          >
            None
          </button>
          <button
            onClick={() => setIsFullscreen(true)}
            className="p-2 rounded-lg bg-surface/50 hover:bg-surface text-neutral hover:text-white transition-colors"
            title="Fullscreen"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
            </svg>
          </button>
        </div>
      </div>

      {/* Compact agent selector */}
      {agentIds.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mb-3 max-h-16 overflow-y-auto">
          {agentIds.map((agentId) => {
            const isSelected = selectedAgents.size === 0 || selectedAgents.has(agentId);
            const color = getAgentColor(agentId);
            return (
              <button
                key={agentId}
                onClick={() => toggleAgent(agentId)}
                className={clsx(
                  'px-2 py-0.5 rounded text-xs transition-all flex items-center gap-1.5',
                  isSelected
                    ? 'bg-surface/80 text-white'
                    : 'bg-surface/30 text-neutral/50'
                )}
                title={getAgentName(agentId)}
              >
                <span
                  className="w-1.5 h-1.5 rounded-full flex-shrink-0"
                  style={{ backgroundColor: color }}
                />
                <span className="truncate max-w-[100px]">{getAgentName(agentId)}</span>
              </button>
            );
          })}
        </div>
      )}

      {chartContent}
    </div>
  );
}
