import { useState, useEffect, useCallback } from 'react';
import { motion } from 'framer-motion';
import { useAdminStore } from '../../stores/admin';
import type {
  BacktestConfig as BacktestConfigType,
  CostEstimate,
  BacktestRunStatus,
} from '../../types/backtest';

interface Props {
  onRunComplete: (runId: string) => void;
}

interface RunningAgent {
  id: string;
  name: string;
  model: string;
  agent_type: string;
  agent_type_description: string;
  character: string;
}

interface BaselineAgent {
  id: string;
  name: string;
  class_path: string;
  description: string;
  config: Record<string, unknown>;
}

// Baseline agents for comparison (no API cost)
const BASELINE_AGENTS = [
  {
    id: 'random_baseline',
    name: 'Random Trader',
    class_path: 'agent_arena.agents.baselines.RandomAgent',
    description: 'Random trading at 20% frequency - statistical baseline',
    config: { trade_frequency: 0.2, position_size: 0.1 },
  },
  {
    id: 'sma_baseline',
    name: 'SMA Crossover',
    class_path: 'agent_arena.agents.baselines.SMAAgent',
    description: 'Simple moving average trend following',
    config: { sma_period: 50, position_size: 0.15, leverage: 3 },
  },
  {
    id: 'momentum_baseline',
    name: 'Momentum',
    class_path: 'agent_arena.agents.baselines.MomentumAgent',
    description: 'Long best performer, rebalances daily',
    config: { rebalance_ticks: 24, position_size: 0.1, long_only: true },
  },
  {
    id: 'buy_hold_baseline',
    name: 'Buy & Hold',
    class_path: 'agent_arena.agents.baselines.BuyAndHoldAgent',
    description: 'Passive benchmark - buy and hold BTC',
    config: { position_size: 0.5, leverage: 1 },
  },
  {
    id: 'mean_reversion_baseline',
    name: 'Mean Reversion',
    class_path: 'agent_arena.agents.baselines.MeanReversionAgent',
    description: 'RSI-based counter-trend strategy',
    config: { rsi_oversold: 30, rsi_overbought: 70, position_size: 0.1 },
  },
];

export default function BacktestConfig({ onRunComplete }: Props) {
  const { getHeaders } = useAdminStore();
  const [config, setConfig] = useState<BacktestConfigType>({
    start_date: getDefaultStartDate(),
    end_date: getDefaultEndDate(),
    symbols: ['PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD', 'PF_DOGEUSD', 'PF_XRPUSD'],
    tick_interval: '1h',
    candle_intervals: ['1h', '4h'],
    candle_limit: 100,
    agent_configs: [],
    run_baselines: true,
  });

  const [costEstimate, setCostEstimate] = useState<CostEstimate | null>(null);
  const [estimating, setEstimating] = useState(false);
  const [runStatus, setRunStatus] = useState<BacktestRunStatus | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runningAgents, setRunningAgents] = useState<RunningAgent[]>([]);
  const [selectedRunningAgents, setSelectedRunningAgents] = useState<Set<string>>(new Set());
  const [loadingAgents, setLoadingAgents] = useState(true);

  function getDefaultStartDate(): string {
    const date = new Date();
    date.setMonth(date.getMonth() - 1);
    return date.toISOString().split('T')[0];
  }

  function getDefaultEndDate(): string {
    const date = new Date();
    date.setDate(date.getDate() - 1);
    return date.toISOString().split('T')[0];
  }

  // Fetch running agents from current competition
  const fetchRunningAgents = useCallback(async () => {
    try {
      setLoadingAgents(true);
      const response = await fetch('/api/agents');
      if (response.ok) {
        const data = await response.json();
        setRunningAgents(data);
        // Select all agents by default
        setSelectedRunningAgents(new Set(data.map((a: RunningAgent) => a.id)));
      }
    } catch (err) {
      console.error('Failed to fetch running agents:', err);
    } finally {
      setLoadingAgents(false);
    }
  }, []);

  useEffect(() => {
    fetchRunningAgents();
  }, [fetchRunningAgents]);

  // Poll for run status when running
  useEffect(() => {
    if (!runStatus || runStatus.status !== 'running') return;

    const controller = new AbortController();
    const interval = setInterval(async () => {
      try {
        const response = await fetch(`/api/backtest/${runStatus.run_id}/status`, {
          signal: controller.signal,
        });
        if (response.ok) {
          const data = await response.json();
          setRunStatus(data);
          if (data.status === 'completed') {
            setRunning(false);
            onRunComplete(data.run_id);
          } else if (data.status === 'failed') {
            setRunning(false);
            setError(data.detail || data.error || 'Backtest failed');
          }
        }
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return;
        console.error('Failed to poll run status:', err);
      }
    }, 2000);

    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [runStatus?.run_id, runStatus?.status, onRunComplete]);

  const handleEstimate = async () => {
    try {
      setEstimating(true);
      setError(null);
      const fullConfig = getFullConfig();
      const response = await fetch('/api/backtest/estimate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getHeaders() },
        body: JSON.stringify(fullConfig),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || data.error || 'Failed to estimate cost');
      }

      const data = await response.json();
      setCostEstimate(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setEstimating(false);
    }
  };

  const handleStart = async () => {
    try {
      setRunning(true);
      setError(null);
      const fullConfig = getFullConfig();
      const response = await fetch('/api/backtest/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getHeaders() },
        body: JSON.stringify(fullConfig),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || data.error || 'Failed to start backtest');
      }

      const data = await response.json();
      setRunStatus({
        run_id: data.run_id,
        status: 'running',
        progress: 0,
        current_tick: 0,
        total_ticks: costEstimate?.total_ticks || 0,
        elapsed_seconds: 0,
        estimated_remaining_seconds: null,
        current_phase: 'Starting...',
      });
    } catch (err) {
      setRunning(false);
      setError(err instanceof Error ? err.message : 'Unknown error');
    }
  };

  const toggleAgent = (agent: BaselineAgent) => {
    setConfig((prev) => {
      const exists = prev.agent_configs.some((a) => a.agent_id === agent.id);
      if (exists) {
        return {
          ...prev,
          agent_configs: prev.agent_configs.filter((a) => a.agent_id !== agent.id),
        };
      } else {
        return {
          ...prev,
          agent_configs: [
            ...prev.agent_configs,
            {
              agent_id: agent.id,
              name: agent.name,
              class_path: agent.class_path,
              config: agent.config,
            },
          ],
        };
      }
    });
    setCostEstimate(null);
  };

  // Build the full config including selected running agents
  const getFullConfig = () => {
    const selectedAgentConfigs = runningAgents
      .filter(a => selectedRunningAgents.has(a.id))
      .map(a => ({
        agent_id: a.id,
        name: a.name,
        use_from_competition: true, // Flag to load from current competition
      }));

    return {
      ...config,
      agent_configs: [...config.agent_configs, ...selectedAgentConfigs],
    };
  };

  const formatDuration = (seconds: number) => {
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
    return `${(seconds / 3600).toFixed(1)}h`;
  };

  return (
    <div className="space-y-6">
      {/* Time Range */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4">Time Range</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label className="block text-sm text-neutral mb-2">Start Date</label>
            <input
              type="date"
              value={config.start_date}
              onChange={(e) => {
                setConfig((prev) => ({ ...prev, start_date: e.target.value }));
                setCostEstimate(null);
              }}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm text-neutral mb-2">End Date</label>
            <input
              type="date"
              value={config.end_date}
              onChange={(e) => {
                setConfig((prev) => ({ ...prev, end_date: e.target.value }));
                setCostEstimate(null);
              }}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm text-neutral mb-2">Tick Interval</label>
            <select
              value={config.tick_interval}
              onChange={(e) => {
                setConfig((prev) => ({ ...prev, tick_interval: e.target.value }));
                setCostEstimate(null);
              }}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            >
              <option value="15m">15 minutes</option>
              <option value="1h">1 hour</option>
              <option value="4h">4 hours</option>
              <option value="1d">1 day</option>
            </select>
          </div>
        </div>
      </div>

      {/* Symbols */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4">Trading Pairs</h2>
        <div className="flex flex-wrap gap-2">
          {['PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD', 'PF_DOGEUSD', 'PF_XRPUSD'].map((symbol) => (
            <button
              key={symbol}
              onClick={() => {
                setConfig((prev) => ({
                  ...prev,
                  symbols: prev.symbols.includes(symbol)
                    ? prev.symbols.filter((s) => s !== symbol)
                    : [...prev.symbols, symbol],
                }));
                setCostEstimate(null);
              }}
              disabled={running}
              className={`px-4 py-2 rounded-lg font-medium transition-all disabled:opacity-50 ${
                config.symbols.includes(symbol)
                  ? 'bg-accent text-white'
                  : 'bg-white/5 text-neutral hover:text-white'
              }`}
            >
              {symbol}
            </button>
          ))}
        </div>
      </div>

      {/* Agents */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4">Agents</h2>

        {/* Running Agents from Current Competition */}
        <div className="mb-6">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-neutral">Current Competition Agents</h3>
            <div className="flex gap-2">
              <button
                onClick={() => setSelectedRunningAgents(new Set(runningAgents.map(a => a.id)))}
                disabled={running || loadingAgents}
                className="text-xs text-accent hover:underline disabled:opacity-50"
              >
                Select All
              </button>
              <button
                onClick={() => setSelectedRunningAgents(new Set())}
                disabled={running || loadingAgents}
                className="text-xs text-neutral hover:text-white disabled:opacity-50"
              >
                Clear
              </button>
            </div>
          </div>
          {loadingAgents ? (
            <div className="text-neutral text-sm">Loading agents...</div>
          ) : runningAgents.length === 0 ? (
            <div className="text-neutral text-sm">No competition running. Start a competition first.</div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {runningAgents.map((agent) => (
                <button
                  key={agent.id}
                  onClick={() => {
                    setSelectedRunningAgents(prev => {
                      const next = new Set(prev);
                      if (next.has(agent.id)) {
                        next.delete(agent.id);
                      } else {
                        next.add(agent.id);
                      }
                      return next;
                    });
                    setCostEstimate(null);
                  }}
                  disabled={running}
                  className={`p-4 rounded-lg text-left transition-all disabled:opacity-50 ${
                    selectedRunningAgents.has(agent.id)
                      ? 'bg-accent/20 border border-accent'
                      : 'bg-white/5 border border-white/10 hover:border-white/20'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="font-medium text-white">{agent.name}</div>
                    <span className={`text-xs px-2 py-0.5 rounded ${
                      agent.agent_type === 'LLM' ? 'bg-blue-500/20 text-blue-400' :
                      agent.agent_type === 'Agentic' ? 'bg-purple-500/20 text-purple-400' :
                      agent.agent_type === 'Skill-Aware' ? 'bg-green-500/20 text-green-400' :
                      'bg-gray-500/20 text-gray-400'
                    }`}>
                      {agent.agent_type}
                    </span>
                  </div>
                  <div className="text-sm text-neutral mt-1">{agent.model}</div>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Baseline toggle */}
        <div className="flex items-center gap-3 mb-4 pb-4 border-b border-white/10">
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={config.run_baselines}
              onChange={(e) => {
                setConfig((prev) => ({ ...prev, run_baselines: e.target.checked }));
                setCostEstimate(null);
              }}
              disabled={running}
              className="w-4 h-4 rounded accent-accent"
            />
            <span className="text-white">Include baseline agents for comparison</span>
          </label>
          <span className="text-neutral text-sm">(Random, SMA, Momentum, Buy&Hold, Mean Reversion)</span>
        </div>

        {/* Baseline agents selection when not including all */}
        {!config.run_baselines && (
          <div className="mb-6">
            <h3 className="text-sm font-medium text-neutral mb-3">Baseline Strategies (optional)</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {BASELINE_AGENTS.map((agent) => (
                <button
                  key={agent.id}
                  onClick={() => toggleAgent(agent)}
                  disabled={running}
                  className={`p-4 rounded-lg text-left transition-all disabled:opacity-50 ${
                    config.agent_configs.some((a) => a.agent_id === agent.id)
                      ? 'bg-accent/20 border border-accent'
                      : 'bg-white/5 border border-white/10 hover:border-white/20'
                  }`}
                >
                  <div className="font-medium text-white">{agent.name}</div>
                  <div className="text-sm text-neutral mt-1">{agent.description}</div>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Cost Estimate */}
      {costEstimate && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className="glass-strong rounded-xl p-6"
        >
          <h2 className="text-lg font-semibold text-white mb-4">Estimated Cost</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="glass-subtle rounded-lg p-4">
              <div className="text-2xl font-bold text-white">
                {costEstimate.total_ticks.toLocaleString()}
              </div>
              <div className="text-sm text-neutral">Total Ticks</div>
            </div>
            <div className="glass-subtle rounded-lg p-4">
              <div className="text-2xl font-bold text-white">
                {formatDuration(costEstimate.duration_hours * 3600)}
              </div>
              <div className="text-sm text-neutral">Duration</div>
            </div>
            <div className="glass-subtle rounded-lg p-4">
              <div className="text-2xl font-bold text-white">
                {costEstimate.estimated_api_calls.toLocaleString()}
              </div>
              <div className="text-sm text-neutral">API Calls</div>
            </div>
            <div className="glass-subtle rounded-lg p-4">
              <div className="text-2xl font-bold text-accent">
                ${costEstimate.estimated_cost_usd.toFixed(2)}
              </div>
              <div className="text-sm text-neutral">Est. Cost</div>
            </div>
          </div>

          {costEstimate.agents.length > 0 && (
            <div className="mt-4 text-sm text-neutral">
              <span className="font-medium">Per agent:</span>{' '}
              {costEstimate.agents.map((a) => (
                <span key={a.agent_id} className="mr-4">
                  {a.agent_id}: ~${a.cost_estimate.toFixed(2)}
                </span>
              ))}
            </div>
          )}
        </motion.div>
      )}

      {/* Run Status */}
      {runStatus && runStatus.status === 'running' && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className="glass-strong rounded-xl p-6"
        >
          <h2 className="text-lg font-semibold text-white mb-4">Running Backtest</h2>
          <div className="space-y-4">
            <div className="flex items-center justify-between text-sm">
              <span className="text-neutral">{runStatus.current_phase}</span>
              <span className="text-white font-mono">
                {runStatus.current_tick} / {runStatus.total_ticks} ticks
              </span>
            </div>
            <div className="h-3 bg-white/10 rounded-full overflow-hidden">
              <motion.div
                className="h-full bg-accent"
                initial={{ width: 0 }}
                animate={{ width: `${runStatus.progress}%` }}
                transition={{ duration: 0.3 }}
              />
            </div>
            <div className="flex items-center justify-between text-sm text-neutral">
              <span>Elapsed: {formatDuration(runStatus.elapsed_seconds)}</span>
              {runStatus.estimated_remaining_seconds && (
                <span>
                  Remaining: ~{formatDuration(runStatus.estimated_remaining_seconds)}
                </span>
              )}
            </div>
          </div>
        </motion.div>
      )}

      {/* Error */}
      {error && (
        <div className="glass-strong rounded-xl p-4 border border-loss/50">
          <p className="text-loss">{error}</p>
        </div>
      )}

      {/* Actions */}
      <div className="flex gap-4">
        <button
          onClick={handleEstimate}
          disabled={
            estimating ||
            running ||
            config.symbols.length === 0 ||
            (!config.run_baselines && config.agent_configs.length === 0 && selectedRunningAgents.size === 0)
          }
          className="flex-1 px-6 py-3 bg-white/10 hover:bg-white/20 disabled:bg-white/5 disabled:cursor-not-allowed rounded-lg font-medium transition-all"
        >
          {estimating ? 'Estimating...' : 'Estimate Cost'}
        </button>
        <button
          onClick={handleStart}
          disabled={
            running ||
            config.symbols.length === 0 ||
            (!config.run_baselines && config.agent_configs.length === 0 && selectedRunningAgents.size === 0)
          }
          className="flex-1 px-6 py-3 bg-accent hover:bg-accent/80 disabled:bg-accent/30 disabled:cursor-not-allowed rounded-lg font-medium transition-all"
        >
          {running ? 'Running...' : 'Start Backtest'}
        </button>
      </div>
    </div>
  );
}
