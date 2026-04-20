import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { useAdminStore } from '../../stores/admin';
import type { EvolutionRunDetail } from '../../types/evolution';

interface Props {
  onRunStarted: () => void;
  onClose: () => void;
}

const SYMBOLS = ['PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD', 'PF_DOGEUSD', 'PF_XRPUSD'];
const INTERVALS = [
  { value: '1m', label: '1 minute' },
  { value: '5m', label: '5 minutes' },
  { value: '15m', label: '15 minutes' },
  { value: '1h', label: '1 hour' },
  { value: '4h', label: '4 hours' },
  { value: '1d', label: '1 day' },
];

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

export default function EvolutionConfig({ onRunStarted, onClose }: Props) {
  const { getHeaders } = useAdminStore();

  // Form state
  const [name, setName] = useState('Evolution Run');
  const [populationSize, setPopulationSize] = useState(20);
  const [generations, setGenerations] = useState(10);
  const [eliteCount, setEliteCount] = useState(3);
  const [mutationRate, setMutationRate] = useState(0.15);
  const [startDate, setStartDate] = useState(getDefaultStartDate);
  const [endDate, setEndDate] = useState(getDefaultEndDate);
  const [tickInterval, setTickInterval] = useState('4h');
  const [symbols, setSymbols] = useState<string[]>(['PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD']);

  // Advanced options
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [useLlmOperators, setUseLlmOperators] = useState(false);
  const [llmOperatorProb, setLlmOperatorProb] = useState(0.3);
  const [useNovelty, setUseNovelty] = useState(false);
  const [usePareto, setUsePareto] = useState(false);

  // Run state
  const [running, setRunning] = useState(false);
  const [runId, setRunId] = useState<string | null>(null);
  const [runDetail, setRunDetail] = useState<EvolutionRunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Poll for run status
  useEffect(() => {
    if (!runId || !running) return;

    const interval = setInterval(async () => {
      try {
        const response = await fetch(`/api/evolution/${runId}`);
        if (response.ok) {
          const data: EvolutionRunDetail = await response.json();
          setRunDetail(data);
          if (data.status === 'completed') {
            setRunning(false);
            onRunStarted();
          } else if (data.status === 'cancelled' || data.status === 'failed') {
            setRunning(false);
          }
        }
      } catch (err) {
        console.error('Failed to poll evolution status:', err);
      }
    }, 3000);

    return () => clearInterval(interval);
  }, [runId, running, onRunStarted]);

  const handleStart = async () => {
    try {
      setRunning(true);
      setError(null);

      const body = {
        name,
        population_size: populationSize,
        generations,
        elite_count: eliteCount,
        mutation_rate: mutationRate,
        backtest_start: startDate,
        backtest_end: endDate,
        tick_interval: tickInterval,
        symbols,
        use_llm_operators: useLlmOperators,
        llm_operator_prob: llmOperatorProb,
        use_novelty: useNovelty,
        use_pareto: usePareto,
      };

      const response = await fetch('/api/evolution/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getHeaders() },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const data = await response.json();
        if (response.status === 403) {
          throw new Error('Admin access required. Set your admin key in the admin panel.');
        }
        if (response.status === 429) {
          throw new Error('Maximum concurrent evolution runs reached. Wait for a run to finish.');
        }
        throw new Error(data.detail || data.error || 'Failed to start evolution');
      }

      const data = await response.json();
      setRunId(data.run_id);
    } catch (err) {
      setRunning(false);
      setError(err instanceof Error ? err.message : 'Unknown error');
    }
  };

  const handleCancel = async () => {
    if (!runId) return;
    try {
      const response = await fetch(`/api/evolution/${runId}/stop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getHeaders() },
      });
      if (response.ok) {
        setRunning(false);
      }
    } catch (err) {
      console.error('Failed to cancel evolution:', err);
    }
  };

  const toggleSymbol = (symbol: string) => {
    setSymbols((prev) =>
      prev.includes(symbol) ? prev.filter((s) => s !== symbol) : [...prev, symbol]
    );
  };

  const progress =
    runDetail && runDetail.max_generations > 0
      ? (runDetail.current_generation / runDetail.max_generations) * 100
      : 0;

  return (
    <motion.div
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
      exit={{ opacity: 0, height: 0 }}
      className="space-y-4"
    >
      {/* Header */}
      <div className="glass-strong rounded-xl p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">New Evolution Run</h2>
          <button
            onClick={onClose}
            disabled={running}
            className="text-neutral hover:text-white transition-colors disabled:opacity-50"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Run Name */}
        <div className="mb-4">
          <label className="block text-sm text-neutral mb-2">Run Name</label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={running}
            className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
          />
        </div>

        {/* Population & Generations */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
          <div>
            <label className="block text-sm text-neutral mb-2">Population</label>
            <input
              type="number"
              min={2}
              max={100}
              value={populationSize}
              onChange={(e) => setPopulationSize(Number(e.target.value))}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm text-neutral mb-2">Generations</label>
            <input
              type="number"
              min={1}
              max={100}
              value={generations}
              onChange={(e) => setGenerations(Number(e.target.value))}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm text-neutral mb-2">Elite Count</label>
            <input
              type="number"
              min={1}
              max={populationSize - 1}
              value={eliteCount}
              onChange={(e) => setEliteCount(Number(e.target.value))}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm text-neutral mb-2">Mutation Rate</label>
            <input
              type="number"
              min={0}
              max={1}
              step={0.05}
              value={mutationRate}
              onChange={(e) => setMutationRate(Number(e.target.value))}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
        </div>

        {/* Date Range & Interval */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <div>
            <label className="block text-sm text-neutral mb-2">Start Date</label>
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm text-neutral mb-2">End Date</label>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            />
          </div>
          <div>
            <label className="block text-sm text-neutral mb-2">Tick Interval</label>
            <select
              value={tickInterval}
              onChange={(e) => setTickInterval(e.target.value)}
              disabled={running}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent disabled:opacity-50"
            >
              {INTERVALS.map((i) => (
                <option key={i.value} value={i.value}>
                  {i.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Symbols */}
        <div className="mb-4">
          <label className="block text-sm text-neutral mb-2">Trading Pairs</label>
          <div className="flex flex-wrap gap-2">
            {SYMBOLS.map((symbol) => (
              <button
                key={symbol}
                onClick={() => toggleSymbol(symbol)}
                disabled={running}
                className={`px-4 py-2 rounded-lg font-medium transition-all disabled:opacity-50 ${
                  symbols.includes(symbol)
                    ? 'bg-accent text-white'
                    : 'bg-white/5 text-neutral hover:text-white'
                }`}
              >
                {symbol}
              </button>
            ))}
          </div>
        </div>

        {/* Advanced Options */}
        <div>
          <button
            onClick={() => setShowAdvanced(!showAdvanced)}
            disabled={running}
            className="flex items-center gap-2 text-sm text-neutral hover:text-white transition-colors disabled:opacity-50"
          >
            <svg
              className={`w-4 h-4 transition-transform ${showAdvanced ? 'rotate-90' : ''}`}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </svg>
            Advanced Options
          </button>

          {showAdvanced && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              className="mt-3 space-y-3 pl-6"
            >
              <label className="flex items-center gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={useLlmOperators}
                  onChange={(e) => setUseLlmOperators(e.target.checked)}
                  disabled={running}
                  className="w-4 h-4 rounded accent-accent"
                />
                <div>
                  <span className="text-white text-sm">LLM Operators</span>
                  <p className="text-xs text-neutral">Use LLM-guided crossover and mutation</p>
                </div>
              </label>

              {useLlmOperators && (
                <div className="ml-7">
                  <label className="block text-sm text-neutral mb-1">
                    LLM Operator Probability: {llmOperatorProb.toFixed(2)}
                  </label>
                  <input
                    type="range"
                    min={0}
                    max={1}
                    step={0.05}
                    value={llmOperatorProb}
                    onChange={(e) => setLlmOperatorProb(Number(e.target.value))}
                    disabled={running}
                    className="w-full accent-accent"
                  />
                </div>
              )}

              <label className="flex items-center gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={useNovelty}
                  onChange={(e) => setUseNovelty(e.target.checked)}
                  disabled={running}
                  className="w-4 h-4 rounded accent-accent"
                />
                <div>
                  <span className="text-white text-sm">Novelty Search</span>
                  <p className="text-xs text-neutral">Reward behavioral diversity alongside fitness</p>
                </div>
              </label>

              <label className="flex items-center gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={usePareto}
                  onChange={(e) => setUsePareto(e.target.checked)}
                  disabled={running}
                  className="w-4 h-4 rounded accent-accent"
                />
                <div>
                  <span className="text-white text-sm">Pareto Selection</span>
                  <p className="text-xs text-neutral">Multi-objective optimization across fitness dimensions</p>
                </div>
              </label>
            </motion.div>
          )}
        </div>
      </div>

      {/* Progress (while running) */}
      {running && runId && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className="glass-strong rounded-xl p-6"
        >
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-white">Running Evolution</h2>
            <span className="flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-400/20 text-yellow-400">
              <span className="w-1.5 h-1.5 rounded-full animate-pulse bg-yellow-400" />
              Running
            </span>
          </div>

          <div className="space-y-3">
            <div className="flex items-center justify-between text-sm">
              <span className="text-neutral">
                Generation {runDetail?.current_generation ?? 0} / {runDetail?.max_generations ?? generations}
              </span>
              <span className="text-white font-mono-numbers">{Math.round(progress)}%</span>
            </div>
            <div className="h-3 bg-white/10 rounded-full overflow-hidden">
              <motion.div
                className="h-full bg-accent"
                initial={{ width: 0 }}
                animate={{ width: `${progress}%` }}
                transition={{ duration: 0.3 }}
              />
            </div>
            {runDetail?.best_fitness != null && (
              <div className="text-sm text-neutral">
                Best fitness: <span className="text-white font-mono-numbers">{runDetail.best_fitness.toFixed(4)}</span>
              </div>
            )}
          </div>
        </motion.div>
      )}

      {/* Completed / Cancelled status */}
      {!running && runDetail && (runDetail.status === 'completed' || runDetail.status === 'cancelled') && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className={`glass-strong rounded-xl p-4 border ${
            runDetail.status === 'completed'
              ? 'border-emerald-500/20 bg-emerald-500/5'
              : 'border-red-500/20 bg-red-500/5'
          }`}
        >
          <p className={runDetail.status === 'completed' ? 'text-emerald-400' : 'text-red-400'}>
            Evolution {runDetail.status}.
            {runDetail.status === 'completed' && runDetail.best_fitness != null && (
              <span className="ml-2">Best fitness: {runDetail.best_fitness.toFixed(4)}</span>
            )}
          </p>
        </motion.div>
      )}

      {/* Error */}
      {error && (
        <div className="glass-strong rounded-xl p-4 border border-red-500/20 bg-red-500/5">
          <p className="text-red-400">{error}</p>
        </div>
      )}

      {/* Actions */}
      <div className="flex gap-4">
        {!running ? (
          <>
            <button
              onClick={onClose}
              className="flex-1 px-6 py-3 bg-white/10 hover:bg-white/20 rounded-lg font-medium transition-all"
            >
              Cancel
            </button>
            <button
              onClick={handleStart}
              disabled={symbols.length === 0}
              className="flex-1 px-6 py-3 bg-accent hover:bg-accent/80 disabled:bg-accent/30 disabled:cursor-not-allowed rounded-lg font-medium transition-all"
            >
              Start Evolution
            </button>
          </>
        ) : (
          <button
            onClick={handleCancel}
            className="flex-1 px-6 py-3 bg-red-500/20 hover:bg-red-500/30 text-red-400 rounded-lg font-medium transition-all"
          >
            Cancel Run
          </button>
        )}
      </div>
    </motion.div>
  );
}
