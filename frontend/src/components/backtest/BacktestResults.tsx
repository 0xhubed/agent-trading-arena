import { useState, useEffect } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
  ReferenceLine,
} from 'recharts';
import { useAdminStore } from '../../stores/admin';
import type {
  BacktestRun,
  BacktestResult,
} from '../../types/backtest';

interface Props {
  runs: BacktestRun[];
  selectedRunId: string | null;
  onSelectRun: (runId: string | null) => void;
  onRefresh: () => void;
}

const COLORS = [
  '#10b981', // emerald
  '#3b82f6', // blue
  '#f59e0b', // amber
  '#ef4444', // red
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#84cc16', // lime
];

export default function BacktestResults({
  runs,
  selectedRunId,
  onSelectRun,
  onRefresh,
}: Props) {
  const { adminKey, getHeaders } = useAdminStore();
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [observerRunning, setObserverRunning] = useState(false);
  const [observerResult, setObserverResult] = useState<{
    status: string;
    message?: string;
    patterns_found?: number;
    skills_updated?: string[];
  } | null>(null);

  useEffect(() => {
    if (!selectedRunId) {
      setResult(null);
      return;
    }

    const fetchResult = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await fetch(`/api/backtest/${selectedRunId}/results`);
        if (!response.ok) {
          const data = await response.json();
          throw new Error(data.detail || data.error || 'Failed to fetch results');
        }
        const data = await response.json();
        setResult(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };

    fetchResult();
  }, [selectedRunId]);

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString();
  };

  const formatPct = (value: number) => {
    const sign = value >= 0 ? '+' : '';
    return `${sign}${(value * 100).toFixed(2)}%`;
  };

  const formatMoney = (value: number) => {
    return `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const runObserverAnalysis = async () => {
    if (!selectedRunId || !adminKey) return;

    try {
      setObserverRunning(true);
      setObserverResult(null);
      setError(null);

      const response = await fetch(`/api/observer/analyze-backtest/${selectedRunId}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...getHeaders(),
        },
      });

      if (response.status === 403) {
        throw new Error('Invalid admin key. Access denied.');
      }

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || 'Observer analysis failed');
      }

      const data = await response.json();
      setObserverResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setObserverRunning(false);
    }
  };

  // Prepare equity curve data for chart
  const equityCurveData = result?.agents
    ? (() => {
        const maxLength = Math.max(...result.agents.map((a) => a.equity_curve.length));
        const data: Record<string, unknown>[] = [];
        for (let i = 0; i < maxLength; i++) {
          const point: Record<string, unknown> = { tick: i };
          result.agents.forEach((agent) => {
            if (agent.equity_curve[i]) {
              point[agent.agent_id] = agent.equity_curve[i].equity;
            }
          });
          data.push(point);
        }
        return data;
      })()
    : [];

  if (runs.length === 0) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center">
        <p className="text-neutral mb-4">No backtest results yet.</p>
        <p className="text-sm text-neutral">
          Configure and run a backtest to see results here.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Run Selector */}
      <div className="glass-strong rounded-xl p-4">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">Backtest Runs</h2>
          <button
            onClick={onRefresh}
            className="px-3 py-1 text-sm text-neutral hover:text-white transition-colors"
          >
            Refresh
          </button>
        </div>
        <div className="flex flex-wrap gap-2">
          {runs.map((run) => (
            <button
              key={run.run_id}
              onClick={() => onSelectRun(run.run_id)}
              className={`px-4 py-2 rounded-lg text-sm transition-all ${
                selectedRunId === run.run_id
                  ? 'bg-accent text-white'
                  : 'bg-white/5 text-neutral hover:text-white hover:bg-white/10'
              }`}
            >
              <div className="font-medium">
                {formatDate(run.start_date)} - {formatDate(run.end_date)}
              </div>
              <div className="text-xs opacity-70">
                {run.symbols.join(', ')} • {run.tick_interval}
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* Loading / Error */}
      {loading && (
        <div className="glass-strong rounded-xl p-8 text-center">
          <div className="animate-spin w-8 h-8 border-2 border-accent border-t-transparent rounded-full mx-auto mb-4" />
          <p className="text-neutral">Loading results...</p>
        </div>
      )}

      {error && (
        <div className="glass-strong rounded-xl p-4 border border-loss/50">
          <p className="text-loss">{error}</p>
        </div>
      )}

      {/* Results */}
      {result && !loading && (
        <>
          {/* Summary */}
          <div className="glass-strong rounded-xl p-6">
            <h2 className="text-lg font-semibold text-white mb-4">Summary</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-white">
                  {result.total_ticks.toLocaleString()}
                </div>
                <div className="text-sm text-neutral">Total Ticks</div>
              </div>
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-white">
                  {result.agents.length}
                </div>
                <div className="text-sm text-neutral">Agents</div>
              </div>
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-white">
                  {Math.round(result.duration_seconds)}s
                </div>
                <div className="text-sm text-neutral">Duration</div>
              </div>
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-white">
                  {result.config.symbols.join(', ')}
                </div>
                <div className="text-sm text-neutral">Symbols</div>
              </div>
            </div>
          </div>

          {/* Equity Curves */}
          <div className="glass-strong rounded-xl p-6">
            <h2 className="text-lg font-semibold text-white mb-4">Equity Curves</h2>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={equityCurveData}>
                  <XAxis
                    dataKey="tick"
                    stroke="#666"
                    tick={{ fill: '#999', fontSize: 11 }}
                    tickLine={{ stroke: '#444' }}
                  />
                  <YAxis
                    stroke="#666"
                    tick={{ fill: '#999', fontSize: 11 }}
                    tickLine={{ stroke: '#444' }}
                    tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
                    domain={['dataMin - 500', 'dataMax + 500']}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(0,0,0,0.9)',
                      border: '1px solid rgba(255,255,255,0.1)',
                      borderRadius: '8px',
                    }}
                    formatter={(value: number) => [formatMoney(value), '']}
                  />
                  <Legend />
                  <ReferenceLine y={10000} stroke="#444" strokeDasharray="3 3" />
                  {result.agents.map((agent, idx) => (
                    <Line
                      key={agent.agent_id}
                      type="monotone"
                      dataKey={agent.agent_id}
                      name={agent.agent_name}
                      stroke={COLORS[idx % COLORS.length]}
                      dot={false}
                      strokeWidth={2}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Agent Rankings */}
          <div className="glass-strong rounded-xl p-6">
            <h2 className="text-lg font-semibold text-white mb-4">Agent Performance</h2>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-neutral border-b border-white/10">
                    <th className="text-left py-3 px-4">#</th>
                    <th className="text-left py-3 px-4">Agent</th>
                    <th className="text-right py-3 px-4">Final Equity</th>
                    <th className="text-right py-3 px-4">Return</th>
                    <th className="text-right py-3 px-4">Win Rate</th>
                    <th className="text-right py-3 px-4">Sharpe</th>
                    <th className="text-right py-3 px-4">Max DD</th>
                    <th className="text-right py-3 px-4">Trades</th>
                  </tr>
                </thead>
                <tbody>
                  {[...result.agents]
                    .sort((a, b) => b.final_equity - a.final_equity)
                    .map((agent, idx) => (
                      <tr
                        key={agent.agent_id}
                        className="border-b border-white/5 hover:bg-white/5"
                      >
                        <td className="py-3 px-4 text-neutral">{idx + 1}</td>
                        <td className="py-3 px-4 font-medium text-white">
                          {agent.agent_name}
                        </td>
                        <td className="py-3 px-4 text-right font-mono text-white">
                          {formatMoney(agent.final_equity)}
                        </td>
                        <td
                          className={`py-3 px-4 text-right font-mono ${
                            agent.total_return >= 0 ? 'text-profit' : 'text-loss'
                          }`}
                        >
                          {formatPct(agent.total_return)}
                        </td>
                        <td className="py-3 px-4 text-right font-mono text-white">
                          {(agent.win_rate * 100).toFixed(1)}%
                        </td>
                        <td className="py-3 px-4 text-right font-mono text-white">
                          {agent.sharpe_ratio?.toFixed(2) || 'N/A'}
                        </td>
                        <td className="py-3 px-4 text-right font-mono text-loss">
                          -{(agent.max_drawdown_pct * 100).toFixed(1)}%
                        </td>
                        <td className="py-3 px-4 text-right text-neutral">
                          {agent.total_trades}
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Statistical Comparisons */}
          {result.comparisons && result.comparisons.length > 0 && (
            <div className="glass-strong rounded-xl p-6">
              <h2 className="text-lg font-semibold text-white mb-4">
                Statistical Comparisons
              </h2>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-neutral border-b border-white/10">
                      <th className="text-left py-3 px-4">Agent</th>
                      <th className="text-left py-3 px-4">vs Baseline</th>
                      <th className="text-right py-3 px-4">Agent Return</th>
                      <th className="text-right py-3 px-4">Baseline Return</th>
                      <th className="text-right py-3 px-4">Outperformance</th>
                      <th className="text-right py-3 px-4">P-Value</th>
                      <th className="text-center py-3 px-4">Significant?</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.comparisons.map((comp) => (
                      <tr
                        key={`${comp.agent_id}-${comp.baseline_id}`}
                        className="border-b border-white/5 hover:bg-white/5"
                      >
                        <td className="py-3 px-4 font-medium text-white">
                          {comp.agent_id}
                        </td>
                        <td className="py-3 px-4 text-neutral">{comp.baseline_id}</td>
                        <td className="py-3 px-4 text-right font-mono text-white">
                          {formatPct(comp.agent_return)}
                        </td>
                        <td className="py-3 px-4 text-right font-mono text-neutral">
                          {formatPct(comp.baseline_return)}
                        </td>
                        <td
                          className={`py-3 px-4 text-right font-mono ${
                            comp.outperformance >= 0 ? 'text-profit' : 'text-loss'
                          }`}
                        >
                          {formatPct(comp.outperformance)}
                        </td>
                        <td className="py-3 px-4 text-right font-mono text-neutral">
                          {comp.p_value?.toFixed(4) || 'N/A'}
                        </td>
                        <td className="py-3 px-4 text-center">
                          {comp.is_significant ? (
                            <span className="inline-flex items-center gap-1 px-2 py-1 bg-profit/20 text-profit text-xs rounded">
                              <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                                <path
                                  fillRule="evenodd"
                                  d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                                  clipRule="evenodd"
                                />
                              </svg>
                              Yes
                            </span>
                          ) : (
                            <span className="text-neutral text-xs">No</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="mt-4 text-xs text-neutral">
                Statistical significance tested using {result.comparisons[0]?.test_used || 'paired t-test'}.
                p &lt; 0.05 indicates statistically significant outperformance.
              </p>
            </div>
          )}

          {/* Observer Analysis */}
          <div className="glass-strong rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h2 className="text-lg font-semibold text-white">Observer Analysis</h2>
                <p className="text-sm text-neutral mt-1">
                  Run the Observer Agent to extract insights and update skills from this backtest.
                </p>
              </div>
              <button
                onClick={runObserverAnalysis}
                disabled={observerRunning || !adminKey}
                className="px-4 py-2 bg-purple-500/20 hover:bg-purple-500/30 disabled:bg-purple-500/10 disabled:cursor-not-allowed text-purple-400 rounded-lg text-sm font-medium transition-all"
                title={!adminKey ? 'Authenticate via Admin Panel first' : ''}
              >
                {observerRunning ? (
                  <span className="flex items-center gap-2">
                    <div className="w-4 h-4 border-2 border-purple-400 border-t-transparent rounded-full animate-spin" />
                    Analyzing...
                  </span>
                ) : (
                  'Analyze with Observer'
                )}
              </button>
            </div>

            {!adminKey && (
              <p className="text-xs text-neutral mt-2">Authenticate via the Admin Panel (click title) to enable Observer analysis.</p>
            )}

            {observerResult && (
              <div className="space-y-4 pt-4 border-t border-white/10">
                <div className={`p-4 rounded-lg ${
                  observerResult.status === 'success'
                    ? 'bg-profit/10 border border-profit/30'
                    : 'bg-loss/10 border border-loss/30'
                }`}>
                  <div className="flex items-center gap-2 mb-2">
                    {observerResult.status === 'success' ? (
                      <svg className="w-5 h-5 text-profit" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                      </svg>
                    ) : (
                      <svg className="w-5 h-5 text-loss" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                      </svg>
                    )}
                    <span className={`font-medium ${
                      observerResult.status === 'success' ? 'text-profit' : 'text-loss'
                    }`}>
                      {observerResult.status === 'success' ? 'Analysis Complete' : 'Analysis Failed'}
                    </span>
                  </div>
                  {observerResult.patterns_found !== undefined && (
                    <p className="text-sm text-neutral">
                      Found <span className="text-white font-medium">{observerResult.patterns_found}</span> patterns
                    </p>
                  )}
                  {observerResult.skills_updated && observerResult.skills_updated.length > 0 && (
                    <p className="text-sm text-neutral mt-1">
                      Updated skills: <span className="text-white">{observerResult.skills_updated.join(', ')}</span>
                    </p>
                  )}
                  {observerResult.message && (
                    <p className="text-sm text-neutral mt-1">{observerResult.message}</p>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Trade Details (collapsed by default) */}
          <details className="glass-strong rounded-xl">
            <summary className="p-6 cursor-pointer text-lg font-semibold text-white hover:text-accent transition-colors">
              Trade Details
            </summary>
            <div className="p-6 pt-0">
              {result.agents.map((agent) => (
                <div key={agent.agent_id} className="mb-6 last:mb-0">
                  <h3 className="font-medium text-white mb-3">
                    {agent.agent_name} ({agent.trades.length} trades)
                  </h3>
                  {agent.trades.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="text-neutral border-b border-white/10">
                            <th className="text-left py-2 px-3">Tick</th>
                            <th className="text-left py-2 px-3">Symbol</th>
                            <th className="text-left py-2 px-3">Action</th>
                            <th className="text-right py-2 px-3">Entry</th>
                            <th className="text-right py-2 px-3">Exit</th>
                            <th className="text-right py-2 px-3">Size</th>
                            <th className="text-right py-2 px-3">P&L</th>
                          </tr>
                        </thead>
                        <tbody>
                          {agent.trades.slice(0, 20).map((trade, idx) => (
                            <tr
                              key={idx}
                              className="border-b border-white/5"
                            >
                              <td className="py-2 px-3 text-neutral">{trade.tick}</td>
                              <td className="py-2 px-3 text-white">{trade.symbol}</td>
                              <td className="py-2 px-3 text-neutral">{trade.action}</td>
                              <td className="py-2 px-3 text-right font-mono text-neutral">
                                {trade.entry_price?.toFixed(2) || '-'}
                              </td>
                              <td className="py-2 px-3 text-right font-mono text-neutral">
                                {trade.exit_price?.toFixed(2) || '-'}
                              </td>
                              <td className="py-2 px-3 text-right font-mono text-neutral">
                                {trade.size.toFixed(4)}
                              </td>
                              <td
                                className={`py-2 px-3 text-right font-mono ${
                                  (trade.pnl || 0) >= 0 ? 'text-profit' : 'text-loss'
                                }`}
                              >
                                {trade.pnl ? formatMoney(trade.pnl) : '-'}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      {agent.trades.length > 20 && (
                        <p className="text-xs text-neutral mt-2">
                          Showing first 20 of {agent.trades.length} trades
                        </p>
                      )}
                    </div>
                  ) : (
                    <p className="text-neutral text-sm">No trades executed</p>
                  )}
                </div>
              ))}
            </div>
          </details>
        </>
      )}
    </div>
  );
}
