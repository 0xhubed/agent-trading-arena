import { useState, useEffect, useCallback } from 'react';
import { motion } from 'framer-motion';
import { useAdminStore } from '../../stores/admin';
import type { DataStatus, FetchProgress } from '../../types/backtest';

export default function DataManager() {
  const { getHeaders } = useAdminStore();
  const [dataStatus, setDataStatus] = useState<DataStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [fetchProgress, setFetchProgress] = useState<FetchProgress | null>(null);
  const [fetchForm, setFetchForm] = useState({
    symbols: ['PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD', 'PF_DOGEUSD', 'PF_XRPUSD'],
    intervals: ['1h'],
    start_date: getDefaultStartDate(),
    end_date: getDefaultEndDate(),
  });

  function getDefaultStartDate(): string {
    const date = new Date();
    date.setMonth(date.getMonth() - 3);
    return date.toISOString().split('T')[0];
  }

  function getDefaultEndDate(): string {
    return new Date().toISOString().split('T')[0];
  }

  const fetchDataStatus = useCallback(async () => {
    try {
      setLoading(true);
      const response = await fetch('/api/backtest/data/status');
      if (!response.ok) throw new Error('Failed to fetch data status');
      const data = await response.json();
      setDataStatus(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDataStatus();
  }, [fetchDataStatus]);

  // Poll for fetch progress when running
  useEffect(() => {
    if (!fetchProgress || fetchProgress.status !== 'running') return;

    const controller = new AbortController();
    const interval = setInterval(async () => {
      try {
        const response = await fetch('/api/backtest/data/fetch/status', {
          signal: controller.signal,
        });
        if (response.ok) {
          const data = await response.json();
          setFetchProgress(data);
          if (data.status === 'completed') {
            fetchDataStatus();
          }
        }
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') return;
        console.error('Failed to poll fetch status:', err);
      }
    }, 2000);

    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [fetchProgress?.status, fetchDataStatus]);

  const handleFetch = async () => {
    try {
      setFetchProgress({ status: 'running', progress: 0, message: 'Starting...' });
      const response = await fetch('/api/backtest/data/fetch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...getHeaders() },
        body: JSON.stringify(fetchForm),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || data.error || 'Failed to start fetch');
      }

      const data = await response.json();
      setFetchProgress({
        status: 'running',
        progress: 0,
        message: data.message,
      });
    } catch (err) {
      setFetchProgress({
        status: 'failed',
        progress: 0,
        error: err instanceof Error ? err.message : 'Unknown error',
      });
    }
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return 'N/A';
    return new Date(dateStr).toLocaleDateString();
  };

  const formatNumber = (num: number) => {
    return num.toLocaleString();
  };

  if (loading && !dataStatus) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center">
        <div className="animate-spin w-8 h-8 border-2 border-accent border-t-transparent rounded-full mx-auto mb-4" />
        <p className="text-neutral">Loading data status...</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Fetch Historical Data Form */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4">
          Fetch Historical Data
        </h2>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
          {/* Symbols */}
          <div>
            <label className="block text-sm text-neutral mb-2">Symbols</label>
            <div className="flex flex-wrap gap-2">
              {['PF_XBTUSD', 'PF_ETHUSD', 'PF_SOLUSD', 'PF_DOGEUSD', 'PF_XRPUSD'].map((symbol) => (
                <button
                  key={symbol}
                  onClick={() => {
                    setFetchForm((prev) => ({
                      ...prev,
                      symbols: prev.symbols.includes(symbol)
                        ? prev.symbols.filter((s) => s !== symbol)
                        : [...prev.symbols, symbol],
                    }));
                  }}
                  className={`px-3 py-1 rounded-lg text-sm font-medium transition-all ${
                    fetchForm.symbols.includes(symbol)
                      ? 'bg-accent text-white'
                      : 'bg-white/5 text-neutral hover:text-white'
                  }`}
                >
                  {symbol}
                </button>
              ))}
            </div>
          </div>

          {/* Intervals */}
          <div>
            <label className="block text-sm text-neutral mb-2">Intervals</label>
            <div className="flex flex-wrap gap-2">
              {['15m', '1h', '4h', '1d'].map((interval) => (
                <button
                  key={interval}
                  onClick={() => {
                    setFetchForm((prev) => ({
                      ...prev,
                      intervals: prev.intervals.includes(interval)
                        ? prev.intervals.filter((i) => i !== interval)
                        : [...prev.intervals, interval],
                    }));
                  }}
                  className={`px-3 py-1 rounded-lg text-sm font-medium transition-all ${
                    fetchForm.intervals.includes(interval)
                      ? 'bg-accent text-white'
                      : 'bg-white/5 text-neutral hover:text-white'
                  }`}
                >
                  {interval}
                </button>
              ))}
            </div>
          </div>

          {/* Date Range */}
          <div>
            <label className="block text-sm text-neutral mb-2">Start Date</label>
            <input
              type="date"
              value={fetchForm.start_date}
              onChange={(e) => setFetchForm((prev) => ({ ...prev, start_date: e.target.value }))}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent"
            />
          </div>

          <div>
            <label className="block text-sm text-neutral mb-2">End Date</label>
            <input
              type="date"
              value={fetchForm.end_date}
              onChange={(e) => setFetchForm((prev) => ({ ...prev, end_date: e.target.value }))}
              className="w-full px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white focus:outline-none focus:border-accent"
            />
          </div>
        </div>

        {/* Fetch Button / Progress */}
        {fetchProgress?.status === 'running' ? (
          <div className="space-y-3">
            <div className="flex items-center justify-between text-sm">
              <span className="text-neutral">
                {fetchProgress.current_symbol && fetchProgress.current_interval
                  ? `Fetching ${fetchProgress.current_symbol} ${fetchProgress.current_interval}...`
                  : fetchProgress.message || 'Fetching data...'}
              </span>
              <span className="text-white font-mono">
                {Math.round(fetchProgress.progress)}%
              </span>
            </div>
            <div className="h-2 bg-white/10 rounded-full overflow-hidden">
              <motion.div
                className="h-full bg-accent"
                initial={{ width: 0 }}
                animate={{ width: `${fetchProgress.progress}%` }}
                transition={{ duration: 0.3 }}
              />
            </div>
          </div>
        ) : (
          <button
            onClick={handleFetch}
            disabled={fetchForm.symbols.length === 0 || fetchForm.intervals.length === 0}
            className="w-full px-6 py-3 bg-accent hover:bg-accent/80 disabled:bg-accent/30 disabled:cursor-not-allowed rounded-lg font-medium transition-all"
          >
            {fetchProgress?.status === 'completed'
              ? 'Fetch More Data'
              : 'Fetch Historical Data'}
          </button>
        )}

        {fetchProgress?.status === 'failed' && (
          <p className="mt-3 text-loss text-sm">{fetchProgress.error}</p>
        )}

        {fetchProgress?.status === 'completed' && (
          <p className="mt-3 text-profit text-sm">Data fetch completed successfully!</p>
        )}
      </div>

      {/* Data Status */}
      <div className="glass-strong rounded-xl p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">Available Data</h2>
          <button
            onClick={fetchDataStatus}
            className="px-3 py-1 text-sm text-neutral hover:text-white transition-colors"
          >
            Refresh
          </button>
        </div>

        {error && (
          <p className="text-loss mb-4">{error}</p>
        )}

        {dataStatus && (
          <>
            {/* Summary */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-white">
                  {formatNumber(dataStatus.total_candles)}
                </div>
                <div className="text-sm text-neutral">Total Candles</div>
              </div>
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-white">
                  {new Set(dataStatus.symbols.map((s) => s.symbol)).size}
                </div>
                <div className="text-sm text-neutral">Symbols</div>
              </div>
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-white">
                  {new Set(dataStatus.symbols.map((s) => s.interval)).size}
                </div>
                <div className="text-sm text-neutral">Intervals</div>
              </div>
              <div className="glass-subtle rounded-lg p-4">
                <div className="text-2xl font-bold text-accent capitalize">
                  {dataStatus.database_backend}
                </div>
                <div className="text-sm text-neutral">Backend</div>
              </div>
            </div>

            {/* Detailed Status Table */}
            {dataStatus.symbols.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-neutral border-b border-white/10">
                      <th className="text-left py-3 px-4">Symbol</th>
                      <th className="text-left py-3 px-4">Interval</th>
                      <th className="text-left py-3 px-4">From</th>
                      <th className="text-left py-3 px-4">To</th>
                      <th className="text-right py-3 px-4">Candles</th>
                      <th className="text-right py-3 px-4">Gaps</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dataStatus.symbols.map((item) => (
                      <tr
                        key={`${item.symbol}-${item.interval}`}
                        className="border-b border-white/5 hover:bg-white/5"
                      >
                        <td className="py-3 px-4 font-medium text-white">
                          {item.symbol}
                        </td>
                        <td className="py-3 px-4 text-neutral">{item.interval}</td>
                        <td className="py-3 px-4 text-neutral font-mono text-xs">
                          {formatDate(item.earliest)}
                        </td>
                        <td className="py-3 px-4 text-neutral font-mono text-xs">
                          {formatDate(item.latest)}
                        </td>
                        <td className="py-3 px-4 text-right text-white font-mono">
                          {formatNumber(item.count)}
                        </td>
                        <td className="py-3 px-4 text-right">
                          <span
                            className={
                              item.gaps > 0 ? 'text-loss' : 'text-profit'
                            }
                          >
                            {item.gaps}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-8 text-neutral">
                <p>No historical data available.</p>
                <p className="text-sm mt-2">
                  Use the form above to fetch data from Kraken Futures.
                </p>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
