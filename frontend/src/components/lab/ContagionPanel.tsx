import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { useAdminStore } from '../../stores/admin';
import type { ContagionLatest, ContagionMetric } from '../../types/lab';

export default function ContagionPanel() {
  const { getHeaders } = useAdminStore();
  const [latest, setLatest] = useState<ContagionLatest | null>(null);
  const [history, setHistory] = useState<ContagionMetric[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);

      const [latestRes, historyRes] = await Promise.all([
        fetch('/api/lab/contagion/latest'),
        fetch('/api/lab/contagion/history?limit=50'),
      ]);

      if (!latestRes.ok) throw new Error(`HTTP ${latestRes.status}`);
      if (!historyRes.ok) throw new Error(`HTTP ${historyRes.status}`);

      const latestData = await latestRes.json();
      const historyData = await historyRes.json();

      setLatest(latestData);
      setHistory(historyData.snapshots || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch');
    } finally {
      setLoading(false);
    }
  };

  const triggerAnalysis = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/lab/contagion/analyze', {
        method: 'POST',
        headers: getHeaders(),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const result = await res.json();
      if (result.status === 'skipped') {
        setError(result.message);
      }
      await fetchData();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Analysis failed');
      setLoading(false);
    }
  };

  const healthColor = (label: string) => {
    switch (label) {
      case 'HEALTHY': return 'text-profit';
      case 'MODERATE': return 'text-yellow-400';
      case 'WARNING': return 'text-loss';
      default: return 'text-neutral';
    }
  };

  const healthBg = (label: string) => {
    switch (label) {
      case 'HEALTHY': return 'bg-profit/20 border-profit/30';
      case 'MODERATE': return 'bg-yellow-400/20 border-yellow-400/30';
      case 'WARNING': return 'bg-loss/20 border-loss/30';
      default: return 'bg-white/5 border-white/10';
    }
  };

  const metricLabel = (type: string) => {
    return type.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  };

  if (loading && !latest) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral animate-pulse-slow">
        Loading contagion metrics...
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-white">System Health</h3>
        <button
          onClick={triggerAnalysis}
          disabled={loading}
          className="px-3 py-1.5 text-xs font-medium rounded-lg bg-accent/20 border border-accent/30 text-accent hover:bg-accent/30 transition-all disabled:opacity-50"
        >
          {loading ? 'Analyzing...' : 'Run Analysis'}
        </button>
      </div>

      {error && (
        <div className="p-3 rounded-lg bg-loss/10 border border-loss/20 text-loss text-sm">
          {error}
        </div>
      )}

      {/* Health banner */}
      {latest && (
        <motion.div
          initial={{ opacity: 0, scale: 0.98 }}
          animate={{ opacity: 1, scale: 1 }}
          className={`rounded-xl p-6 border ${healthBg(latest.health_label)}`}
        >
          <div className="flex items-center justify-between">
            <div>
              <div className="text-xs uppercase tracking-wider text-neutral mb-1">
                Overall System Health
              </div>
              <div className={`text-2xl font-bold ${healthColor(latest.health_label)}`}>
                {latest.system_health !== null
                  ? `${(latest.system_health * 100).toFixed(0)}%`
                  : 'No Data'}
              </div>
            </div>
            <div className={`text-sm font-medium px-3 py-1 rounded-full ${healthBg(latest.health_label)} ${healthColor(latest.health_label)}`}>
              {latest.health_label}
            </div>
          </div>
        </motion.div>
      )}

      {/* Metric cards */}
      {latest && latest.metrics.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {latest.metrics.map((m, i) => (
            <motion.div
              key={m.metric_type}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.1 }}
              className="glass-strong rounded-xl p-4"
            >
              <div className="text-xs text-neutral uppercase tracking-wider mb-2">
                {metricLabel(m.metric_type)}
              </div>
              <div className="flex items-end justify-between">
                <div>
                  {m.sufficient_data && m.value !== null ? (
                    <>
                      <span className={`text-xl font-bold font-mono-numbers ${
                        m.value >= 0.6 ? 'text-profit' :
                        m.value >= 0.3 ? 'text-yellow-400' : 'text-loss'
                      }`}>
                        {m.value.toFixed(3)}
                      </span>
                      <span className="text-xs text-neutral ml-2">
                        / 1.000
                      </span>
                    </>
                  ) : (
                    <span className="text-lg text-neutral">Insufficient Data</span>
                  )}
                </div>
                <div className="text-right text-xs text-neutral">
                  {m.details && typeof m.details === 'object' && 'ticks_analyzed' in m.details && (
                    <div>{String(m.details.ticks_analyzed)} ticks</div>
                  )}
                  {m.agent_count != null && m.agent_count > 0 && <div>{m.agent_count} agents</div>}
                </div>
              </div>

              {/* Simple bar visualization */}
              {m.sufficient_data && m.value !== null && (
                <div className="mt-3 h-1.5 rounded-full bg-white/10 overflow-hidden">
                  <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: `${m.value * 100}%` }}
                    transition={{ duration: 0.8, ease: 'easeOut' }}
                    className={`h-full rounded-full ${
                      m.value >= 0.6 ? 'bg-profit' :
                      m.value >= 0.3 ? 'bg-yellow-400' : 'bg-loss'
                    }`}
                  />
                </div>
              )}
            </motion.div>
          ))}
        </div>
      )}

      {/* History table */}
      {history.length > 0 && (
        <div className="glass-strong rounded-xl overflow-hidden">
          <div className="px-4 py-3 border-b border-white/10">
            <span className="text-sm font-medium text-white">Recent History</span>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/10">
                <th className="text-left px-4 py-2 text-neutral font-medium text-xs">Time</th>
                <th className="text-left px-4 py-2 text-neutral font-medium text-xs">Metric</th>
                <th className="text-right px-4 py-2 text-neutral font-medium text-xs">Value</th>
                <th className="text-right px-4 py-2 text-neutral font-medium text-xs">Agents</th>
              </tr>
            </thead>
            <tbody>
              {history.slice(0, 20).map((s, i) => (
                <tr key={i} className="border-b border-white/5 hover:bg-white/5">
                  <td className="px-4 py-2 text-neutral text-xs font-mono">
                    {s.created_at ? s.created_at.slice(0, 19) : '—'}
                  </td>
                  <td className="px-4 py-2 text-white text-xs">
                    {metricLabel(s.metric_type)}
                  </td>
                  <td className="px-4 py-2 text-right font-mono-numbers">
                    {s.value !== null && s.sufficient_data ? (
                      <span className={
                        s.value >= 0.6 ? 'text-profit' :
                        s.value >= 0.3 ? 'text-yellow-400' : 'text-loss'
                      }>
                        {s.value.toFixed(3)}
                      </span>
                    ) : (
                      <span className="text-neutral">—</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-right text-neutral text-xs">
                    {s.agent_count || '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!latest && !loading && (
        <div className="glass-strong rounded-xl p-8 text-center text-neutral">
          No contagion data available. Run analysis to generate metrics.
        </div>
      )}
    </div>
  );
}
