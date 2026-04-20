import { useState, useEffect } from 'react';

interface MemoryHealth {
  total_reflections: number;
  digested_reflections: number;
  active_principles: number;
}

interface Principle {
  id: number;
  agent_id: string;
  principle: string;
  regime: string;
  confidence: number;
  application_count: number;
  created_at: string;
}

interface DigestionEntry {
  agent_id: string;
  memories_scored: number;
  memories_digested: number;
  memories_pruned: number;
  principles_created: number;
  created_at: string;
}

export function MemoryView() {
  const [health, setHealth] = useState<MemoryHealth | null>(null);
  const [principles, setPrinciples] = useState<Principle[]>([]);
  const [digestionHistory, setDigestionHistory] = useState<DigestionEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch('/api/memory/health').then(r => r.json()),
      fetch('/api/memory/principles').then(r => r.json()),
      fetch('/api/memory/digestion').then(r => r.json()),
    ])
      .then(([healthData, principlesData, digestionData]) => {
        setHealth(healthData);
        setPrinciples(principlesData.principles || []);
        setDigestionHistory(digestionData.history || []);
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div className="text-neutral text-center py-8">Loading memory data...</div>;
  }

  return (
    <div className="space-y-6">
      {/* Health Overview */}
      {health && (
        <div className="grid grid-cols-3 gap-4">
          <div className="glass-strong rounded-xl p-4 text-center">
            <div className="text-2xl font-bold text-white">{health.total_reflections}</div>
            <div className="text-neutral text-sm">Trade Reflections</div>
          </div>
          <div className="glass-strong rounded-xl p-4 text-center">
            <div className="text-2xl font-bold text-accent">{health.digested_reflections}</div>
            <div className="text-neutral text-sm">Digested</div>
          </div>
          <div className="glass-strong rounded-xl p-4 text-center">
            <div className="text-2xl font-bold text-profit">{health.active_principles}</div>
            <div className="text-neutral text-sm">Active Principles</div>
          </div>
        </div>
      )}

      {/* Active Principles */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Active Principles</h2>
        {principles.length === 0 ? (
          <p className="text-neutral text-sm">No principles yet. They emerge after memory digestion.</p>
        ) : (
          <div className="space-y-3">
            {principles.map(p => (
              <div key={p.id} className="glass rounded-lg p-4">
                <div className="text-white">{p.principle}</div>
                <div className="flex gap-4 mt-2 text-xs text-neutral">
                  {p.regime && <span className="px-2 py-0.5 bg-accent/10 rounded">{p.regime}</span>}
                  <span>Confidence: {(p.confidence * 100).toFixed(0)}%</span>
                  <span>Applied {p.application_count}x</span>
                  <span className="text-neutral/60">{p.agent_id}</span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Digestion History */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Digestion History</h2>
        {digestionHistory.length === 0 ? (
          <p className="text-neutral text-sm">No digestion runs yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-neutral border-b border-white/10">
                  <th className="text-left py-2 px-3">Agent</th>
                  <th className="text-right py-2 px-3">Scored</th>
                  <th className="text-right py-2 px-3">Digested</th>
                  <th className="text-right py-2 px-3">Pruned</th>
                  <th className="text-right py-2 px-3">Principles</th>
                  <th className="text-left py-2 px-3">Date</th>
                </tr>
              </thead>
              <tbody>
                {digestionHistory.map((entry, i) => (
                  <tr key={i} className="border-b border-white/5">
                    <td className="py-2 px-3 text-white">{entry.agent_id}</td>
                    <td className="py-2 px-3 text-right">{entry.memories_scored}</td>
                    <td className="py-2 px-3 text-right text-accent">{entry.memories_digested}</td>
                    <td className="py-2 px-3 text-right text-loss">{entry.memories_pruned}</td>
                    <td className="py-2 px-3 text-right text-profit">{entry.principles_created}</td>
                    <td className="py-2 px-3 text-neutral text-xs">
                      {entry.created_at ? new Date(entry.created_at).toLocaleDateString() : ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export default MemoryView;
