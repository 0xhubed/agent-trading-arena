import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { useAdminStore } from '../../stores/admin';
import type { BiasProfile } from '../../types/lab';

interface AgentBias {
  agent_id: string;
  biases: Record<string, BiasProfile>;
}

export default function BiasPanel() {
  const { getHeaders } = useAdminStore();
  const [agents, setAgents] = useState<AgentBias[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchProfiles();
  }, []);

  const fetchProfiles = async () => {
    try {
      setLoading(true);
      setError(null);
      const res = await fetch('/api/lab/bias/profiles');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      // Group profiles by agent_id
      const byAgent: Record<string, Record<string, BiasProfile>> = {};
      for (const p of data.profiles) {
        if (!byAgent[p.agent_id]) byAgent[p.agent_id] = {};
        byAgent[p.agent_id][p.bias_type] = p;
      }

      const agentList = Object.entries(byAgent).map(([agent_id, biases]) => ({
        agent_id,
        biases,
      }));
      agentList.sort((a, b) => a.agent_id.localeCompare(b.agent_id));
      setAgents(agentList);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch');
    } finally {
      setLoading(false);
    }
  };

  const triggerAnalysis = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/lab/bias/analyze', {
        method: 'POST',
        headers: getHeaders(),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const result = await res.json();
      if (result.status === 'skipped') {
        setError(result.message);
      }
      await fetchProfiles();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Analysis failed');
      setLoading(false);
    }
  };

  const scoreColor = (score: number | null, sufficient: boolean) => {
    if (!sufficient || score === null) return 'text-neutral';
    if (score < 0.3) return 'text-profit';
    if (score < 0.6) return 'text-yellow-400';
    return 'text-loss';
  };

  const scoreLabel = (score: number | null, sufficient: boolean) => {
    if (!sufficient || score === null) return 'N/A';
    if (score < 0.3) return 'LOW';
    if (score < 0.6) return 'MOD';
    return 'HIGH';
  };

  if (loading && agents.length === 0) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral animate-pulse-slow">
        Loading bias profiles...
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-white">Agent Bias Profiles</h3>
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

      {agents.length === 0 && !loading ? (
        <div className="glass-strong rounded-xl p-8 text-center text-neutral">
          No bias data available. Run analysis to generate profiles.
        </div>
      ) : (
        <div className="glass-strong rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/10">
                <th className="text-left px-4 py-3 text-neutral font-medium">Agent</th>
                <th className="text-center px-4 py-3 text-neutral font-medium">Disposition</th>
                <th className="text-center px-4 py-3 text-neutral font-medium">Loss Aversion</th>
                <th className="text-center px-4 py-3 text-neutral font-medium">Overconfidence</th>
              </tr>
            </thead>
            <tbody>
              {agents.map((agent, i) => (
                <motion.tr
                  key={agent.agent_id}
                  initial={{ opacity: 0, y: 5 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: i * 0.05 }}
                  className="border-b border-white/5 hover:bg-white/5 transition-colors"
                >
                  <td className="px-4 py-3 font-mono text-white text-xs">
                    {agent.agent_id}
                  </td>
                  {['disposition_effect', 'loss_aversion', 'overconfidence'].map((bias) => {
                    const p = agent.biases[bias];
                    const score = p?.score ?? null;
                    const sufficient = p?.sufficient_data ?? false;
                    return (
                      <td key={bias} className="text-center px-4 py-3">
                        <div className="flex flex-col items-center gap-0.5">
                          <span className={`font-mono-numbers text-sm ${scoreColor(score, sufficient)}`}>
                            {score !== null && sufficient ? score.toFixed(2) : '—'}
                          </span>
                          <span className={`text-[10px] uppercase ${scoreColor(score, sufficient)}`}>
                            {scoreLabel(score, sufficient)}
                          </span>
                          {p && (
                            <span className="text-[10px] text-neutral">
                              n={p.sample_size}
                            </span>
                          )}
                        </div>
                      </td>
                    );
                  })}
                </motion.tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
