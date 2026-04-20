import { useState, useEffect } from 'react';

interface Reflection {
  id: number;
  agent_id: string;
  symbol: string;
  side: string;
  realized_pnl: number;
  outcome: string;
  lesson: string;
  market_regime: string;
  confidence: number;
  metabolic_score: number;
  created_at: string;
}

interface FailureCluster {
  id: number;
  cluster_label: string;
  regime: string;
  failure_mode: string;
  sample_size: number;
  proposed_skill: string;
  created_at: string;
}

interface SkillProposal {
  id: number;
  skill_name: string;
  status: string;
  improvement_pct: number;
  created_at: string;
}

export function ReflexionView() {
  const [reflections, setReflections] = useState<Reflection[]>([]);
  const [clusters, setClusters] = useState<FailureCluster[]>([]);
  const [proposals, setProposals] = useState<SkillProposal[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch('/api/reflexion/reflections?limit=20').then(r => r.json()),
      fetch('/api/reflexion/clusters').then(r => r.json()),
      fetch('/api/reflexion/proposals').then(r => r.json()),
    ])
      .then(([reflData, clusterData, proposalData]) => {
        setReflections(reflData.reflections || []);
        setClusters(clusterData.clusters || []);
        setProposals(proposalData.proposals || []);
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div className="text-neutral text-center py-8">Loading reflexion data...</div>;
  }

  return (
    <div className="space-y-6">
      {/* Trade Reflections */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Trade Reflections</h2>
        {reflections.length === 0 ? (
          <p className="text-neutral text-sm">No reflections yet. They are generated during the daily analysis cycle.</p>
        ) : (
          <div className="space-y-3">
            {reflections.map(r => (
              <div key={r.id} className="glass rounded-lg p-4">
                <div className="flex items-center gap-3 mb-2">
                  <span className={`px-2 py-0.5 rounded text-xs ${
                    r.outcome === 'win' ? 'bg-profit/20 text-profit' : 'bg-loss/20 text-loss'
                  }`}>
                    {r.outcome}
                  </span>
                  <span className="text-white font-medium">{r.symbol} {r.side}</span>
                  <span className={`font-mono-numbers ${r.realized_pnl >= 0 ? 'text-profit' : 'text-loss'}`}>
                    ${r.realized_pnl.toFixed(2)}
                  </span>
                  <span className="text-neutral text-xs">{r.agent_id}</span>
                </div>
                {r.lesson && (
                  <div className="text-sm text-white/80">{r.lesson}</div>
                )}
                <div className="flex gap-3 mt-2 text-xs text-neutral">
                  {r.market_regime && <span>Regime: {r.market_regime}</span>}
                  {r.confidence > 0 && <span>Confidence: {(r.confidence * 100).toFixed(0)}%</span>}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Failure Clusters */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Failure Clusters</h2>
        {clusters.length === 0 ? (
          <p className="text-neutral text-sm">No failure patterns detected yet.</p>
        ) : (
          <div className="space-y-3">
            {clusters.map(c => (
              <div key={c.id} className="glass rounded-lg p-4">
                <div className="flex items-center gap-3 mb-2">
                  <span className="text-white font-medium">{c.cluster_label}</span>
                  {c.regime && (
                    <span className="px-2 py-0.5 bg-accent/10 rounded text-xs text-accent">{c.regime}</span>
                  )}
                  <span className="text-neutral text-xs">{c.sample_size} trades</span>
                </div>
                {c.failure_mode && (
                  <div className="text-sm text-loss/80 mb-1">{c.failure_mode}</div>
                )}
                {c.proposed_skill && (
                  <div className="text-sm text-profit/80">Rule: {c.proposed_skill}</div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Skill Proposals */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Skill Proposals</h2>
        {proposals.length === 0 ? (
          <p className="text-neutral text-sm">No skill proposals yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-neutral border-b border-white/10">
                  <th className="text-left py-2 px-3">Skill</th>
                  <th className="text-left py-2 px-3">Status</th>
                  <th className="text-right py-2 px-3">Improvement</th>
                  <th className="text-left py-2 px-3">Date</th>
                </tr>
              </thead>
              <tbody>
                {proposals.map(p => (
                  <tr key={p.id} className="border-b border-white/5">
                    <td className="py-2 px-3 text-white">{p.skill_name}</td>
                    <td className="py-2 px-3">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        p.status === 'promoted' ? 'bg-profit/20 text-profit' :
                        'bg-accent/20 text-accent'
                      }`}>
                        {p.status}
                      </span>
                    </td>
                    <td className="py-2 px-3 text-right font-mono-numbers">
                      {p.improvement_pct ? `${p.improvement_pct.toFixed(1)}%` : '—'}
                    </td>
                    <td className="py-2 px-3 text-neutral text-xs">
                      {p.created_at ? new Date(p.created_at).toLocaleDateString() : ''}
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

export default ReflexionView;
