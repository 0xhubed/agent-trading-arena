import { useState, useEffect } from 'react';

interface ExperimentRun {
  id: string;
  name: string;
  status: string;
  best_fitness: number | null;
  validation_fitness: number | null;
  overfit_warning: boolean;
  total_cost_usd: number;
  generations_completed: number;
  error: string;
  created_at: string;
}

interface Promotion {
  id: number;
  experiment_id: string;
  fitness: number;
  status: string;
  created_at: string;
}

export function ExperimentView() {
  const [runs, setRuns] = useState<ExperimentRun[]>([]);
  const [promotions, setPromotions] = useState<Promotion[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch('/api/experiment/runs').then(r => r.json()),
      fetch('/api/experiment/promotions').then(r => r.json()),
    ])
      .then(([runsData, promoData]) => {
        setRuns(runsData.runs || []);
        setPromotions(promoData.promotions || []);
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div className="text-neutral text-center py-8">Loading experiments...</div>;
  }

  return (
    <div className="space-y-6">
      {/* Experiment Runs */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Experiment Runs</h2>
        {runs.length === 0 ? (
          <p className="text-neutral text-sm">No experiment runs yet. Start one from the CLI or API.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-neutral border-b border-white/10">
                  <th className="text-left py-2 px-3">Name</th>
                  <th className="text-left py-2 px-3">Status</th>
                  <th className="text-right py-2 px-3">Fitness</th>
                  <th className="text-right py-2 px-3">Val Fitness</th>
                  <th className="text-right py-2 px-3">Cost</th>
                  <th className="text-right py-2 px-3">Gens</th>
                  <th className="text-left py-2 px-3">Date</th>
                </tr>
              </thead>
              <tbody>
                {runs.map(run => (
                  <tr key={run.id} className="border-b border-white/5 hover:bg-white/5">
                    <td className="py-2 px-3 text-white">{run.name}</td>
                    <td className="py-2 px-3">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        run.status === 'completed' ? 'bg-profit/20 text-profit' :
                        run.status === 'running' ? 'bg-accent/20 text-accent' :
                        'bg-loss/20 text-loss'
                      }`}>
                        {run.status}
                        {run.overfit_warning && ' ⚠️'}
                      </span>
                    </td>
                    <td className="py-2 px-3 text-right font-mono-numbers">
                      {run.best_fitness?.toFixed(4) ?? '—'}
                    </td>
                    <td className="py-2 px-3 text-right font-mono-numbers">
                      {run.validation_fitness?.toFixed(4) ?? '—'}
                    </td>
                    <td className="py-2 px-3 text-right font-mono-numbers">
                      ${run.total_cost_usd.toFixed(2)}
                    </td>
                    <td className="py-2 px-3 text-right">{run.generations_completed}</td>
                    <td className="py-2 px-3 text-neutral text-xs">
                      {run.created_at ? new Date(run.created_at).toLocaleDateString() : ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Promotion Queue */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Promotion Queue</h2>
        {promotions.length === 0 ? (
          <p className="text-neutral text-sm">No promotion candidates yet.</p>
        ) : (
          <div className="space-y-3">
            {promotions.map(promo => (
              <div
                key={promo.id}
                className="flex items-center justify-between glass rounded-lg p-4"
              >
                <div>
                  <span className="text-white font-medium">Candidate #{promo.id}</span>
                  <span className="text-neutral text-sm ml-3">
                    Fitness: {promo.fitness?.toFixed(4)}
                  </span>
                  <span className={`ml-3 px-2 py-0.5 rounded text-xs ${
                    promo.status === 'approved' ? 'bg-profit/20 text-profit' :
                    promo.status === 'pending' ? 'bg-accent/20 text-accent' :
                    promo.status === 'deployed' ? 'bg-profit/30 text-profit' :
                    'bg-loss/20 text-loss'
                  }`}>
                    {promo.status}
                  </span>
                </div>
                {promo.status === 'pending' && (
                  <div className="flex gap-2">
                    <button
                      onClick={() => fetch(`/api/experiment/promotions/${promo.id}/approve`, { method: 'POST' }).then(() => window.location.reload())}
                      className="px-3 py-1 bg-profit/20 hover:bg-profit/30 text-profit rounded text-sm"
                    >
                      Approve
                    </button>
                    <button
                      onClick={() => fetch(`/api/experiment/promotions/${promo.id}/reject`, { method: 'POST' }).then(() => window.location.reload())}
                      className="px-3 py-1 bg-loss/20 hover:bg-loss/30 text-loss rounded text-sm"
                    >
                      Reject
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default ExperimentView;
