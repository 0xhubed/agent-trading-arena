import { motion } from 'framer-motion';
import clsx from 'clsx';
import type { EvolutionRun } from '../../types/evolution';

interface Props {
  runs: EvolutionRun[];
  selectedRunId: string | null;
  onSelectRun: (runId: string) => void;
  onRefresh: () => void;
  onStartNew: () => void;
}

const statusConfig: Record<string, { badge: string; dot: string; label: string; pulse?: boolean }> = {
  running: { badge: 'bg-yellow-400/20 text-yellow-400', dot: 'bg-yellow-400', label: 'Running', pulse: true },
  completed: { badge: 'bg-emerald-400/20 text-emerald-400', dot: 'bg-emerald-400', label: 'Completed' },
  cancelled: { badge: 'bg-red-400/20 text-red-400', dot: 'bg-red-400', label: 'Cancelled' },
};

export default function EvolutionRuns({ runs, selectedRunId, onSelectRun, onRefresh, onStartNew }: Props) {
  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  };

  if (runs.length === 0) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center">
        <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-white/5 flex items-center justify-center">
          <svg className="w-8 h-8 text-neutral" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 3.104v5.714a2.25 2.25 0 01-.659 1.591L5 14.5M9.75 3.104c-.251.023-.501.05-.75.082m.75-.082a24.301 24.301 0 014.5 0m0 0v5.714a2.25 2.25 0 00.659 1.591L19 14.5M14.25 3.104c.251.023.501.05.75.082M19 14.5l-2.47 2.47a2.25 2.25 0 01-1.59.659H9.06a2.25 2.25 0 01-1.591-.659L5 14.5m14 0V19a2 2 0 01-2 2H7a2 2 0 01-2-2v-4.5" />
          </svg>
        </div>
        <p className="text-neutral mb-4">No evolution runs yet</p>
        <button
          onClick={onStartNew}
          className="px-5 py-2.5 bg-accent hover:bg-accent/80 rounded-lg text-sm font-medium transition-all"
        >
          Start New Run
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-neutral">{runs.length} run{runs.length !== 1 ? 's' : ''}</p>
        <div className="flex items-center gap-2">
          <button
            onClick={onRefresh}
            className="px-3 py-1 text-sm text-neutral hover:text-white transition-colors"
          >
            Refresh
          </button>
          <button
            onClick={onStartNew}
            className="px-3 py-1 text-sm bg-accent hover:bg-accent/80 rounded-lg font-medium transition-all"
          >
            New Run
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {runs.map((run, idx) => {
          const sc = statusConfig[run.status] || statusConfig.cancelled;
          const progress = run.max_generations > 0
            ? (run.current_generation / run.max_generations) * 100
            : 0;
          const isSelected = selectedRunId === run.run_id;

          return (
            <motion.button
              key={run.run_id}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: idx * 0.05 }}
              onClick={() => onSelectRun(run.run_id)}
              className={clsx(
                'glass-strong rounded-xl p-5 text-left transition-all hover:bg-white/5',
                isSelected && 'ring-2 ring-accent'
              )}
            >
              {/* Header */}
              <div className="flex items-start justify-between mb-3">
                <h3 className="font-medium text-white truncate pr-2">{run.name}</h3>
                <span className={clsx(
                  'flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium whitespace-nowrap',
                  sc.badge
                )}>
                  {sc.pulse && (
                    <span className={clsx('w-1.5 h-1.5 rounded-full animate-pulse', sc.dot)} />
                  )}
                  {sc.label}
                </span>
              </div>

              {/* Progress bar */}
              <div className="mb-3">
                <div className="flex justify-between text-xs text-neutral mb-1">
                  <span>Generation {run.current_generation} / {run.max_generations}</span>
                  <span className="font-mono-numbers">{Math.round(progress)}%</span>
                </div>
                <div className="h-1.5 bg-white/5 rounded-full overflow-hidden">
                  <div
                    className={clsx(
                      'h-full rounded-full transition-all',
                      run.status === 'running' ? 'bg-yellow-400' : 'bg-accent'
                    )}
                    style={{ width: `${progress}%` }}
                  />
                </div>
              </div>

              {/* Stats */}
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <div className="text-neutral text-xs">Population</div>
                  <div className="font-mono-numbers text-white">{run.population_size}</div>
                </div>
                <div>
                  <div className="text-neutral text-xs">Best Fitness</div>
                  <div className="font-mono-numbers text-white">
                    {run.best_fitness != null ? run.best_fitness.toFixed(4) : '-'}
                  </div>
                </div>
              </div>

              {/* Date */}
              <div className="mt-3 pt-3 border-t border-white/5 text-xs text-neutral">
                {formatDate(run.created_at)}
              </div>
            </motion.button>
          );
        })}
      </div>
    </div>
  );
}
