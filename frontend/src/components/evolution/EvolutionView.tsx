import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import clsx from 'clsx';
import EvolutionRuns from './EvolutionRuns';
import EvolutionConfig from './EvolutionConfig';
import FitnessCurve from './FitnessCurve';
import GenomeInspector from './GenomeInspector';
import PopulationHeatmap from './PopulationHeatmap';
import FamilyTree from './FamilyTree';
import ParallelCoordinates from './ParallelCoordinates';
import DiversityMonitor from './DiversityMonitor';
import EvolutionFeed from './EvolutionFeed';
import type { EvolutionTab, EvolutionRun } from '../../types/evolution';

const allTabs: { id: EvolutionTab; label: string; icon: string }[] = [
  {
    id: 'runs',
    label: 'Runs',
    icon: 'M9.75 3.104v5.714a2.25 2.25 0 01-.659 1.591L5 14.5M9.75 3.104c-.251.023-.501.05-.75.082m.75-.082a24.301 24.301 0 014.5 0m0 0v5.714a2.25 2.25 0 00.659 1.591L19 14.5M14.25 3.104c.251.023.501.05.75.082M19 14.5l-2.47 2.47a2.25 2.25 0 01-1.59.659H9.06a2.25 2.25 0 01-1.591-.659L5 14.5m14 0V19a2 2 0 01-2 2H7a2 2 0 01-2-2v-4.5',
  },
  {
    id: 'fitness',
    label: 'Fitness',
    icon: 'M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z',
  },
  {
    id: 'genome',
    label: 'Genome',
    icon: 'M9.348 14.651a3.75 3.75 0 010-5.303m5.304 0a3.75 3.75 0 010 5.303m-7.425 2.122a6.75 6.75 0 010-9.546m9.546 0a6.75 6.75 0 010 9.546M5.106 18.894c-3.808-3.808-3.808-9.98 0-13.789m13.788 0c3.808 3.808 3.808 9.981 0 13.79M12 12h.008v.007H12V12zm.375 0a.375.375 0 11-.75 0 .375.375 0 01.75 0z',
  },
  {
    id: 'lineage',
    label: 'Lineage',
    // tree/branch icon
    icon: 'M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5',
  },
  {
    id: 'parameters',
    label: 'Params',
    // sliders icon
    icon: 'M10.5 6h9.75M10.5 6a1.5 1.5 0 11-3 0m3 0a1.5 1.5 0 10-3 0M3.75 6H7.5m3 12h9.75m-9.75 0a1.5 1.5 0 01-3 0m3 0a1.5 1.5 0 00-3 0m-3.75 0H7.5m9-6h3.75m-3.75 0a1.5 1.5 0 01-3 0m3 0a1.5 1.5 0 00-3 0m-9.75 0h9.75',
  },
  {
    id: 'diversity',
    label: 'Diversity',
    // sparkles icon
    icon: 'M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456zM16.894 20.567L16.5 21.75l-.394-1.183a2.25 2.25 0 00-1.423-1.423L13.5 18.75l1.183-.394a2.25 2.25 0 001.423-1.423l.394-1.183.394 1.183a2.25 2.25 0 001.423 1.423l1.183.394-1.183.394a2.25 2.25 0 00-1.423 1.423z',
  },
  {
    id: 'feed',
    label: 'Feed',
    // rss/signal icon
    icon: 'M12.75 19.5v-.75a7.5 7.5 0 00-7.5-7.5H4.5m0 0v-.75a11.25 11.25 0 0111.25-11.25h.75m-12 12v.75a.75.75 0 001.5 0v-.75m0 0a3.75 3.75 0 013.75-3.75h.75m-4.5 3.75v.75m0 0H5.625a1.875 1.875 0 01-1.875-1.875v0a1.875 1.875 0 011.875-1.875h.75',
  },
];

export default function EvolutionView() {
  const [activeTab, setActiveTab] = useState<EvolutionTab>('runs');
  const [runs, setRuns] = useState<EvolutionRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [showConfig, setShowConfig] = useState(false);

  useEffect(() => {
    fetchRuns();
  }, []);

  const fetchRuns = async () => {
    try {
      setFetchError(null);
      const response = await fetch('/api/evolution/runs');
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data = await response.json();
      setRuns(data.runs || []);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to fetch runs';
      setFetchError(message);
      console.error('Failed to fetch evolution runs:', error);
    }
  };

  const handleSelectRun = (runId: string) => {
    setSelectedRunId(runId);
    setActiveTab('fitness');
  };

  // Get current run's generation count for heatmap
  const selectedRun = runs.find((r) => r.run_id === selectedRunId);
  const totalGenerations = selectedRun?.current_generation ?? 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Evolution</h1>
          <p className="text-neutral text-sm mt-1">
            Genetic algorithm optimization of trading agent parameters
          </p>
        </div>
        <div className="flex items-center gap-3">
          {runs.length > 0 && (
            <span className="text-sm text-neutral">
              {runs.length} run{runs.length !== 1 ? 's' : ''}
            </span>
          )}
          <button
            onClick={() => setShowConfig(true)}
            className="px-4 py-2 bg-accent hover:bg-accent/80 rounded-lg text-sm font-medium transition-all"
          >
            New Run
          </button>
        </div>
      </div>

      {fetchError && (
        <div className="glass-strong rounded-xl p-4 bg-red-500/10 border border-red-500/20">
          <p className="text-red-400 text-sm">Failed to load runs: {fetchError}</p>
        </div>
      )}

      {/* Evolution Config Form */}
      <AnimatePresence>
        {showConfig && (
          <EvolutionConfig
            onRunStarted={() => {
              fetchRuns();
              setShowConfig(false);
            }}
            onClose={() => setShowConfig(false)}
          />
        )}
      </AnimatePresence>

      {/* Tab navigation */}
      <div className="glass-strong rounded-xl p-1 flex gap-1 overflow-x-auto">
        {allTabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={clsx(
              'flex-1 flex items-center justify-center gap-2 py-3 px-3 rounded-lg font-medium text-sm transition-all whitespace-nowrap min-w-0',
              activeTab === tab.id
                ? 'bg-accent text-white'
                : 'text-neutral hover:text-white hover:bg-white/5'
            )}
          >
            <svg
              className="w-4 h-4 flex-shrink-0"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d={tab.icon} />
            </svg>
            <span className="hidden sm:inline">{tab.label}</span>
          </button>
        ))}
      </div>

      {/* Tab content */}
      <AnimatePresence mode="wait">
        <motion.div
          key={activeTab}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -10 }}
          transition={{ duration: 0.2 }}
        >
          {activeTab === 'runs' && (
            <EvolutionRuns
              runs={runs}
              selectedRunId={selectedRunId}
              onSelectRun={handleSelectRun}
              onRefresh={fetchRuns}
              onStartNew={() => setShowConfig(true)}
            />
          )}
          {activeTab === 'fitness' && (
            <div className="space-y-6">
              <FitnessCurve runId={selectedRunId} />
              <PopulationHeatmap runId={selectedRunId} totalGenerations={totalGenerations} />
            </div>
          )}
          {activeTab === 'genome' && (
            <GenomeInspector runId={selectedRunId} />
          )}
          {activeTab === 'lineage' && (
            <FamilyTree runId={selectedRunId} />
          )}
          {activeTab === 'parameters' && (
            <ParallelCoordinates runId={selectedRunId} totalGenerations={totalGenerations} />
          )}
          {activeTab === 'diversity' && (
            <DiversityMonitor runId={selectedRunId} />
          )}
          {activeTab === 'feed' && (
            <EvolutionFeed runId={selectedRunId} />
          )}
        </motion.div>
      </AnimatePresence>
    </div>
  );
}
