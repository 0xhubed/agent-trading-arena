import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import clsx from 'clsx';
import DataManager from './DataManager';
import BacktestConfig from './BacktestConfig';
import BacktestResults from './BacktestResults';
import { useAdminStore } from '../../stores/admin';
import type { BacktestTab, BacktestRun } from '../../types/backtest';

export default function BacktestView() {
  const [activeTab, setActiveTab] = useState<BacktestTab>('results');
  const [runs, setRuns] = useState<BacktestRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [readonlyMode, setReadonlyMode] = useState(false);
  const { getHeaders } = useAdminStore();

  // Check access mode and fetch past backtest runs
  // Re-check when admin key changes
  const adminKey = useAdminStore((s) => s.adminKey);
  useEffect(() => {
    checkAccess();
    fetchRuns();
  }, [adminKey]);

  const checkAccess = async () => {
    try {
      const response = await fetch('/api/backtest/access', {
        headers: getHeaders(),
      });
      if (response.ok) {
        const data = await response.json();
        setReadonlyMode(data.readonly);
        // Default to results tab in readonly mode
        if (data.readonly) {
          setActiveTab('results');
        }
      }
    } catch (error) {
      console.error('Failed to check access mode:', error);
    }
  };

  const fetchRuns = async () => {
    try {
      const response = await fetch('/api/backtest/runs');
      if (response.ok) {
        const data = await response.json();
        setRuns(data.runs || []);
      }
    } catch (error) {
      console.error('Failed to fetch backtest runs:', error);
    }
  };

  const handleRunComplete = (runId: string) => {
    fetchRuns();
    setSelectedRunId(runId);
    setActiveTab('results');
  };

  // In readonly mode, only show the Results tab
  const allTabs: { id: BacktestTab; label: string; icon: string }[] = [
    { id: 'data', label: 'Data', icon: 'M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4' },
    { id: 'configure', label: 'Configure', icon: 'M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z M15 12a3 3 0 11-6 0 3 3 0 016 0z' },
    { id: 'results', label: 'Results', icon: 'M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z' },
  ];

  const tabs = readonlyMode
    ? allTabs.filter((t) => t.id === 'results')
    : allTabs;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Backtesting</h1>
          <p className="text-neutral text-sm mt-1">
            Test trading strategies on historical data
          </p>
        </div>
        {runs.length > 0 && (
          <div className="text-sm text-neutral">
            {runs.length} backtest{runs.length !== 1 ? 's' : ''} completed
          </div>
        )}
      </div>

      {/* Readonly mode banner */}
      {readonlyMode && (
        <div className="glass-subtle rounded-xl p-4 border border-accent/30 flex items-center gap-3">
          <svg className="w-5 h-5 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span className="text-neutral">
            <span className="text-white font-medium">View-only mode.</span>{' '}
            You can browse backtest results, but running new backtests requires admin access.
          </span>
        </div>
      )}

      {/* Tab navigation */}
      <div className="glass-strong rounded-xl p-1 flex gap-1">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={clsx(
              'flex-1 flex items-center justify-center gap-2 py-3 px-4 rounded-lg font-medium text-sm transition-all',
              activeTab === tab.id
                ? 'bg-accent text-white'
                : 'text-neutral hover:text-white hover:bg-white/5'
            )}
          >
            <svg
              className="w-5 h-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.5}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d={tab.icon} />
            </svg>
            {tab.label}
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
          {activeTab === 'data' && <DataManager />}
          {activeTab === 'configure' && (
            <BacktestConfig
              onRunComplete={handleRunComplete}
            />
          )}
          {activeTab === 'results' && (
            <BacktestResults
              runs={runs}
              selectedRunId={selectedRunId}
              onSelectRun={setSelectedRunId}
              onRefresh={fetchRuns}
            />
          )}
        </motion.div>
      </AnimatePresence>
    </div>
  );
}
