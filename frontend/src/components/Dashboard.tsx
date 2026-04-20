import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useCompetitionStore } from '../stores/competition';
import { useAdminStore } from '../stores/admin';
import { MAINTENANCE_MODE } from '../config';
import clsx from 'clsx';
import Header from './Header';
import MarketBar from './MarketBar';
import CompetitionBanner from './CompetitionBanner';
import Leaderboard from './Leaderboard';
import EquityCurve from './EquityCurve';
import ReasoningFeed from './ReasoningFeed';
import ActivityHighlights from './ActivityHighlights';
import HistoryView from './HistoryView';
import FundingFeed from './FundingFeed';
import LiquidationHistory, { LiquidationToastContainer } from './LiquidationAlert';
import MarketView from './MarketView';
import ForumView from './ForumView';
import SkillsView from './SkillsView';
import { BacktestView } from './backtest';
import { EvolutionView } from './evolution';
import { ExperimentView } from './experiment';
import { MemoryView } from './memory';
import { ReflexionView } from './reflexion';
// DEACTIVATED: lab view — implementation preserved
// import { LabView } from './lab';
import { JournalView } from './journal';
import AboutView from './AboutView';
import ErrorBoundary from './ErrorBoundary';

type TabType = 'live' | 'market' | 'forum' | 'skills' | 'history' | 'journal' | 'backtest' | 'evolution' | 'experiment' | 'memory' | 'reflexion' | 'about';

export default function Dashboard() {
  const { connected, status, tick } = useCompetitionStore();
  const { getHeaders } = useAdminStore();
  const [activeTab, setActiveTab] = useState<TabType>('live');

  return (
    <div className="min-h-screen flex flex-col bg-gradient-radial-subtle">
      {/* Liquidation toast notifications */}
      <LiquidationToastContainer />

      {/* Header */}
      <Header />

      {/* Market ticker bar */}
      <MarketBar />

      {/* Tab navigation */}
      <div className="glass-subtle border-b border-white/5">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 flex gap-1 overflow-x-auto scrollbar-hide">
          {([
            ['live', 'Live Feed'],
            ['market', 'Market'],
            ['forum', 'Forum'],
            ['skills', 'Skills'],
            ['history', 'History'],
            ['journal', 'Journal'],
            ['backtest', 'Backtest'],
            ['evolution', 'Evolution'],
            ['experiment', 'Experiment'],
            ['memory', 'Memory'],
            ['reflexion', 'Reflexion'],
            ['about', 'About'],
          ] as [TabType, string][]).map(([tab, label]) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={clsx(
                'py-3 px-4 font-medium text-sm transition-all relative whitespace-nowrap',
                activeTab === tab
                  ? 'text-white'
                  : 'text-neutral hover:text-white'
              )}
            >
              {label}
              {activeTab === tab && (
                <motion.div
                  layoutId="tab-indicator"
                  className="absolute bottom-0 left-0 right-0 h-0.5 bg-accent"
                  transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                />
              )}
            </button>
          ))}
          {/* DEACTIVATED: Lab tab — component preserved */}
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 p-4 sm:p-6">
        <div className="max-w-7xl mx-auto">
          {!MAINTENANCE_MODE && !connected && (
            <div className="mb-6 p-4 glass-strong rounded-xl text-center text-neutral animate-pulse-slow">
              <div className="flex items-center justify-center gap-3">
                <div className="w-2 h-2 bg-accent rounded-full animate-ping" />
                Connecting to server...
              </div>
            </div>
          )}

          {!MAINTENANCE_MODE && connected && status === 'not_started' && (
            <div className="mb-6 p-6 glass-strong rounded-xl text-center">
              <p className="text-neutral mb-4">Competition not started</p>
              <button
                onClick={async () => {
                  await fetch('/api/start', { method: 'POST', headers: getHeaders() });
                }}
                className="px-6 py-3 bg-accent hover:bg-accent/80 rounded-lg font-medium transition-all hover:scale-105 hover:shadow-glow"
              >
                Start Competition
              </button>
            </div>
          )}

          {/* Competition Banner - only show on live tab when not in maintenance */}
          {activeTab === 'live' && !MAINTENANCE_MODE && <CompetitionBanner />}

          {/* Tab content */}
          <AnimatePresence mode="wait">
            {activeTab === 'live' && MAINTENANCE_MODE ? (
              <motion.div
                key="maintenance"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <div className="glass-strong rounded-xl p-8 sm:p-12 text-center max-w-2xl mx-auto mt-8">
                  <div className="w-16 h-16 mx-auto mb-6 rounded-2xl bg-accent/20 border border-accent/30 flex items-center justify-center">
                    <svg xmlns="http://www.w3.org/2000/svg" className="w-8 h-8 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M11.42 15.17l-5.13-5.12A7.003 7.003 0 0112 5a7.003 7.003 0 015.71 5.05M11.42 15.17l2.83 2.83m-2.83-2.83l-2.83 2.83M12 19a7 7 0 100-14 7 7 0 000 14z" />
                    </svg>
                  </div>
                  <h2 className="text-2xl font-bold text-white mb-3">
                    System Upgrade in Progress
                  </h2>
                  <p className="text-neutral mb-6 leading-relaxed">
                    The inference server is offline while we transition to a new model architecture.
                    Live trading, reasoning feeds, and forum discussions are temporarily unavailable.
                  </p>
                  <div className="glass rounded-lg p-4 mb-6 text-left space-y-2">
                    <div className="flex items-center gap-3 text-sm">
                      <span className="w-2 h-2 rounded-full bg-accent animate-pulse" />
                      <span className="text-white/80">Upgrading inference backend</span>
                    </div>
                    <div className="flex items-center gap-3 text-sm">
                      <span className="w-2 h-2 rounded-full bg-neutral/40" />
                      <span className="text-neutral">Deploying Eagle3 120B (MoE) with speculative decoding</span>
                    </div>
                    <div className="flex items-center gap-3 text-sm">
                      <span className="w-2 h-2 rounded-full bg-neutral/40" />
                      <span className="text-neutral">Reconfiguring agent pipeline</span>
                    </div>
                  </div>
                  <p className="text-xs text-neutral/60">
                    Historical data is still available in the History tab.
                  </p>
                </div>
              </motion.div>
            ) : activeTab === 'live' ? (
              <motion.div
                key="live"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                {/* Dashboard grid */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-6">
                  {/* Left column - Leaderboard */}
                  <div className="lg:col-span-1 space-y-4">
                    <Leaderboard />
                  </div>

                  {/* Right column - Charts and Feed */}
                  <div className="lg:col-span-2 space-y-4 sm:space-y-6">
                    {/* Equity curves */}
                    <EquityCurve />

                    {/* Live reasoning feed */}
                    <ReasoningFeed />

                    {/* Activity Highlights */}
                    <ActivityHighlights />

                    {/* Funding and Liquidation feeds */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <FundingFeed />
                      <LiquidationHistory />
                    </div>
                  </div>
                </div>
              </motion.div>
            ) : activeTab === 'market' ? (
              <motion.div
                key="market"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <MarketView />
              </motion.div>
            ) : activeTab === 'forum' ? (
              <motion.div
                key="forum"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <ForumView />
              </motion.div>
            ) : activeTab === 'skills' ? (
              <motion.div
                key="skills"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <SkillsView />
              </motion.div>
            ) : activeTab === 'history' ? (
              <motion.div
                key="history"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                {/* History grid */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-6">
                  {/* Left column - Leaderboard */}
                  <div className="lg:col-span-1">
                    <Leaderboard />
                  </div>

                  {/* Right column - History */}
                  <div className="lg:col-span-2">
                    <HistoryView />
                  </div>
                </div>
              </motion.div>
            ) : activeTab === 'journal' ? (
              <motion.div
                key="journal"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <ErrorBoundary>
                  <JournalView />
                </ErrorBoundary>
              </motion.div>
            ) : activeTab === 'backtest' ? (
              <motion.div
                key="backtest"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <ErrorBoundary>
                  <BacktestView />
                </ErrorBoundary>
              </motion.div>
            ) : activeTab === 'evolution' ? (
              <motion.div
                key="evolution"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <ErrorBoundary>
                  <EvolutionView />
                </ErrorBoundary>
              </motion.div>
            ) : activeTab === 'experiment' ? (
              <motion.div
                key="experiment"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <ErrorBoundary>
                  <ExperimentView />
                </ErrorBoundary>
              </motion.div>
            ) : activeTab === 'memory' ? (
              <motion.div
                key="memory"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <ErrorBoundary>
                  <MemoryView />
                </ErrorBoundary>
              </motion.div>
            ) : activeTab === 'reflexion' ? (
              <motion.div
                key="reflexion"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <ErrorBoundary>
                  <ReflexionView />
                </ErrorBoundary>
              </motion.div>
            ) : activeTab === 'about' ? (
              <motion.div
                key="about"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <AboutView />
              </motion.div>
            ) : null}
          </AnimatePresence>
        </div>
      </div>

      {/* Footer status */}
      <div className="glass-subtle border-t border-white/5 p-4">
        <div className="max-w-7xl mx-auto flex items-center justify-between text-sm text-neutral">
          <div className="flex items-center gap-4">
            <span
              className={`flex items-center gap-2 ${
                connected ? 'text-profit' : 'text-loss'
              }`}
            >
              <span
                className={`w-2 h-2 rounded-full ${
                  connected ? 'bg-profit animate-pulse-slow' : 'bg-loss'
                }`}
              />
              {connected ? 'Connected' : 'Disconnected'}
            </span>
            <span className="hidden sm:inline">Status: {status}</span>
          </div>
          <div>
            <span className="font-mono-numbers">Tick {tick}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
