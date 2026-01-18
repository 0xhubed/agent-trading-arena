import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useCompetitionStore } from '../stores/competition';
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
import ObserverPanel from './ObserverPanel';
import MarketHistory from './MarketHistory';

type TabType = 'live' | 'history';

export default function Dashboard() {
  const { connected, status, tick } = useCompetitionStore();
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
        <div className="max-w-7xl mx-auto px-4 sm:px-6 flex gap-1">
          <button
            onClick={() => setActiveTab('live')}
            className={clsx(
              'py-3 px-4 font-medium text-sm transition-all relative',
              activeTab === 'live'
                ? 'text-white'
                : 'text-neutral hover:text-white'
            )}
          >
            Live Feed
            {activeTab === 'live' && (
              <motion.div
                layoutId="tab-indicator"
                className="absolute bottom-0 left-0 right-0 h-0.5 bg-accent"
                transition={{ type: 'spring', stiffness: 500, damping: 30 }}
              />
            )}
          </button>
          <button
            onClick={() => setActiveTab('history')}
            className={clsx(
              'py-3 px-4 font-medium text-sm transition-all relative',
              activeTab === 'history'
                ? 'text-white'
                : 'text-neutral hover:text-white'
            )}
          >
            History
            {activeTab === 'history' && (
              <motion.div
                layoutId="tab-indicator"
                className="absolute bottom-0 left-0 right-0 h-0.5 bg-accent"
                transition={{ type: 'spring', stiffness: 500, damping: 30 }}
              />
            )}
          </button>
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 p-4 sm:p-6">
        <div className="max-w-7xl mx-auto">
          {!connected && (
            <div className="mb-6 p-4 glass-strong rounded-xl text-center text-neutral animate-pulse-slow">
              <div className="flex items-center justify-center gap-3">
                <div className="w-2 h-2 bg-accent rounded-full animate-ping" />
                Connecting to server...
              </div>
            </div>
          )}

          {connected && status === 'not_started' && (
            <div className="mb-6 p-6 glass-strong rounded-xl text-center">
              <p className="text-neutral mb-4">Competition not started</p>
              <button
                onClick={async () => {
                  await fetch('/api/start', { method: 'POST' });
                }}
                className="px-6 py-3 bg-accent hover:bg-accent/80 rounded-lg font-medium transition-all hover:scale-105 hover:shadow-glow"
              >
                Start Competition
              </button>
            </div>
          )}

          {/* Competition Banner - only show on live tab */}
          {activeTab === 'live' && <CompetitionBanner />}

          {/* Tab content */}
          <AnimatePresence mode="wait">
            {activeTab === 'live' ? (
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

                    {/* Market data history */}
                    <MarketHistory />

                    {/* Live reasoning feed */}
                    <ReasoningFeed />

                    {/* Activity Highlights and Observer */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <ActivityHighlights />
                      <ObserverPanel />
                    </div>

                    {/* Funding and Liquidation feeds */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <FundingFeed />
                      <LiquidationHistory />
                    </div>
                  </div>
                </div>
              </motion.div>
            ) : (
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
            )}
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
