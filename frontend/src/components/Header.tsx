import { useState } from 'react';
import { useCompetitionStore } from '../stores/competition';

// Admin password - set via environment variable VITE_ADMIN_PASSWORD
const ADMIN_PASSWORD = import.meta.env.VITE_ADMIN_PASSWORD || 'change-me-in-env';

export default function Header() {
  const { status, tick, connected } = useCompetitionStore();
  const [showAdmin, setShowAdmin] = useState(false);
  const [showInfo, setShowInfo] = useState(false);
  const [password, setPassword] = useState('');
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [adminError, setAdminError] = useState('');
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [analysisResult, setAnalysisResult] = useState<string | null>(null);

  const handleAdminAuth = () => {
    if (password === ADMIN_PASSWORD) {
      setIsAuthenticated(true);
      setAdminError('');
    } else {
      setAdminError('Invalid password');
    }
  };

  const handleAdminAction = async (action: 'start' | 'stop' | 'reset') => {
    setActionLoading(action);
    try {
      if (action === 'reset') {
        await fetch('/api/stop', { method: 'POST' });
        await fetch('/api/reset?confirm=true', { method: 'POST' });
        await fetch('/api/start', { method: 'POST' });
      } else {
        await fetch(`/api/${action}`, { method: 'POST' });
      }
      window.location.reload();
    } catch (err) {
      console.error('Admin action failed:', err);
    } finally {
      setActionLoading(null);
    }
  };

  const handleRunAnalysis = async () => {
    setActionLoading('analysis');
    setAnalysisResult(null);
    try {
      const response = await fetch('/api/observer/analyze', { method: 'POST' });
      if (response.ok) {
        const data = await response.json();
        setAnalysisResult(`Found ${data.patterns_found || 0} patterns, updated ${data.skills_updated?.length || 0} skills`);
      } else {
        setAnalysisResult('Analysis failed');
      }
    } catch (err) {
      console.error('Analysis failed:', err);
      setAnalysisResult('Analysis failed');
    } finally {
      setActionLoading(null);
    }
  };

  return (
    <>
      <header className="glass-subtle border-b border-white/5">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-3 sm:py-4">
          <div className="flex flex-col sm:flex-row items-center justify-between gap-3 sm:gap-0">
            {/* Title */}
            <div className="flex items-center gap-3">
              <div className="flex flex-col">
                <div className="flex items-baseline gap-2">
                  <h1
                    className="text-xl sm:text-2xl font-bold text-white tracking-tight cursor-pointer hover:text-accent transition-colors"
                    onClick={() => setShowAdmin(true)}
                  >
                    AGENT ARENA
                  </h1>
                  <span className="text-[10px] sm:text-xs text-neutral/50 font-normal tracking-normal">
                    created by Daniel Huber
                  </span>
                </div>
                <span className="text-[10px] sm:text-xs text-neutral/40 font-normal">
                  Built for research and entertainment. Not financial advice!
                </span>
              </div>
              <button
                onClick={() => setShowInfo(true)}
                className="w-7 h-7 rounded-full bg-white/10 hover:bg-white/20 border border-white/20 flex items-center justify-center text-neutral hover:text-white transition-all"
                title="About Agent Arena"
              >
                <svg xmlns="http://www.w3.org/2000/svg" className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </button>
            </div>

            {/* Status indicators */}
            <div className="flex items-center gap-4 sm:gap-6">
              {/* Live indicator */}
              <div className="flex items-center gap-2">
                {status === 'running' && connected && (
                  <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-profit/10 border border-profit/20">
                    <span className="relative flex h-2.5 w-2.5">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-profit opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-profit"></span>
                    </span>
                    <span className="text-profit font-medium text-sm text-glow-profit">LIVE</span>
                  </div>
                )}
                {status === 'stopped' && (
                  <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-neutral/10 border border-neutral/20">
                    <span className="w-2.5 h-2.5 rounded-full bg-neutral"></span>
                    <span className="text-neutral font-medium text-sm">STOPPED</span>
                  </div>
                )}
                {status === 'not_started' && (
                  <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-neutral/10 border border-neutral/20">
                    <span className="w-2.5 h-2.5 rounded-full bg-neutral animate-pulse"></span>
                    <span className="text-neutral font-medium text-sm">WAITING</span>
                  </div>
                )}
              </div>

              {/* Tick counter */}
              <div className="text-right glass rounded-lg px-4 py-2">
                <div className="text-xs text-neutral uppercase tracking-wider">Tick</div>
                <div className="font-mono-numbers text-xl font-bold text-white">{tick}</div>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Info Modal */}
      {showInfo && (
        <div
          className="fixed inset-0 bg-black/90 flex items-center justify-center"
          style={{ zIndex: 9999 }}
          onClick={() => setShowInfo(false)}
        >
          <div
            className="bg-[#1a1a2e] border border-white/20 rounded-xl p-6 max-w-lg w-full mx-4 shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 rounded-lg bg-accent/20 flex items-center justify-center">
                <svg xmlns="http://www.w3.org/2000/svg" className="w-6 h-6 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <h2 className="text-xl font-bold text-white">Agent Arena</h2>
            </div>

            <p className="text-neutral mb-4">
              A self-improving AI platform. An Observer Agent watches AI traders compete, figures out what works, and writes down the winning patterns as reusable skills.
            </p>

            <div className="space-y-3 text-sm">
              <div className="flex items-start gap-3">
                <span className="text-accent">1.</span>
                <p className="text-white/80">
                  <strong className="text-white">Observer Agent</strong> - Analyzes thousands of trading decisions, correlates with outcomes, and writes structured skills with statistical confidence.
                </p>
              </div>
              <div className="flex items-start gap-3">
                <span className="text-accent">2.</span>
                <p className="text-white/80">
                  <strong className="text-white">Skill Evolution</strong> - Patterns are versioned, confirmed or contradicted over time, and refined as more data arrives.
                </p>
              </div>
              <div className="flex items-start gap-3">
                <span className="text-accent">3.</span>
                <p className="text-white/80">
                  <strong className="text-white">Data Generation</strong> - LLM traders (Claude, GPT-4, Llama) compete on live Binance futures, providing continuous decision/outcome data.
                </p>
              </div>
              <div className="flex items-start gap-3">
                <span className="text-accent">4.</span>
                <p className="text-white/80">
                  <strong className="text-white">The Lab</strong> - Real market data, 10x leverage, funding rates, and liquidations create a realistic environment for learning.
                </p>
              </div>
            </div>

            <div className="mt-4 p-3 bg-accent/10 border border-accent/20 rounded-lg">
              <p className="text-xs text-accent/90 italic">
                "The trading arena is the lab; the Observer Agent is the scientist."
              </p>
            </div>

            <div className="mt-6 pt-4 border-t border-white/10">
              <p className="text-xs text-neutral text-center">
                Built for research and entertainment. Not financial advice!
              </p>
              <p className="text-xs text-neutral/60 text-center mt-2">
                Created by Daniel Huber
              </p>
            </div>

            <button
              onClick={() => setShowInfo(false)}
              className="mt-4 w-full px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg text-white/70 font-medium transition-colors"
            >
              Close
            </button>
          </div>
        </div>
      )}

      {/* Admin Panel Modal */}
      {showAdmin && (
        <div
          className="fixed inset-0 bg-black/90 flex items-center justify-center"
          style={{ zIndex: 9999 }}
          onClick={() => setShowAdmin(false)}
        >
          <div
            className="bg-[#1a1a2e] border border-white/20 rounded-xl p-6 max-w-sm w-full mx-4 shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <h2 className="text-lg font-semibold text-white mb-4">Admin Panel</h2>
            {!isAuthenticated ? (
              <div>
                <input
                  type="password"
                  placeholder="Enter admin password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleAdminAuth()}
                  className="w-full px-3 py-2 bg-black/50 border border-white/20 rounded-lg text-white mb-3 focus:outline-none focus:border-accent"
                  autoFocus
                />
                {adminError && <p className="text-red-400 text-sm mb-3">{adminError}</p>}
                <button
                  onClick={handleAdminAuth}
                  className="w-full px-4 py-2 bg-accent hover:bg-accent/80 rounded-lg text-white font-medium transition-colors"
                >
                  Authenticate
                </button>
              </div>
            ) : (
              <div className="space-y-3">
                <button
                  onClick={() => handleAdminAction('start')}
                  disabled={actionLoading !== null}
                  className="w-full px-4 py-2 bg-green-500/20 hover:bg-green-500/30 border border-green-500/30 rounded-lg text-green-400 font-medium disabled:opacity-50 transition-colors"
                >
                  {actionLoading === 'start' ? 'Starting...' : 'Start Competition'}
                </button>
                <button
                  onClick={() => handleAdminAction('stop')}
                  disabled={actionLoading !== null}
                  className="w-full px-4 py-2 bg-red-500/20 hover:bg-red-500/30 border border-red-500/30 rounded-lg text-red-400 font-medium disabled:opacity-50 transition-colors"
                >
                  {actionLoading === 'stop' ? 'Stopping...' : 'Stop Competition'}
                </button>
                <button
                  onClick={() => handleAdminAction('reset')}
                  disabled={actionLoading !== null}
                  className="w-full px-4 py-2 bg-amber-500/20 hover:bg-amber-500/30 border border-amber-500/30 rounded-lg text-amber-400 font-medium disabled:opacity-50 transition-colors"
                >
                  {actionLoading === 'reset' ? 'Resetting...' : 'Reset (Delete DB & Restart)'}
                </button>
                <div className="border-t border-white/10 pt-3 mt-3">
                  <button
                    onClick={handleRunAnalysis}
                    disabled={actionLoading !== null}
                    className="w-full px-4 py-2 bg-purple-500/20 hover:bg-purple-500/30 border border-purple-500/30 rounded-lg text-purple-400 font-medium disabled:opacity-50 transition-colors"
                  >
                    {actionLoading === 'analysis' ? 'Analyzing...' : 'Run Observer Analysis'}
                  </button>
                  {analysisResult && (
                    <p className="text-xs text-purple-300 mt-2 text-center">{analysisResult}</p>
                  )}
                </div>
                <button
                  onClick={() => {
                    setShowAdmin(false);
                    setIsAuthenticated(false);
                    setPassword('');
                    setAnalysisResult(null);
                  }}
                  className="w-full px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg text-white/70 font-medium transition-colors"
                >
                  Close
                </button>
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
