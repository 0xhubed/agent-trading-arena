import { useState } from 'react';
import { useCompetitionStore } from '../stores/competition';
import { useAdminStore } from '../stores/admin';
import { MAINTENANCE_MODE } from '../config';

export default function Header() {
  const { status, tick, connected } = useCompetitionStore();
  const { adminKey, isAuthenticated, setAdminKey, setAuthenticated } = useAdminStore();
  const [showAdmin, setShowAdmin] = useState(false);
  const [adminError, setAdminError] = useState('');
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [analysisResult, setAnalysisResult] = useState<string | null>(null);
  const [journalResult, setJournalResult] = useState<string | null>(null);

  const handleAdminAuth = async () => {
    try {
      const res = await fetch('/api/admin/access', {
        headers: { 'X-Admin-Key': adminKey },
      });
      if (res.ok) {
        const data = await res.json();
        if (!data.readonly) {
          setAuthenticated(true);
          setAdminError('');
        } else {
          setAdminError('Invalid admin key');
        }
      } else {
        setAdminError('Invalid admin key');
      }
    } catch {
      setAdminError('Failed to verify admin key');
    }
  };

  const handleAdminAction = async (action: 'start' | 'stop' | 'reset') => {
    setActionLoading(action);
    const headers: Record<string, string> = { 'X-Admin-Key': adminKey };
    try {
      if (action === 'reset') {
        await fetch('/api/stop', { method: 'POST', headers });
        await fetch('/api/reset?confirm=true', { method: 'POST', headers });
        await fetch('/api/start', { method: 'POST', headers });
      } else {
        await fetch(`/api/${action}`, { method: 'POST', headers });
      }
      window.location.reload();
    } catch (err) {
      console.error('Admin action failed:', err);
    } finally {
      setActionLoading(null);
    }
  };

  const handleGenerateJournal = async () => {
    setActionLoading('journal');
    setJournalResult(null);
    try {
      const response = await fetch('/api/journal/generate', {
        method: 'POST',
        headers: { 'X-Admin-Key': adminKey },
      });
      if (response.ok) {
        const data = await response.json();
        if (data.status === 'in_progress') {
          setJournalResult('Journal generation already in progress');
        } else {
          setJournalResult(`Journal generated for ${data.entry_id || 'today'}`);
        }
      } else {
        setJournalResult('Journal generation failed');
      }
    } catch (err) {
      console.error('Journal generation failed:', err);
      setJournalResult('Journal generation failed');
    } finally {
      setActionLoading(null);
    }
  };

  const handleRunAnalysis = async () => {
    setActionLoading('analysis');
    setAnalysisResult(null);
    try {
      const response = await fetch('/api/observer/analyze', { method: 'POST', headers: { 'X-Admin-Key': adminKey } });
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
            </div>

            {/* Status indicators */}
            <div className="flex items-center gap-4 sm:gap-6">
              {/* Live indicator */}
              <div className="flex items-center gap-2">
                {MAINTENANCE_MODE ? (
                  <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-amber-500/10 border border-amber-500/20">
                    <span className="w-2.5 h-2.5 rounded-full bg-amber-500 animate-pulse"></span>
                    <span className="text-amber-400 font-medium text-sm">UPGRADING</span>
                  </div>
                ) : (
                  <>
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
                  </>
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
                  placeholder="Enter admin key (ARENA_ADMIN_KEY)"
                  value={adminKey}
                  onChange={(e) => setAdminKey(e.target.value)}
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
                  <button
                    onClick={handleGenerateJournal}
                    disabled={actionLoading !== null}
                    className="w-full mt-2 px-4 py-2 bg-blue-500/20 hover:bg-blue-500/30 border border-blue-500/30 rounded-lg text-blue-400 font-medium disabled:opacity-50 transition-colors"
                  >
                    {actionLoading === 'journal' ? 'Generating...' : 'Generate Journal'}
                  </button>
                  {journalResult && (
                    <p className="text-xs text-blue-300 mt-2 text-center">{journalResult}</p>
                  )}
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => {
                      setShowAdmin(false);
                      setAnalysisResult(null);
                      setJournalResult(null);
                    }}
                    className="flex-1 px-4 py-2 bg-white/10 hover:bg-white/20 rounded-lg text-white/70 font-medium transition-colors"
                  >
                    Close
                  </button>
                  <button
                    onClick={() => {
                      setShowAdmin(false);
                      setAuthenticated(false);
                      setAdminKey('');
                      setAnalysisResult(null);
                      setJournalResult(null);
                    }}
                    className="px-4 py-2 bg-red-500/10 hover:bg-red-500/20 border border-red-500/20 rounded-lg text-red-400/70 text-sm font-medium transition-colors"
                  >
                    Logout
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
