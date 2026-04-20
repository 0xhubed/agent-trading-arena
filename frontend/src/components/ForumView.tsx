import { useState, useEffect } from 'react';
import clsx from 'clsx';
import ForumLog from './forum/ForumLog';

interface WitnessSummary {
  id: number;
  witness_type: string;
  insight: string;
  confidence: number;
  symbols: string[];
  timeframe: string | null;
  based_on: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
  valid_until: string | null;
}

type Section = 'messages' | 'witness';

export default function ForumView() {
  const [section, setSection] = useState<Section>('messages');
  const [summaries, setSummaries] = useState<WitnessSummary[]>([]);
  const [loadingWitness, setLoadingWitness] = useState(false);

  useEffect(() => {
    if (section === 'witness') {
      fetchWitnessSummaries();
    }
  }, [section]);

  async function fetchWitnessSummaries() {
    try {
      setLoadingWitness(true);
      const res = await fetch('/api/forum/witness');
      if (res.ok) {
        const data = await res.json();
        setSummaries(data.summaries || []);
      }
    } catch (err) {
      console.error('Failed to fetch witness summaries:', err);
    } finally {
      setLoadingWitness(false);
    }
  }

  function formatTimestamp(ts: string): string {
    const date = new Date(ts);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
    if (diffHours < 1) return 'just now';
    if (diffHours < 24) return `${diffHours}h ago`;
    return date.toLocaleDateString();
  }

  function typeColor(type: string): string {
    switch (type) {
      case 'exit_timing': return 'bg-loss/20 text-loss';
      case 'entry_signal': return 'bg-profit/20 text-profit';
      case 'risk_warning': return 'bg-orange-400/20 text-orange-400';
      case 'regime_insight': return 'bg-purple-400/20 text-purple-400';
      default: return 'bg-accent/20 text-accent';
    }
  }

  return (
    <div className="space-y-4">
      {/* Section toggle */}
      <div className="flex gap-2">
        <button
          onClick={() => setSection('messages')}
          className={clsx(
            'px-4 py-2 rounded-lg text-sm font-medium transition-all',
            section === 'messages'
              ? 'bg-accent text-white'
              : 'bg-surface/50 text-neutral hover:text-white hover:bg-surface'
          )}
        >
          Messages
        </button>
        <button
          onClick={() => setSection('witness')}
          className={clsx(
            'px-4 py-2 rounded-lg text-sm font-medium transition-all',
            section === 'witness'
              ? 'bg-accent text-white'
              : 'bg-surface/50 text-neutral hover:text-white hover:bg-surface'
          )}
        >
          Witness Summaries
        </button>
      </div>

      {/* Content */}
      {section === 'messages' ? (
        <ForumLog />
      ) : (
        <div className="glass-strong rounded-xl p-6">
          <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-purple-400" />
            Observer Witness Summaries
          </h3>
          {loadingWitness ? (
            <div className="flex items-center justify-center h-32 text-neutral">
              <div className="flex items-center gap-2">
                <div className="w-4 h-4 border-2 border-accent border-t-transparent rounded-full animate-spin" />
                Loading summaries...
              </div>
            </div>
          ) : summaries.length === 0 ? (
            <div className="text-center text-neutral/70 py-8">
              No witness summaries yet. The Observer generates these during forum analysis.
            </div>
          ) : (
            <div className="space-y-3">
              {summaries.map((summary) => (
                <div
                  key={summary.id}
                  className="p-4 bg-surface/30 rounded-lg border border-white/5"
                >
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className={clsx(
                        'text-xs font-medium px-2 py-0.5 rounded-full',
                        typeColor(summary.witness_type)
                      )}>
                        {summary.witness_type.replace('_', ' ')}
                      </span>
                      {summary.symbols.length > 0 && (
                        <span className="text-xs text-neutral/60">
                          {summary.symbols.join(', ')}
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-3 text-xs text-neutral">
                      <span className="font-mono-numbers">
                        {(summary.confidence * 100).toFixed(0)}% conf
                      </span>
                      <span>{formatTimestamp(summary.created_at)}</span>
                    </div>
                  </div>
                  <p className="text-sm text-neutral/80 leading-relaxed whitespace-pre-wrap">
                    {summary.insight}
                  </p>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
