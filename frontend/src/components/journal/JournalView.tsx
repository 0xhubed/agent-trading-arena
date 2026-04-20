import { useState, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import JournalEntryView from './JournalEntry';

interface JournalEntryData {
  id: string;
  journal_date: string;
  generated_at: string;
  lookback_hours: number;
  full_markdown?: string;
  market_summary?: string;
  forum_summary?: string;
  learning_summary?: string;
  recommendations?: string;
  agent_reports?: Record<string, string>;
  metrics?: Record<string, unknown>;
  model?: string;
}

export default function JournalView() {
  const [entries, setEntries] = useState<JournalEntryData[]>([]);
  const [selectedEntry, setSelectedEntry] = useState<JournalEntryData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchEntries = useCallback(async () => {
    try {
      const res = await fetch('/api/journal/entries?limit=30');
      if (!res.ok) throw new Error('Failed to fetch entries');
      const data = await res.json();
      setEntries(data.entries || []);
      setError(null);
    } catch (err) {
      setError('Failed to load journal entries');
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchFullEntry = useCallback(async (date: string) => {
    try {
      const res = await fetch(`/api/journal/entries/${date}`);
      if (!res.ok) throw new Error('Failed to fetch entry');
      const data = await res.json();
      if (data.entry) {
        setSelectedEntry(data.entry);
      }
    } catch {
      setError('Failed to load journal entry');
    }
  }, []);

  useEffect(() => {
    fetchEntries();
  }, [fetchEntries]);

  // Auto-select latest entry
  useEffect(() => {
    if (entries.length > 0 && !selectedEntry) {
      fetchFullEntry(entries[0].journal_date);
    }
  }, [entries, selectedEntry, fetchFullEntry]);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-white">Observer Journal</h2>
          <p className="text-sm text-neutral mt-1">
            Daily diagnostic editorial — critical analysis of agent performance
          </p>
        </div>
      </div>

      {error && (
        <div className="p-3 rounded-lg bg-loss/10 border border-loss/20 text-loss text-sm">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Entry list sidebar */}
        <div className="lg:col-span-1">
          <div className="glass-panel rounded-xl p-4 space-y-2">
            <h3 className="text-sm font-medium text-neutral mb-3">Entries</h3>
            {loading ? (
              <div className="text-sm text-neutral animate-pulse">Loading...</div>
            ) : entries.length === 0 ? (
              <div className="text-sm text-neutral">
                No journal entries yet.
              </div>
            ) : (
              <div className="space-y-1 max-h-[600px] overflow-y-auto">
                {entries.map((entry) => (
                  <button
                    key={entry.id}
                    onClick={() => fetchFullEntry(entry.journal_date)}
                    className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-all ${
                      selectedEntry?.journal_date === entry.journal_date
                        ? 'bg-accent/20 text-accent border border-accent/30'
                        : 'text-neutral hover:text-white hover:bg-white/5'
                    }`}
                  >
                    <div className="font-medium">{entry.journal_date}</div>
                    <div className="text-xs opacity-70 mt-0.5">
                      {entry.lookback_hours}h lookback
                      {entry.model && ` · ${entry.model}`}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Main article view */}
        <div className="lg:col-span-3">
          <AnimatePresence mode="wait">
            {selectedEntry ? (
              <motion.div
                key={selectedEntry.journal_date}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <JournalEntryView entry={selectedEntry} />
              </motion.div>
            ) : (
              <motion.div
                key="empty"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="glass-panel rounded-xl p-12 text-center"
              >
                <p className="text-neutral">
                  {loading ? 'Loading...' : 'Select a journal entry to read'}
                </p>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}
