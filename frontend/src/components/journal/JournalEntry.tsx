import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import DOMPurify from 'dompurify';

interface JournalEntryProps {
  entry: {
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
  };
}

function MetricCard({ label, value, subtext }: { label: string; value: string; subtext?: string }) {
  return (
    <div className="glass rounded-lg p-3">
      <div className="text-xs text-neutral">{label}</div>
      <div className="text-lg font-bold text-white font-mono-numbers">{value}</div>
      {subtext && <div className="text-xs text-neutral mt-0.5">{subtext}</div>}
    </div>
  );
}

function safeHtml(text: string): string {
  return DOMPurify.sanitize(formatMarkdown(text));
}

function formatMarkdown(text: string): string {
  // Simple markdown formatting for rendering
  return text
    // Bold
    .replace(/\*\*(.*?)\*\*/g, '<strong class="text-white font-semibold">$1</strong>')
    // Tables - convert markdown tables to HTML
    .replace(/\|(.+)\|\n\|[-| ]+\|\n((?:\|.+\|\n?)*)/g, (_match, header: string, body: string) => {
      const headers = header.split('|').filter((h: string) => h.trim()).map((h: string) =>
        `<th class="px-3 py-1.5 text-left text-xs font-medium text-neutral">${h.trim()}</th>`
      ).join('');
      const rows = body.trim().split('\n').map((row: string) => {
        const cells = row.split('|').filter((c: string) => c.trim()).map((c: string) =>
          `<td class="px-3 py-1.5 text-sm text-white/80">${c.trim()}</td>`
        ).join('');
        return `<tr class="border-t border-white/5">${cells}</tr>`;
      }).join('');
      return `<div class="overflow-x-auto my-3"><table class="w-full"><thead><tr>${headers}</tr></thead><tbody>${rows}</tbody></table></div>`;
    })
    // Code blocks
    .replace(/`([^`]+)`/g, '<code class="px-1.5 py-0.5 rounded bg-white/10 text-accent text-xs font-mono">$1</code>')
    // Lists
    .replace(/^- (.+)$/gm, '<li class="ml-4 text-white/80">$1</li>')
    // Paragraphs (double newline)
    .replace(/\n\n/g, '</p><p class="mt-3 text-white/80 leading-relaxed">')
    // Single newlines in non-list context
    .replace(/\n/g, '<br/>');
}

export default function JournalEntryView({ entry }: JournalEntryProps) {
  const [expandedAgents, setExpandedAgents] = useState<Set<string>>(new Set());
  const metrics = entry.metrics as Record<string, unknown> | undefined;
  const agentStats = (metrics?.agent_stats || {}) as Record<string, {
    trade_count?: number;
    win_rate?: number;
    pnl?: number;
    total_decisions?: number;
    overtrading_score?: number;
  }>;

  const toggleAgent = (agentId: string) => {
    setExpandedAgents(prev => {
      const next = new Set(prev);
      if (next.has(agentId)) next.delete(agentId);
      else next.add(agentId);
      return next;
    });
  };

  // Compute summary metrics
  const totalTrades = Object.values(agentStats).reduce((s, a) => s + (a.trade_count || 0), 0);
  const totalDecisions = Object.values(agentStats).reduce((s, a) => s + (a.total_decisions || 0), 0);
  const avgWinRate = Object.values(agentStats).length > 0
    ? Object.values(agentStats).reduce((s, a) => s + (a.win_rate || 0), 0) / Object.values(agentStats).length
    : 0;
  return (
    <div className="glass-panel rounded-xl overflow-hidden">
      {/* Header bar */}
      <div className="p-6 border-b border-white/5">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-2xl font-bold text-white">
              Journal — {entry.journal_date}
            </h2>
            <p className="text-sm text-neutral mt-1">
              Generated {new Date(entry.generated_at).toLocaleString()}
              {entry.model && ` · ${entry.model}`}
              {` · ${entry.lookback_hours}h lookback`}
            </p>
          </div>
          {entry.full_markdown && (
            <button
              onClick={() => {
                const lines: string[] = [];

                // Frontmatter-style header
                lines.push('---');
                lines.push(`date: ${entry.journal_date}`);
                lines.push(`generated_at: ${entry.generated_at}`);
                lines.push(`lookback_hours: ${entry.lookback_hours}`);
                if (entry.model) lines.push(`model: ${entry.model}`);
                lines.push('---');
                lines.push('');

                // Metric summary
                const agentCount = Object.keys(agentStats).length;
                lines.push(`> **Agents:** ${agentCount} · **Decisions:** ${totalDecisions} · **Trades:** ${totalTrades} · **Avg Win Rate:** ${(avgWinRate * 100).toFixed(1)}%`);
                lines.push('');

                // Clean full_markdown: replace <!-- AGENT: id --> markers with ### headings
                const cleaned = entry.full_markdown!
                  .replace(/<!-- AGENT: (\S+) -->/g, '### $1')
                  .trim();

                lines.push(cleaned);

                const blob = new Blob([lines.join('\n')], { type: 'text/markdown' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `journal-${entry.journal_date}.md`;
                a.click();
                URL.revokeObjectURL(url);
              }}
              title="Download as Markdown"
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm text-neutral hover:text-white hover:bg-white/10 transition-colors border border-white/10"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>
              .md
            </button>
          )}
        </div>

        {/* Metric summary cards */}
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          <MetricCard
            label="Agents"
            value={String(Object.keys(agentStats).length)}
          />
          <MetricCard
            label="Decisions"
            value={String(totalDecisions)}
            subtext={`${totalTrades} trades`}
          />
          <MetricCard
            label="Avg Win Rate"
            value={`${(avgWinRate * 100).toFixed(1)}%`}
          />
        </div>
      </div>

      {/* Article body */}
      <div className="p-6">
        {entry.full_markdown ? (
          <article className="prose-journal space-y-6">
            {/* Render full markdown as sections */}
            {entry.full_markdown.split(/(?=^## )/m).map((section, i) => {
              if (!section.trim()) return null;

              const isAgentReport = section.includes('<!-- AGENT:');
              const headingMatch = section.match(/^## (.+)/);
              const heading = headingMatch ? headingMatch[1] : '';
              const body = headingMatch ? section.slice(headingMatch[0].length) : section;

              return (
                <div key={heading || `section-${i}`} className="border-b border-white/5 pb-6 last:border-0">
                  {heading && (
                    <h3 className="text-lg font-bold text-white mb-3">{heading}</h3>
                  )}

                  {isAgentReport && entry.agent_reports ? (
                    // Render agent reports as collapsible cards
                    <div className="space-y-2">
                      <p
                        className="text-white/80 leading-relaxed"
                        dangerouslySetInnerHTML={{ __html: safeHtml(
                          body.replace(/<!-- AGENT: \S+ -->[\s\S]*$/m, '').trim()
                        )}}
                      />
                      {Object.entries(entry.agent_reports).map(([agentId, report]) => (
                        <div key={agentId} className="glass rounded-lg overflow-hidden">
                          <button
                            onClick={() => toggleAgent(agentId)}
                            className="w-full flex items-center justify-between px-4 py-3 text-left hover:bg-white/5 transition-colors"
                          >
                            <div className="flex items-center gap-3">
                              <span className="text-sm font-medium text-white">{agentId}</span>
                              {agentStats[agentId] && (
                                <span className="text-xs text-neutral">
                                  {agentStats[agentId].trade_count || 0} trades
                                  {' · '}
                                  <span className={(agentStats[agentId].pnl || 0) >= 0 ? 'text-profit' : 'text-loss'}>
                                    ${(agentStats[agentId].pnl || 0).toFixed(2)}
                                  </span>
                                </span>
                              )}
                            </div>
                            <svg
                              className={`w-4 h-4 text-neutral transition-transform ${expandedAgents.has(agentId) ? 'rotate-180' : ''}`}
                              fill="none" viewBox="0 0 24 24" stroke="currentColor"
                            >
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                            </svg>
                          </button>
                          <AnimatePresence>
                            {expandedAgents.has(agentId) && (
                              <motion.div
                                initial={{ height: 0, opacity: 0 }}
                                animate={{ height: 'auto', opacity: 1 }}
                                exit={{ height: 0, opacity: 0 }}
                                transition={{ duration: 0.2 }}
                                className="overflow-hidden"
                              >
                                <div className="px-4 pb-3 border-t border-white/5 pt-3">
                                  <p
                                    className="text-sm text-white/80 leading-relaxed"
                                    dangerouslySetInnerHTML={{ __html: safeHtml(report) }}
                                  />
                                </div>
                              </motion.div>
                            )}
                          </AnimatePresence>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p
                      className="text-white/80 leading-relaxed"
                      dangerouslySetInnerHTML={{ __html: safeHtml(body.trim()) }}
                    />
                  )}
                </div>
              );
            })}
          </article>
        ) : (
          <p className="text-neutral text-center py-8">No article content available</p>
        )}
      </div>
    </div>
  );
}
