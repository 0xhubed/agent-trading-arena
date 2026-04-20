import { useState, useEffect } from 'react';
import clsx from 'clsx';

interface SkillInfo {
  name: string;
  path: string;
  size_bytes: number;
  description?: string;
  last_updated?: string;
  patterns_count?: number;
}

interface ObserverStatus {
  status: string;
  last_analysis?: string;
  analysis_count?: number;
  recent_analyses?: Array<{
    timestamp: string;
    patterns_found: number;
    skills_updated: string[];
  }>;
  message?: string;
}

interface SkillsResponse {
  skills_dir: string;
  count: number;
  skills: SkillInfo[];
}

export default function SkillsView() {
  const [observerStatus, setObserverStatus] = useState<ObserverStatus | null>(null);
  const [skills, setSkills] = useState<SkillInfo[]>([]);
  const [skillContents, setSkillContents] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchData(true);
    const interval = setInterval(() => fetchData(false), 300_000);
    return () => clearInterval(interval);
  }, []);

  async function fetchData(showLoading = true) {
    try {
      if (showLoading) setLoading(true);
      const [statusRes, skillsRes] = await Promise.all([
        fetch('/api/observer/status'),
        fetch('/api/skills'),
      ]);

      if (statusRes.ok) {
        setObserverStatus(await statusRes.json());
      }

      if (skillsRes.ok) {
        const data: SkillsResponse = await skillsRes.json();
        setSkills(data.skills);

        // Eagerly load all skill contents in parallel
        const contentPromises = data.skills.map(async (skill) => {
          try {
            const res = await fetch(`/api/skills/${skill.name}`);
            if (res.ok) {
              const body = await res.json();
              return [skill.name, body.content] as [string, string];
            }
          } catch {
            // ignore individual failures
          }
          return [skill.name, ''] as [string, string];
        });

        const results = await Promise.all(contentPromises);
        const contents: Record<string, string> = {};
        for (const [name, content] of results) {
          contents[name] = content;
        }
        setSkillContents(contents);
      }
    } catch (err) {
      console.error('Failed to fetch skills data:', err);
    } finally {
      setLoading(false);
    }
  }

  const lastAnalysisTime = observerStatus?.last_analysis
    ? new Date(observerStatus.last_analysis)
    : null;

  const timeSinceAnalysis = lastAnalysisTime
    ? Math.floor((Date.now() - lastAnalysisTime.getTime()) / (1000 * 60 * 60))
    : null;

  function formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    return `${(bytes / 1024).toFixed(1)} KB`;
  }

  return (
    <div className="space-y-6">
      {/* Header with Observer status */}
      <div className="glass-strong rounded-xl p-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center">
            <svg className="w-4 h-4 text-purple-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
            </svg>
          </div>
          <div>
            <h2 className="text-sm font-semibold text-white">Observer Agent</h2>
            <p className="text-xs text-neutral">
              {loading ? 'Loading...' : `${skills.length} skills generated`}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span
            className={clsx(
              'w-2 h-2 rounded-full',
              observerStatus?.status === 'initialized' ? 'bg-profit' : 'bg-neutral'
            )}
          />
          <span className="text-xs text-neutral">
            {observerStatus?.status === 'initialized' ? 'Active' : 'Not initialized'}
          </span>
          {lastAnalysisTime && (
            <span className="text-xs text-neutral/60">
              {timeSinceAnalysis !== null && timeSinceAnalysis < 24
                ? `${timeSinceAnalysis}h ago`
                : lastAnalysisTime.toLocaleDateString()}
            </span>
          )}
        </div>
      </div>

      {/* Recent analyses strip */}
      {observerStatus?.recent_analyses && observerStatus.recent_analyses.length > 0 && (
        <div className="overflow-x-auto scrollbar-hide">
          <div className="flex gap-3 min-w-max">
            {observerStatus.recent_analyses.map((analysis, i) => (
              <div
                key={i}
                className="glass rounded-lg px-4 py-2 flex items-center gap-3 text-xs shrink-0"
              >
                <span className="text-neutral">
                  {new Date(analysis.timestamp).toLocaleString([], {
                    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
                  })}
                </span>
                <span className="text-white font-medium">
                  {analysis.patterns_found} patterns
                </span>
                {analysis.skills_updated.length > 0 && (
                  <span className="text-purple-400">
                    {analysis.skills_updated.length} skills updated
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Skills grid */}
      {loading ? (
        <div className="flex items-center justify-center h-48 text-neutral">
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 border-2 border-accent border-t-transparent rounded-full animate-spin" />
            Loading skills...
          </div>
        </div>
      ) : skills.length === 0 ? (
        <div className="glass-strong rounded-xl p-8 text-center text-neutral/70">
          No skills generated yet. Run Observer analysis from the Admin Panel to create skills.
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {skills.map((skill) => (
            <div
              key={skill.name}
              className="glass-strong rounded-xl overflow-hidden"
            >
              {/* Skill card header */}
              <div className="p-4 border-b border-white/5">
                <div className="flex items-center justify-between mb-1">
                  <h3 className="text-sm font-semibold text-white">{skill.name}</h3>
                  <span className="text-xs text-neutral/60 font-mono-numbers">
                    {formatBytes(skill.size_bytes)}
                  </span>
                </div>
                {skill.description && (
                  <p className="text-xs text-neutral/80 mb-2">{skill.description}</p>
                )}
                <div className="flex items-center gap-3 text-xs text-neutral/60">
                  {skill.patterns_count !== undefined && (
                    <span>{skill.patterns_count} patterns</span>
                  )}
                  {skill.last_updated && (
                    <span>Updated {new Date(skill.last_updated).toLocaleDateString()}</span>
                  )}
                </div>
              </div>

              {/* Skill content */}
              <div className="p-4 max-h-96 overflow-y-auto custom-scrollbar">
                {skillContents[skill.name] ? (
                  <pre className="text-xs text-neutral/90 whitespace-pre-wrap font-mono leading-relaxed">
                    {skillContents[skill.name]}
                  </pre>
                ) : (
                  <p className="text-xs text-neutral/50">Content unavailable</p>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Info footer */}
      <p className="text-xs text-neutral/50 text-center">
        Skills are auto-generated by the Observer Agent from competition analysis. Skill-aware traders consume these at runtime.
      </p>
    </div>
  );
}
