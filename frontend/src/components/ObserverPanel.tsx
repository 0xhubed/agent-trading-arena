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

export default function ObserverPanel() {
  const [observerStatus, setObserverStatus] = useState<ObserverStatus | null>(null);
  const [skills, setSkills] = useState<SkillInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState(false);
  const [selectedSkill, setSelectedSkill] = useState<string | null>(null);
  const [skillContent, setSkillContent] = useState<string>('');

  useEffect(() => {
    fetchData();
    // Refresh every 5 minutes
    const interval = setInterval(fetchData, 300000);
    return () => clearInterval(interval);
  }, []);

  async function fetchData() {
    try {
      setLoading(true);
      const [statusRes, skillsRes] = await Promise.all([
        fetch('/api/observer/status'),
        fetch('/api/skills'),
      ]);

      if (statusRes.ok) {
        const status = await statusRes.json();
        setObserverStatus(status);
      }

      if (skillsRes.ok) {
        const data: SkillsResponse = await skillsRes.json();
        setSkills(data.skills);
      }
    } catch (err) {
      console.error('Failed to fetch observer data:', err);
    } finally {
      setLoading(false);
    }
  }

  async function viewSkill(skillName: string) {
    if (selectedSkill === skillName) {
      setSelectedSkill(null);
      setSkillContent('');
      return;
    }

    try {
      const response = await fetch(`/api/skills/${skillName}`);
      if (response.ok) {
        const data = await response.json();
        setSkillContent(data.content);
        setSelectedSkill(skillName);
      }
    } catch (err) {
      console.error('Failed to fetch skill:', err);
    }
  }

  const lastAnalysisTime = observerStatus?.last_analysis
    ? new Date(observerStatus.last_analysis)
    : null;

  const timeSinceAnalysis = lastAnalysisTime
    ? Math.floor((Date.now() - lastAnalysisTime.getTime()) / (1000 * 60 * 60))
    : null;

  return (
    <div className="glass-strong rounded-xl overflow-hidden">
      {/* Header - always visible */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full p-4 flex items-center justify-between hover:bg-surface/30 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center">
            <svg className="w-4 h-4 text-purple-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
            </svg>
          </div>
          <div className="text-left">
            <h3 className="text-sm font-semibold text-white">Observer Agent</h3>
            <p className="text-xs text-neutral">
              {loading ? 'Loading...' : `${skills.length} skills generated`}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {lastAnalysisTime && (
            <span className="text-xs text-neutral">
              {timeSinceAnalysis !== null && timeSinceAnalysis < 24
                ? `${timeSinceAnalysis}h ago`
                : lastAnalysisTime.toLocaleDateString()}
            </span>
          )}
          <svg
            className={clsx(
              'w-4 h-4 text-neutral transition-transform',
              expanded && 'rotate-180'
            )}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {/* Expanded content */}
      {expanded && (
        <div className="border-t border-white/5 p-4 space-y-4">
          {/* Status */}
          <div className="flex items-center gap-2">
            <span
              className={clsx(
                'w-2 h-2 rounded-full',
                observerStatus?.status === 'initialized'
                  ? 'bg-profit'
                  : 'bg-neutral'
              )}
            />
            <span className="text-sm text-neutral">
              {observerStatus?.status === 'initialized'
                ? 'Active'
                : 'Not initialized'}
            </span>
            <span className="text-xs text-neutral/50 ml-auto">
              (Run analysis from Admin Panel)
            </span>
          </div>

          {/* Skills list */}
          <div>
            <h4 className="text-xs font-semibold text-neutral uppercase tracking-wide mb-2">
              Generated Skills
            </h4>
            {skills.length === 0 ? (
              <p className="text-sm text-neutral/70">
                No skills generated yet. Run analysis to create skills.
              </p>
            ) : (
              <div className="space-y-2">
                {skills.map((skill) => (
                  <div key={skill.name}>
                    <button
                      onClick={() => viewSkill(skill.name)}
                      className={clsx(
                        'w-full p-3 rounded-lg text-left transition-all',
                        selectedSkill === skill.name
                          ? 'bg-purple-500/20 border border-purple-500/30'
                          : 'bg-surface/50 hover:bg-surface border border-white/5'
                      )}
                    >
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-white">
                          {skill.name}
                        </span>
                        <svg
                          className={clsx(
                            'w-4 h-4 text-neutral transition-transform',
                            selectedSkill === skill.name && 'rotate-180'
                          )}
                          fill="none"
                          viewBox="0 0 24 24"
                          stroke="currentColor"
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                        </svg>
                      </div>
                      {skill.description && (
                        <p className="text-xs text-neutral mt-1 line-clamp-2">
                          {skill.description}
                        </p>
                      )}
                    </button>

                    {/* Skill content */}
                    {selectedSkill === skill.name && skillContent && (
                      <div className="mt-2 p-3 bg-surface/30 rounded-lg border border-white/5 max-h-64 overflow-y-auto">
                        <pre className="text-xs text-neutral/90 whitespace-pre-wrap font-mono">
                          {skillContent}
                        </pre>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Recent analyses */}
          {observerStatus?.recent_analyses && observerStatus.recent_analyses.length > 0 && (
            <div>
              <h4 className="text-xs font-semibold text-neutral uppercase tracking-wide mb-2">
                Recent Analyses
              </h4>
              <div className="space-y-2">
                {observerStatus.recent_analyses.map((analysis, i) => (
                  <div
                    key={i}
                    className="p-2 bg-surface/30 rounded-lg text-xs flex items-center justify-between"
                  >
                    <span className="text-neutral">
                      {new Date(analysis.timestamp).toLocaleString()}
                    </span>
                    <span className="text-white">
                      {analysis.patterns_found} patterns
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Info */}
          <p className="text-xs text-neutral/60 italic">
            The Observer Agent analyzes competition data and generates trading skills.
            Click the title "AGENT ARENA" to access Admin Panel and run analysis.
          </p>
        </div>
      )}
    </div>
  );
}
