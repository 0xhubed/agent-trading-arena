/**
 * Zustand store for learning agent state management.
 */

import { create } from 'zustand';
import type {
  LearningEvent,
  LearningStats,
  LearningDataPoint,
  LearnedPattern,
  SimilarSituation,
  MetaAnalysis,
  AgentLearningData,
} from '../types/learning';

interface LearningState {
  // Learning events feed (across all agents)
  learningEvents: LearningEvent[];

  // Per-agent learning stats cache
  agentStats: Record<string, LearningStats>;

  // Per-agent learning curve data
  agentLearningCurves: Record<string, LearningDataPoint[]>;

  // Per-agent patterns
  agentPatterns: Record<string, LearnedPattern[]>;

  // Cache for similar situations (by agent + decision)
  similarSituationsCache: Record<string, SimilarSituation[]>;

  // Meta analysis cache (by agent)
  metaAnalysisCache: Record<string, MetaAnalysis>;

  // Loading states
  loading: {
    stats: Record<string, boolean>;
    patterns: Record<string, boolean>;
    situations: Record<string, boolean>;
    meta: Record<string, boolean>;
  };

  // Actions
  addLearningEvent: (event: LearningEvent) => void;
  setLearningEvents: (events: LearningEvent[]) => void;
  updateAgentStats: (agentId: string, stats: LearningStats) => void;
  updateAgentLearningCurve: (agentId: string, curve: LearningDataPoint[]) => void;
  updateAgentPatterns: (agentId: string, patterns: LearnedPattern[]) => void;
  setSimilarSituations: (key: string, situations: SimilarSituation[]) => void;
  setMetaAnalysis: (agentId: string, analysis: MetaAnalysis) => void;
  setAgentLearningData: (data: AgentLearningData) => void;
  setLoading: (type: keyof LearningState['loading'], key: string, loading: boolean) => void;
  clearCache: () => void;
}

export const useLearningStore = create<LearningState>((set) => ({
  // Initial state
  learningEvents: [],
  agentStats: {},
  agentLearningCurves: {},
  agentPatterns: {},
  similarSituationsCache: {},
  metaAnalysisCache: {},
  loading: {
    stats: {},
    patterns: {},
    situations: {},
    meta: {},
  },

  // Actions
  addLearningEvent: (event) =>
    set((state) => ({
      learningEvents: [event, ...state.learningEvents].slice(0, 100),
    })),

  setLearningEvents: (events) =>
    set({ learningEvents: events.slice(0, 100) }),

  updateAgentStats: (agentId, stats) =>
    set((state) => ({
      agentStats: { ...state.agentStats, [agentId]: stats },
    })),

  updateAgentLearningCurve: (agentId, curve) =>
    set((state) => ({
      agentLearningCurves: { ...state.agentLearningCurves, [agentId]: curve },
    })),

  updateAgentPatterns: (agentId, patterns) =>
    set((state) => ({
      agentPatterns: { ...state.agentPatterns, [agentId]: patterns },
    })),

  setSimilarSituations: (key, situations) =>
    set((state) => ({
      similarSituationsCache: { ...state.similarSituationsCache, [key]: situations },
    })),

  setMetaAnalysis: (agentId, analysis) =>
    set((state) => ({
      metaAnalysisCache: { ...state.metaAnalysisCache, [agentId]: analysis },
    })),

  setAgentLearningData: (data) =>
    set((state) => ({
      agentStats: { ...state.agentStats, [data.agent_id]: data.stats },
      agentLearningCurves: { ...state.agentLearningCurves, [data.agent_id]: data.learning_curve },
      agentPatterns: { ...state.agentPatterns, [data.agent_id]: data.patterns },
    })),

  setLoading: (type, key, loading) =>
    set((state) => ({
      loading: {
        ...state.loading,
        [type]: { ...state.loading[type], [key]: loading },
      },
    })),

  clearCache: () =>
    set({
      agentStats: {},
      agentLearningCurves: {},
      agentPatterns: {},
      similarSituationsCache: {},
      metaAnalysisCache: {},
    }),
}));

// API helper functions for fetching learning data
export async function fetchAgentLearning(agentId: string): Promise<AgentLearningData | null> {
  try {
    const response = await fetch(`/api/agents/${agentId}/learning`);
    if (!response.ok) return null;
    return await response.json();
  } catch (error) {
    console.error('Failed to fetch agent learning data:', error);
    return null;
  }
}

export async function fetchAgentPatterns(
  agentId: string,
  minConfidence = 0.5
): Promise<LearnedPattern[]> {
  try {
    const response = await fetch(
      `/api/agents/${agentId}/patterns?min_confidence=${minConfidence}`
    );
    if (!response.ok) return [];
    return await response.json();
  } catch (error) {
    console.error('Failed to fetch agent patterns:', error);
    return [];
  }
}

export async function fetchSimilarSituations(
  agentId: string,
  decisionId?: number,
  limit = 5
): Promise<SimilarSituation[]> {
  try {
    const params = new URLSearchParams({ limit: limit.toString() });
    if (decisionId) params.append('decision_id', decisionId.toString());

    const response = await fetch(
      `/api/agents/${agentId}/similar-situations?${params}`
    );
    if (!response.ok) return [];
    const data = await response.json();
    return data.situations || [];
  } catch (error) {
    console.error('Failed to fetch similar situations:', error);
    return [];
  }
}

export async function fetchMetaAnalysis(agentId: string): Promise<MetaAnalysis | null> {
  try {
    const response = await fetch(`/api/agents/${agentId}/meta-analysis`);
    if (!response.ok) return null;
    return await response.json();
  } catch (error) {
    console.error('Failed to fetch meta analysis:', error);
    return null;
  }
}

export async function fetchLearningEvents(limit = 20): Promise<LearningEvent[]> {
  try {
    const response = await fetch(`/api/learning-events?limit=${limit}`);
    if (!response.ok) return [];
    return await response.json();
  } catch (error) {
    console.error('Failed to fetch learning events:', error);
    return [];
  }
}

export async function fetchRegimePerformance(
  regime?: string,
  minTrades = 10
): Promise<import('../types/learning').RegimePerformance[]> {
  try {
    const params = new URLSearchParams({ min_trades: minTrades.toString() });
    if (regime) params.append('regime', regime);

    const response = await fetch(`/api/regime-performance?${params}`);
    if (!response.ok) return [];
    return await response.json();
  } catch (error) {
    console.error('Failed to fetch regime performance:', error);
    return [];
  }
}
