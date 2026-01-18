/**
 * TypeScript types for learning agent features.
 */

export interface LearningStats {
  patterns_learned: number;
  situations_referenced: number;
  reflections_made: number;
  improvement_vs_baseline: number;
}

export interface LearningDataPoint {
  tick: number;
  win_rate: number;
  sharpe_ratio: number;
  patterns_count: number;
}

export interface LearningCurve {
  data_points: LearningDataPoint[];
  trend: 'improving' | 'stable' | 'declining';
}

export interface SimilarSituation {
  id: string;
  similarity: number;
  timestamp: string;
  regime: string;
  decision: {
    action: string;
    symbol: string;
    confidence: number;
    reasoning: string;
  };
  outcome: {
    realized_pnl: number;
    outcome_score: number;
    was_profitable: boolean;
    exit_reason?: string;
  };
}

export interface PatternCondition {
  [key: string]: unknown;
}

export type PatternType = 'entry_signal' | 'exit_signal' | 'risk_rule' | 'regime_rule';

export interface LearnedPattern {
  id: string;
  pattern_type: PatternType;
  conditions: PatternCondition;
  lesson: string;
  recommended_action?: string;
  confidence: number;
  success_rate: number;
  sample_size: number;
  last_validated: string;
  is_active: boolean;
}

export interface DecisionInfluence {
  type: 'rag' | 'pattern' | 'meta';
  summary: string;
  details: string[];
  weight: number;
}

export interface EnhancedDecision {
  agent_id: string;
  decision: {
    action: string;
    symbol?: string;
    size?: string;
    leverage?: number;
    confidence: number;
    reasoning: string;
  };
  influences?: {
    similar_situations: number;
    matching_patterns: number;
    top_agents_considered: number;
    regime: string;
  };
  is_learning_agent?: boolean;
  timestamp: string;
}

export type LearningEventType = 'pattern_learned' | 'reflection' | 'strategy_shift' | 'rag_retrieval' | 'meta_insight';

export interface LearningEvent {
  id: string;
  agent_id: string;
  agent_name?: string;
  event_type: LearningEventType;
  summary: string;
  details?: Record<string, unknown>;
  timestamp: string;
}

export interface RegimePerformance {
  agent_id: string;
  agent_name?: string;
  regime: string;
  symbol?: string;
  total_trades: number;
  winning_trades: number;
  win_rate: number;
  total_pnl: number;
  sharpe_ratio: number;
  avg_holding_time?: number;
}

export interface MetaAnalysis {
  current_regime: string;
  top_performers: RegimePerformance[];
  this_agent_rank: number | null;
  insight: string;
}

export interface AgentLearningData {
  agent_id: string;
  stats: LearningStats;
  learning_curve: LearningDataPoint[];
  patterns: LearnedPattern[];
  is_learning_agent: boolean;
}
