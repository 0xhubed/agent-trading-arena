/** Lab tab types — bias profiles and contagion metrics. */

export type LabTab = 'bias' | 'contagion';

export interface BiasProfile {
  agent_id: string;
  bias_type: 'disposition_effect' | 'loss_aversion' | 'overconfidence';
  score: number | null;
  sample_size: number;
  sufficient_data: boolean;
  details: Record<string, unknown>;
  created_at: string;
}

export interface ContagionMetric {
  metric_type: 'position_diversity' | 'reasoning_entropy';
  value: number | null;
  sample_size: number;
  sufficient_data: boolean;
  details: Record<string, unknown>;
  tick: number | null;
  agent_count: number | null;
  created_at: string;
}

export interface ContagionLatest {
  metrics: ContagionMetric[];
  system_health: number | null;
  health_label: string;
}
