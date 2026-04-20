/**
 * TypeScript types for the evolution (genetic algorithm) feature.
 * Mirrors API responses from routes_evolution.py + storage.py.
 */

export type EvolutionTab = 'runs' | 'fitness' | 'genome' | 'lineage' | 'parameters' | 'diversity' | 'feed';

export interface EvolutionRun {
  run_id: string;
  name: string;
  status: 'running' | 'completed' | 'cancelled';
  population_size: number;
  max_generations: number;
  current_generation: number;
  best_fitness: number | null;
  created_at: string | null;
  completed_at: string | null;
}

export interface GenerationStats {
  generation: number;
  pop_size: number;
  best_fitness: number;
  avg_fitness: number;
  worst_fitness: number;
}

export interface EvolutionRunDetail {
  run_id: string;
  name: string;
  status: string;
  population_size: number;
  max_generations: number;
  current_generation: number;
  agent_class: string;
  backtest_start: string;
  backtest_end: string;
  tick_interval: string;
  symbols: string[];
  fitness_weights: Record<string, number>;
  config: Record<string, unknown>;
  best_fitness: number | null;
  best_genome_id: string | null;
  created_at: string | null;
  completed_at: string | null;
  generations: GenerationStats[];
}

export interface GenomeParams {
  genome_id: string;
  agent_class: string;
  generation: number;
  model: string;
  temperature: number;
  max_tokens: number;
  character: string;
  confidence_threshold: number;
  position_size_pct: number;
  sl_pct: number;
  tp_pct: number;
  max_leverage: number;
  parent_ids: string[];
  mutations: string[];
}

export interface GenomeEntry {
  genome_id: string;
  genome: GenomeParams;
  fitness: number | null;
  metrics: Record<string, number>;
  parent_ids: string[];
  mutations: string[];
  is_elite: boolean;
}

export interface BestGenome {
  genome_id: string;
  generation: number;
  genome: GenomeParams;
  fitness: number;
  metrics: Record<string, number>;
  is_elite: boolean;
}

export interface LineageGenome {
  genome_id: string;
  generation: number;
  genome: GenomeParams;
  fitness: number | null;
  metrics: Record<string, number>;
  parent_ids: string[];
  mutations: string[];
  is_elite: boolean;
}

export interface DiversityEntry {
  generation: number;
  population_size: number;
  param_variance: Record<string, number>;
  total_variance: number;
  unique_strategies: number;
}

export interface EvolutionEvent {
  run_id: string;
  type: 'crossover' | 'llm_crossover' | 'mutation' | 'llm_mutation' | 'elite_selection' | 'migration' | 'elimination';
  generation?: number;
  parent_ids?: string[];
  child_id?: string;
  genome_id?: string;
  mutations?: string[];
  elite_ids?: string[];
  timestamp?: string;
}
