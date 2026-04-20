/**
 * TypeScript types for backtesting features.
 */

export interface DataSymbolStatus {
  symbol: string;
  interval: string;
  earliest: string | null;
  latest: string | null;
  count: number;
  gaps: number;
}

export interface DataStatus {
  symbols: DataSymbolStatus[];
  total_candles: number;
  database_backend: string;
}

export interface FetchProgress {
  status: 'running' | 'completed' | 'failed';
  progress: number;
  current_symbol?: string;
  current_interval?: string;
  message?: string;
  error?: string;
}

export interface BacktestConfig {
  start_date: string;
  end_date: string;
  symbols: string[];
  tick_interval: string;
  candle_intervals: string[];
  candle_limit: number;
  agent_configs: AgentConfig[];
  run_baselines: boolean;
}

export interface AgentConfig {
  agent_id: string;
  name: string;
  class_path: string;
  config: Record<string, unknown>;
}

export interface CostEstimate {
  total_ticks: number;
  decisions_per_agent: number;
  estimated_api_calls: number;
  estimated_cost_usd: number;
  duration_hours: number;
  agents: {
    agent_id: string;
    calls_estimate: number;
    cost_estimate: number;
  }[];
}

export interface BacktestRunStatus {
  run_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number;
  current_tick: number;
  total_ticks: number;
  elapsed_seconds: number;
  estimated_remaining_seconds: number | null;
  current_phase: string;
  error?: string;
}

export interface EquityPoint {
  tick: number;
  timestamp: string;
  equity: number;
}

export interface TradeRecord {
  tick: number;
  timestamp: string;
  symbol: string;
  action: string;
  side?: string;
  entry_price?: number;
  exit_price?: number;
  size: number;
  leverage?: number;
  pnl?: number;
  fee: number;
}

export interface AgentResult {
  agent_id: string;
  agent_name: string;
  final_equity: number;
  total_return: number;
  total_pnl: number;
  total_trades: number;
  winning_trades: number;
  losing_trades: number;
  win_rate: number;
  sharpe_ratio: number | null;
  max_drawdown_pct: number;
  max_drawdown_amount: number;
  profit_factor: number | null;
  expectancy: number | null;
  avg_trade_pnl: number | null;
  equity_curve: EquityPoint[];
  trades: TradeRecord[];
}

export interface BaselineComparison {
  agent_id: string;
  baseline_id: string;
  agent_return: number;
  baseline_return: number;
  outperformance: number;
  p_value: number | null;
  ci_low: number | null;
  ci_high: number | null;
  is_significant: boolean;
  test_used: string;
}

export interface BacktestResult {
  run_id: string;
  config: {
    start_date: string;
    end_date: string;
    symbols: string[];
    tick_interval: string;
  };
  duration_seconds: number;
  total_ticks: number;
  agents: AgentResult[];
  comparisons: BaselineComparison[];
  created_at: string;
}

export interface BacktestRun {
  run_id: string;
  status: string;
  start_date: string;
  end_date: string;
  symbols: string[];
  tick_interval: string;
  total_ticks: number;
  created_at: string;
  completed_at: string | null;
  duration_seconds: number | null;
}

export type BacktestTab = 'data' | 'configure' | 'results';
