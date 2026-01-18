export interface Agent {
  id: string;
  name: string;
  model: string;
  agent_type: string;  // 'Agentic' | 'LLM' | 'TA' | 'Passive' | 'LangChain' | 'Custom'
  agent_type_description: string;  // e.g., 'LangGraph ReAct + Tools'
  character?: string;
  portfolio?: Portfolio;
  is_learning_agent?: boolean;  // Whether this agent uses learning features
}

export interface Portfolio {
  equity: number;
  available_margin: number;
  used_margin: number;
  margin_utilization: number;
  positions: Position[];
  realized_pnl: number;
  total_pnl: number;
  pnl_percent: number;
}

export interface Position {
  symbol: string;
  side: 'long' | 'short';
  size: number;
  entry_price: number;
  mark_price: number;
  liquidation_price: number;
  margin: number;
  unrealized_pnl: number;
  roe_percent: number;
  leverage: number;
  stop_loss?: number | null;
  take_profit?: number | null;
}

export interface LeaderboardEntry {
  agent_id: string;
  equity: number;
  pnl: number;
  pnl_percent: number;
  positions: number;
  trades: number;
  // Extended metrics (from extended=true API call)
  win_rate?: number;
  sharpe_ratio?: number;
  total_trades?: number;
  profit_factor?: number | null;
  max_drawdown?: number;
  expectancy?: number;
}

export interface Decision {
  action: string;
  symbol?: string;
  size?: string;
  leverage?: number;
  confidence: number;
  reasoning: string;
}

export interface Trade {
  id: string;
  symbol: string;
  side: string;
  size: string;
  price: string;
  leverage: number;
  fee: string;
  realized_pnl?: string;
}

export interface MarketData {
  [symbol: string]: {
    price: number;
    change_24h: number;
  };
}

export interface TickEvent {
  tick: number;
  timestamp: string;
  leaderboard: LeaderboardEntry[];
  market: MarketData;
  decisions: {
    [agentId: string]: {
      action: string;
      reasoning: string;
      confidence: number;
    };
  };
}

export interface DecisionEvent {
  agent_id: string;
  decision: Decision;
  trade?: Trade;
}

export interface HistoryPoint {
  tick: number;
  timestamp: string;
  leaderboard: LeaderboardEntry[];
}
