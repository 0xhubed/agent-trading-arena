"""PostgreSQL + pgvector storage backend."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Union


def parse_timestamp(ts: Union[str, datetime]) -> datetime:
    """Convert ISO string or datetime to timezone-aware datetime for PostgreSQL."""
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    # Parse ISO format string (handles both 'Z' suffix and '+00:00')
    ts_str = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)

try:
    import asyncpg
    from pgvector.asyncpg import register_vector

    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    asyncpg = None


class PostgresStorage:
    """PostgreSQL-based storage with vector support for learning agents."""

    def __init__(self, connection_string: str):
        if not POSTGRES_AVAILABLE:
            raise ImportError(
                "PostgreSQL support requires asyncpg and pgvector. "
                "Install with: pip install agent-arena[postgres]"
            )
        self.connection_string = connection_string
        self.pool: Optional[asyncpg.Pool] = None

    async def initialize(self) -> None:
        """Initialize connection pool and create tables."""
        self.pool = await asyncpg.create_pool(
            self.connection_string,
            min_size=5,
            max_size=20,
            init=self._init_connection,
        )
        await self._create_tables()

    async def _init_connection(self, conn: asyncpg.Connection) -> None:
        """Initialize each connection with pgvector."""
        await register_vector(conn)

    def _ensure_pool(self) -> asyncpg.Pool:
        """Return pool or raise a clear error if not initialized."""
        if self.pool is None:
            raise RuntimeError(
                "PostgreSQL pool is not initialized. "
                "Call initialize() first or check that the pool wasn't closed."
            )
        return self.pool

    async def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def _create_tables(self) -> None:
        """Create all required tables."""
        async with self.pool.acquire() as conn:
            # Core tables (migrated from SQLite)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS decisions (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    tick INTEGER NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    action VARCHAR(50) NOT NULL,
                    symbol VARCHAR(20),
                    size DECIMAL(20,8),
                    leverage INTEGER,
                    confidence REAL,
                    reasoning TEXT,
                    metadata JSONB DEFAULT '{}',
                    trade_id VARCHAR(100),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS trades (
                    id VARCHAR(100) PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    size DECIMAL(20,8) NOT NULL,
                    price DECIMAL(20,8) NOT NULL,
                    leverage INTEGER NOT NULL,
                    fee DECIMAL(20,8) NOT NULL,
                    realized_pnl DECIMAL(20,8),
                    timestamp TIMESTAMPTZ NOT NULL,
                    decision_id INTEGER REFERENCES decisions(id),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS competitions (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    config JSONB NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    ended_at TIMESTAMPTZ,
                    final_leaderboard JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    id SERIAL PRIMARY KEY,
                    competition_id INTEGER REFERENCES competitions(id),
                    tick INTEGER NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    leaderboard JSONB NOT NULL,
                    market_data JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS agent_memories (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    memory_type VARCHAR(50) NOT NULL,
                    content TEXT NOT NULL,
                    importance REAL DEFAULT 0.5,
                    tick INTEGER,
                    timestamp TIMESTAMPTZ NOT NULL,
                    metadata JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS agent_summaries (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    summary_type VARCHAR(50) NOT NULL,
                    content TEXT NOT NULL,
                    period_start TIMESTAMPTZ NOT NULL,
                    period_end TIMESTAMPTZ NOT NULL,
                    tick_count INTEGER,
                    trade_count INTEGER,
                    pnl_summary TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS funding_payments (
                    id SERIAL PRIMARY KEY,
                    tick INTEGER NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    funding_rate DECIMAL(20,10) NOT NULL,
                    notional DECIMAL(20,8) NOT NULL,
                    amount DECIMAL(20,8) NOT NULL,
                    direction VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS liquidations (
                    id SERIAL PRIMARY KEY,
                    tick INTEGER NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    size DECIMAL(20,8) NOT NULL,
                    entry_price DECIMAL(20,8) NOT NULL,
                    liquidation_price DECIMAL(20,8) NOT NULL,
                    mark_price DECIMAL(20,8) NOT NULL,
                    margin_lost DECIMAL(20,8) NOT NULL,
                    fee DECIMAL(20,8) NOT NULL,
                    total_loss DECIMAL(20,8) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS sl_tp_triggers (
                    id SERIAL PRIMARY KEY,
                    tick INTEGER NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    trigger_type VARCHAR(20) NOT NULL,
                    trigger_price DECIMAL(20,8) NOT NULL,
                    mark_price DECIMAL(20,8) NOT NULL,
                    size DECIMAL(20,8) NOT NULL,
                    realized_pnl DECIMAL(20,8) NOT NULL,
                    fee DECIMAL(20,8) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Learning-specific tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS decision_contexts (
                    id SERIAL PRIMARY KEY,
                    decision_id INTEGER REFERENCES decisions(id) UNIQUE,
                    tick INTEGER NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    market_prices JSONB NOT NULL,
                    candles JSONB,
                    indicators JSONB,
                    portfolio_state JSONB NOT NULL,
                    regime VARCHAR(50),
                    volatility_percentile REAL,
                    context_embedding vector(1536),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS decision_outcomes (
                    id SERIAL PRIMARY KEY,
                    decision_id INTEGER REFERENCES decisions(id) UNIQUE,
                    realized_pnl DECIMAL(20,8),
                    holding_duration_ticks INTEGER,
                    max_drawdown_during DECIMAL(20,8),
                    max_profit_during DECIMAL(20,8),
                    exit_reason VARCHAR(50),
                    outcome_score REAL,
                    risk_adjusted_return REAL,
                    price_1h_later DECIMAL(20,8),
                    price_4h_later DECIMAL(20,8),
                    price_24h_later DECIMAL(20,8),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS learned_patterns (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100),
                    pattern_type VARCHAR(50) NOT NULL,
                    pattern_description TEXT NOT NULL,
                    conditions JSONB NOT NULL,
                    recommended_action VARCHAR(50),
                    supporting_decisions INTEGER[],
                    success_rate REAL,
                    sample_size INTEGER,
                    confidence REAL,
                    discovered_at TIMESTAMPTZ DEFAULT NOW(),
                    last_validated TIMESTAMPTZ,
                    is_active BOOLEAN DEFAULT true
                );

                CREATE TABLE IF NOT EXISTS candle_history (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    interval VARCHAR(10) NOT NULL,
                    open_time TIMESTAMPTZ NOT NULL,
                    open DECIMAL(20,8) NOT NULL,
                    high DECIMAL(20,8) NOT NULL,
                    low DECIMAL(20,8) NOT NULL,
                    close DECIMAL(20,8) NOT NULL,
                    volume DECIMAL(30,8) NOT NULL,
                    rsi_14 REAL,
                    sma_20 REAL,
                    sma_50 REAL,
                    UNIQUE(symbol, interval, open_time)
                );

                CREATE TABLE IF NOT EXISTS regime_performance (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    regime VARCHAR(50) NOT NULL,
                    symbol VARCHAR(20),
                    total_trades INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    total_pnl DECIMAL(20,8) DEFAULT 0,
                    sharpe_ratio REAL,
                    avg_holding_time REAL,
                    period_start TIMESTAMPTZ,
                    period_end TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(agent_id, regime, symbol)
                );

                CREATE TABLE IF NOT EXISTS learning_events (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    summary TEXT NOT NULL,
                    details JSONB,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Long-term archival tables
            await conn.execute("""
                -- Daily snapshots for long-term analysis
                CREATE TABLE IF NOT EXISTS daily_snapshots (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    starting_equity DECIMAL(20,8) NOT NULL,
                    ending_equity DECIMAL(20,8) NOT NULL,
                    daily_pnl DECIMAL(20,8) NOT NULL,
                    daily_pnl_pct REAL NOT NULL,
                    trade_count INTEGER DEFAULT 0,
                    win_count INTEGER DEFAULT 0,
                    loss_count INTEGER DEFAULT 0,
                    total_volume DECIMAL(30,8) DEFAULT 0,
                    total_fees DECIMAL(20,8) DEFAULT 0,
                    total_funding DECIMAL(20,8) DEFAULT 0,
                    max_drawdown_pct REAL,
                    sharpe_estimate REAL,
                    avg_confidence REAL,
                    regime_distribution JSONB,
                    symbol_distribution JSONB,
                    skill_version_hash VARCHAR(64),
                    metadata JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(date, agent_id)
                );

                -- Full decision archive with context for ML training
                CREATE TABLE IF NOT EXISTS decision_archive (
                    id SERIAL PRIMARY KEY,
                    decision_id INTEGER,
                    agent_id VARCHAR(100) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    tick INTEGER NOT NULL,
                    action VARCHAR(50) NOT NULL,
                    symbol VARCHAR(20),
                    size DECIMAL(20,8),
                    leverage INTEGER,
                    confidence REAL,
                    reasoning TEXT,

                    -- Market context at decision time
                    market_prices JSONB NOT NULL,
                    market_changes_1h JSONB,
                    market_changes_24h JSONB,
                    volatility_state JSONB,

                    -- Technical indicators
                    indicators JSONB,
                    regime VARCHAR(50),

                    -- Portfolio state
                    portfolio_equity DECIMAL(20,8),
                    portfolio_positions JSONB,
                    available_margin DECIMAL(20,8),

                    -- Outcome (filled later when position closes)
                    outcome_pnl DECIMAL(20,8),
                    outcome_duration_ticks INTEGER,
                    outcome_exit_reason VARCHAR(50),
                    outcome_max_profit DECIMAL(20,8),
                    outcome_max_drawdown DECIMAL(20,8),

                    -- Embedding for similarity search
                    context_embedding vector(1536),

                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Skill version tracking
                CREATE TABLE IF NOT EXISTS skill_versions (
                    id SERIAL PRIMARY KEY,
                    skill_name VARCHAR(100) NOT NULL,
                    version_hash VARCHAR(64) NOT NULL,
                    content TEXT NOT NULL,
                    pattern_count INTEGER DEFAULT 0,
                    active_patterns INTEGER DEFAULT 0,
                    total_samples INTEGER DEFAULT 0,
                    metadata JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(skill_name, version_hash)
                );

                -- Competition sessions for grouping data
                CREATE TABLE IF NOT EXISTS competition_sessions (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(255),
                    config JSONB NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    ended_at TIMESTAMPTZ,
                    total_ticks INTEGER DEFAULT 0,
                    final_leaderboard JSONB,
                    skill_versions_used JSONB,
                    metadata JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Aggregated pattern performance over time
                CREATE TABLE IF NOT EXISTS pattern_performance (
                    id SERIAL PRIMARY KEY,
                    pattern_id VARCHAR(64) NOT NULL,
                    skill_name VARCHAR(100) NOT NULL,
                    date DATE NOT NULL,
                    times_matched INTEGER DEFAULT 0,
                    times_successful INTEGER DEFAULT 0,
                    total_pnl DECIMAL(20,8) DEFAULT 0,
                    avg_confidence REAL,
                    sample_decisions INTEGER[],
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(pattern_id, date)
                );

                -- Arena state for competition resume
                CREATE TABLE IF NOT EXISTS arena_state (
                    id SERIAL PRIMARY KEY,
                    competition_name VARCHAR(255) NOT NULL,
                    last_tick INTEGER NOT NULL,
                    last_timestamp TIMESTAMPTZ NOT NULL,
                    current_prices JSONB NOT NULL,
                    config JSONB,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(competition_name)
                );

                -- Portfolio state per agent for resume
                CREATE TABLE IF NOT EXISTS portfolio_state (
                    id SERIAL PRIMARY KEY,
                    competition_name VARCHAR(255) NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    initial_capital DECIMAL(20,8) NOT NULL,
                    available_margin DECIMAL(20,8) NOT NULL,
                    realized_pnl DECIMAL(20,8) NOT NULL,
                    funding_paid DECIMAL(20,8) DEFAULT 0,
                    funding_received DECIMAL(20,8) DEFAULT 0,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(competition_name, agent_id)
                );

                -- Open positions for resume
                CREATE TABLE IF NOT EXISTS position_state (
                    id SERIAL PRIMARY KEY,
                    competition_name VARCHAR(255) NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    position_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    size DECIMAL(20,8) NOT NULL,
                    entry_price DECIMAL(20,8) NOT NULL,
                    leverage INTEGER NOT NULL,
                    margin DECIMAL(20,8) NOT NULL,
                    opened_at TIMESTAMPTZ NOT NULL,
                    stop_loss_price DECIMAL(20,8),
                    take_profit_price DECIMAL(20,8),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(competition_name, agent_id, position_id)
                );

                -- Pending orders for resume
                CREATE TABLE IF NOT EXISTS pending_order_state (
                    id SERIAL PRIMARY KEY,
                    competition_name VARCHAR(255) NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    order_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    order_type VARCHAR(20) NOT NULL,
                    size DECIMAL(20,8) NOT NULL,
                    limit_price DECIMAL(20,8) NOT NULL,
                    leverage INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    stop_loss_price DECIMAL(20,8),
                    take_profit_price DECIMAL(20,8),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(competition_name, order_id)
                );
            """)

            # Backtest tables
            await conn.execute("""
                -- Candles table for backtest historical data
                CREATE TABLE IF NOT EXISTS candles (
                    symbol VARCHAR(20) NOT NULL,
                    interval VARCHAR(10) NOT NULL,
                    open_time BIGINT NOT NULL,
                    open DECIMAL(20,8) NOT NULL,
                    high DECIMAL(20,8) NOT NULL,
                    low DECIMAL(20,8) NOT NULL,
                    close DECIMAL(20,8) NOT NULL,
                    volume DECIMAL(30,8) NOT NULL,
                    close_time BIGINT NOT NULL,
                    quote_volume DECIMAL(30,8),
                    trade_count INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (symbol, interval, open_time)
                );

                -- Backtest runs metadata
                CREATE TABLE IF NOT EXISTS backtest_runs (
                    id VARCHAR(100) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    config JSONB NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    tick_interval VARCHAR(20) NOT NULL,
                    status VARCHAR(50) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    total_ticks INTEGER,
                    current_tick INTEGER DEFAULT 0,
                    estimated_cost DECIMAL(10,4),
                    actual_cost DECIMAL(10,4),
                    error_message TEXT
                );

                -- Backtest results per agent
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id VARCHAR(100) PRIMARY KEY,
                    run_id VARCHAR(100) NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
                    agent_id VARCHAR(100) NOT NULL,
                    agent_name VARCHAR(255) NOT NULL,
                    total_pnl DECIMAL(20,8) NOT NULL,
                    total_pnl_pct REAL NOT NULL,
                    sharpe_ratio REAL,
                    win_rate REAL,
                    max_drawdown_pct REAL,
                    total_trades INTEGER NOT NULL,
                    winning_trades INTEGER NOT NULL,
                    losing_trades INTEGER NOT NULL,
                    profit_factor REAL,
                    avg_trade_pnl REAL,
                    largest_win DECIMAL(20,8),
                    largest_loss DECIMAL(20,8),
                    total_fees DECIMAL(20,8),
                    equity_curve JSONB,
                    trades JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Statistical comparisons between agents
                CREATE TABLE IF NOT EXISTS backtest_comparisons (
                    id VARCHAR(100) PRIMARY KEY,
                    run_id VARCHAR(100) NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
                    agent_id VARCHAR(100) NOT NULL,
                    baseline_id VARCHAR(100) NOT NULL,
                    outperformance REAL NOT NULL,
                    p_value REAL,
                    ci_low REAL,
                    ci_high REAL,
                    is_significant BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # Create indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_decisions_agent ON decisions(agent_id);
                CREATE INDEX IF NOT EXISTS idx_decisions_tick ON decisions(tick);
                CREATE INDEX IF NOT EXISTS idx_trades_agent ON trades(agent_id);
                CREATE INDEX IF NOT EXISTS idx_snapshots_tick ON snapshots(tick);
                CREATE INDEX IF NOT EXISTS idx_memories_agent ON agent_memories(agent_id);
                CREATE INDEX IF NOT EXISTS idx_memories_type ON agent_memories(memory_type);
                CREATE INDEX IF NOT EXISTS idx_summaries_agent ON agent_summaries(agent_id);
                CREATE INDEX IF NOT EXISTS idx_funding_agent ON funding_payments(agent_id);
                CREATE INDEX IF NOT EXISTS idx_funding_tick ON funding_payments(tick);
                CREATE INDEX IF NOT EXISTS idx_liquidations_agent ON liquidations(agent_id);
                CREATE INDEX IF NOT EXISTS idx_liquidations_tick ON liquidations(tick);
                CREATE INDEX IF NOT EXISTS idx_sl_tp_agent ON sl_tp_triggers(agent_id);
                CREATE INDEX IF NOT EXISTS idx_sl_tp_tick ON sl_tp_triggers(tick);

                CREATE INDEX IF NOT EXISTS idx_contexts_decision ON decision_contexts(decision_id);
                CREATE INDEX IF NOT EXISTS idx_outcomes_decision ON decision_outcomes(decision_id);
                CREATE INDEX IF NOT EXISTS idx_patterns_agent ON learned_patterns(agent_id);
                CREATE INDEX IF NOT EXISTS idx_patterns_type
                    ON learned_patterns(pattern_type, is_active);
                CREATE INDEX IF NOT EXISTS idx_candles_lookup
                    ON candle_history(symbol, interval, open_time DESC);
                CREATE INDEX IF NOT EXISTS idx_regime_perf ON regime_performance(agent_id, regime);
                CREATE INDEX IF NOT EXISTS idx_learning_events
                    ON learning_events(agent_id, timestamp DESC);

                -- Archival indexes
                CREATE INDEX IF NOT EXISTS idx_daily_snapshots_date
                    ON daily_snapshots(date DESC);
                CREATE INDEX IF NOT EXISTS idx_daily_snapshots_agent
                    ON daily_snapshots(agent_id, date DESC);
                CREATE INDEX IF NOT EXISTS idx_decision_archive_agent
                    ON decision_archive(agent_id, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_decision_archive_timestamp
                    ON decision_archive(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_decision_archive_outcome
                    ON decision_archive(outcome_pnl) WHERE outcome_pnl IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_skill_versions_name
                    ON skill_versions(skill_name, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_competition_sessions_date
                    ON competition_sessions(started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_pattern_performance_pattern
                    ON pattern_performance(pattern_id, date DESC);

                -- Backtest indexes
                CREATE INDEX IF NOT EXISTS idx_candles_range
                    ON candles(symbol, interval, open_time, close_time);
                CREATE INDEX IF NOT EXISTS idx_backtest_results_run
                    ON backtest_results(run_id);
                CREATE INDEX IF NOT EXISTS idx_backtest_comparisons_run
                    ON backtest_comparisons(run_id);
                CREATE INDEX IF NOT EXISTS idx_backtest_runs_status
                    ON backtest_runs(status, created_at DESC);
            """)

            # Observer memory tables (self-correcting pattern lifecycle)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS observer_runs (
                    id UUID PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    window_start TIMESTAMPTZ,
                    window_end TIMESTAMPTZ,
                    decisions_analyzed INTEGER DEFAULT 0,
                    trades_analyzed INTEGER DEFAULT 0,
                    agents_observed INTEGER DEFAULT 0,

                    patterns_confirmed INTEGER DEFAULT 0,
                    patterns_contradicted INTEGER DEFAULT 0,
                    patterns_new INTEGER DEFAULT 0,
                    patterns_deprecated INTEGER DEFAULT 0,

                    raw_analysis TEXT,
                    summary JSONB DEFAULT '{}',
                    skills_updated TEXT[],
                    metadata JSONB DEFAULT '{}',

                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS observer_memory (
                    id SERIAL PRIMARY KEY,
                    run_id UUID NOT NULL REFERENCES observer_runs(id),
                    run_timestamp TIMESTAMPTZ NOT NULL,
                    observation_window_start TIMESTAMPTZ,
                    observation_window_end TIMESTAMPTZ,

                    pattern_id VARCHAR(128) NOT NULL,
                    skill_name VARCHAR(64) NOT NULL,
                    pattern_type VARCHAR(64),
                    description TEXT NOT NULL,

                    status VARCHAR(20) NOT NULL DEFAULT 'active',
                    confidence REAL NOT NULL DEFAULT 0.5,
                    sample_size INTEGER DEFAULT 0,
                    times_confirmed INTEGER DEFAULT 0,
                    times_contradicted INTEGER DEFAULT 0,
                    first_seen TIMESTAMPTZ NOT NULL,
                    last_confirmed TIMESTAMPTZ,
                    last_contradicted TIMESTAMPTZ,

                    reasoning TEXT,
                    supporting_evidence JSONB DEFAULT '{}',
                    contradiction_evidence JSONB DEFAULT '{}',
                    metadata JSONB DEFAULT '{}',

                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(run_id, pattern_id)
                );

                CREATE INDEX IF NOT EXISTS idx_observer_memory_pattern
                    ON observer_memory(pattern_id);
                CREATE INDEX IF NOT EXISTS idx_observer_memory_skill
                    ON observer_memory(skill_name);
                CREATE INDEX IF NOT EXISTS idx_observer_memory_status
                    ON observer_memory(status);
                CREATE INDEX IF NOT EXISTS idx_observer_memory_run
                    ON observer_memory(run_timestamp DESC);
            """)

            # Evolution engine tables (M2: parameter evolution via GA)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS evolution_runs (
                    id VARCHAR(100) PRIMARY KEY,
                    name VARCHAR(255),
                    status VARCHAR(50) DEFAULT 'pending',
                    population_size INTEGER NOT NULL,
                    max_generations INTEGER NOT NULL,
                    current_generation INTEGER DEFAULT 0,
                    agent_class VARCHAR(255) NOT NULL,
                    backtest_start DATE NOT NULL,
                    backtest_end DATE NOT NULL,
                    tick_interval VARCHAR(20) NOT NULL,
                    symbols TEXT[] NOT NULL,
                    fitness_weights JSONB NOT NULL,
                    config JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    best_fitness REAL,
                    best_genome_id VARCHAR(100)
                );

                CREATE TABLE IF NOT EXISTS evolution_genomes (
                    id VARCHAR(100) PRIMARY KEY,
                    run_id VARCHAR(100) NOT NULL REFERENCES evolution_runs(id) ON DELETE CASCADE,
                    generation INTEGER NOT NULL,
                    genome JSONB NOT NULL,
                    fitness REAL,
                    metrics JSONB,
                    backtest_run_id VARCHAR(100),
                    parent_ids TEXT[] DEFAULT '{}',
                    mutations TEXT[] DEFAULT '{}',
                    is_elite BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_evo_genomes_run_gen
                    ON evolution_genomes(run_id, generation);
                CREATE INDEX IF NOT EXISTS idx_evo_genomes_fitness
                    ON evolution_genomes(run_id, fitness DESC);
            """)

            # Forum tables (M3: agent discussions and witness summaries)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS forum_messages (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    channel VARCHAR(50) NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    agent_name VARCHAR(200),
                    agent_type VARCHAR(50),
                    content TEXT NOT NULL,
                    reply_to UUID REFERENCES forum_messages(id),
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_forum_channel_time
                    ON forum_messages(channel, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_forum_agent
                    ON forum_messages(agent_id);
                CREATE INDEX IF NOT EXISTS idx_forum_reply
                    ON forum_messages(reply_to);

                CREATE TABLE IF NOT EXISTS forum_witness (
                    id SERIAL PRIMARY KEY,
                    witness_type VARCHAR(50) NOT NULL,
                    insight TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    symbols TEXT[],
                    timeframe VARCHAR(50),
                    based_on JSONB DEFAULT '{}',
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    valid_until TIMESTAMPTZ
                );

                CREATE INDEX IF NOT EXISTS idx_witness_type
                    ON forum_witness(witness_type);
                CREATE INDEX IF NOT EXISTS idx_witness_time
                    ON forum_witness(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_witness_symbols
                    ON forum_witness USING GIN(symbols);

                CREATE TABLE IF NOT EXISTS observer_forum_runs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    timestamp TIMESTAMPTZ NOT NULL,
                    window_start TIMESTAMPTZ,
                    window_end TIMESTAMPTZ,
                    messages_analyzed INTEGER DEFAULT 0,
                    trades_analyzed INTEGER DEFAULT 0,
                    witness_generated INTEGER DEFAULT 0,
                    raw_analysis TEXT,
                    summary JSONB DEFAULT '{}',
                    metadata JSONB DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_observer_forum_runs_time
                    ON observer_forum_runs(timestamp DESC);
            """)

            # Bias profiles table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bias_profiles (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    bias_type VARCHAR(50) NOT NULL,
                    score REAL,
                    sample_size INTEGER NOT NULL DEFAULT 0,
                    sufficient_data BOOLEAN NOT NULL DEFAULT FALSE,
                    details JSONB DEFAULT '{}',
                    evolution_run_id VARCHAR(100),
                    generation INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_bias_agent
                    ON bias_profiles(agent_id);
                CREATE INDEX IF NOT EXISTS idx_bias_agent_type
                    ON bias_profiles(agent_id, bias_type);
            """)

            # Contagion snapshots table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS contagion_snapshots (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    tick INTEGER,
                    metric_type VARCHAR(50) NOT NULL,
                    value REAL,
                    sample_size INTEGER NOT NULL DEFAULT 0,
                    sufficient_data BOOLEAN NOT NULL DEFAULT FALSE,
                    details JSONB DEFAULT '{}',
                    agent_count INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_contagion_metric
                    ON contagion_snapshots(metric_type);
                CREATE INDEX IF NOT EXISTS idx_contagion_time
                    ON contagion_snapshots(created_at DESC);
            """)

            # Observer journal table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS observer_journal (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    journal_date DATE NOT NULL,
                    generated_at TIMESTAMPTZ NOT NULL,
                    lookback_hours INTEGER NOT NULL DEFAULT 24,
                    full_markdown TEXT NOT NULL,
                    market_summary TEXT DEFAULT '',

                    forum_summary TEXT DEFAULT '',
                    learning_summary TEXT DEFAULT '',
                    recommendations TEXT DEFAULT '',
                    agent_reports JSONB DEFAULT '{}',
                    metrics JSONB DEFAULT '{}',
                    model VARCHAR(100)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_journal_date
                    ON observer_journal(journal_date);
                CREATE INDEX IF NOT EXISTS idx_journal_generated
                    ON observer_journal(generated_at DESC);
            """)

            # Experiment orchestration tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS experiment_runs (
                    id VARCHAR(100) PRIMARY KEY,
                    name VARCHAR(255),
                    status VARCHAR(50) DEFAULT 'pending',
                    config JSONB DEFAULT '{}',
                    best_fitness REAL,
                    validation_fitness REAL,
                    overfit_warning BOOLEAN DEFAULT FALSE,
                    total_cost_usd DECIMAL(10,4) DEFAULT 0,
                    generations_completed INTEGER DEFAULT 0,
                    promotion_candidates JSONB DEFAULT '[]',
                    best_genome JSONB,
                    error TEXT DEFAULT '',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_experiment_runs_status
                    ON experiment_runs(status, created_at DESC);

                CREATE TABLE IF NOT EXISTS promotion_queue (
                    id SERIAL PRIMARY KEY,
                    experiment_id VARCHAR(100) REFERENCES experiment_runs(id) ON DELETE CASCADE,
                    genome JSONB NOT NULL,
                    fitness REAL NOT NULL,
                    validation_fitness REAL,
                    status VARCHAR(50) DEFAULT 'pending',
                    reviewed_by VARCHAR(100),
                    reviewed_at TIMESTAMPTZ,
                    deploy_config JSONB,
                    notes TEXT DEFAULT '',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_promotion_queue_status
                    ON promotion_queue(status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_promotion_queue_experiment
                    ON promotion_queue(experiment_id);
            """)

            # Reflexion tables (trade reflections, failure clusters, skill proposals)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_reflections (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    trade_id VARCHAR(100),
                    decision_id INTEGER,
                    symbol VARCHAR(20),
                    side VARCHAR(10),
                    entry_price DECIMAL(20,8),
                    exit_price DECIMAL(20,8),
                    realized_pnl DECIMAL(20,8),
                    market_regime VARCHAR(50),
                    entry_signal TEXT,
                    what_went_right TEXT,
                    what_went_wrong TEXT,
                    lesson TEXT NOT NULL,
                    lesson_embedding vector(1536),
                    outcome VARCHAR(20),
                    confidence REAL,
                    metabolic_score REAL DEFAULT 1.0,
                    last_accessed TIMESTAMPTZ,
                    access_count INTEGER DEFAULT 0,
                    is_digested BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_reflections_agent
                    ON trade_reflections(agent_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_reflections_outcome
                    ON trade_reflections(outcome);
                CREATE INDEX IF NOT EXISTS idx_reflections_regime
                    ON trade_reflections(market_regime);
                CREATE INDEX IF NOT EXISTS idx_reflections_trade_id
                    ON trade_reflections(trade_id);

                CREATE TABLE IF NOT EXISTS failure_clusters (
                    id SERIAL PRIMARY KEY,
                    cluster_label VARCHAR(200) NOT NULL,
                    regime VARCHAR(50),
                    failure_mode TEXT NOT NULL,
                    reflection_ids INTEGER[] DEFAULT '{}',
                    sample_size INTEGER DEFAULT 0,
                    proposed_skill TEXT,
                    proposed_skill_validated BOOLEAN DEFAULT FALSE,
                    improvement_pct REAL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_failure_clusters_regime
                    ON failure_clusters(regime);

                CREATE TABLE IF NOT EXISTS skill_proposals (
                    id SERIAL PRIMARY KEY,
                    cluster_id INTEGER REFERENCES failure_clusters(id),
                    skill_name VARCHAR(100) NOT NULL,
                    skill_content TEXT NOT NULL,
                    treatment_fitness REAL,
                    control_fitness REAL,
                    improvement_pct REAL,
                    status VARCHAR(50) DEFAULT 'proposed',
                    deployed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_skill_proposals_status
                    ON skill_proposals(status);
            """)

            # Metabolic memory tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS abstract_principles (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    principle TEXT NOT NULL,
                    source_type VARCHAR(50) NOT NULL,
                    regime VARCHAR(50),
                    symbol VARCHAR(20),
                    confidence REAL DEFAULT 0.5,
                    application_count INTEGER DEFAULT 0,
                    last_applied TIMESTAMPTZ,
                    source_reflection_ids INTEGER[] DEFAULT '{}',
                    source_pattern_ids INTEGER[] DEFAULT '{}',
                    principle_embedding vector(1536),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_principles_agent
                    ON abstract_principles(agent_id, is_active);
                CREATE INDEX IF NOT EXISTS idx_principles_regime
                    ON abstract_principles(regime);

                CREATE TABLE IF NOT EXISTS memory_access_log (
                    id SERIAL PRIMARY KEY,
                    memory_type VARCHAR(50) NOT NULL,
                    memory_id INTEGER NOT NULL,
                    agent_id VARCHAR(100) NOT NULL,
                    accessed_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_memory_access_log_memory
                    ON memory_access_log(memory_type, memory_id);

                CREATE TABLE IF NOT EXISTS digestion_history (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(100) NOT NULL,
                    memories_scored INTEGER DEFAULT 0,
                    memories_digested INTEGER DEFAULT 0,
                    memories_pruned INTEGER DEFAULT 0,
                    principles_created INTEGER DEFAULT 0,
                    details JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_digestion_history_agent
                    ON digestion_history(agent_id, created_at DESC);
            """)

    # =========================================================================
    # Core Storage Methods (matching SQLiteStorage interface)
    # =========================================================================

    async def save_decision(self, decision: dict) -> int:
        """Save a decision to the database."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO decisions (
                    agent_id, tick, timestamp, action, symbol, size,
                    leverage, confidence, reasoning, metadata, trade_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                decision["agent_id"],
                decision["tick"],
                parse_timestamp(decision["timestamp"]),
                decision["action"],
                decision.get("symbol"),
                Decimal(str(decision["size"])) if decision.get("size") else None,
                decision.get("leverage"),
                decision.get("confidence"),
                decision.get("reasoning"),
                json.dumps(decision.get("metadata", {})),
                decision.get("trade_id"),
            )
            return row["id"]

    async def save_trade(self, trade: dict) -> None:
        """Save a trade to the database."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trades (
                    id, agent_id, symbol, side, size, price,
                    leverage, fee, realized_pnl, timestamp, decision_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (id) DO NOTHING
                """,
                trade["id"],
                trade["agent_id"],
                trade["symbol"],
                trade["side"],
                Decimal(str(trade["size"])),
                Decimal(str(trade["price"])),
                trade["leverage"],
                Decimal(str(trade["fee"])),
                Decimal(str(trade["realized_pnl"])) if trade.get("realized_pnl") else None,
                parse_timestamp(trade["timestamp"]),
                trade.get("decision_id"),
            )

    async def save_snapshot(
        self,
        tick: int,
        timestamp: str,
        leaderboard: list[dict],
        market_data: Optional[dict] = None,
        competition_id: Optional[int] = None,
    ) -> None:
        """Save a tick snapshot."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO snapshots (competition_id, tick, timestamp, leaderboard, market_data)
                VALUES ($1, $2, $3, $4, $5)
                """,
                competition_id,
                tick,
                parse_timestamp(timestamp),
                json.dumps(leaderboard),
                json.dumps(market_data) if market_data else None,
            )

    async def get_recent_decisions(
        self,
        agent_id: str,
        limit: int = 20,
    ) -> list[dict]:
        """Get recent decisions for an agent."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM decisions
                WHERE agent_id = $1
                ORDER BY tick DESC
                LIMIT $2
                """,
                agent_id,
                limit,
            )

            decisions = []
            for row in rows:
                d = dict(row)
                if d.get("metadata"):
                    d["metadata"] = (
                        json.loads(d["metadata"])
                        if isinstance(d["metadata"], str)
                        else d["metadata"]
                    )
                # Convert Decimal to float for JSON serialization
                if d.get("size"):
                    d["size"] = float(d["size"])
                decisions.append(d)

            return decisions

    async def get_agent_trades(
        self,
        agent_id: str,
        limit: int = 50,
    ) -> list[dict]:
        """Get trades for an agent."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM trades
                WHERE agent_id = $1
                ORDER BY timestamp DESC
                LIMIT $2
                """,
                agent_id,
                limit,
            )

            trades = []
            for row in rows:
                t = dict(row)
                # Convert Decimal to float
                for key in ["size", "price", "fee", "realized_pnl"]:
                    if t.get(key) is not None:
                        t[key] = float(t[key])
                trades.append(t)

            return trades

    async def get_leaderboard_history(
        self,
        limit: int = 100,
    ) -> list[dict]:
        """Get historical leaderboard snapshots in ascending order for charts."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT tick, timestamp, leaderboard FROM (
                    SELECT tick, timestamp, leaderboard FROM snapshots
                    ORDER BY tick DESC
                    LIMIT $1
                ) sub ORDER BY tick ASC
                """,
                limit,
            )

            return [
                {
                    "tick": row["tick"],
                    "timestamp": str(row["timestamp"]),
                    "leaderboard": (
                        json.loads(row["leaderboard"])
                        if isinstance(row["leaderboard"], str)
                        else row["leaderboard"]
                    ),
                }
                for row in rows
            ]

    async def save_funding_payment(
        self,
        tick: int,
        timestamp: str,
        payment: dict,
    ) -> None:
        """Save a funding payment record."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO funding_payments (
                    tick, agent_id, symbol, side, funding_rate,
                    notional, amount, direction, timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                tick,
                payment["agent_id"],
                payment["symbol"],
                payment["side"],
                Decimal(str(payment["funding_rate"])),
                Decimal(str(payment["notional"])),
                Decimal(str(payment["amount"])),
                payment["direction"],
                parse_timestamp(timestamp),
            )

    async def save_liquidation(
        self,
        tick: int,
        timestamp: str,
        liquidation: dict,
    ) -> None:
        """Save a liquidation event."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO liquidations (
                    tick, agent_id, symbol, side, size, entry_price,
                    liquidation_price, mark_price, margin_lost, fee,
                    total_loss, timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                tick,
                liquidation["agent_id"],
                liquidation["symbol"],
                liquidation["side"],
                Decimal(str(liquidation["size"])),
                Decimal(str(liquidation["entry_price"])),
                Decimal(str(liquidation["liquidation_price"])),
                Decimal(str(liquidation["mark_price"])),
                Decimal(str(liquidation["margin_lost"])),
                Decimal(str(liquidation["fee"])),
                Decimal(str(liquidation["total_loss"])),
                parse_timestamp(timestamp),
            )

    async def save_sl_tp_trigger(
        self,
        tick: int,
        timestamp: str,
        trigger: dict,
    ) -> None:
        """Save a stop-loss/take-profit trigger event."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO sl_tp_triggers (
                    tick, agent_id, symbol, side, trigger_type,
                    trigger_price, mark_price, size, realized_pnl,
                    fee, timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                tick,
                trigger["agent_id"],
                trigger["symbol"],
                trigger["side"],
                trigger["trigger_type"],
                Decimal(str(trigger["trigger_price"])),
                Decimal(str(trigger["mark_price"])),
                Decimal(str(trigger["size"])),
                Decimal(str(trigger["realized_pnl"])),
                Decimal(str(trigger["fee"])),
                parse_timestamp(timestamp),
            )

    async def get_funding_history(
        self,
        agent_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get funding payment history."""
        async with self.pool.acquire() as conn:
            if agent_id:
                rows = await conn.fetch(
                    """
                    SELECT * FROM funding_payments
                    WHERE agent_id = $1
                    ORDER BY tick DESC
                    LIMIT $2
                    """,
                    agent_id,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM funding_payments
                    ORDER BY tick DESC
                    LIMIT $1
                    """,
                    limit,
                )

            results = []
            for row in rows:
                d = dict(row)
                d["funding_rate"] = float(d["funding_rate"]) if d.get("funding_rate") else 0.0
                d["notional"] = float(d["notional"]) if d.get("notional") else 0.0
                d["amount"] = float(d["amount"]) if d.get("amount") else 0.0
                d["timestamp"] = str(d["timestamp"])
                results.append(d)
            return results

    async def get_liquidation_history(
        self,
        agent_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get liquidation history."""
        async with self.pool.acquire() as conn:
            if agent_id:
                rows = await conn.fetch(
                    """
                    SELECT * FROM liquidations
                    WHERE agent_id = $1
                    ORDER BY tick DESC
                    LIMIT $2
                    """,
                    agent_id,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM liquidations
                    ORDER BY tick DESC
                    LIMIT $1
                    """,
                    limit,
                )

            results = []
            for row in rows:
                d = dict(row)
                for key in [
                    "size", "entry_price", "liquidation_price", "mark_price",
                    "margin_lost", "fee", "total_loss"
                ]:
                    if d.get(key) is not None:
                        d[key] = float(d[key])
                d["timestamp"] = str(d["timestamp"])
                results.append(d)
            return results

    async def get_agent_funding_summary(self, agent_id: str) -> dict:
        """Get funding payment summary using SQL aggregation."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN direction='paid'
                        THEN ABS(amount::numeric) ELSE 0 END), 0) as paid,
                    COALESCE(SUM(CASE WHEN direction='received'
                        THEN ABS(amount::numeric) ELSE 0 END), 0) as received
                FROM funding_payments WHERE agent_id = $1
                """,
                agent_id,
            )
            paid = float(row["paid"]) if row else 0.0
            received = float(row["received"]) if row else 0.0
            return {"paid": paid, "received": received, "net": received - paid}

    async def get_agent_trade_count(self, agent_id: str) -> int:
        """Get trade count using SQL aggregation."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM trades WHERE agent_id = $1",
                agent_id,
            )

    async def get_agent_liquidation_count(self, agent_id: str) -> int:
        """Get liquidation count using SQL aggregation."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM liquidations WHERE agent_id = $1",
                agent_id,
            )

    async def get_agent_behavioral_stats(self, agent_id: str) -> dict:
        """Get behavioral statistics for an agent from decisions and trades."""
        async with self.pool.acquire() as conn:
            # Get action distribution
            action_rows = await conn.fetch(
                """
                SELECT action, COUNT(*) as count
                FROM decisions
                WHERE agent_id = $1
                GROUP BY action
                """,
                agent_id,
            )
            action_distribution = {row["action"]: row["count"] for row in action_rows}

            # Get confidence statistics
            conf_row = await conn.fetchrow(
                """
                SELECT
                    AVG(confidence) as avg_confidence,
                    MIN(confidence) as min_confidence,
                    MAX(confidence) as max_confidence,
                    COUNT(*) as total_decisions
                FROM decisions
                WHERE agent_id = $1 AND confidence IS NOT NULL
                """,
                agent_id,
            )
            avg_conf = conf_row["avg_confidence"]
            min_conf = conf_row["min_confidence"]
            max_conf = conf_row["max_confidence"]
            confidence_stats = {
                "average": round(avg_conf, 4) if avg_conf else 0,
                "min": round(min_conf, 4) if min_conf else 0,
                "max": round(max_conf, 4) if max_conf else 0,
                "total_decisions": conf_row["total_decisions"] or 0,
            }

            # Get symbol distribution from trades
            symbol_rows = await conn.fetch(
                """
                SELECT symbol, COUNT(*) as count
                FROM trades
                WHERE agent_id = $1
                GROUP BY symbol
                """,
                agent_id,
            )
            symbol_distribution = {row["symbol"]: row["count"] for row in symbol_rows}

            # Get long/short ratio from trades
            side_rows = await conn.fetch(
                """
                SELECT side, COUNT(*) as count
                FROM trades
                WHERE agent_id = $1
                GROUP BY side
                """,
                agent_id,
            )
            side_counts = {row["side"]: row["count"] for row in side_rows}
            long_count = side_counts.get("long", 0)
            short_count = side_counts.get("short", 0)
            total_sides = long_count + short_count
            if short_count > 0:
                long_short_ratio = round(long_count / short_count, 2)
            elif long_count > 0:
                long_short_ratio = float('inf')
            else:
                long_short_ratio = 0

            # Get average leverage from trades
            lev_row = await conn.fetchrow(
                """
                SELECT AVG(leverage) as avg_leverage
                FROM trades
                WHERE agent_id = $1
                """,
                agent_id,
            )
            avg_leverage = round(lev_row["avg_leverage"], 2) if lev_row["avg_leverage"] else 0

            return {
                "action_distribution": action_distribution,
                "confidence": confidence_stats,
                "symbol_distribution": symbol_distribution,
                "long_short_ratio": long_short_ratio,
                "long_count": long_count,
                "short_count": short_count,
                "long_pct": round(long_count / total_sides * 100, 1) if total_sides > 0 else 0,
                "short_pct": round(short_count / total_sides * 100, 1) if total_sides > 0 else 0,
                "average_leverage": avg_leverage,
            }

    # =========================================================================
    # Learning-Specific Methods
    # =========================================================================

    async def save_decision_context(
        self,
        decision_id: int,
        context: dict,
    ) -> int:
        """Save enriched context for a decision."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO decision_contexts (
                    decision_id, tick, timestamp, market_prices, candles,
                    indicators, portfolio_state, regime, volatility_percentile
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (decision_id) DO UPDATE SET
                    market_prices = EXCLUDED.market_prices,
                    candles = EXCLUDED.candles,
                    indicators = EXCLUDED.indicators,
                    portfolio_state = EXCLUDED.portfolio_state,
                    regime = EXCLUDED.regime,
                    volatility_percentile = EXCLUDED.volatility_percentile
                RETURNING id
                """,
                decision_id,
                context.get("tick"),
                parse_timestamp(context["timestamp"]) if context.get("timestamp") else None,
                json.dumps(context.get("market_prices", {})),
                json.dumps(context.get("candles", {})),
                json.dumps(context.get("indicators", {})),
                json.dumps(context.get("portfolio_state", {})),
                context.get("regime"),
                context.get("volatility_percentile"),
            )
            return row["id"]

    async def save_decision_outcome(self, outcome) -> int:
        """Save outcome for a decision."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO decision_outcomes (
                    decision_id, realized_pnl, holding_duration_ticks,
                    max_drawdown_during, max_profit_during, exit_reason,
                    outcome_score, risk_adjusted_return
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (decision_id) DO UPDATE SET
                    realized_pnl = EXCLUDED.realized_pnl,
                    holding_duration_ticks = EXCLUDED.holding_duration_ticks,
                    max_drawdown_during = EXCLUDED.max_drawdown_during,
                    max_profit_during = EXCLUDED.max_profit_during,
                    exit_reason = EXCLUDED.exit_reason,
                    outcome_score = EXCLUDED.outcome_score,
                    risk_adjusted_return = EXCLUDED.risk_adjusted_return
                RETURNING id
                """,
                outcome.decision_id,
                outcome.realized_pnl,
                outcome.holding_duration_ticks,
                outcome.max_drawdown_during,
                outcome.max_profit_during,
                outcome.exit_reason,
                outcome.outcome_score,
                outcome.risk_adjusted_return,
            )
            return row["id"]

    async def save_context_embedding(
        self,
        decision_id: int,
        embedding: list[float],
    ) -> None:
        """Save embedding for a decision context."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE decision_contexts
                SET context_embedding = $1::vector
                WHERE decision_id = $2
                """,
                embedding,
                decision_id,
            )

    async def find_similar_contexts(
        self,
        embedding: list[float],
        limit: int = 10,
        min_outcome_score: Optional[float] = None,
        regime: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> list[dict]:
        """Find similar historical contexts using vector similarity."""
        async with self.pool.acquire() as conn:
            query = """
                SELECT
                    dc.id,
                    dc.decision_id,
                    dc.tick,
                    dc.timestamp,
                    dc.market_prices,
                    dc.indicators,
                    dc.portfolio_state,
                    dc.regime,
                    d.action,
                    d.symbol,
                    d.reasoning,
                    d.confidence,
                    do.realized_pnl,
                    do.outcome_score,
                    do.exit_reason,
                    1 - (dc.context_embedding <=> $1::vector) as similarity
                FROM decision_contexts dc
                JOIN decisions d ON dc.decision_id = d.id
                LEFT JOIN decision_outcomes do ON d.id = do.decision_id
                WHERE dc.context_embedding IS NOT NULL
                  AND do.realized_pnl IS NOT NULL
            """

            params = [embedding]
            param_idx = 2

            if min_outcome_score is not None:
                query += f" AND do.outcome_score >= ${param_idx}"
                params.append(min_outcome_score)
                param_idx += 1

            if regime:
                query += f" AND dc.regime = ${param_idx}"
                params.append(regime)
                param_idx += 1

            if symbol:
                query += f" AND d.symbol = ${param_idx}"
                params.append(symbol)
                param_idx += 1

            query += f"""
                ORDER BY dc.context_embedding <=> $1::vector
                LIMIT ${param_idx}
            """
            params.append(limit)

            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                d = dict(row)
                # Convert JSON strings if needed
                for key in ["market_prices", "indicators", "portfolio_state"]:
                    if d.get(key) and isinstance(d[key], str):
                        d[key] = json.loads(d[key])
                # Convert Decimal to float
                if d.get("realized_pnl"):
                    d["realized_pnl"] = float(d["realized_pnl"])
                d["timestamp"] = str(d["timestamp"])
                results.append(d)

            return results

    async def get_decision_context(self, decision_id: int) -> Optional[dict]:
        """Get context for a specific decision."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM decision_contexts
                WHERE decision_id = $1
                """,
                decision_id,
            )
            if not row:
                return None

            d = dict(row)
            for key in ["market_prices", "candles", "indicators", "portfolio_state"]:
                if d.get(key) and isinstance(d[key], str):
                    d[key] = json.loads(d[key])
            return d

    async def get_latest_decision_context(self, agent_id: str) -> Optional[dict]:
        """Get the most recent decision context for an agent."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT dc.* FROM decision_contexts dc
                JOIN decisions d ON dc.decision_id = d.id
                WHERE d.agent_id = $1
                ORDER BY dc.tick DESC
                LIMIT 1
                """,
                agent_id,
            )
            if not row:
                return None

            d = dict(row)
            for key in ["market_prices", "candles", "indicators", "portfolio_state"]:
                if d.get(key) and isinstance(d[key], str):
                    d[key] = json.loads(d[key])
            return d

    async def save_learned_pattern(self, pattern: dict) -> int:
        """Save a learned trading pattern."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO learned_patterns (
                    agent_id, pattern_type, pattern_description, conditions,
                    recommended_action, supporting_decisions, success_rate,
                    sample_size, confidence
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id
                """,
                pattern.get("agent_id"),
                pattern["pattern_type"],
                pattern["pattern_description"],
                json.dumps(pattern["conditions"]),
                pattern.get("recommended_action"),
                pattern.get("supporting_decisions", []),
                pattern.get("success_rate"),
                pattern.get("sample_size"),
                pattern.get("confidence"),
            )
            return row["id"]

    async def get_active_patterns(
        self,
        agent_id: Optional[str] = None,
        pattern_types: Optional[list[str]] = None,
        min_confidence: float = 0.5,
    ) -> list[dict]:
        """Get active learned patterns."""
        async with self.pool.acquire() as conn:
            query = """
                SELECT * FROM learned_patterns
                WHERE is_active = true
                  AND confidence >= $1
            """
            params = [min_confidence]
            param_idx = 2

            if agent_id:
                query += f" AND (agent_id = ${param_idx} OR agent_id IS NULL)"
                params.append(agent_id)
                param_idx += 1

            if pattern_types:
                query += f" AND pattern_type = ANY(${param_idx})"
                params.append(pattern_types)
                param_idx += 1

            query += " ORDER BY confidence DESC"

            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                d = dict(row)
                if d.get("conditions") and isinstance(d["conditions"], str):
                    d["conditions"] = json.loads(d["conditions"])
                if d.get("discovered_at"):
                    d["discovered_at"] = str(d["discovered_at"])
                if d.get("last_validated"):
                    d["last_validated"] = str(d["last_validated"])
                results.append(d)

            return results

    async def get_agent_patterns(
        self,
        agent_id: str,
        pattern_type: Optional[str] = None,
        min_confidence: float = 0.5,
    ) -> list[dict]:
        """Get patterns for a specific agent."""
        return await self.get_active_patterns(
            agent_id=agent_id,
            pattern_types=[pattern_type] if pattern_type else None,
            min_confidence=min_confidence,
        )

    async def get_regime_performance(
        self,
        regime: str,
        symbol: Optional[str] = None,
        min_trades: int = 10,
    ) -> list[dict]:
        """Get agent performance in a specific regime."""
        async with self.pool.acquire() as conn:
            query = """
                SELECT * FROM regime_performance
                WHERE regime = $1
                  AND total_trades >= $2
            """
            params = [regime, min_trades]
            param_idx = 3

            if symbol:
                query += f" AND (symbol = ${param_idx} OR symbol IS NULL)"
                params.append(symbol)

            query += " ORDER BY sharpe_ratio DESC NULLS LAST"

            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                d = dict(row)
                if d.get("total_pnl"):
                    d["total_pnl"] = float(d["total_pnl"])
                if d.get("period_start"):
                    d["period_start"] = str(d["period_start"])
                if d.get("period_end"):
                    d["period_end"] = str(d["period_end"])
                d["win_rate"] = (
                    d["winning_trades"] / d["total_trades"]
                    if d["total_trades"] > 0
                    else 0
                )
                results.append(d)

            return results

    async def update_regime_performance(
        self,
        agent_id: str,
        regime: str,
        symbol: Optional[str],
        trade_result: dict,
    ) -> None:
        """Update regime performance after a trade."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO regime_performance (
                    agent_id, regime, symbol, total_trades, winning_trades,
                    total_pnl, period_start, updated_at
                ) VALUES ($1, $2, $3, 1, $4, $5, NOW(), NOW())
                ON CONFLICT (agent_id, regime, symbol) DO UPDATE SET
                    total_trades = regime_performance.total_trades + 1,
                    winning_trades = regime_performance.winning_trades + $4,
                    total_pnl = regime_performance.total_pnl + $5,
                    period_end = NOW(),
                    updated_at = NOW()
                """,
                agent_id,
                regime,
                symbol,
                1 if trade_result.get("pnl", 0) > 0 else 0,
                Decimal(str(trade_result.get("pnl", 0))),
            )

    async def save_learning_event(
        self,
        agent_id: str,
        event_type: str,
        summary: str,
        details: Optional[dict] = None,
    ) -> int:
        """Save a learning event."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO learning_events (agent_id, event_type, summary, details)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                agent_id,
                event_type,
                summary,
                json.dumps(details) if details else None,
            )
            return row["id"]

    async def get_recent_learning_events(
        self,
        agent_id: Optional[str] = None,
        limit: int = 20,
    ) -> list[dict]:
        """Get recent learning events."""
        async with self.pool.acquire() as conn:
            if agent_id:
                rows = await conn.fetch(
                    """
                    SELECT * FROM learning_events
                    WHERE agent_id = $1
                    ORDER BY timestamp DESC
                    LIMIT $2
                    """,
                    agent_id,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM learning_events
                    ORDER BY timestamp DESC
                    LIMIT $1
                    """,
                    limit,
                )

            results = []
            for row in rows:
                d = dict(row)
                if d.get("details") and isinstance(d["details"], str):
                    d["details"] = json.loads(d["details"])
                d["timestamp"] = str(d["timestamp"])
                results.append(d)

            return results

    async def get_learning_stats(self, agent_id: str) -> dict:
        """Get learning statistics for an agent."""
        async with self.pool.acquire() as conn:
            # Count decisions with contexts
            context_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM decision_contexts dc
                JOIN decisions d ON dc.decision_id = d.id
                WHERE d.agent_id = $1
                """,
                agent_id,
            )

            # Count patterns
            pattern_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM learned_patterns
                WHERE agent_id = $1 OR agent_id IS NULL
                """,
                agent_id,
            )

            # Count learning events
            event_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM learning_events
                WHERE agent_id = $1
                """,
                agent_id,
            )

            return {
                "total_rag_queries": context_count or 0,
                "patterns_learned": pattern_count or 0,
                "reflections_count": event_count or 0,
                "improvement_pct": 0,  # Would need baseline comparison
            }

    async def get_learning_curve(
        self,
        agent_id: str,
        window_size: int = 50,
    ) -> list[dict]:
        """Get learning curve data showing performance over time."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH numbered_trades AS (
                    SELECT
                        t.id,
                        t.timestamp,
                        t.realized_pnl,
                        ROW_NUMBER() OVER (ORDER BY t.timestamp) as trade_num
                    FROM trades t
                    WHERE t.agent_id = $1
                      AND t.realized_pnl IS NOT NULL
                ),
                windowed AS (
                    SELECT
                        trade_num,
                        timestamp,
                        realized_pnl,
                        AVG(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END)
                            OVER (ORDER BY trade_num ROWS BETWEEN $2 PRECEDING AND CURRENT ROW)
                            as rolling_win_rate,
                        SUM(realized_pnl)
                            OVER (ORDER BY trade_num ROWS BETWEEN $2 PRECEDING AND CURRENT ROW)
                            as rolling_pnl
                    FROM numbered_trades
                )
                SELECT
                    trade_num,
                    timestamp,
                    rolling_win_rate,
                    rolling_pnl
                FROM windowed
                WHERE trade_num % 10 = 0 OR trade_num = (SELECT MAX(trade_num) FROM windowed)
                ORDER BY trade_num
                """,
                agent_id,
                window_size,
            )

            return [
                {
                    "trade_num": row["trade_num"],
                    "timestamp": str(row["timestamp"]),
                    "win_rate": float(row["rolling_win_rate"]) if row["rolling_win_rate"] else 0,
                    "rolling_pnl": float(row["rolling_pnl"]) if row["rolling_pnl"] else 0,
                }
                for row in rows
            ]

    async def create_vector_index(self, lists: int = 100) -> None:
        """Create IVFFlat vector index for efficient similarity search."""
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_context_embedding ON decision_contexts
                    USING ivfflat (context_embedding vector_cosine_ops) WITH (lists = {lists})
            """)

    # =========================================================================
    # Long-term Archival Methods
    # =========================================================================

    async def save_daily_snapshot(self, snapshot: dict) -> int:
        """Save a daily performance snapshot for an agent."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO daily_snapshots (
                    date, agent_id, starting_equity, ending_equity,
                    daily_pnl, daily_pnl_pct, trade_count, win_count, loss_count,
                    total_volume, total_fees, total_funding, max_drawdown_pct,
                    sharpe_estimate, avg_confidence, regime_distribution,
                    symbol_distribution, skill_version_hash, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                ON CONFLICT (date, agent_id) DO UPDATE SET
                    ending_equity = EXCLUDED.ending_equity,
                    daily_pnl = EXCLUDED.daily_pnl,
                    daily_pnl_pct = EXCLUDED.daily_pnl_pct,
                    trade_count = EXCLUDED.trade_count,
                    win_count = EXCLUDED.win_count,
                    loss_count = EXCLUDED.loss_count,
                    total_volume = EXCLUDED.total_volume,
                    total_fees = EXCLUDED.total_fees,
                    total_funding = EXCLUDED.total_funding,
                    max_drawdown_pct = EXCLUDED.max_drawdown_pct,
                    sharpe_estimate = EXCLUDED.sharpe_estimate,
                    avg_confidence = EXCLUDED.avg_confidence,
                    regime_distribution = EXCLUDED.regime_distribution,
                    symbol_distribution = EXCLUDED.symbol_distribution,
                    skill_version_hash = EXCLUDED.skill_version_hash,
                    metadata = EXCLUDED.metadata
                RETURNING id
                """,
                snapshot["date"],
                snapshot["agent_id"],
                Decimal(str(snapshot["starting_equity"])),
                Decimal(str(snapshot["ending_equity"])),
                Decimal(str(snapshot["daily_pnl"])),
                snapshot["daily_pnl_pct"],
                snapshot.get("trade_count", 0),
                snapshot.get("win_count", 0),
                snapshot.get("loss_count", 0),
                Decimal(str(snapshot.get("total_volume", 0))),
                Decimal(str(snapshot.get("total_fees", 0))),
                Decimal(str(snapshot.get("total_funding", 0))),
                snapshot.get("max_drawdown_pct"),
                snapshot.get("sharpe_estimate"),
                snapshot.get("avg_confidence"),
                json.dumps(snapshot.get("regime_distribution", {})),
                json.dumps(snapshot.get("symbol_distribution", {})),
                snapshot.get("skill_version_hash"),
                json.dumps(snapshot.get("metadata", {})),
            )
            return row["id"]

    async def archive_decision(self, decision: dict, context: dict) -> int:
        """Archive a decision with full context for ML training."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO decision_archive (
                    decision_id, agent_id, timestamp, tick, action, symbol,
                    size, leverage, confidence, reasoning,
                    market_prices, market_changes_1h, market_changes_24h,
                    volatility_state, indicators, regime,
                    portfolio_equity, portfolio_positions, available_margin
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                RETURNING id
                """,
                decision.get("id"),
                decision["agent_id"],
                decision["timestamp"],
                decision["tick"],
                decision["action"],
                decision.get("symbol"),
                Decimal(str(decision["size"])) if decision.get("size") else None,
                decision.get("leverage"),
                decision.get("confidence"),
                decision.get("reasoning"),
                json.dumps(context.get("market_prices", {})),
                json.dumps(context.get("market_changes_1h", {})),
                json.dumps(context.get("market_changes_24h", {})),
                json.dumps(context.get("volatility_state", {})),
                json.dumps(context.get("indicators", {})),
                context.get("regime"),
                Decimal(str(context["portfolio_equity"])) if context.get("portfolio_equity") else None,
                json.dumps(context.get("portfolio_positions", {})),
                Decimal(str(context["available_margin"])) if context.get("available_margin") else None,
            )
            return row["id"]

    async def update_decision_outcome(
        self,
        archive_id: int,
        outcome: dict,
    ) -> None:
        """Update the outcome for an archived decision."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE decision_archive SET
                    outcome_pnl = $1,
                    outcome_duration_ticks = $2,
                    outcome_exit_reason = $3,
                    outcome_max_profit = $4,
                    outcome_max_drawdown = $5
                WHERE id = $6
                """,
                Decimal(str(outcome["pnl"])) if outcome.get("pnl") is not None else None,
                outcome.get("duration_ticks"),
                outcome.get("exit_reason"),
                Decimal(str(outcome["max_profit"])) if outcome.get("max_profit") is not None else None,
                Decimal(str(outcome["max_drawdown"])) if outcome.get("max_drawdown") is not None else None,
                archive_id,
            )

    async def save_archive_embedding(
        self,
        archive_id: int,
        embedding: list[float],
    ) -> None:
        """Save embedding for an archived decision."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE decision_archive
                SET context_embedding = $1::vector
                WHERE id = $2
                """,
                embedding,
                archive_id,
            )

    async def save_skill_version(
        self,
        skill_name: str,
        version_hash: str,
        content: str,
        metadata: dict,
    ) -> int:
        """Save a skill version for tracking."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO skill_versions (
                    skill_name, version_hash, content,
                    pattern_count, active_patterns, total_samples, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (skill_name, version_hash) DO NOTHING
                RETURNING id
                """,
                skill_name,
                version_hash,
                content,
                metadata.get("pattern_count", 0),
                metadata.get("active_patterns", 0),
                metadata.get("total_samples", 0),
                json.dumps(metadata),
            )
            return row["id"] if row else None

    async def start_competition_session(
        self,
        session_id: str,
        name: str,
        config: dict,
    ) -> int:
        """Start a new competition session."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO competition_sessions (
                    session_id, name, config, started_at
                ) VALUES ($1, $2, $3, NOW())
                ON CONFLICT (session_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    config = EXCLUDED.config,
                    started_at = NOW()
                RETURNING id
                """,
                session_id,
                name,
                json.dumps(config),
            )
            return row["id"]

    async def end_competition_session(
        self,
        session_id: str,
        total_ticks: int,
        final_leaderboard: list[dict],
        skill_versions: dict,
    ) -> None:
        """End a competition session."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE competition_sessions SET
                    ended_at = NOW(),
                    total_ticks = $1,
                    final_leaderboard = $2,
                    skill_versions_used = $3
                WHERE session_id = $4
                """,
                total_ticks,
                json.dumps(final_leaderboard),
                json.dumps(skill_versions),
                session_id,
            )

    async def get_daily_snapshots(
        self,
        agent_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 365,
    ) -> list[dict]:
        """Get daily snapshots for analysis."""
        async with self.pool.acquire() as conn:
            query = "SELECT * FROM daily_snapshots WHERE 1=1"
            params = []
            param_idx = 1

            if agent_id:
                query += f" AND agent_id = ${param_idx}"
                params.append(agent_id)
                param_idx += 1

            if start_date:
                query += f" AND date >= ${param_idx}"
                params.append(start_date)
                param_idx += 1

            if end_date:
                query += f" AND date <= ${param_idx}"
                params.append(end_date)
                param_idx += 1

            query += f" ORDER BY date DESC LIMIT ${param_idx}"
            params.append(limit)

            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                d = dict(row)
                for key in ["starting_equity", "ending_equity", "daily_pnl",
                            "total_volume", "total_fees", "total_funding"]:
                    if d.get(key) is not None:
                        d[key] = float(d[key])
                d["date"] = str(d["date"])
                if d.get("regime_distribution") and isinstance(d["regime_distribution"], str):
                    d["regime_distribution"] = json.loads(d["regime_distribution"])
                if d.get("symbol_distribution") and isinstance(d["symbol_distribution"], str):
                    d["symbol_distribution"] = json.loads(d["symbol_distribution"])
                results.append(d)

            return results

    async def find_similar_archived_decisions(
        self,
        embedding: list[float],
        limit: int = 20,
        min_outcome_pnl: Optional[float] = None,
        regime: Optional[str] = None,
    ) -> list[dict]:
        """Find similar archived decisions using vector similarity."""
        async with self.pool.acquire() as conn:
            query = """
                SELECT
                    id, decision_id, agent_id, timestamp, action, symbol,
                    confidence, reasoning, regime, outcome_pnl, outcome_exit_reason,
                    1 - (context_embedding <=> $1::vector) as similarity
                FROM decision_archive
                WHERE context_embedding IS NOT NULL
            """
            params = [embedding]
            param_idx = 2

            if min_outcome_pnl is not None:
                query += f" AND outcome_pnl >= ${param_idx}"
                params.append(Decimal(str(min_outcome_pnl)))
                param_idx += 1

            if regime:
                query += f" AND regime = ${param_idx}"
                params.append(regime)
                param_idx += 1

            query += f"""
                ORDER BY context_embedding <=> $1::vector
                LIMIT ${param_idx}
            """
            params.append(limit)

            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                d = dict(row)
                if d.get("outcome_pnl") is not None:
                    d["outcome_pnl"] = float(d["outcome_pnl"])
                d["timestamp"] = str(d["timestamp"])
                results.append(d)

            return results

    async def get_agent_performance_over_time(
        self,
        agent_id: str,
        days: int = 30,
    ) -> dict:
        """Get agent performance metrics over time from daily snapshots."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    date, daily_pnl, daily_pnl_pct, ending_equity,
                    trade_count, win_count, max_drawdown_pct
                FROM daily_snapshots
                WHERE agent_id = $1
                  AND date >= CURRENT_DATE - $2 * INTERVAL '1 day'
                ORDER BY date ASC
                """,
                agent_id,
                days,
            )

            if not rows:
                return {"agent_id": agent_id, "data": []}

            data = []
            cumulative_pnl = Decimal("0")
            for row in rows:
                cumulative_pnl += row["daily_pnl"]
                data.append({
                    "date": str(row["date"]),
                    "daily_pnl": float(row["daily_pnl"]),
                    "daily_pnl_pct": row["daily_pnl_pct"],
                    "equity": float(row["ending_equity"]),
                    "cumulative_pnl": float(cumulative_pnl),
                    "trades": row["trade_count"],
                    "wins": row["win_count"],
                    "win_rate": row["win_count"] / row["trade_count"] if row["trade_count"] > 0 else 0,
                    "max_drawdown_pct": row["max_drawdown_pct"],
                })

            return {
                "agent_id": agent_id,
                "days": len(data),
                "total_pnl": float(cumulative_pnl),
                "avg_daily_pnl": float(cumulative_pnl / len(data)) if data else 0,
                "data": data,
            }

    async def create_archive_vector_index(self, lists: int = 100) -> None:
        """Create IVFFlat vector index for decision archive similarity search."""
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_archive_embedding ON decision_archive
                    USING ivfflat (context_embedding vector_cosine_ops) WITH (lists = {lists})
            """)

    # =========================================================================
    # Arena State Persistence (for competition resume)
    # =========================================================================

    async def save_arena_state(
        self,
        competition_name: str,
        tick: int,
        timestamp: datetime,
        current_prices: dict[str, Decimal],
        arena: "TradingArena",
        config: Optional[dict] = None,
    ) -> None:
        """Save complete arena state for resume capability."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Save arena state
                await conn.execute(
                    """
                    INSERT INTO arena_state (competition_name, last_tick, last_timestamp, current_prices, config)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (competition_name) DO UPDATE SET
                        last_tick = EXCLUDED.last_tick,
                        last_timestamp = EXCLUDED.last_timestamp,
                        current_prices = EXCLUDED.current_prices,
                        config = EXCLUDED.config,
                        updated_at = NOW()
                    """,
                    competition_name,
                    tick,
                    timestamp,
                    json.dumps({k: str(v) for k, v in current_prices.items()}),
                    json.dumps(config) if config else None,
                )

                # Save each portfolio
                for agent_id, portfolio in arena.portfolios.items():
                    # Save portfolio state
                    await conn.execute(
                        """
                        INSERT INTO portfolio_state (
                            competition_name, agent_id, initial_capital,
                            available_margin, realized_pnl, funding_paid, funding_received
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (competition_name, agent_id) DO UPDATE SET
                            initial_capital = EXCLUDED.initial_capital,
                            available_margin = EXCLUDED.available_margin,
                            realized_pnl = EXCLUDED.realized_pnl,
                            funding_paid = EXCLUDED.funding_paid,
                            funding_received = EXCLUDED.funding_received,
                            updated_at = NOW()
                        """,
                        competition_name,
                        agent_id,
                        portfolio.initial_capital,
                        portfolio.available_margin,
                        portfolio.realized_pnl,
                        arena.funding_paid.get(agent_id, Decimal("0")),
                        arena.funding_received.get(agent_id, Decimal("0")),
                    )

                    # Delete old positions for this agent, then insert current ones
                    await conn.execute(
                        "DELETE FROM position_state WHERE competition_name = $1 AND agent_id = $2",
                        competition_name,
                        agent_id,
                    )

                    for symbol, position in portfolio.positions.items():
                        await conn.execute(
                            """
                            INSERT INTO position_state (
                                competition_name, agent_id, position_id, symbol, side,
                                size, entry_price, leverage, margin, opened_at,
                                stop_loss_price, take_profit_price
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                            """,
                            competition_name,
                            agent_id,
                            position.id,
                            position.symbol,
                            position.side.value,
                            position.size,
                            position.entry_price,
                            position.leverage,
                            position.margin,
                            position.opened_at,
                            position.stop_loss_price,
                            position.take_profit_price,
                        )

                    # Delete old pending orders, then insert current ones
                    await conn.execute(
                        "DELETE FROM pending_order_state WHERE competition_name = $1 AND agent_id = $2",
                        competition_name,
                        agent_id,
                    )

                    for order in portfolio.pending_orders:
                        await conn.execute(
                            """
                            INSERT INTO pending_order_state (
                                competition_name, agent_id, order_id, symbol, order_type,
                                size, limit_price, leverage, created_at,
                                stop_loss_price, take_profit_price
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            """,
                            competition_name,
                            agent_id,
                            order.id,
                            order.symbol,
                            order.order_type.value,
                            order.size,
                            order.limit_price,
                            order.leverage,
                            order.created_at,
                            order.stop_loss_price,
                            order.take_profit_price,
                        )

    async def load_arena_state(self, competition_name: str) -> Optional[dict]:
        """Load arena state for resuming a competition."""
        async with self.pool.acquire() as conn:
            # Load arena state
            arena_row = await conn.fetchrow(
                "SELECT * FROM arena_state WHERE competition_name = $1",
                competition_name,
            )

            if not arena_row:
                return None

            # Load all portfolio states
            portfolio_rows = await conn.fetch(
                "SELECT * FROM portfolio_state WHERE competition_name = $1",
                competition_name,
            )

            # Load all positions
            position_rows = await conn.fetch(
                "SELECT * FROM position_state WHERE competition_name = $1",
                competition_name,
            )

            # Load all pending orders
            order_rows = await conn.fetch(
                "SELECT * FROM pending_order_state WHERE competition_name = $1",
                competition_name,
            )

            # Organize positions by agent
            positions_by_agent: dict[str, list] = {}
            for row in position_rows:
                agent_id = row["agent_id"]
                if agent_id not in positions_by_agent:
                    positions_by_agent[agent_id] = []
                positions_by_agent[agent_id].append({
                    "id": row["position_id"],
                    "symbol": row["symbol"],
                    "side": row["side"],
                    "size": row["size"],
                    "entry_price": row["entry_price"],
                    "leverage": row["leverage"],
                    "margin": row["margin"],
                    "opened_at": row["opened_at"],
                    "stop_loss_price": row["stop_loss_price"],
                    "take_profit_price": row["take_profit_price"],
                })

            # Organize orders by agent
            orders_by_agent: dict[str, list] = {}
            for row in order_rows:
                agent_id = row["agent_id"]
                if agent_id not in orders_by_agent:
                    orders_by_agent[agent_id] = []
                orders_by_agent[agent_id].append({
                    "id": row["order_id"],
                    "symbol": row["symbol"],
                    "order_type": row["order_type"],
                    "size": row["size"],
                    "limit_price": row["limit_price"],
                    "leverage": row["leverage"],
                    "created_at": row["created_at"],
                    "stop_loss_price": row["stop_loss_price"],
                    "take_profit_price": row["take_profit_price"],
                })

            # Load recent trades per agent (up to Portfolio.MAX_TRADES_HISTORY)
            agent_ids = [row["agent_id"] for row in portfolio_rows]
            trades_by_agent: dict[str, list] = {aid: [] for aid in agent_ids}
            for agent_id in agent_ids:
                trade_rows = await conn.fetch(
                    """
                    SELECT id, symbol, side, size, price, leverage, fee,
                           realized_pnl, timestamp, decision_id
                    FROM trades
                    WHERE agent_id = $1
                    ORDER BY timestamp DESC
                    LIMIT 1000
                    """,
                    agent_id,
                )
                trades_by_agent[agent_id] = list(reversed([dict(r) for r in trade_rows]))

            # Build portfolios dict
            portfolios = {}
            for row in portfolio_rows:
                agent_id = row["agent_id"]
                portfolios[agent_id] = {
                    "initial_capital": row["initial_capital"],
                    "available_margin": row["available_margin"],
                    "realized_pnl": row["realized_pnl"],
                    "funding_paid": row["funding_paid"],
                    "funding_received": row["funding_received"],
                    "positions": positions_by_agent.get(agent_id, []),
                    "pending_orders": orders_by_agent.get(agent_id, []),
                    "trades": trades_by_agent.get(agent_id, []),
                }

            # Parse prices
            prices = {}
            if arena_row["current_prices"]:
                price_data = arena_row["current_prices"]
                if isinstance(price_data, str):
                    price_data = json.loads(price_data)
                prices = {k: Decimal(v) for k, v in price_data.items()}

            return {
                "competition_name": competition_name,
                "last_tick": arena_row["last_tick"],
                "last_timestamp": arena_row["last_timestamp"],
                "current_prices": prices,
                "config": arena_row["config"],
                "portfolios": portfolios,
            }

    async def has_saved_state(self, competition_name: str) -> bool:
        """Check if a competition has saved state."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM arena_state WHERE competition_name = $1",
                competition_name,
            )
            return row is not None

    async def delete_arena_state(self, competition_name: str) -> None:
        """Delete saved arena state (for fresh start)."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM pending_order_state WHERE competition_name = $1",
                    competition_name,
                )
                await conn.execute(
                    "DELETE FROM position_state WHERE competition_name = $1",
                    competition_name,
                )
                await conn.execute(
                    "DELETE FROM portfolio_state WHERE competition_name = $1",
                    competition_name,
                )
                await conn.execute(
                    "DELETE FROM arena_state WHERE competition_name = $1",
                    competition_name,
                )

    async def reset_all(self) -> None:
        """Truncate competition data tables for a clean reset.

        Preserves learning/skills tables for long-term analysis:
        - skill_versions, learned_patterns, pattern_performance
        - regime_performance, agent_memories, agent_summaries
        - learning_events, decision_archive, decision_contexts, decision_outcomes
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                TRUNCATE TABLE
                    decisions,
                    trades,
                    funding_payments,
                    liquidations,
                    sl_tp_triggers,
                    arena_state,
                    portfolio_state,
                    position_state,
                    pending_order_state,
                    snapshots,
                    daily_snapshots,
                    competition_sessions
                CASCADE
            """)

    # =========================================================================
    # Candle Storage Methods (for backtesting)
    # =========================================================================

    async def save_candles(
        self,
        symbol: str,
        interval: str,
        candles: list[dict],
    ) -> int:
        """Save candles to database with upsert (no duplicates)."""
        if not candles:
            return 0

        async with self.pool.acquire() as conn:
            saved = 0
            for candle in candles:
                try:
                    await conn.execute(
                        """
                        INSERT INTO candles (
                            symbol, interval, open_time, open, high, low, close,
                            volume, close_time, quote_volume, trade_count
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (symbol, interval, open_time) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            close_time = EXCLUDED.close_time,
                            quote_volume = EXCLUDED.quote_volume,
                            trade_count = EXCLUDED.trade_count
                        """,
                        symbol,
                        interval,
                        candle["timestamp"],
                        Decimal(str(candle["open"])),
                        Decimal(str(candle["high"])),
                        Decimal(str(candle["low"])),
                        Decimal(str(candle["close"])),
                        Decimal(str(candle["volume"])),
                        candle.get("close_time", candle["timestamp"]),
                        Decimal(str(candle.get("quote_volume", 0))),
                        candle.get("trades", 0),
                    )
                    saved += 1
                except Exception:
                    continue
            return saved

    async def get_candles(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """Get candles for a symbol/interval within time range."""
        async with self.pool.acquire() as conn:
            query = "SELECT * FROM candles WHERE symbol = $1 AND interval = $2"
            params = [symbol, interval]
            param_idx = 3

            if start_time is not None:
                query += f" AND open_time >= ${param_idx}"
                params.append(start_time)
                param_idx += 1

            if end_time is not None:
                query += f" AND open_time <= ${param_idx}"
                params.append(end_time)
                param_idx += 1

            query += " ORDER BY open_time ASC"

            if limit is not None:
                query += f" LIMIT ${param_idx}"
                params.append(limit)

            rows = await conn.fetch(query, *params)

            candles = []
            for row in rows:
                candles.append({
                    "timestamp": row["open_time"],
                    "open": Decimal(str(row["open"])),
                    "high": Decimal(str(row["high"])),
                    "low": Decimal(str(row["low"])),
                    "close": Decimal(str(row["close"])),
                    "volume": Decimal(str(row["volume"])),
                    "close_time": row["close_time"],
                    "quote_volume": Decimal(str(row["quote_volume"])) if row.get("quote_volume") else Decimal(0),
                    "trades": row.get("trade_count", 0),
                })
            return candles

    async def get_candles_at_time(
        self,
        symbol: str,
        interval: str,
        current_time: int,
        limit: int = 100,
    ) -> list[dict]:
        """Get candles up to and including a specific time."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM candles
                WHERE symbol = $1 AND interval = $2 AND close_time <= $3
                ORDER BY open_time DESC
                LIMIT $4
                """,
                symbol,
                interval,
                current_time,
                limit,
            )

            candles = []
            for row in reversed(rows):
                candles.append({
                    "timestamp": row["open_time"],
                    "open": Decimal(str(row["open"])),
                    "high": Decimal(str(row["high"])),
                    "low": Decimal(str(row["low"])),
                    "close": Decimal(str(row["close"])),
                    "volume": Decimal(str(row["volume"])),
                    "close_time": row["close_time"],
                    "quote_volume": Decimal(str(row["quote_volume"])) if row.get("quote_volume") else Decimal(0),
                    "trades": row.get("trade_count", 0),
                })
            return candles

    async def get_data_range(
        self,
        symbol: str,
        interval: str,
    ) -> tuple[Optional[int], Optional[int], int]:
        """Get the available data range for a symbol/interval."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT MIN(open_time), MAX(open_time), COUNT(*)
                FROM candles
                WHERE symbol = $1 AND interval = $2
                """,
                symbol,
                interval,
            )

            if row and row[2] > 0:
                return row[0], row[1], row[2]
            return None, None, 0

    async def get_data_status(self) -> dict[str, dict[str, dict]]:
        """Get status of all available historical data."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, interval, MIN(open_time), MAX(open_time), COUNT(*)
                FROM candles
                GROUP BY symbol, interval
                ORDER BY symbol, interval
                """
            )

            status: dict[str, dict[str, dict]] = {}
            for row in rows:
                symbol, interval, start, end, count = row[0], row[1], row[2], row[3], row[4]
                if symbol not in status:
                    status[symbol] = {}
                status[symbol][interval] = {
                    "start": start,
                    "end": end,
                    "count": count,
                    "start_date": datetime.utcfromtimestamp(start / 1000).isoformat() if start else None,
                    "end_date": datetime.utcfromtimestamp(end / 1000).isoformat() if end else None,
                }
            return status

    # Backtest run management

    async def create_backtest_run(
        self,
        run_id: str,
        name: str,
        config: dict,
        start_date: str,
        end_date: str,
        tick_interval: str,
        estimated_cost: Optional[float] = None,
    ) -> None:
        """Create a new backtest run record."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO backtest_runs (
                    id, name, config, start_date, end_date, tick_interval,
                    status, estimated_cost
                ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)
                """,
                run_id,
                name,
                json.dumps(config),
                start_date,
                end_date,
                tick_interval,
                Decimal(str(estimated_cost)) if estimated_cost else None,
            )

    async def update_backtest_run(
        self,
        run_id: str,
        status: Optional[str] = None,
        current_tick: Optional[int] = None,
        total_ticks: Optional[int] = None,
        started_at: Optional[str] = None,
        completed_at: Optional[str] = None,
        actual_cost: Optional[float] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update backtest run status."""
        updates = []
        params = []
        param_idx = 1

        if status is not None:
            updates.append(f"status = ${param_idx}")
            params.append(status)
            param_idx += 1
        if current_tick is not None:
            updates.append(f"current_tick = ${param_idx}")
            params.append(current_tick)
            param_idx += 1
        if total_ticks is not None:
            updates.append(f"total_ticks = ${param_idx}")
            params.append(total_ticks)
            param_idx += 1
        if started_at is not None:
            updates.append(f"started_at = ${param_idx}")
            params.append(parse_timestamp(started_at))
            param_idx += 1
        if completed_at is not None:
            updates.append(f"completed_at = ${param_idx}")
            params.append(parse_timestamp(completed_at))
            param_idx += 1
        if actual_cost is not None:
            updates.append(f"actual_cost = ${param_idx}")
            params.append(Decimal(str(actual_cost)))
            param_idx += 1
        if error_message is not None:
            updates.append(f"error_message = ${param_idx}")
            params.append(error_message)
            param_idx += 1

        if updates:
            params.append(run_id)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    f"UPDATE backtest_runs SET {', '.join(updates)} WHERE id = ${param_idx}",
                    *params,
                )

    async def get_backtest_run(self, run_id: str) -> Optional[dict]:
        """Get a backtest run by ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM backtest_runs WHERE id = $1",
                run_id,
            )
            if not row:
                return None

            d = dict(row)
            d["config"] = d["config"] if isinstance(d["config"], dict) else json.loads(d["config"])
            d["start_date"] = str(d["start_date"])
            d["end_date"] = str(d["end_date"])
            if d.get("created_at"):
                d["created_at"] = str(d["created_at"])
            if d.get("started_at"):
                d["started_at"] = str(d["started_at"])
            if d.get("completed_at"):
                d["completed_at"] = str(d["completed_at"])
            if d.get("estimated_cost"):
                d["estimated_cost"] = float(d["estimated_cost"])
            if d.get("actual_cost"):
                d["actual_cost"] = float(d["actual_cost"])
            return d

    async def get_backtest_runs(
        self,
        limit: int = 50,
        status: Optional[str] = None,
    ) -> list[dict]:
        """Get list of backtest runs."""
        async with self.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    """
                    SELECT * FROM backtest_runs
                    WHERE status = $1
                    ORDER BY created_at DESC LIMIT $2
                    """,
                    status,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM backtest_runs
                    ORDER BY created_at DESC LIMIT $1
                    """,
                    limit,
                )

            runs = []
            for row in rows:
                d = dict(row)
                d["config"] = d["config"] if isinstance(d["config"], dict) else json.loads(d["config"])
                d["start_date"] = str(d["start_date"])
                d["end_date"] = str(d["end_date"])
                if d.get("created_at"):
                    d["created_at"] = str(d["created_at"])
                if d.get("estimated_cost"):
                    d["estimated_cost"] = float(d["estimated_cost"])
                runs.append(d)
            return runs

    async def save_backtest_result(
        self,
        result_id: str,
        run_id: str,
        agent_id: str,
        agent_name: str,
        metrics: dict,
        equity_curve: list[dict],
        trades: list[dict],
    ) -> None:
        """Save backtest results for an agent."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO backtest_results (
                    id, run_id, agent_id, agent_name,
                    total_pnl, total_pnl_pct, sharpe_ratio, win_rate,
                    max_drawdown_pct, total_trades, winning_trades, losing_trades,
                    profit_factor, avg_trade_pnl, largest_win, largest_loss,
                    total_fees, equity_curve, trades
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                """,
                result_id,
                run_id,
                agent_id,
                agent_name,
                Decimal(str(metrics.get("total_pnl", 0))),
                metrics.get("total_pnl_pct", 0),
                metrics.get("sharpe_ratio"),
                metrics.get("win_rate"),
                metrics.get("max_drawdown_pct"),
                metrics.get("total_trades", 0),
                metrics.get("winning_trades", 0),
                metrics.get("losing_trades", 0),
                metrics.get("profit_factor"),
                metrics.get("avg_trade_pnl"),
                Decimal(str(metrics["largest_win"])) if metrics.get("largest_win") else None,
                Decimal(str(metrics["largest_loss"])) if metrics.get("largest_loss") else None,
                Decimal(str(metrics.get("total_fees", 0))),
                json.dumps(equity_curve),
                json.dumps(trades),
            )

    async def get_backtest_results(self, run_id: str) -> list[dict]:
        """Get all results for a backtest run."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM backtest_results
                WHERE run_id = $1
                ORDER BY total_pnl DESC
                """,
                run_id,
            )

            results = []
            for row in rows:
                d = dict(row)
                d["total_pnl"] = float(d["total_pnl"])
                d["equity_curve"] = d["equity_curve"] if isinstance(d["equity_curve"], list) else json.loads(d["equity_curve"])
                d["trades"] = d["trades"] if isinstance(d["trades"], list) else json.loads(d["trades"])
                if d.get("total_fees"):
                    d["total_fees"] = float(d["total_fees"])
                if d.get("largest_win"):
                    d["largest_win"] = float(d["largest_win"])
                if d.get("largest_loss"):
                    d["largest_loss"] = float(d["largest_loss"])
                results.append(d)
            return results

    async def save_comparison(
        self,
        comparison_id: str,
        run_id: str,
        agent_id: str,
        baseline_id: str,
        outperformance: float,
        p_value: Optional[float],
        ci_low: Optional[float],
        ci_high: Optional[float],
        is_significant: bool,
    ) -> None:
        """Save statistical comparison between agent and baseline."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO backtest_comparisons (
                    id, run_id, agent_id, baseline_id,
                    outperformance, p_value, ci_low, ci_high, is_significant
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                comparison_id,
                run_id,
                agent_id,
                baseline_id,
                outperformance,
                p_value,
                ci_low,
                ci_high,
                is_significant,
            )

    async def get_comparisons(self, run_id: str) -> list[dict]:
        """Get all comparisons for a backtest run."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM backtest_comparisons
                WHERE run_id = $1
                ORDER BY agent_id, baseline_id
                """,
                run_id,
            )

            return [dict(row) for row in rows]

    async def delete_backtest_run(self, run_id: str) -> None:
        """Delete a backtest run and all associated data."""
        async with self.pool.acquire() as conn:
            # Cascading delete handles results and comparisons
            await conn.execute(
                "DELETE FROM backtest_runs WHERE id = $1",
                run_id,
            )

    # =========================================================================
    # Forum Storage Methods (M3)
    # =========================================================================

    async def save_forum_message(
        self,
        channel: str,
        agent_id: str,
        agent_name: str,
        agent_type: str,
        content: str,
        reply_to: Optional[UUID] = None,
        metadata: Optional[dict] = None,
    ) -> UUID:
        """Save a forum message."""
        from uuid import UUID

        if metadata is None:
            metadata = {}

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO forum_messages (
                    channel, agent_id, agent_name, agent_type, content, reply_to, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                """,
                channel,
                agent_id,
                agent_name,
                agent_type,
                content,
                reply_to,
                json.dumps(metadata),
            )
            return row["id"]

    async def get_forum_messages(
        self,
        channels: Optional[list[str]] = None,
        limit: int = 50,
        since: Optional[datetime] = None,
        symbols: Optional[list[str]] = None,
        agent_types: Optional[list[str]] = None,
    ) -> list:
        """Get forum messages with optional filters."""
        from agent_arena.forum.models import ForumMessage

        query_parts = ["SELECT * FROM forum_messages WHERE 1=1"]
        params = []
        param_idx = 1

        if channels:
            query_parts.append(f"AND channel = ANY(${param_idx})")
            params.append(channels)
            param_idx += 1

        if since:
            query_parts.append(f"AND created_at >= ${param_idx}")
            params.append(since)
            param_idx += 1

        if agent_types:
            query_parts.append(f"AND agent_type = ANY(${param_idx})")
            params.append(agent_types)
            param_idx += 1

        if symbols:
            # Filter by symbols in metadata
            query_parts.append(
                f"AND (metadata->>'symbols')::jsonb ?| ${param_idx}"
            )
            params.append(symbols)
            param_idx += 1

        query_parts.append(f"ORDER BY created_at DESC LIMIT ${param_idx}")
        params.append(limit)

        query = " ".join(query_parts)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            messages = []
            for row in rows:
                msg = ForumMessage(
                    id=row["id"],
                    channel=row["channel"],
                    agent_id=row["agent_id"],
                    agent_name=row["agent_name"],
                    agent_type=row["agent_type"],
                    content=row["content"],
                    reply_to=row["reply_to"],
                    metadata=row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"]),
                    created_at=row["created_at"],
                )
                messages.append(msg)
            return messages

    async def get_forum_message_by_id(self, message_id: UUID) -> Optional:
        """Get a specific forum message by ID."""
        from agent_arena.forum.models import ForumMessage

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM forum_messages WHERE id = $1",
                message_id,
            )
            if not row:
                return None

            return ForumMessage(
                id=row["id"],
                channel=row["channel"],
                agent_id=row["agent_id"],
                agent_name=row["agent_name"],
                agent_type=row["agent_type"],
                content=row["content"],
                reply_to=row["reply_to"],
                metadata=row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"]),
                created_at=row["created_at"],
            )

    async def get_forum_thread(self, message_id: UUID, limit: int = 20) -> list:
        """Get all replies to a message."""
        from agent_arena.forum.models import ForumMessage

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM forum_messages
                WHERE reply_to = $1
                ORDER BY created_at ASC
                LIMIT $2
                """,
                message_id,
                limit,
            )
            messages = []
            for row in rows:
                msg = ForumMessage(
                    id=row["id"],
                    channel=row["channel"],
                    agent_id=row["agent_id"],
                    agent_name=row["agent_name"],
                    agent_type=row["agent_type"],
                    content=row["content"],
                    reply_to=row["reply_to"],
                    metadata=row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"]),
                    created_at=row["created_at"],
                )
                messages.append(msg)
            return messages

    async def save_witness_summary(
        self,
        witness_type: str,
        insight: str,
        confidence: float,
        symbols: list[str],
        timeframe: Optional[str] = None,
        based_on: Optional[dict] = None,
        metadata: Optional[dict] = None,
        valid_until: Optional[datetime] = None,
    ) -> int:
        """Save a witness summary."""
        if based_on is None:
            based_on = {}
        if metadata is None:
            metadata = {}

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO forum_witness (
                    witness_type, insight, confidence, symbols, timeframe,
                    based_on, metadata, valid_until
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
                """,
                witness_type,
                insight,
                confidence,
                symbols,
                timeframe,
                json.dumps(based_on),
                json.dumps(metadata),
                valid_until,
            )
            return row["id"]

    async def get_witness_summaries(
        self,
        since: Optional[datetime] = None,
        symbols: Optional[list[str]] = None,
        witness_types: Optional[list[str]] = None,
        min_confidence: float = 0.0,
    ) -> list:
        """Get witness summaries with optional filters."""
        from agent_arena.forum.models import WitnessSummary

        query_parts = [
            "SELECT * FROM forum_witness WHERE confidence >= $1"
        ]
        params = [min_confidence]
        param_idx = 2

        if since:
            query_parts.append(f"AND created_at >= ${param_idx}")
            params.append(since)
            param_idx += 1

        if witness_types:
            query_parts.append(f"AND witness_type = ANY(${param_idx})")
            params.append(witness_types)
            param_idx += 1

        if symbols:
            query_parts.append(f"AND symbols && ${param_idx}")
            params.append(symbols)
            param_idx += 1

        # Exclude expired witness
        query_parts.append("AND (valid_until IS NULL OR valid_until > NOW())")

        query_parts.append("ORDER BY created_at DESC")
        query = " ".join(query_parts)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            summaries = []
            for row in rows:
                summary = WitnessSummary(
                    id=row["id"],
                    witness_type=row["witness_type"],
                    insight=row["insight"],
                    confidence=row["confidence"],
                    symbols=row["symbols"],
                    timeframe=row["timeframe"],
                    based_on=row["based_on"] if isinstance(row["based_on"], dict) else json.loads(row["based_on"]),
                    metadata=row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"]),
                    created_at=row["created_at"],
                    valid_until=row["valid_until"],
                )
                summaries.append(summary)
            return summaries

    async def save_observer_forum_run(
        self,
        run_id: UUID,
        timestamp: datetime,
        window_start: datetime,
        window_end: datetime,
        messages_analyzed: int,
        trades_analyzed: int,
        witness_generated: int,
        raw_analysis: Optional[str] = None,
        summary: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Save observer forum run metadata."""
        if summary is None:
            summary = {}
        if metadata is None:
            metadata = {}

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO observer_forum_runs (
                    id, timestamp, window_start, window_end,
                    messages_analyzed, trades_analyzed, witness_generated,
                    raw_analysis, summary, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                run_id,
                timestamp,
                window_start,
                window_end,
                messages_analyzed,
                trades_analyzed,
                witness_generated,
                raw_analysis,
                json.dumps(summary),
                json.dumps(metadata),
            )

    async def get_observer_forum_runs(
        self, limit: int = 20, since: Optional[datetime] = None
    ) -> list[dict]:
        """Get observer forum runs."""
        query = "SELECT * FROM observer_forum_runs"
        params = []

        if since:
            query += " WHERE timestamp >= $1"
            params.append(since)

        query += " ORDER BY timestamp DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    # =========================================================================
    # Bulk Query Methods (used by Lab analysis)
    # =========================================================================

    async def get_all_agent_ids(self) -> list[str]:
        """Get all distinct agent IDs from decisions."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT agent_id FROM decisions "
                "ORDER BY agent_id"
            )
            return [row["agent_id"] for row in rows]

    async def get_all_decisions(
        self, agent_id: str, limit: int = 0,
    ) -> list[dict]:
        """Get decisions for an agent, ordered by tick."""
        async with self.pool.acquire() as conn:
            if limit > 0:
                rows = await conn.fetch(
                    "SELECT * FROM decisions WHERE agent_id = $1 "
                    "ORDER BY tick DESC LIMIT $2",
                    agent_id, limit,
                )
                rows = list(reversed(rows))  # restore ASC order
            else:
                rows = await conn.fetch(
                    "SELECT * FROM decisions "
                    "WHERE agent_id = $1 ORDER BY tick ASC",
                    agent_id,
                )
            results = []
            for row in rows:
                d = dict(row)
                if d.get("metadata") and isinstance(
                    d["metadata"], str
                ):
                    d["metadata"] = json.loads(d["metadata"])
                if d.get("size") is not None:
                    d["size"] = float(d["size"])
                results.append(d)
            return results

    async def get_all_trades(
        self, agent_id: str, limit: int = 0,
    ) -> list[dict]:
        """Get trades for an agent, ordered by timestamp."""
        async with self.pool.acquire() as conn:
            if limit > 0:
                rows = await conn.fetch(
                    "SELECT * FROM trades WHERE agent_id = $1 "
                    "ORDER BY timestamp DESC LIMIT $2",
                    agent_id, limit,
                )
                rows = list(reversed(rows))
            else:
                rows = await conn.fetch(
                    "SELECT * FROM trades "
                    "WHERE agent_id = $1 "
                    "ORDER BY timestamp ASC",
                    agent_id,
                )
            results = []
            for row in rows:
                t = dict(row)
                for key in [
                    "size", "price", "fee", "realized_pnl",
                ]:
                    if t.get(key) is not None:
                        t[key] = float(t[key])
                results.append(t)
            return results

    # =========================================================================
    # Bias Profile Methods
    # =========================================================================

    async def save_bias_profile(self, profile_dict: dict) -> None:
        """Save a bias profile (one row per bias type)."""
        agent_id = profile_dict["agent_id"]
        timestamp = parse_timestamp(profile_dict["timestamp"])

        async with self.pool.acquire() as conn:
            for bias_key in (
                "disposition_effect", "loss_aversion", "overconfidence",
            ):
                bias = profile_dict.get(bias_key, {})
                await conn.execute(
                    """
                    INSERT INTO bias_profiles (
                        agent_id, timestamp, bias_type, score,
                        sample_size, sufficient_data, details,
                        evolution_run_id, generation
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    agent_id,
                    timestamp,
                    bias["bias_type"],
                    bias.get("value"),
                    bias.get("sample_size", 0),
                    bool(bias.get("sufficient_data")),
                    json.dumps(bias.get("details", {})),
                    profile_dict.get("evolution_run_id"),
                    profile_dict.get("generation"),
                )

    async def get_bias_profiles(
        self, agent_id: Optional[str] = None,
    ) -> list[dict]:
        """Get latest bias profiles per agent+type."""
        async with self.pool.acquire() as conn:
            if agent_id:
                rows = await conn.fetch(
                    """
                    SELECT bp.* FROM bias_profiles bp
                    INNER JOIN (
                        SELECT agent_id, bias_type, MAX(id) as max_id
                        FROM bias_profiles
                        WHERE agent_id = $1
                        GROUP BY agent_id, bias_type
                    ) latest ON bp.agent_id = latest.agent_id
                        AND bp.bias_type = latest.bias_type
                        AND bp.id = latest.max_id
                    ORDER BY bp.agent_id, bp.bias_type
                    """,
                    agent_id,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT bp.* FROM bias_profiles bp
                    INNER JOIN (
                        SELECT agent_id, bias_type, MAX(id) as max_id
                        FROM bias_profiles
                        GROUP BY agent_id, bias_type
                    ) latest ON bp.agent_id = latest.agent_id
                        AND bp.bias_type = latest.bias_type
                        AND bp.id = latest.max_id
                    ORDER BY bp.agent_id, bp.bias_type
                    """
                )

            results = []
            for row in rows:
                d = dict(row)
                if isinstance(d.get("details"), str):
                    d["details"] = json.loads(d["details"])
                results.append(d)
            return results

    async def get_bias_history(
        self,
        agent_id: str,
        bias_type: Optional[str] = None,
    ) -> list[dict]:
        """Get historical bias scores for an agent."""
        async with self.pool.acquire() as conn:
            if bias_type:
                rows = await conn.fetch(
                    """
                    SELECT * FROM bias_profiles
                    WHERE agent_id = $1 AND bias_type = $2
                    ORDER BY created_at ASC
                    """,
                    agent_id, bias_type,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM bias_profiles
                    WHERE agent_id = $1
                    ORDER BY created_at ASC
                    """,
                    agent_id,
                )

            results = []
            for row in rows:
                d = dict(row)
                if isinstance(d.get("details"), str):
                    d["details"] = json.loads(d["details"])
                results.append(d)
            return results

    # =========================================================================
    # Contagion Methods
    # =========================================================================

    async def save_contagion_snapshot(self, snapshot_dict: dict) -> None:
        """Save a contagion snapshot (one row per metric type)."""
        timestamp = parse_timestamp(snapshot_dict["timestamp"])
        tick = snapshot_dict.get("tick")
        agent_count = snapshot_dict.get("agent_count", 0)

        async with self.pool.acquire() as conn:
            for metric_key in (
                "position_diversity", "reasoning_entropy",
            ):
                metric = snapshot_dict.get(metric_key, {})
                await conn.execute(
                    """
                    INSERT INTO contagion_snapshots (
                        timestamp, tick, metric_type, value,
                        sample_size, sufficient_data, details,
                        agent_count
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    timestamp,
                    tick,
                    metric.get("metric_type", metric_key),
                    metric.get("value"),
                    metric.get("sample_size", 0),
                    bool(metric.get("sufficient_data")),
                    json.dumps(metric.get("details", {})),
                    agent_count,
                )

    async def get_contagion_snapshots(
        self,
        metric_type: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get recent contagion snapshots."""
        async with self.pool.acquire() as conn:
            if metric_type:
                rows = await conn.fetch(
                    """
                    SELECT * FROM contagion_snapshots
                    WHERE metric_type = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    metric_type, limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM contagion_snapshots
                    ORDER BY created_at DESC
                    LIMIT $1
                    """,
                    limit,
                )

            results = []
            for row in rows:
                d = dict(row)
                if isinstance(d.get("details"), str):
                    d["details"] = json.loads(d["details"])
                results.append(d)
            return results

    async def get_contagion_latest(self) -> list[dict]:
        """Get the most recent snapshot per metric type."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT cs.* FROM contagion_snapshots cs
                INNER JOIN (
                    SELECT metric_type, MAX(id) as max_id
                    FROM contagion_snapshots
                    GROUP BY metric_type
                ) latest ON cs.metric_type = latest.metric_type
                    AND cs.id = latest.max_id
                ORDER BY cs.metric_type
                """
            )

            results = []
            for row in rows:
                d = dict(row)
                if isinstance(d.get("details"), str):
                    d["details"] = json.loads(d["details"])
                results.append(d)
            return results

    # =========================================================================
    # Observer Journal Methods
    # =========================================================================

    async def save_journal_entry(self, entry: dict) -> str:
        """Save a journal entry. Upserts by journal_date."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO observer_journal (
                    journal_date, generated_at, lookback_hours,
                    full_markdown, market_summary,
                    forum_summary, learning_summary, recommendations,
                    agent_reports, metrics, model
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (journal_date) DO UPDATE SET
                    generated_at = EXCLUDED.generated_at,
                    lookback_hours = EXCLUDED.lookback_hours,
                    full_markdown = EXCLUDED.full_markdown,
                    market_summary = EXCLUDED.market_summary,
                    forum_summary = EXCLUDED.forum_summary,
                    learning_summary = EXCLUDED.learning_summary,
                    recommendations = EXCLUDED.recommendations,
                    agent_reports = EXCLUDED.agent_reports,
                    metrics = EXCLUDED.metrics,
                    model = EXCLUDED.model
                RETURNING id
                """,
                entry["journal_date"] if not isinstance(entry["journal_date"], str)
                    else datetime.fromisoformat(entry["journal_date"]).date(),
                parse_timestamp(entry["generated_at"]),
                entry.get("lookback_hours", 24),
                entry["full_markdown"],
                entry.get("market_summary", ""),
                entry.get("forum_summary", ""),
                entry.get("learning_summary", ""),
                entry.get("recommendations", ""),
                json.dumps(entry.get("agent_reports", {})),
                json.dumps(entry.get("metrics", {})),
                entry.get("model"),
            )
            return str(row["id"])

    async def get_journal_entries(self, limit: int = 30) -> list[dict]:
        """Get journal entries ordered by date descending."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, journal_date, generated_at, lookback_hours,
                       market_summary, forum_summary,
                       learning_summary, recommendations, model,
                       agent_reports, metrics
                FROM observer_journal
                ORDER BY journal_date DESC
                LIMIT $1
                """,
                limit,
            )

            results = []
            for row in rows:
                d = dict(row)
                d["id"] = str(d["id"])
                d["journal_date"] = d["journal_date"].isoformat()
                d["generated_at"] = d["generated_at"].isoformat()
                if isinstance(d.get("agent_reports"), str):
                    d["agent_reports"] = json.loads(d["agent_reports"])
                if isinstance(d.get("metrics"), str):
                    d["metrics"] = json.loads(d["metrics"])
                results.append(d)
            return results

    async def get_journal_entry_by_date(self, journal_date: str) -> Optional[dict]:
        """Get a journal entry by date (YYYY-MM-DD).

        Uses SELECT * intentionally — detail views need full_markdown.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT * FROM observer_journal WHERE journal_date = $1""",
                datetime.fromisoformat(journal_date).date(),
            )

            if not row:
                return None

            d = dict(row)
            d["id"] = str(d["id"])
            d["journal_date"] = d["journal_date"].isoformat()
            d["generated_at"] = d["generated_at"].isoformat()
            if isinstance(d.get("agent_reports"), str):
                d["agent_reports"] = json.loads(d["agent_reports"])
            if isinstance(d.get("metrics"), str):
                d["metrics"] = json.loads(d["metrics"])
            return d

    async def get_latest_journal_entry(self) -> Optional[dict]:
        """Get the most recent journal entry."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT * FROM observer_journal
                   ORDER BY journal_date DESC LIMIT 1"""
            )

            if not row:
                return None

            d = dict(row)
            d["id"] = str(d["id"])
            d["journal_date"] = d["journal_date"].isoformat()
            d["generated_at"] = d["generated_at"].isoformat()
            if isinstance(d.get("agent_reports"), str):
                d["agent_reports"] = json.loads(d["agent_reports"])
            if isinstance(d.get("metrics"), str):
                d["metrics"] = json.loads(d["metrics"])
            return d
