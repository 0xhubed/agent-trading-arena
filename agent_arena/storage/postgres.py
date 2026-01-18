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
