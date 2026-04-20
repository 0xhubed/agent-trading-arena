"""SQLite storage for Agent Arena."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Union

import aiosqlite


class SQLiteStorage:
    """SQLite-based storage for decisions, trades, and competition state."""

    def __init__(self, db_path: Union[str, Path] = "data/arena.db"):
        self.db_path = Path(db_path)
        self._connection: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        """Initialize database and create tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(self.db_path)

        await self._connection.executescript("""
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                tick INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                action TEXT NOT NULL,
                symbol TEXT,
                size TEXT,
                leverage INTEGER,
                confidence REAL,
                reasoning TEXT,
                metadata TEXT,
                trade_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                size TEXT NOT NULL,
                price TEXT NOT NULL,
                leverage INTEGER NOT NULL,
                fee TEXT NOT NULL,
                realized_pnl TEXT,
                timestamp TEXT NOT NULL,
                decision_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS competitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                config TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                final_leaderboard TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                competition_id INTEGER,
                tick INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                leaderboard TEXT NOT NULL,
                market_data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_decisions_agent ON decisions(agent_id);
            CREATE INDEX IF NOT EXISTS idx_decisions_tick ON decisions(tick);
            CREATE INDEX IF NOT EXISTS idx_trades_agent ON trades(agent_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_tick ON snapshots(tick);

            -- Memory tables for agentic traders
            CREATE TABLE IF NOT EXISTS agent_memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                memory_type TEXT NOT NULL,
                content TEXT NOT NULL,
                importance REAL DEFAULT 0.5,
                tick INTEGER,
                timestamp TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS agent_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                summary_type TEXT NOT NULL,
                content TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                tick_count INTEGER,
                trade_count INTEGER,
                pnl_summary TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_memories_agent ON agent_memories(agent_id);
            CREATE INDEX IF NOT EXISTS idx_memories_type ON agent_memories(memory_type);
            CREATE INDEX IF NOT EXISTS idx_memories_importance ON agent_memories(importance);
            CREATE INDEX IF NOT EXISTS idx_summaries_agent ON agent_summaries(agent_id);

            -- Funding payments table
            CREATE TABLE IF NOT EXISTS funding_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick INTEGER NOT NULL,
                agent_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                funding_rate TEXT NOT NULL,
                notional TEXT NOT NULL,
                amount TEXT NOT NULL,
                direction TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Liquidations table
            CREATE TABLE IF NOT EXISTS liquidations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick INTEGER NOT NULL,
                agent_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                size TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                liquidation_price TEXT NOT NULL,
                mark_price TEXT NOT NULL,
                margin_lost TEXT NOT NULL,
                fee TEXT NOT NULL,
                total_loss TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Stop-loss/Take-profit triggers table
            CREATE TABLE IF NOT EXISTS sl_tp_triggers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick INTEGER NOT NULL,
                agent_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                trigger_price TEXT NOT NULL,
                mark_price TEXT NOT NULL,
                size TEXT NOT NULL,
                realized_pnl TEXT NOT NULL,
                fee TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_funding_agent ON funding_payments(agent_id);
            CREATE INDEX IF NOT EXISTS idx_funding_tick ON funding_payments(tick);
            CREATE INDEX IF NOT EXISTS idx_liquidations_agent ON liquidations(agent_id);
            CREATE INDEX IF NOT EXISTS idx_liquidations_tick ON liquidations(tick);
            CREATE INDEX IF NOT EXISTS idx_sl_tp_agent ON sl_tp_triggers(agent_id);
            CREATE INDEX IF NOT EXISTS idx_sl_tp_tick ON sl_tp_triggers(tick);

            -- Bias profiles table
            CREATE TABLE IF NOT EXISTS bias_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                bias_type TEXT NOT NULL,
                score REAL,
                sample_size INTEGER NOT NULL,
                sufficient_data INTEGER NOT NULL DEFAULT 0,
                details TEXT,
                evolution_run_id TEXT,
                generation INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_bias_agent ON bias_profiles(agent_id);

            -- Contagion snapshots table
            CREATE TABLE IF NOT EXISTS contagion_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                tick INTEGER,
                metric_type TEXT NOT NULL,
                value REAL,
                sample_size INTEGER NOT NULL,
                sufficient_data INTEGER NOT NULL DEFAULT 0,
                details TEXT,
                agent_count INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_contagion_tick ON contagion_snapshots(tick);
            CREATE INDEX IF NOT EXISTS idx_contagion_type ON contagion_snapshots(metric_type);

            -- Observer journal table
            CREATE TABLE IF NOT EXISTS observer_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                journal_date TEXT NOT NULL UNIQUE,
                generated_at TEXT NOT NULL,
                lookback_hours INTEGER NOT NULL DEFAULT 24,
                full_markdown TEXT NOT NULL,
                market_summary TEXT DEFAULT '',

                forum_summary TEXT DEFAULT '',
                learning_summary TEXT DEFAULT '',
                recommendations TEXT DEFAULT '',
                agent_reports TEXT DEFAULT '{}',
                metrics TEXT DEFAULT '{}',
                model TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_journal_date ON observer_journal(journal_date);
        """)
        await self._connection.commit()

        # Create backtest-related tables
        from agent_arena.storage.candles import CandleStorage
        await CandleStorage.create_tables(self._connection)

    async def close(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def save_decision(self, decision: dict) -> int:
        """Save a decision to the database."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            """
            INSERT INTO decisions (
                agent_id, tick, timestamp, action, symbol, size,
                leverage, confidence, reasoning, metadata, trade_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision["agent_id"],
                decision["tick"],
                decision["timestamp"],
                decision["action"],
                decision.get("symbol"),
                decision.get("size"),
                decision.get("leverage"),
                decision.get("confidence"),
                decision.get("reasoning"),
                json.dumps(decision.get("metadata", {})),
                decision.get("trade_id"),
            ),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def save_trade(self, trade: dict) -> None:
        """Save a trade to the database."""
        if not self._connection:
            await self.initialize()

        await self._connection.execute(
            """
            INSERT INTO trades (
                id, agent_id, symbol, side, size, price,
                leverage, fee, realized_pnl, timestamp, decision_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade["id"],
                trade["agent_id"],
                trade["symbol"],
                trade["side"],
                trade["size"],
                trade["price"],
                trade["leverage"],
                trade["fee"],
                trade.get("realized_pnl"),
                trade["timestamp"],
                trade.get("decision_id"),
            ),
        )
        await self._connection.commit()

    async def save_snapshot(
        self,
        tick: int,
        timestamp: str,
        leaderboard: list[dict],
        market_data: Optional[dict] = None,
        competition_id: Optional[int] = None,
    ) -> None:
        """Save a tick snapshot."""
        if not self._connection:
            await self.initialize()

        await self._connection.execute(
            """
            INSERT INTO snapshots (competition_id, tick, timestamp, leaderboard, market_data)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                competition_id,
                tick,
                timestamp,
                json.dumps(leaderboard),
                json.dumps(market_data) if market_data else None,
            ),
        )
        await self._connection.commit()

    async def get_recent_decisions(
        self,
        agent_id: str,
        limit: int = 20,
    ) -> list[dict]:
        """Get recent decisions for an agent."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            """
            SELECT * FROM decisions
            WHERE agent_id = ?
            ORDER BY tick DESC
            LIMIT ?
            """,
            (agent_id, limit),
        )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        decisions = []
        for row in rows:
            d = dict(zip(columns, row))
            if d.get("metadata"):
                d["metadata"] = json.loads(d["metadata"])
            decisions.append(d)

        return decisions

    async def get_agent_trades(
        self,
        agent_id: str,
        limit: int = 50,
    ) -> list[dict]:
        """Get trades for an agent."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            """
            SELECT * FROM trades
            WHERE agent_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (agent_id, limit),
        )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        return [dict(zip(columns, row)) for row in rows]

    async def get_leaderboard_history(
        self,
        limit: int = 100,
    ) -> list[dict]:
        """Get historical leaderboard snapshots in ascending order for charts."""
        if not self._connection:
            await self.initialize()

        # Use subquery to get most recent N snapshots, then order ascending for charts
        cursor = await self._connection.execute(
            """
            SELECT tick, timestamp, leaderboard FROM (
                SELECT tick, timestamp, leaderboard FROM snapshots
                ORDER BY tick DESC
                LIMIT ?
            ) ORDER BY tick ASC
            """,
            (limit,),
        )
        rows = await cursor.fetchall()

        return [
            {
                "tick": row[0],
                "timestamp": row[1],
                "leaderboard": json.loads(row[2]),
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
        if not self._connection:
            await self.initialize()

        await self._connection.execute(
            """
            INSERT INTO funding_payments (
                tick, agent_id, symbol, side, funding_rate,
                notional, amount, direction, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tick,
                payment["agent_id"],
                payment["symbol"],
                payment["side"],
                str(payment["funding_rate"]),
                str(payment["notional"]),
                str(payment["amount"]),
                payment["direction"],
                timestamp,
            ),
        )
        await self._connection.commit()

    async def save_liquidation(
        self,
        tick: int,
        timestamp: str,
        liquidation: dict,
    ) -> None:
        """Save a liquidation event."""
        if not self._connection:
            await self.initialize()

        await self._connection.execute(
            """
            INSERT INTO liquidations (
                tick, agent_id, symbol, side, size, entry_price,
                liquidation_price, mark_price, margin_lost, fee,
                total_loss, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tick,
                liquidation["agent_id"],
                liquidation["symbol"],
                liquidation["side"],
                str(liquidation["size"]),
                str(liquidation["entry_price"]),
                str(liquidation["liquidation_price"]),
                str(liquidation["mark_price"]),
                str(liquidation["margin_lost"]),
                str(liquidation["fee"]),
                str(liquidation["total_loss"]),
                timestamp,
            ),
        )
        await self._connection.commit()

    async def save_sl_tp_trigger(
        self,
        tick: int,
        timestamp: str,
        trigger: dict,
    ) -> None:
        """Save a stop-loss/take-profit trigger event."""
        if not self._connection:
            await self.initialize()

        await self._connection.execute(
            """
            INSERT INTO sl_tp_triggers (
                tick, agent_id, symbol, side, trigger_type,
                trigger_price, mark_price, size, realized_pnl,
                fee, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tick,
                trigger["agent_id"],
                trigger["symbol"],
                trigger["side"],
                trigger["trigger_type"],
                str(trigger["trigger_price"]),
                str(trigger["mark_price"]),
                str(trigger["size"]),
                str(trigger["realized_pnl"]),
                str(trigger["fee"]),
                timestamp,
            ),
        )
        await self._connection.commit()

    async def get_funding_history(
        self,
        agent_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get funding payment history."""
        if not self._connection:
            await self.initialize()

        if agent_id:
            cursor = await self._connection.execute(
                """
                SELECT * FROM funding_payments
                WHERE agent_id = ?
                ORDER BY tick DESC
                LIMIT ?
                """,
                (agent_id, limit),
            )
        else:
            cursor = await self._connection.execute(
                """
                SELECT * FROM funding_payments
                ORDER BY tick DESC
                LIMIT ?
                """,
                (limit,),
            )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            # Convert numeric string fields to floats for frontend
            d["funding_rate"] = float(d["funding_rate"]) if d.get("funding_rate") else 0.0
            d["notional"] = float(d["notional"]) if d.get("notional") else 0.0
            d["amount"] = float(d["amount"]) if d.get("amount") else 0.0
            results.append(d)
        return results

    async def get_liquidation_history(
        self,
        agent_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get liquidation history."""
        if not self._connection:
            await self.initialize()

        if agent_id:
            cursor = await self._connection.execute(
                """
                SELECT * FROM liquidations
                WHERE agent_id = ?
                ORDER BY tick DESC
                LIMIT ?
                """,
                (agent_id, limit),
            )
        else:
            cursor = await self._connection.execute(
                """
                SELECT * FROM liquidations
                ORDER BY tick DESC
                LIMIT ?
                """,
                (limit,),
            )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            # Convert numeric string fields to floats for frontend
            d["size"] = float(d["size"]) if d.get("size") else 0.0
            d["entry_price"] = float(d["entry_price"]) if d.get("entry_price") else 0.0
            liq_price = d.get("liquidation_price")
            d["liquidation_price"] = float(liq_price) if liq_price else 0.0
            d["mark_price"] = float(d["mark_price"]) if d.get("mark_price") else 0.0
            d["margin_lost"] = float(d["margin_lost"]) if d.get("margin_lost") else 0.0
            d["fee"] = float(d["fee"]) if d.get("fee") else 0.0
            d["total_loss"] = float(d["total_loss"]) if d.get("total_loss") else 0.0
            results.append(d)
        return results

    async def get_agent_funding_summary(self, agent_id: str) -> dict:
        """Get funding payment summary using SQL aggregation."""
        if not self._connection:
            await self.initialize()
        cursor = await self._connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN direction='paid'
                    THEN ABS(CAST(amount AS REAL)) ELSE 0 END), 0) as paid,
                COALESCE(SUM(CASE WHEN direction='received'
                    THEN ABS(CAST(amount AS REAL)) ELSE 0 END), 0) as received
            FROM funding_payments WHERE agent_id = ?
            """,
            (agent_id,),
        )
        row = await cursor.fetchone()
        paid = float(row[0]) if row else 0.0
        received = float(row[1]) if row else 0.0
        return {"paid": paid, "received": received, "net": received - paid}

    async def get_agent_trade_count(self, agent_id: str) -> int:
        """Get trade count using SQL aggregation."""
        if not self._connection:
            await self.initialize()
        cursor = await self._connection.execute(
            "SELECT COUNT(*) FROM trades WHERE agent_id = ?",
            (agent_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def get_agent_liquidation_count(self, agent_id: str) -> int:
        """Get liquidation count using SQL aggregation."""
        if not self._connection:
            await self.initialize()
        cursor = await self._connection.execute(
            "SELECT COUNT(*) FROM liquidations WHERE agent_id = ?",
            (agent_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def get_agent_behavioral_stats(self, agent_id: str) -> dict:
        """Get behavioral statistics for an agent from decisions and trades."""
        if not self._connection:
            await self.initialize()

        # Get action distribution
        cursor = await self._connection.execute(
            """
            SELECT action, COUNT(*) as count
            FROM decisions
            WHERE agent_id = ?
            GROUP BY action
            """,
            (agent_id,),
        )
        action_rows = await cursor.fetchall()
        action_distribution = {row[0]: row[1] for row in action_rows}

        # Get confidence statistics
        cursor = await self._connection.execute(
            """
            SELECT
                AVG(confidence) as avg_confidence,
                MIN(confidence) as min_confidence,
                MAX(confidence) as max_confidence,
                COUNT(*) as total_decisions
            FROM decisions
            WHERE agent_id = ? AND confidence IS NOT NULL
            """,
            (agent_id,),
        )
        conf_row = await cursor.fetchone()
        confidence_stats = {
            "average": round(conf_row[0], 4) if conf_row[0] else 0,
            "min": round(conf_row[1], 4) if conf_row[1] else 0,
            "max": round(conf_row[2], 4) if conf_row[2] else 0,
            "total_decisions": conf_row[3] or 0,
        }

        # Get symbol distribution from trades
        cursor = await self._connection.execute(
            """
            SELECT symbol, COUNT(*) as count
            FROM trades
            WHERE agent_id = ?
            GROUP BY symbol
            """,
            (agent_id,),
        )
        symbol_rows = await cursor.fetchall()
        symbol_distribution = {row[0]: row[1] for row in symbol_rows}

        # Get long/short ratio from trades
        cursor = await self._connection.execute(
            """
            SELECT side, COUNT(*) as count
            FROM trades
            WHERE agent_id = ?
            GROUP BY side
            """,
            (agent_id,),
        )
        side_rows = await cursor.fetchall()
        side_counts = {row[0]: row[1] for row in side_rows}
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
        cursor = await self._connection.execute(
            """
            SELECT AVG(leverage) as avg_leverage
            FROM trades
            WHERE agent_id = ?
            """,
            (agent_id,),
        )
        lev_row = await cursor.fetchone()
        avg_leverage = round(lev_row[0], 2) if lev_row[0] else 0

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

    async def get_all_decisions(
        self, agent_id: str, limit: int = 0,
    ) -> list[dict]:
        """Get decisions for an agent, ordered by tick ascending."""
        if not self._connection:
            await self.initialize()

        if limit > 0:
            cursor = await self._connection.execute(
                "SELECT * FROM ("
                "  SELECT * FROM decisions WHERE agent_id = ? "
                "  ORDER BY tick DESC LIMIT ?"
                ") sub ORDER BY tick ASC",
                (agent_id, limit),
            )
        else:
            cursor = await self._connection.execute(
                "SELECT * FROM decisions "
                "WHERE agent_id = ? ORDER BY tick ASC",
                (agent_id,),
            )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        decisions = []
        for row in rows:
            d = dict(zip(columns, row))
            if d.get("metadata"):
                d["metadata"] = json.loads(d["metadata"])
            decisions.append(d)
        return decisions

    async def get_all_trades(
        self, agent_id: str, limit: int = 0,
    ) -> list[dict]:
        """Get trades for an agent, ordered by timestamp ascending."""
        if not self._connection:
            await self.initialize()

        if limit > 0:
            cursor = await self._connection.execute(
                "SELECT * FROM ("
                "  SELECT * FROM trades WHERE agent_id = ? "
                "  ORDER BY timestamp DESC LIMIT ?"
                ") sub ORDER BY timestamp ASC",
                (agent_id, limit),
            )
        else:
            cursor = await self._connection.execute(
                "SELECT * FROM trades "
                "WHERE agent_id = ? ORDER BY timestamp ASC",
                (agent_id,),
            )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    async def get_all_agent_ids(self) -> list[str]:
        """Get all distinct agent IDs from decisions."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            "SELECT DISTINCT agent_id FROM decisions ORDER BY agent_id",
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def save_bias_profile(self, profile_dict: dict) -> None:
        """Save a bias profile (one row per bias type)."""
        if not self._connection:
            await self.initialize()

        agent_id = profile_dict["agent_id"]
        timestamp = profile_dict["timestamp"]

        for bias_key in ("disposition_effect", "loss_aversion", "overconfidence"):
            bias = profile_dict.get(bias_key, {})
            await self._connection.execute(
                """
                INSERT INTO bias_profiles (
                    agent_id, timestamp, bias_type, score,
                    sample_size, sufficient_data, details,
                    evolution_run_id, generation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    agent_id,
                    timestamp,
                    bias["bias_type"],
                    bias.get("value"),
                    bias.get("sample_size", 0),
                    1 if bias.get("sufficient_data") else 0,
                    json.dumps(bias.get("details", {})),
                    profile_dict.get("evolution_run_id"),
                    profile_dict.get("generation"),
                ),
            )

        await self._connection.commit()

    async def get_bias_profiles(
        self, agent_id: Optional[str] = None,
    ) -> list[dict]:
        """Get latest bias profiles per agent+type."""
        if not self._connection:
            await self.initialize()

        if agent_id:
            cursor = await self._connection.execute(
                """
                SELECT bp.* FROM bias_profiles bp
                INNER JOIN (
                    SELECT agent_id, bias_type, MAX(id) as max_id
                    FROM bias_profiles
                    WHERE agent_id = ?
                    GROUP BY agent_id, bias_type
                ) latest ON bp.agent_id = latest.agent_id
                    AND bp.bias_type = latest.bias_type
                    AND bp.id = latest.max_id
                ORDER BY bp.agent_id, bp.bias_type
                """,
                (agent_id,),
            )
        else:
            cursor = await self._connection.execute(
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
                """,
            )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if d.get("details"):
                d["details"] = json.loads(d["details"])
            d["sufficient_data"] = bool(d.get("sufficient_data"))
            results.append(d)
        return results

    async def get_bias_history(
        self,
        agent_id: str,
        bias_type: Optional[str] = None,
    ) -> list[dict]:
        """Get historical bias scores for an agent."""
        if not self._connection:
            await self.initialize()

        if bias_type:
            cursor = await self._connection.execute(
                """
                SELECT * FROM bias_profiles
                WHERE agent_id = ? AND bias_type = ?
                ORDER BY created_at ASC
                """,
                (agent_id, bias_type),
            )
        else:
            cursor = await self._connection.execute(
                """
                SELECT * FROM bias_profiles
                WHERE agent_id = ?
                ORDER BY created_at ASC
                """,
                (agent_id,),
            )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if d.get("details"):
                d["details"] = json.loads(d["details"])
            d["sufficient_data"] = bool(d.get("sufficient_data"))
            results.append(d)
        return results

    # --- Contagion Tracker storage ---

    async def save_contagion_snapshot(self, snapshot_dict: dict) -> None:
        """Save a contagion snapshot (one row per metric type)."""
        if not self._connection:
            await self.initialize()

        timestamp = snapshot_dict["timestamp"]
        tick = snapshot_dict.get("tick")
        agent_count = snapshot_dict.get("agent_count", 0)

        for metric_key in ("position_diversity", "reasoning_entropy"):
            metric = snapshot_dict.get(metric_key, {})
            await self._connection.execute(
                """
                INSERT INTO contagion_snapshots (
                    timestamp, tick, metric_type, value,
                    sample_size, sufficient_data, details, agent_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    tick,
                    metric.get("metric_type", metric_key),
                    metric.get("value"),
                    metric.get("sample_size", 0),
                    1 if metric.get("sufficient_data") else 0,
                    json.dumps(metric.get("details", {})),
                    agent_count,
                ),
            )

        await self._connection.commit()

    async def get_contagion_snapshots(
        self,
        metric_type: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get recent contagion snapshots."""
        if not self._connection:
            await self.initialize()

        if metric_type:
            cursor = await self._connection.execute(
                """
                SELECT * FROM contagion_snapshots
                WHERE metric_type = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (metric_type, limit),
            )
        else:
            cursor = await self._connection.execute(
                """
                SELECT * FROM contagion_snapshots
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if d.get("details"):
                d["details"] = json.loads(d["details"])
            d["sufficient_data"] = bool(d.get("sufficient_data"))
            results.append(d)
        return results

    async def get_contagion_latest(self) -> list[dict]:
        """Get the most recent snapshot per metric type."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            """
            SELECT cs.* FROM contagion_snapshots cs
            INNER JOIN (
                SELECT metric_type, MAX(id) as max_id
                FROM contagion_snapshots
                GROUP BY metric_type
            ) latest ON cs.metric_type = latest.metric_type
                AND cs.id = latest.max_id
            ORDER BY cs.metric_type
            """,
        )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if d.get("details"):
                d["details"] = json.loads(d["details"])
            d["sufficient_data"] = bool(d.get("sufficient_data"))
            results.append(d)
        return results

    # =========================================================================
    # Observer Journal Methods
    # =========================================================================

    async def save_journal_entry(self, entry: dict) -> int:
        """Save a journal entry. Upserts by journal_date."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            """
            INSERT INTO observer_journal (
                journal_date, generated_at, lookback_hours,
                full_markdown, market_summary,
                forum_summary, learning_summary, recommendations,
                agent_reports, metrics, model
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(journal_date) DO UPDATE SET
                generated_at = excluded.generated_at,
                lookback_hours = excluded.lookback_hours,
                full_markdown = excluded.full_markdown,
                market_summary = excluded.market_summary,
                forum_summary = excluded.forum_summary,
                learning_summary = excluded.learning_summary,
                recommendations = excluded.recommendations,
                agent_reports = excluded.agent_reports,
                metrics = excluded.metrics,
                model = excluded.model
            """,
            (
                entry["journal_date"] if isinstance(entry["journal_date"], str)
                    else entry["journal_date"].isoformat(),
                entry["generated_at"] if isinstance(entry["generated_at"], str)
                    else entry["generated_at"].isoformat(),
                entry.get("lookback_hours", 24),
                entry["full_markdown"],
                entry.get("market_summary", ""),
                entry.get("forum_summary", ""),
                entry.get("learning_summary", ""),
                entry.get("recommendations", ""),
                json.dumps(entry.get("agent_reports", {})),
                json.dumps(entry.get("metrics", {})),
                entry.get("model"),
            ),
        )
        await self._connection.commit()
        return cursor.lastrowid

    async def get_journal_entries(self, limit: int = 30) -> list[dict]:
        """Get journal entries ordered by date descending."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            """
            SELECT id, journal_date, generated_at, lookback_hours,
                   market_summary, forum_summary,
                   learning_summary, recommendations, model,
                   agent_reports, metrics
            FROM observer_journal
            ORDER BY journal_date DESC
            LIMIT ?
            """,
            (limit,),
        )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            d = dict(zip(columns, row))
            if isinstance(d.get("agent_reports"), str):
                try:
                    d["agent_reports"] = json.loads(d["agent_reports"])
                except (json.JSONDecodeError, TypeError):
                    d["agent_reports"] = {}
            if isinstance(d.get("metrics"), str):
                try:
                    d["metrics"] = json.loads(d["metrics"])
                except (json.JSONDecodeError, TypeError):
                    d["metrics"] = {}
            results.append(d)
        return results

    async def get_journal_entry_by_date(self, journal_date: str) -> dict | None:
        """Get a journal entry by date (YYYY-MM-DD)."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            "SELECT * FROM observer_journal WHERE journal_date = ?",
            (journal_date,),
        )

        row = await cursor.fetchone()
        if not row:
            return None

        columns = [desc[0] for desc in cursor.description]
        d = dict(zip(columns, row))
        if isinstance(d.get("agent_reports"), str):
            try:
                d["agent_reports"] = json.loads(d["agent_reports"])
            except (json.JSONDecodeError, TypeError):
                d["agent_reports"] = {}
        if isinstance(d.get("metrics"), str):
            try:
                d["metrics"] = json.loads(d["metrics"])
            except (json.JSONDecodeError, TypeError):
                d["metrics"] = {}
        return d

    async def get_latest_journal_entry(self) -> dict | None:
        """Get the most recent journal entry."""
        if not self._connection:
            await self.initialize()

        cursor = await self._connection.execute(
            """SELECT * FROM observer_journal
               ORDER BY journal_date DESC LIMIT 1"""
        )

        row = await cursor.fetchone()
        if not row:
            return None

        columns = [desc[0] for desc in cursor.description]
        d = dict(zip(columns, row))
        if isinstance(d.get("agent_reports"), str):
            try:
                d["agent_reports"] = json.loads(d["agent_reports"])
            except (json.JSONDecodeError, TypeError):
                d["agent_reports"] = {}
        if isinstance(d.get("metrics"), str):
            try:
                d["metrics"] = json.loads(d["metrics"])
            except (json.JSONDecodeError, TypeError):
                d["metrics"] = {}
        return d
