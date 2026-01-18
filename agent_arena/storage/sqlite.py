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
        """)
        await self._connection.commit()

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
