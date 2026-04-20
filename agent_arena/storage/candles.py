"""Candle storage for historical data."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Optional, Union

import aiosqlite


class CandleStorage:
    """
    Storage for historical OHLCV candle data.

    Provides methods to store and retrieve candles for backtesting.
    Supports both SQLite and PostgreSQL backends.
    """

    def __init__(self, connection: aiosqlite.Connection):
        self._connection = connection

    @classmethod
    async def create_tables(cls, connection: aiosqlite.Connection) -> None:
        """Create candle-related tables."""
        await connection.executescript("""
            -- Historical candle data (fetched from Kraken Futures)
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open TEXT NOT NULL,
                high TEXT NOT NULL,
                low TEXT NOT NULL,
                close TEXT NOT NULL,
                volume TEXT NOT NULL,
                close_time INTEGER NOT NULL,
                quote_volume TEXT,
                trade_count INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, interval, open_time)
            );

            -- Index for efficient time-range queries during backtest
            CREATE INDEX IF NOT EXISTS idx_candles_lookup
                ON candles (symbol, interval, open_time DESC);
            CREATE INDEX IF NOT EXISTS idx_candles_range
                ON candles (symbol, interval, open_time, close_time);

            -- Backtest runs metadata
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                config TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                tick_interval TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                started_at TEXT,
                completed_at TEXT,
                total_ticks INTEGER,
                current_tick INTEGER DEFAULT 0,
                estimated_cost REAL,
                actual_cost REAL,
                error_message TEXT
            );

            -- Backtest results per agent
            CREATE TABLE IF NOT EXISTS backtest_results (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                total_pnl REAL NOT NULL,
                total_pnl_pct REAL NOT NULL,
                sharpe_ratio REAL,
                win_rate REAL,
                max_drawdown_pct REAL,
                total_trades INTEGER NOT NULL,
                winning_trades INTEGER NOT NULL,
                losing_trades INTEGER NOT NULL,
                profit_factor REAL,
                avg_trade_pnl REAL,
                largest_win REAL,
                largest_loss REAL,
                total_fees REAL,
                equity_curve TEXT,
                trades TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_backtest_results_run
                ON backtest_results (run_id);

            -- Statistical comparisons between agents
            CREATE TABLE IF NOT EXISTS backtest_comparisons (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                baseline_id TEXT NOT NULL,
                outperformance REAL NOT NULL,
                p_value REAL,
                ci_low REAL,
                ci_high REAL,
                is_significant INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_backtest_comparisons_run
                ON backtest_comparisons (run_id);
        """)
        await connection.commit()

    async def save_candles(
        self,
        symbol: str,
        interval: str,
        candles: list[dict],
    ) -> int:
        """
        Save candles to database with upsert (no duplicates).

        Returns number of candles saved.
        """
        if not candles:
            return 0

        saved = 0
        for candle in candles:
            try:
                await self._connection.execute(
                    """
                    INSERT OR REPLACE INTO candles (
                        symbol, interval, open_time, open, high, low, close,
                        volume, close_time, quote_volume, trade_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        symbol,
                        interval,
                        candle["timestamp"],  # open_time in ms
                        str(candle["open"]),
                        str(candle["high"]),
                        str(candle["low"]),
                        str(candle["close"]),
                        str(candle["volume"]),
                        candle.get("close_time", candle["timestamp"]),
                        str(candle.get("quote_volume", 0)),
                        candle.get("trades", 0),
                    ),
                )
                saved += 1
            except Exception:
                continue

        await self._connection.commit()
        return saved

    async def get_candles(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """
        Get candles for a symbol/interval within time range.

        Args:
            symbol: Trading pair (e.g., "PF_XBTUSD")
            interval: Candle interval (e.g., "1h", "4h")
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            limit: Maximum number of candles to return

        Returns:
            List of candle dicts sorted by open_time ascending
        """
        query = "SELECT * FROM candles WHERE symbol = ? AND interval = ?"
        params: list = [symbol, interval]

        if start_time is not None:
            query += " AND open_time >= ?"
            params.append(start_time)

        if end_time is not None:
            query += " AND open_time <= ?"
            params.append(end_time)

        query += " ORDER BY open_time ASC"

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        cursor = await self._connection.execute(query, params)
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        candles = []
        for row in rows:
            d = dict(zip(columns, row))
            candles.append({
                "timestamp": d["open_time"],
                "open": Decimal(d["open"]),
                "high": Decimal(d["high"]),
                "low": Decimal(d["low"]),
                "close": Decimal(d["close"]),
                "volume": Decimal(d["volume"]),
                "close_time": d["close_time"],
                "quote_volume": Decimal(d["quote_volume"]) if d.get("quote_volume") else Decimal(0),
                "trades": d.get("trade_count", 0),
            })

        return candles

    async def get_candles_at_time(
        self,
        symbol: str,
        interval: str,
        current_time: int,
        limit: int = 100,
    ) -> list[dict]:
        """
        Get candles up to and including a specific time.
        Used during backtest replay to simulate historical context.

        Args:
            symbol: Trading pair
            interval: Candle interval
            current_time: Current backtest timestamp in milliseconds
            limit: Number of recent candles to return

        Returns:
            List of candles ending at or before current_time
        """
        cursor = await self._connection.execute(
            """
            SELECT * FROM candles
            WHERE symbol = ? AND interval = ? AND close_time <= ?
            ORDER BY open_time DESC
            LIMIT ?
            """,
            (symbol, interval, current_time, limit),
        )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        candles = []
        for row in reversed(rows):  # Reverse to get ascending order
            d = dict(zip(columns, row))
            candles.append({
                "timestamp": d["open_time"],
                "open": Decimal(d["open"]),
                "high": Decimal(d["high"]),
                "low": Decimal(d["low"]),
                "close": Decimal(d["close"]),
                "volume": Decimal(d["volume"]),
                "close_time": d["close_time"],
                "quote_volume": Decimal(d["quote_volume"]) if d.get("quote_volume") else Decimal(0),
                "trades": d.get("trade_count", 0),
            })

        return candles

    async def get_data_range(
        self,
        symbol: str,
        interval: str,
    ) -> tuple[Optional[int], Optional[int], int]:
        """
        Get the available data range for a symbol/interval.

        Returns:
            Tuple of (earliest_time, latest_time, count)
        """
        cursor = await self._connection.execute(
            """
            SELECT MIN(open_time), MAX(open_time), COUNT(*)
            FROM candles
            WHERE symbol = ? AND interval = ?
            """,
            (symbol, interval),
        )
        row = await cursor.fetchone()

        if row and row[2] > 0:
            return row[0], row[1], row[2]
        return None, None, 0

    async def get_data_status(self) -> dict[str, dict[str, dict]]:
        """
        Get status of all available historical data.

        Returns:
            Dict mapping symbol -> interval -> {start, end, count}
        """
        cursor = await self._connection.execute(
            """
            SELECT symbol, interval, MIN(open_time), MAX(open_time), COUNT(*)
            FROM candles
            GROUP BY symbol, interval
            ORDER BY symbol, interval
            """
        )
        rows = await cursor.fetchall()

        status: dict[str, dict[str, dict]] = {}
        for row in rows:
            symbol, interval, start, end, count = row
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
        await self._connection.execute(
            """
            INSERT INTO backtest_runs (
                id, name, config, start_date, end_date, tick_interval,
                status, estimated_cost
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (
                run_id,
                name,
                json.dumps(config),
                start_date,
                end_date,
                tick_interval,
                estimated_cost,
            ),
        )
        await self._connection.commit()

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

        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if current_tick is not None:
            updates.append("current_tick = ?")
            params.append(current_tick)
        if total_ticks is not None:
            updates.append("total_ticks = ?")
            params.append(total_ticks)
        if started_at is not None:
            updates.append("started_at = ?")
            params.append(started_at)
        if completed_at is not None:
            updates.append("completed_at = ?")
            params.append(completed_at)
        if actual_cost is not None:
            updates.append("actual_cost = ?")
            params.append(actual_cost)
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        if updates:
            params.append(run_id)
            await self._connection.execute(
                f"UPDATE backtest_runs SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            await self._connection.commit()

    async def get_backtest_run(self, run_id: str) -> Optional[dict]:
        """Get a backtest run by ID."""
        cursor = await self._connection.execute(
            "SELECT * FROM backtest_runs WHERE id = ?",
            (run_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return None

        columns = [desc[0] for desc in cursor.description]
        d = dict(zip(columns, row))
        d["config"] = json.loads(d["config"]) if d.get("config") else {}
        return d

    async def get_backtest_runs(
        self,
        limit: int = 50,
        status: Optional[str] = None,
    ) -> list[dict]:
        """Get list of backtest runs."""
        query = "SELECT * FROM backtest_runs"
        params: list = []

        if status:
            query += " WHERE status = ?"
            params.append(status)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor = await self._connection.execute(query, params)
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        runs = []
        for row in rows:
            d = dict(zip(columns, row))
            d["config"] = json.loads(d["config"]) if d.get("config") else {}
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
        await self._connection.execute(
            """
            INSERT INTO backtest_results (
                id, run_id, agent_id, agent_name,
                total_pnl, total_pnl_pct, sharpe_ratio, win_rate,
                max_drawdown_pct, total_trades, winning_trades, losing_trades,
                profit_factor, avg_trade_pnl, largest_win, largest_loss,
                total_fees, equity_curve, trades
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result_id,
                run_id,
                agent_id,
                agent_name,
                metrics.get("total_pnl", 0),
                metrics.get("total_pnl_pct", 0),
                metrics.get("sharpe_ratio"),
                metrics.get("win_rate"),
                metrics.get("max_drawdown_pct"),
                metrics.get("total_trades", 0),
                metrics.get("winning_trades", 0),
                metrics.get("losing_trades", 0),
                metrics.get("profit_factor"),
                metrics.get("avg_trade_pnl"),
                metrics.get("largest_win"),
                metrics.get("largest_loss"),
                metrics.get("total_fees", 0),
                json.dumps(equity_curve),
                json.dumps(trades),
            ),
        )
        await self._connection.commit()

    async def get_backtest_results(self, run_id: str) -> list[dict]:
        """Get all results for a backtest run."""
        cursor = await self._connection.execute(
            """
            SELECT * FROM backtest_results
            WHERE run_id = ?
            ORDER BY total_pnl DESC
            """,
            (run_id,),
        )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        results = []
        for row in rows:
            d = dict(zip(columns, row))
            d["equity_curve"] = json.loads(d["equity_curve"]) if d.get("equity_curve") else []
            d["trades"] = json.loads(d["trades"]) if d.get("trades") else []
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
        await self._connection.execute(
            """
            INSERT INTO backtest_comparisons (
                id, run_id, agent_id, baseline_id,
                outperformance, p_value, ci_low, ci_high, is_significant
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison_id,
                run_id,
                agent_id,
                baseline_id,
                outperformance,
                p_value,
                ci_low,
                ci_high,
                1 if is_significant else 0,
            ),
        )
        await self._connection.commit()

    async def get_comparisons(self, run_id: str) -> list[dict]:
        """Get all comparisons for a backtest run."""
        cursor = await self._connection.execute(
            """
            SELECT * FROM backtest_comparisons
            WHERE run_id = ?
            ORDER BY agent_id, baseline_id
            """,
            (run_id,),
        )
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        comparisons = []
        for row in rows:
            d = dict(zip(columns, row))
            d["is_significant"] = bool(d.get("is_significant"))
            comparisons.append(d)

        return comparisons

    async def delete_backtest_run(self, run_id: str) -> None:
        """Delete a backtest run and all associated data."""
        await self._connection.execute(
            "DELETE FROM backtest_comparisons WHERE run_id = ?",
            (run_id,),
        )
        await self._connection.execute(
            "DELETE FROM backtest_results WHERE run_id = ?",
            (run_id,),
        )
        await self._connection.execute(
            "DELETE FROM backtest_runs WHERE id = ?",
            (run_id,),
        )
        await self._connection.commit()
