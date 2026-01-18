#!/usr/bin/env python3
"""Migrate data from SQLite to PostgreSQL."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from decimal import Decimal, InvalidOperation

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import aiosqlite

try:
    import asyncpg
except ImportError:
    print("Error: asyncpg not installed. Run: pip install asyncpg")
    sys.exit(1)


# Tables to migrate in order (respecting foreign keys)
TABLES = [
    "competitions",
    "decisions",
    "trades",
    "snapshots",
    "agent_memories",
    "agent_summaries",
    "funding_payments",
    "liquidations",
    "sl_tp_triggers",
]

# Column mappings for type conversion
DECIMAL_COLUMNS = {
    "decisions": ["size"],
    "trades": ["size", "price", "fee", "realized_pnl"],
    "funding_payments": ["funding_rate", "notional", "amount"],
    "liquidations": [
        "size", "entry_price", "liquidation_price", "mark_price",
        "margin_lost", "fee", "total_loss"
    ],
    "sl_tp_triggers": ["trigger_price", "mark_price", "size", "realized_pnl", "fee"],
}

JSON_COLUMNS = {
    "decisions": ["metadata"],
    "competitions": ["config", "final_leaderboard"],
    "snapshots": ["leaderboard", "market_data"],
    "agent_memories": ["metadata"],
}


def convert_value(table: str, column: str, value):
    """Convert SQLite value to PostgreSQL compatible type."""
    if value is None:
        return None

    # Handle Decimal columns
    if table in DECIMAL_COLUMNS and column in DECIMAL_COLUMNS[table]:
        if isinstance(value, str):
            try:
                return Decimal(value)
            except InvalidOperation:
                return None
        return Decimal(str(value)) if value else None

    # Handle JSON columns - PostgreSQL asyncpg accepts dicts directly
    if table in JSON_COLUMNS and column in JSON_COLUMNS[table]:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    return value


async def get_table_columns(sqlite_conn, table_name: str) -> list[str]:
    """Get column names for a table."""
    cursor = await sqlite_conn.execute(f"PRAGMA table_info({table_name})")
    rows = await cursor.fetchall()
    return [row[1] for row in rows]


async def migrate_table(
    sqlite_conn: aiosqlite.Connection,
    pg_pool: asyncpg.Pool,
    table_name: str,
    batch_size: int = 1000,
) -> int:
    """Migrate a single table from SQLite to PostgreSQL."""

    # Check if table exists in SQLite
    cursor = await sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    if not await cursor.fetchone():
        print(f"  Table {table_name} doesn't exist in SQLite, skipping")
        return 0

    # Get columns
    columns = await get_table_columns(sqlite_conn, table_name)

    # Count rows
    cursor = await sqlite_conn.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = (await cursor.fetchone())[0]

    if total_rows == 0:
        print(f"  Table {table_name} is empty, skipping")
        return 0

    print(f"  Migrating {total_rows} rows from {table_name}...")

    # Build INSERT statement
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    cols = ", ".join(columns)

    # Handle conflicts based on table
    conflict_clause = ""
    if table_name == "trades":
        conflict_clause = " ON CONFLICT (id) DO NOTHING"
    elif table_name == "decisions":
        # Decisions have auto-increment ID, skip conflicts
        conflict_clause = " ON CONFLICT DO NOTHING"

    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}){conflict_clause}"

    # Migrate in batches
    migrated = 0
    offset = 0

    while offset < total_rows:
        cursor = await sqlite_conn.execute(
            f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        )
        rows = await cursor.fetchall()

        if not rows:
            break

        async with pg_pool.acquire() as conn:
            for row in rows:
                # Convert values
                values = []
                for i, col in enumerate(columns):
                    values.append(convert_value(table_name, col, row[i]))

                try:
                    await conn.execute(insert_query, *values)
                    migrated += 1
                except Exception as e:
                    print(f"    Error migrating row: {e}")
                    continue

        offset += batch_size
        print(f"    Progress: {min(offset, total_rows)}/{total_rows}")

    return migrated


async def reset_sequences(pg_pool: asyncpg.Pool) -> None:
    """Reset PostgreSQL sequences to continue from max ID."""
    sequences = [
        ("decisions", "decisions_id_seq"),
        ("competitions", "competitions_id_seq"),
        ("snapshots", "snapshots_id_seq"),
        ("agent_memories", "agent_memories_id_seq"),
        ("agent_summaries", "agent_summaries_id_seq"),
        ("funding_payments", "funding_payments_id_seq"),
        ("liquidations", "liquidations_id_seq"),
        ("sl_tp_triggers", "sl_tp_triggers_id_seq"),
    ]

    async with pg_pool.acquire() as conn:
        for table, seq in sequences:
            try:
                await conn.execute(f"""
                    SELECT setval('{seq}',
                        COALESCE((SELECT MAX(id) FROM {table}), 0) + 1,
                        false
                    )
                """)
            except Exception as e:
                print(f"  Warning: Could not reset sequence {seq}: {e}")


async def migrate(
    sqlite_path: str = "data/arena.db",
    postgres_url: str = None,
    skip_tables: list[str] = None,
) -> dict:
    """
    Migrate all data from SQLite to PostgreSQL.

    Args:
        sqlite_path: Path to SQLite database file.
        postgres_url: PostgreSQL connection URL.
        skip_tables: List of table names to skip.

    Returns:
        Dict with migration statistics.
    """
    from dotenv import load_dotenv
    load_dotenv()

    postgres_url = postgres_url or os.getenv("DATABASE_URL")
    skip_tables = skip_tables or []

    if not postgres_url:
        raise ValueError(
            "PostgreSQL URL required. Set DATABASE_URL environment variable "
            "or pass postgres_url argument."
        )

    sqlite_path = Path(sqlite_path)
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")

    print(f"Migrating from {sqlite_path} to PostgreSQL...")
    print(f"Connection: {postgres_url.split('@')[1] if '@' in postgres_url else 'hidden'}")

    # Connect to both databases
    sqlite_conn = await aiosqlite.connect(sqlite_path)
    pg_pool = await asyncpg.create_pool(postgres_url, min_size=2, max_size=10)

    stats = {"tables": {}, "total_migrated": 0, "errors": []}

    try:
        # Initialize PostgreSQL tables first
        print("\nInitializing PostgreSQL tables...")
        from agent_arena.storage.postgres import PostgresStorage
        pg_storage = PostgresStorage(postgres_url)
        pg_storage.pool = pg_pool
        await pg_storage._create_tables()
        print("  Tables created successfully")

        # Migrate each table
        print("\nMigrating data...")
        for table in TABLES:
            if table in skip_tables:
                print(f"\nSkipping {table} (in skip list)")
                continue

            print(f"\n{table}:")
            try:
                count = await migrate_table(sqlite_conn, pg_pool, table)
                stats["tables"][table] = count
                stats["total_migrated"] += count
            except Exception as e:
                print(f"  ERROR: {e}")
                stats["errors"].append({"table": table, "error": str(e)})

        # Reset sequences
        print("\nResetting sequences...")
        await reset_sequences(pg_pool)

        print("\n" + "=" * 50)
        print("Migration Complete!")
        print("=" * 50)
        print(f"Total rows migrated: {stats['total_migrated']}")
        for table, count in stats["tables"].items():
            print(f"  {table}: {count}")
        if stats["errors"]:
            print(f"\nErrors: {len(stats['errors'])}")
            for err in stats["errors"]:
                print(f"  {err['table']}: {err['error']}")

    finally:
        await sqlite_conn.close()
        await pg_pool.close()

    return stats


async def verify_migration(postgres_url: str = None) -> None:
    """Verify migration by comparing row counts."""
    from dotenv import load_dotenv
    load_dotenv()

    postgres_url = postgres_url or os.getenv("DATABASE_URL")

    pg_pool = await asyncpg.create_pool(postgres_url)

    print("\nVerifying PostgreSQL data...")

    try:
        async with pg_pool.acquire() as conn:
            for table in TABLES:
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    print(f"  {table}: {count} rows")
                except Exception as e:
                    print(f"  {table}: ERROR - {e}")
    finally:
        await pg_pool.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Migrate Agent Arena data to PostgreSQL")
    parser.add_argument(
        "--sqlite",
        default="data/arena.db",
        help="Path to SQLite database (default: data/arena.db)"
    )
    parser.add_argument(
        "--postgres-url",
        help="PostgreSQL URL (default: from DATABASE_URL env)"
    )
    parser.add_argument(
        "--skip",
        nargs="*",
        default=[],
        help="Tables to skip"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing PostgreSQL data"
    )

    args = parser.parse_args()

    if args.verify_only:
        asyncio.run(verify_migration(args.postgres_url))
    else:
        asyncio.run(migrate(
            sqlite_path=args.sqlite,
            postgres_url=args.postgres_url,
            skip_tables=args.skip,
        ))
