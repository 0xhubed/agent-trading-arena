"""REST API routes for Agent Arena."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException

router = APIRouter()

# These will be set by the app on startup
_storage = None
_arena = None
_runner = None


def set_dependencies(storage: Any, arena: Any, runner: Any) -> None:
    """Set module-level dependencies."""
    global _storage, _arena, _runner
    _storage = storage
    _arena = arena
    _runner = runner


@router.get("/status")
async def get_status() -> dict:
    """Get competition status."""
    if not _runner:
        return {
            "status": "not_started",
            "tick": 0,
            "running": False,
        }

    return {
        "status": "running" if _runner.running else "stopped",
        "tick": _runner.tick,
        "running": _runner.running,
        "started_at": (
            _runner.started_at.isoformat().replace("+00:00", "Z")
            if _runner.started_at else None
        ),
        "config": {
            "name": _runner.config.name,
            "symbols": _runner.config.symbols,
            "interval_seconds": _runner.config.interval_seconds,
        },
    }


@router.get("/leaderboard")
async def get_leaderboard(extended: bool = False) -> list[dict]:
    """
    Get current leaderboard.

    Args:
        extended: If True, include additional metrics (win rate, Sharpe, etc.)
    """
    if not _arena:
        return []

    leaderboard = _arena.get_leaderboard()

    if extended:
        # Add analytics data to each entry
        all_analytics = _arena.get_all_analytics()
        for entry in leaderboard:
            agent_id = entry.get("agent_id")
            analytics = all_analytics.get(agent_id)
            if analytics:
                entry["total_trades"] = analytics.total_trades
                entry["closed_trades"] = analytics.closed_trades
                # Only show win rate, profit factor, expectancy if there are closed trades
                if analytics.closed_trades > 0:
                    entry["win_rate"] = round(analytics.win_rate * 100, 1)
                    pf = analytics.profit_factor
                    entry["profit_factor"] = round(pf, 2) if pf != float('inf') else None
                    entry["expectancy"] = round(float(analytics.expectancy), 2)
                else:
                    entry["win_rate"] = None
                    entry["profit_factor"] = None
                    entry["expectancy"] = None
                # Sharpe ratio is None if 0 (insufficient data)
                if analytics.sharpe_ratio != 0:
                    entry["sharpe_ratio"] = round(analytics.sharpe_ratio, 2)
                else:
                    entry["sharpe_ratio"] = None
                entry["max_drawdown"] = round(analytics.max_drawdown * 100, 1)

    return leaderboard


@router.get("/agents")
async def get_agents() -> list[dict]:
    """Get all agents with their portfolios."""
    if not _runner or not _arena:
        return []

    agents = []
    for agent_id, agent in _runner.agents.items():
        portfolio = _arena.get_portfolio(agent_id)
        agents.append({
            "id": agent_id,
            "name": agent.name,
            "model": getattr(agent, "model", "unknown"),
            "agent_type": agent.agent_type,
            "agent_type_description": agent.agent_type_description,
            "character": getattr(agent, "character", ""),
            "portfolio": portfolio.to_context() if portfolio else None,
        })
    return agents


@router.get("/agents/{agent_id}")
async def get_agent(agent_id: str) -> dict:
    """Get detailed info for a specific agent."""
    # Get recent decisions and trades from storage
    decisions = []
    trades = []
    if _storage:
        decisions = await _storage.get_recent_decisions(agent_id, limit=20)
        trades = await _storage.get_agent_trades(agent_id, limit=50)

    # If competition is running, get live data
    if _runner and _arena:
        agent = _runner.agents.get(agent_id)
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")

        portfolio = _arena.get_portfolio(agent_id)

        return {
            "id": agent_id,
            "name": agent.name,
            "model": getattr(agent, "model", "unknown"),
            "agent_type": agent.agent_type,
            "agent_type_description": agent.agent_type_description,
            "character": getattr(agent, "character", ""),
            "portfolio": portfolio.to_context() if portfolio else None,
            "recent_decisions": decisions,
            "trades": trades,
        }

    # Competition not running - return historical data if we have any
    if not decisions and not trades:
        raise HTTPException(status_code=404, detail="Agent not found (competition not running)")

    # Return basic info from historical data
    return {
        "id": agent_id,
        "name": agent_id,  # Use ID as name when competition not running
        "model": "unknown",
        "agent_type": "unknown",
        "agent_type_description": "",
        "character": "",
        "portfolio": None,
        "recent_decisions": decisions,
        "trades": trades,
    }


@router.get("/history/leaderboard")
async def get_leaderboard_history(limit: int = 100) -> list[dict]:
    """Get historical leaderboard data for charts."""
    if not _storage:
        return []
    return await _storage.get_leaderboard_history(limit=limit)


@router.get("/history/decisions")
async def get_decisions_history(agent_id: Optional[str] = None, limit: int = 50) -> list[dict]:
    """Get historical decisions."""
    if not _storage:
        return []

    if agent_id:
        return await _storage.get_recent_decisions(agent_id, limit=limit)

    # Get decisions for all agents
    if not _runner:
        return []

    all_decisions = []
    for aid in _runner.agents.keys():
        decisions = await _storage.get_recent_decisions(aid, limit=limit // len(_runner.agents))
        all_decisions.extend(decisions)

    # Sort by tick descending
    all_decisions.sort(key=lambda x: x.get("tick", 0), reverse=True)
    return all_decisions[:limit]


@router.get("/market")
async def get_market_data() -> dict:
    """Get current market prices."""
    if not _arena:
        return {}
    return {
        symbol: float(price)
        for symbol, price in _arena.current_prices.items()
    }


@router.get("/market/history")
async def get_market_history(
    symbol: Optional[str] = None,
    interval: str = "1h",
    limit: int = 100,
) -> dict:
    """
    Get historical candle data for market symbols.

    Args:
        symbol: Specific symbol (e.g., BTCUSDT). If None, returns all configured symbols.
        interval: Candle interval (1m, 5m, 15m, 1h, 4h, 1d). Default: 1h
        limit: Number of candles to fetch (max 200). Default: 100

    Returns:
        Dict with symbol as key and list of candles as value.
        Each candle: {open, high, low, close, volume, timestamp}
    """
    if not _runner:
        return {"error": "Competition not running", "candles": {}}

    # Find the Binance provider
    binance_provider = None
    for provider in _runner.providers:
        if provider.name == "binance":
            binance_provider = provider
            break

    if not binance_provider:
        return {"error": "Binance provider not available", "candles": {}}

    # Determine symbols to fetch
    symbols = [symbol] if symbol else _runner.config.symbols

    # Validate interval
    valid_intervals = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "1d"]
    if interval not in valid_intervals:
        return {"error": f"Invalid interval. Use one of: {valid_intervals}", "candles": {}}

    # Limit the number of candles
    limit = min(limit, 200)

    try:
        candles = await binance_provider.get_candles_multi(
            symbols=symbols,
            intervals=[interval],
            limit=limit,
        )

        # Format response
        result = {}
        for sym in symbols:
            if sym in candles and interval in candles[sym]:
                result[sym] = candles[sym][interval]
            else:
                result[sym] = []

        return {"interval": interval, "limit": limit, "candles": result}

    except Exception as e:
        return {"error": str(e), "candles": {}}


@router.get("/funding")
async def get_funding_history(agent_id: Optional[str] = None, limit: int = 100) -> list[dict]:
    """Get funding payment history."""
    if not _storage:
        return []
    return await _storage.get_funding_history(agent_id=agent_id, limit=limit)


@router.get("/liquidations")
async def get_liquidations_history(
    agent_id: Optional[str] = None,
    limit: int = 100,
) -> list[dict]:
    """Get liquidation history."""
    if not _storage:
        return []
    return await _storage.get_liquidation_history(agent_id=agent_id, limit=limit)


@router.get("/agents/{agent_id}/stats")
async def get_agent_stats(agent_id: str) -> dict:
    """
    Get comprehensive agent statistics including analytics.

    Returns trade stats, P&L metrics, risk metrics, and performance ratios.
    """
    if not _runner or not _arena:
        raise HTTPException(status_code=404, detail="Competition not running")

    agent = _runner.agents.get(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    # Get portfolio
    portfolio = _arena.get_portfolio(agent_id)
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    # Calculate analytics
    analytics = _arena.get_analytics(agent_id)

    # Get trade counts from storage
    trades = []
    funding_summary = {"paid": 0.0, "received": 0.0, "net": 0.0}
    liquidation_count = 0

    if _storage:
        trades = await _storage.get_agent_trades(agent_id, limit=1000)

        # Get funding summary
        funding_history = await _storage.get_funding_history(agent_id=agent_id, limit=1000)
        for f in funding_history:
            amount = float(f.get("amount", 0))
            if f.get("direction") == "paid":
                funding_summary["paid"] += abs(amount)
            else:
                funding_summary["received"] += abs(amount)
        funding_summary["net"] = funding_summary["received"] - funding_summary["paid"]

        # Get liquidation count
        liquidations = await _storage.get_liquidation_history(agent_id=agent_id, limit=1000)
        liquidation_count = len(liquidations)

    return {
        "agent_id": agent_id,
        "name": agent.name,
        "portfolio": portfolio.to_context(),
        "analytics": analytics.to_dict() if analytics else None,
        "trade_count": len(trades),
        "funding": funding_summary,
        "liquidations": liquidation_count,
    }


@router.get("/analytics")
async def get_all_analytics() -> dict[str, dict]:
    """Get analytics for all agents."""
    if not _arena:
        return {}

    all_analytics = _arena.get_all_analytics()
    return {
        agent_id: analytics.to_dict() if analytics else None
        for agent_id, analytics in all_analytics.items()
    }


@router.get("/agents/{agent_id}/behavioral")
async def get_agent_behavioral(agent_id: str) -> dict:
    """
    Get behavioral statistics for an agent.

    Includes action distribution, confidence stats, symbol distribution,
    long/short ratio, and average leverage.
    """
    if not _runner:
        raise HTTPException(status_code=404, detail="Competition not running")

    agent = _runner.agents.get(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if not _storage:
        return {}

    return await _storage.get_agent_behavioral_stats(agent_id)


@router.get("/agents/{agent_id}/full")
async def get_agent_full(agent_id: str) -> dict:
    """
    Get comprehensive agent data including portfolio, analytics, behavioral stats,
    recent decisions, and trades.
    """
    # Get data from storage first
    decisions = []
    trades = []
    behavioral = {}
    funding_summary = {"paid": 0.0, "received": 0.0, "net": 0.0}
    liquidation_count = 0

    if _storage:
        decisions = await _storage.get_recent_decisions(agent_id, limit=50)
        trades = await _storage.get_agent_trades(agent_id, limit=100)
        behavioral = await _storage.get_agent_behavioral_stats(agent_id)

        # Get funding summary
        funding_history = await _storage.get_funding_history(agent_id=agent_id, limit=1000)
        for f in funding_history:
            amount = float(f.get("amount", 0))
            if f.get("direction") == "paid":
                funding_summary["paid"] += abs(amount)
            else:
                funding_summary["received"] += abs(amount)
        funding_summary["net"] = funding_summary["received"] - funding_summary["paid"]

        # Get liquidation count
        liquidations = await _storage.get_liquidation_history(agent_id=agent_id, limit=1000)
        liquidation_count = len(liquidations)

    # If competition is running, get live data
    if _runner and _arena:
        agent = _runner.agents.get(agent_id)
        if not agent:
            raise HTTPException(status_code=404, detail="Agent not found")

        portfolio = _arena.get_portfolio(agent_id)
        analytics = _arena.get_analytics(agent_id)

        return {
            "id": agent_id,
            "name": agent.name,
            "model": getattr(agent, "model", "unknown"),
            "agent_type": agent.agent_type,
            "agent_type_description": agent.agent_type_description,
            "character": getattr(agent, "character", ""),
            "portfolio": portfolio.to_context() if portfolio else None,
            "analytics": analytics.to_dict() if analytics else None,
            "behavioral": behavioral,
            "funding": funding_summary,
            "liquidations": liquidation_count,
            "recent_decisions": decisions,
            "trades": trades,
        }

    # Competition not running - return historical data if we have any
    if not decisions and not trades:
        raise HTTPException(status_code=404, detail="Agent not found (competition not running)")

    # Return historical data without live portfolio/analytics
    return {
        "id": agent_id,
        "name": agent_id,
        "model": "unknown",
        "agent_type": "unknown",
        "agent_type_description": "",
        "character": "",
        "portfolio": None,
        "analytics": None,
        "behavioral": behavioral,
        "funding": funding_summary,
        "liquidations": liquidation_count,
        "recent_decisions": decisions,
        "trades": trades,
    }


# =============================================================================
# Learning Agent Endpoints
# =============================================================================


@router.get("/agents/{agent_id}/learning")
async def get_agent_learning(agent_id: str) -> dict:
    """
    Get learning statistics for a learning agent.

    Returns patterns learned, RAG usage stats, and learning curve data.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not available")

    # Check if this is a learning agent
    is_learning_agent = False
    if _runner:
        agent = _runner.agents.get(agent_id)
        if agent and hasattr(agent, "_embedder"):
            is_learning_agent = True

    try:
        # Get learning stats
        stats = await _storage.get_learning_stats(agent_id)

        # Get patterns
        patterns = await _storage.get_agent_patterns(agent_id, min_confidence=0.3)

        # Get learning curve
        curve = await _storage.get_learning_curve(agent_id)

        return {
            "agent_id": agent_id,
            "is_learning_agent": is_learning_agent,
            "stats": {
                "patterns_learned": stats.get("patterns_learned", 0),
                "situations_referenced": stats.get("total_rag_queries", 0),
                "reflections_made": stats.get("reflections_count", 0),
                "improvement_vs_baseline": stats.get("improvement_pct", 0),
            },
            "learning_curve": curve,
            "patterns": patterns[:10],  # Top 10 patterns
        }
    except Exception as e:
        # Return empty data if storage doesn't support learning features
        return {
            "agent_id": agent_id,
            "is_learning_agent": is_learning_agent,
            "stats": {
                "patterns_learned": 0,
                "situations_referenced": 0,
                "reflections_made": 0,
                "improvement_vs_baseline": 0,
            },
            "learning_curve": [],
            "patterns": [],
            "error": str(e) if not is_learning_agent else None,
        }


@router.get("/agents/{agent_id}/patterns")
async def get_agent_patterns(
    agent_id: str,
    pattern_type: Optional[str] = None,
    min_confidence: float = 0.5,
) -> list[dict]:
    """
    Get learned patterns for an agent.

    Args:
        agent_id: Agent ID
        pattern_type: Filter by type (entry_signal, exit_signal, risk_rule, regime_rule)
        min_confidence: Minimum confidence threshold (0-1)
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not available")

    try:
        return await _storage.get_agent_patterns(
            agent_id=agent_id,
            pattern_type=pattern_type,
            min_confidence=min_confidence,
        )
    except AttributeError:
        # Storage doesn't support learning features
        return []


@router.get("/agents/{agent_id}/similar-situations")
async def get_similar_situations(
    agent_id: str,
    decision_id: Optional[int] = None,
    limit: int = 5,
) -> dict:
    """
    Get similar historical situations for a decision context.

    If decision_id is provided, uses that decision's context.
    Otherwise, uses the latest decision context.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not available")

    try:
        # Get context for the decision
        if decision_id:
            context = await _storage.get_decision_context(decision_id)
        else:
            context = await _storage.get_latest_decision_context(agent_id)

        if not context:
            return {
                "current_context": None,
                "situations": [],
                "message": "No decision context found",
            }

        # Get embedding service
        from agent_arena.core.embeddings import get_embedding_service
        embedder = get_embedding_service()

        # Generate embedding
        embedding = await embedder.embed_context(context)

        # Find similar contexts
        situations = await _storage.find_similar_contexts(
            embedding=embedding,
            limit=limit,
        )

        return {
            "current_context": {
                "regime": context.get("regime"),
                "volatility_percentile": context.get("volatility_percentile"),
                "tick": context.get("tick"),
            },
            "situations": situations,
        }

    except Exception as e:
        return {
            "current_context": None,
            "situations": [],
            "error": str(e),
        }


@router.get("/agents/{agent_id}/meta-analysis")
async def get_meta_analysis(agent_id: str) -> dict:
    """
    Get meta-learning analysis for an agent.

    Shows how this agent compares to top performers in the current regime.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not available")

    # Get current regime
    regime = "unknown"
    if _runner and hasattr(_runner, "last_context"):
        regime = getattr(_runner, "last_context", {}).get("regime", "unknown")

    try:
        # Get top performers in this regime
        top_performers = await _storage.get_regime_performance(
            regime=regime,
            min_trades=5,
        )

        # Find this agent's ranking
        this_agent = next(
            (p for p in top_performers if p.get("agent_id") == agent_id),
            None
        )
        this_agent_rank = None
        if this_agent:
            this_agent_rank = top_performers.index(this_agent) + 1

        # Generate insight
        insight = _generate_regime_insight(regime, top_performers, this_agent)

        return {
            "current_regime": regime,
            "top_performers": top_performers[:5],
            "this_agent": this_agent,
            "this_agent_rank": this_agent_rank,
            "total_agents_in_regime": len(top_performers),
            "insight": insight,
        }

    except Exception as e:
        return {
            "current_regime": regime,
            "top_performers": [],
            "this_agent": None,
            "this_agent_rank": None,
            "error": str(e),
        }


@router.get("/learning-events")
async def get_learning_events(
    agent_id: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """
    Get recent learning events across all agents or for a specific agent.

    Events include pattern discoveries, reflections, strategy shifts, etc.
    """
    if not _storage:
        return []

    try:
        return await _storage.get_recent_learning_events(
            agent_id=agent_id,
            limit=limit,
        )
    except AttributeError:
        # Storage doesn't support learning features
        return []


@router.get("/regime-performance")
async def get_regime_performance(
    regime: Optional[str] = None,
    min_trades: int = 10,
) -> list[dict]:
    """
    Get agent performance breakdown by market regime.

    If regime is not specified, returns performance for all regimes.
    """
    if not _storage:
        return []

    # If no regime specified, use current regime
    if not regime and _runner:
        regime = getattr(_runner, "last_context", {}).get("regime", "unknown")

    if not regime or regime == "unknown":
        # Return empty - need a specific regime
        return []

    try:
        return await _storage.get_regime_performance(
            regime=regime,
            min_trades=min_trades,
        )
    except AttributeError:
        return []


def _generate_regime_insight(
    regime: str,
    top_performers: list[dict],
    this_agent: Optional[dict],
) -> str:
    """Generate human-readable insight about regime performance."""
    if not top_performers:
        return f"No performance data available for {regime} regime."

    top = top_performers[0] if top_performers else None
    avg_win_rate = (
        sum(p.get("win_rate", 0) for p in top_performers) / len(top_performers)
        if top_performers else 0
    )

    insights = []

    if top:
        insights.append(
            f"Top performer in {regime}: {top.get('agent_id')} "
            f"with {top.get('win_rate', 0):.0%} win rate"
        )

    if this_agent:
        win_rate = this_agent.get("win_rate", 0)
        if win_rate > avg_win_rate:
            insights.append(
                f"You're performing above average ({win_rate:.0%} vs {avg_win_rate:.0%})"
            )
        else:
            insights.append(
                f"Room for improvement: {win_rate:.0%} vs average {avg_win_rate:.0%}"
            )
    else:
        insights.append("No performance data for this agent in current regime")

    # Regime-specific advice
    regime_advice = {
        "trending_up": "Consider trend-following strategies with trailing stops.",
        "trending_down": "Short positions may be favorable. Manage risk carefully.",
        "ranging": "Mean-reversion at support/resistance. Smaller positions.",
        "volatile": "Reduce exposure. Wait for clarity or use wider stops.",
    }
    if regime in regime_advice:
        insights.append(regime_advice[regime])

    return " ".join(insights)


# =============================================================================
# Admin Endpoints
# =============================================================================


@router.post("/reset")
async def reset_database(confirm: bool = False) -> dict:
    """
    Reset the database to clean state.

    WARNING: This will delete ALL data including decisions, trades,
    learned patterns, and competition history.

    Args:
        confirm: Must be True to actually reset (safety check)

    Returns:
        Status message with counts of deleted records
    """
    if not confirm:
        return {
            "status": "aborted",
            "message": "Reset requires confirm=true parameter. "
                       "Example: POST /api/reset?confirm=true",
            "warning": "This will DELETE ALL DATA!",
        }

    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not available")

    # Stop competition if running
    if _runner and _runner.running:
        _runner.stop()

    # Reset arena state if available
    if _arena:
        _arena.reset()

    # Get counts before reset
    counts = {}
    tables = [
        "decisions", "trades", "snapshots", "competitions",
        "agent_memories", "agent_summaries", "funding_payments",
        "liquidations", "sl_tp_triggers", "decision_contexts",
        "decision_outcomes", "learned_patterns", "candle_history",
        "regime_performance", "learning_events",
    ]

    try:
        # For PostgreSQL storage
        if hasattr(_storage, "pool") and _storage.pool:
            async with _storage.pool.acquire() as conn:
                # Get counts
                for table in tables:
                    try:
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                        counts[table] = count
                    except Exception:
                        counts[table] = 0

                # Truncate all tables
                await conn.execute(f"""
                    TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE
                """)

        # For SQLite storage
        elif hasattr(_storage, "db_path"):
            import aiosqlite
            async with aiosqlite.connect(_storage.db_path) as db:
                for table in tables:
                    try:
                        cursor = await db.execute(f"SELECT COUNT(*) FROM {table}")
                        row = await cursor.fetchone()
                        counts[table] = row[0] if row else 0
                        await db.execute(f"DELETE FROM {table}")
                    except Exception:
                        counts[table] = 0
                await db.commit()

        total_deleted = sum(counts.values())

        return {
            "status": "success",
            "message": f"Database reset complete. Deleted {total_deleted} records.",
            "deleted_counts": counts,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Reset failed: {str(e)}"
        )


# =============================================================================
# Observer Agent Endpoints
# =============================================================================

# Global observer instance
_observer: Optional["ObserverAgent"] = None


@router.post("/observer/analyze")
async def run_observer_analysis(
    lookback_hours: int = 24,
    min_confidence: float = 0.6,
    min_sample_size: int = 10,
) -> dict:
    """
    Run the Observer Agent to analyze competition data and update skills.

    This triggers the daily learning cycle:
    1. Collect decisions, trades, outcomes from the past N hours
    2. Analyze patterns using LLM
    3. Generate/update SKILL.md files

    Args:
        lookback_hours: Hours of data to analyze (default: 24)
        min_confidence: Minimum confidence for pattern inclusion (default: 0.6)
        min_sample_size: Minimum sample size for statistical significance (default: 10)

    Returns:
        Analysis summary with patterns found and skills updated
    """
    global _observer

    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not available")

    # Import here to avoid circular imports
    from agent_arena.agents.observer_agent import ObserverAgent

    # Create or reuse observer
    if _observer is None:
        _observer = ObserverAgent(
            storage=_storage,
            skills_dir="skills",
            min_confidence=min_confidence,
            min_sample_size=min_sample_size,
        )

    try:
        result = await _observer.run_daily_analysis(lookback_hours=lookback_hours)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Observer analysis failed: {str(e)}"
        )


@router.get("/skills")
async def list_skills() -> dict:
    """
    List all available trading skills.

    Returns metadata about each skill including:
    - Name and description
    - Last update time
    - Pattern count
    - Confidence threshold
    """
    from pathlib import Path
    import json

    skills_dir = Path("skills")
    skills = []

    if skills_dir.exists():
        for skill_dir in skills_dir.iterdir():
            if skill_dir.is_dir():
                skill_file = skill_dir / "SKILL.md"
                meta_file = skill_dir / ".skill_meta.json"

                if skill_file.exists():
                    skill_info = {
                        "name": skill_dir.name,
                        "path": str(skill_file),
                        "size_bytes": skill_file.stat().st_size,
                    }

                    # Load metadata if available
                    if meta_file.exists():
                        try:
                            meta = json.loads(meta_file.read_text())
                            skill_info.update(meta)
                        except json.JSONDecodeError:
                            pass

                    # Parse frontmatter for description
                    content = skill_file.read_text()
                    if content.startswith("---"):
                        parts = content.split("---", 2)
                        if len(parts) >= 3:
                            for line in parts[1].strip().split("\n"):
                                if line.startswith("description:"):
                                    skill_info["description"] = line.split(":", 1)[1].strip()
                                    break

                    skills.append(skill_info)

    return {
        "skills_dir": str(skills_dir),
        "count": len(skills),
        "skills": skills,
    }


@router.get("/skills/{skill_name}")
async def get_skill(skill_name: str) -> dict:
    """
    Get the content of a specific skill.

    Args:
        skill_name: Name of the skill (e.g., "trading-wisdom", "market-regimes")

    Returns:
        Skill content and metadata
    """
    from pathlib import Path
    import json

    skill_file = Path(f"skills/{skill_name}/SKILL.md")

    if not skill_file.exists():
        raise HTTPException(status_code=404, detail=f"Skill '{skill_name}' not found")

    content = skill_file.read_text()

    # Load metadata
    meta_file = skill_file.parent / ".skill_meta.json"
    meta = {}
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text())
        except json.JSONDecodeError:
            pass

    return {
        "name": skill_name,
        "content": content,
        "metadata": meta,
    }


@router.get("/observer/status")
async def get_observer_status() -> dict:
    """
    Get the status of the Observer Agent.

    Returns:
        Observer status including last analysis time and history
    """
    global _observer

    if _observer is None:
        return {
            "status": "not_initialized",
            "message": "Observer has not been run yet. Use POST /observer/analyze to run.",
        }

    return {
        "status": "initialized",
        "last_analysis": _observer._last_analysis.isoformat() if _observer._last_analysis else None,
        "analysis_count": len(_observer._analysis_history),
        "recent_analyses": _observer._analysis_history[-5:] if _observer._analysis_history else [],
    }
