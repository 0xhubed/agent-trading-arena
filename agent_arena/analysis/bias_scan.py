"""Behavioral bias calculators for trading agents.

All calculators are pure functions taking lists of dicts (from storage).
No storage dependency — fully testable with synthetic data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .bias_models import BiasProfile, BiasScore

# ---------------------------------------------------------------------------
# Shared Trade Lookup Helpers (#3 + #4 from review)
# ---------------------------------------------------------------------------

def _build_trade_lookups(
    trades: list[dict],
) -> tuple[dict, dict]:
    """Build lookup dicts for trades by decision_id and trade_id.

    Returns (trade_by_decision_id, trade_by_trade_id).
    """
    by_decision: dict = {}
    by_trade_id: dict = {}
    for t in trades:
        did = t.get("decision_id")
        if did is not None:
            by_decision[did] = t
        tid = t.get("id")
        if tid is not None:
            by_trade_id[tid] = t
    return by_decision, by_trade_id


def _find_close_trade(
    dec: dict,
    trade_by_decision: dict,
    trade_by_id: dict,
) -> dict | None:
    """Find the trade for a close decision.

    Tries decision_id lookup first, falls back to trade_id field on the decision.
    """
    trade = trade_by_decision.get(dec.get("id"))
    if trade is not None:
        return trade
    tid = dec.get("trade_id")
    if tid:
        return trade_by_id.get(tid)
    return None


def _parse_realized_pnl(trade: dict | None) -> float:
    """Extract realized_pnl from a trade dict, defaulting to 0.0."""
    if trade is None:
        return 0.0
    pnl = trade.get("realized_pnl")
    if pnl is None:
        return 0.0
    try:
        return float(pnl)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Open/Close Pair Matching
# ---------------------------------------------------------------------------

def _match_open_close_pairs(
    decisions: list[dict],
    trades: list[dict],
) -> list[dict]:
    """Match open decisions to their corresponding close decisions.

    Strategy: Sort decisions by tick ascending. Track the most recent executed
    open per (agent_id, symbol). TradingArena only allows one position per
    symbol, so we use a simple dict (not a stack). Only opens that actually
    resulted in a trade are tracked — rejected opens are ignored.

    When a ``close`` decision appears, match it against the tracked open for
    that key. Look up the closing trade to get realized_pnl.

    Returns list of matched pairs with: open_tick, close_tick, hold_duration,
    open_size, realized_pnl, open_confidence.
    """
    trade_by_decision, trade_by_id = _build_trade_lookups(trades)

    sorted_decs = sorted(decisions, key=lambda d: d.get("tick", 0))

    open_actions = {"open_long", "open_short", "limit_long", "limit_short"}
    # Single open per (agent, symbol) — mirrors TradingArena's one-position model
    current_open: dict[tuple[str, str], dict] = {}
    pairs: list[dict] = []

    for dec in sorted_decs:
        action = dec.get("action", "")
        symbol = dec.get("symbol")
        agent_id = dec.get("agent_id", "")

        if not symbol:
            continue

        key = (agent_id, symbol)

        if action in open_actions:
            # Only track opens that actually resulted in a trade (#1 from review)
            if trade_by_decision.get(dec.get("id")) is not None:
                current_open[key] = dec

        elif action == "close":
            open_dec = current_open.pop(key, None)
            if open_dec is None:
                continue

            close_trade = _find_close_trade(dec, trade_by_decision, trade_by_id)
            realized_pnl = _parse_realized_pnl(close_trade)

            open_tick = open_dec.get("tick", 0)
            close_tick = dec.get("tick", 0)

            open_confidence = open_dec.get("confidence")
            if open_confidence is not None:
                try:
                    open_confidence = float(open_confidence)
                except (ValueError, TypeError):
                    open_confidence = None

            open_size = open_dec.get("size")
            if open_size is not None:
                try:
                    open_size = float(open_size)
                except (ValueError, TypeError):
                    open_size = None

            pairs.append({
                "agent_id": agent_id,
                "symbol": symbol,
                "open_tick": open_tick,
                "close_tick": close_tick,
                "hold_duration": close_tick - open_tick,
                "open_size": open_size,
                "realized_pnl": realized_pnl,
                "open_confidence": open_confidence,
            })

    return pairs


# ---------------------------------------------------------------------------
# Disposition Effect
# ---------------------------------------------------------------------------

def calculate_disposition_effect(
    pairs: list[dict],
    min_winners: int = 10,
    min_losers: int = 10,
) -> BiasScore:
    """Measure the disposition effect — tendency to hold losers longer than winners.

    Score = avg_loser_duration / (avg_loser_duration + avg_winner_duration)
    0.0 = holds winners much longer (good), 0.5 = equal, 1.0 = holds losers much longer.
    """
    winners = [p for p in pairs if p["realized_pnl"] > 0]
    losers = [p for p in pairs if p["realized_pnl"] < 0]

    if len(winners) < min_winners or len(losers) < min_losers:
        return BiasScore(
            bias_type="disposition_effect",
            value=None,
            sample_size=len(pairs),
            sufficient_data=False,
            details={
                "winners": len(winners),
                "losers": len(losers),
                "min_required": min_winners,
            },
        )

    avg_winner_dur = sum(p["hold_duration"] for p in winners) / len(winners)
    avg_loser_dur = sum(p["hold_duration"] for p in losers) / len(losers)

    denominator = avg_loser_dur + avg_winner_dur
    if denominator == 0:
        score = 0.5
    else:
        score = avg_loser_dur / denominator

    return BiasScore(
        bias_type="disposition_effect",
        value=max(0.0, min(1.0, score)),
        sample_size=len(winners) + len(losers),
        sufficient_data=True,
        details={
            "avg_winner_duration": round(avg_winner_dur, 2),
            "avg_loser_duration": round(avg_loser_dur, 2),
            "winners": len(winners),
            "losers": len(losers),
        },
    )


# ---------------------------------------------------------------------------
# Loss Aversion
# ---------------------------------------------------------------------------

def calculate_loss_aversion(
    decisions: list[dict],
    trades: list[dict],
    min_post_win: int = 10,
    min_post_loss: int = 10,
) -> BiasScore:
    """Measure loss aversion — tendency to reduce size after losses.

    For each close decision, find the next open by the same agent and compare
    avg position size after wins vs after losses.

    Score = 1 - (avg_size_after_loss / avg_size_after_win), clamped [0, 1].
    0.0 = equal sizing, 1.0 = dramatic size reduction after losses.
    """
    trade_by_decision, trade_by_id = _build_trade_lookups(trades)

    open_actions = {"open_long", "open_short", "limit_long", "limit_short"}
    sorted_decs = sorted(decisions, key=lambda d: d.get("tick", 0))

    # Group by agent_id
    by_agent: dict[str, list[dict]] = {}
    for dec in sorted_decs:
        by_agent.setdefault(dec.get("agent_id", ""), []).append(dec)

    sizes_after_win: list[float] = []
    sizes_after_loss: list[float] = []

    for agent_id, agent_decs in by_agent.items():
        last_close_pnl: Optional[float] = None

        for dec in agent_decs:
            action = dec.get("action", "")

            if action == "close":
                close_trade = _find_close_trade(
                    dec, trade_by_decision, trade_by_id,
                )
                pnl = close_trade.get("realized_pnl") if close_trade else None
                if pnl is not None:
                    try:
                        last_close_pnl = float(pnl)
                    except (ValueError, TypeError):
                        last_close_pnl = None
                else:
                    last_close_pnl = None

            elif action in open_actions and last_close_pnl is not None:
                size = dec.get("size")
                if size is not None:
                    try:
                        size_val = float(size)
                    except (ValueError, TypeError):
                        continue

                    if last_close_pnl > 0:
                        sizes_after_win.append(size_val)
                    elif last_close_pnl < 0:
                        sizes_after_loss.append(size_val)

                # Reset after consuming
                last_close_pnl = None

    if len(sizes_after_win) < min_post_win or len(sizes_after_loss) < min_post_loss:
        return BiasScore(
            bias_type="loss_aversion",
            value=None,
            sample_size=len(sizes_after_win) + len(sizes_after_loss),
            sufficient_data=False,
            details={
                "post_win_opens": len(sizes_after_win),
                "post_loss_opens": len(sizes_after_loss),
                "min_required": min_post_win,
            },
        )

    avg_after_win = sum(sizes_after_win) / len(sizes_after_win)
    avg_after_loss = sum(sizes_after_loss) / len(sizes_after_loss)

    if avg_after_win == 0:
        score = 0.0
    else:
        score = 1.0 - (avg_after_loss / avg_after_win)

    return BiasScore(
        bias_type="loss_aversion",
        value=max(0.0, min(1.0, score)),
        sample_size=len(sizes_after_win) + len(sizes_after_loss),
        sufficient_data=True,
        details={
            "avg_size_after_win": round(avg_after_win, 4),
            "avg_size_after_loss": round(avg_after_loss, 4),
            "post_win_opens": len(sizes_after_win),
            "post_loss_opens": len(sizes_after_loss),
        },
    )


# ---------------------------------------------------------------------------
# Overconfidence
# ---------------------------------------------------------------------------

def calculate_overconfidence(
    pairs: list[dict],
    min_samples: int = 20,
) -> BiasScore:
    """Measure overconfidence — poor calibration between confidence and outcome.

    Pearson correlation between confidence and binary outcome (win=1, loss=0).
    Score = (1 - correlation) / 2, clamped [0, 1].
    0.0 = perfectly calibrated, 0.5 = decorrelated, 1.0 = anti-calibrated.
    """
    # Filter pairs that have confidence values
    valid = [
        p for p in pairs
        if p.get("open_confidence") is not None and p["realized_pnl"] != 0
    ]

    if len(valid) < min_samples:
        return BiasScore(
            bias_type="overconfidence",
            value=None,
            sample_size=len(valid),
            sufficient_data=False,
            details={
                "pairs_with_confidence": len(valid),
                "min_required": min_samples,
            },
        )

    confidences = [p["open_confidence"] for p in valid]
    outcomes = [1.0 if p["realized_pnl"] > 0 else 0.0 for p in valid]

    correlation = _pearson_correlation(confidences, outcomes)

    if correlation is None:
        score = 0.5  # No variance = assume decorrelated
    else:
        score = (1.0 - correlation) / 2.0

    return BiasScore(
        bias_type="overconfidence",
        value=max(0.0, min(1.0, score)),
        sample_size=len(valid),
        sufficient_data=True,
        details={
            "correlation": round(correlation, 4) if correlation is not None else None,
            "avg_confidence": round(sum(confidences) / len(confidences), 4),
            "win_rate": round(sum(outcomes) / len(outcomes), 4),
            "pairs_with_confidence": len(valid),
        },
    )


def _pearson_correlation(x: list[float], y: list[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient without numpy."""
    n = len(x)
    if n < 2:
        return None

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    var_y = sum((yi - mean_y) ** 2 for yi in y)

    denom = (var_x * var_y) ** 0.5
    if denom == 0:
        return None

    return cov / denom


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def analyze_agent_biases(
    agent_id: str,
    decisions: list[dict],
    trades: list[dict],
) -> BiasProfile:
    """Run all three bias calculators and return a complete BiasProfile."""
    pairs = _match_open_close_pairs(decisions, trades)

    disposition = calculate_disposition_effect(pairs)
    loss_av = calculate_loss_aversion(decisions, trades)
    overconf = calculate_overconfidence(pairs)

    return BiasProfile(
        agent_id=agent_id,
        timestamp=datetime.now(timezone.utc),
        disposition_effect=disposition,
        loss_aversion=loss_av,
        overconfidence=overconf,
    )
