"""Risk Calculator tool - position sizing, stop-loss, risk/reward."""

import json
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field

from agent_arena.agentic.tools.base import TradingTool


class RiskCalculatorInput(BaseModel):
    """Input schema for risk calculations."""

    symbol: str = Field(description="Trading symbol")
    side: str = Field(description="Position side: 'long' or 'short'")
    entry_price: Optional[float] = Field(
        default=None, description="Entry price (uses current price if not provided)"
    )
    stop_loss_pct: float = Field(default=2.0, description="Stop loss percentage from entry")
    take_profit_pct: float = Field(default=4.0, description="Take profit percentage from entry")
    risk_per_trade_pct: float = Field(
        default=2.0, description="Maximum portfolio risk per trade as percentage"
    )


class RiskCalculatorTool(TradingTool):
    """
    Calculate position size and risk parameters.

    Provides:
    - Optimal position size based on risk tolerance
    - Stop-loss and take-profit levels
    - Risk/reward ratio
    - Margin requirements
    """

    name: str = "risk_calculator"
    description: str = """Calculate optimal position size and risk parameters.
Use this BEFORE opening any position to ensure proper risk management.
Input: symbol, side (long/short), stop_loss_pct, take_profit_pct, risk_per_trade_pct.
Returns: Position size, stop-loss price, take-profit price, risk/reward ratio."""

    args_schema: type[BaseModel] = RiskCalculatorInput

    def _run(
        self,
        symbol: str,
        side: str,
        entry_price: Optional[float] = None,
        stop_loss_pct: float = 2.0,
        take_profit_pct: float = 4.0,
        risk_per_trade_pct: float = 2.0,
    ) -> str:
        """Calculate risk parameters."""
        portfolio = self._context.get("portfolio", {})
        market = self._context.get("market", {})

        equity = Decimal(str(portfolio.get("equity", 10000)))
        available_margin = Decimal(str(portfolio.get("available_margin", 10000)))

        # Get current price if not provided
        if entry_price is None:
            symbol_data = market.get(symbol, {})
            entry_price = float(symbol_data.get("price", 0))

        if entry_price <= 0:
            return json.dumps({"error": f"No price data available for {symbol}"})

        entry = Decimal(str(entry_price))
        side = side.lower()

        if side not in ["long", "short"]:
            return json.dumps({"error": f"Invalid side: {side}. Use 'long' or 'short'"})

        # Calculate stop-loss and take-profit prices
        if side == "long":
            stop_loss = entry * (1 - Decimal(str(stop_loss_pct)) / 100)
            take_profit = entry * (1 + Decimal(str(take_profit_pct)) / 100)
        else:
            stop_loss = entry * (1 + Decimal(str(stop_loss_pct)) / 100)
            take_profit = entry * (1 - Decimal(str(take_profit_pct)) / 100)

        # Calculate risk per unit
        risk_per_unit = abs(entry - stop_loss)

        # Calculate position size based on risk tolerance
        risk_amount = equity * Decimal(str(risk_per_trade_pct)) / 100

        if risk_per_unit > 0:
            position_size = risk_amount / risk_per_unit
        else:
            position_size = Decimal("0")

        # Apply constraints
        # Max 25% of equity per position
        max_position_value = equity * Decimal("0.25")
        max_size_by_value = max_position_value / entry
        position_size = min(position_size, max_size_by_value)

        # Ensure we have margin (assuming up to 10x leverage)
        margin_required_at_1x = position_size * entry
        min_margin_required = margin_required_at_1x / 10  # At max 10x leverage

        if min_margin_required > available_margin:
            position_size = (available_margin * 10) / entry

        # Calculate recommended leverage
        position_value = position_size * entry
        if position_value > 0:
            recommended_leverage = min(10, max(1, int(float(equity) / float(min_margin_required))))
        else:
            recommended_leverage = 1

        # Risk/Reward ratio
        reward_per_unit = abs(take_profit - entry)
        risk_reward = float(reward_per_unit / risk_per_unit) if risk_per_unit > 0 else 0

        # Calculate potential outcomes
        max_loss = float(risk_amount)
        potential_profit = float(risk_amount * Decimal(str(risk_reward)))

        result = {
            "symbol": symbol,
            "side": side,
            "analysis": {
                "entry_price": float(round(entry, 2)),
                "stop_loss_price": float(round(stop_loss, 2)),
                "take_profit_price": float(round(take_profit, 2)),
                "risk_reward_ratio": round(risk_reward, 2),
            },
            "recommendation": {
                "position_size": float(round(position_size, 8)),
                "recommended_leverage": recommended_leverage,
                "margin_required": float(round(position_value / recommended_leverage, 2)),
            },
            "risk_assessment": {
                "max_loss_usd": round(max_loss, 2),
                "potential_profit_usd": round(potential_profit, 2),
                "risk_as_pct_of_equity": float(risk_per_trade_pct),
            },
            "constraints_applied": {
                "max_position_pct": 25,
                "max_leverage": 10,
                "available_margin": float(round(available_margin, 2)),
            },
            "verdict": self._get_verdict(risk_reward, position_size),
        }

        return json.dumps(result, indent=2)

    def _get_verdict(self, risk_reward: float, position_size: Decimal) -> str:
        """Get trading verdict based on calculations."""
        if position_size <= 0:
            return "SKIP - Position size too small or insufficient margin"
        elif risk_reward < 1:
            return "CAUTION - Risk/reward ratio below 1:1, consider wider take profit"
        elif risk_reward < 1.5:
            return "ACCEPTABLE - Marginal risk/reward, proceed with caution"
        elif risk_reward < 2:
            return "GOOD - Favorable risk/reward ratio"
        else:
            return "EXCELLENT - Strong risk/reward ratio, favorable setup"
