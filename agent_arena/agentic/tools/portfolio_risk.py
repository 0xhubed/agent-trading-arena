"""Portfolio risk analysis tool - evaluates exposure and position sizing."""

import json
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel

from agent_arena.agentic.tools.base import TradingTool


class PortfolioRiskTool(TradingTool):
    """
    Evaluates current portfolio risk and exposure.

    Analyzes:
    - Long/short exposure balance
    - Symbol concentration risk
    - Margin utilization
    - Maximum position sizes available
    - Risk per position

    Call before opening new positions to ensure proper sizing
    and portfolio balance.
    """

    name: str = "portfolio_risk_analysis"
    description: str = """Analyzes current portfolio for:
- Long/short exposure balance
- Symbol concentration risk
- Margin utilization
- Maximum position sizes available
- Recommendations for position sizing

Call before opening new positions to ensure proper sizing.
No input required."""

    args_schema: Optional[type[BaseModel]] = None

    def _run(self) -> str:
        """Analyze portfolio risk."""
        context = self._context
        portfolio = context.get("portfolio", {})
        market = context.get("market", {})
        positions = portfolio.get("positions", [])

        equity = Decimal(str(portfolio.get("equity", 10000)))
        available_margin = Decimal(str(portfolio.get("available_margin", equity)))
        realized_pnl = Decimal(str(portfolio.get("realized_pnl", 0)))

        # Calculate exposures
        long_notional = Decimal(0)
        short_notional = Decimal(0)
        symbol_exposure: dict[str, Decimal] = {}
        position_details = []

        for pos in positions:
            size = Decimal(str(pos.get("size", 0)))
            entry_price = Decimal(str(pos.get("entry_price", 0)))
            notional = size * entry_price
            symbol = pos.get("symbol", "")
            side = pos.get("side", "")
            unrealized_pnl = Decimal(str(pos.get("unrealized_pnl", 0)))

            if side == "long":
                long_notional += notional
            else:
                short_notional += notional

            symbol_exposure[symbol] = symbol_exposure.get(symbol, Decimal(0)) + notional

            position_details.append({
                "symbol": symbol,
                "side": side,
                "notional": f"${float(notional):,.2f}",
                "pnl": f"${float(unrealized_pnl):+,.2f}",
                "pct_of_equity": f"{float(notional/equity*100):.1f}%",
            })

        # Concentration analysis
        max_symbol_exposure = max(symbol_exposure.values()) if symbol_exposure else Decimal(0)
        if max_symbol_exposure > equity * Decimal("0.3"):
            concentration_risk = "HIGH"
        elif max_symbol_exposure > equity * Decimal("0.2"):
            concentration_risk = "MEDIUM"
        else:
            concentration_risk = "LOW"

        # Calculate room for new positions
        max_position_pct = Decimal("0.25")  # 25% max per position
        max_position = equity * max_position_pct
        room_for_new = min(available_margin, max_position)

        # Net exposure
        net_exposure = long_notional - short_notional
        total_exposure = long_notional + short_notional
        net_exposure_pct = float(net_exposure / equity * 100) if equity else 0
        total_exposure_pct = float(total_exposure / equity * 100) if equity else 0

        # Margin utilization
        used_margin = equity - available_margin
        margin_utilization = float(used_margin / equity * 100) if equity else 0

        # Build recommendations
        recommendations = []

        if net_exposure_pct > 50:
            recommendations.append({
                "type": "HEAVY_LONG",
                "message": (
                    f"Net long exposure is {net_exposure_pct:+.1f}%. "
                    "Consider hedging with shorts or taking profits."
                ),
                "severity": "MEDIUM",
            })
        elif net_exposure_pct < -50:
            recommendations.append({
                "type": "HEAVY_SHORT",
                "message": (
                    f"Net short exposure is {net_exposure_pct:+.1f}%. "
                    "Consider covering shorts in uptrending market."
                ),
                "severity": "MEDIUM",
            })

        if concentration_risk == "HIGH":
            if symbol_exposure:
                max_symbol = max(symbol_exposure.items(), key=lambda x: x[1])[0]
            else:
                max_symbol = None
            if max_symbol:
                recommendations.append({
                    "type": "CONCENTRATED",
                    "message": f"High concentration in {max_symbol}. Diversify.",
                    "severity": "HIGH",
                })

        if margin_utilization > 80:
            recommendations.append({
                "type": "HIGH_MARGIN",
                "message": (
                    f"Margin utilization at {margin_utilization:.0f}%. "
                    "Close positions to free up capital."
                ),
                "severity": "HIGH",
            })
        elif margin_utilization > 60:
            recommendations.append({
                "type": "ELEVATED_MARGIN",
                "message": (
                    f"Margin utilization at {margin_utilization:.0f}%. "
                    "Be cautious with new positions."
                ),
                "severity": "MEDIUM",
            })

        if available_margin < equity * Decimal("0.2"):
            recommendations.append({
                "type": "LOW_MARGIN",
                "message": (
                    "Less than 20% margin available. "
                    "Prioritize closing positions over opening new ones."
                ),
                "severity": "HIGH",
            })

        # Suggested position sizes for each symbol
        suggested_sizes = {}
        for symbol, data in market.items():
            price = Decimal(str(data.get("price", 1)))
            if price > 0:
                # Conservative size based on available margin and risk
                risk_adjusted_margin = room_for_new * Decimal("0.8")  # Use 80% of available
                max_size = risk_adjusted_margin / price

                # Adjust based on portfolio state
                if margin_utilization > 50:
                    max_size *= Decimal("0.5")  # Half size if margin is stretched
                if concentration_risk == "HIGH" and symbol in symbol_exposure:
                    max_size *= Decimal("0.25")  # Quarter size if already concentrated

                suggested_sizes[symbol] = {
                    "max_size": float(max_size),
                    "conservative_size": float(max_size * Decimal("0.5")),
                    "reason": self._get_size_reason(symbol, symbol_exposure, concentration_risk),
                }

        analysis = {
            "portfolio_status": {
                "equity": f"${float(equity):,.2f}",
                "available_margin": f"${float(available_margin):,.2f}",
                "margin_utilization": f"{margin_utilization:.1f}%",
                "realized_pnl": f"${float(realized_pnl):+,.2f}",
            },
            "exposure": {
                "long": (
                    f"${float(long_notional):,.2f} "
                    f"({float(long_notional/equity*100):.1f}%)"
                ),
                "short": (
                    f"${float(short_notional):,.2f} "
                    f"({float(short_notional/equity*100):.1f}%)"
                ),
                "net": f"${float(net_exposure):,.2f} ({net_exposure_pct:+.1f}%)",
                "total": f"${float(total_exposure):,.2f} ({total_exposure_pct:.1f}%)",
            },
            "positions": position_details,
            "risk_assessment": {
                "concentration_risk": concentration_risk,
                "margin_risk": (
                    "HIGH" if margin_utilization > 80
                    else "MEDIUM" if margin_utilization > 50
                    else "LOW"
                ),
                "direction_bias": (
                    "LONG" if net_exposure_pct > 20
                    else "SHORT" if net_exposure_pct < -20
                    else "NEUTRAL"
                ),
            },
            "sizing_guidance": {
                "max_new_position": f"${float(room_for_new):,.2f}",
                "recommended_max_position": f"${float(room_for_new * Decimal('0.5')):,.2f}",
                "suggested_sizes": suggested_sizes,
            },
            "recommendations": recommendations or [{
                "type": "OK",
                "message": "Portfolio is well-balanced.",
                "severity": "INFO",
            }],
        }

        return json.dumps(analysis, indent=2)

    def _get_size_reason(
        self, symbol: str, symbol_exposure: dict[str, Decimal], concentration_risk: str
    ) -> str:
        """Get reason for suggested position size."""
        if symbol in symbol_exposure:
            if concentration_risk == "HIGH":
                return "Reduced due to existing position + high concentration"
            return "Reduced due to existing position"
        elif concentration_risk == "HIGH":
            return "Normal - helps diversify portfolio"
        return "Full size available"
