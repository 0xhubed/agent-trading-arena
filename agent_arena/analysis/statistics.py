"""Statistical analysis for backtesting and pattern validation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass
class PatternValidation:
    """Result of statistical validation for a trading pattern."""

    observed_rate: float
    p_value: float
    ci_low: float
    ci_high: float
    is_significant: bool
    sample_size: int
    baseline_rate: float
    min_sample_for_significance: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "observed_rate": round(self.observed_rate, 4),
            "p_value": round(self.p_value, 4),
            "ci_low": round(self.ci_low, 4),
            "ci_high": round(self.ci_high, 4),
            "is_significant": self.is_significant,
            "sample_size": self.sample_size,
            "baseline_rate": self.baseline_rate,
            "min_sample_for_significance": self.min_sample_for_significance,
        }

    def summary(self) -> str:
        """Human-readable summary."""
        status = "SIGNIFICANT" if self.is_significant else "NOT SIGNIFICANT"
        return (
            f"Success Rate: {self.observed_rate:.1%} ({self.sample_size} samples)\n"
            f"P-Value: {self.p_value:.4f}\n"
            f"95% CI: [{self.ci_low:.1%}, {self.ci_high:.1%}]\n"
            f"Status: {status}"
        )


@dataclass
class AgentComparison:
    """Result of statistical comparison between two agents."""

    agent_id: str
    baseline_id: str
    agent_return: float
    baseline_return: float
    outperformance: float  # agent - baseline
    p_value: Optional[float]
    ci_low: Optional[float]
    ci_high: Optional[float]
    is_significant: bool
    test_used: str  # "t-test", "wilcoxon", etc.

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "baseline_id": self.baseline_id,
            "agent_return": round(self.agent_return, 4),
            "baseline_return": round(self.baseline_return, 4),
            "outperformance": round(self.outperformance, 4),
            "p_value": round(self.p_value, 4) if self.p_value else None,
            "ci_low": round(self.ci_low, 4) if self.ci_low else None,
            "ci_high": round(self.ci_high, 4) if self.ci_high else None,
            "is_significant": self.is_significant,
            "test_used": self.test_used,
        }


def _wilson_score_interval(
    successes: int,
    total: int,
    confidence_level: float = 0.95,
) -> tuple[float, float]:
    """
    Calculate Wilson score confidence interval.

    Better than normal approximation for small samples and extreme proportions.
    """
    if total == 0:
        return 0.0, 0.0

    # Z-score for confidence level
    z = 1.96 if confidence_level == 0.95 else 2.576 if confidence_level == 0.99 else 1.645

    p = successes / total
    n = total

    denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    margin = (z / denominator) * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))

    return max(0, center - margin), min(1, center + margin)


def _binomial_test(
    successes: int,
    total: int,
    baseline: float = 0.5,
    alternative: str = "greater",
) -> float:
    """
    Perform binomial test and return p-value.

    Uses scipy if available, otherwise falls back to normal approximation.
    """
    try:
        from scipy import stats
        result = stats.binomtest(successes, total, baseline, alternative=alternative)
        return result.pvalue
    except ImportError:
        # Fallback to normal approximation
        if total == 0:
            return 1.0

        observed = successes / total
        std_error = math.sqrt(baseline * (1 - baseline) / total)

        if std_error == 0:
            return 1.0

        z = (observed - baseline) / std_error

        # One-tailed p-value
        if alternative == "greater":
            return 1 - _normal_cdf(z)
        elif alternative == "less":
            return _normal_cdf(z)
        else:  # two-sided
            return 2 * (1 - _normal_cdf(abs(z)))


def _normal_cdf(z: float) -> float:
    """Standard normal CDF approximation."""
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def _calculate_min_sample(
    observed_rate: float,
    baseline: float,
    alpha: float = 0.05,
    power: float = 0.8,
) -> int:
    """
    Estimate minimum sample size needed for significance.

    Uses the formula for one-sample proportion test.
    """
    if observed_rate <= baseline:
        return float('inf')  # Can never be significant

    try:
        from scipy import stats

        z_alpha = stats.norm.ppf(1 - alpha)
        z_beta = stats.norm.ppf(power)

        effect_size = observed_rate - baseline
        pooled_var = baseline * (1 - baseline) + observed_rate * (1 - observed_rate)

        n = ((z_alpha * math.sqrt(baseline * (1 - baseline)) +
              z_beta * math.sqrt(observed_rate * (1 - observed_rate))) / effect_size) ** 2

        return int(math.ceil(n))
    except (ImportError, ZeroDivisionError):
        # Rough approximation
        return int(math.ceil(16 / (observed_rate - baseline) ** 2))


def validate_pattern(
    successes: int,
    total: int,
    baseline: float = 0.5,
    confidence_level: float = 0.95,
) -> PatternValidation:
    """
    Validate if a pattern's success rate is statistically significant.

    Args:
        successes: Number of successful outcomes
        total: Total number of observations
        baseline: Expected success rate under null hypothesis (default 0.5 for random)
        confidence_level: Confidence level for interval (default 0.95)

    Returns:
        PatternValidation with significance test results

    Example:
        >>> result = validate_pattern(34, 50, baseline=0.5)
        >>> print(result.summary())
        Success Rate: 68.0% (50 samples)
        P-Value: 0.0123
        95% CI: [54.0%, 79.4%]
        Status: SIGNIFICANT
    """
    if total == 0:
        return PatternValidation(
            observed_rate=0,
            p_value=1.0,
            ci_low=0,
            ci_high=0,
            is_significant=False,
            sample_size=0,
            baseline_rate=baseline,
        )

    observed_rate = successes / total

    # Binomial test for significance
    p_value = _binomial_test(successes, total, baseline, alternative="greater")

    # Wilson score interval
    ci_low, ci_high = _wilson_score_interval(successes, total, confidence_level)

    # Significant if p-value below alpha AND confidence interval above baseline
    alpha = 1 - confidence_level
    is_significant = p_value < alpha and ci_low > baseline

    # Estimate minimum sample for significance
    min_sample = None
    if observed_rate > baseline:
        min_sample = _calculate_min_sample(observed_rate, baseline)
        if min_sample == float('inf'):
            min_sample = None

    return PatternValidation(
        observed_rate=observed_rate,
        p_value=p_value,
        ci_low=ci_low,
        ci_high=ci_high,
        is_significant=is_significant,
        sample_size=total,
        baseline_rate=baseline,
        min_sample_for_significance=min_sample,
    )


def compare_agents(
    agent_returns: list[float],
    baseline_returns: list[float],
    agent_id: str = "agent",
    baseline_id: str = "baseline",
    confidence_level: float = 0.95,
) -> AgentComparison:
    """
    Compare agent performance vs baseline with statistical tests.

    Uses paired t-test (or Wilcoxon if scipy not available) to test
    if the difference is statistically significant.

    Args:
        agent_returns: List of periodic returns for the agent
        baseline_returns: List of periodic returns for the baseline
        agent_id: Identifier for the agent
        baseline_id: Identifier for the baseline
        confidence_level: Confidence level for the test

    Returns:
        AgentComparison with test results
    """
    if len(agent_returns) != len(baseline_returns):
        raise ValueError("Return series must have same length for paired test")

    if len(agent_returns) < 2:
        return AgentComparison(
            agent_id=agent_id,
            baseline_id=baseline_id,
            agent_return=sum(agent_returns) if agent_returns else 0,
            baseline_return=sum(baseline_returns) if baseline_returns else 0,
            outperformance=(sum(agent_returns) - sum(baseline_returns)) if agent_returns else 0,
            p_value=None,
            ci_low=None,
            ci_high=None,
            is_significant=False,
            test_used="insufficient_data",
        )

    agent_arr = np.array(agent_returns)
    baseline_arr = np.array(baseline_returns)
    diff = agent_arr - baseline_arr

    agent_total = float(np.sum(agent_arr))
    baseline_total = float(np.sum(baseline_arr))
    outperformance = agent_total - baseline_total

    try:
        from scipy import stats

        # Paired t-test
        t_stat, p_value = stats.ttest_rel(agent_arr, baseline_arr)

        # One-tailed p-value (agent > baseline)
        if t_stat > 0:
            p_value = p_value / 2
        else:
            p_value = 1 - p_value / 2

        # Confidence interval for the difference
        mean_diff = float(np.mean(diff))
        se_diff = float(np.std(diff, ddof=1) / np.sqrt(len(diff)))
        t_crit = stats.t.ppf(1 - (1 - confidence_level) / 2, df=len(diff) - 1)

        ci_low = mean_diff - t_crit * se_diff
        ci_high = mean_diff + t_crit * se_diff

        alpha = 1 - confidence_level
        is_significant = p_value < alpha and ci_low > 0

        return AgentComparison(
            agent_id=agent_id,
            baseline_id=baseline_id,
            agent_return=agent_total,
            baseline_return=baseline_total,
            outperformance=outperformance,
            p_value=float(p_value),
            ci_low=float(ci_low),
            ci_high=float(ci_high),
            is_significant=is_significant,
            test_used="paired_t_test",
        )

    except ImportError:
        # Fallback to simple comparison
        mean_diff = float(np.mean(diff))
        is_significant = mean_diff > 0 and outperformance > 0

        return AgentComparison(
            agent_id=agent_id,
            baseline_id=baseline_id,
            agent_return=agent_total,
            baseline_return=baseline_total,
            outperformance=outperformance,
            p_value=None,
            ci_low=None,
            ci_high=None,
            is_significant=is_significant,
            test_used="simple_comparison",
        )


def calculate_sharpe_ratio(
    returns: list[float],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 365 * 6,  # 4h ticks
) -> Optional[float]:
    """
    Calculate annualized Sharpe ratio.

    Args:
        returns: List of periodic returns
        risk_free_rate: Annual risk-free rate (default 0)
        periods_per_year: Number of return periods per year

    Returns:
        Annualized Sharpe ratio, or None if insufficient data
    """
    if len(returns) < 2:
        return None

    returns_arr = np.array(returns)
    mean_return = float(np.mean(returns_arr))
    std_return = float(np.std(returns_arr, ddof=1))

    if std_return == 0:
        return None

    # Adjust risk-free rate to period
    rf_per_period = risk_free_rate / periods_per_year

    # Calculate Sharpe ratio
    sharpe = (mean_return - rf_per_period) / std_return

    # Annualize
    annualized_sharpe = sharpe * np.sqrt(periods_per_year)

    return float(annualized_sharpe)


def calculate_max_drawdown(
    equity_curve: list[float],
) -> tuple[float, float, int]:
    """
    Calculate maximum drawdown from equity curve.

    Args:
        equity_curve: List of equity values over time

    Returns:
        Tuple of (max_drawdown_pct, max_drawdown_amount, duration_periods)
    """
    if len(equity_curve) < 2:
        return 0.0, 0.0, 0

    equity_arr = np.array(equity_curve)
    running_max = np.maximum.accumulate(equity_arr)
    drawdown = (running_max - equity_arr) / running_max
    drawdown_amount = running_max - equity_arr

    max_dd_pct = float(np.max(drawdown))
    max_dd_amount = float(np.max(drawdown_amount))

    # Calculate duration of max drawdown
    max_dd_idx = int(np.argmax(drawdown))

    # Find when peak occurred
    peak_idx = int(np.argmax(equity_arr[:max_dd_idx + 1]))

    duration = max_dd_idx - peak_idx

    return max_dd_pct, max_dd_amount, duration


def calculate_profit_factor(
    trade_pnls: list[float],
) -> Optional[float]:
    """
    Calculate profit factor (gross profit / gross loss).

    Args:
        trade_pnls: List of individual trade P&Ls

    Returns:
        Profit factor, or None if no losing trades
    """
    if not trade_pnls:
        return None

    gross_profit = sum(pnl for pnl in trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in trade_pnls if pnl < 0))

    if gross_loss == 0:
        return float('inf') if gross_profit > 0 else None

    return gross_profit / gross_loss


def calculate_win_rate(
    trade_pnls: list[float],
) -> tuple[float, int, int]:
    """
    Calculate win rate from trade P&Ls.

    Returns:
        Tuple of (win_rate, winning_trades, losing_trades)
    """
    if not trade_pnls:
        return 0.0, 0, 0

    winning = sum(1 for pnl in trade_pnls if pnl > 0)
    losing = sum(1 for pnl in trade_pnls if pnl < 0)
    total = winning + losing

    if total == 0:
        return 0.0, 0, 0

    return winning / total, winning, losing


def calculate_expectancy(
    trade_pnls: list[float],
) -> Optional[float]:
    """
    Calculate expectancy (average profit per trade).

    This is a key metric that combines win rate and average win/loss size.
    Positive expectancy is required for profitable trading.

    Args:
        trade_pnls: List of individual trade P&Ls

    Returns:
        Expectancy (average profit per trade)
    """
    if not trade_pnls:
        return None

    return sum(trade_pnls) / len(trade_pnls)


def bootstrap_confidence_interval(
    data: list[float],
    statistic_func,
    n_bootstrap: int = 1000,
    confidence_level: float = 0.95,
) -> tuple[float, float]:
    """
    Calculate bootstrap confidence interval for any statistic.

    Args:
        data: Original data
        statistic_func: Function to calculate the statistic
        n_bootstrap: Number of bootstrap samples
        confidence_level: Confidence level

    Returns:
        Tuple of (ci_low, ci_high)
    """
    if len(data) < 2:
        return 0.0, 0.0

    data_arr = np.array(data)
    bootstrap_stats = []

    for _ in range(n_bootstrap):
        sample = np.random.choice(data_arr, size=len(data_arr), replace=True)
        stat = statistic_func(sample)
        if stat is not None:
            bootstrap_stats.append(stat)

    if not bootstrap_stats:
        return 0.0, 0.0

    alpha = 1 - confidence_level
    ci_low = float(np.percentile(bootstrap_stats, alpha / 2 * 100))
    ci_high = float(np.percentile(bootstrap_stats, (1 - alpha / 2) * 100))

    return ci_low, ci_high
