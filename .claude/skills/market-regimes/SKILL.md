---
name: market-regimes
description: Market regime detection and regime-specific trading strategies. Use when analyzing market conditions to select appropriate strategy.
---

# Market Regimes

> Last updated: 2026-03-09 20:08 UTC
> Active patterns: 38
> Total samples: 0
> Confidence threshold: 60%

## How to Use This Skill

1. Identify the current market regime using price action and volatility
2. Look up the recommended strategy for that regime below
3. Adjust your trading approach accordingly
4. Monitor for regime changes

## Regime Strategies

### Uniform Bearish

**Recommended approach** (95% confidence, seen 1x):
> HIGH-frequency SHORT-biased strategy (150-200 trades/24h) with multi-timeframe bearish confirmation. skill_aware_oss achieved +$2911.52 with 200 trades. Focus on highest-volatility losers: SOLUSDT (-11.31%), ETHUSDT (-9.62%).
- Total observations: 0
- First identified: 2026-02-01

### Uniform Bearish

**Recommended approach** (92% confidence, seen 1x):
> Avoid LONG positions entirely regardless of oversold signals. RSI ~28 oversold readings are continuation signals in strong downtrends, not reversal signals. skill_aware_oss correctly used oversold RSI as SHORT confirmation.
- Total observations: 0
- First identified: 2026-02-01

### Uniform Bullish

**Recommended approach** (92% confidence, seen 1x):
> ZERO trading or passive LONG-only allocation. When ALL assets gain (+1.33% to +3.82%), any SHORT-biased strategy loses. index_fund's $0 PnL beats all active agents. If must trade, use <20 trades/24h with LONG bias only.
- Total observations: 0
- First identified: 2026-02-02

### Uniform Bearish

**Recommended approach** (90% confidence, seen 1x):
> Trade frequency sweet spot is 149-200 trades/24h. Below 20 trades (ta_baseline: -$99, gpt_simple: -$163) fails to capture momentum. Zero trades (index_fund) misses directional opportunity entirely.
- Total observations: 0
- First identified: 2026-02-01

### Uniform Bullish

**Recommended approach** (90% confidence, seen 1x):
> CRITICAL: Multi-timeframe bearish alignment on lower timeframes (15m, 1h, 4h) does NOT override daily/weekly bullish regime. Agents must incorporate higher timeframe trend before acting on short-term signals.
- Total observations: 0
- First identified: 2026-02-02

### Uniform Bearish

**Recommended approach** (88% confidence, seen 1x):
> Position sizing at 2% equity risk with 2:1 reward ratio SUCCEEDS when combined with trend-aligned entries. skill_aware_oss consistently cited this framework in high-confidence decisions.
- Total observations: 0
- First identified: 2026-02-01

### Uniform Bullish

**Recommended approach** (88% confidence, seen 1x):
> Technical analysis baseline with LOW frequency (38 trades/24h) minimizes losses when agent bias conflicts with market direction. ta_baseline lost only -$120 vs -$963 for high-frequency SHORT agents.
- Total observations: 0
- First identified: 2026-02-02

### Uniform Bearish

**Recommended approach** (87% confidence, seen 1x):
> SHORT-biased strategy with 150-183 trades/24h is optimal IF position management focuses on 'locking in profits' rather than 'cutting losses to free margin'. skill_aware_oss: +$1682.05 with 183 trades. Focus shorts on highest-volatility losers: ETHUSDT (-16.34%), DOGEUSDT (-15.79%), SOLUSDT (-14.28%).
- Total observations: 0
- First identified: 2026-01-31

### Uniform Bearish

**Recommended approach** (85% confidence, seen 1x):
> Avoid LONG positions entirely. agentic_gptoss opened BTC long with 0.93 confidence during -7.95% BTC decline, contributing to -$246.40 loss. Market indicators showing 'bullish bias' are false signals in uniform bearish conditions.
- Total observations: 0
- First identified: 2026-01-31

### Mixed Mostly Bullish

**Recommended approach** (83% confidence, seen 1x):
> SHORT-biased strategy ONLY works when agent has strong conviction and holds through noise. skill_aware_oss succeeded (+$409) while agentic_gptoss failed (-$619) with similar trade counts. Key differentiator: skill_aware_oss maintained position discipline.
- Total observations: 0
- First identified: 2026-01-31

### Uniform Bearish

**Recommended approach** (83% confidence, seen 1x):
> index_fund's $0 PnL with 0 trades is suboptimal when all assets decline 7-16%. Any SHORT exposure would have been profitable. Passive long-only allocation fails in bearish regimes.
- Total observations: 0
- First identified: 2026-01-31

### Uniform Bearish

**Recommended approach** (80% confidence, seen 1x):
> SHORT-biased strategy with 45-55 trades/24h. Use RSI oversold + bearish MACD + price below SMA as entry confirmation. skill_aware_oss achieved +$691.44 vs index_fund $0.
- Total observations: 0
- First identified: 2026-01-30

### Uniform Bullish

**Recommended approach** (80% confidence, seen 1x):
> Focus LONG positions on highest momentum assets: DOGEUSDT (+3.82%) and SOLUSDT (+3.38%) outperformed BTC/ETH. Momentum leaders in bull markets offer best risk/reward.
- Total observations: 0
- First identified: 2026-02-02

### Mixed Mostly Bullish

**Recommended approach** (80% confidence, seen 1x):
> Focus shorts on the ONE bearish asset (ETHUSDT -0.99%) rather than fighting bullish momentum on BTC/SOL. Agents shorting BTC (+1.57%) and SOL (+3.90%) suffered.
- Total observations: 0
- First identified: 2026-01-31

### Uniform Bearish

**Recommended approach** (80% confidence, seen 1x):
> Low trade frequency (20 trades) with simple technical signals (SMA crossover, ROE targets) provides consistent positive returns. ta_baseline: +$20.54. Avoid complex multi-timeframe analysis that creates 'conflicting signals' in clearly directional markets.
- Total observations: 0
- First identified: 2026-01-31

### Uniform Bearish

**Recommended approach** (78% confidence, seen 1x):
> Avoid LONG positions entirely. index_fund's long-only allocation with 1.0 confidence failed to generate returns despite 'conviction'. Market direction trumps allocation strategy.
- Total observations: 0
- First identified: 2026-01-30

### Mixed Mostly Bullish

**Recommended approach** (77% confidence, seen 1x):
> Trade frequency sweet spot is 36-172 trades/24h. Below 36 (gpt_simple: 7) limits upside. Above 165 with poor discipline (agentic_gptoss) creates excessive churn.
- Total observations: 0
- First identified: 2026-01-31

### Uniform Bearish

**Recommended approach** (75% confidence, seen 1x):
> Trade frequency sweet spot is 48-63 trades/24h. Below 10 trades misses opportunities (gpt_simple -$73.60, ta_baseline $0). Above 60 trades shows diminishing returns (agentic_gptoss only +$79.87 with 63 trades).
- Total observations: 0
- First identified: 2026-01-30

### Mixed Mostly Bullish

**Recommended approach** (75% confidence, seen 1x):
> index_fund's $0 PnL with 0 trades is suboptimal when 4/5 assets are positive. Passive long exposure would have captured +1.45% average gain.
- Total observations: 0
- First identified: 2026-01-31

### Uniform Bearish Mild

**Recommended approach** (75% confidence, seen 1x):
> Market was uniformly bearish but with mild declines (BNB -1.50%, BTC -1.18%, ETH -0.78%, SOL -2.53%, DOGE -0.81%). Best performers used moderate-frequency SHORT-biased trading (31-32 trades) with selective entries. Forum-aware sentiment filtering and agentic multi-step reasoning both profited. Momentum and contrarian strategies lost money. Zero trading preserved capital perfectly.
- Total observations: 0
- First identified: 2026-03-07

### Mixed Low Volatility

**Recommended approach** (75% confidence, seen 1x):
> Market was mixed with small daily moves (BNB -0.23%, BTC +0.30%, ETH +0.36%, SOL -0.30%, DOGE -0.48%). Best approach: selective moderate-frequency trading with sentiment/forum filtering (24-31 trades). Forum-aware's +$232.17 dominated. Avoid momentum-following and contrarian strategies which both lost money. Zero trading is safe but suboptimal when forum-aware can extract alpha.
- Total observations: 0
- First identified: 2026-03-08

### Uniform Bullish Moderate

**Recommended approach** (75% confidence, seen 1x):
> Self-reflective position management (journal-aware) is the ONLY approach that profited among active agents. Meta-cognitive exits ('we are overtrading', 'close to reduce exposure') preserved gains and limited losses from wrong-direction entries.
- Total observations: 0
- First identified: 2026-03-09

### Uniform Bearish Moderate

**Recommended approach** (73% confidence, seen 1x):
> Avoid LONG-biased strategies entirely. Momentum (+8 trades, -$34.40) and contrarian (+9 trades, -$15.04) strategies that likely entered LONGs all lost money. Even low-frequency LONG entries (qwen35_simple, 4 trades, -$99.44) suffered large losses.
- Total observations: 0
- First identified: 2026-03-06

### Uniform Bearish Mild

**Recommended approach** (73% confidence, seen 1x):
> SOL was the worst performer (-2.53%) and BNB second worst (-1.50%), making them the best SHORT targets. ETH had the smallest decline (-0.78%), making it the worst SHORT target. Focus SHORT entries on highest-beta assets.
- Total observations: 0
- First identified: 2026-03-07

### Uniform Bearish Mild

**Recommended approach** (70% confidence, seen 1x):
> Trade frequency sweet spot in this mild bearish window was 31-32 trades/24h. Below that (11 trades contrarian) still lost money due to wrong direction. Above that is not represented. The key differentiator was directional alignment (SHORT bias) combined with selective entry filtering.
- Total observations: 0
- First identified: 2026-03-07

### Mixed Low Volatility

**Recommended approach** (70% confidence, seen 1x):
> In low-volatility mixed markets, trade frequency sweet spot is 24-31 trades/24h. Below that (2-16 trades) either breaks even or loses small amounts. Above that (33 trades for momentum) loses more. The key differentiator is signal quality, not frequency.
- Total observations: 0
- First identified: 2026-03-08

### Uniform Bearish

**Recommended approach** (65% confidence, seen 1x):
> Focus shorts on highest-volatility losers: ETHUSDT (-2.31%) offered best short opportunity vs SOLUSDT (-1.31%). Volatility creates profit potential.
- Total observations: 0
- First identified: 2026-01-30

### Uniform Bearish Moderate

**Recommended approach** (65% confidence, seen 1x):
> HIGH-frequency SHORT-biased trading with self-reflective position management (30-48 trades/24h). Journal-aware's 48 trades (+$296.01) and agentic's 30 trades (+$193.68) both profited by aligning with the bearish trend. Self-reflective exits helped capture profits and limit drawdowns. This CONTRADICTS prior patterns suggesting ultra-low frequency is optimal in bearish markets — when agents correctly identify the bearish regime, higher frequency with SHORT bias is profitable.
- Total observations: 0
- First identified: 2026-03-06

### Uniform Bearish Moderate

**Recommended approach** (60% confidence, seen 1x):
> Focus SHORT entries on highest-beta assets. ETHUSDT (-4.72%) and SOLUSDT (-4.40%) had the largest declines, offering the most profit potential. BTCUSDT (-4.29%) was also a strong SHORT candidate. BNBUSDT (-2.99%) had the smallest decline.
- Total observations: 0
- First identified: 2026-03-06

### Uniform Bearish Moderate

**Recommended approach** (55% confidence, seen 1x):
> Ultra-low frequency trading (13-16 trades/24h) with simple strategies. Market was uniformly bearish (BNB -1.70%, BTC -3.47%, ETH -4.63%, SOL -4.70%, DOGE -8.71%). Only qwen35 simple models profited. Zero trading also preserved capital. Avoid high-frequency trading (27-62 trades) which amplified losses. DOGE had the largest decline (-8.71%), making it the highest-beta SHORT target.
- Total observations: 0
- First identified: 2026-03-05

### Uniform Bearish Moderate

**Recommended approach** (52% confidence, seen 1x):
> If trading actively, cap at 16 trades/24h maximum. qwen35_122b_simple's 16 trades (+$50.27) was the sweet spot. Above 20 trades, all agents lost money. The relationship: 0 trades = $0, 13-16 trades = +$35-50, 14 trades (ta_bot) = -$64, 24 trades = -$6, 27 trades = -$229, 62 trades = -$71.
- Total observations: 0
- First identified: 2026-03-05

### Uniform Bearish Moderate

**Recommended approach** (48% confidence, seen 1x):
> Focus SHORT entries on highest-beta assets. DOGE (-8.71%) had nearly 5x the decline of BNB (-1.70%). SOL (-4.70%) and ETH (-4.63%) were also strong SHORT candidates. BNB was the most resilient, suggesting it should be avoided for SHORT entries.
- Total observations: 0
- First identified: 2026-03-05

### Uniform Bullish

**Recommended approach** (47% confidence, seen 1x):
> Passive index fund allocation or zero trading. When ALL assets are positive (BNB +1.23%, BTC +1.49%, ETH +1.08%, SOL +0.22%, DOGE +0.26%), active trading destroys value through transaction costs. index_fund achieved $0.00 vs -$134.51 for most active trader
- Total observations: 0
- First identified: 2026-01-28

### Uniform Bullish

**Recommended approach** (45% confidence, seen 1x):
> If trading, use extremely low frequency (<10 trades/24h) and hold positions longer. The 8 trades from index_fund vs 166 from agentic_gptoss shows 20x frequency difference with $134.51 performance gap
- Total observations: 0
- First identified: 2026-01-28

### Uniform Bullish Strong

**Recommended approach** (45% confidence, seen 1x):
> ZERO shorting rule. When ALL 5 assets gain >5% in 24h, any short position is catastrophic. This single rule would have prevented ~$838 in combined losses from gptoss_agentic and gptoss_forum_aware.
- Total observations: 0
- First identified: 2026-03-04

### Mixed Slightly Bullish

**Recommended approach** (43% confidence, seen 1x):
> Technical analysis baseline with moderate frequency (30-40 trades/24h) outperforms LLM-based multi-timeframe analysis. ta_baseline lost only $45.95 vs $134.51 for agentic_gptoss
- Total observations: 0
- First identified: 2026-01-28

### Uniform Bullish Moderate

**Recommended approach** (15% confidence, seen 2x):
> Trade frequency inversely correlates with losses when directional bias is wrong: 0 trades = $0, 6 trades = -$22.72, 15 trades = -$25.04, 27 trades = -$86 to +$55, 30-31 trades = -$97 to -$341. Each additional wrong-direction trade costs approximately $3-11.
- Total observations: 0
- First identified: 2026-03-02

### Uniform Bullish Moderate

**Recommended approach** (13% confidence, seen 2x):
> Zero trading or ultra-low frequency LONG-only entries. Market was uniformly bullish (BNB +3.31%, BTC +2.68%, ETH +3.90%, SOL +5.07%, DOGE +2.46%). Only journal-aware with self-reflective management profited (+$55.48). All other active agents lost money, suggesting most were SHORT-biased. SOL was the best performer (+5.07%), making it the top LONG target.
- Total observations: 0
- First identified: 2026-03-02

---

## Confidence Guide

| Confidence | Interpretation |
|------------|----------------|
| 90%+ | High confidence - strong historical support |
| 70-90% | Moderate confidence - use with other signals |
| 60-70% | Low confidence - consider as one input |
| <60% | Experimental - needs more data |

*This skill is automatically generated and updated by the Observer Agent.*
