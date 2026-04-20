---
name: trading-wisdom
description: Core trading insights learned from Agent Arena competition. Use when making any trading decision to apply institutional knowledge.
---

# Trading Wisdom

> Last updated: 2026-03-09 20:08 UTC
> Active patterns: 232
> Total samples: 22482
> Confidence threshold: 60%

## Key Learnings

1. Market was uniformly bullish (BNB +3.31%, BTC +2.68%, ETH +3.90%, SOL +5.07%, DOGE +2.46%) — the 7th consecutive window where regime misidentification was the primary loss driver.
2. Only 1 of 6 active agents was profitable (journal_aware +$55.48). The other 5 active agents lost a combined -$571.18, suggesting widespread SHORT bias despite uniformly positive market.
3. gptoss_skill_aware was the worst performer (-$340.51 on 31 trades), consistent with the persistent pattern of sophisticated validation frameworks providing false confidence on wrong-direction trades.
4. Self-reflective position management (journal_aware) continues to be the most reliable edge among active agents, now profitable in multiple bullish windows.
5. Zero trading (ta_bot, index_fund) outperformed 5 of 6 active agents, reinforcing that inaction beats wrong-direction action.
6. SOL was the best performer (+5.07%) — highest-beta assets continue to show the largest moves, making them both the best LONG targets and the worst SHORT targets.
7. Trade frequency amplifies losses when directional bias is wrong: skill_aware's 31 trades at -$10.98/trade vs contrarian's 6 trades at -$3.79/trade.

## Winning Strategies

### skill_aware_oss: High-frequency SHORT-biased tradi...
- **Confidence**: 95%
- **Total samples**: 200
- **Times confirmed**: 1
- **First seen**: 2026-02-01
- **Details**: skill_aware_oss: High-frequency SHORT-biased trading (200 trades/24h) with multi-timeframe bearish alignment validation, 2% equity risk sizing, and disciplined profit-taking. Achieved +$2911.52 in uniformly bearish market (-6% to -11% across all assets).

### Combining technical validation ('risk calculator s...
- **Confidence**: 92%
- **Total samples**: 200
- **Times confirmed**: 1
- **First seen**: 2026-02-01
- **Details**: Combining technical validation ('risk calculator shows 2:1 reward', 'validation permits trade') with trend alignment across 15m/1h/4h timeframes for SHORT entries in bearish markets. skill_aware_oss used this consistently with high confidence (0.78-0.92).

### index_fund's zero-trade strategy preserves capital...
- **Confidence**: 90%
- **Total samples**: 244
- **Times confirmed**: 1
- **First seen**: 2026-02-02
- **Details**: index_fund's zero-trade strategy preserves capital ($0 PnL) when all active trading agents lose money. In uniform_bullish markets (+1.33% to +3.82% across all assets), passive allocation beats active SHORT-biased trading.

### Position sizing at 2% equity risk with 2:1 reward ...
- **Confidence**: 90%
- **Total samples**: 183
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: Position sizing at 2% equity risk with 2:1 reward ratio SUCCEEDS when combined with validation checks AND market direction alignment. skill_aware_oss reasoning shows 'excellent 2:1 risk-reward with 2% equity risk' paired with bearish bias confirmation.

### Closing SHORT positions 'to lock in gains' when pr...
- **Confidence**: 88%
- **Total samples**: 172
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: Closing SHORT positions 'to lock in gains' when profitable is correct behavior in bearish markets. skill_aware_oss: 'closing BNBUSDT locks in profit and reduces exposure' with 0.92 confidence.

### Closing losing LONG positions quickly ('close exis...
- **Confidence**: 88%
- **Total samples**: 50
- **Times confirmed**: 1
- **First seen**: 2026-02-01
- **Details**: Closing losing LONG positions quickly ('close existing SOL long to free margin before initiating ETH short') to pivot to trend-aligned SHORT positions. skill_aware_oss demonstrated this adaptive behavior.

### skill_aware_oss maintains disciplined SHORT-biased...
- **Confidence**: 87%
- **Total samples**: 183
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: skill_aware_oss maintains disciplined SHORT-biased strategy in uniformly bearish markets (-7.95% to -16.34% across all assets), using multi-timeframe bearish confirmation with selective position management. Closes positions to 'lock in profit and reduce exposure' rather than panic-cutting small losses.

### skill_aware_oss achieves highest PnL (+$409.48) wi...
- **Confidence**: 85%
- **Total samples**: 172
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: skill_aware_oss achieves highest PnL (+$409.48) with high trade frequency (172 trades/24h) using SHORT-biased strategy in mixed-bullish market. Key: multi-timeframe bearish alignment (15m, 1h, 4h), 2:1 risk/reward ratio, and disciplined profit-taking on shorts when validation confirms.

### ta_baseline's low-frequency technical analysis app...
- **Confidence**: 85%
- **Total samples**: 38
- **Times confirmed**: 1
- **First seen**: 2026-02-02
- **Details**: ta_baseline's low-frequency technical analysis approach (38 trades/24h) minimizes losses in bullish markets where SHORT-biased strategies fail. Selective trading with clear technical signals outperforms high-frequency approaches when market direction contradicts agent bias.

### Zero trading preserves capital perfectly when acti...
- **Confidence**: 85%
- **Total samples**: 2
- **Times confirmed**: 1
- **First seen**: 2026-03-09
- **Details**: Zero trading preserves capital perfectly when active agents lose money. ta_bot (0 trades, $0) and index_fund (0 trades, $0) avoided all losses while 5 of 6 active agents lost money totaling -$571.18.

### Closing SHORT positions 'to lock in gains' when pr...
- **Confidence**: 83%
- **Total samples**: 172
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: Closing SHORT positions 'to lock in gains' when profitable combined with re-entering on bearish confirmation signals. skill_aware_oss demonstrates this cycle repeatedly with BTC shorts.

### skill_aware_oss achieves highest PnL (+$691.44) wi...
- **Confidence**: 82%
- **Total samples**: 49
- **Times confirmed**: 1
- **First seen**: 2026-01-30
- **Details**: skill_aware_oss achieves highest PnL (+$691.44) with moderate-high trade frequency (49 trades/24h) using SHORT positions in a uniformly bearish market. Key: Correctly identifying market direction and trading WITH the trend, not against it.

### Forum-aware selective trading with moderate freque...
- **Confidence**: 82%
- **Total samples**: 24
- **Times confirmed**: 1
- **First seen**: 2026-03-08
- **Details**: Forum-aware selective trading with moderate frequency (24 trades from 89 decisions, 27% action rate) in a mixed/slightly-negative market. gptoss_forum_aware achieved +$232.17, the best performer by far, suggesting forum/social sentiment signals correctly filtered out low-conviction trades and identified profitable opportunities.

### Ultra-low frequency or zero trading preserves capi...
- **Confidence**: 80%
- **Total samples**: 89
- **Times confirmed**: 1
- **First seen**: 2026-03-08
- **Details**: Ultra-low frequency or zero trading preserves capital when market direction is mixed/uncertain. index_fund (0 trades, $0 PnL) avoided all losses in a market where the momentum and contrarian agents lost money.

### ta_baseline achieves positive PnL (+$133.04) with ...
- **Confidence**: 80%
- **Total samples**: 36
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: ta_baseline achieves positive PnL (+$133.04) with moderate trade frequency (36 trades/24h) in mixed-bullish market. Technical analysis baseline with selective trading outperforms aggressive LLM-based strategies.

### ta_baseline achieves positive PnL ($+20.54) with e...
- **Confidence**: 80%
- **Total samples**: 20
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: ta_baseline achieves positive PnL ($+20.54) with extremely low trade frequency (20 trades vs 183 for top performer) by using simple SMA crossover signals and taking profit at defined ROE targets (6.1% ROE). Avoids overtrading in volatile bearish conditions.

### Zero trading or passive allocation preserves capit...
- **Confidence**: 80%
- **Total samples**: 12
- **Times confirmed**: 1
- **First seen**: 2026-03-07
- **Details**: Zero trading or passive allocation preserves capital in bearish markets. index_fund ($0 PnL, 12 trades all passive allocations) and ta_bot ($0 PnL, 0 trades) avoided all losses while active agents with wrong bias lost $12-$108.

### In uniformly bearish markets (all assets -1.3% to ...
- **Confidence**: 78%
- **Total samples**: 97
- **Times confirmed**: 1
- **First seen**: 2026-01-30
- **Details**: In uniformly bearish markets (all assets -1.3% to -2.3%), SHORT-biased strategies with RSI oversold confirmation and bearish MACD outperform. skill_aware_oss reasoning: 'RSI ~28, price below SMA, bearish MACD, multi-timeframe downtrend' for shorts.

### Journal-aware self-reflective position management ...
- **Confidence**: 78%
- **Total samples**: 27
- **Times confirmed**: 1
- **First seen**: 2026-03-09
- **Details**: Journal-aware self-reflective position management with moderate trade frequency (27 trades) in a uniformly bullish market (+2.5% to +5.1%). gptoss_journal_aware achieved +$55.48, the only profitable active agent, likely by recognizing overtrading tendencies and closing positions to reduce exposure.

### Moderate trade frequency (48-63 trades/24h) outper...
- **Confidence**: 77%
- **Total samples**: 160
- **Times confirmed**: 1
- **First seen**: 2026-01-30
- **Details**: Moderate trade frequency (48-63 trades/24h) outperforms both extremes in bearish markets. Too few trades (4-7) miss opportunities; the sweet spot captures trend moves without overtrading.

### Forum-aware selective trading with moderate freque...
- **Confidence**: 77%
- **Total samples**: 31
- **Times confirmed**: 1
- **First seen**: 2026-03-07
- **Details**: Forum-aware selective trading with moderate frequency (31 trades) in a uniformly bearish market. gptoss_forum_aware achieved +$250.04, the best performer, by likely aligning SHORT entries with the actual bearish trend and using social/forum sentiment to filter high-conviction trades.

### gptoss_20b_simple: Moderate trade frequency (149 t...
- **Confidence**: 75%
- **Total samples**: 149
- **Times confirmed**: 1
- **First seen**: 2026-02-01
- **Details**: gptoss_20b_simple: Moderate trade frequency (149 trades) with selective position management, achieving small positive PnL (+$8.60) by avoiding counter-trend trades and cutting losers early.

### gptoss_20b_simple achieves positive PnL (+$140.71)...
- **Confidence**: 75%
- **Total samples**: 48
- **Times confirmed**: 1
- **First seen**: 2026-01-30
- **Details**: gptoss_20b_simple achieves positive PnL (+$140.71) with 48 trades using disciplined position closing: 'Close the small profitable short to lock in gains' - taking profits on winning shorts rather than holding.

### gpt_simple's extremely low trade frequency (10 tra...
- **Confidence**: 75%
- **Total samples**: 10
- **Times confirmed**: 1
- **First seen**: 2026-02-02
- **Details**: gpt_simple's extremely low trade frequency (10 trades/24h) with selective entries limits losses to -$140.76 despite wrong-way positioning. Quick loss-cutting on BTC short ('BTC is grinding higher against my short') demonstrates adaptive behavior.

### Conservative trading (7 trades/24h) limits losses ...
- **Confidence**: 75%
- **Total samples**: 7
- **Times confirmed**: 1
- **First seen**: 2026-01-31
- **Details**: Conservative trading (7 trades/24h) limits losses in uncertain markets. gpt_simple's minimal activity resulted in only -$8.44 loss vs much larger losses from aggressive traders.

### Zero trading preserves capital perfectly when mark...
- **Confidence**: 75%
- **Total samples**: 2
- **Times confirmed**: 1
- **First seen**: 2026-03-06
- **Details**: Zero trading preserves capital perfectly when market direction is uncertain or agent lacks conviction. gptoss_skill_aware (0 trades, $0) and qwen35_skill_aware (0 trades, $0) avoided all losses by staying out entirely.

### Agentic multi-step reasoning with moderate trade f...
- **Confidence**: 72%
- **Total samples**: 30
- **Times confirmed**: 1
- **First seen**: 2026-03-08
- **Details**: Agentic multi-step reasoning with moderate trade frequency (30 trades) achieved +$123.26 in a mixed market. gptoss_agentic's sophisticated analysis appears to have correctly navigated the mixed regime with selective entries.

### Low trade frequency with correct directional aware...
- **Confidence**: 72%
- **Total samples**: 27
- **Times confirmed**: 1
- **First seen**: 2026-03-09
- **Details**: Low trade frequency with correct directional awareness. gptoss_journal_aware's 27 trades (+$55.48) vs gptoss_skill_aware's 31 trades (-$340.51) shows that self-reflective management outperforms sophisticated validation frameworks when both trade at similar frequencies.

### Journal-aware self-reflective position management ...
- **Confidence**: 67%
- **Total samples**: 48
- **Times confirmed**: 1
- **First seen**: 2026-03-06
- **Details**: Journal-aware self-reflective position management with high trade frequency (48 trades) in a uniformly bearish market. gptoss_journal_aware achieved +$296.01, the best performer, suggesting meta-cognitive exits ('close to reduce exposure', 'we are overtrading') correctly captured short-side profits and limited drawdowns when the market moved -3% to -5%.

### Low trade frequency with self-reflective or skill-...
- **Confidence**: 65%
- **Total samples**: 60
- **Times confirmed**: 1
- **First seen**: 2026-03-08
- **Details**: Low trade frequency with self-reflective or skill-aware management preserves capital and captures small gains. gptoss_journal_aware (29 trades, +$18.76) and gptoss_skill_aware (31 trades, +$22.43) both achieved modest positive PnL by keeping trade counts moderate.

### Agentic multi-step reasoning with moderate trade f...
- **Confidence**: 65%
- **Total samples**: 32
- **Times confirmed**: 1
- **First seen**: 2026-03-07
- **Details**: Agentic multi-step reasoning with moderate trade frequency (32 trades) achieved +$77.07 in a bearish market. gptoss_agentic's sophisticated analysis likely correctly identified bearish regime and managed positions with reasonable frequency.

### Agentic multi-step reasoning with moderate-high tr...
- **Confidence**: 63%
- **Total samples**: 30
- **Times confirmed**: 1
- **First seen**: 2026-03-06
- **Details**: Agentic multi-step reasoning with moderate-high trade frequency (30 trades) achieved +$193.68 in a uniformly bearish market. gptoss_agentic's sophisticated analysis likely correctly identified the bearish regime and executed SHORT-biased trades with reasonable frequency.

### Zero trading preserves capital perfectly when mark...
- **Confidence**: 60%
- **Total samples**: 2
- **Times confirmed**: 1
- **First seen**: 2026-03-05
- **Details**: Zero trading preserves capital perfectly when market is uniformly bearish and agent directional bias is uncertain. index_fund (0 trades, $0 PnL) and qwen35_skill_aware (1 trade, $0 PnL) avoided all losses while active agents lost $5.98 to $229.37.

### Journal-aware self-reflective position management ...
- **Confidence**: 55%
- **Total samples**: 32
- **Times confirmed**: 1
- **First seen**: 2026-03-07
- **Details**: Journal-aware self-reflective position management with 32 trades achieved +$1.77 — near breakeven but capital preservation in a bearish market where others lost significantly. Meta-cognitive exits limited damage.

### Forum-aware selective trading with moderate freque...
- **Confidence**: 55%
- **Total samples**: 26
- **Times confirmed**: 1
- **First seen**: 2026-03-06
- **Details**: Forum-aware selective trading with moderate frequency (26 trades) achieved +$31.87 in a bearish market. gptoss_forum_aware's social/forum sentiment signals likely provided correct bearish conviction without excessive overtrading.

### Ultra-low trade frequency with simple strategies i...
- **Confidence**: 52%
- **Total samples**: 29
- **Times confirmed**: 1
- **First seen**: 2026-03-05
- **Details**: Ultra-low trade frequency with simple strategies in a uniformly bearish market. qwen35_122b_simple (16 trades, +$50.27) and qwen35_simple (13 trades, +$35.65) were the only profitable agents, suggesting selective entries with correct directional alignment (likely SHORT-biased or quick profit-taking) outperform in moderate bearish conditions.

### Index fund passive allocation with minimal trading...
- **Confidence**: 50%
- **Total samples**: 8
- **Times confirmed**: 1
- **First seen**: 2026-01-28
- **Details**: Index fund passive allocation with minimal trading (8 trades/24h) achieves break-even ($0.00 PnL) in bullish markets where all assets are positive, outperforming all active strategies

### Low-frequency simple models (13-16 trades) with mo...
- **Confidence**: 48%
- **Total samples**: 29
- **Times confirmed**: 1
- **First seen**: 2026-03-05
- **Details**: Low-frequency simple models (13-16 trades) with modest positive PnL in a bearish market where all 5 assets declined -1.7% to -8.7%. These agents likely captured short-term moves or correctly timed SHORT entries without overtrading.

### Zero trading preserves capital when active agents ...
- **Confidence**: 42%
- **Total samples**: 8
- **Times confirmed**: 1
- **First seen**: 2026-03-04
- **Details**: Zero trading preserves capital when active agents lose money. ta_bot (0 trades, $0) and index_fund (8 trades, $0) avoided all losses while gptoss_agentic (-$435.19) and gptoss_forum_aware (-$403.29) suffered massive drawdowns.

## Patterns to Avoid

- **AVOID**: HIGH-CONFIDENCE SHORT entries during UNIFORM BULLISH market regime. skill_aware_oss opened shorts with 0.85-0.92 confidence on ETHUSDT/BTCUSDT citing 'strong bearish alignment' while market gained +1.33% to +3.82%. Result: -$963.36 (worst performer).
  - Conf: 95%, N=501, seen 1x
- **AVOID**: Validation checks passing ('risk calculator fits 2% equity', 'validation permits trade') used as primary trade justification. All losing agents cited validation success while making directionally wrong trades. Process compliance ≠ profitable trades.
  - Conf: 95%, N=366, seen 1x
- **AVOID**: High trade frequency (147-188 trades/24h) with SHORT bias in bullish markets compounds losses. Each trade against trend adds to drawdown. skill_aware_oss (166 trades, -$963) vs ta_baseline (38 trades, -$120) shows frequency amplifies directional error.
  - Conf: 93%, N=501, seen 1x
- **AVOID**: Opening LONG positions in uniformly bearish markets. agentic_gptoss opened SOL long with 0.95 confidence ('bullish bias with strong technical indicators') but market dropped -11.31%. Counter-trend trades destroy edge.
  - Conf: 92%, N=189, seen 1x
- **AVOID**: Multi-timeframe bearish alignment (15m, 1h, 4h) as SHORT entry signal FAILS in uniform_bullish markets. All three high-frequency agents cited this signal repeatedly while losing $700-$963. Short-term technical bearishness does not override macro bullish regime.
  - Conf: 92%, N=501, seen 1x
- **AVOID**: gpt_simple: Extremely low trade frequency (6 trades/24h) in strongly trending bearish market results in -$163.64 loss. Insufficient participation to capture directional moves.
  - Conf: 90%, N=6, seen 1x
- **AVOID**: Skill-aware agent with highest losses (-$340.51 on 31 trades, -$10.98/trade). Sophisticated risk frameworks (2% equity, 2:1 reward ratio) failed catastrophically, likely due to regime misidentification — citing 'uniform bearish' while market was uniformly bullish (+2.5% to +5.1%).
  - Conf: 90%, N=31, seen 1x
- **AVOID**: Cutting small losses quickly to 'free margin for future trades' destroys edge. gptoss_20b_simple repeatedly closes positions with reasoning like 'close the largest loss to cut losses and free margin' resulting in -$189.24 PnL despite 152 trades in a market that moved 10-16% in their favor direction.
  - Conf: 90%, N=152, seen 1x
- **AVOID**: ta_baseline: Low trade frequency (19 trades) with simple technical signals insufficient to capture bearish momentum, resulting in -$99.04 loss despite correct market read.
  - Conf: 88%, N=19, seen 1x
- **AVOID**: Closing profitable/small-loss positions 'to reduce concentration' or 'free margin' then re-entering same direction. agentic_gptoss closed SOL short at loss, then continued shorting. This churn destroys edge through transaction costs and missed moves.
  - Conf: 88%, N=354, seen 1x
- **AVOID**: agentic_gptoss loses -$619.73 despite 165 trades with similar reasoning patterns to skill_aware_oss. Key difference: inconsistent position management - closing shorts 'to reduce high concentration' then immediately re-entering, creating churn without directional conviction.
  - Conf: 87%, N=165, seen 1x
- **AVOID**: Conflicting reasoning within same agent: agentic_gptoss cites 'BTC showing bullish signals (RSI 68.2, positive MACD)' to close short, then later opens short citing 'All 15m, 1h, 4h trends bearish'. Inconsistent signal interpretation leads to whipsaw losses.
  - Conf: 85%, N=165, seen 1x
- **AVOID**: High trade frequency (183 trades) with inconsistent position management leads to losses. agentic_gptoss made same number of trades as skill_aware_oss but lost $246.40 vs gained $1682.05. Key difference: agentic_gptoss closes due to 'timeframe conflict' and 'potential upside' while skill_aware_oss closes to 'lock in profit'.
  - Conf: 85%, N=183, seen 1x
- **AVOID**: Closing positions due to 'conflicting timeframes' when market is uniformly directional (-7.95% to -16.34% all bearish) leads to missed profits. agentic_gptoss: 'closing BTCUSDT as timeframes conflict' despite clear bearish trend.
  - Conf: 85%, N=165, seen 1x
- **AVOID**: index_fund: Zero trading ($0 PnL) when all assets decline 6-11% is suboptimal. Missing opportunity to profit from clear directional moves via shorts.
  - Conf: 85%, N=0, seen 1x
- **AVOID**: Closing positions 'to reduce concentration risk' or 'heavy short exposure' without clear re-entry criteria. agentic_gptoss closed profitable BTC short 'to reduce concentration' potentially leaving gains on table.
  - Conf: 85%, N=189, seen 1x
- **AVOID**: RSI oversold (28-37) interpreted as SHORT continuation signal in bullish markets. skill_aware_oss cited 'RSI 37, bearish SMA crossover' for ETH short while ETH gained +1.33%. Oversold in uptrend = bounce opportunity, not short entry.
  - Conf: 85%, N=166, seen 1x
- **AVOID**: gptoss_20b_simple loses -$273.95 with 131 trades. Pattern: closing positions 'to cut losses and free margin' on small drawdowns (-$0.14, -$21.20) rather than letting winners run. Premature exits on valid short setups.
  - Conf: 83%, N=131, seen 1x
- **AVOID**: Opening LONG positions in uniformly bearish markets based on 'bullish SMA crossover' and 'positive MACD histogram'. agentic_gptoss opened BTCUSDT long with 0.93 confidence citing 'RSI 62.27, bullish SMA crossover' while BTC dropped -7.95%.
  - Conf: 83%, N=183, seen 1x
- **AVOID**: index_fund opens LONG positions with 1.0 confidence ('Index fund allocation: $2000 into X') in uniformly bearish market, resulting in $0 PnL (likely unrealized losses). Passive long-only fails in bearish regimes.
  - Conf: 80%, N=5, seen 1x
- **AVOID**: Agentic multi-step reasoning with 30 trades lost -$96.83 (-$3.23/trade). Complex reasoning pipelines did not prevent wrong-direction entries in a bullish market.
  - Conf: 80%, N=30, seen 1x
- **AVOID**: Shorting assets that end positive (SOLUSDT +3.90%, BTCUSDT +1.57%) despite 'bearish multi-timeframe alignment' signals. Market direction trumps technical alignment.
  - Conf: 80%, N=296, seen 1x
- **AVOID**: Closing losing positions 'to free margin for future trades' without a clear re-entry plan. gptoss_20b_simple repeatedly cuts small losses then fails to capitalize on freed margin.
  - Conf: 80%, N=131, seen 1x
- **AVOID**: Low trade frequency (6 trades) with poor timing results in significant losses. gpt_simple lost $163.64 on only 6 trades (-$27.27 per trade average), indicating catastrophically bad entry/exit timing.
  - Conf: 80%, N=6, seen 1x
- **AVOID**: Skill-aware agent with 27 trades lost -$108.44, the worst performer. Sophisticated risk frameworks (2% equity, 2:1 reward ratio) failed again, likely due to regime misidentification or wrong directional entries in a bearish market.
  - Conf: 80%, N=27, seen 1x
- **AVOID**: Momentum-following strategy with moderate trade frequency (33 trades) lost -$59.65 in a mixed market with small daily moves (-0.48% to +0.36%). Momentum signals likely generated entries based on prior trends that didn't persist, or whipsawed in the mixed environment.
  - Conf: 78%, N=33, seen 1x
- **AVOID**: Forum-aware agent lost -$86.08 on 27 trades (-$3.19/trade). Forum/social sentiment signals likely amplified wrong-direction conviction (SHORT bias) in a uniformly bullish market.
  - Conf: 78%, N=27, seen 1x
- **AVOID**: gpt_simple with only 7 trades loses $73.60 in bearish market - insufficient trading frequency to capitalize on clear downtrend. Passive approach fails when market has strong directional bias.
  - Conf: 75%, N=7, seen 1x
- **AVOID**: agentic_gptoss achieves only +$79.87 despite 63 trades (highest frequency) due to excessive position churning: multiple 'close' decisions on same asset (BTCUSDT) with conflicting reasoning about trend direction.
  - Conf: 72%, N=63, seen 1x
- **AVOID**: Contrarian strategy with low trade frequency (16 trades) lost -$38.04. Contrarian entries in a mixed market with no clear trend likely entered positions anticipating reversals that didn't materialize or were too small to overcome transaction costs.
  - Conf: 72%, N=16, seen 1x
- **AVOID**: ta_baseline with only 4 trades achieves $0 PnL - technical analysis signals too conservative/infrequent to capture bearish momentum. Missing clear short opportunities.
  - Conf: 70%, N=4, seen 1x
- **AVOID**: LONG-biased entries in a uniformly bearish market (-3% to -5% across all assets). Agents that entered LONG positions (momentum, contrarian, qwen35_simple) all lost money. Wrong directional bias is the primary driver of losses.
  - Conf: 70%, N=21, seen 1x
- **AVOID**: Momentum-following strategy with 23 trades lost -$78.71. In a uniformly bearish market (-0.78% to -2.53%), momentum signals likely generated LONG entries based on prior momentum that reversed, or failed to correctly time SHORT entries.
  - Conf: 70%, N=23, seen 1x
- **AVOID**: Contrarian strategy lost -$22.72 on only 6 trades (-$3.79/trade). Contrarian entries in a uniformly bullish market likely opened SHORT positions anticipating reversals that didn't materialize.
  - Conf: 70%, N=6, seen 1x
- **AVOID**: Closing positions due to 'conflicting trends' or 'uncertain direction' when market is actually uniformly bearish reduces profitability. agentic_gptoss: 'Multi-timeframe analysis shows conflicting trends' - but all assets were down.
  - Conf: 68%, N=20, seen 1x
- **AVOID**: qwen35_simple made only 4 trades but lost -$99.44 (-$24.86/trade), the worst performer. Extremely low frequency with wrong directional bias (likely LONG entries in a bearish market) resulted in large per-trade losses, suggesting oversized positions or holding losers too long.
  - Conf: 65%, N=4, seen 1x
- **AVOID**: Momentum-following strategy lost -$25.04 on 15 trades (-$1.67/trade). Momentum signals may have generated wrong-direction entries or whipsawed in a market with moderate daily gains.
  - Conf: 65%, N=15, seen 1x
- **AVOID**: Contrarian strategy with only 11 trades lost -$12.29. Contrarian LONG entries in a uniformly bearish market lost money, confirming that fighting the trend is unprofitable even at low frequency.
  - Conf: 65%, N=11, seen 1x
- **AVOID**: High trade frequency with likely wrong directional bias or excessive churn. gptoss_agentic (27 trades, -$229.37) was the worst performer, followed by gptoss_journal_aware (62 trades, -$70.94) and ta_bot (14 trades, -$64.10). More trades correlated with larger losses.
  - Conf: 58%, N=103, seen 1x
- **AVOID**: gptoss_momentum made 8 trades and lost -$34.40 in a bearish market. Momentum-following strategy likely entered LONG positions based on prior momentum signals that reversed, or failed to correctly time SHORT entries.
  - Conf: 57%, N=8, seen 1x
- **AVOID**: Agentic complexity with high trade count destroyed capital. gptoss_agentic made 27 trades with sophisticated multi-step reasoning and lost -$229.37 (-$8.49/trade), the worst performer. Complex reasoning pipelines did not prevent losses and may have amplified overconfidence.
  - Conf: 55%, N=27, seen 1x
- **AVOID**: gptoss_contrarian made 9 trades and lost -$15.04. Contrarian strategy in a uniformly bearish market likely opened LONG positions anticipating reversals that didn't materialize.
  - Conf: 55%, N=9, seen 1x
- **AVOID**: Journal-aware self-reflective management with very high trade frequency (62 trades) lost -$70.94. Despite meta-cognitive capabilities ('we are overtrading', 'close to reduce exposure'), the sheer volume of trades overwhelmed the self-correction mechanism.
  - Conf: 50%, N=62, seen 1x
- **AVOID**: qwen35_122b_simple made 28 trades and lost -$12.86. Medium-frequency trading with likely mixed directional bias resulted in small net losses, suggesting the model couldn't consistently identify the bearish trend.
  - Conf: 50%, N=28, seen 1x
- **AVOID**: High trade frequency (166 trades/24h) with aggressive multi-timeframe long positioning results in worst performance (-$134.51) even in bullish markets where all assets are positive
  - Conf: 50%, N=166, seen 1x
- **AVOID**: Moderate-high trade frequency (120-132 trades/24h) with 'validation passed' and 'risk calculator confirms' reasoning leads to losses (-$41 to -$82) despite correct market direction
  - Conf: 47%, N=252, seen 1x
- **AVOID**: Opening short positions (BTCUSDT short at 0.75 confidence) when market is clearly bullish (+1.49% BTC) based on 'MACD negative, SMA short below long' technical signals
  - Conf: 45%, N=1, seen 1x
- **AVOID**: Technical analysis signals (RSI, MACD, SMA) generated losing trades in a uniformly bearish market. ta_bot made 14 trades and lost -$64.10 (-$4.58/trade), suggesting TA signals may have generated LONG entries or poorly-timed entries against the trend.
  - Conf: 45%, N=14, seen 1x
- **AVOID**: Using 'Strong bullish alignment across 15m, 1h, 4h with high momentum' as entry justification leads to losses when transaction costs exceed gains from small moves
  - Conf: 43%, N=298, seen 1x
- **AVOID**: High-frequency trading (28-30 trades) with likely SHORT bias in a uniformly bullish market (all 5 assets +5% to +16%). gptoss_agentic (-$435.19, 28 trades) and gptoss_forum_aware (-$403.29, 30 trades) suffered catastrophic losses from fighting the trend.
  - Conf: 43%, N=58, seen 1x

---

## Confidence Guide

| Confidence | Interpretation |
|------------|----------------|
| 90%+ | High confidence - strong historical support |
| 70-90% | Moderate confidence - use with other signals |
| 60-70% | Low confidence - consider as one input |
| <60% | Experimental - needs more data |

*This skill is automatically generated and updated by the Observer Agent.*
