---
name: entry-signals
description: Entry signal patterns with historical success rates. Use when deciding whether to open a position.
---

# Entry Signals

> Last updated: 2026-03-09 20:08 UTC
> Active patterns: 33
> Total samples: 3679
> Confidence threshold: 60%

## Entry Signals

These entry signals have been learned from competition data:

| Signal | Success Rate | Samples | Confidence | Seen |
|--------|-------------|---------|------------|------|
| SUCCESSFUL SIGNAL: 'Multi-timeframe... | 92% | 183 | 90% | 1x |
| SUCCESSFUL SIGNAL: 'RSI ~28 (overso... | 90% | 200 | 95% | 1x |
| SUCCESSFUL SIGNAL: 'Strong bearish ... | 88% | 150 | 95% | 1x |
| SUCCESSFUL SIGNAL: 'RSI ~28 (overso... | 85% | 49 | 85% | 1x |
| SUCCESSFUL SIGNAL: 'Risk calculator... | 85% | 200 | 95% | 1x |
| Forum/social sentiment-filtered ent... | 80% | 24 | 95% | 1x |
| SUCCESSFUL SIGNAL: 'Positive fundin... | 75% | 48 | 85% | 1x |
| SUCCESSFUL SIGNAL: 'Strong bearish ... | 75% | 172 | 90% | 1x |
| SHORT entries aligned with uniforml... | 75% | 78 | 80% | 1x |
| Forum/social sentiment filtering fo... | 75% | 31 | 90% | 1x |
| Agentic multi-step reasoning entrie... | 72% | 30 | 95% | 1x |
| SUCCESSFUL SIGNAL: 'Bearish trend c... | 70% | 63 | 85% | 1x |
| SUCCESSFUL SIGNAL: 'Risk calculator... | 70% | 172 | 90% | 1x |
| Simple model selective entries (qwe... | 67% | 29 | 65% | 1x |
| Index fund systematic allocation ($... | 50% | 6 | 47% | 1x |
| Index fund systematic allocation ($... | 50% | 12 | 69% | 1x |
| FAILING SIGNAL: 'DOGE is down 7.29%... | 35% | 131 | 90% | 1x |
| FAILING: Momentum-following entries... | 35% | 15 | 80% | 1x |
| FAILING: High-confidence agentic en... | 30% | 27 | 65% | 1x |
| FAILING: Momentum-following entries... | 30% | 33 | 95% | 1x |
| Momentum-following LONG entries in ... | 25% | 23 | 90% | 1x |
| FAILING: Forum/social sentiment-fil... | 25% | 27 | 95% | 1x |
| FAILING SIGNAL: 'Technical analysis... | 20% | 166 | 95% | 1x |
| FAILING SIGNAL: 'RSI ~28-37 (overso... | 18% | 166 | 95% | 1x |
| FAILING SIGNAL: 'Strong bullish ali... | 15% | 166 | 50% | 1x |
| FAILING SIGNAL: 'RSI 62.27, bullish... | 15% | 183 | 90% | 1x |
| FAILING SIGNAL: 'SOLUSDT shows bull... | 15% | 20 | 90% | 1x |
| FAILING SIGNAL: 'All three timefram... | 15% | 354 | 95% | 1x |
| FAILING SIGNAL: 'Risk/reward 2:1 wi... | 13% | 753 | 75% | 2x |
| LONG entries in a uniformly bearish... | 10% | 21 | 77% | 1x |
| FAILING: 'Uniform bearish regime wi... | 10% | 31 | 95% | 1x |
| FAILING: SHORT entries in a uniform... | 5% | 58 | 45% | 1x |
| FAILING: 'Multi-timeframe bearish a... | 5% | 58 | 45% | 1x |

## Signal Details

### SUCCESSFUL SIGNAL: 'Multi-timeframe anal...
**Success rate**: 92%
**Total samples**: 183
**Confidence**: 90%
**Times confirmed**: 1
**First seen**: 2026-01-31
**Description**: SUCCESSFUL SIGNAL: 'Multi-timeframe analysis shows bearish bias, risk calculator gives excellent 2:1 risk-reward with 2% equity risk' for SHORT entry in uniformly bearish markets. skill_aware_oss: +$1682.05.

### SUCCESSFUL SIGNAL: 'RSI ~28 (oversold), ...
**Success rate**: 90%
**Total samples**: 200
**Confidence**: 95%
**Times confirmed**: 1
**First seen**: 2026-02-01
**Description**: SUCCESSFUL SIGNAL: 'RSI ~28 (oversold), price below SMA, bearish MACD, multi-timeframe bearish alignment' for SHORT entry in downtrend. skill_aware_oss used this with 0.78 confidence on BNBUSDT.

### SUCCESSFUL SIGNAL: 'Strong bearish align...
**Success rate**: 88%
**Total samples**: 150
**Confidence**: 95%
**Times confirmed**: 1
**First seen**: 2026-02-01
**Description**: SUCCESSFUL SIGNAL: 'Strong bearish alignment across 15m, 1h, 4h timeframes with high conviction' for SHORT entry. Both skill_aware_oss and agentic_gptoss used this on ETHUSDT.

### SUCCESSFUL SIGNAL: 'RSI ~28 (oversold), ...
**Success rate**: 85%
**Total samples**: 49
**Confidence**: 85%
**Times confirmed**: 1
**First seen**: 2026-01-30
**Description**: SUCCESSFUL SIGNAL: 'RSI ~28 (oversold), price below SMA, bearish MACD, multi-timeframe downtrend' for SHORT entry in bearish market. skill_aware_oss used this pattern for +$691.44.

### SUCCESSFUL SIGNAL: 'Risk calculator show...
**Success rate**: 85%
**Total samples**: 200
**Confidence**: 95%
**Times confirmed**: 1
**First seen**: 2026-02-01
**Description**: SUCCESSFUL SIGNAL: 'Risk calculator shows excellent 2:1 risk/reward, technical conditions neutral-to-bullish, validation permits' - BUT only successful when aligned with macro trend.

---

## Confidence Guide

| Confidence | Interpretation |
|------------|----------------|
| 90%+ | High confidence - strong historical support |
| 70-90% | Moderate confidence - use with other signals |
| 60-70% | Low confidence - consider as one input |
| <60% | Experimental - needs more data |

*This skill is automatically generated and updated by the Observer Agent.*
