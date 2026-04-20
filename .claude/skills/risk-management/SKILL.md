---
name: risk-management
description: Risk management rules learned from competition outcomes. Use when sizing positions or setting stop-losses.
---

# Risk Management

> Last updated: 2026-03-09 20:08 UTC
> Active patterns: 54
> Total samples: 8500
> Confidence threshold: 60%

## Core Principles

These rules are derived from analyzing profitable vs losing trades:

| Rule | Success Rate | Samples | Confidence | Seen |
|------|-------------|---------|------------|------|
| Trade frequency inversely correlates wit... | 95% | 473 | 50% | 1x |
| When ALL 5 tracked assets are positive o... | 95% | 58 | 45% | 1x |
| Position sizing at 2% equity risk with 2... | 92% | 183 | 90% | 1x |
| Position sizing at 2% equity risk with 2... | 92% | 200 | 95% | 1x |
| Cap trade frequency at 10 trades/24h max... | 90% | 84 | 45% | 1x |
| Closing LONG positions quickly to pivot ... | 88% | 50 | 95% | 1x |
| Trade frequency must be inversely propor... | 88% | 109 | 45% | 1x |
| Multi-timeframe bearish alignment (15m, ... | 88% | 383 | 99% | 2x |
| In uniformly bearish markets, 'low-sever... | 85% | 49 | 85% | 1x |
| Validation checks passing ('validation p... | 85% | 200 | 95% | 1x |
| High trade frequency (189-200 trades/24h... | 85% | 389 | 95% | 1x |
| When ALL 5 tracked assets are positive o... | 85% | 113 | 95% | 1x |
| Position sizing at 2% equity risk with 2... | 80% | 49 | 85% | 1x |
| Self-reflective position management (jou... | 80% | 48 | 80% | 1x |
| Forum/social sentiment filtering provide... | 80% | 24 | 95% | 1x |
| Closing SHORT positions 'to lock in gain... | 78% | 48 | 85% | 1x |
| Low leverage (2x) on SHORT positions wit... | 75% | 48 | 85% | 1x |
| Position sizing at 2% equity risk with 2... | 75% | 172 | 90% | 1x |
| Low trade frequency (<40 trades/24h) pre... | 75% | 48 | 95% | 1x |
| In uniformly bearish markets, SHORT-bias... | 75% | 3 | 41% | 1x |
| Avoid momentum-following strategies in m... | 75% | 33 | 95% | 1x |
| Self-reflective position management (rec... | 75% | 27 | 95% | 1x |
| Contrarian strategies underperform in mi... | 70% | 16 | 82% | 1x |
| Cap trade frequency at 15 trades/24h max... | 70% | 113 | 95% | 1x |
| Cap trade frequency at 16 trades/24h max... | 67% | 86 | 65% | 1x |
| In mixed/low-volatility markets (daily m... | 67% | 146 | 95% | 1x |
| Self-reflective position management (jou... | 65% | 65 | 45% | 1x |
| Multi-timeframe bearish alignment (15m, ... | 50% | 337 | 90% | 1x |
| High trade frequency (165-172 trades/24h... | 50% | 337 | 90% | 1x |
| High trade frequency (183 trades/24h) ca... | 50% | 366 | 90% | 1x |
| In uniformly bearish markets with mild d... | 50% | 6 | 57% | 1x |
| Validation checks passing ('all checks p... | 41% | 998 | 94% | 2x |
| Closing positions due to 'conflicting ti... | 35% | 20 | 80% | 1x |
| Trade frequency inversely correlates wit... | 33% | 86 | 65% | 1x |
| Cutting small losses quickly (-$0.14 to ... | 30% | 131 | 90% | 1x |
| Cutting small losses quickly (-$38 on SO... | 30% | 147 | 95% | 1x |
| Contrarian LONG entries in bullish marke... | 30% | 6 | 62% | 1x |
| Closing positions 'to reduce concentrati... | 30% | 708 | 99% | 3x |
| Closing positions 'to reduce concentrati... | 25% | 165 | 90% | 1x |
| Validation checks passing ('all checks p... | 25% | 501 | 95% | 1x |
| High concentration warnings (127% of equ... | 25% | 166 | 95% | 1x |
| Multi-timeframe alignment (15m, 1h, 4h) ... | 15% | 298 | 50% | 1x |
| Low trade frequency (<20 trades/24h) in ... | 15% | 25 | 95% | 1x |
| Position sizing at 2% equity risk with 2... | 15% | 166 | 95% | 1x |
| 2% equity risk with 2:1 reward ratio con... | 15% | 58 | 45% | 1x |
| 2% equity risk with 2:1 reward ratio FAI... | 10% | 31 | 95% | 1x |
| Cutting small losses quickly (-$0.01 to ... | 5% | 152 | 90% | 1x |
| Position sizing at 2% equity risk with 2... | 0% | 334 | 50% | 1x |
| High leverage (4x-5x) with 'optimal' ris... | 0% | 132 | 50% | 1x |
| Self-reflective position management (jou... | 0% | 62 | 65% | 1x |
| Low-frequency LONG entries in a bearish ... | 0% | 4 | 43% | 1x |
| Contrarian LONG entries in uniformly bea... | 0% | 9 | 53% | 1x |
| Skill-aware validation frameworks (2% eq... | 0% | 27 | 90% | 1x |
| Contrarian LONG entries in bearish marke... | 0% | 11 | 67% | 1x |

## Top Risk Rules

### Trade frequency inversely correlates with performance in bullish markets: 8 trades = $0.00, 30-34 trades = -$46 to -$51, 120-166 trades = -$41 to -$135
- Success rate: 95%
- Based on 473 observations
- Confidence: 50% (seen 1 times)
- First identified: 2026-01-28

### When ALL 5 tracked assets are positive over 24h with gains exceeding +5%, immediately halt ALL SHORT entries regardless of any lower-timeframe signals, validation checks, or risk calculators.
- Success rate: 95%
- Based on 58 observations
- Confidence: 45% (seen 1 times)
- First identified: 2026-03-04

### Position sizing at 2% equity risk with 2:1 reward ratio SUCCEEDS when agent maintains SHORT bias in bearish markets and uses validation to confirm market direction alignment.
- Success rate: 92%
- Based on 183 observations
- Confidence: 90% (seen 1 times)
- First identified: 2026-01-31

### Position sizing at 2% equity risk with 2:1 reward ratio SUCCEEDS when market direction aligns with position bias and agent maintains high trade frequency (150-200/24h).
- Success rate: 92%
- Based on 200 observations
- Confidence: 95% (seen 1 times)
- First identified: 2026-02-01

### Cap trade frequency at 10 trades/24h maximum in strongly bullish markets when agent directional bias is uncertain. qwen35_skill_aware's 7 trades (+$250.52) vs gptoss_agentic's 28 trades (-$435.19).
- Success rate: 90%
- Based on 84 observations
- Confidence: 45% (seen 1 times)
- First identified: 2026-03-04

## General Guidelines

- Never risk more than 2% of equity on a single trade
- Use stop-losses on every position
- Reduce position size in high volatility regimes
- Don't add to losing positions

---

## Confidence Guide

| Confidence | Interpretation |
|------------|----------------|
| 90%+ | High confidence - strong historical support |
| 70-90% | Moderate confidence - use with other signals |
| 60-70% | Low confidence - consider as one input |
| <60% | Experimental - needs more data |

*This skill is automatically generated and updated by the Observer Agent.*
