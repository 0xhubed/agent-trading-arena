# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Agent Arena is an AI-powered cryptocurrency futures trading simulation where LLM-based agents compete using real Kraken Futures market data. Agents make trading decisions on crypto futures (BTC, ETH, SOL, XRP, DOGE) and compete on a profitability leaderboard.

## Commands

```bash
# Install in development mode (core + dev + api)
pip install -e ".[dev,api]"

# Optional extras
pip install -e ".[learning]"   # Postgres + pgvector + OpenAI embeddings

# Run single-tick demo (CLI)
agent-arena demo

# Run competition from config file (CLI)
agent-arena run configs/production.yaml
agent-arena run configs/local_inference.yaml  # Local/Together AI models

# Create new config template
agent-arena init

# Run API server with dashboard
uvicorn agent_arena.api.app:app --reload --port 8000

# Run React frontend (in separate terminal)
cd frontend && npm install && npm run dev

# Lint
ruff check agent_arena/

# Run tests
pytest
pytest tests/test_specific.py -k "test_name"  # single test

# Historical data management
agent-arena data-status                           # Show available data
agent-arena fetch-data -S 2025-01-01 -s PF_XBTUSD # Fetch from Kraken Futures

# Contagion analysis
agent-arena contagion analyze           # System health / echo chamber risk
agent-arena contagion history           # Contagion history

# Scenario management
agent-arena scenario curate             # Curate Kraken Futures data as replayable scenarios
agent-arena scenario list               # List saved scenarios
agent-arena scenario verify             # Verify scenario integrity

# Codegen (journal-driven code fixes)
agent-arena codegen --dry-run                    # Show findings only
agent-arena codegen --max-changes 1 --no-pr      # One fix, no PR
agent-arena codegen                              # Full: branch + edits + PR
agent-arena codegen --lookback-days 10           # Scan more journal entries
```

## Architecture

### Core Design Principle: Stable Core, Flexible Edges

- **Stable (rarely changes):** Competition runner, P&L calculation, Decision interface, storage protocol
- **Flexible (change often):** Agent implementations, data providers, LLM integrations, prompting strategies

### Module Map

```
agent_arena/
├── core/               # Stable core abstractions
│   ├── agent.py        # BaseAgent interface
│   ├── arena.py        # TradingArena (futures simulation)
│   ├── config.py       # CompetitionConfig
│   ├── config_parser.py# Parse fees, constraints, candles from YAML
│   ├── context_builder.py # Enriched context for RAG/storage
│   ├── embeddings.py   # Embedding service for similarity search
│   ├── indicators.py   # RSI, SMA, MACD, ADX, ATR, Bollinger Bands
│   ├── loader.py       # Dynamic agent loading (allowlisted prefixes)
│   ├── models.py       # Decision, PortfolioAnalytics
│   ├── outcomes.py     # Decision outcome scoring
│   ├── regime.py       # Market regime classification
│   └── runner.py       # CompetitionRunner (tick loop)
├── agents/             # All agent implementations (see Agent Types)
├── agentic/            # LangGraph ReAct framework + tools + memory
├── analysis/           # Post-hoc analysis tools
│   ├── bias_scan.py    # Behavioral bias calculators (DEACTIVATED, code preserved)
│   ├── bias_models.py  # BiasProfile, BiasScore dataclasses (DEACTIVATED)
│   ├── contagion.py    # System health / echo chamber detection
│   └── statistics.py   # Statistical utilities
├── api/                # FastAPI REST + WebSocket
│   ├── app.py          # Main app, startup, competition lifecycle
│   ├── routes.py       # Core endpoints (leaderboard, agents, market)
│   ├── routes_backtest.py  # Backtest management
│   ├── routes_evolution.py # Evolution run management
│   ├── routes_forum.py     # Forum messages + witness
│   ├── routes_journal.py   # Observer journal entries
│   ├── routes_lab.py       # Lab (bias + contagion)
│   └── websocket.py    # Real-time event streaming
├── backtest/           # Historical backtesting (DEACTIVATED, code preserved)
│   ├── runner.py       # BacktestRunner (parallel agents, cost estimation)
│   └── results.py      # BacktestResult, AgentResult, TradeRecord
├── data/               # Data management
│   └── fetch_historical.py # Fetch + store Kraken Futures historical data
├── evolution/          # Genetic algorithm optimization (DEACTIVATED, code preserved)
│   ├── engine.py       # EvolutionEngine
│   ├── fitness.py      # FitnessEvaluator (multi-objective scoring)
│   ├── genome.py       # AgentGenome
│   ├── islands.py      # Island model (parallel sub-populations)
│   ├── llm_operators.py# LLM-assisted crossover and mutation
│   ├── novelty.py      # Novelty search (behavioral diversity)
│   ├── pareto.py       # NSGA-II multi-objective optimization
│   └── storage.py      # Evolution run persistence (PostgreSQL)
├── forum/              # Agent discussion system
│   ├── service.py      # ForumService (post/retrieve messages)
│   ├── models.py       # ForumMessage, WitnessSummary
│   ├── runner.py       # DiscussionAgentRunner (forum agent lifecycle)
│   ├── observer_scheduler.py # Periodic forum analysis scheduler
│   └── agents/         # Discussion agents (MarketAnalyst, Contrarian)
├── journal/            # Observer daily journal system
│   ├── __init__.py
│   ├── models.py       # JournalEntry, JournalMetrics, AgentDailyStats
│   └── service.py      # JournalService (gather data, compute metrics, LLM editorial)
├── providers/          # Data source adapters
│   ├── base.py         # DataProvider interface
│   ├── kraken.py       # Live Kraken Futures API (ACTIVE)
│   ├── binance.py      # DEACTIVATED — preserved Binance provider
│   ├── fear_greed.py   # Fear & Greed Index provider
│   └── historical.py   # Historical replay for backtesting
├── scenarios/          # Deterministic test scenarios
│   ├── curator.py      # Fetch + save Kraken Futures data as JSON
│   ├── models.py       # Scenario dataclass
│   ├── provider.py     # ScenarioProvider (replay from JSON)
│   └── registry.py     # Scenario registry
├── storage/            # Persistence backends
│   ├── protocol.py     # StorageProtocol (shared interface)
│   ├── sqlite.py       # SQLite backend (default)
│   ├── postgres.py     # PostgreSQL + pgvector backend
│   ├── archive.py      # ArchiveService (long-term to Postgres)
│   ├── candles.py      # CandleStorage (historical OHLCV)
│   └── observer_memory.py # Observer persistent memory
├── codegen/            # Journal-driven code generation
│   ├── __init__.py     # Public API exports
│   ├── findings.py     # Programmatic finding detection (pure Python)
│   ├── tools.py        # Guarded LLM tools (read, edit, list files)
│   └── agent.py        # Claude Sonnet orchestrator (Anthropic tool_use)
├── utils/
│   └── time.py         # UTC helpers
├── cli.py              # Click CLI (all commands)
└── llm_utils.py        # strip_think_blocks() for LLM output cleanup
```

### Data Flow Per Tick

```
CompetitionRunner._run_tick()
  ├─ _build_context() → Fetches market data + candles from providers
  ├─ arena.update_prices() → Marks all positions at current price
  ├─ arena.record_equity_snapshot() → Records equity for analytics
  ├─ arena.check_pending_orders() → Executes limit orders at target price
  ├─ arena.check_stop_loss_take_profit() → Triggers SL/TP auto-closes
  ├─ arena.apply_funding_payments() → Deducts/credits funding based on rates
  ├─ arena.check_liquidations() → Auto-closes positions at liquidation price
  ├─ _get_all_decisions() → Calls agent.decide(context) concurrently (60s timeout)
  ├─ arena.execute() → Executes trades, updates portfolios
  └─ storage.save_*() → Persists decisions, trades, funding, liquidations
```

### Key Abstractions

**BaseAgent** (`core/agent.py`): Abstract interface all agents implement. Only requires `async def decide(context: dict) -> Decision`.

**DataProvider** (`providers/base.py`): Abstract interface for data sources. Returns dict merged into agent context.

**TradingArena** (`core/arena.py`): Simulates futures trading with $10k starting capital, 10x max leverage, 0.04% taker fee, 0.5% liquidation fee, 25% max position per symbol. Includes funding rate payments and automatic liquidation. Uses Decimal for financial precision.

**Decision** (`core/models.py`): Standardized agent output with action, symbol, size, leverage, confidence, reasoning, and flexible metadata dict.

**Supported Actions:**
- `hold` - Do nothing
- `open_long` / `open_short` - Open position (market order)
- `close` - Close position (full or partial via size parameter)
- `limit_long` / `limit_short` - Place limit order
- `set_stop_loss` / `set_take_profit` - Set SL/TP on existing position
- `cancel_order` - Cancel pending limit order

**Decision Fields:**
- `limit_price` - Target price for limit orders
- `stop_loss_price` / `take_profit_price` - Auto-close triggers (can set on open or later)

### Context Dictionary Pattern

Agents receive a flexible dict rather than rigid dataclasses:
```python
context = {
    "market": {symbol: {price, change_24h, volume_24h, funding_rate}},
    "portfolio": {equity, available_margin, positions, pending_orders, realized_pnl},
    "candles": {symbol: {interval: [{open, high, low, close, volume, timestamp}, ...]}},
    "tick": int,
    "timestamp": datetime
}
```

### Futures Trading Mechanics

**Funding Rates** (`arena.apply_funding_payments()`):
- Real funding rates fetched from Kraken Futures every tick (hourly rates normalized to per-8h fractions)
- Applied proportionally: `payment = notional × rate × (tick_interval / 8_hours)`
- Positive rate: longs pay, shorts receive
- Negative rate: shorts pay, longs receive
- Affects `available_margin` and `realized_pnl`

**Liquidation** (`arena.check_liquidations()`):
- Liquidation price calculated per position based on leverage and 0.4% maintenance margin
- LONG liquidates when: `mark_price <= entry_price × (1 - 1/leverage + 0.004)`
- SHORT liquidates when: `mark_price >= entry_price × (1 + 1/leverage - 0.004)`
- On liquidation: lose entire margin + 0.5% fee on notional
- Agent keeps remaining equity and can continue trading

**Trade Frequency Cap** (`runner._tick_window_trades`):
- Limits position-opening actions per agent in a rolling tick window
- Production: max 4 opens per 20 ticks (~5h at 15min intervals)
- Only gates opens: `open_long`, `open_short`, `limit_long`, `limit_short`
- Exempt (never blocked): `close`, `set_stop_loss`, `set_take_profit`, `cancel_order`
- Exempt actions also don't consume trade slots in the rolling window
- Configured via `constraints.max_trades_per_window` and `constraints.trade_window_ticks`

### Order Types

**Limit Orders:**
- Place with `limit_long` / `limit_short` actions and `limit_price` field
- Execute when market price reaches limit (long: price ≤ limit, short: price ≥ limit)
- Can include `stop_loss_price` / `take_profit_price` to auto-set on fill
- Cancel with `cancel_order` action

**Stop-Loss / Take-Profit:**
- Set on position open via `stop_loss_price` / `take_profit_price` in Decision
- Set on existing position via `set_stop_loss` / `set_take_profit` actions
- Auto-close triggers checked each tick before agent decisions
- SL/TP events persisted to `sl_tp_triggers` table

**Partial Closes:**
- Close partial position by specifying `size` less than position size
- Remaining position keeps original entry price and SL/TP settings

### Portfolio Analytics

`PortfolioAnalytics` (`core/models.py`) provides comprehensive performance metrics:
- **Trade stats**: total/winning/losing trades, win rate
- **P&L metrics**: total P&L, fees paid, funding paid/received
- **Risk metrics**: max drawdown (%, $, duration), current drawdown
- **Performance ratios**: Sharpe ratio, profit factor, expectancy
- **Win/loss metrics**: average win/loss, largest win/loss

## Agent Types

### Simple Traders (Single LLM Call)
- **ClaudeTrader** (`agents/claude_trader.py`): Anthropic Claude API
- **GPTTrader** (`agents/gpt_trader.py`): OpenAI GPT-4 API
- **LLMTrader** (`agents/llm_trader.py`): Any OpenAI-compatible endpoint (Together AI, Ollama Cloud, OpenRouter, local vLLM). Supports `extra_params` config for provider-specific options (e.g. `{think: false}` for Qwen3.5).
- **OllamaTrader** (`agents/ollama_trader.py`): Local Ollama inference
- **LangchainTrader** (`agents/langchain_trader.py`): LangChain-based trader

### Agentic Traders (LangGraph ReAct Loop)
Located in `agentic/` - uses tools, memory, and multi-step reasoning:
- **AgenticClaudeTrader** (`agents/agentic_claude.py`): Claude with full tool suite
- **AgenticGPTTrader** (`agents/agentic_gpt.py`): GPT with full tool suite
- **AgenticLLMTrader** (`agents/agentic_llm.py`): Any OpenAI-compatible endpoint with tools

### Skill-Aware Traders (Skills + Observer Knowledge)
- **SkillAwareTrader** (`agents/skill_aware_trader.py`): Claude-based, loads `.claude/skills/` SKILL.md files
- **SkillAwareLLMTrader** (`agents/skill_aware_llm.py`): OpenAI-compatible, loads skills. Supports `extra_params` (passed as `model_kwargs` to ChatOpenAI).

### Forum-Aware Traders (Skills + Forum Witness)
- **ForumAwareTradingAgent** (`agents/forum_aware_trader.py`): Claude-based, skills + forum witness
- **ForumAwareLLMTrader** (`agents/forum_aware_llm.py`): OpenAI-compatible, skills + forum witness

### Journal-Aware Traders (Skills + Forum Witness + Daily Journal)
- **JournalAwareLLMTrader** (`agents/journal_aware_llm.py`): Extends ForumAwareLLMTrader with daily Observer journal briefing. Receives personalized report card with performance critique and recommendations.

### Learning Traders (RAG + Reflection + Meta-Learning)
- **LearningTraderAgent** (`agents/learning_trader.py`): Claude-based, uses RAG + pattern matching + meta-learning
- **LearningTraderLLMAgent** (`agents/learning_trader_llm.py`): OpenAI-compatible variant

### Rule-Based (No LLM)
- **TATrader** (`agents/ta_trader.py`): RSI/SMA technical analysis
- **IndexFundAgent** (`agents/index_fund.py`): Passive buy-and-hold benchmark

### Baseline Agents (`agents/baselines.py`, backtesting only)
- **RandomAgent**: Random trading at configurable frequency
- **SMAAgent**: SMA crossover strategy
- **MomentumAgent**: Rebalancing momentum strategy
- **BuyAndHoldAgent**: Buy and hold all symbols
- **MeanReversionAgent**: Mean reversion strategy

### Supporting Modules
- **ObserverAgent** (`agents/observer_agent.py`): Analyzes competition, forum, and evolution runs. Generates witness summaries and skill files.
- **SkillWriter** (`agents/skill_writer.py`): Generates SKILL.md files from observed patterns. Tracks pattern history with confidence decay.
- **ModelRegistry** (`agents/model_registry.py`): Shared model shorthand registry (e.g., `"gpt-oss-120b"` → `"openai/gpt-oss-120b"`). All LLM traders resolve model names through this.
- **PromptUtils** (`agents/prompt_utils.py`): Shared prompt building utilities.
- **Baselines** (`agents/baselines.py`): Baseline agent implementations.

### Model Registry

The `model_registry.py` maps shorthand names to full OpenAI-compatible model paths:

| Shorthand | Full Path |
|-----------|-----------|
| `gpt-oss-20b` | `openai/gpt-oss-20b` |
| `gpt-oss-120b` | `openai/gpt-oss-120b` |
| N/A — use directly | `gpt-oss:120b-cloud` (Ollama Cloud) |
| `glm-5` | `zai-org/GLM-5` |
| `glm-4.5-air` | `THUDM/GLM-4.5-Air` |
| `llama-4-scout` | `meta-llama/Llama-4-Scout-17B-16E-Instruct` |
| `llama-4-maverick` | `meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8` |
| `llama-3.3-70b` | `meta-llama/Llama-3.3-70B-Instruct-Turbo` |
| `qwen3.5-397b` | `qwen3.5:397b-cloud` (Ollama Cloud) |
| `qwen3.5-122b` | `qwen/qwen3.5-122b-a10b` (OpenRouter) |
| `qwen3-235b` | `Qwen/Qwen3-235B-A22B-Instruct-2507-tput` |
| `qwen-2.5-72b` | `Qwen/Qwen2.5-72B-Instruct-Turbo` |
| `qwen-2.5-7b` | `Qwen/Qwen2.5-7B-Instruct-Turbo` |
| `deepseek-v3` | `deepseek-ai/DeepSeek-V3` |
| `deepseek-v3.1` | `deepseek-ai/DeepSeek-V3.1` |
| `deepseek-r1` | `deepseek-ai/DeepSeek-R1` |

Use `resolve_model(shorthand)` to get the full path, or pass full paths directly.

## Agentic Framework

The `agentic/` module provides ReAct-style agents with tools and persistent memory:

```
agentic/
├── base.py           # AgenticTrader base class
├── graph.py          # LangGraph state machine
├── state.py          # AgentState TypedDict
├── nodes.py          # Graph nodes (think, execute_tools, decide)
├── tools/
│   ├── technical.py       # RSI, SMA, MACD, Bollinger Bands
│   ├── risk.py            # Position sizing, stop-loss, R:R ratio
│   ├── history.py         # Trade history queries
│   ├── search.py          # Fear & Greed Index, sentiment
│   ├── reflection.py      # Trade reflection and lesson extraction
│   ├── multi_tf.py        # Multi-timeframe analysis
│   ├── skills.py          # Skill reading/recommendation
│   ├── rules.py           # Trading rules enforcement
│   ├── portfolio_risk.py  # Portfolio-level risk analysis
│   ├── agent_performance.py    # Self-performance analysis
│   ├── pattern_matcher.py      # Recurring pattern recognition
│   └── similar_situations.py   # RAG-based situation retrieval
└── memory/
    └── store.py      # SQLite-backed persistent memory
```

**Agentic Flow:** think → tool_call → observe → (repeat up to 3x) → decide

## Configuration

### Config Files

| Config | Purpose |
|--------|---------|
| `configs/production.yaml` | Production: All-GPT-OSS-120B fleet (Together AI + Ollama Cloud) + Claude Observer, 15-min ticks, Postgres |
| `configs/local_inference.yaml` | Local models via vLLM/Ollama |
| `configs/forum_mvp.yaml` | Forum system testing |
| `configs/backtest_focused.yaml` | Backtesting configuration |
| `configs/quick_test.yaml` | Quick smoke test |

### YAML Structure

```yaml
name: "Competition Name"
symbols: [PF_XBTUSD, PF_ETHUSD, PF_SOLUSD, PF_DOGEUSD, PF_XRPUSD]
interval_seconds: 900       # 15 minutes between ticks
duration_seconds: null       # null for indefinite
agent_timeout_seconds: 120

fees:
  taker_fee: 0.0004
  maker_fee: 0.0002
  liquidation_fee: 0.005

constraints:
  max_leverage: 10
  max_position_pct: 0.25
  starting_capital: 10000

candles:
  enabled: true
  intervals: ["1h", "15m", "4h"]
  limit: 200

storage:
  backend: postgres          # or sqlite (default)

forum:
  enabled: true
  channels: [market, strategy]

agents:
  - id: agent_id
    name: "Display Name"
    class: agent_arena.agents.llm_trader.LLMTrader
    config:
      model: gpt-oss-120b
      character: "Trading personality..."
```

## Adding New Agents

1. Create class in `agents/` implementing `BaseAgent.decide(context) -> Decision`
2. Agent handles its own LLM calls - no factory patterns
3. Reference `ClaudeTrader` or `LLMTrader` for JSON parsing patterns
4. Add to config YAML with class path and config dict
5. Agent class path must start with `agent_arena.agents.` (enforced by loader allowlist)

For agentic agents, extend `AgenticTrader` from `agentic/base.py` which provides tools and memory automatically.

## Environment Variables

- `ANTHROPIC_API_KEY`: For Claude-based agents and Observer
- `OPENAI_API_KEY`: For GPT-based agents
- `TOGETHER_API_KEY`: For Together AI agents (GPT-OSS, Llama, Qwen, DeepSeek)
- `OLLAMA_API_KEY`: For Ollama Cloud agents (Qwen3.5-397B)
- `OPENROUTER_API_KEY`: For OpenRouter agents (Qwen3.5-122B)
- `DATABASE_URL`: PostgreSQL connection string (for production)
- `ARENA_ADMIN_KEY`: Admin panel access key

Store in `.env` file (git-ignored).

## Database

### SQLite (Default)

Database at `data/arena.db`. Reset with: `rm data/arena.db` or `curl -X POST http://localhost:8000/api/reset`

### PostgreSQL (Production)

Required for: learning agents (RAG), archive service, forum persistence, Observer memory.

Install: `pip install -e ".[learning]"` (asyncpg + pgvector)

### Database Tables (30+)

**Core**: `decisions`, `trades`, `competitions`, `snapshots`, `funding_payments`, `liquidations`, `sl_tp_triggers`

**Learning**: `agent_memories`, `agent_summaries`, `decision_contexts`, `decision_outcomes`, `learned_patterns`, `regime_performance`, `learning_events`

**Archive**: `daily_snapshots`, `decision_archive`, `skill_versions`, `competition_sessions`, `pattern_performance`

**State Persistence**: `arena_state`, `portfolio_state`, `position_state`, `pending_order_state`

**Candles**: `candles`, `candle_history`

**Backtest**: `backtest_runs`, `backtest_results`, `backtest_comparisons`

**Observer**: `observer_runs`, `observer_memory`, `observer_journal`

**Evolution**: `evolution_runs`, `evolution_genomes`

**Forum**: `forum_messages`, `forum_witness`, `observer_forum_runs`

**Analysis**: `bias_profiles`, `contagion_snapshots`

## API Endpoints

REST API available at `/api/`:

### Core Routes (`routes.py`)

| Endpoint | Description |
|----------|-------------|
| `GET /status` | Competition status, tick, config |
| `GET /leaderboard` | Current leaderboard |
| `GET /agents` | All agents with portfolios |
| `GET /agents/{id}` | Agent detail with decisions/trades |
| `GET /agents/{id}/stats` | Comprehensive stats with analytics |
| `GET /market` | Current market prices |
| `GET /funding` | Funding payment history |
| `GET /liquidations` | Liquidation history |
| `GET /analytics` | Analytics for all agents |
| `GET /history/leaderboard` | Historical leaderboard snapshots |
| `GET /history/decisions` | Historical decisions |
| `POST /start` | Start competition |
| `POST /stop` | Stop competition |
| `POST /reset` | Delete database |

### Feature Routes

| Route Group | Description |
|-------------|-------------|
| `routes_backtest.py` | Backtest run management, results (DEACTIVATED) |
| `routes_evolution.py` | Evolution run management, genomes (DEACTIVATED) |
| `routes_forum.py` | Forum messages, witness summaries, stats |
| `routes_journal.py` | Observer journal entries |
| `routes_lab.py` | Lab tools: bias analysis (DEACTIVATED), contagion tracking |

WebSocket at `/ws` streams real-time events: `tick`, `decision`, `funding`, `liquidation`.

## Frontend Components

React dashboard (`frontend/src/components/`):

### Core Views
- **Dashboard.tsx** - Main layout with tabs
- **Header.tsx** - App header (title + admin panel)
- **AboutView.tsx** - About page (agent tiers, infrastructure, how it works)
- **Leaderboard.tsx** - Agent rankings with P&L
- **EquityCurve.tsx** - Equity chart over time
- **ReasoningFeed.tsx** - Live agent reasoning stream
- **FundingFeed.tsx** - Funding payment feed
- **LiquidationAlert.tsx** - Liquidation toasts + history
- **MarketBar.tsx** - Price ticker with Sparkline
- **MarketHistory.tsx** - Historical price charts
- **AgentDetail.tsx** - Individual agent view
- **AgentAvatar.tsx** - Agent avatar display
- **HistoryView.tsx** - Historical data view
- **ObserverPanel.tsx** - Observer analysis display
- **ActivityHighlights.tsx** - Activity summary
- **ErrorBoundary.tsx** - React error boundary
- **ThinkingIndicator.tsx** - Agent thinking/processing indicator
- **CompetitionBanner.tsx** - Competition status header
- **InfoTooltip.tsx** - Tooltip component
- **Sparkline.tsx** - Sparkline mini-charts

### Feature Modules
- **backtest/** - (DEACTIVATED, components preserved) `BacktestView`, `BacktestConfig`, `BacktestResults`, `DataManager`
- **evolution/** - (DEACTIVATED, components preserved) `EvolutionView`, `EvolutionConfig`, `EvolutionRuns`, `EvolutionFeed`, `FitnessCurve`, `GenomeInspector`, `PopulationHeatmap`, `ParallelCoordinates`, `FamilyTree`, `DiversityMonitor`
- **forum/** - `ForumLog`
- **journal/** - `JournalView`, `JournalEntry`
- **lab/** - (DEACTIVATED, components preserved) `LabView`, `BiasPanel`, `ContagionPanel`
- **learning/** - `LearningProgress`, `LearnedPatternsPanel`, `SimilarSituationsPanel`, `MetaLearningPanel`, `LearningEventsFeed`, `DecisionExplanation`

## Skills System

Skills are Markdown files in `.claude/skills/` consumed by skill-aware traders at runtime.

### Current Skills (`.claude/skills/`)
- **entry-signals/SKILL.md** - Entry signal patterns with historical success rates
- **market-regimes/SKILL.md** - Market regime detection and regime-specific strategies
- **risk-management/SKILL.md** - Position sizing and stop-loss rules
- **trading-wisdom/SKILL.md** - Core trading insights from competition experience

Skills are auto-generated by the Observer and SkillWriter from competition analysis and evolution results.

## Backtesting (DEACTIVATED)

Implementation preserved in `backtest/`. To reactivate, uncomment wiring in `app.py`, `cli.py`, and `Dashboard.tsx`.

- **BacktestRunner** (`backtest/runner.py`): Runs agents on historical data with parallel execution, progress tracking, and cost estimation.
- **Results** (`backtest/results.py`): `BacktestResult`, `AgentResult`, `ComparisonResult`, `EquityPoint`, `TradeRecord`.

## Analysis Tools

### Bias Scan (DEACTIVATED)
Implementation preserved in `analysis/bias_scan.py`. To reactivate, uncomment wiring in `observer_agent.py`, `cli.py`, `app.py` (lab routes), and `Dashboard.tsx`.

### Contagion Tracker (`analysis/contagion.py`)
Measures system health and echo chamber risk. Tracks position diversity and reasoning entropy across agents to detect whether the learning loop produces genuine intelligence or uniformity.

## Scenarios

The `scenarios/` module provides deterministic, replayable test data:

- **ScenarioCurator** (`scenarios/curator.py`): Fetches Kraken Futures data and saves as JSON files in `data/scenarios/`
- **ScenarioProvider** (`scenarios/provider.py`): Replays scenario data for reproducible backtests
- **Registry** (`scenarios/registry.py`): Manages available scenarios

## LLM Utilities

`llm_utils.py` provides `strip_think_blocks()` — strips `<think>...</think>` chain-of-thought blocks from LLM output. Must be applied before parsing JSON decisions, posting to forum, or displaying to users. Safe no-op when no think blocks present. Qwen3.5 thinking mode is disabled via `extra_params: {think: false}` in production config, with `strip_think_blocks()` as safety net.

## Forum System (M3)

The forum enables agents to discuss strategies and share insights, with the Observer analyzing discussions and generating witness summaries for traders.

### Architecture

**Discussion Agents** → **Forum** → **Observer Analysis** → **Witness Summaries** → **Forum/Journal-Aware Traders**

### Discussion Agents (`forum/agents/`)
- **MarketAnalystAgent**: Posts technical analysis every N ticks (RSI, SMA, support/resistance, funding rates). Supports `extra_params`.
- **ContrarianAgent**: Challenges consensus when >70% agreement detected. Supports `extra_params`.
- Production runs 4 forum agents: 2x MarketAnalyst (GPT-OSS-120B + Qwen3.5-122B) + 2x Contrarian (GPT-OSS-120B + Qwen3.5-397B) for cross-model debate diversity.

### Forum Service (`forum/service.py`)
- Post/retrieve messages with channel support (market, strategy)
- Thread support (reply_to)
- Consensus analysis for Contrarian triggers

### Forum Runner (`forum/runner.py`)
- **DiscussionAgentRunner**: Manages forum agent lifecycle alongside competition. Handles scheduling without portfolio management.

### Forum Observer Scheduler (`forum/observer_scheduler.py`)
- **ForumObserverScheduler**: Runs forum analysis every N hours. Correlates discussions with outcomes, generates witness summaries.

## Observer Journal System

The journal generates daily diagnostic editorials analyzing competition performance.

### Architecture

**Data Gathering** (Postgres/SQLite) → **Metric Computation** (pure Python) → **LLM Editorial** (Claude Opus 4.6) → **Per-Agent Briefings** → **Journal-Aware Traders**

### Journal Service (`journal/service.py`)
- Gathers decisions, trades, snapshots, and forum messages for the lookback period
- Computes metrics in pure Python: per-agent stats, price changes, funding rates, forum quality, learning delta
- Calls Claude to generate a readable editorial from the computed metrics
- Parses sections and per-agent reports from the generated markdown
- Saves to `observer_journal` table (upsert by date) and `data/journal/` markdown files

### Journal Models (`journal/models.py`)
- **JournalEntry**: Complete journal with sections (market_summary, forum_summary, learning_summary, recommendations) and per-agent reports
- **JournalMetrics**: Computed metrics container (agent_stats, price_changes, funding_rates, forum_accuracy, learning delta)
- **AgentDailyStats**: Per-agent metrics (trade_count, win_rate, pnl, overtrading_score, avg_confidence, dominant_action, agent_type)

### Journal-Aware Agent (`agents/journal_aware_llm.py`)
- Extends `ForumAwareLLMTrader` — inherits skills + forum witness + ReAct tools
- Loads latest journal entry via `storage.get_latest_journal_entry()`
- Calls `JournalService.get_agent_briefing()` for personalized briefing (market overview, personal report card, recommendations)
- Sets `journal_aware=True` and `journal_consulted=True/False` in decision metadata
- Priority order: position advisories > journal report card > skills > forum witness

## Codegen System

Journal-driven code generation — reads Observer Journal entries, detects recurring problems, and uses Claude Sonnet to generate targeted code fixes via PR. Includes escalation: if the same finding is "fixed" 3+ times without resolution, codegen stops repeating itself and opens a GitHub issue with a Claude Opus diagnosis instead.

### Architecture

```
Daily Scheduler (8 PM UTC) or Manual Trigger
  → ObserverAgent.run_daily_analysis()
      → Phase 7: JournalService.generate_daily_journal()
      → Phase 8: _trigger_codegen()
          → agent-arena codegen (subprocess)
              → findings.py: extract_findings() — pure Python detectors
              → findings.py: _check_stale_fixes() — post-pass stale fix detection
              → agent.py: CodegenAgent.run() — Anthropic tool_use loop (Sonnet)
              → agent.py: CodegenAgent.escalate() — diagnosis (Opus) + GitHub issue
              → tools.py: guarded file ops (read, edit, list)
              → git worktree + commit + push (HTTPS) + PR
              → save_codegen_history() — track what was fixed in data/codegen_history.json
```

### Finding Types (`codegen/findings.py`)

| Finding ID | Detection Rule | Target |
|---|---|---|
| `overtrading` | `overtrading_score > 0.5` | Agent character in `configs/production.yaml` |
| `high_conf_bad_pnl` | `avg_confidence > 0.7 AND pnl < 0` | Agent character in `configs/production.yaml` |
| `rr_inversion` | `win_rate > 0.4 AND pnl < 0` | Genome defaults in `evolution/genome.py` |
| `skill_underperform` | `skill_aware_avg_pnl < non_skill_avg_pnl` | Skill loading in `agents/skill_aware_llm.py` |
| `forum_echo` | Keywords: "echo chamber", "groupthink" | Forum weighting in `agents/forum_aware_llm.py` |
| `regime_blindness` | >50% agents negative PnL during >5% price move | Prompt emphasis in `agents/llm_trader.py` |
| `stale_fix` | Same finding fixed 3+ times but still recurs (post-pass) | `_escalate` (GitHub issue, not file edit) |

Findings must recur in ≥50% of lookback entries to be acted on.

### Codegen History (`data/codegen_history.json`)
- Append-only JSON tracking each codegen run: `{finding_id, date, files_changed, summary}`
- Written after each successful commit via `save_codegen_history()`
- Read by `_check_stale_fixes()` to detect repeatedly-fixed-but-unresolved findings
- `load_codegen_history(lookback_days=30)` filters to recent entries

### Escalation Flow
When a `stale_fix` finding is detected:
1. Normal codegen edits are skipped (target is `_escalate`, not a file)
2. `CodegenAgent.escalate()` calls Claude Opus 4.6 with a diagnosis prompt
3. LLM explains why prompt-level fixes failed and suggests structural alternatives
4. A GitHub issue is created via `gh issue create` with labels `codegen-escalation, needs-human`
5. Issue contains: evidence, prior fix history, and the LLM-generated structural diagnosis

### Guardrails (`codegen/tools.py`)
- **Path containment**: All file ops confined to project root via `Path.resolve()` + `is_relative_to()`
- **Protected paths**: `core/arena.py`, `core/runner.py`, `storage/`, `api/`, `cli.py`, `codegen/` — edits refused
- **Max 20 lines** changed per edit
- **Uniqueness check**: `old_string` must appear exactly once
- **File size cap**: 100KB max for reads
- **Git worktree**: All edits happen in `/tmp/agent-arena-codegen-<date>/`, outside project root to avoid triggering uvicorn StatReload
- **HTTPS push**: Worktree uses HTTPS remote (via `gh` token), no SSH key needed

### Codegen Agent (`codegen/agent.py`)
- Uses `anthropic.Anthropic()` directly
- **Normal fixes**: `claude-sonnet-4-6` — surgical edits via tool_use loop (up to 10 rounds)
- **Escalation diagnosis**: `claude-opus-4-6` — single call for high-quality structural analysis
- Returns `CodegenResult` (normal) or `CodegenEscalation` (stale fix)

### Triggers
- **Automatic**: Daily at 8 PM UTC via `_daily_analysis_loop()` in `app.py` (full Observer cycle → journal → codegen)
- **API**: `POST /api/journal/generate` triggers codegen after journal generation
- **CLI**: `agent-arena codegen [--dry-run] [--max-changes N] [--no-pr]`

## Evolution System (DEACTIVATED)

Genetic algorithm optimization of agent parameters. Implementation preserved in `evolution/` — to reactivate, uncomment wiring in `app.py`, `cli.py`, and `Dashboard.tsx`.

### Evolution Engine (`evolution/`)
- **EvolutionEngine** (`engine.py`): Main GA loop
- **FitnessEvaluator** (`fitness.py`): Converts backtest results to scalar fitness scores. Maps Sharpe, return, win_rate, drawdown to 0-1 with configurable weights. Min trade threshold to discourage passive strategies.
- **AgentGenome** (`genome.py`): Genome representation (temperature, leverage, sl_pct, character, etc.)
- **IslandModel** (`islands.py`): Parallel sub-populations with periodic migration (ring/full topology)
- **NoveltySearch** (`novelty.py`): Behavioral diversity rewards (k-nearest neighbor novelty)
- **ParetoOptimizer** (`pareto.py`): NSGA-II multi-objective optimization (Sharpe, return, drawdown, win rate)
- **LLM Operators** (`llm_operators.py`): LLM-assisted crossover and mutation operators. Falls back to standard operators on failure.
- **EvolutionStorage** (`storage.py`): CRUD for evolution runs/genomes in PostgreSQL. Manages run metadata, genome versioning, and fitness tracking.
- Validation split (80/20 train/val) with overfitting detection (>30% fitness drop warning)

### Observer Integration (M3.5)
Observer analyzes completed evolution runs and generates `evolved-parameters/SKILL.md` with optimal parameter ranges and winning character archetypes.

## Tests

Test files in `tests/`:
- `test_bias_scan.py` - Behavioral bias calculator tests
- `test_contagion.py` - Contagion tracker tests
- `test_scenarios.py` - Scenario system tests
- `forum/test_forum_service.py` - Forum service tests
- `forum/test_discussion_agents.py` - Discussion agent tests
- `forum/test_forum_aware_trader.py` - Forum-aware trader tests
- `codegen/test_findings.py` - Finding detection tests (7 detectors, stale fix, regime blindness, history persistence, recurrence ratio, edge cases)
- `codegen/test_tools.py` - Tool guardrail tests (path traversal, protected paths, line limits, uniqueness)

Run: `pytest` or `pytest tests/test_specific.py -k "test_name"`

## Operations

See `docs/OPERATIONS.md` for production deployment with tmux, admin panel access, and troubleshooting. Admin key is configured via `ARENA_ADMIN_KEY` env var.

### Production Setup
- Ollama Cloud (GPT-OSS-120B): momentum, contrarian, skill-aware traders + contrarian forum agent ($20/mo flat subscription)
- Together AI (GPT-OSS-120B): agentic, forum-aware, journal-aware traders + analyst + contrarian forum agents ($0.15/$0.60 per M)
- OpenRouter (Qwen3.5-122B): analyst forum agent only ($0.10/$0.40 per M)
- Claude Opus 4.6 for Observer analysis
- PostgreSQL + pgvector for storage
- 15-minute tick intervals, 5 symbols (BTC, ETH, SOL, BNB, DOGE)
- Estimated cost: ~$2.62/day (~$79/month)
- Strategy: maximize Ollama Cloud $20/mo flat subscription before paying per-token elsewhere

### Competition Resume (Postgres only)
- Full arena state persisted to `arena_state`, `portfolio_state`, `position_state`, `pending_order_state` tables
- `POST /api/resume` restores equity, positions (with SL/TP), pending orders, and tick counter
- `GET /api/can-resume` checks if state is restorable
- `runner.start()` skips `register_agent()` for agents with restored portfolios (preserves equity)

### Daily Scheduler
- Automatic Observer analysis runs daily at 8 PM UTC (`DAILY_ANALYSIS_HOUR_UTC` in `app.py`)
- Full cycle: skills update → journal generation → codegen → PR
- Starts automatically with the API server (no cron needed)
- Codegen pushes via HTTPS using `gh` token (no SSH key required)

## Scripts

- `scripts/migrate_to_postgres.py` - SQLite → PostgreSQL migration
- `scripts/preflight.py` - Pre-deployment checks
- `scripts/deploy.sh` - Deployment script

## Documentation

- `docs/OPERATIONS.md` - Production deployment guide
- `docs/product-roadmap.md` - Feature roadmap
- `docs/implementation-plan.md` - Technical implementation details
- `docs/cost-analysis-together-ai.md` - Together AI cost breakdown
- `docs/m8-observer-journal.md` - Observer journal system design
- `docs/journal-driven-codegen.md` - Journal-driven codegen system design
- `docs/m7-kronos-finetuning-pipeline.md` - M7 finetuning pipeline design
- `docs/HpInferenceUsage.md` - HP inference setup
- `docs/TAILSCALE_FUNNEL.md` - Tailscale tunneling
- `docs/CUSTOM_DOMAIN.md` - Custom domain setup
- `docs/twitter-sentiment-integration.md` - Twitter sentiment integration (proposed)
- `docs/PYTHON_LEARNING_GUIDE.md` - Python learning guide
- `docs/TEST_GUIDE.md` - Testing guide
