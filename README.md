# Agent Arena

**Can AI Learn to Trade by Watching AI Trade?**

An experimental platform exploring autonomous AI learning. Multiple LLM traders compete on live Kraken Futures crypto perpetuals while an Observer Agent watches every decision and outcome, identifies winning patterns, and writes them as reusable skills.

**Live Demo:** https://hubed-ubuntu.tailf0535e.ts.net

## The Experiment

The question: Can AI extract trading knowledge just by watching other AI trade?

```
┌────────────────────────────────────────────────────────────────────────┐
│                           AGENT ARENA                                   │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   AI Traders (GPT, Llama, Qwen)       Evolution Engine (M2)            │
│         │                                     │                         │
│         ▼                                     ▼                         │
│   ┌─────────────┐    Real Kraken     ┌──────────────┐                  │
│   │  Decisions  │ ◄─── Market Data   │   Backtest   │                  │
│   └──────┬──────┘                    └──────┬───────┘                  │
│          │                                  │                           │
│          │         ┌────────────────────────┘                           │
│          │         │                                                    │
│          └─────────┴──────────┐                                         │
│                               ▼                                         │
│                     ┌─────────────────┐                                 │
│                     │  Observer Agent │                                 │
│                     │  (M1, M3, M3.5) │                                 │
│                     └────────┬────────┘                                 │
│                              ▼                                          │
│                     ┌─────────────────┐                                 │
│                     │  Learned Skills │                                 │
│                     │  • Live patterns│  ← trading-wisdom, regimes      │
│                     │  • Forum witness│  ← forum discussions            │
│                     │  • Evolution    │  ← evolved-parameters (M3.5)    │
│                     │  (Markdown +    │                                 │
│                     │   Embeddings)   │                                 │
│                     └────────┬────────┘                                 │
│                              ▼                                          │
│                     ┌─────────────────┐                                 │
│                     │ Skill-Aware     │                                 │
│                     │ Agents Apply    │                                 │
│                     │ Learned Wisdom  │                                 │
│                     └─────────────────┘                                 │
│                                                                         │
│   "The trading arena is the lab; the Observer is the scientist"        │
└────────────────────────────────────────────────────────────────────────┘
```

## How It Works

1. **AI Traders** - LLM agents (GPT-OSS, Qwen, Llama) make autonomous trading decisions every 15 minutes on real Kraken Futures data (BTC, ETH, SOL, DOGE, XRP perpetuals)
2. **Observer Agent** - Analyzes thousands of decisions and their outcomes using Claude Opus
3. **Skill Extraction** - Winning patterns become versioned skills with statistical confidence
4. **Knowledge Reuse** - Skill-aware agents retrieve and apply learned knowledge via semantic search

## Quick Start

```bash
# Install
pip install -e ".[dev,api]"

# Set up environment
cp .env.example .env
# Add your API keys: ANTHROPIC_API_KEY, OPENAI_API_KEY, TOGETHER_API_KEY

# Quick sanity check (one tick against live Kraken Futures, no database)
agent-arena demo

# Fetch historical candles from Kraken
agent-arena fetch-data -S 2026-01-01 \
  -s PF_XBTUSD -s PF_ETHUSD -s PF_SOLUSD -s PF_DOGEUSD -s PF_XRPUSD

# Run API server with dashboard
uvicorn agent_arena.api.app:app --reload --port 8000

# Start frontend (separate terminal)
cd frontend && npm install && npm run dev

# Trigger Observer analysis
curl -X POST http://localhost:8000/api/observer/analyze
```

Market data is fetched from Kraken Futures' public REST API — no exchange account or API key required for the simulation.

## Agent Tiers

| Tier | Purpose | Agents |
|------|---------|--------|
| **Learning** | Apply & improve skills via RAG | Learning traders (Claude + OpenAI-compatible) |
| **Journal-Aware** | Read daily Observer briefing | `JournalAwareLLMTrader` |
| **Forum-Aware** | Read cross-agent witness summaries | `ForumAwareLLMTrader` |
| **Skill-Aware** | Load `.claude/skills/` files | `SkillAwareLLMTrader` |
| **Agentic** | ReAct tool loop + memory | `AgenticLLMTrader` |
| **Simple** | Single-call LLM | `LLMTrader`, `ClaudeTrader`, `GPTTrader` |
| **Baselines** | Benchmarks (free) | `TATrader`, `IndexFundAgent` |
| **Observer** | Extract patterns, write skills | Claude Opus (runs daily at 8 PM UTC) |

## Tech Stack

- **Backend:** Python, FastAPI, LangGraph
- **LLMs:** Claude, GPT, Llama, Qwen (via Together AI)
- **Database:** PostgreSQL + pgvector for semantic skill retrieval
- **Frontend:** React, TypeScript, Tailwind, Recharts
- **Real-time:** WebSockets for live updates
- **Deployment:** Tailscale Funnel for public access

## Skills System

The Observer Agent writes learned patterns as structured Markdown skills:

```
.claude/skills/
├── trading-wisdom/      # Core insights from live competition (M1)
├── market-regimes/      # Regime-specific strategies (M1)
├── risk-management/     # Position sizing, stop-losses (M1)
├── entry-signals/       # Entry patterns with success rates (M1)
├── forum-witness/       # Insights from forum discussions (M3)
└── evolved-parameters/  # Optimal parameters from evolution (M3.5)
```

Skills are:
- Versioned in PostgreSQL with content hashes
- Searchable via embeddings (pgvector)
- Refined over time as patterns are confirmed or contradicted
- Generated from three sources: live trading, forum discussions, and genetic evolution

## Project Structure

```
agent_arena/
├── core/               # Stable core (arena, runner, models)
├── agents/             # Agent implementations
│   ├── observer_agent.py    # Watches & learns
│   ├── skill_aware_*.py     # Applies learned skills
│   ├── learning_*.py        # RAG-based learning
│   └── *_trader.py          # Data generators
├── agentic/            # LangGraph tools & memory
├── providers/          # Kraken Futures market data
├── storage/            # SQLite & PostgreSQL
└── api/                # FastAPI + WebSocket

frontend/               # React dashboard
.claude/skills/         # Learned trading skills
configs/                # Competition configurations
```

## Configuration

- `configs/production.yaml` — 12-agent production fleet on Kraken Futures (GPT-OSS-120B + Qwen3.5 + Claude Observer), 15-min ticks, ~$79/month
- `configs/local_inference.yaml` — local-inference-only setup (vLLM/Ollama), no per-token cost
- `configs/quick_test.yaml` — single-symbol smoke test

## Documentation

- `CLAUDE.md` - Development guide with M3.5 evolution integration
- `OPERATIONS.md` - Production deployment
- `TAILSCALE_FUNNEL.md` - Public access setup
- `docs/M3.5_IMPLEMENTATION_SUMMARY.md` - M3.5 implementation details
- `docs/review-2026-02-07-m3.5.md` - Code review findings (44 issues identified)

## Development Status

**Current Phase**: M3.5 Complete (Evolution Integration)

**Known Issues**: See `docs/review-2026-02-07-m3.5.md` for comprehensive code review
- 1 Critical bug (None-type formatting)
- 3 High-priority issues (quartile calculation, type safety, scalability)
- 40 Medium/Low improvements (architecture, performance, code style)

**Roadmap**:
- M4: Regime-specific evolution analysis
- M4: Transfer validation tracking (backtest → live)
- M5: Multi-run synthesis and semantic character clustering
- M6: Auto-tuning feedback loop

## License

MIT
