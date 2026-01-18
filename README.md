# Agent Arena

**Can AI Learn to Trade by Watching AI Trade?**

An experimental platform exploring autonomous AI learning. Multiple LLM traders compete on simulated crypto futures (using live Binance market data) while an Observer Agent watches every decision and outcome, identifies winning patterns, and writes them as reusable skills.

> **Note:** This is a paper trading simulation - no real money is involved.

## The Experiment

The question: Can AI extract trading knowledge just by watching other AI trade?

```
┌─────────────────────────────────────────────────────────────────┐
│                        AGENT ARENA                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   AI Traders (GPT, Llama, Qwen)                                 │
│         │                                                        │
│         ▼                                                        │
│   ┌─────────────┐    Real Binance    ┌──────────────┐           │
│   │  Decisions  │ ◄─── Market Data   │   Outcomes   │           │
│   └──────┬──────┘                    └──────┬───────┘           │
│          │                                  │                    │
│          └──────────────┬───────────────────┘                    │
│                         ▼                                        │
│               ┌─────────────────┐                                │
│               │  Observer Agent │                                │
│               │  (Watches All)  │                                │
│               └────────┬────────┘                                │
│                        ▼                                         │
│               ┌─────────────────┐                                │
│               │  Learned Skills │                                │
│               │  (Markdown +    │                                │
│               │   Embeddings)   │                                │
│               └────────┬────────┘                                │
│                        ▼                                         │
│               ┌─────────────────┐                                │
│               │ Skill-Aware     │                                │
│               │ Agents Apply    │                                │
│               │ Learned Wisdom  │                                │
│               └─────────────────┘                                │
│                                                                  │
│   "The trading arena is the lab; the Observer is the scientist" │
└─────────────────────────────────────────────────────────────────┘
```

## How It Works

1. **AI Traders** - LLM agents (GPT, Llama, Qwen) make autonomous trading decisions every 5 minutes on real market data
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

# Run API server with dashboard
uvicorn agent_arena.api.app:app --reload --port 8000

# Start frontend (separate terminal)
cd frontend && npm install && npm run dev

# Trigger Observer analysis
curl -X POST http://localhost:8000/api/observer/analyze
```

## Agent Tiers

| Tier | Purpose | Agents |
|------|---------|--------|
| **Learning** | Apply & improve skills | Skill-Aware, Skill-Only traders |
| **Data Generation** | Generate decision/outcome data | GPT-4, Qwen, Llama, DeepSeek |
| **Baselines** | Benchmarks (no LLM cost) | TA Bot, Index Fund |
| **Observer** | Extract patterns, write skills | Claude Opus |

## Tech Stack

- **Backend:** Python, FastAPI, LangGraph
- **LLMs:** Claude, GPT, Llama, Qwen (via Together AI)
- **Database:** SQLite (default) or PostgreSQL + pgvector for semantic skill retrieval
- **Frontend:** React, TypeScript, Tailwind, Recharts
- **Real-time:** WebSockets for live updates

## Skills System

The Observer Agent writes learned patterns as structured Markdown skills:

```
skills/
├── trading-wisdom/      # Core insights
├── market-regimes/      # Regime-specific strategies
├── risk-management/     # Position sizing, stop-losses
└── entry-signals/       # Entry patterns with success rates
```

Skills are:
- Versioned in PostgreSQL with content hashes
- Searchable via embeddings (pgvector)
- Refined over time as patterns are confirmed or contradicted

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
├── providers/          # Binance market data
├── storage/            # SQLite & PostgreSQL
└── api/                # FastAPI + WebSocket

frontend/               # React dashboard
skills/                 # Learned trading skills
configs/                # Competition configurations
```

## Configuration

See `configs/lean_diverse.yaml` for a cost-optimized setup (~$5/day) with 11 agents.

## License

MIT
