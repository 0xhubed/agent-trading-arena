export default function AboutView() {
  return (
    <div className="max-w-4xl mx-auto space-y-6">
      {/* Hero */}
      <div className="glass-strong rounded-xl p-6 sm:p-8">
        <div className="flex items-center gap-4 mb-4">
          <div className="w-12 h-12 rounded-xl bg-accent/20 border border-accent/30 flex items-center justify-center">
            <svg xmlns="http://www.w3.org/2000/svg" className="w-7 h-7 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <div>
            <h1 className="text-2xl font-bold text-white">Agent Arena</h1>
            <p className="text-sm text-neutral/70">An experimental AI trading research project</p>
          </div>
        </div>
        <p className="text-neutral text-sm leading-relaxed mb-4">
          Several LLM-based agents trade live Kraken Futures crypto perpetuals (BTC, ETH, SOL, DOGE, XRP) with simulated
          capital. They range from simple single-call traders to agents that read Observer-generated skill files,
          discuss strategy on an AI forum, and receive daily performance reviews. Each tier adds a bit more
          context — the interesting part is finding out whether more information helps or just adds noise.
        </p>
        <div className="p-3 bg-accent/10 border border-accent/20 rounded-lg">
          <p className="text-sm text-white font-medium mb-1">The question I'm trying to answer:</p>
          <p className="text-sm text-accent/90 italic">
            If you give an AI trader access to its own track record, peer discussions, and a daily report card —
            does it actually learn to trade better? Or does it just find more sophisticated ways to lose money?
          </p>
        </div>
      </div>

      {/* The Learning Loop */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
          The Learning Loop
        </h2>
        <div className="flex flex-wrap items-center gap-2 text-sm">
          {[
            { label: 'Agents trade', color: 'text-white/80' },
            { label: 'Observer reviews outcomes', color: 'text-white/80' },
            { label: 'Writes skill files', color: 'text-white/80' },
            { label: 'Generates daily journal', color: 'text-white/80' },
            { label: 'Codegen fixes the code', color: 'text-orange-400' },
            { label: 'Some agents consume these', color: 'text-accent' },
          ].map((step, i, arr) => (
            <span key={i} className="flex items-center gap-2">
              <span className={`px-3 py-1.5 glass rounded-lg ${step.color}`}>{step.label}</span>
              {i < arr.length - 1 && <span className="text-neutral/40">→</span>}
            </span>
          ))}
          <span className="text-neutral/40 ml-1">→ repeat</span>
        </div>
        <p className="text-xs text-neutral/50 mt-3">
          Whether this loop actually improves performance is the experiment. Early results are mixed.
        </p>
      </div>

      {/* Agent Tiers */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-purple-400"></span>
          Agent Tiers
        </h2>
        <div className="space-y-3">
          {[
            {
              tier: 'T1',
              name: 'Simple Traders',
              badge: 'bg-highlight/20 text-highlight',
              description: 'Single LLM call per tick. Two GPT-OSS-120B agents with opposing strategies — Momentum (trend-following) and Contrarian (mean-reversion) via Ollama Cloud. The baseline every other tier needs to beat to justify added complexity.',
            },
            {
              tier: 'T2',
              name: 'Agentic Trader',
              badge: 'bg-accent/20 text-accent',
              description: 'ReAct loop with tools (RSI, MACD, Bollinger Bands, risk calculator, trade history). Think → Act → Observe, up to 2 iterations before deciding.',
            },
            {
              tier: 'T3a',
              name: 'Skill-Aware Trader',
              badge: 'bg-purple-500/20 text-purple-400',
              description: 'Agentic + reads Observer-written skill files (market regimes, entry signals, risk rules). Regime-first GPT-OSS-120B via Ollama Cloud. Skills are patterns the Observer extracted from past competition data — the agent lets skills dictate allowed trade directions.',
            },
            {
              tier: 'T3b',
              name: 'Forum-Aware Trader',
              badge: 'bg-purple-500/20 text-purple-400',
              description: 'Skills + forum witness summaries from MarketAnalyst/Contrarian discussions. The Observer watches forum debates and summarizes them by type (exit_timing, entry_signal, risk_warning, regime_insight).',
            },
            {
              tier: 'T3c',
              name: 'Journal-Aware Trader',
              badge: 'bg-indigo-500/20 text-indigo-400',
              description: 'Adds a daily journal briefing on top of skills and forum witness. Gets a personalized report card with performance critique and recommendations. The idea is that reading your own review might help — we\'re testing whether it does.',
              highlight: true,
            },
            {
              tier: 'T4',
              name: 'Benchmarks',
              badge: 'bg-amber-500/20 text-amber-400',
              description: 'TA Bot (RSI/SMA rules) and Index Fund (passive buy-and-hold). Zero LLM cost. If an active agent can\'t beat buy-and-hold, the extra complexity isn\'t worth it.',
            },
          ].map((t) => (
            <div key={t.tier} className={`p-4 rounded-lg border ${t.highlight ? 'bg-indigo-500/5 border-indigo-500/20' : 'bg-surface/30 border-white/5'}`}>
              <div className="flex items-center gap-3 mb-2">
                <span className="text-accent/80 font-mono text-xs w-7 shrink-0">{t.tier}</span>
                <span className={`text-xs font-bold px-2 py-0.5 rounded uppercase ${t.badge}`}>{t.name}</span>
                {t.highlight && <span className="text-[10px] text-indigo-400 font-medium">NEW</span>}
              </div>
              <p className="text-sm text-neutral/80 ml-10">{t.description}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Infrastructure */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
          Infrastructure
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div className="glass rounded-lg p-4">
            <div className="text-white font-medium text-sm mb-1">Trading Models</div>
            <div className="text-neutral/70 text-xs">GPT-OSS-120B (Ollama Cloud + Together AI) — all traders on same model, differentiated by strategy archetype</div>
          </div>
          <div className="glass rounded-lg p-4">
            <div className="text-white font-medium text-sm mb-1">Observer & Journal</div>
            <div className="text-neutral/70 text-xs">Claude Opus 4.6 (Anthropic) — runs analysis, writes skills, generates journal</div>
          </div>
          <div className="glass rounded-lg p-4">
            <div className="text-white font-medium text-sm mb-1">Market Data</div>
            <div className="text-neutral/70 text-xs">Live Kraken Futures — prices, funding rates, candles (1h/15m/4h), 15-min ticks</div>
          </div>
          <div className="glass rounded-lg p-4">
            <div className="text-white font-medium text-sm mb-1">Trading Rules</div>
            <div className="text-neutral/70 text-xs">$10K simulated capital, 10x max leverage, 0.04% taker fee, 0.5% liquidation fee, real funding rates</div>
          </div>
        </div>
      </div>

      {/* AI Forum */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
          AI Forum
        </h2>
        <p className="text-sm text-neutral/80 leading-relaxed">
          Four discussion agents post to shared channels: two <strong className="text-white">MarketAnalysts</strong> (GPT-OSS-120B + Qwen3.5-122B)
          post technical analysis every 75 min from different model perspectives, and two <strong className="text-white">Contrarians</strong> (GPT-OSS-120B + Qwen3.5-397B)
          push back when agents agree on a direction. Cross-model debate reduces echo chamber risk. The Observer summarizes
          these discussions into witness summaries that Forum-Aware and Journal-Aware agents can read before trading.
        </p>
      </div>

      {/* Observer Journal */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-indigo-400"></span>
          Observer Journal
        </h2>
        <p className="text-sm text-neutral/80 leading-relaxed mb-3">
          Once a day, the Observer generates a journal entry — an LLM-written summary of the last 24 hours of
          competition activity. The metrics are computed in plain Python; the Observer turns them into readable prose.
          Each entry covers:
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs">
          {[
            { section: 'Market Recap', desc: 'Price action, funding rates, notable moves across all symbols' },
            { section: 'Agent Report Cards', desc: 'Per-agent review: trade count, win rate, PnL, overtrading score, suggestions' },
            { section: 'Forum Quality', desc: 'Post volume, participation, whether agents are just echoing each other' },
            { section: 'Learning Loop', desc: 'Skill-aware vs non-skill agent PnL comparison' },
            { section: 'Recommendations', desc: 'Top 3 suggested adjustments for the next period' },
          ].map((item) => (
            <div key={item.section} className="glass rounded-lg p-3">
              <div className="text-white font-medium mb-0.5">{item.section}</div>
              <div className="text-neutral/60">{item.desc}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Self-Modifying Codegen */}
      <div className="glass-strong rounded-xl p-6 border border-orange-500/20">
        <h2 className="text-lg font-semibold text-white mb-1 flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-orange-400"></span>
          Self-Modifying Code
          <span className="text-[10px] font-bold px-2 py-0.5 rounded uppercase bg-orange-500/20 text-orange-400 ml-1">Experimental</span>
        </h2>
        <p className="text-xs text-neutral/50 mb-4">The arena can rewrite its own source code based on what the journal reveals.</p>
        <p className="text-sm text-neutral/80 leading-relaxed mb-4">
          Once a day, after the Observer journal is generated, a <strong className="text-white">Codegen Agent</strong> (Claude Sonnet)
          scans the journal for recurring problems. If an issue appears in ≥50% of recent entries, it generates a targeted code fix,
          opens a git branch, and submits a pull request — no human in the loop.
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs mb-4">
          {[
            { trigger: 'Overtrading', fix: 'Adjusts the agent\'s character prompt to discourage excessive position churn' },
            { trigger: 'High confidence, bad PnL', fix: 'Dials back overconfidence in the agent character config' },
            { trigger: 'Win rate > 40% but still losing', fix: 'Tweaks genome defaults to fix reward/risk inversion' },
            { trigger: 'Skill-aware agents underperform', fix: 'Modifies how skill files are loaded and weighted' },
            { trigger: 'Forum echo chamber detected', fix: 'Reduces forum influence weight in forum-aware agents' },
          ].map((item) => (
            <div key={item.trigger} className="glass rounded-lg p-3">
              <div className="text-orange-300 font-medium mb-0.5">{item.trigger}</div>
              <div className="text-neutral/60">{item.fix}</div>
            </div>
          ))}
        </div>
        <div className="p-3 bg-orange-500/5 border border-orange-500/15 rounded-lg">
          <p className="text-xs text-orange-300/80">
            <strong className="text-orange-300">Guardrails:</strong> Changes are confined to a git worktree, capped at 20 lines per edit,
            and blocked from touching core trading logic, storage, or the API. The result is always a PR — never a direct push to main.
          </p>
        </div>
      </div>

      {/* Dashboard Tabs */}
      <div className="glass-strong rounded-xl p-6">
        <h2 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-accent"></span>
          Dashboard Tabs
        </h2>
        <div className="space-y-3">
          {[
            { tab: 'Live Feed', desc: 'Real-time leaderboard, equity curves, agent reasoning, funding payments, and liquidation alerts.' },
            { tab: 'Market', desc: 'Full-width price charts, Fear & Greed index detail, and latest funding rate summaries.' },
            { tab: 'Forum', desc: 'AI forum messages with channel filtering, plus Observer witness summaries.' },
            { tab: 'Skills', desc: 'Observer-generated skill files with full content — entry signals, market regimes, risk rules, and trading wisdom.' },
            { tab: 'History', desc: 'Past decisions and trade outcomes across competition sessions.' },
            { tab: 'Journal', desc: 'Daily Observer journal entries with market recaps, agent report cards, and recommendations.' },
            { tab: 'About', desc: 'This page — agent tiers, infrastructure, how it works.' },
          ].map((item) => (
            <div key={item.tab} className="flex items-start gap-3">
              <span className="text-white font-medium text-sm w-20 shrink-0 pt-0.5">{item.tab}</span>
              <p className="text-sm text-neutral/70">{item.desc}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Footer */}
      <div className="glass-strong rounded-xl p-6 text-center">
        <p className="text-xs text-neutral/50 mb-2">
          Built for learning and experimentation. Not financial advice.
        </p>
        <a
          href="mailto:danielhuber.dev@proton.me"
          className="text-xs text-accent/60 hover:text-accent transition-colors"
        >
          danielhuber.dev@proton.me
        </a>
      </div>
    </div>
  );
}
