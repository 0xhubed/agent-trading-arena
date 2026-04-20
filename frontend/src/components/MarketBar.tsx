import { useEffect, useState } from 'react';
import { useCompetitionStore } from '../stores/competition';
import clsx from 'clsx';

interface FearGreed {
  value: number | null;
  classification: string;
}

function fearGreedColor(value: number): string {
  if (value <= 25) return 'text-loss bg-loss/10';
  if (value <= 45) return 'text-orange-400 bg-orange-400/10';
  if (value <= 55) return 'text-neutral bg-white/5';
  if (value <= 75) return 'text-green-400 bg-green-400/10';
  return 'text-profit bg-profit/10';
}

export default function MarketBar() {
  const { market } = useCompetitionStore();
  const [fearGreed, setFearGreed] = useState<FearGreed | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function fetchFearGreed() {
      try {
        const res = await fetch('/api/fear-greed');
        if (res.ok) {
          const data = await res.json();
          if (!cancelled) setFearGreed(data);
        }
      } catch (err) {
        console.warn('Fear & Greed fetch failed:', err);
      }
    }

    fetchFearGreed();
    // Refresh every 10 minutes (matches API cache TTL)
    const interval = setInterval(fetchFearGreed, 600_000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  const symbols = Object.entries(market);

  if (symbols.length === 0) {
    return (
      <div className="glass-subtle border-b border-white/5 py-2 sm:py-3 px-4 sm:px-6">
        <div className="max-w-7xl mx-auto text-center text-neutral text-sm">
          <span className="animate-pulse-slow">Waiting for market data...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="glass-subtle border-b border-white/5 py-1.5 sm:py-2 px-3 sm:px-4 overflow-x-auto scrollbar-hide">
      <div className="max-w-7xl mx-auto flex items-center justify-start sm:justify-center gap-3 sm:gap-5 min-w-max sm:min-w-0">
        {fearGreed && fearGreed.value !== null && (
          <div
            title="Crypto Fear & Greed Index"
            className={clsx(
              'flex items-center gap-1.5 text-xs px-2 py-0.5 rounded-full border border-white/10',
              fearGreedColor(fearGreed.value)
            )}
          >
            <span className="font-medium">F&G</span>
            <span className="font-mono-numbers">{fearGreed.value}</span>
            <span className="hidden sm:inline opacity-70">{fearGreed.classification}</span>
          </div>
        )}
        {symbols.map(([symbol, data]) => (
          <div key={symbol} className="flex items-center gap-1.5 sm:gap-2">
            <span className="font-medium text-white text-xs sm:text-sm">{symbol.replace('USDT', '')}</span>
            <span className="font-mono-numbers text-xs sm:text-sm text-white">
              ${data.price.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}
            </span>
            <span
              className={clsx(
                'font-mono-numbers text-xs px-1.5 py-0.5 rounded-full',
                data.change_24h >= 0
                  ? 'text-profit bg-profit/10'
                  : 'text-loss bg-loss/10'
              )}
            >
              {data.change_24h >= 0 ? '+' : ''}
              {data.change_24h.toFixed(2)}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
