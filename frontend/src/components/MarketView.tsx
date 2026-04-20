import { useState, useEffect, useMemo } from 'react';
import { useCompetitionStore } from '../stores/competition';
import type { FundingPayment } from '../stores/competition';
import clsx from 'clsx';
import MarketHistory from './MarketHistory';

interface FearGreed {
  value: number | null;
  classification: string;
}

function fearGreedColor(value: number): string {
  if (value <= 25) return 'text-loss';
  if (value <= 45) return 'text-orange-400';
  if (value <= 55) return 'text-neutral';
  if (value <= 75) return 'text-green-400';
  return 'text-profit';
}

function fearGreedBg(value: number): string {
  if (value <= 25) return 'bg-loss/20';
  if (value <= 45) return 'bg-orange-400/20';
  if (value <= 55) return 'bg-white/5';
  if (value <= 75) return 'bg-green-400/20';
  return 'bg-profit/20';
}

function gaugeRotation(value: number): number {
  // Map 0-100 to -90deg to 90deg
  return (value / 100) * 180 - 90;
}

export default function MarketView() {
  const { fundingPayments } = useCompetitionStore();
  const [fearGreed, setFearGreed] = useState<FearGreed | null>(null);
  const [loadingFG, setLoadingFG] = useState(true);

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
      } finally {
        if (!cancelled) setLoadingFG(false);
      }
    }
    fetchFearGreed();
    const interval = setInterval(fetchFearGreed, 600_000);
    return () => { cancelled = true; clearInterval(interval); };
  }, []);

  // Group funding payments by symbol, showing latest rate and direction
  const fundingSummary = useMemo(() => {
    const bySymbol: Record<string, FundingPayment> = {};
    for (const payment of fundingPayments) {
      if (!bySymbol[payment.symbol] || payment.timestamp > bySymbol[payment.symbol].timestamp) {
        bySymbol[payment.symbol] = payment;
      }
    }
    return Object.entries(bySymbol).sort(([a], [b]) => a.localeCompare(b));
  }, [fundingPayments]);

  return (
    <div className="space-y-6">
      {/* Market History chart - full width, always expanded */}
      <MarketHistory defaultExpanded collapsible={false} chartHeight="h-80" />

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Fear & Greed Panel */}
        <div className="glass-strong rounded-xl p-6">
          <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-accent" />
            Fear & Greed Index
          </h3>
          {loadingFG ? (
            <div className="flex items-center justify-center h-32 text-neutral">
              <div className="w-4 h-4 border-2 border-accent border-t-transparent rounded-full animate-spin" />
            </div>
          ) : fearGreed && fearGreed.value !== null ? (
            <div className="flex flex-col items-center gap-4">
              {/* Gauge */}
              <div className="relative w-40 h-20 overflow-hidden">
                <div className="absolute inset-0 rounded-t-full border-4 border-white/10" />
                <div
                  className="absolute bottom-0 left-1/2 w-1 h-16 origin-bottom rounded-full bg-white transition-transform duration-700"
                  style={{ transform: `translateX(-50%) rotate(${gaugeRotation(fearGreed.value)}deg)` }}
                />
                <div className="absolute bottom-0 left-1/2 -translate-x-1/2 w-3 h-3 rounded-full bg-white" />
              </div>
              {/* Value + classification */}
              <div className="text-center">
                <div className={clsx('text-4xl font-bold font-mono-numbers', fearGreedColor(fearGreed.value))}>
                  {fearGreed.value}
                </div>
                <div className={clsx(
                  'text-sm font-medium mt-1 px-3 py-1 rounded-full inline-block',
                  fearGreedBg(fearGreed.value),
                  fearGreedColor(fearGreed.value)
                )}>
                  {fearGreed.classification}
                </div>
              </div>
              <p className="text-xs text-neutral/60 text-center">
                Crypto Fear & Greed Index (0 = Extreme Fear, 100 = Extreme Greed)
              </p>
            </div>
          ) : (
            <div className="flex items-center justify-center h-32 text-neutral text-sm">
              Fear & Greed data unavailable
            </div>
          )}
        </div>

        {/* Funding Rates Summary */}
        <div className="glass-strong rounded-xl p-6">
          <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-highlight" />
            Latest Funding Rates
          </h3>
          {fundingSummary.length === 0 ? (
            <div className="flex items-center justify-center h-32 text-neutral text-sm">
              No funding data yet
            </div>
          ) : (
            <div className="space-y-3">
              {fundingSummary.map(([symbol, payment]) => (
                <div
                  key={symbol}
                  className="flex items-center justify-between p-3 bg-surface/30 rounded-lg border border-white/5"
                >
                  <span className="text-sm font-medium text-white">
                    {symbol.replace('USDT', '')}
                  </span>
                  <div className={clsx(
                    'text-sm font-mono-numbers font-medium',
                    payment.funding_rate >= 0 ? 'text-profit' : 'text-loss'
                  )}>
                    {payment.funding_rate >= 0 ? '+' : ''}
                    {(payment.funding_rate * 100).toFixed(4)}%
                  </div>
                </div>
              ))}
              <p className="text-xs text-neutral/60 pt-1">
                Positive rate: longs pay, shorts receive. Updated each tick.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
