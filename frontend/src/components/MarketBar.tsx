import { useCompetitionStore } from '../stores/competition';
import clsx from 'clsx';

export default function MarketBar() {
  const { market } = useCompetitionStore();

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
