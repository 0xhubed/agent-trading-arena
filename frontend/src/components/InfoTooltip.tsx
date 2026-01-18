import clsx from 'clsx';

interface InfoTooltipProps {
  content: string | React.ReactNode;
  position?: 'top' | 'bottom' | 'left' | 'right';
  maxWidth?: number;
}

export function InfoTooltip({ content, position = 'top', maxWidth = 280 }: InfoTooltipProps) {
  return (
    <span className="relative inline-flex items-center group ml-1.5 align-middle">
      {/* Info icon */}
      <svg
        className="w-3.5 h-3.5 text-neutral/40 hover:text-accent cursor-help transition-colors"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
      >
        <circle cx="12" cy="12" r="10" strokeWidth={2} />
        <path strokeLinecap="round" strokeWidth={2} d="M12 16v-4M12 8h.01" />
      </svg>

      {/* Tooltip popup */}
      <div
        className={clsx(
          'absolute z-50 invisible group-hover:visible opacity-0 group-hover:opacity-100',
          'transition-all duration-200 pointer-events-none',
          'bg-[#1a1a2e] border border-white/10 rounded-lg shadow-xl p-3',
          'text-xs text-neutral leading-relaxed font-normal',
          position === 'top' && 'bottom-full mb-2 left-1/2 -translate-x-1/2',
          position === 'bottom' && 'top-full mt-2 left-1/2 -translate-x-1/2',
          position === 'left' && 'right-full mr-2 top-1/2 -translate-y-1/2',
          position === 'right' && 'left-full ml-2 top-1/2 -translate-y-1/2'
        )}
        style={{ maxWidth, minWidth: 200 }}
      >
        {content}
        {/* Arrow */}
        <div
          className={clsx(
            'absolute w-2 h-2 bg-[#1a1a2e] rotate-45',
            position === 'top' && 'bottom-[-5px] left-1/2 -translate-x-1/2 border-r border-b border-white/10',
            position === 'bottom' && 'top-[-5px] left-1/2 -translate-x-1/2 border-l border-t border-white/10',
            position === 'left' && 'right-[-5px] top-1/2 -translate-y-1/2 border-t border-r border-white/10',
            position === 'right' && 'left-[-5px] top-1/2 -translate-y-1/2 border-b border-l border-white/10'
          )}
        />
      </div>
    </span>
  );
}

// Trading glossary with explanations
export const GLOSSARY = {
  // Performance Metrics
  winRate: (
    <div>
      <div className="font-semibold text-white mb-1">Win Rate</div>
      <div className="mb-2">Percentage of closed trades that were profitable.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div>• 50%+ is generally good</div>
        <div>• Low win rate can still be profitable with good R:R</div>
      </div>
    </div>
  ),

  sharpeRatio: (
    <div>
      <div className="font-semibold text-white mb-1">Sharpe Ratio</div>
      <div className="mb-2">Risk-adjusted return - how much return per unit of volatility.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div>• &lt; 0: Losing money</div>
        <div>• 0-1: Suboptimal</div>
        <div>• 1-2: Good</div>
        <div>• &gt; 2: Excellent</div>
      </div>
    </div>
  ),

  maxDrawdown: (
    <div>
      <div className="font-semibold text-white mb-1">Maximum Drawdown</div>
      <div className="mb-2">Largest peak-to-trough decline in equity. Shows worst-case scenario.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div>• 10-20%: Moderate risk</div>
        <div>• &gt; 30%: High risk strategy</div>
      </div>
    </div>
  ),

  profitFactor: (
    <div>
      <div className="font-semibold text-white mb-1">Profit Factor</div>
      <div className="mb-2">Gross profits divided by gross losses.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div>• &lt; 1.0: Losing strategy</div>
        <div>• 1.5-2.0: Good</div>
        <div>• &gt; 2.0: Excellent</div>
      </div>
    </div>
  ),

  expectancy: (
    <div>
      <div className="font-semibold text-white mb-1">Expectancy</div>
      <div className="mb-2">Average profit/loss per trade.</div>
      <div className="text-white/60 text-[10px]">
        = (Win% × Avg Win) - (Loss% × Avg Loss)
      </div>
    </div>
  ),

  avgWin: (
    <div>
      <div className="font-semibold text-white mb-1">Average Win</div>
      <div>Mean profit on winning trades. Compare with Avg Loss for risk/reward assessment.</div>
    </div>
  ),

  avgLoss: (
    <div>
      <div className="font-semibold text-white mb-1">Average Loss</div>
      <div>Mean loss on losing trades. Should ideally be smaller than Avg Win.</div>
    </div>
  ),

  totalTrades: (
    <div>
      <div className="font-semibold text-white mb-1">Round-trips</div>
      <div>Completed trades (positions opened and closed). Each round-trip has a realized P&L.</div>
    </div>
  ),

  // Fees & Costs
  totalFees: (
    <div>
      <div className="font-semibold text-white mb-1">Total Fees Paid</div>
      <div className="mb-2">Sum of all transaction fees deducted from P&L.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div>• Taker: 0.04% per trade</div>
        <div>• Maker: 0.02% (limit orders)</div>
        <div>• Liquidation: 0.5% penalty</div>
      </div>
    </div>
  ),

  netFunding: (
    <div>
      <div className="font-semibold text-white mb-1">Net Funding</div>
      <div className="mb-2">Funding rate payments received minus paid.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div className="text-profit">+ Positive: Earned from funding</div>
        <div className="text-loss">- Negative: Paid in funding</div>
      </div>
      <div className="mt-2 text-white/60 text-[10px]">
        Longs pay shorts when rate is positive, and vice versa.
      </div>
    </div>
  ),

  totalCosts: (
    <div>
      <div className="font-semibold text-white mb-1">Total Costs</div>
      <div>Combined trading fees + negative funding payments. Shows true cost of trading activity.</div>
    </div>
  ),

  liquidations: (
    <div>
      <div className="font-semibold text-white mb-1">Liquidations</div>
      <div className="mb-2">Forced position closures when margin is insufficient.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div>• Entire position margin is lost</div>
        <div>• Additional 0.5% fee charged</div>
        <div>• Good strategies avoid these</div>
      </div>
    </div>
  ),

  // Behavioral
  positionBias: (
    <div>
      <div className="font-semibold text-white mb-1">Position Bias</div>
      <div>Ratio of long vs short positions taken. Shows directional tendency of the strategy.</div>
    </div>
  ),

  avgLeverage: (
    <div>
      <div className="font-semibold text-white mb-1">Average Leverage</div>
      <div className="mb-2">Mean leverage used across trades. Higher = more risk.</div>
      <div className="text-white/60 text-[10px]">
        Max allowed: 10x
      </div>
    </div>
  ),

  confidence: (
    <div>
      <div className="font-semibold text-white mb-1">Confidence Stats</div>
      <div>Self-reported confidence level from the AI agent's decisions (0-100%).</div>
    </div>
  ),

  // Charts
  equityCurve: (
    <div>
      <div className="font-semibold text-white mb-1">Equity Curve</div>
      <div className="mb-2">Portfolio value over time including unrealized P&L.</div>
      <div className="text-white/60 text-[10px] space-y-0.5">
        <div>• Oscillations = leveraged positions moving with price</div>
        <div>• Smooth = conservative or well-hedged</div>
        <div>• Volatile = concentrated/leveraged exposure</div>
      </div>
    </div>
  ),
};

export default InfoTooltip;
