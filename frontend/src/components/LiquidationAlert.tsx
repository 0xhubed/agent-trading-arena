import { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useCompetitionStore, Liquidation } from '../stores/competition';

function LiquidationToast({ liquidation, onDismiss }: { liquidation: Liquidation; onDismiss: () => void }) {
  const { agents } = useCompetitionStore();
  const agentName = agents.find((a) => a.id === liquidation.agent_id)?.name || liquidation.agent_id;

  useEffect(() => {
    const timer = setTimeout(onDismiss, 8000);
    return () => clearTimeout(timer);
  }, [onDismiss]);

  return (
    <motion.div
      initial={{ opacity: 0, y: -50, scale: 0.9 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      exit={{ opacity: 0, y: -20, scale: 0.9 }}
      className="glass-strong rounded-xl p-4 border-2 border-loss/50 shadow-2xl max-w-sm"
    >
      <div className="flex items-start gap-3">
        {/* Skull icon */}
        <div className="text-3xl animate-pulse">
          <span role="img" aria-label="liquidation">
            &#128128;
          </span>
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-xs font-bold text-loss uppercase tracking-wider animate-pulse">
              LIQUIDATED
            </span>
          </div>

          <div className="font-bold text-white truncate">{agentName}</div>

          <div className="mt-2 space-y-1 text-sm">
            <div className="flex justify-between">
              <span className="text-neutral">Symbol</span>
              <span className="font-medium">{liquidation.symbol}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-neutral">Side</span>
              <span className={liquidation.side === 'long' ? 'text-profit' : 'text-loss'}>
                {liquidation.side.toUpperCase()}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-neutral">Entry</span>
              <span className="font-mono-numbers">${liquidation.entry_price.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-neutral">Liq. Price</span>
              <span className="font-mono-numbers text-loss">
                ${liquidation.liquidation_price.toLocaleString()}
              </span>
            </div>
            <div className="flex justify-between border-t border-white/10 pt-1 mt-1">
              <span className="text-neutral">Total Loss</span>
              <span className="font-mono-numbers font-bold text-loss">
                -${liquidation.total_loss.toFixed(2)}
              </span>
            </div>
          </div>
        </div>

        <button
          onClick={onDismiss}
          className="text-neutral hover:text-white transition-colors p-1"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
    </motion.div>
  );
}

function LiquidationHistoryItem({ liquidation }: { liquidation: Liquidation }) {
  const { agents } = useCompetitionStore();
  const agentName = agents.find((a) => a.id === liquidation.agent_id)?.name || liquidation.agent_id;

  return (
    <div className="p-3 glass-subtle rounded-lg border border-loss/20">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-lg">&#128128;</span>
          <span className="font-medium text-sm truncate">{agentName}</span>
        </div>
        <span className="font-mono-numbers text-sm font-medium text-loss">
          -${liquidation.total_loss.toFixed(2)}
        </span>
      </div>
      <div className="mt-1.5 flex items-center gap-2 text-xs text-neutral">
        <span className="font-medium">{liquidation.symbol}</span>
        <span className="opacity-50">|</span>
        <span className={liquidation.side === 'long' ? 'text-profit' : 'text-loss'}>
          {liquidation.side.toUpperCase()}
        </span>
        <span className="opacity-50">|</span>
        <span>Tick {liquidation.tick}</span>
      </div>
    </div>
  );
}

export function LiquidationToastContainer() {
  const { liquidations } = useCompetitionStore();
  const [shownIds, setShownIds] = useState<Set<string>>(new Set());
  const [activeToasts, setActiveToasts] = useState<Liquidation[]>([]);

  // Track new liquidations and show toasts
  useEffect(() => {
    const newLiquidations = liquidations.filter((l) => {
      const id = `${l.agent_id}-${l.symbol}-${l.tick}`;
      return !shownIds.has(id);
    });

    if (newLiquidations.length > 0) {
      setActiveToasts((prev) => [...newLiquidations, ...prev].slice(0, 3));
      setShownIds((prev) => {
        const newSet = new Set(prev);
        newLiquidations.forEach((l) => {
          newSet.add(`${l.agent_id}-${l.symbol}-${l.tick}`);
        });
        return newSet;
      });
    }
  }, [liquidations, shownIds]);

  const dismissToast = (liquidation: Liquidation) => {
    setActiveToasts((prev) =>
      prev.filter(
        (l) =>
          !(l.agent_id === liquidation.agent_id && l.symbol === liquidation.symbol && l.tick === liquidation.tick)
      )
    );
  };

  return (
    <div className="fixed top-20 right-4 z-50 space-y-3">
      <AnimatePresence>
        {activeToasts.map((liquidation) => (
          <LiquidationToast
            key={`${liquidation.agent_id}-${liquidation.symbol}-${liquidation.tick}`}
            liquidation={liquidation}
            onDismiss={() => dismissToast(liquidation)}
          />
        ))}
      </AnimatePresence>
    </div>
  );
}

export default function LiquidationHistory() {
  const { liquidations } = useCompetitionStore();

  // Get recent liquidations (last 10)
  const recentLiquidations = liquidations.slice(0, 10);

  // Calculate total losses
  const totalLosses = liquidations.reduce((sum, l) => sum + l.total_loss, 0);

  return (
    <div className="glass-strong rounded-xl p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="font-semibold text-sm flex items-center gap-2">
          <span>&#128128;</span>
          Liquidations
        </h3>
        {liquidations.length > 0 && (
          <div className="flex items-center gap-3 text-xs">
            <span className="text-neutral">
              Total:{' '}
              <span className="font-mono-numbers font-medium text-loss">
                -${totalLosses.toFixed(2)}
              </span>
            </span>
          </div>
        )}
      </div>

      {recentLiquidations.length === 0 ? (
        <div className="text-center text-neutral text-sm py-6">
          No liquidations yet
        </div>
      ) : (
        <div className="space-y-2 max-h-[200px] overflow-y-auto scrollbar-thin">
          {recentLiquidations.map((liquidation, index) => (
            <LiquidationHistoryItem
              key={`${liquidation.agent_id}-${liquidation.symbol}-${liquidation.tick}-${index}`}
              liquidation={liquidation}
            />
          ))}
        </div>
      )}
    </div>
  );
}
