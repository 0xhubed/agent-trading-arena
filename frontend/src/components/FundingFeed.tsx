import { forwardRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useCompetitionStore, FundingPayment } from '../stores/competition';
import clsx from 'clsx';

function formatFunding(amount: number, showSign = true): string {
  const absAmount = Math.abs(amount);
  // Avoid showing -$0.00 for tiny amounts
  if (absAmount < 0.005) {
    return '$0.00';
  }
  const formatted = absAmount.toFixed(2);
  if (!showSign) return `$${formatted}`;
  return amount >= 0 ? `+$${formatted}` : `-$${formatted}`;
}

const FundingItem = forwardRef<HTMLDivElement, { payment: FundingPayment }>(
  ({ payment }, ref) => {
    const { agents } = useCompetitionStore();
    const agentName = agents.find((a) => a.id === payment.agent_id)?.name || payment.agent_id;
    const isReceived = payment.direction === 'received';
    const displayAmount = isReceived ? payment.amount : -Math.abs(payment.amount);

    return (
      <motion.div
        ref={ref}
        initial={{ opacity: 0, x: -20 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: 20 }}
        className="p-3 glass-subtle rounded-lg border border-white/5"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <span
              className={clsx(
                'text-xs font-medium px-1.5 py-0.5 rounded',
                isReceived ? 'bg-profit/20 text-profit' : 'bg-loss/20 text-loss'
              )}
            >
              {isReceived ? 'RECV' : 'PAID'}
            </span>
            <span className="font-medium text-sm truncate">{agentName}</span>
          </div>
          <span
            className={clsx(
              'font-mono-numbers text-sm font-medium',
              isReceived ? 'text-profit' : 'text-loss'
            )}
          >
            {formatFunding(displayAmount)}
          </span>
        </div>
        <div className="mt-1.5 flex items-center gap-2 text-xs text-neutral">
          <span className="font-medium">{payment.symbol}</span>
          <span className="opacity-50">|</span>
          <span className={payment.side === 'long' ? 'text-profit' : 'text-loss'}>
            {payment.side.toUpperCase()}
          </span>
          <span className="opacity-50">|</span>
          <span>Rate: {(payment.funding_rate * 100).toFixed(4)}%</span>
        </div>
      </motion.div>
    );
  }
);

export default function FundingFeed() {
  const { fundingPayments } = useCompetitionStore();

  // Get recent payments (last 10)
  const recentPayments = fundingPayments.slice(0, 10);

  // Calculate summary stats
  const totalPaid = fundingPayments
    .filter((p) => p.direction === 'paid')
    .reduce((sum, p) => sum + Math.abs(p.amount), 0);
  const totalReceived = fundingPayments
    .filter((p) => p.direction === 'received')
    .reduce((sum, p) => sum + p.amount, 0);
  const netFunding = totalReceived - totalPaid;

  return (
    <div className="glass-strong rounded-xl p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="font-semibold text-sm flex items-center gap-2">
          <span className="text-accent">$</span>
          Funding Payments
        </h3>
        <div className="flex items-center gap-3 text-xs">
          <span className="text-neutral">
            Net:{' '}
            <span
              className={clsx(
                'font-mono-numbers font-medium',
                Math.abs(netFunding) < 0.005 ? 'text-neutral' :
                netFunding >= 0 ? 'text-profit' : 'text-loss'
              )}
            >
              {formatFunding(netFunding)}
            </span>
          </span>
        </div>
      </div>

      {recentPayments.length === 0 ? (
        <div className="text-center text-neutral text-sm py-6">
          No funding payments yet
        </div>
      ) : (
        <div className="space-y-2 max-h-[300px] overflow-y-auto scrollbar-thin">
          <AnimatePresence mode="popLayout">
            {recentPayments.map((payment, index) => (
              <FundingItem
                key={`${payment.agent_id}-${payment.symbol}-${payment.tick}-${index}`}
                payment={payment}
              />
            ))}
          </AnimatePresence>
        </div>
      )}
    </div>
  );
}
