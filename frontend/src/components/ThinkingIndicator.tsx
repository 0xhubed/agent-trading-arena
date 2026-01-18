import { motion } from 'framer-motion';
import clsx from 'clsx';

interface ThinkingIndicatorProps {
  size?: 'sm' | 'md';
  className?: string;
}

export default function ThinkingIndicator({ size = 'sm', className }: ThinkingIndicatorProps) {
  const dotSize = size === 'sm' ? 'w-1 h-1' : 'w-1.5 h-1.5';

  return (
    <div className={clsx('flex items-center gap-0.5', className)}>
      {[0, 1, 2].map((i) => (
        <motion.div
          key={i}
          className={clsx(dotSize, 'rounded-full bg-accent')}
          animate={{
            opacity: [0.3, 1, 0.3],
            scale: [0.8, 1, 0.8],
          }}
          transition={{
            duration: 0.8,
            repeat: Infinity,
            delay: i * 0.15,
            ease: 'easeInOut',
          }}
        />
      ))}
    </div>
  );
}

export function ThinkingBadge({ className }: { className?: string }) {
  return (
    <div className={clsx(
      'inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-accent/10 text-accent text-xs',
      className
    )}>
      <ThinkingIndicator size="sm" />
      <span className="font-medium">Thinking</span>
    </div>
  );
}
