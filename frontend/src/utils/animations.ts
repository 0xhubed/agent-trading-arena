import { Variants } from 'framer-motion';

// Page transitions
export const pageTransition: Variants = {
  initial: { opacity: 0 },
  animate: { opacity: 1, transition: { duration: 0.3 } },
  exit: { opacity: 0, transition: { duration: 0.2 } },
};

export const slideUp: Variants = {
  initial: { opacity: 0, y: 20 },
  animate: { opacity: 1, y: 0, transition: { duration: 0.3, ease: 'easeOut' } },
  exit: { opacity: 0, y: -10, transition: { duration: 0.2 } },
};

export const slideInFromRight: Variants = {
  initial: { opacity: 0, x: 20 },
  animate: { opacity: 1, x: 0, transition: { duration: 0.3, ease: 'easeOut' } },
  exit: { opacity: 0, x: 20, transition: { duration: 0.2 } },
};

// List animations with stagger
export const staggerContainer: Variants = {
  initial: { opacity: 0 },
  animate: {
    opacity: 1,
    transition: {
      staggerChildren: 0.05,
      delayChildren: 0.1,
    },
  },
};

export const listItem: Variants = {
  initial: { opacity: 0, x: -10 },
  animate: {
    opacity: 1,
    x: 0,
    transition: { duration: 0.2, ease: 'easeOut' },
  },
};

// Scale animations
export const scaleIn: Variants = {
  initial: { opacity: 0, scale: 0.95 },
  animate: {
    opacity: 1,
    scale: 1,
    transition: { duration: 0.2, ease: 'easeOut' },
  },
};

export const popIn: Variants = {
  initial: { opacity: 0, scale: 0.8 },
  animate: {
    opacity: 1,
    scale: 1,
    transition: { type: 'spring', stiffness: 300, damping: 20 },
  },
};

// Feed item animation (for new items appearing)
export const feedItem: Variants = {
  initial: { opacity: 0, height: 0, marginBottom: 0 },
  animate: {
    opacity: 1,
    height: 'auto',
    marginBottom: 12,
    transition: { duration: 0.3, ease: 'easeOut' },
  },
  exit: {
    opacity: 0,
    height: 0,
    marginBottom: 0,
    transition: { duration: 0.2 },
  },
};

// Pulse animation for emphasis
export const pulse: Variants = {
  initial: { scale: 1 },
  animate: {
    scale: [1, 1.02, 1],
    transition: { duration: 0.3 },
  },
};

// Glow animation for highlights
export const glow: Variants = {
  initial: { boxShadow: '0 0 0 rgba(99, 102, 241, 0)' },
  animate: {
    boxShadow: [
      '0 0 0 rgba(99, 102, 241, 0)',
      '0 0 20px rgba(99, 102, 241, 0.3)',
      '0 0 0 rgba(99, 102, 241, 0)',
    ],
    transition: { duration: 1.5, repeat: Infinity },
  },
};

// Number change animation
export const numberChange: Variants = {
  initial: { opacity: 0.6, scale: 0.98 },
  animate: {
    opacity: 1,
    scale: 1,
    transition: { duration: 0.15 },
  },
};
