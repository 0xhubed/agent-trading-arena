import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import clsx from 'clsx';
import BiasPanel from './BiasPanel';
import ContagionPanel from './ContagionPanel';
import type { LabTab } from '../../types/lab';

const tabs: { id: LabTab; label: string; description: string }[] = [
  { id: 'bias', label: 'Bias Profiles', description: 'Per-agent behavioral biases' },
  { id: 'contagion', label: 'System Health', description: 'Contagion & echo chamber detection' },
];

export default function LabView() {
  const [activeTab, setActiveTab] = useState<LabTab>('bias');

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-xl font-bold text-white">Lab</h2>
        <p className="text-sm text-neutral mt-1">
          Diagnostic tools for the learning loop
        </p>
      </div>

      {/* Sub-tab navigation */}
      <div className="flex gap-2">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={clsx(
              'px-4 py-2 rounded-lg text-sm font-medium transition-all',
              activeTab === tab.id
                ? 'bg-accent/20 text-accent border border-accent/30'
                : 'glass text-neutral hover:text-white hover:bg-white/5'
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <AnimatePresence mode="wait">
        {activeTab === 'bias' ? (
          <motion.div
            key="bias"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            transition={{ duration: 0.2 }}
          >
            <BiasPanel />
          </motion.div>
        ) : (
          <motion.div
            key="contagion"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            transition={{ duration: 0.2 }}
          >
            <ContagionPanel />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
