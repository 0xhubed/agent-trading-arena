import { motion, AnimatePresence } from 'framer-motion';
import { useCompetitionStore } from '../stores/competition';
import clsx from 'clsx';
import AgentAvatar from './AgentAvatar';
import { feedItem } from '../utils/animations';

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);

  if (diffSec < 60) return 'just now';
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return date.toLocaleDateString();
}

function getActionStyles(action: string): { text: string; bg: string; glow: string } {
  switch (action) {
    case 'open_long':
      return { text: 'text-profit', bg: 'bg-profit/10', glow: 'glow-profit' };
    case 'open_short':
      return { text: 'text-loss', bg: 'bg-loss/10', glow: 'glow-loss' };
    case 'close':
      return { text: 'text-highlight', bg: 'bg-highlight/10', glow: 'glow-highlight' };
    default:
      return { text: 'text-neutral', bg: 'bg-neutral/10', glow: '' };
  }
}

function getActionLabel(action: string): string {
  switch (action) {
    case 'open_long':
      return 'LONG';
    case 'open_short':
      return 'SHORT';
    case 'close':
      return 'CLOSE';
    default:
      return 'HOLD';
  }
}

function formatAgentName(name: string): string {
  // Remove "The " prefix for cleaner display
  if (name.startsWith('The ')) {
    return name.slice(4);
  }
  return name;
}

function formatReasoning(reasoning: string): JSX.Element {
  if (!reasoning) {
    return <span className="text-neutral/50">No reasoning provided</span>;
  }

  // Split reasoning into sentences for better readability
  const sentences = reasoning.split(/(?<=[.!?])\s+/);

  // If it's a long reasoning (more than 2 sentences), format as bullet points
  if (sentences.length > 2) {
    return (
      <ul className="list-none space-y-1">
        {sentences.slice(0, 4).map((sentence, i) => {
          const formatted = highlightText(sentence.trim());
          return (
            <li key={i} className="flex gap-2">
              <span className="text-accent/60 flex-shrink-0">•</span>
              <span>{formatted}</span>
            </li>
          );
        })}
        {sentences.length > 4 && (
          <li className="text-neutral/50 text-xs ml-4">
            +{sentences.length - 4} more...
          </li>
        )}
      </ul>
    );
  }

  return <span>{highlightText(reasoning)}</span>;
}

function highlightText(text: string): JSX.Element {
  // Simple regex-based highlighting
  const parts: (string | JSX.Element)[] = [];
  let lastIndex = 0;
  let key = 0;

  // Combined pattern for all highlights
  const combinedPattern = /\b([A-Z]{2,}USDT?)\b|([+-]?\d+\.?\d*%)|(\b(?:bullish|bearish|long|short|momentum|trend|RSI|MACD|overbought|oversold)\b)/gi;

  let match;
  while ((match = combinedPattern.exec(text)) !== null) {
    // Add text before match
    if (match.index > lastIndex) {
      parts.push(text.slice(lastIndex, match.index));
    }

    const matchedText = match[0];
    const isSymbol = match[1];
    const isPercent = match[2];

    if (isSymbol) {
      parts.push(
        <span key={key++} className="font-mono text-highlight font-medium">
          {matchedText}
        </span>
      );
    } else if (isPercent) {
      const isPositive = !matchedText.startsWith('-');
      parts.push(
        <span
          key={key++}
          className={clsx(
            'font-mono font-medium',
            isPositive ? 'text-profit' : 'text-loss'
          )}
        >
          {matchedText}
        </span>
      );
    } else {
      // Trading term
      const lowerTerm = matchedText.toLowerCase();
      const isBullish = ['bullish', 'long', 'momentum'].includes(lowerTerm);
      const isBearish = ['bearish', 'short', 'overbought'].includes(lowerTerm);
      parts.push(
        <span
          key={key++}
          className={clsx(
            'font-medium',
            isBullish ? 'text-profit/80' : isBearish ? 'text-loss/80' : 'text-accent/80'
          )}
        >
          {matchedText}
        </span>
      );
    }

    lastIndex = match.index + matchedText.length;
  }

  // Add remaining text
  if (lastIndex < text.length) {
    parts.push(text.slice(lastIndex));
  }

  return <>{parts}</>;
}

export default function ReasoningFeed() {
  const { recentDecisions, agents } = useCompetitionStore();

  const getAgentName = (agentId: string) => {
    return agents.find((a) => a.id === agentId)?.name || agentId;
  };

  return (
    <div className="glass-strong rounded-xl p-4 sm:p-6">
      <h2 className="text-lg font-semibold mb-4 text-white flex items-center gap-2">
        <span className="w-1.5 h-1.5 rounded-full bg-highlight"></span>
        Live Reasoning
      </h2>

      {recentDecisions.length === 0 ? (
        <div className="text-center text-neutral py-8">
          <div className="w-12 h-12 mx-auto mb-3 rounded-full bg-surface flex items-center justify-center">
            <span className="text-2xl animate-pulse-slow">💭</span>
          </div>
          Waiting for agent decisions...
        </div>
      ) : (
        <div className="space-y-3 sm:space-y-4 max-h-64 sm:max-h-96 overflow-y-auto pr-2">
          <AnimatePresence initial={false}>
            {[...recentDecisions]
              .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
              .map((item, index) => {
              const actionStyles = getActionStyles(item.decision.action);
              const isTradeAction = item.decision.action !== 'hold';

              return (
                <motion.div
                  key={`${item.agent_id}-${item.timestamp}-${index}`}
                  variants={feedItem}
                  initial="initial"
                  animate="animate"
                  exit="exit"
                  layout
                className={clsx(
                  'p-3 sm:p-4 rounded-lg border transition-all duration-200',
                  isTradeAction
                    ? `bg-surface/80 border-white/10 ${actionStyles.glow}`
                    : 'bg-surface/50 border-white/5'
                )}
              >
                {/* Header */}
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <AgentAvatar agentId={item.agent_id} size={28} />
                    <span className="font-medium text-white text-sm sm:text-base" title={item.agent_name || getAgentName(item.agent_id)}>
                      {formatAgentName(item.agent_name || getAgentName(item.agent_id))}
                    </span>
                    <span className="text-xs text-neutral">
                      {formatTimestamp(item.timestamp)}
                    </span>
                  </div>
                  <span
                    className={clsx(
                      'text-xs font-mono-numbers font-medium px-2 py-1 rounded-full',
                      item.decision.confidence >= 0.7
                        ? 'bg-profit/20 text-profit'
                        : item.decision.confidence >= 0.4
                        ? 'bg-neutral/20 text-neutral'
                        : 'bg-loss/20 text-loss'
                    )}
                  >
                    {Math.round(item.decision.confidence * 100)}%
                  </span>
                </div>

                {/* Reasoning */}
                <div className="text-sm text-neutral/90 mb-3 leading-relaxed">
                  {formatReasoning(item.decision.reasoning)}
                </div>

                {/* Action badge */}
                <div className="flex items-center gap-2">
                  <span
                    className={clsx(
                      'text-xs font-bold px-2.5 py-1 rounded-full',
                      actionStyles.text,
                      actionStyles.bg
                    )}
                  >
                    {getActionLabel(item.decision.action)}
                  </span>
                  {item.decision.symbol && (
                    <span className="text-xs text-neutral font-mono-numbers">
                      {item.decision.symbol}
                    </span>
                  )}
                </div>
              </motion.div>
            );
          })}
        </AnimatePresence>
        </div>
      )}
    </div>
  );
}
