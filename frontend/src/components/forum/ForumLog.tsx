import { motion, AnimatePresence } from 'framer-motion';
import { useState, useEffect } from 'react';
import clsx from 'clsx';
import { feedItem } from '../../utils/animations';

interface ForumMessage {
  id: string;
  channel: string;
  agent_id: string;
  agent_name: string;
  agent_type: 'discussion' | 'trading';
  content: string;
  reply_to?: string | null;
  metadata: Record<string, any>;
  created_at: string;
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);

  if (diffSec < 60) return 'just now';
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}

function getAgentTypeStyles(agentType: string): { bg: string; text: string; border: string } {
  switch (agentType) {
    case 'discussion':
      return { bg: 'bg-accent/10', text: 'text-accent', border: 'border-accent/30' };
    case 'trading':
      return { bg: 'bg-profit/10', text: 'text-profit', border: 'border-profit/30' };
    default:
      return { bg: 'bg-neutral/10', text: 'text-neutral', border: 'border-neutral/20' };
  }
}

function getChannelColor(channel: string): string {
  switch (channel) {
    case 'market':
      return 'text-highlight';
    case 'strategy':
      return 'text-accent';
    default:
      return 'text-neutral';
  }
}

/** Highlight trading terms, symbols, and percentages */
function highlightText(text: string): (string | JSX.Element)[] | JSX.Element {
  const parts: (string | JSX.Element)[] = [];
  let lastIndex = 0;
  let key = 0;

  const pattern = /\b([A-Z]{2,}USDT?)\b|([+-]?\d+\.?\d*%)|(\b(?:bullish|bearish|long|short|RSI|SMA|MACD|resistance|support|funding|overbought|oversold|trending|ranging|liquidation|stop.?loss|take.?profit)\b)/gi;

  let match;
  while ((match = pattern.exec(text)) !== null) {
    if (match.index > lastIndex) {
      parts.push(text.slice(lastIndex, match.index));
    }
    const matched = match[0];
    parts.push(
      <span key={key++} className="text-foreground font-medium">
        {matched}
      </span>
    );
    lastIndex = match.index + matched.length;
  }

  if (lastIndex < text.length) {
    parts.push(text.slice(lastIndex));
  }

  return parts.length > 0 ? parts : <>{text}</>;
}

/** Process inline bold (**text**) within a text string */
function processInlineBold(text: string, keyPrefix: string): (string | JSX.Element)[] {
  const parts: (string | JSX.Element)[] = [];
  const boldPattern = /\*\*(.+?)\*\*/g;
  let lastIndex = 0;
  let match;
  let key = 0;

  while ((match = boldPattern.exec(text)) !== null) {
    if (match.index > lastIndex) {
      const before = text.slice(lastIndex, match.index);
      const highlighted = highlightText(before);
      if (Array.isArray(highlighted)) {
        parts.push(...highlighted);
      } else {
        parts.push(highlighted);
      }
    }
    parts.push(
      <strong key={`${keyPrefix}-b${key++}`} className="text-foreground font-semibold">
        {match[1]}
      </strong>
    );
    lastIndex = match.index + match[0].length;
  }

  if (lastIndex < text.length) {
    const remaining = text.slice(lastIndex);
    const highlighted = highlightText(remaining);
    if (Array.isArray(highlighted)) {
      parts.push(...highlighted);
    } else {
      parts.push(highlighted);
    }
  }

  return parts;
}

/** Rich content formatter with markdown-like syntax */
function formatContent(content: string): JSX.Element {
  const lines = content.split('\n');
  const elements: JSX.Element[] = [];

  lines.forEach((line, i) => {
    const trimmed = line.trim();

    // ### Subheadings
    if (trimmed.startsWith('### ')) {
      elements.push(
        <h4 key={i} className="text-sm font-bold text-foreground mt-3 mb-1">
          {trimmed.slice(4)}
        </h4>
      );
    }
    // ## Headings
    else if (trimmed.startsWith('## ')) {
      elements.push(
        <h3 key={i} className="text-base font-bold text-foreground mt-3 mb-1">
          {trimmed.slice(3)}
        </h3>
      );
    }
    // Horizontal rules
    else if (/^[-*_]{3,}$/.test(trimmed)) {
      elements.push(
        <hr key={i} className="border-white/10 my-3" />
      );
    }
    // Full-line bold as section header
    else if (trimmed.startsWith('**') && trimmed.endsWith('**') && !trimmed.slice(2, -2).includes('**')) {
      elements.push(
        <div key={i} className="font-semibold text-foreground mt-3 mb-1">
          {trimmed.slice(2, -2)}
        </div>
      );
    }
    // Bullet points
    else if (trimmed.startsWith('- ') || trimmed.startsWith('* ')) {
      const text = trimmed.slice(2);
      elements.push(
        <div key={i} className="flex gap-2 ml-3 py-0.5">
          <span className="text-accent/50 flex-shrink-0 mt-0.5">•</span>
          <span className="text-neutral/90 leading-relaxed">
            {processInlineBold(text, `li-${i}`)}
          </span>
        </div>
      );
    }
    // Numbered lists
    else if (/^\d+[.)]\s/.test(trimmed)) {
      const numMatch = trimmed.match(/^(\d+[.)]\s)(.*)/);
      if (numMatch) {
        elements.push(
          <div key={i} className="flex gap-2 ml-3 py-0.5">
            <span className="text-accent/50 flex-shrink-0 font-mono text-xs mt-0.5 w-5 text-right">
              {numMatch[1].trim()}
            </span>
            <span className="text-neutral/90 leading-relaxed">
              {processInlineBold(numMatch[2], `ol-${i}`)}
            </span>
          </div>
        );
      }
    }
    // Code blocks (inline backticks)
    else if (trimmed.includes('`')) {
      const formatted = trimmed.split('`').map((part, idx) =>
        idx % 2 === 1 ? (
          <code key={idx} className="bg-background/60 px-1.5 py-0.5 rounded text-accent text-sm font-mono">
            {part}
          </code>
        ) : (
          <span key={idx}>{processInlineBold(part, `code-${i}-${idx}`)}</span>
        )
      );
      elements.push(
        <div key={i} className="text-neutral/90 leading-relaxed py-0.5">
          {formatted}
        </div>
      );
    }
    // Warning/Alert lines
    else if (trimmed.startsWith('⚠️') || trimmed.startsWith('⚡')) {
      elements.push(
        <div key={i} className="text-warning font-medium py-1 px-3 bg-warning/5 rounded mt-1 mb-1">
          {trimmed}
        </div>
      );
    }
    // Regular text with inline bold
    else if (trimmed) {
      elements.push(
        <p key={i} className="text-neutral/90 leading-relaxed py-0.5">
          {processInlineBold(trimmed, `p-${i}`)}
        </p>
      );
    }
    // Empty lines → spacing
    else if (i > 0 && i < lines.length - 1) {
      elements.push(<div key={i} className="h-2" />);
    }
  });

  return <div className="space-y-0.5">{elements}</div>;
}

/** Preview length before collapsing */
const COLLAPSE_THRESHOLD = 400;

function MessageCard({ message }: { message: ForumMessage }) {
  const [expanded, setExpanded] = useState(false);
  const isLong = message.content.length > COLLAPSE_THRESHOLD;
  const typeStyles = getAgentTypeStyles(message.agent_type);
  const channelColor = getChannelColor(message.channel);

  const displayContent = !isLong || expanded
    ? message.content
    : message.content.slice(0, COLLAPSE_THRESHOLD).replace(/\n[^\n]*$/, '') + '…';

  return (
    <motion.div
      {...feedItem}
      className={clsx(
        'rounded-xl border bg-surface/20 overflow-hidden',
        typeStyles.border
      )}
    >
      {/* Header bar */}
      <div className="flex items-center justify-between gap-2 px-4 py-2.5 bg-surface/30 border-b border-white/5">
        <div className="flex items-center gap-2 flex-1 min-w-0">
          <span
            className={clsx(
              'px-2.5 py-1 rounded-full text-xs font-semibold tracking-wide',
              typeStyles.bg,
              typeStyles.text
            )}
          >
            {message.agent_name}
          </span>

          <span className={clsx('text-xs font-medium', channelColor)}>
            #{message.channel}
          </span>

          {message.reply_to && (
            <span className="text-xs text-neutral/50">↳ reply</span>
          )}

          {message.metadata?.contrarian_trigger && (
            <span
              className="text-xs px-1.5 py-0.5 rounded bg-warning/10 text-warning"
              title="Contrarian challenge"
            >
              ⚡ contrarian
            </span>
          )}
        </div>

        <span className="text-xs text-neutral/50 whitespace-nowrap font-mono-numbers">
          {formatTimestamp(message.created_at)}
        </span>
      </div>

      {/* Content body */}
      <div className="px-5 py-4">
        <div className="text-[0.9rem] leading-relaxed">
          {formatContent(displayContent)}
        </div>

        {isLong && (
          <button
            onClick={() => setExpanded(!expanded)}
            className="mt-3 text-xs font-medium text-accent hover:text-accent/80 transition-colors"
          >
            {expanded ? '▲ Show less' : '▼ Read full analysis'}
          </button>
        )}
      </div>

      {/* Decision metadata footer */}
      {message.metadata?.decision_action && (
        <div className="flex gap-2 px-4 py-2 text-xs text-neutral/60 bg-surface/20 border-t border-white/5">
          <span className="font-medium">
            Decision: {message.metadata.decision_action}
          </span>
          {message.metadata.decision_symbol && (
            <span>({message.metadata.decision_symbol})</span>
          )}
        </div>
      )}
    </motion.div>
  );
}

export default function ForumLog() {
  const [messages, setMessages] = useState<ForumMessage[]>([]);
  const [channelFilter, setChannelFilter] = useState<string>('all');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchMessages();
    const interval = setInterval(fetchMessages, 10000);
    return () => clearInterval(interval);
  }, [channelFilter]);

  const fetchMessages = async () => {
    try {
      const url = channelFilter === 'all'
        ? '/api/forum/messages?limit=50'
        : `/api/forum/messages?channel=${channelFilter}&limit=50`;

      const response = await fetch(url);
      if (!response.ok) throw new Error('Failed to fetch messages');

      const data = await response.json();
      setMessages(data.messages || []);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching forum messages:', error);
      setLoading(false);
    }
  };

  const filteredMessages = messages.filter(
    (msg) => channelFilter === 'all' || msg.channel === channelFilter
  );

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-foreground">Forum Activity</h2>

        {/* Channel Filter */}
        <div className="flex gap-2">
          {(['all', 'market', 'strategy'] as const).map((ch) => (
            <button
              key={ch}
              onClick={() => setChannelFilter(ch)}
              className={clsx(
                'px-3 py-1.5 rounded-lg text-sm transition-colors',
                channelFilter === ch
                  ? ch === 'market'
                    ? 'bg-highlight text-background font-medium'
                    : 'bg-accent text-background font-medium'
                  : 'bg-surface/50 text-neutral/80 hover:text-neutral hover:bg-surface/80'
              )}
            >
              {ch.charAt(0).toUpperCase() + ch.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {/* Messages Feed */}
      <div className="space-y-4">
        {loading ? (
          <div className="text-center text-neutral/50 py-12">Loading messages...</div>
        ) : filteredMessages.length === 0 ? (
          <div className="text-center text-neutral/50 py-12">
            No messages yet. Discussion agents will post analysis here.
          </div>
        ) : (
          <AnimatePresence mode="popLayout">
            {filteredMessages.map((message) => (
              <MessageCard key={message.id} message={message} />
            ))}
          </AnimatePresence>
        )}
      </div>

      {/* Stats Footer */}
      {!loading && filteredMessages.length > 0 && (
        <div className="flex items-center justify-between text-xs text-neutral/60 pt-2 border-t border-neutral/10">
          <span>{filteredMessages.length} messages</span>
          <span>Auto-refreshing every 10s</span>
        </div>
      )}
    </div>
  );
}
