import { useState, useEffect, useMemo } from 'react';
import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import clsx from 'clsx';

interface Candle {
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  timestamp: string;
}

interface MarketHistoryData {
  interval: string;
  limit: number;
  candles: Record<string, Candle[]>;
  error?: string;
}

const INTERVALS = ['15m', '1h', '4h', '1d'];

export default function MarketHistory() {
  const [data, setData] = useState<MarketHistoryData | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [interval, setInterval] = useState('1h');
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    fetchData();
  }, [interval]);

  async function fetchData() {
    try {
      setLoading(true);
      const response = await fetch(`/api/market/history?interval=${interval}&limit=48`);
      if (response.ok) {
        const result: MarketHistoryData = await response.json();
        setData(result);
        // Auto-select first symbol if none selected
        if (!selectedSymbol && result.candles) {
          const symbols = Object.keys(result.candles);
          if (symbols.length > 0) {
            setSelectedSymbol(symbols[0]);
          }
        }
      }
    } catch (err) {
      console.error('Failed to fetch market history:', err);
    } finally {
      setLoading(false);
    }
  }

  const symbols = useMemo(() => {
    if (!data?.candles) return [];
    return Object.keys(data.candles);
  }, [data]);

  const chartData = useMemo(() => {
    if (!data?.candles || !selectedSymbol || !data.candles[selectedSymbol]) {
      return [];
    }

    return data.candles[selectedSymbol].map((candle) => {
      const timestamp = new Date(candle.timestamp);
      return {
        time: timestamp.getTime(),
        displayTime: timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume,
        // Calculate color based on candle direction
        color: candle.close >= candle.open ? '#00ff88' : '#ff4466',
      };
    });
  }, [data, selectedSymbol]);

  // Calculate price change for each symbol
  const symbolStats = useMemo(() => {
    if (!data?.candles) return {};

    const stats: Record<string, { change: number; lastPrice: number }> = {};
    for (const [symbol, candles] of Object.entries(data.candles)) {
      if (candles.length >= 2) {
        const firstCandle = candles[0];
        const lastCandle = candles[candles.length - 1];
        const change = ((lastCandle.close - firstCandle.open) / firstCandle.open) * 100;
        stats[symbol] = { change, lastPrice: lastCandle.close };
      }
    }
    return stats;
  }, [data]);

  // Y-axis domain with padding
  const yDomain = useMemo(() => {
    if (chartData.length === 0) return [0, 100];
    const prices = chartData.flatMap((d) => [d.high, d.low]);
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const padding = (max - min) * 0.05;
    return [min - padding, max + padding];
  }, [chartData]);

  return (
    <div className="glass-strong rounded-xl overflow-hidden">
      {/* Header - always visible */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full p-4 flex items-center justify-between hover:bg-surface/30 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-highlight/20 flex items-center justify-center">
            <svg className="w-4 h-4 text-highlight" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
            </svg>
          </div>
          <div className="text-left">
            <h3 className="text-sm font-semibold text-white">Market Data</h3>
            <p className="text-xs text-neutral">
              {loading ? 'Loading...' : `${symbols.length} symbols`}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {selectedSymbol && symbolStats[selectedSymbol] && (
            <span className={clsx(
              'text-xs font-mono-numbers',
              symbolStats[selectedSymbol].change >= 0 ? 'text-profit' : 'text-loss'
            )}>
              {symbolStats[selectedSymbol].change >= 0 ? '+' : ''}
              {symbolStats[selectedSymbol].change.toFixed(2)}%
            </span>
          )}
          <svg
            className={clsx(
              'w-4 h-4 text-neutral transition-transform',
              expanded && 'rotate-180'
            )}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {/* Expanded content */}
      {expanded && (
        <div className="border-t border-white/5 p-4 space-y-4">
          {/* Symbol selector */}
          <div className="flex flex-wrap gap-2">
            {symbols.map((symbol) => (
              <button
                key={symbol}
                onClick={() => setSelectedSymbol(symbol)}
                className={clsx(
                  'px-3 py-1.5 rounded-lg text-xs font-medium transition-all',
                  selectedSymbol === symbol
                    ? 'bg-accent text-white'
                    : 'bg-surface/50 text-neutral hover:bg-surface hover:text-white'
                )}
              >
                <span>{symbol.replace('USDT', '')}</span>
                {symbolStats[symbol] && (
                  <span className={clsx(
                    'ml-2 font-mono-numbers',
                    symbolStats[symbol].change >= 0 ? 'text-profit' : 'text-loss'
                  )}>
                    {symbolStats[symbol].change >= 0 ? '+' : ''}
                    {symbolStats[symbol].change.toFixed(1)}%
                  </span>
                )}
              </button>
            ))}
          </div>

          {/* Interval selector */}
          <div className="flex gap-1">
            {INTERVALS.map((int) => (
              <button
                key={int}
                onClick={() => setInterval(int)}
                className={clsx(
                  'px-2 py-1 rounded text-xs font-medium transition-all',
                  interval === int
                    ? 'bg-highlight/20 text-highlight'
                    : 'text-neutral hover:text-white'
                )}
              >
                {int}
              </button>
            ))}
          </div>

          {/* Chart */}
          {loading ? (
            <div className="h-48 flex items-center justify-center text-neutral">
              <div className="flex items-center gap-2">
                <div className="w-4 h-4 border-2 border-accent border-t-transparent rounded-full animate-spin" />
                Loading chart...
              </div>
            </div>
          ) : chartData.length === 0 ? (
            <div className="h-48 flex items-center justify-center text-neutral">
              No data available
            </div>
          ) : (
            <div className="h-48">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart
                  data={chartData}
                  margin={{ top: 5, right: 10, left: 0, bottom: 5 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
                  <XAxis
                    dataKey="time"
                    stroke="#8888aa"
                    fontSize={10}
                    tickLine={false}
                    axisLine={false}
                    type="number"
                    domain={['dataMin', 'dataMax']}
                    tickFormatter={(value) => {
                      const date = new Date(value);
                      return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                    }}
                  />
                  <YAxis
                    yAxisId="price"
                    stroke="#8888aa"
                    fontSize={10}
                    tickLine={false}
                    axisLine={false}
                    domain={yDomain}
                    tickFormatter={(value) => {
                      if (value >= 10000) return `$${(value / 1000).toFixed(0)}k`;
                      if (value >= 100) return `$${value.toFixed(0)}`;
                      return `$${value.toFixed(2)}`;
                    }}
                    width={55}
                  />
                  <YAxis
                    yAxisId="volume"
                    orientation="right"
                    stroke="#8888aa"
                    fontSize={10}
                    tickLine={false}
                    axisLine={false}
                    width={0}
                    hide
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(18, 18, 26, 0.95)',
                      border: '1px solid rgba(255, 255, 255, 0.1)',
                      borderRadius: '8px',
                      fontSize: '11px',
                      backdropFilter: 'blur(10px)',
                    }}
                    labelStyle={{ color: '#8888aa' }}
                    labelFormatter={(value) => {
                      const date = new Date(value);
                      return date.toLocaleString([], {
                        month: 'short',
                        day: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit',
                      });
                    }}
                    formatter={(value: number, name: string) => {
                      if (name === 'volume') return [value.toLocaleString(), 'Volume'];
                      return [`$${value.toLocaleString()}`, name.charAt(0).toUpperCase() + name.slice(1)];
                    }}
                  />
                  {/* Volume bars */}
                  <Bar
                    yAxisId="volume"
                    dataKey="volume"
                    fill="#6366f133"
                    name="volume"
                  />
                  {/* Price line */}
                  <Line
                    yAxisId="price"
                    type="monotone"
                    dataKey="close"
                    stroke="#22d3ee"
                    strokeWidth={2}
                    dot={false}
                    name="close"
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Current prices summary */}
          {selectedSymbol && symbolStats[selectedSymbol] && (
            <div className="flex items-center justify-between text-xs pt-2 border-t border-white/5">
              <span className="text-neutral">
                {selectedSymbol} - {interval} candles
              </span>
              <span className="font-mono-numbers text-white">
                ${symbolStats[selectedSymbol].lastPrice.toLocaleString()}
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
