import { useCompetitionStore } from '../stores/competition';
import clsx from 'clsx';

function StatBox({
  label,
  value,
  subValue,
  color = 'default'
}: {
  label: string;
  value: string | number;
  subValue?: string;
  color?: 'default' | 'profit' | 'loss' | 'accent';
}) {
  return (
    <div className="text-center px-4 py-2">
      <div className="text-xs text-neutral uppercase tracking-wide mb-1">{label}</div>
      <div className={clsx(
        'font-mono-numbers text-xl font-bold',
        color === 'profit' ? 'text-profit' :
        color === 'loss' ? 'text-loss' :
        color === 'accent' ? 'text-accent' :
        'text-white'
      )}>
        {value}
      </div>
      {subValue && (
        <div className="text-xs text-neutral mt-0.5">{subValue}</div>
      )}
    </div>
  );
}

export default function CompetitionBanner() {
  const { status, tick, leaderboard, agents } = useCompetitionStore();

  // Calculate stats
  const totalAgents = agents.length;
  const activePositions = leaderboard.reduce((sum, e) => sum + e.positions, 0);
  const totalTrades = leaderboard.reduce((sum, e) => sum + (e.total_trades ?? e.trades ?? 0), 0);

  // Leader vs laggard spread
  const sortedByPnl = [...leaderboard].sort((a, b) => b.pnl_percent - a.pnl_percent);
  const leader = sortedByPnl[0];
  const laggard = sortedByPnl[sortedByPnl.length - 1];
  const spread = leader && laggard ? leader.pnl_percent - laggard.pnl_percent : 0;

  // Total equity in play
  const totalEquity = leaderboard.reduce((sum, e) => sum + e.equity, 0);

  // Average P&L
  const avgPnl = leaderboard.length > 0
    ? leaderboard.reduce((sum, e) => sum + e.pnl_percent, 0) / leaderboard.length
    : 0;

  // Format duration from ticks (assuming 60s intervals)
  const formatDuration = (ticks: number) => {
    const totalSeconds = ticks * 60;
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    if (hours > 0) {
      return `${hours}h ${minutes}m`;
    }
    return `${minutes}m`;
  };

  const leaderAgent = agents.find(a => a.id === leader?.agent_id);
  const laggardAgent = agents.find(a => a.id === laggard?.agent_id);

  return (
    <div className="glass-strong rounded-xl p-4 mb-4 sm:mb-6">
      {/* Status indicator */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className={clsx(
            'w-2 h-2 rounded-full',
            status === 'running' ? 'bg-profit animate-pulse' :
            status === 'stopped' ? 'bg-loss' : 'bg-neutral'
          )} />
          <span className="text-sm font-medium text-white capitalize">
            {status === 'not_started' ? 'Waiting to Start' : status}
          </span>
        </div>
        <div className="text-xs text-neutral">
          Tick #{tick} {tick > 0 && `(${formatDuration(tick)})`}
        </div>
      </div>

      {/* Stats row */}
      <div className="flex flex-wrap items-center justify-between divide-x divide-white/10">
        <StatBox
          label="Agents"
          value={totalAgents}
          subValue={`${activePositions} positions`}
        />
        <StatBox
          label="Total Trades"
          value={totalTrades}
        />
        <StatBox
          label="Total Equity"
          value={`$${(totalEquity / 1000).toFixed(1)}k`}
          subValue={`Avg: ${avgPnl >= 0 ? '+' : ''}${avgPnl.toFixed(2)}%`}
          color={avgPnl >= 0 ? 'profit' : 'loss'}
        />
        <StatBox
          label="Spread"
          value={`${spread.toFixed(1)}%`}
          subValue="Leader vs Last"
          color="accent"
        />
      </div>

      {/* Leader vs Laggard highlight */}
      {leader && laggard && leaderboard.length > 1 && (
        <div className="mt-3 pt-3 border-t border-white/10 flex items-center justify-between text-xs">
          <div className="flex items-center gap-2">
            <span className="text-profit">
              {leaderAgent?.name || leader.agent_id}
            </span>
            <span className="font-mono-numbers text-profit font-medium">
              +{leader.pnl_percent.toFixed(2)}%
            </span>
          </div>
          <div className="text-neutral">vs</div>
          <div className="flex items-center gap-2">
            <span className="font-mono-numbers text-loss font-medium">
              {laggard.pnl_percent.toFixed(2)}%
            </span>
            <span className="text-loss">
              {laggardAgent?.name || laggard.agent_id}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
