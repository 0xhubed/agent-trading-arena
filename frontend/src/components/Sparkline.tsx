import { useMemo } from 'react';

interface SparklineProps {
  data: number[];
  width?: number;
  height?: number;
  color?: 'profit' | 'loss' | 'neutral' | 'auto';
  strokeWidth?: number;
  showArea?: boolean;
}

export default function Sparkline({
  data,
  width = 60,
  height = 20,
  color = 'auto',
  strokeWidth = 1.5,
  showArea = true,
}: SparklineProps) {
  const { path, areaPath, strokeColor, fillColor } = useMemo(() => {
    if (data.length < 2) {
      return { path: '', areaPath: '', strokeColor: '#6b7280', fillColor: 'transparent', trend: 0 };
    }

    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;
    const padding = 2;

    // Normalize data to fit in the SVG
    const points = data.map((value, index) => {
      const x = padding + (index / (data.length - 1)) * (width - padding * 2);
      const y = padding + (1 - (value - min) / range) * (height - padding * 2);
      return { x, y };
    });

    // Build path
    const linePath = points.map((p, i) => (i === 0 ? `M ${p.x} ${p.y}` : `L ${p.x} ${p.y}`)).join(' ');

    // Build area path (fill under the line)
    const areaPathStr = `${linePath} L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;

    // Determine trend
    const trendValue = data[data.length - 1] - data[0];

    // Determine color
    let stroke = '#6b7280';
    let fill = 'transparent';

    if (color === 'auto') {
      if (trendValue > 0) {
        stroke = '#22c55e';
        fill = 'rgba(34, 197, 94, 0.1)';
      } else if (trendValue < 0) {
        stroke = '#ef4444';
        fill = 'rgba(239, 68, 68, 0.1)';
      }
    } else if (color === 'profit') {
      stroke = '#22c55e';
      fill = 'rgba(34, 197, 94, 0.1)';
    } else if (color === 'loss') {
      stroke = '#ef4444';
      fill = 'rgba(239, 68, 68, 0.1)';
    }

    return {
      path: linePath,
      areaPath: areaPathStr,
      strokeColor: stroke,
      fillColor: showArea ? fill : 'transparent',
      trend: trendValue,
    };
  }, [data, width, height, color, showArea]);

  if (data.length < 2) {
    return (
      <div
        className="flex items-center justify-center text-neutral text-xs"
        style={{ width, height }}
      >
        -
      </div>
    );
  }

  return (
    <svg
      width={width}
      height={height}
      className="overflow-visible"
      viewBox={`0 0 ${width} ${height}`}
    >
      {showArea && (
        <path
          d={areaPath}
          fill={fillColor}
        />
      )}
      <path
        d={path}
        fill="none"
        stroke={strokeColor}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
