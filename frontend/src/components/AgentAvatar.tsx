import { useMemo } from 'react';

interface AgentAvatarProps {
  agentId: string;
  size?: number;
  className?: string;
}

// Simple hash function for deterministic randomness
function hashCode(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash);
}

// Generate pseudo-random number from seed
function seededRandom(seed: number): () => number {
  let state = seed;
  return () => {
    state = (state * 9301 + 49297) % 233280;
    return state / 233280;
  };
}

// Color palette - vibrant colors that work well on dark backgrounds
const SHAPE_COLORS = [
  '#6366f1', // indigo (accent)
  '#22d3ee', // cyan (highlight)
  '#00ff88', // green (profit)
  '#8b5cf6', // violet
  '#f59e0b', // amber
  '#ec4899', // pink
  '#14b8a6', // teal
  '#f43f5e', // rose
];

export default function AgentAvatar({ agentId, size = 40, className = '' }: AgentAvatarProps) {
  const svg = useMemo(() => {
    const hash = hashCode(agentId);
    const random = seededRandom(hash);

    // Select primary and secondary colors
    const primaryIdx = Math.floor(random() * SHAPE_COLORS.length);
    const secondaryIdx = (primaryIdx + 2 + Math.floor(random() * 3)) % SHAPE_COLORS.length;
    const primary = SHAPE_COLORS[primaryIdx];
    const secondary = SHAPE_COLORS[secondaryIdx];

    // Determine pattern type (0-5)
    const patternType = Math.floor(random() * 6);

    // Generate unique gradient ID
    const gradientId = `grad-${agentId.replace(/[^a-zA-Z0-9]/g, '')}`;

    // Build shapes based on pattern type
    let shapes = '';

    switch (patternType) {
      case 0: // Concentric rings
        shapes = `
          <circle cx="20" cy="20" r="17" fill="none" stroke="${primary}" stroke-width="2" opacity="0.3"/>
          <circle cx="20" cy="20" r="12" fill="none" stroke="${secondary}" stroke-width="2" opacity="0.5"/>
          <circle cx="20" cy="20" r="7" fill="${primary}" opacity="0.8"/>
          <circle cx="20" cy="20" r="3" fill="${secondary}"/>
        `;
        break;

      case 1: // Diamond layers
        shapes = `
          <rect x="10" y="10" width="20" height="20" rx="2" transform="rotate(45 20 20)" fill="${primary}" opacity="0.25"/>
          <rect x="13" y="13" width="14" height="14" rx="1" transform="rotate(45 20 20)" fill="${secondary}" opacity="0.5"/>
          <rect x="16" y="16" width="8" height="8" rx="1" transform="rotate(45 20 20)" fill="${primary}"/>
        `;
        break;

      case 2: // Triangle formation
        shapes = `
          <polygon points="20,3 37,33 3,33" fill="${primary}" opacity="0.3"/>
          <polygon points="20,9 32,30 8,30" fill="${secondary}" opacity="0.5"/>
          <polygon points="20,15 26,27 14,27" fill="${primary}"/>
        `;
        break;

      case 3: // Hexagon layers
        shapes = `
          <polygon points="20,3 34,11 34,27 20,35 6,27 6,11" fill="${primary}" opacity="0.25"/>
          <polygon points="20,8 29,13 29,25 20,30 11,25 11,13" fill="${secondary}" opacity="0.5"/>
          <polygon points="20,13 25,16 25,22 20,25 15,22 15,16" fill="${primary}"/>
        `;
        break;

      case 4: // Stacked bars
        const barCount = 3 + Math.floor(random() * 2);
        const barHeight = 6;
        const startY = (40 - barCount * (barHeight + 2)) / 2;
        for (let i = 0; i < barCount; i++) {
          const y = startY + i * (barHeight + 2);
          const width = 10 + random() * 20;
          const x = (40 - width) / 2;
          const color = i % 2 === 0 ? primary : secondary;
          const opacity = 0.4 + i * 0.15;
          shapes += `<rect x="${x}" y="${y}" width="${width}" height="${barHeight}" rx="3" fill="${color}" opacity="${Math.min(opacity, 1)}"/>`;
        }
        break;

      case 5: // Orbital rings
        const ringCount = 2 + Math.floor(random() * 2);
        shapes = `<circle cx="20" cy="20" r="4" fill="${primary}"/>`;
        for (let i = 0; i < ringCount; i++) {
          const radius = 8 + i * 5;
          const rotation = random() * 60 - 30;
          const color = i % 2 === 0 ? secondary : primary;
          const opacity = 0.4 + i * 0.1;
          shapes += `
            <ellipse cx="20" cy="20" rx="${radius}" ry="${radius * 0.4}"
                     fill="none" stroke="${color}" stroke-width="1.5"
                     opacity="${opacity}"
                     transform="rotate(${rotation} 20 20)"/>
          `;
        }
        break;
    }

    return `
      <svg viewBox="0 0 40 40" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <radialGradient id="${gradientId}">
            <stop offset="0%" stop-color="${primary}" stop-opacity="0.15"/>
            <stop offset="100%" stop-color="transparent"/>
          </radialGradient>
        </defs>
        <rect width="40" height="40" rx="8" fill="url(#${gradientId})"/>
        ${shapes}
      </svg>
    `;
  }, [agentId]);

  return (
    <div
      className={`flex-shrink-0 ${className}`}
      style={{
        width: size,
        height: size,
        borderRadius: '8px',
        background: 'rgba(18, 18, 26, 0.8)',
        border: '1px solid rgba(255, 255, 255, 0.05)',
      }}
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}
