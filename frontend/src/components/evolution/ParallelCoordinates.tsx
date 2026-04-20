import { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import type { GenomeEntry } from '../../types/evolution';

interface Props {
  runId: string | null;
  totalGenerations: number;
}

const PARAM_KEYS = [
  'temperature',
  'confidence_threshold',
  'position_size_pct',
  'sl_pct',
  'tp_pct',
  'max_leverage',
];

const PARAM_RANGES: Record<string, [number, number]> = {
  temperature: [0.1, 1.0],
  confidence_threshold: [0.3, 0.9],
  position_size_pct: [0.05, 0.25],
  sl_pct: [0.01, 0.05],
  tp_pct: [0.02, 0.10],
  max_leverage: [1, 10],
};

export default function ParallelCoordinates({ runId, totalGenerations }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [genomes, setGenomes] = useState<GenomeEntry[]>([]);
  const [generation, setGeneration] = useState(totalGenerations);
  const [hoveredGenome, setHoveredGenome] = useState<string | null>(null);

  useEffect(() => {
    setGeneration(totalGenerations);
  }, [totalGenerations]);

  useEffect(() => {
    if (!runId) return;
    const controller = new AbortController();

    fetch(`/api/evolution/${runId}/generations?generation=${generation}`, {
      signal: controller.signal,
    })
      .then((r) => r.json())
      .then((data) => setGenomes(data.genomes || []))
      .catch((err) => {
        if (err.name !== 'AbortError') console.error(err);
      });

    return () => controller.abort();
  }, [runId, generation]);

  useEffect(() => {
    if (!svgRef.current || genomes.length === 0) return;

    try {
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const margin = { top: 30, right: 20, bottom: 20, left: 20 };
    const width = (svgRef.current.clientWidth || 700) - margin.left - margin.right;
    const height = 350 - margin.top - margin.bottom;

    const g = svg
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Y scales for each axis
    const yScales: Record<string, d3.ScaleLinear<number, number>> = {};
    for (const key of PARAM_KEYS) {
      const [lo, hi] = PARAM_RANGES[key];
      yScales[key] = d3.scaleLinear().domain([lo, hi]).range([height, 0]);
    }

    // X position for each axis
    const xScale = d3.scalePoint<string>()
      .domain(PARAM_KEYS)
      .range([0, width]);

    // Color by fitness
    const fitnesses = genomes.map((g) => g.fitness ?? 0);
    const minFitness = fitnesses.length > 0 ? (d3.min(fitnesses) ?? 0) : 0;
    const maxFitness = fitnesses.length > 0 ? (d3.max(fitnesses) ?? 1) : 1;
    const colorScale = d3.scaleLinear<string>()
      .domain([minFitness, Math.max(maxFitness, minFitness + 0.001)])
      .range(['#ef4444', '#22c55e']);

    // Draw lines
    const line = d3.line<[string, number]>()
      .x(([key]) => xScale(key) || 0)
      .y(([key, val]) => yScales[key](val));

    g.selectAll('.genome-line')
      .data(genomes)
      .join('path')
      .attr('class', 'genome-line')
      .attr('d', (genome) => {
        const points: [string, number][] = PARAM_KEYS.map((key) => {
          const value = genome.genome[key as keyof typeof genome.genome];
          const numValue = typeof value === 'number' ? value :
            (PARAM_RANGES[key][0] + PARAM_RANGES[key][1]) / 2;
          return [key, numValue];
        });
        return line(points);
      })
      .attr('fill', 'none')
      .attr('stroke', (d) => colorScale(d.fitness ?? 0))
      .attr('stroke-width', (d) => d.genome_id === hoveredGenome ? 3 : 1.5)
      .attr('stroke-opacity', (d) =>
        hoveredGenome ? (d.genome_id === hoveredGenome ? 1 : 0.15) : 0.6
      )
      .attr('cursor', 'pointer')
      .on('mouseover', (_e, d) => setHoveredGenome(d.genome_id))
      .on('mouseout', () => setHoveredGenome(null));

    // Draw axes
    for (const key of PARAM_KEYS) {
      const x = xScale(key) || 0;

      g.append('g')
        .attr('transform', `translate(${x},0)`)
        .call(d3.axisLeft(yScales[key]).ticks(5))
        .selectAll('text')
        .attr('fill', '#9ca3af')
        .attr('font-size', '10px');

      g.append('g')
        .attr('transform', `translate(${x},0)`)
        .selectAll('line, path')
        .attr('stroke', '#374151');

      g.append('text')
        .attr('x', x)
        .attr('y', -12)
        .attr('text-anchor', 'middle')
        .attr('fill', '#d1d5db')
        .attr('font-size', '11px')
        .text(key.replace(/_/g, ' '));
    }
    } catch (error) {
      console.error('D3 rendering error:', error);
    }
  }, [genomes, hoveredGenome]);

  if (!runId) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        Select a run to view parameters
      </div>
    );
  }

  return (
    <div className="glass-strong rounded-xl p-4 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-white">Parameter Space</h2>
        <div className="flex items-center gap-2">
          <label className="text-sm text-neutral">Generation:</label>
          <input
            type="range"
            min={0}
            max={totalGenerations}
            value={generation}
            onChange={(e) => setGeneration(parseInt(e.target.value))}
            className="w-32"
          />
          <span className="text-sm text-white w-8">{generation}</span>
        </div>
      </div>

      <svg
        ref={svgRef}
        className="w-full rounded-lg bg-black/20"
        style={{ height: '350px' }}
      />

      {hoveredGenome && (
        <div className="text-xs text-neutral">
          Genome: <span className="text-white font-mono">{hoveredGenome.slice(0, 12)}</span>
          {' | '}
          Fitness: {genomes.find((g) => g.genome_id === hoveredGenome)?.fitness?.toFixed(4) ?? 'N/A'}
        </div>
      )}
    </div>
  );
}
