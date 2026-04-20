import { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import type { LineageGenome } from '../../types/evolution';

interface Props {
  runId: string | null;
}

interface TreeNode {
  id: string;
  generation: number;
  fitness: number;
  isElite: boolean;
  mutations: string[];
}

interface TreeLink {
  source: string;
  target: string;
}

export default function FamilyTree({ runId }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [genomes, setGenomes] = useState<LineageGenome[]>([]);
  const [loading, setLoading] = useState(false);
  const [hoveredNode, setHoveredNode] = useState<TreeNode | null>(null);

  useEffect(() => {
    if (!runId) return;
    const controller = new AbortController();
    setLoading(true);

    fetch(`/api/evolution/${runId}/lineage`, { signal: controller.signal })
      .then((r) => r.json())
      .then((data) => setGenomes(data.genomes || []))
      .catch((err) => {
        if (err.name !== 'AbortError') console.error(err);
      })
      .finally(() => setLoading(false));

    return () => controller.abort();
  }, [runId]);

  useEffect(() => {
    if (!svgRef.current || genomes.length === 0) return;

    let simulation: d3.Simulation<any, any> | null = null;

    try {
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const width = svgRef.current.clientWidth || 800;
    const height = svgRef.current.clientHeight || 500;

    // Build nodes and links
    const nodes: TreeNode[] = genomes.map((g) => ({
      id: g.genome_id,
      generation: g.generation,
      fitness: g.fitness ?? 0,
      isElite: g.is_elite,
      mutations: g.mutations,
    }));

    const nodeIds = new Set(nodes.map((n) => n.id));
    const links: TreeLink[] = [];
    for (const g of genomes) {
      for (const pid of g.parent_ids) {
        if (nodeIds.has(pid)) {
          links.push({ source: pid, target: g.genome_id });
        }
      }
    }

    // Color scale: fitness 0-1 mapped to red-yellow-green
    const colorScale = d3.scaleLinear<string>()
      .domain([0, 0.3, 0.6, 1])
      .range(['#ef4444', '#f59e0b', '#84cc16', '#22c55e']);

    // Create simulation
    simulation = d3.forceSimulation(nodes as any)
      .force('link', d3.forceLink(links as any).id((d: any) => d.id).distance(40).strength(0.5))
      .force('charge', d3.forceManyBody().strength(-30))
      .force('x', d3.forceX((d: any) => (d.generation / Math.max(1, d3.max(nodes, (n) => n.generation) || 1)) * (width - 100) + 50).strength(0.8))
      .force('y', d3.forceY(height / 2).strength(0.1))
      .force('collision', d3.forceCollide(8));

    const g = svg.append('g');

    // Zoom
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.3, 5])
      .on('zoom', (event) => g.attr('transform', event.transform));
    svg.call(zoom);

    // Links
    const link = g.append('g')
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('stroke', '#374151')
      .attr('stroke-opacity', 0.4)
      .attr('stroke-width', 1);

    // Nodes
    const node = g.append('g')
      .selectAll('circle')
      .data(nodes)
      .join('circle')
      .attr('r', (d) => d.isElite ? 7 : 5)
      .attr('fill', (d) => colorScale(d.fitness))
      .attr('stroke', (d) => d.isElite ? '#fbbf24' : 'none')
      .attr('stroke-width', (d) => d.isElite ? 2 : 0)
      .attr('cursor', 'pointer')
      .on('mouseover', (_event, d) => setHoveredNode(d))
      .on('mouseout', () => setHoveredNode(null));

    simulation.on('tick', () => {
      link
        .attr('x1', (d: any) => d.source.x)
        .attr('y1', (d: any) => d.source.y)
        .attr('x2', (d: any) => d.target.x)
        .attr('y2', (d: any) => d.target.y);
      node
        .attr('cx', (d: any) => d.x)
        .attr('cy', (d: any) => d.y);
    });

    } catch (error) {
      console.error('D3 rendering error:', error);
    }

    return () => { simulation?.stop(); };
  }, [genomes]);

  if (!runId) {
    return (
      <div className="glass-strong rounded-xl p-8 text-center text-neutral">
        Select a run to view lineage
      </div>
    );
  }

  return (
    <div className="glass-strong rounded-xl p-4 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-white">Family Tree</h2>
        <div className="flex items-center gap-4 text-xs text-neutral">
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-full bg-red-500 inline-block" /> Low fitness
          </span>
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-full bg-green-500 inline-block" /> High fitness
          </span>
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-full border-2 border-yellow-400 inline-block" /> Elite
          </span>
        </div>
      </div>

      <div className="relative">
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/50 rounded-lg z-10">
            <span className="text-neutral">Loading lineage...</span>
          </div>
        )}
        <svg
          ref={svgRef}
          className="w-full rounded-lg bg-black/20"
          style={{ height: '500px' }}
        />
        {hoveredNode && (
          <div className="absolute top-2 right-2 glass-strong rounded-lg p-3 text-xs space-y-1 max-w-xs">
            <div className="text-white font-mono">{hoveredNode.id.slice(0, 12)}</div>
            <div className="text-neutral">Gen {hoveredNode.generation}</div>
            <div className="text-neutral">Fitness: {hoveredNode.fitness.toFixed(4)}</div>
            {hoveredNode.isElite && <div className="text-yellow-400">Elite</div>}
            {hoveredNode.mutations.length > 0 && (
              <div className="text-neutral text-[10px]">
                {hoveredNode.mutations.slice(0, 3).join(', ')}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
