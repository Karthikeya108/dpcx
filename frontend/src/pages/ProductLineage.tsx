import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { ArrowLeft, RefreshCw } from "lucide-react";
import {
  ReactFlow,
  Background,
  Controls,
  Handle,
  Position,
  type Node,
  type Edge,
  MarkerType,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  getProductLineage,
  type LineageGraph,
  type LineageNode,
} from "@/lib/api";
import { domainLabel } from "@/lib/utils";

function ProductNode({ data }: { data: LineageNode & { isCurrent: boolean } }) {
  const navigate = useNavigate();
  const contracts = data.output_contracts || [];

  return (
    <div
      className={`rounded-lg border-2 bg-white shadow-md min-w-[200px] relative ${
        data.isCurrent ? "border-blue-500" : "border-gray-200"
      }`}
    >
      {/* Target handle (left side) */}
      <Handle type="target" position={Position.Left} className="!bg-blue-500 !w-2 !h-2" />

      <div className="p-3 pr-5">
        <div className="flex items-center justify-between mb-1">
          <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide">
            {domainLabel(data.domain)}
          </span>
          <span
            className={`text-[10px] rounded-full px-1.5 py-0.5 ${
              data.status === "active"
                ? "bg-green-100 text-green-700"
                : "bg-gray-100 text-gray-500"
            }`}
          >
            {data.status}
          </span>
        </div>
        <button
          onClick={() => navigate(`/data-products/${data.id}`)}
          className="text-sm font-semibold hover:text-blue-600 text-left"
        >
          {data.name}
        </button>
      </div>

      {/* Output port squares — small blue squares on the right border */}
      {contracts.length > 0 ? (
        contracts.map((c, i) => {
          const topPercent =
            contracts.length === 1
              ? 50
              : 20 + (i * 60) / (contracts.length - 1);
          return (
            <div
              key={c.id}
              className="absolute group"
              style={{
                right: -5,
                top: `${topPercent}%`,
                transform: "translateY(-50%)",
              }}
            >
              {/* The blue square port */}
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  navigate(`/data-contracts/${c.id}`);
                }}
                className="w-2.5 h-2.5 bg-blue-500 rounded-sm border border-blue-600 hover:bg-blue-400 cursor-pointer"
                title={`${c.name} v${c.version}`}
              />
              {/* Tooltip on hover */}
              <div className="absolute right-4 top-1/2 -translate-y-1/2 hidden group-hover:block z-50 pointer-events-none">
                <div className="bg-gray-900 text-white text-[10px] px-2 py-1 rounded shadow-lg whitespace-nowrap">
                  {c.name} v{c.version}
                </div>
              </div>
            </div>
          );
        })
      ) : (
        /* Single empty port indicator */
        <div
          className="absolute group"
          style={{ right: -5, top: "50%", transform: "translateY(-50%)" }}
        >
          <div className="w-2.5 h-2.5 bg-gray-300 rounded-sm border border-gray-400" />
          <div className="absolute right-4 top-1/2 -translate-y-1/2 hidden group-hover:block z-50 pointer-events-none">
            <div className="bg-gray-900 text-white text-[10px] px-2 py-1 rounded shadow-lg whitespace-nowrap">
              No contract
            </div>
          </div>
        </div>
      )}

      {/* Source handle (right side, hidden behind port squares) */}
      <Handle
        type="source"
        position={Position.Right}
        className="!bg-transparent !border-none !w-0 !h-0"
        style={{ right: -1, top: "50%" }}
      />
    </div>
  );
}

const nodeTypes = { productNode: ProductNode };

export default function ProductLineage() {
  const { id } = useParams<{ id: string }>();
  const [lineage, setLineage] = useState<LineageGraph | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    getProductLineage(id)
      .then(setLineage)
      .finally(() => setLoading(false));
  }, [id]);

  const { nodes, edges } = useMemo(() => {
    if (!lineage) return { nodes: [] as Node[], edges: [] as Edge[] };

    // Layout: place current product in center, sources on left, targets on right
    const currentNode = lineage.nodes.find((n) => n.id === id);
    const sourceNodes = lineage.edges
      .filter((e) => e.target === id)
      .map((e) => lineage.nodes.find((n) => n.id === e.source)!)
      .filter(Boolean);
    const targetNodes = lineage.edges
      .filter((e) => e.source === id)
      .map((e) => lineage.nodes.find((n) => n.id === e.target)!)
      .filter(Boolean);

    const nodeMap = new Map<string, Node>();

    // Place source nodes on the left
    sourceNodes.forEach((node, i) => {
      const ySpacing = 140;
      const startY = -(sourceNodes.length - 1) * ySpacing / 2;
      nodeMap.set(node.id, {
        id: node.id,
        type: "productNode",
        position: { x: 0, y: startY + i * ySpacing },
        data: { ...node, isCurrent: false },
      });
    });

    // Place current node in center
    if (currentNode) {
      nodeMap.set(currentNode.id, {
        id: currentNode.id,
        type: "productNode",
        position: { x: 400, y: 0 },
        data: { ...currentNode, isCurrent: true },
      });
    }

    // Place target nodes on the right
    targetNodes.forEach((node, i) => {
      const ySpacing = 140;
      const startY = -(targetNodes.length - 1) * ySpacing / 2;
      nodeMap.set(node.id, {
        id: node.id,
        type: "productNode",
        position: { x: 800, y: startY + i * ySpacing },
      data: { ...node, isCurrent: false },
      });
    });

    // Also add any remaining nodes not yet placed
    lineage.nodes.forEach((node) => {
      if (!nodeMap.has(node.id)) {
        nodeMap.set(node.id, {
          id: node.id,
          type: "productNode",
          position: { x: 400, y: 300 },
          data: { ...node, isCurrent: node.id === id },
        });
      }
    });

    const flowEdges: Edge[] = lineage.edges.map((edge, i) => ({
      id: `edge-${i}`,
      source: edge.source,
      target: edge.target,
      animated: true,
      style: { stroke: "#6366f1", strokeWidth: 2 },
      markerEnd: { type: MarkerType.ArrowClosed, color: "#6366f1" },
    }));

    return { nodes: Array.from(nodeMap.values()), edges: flowEdges };
  }, [lineage, id]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <Link
          to={`/data-products/${id}`}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-primary mb-4"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to Product
        </Link>
        <h1 className="text-3xl font-bold">Data Product Lineage</h1>
        <p className="text-muted-foreground mt-1">
          Product-level data flow with output port contracts
        </p>
      </div>

      <div className="rounded-lg border bg-white" style={{ height: "600px" }}>
        {nodes.length === 0 ? (
          <div className="flex items-center justify-center h-full text-muted-foreground">
            <p>
              No lineage data available. Lineage is derived from Unity Catalog
              table-level lineage.
            </p>
          </div>
        ) : (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.3 }}
            minZoom={0.3}
            maxZoom={1.5}
          >
            <Background />
            <Controls />
          </ReactFlow>
        )}
      </div>

      {/* Legend */}
      <div className="flex items-center gap-6 text-xs text-muted-foreground">
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded border-2 border-blue-500 bg-white" />
          <span>Current Product</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 rounded border-2 border-gray-200 bg-white" />
          <span>Connected Product</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 bg-blue-500 rounded-sm" />
          <span>Output Port Contract (hover for name)</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 bg-gray-300 rounded-sm" />
          <span>No Contract</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-8 h-0.5 bg-indigo-500" />
          <span>Data Flow</span>
        </div>
      </div>
    </div>
  );
}
