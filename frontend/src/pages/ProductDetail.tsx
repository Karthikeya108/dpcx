import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  ArrowLeft,
  CheckCircle,
  FileText,
  GitBranch,
  History,
  Package,
  Plus,
  RefreshCw,
  Send,
  ShieldAlert,
  Sparkles,
  Table2,
  XCircle,
} from "lucide-react";
import {
  getProduct,
  getContracts,
  generateContracts,
  getVersions,
  detectChanges,
  createVersion,
  publishVersion,
  deprecateVersion,
  type DataProductDetail,
  type DataContract,
  type DataProductVersion,
  type DetectChangesResult,
  type GenerateContractsResult,
} from "@/lib/api";
import { domainLabel } from "@/lib/utils";

export default function ProductDetail() {
  const { id } = useParams<{ id: string }>();
  const [product, setProduct] = useState<DataProductDetail | null>(null);
  const [contracts, setContracts] = useState<DataContract[]>([]);
  const [versions, setVersions] = useState<DataProductVersion[]>([]);
  const [detection, setDetection] = useState<DetectChangesResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [generating, setGenerating] = useState(false);
  const [genResult, setGenResult] = useState<GenerateContractsResult | null>(null);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [versionAction, setVersionAction] = useState<string | null>(null);

  const loadData = async () => {
    if (!id) return;
    setLoading(true);
    try {
      const [prod, cons, vers] = await Promise.all([
        getProduct(id),
        getContracts({ product_id: id }),
        getVersions(id),
      ]);
      setProduct(prod);
      setContracts(cons);
      setVersions(vers);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
  }, [id]);

  const handleGenerateContracts = async () => {
    if (!product) return;
    setGenerating(true);
    setGenResult(null);
    try {
      const result = await generateContracts(product.id);
      setGenResult(result);
      // Reload contracts
      const updated = await getContracts({ product_id: product.id });
      setContracts(updated);
      // Reload product for updated contract_count
      const prod = await getProduct(product.id);
      setProduct(prod);
    } finally {
      setGenerating(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!product) {
    return <p className="text-muted-foreground">Product not found.</p>;
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Link
          to="/data-products"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-primary mb-4"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to Products
        </Link>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Package className="h-8 w-8 text-primary" />
            <div>
              <h1 className="text-3xl font-bold">
                {product.display_name || product.name}
              </h1>
              <p className="text-muted-foreground">
                {domainLabel(product.domain)} / {product.subdomain} &middot; tag:{" "}
                <code className="bg-muted px-1 rounded text-xs">
                  {product.tag_value}
                </code>
              </p>
            </div>
            {product.has_pii && (
              <span className="inline-flex items-center gap-1 rounded-full bg-orange-100 px-2.5 py-0.5 text-xs font-medium text-orange-700">
                <ShieldAlert className="h-3 w-3" />
                Contains PII
              </span>
            )}
          </div>
          <div className="flex gap-2">
            <Link
              to={`/data-products/${product.id}/lineage`}
              className="inline-flex items-center gap-2 rounded-md border px-4 py-2 text-sm hover:bg-accent"
            >
              <GitBranch className="h-4 w-4" />
              View Lineage
            </Link>
            <button
              onClick={handleGenerateContracts}
              disabled={generating}
              className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
            >
              {generating ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="h-4 w-4" />
              )}
              {generating
                ? "Generating..."
                : contracts.length > 0
                ? "Regenerate Contracts"
                : "Generate Contracts"}
            </button>
          </div>
        </div>
      </div>

      {/* Generation result banner */}
      {genResult && (
        <div className="rounded-lg border border-green-200 bg-green-50 p-4">
          <div className="flex items-center gap-2 text-green-700">
            <CheckCircle className="h-5 w-5" />
            <p className="text-sm font-medium">
              Generated {genResult.contracts_created} output port contract
              {genResult.contracts_created !== 1 ? "s" : ""} for{" "}
              {genResult.product_name}
            </p>
          </div>
          <p className="text-xs text-green-600 mt-1">
            One ODCS contract per table, auto-populated with schema, column
            metadata, PII tags, quality rules, and ownership.
          </p>
        </div>
      )}

      {/* Info cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="rounded-lg border p-4">
          <p className="text-sm text-muted-foreground">Tables</p>
          <p className="text-2xl font-bold">{product.table_count}</p>
        </div>
        <div className="rounded-lg border p-4">
          <p className="text-sm text-muted-foreground">Contracts</p>
          <p className="text-2xl font-bold">{product.contract_count}</p>
        </div>
        <div className="rounded-lg border p-4">
          <p className="text-sm text-muted-foreground">Status</p>
          <p className="text-2xl font-bold capitalize">{product.status}</p>
        </div>
      </div>

      {/* Description */}
      {product.description && (
        <div className="rounded-lg border p-6">
          <h2 className="text-lg font-semibold mb-2">Description</h2>
          <p className="text-sm text-muted-foreground">{product.description}</p>
        </div>
      )}

      {/* Contracts */}
      {contracts.length > 0 && (
        <div className="rounded-lg border">
          <div className="p-4 border-b">
            <h2 className="text-lg font-semibold flex items-center gap-2">
              <FileText className="h-5 w-5 text-purple-500" />
              Output Port Contracts ({contracts.length})
            </h2>
            <p className="text-xs text-muted-foreground mt-1">
              One ODCS contract per table asset in this data product
            </p>
          </div>
          <div className="divide-y">
            {contracts.map((contract) => (
              <Link
                key={contract.id}
                to={`/data-contracts/${contract.id}`}
                className="flex items-center justify-between p-4 hover:bg-muted/30"
              >
                <div>
                  <p className="font-medium text-sm">{contract.name}</p>
                  <p className="text-xs text-muted-foreground">
                    {contract.description || "No description"}
                  </p>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-xs font-mono text-muted-foreground">
                    v{contract.version}
                  </span>
                  <span
                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                      contract.status === "published"
                        ? "bg-green-100 text-green-700"
                        : contract.status === "draft"
                        ? "bg-yellow-100 text-yellow-700"
                        : "bg-gray-100 text-gray-600"
                    }`}
                  >
                    {contract.status}
                  </span>
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* Versions */}
      <div className="rounded-lg border">
        <div className="p-4 border-b flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold flex items-center gap-2">
              <History className="h-5 w-5 text-indigo-500" />
              Version History
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">
              Current: <span className="font-mono font-medium">v{product.current_version}</span>
            </p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={async () => {
                if (!id) return;
                setVersionAction("detecting");
                try {
                  const d = await detectChanges(id);
                  setDetection(d);
                } finally {
                  setVersionAction(null);
                }
              }}
              disabled={versionAction !== null}
              className="inline-flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50"
            >
              <RefreshCw className={`h-3 w-3 ${versionAction === "detecting" ? "animate-spin" : ""}`} />
              Detect Changes
            </button>
            <button
              onClick={async () => {
                if (!id) return;
                setVersionAction("creating");
                try {
                  await createVersion(id);
                  const vers = await getVersions(id);
                  setVersions(vers);
                  const prod = await getProduct(id);
                  setProduct(prod);
                  setDetection(null);
                } finally {
                  setVersionAction(null);
                }
              }}
              disabled={versionAction !== null}
              className="inline-flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
            >
              <Plus className="h-3 w-3" />
              {versionAction === "creating" ? "Creating..." : "Create Version"}
            </button>
          </div>
        </div>

        {/* Detection result */}
        {detection && (
          <div className={`mx-4 mt-3 rounded-md p-3 text-sm ${
            detection.change_type === "major" ? "bg-red-50 border border-red-200" :
            detection.change_type === "minor" ? "bg-yellow-50 border border-yellow-200" :
            detection.change_type === "patch" ? "bg-blue-50 border border-blue-200" :
            "bg-gray-50 border"
          }`}>
            <div className="flex items-center justify-between">
              <div>
                {detection.change_type ? (
                  <>
                    <span className={`text-xs font-medium uppercase px-1.5 py-0.5 rounded ${
                      detection.change_type === "major" ? "bg-red-100 text-red-700" :
                      detection.change_type === "minor" ? "bg-yellow-100 text-yellow-700" :
                      "bg-blue-100 text-blue-700"
                    }`}>{detection.change_type}</span>
                    <span className="ml-2 text-xs">
                      {detection.current_version} → <span className="font-mono font-medium">{detection.new_version}</span>
                    </span>
                  </>
                ) : (
                  <span className="text-xs text-muted-foreground">No changes detected since last published version</span>
                )}
              </div>
              <button onClick={() => setDetection(null)} className="text-muted-foreground hover:text-foreground">
                <XCircle className="h-4 w-4" />
              </button>
            </div>
            {detection.change_summary && detection.change_type && (
              <p className="text-xs mt-2 text-muted-foreground">{detection.change_summary}</p>
            )}
          </div>
        )}

        {/* Version list */}
        {versions.length > 0 ? (
          <div className="divide-y">
            {versions.map((v) => (
              <div key={v.id} className="p-4 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <span className="font-mono text-sm font-medium">v{v.version}</span>
                  <span className={`text-[10px] uppercase font-medium px-1.5 py-0.5 rounded-full ${
                    v.status === "published" ? "bg-green-100 text-green-700" :
                    v.status === "draft" ? "bg-yellow-100 text-yellow-700" :
                    v.status === "deprecated" ? "bg-red-100 text-red-700" :
                    "bg-gray-100 text-gray-500"
                  }`}>{v.status}</span>
                  {v.change_type && (
                    <span className={`text-[10px] uppercase px-1.5 py-0.5 rounded ${
                      v.change_type === "major" ? "bg-red-50 text-red-600" :
                      v.change_type === "minor" ? "bg-yellow-50 text-yellow-600" :
                      "bg-blue-50 text-blue-600"
                    }`}>{v.change_type}</span>
                  )}
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-xs text-muted-foreground max-w-xs truncate">
                    {v.change_summary || "—"}
                  </span>
                  {v.status === "draft" && (
                    <button
                      onClick={async () => {
                        if (!id) return;
                        setVersionAction(v.id);
                        try {
                          await publishVersion(id, v.id);
                          const vers = await getVersions(id);
                          setVersions(vers);
                        } finally {
                          setVersionAction(null);
                        }
                      }}
                      disabled={versionAction !== null}
                      className="inline-flex items-center gap-1 rounded-md bg-green-600 px-2 py-1 text-[11px] text-white hover:bg-green-500 disabled:opacity-50"
                    >
                      <Send className="h-3 w-3" />
                      Publish
                    </button>
                  )}
                  {v.status === "published" && (
                    <button
                      onClick={async () => {
                        if (!id) return;
                        setVersionAction(v.id);
                        try {
                          await deprecateVersion(id, v.id);
                          const vers = await getVersions(id);
                          setVersions(vers);
                        } finally {
                          setVersionAction(null);
                        }
                      }}
                      disabled={versionAction !== null}
                      className="inline-flex items-center gap-1 rounded-md border border-red-200 px-2 py-1 text-[11px] text-red-600 hover:bg-red-50 disabled:opacity-50"
                    >
                      Deprecate
                    </button>
                  )}
                  <span className="text-[10px] text-muted-foreground">
                    {v.created_at ? new Date(v.created_at).toLocaleDateString() : ""}
                  </span>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="p-4 text-sm text-muted-foreground">
            No versions yet. Click "Create Version" to snapshot the current schema.
          </div>
        )}
      </div>

      {/* Tables */}
      <div className="rounded-lg border">
        <div className="p-4 border-b">
          <h2 className="text-lg font-semibold flex items-center gap-2">
            <Table2 className="h-5 w-5" />
            Tables ({product.tables.length})
          </h2>
        </div>
        <div className="divide-y">
          {product.tables.map((table) => (
            <div key={table.id}>
              <button
                onClick={() =>
                  setExpandedTable(
                    expandedTable === table.id ? null : table.id
                  )
                }
                className="w-full text-left p-4 hover:bg-muted/30"
              >
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-medium">{table.full_name}</p>
                    <p className="text-xs text-muted-foreground">
                      {table.description || "No description"}
                    </p>
                  </div>
                  <div className="flex items-center gap-4 text-sm text-muted-foreground">
                    <span>{table.column_count ?? 0} columns</span>
                    {table.row_count != null && (
                      <span>{table.row_count.toLocaleString()} rows</span>
                    )}
                  </div>
                </div>
              </button>
              {expandedTable === table.id && table.columns.length > 0 && (
                <div className="px-4 pb-4">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left py-2 px-2 font-medium">#</th>
                        <th className="text-left py-2 px-2 font-medium">Column</th>
                        <th className="text-left py-2 px-2 font-medium">Type</th>
                        <th className="text-left py-2 px-2 font-medium">Description</th>
                        <th className="text-left py-2 px-2 font-medium">PII</th>
                        <th className="text-left py-2 px-2 font-medium">Nullable</th>
                      </tr>
                    </thead>
                    <tbody>
                      {table.columns.map((col, idx) => (
                        <tr key={col.id} className="border-b last:border-0">
                          <td className="py-1.5 px-2 text-muted-foreground">
                            {idx + 1}
                          </td>
                          <td className="py-1.5 px-2 font-mono">
                            {col.column_name}
                          </td>
                          <td className="py-1.5 px-2 text-muted-foreground">
                            {col.data_type}
                          </td>
                          <td className="py-1.5 px-2 text-muted-foreground">
                            {col.description || "-"}
                          </td>
                          <td className="py-1.5 px-2">
                            {col.is_pii && (
                              <ShieldAlert className="h-3 w-3 text-orange-500" />
                            )}
                          </td>
                          <td className="py-1.5 px-2 text-muted-foreground">
                            {col.is_nullable ? "Yes" : "No"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
