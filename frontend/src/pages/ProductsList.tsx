import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Package, Search, ShieldAlert, RefreshCw, GitBranch } from "lucide-react";
import { getProducts, syncProducts, type DataProduct } from "@/lib/api";
import { domainLabel } from "@/lib/utils";

export default function ProductsList() {
  const [products, setProducts] = useState<DataProduct[]>([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [search, setSearch] = useState("");
  const [domainFilter, setDomainFilter] = useState("");

  const loadProducts = async () => {
    setLoading(true);
    try {
      const data = await getProducts(domainFilter ? { domain: domainFilter } : undefined);
      setProducts(data);
    } catch {
      // handle error
    } finally {
      setLoading(false);
    }
  };

  const handleSync = async () => {
    setSyncing(true);
    try {
      await syncProducts();
      await loadProducts();
    } finally {
      setSyncing(false);
    }
  };

  useEffect(() => {
    loadProducts();
  }, [domainFilter]);

  const domains = [...new Set(products.map((p) => p.domain))];
  const filtered = products.filter((p) => {
    const term = search.toLowerCase();
    return (
      p.name.toLowerCase().includes(term) ||
      (p.display_name || "").toLowerCase().includes(term) ||
      p.domain.toLowerCase().includes(term)
    );
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Data Products</h1>
          <p className="text-muted-foreground mt-1">
            Discover and manage your data products
          </p>
        </div>
        <button
          onClick={handleSync}
          disabled={syncing}
          className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${syncing ? "animate-spin" : ""}`} />
          {syncing ? "Syncing..." : "Sync from UC"}
        </button>
      </div>

      {/* Filters */}
      <div className="flex gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search products..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-10 pr-4 py-2 rounded-md border bg-background text-sm"
          />
        </div>
        <select
          value={domainFilter}
          onChange={(e) => setDomainFilter(e.target.value)}
          className="rounded-md border bg-background px-3 py-2 text-sm"
        >
          <option value="">All Domains</option>
          {domains.map((d) => (
            <option key={d} value={d}>{domainLabel(d)}</option>
          ))}
        </select>
      </div>

      {/* Products table */}
      {loading ? (
        <div className="flex items-center justify-center h-32">
          <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : filtered.length === 0 ? (
        <div className="text-center py-12 text-muted-foreground">
          <Package className="h-12 w-12 mx-auto mb-4 opacity-50" />
          <p>No data products found.</p>
          <p className="text-sm mt-1">Sync from Unity Catalog to discover data products.</p>
        </div>
      ) : (
        <div className="rounded-lg border">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                <th className="text-left py-3 px-4 font-medium">Name</th>
                <th className="text-left py-3 px-4 font-medium">Domain</th>
                <th className="text-left py-3 px-4 font-medium">Tables</th>
                <th className="text-left py-3 px-4 font-medium">Status</th>
                <th className="text-left py-3 px-4 font-medium">PII</th>
                <th className="text-left py-3 px-4 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((product) => (
                <tr key={product.id} className="border-b last:border-0 hover:bg-muted/30">
                  <td className="py-3 px-4">
                    <Link
                      to={`/data-products/${product.id}`}
                      className="font-medium text-primary hover:underline"
                    >
                      {product.display_name || product.name}
                    </Link>
                    <p className="text-xs text-muted-foreground">
                      tag: {product.tag_value}
                    </p>
                  </td>
                  <td className="py-3 px-4">
                    <span className="inline-flex items-center rounded-full bg-blue-100 px-2.5 py-0.5 text-xs font-medium text-blue-700">
                      {domainLabel(product.domain)}
                    </span>
                  </td>
                  <td className="py-3 px-4">{product.table_count}</td>
                  <td className="py-3 px-4">
                    <span className="inline-flex items-center rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-700">
                      {product.status}
                    </span>
                  </td>
                  <td className="py-3 px-4">
                    {product.has_pii && (
                      <ShieldAlert className="h-4 w-4 text-orange-500" />
                    )}
                  </td>
                  <td className="py-3 px-4">
                    <Link
                      to={`/data-products/${product.id}/lineage`}
                      className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-primary"
                    >
                      <GitBranch className="h-3 w-3" />
                      Lineage
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
