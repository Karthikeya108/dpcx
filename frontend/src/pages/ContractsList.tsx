import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { FileText, RefreshCw, Search, Upload } from "lucide-react";
import { getContracts, uploadOdcs, type DataContract } from "@/lib/api";

export default function ContractsList() {
  const [contracts, setContracts] = useState<DataContract[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  const loadContracts = async () => {
    setLoading(true);
    try {
      const data = await getContracts(
        statusFilter ? { status: statusFilter } : undefined
      );
      setContracts(data);
    } finally {
      setLoading(false);
    }
  };

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    try {
      await uploadOdcs(file);
      await loadContracts();
    } catch {
      // handle error
    }
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  useEffect(() => {
    loadContracts();
  }, [statusFilter]);

  const filtered = contracts.filter((c) => {
    const term = search.toLowerCase();
    return (
      c.name.toLowerCase().includes(term) ||
      (c.product_name || "").toLowerCase().includes(term)
    );
  });

  const statusColor = (status: string) => {
    switch (status) {
      case "published":
        return "bg-green-100 text-green-700";
      case "draft":
        return "bg-yellow-100 text-yellow-700";
      case "deprecated":
        return "bg-red-100 text-red-700";
      default:
        return "bg-gray-100 text-gray-600";
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Data Contracts</h1>
          <p className="text-muted-foreground mt-1">
            ODCS-compliant data contracts for your data products
          </p>
        </div>
        <div>
          <input
            ref={fileInputRef}
            type="file"
            accept=".yaml,.yml"
            onChange={handleUpload}
            className="hidden"
          />
          <button
            onClick={() => fileInputRef.current?.click()}
            className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
          >
            <Upload className="h-4 w-4" />
            Upload ODCS
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search contracts..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-10 pr-4 py-2 rounded-md border bg-background text-sm"
          />
        </div>
        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="rounded-md border bg-background px-3 py-2 text-sm"
        >
          <option value="">All Statuses</option>
          <option value="draft">Draft</option>
          <option value="published">Published</option>
          <option value="deprecated">Deprecated</option>
        </select>
      </div>

      {/* Contracts table */}
      {loading ? (
        <div className="flex items-center justify-center h-32">
          <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : filtered.length === 0 ? (
        <div className="text-center py-12 text-muted-foreground">
          <FileText className="h-12 w-12 mx-auto mb-4 opacity-50" />
          <p>No data contracts found.</p>
          <p className="text-sm mt-1">
            Create a contract from a data product or upload an ODCS YAML file.
          </p>
        </div>
      ) : (
        <div className="rounded-lg border">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                <th className="text-left py-3 px-4 font-medium">Name</th>
                <th className="text-left py-3 px-4 font-medium">Version</th>
                <th className="text-left py-3 px-4 font-medium">Product</th>
                <th className="text-left py-3 px-4 font-medium">Status</th>
                <th className="text-left py-3 px-4 font-medium">Type</th>
                <th className="text-left py-3 px-4 font-medium">Updated</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((contract) => (
                <tr
                  key={contract.id}
                  className="border-b last:border-0 hover:bg-muted/30"
                >
                  <td className="py-3 px-4">
                    <Link
                      to={`/data-contracts/${contract.id}`}
                      className="font-medium text-primary hover:underline"
                    >
                      {contract.name}
                    </Link>
                  </td>
                  <td className="py-3 px-4 font-mono text-xs">
                    {contract.version}
                  </td>
                  <td className="py-3 px-4 text-muted-foreground">
                    {contract.product_name || "-"}
                  </td>
                  <td className="py-3 px-4">
                    <span
                      className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${statusColor(contract.status)}`}
                    >
                      {contract.status}
                    </span>
                  </td>
                  <td className="py-3 px-4 text-muted-foreground capitalize">
                    {contract.contract_type}
                  </td>
                  <td className="py-3 px-4 text-xs text-muted-foreground">
                    {contract.updated_at
                      ? new Date(contract.updated_at).toLocaleDateString()
                      : "-"}
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
