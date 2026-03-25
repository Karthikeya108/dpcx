import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Package,
  Table2,
  FileText,
  ShieldAlert,
  RefreshCw,
} from "lucide-react";
import { getStats, type DashboardStats } from "@/lib/api";
import { domainLabel } from "@/lib/utils";

function StatCard({
  icon: Icon,
  label,
  value,
  color,
}: {
  icon: React.ElementType;
  label: string;
  value: number | string;
  color: string;
}) {
  return (
    <div className="rounded-lg border bg-card p-6">
      <div className="flex items-center gap-4">
        <div className={`rounded-md p-2 ${color}`}>
          <Icon className="h-5 w-5 text-white" />
        </div>
        <div>
          <p className="text-sm text-muted-foreground">{label}</p>
          <p className="text-2xl font-bold">{value}</p>
        </div>
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  const loadStats = async () => {
    setLoading(true);
    try {
      const data = await getStats();
      setStats(data);
    } catch {
      // Stats endpoint may not be ready yet
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStats();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Dashboard</h1>
          <p className="text-muted-foreground mt-1">
            Overview of your data products and contracts
          </p>
        </div>
        <button
          onClick={loadStats}
          className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
        >
          <RefreshCw className="h-4 w-4" />
          Refresh
        </button>
      </div>

      {/* Stats cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          icon={Package}
          label="Data Products"
          value={stats?.total_products ?? 0}
          color="bg-blue-500"
        />
        <StatCard
          icon={Table2}
          label="Total Tables"
          value={stats?.total_tables ?? 0}
          color="bg-green-500"
        />
        <StatCard
          icon={FileText}
          label="Data Contracts"
          value={stats?.total_contracts ?? 0}
          color="bg-purple-500"
        />
        <StatCard
          icon={ShieldAlert}
          label="Products with PII"
          value={stats?.pii_product_count ?? 0}
          color="bg-orange-500"
        />
      </div>

      {/* Domain breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="rounded-lg border bg-card p-6">
          <h2 className="text-lg font-semibold mb-4">Products by Domain</h2>
          {stats && Object.keys(stats.products_by_domain).length > 0 ? (
            <div className="space-y-3">
              {Object.entries(stats.products_by_domain).map(([domain, count]) => (
                <div key={domain} className="flex items-center justify-between">
                  <span className="text-sm">{domainLabel(domain)}</span>
                  <div className="flex items-center gap-2">
                    <div className="h-2 bg-blue-500 rounded-full" style={{ width: `${count * 30}px` }} />
                    <span className="text-sm font-medium">{count}</span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              No data products synced yet.{" "}
              <Link to="/settings" className="text-primary underline">
                Trigger a scan
              </Link>{" "}
              to discover products from Unity Catalog.
            </p>
          )}
        </div>

        <div className="rounded-lg border bg-card p-6">
          <h2 className="text-lg font-semibold mb-4">Contracts by Status</h2>
          {stats && Object.keys(stats.contracts_by_status).length > 0 ? (
            <div className="space-y-3">
              {Object.entries(stats.contracts_by_status).map(([status, count]) => (
                <div key={status} className="flex items-center justify-between">
                  <span className="text-sm capitalize">{status}</span>
                  <span className="inline-flex items-center rounded-full bg-secondary px-2.5 py-0.5 text-xs font-medium">
                    {count}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              No data contracts created yet.{" "}
              <Link to="/data-contracts" className="text-primary underline">
                Create one
              </Link>{" "}
              from a data product.
            </p>
          )}
        </div>
      </div>

      {/* Recent scans */}
      <div className="rounded-lg border bg-card p-6">
        <h2 className="text-lg font-semibold mb-4">Recent Scans</h2>
        {stats && stats.recent_scans.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-2 font-medium">Status</th>
                  <th className="text-left py-2 font-medium">Products Found</th>
                  <th className="text-left py-2 font-medium">Tables Found</th>
                  <th className="text-left py-2 font-medium">Started</th>
                </tr>
              </thead>
              <tbody>
                {stats.recent_scans.map((scan) => (
                  <tr key={scan.id} className="border-b last:border-0">
                    <td className="py-2">
                      <span
                        className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${
                          scan.status === "completed"
                            ? "bg-green-100 text-green-700"
                            : scan.status === "failed"
                            ? "bg-red-100 text-red-700"
                            : "bg-yellow-100 text-yellow-700"
                        }`}
                      >
                        {scan.status}
                      </span>
                    </td>
                    <td className="py-2">{scan.products_found}</td>
                    <td className="py-2">{scan.tables_found}</td>
                    <td className="py-2 text-muted-foreground">
                      {scan.started_at
                        ? new Date(scan.started_at).toLocaleString()
                        : "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">No scans performed yet.</p>
        )}
      </div>
    </div>
  );
}
