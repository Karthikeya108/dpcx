import { useEffect, useState } from "react";
import { RefreshCw, Save, Play } from "lucide-react";
import {
  getSettings,
  updateSetting,
  triggerScan,
  getScanHistory,
  type AppSetting,
  type ScanJob,
} from "@/lib/api";

export default function Settings() {
  const [settings, setSettings] = useState<AppSetting[]>([]);
  const [scanHistory, setScanHistory] = useState<ScanJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);
  const [editValues, setEditValues] = useState<Record<string, string>>({});
  const [scanPrefix, setScanPrefix] = useState("");
  const [scanSuffix, setScanSuffix] = useState("");

  const loadData = async () => {
    setLoading(true);
    try {
      const [settingsData, historyData] = await Promise.all([
        getSettings(),
        getScanHistory(10),
      ]);
      setSettings(settingsData);
      setScanHistory(historyData);
      const vals: Record<string, string> = {};
      settingsData.forEach((s) => (vals[s.key] = s.value));
      setEditValues(vals);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  const handleSave = async (key: string) => {
    setSaving(key);
    try {
      await updateSetting(key, editValues[key]);
    } finally {
      setSaving(null);
    }
  };

  const handleScan = async () => {
    setScanning(true);
    try {
      await triggerScan({
        tag_prefix: scanPrefix || undefined,
        tag_suffix: scanSuffix || undefined,
      });
      const history = await getScanHistory(10);
      setScanHistory(history);
    } finally {
      setScanning(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold">Settings</h1>
        <p className="text-muted-foreground mt-1">
          Configure workspace connections and scan parameters
        </p>
      </div>

      {/* Connection settings */}
      <div className="rounded-lg border">
        <div className="p-4 border-b">
          <h2 className="text-lg font-semibold">Connection Settings</h2>
        </div>
        <div className="divide-y">
          {settings.map((setting) => (
            <div key={setting.key} className="p-4 flex items-center gap-4">
              <div className="flex-1">
                <label className="text-sm font-medium">{setting.key}</label>
                {setting.description && (
                  <p className="text-xs text-muted-foreground">
                    {setting.description}
                  </p>
                )}
              </div>
              <input
                value={editValues[setting.key] || ""}
                onChange={(e) =>
                  setEditValues({ ...editValues, [setting.key]: e.target.value })
                }
                className="w-96 px-3 py-1.5 rounded-md border text-sm font-mono"
              />
              <button
                onClick={() => handleSave(setting.key)}
                disabled={
                  saving === setting.key ||
                  editValues[setting.key] === setting.value
                }
                className="inline-flex items-center gap-1 rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
              >
                <Save className="h-3 w-3" />
                {saving === setting.key ? "..." : "Save"}
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* Scan configuration */}
      <div className="rounded-lg border">
        <div className="p-4 border-b">
          <h2 className="text-lg font-semibold">Data Products Scan</h2>
          <p className="text-xs text-muted-foreground mt-1">
            Scan Unity Catalog for tables with governed tags to discover data
            products
          </p>
        </div>
        <div className="p-4 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium">Tag Prefix Filter</label>
              <input
                value={scanPrefix}
                onChange={(e) => setScanPrefix(e.target.value)}
                placeholder="e.g., customer_"
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              />
            </div>
            <div>
              <label className="text-sm font-medium">Tag Suffix Filter</label>
              <input
                value={scanSuffix}
                onChange={(e) => setScanSuffix(e.target.value)}
                placeholder="e.g., _v2"
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              />
            </div>
          </div>
          <button
            onClick={handleScan}
            disabled={scanning}
            className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            {scanning ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <Play className="h-4 w-4" />
            )}
            {scanning ? "Scanning..." : "Trigger Scan"}
          </button>
        </div>
      </div>

      {/* Scan history */}
      <div className="rounded-lg border">
        <div className="p-4 border-b">
          <h2 className="text-lg font-semibold">Scan History</h2>
        </div>
        {scanHistory.length === 0 ? (
          <div className="p-4 text-sm text-muted-foreground">
            No scans performed yet.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left py-2 px-4 font-medium">Status</th>
                  <th className="text-left py-2 px-4 font-medium">Prefix</th>
                  <th className="text-left py-2 px-4 font-medium">Suffix</th>
                  <th className="text-left py-2 px-4 font-medium">Products</th>
                  <th className="text-left py-2 px-4 font-medium">Tables</th>
                  <th className="text-left py-2 px-4 font-medium">Started</th>
                  <th className="text-left py-2 px-4 font-medium">Error</th>
                </tr>
              </thead>
              <tbody>
                {scanHistory.map((scan) => (
                  <tr key={scan.id} className="border-b last:border-0">
                    <td className="py-2 px-4">
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
                    <td className="py-2 px-4 text-muted-foreground">
                      {scan.tag_prefix || "-"}
                    </td>
                    <td className="py-2 px-4 text-muted-foreground">
                      {scan.tag_suffix || "-"}
                    </td>
                    <td className="py-2 px-4">{scan.products_found}</td>
                    <td className="py-2 px-4">{scan.tables_found}</td>
                    <td className="py-2 px-4 text-xs text-muted-foreground">
                      {scan.started_at
                        ? new Date(scan.started_at).toLocaleString()
                        : "-"}
                    </td>
                    <td className="py-2 px-4 text-xs text-red-500 max-w-xs truncate">
                      {scan.error_message || "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
