import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  ArrowLeft,
  Download,
  FileText,
  RefreshCw,
  Save,
} from "lucide-react";
import {
  getContract,
  updateContract,
  downloadOdcs,
  type DataContractDetail,
} from "@/lib/api";

export default function ContractDetail() {
  const { id } = useParams<{ id: string }>();
  const [contract, setContract] = useState<DataContractDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [editMode, setEditMode] = useState(false);
  const [form, setForm] = useState({
    name: "",
    version: "",
    description: "",
    status: "",
    owner: "",
    contract_type: "",
    odcs_yaml: "",
  });

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    getContract(id)
      .then((data) => {
        setContract(data);
        setForm({
          name: data.name,
          version: data.version,
          description: data.description || "",
          status: data.status,
          owner: data.owner || "",
          contract_type: data.contract_type,
          odcs_yaml: data.odcs_yaml || "",
        });
      })
      .finally(() => setLoading(false));
  }, [id]);

  const handleSave = async () => {
    if (!id) return;
    setSaving(true);
    try {
      const updated = await updateContract(id, form);
      setContract({ ...contract!, ...updated });
      setEditMode(false);
    } finally {
      setSaving(false);
    }
  };

  const handleDownload = async () => {
    if (!id) return;
    try {
      const blob = await downloadOdcs(id);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${contract?.name || "contract"}-v${contract?.version || "1.0.0"}.odcs.yaml`;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      // handle error
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!contract) {
    return <p className="text-muted-foreground">Contract not found.</p>;
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Link
          to="/data-contracts"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-primary mb-4"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to Contracts
        </Link>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <FileText className="h-8 w-8 text-purple-500" />
            <div>
              <h1 className="text-3xl font-bold">{contract.name}</h1>
              <p className="text-muted-foreground">
                v{contract.version} &middot;{" "}
                <span className="capitalize">{contract.status}</span>
                {contract.product_name && (
                  <>
                    {" "}
                    &middot; Product:{" "}
                    <Link
                      to={`/data-products/${contract.product_id}`}
                      className="text-primary hover:underline"
                    >
                      {contract.product_name}
                    </Link>
                  </>
                )}
              </p>
            </div>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleDownload}
              className="inline-flex items-center gap-2 rounded-md border px-4 py-2 text-sm hover:bg-accent"
            >
              <Download className="h-4 w-4" />
              Download ODCS
            </button>
            {editMode ? (
              <>
                <button
                  onClick={() => {
                    setEditMode(false);
                    if (contract) {
                      setForm({
                        name: contract.name,
                        version: contract.version,
                        description: contract.description || "",
                        status: contract.status,
                        owner: contract.owner || "",
                        contract_type: contract.contract_type,
                        odcs_yaml: contract.odcs_yaml || "",
                      });
                    }
                  }}
                  className="inline-flex items-center gap-2 rounded-md border border-red-200 bg-red-50 px-4 py-2 text-sm text-red-600 hover:bg-red-100"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSave}
                  disabled={saving}
                  className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
                >
                  <Save className="h-4 w-4" />
                  {saving ? "Saving..." : "Save Changes"}
                </button>
              </>
            ) : (
              <button
                onClick={() => setEditMode(true)}
                className="inline-flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
              >
                Edit
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Contract fields */}
      <div className="grid grid-cols-2 gap-6">
        <div className="space-y-4">
          <div>
            <label className="text-sm font-medium">Name</label>
            {editMode ? (
              <input
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              />
            ) : (
              <p className="text-sm mt-1">{contract.name}</p>
            )}
          </div>
          <div>
            <label className="text-sm font-medium">Version</label>
            {editMode ? (
              <input
                value={form.version}
                onChange={(e) => setForm({ ...form, version: e.target.value })}
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              />
            ) : (
              <p className="text-sm mt-1 font-mono">{contract.version}</p>
            )}
          </div>
          <div>
            <label className="text-sm font-medium">Status</label>
            {editMode ? (
              <select
                value={form.status}
                onChange={(e) => setForm({ ...form, status: e.target.value })}
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              >
                <option value="draft">Draft</option>
                <option value="published">Published</option>
                <option value="deprecated">Deprecated</option>
              </select>
            ) : (
              <p className="text-sm mt-1 capitalize">{contract.status}</p>
            )}
          </div>
          <div>
            <label className="text-sm font-medium">Owner</label>
            {editMode ? (
              <input
                value={form.owner}
                onChange={(e) => setForm({ ...form, owner: e.target.value })}
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              />
            ) : (
              <p className="text-sm mt-1">{contract.owner || "-"}</p>
            )}
          </div>
          <div>
            <label className="text-sm font-medium">Type</label>
            {editMode ? (
              <select
                value={form.contract_type}
                onChange={(e) =>
                  setForm({ ...form, contract_type: e.target.value })
                }
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              >
                <option value="output">Output</option>
                <option value="input">Input</option>
              </select>
            ) : (
              <p className="text-sm mt-1 capitalize">
                {contract.contract_type}
              </p>
            )}
          </div>
        </div>

        <div className="space-y-4">
          <div>
            <label className="text-sm font-medium">Description</label>
            {editMode ? (
              <textarea
                value={form.description}
                onChange={(e) =>
                  setForm({ ...form, description: e.target.value })
                }
                rows={4}
                className="w-full mt-1 px-3 py-2 rounded-md border text-sm"
              />
            ) : (
              <p className="text-sm mt-1 text-muted-foreground">
                {contract.description || "No description"}
              </p>
            )}
          </div>
        </div>
      </div>

      {/* ODCS YAML preview */}
      <div className="rounded-lg border">
        <div className="p-4 border-b flex items-center justify-between">
          <h2 className="text-lg font-semibold">ODCS YAML</h2>
        </div>
        <div className="p-4">
          {editMode ? (
            <textarea
              value={form.odcs_yaml}
              onChange={(e) =>
                setForm({ ...form, odcs_yaml: e.target.value })
              }
              rows={20}
              className="w-full px-3 py-2 rounded-md border text-xs font-mono bg-muted"
            />
          ) : (
            <pre className="text-xs font-mono bg-muted p-4 rounded-md overflow-auto max-h-96">
              {contract.odcs_yaml || "No ODCS YAML generated yet."}
            </pre>
          )}
        </div>
      </div>

      {/* Version history */}
      {contract.versions.length > 0 && (
        <div className="rounded-lg border">
          <div className="p-4 border-b">
            <h2 className="text-lg font-semibold">Version History</h2>
          </div>
          <div className="divide-y">
            {contract.versions.map((v) => (
              <div key={v.id} className="p-4 flex items-center justify-between">
                <div>
                  <p className="text-sm font-mono font-medium">v{v.version}</p>
                  <p className="text-xs text-muted-foreground">
                    {v.change_summary || "No description"}
                  </p>
                </div>
                <p className="text-xs text-muted-foreground">
                  {v.created_at
                    ? new Date(v.created_at).toLocaleString()
                    : "-"}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
