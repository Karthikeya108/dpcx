import axios from "axios";

const api = axios.create({
  baseURL: "/api",
});

// ─── Types ───

export interface DataProduct {
  id: string;
  name: string;
  display_name: string | null;
  description: string | null;
  domain: string;
  subdomain: string | null;
  tag_value: string;
  status: string;
  table_count: number;
  total_row_count: number;
  has_pii: boolean;
  created_at: string | null;
  updated_at: string | null;
}

export interface DataProductColumn {
  id: string;
  column_name: string;
  data_type: string;
  description: string | null;
  is_pii: boolean;
  is_nullable: boolean;
  ordinal_position: number | null;
}

export interface DataProductTable {
  id: string;
  catalog_name: string;
  schema_name: string;
  table_name: string;
  full_name: string;
  table_type: string | null;
  description: string | null;
  row_count: number | null;
  column_count: number | null;
  columns: DataProductColumn[];
}

export interface DataProductDetail extends DataProduct {
  tables: DataProductTable[];
  contract_count: number;
}

export interface DataContract {
  id: string;
  product_id: string | null;
  name: string;
  version: string;
  description: string | null;
  status: string;
  contract_type: string;
  odcs_yaml: string | null;
  owner: string | null;
  created_by: string | null;
  created_at: string | null;
  updated_at: string | null;
  product_name: string | null;
}

export interface DataContractDetail extends DataContract {
  versions: {
    id: string;
    version: string;
    change_summary: string | null;
    created_by: string | null;
    created_at: string | null;
  }[];
}

export interface LineageNode {
  id: string;
  name: string;
  domain: string;
  status: string;
  input_contracts: { id: string; name: string; version: string }[];
  output_contracts: { id: string; name: string; version: string }[];
}

export interface LineageEdge {
  source: string;
  target: string;
  source_tables: string[];
  target_tables: string[];
}

export interface LineageGraph {
  nodes: LineageNode[];
  edges: LineageEdge[];
}

export interface ScanJob {
  id: string;
  job_type: string;
  tag_prefix: string | null;
  tag_suffix: string | null;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  tables_found: number;
  products_found: number;
  error_message: string | null;
  created_at: string | null;
}

export interface AppSetting {
  key: string;
  value: string;
  description: string | null;
}

export interface GenerateContractsResult {
  product_id: string;
  product_name: string;
  contracts_created: number;
  contracts: DataContract[];
}

export interface DashboardStats {
  total_products: number;
  total_tables: number;
  total_contracts: number;
  products_by_domain: Record<string, number>;
  contracts_by_status: Record<string, number>;
  pii_product_count: number;
  recent_scans: ScanJob[];
}

export interface DataProductVersion {
  id: string;
  product_id: string;
  version: string;
  status: string;
  change_type: string | null;
  change_summary: string | null;
  published_by: string | null;
  published_at: string | null;
  deprecated_at: string | null;
  created_at: string | null;
}

export interface DetectChangesResult {
  change_type: string | null;
  diff: Record<string, string[]>;
  change_summary: string;
  current_version: string;
  new_version: string | null;
}

export interface VersionDiff {
  version_a: string;
  version_b: string;
  change_type: string | null;
  diff: Record<string, string[]>;
  change_summary: string;
}

// ─── API functions ───

export const getStats = () => api.get<DashboardStats>("/stats").then((r) => r.data);

export const getProducts = (params?: { domain?: string; status?: string }) =>
  api.get<DataProduct[]>("/data-products", { params }).then((r) => r.data);

export const getProduct = (id: string) =>
  api.get<DataProductDetail>(`/data-products/${id}`).then((r) => r.data);

export const syncProducts = (params?: { tag_prefix?: string; tag_suffix?: string }) =>
  api.post<ScanJob>("/data-products/sync", null, { params }).then((r) => r.data);

export const generateContracts = (productId: string) =>
  api.post<GenerateContractsResult>(`/data-products/${productId}/generate-contracts`).then((r) => r.data);

export const getVersions = (productId: string) =>
  api.get<DataProductVersion[]>(`/data-products/${productId}/versions`).then((r) => r.data);

export const detectChanges = (productId: string) =>
  api.post<DetectChangesResult>(`/data-products/${productId}/versions/detect`).then((r) => r.data);

export const createVersion = (productId: string) =>
  api.post<DataProductVersion>(`/data-products/${productId}/versions`).then((r) => r.data);

export const publishVersion = (productId: string, versionId: string) =>
  api.put<DataProductVersion>(`/data-products/${productId}/versions/${versionId}/publish`).then((r) => r.data);

export const deprecateVersion = (productId: string, versionId: string) =>
  api.put<DataProductVersion>(`/data-products/${productId}/versions/${versionId}/deprecate`).then((r) => r.data);

export const getProductLineage = (id: string) =>
  api.get<LineageGraph>(`/data-products/${id}/lineage`).then((r) => r.data);

export const getContracts = (params?: { status?: string; product_id?: string }) =>
  api.get<DataContract[]>("/data-contracts", { params }).then((r) => r.data);

export const getContract = (id: string) =>
  api.get<DataContractDetail>(`/data-contracts/${id}`).then((r) => r.data);

export const createContract = (data: {
  product_id?: string;
  name: string;
  version?: string;
  description?: string;
  contract_type?: string;
  owner?: string;
}) => api.post<DataContract>("/data-contracts", data).then((r) => r.data);

export const updateContract = (
  id: string,
  data: Partial<{
    name: string;
    version: string;
    description: string;
    status: string;
    contract_type: string;
    odcs_yaml: string;
    owner: string;
  }>
) => api.put<DataContract>(`/data-contracts/${id}`, data).then((r) => r.data);

export const downloadOdcs = (id: string) =>
  api.get(`/data-contracts/${id}/odcs`, { responseType: "blob" }).then((r) => r.data);

export const uploadOdcs = (file: File, productId?: string) => {
  const formData = new FormData();
  formData.append("file", file);
  return api
    .post<DataContract>("/data-contracts/upload", formData, {
      params: productId ? { product_id: productId } : {},
      headers: { "Content-Type": "multipart/form-data" },
    })
    .then((r) => r.data);
};

export const getSettings = () =>
  api.get<AppSetting[]>("/settings").then((r) => r.data);

export const updateSetting = (key: string, value: string, description?: string) =>
  api.put<AppSetting>(`/settings/${key}`, { value, description }).then((r) => r.data);

export const triggerScan = (data: {
  tag_prefix?: string;
  tag_suffix?: string;
  metastore_id?: string;
}) => api.post<ScanJob>("/scan/trigger", data).then((r) => r.data);

export const getScanHistory = (limit?: number) =>
  api.get<ScanJob[]>("/scan/history", { params: { limit } }).then((r) => r.data);
