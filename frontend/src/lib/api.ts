/**
 * Typed API client for the CobolShift backend.
 * All requests go through Next.js rewrites → /api/* → http://localhost:8000/*
 */

import axios from "axios";

export const apiClient = axios.create({
  baseURL: "/api",
  headers: { "Content-Type": "application/json" },
});

// ---------------------------------------------------------------------------
// Types (mirrors backend/api/models.py)
// ---------------------------------------------------------------------------

export interface Project {
  id: string;
  name: string;
  description: string | null;
  source_type: string;
  target_type: string;
  config_json: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface Copybook {
  id: string;
  project_id: string;
  filename: string;
  file_path: string;
  file_checksum: string | null;
  parsed_at: string | null;
  schema_ir: Record<string, unknown> | null;
  parse_errors: string[] | null;
  created_at: string;
}

export interface SourceFile {
  id: string;
  project_id: string;
  copybook_id: string | null;
  filename: string;
  file_path: string;
  file_checksum: string | null;
  record_format: string | null;
  record_length: number | null;
  encoding: string;
  total_records: number | null;
  created_at: string;
}

export interface MigrationRun {
  id: string;
  project_id: string;
  run_number: number;
  run_type: string;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  rows_extracted: number;
  rows_loaded: number;
  rows_rejected: number;
  error_message: string | null;
  config_snapshot: Record<string, unknown>;
}

export interface RunSummary {
  run_id: string;
  project_id: string;
  run_number: number;
  run_type: string;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  elapsed_seconds: number | null;
  tables_total: number;
  tables_completed: number;
  tables_failed: number;
  rows_extracted: number;
  rows_loaded: number;
  rows_rejected: number;
  error_message: string | null;
}

export interface TableState {
  id: string;
  run_id: string;
  table_name: string;
  status: string;
  rows_extracted: number;
  rows_loaded: number;
  rows_rejected: number;
  source_checksum: string | null;
  error_message: string | null;
  started_at: string | null;
  finished_at: string | null;
}

export interface RejectionEntry {
  id: string;
  run_id: string;
  table_state_id: string | null;
  source_line_num: number | null;
  decoded_partial: Record<string, unknown> | null;
  error_type: string | null;
  error_message: string | null;
  created_at: string;
}

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

export const projectsApi = {
  list: () => apiClient.get<Project[]>("/projects").then((r) => r.data),
  get: (id: string) => apiClient.get<Project>(`/projects/${id}`).then((r) => r.data),
  create: (data: Partial<Project>) =>
    apiClient.post<Project>("/projects", data).then((r) => r.data),
  update: (id: string, data: Partial<Project>) =>
    apiClient.patch<Project>(`/projects/${id}`, data).then((r) => r.data),
  delete: (id: string) => apiClient.delete(`/projects/${id}`),

  listCopybooks: (projectId: string) =>
    apiClient.get<Copybook[]>(`/projects/${projectId}/copybooks`).then((r) => r.data),
  uploadCopybook: (projectId: string, file: File) => {
    const form = new FormData();
    form.append("file", file);
    return apiClient
      .post<Copybook>(`/projects/${projectId}/copybooks`, form, {
        headers: { "Content-Type": "multipart/form-data" },
      })
      .then((r) => r.data);
  },

  listSourceFiles: (projectId: string) =>
    apiClient.get<SourceFile[]>(`/projects/${projectId}/source-files`).then((r) => r.data),
};

export const schemaApi = {
  parse: (copybookId: string) =>
    apiClient.post("/schema/parse", { copybook_id: copybookId }).then((r) => r.data),
  generateDDL: (copybookId: string, dialect: string) =>
    apiClient
      .post("/schema/ddl", { copybook_id: copybookId, dialect })
      .then((r) => r.data),
};

export const migrationsApi = {
  list: (projectId: string) =>
    apiClient
      .get<MigrationRun[]>("/migrations", { params: { project_id: projectId } })
      .then((r) => r.data),
  get: (runId: string) =>
    apiClient.get<RunSummary>(`/migrations/${runId}`).then((r) => r.data),
  start: (projectId: string, runType = "full_load") =>
    apiClient
      .post<MigrationRun>("/migrations", { project_id: projectId, run_type: runType })
      .then((r) => r.data),
  cancel: (runId: string) => apiClient.delete(`/migrations/${runId}`),
  getTableStates: (runId: string) =>
    apiClient.get<TableState[]>(`/migrations/${runId}/tables`).then((r) => r.data),
  getRejections: (runId: string) =>
    apiClient
      .get<RejectionEntry[]>(`/migrations/${runId}/rejections`)
      .then((r) => r.data),
};
