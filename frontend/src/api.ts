import type {
  CompareJobStatus,
  ComparisonReport,
  DatabaseSchemaResult,
  ExportFormat,
  ExportResult,
  ExportSource,
  BatchRowEdit,
  BatchRowUpdateResult,
  QueryCompareResult,
  RowUpdateResult,
  SqlQueryResult,
  TableColumn,
  ToolRunResult
} from "./types";

const baseUrl = window.dbcompare?.apiBaseUrl ?? "http://127.0.0.1:8765";

async function post<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    throw new Error(payload?.detail ?? response.statusText);
  }

  return response.json() as Promise<T>;
}

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, { signal });
  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    throw new Error(payload?.detail ?? response.statusText);
  }
  return response.json() as Promise<T>;
}

export function compareDatabases(payload: {
  db1_path: string;
  db2_path: string;
  included_tables: string[];
  excluded_tables: string[];
  ignore_datetime: boolean;
  decimal_precision: number;
  validate_db: boolean;
  max_result_rows_per_table?: number;
}, signal?: AbortSignal) {
  return post<ComparisonReport>("/api/compare", payload, signal);
}

export type ComparePayload = Parameters<typeof compareDatabases>[0];

export function startCompareJob(payload: ComparePayload, signal?: AbortSignal) {
  return post<{ job_id: string }>("/api/compare/jobs", payload, signal);
}

export function getCompareJob(jobId: string, signal?: AbortSignal) {
  return get<CompareJobStatus>(`/api/compare/jobs/${jobId}`, signal);
}

export function getCompareJobResult(jobId: string, signal?: AbortSignal) {
  return get<ComparisonReport>(`/api/compare/jobs/${jobId}/result`, signal);
}

export function cancelCompareJob(jobId: string) {
  return post<CompareJobStatus>(`/api/compare/jobs/${jobId}/cancel`, {});
}

export function sanitizeDatabase(dbPath: string, queries: string[]) {
  return post<ToolRunResult>("/api/tools/sanitize", { db_path: dbPath, queries });
}

export function convertDatabase(dbPath: string) {
  return post<ToolRunResult>("/api/tools/convert", { db_path: dbPath });
}

export function buildFtsDatabase(dbPath: string) {
  return post<ToolRunResult>("/api/tools/fts", { db_path: dbPath });
}

export function repairSettingsTable(dbPath: string) {
  return post<ToolRunResult>("/api/tools/settings-repair", { db_path: dbPath });
}

export function listTables(dbPath: string) {
  return post<{ tables: string[]; views: string[] }>("/api/sql/tables", { db_path: dbPath });
}

export function tableInfo(dbPath: string, table: string) {
  return post<{
    table: string;
    row_count: number;
    columns: TableColumn[];
    foreign_keys: { table: string; from: string; to: string }[];
  }>("/api/sql/table-info", { db_path: dbPath, table });
}

export function databaseSchema(dbPath: string) {
  return post<DatabaseSchemaResult>("/api/sql/schema", { db_path: dbPath });
}

export function databaseChecks(dbPath: string) {
  return post<{
    user_version: number;
    integrity: Record<string, unknown>[];
    foreign_keys: Record<string, unknown>[];
  }>("/api/sql/checks", { db_path: dbPath });
}

export function databaseVersion(dbPath: string) {
  return post<{ user_version: number }>("/api/sql/version", { db_path: dbPath });
}

export function tableRows(dbPath: string, table: string, limit = 1000, offset = 0) {
  return post<{ table: string; columns: string[]; rows: Record<string, unknown>[]; row_count: number }>(
    `/api/sql/rows?limit=${limit}&offset=${offset}`,
    { db_path: dbPath, table }
  );
}

export function updateRow(
  dbPath: string,
  table: string,
  key: Record<string, unknown>,
  values: Record<string, unknown>
) {
  return post<RowUpdateResult>("/api/sql/update-row", {
    db_path: dbPath,
    table,
    key,
    values
  });
}

export function updateRowsBatch(dbPath: string, edits: BatchRowEdit[]) {
  return post<BatchRowUpdateResult>("/api/sql/update-rows-batch", {
    db_path: dbPath,
    edits
  });
}

export function executeSql(dbPath: string, sql: string, allowWrite = false, limit = 500) {
  return post<SqlQueryResult>("/api/sql/query", {
    db_path: dbPath,
    sql,
    allow_write: allowWrite,
    limit
  });
}

export function compareQuery(dbPath: string, leftSql: string, rightSql: string, limit = 1000) {
  return post<QueryCompareResult>("/api/sql/compare-query", {
    db_path: dbPath,
    left_sql: leftSql,
    right_sql: rightSql,
    limit
  });
}

export function exportSqlData(payload: {
  dbPath: string;
  source: ExportSource;
  format: ExportFormat;
  table?: string;
  sql?: string;
  limit?: number;
}) {
  return post<ExportResult>("/api/sql/export", {
    db_path: payload.dbPath,
    source: payload.source,
    format: payload.format,
    table: payload.table,
    sql: payload.sql,
    limit: payload.limit ?? 10000
  });
}

export function relatedRows(dbPath: string, table: string, column: string, value: unknown, limit = 250) {
  return post<{ table: string; columns: string[]; rows: Record<string, unknown>[] }>("/api/sql/related-rows", {
    db_path: dbPath,
    table,
    column,
    value,
    limit
  });
}
