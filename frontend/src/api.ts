import type { ComparisonReport, SqlQueryResult, ToolRunResult } from "./types";

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

export function compareDatabases(payload: {
  db1_path: string;
  db2_path: string;
  included_tables: string[];
  excluded_tables: string[];
  ignore_datetime: boolean;
  decimal_precision: number;
  validate_db: boolean;
}, signal?: AbortSignal) {
  return post<ComparisonReport>("/api/compare", payload, signal);
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
    columns: { name: string; type: string; pk: number }[];
    foreign_keys: { table: string; from: string; to: string }[];
  }>("/api/sql/table-info", { db_path: dbPath, table });
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
  return post<{ table: string; columns: string[]; rows: Record<string, unknown>[] }>(
    `/api/sql/rows?limit=${limit}&offset=${offset}`,
    { db_path: dbPath, table }
  );
}

export function executeSql(dbPath: string, sql: string, allowWrite = false, limit = 500) {
  return post<SqlQueryResult>("/api/sql/query", {
    db_path: dbPath,
    sql,
    allow_write: allowWrite,
    limit
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
