export type TableDataResult = {
  table: string;
  col_names: string[];
  rows_only_in_db1: Record<string, unknown>[];
  rows_only_in_db2: Record<string, unknown>[];
  all_db1_rows?: Record<string, unknown>[];
  all_db2_rows?: Record<string, unknown>[];
  rows_only_in_db1_count?: number;
  rows_only_in_db2_count?: number;
  modified_rows_count?: number;
  all_db1_rows_count?: number;
  all_db2_rows_count?: number;
  result_limited?: boolean;
  modified_rows: {
    pk: Record<string, unknown>;
    column_changes: [string, unknown, unknown][];
    db1_row?: Record<string, unknown>;
    db2_row?: Record<string, unknown>;
  }[];
  column_schema_diff?: {
    table: string;
    only_in_db1: string[];
    only_in_db2: string[];
  } | null;
};

export type ComparisonReport = {
  version: {
    db1_version: number;
    db2_version: number;
  };
  schema: {
    added_tables: string[];
    removed_tables: string[];
    column_diffs: {
      table: string;
      only_in_db1: string[];
      only_in_db2: string[];
    }[];
  };
  data: TableDataResult[];
  db1_label: string;
  db2_label: string;
};

export type CompareJobStatus = {
  id: string;
  status: "queued" | "running" | "cancelling" | "cancelled" | "completed" | "failed";
  message: string;
  percent: number;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  error: string | null;
  has_result: boolean;
};

export type SqlQueryResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  affected_rows?: number;
  elapsed_ms?: number;
  truncated?: boolean;
  truncated_at?: number | null;
};

export type TableColumn = {
  cid?: number;
  name: string;
  type: string;
  notnull?: boolean;
  default?: unknown;
  pk: number;
};

export type DatabaseSchemaTable = {
  name: string;
  type: "table" | "view";
  sql: string | null;
  row_count: number | null;
  columns: TableColumn[];
  foreign_keys: {
    id?: number;
    seq?: number;
    table: string;
    from: string;
    to: string;
    on_update?: string;
    on_delete?: string;
    match?: string;
  }[];
  indexes: Record<string, unknown>[];
};

export type DatabaseSchemaResult = {
  tables: DatabaseSchemaTable[];
  user_version: number;
  page_count: number;
  page_size: number;
};

export type QueryCompareResult = {
  columns: string[];
  left: SqlQueryResult;
  right: SqlQueryResult;
  only_in_db1: Record<string, unknown>[];
  only_in_db2: Record<string, unknown>[];
  common_count: number;
  match: boolean;
};

export type ExportSource = "table" | "query" | "database" | "schema";
export type ExportFormat = "csv" | "sql" | "sqlite" | "vyp" | "vyb";

export type ExportResult = {
  path: string;
  format: string;
  row_count: number | null;
};

export type RowUpdateResult = {
  updated_count: number;
  row: Record<string, unknown> | null;
  mode: "direct" | "repacked";
  output_vyp?: string | null;
  output_vyb?: string | null;
};

export type BatchRowEdit = {
  table: string;
  key: Record<string, unknown>;
  values: Record<string, unknown>;
};

export type BatchRowUpdateResult = {
  path: string;
  format: string;
  updated_count: number;
};

export type ToolRunResult = {
  output_vyp: string;
  output_vyb: string;
  messages: {
    message: string;
    percent: number;
  }[];
  summary?: Record<string, unknown>;
};

declare global {
  interface Window {
    dbcompare?: {
      openDatabase: () => Promise<string | null>;
      saveGeneratedFile: (
        sourcePath: string,
        defaultName?: string
      ) => Promise<{ saved: boolean; path?: string; error?: string }>;
      apiBaseUrl: string;
      initialOpenFile?: string;
      backendStatus: () => Promise<{ ok: boolean; apiBaseUrl: string; error?: string | null }>;
    };
  }
}
