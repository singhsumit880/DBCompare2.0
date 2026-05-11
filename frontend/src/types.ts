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
  truncated_at?: number;
};

export type TableColumn = {
  name: string;
  type: string;
  pk: number;
};

export type RowUpdateResult = {
  updated_count: number;
  row: Record<string, unknown> | null;
  mode: "direct" | "repacked";
  output_vyp?: string | null;
  output_vyb?: string | null;
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
