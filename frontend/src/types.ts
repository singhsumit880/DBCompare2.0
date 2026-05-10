export type TableDataResult = {
  table: string;
  col_names: string[];
  rows_only_in_db1: Record<string, unknown>[];
  rows_only_in_db2: Record<string, unknown>[];
  all_db1_rows?: Record<string, unknown>[];
  all_db2_rows?: Record<string, unknown>[];
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

export type SqlQueryResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  truncated_at?: number;
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
    };
  }
}
