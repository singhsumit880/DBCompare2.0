import { useEffect, useMemo, useRef, useState, type CSSProperties, type KeyboardEvent as ReactKeyboardEvent, type UIEvent } from "react";
import {
  AlertTriangle,
  ArrowLeftToLine,
  ArrowRight,
  ArrowRightToLine,
  BarChart3,
  CheckCircle2,
  Code2,
  Database,
  Download,
  Eye,
  FileDown,
  FolderOpen,
  GitCompare,
  Link2,
  Minus,
  Network,
  Play,
  Plus,
  RefreshCcw,
  RotateCcw,
  Rows3,
  Search,
  Settings2,
  Square,
  Table2,
  Wrench
} from "lucide-react";
import {
  buildFtsDatabase,
  cancelCompareJob,
  convertDatabase,
  databaseChecks,
  databaseSchema,
  databaseVersion,
  executeSql,
  exportSqlData,
  getCompareJob,
  getCompareJobResult,
  listTables,
  relatedRows,
  repairSettingsTable,
  sanitizeDatabase,
  startCompareJob,
  tableInfo,
  tableRows,
  updateRowsBatch
} from "./api";
import type {
  ComparisonReport,
  BatchRowEdit,
  DatabaseSchemaResult,
  ExportFormat,
  ExportSource,
  SqlQueryResult,
  TableColumn,
  TableDataResult,
  ToolRunResult
} from "./types";

const DEFAULT_EXCLUDES = [
  "kb_fts_vtable",
  "kb_fts_vtable_content",
  "kb_fts_vtable_segdir",
  "kb_fts_vtable_segments",
  "kb_images",
  "kb_item_images",
  "kb_settings",
  "kb_txn_message_config",
  "sqlite_sequence"
];

const DEFAULT_SANITIZE_QUERIES = `UPDATE kb_names SET phone_number = '', email = '';
UPDATE kb_settings SET setting_value = '' WHERE setting_key = 'VYAPAR.TXNMSGOWNERNUMBER';
UPDATE kb_firms SET firm_phone = '665565', firm_email = '';
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.CATALOGUEID', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.CATALOGUEUID', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.CATALOGUEALIAS', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.AUTOSHAREINVOICESONVYAPARNETWORK', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('service_reminders_enabled', '0');
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.TXNMSGTOOWNER', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.TXNUPDATEMESSAGEENABLED', '0');
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.TRANSACTIONMESSAGEENABLED', '0');
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.VYAPAR.SYNCENABLED', '0');
UPDATE kb_transactions SET mobile_no = '';
UPDATE kb_transactions SET additional_details_json = NULL;
UPDATE repeat_invoice_template SET next_due_date = NULL, end_date = NULL, week_days = NULL, on_day = NULL, paused_until = NULL, txn_json = '{}';`;

const SQL_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "JOIN",
  "LEFT JOIN",
  "INNER JOIN",
  "ON",
  "GROUP BY",
  "ORDER BY",
  "HAVING",
  "LIMIT",
  "OFFSET",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "DISTINCT",
  "AS",
  "AND",
  "OR",
  "NOT",
  "IN",
  "LIKE",
  "BETWEEN",
  "IS NULL",
  "IS NOT NULL",
  "INSERT INTO",
  "UPDATE",
  "SET",
  "DELETE FROM",
  "CREATE TABLE",
  "ALTER TABLE",
  "DROP TABLE"
];

const SQL_SNIPPETS = [
  "SELECT *",
  "WHERE",
  "JOIN",
  "GROUP BY",
  "ORDER BY",
  "COUNT(*)",
  "INSERT",
  "UPDATE",
  "DELETE"
];

type NavigationItem = {
  label: string;
  table: string;
  query?: string;
  related?: {
    column: string;
    value: unknown;
  };
};

type SelectedRowState = {
  row: Record<string, unknown>;
  key: Record<string, unknown>;
};

type SqlTextCompareResult = {
  match: boolean;
  leftOnly: string[];
  rightOnly: string[];
  sharedCount: number;
  leftLineCount: number;
  rightLineCount: number;
};

type DiffWordPart = {
  text: string;
  changed: boolean;
};

type Screen = "compare" | "sqlCompare" | "sql" | "sanitizer" | "settings" | "fts";
type DetailTab = "modified" | "complete" | "db1" | "db2" | "schema";

function valueText(value: unknown) {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formattedValueText(value: unknown) {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "object") return JSON.stringify(value, null, 2);

  const text = String(value);
  const trimmed = text.trim();
  const looksLikeJson =
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"));

  if (!looksLikeJson) return text;

  try {
    return JSON.stringify(JSON.parse(trimmed), null, 2);
  } catch {
    return text;
  }
}

function escapeHtml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function highlightedSqlHtml(sql: string, tableNames: string[], columnNames: string[], errorToken = "") {
  const tableSet = new Set(tableNames.map((name) => name.toLowerCase()));
  const columnSet = new Set(columnNames.map((name) => name.toLowerCase()));
  const keywordSet = new Set(SQL_KEYWORDS.flatMap((keyword) => keyword.toUpperCase().split(/\s+/)));
  const tokenPattern = /("[^"]*"|'[^']*'|\b[A-Za-z_][A-Za-z0-9_]*\b|\d+(?:\.\d+)?|--.*?$|[(),.;*=<>+-])/gim;
  let cursor = 0;
  let html = "";

  for (const match of sql.matchAll(tokenPattern)) {
    const token = match[0];
    const index = match.index ?? 0;
    html += escapeHtml(sql.slice(cursor, index));

    const unquoted = token.replace(/^["']|["']$/g, "");
    const normalized = unquoted.toLowerCase();
    const upper = token.toUpperCase();
    let className = "";

    if (errorToken && unquoted.toLowerCase() === errorToken.toLowerCase()) className = "error-token";
    else if (token.startsWith("--")) className = "comment";
    else if (token.startsWith("'")) className = "string";
    else if (/^\d/.test(token)) className = "number";
    else if (tableSet.has(normalized)) className = "table-name";
    else if (columnSet.has(normalized)) className = "column-name";
    else if (keywordSet.has(upper)) className = "keyword";

    html += className ? `<span class="${className}">${escapeHtml(token)}</span>` : escapeHtml(token);
    cursor = index + token.length;
  }

  html += escapeHtml(sql.slice(cursor));
  return html || " ";
}

function formatSqlText(sql: string) {
  const normalized = sql
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .replace(/\s*;\s*/g, ";\n")
    .trim();
  const breakBefore = [
    "FROM",
    "WHERE",
    "LEFT JOIN",
    "INNER JOIN",
    "JOIN",
    "ON",
    "GROUP BY",
    "HAVING",
    "ORDER BY",
    "LIMIT",
    "OFFSET",
    "VALUES",
    "SET"
  ];

  return breakBefore
    .reduce((text, keyword) => {
      const pattern = new RegExp(`\\s+${keyword.replace(/\s+/g, "\\s+")}\\s+`, "gi");
      return text.replace(pattern, `\n${keyword} `);
    }, normalized)
    .replace(/,\s*/g, ",\n  ")
    .replace(/\n\s*\n/g, "\n")
    .trim();
}

function sqlErrorToken(message: string) {
  return message.match(/near\s+"([^"]+)"/i)?.[1] ?? "";
}
function compareSqlText(leftSql: string, rightSql: string, caseSensitive: boolean): SqlTextCompareResult {
  const normalizeLines = (query: string) =>
    formatSqlText(query)
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
  const signature = (line: string) => {
    const normalized = line.replace(/\s+/g, " ");
    return caseSensitive ? normalized : normalized.toLowerCase();
  };
  const leftLines = normalizeLines(leftSql);
  const rightLines = normalizeLines(rightSql);
  const leftBySignature = new Map(leftLines.map((line) => [signature(line), line]));
  const rightBySignature = new Map(rightLines.map((line) => [signature(line), line]));
  const leftKeys = new Set(leftBySignature.keys());
  const rightKeys = new Set(rightBySignature.keys());
  const leftOnly = [...leftKeys].filter((key) => !rightKeys.has(key)).map((key) => leftBySignature.get(key) ?? key);
  const rightOnly = [...rightKeys].filter((key) => !leftKeys.has(key)).map((key) => rightBySignature.get(key) ?? key);
  const sharedCount = [...leftKeys].filter((key) => rightKeys.has(key)).length;

  return {
    match: leftOnly.length === 0 && rightOnly.length === 0 && leftLines.length === rightLines.length,
    leftOnly,
    rightOnly,
    sharedCount,
    leftLineCount: leftLines.length,
    rightLineCount: rightLines.length
  };
}

function diffWordParts(left: string, right: string, caseSensitive: boolean): { left: DiffWordPart[]; right: DiffWordPart[] } {
  const leftWords = left.match(/\S+\s*/g) ?? [];
  const rightWords = right.match(/\S+\s*/g) ?? [];
  const normalize = (word: string) => caseSensitive ? word.trim() : word.trim().toLowerCase();
  const leftTokens = leftWords.map(normalize);
  const rightTokens = rightWords.map(normalize);
  const dp = Array.from({ length: leftTokens.length + 1 }, () => Array(rightTokens.length + 1).fill(0));

  for (let i = leftTokens.length - 1; i >= 0; i -= 1) {
    for (let j = rightTokens.length - 1; j >= 0; j -= 1) {
      dp[i][j] = leftTokens[i] === rightTokens[j]
        ? dp[i + 1][j + 1] + 1
        : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }

  const leftChanged = new Set<number>();
  const rightChanged = new Set<number>();
  let i = 0;
  let j = 0;
  while (i < leftTokens.length || j < rightTokens.length) {
    if (i < leftTokens.length && j < rightTokens.length && leftTokens[i] === rightTokens[j]) {
      i += 1;
      j += 1;
    } else if (j >= rightTokens.length || (i < leftTokens.length && dp[i + 1][j] >= dp[i][j + 1])) {
      leftChanged.add(i);
      i += 1;
    } else {
      rightChanged.add(j);
      j += 1;
    }
  }

  return {
    left: leftWords.map((text, index) => ({ text, changed: leftChanged.has(index) })),
    right: rightWords.map((text, index) => ({ text, changed: rightChanged.has(index) }))
  };
}

type JsonDiffRow = {
  path: string;
  status: "changed" | "added" | "removed";
  db1Value: unknown;
  db2Value: unknown;
};

function parseJsonValue(value: unknown): { ok: boolean; value: unknown } {
  if (value === null || value === undefined) return { ok: false, value };
  if (typeof value === "object") return { ok: true, value };

  const text = String(value).trim();
  const looksLikeJson =
    (text.startsWith("{") && text.endsWith("}")) ||
    (text.startsWith("[") && text.endsWith("]"));

  if (!looksLikeJson) return { ok: false, value };

  try {
    return { ok: true, value: JSON.parse(text) };
  } catch {
    return { ok: false, value };
  }
}

function stableJsonText(value: unknown) {
  return JSON.stringify(value);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function jsonPath(parent: string, key: string | number) {
  if (typeof key === "number") return `${parent}[${key}]`;
  return parent ? `${parent}.${key}` : key;
}

function diffJsonValues(db1Value: unknown, db2Value: unknown, path = ""): JsonDiffRow[] {
  if (stableJsonText(db1Value) === stableJsonText(db2Value)) return [];

  if (Array.isArray(db1Value) && Array.isArray(db2Value)) {
    const max = Math.max(db1Value.length, db2Value.length);
    const rows: JsonDiffRow[] = [];
    for (let i = 0; i < max; i += 1) {
      const nextPath = jsonPath(path, i);
      if (i >= db1Value.length) {
        rows.push({ path: nextPath, status: "added", db1Value: undefined, db2Value: db2Value[i] });
      } else if (i >= db2Value.length) {
        rows.push({ path: nextPath, status: "removed", db1Value: db1Value[i], db2Value: undefined });
      } else {
        rows.push(...diffJsonValues(db1Value[i], db2Value[i], nextPath));
      }
    }
    return rows;
  }

  if (isPlainObject(db1Value) && isPlainObject(db2Value)) {
    const keys = Array.from(new Set([...Object.keys(db1Value), ...Object.keys(db2Value)])).sort();
    const rows: JsonDiffRow[] = [];
    for (const key of keys) {
      const nextPath = jsonPath(path, key);
      if (!(key in db1Value)) {
        rows.push({ path: nextPath, status: "added", db1Value: undefined, db2Value: db2Value[key] });
      } else if (!(key in db2Value)) {
        rows.push({ path: nextPath, status: "removed", db1Value: db1Value[key], db2Value: undefined });
      } else {
        rows.push(...diffJsonValues(db1Value[key], db2Value[key], nextPath));
      }
    }
    return rows;
  }

  return [{ path: path || "$", status: "changed", db1Value, db2Value }];
}

function jsonDiffForValues(db1Value: unknown, db2Value: unknown) {
  const parsedDb1 = parseJsonValue(db1Value);
  const parsedDb2 = parseJsonValue(db2Value);
  if (!parsedDb1.ok || !parsedDb2.ok) return null;
  return diffJsonValues(parsedDb1.value, parsedDb2.value);
}

function splitCsv(value: string) {
  return value
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function toggleListValue(values: string[], value: string) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value];
}

function changedCount(table: TableDataResult) {
  return tableCount(table, "modified") + tableCount(table, "db1") + tableCount(table, "db2");
}

function hasAnyTableChange(table: TableDataResult) {
  return changedCount(table) > 0 || Boolean(table.column_schema_diff);
}

function schemaDiffCount(table: TableDataResult) {
  const diff = table.column_schema_diff;
  if (!diff) return 0;
  return diff.only_in_db1.length + diff.only_in_db2.length;
}

function tableCount(table: TableDataResult, kind: "modified" | "db1" | "db2") {
  if (kind === "modified") return table.modified_rows_count ?? table.modified_rows.length;
  if (kind === "db1") return table.rows_only_in_db1_count ?? table.rows_only_in_db1.length;
  return table.rows_only_in_db2_count ?? table.rows_only_in_db2.length;
}

function fileNameFromPath(filePath: string) {
  return filePath.split(/[\\/]/).pop() || "database-output";
}

function makeEditableRowKey(row: Record<string, unknown>, columns: TableColumn[], visibleColumns: string[]) {
  const pkColumns = columns
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name)
    .filter((column) => Object.prototype.hasOwnProperty.call(row, column));
  const keyColumns = pkColumns.length ? pkColumns : visibleColumns;
  return Object.fromEntries(keyColumns.map((column) => [column, row[column]]));
}

function rowMatchesKey(row: Record<string, unknown>, key: Record<string, unknown>) {
  return Object.entries(key).every(([column, value]) => row[column] === value);
}

async function pickDatabase(setter: (path: string) => void) {
  const selected = await window.dbcompare?.openDatabase();
  if (selected) setter(selected);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function App() {
  const [screen, setScreen] = useState<Screen>("sql");
  const [collapsed, setCollapsed] = useState(true);
  const [db1, setDb1] = useState("");
  const [db2, setDb2] = useState("");
  const [activeDb, setActiveDb] = useState("");
  const [backendReady, setBackendReady] = useState(false);
  const [backendError, setBackendError] = useState<string | null>(null);

  useEffect(() => {
    const launchFile = window.dbcompare?.initialOpenFile;
    if (!launchFile) return;
    setActiveDb(launchFile);
    setScreen("sql");
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function pollBackend() {
      while (!cancelled) {
        try {
          const status = await window.dbcompare?.backendStatus();
          if (cancelled) return;
          if (status?.ok) {
            setBackendReady(true);
            setBackendError(null);
            return;
          }
          setBackendError(status?.error ?? null);
        } catch (error) {
          if (cancelled) return;
          setBackendError(error instanceof Error ? error.message : "Local service is still starting.");
        }
        await sleep(350);
      }
    }

    pollBackend();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <main className={`app-shell ${collapsed ? "nav-collapsed" : ""}`}>
      <aside className="sidebar">
        <div className="brand-row">
          <div className="brand">
            <img src="./icon-512.png" alt="" />
            {!collapsed && (
              <div>
                <strong>DB Explorer Pro</strong>
              </div>
            )}
          </div>
        </div>
        <nav>
          <button className={screen === "sql" ? "active" : ""} onClick={() => setScreen("sql")} title="Open DB" aria-label="Open DB" data-label="Open DB">
            <img className="menu-icon" src="./menu-icons/open-db.png" alt="" />
            {!collapsed && <span>Open DB</span>}
          </button>
          <button className={screen === "compare" ? "active" : ""} onClick={() => setScreen("compare")} title="DB Compare" aria-label="DB Compare" data-label="DB Compare">
            <img className="menu-icon" src="./menu-icons/db-compare.png" alt="" />
            {!collapsed && <span>DB Compare</span>}
          </button>
          <button className={screen === "sqlCompare" ? "active" : ""} onClick={() => setScreen("sqlCompare")} title="SQL Compare" aria-label="SQL Compare" data-label="SQL Compare">
            <GitCompare className="menu-icon" size={20} />
            {!collapsed && <span>SQL Compare</span>}
          </button>
          <button className={screen === "sanitizer" ? "active" : ""} onClick={() => setScreen("sanitizer")} title="DB Sanitizer" aria-label="DB Sanitizer" data-label="DB Sanitizer">
            <img className="menu-icon" src="./menu-icons/db-sanitizer.png" alt="" />
            {!collapsed && <span>DB Sanitizer</span>}
          </button>
          <button className={screen === "settings" ? "active" : ""} onClick={() => setScreen("settings")} title="Setting Table Repair" aria-label="Setting Table Repair" data-label="Setting Table Repair">
            <img className="menu-icon" src="./menu-icons/setting-repair.png" alt="" />
            {!collapsed && <span>Setting Table Repair</span>}
          </button>
          <button className={screen === "fts" ? "active" : ""} onClick={() => setScreen("fts")} title="FTS Table Generator" aria-label="FTS Table Generator" data-label="FTS Table Generator">
            <img className="menu-icon" src="./menu-icons/fts.png" alt="" />
            {!collapsed && <span>FTS Table Generator</span>}
          </button>
        </nav>
        <button className="icon-button sidebar-collapse" title={collapsed ? "Expand menu" : "Collapse menu"} onClick={() => setCollapsed((value) => !value)}>
          {collapsed ? <ArrowRightToLine size={18} /> : <ArrowLeftToLine size={18} />}
        </button>
      </aside>

      <section className="workspace">
        {!backendReady ? (
          <section className="sql-empty-start">
            <div className="hero-summary startup-summary">
              <div>
                <h1>Starting DB Explorer Pro</h1>
                <p>The window is ready. The local database service is warming up in the background.</p>
              </div>
              <div className="summary-pills">
                <span>{backendError ? backendError : "Starting local service..."}</span>
              </div>
            </div>
          </section>
        ) : screen === "compare" ? (
          <CompareWorkspace db1={db1} db2={db2} setDb1={setDb1} setDb2={setDb2} />
        ) : screen === "sqlCompare" ? (
          <SqlCompareWorkspace />
        ) : screen === "sql" ? (
          <SqlExplorer dbPath={activeDb} setDbPath={setActiveDb} />
        ) : screen === "sanitizer" ? (
          <SanitizerWorkspace dbPath={activeDb} setDbPath={setActiveDb} />
        ) : screen === "settings" ? (
          <SettingsRepairWorkspace dbPath={activeDb} setDbPath={setActiveDb} />
        ) : (
          <FtsWorkspace dbPath={activeDb} setDbPath={setActiveDb} />
        )}
      </section>
    </main>
  );
}

function PathPicker({
  label,
  value,
  onChange,
  icon = "folder"
}: {
  label: string;
  value: string;
  onChange: (path: string) => void;
  icon?: "search" | "folder";
}) {
  return (
    <label className="path-picker">
      <span>{label}</span>
      <input value={value} onChange={(event) => onChange(event.target.value)} placeholder="Select database file" />
      <button type="button" title="Browse database file" onClick={() => pickDatabase(onChange)}>
        {icon === "folder" ? <FolderOpen size={17} /> : <Search size={17} />}
      </button>
    </label>
  );
}

function MultiSelectDropdown({
  label,
  values,
  options,
  onChange
}: {
  label: string;
  values: string[];
  options: string[];
  onChange: (values: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const summary = values.length ? `${values.length} selected` : "None selected";

  useEffect(() => {
    if (!open) return;

    function handlePointerDown(event: PointerEvent) {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
      }
    }

    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [open]);

  return (
    <div className="multi-select" ref={rootRef}>
      <span>{label}</span>
      <button type="button" className="multi-select-trigger" onClick={() => setOpen((value) => !value)}>
        <span>{summary}</span>
        <small>{values.slice(0, 2).join(", ")}{values.length > 2 ? "..." : ""}</small>
      </button>
      {open && (
        <div className="multi-select-menu">
          <div className="multi-select-actions">
            <button type="button" onClick={() => onChange(options)}>All</button>
            <button type="button" onClick={() => onChange([])}>None</button>
          </div>
          {options.map((option) => (
            <label key={option}>
              <input
                type="checkbox"
                checked={values.includes(option)}
                onChange={() => onChange(toggleListValue(values, option))}
              />
              <span>{option}</span>
            </label>
          ))}
        </div>
      )}
    </div>
  );
}

function ToolOutput({ result }: { result: ToolRunResult | null }) {
  const [downloadStatus, setDownloadStatus] = useState("");

  if (!result) {
    return <div className="muted-block compact">No output yet</div>;
  }

  async function saveOutput(sourcePath: string, label: string) {
    if (!window.dbcompare?.saveGeneratedFile) {
      setDownloadStatus("Save dialog is not available. Restart the desktop app and try again.");
      return;
    }

    setDownloadStatus("");
    try {
      const response = await window.dbcompare.saveGeneratedFile(sourcePath, fileNameFromPath(sourcePath));
      if (response.error) {
        setDownloadStatus(response.error);
      } else if (response.saved && response.path) {
        setDownloadStatus(`${label} saved to ${response.path}`);
      } else {
        setDownloadStatus("Save cancelled.");
      }
    } catch (error) {
      setDownloadStatus(error instanceof Error ? error.message : "Save failed.");
    }
  }

  const messages = result.messages.filter(
    (item, index, list) =>
      index === list.findIndex((entry) => entry.percent === item.percent && entry.message === item.message)
  );

  return (
    <div className="tool-output">
      <div className="tool-output-files">
        <div>
          <span>Output .vyb</span>
          <code>{result.output_vyb}</code>
        </div>
        <div>
          <span>Output .vyp</span>
          <code>{result.output_vyp}</code>
        </div>
      </div>
      <div className="tool-downloads">
        <button type="button" onClick={() => saveOutput(result.output_vyb, ".vyb")}>
          <Download size={16} /> Download .vyb
        </button>
        <button type="button" onClick={() => saveOutput(result.output_vyp, ".vyp")}>
          <Download size={16} /> Download .vyp
        </button>
        {downloadStatus && <span>{downloadStatus}</span>}
      </div>
      {result.summary && (
        <div className="tool-summary">
          {Object.entries(result.summary).map(([key, value]) => (
            <span key={key}>
              <b>{key.replace(/_/g, " ")}</b>
              <em>{valueText(value)}</em>
            </span>
          ))}
        </div>
      )}
      <div className="tool-log">
        <strong>Output log</strong>
        {messages.map((item, index) => (
          <p key={index}>
            <b>{item.percent}%</b>
            <span>{item.message}</span>
          </p>
        ))}
      </div>
    </div>
  );
}

function SanitizerWorkspace({
  dbPath,
  setDbPath
}: {
  dbPath: string;
  setDbPath: (path: string) => void;
}) {
  const [queries, setQueries] = useState(DEFAULT_SANITIZE_QUERIES);
  const [sanitizeResult, setSanitizeResult] = useState<ToolRunResult | null>(null);
  const [status, setStatus] = useState("Select a database, then run sanitizer.");
  const [running, setRunning] = useState<"sanitize" | "convert" | null>(null);

  const queryList = useMemo(
    () => queries.split(";").map((query) => query.trim()).filter(Boolean),
    [queries]
  );

  function resetSanitizer() {
    setDbPath("");
    setQueries(DEFAULT_SANITIZE_QUERIES);
    setSanitizeResult(null);
    setRunning(null);
    setStatus("Select a database, then run sanitizer.");
  }

  async function runSanitize() {
    if (!dbPath) {
      setStatus("Select a database first.");
      return;
    }
    if (!queryList.length) {
      setStatus("Add at least one SQL query before sanitizing.");
      return;
    }
    setRunning("sanitize");
    setStatus("Sanitizing database");
    try {
      const result = await sanitizeDatabase(dbPath, queryList);
      setSanitizeResult(result);
      setStatus(`Sanitized database created: ${result.output_vyb}`);
    } catch (error) {
      setStatus(error instanceof Error ? `Sanitizer failed: ${error.message}` : "Sanitizer failed.");
    } finally {
      setRunning(null);
    }
  }

  async function runConvert() {
    if (!dbPath) {
      setStatus("Select a database first.");
      return;
    }
    setRunning("convert");
    setStatus("Converting / repacking database");
    try {
      const result = await convertDatabase(dbPath);
      setSanitizeResult(result);
      setStatus(`Converted database created: ${result.output_vyb}`);
    } catch (error) {
      setStatus(error instanceof Error ? `Convert failed: ${error.message}` : "Convert failed.");
    } finally {
      setRunning(null);
    }
  }

  return (
    <>
      <section className="compare-form">
        <div className="form-card input-card single">
          <PathPicker label="Database" value={dbPath} onChange={setDbPath} />
          <button className="reset-button" onClick={resetSanitizer} disabled={Boolean(running)}>
            <RotateCcw size={16} /> Reset
          </button>
        </div>
      </section>

      <section className="tools-grid single-tool">
        <section className="panel tool-panel">
          <div className="panel-title">
            <Wrench size={18} />
            <strong>DB Sanitizer</strong>
          </div>
          <p className="tool-copy">Runs SQL against a copied database to generate fresh .vyp and .vyb outputs. Also supports seamless conversion between .vyp and .vyb formats. </p>
          <textarea
            className="tool-sql"
            value={queries}
            onChange={(event) => setQueries(event.target.value)}
            spellCheck={false}
          />
          <div className="tool-actions">
            <button className="primary" onClick={runSanitize} disabled={Boolean(running)}>
              <Play size={17} /> {running === "sanitize" ? "Sanitizing" : "Run sanitizer"}
            </button>
            <button onClick={runConvert} disabled={Boolean(running)}>
              <RefreshCcw size={17} /> {running === "convert" ? "Converting" : "Convert / Repack"}
            </button>
          </div>
          <ToolOutput result={sanitizeResult} />
        </section>
      </section>

      {running && (
        <div className="compare-overlay">
          <div className="compare-loader" />
          <strong>{running === "convert" ? "Converting database" : "Sanitizing database"}</strong>
          <span>Working on a copied output file</span>
        </div>
      )}

      <footer className="statusbar">{status}</footer>
    </>
  );
}

function FtsWorkspace({
  dbPath,
  setDbPath
}: {
  dbPath: string;
  setDbPath: (path: string) => void;
}) {
  const [ftsResult, setFtsResult] = useState<ToolRunResult | null>(null);
  const [status, setStatus] = useState("Select a database, then build the FTS table.");
  const [running, setRunning] = useState(false);

  function resetFts() {
    setDbPath("");
    setFtsResult(null);
    setRunning(false);
    setStatus("Select a database, then build the FTS table.");
  }

  async function runFts() {
    if (!dbPath) {
      setStatus("Select a database first.");
      return;
    }
    setRunning(true);
    setStatus("Building FTS table");
    try {
      const result = await buildFtsDatabase(dbPath);
      setFtsResult(result);
      setStatus(`FTS database created: ${result.output_vyb}`);
    } catch (error) {
      setStatus(error instanceof Error ? `FTS build failed: ${error.message}` : "FTS build failed.");
    } finally {
      setRunning(false);
    }
  }

  return (
    <>
      <section className="compare-form">
        <div className="form-card input-card single">
          <PathPicker label="Database" value={dbPath} onChange={setDbPath} />
          <button className="reset-button" onClick={resetFts} disabled={running}>
            <RotateCcw size={16} /> Reset
          </button>
        </div>
      </section>

      <section className="tools-grid single-tool">
        <section className="panel tool-panel">
          <div className="panel-title">
            <Rows3 size={18} />
            <strong>FTS Table Generator</strong>
          </div>
          <p className="tool-copy">
            Rebuilds the Vyapar FTS3 search table on a copy. It drops old FTS tables, creates kb_fts_vtable, and repacks the result.
          </p>
          <div className="fts-summary">
            <span>Indexes party names, phone numbers, transaction text, invoice numbers, item details, serials, and payments.</span>
          </div>
          <div className="tool-actions">
            <button className="primary" onClick={runFts} disabled={running}>
              <Play size={17} /> {running ? "Building FTS" : "Build FTS table"}
            </button>
          </div>
          <ToolOutput result={ftsResult} />
        </section>
      </section>

      {running && (
        <div className="compare-overlay">
          <div className="compare-loader" />
          <strong>Building FTS table</strong>
          <span>Working on a copied output file</span>
        </div>
      )}

      <footer className="statusbar">{status}</footer>
    </>
  );
}

function SettingsRepairWorkspace({
  dbPath,
  setDbPath
}: {
  dbPath: string;
  setDbPath: (path: string) => void;
}) {
  const [repairResult, setRepairResult] = useState<ToolRunResult | null>(null);
  const [status, setStatus] = useState("Select a database, then repair the settings table.");
  const [running, setRunning] = useState(false);

  function resetSettingsRepair() {
    setDbPath("");
    setRepairResult(null);
    setRunning(false);
    setStatus("Select a database, then repair the settings table.");
  }

  async function runRepair() {
    if (!dbPath) {
      setStatus("Select a database first.");
      return;
    }
    setRunning(true);
    setStatus("Repairing kb_settings table");
    try {
      const result = await repairSettingsTable(dbPath);
      setRepairResult(result);
      setStatus(`Settings table repaired: ${result.output_vyb}`);
    } catch (error) {
      if (error instanceof Error && /not found/i.test(error.message)) {
        setStatus("Settings repair route is not available in the running backend. Restart the app and try again.");
      } else {
        setStatus(error instanceof Error ? `Settings repair failed: ${error.message}` : "Settings repair failed.");
      }
    } finally {
      setRunning(false);
    }
  }

  return (
    <>
      <section className="compare-form">
        <div className="form-card input-card single">
          <PathPicker label="Database" value={dbPath} onChange={setDbPath} />
          <button className="reset-button" onClick={resetSettingsRepair} disabled={running}>
            <RotateCcw size={16} /> Reset
          </button>
        </div>
      </section>

      <section className="tools-grid single-tool">
        <section className="panel tool-panel">
          <div className="panel-title">
            <Table2 size={18} />
            <strong>Setting Table Repair</strong>
          </div>
          <p className="tool-copy">
            Repairs kb_settings on a copied database by recreating the table and reinserting existing settings while resolving duplicate IDs and keys.
          </p>
          <div className="fts-summary">
            <span>Input data is preserved in the source file. Outputs are generated as repaired .vyp and .vyb copies.</span>
          </div>
          <div className="tool-actions">
            <button className="primary" onClick={runRepair} disabled={running}>
              <Wrench size={17} /> {running ? "Repairing" : "Repair settings table"}
            </button>
          </div>
          <ToolOutput result={repairResult} />
        </section>
      </section>

      {running && (
        <div className="compare-overlay">
          <div className="compare-loader" />
          <strong>Repairing settings table</strong>
          <span>Working on a copied output file</span>
        </div>
      )}

      <footer className="statusbar">{status}</footer>
    </>
  );
}

function CompareWorkspace({
  db1,
  db2,
  setDb1,
  setDb2
}: {
  db1: string;
  db2: string;
  setDb1: (path: string) => void;
  setDb2: (path: string) => void;
}) {
  const [include, setInclude] = useState("");
  const [excludedTables, setExcludedTables] = useState<string[]>(DEFAULT_EXCLUDES);
  const [ignoreDates, setIgnoreDates] = useState(true);
  const [validate, setValidate] = useState(true);
  const [precision, setPrecision] = useState(5);
  const [report, setReport] = useState<ComparisonReport | null>(null);
  const [detailTable, setDetailTable] = useState<TableDataResult | null>(null);
  const [status, setStatus] = useState("Ready");
  const [isComparing, setIsComparing] = useState(false);
  const [compareStartedAt, setCompareStartedAt] = useState<number | null>(null);
  const [compareDuration, setCompareDuration] = useState<number | null>(null);
  const [compareController, setCompareController] = useState<AbortController | null>(null);
  const [compareJobId, setCompareJobId] = useState<string | null>(null);
  const [compareProgress, setCompareProgress] = useState({ message: "", percent: 0 });

  const tableResults = useMemo(() => report?.data ?? [], [report]);
  const affectedTables = tableResults.filter(hasAnyTableChange);
  const totalRowChanges = tableResults.reduce((sum, table) => sum + changedCount(table), 0);

  async function cancelCompare() {
    compareController?.abort();
    if (compareJobId) {
      await cancelCompareJob(compareJobId).catch(() => null);
    }
    setIsComparing(false);
    setCompareJobId(null);
    setStatus("Comparison cancelled");
  }

  function resetCompare() {
    compareController?.abort();
    setDb1("");
    setDb2("");
    setInclude("");
    setExcludedTables(DEFAULT_EXCLUDES);
    setIgnoreDates(true);
    setValidate(true);
    setPrecision(5);
    setReport(null);
    setDetailTable(null);
    setIsComparing(false);
    setCompareStartedAt(null);
    setCompareDuration(null);
    setCompareController(null);
    setCompareJobId(null);
    setCompareProgress({ message: "", percent: 0 });
    setStatus("Ready");
  }

  async function runCompare() {
    if (!db1 || !db2) {
      setStatus("Select both databases before comparing");
      return;
    }
    const controller = new AbortController();
    const started = performance.now();
    setCompareController(controller);
    setCompareStartedAt(started);
    setCompareDuration(null);
    setIsComparing(true);
    setCompareProgress({ message: "Starting comparison", percent: 0 });
    setStatus("Starting comparison");
    setDetailTable(null);
    try {
      const payload = {
        db1_path: db1,
        db2_path: db2,
        included_tables: splitCsv(include),
        excluded_tables: excludedTables,
        ignore_datetime: ignoreDates,
        decimal_precision: precision,
        validate_db: validate,
        max_result_rows_per_table: 500
      };
      const { job_id: jobId } = await startCompareJob(payload, controller.signal);
      setCompareJobId(jobId);

      let completed = false;
      while (!completed) {
        await sleep(500);
        if (controller.signal.aborted) {
          await cancelCompareJob(jobId).catch(() => null);
          throw new DOMException("Comparison cancelled", "AbortError");
        }
        const job = await getCompareJob(jobId, controller.signal);
        setCompareProgress({ message: job.message, percent: job.percent });
        setStatus(`${job.message}${job.percent ? ` (${job.percent}%)` : ""}`);
        if (job.status === "completed") {
          completed = true;
        } else if (job.status === "cancelled") {
          throw new DOMException("Comparison cancelled", "AbortError");
        } else if (job.status === "failed") {
          throw new Error(job.error || job.message || "Comparison failed");
        }
      }

      const nextReport = await getCompareJobResult(jobId, controller.signal);
      const seconds = (performance.now() - started) / 1000;
      setReport(nextReport);
      setCompareDuration(seconds);
      setStatus(
        nextReport.data.some(hasAnyTableChange)
          ? `Comparison completed in ${seconds.toFixed(2)}s - differences found`
          : `Comparison completed in ${seconds.toFixed(2)}s`
      );
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        setStatus("Comparison cancelled");
      } else {
        setStatus(error instanceof Error ? `Comparison failed: ${error.message}` : "Comparison failed");
      }
    } finally {
      setIsComparing(false);
      setCompareController(null);
      setCompareJobId(null);
    }
  }

  return (
    <>
      <section className="compare-form">
        <div className="form-card input-card compare-input-card">
          <PathPicker label="Database 1" value={db1} onChange={setDb1} />
          <PathPicker label="Database 2" value={db2} onChange={setDb2} />
          <button className="reset-button" onClick={resetCompare} disabled={isComparing}>
            <RotateCcw size={16} /> Reset
          </button>
        </div>

        <div className="form-card option-card">
          <label>
            Include table
            <input value={include} onChange={(event) => setInclude(event.target.value)} placeholder="Optional: table1, table2" />
          </label>
          <MultiSelectDropdown
            label="Exclude table"
            values={excludedTables}
            options={DEFAULT_EXCLUDES}
            onChange={setExcludedTables}
          />
          <label className="toggle">
            <input type="checkbox" checked={validate} onChange={(event) => setValidate(event.target.checked)} />
            <span>Validate table</span>
          </label>
          <label className="toggle">
            <input type="checkbox" checked={ignoreDates} onChange={(event) => setIgnoreDates(event.target.checked)} />
            <span>Ignore date</span>
          </label>
          <label className="precision-field">
            Decimal precision
            <input
              type="number"
              min={0}
              max={15}
              value={precision}
              onChange={(event) => setPrecision(Number(event.target.value))}
            />
          </label>
          <button className="primary compare-button" onClick={runCompare} disabled={isComparing}>
            <Search size={18} /> Compare
          </button>
          {isComparing && (
            <button className="cancel-button" onClick={cancelCompare}>
              Cancel
            </button>
          )}
        </div>
      </section>

      <section className="result-layout">
        <ResultSummary
          report={report}
          affectedTables={affectedTables.length}
          totalRowChanges={totalRowChanges}
          isComparing={isComparing}
          compareDuration={compareDuration}
        />
        <ComparisonTable tables={tableResults} onViewMore={setDetailTable} />
        <SchemaPanel report={report} />
      </section>

      {isComparing && (
        <div className="compare-overlay">
          <div className="compare-loader" />
          <strong>{compareProgress.message || "Comparing databases"}</strong>
          <div className="progress-track">
            <span style={{ width: `${compareProgress.percent}%` }} />
          </div>
          <span>{compareStartedAt ? `${((performance.now() - compareStartedAt) / 1000).toFixed(1)}s elapsed` : "Working"}</span>
          <button onClick={cancelCompare}>Cancel</button>
        </div>
      )}

      <footer className="statusbar">{status}</footer>
      {detailTable && <TableDetailModal table={detailTable} db1Path={db1} db2Path={db2} onClose={() => setDetailTable(null)} />}
    </>
  );
}

function SqlCompareWorkspace() {
  const [leftSql, setLeftSql] = useState("");
  const [rightSql, setRightSql] = useState("");
  const [caseSensitive, setCaseSensitive] = useState(false);
  const [compareResult, setCompareResult] = useState<SqlTextCompareResult | null>(null);

  function runCompare() {
    if (!leftSql.trim() || !rightSql.trim()) {
      setCompareResult(null);
      return;
    }
    setCompareResult(compareSqlText(leftSql, rightSql, caseSensitive));
  }

  useEffect(() => {
    if (!compareResult) return;
    if (!leftSql.trim() || !rightSql.trim()) {
      setCompareResult(null);
      return;
    }
    setCompareResult(compareSqlText(leftSql, rightSql, caseSensitive));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [caseSensitive]);

  return (
    <section className="standalone-sql-compare">
      <QueryComparePanel
        leftSql={leftSql}
        rightSql={rightSql}
        setLeftSql={setLeftSql}
        setRightSql={setRightSql}
        caseSensitive={caseSensitive}
        setCaseSensitive={setCaseSensitive}
        compareResult={compareResult}
        onCompare={runCompare}
      />
    </section>
  );
}

function ResultSummary({
  report,
  affectedTables,
  totalRowChanges,
  isComparing,
  compareDuration
}: {
  report: ComparisonReport | null;
  affectedTables: number;
  totalRowChanges: number;
  isComparing: boolean;
  compareDuration: number | null;
}) {
  const versionDiffers = report ? report.version.db1_version !== report.version.db2_version : false;
  const schemaChanges = report
    ? report.schema.added_tables.length + report.schema.removed_tables.length + report.schema.column_diffs.length
    : 0;

  return (
    <section className="hero-summary">
      <div>
        <span className="eyebrow">Comparison result</span>
        <h1>{report ? (affectedTables || schemaChanges || versionDiffers ? "Differences found" : "Databases match") : "Ready to compare"}</h1>
        <p>
          {isComparing
            ? "Comparison is running. Large databases can take a little time."
            : report
            ? `${affectedTables} table(s) affected, ${totalRowChanges} row change(s), ${schemaChanges} schema change(s)`
            : "Select two database files, tune filters, and run comparison."}
        </p>
      </div>
      <div className="summary-pills">
        <span>DB1 Version: {report?.version.db1_version ?? "-"}</span>
        <span>DB2 Version: {report?.version.db2_version ?? "-"}</span>
        {compareDuration !== null && <span>Completed in {compareDuration.toFixed(2)}s</span>}
        {versionDiffers && <span className="warning">Versions differ</span>}
      </div>
    </section>
  );
}

function ComparisonTable({
  tables,
  onViewMore
}: {
  tables: TableDataResult[];
  onViewMore: (table: TableDataResult) => void;
}) {
  const [tableFilter, setTableFilter] = useState("");
  const [changedOnly, setChangedOnly] = useState(true);
  const visibleTables = tables.filter((table) => {
    const matchesSearch = table.table.toLowerCase().includes(tableFilter.toLowerCase());
    const matchesChange = !changedOnly || hasAnyTableChange(table);
    return matchesSearch && matchesChange;
  });

  return (
    <section className="panel comparison-panel">
      <div className="panel-title">
        <Table2 size={18} />
        <strong>Data Changes</strong>
        <span>{visibleTables.length}/{tables.length}</span>
      </div>
      <div className="table-tools">
        <label className="table-filter">
          <Search size={16} />
          <input
            value={tableFilter}
            onChange={(event) => setTableFilter(event.target.value)}
            placeholder="Search tables: kb_names, txn, item..."
          />
        </label>
        <div className="segmented">
          <button className={!changedOnly ? "active" : ""} onClick={() => setChangedOnly(false)}>All tables</button>
          <button className={changedOnly ? "active" : ""} onClick={() => setChangedOnly(true)}>Changed only</button>
        </div>
      </div>
      <div className="summary-table">
        <table>
          <thead>
            <tr>
              <th>Table Name</th>
              <th>Schema Change</th>
              <th>Data Differences</th>
              <th>DB1 Only</th>
              <th>DB2 Only</th>
              <th>View More Details</th>
            </tr>
          </thead>
          <tbody>
            {visibleTables.map((table) => {
              const schemaDiffers = Boolean(table.column_schema_diff);
              const rowChanges = changedCount(table);
              return (
                <tr key={table.table} className={hasAnyTableChange(table) ? "has-change" : "clean"}>
                  <td>{table.table}</td>
                  <td className={schemaDiffers ? "warn" : "ok"}>
                    {schemaDiffers ? `Changed (${schemaDiffCount(table)})` : "Match"}
                  </td>
                  <td>{tableCount(table, "modified") || "-"}</td>
                  <td>{tableCount(table, "db1") || "-"}</td>
                  <td>{tableCount(table, "db2") || "-"}</td>
                  <td>
                    {(rowChanges || schemaDiffers) ? (
                      <button className="link-button" onClick={() => onViewMore(table)}>
                        View <ArrowRight size={14} />
                      </button>
                    ) : (
                      <span className="muted-text">-</span>
                    )}
                  </td>
                </tr>
              );
            })}
            {!visibleTables.length && (
              <tr>
                <td colSpan={6}>
                  <NoData message="No tables match the current filter" />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SchemaPanel({ report }: { report: ComparisonReport | null }) {
  const totalChanges = report
    ? report.schema.added_tables.length + report.schema.removed_tables.length + report.schema.column_diffs.length
    : 0;

  return (
    <section className="panel schema-panel">
      <div className="panel-title">
        <AlertTriangle size={18} />
        <strong>Schema</strong>
        <span>{totalChanges}</span>
      </div>
      <div className="schema-list">
        {!report && <div className="muted-block">No schema loaded</div>}
        {report && totalChanges === 0 && <div className="muted-block compact">No schema differences</div>}
        {report?.schema.added_tables.map((table) => (
          <div className="schema-item added" key={`added-${table}`}>
            <strong>New table added</strong>
            <span>{table} exists in Database 2 only.</span>
          </div>
        ))}
        {report?.schema.removed_tables.map((table) => (
          <div className="schema-item removed" key={`removed-${table}`}>
            <strong>Table missing in Database 2</strong>
            <span>{table} exists in Database 1 only.</span>
          </div>
        ))}
        {report?.schema.column_diffs.map((diff) => (
          <div className="schema-item changed" key={diff.table}>
            <strong>Column changes in {diff.table}</strong>
            <span>
              {diff.only_in_db2.length
                ? `New column(s) added in Database 2: ${diff.only_in_db2.join(", ")}`
                : "No new columns in Database 2."}
            </span>
            <span>
              {diff.only_in_db1.length
                ? `Column(s) only in Database 1: ${diff.only_in_db1.join(", ")}`
                : "No Database 1-only columns."}
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

function TableDetailModal({
  table,
  db1Path,
  db2Path,
  onClose
}: {
  table: TableDataResult;
  db1Path: string;
  db2Path: string;
  onClose: () => void;
}) {
  const schemaChanges = schemaDiffCount(table);
  const [tab, setTab] = useState<DetailTab>(
    tableCount(table, "modified")
      ? "modified"
      : tableCount(table, "db1")
        ? "db1"
        : tableCount(table, "db2")
          ? "db2"
          : schemaChanges
            ? "schema"
            : "complete"
  );

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <section className="detail-modal" onClick={(event) => event.stopPropagation()}>
        <header>
          <div>
            <span className="eyebrow">View more details</span>
            <h2>{table.table}</h2>
          </div>
          <button onClick={onClose}>Close</button>
        </header>
        <div className="detail-tabs">
          <button className={`${tab === "modified" ? "active" : ""} ${tableCount(table, "modified") > 0 ? "has-count" : ""}`} onClick={() => setTab("modified")}>Values Changed ({tableCount(table, "modified")})</button>
          <button className={`${tab === "db1" ? "active" : ""} ${tableCount(table, "db1") > 0 ? "has-count" : ""}`} onClick={() => setTab("db1")}>DB1 only ({tableCount(table, "db1")})</button>
          <button className={`${tab === "db2" ? "active" : ""} ${tableCount(table, "db2") > 0 ? "has-count" : ""}`} onClick={() => setTab("db2")}>DB2 only ({tableCount(table, "db2")})</button>
          <button className={tab === "complete" ? "active" : ""} onClick={() => setTab("complete")}>Complete Data</button>
          <button className={`${tab === "schema" ? "active" : ""} ${schemaChanges > 0 ? "has-count" : ""}`} onClick={() => setTab("schema")}>
            Schema Change ({schemaChanges})
          </button>
        </div>
        <div className="detail-content">
          {tab === "modified" && <ModifiedGrid table={table} />}
          {tab === "complete" && <CompleteDataView table={table} db1Path={db1Path} db2Path={db2Path} />}
          {tab === "db1" && <ObjectGrid rows={table.rows_only_in_db1} />}
          {tab === "db2" && <ObjectGrid rows={table.rows_only_in_db2} />}
          {tab === "schema" && <SchemaDetail table={table} />}
        </div>
      </section>
    </div>
  );
}

function ModifiedGrid({ table }: { table: TableDataResult }) {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [filter, setFilter] = useState("");
  const [fieldDetail, setFieldDetail] = useState<{
    column: string;
    db1Value: unknown;
    db2Value: unknown;
  } | null>(null);
  const selected = table.modified_rows[selectedIndex] ?? table.modified_rows[0];
  const totalFieldChanges = table.modified_rows.reduce((sum, row) => sum + row.column_changes.length, 0);
  const modifiedTotal = tableCount(table, "modified");

  const rowSummaries = table.modified_rows.map((row, index) => ({
    index,
    pk: Object.entries(row.pk).map(([key, value]) => `${key}=${valueText(value)}`).join(", "),
    count: row.column_changes.length
  }));

  const visibleRows = rowSummaries.filter((row) =>
    row.pk.toLowerCase().includes(filter.toLowerCase())
  );

  const visibleChanges = selected
    ? selected.column_changes.filter(([column, db1Value, db2Value]) => {
        const text = `${column} ${valueText(db1Value)} ${valueText(db2Value)}`.toLowerCase();
        return text.includes(filter.toLowerCase());
      })
    : [];

  if (!table.modified_rows.length) return <NoData message="No modified rows available" />;

  return (
    <div className="modified-workspace">
      <div className="diff-summary-strip">
        <span>{modifiedTotal} modified row(s)</span>
        <span>{totalFieldChanges} changed field(s)</span>
        <span>{selected ? `${selected.column_changes.length} in selected row` : "No row selected"}</span>
        {table.result_limited && <span>Showing first {table.modified_rows.length} sampled row(s)</span>}
      </div>

      <label className="diff-filter">
        <Search size={16} />
        <input
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Search primary key, column, or value"
        />
      </label>

      <div className="modified-master-detail">
        <aside className="changed-row-list">
          <div className="subhead">Changed Rows</div>
          {visibleRows.map((row) => (
            <button
              key={row.index}
              className={row.index === selectedIndex ? "active" : ""}
              onClick={() => setSelectedIndex(row.index)}
            >
              <span>{row.pk}</span>
              <small>{row.count} field(s)</small>
            </button>
          ))}
        </aside>

        <section className="field-diff-panel">
          <div className="selected-row-title">
            <span>Selected row</span>
            <strong>{selected ? Object.entries(selected.pk).map(([key, value]) => `${key}=${valueText(value)}`).join(", ") : "-"}</strong>
          </div>
          <div className="field-diff-table-wrap">
            <table className="detail-grid field-diff-grid">
              <thead>
                <tr>
                  <th>Column</th>
                  <th>Database 1</th>
                  <th>Database 2</th>
                  <th>Change</th>
                </tr>
              </thead>
              <tbody>
                {visibleChanges.map(([column, db1Value, db2Value]) => (
                  <tr key={column}>
                    <td>{column}</td>
                    <td className="removed-cell">
                      <div className="long-value">{formattedValueText(db1Value)}</div>
                    </td>
                    <td className="added-cell">
                      <div className="long-value">{formattedValueText(db2Value)}</div>
                    </td>
                    <td>
                      <button
                        type="button"
                        className="change-inline-button"
                        title="Open full change details"
                        onClick={() => setFieldDetail({ column, db1Value, db2Value })}
                      >
                        <div className="change-inline">
                          <span>{valueText(db1Value)}</span>
                          <em>--&gt;</em>
                          <span>{valueText(db2Value)}</span>
                        </div>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!visibleChanges.length && <div className="muted-block compact">No matching field changes</div>}
        </section>
      </div>
      {fieldDetail && (
        <FieldDetailModal
          column={fieldDetail.column}
          db1Value={fieldDetail.db1Value}
          db2Value={fieldDetail.db2Value}
          onClose={() => setFieldDetail(null)}
        />
      )}
    </div>
  );
}

function CompleteDataView({
  table,
  db1Path,
  db2Path
}: {
  table: TableDataResult;
  db1Path: string;
  db2Path: string;
}) {
  const [filter, setFilter] = useState("");
  const [loadedDb1Rows, setLoadedDb1Rows] = useState<Record<string, unknown>[]>([]);
  const [loadedDb2Rows, setLoadedDb2Rows] = useState<Record<string, unknown>[]>([]);
  const [loadStatus, setLoadStatus] = useState("Loading complete table data");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(100);
  const [db1Total, setDb1Total] = useState<number | null>(null);
  const [db2Total, setDb2Total] = useState<number | null>(null);

  useEffect(() => {
    setPage(0);
  }, [table.table, db1Path, db2Path]);

  useEffect(() => {
    let cancelled = false;
    setLoadedDb1Rows([]);
    setLoadedDb2Rows([]);
    setDb1Total(null);
    setDb2Total(null);

    async function loadCompleteRows() {
      try {
        setLoadStatus(`Loading page ${page + 1}`);
        const offset = page * pageSize;
        const [db1Info, db2Info, db1Data, db2Data] = await Promise.all([
          tableInfo(db1Path, table.table),
          tableInfo(db2Path, table.table),
          tableRows(db1Path, table.table, pageSize, offset),
          tableRows(db2Path, table.table, pageSize, offset)
        ]);
        if (!cancelled) {
          setLoadedDb1Rows(addSyntheticPk(db1Data.rows));
          setLoadedDb2Rows(addSyntheticPk(db2Data.rows));
          setDb1Total(db1Info.row_count);
          setDb2Total(db2Info.row_count);
          setLoadStatus(`Page ${page + 1}: ${db1Data.rows.length} DB1 row(s), ${db2Data.rows.length} DB2 row(s)`);
        }
      } catch (error) {
        if (!cancelled) {
          setLoadStatus(error instanceof Error ? error.message : "Unable to load complete table data");
        }
      }
    }

    loadCompleteRows();
    return () => {
      cancelled = true;
    };
  }, [db1Path, db2Path, table.table, page, pageSize]);

  const db1Rows = loadedDb1Rows;
  const db2Rows = loadedDb2Rows;

  const modifiedKeys = new Set(table.modified_rows.map((row) => pkObjectKey(row.pk)));
  const db1OnlyKeys = new Set(table.rows_only_in_db1.map(rowKey));
  const db2OnlyKeys = new Set(table.rows_only_in_db2.map(rowKey));

  const db1Columns = columnsForRows(db1Rows);
  const db2Columns = columnsForRows(db2Rows);
  const visibleDb1Rows = filterRows(db1Rows, filter);
  const visibleDb2Rows = filterRows(db2Rows, filter);

  if (!db1Rows.length && !db2Rows.length) {
    return <NoData message={loadStatus || "No complete table data available"} />;
  }

  return (
    <div className="complete-data-workspace">
      <div className="diff-summary-strip">
        <span>DB1: {db1Total ?? db1Rows.length} row(s)</span>
        <span>DB2: {db2Total ?? db2Rows.length} row(s)</span>
        <span>{tableCount(table, "modified")} modified</span>
        <span>{tableCount(table, "db1")} DB1 only</span>
        <span>{tableCount(table, "db2")} DB2 only</span>
        <span>{loadStatus}</span>
      </div>

      <label className="diff-filter">
        <Search size={16} />
        <input
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Search complete table data"
        />
      </label>

      <div className="pagination-bar">
        <label>
          Page size
          <select
            value={pageSize}
            onChange={(event) => {
              setPageSize(Number(event.target.value));
              setPage(0);
            }}
          >
            {[50, 100, 250, 500].map((size) => <option key={size} value={size}>{size}</option>)}
          </select>
        </label>
        <button onClick={() => setPage((value) => Math.max(0, value - 1))} disabled={page === 0}>Previous</button>
        <span>Page {page + 1}</span>
        <button
          onClick={() => setPage((value) => value + 1)}
          disabled={
            (db1Total !== null && (page + 1) * pageSize >= db1Total) &&
            (db2Total !== null && (page + 1) * pageSize >= db2Total)
          }
        >
          Next
        </button>
      </div>

      <div className="complete-data-split">
        <CompleteDataTable
          title="Database 1"
          rows={visibleDb1Rows}
          columns={db1Columns}
          modifiedKeys={modifiedKeys}
          onlyKeys={db1OnlyKeys}
          onlyLabel="DB1 only"
        />
        <CompleteDataTable
          title="Database 2"
          rows={visibleDb2Rows}
          columns={db2Columns}
          modifiedKeys={modifiedKeys}
          onlyKeys={db2OnlyKeys}
          onlyLabel="DB2 only"
        />
      </div>
    </div>
  );
}

function rowKey(row: Record<string, unknown>) {
  return JSON.stringify(row._pk ?? "");
}

function addSyntheticPk(rows: Record<string, unknown>[]) {
  return rows.map((row, index) => {
    if ("_pk" in row) return row;
    const likelyPk = Object.entries(row).find(([key]) => key.toLowerCase() === "id" || key.toLowerCase().endsWith("_id"));
    return { _pk: likelyPk ? likelyPk[1] : index + 1, ...row };
  });
}

function pkObjectKey(pk: Record<string, unknown>) {
  const values = Object.values(pk);
  return JSON.stringify(values.length === 1 ? values[0] : values);
}

function columnsForRows(rows: Record<string, unknown>[]) {
  return Array.from(new Set(rows.flatMap((row) => Object.keys(row)))).filter((column) => column !== "_pk");
}

function filterRows(rows: Record<string, unknown>[], filter: string) {
  if (!filter.trim()) return rows;
  const needle = filter.toLowerCase();
  return rows.filter((row) =>
    Object.entries(row).some(([key, value]) => `${key} ${valueText(value)}`.toLowerCase().includes(needle))
  );
}

function CompleteDataTable({
  title,
  rows,
  columns,
  modifiedKeys,
  onlyKeys,
  onlyLabel
}: {
  title: string;
  rows: Record<string, unknown>[];
  columns: string[];
  modifiedKeys: Set<string>;
  onlyKeys: Set<string>;
  onlyLabel: string;
}) {
  return (
    <section className="complete-table-panel">
      <div className="selected-row-title">
        <span>{title}</span>
        <strong>{rows.length} row(s)</strong>
      </div>
      <div className="field-diff-table-wrap">
        {!rows.length ? (
          <NoData />
        ) : (
          <table className="detail-grid complete-row-grid">
            <thead>
              <tr>
                <th>Status</th>
                <th>PK</th>
                {columns.map((column) => <th key={column}>{column}</th>)}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => {
                const key = rowKey(row);
                const isOnly = onlyKeys.has(key);
                const isModified = modifiedKeys.has(key);
                return (
                  <tr key={`${key}-${index}`} className={isOnly ? "only-row" : isModified ? "modified-row" : ""}>
                    <td>
                      {isOnly ? <span className="row-status only">{onlyLabel}</span> :
                        isModified ? <span className="row-status modified">Modified</span> :
                        <span className="row-status clean">Match</span>}
                    </td>
                    <td>{valueText(row._pk)}</td>
                    {columns.map((column) => (
                      <td key={column}>
                        <div className="long-value">{formattedValueText(row[column])}</div>
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

function FieldDetailModal({
  column,
  db1Value,
  db2Value,
  onClose
}: {
  column: string;
  db1Value: unknown;
  db2Value: unknown;
  onClose: () => void;
}) {
  const jsonDiff = jsonDiffForValues(db1Value, db2Value);
  return (
    <div className="field-detail-backdrop" onClick={onClose}>
      <section className={`field-detail-modal ${jsonDiff ? "has-json-diff" : ""}`} onClick={(event) => event.stopPropagation()}>
        <header>
          <div>
            <span className="eyebrow">Changed field</span>
            <h3>{column}</h3>
          </div>
          <button onClick={onClose}>Close</button>
        </header>
        <div className="field-detail-summary">
          <strong>Full change</strong>
          <span>{valueText(db1Value)}</span>
          <em>--&gt;</em>
          <span>{valueText(db2Value)}</span>
        </div>
        {jsonDiff && (
          <section className="json-diff-section">
            <div className="json-diff-title">
              <strong>JSON differences</strong>
              <span>{jsonDiff.length} path(s)</span>
            </div>
            {jsonDiff.length ? (
              <div className="json-diff-table-wrap">
                <table className="json-diff-table">
                  <thead>
                    <tr>
                      <th>Path</th>
                      <th>Type</th>
                      <th>Database 1</th>
                      <th>Database 2</th>
                    </tr>
                  </thead>
                  <tbody>
                    {jsonDiff.map((row) => (
                      <tr key={`${row.path}-${row.status}`}>
                        <td>{row.path}</td>
                        <td><span className={`json-status ${row.status}`}>{row.status}</span></td>
                        <td>{formattedValueText(row.db1Value)}</td>
                        <td>{formattedValueText(row.db2Value)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="muted-block compact">JSON values match</div>
            )}
          </section>
        )}
        <div className="field-detail-grid">
          <section>
            <h4>Database 1</h4>
            <pre>{formattedValueText(db1Value)}</pre>
          </section>
          <section>
            <h4>Database 2</h4>
            <pre>{formattedValueText(db2Value)}</pre>
          </section>
        </div>
      </section>
    </div>
  );
}

function ObjectGrid({ rows }: { rows: Record<string, unknown>[] }) {
  const columns = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
  if (!rows.length) return <NoData />;

  return (
    <table className="detail-grid">
      <thead>
        <tr>
          {columns.map((column) => <th key={column}>{column}</th>)}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => (
          <tr key={index}>
            {columns.map((column) => <td key={column}>{valueText(row[column])}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function SchemaDetail({ table }: { table: TableDataResult }) {
  const diff = table.column_schema_diff;
  if (!diff) return <NoData message="No schema differences available" />;
  return (
    <div className="schema-detail">
      <section>
        <h3>Columns Removed In Database 2 ({diff.only_in_db1.length})</h3>
        {diff.only_in_db1.map((column) => (
          <div key={column} className="schema-change-chip removed">
            <strong>Column removed</strong>
            <span>{column}</span>
          </div>
        ))}
        {!diff.only_in_db1.length && <em>None</em>}
      </section>
      <section>
        <h3>Columns Added In Database 2 ({diff.only_in_db2.length})</h3>
        {diff.only_in_db2.map((column) => (
          <div key={column} className="schema-change-chip added">
            <strong>Column added</strong>
            <span>{column}</span>
          </div>
        ))}
        {!diff.only_in_db2.length && <em>None</em>}
      </section>
    </div>
  );
}

function NoData({ message = "No data available" }: { message?: string }) {
  return (
    <div className="no-data-state">
      <Database size={34} />
      <span>{message}</span>
    </div>
  );
}

function SqlExplorer({ dbPath, setDbPath }: { dbPath: string; setDbPath: (path: string) => void }) {
  const browsePageSize = 1000;
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState("");
  const [sql, setSql] = useState("");
  const [result, setResult] = useState<SqlQueryResult | null>(null);
  const [tableColumns, setTableColumns] = useState<TableColumn[]>([]);
  const [foreignKeys, setForeignKeys] = useState<{ table: string; from: string; to: string }[]>([]);
  const [status, setStatus] = useState("Load a database to browse tables and run SQL.");
  const [dbVersion, setDbVersion] = useState<number | null>(null);
  const [checkStatus, setCheckStatus] = useState("");
  const [checkDetails, setCheckDetails] = useState<Record<string, unknown>[]>([]);
  const [isDbLoaded, setIsDbLoaded] = useState(false);
  const [isLoadingDb, setIsLoadingDb] = useState(false);
  const [tableFilter, setTableFilter] = useState("");
  const [explorerTab, setExplorerTab] = useState<"query" | "data" | "diagram" | "export" | "health">("data");
  const [schema, setSchema] = useState<DatabaseSchemaResult | null>(null);
  const [allowWrite, setAllowWrite] = useState(false);
  const [queryLimit, setQueryLimit] = useState(1000);
  const [exportStatus, setExportStatus] = useState("");
  const [versionStatus, setVersionStatus] = useState("");
  const [editorCursor, setEditorCursor] = useState(0);
  const [activeSuggestionIndex, setActiveSuggestionIndex] = useState(0);
  const [queryError, setQueryError] = useState<{ message: string; token: string } | null>(null);
  const [uiDensity, setUiDensity] = useState<"comfortable" | "compact">(() =>
    (localStorage.getItem("dbcompare.sqlDensity") as "comfortable" | "compact" | null) ?? "comfortable"
  );
  const [accent, setAccent] = useState<"blue" | "green" | "slate">(() =>
    (localStorage.getItem("dbcompare.sqlAccent") as "blue" | "green" | "slate" | null) ?? "blue"
  );
  const [navigationTrail, setNavigationTrail] = useState<NavigationItem[]>([]);
  const [isLoadingMoreRows, setIsLoadingMoreRows] = useState(false);
  const [queryHistory, setQueryHistory] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem("dbcompare.queryHistory") ?? "[]");
    } catch {
      return [];
    }
  });
  const [selectedRow, setSelectedRow] = useState<SelectedRowState | null>(null);
  const [pendingEdits, setPendingEdits] = useState<BatchRowEdit[]>([]);
  const [editStatus, setEditStatus] = useState("");
  const [isSavingEdits, setIsSavingEdits] = useState(false);
  const sqlEditorRef = useRef<HTMLTextAreaElement | null>(null);
  const dataRequestIdRef = useRef(0);
  const fkByColumn = new Map(foreignKeys.map((fk) => [fk.from, fk]));
  const fkColumns = new Set(fkByColumn.keys());
  const visibleTables = tables.filter((table) => table.toLowerCase().includes(tableFilter.toLowerCase()));
  const schemaRelations = schema?.tables.flatMap((table) =>
    table.foreign_keys.map((fk) => ({ fromTable: table.name, from: fk.from, toTable: fk.table, to: fk.to }))
  ) ?? [];
  const allColumnNames = useMemo(() => {
    const names = new Set<string>();
    schema?.tables.forEach((table) => table.columns.forEach((column) => names.add(column.name)));
    return Array.from(names).sort((left, right) => left.localeCompare(right));
  }, [schema]);
  const autocompleteToken = useMemo(() => {
    const beforeCursor = sql.slice(0, editorCursor);
    return beforeCursor.match(/[A-Za-z0-9_."']+$/)?.[0] ?? "";
  }, [editorCursor, sql]);
  const querySuggestions = useMemo(() => {
    const normalizedToken = autocompleteToken.replace(/^["']/, "").toLowerCase();
    if (!normalizedToken) return [];

    const candidates = [
      ...SQL_KEYWORDS.map((value) => ({ value, type: "Keyword" })),
      ...tables.map((value) => ({ value, type: "Table" })),
      ...allColumnNames.map((value) => ({ value, type: "Column" }))
    ];
    const seen = new Set<string>();

    return candidates
      .filter((candidate) => candidate.value.toLowerCase().startsWith(normalizedToken))
      .filter((candidate) => {
        const key = `${candidate.type}:${candidate.value}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .slice(0, 10);
  }, [allColumnNames, autocompleteToken, tables]);
  const highlightedSql = useMemo(
    () => highlightedSqlHtml(sql, tables, allColumnNames, queryError?.token ?? ""),
    [allColumnNames, queryError?.token, sql, tables]
  );

  useEffect(() => {
    if (dbPath) {
      loadTables();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbPath]);

  useEffect(() => {
    localStorage.setItem("dbcompare.sqlDensity", uiDensity);
  }, [uiDensity]);

  useEffect(() => {
    localStorage.setItem("dbcompare.sqlAccent", accent);
  }, [accent]);

  useEffect(() => {
    setActiveSuggestionIndex(0);
  }, [autocompleteToken]);


  function saveQueryHistory(nextHistory: string[]) {
    setQueryHistory(nextHistory);
    localStorage.setItem("dbcompare.queryHistory", JSON.stringify(nextHistory));
  }

  function rememberQuery(query: string) {
    const clean = query.trim();
    if (!clean) return;
    saveQueryHistory([clean, ...queryHistory.filter((item) => item !== clean)].slice(0, 20));
  }

  async function loadDatabaseVersion() {
    try {
      const version = await databaseVersion(dbPath);
      setDbVersion(version.user_version);
      return version.user_version;
    } catch {
      try {
        const fallback = await executeSql(dbPath, "PRAGMA user_version;", false, 1);
        const value = fallback.rows[0]?.user_version ?? Object.values(fallback.rows[0] ?? {})[0];
        const parsed = Number(value);
        setDbVersion(Number.isFinite(parsed) ? parsed : null);
        return Number.isFinite(parsed) ? parsed : null;
      } catch {
        setDbVersion(null);
        return null;
      }
    }
  }

  async function loadTables() {
    if (!dbPath) {
      setStatus("Select a database first.");
      return;
    }
    setIsLoadingDb(true);
    setStatus("Loading tables");
    try {
      const data = await listTables(dbPath);
      setTables(data.tables);
      setIsDbLoaded(true);
      setCheckStatus("");
      setCheckDetails([]);
      setStatus(`${data.tables.length} table(s) loaded.`);
      databaseSchema(dbPath)
        .then(setSchema)
        .catch(() => setSchema(null));

      loadDatabaseVersion()
        .then((version) => {
          setStatus(`${data.tables.length} table(s) loaded.${version === null ? "" : ` DB Version: ${version}.`}`);
        })
        .catch(() => undefined);

    } catch (error) {
      setIsDbLoaded(false);
      setTables([]);
      setStatus(error instanceof Error ? error.message : "Unable to load database.");
    } finally {
      setIsLoadingDb(false);
    }
  }

  function resetExplorer() {
    setDbPath("");
    setTables([]);
    setSelectedTable("");
    setSql("");
    setResult(null);
    setTableColumns([]);
    setForeignKeys([]);
    setStatus("Load a database to browse tables and run SQL.");
    setDbVersion(null);
    setCheckStatus("");
    setCheckDetails([]);
    setIsDbLoaded(false);
    setIsLoadingDb(false);
    setTableFilter("");
    setExplorerTab("data");
    setSchema(null);
    setExportStatus("");
    setNavigationTrail([]);
    setSelectedRow(null);
    setIsLoadingMoreRows(false);
  }

  async function runDatabaseCheck(kind: "integrity" | "foreign") {
    if (!dbPath) {
      setCheckStatus("Select a database first.");
      return;
    }
    setCheckStatus(kind === "integrity" ? "Running integrity check" : "Running foreign-key check");
    setCheckDetails([]);
    try {
      const checks = await databaseChecks(dbPath);
      if (kind === "integrity") {
        const ok = checks.integrity.length === 1 && String(Object.values(checks.integrity[0])[0]).toLowerCase() === "ok";
        setCheckStatus(ok ? "Integrity check: no issues found." : `Integrity check found ${checks.integrity.length} issue(s).`);
        setCheckDetails(ok ? [] : checks.integrity);
      } else {
        setCheckStatus(checks.foreign_keys.length ? `Foreign-key check found ${checks.foreign_keys.length} issue(s).` : "Foreign-key check: no issues found.");
        setCheckDetails(checks.foreign_keys);
      }
      setDbVersion(checks.user_version);
    } catch (error) {
      try {
        const fallback = await executeSql(
          dbPath,
          kind === "integrity" ? "PRAGMA integrity_check;" : "PRAGMA foreign_key_check;",
          false,
          5000
        );
        if (kind === "integrity") {
          const ok = fallback.rows.length === 1 && String(Object.values(fallback.rows[0] ?? {})[0]).toLowerCase() === "ok";
          setCheckStatus(ok ? "Integrity check: no issues found." : `Integrity check found ${fallback.rows.length} issue(s).`);
          setCheckDetails(ok ? [] : fallback.rows);
        } else {
          setCheckStatus(fallback.rows.length ? `Foreign-key check found ${fallback.rows.length} issue(s).` : "Foreign-key check: no issues found.");
          setCheckDetails(fallback.rows);
        }
      } catch {
        const label = kind === "integrity" ? "Integrity check" : "Foreign-key check";
        setCheckStatus(`${label} could not run.`);
        setCheckDetails([]);
      }
    }
  }

  async function inspectTable(table: string, keepTrail = false) {
    const requestId = ++dataRequestIdRef.current;
    setSelectedTable(table);
    setExplorerTab("data");
    if (!keepTrail) {
      setNavigationTrail([{ label: table, table }]);
    }
    setStatus(`Opening ${table}`);
    try {
      const [info, rows] = await Promise.all([tableInfo(dbPath, table), tableRows(dbPath, table, browsePageSize, 0)]);
      if (requestId !== dataRequestIdRef.current) return;
      setForeignKeys(info.foreign_keys);
      setTableColumns(info.columns);
      setResult({ columns: rows.columns, rows: rows.rows, row_count: rows.row_count ?? info.row_count });
      setStatus(`${table}: ${info.row_count} row(s), ${info.columns.length} column(s). Loaded ${rows.rows.length}.`);
    } catch (error) {
      if (requestId !== dataRequestIdRef.current) return;
      setStatus(error instanceof Error ? error.message : "Unable to inspect table.");
    }
  }

  function editKey(edit: Pick<BatchRowEdit, "table" | "key">) {
    return `${edit.table}:${JSON.stringify(edit.key, Object.keys(edit.key).sort())}`;
  }

  function selectTableFromSidebar(table: string) {
    if (explorerTab === "export") {
      setSelectedTable(table);
      setNavigationTrail([{ label: table, table }]);
      setStatus(`Selected ${table} for export.`);
      return;
    }

    inspectTable(table);
  }

  async function loadMoreTableRows() {
    if (!dbPath || !selectedTable || explorerTab !== "data" || !result || isLoadingMoreRows) return;
    if (result.rows.length >= result.row_count) return;
    const table = selectedTable;
    const offset = result.rows.length;
    setIsLoadingMoreRows(true);
    try {
      const data = await tableRows(dbPath, table, browsePageSize, offset);
      setResult((current) => {
        if (!current || selectedTable !== table) return current;
        return {
          columns: data.columns.length ? data.columns : current.columns,
          rows: [...current.rows, ...data.rows],
          row_count: data.row_count ?? current.row_count
        };
      });
      setStatus(`${table}: loaded ${Math.min(offset + data.rows.length, data.row_count)} of ${data.row_count} row(s).`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to load more rows.");
    } finally {
      setIsLoadingMoreRows(false);
    }
  }

  async function runSql() {
    if (!dbPath) {
      setStatus("Select a database first.");
      return;
    }
    if (!sql.trim()) {
      setStatus("Enter a SQL query first.");
      return;
    }
    setQueryError(null);
    setStatus("Running query");
    try {
      const data = await executeSql(dbPath, sql, allowWrite, queryLimit);
      setResult(data);
      rememberQuery(sql);
      setStatus(`${data.row_count} row(s) returned${data.elapsed_ms ? ` in ${data.elapsed_ms} ms` : ""}.`);
      if (allowWrite) {
        databaseSchema(dbPath).then(setSchema).catch(() => undefined);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Query failed.";
      setQueryError({ message, token: sqlErrorToken(message) });
      setStatus(message);
    }
  }

  async function saveDatabaseVersion(nextVersion: number) {
    if (!dbPath || !Number.isInteger(nextVersion) || nextVersion < 0) {
      setVersionStatus("Enter a valid non-negative DB version.");
      return;
    }
    setVersionStatus("Saving DB version");
    try {
      await executeSql(dbPath, `PRAGMA user_version = ${nextVersion}`, true, 1);
      const version = await databaseVersion(dbPath);
      setDbVersion(version.user_version);
      setVersionStatus("DB version saved.");
    } catch (error) {
      setVersionStatus(error instanceof Error ? error.message : "Unable to save DB version.");
    }
  }


  async function runExport(source: ExportSource, format: ExportFormat) {
    if (!dbPath) {
      setExportStatus("Select a database first.");
      return;
    }
    if (source === "table" && !selectedTable) {
      setExportStatus("Select a table first.");
      return;
    }
    setExportStatus("Preparing export");
    try {
      const response = await exportSqlData({
        dbPath,
        source,
        format,
        table: selectedTable,
        sql,
        limit: queryLimit
      });
      const saved = await window.dbcompare?.saveGeneratedFile(response.path, fileNameFromPath(response.path));
      setExportStatus(saved?.path ? `Export saved to ${saved.path}` : `Export ready: ${response.path}`);
    } catch (error) {
      setExportStatus(error instanceof Error ? error.message : "Export failed.");
    }
  }

  async function navigateForeignKey(fk: { table: string; from: string; to: string }, value: unknown) {
    if (value === null || value === undefined || value === "") return;
    const requestId = ++dataRequestIdRef.current;
    const sourceTable = selectedTable;
    setExplorerTab("data");
    setResult(null);
    setStatus(`Opening ${fk.table} where ${fk.to} = ${valueText(value)}`);
    try {
      const [info, data] = await Promise.all([
        tableInfo(dbPath, fk.table),
        relatedRows(dbPath, fk.table, fk.to, value, 250)
      ]);
      if (requestId !== dataRequestIdRef.current) return;
      setSelectedTable(fk.table);
      setTableColumns(info.columns);
      setSql(`SELECT * FROM "${fk.table}" WHERE "${fk.to}" = ${typeof value === "number" ? value : `'${String(value).replace(/'/g, "''")}'`} LIMIT 250;`);
      setNavigationTrail((trail) => {
        const baseTrail = trail.length ? trail : sourceTable ? [{ label: sourceTable, table: sourceTable }] : [];
        return [
          ...baseTrail,
          {
            label: `${fk.table} (${fk.to}=${valueText(value)})`,
            table: fk.table,
            related: { column: fk.to, value }
          }
        ];
      });
      setForeignKeys(info.foreign_keys);
      setResult({ columns: data.columns, rows: data.rows, row_count: data.rows.length });
      setStatus(`${data.rows.length} related row(s) found in ${fk.table} for ${fk.to} = ${valueText(value)}.`);
    } catch (error) {
      if (requestId !== dataRequestIdRef.current) return;
      setResult({ columns: [], rows: [], row_count: 0 });
      setStatus(error instanceof Error ? error.message : "Unable to open related row.");
    }
  }

  function insertSnippet(snippet: string) {
    setSql((current) => {
      if (snippet === "SELECT *" && selectedTable) return `SELECT * FROM ${selectedTable};`;
      if (snippet === "COUNT(*)" && selectedTable) return `SELECT COUNT(*) AS total FROM ${selectedTable};`;
      if (snippet === "WHERE") return `${current.trim().replace(/;$/, "")}\nWHERE `;
      if (snippet === "JOIN") return `${current.trim().replace(/;$/, "")}\nJOIN  ON `;
      if (snippet === "GROUP BY") return `${current.trim().replace(/;$/, "")}\nGROUP BY `;
      if (snippet === "ORDER BY") return `${current.trim().replace(/;$/, "")}\nORDER BY `;
      if (snippet === "INSERT") return selectedTable ? `INSERT INTO ${selectedTable} () VALUES ();` : "INSERT INTO  () VALUES ();";
      if (snippet === "UPDATE") return selectedTable ? `UPDATE ${selectedTable} SET  WHERE ;` : "UPDATE  SET  WHERE ;";
      if (snippet === "DELETE") return selectedTable ? `DELETE FROM ${selectedTable} WHERE ;` : "DELETE FROM  WHERE ;";
      return `${current}${current.endsWith(" ") || !current ? "" : " "}${snippet} `;
    });
  }

  function prettifyQuery() {
    const formatted = formatSqlText(sql);
    setSql(formatted);
    setEditorCursor(formatted.length);
    requestAnimationFrame(() => {
      sqlEditorRef.current?.focus();
      sqlEditorRef.current?.setSelectionRange(formatted.length, formatted.length);
    });
  }


  function updateEditorCursor(target: HTMLTextAreaElement) {
    setEditorCursor(target.selectionStart ?? 0);
  }

  function applyQuerySuggestion(value: string) {
    const editor = sqlEditorRef.current;
    const cursor = editor?.selectionStart ?? editorCursor;
    const beforeCursor = sql.slice(0, cursor);
    const afterCursor = sql.slice(cursor);
    const tokenMatch = beforeCursor.match(/[A-Za-z0-9_."']+$/);
    const tokenStart = tokenMatch ? cursor - tokenMatch[0].length : cursor;
    const nextValue = value;
    const nextSql = `${sql.slice(0, tokenStart)}${nextValue}${afterCursor}`;
    const nextCursor = tokenStart + nextValue.length;

    setSql(nextSql);
    setEditorCursor(nextCursor);
    setActiveSuggestionIndex(0);
    requestAnimationFrame(() => {
      sqlEditorRef.current?.focus();
      sqlEditorRef.current?.setSelectionRange(nextCursor, nextCursor);
    });
  }

  function handleSqlKeyDown(event: ReactKeyboardEvent<HTMLTextAreaElement>) {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      runSql();
      return;
    }

    if (!querySuggestions.length) return;

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveSuggestionIndex((index) => (index + 1) % querySuggestions.length);
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveSuggestionIndex((index) => (index - 1 + querySuggestions.length) % querySuggestions.length);
      return;
    }

    if (event.key === "Tab" || event.key === "Enter") {
      event.preventDefault();
      applyQuerySuggestion(querySuggestions[activeSuggestionIndex]?.value ?? querySuggestions[0].value);
      return;
    }

    if (event.key === "Escape") {
      setEditorCursor(0);
    }
  }

  async function openTrailItem(item: NavigationItem, index: number) {
    const requestId = ++dataRequestIdRef.current;
    setExplorerTab("data");
    setResult(null);
    setStatus(`Opening ${item.label}`);
    try {
      const [info, data] = await Promise.all([
        tableInfo(dbPath, item.table),
        item.related
          ? relatedRows(dbPath, item.table, item.related.column, item.related.value, 250)
          : item.query
            ? executeSql(dbPath, item.query, false, 250)
            : tableRows(dbPath, item.table, browsePageSize, 0)
      ]);
      if (requestId !== dataRequestIdRef.current) return;
      const nextRowCount = "row_count" in data ? data.row_count : data.rows.length;
      setSelectedTable(item.table);
      setForeignKeys(info.foreign_keys);
      setTableColumns(info.columns);
      setResult({ columns: data.columns, rows: data.rows, row_count: nextRowCount });
      setSql(
        item.query ??
          (item.related
            ? `SELECT * FROM "${item.table}" WHERE "${item.related.column}" = ${typeof item.related.value === "number" ? item.related.value : `'${String(item.related.value).replace(/'/g, "''")}'`} LIMIT 250;`
            : `SELECT * FROM "${item.table}" LIMIT 250;`)
      );
      setNavigationTrail((trail) => trail.slice(0, index + 1));
      setStatus(`${item.label}: ${data.rows.length} row(s).`);
    } catch (error) {
      if (requestId !== dataRequestIdRef.current) return;
      setResult({ columns: [], rows: [], row_count: 0 });
      setStatus(error instanceof Error ? error.message : "Unable to open breadcrumb item.");
    }
  }

  async function stageRowEdit(
    key: Record<string, unknown>,
    values: Record<string, unknown>
  ): Promise<void> {
    if (!selectedTable) {
      throw new Error("Select a database table before editing.");
    }

    const nextRow = { ...(selectedRow?.row ?? {}), ...values };
    const nextEdit: BatchRowEdit = { table: selectedTable, key, values };
    const nextEditKey = editKey(nextEdit);
    setPendingEdits((current) => {
      const existing = current.find((edit) => editKey(edit) === nextEditKey);
      if (!existing) return [...current, nextEdit];
      return current.map((edit) =>
        editKey(edit) === nextEditKey
          ? { ...edit, values: { ...edit.values, ...values } }
          : edit
      );
    });
    setResult((current) => {
      if (!current) return current;
      return {
        ...current,
        rows: current.rows.map((row) => (rowMatchesKey(row, key) ? nextRow : row))
      };
    });
    setSelectedRow({
      row: nextRow,
      key: makeEditableRowKey(nextRow, tableColumns, result?.columns ?? Object.keys(nextRow))
    });
    setEditStatus("Change staged. Save and download the edited DB when finished.");
  }

  async function saveAndDownloadEditedDb() {
    if (!dbPath || !pendingEdits.length || isSavingEdits) return;
    setIsSavingEdits(true);
    setEditStatus("Creating edited database");
    try {
      const response = await updateRowsBatch(dbPath, pendingEdits);
      const saved = await window.dbcompare?.saveGeneratedFile(response.path, fileNameFromPath(response.path));
      setEditStatus(saved?.path ? `Edited DB saved to ${saved.path}` : `Edited DB ready: ${response.path}`);
      if (saved?.saved) {
        setPendingEdits([]);
      }
    } catch (error) {
      setEditStatus(error instanceof Error ? error.message : "Unable to save edited database.");
    } finally {
      setIsSavingEdits(false);
    }
  }

  function clearPendingEdits() {
    setPendingEdits([]);
    setEditStatus("Pending edits cleared. Reload the table to discard previewed cell changes.");
  }

  return (
    <>
      <section className={`sql-workbench ${uiDensity} accent-${accent}`}>
        <header className="sql-topbar">
          <div className="sql-tabs">
            {[
              ["data", "Data", Table2],
              ["diagram", "DB Diagram", Network],
              ["query", "Run Query", Code2],
              ["health", "Health", CheckCircle2],
              ["export", "Export", FileDown]
            ].map(([tab, label, Icon]) => {
              const TabIcon = Icon as typeof Code2;
              return (
                <button key={String(tab)} className={explorerTab === tab ? "active" : ""} onClick={() => setExplorerTab(tab as typeof explorerTab)}>
                  <TabIcon size={15} /> {String(label)}
                </button>
              );
            })}
          </div>
          <div className="sql-top-actions">
            <button title="Reset" onClick={resetExplorer} disabled={isLoadingDb}>
              <RotateCcw size={16} />
            </button>
          </div>
        </header>
        <div className="sql-source-row">
          <PathPicker label="Database" value={dbPath} onChange={setDbPath} />
        </div>
      </section>
      {!isDbLoaded ? (
        <section className="sql-empty-start">
          <div className="hero-summary">
            <div>
              <h1>{isLoadingDb ? "Loading database" : "Open Database"}</h1>
              <p>Select a database file above. The table browser will appear after the database is loaded.</p>
            </div>
            <div className="summary-pills">
              <span>{status}</span>
            </div>
          </div>
        </section>
      ) : (
      <section className={`sql-grid sql-workbench-body ${uiDensity} accent-${accent} ${explorerTab === "diagram" ? "diagram-mode" : ""} ${explorerTab === "query" ? "query-mode" : ""} ${explorerTab === "health" ? "health-mode" : ""}`}>
        {explorerTab !== "diagram" && explorerTab !== "query" && explorerTab !== "health" && (
          <section className="panel table-browser">
            <div className="panel-title">
              <Table2 size={18} />
              <strong>Objects</strong>
              <span>{tables.length}</span>
            </div>
            <div className="table-search">
              <Search size={16} />
              <input value={tableFilter} onChange={(event) => setTableFilter(event.target.value)} placeholder="Search tables or columns" />
            </div>
            <div className="table-list">
              {visibleTables.map((table) => (
                <button key={table} className={table === selectedTable ? "selected" : ""} onClick={() => selectTableFromSidebar(table)} title={table}>
                  <Table2 size={17} />
                  <span>{table}</span>
                  <small>{schema?.tables.find((item) => item.name === table)?.row_count ?? ""}</small>
                </button>
              ))}
            </div>
          </section>
        )}
        <section className="panel sql-panel">
          {explorerTab === "query" ? (
            <>
              <div className="editor-bar">
                <div>
                  <strong>SQL Query Editor</strong>
                  <span>{selectedTable ? `Active table: ${selectedTable}` : "Write SQL against the selected database"} · Ctrl/Cmd + Enter to run</span>
                </div>
                <div className="editor-actions">
                  <button onClick={prettifyQuery}>Prettify</button>
                  <label className="sql-toggle">
                    <input type="checkbox" checked={allowWrite} onChange={(event) => setAllowWrite(event.target.checked)} />
                    Write
                  </label>
                  <label className="sql-limit">
                    Limit
                    <input type="number" min={1} max={10000} value={queryLimit} onChange={(event) => setQueryLimit(Number(event.target.value) || 1000)} />
                  </label>
                  <button className="primary" onClick={runSql}>
                    <Play size={15} /> Run Query
                  </button>
                  <button className="ghost-button" onClick={() => setSql("")}>
                    <RefreshCcw size={16} /> Clear
                  </button>
                </div>
              </div>
              <div className="sql-editor-wrap">
                <pre className="sql-highlight" aria-hidden="true" dangerouslySetInnerHTML={{ __html: highlightedSql }} />
                <textarea
                  ref={sqlEditorRef}
                  className="sql-editor"
                  value={sql}
                  onChange={(event) => {
                    setSql(event.target.value);
                    updateEditorCursor(event.target);
                  }}
                  onClick={(event) => updateEditorCursor(event.currentTarget)}
                  onKeyUp={(event) => updateEditorCursor(event.currentTarget)}
                  onKeyDown={handleSqlKeyDown}
                  spellCheck={false}
                />
                <div className="sql-inline-snippets">
                  {SQL_SNIPPETS.map((snippet) => (
                    <button key={snippet} onClick={() => insertSnippet(snippet)}>{snippet}</button>
                  ))}
                </div>
                {querySuggestions.length > 0 && (
                  <div className="sql-suggestions">
                    {querySuggestions.map((suggestion, index) => (
                      <button
                        key={`${suggestion.type}-${suggestion.value}`}
                        className={index === activeSuggestionIndex ? "active" : ""}
                        onMouseDown={(event) => {
                          event.preventDefault();
                          applyQuerySuggestion(suggestion.value);
                        }}
                      >
                        <strong>{suggestion.value}</strong>
                        <span>{suggestion.type}</span>
                      </button>
                    ))}
                  </div>
                )}
                {queryError && (
                  <div className="sql-inline-error">
                    <AlertTriangle size={15} />
                    <span>{queryError.token ? `Near "${queryError.token}": ${queryError.message}` : queryError.message}</span>
                  </div>
                )}
              </div>
              <div className="query-history">
                <span>Recent queries</span>
                <div>
                  {queryHistory.slice(0, 5).map((query) => (
                    <button key={query} title={query} onClick={() => setSql(query)}>
                      {query}
                    </button>
                  ))}
                  {queryHistory.length > 0 && <button onClick={() => saveQueryHistory([])}>Clear history</button>}
                </div>
              </div>
            </>
          ) : (
            <>
              {explorerTab === "data" && (
                <div className="browse-heading compact">
                  <strong>{selectedTable || "Select a table"}</strong>
                </div>
              )}
              {explorerTab === "data" && pendingEdits.length > 0 && (
                <div className="pending-edit-bar">
                  <span>{pendingEdits.length} staged row edit(s)</span>
                  {editStatus && <strong>{editStatus}</strong>}
                  <button onClick={saveAndDownloadEditedDb} disabled={isSavingEdits}>
                    <Download size={15} /> {isSavingEdits ? "Saving..." : "Save & Download Edited DB"}
                  </button>
                  <button onClick={clearPendingEdits} disabled={isSavingEdits}>Clear</button>
                </div>
              )}
              {explorerTab === "diagram" && (
                <SchemaDiagram schema={schema} selectedTable={selectedTable} relations={schemaRelations} />
              )}
              {explorerTab === "export" && (
                <ExportPanel
                  selectedTable={selectedTable}
                  status={exportStatus}
                  onExport={runExport}
                />
              )}
              {explorerTab === "health" && (
                <HealthPanel
                  checkStatus={checkStatus}
                  checkDetails={checkDetails}
                  onIntegrity={() => runDatabaseCheck("integrity")}
                  onForeign={() => runDatabaseCheck("foreign")}
                  schema={schema}
                  dbVersion={dbVersion}
                  versionStatus={versionStatus}
                  onSaveVersion={saveDatabaseVersion}
                />
              )}
            </>
          )}
          {navigationTrail.length > 1 && (
            <div className="fk-trail">
              {navigationTrail.map((item, index) => (
                <span key={`${item.label}-${index}`}>
                  {index > 0 && <ArrowRight size={13} />}
                  <button onClick={() => openTrailItem(item, index)}>{item.label}</button>
                </span>
              ))}
            </div>
          )}
          {checkDetails.length > 0 && (
            <div className="check-details">
              <strong>Check details</strong>
              <div>
                {checkDetails.slice(0, 20).map((row, index) => (
                  <code key={index}>{JSON.stringify(row)}</code>
                ))}
              </div>
            </div>
          )}
          {(explorerTab === "query" || explorerTab === "data") && (
            <>
              {explorerTab === "query" && (
                <div className="sql-status">
                  <span>{status}</span>
                  {result?.truncated && <strong>Limited at {result.truncated_at} rows</strong>}
                </div>
              )}
              <ResultTable
                result={result}
                tableKey={`${explorerTab}:${selectedTable}`}
                fkColumns={fkColumns}
                fkByColumn={fkByColumn}
                onOpenRow={(row) => {
                  if (!result) return;
                  setSelectedRow({
                    row,
                    key: makeEditableRowKey(row, tableColumns, result.columns)
                  });
                }}
                onNavigateForeignKey={navigateForeignKey}
                onLoadMore={loadMoreTableRows}
                canLoadMore={explorerTab === "data" && Boolean(result && result.rows.length < result.row_count)}
                isLoadingMore={isLoadingMoreRows}
              />
            </>
          )}
          {explorerTab === "query" && (
            <div className="sql-bottom-bar">
              <span>Connected</span>
              <span>{result?.row_count ?? 0} rows</span>
              <span>{result?.elapsed_ms ?? "-"} ms</span>
            </div>
          )}
        </section>
      </section>
      )}
      {selectedRow && (
        <RowModal
          row={selectedRow.row}
          rowKey={selectedRow.key}
          table={selectedTable}
          canEdit={explorerTab === "data" && Boolean(selectedTable)}
          onSave={stageRowEdit}
          onClose={() => setSelectedRow(null)}
        />
      )}
    </>
  );
}

function SchemaDiagram({
  schema,
  selectedTable,
  relations
}: {
  schema: DatabaseSchemaResult | null;
  selectedTable: string;
  relations: { fromTable: string; from: string; toTable: string; to: string }[];
}) {
  const [focusedTable, setFocusedTable] = useState(selectedTable);
  const [zoom, setZoom] = useState(0.85);

  useEffect(() => {
    setFocusedTable("");
  }, [schema]);

  const tableByName = useMemo(() => new Map(schema?.tables.map((table) => [table.name, table]) ?? []), [schema]);

  if (!schema) return <div className="muted-block">Schema is loading</div>;

  const tables = schema.tables.filter((table) => table.type === "table");
  const tableNames = new Set(tables.map((table) => table.name));
  const validRelations = relations.filter((relation) => tableNames.has(relation.fromTable) && tableNames.has(relation.toTable));
  const activeTable = focusedTable && tableNames.has(focusedTable) ? focusedTable : "";
  const activeRelations = activeTable
    ? validRelations.filter((relation) => relation.fromTable === activeTable || relation.toTable === activeTable)
    : validRelations;
  const relationCounts = new Map<string, number>();
  const relationColors = ["#497fbd", "#0f9f8f", "#9b6bca", "#d78137", "#d05f7a", "#6875d1", "#3b8a54", "#b65f3b"];

  validRelations.forEach((relation) => {
    relationCounts.set(relation.fromTable, (relationCounts.get(relation.fromTable) ?? 0) + 1);
    relationCounts.set(relation.toTable, (relationCounts.get(relation.toTable) ?? 0) + 1);
  });

  const visibleTableNames = activeTable
    ? [activeTable, ...Array.from(new Set(activeRelations.flatMap((relation) => [relation.fromTable, relation.toTable]))).filter((name) => name !== activeTable)]
    : tables
        .slice()
        .sort((left, right) => (relationCounts.get(right.name) ?? 0) - (relationCounts.get(left.name) ?? 0) || left.name.localeCompare(right.name))
        .slice(0, 36)
        .map((table) => table.name);

  const visibleSet = new Set(visibleTableNames);
  const visibleRelations = (activeTable ? activeRelations : validRelations).filter(
    (relation) => visibleSet.has(relation.fromTable) && visibleSet.has(relation.toTable)
  );
  const pairIndexes = new Map<string, number>();
  const pairTotals = new Map<string, number>();

  visibleRelations.forEach((relation) => {
    const pairKey = [relation.fromTable, relation.toTable].sort().join("__");
    pairTotals.set(pairKey, (pairTotals.get(pairKey) ?? 0) + 1);
  });

  const incomingByTable = new Map<string, typeof visibleRelations>();
  const outgoingByTable = new Map<string, typeof visibleRelations>();

  visibleRelations.forEach((relation) => {
    incomingByTable.set(relation.toTable, [...(incomingByTable.get(relation.toTable) ?? []), relation]);
    outgoingByTable.set(relation.fromTable, [...(outgoingByTable.get(relation.fromTable) ?? []), relation]);
  });

  const CARD_WIDTH = 340;
  const HEADER_HEIGHT = 52;
  const ROW_HEIGHT = 38;
  const COLUMN_GAP = 560;
  const ROW_GAP = 120;
  const CANVAS_PADDING = 90;

  const visibleTables = visibleTableNames
    .map((name) => tableByName.get(name))
    .filter((table): table is NonNullable<ReturnType<typeof tableByName.get>> => Boolean(table));

  const getDisplayColumns = (table: (typeof visibleTables)[number]) => {
    const relationColumns = new Set(
      visibleRelations
        .filter((relation) => relation.fromTable === table.name || relation.toTable === table.name)
        .flatMap((relation) => [
          relation.fromTable === table.name ? relation.from : "",
          relation.toTable === table.name ? relation.to : ""
        ])
        .filter(Boolean)
    );
    const priorityColumns = table.columns.filter((column) => column.pk || relationColumns.has(column.name));
    const remainingColumns = table.columns.filter((column) => !priorityColumns.some((item) => item.name === column.name));
    const visibleColumnCount = Math.max(priorityColumns.length, Math.max(7, Math.min(12, table.columns.length)));
    return [...priorityColumns, ...remainingColumns].slice(0, visibleColumnCount);
  };

  const columnLists = new Map(visibleTables.map((table) => [table.name, getDisplayColumns(table)]));
  const cardHeights = new Map(
    visibleTables.map((table) => {
      const columns = columnLists.get(table.name) ?? [];
      return [table.name, HEADER_HEIGHT + columns.length * ROW_HEIGHT + (table.columns.length > columns.length ? 32 : 0)];
    })
  );

  const focused = activeTable ? visibleTables.filter((table) => table.name === activeTable) : [];
  const parentTables = activeTable
    ? visibleTables.filter((table) => visibleRelations.some((relation) => relation.fromTable === activeTable && relation.toTable === table.name))
    : [];
  const childTables = activeTable
    ? visibleTables.filter((table) => visibleRelations.some((relation) => relation.toTable === activeTable && relation.fromTable === table.name))
    : [];
  const peerTables = activeTable
    ? visibleTables.filter((table) => table.name !== activeTable && !parentTables.includes(table) && !childTables.includes(table))
    : [];
  const leftTables = activeTable
    ? parentTables
    : visibleTables.filter((table) => !(incomingByTable.get(table.name)?.length ?? 0));
  const middleTables = !activeTable
    ? visibleTables.filter((table) => (incomingByTable.get(table.name)?.length ?? 0) && (outgoingByTable.get(table.name)?.length ?? 0))
    : [];
  const rightTables = activeTable
    ? [...childTables, ...peerTables]
    : visibleTables.filter((table) => !leftTables.includes(table) && !middleTables.includes(table));
  const fallbackSplit = Math.ceil(visibleTables.length / 2);
  const layoutColumns = activeTable
    ? [leftTables, focused, rightTables].filter((column) => column.length)
    : leftTables.length || middleTables.length || rightTables.length
      ? [leftTables, middleTables, rightTables].filter((column) => column.length)
      : [visibleTables.slice(0, fallbackSplit), visibleTables.slice(fallbackSplit)].filter((column) => column.length);
  const positionedTables = new Map<string, { x: number; y: number; width: number; height: number }>();
  let canvasHeight = 0;

  layoutColumns.forEach((columnTables, columnIndex) => {
    let y = CANVAS_PADDING;
    columnTables.forEach((table) => {
      const height = cardHeights.get(table.name) ?? HEADER_HEIGHT;
      positionedTables.set(table.name, {
        x: CANVAS_PADDING + columnIndex * COLUMN_GAP,
        y,
        width: CARD_WIDTH,
        height
      });
      y += height + ROW_GAP;
    });
    canvasHeight = Math.max(canvasHeight, y + CANVAS_PADDING);
  });

  const canvasWidth = Math.max(980, CANVAS_PADDING * 2 + Math.max(1, layoutColumns.length) * CARD_WIDTH + Math.max(0, layoutColumns.length - 1) * (COLUMN_GAP - CARD_WIDTH));
  canvasHeight = Math.max(600, canvasHeight);

  const columnAnchor = (tableName: string, columnName: string, side: "left" | "right") => {
    const position = positionedTables.get(tableName);
    const columns = columnLists.get(tableName) ?? [];
    if (!position) return { x: 0, y: 0 };
      const columnIndex = Math.max(0, columns.findIndex((column) => column.name === columnName));
      return {
        x: side === "left" ? position.x : position.x + position.width,
        y: position.y + HEADER_HEIGHT + columnIndex * ROW_HEIGHT + ROW_HEIGHT / 2
      };
  };

  const selectedRelationKeys = new Set(visibleRelations.map((relation) => `${relation.fromTable}.${relation.from}->${relation.toTable}.${relation.to}`));
  const updateZoom = (nextZoom: number) => setZoom(Math.min(1.4, Math.max(0.45, Number(nextZoom.toFixed(2)))));

  return (
    <div className="schema-workspace diagram-full">
      <div className="schema-graph-shell">
        <div className="schema-graph-toolbar">
          <div>
            <strong>{activeTable ? `${activeTable} relationships` : "Database relationships"}</strong>
            <span>{visibleRelations.length ? `${visibleRelations.length} connection(s) shown` : "No foreign-key links found"}</span>
          </div>
          <label className="schema-table-select">
            <Table2 size={15} />
            <select value={activeTable} onChange={(event) => setFocusedTable(event.target.value)}>
              <option value="">All relationship tables</option>
              {tables.map((table) => (
                <option key={table.name} value={table.name}>{table.name}</option>
              ))}
            </select>
          </label>
          <div className="schema-zoom-controls">
            <button onClick={() => updateZoom(zoom - 0.1)} title="Zoom out" aria-label="Zoom out"><Minus size={16} /></button>
            <span>{Math.round(zoom * 100)}%</span>
            <button onClick={() => updateZoom(zoom + 0.1)} title="Zoom in" aria-label="Zoom in"><Plus size={16} /></button>
            <button onClick={() => updateZoom(0.85)} title="Reset zoom" aria-label="Reset zoom"><RotateCcw size={16} /></button>
            <button onClick={() => setFocusedTable("")} disabled={!activeTable} title="Show all tables" aria-label="Show all tables"><Network size={16} /></button>
          </div>
        </div>
        <div className="schema-graph-canvas" style={{ minWidth: canvasWidth * zoom, minHeight: canvasHeight * zoom }}>
          <div className="schema-graph-stage" style={{ width: canvasWidth, height: canvasHeight, transform: `scale(${zoom})` }}>
          <svg className="schema-links" width={canvasWidth} height={canvasHeight} viewBox={`0 0 ${canvasWidth} ${canvasHeight}`}>
            <defs>
              {relationColors.map((color, index) => (
                <marker key={color} id={`schema-arrow-${index}`} viewBox="0 0 14 14" refX="12" refY="7" markerWidth="12" markerHeight="12" orient="auto">
                  <path d="M2 2 L12 7 L2 12 Z" fill={color} />
                </marker>
              ))}
            </defs>
            {visibleRelations.map((relation, index) => {
              const parent = positionedTables.get(relation.toTable);
              const child = positionedTables.get(relation.fromTable);
              if (!parent || !child) return null;

              const parentIsLeft = parent.x <= child.x;
              const start = columnAnchor(relation.toTable, relation.to, parentIsLeft ? "right" : "left");
              const end = columnAnchor(relation.fromTable, relation.from, parentIsLeft ? "left" : "right");
              const pairKey = [relation.fromTable, relation.toTable].sort().join("__");
              const pairIndex = pairIndexes.get(pairKey) ?? 0;
              const pairTotal = pairTotals.get(pairKey) ?? 1;
              pairIndexes.set(pairKey, pairIndex + 1);
              const colorIndex = index % relationColors.length;
              const color = relationColors[colorIndex];
              const laneOffset = (pairIndex - (pairTotal - 1) / 2) * 46;
              const routeDirection = parentIsLeft ? 1 : -1;
              const startStub = start.x + routeDirection * (42 + Math.abs(laneOffset) * 0.12);
              const endStub = end.x - routeDirection * 42;
              const openSpaceMid = (startStub + endStub) / 2;
              const laneX = openSpaceMid + laneOffset;
              const path = `M ${start.x} ${start.y} H ${startStub} H ${laneX} V ${end.y} H ${endStub} H ${end.x}`;
              const key = `${relation.fromTable}.${relation.from}->${relation.toTable}.${relation.to}`;

              return (
                <g key={key} className={selectedRelationKeys.has(key) ? "schema-link active" : "schema-link"} style={{ "--relation-color": color } as CSSProperties}>
                  <path d={path} markerEnd={`url(#schema-arrow-${colorIndex})`} />
                  <circle cx={start.x} cy={start.y} r="5" />
                  <text x={start.x + (parentIsLeft ? 12 : -28)} y={start.y - 8}>1</text>
                  <text className="many-label" x={end.x + (parentIsLeft ? -32 : 12)} y={end.y - 10}>*</text>
                </g>
              );
            })}
          </svg>
          {visibleTables.map((table) => {
            const position = positionedTables.get(table.name);
            const displayColumns = columnLists.get(table.name) ?? [];
            const relatedColumns = new Set(
              visibleRelations
                .filter((relation) => relation.fromTable === table.name || relation.toTable === table.name)
                .flatMap((relation) => [
                  relation.fromTable === table.name ? relation.from : "",
                  relation.toTable === table.name ? relation.to : ""
                ])
                .filter(Boolean)
            );
            if (!position) return null;

            return (
              <button
                key={table.name}
                type="button"
                className={`schema-table-card ${table.name === activeTable ? "focused" : ""}`}
                style={{ left: position.x, top: position.y, width: position.width }}
                onClick={() => setFocusedTable(table.name)}
                title={`Focus ${table.name} relationships`}
              >
                <strong><Table2 size={16} /> {table.name}</strong>
                {displayColumns.map((column) => {
                  const isForeignKey = table.foreign_keys.some((fk) => fk.from === column.name);
                  const isRelated = relatedColumns.has(column.name);

                  return (
                    <span key={column.name} className={`${column.pk ? "pk" : ""} ${isForeignKey ? "fk" : ""} ${isRelated ? "connected" : ""}`}>
                      <b>{column.pk ? "PK" : isForeignKey ? <Link2 size={14} /> : ""}</b>
                      <em>{column.name}</em>
                      <small>{column.type || "TEXT"}{column.notnull ? " NN" : ""}</small>
                    </span>
                  );
                })}
                {table.columns.length > displayColumns.length && <i>+{table.columns.length - displayColumns.length} more columns</i>}
              </button>
            );
          })}
          {!validRelations.length && <div className="schema-empty-graph">No foreign-key references were found in this database.</div>}
          </div>
        </div>
      </div>
    </div>
  );
}

function ExportPanel({
  selectedTable,
  status,
  onExport
}: {
  selectedTable: string;
  status: string;
  onExport: (source: ExportSource, format: ExportFormat) => void;
}) {
  const [scope, setScope] = useState<"table" | "database">("table");
  const dataSource = scope === "table" ? "table" : "database";
  return (
    <div className="export-suite">
      <header>
        <div>
          <strong>Export</strong>
          <span>{selectedTable ? `Selected table: ${selectedTable}` : "Choose a table or export the whole database"}</span>
        </div>
        {status && <b>{status}</b>}
      </header>
      <div className="export-scope">
        <button className={scope === "table" ? "active" : ""} onClick={() => setScope("table")} disabled={!selectedTable}>
          Specific table
        </button>
        <button className={scope === "database" ? "active" : ""} onClick={() => setScope("database")}>
          Full database
        </button>
      </div>
      <div className="export-grid">
        <button onClick={() => onExport(dataSource, "csv")} disabled={scope === "table" && !selectedTable}>
          <FileDown size={20} />
          <strong>CSV With Data</strong>
          <span>{scope === "table" ? "Selected table as CSV." : "ZIP with one CSV per table."}</span>
        </button>
        <button onClick={() => onExport(dataSource, "sql")} disabled={scope === "table" && !selectedTable}>
          <Database size={20} />
          <strong>Insert SQL With Data</strong>
          <span>{scope === "table" ? "INSERT script for selected table." : "Schema plus INSERT data for all tables."}</span>
        </button>
        <button onClick={() => onExport("schema", "sql")}>
          <Code2 size={20} />
          <strong>Export Schema</strong>
          <span>Complete database schema without table data.</span>
        </button>
        <button onClick={() => onExport("database", "sqlite")}>
          <SaveIcon />
          <strong>SQLite Copy</strong>
          <span>Plain database file export.</span>
        </button>
        <button onClick={() => onExport("database", "vyp")}>
          <Database size={20} />
          <strong>VYP Copy</strong>
          <span>Extracted Vyapar database format.</span>
        </button>
        <button onClick={() => onExport("database", "vyb")}>
          <Database size={20} />
          <strong>VYB Archive</strong>
          <span>Repacked archive for sharing or restore.</span>
        </button>
      </div>
    </div>
  );
}

function SaveIcon() {
  return <Download size={20} />;
}

function HealthPanel({
  checkStatus,
  checkDetails,
  onIntegrity,
  onForeign,
  schema,
  dbVersion,
  versionStatus,
  onSaveVersion
}: {
  checkStatus: string;
  checkDetails: Record<string, unknown>[];
  onIntegrity: () => void;
  onForeign: () => void;
  schema: DatabaseSchemaResult | null;
  dbVersion: number | null;
  versionStatus: string;
  onSaveVersion: (version: number) => void;
}) {
  const [draftVersion, setDraftVersion] = useState(String(dbVersion ?? ""));
  const tableCount = schema?.tables.filter((table) => table.type === "table").length ?? 0;
  const viewCount = schema?.tables.filter((table) => table.type === "view").length ?? 0;
  const sizeText = schema ? `${Math.round((schema.page_count * schema.page_size) / 1024)} KB` : "-";

  useEffect(() => {
    setDraftVersion(String(dbVersion ?? ""));
  }, [dbVersion]);

  return (
    <div className="health-panel">
      <div className="health-metrics">
        <span><strong>{tableCount}</strong> Tables</span>
        <span><strong>{viewCount}</strong> Views</span>
        <span className="db-version-card">
          <strong>DB Version</strong>
          <label>
            <input
              type="number"
              min={0}
              value={draftVersion}
              onChange={(event) => setDraftVersion(event.target.value)}
            />
            <button onClick={() => onSaveVersion(Number(draftVersion))}>Save</button>
          </label>
          {versionStatus && <small>{versionStatus}</small>}
        </span>
        <span><strong>{sizeText}</strong> Estimated Size</span>
      </div>
      <div className="health-actions">
        <button onClick={onIntegrity}><CheckCircle2 size={16} /> Integrity Check</button>
        <button onClick={onForeign}><Network size={16} /> Foreign Key Check</button>
      </div>
      <div className="health-score">
        <strong>{checkDetails.length ? "Issues Found" : "Schema Health"}</strong>
        <span>{checkStatus || "Run checks to validate database integrity and relationships."}</span>
        <b>{checkDetails.length ? `${checkDetails.length} issue(s)` : `${schema?.tables.length ?? 0} objects`}</b>
      </div>
      {checkDetails.length > 0 && (
        <div className="health-details">
          {checkDetails.slice(0, 50).map((row, index) => (
            <code key={index}>{JSON.stringify(row)}</code>
          ))}
        </div>
      )}
    </div>
  );
}

function QueryComparePanel({
  leftSql,
  rightSql,
  setLeftSql,
  setRightSql,
  caseSensitive,
  setCaseSensitive,
  compareResult,
  onCompare
}: {
  leftSql: string;
  rightSql: string;
  setLeftSql: (sql: string) => void;
  setRightSql: (sql: string) => void;
  caseSensitive: boolean;
  setCaseSensitive: (value: boolean) => void;
  compareResult: SqlTextCompareResult | null;
  onCompare: () => void;
}) {
  const removalCount = compareResult?.leftOnly.length ?? 0;
  const additionCount = compareResult?.rightOnly.length ?? 0;
  const maxDiffRows = Math.max(removalCount, additionCount);
  const renderDiffText = (text: string, pairedText: string, side: "left" | "right") => {
    const parts = side === "left"
      ? diffWordParts(text, pairedText, caseSensitive).left
      : diffWordParts(pairedText, text, caseSensitive).right;
    return parts.map((part, index) => (
      <mark key={`${part.text}-${index}`} className={part.changed ? "changed" : ""}>
        {part.text}
      </mark>
    ));
  };

  return (
    <div className="query-compare-panel">
      <div className="compare-query-editors">
        <label>
          <span>Query A</span>
          <textarea value={leftSql} onChange={(event) => setLeftSql(event.target.value)} spellCheck={false} />
        </label>
        <label>
          <span>Query B</span>
          <textarea value={rightSql} onChange={(event) => setRightSql(event.target.value)} spellCheck={false} />
        </label>
      </div>
      <div className="compare-target-row">
        <label className="compare-case-toggle">
          <input
            type="checkbox"
            checked={caseSensitive}
            onChange={(event) => setCaseSensitive(event.target.checked)}
          />
          Case sensitive match
        </label>
        <button onClick={onCompare}><GitCompare size={16} /> Compare Queries</button>
      </div>
      {compareResult ? (
        <div className={compareResult.match ? "compare-status matched" : "compare-status mismatched"}>
          <strong>{compareResult.match ? "Matched" : "Mismatched"}</strong>
          <span>{compareResult.sharedCount} shared {caseSensitive ? "case-sensitive" : "case-insensitive"} SQL line(s)</span>
        </div>
      ) : (
        <div className="muted-block compact">Paste two SQL queries to compare their text structure.</div>
      )}
      {compareResult && (
        <div className="sql-text-diff">
          <div className="sql-diff-pane removed">
            <header>
              <strong>{removalCount} removal{removalCount === 1 ? "" : "s"}</strong>
              <span>{compareResult.leftLineCount} line{compareResult.leftLineCount === 1 ? "" : "s"}</span>
            </header>
            <div className="sql-diff-lines">
              {maxDiffRows ? Array.from({ length: maxDiffRows }).map((_, index) => (
                <code key={index} className={compareResult.leftOnly[index] ? "" : "empty"}>
                  <b>{compareResult.leftOnly[index] ? index + 1 : ""}</b>
                  <span>
                    {compareResult.leftOnly[index]
                      ? renderDiffText(compareResult.leftOnly[index], compareResult.rightOnly[index] ?? "", "left")
                      : ""}
                  </span>
                </code>
              )) : (
                <code className="empty"><b /> <span>No removals</span></code>
              )}
            </div>
          </div>
          <div className="sql-diff-pane added">
            <header>
              <strong>{additionCount} addition{additionCount === 1 ? "" : "s"}</strong>
              <span>{compareResult.rightLineCount} line{compareResult.rightLineCount === 1 ? "" : "s"}</span>
            </header>
            <div className="sql-diff-lines">
              {maxDiffRows ? Array.from({ length: maxDiffRows }).map((_, index) => (
                <code key={index} className={compareResult.rightOnly[index] ? "" : "empty"}>
                  <b>{compareResult.rightOnly[index] ? index + 1 : ""}</b>
                  <span>
                    {compareResult.rightOnly[index]
                      ? renderDiffText(compareResult.rightOnly[index], compareResult.leftOnly[index] ?? "", "right")
                      : ""}
                  </span>
                </code>
              )) : (
                <code className="empty"><b /> <span>No additions</span></code>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function ResultTable({
  result,
  tableKey,
  fkColumns,
  fkByColumn,
  onOpenRow,
  onNavigateForeignKey,
  onLoadMore,
  canLoadMore = false,
  isLoadingMore = false
}: {
  result: SqlQueryResult | null;
  tableKey: string;
  fkColumns: Set<string>;
  fkByColumn: Map<string, { table: string; from: string; to: string }>;
  onOpenRow: (row: Record<string, unknown>) => void;
  onNavigateForeignKey: (fk: { table: string; from: string; to: string }, value: unknown) => void;
  onLoadMore?: () => void;
  canLoadMore?: boolean;
  isLoadingMore?: boolean;
}) {
  const renderStep = 300;
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [sort, setSort] = useState<{ column: string; direction: "asc" | "desc" } | null>(null);
  const [renderLimit, setRenderLimit] = useState(renderStep);
  const [selectedJsonCell, setSelectedJsonCell] = useState<{ column: string; value: unknown } | null>(null);

  const visibleRows = useMemo(() => {
    if (!result) return [];
    const activeFilters = Object.entries(filters).filter(([, value]) => value.trim());
    const filtered = result.rows.filter((row) =>
      activeFilters.every(([column, filter]) =>
        valueText(row[column]).toLowerCase().includes(filter.trim().toLowerCase())
      )
    );
    if (!sort) return filtered;
    return [...filtered].sort((a, b) => {
      const left = a[sort.column];
      const right = b[sort.column];
      const leftNumber = typeof left === "number" ? left : Number(left);
      const rightNumber = typeof right === "number" ? right : Number(right);
      const bothNumeric = Number.isFinite(leftNumber) && Number.isFinite(rightNumber);
      const compare = bothNumeric
        ? leftNumber - rightNumber
        : valueText(left).localeCompare(valueText(right), undefined, { numeric: true, sensitivity: "base" });
      return sort.direction === "asc" ? compare : -compare;
    });
  }, [filters, result, sort]);

  useEffect(() => {
    setRenderLimit(renderStep);
  }, [filters, sort, tableKey]);

  const renderedRows = visibleRows.slice(0, renderLimit);

  function toggleSort(column: string) {
    setSort((current) => {
      if (!current || current.column !== column) return { column, direction: "asc" };
      if (current.direction === "asc") return { column, direction: "desc" };
      return null;
    });
  }

  function handleScroll(event: UIEvent<HTMLDivElement>) {
    const target = event.currentTarget;
    const nearBottom = target.scrollTop + target.clientHeight >= target.scrollHeight - 120;
    if (!nearBottom) return;
    if (visibleRows.length > renderLimit) {
      setRenderLimit((current) => Math.min(current + renderStep, visibleRows.length));
      return;
    }
    if (canLoadMore && !isLoadingMore) {
      onLoadMore?.();
    }
  }

  if (!result) return <div className="muted-block">No query result</div>;

  return (
    <div className="data-table" onScroll={handleScroll}>
      <table>
        <thead>
          <tr>
            {result.columns.map((column) => (
              <th key={column} className={fkColumns.has(column) ? "fk" : ""}>
                <button className="column-sort" onClick={() => toggleSort(column)} title="Sort column">
                  {fkColumns.has(column) && <Link2 size={13} />}
                  <span>{column}</span>
                  <em>{sort?.column === column ? (sort.direction === "asc" ? "↑" : "↓") : "↕"}</em>
                </button>
              </th>
            ))}
          </tr>
          <tr className="filter-row">
            {result.columns.map((column) => (
              <th key={`${column}-filter`}>
                <input
                  value={filters[column] ?? ""}
                  onChange={(event) => setFilters((current) => ({ ...current, [column]: event.target.value }))}
                  placeholder="Filter"
                  onClick={(event) => event.stopPropagation()}
                />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {renderedRows.map((row, rowIndex) => (
            <tr key={rowIndex} onClick={() => onOpenRow(row)}>
              {result.columns.map((column) => {
                const fk = fkByColumn.get(column);
                const jsonValue = parseJsonValue(row[column]);
                return (
                  <td key={column} className={fkColumns.has(column) ? "fk-cell" : ""}>
                    {fk ? (
                      <button
                        className="fk-value"
                        title="Double-click to open related row"
                        onClick={(event) => {
                          event.stopPropagation();
                        }}
                        onDoubleClick={(event) => {
                          event.stopPropagation();
                          onNavigateForeignKey(fk, row[column]);
                        }}
                      >
                        {valueText(row[column])}
                      </button>
                    ) : jsonValue.ok ? (
                      <div className="json-cell">
                        <span title={formattedValueText(row[column])}>{valueText(row[column])}</span>
                        <button
                          className="json-pretty-button"
                          onClick={(event) => {
                            event.stopPropagation();
                            setSelectedJsonCell({ column, value: row[column] });
                          }}
                        >
                          Pretty
                        </button>
                      </div>
                    ) : (
                      valueText(row[column])
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
          {!visibleRows.length && (
            <tr>
              <td colSpan={result.columns.length || 1} className="empty-cell">
                No rows match current filters
              </td>
            </tr>
          )}
          {(visibleRows.length > renderedRows.length || canLoadMore || isLoadingMore) && (
            <tr>
              <td colSpan={result.columns.length || 1} className="empty-cell">
                {visibleRows.length > renderedRows.length ? (
                  <button className="link-button" onClick={() => setRenderLimit((current) => current + renderStep)}>
                    Show next {Math.min(renderStep, visibleRows.length - renderedRows.length)} loaded row(s)
                  </button>
                ) : isLoadingMore ? (
                  "Loading more rows..."
                ) : canLoadMore ? (
                  <button className="link-button" onClick={onLoadMore}>
                    Load more rows
                  </button>
                ) : null}
              </td>
            </tr>
          )}
        </tbody>
      </table>
      {selectedJsonCell && (
        <JsonPrettyModal
          column={selectedJsonCell.column}
          value={selectedJsonCell.value}
          onClose={() => setSelectedJsonCell(null)}
        />
      )}
    </div>
  );
}

function JsonPrettyModal({
  column,
  value,
  onClose
}: {
  column: string;
  value: unknown;
  onClose: () => void;
}) {
  const parsed = parseJsonValue(value);
  const pretty = parsed.ok ? JSON.stringify(parsed.value, null, 2) : formattedValueText(value);

  return (
    <div className="field-detail-backdrop" onClick={onClose}>
      <section className="field-detail-modal json-pretty-modal" onClick={(event) => event.stopPropagation()}>
        <header>
          <div>
            <span className="eyebrow">Pretty JSON</span>
            <h3>{column}</h3>
          </div>
          <button onClick={onClose}>Close</button>
        </header>
        <div className="json-pretty-body">
          <pre>{pretty}</pre>
        </div>
      </section>
    </div>
  );
}

function RowModal({
  row,
  rowKey,
  table,
  canEdit,
  onSave,
  onClose
}: {
  row: Record<string, unknown>;
  rowKey: Record<string, unknown>;
  table: string;
  canEdit: boolean;
  onSave: (key: Record<string, unknown>, values: Record<string, unknown>) => Promise<void>;
  onClose: () => void;
}) {
  const [draft, setDraft] = useState<Record<string, string>>(() =>
    Object.fromEntries(Object.entries(row).map(([key, value]) => [key, value === null || value === undefined ? "" : String(value)]))
  );
  const [isEditing, setIsEditing] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState("");

  const changedValues = useMemo(() => {
    const values: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(draft)) {
      const original = row[key];
      if (value !== (original === null || original === undefined ? "" : String(original))) {
        values[key] = value;
      }
    }
    return values;
  }, [draft, row]);
  const hasChanges = Object.keys(changedValues).length > 0;

  useEffect(() => {
    setDraft(Object.fromEntries(Object.entries(row).map(([key, value]) => [key, value === null || value === undefined ? "" : String(value)])));
  }, [row]);

  async function saveChanges() {
    if (!hasChanges || isSaving) return;
    setIsSaving(true);
    setSaveStatus("Staging row changes");
    try {
      await onSave(rowKey, changedValues);
      setSaveStatus("Changes staged.");
      setIsEditing(false);
    } catch (error) {
      setSaveStatus(error instanceof Error ? error.message : "Unable to stage row changes.");
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <section className="row-modal" onClick={(event) => event.stopPropagation()}>
        <header>
          <div>
            <strong>Entire Row</strong>
            <span>{table}</span>
          </div>
          <div className="row-modal-actions">
            {canEdit && (
              <button onClick={() => setIsEditing((value) => !value)}>
                {isEditing ? "View" : "Edit"}
              </button>
            )}
            <button onClick={onClose}>Close</button>
          </div>
        </header>
        {canEdit && (
          <div className="row-edit-bar">
            <span>{Object.keys(rowKey).length} key field(s)</span>
            {saveStatus && <strong>{saveStatus}</strong>}
          </div>
        )}
        <div className="row-fields">
          {Object.entries(row).map(([key, value]) => (
            <label key={key}>
              <span>{key}</span>
              {isEditing ? (
                <textarea
                  value={draft[key] ?? ""}
                  onChange={(event) => setDraft((current) => ({ ...current, [key]: event.target.value }))}
                  spellCheck={false}
                />
              ) : (
                <code>{valueText(value)}</code>
              )}
            </label>
          ))}
        </div>
        {isEditing && (
          <footer className="row-save-footer">
            <button disabled={!hasChanges || isSaving} onClick={saveChanges}>
              {isSaving ? "Staging..." : "Stage changes"}
            </button>
            <span>{hasChanges ? `${Object.keys(changedValues).length} field(s) changed` : "No changes"}</span>
          </footer>
        )}
      </section>
    </div>
  );
}
