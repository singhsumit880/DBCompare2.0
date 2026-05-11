import { useEffect, useMemo, useRef, useState, type UIEvent } from "react";
import {
  AlertTriangle,
  ArrowLeftToLine,
  ArrowRight,
  ArrowRightToLine,
  Database,
  Download,
  FolderOpen,
  Link2,
  Play,
  RefreshCcw,
  RotateCcw,
  Rows3,
  Search,
  Table2,
  Wrench
} from "lucide-react";
import {
  buildFtsDatabase,
  cancelCompareJob,
  convertDatabase,
  databaseChecks,
  databaseVersion,
  executeSql,
  getCompareJob,
  getCompareJobResult,
  listTables,
  repairSettingsTable,
  sanitizeDatabase,
  startCompareJob,
  tableInfo,
  tableRows,
  updateRow
} from "./api";
import type { ComparisonReport, RowUpdateResult, SqlQueryResult, TableColumn, TableDataResult, ToolRunResult } from "./types";

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
UPDATE kb_transactions SET mobile_no = '';`;

type NavigationItem = {
  label: string;
  table: string;
  query?: string;
};

type SelectedRowState = {
  row: Record<string, unknown>;
  key: Record<string, unknown>;
};

type Screen = "compare" | "sql" | "sanitizer" | "settings" | "fts";
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
  const [collapsed, setCollapsed] = useState(false);
  const [db1, setDb1] = useState("");
  const [db2, setDb2] = useState("");
  const [activeDb, setActiveDb] = useState("");

  useEffect(() => {
    const launchFile = window.dbcompare?.initialOpenFile;
    if (!launchFile) return;
    setActiveDb(launchFile);
    setScreen("sql");
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
                <span>2.0</span>
              </div>
            )}
          </div>
        </div>
        <nav>
          <button className={screen === "sql" ? "active" : ""} onClick={() => setScreen("sql")}>
            <img className="menu-icon" src="./menu-icons/open-db.png" alt="" />
            {!collapsed && <span>Open DB</span>}
          </button>
          <button className={screen === "compare" ? "active" : ""} onClick={() => setScreen("compare")}>
            <img className="menu-icon" src="./menu-icons/db-compare.png" alt="" />
            {!collapsed && <span>DB Compare</span>}
          </button>
          <button className={screen === "sanitizer" ? "active" : ""} onClick={() => setScreen("sanitizer")}>
            <img className="menu-icon" src="./menu-icons/db-sanitizer.png" alt="" />
            {!collapsed && <span>DB Sanitizer</span>}
          </button>
          <button className={screen === "settings" ? "active" : ""} onClick={() => setScreen("settings")}>
            <img className="menu-icon" src="./menu-icons/setting-repair.png" alt="" />
            {!collapsed && <span>Setting Table Repair</span>}
          </button>
          <button className={screen === "fts" ? "active" : ""} onClick={() => setScreen("fts")}>
            <img className="menu-icon" src="./menu-icons/fts.png" alt="" />
            {!collapsed && <span>FTS Table Generator</span>}
          </button>
        </nav>
        <button className="icon-button sidebar-collapse" title="Collapse menu" onClick={() => setCollapsed((value) => !value)}>
          {collapsed ? <ArrowRightToLine size={18} /> : <ArrowLeftToLine size={18} />}
        </button>
      </aside>

      <section className="workspace">
        {screen === "compare" ? (
          <CompareWorkspace db1={db1} db2={db2} setDb1={setDb1} setDb2={setDb2} />
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
              <th>Schema Config</th>
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
                    {schemaDiffers ? `Differs (${schemaDiffCount(table)})` : "Match"}
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
  const [tab, setTab] = useState<DetailTab>(
    tableCount(table, "modified") ? "modified" : tableCount(table, "db1") ? "db1" : tableCount(table, "db2") ? "db2" : "schema"
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
          <button className={`${tab === "modified" ? "active" : ""} ${tableCount(table, "modified") > 0 ? "has-count" : ""}`} onClick={() => setTab("modified")}>Modified ({tableCount(table, "modified")})</button>
          <button className={`${tab === "db1" ? "active" : ""} ${tableCount(table, "db1") > 0 ? "has-count" : ""}`} onClick={() => setTab("db1")}>DB1 only ({tableCount(table, "db1")})</button>
          <button className={`${tab === "db2" ? "active" : ""} ${tableCount(table, "db2") > 0 ? "has-count" : ""}`} onClick={() => setTab("db2")}>DB2 only ({tableCount(table, "db2")})</button>
          <button className={tab === "complete" ? "active" : ""} onClick={() => setTab("complete")}>Complete Data</button>
          <button className={`${tab === "schema" ? "active" : ""} ${schemaDiffCount(table) > 0 ? "has-count" : ""}`} onClick={() => setTab("schema")}>
            Schema ({schemaDiffCount(table)})
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
                      <button className="change-pill" onClick={() => setFieldDetail({ column, db1Value, db2Value })}>
                        View change
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
  const [loadedDb1Rows, setLoadedDb1Rows] = useState<Record<string, unknown>[]>(table.all_db1_rows ?? []);
  const [loadedDb2Rows, setLoadedDb2Rows] = useState<Record<string, unknown>[]>(table.all_db2_rows ?? []);
  const [loadStatus, setLoadStatus] = useState("Loading complete table data");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(100);
  const [db1Total, setDb1Total] = useState<number | null>(null);
  const [db2Total, setDb2Total] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadCompleteRows() {
      if ((table.all_db1_rows?.length || table.all_db2_rows?.length) && !cancelled) {
        setDb1Total(table.all_db1_rows_count ?? table.all_db1_rows?.length ?? 0);
        setDb2Total(table.all_db2_rows_count ?? table.all_db2_rows?.length ?? 0);
        setLoadStatus("Showing complete data from comparison result");
        return;
      }

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
  }, [db1Path, db2Path, table, page, pageSize]);

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
      <section className="field-detail-modal" onClick={(event) => event.stopPropagation()}>
        <header>
          <div>
            <span className="eyebrow">Changed field</span>
            <h3>{column}</h3>
          </div>
          <button onClick={onClose}>Close</button>
        </header>
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
        <h3>Only in Database 1 ({diff.only_in_db1.length})</h3>
        {diff.only_in_db1.map((column) => <span key={column}>{column}</span>)}
        {!diff.only_in_db1.length && <em>None</em>}
      </section>
      <section>
        <h3>Only in Database 2 ({diff.only_in_db2.length})</h3>
        {diff.only_in_db2.map((column) => <span key={column}>{column}</span>)}
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
  const [sql, setSql] = useState("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name;");
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
  const [explorerTab, setExplorerTab] = useState<"browse" | "query">("browse");
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
  const fkByColumn = new Map(foreignKeys.map((fk) => [fk.from, fk]));
  const fkColumns = new Set(fkByColumn.keys());
  const visibleTables = tables.filter((table) => table.toLowerCase().includes(tableFilter.toLowerCase()));

  useEffect(() => {
    if (dbPath) {
      loadTables();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbPath]);

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

      loadDatabaseVersion()
        .then((version) => {
          setStatus(`${data.tables.length} table(s) loaded.${version === null ? "" : ` DB Version: ${version}.`}`);
        })
        .catch(() => undefined);

      if (data.tables[0]) {
        await inspectTable(data.tables[0]);
      }
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
    setSql("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name;");
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
    setExplorerTab("browse");
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
    setSelectedTable(table);
    setExplorerTab("browse");
    if (!keepTrail) {
      setNavigationTrail([{ label: table, table }]);
    }
    setStatus(`Opening ${table}`);
    try {
      const [info, rows] = await Promise.all([tableInfo(dbPath, table), tableRows(dbPath, table, browsePageSize, 0)]);
      setForeignKeys(info.foreign_keys);
      setTableColumns(info.columns);
      setResult({ columns: rows.columns, rows: rows.rows, row_count: rows.row_count ?? info.row_count });
      setSql(`SELECT * FROM "${table}" LIMIT 250;`);
      setStatus(`${table}: ${info.row_count} row(s), ${info.columns.length} column(s). Loaded ${rows.rows.length}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to inspect table.");
    }
  }

  async function loadMoreTableRows() {
    if (!dbPath || !selectedTable || explorerTab !== "browse" || !result || isLoadingMoreRows) return;
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
    setStatus("Running query");
    try {
      const data = await executeSql(dbPath, sql);
      setResult(data);
      rememberQuery(sql);
      setStatus(`${data.row_count} row(s) returned.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Query failed.");
    }
  }

  async function navigateForeignKey(fk: { table: string; from: string; to: string }, value: unknown) {
    if (value === null || value === undefined || value === "") return;
    const escaped = String(value).replace(/'/g, "''");
    const query = `SELECT * FROM "${fk.table}" WHERE "${fk.to}" = '${escaped}' LIMIT 250;`;
    const sourceTable = selectedTable;
    setExplorerTab("browse");
    setResult(null);
    setStatus(`Opening ${fk.table} where ${fk.to} = ${valueText(value)}`);
    try {
      const [info, data] = await Promise.all([
        tableInfo(dbPath, fk.table),
        executeSql(dbPath, query, false, 250)
      ]);
      setSelectedTable(fk.table);
      setTableColumns(info.columns);
      setSql(query);
      setNavigationTrail((trail) => {
        const baseTrail = trail.length ? trail : sourceTable ? [{ label: sourceTable, table: sourceTable }] : [];
        return [...baseTrail, { label: `${fk.table} (${fk.to}=${valueText(value)})`, table: fk.table, query }];
      });
      setForeignKeys(info.foreign_keys);
      setResult({ columns: data.columns, rows: data.rows, row_count: data.row_count });
      rememberQuery(query);
      setStatus(`${data.rows.length} related row(s) found in ${fk.table} for ${fk.to} = ${valueText(value)}.`);
    } catch (error) {
      setResult({ columns: [], rows: [], row_count: 0 });
      setStatus(error instanceof Error ? error.message : "Unable to open related row.");
    }
  }

  function insertSnippet(snippet: string) {
    setSql((current) => {
      if (snippet === "SELECT *" && selectedTable) return `SELECT * FROM "${selectedTable}" LIMIT 100;`;
      if (snippet === "COUNT(*)" && selectedTable) return `SELECT COUNT(*) AS total FROM "${selectedTable}";`;
      if (snippet === "LIMIT 100") return current.trim().replace(/;$/, "") + " LIMIT 100;";
      return current;
    });
  }

  async function openTrailItem(item: NavigationItem, index: number) {
    setExplorerTab("browse");
    setResult(null);
    setStatus(`Opening ${item.label}`);
    try {
      const [info, data] = await Promise.all([
        tableInfo(dbPath, item.table),
        item.query ? executeSql(dbPath, item.query, false, 250) : tableRows(dbPath, item.table, browsePageSize, 0)
      ]);
      setSelectedTable(item.table);
      setForeignKeys(info.foreign_keys);
      setTableColumns(info.columns);
      setResult({ columns: data.columns, rows: data.rows, row_count: data.row_count ?? data.rows.length });
      setSql(item.query ?? `SELECT * FROM "${item.table}" LIMIT 250;`);
      setNavigationTrail((trail) => trail.slice(0, index + 1));
      setStatus(`${item.label}: ${data.rows.length} row(s).`);
    } catch (error) {
      setResult({ columns: [], rows: [], row_count: 0 });
      setStatus(error instanceof Error ? error.message : "Unable to open breadcrumb item.");
    }
  }

  async function saveRowEdit(
    key: Record<string, unknown>,
    values: Record<string, unknown>
  ): Promise<RowUpdateResult> {
    if (!dbPath || !selectedTable) {
      throw new Error("Select a database table before saving.");
    }

    const response = await updateRow(dbPath, selectedTable, key, values);
    const nextRow = response.row ?? { ...(selectedRow?.row ?? {}), ...values };
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
    setStatus(
      response.mode === "repacked"
        ? "Row edited. Save the generated .vyb copy to keep archive changes."
        : "Row edited and saved directly."
    );
    return response;
  }

  return (
    <>
      <section className="compare-form">
        <div className="form-card input-card sql-input-card">
          <PathPicker label="Database" value={dbPath} onChange={setDbPath} />
          <button className="reset-button" onClick={resetExplorer} disabled={isLoadingDb}>
            <RotateCcw size={16} /> Reset
          </button>
        </div>
        {isDbLoaded && (
          <div className="db-info-strip">
            <span>{tables.length} table(s)</span>
            <span>DB Version: {dbVersion ?? "-"}</span>
            <button onClick={() => setExplorerTab("query")}>
              <Play size={15} /> Execute query
            </button>
            <button onClick={() => runDatabaseCheck("integrity")}>Integrity check</button>
            <button onClick={() => runDatabaseCheck("foreign")}>Foreign-key check</button>
            {checkStatus && !/not found/i.test(checkStatus) && <strong>{checkStatus}</strong>}
          </div>
        )}
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
      <section className="sql-grid">
        <section className="panel table-browser">
          <div className="panel-title">
            <Table2 size={18} />
            <strong>Tables</strong>
            <span>{tables.length}</span>
          </div>
          <div className="table-search">
            <Search size={16} />
            <input value={tableFilter} onChange={(event) => setTableFilter(event.target.value)} placeholder="Search tables" />
          </div>
          <div className="table-list">
            {visibleTables.map((table) => (
              <button key={table} className={table === selectedTable ? "selected" : ""} onClick={() => inspectTable(table)}>
                <Table2 size={17} />
                <span>{table}</span>
              </button>
            ))}
          </div>
        </section>
        <section className="panel sql-panel">
          <div className="explorer-tabs single">
            <button className={explorerTab === "browse" ? "active" : ""} onClick={() => setExplorerTab("browse")}>
              Browse Data
            </button>
          </div>
          {explorerTab === "query" ? (
            <>
              <div className="editor-bar">
                <div>
                  <strong>SQL Console</strong>
                  <span>{selectedTable || "No table selected"}</span>
                </div>
                <div className="editor-actions">
                  <button onClick={() => insertSnippet("SELECT *")}>SELECT *</button>
                  <button onClick={() => insertSnippet("COUNT(*)")}>COUNT(*)</button>
                  <button onClick={() => insertSnippet("LIMIT 100")}>LIMIT 100</button>
                  <button className="ghost-button" onClick={() => setSql("")}>
                    <RefreshCcw size={16} /> Clear
                  </button>
                </div>
              </div>
              <textarea value={sql} onChange={(event) => setSql(event.target.value)} spellCheck={false} />
              <button className="floating-run" title="Execute query" onClick={runSql}>
                <Play size={20} />
              </button>
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
            <div className="browse-heading compact">
              <span>
                {selectedTable
                  ? `${selectedTable} - ${result?.row_count ?? 0} row(s)${result ? `, loaded ${result.rows.length}` : ""}`
                  : "Select a table"}
              </span>
            </div>
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
          {explorerTab === "query" && <div className="sql-status">{status}</div>}
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
            canLoadMore={explorerTab === "browse" && Boolean(result && result.rows.length < result.row_count)}
            isLoadingMore={isLoadingMoreRows}
          />
        </section>
      </section>
      )}
      {selectedRow && (
        <RowModal
          row={selectedRow.row}
          rowKey={selectedRow.key}
          table={selectedTable}
          canEdit={explorerTab === "browse" && Boolean(selectedTable)}
          onSave={saveRowEdit}
          onClose={() => setSelectedRow(null)}
        />
      )}
    </>
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
  onSave: (key: Record<string, unknown>, values: Record<string, unknown>) => Promise<RowUpdateResult>;
  onClose: () => void;
}) {
  const [draft, setDraft] = useState<Record<string, string>>(() =>
    Object.fromEntries(Object.entries(row).map(([key, value]) => [key, value === null || value === undefined ? "" : String(value)]))
  );
  const [isEditing, setIsEditing] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState("");
  const [generatedPath, setGeneratedPath] = useState<string | null>(null);

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
    setSaveStatus("Saving row");
    setGeneratedPath(null);
    try {
      const response = await onSave(rowKey, changedValues);
      setSaveStatus(response.mode === "repacked" ? "Edited copy created. Save the new .vyb file." : "Saved directly.");
      setGeneratedPath(response.output_vyb ?? null);
      setIsEditing(false);
    } catch (error) {
      setSaveStatus(error instanceof Error ? error.message : "Unable to save row.");
    } finally {
      setIsSaving(false);
    }
  }

  async function saveGeneratedCopy() {
    if (!generatedPath || !window.dbcompare?.saveGeneratedFile) return;
    const result = await window.dbcompare.saveGeneratedFile(generatedPath, fileNameFromPath(generatedPath));
    if (result.error) {
      setSaveStatus(result.error);
    } else if (result.saved && result.path) {
      setSaveStatus(`Saved copy to ${result.path}`);
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
            {generatedPath && (
              <button onClick={saveGeneratedCopy}>Save edited .vyb copy</button>
            )}
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
              {isSaving ? "Saving..." : "Save changes"}
            </button>
            <span>{hasChanges ? `${Object.keys(changedValues).length} field(s) changed` : "No changes"}</span>
          </footer>
        )}
      </section>
    </div>
  );
}
