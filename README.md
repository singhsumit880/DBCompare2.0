# DBCompare 2.0 / DB Explorer Pro

DBCompare 2.0 is a desktop utility for comparing, browsing, repairing, and sanitizing SQLite-based database files. It is designed especially for Vyapar-style `.vyp` and `.vyb` files, while also supporting normal SQLite databases such as `.db`, `.sqlite`, `.db3`, `.sqlitedb`, `.s3db`, and `.sl3`.

The project includes two interfaces:

- **Modern desktop app**: Electron + React UI with a local FastAPI backend.
- **Legacy desktop app**: Tkinter/ttkbootstrap UI that calls the same core Python database logic.

The modern app is the recommended interface for day-to-day use.

## What This App Does

DBCompare helps you inspect two database files side by side and understand exactly what changed. It can compare schema, database version, table data, JSON fields, and row-level differences. It also includes practical tools for sanitizing customer/business data, converting or repacking `.vyp` / `.vyb` files, creating FTS search databases, repairing settings tables, and running SQL queries safely.

## Feature Highlights

- **Database comparison**
  - Compare two SQLite, `.vyp`, or `.vyb` files.
  - Detect added, removed, and modified rows.
  - Detect schema changes and missing columns.
  - Compare SQLite `PRAGMA user_version`.
  - Show row-level and field-level differences.
  - Expand changed JSON fields with structured JSON diff views.
  - Configure ignored tables, datetime handling, decimal precision, and result limits.

- **SQL explorer**
  - Browse tables and table metadata.
  - View paginated table rows.
  - Run SQL queries.
  - Optional write mode for update workflows.
  - Export table or query results.
  - Inspect related rows using foreign-key style relationships.
  - Compare two SQL query texts visually.

- **Database sanitizer**
  - Run a default set of sanitization SQL statements.
  - Edit or add custom SQL statements before running.
  - Generate sanitized output copies instead of editing the original file directly.
  - Useful for removing phone numbers, emails, transaction message settings, mobile numbers, and repeat invoice data.

- **Vyapar file support**
  - `.vyp` files are treated as SQLite databases.
  - `.vyb` files are treated as zipped backups containing `.vyp` data.
  - Tools can extract, process, and repack `.vyb` outputs.

- **Repair and utility tools**
  - Repair `kb_settings` data.
  - Convert / repack database files.
  - Build FTS database support for faster text search.
  - Validate database health.

- **Packaging**
  - Build a Windows portable executable.
  - Build an NSIS installer target when the local environment supports it.
  - Build the Python backend into a standalone executable with PyInstaller.

## Project Structure

```text
DBCompare2.0/
|-- backend/              # FastAPI backend used by Electron app
|-- core/                 # Shared database logic: compare, sanitize, repair, FTS, file IO
|-- electron/             # Electron main/preload code and packaging assets
|-- frontend/             # React + TypeScript frontend
|-- ui/                   # Legacy Tkinter UI
|-- web/                  # Additional web/static assets if present
|-- dist/                 # Built frontend output
|-- backend-dist/         # Built backend executable output
|-- release/              # Electron packaged output
|-- main.py               # Legacy Tkinter app entry point
|-- package.json          # Node/Electron scripts and package config
|-- requirements.txt       # Runtime Python dependencies
`-- requirements-build.txt # Build-only Python dependencies
```

## Requirements

Install these before running the project from source:

- **Windows 10/11** recommended for packaged `.exe` builds.
- **Python 3.10+**.
- **Node.js 20+** recommended.
- **npm**.

Python dependencies:

```text
ttkbootstrap
fastapi
uvicorn
pyinstaller
```

Node dependencies are installed from `package.json`.

## Quick Start: Modern Desktop App

From the project root:

```powershell
pip install -r requirements.txt
pip install -r requirements-build.txt
npm install
npm run dev
```

This starts:

- Vite React UI at `http://127.0.0.1:5173`
- Electron desktop shell
- Local Python backend API

The Electron app automatically connects to the local backend.

## Quick Start: Legacy Tkinter App

If you want to run the older Python desktop UI:

```powershell
pip install -r requirements.txt
python main.py
```

The legacy UI is still useful for direct Python-based workflows, but the modern Electron app has the richer interface.

## How To Use

### Compare Two Databases

1. Open the modern desktop app.
2. Go to **DB Compare**.
3. Select **Database 1** and **Database 2**.
4. Adjust options if needed:
   - ignored tables
   - datetime normalization
   - decimal precision
   - max rows per table
5. Click **Compare**.
6. Review:
   - changed tables
   - rows only in DB1
   - rows only in DB2
   - modified rows
   - schema differences
   - JSON field differences

### Browse and Query a Database

1. Go to **SQL Explorer**.
2. Select a database file.
3. Browse tables from the sidebar.
4. View table rows, schema, health checks, and related data.
5. Run SQL queries in the editor.
6. Export data when needed.

Write operations are intentionally gated behind write-mode controls. Use them carefully and prefer working on a copy.

### Sanitize a Database

1. Go to **DB Sanitizer**.
2. Select a `.vyp`, `.vyb`, or SQLite database.
3. Review the default SQL statements.
4. Add, remove, or edit SQL statements as needed.
5. Run the sanitizer.
6. Download or use the generated sanitized output files.

The default sanitizer queries include cleanup for common personal/business fields and transaction metadata, including:

```sql
UPDATE kb_names SET phone_number = '', email = '';
UPDATE kb_settings SET setting_value = '' WHERE setting_key = 'VYAPAR.TXNMSGOWNERNUMBER';
UPDATE kb_firms SET firm_phone = '665565', firm_email = '';
UPDATE kb_transactions SET mobile_no = '';
UPDATE kb_transactions SET additional_details_json = NULL;
UPDATE repeat_invoice_template SET next_due_date = NULL, end_date = NULL, week_days = NULL, on_day = NULL, paused_until = NULL, txn_json = '{}';
```

### Convert / Repack Files

Use **Convert / Repack** when you need to extract or rebuild `.vyp` / `.vyb` style outputs. For `.vyb`, the app handles extracting the `.vyp`, processing it, and creating a repacked `.vyb` output.

### Repair Settings Table

Use **Settings Repair** to create repaired database copies when `kb_settings` data is missing or inconsistent.

### Build FTS

Use **FTS Builder** to prepare a database for faster full-text searching across transaction, item, party, serial, invoice, and payment-style data.

## Supported File Types

Common supported inputs:

- `.vyp`
- `.vyb`
- `.db`
- `.sqlite`
- `.db3`
- `.sqlitedb`
- `.s3db`
- `.sl3`

Notes:

- `.vyp` is handled as a SQLite database.
- `.vyb` is handled as a zip-style backup that contains a `.vyp` database.
- For non-`.vyp` / non-`.vyb` SQLite inputs, output files preserve the original style where the tool supports it.

## Development Commands

Install dependencies:

```powershell
pip install -r requirements.txt
pip install -r requirements-build.txt
npm install
```

Run only the backend:

```powershell
npm run backend
```

Run only the React UI:

```powershell
npm run dev:ui
```

Run Electron development mode:

```powershell
npm run dev
```

Build the React UI:

```powershell
npm run build:ui
```

Build the Python backend executable:

```powershell
npm run build:backend
```

Build the unpacked Electron app:

```powershell
npm run pack
```

## Create Windows Executable

To build the Windows installer and portable executable:

```powershell
npm run dist:win
```

Expected outputs are written to:

```text
release/
```

Typical artifacts:

```text
release/DB Explorer Pro 1.0.0.exe
release/DB Explorer Pro Setup 1.0.0.exe
release/win-unpacked/DB Explorer Pro.exe
```

If the NSIS installer step fails on your machine, you can still create the portable executable:

```powershell
npx electron-builder --win portable
```

The portable output is:

```text
release/DB Explorer Pro 1.0.0.exe
```

## Backend API

The modern app uses a local FastAPI service. The Electron shell starts the backend automatically in packaged mode.

Useful endpoints include:

- `GET /health`
- `POST /api/compare`
- `POST /api/compare/jobs`
- `GET /api/compare/jobs/{job_id}`
- `GET /api/compare/jobs/{job_id}/result`
- `POST /api/compare/jobs/{job_id}/cancel`
- `POST /api/tools/sanitize`
- `POST /api/tools/convert`
- `POST /api/tools/fts`
- `POST /api/tools/settings-repair`
- `POST /api/sql/tables`
- `POST /api/sql/table-info`
- `POST /api/sql/schema`
- `POST /api/sql/rows`
- `POST /api/sql/query`
- `POST /api/sql/export`
- `POST /api/sql/compare-query`

By default, the backend runs on localhost only.

## Safety Notes

- Prefer working on copies of production databases.
- Sanitizer, repair, and conversion flows generate output copies where applicable.
- SQL Explorer write mode can modify data. Enable it only when you intend to write changes.
- Keep backups of original `.vyp` and `.vyb` files before running repair or custom SQL workflows.

## Troubleshooting

### Electron opens but says backend is starting

Wait a few seconds. The app polls the local backend while it starts. If it does not become ready:

1. Restart the app.
2. Check that antivirus software is not blocking `dbcompare-api.exe`.
3. Rebuild the backend:

```powershell
npm run build:backend
```

### Vite build fails with spawn or permission errors

Close running app instances and terminals that may be locking files, then retry:

```powershell
npm run build:ui
```

On restricted machines, run the terminal as a user with permission to execute local Node helper binaries.

### NSIS installer build fails

Build the portable executable instead:

```powershell
npx electron-builder --win portable
```

### Python module not found

Reinstall dependencies:

```powershell
pip install -r requirements.txt
pip install -r requirements-build.txt
```

### Database does not open

Check that:

- the file exists,
- the file is not locked by another application,
- the file is a valid SQLite database or supported `.vyp` / `.vyb` file,
- you have read permission for the file path.

## Contributing

Pull requests are welcome. If you add features that require new dependencies, update:

- `requirements.txt`
- `requirements-build.txt`
- `package.json`
- this README

Please keep core database behavior in `core/` where possible so both UI layers can reuse it.

## License

No license file is currently included. Add a license before distributing publicly if this repository will be open sourced.
