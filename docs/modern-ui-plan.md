# DBCompare Modern UI Plan

## Decision

Use Electron + React + TypeScript for the desktop UI and keep Python as the database engine.

The existing `core/` package remains the source of truth for database behavior. The new `backend/`
package exposes that logic through a local FastAPI API consumed by the React UI.

## Runtime Shape

```text
Electron desktop shell
  - native app window
  - file picker bridge
  - starts/stops Python API

React UI
  - compare workspace
  - SQL explorer
  - row popup viewer
  - foreign-key navigation surface

Python FastAPI backend
  - wraps existing core modules
  - exposes compare and SQLite inspection APIs
```

## Development

Install Python dependencies:

```powershell
pip install -r requirements.txt
```

Install frontend dependencies:

```powershell
npm install
```

Run the modern app:

```powershell
npm run dev
```

The legacy Tkinter app remains available:

```powershell
python main.py
```

## Packaging Direction

Create a backend executable first:

```powershell
pip install -r requirements.txt -r requirements-build.txt
pyinstaller --name dbcompare-api --onefile --distpath backend-dist --workpath build/pyinstaller backend/run.py
```

Then build the desktop app:

```powershell
npm run dist:win
```

For macOS, build on macOS:

```bash
npm run dist:mac
```

macOS signing and notarization can be added to `electron-builder` config once certificates are ready.

## Feature Parity Guard

Do not rewrite comparison, sanitization, FTS, settings repair, `.vyp`, or `.vyb` handling in the UI.
All UI operations should call the Python backend, and the backend should call existing `core/` functions.
