# DBCompare2.0

A small GUI utility for working with Vyapar-style SQLite databases (vyp/vyb) and general SQLite files.

This repository provides tools to compare two databases, sanitize data, generate FTS tables, and repair/export/import tables. It supports a variety of SQLite file extensions and preserves input file extensions for output (except for .vyp/.vyb which keep their special behavior).

## Install dependencies

Create a virtual environment (recommended) and install dependencies listed in `requirements.txt`:


## Quick start - legacy Tkinter app

- Run the app:

PowerShell:
```powershell
python main.py
```

- Select input files. Supported input types include common SQLite extensions (for example: `.db`, `.sqlite`, `.db3`, `.sqlitedb`, `.s3db`, `.sl3`) as well as `.vyp` and `.vyb`.

Notes:
- `.vyb` is treated as a zip archive containing a `.vyp` file. The app will extract the `.vyp`, operate on it, and repackage `.vyb` where applicable.
- For non `.vyp`/`.vyb` inputs the output will preserve the same extension as input (so a `.db` input will produce a `.db` output), as requested.


## Contributing

PRs welcome. If you add features that require new dependencies, please update `requirements.txt` and the README install instructions.

## Modern desktop UI

A new Electron + React UI is being developed beside the existing Tkinter app.
It uses a local Python FastAPI backend so the existing database logic in `core/`
stays intact.

Development:

```powershell
pip install -r requirements.txt
npm install
npm run dev
```

Packaging notes for `.exe` and `.dmg` are in `docs/modern-ui-plan.md`.



