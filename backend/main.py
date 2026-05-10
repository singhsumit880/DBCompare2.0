"""
FastAPI bridge for the modern DBCompare UI.

This layer intentionally wraps the existing core modules instead of
reimplementing database behavior in the frontend.
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict, is_dataclass
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from core.comparator import (
    DEFAULT_DECIMAL_PRECISION,
    DEFAULT_EXCLUDED_TABLES,
    DatabaseComparator,
)
from core.db_io import extract_database_file, is_valid_sqlite
from core.fts_builder import process_fts
from core.sanitizer import convert_file, process_sanitization
from core.settings_repair import process_settings_repair
from core.utils import cleanup_dir


app = FastAPI(title="DBCompare 2.0 API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CompareRequest(BaseModel):
    db1_path: str
    db2_path: str
    included_tables: list[str] = Field(default_factory=list)
    excluded_tables: list[str] = Field(default_factory=lambda: sorted(DEFAULT_EXCLUDED_TABLES))
    ignore_datetime: bool = True
    decimal_precision: int = DEFAULT_DECIMAL_PRECISION
    validate_db: bool = True


class DatabasePathRequest(BaseModel):
    db_path: str


class TableRequest(DatabasePathRequest):
    table: str


class QueryRequest(DatabasePathRequest):
    sql: str
    limit: int = 500
    allow_write: bool = False


class RowRequest(TableRequest):
    key: dict[str, Any]


class RelatedRowRequest(TableRequest):
    column: str
    value: Any
    limit: int = 250


class SanitizeRequest(DatabasePathRequest):
    queries: list[str] = Field(default_factory=list)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class ResolvedDatabase:
    def __init__(self, source_path: str):
        self.source_path = source_path
        self.temp_dirs: list[str] = []
        self.path = extract_database_file(source_path, self.temp_dirs)

    def close(self) -> None:
        for temp_dir in self.temp_dirs:
            cleanup_dir(temp_dir)
        self.temp_dirs = []

    def __enter__(self) -> "ResolvedDatabase":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def _connect_resolved(db_path: str) -> tuple[ResolvedDatabase, sqlite3.Connection]:
    resolved = ResolvedDatabase(db_path)
    conn = sqlite3.connect(resolved.path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return resolved, conn


def _table_columns(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    return [
        {
            "cid": row["cid"],
            "name": row["name"],
            "type": row["type"],
            "notnull": bool(row["notnull"]),
            "default": row["dflt_value"],
            "pk": row["pk"],
        }
        for row in rows
    ]


def _foreign_keys(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = conn.execute(f"PRAGMA foreign_key_list({_quote_identifier(table)})").fetchall()
    return [
        {
            "id": row["id"],
            "seq": row["seq"],
            "table": row["table"],
            "from": row["from"],
            "to": row["to"],
            "on_update": row["on_update"],
            "on_delete": row["on_delete"],
            "match": row["match"],
        }
        for row in rows
    ]


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: _jsonable(row[key]) for key in row.keys()}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/compare")
def compare_databases(request: CompareRequest) -> dict[str, Any]:
    comparator = DatabaseComparator()
    try:
        report = comparator.compare(
            request.db1_path,
            request.db2_path,
            included_tables=set(request.included_tables),
            excluded_tables=set(request.excluded_tables),
            ignore_datetime=request.ignore_datetime,
            decimal_precision=request.decimal_precision,
            validate_db=request.validate_db,
        )
        return _jsonable(report)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/tools/sanitize")
def sanitize_database(request: SanitizeRequest) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []

    def progress(message: str, pct: int) -> None:
        messages.append({"message": message, "percent": pct})

    try:
        output_vyp, output_vyb = process_sanitization(request.db_path, request.queries, progress)
        return {
            "output_vyp": output_vyp,
            "output_vyb": output_vyb,
            "messages": messages,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/tools/convert")
def convert_database(request: DatabasePathRequest) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []

    def progress(message: str, pct: int) -> None:
        messages.append({"message": message, "percent": pct})

    try:
        output_vyp, output_vyb = convert_file(request.db_path, progress)
        return {
            "output_vyp": output_vyp,
            "output_vyb": output_vyb,
            "messages": messages,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/tools/fts")
def build_fts_database(request: DatabasePathRequest) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []

    def progress(message: str, pct: int) -> None:
        messages.append({"message": message, "percent": pct})

    try:
        output_vyp, output_vyb = process_fts(request.db_path, progress)
        return {
            "output_vyp": output_vyp,
            "output_vyb": output_vyb,
            "messages": messages,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/tools/settings-repair")
def repair_settings_database(request: DatabasePathRequest) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []

    def progress(message: str, pct: int) -> None:
        messages.append({"message": message, "percent": pct})

    try:
        output_vyp, output_vyb, summary = process_settings_repair(request.db_path, progress)
        return {
            "output_vyp": output_vyp,
            "output_vyb": output_vyb,
            "messages": messages,
            "summary": _jsonable(summary),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/sql/validate")
def validate_database_path(request: DatabasePathRequest) -> dict[str, Any]:
    exists = os.path.exists(request.db_path)
    return {"exists": exists, "valid_sqlite": exists and is_valid_sqlite(request.db_path)}


@app.post("/api/sql/tables")
def list_tables(request: DatabasePathRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        views = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        return {
            "tables": [row["name"] for row in tables],
            "views": [row["name"] for row in views],
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/table-info")
def table_info(request: TableRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        columns = _table_columns(conn, request.table)
        foreign_keys = _foreign_keys(conn, request.table)
        indexes = conn.execute(f"PRAGMA index_list({_quote_identifier(request.table)})").fetchall()
        count = conn.execute(f"SELECT COUNT(*) AS c FROM {_quote_identifier(request.table)}").fetchone()["c"]
        return {
            "table": request.table,
            "row_count": count,
            "columns": columns,
            "foreign_keys": foreign_keys,
            "indexes": [_row_to_dict(row) for row in indexes],
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/checks")
def database_checks(request: DatabasePathRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        integrity = conn.execute("PRAGMA integrity_check").fetchall()
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        return {
            "user_version": version,
            "integrity": [_row_to_dict(row) for row in integrity],
            "foreign_keys": [_row_to_dict(row) for row in foreign_keys],
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/version")
def database_version(request: DatabasePathRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        return {"user_version": version}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/rows")
def table_rows(request: TableRequest, limit: int = 250, offset: int = 0) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        table = _quote_identifier(request.table)
        rows = conn.execute(f"SELECT * FROM {table} LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        return {
            "table": request.table,
            "columns": [desc[0] for desc in conn.execute(f"SELECT * FROM {table} LIMIT 0").description],
            "rows": [_row_to_dict(row) for row in rows],
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/row")
def get_row(request: RowRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        if not request.key:
            raise ValueError("Row key is required")
        where = " AND ".join(f"{_quote_identifier(col)} = ?" for col in request.key)
        values = list(request.key.values())
        sql = f"SELECT * FROM {_quote_identifier(request.table)} WHERE {where} LIMIT 1"
        row = conn.execute(sql, values).fetchone()
        return {"row": _row_to_dict(row) if row else None}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/related-rows")
def related_rows(request: RelatedRowRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        table = _quote_identifier(request.table)
        column = _quote_identifier(request.column)
        sql = (
            f"SELECT * FROM {table} "
            f"WHERE {column} = ? LIMIT ?"
        )
        rows = conn.execute(sql, (request.value, request.limit)).fetchall()
        if not rows and request.value is not None:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE CAST({column} AS TEXT) = CAST(? AS TEXT) LIMIT ?",
                (request.value, request.limit),
            ).fetchall()
        return {
            "table": request.table,
            "columns": [desc[0] for desc in conn.execute(f"SELECT * FROM {table} LIMIT 0").description],
            "rows": [_row_to_dict(row) for row in rows],
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/query")
def execute_query(request: QueryRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        sql = request.sql.strip()
        if not sql:
            raise ValueError("SQL query is empty")
        first_token = sql.split(None, 1)[0].lower()
        is_read = first_token in {"select", "pragma", "with", "explain"}
        if not is_read and not request.allow_write:
            raise ValueError("Write queries require allow_write=true")

        resolved, conn = _connect_resolved(request.db_path)
        cursor = conn.execute(sql)
        if cursor.description:
            rows = cursor.fetchmany(max(1, min(request.limit, 5000)))
            return {
                "columns": [desc[0] for desc in cursor.description],
                "rows": [_row_to_dict(row) for row in rows],
                "row_count": len(rows),
                "truncated_at": request.limit,
            }

        conn.commit()
        return {"columns": [], "rows": [], "row_count": cursor.rowcount}
    except Exception as exc:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()
