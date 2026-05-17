"""
FastAPI bridge for the modern DBCompare UI.

This layer intentionally wraps the existing core modules instead of
reimplementing database behavior in the frontend.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import asyncio
import csv
import json
import zipfile
import threading
import time
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request
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
from core.utils import cleanup_dir, get_temp_dir, zip_vyp


app = FastAPI(title="DBCompare 2.0 API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "null"],
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
    max_result_rows_per_table: int = 500


class DatabasePathRequest(BaseModel):
    db_path: str


class TableRequest(DatabasePathRequest):
    table: str


class QueryRequest(DatabasePathRequest):
    sql: str
    limit: int = 500
    allow_write: bool = False


class QueryCompareRequest(BaseModel):
    db_path: str
    left_sql: str
    right_sql: str
    limit: int = 1000


class ExportRequest(DatabasePathRequest):
    source: str = "table"
    format: str = "csv"
    table: str | None = None
    sql: str | None = None
    limit: int = 10000


class RowRequest(TableRequest):
    key: dict[str, Any]


class RowUpdateRequest(RowRequest):
    values: dict[str, Any]


class BatchRowEdit(BaseModel):
    table: str
    key: dict[str, Any]
    values: dict[str, Any]


class BatchRowUpdateRequest(DatabasePathRequest):
    edits: list[BatchRowEdit] = Field(default_factory=list)


class RelatedRowRequest(TableRequest):
    column: str
    value: Any
    limit: int = 250


class SanitizeRequest(DatabasePathRequest):
    queries: list[str] = Field(default_factory=list)


compare_jobs: dict[str, dict[str, Any]] = {}
compare_jobs_lock = threading.Lock()
COMPARE_JOB_TTL_SECONDS = 60 * 60


def _run_compare(request: CompareRequest, cancel_event: threading.Event, progress: Any | None = None) -> dict[str, Any]:
    comparator = DatabaseComparator()
    if progress:
        comparator.set_progress(progress)
    report = comparator.compare(
        request.db1_path,
        request.db2_path,
        included_tables=set(request.included_tables),
        excluded_tables=set(request.excluded_tables),
        ignore_datetime=request.ignore_datetime,
        decimal_precision=request.decimal_precision,
        validate_db=request.validate_db,
        max_result_rows_per_table=request.max_result_rows_per_table,
        cancel_check=cancel_event.is_set,
    )
    return _jsonable(report)


def _cleanup_compare_jobs() -> None:
    cutoff = time.time() - COMPARE_JOB_TTL_SECONDS
    with compare_jobs_lock:
        stale_ids = [
            job_id
            for job_id, job in compare_jobs.items()
            if job.get("finished_at") and job["finished_at"] < cutoff
        ]
        for job_id in stale_ids:
            compare_jobs.pop(job_id, None)


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


def _apply_row_update(db_file: str, table_name: str, key: dict[str, Any], values: dict[str, Any]) -> int:
    if not key:
        raise ValueError("Row key is required")
    if not values:
        raise ValueError("No values were changed")

    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        table_columns = {column["name"] for column in _table_columns(conn, table_name)}
        invalid_key_columns = [column for column in key if column not in table_columns]
        invalid_value_columns = [column for column in values if column not in table_columns]
        if invalid_key_columns or invalid_value_columns:
            invalid = ", ".join(invalid_key_columns + invalid_value_columns)
            raise ValueError(f"Unknown column(s): {invalid}")

        set_clause = ", ".join(f"{_quote_identifier(column)} = ?" for column in values)
        where_parts: list[str] = []
        where_values: list[Any] = []
        for column, value in key.items():
            if value is None:
                where_parts.append(f"{_quote_identifier(column)} IS NULL")
            else:
                where_parts.append(f"{_quote_identifier(column)} = ?")
                where_values.append(value)
        where_clause = " AND ".join(where_parts)
        match_count = conn.execute(
            f"SELECT COUNT(*) AS c FROM {_quote_identifier(table_name)} WHERE {where_clause}",
            where_values,
        ).fetchone()["c"]
        if match_count != 1:
            raise ValueError(f"Expected row key to match 1 row, matched {match_count}")

        sql = f"UPDATE {_quote_identifier(table_name)} SET {set_clause} WHERE {where_clause}"
        cursor = conn.execute(sql, [*values.values(), *where_values])
        conn.commit()
        return cursor.rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _run_sql_query(db_path: str, sql: str, limit: int = 500, allow_write: bool = False) -> dict[str, Any]:
    resolved = conn = None
    try:
        clean_sql = sql.strip()
        if not clean_sql:
            raise ValueError("SQL query is empty")
        first_token = clean_sql.split(None, 1)[0].lower()
        is_read = first_token in {"select", "pragma", "with", "explain"}
        if not is_read and not allow_write:
            raise ValueError("Write queries require allow_write=true")

        resolved, conn = _connect_resolved(db_path)
        started = time.perf_counter()
        cursor = conn.execute(clean_sql)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        if cursor.description:
            capped_limit = max(1, min(limit, 10000))
            rows = cursor.fetchmany(capped_limit + 1)
            is_truncated = len(rows) > capped_limit
            visible_rows = rows[:capped_limit]
            return {
                "columns": [desc[0] for desc in cursor.description],
                "rows": [_row_to_dict(row) for row in visible_rows],
                "row_count": len(visible_rows),
                "truncated": is_truncated,
                "truncated_at": capped_limit if is_truncated else None,
                "elapsed_ms": elapsed_ms,
                "affected_rows": cursor.rowcount if cursor.rowcount != -1 else 0,
            }

        conn.commit()
        return {
            "columns": [],
            "rows": [],
            "row_count": cursor.rowcount,
            "truncated": False,
            "elapsed_ms": elapsed_ms,
            "affected_rows": cursor.rowcount,
        }
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


def _schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    table_rows = conn.execute(
        "SELECT name, type, sql FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY type, name"
    ).fetchall()
    tables: list[dict[str, Any]] = []
    for row in table_rows:
        name = row["name"]
        is_table = row["type"] == "table"
        count = None
        if is_table and not name.startswith("sqlite_"):
            try:
                count = conn.execute(f"SELECT COUNT(*) AS c FROM {_quote_identifier(name)}").fetchone()["c"]
            except sqlite3.Error:
                count = None
        tables.append({
            "name": name,
            "type": row["type"],
            "sql": row["sql"],
            "row_count": count,
            "columns": _table_columns(conn, name),
            "foreign_keys": _foreign_keys(conn, name) if is_table else [],
            "indexes": [_row_to_dict(index) for index in conn.execute(f"PRAGMA index_list({_quote_identifier(name)})").fetchall()] if is_table else [],
        })
    return {"tables": tables}


def _write_rows_export(path: str, columns: list[str], rows: list[dict[str, Any]], export_format: str, table_name: str = "export") -> None:
    if export_format == "sql":
        with open(path, "w", encoding="utf-8", newline="") as handle:
            quoted_table = _quote_identifier(table_name)
            for row in rows:
                quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
                values = []
                for column in columns:
                    value = row.get(column)
                    if value is None:
                        values.append("NULL")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    else:
                        values.append("'" + str(value).replace("'", "''") + "'")
                handle.write(f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({', '.join(values)});\n")
        return

    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _export_database_csv_zip(conn: sqlite3.Connection, path: str, tables: list[str], limit: int) -> int:
    row_total = 0
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for table_name in tables:
            cursor = conn.execute(f"SELECT * FROM {_quote_identifier(table_name)} LIMIT ?", (limit,))
            columns = [desc[0] for desc in cursor.description]
            rows = [_row_to_dict(row) for row in cursor.fetchall()]
            row_total += len(rows)
            csv_lines: list[str] = []
            output = csv_lines.append
            output(",".join(columns))
            for row in rows:
                output(",".join('"' + str(row.get(column, "")).replace('"', '""') + '"' for column in columns))
            archive.writestr(f"{table_name}.csv", "\n".join(csv_lines))
    return row_total


def _export_database_sql(conn: sqlite3.Connection, path: str, tables: list[str], include_data: bool, limit: int) -> int:
    row_total = 0
    with open(path, "w", encoding="utf-8", newline="") as handle:
        schema_rows = conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'view', 'index', 'trigger') AND sql IS NOT NULL ORDER BY type, name"
        ).fetchall()
        for row in schema_rows:
            handle.write(f"{row['sql']};\n\n")
        if not include_data:
            return 0
        for table_name in tables:
            cursor = conn.execute(f"SELECT * FROM {_quote_identifier(table_name)} LIMIT ?", (limit,))
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                row_total += 1
                quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
                values = []
                for column in columns:
                    value = row[column]
                    if value is None:
                        values.append("NULL")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    else:
                        values.append("'" + str(value).replace("'", "''") + "'")
                handle.write(f"INSERT INTO {_quote_identifier(table_name)} ({quoted_columns}) VALUES ({', '.join(values)});\n")
            handle.write("\n")
    return row_total


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/compare")
async def compare_databases(request: CompareRequest, http_request: Request) -> dict[str, Any]:
    cancel_event = threading.Event()

    async def watch_disconnect() -> None:
        while not cancel_event.is_set():
            if await http_request.is_disconnected():
                cancel_event.set()
                return
            await asyncio.sleep(0.25)

    watcher = asyncio.create_task(watch_disconnect())
    try:
        return await asyncio.to_thread(_run_compare, request, cancel_event)
    except InterruptedError as exc:
        raise HTTPException(status_code=499, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        cancel_event.set()
        watcher.cancel()


@app.post("/api/compare/jobs")
def start_compare_job(request: CompareRequest) -> dict[str, str]:
    _cleanup_compare_jobs()
    job_id = uuid.uuid4().hex
    cancel_event = threading.Event()
    job = {
        "id": job_id,
        "status": "queued",
        "message": "Queued",
        "percent": 0,
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
        "cancel_event": cancel_event,
    }

    def update_progress(message: str, percent: int | None = None) -> None:
        with compare_jobs_lock:
            current = compare_jobs.get(job_id)
            if not current:
                return
            current["message"] = message
            if percent is not None:
                current["percent"] = max(0, min(100, int(percent)))

    def worker() -> None:
        with compare_jobs_lock:
            job["status"] = "running"
            job["started_at"] = time.time()
            job["message"] = "Starting comparison"
            job["percent"] = 1
        try:
            result = _run_compare(request, cancel_event, update_progress)
            with compare_jobs_lock:
                job["status"] = "completed"
                job["message"] = "Comparison complete"
                job["percent"] = 100
                job["result"] = result
                job["finished_at"] = time.time()
        except InterruptedError:
            with compare_jobs_lock:
                job["status"] = "cancelled"
                job["message"] = "Comparison cancelled"
                job["finished_at"] = time.time()
        except Exception as exc:
            with compare_jobs_lock:
                job["status"] = "failed"
                job["message"] = "Comparison failed"
                job["error"] = str(exc)
                job["finished_at"] = time.time()

    with compare_jobs_lock:
        compare_jobs[job_id] = job

    thread = threading.Thread(target=worker, name=f"compare-{job_id}", daemon=True)
    thread.start()
    return {"job_id": job_id}


def _public_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job["id"],
        "status": job["status"],
        "message": job["message"],
        "percent": job["percent"],
        "created_at": job["created_at"],
        "started_at": job["started_at"],
        "finished_at": job["finished_at"],
        "error": job["error"],
        "has_result": job["result"] is not None,
    }


@app.get("/api/compare/jobs/{job_id}")
def get_compare_job(job_id: str) -> dict[str, Any]:
    with compare_jobs_lock:
        job = compare_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Compare job not found")
        return _public_job(job)


@app.get("/api/compare/jobs/{job_id}/result")
def get_compare_job_result(job_id: str) -> dict[str, Any]:
    with compare_jobs_lock:
        job = compare_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Compare job not found")
        if job["status"] != "completed":
            raise HTTPException(status_code=409, detail=f"Compare job is {job['status']}")
        return job["result"]


@app.post("/api/compare/jobs/{job_id}/cancel")
def cancel_compare_job(job_id: str) -> dict[str, Any]:
    with compare_jobs_lock:
        job = compare_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Compare job not found")
        job["cancel_event"].set()
        if job["status"] in {"queued", "running"}:
            job["status"] = "cancelling"
            job["message"] = "Cancelling comparison"
        return _public_job(job)


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


@app.post("/api/sql/schema")
def database_schema(request: DatabasePathRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        resolved, conn = _connect_resolved(request.db_path)
        snapshot = _schema_snapshot(conn)
        snapshot["user_version"] = conn.execute("PRAGMA user_version").fetchone()[0]
        snapshot["page_count"] = conn.execute("PRAGMA page_count").fetchone()[0]
        snapshot["page_size"] = conn.execute("PRAGMA page_size").fetchone()[0]
        return snapshot
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
        count = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        rows = conn.execute(f"SELECT * FROM {table} LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        return {
            "table": request.table,
            "columns": [desc[0] for desc in conn.execute(f"SELECT * FROM {table} LIMIT 0").description],
            "rows": [_row_to_dict(row) for row in rows],
            "row_count": count,
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


@app.post("/api/sql/update-row")
def update_row(request: RowUpdateRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        source_ext = os.path.splitext(request.db_path)[1].lower()
        resolved = ResolvedDatabase(request.db_path)
        updated_count = _apply_row_update(resolved.path, request.table, request.key, request.values)
        if updated_count != 1:
            raise ValueError(f"Expected to update 1 row, updated {updated_count}")

        if source_ext == ".vyb":
            zip_vyp(resolved.path, request.db_path)

        conn = sqlite3.connect(resolved.path)
        conn.row_factory = sqlite3.Row
        where_parts: list[str] = []
        where_values: list[Any] = []
        lookup_key = {**request.key, **request.values}
        for column, value in lookup_key.items():
            if value is None:
                where_parts.append(f"{_quote_identifier(column)} IS NULL")
            else:
                where_parts.append(f"{_quote_identifier(column)} = ?")
                where_values.append(value)
        row = conn.execute(
            f"SELECT * FROM {_quote_identifier(request.table)} WHERE {' AND '.join(where_parts)} LIMIT 1",
            where_values,
        ).fetchone()

        return {
            "updated_count": updated_count,
            "row": _row_to_dict(row) if row else None,
            "mode": "direct",
            "output_vyp": None,
            "output_vyb": None,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()


@app.post("/api/sql/update-rows-batch")
def update_rows_batch(request: BatchRowUpdateRequest) -> dict[str, Any]:
    if not request.edits:
        raise HTTPException(status_code=400, detail="No edits to save")

    resolved = None
    try:
        source_ext = os.path.splitext(request.db_path)[1].lower()
        temp_dir = get_temp_dir("temp_export")
        base = os.path.splitext(os.path.basename(request.db_path))[0] or "database"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        resolved = ResolvedDatabase(request.db_path)

        if source_ext == ".vyb":
            working_path = os.path.join(temp_dir, f"{base}_edited_{timestamp}.vyp")
            output_path = os.path.join(temp_dir, f"{base}_edited_{timestamp}.vyb")
            shutil.copy(resolved.path, working_path)
        else:
            extension = source_ext.replace(".", "") or "db"
            output_path = os.path.join(temp_dir, f"{base}_edited_{timestamp}.{extension}")
            working_path = output_path
            shutil.copy(resolved.path, working_path)

        total_updated = 0
        for edit in request.edits:
            if not edit.values:
                continue
            updated_count = _apply_row_update(working_path, edit.table, edit.key, edit.values)
            if updated_count != 1:
                raise ValueError(f"Expected to update 1 row in {edit.table}, updated {updated_count}")
            total_updated += updated_count

        if source_ext == ".vyb":
            zip_vyp(working_path, output_path)

        return {
            "path": output_path,
            "format": "vyb" if source_ext == ".vyb" else (source_ext.replace(".", "") or "db"),
            "updated_count": total_updated,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
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
    try:
        return _run_sql_query(request.db_path, request.sql, request.limit, request.allow_write)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/sql/compare-query")
def compare_query(request: QueryCompareRequest) -> dict[str, Any]:
    try:
        left = _run_sql_query(request.db_path, request.left_sql, request.limit, False)
        right = _run_sql_query(request.db_path, request.right_sql, request.limit, False)
        left_signatures = {json.dumps(row, sort_keys=True, default=str): row for row in left["rows"]}
        right_signatures = {json.dumps(row, sort_keys=True, default=str): row for row in right["rows"]}
        only_left = [left_signatures[key] for key in sorted(left_signatures.keys() - right_signatures.keys())]
        only_right = [right_signatures[key] for key in sorted(right_signatures.keys() - left_signatures.keys())]
        common = len(set(left_signatures) & set(right_signatures))
        return {
            "columns": sorted(set(left["columns"]) | set(right["columns"])),
            "left": left,
            "right": right,
            "only_in_db1": only_left[:request.limit],
            "only_in_db2": only_right[:request.limit],
            "common_count": common,
            "match": not only_left and not only_right and left["columns"] == right["columns"],
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/sql/export")
def export_sql_data(request: ExportRequest) -> dict[str, Any]:
    resolved = conn = None
    try:
        export_format = request.format.lower()
        source = request.source.lower()
        if export_format not in {"csv", "sql", "sqlite", "vyp", "vyb"}:
            raise ValueError("Unsupported export format")
        temp_dir = get_temp_dir("temp_export")
        base = os.path.splitext(os.path.basename(request.db_path))[0] or "database"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if source == "database":
            resolved = ResolvedDatabase(request.db_path)
            if export_format in {"csv", "sql"}:
                conn = sqlite3.connect(resolved.path)
                conn.row_factory = sqlite3.Row
                tables = [
                    row["name"]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                    ).fetchall()
                ]
                if export_format == "csv":
                    output_path = os.path.join(temp_dir, f"{base}_csv_{timestamp}.zip")
                    row_count = _export_database_csv_zip(conn, output_path, tables, max(1, min(request.limit, 100000)))
                else:
                    output_path = os.path.join(temp_dir, f"{base}_data_{timestamp}.sql")
                    row_count = _export_database_sql(conn, output_path, tables, True, max(1, min(request.limit, 100000)))
                return {"path": output_path, "format": export_format, "row_count": row_count}
            extension = "db" if export_format == "sqlite" else export_format
            output_path = os.path.join(temp_dir, f"{base}_{timestamp}.{extension}")
            if export_format == "vyb":
                zip_vyp(resolved.path, output_path)
            elif export_format == "vyp":
                shutil.copy(resolved.path, output_path)
            elif export_format == "sqlite":
                shutil.copy(resolved.path, output_path)
            else:
                raise ValueError("Database export supports sqlite, vyp, or vyb")
            return {"path": output_path, "format": export_format, "row_count": None}

        resolved, conn = _connect_resolved(request.db_path)
        if source == "schema":
            output_path = os.path.join(temp_dir, f"{base}_schema_{timestamp}.sql")
            tables = [
                row["name"]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
            ]
            _export_database_sql(conn, output_path, tables, False, 0)
            return {"path": output_path, "format": "sql", "row_count": 0}

        if source == "table":
            if not request.table:
                raise ValueError("Table export requires a table name")
            table = _quote_identifier(request.table)
            cursor = conn.execute(f"SELECT * FROM {table} LIMIT ?", (max(1, min(request.limit, 100000)),))
            table_name = request.table
        elif source == "query":
            if not request.sql:
                raise ValueError("Query export requires SQL")
            cursor = conn.execute(request.sql)
            table_name = "query_export"
        else:
            raise ValueError("Unsupported export source")

        if not cursor.description:
            raise ValueError("Export query did not return rows")
        columns = [desc[0] for desc in cursor.description]
        capped_limit = max(1, min(request.limit, 100000))
        rows = [_row_to_dict(row) for row in cursor.fetchmany(capped_limit)]
        extension = "csv" if export_format == "csv" else export_format
        output_path = os.path.join(temp_dir, f"{base}_{source}_{timestamp}.{extension}")
        _write_rows_export(output_path, columns, rows, export_format, table_name)
        return {"path": output_path, "format": export_format, "row_count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if conn:
            conn.close()
        if resolved:
            resolved.close()
