"""
FastAPI bridge for the modern DBCompare UI.

This layer intentionally wraps the existing core modules instead of
reimplementing database behavior in the frontend.
"""
from __future__ import annotations

import os
import sqlite3
import asyncio
import threading
import time
import uuid
from dataclasses import asdict, is_dataclass
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
from core.utils import cleanup_dir


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


class RowRequest(TableRequest):
    key: dict[str, Any]


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
