"""
core/sanitizer.py
Database sanitization: execute SQL queries on a copy, repack to .vyb.
"""
import os
import sqlite3
import shutil
import logging
from typing import List, Callable, Optional

from core.utils import (
    get_temp_dir, unzip_vyb, zip_vyp, find_vyp_in_dir, safe_output_paths
)

ProgressCallback = Callable[[str, int], None]


def execute_queries(
    input_db: str,
    output_db: str,
    queries: List[str],
    progress_cb: ProgressCallback = None,
) -> None:
    """
    Copy input_db → output_db, then execute each non-empty SQL query.
    Rolls back and re-raises on error.
    """
    def _emit(msg, pct):
        if progress_cb:
            progress_cb(msg, pct)

    shutil.copy(input_db, output_db)
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(output_db)
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        for i, query in enumerate(queries):
            q = query.strip()
            if q:
                cursor.execute(q)
                _emit(f"Executed: {q[:60]}…" if len(q) > 60 else f"Executed: {q}", 0)
        conn.commit()
        _emit("All queries executed successfully", 100)
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logging.error(f"SQL execution error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def process_sanitization(
    input_file: str,
    queries: List[str],
    progress_cb: ProgressCallback = None,
) -> tuple[str, str]:
    """
    Full sanitization pipeline:
      1. If .vyb → extract .vyp
      2. Execute SQL queries on a copy
      3. Repack to .vyb
    Returns (output_vyp_path, output_vyb_path).
    """
    def _emit(msg, pct):
        if progress_cb:
            progress_cb(msg, pct)

    temp_dir = get_temp_dir('temp_sanitize')
    input_filename = os.path.basename(input_file)
    output_filename = f"Sanitized_{input_filename}"
    # Use splitext for safe extension handling
    output_vyp, output_vyb = safe_output_paths(temp_dir, output_filename)

    _emit("Preparing input file…", 10)

    if input_file.lower().endswith('.vyb'):
        unzip_vyb(input_file, temp_dir)
        vyp_file = find_vyp_in_dir(temp_dir)
        if not vyp_file:
            raise ValueError("No .vyp file found in the .vyb archive")
    else:
        vyp_file = input_file

    _emit("Running SQL queries…", 30)
    execute_queries(vyp_file, output_vyp, queries)

    _emit("Repacking to .vyb…", 70)
    zip_vyp(output_vyp, output_vyb)

    _emit("Done", 100)
    return output_vyp, output_vyb


def convert_file(
    input_file: str,
    progress_cb: ProgressCallback = None,
) -> tuple[str, str]:
    """
    Convert / repack:
      - .vyb → extract .vyp, repack freshly to .vyb
      - .vyp → pack to .vyb
    Returns (output_vyp_path, output_vyb_path).
    """
    from datetime import datetime

    def _emit(msg, pct):
        if progress_cb:
            progress_cb(msg, pct)

    temp_dir = get_temp_dir('temp_sanitize')
    input_filename = os.path.basename(input_file)
    base = os.path.splitext(input_filename)[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = os.path.splitext(input_file)[1].lower()

    if ext == '.vyb':
        _emit("Extracting .vyb…", 10)
        unzip_vyb(input_file, temp_dir)
        vyp_file = find_vyp_in_dir(temp_dir)
        if not vyp_file:
            raise ValueError("No .vyp file found in the .vyb archive")
        _emit("Repacking…", 50)
        out_vyp = os.path.join(temp_dir, f"converted_{base}_{timestamp}.vyp")
        out_vyb = os.path.join(temp_dir, f"converted_{base}_{timestamp}.vyb")
        shutil.copy(vyp_file, out_vyp)
        zip_vyp(out_vyp, out_vyb)
    elif ext == '.vyp':
        _emit("Packing .vyp → .vyb…", 30)
        out_vyp = os.path.join(temp_dir, f"converted_{base}_{timestamp}.vyp")
        out_vyb = os.path.join(temp_dir, f"converted_{base}_{timestamp}.vyb")
        shutil.copy(input_file, out_vyp)
        zip_vyp(out_vyp, out_vyb)
    else:
        raise ValueError(f"Unsupported extension: {ext}")

    _emit("Done", 100)
    return out_vyp, out_vyb
