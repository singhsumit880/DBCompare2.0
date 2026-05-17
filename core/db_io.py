"""
core/db_io.py
Database file I/O: validation, extraction, connection helpers.
"""
import os
import sqlite3
import zipfile
import logging
import shutil
import uuid
from typing import List, Optional

from core.utils import unzip_vyb, find_vyp_in_dir, get_temp_dir, cleanup_dir

SUPPORTED_EXTENSIONS = {'.vyp', '.zip', '.vyb', '.sqlite', '.sqlite3', '.db'}


def is_valid_sqlite(db_path: str) -> bool:
    """Return True if the file is a valid, non-empty SQLite database."""
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        conn.close()
        return len(tables) > 0
    except Exception:
        return False


def extract_vyp_from_vyb(vyb_path: str, temp_dirs: List[str]) -> str:
    """Extract the .vyp file from a .vyb (ZIP) archive. Returns extracted path."""
    try:
        with zipfile.ZipFile(vyb_path, 'r') as zf:
            vyp_files = [f for f in zf.namelist() if f.lower().endswith('.vyp')]
            if not vyp_files:
                raise ValueError("No .vyp file found in the .vyb archive")

            temp_dir = get_temp_dir(f'_extract_tmp_{uuid.uuid4().hex}')
            temp_dirs.append(temp_dir)
            vyp_name = vyp_files[0]
            extracted_path = os.path.join(temp_dir, os.path.basename(vyp_name))

            with open(extracted_path, 'wb') as f:
                f.write(zf.read(vyp_name))

            if not is_valid_sqlite(extracted_path):
                raise ValueError("Extracted .vyp is not a valid SQLite database")

            return extracted_path

    except zipfile.BadZipFile:
        raise ValueError("Invalid .vyb archive (bad zip)")
    except Exception as e:
        raise ValueError(f"Extraction error: {e}")


def extract_database_file(file_path: str, temp_dirs: List[str]) -> str:
    """
    Resolve any archive or database file to a plain .sqlite path.
    Supports .vyp, .sqlite, .db (direct), .vyb and .zip (extract first).
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext in ('.vyp', '.sqlite', '.sqlite3', '.db'):
        return file_path

    if ext == '.vyb':
        extracted = extract_vyp_from_vyb(file_path, temp_dirs)
        if extracted:
            return extracted
        raise ValueError(f"Failed to extract .vyp from {file_path}")

    if ext == '.zip':
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                for info in zf.infolist():
                    if any(info.filename.lower().endswith(e) for e in ('.sqlite', '.db')):
                        temp_dir = get_temp_dir(f'_zip_extract_tmp_{uuid.uuid4().hex}')
                        temp_dirs.append(temp_dir)
                        dest = os.path.join(temp_dir, os.path.basename(info.filename))
                        with open(dest, 'wb') as f:
                            f.write(zf.read(info.filename))
                        if is_valid_sqlite(dest):
                            return dest
                        os.remove(dest)
            raise ValueError("No valid SQLite database found in ZIP")
        except zipfile.BadZipFile:
            raise ValueError("Invalid ZIP archive")
        except Exception as e:
            raise ValueError(f"ZIP extraction error: {e}")

    raise ValueError(f"Unsupported file format: {ext}")


def validate_database(db_path: str) -> bool:
    """Run SQLite integrity and foreign-key checks. Returns True if healthy."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA integrity_check;")
        result = cursor.fetchone()
        if not result or result[0] != 'ok':
            logging.error(f"Integrity check failed for {db_path}: {result}")
            return False
        cursor.execute("PRAGMA foreign_key_check;")
        violations = cursor.fetchall()
        if violations:
            logging.error(f"Foreign key violations in {db_path}: {violations[:5]}")
            return False
        return True
    except sqlite3.Error as e:
        logging.error(f"Database validation error for {db_path}: {e}")
        return False
    finally:
        if conn:
            conn.close()


def open_connection(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with foreign keys enabled."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn
