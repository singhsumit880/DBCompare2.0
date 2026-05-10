"""
core/settings_repair.py
Repair the kb_settings table: drop → recreate → reinsert, deduplicating as needed.
All progress reporting is done via progress_cb — never directly touches UI widgets.
"""
import os
import sqlite3
import shutil
import logging
from dataclasses import dataclass
from typing import Callable, Optional

from core.utils import get_temp_dir, unzip_vyb, zip_vyp, find_vyp_in_dir

ProgressCallback = Callable[[str, int], None]


@dataclass
class RepairSummary:
    original_count: int
    final_count: int
    inserted: int
    updated: int
    skipped: int
    duplicate_ids: int
    duplicate_keys: int


def repair_settings(
    input_db: str,
    output_db: str,
    progress_cb: ProgressCallback = None,
) -> RepairSummary:
    """
    Copy input_db → output_db, repair kb_settings in the copy.
    Returns a RepairSummary with counts.
    All status messages go via progress_cb; never touches Tkinter widgets.
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

        _emit("Checking kb_settings table…", 10)

        # Verify table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='kb_settings'"
        )
        if not cursor.fetchone():
            raise ValueError("kb_settings table does not exist in the database")

        # Verify expected columns
        cursor.execute("PRAGMA table_info(kb_settings)")
        columns = [row[1] for row in cursor.fetchall()]
        for required in ('setting_id', 'setting_key', 'setting_value'):
            if required not in columns:
                raise ValueError(
                    f"kb_settings is missing expected column: {required}"
                )

        # Export current data
        _emit("Exporting data…", 20)
        cursor.execute("SELECT setting_id, setting_key, setting_value FROM kb_settings")
        settings_data = cursor.fetchall()

        if not settings_data:
            _emit("kb_settings is empty — nothing to repair", 100)
            return RepairSummary(0, 0, 0, 0, 0, 0, 0)

        # Find duplicates (for reporting)
        seen_ids: set = set()
        seen_keys: set = set()
        dup_ids, dup_keys = 0, 0
        for sid, skey, _ in settings_data:
            if sid in seen_ids:
                dup_ids += 1
            else:
                seen_ids.add(sid)
            if skey in seen_keys:
                dup_keys += 1
            else:
                seen_keys.add(skey)

        if dup_ids:
            _emit(f"Found {dup_ids} duplicate setting_id(s)", 30)
        if dup_keys:
            _emit(f"Found {dup_keys} duplicate setting_key(s)", 35)

        # Drop and recreate
        _emit("Recreating kb_settings table…", 40)
        cursor.execute("DROP TABLE IF EXISTS kb_settings")
        cursor.execute("""
            CREATE TABLE kb_settings (
                setting_id    INTEGER PRIMARY KEY,
                setting_key   TEXT UNIQUE,
                setting_value TEXT
            )
        """)

        # Reinsert, handling duplicates
        _emit("Reinserting data…", 60)
        inserted = updated = skipped = 0
        for setting_id, setting_key, setting_value in settings_data:
            try:
                cursor.execute(
                    "INSERT INTO kb_settings (setting_id, setting_key, setting_value) VALUES (?,?,?)",
                    (setting_id, setting_key, setting_value),
                )
                inserted += 1
            except sqlite3.IntegrityError as e:
                err = str(e).lower()
                if 'setting_id' in err:
                    # Duplicate PK — let SQLite assign a new one
                    try:
                        cursor.execute(
                            "INSERT INTO kb_settings (setting_key, setting_value) VALUES (?,?)",
                            (setting_key, setting_value),
                        )
                        inserted += 1
                    except sqlite3.IntegrityError:
                        # Duplicate key as well — update
                        cursor.execute(
                            "UPDATE kb_settings SET setting_value=? WHERE setting_key=?",
                            (setting_value, setting_key),
                        )
                        updated += 1
                elif 'setting_key' in err:
                    cursor.execute(
                        "UPDATE kb_settings SET setting_value=? WHERE setting_key=?",
                        (setting_value, setting_key),
                    )
                    updated += 1
                else:
                    skipped += 1
                    logging.warning(f"Skipped row ({setting_id}, {setting_key}): {e}")

        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM kb_settings")
        final_count = cursor.fetchone()[0]
        _emit("Repair complete", 100)

        return RepairSummary(
            original_count=len(settings_data),
            final_count=final_count,
            inserted=inserted,
            updated=updated,
            skipped=skipped,
            duplicate_ids=dup_ids,
            duplicate_keys=dup_keys,
        )

    except (sqlite3.Error, ValueError):
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def process_settings_repair(
    input_file: str,
    progress_cb: ProgressCallback = None,
) -> tuple[str, str]:
    """
    Full settings repair pipeline: extract if .vyb, repair, repack.
    Returns (output_vyp_path, output_vyb_path).
    """
    def _emit(msg, pct):
        if progress_cb:
            progress_cb(msg, pct)

    temp_dir = get_temp_dir('temp_settings')
    input_filename = os.path.basename(input_file)
    from core.utils import safe_output_paths
    output_vyp, output_vyb = safe_output_paths(temp_dir, f"REPAIRED_{input_filename}")

    _emit("Preparing input file…", 5)
    if input_file.lower().endswith('.vyb'):
        unzip_vyb(input_file, temp_dir)
        vyp_file = find_vyp_in_dir(temp_dir)
        if not vyp_file:
            raise ValueError("No .vyp file found in the .vyb archive")
    else:
        vyp_file = input_file

    summary = repair_settings(vyp_file, output_vyp, progress_cb)

    _emit("Repacking to .vyb…", 90)
    zip_vyp(output_vyp, output_vyb)
    _emit("Done", 100)
    return output_vyp, output_vyb, summary
