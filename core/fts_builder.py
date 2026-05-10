"""
core/fts_builder.py
Rebuild the FTS3 full-text search table in a Vyapar database.
"""
import os
import sqlite3
import shutil
import logging
from typing import Callable, Optional

from core.utils import get_temp_dir, unzip_vyb, zip_vyp, find_vyp_in_dir

ProgressCallback = Callable[[str, int], None]

_FTS_DROP_TABLES = [
    'kb_fts_vtable',
    'kb_fts_vtable_content',
    'kb_fts_vtable_segdir',
    'kb_fts_vtable_segments',
    'kb_fts_vtable_config',
    'kb_fts_vtable_data',
    'kb_fts_vtable_docsize',
    'kb_fts_vtable_idx',
]

_EXPECTED_FTS_TABLES = {
    'kb_fts_vtable',
    'kb_fts_vtable_content',
    'kb_fts_vtable_segdir',
    'kb_fts_vtable_segments',
}

# Note: NO trailing comma before ) — fixed from original db.py
_CREATE_FTS = """
CREATE VIRTUAL TABLE kb_fts_vtable USING fts3(
    fts_name_id,
    fts_txn_id,
    fts_text
)
"""

_INSERT_FTS = """
INSERT INTO kb_fts_vtable(fts_name_id, fts_txn_id, fts_text)
SELECT
    t1.txn_name_id,
    t1.txn_id,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        COALESCE(t1.party_phone, '') || ' ' ||
        COALESCE(t1.party_name, '') || ' ' ||
        COALESCE(t1.category_name, '') || ' ' ||
        COALESCE(t1.display_name, '') || ' ' ||
        COALESCE(t1.txn_description, '') || ' ' ||
        COALESCE(t1.cash_amount, '') || ' ' ||
        COALESCE(t1.balance_amount, '') || ' ' ||
        COALESCE(t1.total_amount, '') || ' ' ||
        COALESCE(t1.prefix, '') || ' ' ||
        COALESCE(t1.invoice_number, '') || ' ' ||
        COALESCE(t1.prefix, '') || COALESCE(t1.invoice_number,'') || ' ' ||
        COALESCE(t1.pr,'') || ' ' ||
        COALESCE(t1.txn_eway_bill_number,'') || ' ' ||
        COALESCE(group_concat(l1.ldata, ' '), ''),
    '  ', ' '), '  ', ' '), '  ', ' '), '  ', ' '), '  ', ' ')
FROM (
    SELECT
        txn.*,
        group_concat(pm.payment_reference, ' ') pr
    FROM (
        SELECT
            t.txn_id,
            t.txn_name_id,
            t.txn_cash_amount          cash_amount,
            t.txn_balance_amount       balance_amount,
            t.txn_cash_amount + t.txn_balance_amount  total_amount,
            p.prefix_value             prefix,
            t.txn_ref_number_char      invoice_number,
            t.txn_description,
            t.txn_eway_bill_number,
            t.txn_display_name         display_name,
            n.full_name                party_name,
            n.phone_number             party_phone,
            c.full_name                category_name
        FROM kb_transactions t
        LEFT JOIN kb_names   n ON t.txn_name_id    = n.name_id
        LEFT JOIN kb_names   c ON t.txn_category_id= c.name_id
        LEFT JOIN kb_prefix  p ON t.txn_prefix_id  = p.prefix_id
    ) txn
    LEFT JOIN txn_payment_mapping pm ON txn.txn_id = pm.txn_id
    GROUP BY txn.txn_id
) t1
LEFT JOIN (
    SELECT
        li.lineitem_id,
        li.lineitem_txn_id,
        (COALESCE(i.item_name,'') || ' ' ||
         COALESCE(i.item_code,'') || ' ' ||
         COALESCE(i.item_hsn_sac_code,'') || ' ' ||
         COALESCE(li.lineitem_batch_number,'') || ' ' ||
         COALESCE(li.lineitem_serial_number,'') || ' ' ||
         COALESCE(li.lineitem_count,'') || ' ' ||
         COALESCE(li.lineitem_description,'') || ' ' ||
         COALESCE(li.sn,'')
        ) ldata
    FROM (
        SELECT
            l.lineitem_txn_id,
            l.lineitem_id,
            l.item_id,
            l.lineitem_batch_number,
            l.lineitem_serial_number,
            l.lineitem_count,
            l.lineitem_description,
            group_concat(sd.serial_number, ' ') sn
        FROM kb_lineitems l
        LEFT JOIN kb_serial_mapping sm ON l.lineitem_id   = sm.serial_mapping_lineitem_id
        LEFT JOIN kb_serial_details sd ON sm.serial_mapping_serial_id = sd.serial_id
        GROUP BY l.lineitem_id
    ) li
    LEFT JOIN kb_items i ON li.item_id = i.item_id
) l1 ON t1.txn_id = l1.lineitem_txn_id
GROUP BY t1.txn_id
"""


def build_fts_table(
    input_db: str,
    output_db: str,
    progress_cb: ProgressCallback = None,
) -> int:
    """
    Copy input_db → output_db, drop old FTS tables, rebuild FTS3 table.
    Returns the number of FTS records inserted.
    """
    def _emit(msg, pct):
        if progress_cb:
            progress_cb(msg, pct)

    shutil.copy(input_db, output_db)
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(output_db)
        conn.execute("PRAGMA foreign_keys = ON;")

        _emit("Dropping old FTS tables…", 10)
        for tbl in _FTS_DROP_TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {tbl};")

        _emit("Creating FTS3 table…", 30)
        conn.execute(_CREATE_FTS)

        _emit("Populating FTS index…", 50)
        conn.execute(_INSERT_FTS)
        conn.commit()

        # Remove any unexpected FTS-related tables
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kb_fts_vtable%'"
        )
        actual = {r[0] for r in cur.fetchall()}
        unexpected = actual - _EXPECTED_FTS_TABLES
        for tbl in unexpected:
            conn.execute(f"DROP TABLE IF EXISTS {tbl};")
        if unexpected:
            conn.commit()
            _emit(f"Removed unexpected FTS tables: {', '.join(unexpected)}", 90)

        count = conn.execute("SELECT COUNT(*) FROM kb_fts_vtable").fetchone()[0]
        _emit(f"FTS table created with {count} records", 100)
        return count

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logging.error(f"FTS build error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def process_fts(
    input_file: str,
    progress_cb: ProgressCallback = None,
) -> tuple[str, str]:
    """
    Full FTS pipeline: extract if .vyb, build FTS, repack.
    Returns (output_vyp_path, output_vyb_path).
    """
    def _emit(msg, pct):
        if progress_cb:
            progress_cb(msg, pct)

    temp_dir = get_temp_dir('temp_fts')
    input_filename = os.path.basename(input_file)
    from core.utils import safe_output_paths
    output_vyp, output_vyb = safe_output_paths(temp_dir, f"FTS_{input_filename}")

    _emit("Preparing input file…", 5)
    if input_file.lower().endswith('.vyb'):
        unzip_vyb(input_file, temp_dir)
        vyp_file = find_vyp_in_dir(temp_dir)
        if not vyp_file:
            raise ValueError("No .vyp file found in the .vyb archive")
    else:
        vyp_file = input_file

    build_fts_table(vyp_file, output_vyp, progress_cb)

    _emit("Repacking to .vyb…", 90)
    zip_vyp(output_vyp, output_vyb)
    _emit("Done", 100)
    return output_vyp, output_vyb
