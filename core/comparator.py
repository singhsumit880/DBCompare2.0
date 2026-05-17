"""
core/comparator.py
Pure diff engine — no UI dependencies.
Returns structured dataclass objects; the UI builds its own display from them.
"""
import sqlite3
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Callable, Tuple, Any

from core.db_io import (
    extract_database_file, validate_database, open_connection, is_valid_sqlite
)
from core.utils import cleanup_dir

DEFAULT_EXCLUDED_TABLES: Set[str] = {
    'sqlite_sequence', 'kb_fts_vtable', 'kb_fts_vtable_content',
    'kb_fts_vtable_segdir', 'kb_images', 'kb_item_images',
    'kb_fts_vtable_segments', 'kb_txn_message_config', 'kb_settings'
}
DEFAULT_IGNORED_TYPES: Set[str] = {"date", "datetime", "timestamp"}
DEFAULT_DECIMAL_PRECISION: int = 5

ProgressCallback = Callable[[str, Optional[int]], None]
CancelCheck = Callable[[], bool]


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ColumnDiff:
    table: str
    only_in_db1: List[str] = field(default_factory=list)
    only_in_db2: List[str] = field(default_factory=list)


@dataclass
class VersionResult:
    db1_version: int
    db2_version: int

    @property
    def differ(self) -> bool:
        return self.db1_version != self.db2_version


@dataclass
class SchemaResult:
    added_tables: List[str] = field(default_factory=list)       # in DB2 but not DB1
    removed_tables: List[str] = field(default_factory=list)     # in DB1 but not DB2
    column_diffs: List[ColumnDiff] = field(default_factory=list)

    @property
    def has_differences(self) -> bool:
        return bool(self.added_tables or self.removed_tables or self.column_diffs)


@dataclass
class ModifiedRow:
    pk: Dict[str, Any]
    column_changes: List[Tuple[str, Any, Any]]   # (col_name, db1_val, db2_val)
    db1_row: Dict[str, Any] = field(default_factory=dict)
    db2_row: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableDataResult:
    table: str
    col_names: List[str]                    # common columns used for comparison
    rows_only_in_db1: List[Dict[str, Any]] = field(default_factory=list)
    rows_only_in_db2: List[Dict[str, Any]] = field(default_factory=list)
    modified_rows: List[ModifiedRow] = field(default_factory=list)
    all_db1_rows: List[Dict[str, Any]] = field(default_factory=list)
    all_db2_rows: List[Dict[str, Any]] = field(default_factory=list)
    rows_only_in_db1_count: int = 0
    rows_only_in_db2_count: int = 0
    modified_rows_count: int = 0
    all_db1_rows_count: int = 0
    all_db2_rows_count: int = 0
    result_limited: bool = False
    # If schemas differ, this holds the column-level diff for banner display
    column_schema_diff: Optional['ColumnDiff'] = None

    @property
    def schema_differs(self) -> bool:
        return self.column_schema_diff is not None

    @property
    def has_differences(self) -> bool:
        return bool(self.rows_only_in_db1_count or self.rows_only_in_db2_count or self.modified_rows_count)


@dataclass
class ComparisonReport:
    version: VersionResult
    schema: SchemaResult
    data: List[TableDataResult] = field(default_factory=list)
    db1_label: str = "DB1"
    db2_label: str = "DB2"

    @property
    def has_any_differences(self) -> bool:
        return (
            self.version.differ
            or self.schema.has_differences
            or any(t.has_differences for t in self.data)
        )


# ---------------------------------------------------------------------------
# Comparator engine
# ---------------------------------------------------------------------------

class DatabaseComparator:
    def __init__(self):
        self.temp_dirs: List[str] = []
        self._db1_conn: Optional[sqlite3.Connection] = None
        self._db2_conn: Optional[sqlite3.Connection] = None
        self._progress: ProgressCallback = lambda msg, pct: None
        self.decimal_precision = DEFAULT_DECIMAL_PRECISION
        self.ignored_types: Set[str] = DEFAULT_IGNORED_TYPES
        self.max_result_rows_per_table = 500
        self._cancel_check: CancelCheck = lambda: False

    def set_progress(self, callback: ProgressCallback) -> None:
        self._progress = callback

    def _emit(self, msg: str, pct: Optional[int] = None) -> None:
        self._progress(msg, pct)

    def _check_cancelled(self) -> None:
        if self._cancel_check():
            raise InterruptedError("Comparison cancelled")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compare(
        self,
        db1_path: str,
        db2_path: str,
        included_tables: Set[str] = None,
        excluded_tables: Set[str] = None,
        ignore_datetime: bool = True,
        decimal_precision: int = DEFAULT_DECIMAL_PRECISION,
        validate_db: bool = True,
        max_result_rows_per_table: int = 500,
        cancel_check: Optional[CancelCheck] = None,
    ) -> ComparisonReport:

        self.decimal_precision = decimal_precision
        self.ignored_types = DEFAULT_IGNORED_TYPES if ignore_datetime else set()
        self.max_result_rows_per_table = max(0, max_result_rows_per_table)
        self._cancel_check = cancel_check or (lambda: False)
        included_tables = included_tables or set()
        excluded_tables = excluded_tables or set()

        try:
            self._check_cancelled()
            self._connect(db1_path, db2_path, validate_db)

            db1_tables = set(self._get_tables(self._db1_conn))
            db2_tables = set(self._get_tables(self._db2_conn))

            # Apply include filter
            if included_tables:
                self._assert_tables_exist(self._db1_conn, included_tables, "DB1")
                self._assert_tables_exist(self._db2_conn, included_tables, "DB2")
                db1_tables &= included_tables
                db2_tables &= included_tables

            # Apply exclude filter
            db1_tables -= excluded_tables
            db2_tables -= excluded_tables

            version = self._compare_versions()
            self._check_cancelled()
            schema  = self._compare_schemas(db1_tables, db2_tables)
            self._check_cancelled()
            data    = self._compare_data(db1_tables & db2_tables)

            return ComparisonReport(
                version=version,
                schema=schema,
                data=data,
                db1_label=db1_path,
                db2_label=db2_path,
            )

        finally:
            self._close()
            self._cleanup_temps()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self, db1_path: str, db2_path: str, validate_db: bool) -> None:
        self._close()
        p1 = extract_database_file(db1_path, self.temp_dirs)
        if validate_db and not validate_database(p1):
            raise ValueError(f"Validation failed for first database: {p1}")
        self._db1_conn = open_connection(p1)

        p2 = extract_database_file(db2_path, self.temp_dirs)
        if validate_db and not validate_database(p2):
            raise ValueError(f"Validation failed for second database: {p2}")
        self._db2_conn = open_connection(p2)

    def _close(self) -> None:
        if self._db1_conn:
            try:
                self._db1_conn.close()
            except Exception:
                pass
            self._db1_conn = None
        if self._db2_conn:
            try:
                self._db2_conn.close()
            except Exception:
                pass
            self._db2_conn = None

    def _cleanup_temps(self) -> None:
        for d in self.temp_dirs:
            cleanup_dir(d)
        self.temp_dirs = []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_tables(conn: sqlite3.Connection) -> List[str]:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [r[0] for r in cur.fetchall() if r[0] != 'sqlite_sequence']

    @staticmethod
    def _assert_tables_exist(conn: sqlite3.Connection, names: Set[str], label: str) -> None:
        existing = set(DatabaseComparator._get_tables(conn))
        missing = names - existing
        if missing:
            raise ValueError(f"Tables not found in {label}: {', '.join(sorted(missing))}")

    @staticmethod
    def _get_columns(conn: sqlite3.Connection, table: str) -> List[Dict]:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info(\"{table}\");")
        return [
            {'cid': r[0], 'name': r[1], 'type': r[2],
             'notnull': r[3], 'dflt_value': r[4], 'pk': r[5]}
            for r in cur.fetchall()
        ]

    @staticmethod
    def _get_user_version(conn: sqlite3.Connection) -> int:
        cur = conn.cursor()
        cur.execute("PRAGMA user_version;")
        return cur.fetchone()[0]

    @staticmethod
    def _quote(name: str) -> str:
        return f'"{name}"'

    # ------------------------------------------------------------------
    # Diff sub-routines
    # ------------------------------------------------------------------

    def _compare_versions(self) -> VersionResult:
        return VersionResult(
            db1_version=self._get_user_version(self._db1_conn),
            db2_version=self._get_user_version(self._db2_conn),
        )

    def _compare_schemas(
        self, db1_tables: Set[str], db2_tables: Set[str]
    ) -> SchemaResult:
        self._emit("Comparing schemas…", 10)
        result = SchemaResult(
            added_tables=sorted(db2_tables - db1_tables),
            removed_tables=sorted(db1_tables - db2_tables),
        )
        common = sorted(db1_tables & db2_tables)
        for i, table in enumerate(common):
            self._check_cancelled()
            self._emit(f"Schema: {table}", 10 + int(30 * i / max(len(common), 1)))
            c1_names = {c['name'] for c in self._get_columns(self._db1_conn, table)}
            c2_names = {c['name'] for c in self._get_columns(self._db2_conn, table)}
            only1 = sorted(c1_names - c2_names)
            only2 = sorted(c2_names - c1_names)
            if only1 or only2:
                result.column_diffs.append(ColumnDiff(table=table, only_in_db1=only1, only_in_db2=only2))
        self._emit("Schema comparison complete", 40)
        return result

    def _round(self, value: Any) -> Any:
        if isinstance(value, float):
            try:
                return round(value, self.decimal_precision)
            except Exception:
                pass
        return value

    def _values_equal(self, v1: Any, v2: Any) -> bool:
        if v1 == v2:
            return True
        if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
            return self._round(v1) == self._round(v2)
        return False

    def _compare_data(self, common_tables: Set[str]) -> List[TableDataResult]:
        self._emit("Comparing data…", 40)
        results: List[TableDataResult] = []

        tables = sorted(common_tables)
        for i, table in enumerate(tables):
            self._check_cancelled()
            self._emit(f"Data: {table}", 40 + int(55 * i / max(len(tables), 1)))

            cols1 = self._get_columns(self._db1_conn, table)
            cols2 = self._get_columns(self._db2_conn, table)

            col1_names = [c['name'] for c in cols1]
            col2_names = [c['name'] for c in cols2]
            col1_set   = set(col1_names)
            col2_set   = set(col2_names)

            # Detect schema differences between the two tables
            only_in_db1 = sorted(col1_set - col2_set)
            only_in_db2 = sorted(col2_set - col1_set)
            schema_diff = (
                ColumnDiff(table=table, only_in_db1=only_in_db1, only_in_db2=only_in_db2)
                if (only_in_db1 or only_in_db2) else None
            )

            # Compare on the intersection of columns that exist in both DBs
            common_cols = [c for c in col1_names if c in col2_set]

            # Build a lookup for column metadata using DB1 columns
            col1_meta = {c['name']: c for c in cols1}

            tdr = TableDataResult(
                table=table,
                col_names=common_cols,
                column_schema_diff=schema_diff,
            )

            if not common_cols:
                # No columns in common at all — nothing to compare
                results.append(tdr)
                continue

            # For PK we prefer columns that exist in both DBs
            pk_cols = [c['name'] for c in cols1 if c['pk'] and c['name'] in col2_set]
            if not pk_cols:
                pk_cols = common_cols[:1]

            order_by = ", ".join(f"CAST({self._quote(c)} AS TEXT)" for c in pk_cols)

            # Fetch only common columns from each DB
            cols_str = ", ".join(self._quote(c) for c in common_cols)
            query    = f'SELECT {cols_str} FROM {self._quote(table)} ORDER BY {order_by};'

            count_query = f"SELECT COUNT(*) FROM {self._quote(table)}"
            tdr.all_db1_rows_count = self._db1_conn.execute(count_query).fetchone()[0]
            tdr.all_db2_rows_count = self._db2_conn.execute(count_query).fetchone()[0]

            pk_indices = [common_cols.index(c) for c in pk_cols]

            def make_pk(row: tuple) -> Tuple[str, ...]:
                return tuple('' if row[idx] is None else str(row[idx]) for idx in pk_indices)

            def pk_for_result(pk: Tuple[str, ...]) -> Any:
                return pk if len(pk) > 1 else pk[0]

            def pk_dict_for_result(pk: Tuple[str, ...]) -> Dict[str, Any]:
                return dict(zip(pk_cols, pk))

            def rounded_row(row: tuple) -> Dict[str, Any]:
                return dict(zip(common_cols, (self._round(v) for v in row)))

            def record_db1_only(pk: Tuple[str, ...], row: tuple) -> None:
                tdr.rows_only_in_db1_count += 1
                if len(tdr.rows_only_in_db1) < self.max_result_rows_per_table:
                    tdr.rows_only_in_db1.append({'_pk': pk_for_result(pk), **rounded_row(row)})
                else:
                    tdr.result_limited = True

            def record_db2_only(pk: Tuple[str, ...], row: tuple) -> None:
                tdr.rows_only_in_db2_count += 1
                if len(tdr.rows_only_in_db2) < self.max_result_rows_per_table:
                    tdr.rows_only_in_db2.append({'_pk': pk_for_result(pk), **rounded_row(row)})
                else:
                    tdr.result_limited = True

            def record_if_modified(pk: Tuple[str, ...], row1: tuple, row2: tuple) -> None:
                changes: List[Tuple[str, Any, Any]] = []
                for idx, (v1, v2) in enumerate(zip(row1, row2)):
                    col_name = common_cols[idx]
                    col_meta = col1_meta.get(col_name, {})
                    col_type = col_meta.get('type', '').lower()
                    if any(ign in col_type for ign in self.ignored_types):
                        continue
                    if not self._values_equal(v1, v2):
                        changes.append((col_name, self._round(v1), self._round(v2)))
                if not changes:
                    return

                tdr.modified_rows_count += 1
                if len(tdr.modified_rows) < self.max_result_rows_per_table:
                    tdr.modified_rows.append(
                        ModifiedRow(
                            pk=pk_dict_for_result(pk),
                            column_changes=changes,
                            db1_row=rounded_row(row1),
                            db2_row=rounded_row(row2),
                        )
                    )
                else:
                    tdr.result_limited = True

            db1_cursor = self._db1_conn.execute(query)
            db2_cursor = self._db2_conn.execute(query)
            row1 = db1_cursor.fetchone()
            row2 = db2_cursor.fetchone()

            while row1 is not None or row2 is not None:
                if (tdr.modified_rows_count + tdr.rows_only_in_db1_count + tdr.rows_only_in_db2_count) % 1000 == 0:
                    self._check_cancelled()
                pk1 = make_pk(row1) if row1 is not None else None
                pk2 = make_pk(row2) if row2 is not None else None

                if row2 is None:
                    record_db1_only(pk1, row1)
                    row1 = db1_cursor.fetchone()

                elif row1 is None:
                    record_db2_only(pk2, row2)
                    row2 = db2_cursor.fetchone()

                elif pk1 < pk2:
                    record_db1_only(pk1, row1)
                    row1 = db1_cursor.fetchone()

                elif pk2 < pk1:
                    record_db2_only(pk2, row2)
                    row2 = db2_cursor.fetchone()

                else:
                    record_if_modified(pk1, row1, row2)
                    row1 = db1_cursor.fetchone()
                    row2 = db2_cursor.fetchone()

            results.append(tdr)

        self._emit("Data comparison complete", 95)
        return results
