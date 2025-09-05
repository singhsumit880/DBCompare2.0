import os
import sys
import sqlite3
import zipfile
import tempfile
import logging
import threading
import shutil
import atexit
import queue
from typing import List, Dict, Tuple, Optional, Set
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext, font, simpledialog
import ttkbootstrap as ttkb
from ttkbootstrap.constants import *
from threading import Thread
from ttkbootstrap.icons import Icon

# Constants for both applications
SUPPORTED_EXTENSIONS = {'.vyp', '.zip', '.vyb', '.sqlite', '.db'}
TEMP_DIR = tempfile.gettempdir()
DEFAULT_EXCLUDED_TABLES = {
    'sqlite_sequence', 'kb_fts_vtable', 'kb_fts_vtable_content',
    'kb_fts_vtable_segdir', 'kb_images', 'kb_item_images',
    'kb_fts_vtable_segments', 'kb_txn_message_config', 'kb_settings'
}
DEFAULT_IGNORED_TYPES = {"date", "datetime", "timestamp"}
DEFAULT_DECIMAL_PRECISION = 5

# Utility functions
def is_valid_sqlite(db_path: str) -> bool:
    """Check if file is a valid SQLite database."""
    if not os.path.exists(db_path):
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        conn.close()
        return len(tables) > 0
    except:
        return False

def extract_vyp_from_vyb(vyb_path: str, temp_dirs: List[str]) -> Optional[str]:
    """Extract the .vyp file from a .vyb (ZIP) archive."""
    try:
        with zipfile.ZipFile(vyb_path, 'r') as zip_ref:
            vyp_files = [f for f in zip_ref.namelist() if f.lower().endswith('.vyp')]
            if not vyp_files:
                raise ValueError("No .vyp file found in the .vyb archive")

            temp_dir = tempfile.mkdtemp()
            temp_dirs.append(temp_dir)
            vyp_file = vyp_files[0]
            extracted_path = os.path.join(temp_dir, os.path.basename(vyp_file))
            
            with open(extracted_path, 'wb') as f:
                f.write(zip_ref.read(vyp_file))
            
            if not is_valid_sqlite(extracted_path):
                raise ValueError("Extracted file is not a valid SQLite database")
            
            return extracted_path
    except zipfile.BadZipFile:
        raise ValueError("Invalid .vyb archive")
    except Exception as e:
        raise ValueError(f"Extraction error: {str(e)}")

#-------------Home Page-----------------
class HomeTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.style = ttkb.Style()
        self.create_widgets()
    
    def create_widgets(self):
        # Main container frame with padding
        main_frame = ttk.Frame(self, padding=(20, 10))
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Header Section
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Main Title
        ttk.Label(
            header_frame,
            text="Vyapar Database Tools Suite",
            font=('Helvetica', 24, 'bold'),
            foreground="#4B0082",  # Indigo
            anchor=tk.CENTER
        ).pack(fill=tk.X, pady=(10, 5))
        
        # Subtitle
        ttk.Label(
            header_frame,
            text="Powerful utilities for Vyapar database management",
            font=('Helvetica', 14),
            foreground="#6A5ACD",  # Slate Blue
            anchor=tk.CENTER
        ).pack(fill=tk.X, pady=(0, 0))
        

        # Features Container
        features_frame = ttk.Frame(main_frame)
        features_frame.pack(fill=tk.BOTH, expand=True)

        # === Left Top Panel - Comparison Tool ===
        left_top_panel = ttk.Frame(features_frame, padding=10, style='info.TFrame')
        left_top_panel.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")

        ttk.Label(
            left_top_panel,
            text="Database Comparison",
            font=('Helvetica', 18, 'bold'),
            foreground="#006400",  # Dark Green
            anchor=tk.CENTER
        ).pack(fill=tk.X, pady=(0, 5))

        feature_text1 = """
        • Compare two Vyapar database files
        • Analyze schema differences
        • Detect data inconsistencies
        • Support for multiple formats: [.vyp, .vyb]
        """

        try:
            info_bg = self.style.colors.get('info')
        except Exception:
            info_bg = '#E6F3FF'

        text1 = tk.Text(
            left_top_panel,
            wrap=tk.WORD,
            font=('Segoe UI', 11),
            bg=info_bg,
            padx=5,
            pady=5,
            height=5,
            relief=tk.FLAT
        )
        text1.insert(tk.END, feature_text1)
        text1.configure(state='disabled')
        text1.pack(fill=tk.BOTH, expand=True)

        # === Right Top Panel - Sanitization Tool ===
        right_top_panel = ttk.Frame(features_frame, padding=15, style='success.TFrame')
        right_top_panel.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")

        ttk.Label(
            right_top_panel,
            text="Database Sanitization",
            font=('Helvetica', 18, 'bold'),
            foreground="#8B0000",  # Dark Red
            anchor=tk.CENTER
        ).pack(fill=tk.X, pady=(0, 5))

        feature_text2 = """
        • Remove sensitive information
        • Convert between .vyp and .vyb
        • Built-in sanitization templates:
        • Clear contact details
        • Reset catalog settings
        """

        try:
            success_bg = self.style.colors.get('success')
        except Exception:
            success_bg = '#E6FFE6'

        text2 = tk.Text(
            right_top_panel,
            wrap=tk.WORD,
            font=('Segoe UI', 11),
            bg=success_bg,
            padx=5,
            pady=5,
            height=5,
            relief=tk.FLAT
        )
        text2.insert(tk.END, feature_text2)
        text2.configure(state='disabled')
        text2.pack(fill=tk.BOTH, expand=True)

        # === Left Bottom Panel - FTS Tool ===
        left_bottom_panel = ttk.Frame(features_frame, padding=15, style='warning.TFrame')
        left_bottom_panel.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")

        ttk.Label(
            left_bottom_panel,
            text="FTS Table Generator",
            font=('Helvetica', 18, 'bold'),
            foreground="#DAA520",  # Goldenrod
            anchor=tk.CENTER
        ).pack(fill=tk.X, pady=(0, 5))

        feature_text3 = """
        • Generate Full Text Search (FTS) tables
        • Supports FTS3 
        • Indexes multiple business-related fields
        • Easy one-click generation
        """

        text3 = tk.Text(
            left_bottom_panel,
            wrap=tk.WORD,
            font=('Segoe UI', 11),
            bg='#FFF9E6',
            padx=5,
            pady=5,
            height=5,
            relief=tk.FLAT
        )
        text3.insert(tk.END, feature_text3)
        text3.configure(state='disabled')
        text3.pack(fill=tk.BOTH, expand=True)

        # === Right Bottom Panel - Settings Generator ===
        right_bottom_panel = ttk.Frame(features_frame, padding=15, style='secondary.TFrame')
        right_bottom_panel.grid(row=1, column=1, padx=5, pady=5, sticky="nsew")

        ttk.Label(
            right_bottom_panel,
            text="Settings Table Generator",
            font=('Helvetica', 18, 'bold'),
            foreground="#4B0082",  # Indigo
            anchor=tk.CENTER
        ).pack(fill=tk.X, pady=(0, 5))

        feature_text4 = """
        • Auto-generate settings entries
        • Customize Vyapar behavior via keys
        • Use as a boilerplate generator
        • Detect and fix common issues in one click
        """

        text4 = tk.Text(
            right_bottom_panel,
            wrap=tk.WORD,
            font=('Segoe UI', 11),
            bg='#F0F0FF',
            padx=5,
            pady=5,
            height=5,
            relief=tk.FLAT
        )
        text4.insert(tk.END, feature_text4)
        text4.configure(state='disabled')
        text4.pack(fill=tk.BOTH, expand=True)

        # Make grid responsive
        features_frame.columnconfigure(0, weight=1)
        features_frame.columnconfigure(1, weight=1)
        features_frame.rowconfigure(0, weight=1)
        features_frame.rowconfigure(1, weight=1)



        # Quick Start Section
        quick_start_frame = ttk.LabelFrame(
            main_frame,
            text=" Quick Start Guide ",
            padding=15,
            style='primary.TLabelframe'
        )
        quick_start_frame.pack(fill=tk.X, pady=(20, 10))
        
        steps = [
            ("1. Select the appropriate tab for your task", "#7700FF"),
            ("2. Load your database file(s)", "#7700FF"),
            ("3. Configure your comparison/sanitization/FTS and Settings options", "#7700FF"),
            ("4. Execute the operation","#7700FF"),
            ("5. Save or export your results", "#7700FF")
        ]
        
        for text, color in steps:
            ttk.Label(
                quick_start_frame,
                text=text,
                font=('Segoe UI', 10, 'italic'),
                foreground=color,
                anchor=tk.W
            ).pack(fill=tk.X, pady=2)
        
        # Footer
        footer_frame = ttk.Frame(main_frame)
        footer_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Label(
            footer_frame,
            text="© 2025 Vyapar Database Tools | Version 2.0",
            font=('Segoe UI', 8),
            foreground="#666666",
            anchor=tk.CENTER
        ).pack(fill=tk.X)

# ----------------- Database Comparison Tool -----------------
class DatabaseComparator:
    def __init__(self):
        self.db1_path = None
        self.db2_path = None
        self.db1_conn = None
        self.db2_conn = None
        self.included_tables = set()
        self.excluded_tables = set(DEFAULT_EXCLUDED_TABLES)
        self.ignored_data_types = set(DEFAULT_IGNORED_TYPES)
        self.decimal_precision = DEFAULT_DECIMAL_PRECISION
        self.progress_callback = None
        self.validate_db = True
        self.temp_dirs = []
        self.visual_diff_data = []

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def update_progress(self, message: str, percent: int = None):
        if self.progress_callback:
            self.progress_callback(message, percent)

    def extract_database_file(self, file_path: str) -> str:
        """Handle extraction of database files from various formats."""
        if file_path.lower().endswith(('.vyp', '.sqlite', '.db')):
            return file_path
        elif file_path.lower().endswith('.vyb'):
            extracted_path = extract_vyp_from_vyb(file_path, self.temp_dirs)
            if extracted_path:
                return extracted_path
            raise ValueError(f"Failed to extract .vyp from {file_path}")
        elif file_path.lower().endswith('.zip'):
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    for file_info in zip_ref.infolist():
                        if any(file_info.filename.lower().endswith(ext) for ext in ['.sqlite', '.db']):
                            temp_dir = tempfile.mkdtemp()
                            self.temp_dirs.append(temp_dir)
                            extracted_path = os.path.join(temp_dir, os.path.basename(file_info.filename))
                            with open(extracted_path, 'wb') as f:
                                f.write(zip_ref.read(file_info.filename))
                            if is_valid_sqlite(extracted_path):
                                return extracted_path
                            os.remove(extracted_path)
                    raise ValueError("No valid database found in zip")
            except Exception as e:
                raise ValueError(f"Zip extraction error: {str(e)}")
        else:
            raise ValueError("Unsupported file format")

    def validate_database(self, db_path: str) -> bool:
        if not self.validate_db:
            return True
            
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA integrity_check;")
            integrity_result = cursor.fetchone()
            if integrity_result[0] != "ok":
                error_msg = f"Integrity check failed for {db_path}: {integrity_result}"
                logging.error(error_msg)
                return False
            
            cursor.execute("PRAGMA foreign_key_check;")
            if cursor.fetchall():
                error_msg = f"Foreign key violations in {db_path}"
                logging.error(error_msg)
                return False
            
            return True
        except sqlite3.Error as e:
            logging.error(f"Database validation error for {db_path}: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def connect_databases(self, db1_path: str, db2_path: str):
        try:
            self.cleanup_temp_files()
            
            self.db1_path = self.extract_database_file(db1_path)
            if self.validate_db and not self.validate_database(self.db1_path):
                raise ValueError(f"Validation failed for first database: {self.db1_path}")
            self.db1_conn = sqlite3.connect(self.db1_path)
            self.db1_conn.execute("PRAGMA foreign_keys = ON;")
            
            self.db2_path = self.extract_database_file(db2_path)
            if self.validate_db and not self.validate_database(self.db2_path):
                raise ValueError(f"Validation failed for second database: {self.db2_path}")
            self.db2_conn = sqlite3.connect(self.db2_path)
            self.db2_conn.execute("PRAGMA foreign_keys = ON;")
        except Exception as e:
            if hasattr(self, 'db1_conn') and self.db1_conn:
                self.db1_conn.close()
            if hasattr(self, 'db2_conn') and self.db2_conn:
                self.db2_conn.close()
            raise

    def cleanup_temp_files(self):
        for temp_dir in self.temp_dirs:
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
            except Exception as e:
                logging.error(f"Error cleaning temp dir {temp_dir}: {str(e)}")
        self.temp_dirs = []

    def get_table_list(self, conn: sqlite3.Connection) -> List[str]:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']

    def validate_table_names(self, conn: sqlite3.Connection, table_names: Set[str]) -> Set[str]:
        existing_tables = set(self.get_table_list(conn))
        invalid_tables = table_names - existing_tables
        if invalid_tables:
            raise ValueError(f"Tables not found: {', '.join(sorted(invalid_tables))}")
        return table_names

    def get_column_info(self, conn: sqlite3.Connection, table_name: str) -> List[Dict]:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        return [
            {'cid': row[0], 'name': row[1], 'type': row[2], 
             'notnull': row[3], 'dflt_value': row[4], 'pk': row[5]}
            for row in cursor.fetchall()
        ]

    def compare_databases(self, db1_path: str, db2_path: str, 
                         included_tables: Set[str] = None, 
                         excluded_tables: Set[str] = None,
                         ignore_datetime: bool = True,
                         decimal_precision: int = DEFAULT_DECIMAL_PRECISION,
                         validate_db: bool = True) -> str:
        try:
            self.validate_db = validate_db
            self.included_tables = included_tables or set()
            self.excluded_tables = excluded_tables or set()
            self.ignored_data_types = DEFAULT_IGNORED_TYPES if ignore_datetime else set()
            self.decimal_precision = decimal_precision
            
            self.connect_databases(db1_path, db2_path)
            
            db1_tables = set(self.get_table_list(self.db1_conn))
            db2_tables = set(self.get_table_list(self.db2_conn))
            
            if self.included_tables:
                try:
                    valid_db1_tables = self.validate_table_names(self.db1_conn, self.included_tables)
                    valid_db2_tables = self.validate_table_names(self.db2_conn, self.included_tables)
                    common_included = valid_db1_tables & valid_db2_tables
                    db1_tables = db1_tables.intersection(common_included)
                    db2_tables = db2_tables.intersection(common_included)
                except ValueError as e:
                    raise ValueError(f"Table validation error: {str(e)}")
            
            db1_tables = db1_tables.difference(self.excluded_tables)
            db2_tables = db2_tables.difference(self.excluded_tables)
            
            version_diff = self._compare_versions()
            schema_diff = self._compare_schemas(db1_tables, db2_tables)
            data_diff = self._compare_data(db1_tables.intersection(db2_tables))
            
            if not any(("differ" in version_diff, 
                       "differences" in schema_diff.lower(),
                       "differences" in data_diff.lower())):
                return "=== Comparison Results ===\n\nNo differences found between the databases\n\nCompared:\n- Database versions\n- Schema (tables and columns)\n- Data (all rows and values)"
            
            return version_diff + "\n" + schema_diff + "\n" + data_diff
        except Exception as e:
            logging.error(f"Error comparing databases: {str(e)}", exc_info=True)
            raise
        finally:
            if hasattr(self, 'db1_conn') and self.db1_conn:
                self.db1_conn.close()
            if hasattr(self, 'db2_conn') and self.db2_conn:
                self.db2_conn.close()
            self.cleanup_temp_files()

    def _compare_versions(self) -> str:
        db1_version = self._get_user_version(self.db1_conn)
        db2_version = self._get_user_version(self.db2_conn)
        
        diff = ["=== Database Version ===\n"]
        diff.append(f"Database 1 User Version: {db1_version}\n")
        diff.append(f"Database 2 User Version: {db2_version}\n")
        
        if db1_version != db2_version:
            diff.append("** WARNING: Database versions differ **\n")
        
        return "".join(diff)

    def _get_user_version(self, conn: sqlite3.Connection) -> int:
        cursor = conn.cursor()
        cursor.execute("PRAGMA user_version;")
        return cursor.fetchone()[0]

    def _compare_schemas(self, db1_tables: Set[str], db2_tables: Set[str]) -> str:
        self.update_progress("Comparing schemas...", 10)
        
        diff_report = ["=== Schema Differences ===\n"]
        added_tables = db2_tables - db1_tables
        removed_tables = db1_tables - db2_tables
        common_tables = db1_tables.intersection(db2_tables)
        
        if added_tables:
            diff_report.append(f"New Tables added in DB2 {sorted(added_tables)}\n\n")
        
        if removed_tables:
            diff_report.append(f"Tables removed in DB2 {sorted(removed_tables)}\n\n")
        
        schema_differences_found = False
        for i, table in enumerate(sorted(common_tables)):
            self.update_progress(f"Comparing table schema: {table}", 10 + int(70 * i / len(common_tables)))
            
            db1_cols = self.get_column_info(self.db1_conn, table)
            db2_cols = self.get_column_info(self.db2_conn, table)
            
            db1_col_names = {col['name'] for col in db1_cols}
            db2_col_names = {col['name'] for col in db2_cols}
            
            cols_only_in_db1 = db1_col_names - db2_col_names
            cols_only_in_db2 = db2_col_names - db1_col_names
            
            if cols_only_in_db1 or cols_only_in_db2:
                schema_differences_found = True
                diff_report.append(f"*Table: {table}*\n")
                if cols_only_in_db1:
                    diff_report.append(f"  Columns only in DB1: {', '.join(sorted(cols_only_in_db1))}\n")
                if cols_only_in_db2:
                    diff_report.append(f"  Columns only in DB2: {', '.join(sorted(cols_only_in_db2))}\n")
                diff_report.append("\n")
        
        if not (added_tables or removed_tables or schema_differences_found):
            diff_report.append("No schema differences found\n")
        
        self.update_progress("Schema comparison complete", 80)
        return "".join(diff_report)

    def _round_if_float(self, value):
        if isinstance(value, float):
            try:
                return round(value, self.decimal_precision)
            except:
                return value
        return value

    def _values_equal(self, val1, val2):
        if val1 == val2:
            return True
        
        if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            return self._round_if_float(val1) == self._round_if_float(val2)
        
        return False

    def _compare_data(self, common_tables: Set[str]) -> str:
        self.update_progress("Comparing data...", 80)
        diff_report = ["=== Data Differences ===\n"]
        data_differences_found = False
        
        for i, table in enumerate(sorted(common_tables)):
            self.update_progress(f"Comparing data in table: {table}", 80 + int(20 * i / len(common_tables)))
            
            db1_cols = self.get_column_info(self.db1_conn, table)
            db2_cols = self.get_column_info(self.db2_conn, table)
            
            if len(db1_cols) != len(db2_cols):
                diff_report.append(f"*Table: {table}* has different column structures - skipping data comparison\n\n")
                continue
            
            pk_cols = [col['name'] for col in db1_cols if col['pk']] or [col['name'] for col in db1_cols]
            col_names = [col['name'] for col in db1_cols]
            order_by = ", ".join(pk_cols)
            
            db1_data = self._fetch_all_data(self.db1_conn, table, col_names, order_by)
            db2_data = self._fetch_all_data(self.db2_conn, table, col_names, order_by)
            
            pk_indices = [col_names.index(col) for col in pk_cols]
            def get_pk(row): 
                # Handle None values in primary keys by converting them to empty strings
                pk_tuple = tuple('' if val is None else val for val in (row[i] for i in pk_indices))
                return pk_tuple if len(pk_tuple) > 1 else pk_tuple[0]
                
            db1_map = {get_pk(row): row for row in db1_data}
            db2_map = {get_pk(row): row for row in db2_data}
            
            rows_only_in_db1 = []
            rows_only_in_db2 = []
            modified_rows = []
            
            # Get all primary keys and sort them safely
            all_pks = set(db1_map.keys()).union(set(db2_map.keys()))
            try:
                sorted_pks = sorted(all_pks)
            except TypeError:
                # Fallback to string representation if sorting fails
                sorted_pks = sorted(all_pks, key=lambda x: str(x))
            
            for pk in sorted_pks:
                if pk in db1_map and pk not in db2_map:
                    rows_only_in_db1.append(pk)
                elif pk not in db1_map and pk in db2_map:
                    rows_only_in_db2.append(pk)
                else:
                    row1 = db1_map[pk]
                    row2 = db2_map[pk]
                    
                    differences = []
                    for i, (val1, val2) in enumerate(zip(row1, row2)):
                        col_name = col_names[i]
                        col_type = db1_cols[i]['type'].lower()
                        
                        if any(ignored in col_type for ignored in self.ignored_data_types):
                            continue
                            
                        if not self._values_equal(val1, val2):
                            differences.append((col_name, val1, val2))
                    
                    if differences:
                        modified_rows.append((pk, differences))
                        # Store for visual viewer
                        diff_entry = {
                            "table": table,
                            "pk": dict(zip(pk_cols, pk)) if isinstance(pk, tuple) else {pk_cols[0]: pk},
                            "columns": [
                                {"name": col_name, "db1": val1, "db2": val2}
                                for col_name, val1, val2 in differences
                            ]
                        }
                        self.visual_diff_data.append(diff_entry)
                                
            if rows_only_in_db1 or rows_only_in_db2 or modified_rows:
                data_differences_found = True
                diff_report.append(f"*Table: {table}*\n")
                
                if rows_only_in_db1:
                    diff_report.append(f"  Rows only in DB1 (Columns: {', '.join(col_names)}):\n")
                    for pk in rows_only_in_db1:
                        row_dict = {col: self._round_if_float(val) for col, val in zip(col_names, db1_map[pk])}
                        diff_report.append(f"    {row_dict}\n")
                
                if rows_only_in_db2:
                    diff_report.append(f"  Rows only in DB2 (Columns: {', '.join(col_names)}):\n")
                    for pk in rows_only_in_db2:
                        row_dict = {col: self._round_if_float(val) for col, val in zip(col_names, db2_map[pk])}
                        diff_report.append(f"    {row_dict}\n")
                
                if modified_rows:
                    diff_report.append("  Modified rows:\n")
                    for pk, differences in modified_rows:
                        if isinstance(pk, tuple):
                            pk_dict = dict(zip(pk_cols, pk))
                        else:
                            pk_dict = {pk_cols[0]: pk}
                        diff_report.append(f"    Primary Key: {pk_dict}\n")
                        for col_name, val1, val2 in differences:
                            rounded_val1 = self._round_if_float(val1)
                            rounded_val2 = self._round_if_float(val2)
                            diff_report.append(f"      {col_name}: {rounded_val1} → {rounded_val2}\n")
                
                diff_report.append("\n")
        
        if not data_differences_found:
            diff_report.append("No data differences found\n")
        
        self.update_progress("Data comparison complete", 100)
        return "".join(diff_report)

    def _fetch_all_data(self, conn: sqlite3.Connection, table: str, 
                       columns: List[str], order_by: str) -> List[Tuple]:
        cursor = conn.cursor()
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table} ORDER BY {order_by};"
        cursor.execute(query)
        return cursor.fetchall()

class DatabaseComparisonTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.comparator = DatabaseComparator()
        self.comparator.set_progress_callback(self.update_progress)
        
        # Current theme (default to dark)
        self.current_theme = "cyborg"
        self.style = ttkb.Style(theme=self.current_theme)
        
        # Font configuration
        self.default_font = font.nametofont("TkDefaultFont")
        self.default_font.configure(family="Segoe UI", size=10)
        self.text_font = font.Font(family="Consolas", size=10)
        self.title_font = font.Font(family="Calibri", size=12, weight="bold")
        
        self.create_widgets()
        self.setup_theme_colors()
    
    def setup_theme_colors(self):
        """Set color tags based on current theme"""
        # Get current theme colors
        bg_color = self.style.colors.bg
        fg_color = self.style.colors.fg
        
        # Configure base colors
        self.result_text.configure(
            bg=bg_color,
            fg=fg_color,
            insertbackground=fg_color,
            selectbackground=self.style.colors.selectbg,
            selectforeground=self.style.colors.selectfg
        )
        
        # Configure tags for both themes
        if self.current_theme == "cyborg":
            # Dark theme colors
            tag_colors = {
                'version': '#5D9BFF',
                'schema': '#B57DFF',
                'data': '#FFA040',
                'table': '#00E5D2',
                'added': '#7CFC00',
                'removed': '#FF6B6B',
                'modified': '#FFD700',
                'column': '#E066FF',
                'no_diff': '#90EE90',
                'highlight': '#FFFF00'
            }
        else:
            # Light theme colors
            tag_colors = {
                'version': '#1E56A0',
                'schema': '#6A1B9A',
                'data': '#E65100',
                'table': '#00897B',
                'added': '#2E7D32',
                'removed': '#C62828',
                'modified': '#F9A825',
                'column': '#9C27B0',
                'no_diff': '#4CAF50',
                'highlight': '#FFFF00'
            }
        
        # Configure all tags
        for tag, color in tag_colors.items():
            self.result_text.tag_config(tag, foreground=color)
        
        # Force a complete refresh of the display
        self.refresh_results_display()

    def refresh_results_display(self):
        """Force a complete refresh of the results display"""
        if hasattr(self, 'last_diff_report'):
            # Save scroll position
            xview = self.result_text.xview()
            yview = self.result_text.yview()
            
            # Get current content
            current_content = self.result_text.get("1.0", tk.END)
            
            # Clear and re-insert content
            self.result_text.delete("1.0", tk.END)
            self.result_text.insert(tk.END, current_content)
            
            # Restore scroll position
            self.result_text.xview_moveto(xview[0])
            self.result_text.yview_moveto(yview[0])
    
    def toggle_validation_state(self):
        """Update the validation status label based on toggle state"""
        if self.validate_db_var.get():
            self.validation_status.config(text="Validation: ON", foreground="green")
        else:
            self.validation_status.config(text="Validation: OFF", foreground="red")
    
    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # File selection frame
        file_frame = ttk.LabelFrame(main_frame, text="Database Files", padding="10")
        file_frame.pack(fill=tk.X, pady=5)
        
        # Configure column weights to make column 1 expandable
        file_frame.columnconfigure(1, weight=1)

        # Database 1
        ttk.Label(file_frame, text="Database 1:").grid(row=0, column=0, sticky=tk.W)
        self.db1_entry = ttk.Entry(file_frame)
        self.db1_entry.grid(row=0, column=1, padx=5, sticky=tk.EW)
        ttk.Button(
            file_frame, 
            text="📂 Browse...", 
            command=self.browse_db1,
            bootstyle="info"
        ).grid(row=0, column=2, padx=5)
        
        # Database 2
        ttk.Label(file_frame, text="Database 2:").grid(row=1, column=0, sticky=tk.W)
        self.db2_entry = ttk.Entry(file_frame)
        self.db2_entry.grid(row=1, column=1, padx=5, sticky=tk.EW)
        ttk.Button(
            file_frame, 
            text="📂 Browse...", 
            command=self.browse_db2,
            bootstyle="info"
        ).grid(row=1, column=2, padx=5)
        
        # Filters frame
        filter_frame = ttk.LabelFrame(main_frame, text="Filters", padding="10")
        filter_frame.pack(fill=tk.X, pady=5)

        # Allow column 1 to expand with window resize
        filter_frame.columnconfigure(1, weight=1)
        filter_frame.columnconfigure(2, weight=1)  # Optional: if columnspan=2 used

        # Table inclusion
        ttk.Label(filter_frame, text="Include Tables (comma separated):").grid(row=0, column=0, sticky=tk.W)
        self.include_entry = ttk.Entry(filter_frame)
        self.include_entry.grid(row=0, column=1, padx=5, sticky="ew", columnspan=2)

        # Table exclusion
        ttk.Label(filter_frame, text="Exclude Tables (comma separated):").grid(row=1, column=0, sticky=tk.W)
        self.exclude_entry = ttk.Entry(filter_frame)
        self.exclude_entry.grid(row=1, column=1, padx=5, sticky="ew", columnspan=2)
        self.exclude_entry.insert(0, ", ".join(sorted(DEFAULT_EXCLUDED_TABLES)))

        # Options frame
        options_frame = ttk.LabelFrame(main_frame, text="Comparison Options", padding="10")
        options_frame.pack(fill=tk.X, pady=5)

        # Configure 4 equal-width columns
        for col in range(4):
            options_frame.columnconfigure(col, weight=1)

        # Row 0 - All 4 items
        # 1. Validate databases
        self.validate_db_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            options_frame,
            text="🔍 Validate databases (recommended)",
            variable=self.validate_db_var,
            bootstyle="round-toggle",
            command=self.toggle_validation_state
        ).grid(row=0, column=0, sticky="ew", padx=5)

        # 2. Validation status
        self.validation_status = ttk.Label(
            options_frame,
            text="Validation: ON",
            foreground="green"
        )
        self.validation_status.grid(row=0, column=1, sticky="ew", padx=5)

        # 3. Ignore datetime toggle
        self.ignore_datetime_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            options_frame,
            text="⏱️ Ignore date/datetime/timestamp",
            variable=self.ignore_datetime_var,
            bootstyle="round-toggle"
        ).grid(row=0, column=2, sticky="ew", padx=5)

        # 4. Decimal precision (label + spinbox in one frame for alignment)
        precision_frame = ttk.Frame(options_frame)
        precision_frame.grid(row=0, column=3, sticky="ew", padx=5)
        precision_frame.columnconfigure(1, weight=1)

        ttk.Label(precision_frame, text="🔢 Decimal precision:").grid(row=0, column=0, sticky="e", padx=(0, 5))
        self.decimal_precision_var = tk.StringVar(value=str(DEFAULT_DECIMAL_PRECISION))
        self.decimal_precision_spin = ttk.Spinbox(
            precision_frame,
            from_=0,
            to=15,
            textvariable=self.decimal_precision_var,
            width=5
        )
        self.decimal_precision_spin.grid(row=0, column=1, sticky="w")

        # Button frame (stretches across, but content will be centered)
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=5)

        # Inner frame to hold buttons and center them
        center_frame = ttk.Frame(button_frame)
        center_frame.pack(anchor="center")  # Center this inner frame in button_frame

        # Compare button
        self.compare_btn = ttk.Button(
            center_frame, 
            text="🔍 Compare Databases", 
            command=self.compare_databases,
            bootstyle="primary"
        )
        self.compare_btn.pack(side=tk.LEFT, padx=10)

        # Reset button
        self.reset_btn = ttk.Button(
            center_frame, 
            text="🔄 Reset", 
            command=self.reset_ui,
            bootstyle="warning"
        )
        self.reset_btn.pack(side=tk.LEFT, padx=10)

        # Visual Diff button (hidden initially)
        self.visual_diff_btn = ttk.Button(
            center_frame,
            text="🖼️ View Visual Diff",
            command=self.show_visual_diff,
            bootstyle="secondary"
        )
        self.visual_diff_btn.pack(side=tk.LEFT, padx=10)
        self.visual_diff_btn.pack_forget()  # Hide initially


        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            main_frame, 
            variable=self.progress_var, 
            maximum=100,
            bootstyle="striped"
        )
        self.progress_bar.pack(fill=tk.X, pady=5)
        
        # Status label
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_label = ttk.Label(main_frame, textvariable=self.status_var)
        self.status_label.pack(fill=tk.X, pady=5)
        
        # Results frame
        result_frame = ttk.LabelFrame(main_frame, text="Comparison Results", padding="10")
        result_frame.pack(fill=tk.BOTH, expand=True)
        
        # Horizontal scrollbar
        self.hscroll = ttk.Scrollbar(result_frame, orient=tk.HORIZONTAL)
        self.hscroll.pack(side=tk.BOTTOM, fill=tk.X)

        # Vertical scrollbar
        self.vscroll = ttk.Scrollbar(result_frame, orient=tk.VERTICAL)
        self.vscroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Text widget with both scrollbars
        self.result_text = tk.Text(
            result_frame, 
            wrap=tk.NONE,
            font=self.text_font,
            xscrollcommand=self.hscroll.set,
            yscrollcommand=self.vscroll.set
        )
        self.result_text.pack(fill=tk.BOTH, expand=True)

        # Configure scrollbars to control the text widget
        self.hscroll.config(command=self.result_text.xview)
        self.vscroll.config(command=self.result_text.yview)

        # Configure tags for colored diff
        self.setup_theme_colors()
        
        # Context menu
        self.create_context_menu()
    
    def create_context_menu(self):
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(label="📋 Copy", command=self.copy_text)
        self.context_menu.add_command(label="🧹 Clear Results", command=self.clear_results)
        self.result_text.bind("<Button-3>", self.show_context_menu)
    
    def show_context_menu(self, event):
        try:
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()
    
    def copy_text(self):
        selected_text = self.result_text.get(tk.SEL_FIRST, tk.SEL_LAST)
        if selected_text:
            self.clipboard_clear()
            self.clipboard_append(selected_text)
    
    def clear_results(self):
        self.result_text.delete(1.0, tk.END)
    
    def reset_ui(self):
        """Reset all inputs and outputs"""
        self.comparator = DatabaseComparator()
        self.comparator.set_progress_callback(self.update_progress)
        self.db1_entry.delete(0, tk.END)
        self.db2_entry.delete(0, tk.END)
        self.include_entry.delete(0, tk.END)
        self.exclude_entry.delete(0, tk.END)
        self.exclude_entry.insert(0, ", ".join(sorted(DEFAULT_EXCLUDED_TABLES)))
        self.validate_db_var.set(True)
        self.ignore_datetime_var.set(True)
        self.decimal_precision_var.set(str(DEFAULT_DECIMAL_PRECISION))
        self.result_text.delete(1.0, tk.END)
        self.status_var.set("Ready")
        self.progress_var.set(0)
        self.toggle_validation_state()  # Update the validation status label
        self.visual_diff_btn.pack_forget()

    
    def browse_db1(self):
        file_path = filedialog.askopenfilename(
            title="Select First Database",
            filetypes=[("Database Files", "*.vyp *.vyb *.sqlite *.db *.zip"), ("All Files", "*.*")]
        )
        if file_path:
            self.db1_entry.delete(0, tk.END)
            self.db1_entry.insert(0, file_path)
    
    def browse_db2(self):
        file_path = filedialog.askopenfilename(
            title="Select Second Database",
            filetypes=[("Database Files", "*.vyp *.vyb *.sqlite *.db *.zip"), ("All Files", "*.*")]
        )
        if file_path:
            self.db2_entry.delete(0, tk.END)
            self.db2_entry.insert(0, file_path)
    
    def update_progress(self, message: str, percent: int = None):
        self.status_var.set(message)
        if percent is not None:
            self.progress_var.set(percent)
        self.update_idletasks()
    
    def compare_databases(self):
        db1_path = self.db1_entry.get()
        db2_path = self.db2_entry.get()
        
        if not db1_path or not db2_path:
            messagebox.showerror("Error", "Please select both database files")
            return
        
        # Get filters
        included_tables = set()
        include_text = self.include_entry.get().strip()
        if include_text:
            included_tables = {t.strip() for t in include_text.split(",") if t.strip()}
        
        excluded_tables = set()
        exclude_text = self.exclude_entry.get().strip()
        if exclude_text:
            excluded_tables = {t.strip() for t in exclude_text.split(",") if t.strip()}
        
        # Get options
        ignore_datetime = self.ignore_datetime_var.get()
        validate_db = self.validate_db_var.get()
        try:
            decimal_precision = int(self.decimal_precision_var.get())
            if decimal_precision < 0 or decimal_precision > 15:
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", "Decimal precision must be between 0 and 15")
            return
        
        try:
            self.compare_btn.config(state=tk.DISABLED)
            self.reset_btn.config(state=tk.DISABLED)
            self.result_text.delete(1.0, tk.END)
            self.result_text.insert(tk.END, "Comparing databases...\n")
            
            # Run comparison in background thread
            thread = threading.Thread(
                target=self._run_comparison_thread,
                args=(db1_path, db2_path, included_tables, excluded_tables, 
                     ignore_datetime, decimal_precision, validate_db),
                daemon=True
            )
            thread.start()
            
        except Exception as e:
            self.handle_error(str(e))
            self.compare_btn.config(state=tk.NORMAL)
            self.reset_btn.config(state=tk.NORMAL)
    
    def _run_comparison_thread(self, db1_path, db2_path, included_tables, 
                             excluded_tables, ignore_datetime, 
                             decimal_precision, validate_db):
        try:
            diff_report = self.comparator.compare_databases(
                db1_path, db2_path,
                included_tables, excluded_tables,
                ignore_datetime, decimal_precision,
                validate_db
            )
            
            self.after(0, self.display_results, diff_report)
        except Exception as e:
            self.after(0, self.handle_error, str(e))
        finally:
            self.after(0, lambda: self.compare_btn.config(state=tk.NORMAL))
            self.after(0, lambda: self.reset_btn.config(state=tk.NORMAL))
            self.after(0, self.update_progress, "Comparison complete", 100)
    
    def display_results(self, diff_report: str):
        self.result_text.delete(1.0, tk.END)
        
        if "No differences found" in diff_report:
            self.result_text.insert(tk.END, diff_report, 'no_diff')
            return
        
        current_section = None
        for line in diff_report.splitlines():
            if line.startswith('==='):
                if 'Database Version' in line:
                    current_section = 'version'
                    self.result_text.insert(tk.END, line + '\n', 'version')
                elif 'Schema Differences' in line:
                    current_section = 'schema'
                    self.result_text.insert(tk.END, line + '\n', 'schema')
                elif 'Data Differences' in line:
                    current_section = 'data'
                    self.result_text.insert(tk.END, line + '\n', 'data')
                else:
                    self.result_text.insert(tk.END, line + '\n')
            elif line.startswith('*Table:'):
                self.result_text.insert(tk.END, line + '\n', 'table')
            elif 'Columns:' in line:
                self.result_text.insert(tk.END, line + '\n', 'column')
            elif any(x in line for x in ['only in DB1', 'only in DB2']):
                self.result_text.insert(tk.END, line + '\n', 'removed' if 'DB1' in line else 'added')
            elif '→' in line:
                self.result_text.insert(tk.END, line + '\n', 'modified')
            elif 'No ' in line and 'differences' in line:
                self.result_text.insert(tk.END, line + '\n', 'no_diff')
            else:
                self.result_text.insert(tk.END, line + '\n')
        
        self.result_text.see("1.0")

        # Show visual diff button only if modified rows exist
        if hasattr(self.comparator, 'visual_diff_data') and self.comparator.visual_diff_data:
            if len(self.comparator.visual_diff_data) <= 500:
                self.visual_diff_btn.pack()  # Auto-show for small datasets
            else:
                if messagebox.askyesno("Large Diff", "Visual diff is large. Show it anyway?"):
                    self.visual_diff_btn.pack()
    

    
    def show_visual_diff(self):
        data = self.comparator.visual_diff_data
        if not data:
            messagebox.showinfo("No Diff", "No modified rows to display")
            return

        win = tk.Toplevel(self)
        win.title("Visual Diff Viewer")
        win.geometry("1200x600")
        
        # Create a frame for the treeview and scrollbars
        tree_frame = ttk.Frame(win)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)


        # Create treeview with both scrollbars
        tree = ttk.Treeview(tree_frame, columns=("Table", "PK", "Column", "DB1", "DB2"), show="headings")
        
        # Configure column headings
        tree.heading("Table", text="Table")
        tree.heading("PK", text="Primary Key")
        tree.heading("Column", text="Column")
        tree.heading("DB1", text="Value in DB1")
        tree.heading("DB2", text="Value in DB2")
        
        # Configure column widths and stretch
        tree.column("Table", width=150, stretch=tk.NO)
        tree.column("PK", width=200, stretch=tk.NO)
        tree.column("Column", width=150, stretch=tk.NO)
        tree.column("DB1", width=250, stretch=tk.YES)
        tree.column("DB2", width=250, stretch=tk.YES)

        # Add scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # Grid layout
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # Configure grid weights
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)


        # Populate tree with data
        for row in data:
            table = row["table"]
            pk = ", ".join(f"{k}={v}" for k, v in row["pk"].items())
            for col in row["columns"]:
                db1 = str(col["db1"])
                db2 = str(col["db2"])
                tree.insert("", tk.END, values=(table, pk, col["name"], db1, db2))

        # Add color tags if needed
        def style_row(event=None):
            for item in tree.get_children():
                values = tree.item(item)["values"]
                if values[3] != values[4]:  # DB1 != DB2
                    tree.item(item, tags=("changed",))
                
                is_dark = self.current_theme
                if is_dark == "cyborg":
                    tree.tag_configure("changed", background="#2C2C2C", foreground="#FFFFFF")   
                else:
                    # Light theme colors
                    tree.tag_configure("changed", background="#EBEEEF", foreground="#080808")
            

        tree.bind("<<TreeviewOpen>>", style_row)

        style_row()

    def handle_error(self, error_msg: str):
        messagebox.showerror("Error", error_msg)
        logging.error(error_msg)
        self.status_var.set(f"Error: {error_msg}")

# ----------------- Database Sanitization Tool -----------------


class DatabaseSanitizerTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.style = ttkb.Style()  # <-- Add this line
        self.create_widgets()
        self.setup_theme_colors()
        
        # Register cleanup function
        atexit.register(self.cleanup_temp_dir)
    
    def setup_theme_colors(self):
        """Set color tags based on current theme"""
        if self.style.theme_use() == "cyborg":
            # Dark theme colors
            self.status_text.tag_config('error', foreground='#FF6B6B')
            self.status_text.tag_config('success', foreground='#7CFC00')
            self.status_text.configure(bg='#222222', fg='#FFFFFF')
        else:
            # Light theme colors
            self.status_text.tag_config('error', foreground='red')
            self.status_text.tag_config('success', foreground='green')
            self.status_text.configure(bg='#FFFFFF', fg='#000000')
    
    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Header frame
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(pady=20)
        
        # Header with icon
        ttk.Label(
            header_frame, 
            text="🔒 Database Sanitization Tool", 
            font=('Helvetica', 20, 'bold'),
            bootstyle="primary"
        ).pack()
        
        # Input file selection
        input_frame = ttk.LabelFrame(main_frame, text="📁 Select Input File (.vyb or .vyp)", bootstyle="info")
        input_frame.pack(pady=10, padx=20, fill=tk.X)
        
        self.input_file_entry = ttk.Entry(input_frame, font=('Helvetica', 12, 'bold'))
        self.input_file_entry.pack(side=tk.LEFT, padx=5, pady=5, expand=True, fill=tk.X)
        
        browse_button = ttk.Button(
            input_frame, 
            text="📂 Browse", 
            command=self.browse_input_file, 
            bootstyle="info",
            width=10
        )
        browse_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # SQL query input
        query_frame = ttk.LabelFrame(main_frame, text="📝 Enter SQL Queries (separated by ;)", bootstyle="info")
        query_frame.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        
        self.query_entry = scrolledtext.ScrolledText(
            query_frame, 
            height=10, 
            font=('Helvetica', 12), 
            wrap=tk.WORD,
            padx=5,
            pady=5
        )
        self.query_entry.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Pre-fill the query entry with the provided queries
        self.query_entry.insert(tk.END, """
UPDATE kb_names SET phone_number = '', email = '';
UPDATE kb_settings SET setting_value = '' WHERE setting_key = 'VYAPAR.TXNMSGOWNERNUMBER';
UPDATE kb_firms SET firm_phone = '665565', firm_email = '';
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.CATALOGUEID', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.CATALOGUEUID', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.CATALOGUEALIAS', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.AUTOSHAREINVOICESONVYAPARNETWORK', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('service_reminders_enabled', '0');  
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.TXNMSGTOOWNER', NULL);
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.TXNUPDATEMESSAGEENABLED', '0'); 
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.TRANSACTIONMESSAGEENABLED', '0');
INSERT OR REPLACE INTO kb_settings (setting_key, setting_value) VALUES ('VYAPAR.VYAPAR.SYNCENABLED', '0');  
UPDATE kb_transactions SET mobile_no = '';                             
""")
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=10)
        
        execute_button = ttk.Button(
            button_frame, 
            text="⚡ Execute SQL Queries", 
            command=self.execute_process, 
            bootstyle="success",
            width=25
        )
        execute_button.pack(side=tk.LEFT, padx=10)
        
        convert_button = ttk.Button(
            button_frame, 
            text="🔄 Convert/Repack File", 
            command=self.convert_file, 
            bootstyle="primary",
            width=20
        )
        convert_button.pack(side=tk.LEFT, padx=10)
    
        
        # Progress bar
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(pady=10, padx=20, fill=tk.X)
        
        self.progress_bar = ttk.Progressbar(
            progress_frame, 
            orient="horizontal", 
            mode="determinate",
            bootstyle="success striped"
        )
        self.progress_bar.pack(fill=tk.X)
        
        # Status text with custom tags
        status_frame = ttk.LabelFrame(main_frame, text="📋 Operation Log", bootstyle="info")
        status_frame.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        
        self.status_text = scrolledtext.ScrolledText(
            status_frame, 
            height=10, 
            font=('Helvetica', 12), 
            wrap=tk.WORD,
            padx=5,
            pady=5
        )
        self.status_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Configure text tags for styling
        self.setup_theme_colors()
        
        # Download buttons
        download_frame = ttk.Frame(main_frame)
        download_frame.pack(pady=10, padx=20)
        
        self.download_vyb_button = ttk.Button(
            download_frame, 
            text="⬇️ Download .vyb", 
            state=DISABLED, 
            command=self.download_vyb, 
            bootstyle="primary",
            width=20
        )
        self.download_vyb_button.pack(side=tk.LEFT, padx=5)
        
        self.download_vyp_button = ttk.Button(
            download_frame, 
            text="⬇️ Download .vyp", 
            state=DISABLED, 
            command=self.download_vyp, 
            bootstyle="primary",
            width=20
        )
        self.download_vyp_button.pack(side=tk.LEFT, padx=5)
    
    def unzip_vyb(self, vyb_file, extract_to):
        with zipfile.ZipFile(vyb_file, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def zip_vyp(self, vyp_file, vyb_file):
        with zipfile.ZipFile(vyb_file, 'w') as zip_ref:
            zip_ref.write(vyp_file, os.path.basename(vyp_file))

    def execute_queries_and_save(self, input_db, output_db, queries):
        try:
            # Copy the input database to the output database
            shutil.copy(input_db, output_db)

            # Connect to the output database
            conn = sqlite3.connect(output_db)
            cursor = conn.cursor()

            # Enable foreign key checks
            cursor.execute("PRAGMA foreign_keys = ON;")

            # Check database integrity
            # cursor.execute("PRAGMA integrity_check;")
            # integrity_check = cursor.fetchone()
            # if integrity_check[0] != "ok":
            #     raise sqlite3.Error(f"Database integrity check failed: {integrity_check[0]}")

            # Execute each query
            for query in queries:
                if query.strip():  # Skip empty queries
                    cursor.execute(query)
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def process_file(self, input_file, queries):
        temp_dir = "temp"
        os.makedirs(temp_dir, exist_ok=True)

        try:
            # Generate output file name
            input_filename = os.path.basename(input_file)
            output_filename = f"Sanitized_{input_filename}"
            output_vyp = os.path.join(temp_dir, output_filename.replace(".vyb", ".vyp"))
            output_vyb = os.path.join(temp_dir, output_filename.replace(".vyp", ".vyb"))

            # If input is .vyb, unzip it to get .vyp
            if input_file.endswith(".vyb"):
                self.progress_bar["value"] = 20
                self.update()
                self.unzip_vyb(input_file, temp_dir)
                # Find the extracted .vyp file in the temp directory
                extracted_files = os.listdir(temp_dir)
                vyp_file = None
                for file in extracted_files:
                    if file.endswith(".vyp"):
                        vyp_file = os.path.join(temp_dir, file)
                        break
                if not vyp_file:
                    raise ValueError("No .vyp file found in the extracted .vyb archive.")
            else:
                # If input is .vyp, use it directly
                vyp_file = input_file

            self.progress_bar["value"] = 40
            self.update()

            # Execute the queries and save the modified database
            self.execute_queries_and_save(vyp_file, output_vyp, queries)

            self.progress_bar["value"] = 60
            self.update()

            # Always generate both .vyp and .vyb files
            # Zip the .vyp file into .vyb
            self.zip_vyp(output_vyp, output_vyb)

            self.progress_bar["value"] = 100
            self.update()
            self.status_text.insert(tk.END, f"✓ Output .vyp file generated: {output_vyp}\n", 'success')
            self.status_text.insert(tk.END, f"✓ Output .vyb file generated: {output_vyb}\n", 'success')
            logging.info(f"Output .vyp file generated: {output_vyp}")
            logging.info(f"Output .vyb file generated: {output_vyb}")

            # Enable download buttons if the output files exist
            if os.path.exists(output_vyp) and os.path.exists(output_vyb):
                self.download_vyb_button.config(state=NORMAL)
                self.download_vyp_button.config(state=NORMAL)
        except Exception as e:
            self.status_text.insert(tk.END, f"✗ Error: {str(e)}\n", 'error')
            logging.error(f"Error: {str(e)}")
            self.progress_bar["value"] = 0

    def convert_file(self):
        input_file = self.input_file_entry.get()
        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: Please select an input file first\n", 'error')
            return
        
        self.progress_bar["value"] = 0
        Thread(target=self.do_conversion_and_repacking, args=(input_file,), daemon=True).start()

    def do_conversion_and_repacking(self, input_file):
        temp_dir = "temp"
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            input_filename = os.path.basename(input_file)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(input_filename)[0]
            input_ext = os.path.splitext(input_file)[1].lower()
            
            self.progress_bar["value"] = 10
            self.update()
            
            if input_ext == ".vyb":
                # First unzip the .vyb file to get the .vyp
                self.unzip_vyb(input_file, temp_dir)
                
                # Find the extracted .vyp file
                extracted_files = os.listdir(temp_dir)
                vyp_file = None
                for file in extracted_files:
                    if file.endswith(".vyp"):
                        vyp_file = os.path.join(temp_dir, file)
                        break
                
                if not vyp_file:
                    raise ValueError("No .vyp file found in the extracted .vyb archive.")
                
                self.progress_bar["value"] = 30
                self.update()
                
                # Create converted .vyp file (just rename the extracted file)
                converted_vyp = os.path.join(temp_dir, f"converted_{base_name}_{timestamp}.vyp")
                os.rename(vyp_file, converted_vyp)
                
                # Create repacked .vyb file (zip the .vyp back to .vyb)
                repacked_vyb = os.path.join(temp_dir, f"repacked_{base_name}_{timestamp}.vyb")
                self.zip_vyp(converted_vyp, repacked_vyb)
                
                self.progress_bar["value"] = 70
                self.update()
                
                # Also create a converted .vyp to .vyb conversion (different from repacked)
                converted_vyb = os.path.join(temp_dir, f"converted_{base_name}_to_vyb_{timestamp}.vyb")
                self.zip_vyp(converted_vyb, converted_vyb)
                
                self.status_text.insert(tk.END, f"✓ Converted .vyp file generated: {converted_vyp}\n", 'success')
                self.status_text.insert(tk.END, f"✓ Repacked .vyb file generated: {repacked_vyb}\n", 'success')
                self.status_text.insert(tk.END, f"✓ Converted .vyb file generated: {converted_vyb}\n", 'success')
                
            elif input_ext == ".vyp":
                # Create converted .vyb file
                converted_vyb = os.path.join(temp_dir, f"converted_{base_name}_{timestamp}.vyb")
                self.zip_vyp(input_file, converted_vyb)
                
                self.progress_bar["value"] = 30
                self.update()
                
                # Create repacked .vyp file (just copy with new name)
                repacked_vyp = os.path.join(temp_dir, f"repacked_{base_name}_{timestamp}.vyp")
                shutil.copy(input_file, repacked_vyp)
                
                self.progress_bar["value"] = 70
                self.update()
                
                # Also create a converted .vyb to .vyp conversion (unzip the just created .vyb)
                self.unzip_vyb(converted_vyb, temp_dir)
                
                # Find the extracted .vyp file
                extracted_files = os.listdir(temp_dir)
                converted_vyp = None
                for file in extracted_files:
                    if file.endswith(".vyp") and file != os.path.basename(repacked_vyp):
                        converted_vyp = os.path.join(temp_dir, file)
                        new_name = os.path.join(temp_dir, f"converted_{base_name}_to_vyp_{timestamp}.vyp")
                        os.rename(converted_vyp, new_name)
                        converted_vyp = new_name
                        break
                
                self.status_text.insert(tk.END, f"✓ Converted .vyb file generated: {converted_vyb}\n", 'success')
                self.status_text.insert(tk.END, f"✓ Repacked .vyp file generated: {repacked_vyp}\n", 'success')
                if converted_vyp:
                    self.status_text.insert(tk.END, f"✓ Converted .vyp file generated: {converted_vyp}\n", 'success')
            
            self.progress_bar["value"] = 100
            self.update()
            
            # Enable download buttons
            self.download_vyb_button.config(state=NORMAL)
            self.download_vyp_button.config(state=NORMAL)
            
        except Exception as e:
            self.status_text.insert(tk.END, f"✗ Error during conversion/repacking: {str(e)}\n", 'error')
            logging.error(f"Error during conversion/repacking: {str(e)}")
            self.progress_bar["value"] = 0

    def browse_input_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("VYB & VYP Files", "*.vyb *.vyp")])
        self.input_file_entry.delete(0, tk.END)
        self.input_file_entry.insert(0, file_path)

    def execute_process(self):
        input_file = self.input_file_entry.get()
        queries = self.query_entry.get("1.0", tk.END).strip().split(";")  # Split queries by ;

        if not input_file or not queries:
            self.status_text.insert(tk.END, "✗ Error: Please provide both input file and SQL queries\n", 'error')
            return

        self.progress_bar["value"] = 0
        Thread(target=self.process_file, args=(input_file, queries), daemon=True).start()

    def download_vyp(self):
        input_file = self.input_file_entry.get()
        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: No input file selected\n", 'error')
            return

        # Look for all possible .vyp files
        input_filename = os.path.basename(input_file)
        base_name = os.path.splitext(input_filename)[0]
        
        possible_files = []
        for f in os.listdir("temp"):
            if (f.startswith(f"Sanitized_{base_name}") or 
                f.startswith(f"converted_{base_name}") or 
                f.startswith(f"repacked_{base_name}")) and f.endswith(".vyp"):
                possible_files.append(f)
        
        if not possible_files:
            self.status_text.insert(tk.END, f"✗ Error: No .vyp output files found for this input\n", 'error')
            return
        
        # If multiple files, let user choose
        if len(possible_files) > 1:
            choice = simpledialog.askstring("Select File", 
                                         "Multiple .vyp files found. Enter the number:\n" + 
                                         "\n".join(f"{i+1}. {f}" for i, f in enumerate(possible_files)),
                                         parent=self)
            try:
                choice_idx = int(choice) - 1
                if choice_idx < 0 or choice_idx >= len(possible_files):
                    raise ValueError
                selected_file = possible_files[choice_idx]
            except:
                self.status_text.insert(tk.END, "✗ Invalid selection\n", 'error')
                return
        else:
            selected_file = possible_files[0]
        
        output_vyp = os.path.join("temp", selected_file)
        
        file_path = filedialog.asksaveasfilename(defaultextension=".vyp", 
                                               filetypes=[("VYP Files", "*.vyp")], 
                                               initialfile=selected_file)
        if file_path:
            shutil.copy(output_vyp, file_path)
            self.status_text.insert(tk.END, f"✓ File saved successfully: {file_path}\n", 'success')

    def download_vyb(self):
        input_file = self.input_file_entry.get()
        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: No input file selected\n", 'error')
            return

        # Look for all possible .vyb files
        input_filename = os.path.basename(input_file)
        base_name = os.path.splitext(input_filename)[0]
        
        possible_files = []
        for f in os.listdir("temp"):
            if (f.startswith(f"Sanitized_{base_name}") or 
                f.startswith(f"converted_{base_name}") or 
                f.startswith(f"repacked_{base_name}")) and f.endswith(".vyb"):
                possible_files.append(f)
        
        if not possible_files:
            self.status_text.insert(tk.END, f"✗ Error: No .vyb output files found for this input\n", 'error')
            return
        
        # If multiple files, let user choose
        if len(possible_files) > 1:
            choice = simpledialog.askstring("Select File", 
                                         "Multiple .vyb files found. Enter the number:\n" + 
                                         "\n".join(f"{i+1}. {f}" for i, f in enumerate(possible_files)),
                                         parent=self)
            try:
                choice_idx = int(choice) - 1
                if choice_idx < 0 or choice_idx >= len(possible_files):
                    raise ValueError
                selected_file = possible_files[choice_idx]
            except:
                self.status_text.insert(tk.END, "✗ Invalid selection\n", 'error')
                return
        else:
            selected_file = possible_files[0]
        
        output_vyb = os.path.join("temp", selected_file)
        
        file_path = filedialog.asksaveasfilename(defaultextension=".vyb", 
                                               filetypes=[("VYB Files", "*.vyb")], 
                                               initialfile=selected_file)
        if file_path:
            if selected_file.startswith(("converted_", "repacked_")):
                shutil.copy(output_vyb, file_path)
            else:
                # For sanitized files, zip the corresponding .vyp file
                output_vyp = output_vyb.replace(".vyb", ".vyp")
                if os.path.exists(output_vyp):
                    self.zip_vyp(output_vyp, file_path)
            self.status_text.insert(tk.END, f"✓ File saved successfully: {file_path}\n", 'success')

    def cleanup_temp_dir(self):
        if os.path.exists("temp"):
            shutil.rmtree("temp", ignore_errors=True)


# ----------------- FTS TAB -----------------

class FTSTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.style = ttkb.Style()
        self.create_widgets()
        self.setup_theme_colors()
        
        # Register cleanup function
        atexit.register(self.cleanup_temp_dir)
    
    def setup_theme_colors(self):
        """Set color tags based on current theme"""
        if self.style.theme_use() == "cyborg":
            # Dark theme colors
            self.status_text.tag_config('error', foreground='#FF6B6B')
            self.status_text.tag_config('success', foreground='#7CFC00')
            self.status_text.configure(bg='#222222', fg='#FFFFFF')
        else:
            # Light theme colors
            self.status_text.tag_config('error', foreground='red')
            self.status_text.tag_config('success', foreground='green')
            self.status_text.configure(bg='#FFFFFF', fg='#000000')
    
    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Header frame
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(pady=20)
        
        # Header with icon
        ttk.Label(
            header_frame, 
            text="🔍 FTS Table Generator", 
            font=('Helvetica', 20, 'bold'),
            bootstyle="primary"
        ).pack()
        
        # Input file selection
        input_frame = ttk.LabelFrame(main_frame, text="📁 Select Input File (.vyb or .vyp)", bootstyle="info")
        input_frame.pack(pady=10, padx=20, fill=tk.X)
        
        self.input_file_entry = ttk.Entry(input_frame, font=('Helvetica', 12, 'bold'))
        self.input_file_entry.pack(side=tk.LEFT, padx=5, pady=5, expand=True, fill=tk.X)
        
        browse_button = ttk.Button(
            input_frame, 
            text="📂 Browse", 
            command=self.browse_input_file, 
            bootstyle="info",
            width=10
        )
        browse_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # Information text
        info_frame = ttk.LabelFrame(main_frame, text="ℹ️ About FTS Table", bootstyle="info")
        info_frame.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        
        info_text = scrolledtext.ScrolledText(
            info_frame, 
            height=8, 
            font=('Helvetica', 11), 
            wrap=tk.WORD,
            padx=5,
            pady=5
        )
        info_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        info_text.insert(tk.END, """
This utility re-creates a Full Text Search (FTS) table in Vyapar databases.

The FTS table (kb_fts_vtable) will index:
- Party names and phone numbers
- Transaction descriptions and amounts
- Invoice numbers and prefixes
- Item details (names, codes, HSN/SAC)
- Batch/serial numbers & Payment references

The FTS table uses SQLite's FTS3 engine for improved search results.

Note: This process may take several minutes for large databases.
""")
        info_text.config(state=tk.DISABLED)
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=10)
        
        create_fts_button = ttk.Button(
            button_frame, 
            text="⚡ Create FTS Table", 
            command=self.execute_fts_creation, 
            bootstyle="success",
            width=20
        )
        create_fts_button.pack(side=tk.LEFT, padx=10)
        
        # Progress bar
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(pady=10, padx=20, fill=tk.X)
        
        self.progress_bar = ttk.Progressbar(
            progress_frame, 
            orient="horizontal", 
            mode="determinate",
            bootstyle="success striped"
        )
        self.progress_bar.pack(fill=tk.X)
        
        # Status text with custom tags
        status_frame = ttk.LabelFrame(main_frame, text="📋 Operation Log", bootstyle="info")
        status_frame.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        
        self.status_text = scrolledtext.ScrolledText(
            status_frame, 
            height=10, 
            font=('Helvetica', 12), 
            wrap=tk.WORD,
            padx=5,
            pady=5
        )
        self.status_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Configure text tags for styling
        self.setup_theme_colors()
        
        # Download buttons
        download_frame = ttk.Frame(main_frame)
        download_frame.pack(pady=10, padx=20)
        
        self.download_vyb_button = ttk.Button(
            download_frame, 
            text="⬇️ Download .vyb", 
            state=DISABLED, 
            command=self.download_vyb, 
            bootstyle="primary",
            width=20
        )
        self.download_vyb_button.pack(side=tk.LEFT, padx=5)
        
        self.download_vyp_button = ttk.Button(
                       download_frame, 
            text="⬇️ Download .vyp", 
            state=DISABLED, 
            command=self.download_vyp, 
            bootstyle="primary",
            width=20
        )
        self.download_vyp_button.pack(side=tk.LEFT, padx=5)
    
    def unzip_vyb(self, vyb_file, extract_to):
        with zipfile.ZipFile(vyb_file, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def zip_vyp(self, vyp_file, vyb_file):
        with zipfile.ZipFile(vyb_file, 'w') as zip_ref:
            zip_ref.write(vyp_file, os.path.basename(vyp_file))

    def create_fts_table(self, input_db, output_db):
        try:
            # Copy the input database to the output database
            shutil.copy(input_db, output_db)

            # Connect to the output database
            conn = sqlite3.connect(output_db)
            cursor = conn.cursor()

            # Enable foreign key checks
            cursor.execute("PRAGMA foreign_keys = ON;")

            # Check database integrity
            # cursor.execute("PRAGMA integrity_check;")
            # integrity_check = cursor.fetchone()
            # if integrity_check[0] != "ok":
            #     raise sqlite3.Error(f"Database integrity check failed: {integrity_check[0]}")

            # First drop all existing FTS-related tables
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable;")
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable_content;")
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable_segdir;")
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable_segments;")
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable_config;")
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable_data;")
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable_docsize;")
            cursor.execute("DROP TABLE IF EXISTS kb_fts_vtable_idx;")
            
            # Create the FTS table using FTS3/4 syntax which creates the expected tables
            cursor.execute("""
            CREATE VIRTUAL TABLE kb_fts_vtable USING fts3(
                fts_name_id, 
                fts_txn_id, 
                fts_text,
            );""")
            
            # Data population query
            cursor.execute("""
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
                ' ', ' '), ' ', ' '), ' ', ' '), ' ', ' '), ' ', ' ') 
            FROM (
                SELECT 
                    txn.*, 
                    group_concat(pm.payment_reference, ' ') pr 
                FROM (
                    SELECT 
                        t.txn_id txn_id, 
                        t.txn_name_id, 
                        t.txn_cash_amount cash_amount, 
                        t.txn_balance_amount balance_amount, 
                        t.txn_cash_amount+t.txn_balance_amount total_amount, 
                        p.prefix_value prefix, 
                        t.txn_ref_number_char invoice_number, 
                        t.txn_description txn_description, 
                        t.txn_eway_bill_number txn_eway_bill_number, 
                        t.txn_display_name display_name, 
                        n.full_name party_name, 
                        n.phone_number party_phone, 
                        c.full_name category_name 
                    FROM kb_transactions t 
                    LEFT JOIN kb_names n ON t.txn_name_id = n.name_id 
                    LEFT JOIN kb_names c ON t.txn_category_id = c.name_id 
                    LEFT JOIN kb_prefix p ON t.txn_prefix_id = p.prefix_id
                ) txn 
                LEFT JOIN txn_payment_mapping pm ON txn.txn_id = pm.txn_id 
                GROUP BY txn.txn_id
            ) t1 
            LEFT JOIN (
                SELECT 
                    li.lineitem_id, 
                    li.lineitem_txn_id, 
                    (COALESCE(i.item_name, '') || ' ' || 
                    COALESCE(i.item_code,'') || ' ' || 
                    COALESCE(i.item_hsn_sac_code,'')|| ' ' || 
                    COALESCE(li.lineitem_batch_number,'')|| ' ' || 
                    COALESCE(li.lineitem_serial_number,'')|| ' ' || 
                    COALESCE(li.lineitem_count,'')|| ' ' || 
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
                        group_concat(sd.serial_number , ' ') sn 
                    FROM kb_lineitems l 
                    LEFT JOIN kb_serial_mapping sm ON l.lineitem_id = sm.serial_mapping_lineitem_id 
                    LEFT JOIN kb_serial_details sd ON sm.serial_mapping_serial_id = sd.serial_id 
                    GROUP BY l.lineitem_id
                ) li 
                LEFT JOIN kb_items i ON li.item_id = i.item_id
            ) l1 ON t1.txn_id = l1.lineitem_txn_id 
            GROUP BY t1.txn_id;
            """)

            conn.commit()
            
            # Verify only the expected tables were created
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kb_fts_vtable%'")
            fts_tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = {
                'kb_fts_vtable',
                'kb_fts_vtable_content',
                'kb_fts_vtable_segdir',
                'kb_fts_vtable_segments'
            }
            
            unexpected_tables = set(fts_tables) - expected_tables
            if unexpected_tables:
                for table in unexpected_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table};")
                conn.commit()
                self.status_text.insert(tk.END, f"✓ Removed unexpected FTS tables: {', '.join(unexpected_tables)}\n", 'success')
            
            # Verify FTS table was created
            cursor.execute("SELECT count(*) FROM kb_fts_vtable")
            count = cursor.fetchone()[0]
            self.status_text.insert(tk.END, f"✓ FTS table created with {count} records\n", 'success')
            self.status_text.insert(tk.END, f"✓ Created tables: {', '.join(expected_tables & set(fts_tables))}\n", 'success')
            
        except sqlite3.Error as e:
            conn.rollback()
            self.status_text.insert(tk.END, f"✗ Error creating FTS table: {str(e)}\n", 'error')
            raise e
        finally:
            conn.close()

    def process_file_with_fts(self, input_file):
        temp_dir = "temp_fts"
        os.makedirs(temp_dir, exist_ok=True)

        try:
            # Generate output file name
            input_filename = os.path.basename(input_file)
            output_filename = f"FTS_{input_filename}"
            output_vyp = os.path.join(temp_dir, output_filename.replace(".vyb", ".vyp"))
            output_vyb = os.path.join(temp_dir, output_filename.replace(".vyp", ".vyb"))

            # If input is .vyb, unzip it to get .vyp
            if input_file.endswith(".vyb"):
                self.progress_bar["value"] = 20
                self.update()
                self.unzip_vyb(input_file, temp_dir)
                # Find the extracted .vyp file in the temp directory
                extracted_files = os.listdir(temp_dir)
                vyp_file = None
                for file in extracted_files:
                    if file.endswith(".vyp"):
                        vyp_file = os.path.join(temp_dir, file)
                        break
                if not vyp_file:
                    raise ValueError("No .vyp file found in the extracted .vyb archive.")
            else:
                # If input is .vyp, use it directly
                vyp_file = input_file

            self.progress_bar["value"] = 40
            self.update()

            # Create the FTS table
            self.create_fts_table(vyp_file, output_vyp)

            self.progress_bar["value"] = 60
            self.update()

            # Always generate both .vyp and .vyb files
            # Zip the .vyp file into .vyb
            self.zip_vyp(output_vyp, output_vyb)

            self.progress_bar["value"] = 100
            self.update()
            self.status_text.insert(tk.END, f"✓ Output .vyp file with FTS table generated: {output_vyp}\n", 'success')
            self.status_text.insert(tk.END, f"✓ Output .vyb file with FTS table generated: {output_vyb}\n", 'success')

            # Enable download buttons if the output files exist
            if os.path.exists(output_vyp) and os.path.exists(output_vyb):
                self.download_vyb_button.config(state=NORMAL)
                self.download_vyp_button.config(state=NORMAL)
        except Exception as e:
            self.status_text.insert(tk.END, f"✗ Error: {str(e)}\n", 'error')
            self.progress_bar["value"] = 0

    def browse_input_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("VYB & VYP Files", "*.vyb *.vyp")])
        self.input_file_entry.delete(0, tk.END)
        self.input_file_entry.insert(0, file_path)

    def execute_fts_creation(self):
        input_file = self.input_file_entry.get()

        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: Please select an input file first\n", 'error')
            return

        self.progress_bar["value"] = 0
        Thread(target=self.process_file_with_fts, args=(input_file,), daemon=True).start()

    def download_vyp(self):
        input_file = self.input_file_entry.get()
        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: No input file selected\n", 'error')
            return

        # Look for FTS .vyp files
        input_filename = os.path.basename(input_file)
        possible_files = [f for f in os.listdir("temp_fts") 
                         if f.startswith(f"FTS_{os.path.splitext(input_filename)[0]}") 
                         and f.endswith(".vyp")]
        
        if not possible_files:
            self.status_text.insert(tk.END, f"✗ Error: No FTS .vyp output files found\n", 'error')
            return
        
        output_vyp = os.path.join("temp_fts", possible_files[0])
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".vyp", 
            filetypes=[("VYP Files", "*.vyp")], 
            initialfile=possible_files[0]
        )
        if file_path:
            shutil.copy(output_vyp, file_path)
            self.status_text.insert(tk.END, f"✓ File saved successfully: {file_path}\n", 'success')

    def download_vyb(self):
        input_file = self.input_file_entry.get()
        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: No input file selected\n", 'error')
            return

        # Look for FTS .vyb files
        input_filename = os.path.basename(input_file)
        possible_files = [f for f in os.listdir("temp_fts") 
                         if f.startswith(f"FTS_{os.path.splitext(input_filename)[0]}") 
                         and f.endswith(".vyb")]
        
        if not possible_files:
            self.status_text.insert(tk.END, f"✗ Error: No FTS .vyb output files found\n", 'error')
            return
        
        output_vyb = os.path.join("temp_fts", possible_files[0])
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".vyb", 
            filetypes=[("VYB Files", "*.vyb")], 
            initialfile=possible_files[0]
        )
        if file_path:
            shutil.copy(output_vyb, file_path)
            self.status_text.insert(tk.END, f"✓ File saved successfully: {file_path}\n", 'success')

    def cleanup_temp_dir(self):
        if os.path.exists("temp_fts"):
            shutil.rmtree("temp_fts", ignore_errors=True)


#--------------Setting Tab -------------------
# ----------------- Settings Tab -----------------
class SettingsTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.style = ttkb.Style()
        self.create_widgets()
        self.setup_theme_colors()
        
        # Register cleanup function
        atexit.register(self.cleanup_temp_dir)
    
    def setup_theme_colors(self):
        """Set color tags based on current theme"""
        if self.style.theme_use() == "cyborg":
            # Dark theme colors
            self.status_text.tag_config('error', foreground='#FF6B6B')
            self.status_text.tag_config('success', foreground='#7CFC00')
            self.status_text.tag_config('warning', foreground='#FFA040')
            self.status_text.configure(bg='#222222', fg='#FFFFFF')
        else:
            # Light theme colors
            self.status_text.tag_config('error', foreground='red')
            self.status_text.tag_config('success', foreground='green')
            self.status_text.tag_config('warning', foreground='orange')
            self.status_text.configure(bg='#FFFFFF', fg='#000000')
    
    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Header frame
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(pady=20)
        
        # Header with icon
        ttk.Label(
            header_frame, 
            text="🔧 Setting Table Repair Utility", 
            font=('Helvetica', 20, 'bold'),
            bootstyle="primary"
        ).pack()
        
        # Input file selection
        input_frame = ttk.LabelFrame(main_frame, text="📁 Select Input File (.vyb or .vyp)", bootstyle="info")
        input_frame.pack(pady=10, padx=20, fill=tk.X)
        
        self.input_file_entry = ttk.Entry(input_frame, font=('Helvetica', 12, 'bold'))
        self.input_file_entry.pack(side=tk.LEFT, padx=5, pady=5, expand=True, fill=tk.X)
        
        browse_button = ttk.Button(
            input_frame, 
            text="📂 Browse", 
            command=self.browse_input_file, 
            bootstyle="info",
            width=10
        )
        browse_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # Information text
        info_frame = ttk.LabelFrame(main_frame, text="ℹ️ About Settings Table Repair", bootstyle="info")
        info_frame.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        
        info_text = scrolledtext.ScrolledText(
            info_frame, 
            height=8, 
            font=('Helvetica', 11), 
            wrap=tk.WORD,
            padx=5,
            pady=5
        )
        info_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        info_text.insert(tk.END, """
This utility repairs the kb_settings table in Vyapar databases by:

1. Exporting all existing settings data (setting_id, setting_key, setting_value)
2. Dropping the potentially corrupted kb_settings table
3. Recreating the table with proper schema and constraints
4. Reinserting the settings data while resolving any duplicates

Key Features:
- Ensures no duplicates in both setting_id and setting_key
- Preserves all original data where possible
- Generates new IDs for duplicate setting_ids
**Note: This is a non-destructive operation - original data is preserved in the output file.
""")
        info_text.config(state=tk.DISABLED)
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=10)
        
        repair_button = ttk.Button(
            button_frame, 
            text="🔧 Repair Settings Table", 
            command=self.execute_repair, 
            bootstyle="success",
            width=25
        )
        repair_button.pack(side=tk.LEFT, padx=10)
        
        # Progress bar
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(pady=10, padx=20, fill=tk.X)
        
        self.progress_bar = ttk.Progressbar(
            progress_frame, 
            orient="horizontal", 
            mode="determinate",
            bootstyle="success striped"
        )
        self.progress_bar.pack(fill=tk.X)
        
        # Status text with custom tags
        status_frame = ttk.LabelFrame(main_frame, text="📋 Operation Log", bootstyle="info")
        status_frame.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        
        self.status_text = scrolledtext.ScrolledText(
            status_frame, 
            height=10, 
            font=('Helvetica', 12), 
            wrap=tk.WORD,
            padx=5,
            pady=5
        )
        self.status_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Configure text tags for styling
        self.setup_theme_colors()
        
        # Download buttons
        download_frame = ttk.Frame(main_frame)
        download_frame.pack(pady=10, padx=20)
        
        self.download_vyb_button = ttk.Button(
            download_frame, 
            text="⬇️ Download .vyb", 
            state=DISABLED, 
            command=self.download_vyb, 
            bootstyle="primary",
            width=20
        )
        self.download_vyb_button.pack(side=tk.LEFT, padx=5)
        
        self.download_vyp_button = ttk.Button(
            download_frame, 
            text="⬇️ Download .vyp", 
            state=DISABLED, 
            command=self.download_vyp, 
            bootstyle="primary",
            width=20
        )
        self.download_vyp_button.pack(side=tk.LEFT, padx=5)
    
    def unzip_vyb(self, vyb_file, extract_to):
        with zipfile.ZipFile(vyb_file, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def zip_vyp(self, vyp_file, vyb_file):
        with zipfile.ZipFile(vyb_file, 'w') as zip_ref:
            zip_ref.write(vyp_file, os.path.basename(vyp_file))

    def thread_safe_status(self, msg, tag=None):
        self.after(0, lambda: self.status_text.insert('end', msg + '\n', tag))

    def thread_safe_progress(self, value):
        self.after(0, lambda: self.progress_bar.config(value=value))

    def repair_settings_table(self, input_db, output_db):
        conn = None
        try:
            self.thread_safe_progress(10)
            self.thread_safe_status("Opening database...")
            # Copy the input database to the output database
            shutil.copy(input_db, output_db)

            # Connect to the output database
            conn = sqlite3.connect(output_db)
            cursor = conn.cursor()

            # Enable foreign key checks
            cursor.execute("PRAGMA foreign_keys = ON;")

            # Step 1: Check if kb_settings table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kb_settings'")
            if not cursor.fetchone():
                raise ValueError("kb_settings table does not exist in the database")

            # Step 2: Get the current schema of kb_settings table
            cursor.execute("PRAGMA table_info(kb_settings)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'setting_id' not in columns or 'setting_key' not in columns or 'setting_value' not in columns:
                raise ValueError("kb_settings table doesn't have the expected columns (setting_id, setting_key, setting_value)")

            # Step 3: Export all data from kb_settings table
            cursor.execute("SELECT setting_id, setting_key, setting_value FROM kb_settings")
            settings_data = cursor.fetchall()
            
            if not settings_data:
                self.status_text.insert(tk.END, "✓ kb_settings table is empty - no repair needed\n")
                return

            # Step 4: Find and log duplicate setting_ids and setting_keys
            setting_ids = {}
            setting_keys = {}
            duplicates = {'id': [], 'key': []}
            
            for row in settings_data:
                setting_id, setting_key, _ = row
                if setting_id in setting_ids:
                    duplicates['id'].append(setting_id)
                else:
                    setting_ids[setting_id] = True
                    
                if setting_key in setting_keys:
                    duplicates['key'].append(setting_key)
                else:
                    setting_keys[setting_key] = True
            
            if duplicates['id']:
                self.status_text.insert(tk.END, f"⚠ Found {len(duplicates['id'])} duplicate setting_ids\n", 'warning')
            if duplicates['key']:
                self.status_text.insert(tk.END, f"⚠ Found {len(duplicates['key'])} duplicate setting_keys\n", 'warning')

            # Step 5: Drop the kb_settings table
            cursor.execute("DROP TABLE IF EXISTS kb_settings")
            
            # Step 6: Recreate the kb_settings table with proper schema and constraints
            cursor.execute("""
            CREATE TABLE kb_settings (
                setting_id INTEGER PRIMARY KEY,
                setting_key TEXT UNIQUE,
                setting_value TEXT
            )""")

            # Step 7: Insert the exported data back, handling duplicates
            inserted_count = 0
            updated_count = 0
            skipped_count = 0
            
            for row in settings_data:
                setting_id, setting_key, setting_value = row
                
                # First try to insert with original setting_id
                try:
                    cursor.execute("""
                    INSERT INTO kb_settings (setting_id, setting_key, setting_value)
                    VALUES (?, ?, ?)
                    """, (setting_id, setting_key, setting_value))
                    inserted_count += 1
                except sqlite3.IntegrityError as e:
                    # If duplicate setting_id, let SQLite auto-generate a new one
                    if "setting_id" in str(e):
                        try:
                            cursor.execute("""
                            INSERT INTO kb_settings (setting_key, setting_value)
                            VALUES (?, ?)
                            """, (setting_key, setting_value))
                            inserted_count += 1
                        except sqlite3.IntegrityError:
                            # If duplicate key, update the existing record
                            cursor.execute("""
                            UPDATE kb_settings 
                            SET setting_value = ?
                            WHERE setting_key = ?
                            """, (setting_value, setting_key))
                            updated_count += 1
                    # If duplicate setting_key, update the existing record
                    elif "setting_key" in str(e):
                        cursor.execute("""
                        UPDATE kb_settings 
                        SET setting_value = ?
                        WHERE setting_key = ?
                        """, (setting_value, setting_key))
                        updated_count += 1
                    else:
                        skipped_count += 1

            conn.commit()
            
            # Verify the repair
            cursor.execute("SELECT COUNT(*) FROM kb_settings")
            final_count = cursor.fetchone()[0]
            
            self.status_text.insert(tk.END, f"✓ kb_settings table repaired successfully\n", 'success')
            self.status_text.insert(tk.END, f"✓ Original records: {len(settings_data)}\n")
            self.status_text.insert(tk.END, f"✓ Final records: {final_count}\n")
            self.status_text.insert(tk.END, f"✓ Records inserted: {inserted_count}\n")
            self.status_text.insert(tk.END, f"✓ Records updated: {updated_count}\n")
            if skipped_count > 0:
                self.status_text.insert(tk.END, f"⚠ Records skipped: {skipped_count}\n", 'warning')
            
        except sqlite3.Error as e:
            conn.rollback()
            self.status_text.insert(tk.END, f"✗ Error repairing kb_settings table: {str(e)}\n", 'error')
            raise e
        except ValueError as e:
            self.status_text.insert(tk.END, f"✗ {str(e)}\n", 'error')
            raise e
        finally:
            conn.close()

    def process_file(self, input_file):
        temp_dir = "temp_settings"
        os.makedirs(temp_dir, exist_ok=True)

        try:
            # Generate output file name
            input_filename = os.path.basename(input_file)
            output_filename = f"REPAIRED_{input_filename}"
            output_vyp = os.path.join(temp_dir, output_filename.replace(".vyb", ".vyp"))
            output_vyb = os.path.join(temp_dir, output_filename.replace(".vyp", ".vyb"))

            # If input is .vyb, unzip it to get .vyp
            if input_file.endswith(".vyb"):
                self.progress_bar["value"] = 20
                self.update()
                self.unzip_vyb(input_file, temp_dir)
                # Find the extracted .vyp file in the temp directory
                extracted_files = os.listdir(temp_dir)
                vyp_file = None
                for file in extracted_files:
                    if file.endswith(".vyp"):
                        vyp_file = os.path.join(temp_dir, file)
                        break
                if not vyp_file:
                    raise ValueError("No .vyp file found in the extracted .vyb archive.")
            else:
                # If input is .vyp, use it directly
                vyp_file = input_file

            self.progress_bar["value"] = 40
            self.update()

            # Repair the settings table
            self.repair_settings_table(vyp_file, output_vyp)

            self.progress_bar["value"] = 60
            self.update()

            # Always generate both .vyp and .vyb files
            # Zip the .vyp file into .vyb
            self.zip_vyp(output_vyp, output_vyb)

            self.progress_bar["value"] = 100
            self.update()
            self.status_text.insert(tk.END, f"✓ Output .vyp file with repaired settings table: {output_vyp}\n", 'success')
            self.status_text.insert(tk.END, f"✓ Output .vyb file with repaired settings table: {output_vyb}\n", 'success')

            # Enable download buttons if the output files exist
            if os.path.exists(output_vyp) and os.path.exists(output_vyb):
                self.download_vyb_button.config(state=NORMAL)
                self.download_vyp_button.config(state=NORMAL)
        except Exception as e:
            self.status_text.insert(tk.END, f"✗ Error: {str(e)}\n", 'error')
            self.progress_bar["value"] = 0

    def browse_input_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("VYB & VYP Files", "*.vyb *.vyp")])
        self.input_file_entry.delete(0, tk.END)
        self.input_file_entry.insert(0, file_path)

    def execute_repair(self):
        input_file = self.input_file_entry.get()

        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: Please select an input file first\n", 'error')
            return

        self.progress_bar["value"] = 0
        Thread(target=self.process_file, args=(input_file,), daemon=True).start()

    def download_vyp(self):
        input_file = self.input_file_entry.get()
        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: No input file selected\n", 'error')
            return

        # Look for repaired .vyp files
        input_filename = os.path.basename(input_file)
        possible_files = [f for f in os.listdir("temp_settings") 
                         if f.startswith(f"REPAIRED_{os.path.splitext(input_filename)[0]}") 
                         and f.endswith(".vyp")]
        
        if not possible_files:
            self.status_text.insert(tk.END, f"✗ Error: No repaired .vyp output files found\n", 'error')
            return
        
        output_vyp = os.path.join("temp_settings", possible_files[0])
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".vyp", 
            filetypes=[("VYP Files", "*.vyp")], 
            initialfile=possible_files[0]
        )
        if file_path:
            shutil.copy(output_vyp, file_path)
            self.status_text.insert(tk.END, f"✓ File saved successfully: {file_path}\n", 'success')

    def download_vyb(self):
        input_file = self.input_file_entry.get()
        if not input_file:
            self.status_text.insert(tk.END, "✗ Error: No input file selected\n", 'error')
            return

        # Look for repaired .vyb files
        input_filename = os.path.basename(input_file)
        possible_files = [f for f in os.listdir("temp_settings") 
                         if f.startswith(f"REPAIRED_{os.path.splitext(input_filename)[0]}") 
                         and f.endswith(".vyb")]
        
        if not possible_files:
            self.status_text.insert(tk.END, f"✗ Error: No repaired .vyb output files found\n", 'error')
            return
        
        output_vyb = os.path.join("temp_settings", possible_files[0])
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".vyb", 
            filetypes=[("VYB Files", "*.vyb")], 
            initialfile=possible_files[0]
        )
        if file_path:
            shutil.copy(output_vyb, file_path)
            self.status_text.insert(tk.END, f"✓ File saved successfully: {file_path}\n", 'success')

    def cleanup_temp_dir(self):
        if os.path.exists("temp_settings"):
            shutil.rmtree("temp_settings", ignore_errors=True)


# ----------------- Main Application -----------------
class DatabaseToolApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Vyapar Database Utility Tool")
        # self.root.iconbitmap("icon.ico")
        self.root.geometry("1200x850")

        
        # Configure styles
        self.style = ttkb.Style(theme="cyborg")
        self.current_theme = "cyborg"
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Create tabs
        self.home_tab = HomeTab(self.notebook)
        self.comparison_tab = DatabaseComparisonTab(self.notebook)
        self.sanitizer_tab = DatabaseSanitizerTab(self.notebook)
        self.fts_tab = FTSTab(self.notebook)
        self.settings_tab = SettingsTab(self.notebook)
        
        # Add tabs to notebook
        self.notebook.add(self.home_tab, text="🏠 Home")
        self.notebook.add(self.comparison_tab, text="🔍 Database Comparison")
        self.notebook.add(self.sanitizer_tab, text="🧹 Database Sanitization")
        self.notebook.add(self.fts_tab, text="⚙️ FTS Table Generator")
        self.notebook.add(self.settings_tab, text="⚙️ Setting Table Repair")
        
        self.notebook.select(self.home_tab)  # Optional: select Home tab by default

        #Theme toggle button
        self.theme_btn = ttk.Button(
            root, 
            text="☀️" if self.current_theme == "cyborg" else "🌙",
            command=self.toggle_theme,
            width=3,
            bootstyle="primary"
        )
        self.theme_btn.pack(side=tk.RIGHT, padx=5, pady=5)
    
    def toggle_theme(self):
        """Switch between dark and light themes"""
        if self.current_theme == "cyborg":
            self.current_theme = "pulse"
        else:
            self.current_theme = "cyborg"
        
        # Update the style
        self.style.theme_use(self.current_theme)
        self.theme_btn.config(text="☀️" if self.current_theme == "cyborg" else "🌙")
        
        # Update the tabs
        self.comparison_tab.current_theme = self.current_theme
        self.comparison_tab.style.theme_use(self.current_theme)
        self.comparison_tab.setup_theme_colors()
        self.sanitizer_tab.style.theme_use(self.current_theme)
        self.sanitizer_tab.setup_theme_colors()
        
        # Force refresh of the comparison results display
        if hasattr(self.comparison_tab, 'result_text'):
            current_content = self.comparison_tab.result_text.get("1.0", tk.END)
            self.comparison_tab.result_text.delete("1.0", tk.END)
            self.comparison_tab.result_text.insert(tk.END, current_content)

def main():
    root = ttkb.Window(themename="cyborg")
    app = DatabaseToolApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()