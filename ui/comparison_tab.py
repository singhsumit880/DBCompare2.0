"""
ui/comparison_tab.py
Database Comparison tab — uses DiffViewer and FilePicker widgets.
Refactored layout to Horizontal top controls and Full-width bottom DiffViewer.
"""
import threading
import tkinter as tk
from tkinter import ttk, messagebox, font

import ttkbootstrap as ttkb
from ttkbootstrap.constants import *

from core.comparator import (
    DatabaseComparator, ComparisonReport,
    DEFAULT_EXCLUDED_TABLES, DEFAULT_DECIMAL_PRECISION,
)
from ui.widgets.file_picker import FilePicker
from ui.widgets.diff_viewer import DiffViewer


class ComparisonTab(ttk.Frame):
    def __init__(self, parent, style: ttkb.Style):
        super().__init__(parent)
        self._style = style
        self._comparator = DatabaseComparator()
        self._theme = style.theme_use()
        self._report: ComparisonReport | None = None
        self._build()

    # ------------------------------------------------------------------
    # UI Construction
    # ------------------------------------------------------------------

    def _build(self):
        # ── Top Controls Bar ──────────────────────────────────────────
        top_bar = ttk.Frame(self, padding=(10, 10, 10, 5))
        top_bar.pack(fill=tk.X)

        # 3-column layout for top controls
        top_bar.columnconfigure(0, weight=2)
        top_bar.columnconfigure(1, weight=1)
        top_bar.columnconfigure(2, weight=1)

        # Col 0: Files
        files_frame = ttk.LabelFrame(top_bar, text="  📁  Database Files  ", padding=10)
        files_frame.grid(row=0, column=0, sticky='nsew', padx=(0, 5))

        self._db1_picker = FilePicker(
            files_frame,
            label="Database 1:",
            filetypes=[("Database Files", "*.vyp *.vyb *.sqlite *.db *.zip"),
                       ("All Files", "*.*")],
            dialog_title="Select First Database",
        )
        self._db1_picker.pack(fill=tk.X, pady=(0, 4))

        self._db2_picker = FilePicker(
            files_frame,
            label="Database 2:",
            filetypes=[("Database Files", "*.vyp *.vyb *.sqlite *.db *.zip"),
                       ("All Files", "*.*")],
            dialog_title="Select Second Database",
        )
        self._db2_picker.pack(fill=tk.X)

        # Col 1: Filters
        filter_frame = ttk.LabelFrame(top_bar, text="  🔎  Table Filters  ", padding=10)
        filter_frame.grid(row=0, column=1, sticky='nsew', padx=5)
        filter_frame.columnconfigure(1, weight=1)

        ttk.Label(filter_frame, text="Include tables:").grid(row=0, column=0, sticky='w', padx=(0,4))
        self._include_entry = ttk.Entry(filter_frame)
        self._include_entry.grid(row=0, column=1, sticky='ew', pady=2)

        ttk.Label(filter_frame, text="Exclude tables:").grid(row=1, column=0, sticky='w', padx=(0,4))
        self._exclude_entry = ttk.Entry(filter_frame)
        self._exclude_entry.grid(row=1, column=1, sticky='ew', pady=2)
        self._exclude_entry.insert(0, ', '.join(sorted(DEFAULT_EXCLUDED_TABLES)))

        # Col 2: Options & Buttons
        opt_frame = ttk.LabelFrame(top_bar, text="  ⚙️  Options & Actions  ", padding=10)
        opt_frame.grid(row=0, column=2, sticky='nsew', padx=(5, 0))

        # Options Row (Checkboxes and Precision)
        row_f = ttk.Frame(opt_frame)
        row_f.pack(fill=tk.X, pady=(0, 6))

        self._validate_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            row_f, text="Validate DBs", variable=self._validate_var, bootstyle='round-toggle'
        ).pack(side=tk.LEFT, padx=(0, 10))
        
        self._datetime_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            row_f, text="Ignore dates", variable=self._datetime_var, bootstyle='round-toggle'
        ).pack(side=tk.LEFT, padx=(0, 15))

        ttk.Label(row_f, text="Decimal precision:").pack(side=tk.LEFT, padx=(0,6))
        self._precision_var = tk.StringVar(value=str(DEFAULT_DECIMAL_PRECISION))
        ttk.Spinbox(row_f, from_=0, to=15, textvariable=self._precision_var, width=5).pack(side=tk.LEFT)

        # Buttons
        btn_frame = ttk.Frame(opt_frame)
        btn_frame.pack(fill=tk.X, pady=(4, 0))
        self._compare_btn = ttk.Button(btn_frame, text="🔍 Compare", command=self._run_comparison, bootstyle='primary')
        self._compare_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 2))
        self._reset_btn = ttk.Button(btn_frame, text="🔄 Reset", command=self._reset, bootstyle='warning-outline')
        self._reset_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(2, 0))

        # ── Status Bar ───────────────────────────────────────────────
        status_bar = ttk.Frame(self, padding=(10, 0, 10, 10))
        status_bar.pack(fill=tk.X)
        
        self._status_var = tk.StringVar(value="Ready")
        ttk.Label(status_bar, textvariable=self._status_var, font=('Segoe UI', 10, 'italic')).pack(side=tk.LEFT)
        
        self._progress_var = tk.DoubleVar()
        self._progress_bar = ttk.Progressbar(status_bar, variable=self._progress_var, maximum=100, bootstyle='striped')
        self._progress_bar.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(10, 0))

        # ── Diff Viewer (Full Width) ──────────────────────────────────
        self._diff_viewer = DiffViewer(self, theme=self._theme)
        self._diff_viewer.pack(fill=tk.BOTH, expand=True)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _reset(self):
        self._comparator = DatabaseComparator()
        self._report = None
        self._db1_picker.clear()
        self._db2_picker.clear()
        self._include_entry.delete(0, tk.END)
        self._exclude_entry.delete(0, tk.END)
        self._exclude_entry.insert(0, ', '.join(sorted(DEFAULT_EXCLUDED_TABLES)))
        self._validate_var.set(True)
        self._datetime_var.set(True)
        self._precision_var.set(str(DEFAULT_DECIMAL_PRECISION))
        self._progress_var.set(0)
        self._status_var.set("Ready")
        self._diff_viewer.clear()

    def _run_comparison(self):
        db1 = self._db1_picker.get()
        db2 = self._db2_picker.get()
        if not db1 or not db2:
            messagebox.showerror("Error", "Please select both database files.")
            return

        include_text = self._include_entry.get().strip()
        included = {t.strip() for t in include_text.split(',') if t.strip()} if include_text else set()
        exclude_text = self._exclude_entry.get().strip()
        excluded = {t.strip() for t in exclude_text.split(',') if t.strip()} if exclude_text else set()

        try:
            precision = int(self._precision_var.get())
            if not 0 <= precision <= 15:
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", "Decimal precision must be between 0 and 15.")
            return

        self._compare_btn.config(state='disabled')
        self._reset_btn.config(state='disabled')
        self._diff_viewer.clear()
        self._progress_var.set(0)
        self._status_var.set("Starting comparison…")

        # Clear carry-over diff data
        self._comparator.visual_diff_data_reset = True

        threading.Thread(
            target=self._comparison_worker,
            args=(db1, db2, included, excluded,
                  self._datetime_var.get(), precision, self._validate_var.get()),
            daemon=True,
        ).start()

    def _comparison_worker(self, db1, db2, included, excluded,
                           ignore_dt, precision, validate):
        try:
            self._comparator.set_progress(self._progress_cb)
            report = self._comparator.compare(
                db1, db2, included, excluded, ignore_dt, precision, validate
            )
            self.after(0, self._show_results, report)
        except Exception as e:
            self.after(0, self._show_error, str(e))
        finally:
            self.after(0, lambda: self._compare_btn.config(state='normal'))
            self.after(0, lambda: self._reset_btn.config(state='normal'))
            self.after(0, lambda: self._progress_var.set(100))

    def _progress_cb(self, msg: str, pct: int | None):
        self.after(0, lambda m=msg, p=pct: self._update_progress(m, p))

    def _update_progress(self, msg: str, pct: int | None):
        self._status_var.set(msg)
        if pct is not None:
            self._progress_var.set(pct)

    def _show_results(self, report: ComparisonReport):
        self._report = report
        self._diff_viewer.render(report)
        if report.has_any_differences:
            self._status_var.set("Comparison complete — differences found")
        else:
            self._status_var.set("Comparison complete — databases are identical")

    def _show_error(self, msg: str):
        messagebox.showerror("Comparison Error", msg)
        self._status_var.set(f"Error: {msg[:80]}")

    # ------------------------------------------------------------------
    # Theme
    # ------------------------------------------------------------------

    def apply_theme(self, theme: str):
        self._theme = theme
        self._diff_viewer.set_theme(theme)
