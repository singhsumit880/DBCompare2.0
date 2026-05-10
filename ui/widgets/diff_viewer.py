"""
ui/widgets/diff_viewer.py
Complete redesign — clean three-card layout using native ttk styles.

Panel 1 ─ Version status bar (single line)
Panel 2 ─ Schema changes (ttk.Treeview tree, collapsible)
Panel 3 ─ Data changes summary table (one row per table with counts)
           → "View →" button opens TableDiffWindow
"""
import tkinter as tk
from tkinter import ttk
from typing import Optional

import ttkbootstrap as ttkb
from ttkbootstrap.constants import *

from core.comparator import ComparisonReport, TableDataResult

class DiffViewer(ttk.Frame):
    """
    Three-section structured diff display.
    Never puts all diff data in one flat scrollable list.
    """

    def __init__(self, parent, theme: str = 'superhero', **kwargs):
        super().__init__(parent, **kwargs)
        self._theme = theme
        self._report: Optional[ComparisonReport] = None
        self._open_windows: list = []   # track opened TableDiffWindows
        
        # We fetch exact hex colors from ttkbootstrap style for text tags
        # so they adapt dynamically to light/dark themes
        # We will dynamically pull these in `_apply_palette`
        self._style = ttkb.Style()
        
        self._build()
        self._apply_palette()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render(self, report: ComparisonReport) -> None:
        self._report = report
        self._render_version(report)
        self._render_schema(report)
        self._render_data_summary(report)

    def clear(self) -> None:
        # Close any open detail windows
        for w in self._open_windows:
            try:
                w.destroy()
            except Exception:
                pass
        self._open_windows = []
        self._version_lbl.config(text='')
        self._schema_tree.delete(*self._schema_tree.get_children())
        for row in self._summary_tree.get_children():
            self._summary_tree.delete(row)
        self._schema_count_var.set('')
        self._data_count_var.set('')

    def set_theme(self, theme: str) -> None:
        self._theme = theme
        self._apply_palette()
        if self._report:
            self.render(self._report)

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def _build(self):
        # ── SECTION 1: Version status bar ─────────────────────────────
        self._ver_frame = ttk.Frame(self, padding=(16, 10))
        self._ver_frame.pack(fill=tk.X, padx=10, pady=(10, 4))
        
        self._version_lbl = ttk.Label(self._ver_frame, text='', font=('Segoe UI', 12, 'bold'))
        self._version_lbl.pack(fill=tk.X)

        # ── SECTION 2: Schema changes ──────────────────────────────────
        s2_header = ttk.Frame(self)
        s2_header.pack(fill=tk.X, padx=15, pady=(4, 0))

        ttk.Label(s2_header, text="Schema Changes",
                  font=('Segoe UI', 11, 'bold'),
                  bootstyle='primary').pack(side=tk.LEFT)

        self._schema_count_var = tk.StringVar()
        ttk.Label(s2_header, textvariable=self._schema_count_var,
                  font=('Segoe UI', 10), bootstyle='secondary').pack(side=tk.RIGHT)

        s2_card = ttk.Frame(self)
        s2_card.pack(fill=tk.X, padx=10, pady=(2, 6))

        self._schema_tree = ttk.Treeview(s2_card, show='tree', height=6,
                                          selectmode='none')
        s2_vsb = ttk.Scrollbar(s2_card, orient=tk.VERTICAL,
                                command=self._schema_tree.yview)
        self._schema_tree.configure(yscrollcommand=s2_vsb.set)
        s2_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self._schema_tree.pack(fill=tk.X, expand=True)

        # ── SECTION 3: Data changes summary ───────────────────────────
        s3_header = ttk.Frame(self)
        s3_header.pack(fill=tk.X, padx=15, pady=(4, 0))

        ttk.Label(s3_header, text="Data Changes  —  click a row to inspect",
                  font=('Segoe UI', 11, 'bold'),
                  bootstyle='info').pack(side=tk.LEFT)

        self._data_count_var = tk.StringVar()
        ttk.Label(s3_header, textvariable=self._data_count_var,
                  font=('Segoe UI', 10), bootstyle='secondary').pack(side=tk.RIGHT)

        s3_card = ttk.Frame(self)
        s3_card.pack(fill=tk.BOTH, expand=True, padx=10, pady=(2, 10))
        s3_card.rowconfigure(0, weight=1)
        s3_card.columnconfigure(0, weight=1)

        cols = ('table', 'schema', 'modified', 'db1_only', 'db2_only', 'action')
        self._summary_tree = ttk.Treeview(
            s3_card, columns=cols, show='headings',
            selectmode='browse',
        )
        # Wider columns for better alignment and readability
        _h = {
            'table':    ('  Table Name',       300, True),
            'schema':   ('  Schema Config',     120, True),
            'modified': ('  ~ Rows Modified',   100, True),
            'db1_only': ('  - DB1 Only',        100, True),
            'db2_only': ('  + DB2 Only',        100, True),
            'action':   (' View More Details ', 80,  True),
        }
        for col, (text, w, stretch) in _h.items():
            self._summary_tree.heading(col, text=text, anchor='w')
            self._summary_tree.column(col, width=w, stretch=stretch,
                                       minwidth=90, anchor='center')
        self._summary_tree.column('table', anchor='w')

        s3_vsb = ttk.Scrollbar(s3_card, orient=tk.VERTICAL,
                                command=self._summary_tree.yview)
        s3_hsb = ttk.Scrollbar(s3_card, orient=tk.HORIZONTAL,
                                command=self._summary_tree.xview)
        self._summary_tree.configure(yscrollcommand=s3_vsb.set,
                                      xscrollcommand=s3_hsb.set)
        self._summary_tree.grid(row=0, column=0, sticky='nsew')
        s3_vsb.grid(row=0, column=1, sticky='ns')
        s3_hsb.grid(row=1, column=0, sticky='ew')

        # Double-click or single-click on a row opens the detail window
        self._summary_tree.bind('<Double-1>',  self._on_row_activate)
        self._summary_tree.bind('<Return>',    self._on_row_activate)
        self._summary_tree.bind('<Button-1>',  self._on_single_click)

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render_version(self, report: ComparisonReport):
        v = report.version
        if v.differ:
            icon, bootstyle, status = '⚠', 'warning', 'VERSIONS DIFFER'
        else:
            icon, bootstyle, status = '✓', 'success', 'Versions match'

        text = (
            f"{icon}  DB1 Version:  {v.db1_version}   |   "
            f"DB2 Version:  {v.db2_version}   |   {status}"
        )
        self._version_lbl.configure(text=text, bootstyle=bootstyle)

    def _render_schema(self, report: ComparisonReport):
        self._schema_tree.delete(*self._schema_tree.get_children())
        s = report.schema

        n = len(s.added_tables) + len(s.removed_tables) + len(s.column_diffs)
        self._schema_count_var.set(f"{n} change(s)" if n else "No changes")

        if not s.has_differences:
            self._schema_tree.insert('', tk.END, text="  ✓  No schema differences",
                                      tags=('ok',))
            return

        if s.added_tables:
            parent = self._schema_tree.insert(
                '', tk.END, text=f"  [+] Tables added in DB2  ({len(s.added_tables)})",
                open=True, tags=('added',)
            )
            for t in s.added_tables:
                self._schema_tree.insert(parent, tk.END, text=f"    ✚  {t}",
                                          tags=('added',))

        if s.removed_tables:
            parent = self._schema_tree.insert(
                '', tk.END, text=f"  [-] Tables removed in DB2  ({len(s.removed_tables)})",
                open=True, tags=('removed',)
            )
            for t in s.removed_tables:
                self._schema_tree.insert(parent, tk.END, text=f"    ✖  {t}",
                                          tags=('removed',))

        if s.column_diffs:
            parent = self._schema_tree.insert(
                '', tk.END, text=f"  [~] Column changes  ({len(s.column_diffs)} table(s))",
                open=True, tags=('changed',)
            )
            for cd in s.column_diffs:
                tbl = self._schema_tree.insert(
                    parent, tk.END, text=f"    📋  {cd.table}", open=True,
                    tags=('table_label',)
                )
                for col in cd.only_in_db2:
                    self._schema_tree.insert(tbl, tk.END,
                                              text=f"      ✚  {col}  (added in DB2)",
                                              tags=('added',))
                for col in cd.only_in_db1:
                    self._schema_tree.insert(tbl, tk.END,
                                              text=f"      ✖  {col}  (removed in DB2)",
                                              tags=('removed',))

    def _render_data_summary(self, report: ComparisonReport):
        for row in self._summary_tree.get_children():
            self._summary_tree.delete(row)

        # Store tdata indexed by iid for click handler
        self._tdata_map: dict = {}

        affected = 0
        for tdata in report.data:
            n_mod = len(tdata.modified_rows)
            n_rem = len(tdata.rows_only_in_db1)
            n_add = len(tdata.rows_only_in_db2)
            schema_icon = '⚠ Differs' if tdata.schema_differs else '✓ Match'
            any_change = n_mod or n_rem or n_add or tdata.schema_differs

            if any_change:
                affected += 1

            tag = (
                'has_diff'    if (n_mod or n_rem or n_add) else
                'schema_only' if tdata.schema_differs else
                'clean'
            )
            action = '  View →  ' if any_change else ''

            iid = self._summary_tree.insert(
                '', tk.END,
                values=(
                    f"  {tdata.table}",
                    schema_icon,
                    n_mod or '—',
                    n_rem or '—',
                    n_add or '—',
                    action,
                ),
                tags=(tag,),
            )
            self._tdata_map[iid] = tdata

        total_row_changes = sum(
            len(t.rows_only_in_db1) + len(t.rows_only_in_db2) + len(t.modified_rows)
            for t in report.data
        )
        self._data_count_var.set(
            f"{affected} table(s) affected, {total_row_changes} row change(s)"
            if affected else "No data differences"
        )

    # ------------------------------------------------------------------
    # Interaction
    # ------------------------------------------------------------------

    def _on_single_click(self, event):
        """Open detail window when clicking the 'View →' action column."""
        col = self._summary_tree.identify_column(event.x)
        if col == '#6':   # 6th column = action
            iid = self._summary_tree.identify_row(event.y)
            if iid:
                self._open_detail(iid)

    def _on_row_activate(self, event):
        """Open detail window on double-click or Enter."""
        sel = self._summary_tree.selection()
        if sel:
            self._open_detail(sel[0])

    def _open_detail(self, iid: str):
        tdata = self._tdata_map.get(iid)
        if not tdata:
            return
        from ui.widgets.table_diff_window import TableDiffWindow
        win = TableDiffWindow(self.winfo_toplevel(), tdata)
        self._open_windows.append(win)

    # ------------------------------------------------------------------
    # Tags / palette
    # ------------------------------------------------------------------

    def _apply_palette(self):
        # Apply specific hex values referenced from db.py custom theming
        # Schema tree
        self._schema_tree.tag_configure('ok',          foreground='#90EE90')
        self._schema_tree.tag_configure('added',       foreground='#7CFC00')
        self._schema_tree.tag_configure('removed',     foreground='#FF6B6B')
        self._schema_tree.tag_configure('changed',     foreground='#FFD700')
        self._schema_tree.tag_configure('table_label', foreground='#00E5D2', font=('Consolas', 11, 'bold'))
        
        # Data changes summary
        self._summary_tree.tag_configure('has_diff',    foreground='#FFA040')
        self._summary_tree.tag_configure('schema_only', foreground='#B57DFF')
        self._summary_tree.tag_configure('clean',       foreground='#90EE90')
        self._summary_tree.tag_configure('ok',          foreground='#90EE90')
