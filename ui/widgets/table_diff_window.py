"""
ui/widgets/table_diff_window.py
Per-table detailed diff window (Toplevel).
Opens cleanly when user clicks "View →" in the DiffViewer summary table.
"""
import tkinter as tk
from tkinter import ttk
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.comparator import TableDataResult

# ── Colour constants (always dark — window has its own palette) ────────────
_BG       = '#1a1b2e'
_PANEL    = '#16213e'
_CARD     = '#0f3460'
_FG       = '#e2e8f0'
_SUBTEXT  = '#94a3b8'
_ADDED    = '#22c55e'
_REMOVED  = '#ef4444'
_MODIFIED = '#f97316'
_SCHEMA1  = '#f59e0b'   # amber  — DB1-only columns
_SCHEMA2  = '#a78bfa'   # violet — DB2-only columns
_NOTE     = '#64748b'
_ACCENT   = '#60a5fa'
_BORDER   = '#1e293b'


def _make_tag_tree(tree: ttk.Treeview):
    """Apply standard row tags to a Treeview."""
    tree.tag_configure('added',      foreground=_ADDED)
    tree.tag_configure('removed',    foreground=_REMOVED)
    tree.tag_configure('modified',   foreground=_MODIFIED)
    tree.tag_configure('schema_db1', foreground=_SCHEMA1, font=('Consolas', 10, 'bold'))
    tree.tag_configure('schema_db2', foreground=_SCHEMA2, font=('Consolas', 10, 'bold'))
    tree.tag_configure('note',       foreground=_NOTE,    font=('Consolas', 10, 'italic'))
    tree.tag_configure('pk',         foreground=_ACCENT,  font=('Consolas', 10, 'bold'))


class TableDiffWindow(tk.Toplevel):
    """
    Detailed diff for a single table — opens as a modal-ish Toplevel.
    Shows schema diff banner, then tabs: Modified | DB1 Only | DB2 Only.
    """

    def __init__(self, parent, tdata: 'TableDataResult'):
        super().__init__(parent)
        self.title(f"Table Diff  ›  {tdata.table}")
        self.geometry("1100x650")
        self.minsize(800, 500)
        self.configure(bg=_BG)
        # Removed transient to keep OS minimize buttons active
        self.state('zoomed')
        
        # Force window to the front
        self.attributes('-topmost', True)
        self.after(100, lambda: self.attributes('-topmost', False))
        
        self.grab_set()
        self.lift()
        self.focus_force()

        self._tdata = tdata
        self._build(tdata)

    # ------------------------------------------------------------------

    def _build(self, tdata: 'TableDataResult'):
        # ── Header bar ───────────────────────────────────────────────
        hdr = tk.Frame(self, bg=_CARD, padx=16, pady=10)
        hdr.pack(fill=tk.X)

        tk.Label(hdr, text=f"📋  {tdata.table}", bg=_CARD, fg=_FG,
                 font=('Segoe UI', 15, 'bold')).pack(side=tk.LEFT)

        # Summary chips
        chips = []
        if tdata.schema_differs:
            chips.append(("⚠ Schema differs", _SCHEMA1))
        n_mod = len(tdata.modified_rows)
        n_add = len(tdata.rows_only_in_db2)
        n_rem = len(tdata.rows_only_in_db1)
        if n_mod: chips.append((f"~ {n_mod} modified", _MODIFIED))
        if n_add: chips.append((f"+ {n_add} added",    _ADDED))
        if n_rem: chips.append((f"- {n_rem} removed",  _REMOVED))
        if not chips:
            chips.append(("✓ No row differences", _ADDED))

        for text, colour in chips:
            chip = tk.Frame(hdr, bg=colour, padx=8, pady=3)
            chip.pack(side=tk.RIGHT, padx=(4, 0))
            tk.Label(chip, text=text, bg=colour, fg='#000000',
                     font=('Segoe UI', 10, 'bold')).pack()

        # ── Schema banner (only if schema differs) ───────────────────
        if tdata.schema_differs:
            sd = tdata.column_schema_diff
            sb = tk.Frame(self, bg=_PANEL, padx=12, pady=8)
            sb.pack(fill=tk.X)

            tk.Label(sb, text="Schema Differences", bg=_PANEL, fg=_ACCENT,
                     font=('Segoe UI', 11, 'bold')).pack(anchor='w')

            row_f = tk.Frame(sb, bg=_PANEL)
            row_f.pack(fill=tk.X, pady=(4, 0))

            if sd.only_in_db1:
                db1_f = tk.Frame(row_f, bg='#1c1200', padx=10, pady=6, bd=0)
                db1_f.pack(side=tk.LEFT, padx=(0, 8))
                tk.Label(db1_f, text="← Only in DB1  (missing in DB2)",
                         bg='#1c1200', fg=_SCHEMA1,
                         font=('Consolas', 10, 'bold')).pack(anchor='w')
                for col in sd.only_in_db1:
                    tk.Label(db1_f, text=f"  ✖  {col}",
                             bg='#1c1200', fg=_SCHEMA1,
                             font=('Consolas', 10)).pack(anchor='w')

            if sd.only_in_db2:
                db2_f = tk.Frame(row_f, bg='#110a1a', padx=10, pady=6, bd=0)
                db2_f.pack(side=tk.LEFT)
                tk.Label(db2_f, text="→ Only in DB2  (missing in DB1)",
                         bg='#110a1a', fg=_SCHEMA2,
                         font=('Consolas', 10, 'bold')).pack(anchor='w')
                for col in sd.only_in_db2:
                    tk.Label(db2_f, text=f"  ✚  {col}",
                             bg='#110a1a', fg=_SCHEMA2,
                             font=('Consolas', 10)).pack(anchor='w')

            if tdata.col_names:
                tk.Label(sb,
                         text=f"ℹ  Data comparison run on {len(tdata.col_names)} common column(s).",
                         bg=_PANEL, fg=_NOTE,
                         font=('Segoe UI', 9, 'italic')).pack(anchor='w', pady=(6, 0))

        # ── Notebook with 3 tabs ──────────────────────────────────────
        sep = tk.Frame(self, bg=_BORDER, height=1)
        sep.pack(fill=tk.X)

        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=12, pady=8)

        # Tab 1 — Modified rows
        tab_mod = tk.Frame(nb, bg=_BG)
        nb.add(tab_mod, text=f"  ~ Modified ({n_mod})  ")
        self._build_modified_tab(tab_mod, tdata)

        # Tab 2 — DB1 only
        tab_db1 = tk.Frame(nb, bg=_BG)
        nb.add(tab_db1, text=f"  - DB1 Only ({n_rem})  ")
        self._build_single_db_tab(tab_db1, tdata.rows_only_in_db1,
                                   tdata.col_names, 'removed', "DB1 Only")

        # Tab 3 — DB2 only
        tab_db2 = tk.Frame(nb, bg=_BG)
        nb.add(tab_db2, text=f"  + DB2 Only ({n_add})  ")
        self._build_single_db_tab(tab_db2, tdata.rows_only_in_db2,
                                   tdata.col_names, 'added', "DB2 Only")

        # Select most relevant tab
        if n_mod:
            nb.select(0)
        elif n_rem:
            nb.select(1)
        elif n_add:
            nb.select(2)

    # ------------------------------------------------------------------
    # Tab builders
    # ------------------------------------------------------------------

    def _build_modified_tab(self, parent: tk.Frame, tdata: 'TableDataResult'):
        """
        Shows modified rows.
        Columns: PK, Column Name, DB1 Value, DB2 Value
        """
        if not tdata.modified_rows:
            tk.Label(parent, text="No modified rows", bg=_BG, fg=_SUBTEXT,
                     font=('Segoe UI', 12)).pack(expand=True)
            return

        cols = ('pk', 'column', 'db1_value', 'db2_value')
        tree, _ = self._make_tree(parent, cols, {
            'pk':        ('  Primary Key',   200, True),
            'column':    (' |  Column',      150, True),
            'db1_value': (' |  DB1 Value',   300, True),
            'db2_value': (' |  DB2 Value',   300, True),
        })
        _make_tag_tree(tree)

        for mod in tdata.modified_rows:
            pk_str = ', '.join(f'{k}={v}' for k, v in mod.pk.items())
            first = True
            for col_name, v1, v2 in mod.column_changes:
                pk_display = pk_str if first else ''
                first = False
                tree.insert('', tk.END,
                            values=(f" {pk_display}", f" |  {col_name}", f" |  {v1}", f" |  {v2}"),
                            tags=('modified',))
            # Blank separator between rows
            tree.insert('', tk.END, values=('', '|', '|', '|'), tags=('note',))

    def _build_single_db_tab(self, parent: tk.Frame, rows: list,
                              col_names: list, tag: str, label: str):
        """Shows rows that exist in one DB only."""
        if not rows:
            tk.Label(parent, text=f"No {label} rows", bg=_BG, fg=_SUBTEXT,
                     font=('Segoe UI', 12)).pack(expand=True)
            return

        # Use col_names, but strip internal _pk key
        display_cols = [c for c in col_names if c != '_pk']
        if not display_cols and rows:
            display_cols = [k for k in rows[0] if k != '_pk']

        col_defs = {c: (f'  {c}' if i == 0 else f' |  {c}', max(80, min(200, len(c) * 10)), True)
                    for i, c in enumerate(display_cols)}
        tree, _ = self._make_tree(parent, display_cols, col_defs)
        _make_tag_tree(tree)

        for row_dict in rows:
            vals = tuple(f"  {row_dict.get(c, '')}" if i == 0 else f" |  {row_dict.get(c, '')}" 
                         for i, c in enumerate(display_cols))
            tree.insert('', tk.END, values=vals, tags=(tag,))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_tree(self, parent: tk.Frame, cols: tuple | list,
                   headings: dict) -> tuple:
        frame = tk.Frame(parent, bg=_BG)
        frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        tree = ttk.Treeview(frame, columns=cols, show='headings',
                            selectmode='browse')
        for col in cols:
            text, width, stretch = headings.get(col, (col, 120, False))
            tree.heading(col, text=text, anchor='w')
            tree.column(col, width=width, stretch=stretch,
                        minwidth=60, anchor='w')

        vsb = ttk.Scrollbar(frame, orient=tk.VERTICAL,   command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid (row=0, column=1, sticky='ns')
        hsb.grid (row=1, column=0, sticky='ew')

        return tree, frame
