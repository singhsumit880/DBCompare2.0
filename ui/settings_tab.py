"""
ui/settings_tab.py
Settings Table Repair tab.
"""
import os
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog

import ttkbootstrap as ttkb
from ttkbootstrap.constants import *

from core.settings_repair import process_settings_repair
from ui.widgets.file_picker import FilePicker


class SettingsTab(ttk.Frame):
    def __init__(self, parent, style: ttkb.Style):
        super().__init__(parent)
        self._style = style
        self._out_vyp: str | None = None
        self._out_vyb: str | None = None
        self._build()
        self._apply_theme_colors()

    def _build(self):
        main = ttk.Frame(self, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        ttk.Label(main, text="🔧 Settings Table Repair Utility",
                  font=('Segoe UI', 18, 'bold'), bootstyle='primary').pack(pady=(0, 12))

        # File picker
        fp_frame = ttk.LabelFrame(main, text="  📁  Input File  ", bootstyle='info', padding=10)
        fp_frame.pack(fill=tk.X, pady=(0, 8))
        self._file_picker = FilePicker(
            fp_frame,
            label="File:",
            filetypes=[("VYB & VYP Files", "*.vyb *.vyp"), ("All Files", "*.*")],
            dialog_title="Select Database File",
        )
        self._file_picker.pack(fill=tk.X)

        # Info
        info_frame = ttk.LabelFrame(main, text="  ℹ️  About Settings Table Repair  ",
                                    bootstyle='info', padding=10)
        info_frame.pack(fill=tk.X, pady=(0, 8))
        info = tk.Text(info_frame, height=7, wrap=tk.WORD, font=('Segoe UI', 10),
                       relief=tk.FLAT, state='normal')
        info.insert(tk.END, (
            "This utility repairs the kb_settings table in Vyapar databases by:\n\n"
            "  1. Exporting all existing data (setting_id, setting_key, setting_value)\n"
            "  2. Dropping the potentially corrupted kb_settings table\n"
            "  3. Recreating the table with proper schema and UNIQUE constraints\n"
            "  4. Reinserting data while resolving duplicates:\n"
            "       - Duplicate setting_id  → SQLite assigns a new auto-increment ID\n"
            "       - Duplicate setting_key → Updates the existing record's value\n\n"
            "Note: This is non-destructive — the original file is never modified."
        ))
        info.configure(state='disabled')
        info.pack(fill=tk.BOTH, expand=True)

        # Button
        btn_row = ttk.Frame(main)
        btn_row.pack(pady=(0, 8))
        ttk.Button(btn_row, text="🔧 Repair Settings Table",
                   command=self._run, bootstyle='success', width=25).pack()

        # Progress
        self._progress_bar = ttk.Progressbar(main, mode='determinate', bootstyle='success striped')
        self._progress_bar.pack(fill=tk.X, pady=(0, 6))

        # Log
        log_frame = ttk.LabelFrame(main, text="  📋  Operation Log  ", bootstyle='info', padding=8)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
        self._log = scrolledtext.ScrolledText(log_frame, height=8, font=('Consolas', 10), wrap=tk.WORD)
        self._log.pack(fill=tk.BOTH, expand=True)

        # Download buttons
        dl_row = ttk.Frame(main)
        dl_row.pack()
        self._dl_vyb_btn = ttk.Button(dl_row, text="⬇️ Download .vyb",
                                      state=DISABLED, command=self._download_vyb,
                                      bootstyle='primary', width=20)
        self._dl_vyb_btn.pack(side=tk.LEFT, padx=6)
        self._dl_vyp_btn = ttk.Button(dl_row, text="⬇️ Download .vyp",
                                      state=DISABLED, command=self._download_vyp,
                                      bootstyle='primary', width=20)
        self._dl_vyp_btn.pack(side=tk.LEFT, padx=6)

    # ------------------------------------------------------------------
    # Thread-safe helpers
    # ------------------------------------------------------------------

    def _log_msg(self, msg: str, tag: str = ''):
        self.after(0, lambda m=msg, t=tag: self._log.insert(tk.END, m + '\n', t))
        self.after(0, lambda: self._log.see(tk.END))

    def _set_progress(self, pct: int):
        self.after(0, lambda p=pct: self._progress_bar.config(value=p))

    def _progress_cb(self, msg: str, pct: int):
        tag = 'success' if ('✓' in msg or 'Done' in msg or 'complete' in msg.lower()) else \
              'warning' if '⚠' in msg else \
              'error' if '✗' in msg else ''
        self._log_msg(msg, tag)
        self._set_progress(pct)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _run(self):
        input_file = self._file_picker.get()
        if not input_file:
            self._log_msg("✗ Please select an input file first", 'error')
            return
        self._log.delete('1.0', tk.END)
        self._progress_bar['value'] = 0
        self._dl_vyb_btn.config(state=DISABLED)
        self._dl_vyp_btn.config(state=DISABLED)
        threading.Thread(target=self._worker, args=(input_file,), daemon=True).start()

    def _worker(self, input_file):
        try:
            result = process_settings_repair(input_file, self._progress_cb)
            # result may be a 3-tuple: (vyp, vyb, summary)
            if isinstance(result, tuple) and len(result) == 3:
                vyp, vyb, summary = result
            else:
                vyp, vyb = result
                summary = None

            self._out_vyp = vyp
            self._out_vyb = vyb

            if summary:
                self._log_msg(f"✓ Original records : {summary.original_count}", 'success')
                self._log_msg(f"✓ Final records    : {summary.final_count}", 'success')
                self._log_msg(f"✓ Inserted         : {summary.inserted}", 'success')
                self._log_msg(f"  Updated          : {summary.updated}")
                if summary.skipped:
                    self._log_msg(f"⚠ Skipped          : {summary.skipped}", 'warning')
            self._log_msg(f"✓ Output .vyp: {vyp}", 'success')
            self._log_msg(f"✓ Output .vyb: {vyb}", 'success')
            self.after(0, lambda: self._dl_vyb_btn.config(state=NORMAL))
            self.after(0, lambda: self._dl_vyp_btn.config(state=NORMAL))
        except Exception as e:
            self._log_msg(f"✗ Error: {e}", 'error')
            self.after(0, lambda: self._progress_bar.config(value=0))

    def _download_vyb(self):
        if not self._out_vyb or not os.path.exists(self._out_vyb):
            self._log_msg("✗ No .vyb output available", 'error')
            return
        path = filedialog.asksaveasfilename(
            defaultextension='.vyb', filetypes=[("VYB Files", "*.vyb")],
            initialfile=os.path.basename(self._out_vyb),
        )
        if path:
            import shutil; shutil.copy(self._out_vyb, path)
            self._log_msg(f"✓ Saved: {path}", 'success')

    def _download_vyp(self):
        if not self._out_vyp or not os.path.exists(self._out_vyp):
            self._log_msg("✗ No .vyp output available", 'error')
            return
        path = filedialog.asksaveasfilename(
            defaultextension='.vyp', filetypes=[("VYP Files", "*.vyp")],
            initialfile=os.path.basename(self._out_vyp),
        )
        if path:
            import shutil; shutil.copy(self._out_vyp, path)
            self._log_msg(f"✓ Saved: {path}", 'success')

    # ------------------------------------------------------------------
    # Theme
    # ------------------------------------------------------------------

    def apply_theme(self, theme: str):
        self._apply_theme_colors(theme)

    def _apply_theme_colors(self, theme: str = None):
        if theme is None:
            try:
                theme = self._style.theme_use()
            except Exception:
                theme = 'cyborg'
        if theme == 'superhero' or theme == 'cyborg':
            self._log.tag_config('error',   foreground=self._style.colors.danger)
            self._log.tag_config('success', foreground=self._style.colors.success)
            self._log.tag_config('warning', foreground=self._style.colors.warning)
            self._log.configure(bg=self._style.colors.dark, fg=self._style.colors.light)
        else:
            self._log.tag_config('error',   foreground=self._style.colors.danger)
            self._log.tag_config('success', foreground=self._style.colors.success)
            self._log.tag_config('warning', foreground=self._style.colors.warning)
            self._log.configure(bg=self._style.colors.light, fg=self._style.colors.dark)
