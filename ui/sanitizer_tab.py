"""
ui/sanitizer_tab.py
Database Sanitization tab.
"""
import os
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog

import ttkbootstrap as ttkb
from ttkbootstrap.constants import *

from core.sanitizer import process_sanitization, convert_file
from ui.widgets.file_picker import FilePicker

_DEFAULT_QUERIES = """\
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
UPDATE kb_transactions SET additional_details_json = NULL;
UPDATE repeat_invoice_template SET next_due_date = NULL, end_date = NULL, week_days = NULL, on_day = NULL, paused_until = NULL, txn_json = '{}';
"""


class SanitizerTab(ttk.Frame):
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

        ttk.Label(main, text="🔒 Database Sanitization Tool",
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

        # SQL editor
        sql_frame = ttk.LabelFrame(main, text="  📝  SQL Queries (separated by ;)  ",
                                   bootstyle='info', padding=10)
        sql_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
        self._query_text = scrolledtext.ScrolledText(
            sql_frame, height=10, font=('Consolas', 10), wrap=tk.WORD, padx=5, pady=5,
        )
        self._query_text.pack(fill=tk.BOTH, expand=True)
        self._query_text.insert(tk.END, _DEFAULT_QUERIES.strip())

        # Buttons
        btn_row = ttk.Frame(main)
        btn_row.pack(pady=(0, 8))
        ttk.Button(btn_row, text="⚡ Execute SQL",
                   command=self._run_sanitize, bootstyle='success', width=20).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_row, text="🔄 Convert / Repack",
                   command=self._run_convert, bootstyle='primary-outline', width=20).pack(side=tk.LEFT, padx=6)

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
    # Thread-safe logging helpers
    # ------------------------------------------------------------------

    def _log_msg(self, msg: str, tag: str = ''):
        self.after(0, lambda m=msg, t=tag: self._log.insert(tk.END, m + '\n', t))
        self.after(0, lambda: self._log.see(tk.END))

    def _set_progress(self, pct: int):
        self.after(0, lambda p=pct: self._progress_bar.config(value=p))

    def _progress_cb(self, msg: str, pct: int):
        tag = 'success' if '✓' in msg or 'Done' in msg else 'error' if '✗' in msg else ''
        self._log_msg(msg, tag)
        self._set_progress(pct)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _run_sanitize(self):
        input_file = self._file_picker.get()
        if not input_file:
            self._log_msg("✗ Please select an input file first", 'error')
            return
        queries = [q for q in self._query_text.get('1.0', tk.END).split(';') if q.strip()]
        self._log.delete('1.0', tk.END)
        self._progress_bar['value'] = 0
        self._dl_vyb_btn.config(state=DISABLED)
        self._dl_vyp_btn.config(state=DISABLED)
        threading.Thread(target=self._sanitize_worker, args=(input_file, queries), daemon=True).start()

    def _sanitize_worker(self, input_file, queries):
        try:
            vyp, vyb = process_sanitization(input_file, queries, self._progress_cb)
            self._out_vyp = vyp
            self._out_vyb = vyb
            self._log_msg(f"✓ Output .vyp: {vyp}", 'success')
            self._log_msg(f"✓ Output .vyb: {vyb}", 'success')
            self.after(0, lambda: self._dl_vyb_btn.config(state=NORMAL))
            self.after(0, lambda: self._dl_vyp_btn.config(state=NORMAL))
        except Exception as e:
            self._log_msg(f"✗ Error: {e}", 'error')

    def _run_convert(self):
        input_file = self._file_picker.get()
        if not input_file:
            self._log_msg("✗ Please select an input file first", 'error')
            return
        self._log.delete('1.0', tk.END)
        self._progress_bar['value'] = 0
        threading.Thread(target=self._convert_worker, args=(input_file,), daemon=True).start()

    def _convert_worker(self, input_file):
        try:
            vyp, vyb = convert_file(input_file, self._progress_cb)
            self._out_vyp = vyp
            self._out_vyb = vyb
            self._log_msg(f"✓ Converted .vyp: {vyp}", 'success')
            self._log_msg(f"✓ Converted .vyb: {vyb}", 'success')
            self.after(0, lambda: self._dl_vyb_btn.config(state=NORMAL))
            self.after(0, lambda: self._dl_vyp_btn.config(state=NORMAL))
        except Exception as e:
            self._log_msg(f"✗ Error: {e}", 'error')

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
            self._log.configure(bg=self._style.colors.dark, fg=self._style.colors.light)
        else:
            self._log.tag_config('error',   foreground=self._style.colors.danger)
            self._log.tag_config('success', foreground=self._style.colors.success)
            self._log.configure(bg=self._style.colors.light, fg=self._style.colors.dark)
