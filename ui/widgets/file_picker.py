"""
ui/widgets/file_picker.py
Reusable "Label + Entry + Browse" row widget.
"""
import tkinter as tk
from tkinter import filedialog, ttk


class FilePicker(ttk.Frame):
    """
    A single-row widget:  [Label]  [___ entry ___]  [📂 Browse]

    Parameters
    ----------
    label       : str  — text for the label
    filetypes   : list — passed to filedialog.askopenfilename
    on_change   : callable(path: str) — called when a file is selected
    dialog_title: str  — title for the file dialog
    """

    def __init__(
        self,
        parent,
        label: str = "File:",
        filetypes=None,
        on_change=None,
        dialog_title: str = "Select File",
        **kwargs,
    ):
        super().__init__(parent, **kwargs)
        self._on_change = on_change
        self._filetypes = filetypes or [("All Files", "*.*")]
        self._dialog_title = dialog_title

        self.columnconfigure(1, weight=1)

        ttk.Label(self, text=label, width=14, anchor="w").grid(
            row=0, column=0, sticky="w", padx=(0, 5)
        )

        self._var = tk.StringVar()
        self._entry = ttk.Entry(self, textvariable=self._var)
        self._entry.grid(row=0, column=1, sticky="ew", padx=(0, 5))

        ttk.Button(
            self,
            text="📂 Browse…",
            command=self._browse,
            bootstyle="info-outline",
            width=12,
        ).grid(row=0, column=2, sticky="e")

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get(self) -> str:
        return self._var.get().strip()

    def set(self, path: str) -> None:
        self._var.set(path)

    def clear(self) -> None:
        self._var.set("")

    # ------------------------------------------------------------------

    def _browse(self):
        path = filedialog.askopenfilename(
            title=self._dialog_title,
            filetypes=self._filetypes,
        )
        if path:
            self._var.set(path)
            if self._on_change:
                self._on_change(path)
