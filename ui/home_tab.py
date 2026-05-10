"""
ui/home_tab.py
Modern Dashboard Landing Page.
"""
import tkinter as tk
from tkinter import ttk
import ttkbootstrap as ttkb

class HomeTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self._build()

    def _build(self):
        # ── Main Scrollable Container ──
        canvas = tk.Canvas(self, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=canvas.yview)
        
        self.scrollable_frame = ttk.Frame(canvas, padding=30)
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # ── Hero Section ──────────────────────────────────────────────
        hero_frame = ttk.Frame(self.scrollable_frame)
        hero_frame.pack(fill=tk.X, pady=(0, 40))
        
        ttk.Label(
            hero_frame,
            text="Welcome to DBCompare PRO",
            font=('Segoe UI', 29, 'bold'),
            foreground="#4B0082"
        ).pack(anchor='w')
        
        ttk.Label(
            hero_frame,
            text="Advanced database inspection, sanitization, and restoration suite for Vyapar.",
            font=('Segoe UI', 15),
            foreground="#6A5ACD"
        ).pack(anchor='w', pady=(5, 0))

        # ── Feature Cards Grid ────────────────────────────────────────
        cards_container = ttk.Frame(self.scrollable_frame)
        cards_container.pack(fill=tk.BOTH, expand=True)
        cards_container.columnconfigure(0, weight=1)
        cards_container.columnconfigure(1, weight=1)

        cards = [
            (
                "primary", "#006400",
                "🔍 Comparison Engine",
                "Advanced structural diffs",
                "• Version & Schema mismatch detection\n"
                "• Unified column comparison UI\n"
                "• Exportable detailed diff reports"
            ),
            (
                "success", "#8B0000",
                "🔒 Data Sanitizer",
                "Secure data masking",
                "• Scrub sensitive PII & financial data\n"
                "• Zero-trace background execution\n"
                "• Automatic safe `.vyp` repacking"
            ),
            (
                "warning", "#DAA520",
                "⚙️ FTS Generator",
                "Search index rebuild",
                "• Fix broken internal Vyapar search\n"
                "• Full text index regeneration\n"
                "• Safe background transaction handling"
            ),
            (
                "info", "#4B0082",
                "🔧 Settings Repair",
                "Schema corruption fix",
                "• Detects & fixes duplicate setting keys\n"
                "• Clean schema recreation\n"
                "• Non-destructive copy preservation"
            ),
        ]

        for i, (bootstyle, title_color, title, subtitle, body) in enumerate(cards):
            row = i // 2
            col = i % 2
            
            # Use Labelframe as a modern card
            card = ttk.Labelframe(
                cards_container, 
                text=f"  {title.split(' ')[0]}  ", 
                bootstyle=bootstyle,
                padding=20
            )
            card.grid(row=row, column=col, padx=15, pady=15, sticky='nsew')
            
            ttk.Label(
                card,
                text=' '.join(title.split(' ')[1:]),
                font=('Segoe UI', 17, 'bold'),
                foreground=title_color
            ).pack(anchor='w', pady=(0, 5))
            
            ttk.Label(
                card,
                text=subtitle,
                font=('Segoe UI', 12, 'italic'),
                bootstyle="secondary"
            ).pack(anchor='w', pady=(0, 15))
            
            # Using Labels instead of Text for modern transparent backgrounds
            for line in body.split('\n'):
                ttk.Label(
                    card,
                    text=line,
                    font=('Segoe UI', 12)
                ).pack(anchor='w', pady=2)

        # ── Footer ────────────────────────────────────────────────────
        ttk.Separator(self.scrollable_frame).pack(fill=tk.X, pady=(40, 20))
        ttk.Label(
            self.scrollable_frame,
            text="v2.0.0-PRO  |  Built with a modern reactive UI",
            font=('Segoe UI', 10),
            bootstyle="secondary"
        ).pack(anchor='center')
