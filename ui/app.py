"""
ui/app.py
Main application window. Owns the single ttkb.Style instance,
the notebook, and the theme toggle. Updates ALL tabs on theme change.
"""
import os
import tkinter as tk
from tkinter import ttk

import ttkbootstrap as ttkb

from ui.home_tab import HomeTab
from ui.comparison_tab import ComparisonTab
from ui.sanitizer_tab import SanitizerTab
from ui.fts_tab import FTSTab
from ui.settings_tab import SettingsTab


class DatabaseToolApp:
    """Top-level application controller with a modern Sidebar layout."""

    _THEMES = {
        'dark':  'superhero',
        'light': 'yeti',
    }

    def __init__(self, root: ttkb.Window):
        self.root = root
        self.root.title("DBCompare 2.0")
        self.root.geometry("1300x860")
        self.root.minsize(1000, 700)
        self.root.state('zoomed')

        # Set icon if available
        _icon = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "icon.ico")
        if os.path.exists(_icon):
            try:
                self.root.iconbitmap(_icon)
            except Exception:
                pass

        # Use striking, modern themes
        self._current_mode = 'dark'
        self._style = ttkb.Style(theme=self._THEMES[self._current_mode])
        
        # Customize toolbutton style for sidebar nav
        self._style.configure('Nav.Toolbutton', font=('Segoe UI', 12, 'bold'), anchor='w', padding=(20, 12))

        # ── State ──
        self._current_tab_name = tk.StringVar(value="home")
        self._frames = {}

        self._build_ui()

    def _build_ui(self):
        # ── Main Container ──
        main_container = ttk.Frame(self.root)
        main_container.pack(fill=tk.BOTH, expand=True)

        # ── Top Navigation Bar ──
        self.top_nav = ttk.Frame(main_container, padding=10)
        self.top_nav.pack(fill=tk.X, side=tk.TOP)
        
        # Add a sleek horizontal separator below the nav
        ttk.Separator(main_container, orient=tk.HORIZONTAL).pack(fill=tk.X)

        # ── Content Container ──
        self.content_area = ttk.Frame(main_container, padding=15)
        self.content_area.pack(fill=tk.BOTH, expand=True)

        # Build Navigation
        self._build_top_nav()

        # Build tabs
        self._frames['home']       = HomeTab(self.content_area)
        self._frames['comparison'] = ComparisonTab(self.content_area, self._style)
        self._frames['sanitizer']  = SanitizerTab(self.content_area, self._style)
        self._frames['fts']        = FTSTab(self.content_area, self._style)
        self._frames['settings']   = SettingsTab(self.content_area, self._style)

        for f in self._frames.values():
            f.grid(row=0, column=0, sticky="nsew")
        self.content_area.rowconfigure(0, weight=1)
        self.content_area.columnconfigure(0, weight=1)

        # Select initial
        self._on_nav_change()

    def _build_top_nav(self):
        # Brand Header
        brand_frame = ttk.Frame(self.top_nav)
        brand_frame.pack(side=tk.LEFT, padx=(10, 30))
        
        ttk.Label(brand_frame, text="DBCompare", font=('Segoe UI', 17, 'bold'), foreground=self._style.colors.primary).pack(side=tk.LEFT)
        ttk.Label(brand_frame, text=" PRO", font=('Segoe UI', 11, 'bold'), foreground=self._style.colors.warning).pack(side=tk.LEFT, padx=(2, 0), pady=(4, 0))

        # ── Nav Links Container ──
        nav_container = ttk.Frame(self.top_nav)
        nav_container.pack(side=tk.LEFT, fill=tk.Y)

        self._style.configure('TopNav.Toolbutton', font=('Segoe UI', 11, 'bold'), padding=(15, 8))

        nav_items = [
            ("🏠 Home", 'home'),
            ("🔍 Compare DBs", 'comparison'),
            ("🔒 Sanitizer", 'sanitizer'),
            ("⚙️ FTS Builder", 'fts'),
            ("🔧 Settings Repair", 'settings'),
        ]

        for text, val in nav_items:
            rb = ttk.Radiobutton(
                nav_container, 
                text=text, 
                value=val, 
                variable=self._current_tab_name,
                style='TopNav.Toolbutton',
                command=self._on_nav_change
            )
            rb.pack(side=tk.LEFT, padx=4)

        # ── Theme Toggle ──
        self._theme_btn = ttk.Button(
            self.top_nav,
            text="☀️ Light Mode",
            command=self._toggle_theme,
            bootstyle='outline',
        )
        self._theme_btn.pack(side=tk.RIGHT, padx=10)

    def _on_nav_change(self):
        selected = self._current_tab_name.get()
        frame = self._frames.get(selected)
        if frame:
            frame.lift()

    # ------------------------------------------------------------------
    # Theme toggle
    # ------------------------------------------------------------------

    def _toggle_theme(self):
        self._current_mode = 'light' if self._current_mode == 'dark' else 'dark'
        theme = self._THEMES[self._current_mode]

        self._style.theme_use(theme)
        self._theme_btn.config(text="☀️  Light Mode" if self._current_mode == 'dark' else "🌙  Dark Mode")

        # Propagate to all themed tabs
        for name, tab in self._frames.items():
            if hasattr(tab, 'apply_theme'):
                tab.apply_theme(theme)
