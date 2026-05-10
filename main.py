"""
main.py — Entry point for DBCompare 2.0 (modular rewrite)
Run: python main.py
"""
import ttkbootstrap as ttkb
from ui.app import DatabaseToolApp


def main():
    root = ttkb.Window(themename="cyborg")
    app = DatabaseToolApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
