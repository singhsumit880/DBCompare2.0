"""Runner for the DBCompare API."""
from pathlib import Path
import sys

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.main import app


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8765)
