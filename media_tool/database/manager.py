# media_tool/database/manager.py
import sqlite3
from pathlib import Path
from .init import init_db_if_needed
from importlib.resources import files as ir_files  # stdlib, Python 3.9+

class DatabaseManager:
    """Manages SQLite database connections and operations."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        init_db_if_needed(self.db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        # Pragmas for performance & integrity
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")

    def get_connection(self):
        """Backward-compatible accessor used throughout the codebase."""
        return self.conn

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
