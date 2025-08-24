# media_tool/database/manager.py
import sqlite3
from pathlib import Path

from ..models.file_record import FileRecord
from .init import init_db_if_needed
from typing import List
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

    def batch_insert_files(self, records: List[FileRecord], batch_size: int = 1000):
        """
        Efficiently insert multiple file records, matching the updated 'files' schema.

        Schema columns inserted (explicit order):
        hash_sha256, phash, width, height, size_bytes, type, drive_id,
        path_on_drive, is_large, copied, duplicate_of, group_id,
        review_status, reviewed_at, review_note, central_path, fast_fp
        """
        total = len(records)
        print(f"  - Batch inserting {total:,} records...", end="", flush=True)

        rows = []
        for rec in records:
            rows.append((
                rec.sha256,           # hash_sha256
                rec.phash,            # phash
                rec.width,            # width
                rec.height,           # height
                rec.size_bytes,       # size_bytes
                rec.file_type,        # type
                rec.drive_id,         # drive_id
                rec.path,             # path_on_drive
                int(rec.is_large),    # is_large
                0,                    # copied (default 0)
                None,                 # duplicate_of
                None,                 # group_id
                'undecided',          # review_status (schema default, but explicit)
                None,                 # reviewed_at
                None,                 # review_note
                None,                 # central_path
                rec.fast_fp           # fast_fp
            ))

        inserted = 0
        with self.get_connection() as conn:
            for i in range(0, total, batch_size):
                batch = rows[i:i + batch_size]
                conn.executemany("""
                    INSERT OR IGNORE INTO files
                    (hash_sha256, phash, width, height, size_bytes, type, drive_id,
                    path_on_drive, is_large, copied, duplicate_of, group_id,
                    review_status, reviewed_at, review_note, central_path, fast_fp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                inserted += len(batch)
                if i + batch_size < total:
                    print(f"\r  - Batch inserting {inserted:,}/{total:,} records...", end="", flush=True)

            conn.commit()
        print(f"\r  - Inserted {inserted:,} records ✓")
