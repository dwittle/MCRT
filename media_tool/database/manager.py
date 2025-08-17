#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database manager for the Media Consolidation Tool.
"""

import sqlite3
import contextlib
from pathlib import Path
from typing import List

from ..models.file_record import FileRecord
from .schema import MAIN_SCHEMA, CHECKPOINT_SCHEMA


class DatabaseManager:
    """Manages SQLite database connections and operations."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_schema()
    
    @contextlib.contextmanager
    def get_connection(self):
        """Get a database connection with proper cleanup."""
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_schema(self):
        """Initialize database schema if needed."""
        with self.get_connection() as conn:
            conn.executescript(MAIN_SCHEMA)
            conn.executescript(CHECKPOINT_SCHEMA)
            conn.commit()
    
    def batch_insert_files(self, records: List[FileRecord], batch_size: int = 1000):
        """Efficiently insert multiple file records."""
        print(f"  - Batch inserting {len(records):,} records...", end="", flush=True)
        
        with self.get_connection() as conn:
            rows = []
            for rec in records:
                rows.append((
                    rec.sha256, rec.phash, rec.width, rec.height, rec.size_bytes,
                    rec.file_type, rec.drive_id, rec.path, int(rec.is_large),
                    0, None, None, None, rec.fast_fp
                ))
            
            # Process in batches
            inserted = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                conn.executemany("""
                    INSERT OR IGNORE INTO files
                    (hash_sha256, phash, width, height, size_bytes, type, drive_id, 
                     path_on_drive, is_large, copied, duplicate_of, group_id, central_path, fast_fp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                inserted += len(batch)
                if i + batch_size < len(rows):
                    print(f"\r  - Batch inserting {inserted:,}/{len(rows):,} records...", 
                          end="", flush=True)
                    
            conn.commit()
            print(f"\r  - Inserted {inserted:,} records ✓")