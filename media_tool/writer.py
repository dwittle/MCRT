from queue import Queue
from threading import Thread
import sqlite3
from typing import Iterable, Tuple, Optional, Any

class SQLiteWriter:
    def __init__(self, db_path: str, batch_size: int = 500, queue_max: int = 2000):
        self.db_path = db_path
        self.batch_size = batch_size
        self.q: "Queue[Optional[Tuple[Any, ...]]]" = Queue(maxsize=queue_max)
        self._stop = object()
        self._th = Thread(target=self._run, daemon=True)
        self._th.start()

    def _run(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        batch = []
        try:
            while True:
                item = self.q.get()
                if item is self._stop:
                    break
                batch.append(item)
                if len(batch) >= self.batch_size:
                    self._flush(conn, batch); batch.clear()
            if batch:
                self._flush(conn, batch)
        finally:
            conn.close()

    def _flush(self, conn, batch: Iterable[Tuple[Any, ...]]):
        conn.executemany(
            """
            INSERT OR IGNORE INTO files
            (hash_sha256, phash, width, height, size_bytes, type, drive_id,
             path_on_drive, is_large, copied, duplicate_of, group_id, central_path, fast_fp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
        conn.commit()

    def submit(self, row: Tuple[Any, ...]):
        self.q.put(row)

    def close(self):
        self.q.put(self._stop)
        self._th.join()
