from pathlib import Path
import sqlite3

SCHEMA_FILE = Path(__file__).with_name("schema.sql")

def init_db_if_needed(db_path: Path):
    db_path = Path(db_path)
    create_new = not db_path.exists()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        if create_new:
            with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
                conn.executescript(f.read())
            conn.commit()
    finally:
        conn.close()
