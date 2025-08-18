import sqlite3
from itertools import groupby
from typing import Optional

def hamdist(a: int, b: int) -> int:
    return (a ^ b).bit_count()

def _px(w: Optional[int], h: Optional[int]) -> int:
    return (w or 0) * (h or 0)

def _parse_phash_hex(p_hex):
    if p_hex is None:
        return None
    try:
        s = str(p_hex).strip().lower()
        if s.startswith("0x"): s = s[2:]
        return int(s, 16)
    except Exception:
        return None

def _ensure_group_for(conn: sqlite3.Connection, file_id: int) -> int:
    row = conn.execute("SELECT group_id FROM files WHERE file_id=?", (file_id,)).fetchone()
    if row and row[0]:
        return row[0]
    cur = conn.execute(
        "INSERT INTO groups (original_file_id, created_at) VALUES (?, datetime('now'))",
        (file_id,),
    )
    gid = cur.lastrowid
    conn.execute("UPDATE files SET group_id=?, duplicate_of=NULL WHERE file_id=?", (gid, file_id))
    return gid

def group_duplicates(conn: sqlite3.Connection, phash_threshold: int = 5, size_bucket: int = 100*1024*1024):
    conn.execute("PRAGMA foreign_keys=ON")

    # 1) exact dupes by sha256
    rows = conn.execute(
        "SELECT file_id, hash_sha256, size_bytes, phash, width, height "
        "FROM files WHERE hash_sha256 IS NOT NULL ORDER BY hash_sha256"
    ).fetchall()

    for sha, bucket in groupby(rows, key=lambda r: r[1]):
        b = list(bucket)
        if len(b) < 2: continue
        original = max(b, key=lambda r: (_px(r[4], r[5]), r[2]))
        gid = _ensure_group_for(conn, original[0])
        for r in b:
            if r[0] == original[0]: continue
            conn.execute("UPDATE files SET group_id=?, duplicate_of=? WHERE file_id=?", (gid, original[0], r[0]))
    conn.commit()

    # 2) near dupes by phash (phash stored as TEXT)
    rows = conn.execute(
        "SELECT file_id, phash, width, height, size_bytes "
        "FROM files WHERE phash IS NOT NULL AND group_id IS NULL ORDER BY size_bytes"
    ).fetchall()

    def bucket_key(r): return int(r[4] // size_bucket)
    for _, bucket in groupby(rows, key=bucket_key):
        tmp = []
        for r in bucket:
            p = _parse_phash_hex(r[1])
            if p is None: continue
            tmp.append((r[0], p, r[2], r[3], r[4]))
        tmp.sort(key=lambda r: r[1])
        used = set()
        for i, a in enumerate(tmp):
            if a[0] in used: continue
            group = [a]
            for j in range(i+1, len(tmp)):
                if tmp[j][0] in used: continue
                if hamdist(a[1], tmp[j][1]) <= phash_threshold:
                    group.append(tmp[j])
            if len(group) > 1:
                original = max(group, key=lambda r: (_px(r[2], r[3]), r[4]))
                gid = _ensure_group_for(conn, original[0])
                for r in group:
                    used.add(r[0])
                    if r[0] == original[0]: continue
                    conn.execute("UPDATE files SET group_id=?, duplicate_of=? WHERE file_id=?", (gid, original[0], r[0]))
        conn.commit()
