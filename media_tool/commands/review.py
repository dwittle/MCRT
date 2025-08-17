#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Review and correction command implementations for the Media Consolidation Tool.
"""

import csv
from pathlib import Path
from typing import Optional

from ..config import REVIEW_STATUSES, LARGE_FILE_BYTES
from ..database.manager import DatabaseManager
from ..utils.path import ensure_dir
from ..utils.time import now_iso, utc_now_str


def cmd_make_original(db_manager: DatabaseManager, central: Path, file_id: int):
    """Make a file its own original (split from group)."""
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT group_id FROM files WHERE file_id=?", (file_id,)).fetchone()
        if not row:
            print("File not found")
            return
            
        # Create new group
        cursor = conn.execute("INSERT INTO groups (original_file_id) VALUES (?)", (file_id,))
        new_group_id = cursor.lastrowid
        
        # Update file
        conn.execute("UPDATE files SET group_id=?, duplicate_of=NULL WHERE file_id=?", 
                    (new_group_id, file_id))
        conn.commit()
        
        print(f"File {file_id} is now original of new group {new_group_id}")


def cmd_promote(db_manager: DatabaseManager, central: Path, file_id: int):
    """Promote file to be group's original."""
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT group_id FROM files WHERE file_id=?", (file_id,)).fetchone()
        if not row or not row[0]:
            print("File not found or not in a group")
            return
            
        group_id = row[0]
        
        # Get current original
        orig_row = conn.execute("""
            SELECT original_file_id FROM groups WHERE group_id=?
        """, (group_id,)).fetchone()
        
        old_original_id = orig_row[0] if orig_row else None
        
        # Update group and relationships
        conn.execute("UPDATE groups SET original_file_id=? WHERE group_id=?", (file_id, group_id))
        conn.execute("UPDATE files SET duplicate_of=NULL WHERE file_id=?", (file_id,))
        
        if old_original_id:
            conn.execute("UPDATE files SET duplicate_of=? WHERE file_id=?", (file_id, old_original_id))
        
        conn.execute("UPDATE files SET duplicate_of=? WHERE group_id=? AND file_id != ?",
                    (file_id, group_id, file_id))
        conn.commit()
        
        print(f"Promoted file {file_id} to original of group {group_id}")


def cmd_move_to_group(db_manager: DatabaseManager, central: Path, file_id: int, target_group_id: int):
    """Move file to existing group."""
    with db_manager.get_connection() as conn:
        # Get target group's original
        orig_row = conn.execute("SELECT original_file_id FROM groups WHERE group_id=?", 
                               (target_group_id,)).fetchone()
        if not orig_row:
            print("Target group not found")
            return
            
        target_original = orig_row[0]
        conn.execute("UPDATE files SET group_id=?, duplicate_of=? WHERE file_id=?",
                    (target_group_id, target_original, file_id))
        conn.commit()
        
        print(f"Moved file {file_id} to group {target_group_id}")


def cmd_mark(db_manager: DatabaseManager, file_id: int, status: str, note: Optional[str]):
    """Mark file review status."""
    if status not in REVIEW_STATUSES:
        print(f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
        return
        
    with db_manager.get_connection() as conn:
        conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE file_id=?",
                    (status, now_iso(), note, file_id))
        conn.commit()
        
    print(f"Marked file {file_id} as {status}")


def cmd_mark_group(db_manager: DatabaseManager, group_id: int, status: str, note: Optional[str]):
    """Mark entire group review status."""
    if status not in REVIEW_STATUSES:
        print(f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
        return
        
    with db_manager.get_connection() as conn:
        conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE group_id=?",
                    (status, now_iso(), note, group_id))
        conn.commit()
        
    print(f"Marked group {group_id} as {status}")


def cmd_bulk_mark(db_manager: DatabaseManager, path_like: str, status: str):
    """Bulk mark files by path pattern."""
    if status not in REVIEW_STATUSES:
        print(f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
        return
        
    with db_manager.get_connection() as conn:
        like_pattern = f"%{path_like}%"
        cursor = conn.execute("UPDATE files SET review_status=?, reviewed_at=? WHERE path_on_drive LIKE ?",
                            (status, now_iso(), like_pattern))
        conn.commit()
        
    print(f"Bulk marked {cursor.rowcount} files matching '{path_like}' as {status}")


def cmd_review_queue(db_manager: DatabaseManager, limit: int):
    """Show review queue."""
    with db_manager.get_connection() as conn:
        rows = conn.execute("""
            SELECT file_id, COALESCE(group_id, -1), type, width, height, size_bytes, 
                   review_status, path_on_drive
            FROM files
            WHERE review_status='undecided'
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
    
    if not rows:
        print("No undecided items in review queue")
        return
        
    print("file_id | group_id | type  | dimensions | size_bytes | status     | path")
    print("-" * 80)
    for r in rows:
        file_id, gid, typ, w, h, size, status, path = r
        dims = f"{w}x{h}" if (w and h) else "-"
        print(f"{file_id:7d} | {gid:8d} | {typ:5s} | {dims:>10s} | {size or 0:10d} | {status:10s} | {path}")


def cmd_export_backup_list(db_manager: DatabaseManager, out_path: Path, 
                          include_undecided: bool, include_large: bool):
    """Export backup manifest CSV."""
    print(f"[{utc_now_str()}] Exporting backup manifest to {out_path}...")
    
    status_filter = "IN ('keep','undecided')" if include_undecided else "= 'keep'"
    
    with db_manager.get_connection() as conn:
        print("  - Querying originals...", end="", flush=True)
        rows = conn.execute(f"""
            SELECT g.group_id, f.file_id, f.central_path, f.width, f.height, 
                   f.size_bytes, f.hash_sha256, f.review_status
            FROM groups g
            JOIN files f ON f.file_id = g.original_file_id
            WHERE f.review_status {status_filter}
            AND ((? = 1) OR (f.is_large = 0))
            ORDER BY g.group_id
        """, (1 if include_large else 0,)).fetchall()
        print(f" {len(rows):,} records")
    
    print("  - Writing CSV...", end="", flush=True)
    ensure_dir(out_path.parent)
    with out_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["group_id", "original_file_id", "central_original_path",
                        "width", "height", "size_bytes", "hash_sha256", "review_status"])
        writer.writerows(rows)
    print(" ✓")
    
    # Calculate totals
    total_size = sum(row[5] or 0 for row in rows)
    total_gb = total_size / (1024**3)
    
    print(f"[{utc_now_str()}] Export complete:")
    print(f"  - Files: {len(rows):,} originals")
    print(f"  - Size: {total_gb:.1f} GB")
    print(f"  - Location: {out_path}")