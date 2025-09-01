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
from ..jsonio import success, error


def cmd_make_original(db_manager: DatabaseManager, central: Path, file_id: int, as_json: bool = False):
    """Make a file its own original (split from group)."""
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT group_id, path_on_drive FROM files WHERE file_id=?", (file_id,)).fetchone()
        if not row:
            if as_json:
                return error("make-original", f"File {file_id} not found")
            else:
                print("File not found")
                return
        
        old_group_id, file_path = row
        
        # Create new group
        cursor = conn.execute("INSERT INTO groups (original_file_id) VALUES (?)", (file_id,))
        new_group_id = cursor.lastrowid
        
        # Update file
        conn.execute("UPDATE files SET group_id=?, duplicate_of=NULL WHERE file_id=?", 
                    (new_group_id, file_id))
        conn.commit()
        
        if as_json:
            return success("make-original", {
                "file_id": file_id,
                "old_group_id": old_group_id,
                "new_group_id": new_group_id,
                "file_path": file_path
            })
        else:
            print(f"File {file_id} is now original of new group {new_group_id}")


def cmd_promote(db_manager: DatabaseManager, central: Path, file_id: int, as_json: bool = False):
    """Promote file to be group's original."""
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT group_id, path_on_drive FROM files WHERE file_id=?", (file_id,)).fetchone()
        if not row or not row[0]:
            if as_json:
                return error("promote", f"File {file_id} not found or not in a group")
            else:
                print("File not found or not in a group")
                return
        
        group_id, file_path = row
        
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
        
        if as_json:
            return success("promote", {
                "file_id": file_id,
                "group_id": group_id,
                "old_original_id": old_original_id,
                "file_path": file_path
            })
        else:
            print(f"Promoted file {file_id} to original of group {group_id}")


def cmd_move_to_group(db_manager: DatabaseManager, central: Path, file_id: int, target_group_id: int, as_json: bool = False):
    """Move file to existing group."""
    with db_manager.get_connection() as conn:
        # Check if file exists
        file_row = conn.execute("SELECT group_id, path_on_drive FROM files WHERE file_id=?", (file_id,)).fetchone()
        if not file_row:
            if as_json:
                return error("move-to-group", f"File {file_id} not found")
            else:
                print("File not found")
                return
        
        old_group_id, file_path = file_row
        
        # Get target group's original
        orig_row = conn.execute("SELECT original_file_id FROM groups WHERE group_id=?", 
                               (target_group_id,)).fetchone()
        if not orig_row:
            if as_json:
                return error("move-to-group", f"Target group {target_group_id} not found")
            else:
                print("Target group not found")
                return
        
        target_original = orig_row[0]
        conn.execute("UPDATE files SET group_id=?, duplicate_of=? WHERE file_id=?",
                    (target_group_id, target_original, file_id))
        conn.commit()
        
        if as_json:
            return success("move-to-group", {
                "file_id": file_id,
                "old_group_id": old_group_id,
                "new_group_id": target_group_id,
                "target_original_id": target_original,
                "file_path": file_path
            })
        else:
            print(f"Moved file {file_id} to group {target_group_id}")


def cmd_mark(db_manager: DatabaseManager, file_id: int, new_status: str, note: Optional[str] = None, as_json: bool = False):
    """Mark file review status."""
    if new_status not in REVIEW_STATUSES:
        if as_json:
            return error("mark", f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
        else:
            print(f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
            return
    
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT review_status, path_on_drive FROM files WHERE file_id=?", (file_id,)).fetchone()
        if not row:
            if as_json:
                return error("mark", f"File {file_id} not found")
            else:
                print("File not found")
                return

        old_status, file_path = row

        conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE file_id=?",
                    (new_status, now_iso(), note, file_id))
        conn.commit()
        
    if as_json:
        return success("mark", {
            "file_id": file_id,
            "old_status": old_status,
            "new_status": new_status,
            "note": note,
            "file_path": file_path,
            "changed": old_status != new_status
        })
    else:
        print(f"Marked file {file_id} as {new_status}")


def cmd_mark_group(db_manager: DatabaseManager, group_id: int, new_status: str, note: Optional[str] = None, as_json: bool = False):
    """Mark entire group review status."""
    with db_manager.get_connection() as conn:
        # Check if group exists
        group_row = conn.execute("SELECT original_file_id FROM groups WHERE group_id=?", (group_id,)).fetchone()
        if not group_row:
            if as_json:
                return error("mark-group", f"Group {group_id} not found")
            else:
                print("Group not found")
                return
        
        # Update all files in the group
        cursor = conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE group_id=?", 
                             (new_status, now_iso(), note, group_id))
        conn.commit()
        updated_count = cursor.rowcount or 0

    if as_json:
        return success("mark-group", {
            "group_id": group_id,
            "new_status": new_status,
            "note": note,
            "files_updated": updated_count
        })
    else:
        print(f"Marked group {group_id} as {new_status} ({updated_count} files updated)")


def cmd_bulk_mark(db_manager: DatabaseManager, path_like: str, new_status: str, 
                 limit: int = 100, preview: bool = False, as_json: bool = False):
    """Bulk mark files by path pattern."""
    with db_manager.get_connection() as conn:
        # Get matches
        matches = conn.execute(
            "SELECT file_id, path_on_drive FROM files WHERE path_on_drive LIKE ? LIMIT ?",
            (path_like, limit)
        ).fetchall()

        total_matches = conn.execute("SELECT COUNT(1) FROM files WHERE path_on_drive LIKE ?", (path_like,)).fetchone()[0]
        
        sample_files = [{"file_id": f, "path_on_drive": p} for (f, p) in matches]

        if preview:
            if as_json:
                return success("bulk-mark", {
                    "mode": "preview",
                    "pattern": path_like,
                    "total_matches": int(total_matches),
                    "sample_files": sample_files,
                    "limit": limit
                })
            else:
                print(f"Preview: Found {total_matches} files matching pattern '{path_like}'")
                print(f"Sample files (showing first {len(matches)}):")
                for file_id, path in matches:
                    print(f"  {file_id}: {path}")
                return

        # Apply changes
        conn.execute("UPDATE files SET review_status=?, reviewed_at=? WHERE path_on_drive LIKE ?", 
                    (new_status, now_iso(), path_like))
        conn.commit()

    if as_json:
        return success("bulk-mark", {
            "mode": "apply",
            "pattern": path_like,
            "new_status": new_status,
            "total_matches": int(total_matches),
            "limit": limit
        })
    else:
        print(f"Bulk marked {total_matches} files as {new_status}")


def cmd_review_queue(db_manager: DatabaseManager, limit: int = 100, as_json: bool = False):
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
    
    if as_json:
        items = []
        for r in rows:
            file_id, gid, typ, w, h, size, status, path = r
            items.append({
                "file_id": file_id,
                "group_id": gid if gid != -1 else None,
                "type": typ,
                "width": w,
                "height": h,
                "dimensions": f"{w}x{h}" if (w and h) else None,
                "size_bytes": size,
                "review_status": status,
                "path_on_drive": path
            })
        
        return success("review-queue", {
            "items": items,
            "count": len(items),
            "limit": limit
        })
    
    # Human-readable output
    if not rows:
        print("No items in review queue.")
        return
        
    print(f"Review queue ({len(rows)} items, limit={limit}):")
    print("file_id | group_id | type  | dimensions | size_bytes | status     | path")
    print("-" * 80)
    for r in rows:
        file_id, gid, typ, w, h, size, status, path = r
        dims = f"{w}x{h}" if (w and h) else "-"
        print(f"{file_id:7d} | {gid:8d} | {typ:5s} | {dims:>10s} | {size or 0:10d} | {status:10s} | {path}")


def cmd_export_backup_list(db_manager: DatabaseManager, out_path: Path, include_undecided: bool = False, 
                          include_large: bool = False, include_originals: bool = False, as_json: bool = False):
    """Export backup manifest CSV with enhanced filtering options."""
    with db_manager.get_connection() as conn:
        where_conditions = []
        
        # Base condition for files we want to include
        status_conditions = []
        
        # Always include files marked as 'keep'
        status_conditions.append("review_status = 'keep'")
        
        # Include undecided files if requested
        if include_undecided:
            status_conditions.append("review_status = 'undecided'")
        
        # Include originals even if undecided (when include_originals is True)
        if include_originals:
            # Include files that are group originals, regardless of status
            # (but only if they're undecided, since 'keep' originals are already included above)
            status_conditions.append("""
                (review_status = 'undecided' AND 
                 EXISTS (SELECT 1 FROM groups g WHERE g.original_file_id = files.file_id))
            """)
        
        # Combine status conditions
        if status_conditions:
            where_conditions.append(f"({' OR '.join(status_conditions)})")
        else:
            # If no status conditions, include nothing (shouldn't happen with current logic)
            where_conditions.append("1=0")
        
        # Handle large files
        if not include_large:
            where_conditions.append("is_large = 0")

        # Build final query
        where_clause = ' AND '.join(where_conditions) if where_conditions else '1=1'
        
        query = f"""
            SELECT file_id, path_on_drive, central_path, size_bytes, type, review_status,
                   CASE WHEN EXISTS (SELECT 1 FROM groups g WHERE g.original_file_id = files.file_id)
                        THEN 1 ELSE 0 END as is_original
            FROM files
            WHERE {where_clause}
            ORDER BY is_original DESC, path_on_drive
        """
        
        rows = conn.execute(query).fetchall()

    ensure_dir(out_path.parent)
    
    with out_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["file_id", "path_on_drive", "central_path", "size_bytes", "type", "review_status", "is_original"])
        writer.writerows(rows)
    
    # Count originals vs regular files for reporting
    original_count = sum(1 for row in rows if row[6])  # is_original column
    regular_count = len(rows) - original_count
    
    if as_json:
        return success("export-backup-list", {
            "output_file": str(out_path),
            "records_exported": len(rows),
            "originals_count": original_count,
            "regular_files_count": regular_count,
            "include_undecided": include_undecided,
            "include_large": include_large,
            "include_originals": include_originals,
            "filters_applied": {
                "exclude_undecided": not include_undecided,
                "exclude_large": not include_large,
                "include_undecided_originals": include_originals
            }
        })
    else:
        print(f"Exported {len(rows)} records to {out_path}")
        if include_originals and original_count > 0:
            print(f"  - Included {original_count} originals (even if undecided)")
        if regular_count > 0:
            print(f"  - Included {regular_count} regular files")