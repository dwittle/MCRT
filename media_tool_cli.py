#!/usr/bin/env python3
"""
Complete CLI for Media Tool with all commands needed for UI support.
This replaces your current media_tool_cli.py with full functionality.
"""

import argparse
import sys
from pathlib import Path

# Add the current directory to Python path so media_tool can be imported
sys.path.insert(0, str(Path(__file__).parent))

# Import the working scanner instead of the broken command
from media_tool.scanning.scanner import OptimizedScanner
from media_tool.database.manager import DatabaseManager

def cmd_scan(args):
    """Execute scan command."""
    # Use OptimizedScanner directly since it works
    db_path = Path(args.db)
    central_path = Path(args.central)
    
    scanner = OptimizedScanner(db_path, central_path)
    
    scanner.scan_source(
        source=Path(args.source),
        workers=args.workers,
        io_workers=args.io_workers,
        phash_threshold=args.phash_threshold,
        chunk_size=args.chunk_size,
        auto_checkpoint=not args.no_checkpoints,
        hash_large=args.hash_large,
        skip_discovery=args.skip_discovery
    )

def cmd_stats(args):
    """Show database statistics."""
    db_manager = DatabaseManager(Path(args.db))
    
    with db_manager.get_connection() as conn:
        # Basic counts
        file_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        group_count = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
        drive_count = conn.execute("SELECT COUNT(*) FROM drives").fetchone()[0]
        
        # File status breakdown
        status_counts = dict(conn.execute("""
            SELECT review_status, COUNT(*) 
            FROM files 
            GROUP BY review_status
        """).fetchall())
        
        # Size statistics
        size_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_files,
                SUM(size_bytes) as total_bytes,
                AVG(size_bytes) as avg_bytes,
                SUM(CASE WHEN is_large=1 THEN 1 ELSE 0 END) as large_files
            FROM files
        """).fetchone()
        
        print("=== Database Statistics ===")
        print(f"Files: {file_count:,}")
        print(f"Groups: {group_count:,}")
        print(f"Drives: {drive_count}")
        print()
        
        print("Review Status:")
        for status, count in status_counts.items():
            print(f"  {status}: {count:,}")
        print()
        
        if size_stats[1]:  # total_bytes
            total_gb = size_stats[1] / (1024**3)
            avg_mb = size_stats[2] / (1024**2) if size_stats[2] else 0
            print(f"Storage: {total_gb:.1f} GB total, {avg_mb:.1f} MB average")
            print(f"Large files (>500MB): {size_stats[3]:,}")

def cmd_review_queue(args):
    """Show review queue."""
    db_manager = DatabaseManager(Path(args.db))
    
    with db_manager.get_connection() as conn:
        rows = conn.execute("""
            SELECT file_id, COALESCE(group_id, -1), type, width, height, size_bytes, 
                   review_status, path_on_drive
            FROM files
            WHERE review_status='undecided'
            ORDER BY created_at DESC
            LIMIT ?
        """, (args.limit,)).fetchall()
    
    if not rows:
        print("No undecided items in review queue")
        return
        
    print("file_id | group_id | type  | dimensions | size_bytes | status     | path")
    print("-" * 80)
    for r in rows:
        file_id, gid, typ, w, h, size, status, path = r
        dims = f"{w}x{h}" if (w and h) else "-"
        print(f"{file_id:7d} | {gid:8d} | {typ:5s} | {dims:>10s} | {size or 0:10d} | {status:10s} | {path}")

def cmd_mark(args):
    """Mark file review status."""
    db_manager = DatabaseManager(Path(args.db))
    
    if args.status not in ['undecided', 'keep', 'not_needed']:
        print(f"Invalid status. Use: undecided, keep, not_needed")
        return
        
    with db_manager.get_connection() as conn:
        from datetime import datetime
        timestamp = datetime.now().isoformat() + 'Z'
        conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE file_id=?",
                    (args.status, timestamp, args.note or '', args.file_id))
        conn.commit()
        
    print(f"Marked file {args.file_id} as {args.status}")

def cmd_mark_group(args):
    """Mark entire group review status."""
    db_manager = DatabaseManager(Path(args.db))
    
    if args.status not in ['undecided', 'keep', 'not_needed']:
        print(f"Invalid status. Use: undecided, keep, not_needed")
        return
        
    with db_manager.get_connection() as conn:
        from datetime import datetime
        timestamp = datetime.now().isoformat() + 'Z'
        cursor = conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE group_id=?",
                            (args.status, timestamp, args.note or '', args.group_id))
        conn.commit()
        
    print(f"Marked group {args.group_id} as {args.status} ({cursor.rowcount} files updated)")

def cmd_bulk_mark(args):
    """Bulk mark files by path pattern."""
    db_manager = DatabaseManager(Path(args.db))
    
    if args.status not in ['undecided', 'keep', 'not_needed']:
        print(f"Invalid status. Use: undecided, keep, not_needed")
        return
        
    with db_manager.get_connection() as conn:
        from datetime import datetime
        timestamp = datetime.now().isoformat() + 'Z'
        like_pattern = f"%{args.path_like}%"
        cursor = conn.execute("UPDATE files SET review_status=?, reviewed_at=? WHERE path_on_drive LIKE ?",
                            (args.status, timestamp, like_pattern))
        conn.commit()
        
    print(f"Bulk marked {cursor.rowcount} files matching '{args.path_like}' as {args.status}")

def cmd_promote(args):
    """Promote file to be group's original."""
    db_manager = DatabaseManager(Path(args.db))
    
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT group_id FROM files WHERE file_id=?", (args.file_id,)).fetchone()
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
        conn.execute("UPDATE groups SET original_file_id=? WHERE group_id=?", (args.file_id, group_id))
        conn.execute("UPDATE files SET duplicate_of=NULL WHERE file_id=?", (args.file_id,))
        
        if old_original_id:
            conn.execute("UPDATE files SET duplicate_of=? WHERE file_id=?", (args.file_id, old_original_id))
        
        conn.execute("UPDATE files SET duplicate_of=? WHERE group_id=? AND file_id != ?",
                    (args.file_id, group_id, args.file_id))
        conn.commit()
        
        print(f"Promoted file {args.file_id} to original of group {group_id}")

def cmd_export_backup_list(args):
    """Export backup manifest CSV."""
    db_manager = DatabaseManager(Path(args.db))
    
    print(f"Exporting backup manifest to {args.out}...")
    
    status_filter = "IN ('keep','undecided')" if args.include_undecided else "= 'keep'"
    
    with db_manager.get_connection() as conn:
        print("  - Querying originals...", end="", flush=True)
        rows = conn.execute(f"""
            SELECT g.group_id, f.file_id, f.path_on_drive, f.width, f.height, 
                   f.size_bytes, f.hash_sha256, f.review_status
            FROM groups g
            JOIN files f ON f.file_id = g.original_file_id
            WHERE f.review_status {status_filter}
            AND ((? = 1) OR (f.is_large = 0))
            ORDER BY g.group_id
        """, (1 if args.include_large else 0,)).fetchall()
        print(f" {len(rows):,} records")
    
    print("  - Writing CSV...", end="", flush=True)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    
    import csv
    with open(args.out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["group_id", "original_file_id", "path", 
                        "width", "height", "size_bytes", "hash_sha256", "review_status"])
        writer.writerows(rows)
    print(" ✓")
    
    # Calculate totals
    total_size = sum(row[5] or 0 for row in rows)
    total_gb = total_size / (1024**3)
    
    print(f"Export complete:")
    print(f"  - Files: {len(rows):,} originals")
    print(f"  - Size: {total_gb:.1f} GB")
    print(f"  - Location: {args.out}")

def cmd_cleanup_checkpoints(args):
    """Clean up old checkpoints."""
    from media_tool.checkpoint.manager import CheckpointManager
    
    db_manager = DatabaseManager(Path(args.db))
    checkpoint_manager = CheckpointManager(db_manager)
    
    if args.scan_id:
        checkpoint_manager.cleanup_checkpoint(args.scan_id)
        print(f"Cleaned up checkpoint: {args.scan_id}")
    else:
        checkpoint_manager.cleanup_old_checkpoints(args.days)
        print(f"Cleaned up checkpoints older than {args.days} days")

def main():
    """Main CLI entry point with complete command support."""
    parser = argparse.ArgumentParser(description="Media Consolidation Tool - Complete CLI")
    
    # Global options
    parser.add_argument("--db", default="media_index.db", help="Database path")
    
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")
    
    # SCAN command
    scan_parser = subparsers.add_parser("scan", help="Scan source path for media files")
    scan_parser.add_argument("--source", required=True, help="Source path to scan")
    scan_parser.add_argument("--central", required=True, help="Central storage directory")
    scan_parser.add_argument("--workers", type=int, default=6, help="Number of workers")
    scan_parser.add_argument("--io-workers", type=int, default=2, help="I/O workers")
    scan_parser.add_argument("--phash-threshold", type=int, default=5, help="pHash threshold")
    scan_parser.add_argument("--chunk-size", type=int, default=100, help="Chunk size")
    scan_parser.add_argument("--no-checkpoints", action="store_true", help="Disable checkpoints")
    scan_parser.add_argument("--hash-large", action="store_true", help="Hash large files")
    scan_parser.add_argument("--skip-discovery", action="store_true", help="Skip discovery")
    
    # STATS command
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.add_argument("--detailed", action="store_true", help="Show detailed breakdown")
    
    # REVIEW-QUEUE command
    queue_parser = subparsers.add_parser("review-queue", help="Show review queue")
    queue_parser.add_argument("--limit", type=int, default=100, help="Maximum items to show")
    
    # MARK command
    mark_parser = subparsers.add_parser("mark", help="Mark file review status")
    mark_parser.add_argument("--file-id", type=int, required=True, help="File ID to mark")
    mark_parser.add_argument("--status", choices=['undecided', 'keep', 'not_needed'], required=True, help="Review status")
    mark_parser.add_argument("--note", help="Optional review note")
    
    # MARK-GROUP command
    mark_group_parser = subparsers.add_parser("mark-group", help="Mark entire group status")
    mark_group_parser.add_argument("--group-id", type=int, required=True, help="Group ID to mark")
    mark_group_parser.add_argument("--status", choices=['undecided', 'keep', 'not_needed'], required=True, help="Review status")
    mark_group_parser.add_argument("--note", help="Optional review note")
    
    # BULK-MARK command
    bulk_mark_parser = subparsers.add_parser("bulk-mark", help="Bulk mark by path pattern")
    bulk_mark_parser.add_argument("--path-like", required=True, help="Path substring to match")
    bulk_mark_parser.add_argument("--status", choices=['undecided', 'keep', 'not_needed'], required=True, help="Review status")
    
    # PROMOTE command
    promote_parser = subparsers.add_parser("promote", help="Promote file to be group's original")
    promote_parser.add_argument("--file-id", type=int, required=True, help="File ID to promote")
    
    # EXPORT-BACKUP-LIST command
    export_parser = subparsers.add_parser("export-backup-list", help="Export backup manifest")
    export_parser.add_argument("--out", required=True, help="Output CSV file path")
    export_parser.add_argument("--include-undecided", action="store_true", help="Include undecided items")
    export_parser.add_argument("--include-large", action="store_true", help="Include large files")
    
    # CLEANUP-CHECKPOINTS command
    cleanup_parser = subparsers.add_parser("cleanup-checkpoints", help="Clean up old checkpoints")
    cleanup_parser.add_argument("--days", type=int, default=7, help="Remove checkpoints older than N days")
    cleanup_parser.add_argument("--scan-id", help="Remove specific checkpoint by scan ID")
    
    # Parse arguments
    args = parser.parse_args()
    
    try:
        # Execute commands
        if args.command == "scan":
            cmd_scan(args)
        elif args.command == "stats":
            cmd_stats(args)
        elif args.command == "review-queue":
            cmd_review_queue(args)
        elif args.command == "mark":
            cmd_mark(args)
        elif args.command == "mark-group":
            cmd_mark_group(args)
        elif args.command == "bulk-mark":
            cmd_bulk_mark(args)
        elif args.command == "promote":
            cmd_promote(args)
        elif args.command == "export-backup-list":
            cmd_export_backup_list(args)
        elif args.command == "cleanup-checkpoints":
            cmd_cleanup_checkpoints(args)
    
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()