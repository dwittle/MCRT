#!/usr/bin/env python3
"""
Complete CLI for Media Tool with all commands needed for UI support.
This replaces your current media_tool_cli.py with full functionality.
"""

import argparse
import sys, re
from pathlib import Path

# Add the current directory to Python path so media_tool can be imported
sys.path.insert(0, str(Path(__file__).parent))

# Import the working scanner instead of the broken command
from media_tool.scanning.scanner import OptimizedScanner
from media_tool.database.manager import DatabaseManager

def cmd_scan(args):
    """Execute scan command."""
    # Parse minimum size if provided
    min_size_bytes = 0
    if args.min_size:
        try:
            min_size_bytes = parse_file_size(args.min_size)
            print(f"📏 Minimum file size filter: {format_file_size(min_size_bytes)} ({min_size_bytes:,} bytes)")
            print(f"   Files smaller than this will be ignored during scan")
        except ValueError as e:
            print(f"❌ Error: {e}")
            print(f"   Examples: '1M' (1 megabyte), '500K' (500 kilobytes), '2.5G' (2.5 gigabytes)")
            return

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
    """Bulk mark with accurate preview messages."""
    import re
    from media_tool.database.manager import DatabaseManager
    from datetime import datetime
    from pathlib import Path
    
    try:
        from tabulate import tabulate
    except ImportError:
        print("❌ tabulate not installed. Install with: pip install tabulate")
        return
    
    db_manager = DatabaseManager(Path(args.db))
    
    # Pattern matching setup
    use_regex = getattr(args, 'regex', False)
    pattern = args.path_like
    limit = getattr(args, 'limit', 100)
    show_paths = getattr(args, 'show_paths', False)
    
    # Search info
    search_info = [
        ["Pattern", f"'{pattern}'"],
        ["Mode", "Regular Expression" if use_regex else "SQL LIKE (substring)"],
        ["Preview limit", f"{limit:,} files"],
        ["Display", "Full paths" if show_paths else "Filenames only"]
    ]
    
    print("🔍 BULK-MARK PATTERN SEARCH")
    print(tabulate(search_info, tablefmt="plain"))
    print()
    
    with db_manager.get_connection() as conn:
        # STEP 1: Get TOTAL count first (most important fix)
        if use_regex:
            # For regex, we need to get all files and count matches
            all_files = conn.execute("""
                SELECT file_id, path_on_drive, review_status
                FROM files 
                ORDER BY path_on_drive
            """).fetchall()
            
            try:
                regex_pattern = re.compile(pattern, re.IGNORECASE)
                total_matches = 0
                all_matching_files = []
                
                for file_info in all_files:
                    path = file_info[1]
                    if regex_pattern.search(path):
                        total_matches += 1
                        all_matching_files.append(file_info)
                        
            except re.error as e:
                print(f"❌ Invalid regular expression: {e}")
                return
        else:
            # For LIKE, get total count efficiently
            like_pattern = f"%{pattern}%"
            
            # COUNT QUERY - Get exact total that will be affected
            total_matches = conn.execute("""
                SELECT COUNT(*) 
                FROM files 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
            """, (like_pattern,)).fetchone()[0]
        
        if total_matches == 0:
            print(f"❌ No files found matching pattern: '{pattern}'")
            return
        
        # STEP 2: Get sample for preview (limited query)
        if use_regex:
            # Use the matches we already found
            display_files = all_matching_files[:limit]
        else:
            # Get limited sample for display
            display_files = conn.execute("""
                SELECT 
                    file_id, path_on_drive, size_bytes, width, height, 
                    review_status, type, group_id
                FROM files 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
                ORDER BY path_on_drive
                LIMIT ?
            """, (like_pattern, limit)).fetchall()
        
        # PREVIEW MODE: Show accurate total count
        if not args.status:
            print(f"📊 PATTERN MATCHES {total_matches:,} FILES TOTAL")
            print("=" * 80)
            
            # Show status breakdown for ALL matches (not just sample)
            if use_regex:
                # Count from our regex matches
                status_groups = {}
                for file_info in all_matching_files:
                    status = file_info[2]  # review_status
                    status_groups[status] = status_groups.get(status, 0) + 1
            else:
                # Get status breakdown for ALL matches
                status_breakdown = conn.execute("""
                    SELECT review_status, COUNT(*) 
                    FROM files 
                    WHERE LOWER(path_on_drive) LIKE LOWER(?)
                    GROUP BY review_status
                """, (like_pattern,)).fetchall()
                
                status_groups = dict(status_breakdown)
            
            status_data = [[status, f"{count:,}"] for status, count in sorted(status_groups.items())]
            
            print("📊 STATUS BREAKDOWN (ALL FILES):")
            print(tabulate(status_data, headers=["Status", "Files"], tablefmt="plain"))
            
            # File sample table
            table_data = []
            for file_info in display_files:
                if len(file_info) >= 8:
                    file_id, path, size_bytes, width, height, review_status, file_type, group_id = file_info
                else:
                    # Handle different query results
                    file_id, path = file_info[0], file_info[1]
                    size_bytes = width = height = file_type = group_id = None
                    review_status = file_info[2] if len(file_info) > 2 else 'unknown'
                
                if show_paths:
                    display_path = path
                else:
                    display_path = Path(path).name
                
                size_mb = f"{size_bytes / (1024*1024):.1f}" if size_bytes else "0"
                dimensions = f"{width}×{height}" if (width and height) else "Unknown"
                group_str = str(group_id) if group_id else "-"
                
                table_data.append([
                    file_id,
                    review_status,
                    f"{size_mb} MB",
                    dimensions,
                    group_str,
                    display_path
                ])
            
            headers = ["ID", "Status", "Size", "Dimensions", "Group", 
                      "Full Path" if show_paths else "Filename"]
            
            print(f"\n📋 SAMPLE FILES (showing {len(display_files):,} of {total_matches:,} total):")
            print(tabulate(table_data, headers=headers, tablefmt="plain", maxcolwidths=None, stralign="left"))
            
            # CLEAR WARNING about total impact
            if total_matches > limit:
                print(f"\n⚠️  IMPORTANT: This preview shows only {limit:,} files")
                print(f"   BUT THE PATTERN MATCHES {total_matches:,} FILES TOTAL")
                print(f"   All {total_matches:,} files will be affected when you apply a status")
            
            # Action suggestions with accurate counts
            print(f"\n💡 ACTIONS (will affect ALL {total_matches:,} matching files):")
            regex_flag = " --regex" if use_regex else ""
            
            action_data = [
                ["Keep all", f"./media_tool_cli.py bulk-mark --path-like '{pattern}'{regex_flag} --status keep"],
                ["Mark not needed", f"./media_tool_cli.py bulk-mark --path-like '{pattern}'{regex_flag} --status not_needed"],
                ["Reset to undecided", f"./media_tool_cli.py bulk-mark --path-like '{pattern}'{regex_flag} --status undecided"]
            ]
            
            print(tabulate(action_data, headers=["Action", f"Command (affects {total_matches:,} files)"], tablefmt="plain"))
            
            # Impact summary with accurate totals
            if not use_regex:
                # Get total size for all matches
                total_size_result = conn.execute("""
                    SELECT SUM(size_bytes) 
                    FROM files 
                    WHERE LOWER(path_on_drive) LIKE LOWER(?)
                """, (like_pattern,)).fetchone()[0]
            else:
                total_size_result = sum(f[2] or 0 for f in all_matching_files)
            
            impact_data = [
                ["Total files", f"{total_matches:,}"],
                ["Total size", f"{(total_size_result or 0) / (1024**3):.2f} GB"],
                ["Sample shown", f"{len(display_files):,}"]
            ]
            
            print(f"\n📈 IMPACT SUMMARY:")
            print(tabulate(impact_data, tablefmt="plain"))
            
            return
        
        # MARK MODE: Show accurate counts before marking
        print(f"📝 BULK MARKING ANALYSIS")
        
        # Get accurate status change counts
        if use_regex:
            status_changes = {}
            for file_info in all_matching_files:
                current_status = file_info[2]  # review_status
                if current_status != args.status:
                    status_changes[current_status] = status_changes.get(current_status, 0) + 1
        else:
            # Get status changes for all matches
            status_change_data = conn.execute("""
                SELECT review_status, COUNT(*) 
                FROM files 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
                AND review_status != ?
                GROUP BY review_status
            """, (like_pattern, args.status)).fetchall()
            
            status_changes = dict(status_change_data)
        
        if status_changes:
            change_data = [[old_status, args.status, f"{count:,}"] 
                          for old_status, count in status_changes.items()]
            
            print("STATUS CHANGES (ALL MATCHING FILES):")
            print(tabulate(change_data, headers=["From", "To", "Files"], tablefmt="plain"))
            
            total_changes = sum(status_changes.values())
            unchanged = total_matches - total_changes
            
            print(f"\nSUMMARY:")
            print(f"   Will change: {total_changes:,} files")
            print(f"   Already correct: {unchanged:,} files")
            print(f"   Total affected: {total_matches:,} files")
        else:
            print(f"   All {total_matches:,} files already marked as '{args.status}'")
            print("   No changes needed")
            return
        
        # Perform the update
        timestamp = datetime.now().isoformat() + 'Z'
        
        if use_regex:
            file_ids = [str(f[0]) for f in all_matching_files]
            if file_ids:
                placeholders = ','.join(['?' for _ in file_ids])
                cursor = conn.execute(f"""
                    UPDATE files 
                    SET review_status = ?, reviewed_at = ? 
                    WHERE file_id IN ({placeholders})
                """, [args.status, timestamp] + file_ids)
        else:
            cursor = conn.execute("""
                UPDATE files 
                SET review_status = ?, reviewed_at = ? 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
            """, (args.status, timestamp, like_pattern))
        
        conn.commit()
        updated_count = cursor.rowcount
        
        # Show accurate completion message
        result_data = [
            ["Pattern", f"'{pattern}'"],
            ["New status", args.status],
            ["Files updated", f"{updated_count:,}"],
            ["Total matches", f"{total_matches:,}"]
        ]
        
        print(f"\n✅ BULK MARK COMPLETED:")
        print(tabulate(result_data, tablefmt="plain"))
        
        # Verify the counts match
        if updated_count != total_matches:
            print(f"\n📝 Note: {total_matches - updated_count:,} files were already marked as '{args.status}'")


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

def parse_file_size(size_str):
    """Parse human-readable file size strings."""
    if not size_str:
        return 0
    
    size_str = size_str.strip().upper()
    
    # Size multipliers (binary units)
    multipliers = {
        'B': 1,
        'K': 1024,
        'KB': 1024,
        'M': 1024 ** 2,
        'MB': 1024 ** 2,
        'G': 1024 ** 3,
        'GB': 1024 ** 3,
        'T': 1024 ** 4,
        'TB': 1024 ** 4,
    }
    
    # Pattern: number + optional unit
    pattern = r'^(\d+(?:\.\d+)?)\s*([KMGT]?B?)\s*$'
    match = re.match(pattern, size_str)
    
    if not match:
        raise ValueError(f"Invalid size format: {size_str}. Use formats like '1M', '500K', '2.5G'")
    
    number_str, unit = match.groups()
    number = float(number_str)
    
    # Default to bytes if no unit
    if not unit:
        unit = 'B'
    
    if unit not in multipliers:
        raise ValueError(f"Unknown unit: {unit}. Use B, K, KB, M, MB, G, GB, T, TB")
    
    return int(number * multipliers[unit])

def format_file_size(bytes_value):
    """Format bytes as human-readable string."""
    if bytes_value == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    size = float(bytes_value)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if size == int(size):
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"

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
    scan_parser.add_argument("--min-size", type=str, help="Minimum file size to include. Examples: '1M', '500K', '2.5G'. Files smaller are ignored.")
    
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
    bulk_mark_parser = subparsers.add_parser("bulk-mark", help="Bulk mark by path pattern (or preview matches)")
    bulk_mark_parser.add_argument("--path-like", required=True, help="Path pattern to match")
    bulk_mark_parser.add_argument("--status", choices=['undecided', 'keep', 'not_needed'], 
                                 help="Status to apply. If omitted, shows matching files instead.")
    bulk_mark_parser.add_argument("--regex", action="store_true", 
                                 help="Treat pattern as regular expression")
    bulk_mark_parser.add_argument("--limit", type=int, default=100, 
                                 help="Max matches to show in preview (default: 100)")
    bulk_mark_parser.add_argument("--show-paths", action="store_true",
                                 help="Show full paths instead of just filenames")
    
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