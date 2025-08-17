#!/usr/bin/env python3
"""
Complete Media Tool CLI with JSON support, auto-promotion, and all enhancements.
Supports: scanning, review, bulk operations, export, JSON output for UI integration.
"""

import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime

# Add the current directory to Python path so media_tool can be imported
# (smart path resolution):
current_dir = Path(__file__).parent
parent_dir = current_dir.parent

if (current_dir / 'media_tool').exists():
    sys.path.insert(0, str(current_dir))
elif (parent_dir / 'media_tool').exists():
    sys.path.insert(0, str(parent_dir))

# Import the working scanner and database manager
from media_tool.scanning.scanner import OptimizedScanner
from media_tool.database.manager import DatabaseManager

def parse_file_size(size_str):
    """Parse human-readable file size strings like '1M', '500K', '2.5G'."""
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

def check_auto_promotion(db_manager, file_id, json_mode=False):
    """
    Check if a file should be auto-promoted to original after marking.
    
    Auto-promotion happens when:
    1. All other files in a group are marked "not_needed"
    2. Only one file remains that is not "not_needed"
    3. That file is not already the original
    
    Args:
        db_manager: Database manager instance
        file_id: The file that was just marked
        json_mode: If True, suppress debug prints for JSON output
    """
    
    with db_manager.get_connection() as conn:
        # Get the group_id for this file
        file_info = conn.execute("""
            SELECT group_id FROM files WHERE file_id = ?
        """, (file_id,)).fetchone()
        
        if not file_info or not file_info[0]:
            # File is not in a group, no auto-promotion needed
            return None
        
        group_id = file_info[0]
        
        # Get all files in this group with their current status
        group_files = conn.execute("""
            SELECT file_id, review_status, 
                   CASE WHEN file_id = g.original_file_id THEN 1 ELSE 0 END as is_original
            FROM files f
            JOIN groups g ON g.group_id = f.group_id
            WHERE f.group_id = ?
        """, (group_id,)).fetchall()
        
        if len(group_files) <= 1:
            # Single file group, no auto-promotion needed
            return None
        
        # Count files by status
        not_needed_files = [f for f in group_files if f[1] == 'not_needed']
        remaining_files = [f for f in group_files if f[1] != 'not_needed']
        current_original = [f for f in group_files if f[2] == 1]
        
        # Only print debug info if not in JSON mode
        if not json_mode:
            print(f"🔍 Group {group_id} auto-promotion check:")
            print(f"   Total files: {len(group_files)}")
            print(f"   Not needed: {len(not_needed_files)}")
            print(f"   Remaining: {len(remaining_files)}")
            print(f"   Current original: {current_original[0][0] if current_original else 'None'}")
        
        # Auto-promotion condition: exactly 1 file remaining that's not marked "not_needed"
        if len(remaining_files) == 1:
            last_file_id, last_file_status, last_file_is_original = remaining_files[0]
            
            # Only promote if it's not already the original
            if not last_file_is_original:
                if not json_mode:
                    print(f"🎯 Auto-promoting file {last_file_id} in group {group_id} (last remaining)")
                
                # Update the group's original_file_id
                conn.execute("""
                    UPDATE groups SET original_file_id = ? WHERE group_id = ?
                """, (last_file_id, group_id))
                
                # Clear duplicate_of for the new original
                conn.execute("""
                    UPDATE files SET duplicate_of = NULL WHERE file_id = ?
                """, (last_file_id,))
                
                # Set old original as duplicate of new original (if there was one)
                if current_original:
                    old_original_id = current_original[0][0]
                    conn.execute("""
                        UPDATE files SET duplicate_of = ? WHERE file_id = ?
                    """, (last_file_id, old_original_id))
                
                # Update all other files in group to be duplicates of new original
                conn.execute("""
                    UPDATE files SET duplicate_of = ? 
                    WHERE group_id = ? AND file_id != ?
                """, (last_file_id, group_id, last_file_id))
                
                conn.commit()
                
                return {
                    'auto_promoted': True,
                    'new_original_id': last_file_id,
                    'old_original_id': current_original[0][0] if current_original else None,
                    'group_id': group_id,
                    'reason': 'last_remaining_file'
                }
            else:
                if not json_mode:
                    print(f"ℹ️ File {last_file_id} already original in group {group_id}")
        else:
            if not json_mode:
                print(f"ℹ️ No auto-promotion: {len(remaining_files)} files still unmarked in group {group_id}")
    
    return None

def cmd_scan(args):
    """Execute scan command with optional size filtering."""
    
    # Parse minimum size if provided
    min_size_bytes = 0
    if args.min_size:
        try:
            min_size_bytes = parse_file_size(args.min_size)
            print(f"🔍 Minimum file size filter: {format_file_size(min_size_bytes)} ({min_size_bytes:,} bytes)")
            print(f"   Files smaller than this will be ignored during scan")
        except ValueError as e:
            print(f"❌ Error: {e}")
            print(f"   Examples: '1M' (1 megabyte), '500K' (500 kilobytes), '2.5G' (2.5 gigabytes)")
            return
    
    # Use OptimizedScanner with size filter
    db_path = Path(args.db)
    central_path = Path(args.central)
    
    scanner = OptimizedScanner(db_path, central_path)
    
    scanner.scan_source(
        source=Path(args.source),
        wsl_mode=args.wsl_hfs_mode,
        drive_label=args.drive_label,
        drive_id_hint=args.drive_id,
        hash_large=args.hash_large,
        workers=args.workers,
        io_workers=args.io_workers,
        phash_threshold=args.phash_threshold,
        chunk_size=args.chunk_size,
        auto_checkpoint=not args.no_checkpoints,
        skip_discovery=args.skip_discovery,
        min_file_size=min_size_bytes
    )

def cmd_stats(args):
    """Show database statistics with optional JSON output."""
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
                SUM(CASE WHEN is_large=1 THEN 1 ELSE 0 END) as large_files,
                COUNT(CASE WHEN type='image' THEN 1 END) as image_count,
                COUNT(CASE WHEN type='video' THEN 1 END) as video_count
            FROM files
        """).fetchone()
        
        # Group statistics (if detailed)
        group_stats = {}
        if args.detailed:
            group_breakdown = conn.execute("""
                SELECT 
                    COUNT(CASE WHEN file_count = 1 THEN 1 END) as single_file_groups,
                    COUNT(CASE WHEN file_count > 1 THEN 1 END) as multi_file_groups,
                    MAX(file_count) as largest_group_size,
                    AVG(CAST(file_count AS FLOAT)) as avg_group_size
                FROM (
                    SELECT group_id, COUNT(*) as file_count
                    FROM files
                    WHERE group_id IS NOT NULL
                    GROUP BY group_id
                )
            """).fetchone()
            
            if group_breakdown:
                group_stats = {
                    'single_file_groups': group_breakdown[0] or 0,
                    'multi_file_groups': group_breakdown[1] or 0,
                    'largest_group_size': group_breakdown[2] or 0,
                    'avg_group_size': group_breakdown[3] or 0
                }
        
        # Prepare JSON data
        json_data = {
            "command": "stats",
            "timestamp": datetime.now().isoformat() + "Z",
            "data": {
                "files": {
                    "total": file_count,
                    "images": size_stats[4] if size_stats else 0,
                    "videos": size_stats[5] if size_stats else 0,
                    "large_files": size_stats[3] if size_stats else 0
                },
                "groups": {
                    "total": group_count,
                    "single_file": group_stats.get('single_file_groups', 0),
                    "multi_file": group_stats.get('multi_file_groups', 0),
                    "largest_size": group_stats.get('largest_group_size', 0),
                    "average_size": round(group_stats.get('avg_group_size', 0), 1)
                },
                "review_status": {
                    "undecided": status_counts.get('undecided', 0),
                    "keep": status_counts.get('keep', 0),
                    "not_needed": status_counts.get('not_needed', 0)
                },
                "storage": {
                    "total_bytes": size_stats[1] if size_stats else 0,
                    "total_gb": round((size_stats[1] or 0) / (1024**3), 2),
                    "average_mb": round((size_stats[2] or 0) / (1024**2), 1)
                },
                "drives": drive_count
            },
            "detailed": args.detailed
        }
    
    if args.json:
        print(json.dumps(json_data, indent=2))
    else:
        # Human-readable output
        print("=== Database Statistics ===")
        print(f"Files: {file_count:,}")
        print(f"Groups: {group_count:,}")
        print(f"Drives: {drive_count}")
        print()
        
        print("Review Status:")
        for status, count in status_counts.items():
            print(f"  {status}: {count:,}")
        print()
        
        if size_stats and size_stats[1]:  # total_bytes
            total_gb = size_stats[1] / (1024**3)
            avg_mb = size_stats[2] / (1024**2) if size_stats[2] else 0
            print(f"Storage: {total_gb:.1f} GB total, {avg_mb:.1f} MB average")
            print(f"Large files (>500MB): {size_stats[3]:,}")
            
        if args.detailed and group_stats:
            print(f"\nGroup Details:")
            print(f"  Single-file groups: {group_stats.get('single_file_groups', 0):,}")
            print(f"  Multi-file groups: {group_stats.get('multi_file_groups', 0):,}")
            print(f"  Largest group: {group_stats.get('largest_group_size', 0)} files")
            print(f"  Average group size: {group_stats.get('avg_group_size', 0):.1f} files")

def cmd_review_queue(args):
    """Show review queue with optional JSON output."""
    db_manager = DatabaseManager(Path(args.db))
    
    with db_manager.get_connection() as conn:
        rows = conn.execute("""
            SELECT 
                f.file_id, COALESCE(f.group_id, -1), f.type, f.width, f.height, 
                f.size_bytes, f.review_status, f.path_on_drive, d.label as drive_label
            FROM files f
            LEFT JOIN drives d ON d.drive_id = f.drive_id
            WHERE f.review_status='undecided'
            ORDER BY f.created_at DESC
            LIMIT ?
        """, (args.limit,)).fetchall()
        
        # Get total undecided count
        total_undecided = conn.execute("""
            SELECT COUNT(*) FROM files WHERE review_status='undecided'
        """).fetchone()[0]
        
        # Prepare data
        files_data = []
        for r in rows:
            file_id, gid, typ, w, h, size, status, path, drive_label = r
            
            files_data.append({
                "file_id": file_id,
                "group_id": gid if gid != -1 else None,
                "type": typ,
                "width": w,
                "height": h,
                "dimensions": f"{w}x{h}" if (w and h) else "Unknown",
                "size_bytes": size or 0,
                "size_mb": round((size or 0) / (1024*1024), 1),
                "review_status": status,
                "path": path,
                "filename": Path(path).name,
                "drive_label": drive_label
            })
        
        json_data = {
            "command": "review-queue",
            "timestamp": datetime.now().isoformat() + "Z",
            "data": {
                "files": files_data,
                "total_undecided": total_undecided,
                "limit": args.limit,
                "showing": len(files_data)
            }
        }
    
    if args.json:
        print(json.dumps(json_data, indent=2))
    else:
        # Human-readable table output
        if not rows:
            print("✅ No undecided items in review queue")
            return
        
        try:
            from tabulate import tabulate
            
            table_data = []
            for file_data in files_data:
                table_data.append([
                    file_data["file_id"],
                    file_data["group_id"] or "-",
                    file_data["type"],
                    file_data["dimensions"],
                    f"{file_data['size_mb']} MB",
                    file_data["review_status"],
                    file_data["filename"]
                ])
            
            print(f"📋 REVIEW QUEUE ({len(files_data):,} of {total_undecided:,} undecided files)")
            print(tabulate(table_data, 
                          headers=["ID", "Group", "Type", "Dimensions", "Size", "Status", "Filename"],
                          tablefmt="plain", maxcolwidths=None))
        except ImportError:
            # Fallback without tabulate
            print("file_id | group_id | type  | dimensions | size_bytes | status     | path")
            print("-" * 80)
            for r in rows:
                file_id, gid, typ, w, h, size, status, path = r
                dims = f"{w}x{h}" if (w and h) else "-"
                print(f"{file_id:7d} | {gid:8d} | {typ:5s} | {dims:>10s} | {size or 0:10d} | {status:10s} | {path}")

def cmd_bulk_mark(args):
    """Enhanced bulk mark with JSON support and accurate preview."""
    import re
    from media_tool.database.manager import DatabaseManager
    from datetime import datetime
    from pathlib import Path
    
    db_manager = DatabaseManager(Path(args.db))
    
    # Pattern setup
    use_regex = getattr(args, 'regex', False)
    pattern = args.path_like
    limit = getattr(args, 'limit', 100)
    show_paths = getattr(args, 'show_paths', False)
    
    with db_manager.get_connection() as conn:
        # Get TOTAL count first for accurate preview
        if use_regex:
            # For regex, get all files and count matches
            all_files = conn.execute("SELECT file_id, path_on_drive, review_status FROM files").fetchall()
            
            try:
                regex_pattern = re.compile(pattern, re.IGNORECASE)
                matching_file_ids = []
                status_groups = {}
                
                for file_id, path, review_status in all_files:
                    if regex_pattern.search(path):
                        matching_file_ids.append(file_id)
                        status_groups[review_status] = status_groups.get(review_status, 0) + 1
                        
                total_matches = len(matching_file_ids)
                
            except re.error as e:
                error_data = {
                    "command": "bulk-mark",
                    "error": f"Invalid regular expression: {e}",
                    "pattern": pattern
                }
                
                if args.json:
                    print(json.dumps(error_data, indent=2))
                else:
                    print(f"❌ Invalid regular expression: {e}")
                return
        else:
            # For LIKE, get total count efficiently
            like_pattern = f"%{pattern}%"
            
            total_matches = conn.execute("""
                SELECT COUNT(*) 
                FROM files 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
            """, (like_pattern,)).fetchone()[0]
            
            # Get status breakdown for all matches
            status_breakdown = conn.execute("""
                SELECT review_status, COUNT(*) 
                FROM files 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
                GROUP BY review_status
            """, (like_pattern,)).fetchall()
            
            status_groups = dict(status_breakdown)
        
        if total_matches == 0:
            no_match_data = {
                "command": "bulk-mark",
                "pattern": pattern,
                "mode": "regex" if use_regex else "like",
                "total_matches": 0,
                "error": "No files found matching pattern"
            }
            
            if args.json:
                print(json.dumps(no_match_data, indent=2))
            else:
                print(f"❌ No files found matching pattern: '{pattern}'")
            return
        
        # Get sample files for preview
        if use_regex and matching_file_ids:
            placeholders = ','.join(['?' for _ in matching_file_ids[:limit]])
            sample_files = conn.execute(f"""
                SELECT 
                    file_id, path_on_drive, size_bytes, width, height, 
                    review_status, type, group_id
                FROM files 
                WHERE file_id IN ({placeholders})
                ORDER BY path_on_drive
            """, matching_file_ids[:limit]).fetchall()
        else:
            sample_files = conn.execute("""
                SELECT 
                    file_id, path_on_drive, size_bytes, width, height, 
                    review_status, type, group_id
                FROM files 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
                ORDER BY path_on_drive
                LIMIT ?
            """, (like_pattern, limit)).fetchall()
        
        # Prepare sample data
        sample_files_data = []
        for file_info in sample_files:
            file_id, path, size_bytes, width, height, review_status, file_type, group_id = file_info
            
            sample_files_data.append({
                "file_id": file_id,
                "path": path,
                "filename": Path(path).name,
                "size_bytes": size_bytes or 0,
                "size_mb": round((size_bytes or 0) / (1024*1024), 1),
                "width": width,
                "height": height,
                "dimensions": f"{width}×{height}" if (width and height) else "Unknown",
                "review_status": review_status,
                "file_type": file_type,
                "group_id": group_id
            })
        
        # Calculate total impact
        if use_regex:
            total_size = sum(f[2] or 0 for f in sample_files)  # Approximate from sample
        else:
            total_size_result = conn.execute("""
                SELECT SUM(size_bytes) 
                FROM files 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
            """, (like_pattern,)).fetchone()[0]
            total_size = total_size_result or 0
        
        groups_affected = len({f["group_id"] for f in sample_files_data if f["group_id"]})
        
        # PREVIEW MODE
        if not args.status:
            preview_data = {
                "command": "bulk-mark",
                "timestamp": datetime.now().isoformat() + "Z",
                "pattern": pattern,
                "mode": "regex" if use_regex else "like",
                "action": "preview",
                "data": {
                    "total_matches": total_matches,
                    "status_breakdown": status_groups,
                    "sample_files": sample_files_data,
                    "impact": {
                        "files_affected": total_matches,
                        "total_size_bytes": total_size,
                        "total_size_gb": round(total_size / (1024**3), 2),
                        "groups_affected": groups_affected
                    }
                },
                "preview_limit": limit,
                "showing_sample": len(sample_files_data)
            }
            
            if args.json:
                print(json.dumps(preview_data, indent=2))
            else:
                # Human-readable output with tabulate
                try:
                    from tabulate import tabulate
                    
                    search_info = [
                        ["Pattern", f"'{pattern}'"],
                        ["Mode", "Regular Expression" if use_regex else "SQL LIKE (substring)"],
                        ["Total matches", f"{total_matches:,} files"],
                        ["Preview showing", f"{len(sample_files_data):,} files"]
                    ]
                    
                    print("🔍 BULK-MARK PATTERN SEARCH")
                    print(tabulate(search_info, tablefmt="plain"))
                    print()
                    
                    print(f"📊 PATTERN MATCHES {total_matches:,} FILES TOTAL")
                    print("=" * 80)
                    
                    # Status breakdown
                    status_data = [[status, f"{count:,}"] for status, count in sorted(status_groups.items())]
                    print("📊 STATUS BREAKDOWN (ALL MATCHING FILES):")
                    print(tabulate(status_data, headers=["Status", "Files"], tablefmt="plain"))
                    
                    # Sample files
                    table_data = []
                    for file_data in sample_files_data:
                        display_path = file_data["path"] if show_paths else file_data["filename"]
                        
                        table_data.append([
                            file_data["file_id"],
                            file_data["review_status"],
                            f"{file_data['size_mb']} MB",
                            file_data["dimensions"],
                            file_data["group_id"] or "-",
                            display_path
                        ])
                    
                    headers = ["ID", "Status", "Size", "Dimensions", "Group", 
                              "Full Path" if show_paths else "Filename"]
                    
                    print(f"\n📋 SAMPLE FILES (showing {len(sample_files_data):,} of {total_matches:,} total):")
                    print(tabulate(table_data, headers=headers, tablefmt="plain", maxcolwidths=None, stralign="left"))
                    
                    if total_matches > limit:
                        print(f"\n⚠️  IMPORTANT: Preview shows {limit:,} files")
                        print(f"   BUT PATTERN MATCHES {total_matches:,} FILES TOTAL")
                        print(f"   ALL {total_matches:,} files will be affected by status changes")
                    
                    # Action suggestions
                    print(f"\n💡 ACTIONS (will affect ALL {total_matches:,} matching files):")
                    regex_flag = " --regex" if use_regex else ""
                    
                    action_data = [
                        ["Mark not needed", f"./media_tool_cli.py bulk-mark --path-like '{pattern}'{regex_flag} --status not_needed"],
                        ["Keep all", f"./media_tool_cli.py bulk-mark --path-like '{pattern}'{regex_flag} --status keep"],
                        ["Reset to undecided", f"./media_tool_cli.py bulk-mark --path-like '{pattern}'{regex_flag} --status undecided"]
                    ]
                    
                    print(tabulate(action_data, headers=["Action", f"Command (affects {total_matches:,} files)"], 
                                  tablefmt="plain", maxcolwidths=None))
                    
                    # Impact summary
                    impact_data = [
                        ["Total files", f"{total_matches:,}"],
                        ["Total size", f"{total_size / (1024**3):.2f} GB"],
                        ["Groups affected", f"{groups_affected:,}"]
                    ]
                    
                    print(f"\n📈 IMPACT SUMMARY:")
                    print(tabulate(impact_data, tablefmt="plain"))
                    
                except ImportError:
                    print(f"📊 Found {total_matches:,} files matching '{pattern}'")
                    print("Install tabulate for better formatting: pip install tabulate")
            
            return
        
        # MARK MODE: Actually update files
        if args.status not in ['undecided', 'keep', 'not_needed']:
            error_data = {
                "command": "bulk-mark",
                "error": f"Invalid status: {args.status}",
                "valid_statuses": ['undecided', 'keep', 'not_needed']
            }
            
            if args.json:
                print(json.dumps(error_data, indent=2))
            else:
                print(f"❌ Invalid status: {args.status}")
            return
        
        # Calculate status changes
        changes_to_make = sum(count for status, count in status_groups.items() if status != args.status)
        already_correct = status_groups.get(args.status, 0)
        
        # Perform the update
        timestamp = datetime.now().isoformat() + 'Z'
        
        if use_regex and matching_file_ids:
            placeholders = ','.join(['?' for _ in matching_file_ids])
            cursor = conn.execute(f"""
                UPDATE files 
                SET review_status = ?, reviewed_at = ? 
                WHERE file_id IN ({placeholders})
            """, [args.status, timestamp] + matching_file_ids)
        else:
            cursor = conn.execute("""
                UPDATE files 
                SET review_status = ?, reviewed_at = ? 
                WHERE LOWER(path_on_drive) LIKE LOWER(?)
            """, (args.status, timestamp, like_pattern))
        
        conn.commit()
        updated_count = cursor.rowcount
        
        # Check for auto-promotions if we marked files as "not_needed"
        auto_promotions = []
        if args.status == 'not_needed' and not args.json:
            print(f"\n🔍 Checking for auto-promotion opportunities...")
            
            # Get unique group IDs that were affected
            if use_regex and matching_file_ids:
                placeholders = ','.join(['?' for _ in matching_file_ids])
                affected_groups = conn.execute(f"""
                    SELECT DISTINCT group_id FROM files 
                    WHERE file_id IN ({placeholders}) AND group_id IS NOT NULL
                """, matching_file_ids).fetchall()
            else:
                affected_groups = conn.execute("""
                    SELECT DISTINCT group_id FROM files 
                    WHERE LOWER(path_on_drive) LIKE LOWER(?) AND group_id IS NOT NULL
                """, (like_pattern,)).fetchall()
            
            for (group_id,) in affected_groups:
                # Check one file from this group for auto-promotion
                sample_file = conn.execute("""
                    SELECT file_id FROM files WHERE group_id = ? LIMIT 1
                """, (group_id,)).fetchone()
                
                if sample_file:
                    auto_promo = check_auto_promotion(db_manager, sample_file[0], json_mode=False)
                    if auto_promo:
                        auto_promotions.append(auto_promo)
        
        # Prepare result data
        result_data = {
            "command": "bulk-mark",
            "timestamp": datetime.now().isoformat() + "Z",
            "pattern": pattern,
            "mode": "regex" if use_regex else "like",
            "action": args.status,
            "data": {
                "total_matches": total_matches,
                "status_changes": {k: v for k, v in status_groups.items() if k != args.status},
                "files_updated": updated_count,
                "files_unchanged": already_correct,
                "files_affected": total_matches,
                "auto_promotions": auto_promotions
            },
            "result": "success"
        }
        
        if args.json:
            print(json.dumps(result_data, indent=2))
        else:
            try:
                from tabulate import tabulate
                
                print(f"🔍 BULK MARKING: {total_matches:,} files → '{args.status}'")
                
                if status_groups:
                    change_data = [[old_status, args.status, f"{count:,}"] 
                                  for old_status, count in status_groups.items() if old_status != args.status]
                    
                    if change_data:
                        print("\nSTATUS CHANGES:")
                        print(tabulate(change_data, headers=["From", "To", "Files"], tablefmt="plain"))
                
                result_table = [
                    ["Pattern", f"'{pattern}'"],
                    ["New status", args.status],
                    ["Files updated", f"{updated_count:,}"],
                    ["Total matches", f"{total_matches:,}"]
                ]
                
                print(f"\n✅ BULK MARK COMPLETED:")
                print(tabulate(result_table, tablefmt="plain"))
                
                if auto_promotions:
                    print(f"\n🎯 AUTO-PROMOTIONS ({len(auto_promotions)} groups):")
                    for promo in auto_promotions:
                        print(f"   Group {promo['group_id']}: File {promo['new_original_id']} → Original")
                
            except ImportError:
                print(f"✅ Bulk marked {updated_count:,} files matching '{pattern}' as {args.status}")
                if auto_promotions:
                    print(f"🎯 Auto-promoted {len(auto_promotions)} files to original")

def cmd_mark(args):
    """Mark file with optional JSON output and auto-promotion."""
    db_manager = DatabaseManager(Path(args.db))
    
    if args.status not in ['undecided', 'keep', 'not_needed']:
        error_data = {
            "command": "mark",
            "error": f"Invalid status: {args.status}",
            "valid_statuses": ['undecided', 'keep', 'not_needed']
        }
        
        if args.json:
            print(json.dumps(error_data, indent=2))
        else:
            print(f"❌ Invalid status: {args.status}")
        return
    
    with db_manager.get_connection() as conn:
        # Check if file exists and get current status
        file_info = conn.execute("SELECT review_status FROM files WHERE file_id=?", (args.file_id,)).fetchone()
        
        if not file_info:
            error_data = {
                "command": "mark",
                "error": f"File {args.file_id} not found",
                "file_id": args.file_id
            }
            
            if args.json:
                print(json.dumps(error_data, indent=2))
            else:
                print(f"❌ File {args.file_id} not found")
            return
        
        old_status = file_info[0]
        timestamp = datetime.now().isoformat() + 'Z'
        
        conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE file_id=?",
                    (args.status, timestamp, args.note or '', args.file_id))
        conn.commit()
        
        # Check for auto-promotion if we just marked something as "not_needed"
        auto_promotion = None
        if args.status == 'not_needed':
            auto_promotion = check_auto_promotion(db_manager, args.file_id, json_mode=args.json)
        
        result_data = {
            "command": "mark",
            "timestamp": timestamp,
            "data": {
                "file_id": args.file_id,
                "old_status": old_status,
                "new_status": args.status,
                "note": args.note or "",
                "changed": old_status != args.status,
                "auto_promotion": auto_promotion
            },
            "result": "success"
        }
        
        if args.json:
            print(json.dumps(result_data, indent=2))
        else:
            if old_status != args.status:
                print(f"✅ File {args.file_id}: {old_status} → {args.status}")
                
                if auto_promotion:
                    print(f"🎯 Auto-promoted file {auto_promotion['new_original_id']} to original (last remaining in group {auto_promotion['group_id']})")
            else:
                print(f"ℹ️ File {args.file_id} already marked as {args.status}")

def cmd_mark_group(args):
    """Mark group with optional JSON output."""
    db_manager = DatabaseManager(Path(args.db))
    
    with db_manager.get_connection() as conn:
        # Get current group status
        group_files = conn.execute("""
            SELECT review_status, COUNT(*) 
            FROM files 
            WHERE group_id = ?
            GROUP BY review_status
        """, (args.group_id,)).fetchall()
        
        if not group_files:
            error_data = {
                "command": "mark-group",
                "error": f"Group {args.group_id} not found or empty",
                "group_id": args.group_id
            }
            
            if args.json:
                print(json.dumps(error_data, indent=2))
            else:
                print(f"❌ Group {args.group_id} not found or empty")
            return
        
        old_status_breakdown = dict(group_files)
        total_files = sum(old_status_breakdown.values())
        
        timestamp = datetime.now().isoformat() + 'Z'
        cursor = conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE group_id=?",
                            (args.status, timestamp, args.note or '', args.group_id))
        conn.commit()
        
        result_data = {
            "command": "mark-group",
            "timestamp": timestamp,
            "data": {
                "group_id": args.group_id,
                "new_status": args.status,
                "old_status_breakdown": old_status_breakdown,
                "files_updated": cursor.rowcount,
                "total_files": total_files,
                "note": args.note or ""
            },
            "result": "success"
        }
        
        if args.json:
            print(json.dumps(result_data, indent=2))
        else:
            print(f"✅ Marked group {args.group_id} as {args.status} ({cursor.rowcount} files updated)")

def cmd_promote(args):
    """Promote file with optional JSON output."""
    db_manager = DatabaseManager(Path(args.db))
    
    with db_manager.get_connection() as conn:
        # Get file's group
        file_info = conn.execute("SELECT group_id FROM files WHERE file_id = ?", (args.file_id,)).fetchone()
        if not file_info:
            error_data = {
                "command": "promote",
                "error": f"File {args.file_id} not found",
                "file_id": args.file_id
            }
            
            if args.json:
                print(json.dumps(error_data, indent=2))
            else:
                print(f"❌ File {args.file_id} not found")
            return
        
        group_id = file_info[0]
        if not group_id:
            error_data = {
                "command": "promote",
                "error": f"File {args.file_id} is not in a group",
                "file_id": args.file_id
            }
            
            if args.json:
                print(json.dumps(error_data, indent=2))
            else:
                print(f"❌ File {args.file_id} is not in a group")
            return
        
        # Get current original
        current_orig = conn.execute("SELECT original_file_id FROM groups WHERE group_id = ?", (group_id,)).fetchone()
        old_original_id = current_orig[0] if current_orig else None
        
        # Update group and relationships
        conn.execute("UPDATE groups SET original_file_id = ? WHERE group_id = ?", (args.file_id, group_id))
        conn.execute("UPDATE files SET duplicate_of = NULL WHERE file_id = ?", (args.file_id,))
        
        if old_original_id:
            conn.execute("UPDATE files SET duplicate_of = ? WHERE file_id = ?", (args.file_id, old_original_id))
        
        conn.execute("UPDATE files SET duplicate_of = ? WHERE group_id = ? AND file_id != ?",
                    (args.file_id, group_id, args.file_id))
        conn.commit()
        
        result_data = {
            "command": "promote",
            "timestamp": datetime.now().isoformat() + "Z",
            "data": {
                "file_id": args.file_id,
                "group_id": group_id,
                "old_original_id": old_original_id,
                "new_original_id": args.file_id
            },
            "result": "success"
        }
        
        if args.json:
            print(json.dumps(result_data, indent=2))
        else:
            print(f"✅ Promoted file {args.file_id} to original of group {group_id}")

def cmd_export_backup_list(args):
    """Export backup list with optional JSON metadata."""
    db_manager = DatabaseManager(Path(args.db))
    
    # Only show progress messages if not in JSON mode
    if not args.json:
        print(f"Exporting backup manifest to {args.out}...")
    
    status_filter = "IN ('keep','undecided')" if args.include_undecided else "= 'keep'"
    
    with db_manager.get_connection() as conn:
        if not args.json:
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
        
        if not args.json:
            print(f" {len(rows):,} records")
    
    if not args.json:
        print("  - Writing CSV...", end="", flush=True)
        
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    
    import csv
    with open(args.out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["group_id", "original_file_id", "path", 
                        "width", "height", "size_bytes", "hash_sha256", "review_status"])
        writer.writerows(rows)
    
    if not args.json:
        print(" ✓")
    
    # Calculate totals
    total_size = sum(row[5] or 0 for row in rows)
    total_gb = total_size / (1024**3)
    
    export_metadata = {
        "command": "export-backup-list",
        "timestamp": datetime.now().isoformat() + "Z",
        "output_file": args.out,
        "options": {
            "include_undecided": args.include_undecided,
            "include_large": args.include_large
        },
        "data": {
            "files_exported": len(rows),
            "total_size_bytes": total_size,
            "total_size_gb": round(total_gb, 2)
        },
        "result": "success"
    }
    
    if args.json:
        print(json.dumps(export_metadata, indent=2))
    else:
        print(f"Export complete:")
        print(f"  - Files: {len(rows):,} originals")
        print(f"  - Size: {total_gb:.1f} GB")
        print(f"  - Location: {args.out}")

def cmd_cleanup_checkpoints(args):
    """Clean up checkpoints with optional JSON output."""
    from media_tool.checkpoint.manager import CheckpointManager
    
    db_manager = DatabaseManager(Path(args.db))
    checkpoint_manager = CheckpointManager(db_manager)
    
    if args.scan_id:
        checkpoint_manager.cleanup_checkpoint(args.scan_id)
        result_data = {
            "command": "cleanup-checkpoints",
            "timestamp": datetime.now().isoformat() + "Z",
            "action": "single",
            "scan_id": args.scan_id,
            "result": "success"
        }
        
        if args.json:
            print(json.dumps(result_data, indent=2))
        else:
            print(f"✅ Cleaned up checkpoint: {args.scan_id}")
    else:
        # Get count before cleanup
        checkpoints_before = len(checkpoint_manager.list_checkpoints())
        checkpoint_manager.cleanup_old_checkpoints(args.days)
        checkpoints_after = len(checkpoint_manager.list_checkpoints())
        
        cleaned_count = checkpoints_before - checkpoints_after
        
        result_data = {
            "command": "cleanup-checkpoints",
            "timestamp": datetime.now().isoformat() + "Z",
            "action": "bulk",
            "days": args.days,
            "data": {
                "checkpoints_before": checkpoints_before,
                "checkpoints_after": checkpoints_after,
                "checkpoints_cleaned": cleaned_count
            },
            "result": "success"
        }
        
        if args.json:
            print(json.dumps(result_data, indent=2))
        else:
            print(f"✅ Cleaned up {cleaned_count} checkpoints older than {args.days} days")

def create_parser():
    """Create argument parser with JSON support."""
    parser = argparse.ArgumentParser(
        description="Media Consolidation Tool - Complete CLI with JSON Support and Auto-Promotion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic scan with size filter
  %(prog)s scan --source ~/Pictures --central ./data --min-size 1M
  
  # Preview bulk operations
  %(prog)s bulk-mark --path-like screenshot
  %(prog)s bulk-mark --path-like "IMG_\\d{4}" --regex
  
  # Execute bulk operations  
  %(prog)s bulk-mark --path-like screenshot --status not_needed
  
  # JSON output for UI integration
  %(prog)s stats --json
  %(prog)s review-queue --json --limit 50
  %(prog)s bulk-mark --path-like pattern --json
  
  # Mark individual files (auto-promotion when appropriate)
  %(prog)s mark --file-id 123 --status not_needed
        """
    )
    
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
    scan_parser.add_argument("--wsl-hfs-mode", action="store_true", help="WSL HFS+ drive detection")
    scan_parser.add_argument("--drive-label", help="Override drive label")
    scan_parser.add_argument("--drive-id", help="Override drive ID")
    scan_parser.add_argument("--min-size", type=str, help="Minimum file size (e.g., '1M', '500K', '2.5G')")
    
    # STATS command
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.add_argument("--detailed", action="store_true", help="Show detailed breakdown")
    stats_parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    # REVIEW-QUEUE command
    queue_parser = subparsers.add_parser("review-queue", help="Show review queue")
    queue_parser.add_argument("--limit", type=int, default=100, help="Maximum items to show")
    queue_parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    # BULK-MARK command
    bulk_mark_parser = subparsers.add_parser("bulk-mark", help="Bulk mark by path pattern with auto-promotion")
    bulk_mark_parser.add_argument("--path-like", required=True, help="Path pattern to match")
    bulk_mark_parser.add_argument("--status", choices=['undecided', 'keep', 'not_needed'], 
                                 help="Status to apply (omit for preview)")
    bulk_mark_parser.add_argument("--regex", action="store_true", help="Use regex pattern")
    bulk_mark_parser.add_argument("--limit", type=int, default=100, help="Max preview results")
    bulk_mark_parser.add_argument("--show-paths", action="store_true", help="Show full paths")
    bulk_mark_parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    # MARK command
    mark_parser = subparsers.add_parser("mark", help="Mark file review status with auto-promotion")
    mark_parser.add_argument("--file-id", type=int, required=True, help="File ID to mark")
    mark_parser.add_argument("--status", choices=['undecided', 'keep', 'not_needed'], required=True)
    mark_parser.add_argument("--note", help="Optional review note")
    mark_parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    # MARK-GROUP command
    mark_group_parser = subparsers.add_parser("mark-group", help="Mark entire group status")
    mark_group_parser.add_argument("--group-id", type=int, required=True, help="Group ID to mark")
    mark_group_parser.add_argument("--status", choices=['undecided', 'keep', 'not_needed'], required=True)
    mark_group_parser.add_argument("--note", help="Optional review note")
    mark_group_parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    # PROMOTE command
    promote_parser = subparsers.add_parser("promote", help="Promote file to be group's original")
    promote_parser.add_argument("--file-id", type=int, required=True, help="File ID to promote")
    promote_parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    # EXPORT-BACKUP-LIST command
    export_parser = subparsers.add_parser("export-backup-list", help="Export backup manifest")
    export_parser.add_argument("--out", required=True, help="Output CSV file path")
    export_parser.add_argument("--include-undecided", action="store_true", help="Include undecided items")
    export_parser.add_argument("--include-large", action="store_true", help="Include large files")
    export_parser.add_argument("--json", action="store_true", help="Output metadata in JSON format")
    
    # CLEANUP-CHECKPOINTS command
    cleanup_parser = subparsers.add_parser("cleanup-checkpoints", help="Clean up old checkpoints")
    cleanup_parser.add_argument("--days", type=int, default=7, help="Remove checkpoints older than N days")
    cleanup_parser.add_argument("--scan-id", help="Remove specific checkpoint by scan ID")
    cleanup_parser.add_argument("--json", action="store_true", help="Output in JSON format")
    
    return parser

def main():
    """Main CLI entry point with complete command support, JSON output, and auto-promotion."""
    parser = create_parser()
    args = parser.parse_args()
    
    try:
        # Execute commands
        if args.command == "scan":
            cmd_scan(args)
        elif args.command == "stats":
            cmd_stats(args)
        elif args.command == "review-queue":
            cmd_review_queue(args)
        elif args.command == "bulk-mark":
            cmd_bulk_mark(args)
        elif args.command == "mark":
            cmd_mark(args)
        elif args.command == "mark-group":
            cmd_mark_group(args)
        elif args.command == "promote":
            cmd_promote(args)
        elif args.command == "export-backup-list":
            cmd_export_backup_list(args)
        elif args.command == "cleanup-checkpoints":
            cmd_cleanup_checkpoints(args)
    
    except KeyboardInterrupt:
        error_data = {
            "error": "Operation interrupted by user",
            "timestamp": datetime.now().isoformat() + "Z"
        }
        
        if hasattr(args, 'json') and args.json:
            print(json.dumps(error_data, indent=2))
        else:
            print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        error_data = {
            "error": str(e),
            "timestamp": datetime.now().isoformat() + "Z",
            "command": args.command if hasattr(args, 'command') else None
        }
        
        if hasattr(args, 'json') and args.json:
            print(json.dumps(error_data, indent=2))
        else:
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()