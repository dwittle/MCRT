#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Statistics command implementations for the Media Consolidation Tool.
"""

from ..config import LARGE_FILE_BYTES
from ..database.manager import DatabaseManager


def cmd_show_stats(db_manager: DatabaseManager, detailed: bool = False):
    """Show database statistics."""
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
            print(f"Large files (>{LARGE_FILE_BYTES//(1024**2)}MB): {size_stats[3]:,}")
        
        if detailed:
            print("\n=== Detailed Breakdown ===")
            
            # Type breakdown
            type_counts = dict(conn.execute("""
                SELECT type, COUNT(*) FROM files GROUP BY type
            """).fetchall())
            print("File types:")
            for ftype, count in type_counts.items():
                print(f"  {ftype}: {count:,}")
            
            # Drive breakdown
            drive_info = conn.execute("""
                SELECT d.label, d.mount_path, COUNT(f.file_id) as file_count,
                       SUM(f.size_bytes) as total_bytes
                FROM drives d
                LEFT JOIN files f ON f.drive_id = d.drive_id
                GROUP BY d.drive_id
                ORDER BY file_count DESC
            """).fetchall()
            
            print("\nDrive breakdown:")
            for label, mount, count, bytes_total in drive_info:
                gb = (bytes_total or 0) / (1024**3)
                print(f"  {label or 'Unknown'} ({mount}): {count:,} files, {gb:.1f} GB")