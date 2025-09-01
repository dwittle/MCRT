#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Statistics command implementations for the Media Consolidation Tool.

- Uses Python logging instead of print.
- New arg: as_json=False. If True, writes a JSON object to stdout.
"""

import json
import logging
import sys
from typing import Dict, Any

from ..config import LARGE_FILE_BYTES
from ..database.manager import DatabaseManager


def cmd_show_stats(
    db_manager: DatabaseManager,
    detailed: bool = False,
    as_json: bool = False,
) -> Dict[str, Any]:
    """Show database statistics.

    Args:
        db_manager: DatabaseManager instance.
        detailed: If True, include per-type and per-drive breakdowns.
        as_json: If True, emit a single JSON object to stdout instead of logs.

    Returns:
        A dict of computed statistics (returned regardless of output mode).
    """
    logger = logging.getLogger(__name__)

    with db_manager.get_connection() as conn:
        # Basic counts
        file_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        group_count = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
        drive_count = conn.execute("SELECT COUNT(*) FROM drives").fetchone()[0]

        # File status breakdown
        status_rows = conn.execute(
            """
            SELECT review_status, COUNT(*)
            FROM files
            GROUP BY review_status
            """
        ).fetchall()
        status_counts = {row[0] if row[0] is not None else "unknown": row[1] for row in status_rows}

        # Size statistics
        size_stats = conn.execute(
            """
            SELECT
                COUNT(*) AS total_files,
                SUM(size_bytes) AS total_bytes,
                AVG(size_bytes) AS avg_bytes,
                SUM(CASE WHEN is_large=1 THEN 1 ELSE 0 END) AS large_files
            FROM files
            """
        ).fetchone()
        total_files = size_stats[0] or 0
        total_bytes = size_stats[1] or 0
        avg_bytes = size_stats[2] or 0
        large_files = size_stats[3] or 0

        results: Dict[str, Any] = {
            "counts": {
                "files": int(file_count or 0),
                "groups": int(group_count or 0),
                "drives": int(drive_count or 0),
            },
            "review_status": status_counts,
            "storage": {
                "total_files": int(total_files),
                "total_bytes": int(total_bytes),
                "avg_bytes": int(avg_bytes) if avg_bytes else 0,
                "large_files": int(large_files or 0),
                "large_threshold_bytes": int(LARGE_FILE_BYTES),
            },
        }

        if detailed or as_json:
            # Type breakdown
            type_rows = conn.execute(
                "SELECT type, COUNT(*) FROM files GROUP BY type"
            ).fetchall()
            type_counts = {row[0] if row[0] is not None else "unknown": row[1] for row in type_rows}
            results["types"] = type_counts

        drive_rows = conn.execute(
            """
            SELECT
                d.label,
                d.mount_path AS mount_path,
                COUNT(f.file_id) AS file_count,
                COALESCE(SUM(f.size_bytes), 0) AS total_bytes
            FROM drives d
            LEFT JOIN files f ON f.drive_id = d.drive_id
            GROUP BY d.drive_id
            ORDER BY file_count DESC
            """
        ).fetchall()

        results["drives"] = []
        for (label, mount_path, count, bytes_total) in drive_rows:
            results["drives"].append({
                "label": label,
                "mount_path": mount_path,
                "file_count": int(count or 0),
                "total_bytes": int(bytes_total or 0),
            })

    # Output
    if as_json:
        sys.stdout.write(json.dumps(results, indent=2, ensure_ascii=False))
        sys.stdout.write("\n")
    else:
        logger.info("=== Database Statistics ===")
        logger.info("Files: %s", f"{results['counts']['files']:,}")
        logger.info("Groups: %s", f"{results['counts']['groups']:,}")
        logger.info("Drives: %s", f"{results['counts']['drives']:,}")

        logger.info("Review Status:")
        if results["review_status"]:
            for status, count in results["review_status"].items():
                logger.info("  %s: %s", status, f"{count:,}")
        else:
            logger.info("  (none)")

        total_gb = results["storage"]["total_bytes"] / (1024 ** 3) if results["storage"]["total_bytes"] else 0.0
        avg_mb = results["storage"]["avg_bytes"] / (1024 ** 2) if results["storage"]["avg_bytes"] else 0.0
        logger.info("Storage: %.1f GB total, %.1f MB average", total_gb, avg_mb)

        threshold_mb = results["storage"]["large_threshold_bytes"] // (1024 ** 2)
        logger.info("Large files (>%s MB): %s", f"{threshold_mb:,}", f"{results['storage']['large_files']:,}")

        if detailed:
            logger.info("=== Detailed Breakdown ===")
            logger.info("File types:")
            for ftype, count in results.get("types", {}).items():
                logger.info("  %s: %s", ftype, f"{count:,}")

            logger.info("Drive breakdown:")
            for d in results.get("drives", []):
                gb = (d["total_bytes"] or 0) / (1024 ** 3)
                logger.info("  %s (%s): %s files, %.1f GB",
                            d["label"], d["mount"], f"{d['file_count']:,}", gb)

    return results
