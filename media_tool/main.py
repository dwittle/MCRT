#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main CLI entry point for the Media Consolidation & Review Tool.
"""

import argparse
import sys
import logging
from pathlib import Path

from .config import REVIEW_STATUSES, DEFAULT_PHASH_THRESHOLD, LARGE_FILE_BYTES
from .database.manager import DatabaseManager
from .database.init import init_db_if_needed
from .commands.scan import ScanCommand
from .commands.checkpoint import cmd_list_checkpoints, cmd_cleanup_checkpoints, cmd_checkpoint_info
from .commands.review import (
    cmd_make_original, cmd_promote, cmd_move_to_group, cmd_mark, cmd_mark_group,
    cmd_bulk_mark, cmd_review_queue, cmd_export_backup_list
)
from .commands.stats import cmd_show_stats
from .jsonio import enable_json_logging


def setup_logging(verbose: bool):
    """Configure logging for the CLI tool."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.debug("Verbose logging enabled (DEBUG level).")


def create_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Media Consolidation & Review Tool - Enhanced with Checkpoint Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Scan a drive with checkpoint support
  %(prog)s scan --source /mnt/photos --central ./data --workers 4
  
  # Resume interrupted scan
  %(prog)s scan --source /mnt/photos --central ./data --resume-scan-id scan_20241210_143012_a1b2c3d4
  
  # Checkpoint management
  %(prog)s list-checkpoints --json
  %(prog)s checkpoint-info --scan-id scan_20241210_143012_a1b2c3d4 --json
  %(prog)s cleanup-checkpoints --days 7 --json
  
  # Review workflow
  %(prog)s review-queue --limit 50 --json
  %(prog)s mark --file-id 123 --status keep --json
  %(prog)s export-backup-list --out backup.csv --json
        """
    )
    
    # Global options
    parser.add_argument("--db", default="media_index.db", 
                       help="SQLite database path (default: media_index.db)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose (DEBUG) output")
    parser.add_argument("--json", action="store_true",
                       help="Output results as JSON instead of human-readable text")
    
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")
    
    # Add subcommands
    _add_scan_parser(subparsers)
    _add_checkpoint_parsers(subparsers)
    _add_correction_parsers(subparsers)
    _add_review_parsers(subparsers)
    _add_stats_parser(subparsers)
    
    return parser


def _add_scan_parser(subparsers):
    """Add scan command parser."""
    scan_parser = subparsers.add_parser("scan", help="Scan source path for media files")
    scan_parser.add_argument("--source", required=True,
                           help="Source path to scan (drive mount point)")
    scan_parser.add_argument("--central", required=True,
                           help="Central storage directory")
    scan_parser.add_argument("--wsl-hfs-mode", action="store_true",
                           help="Use WSL HFS+ drive detection (lsblk)")
    scan_parser.add_argument("--drive-label",
                           help="Override detected drive label")
    scan_parser.add_argument("--drive-id", 
                           help="Override detected drive serial/UUID")
    scan_parser.add_argument("--phash-threshold", type=int, default=DEFAULT_PHASH_THRESHOLD,
                           help=f"Perceptual hash Hamming distance threshold (default: {DEFAULT_PHASH_THRESHOLD})")
    scan_parser.add_argument("--workers", type=int, default=6,
                           help="Number of worker threads (default: 6)")
    scan_parser.add_argument("--io-workers", type=int, default=2,
                           help="Number of I/O worker threads for slow drives (default: 2)")
    scan_parser.add_argument("--large-threshold-mb", type=int, default=500,
                           help="Large file threshold in MB (default: 500)")
    scan_parser.add_argument("--max-phash-pixels", type=int, default=24_000_000,
                           help="Skip pHash for images larger than this (default: 24M pixels)")
    scan_parser.add_argument("--hash-large", action="store_true",
                           help="Compute hashes for large files (slower but more accurate)")
    scan_parser.add_argument("--skip-discovery", action="store_true",
                           help="Skip file discovery, reuse cached candidates")
    scan_parser.add_argument("--chunk-size", type=int, default=100,
                           help="Process files in chunks to reduce I/O contention (default: 100)")
    
    # Checkpoint options
    scan_parser.add_argument("--resume-scan-id", 
                           help="Resume scan from checkpoint with this ID")
    scan_parser.add_argument("--no-checkpoints", action="store_true",
                           help="Disable checkpoint saving")


def _add_checkpoint_parsers(subparsers):
    """Add checkpoint command parsers."""
    list_chk_parser = subparsers.add_parser("list-checkpoints", help="List available checkpoints")
    list_chk_parser.add_argument("--source", help="Filter by source path")
    list_chk_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    info_chk_parser = subparsers.add_parser("checkpoint-info", help="Show checkpoint details")
    info_chk_parser.add_argument("--scan-id", required=True, help="Checkpoint scan ID")
    info_chk_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    cleanup_chk_parser = subparsers.add_parser("cleanup-checkpoints", help="Clean up old checkpoints")
    cleanup_chk_parser.add_argument("--days", type=int, default=7,
                                   help="Remove checkpoints older than N days (default: 7)")
    cleanup_chk_parser.add_argument("--scan-id", help="Remove specific checkpoint by scan ID")
    cleanup_chk_parser.add_argument("--json", action="store_true", help="Output as JSON")


def _add_correction_parsers(subparsers):
    """Add correction command parsers."""
    make_orig_parser = subparsers.add_parser("make-original", 
                                           help="Split file into its own group as original")
    make_orig_parser.add_argument("--file-id", type=int, required=True,
                                help="File ID to make original")
    make_orig_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    promote_parser = subparsers.add_parser("promote",
                                         help="Promote file to be group's original")
    promote_parser.add_argument("--file-id", type=int, required=True,
                              help="File ID to promote")
    promote_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    move_parser = subparsers.add_parser("move-to-group",
                                      help="Move file to existing group")
    move_parser.add_argument("--file-id", type=int, required=True,
                           help="File ID to move")
    move_parser.add_argument("--group-id", type=int, required=True,
                           help="Target group ID")
    move_parser.add_argument("--json", action="store_true", help="Output as JSON")


def _add_review_parsers(subparsers):
    """Add review command parsers."""
    mark_parser = subparsers.add_parser("mark", help="Mark file review status")
    mark_parser.add_argument("--file-id", type=int, required=True,
                           help="File ID to mark")
    mark_parser.add_argument("--status", choices=list(REVIEW_STATUSES), required=True,
                           help="Review status")
    mark_parser.add_argument("--note", help="Optional review note")
    mark_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    mark_group_parser = subparsers.add_parser("mark-group", help="Mark entire group status")
    mark_group_parser.add_argument("--group-id", type=int, required=True,
                                 help="Group ID to mark")
    mark_group_parser.add_argument("--status", choices=list(REVIEW_STATUSES), required=True,
                                 help="Review status")
    mark_group_parser.add_argument("--note", help="Optional review note")
    mark_group_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    bulk_mark_parser = subparsers.add_parser("bulk-mark", help="Bulk mark by path pattern")
    bulk_mark_parser.add_argument("--path-like", required=True,
                                help="Path substring to match")
    bulk_mark_parser.add_argument("--status", choices=list(REVIEW_STATUSES), required=True,
                                help="Review status")
    bulk_mark_parser.add_argument("--limit", type=int, default=100,
                                help="Maximum files to process (default: 100)")
    bulk_mark_parser.add_argument("--preview", action="store_true",
                                help="Preview matches without applying changes")
    bulk_mark_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    queue_parser = subparsers.add_parser("review-queue", help="Show review queue")
    queue_parser.add_argument("--limit", type=int, default=100,
                            help="Maximum items to show (default: 100)")
    queue_parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    export_parser = subparsers.add_parser("export-backup-list", help="Export backup manifest")
    export_parser.add_argument("--out", required=True,
                              help="Output CSV file path")
    export_parser.add_argument("--include-undecided", action="store_true",
                              help="Include undecided items in export")
    export_parser.add_argument("--include-large", action="store_true", 
                              help="Include large files in export")
    export_parser.add_argument("--json", action="store_true", help="Output as JSON")


def _add_stats_parser(subparsers):
    """Add stats command parser."""
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.add_argument("--detailed", action="store_true",
                            help="Show detailed breakdown")
    stats_parser.add_argument("--json", action="store_true",
                            help="Output statistics as JSON")


def main():
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Setup logging based on --verbose (but suppress if JSON output requested)
    if getattr(args, 'json', False):
        # For JSON output, send logs to stderr and suppress info noise
        enable_json_logging()
    else:
        setup_logging(args.verbose)
    
    logging.debug("Parsed arguments: %s", args)
    
    # Initialize database manager
    db_path = Path(args.db)
    logging.info("Using database: %s", db_path)
    init_db_if_needed(db_path)
    db_manager = DatabaseManager(db_path)
    logging.debug("Database manager initialized.")
    
    try:
        # Apply global configuration from CLI args
        if args.command == "scan":
            import media_tool.config as config
            if hasattr(args, 'phash_threshold'):
                config.PHASH_THRESHOLD = args.phash_threshold
                logging.debug("Set PHASH_THRESHOLD = %d", args.phash_threshold)
            if hasattr(args, 'large_threshold_mb'):
                config.LARGE_FILE_BYTES = args.large_threshold_mb * 1024 * 1024
                logging.debug("Set LARGE_FILE_BYTES = %d", config.LARGE_FILE_BYTES)
        
        # Execute commands
        if args.command == "scan":
            logging.info("Starting scan command.")
            central_path = Path(args.central)
            scanner = ScanCommand(db_path, central_path)
            
            scanner.execute(
                source=Path(args.source),
                wsl_mode=args.wsl_hfs_mode,
                drive_label=args.drive_label,
                drive_id_hint=args.drive_id,
                hash_large=args.hash_large,
                workers=args.workers,
                io_workers=args.io_workers,
                phash_threshold=args.phash_threshold,
                skip_discovery=args.skip_discovery,
                max_phash_pixels=args.max_phash_pixels,
                chunk_size=args.chunk_size,
                resume_scan_id=args.resume_scan_id,
                auto_checkpoint=not args.no_checkpoints
            )
            logging.info("Scan completed.")
        
        elif args.command == "list-checkpoints":
            logging.info("Listing checkpoints...")
            return cmd_list_checkpoints(db_manager, args.source, getattr(args, 'json', False))
        
        elif args.command == "checkpoint-info":
            logging.info("Fetching checkpoint info for scan_id=%s", args.scan_id)
            return cmd_checkpoint_info(db_manager, args.scan_id, getattr(args, 'json', False))
        
        elif args.command == "cleanup-checkpoints":
            logging.info("Cleaning up checkpoints (days=%d, scan_id=%s)", args.days, getattr(args, 'scan_id', None))
            return cmd_cleanup_checkpoints(db_manager, args.days, getattr(args, 'scan_id', None), getattr(args, 'json', False))
        
        elif args.command == "make-original":
            logging.info("Making file %d original", args.file_id)
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT central_path FROM files WHERE central_path IS NOT NULL LIMIT 1").fetchone()
                central = Path(row[0]).parents[1] if row else Path.cwd()
            return cmd_make_original(db_manager, central, args.file_id, getattr(args, 'json', False))
        
        elif args.command == "promote":
            logging.info("Promoting file %d to group original", args.file_id)
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT central_path FROM files WHERE central_path IS NOT NULL LIMIT 1").fetchone()
                central = Path(row[0]).parents[1] if row else Path.cwd()
            return cmd_promote(db_manager, central, args.file_id, getattr(args, 'json', False))
        
        elif args.command == "move-to-group":
            logging.info("Moving file %d to group %d", args.file_id, args.group_id)
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT central_path FROM files WHERE central_path IS NOT NULL LIMIT 1").fetchone()
                central = Path(row[0]).parents[1] if row else Path.cwd()
            return cmd_move_to_group(db_manager, central, args.file_id, args.group_id, getattr(args, 'json', False))
        
        elif args.command == "mark":
            logging.info("Marking file %d as %s", args.file_id, args.status)
            return cmd_mark(db_manager, args.file_id, args.status, getattr(args, 'note', None), getattr(args, 'json', False))
        
        elif args.command == "mark-group":
            logging.info("Marking group %d as %s", args.group_id, args.status)
            return cmd_mark_group(db_manager, args.group_id, args.status, getattr(args, 'note', None), getattr(args, 'json', False))
        
        elif args.command == "bulk-mark":
            logging.info("Bulk marking files where path LIKE '%s' as %s", args.path_like, args.status)
            return cmd_bulk_mark(db_manager, args.path_like, args.status, 
                               getattr(args, 'limit', 100), getattr(args, 'preview', False), getattr(args, 'json', False))
        
        elif args.command == "review-queue":
            logging.info("Showing review queue (limit=%d)", args.limit)
            return cmd_review_queue(db_manager, args.limit, getattr(args, 'json', False))
        
        elif args.command == "export-backup-list":
            logging.info("Exporting backup list to %s", args.out)
            return cmd_export_backup_list(db_manager, Path(args.out), 
                                         args.include_undecided, args.include_large, getattr(args, 'json', False))
        
        elif args.command == "stats":
            logging.info("Showing database stats (detailed=%s)", args.detailed)
            return cmd_show_stats(db_manager, args.detailed, getattr(args, 'json', False))
    
    except KeyboardInterrupt:
        if getattr(args, 'json', False):
            from .jsonio import error
            return error(args.command, "Operation interrupted by user", code=130)
        else:
            logging.warning("Operation interrupted by user.")
            if args.command == "scan" and hasattr(args, 'resume_scan_id') and not args.no_checkpoints:
                print("💡 You can resume this scan later using the checkpoint system.")
            sys.exit(130)
    except Exception as e:
        if getattr(args, 'json', False):
            from .jsonio import error
            debug_info = {"exception_type": type(e).__name__} if args.verbose else None
            return error(args.command, str(e), debug=debug_info, code=1)
        else:
            logging.error("Error occurred: %s", e, exc_info=args.verbose)
            sys.exit(1)


if __name__ == "__main__":
    main()