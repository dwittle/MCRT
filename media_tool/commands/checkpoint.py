#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Checkpoint command implementations for the Media Consolidation Tool.
"""

from typing import Optional

from ..database.manager import DatabaseManager
from ..checkpoint.manager import CheckpointManager


def cmd_list_checkpoints(db_manager: DatabaseManager, source_path: Optional[str] = None):
    """List available checkpoints."""
    checkpoint_manager = CheckpointManager(db_manager)
    checkpoints = checkpoint_manager.list_checkpoints(source_path)
    
    if not checkpoints:
        print("No checkpoints found.")
        return
    
    print("Available checkpoints:")
    print(f"{'Scan ID':<25} {'Source Path':<40} {'Stage':<12} {'Timestamp':<20} {'Items':<10}")
    print("-" * 110)
    
    for scan_id, source, stage, timestamp, processed_count in checkpoints:
        # Truncate long paths
        short_source = source if len(source) <= 37 else "..." + source[-34:]
        print(f"{scan_id:<25} {short_source:<40} {stage:<12} {timestamp:<20} {processed_count:<10,}")


def cmd_cleanup_checkpoints(db_manager: DatabaseManager, days: int = 7, scan_id: Optional[str] = None):
    """Clean up old checkpoints."""
    checkpoint_manager = CheckpointManager(db_manager)
    
    if scan_id:
        checkpoint_manager.cleanup_checkpoint(scan_id)
        print(f"Cleaned up checkpoint: {scan_id}")
    else:
        checkpoint_manager.cleanup_old_checkpoints(days)
        print(f"Cleaned up checkpoints older than {days} days")


def cmd_checkpoint_info(db_manager: DatabaseManager, scan_id: str):
    """Show detailed checkpoint information."""
    checkpoint_manager = CheckpointManager(db_manager)
    checkpoint = checkpoint_manager.load_checkpoint(scan_id)
    
    if not checkpoint:
        print(f"Checkpoint {scan_id} not found.")
        return
    
    print(f"=== Checkpoint: {scan_id} ===")
    print(f"Source: {checkpoint.source_path}")
    print(f"Stage: {checkpoint.stage}")
    print(f"Timestamp: {checkpoint.timestamp}")
    print(f"Drive ID: {checkpoint.drive_id}")
    print(f"Processed: {checkpoint.processed_count:,} items")
    
    if checkpoint.stage == 'extraction':
        print(f"Batch: {checkpoint.batch_number + 1}")
    
    if checkpoint.discovered_files:
        print(f"Discovered files: {len(checkpoint.discovered_files):,}")
    
    if checkpoint.config:
        print("\nConfiguration:")
        for key, value in checkpoint.config.items():
            print(f"  {key}: {value}")