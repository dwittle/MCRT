#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Checkpoint command implementations for the Media Consolidation Tool.
"""

from typing import Optional
from ..database.manager import DatabaseManager
from ..checkpoint.manager import CheckpointManager
from ..jsonio import success, error


def cmd_list_checkpoints(db_manager: DatabaseManager, source_path: Optional[str] = None, as_json: bool = False):
    """List available checkpoints."""
    checkpoint_manager = CheckpointManager(db_manager)
    checkpoints = checkpoint_manager.list_checkpoints(source_path)
    
    if as_json:
        checkpoint_data = []
        for scan_id, source, stage, timestamp, processed_count in checkpoints:
            checkpoint_data.append({
                "scan_id": scan_id,
                "source_path": source,
                "stage": stage,
                "timestamp": timestamp,
                "processed_count": processed_count
            })
        
        return success("list-checkpoints", {
            "checkpoints": checkpoint_data,
            "total_count": len(checkpoints),
            "source_filter": source_path
        })
    
    # Human-readable output
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


def cmd_cleanup_checkpoints(db_manager: DatabaseManager, days: int = 7, scan_id: Optional[str] = None, as_json: bool = False):
    """Clean up old checkpoints."""
    checkpoint_manager = CheckpointManager(db_manager)
    
    if scan_id:
        # Check if checkpoint exists before cleanup
        checkpoint = checkpoint_manager.load_checkpoint(scan_id)
        if not checkpoint:
            if as_json:
                return error("cleanup-checkpoints", f"Checkpoint {scan_id} not found")
            else:
                print(f"Checkpoint {scan_id} not found.")
                return
        
        checkpoint_manager.cleanup_checkpoint(scan_id)
        if as_json:
            return success("cleanup-checkpoints", {
                "mode": "single",
                "scan_id": scan_id,
                "message": f"Cleaned up checkpoint: {scan_id}"
            })
        else:
            print(f"Cleaned up checkpoint: {scan_id}")
    else:
        # Get count before cleanup for reporting
        checkpoints_before = checkpoint_manager.list_checkpoints()
        checkpoint_manager.cleanup_old_checkpoints(days)
        checkpoints_after = checkpoint_manager.list_checkpoints()
        
        cleaned_count = len(checkpoints_before) - len(checkpoints_after)
        
        if as_json:
            return success("cleanup-checkpoints", {
                "mode": "bulk",
                "days": days,
                "cleaned_count": cleaned_count,
                "remaining_count": len(checkpoints_after),
                "message": f"Cleaned up {cleaned_count} checkpoints older than {days} days"
            })
        else:
            print(f"Cleaned up {cleaned_count} checkpoints older than {days} days")


def cmd_checkpoint_info(db_manager: DatabaseManager, scan_id: str, as_json: bool = False):
    """Show detailed checkpoint information."""
    checkpoint_manager = CheckpointManager(db_manager)
    checkpoint = checkpoint_manager.load_checkpoint(scan_id)
    
    if not checkpoint:
        if as_json:
            return error("checkpoint-info", f"Checkpoint {scan_id} not found")
        else:
            print(f"Checkpoint {scan_id} not found.")
            return
    
    if as_json:
        return success("checkpoint-info", {
            "scan_id": checkpoint.scan_id,
            "source_path": checkpoint.source_path,
            "stage": checkpoint.stage,
            "timestamp": checkpoint.timestamp,
            "drive_id": checkpoint.drive_id,
            "processed_count": checkpoint.processed_count,
            "batch_number": checkpoint.batch_number,
            "discovered_files_count": len(checkpoint.discovered_files) if checkpoint.discovered_files else 0,
            "config": checkpoint.config or {}
        })
    
    # Human-readable output
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