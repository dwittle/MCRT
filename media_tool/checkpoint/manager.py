#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Checkpoint manager for resumable scans in the Media Consolidation Tool.
"""

import hashlib
import json
import pickle
import datetime as dt
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple

from ..models.checkpoint import ScanCheckpoint
from ..utils.path import ensure_dir
from ..database.manager import DatabaseManager


class CheckpointManager:
    """Manages scan checkpoints for resumability."""
    
    def __init__(self, db_manager: DatabaseManager, checkpoint_dir: Path = None):
        self.db_manager = db_manager
        self.checkpoint_dir = checkpoint_dir or Path(".checkpoints")
        ensure_dir(self.checkpoint_dir)
    
    def generate_scan_id(self, source_path: str) -> str:
        """Generate unique scan ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_hash = hashlib.md5(source_path.encode()).hexdigest()[:8]
        return f"scan_{timestamp}_{path_hash}"
    
    def save_checkpoint(self, checkpoint: ScanCheckpoint) -> Path:
        """Save checkpoint to disk and database."""
        checkpoint_file = self.checkpoint_dir / f"{checkpoint.scan_id}.pkl"
        
        # Save checkpoint data to file
        with checkpoint_file.open('wb') as f:
            pickle.dump(checkpoint, f)
        
        # Save checkpoint reference to database
        with self.db_manager.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO scan_checkpoints 
                (scan_id, source_path, drive_id, stage, timestamp, processed_count, 
                 batch_number, config_json, checkpoint_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                checkpoint.scan_id, checkpoint.source_path, checkpoint.drive_id,
                checkpoint.stage, checkpoint.timestamp, checkpoint.processed_count,
                checkpoint.batch_number, json.dumps(checkpoint.config or {}),
                str(checkpoint_file)
            ))
            conn.commit()
        
        print(f"  💾 Checkpoint saved: {checkpoint.stage} stage, {checkpoint.processed_count:,} items processed")
        return checkpoint_file
    
    def load_checkpoint(self, scan_id: str) -> Optional[ScanCheckpoint]:
        """Load checkpoint from disk."""
        try:
            with self.db_manager.get_connection() as conn:
                row = conn.execute("""
                    SELECT checkpoint_file FROM scan_checkpoints WHERE scan_id = ?
                """, (scan_id,)).fetchone()
                
                if not row:
                    return None
                
                checkpoint_file = Path(row[0])
                if not checkpoint_file.exists():
                    print(f"Warning: Checkpoint file {checkpoint_file} not found")
                    return None
                
                with checkpoint_file.open('rb') as f:
                    checkpoint = pickle.load(f)
                
                return checkpoint
                
        except Exception as e:
            print(f"Error loading checkpoint {scan_id}: {e}")
            return None
    
    def list_checkpoints(self, source_path: Optional[str] = None) -> List[Tuple[str, str, str, str, int]]:
        """List available checkpoints."""
        with self.db_manager.get_connection() as conn:
            if source_path:
                rows = conn.execute("""
                    SELECT scan_id, source_path, stage, timestamp, processed_count
                    FROM scan_checkpoints 
                    WHERE source_path = ?
                    ORDER BY timestamp DESC
                """, (source_path,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT scan_id, source_path, stage, timestamp, processed_count
                    FROM scan_checkpoints 
                    ORDER BY timestamp DESC
                """).fetchall()
        
        return rows
    
    def cleanup_checkpoint(self, scan_id: str):
        """Remove completed checkpoint."""
        try:
            with self.db_manager.get_connection() as conn:
                row = conn.execute("""
                    SELECT checkpoint_file FROM scan_checkpoints WHERE scan_id = ?
                """, (scan_id,)).fetchone()
                
                if row:
                    checkpoint_file = Path(row[0])
                    if checkpoint_file.exists():
                        checkpoint_file.unlink()
                
                conn.execute("DELETE FROM scan_checkpoints WHERE scan_id = ?", (scan_id,))
                conn.commit()
                
        except Exception as e:
            print(f"Warning: Failed to cleanup checkpoint {scan_id}: {e}")
    
    def cleanup_old_checkpoints(self, days: int = 7):
        """Clean up checkpoints older than specified days."""
        cutoff = datetime.now() - dt.timedelta(days=days)
        cutoff_str = cutoff.isoformat() + "Z"
        
        with self.db_manager.get_connection() as conn:
            old_checkpoints = conn.execute("""
                SELECT scan_id, checkpoint_file FROM scan_checkpoints 
                WHERE timestamp < ?
            """, (cutoff_str,)).fetchall()
            
            for scan_id, checkpoint_file in old_checkpoints:
                try:
                    Path(checkpoint_file).unlink(missing_ok=True)
                except Exception:
                    pass
            
            conn.execute("DELETE FROM scan_checkpoints WHERE timestamp < ?", (cutoff_str,))
            conn.commit()
            
            print(f"Cleaned up {len(old_checkpoints)} old checkpoints")