#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scan command implementation for the Media Consolidation Tool.
"""

import json
import os
import sqlite3
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional, Tuple, Set

from ..config import SUPPORTED_EXT, IMAGE_EXT, VIDEO_EXT, LARGE_FILE_BYTES
from ..database.manager import DatabaseManager
from ..checkpoint.manager import CheckpointManager
from ..models.checkpoint import ScanCheckpoint
from ..models.file_record import FileRecord
from ..scanning.detector import DuplicateDetector
from ..scanning.extractor import FeatureExtractor
from ..storage.drive import DriveManager
from ..utils.path import ensure_dir
from ..utils.time import utc_now_str


class ScanCommand:
    """Main scanner with pipeline architecture and checkpoint support."""
    
    def __init__(self, db_path: Path, central_path: Path):
        self.db_manager = DatabaseManager(db_path)
        self.central_path = central_path
        self.duplicate_detector = DuplicateDetector(self.db_manager)
        self.checkpoint_manager = CheckpointManager(self.db_manager)
        ensure_dir(central_path)
    
    def execute(self, source: Path, wsl_mode: bool = False, 
               drive_label: Optional[str] = None, drive_id_hint: Optional[str] = None,
               hash_large: bool = False, workers: int = 6, io_workers: int = 2,
               phash_threshold: int = 5, skip_discovery: bool = False,
               max_phash_pixels: int = 24_000_000, chunk_size: int = 100,
               resume_scan_id: Optional[str] = None, auto_checkpoint: bool = True):
        """Execute optimized scanning with checkpoint support."""
        
        # Configuration for checkpoints
        scan_config = {
            'wsl_mode': wsl_mode,
            'drive_label': drive_label,
            'drive_id_hint': drive_id_hint,
            'hash_large': hash_large,
            'workers': workers,
            'io_workers': io_workers,
            'phash_threshold': phash_threshold,
            'max_phash_pixels': max_phash_pixels,
            'chunk_size': chunk_size
        }
        
        self._print_scan_header(source, workers, io_workers, phash_threshold, 
                               hash_large, max_phash_pixels, chunk_size, auto_checkpoint)
        
        # Clean up old checkpoints
        if auto_checkpoint:
            self.checkpoint_manager.cleanup_old_checkpoints()
        
        # Handle resume logic
        checkpoint = self._handle_resume(resume_scan_id, source)
        
        # Get or create drive
        drive_id = self._get_drive_id(source, wsl_mode, drive_label, drive_id_hint, checkpoint)
        
        # Generate scan ID if starting new scan
        scan_id = checkpoint.scan_id if checkpoint else self.checkpoint_manager.generate_scan_id(str(source))
        
        try:
            # Execute scan pipeline
            self._execute_scan_pipeline(
                source, drive_id, scan_id, scan_config, checkpoint, 
                skip_discovery, auto_checkpoint, hash_large, io_workers, 
                max_phash_pixels, chunk_size, phash_threshold
            )
            
        except KeyboardInterrupt:
            if auto_checkpoint:
                print(f"\n⚠️  Scan interrupted! Resume with: --resume-scan-id {scan_id}")
            raise
        except Exception as e:
            if auto_checkpoint:
                print(f"\n❌ Scan failed! Resume with: --resume-scan-id {scan_id}")
            raise
        
        self._print_scan_footer()
    
    def _print_scan_header(self, source: Path, workers: int, io_workers: int, 
                          phash_threshold: int, hash_large: bool, max_phash_pixels: int,
                          chunk_size: int, auto_checkpoint: bool):
        """Print scan configuration header."""
        print("=" * 80)
        print(f"MEDIA TOOL OPTIMIZED SCAN - {utc_now_str()}")
        print("=" * 80)
        print(f"Source: {source}")
        print(f"Central: {self.central_path}")
        print(f"Workers: {workers} (I/O workers: {io_workers}), pHash threshold: {phash_threshold}")
        print(f"Large file threshold: {LARGE_FILE_BYTES//(1024**2)} MB")
        print(f"Hash large files: {hash_large}")
        print(f"Max pHash pixels: {max_phash_pixels:,}")
        print(f"Chunk size: {chunk_size}")
        print(f"Checkpoints: {'Enabled' if auto_checkpoint else 'Disabled'}")
        print()
    
    def _print_scan_footer(self):
        """Print scan completion footer."""
        print("=" * 80)
        print(f"SCAN COMPLETED - {utc_now_str()}")
        print("=" * 80)
    
    def _handle_resume(self, resume_scan_id: Optional[str], source: Path) -> Optional[ScanCheckpoint]:
        """Handle scan resume logic."""
        if not resume_scan_id:
            return None
            
        checkpoint = self.checkpoint_manager.load_checkpoint(resume_scan_id)
        if not checkpoint:
            print(f"❌ Could not load checkpoint {resume_scan_id}")
            return None
            
        print(f"📋 Resuming scan from checkpoint: {checkpoint.stage} stage")
        print(f"    - Processed: {checkpoint.processed_count:,} items")
        print(f"    - Timestamp: {checkpoint.timestamp}")
        
        # Validate checkpoint matches current parameters
        if checkpoint.source_path != str(source):
            print(f"❌ Source path mismatch: checkpoint={checkpoint.source_path}, current={source}")
            return None
            
        return checkpoint
    
    def _get_drive_id(self, source: Path, wsl_mode: bool, drive_label: Optional[str],
                     drive_id_hint: Optional[str], checkpoint: Optional[ScanCheckpoint]) -> int:
        """Get or create drive ID."""
        if checkpoint and checkpoint.drive_id:
            print(f"[{utc_now_str()}] Using cached drive ID: {checkpoint.drive_id}")
            return checkpoint.drive_id
        
        print(f"[{utc_now_str()}] Identifying drive...")
        return self._get_or_create_drive(source, wsl_mode, drive_label, drive_id_hint)
    
    def _execute_scan_pipeline(self, source: Path, drive_id: int, scan_id: str,
                              scan_config: dict, checkpoint: Optional[ScanCheckpoint],
                              skip_discovery: bool, auto_checkpoint: bool, hash_large: bool,
                              io_workers: int, max_phash_pixels: int, chunk_size: int,
                              phash_threshold: int):
        """Execute the main scan pipeline."""
        # Stage 1: Discovery
        candidates = self._discovery_stage(
            source, skip_discovery, scan_id, drive_id, scan_config, 
            auto_checkpoint, checkpoint
        )
        
        if not candidates:
            print("No media files found.")
            return
        
        # Stage 2: Feature extraction
        records = self._extraction_stage(
            candidates, drive_id, hash_large, io_workers, max_phash_pixels,
            chunk_size, scan_id, scan_config, auto_checkpoint, checkpoint
        )
        
        # Stage 3: Duplicate detection and grouping
        self._grouping_stage(
            records, phash_threshold, scan_id, scan_config, 
            auto_checkpoint, checkpoint
        )
        
        # Mark scan as completed
        if auto_checkpoint:
            completed_checkpoint = ScanCheckpoint(
                scan_id=scan_id,
                source_path=str(source),
                drive_id=drive_id,
                stage='completed',
                timestamp=utc_now_str(),
                processed_count=len(records) if records else 0,
                config=scan_config
            )
            self.checkpoint_manager.save_checkpoint(completed_checkpoint)
            print(f"  ✅ Scan marked as completed (ID: {scan_id})")
        
        # Final statistics
        self._print_final_stats()
    
    def _discovery_stage(self, source: Path, skip_discovery: bool, scan_id: str,
                        drive_id: int, config: dict, auto_checkpoint: bool,
                        checkpoint: Optional[ScanCheckpoint]) -> List[Tuple[Path, int]]:
        """Execute file discovery stage."""
        if checkpoint and checkpoint.stage in ['extraction', 'grouping', 'completed']:
            print(f"[{utc_now_str()}] Loading cached discovered files...")
            candidates = [(Path(p), s) for p, s in checkpoint.discovered_files]
            print(f"  - Loaded {len(candidates):,} files from checkpoint")
            return candidates
        
        # Implementation continues with existing discovery logic...
        return self._discover_files_with_checkpoint(
            source, skip_discovery, scan_id, drive_id, config, auto_checkpoint
        )
    
    def _extraction_stage(self, candidates: List[Tuple[Path, int]], drive_id: int,
                         hash_large: bool, io_workers: int, max_phash_pixels: int,
                         chunk_size: int, scan_id: str, config: dict, 
                         auto_checkpoint: bool, checkpoint: Optional[ScanCheckpoint]) -> List[FileRecord]:
        """Execute feature extraction stage."""
        if checkpoint and checkpoint.stage in ['grouping', 'completed']:
            print(f"[{utc_now_str()}] Skipping feature extraction (already completed)")
            return self._load_records_from_db(drive_id)
        
        start_batch = checkpoint.batch_number if checkpoint and checkpoint.stage == 'extraction' else 0
        return self._extract_features_with_checkpoint(
            candidates, drive_id, hash_large, io_workers, max_phash_pixels,
            chunk_size, scan_id, config, auto_checkpoint, start_batch
        )
    
    def _grouping_stage(self, records: List[FileRecord], phash_threshold: int,
                       scan_id: str, config: dict, auto_checkpoint: bool,
                       checkpoint: Optional[ScanCheckpoint]):
        """Execute duplicate detection and grouping stage."""
        if checkpoint and checkpoint.stage == 'completed':
            print(f"[{utc_now_str()}] Skipping grouping (scan already completed)")
            return
        
        if auto_checkpoint:
            checkpoint = ScanCheckpoint(
                scan_id=scan_id,
                source_path=config.get('source_path', ''),
                drive_id=records[0].drive_id if records else 0,
                stage='grouping',
                timestamp=utc_now_str(),
                processed_count=len(records),
                config=config
            )
            self.checkpoint_manager.save_checkpoint(checkpoint)
        
        self._process_duplicates_and_groups(records, phash_threshold)
    
    def _get_or_create_drive(self, source: Path, wsl_mode: bool,
                           drive_label: Optional[str], drive_id_hint: Optional[str]) -> int:
        """Get or create drive record."""
        # Implementation from original code
        pass  # Placeholder - implement existing logic
    
    def _discover_files_with_checkpoint(self, source: Path, skip_discovery: bool,
                                      scan_id: str, drive_id: int, config: dict,
                                      auto_checkpoint: bool) -> List[Tuple[Path, int]]:
        """Discovery with checkpoint support."""
        # Implementation from original code
        pass  # Placeholder - implement existing logic
    
    def _extract_features_with_checkpoint(self, candidates: List[Tuple[Path, int]],
                                        drive_id: int, hash_large: bool, io_workers: int,
                                        max_phash_pixels: int, chunk_size: int,
                                        scan_id: str, config: dict, auto_checkpoint: bool,
                                        start_batch: int) -> List[FileRecord]:
        """Feature extraction with checkpoint support."""
        # Implementation from original code  
        pass  # Placeholder - implement existing logic
    
    def _load_records_from_db(self, drive_id: int) -> List[FileRecord]:
        """Load FileRecord objects from database for resuming."""
        # Implementation from original code
        pass  # Placeholder - implement existing logic
    
    def _process_duplicates_and_groups(self, records: List[FileRecord], phash_threshold: int):
        """Process records for duplicates and create groups."""
        # Implementation from original code
        pass  # Placeholder - implement existing logic
    
    def _print_final_stats(self):
        """Print final scan statistics."""
        # Implementation from original code
        pass  # Placeholder - implement existing logic