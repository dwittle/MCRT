#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main scanner integration for the Media Consolidation Tool.
Coordinates all scanning phases: discovery, extraction, and grouping.
"""

import csv
from datetime import datetime, timezone
import os
import sqlite3
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional, Tuple, Set

from ..config import LARGE_FILE_BYTES, IMAGE_EXT, VIDEO_EXT
from ..database.manager import DatabaseManager
from ..checkpoint.manager import CheckpointManager
from ..models.checkpoint import ScanCheckpoint
from ..models.file_record import FileRecord
from ..scanning.discovery import FileDiscovery
from ..scanning.detector import DuplicateDetector
from ..scanning.extractor import FeatureExtractor
from ..storage.drive import DriveManager
from ..utils.path import ensure_dir
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeout
from typing import Any, Callable

class OptimizedScanner:
    """
    Main scanner that coordinates all scanning phases with checkpoint support.
    This is the high-level orchestrator that was previously the main scanning class.
    """
    
    def __init__(self, db_path: Path, central_path: Path):
        self.db_manager = DatabaseManager(db_path)
        self.central_path = central_path
        self.duplicate_detector = DuplicateDetector(self.db_manager)
        self.checkpoint_manager = CheckpointManager(self.db_manager)
        self.file_discovery = FileDiscovery(self.checkpoint_manager)
        ensure_dir(central_path)
        
    def utc_now_str(self) -> str:
        """Return current UTC time in ISO-8601 format with 'Z'."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def now_iso(self) -> str:
        """Return current UTC time in ISO format for database storage."""
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    def with_timeout(fn: Callable[..., Any], seconds: float, *args, **kwargs) -> Any:
        if seconds is None or seconds <= 0:
            return fn(*args, **kwargs)
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fn, *args, **kwargs)
            try:
                return fut.result(timeout=seconds)
            except _FuturesTimeout as e:
                raise TimeoutError(f"Operation exceeded {seconds} seconds") from e

    def _run_with_timeout(self, label: str, seconds: float, func, **kwargs):
        """ Run a stage function with optional timeout (0/None disables)."""
        if not seconds or seconds <= 0:
            return func(**kwargs)
        try:
            # Run via utils.timeouts.with_timeout
            return self.with_timeout(lambda: func(**kwargs), seconds)
        except TimeoutError:
            print(f"[TIMEOUT] Stage '{label}' exceeded {seconds} seconds")
            raise
        self.db_manager = DatabaseManager(db_path)
        self.central_path = central_path
        self.duplicate_detector = DuplicateDetector(self.db_manager)
        self.checkpoint_manager = CheckpointManager(self.db_manager)
        self.file_discovery = FileDiscovery(self.checkpoint_manager)
        ensure_dir(central_path)
    
    def scan_source(self, source: Path, wsl_mode: bool = False, 
                   drive_label: Optional[str] = None, drive_id_hint: Optional[str] = None,
                   hash_large: bool = False, workers: int = 6, io_workers: int = 2,
                   phash_threshold: int = 5, skip_discovery: bool = False,
                   max_phash_pixels: int = 24_000_000, chunk_size: int = 100,
                   resume_scan_id: Optional[str] = None, auto_checkpoint: bool = True):
        """
        Execute complete scan pipeline with checkpoint support.
        This is the main entry point that coordinates all scanning phases.
        """
        # Configuration for checkpoints
        scan_config = {
            'source_path': str(source),
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
            
            # Mark scan as completed
            if auto_checkpoint:
                self._mark_scan_completed(scan_id, str(source), drive_id, scan_config)
            
            # Final statistics
            self._print_final_stats()
            
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
        print(f"MEDIA TOOL OPTIMIZED SCAN - {self.utc_now_str()}")
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
        print(f"SCAN COMPLETED - {self.utc_now_str()}")
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
            print(f"[{self.utc_now_str()}] Using cached drive ID: {checkpoint.drive_id}")
            return checkpoint.drive_id
        
        print(f"[{self.utc_now_str()}] Identifying drive...")
        return self._get_or_create_drive(source, wsl_mode, drive_label, drive_id_hint)
    
    def _execute_scan_pipeline(self, source: Path, drive_id: int, scan_id: str,
                              scan_config: dict, checkpoint: Optional[ScanCheckpoint],
                              skip_discovery: bool, auto_checkpoint: bool, hash_large: bool,
                              io_workers: int, max_phash_pixels: int, chunk_size: int,
                              phash_threshold: int):
        """Execute the main scan pipeline stages."""
        
        # Stage 1: Discovery
        candidates = self._run_with_timeout('discovery', float(os.getenv('MCRT_STAGE_TIMEOUT_SECONDS', '0')), self._discovery_stage, 
            source, skip_discovery, scan_id, drive_id, scan_config, 
            auto_checkpoint, checkpoint
        )
        
        if not candidates:
            print("No media files found.")
            return
        
        # Stage 2: Feature extraction
        records = self._run_with_timeout('extraction', float(os.getenv('MCRT_STAGE_TIMEOUT_SECONDS', '0')), self._extraction_stage, 
            candidates, drive_id, hash_large, io_workers, max_phash_pixels,
            chunk_size, scan_id, scan_config, auto_checkpoint, checkpoint
        )
        
        # Stage 3: Duplicate detection and grouping
        self._run_with_timeout('grouping', float(os.getenv('MCRT_STAGE_TIMEOUT_SECONDS', '0')), self._grouping_stage, 
            records, phash_threshold, scan_id, scan_config, 
            auto_checkpoint, checkpoint
        )
    
    def _discovery_stage(self, source: Path, skip_discovery: bool, scan_id: str,
                        drive_id: int, config: dict, auto_checkpoint: bool,
                        checkpoint: Optional[ScanCheckpoint]) -> List[Tuple[Path, int]]:
        """Execute file discovery stage."""
        if checkpoint and checkpoint.stage in ['extraction', 'grouping', 'completed']:
            print(f"[{self.utc_now_str()}] Loading cached discovered files...")
            candidates = [(Path(p), s) for p, s in checkpoint.discovered_files]
            print(f"  - Loaded {len(candidates):,} files from checkpoint")
            return candidates
        
        # Use FileDiscovery class for discovery
        return self.file_discovery.discover_files(
            source=source,
            skip_discovery=skip_discovery,
            scan_id=scan_id,
            drive_id=drive_id,
            config=config,
            auto_checkpoint=auto_checkpoint
        )
    
    def _extraction_stage(self, candidates: List[Tuple[Path, int]], drive_id: int,
                         hash_large: bool, io_workers: int, max_phash_pixels: int,
                         chunk_size: int, scan_id: str, config: dict, 
                         auto_checkpoint: bool, checkpoint: Optional[ScanCheckpoint]) -> List[FileRecord]:
        """Execute feature extraction stage."""
        if checkpoint and checkpoint.stage in ['grouping', 'completed']:
            print(f"[{self.utc_now_str()}] Skipping feature extraction (already completed)")
            return self._load_records_from_db(drive_id, str(config.get('source_path', '')))
        
        with self.db_manager.get_connection() as conn:
            existing_count = conn.execute("""
                SELECT COUNT(*) FROM files WHERE drive_id = ?
            """, (drive_id,)).fetchone()[0]
        
        if existing_count >= len(candidates):
            print(f"[{self.utc_now_str()}] Files already processed ({existing_count} files in DB), skipping extraction and grouping")
            print(f"  - Candidates to process: {len(candidates)}")
            print(f"  - Files already in DB: {existing_count}")
            return self._load_records_from_db(drive_id, str(config.get('source_path', '')))

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
            print(f"[{self.utc_now_str()}] Skipping grouping (scan already completed)")
            return
        
        if records:  # Make sure we have records to check
            with self.db_manager.get_connection() as conn:
                # Check how many files are already grouped
                grouped_count = conn.execute("""
                    SELECT COUNT(*) FROM files 
                    WHERE drive_id = ? AND group_id IS NOT NULL
                """, (records[0].drive_id,)).fetchone()[0]
                
                if grouped_count >= len(records):
                    print(f"[{self.utc_now_str()}] Files already grouped ({grouped_count} files), skipping grouping stage")
                    print(f"  - Records to group: {len(records)}")
                    print(f"  - Files already grouped: {grouped_count}")
                    return  # Exit early, don't do any grouping

        # Save grouping checkpoint
        if auto_checkpoint:
            grouping_checkpoint = ScanCheckpoint(
                scan_id=scan_id,
                source_path=config.get('source_path', ''),
                drive_id=records[0].drive_id if records else 0,
                stage='grouping',
                timestamp=self.utc_now_str(),
                processed_count=len(records),
                config=config
            )
            self.checkpoint_manager.save_checkpoint(grouping_checkpoint)
        
        # Execute grouping logic
        self._process_duplicates_and_groups(records, phash_threshold)
    
    def _get_or_create_drive(self, source: Path, wsl_mode: bool,
                           drive_label: Optional[str], drive_id_hint: Optional[str]) -> int:
        """Get or create drive record."""
        print(f"  - Detecting drive information...", end="", flush=True)
        label, serial_or_uuid, mount_path = DriveManager.detect_drive_info(source, wsl_mode)
        
        # Override with user-provided values
        label = drive_label or label
        serial_or_uuid = drive_id_hint or serial_or_uuid
        
        with self.db_manager.get_connection() as conn:
            row = conn.execute("SELECT drive_id FROM drives WHERE mount_path=?", (mount_path,)).fetchone()
            if row:
                print(f" found existing drive {row[0]}")
                return int(row[0])
            
            cursor = conn.execute(
                "INSERT INTO drives (label, serial_or_uuid, mount_path) VALUES (?, ?, ?)",
                (label, serial_or_uuid, mount_path)
            )
            conn.commit()
            drive_id = cursor.lastrowid
            
            print(f" created new drive {drive_id}")
            if label:
                print(f"    Label: {label}")
            if serial_or_uuid:
                print(f"    ID: {serial_or_uuid}")
            print(f"    Mount: {mount_path}")
            
            return drive_id
    
    def _extract_features_with_checkpoint(self, candidates: List[Tuple[Path, int]],
                                        drive_id: int, hash_large: bool, io_workers: int,
                                        max_phash_pixels: int, chunk_size: int,
                                        scan_id: str, config: dict, auto_checkpoint: bool,
                                        start_batch: int) -> List[FileRecord]:
        """Feature extraction with checkpoint support."""
        
        print(f"[{self.utc_now_str()}] Analyzing file characteristics...")
        
        # Pre-analysis for optimization
        size_counts = Counter(size for _, size in candidates)
        existing_buckets = self.duplicate_detector.get_existing_buckets()
        existing_sizes = self._get_existing_sizes()
        
        # Show optimization stats if starting fresh
        if start_batch == 0:
            self._print_extraction_stats(candidates, size_counts, existing_sizes)
        
        feature_extractor = FeatureExtractor(
            max_phash_pixels=max_phash_pixels,
            hash_large=hash_large
        )
        
        records = []
        processed_count = 0
        
        print(f"[{self.utc_now_str()}] Processing {len(candidates):,} files with {io_workers} I/O workers (chunk size: {chunk_size})...")
        if start_batch > 0:
            print(f"  - Resuming from batch {start_batch + 1}")
        
        # Process in chunks to reduce I/O contention
        total_chunks = (len(candidates) + chunk_size - 1) // chunk_size
        
        for chunk_idx in range(start_batch, total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(candidates))
            chunk = candidates[start_idx:end_idx]
            
            print(f"  - Processing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk)} files)...")
            
            # Process chunk with limited I/O workers
            chunk_records = self._process_extraction_chunk(
                chunk, drive_id, feature_extractor, size_counts, 
                existing_sizes, existing_buckets, io_workers
            )
            
            # Insert chunk to database immediately
            if chunk_records:
                self.db_manager.batch_insert_files(chunk_records)
                records.extend(chunk_records)
            
            processed_count += len(chunk)
            
            # Save checkpoint after every few chunks
            if auto_checkpoint and (chunk_idx + 1) % 5 == 0:
                checkpoint = ScanCheckpoint(
                    scan_id=scan_id,
                    source_path=config.get('source_path', ''),
                    drive_id=drive_id,
                    stage='extraction',
                    timestamp=self.utc_now_str(),
                    discovered_files=[(str(p), s) for p, s in candidates],
                    processed_count=processed_count,
                    batch_number=chunk_idx,
                    config=config
                )
                self.checkpoint_manager.save_checkpoint(checkpoint)
        
        # Final extraction checkpoint
        if auto_checkpoint:
            final_checkpoint = ScanCheckpoint(
                scan_id=scan_id,
                source_path=config.get('source_path', ''),
                drive_id=drive_id,
                stage='extraction',
                timestamp=self.utc_now_str(),
                discovered_files=[(str(p), s) for p, s in candidates],
                processed_count=processed_count,
                batch_number=total_chunks - 1,
                config=config
            )
            self.checkpoint_manager.save_checkpoint(final_checkpoint)
        
        print(f"[{self.utc_now_str()}] Feature extraction complete: {len(records):,} records processed")
        return records
    
    def _process_extraction_chunk(self, chunk: List[Tuple[Path, int]], drive_id: int,
                                 extractor: FeatureExtractor, size_counts: Counter,
                                 existing_sizes: Set[int], existing_buckets: Set[Tuple[int, str]],
                                 io_workers: int) -> List[FileRecord]:
        """Process a single extraction chunk with threading."""
        chunk_records = []
        
        with ThreadPoolExecutor(max_workers=io_workers) as executor:
            futures = []
            
            for path, size in chunk:
                unique_size = size_counts[size] == 1 and size not in existing_sizes
                
                future = executor.submit(
                    extractor.extract_features,
                    path, size, unique_size, existing_buckets
                )
                futures.append((future, drive_id))
            
            # Collect chunk results
            for future, drive_id in futures:
                try:
                    record = future.result()
                    if record:
                        record.drive_id = drive_id
                        chunk_records.append(record)
                except Exception as e:
                    print(f"Feature extraction error: {e}")
        
        return chunk_records
    
    def _load_records_from_db(self, drive_id: int, source_path: str) -> List[FileRecord]:
        """Load FileRecord objects from database for resuming."""
        print(f"[{self.utc_now_str()}] Loading processed records from database...")
        
        records = []
        with self.db_manager.get_connection() as conn:
            rows = conn.execute("""
                SELECT path_on_drive, size_bytes, type, hash_sha256, phash, 
                       width, height, is_large, fast_fp
                FROM files 
                WHERE drive_id = ?
            """, (drive_id,)).fetchall()
            
            for row in rows:
                path, size, file_type, sha256, phash, width, height, is_large, fast_fp = row
                
                record = FileRecord(
                    path=path,
                    size_bytes=size,
                    file_type=file_type,
                    drive_id=drive_id,
                    fast_fp=fast_fp,
                    sha256=sha256,
                    width=width,
                    height=height,
                    phash=phash,
                    is_large=bool(is_large)
                )
                records.append(record)
        
        print(f"  - Loaded {len(records):,} processed records")
        return records
    
    def _process_duplicates_and_groups(self, records: List[FileRecord], phash_threshold: int):
        """Process records for duplicates and create groups."""
        print(f"[{self.utc_now_str()}] Analyzing {len(records):,} records for duplicates...")
        
        # Group similar files within the current batch (FIXED LOGIC)
        groups = self._group_similar_records(records, phash_threshold)
        
        print(f"[{self.utc_now_str()}] Grouping complete:")
        
        duplicate_groups = [g for g in groups if len(g) > 1] 
        single_files = [g for g in groups if len(g) == 1]
        
        print(f"  - Groups with duplicates: {len(duplicate_groups)}")
        print(f"  - Single files: {len(single_files)}")
        
        # Process groups
        total_duplicates = 0
        for group_records in duplicate_groups:
            total_duplicates += len(group_records) - 1  # All but one are duplicates
            self._create_group_from_records(group_records)
        
        # Process single files
        for group_records in single_files:
            self._create_group_from_records(group_records)
        
        print(f"  - Total duplicates found: {total_duplicates}")
        print(f"[{self.utc_now_str()}] Database updates complete")

    def _group_similar_records(self, records: List[FileRecord], phash_threshold: int) -> List[List[FileRecord]]:
        """Group similar records within the batch."""
        
        # Group by SHA-256 first (exact duplicates)
        from collections import defaultdict
        import imagehash
        
        sha_groups = defaultdict(list)
        no_sha_records = []
        
        for record in records:
            if record.sha256:
                sha_groups[record.sha256].append(record)
            else:
                no_sha_records.append(record)
        
        # Group by perceptual hash
        phash_groups = defaultdict(list)  
        no_phash_records = []
        
        for record in no_sha_records:
            if record.phash and record.file_type == 'image':
                phash_groups[record.phash].append(record)
            else:
                no_phash_records.append(record)
        
        # Find similar pHash groups
        similar_phash_groups = []
        processed_phashes = set()
        
        for phash1, records1 in phash_groups.items():
            if phash1 in processed_phashes:
                continue
                
            similar_group = records1.copy()
            processed_phashes.add(phash1)
            
            for phash2, records2 in phash_groups.items():
                if phash2 in processed_phashes:
                    continue
                    
                try:
                    hash1 = imagehash.hex_to_hash(phash1)
                    hash2 = imagehash.hex_to_hash(phash2)
                    distance = hash1 - hash2
                    
                    if distance <= phash_threshold:
                        similar_group.extend(records2)
                        processed_phashes.add(phash2)
                except Exception:
                    continue
            
            similar_phash_groups.append(similar_group)
        
        # Collect all groups
        all_groups = []
        
        # Add SHA groups
        for group_records in sha_groups.values():
            all_groups.append(group_records)
        
        # Add pHash groups  
        all_groups.extend(similar_phash_groups)
        
        # Add single files
        for record in no_phash_records:
            all_groups.append([record])
        
        return all_groups

    def _create_group_from_records(self, group_records: List[FileRecord]):
        """Create a group from similar records."""
        if not group_records:
            return
        
        # Find the best original (largest resolution, then largest file size)
        original_record = max(group_records, key=lambda r: (r.pixels, r.size_bytes))
        
        with self.db_manager.get_connection() as conn:
            # Insert all files first
            file_ids = []
            for record in group_records:
                file_id = self._insert_or_get_file(conn, record)
                file_ids.append(file_id)
            
            # Create group with best original
            original_file_id = file_ids[group_records.index(original_record)]
            group_cursor = conn.execute("INSERT INTO groups (original_file_id) VALUES (?)", (original_file_id,))
            group_id = group_cursor.lastrowid
            
            # Update all files in group
            for file_id, record in zip(file_ids, group_records):
                if file_id == original_file_id:
                    # This is the original
                    conn.execute("UPDATE files SET group_id=?, duplicate_of=NULL WHERE file_id=?", 
                            (group_id, file_id))
                else:
                    # This is a duplicate
                    conn.execute("UPDATE files SET group_id=?, duplicate_of=? WHERE file_id=?",
                            (group_id, original_file_id, file_id))
            
            conn.commit()
    
    def _print_extraction_stats(self, candidates: List[Tuple[Path, int]], 
                               size_counts: Counter, existing_sizes: Set[int]):
        """Print feature extraction optimization statistics."""
        # File type analysis
        type_counts = Counter()
        for path, _ in candidates:
            ext = path.suffix.lower()
            if ext in IMAGE_EXT:
                type_counts['image'] += 1
            elif ext in VIDEO_EXT:
                type_counts['video'] += 1
            else:
                type_counts['unknown'] += 1
        
        print(f"  - File type breakdown: {dict(type_counts)}")
        
        # Optimization stats
        unique_sizes = sum(1 for count in size_counts.values() if count == 1)
        repeated_sizes = len(size_counts) - unique_sizes
        total_repeated_files = sum(count for count in size_counts.values() if count > 1)
        files_needing_sha = sum(1 for _, size in candidates 
                               if not (size_counts[size] == 1 and size not in existing_sizes))
        
        print(f"  - Size analysis: {unique_sizes:,} unique sizes, {repeated_sizes:,} repeated sizes")
        print(f"  - Potential duplicates: {total_repeated_files:,} files with repeated sizes")
        print(f"  - Will compute SHA: {files_needing_sha:,} files")
        print(f"  - Will compute pHash: {type_counts.get('image', 0):,} images")
    
    def _get_existing_sizes(self) -> Set[int]:
        """Get sizes of existing files."""
        with self.db_manager.get_connection() as conn:
            return {row[0] for row in conn.execute("SELECT DISTINCT size_bytes FROM files")}
    
    def _batch_insert_large_files(self, records: List[FileRecord]):
        """Batch insert large file placeholders."""
        print(f"[{self.utc_now_str()}] Inserting {len(records):,} large file placeholders...")
        self.db_manager.batch_insert_files(records)
    
    def _create_new_groups(self, records: List[FileRecord]):
        """Create new groups for original files."""
        if not records:
            return
            
        print(f"[{self.utc_now_str()}] Creating {len(records):,} new groups...")
        
        created_groups = 0
        start_time = time.perf_counter()
        
        with self.db_manager.get_connection() as conn:
            for record in records:
                # Insert file if it doesn't exist
                file_id = self._insert_or_get_file(conn, record)
                
                # Create group
                group_cursor = conn.execute("INSERT INTO groups (original_file_id) VALUES (?)", (file_id,))
                group_id = group_cursor.lastrowid
                
                # Update file with group
                conn.execute("UPDATE files SET group_id=? WHERE file_id=?", (group_id, file_id))
                created_groups += 1
                
                if created_groups % 1000 == 0:
                    conn.commit()
                    elapsed = time.perf_counter() - start_time
                    rate = created_groups / elapsed if elapsed > 0 else 0
                    print(f"  - Created {created_groups:,}/{len(records):,} groups ({rate:.0f}/s)...", flush=True)
            
            conn.commit()
        
        elapsed = time.perf_counter() - start_time
        rate = created_groups / elapsed if elapsed > 0 else 0
        print(f"  - Created {created_groups:,} new groups in {elapsed:.1f}s ({rate:.0f} groups/s) ✓")
    
    def _process_similar_files(self, similar: List[Tuple[FileRecord, int]]):
        """Process files that go into existing groups."""
        if not similar:
            return
            
        print(f"[{self.utc_now_str()}] Processing {len(similar):,} files into existing groups...")
        
        promotions = 0
        duplicates_added = 0
        
        with self.db_manager.get_connection() as conn:
            for record, group_id in similar:
                # Insert file
                file_id = self._insert_or_get_file(conn, record, group_id)
                
                # Check if this should become the new original
                current_orig = conn.execute("""
                    SELECT f.file_id, f.width, f.height, f.size_bytes
                    FROM groups g JOIN files f ON f.file_id = g.original_file_id
                    WHERE g.group_id = ?
                """, (group_id,)).fetchone()
                
                if self._should_promote(record, current_orig):
                    self._promote_to_original(conn, file_id, group_id, current_orig[0] if current_orig else None)
                    promotions += 1
                else:
                    # Set as duplicate of current original
                    if current_orig:
                        conn.execute("UPDATE files SET duplicate_of=? WHERE file_id=?", 
                                   (current_orig[0], file_id))
                    duplicates_added += 1
            
            conn.commit()
        
        print(f"  - Promotions: {promotions:,} files became new group originals")
        print(f"  - Duplicates: {duplicates_added:,} files added as duplicates")
    
    def _insert_or_get_file(self, conn: sqlite3.Connection, record: FileRecord, 
                           group_id: Optional[int] = None) -> int:
        """Insert file or get existing file ID."""
        # Check if file already exists
        existing_file = conn.execute("""
            SELECT file_id FROM files 
            WHERE drive_id = ? AND path_on_drive = ?
        """, (record.drive_id, record.path)).fetchone()
        
        if existing_file:
            file_id = existing_file[0]
            if group_id:
                conn.execute("UPDATE files SET group_id = ? WHERE file_id = ?", (group_id, file_id))
        else:
            # Insert new file
            cursor = conn.execute("""
                INSERT INTO files 
                (hash_sha256, phash, width, height, size_bytes, type, drive_id,
                 path_on_drive, is_large, copied, group_id, fast_fp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.sha256, record.phash, record.width, record.height,
                record.size_bytes, record.file_type, record.drive_id,
                record.path, int(record.is_large), 0, group_id, record.fast_fp
            ))
            file_id = cursor.lastrowid
        
        return file_id
    
    def _should_promote(self, candidate: FileRecord, current_orig: Optional[Tuple]) -> bool:
        """Check if candidate should become the group original - largest wins."""
        if not current_orig:
            return True
            
        _, curr_w, curr_h, curr_size = current_orig
        curr_pixels = (curr_w or 0) * (curr_h or 0)
        
        # Larger pixel count wins (higher resolution)
        if candidate.pixels > curr_pixels:
            print(f"    Promoting {Path(candidate.path).name}: {candidate.width}x{candidate.height} "
                  f"({candidate.pixels:,} pixels) > {curr_w}x{curr_h} ({curr_pixels:,} pixels)")
            return True
        
        # Same pixels, larger file size wins (less compression)
        if candidate.pixels == curr_pixels and candidate.size_bytes > (curr_size or 0):
            size_mb = candidate.size_bytes / (1024*1024)
            curr_mb = (curr_size or 0) / (1024*1024)
            print(f"    Promoting {Path(candidate.path).name}: same resolution but larger file "
                  f"({size_mb:.1f}MB > {curr_mb:.1f}MB)")
            return True
            
        return False
    
    def _promote_to_original(self, conn: sqlite3.Connection, new_file_id: int, 
                           group_id: int, old_original_id: Optional[int]):
        """Promote file to be group original."""
        conn.execute("UPDATE groups SET original_file_id=? WHERE group_id=?", (new_file_id, group_id))
        conn.execute("UPDATE files SET duplicate_of=NULL WHERE file_id=?", (new_file_id,))
        
        if old_original_id:
            conn.execute("UPDATE files SET duplicate_of=? WHERE file_id=?", (new_file_id, old_original_id))
        
        conn.execute("UPDATE files SET duplicate_of=? WHERE group_id=? AND file_id != ?",
                    (new_file_id, group_id, new_file_id))
    
    def _generate_large_files_review(self, large_files: List[FileRecord]):
        """Generate CSV of large files for manual review."""
        review_path = self.central_path / "large_files_review.csv"
        ensure_dir(review_path.parent)
        
        print(f"  - Writing large files review to {review_path}...", end="", flush=True)
        
        with review_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["source_path", "size_bytes", "size_mb", "type"])
            for record in large_files:
                size_mb = record.size_bytes / (1024 * 1024)
                writer.writerow([record.path, record.size_bytes, f"{size_mb:.1f}", record.file_type])
        
        print(f" {len(large_files):,} files ✓")
        print(f"> Review large files: {review_path}")
    
    def _mark_scan_completed(self, scan_id: str, source_path: str, drive_id: int, config: dict):
        """Mark scan as completed in checkpoint system."""
        completed_checkpoint = ScanCheckpoint(
            scan_id=scan_id,
            source_path=source_path,
            drive_id=drive_id,
            stage='completed',
            timestamp=self.utc_now_str(),
            processed_count=0,  # Will be updated with actual count
            config=config
        )
        self.checkpoint_manager.save_checkpoint(completed_checkpoint)
        print(f"  ✅ Scan marked as completed (ID: {scan_id})")
    
    def _print_final_stats(self):
        """Print final scan statistics."""
        print(f"[{self.utc_now_str()}] Generating scan summary...")
        
        with self.db_manager.get_connection() as conn:
            # Overall counts
            total_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            total_groups = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
            
            # Size breakdown
            size_stats = conn.execute("""
                SELECT 
                    SUM(size_bytes) as total_bytes,
                    COUNT(CASE WHEN is_large=1 THEN 1 END) as large_count,
                    COUNT(CASE WHEN type='image' THEN 1 END) as image_count,
                    COUNT(CASE WHEN type='video' THEN 1 END) as video_count
                FROM files
            """).fetchone()
            
            # Duplicate stats
            dup_stats = conn.execute("""
                SELECT 
                    COUNT(CASE WHEN duplicate_of IS NOT NULL THEN 1 END) as duplicates,
                    COUNT(CASE WHEN duplicate_of IS NULL THEN 1 END) as originals
                FROM files
            """).fetchone()
            
            total_gb = (size_stats[0] or 0) / (1024**3)
            
            print()
            print("=== SCAN SUMMARY ===")
            print(f"Total files processed: {total_files:,}")
            print(f"Groups created: {total_groups:,}")
            print(f"Total storage: {total_gb:.1f} GB")
            print()
            print(f"File types: {size_stats[2]:,} images, {size_stats[3]:,} videos")
            print(f"Large files (>{LARGE_FILE_BYTES//(1024**2)}MB): {size_stats[1]:,}")
            print()
            print(f"Deduplication: {dup_stats[1]:,} originals, {dup_stats[0]:,} duplicates")
            if total_files > 0:
                dedup_ratio = (dup_stats[0] / total_files) * 100
                print(f"Duplicate ratio: {dedup_ratio:.1f}%")
            print()