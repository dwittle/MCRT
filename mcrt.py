#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Media Consolidation & Review Tool - Enhanced with Checkpoint Support

Key enhancements:
1. Comprehensive checkpoint system for resuming interrupted scans
2. Progress persistence at discovery, feature extraction, and grouping stages
3. Automatic cleanup of completed checkpoints
4. Validation of checkpoint integrity
"""

import argparse
import contextlib
import csv
import datetime as dt
import hashlib
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import json
import warnings
import pickle
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict, Set, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from queue import Queue, Empty
from dataclasses import dataclass, asdict
from collections import defaultdict, Counter

import imagehash
from tqdm import tqdm
from PIL import Image

# Suppress PIL warnings
warnings.filterwarnings("ignore", category=UserWarning, 
                       message=".*Palette images with Transparency expressed in bytes.*")

# Configuration globals (set by CLI args)
PHASH_THRESHOLD = 5
LARGE_FILE_BYTES = 500 * 1024 * 1024
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp", ".heic"}
VIDEO_EXT = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".mpeg", ".mpg"}
SUPPORTED_EXT = IMAGE_EXT | VIDEO_EXT

ORIGINALS_DIRNAME = "originals"
DUPLICATES_DIRNAME = "duplicates" 
GROUPS_DIRNAME = "groups"
REVIEW_STATUSES = {"undecided", "keep", "not_needed"}

def utc_now_str():
    """Return current UTC time in ISO-8601 format with 'Z'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

@dataclass
class ScanCheckpoint:
    """Checkpoint data structure for resuming scans"""
    scan_id: str
    source_path: str
    drive_id: int
    stage: str  # 'discovery', 'extraction', 'grouping', 'completed'
    timestamp: str
    
    # Stage-specific data
    discovered_files: Optional[List[Tuple[str, int]]] = None  # (path, size)
    processed_count: int = 0
    batch_number: int = 0
    
    # Configuration
    config: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScanCheckpoint':
        return cls(**data)

@dataclass
class FileRecord:
    """Immutable file record for pipeline processing"""
    path: str
    size_bytes: int
    file_type: str  # 'image' or 'video'
    drive_id: int
    
    # Computed features (filled by pipeline stages)
    fast_fp: Optional[str] = None
    sha256: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    phash: Optional[str] = None
    is_large: bool = False
    
    @property
    def pixels(self) -> int:
        return (self.width or 0) * (self.height or 0)

class CheckpointManager:
    """Manages scan checkpoints for resumability"""
    
    def __init__(self, db_manager, checkpoint_dir: Path = None):
        self.db_manager = db_manager
        self.checkpoint_dir = checkpoint_dir or Path(".checkpoints")
        ensure_dir(self.checkpoint_dir)
        self._init_checkpoint_schema()
    
    def _init_checkpoint_schema(self):
        """Initialize checkpoint tables"""
        with self.db_manager.get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS scan_checkpoints (
                    scan_id TEXT PRIMARY KEY,
                    source_path TEXT NOT NULL,
                    drive_id INTEGER,
                    stage TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    processed_count INTEGER DEFAULT 0,
                    batch_number INTEGER DEFAULT 0,
                    config_json TEXT,
                    checkpoint_file TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_checkpoints_stage ON scan_checkpoints(stage);
                CREATE INDEX IF NOT EXISTS idx_checkpoints_timestamp ON scan_checkpoints(timestamp);
            """)
            conn.commit()
    
    def generate_scan_id(self, source_path: str) -> str:
        """Generate unique scan ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_hash = hashlib.md5(source_path.encode()).hexdigest()[:8]
        return f"scan_{timestamp}_{path_hash}"
    
    def save_checkpoint(self, checkpoint: ScanCheckpoint) -> Path:
        """Save checkpoint to disk and database"""
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
        """Load checkpoint from disk"""
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
        """List available checkpoints"""
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
        """Remove completed checkpoint"""
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
        """Clean up checkpoints older than specified days"""
        cutoff = datetime.now() - dt.timedelta(days=days)
        cutoff_str = cutoff.isoformat() + "Z"
        
        with self.db_manager.get_connection() as conn:
            old_checkpoints = conn.execute("""
                SELECT scan_id, checkpoint_file FROM scan_checkpoints 
                WHERE timestamp < ?
            """, (cutoff_str,)).fetchall()
            
            cleaned = 0
            for scan_id, checkpoint_file in old_checkpoints:
                try:
                    if Path(checkpoint_file).exists():
                        Path(checkpoint_file).unlink()
                    conn.execute("DELETE FROM scan_checkpoints WHERE scan_id = ?", (scan_id,))
                    cleaned += 1
                except Exception:
                    continue
            
            conn.commit()
            
        if cleaned > 0:
            print(f"  🧹 Cleaned up {cleaned} old checkpoints")

class DatabaseManager:
    """Thread-safe database operations with connection pooling"""
    
    def __init__(self, db_path: Path, max_connections: int = 4):
        self.db_path = db_path
        self.max_connections = max_connections
        self._connections = Queue(maxsize=max_connections)
        
        # Initialize connections
        for _ in range(max_connections):
            conn = self._create_connection()
            self._connections.put(conn)
        
        # Initialize schema
        self._init_schema()
    
    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON") 
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn
    
    @contextlib.contextmanager
    def get_connection(self):
        """Get a database connection from the pool"""
        conn = self._connections.get()
        try:
            yield conn
        finally:
            self._connections.put(conn)
    
    def _init_schema(self):
        """Initialize database schema"""
        schema = """
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;
        
        CREATE TABLE IF NOT EXISTS drives (
            drive_id INTEGER PRIMARY KEY,
            label TEXT,
            serial_or_uuid TEXT,
            mount_path TEXT UNIQUE
        );
        
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER PRIMARY KEY,
            original_file_id INTEGER,
            FOREIGN KEY(original_file_id) REFERENCES files(file_id) ON DELETE SET NULL
        );
        
        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY,
            hash_sha256 TEXT,
            phash TEXT,
            width INTEGER,
            height INTEGER,
            size_bytes INTEGER,
            type TEXT,
            drive_id INTEGER,
            path_on_drive TEXT,
            is_large INTEGER DEFAULT 0,
            copied INTEGER DEFAULT 0,
            duplicate_of INTEGER,
            group_id INTEGER,
            review_status TEXT DEFAULT 'undecided',
            reviewed_at TEXT,
            review_note TEXT,
            central_path TEXT,
            fast_fp TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(drive_id) REFERENCES drives(drive_id) ON DELETE SET NULL,
            FOREIGN KEY(duplicate_of) REFERENCES files(file_id) ON DELETE SET NULL,
            FOREIGN KEY(group_id) REFERENCES groups(group_id) ON DELETE SET NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_files_sha ON files(hash_sha256);
        CREATE INDEX IF NOT EXISTS idx_files_phash ON files(phash);
        CREATE INDEX IF NOT EXISTS idx_files_size_fp ON files(size_bytes, fast_fp);
        CREATE INDEX IF NOT EXISTS idx_files_group ON files(group_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_files_unique_path ON files(drive_id, path_on_drive);
        
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY,
            ts TEXT,
            action TEXT,
            details TEXT
        );
        """
        
        with self.get_connection() as conn:
            conn.executescript(schema)
            conn.commit()
    
    def batch_insert_files(self, records: List[FileRecord], batch_size: int = 1000):
        """Efficiently insert multiple file records"""
        print(f"  - Batch inserting {len(records):,} records...", end="", flush=True)
        
        with self.get_connection() as conn:
            rows = []
            for rec in records:
                rows.append((
                    rec.sha256, rec.phash, rec.width, rec.height, rec.size_bytes,
                    rec.file_type, rec.drive_id, rec.path, int(rec.is_large),
                    0, None, None, None, rec.fast_fp
                ))
            
            # Process in batches
            inserted = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                conn.executemany("""
                    INSERT OR IGNORE INTO files
                    (hash_sha256, phash, width, height, size_bytes, type, drive_id, 
                     path_on_drive, is_large, copied, duplicate_of, group_id, central_path, fast_fp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                inserted += len(batch)
                if i + batch_size < len(rows):
                    print(f"\r  - Batch inserting {inserted:,}/{len(rows):,} records...", end="", flush=True)
                    
            conn.commit()
            print(f"\r  - Inserted {inserted:,} records ✓")

class FeatureExtractor:
    """Optimized feature extraction with caching"""
    
    def __init__(self, max_phash_pixels: int = 36_000_000, hash_large: bool = False):
        self.max_phash_pixels = max_phash_pixels
        self.hash_large = hash_large
        
    def extract_features(self, file_path: Path, size_bytes: int, unique_size: bool, 
                        existing_buckets: Set[Tuple[int, str]]) -> Optional[FileRecord]:
        """Extract features for a single file"""
        try:
            ext = file_path.suffix.lower()
            file_type = 'image' if ext in IMAGE_EXT else 'video'
            
            record = FileRecord(
                path=str(file_path),
                size_bytes=size_bytes,
                file_type=file_type,
                drive_id=0,  # Set by caller
                is_large=size_bytes > LARGE_FILE_BYTES
            )
            
            # Skip expensive operations for large files unless explicitly enabled
            if record.is_large and not self.hash_large:
                return record
            
            # Fast fingerprint for duplicate pre-filtering
            record.fast_fp = self._compute_fast_fingerprint(file_path, size_bytes)
            
            # Only compute SHA if there might be duplicates
            need_sha = not unique_size and record.fast_fp and (size_bytes, record.fast_fp) in existing_buckets
            
            if need_sha:
                record.sha256 = self._compute_sha256(file_path)
            
            # Image processing - always try to get dimensions and phash for images
            if file_type == 'image':
                try:
                    with Image.open(file_path) as img:
                        record.width, record.height = img.size
                        
                        # Always compute phash for images to enable grouping
                        # (Skip only if image is too large or if we found exact SHA duplicate)
                        if (record.pixels <= self.max_phash_pixels and not record.sha256):
                            record.phash = str(imagehash.phash(img))
                except Exception as e:
                    # Debug why image processing is failing
                    print(f"Image processing failed for {file_path}: {e}", file=sys.stderr)
                    return None
            
            return record
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}", file=sys.stderr)
            return None
    
    def _compute_fast_fingerprint(self, path: Path, size_bytes: int) -> Optional[str]:
        """Fast partial hash of first/last blocks"""
        try:
            h = hashlib.sha256()
            with path.open('rb') as f:
                start_data = f.read(65536)
                h.update(start_data)
                if size_bytes > 131072:
                    f.seek(-65536, os.SEEK_END) 
                    end_data = f.read(65536)
                    h.update(end_data)
            return h.hexdigest()[:16]
        except Exception:
            return None
    
    def _compute_sha256(self, path: Path) -> str:
        """Compute full SHA-256"""
        h = hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

class DuplicateDetector:
    """Optimized duplicate detection with in-memory indexing"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self._sha_to_group: Dict[str, int] = {}
        self._phash_groups: Dict[str, Set[int]] = defaultdict(set)
        self._size_fp_buckets: Set[Tuple[int, str]] = set()
        self._refresh_indices()
    
    def _refresh_indices(self):
        """Load existing data into memory for fast lookups"""
        print(f"[{utc_now_str()}] Loading existing file indices...")
        
        with self.db_manager.get_connection() as conn:
            # SHA index for exact duplicates
            print("  - Loading SHA hash index...", end="", flush=True)
            sha_rows = conn.execute("""
                SELECT f.hash_sha256, f.group_id 
                FROM files f 
                WHERE f.hash_sha256 IS NOT NULL AND f.group_id IS NOT NULL
            """).fetchall()
            
            for sha, group_id in sha_rows:
                self._sha_to_group[sha] = group_id
            print(f" {len(sha_rows):,} entries")
            
            # Phash index for similar images
            print("  - Loading perceptual hash index...", end="", flush=True)
            phash_rows = conn.execute("""
                SELECT f.phash, f.group_id 
                FROM files f 
                WHERE f.phash IS NOT NULL AND f.group_id IS NOT NULL
            """).fetchall()
            
            for phash, group_id in phash_rows:
                if phash:
                    self._phash_groups[phash].add(group_id)
            print(f" {len(phash_rows):,} entries")
            
            # Size+fingerprint buckets
            print("  - Loading size+fingerprint buckets...", end="", flush=True)
            bucket_rows = conn.execute("""
                SELECT size_bytes, fast_fp 
                FROM files 
                WHERE fast_fp IS NOT NULL
            """).fetchall()
            
            for size, fp in bucket_rows:
                self._size_fp_buckets.add((size, fp))
            print(f" {len(bucket_rows):,} entries")
            
        print(f"[{utc_now_str()}] Index loading complete")
    
    def find_duplicate_group(self, record: FileRecord, phash_threshold: int = 5) -> Optional[int]:
        """Find existing group for this record, if any"""
        
        # Exact SHA match
        if record.sha256 and record.sha256 in self._sha_to_group:
            return self._sha_to_group[record.sha256]
        
        # Perceptual hash similarity (images only)
        if record.phash and record.file_type == 'image':
            return self._find_similar_phash_group(record.phash, phash_threshold)
        
        return None
    
    def _find_similar_phash_group(self, target_phash: str, threshold: int) -> Optional[int]:
        """Find group with similar perceptual hash using optimized search"""
        try:
            target_hash = imagehash.hex_to_hash(target_phash)
            best_group = None
            best_distance = threshold + 1
            
            for existing_phash, group_ids in self._phash_groups.items():
                try:
                    existing_hash = imagehash.hex_to_hash(existing_phash)
                    distance = target_hash - existing_hash
                    
                    if distance <= threshold and distance < best_distance:
                        best_distance = distance
                        best_group = next(iter(group_ids))  # Get any group from the set
                        
                        if distance == 0:  # Perfect match, early exit
                            break
                            
                except Exception:
                    continue
            
            return best_group
        except Exception:
            return None

    def get_existing_buckets(self) -> Set[Tuple[int, str]]:
        """Get existing (size, fast_fp) buckets for optimization"""
        return self._size_fp_buckets.copy()

class DriveManager:
    """Handle drive detection and management"""
    
    @staticmethod
    def detect_drive_info(source: Path, wsl_mode: bool) -> Tuple[Optional[str], Optional[str], str]:
        """Detect drive label, serial/uuid, and mount path"""
        if wsl_mode:
            return DriveManager._detect_wsl_drive(source)
        else:
            return DriveManager._detect_windows_drive(source)
    
    @staticmethod
    def _detect_windows_drive(source: Path) -> Tuple[Optional[str], Optional[str], str]:
        """Windows drive detection via wmic"""
        drive = source.drive or (str(source)[:3] if len(str(source)) >= 3 and str(source)[1:3] == ":\\" else None)
        mount_path = drive if drive else str(source.anchor)
        label, serial = None, None
        
        try:
            cmd = ["wmic", "logicaldisk", "get", "DeviceID,VolumeName,VolumeSerialNumber"]
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
            for line in out.splitlines():
                if not line.strip() or "DeviceID" in line:
                    continue
                parts = line.split()
                if len(parts) >= 1 and parts[0].upper() == (drive[:-1]).upper():
                    serial = parts[-1]
                    if len(parts) > 2:
                        label = " ".join(parts[1:-1]) or None
                    break
        except Exception:
            pass
        
        return label, serial, mount_path
    
    @staticmethod
    def _detect_wsl_drive(source: Path) -> Tuple[Optional[str], Optional[str], str]:
        """WSL drive detection via lsblk"""
        mount_path = str(source)
        label, uuid = None, None
        
        try:
            out = subprocess.check_output(["lsblk", "-o", "NAME,LABEL,UUID,MOUNTPOINT", "-P"], text=True)
            for line in out.splitlines():
                fields = {}
                for kv in line.split():
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        fields[k] = v.strip('"')
                if fields.get("MOUNTPOINT") == mount_path:
                    label = fields.get("LABEL")
                    uuid = fields.get("UUID") 
                    break
        except Exception:
            pass
        
        return label, uuid, mount_path

class OptimizedScanner:
    """Main scanner with pipeline architecture and checkpoint support"""
    
    def __init__(self, db_path: Path, central_path: Path):
        self.db_manager = DatabaseManager(db_path)
        self.central_path = central_path
        self.duplicate_detector = DuplicateDetector(self.db_manager)
        self.checkpoint_manager = CheckpointManager(self.db_manager)
        ensure_dir(central_path)
    
    def scan_source(self, source: Path, wsl_mode: bool = False, 
                   drive_label: Optional[str] = None, drive_id_hint: Optional[str] = None,
                   hash_large: bool = False, workers: int = 6, io_workers: int = 2,
                   phash_threshold: int = 5, skip_discovery: bool = False,
                   max_phash_pixels: int = 24_000_000, chunk_size: int = 100,
                   resume_scan_id: Optional[str] = None, auto_checkpoint: bool = True):
        """Optimized scanning with checkpoint support"""
        
        global PHASH_THRESHOLD
        PHASH_THRESHOLD = phash_threshold
        
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
        
        # Clean up old checkpoints
        if auto_checkpoint:
            self.checkpoint_manager.cleanup_old_checkpoints()
        
        # Handle resume logic
        checkpoint = None
        if resume_scan_id:
            checkpoint = self.checkpoint_manager.load_checkpoint(resume_scan_id)
            if not checkpoint:
                print(f"❌ Could not load checkpoint {resume_scan_id}")
                return
            print(f"📋 Resuming scan from checkpoint: {checkpoint.stage} stage")
            print(f"    - Processed: {checkpoint.processed_count:,} items")
            print(f"    - Timestamp: {checkpoint.timestamp}")
            
            # Validate checkpoint matches current parameters
            if checkpoint.source_path != str(source):
                print(f"❌ Source path mismatch: checkpoint={checkpoint.source_path}, current={source}")
                return
        
        # Get or create drive (use cached if resuming)
        if checkpoint and checkpoint.drive_id:
            drive_id = checkpoint.drive_id
            print(f"[{utc_now_str()}] Using cached drive ID: {drive_id}")
        else:
            print(f"[{utc_now_str()}] Identifying drive...")
            drive_id = self._get_or_create_drive(source, wsl_mode, drive_label, drive_id_hint)
        
        # Generate scan ID if starting new scan
        if not checkpoint:
            scan_id = self.checkpoint_manager.generate_scan_id(str(source))
        else:
            scan_id = checkpoint.scan_id
        
        try:
            # Stage 1: Discovery
            if checkpoint and checkpoint.stage in ['extraction', 'grouping', 'completed']:
                print(f"[{utc_now_str()}] Loading cached discovered files...")
                candidates = [(Path(p), s) for p, s in checkpoint.discovered_files]
                print(f"  - Loaded {len(candidates):,} files from checkpoint")
            else:
                candidates = self._discover_files_with_checkpoint(
                    source, skip_discovery, scan_id, drive_id, scan_config, auto_checkpoint
                )
                if not candidates:
                    print("No media files found.")
                    return
            
            # Stage 2: Feature extraction
            if checkpoint and checkpoint.stage in ['grouping', 'completed']:
                print(f"[{utc_now_str()}] Skipping feature extraction (already completed)")
                # Records are already in database, we'll query them for grouping
                records = self._load_records_from_db(drive_id, str(source))
            else:
                start_batch = checkpoint.batch_number if checkpoint and checkpoint.stage == 'extraction' else 0
                records = self._extract_features_with_checkpoint(
                    candidates, drive_id, hash_large, io_workers, max_phash_pixels, 
                    chunk_size, scan_id, scan_config, auto_checkpoint, start_batch
                )
            
            # Stage 3: Duplicate detection and grouping
            if checkpoint and checkpoint.stage == 'completed':
                print(f"[{utc_now_str()}] Skipping grouping (scan already completed)")
            else:
                self._process_duplicates_and_groups_with_checkpoint(
                    records, phash_threshold, scan_id, scan_config, auto_checkpoint
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
            
        except KeyboardInterrupt:
            if auto_checkpoint:
                print(f"\n⚠️  Scan interrupted! Resume with: --resume-scan-id {scan_id}")
            raise
        except Exception as e:
            if auto_checkpoint:
                print(f"\n❌ Scan failed! Resume with: --resume-scan-id {scan_id}")
            raise
        
        print("=" * 80)
        print(f"SCAN COMPLETED - {utc_now_str()}")
        print("=" * 80)
    
    def _discover_files_with_checkpoint(self, source: Path, skip_discovery: bool,
                                      scan_id: str, drive_id: int, config: Dict,
                                      auto_checkpoint: bool) -> List[Tuple[Path, int]]:
        """Discovery with checkpoint support"""
        candidates_file = "last_candidates.json"
        
        if skip_discovery and Path(candidates_file).exists():
            print(f"[{utc_now_str()}] Loading cached candidates from {candidates_file}...")
            with open(candidates_file, 'r') as f:
                paths = json.load(f)
            
            print("  - Validating cached paths...", end="", flush=True)
            valid_candidates = []
            for p in paths:
                try:
                    path_obj = Path(p)
                    if path_obj.exists():
                        valid_candidates.append((path_obj, path_obj.stat().st_size))
                except Exception:
                    continue
            print(f" {len(valid_candidates):,} valid files")
            
            # Save discovery checkpoint
            if auto_checkpoint:
                checkpoint = ScanCheckpoint(
                    scan_id=scan_id,
                    source_path=str(source),
                    drive_id=drive_id,
                    stage='discovery',
                    timestamp=utc_now_str(),
                    discovered_files=[(str(p), s) for p, s in valid_candidates],
                    processed_count=len(valid_candidates),
                    config=config
                )
                self.checkpoint_manager.save_checkpoint(checkpoint)
            
            return valid_candidates
        
        print(f"[{utc_now_str()}] Discovering media files in {source}...")
        candidates = []
        total_scanned = 0
        permission_errors = 0
        
        def _scan_recursive(path: Path):
            nonlocal total_scanned, permission_errors
            try:
                with os.scandir(path) as entries:
                    for entry in entries:
                        total_scanned += 1
                        if total_scanned % 10000 == 0:
                            print(f"  - Scanned {total_scanned:,} items, found {len(candidates):,} media files...", flush=True)
                            
                            # Periodic checkpoint during discovery
                            if auto_checkpoint and total_scanned % 50000 == 0:
                                checkpoint = ScanCheckpoint(
                                    scan_id=scan_id,
                                    source_path=str(source),
                                    drive_id=drive_id,
                                    stage='discovery',
                                    timestamp=utc_now_str(),
                                    discovered_files=[(str(p), s) for p, s in candidates],
                                    processed_count=len(candidates),
                                    config=config
                                )
                                self.checkpoint_manager.save_checkpoint(checkpoint)
                            
                        if entry.is_file():
                            if Path(entry.name).suffix.lower() in SUPPORTED_EXT:
                                try:
                                    size = entry.stat().st_size
                                    candidates.append((Path(entry.path), size))
                                except OSError:
                                    permission_errors += 1
                                    continue
                        elif entry.is_dir():
                            _scan_recursive(Path(entry.path))
            except (PermissionError, OSError):
                permission_errors += 1
                pass
        
        start_time = time.perf_counter()
        _scan_recursive(source)
        elapsed = time.perf_counter() - start_time
        
        # Cache candidates for reuse
        try:
            with open(candidates_file, 'w') as f:
                json.dump([str(p) for p, _ in candidates], f)
            print(f"  - Cached candidates to {candidates_file}")
        except Exception:
            pass
        
        # Save discovery checkpoint
        if auto_checkpoint:
            checkpoint = ScanCheckpoint(
                scan_id=scan_id,
                source_path=str(source),
                drive_id=drive_id,
                stage='discovery',
                timestamp=utc_now_str(),
                discovered_files=[(str(p), s) for p, s in candidates],
                processed_count=len(candidates),
                config=config
            )
            self.checkpoint_manager.save_checkpoint(checkpoint)
        
        print(f"[{utc_now_str()}] Discovery complete: {len(candidates):,} media files")
        print(f"  - Total scanned: {total_scanned:,} items in {elapsed:.1f}s ({total_scanned/elapsed:.0f} items/s)")
        if permission_errors > 0:
            print(f"  - Permission errors: {permission_errors:,}")
        
        return candidates
    
    def _extract_features_with_checkpoint(self, candidates: List[Tuple[Path, int]], 
                                        drive_id: int, hash_large: bool, io_workers: int, 
                                        max_phash_pixels: int, chunk_size: int,
                                        scan_id: str, config: Dict, auto_checkpoint: bool,
                                        start_batch: int = 0) -> List[FileRecord]:
        """Feature extraction with checkpoint support"""
        
        print(f"[{utc_now_str()}] Analyzing file characteristics...")
        
        # Pre-analysis for optimization
        size_counts = Counter(size for _, size in candidates)
        existing_buckets = self.duplicate_detector.get_existing_buckets()
        existing_sizes = self._get_existing_sizes()
        
        # File type analysis
        type_counts = Counter()
        for path, _ in candidates:
            ext = Path(path).suffix.lower()
            if ext in IMAGE_EXT:
                type_counts['image'] += 1
            elif ext in VIDEO_EXT:
                type_counts['video'] += 1
            else:
                type_counts['unknown'] += 1
        
        print(f"  - File type breakdown: {dict(type_counts)}")
        
        # Show optimization stats (abbreviated for resuming)
        if start_batch == 0:
            unique_sizes = sum(1 for count in size_counts.values() if count == 1)
            repeated_sizes = len(size_counts) - unique_sizes
            total_repeated_files = sum(count for count in size_counts.values() if count > 1)
            files_needing_sha = 0
            files_needing_phash = type_counts.get('image', 0)
            
            for path, size in candidates:
                unique_in_scan = size_counts[size] == 1 and size not in existing_sizes
                if not unique_in_scan:
                    files_needing_sha += 1
                    
            print(f"  - Size analysis: {unique_sizes:,} unique sizes, {repeated_sizes:,} repeated sizes")
            print(f"  - Potential duplicates: {total_repeated_files:,} files with repeated sizes")
            print(f"  - Will compute SHA: {files_needing_sha:,} files")
            print(f"  - Will compute pHash: {files_needing_phash:,} images")
        
        feature_extractor = FeatureExtractor(
            max_phash_pixels=max_phash_pixels,
            hash_large=hash_large
        )
        
        records = []
        
        # Counters for progress reporting
        processed_count = 0
        sha_computed = 0
        phash_computed = 0
        large_skipped = 0
        errors = 0
        
        print(f"[{utc_now_str()}] Processing {len(candidates):,} files with {io_workers} I/O workers (chunk size: {chunk_size})...")
        if start_batch > 0:
            print(f"  - Resuming from batch {start_batch + 1}")
        
        # Process in chunks to reduce I/O contention on slow drives
        total_chunks = (len(candidates) + chunk_size - 1) // chunk_size
        
        for chunk_idx in range(start_batch, total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(candidates))
            chunk = candidates[start_idx:end_idx]
            
            print(f"  - Processing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk)} files)...")
            
            # Process chunk with limited I/O workers
            with ThreadPoolExecutor(max_workers=io_workers) as executor:
                futures = []
                
                for path, size in chunk:
                    unique_size = size_counts[size] == 1 and size not in existing_sizes
                    
                    future = executor.submit(
                        feature_extractor.extract_features,
                        path, size, unique_size, existing_buckets
                    )
                    futures.append((future, drive_id))
                
                # Collect chunk results
                chunk_start_time = time.perf_counter()
                chunk_records = []
                for i, (future, drive_id) in enumerate(futures, 1):
                    try:
                        record = future.result()
                        if record:
                            record.drive_id = drive_id
                            chunk_records.append(record)
                            
                            # Update counters
                            if record.sha256:
                                sha_computed += 1
                            if record.phash:
                                phash_computed += 1
                            if record.is_large:
                                large_skipped += 1
                        else:
                            errors += 1
                            
                        processed_count += 1
                        
                    except Exception as e:
                        errors += 1
                        if errors <= 10:  # Only show first few errors
                            print(f"Feature extraction error: {e}", file=sys.stderr)
                
                chunk_elapsed = time.perf_counter() - chunk_start_time
                chunk_rate = len(chunk) / chunk_elapsed if chunk_elapsed > 0 else 0
                
                print(f"    Chunk completed: {len(chunk)} files in {chunk_elapsed:.1f}s ({chunk_rate:.0f} files/s)")
                
                # Insert chunk to database immediately
                if chunk_records:
                    self.db_manager.batch_insert_files(chunk_records)
                    records.extend(chunk_records)
                
                # Save checkpoint after every few chunks
                if auto_checkpoint and (chunk_idx + 1) % 5 == 0:
                    checkpoint = ScanCheckpoint(
                        scan_id=scan_id,
                        source_path=config.get('source_path', ''),
                        drive_id=drive_id,
                        stage='extraction',
                        timestamp=utc_now_str(),
                        discovered_files=[(str(p), s) for p, s in candidates],
                        processed_count=processed_count,
                        batch_number=chunk_idx,
                        config=config
                    )
                    self.checkpoint_manager.save_checkpoint(checkpoint)
        
        # Final extraction checkpoint
        if auto_checkpoint:
            checkpoint = ScanCheckpoint(
                scan_id=scan_id,
                source_path=config.get('source_path', ''),
                drive_id=drive_id,
                stage='extraction',
                timestamp=utc_now_str(),
                discovered_files=[(str(p), s) for p, s in candidates],
                processed_count=processed_count,
                batch_number=total_chunks - 1,
                config=config
            )
            self.checkpoint_manager.save_checkpoint(checkpoint)
        
        # Final summary
        print(f"[{utc_now_str()}] Feature extraction complete:")
        print(f"  - Processed: {processed_count:,} files")
        print(f"  - SHA computed: {sha_computed:,} files")
        print(f"  - pHash computed: {phash_computed:,} images") 
        print(f"  - Large files: {large_skipped:,} files")
        if errors > 0:
            print(f"  - Errors: {errors:,} files failed processing")
        
        return records
    
    def _load_records_from_db(self, drive_id: int, source_path: str) -> List[FileRecord]:
        """Load FileRecord objects from database for resuming"""
        print(f"[{utc_now_str()}] Loading processed records from database...")
        
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
    
    def _process_duplicates_and_groups_with_checkpoint(self, records: List[FileRecord], 
                                                     phash_threshold: int, scan_id: str,
                                                     config: Dict, auto_checkpoint: bool):
        """Process duplicates and groups with checkpoint support"""
        
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
        
        # Rest of the grouping logic remains the same
        self._process_duplicates_and_groups(records, phash_threshold)
    
    # Existing methods remain unchanged...
    def _get_or_create_drive(self, source: Path, wsl_mode: bool, 
                           drive_label: Optional[str], drive_id_hint: Optional[str]) -> int:
        """Get or create drive record"""
        print(f"  - Detecting drive information...", end="", flush=True)
        label, serial_or_uuid, mount_path = DriveManager.detect_drive_info(source, wsl_mode)
        
        # Override with user-provided values
        original_label = label
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
    
    def _get_existing_sizes(self) -> Set[int]:
        """Get sizes of existing files"""
        with self.db_manager.get_connection() as conn:
            return {row[0] for row in conn.execute("SELECT DISTINCT size_bytes FROM files")}
    
    def _process_duplicates_and_groups(self, records: List[FileRecord], phash_threshold: int):
        """Process records for duplicates and create groups"""
        
        # Categorize records
        large_files = []
        similar_groups = []
        new_originals = []
        
        print(f"[{utc_now_str()}] Analyzing {len(records):,} records for duplicates...")
        
        # Progress tracking
        processed = 0
        start_time = time.perf_counter()
        
        for record in records:
            processed += 1
            
            if record.is_large and not record.sha256:
                large_files.append(record)
            else:
                existing_group = self.duplicate_detector.find_duplicate_group(record, phash_threshold)
                
                if existing_group:
                    similar_groups.append((record, existing_group))
                else:
                    new_originals.append(record)
            
            # Progress updates
            if processed % 2000 == 0:
                elapsed = time.perf_counter() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                print(f"  - Analyzed {processed:,}/{len(records):,} records ({rate:.0f}/s) | "
                      f"Large: {len(large_files):,}, Similar: {len(similar_groups):,}, New: {len(new_originals):,}", flush=True)
        
        elapsed = time.perf_counter() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        
        print(f"[{utc_now_str()}] Duplicate analysis complete ({rate:.0f} records/s):")
        print(f"  - Large files: {len(large_files):,}")
        print(f"  - Files joining existing groups: {len(similar_groups):,}")
        print(f"  - New originals: {len(new_originals):,}")
        
        if len(similar_groups) > 0:
            print(f"  - 🔍 Found {len(similar_groups):,} potential resized duplicates!")
        else:
            print(f"  - ✨ No duplicates found - very clean image collection!")
        
        # Process each category efficiently
        if large_files:
            self._batch_insert_large_files(large_files)
        
        if new_originals:
            self._create_new_groups(new_originals)
        
        if similar_groups:
            self._process_similar_files(similar_groups)
        
        # Generate reports
        if large_files:
            print(f"[{utc_now_str()}] Generating large files review...")
            self._generate_large_files_review(large_files)
        
        print(f"[{utc_now_str()}] Database updates complete")
    
    def _batch_insert_large_files(self, records: List[FileRecord]):
        """Batch insert large file placeholders"""
        print(f"[{utc_now_str()}] Inserting {len(records):,} large file placeholders...")
        self.db_manager.batch_insert_files(records)
    
    def _create_new_groups(self, records: List[FileRecord]):
        """Create new groups for original files"""
        if not records:
            return
            
        print(f"[{utc_now_str()}] Creating {len(records):,} new groups...")
        
        batch_size = 1000
        created_groups = 0
        start_time = time.perf_counter()
        
        with self.db_manager.get_connection() as conn:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                for record in batch:
                    # Check if file already exists (from earlier batch insertion)
                    existing_file = conn.execute("""
                        SELECT file_id FROM files 
                        WHERE drive_id = ? AND path_on_drive = ?
                    """, (record.drive_id, record.path)).fetchone()
                    
                    if existing_file:
                        # File already exists, just use its ID
                        file_id = existing_file[0]
                    else:
                        # Insert new file
                        cursor = conn.execute("""
                            INSERT INTO files 
                            (hash_sha256, phash, width, height, size_bytes, type, drive_id,
                             path_on_drive, is_large, copied, fast_fp)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            record.sha256, record.phash, record.width, record.height,
                            record.size_bytes, record.file_type, record.drive_id,
                            record.path, int(record.is_large), 0, record.fast_fp
                        ))
                        file_id = cursor.lastrowid
                    
                    # Create group
                    group_cursor = conn.execute("INSERT INTO groups (original_file_id) VALUES (?)", (file_id,))
                    group_id = group_cursor.lastrowid
                    
                    # Update file with group
                    conn.execute("UPDATE files SET group_id=? WHERE file_id=?", (group_id, file_id))
                    created_groups += 1
                
                conn.commit()
                
                # Progress update
                if created_groups % 1000 == 0 and created_groups < len(records):
                    elapsed = time.perf_counter() - start_time
                    rate = created_groups / elapsed if elapsed > 0 else 0
                    print(f"  - Created {created_groups:,}/{len(records):,} groups ({rate:.0f}/s)...", flush=True)
        
        elapsed = time.perf_counter() - start_time
        rate = created_groups / elapsed if elapsed > 0 else 0
        print(f"  - Created {created_groups:,} new groups in {elapsed:.1f}s ({rate:.0f} groups/s) ✓")
    
    def _process_similar_files(self, similar: List[Tuple[FileRecord, int]]):
        """Process files that go into existing groups"""
        if not similar:
            return
            
        print(f"[{utc_now_str()}] Processing {len(similar):,} files into existing groups...")
        
        promotions = 0
        duplicates_added = 0
        processed = 0
        start_time = time.perf_counter()
        
        with self.db_manager.get_connection() as conn:
            for record, group_id in similar:
                processed += 1
                
                # Check if file already exists (from earlier batch insertion)
                existing_file = conn.execute("""
                    SELECT file_id FROM files 
                    WHERE drive_id = ? AND path_on_drive = ?
                """, (record.drive_id, record.path)).fetchone()
                
                if existing_file:
                    # File already exists, update it with group info
                    new_file_id = existing_file[0]
                    conn.execute("""
                        UPDATE files SET group_id = ? WHERE file_id = ?
                    """, (group_id, new_file_id))
                else:
                    # Insert file into group
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
                    new_file_id = cursor.lastrowid
                
                # Check if this should become the new original
                current_orig = conn.execute("""
                    SELECT f.file_id, f.width, f.height, f.size_bytes
                    FROM groups g JOIN files f ON f.file_id = g.original_file_id
                    WHERE g.group_id = ?
                """, (group_id,)).fetchone()
                
                if self._should_promote(record, current_orig):
                    self._promote_to_original(conn, new_file_id, group_id, current_orig[0] if current_orig else None)
                    promotions += 1
                else:
                    # Set as duplicate of current original
                    if current_orig:
                        conn.execute("UPDATE files SET duplicate_of=? WHERE file_id=?", 
                                   (current_orig[0], new_file_id))
                    duplicates_added += 1
                
                # Progress updates every 500 files
                if processed % 500 == 0:
                    elapsed = time.perf_counter() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    print(f"  - Added {processed:,}/{len(similar):,} files ({rate:.0f}/s) | "
                          f"Promotions: {promotions:,}, Duplicates: {duplicates_added:,}", flush=True)
            
            conn.commit()
        
        elapsed = time.perf_counter() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        print(f"  - Completed: {processed:,} files in {elapsed:.1f}s ({rate:.0f} files/s)")
        print(f"  - Promotions: {promotions:,} files became new group originals")
        print(f"  - Duplicates: {duplicates_added:,} files added as duplicates")
    
    def _should_promote(self, candidate: FileRecord, current_orig: Optional[Tuple]) -> bool:
        """Check if candidate should become the group original - largest wins"""
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
        """Promote file to be group original"""
        conn.execute("UPDATE groups SET original_file_id=? WHERE group_id=?", (new_file_id, group_id))
        conn.execute("UPDATE files SET duplicate_of=NULL WHERE file_id=?", (new_file_id,))
        
        if old_original_id:
            conn.execute("UPDATE files SET duplicate_of=? WHERE file_id=?", (new_file_id, old_original_id))
        
        conn.execute("UPDATE files SET duplicate_of=? WHERE group_id=? AND file_id != ?",
                    (new_file_id, group_id, new_file_id))
    
    def _generate_large_files_review(self, large_files: List[FileRecord]):
        """Generate CSV of large files for manual review"""
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
    
    def _print_final_stats(self):
        """Print final scan statistics"""
        print(f"[{utc_now_str()}] Generating scan summary...")
        
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

# ---------------------------
# Checkpoint management commands
# ---------------------------

def cmd_list_checkpoints(db_manager: DatabaseManager, source_path: Optional[str] = None):
    """List available checkpoints"""
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
    """Clean up old checkpoints"""
    checkpoint_manager = CheckpointManager(db_manager)
    
    if scan_id:
        checkpoint_manager.cleanup_checkpoint(scan_id)
        print(f"Cleaned up checkpoint: {scan_id}")
    else:
        checkpoint_manager.cleanup_old_checkpoints(days)
        print(f"Cleaned up checkpoints older than {days} days")

def cmd_checkpoint_info(db_manager: DatabaseManager, scan_id: str):
    """Show detailed checkpoint information"""
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

# ---------------------------
# Review and correction commands (unchanged interface)
# ---------------------------

def cmd_make_original(db_manager: DatabaseManager, central: Path, file_id: int):
    """Make a file its own original (split from group)"""
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT group_id FROM files WHERE file_id=?", (file_id,)).fetchone()
        if not row:
            print("File not found")
            return
            
        # Create new group
        cursor = conn.execute("INSERT INTO groups (original_file_id) VALUES (?)", (file_id,))
        new_group_id = cursor.lastrowid
        
        # Update file
        conn.execute("UPDATE files SET group_id=?, duplicate_of=NULL WHERE file_id=?", 
                    (new_group_id, file_id))
        conn.commit()
        
        print(f"File {file_id} is now original of new group {new_group_id}")

def cmd_promote(db_manager: DatabaseManager, central: Path, file_id: int):
    """Promote file to be group's original"""
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT group_id FROM files WHERE file_id=?", (file_id,)).fetchone()
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
        conn.execute("UPDATE groups SET original_file_id=? WHERE group_id=?", (file_id, group_id))
        conn.execute("UPDATE files SET duplicate_of=NULL WHERE file_id=?", (file_id,))
        
        if old_original_id:
            conn.execute("UPDATE files SET duplicate_of=? WHERE file_id=?", (file_id, old_original_id))
        
        conn.execute("UPDATE files SET duplicate_of=? WHERE group_id=? AND file_id != ?",
                    (file_id, group_id, file_id))
        conn.commit()
        
        print(f"Promoted file {file_id} to original of group {group_id}")

def cmd_move_to_group(db_manager: DatabaseManager, central: Path, file_id: int, target_group_id: int):
    """Move file to existing group"""
    with db_manager.get_connection() as conn:
        # Get target group's original
        orig_row = conn.execute("SELECT original_file_id FROM groups WHERE group_id=?", 
                               (target_group_id,)).fetchone()
        if not orig_row:
            print("Target group not found")
            return
            
        target_original = orig_row[0]
        conn.execute("UPDATE files SET group_id=?, duplicate_of=? WHERE file_id=?",
                    (target_group_id, target_original, file_id))
        conn.commit()
        
        print(f"Moved file {file_id} to group {target_group_id}")

def cmd_mark(db_manager: DatabaseManager, file_id: int, status: str, note: Optional[str]):
    """Mark file review status"""
    if status not in REVIEW_STATUSES:
        print(f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
        return
        
    with db_manager.get_connection() as conn:
        conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE file_id=?",
                    (status, now_iso(), note, file_id))
        conn.commit()
        
    print(f"Marked file {file_id} as {status}")

def cmd_mark_group(db_manager: DatabaseManager, group_id: int, status: str, note: Optional[str]):
    """Mark entire group review status"""
    if status not in REVIEW_STATUSES:
        print(f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
        return
        
    with db_manager.get_connection() as conn:
        conn.execute("UPDATE files SET review_status=?, reviewed_at=?, review_note=? WHERE group_id=?",
                    (status, now_iso(), note, group_id))
        conn.commit()
        
    print(f"Marked group {group_id} as {status}")

def cmd_bulk_mark(db_manager: DatabaseManager, path_like: str, status: str):
    """Bulk mark files by path pattern"""
    if status not in REVIEW_STATUSES:
        print(f"Invalid status. Use: {', '.join(REVIEW_STATUSES)}")
        return
        
    with db_manager.get_connection() as conn:
        like_pattern = f"%{path_like}%"
        cursor = conn.execute("UPDATE files SET review_status=?, reviewed_at=? WHERE path_on_drive LIKE ?",
                            (status, now_iso(), like_pattern))
        conn.commit()
        
    print(f"Bulk marked {cursor.rowcount} files matching '{path_like}' as {status}")

def cmd_review_queue(db_manager: DatabaseManager, limit: int):
    """Show review queue"""
    with db_manager.get_connection() as conn:
        rows = conn.execute("""
            SELECT file_id, COALESCE(group_id, -1), type, width, height, size_bytes, 
                   review_status, path_on_drive
            FROM files
            WHERE review_status='undecided'
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
    
    if not rows:
        print("No undecided items in review queue")
        return
        
    print("file_id | group_id | type  | dimensions | size_bytes | status     | path")
    print("-" * 80)
    for r in rows:
        file_id, gid, typ, w, h, size, status, path = r
        dims = f"{w}x{h}" if (w and h) else "-"
        print(f"{file_id:7d} | {gid:8d} | {typ:5s} | {dims:>10s} | {size or 0:10d} | {status:10s} | {path}")

def cmd_export_backup_list(db_manager: DatabaseManager, out_path: Path, 
                          include_undecided: bool, include_large: bool):
    """Export backup manifest CSV"""
    print(f"[{utc_now_str()}] Exporting backup manifest to {out_path}...")
    
    status_filter = "IN ('keep','undecided')" if include_undecided else "= 'keep'"
    
    with db_manager.get_connection() as conn:
        print("  - Querying originals...", end="", flush=True)
        rows = conn.execute(f"""
            SELECT g.group_id, f.file_id, f.central_path, f.width, f.height, 
                   f.size_bytes, f.hash_sha256, f.review_status
            FROM groups g
            JOIN files f ON f.file_id = g.original_file_id
            WHERE f.review_status {status_filter}
            AND ((? = 1) OR (f.is_large = 0))
            ORDER BY g.group_id
        """, (1 if include_large else 0,)).fetchall()
        print(f" {len(rows):,} records")
    
    print("  - Writing CSV...", end="", flush=True)
    ensure_dir(out_path.parent)
    with out_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["group_id", "original_file_id", "central_original_path",
                        "width", "height", "size_bytes", "hash_sha256", "review_status"])
        writer.writerows(rows)
    print(" ✓")
    
    # Calculate totals
    total_size = sum(row[5] or 0 for row in rows)
    total_gb = total_size / (1024**3)
    
    print(f"[{utc_now_str()}] Export complete:")
    print(f"  - Files: {len(rows):,} originals")
    print(f"  - Size: {total_gb:.1f} GB")
    print(f"  - Location: {out_path}")

def cmd_show_stats(db_manager: DatabaseManager, detailed: bool = False):
    """Show database statistics"""
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

# ---------------------------
# CLI Integration with Checkpoint Support
# ---------------------------

def main():
    """Main CLI entry point with checkpoint support"""
    global PHASH_THRESHOLD, LARGE_FILE_BYTES
    
    parser = argparse.ArgumentParser(
        description="Media Consolidation & Review Tool - Enhanced with Checkpoint Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan a drive with checkpoint support
  %(prog)s scan --source /mnt/photos --central ./data --workers 4
  
  # Resume interrupted scan
  %(prog)s scan --source /mnt/photos --central ./data --resume-scan-id scan_20241210_143012_a1b2c3d4
  
  # Checkpoint management
  %(prog)s list-checkpoints
  %(prog)s checkpoint-info --scan-id scan_20241210_143012_a1b2c3d4
  %(prog)s cleanup-checkpoints --days 7
  
  # Review workflow
  %(prog)s review-queue --limit 50
  %(prog)s mark --file-id 123 --status keep
  %(prog)s export-backup-list --out backup.csv
        """
    )
    
    # Global options
    parser.add_argument("--db", default="media_index.db", 
                       help="SQLite database path (default: media_index.db)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose output")
    
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")
    
    # ================================
    # SCAN command (enhanced)
    # ================================
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
    scan_parser.add_argument("--phash-threshold", type=int, default=5,
                           help="Perceptual hash Hamming distance threshold (default: 5)")
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
    
    # ================================
    # CHECKPOINT commands
    # ================================
    list_chk_parser = subparsers.add_parser("list-checkpoints", help="List available checkpoints")
    list_chk_parser.add_argument("--source", help="Filter by source path")
    
    info_chk_parser = subparsers.add_parser("checkpoint-info", help="Show checkpoint details")
    info_chk_parser.add_argument("--scan-id", required=True, help="Checkpoint scan ID")
    
    cleanup_chk_parser = subparsers.add_parser("cleanup-checkpoints", help="Clean up old checkpoints")
    cleanup_chk_parser.add_argument("--days", type=int, default=7,
                                   help="Remove checkpoints older than N days (default: 7)")
    cleanup_chk_parser.add_argument("--scan-id", help="Remove specific checkpoint by scan ID")
    
    # ================================
    # CORRECTION commands
    # ================================
    make_orig_parser = subparsers.add_parser("make-original", 
                                           help="Split file into its own group as original")
    make_orig_parser.add_argument("--file-id", type=int, required=True,
                                help="File ID to make original")
    
    promote_parser = subparsers.add_parser("promote",
                                         help="Promote file to be group's original")
    promote_parser.add_argument("--file-id", type=int, required=True,
                              help="File ID to promote")
    
    move_parser = subparsers.add_parser("move-to-group",
                                      help="Move file to existing group")
    move_parser.add_argument("--file-id", type=int, required=True,
                           help="File ID to move")
    move_parser.add_argument("--group-id", type=int, required=True,
                           help="Target group ID")
    
    # ================================
    # REVIEW commands  
    # ================================
    mark_parser = subparsers.add_parser("mark", help="Mark file review status")
    mark_parser.add_argument("--file-id", type=int, required=True,
                           help="File ID to mark")
    mark_parser.add_argument("--status", choices=list(REVIEW_STATUSES), required=True,
                           help="Review status")
    mark_parser.add_argument("--note", help="Optional review note")
    
    mark_group_parser = subparsers.add_parser("mark-group", help="Mark entire group status")
    mark_group_parser.add_argument("--group-id", type=int, required=True,
                                 help="Group ID to mark")
    mark_group_parser.add_argument("--status", choices=list(REVIEW_STATUSES), required=True,
                                 help="Review status")
    mark_group_parser.add_argument("--note", help="Optional review note")
    
    bulk_mark_parser = subparsers.add_parser("bulk-mark", help="Bulk mark by path pattern")
    bulk_mark_parser.add_argument("--path-like", required=True,
                                help="Path substring to match")
    bulk_mark_parser.add_argument("--status", choices=list(REVIEW_STATUSES), required=True,
                                help="Review status")
    
    queue_parser = subparsers.add_parser("review-queue", help="Show review queue")
    queue_parser.add_argument("--limit", type=int, default=100,
                            help="Maximum items to show (default: 100)")
    
    export_parser = subparsers.add_parser("export-backup-list", help="Export backup manifest")
    export_parser.add_argument("--out", required=True,
                              help="Output CSV file path")
    export_parser.add_argument("--include-undecided", action="store_true",
                              help="Include undecided items in export")
    export_parser.add_argument("--include-large", action="store_true", 
                              help="Include large files in export")
    
    # ================================
    # STATS command
    # ================================
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.add_argument("--detailed", action="store_true",
                            help="Show detailed breakdown")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Initialize database manager
    db_path = Path(args.db)
    db_manager = DatabaseManager(db_path)
    
    try:
        # Apply global configuration from CLI args
        if args.command == "scan":
            if hasattr(args, 'phash_threshold'):
                PHASH_THRESHOLD = args.phash_threshold
            if hasattr(args, 'large_threshold_mb'):
                LARGE_FILE_BYTES = args.large_threshold_mb * 1024 * 1024
        
        # Execute commands
        if args.command == "scan":
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
                skip_discovery=args.skip_discovery,
                max_phash_pixels=args.max_phash_pixels,
                chunk_size=args.chunk_size,
                resume_scan_id=args.resume_scan_id,
                auto_checkpoint=not args.no_checkpoints
            )
        
        # Checkpoint commands
        elif args.command == "list-checkpoints":
            cmd_list_checkpoints(db_manager, args.source)
        
        elif args.command == "checkpoint-info":
            cmd_checkpoint_info(db_manager, args.scan_id)
        
        elif args.command == "cleanup-checkpoints":
            cmd_cleanup_checkpoints(db_manager, args.days, args.scan_id)
        
        # File management commands
        elif args.command == "make-original":
            # Infer central path from existing records
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT central_path FROM files WHERE central_path IS NOT NULL LIMIT 1").fetchone()
                central = Path(row[0]).parents[1] if row else Path.cwd()
            cmd_make_original(db_manager, central, args.file_id)
        
        elif args.command == "promote":
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT central_path FROM files WHERE central_path IS NOT NULL LIMIT 1").fetchone()
                central = Path(row[0]).parents[1] if row else Path.cwd()
            cmd_promote(db_manager, central, args.file_id)
        
        elif args.command == "move-to-group":
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT central_path FROM files WHERE central_path IS NOT NULL LIMIT 1").fetchone()
                central = Path(row[0]).parents[1] if row else Path.cwd()
            cmd_move_to_group(db_manager, central, args.file_id, args.group_id)
        
        # Review commands
        elif args.command == "mark":
            cmd_mark(db_manager, args.file_id, args.status, args.note)
        
        elif args.command == "mark-group":
            cmd_mark_group(db_manager, args.group_id, args.status, args.note)
        
        elif args.command == "bulk-mark":
            cmd_bulk_mark(db_manager, args.path_like, args.status)
        
        elif args.command == "review-queue":
            cmd_review_queue(db_manager, args.limit)
        
        elif args.command == "export-backup-list":
            cmd_export_backup_list(db_manager, Path(args.out), 
                                 args.include_undecided, args.include_large)
        
        elif args.command == "stats":
            cmd_show_stats(db_manager, args.detailed)
    
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        if args.command == "scan" and hasattr(args, 'resume_scan_id') and not args.no_checkpoints:
            print("💡 You can resume this scan later using the checkpoint system.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()