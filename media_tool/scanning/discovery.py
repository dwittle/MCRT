#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File discovery logic for the Media Consolidation Tool.
Handles recursive scanning of directories to find media files.
"""

import json
import os
import time
from pathlib import Path
from typing import List, Tuple, Optional

from ..config import SUPPORTED_EXT, DEFAULT_SMALL_FILE_BYTES 
from ..models.checkpoint import ScanCheckpoint
from ..checkpoint.manager import CheckpointManager
from ..utils.time import utc_now_str


class FileDiscovery:
    """Handles file discovery with caching and checkpoint support."""
    
    def __init__(self, checkpoint_manager: Optional[CheckpointManager] = None):
        self.checkpoint_manager = checkpoint_manager
    
    def discover_files(self, source: Path, skip_discovery: bool = False, 
                      scan_id: Optional[str] = None, drive_id: Optional[int] = None,
                      config: Optional[dict] = None, auto_checkpoint: bool = True) -> List[Tuple[Path, int]]:
        """
        Discover media files in source directory with checkpoint support.
        
        Args:
            source: Source directory to scan
            skip_discovery: Skip discovery and use cached candidates if available
            scan_id: Unique scan identifier for checkpointing
            drive_id: Drive identifier for checkpointing
            config: Scan configuration for checkpointing
            auto_checkpoint: Whether to save periodic checkpoints
            
        Returns:
            List of (file_path, file_size) tuples for discovered media files
        """
        candidates_file = "last_candidates.json"
        
        # Try to load cached candidates if skip_discovery is enabled
        if skip_discovery and Path(candidates_file).exists():
            return self._load_cached_candidates(candidates_file)
        
        # Perform fresh discovery
        print(f"[{utc_now_str()}] Discovering media files in {source}...")
        
        candidates = []
        stats = {
            'total_scanned': 0,
            'permission_errors': 0,
            'media_files_found': 0
        }
        
        start_time = time.perf_counter()
        
        # Recursive scan with progress tracking
        self._scan_recursive(
            source, candidates, stats, scan_id, drive_id, 
            config, auto_checkpoint
        )
        
        elapsed = time.perf_counter() - start_time
        
        # Cache candidates for potential reuse
        self._cache_candidates(candidates, candidates_file)
        
        # Save final discovery checkpoint
        if auto_checkpoint and self.checkpoint_manager and scan_id:
            self._save_discovery_checkpoint(
                scan_id, source, drive_id, candidates, config
            )
        
        # Print discovery summary
        self._print_discovery_summary(candidates, stats, elapsed)
        
        return candidates
    
    def _load_cached_candidates(self, candidates_file: str) -> List[Tuple[Path, int]]:
        """Load and validate cached candidate files."""
        print(f"[{utc_now_str()}] Loading cached candidates from {candidates_file}...")
        
        try:
            with open(candidates_file, 'r') as f:
                cached_paths = json.load(f)
            
            print("  - Validating cached paths...", end="", flush=True)
            valid_candidates = []
            invalid_count = 0
            
            for path_str in cached_paths:
                try:
                    path_obj = Path(path_str)
                    if path_obj.exists():
                        size = path_obj.stat().st_size
                        valid_candidates.append((path_obj, size))
                    else:
                        invalid_count += 1
                except Exception:
                    invalid_count += 1
                    continue
            
            print(f" {len(valid_candidates):,} valid files")
            if invalid_count > 0:
                print(f"  - Skipped {invalid_count:,} invalid/missing cached files")
            
            return valid_candidates
            
        except Exception as e:
            print(f"Error loading cached candidates: {e}")
            print("Falling back to fresh discovery...")
            return []
    
    def _scan_recursive(self, path: Path, candidates: List[Tuple[Path, int]], 
                       stats: dict, scan_id: Optional[str], drive_id: Optional[int],
                       config: Optional[dict], auto_checkpoint: bool):
        """Recursively scan directory for media files."""
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    stats['total_scanned'] += 1
                    
                    # Progress reporting
                    if stats['total_scanned'] % 10000 == 0:
                        print(f"  - Scanned {stats['total_scanned']:,} items, "
                              f"found {len(candidates):,} media files...", flush=True)
                        
                        # Periodic checkpoint during discovery
                        if (auto_checkpoint and self.checkpoint_manager and 
                            scan_id and stats['total_scanned'] % 50000 == 0):
                            self._save_periodic_checkpoint(
                                scan_id, path, drive_id, candidates, config, stats
                            )
                    
                    # Process files
                    if entry.is_file():
                        if self._is_media_file(entry.name):
                            try:
                                size = entry.stat().st_size
                                # SIZE FILTER - Skip files smaller than minimum
                                if DEFAULT_SMALL_FILE_BYTES > 0 and size < DEFAULT_SMALL_FILE_BYTES:
                                    stats['filtered_small'] = stats.get('filtered_small', 0) + 1
                                    continue
                                candidates.append((Path(entry.path), size))
                                stats['media_files_found'] += 1
                            except OSError:
                                stats['permission_errors'] += 1
                                continue
                    
                    # Recurse into directories
                    elif entry.is_dir():
                        try:
                            self._scan_recursive(
                                Path(entry.path), candidates, stats, 
                                scan_id, drive_id, config, auto_checkpoint
                            )
                        except (PermissionError, OSError):
                            stats['permission_errors'] += 1
                            continue
                        
        except (PermissionError, OSError):
            stats['permission_errors'] += 1
    
    def _is_media_file(self, filename: str) -> bool:
        """Check if file is a supported media type."""
        return Path(filename).suffix.lower() in SUPPORTED_EXT
    
    def _cache_candidates(self, candidates: List[Tuple[Path, int]], 
                         candidates_file: str):
        """Cache discovered candidates for potential reuse."""
        try:
            candidate_paths = [str(path) for path, _ in candidates]
            with open(candidates_file, 'w') as f:
                json.dump(candidate_paths, f)
            print(f"  - Cached {len(candidates):,} candidates to {candidates_file}")
        except Exception as e:
            print(f"  - Warning: Could not cache candidates: {e}")
    
    def _save_discovery_checkpoint(self, scan_id: str, source: Path, 
                                  drive_id: Optional[int], 
                                  candidates: List[Tuple[Path, int]], 
                                  config: Optional[dict]):
        """Save discovery completion checkpoint."""
        if not self.checkpoint_manager:
            return
            
        checkpoint = ScanCheckpoint(
            scan_id=scan_id,
            source_path=str(source),
            drive_id=drive_id or 0,
            stage='discovery',
            timestamp=utc_now_str(),
            discovered_files=[(str(p), s) for p, s in candidates],
            processed_count=len(candidates),
            config=config or {}
        )
        self.checkpoint_manager.save_checkpoint(checkpoint)
    
    def _save_periodic_checkpoint(self, scan_id: str, current_path: Path, 
                                 drive_id: Optional[int], 
                                 candidates: List[Tuple[Path, int]], 
                                 config: Optional[dict], stats: dict):
        """Save periodic checkpoint during discovery."""
        if not self.checkpoint_manager:
            return
            
        checkpoint = ScanCheckpoint(
            scan_id=scan_id,
            source_path=config.get('source_path', str(current_path)) if config else str(current_path),
            drive_id=drive_id or 0,
            stage='discovery',
            timestamp=utc_now_str(),
            discovered_files=[(str(p), s) for p, s in candidates],
            processed_count=len(candidates),
            config=config or {}
        )
        self.checkpoint_manager.save_checkpoint(checkpoint)
    
    def _print_discovery_summary(self, candidates: List[Tuple[Path, int]], 
                                stats: dict, elapsed: float):
        """Print discovery completion summary."""
        print(f"[{utc_now_str()}] Discovery complete: {len(candidates):,} media files")
        print(f"  - Total scanned: {stats['total_scanned']:,} items in {elapsed:.1f}s "
              f"({stats['total_scanned']/elapsed:.0f} items/s)")
        
        if stats['permission_errors'] > 0:
            print(f"  - Permission errors: {stats['permission_errors']:,}")
        
        # File type breakdown
        if candidates:
            from ..config import IMAGE_EXT, VIDEO_EXT
            image_count = sum(1 for path, _ in candidates 
                            if path.suffix.lower() in IMAGE_EXT)
            video_count = sum(1 for path, _ in candidates 
                            if path.suffix.lower() in VIDEO_EXT)
            
            print(f"  - File types: {image_count:,} images, {video_count:,} videos")
            
            # Size statistics
            sizes = [size for _, size in candidates]
            total_gb = sum(sizes) / (1024**3)
            avg_mb = (sum(sizes) / len(sizes)) / (1024**2) if sizes else 0
            
            print(f"  - Total size: {total_gb:.1f} GB (avg: {avg_mb:.1f} MB per file)")


class DirectoryWalker:
    """Alternative directory walker with different traversal strategies."""
    
    @staticmethod
    def walk_breadth_first(root: Path) -> List[Path]:
        """Breadth-first directory traversal."""
        directories = [root]
        all_files = []
        
        while directories:
            current_dir = directories.pop(0)
            try:
                with os.scandir(current_dir) as entries:
                    for entry in entries:
                        if entry.is_file():
                            all_files.append(Path(entry.path))
                        elif entry.is_dir():
                            directories.append(Path(entry.path))
            except (PermissionError, OSError):
                continue
                
        return all_files
    
    @staticmethod
    def walk_depth_first(root: Path) -> List[Path]:
        """Depth-first directory traversal (default os.walk behavior)."""
        all_files = []
        
        for dir_path, _, filenames in os.walk(root):
            for filename in filenames:
                all_files.append(Path(dir_path) / filename)
                
        return all_files


class MediaFileFilter:
    """Filters for media file discovery."""
    
    @staticmethod
    def filter_by_size(candidates: List[Tuple[Path, int]], 
                      min_size: int = 0, max_size: Optional[int] = None) -> List[Tuple[Path, int]]:
        """Filter candidates by file size."""
        filtered = []
        for path, size in candidates:
            if size >= min_size and (max_size is None or size <= max_size):
                filtered.append((path, size))
        return filtered
    
    @staticmethod
    def filter_by_extension(candidates: List[Tuple[Path, int]], 
                           allowed_extensions: set) -> List[Tuple[Path, int]]:
        """Filter candidates by file extension."""
        filtered = []
        for path, size in candidates:
            if path.suffix.lower() in allowed_extensions:
                filtered.append((path, size))
        return filtered
    
    @staticmethod
    def filter_by_pattern(candidates: List[Tuple[Path, int]], 
                         include_patterns: List[str] = None,
                         exclude_patterns: List[str] = None) -> List[Tuple[Path, int]]:
        """Filter candidates by path patterns."""
        import fnmatch
        
        filtered = []
        for path, size in candidates:
            path_str = str(path)
            
            # Check include patterns
            if include_patterns:
                if not any(fnmatch.fnmatch(path_str, pattern) for pattern in include_patterns):
                    continue
            
            # Check exclude patterns
            if exclude_patterns:
                if any(fnmatch.fnmatch(path_str, pattern) for pattern in exclude_patterns):
                    continue
            
            filtered.append((path, size))
            
        return filtered


# Convenience functions for common discovery operations
def discover_media_files(source: Path, **kwargs) -> List[Tuple[Path, int]]:
    """Convenience function for media file discovery."""
    discovery = FileDiscovery()
    return discovery.discover_files(source, **kwargs)


def discover_with_filters(source: Path, min_size: int = 0, 
                         max_size: Optional[int] = None,
                         extensions: Optional[set] = None,
                         include_patterns: Optional[List[str]] = None,
                         exclude_patterns: Optional[List[str]] = None,
                         **kwargs) -> List[Tuple[Path, int]]:
    """Discover files with filtering options."""
    discovery = FileDiscovery()
    candidates = discovery.discover_files(source, **kwargs)
    
    # Apply filters
    if min_size > 0 or max_size is not None:
        candidates = MediaFileFilter.filter_by_size(candidates, min_size, max_size)
    
    if extensions:
        candidates = MediaFileFilter.filter_by_extension(candidates, extensions)
    
    if include_patterns or exclude_patterns:
        candidates = MediaFileFilter.filter_by_pattern(
            candidates, include_patterns, exclude_patterns
        )
    
    return candidates