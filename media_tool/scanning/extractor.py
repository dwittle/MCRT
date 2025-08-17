#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Feature extraction for the Media Consolidation Tool.
"""

import hashlib
import os
import sys
import warnings
from pathlib import Path
from typing import Optional, Set, Tuple

import imagehash
from PIL import Image

from ..config import IMAGE_EXT, VIDEO_EXT, LARGE_FILE_BYTES
from ..models.file_record import FileRecord

# Suppress PIL warnings
warnings.filterwarnings("ignore", category=UserWarning, 
                       message=".*Palette images with Transparency expressed in bytes.*")


class FeatureExtractor:
    """Optimized feature extraction with caching."""
    
    def __init__(self, max_phash_pixels: int = 36_000_000, hash_large: bool = False):
        self.max_phash_pixels = max_phash_pixels
        self.hash_large = hash_large
        
    def extract_features(self, file_path: Path, size_bytes: int, unique_size: bool, 
                        existing_buckets: Set[Tuple[int, str]]) -> Optional[FileRecord]:
        """Extract features for a single file."""
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
        """Fast partial hash of first/last blocks."""
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
        """Compute full SHA-256."""
        h = hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()