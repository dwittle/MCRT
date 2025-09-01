#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Duplicate detection for the Media Consolidation Tool.
"""

import logging
from collections import defaultdict
from typing import Dict, Set, Optional, Tuple

import imagehash

from ..database.manager import DatabaseManager
from ..models.file_record import FileRecord
from ..utils.time import utc_now_str

logger = logging.getLogger(__name__)


class DuplicateDetector:
    """Optimized duplicate detection with in-memory indexing."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self._sha_to_group: Dict[str, int] = {}
        self._phash_groups: Dict[str, Set[int]] = defaultdict(set)
        self._size_fp_buckets: Set[Tuple[int, str]] = set()
        self._refresh_indices()
    
    def _refresh_indices(self):
        """Load existing data into memory for fast lookups."""
        logger.info("Loading existing file indices...")
        
        with self.db_manager.get_connection() as conn:
            # SHA index for exact duplicates
            logger.debug("Loading SHA hash index...")
            sha_rows = conn.execute("""
                SELECT f.hash_sha256, f.group_id 
                FROM files f 
                WHERE f.hash_sha256 IS NOT NULL AND f.group_id IS NOT NULL
            """).fetchall()
            
            for sha, group_id in sha_rows:
                self._sha_to_group[sha] = group_id
            logger.debug("Loaded %d SHA hash entries", len(sha_rows))
            
            # Phash index for similar images
            logger.debug("Loading perceptual hash index...")
            phash_rows = conn.execute("""
                SELECT f.phash, f.group_id 
                FROM files f 
                WHERE f.phash IS NOT NULL AND f.group_id IS NOT NULL
            """).fetchall()
            
            for phash, group_id in phash_rows:
                if phash:
                    self._phash_groups[phash].add(group_id)
            logger.debug("Loaded %d perceptual hash entries", len(phash_rows))
            
            # Size+fingerprint buckets
            logger.debug("Loading size+fingerprint buckets...")
            bucket_rows = conn.execute("""
                SELECT size_bytes, fast_fp 
                FROM files 
                WHERE fast_fp IS NOT NULL
            """).fetchall()
            
            for size, fp in bucket_rows:
                self._size_fp_buckets.add((size, fp))
            logger.debug("Loaded %d size+fingerprint buckets", len(bucket_rows))
            
        logger.info("Index loading complete")
    
    def find_duplicate_group(self, record: FileRecord, phash_threshold: int = 5) -> Optional[int]:
        """Find existing group for this record, if any."""
        
        # Exact SHA match
        if record.sha256 and record.sha256 in self._sha_to_group:
            return self._sha_to_group[record.sha256]
        
        # Perceptual hash similarity (images only)
        if record.phash and record.file_type == 'image':
            return self._find_similar_phash_group(record.phash, phash_threshold)
        
        return None
    
    def _find_similar_phash_group(self, target_phash: str, threshold: int) -> Optional[int]:
        """Find group with similar perceptual hash using optimized search."""
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
        """Get existing (size, fast_fp) buckets for optimization."""
        return self._size_fp_buckets.copy()