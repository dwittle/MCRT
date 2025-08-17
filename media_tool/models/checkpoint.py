#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data structures for checkpoints in the Media Consolidation Tool.
"""

from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple, Dict, Any


@dataclass
class ScanCheckpoint:
    """Checkpoint data structure for resuming scans."""
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
        """Convert checkpoint to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScanCheckpoint':
        """Create checkpoint from dictionary."""
        return cls(**data)