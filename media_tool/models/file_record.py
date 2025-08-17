#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data structures for file records in the Media Consolidation Tool.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FileRecord:
    """Immutable file record for pipeline processing."""
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
        """Return pixel count for images."""
        return (self.width or 0) * (self.height or 0)