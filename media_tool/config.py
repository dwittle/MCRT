#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Global configuration and constants for the Media Consolidation Tool.
"""

from pathlib import Path
from typing import Set

# File type categories
IMAGE_EXT: Set[str] = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp", ".heic"}
VIDEO_EXT: Set[str] = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".mpeg", ".mpg"}
SUPPORTED_EXT: Set[str] = IMAGE_EXT | VIDEO_EXT

# Directory names for organization
ORIGINALS_DIRNAME = "originals"
DUPLICATES_DIRNAME = "duplicates"
GROUPS_DIRNAME = "groups"

# Review statuses
REVIEW_STATUSES = {"undecided", "keep", "not_needed"}

# Default thresholds (can be overridden by CLI)
DEFAULT_PHASH_THRESHOLD = 5
DEFAULT_SMALL_FILE_BYTES = 1 * 1024 * 1024  # 1MB
DEFAULT_LARGE_FILE_BYTES = 500 * 1024 * 1024  # 500MB
DEFAULT_MAX_PHASH_PIXELS = 24_000_000

# Processing defaults
DEFAULT_WORKERS = 6
DEFAULT_IO_WORKERS = 2
DEFAULT_CHUNK_SIZE = 100

# Global variables that can be modified by CLI
PHASH_THRESHOLD = DEFAULT_PHASH_THRESHOLD
LARGE_FILE_BYTES = DEFAULT_LARGE_FILE_BYTES