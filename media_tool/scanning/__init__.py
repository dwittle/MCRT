"""Scanning and processing modules for the Media Consolidation Tool."""

from .extractor import FeatureExtractor
from .detector import DuplicateDetector
from .discovery import FileDiscovery, DirectoryWalker, MediaFileFilter, discover_media_files, discover_with_filters
from .scanner import OptimizedScanner

__all__ = [
    'FeatureExtractor',
    'DuplicateDetector', 
    'FileDiscovery',
    'DirectoryWalker',
    'MediaFileFilter',
    'OptimizedScanner',
    'discover_media_files',
    'discover_with_filters'
]