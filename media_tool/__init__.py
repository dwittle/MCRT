"""Media Consolidation & Review Tool - Enhanced with Checkpoint Support."""

__version__ = "2.0.0"
__author__ = "Media Tool Team"

# Import key classes for convenient top-level access
from .commands import ScanCommand
from .database import DatabaseManager
from .checkpoint import CheckpointManager
from .scanning import OptimizedScanner, FileDiscovery, FeatureExtractor, DuplicateDetector
from .models import FileRecord, ScanCheckpoint

# Common convenience imports
from .utils import utc_now_str, now_iso, ensure_dir

__all__ = [
    # Core classes
    'ScanCommand',
    'DatabaseManager', 
    'CheckpointManager',
    'OptimizedScanner',
    
    # Scanning components
    'FileDiscovery',
    'FeatureExtractor',
    'DuplicateDetector',
    
    # Data models
    'FileRecord',
    'ScanCheckpoint',
    
    # Utilities
    'utc_now_str',
    'now_iso', 
    'ensure_dir',
    
    # Package metadata
    '__version__',
    '__author__'
]