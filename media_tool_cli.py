#!/usr/bin/env python3
"""
Entry point script for Media Consolidation Tool CLI.
Updated to use OptimizedScanner directly since ScanCommand has issues.
"""

import sys
from pathlib import Path

# Add the current directory to Python path so media_tool can be imported
sys.path.insert(0, str(Path(__file__).parent))

# Import the working scanner instead of the broken command
from media_tool.scanning.scanner import OptimizedScanner

def main():
    """Main function using OptimizedScanner directly."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Media Consolidation Tool (Direct Scanner)")
    parser.add_argument("command", choices=["scan"], help="Command to run")
    parser.add_argument("--source", required=True, help="Source path to scan")
    parser.add_argument("--central", required=True, help="Central storage directory")
    parser.add_argument("--workers", type=int, default=6, help="Number of workers")
    parser.add_argument("--io-workers", type=int, default=2, help="I/O workers")
    parser.add_argument("--phash-threshold", type=int, default=5, help="pHash threshold")
    parser.add_argument("--chunk-size", type=int, default=100, help="Chunk size")
    parser.add_argument("--no-checkpoints", action="store_true", help="Disable checkpoints")
    parser.add_argument("--hash-large", action="store_true", help="Hash large files")
    parser.add_argument("--skip-discovery", action="store_true", help="Skip discovery")
    
    args = parser.parse_args()
    
    if args.command == "scan":
        # Use OptimizedScanner directly since it works
        db_path = Path("media_index.db")
        central_path = Path(args.central)
        
        scanner = OptimizedScanner(db_path, central_path)
        
        scanner.scan_source(
            source=Path(args.source),
            workers=args.workers,
            io_workers=args.io_workers,
            phash_threshold=args.phash_threshold,
            chunk_size=args.chunk_size,
            auto_checkpoint=not args.no_checkpoints,
            hash_large=args.hash_large,
            skip_discovery=args.skip_discovery
        )
    
if __name__ == "__main__":
    main()