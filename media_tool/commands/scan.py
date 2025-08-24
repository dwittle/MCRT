#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scan command (thin wrapper).
This module intentionally delegates ALL scanning logic to the engine in `scanner.py`
to avoid duplication and drift. It keeps only the CLI-facing `ScanCommand`.
"""

from pathlib import Path
from typing import Optional

# Engine
from ..scanning.scanner import OptimizedScanner


class ScanCommand:
    def __init__(self, db_path: Path, central_path: Path):
        # Keep the same constructor signature for compatibility with main.py
        self.db_path = db_path
        self.central_path = central_path
        self.engine = OptimizedScanner(db_path, central_path)

    def execute(
        self,
        source: Path,
        wsl_mode: bool = False,
        drive_label: Optional[str] = None,
        drive_id_hint: Optional[str] = None,
        hash_large: bool = False,
        workers: int = 6,
        io_workers: int = 2,
        phash_threshold: int = 5,
        skip_discovery: bool = False,
        max_phash_pixels: int = 24_000_000,
        chunk_size: int = 100,
        resume_scan_id: Optional[str] = None,
        auto_checkpoint: bool = True,
    ):
        """Run a scan by delegating to OptimizedScanner."""
        scan_config = {
            "workers": workers,
            "io_workers": io_workers,
            "phash_threshold": phash_threshold,
            "max_phash_pixels": max_phash_pixels,
            "chunk_size": chunk_size,
            "source_path": str(source),
            "hash_large": hash_large,
            "auto_checkpoint": auto_checkpoint,
        }

        # Friendly banner
        self.engine._print_scan_header(
            source, workers, io_workers, phash_threshold, hash_large, max_phash_pixels, chunk_size, auto_checkpoint
        )

        if auto_checkpoint:
            self.engine.checkpoint_manager.cleanup_old_checkpoints()

        checkpoint = self.engine._handle_resume(resume_scan_id, source)

        drive_id = self.engine._get_or_create_drive(source, wsl_mode, drive_label, drive_id_hint)
        scan_id = checkpoint.scan_id if checkpoint else self.engine.checkpoint_manager.generate_scan_id(str(source))

        try:
            # Stage 1: Discovery
            candidates = self.engine._discovery_stage(
                source=source,
                skip_discovery=skip_discovery,
                scan_id=scan_id,
                drive_id=drive_id,
                config=scan_config,
                auto_checkpoint=auto_checkpoint,
                checkpoint=checkpoint,
            )
            if not candidates:
                print("No media files found.")
                return

            # Stage 2: Extraction
            records = self.engine._extraction_stage(
                candidates=candidates,
                drive_id=drive_id,
                hash_large=hash_large,
                io_workers=io_workers,
                max_phash_pixels=max_phash_pixels,
                chunk_size=chunk_size,
                scan_id=scan_id,
                config=scan_config,
                auto_checkpoint=auto_checkpoint,
                checkpoint=checkpoint,
            )

            # Stage 3: Grouping
            self.engine._grouping_stage(
                records=records,
                phash_threshold=phash_threshold,
                scan_id=scan_id,
                config=scan_config,
                auto_checkpoint=auto_checkpoint,
                checkpoint=checkpoint,
            )

            self.engine._print_final_stats()

        except KeyboardInterrupt:
            if auto_checkpoint:
                print(f"\n⚠️  Scan interrupted! Resume with: --resume-scan-id {scan_id}")
            print("Operation interrupted by user")
