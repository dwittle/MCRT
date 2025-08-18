#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scan command implementation for the Media Consolidation Tool.
- Uses process-based feature extraction (SHA-256, pHash -> hex TEXT) + single SQLite writer
- Supports checkpointing flow you already have
- Groups exact & near-duplicates (Hamming on parsed pHash)
"""

import json
import os
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

from ..config import SUPPORTED_EXT, IMAGE_EXT, VIDEO_EXT, LARGE_FILE_BYTES
from ..database.manager import DatabaseManager
from ..checkpoint.manager import CheckpointManager
from ..models.checkpoint import ScanCheckpoint
from types import SimpleNamespace
from ..storage.drive import DriveManager
from ..utils.path import ensure_dir
from ..utils.time import utc_now_str

# New pipeline / writer / grouping (ensure these modules exist in your tree)
from ..scanning.pipeline import run_scan_pipeline
from ..writer import SQLiteWriter
from ..grouping import group_duplicates


class ScanCommand:
    """Main scanner with pipeline architecture and checkpoint support."""

    def __init__(self, db_path: Path, central_path: Path):
        self.db_manager = DatabaseManager(db_path)
        self.db_path = Path(db_path)
        self.central_path = central_path
        self.checkpoint_manager = CheckpointManager(self.db_manager)
        self.drive_manager = DriveManager()
        ensure_dir(central_path)

    # --------------------------------------------------------------------- #
    # Entry point
    # --------------------------------------------------------------------- #
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
        """Execute optimized scanning with checkpoint support."""

        scan_config = {
            "wsl_mode": wsl_mode,
            "drive_label": drive_label,
            "drive_id_hint": drive_id_hint,
            "hash_large": hash_large,
            "workers": workers,
            "io_workers": io_workers,
            "phash_threshold": phash_threshold,
            "max_phash_pixels": max_phash_pixels,
            "chunk_size": chunk_size,
            "source_path": str(source),
        }

        self._print_scan_header(
            source, workers, io_workers, phash_threshold, hash_large, max_phash_pixels, chunk_size, auto_checkpoint
        )

        if auto_checkpoint:
            self.checkpoint_manager.cleanup_old_checkpoints()

        checkpoint = self._handle_resume(resume_scan_id, source)

        # Drive
        drive_id = self._get_drive_id(source, wsl_mode, drive_label, drive_id_hint, checkpoint)

        # Scan id
        scan_id = checkpoint.scan_id if checkpoint else self.checkpoint_manager.generate_scan_id(str(source))

        try:
            # -------------------------- Stage 1: Discovery --------------------------
            candidates = self._discovery_stage(
                source, skip_discovery, scan_id, drive_id, scan_config, auto_checkpoint, checkpoint
            )
            if not candidates:
                print("No media files found.")
                return

            # -------------------------- Stage 2: Extraction -------------------------
            # Uses process pool + single writer. Stores:
            #   hash_sha256 (TEXT), phash (TEXT hex or NULL), width, height, size_bytes,
            #   type, drive_id, path_on_drive, is_large, copied, duplicate_of, group_id, central_path, fast_fp
            records = self._extraction_stage(
                candidates,
                drive_id,
                hash_large,
                io_workers,
                max_phash_pixels,
                chunk_size,
                scan_id,
                scan_config,
                auto_checkpoint,
                checkpoint,
            )

            # -------------------------- Stage 3: Grouping ---------------------------
            self._grouping_stage(records, phash_threshold, scan_id, scan_config, auto_checkpoint, checkpoint)

            # Completed
            if auto_checkpoint:
                completed_checkpoint = ScanCheckpoint(
                    scan_id=scan_id,
                    source_path=str(source),
                    drive_id=drive_id,
                    stage="completed",
                    timestamp=utc_now_str(),
                    processed_count=len(records) if records else 0,
                    config=scan_config,
                )
                self.checkpoint_manager.save_checkpoint(completed_checkpoint)
                print(f"  ✅ Scan marked as completed (ID: {scan_id})")

            self._print_final_stats()

        except KeyboardInterrupt:
            if auto_checkpoint:
                print(f"\n⚠️  Scan interrupted! Resume with: --resume-scan-id {scan_id}")
            raise
        except Exception:
            if auto_checkpoint:
                print(f"\n❌ Scan failed! Resume with: --resume-scan-id {scan_id}")
            raise

    # --------------------------------------------------------------------- #
    # Printing
    # --------------------------------------------------------------------- #
    def _print_scan_header(
        self,
        source: Path,
        workers: int,
        io_workers: int,
        phash_threshold: int,
        hash_large: bool,
        max_phash_pixels: int,
        chunk_size: int,
        auto_checkpoint: bool,
    ):
        print("=" * 80)
        print(f"MEDIA TOOL OPTIMIZED SCAN - {utc_now_str()}")
        print("=" * 80)
        print(f"Source: {source}")
        print(f"Central: {self.central_path}")
        print(f"Workers: {workers} (I/O workers: {io_workers}), pHash threshold: {phash_threshold}")
        print(f"Large file threshold: {LARGE_FILE_BYTES//(1024**2)} MB")
        print(f"Hash large files: {hash_large}")
        print(f"Max pHash pixels: {max_phash_pixels:,}")
        print(f"Chunk size: {chunk_size}")
        print(f"Checkpoints: {'Enabled' if auto_checkpoint else 'Disabled'}")
        print()

    def _print_final_stats(self):
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            grouped = conn.execute("SELECT COUNT(*) FROM files WHERE group_id IS NOT NULL").fetchone()[0]
            exact = conn.execute("SELECT COUNT(*) FROM files WHERE duplicate_of IS NOT NULL").fetchone()[0]
        print("=" * 80)
        print(f"SCAN COMPLETED - {utc_now_str()}")
        print(f"Files in DB: {total:,}")
        print(f"In groups:  {grouped:,}")
        print(f"Duplicates: {exact:,}")
        print("=" * 80)

    # --------------------------------------------------------------------- #
    # Resume / Drive
    # --------------------------------------------------------------------- #
    def _handle_resume(self, resume_scan_id: Optional[str], source: Path) -> Optional[ScanCheckpoint]:
        if not resume_scan_id:
            return None
        checkpoint = self.checkpoint_manager.load_checkpoint(resume_scan_id)
        if not checkpoint:
            print(f"❌ Could not load checkpoint {resume_scan_id}")
            return None
        print(f"📋 Resuming scan from checkpoint: {checkpoint.stage} stage")
        print(f"    - Processed: {checkpoint.processed_count:,} items")
        print(f"    - Timestamp: {checkpoint.timestamp}")
        if checkpoint.source_path != str(source):
            print(f"❌ Source path mismatch: checkpoint={checkpoint.source_path}, current={source}")
            return None
        return checkpoint

    def _get_drive_id(
        self,
        source: Path,
        wsl_mode: bool,
        drive_label: Optional[str],
        drive_id_hint: Optional[str],
        checkpoint: Optional[ScanCheckpoint],
    ) -> int:
        if checkpoint and checkpoint.drive_id:
            print(f"[{utc_now_str()}] Using cached drive ID: {checkpoint.drive_id}")
            return checkpoint.drive_id

        print(f"[{utc_now_str()}] Identifying drive...")
        dm = self.drive_manager
        src = str(source)

        # Try common DriveManager APIs
        for name in ("get_or_create_drive_id", "get_or_create_drive", "ensure_drive_id", "ensure_drive"):
            if hasattr(dm, name):
                fn = getattr(dm, name)
                try:
                    res = fn(source_path=src, wsl_mode=wsl_mode, drive_label=drive_label, drive_id_hint=drive_id_hint)
                except TypeError:
                    try:
                        res = fn(src)
                    except TypeError:
                        continue
                # normalize return → int id
                if isinstance(res, int):
                    return res
                if isinstance(res, dict) and "drive_id" in res:
                    return int(res["drive_id"])
                if hasattr(res, "drive_id"):
                    return int(res.drive_id)

        # Fallback: query/insert directly (adjust column names if your schema differs)
        with sqlite3.connect(self.db_path) as conn:
            if drive_id_hint:
                row = conn.execute("SELECT drive_id FROM drives WHERE drive_id = ?", (drive_id_hint,)).fetchone()
                if row:
                    return int(row[0])
            if drive_label:
                row = conn.execute("SELECT drive_id FROM drives WHERE label = ?", (drive_label,)).fetchone()
                if row:
                    return int(row[0])
            cur = conn.execute(
                "INSERT INTO drives (label, root_path, created_at) VALUES (?, ?, datetime('now'))",
                (drive_label or "unknown", src),
            )
            return int(cur.lastrowid)


    # --------------------------------------------------------------------- #
    # Pipeline stages
    # --------------------------------------------------------------------- #
    def _discovery_stage(
        self,
        source: Path,
        skip_discovery: bool,
        scan_id: str,
        drive_id: int,
        config: dict,
        auto_checkpoint: bool,
        checkpoint: Optional[ScanCheckpoint],
    ) -> List[Tuple[Path, int]]:
        if checkpoint and checkpoint.stage in ["extraction", "grouping", "completed"]:
            print(f"[{utc_now_str()}] Loading cached discovered files...")
            return [(Path(p), s) for p, s in (checkpoint.discovered_files or [])]

        if skip_discovery:
            print(f"[{utc_now_str()}] Skipping discovery; loading candidates from last_candidates.json ...")
            try:
                with open("last_candidates.json", "r", encoding="utf-8") as f:
                    items = json.load(f)
                candidates = [(Path(p), int(s)) for p, s in items]
                print(f"  - Loaded {len(candidates):,} candidates.")
                return candidates
            except Exception:
                print("  - No last_candidates.json; running discovery instead.")

        exts = set(SUPPORTED_EXT or (IMAGE_EXT + VIDEO_EXT))
        candidates: List[Tuple[Path, int]] = []
        for root, _, files in os.walk(source, followlinks=False):
            for name in files:
                p = Path(root) / name
                try:
                    if p.suffix.lower() not in exts:
                        continue
                    st = p.stat()
                    if not st.st_size:
                        continue
                    candidates.append((p, st.st_size))
                except OSError:
                    continue

        print(f"[{utc_now_str()}] Discovered {len(candidates):,} candidate files.")

        if auto_checkpoint:
            cp = ScanCheckpoint(
                scan_id=scan_id,
                source_path=str(source),
                drive_id=drive_id,
                stage="extraction",
                timestamp=utc_now_str(),
                processed_count=len(candidates),
                config=config,
                discovered_files=[(str(p), s) for p, s in candidates],
            )
            self.checkpoint_manager.save_checkpoint(cp)
            with open("last_candidates.json", "w", encoding="utf-8") as f:
                json.dump(cp.discovered_files, f)

        return candidates

    def _extraction_stage(
        self,
        candidates: List[Tuple[Path, int]],
        drive_id: int,
        hash_large: bool,
        io_workers: int,
        max_phash_pixels: int,
        chunk_size: int,
        scan_id: str,
        config: dict,
        auto_checkpoint: bool,
        checkpoint: Optional[ScanCheckpoint],
    ) -> List[SimpleNamespace]:
        if checkpoint and checkpoint.stage in ["grouping", "completed"]:
            print(f"[{utc_now_str()}] Skipping feature extraction (already completed)")
            return self._load_records_from_db(drive_id)

        # Use the process-based pipeline; it will walk config["source_path"].
        writer = SQLiteWriter(str(self.db_path))
        try:
            run_scan_pipeline(
                root=config["source_path"],
                writer=writer,
                drive_id=drive_id,
                large_file_bytes=LARGE_FILE_BYTES,
                max_phash_pixels=max_phash_pixels,
                io_workers=io_workers,
                cpu_workers=None,      # defaults to os.cpu_count()
                filetype="image",
            )
        finally:
            writer.close()

        if auto_checkpoint:
            cp = ScanCheckpoint(
                scan_id=scan_id,
                source_path=config["source_path"],
                drive_id=drive_id,
                stage="grouping",
                timestamp=utc_now_str(),
                processed_count=0,
                config=config,
            )
            self.checkpoint_manager.save_checkpoint(cp)

        return self._load_records_from_db(drive_id)

    def _grouping_stage(
        self,
        records: List[SimpleNamespace],
        phash_threshold: int,
        scan_id: str,
        config: dict,
        auto_checkpoint: bool,
        checkpoint: Optional[ScanCheckpoint],
    ):
        if checkpoint and checkpoint.stage == "completed":
            print(f"[{utc_now_str()}] Skipping grouping (scan already completed)")
            return

        if auto_checkpoint:
            cp = ScanCheckpoint(
                scan_id=scan_id,
                source_path=config["source_path"],
                drive_id=records[0].drive_id if records else 0,
                stage="grouping",
                timestamp=utc_now_str(),
                processed_count=len(records),
                config=config,
            )
            self.checkpoint_manager.save_checkpoint(cp)

        # Run grouping directly in SQL (works with phash TEXT; grouping parses it to int)
        with sqlite3.connect(self.db_path) as conn:
            group_duplicates(conn, phash_threshold=phash_threshold)

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #
    def _load_records_from_db(self, drive_id: int) -> List[SimpleNamespace]:
        """Load minimal records (no dependency on FileRecord signature)."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT file_id, drive_id FROM files WHERE drive_id = ?",
                (drive_id,),
            ).fetchall()
        # simple objects with .file_id and .drive_id so other code keeps working
        return [SimpleNamespace(file_id=r[0], drive_id=r[1]) for r in rows]
