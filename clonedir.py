#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
copy_matching.py — Copy files matching one or more regex/glob patterns from SRC to DST, preserving tree.

Examples:
  # REGEX: copy all .py and .txt files (case-insensitive), match on path
  python copy_matching.py ./media_tool ./tmp_media_tool '.*\.py$' '.*\.txt$' -i --on path --dry-run

  # GLOB: copy all .py and .md files
  python copy_matching.py ./media_tool ./tmp_media_tool '*.py' '*.md' --glob --dry-run

  # PRESET: copy common images
  python copy_matching.py ./photos ./out --preset images --dry-run

  # PRESET: copy images + videos (media)
  python copy_matching.py ./src ./dst --preset media --dry-run

Notes:
- Provide one or more PATTERNs (regex by default, or glob if --glob).
- Existing files at the destination are skipped unless --overwrite is set.
- --on controls whether we match against just the filename or the relative path.
"""

import argparse
import fnmatch
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Tuple, Optional, List

def is_subpath(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False

def iter_files(src: Path, follow_symlinks: bool, exclude_root: Optional[Path]) -> Iterable[Path]:
    top_excluded = None
    if exclude_root and is_subpath(exclude_root, src):
        rel = exclude_root.resolve().relative_to(src.resolve())
        if rel.parts:
            top_excluded = rel.parts[0]

    for root, dirs, files in os.walk(src, followlinks=follow_symlinks):
        if top_excluded and top_excluded in dirs:
            dirs.remove(top_excluded)
        for name in files:
            yield Path(root) / name

def compile_pattern(pat: str, ignore_case: bool, use_glob: bool) -> re.Pattern:
    if use_glob:
        regex_text = fnmatch.translate(pat)  # adds \Z(?ms)
        flags = re.IGNORECASE if ignore_case else 0
        return re.compile(regex_text, flags)
    else:
        flags = re.IGNORECASE if ignore_case else 0
        return re.compile(pat, flags)

def compile_patterns(pats: List[str], ignore_case: bool, use_glob: bool) -> List[re.Pattern]:
    return [compile_pattern(p, ignore_case, use_glob) for p in pats]

def parse_size(s: str) -> int:
    """Parse human-friendly size strings into bytes. e.g. 10K, 20M, 3G."""
    multipliers = {"k": 1024, "m": 1024**2, "g": 1024**3}
    s = s.strip().lower()
    if s[-1] in multipliers:
        return int(float(s[:-1]) * multipliers[s[-1]])
    return int(s)

def should_copy(p: Path, src: Path, patterns: List[re.Pattern], match_on: str,
                min_size: Optional[int], max_size: Optional[int]) -> bool:
    target = p.name if match_on == "name" else p.relative_to(src).as_posix()
    if not any(rgx.search(target) for rgx in patterns):
        return False

    try:
        size = p.stat().st_size
    except OSError:
        return False

    if min_size is not None and size < min_size:
        return False
    if max_size is not None and size > max_size:
        return False
    return True

def copy_one(src_file: Path, src_root: Path, dst_root: Path,
             overwrite: bool, dry_run: bool, verbose: bool) -> Tuple[bool, str]:
    rel = None
    try:
        rel = src_file.relative_to(src_root)
        dst_file = dst_root / rel
        dst_file.parent.mkdir(parents=True, exist_ok=True)

        if dst_file.exists() and not overwrite:
            if verbose:
                print(f"skip (exists): {rel}")
            return False, "exists"

        if dry_run:
            if verbose:
                action = "would overwrite" if dst_file.exists() else "would copy"
                print(f"{action}: {rel}")
            return True, "dryrun"

        shutil.copy2(src_file, dst_file)
        if verbose:
            print(f"copied: {rel}")
        return True, "copied"

    except FileNotFoundError:
        if verbose:
            print(f"error (not found): {src_file}")
        return False, "error_not_found"
    except PermissionError:
        if verbose:
            print(f"error (permission): {src_file}")
        return False, "error_permission"
    except OSError as e:
        if verbose:
            print(f"error (os: {e}): {src_file}")
        return False, f"error_{e.errno or 'os'}"
    except Exception as e:
        if verbose:
            print(f"error (unexpected: {e}): {src_file}")
        return False, "error_other"


# --- Preset helpers ----------------------------------------------------------

IMAGE_EXTS = [
    "jpg", "jpeg", "png", "gif", "bmp", "tif", "tiff", "webp",
    "heic", "heif", "raw", "cr2", "nef", "arw", "orf", "rw2", "dng", "psd"
]

VIDEO_EXTS = [
    "mp4", "mov", "avi", "mkv", "wmv", "m4v", "webm", "hevc",
    "mts", "m2ts", "3gp", "flv", "mpeg", "mpg"
]

def preset_globs(name: str) -> List[str]:
    if name == "images":
        return [f"*.{e}" for e in IMAGE_EXTS]
    if name == "videos":
        return [f"*.{e}" for e in VIDEO_EXTS]
    if name == "media":  # images + videos
        return [f"*.{e}" for e in IMAGE_EXTS + VIDEO_EXTS]
    return []

# --- CLI ---------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Copy files matching one or more regex/glob patterns, preserving directory structure."
    )
    ap.add_argument("src", type=Path, help="Source directory")
    ap.add_argument("dst", type=Path, help="Destination directory")
    ap.add_argument("patterns", nargs="*", help="One or more patterns (regex by default, or glob if --glob). Optional if --preset is used.")
    ap.add_argument("--glob", action="store_true", help="Treat patterns as shell-style globs (e.g., '*.py')")
    ap.add_argument("-i", "--ignore-case", action="store_true", help="Case-insensitive match")
    ap.add_argument("--on", choices=["name", "path"], default="name",
                    help="Apply match to file 'name' (default) or relative 'path'")
    ap.add_argument("--follow-symlinks", action="store_true", help="Follow symlinked directories")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing files at destination")
    ap.add_argument("--min-size", type=parse_size,
                help="Only copy files >= this size (e.g., 10K, 20M, 1G)")
    ap.add_argument("--max-size", type=parse_size,
                help="Only copy files <= this size (e.g., 100M, 2G)")
    ap.add_argument("--workers", type=int, default=4, help="Copy in parallel with N workers (default: 4)")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be copied but make no changes")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose progress output")
    ap.add_argument(
        "--preset",
        choices=["images", "videos", "media"],
        help="Use a preset list of common file globs. Implies --glob."
    )
    args = ap.parse_args()

    src: Path = args.src
    dst: Path = args.dst

    if not src.is_dir():
        raise SystemExit(f"Source directory not found or not a directory: {src}")

    # Build the full pattern list
    effective_patterns: List[str] = list(args.patterns)
    if args.preset:
        args.glob = True  # presets are glob-based
        effective_patterns.extend(preset_globs(args.preset))

    if not effective_patterns:
        raise SystemExit("No patterns provided. Specify one or more PATTERNs or use --preset.")

    # Compile patterns
    try:
        compiled = compile_patterns(effective_patterns, args.ignore_case, args.glob)
    except re.error as e:
        raise SystemExit(f"Invalid regex: {e}")

    # Walk & match
    matched: list[Path] = []
    for p in iter_files(src, follow_symlinks=args.follow_symlinks, exclude_root=dst):
        if should_copy(p, src, compiled, args.on, args.min_size, args.max_size):
            matched.append(p)

    if args.verbose:
        mode = "glob" if args.glob else "regex"
        scope = f"{args.on} {mode} /{', '.join(effective_patterns)}/" + (" i" if args.ignore_case else "")
        print(f"Matched {len(matched):,} files in {src} ({scope}).")

    # Copy
    copied = 0
    skipped = 0
    if args.workers > 1 and not args.dry_run:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(copy_one, p, src, dst, args.overwrite, args.dry_run, args.verbose) for p in matched]
            for f in as_completed(futs):
                ok, status = f.result()
                if ok and status in ("copied", "dryrun"):
                    copied += 1
                else:
                    skipped += 1
    else:
        for p in matched:
            ok, status = copy_one(p, src, dst, args.overwrite, args.dry_run, args.verbose)
            if ok and status in ("copied", "dryrun"):
                copied += 1
            else:
                skipped += 1

    if args.dry_run:
        print(f"Dry-run complete. Would copy: {copied:,}  Skipped: {skipped:,}")
    else:
        print(f"Done. Copied: {copied:,}  Skipped: {skipped:,}  Destination: {dst}")

if __name__ == "__main__":
    main()
