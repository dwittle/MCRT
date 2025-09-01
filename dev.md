# media\_tool — technical overview (for devs)

## Purpose & high-level flow

**media\_tool** indexes large photo/video folders into SQLite, fingerprints images, and groups duplicates (exact and near-duplicates) for later review/export. The CLI is `media_tool.main` with subcommands for scanning, stats, checkpoint ops, and review/export utilities. &#x20;

The **scan** path delegates to an engine (`OptimizedScanner`) that orchestrates discovery → feature extraction → grouping, with resumable checkpoints and chunked, multi-threaded I/O.&#x20;

---

## Architecture & key modules

### CLI & commands

* `main.py` wires subcommands and global flags (e.g., `--db`, `--verbose`). It also exposes knobs like `--phash-threshold` and `--large-threshold-mb` by mutating the `config` module at runtime before invoking the scan. &#x20;
* `commands/scan.py` is a thin wrapper; it just instantiates `OptimizedScanner` with the DB and central paths.&#x20;
* Review & export:

  * `review-queue` prints undecided items with basic metadata.&#x20;
  * `export-backup-list` emits a CSV manifest of *originals* (optionally including undecided/large). &#x20;
* `stats` culminates in a structured result (JSON or logs) with counts, storage totals, per-status breakdown, and drive summaries. &#x20;

### Database layer

* `DatabaseManager` sets up a single SQLite connection with pragmatic defaults (FKs on, WAL, synchronous=NORMAL).&#x20;
* On first use, `init_db_if_needed` loads `schema.sql` if the DB is missing. (The code references `SCHEMA_FILE` and executes it.)&#x20;
* Bulk ingestion: `batch_insert_files(records, batch_size=1000)` performs `INSERT OR IGNORE` against the `files` table. Insert order reflects the column list: `{hash_sha256, phash, width, height, size_bytes, type, drive_id, path_on_drive, is_large, copied, duplicate_of, group_id, review_status, reviewed_at, review_note, central_path, fast_fp}`. &#x20;

> From usage across the codebase, the DB holds `files`, `groups`, and `scan_checkpoints`. `files` carries image/video metadata, hashes, grouping pointers, and review fields; `groups` maps one “original” to many duplicates; `scan_checkpoints` tracks resumable scans. (See queries in grouping, stats, and checkpoint modules.)  &#x20;

### Checkpointing & resume

* `CheckpointManager` generates scan IDs (`scan_YYYYMMDD_HHMMSS_<hash>`), pickles checkpoints to `.checkpoints/<id>.pkl`, and mirrors metadata to `scan_checkpoints` (stage, processed\_count, batch\_number, config). &#x20;
* Listing/cleaning leverages simple SQL over `scan_checkpoints`. &#x20;
* The scanner:

  * cleans old checkpoints (if enabled), handles `--resume-scan-id`, validates source, and continues. &#x20;
  * auto-saves a checkpoint every 5 chunks in the **extraction** stage and once at the end of the stage. &#x20;
  * can reconstruct `FileRecord`s from DB when resuming.&#x20;

### Drive detection

* `DriveManager.detect_drive_info` supports Windows volumes via `wmic` and WSL via `lsblk`, returning `(label, serial/uuid, mount_path)`. &#x20;

---

## Scanning pipeline (engine)

### Orchestration

`OptimizedScanner.execute_scan(...)` prints a config header (workers, thresholds), resolves the drive, runs the pipeline, then prints a final summary (counts/types, large threshold, dedup ratio).  &#x20;

### Feature extraction (I/O bound, threaded)

Extraction is chunked and processed with a `ThreadPoolExecutor` (I/O parallelism), returning `FileRecord`s for each candidate.&#x20;

Per file:

* Type inference from extension (`image`/`video`); mark “large” by size threshold.&#x20;
* **Fast fingerprint** (pre-filter): SHA-256 over first/last 64 KiB; stored as a 16-hex “fast\_fp” (very cheap).&#x20;
* **Full SHA-256** only when likely useful: if the size repeats and the `(size, fast_fp)` bucket already exists.&#x20;
* **Images**: get `width/height`; compute **pHash** (perceptual hash) below a pixel cap (skips if SHA already proved duplicate). Uses `imagehash` if available. &#x20;

The module also prints optimization stats: unique vs repeated sizes, estimated SHA work, and expected pHash count.&#x20;

### Grouping & deduplication

Within each processed batch:

1. Group exact duplicates by `sha256`.&#x20;
2. For items without SHA, group images by **pHash** similarity using a Hamming-distance threshold (`phash_threshold`). Multiple pHash buckets within threshold are merged.&#x20;
3. Persist each group: choose the **original** as the max `(pixels, size_bytes)` and mark other files as `duplicate_of=<original_id>`, also setting `group_id`. &#x20;

After grouping, the engine reports groups with/without duplicates and a total duplicate count, then commits DB updates.&#x20;

---

## Data model (practical view)

`FileRecord` instances supply everything `batch_insert_files` needs; from that insert signature you can infer fields: `sha256`, `phash`, `width`, `height`, `size_bytes`, `file_type`, `drive_id`, `path`, `is_large`, `fast_fp`, plus review/group pointers managed later.&#x20;

Tables (inferred by usage):

* **files**: columns mapped above, including `review_status`, `duplicate_of`, `group_id`, `central_path`, timestamps. Used heavily by stats, review, resume. &#x20;
* **groups**: holds `original_file_id`.&#x20;
* **scan\_checkpoints**: `(scan_id, source_path, drive_id, stage, timestamp, processed_count, batch_number, config_json, checkpoint_file)`.&#x20;

---

## Operational behaviors & performance notes

* **SQLite pragmas** (WAL + NORMAL + FK) balance safety with throughput; bulk inserts are batched. &#x20;
* **I/O parallelism** uses threads; hashing is short-circuiting (fast-fp first, conditional full SHA). &#x20;
* **Large file policy**: classification at `LARGE_FILE_BYTES`; surfaced in headers and summaries (you can also toggle `--hash-large`). &#x20;
* **Checkpoint cadence**: every 5 chunks during extraction + a final write; resumable with validation against the source path. &#x20;

---

## What to extend or watch for

* **Schema evolution**: Inserts use explicit column lists; keep them in sync with any `schema.sql` edits.&#x20;
* **pHash thresholds**: CLI exposes a threshold; default comes from `config`. This directly impacts near-duplicate grouping density.&#x20;
* **Drive ID & central paths**: drive metadata and mount detection are abstracted in `DriveManager`; ensure portability if you add Linux/macOS native paths outside WSL/Windows. &#x20;
* **Stats/reporting**: `cmd_show_stats` already assembles JSON; easy surface for API/UX integration.&#x20;

