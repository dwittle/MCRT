-- =========================================================
-- Fresh schema for Media Consolidation & Review Tool (MCRT)
-- Drops any existing tables and recreates them from scratch
-- =========================================================

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=OFF;  -- temporarily OFF for drop/recreate

-- ---------- Drop old tables if present ----------
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS groups;
DROP TABLE IF EXISTS scan_checkpoints;
DROP TABLE IF EXISTS drives;

PRAGMA foreign_keys=ON;

-- ---------- Drives ----------
CREATE TABLE drives (
  drive_id    INTEGER PRIMARY KEY,
  label       TEXT,
  root_path   TEXT,
  wsl_mode    INTEGER DEFAULT 0,

  -- New columns used by fast, no-probe drive identification
  fingerprint TEXT,          -- stable ID: by-id name, PARTUUID, WWID, maj:min, or mount point
  model       TEXT,
  serial_or_uuid      TEXT,
  wwid        TEXT,
  partuuid    TEXT,
  device      TEXT,          -- e.g., /dev/sdd2 (may be empty on WSL physical mounts)
  mount_point TEXT,          -- e.g., /mnt/wsl/PHYSICALDRIVE3p2

  created_at  TEXT DEFAULT (datetime('now'))
);

-- Fingerprint should be unique when present (NULLs allowed)
CREATE UNIQUE INDEX idx_drives_fingerprint ON drives(fingerprint);

-- ---------- Groups ----------
CREATE TABLE groups (
  group_id          INTEGER PRIMARY KEY,
  original_file_id  INTEGER,
  created_at        TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(original_file_id) REFERENCES files(file_id) ON DELETE SET NULL
);

-- ---------- Files ----------
CREATE TABLE files (
  file_id       INTEGER PRIMARY KEY,
  hash_sha256   TEXT,
  phash         TEXT,
  width         INTEGER,
  height        INTEGER,
  size_bytes    INTEGER,
  type          TEXT,
  drive_id      INTEGER,
  path_on_drive TEXT,
  is_large      INTEGER DEFAULT 0,
  copied        INTEGER DEFAULT 0,
  duplicate_of  INTEGER,
  group_id      INTEGER,
  review_status TEXT DEFAULT 'undecided',
  reviewed_at   TEXT,
  review_note   TEXT,
  central_path  TEXT,
  fast_fp       TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(drive_id)     REFERENCES drives(drive_id)  ON DELETE SET NULL,
  FOREIGN KEY(duplicate_of) REFERENCES files(file_id)    ON DELETE SET NULL,
  FOREIGN KEY(group_id)     REFERENCES groups(group_id)  ON DELETE SET NULL
);

-- Useful indexes
CREATE UNIQUE INDEX idx_files_unique_path ON files(drive_id, path_on_drive);
CREATE INDEX idx_files_sha       ON files(hash_sha256);
CREATE INDEX idx_files_phash     ON files(phash);
CREATE INDEX idx_files_size_fp   ON files(size_bytes, fast_fp);
CREATE INDEX idx_files_group     ON files(group_id);

-- ---------- Scan checkpoints ----------
CREATE TABLE scan_checkpoints (
  scan_id         TEXT PRIMARY KEY,
  source_path     TEXT NOT NULL,
  drive_id        INTEGER,
  stage           TEXT NOT NULL,
  timestamp       TEXT NOT NULL DEFAULT (datetime('now')),
  processed_count INTEGER DEFAULT 0,
  batch_number    INTEGER DEFAULT 0,
  config_json     TEXT,
  checkpoint_file TEXT,
  discovered_json TEXT
);

CREATE INDEX idx_checkpoints_stage     ON scan_checkpoints(stage);
CREATE INDEX idx_checkpoints_timestamp ON scan_checkpoints(timestamp);

-- Final safety PRAGMAs
PRAGMA foreign_keys=ON;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
