#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database schema definitions for the Media Consolidation Tool.
"""

# Main schema for media files and groups
MAIN_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS drives (
    drive_id INTEGER PRIMARY KEY,
    label TEXT,
    serial_or_uuid TEXT,
    mount_path TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY,
    original_file_id INTEGER,
    FOREIGN KEY(original_file_id) REFERENCES files(file_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY,
    hash_sha256 TEXT,
    phash TEXT,
    width INTEGER,
    height INTEGER,
    size_bytes INTEGER,
    type TEXT,
    drive_id INTEGER,
    path_on_drive TEXT,
    is_large INTEGER DEFAULT 0,
    copied INTEGER DEFAULT 0,
    duplicate_of INTEGER,
    group_id INTEGER,
    review_status TEXT DEFAULT 'undecided',
    reviewed_at TEXT,
    review_note TEXT,
    central_path TEXT,
    fast_fp TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(drive_id) REFERENCES drives(drive_id) ON DELETE SET NULL,
    FOREIGN KEY(duplicate_of) REFERENCES files(file_id) ON DELETE SET NULL,
    FOREIGN KEY(group_id) REFERENCES groups(group_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_files_sha ON files(hash_sha256);
CREATE INDEX IF NOT EXISTS idx_files_phash ON files(phash);
CREATE INDEX IF NOT EXISTS idx_files_size_fp ON files(size_bytes, fast_fp);
CREATE INDEX IF NOT EXISTS idx_files_group ON files(group_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_files_unique_path ON files(drive_id, path_on_drive);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY,
    ts TEXT,
    action TEXT,
    details TEXT
);
"""

# Checkpoint schema
CHECKPOINT_SCHEMA = """
CREATE TABLE IF NOT EXISTS scan_checkpoints (
    scan_id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    drive_id INTEGER,
    stage TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    processed_count INTEGER DEFAULT 0,
    batch_number INTEGER DEFAULT 0,
    config_json TEXT,
    checkpoint_file TEXT
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_stage ON scan_checkpoints(stage);
CREATE INDEX IF NOT EXISTS idx_checkpoints_timestamp ON scan_checkpoints(timestamp);
"""