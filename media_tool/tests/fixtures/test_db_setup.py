#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test database setup and fixtures for the Media Consolidation Tool.
"""

import sqlite3
import tempfile
from pathlib import Path
from typing import List, Tuple
from datetime import datetime, timezone

def create_test_schema(db_path: Path):
    """Create the test database schema."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Create schema - based on your existing schema
        conn.executescript("""
            -- Drives table
            CREATE TABLE IF NOT EXISTS drives (
                drive_id INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT,
                serial_or_uuid TEXT,
                mount_path TEXT UNIQUE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Groups table  
            CREATE TABLE IF NOT EXISTS groups (
                group_id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_file_id INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (original_file_id) REFERENCES files(file_id)
            );
            
            -- Files table
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash_sha256 TEXT,
                phash TEXT,
                width INTEGER,
                height INTEGER,
                size_bytes INTEGER NOT NULL,
                type TEXT NOT NULL CHECK (type IN ('image', 'video')),
                drive_id INTEGER NOT NULL,
                path_on_drive TEXT NOT NULL,
                is_large INTEGER DEFAULT 0 CHECK (is_large IN (0, 1)),
                copied INTEGER DEFAULT 0 CHECK (copied IN (0, 1)),
                duplicate_of INTEGER,
                group_id INTEGER,
                review_status TEXT DEFAULT 'undecided' CHECK (review_status IN ('undecided', 'keep', 'not_needed')),
                reviewed_at DATETIME,
                review_note TEXT,
                central_path TEXT,
                fast_fp TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (drive_id) REFERENCES drives(drive_id),
                FOREIGN KEY (duplicate_of) REFERENCES files(file_id),
                FOREIGN KEY (group_id) REFERENCES groups(group_id),
                UNIQUE(drive_id, path_on_drive)
            );
            
            -- Scan checkpoints table
            CREATE TABLE IF NOT EXISTS scan_checkpoints (
                scan_id TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                drive_id INTEGER,
                stage TEXT NOT NULL CHECK (stage IN ('discovery', 'extraction', 'grouping', 'completed')),
                timestamp DATETIME NOT NULL,
                processed_count INTEGER DEFAULT 0,
                batch_number INTEGER DEFAULT 0,
                config_json TEXT,
                checkpoint_file TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (drive_id) REFERENCES drives(drive_id)
            );
            
            -- Indexes for performance
            CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash_sha256);
            CREATE INDEX IF NOT EXISTS idx_files_phash ON files(phash);
            CREATE INDEX IF NOT EXISTS idx_files_drive ON files(drive_id);
            CREATE INDEX IF NOT EXISTS idx_files_group ON files(group_id);
            CREATE INDEX IF NOT EXISTS idx_files_review_status ON files(review_status);
            CREATE INDEX IF NOT EXISTS idx_files_size ON files(size_bytes);
            CREATE INDEX IF NOT EXISTS idx_checkpoints_stage ON scan_checkpoints(stage);
        """)
        conn.commit()
    finally:
        conn.close()


def populate_test_data(db_path: Path):
    """Populate the test database with sample data."""
    conn = sqlite3.connect(str(db_path))
    try:
        # Temporarily disable foreign key checks during data insertion
        conn.execute("PRAGMA foreign_keys=OFF")
        
        print("  - Inserting drives...")
        try:
            drives_data = [
                (1, "Test Drive 1", "ABC123", "/mnt/drive1"),
                (2, "Photo Drive", "DEF456", "/mnt/photos"), 
                (3, "Video Drive", "GHI789", "/mnt/videos")
            ]
            
            conn.executemany("""
                INSERT OR REPLACE INTO drives (drive_id, label, serial_or_uuid, mount_path)
                VALUES (?, ?, ?, ?)
            """, drives_data)
            conn.commit()
            print(f"    Inserted {len(drives_data)} drives")
        except Exception as e:
            print(f"    Error inserting drives: {e}")
            raise
        
        print("  - Inserting files...")
        try:
            files_data = [
                # (file_id, hash_sha256, phash, width, height, size_bytes, type, drive_id, path_on_drive, is_large, group_id, duplicate_of, review_status, central_path, fast_fp)
                
                # Files for Group 1 - Image duplicates (different sizes)
                (1, "abc123def456", "1a2b3c4d5e6f7890", 1920, 1080, 2500000, "image", 1, "/photos/vacation1.jpg", 0, 1, None, "undecided", None, "fp001"),
                (2, "abc123def456", "1a2b3c4d5e6f7890", 1920, 1080, 2600000, "image", 1, "/photos/vacation1_copy.jpg", 0, 1, 1, "undecided", None, "fp001"),
                
                # Files for Group 2 - Similar images (different phashes but close)
                (3, "def456ghi789", "2b3c4d5e6f789012", 1280, 720, 1800000, "image", 2, "/pics/sunset.jpg", 0, 2, None, "keep", None, "fp002"),
                (4, "ghi789jkl012", "2b3c4d5e6f789013", 1280, 720, 1850000, "image", 2, "/pics/sunset_edited.jpg", 0, 2, 3, "keep", None, "fp003"),
                
                # Files for Group 3 - Video files
                (5, "jkl012mno345", None, 1920, 1080, 15000000, "video", 3, "/videos/movie.mp4", 0, 3, None, "keep", None, "fp004"),
                (6, "jkl012mno345", None, 1920, 1080, 15000000, "video", 3, "/videos/movie_backup.mp4", 0, 3, 5, "not_needed", None, "fp004"),
                
                # Files for Group 4 - Large files
                (7, "mno345pqr678", "3c4d5e6f78901234", 4000, 3000, 600000000, "image", 1, "/large/huge_photo.tiff", 1, 4, None, "undecided", None, "fp005"),
                (8, "pqr678stu901", "4d5e6f7890123456", 3840, 2160, 550000000, "image", 2, "/large/4k_image.png", 1, 4, 7, "undecided", None, "fp006"),
                
                # Files for Group 5 - Single file group
                (9, "stu901vwx234", "5e6f789012345678", 800, 600, 450000, "image", 1, "/photos/single.jpg", 0, 5, None, "keep", None, "fp007"),
                
                # Ungrouped files
                (10, "vwx234yza567", "6f78901234567890", 1024, 768, 320000, "image", 2, "/misc/random1.jpg", 0, None, None, "undecided", None, "fp008"),
                (11, "yza567bcd890", None, 1280, 720, 8500000, "video", 3, "/misc/clip.avi", 0, None, None, "not_needed", None, "fp009"),
                (12, "bcd890efg123", "789012345678901a", 640, 480, 180000, "image", 1, "/old/tiny.gif", 0, None, None, "keep", None, "fp010")
            ]
            
            conn.executemany("""
                INSERT OR REPLACE INTO files 
                (file_id, hash_sha256, phash, width, height, size_bytes, type, drive_id, path_on_drive, 
                 is_large, group_id, duplicate_of, review_status, central_path, fast_fp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, files_data)
            conn.commit()
            print(f"    Inserted {len(files_data)} files")
        except Exception as e:
            print(f"    Error inserting files: {e}")
            print(f"    This might be a CHECK constraint violation")
            print(f"    Expected review_status values: 'undecided', 'keep', 'not_needed'")
            raise
        
        print("  - Inserting groups...")
        try:
            groups_data = [
                (1, 1),  # group_id, original_file_id - file 1 is original
                (2, 3),  # group_id, original_file_id - file 3 is original  
                (3, 5),  # group_id, original_file_id - file 5 is original
                (4, 7),  # group_id, original_file_id - file 7 is original
                (5, 9)   # group_id, original_file_id - file 9 is original
            ]
            
            conn.executemany("""
                INSERT OR REPLACE INTO groups (group_id, original_file_id)
                VALUES (?, ?)
            """, groups_data)
            conn.commit()
            print(f"    Inserted {len(groups_data)} groups")
        except Exception as e:
            print(f"    Error inserting groups: {e}")
            raise
        
        print("  - Inserting checkpoints...")
        try:
            # Create simple checkpoint files that exist (just empty files for testing)
            temp_dir = db_path.parent
            checkpoint_files = []
            for i in range(1, 4):
                checkpoint_file = temp_dir / f"checkpoint{i}.pkl"
                # Create empty files so they exist for testing
                checkpoint_file.touch()
                checkpoint_files.append(str(checkpoint_file))
            
            checkpoints_data = [
                ("scan_20241210_143012_a1b2c3d4", "/mnt/drive1", 1, "completed", "2024-12-10T14:30:12Z", 1000, 0, '{"workers": 6}', checkpoint_files[0]),
                ("scan_20241211_090000_b2c3d4e5", "/mnt/photos", 2, "extraction", "2024-12-11T09:00:00Z", 500, 2, '{"workers": 4}', checkpoint_files[1]),
                ("scan_20241212_160000_c3d4e5f6", "/mnt/videos", 3, "grouping", "2024-12-12T16:00:00Z", 750, 0, '{"workers": 8}', checkpoint_files[2])
            ]
            
            conn.executemany("""
                INSERT OR REPLACE INTO scan_checkpoints 
                (scan_id, source_path, drive_id, stage, timestamp, processed_count, batch_number, config_json, checkpoint_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, checkpoints_data)
            conn.commit()
            print(f"    Inserted {len(checkpoints_data)} checkpoints")
        except Exception as e:
            print(f"    Error inserting checkpoints: {e}")
            raise
        
        print(f"✅ Test database populated with {len(files_data)} files, {len(groups_data)} groups, {len(drives_data)} drives, {len(checkpoints_data)} checkpoints")
        
    except Exception as e:
        print(f"Database population failed: {e}")
        raise
    finally:
        conn.close()


def create_test_database() -> Path:
    """Create a complete test database and return its path."""
    # Create temporary database file
    temp_dir = Path(tempfile.mkdtemp(prefix="media_tool_test_"))
    db_path = temp_dir / "test_media.db"
    
    print(f"Creating test database at: {db_path}")
    
    try:
        # Create schema and populate data
        create_test_schema(db_path)
        populate_test_data(db_path)
        
        return db_path
    except Exception as e:
        print(f"Error during database creation: {e}")
        # Clean up on failure
        if db_path.exists():
            db_path.unlink()
        # Clean up checkpoint files
        for checkpoint_file in temp_dir.glob("checkpoint*.pkl"):
            checkpoint_file.unlink(missing_ok=True)
        temp_dir.rmdir()
        raise


def print_database_summary(db_path: Path):
    """Print a summary of the test database contents."""
    conn = sqlite3.connect(str(db_path))
    try:
        # Get counts
        file_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        group_count = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0] 
        drive_count = conn.execute("SELECT COUNT(*) FROM drives").fetchone()[0]
        checkpoint_count = conn.execute("SELECT COUNT(*) FROM scan_checkpoints").fetchone()[0]
        
        # Get status breakdown
        status_counts = conn.execute("""
            SELECT review_status, COUNT(*) 
            FROM files 
            GROUP BY review_status
        """).fetchall()
        
        # Get type breakdown  
        type_counts = conn.execute("""
            SELECT type, COUNT(*) 
            FROM files 
            GROUP BY type
        """).fetchall()
        
        print("\n=== Test Database Summary ===")
        print(f"Files: {file_count}")
        print(f"Groups: {group_count}") 
        print(f"Drives: {drive_count}")
        print(f"Checkpoints: {checkpoint_count}")
        
        print("\nReview Status Breakdown:")
        for status, count in status_counts:
            print(f"  {status}: {count}")
            
        print("\nFile Type Breakdown:")
        for file_type, count in type_counts:
            print(f"  {file_type}: {count}")
        
        print(f"\nDatabase location: {db_path}")
        print("=" * 30)
        
    finally:
        conn.close()


if __name__ == "__main__":
    # Create and display test database
    test_db = create_test_database()
    print_database_summary(test_db)