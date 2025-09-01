#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Comprehensive unit tests for Media Consolidation Tool CLI commands.
"""

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
import os

# Add the media_tool package to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from media_tool.database.manager import DatabaseManager
from media_tool.commands.checkpoint import cmd_list_checkpoints, cmd_cleanup_checkpoints, cmd_checkpoint_info
from media_tool.commands.review import (
    cmd_make_original, cmd_promote, cmd_move_to_group, cmd_mark, cmd_mark_group,
    cmd_bulk_mark, cmd_review_queue, cmd_export_backup_list
)
from media_tool.commands.stats import cmd_show_stats
from media_tool.checkpoint.manager import CheckpointManager
from media_tool.models.checkpoint import ScanCheckpoint
from tests.fixtures.test_db_setup import create_test_database


class TestDatabaseFixture:
    """Test fixture that provides a fresh test database for each test."""
    
    @pytest.fixture
    def test_db(self):
        """Create a fresh test database for each test."""
        db_path = create_test_database()
        db_manager = DatabaseManager(db_path)
        yield db_manager
        # Cleanup
        db_manager.close()
        db_path.unlink(missing_ok=True)
        # Clean up checkpoint files too
        for checkpoint_file in db_path.parent.glob("checkpoint*.pkl"):
            checkpoint_file.unlink(missing_ok=True)
        db_path.parent.rmdir()


class TestCheckpointCommands(TestDatabaseFixture):
    """Test checkpoint-related CLI commands."""
    
    def test_list_checkpoints_human_readable(self, test_db):
        """Test listing checkpoints in human-readable format."""
        # Capture stdout
        with patch('builtins.print') as mock_print:
            cmd_list_checkpoints(test_db, as_json=False)
        
        # Verify output was printed
        assert mock_print.called
        printed_lines = [call.args[0] for call in mock_print.call_args_list]
        assert any("Available checkpoints:" in line for line in printed_lines)
        assert any("scan_20241210_143012_a1b2c3d4" in line for line in printed_lines)
    
    def test_list_checkpoints_json(self, test_db):
        """Test listing checkpoints in JSON format.""" 
        result = cmd_list_checkpoints(test_db, as_json=True)
        
        # Should return success code 0
        assert result == 0
        
        # Verify JSON structure by checking the actual data
        with test_db.get_connection() as conn:
            checkpoints = conn.execute("""
                SELECT scan_id, source_path, stage, timestamp, processed_count
                FROM scan_checkpoints ORDER BY timestamp DESC
            """).fetchall()
        
        assert len(checkpoints) == 3  # From test data
        assert checkpoints[0][0] == "scan_20241212_160000_c3d4e5f6"  # Most recent first
    
    def test_list_checkpoints_with_filter(self, test_db):
        """Test listing checkpoints filtered by source path."""
        result = cmd_list_checkpoints(test_db, source_path="/mnt/photos", as_json=True)
        assert result == 0
    
    def test_list_checkpoints_empty(self, test_db):
        """Test listing checkpoints when none exist."""
        # Clear checkpoints table
        with test_db.get_connection() as conn:
            conn.execute("DELETE FROM scan_checkpoints")
            conn.commit()
        
        with patch('builtins.print') as mock_print:
            cmd_list_checkpoints(test_db, as_json=False)
        
        mock_print.assert_called_with("No checkpoints found.")
    
    @patch('media_tool.checkpoint.manager.CheckpointManager.load_checkpoint')
    def test_checkpoint_info_json(self, mock_load, test_db):
        """Test getting checkpoint info in JSON format."""
        # Create a mock checkpoint
        mock_checkpoint = type('MockCheckpoint', (), {
            'scan_id': 'scan_20241210_143012_a1b2c3d4',
            'source_path': '/mnt/drive1',
            'stage': 'completed',
            'timestamp': '2024-12-10T14:30:12Z',
            'drive_id': 1,
            'processed_count': 1000,
            'batch_number': 0,
            'discovered_files': [],
            'config': {'workers': 6}
        })()
        mock_load.return_value = mock_checkpoint
        
        result = cmd_checkpoint_info(test_db, "scan_20241210_143012_a1b2c3d4", as_json=True)
        assert result == 0
    
    def test_checkpoint_info_not_found(self, test_db):
        """Test getting info for non-existent checkpoint."""
        with patch('builtins.print') as mock_print:
            cmd_checkpoint_info(test_db, "nonexistent_scan_id", as_json=False)
        
        mock_print.assert_called_with("Checkpoint nonexistent_scan_id not found.")
    
    @patch('media_tool.checkpoint.manager.CheckpointManager.load_checkpoint')
    @patch('media_tool.checkpoint.manager.CheckpointManager.cleanup_checkpoint')
    def test_cleanup_specific_checkpoint(self, mock_cleanup, mock_load, test_db):
        """Test cleaning up a specific checkpoint."""
        # Mock that checkpoint exists
        mock_checkpoint = type('MockCheckpoint', (), {
            'scan_id': 'scan_20241210_143012_a1b2c3d4',
            'source_path': '/mnt/drive1'
        })()
        mock_load.return_value = mock_checkpoint
        
        result = cmd_cleanup_checkpoints(test_db, scan_id="scan_20241210_143012_a1b2c3d4", as_json=True)
        assert result == 0
        mock_cleanup.assert_called_once_with("scan_20241210_143012_a1b2c3d4")
    
    def test_cleanup_nonexistent_checkpoint(self, test_db):
        """Test cleaning up a checkpoint that doesn't exist."""
        result = cmd_cleanup_checkpoints(test_db, scan_id="nonexistent", as_json=True)
        # Should return error code 1
        assert result == 1


class TestReviewCommands(TestDatabaseFixture):
    """Test review and correction CLI commands."""
    
    def test_make_original_success(self, test_db):
        """Test making a file its own original."""
        central = Path("/tmp/central")
        result = cmd_make_original(test_db, central, file_id=2, as_json=True)
        assert result == 0
        
        # Verify database changes
        with test_db.get_connection() as conn:
            file_row = conn.execute("SELECT group_id, duplicate_of FROM files WHERE file_id=2").fetchone()
            assert file_row[1] is None  # No longer a duplicate
            
            # Should have created new group
            new_group = conn.execute("SELECT original_file_id FROM groups WHERE original_file_id=2").fetchone()
            assert new_group is not None
    
    def test_make_original_file_not_found(self, test_db):
        """Test making original with non-existent file ID."""
        central = Path("/tmp/central")
        result = cmd_make_original(test_db, central, file_id=999, as_json=True)
        assert result == 1
    
    def test_promote_success(self, test_db):
        """Test promoting a file to group original."""
        central = Path("/tmp/central")
        result = cmd_promote(test_db, central, file_id=4, as_json=True)  # Promote file 4 in group 2
        assert result == 0
        
        # Verify database changes
        with test_db.get_connection() as conn:
            group_row = conn.execute("SELECT original_file_id FROM groups WHERE group_id=2").fetchone()
            assert group_row[0] == 4  # File 4 should now be the original
    
    def test_promote_not_in_group(self, test_db):
        """Test promoting a file that's not in a group."""
        central = Path("/tmp/central")
        result = cmd_promote(test_db, central, file_id=10, as_json=True)  # File 10 is ungrouped
        assert result == 1
    
    def test_move_to_group_success(self, test_db):
        """Test moving a file to an existing group."""
        central = Path("/tmp/central")
        result = cmd_move_to_group(test_db, central, file_id=10, target_group_id=1, as_json=True)
        assert result == 0
        
        # Verify database changes
        with test_db.get_connection() as conn:
            file_row = conn.execute("SELECT group_id, duplicate_of FROM files WHERE file_id=10").fetchone()
            assert file_row[0] == 1  # Should be in group 1
            assert file_row[1] == 1  # Should be duplicate of file 1 (group 1 original)
    
    def test_move_to_nonexistent_group(self, test_db):
        """Test moving file to non-existent group."""
        central = Path("/tmp/central")
        result = cmd_move_to_group(test_db, central, file_id=10, target_group_id=999, as_json=True)
        assert result == 1
    
    def test_mark_file_success(self, test_db):
        """Test marking a file's review status."""
        result = cmd_mark(test_db, file_id=1, new_status="keep", note="Test note", as_json=True)
        assert result == 0
        
        # Verify database changes
        with test_db.get_connection() as conn:
            file_row = conn.execute("SELECT review_status, review_note FROM files WHERE file_id=1").fetchone()
            assert file_row[0] == "keep"
            assert file_row[1] == "Test note"
    
    def test_mark_file_invalid_status(self, test_db):
        """Test marking file with invalid status."""
        result = cmd_mark(test_db, file_id=1, new_status="invalid_status", as_json=True)
        assert result == 1
    
    def test_mark_group_success(self, test_db):
        """Test marking an entire group's review status."""
        result = cmd_mark_group(test_db, group_id=1, new_status="keep", as_json=True)
        assert result == 0
        
        # Verify all files in group 1 were updated
        with test_db.get_connection() as conn:
            group_files = conn.execute("SELECT review_status FROM files WHERE group_id=1").fetchall()
            assert all(row[0] == "keep" for row in group_files)
    
    def test_mark_nonexistent_group(self, test_db):
        """Test marking non-existent group."""
        result = cmd_mark_group(test_db, group_id=999, new_status="keep", as_json=True)
        assert result == 1
    
    def test_bulk_mark_preview(self, test_db):
        """Test bulk mark in preview mode."""
        result = cmd_bulk_mark(test_db, path_like="%photos%", new_status="keep", 
                              preview=True, as_json=True)
        assert result == 0
        
        # Verify no actual changes were made
        with test_db.get_connection() as conn:
            unchanged = conn.execute("SELECT COUNT(*) FROM files WHERE review_status='undecided'").fetchone()[0]
            assert unchanged > 0  # Should still have undecided files
    
    def test_bulk_mark_apply(self, test_db):
        """Test bulk mark apply mode."""
        # Count files matching pattern before
        with test_db.get_connection() as conn:
            before_count = conn.execute("SELECT COUNT(*) FROM files WHERE path_on_drive LIKE '%photos%'").fetchone()[0]
        
        result = cmd_bulk_mark(test_db, path_like="%photos%", new_status="keep", 
                              preview=False, as_json=True)
        assert result == 0
        
        # Verify changes were applied
        with test_db.get_connection() as conn:
            after_count = conn.execute("SELECT COUNT(*) FROM files WHERE path_on_drive LIKE '%photos%' AND review_status='keep'").fetchone()[0]
            assert after_count == before_count
    
    def test_review_queue_with_items(self, test_db):
        """Test review queue when items exist."""
        result = cmd_review_queue(test_db, limit=5, as_json=True)
        assert result == 0
    
    def test_review_queue_empty(self, test_db):
        """Test review queue when no undecided items exist."""
        # Mark all files as decided
        with test_db.get_connection() as conn:
            conn.execute("UPDATE files SET review_status='keep'")
            conn.commit()
        
        with patch('builtins.print') as mock_print:
            cmd_review_queue(test_db, limit=5, as_json=False)
        
        mock_print.assert_called_with("No items in review queue.")
    
    def test_export_backup_list_success(self, test_db):
        """Test exporting backup list."""
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "backup.csv"
        
        try:
            result = cmd_export_backup_list(test_db, csv_path, as_json=True)
            assert result == 0
            
            # Verify CSV was created and has content
            assert csv_path.exists()
            with csv_path.open() as f:
                lines = f.readlines()
                assert len(lines) > 1  # Header + data rows
                assert "file_id" in lines[0]  # Header row
        
        finally:
            # Cleanup
            csv_path.unlink(missing_ok=True)
            temp_dir.rmdir()
    
    def test_export_backup_list_with_filters(self, test_db):
        """Test exporting backup list with inclusion filters."""
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "backup_filtered.csv"
        
        try:
            result = cmd_export_backup_list(test_db, csv_path, include_undecided=True, 
                                           include_large=True, as_json=True)
            assert result == 0
            assert csv_path.exists()
        
        finally:
            # Cleanup
            csv_path.unlink(missing_ok=True)
            temp_dir.rmdir()


class TestStatsCommands(TestDatabaseFixture):
    """Test statistics CLI commands."""
    
    def test_show_stats_basic(self, test_db):
        """Test showing basic statistics."""
        result = cmd_show_stats(test_db, detailed=False, as_json=True)
        assert result == 0
    
    def test_show_stats_detailed(self, test_db):
        """Test showing detailed statistics.""" 
        result = cmd_show_stats(test_db, detailed=True, as_json=True)
        assert result == 0
    
    def test_show_stats_human_readable(self, test_db):
        """Test showing stats in human-readable format."""
        with patch('logging.getLogger') as mock_logger:
            mock_log = MagicMock()
            mock_logger.return_value = mock_log
            
            result = cmd_show_stats(test_db, detailed=True, as_json=False)
            
            # Should have logged statistics
            assert mock_log.info.called
            logged_messages = [call.args[0] for call in mock_log.info.call_args_list]
            assert any("Database Statistics" in msg for msg in logged_messages)


class TestErrorHandling(TestDatabaseFixture):
    """Test error handling across commands."""
    
    def test_database_connection_error(self):
        """Test handling of database connection errors."""
        # Create a path that will fail during operation, not construction
        from pathlib import Path
        import tempfile
        
        # Create a temporary directory and immediately delete it
        temp_dir = Path(tempfile.mkdtemp())
        temp_dir.rmdir()
        bad_db_path = temp_dir / "nonexistent.db"
        
        # This should fail when trying to actually use the database
        try:
            from media_tool.database.manager import DatabaseManager
            bad_db = DatabaseManager(bad_db_path)
            
            # This should raise an exception
            with pytest.raises(Exception):
                with bad_db.get_connection() as conn:
                    conn.execute("SELECT 1").fetchone()
        except Exception:
            # If DatabaseManager constructor fails, that's also expected
            pass
    
    def test_file_operations_with_readonly_db(self, test_db):
        """Test operations when database is read-only."""
        # This test would require making the database read-only
        # Implementation depends on your error handling strategy
        pass


class TestIntegrationScenarios(TestDatabaseFixture):
    """Integration tests that simulate real workflows."""
    
    def test_complete_review_workflow(self, test_db):
        """Test a complete file review workflow."""
        # 1. Check review queue
        queue_result = cmd_review_queue(test_db, limit=10, as_json=True)
        assert queue_result == 0
        
        # 2. Mark some files
        mark_result = cmd_mark(test_db, file_id=1, new_status="keep", as_json=True)
        assert mark_result == 0
        
        # 3. Promote a file in group
        promote_result = cmd_promote(test_db, Path("/tmp"), file_id=2, as_json=True)
        assert promote_result == 0
        
        # 4. Export backup list
        temp_dir = Path(tempfile.mkdtemp())
        csv_path = temp_dir / "workflow_export.csv"
        
        try:
            export_result = cmd_export_backup_list(test_db, csv_path, as_json=True)
            assert export_result == 0
            assert csv_path.exists()
        
        finally:
            csv_path.unlink(missing_ok=True)
            temp_dir.rmdir()
    
    @patch('media_tool.checkpoint.manager.CheckpointManager.load_checkpoint')
    def test_checkpoint_management_workflow(self, mock_load, test_db):
        """Test checkpoint management workflow."""
        # Mock checkpoint for info command
        mock_checkpoint = type('MockCheckpoint', (), {
            'scan_id': 'scan_20241210_143012_a1b2c3d4',
            'source_path': '/mnt/drive1',
            'stage': 'completed',
            'timestamp': '2024-12-10T14:30:12Z',
            'drive_id': 1,
            'processed_count': 1000,
            'batch_number': 0,
            'discovered_files': [],
            'config': {'workers': 6}
        })()
        mock_load.return_value = mock_checkpoint
        
        # 1. List checkpoints
        list_result = cmd_list_checkpoints(test_db, as_json=True)
        assert list_result == 0
        
        # 2. Get info on specific checkpoint
        info_result = cmd_checkpoint_info(test_db, "scan_20241210_143012_a1b2c3d4", as_json=True)
        assert info_result == 0
        
        # 3. Clean up old checkpoints
        cleanup_result = cmd_cleanup_checkpoints(test_db, days=30, as_json=True)
        assert cleanup_result == 0


# Test runner configuration
def pytest_configure():
    """Configure pytest with custom markers."""
    pytest.main.add_marker("slow", "marks tests as slow")
    pytest.main.add_marker("integration", "marks tests as integration tests")


if __name__ == "__main__":
    # Run the tests
    pytest.main([
        __file__,
        "-v",  # Verbose output
        "--tb=short",  # Short traceback format
        "--strict-markers",  # Treat unregistered markers as errors
        "-x",  # Stop on first failure
    ])