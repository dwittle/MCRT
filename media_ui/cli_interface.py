#!/usr/bin/env python3
import json
import subprocess
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

class MediaToolCLI:
    """CLI interface with automatic path detection and enhanced debugging."""
    
    def __init__(self, cli_path: str = None, db_path: str = None):
        # Auto-detect paths
        self.cli_path = self._find_cli_path(cli_path)
        self.db_path = self._find_db_path(db_path)
        
        print(f"🔧 CLI Interface initialized:")
        print(f"   CLI: {self.cli_path}")  
        print(f"   DB: {self.db_path}")
        
        # Test CLI accessibility
        self._test_cli_basic()
    
    def _find_cli_path(self, cli_path):
        """Find CLI script automatically."""
        if cli_path and Path(cli_path).exists():
            return cli_path
        
        # Check environment variable
        env_path = os.environ.get('MEDIA_CLI')
        if env_path and Path(env_path).exists():
            return env_path
        
        # Check common locations
        possible_paths = [
            './media_tool_cli.py',      # Same directory
            '../media_tool_cli.py',     # Parent directory
            '../../media_tool_cli.py'   # Grandparent directory
        ]
        
        for path in possible_paths:
            abs_path = Path(path).resolve()
            print(f"🔍 Checking CLI path: {abs_path} - {'EXISTS' if abs_path.exists() else 'NOT FOUND'}")
            if abs_path.exists():
                return str(abs_path)
        
        raise FileNotFoundError("media_tool_cli.py not found in any expected location")
    
    def _find_db_path(self, db_path):
        """Find database automatically."""
        if db_path and Path(db_path).exists():
            return db_path
        
        # Check environment variable
        env_path = os.environ.get('MEDIA_DB_PATH')
        if env_path and Path(env_path).exists():
            return env_path
        
        # Check common locations
        possible_paths = [
            './media_index.db',      # Same directory
            '../media_index.db',     # Parent directory  
            '../../media_index.db'   # Grandparent directory
        ]
        
        for path in possible_paths:
            abs_path = Path(path).resolve()
            print(f"🔍 Checking DB path: {abs_path} - {'EXISTS' if abs_path.exists() else 'NOT FOUND'}")
            if abs_path.exists():
                return str(abs_path)
        
        raise FileNotFoundError("media_index.db not found in any expected location")
    
    def _test_cli_basic(self):
        """Test basic CLI functionality."""
        try:
            # Test if CLI script is executable
            cli_path = Path(self.cli_path)
            if not os.access(cli_path, os.X_OK):
                print(f"⚠️ CLI script is not executable: {cli_path}")
                print(f"   Run: chmod +x {cli_path}")
            
            # Test basic command
            print(f"🧪 Testing CLI with basic command...")
            success, stdout, stderr = self.run_command('--help', timeout=10)
            if success:
                print(f"✅ CLI help command successful")
            else:
                print(f"❌ CLI help command failed:")
                print(f"   STDERR: {stderr}")
                print(f"   STDOUT: {stdout}")
                
        except Exception as e:
            print(f"❌ CLI test error: {e}")
    
    def run_command(self, *args, timeout: int = 60) -> Tuple[bool, str, str]:
        """Run CLI command with enhanced debugging."""
        cmd = [self.cli_path, '--db', self.db_path] + list(str(arg) for arg in args)
        
        print(f"🔧 Running command: {' '.join(cmd)}")
        print(f"🔧 Working directory: {os.getcwd()}")
        print(f"🔧 Python path: {os.environ.get('PYTHONPATH', 'Not set')}")
        
        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=timeout,
                cwd=os.getcwd()
            )
            
            print(f"🔧 Return code: {result.returncode}")
            if result.stdout:
                print(f"🔧 STDOUT (first 500 chars): {result.stdout[:500]}")
            if result.stderr:
                print(f"🔧 STDERR: {result.stderr}")
                
            return result.returncode == 0, result.stdout, result.stderr
            
        except subprocess.TimeoutExpired:
            error_msg = f"Command timed out after {timeout}s"
            print(f"❌ {error_msg}")
            return False, "", error_msg
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Command execution error: {error_msg}")
            return False, "", error_msg
    
    def run_json_command(self, *args, timeout: int = 60) -> Dict[str, Any]:
        """Run CLI command with --json flag and parse result."""
        success, stdout, stderr = self.run_command(*args, '--json', timeout=timeout)
        
        if not success:
            error_msg = stderr or 'Command failed'
            print(f"❌ JSON command failed: {error_msg}")
            return {
                'error': error_msg,
                'command': ' '.join(str(arg) for arg in args),
                'debug_info': {
                    'success': success,
                    'stdout': stdout[:500] if stdout else None,
                    'stderr': stderr[:500] if stderr else None
                }
            }
        
        if not stdout.strip():
            print(f"❌ Empty stdout from command")
            return {
                'error': 'Empty response from CLI',
                'command': ' '.join(str(arg) for arg in args)
            }
        
        try:
            parsed = json.loads(stdout)
            print(f"✅ JSON parsed successfully")
            return parsed
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            print(f"❌ Raw output: {stdout[:500]}")
            return {
                'error': f'Invalid JSON response: {e}',
                'raw_output': stdout[:1000],
                'command': ' '.join(str(arg) for arg in args)
            }
    
    def get_stats(self, detailed: bool = False) -> Dict[str, Any]:
        """Get database statistics with enhanced error handling."""
        print(f"📊 Getting stats (detailed={detailed})...")
        args = ['stats']
        if detailed:
            args.append('--detailed')
        
        result = self.run_json_command(*args)
        
        # If CLI returns error, try to get basic info another way
        if 'error' in result:
            print(f"📊 CLI stats failed, trying direct database access...")
            return self._get_stats_fallback()
        
        return result
    
    def _get_stats_fallback(self) -> Dict[str, Any]:
        """Fallback stats method using direct database access."""
        try:
            import sqlite3
            from datetime import datetime
            
            print(f"📊 Attempting direct database connection to: {self.db_path}")
            
            with sqlite3.connect(self.db_path) as conn:
                # Basic counts
                file_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
                group_count = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
                
                # Status breakdown
                status_rows = conn.execute("""
                    SELECT review_status, COUNT(*) 
                    FROM files 
                    GROUP BY review_status
                """).fetchall()
                
                status_counts = dict(status_rows)
                
                # Basic size info
                size_info = conn.execute("""
                    SELECT 
                        COUNT(*) as total_files,
                        COALESCE(SUM(size_bytes), 0) as total_bytes,
                        COUNT(CASE WHEN type='image' THEN 1 END) as image_count,
                        COUNT(CASE WHEN type='video' THEN 1 END) as video_count
                    FROM files
                """).fetchone()
                
                total_bytes = size_info[1] or 0
                
                return {
                    "command": "stats",
                    "timestamp": datetime.now().isoformat() + "Z",
                    "data": {
                        "files": {
                            "total": file_count,
                            "images": size_info[2] or 0,
                            "videos": size_info[3] or 0,
                            "large_files": 0
                        },
                        "groups": {
                            "total": group_count,
                            "single_file": 0,
                            "multi_file": group_count,
                            "largest_size": 0,
                            "average_size": 0
                        },
                        "review_status": {
                            "undecided": status_counts.get('undecided', 0),
                            "keep": status_counts.get('keep', 0),
                            "not_needed": status_counts.get('not_needed', 0)
                        },
                        "storage": {
                            "total_bytes": total_bytes,
                            "total_gb": round(total_bytes / (1024**3), 2),
                            "average_mb": round((total_bytes / file_count / (1024**2)) if file_count > 0 else 0, 1)
                        },
                        "drives": 0
                    },
                    "fallback": True
                }
                
        except Exception as e:
            print(f"❌ Fallback stats failed: {e}")
            return {
                'error': f'Both CLI and direct database access failed: {e}',
                'cli_path': self.cli_path,
                'db_path': self.db_path,
                'working_dir': os.getcwd()
            }
    
    def get_review_queue(self, limit: int = 50) -> Dict[str, Any]:
        """Get review queue via JSON CLI."""
        return self.run_json_command('review-queue', '--limit', limit)
    
    def get_file_info(self, file_id: int) -> Dict[str, Any]:
        """Get file information."""
        # This would need to be implemented in your CLI
        # For now, return a basic structure
        return {
            "file_id": file_id,
            "path_on_drive": "Unknown",
            "width": None,
            "height": None,
            "size_bytes": None,
            "review_status": "undecided",
            "is_original": False,
            "drive_label": None
        }
    
    def get_file_path_info(self, file_id: int) -> Optional[Dict[str, Any]]:
        """Get file path information for serving."""
        try:
            import sqlite3
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute("""
                    SELECT f.path_on_drive, d.mount_path
                    FROM files f
                    LEFT JOIN drives d ON d.drive_id = f.drive_id
                    WHERE f.file_id = ?
                """, (file_id,)).fetchone()
                
                if row:
                    return {
                        'path_on_drive': row[0],
                        'mount_path': row[1] or ''
                    }
                return None
        except Exception as e:
            print(f"❌ Error getting file path info: {e}")
            return None
    
    def mark_file(self, file_id: int, status: str, note: str = '') -> Dict[str, Any]:
        """Mark file via JSON CLI."""
        return self.run_json_command('mark', '--file-id', file_id, '--status', status, '--note', note)
    
    def mark_group(self, group_id: int, status: str, note: str = '') -> Dict[str, Any]:
        """Mark group via JSON CLI."""
        return self.run_json_command('mark-group', '--group-id', group_id, '--status', status, '--note', note)
    
    def promote_file(self, file_id: int) -> Dict[str, Any]:
        """Promote file via JSON CLI."""
        return self.run_json_command('promote', '--file-id', file_id)
    
    def bulk_mark_preview(self, pattern: str, regex: bool = False, limit: int = 100, show_paths: bool = False) -> Dict[str, Any]:
        """Preview bulk mark operation."""
        args = ['bulk-mark', '--path-like', pattern, '--limit', limit]
        if regex:
            args.append('--regex')
        if show_paths:
            args.append('--show-paths')
        
        return self.run_json_command(*args)
    
    def bulk_mark_execute(self, pattern: str, status: str, regex: bool = False) -> Dict[str, Any]:
        """Execute bulk mark operation."""
        args = ['bulk-mark', '--path-like', pattern, '--status', status]
        if regex:
            args.append('--regex')
        
        return self.run_json_command(*args)
    
    def export_backup_list(self, filename: str, include_undecided: bool = False, include_large: bool = False) -> Dict[str, Any]:
        """Export backup list."""
        args = ['export-backup-list', '--out', filename]
        if include_undecided:
            args.append('--include-undecided')
        if include_large:
            args.append('--include-large')
        
        return self.run_json_command(*args)
    
    def cleanup_checkpoints(self, days: int = 7, scan_id: str = None) -> Dict[str, Any]:
        """Clean up checkpoints."""
        args = ['cleanup-checkpoints', '--days', days]
        if scan_id:
            args.extend(['--scan-id', scan_id])
        
        return self.run_json_command(*args)

    def get_groups_data(self, page: int = 1, per_page: int = 20, status: str = 'undecided') -> Dict[str, Any]:
        """Get groups data - placeholder for now."""
        return {
            'groups': [],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_groups': 0,
                'total_pages': 0,
                'has_prev': False,
                'has_next': False
            },
            'status_filter': status
        }
    
    def get_singles_data(self, page: int = 1, per_page: int = 50, status: str = 'undecided') -> Dict[str, Any]:
        """Get singles data - placeholder for now."""
        return {
            'files': [],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_files': 0,
                'total_pages': 0,
                'has_prev': False,
                'has_next': False
            },
            'status_filter': status
        }

    def get_groups_data(self, page: int = 1, per_page: int = 20, status: str = 'undecided') -> Dict[str, Any]:
        """Get groups data with pagination and proper status filtering."""
        try:
            import sqlite3
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                print(f"🔍 Getting groups data: page={page}, per_page={per_page}, status={status}")
                
                # Build status filter - we want groups that have files with the specified status
                if status == 'all':
                    status_filter = ""
                    status_params = []
                    print(f"📊 Filtering: All groups")
                else:
                    # Get groups that have at least one file with the specified status
                    status_filter = """
                        AND EXISTS (
                            SELECT 1 FROM files f2 
                            WHERE f2.group_id = g.group_id 
                            AND f2.review_status = ?
                        )
                    """
                    status_params = [status]
                    print(f"📊 Filtering: Groups with {status} files")
                
                # Get total groups count with filter
                total_query = f"""
                    SELECT COUNT(DISTINCT g.group_id) 
                    FROM groups g 
                    WHERE 1=1 {status_filter}
                """
                total_groups = conn.execute(total_query, status_params).fetchone()[0]
                print(f"📊 Found {total_groups} groups matching filter")
                
                # Calculate pagination
                total_pages = max(1, (total_groups + per_page - 1) // per_page)
                offset = (page - 1) * per_page
                
                # Get groups for current page
                groups_query = f"""
                    SELECT DISTINCT g.group_id, g.original_file_id
                    FROM groups g
                    WHERE 1=1 {status_filter}
                    ORDER BY g.group_id
                    LIMIT ? OFFSET ?
                """
                
                groups = conn.execute(groups_query, status_params + [per_page, offset]).fetchall()
                print(f"📊 Retrieved {len(groups)} groups for page {page}")
                
                # Get files for each group
                groups_data = []
                for group in groups:
                    group_id = group['group_id']
                    
                    # Get all files in this group
                    files = conn.execute("""
                        SELECT 
                            f.file_id, f.path_on_drive, f.size_bytes, f.width, f.height,
                            f.review_status, f.type, f.group_id, f.duplicate_of,
                            d.label as drive_label,
                            CASE WHEN f.file_id = g.original_file_id THEN 1 ELSE 0 END as is_original
                        FROM files f
                        LEFT JOIN drives d ON d.drive_id = f.drive_id
                        LEFT JOIN groups g ON g.group_id = f.group_id
                        WHERE f.group_id = ?
                        ORDER BY is_original DESC, f.file_id
                    """, (group_id,)).fetchall()
                    
                    # Count files by status in this group
                    status_counts = {}
                    for file in files:
                        status = file['review_status']
                        status_counts[status] = status_counts.get(status, 0) + 1
                    
                    group_dict = {
                        'group_id': group_id,
                        'original_file_id': group['original_file_id'],
                        'file_count': len(files),
                        'status_counts': status_counts
                    }
                    
                    files_list = []
                    for file in files:
                        files_list.append({
                            'file_id': file['file_id'],
                            'path_on_drive': file['path_on_drive'],
                            'size_bytes': file['size_bytes'] or 0,
                            'width': file['width'],
                            'height': file['height'],
                            'review_status': file['review_status'],
                            'type': file['type'],
                            'drive_label': file['drive_label'],
                            'is_original': bool(file['is_original'])
                        })
                    
                    groups_data.append({
                        'group': group_dict,
                        'files': files_list
                    })
                
                result = {
                    'groups': groups_data,
                    'pagination': {
                        'page': page,
                        'per_page': per_page,
                        'total_groups': total_groups,
                        'total_pages': total_pages,
                        'has_prev': page > 1,
                        'has_next': page < total_pages
                    },
                    'status_filter': status
                }
                
                print(f"✅ Returning {len(groups_data)} groups, page {page} of {total_pages}")
                return result
                
        except Exception as e:
            print(f"❌ Error getting groups data: {e}")
            import traceback
            traceback.print_exc()
            return {
                'error': str(e),
                'groups': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_groups': 0,
                    'total_pages': 0,
                    'has_prev': False,
                    'has_next': False
                },
                'status_filter': status
            }

    def get_singles_data(self, page: int = 1, per_page: int = 50, status: str = 'undecided') -> Dict[str, Any]:
        """Get singles (non-grouped files) data with pagination."""
        try:
            import sqlite3
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Build status filter
                if status == 'all':
                    status_filter = ""
                    status_params = []
                else:
                    status_filter = "AND f.review_status = ?"
                    status_params = [status]
                
                # Get total singles count (files not in groups)
                total_query = f"""
                    SELECT COUNT(*) 
                    FROM files f 
                    WHERE f.group_id IS NULL {status_filter}
                """
                total_files = conn.execute(total_query, status_params).fetchone()[0]
                
                # Calculate pagination
                total_pages = (total_files + per_page - 1) // per_page
                offset = (page - 1) * per_page
                
                # Get files for current page
                files_query = f"""
                    SELECT 
                        f.file_id, f.path_on_drive, f.size_bytes, f.width, f.height,
                        f.review_status, f.type, d.label as drive_label
                    FROM files f
                    LEFT JOIN drives d ON d.drive_id = f.drive_id
                    WHERE f.group_id IS NULL {status_filter}
                    ORDER BY f.file_id
                    LIMIT ? OFFSET ?
                """
                
                files = conn.execute(files_query, status_params + [per_page, offset]).fetchall()
                
                files_list = []
                for file in files:
                    files_list.append({
                        'file_id': file['file_id'],
                        'path_on_drive': file['path_on_drive'],
                        'size_bytes': file['size_bytes'] or 0,
                        'width': file['width'],
                        'height': file['height'],
                        'review_status': file['review_status'],
                        'type': file['type'],
                        'drive_label': file['drive_label']
                    })
                
                return {
                    'files': files_list,
                    'pagination': {
                        'page': page,
                        'per_page': per_page,
                        'total_files': total_files,
                        'total_pages': total_pages,
                        'has_prev': page > 1,
                        'has_next': page < total_pages
                    },
                    'status_filter': status
                }
                
        except Exception as e:
            print(f"❌ Error getting singles data: {e}")
            return {
                'error': str(e),
                'files': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_files': 0,
                    'total_pages': 0,
                    'has_prev': False,
                    'has_next': False
                },
                'status_filter': status
            }

    def get_file_info(self, file_id: int) -> Dict[str, Any]:
        """Get detailed file information."""
        try:
            import sqlite3
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                file_info = conn.execute("""
                    SELECT 
                        f.file_id, f.path_on_drive, f.size_bytes, f.width, f.height,
                        f.review_status, f.type, f.group_id, f.duplicate_of, f.review_note,
                        f.hash_sha256, f.hash_phash, f.is_large,
                        d.label as drive_label, d.mount_path,
                        CASE WHEN f.file_id = g.original_file_id THEN 1 ELSE 0 END as is_original
                    FROM files f
                    LEFT JOIN drives d ON d.drive_id = f.drive_id
                    LEFT JOIN groups g ON g.group_id = f.group_id
                    WHERE f.file_id = ?
                """, (file_id,)).fetchone()
                
                if not file_info:
                    return {'error': f'File {file_id} not found'}
                
                return {
                    'file_id': file_info['file_id'],
                    'path_on_drive': file_info['path_on_drive'],
                    'size_bytes': file_info['size_bytes'],
                    'width': file_info['width'],
                    'height': file_info['height'],
                    'review_status': file_info['review_status'],
                    'type': file_info['type'],
                    'group_id': file_info['group_id'],
                    'review_note': file_info['review_note'],
                    'drive_label': file_info['drive_label'],
                    'is_original': bool(file_info['is_original']) if file_info['is_original'] else False,
                    'is_large': bool(file_info['is_large']) if file_info['is_large'] else False
                }
                
        except Exception as e:
            print(f"❌ Error getting file info: {e}")
            return {'error': str(e)}