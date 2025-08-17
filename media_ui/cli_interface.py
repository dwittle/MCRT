#!/usr/bin/env python3
import json
import subprocess
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

class MediaToolCLI:
    """CLI interface with automatic path detection."""
    
    def __init__(self, cli_path: str = None, db_path: str = None):
        # Auto-detect paths
        self.cli_path = self._find_cli_path(cli_path)
        self.db_path = self._find_db_path(db_path)
        
        print(f"🔧 CLI Interface initialized:")
        print(f"   CLI: {self.cli_path}")  
        print(f"   DB: {self.db_path}")
    
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
            if Path(path).exists():
                return path
        
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
            if Path(path).exists():
                return path
        
        raise FileNotFoundError("media_index.db not found in any expected location")
    
    def run_command(self, *args, timeout: int = 60) -> Tuple[bool, str, str]:
        """Run CLI command."""
        cmd = [self.cli_path, '--db', self.db_path] + list(str(arg) for arg in args)
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return False, "", f"Command timed out after {timeout}s"
        except Exception as e:
            return False, "", str(e)
    
    def run_json_command(self, *args, timeout: int = 60) -> Dict[str, Any]:
        """Run CLI command with --json flag and parse result."""
        success, stdout, stderr = self.run_command(*args, '--json', timeout=timeout)
        
        if not success:
            return {
                'error': stderr or 'Command failed',
                'command': ' '.join(str(arg) for arg in args)
            }
        
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as e:
            return {
                'error': f'Invalid JSON response: {e}',
                'raw_output': stdout
            }
    
    # Add all the other methods from the previous cli_interface.py artifact...
    def get_stats(self, detailed: bool = False) -> Dict[str, Any]:
        """Get database statistics."""
        args = ['stats']
        if detailed:
            args.append('--detailed')
        return self.run_json_command(*args)
    
    # ... (copy all other methods from ui_cli_interface artifact)

