#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Validation script to ensure test setup is working correctly.
Run this before running the full test suite.
"""

import sys
import sqlite3
from pathlib import Path
import importlib.util
import tempfile

# Add media_tool to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_imports():
    """Verify all required modules can be imported."""
    print("Checking imports...")
    
    required_modules = [
        'media_tool.database.manager',
        'media_tool.commands.checkpoint', 
        'media_tool.commands.review',
        'media_tool.commands.stats',
        'media_tool.jsonio',
        'media_tool.config'
    ]
    
    failed_imports = []
    
    for module_name in required_modules:
        try:
            importlib.import_module(module_name)
            print(f"  ✓ {module_name}")
        except ImportError as e:
            print(f"  ✗ {module_name}: {e}")
            failed_imports.append(module_name)
    
    return len(failed_imports) == 0


def check_test_database():
    """Verify test database can be created and populated."""
    print("\nChecking test database creation...")
    
    try:
        from tests.fixtures.test_db_setup import create_test_database
        
        # Create test database
        test_db = create_test_database()
        print(f"  ✓ Database created at: {test_db}")
        
        # Verify schema
        conn = sqlite3.connect(test_db)
        try:
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """).fetchall()
            
            expected_tables = {'drives', 'files', 'groups', 'scan_checkpoints'}
            actual_tables = {row[0] for row in tables}
            
            if expected_tables.issubset(actual_tables):
                print(f"  ✓ All required tables present: {sorted(actual_tables)}")
            else:
                missing = expected_tables - actual_tables
                print(f"  ✗ Missing tables: {missing}")
                return False
            
            # Verify data
            file_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            group_count = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
            drive_count = conn.execute("SELECT COUNT(*) FROM drives").fetchone()[0]
            checkpoint_count = conn.execute("SELECT COUNT(*) FROM scan_checkpoints").fetchone()[0]
            
            print(f"  ✓ Test data: {file_count} files, {group_count} groups, {drive_count} drives, {checkpoint_count} checkpoints")
            
            return True
            
        finally:
            conn.close()
            # Cleanup
            test_db.unlink(missing_ok=True)
            test_db.parent.rmdir()
    
    except Exception as e:
        print(f"  ✗ Database creation failed: {e}")
        return False


def check_json_functionality():
    """Verify JSON output functionality works."""
    print("\nChecking JSON functionality...")
    
    try:
        from media_tool.jsonio import success, error
        
        # Test success response
        result = success("test", {"key": "value"})
        if result == 0:  # jsonio returns exit codes
            print("  ✓ JSON success response")
        else:
            print("  ✗ JSON success response failed")
            return False
        
        # Test error response  
        result = error("test", "test error")
        if result == 1:  # jsonio returns exit codes
            print("  ✓ JSON error response")
        else:
            print("  ✗ JSON error response failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ JSON functionality failed: {e}")
        return False


def check_cli_commands():
    """Verify CLI commands can be imported and basic functionality works."""
    print("\nChecking CLI commands...")
    
    try:
        # Create minimal test database
        temp_dir = Path(tempfile.mkdtemp())
        db_path = temp_dir / "minimal_test.db"
        
        from media_tool.database.manager import DatabaseManager
        from media_tool.database.init import init_db_if_needed
        
        # Initialize database
        init_db_if_needed(db_path)
        db_manager = DatabaseManager(db_path)
        
        # Test basic command imports and execution
        from media_tool.commands.checkpoint import cmd_list_checkpoints
        from media_tool.commands.stats import cmd_show_stats
        
        # Test commands (should not crash)
        cmd_list_checkpoints(db_manager, as_json=True)
        print("  ✓ cmd_list_checkpoints")
        
        cmd_show_stats(db_manager, as_json=True) 
        print("  ✓ cmd_show_stats")
        
        db_manager.close()
        
        # Cleanup
        db_path.unlink(missing_ok=True)
        temp_dir.rmdir()
        
        return True
        
    except Exception as e:
        print(f"  ✗ CLI commands check failed: {e}")
        return False


def check_pytest_availability():
    """Check if pytest and related packages are available."""
    print("\nChecking pytest availability...")
    
    pytest_packages = ['pytest', 'pytest_cov']
    missing_packages = []
    
    for package in pytest_packages:
        try:
            importlib.import_module(package.replace('-', '_'))
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  ✗ {package} not found")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n  Install missing packages with:")
        print(f"  pip install {' '.join(missing_packages)}")
        return False
    
    return True


def run_mini_test():
    """Run a mini test to verify everything works together."""
    print("\nRunning mini integration test...")
    
    try:
        from tests.fixtures.test_db_setup import create_test_database
        from media_tool.database.manager import DatabaseManager
        from media_tool.commands.review import cmd_mark
        
        # Create test database
        test_db_path = create_test_database()
        db_manager = DatabaseManager(test_db_path)
        
        # Try to mark a file
        result = cmd_mark(db_manager, file_id=1, new_status="keep", as_json=True)
        
        if result == 0:
            print("  ✓ Mini integration test passed")
            success = True
        else:
            print("  ✗ Mini integration test failed")
            success = False
        
        # Cleanup
        db_manager.close()
        test_db_path.unlink(missing_ok=True)
        # Clean up checkpoint files too
        for checkpoint_file in test_db_path.parent.glob("checkpoint*.pkl"):
            checkpoint_file.unlink(missing_ok=True)
        test_db_path.parent.rmdir()
        
        return success
        
    except Exception as e:
        print(f"  ✗ Mini integration test failed: {e}")
        return False


def main():
    """Run all validation checks."""
    print("Media Tool Test Setup Validation")
    print("=" * 40)
    
    checks = [
        ("Module Imports", check_imports),
        ("Test Database", check_test_database), 
        ("JSON Functionality", check_json_functionality),
        ("CLI Commands", check_cli_commands),
        ("PyTest Availability", check_pytest_availability),
        ("Mini Integration Test", run_mini_test)
    ]
    
    results = {}
    all_passed = True
    
    for check_name, check_func in checks:
        print()
        try:
            results[check_name] = check_func()
            if not results[check_name]:
                all_passed = False
        except Exception as e:
            print(f"  ✗ {check_name} check crashed: {e}")
            results[check_name] = False
            all_passed = False
    
    # Summary
    print("\n" + "=" * 40)
    print("VALIDATION SUMMARY")
    print("=" * 40)
    
    for check_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        icon = "✓" if passed else "✗"
        print(f"{icon} {check_name}: {status}")
    
    print("=" * 40)
    if all_passed:
        print("✓ ALL CHECKS PASSED - Ready to run tests!")
        return 0
    else:
        print("✗ SOME CHECKS FAILED - Fix issues before running tests")
        return 1


if __name__ == "__main__":
    sys.exit(main())