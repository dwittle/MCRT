#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test runner script for the Media Consolidation Tool.
Creates test database and runs comprehensive unit tests.
"""

import sys
import subprocess
import tempfile
from pathlib import Path
import argparse
import json
import time
from datetime import datetime

# Add media_tool to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tests.fixtures.test_db_setup import create_test_database, print_database_summary


def install_dependencies():
    """Install required test dependencies."""
    dependencies = ["pytest", "pytest-cov", "pytest-mock", "pytest-xdist"]
    
    print("Installing test dependencies...")
    for dep in dependencies:
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", dep], 
                         check=True, capture_output=True)
            print(f"✅ {dep}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {dep}: {e}")
            return False
    
    return True


def run_tests(test_path: str = None, coverage: bool = True, parallel: bool = False, 
              verbose: bool = True, markers: str = None):
    """Run the test suite with various options."""
    
    cmd = [sys.executable, "-m", "pytest"]
    
    # Test path
    if test_path:
        cmd.append(test_path)
    else:
        cmd.append(str(Path(__file__).parent))
    
    # Coverage
    if coverage:
        cmd.extend(["--cov=media_tool", "--cov-report=html", "--cov-report=term"])
    
    # Parallel execution
    if parallel:
        cmd.extend(["-n", "auto"])
    
    # Verbosity
    if verbose:
        cmd.append("-v")
    
    # Test markers
    if markers:
        cmd.extend(["-m", markers])
    
    # Additional options
    cmd.extend([
        "--tb=short",
        "--strict-markers",
        "--disable-warnings"  # Suppress warnings for cleaner output
    ])
    
    print(f"Running command: {' '.join(cmd)}")
    print("=" * 50)
    
    start_time = time.time()
    result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    end_time = time.time()
    
    print("=" * 50)
    print(f"Tests completed in {end_time - start_time:.2f} seconds")
    
    return result.returncode == 0


def run_specific_test_categories():
    """Run tests by category with detailed reporting."""
    
    categories = {
        "checkpoint": "tests/test_cli_commands.py::TestCheckpointCommands",
        "review": "tests/test_cli_commands.py::TestReviewCommands", 
        "stats": "tests/test_cli_commands.py::TestStatsCommands",
        "integration": "tests/test_cli_commands.py::TestIntegrationScenarios"
    }
    
    results = {}
    
    for category, test_path in categories.items():
        print(f"\n🧪 Running {category} tests...")
        print("-" * 30)
        
        success = run_tests(test_path, coverage=False, verbose=True)
        results[category] = "PASS" if success else "FAIL"
        
        if not success:
            print(f"❌ {category} tests failed")
        else:
            print(f"✅ {category} tests passed")
    
    # Summary
    print("\n📊 Test Results Summary:")
    print("=" * 30)
    for category, result in results.items():
        status_icon = "✅" if result == "PASS" else "❌"
        print(f"{status_icon} {category}: {result}")
    
    all_passed = all(result == "PASS" for result in results.values())
    return all_passed


def generate_test_report():
    """Generate a detailed test report with JSON output."""
    
    # Run tests with JSON report
    cmd = [
        sys.executable, "-m", "pytest",
        str(Path(__file__).parent),
        "--json-report", "--json-report-file=test_report.json",
        "-v"
    ]
    
    subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    
    # Read and display report
    report_file = Path(__file__).parent.parent / "test_report.json"
    if report_file.exists():
        with report_file.open() as f:
            report = json.load(f)
        
        print("\n📋 Detailed Test Report:")
        print("=" * 50)
        print(f"Tests run: {report['summary']['total']}")
        print(f"Passed: {report['summary'].get('passed', 0)}")
        print(f"Failed: {report['summary'].get('failed', 0)}")
        print(f"Errors: {report['summary'].get('error', 0)}")
        print(f"Skipped: {report['summary'].get('skipped', 0)}")
        print(f"Duration: {report['duration']:.2f}s")
        
        # Show failed tests if any
        if report['summary'].get('failed', 0) > 0:
            print("\n❌ Failed Tests:")
            for test in report['tests']:
                if test['outcome'] == 'failed':
                    print(f"  - {test['nodeid']}: {test['call']['longrepr']}")
        
        return report
    
    return None


def benchmark_database_operations():
    """Benchmark database operations for performance testing."""
    
    print("\n⚡ Running database performance benchmarks...")
    print("=" * 50)
    
    # Create test database
    test_db_path = create_test_database()
    
    try:
        from media_tool.database.manager import DatabaseManager
        db_manager = DatabaseManager(test_db_path)
        
        # Benchmark various operations
        benchmarks = {}
        
        # Test 1: File queries
        start = time.time()
        with db_manager.get_connection() as conn:
            for _ in range(100):
                conn.execute("SELECT COUNT(*) FROM files").fetchone()
        benchmarks['file_queries'] = time.time() - start
        
        # Test 2: Complex joins
        start = time.time()
        with db_manager.get_connection() as conn:
            for _ in range(10):
                conn.execute("""
                    SELECT f.file_id, f.path_on_drive, g.group_id, d.label
                    FROM files f
                    LEFT JOIN groups g ON f.group_id = g.group_id
                    LEFT JOIN drives d ON f.drive_id = d.drive_id
                    LIMIT 100
                """).fetchall()
        benchmarks['complex_joins'] = time.time() - start
        
        # Test 3: Bulk updates
        start = time.time()
        with db_manager.get_connection() as conn:
            conn.execute("UPDATE files SET review_status='keep' WHERE file_id IN (1,2,3)")
            conn.commit()
        benchmarks['bulk_updates'] = time.time() - start
        
        # Display results
        print("Benchmark Results:")
        for operation, duration in benchmarks.items():
            print(f"  {operation}: {duration:.4f}s")
        
        db_manager.close()
        
    finally:
        # Cleanup
        test_db_path.unlink(missing_ok=True)
        test_db_path.parent.rmdir()
    
    return benchmarks


def main():
    """Main test runner with various options."""
    
    parser = argparse.ArgumentParser(description="Media Tool Test Runner")
    parser.add_argument("--install-deps", action="store_true", 
                       help="Install test dependencies")
    parser.add_argument("--coverage", action="store_true", default=True,
                       help="Run with coverage reporting")
    parser.add_argument("--parallel", action="store_true",
                       help="Run tests in parallel")
    parser.add_argument("--categories", action="store_true",
                       help="Run tests by category")
    parser.add_argument("--benchmark", action="store_true",
                       help="Run performance benchmarks")
    parser.add_argument("--report", action="store_true",
                       help="Generate detailed JSON report")
    parser.add_argument("--test-db-only", action="store_true",
                       help="Only create and display test database")
    parser.add_argument("--markers", 
                       help="Run tests with specific pytest markers")
    parser.add_argument("--path", 
                       help="Run specific test file or directory")
    
    args = parser.parse_args()
    
    print("🧪 Media Tool Test Runner")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Install dependencies if requested
    if args.install_deps:
        if not install_dependencies():
            return 1
        print()
    
    # Create test database and show summary
    if args.test_db_only:
        test_db = create_test_database()
        print_database_summary(test_db)
        return 0
    
    # Run benchmarks if requested
    if args.benchmark:
        benchmark_database_operations()
        print()
    
    # Choose test execution mode
    success = True
    
    if args.categories:
        success = run_specific_test_categories()
    elif args.report:
        report = generate_test_report()
        success = report and report['summary'].get('failed', 0) == 0
    else:
        success = run_tests(
            test_path=args.path,
            coverage=args.coverage,
            parallel=args.parallel,
            markers=args.markers
        )
    
    # Final summary
    print("\n🎯 Final Result:")
    print("=" * 20)
    if success:
        print("✅ All tests passed!")
        return_code = 0
    else:
        print("❌ Some tests failed!")
        return_code = 1
    
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return return_code


if __name__ == "__main__":
    sys.exit(main())