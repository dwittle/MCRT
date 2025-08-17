#!/usr/bin/env python3
"""
Complete Flask application from our refactoring.
JSON-driven UI with separated templates and full functionality.
"""

import os
from pathlib import Path
from flask import Flask, request, jsonify, send_file, render_template, abort

from cli_interface import MediaToolCLI

# Create Flask app with template and static directories
app = Flask(__name__, 
           template_folder='templates',
           static_folder='static')
app.secret_key = os.environ.get('SECRET_KEY', 'media-tool-ui-secret')

# Initialize CLI interface with auto-detection
try:
    cli = MediaToolCLI()
    print("✅ CLI interface initialized successfully")
except Exception as e:
    print(f"❌ CLI initialization failed: {e}")
    exit(1)

@app.route('/')
def dashboard():
    try:
        stats_data = cli.get_stats(detailed=True)
        print("Stats data:", stats_data)  # Debug print
        
        if 'error' in stats_data:
            return render_template('error.html', error=stats_data['error']), 500
        
        return render_template('dashboard.html', stats=stats_data['data'])
    except Exception as e:
        print(f"Dashboard error: {e}")  # Debug print
        return render_template('error.html', error=str(e)), 500

@app.route('/groups')
def view_groups():
    """Groups review page with image previews and pagination."""
    try:
        page = request.args.get('page', 1, type=int)
        status = request.args.get('status', 'undecided')
        
        groups_data = cli.get_groups_data(page=page, per_page=20, status=status)
        
        if 'error' in groups_data:
            return render_template('error.html', error=groups_data['error']), 500
        
        return render_template('groups.html', **groups_data)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

@app.route('/singles')
def view_singles():
    """Individual images review page with pagination."""
    try:
        page = request.args.get('page', 1, type=int)
        status = request.args.get('status', 'undecided')
        
        singles_data = cli.get_singles_data(page=page, per_page=50, status=status)
        
        if 'error' in singles_data:
            return render_template('error.html', error=singles_data['error']), 500
        
        return render_template('singles.html', **singles_data)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

@app.route('/bulk')
def bulk_actions():
    """Bulk actions page with enhanced preview functionality."""
    try:
        return render_template('bulk.html')
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

@app.route('/export')
def export_page():
    """Export options page."""
    try:
        return render_template('export.html')
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

# Image serving routes
@app.route('/image/<int:file_id>')
def serve_image(file_id):
    """Serve full-size image file."""
    try:
        file_info = cli.get_file_path_info(file_id)
        
        if not file_info:
            abort(404)
        
        # Construct full path
        path_on_drive = file_info['path_on_drive']
        mount_path = file_info['mount_path']
        
        if mount_path and mount_path.strip():
            full_path = Path(mount_path) / path_on_drive
        else:
            full_path = Path(path_on_drive)
        
        if not full_path.exists():
            abort(404)
        
        return send_file(str(full_path))
    except Exception as e:
        abort(404)

@app.route('/thumbnail/<int:file_id>')
def serve_thumbnail(file_id):
    """Serve thumbnail image (fallback to full image)."""
    # For now, just serve the full image
    # TODO: Implement actual thumbnail generation
    return serve_image(file_id)

# API endpoints using JSON CLI integration
@app.route('/api/stats')
def api_stats():
    """Get statistics via JSON CLI."""
    try:
        detailed = request.args.get('detailed', 'false').lower() == 'true'
        result = cli.get_stats(detailed=detailed)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/review-queue')
def api_review_queue():
    """Get review queue via JSON CLI."""
    try:
        limit = request.args.get('limit', 50, type=int)
        result = cli.get_review_queue(limit=limit)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/file-info/<int:file_id>')
def api_file_info(file_id):
    """Get detailed file information."""
    try:
        result = cli.get_file_info(file_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/mark-file', methods=['POST'])
def api_mark_file():
    """Mark individual file via JSON CLI."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        file_id = data.get('file_id')
        status = data.get('status')
        note = data.get('note', '')
        
        if not file_id or not status:
            return jsonify({'error': 'Missing file_id or status'}), 400
        
        if status not in ['keep', 'not_needed', 'undecided']:
            return jsonify({'error': 'Invalid status'}), 400
        
        result = cli.mark_file(file_id, status, note)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/mark-group', methods=['POST'])
def api_mark_group():
    """Mark entire group via JSON CLI."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        group_id = data.get('group_id')
        status = data.get('status')
        note = data.get('note', '')
        
        if not group_id or not status:
            return jsonify({'error': 'Missing group_id or status'}), 400
        
        if status not in ['keep', 'not_needed', 'undecided']:
            return jsonify({'error': 'Invalid status'}), 400
        
        result = cli.mark_group(group_id, status, note)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/promote-file', methods=['POST'])
def api_promote_file():
    """Promote file to group original via JSON CLI."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        file_id = data.get('file_id')
        
        if not file_id:
            return jsonify({'error': 'Missing file_id'}), 400
        
        result = cli.promote_file(file_id)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bulk-mark-preview', methods=['POST'])
def api_bulk_mark_preview():
    """Preview bulk mark operation via JSON CLI."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        pattern = data.get('pattern')
        if not pattern:
            return jsonify({'error': 'Missing pattern'}), 400
        
        regex = data.get('regex', False)
        limit = data.get('limit', 100)
        show_paths = data.get('show_paths', False)
        
        result = cli.bulk_mark_preview(pattern, regex, limit, show_paths)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bulk-mark-execute', methods=['POST'])
def api_bulk_mark_execute():
    """Execute bulk mark operation via JSON CLI."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        pattern = data.get('pattern')
        status = data.get('status')
        
        if not pattern or not status:
            return jsonify({'error': 'Missing pattern or status'}), 400
        
        if status not in ['keep', 'not_needed', 'undecided']:
            return jsonify({'error': 'Invalid status'}), 400
        
        regex = data.get('regex', False)
        
        result = cli.bulk_mark_execute(pattern, status, regex)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-backup', methods=['POST'])
def api_export_backup():
    """Export backup list via JSON CLI."""
    try:
        data = request.get_json() or {}
        include_undecided = data.get('include_undecided', False)
        include_large = data.get('include_large', False)
        
        # Generate filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'backup_list_{timestamp}.csv'
        
        result = cli.export_backup_list(filename, include_undecided, include_large)
        
        if 'error' in result:
            return jsonify(result), 500
        
        if Path(filename).exists():
            return send_file(filename, as_attachment=True, download_name=filename)
        else:
            return jsonify({'error': 'Export file not created'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-stats')
def api_export_stats():
    """Export detailed stats as text file."""
    try:
        success, stdout, stderr = cli.run_command('stats', '--detailed')
        
        if not success:
            return jsonify({'error': stderr}), 500
        
        # Create temporary file
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_file = f'media_stats_{timestamp}.txt'
        
        with open(temp_file, 'w') as f:
            f.write(f"Media Tool Statistics Report\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write("=" * 50 + "\n\n")
            f.write(stdout)
        
        return send_file(temp_file, as_attachment=True, download_name=temp_file)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cleanup-checkpoints', methods=['POST'])
def api_cleanup_checkpoints():
    """Cleanup checkpoints via JSON CLI."""
    try:
        data = request.get_json() or {}
        days = data.get('days', 7)
        scan_id = data.get('scan_id')
        
        result = cli.cleanup_checkpoints(days, scan_id)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Search functionality
@app.route('/search')
def search_files():
    """Search files by filename pattern."""
    try:
        query = request.args.get('q', '').strip()
        if not query:
            return render_template('search.html', query='', files=[], total=0)
        
        # Use bulk-mark preview for search functionality
        search_result = cli.bulk_mark_preview(query, regex=False, limit=200, show_paths=True)
        
        if 'error' in search_result:
            return render_template('error.html', error=search_result['error']), 500
        
        files = search_result['data']['sample_files']
        total = search_result['data']['total_matches']
        
        return render_template('search.html', query=query, files=files, total=total)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return render_template('error.html', 
                         error="Page not found",
                         details="The requested page could not be found."), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return render_template('error.html',
                         error="Internal server error", 
                         details="An unexpected error occurred."), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Handle uncaught exceptions."""
    return render_template('error.html',
                         error="Application error",
                         details=str(e)), 500

# Template filters for better formatting
@app.template_filter('filesize')
def filesize_filter(bytes_value):
    """Format bytes as human-readable file size."""
    if not bytes_value:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    size = float(bytes_value)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if size == int(size):
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"

@app.template_filter('megapixels') 
def megapixels_filter(width, height):
    """Calculate and format megapixels."""
    if not width or not height:
        return "Unknown"
    
    mp = (width * height) / 1000000
    return f"{mp:.1f} MP"

@app.template_filter('filename')
def filename_filter(path):
    """Extract filename from path."""
    return Path(path).name if path else "Unknown"

# Context processors for global template variables
@app.context_processor
def inject_globals():
    """Inject global variables into all templates."""
    return {
        'app_version': '2.0.0',
        'app_name': 'Media Review Tool'
    }

# Health check endpoint
@app.route('/health')
def health_check():
    """Health check endpoint for monitoring."""
    try:
        # Test CLI connection
        success, stdout, stderr = cli.run_command('stats')
        
        health_data = {
            'status': 'healthy' if success else 'unhealthy',
            'cli_available': success,
            'database_accessible': success,
            'error': stderr if not success else None
        }
        
        status_code = 200 if success else 503
        return jsonify(health_data), status_code
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 503

# Debug endpoint (only in development)
@app.route('/debug/info')
def debug_info():
    """Debug information endpoint."""
    if not app.debug:
        abort(404)
    
    try:
        debug_data = {
            'cli_path': cli.cli_path,
            'db_path': cli.db_path,
            'cli_exists': Path(cli.cli_path).exists(),
            'db_exists': Path(cli.db_path).exists(),
            'working_directory': os.getcwd(),
            'python_path': os.environ.get('PYTHONPATH'),
            'environment': {
                'MEDIA_CLI': os.environ.get('MEDIA_CLI'),
                'MEDIA_DB_PATH': os.environ.get('MEDIA_DB_PATH')
            }
        }
        
        return jsonify(debug_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Validate setup before starting
    print("🔍 Validating setup...")
    
    # Check if CLI is accessible
    try:
        success, stdout, stderr = cli.run_command('stats')
        if success:
            print("✅ CLI test successful")
        else:
            print(f"❌ CLI test failed: {stderr}")
            print("Make sure you've run a scan first: ./media_tool_cli.py scan --source <path> --central <path>")
    except Exception as e:
        print(f"❌ CLI validation error: {e}")
    
    # Check template directory
    template_dir = Path('templates')
    if not template_dir.exists():
        print("⚠️  Templates directory not found - some pages may not work")
        print("Copy template files from artifacts to templates/ directory")
    
    # Check static directory
    static_dir = Path('static')
    if not static_dir.exists():
        print("⚠️  Static directory not found - styling may not work")
        print("Copy CSS and JS files from artifacts to static/ directory")
    
    print("=" * 80)
    print("🚀 Enhanced Media Review Web Interface - JSON-Driven")
    print("=" * 80)
    print(f"📂 Database: {cli.db_path}")
    print(f"⚡ CLI Command: {cli.cli_path}")
    print(f"🌐 URL: http://localhost:5000")
    print("")
    print("✨ Features:")
    print("  📊 JSON-driven dashboard with real-time stats")
    print("  📁 Group review with image previews and promotion")
    print("  🖼️  Individual image review with keyboard shortcuts")
    print("  ⚡ Enhanced bulk operations with accurate preview")
    print("  🔍 Full-size image viewing with modal dialogs")
    print("  📥 Export functionality with CSV generation")
    print("  📱 Mobile-responsive design")
    print("=" * 80)
    
    # Start the server
    app.run(debug=True, host='0.0.0.0', port=5000)