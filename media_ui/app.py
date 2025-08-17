#!/usr/bin/env python3
"""
Complete Flask application with enhanced debugging and error handling.
JSON-driven UI with separated templates and full functionality.
"""

import os
import traceback
import io
from pathlib import Path
from flask import Flask, request, jsonify, send_file, render_template, abort, Response

# Import PIL for placeholder image generation
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
    print("✅ PIL (Pillow) imported successfully")
except ImportError:
    PIL_AVAILABLE = False
    print("❌ PIL (Pillow) not available - install with: pip install Pillow")

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
    traceback.print_exc()
    exit(1)

def create_placeholder_image(width=400, height=300, text="Image Not Found", file_id=None):
    """Create a placeholder image when the original file is missing."""
    if not PIL_AVAILABLE:
        print("❌ PIL not available for placeholder generation")
        return None
        
    try:
        # Create image with a neutral background
        img = Image.new('RGB', (width, height), color='#f0f0f0')
        draw = ImageDraw.Draw(img)
        
        # Try to use a default font, fallback to basic if not available
        try:
            # Try to find a system font
            font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
            font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        except:
            try:
                # Fallback for different systems
                font_large = ImageFont.truetype("arial.ttf", 24)
                font_small = ImageFont.truetype("arial.ttf", 16)
            except:
                # Use default font if no system fonts available
                font_large = ImageFont.load_default()
                font_small = ImageFont.load_default()
        
        # Draw border
        border_color = '#cccccc'
        draw.rectangle([10, 10, width-10, height-10], outline=border_color, width=2)
        
        # Draw diagonal lines to indicate missing image
        draw.line([20, 20, width-20, height-20], fill='#dddddd', width=3)
        draw.line([20, height-20, width-20, 20], fill='#dddddd', width=3)
        
        # Draw main text
        text_color = '#666666'
        
        # Calculate text position (centered)
        bbox = draw.textbbox((0, 0), text, font=font_large)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        text_x = (width - text_width) // 2
        text_y = (height - text_height) // 2 - 20
        
        draw.text((text_x, text_y), text, fill=text_color, font=font_large)
        
        # Draw file ID if provided
        if file_id:
            id_text = f"File ID: {file_id}"
            bbox = draw.textbbox((0, 0), id_text, font=font_small)
            id_width = bbox[2] - bbox[0]
            id_x = (width - id_width) // 2
            id_y = text_y + 40
            draw.text((id_x, id_y), id_text, fill=text_color, font=font_small)
        
        # Add a small icon-like rectangle in the center
        icon_size = 40
        icon_x = (width - icon_size) // 2
        icon_y = (height - icon_size) // 2 - 60
        draw.rectangle([icon_x, icon_y, icon_x + icon_size, icon_y + icon_size], 
                       fill='#e0e0e0', outline='#999999', width=2)
        
        # Draw a simple "?" in the icon
        question_mark = "?"
        bbox = draw.textbbox((0, 0), question_mark, font=font_large)
        q_width = bbox[2] - bbox[0]
        q_height = bbox[3] - bbox[1]
        q_x = icon_x + (icon_size - q_width) // 2
        q_y = icon_y + (icon_size - q_height) // 2
        draw.text((q_x, q_y), question_mark, fill='#888888', font=font_large)
        
        return img
        
    except Exception as e:
        print(f"❌ Error creating placeholder image: {e}")
        return None

def create_simple_placeholder_text(file_id, error_reason):
    """Create a simple text placeholder when PIL is not available."""
    placeholder_text = f"""
    IMAGE NOT FOUND
    
    File ID: {file_id}
    Reason: {error_reason}
    
    Install Pillow for better placeholders:
    pip install Pillow
    """
    return placeholder_text.encode('utf-8')

def serve_placeholder_image(file_id, error_reason="File not found"):
    """Serve a placeholder image as a Flask response."""
    try:
        if PIL_AVAILABLE:
            # Create placeholder image with PIL
            placeholder = create_placeholder_image(
                width=400, 
                height=300, 
                text="Missing Image",
                file_id=file_id
            )
            
            if placeholder:
                # Convert to bytes
                img_buffer = io.BytesIO()
                placeholder.save(img_buffer, format='JPEG', quality=85)
                img_buffer.seek(0)
                
                # Create response with proper headers
                response = Response(
                    img_buffer.getvalue(),
                    mimetype='image/jpeg',
                    headers={
                        'Cache-Control': 'no-cache, no-store, must-revalidate',
                        'Pragma': 'no-cache',
                        'Expires': '0',
                        'X-Error-Reason': error_reason,
                        'X-File-ID': str(file_id)
                    }
                )
                
                print(f"📷 Serving PIL placeholder for file {file_id}: {error_reason}")
                return response
        
        # Fallback to text placeholder
        placeholder_text = create_simple_placeholder_text(file_id, error_reason)
        
        response = Response(
            placeholder_text,
            mimetype='text/plain',
            headers={
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache', 
                'Expires': '0',
                'X-Error-Reason': error_reason,
                'X-File-ID': str(file_id)
            }
        )
        
        print(f"📄 Serving text placeholder for file {file_id}: {error_reason}")
        return response
        
    except Exception as e:
        print(f"❌ Error creating any placeholder: {e}")
        # Final fallback
        return Response(
            f"Image {file_id} not available: {error_reason}",
            mimetype='text/plain',
            status=404
        )

def serve_thumbnail_placeholder(file_id, error_reason="File not found"):
    """Serve a smaller placeholder for thumbnails."""
    try:
        if PIL_AVAILABLE:
            # Create smaller placeholder for thumbnails
            placeholder = create_placeholder_image(
                width=200, 
                height=150, 
                text="Missing",
                file_id=file_id
            )
            
            if placeholder:
                img_buffer = io.BytesIO()
                placeholder.save(img_buffer, format='JPEG', quality=80)
                img_buffer.seek(0)
                
                response = Response(
                    img_buffer.getvalue(),
                    mimetype='image/jpeg',
                    headers={
                        'Cache-Control': 'no-cache, no-store, must-revalidate',
                        'X-Error-Reason': error_reason,
                        'X-File-ID': str(file_id)
                    }
                )
                
                print(f"🖼️ Serving thumbnail placeholder for file {file_id}")
                return response
        
        # Text fallback for thumbnails
        response = Response(
            f"Thumbnail {file_id} missing",
            mimetype='text/plain',
            headers={'X-Error-Reason': error_reason}
        )
        
        return response
        
    except Exception as e:
        print(f"❌ Error creating thumbnail placeholder: {e}")
        return Response(f"Thumbnail {file_id} error", mimetype='text/plain', status=404)

# MAIN ROUTES
@app.route('/')
def dashboard():
    """Dashboard showing collection statistics with enhanced debugging."""
    try:
        print("📊 Dashboard route called")
        stats_data = cli.get_stats(detailed=True)
        print(f"📊 Stats data received: {type(stats_data)}")
        
        if 'error' in stats_data:
            print(f"❌ Stats error: {stats_data['error']}")
            
            # Create fallback stats for display
            fallback_stats = {
                "files": {"total": 0, "images": 0, "videos": 0, "large_files": 0},
                "groups": {"total": 0, "single_file": 0, "multi_file": 0, "largest_size": 0, "average_size": 0},
                "review_status": {"undecided": 0, "keep": 0, "not_needed": 0},
                "storage": {"total_bytes": 0, "total_gb": 0.0, "average_mb": 0.0},
                "drives": 0
            }
            
            error_msg = f"CLI Error: {stats_data.get('error', 'Unknown error')}"
            if 'debug_info' in stats_data:
                debug = stats_data['debug_info']
                error_msg += f"\nSTDOUT: {debug.get('stdout', 'None')}"
                error_msg += f"\nSTDERR: {debug.get('stderr', 'None')}"
            
            return render_template('dashboard.html', 
                                 stats=fallback_stats, 
                                 error_message=error_msg)
        
        # Check if we got the expected data structure
        if 'data' not in stats_data:
            print(f"❌ Unexpected stats data structure: {stats_data}")
            return render_template('error.html', 
                                 error=f"Unexpected data structure from CLI: {stats_data}"), 500
        
        print("✅ Rendering dashboard with stats")
        return render_template('dashboard.html', stats=stats_data['data'])
        
    except Exception as e:
        print(f"❌ Dashboard error: {e}")
        traceback.print_exc()
        return render_template('error.html', error=str(e)), 500

@app.route('/groups')
def view_groups():
    """Groups review page with image previews and pagination."""
    try:
        page = request.args.get('page', 1, type=int)
        status = request.args.get('status', 'undecided')
        
        print(f"📁 Groups route called: page={page}, status={status}")
        
        groups_data = cli.get_groups_data(page=page, per_page=20, status=status)
        
        if 'error' in groups_data:
            print(f"❌ Groups error: {groups_data['error']}")
            return render_template('error.html', error=groups_data['error']), 500
        
        print(f"✅ Groups data: {len(groups_data.get('groups', []))} groups")
        return render_template('groups.html', **groups_data)
        
    except Exception as e:
        print(f"❌ Groups error: {e}")
        traceback.print_exc()
        return render_template('error.html', error=str(e)), 500

@app.route('/singles')
def view_singles():
    """Individual images review page with pagination."""
    try:
        page = request.args.get('page', 1, type=int)
        status = request.args.get('status', 'undecided')
        
        print(f"🖼️ Singles route called: page={page}, status={status}")
        
        singles_data = cli.get_singles_data(page=page, per_page=50, status=status)
        
        if 'error' in singles_data:
            print(f"❌ Singles error: {singles_data['error']}")
            return render_template('error.html', error=singles_data['error']), 500
        
        print(f"✅ Singles data: {len(singles_data.get('files', []))} files")
        return render_template('singles.html', **singles_data)
        
    except Exception as e:
        print(f"❌ Singles error: {e}")
        traceback.print_exc()
        return render_template('error.html', error=str(e)), 500

@app.route('/bulk')
def bulk_actions():
    """Bulk actions page with enhanced preview functionality."""
    try:
        return render_template('bulk.html')
    except Exception as e:
        print(f"❌ Bulk actions error: {e}")
        traceback.print_exc()
        return render_template('error.html', error=str(e)), 500

@app.route('/export')
def export_page():
    """Export options page."""
    try:
        return render_template('export.html')
    except Exception as e:
        print(f"❌ Export page error: {e}")
        traceback.print_exc()
        return render_template('error.html', error=str(e)), 500

# IMAGE SERVING ROUTES
@app.route('/image/<int:file_id>')
def serve_image(file_id):
    """Serve full-size image file with placeholder fallback."""
    try:
        print(f"🖼️ Serving image: {file_id}")
        file_info = cli.get_file_path_info(file_id)
        
        if not file_info:
            print(f"❌ File {file_id} not found in database")
            return serve_placeholder_image(file_id, "File not in database")
        
        # Construct full path
        path_on_drive = file_info['path_on_drive']
        mount_path = file_info['mount_path']
        
        if mount_path and mount_path.strip():
            full_path = Path(mount_path) / path_on_drive
        else:
            full_path = Path(path_on_drive)
        
        print(f"🖼️ Full path: {full_path}")
        
        if not full_path.exists():
            print(f"⚠️ File not found on filesystem: {full_path}")
            return serve_placeholder_image(file_id, f"File missing: {full_path.name}")
        
        # Check if file is readable
        if not os.access(full_path, os.R_OK):
            print(f"⚠️ File not readable: {full_path}")
            return serve_placeholder_image(file_id, "File not readable")
        
        # Try to serve the actual file
        try:
            print(f"✅ Serving actual file: {full_path}")
            return send_file(str(full_path))
        except Exception as e:
            print(f"⚠️ Error sending file {full_path}: {e}")
            return serve_placeholder_image(file_id, f"File error: {str(e)[:50]}")
        
    except Exception as e:
        print(f"❌ Image serving error: {e}")
        return serve_placeholder_image(file_id, "Server error")

@app.route('/thumbnail/<int:file_id>')
def serve_thumbnail(file_id):
    """Serve thumbnail image with placeholder fallback."""
    try:
        print(f"🖼️ Serving thumbnail: {file_id}")
        file_info = cli.get_file_path_info(file_id)
        
        if not file_info:
            print(f"❌ File {file_id} not found in database")
            return serve_thumbnail_placeholder(file_id, "File not in database")
        
        # Construct full path
        path_on_drive = file_info['path_on_drive']
        mount_path = file_info['mount_path']
        
        if mount_path and mount_path.strip():
            full_path = Path(mount_path) / path_on_drive
        else:
            full_path = Path(path_on_drive)
        
        if not full_path.exists():
            print(f"⚠️ Thumbnail source not found: {full_path}")
            return serve_thumbnail_placeholder(file_id, "File missing")
        
        # For thumbnails, we should ideally generate actual thumbnails
        # For now, serve the full image (browser will resize)
        # TODO: Implement actual thumbnail generation
        try:
            return send_file(str(full_path))
        except Exception as e:
            print(f"⚠️ Error serving thumbnail: {e}")
            return serve_thumbnail_placeholder(file_id, str(e)[:50])
        
    except Exception as e:
        print(f"❌ Thumbnail serving error: {e}")
        return serve_thumbnail_placeholder(file_id, "Server error")

# API ENDPOINTS
@app.route('/api/stats')
def api_stats():
    """Get statistics via JSON CLI."""
    try:
        detailed = request.args.get('detailed', 'false').lower() == 'true'
        print(f"📊 API stats called: detailed={detailed}")
        
        result = cli.get_stats(detailed=detailed)
        return jsonify(result)
        
    except Exception as e:
        print(f"❌ API stats error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/review-queue')
def api_review_queue():
    """Get review queue via JSON CLI."""
    try:
        limit = request.args.get('limit', 50, type=int)
        print(f"📋 API review queue called: limit={limit}")
        
        result = cli.get_review_queue(limit=limit)
        return jsonify(result)
        
    except Exception as e:
        print(f"❌ API review queue error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/file-info/<int:file_id>')
def api_file_info(file_id):
    """Get detailed file information."""
    try:
        print(f"ℹ️ API file info called: {file_id}")
        result = cli.get_file_info(file_id)
        return jsonify(result)
        
    except Exception as e:
        print(f"❌ API file info error: {e}")
        traceback.print_exc()
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
        
        print(f"✏️ API mark file: {file_id} -> {status}")
        
        if not file_id or not status:
            return jsonify({'error': 'Missing file_id or status'}), 400
        
        if status not in ['keep', 'not_needed', 'undecided']:
            return jsonify({'error': 'Invalid status'}), 400
        
        result = cli.mark_file(file_id, status, note)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
        
    except Exception as e:
        print(f"❌ API mark file error: {e}")
        traceback.print_exc()
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
        
        print(f"🔍 API bulk mark preview: pattern='{pattern}', regex={regex}")
        
        result = cli.bulk_mark_preview(pattern, regex, limit, show_paths)
        
        if 'error' in result:
            return jsonify(result), 500
        
        return jsonify(result)
        
    except Exception as e:
        print(f"❌ API bulk mark preview error: {e}")
        traceback.print_exc()
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

# ERROR HANDLERS
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    print(f"❌ 404 error: {request.url}")
    return render_template('error.html', 
                         error="Page not found",
                         details="The requested page could not be found."), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    print(f"❌ 500 error: {error}")
    traceback.print_exc()
    return render_template('error.html',
                         error="Internal server error", 
                         details="An unexpected error occurred."), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Handle uncaught exceptions."""
    print(f"❌ Uncaught exception: {e}")
    traceback.print_exc()
    return render_template('error.html',
                         error="Application error",
                         details=str(e)), 500

# TEMPLATE FILTERS
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

# CONTEXT PROCESSORS
@app.context_processor
def inject_globals():
    """Inject global variables into all templates."""
    return {
        'app_version': '2.0.0',
        'app_name': 'Media Review Tool'
    }

# HEALTH AND DEBUG ENDPOINTS
@app.route('/health')
def health_check():
    """Health check endpoint for monitoring."""
    try:
        print("🏥 Health check called")
        
        # Test CLI connection
        success, stdout, stderr = cli.run_command('--help')
        
        health_data = {
            'status': 'healthy' if success else 'unhealthy',
            'cli_available': success,
            'database_accessible': Path(cli.db_path).exists(),
            'cli_path': cli.cli_path,
            'db_path': cli.db_path,
            'working_dir': os.getcwd(),
            'error': stderr if not success else None
        }
        
        status_code = 200 if success else 503
        return jsonify(health_data), status_code
        
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 503

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
            'cli_executable': os.access(cli.cli_path, os.X_OK) if Path(cli.cli_path).exists() else False,
            'working_directory': os.getcwd(),
            'python_path': os.environ.get('PYTHONPATH'),
            'pil_available': PIL_AVAILABLE,
            'environment': {
                'MEDIA_CLI': os.environ.get('MEDIA_CLI'),
                'MEDIA_DB_PATH': os.environ.get('MEDIA_DB_PATH')
            },
            'files_in_current_dir': [str(p) for p in Path('.').iterdir()],
            'template_dir_exists': Path('templates').exists(),
            'static_dir_exists': Path('static').exists()
        }
        
        # Test CLI command
        success, stdout, stderr = cli.run_command('--help')
        debug_data['cli_help_test'] = {
            'success': success,
            'stdout_preview': stdout[:200] if stdout else None,
            'stderr': stderr
        }
        
        return jsonify(debug_data)
        
    except Exception as e:
        print(f"❌ Debug info error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Check if PIL is available
    if not PIL_AVAILABLE:
        print("⚠️  PIL (Pillow) not available - placeholder images will be text-only")
        print("   Install with: pip install Pillow")
    
    # Validate setup before starting
    print("🔍 Validating setup...")
    
    # Check if CLI is accessible
    try:
        success, stdout, stderr = cli.run_command('--help')
        if success:
            print("✅ CLI help test successful")
        else:
            print(f"❌ CLI help test failed:")
            print(f"   STDERR: {stderr}")
            print(f"   STDOUT: {stdout}")
    except Exception as e:
        print(f"❌ CLI validation error: {e}")
    
    # Check template directory
    template_dir = Path('templates')
    if not template_dir.exists():
        print("⚠️  Templates directory not found - some pages may not work")
        print("Copy template files from artifacts to templates/ directory")
        # Create basic templates directory structure
        template_dir.mkdir(exist_ok=True)
    
    # Check static directory
    static_dir = Path('static')
    if not static_dir.exists():
        print("⚠️  Static directory not found - styling may not work")
        print("Copy CSS and JS files from artifacts to static/ directory")
        # Create basic static directory structure
        static_dir.mkdir(exist_ok=True)
        (static_dir / 'css').mkdir(exist_ok=True)
        (static_dir / 'js').mkdir(exist_ok=True)
    
    print("=" * 80)
    print("🚀 Enhanced Media Review Web Interface - JSON-Driven")
    print("=" * 80)
    print(f"📂 Database: {cli.db_path}")
    print(f"⚡ CLI Command: {cli.cli_path}")
    print(f"🌐 URL: http://localhost:5000")
    print(f"🔧 Debug URL: http://localhost:5000/debug/info")
    print(f"🏥 Health Check: http://localhost:5000/health")
    print("")
    print("✨ Features:")
    print("  📊 JSON-driven dashboard with real-time stats")
    print("  📁 Group review with image previews and promotion")
    print("  🖼️  Individual image review with keyboard shortcuts")
    print("  ⚡ Enhanced bulk operations with accurate preview")
    print("  🔍 Full-size image viewing with modal dialogs")
    print("  📥 Export functionality with CSV generation")
    print("  📱 Mobile-responsive design")
    print("  🖼️  Smart placeholder images for missing files")
    print("=" * 80)
    
    # Start the server
    app.run(debug=True, host='0.0.0.0', port=5000)