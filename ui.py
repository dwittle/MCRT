#!/usr/bin/env python3
"""
Flask Web Interface for Media Tool Review
Provides visual interface for reviewing grouped and individual images
"""

import os
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, send_file, abort
try:
    from flask import Markup
except ImportError:
    from markupsafe import Markup

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this'

# Configuration
DB_PATH = 'master_media.db'  # Path to your media tool database
IMAGES_PER_PAGE = 50
THUMBNAIL_SIZE = 250

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    return conn

def generate_groups_template(groups_with_files, page, total_groups, status_filter):
    """Generate groups template HTML"""
    # Filter options
    filter_options = {
        'undecided': 'selected' if status_filter == 'undecided' else '',
        'keep': 'selected' if status_filter == 'keep' else '',
        'not_needed': 'selected' if status_filter == 'not_needed' else '',
        'all': 'selected' if status_filter == 'all' else ''
    }
    
    # Build groups HTML
    groups_html = ""
    for item in groups_with_files:
        group = item['group']
        files = item['files']
        
        files_html = ""
        for file in files:
            original_badge = '<div class="original-badge">ORIGINAL</div>' if file['is_original'] else ''
            promote_btn = '' if file['is_original'] else f'<button class="btn btn-promote" onclick="promoteFile({file["file_id"]})">Make Original</button>'
            
            size_mb = file['size_bytes'] / (1024*1024) if file['size_bytes'] else 0
            megapixels = (file['width'] * file['height']) / 1000000 if file['width'] and file['height'] else 0
            
            files_html += f"""
            <div class="image-card" data-file-id="{file['file_id']}">
                <div class="image-container" onclick="showImage({file['file_id']})">
                    <img src="/thumbnail/{file['file_id']}" alt="Image {file['file_id']}" loading="lazy">
                    {original_badge}
                    <div class="status-badge status-{file['review_status']}">{file['review_status']}</div>
                </div>
                
                <div class="image-info">
                    <div class="title">{Path(file['path_on_drive']).name}</div>
                    <div class="image-meta">
                        <span>{file['width'] or '?'} × {file['height'] or '?'}</span>
                        <span>{size_mb:.1f} MB</span>
                        <span>{file['drive_label'] or 'Unknown Drive'}</span>
                        <span>{megapixels:.1f} MP</span>
                    </div>
                    
                    <div class="actions">
                        {promote_btn}
                        <button class="btn btn-keep" onclick="markFile({file['file_id']}, 'keep')">Keep</button>
                        <button class="btn btn-skip" onclick="markFile({file['file_id']}, 'not_needed')">Skip</button>
                    </div>
                </div>
            </div>
            """
        
        groups_html += f"""
        <div class="group-container" data-group-id="{group['group_id']}">
            <div class="group-header">
                <h3>Group {group['group_id']} ({len(files)} versions)</h3>
                <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem;">
                    <button class="btn btn-keep" onclick="markGroup({group['group_id']}, 'keep')">Keep Group</button>
                    <button class="btn btn-skip" onclick="markGroup({group['group_id']}, 'not_needed')">Skip Group</button>
                    <button class="btn btn-undo" onclick="markGroup({group['group_id']}, 'undecided')">Reset to Undecided</button>
                </div>
            </div>
            
            <div class="group-files">
                {files_html}
            </div>
        </div>
        """
    
    # Pagination
    prev_link = f'<a href="/groups?status={status_filter}&page={page-1}">&laquo; Previous</a>' if page > 1 else ''
    next_link = f'<a href="/groups?status={status_filter}&page={page+1}">Next &raquo;</a>' if total_groups > page * 10 else ''
    
    template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Media Review Tool - Groups</title>
        <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
        <div class="header">
            <div class="container">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/groups" class="active">Groups</a>
                    <a href="/singles">Singles</a>
                </nav>
            </div>
        </div>

        <div class="container">
            <div id="success-message" class="success-message"></div>
            <div id="error-message" class="error-message"></div>
            
            <h2>Image Groups - Duplicate Detection</h2>

            <div class="filters">
                <label>Filter by status:</label>
                <select onchange="window.location.href=`/groups?status=${{this.value}}&page=1`">
                    <option value="undecided" {filter_options['undecided']}>Undecided</option>
                    <option value="keep" {filter_options['keep']}>Keep</option>
                    <option value="not_needed" {filter_options['not_needed']}>Not Needed</option>
                    <option value="all" {filter_options['all']}>All</option>
                </select>
                <span style="margin-left: auto; color: var(--text-muted);">
                    Showing groups {(page-1)*10 + 1}-{min(page*10, total_groups)} of {total_groups:,}
                </span>
            </div>

            {groups_html}

            <div class="pagination">
                {prev_link}
                <span class="current">Page {page}</span>
                {next_link}
            </div>
        </div>
        
        <!-- Modal for full-size image viewing -->
        <div id="imageModal" class="modal">
            <span class="close" onclick="closeModal()">&times;</span>
            <div class="modal-content">
                <img id="modalImage" src="" alt="">
                <div class="modal-info">
                    <div id="modalInfo">Loading...</div>
                </div>
            </div>
        </div>

        <script src="/static/app.js"></script>
    </body>
    </html>
    """
    
    return Markup(template)

def generate_singles_template(files, page, total_files, status_filter, images_per_page):
    """Generate singles template HTML"""
    # Filter options
    filter_options = {
        'undecided': 'selected' if status_filter == 'undecided' else '',
        'keep': 'selected' if status_filter == 'keep' else '',
        'not_needed': 'selected' if status_filter == 'not_needed' else '',
        'all': 'selected' if status_filter == 'all' else ''
    }
    
    # Build image grid
    images_html = ""
    for file in files:
        size_mb = file['size_bytes'] / (1024*1024) if file['size_bytes'] else 0
        megapixels = (file['width'] * file['height']) / 1000000 if file['width'] and file['height'] else 0
        
        images_html += f"""
        <div class="image-card" data-file-id="{file['file_id']}">
            <div class="image-container" onclick="showImage({file['file_id']})">
                <img src="/thumbnail/{file['file_id']}" alt="Image {file['file_id']}" loading="lazy">
                <div class="status-badge status-{file['review_status']}">{file['review_status']}</div>
            </div>
            
            <div class="image-info">
                <div class="title">{Path(file['path_on_drive']).name}</div>
                <div class="image-meta">
                    <span>{file['width'] or '?'} × {file['height'] or '?'}</span>
                    <span>{size_mb:.1f} MB</span>
                    <span>{file['drive_label'] or 'Unknown'}</span>
                    <span>{megapixels:.1f} MP</span>
                </div>
                
                <div class="actions">
                    <button class="btn btn-keep" onclick="markFile({file['file_id']}, 'keep')">Keep</button>
                    <button class="btn btn-skip" onclick="markFile({file['file_id']}, 'not_needed')">Skip</button>
                    <button class="btn btn-undo" onclick="markFile({file['file_id']}, 'undecided')">Undo</button>
                </div>
            </div>
        </div>
        """
    
    # Pagination
    total_pages = (total_files + images_per_page - 1) // images_per_page
    prev_link = f'<a href="/singles?status={status_filter}&page={page-1}">&laquo; Previous</a>' if page > 1 else ''
    next_link = f'<a href="/singles?status={status_filter}&page={page+1}">Next &raquo;</a>' if page < total_pages else ''
    
    # Page numbers
    page_links = ""
    for p in range(max(1, page-2), min(page+3, total_pages + 1)):
        if p == page:
            page_links += f'<span class="current">{p}</span>'
        else:
            page_links += f'<a href="/singles?status={status_filter}&page={p}">{p}</a>'
    
    template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Media Review Tool - Singles</title>
        <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
        <div class="header">
            <div class="container">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/groups">Groups</a>
                    <a href="/singles" class="active">Singles</a>
                </nav>
            </div>
        </div>

        <div class="container">
            <div id="success-message" class="success-message"></div>
            <div id="error-message" class="error-message"></div>
            
            <h2>Individual Images</h2>

            <div class="filters">
                <label>Filter by status:</label>
                <select onchange="window.location.href=`/singles?status=${{this.value}}&page=1`">
                    <option value="undecided" {filter_options['undecided']}>Undecided</option>
                    <option value="keep" {filter_options['keep']}>Keep</option>
                    <option value="not_needed" {filter_options['not_needed']}>Not Needed</option>
                    <option value="all" {filter_options['all']}>All</option>
                </select>
                <span style="margin-left: auto; color: var(--text-muted);">
                    Showing {(page-1)*images_per_page + 1}-{min(page*images_per_page, total_files)} of {total_files:,}
                </span>
            </div>

            <div class="image-grid">
                {images_html}
            </div>

            <div class="pagination">
                {prev_link}
                {page_links}
                {next_link}
            </div>
        </div>
        
        <!-- Modal for full-size image viewing -->
        <div id="imageModal" class="modal">
            <span class="close" onclick="closeModal()">&times;</span>
            <div class="modal-content">
                <img id="modalImage" src="" alt="">
                <div class="modal-info">
                    <div id="modalInfo">Loading...</div>
                </div>
            </div>
        </div>

        <div class="keyboard-help">
            <strong>Keyboard Shortcuts:</strong><br>
            K = Keep, N = Not needed, U = Undo<br>
            ←/→ or H/J = Navigate<br>
            Space = View full size, Esc = Close
        </div>

        <script src="/static/app.js"></script>
    </body>
    </html>
    """
    
    return Markup(template)

@app.route('/')
def dashboard():
    """Main dashboard showing statistics"""
    conn = get_db_connection()
    
    # Get overall statistics
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_files,
            COUNT(CASE WHEN review_status = 'keep' THEN 1 END) as keep_count,
            COUNT(CASE WHEN review_status = 'not_needed' THEN 1 END) as not_needed_count,
            COUNT(CASE WHEN review_status = 'undecided' THEN 1 END) as undecided_count,
            COUNT(CASE WHEN duplicate_of IS NULL THEN 1 END) as originals,
            COUNT(CASE WHEN duplicate_of IS NOT NULL THEN 1 END) as duplicates,
            COUNT(DISTINCT group_id) as total_groups
        FROM files
    """).fetchone()
    
    # Get group statistics
    group_stats = conn.execute("""
        SELECT 
            COUNT(CASE WHEN file_count = 1 THEN 1 END) as single_file_groups,
            COUNT(CASE WHEN file_count > 1 THEN 1 END) as multi_file_groups,
            MAX(file_count) as largest_group_size
        FROM (
            SELECT group_id, COUNT(*) as file_count
            FROM files
            WHERE group_id IS NOT NULL
            GROUP BY group_id
        )
    """).fetchone()
    
    conn.close()
    
    template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Media Review Tool - Dashboard</title>
        <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
        <div class="header">
            <div class="container">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/" class="active">Dashboard</a>
                    <a href="/groups">Groups</a>
                    <a href="/singles">Singles</a>
                </nav>
            </div>
        </div>

        <div class="container">
            <div id="success-message" class="success-message"></div>
            
            <h2>Collection Overview</h2>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="number">{stats['total_files']:,}</div>
                    <div class="label">Total Files</div>
                </div>
                <div class="stat-card">
                    <div class="number">{stats['total_groups']:,}</div>
                    <div class="label">Groups</div>
                </div>
                <div class="stat-card">
                    <div class="number">{stats['keep_count']:,}</div>
                    <div class="label">Marked Keep</div>
                </div>
                <div class="stat-card">
                    <div class="number">{stats['not_needed_count']:,}</div>
                    <div class="label">Not Needed</div>
                </div>
                <div class="stat-card">
                    <div class="number">{stats['undecided_count']:,}</div>
                    <div class="label">Need Review</div>
                </div>
                <div class="stat-card">
                    <div class="number">{stats['duplicates']:,}</div>
                    <div class="label">Duplicates Found</div>
                </div>
            </div>

            <div class="stats-grid">
                <div class="stat-card">
                    <div class="number">{group_stats['multi_file_groups']:,}</div>
                    <div class="label">Groups with Duplicates</div>
                </div>
                <div class="stat-card">
                    <div class="number">{group_stats['single_file_groups']:,}</div>
                    <div class="label">Unique Images</div>
                </div>
                <div class="stat-card">
                    <div class="number">{group_stats['largest_group_size'] or 0}</div>
                    <div class="label">Largest Group Size</div>
                </div>
            </div>

            <h3>Quick Actions</h3>
            <div style="display: flex; gap: 1rem; margin-top: 1rem;">
                <a href="/groups?status=undecided" class="btn btn-keep">Review Duplicate Groups ({group_stats['multi_file_groups']:,})</a>
                <a href="/singles?status=undecided" class="btn btn-keep">Review Individual Images ({group_stats['single_file_groups']:,})</a>
            </div>
            
            <div style="margin-top: 2rem; padding: 1rem; background: var(--surface); border-radius: 8px;">
                <h4>Review Progress</h4>
                <div style="margin-top: 1rem;">
                    <div>✅ Kept: {stats['keep_count']:,} files</div>
                    <div>❌ Not needed: {stats['not_needed_count']:,} files</div>
                    <div>⏳ Undecided: {stats['undecided_count']:,} files</div>
                </div>
            </div>
        </div>
        
        <script src="/static/app.js"></script>
    </body>
    </html>
    """
    
    return Markup(template)

@app.route('/groups')
def view_groups():
    """View groups with multiple files for review"""
    page = request.args.get('page', 1, type=int)
    status_filter = request.args.get('status', 'undecided')
    
    conn = get_db_connection()
    
    # Get groups with multiple files that need review
    offset = (page - 1) * 10
    
    # Build status filter
    status_condition = "AND f.review_status = ?" if status_filter != 'all' else ""
    status_params = [status_filter] if status_filter != 'all' else []
    
    groups_query = f"""
        SELECT 
            g.group_id,
            g.original_file_id,
            COUNT(f.file_id) as file_count,
            GROUP_CONCAT(f.review_status) as all_statuses
        FROM groups g
        JOIN files f ON f.group_id = g.group_id
        WHERE f.type = 'image'
        {status_condition}
        GROUP BY g.group_id
        HAVING COUNT(f.file_id) > 1
        ORDER BY file_count DESC, g.group_id
        LIMIT 10 OFFSET ?
    """
    
    groups = conn.execute(groups_query, status_params + [offset]).fetchall()
    
    # Get files for each group
    groups_with_files = []
    for group in groups:
        files = conn.execute("""
            SELECT 
                f.file_id,
                f.path_on_drive,
                f.width,
                f.height,
                f.size_bytes,
                f.review_status,
                f.duplicate_of,
                d.label as drive_label,
                (f.file_id = g.original_file_id) as is_original
            FROM files f
            JOIN groups g ON g.group_id = f.group_id
            LEFT JOIN drives d ON d.drive_id = f.drive_id
            WHERE f.group_id = ?
            ORDER BY is_original DESC, f.width * f.height DESC
        """, [group['group_id']]).fetchall()
        
        groups_with_files.append({
            'group': group,
            'files': files
        })
    
    # Get total count for pagination
    count_query = f"""
        SELECT COUNT(DISTINCT g.group_id)
        FROM groups g
        JOIN files f ON f.group_id = g.group_id
        WHERE f.type = 'image'
        {status_condition}
        GROUP BY g.group_id
        HAVING COUNT(f.file_id) > 1
    """
    
    total_groups = len(conn.execute(count_query, status_params).fetchall())
    
    conn.close()
    
    return generate_groups_template(groups_with_files, page, total_groups, status_filter)

@app.route('/singles')
def view_singles():
    """View individual images (single-file groups) for review"""
    page = request.args.get('page', 1, type=int)
    status_filter = request.args.get('status', 'undecided')
    
    conn = get_db_connection()
    
    offset = (page - 1) * IMAGES_PER_PAGE
    
    # Build status filter
    status_condition = "AND f.review_status = ?" if status_filter != 'all' else ""
    status_params = [status_filter] if status_filter != 'all' else []
    
    files_query = f"""
        SELECT 
            f.file_id,
            f.path_on_drive,
            f.width,
            f.height,
            f.size_bytes,
            f.review_status,
            f.group_id,
            d.label as drive_label
        FROM files f
        LEFT JOIN drives d ON d.drive_id = f.drive_id
        WHERE f.type = 'image'
        AND f.group_id IN (
            SELECT group_id 
            FROM files 
            GROUP BY group_id 
            HAVING COUNT(*) = 1
        )
        {status_condition}
        ORDER BY f.file_id
        LIMIT ? OFFSET ?
    """
    
    files = conn.execute(files_query, status_params + [IMAGES_PER_PAGE, offset]).fetchall()
    
    # Get total count
    count_query = f"""
        SELECT COUNT(f.file_id)
        FROM files f
        WHERE f.type = 'image'
        AND f.group_id IN (
            SELECT group_id 
            FROM files 
            GROUP BY group_id 
            HAVING COUNT(*) = 1
        )
        {status_condition}
    """
    
    total_files = conn.execute(count_query, status_params).fetchone()[0]
    
    conn.close()
    
    return generate_singles_template(files, page, total_files, status_filter, IMAGES_PER_PAGE)

@app.route('/image/<int:file_id>')
def serve_image(file_id):
    """Serve image file"""
    conn = get_db_connection()
    file_info = conn.execute("""
        SELECT f.path_on_drive, d.mount_path 
        FROM files f
        LEFT JOIN drives d ON d.drive_id = f.drive_id
        WHERE f.file_id = ?
    """, [file_id]).fetchone()
    conn.close()
    
    if not file_info:
        abort(404)
    
    # Construct full path
    if file_info['mount_path']:
        full_path = Path(file_info['mount_path']) / file_info['path_on_drive']
    else:
        full_path = Path(file_info['path_on_drive'])
    
    if not full_path.exists():
        abort(404)
    
    return send_file(str(full_path))

@app.route('/thumbnail/<int:file_id>')
def serve_thumbnail(file_id):
    """Serve thumbnail version of image"""
    # For now, just serve the full image - you could implement thumbnail generation here
    return serve_image(file_id)

@app.route('/api/mark_file', methods=['POST'])
def mark_file():
    """Mark a file with review status"""
    data = request.get_json()
    file_id = data.get('file_id')
    status = data.get('status')
    note = data.get('note', '')
    
    if status not in ['keep', 'not_needed', 'undecided']:
        return jsonify({'error': 'Invalid status'}), 400
    
    conn = get_db_connection()
    conn.execute("""
        UPDATE files 
        SET review_status = ?, reviewed_at = ?, review_note = ?
        WHERE file_id = ?
    """, [status, datetime.now().isoformat() + 'Z', note, file_id])
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@app.route('/api/mark_group', methods=['POST'])
def mark_group():
    """Mark entire group with review status"""
    data = request.get_json()
    group_id = data.get('group_id')
    status = data.get('status')
    note = data.get('note', '')
    
    if status not in ['keep', 'not_needed', 'undecided']:
        return jsonify({'error': 'Invalid status'}), 400
    
    conn = get_db_connection()
    cursor = conn.execute("""
        UPDATE files 
        SET review_status = ?, reviewed_at = ?, review_note = ?
        WHERE group_id = ?
    """, [status, datetime.now().isoformat() + 'Z', note, group_id])
    
    conn.commit()
    rows_affected = cursor.rowcount
    conn.close()
    
    return jsonify({'success': True, 'files_updated': rows_affected})

@app.route('/api/promote_file', methods=['POST'])
def promote_file():
    """Promote a file to be the group original"""
    data = request.get_json()
    file_id = data.get('file_id')
    
    conn = get_db_connection()
    
    # Get file's group
    file_info = conn.execute("SELECT group_id FROM files WHERE file_id = ?", [file_id]).fetchone()
    if not file_info:
        conn.close()
        return jsonify({'error': 'File not found'}), 404
    
    group_id = file_info['group_id']
    
    # Get current original
    current_orig = conn.execute("""
        SELECT original_file_id FROM groups WHERE group_id = ?
    """, [group_id]).fetchone()
    
    old_original_id = current_orig['original_file_id'] if current_orig else None
    
    # Update group original
    conn.execute("UPDATE groups SET original_file_id = ? WHERE group_id = ?", [file_id, group_id])
    
    # Update duplicate relationships
    conn.execute("UPDATE files SET duplicate_of = NULL WHERE file_id = ?", [file_id])
    
    if old_original_id:
        conn.execute("UPDATE files SET duplicate_of = ? WHERE file_id = ?", [file_id, old_original_id])
    
    conn.execute("""
        UPDATE files SET duplicate_of = ? 
        WHERE group_id = ? AND file_id != ?
    """, [file_id, group_id, file_id])
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@app.route('/api/file_info/<int:file_id>')
def get_file_info(file_id):
    """Get detailed file information"""
    conn = get_db_connection()
    file_info = conn.execute("""
        SELECT 
            f.*,
            d.label as drive_label,
            d.mount_path,
            g.original_file_id,
            (f.file_id = g.original_file_id) as is_original
        FROM files f
        LEFT JOIN drives d ON d.drive_id = f.drive_id
        LEFT JOIN groups g ON g.group_id = f.group_id
        WHERE f.file_id = ?
    """, [file_id]).fetchone()
    conn.close()
    
    if not file_info:
        return jsonify({'error': 'File not found'}), 404
    
    # Convert to dict and add computed fields
    result = dict(file_info)
    if result['width'] and result['height']:
        result['pixels'] = result['width'] * result['height']
        result['megapixels'] = round(result['pixels'] / 1000000, 1)
        result['aspect_ratio'] = round(result['width'] / result['height'], 2)
    
    if result['size_bytes']:
        result['size_mb'] = round(result['size_bytes'] / (1024 * 1024), 1)
    
    return jsonify(result)

# Add route for serving static CSS and JS
@app.route('/static/style.css')
def static_css():
    css = """
        :root {
            --primary: #2563eb;
            --success: #059669;
            --warning: #d97706;
            --danger: #dc2626;
            --background: #f8fafc;
            --surface: #ffffff;
            --text: #1e293b;
            --text-muted: #64748b;
            --border: #e2e8f0;
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
            background: var(--background);
            color: var(--text);
            line-height: 1.6;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 1rem 0;
            margin-bottom: 2rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        
        .nav {
            display: flex;
            gap: 2rem;
            align-items: center;
        }
        
        .nav h1 {
            color: var(--primary);
            font-size: 1.5rem;
        }
        
        .nav a {
            color: var(--text);
            text-decoration: none;
            padding: 0.5rem 1rem;
            border-radius: 6px;
            transition: background 0.2s;
        }
        
        .nav a:hover, .nav a.active {
            background: var(--primary);
            color: white;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }
        
        .stat-card {
            background: var(--surface);
            padding: 1.5rem;
            border-radius: 8px;
            border: 1px solid var(--border);
            text-align: center;
        }
        
        .stat-card .number {
            font-size: 2rem;
            font-weight: bold;
            color: var(--primary);
        }
        
        .stat-card .label {
            color: var(--text-muted);
            margin-top: 0.5rem;
        }
        
        .image-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }
        
        .image-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            overflow: hidden;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .image-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        
        .image-card.highlighted {
            border: 3px solid var(--primary);
            transform: scale(1.02);
        }
        
        .image-container {
            position: relative;
            width: 100%;
            height: 250px;
            overflow: hidden;
            background: #f1f5f9;
            cursor: pointer;
        }
        
        .image-container img {
            width: 100%;
            height: 100%;
            object-fit: cover;
            transition: transform 0.3s;
        }
        
        .image-container:hover img {
            transform: scale(1.05);
        }
        
        .original-badge {
            position: absolute;
            top: 8px;
            left: 8px;
            background: var(--success);
            color: white;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: bold;
        }
        
        .status-badge {
            position: absolute;
            top: 8px;
            right: 8px;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: bold;
        }
        
        .status-keep { background: var(--success); color: white; }
        .status-not_needed { background: var(--danger); color: white; }
        .status-undecided { background: var(--warning); color: white; }
        
        .image-info {
            padding: 1rem;
        }
        
        .image-info .title {
            font-weight: 600;
            margin-bottom: 0.5rem;
            font-size: 0.9rem;
            word-break: break-all;
        }
        
        .image-meta {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.5rem;
            font-size: 0.8rem;
            color: var(--text-muted);
            margin-bottom: 1rem;
        }
        
        .actions {
            display: flex;
            gap: 0.5rem;
        }
        
        .btn {
            padding: 0.5rem 1rem;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.8rem;
            font-weight: 500;
            transition: background 0.2s;
            flex: 1;
            text-decoration: none;
            text-align: center;
            display: inline-block;
        }
        
        .btn-keep { background: var(--success); color: white; }
        .btn-keep:hover { background: #047857; }
        
        .btn-skip { background: var(--danger); color: white; }
        .btn-skip:hover { background: #b91c1c; }
        
        .btn-undo { background: var(--text-muted); color: white; }
        .btn-undo:hover { background: #475569; }
        
        .btn-promote { background: var(--primary); color: white; }
        .btn-promote:hover { background: #1d4ed8; }
        
        .group-container {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            margin-bottom: 2rem;
            overflow: hidden;
        }
        
        .group-header {
            background: #f1f5f9;
            padding: 1rem;
            border-bottom: 1px solid var(--border);
        }
        
        .group-files {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            padding: 1rem;
        }
        
        .pagination {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 1rem;
            margin: 2rem 0;
        }
        
        .pagination a, .pagination .current {
            padding: 0.5rem 1rem;
            border: 1px solid var(--border);
            border-radius: 4px;
            text-decoration: none;
            color: var(--text);
        }
        
        .pagination .current {
            background: var(--primary);
            color: white;
        }
        
        .filters {
            background: var(--surface);
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 2rem;
            display: flex;
            gap: 1rem;
            align-items: center;
        }
        
        .filters select {
            padding: 0.5rem;
            border: 1px solid var(--border);
            border-radius: 4px;
        }
        
        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.9);
        }
        
        .modal-content {
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            height: 100%;
            padding: 2rem;
        }
        
        .modal img {
            max-width: 90%;
            max-height: 80%;
            object-fit: contain;
        }
        
        .modal-info {
            background: var(--surface);
            padding: 1rem;
            border-radius: 8px;
            margin-top: 1rem;
            text-align: center;
        }
        
        .close {
            position: absolute;
            top: 2rem;
            right: 2rem;
            color: white;
            font-size: 2rem;
            cursor: pointer;
        }
        
        .success-message, .error-message {
            padding: 0.75rem;
            border-radius: 4px;
            margin-bottom: 1rem;
            display: none;
        }
        
        .success-message {
            background: #dcfce7;
            color: #166534;
        }
        
        .error-message {
            background: #fef2f2;
            color: #991b1b;
        }
        
        .keyboard-help {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: var(--surface);
            padding: 1rem;
            border-radius: 8px;
            border: 1px solid var(--border);
            font-size: 0.8rem;
            color: var(--text-muted);
        }
    """
    return css, 200, {'Content-Type': 'text/css'}

@app.route('/static/app.js')
def static_js():
    js = """
        function showImage(fileId) {
            const modal = document.getElementById('imageModal');
            const img = document.getElementById('modalImage');
            const info = document.getElementById('modalInfo');
            
            img.src = `/image/${fileId}`;
            modal.style.display = 'block';
            
            fetch(`/api/file_info/${fileId}`)
                .then(response => response.json())
                .then(data => {
                    info.innerHTML = `
                        <h3>${data.path_on_drive.split('/').pop()}</h3>
                        <p><strong>Dimensions:</strong> ${data.width} × ${data.height} (${data.megapixels || 'Unknown'} MP)</p>
                        <p><strong>Size:</strong> ${data.size_mb || 'Unknown'} MB</p>
                        <p><strong>Drive:</strong> ${data.drive_label || 'Unknown'}</p>
                        <p><strong>Status:</strong> <span class="status-badge status-${data.review_status}">${data.review_status}</span></p>
                        ${data.is_original ? '<p><strong>Group Original</strong></p>' : ''}
                    `;
                })
                .catch(err => {
                    info.innerHTML = '<p>Error loading file info</p>';
                });
        }
        
        function closeModal() {
            document.getElementById('imageModal').style.display = 'none';
        }
        
        window.onclick = function(event) {
            const modal = document.getElementById('imageModal');
            if (event.target == modal) {
                closeModal();
            }
        }
        
        function markFile(fileId, status, note = '') {
            fetch('/api/mark_file', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, status: status, note: note })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showMessage(`File marked as ${status}`, 'success');
                    updateFileStatus(fileId, status);
                } else {
                    showMessage(`Error: ${data.error}`, 'error');
                }
            })
            .catch(error => showMessage(`Network error: ${error}`, 'error'));
        }
        
        function markGroup(groupId, status, note = '') {
            fetch('/api/mark_group', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ group_id: groupId, status: status, note: note })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showMessage(`Group marked as ${status} (${data.files_updated} files)`, 'success');
                    document.querySelectorAll(`[data-group-id="${groupId}"] .status-badge`).forEach(badge => {
                        badge.className = `status-badge status-${status}`;
                        badge.textContent = status;
                    });
                } else {
                    showMessage(`Error: ${data.error}`, 'error');
                }
            })
            .catch(error => showMessage(`Network error: ${error}`, 'error'));
        }
        
        function promoteFile(fileId) {
            fetch('/api/promote_file', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showMessage('File promoted to group original', 'success');
                    setTimeout(() => location.reload(), 1000);
                } else {
                    showMessage(`Error: ${data.error}`, 'error');
                }
            })
            .catch(error => showMessage(`Network error: ${error}`, 'error'));
        }
        
        function updateFileStatus(fileId, status) {
            const badge = document.querySelector(`[data-file-id="${fileId}"] .status-badge`);
            if (badge) {
                badge.className = `status-badge status-${status}`;
                badge.textContent = status;
            }
        }
        
        function showMessage(message, type) {
            const element = document.getElementById(`${type}-message`);
            if (element) {
                element.textContent = message;
                element.style.display = 'block';
                setTimeout(() => element.style.display = 'none', 3000);
            }
        }
        
        // Keyboard shortcuts for singles page
        if (window.location.pathname === '/singles') {
            let currentImageIndex = 0;
            const images = document.querySelectorAll('.image-card');
            
            document.addEventListener('keydown', function(e) {
                if (document.getElementById('imageModal') && document.getElementById('imageModal').style.display === 'block') return;
                
                const currentCard = images[currentImageIndex];
                if (!currentCard) return;
                
                const fileId = currentCard.getAttribute('data-file-id');
                
                switch(e.key) {
                    case 'k':
                        markFile(parseInt(fileId), 'keep');
                        nextImage();
                        break;
                    case 'n':
                        markFile(parseInt(fileId), 'not_needed');
                        nextImage();
                        break;
                    case 'u':
                        markFile(parseInt(fileId), 'undecided');
                        break;
                    case 'ArrowRight':
                    case 'j':
                        nextImage();
                        break;
                    case 'ArrowLeft':
                    case 'h':
                        prevImage();
                        break;
                    case ' ':
                        e.preventDefault();
                        showImage(parseInt(fileId));
                        break;
                    case 'Escape':
                        closeModal();
                        break;
                }
            });
            
            function nextImage() {
                if (currentImageIndex < images.length - 1) {
                    highlightImage(currentImageIndex + 1);
                }
            }
            
            function prevImage() {
                if (currentImageIndex > 0) {
                    highlightImage(currentImageIndex - 1);
                }
            }
            
            function highlightImage(index) {
                images[currentImageIndex]?.classList.remove('highlighted');
                currentImageIndex = index;
                const card = images[currentImageIndex];
                
                if (card) {
                    card.classList.add('highlighted');
                    card.scrollIntoView({ behavior: 'smooth', block: 'center' });
                }
            }
            
            if (images.length > 0) {
                highlightImage(0);
            }
        }
    """
    return js, 200, {'Content-Type': 'application/javascript'}

if __name__ == '__main__':
    # Check if database exists
    if not Path(DB_PATH).exists():
        print(f"Error: Database {DB_PATH} not found!")
        print("Please run media_tool.py scan first to create the database.")
        exit(1)
    
    print("=" * 60)
    print("🚀 Media Review Web Interface Starting")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"URL: http://localhost:5000")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)