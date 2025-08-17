#!/usr/bin/env python3
"""
Enhanced CLI-driven web interface with full image viewing capabilities.
Includes image previews, full-size viewing, and group visualization.
"""

import json
import subprocess
import os
from pathlib import Path
from flask import Flask, request, jsonify, send_file, render_template_string, abort

app = Flask(__name__)
app.secret_key = 'media-tool-ui-secret'

# Configuration
DB_PATH = os.environ.get('MEDIA_DB_PATH', 'media_index.db')
CLI_COMMAND = os.environ.get('MEDIA_CLI', './media_tool_cli.py')

class MediaToolCLI:
    """Enhanced interface to the media tool CLI with group support."""
    
    def __init__(self, cli_path=CLI_COMMAND, db_path=DB_PATH):
        self.cli_path = cli_path
        self.db_path = db_path
    
    def run_command(self, *args, timeout=30):
        """Run CLI command and return (success, stdout, stderr)."""
        cmd = [self.cli_path, '--db', self.db_path] + list(str(arg) for arg in args)
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return False, "", "Command timed out"
        except Exception as e:
            return False, "", str(e)
    
    def get_stats(self):
        """Get database statistics."""
        success, stdout, stderr = self.run_command('stats', '--detailed')
        if not success:
            return {'error': stderr}
        
        # Parse CLI output
        stats = {'total_files': 0, 'total_groups': 0, 'undecided_count': 0, 'keep_count': 0, 'not_needed_count': 0}
        
        for line in stdout.split('\n'):
            line = line.strip()
            if line.startswith('Files:'):
                stats['total_files'] = int(line.split(':')[1].strip().replace(',', ''))
            elif line.startswith('Groups:'):
                stats['total_groups'] = int(line.split(':')[1].strip().replace(',', ''))
            elif line.strip().startswith('undecided:'):
                stats['undecided_count'] = int(line.split(':')[1].strip().replace(',', ''))
            elif line.strip().startswith('keep:'):
                stats['keep_count'] = int(line.split(':')[1].strip().replace(',', ''))
            elif line.strip().startswith('not_needed:'):
                stats['not_needed_count'] = int(line.split(':')[1].strip().replace(',', ''))
        
        return stats
    
    def get_groups_data(self, limit=20):
        """Get groups data by querying database directly (since CLI doesn't have groups command yet)."""
        import sqlite3
        
        groups_data = []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Get groups with multiple files
                groups = conn.execute("""
                    SELECT 
                        g.group_id,
                        g.original_file_id,
                        COUNT(f.file_id) as file_count
                    FROM groups g
                    JOIN files f ON f.group_id = g.group_id
                    WHERE f.type = 'image'
                    GROUP BY g.group_id
                    HAVING COUNT(f.file_id) > 1
                    ORDER BY file_count DESC, g.group_id
                    LIMIT ?
                """, [limit]).fetchall()
                
                for group in groups:
                    # Get files for this group
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
                        ORDER BY is_original DESC, (f.width * f.height) DESC
                    """, [group['group_id']]).fetchall()
                    
                    groups_data.append({
                        'group': dict(group),
                        'files': [dict(f) for f in files]
                    })
        
        except Exception as e:
            print(f"Error getting groups data: {e}")
            return []
        
        return groups_data
    
    def get_file_path_info(self, file_id):
        """Get file path for serving images."""
        import sqlite3
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                file_info = conn.execute("""
                    SELECT f.path_on_drive, d.mount_path 
                    FROM files f
                    LEFT JOIN drives d ON d.drive_id = f.drive_id
                    WHERE f.file_id = ?
                """, [file_id]).fetchone()
                
                if file_info:
                    return dict(file_info)
        except Exception as e:
            print(f"Error getting file path: {e}")
        
        return None
    
    # CLI command wrappers (same as before)
    def mark_file(self, file_id, status, note=''):
        """Mark file status."""
        args = ['mark', '--file-id', file_id, '--status', status]
        if note:
            args.extend(['--note', note])
        
        success, stdout, stderr = self.run_command(*args)
        return success, stdout if success else stderr
    
    def bulk_mark(self, path_pattern, status):
        """Bulk mark files."""
        success, stdout, stderr = self.run_command('bulk-mark', '--path-like', path_pattern, '--status', status)
        return success, stdout if success else stderr
    
    def promote_file(self, file_id):
        """Promote file to group original."""
        success, stdout, stderr = self.run_command('promote', '--file-id', file_id)
        return success, stdout if success else stderr

# Initialize CLI interface
cli = MediaToolCLI()

@app.route('/')
def dashboard():
    """Enhanced dashboard with better statistics."""
    stats = cli.get_stats()
    
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review Tool - Dashboard</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            :root {
                --primary: #1e40af;
                --success: #10b981;
                --warning: #f59e0b;
                --danger: #ef4444;
                --background: #f1f5f9;
                --surface: #ffffff;
                --text: #1f2937;
                --text-muted: #6b7280;
                --border: #e5e7eb;
            }
            
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
                background: var(--background);
                color: var(--text);
                line-height: 1.6;
            }
            
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            
            .header {
                background: var(--surface);
                padding: 24px;
                border-radius: 12px;
                margin-bottom: 24px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            
            .nav {
                display: flex;
                gap: 24px;
                align-items: center;
                flex-wrap: wrap;
            }
            
            .nav h1 {
                color: var(--primary);
                font-size: 1.75rem;
                margin: 0;
            }
            
            .nav a {
                color: var(--text);
                text-decoration: none;
                padding: 10px 16px;
                border-radius: 8px;
                font-weight: 500;
                transition: all 0.2s;
            }
            
            .nav a:hover, .nav a.active {
                background: var(--primary);
                color: white;
            }
            
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 20px;
                margin-bottom: 32px;
            }
            
            .stat-card {
                background: var(--surface);
                padding: 24px;
                border-radius: 12px;
                text-align: center;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                border: 1px solid var(--border);
                transition: transform 0.2s;
            }
            
            .stat-card:hover { transform: translateY(-2px); }
            
            .stat-card .number {
                font-size: 2.5rem;
                font-weight: bold;
                color: var(--primary);
                margin-bottom: 8px;
            }
            
            .stat-card .label {
                color: var(--text-muted);
                font-size: 0.9rem;
                font-weight: 500;
            }
            
            .actions {
                display: flex;
                gap: 16px;
                flex-wrap: wrap;
                margin-bottom: 24px;
            }
            
            .btn {
                padding: 12px 24px;
                background: var(--primary);
                color: white;
                text-decoration: none;
                border-radius: 8px;
                font-weight: 500;
                display: inline-flex;
                align-items: center;
                gap: 8px;
                border: none;
                cursor: pointer;
                transition: background 0.2s;
            }
            
            .btn:hover { background: #1e3a8a; }
            .btn-secondary { background: var(--text-muted); }
            .btn-secondary:hover { background: #4b5563; }
            
            .info-box {
                background: var(--surface);
                padding: 24px;
                border-radius: 12px;
                margin-top: 24px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                border: 1px solid var(--border);
            }
            
            .progress-bar {
                background: var(--border);
                height: 10px;
                border-radius: 5px;
                overflow: hidden;
                margin-top: 12px;
            }
            
            .progress-fill {
                background: var(--success);
                height: 100%;
                transition: width 0.3s ease;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/" class="active">Dashboard</a>
                    <a href="/groups">📁 Review Groups</a>
                    <a href="/singles">🖼️ Review Singles</a>
                    <a href="/bulk">⚡ Bulk Actions</a>
                    <a href="/export">📥 Export</a>
                </nav>
            </div>

            {% if stats.error %}
            <div style="background: #fef2f2; color: #991b1b; padding: 16px; border-radius: 8px; border: 1px solid #fecaca;">
                <strong>❌ Error:</strong> {{ stats.error }}
            </div>
            {% else %}
            
            <h2>Collection Overview</h2>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="number">{{ "{:,}".format(stats.total_files) }}</div>
                    <div class="label">Total Files</div>
                </div>
                <div class="stat-card">
                    <div class="number">{{ "{:,}".format(stats.total_groups) }}</div>
                    <div class="label">Groups</div>
                </div>
                <div class="stat-card">
                    <div class="number">{{ "{:,}".format(stats.keep_count) }}</div>
                    <div class="label">✅ Marked Keep</div>
                </div>
                <div class="stat-card">
                    <div class="number">{{ "{:,}".format(stats.not_needed_count) }}</div>
                    <div class="label">❌ Not Needed</div>
                </div>
                <div class="stat-card">
                    <div class="number">{{ "{:,}".format(stats.undecided_count) }}</div>
                    <div class="label">⏳ Need Review</div>
                </div>
            </div>

            <h3>Review Workflow</h3>
            <div class="actions">
                <a href="/groups" class="btn">
                    📁 Review Duplicate Groups
                    {% if stats.undecided_count > 0 %}<span style="background: var(--warning); color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.8rem; margin-left: 8px;">{{ stats.undecided_count }}</span>{% endif %}
                </a>
                <a href="/singles" class="btn btn-secondary">
                    🖼️ Review Individual Images
                </a>
                <a href="/bulk" class="btn btn-secondary">
                    ⚡ Bulk Actions
                </a>
                <a href="/export" class="btn btn-secondary">
                    📥 Export Results
                </a>
            </div>
            
            {% if stats.undecided_count > 0 %}
            <div class="info-box">
                <h4>📊 Review Progress</h4>
                <div style="margin-top: 16px;">
                    <div>✅ Reviewed: {{ "{:,}".format(stats.keep_count + stats.not_needed_count) }} files</div>
                    <div>⏳ Remaining: {{ "{:,}".format(stats.undecided_count) }} files</div>
                </div>
                
                {% set progress = ((stats.keep_count + stats.not_needed_count) / stats.total_files * 100) if stats.total_files > 0 else 0 %}
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {{ progress }}%"></div>
                </div>
                <div style="margin-top: 8px; font-size: 0.9rem; color: var(--text-muted);">
                    {{ "%.1f"|format(progress) }}% complete
                </div>
            </div>
            {% endif %}
            
            {% endif %}
        </div>
    </body>
    </html>
    """
    
    return render_template_string(template, stats=stats)

@app.route('/groups')
def view_groups():
    """Enhanced groups view with image previews."""
    page = request.args.get('page', 1, type=int)
    groups_data = cli.get_groups_data(limit=10)
    
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review - Image Groups</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            :root {
                --primary: #1e40af;
                --success: #10b981;
                --warning: #f59e0b;
                --danger: #ef4444;
                --background: #f1f5f9;
                --surface: #ffffff;
                --text: #1f2937;
                --text-muted: #6b7280;
                --border: #e5e7eb;
            }
            
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
                background: var(--background);
                color: var(--text);
                line-height: 1.6;
            }
            
            .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
            
            .header {
                background: var(--surface);
                padding: 20px;
                border-radius: 12px;
                margin-bottom: 24px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            
            .nav {
                display: flex;
                gap: 20px;
                align-items: center;
                flex-wrap: wrap;
            }
            
            .nav h1 { color: var(--primary); margin: 0; }
            .nav a { color: var(--text); text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; }
            .nav a:hover, .nav a.active { background: var(--primary); color: white; }
            
            .group-container {
                background: var(--surface);
                border: 1px solid var(--border);
                border-radius: 12px;
                margin-bottom: 24px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            
            .group-header {
                background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
                padding: 20px;
                border-bottom: 1px solid var(--border);
            }
            
            .group-header h3 {
                color: var(--primary);
                margin-bottom: 12px;
            }
            
            .group-actions {
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
            }
            
            .image-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 16px;
                padding: 20px;
            }
            
            .image-card {
                border: 2px solid transparent;
                border-radius: 8px;
                overflow: hidden;
                transition: all 0.3s ease;
                background: #f8fafc;
            }
            
            .image-card:hover {
                transform: translateY(-4px);
                box-shadow: 0 8px 25px rgba(0,0,0,0.15);
            }
            
            .image-card.original {
                border-color: var(--success);
                box-shadow: 0 0 0 1px var(--success);
            }
            
            .image-container {
                position: relative;
                width: 100%;
                height: 200px;
                overflow: hidden;
                cursor: pointer;
                background: #f1f5f9;
            }
            
            .image-container img {
                width: 100%;
                height: 100%;
                object-fit: cover;
                transition: transform 0.3s ease;
            }
            
            .image-container:hover img {
                transform: scale(1.1);
            }
            
            .original-badge {
                position: absolute;
                top: 8px;
                left: 8px;
                background: var(--success);
                color: white;
                padding: 4px 12px;
                border-radius: 6px;
                font-size: 0.75rem;
                font-weight: bold;
                box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            }
            
            .status-badge {
                position: absolute;
                top: 8px;
                right: 8px;
                padding: 4px 8px;
                border-radius: 6px;
                font-size: 0.75rem;
                font-weight: bold;
                box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            }
            
            .status-keep { background: var(--success); color: white; }
            .status-not_needed { background: var(--danger); color: white; }
            .status-undecided { background: var(--warning); color: white; }
            
            .image-info {
                padding: 16px;
                background: white;
            }
            
            .image-title {
                font-weight: 600;
                margin-bottom: 8px;
                font-size: 0.9rem;
                word-break: break-all;
            }
            
            .image-meta {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 4px;
                font-size: 0.8rem;
                color: var(--text-muted);
                margin-bottom: 12px;
            }
            
            .image-actions {
                display: flex;
                gap: 4px;
            }
            
            .btn {
                padding: 6px 12px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-size: 0.8rem;
                font-weight: 500;
                flex: 1;
                transition: all 0.2s;
            }
            
            .btn-keep { background: var(--success); color: white; }
            .btn-keep:hover { background: #059669; }
            .btn-skip { background: var(--danger); color: white; }
            .btn-skip:hover { background: #dc2626; }
            .btn-promote { background: var(--primary); color: white; }
            .btn-promote:hover { background: #1e3a8a; }
            .btn-group { background: var(--text-muted); color: white; padding: 8px 16px; }
            .btn-group:hover { background: #4b5563; }
            
            /* Modal styles */
            .modal {
                display: none;
                position: fixed;
                z-index: 1000;
                left: 0;
                top: 0;
                width: 100%;
                height: 100%;
                background: rgba(0,0,0,0.9);
                animation: fadeIn 0.3s ease;
            }
            
            @keyframes fadeIn {
                from { opacity: 0; }
                to { opacity: 1; }
            }
            
            .modal-content {
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                height: 100%;
                padding: 40px;
            }
            
            .modal img {
                max-width: 90%;
                max-height: 80%;
                object-fit: contain;
                border-radius: 8px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.5);
            }
            
            .modal-info {
                background: var(--surface);
                padding: 20px;
                border-radius: 12px;
                margin-top: 20px;
                text-align: center;
                max-width: 400px;
            }
            
            .close {
                position: absolute;
                top: 30px;
                right: 40px;
                color: white;
                font-size: 3rem;
                cursor: pointer;
                z-index: 1001;
                transition: color 0.2s;
            }
            
            .close:hover { color: #f87171; }
            
            .message {
                padding: 12px 20px;
                border-radius: 8px;
                margin-bottom: 20px;
                display: none;
                font-weight: 500;
            }
            
            .success {
                background: #d1fae5;
                color: #065f46;
                border: 1px solid #a7f3d0;
            }
            
            .error {
                background: #fee2e2;
                color: #991b1b;
                border: 1px solid #fca5a5;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/groups" class="active">📁 Review Groups</a>
                    <a href="/singles">🖼️ Review Singles</a>
                    <a href="/bulk">⚡ Bulk Actions</a>
                    <a href="/export">📥 Export</a>
                </nav>
            </div>

            <div id="message" class="message"></div>
            
            <h2>Image Groups - Duplicate Detection</h2>
            
            {% for group_data in groups %}
            {% set group = group_data.group %}
            {% set files = group_data.files %}
            
            <div class="group-container" data-group-id="{{ group.group_id }}">
                <div class="group-header">
                    <h3>Group {{ group.group_id }} - {{ files|length }} images</h3>
                    <p style="color: var(--text-muted); margin-bottom: 12px;">
                        Choose the best version and mark others as not needed
                    </p>
                    <div class="group-actions">
                        <button class="btn btn-group" onclick="markGroup({{ group.group_id }}, 'keep')">✅ Keep Group</button>
                        <button class="btn btn-group" onclick="markGroup({{ group.group_id }}, 'not_needed')">❌ Skip Group</button>
                        <button class="btn btn-group" onclick="markGroup({{ group.group_id }}, 'undecided')">↩️ Reset Group</button>
                    </div>
                </div>
                
                <div class="image-grid">
                    {% for file in files %}
                    <div class="image-card {{ 'original' if file.is_original else '' }}" data-file-id="{{ file.file_id }}">
                        <div class="image-container" onclick="showImage({{ file.file_id }})">
                            <img src="/image/{{ file.file_id }}" alt="Image {{ file.file_id }}" loading="lazy">
                            {% if file.is_original %}
                            <div class="original-badge">ORIGINAL</div>
                            {% endif %}
                            <div class="status-badge status-{{ file.review_status }}">{{ file.review_status }}</div>
                        </div>
                        
                        <div class="image-info">
                            <div class="image-title">{{ file.path_on_drive.split('/')[-1] }}</div>
                            <div class="image-meta">
                                <span>{{ file.width or '?' }}×{{ file.height or '?' }}</span>
                                <span>{{ "%.1f"|format(file.size_bytes / 1024 / 1024) }} MB</span>
                                <span>{{ ((file.width or 0) * (file.height or 0) / 1000000) | round(1) }} MP</span>
                                <span>{{ file.drive_label or 'Drive' }}</span>
                            </div>
                            
                            <div class="image-actions">
                                {% if not file.is_original %}
                                <button class="btn btn-promote" onclick="promoteFile({{ file.file_id }})">👑 Make Original</button>
                                {% endif %}
                                <button class="btn btn-keep" onclick="markFile({{ file.file_id }}, 'keep')">✅</button>
                                <button class="btn btn-skip" onclick="markFile({{ file.file_id }}, 'not_needed')">❌</button>
                            </div>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endfor %}
            
            {% if not groups %}
            <div style="background: white; padding: 48px; text-align: center; border-radius: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3>🎉 No duplicate groups found!</h3>
                <p>All images are unique, or groups have been reviewed.</p>
                <a href="/singles" class="btn">Review Individual Images</a>
            </div>
            {% endif %}
        </div>
        
        <!-- Full-size image modal -->
        <div id="imageModal" class="modal">
            <span class="close" onclick="closeModal()">&times;</span>
            <div class="modal-content">
                <img id="modalImage" src="" alt="">
                <div class="modal-info">
                    <div id="modalInfo">Loading...</div>
                </div>
            </div>
        </div>

        <script>
            function showImage(fileId) {
                const modal = document.getElementById('imageModal');
                const img = document.getElementById('modalImage');
                const info = document.getElementById('modalInfo');
                
                img.src = `/image/${fileId}`;
                modal.style.display = 'block';
                
                // Load file info
                fetch(`/api/file-info/${fileId}`)
                    .then(response => response.json())
                    .then(data => {
                        const filename = data.path_on_drive.split('/').pop();
                        const megapixels = data.width && data.height ? 
                            ((data.width * data.height) / 1000000).toFixed(1) : 'Unknown';
                        const sizeMB = data.size_bytes ? 
                            (data.size_bytes / 1024 / 1024).toFixed(1) : 'Unknown';
                        
                        info.innerHTML = `
                            <h3>${filename}</h3>
                            <p><strong>Dimensions:</strong> ${data.width || '?'} × ${data.height || '?'} (${megapixels} MP)</p>
                            <p><strong>Size:</strong> ${sizeMB} MB</p>
                            <p><strong>Status:</strong> <span class="status-badge status-${data.review_status}">${data.review_status}</span></p>
                            ${data.is_original ? '<p><strong>🏆 Group Original</strong></p>' : ''}
                        `;
                    })
                    .catch(err => {
                        info.innerHTML = '<p>Error loading file info</p>';
                    });
            }
            
            function closeModal() {
                document.getElementById('imageModal').style.display = 'none';
            }
            
            // Close modal on click outside
            window.onclick = function(event) {
                const modal = document.getElementById('imageModal');
                if (event.target == modal) {
                    closeModal();
                }
            }
            
            // Close modal on escape key
            document.addEventListener('keydown', function(e) {
                if (e.key === 'Escape') {
                    closeModal();
                }
            });
            
            function markFile(fileId, status) {
                fetch('/api/mark-file', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ file_id: fileId, status: status })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showMessage(`File ${fileId} marked as ${status}`, 'success');
                        updateFileStatus(fileId, status);
                    } else {
                        showMessage(`Error: ${data.error}`, 'error');
                    }
                })
                .catch(error => showMessage(`Network error: ${error}`, 'error'));
            }
            
            function markGroup(groupId, status) {
                fetch('/api/mark-group', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ group_id: groupId, status: status })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showMessage(`Group ${groupId} marked as ${status}`, 'success');
                        // Update all status badges in the group
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
                if (!confirm('Make this image the group original?')) return;
                
                fetch('/api/promote-file', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ file_id: fileId })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showMessage(`File ${fileId} promoted to group original`, 'success');
                        setTimeout(() => location.reload(), 1500);
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
            
            function showMessage(text, type) {
                const msg = document.getElementById('message');
                msg.textContent = text;
                msg.className = `message ${type}`;
                msg.style.display = 'block';
                setTimeout(() => msg.style.display = 'none', 3000);
            }
        </script>
    </body>
    </html>
    """
    
    return render_template_string(template, groups=groups_data)

@app.route('/singles')
def view_singles():
    """Enhanced singles view with image grid."""
    page = request.args.get('page', 1, type=int)
    limit = 24  # Grid layout works better with smaller numbers
    
    # Get single files via database query (since CLI doesn't have this command yet)
    files = []
    try:
        import sqlite3
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            
            offset = (page - 1) * limit
            
            files_data = conn.execute("""
                SELECT 
                    f.file_id,
                    f.path_on_drive,
                    f.width,
                    f.height,
                    f.size_bytes,
                    f.review_status,
                    d.label as drive_label
                FROM files f
                LEFT JOIN drives d ON d.drive_id = f.drive_id
                WHERE f.type = 'image'
                AND f.review_status = 'undecided'
                AND f.group_id IN (
                    SELECT group_id 
                    FROM files 
                    GROUP BY group_id 
                    HAVING COUNT(*) = 1
                )
                ORDER BY f.file_id
                LIMIT ? OFFSET ?
            """, [limit, offset]).fetchall()
            
            files = [dict(f) for f in files_data]
    
    except Exception as e:
        print(f"Error getting singles: {e}")
    
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review - Individual Images</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            /* Same CSS variables and base styles as groups page */
            :root {
                --primary: #1e40af;
                --success: #10b981;
                --warning: #f59e0b;
                --danger: #ef4444;
                --background: #f1f5f9;
                --surface: #ffffff;
                --text: #1f2937;
                --text-muted: #6b7280;
                --border: #e5e7eb;
            }
            
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; background: var(--background); color: var(--text); }
            .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
            .header { background: var(--surface); padding: 20px; border-radius: 12px; margin-bottom: 24px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .nav { display: flex; gap: 20px; align-items: center; }
            .nav h1 { color: var(--primary); margin: 0; }
            .nav a { color: var(--text); text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; }
            .nav a:hover, .nav a.active { background: var(--primary); color: white; }
            
            .singles-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
                gap: 20px;
                margin-bottom: 32px;
            }
            
            .single-card {
                background: var(--surface);
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                border: 2px solid transparent;
                transition: all 0.3s ease;
            }
            
            .single-card:hover {
                transform: translateY(-4px);
                box-shadow: 0 8px 25px rgba(0,0,0,0.15);
            }
            
            .single-card.highlighted {
                border-color: var(--primary);
                transform: scale(1.02);
            }
            
            .single-image {
                position: relative;
                width: 100%;
                height: 250px;
                overflow: hidden;
                cursor: pointer;
                background: #f8fafc;
            }
            
            .single-image img {
                width: 100%;
                height: 100%;
                object-fit: cover;
                transition: transform 0.3s ease;
            }
            
            .single-image:hover img { transform: scale(1.1); }
            
            .single-info {
                padding: 16px;
            }
            
            .single-title {
                font-weight: 600;
                margin-bottom: 8px;
                font-size: 0.95rem;
                word-break: break-all;
            }
            
            .single-meta {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 8px;
                font-size: 0.85rem;
                color: var(--text-muted);
                margin-bottom: 16px;
            }
            
            .single-actions {
                display: flex;
                gap: 8px;
            }
            
            .btn {
                padding: 10px 16px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-size: 0.9rem;
                font-weight: 500;
                flex: 1;
                transition: all 0.2s;
            }
            
            .btn-keep { background: var(--success); color: white; }
            .btn-keep:hover { background: #059669; }
            .btn-skip { background: var(--danger); color: white; }
            .btn-skip:hover { background: #dc2626; }
            .btn-undo { background: var(--text-muted); color: white; }
            .btn-undo:hover { background: #4b5563; }
            
            .status-badge {
                position: absolute;
                top: 12px;
                right: 12px;
                padding: 6px 12px;
                border-radius: 6px;
                font-size: 0.8rem;
                font-weight: bold;
                box-shadow: 0 2px 4px rgba(0,0,0,0.3);
            }
            
            .status-keep { background: var(--success); color: white; }
            .status-not_needed { background: var(--danger); color: white; }
            .status-undecided { background: var(--warning); color: white; }
            
            .keyboard-help {
                position: fixed;
                bottom: 20px;
                right: 20px;
                background: var(--surface);
                padding: 16px;
                border-radius: 12px;
                font-size: 0.85rem;
                color: var(--text-muted);
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                border: 1px solid var(--border);
            }
            
            /* Modal styles (same as groups) */
            .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.9); }
            .modal-content { display: flex; flex-direction: column; justify-content: center; align-items: center; height: 100%; padding: 40px; }
            .modal img { max-width: 90%; max-height: 80%; object-fit: contain; border-radius: 8px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); }
            .modal-info { background: var(--surface); padding: 20px; border-radius: 12px; margin-top: 20px; text-align: center; max-width: 400px; }
            .close { position: absolute; top: 30px; right: 40px; color: white; font-size: 3rem; cursor: pointer; }
            .close:hover { color: #f87171; }
            
            .message { padding: 12px 20px; border-radius: 8px; margin-bottom: 20px; display: none; font-weight: 500; }
            .success { background: #d1fae5; color: #065f46; border: 1px solid #a7f3d0; }
            .error { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/groups">📁 Review Groups</a>
                    <a href="/singles" class="active">🖼️ Review Singles</a>
                    <a href="/bulk">⚡ Bulk Actions</a>
                    <a href="/export">📥 Export</a>
                </nav>
            </div>

            <div id="message" class="message"></div>
            
            {% if files %}
            <h2>Individual Images ({{ files|length }} files)</h2>
            <p style="color: var(--text-muted); margin-bottom: 24px;">
                These are unique images with no duplicates detected. Review each one to keep or skip.
            </p>
            
            <div class="singles-grid">
                {% for file in files %}
                <div class="single-card" data-file-id="{{ file.file_id }}">
                    <div class="single-image" onclick="showImage({{ file.file_id }})">
                        <img src="/image/{{ file.file_id }}" alt="Image {{ file.file_id }}" loading="lazy">
                        <div class="status-badge status-{{ file.review_status }}">{{ file.review_status }}</div>
                    </div>
                    
                    <div class="single-info">
                        <div class="single-title">{{ file.path_on_drive.split('/')[-1] }}</div>
                        <div class="single-meta">
                            <span>{{ file.width or '?' }}×{{ file.height or '?' }}</span>
                            <span>{{ "%.1f"|format(file.size_bytes / 1024 / 1024) }} MB</span>
                            <span>{{ ((file.width or 0) * (file.height or 0) / 1000000) | round(1) }} MP</span>
                            <span>{{ file.drive_label or 'Drive' }}</span>
                        </div>
                        
                        <div class="single-actions">
                            <button class="btn btn-keep" onclick="markFile({{ file.file_id }}, 'keep')">✅ Keep</button>
                            <button class="btn btn-skip" onclick="markFile({{ file.file_id }}, 'not_needed')">❌ Skip</button>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
            {% else %}
            <div style="background: white; padding: 48px; text-align: center; border-radius: 12px;">
                <h3>🎉 No individual images to review!</h3>
                <p>All single images have been reviewed, or they're all in groups.</p>
                <a href="/groups" class="btn">Review Groups</a>
            </div>
            {% endif %}
        </div>
        
        <!-- Full-size image modal -->
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
            <strong>⌨️ Keyboard Shortcuts:</strong><br>
            <strong>K</strong> = Keep • <strong>N</strong> = Not needed<br>
            <strong>←/→</strong> or <strong>H/J</strong> = Navigate<br>
            <strong>Space</strong> = Full size • <strong>Esc</strong> = Close
        </div>

        <script>
            let currentIndex = 0;
            const cards = document.querySelectorAll('.single-card');
            
            // Same modal and message functions as groups page
            function showImage(fileId) {
                const modal = document.getElementById('imageModal');
                const img = document.getElementById('modalImage');
                const info = document.getElementById('modalInfo');
                
                img.src = `/image/${fileId}`;
                modal.style.display = 'block';
                
                fetch(`/api/file-info/${fileId}`)
                    .then(response => response.json())
                    .then(data => {
                        const filename = data.path_on_drive.split('/').pop();
                        const megapixels = data.width && data.height ? 
                            ((data.width * data.height) / 1000000).toFixed(1) : 'Unknown';
                        const sizeMB = data.size_bytes ? 
                            (data.size_bytes / 1024 / 1024).toFixed(1) : 'Unknown';
                        
                        info.innerHTML = `
                            <h3>${filename}</h3>
                            <p><strong>Dimensions:</strong> ${data.width || '?'} × ${data.height || '?'} (${megapixels} MP)</p>
                            <p><strong>Size:</strong> ${sizeMB} MB</p>
                            <p><strong>Status:</strong> <span class="status-badge status-${data.review_status}">${data.review_status}</span></p>
                        `;
                    })
                    .catch(err => info.innerHTML = '<p>Error loading file info</p>');
            }
            
            function closeModal() {
                document.getElementById('imageModal').style.display = 'none';
            }
            
            window.onclick = function(event) {
                if (event.target == document.getElementById('imageModal')) {
                    closeModal();
                }
            }
            
            function markFile(fileId, status) {
                fetch('/api/mark-file', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ file_id: fileId, status: status })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showMessage(`File ${fileId} marked as ${status}`, 'success');
                        updateFileStatus(fileId, status);
                        if (status !== 'undecided') {
                            moveToNext();
                        }
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
            
            function showMessage(text, type) {
                const msg = document.getElementById('message');
                msg.textContent = text;
                msg.className = `message ${type}`;
                msg.style.display = 'block';
                setTimeout(() => msg.style.display = 'none', 3000);
            }
            
            // Navigation and keyboard shortcuts
            function highlightCard(index) {
                cards.forEach(c => c.classList.remove('highlighted'));
                if (cards[index]) {
                    cards[index].classList.add('highlighted');
                    cards[index].scrollIntoView({ behavior: 'smooth', block: 'center' });
                    currentIndex = index;
                }
            }
            
            function moveToNext() {
                if (currentIndex < cards.length - 1) {
                    highlightCard(currentIndex + 1);
                }
            }
            
            function moveToPrev() {
                if (currentIndex > 0) {
                    highlightCard(currentIndex - 1);
                }
            }
            
            // Keyboard shortcuts
            document.addEventListener('keydown', function(e) {
                if (document.getElementById('imageModal').style.display === 'block') {
                    if (e.key === 'Escape') closeModal();
                    return;
                }
                
                if (cards.length === 0) return;
                
                const currentCard = cards[currentIndex];
                const fileId = parseInt(currentCard.getAttribute('data-file-id'));
                
                switch(e.key.toLowerCase()) {
                    case 'k':
                        e.preventDefault();
                        markFile(fileId, 'keep');
                        break;
                    case 'n':
                        e.preventDefault();
                        markFile(fileId, 'not_needed');
                        break;
                    case 'u':
                        e.preventDefault();
                        markFile(fileId, 'undecided');
                        break;
                    case 'arrowright':
                    case 'j':
                        e.preventDefault();
                        moveToNext();
                        break;
                    case 'arrowleft':
                    case 'h':
                        e.preventDefault();
                        moveToPrev();
                        break;
                    case ' ':
                        e.preventDefault();
                        showImage(fileId);
                        break;
                }
            });
            
            // Initialize first card highlight
            if (cards.length > 0) {
                highlightCard(0);
            }
        </script>
    </body>
    </html>
    """
    
    return render_template_string(template, files=files)

# Image serving routes
@app.route('/image/<int:file_id>')
def serve_image(file_id):
    """Serve full-size image file."""
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

@app.route('/api/file-info/<int:file_id>')
def get_file_info(file_id):
    """Get file info via database query (temporary until CLI supports this)."""
    import sqlite3
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
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
            
            if not file_info:
                return jsonify({'error': 'File not found'}), 404
            
            result = dict(file_info)
            
            # Add computed fields
            if result['width'] and result['height']:
                result['pixels'] = result['width'] * result['height']
                result['megapixels'] = round(result['pixels'] / 1000000, 1)
                result['aspect_ratio'] = round(result['width'] / result['height'], 2)
            
            if result['size_bytes']:
                result['size_mb'] = round(result['size_bytes'] / (1024 * 1024), 1)
            
            return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoints using CLI
@app.route('/api/mark-file', methods=['POST'])
def api_mark_file():
    """Mark file via CLI."""
    data = request.get_json()
    file_id = data.get('file_id')
    status = data.get('status')
    note = data.get('note', '')
    
    success, message = cli.mark_file(file_id, status, note)
    
    return jsonify({
        'success': success,
        'message': message if success else None,
        'error': message if not success else None
    })

@app.route('/api/mark-group', methods=['POST'])
def api_mark_group():
    """Mark group via CLI."""
    data = request.get_json()
    group_id = data.get('group_id')
    status = data.get('status')
    note = data.get('note', '')
    
    # Use mark-group CLI command
    args = ['mark-group', '--group-id', str(group_id), '--status', status]
    if note:
        args.extend(['--note', note])
    
    success, stdout, stderr = cli.run_command(*args)
    
    return jsonify({
        'success': success,
        'message': stdout if success else None,
        'error': stderr if not success else None
    })

@app.route('/api/promote-file', methods=['POST'])
def api_promote_file():
    """Promote file via CLI."""
    data = request.get_json()
    file_id = data.get('file_id')
    
    success, message = cli.promote_file(file_id)
    
    return jsonify({
        'success': success,
        'message': message if success else None,
        'error': message if not success else None
    })

@app.route('/bulk')
def bulk_actions():
    """Bulk actions page (same as before)."""
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review - Bulk Actions</title>
        <meta charset="UTF-8">
        <style>
            /* Simplified styling for bulk actions */
            body { font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #f1f5f9; }
            .container { max-width: 800px; margin: 0 auto; }
            .header { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .nav { display: flex; gap: 20px; align-items: center; }
            .nav h1 { color: #1e40af; margin: 0; }
            .nav a { color: #374151; text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; }
            .nav a:hover, .nav a.active { background: #1e40af; color: white; }
            .form-box { background: white; padding: 24px; border-radius: 8px; margin-bottom: 20px; }
            .form-group { margin-bottom: 16px; }
            .form-group label { display: block; margin-bottom: 8px; font-weight: 600; }
            .form-group input, .form-group select { width: 100%; padding: 10px; border: 1px solid #d1d5db; border-radius: 6px; }
            .btn { padding: 12px 24px; background: #1e40af; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 500; }
            .btn:hover { background: #1e3a8a; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/groups">📁 Review Groups</a>
                    <a href="/singles">🖼️ Review Singles</a>
                    <a href="/bulk" class="active">⚡ Bulk Actions</a>
                    <a href="/export">📥 Export</a>
                </nav>
            </div>

            <div class="form-box">
                <h3>📁 Bulk Mark by Path Pattern</h3>
                <div class="form-group">
                    <label for="path-pattern">Path Pattern:</label>
                    <input type="text" id="path-pattern" placeholder="e.g., Screenshot, thumbnails, test1">
                </div>
                <div class="form-group">
                    <label for="bulk-status">Mark as:</label>
                    <select id="bulk-status">
                        <option value="keep">Keep</option>
                        <option value="not_needed">Not Needed</option>
                        <option value="undecided">Undecided</option>
                    </select>
                </div>
                <button class="btn" onclick="bulkMark()">Apply Bulk Action</button>
            </div>
        </div>
        
        <script>
            function bulkMark() {
                const pattern = document.getElementById('path-pattern').value;
                const status = document.getElementById('bulk-status').value;
                
                if (!pattern.trim()) {
                    alert('Please enter a path pattern');
                    return;
                }
                
                if (!confirm(`Mark all files containing "${pattern}" as ${status}?`)) return;
                
                fetch('/api/bulk-mark', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ path_pattern: pattern, status: status })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        alert(`Success: ${data.message}`);
                        document.getElementById('path-pattern').value = '';
                    } else {
                        alert(`Error: ${data.error}`);
                    }
                });
            }
        </script>
    </body>
    </html>
    """
    
    return render_template_string(template)

@app.route('/api/bulk-mark', methods=['POST'])
def api_bulk_mark():
    """Bulk mark files via CLI."""
    data = request.get_json()
    path_pattern = data.get('path_pattern')
    status = data.get('status')
    
    success, message = cli.bulk_mark(path_pattern, status)
    
    return jsonify({
        'success': success,
        'message': message if success else None,
        'error': message if not success else None
    })

if __name__ == '__main__':
    # Validate setup
    if not Path(DB_PATH).exists():
        print(f"❌ Database {DB_PATH} not found!")
        print("Please run: ./media_tool_cli.py scan --source <path> --central <path>")
        exit(1)
    
    if not Path(CLI_COMMAND).exists():
        print(f"❌ CLI command {CLI_COMMAND} not found!")
        print("Please ensure the CLI script is available and executable.")
        exit(1)
    
    print("=" * 80)
    print("🚀 Enhanced Media Review Web Interface - CLI-Driven")
    print("=" * 80)
    print(f"📂 Database: {DB_PATH}")
    print(f"⚡ CLI Command: {CLI_COMMAND}")
    print(f"🌐 URL: http://localhost:5000")
    print("")
    print("✨ Features:")
    print("  📊 Dashboard with statistics")
    print("  📁 Group review with image previews")
    print("  🖼️  Individual image review")
    print("  🔍 Click images for full-size view")
    print("  👑 Promote images to group originals")
    print("  ⚡ Bulk actions and export")
    print("  ⌨️  Keyboard shortcuts for fast review")
    print("=" * 80)
    
    app.run(debug=True, host='0.0.0.0', port=5000)