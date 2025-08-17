#!/usr/bin/env python3
"""
Simple CLI-driven web interface for Media Tool Review.
All operations are performed through CLI commands for consistency.
"""

import json
import subprocess
import os
from pathlib import Path
from flask import Flask, request, jsonify, send_file, render_template_string

app = Flask(__name__)
app.secret_key = 'media-tool-ui-secret'

# Configuration
DB_PATH = os.environ.get('MEDIA_DB_PATH', 'media_index.db')
CLI_COMMAND = os.environ.get('MEDIA_CLI', './media_tool_cli.py')

class MediaToolCLI:
    """Interface to the media tool CLI."""
    
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
        
        # Parse CLI output (simplified)
        stats = {'total_files': 0, 'total_groups': 0, 'undecided_count': 0, 'keep_count': 0, 'not_needed_count': 0}
        
        for line in stdout.split('\n'):
            line = line.strip()
            if 'Files:' in line and line.split(':')[0].strip() == 'Files':
                stats['total_files'] = int(line.split(':')[1].strip().replace(',', ''))
            elif 'Groups:' in line and line.split(':')[0].strip() == 'Groups':
                stats['total_groups'] = int(line.split(':')[1].strip().replace(',', ''))
            elif line.startswith('undecided:'):
                stats['undecided_count'] = int(line.split(':')[1].strip().replace(',', ''))
            elif line.startswith('keep:'):
                stats['keep_count'] = int(line.split(':')[1].strip().replace(',', ''))
            elif line.startswith('not_needed:'):
                stats['not_needed_count'] = int(line.split(':')[1].strip().replace(',', ''))
        
        return stats
    
    def get_review_queue(self, limit=50):
        """Get review queue."""
        success, stdout, stderr = self.run_command('review-queue', '--limit', limit)
        if not success:
            return []
        
        files = []
        lines = stdout.split('\n')
        
        for line in lines:
            if '|' in line and not line.startswith('file_id') and not line.startswith('-'):
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 7:
                    try:
                        files.append({
                            'file_id': int(parts[0]),
                            'group_id': int(parts[1]) if parts[1] != '-1' else None,
                            'type': parts[2],
                            'dimensions': parts[3],
                            'size_bytes': int(parts[4]) if parts[4].isdigit() else 0,
                            'status': parts[5],
                            'filename': parts[6].split('/')[-1]
                        })
                    except (ValueError, IndexError):
                        continue
        
        return files
    
    def mark_file(self, file_id, status, note=''):
        """Mark file status."""
        args = ['mark', '--file-id', file_id, '--status', status]
        if note:
            args.extend(['--note', note])
        
        success, stdout, stderr = self.run_command(*args)
        return success, stdout if success else stderr
    
    def mark_group(self, group_id, status, note=''):
        """Mark group status."""
        args = ['mark-group', '--group-id', group_id, '--status', status]
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
    
    def export_backup_list(self, output_path, include_undecided=False, include_large=False):
        """Export backup list."""
        args = ['export-backup-list', '--out', output_path]
        if include_undecided:
            args.append('--include-undecided')
        if include_large:
            args.append('--include-large')
        
        success, stdout, stderr = self.run_command(*args)
        return success, stdout if success else stderr

# Initialize CLI interface
cli = MediaToolCLI()

@app.route('/')
def dashboard():
    """Dashboard showing collection statistics."""
    stats = cli.get_stats()
    
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review Tool</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 20px; background: #f1f5f9; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: white; padding: 24px; border-radius: 12px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            .nav { display: flex; gap: 24px; align-items: center; }
            .nav h1 { color: #1e40af; margin: 0; font-size: 1.75rem; }
            .nav a { color: #374151; text-decoration: none; padding: 8px 16px; border-radius: 8px; font-weight: 500; }
            .nav a:hover, .nav a.active { background: #1e40af; color: white; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 32px; }
            .stat-card { background: white; padding: 24px; border-radius: 12px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #e5e7eb; }
            .stat-card .number { font-size: 2.5rem; font-weight: bold; color: #1e40af; margin-bottom: 8px; }
            .stat-card .label { color: #6b7280; font-size: 0.9rem; font-weight: 500; }
            .actions { display: flex; gap: 16px; flex-wrap: wrap; }
            .btn { padding: 12px 24px; background: #1e40af; color: white; text-decoration: none; border-radius: 8px; font-weight: 500; display: inline-flex; align-items: center; gap: 8px; border: none; cursor: pointer; }
            .btn:hover { background: #1e3a8a; }
            .btn-secondary { background: #6b7280; }
            .btn-secondary:hover { background: #4b5563; }
            .info-box { background: white; padding: 24px; border-radius: 12px; margin-top: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            .progress-bar { background: #e5e7eb; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 12px; }
            .progress-fill { background: #10b981; height: 100%; transition: width 0.3s; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/" class="active">Dashboard</a>
                    <a href="/review">Review Files</a>
                    <a href="/bulk">Bulk Actions</a>
                    <a href="/export">Export</a>
                </nav>
            </div>

            {% if stats.error %}
            <div style="background: #fef2f2; color: #991b1b; padding: 16px; border-radius: 8px;">
                <strong>Error:</strong> {{ stats.error }}
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
                    <div class="label">Marked Keep</div>
                </div>
                <div class="stat-card">
                    <div class="number">{{ "{:,}".format(stats.not_needed_count) }}</div>
                    <div class="label">Not Needed</div>
                </div>
                <div class="stat-card">
                    <div class="number">{{ "{:,}".format(stats.undecided_count) }}</div>
                    <div class="label">Need Review</div>
                </div>
                <div class="stat-card">
                    <div class="number">{{ "{:.1f}".format(stats.total_size_gb) if stats.total_size_gb else "0.0" }} GB</div>
                    <div class="label">Total Size</div>
                </div>
            </div>

            <h3>Quick Actions</h3>
            <div class="actions">
                <a href="/review" class="btn">
                    🔍 Review Files ({{ stats.undecided_count }})
                </a>
                <a href="/bulk" class="btn btn-secondary">
                    ⚡ Bulk Actions
                </a>
                <a href="/export" class="btn btn-secondary">
                    📋 Export Results
                </a>
                <button class="btn btn-secondary" onclick="refreshStats()">
                    🔄 Refresh Stats
                </button>
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
                <div style="margin-top: 8px; font-size: 0.9rem; color: #6b7280;">
                    {{ "%.1f"|format(progress) }}% complete
                </div>
            </div>
            {% endif %}
            
            {% endif %}
        </div>
        
        <script>
            function refreshStats() {
                location.reload();
            }
        </script>
    </body>
    </html>
    """
    
    return render_template_string(template, stats=stats)

@app.route('/review')
def review():
    """File review interface."""
    limit = request.args.get('limit', 50, type=int)
    files = cli.get_review_queue(limit)
    
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review - Review Files</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #f1f5f9; }
            .container { max-width: 1400px; margin: 0 auto; }
            .header { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .nav { display: flex; gap: 20px; align-items: center; }
            .nav h1 { color: #1e40af; margin: 0; }
            .nav a { color: #374151; text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; }
            .nav a:hover, .nav a.active { background: #1e40af; color: white; }
            .file-table { background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            .file-table table { width: 100%; border-collapse: collapse; }
            .file-table th, .file-table td { padding: 12px; text-align: left; border-bottom: 1px solid #e5e7eb; }
            .file-table th { background: #f9fafb; font-weight: 600; color: #374151; }
            .file-table tr:hover { background: #f9fafb; }
            .file-table tr.highlighted { background: #dbeafe !important; }
            .btn { padding: 6px 12px; border: none; border-radius: 4px; cursor: pointer; font-size: 0.8rem; margin: 0 2px; font-weight: 500; }
            .btn-keep { background: #10b981; color: white; }
            .btn-keep:hover { background: #059669; }
            .btn-skip { background: #ef4444; color: white; }
            .btn-skip:hover { background: #dc2626; }
            .btn-undo { background: #6b7280; color: white; }
            .btn-undo:hover { background: #4b5563; }
            .status-badge { padding: 4px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
            .status-keep { background: #d1fae5; color: #065f46; }
            .status-not_needed { background: #fee2e2; color: #991b1b; }
            .status-undecided { background: #fef3c7; color: #92400e; }
            .message { padding: 12px; border-radius: 6px; margin-bottom: 16px; display: none; }
            .success { background: #d1fae5; color: #065f46; }
            .error { background: #fee2e2; color: #991b1b; }
            .keyboard-help { position: fixed; bottom: 20px; right: 20px; background: white; padding: 12px; border-radius: 8px; font-size: 0.8rem; color: #6b7280; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/review" class="active">Review Files</a>
                    <a href="/bulk">Bulk Actions</a>
                    <a href="/export">Export</a>
                </nav>
            </div>

            <div id="message" class="message"></div>
            
            {% if files %}
            <h2>Review Queue ({{ files|length }} files)</h2>
            
            <div class="file-table">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Group</th>
                            <th>Dimensions</th>
                            <th>Size</th>
                            <th>Status</th>
                            <th>Filename</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for file in files %}
                        <tr id="file-{{ file.file_id }}" data-file-id="{{ file.file_id }}">
                            <td>{{ file.file_id }}</td>
                            <td>{{ file.group_id or '-' }}</td>
                            <td>{{ file.dimensions }}</td>
                            <td>{{ "{:,.1f}".format(file.size_bytes / 1024 / 1024) }} MB</td>
                            <td><span class="status-badge status-{{ file.status }}">{{ file.status }}</span></td>
                            <td title="{{ file.filename }}">{{ file.filename[:50] }}{{ '...' if file.filename|length > 50 else '' }}</td>
                            <td>
                                <button class="btn btn-keep" onclick="markFile({{ file.file_id }}, 'keep')">Keep</button>
                                <button class="btn btn-skip" onclick="markFile({{ file.file_id }}, 'not_needed')">Skip</button>
                                <button class="btn btn-undo" onclick="markFile({{ file.file_id }}, 'undecided')">Reset</button>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            {% else %}
            <div style="background: white; padding: 48px; text-align: center; border-radius: 12px;">
                <h3>🎉 All files reviewed!</h3>
                <p>No files need review. All files have been marked as 'keep' or 'not_needed'.</p>
                <a href="/" class="btn">Back to Dashboard</a>
            </div>
            {% endif %}
        </div>

        <div class="keyboard-help">
            <strong>Keyboard Shortcuts:</strong><br>
            K = Keep • N = Not needed • U = Reset<br>
            ↓/J = Next • ↑/K = Previous
        </div>

        <script>
            let currentIndex = 0;
            const rows = document.querySelectorAll('tbody tr');
            
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
                        updateRowStatus(fileId, status);
                        if (status !== 'undecided') {
                            moveToNext();
                        }
                    } else {
                        showMessage(`Error: ${data.error}`, 'error');
                    }
                })
                .catch(error => showMessage(`Network error: ${error}`, 'error'));
            }
            
            function updateRowStatus(fileId, status) {
                const badge = document.querySelector(`#file-${fileId} .status-badge`);
                if (badge) {
                    badge.className = `status-badge status-${status}`;
                    badge.textContent = status;
                }
            }
            
            function highlightRow(index) {
                rows.forEach(r => r.classList.remove('highlighted'));
                if (rows[index]) {
                    rows[index].classList.add('highlighted');
                    rows[index].scrollIntoView({ behavior: 'smooth', block: 'center' });
                    currentIndex = index;
                }
            }
            
            function moveToNext() {
                if (currentIndex < rows.length - 1) {
                    highlightRow(currentIndex + 1);
                }
            }
            
            function moveToPrev() {
                if (currentIndex > 0) {
                    highlightRow(currentIndex - 1);
                }
            }
            
            function showMessage(text, type) {
                const msg = document.getElementById('message');
                msg.textContent = text;
                msg.className = `message ${type}`;
                msg.style.display = 'block';
                setTimeout(() => msg.style.display = 'none', 3000);
            }
            
            // Keyboard shortcuts
            document.addEventListener('keydown', function(e) {
                if (rows.length === 0) return;
                
                const currentRow = rows[currentIndex];
                const fileId = parseInt(currentRow.getAttribute('data-file-id'));
                
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
                    case 'arrowdown':
                    case 'j':
                        e.preventDefault();
                        moveToNext();
                        break;
                    case 'arrowup':
                        e.preventDefault();
                        moveToPrev();
                        break;
                }
            });
            
            // Initialize first row highlight
            if (rows.length > 0) {
                highlightRow(0);
            }
        </script>
    </body>
    </html>
    """
    
    return render_template_string(template, files=files)

@app.route('/bulk')
def bulk_actions():
    """Bulk actions interface."""
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review - Bulk Actions</title>
        <meta charset="UTF-8">
        <style>
            body { font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #f1f5f9; }
            .container { max-width: 800px; margin: 0 auto; }
            .header { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .nav { display: flex; gap: 20px; align-items: center; }
            .nav h1 { color: #1e40af; margin: 0; }
            .nav a { color: #374151; text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; }
            .nav a:hover, .nav a.active { background: #1e40af; color: white; }
            .form-box { background: white; padding: 24px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            .form-group { margin-bottom: 16px; }
            .form-group label { display: block; margin-bottom: 8px; font-weight: 600; color: #374151; }
            .form-group input, .form-group select { width: 100%; padding: 10px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 1rem; }
            .btn { padding: 12px 24px; background: #1e40af; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 500; }
            .btn:hover { background: #1e3a8a; }
            .btn-danger { background: #ef4444; }
            .btn-danger:hover { background: #dc2626; }
            .message { padding: 12px; border-radius: 6px; margin-bottom: 16px; display: none; }
            .success { background: #d1fae5; color: #065f46; }
            .error { background: #fee2e2; color: #991b1b; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/review">Review Files</a>
                    <a href="/bulk" class="active">Bulk Actions</a>
                    <a href="/export">Export</a>
                </nav>
            </div>

            <div id="message" class="message"></div>
            
            <h2>Bulk Actions</h2>
            
            <div class="form-box">
                <h3>📁 Bulk Mark by Path Pattern</h3>
                <p>Mark multiple files based on path pattern matching.</p>
                
                <div class="form-group">
                    <label for="path-pattern">Path Pattern:</label>
                    <input type="text" id="path-pattern" placeholder="e.g., Screenshot, DCIM, thumbnails" 
                           title="Files containing this text in their path will be marked">
                    <small style="color: #6b7280;">Examples: "Screenshot" (all screenshots), "2023" (files from 2023), "thumbnails" (thumbnail folders)</small>
                </div>
                
                <div class="form-group">
                    <label for="bulk-status">Mark as:</label>
                    <select id="bulk-status">
                        <option value="keep">Keep</option>
                        <option value="not_needed" selected>Not Needed</option>
                        <option value="undecided">Undecided</option>
                    </select>
                </div>
                
                <button class="btn" onclick="bulkMark()">Apply Bulk Action</button>
            </div>
            
            <div class="form-box">
                <h3>🧹 Cleanup Operations</h3>
                <p>Database maintenance and cleanup operations.</p>
                
                <div style="display: flex; gap: 12px; flex-wrap: wrap;">
                    <button class="btn" onclick="showDetailedStats()">📊 Show Detailed Stats</button>
                    <button class="btn btn-danger" onclick="cleanupCheckpoints()">🗑️ Cleanup Checkpoints</button>
                </div>
            </div>
        </div>

        <script>
            function bulkMark() {
                const pattern = document.getElementById('path-pattern').value;
                const status = document.getElementById('bulk-status').value;
                
                if (!pattern.trim()) {
                    showMessage('Please enter a path pattern', 'error');
                    return;
                }
                
                if (!confirm(`Mark all files containing "${pattern}" as ${status}?`)) {
                    return;
                }
                
                fetch('/api/bulk-mark', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ path_pattern: pattern, status: status })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showMessage(data.message, 'success');
                        document.getElementById('path-pattern').value = '';
                    } else {
                        showMessage(`Error: ${data.error}`, 'error');
                    }
                })
                .catch(error => showMessage(`Network error: ${error}`, 'error'));
            }
            
            function showDetailedStats() {
                fetch('/api/detailed-stats')
                .then(response => response.text())
                .then(data => {
                    const newWindow = window.open('', '_blank');
                    newWindow.document.write(`
                        <html>
                        <head><title>Detailed Statistics</title></head>
                        <body style="font-family: monospace; padding: 20px; background: #f8fafc;">
                            <h2>Detailed Statistics</h2>
                            <pre style="background: white; padding: 20px; border-radius: 8px;">${data}</pre>
                        </body>
                        </html>
                    `);
                })
                .catch(error => showMessage(`Error loading stats: ${error}`, 'error'));
            }
            
            function cleanupCheckpoints() {
                if (!confirm('Clean up checkpoints older than 7 days?')) return;
                
                fetch('/api/cleanup-checkpoints', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showMessage(data.message, 'success');
                    } else {
                        showMessage(`Error: ${data.error}`, 'error');
                    }
                })
                .catch(error => showMessage(`Network error: ${error}`, 'error'));
            }
            
            function showMessage(text, type) {
                const msg = document.getElementById('message');
                msg.textContent = text;
                msg.className = `message ${type}`;
                msg.style.display = 'block';
                setTimeout(() => msg.style.display = 'none', 5000);
            }
        </script>
    </body>
    </html>
    """
    
    return render_template_string(template)

@app.route('/export')
def export_page():
    """Export interface."""
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Media Review - Export</title>
        <meta charset="UTF-8">
        <style>
            body { font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #f1f5f9; }
            .container { max-width: 800px; margin: 0 auto; }
            .header { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .nav { display: flex; gap: 20px; align-items: center; }
            .nav h1 { color: #1e40af; margin: 0; }
            .nav a { color: #374151; text-decoration: none; padding: 8px 16px; border-radius: 6px; font-weight: 500; }
            .nav a:hover, .nav a.active { background: #1e40af; color: white; }
            .form-box { background: white; padding: 24px; border-radius: 8px; margin-bottom: 20px; }
            .btn { padding: 12px 24px; background: #1e40af; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 500; text-decoration: none; display: inline-block; }
            .btn:hover { background: #1e3a8a; }
            .checkbox-group { margin: 16px 0; }
            .checkbox-group label { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
            .message { padding: 12px; border-radius: 6px; margin-bottom: 16px; display: none; }
            .success { background: #d1fae5; color: #065f46; }
            .error { background: #fee2e2; color: #991b1b; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <nav class="nav">
                    <h1>📸 Media Review Tool</h1>
                    <a href="/">Dashboard</a>
                    <a href="/review">Review Files</a>
                    <a href="/bulk">Bulk Actions</a>
                    <a href="/export" class="active">Export</a>
                </nav>
            </div>

            <div id="message" class="message"></div>
            
            <h2>Export Options</h2>
            
            <div class="form-box">
                <h3>📋 Export Backup List</h3>
                <p>Generate a CSV file listing all files marked for backup.</p>
                
                <div class="checkbox-group">
                    <label>
                        <input type="checkbox" id="include-undecided" checked>
                        Include undecided files
                    </label>
                    <label>
                        <input type="checkbox" id="include-large">
                        Include large files (>500MB)
                    </label>
                </div>
                
                <button class="btn" onclick="exportBackupList()">📥 Download Backup List</button>
            </div>
            
            <div class="form-box">
                <h3>📊 Export Statistics</h3>
                <p>Download detailed collection statistics.</p>
                
                <button class="btn" onclick="exportStats()">📈 Download Stats Report</button>
            </div>
        </div>

        <script>
            function exportBackupList() {
                const includeUndecided = document.getElementById('include-undecided').checked;
                const includeLarge = document.getElementById('include-large').checked;
                
                let url = '/api/export-backup';
                const params = new URLSearchParams();
                if (includeUndecided) params.append('include_undecided', 'true');
                if (includeLarge) params.append('include_large', 'true');
                
                if (params.toString()) {
                    url += '?' + params.toString();
                }
                
                // Create download link
                const link = document.createElement('a');
                link.href = url;
                link.download = '';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                
                showMessage('Backup list download started', 'success');
            }
            
            function exportStats() {
                fetch('/api/export-stats')
                .then(response => response.blob())
                .then(blob => {
                    const url = window.URL.createObjectURL(blob);
                    const link = document.createElement('a');
                    link.href = url;
                    link.download = `media_stats_${new Date().toISOString().slice(0,10)}.txt`;
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                    window.URL.revokeObjectURL(url);
                })
                .catch(error => showMessage(`Export error: ${error}`, 'error'));
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
    
    return render_template_string(template)

# API endpoints that call CLI
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

@app.route('/api/bulk-mark', methods=['POST'])
def api_bulk_mark():
    """Bulk mark files via CLI."""
    data = request.get_json()
    path_pattern = data.get('path_pattern')
    status = data.get('status')
    
    success, message = cli.bulk_mark(path_pattern, status)
    
    return jsonify({
        'success': success,
        'message': f'Bulk operation completed: {message}' if success else None,
        'error': message if not success else None
    })

@app.route('/api/detailed-stats')
def api_detailed_stats():
    """Get detailed stats via CLI."""
    success, stdout, stderr = cli.run_command('stats', '--detailed')
    
    if success:
        return stdout, 200, {'Content-Type': 'text/plain'}
    else:
        return f"Error: {stderr}", 500

@app.route('/api/cleanup-checkpoints', methods=['POST'])
def api_cleanup_checkpoints():
    """Cleanup checkpoints via CLI."""
    success, stdout, stderr = cli.run_command('cleanup-checkpoints', '--days', '7')
    
    return jsonify({
        'success': success,
        'message': 'Cleaned up old checkpoints' if success else None,
        'error': stderr if not success else None
    })

@app.route('/api/export-backup')
def api_export_backup():
    """Export backup list via CLI."""
    include_undecided = request.args.get('include_undecided') == 'true'
    include_large = request.args.get('include_large') == 'true'
    
    # Generate filename
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'backup_list_{timestamp}.csv'
    
    # Build CLI command
    args = ['export-backup-list', '--out', filename]
    if include_undecided:
        args.append('--include-undecided')
    if include_large:
        args.append('--include-large')
    
    success, stdout, stderr = cli.run_command(*args)
    
    if success and Path(filename).exists():
        return send_file(filename, as_attachment=True, download_name=filename)
    else:
        return f"Export failed: {stderr}", 500

@app.route('/api/export-stats')
def api_export_stats():
    """Export detailed stats as text file."""
    success, stdout, stderr = cli.run_command('stats', '--detailed')
    
    if success:
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create temporary file
        temp_file = f'media_stats_{timestamp}.txt'
        with open(temp_file, 'w') as f:
            f.write(f"Media Tool Statistics Report\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write("=" * 50 + "\n\n")
            f.write(stdout)
        
        return send_file(temp_file, as_attachment=True, download_name=temp_file)
    else:
        return f"Stats export failed: {stderr}", 500

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
    
    print("=" * 70)
    print("🚀 Media Review Web Interface - CLI-Driven Architecture")
    print("=" * 70)
    print(f"📂 Database: {DB_PATH}")
    print(f"⚡ CLI Command: {CLI_COMMAND}")
    print(f"🌐 URL: http://localhost:5000")
    print("")
    print("✨ All operations go through CLI for consistency!")
    print("✨ No direct database access - pure CLI integration!")
    print("=" * 70)
    
    app.run(debug=True, host='0.0.0.0', port=5000)