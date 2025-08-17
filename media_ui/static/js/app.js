/**
 * Media Review Tool - Complete JavaScript Functionality
 * Handles all UI interactions, image viewing, and API calls
 */

// Global state
let currentImageIndex = 0;
let currentImages = [];

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
    initializeImageNavigation();
    initializeKeyboardShortcuts();
    checkForUpdates();
});

// Image viewing and modal functionality
function showImage(fileId) {
    const modal = document.getElementById('imageModal');
    const img = document.getElementById('modalImage');
    const info = document.getElementById('modalInfo');
    const loading = document.querySelector('.loading');
    
    modal.style.display = 'block';
    img.style.display = 'none';
    document.querySelector('.modal-info').style.display = 'none';
    loading.style.display = 'block';
    
    // Load full-size image
    img.onload = function() {
        loading.style.display = 'none';
        img.style.display = 'block';
        document.querySelector('.modal-info').style.display = 'block';
    };
    
    img.onerror = function() {
        loading.innerHTML = 'Error loading image';
    };
    
    img.src = `/image/${fileId}`;
    
    // Load file info via API
    fetch(`/api/file-info/${fileId}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                info.innerHTML = `<p>Error: ${data.error}</p>`;
                return;
            }
            
            const filename = data.path_on_drive ? data.path_on_drive.split('/').pop() : 'Unknown';
            const megapixels = data.width && data.height ? 
                ((data.width * data.height) / 1000000).toFixed(1) : 'Unknown';
            const sizeMB = data.size_bytes ? 
                (data.size_bytes / 1024 / 1024).toFixed(1) : 'Unknown';
            
            info.innerHTML = `
                <h3>${filename}</h3>
                <div class="file-details">
                    <p><strong>Dimensions:</strong> ${data.width || '?'} × ${data.height || '?'} (${megapixels} MP)</p>
                    <p><strong>Size:</strong> ${sizeMB} MB</p>
                    <p><strong>Status:</strong> <span class="status-badge status-${data.review_status}">${data.review_status}</span></p>
                    ${data.is_original ? '<p><strong>🏆 Group Original</strong></p>' : ''}
                    ${data.drive_label ? `<p><strong>Drive:</strong> ${data.drive_label}</p>` : ''}
                </div>
            `;
        })
        .catch(err => {
            info.innerHTML = '<p>Error loading file information</p>';
        });
}

function closeModal() {
    const modal = document.getElementById('imageModal');
    modal.style.display = 'none';
    document.getElementById('modalImage').src = '';
}

// File and group marking
function markFile(fileId, status) {
    fetch('/api/mark-file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ file_id: fileId, status: status })
    })
    .then(response => response.json())
    .then(data => {
        if (data.result === 'success') {
            const changed = data.data.changed;
            if (changed) {
                showMessage(`File ${fileId}: ${data.data.old_status} → ${status}`, 'success');
                updateFileStatus(fileId, status);
                
                // Auto-advance in singles view
                if (window.location.pathname === '/singles' && status !== 'undecided') {
                    setTimeout(moveToNext, 500);
                }
            } else {
                showMessage(`File ${fileId} already marked as ${status}`, 'success');
            }
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
        if (data.result === 'success') {
            const filesUpdated = data.data.files_updated;
            showMessage(`Group ${groupId} marked as ${status} (${filesUpdated} files)`, 'success');
            
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
        if (data.result === 'success') {
            showMessage(`File ${fileId} promoted to group original`, 'success');
            setTimeout(() => location.reload(), 1500);
        } else {
            showMessage(`Error: ${data.error}`, 'error');
        }
    })
    .catch(error => showMessage(`Network error: ${error}`, 'error'));
}

// UI helpers
function updateFileStatus(fileId, status) {
    const badge = document.querySelector(`[data-file-id="${fileId}"] .status-badge`);
    if (badge) {
        badge.className = `status-badge status-${status}`;
        badge.textContent = status;
    }
}

function showMessage(text, type) {
    const msg = document.getElementById('message');
    if (!msg) return;
    
    msg.textContent = text;
    msg.className = `message ${type}`;
    msg.style.display = 'block';
    
    // Auto-hide after 3 seconds
    setTimeout(() => {
        msg.style.display = 'none';
    }, 3000);
}

// Navigation and keyboard shortcuts
function initializeImageNavigation() {
    // Initialize image array for navigation
    const imageCards = document.querySelectorAll('.image-card, .single-card');
    currentImages = Array.from(imageCards);
    
    if (currentImages.length > 0) {
        highlightImage(0);
    }
}

function initializeKeyboardShortcuts() {
    document.addEventListener('keydown', function(e) {
        // Don't handle shortcuts if modal is open
        if (document.getElementById('imageModal') && 
            document.getElementById('imageModal').style.display === 'block') {
            if (e.key === 'Escape') {
                closeModal();
            }
            return;
        }
        
        // Don't handle shortcuts if user is typing in an input
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') {
            return;
        }
        
        if (currentImages.length === 0) return;
        
        const currentCard = currentImages[currentImageIndex];
        if (!currentCard) return;
        
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
            case 'escape':
                closeModal();
                break;
        }
    });
}

function highlightImage(index) {
    if (index < 0 || index >= currentImages.length) return;
    
    // Remove previous highlight
    currentImages.forEach(card => card.classList.remove('highlighted'));
    
    // Add highlight to current
    const card = currentImages[index];
    card.classList.add('highlighted');
    card.scrollIntoView({ behavior: 'smooth', block: 'center' });
    
    currentImageIndex = index;
}

function moveToNext() {
    if (currentImageIndex < currentImages.length - 1) {
        highlightImage(currentImageIndex + 1);
    }
}

function moveToPrev() {
    if (currentImageIndex > 0) {
        highlightImage(currentImageIndex - 1);
    }
}

// Modal event handlers
window.onclick = function(event) {
    const modal = document.getElementById('imageModal');
    if (event.target === modal) {
        closeModal();
    }
}

// Auto-refresh functionality
function checkForUpdates() {
    // Only auto-refresh on dashboard
    if (window.location.pathname === '/') {
        setInterval(refreshStatsIfNeeded, 30000); // Every 30 seconds
    }
}

function refreshStatsIfNeeded() {
    fetch('/api/stats')
        .then(response => response.json())
        .then(data => {
            if (data.data && data.data.review_status) {
                // Check if stats have changed significantly
                const currentUndecided = parseInt(document.querySelector('.stat-card .number').textContent.replace(/,/g, ''));
                const newUndecided = data.data.review_status.undecided;
                
                if (Math.abs(currentUndecided - newUndecided) > 10) {
                    // Significant change, offer to refresh
                    if (confirm('Collection statistics have changed. Refresh the page?')) {
                        location.reload();
                    }
                }
            }
        })
        .catch(error => {
            console.log('Stats check failed:', error);
        });
}

// Utility functions
function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let unitIndex = 0;
    let size = bytes;
    
    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
    }
    
    return `${size.toFixed(1)} ${units[unitIndex]}`;
}

function formatNumber(num) {
    return num.toLocaleString();
}

// Export functionality
function downloadFile(url, filename) {
    const link = document.createElement('a');
    link.href = url;
    link.download = filename || '';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// Global error handler
window.addEventListener('error', function(e) {
    console.error('JavaScript error:', e.error);
    showMessage('An unexpected error occurred. Check the console for details.', 'error');
});

// Performance optimization - lazy load images
function setupLazyLoading() {
    if ('IntersectionObserver' in window) {
        const imageObserver = new IntersectionObserver(function(entries, observer) {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const img = entry.target;
                    img.src = img.dataset.src;
                    img.classList.remove('lazy');
                    observer.unobserve(img);
                }
            });
        });
        
        document.querySelectorAll('img[data-src]').forEach(img => {
            imageObserver.observe(img);
        });
    }
}

// Initialize lazy loading if supported
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setupLazyLoading);
} else {
    setupLazyLoading();
}