/**
 * Media Review Tool - Complete JavaScript Functionality
 * Handles all UI interactions, image viewing, API calls, and enhanced error handling
 */

// Global state
let currentImageIndex = 0;
let currentImages = [];

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
    initializeImageNavigation();
    initializeKeyboardShortcuts();
    setupImageErrorHandling();
    setupAdvancedLazyLoading();
    addImageTooltips();
    checkForUpdates();
    addImageStateCSS();
});

// Add tooltips to images showing full file path
function addImageTooltips() {
    const imageContainers = document.querySelectorAll('.image-container, .single-image');
    
    imageContainers.forEach(container => {
        const img = container.querySelector('img');
        const card = container.closest('.image-card, .single-card');
        
        if (img && card) {
            const fileId = card.getAttribute('data-file-id');
            
            if (fileId) {
                // Add loading tooltip initially
                img.setAttribute('data-tooltip', 'Loading file information...');
                
                // Fetch file info for tooltip
                fetch(`/api/file-info/${fileId}`)
                    .then(response => response.json())
                    .then(data => {
                        if (data && !data.error) {
                            const filename = data.path_on_drive ? data.path_on_drive.split('/').pop() : 'Unknown';
                            const completePath = data.complete_path || data.path_on_drive || 'Unknown path';
                            const sizeMB = data.size_bytes ? (data.size_bytes / 1024 / 1024).toFixed(1) : 'Unknown';
                            const dimensions = data.width && data.height ? `${data.width} × ${data.height}` : 'Unknown';
                            
                            // Create detailed tooltip - using title for better browser support
                            const tooltipText = `${filename}

📏 ${dimensions} • 📦 ${sizeMB} MB • 🏷️ ${data.review_status}
${data.group_id ? `👥 Group ${data.group_id}` : '📄 Single file'} • 💿 ${data.drive_label || 'Unknown'}

📁 ${completePath}

💡 Click to view full size`;

                            img.title = tooltipText;
                            img.setAttribute('data-tooltip', tooltipText);
                        } else {
                            const fallbackText = `File ID: ${fileId}

💡 Click to view full size

⚠️ File info unavailable`;
                            img.title = fallbackText;
                            img.setAttribute('data-tooltip', fallbackText);
                        }
                    })
                    .catch(error => {
                        console.log('Could not load file info for tooltip:', error);
                        const fallbackText = `File ID: ${fileId}

💡 Click to view full size`;
                        img.title = fallbackText;
                        img.setAttribute('data-tooltip', fallbackText);
                    });
            }
        }
    });
}

// Enhanced image loading with better error handling
function setupImageErrorHandling() {
    // Handle image load errors gracefully
    const images = document.querySelectorAll('img[src*="/image/"], img[src*="/thumbnail/"]');
    
    images.forEach(img => {
        // Add loading state
        img.classList.add('loading');
        
        img.addEventListener('load', function() {
            img.classList.remove('loading');
            img.classList.add('loaded');
            
            // Check if this is actually a placeholder image
            const errorReason = img.getAttribute('data-error');
            if (errorReason) {
                img.classList.add('placeholder');
                console.log(`Image ${img.src} loaded as placeholder: ${errorReason}`);
            }
        });
        
        img.addEventListener('error', function() {
            console.log(`Image failed to load: ${img.src}`);
            img.classList.remove('loading');
            img.classList.add('error');
            
            // The server should now return a placeholder instead of 404
            // But just in case, we can handle it here too
            if (!img.src.includes('placeholder=true')) {
                // Try to reload with placeholder parameter
                const originalSrc = img.src;
                img.src = originalSrc + (originalSrc.includes('?') ? '&' : '?') + 'placeholder=true';
            }
        });
    });
}

// Enhanced image modal with error handling
function showImage(fileId) {
    const modal = document.getElementById('imageModal');
    const img = document.getElementById('modalImage');
    const info = document.getElementById('modalInfo');
    const loading = document.querySelector('.loading');
    
    modal.style.display = 'block';
    img.style.display = 'none';
    document.querySelector('.modal-info').style.display = 'none';
    loading.style.display = 'block';
    loading.textContent = 'Loading image...';
    
    // Reset image state
    img.classList.remove('placeholder', 'error');
    img.onload = null;
    img.onerror = null;
    
    // Enhanced load handler
    img.onload = function() {
        loading.style.display = 'none';
        img.style.display = 'block';
        document.querySelector('.modal-info').style.display = 'block';
        
        // Check response headers for error information
        fetch(`/image/${fileId}`, {method: 'HEAD'})
            .then(response => {
                const errorReason = response.headers.get('X-Error-Reason');
                if (errorReason) {
                    // This is a placeholder image
                    img.classList.add('placeholder');
                    
                    // Add error info to the modal
                    const errorDiv = document.createElement('div');
                    errorDiv.className = 'error-notice';
                    errorDiv.innerHTML = `
                        <div style="background: #fff3cd; border: 1px solid #ffeaa7; border-radius: 4px; padding: 12px; margin-bottom: 16px;">
                            <strong>⚠️ Image Issue:</strong> ${errorReason}
                        </div>
                    `;
                    info.insertBefore(errorDiv, info.firstChild);
                }
            })
            .catch(err => {
                console.log('Could not check image headers:', err);
            });
    };
    
    // Enhanced error handler
    img.onerror = function() {
        loading.style.display = 'none';
        img.style.display = 'block';
        img.classList.add('error');
        document.querySelector('.modal-info').style.display = 'block';
        
        // Show error message
        info.innerHTML = `
            <div class="error-notice">
                <div style="background: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px; padding: 12px;">
                    <strong>❌ Error:</strong> Could not load image ${fileId}
                </div>
            </div>
        `;
    };
    
    // Set image source
    img.src = `/image/${fileId}`;
    
    // Load file info via API (this should still work even if image is missing)
    fetch(`/api/file-info/${fileId}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                info.innerHTML += `<div class="error-notice"><p>File info error: ${data.error}</p></div>`;
                return;
            }
            
            const filename = data.path_on_drive ? data.path_on_drive.split('/').pop() : 'Unknown';
            const megapixels = data.width && data.height ? 
                ((data.width * data.height) / 1000000).toFixed(1) : 'Unknown';
            const sizeMB = data.size_bytes ? 
                (data.size_bytes / 1024 / 1024).toFixed(1) : 'Unknown';
            
            const fileInfoHtml = `
                <h3>${filename}</h3>
                <div class="file-details">
                    <p><strong>Dimensions:</strong> ${data.width || '?'} × ${data.height || '?'} (${megapixels} MP)</p>
                    <p><strong>Size:</strong> ${sizeMB} MB</p>
                    <p><strong>Status:</strong> <span class="status-badge status-${data.review_status}">${data.review_status}</span></p>
                    ${data.is_original ? '<p><strong>🏆 Group Original</strong></p>' : ''}
                    ${data.drive_label ? `<p><strong>Drive:</strong> ${data.drive_label}</p>` : ''}
                    <p><strong>File ID:</strong> ${fileId}</p>
                </div>
            `;
            
            // Add file info after any error notices
            const existingError = info.querySelector('.error-notice');
            if (existingError) {
                existingError.insertAdjacentHTML('afterend', fileInfoHtml);
            } else {
                info.innerHTML = fileInfoHtml;
            }
        })
        .catch(err => {
            console.error('Error loading file info:', err);
            info.innerHTML += `<div class="error-notice"><p>Could not load file information</p></div>`;
        });
}

function closeModal() {
    const modal = document.getElementById('imageModal');
    modal.style.display = 'none';
    document.getElementById('modalImage').src = '';
    
    // Clear any error notices
    const info = document.getElementById('modalInfo');
    const errorNotices = info.querySelectorAll('.error-notice');
    errorNotices.forEach(notice => notice.remove());
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
            
            // Update the UI dynamically instead of reloading
            updateGroupAfterPromotion(fileId, data.data.group_id, data.data.old_original_id);
            
        } else {
            showMessage(`Error: ${data.error}`, 'error');
        }
    })
    .catch(error => showMessage(`Network error: ${error}`, 'error'));
}

function promoteFileFromModal(fileId, groupId) {
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
            
            // Update the UI dynamically
            updateGroupAfterPromotion(fileId, data.data.group_id, data.data.old_original_id);
            
            // Close modal and refresh its content for the new original
            closeModal();
            setTimeout(() => showImage(fileId), 300);
            
        } else {
            showMessage(`Error: ${data.error}`, 'error');
        }
    })
    .catch(error => showMessage(`Network error: ${error}`, 'error'));
}

function updateGroupAfterPromotion(newOriginalId, groupId, oldOriginalId) {
    console.log(`🔄 Updating group ${groupId}: ${oldOriginalId} → ${newOriginalId}`);
    
    // Find all cards in the same group
    const allCards = document.querySelectorAll(`[data-file-id]`);
    let cardsInGroup = [];
    
    // First, identify all cards in this group
    allCards.forEach(card => {
        const cardFileId = parseInt(card.getAttribute('data-file-id'));
        
        // Check if this card is in the target group
        // We need to check both data-group-id attribute and by looking for the group container
        const groupContainer = card.closest(`[data-group-id="${groupId}"]`);
        const cardGroupId = card.getAttribute('data-group-id');
        
        if (groupContainer || (cardGroupId && parseInt(cardGroupId) === groupId)) {
            cardsInGroup.push({card, fileId: cardFileId});
            console.log(`📋 Found card in group ${groupId}: File ${cardFileId}`);
        }
    });
    
    console.log(`📊 Found ${cardsInGroup.length} cards in group ${groupId}`);
    
    cardsInGroup.forEach(({card, fileId}) => {
        // Remove original styling and badge from old original
        if (fileId === oldOriginalId) {
            console.log(`📤 Removing original status from file ${fileId}`);
            
            card.classList.remove('original');
            const oldBadge = card.querySelector('.original-badge');
            if (oldBadge) {
                oldBadge.style.animation = 'badgeDisappear 0.3s ease-out forwards';
                setTimeout(() => oldBadge.remove(), 300);
            }
            
            // Add promote button back to old original (if in groups view)
            const actions = card.querySelector('.image-actions');
            if (actions && !actions.querySelector('.btn-promote')) {
                const promoteBtn = document.createElement('button');
                promoteBtn.className = 'btn btn-promote';
                promoteBtn.onclick = () => promoteFile(fileId);
                promoteBtn.innerHTML = '👑 Make Original';
                promoteBtn.style.animation = 'buttonSlideIn 0.3s ease-out';
                actions.appendChild(promoteBtn);
            }
        }
        
        // Add original styling and badge to new original
        if (fileId === newOriginalId) {
            console.log(`📥 Adding original status to file ${fileId}`);
            
            card.classList.add('original');
            
            // Add original badge if not present
            const imageContainer = card.querySelector('.image-container, .single-image');
            if (imageContainer && !imageContainer.querySelector('.original-badge')) {
                const badge = document.createElement('div');
                badge.className = 'original-badge';
                badge.textContent = 'ORIGINAL';
                badge.style.animation = 'badgeAppear 0.5s ease-out';
                imageContainer.appendChild(badge);
            }
            
            // Remove promote button from new original
            const promoteBtn = card.querySelector('.btn-promote');
            if (promoteBtn) {
                promoteBtn.style.animation = 'buttonSlideOut 0.3s ease-out forwards';
                setTimeout(() => promoteBtn.remove(), 300);
            }
        }
    });
    
    // Update tooltips for affected images
    setTimeout(() => {
        updateImageTooltip(newOriginalId);
        if (oldOriginalId) {
            updateImageTooltip(oldOriginalId);
        }
    }, 100);
    
    console.log(`✅ Group ${groupId} promotion update complete`);
}

function updateImageTooltip(fileId) {
    // Find the image and update its tooltip
    const card = document.querySelector(`[data-file-id="${fileId}"]`);
    if (card) {
        const img = card.querySelector('img');
        if (img) {
            // Refresh the tooltip with updated info
            fetch(`/api/file-info/${fileId}`)
                .then(response => response.json())
                .then(data => {
                    if (data && !data.error) {
                        const filename = data.path_on_drive ? data.path_on_drive.split('/').pop() : 'Unknown';
                        const completePath = data.complete_path || data.path_on_drive || 'Unknown path';
                        const sizeMB = data.size_bytes ? (data.size_bytes / 1024 / 1024).toFixed(1) : 'Unknown';
                        const dimensions = data.width && data.height ? `${data.width} × ${data.height}` : 'Unknown';
                        
                        const tooltipText = `${filename}

📏 ${dimensions} • 📦 ${sizeMB} MB • 🏷️ ${data.review_status}
${data.group_id ? `👥 Group ${data.group_id}` : '📄 Single file'} • 💿 ${data.drive_label || 'Unknown'}
${data.is_original ? '👑 GROUP ORIGINAL' : ''}

📁 ${completePath}

💡 Click to view full size`;

                        img.title = tooltipText;
                        img.setAttribute('data-tooltip', tooltipText);
                    }
                })
                .catch(error => {
                    console.log('Could not update tooltip:', error);
                });
        }
    }
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

// Image lazy loading with error handling
function setupAdvancedLazyLoading() {
    if ('IntersectionObserver' in window) {
        const imageObserver = new IntersectionObserver(function(entries, observer) {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const img = entry.target;
                    const src = img.dataset.src || img.src;
                    
                    img.classList.add('loading');
                    
                    img.onload = function() {
                        img.classList.remove('lazy', 'loading');
                        img.classList.add('loaded');
                    };
                    
                    img.onerror = function() {
                        img.classList.remove('loading');
                        img.classList.add('error');
                        console.log(`Lazy image failed: ${src}`);
                    };
                    
                    img.src = src;
                    observer.unobserve(img);
                }
            });
        }, {
            // Load images when they're 50px away from viewport
            rootMargin: '50px'
        });
        
        document.querySelectorAll('img[data-src], img.lazy').forEach(img => {
            imageObserver.observe(img);
        });
    }
}

// Add CSS styles for image states
function addImageStateCSS() {
    const imageStateCSS = `
        <style id="image-state-css">
        .image-container img.loading,
        .single-image img.loading {
            opacity: 0.6;
            background: #f0f0f0;
        }
        
        .image-container img.error,
        .single-image img.error {
            opacity: 0.8;
            border: 2px dashed #ff6b6b;
        }
        
        .image-container img.placeholder,
        .single-image img.placeholder {
            opacity: 0.9;
            border: 1px solid #ffd93d;
        }
        
        .error-notice {
            margin-bottom: 16px;
        }
        
        .error-notice div {
            font-size: 0.9rem;
            line-height: 1.4;
        }
        
        /* Loading spinner effect */
        .image-container.loading::after,
        .single-image.loading::after {
            content: "Loading...";
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: rgba(255, 255, 255, 0.9);
            padding: 8px 16px;
            border-radius: 4px;
            font-size: 0.8rem;
            color: #666;
            pointer-events: none;
        }
        
        /* Placeholder image styling */
        .modal img.placeholder {
            border: 2px dashed #ffd93d;
            background: #fffbf0;
        }
        
        /* Error state styling */
        .modal img.error {
            border: 2px dashed #ff6b6b;
            background: #fff5f5;
        }
        
        /* Image loading states */
        .image-container,
        .single-image {
            position: relative;
            overflow: hidden;
        }
        
        .image-container img,
        .single-image img {
            transition: opacity 0.3s ease;
        }
        
        /* Status badges for different image states */
        .image-container .image-status,
        .single-image .image-status {
            position: absolute;
            bottom: 8px;
            left: 8px;
            background: rgba(0, 0, 0, 0.7);
            color: white;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 0.7rem;
            font-weight: 600;
        }
        
        .image-container .image-status.missing,
        .single-image .image-status.missing {
            background: rgba(255, 193, 7, 0.9);
            color: #000;
        }
        
        .image-container .image-status.error,
        .single-image .image-status.error {
            background: rgba(220, 53, 69, 0.9);
        }
        </style>
    `;
    
    // Only add if not already present
    if (!document.getElementById('image-state-css')) {
        document.head.insertAdjacentHTML('beforeend', imageStateCSS);
    }
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

// Enhanced error handling for network requests
function handleNetworkError(error, context) {
    console.error(`Network error in ${context}:`, error);
    
    let message = 'Network error occurred';
    if (error.message) {
        message += `: ${error.message}`;
    }
    
    showMessage(message, 'error');
}

// Global error handler
window.addEventListener('error', function(e) {
    console.error('JavaScript error:', e.error);
    
    // Only show user-facing errors for critical issues
    if (e.error && e.error.message && !e.error.message.includes('ResizeObserver')) {
        showMessage('An unexpected error occurred. Check the console for details.', 'error');
    }
});

// Unhandled promise rejection handler
window.addEventListener('unhandledrejection', function(e) {
    console.error('Unhandled promise rejection:', e.reason);
    
    // Don't show all promise rejections to users, just log them
    if (e.reason && typeof e.reason === 'object' && e.reason.message) {
        console.error('Promise rejection details:', e.reason.message);
    }
});

// Debug helpers (only in development)
if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') {
    // Add debug helpers
    window.debugMRT = {
        currentImages: () => currentImages,
        currentIndex: () => currentImageIndex,
        showImage: showImage,
        markFile: markFile,
        markGroup: markGroup,
        testPlaceholder: (fileId) => {
            const img = new Image();
            img.onload = () => console.log(`Placeholder for ${fileId} loaded`);
            img.onerror = () => console.log(`Placeholder for ${fileId} failed`);
            img.src = `/image/${fileId}`;
        }
    };
    
    console.log('🔧 Debug helpers available: window.debugMRT');
}