#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Media Review Tool - Web UI Package

A clean, JSON-driven web interface for the Media Consolidation Tool.
Provides visual review capabilities while maintaining CLI-driven architecture.
"""

__version__ = "2.0.0"
__author__ = "Media Tool Team"

# Package metadata
__title__ = "Media Review Tool Web UI"
__description__ = "JSON-driven web interface for media consolidation and review"
__url__ = "https://github.com/your-org/media-tool"

# Import key components for convenient access
from .cli_interface import MediaToolCLI
from .app import app

# Configuration defaults
DEFAULT_CONFIG = {
    'DB_PATH': 'media_index.db',
    'CLI_COMMAND': './media_tool_cli.py',
    'HOST': '0.0.0.0',
    'PORT': 5000,
    'DEBUG': True,
    'SECRET_KEY': 'media-tool-ui-secret'
}

# Convenience functions
def create_app(config=None):
    """
    Create and configure the Flask application.
    
    Args:
        config: Optional configuration dict to override defaults
        
    Returns:
        Configured Flask application
    """
    from .app import app
    
    if config:
        for key, value in config.items():
            app.config[key] = value
    
    return app

def run_ui(host='0.0.0.0', port=5000, debug=True):
    """
    Run the UI server with specified configuration.
    
    Args:
        host: Host to bind to (default: '0.0.0.0')
        port: Port to bind to (default: 5000)
        debug: Enable debug mode (default: True)
    """
    from .app import app
    app.run(host=host, port=port, debug=debug)

def validate_setup():
    """
    Validate that the UI setup is correct.
    
    Returns:
        tuple: (is_valid, error_messages)
    """
    errors = []
    
    # Check for required files
    from pathlib import Path
    
    required_files = [
        DEFAULT_CONFIG['DB_PATH'],
        DEFAULT_CONFIG['CLI_COMMAND']
    ]
    
    for file_path in required_files:
        if not Path(file_path).exists():
            errors.append(f"Required file not found: {file_path}")
    
    # Check CLI functionality
    try:
        cli = MediaToolCLI()
        success, stdout, stderr = cli.run_command('stats', '--json')
        if not success:
            errors.append(f"CLI test failed: {stderr}")
    except Exception as e:
        errors.append(f"CLI interface error: {e}")
    
    # Check Flask dependencies
    try:
        import flask
        import tabulate
    except ImportError as e:
        errors.append(f"Missing dependency: {e}")
    
    return len(errors) == 0, errors

# Package exports
__all__ = [
    'MediaToolCLI',
    'app', 
    'create_app',
    'run_ui',
    'validate_setup',
    'DEFAULT_CONFIG',
    '__version__'
]

# Package-level docstring for help()
def get_package_info():
    """Get package information and usage instructions."""
    return f"""
Media Review Tool Web UI v{__version__}

DESCRIPTION:
{__description__}

FEATURES:
✅ JSON-driven architecture (no string parsing)
✅ Rich image preview and group visualization  
✅ Bulk operations with accurate preview
✅ Keyboard shortcuts for efficient review
✅ Mobile-responsive design
✅ CLI-driven backend (single source of truth)

QUICK START:
>>> import media_ui
>>> is_valid, errors = media_ui.validate_setup()
>>> if is_valid:
...     media_ui.run_ui()
... else:
...     print("Setup errors:", errors)

MANUAL SETUP:
1. Ensure database exists: ./media_tool_cli.py scan --source <path> --central ./data
2. Install dependencies: pip install flask tabulate
3. Run UI: python -m media_ui or media_ui.run_ui()
4. Visit: http://localhost:5000

COMPONENTS:
- MediaToolCLI: JSON-driven CLI interface
- Flask app: Web routes and API endpoints
- Templates: Jinja2 templates in templates/
- Static files: CSS and JavaScript in static/

For more info: help(media_ui.MediaToolCLI)
"""

# Show package info when imported
if __name__ != "__main__":
    # Silent import - don't print anything
    pass
else:
    # Direct execution - show info
    print(get_package_info())