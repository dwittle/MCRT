# Media Review Tool - Refactored UI

## Structure
```
media_ui/
├── app.py              # Flask application (routes only)
├── cli_interface.py    # CLI integration with JSON parsing
├── config.py           # Configuration
├── templates/          # Jinja2 templates
│   ├── base.html       # Base template with navigation
│   ├── dashboard.html  # Statistics dashboard
│   ├── groups.html     # Group review with image previews
│   ├── singles.html    # Individual image review
│   ├── bulk.html       # Bulk operations with preview
│   ├── export.html     # Export options
│   └── error.html      # Error pages
├── static/
│   ├── css/
│   │   └── style.css   # Complete CSS styles
│   └── js/
│       └── app.js      # JavaScript functionality
└── requirements.txt    # Python dependencies

## Features
✅ JSON-driven data flow (no string parsing)
✅ Separated templates and static files
✅ Rich bulk operations with preview
✅ Image viewing with modal dialogs
✅ Keyboard shortcuts for efficiency
✅ Responsive design for all screen sizes
✅ Real-time updates and progress tracking

## Usage
1. Install dependencies: pip install flask tabulate
2. Start UI: ./start_media_ui.sh
3. Visit: http://localhost:5000

## Architecture
- **Flask app**: Routes and API endpoints only
- **CLI interface**: JSON integration with robust error handling
- **Templates**: Reusable Jinja2 templates with inheritance
- **Static assets**: Organized CSS and JavaScript
- **JSON data flow**: All operations via enhanced CLI with --json flags
