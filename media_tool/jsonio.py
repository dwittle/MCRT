# media_tool/jsonio.py
from __future__ import annotations
import json, logging, sys
from typing import Any, Dict, Optional

def enable_json_logging():
    """Send logs to stderr and suppress info noise when emitting JSON to stdout."""
    # Drop existing handlers to avoid duplicate logs
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)
    logging.basicConfig(stream=sys.stderr, level=logging.ERROR)

def success(command: str, data: Dict[str, Any] | list | None = None,
            meta: Optional[Dict[str, Any]] = None, code: int = 0) -> int:
    payload = {"result": "success", "command": command, "data": data if data is not None else {}}
    if meta:
        payload["meta"] = meta
    # Always print JSON to stdout, logs go to stderr
    print(json.dumps(payload, ensure_ascii=False), file=sys.stdout)
    sys.stdout.flush()  # Ensure immediate output
    return code

def error(command: str, message: str, debug: Optional[Dict[str, Any]] = None, code: int = 1) -> int:
    payload = {"result": "error", "command": command, "error": message}
    if debug:
        payload["debug"] = debug
    # Always print JSON to stdout, logs go to stderr
    print(json.dumps(payload, ensure_ascii=False), file=sys.stdout)
    sys.stdout.flush()  # Ensure immediate output
    return code