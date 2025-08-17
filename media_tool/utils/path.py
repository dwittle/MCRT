#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Path utility functions for the Media Consolidation Tool.
"""

from pathlib import Path


def ensure_dir(p: Path) -> None:
    """Ensure directory exists, creating it if necessary."""
    p.mkdir(parents=True, exist_ok=True)