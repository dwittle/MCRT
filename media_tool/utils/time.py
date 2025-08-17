#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Time utility functions for the Media Consolidation Tool.
"""

import datetime as dt
from datetime import datetime, timezone


def utc_now_str() -> str:
    """Return current UTC time in ISO-8601 format with 'Z'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    """Return current UTC time in ISO format for database storage."""
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"