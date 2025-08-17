#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Drive detection and management for the Media Consolidation Tool.
"""

import subprocess
from pathlib import Path
from typing import Tuple, Optional


class DriveManager:
    """Handle drive detection and management."""
    
    @staticmethod
    def detect_drive_info(source: Path, wsl_mode: bool) -> Tuple[Optional[str], Optional[str], str]:
        """Detect drive label, serial/uuid, and mount path."""
        if wsl_mode:
            return DriveManager._detect_wsl_drive(source)
        else:
            return DriveManager._detect_windows_drive(source)
    
    @staticmethod
    def _detect_windows_drive(source: Path) -> Tuple[Optional[str], Optional[str], str]:
        """Windows drive detection via wmic."""
        drive = source.drive or (str(source)[:3] if len(str(source)) >= 3 and str(source)[1:3] == ":\\" else None)
        mount_path = drive if drive else str(source.anchor)
        label, serial = None, None
        
        try:
            cmd = ["wmic", "logicaldisk", "get", "DeviceID,VolumeName,VolumeSerialNumber"]
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
            for line in out.splitlines():
                if not line.strip() or "DeviceID" in line:
                    continue
                parts = line.split()
                if len(parts) >= 1 and parts[0].upper() == (drive[:-1]).upper():
                    serial = parts[-1]
                    if len(parts) > 2:
                        label = " ".join(parts[1:-1]) or None
                    break
        except Exception:
            pass
        
        return label, serial, mount_path
    
    @staticmethod
    def _detect_wsl_drive(source: Path) -> Tuple[Optional[str], Optional[str], str]:
        """WSL drive detection via lsblk."""
        mount_path = str(source)
        label, uuid = None, None
        
        try:
            out = subprocess.check_output(["lsblk", "-o", "NAME,LABEL,UUID,MOUNTPOINT", "-P"], text=True)
            for line in out.splitlines():
                fields = {}
                for kv in line.split():
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        fields[k] = v.strip('"')
                if fields.get("MOUNTPOINT") == mount_path:
                    label = fields.get("LABEL")
                    uuid = fields.get("UUID") 
                    break
        except Exception:
            pass
        
        return label, uuid, mount_path