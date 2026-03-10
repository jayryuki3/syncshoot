"""Local/external drive destination handler.

Handles file operations on locally mounted volumes:
- Copy, move, delete, mkdir
- Volume detection and free space checking
- Filesystem type detection (APFS, NTFS, ext4, exFAT)
- Eject/unmount support
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psutil

from config import DestinationType


@dataclass
class VolumeInfo:
    """Information about a mounted volume."""
    mount_point: Path
    label: str
    device: str
    fstype: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent_used: float

    @property
    def free_gb(self) -> float:
        return self.free_bytes / (1024 ** 3)

    @property
    def total_gb(self) -> float:
        return self.total_bytes / (1024 ** 3)

    @property
    def is_removable(self) -> bool:
        """Heuristic: removable if mounted under /Volumes, /media, or drive letter."""
        mp = str(self.mount_point)
        return (
            mp.startswith("/Volumes/")
            or mp.startswith("/media/")
            or mp.startswith("/mnt/")
            or (len(mp) == 3 and mp[1] == ":" and mp[2] == "\\")
        )


class LocalDestination:
    """Handler for local/external drive destinations."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.dest_type = DestinationType.LOCAL

    def ensure_dir(self, rel_path: str) -> Path:
        """Create directory structure under root."""
        target = self.root / rel_path
        target.mkdir(parents=True, exist_ok=True)
        return target

    def write_file(self, rel_path: str, data: bytes) -> Path:
        """Write raw bytes to a file."""
        target = self.root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return target

    def delete_file(self, rel_path: str) -> bool:
        """Delete a file. Returns True if deleted."""
        target = self.root / rel_path
        if target.exists():
            target.unlink()
            return True
        return False

    def delete_dir(self, rel_path: str, force: bool = False) -> bool:
        """Delete a directory. If force, remove non-empty."""
        target = self.root / rel_path
        if not target.exists():
            return False
        if force:
            shutil.rmtree(target)
        else:
            target.rmdir()
        return True

    def file_exists(self, rel_path: str) -> bool:
        return (self.root / rel_path).exists()

    def file_size(self, rel_path: str) -> int:
        target = self.root / rel_path
        return target.stat().st_size if target.exists() else 0

    def list_files(self, rel_dir: str = "") -> list[str]:
        """List all files under a relative directory."""
        target = self.root / rel_dir
        if not target.is_dir():
            return []
        result = []
        for dirpath, _, filenames in os.walk(target):
            for fn in filenames:
                fp = Path(dirpath) / fn
                result.append(str(fp.relative_to(self.root)))
        return sorted(result)

    def free_space(self) -> int:
        """Return free space in bytes."""
        try:
            usage = shutil.disk_usage(self.root)
            return usage.free
        except OSError:
            return 0

    def has_space_for(self, needed_bytes: int) -> bool:
        return self.free_space() >= needed_bytes

    @property
    def volume_info(self) -> Optional[VolumeInfo]:
        return get_volume_info(self.root)


# -- Volume Discovery ----------------------------------------------------------
def list_volumes() -> list[VolumeInfo]:
    """List all mounted volumes with their info."""
    volumes = []
    seen_devices = set()

    for part in psutil.disk_partitions(all=False):
        if part.device in seen_devices:
            continue
        seen_devices.add(part.device)

        try:
            usage = psutil.disk_usage(part.mountpoint)
            label = Path(part.mountpoint).name or part.device
            volumes.append(VolumeInfo(
                mount_point=Path(part.mountpoint),
                label=label,
                device=part.device,
                fstype=part.fstype,
                total_bytes=usage.total,
                used_bytes=usage.used,
                free_bytes=usage.free,
                percent_used=usage.percent,
            ))
        except (PermissionError, OSError):
            continue

    return volumes


def get_volume_info(path: Path) -> Optional[VolumeInfo]:
    """Get volume info for the volume containing *path*."""
    path = Path(path).resolve()
    for vol in list_volumes():
        try:
            path.relative_to(vol.mount_point)
            return vol
        except ValueError:
            continue
    return None


def get_filesystem_type(path: Path) -> str:
    """Detect filesystem type for the volume containing *path*."""
    info = get_volume_info(path)
    return info.fstype if info else "unknown"


# -- Eject / Unmount -----------------------------------------------------------
def eject_volume(mount_point: Path) -> tuple[bool, str]:
    """Attempt to eject/unmount a volume. Returns (success, message)."""
    mp = str(mount_point)
    system = platform.system()

    try:
        if system == "Darwin":
            result = subprocess.run(
                ["diskutil", "eject", mp],
                capture_output=True, text=True, timeout=30,
            )
            return result.returncode == 0, result.stdout.strip() or result.stderr.strip()

        elif system == "Linux":
            result = subprocess.run(
                ["umount", mp],
                capture_output=True, text=True, timeout=30,
            )
            return result.returncode == 0, result.stdout.strip() or result.stderr.strip()

        elif system == "Windows":
            # Windows doesn\'t have a simple CLI eject; return guidance
            return False, "Use \'Safely Remove Hardware\' in the system tray"

        return False, f"Unsupported platform: {system}"

    except subprocess.TimeoutExpired:
        return False, "Eject timed out"
    except FileNotFoundError:
        return False, "Eject command not found"
    except Exception as exc:
        return False, str(exc)
