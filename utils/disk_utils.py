"""Disk and volume utility functions.

Cross-platform helpers for:
- Volume/disk enumeration
- Mount/unmount event detection
- Free space, total space, filesystem type queries
- Volume label and serial number reading
"""

from __future__ import annotations

import logging
import platform
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Set

import psutil

logger = logging.getLogger(__name__)


@dataclass
class DiskInfo:
    """Information about a disk/volume."""
    mount_point: str
    device: str
    fstype: str
    label: str
    total: int          # bytes
    used: int           # bytes
    free: int           # bytes
    percent: float
    opts: str = ""      # mount options

    @property
    def total_gb(self) -> float:
        return self.total / (1024 ** 3)

    @property
    def free_gb(self) -> float:
        return self.free / (1024 ** 3)

    @property
    def is_removable(self) -> bool:
        mp = self.mount_point
        return (
            mp.startswith("/Volumes/")
            or mp.startswith("/media/")
            or mp.startswith("/mnt/")
            or (len(mp) >= 3 and mp[1] == ":" and mp[0].isalpha())
        )

    @property
    def is_network(self) -> bool:
        return self.fstype.lower() in ("nfs", "cifs", "smbfs", "afpfs", "webdav")


# ── Enumeration ───────────────────────────────────────────────────────────────
def list_disks(include_virtual: bool = False) -> list[DiskInfo]:
    """List all mounted disks/volumes.

    Args:
        include_virtual: Include tmpfs, devtmpfs, etc.
    """
    disks = []
    seen = set()
    virtual_fs = {"tmpfs", "devtmpfs", "proc", "sysfs", "debugfs", "securityfs",
                  "cgroup", "cgroup2", "fusectl", "overlay", "squashfs"}

    for part in psutil.disk_partitions(all=include_virtual):
        if part.mountpoint in seen:
            continue
        seen.add(part.mountpoint)

        if not include_virtual and part.fstype in virtual_fs:
            continue

        try:
            usage = psutil.disk_usage(part.mountpoint)
            label = _get_volume_label(part.mountpoint, part.device)
            disks.append(DiskInfo(
                mount_point=part.mountpoint,
                device=part.device,
                fstype=part.fstype,
                label=label,
                total=usage.total,
                used=usage.used,
                free=usage.free,
                percent=usage.percent,
                opts=part.opts,
            ))
        except (PermissionError, OSError):
            continue

    return disks


def _get_volume_label(mount_point: str, device: str) -> str:
    """Try to get a human-readable volume label."""
    system = platform.system()

    if system == "Darwin" and mount_point.startswith("/Volumes/"):
        return Path(mount_point).name

    if system == "Linux":
        try:
            result = subprocess.run(
                ["lsblk", "-no", "LABEL", device],
                capture_output=True, text=True, timeout=5,
            )
            label = result.stdout.strip()
            if label:
                return label
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

    if system == "Windows":
        try:
            # Use vol command or wmic
            drive = mount_point.rstrip("\\")
            result = subprocess.run(
                ["wmic", "logicaldisk", "where", f"DeviceID='{drive}'",
                 "get", "VolumeName", "/value"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                if "=" in line:
                    return line.split("=", 1)[1].strip()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

    return Path(mount_point).name or device


def get_disk_for_path(path: Path) -> Optional[DiskInfo]:
    """Find which disk contains the given path."""
    path = Path(path).resolve()
    best = None
    best_len = 0
    for disk in list_disks():
        mp = disk.mount_point
        try:
            path.relative_to(mp)
            if len(mp) > best_len:
                best = disk
                best_len = len(mp)
        except ValueError:
            continue
    return best


def free_space(path: Path) -> int:
    """Return free bytes on the volume containing *path*."""
    try:
        import shutil
        return shutil.disk_usage(path).free
    except OSError:
        return 0


def has_space(path: Path, needed: int) -> bool:
    """Check if the volume has enough free space."""
    return free_space(path) >= needed


# ── Mount/Unmount Event Detection ─────────────────────────────────────────────
# Callback: (event_type, disk_info) where event_type is "mounted" or "unmounted"
MountEventCB = Callable[[str, DiskInfo], None]


class MountWatcher:
    """Polls for volume mount/unmount events."""

    def __init__(self, interval: float = 2.0):
        self._interval = interval
        self._callbacks: list[MountEventCB] = []
        self._known: Set[str] = set()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_callback(self, cb: MountEventCB):
        self._callbacks.append(cb)

    def start(self):
        """Start polling for mount events."""
        self._known = {d.mount_point for d in list_disks()}
        self._running = True
        self._thread = threading.Thread(target=self._poll, daemon=True, name="mount-watcher")
        self._thread.start()
        logger.info("Mount watcher started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Mount watcher stopped")

    def _poll(self):
        while self._running:
            time.sleep(self._interval)
            try:
                current_disks = list_disks()
                current_mps = {d.mount_point for d in current_disks}
                disk_map = {d.mount_point: d for d in current_disks}

                # New mounts
                for mp in current_mps - self._known:
                    disk = disk_map[mp]
                    logger.info(f"Volume mounted: {disk.label} ({mp})")
                    for cb in self._callbacks:
                        try:
                            cb("mounted", disk)
                        except Exception as exc:
                            logger.error(f"Mount callback error: {exc}")

                # Unmounts
                for mp in self._known - current_mps:
                    # Create a minimal DiskInfo for the unmounted volume
                    disk = DiskInfo(
                        mount_point=mp, device="", fstype="",
                        label=Path(mp).name, total=0, used=0, free=0, percent=0,
                    )
                    logger.info(f"Volume unmounted: {mp}")
                    for cb in self._callbacks:
                        try:
                            cb("unmounted", disk)
                        except Exception as exc:
                            logger.error(f"Unmount callback error: {exc}")

                self._known = current_mps

            except Exception as exc:
                logger.error(f"Mount watcher poll error: {exc}")

    @property
    def known_mount_points(self) -> Set[str]:
        return set(self._known)

    @property
    def known_labels(self) -> Set[str]:
        disks = list_disks()
        return {d.label for d in disks if d.label}
