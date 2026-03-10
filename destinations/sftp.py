"""SFTP remote destination handler via paramiko.

Features:
- Chunked upload/download with progress callbacks
- Remote directory creation and listing
- Connection pooling and auto-reconnect
- Configurable timeout and keepalive
"""

from __future__ import annotations

import logging
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, Optional

from config import DEFAULT_CHUNK_SIZE, DestinationType

logger = logging.getLogger(__name__)

# Progress: (bytes_transferred, total_bytes)
SFTPProgressCB = Callable[[int, int], None]


@dataclass
class SFTPConfig:
    """Configuration for an SFTP destination."""
    host: str
    port: int = 22
    username: str = ""
    password: Optional[str] = None
    key_path: Optional[str] = None      # path to private key file
    remote_root: str = "/"
    timeout: float = 30.0
    keepalive_interval: int = 60
    chunk_size: int = DEFAULT_CHUNK_SIZE


class SFTPDestination:
    """Handler for SFTP remote destinations."""

    def __init__(self, config: SFTPConfig):
        self.config = config
        self.dest_type = DestinationType.SFTP
        self._transport = None
        self._sftp = None

    # -- Connection Management -------------------------------------------------
    def connect(self):
        """Establish SFTP connection."""
        import paramiko

        self._transport = paramiko.Transport((self.config.host, self.config.port))
        self._transport.set_keepalive(self.config.keepalive_interval)

        if self.config.key_path:
            key = paramiko.RSAKey.from_private_key_file(self.config.key_path)
            self._transport.connect(username=self.config.username, pkey=key)
        elif self.config.password:
            self._transport.connect(
                username=self.config.username,
                password=self.config.password,
            )
        else:
            raise ValueError("Either password or key_path must be provided")

        self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        self._sftp.get_channel().settimeout(self.config.timeout)
        logger.info(f"SFTP connected: {self.config.host}:{self.config.port}")

    def disconnect(self):
        """Close SFTP connection."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None
        logger.info("SFTP disconnected")

    def _ensure_connected(self):
        """Auto-reconnect if connection dropped."""
        if self._sftp is None or self._transport is None or not self._transport.is_active():
            self.connect()

    @property
    def sftp(self):
        self._ensure_connected()
        return self._sftp

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    # -- Path Helpers ----------------------------------------------------------
    def _remote_path(self, rel_path: str) -> str:
        return str(PurePosixPath(self.config.remote_root) / rel_path)

    # -- Directory Operations --------------------------------------------------
    def mkdir_p(self, remote_dir: str):
        """Recursively create remote directories."""
        parts = PurePosixPath(remote_dir).parts
        current = ""
        for part in parts:
            current = str(PurePosixPath(current) / part)
            try:
                self.sftp.stat(current)
            except FileNotFoundError:
                try:
                    self.sftp.mkdir(current)
                except OSError:
                    pass

    # -- Upload ----------------------------------------------------------------
    def upload_file(
        self,
        local_path: Path,
        rel_path: str,
        progress_cb: Optional[SFTPProgressCB] = None,
    ) -> dict:
        """Upload a file to SFTP with chunked transfer."""
        local_path = Path(local_path)
        remote = self._remote_path(rel_path)
        file_size = local_path.stat().st_size

        # Ensure remote directory exists
        remote_dir = str(PurePosixPath(remote).parent)
        self.mkdir_p(remote_dir)

        uploaded = [0]

        def _cb(sent, total):
            uploaded[0] = sent
            if progress_cb:
                progress_cb(sent, total)

        self.sftp.put(str(local_path), remote, callback=_cb)

        # Verify size
        remote_stat = self.sftp.stat(remote)
        return {
            "remote_path": remote,
            "local_size": file_size,
            "remote_size": remote_stat.st_size,
            "match": file_size == remote_stat.st_size,
        }

    # -- Download --------------------------------------------------------------
    def download_file(
        self,
        rel_path: str,
        local_path: Path,
        progress_cb: Optional[SFTPProgressCB] = None,
    ) -> Path:
        """Download a file from SFTP."""
        remote = self._remote_path(rel_path)
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        def _cb(sent, total):
            if progress_cb:
                progress_cb(sent, total)

        self.sftp.get(remote, str(local_path), callback=_cb)
        return local_path

    # -- List ------------------------------------------------------------------
    def list_files(self, rel_dir: str = "") -> list[dict]:
        """List files in a remote directory recursively."""
        remote = self._remote_path(rel_dir)
        results = []
        self._walk_remote(remote, rel_dir, results)
        return results

    def _walk_remote(self, remote_dir: str, rel_prefix: str, results: list):
        try:
            entries = self.sftp.listdir_attr(remote_dir)
        except (FileNotFoundError, OSError):
            return

        for entry in entries:
            remote_path = f"{remote_dir}/{entry.filename}"
            rel = f"{rel_prefix}/{entry.filename}" if rel_prefix else entry.filename

            if stat.S_ISDIR(entry.st_mode or 0):
                self._walk_remote(remote_path, rel, results)
            else:
                results.append({
                    "path": rel,
                    "size": entry.st_size,
                    "mtime": entry.st_mtime,
                })

    # -- Delete ----------------------------------------------------------------
    def delete_file(self, rel_path: str) -> bool:
        remote = self._remote_path(rel_path)
        try:
            self.sftp.remove(remote)
            return True
        except (FileNotFoundError, OSError):
            return False

    # -- Exists / Size ---------------------------------------------------------
    def file_exists(self, rel_path: str) -> bool:
        remote = self._remote_path(rel_path)
        try:
            self.sftp.stat(remote)
            return True
        except (FileNotFoundError, OSError):
            return False

    def file_size(self, rel_path: str) -> int:
        remote = self._remote_path(rel_path)
        try:
            return self.sftp.stat(remote).st_size
        except (FileNotFoundError, OSError):
            return 0
