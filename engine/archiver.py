"""Versioned archive manager with retention policies.

When files are deleted or replaced during sync/mirror operations,
they are moved to an archive folder instead of permanent removal.

Features:
- Configurable archive directory
- Retention by count (keep N versions) and/or age (keep for N days)
- Optional gzip compression
- Browse, search, and restore archived versions
- Archive maintenance (prune, relocate)
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from config import (
    ARCHIVE_DEFAULT_DIR,
    ARCHIVE_MAX_VERSIONS,
    ARCHIVE_MAX_AGE_DAYS,
    ARCHIVE_COMPRESS,
)


# ── Archive Entry ─────────────────────────────────────────────────────────────
@dataclass
class ArchiveEntry:
    """One archived version of a file."""
    rel_path: str               # original relative path
    archive_path: Path          # physical location in archive
    version: int
    archived_at: float          # timestamp
    original_size: int
    archived_size: int          # may differ if compressed
    compressed: bool = False
    original_mtime: float = 0.0
    metadata: dict = field(default_factory=dict)


@dataclass
class ArchiveManifest:
    """Manifest tracking all archived files for a task/root."""
    root_name: str
    archive_dir: Path
    entries: dict[str, list[ArchiveEntry]] = field(default_factory=dict)  # rel_path -> versions

    @property
    def total_files(self) -> int:
        return sum(len(v) for v in self.entries.values())

    @property
    def total_size(self) -> int:
        return sum(e.archived_size for versions in self.entries.values() for e in versions)

    @property
    def unique_files(self) -> int:
        return len(self.entries)


# ── Archive Manager ───────────────────────────────────────────────────────────
class ArchiveManager:
    """Manages file archiving, retrieval, and retention."""

    def __init__(
        self,
        archive_dir: Optional[Path] = None,
        max_versions: int = ARCHIVE_MAX_VERSIONS,
        max_age_days: int = ARCHIVE_MAX_AGE_DAYS,
        compress: bool = ARCHIVE_COMPRESS,
    ):
        self.archive_dir = Path(archive_dir or ARCHIVE_DEFAULT_DIR)
        self.max_versions = max_versions
        self.max_age_days = max_age_days
        self.compress = compress
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self.archive_dir / "manifest.json"
        self._manifest = self._load_manifest()

    # ── Manifest Persistence ──────────────────────────────────────────────
    def _load_manifest(self) -> ArchiveManifest:
        if self._manifest_path.exists():
            try:
                data = json.loads(self._manifest_path.read_text())
                manifest = ArchiveManifest(
                    root_name=data.get("root_name", ""),
                    archive_dir=self.archive_dir,
                )
                for rp, versions in data.get("entries", {}).items():
                    manifest.entries[rp] = [
                        ArchiveEntry(
                            rel_path=rp,
                            archive_path=Path(v["archive_path"]),
                            version=v["version"],
                            archived_at=v["archived_at"],
                            original_size=v["original_size"],
                            archived_size=v["archived_size"],
                            compressed=v.get("compressed", False),
                            original_mtime=v.get("original_mtime", 0.0),
                            metadata=v.get("metadata", {}),
                        )
                        for v in versions
                    ]
                return manifest
            except (json.JSONDecodeError, KeyError, OSError):
                pass
        return ArchiveManifest(root_name="", archive_dir=self.archive_dir)

    def _save_manifest(self):
        data = {
            "root_name": self._manifest.root_name,
            "entries": {},
        }
        for rp, versions in self._manifest.entries.items():
            data["entries"][rp] = [
                {
                    "archive_path": str(e.archive_path),
                    "version": e.version,
                    "archived_at": e.archived_at,
                    "original_size": e.original_size,
                    "archived_size": e.archived_size,
                    "compressed": e.compressed,
                    "original_mtime": e.original_mtime,
                    "metadata": e.metadata,
                }
                for e in versions
            ]
        self._manifest_path.write_text(json.dumps(data, indent=2))

    # ── Archive a File ────────────────────────────────────────────────────
    def archive(self, file_path: Path, rel_path: str, root_name: str = "") -> ArchiveEntry:
        """Archive a file before it's overwritten or deleted.

        Args:
            file_path: Absolute path to the file to archive.
            rel_path: Relative path (used as key in manifest).
            root_name: Task or source root identifier.

        Returns:
            The ArchiveEntry for the newly archived version.
        """
        self._manifest.root_name = root_name or self._manifest.root_name
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"Cannot archive: {file_path}")

        stat = file_path.stat()
        versions = self._manifest.entries.get(rel_path, [])
        version_num = (max(e.version for e in versions) + 1) if versions else 1

        # Build archive sub-path: archive_dir / rel_path_dir / filename.vN[.gz]
        archive_name = f"{file_path.stem}.v{version_num}{file_path.suffix}"
        if self.compress:
            archive_name += ".gz"
        archive_subdir = self.archive_dir / Path(rel_path).parent
        archive_subdir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_subdir / archive_name

        # Copy or compress
        if self.compress:
            with open(file_path, "rb") as f_in:
                with gzip.open(archive_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(file_path, archive_path)

        archived_size = archive_path.stat().st_size

        entry = ArchiveEntry(
            rel_path=rel_path,
            archive_path=archive_path,
            version=version_num,
            archived_at=time.time(),
            original_size=stat.st_size,
            archived_size=archived_size,
            compressed=self.compress,
            original_mtime=stat.st_mtime,
        )

        if rel_path not in self._manifest.entries:
            self._manifest.entries[rel_path] = []
        self._manifest.entries[rel_path].append(entry)

        self._save_manifest()
        self._enforce_retention(rel_path)
        return entry

    # ── Restore ───────────────────────────────────────────────────────────
    def restore(
        self,
        rel_path: str,
        version: Optional[int] = None,
        restore_to: Optional[Path] = None,
    ) -> Path:
        """Restore an archived file version.

        Args:
            rel_path: The original relative path.
            version: Version number to restore (latest if None).
            restore_to: Custom restore location (original location if None).

        Returns:
            Path where the file was restored.
        """
        versions = self._manifest.entries.get(rel_path, [])
        if not versions:
            raise FileNotFoundError(f"No archived versions for: {rel_path}")

        if version is not None:
            entry = next((e for e in versions if e.version == version), None)
            if entry is None:
                raise FileNotFoundError(f"Version {version} not found for: {rel_path}")
        else:
            entry = versions[-1]  # latest

        if not entry.archive_path.exists():
            raise FileNotFoundError(f"Archive file missing: {entry.archive_path}")

        if restore_to is None:
            restore_to = Path(rel_path)
        else:
            restore_to = Path(restore_to)

        restore_to.parent.mkdir(parents=True, exist_ok=True)

        if entry.compressed:
            with gzip.open(entry.archive_path, "rb") as f_in:
                with open(restore_to, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(entry.archive_path, restore_to)

        return restore_to

    # ── Browse / Search ───────────────────────────────────────────────────
    def list_archived_files(self) -> list[str]:
        """Return all unique relative paths that have archived versions."""
        return sorted(self._manifest.entries.keys())

    def get_versions(self, rel_path: str) -> list[ArchiveEntry]:
        """Get all archived versions for a file."""
        return self._manifest.entries.get(rel_path, [])

    def search(self, pattern: str) -> list[str]:
        """Search archived file paths by glob pattern."""
        import fnmatch
        return [rp for rp in self._manifest.entries if fnmatch.fnmatch(rp, pattern)]

    # ── Retention Enforcement ─────────────────────────────────────────────
    def _enforce_retention(self, rel_path: str):
        """Apply retention policy to a specific file's versions."""
        versions = self._manifest.entries.get(rel_path, [])
        if not versions:
            return

        now = time.time()
        max_age_secs = self.max_age_days * 86400

        # Remove by age
        to_keep = []
        for entry in versions:
            if max_age_secs > 0 and (now - entry.archived_at) > max_age_secs:
                self._delete_archive_file(entry)
            else:
                to_keep.append(entry)

        # Remove by count (keep newest N)
        if self.max_versions > 0 and len(to_keep) > self.max_versions:
            to_remove = to_keep[:-self.max_versions]
            to_keep = to_keep[-self.max_versions:]
            for entry in to_remove:
                self._delete_archive_file(entry)

        self._manifest.entries[rel_path] = to_keep
        if not to_keep:
            del self._manifest.entries[rel_path]

        self._save_manifest()

    def _delete_archive_file(self, entry: ArchiveEntry):
        """Delete the physical archive file."""
        try:
            if entry.archive_path.exists():
                entry.archive_path.unlink()
        except OSError:
            pass

    # ── Bulk Maintenance ──────────────────────────────────────────────────
    def prune(self):
        """Apply retention policies to ALL archived files."""
        for rp in list(self._manifest.entries.keys()):
            self._enforce_retention(rp)

    def clear(self):
        """Delete all archived files and reset manifest."""
        for versions in self._manifest.entries.values():
            for entry in versions:
                self._delete_archive_file(entry)
        self._manifest.entries.clear()
        self._save_manifest()

    def relocate(self, new_dir: Path):
        """Move entire archive to a new directory."""
        new_dir = Path(new_dir)
        new_dir.mkdir(parents=True, exist_ok=True)
        for rp, versions in self._manifest.entries.items():
            for entry in versions:
                if entry.archive_path.exists():
                    new_path = new_dir / entry.archive_path.relative_to(self.archive_dir)
                    new_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(entry.archive_path), str(new_path))
                    entry.archive_path = new_path
        self.archive_dir = new_dir
        self._manifest.archive_dir = new_dir
        self._manifest_path = new_dir / "manifest.json"
        self._save_manifest()

    @property
    def manifest(self) -> ArchiveManifest:
        return self._manifest
