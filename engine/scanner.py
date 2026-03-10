"""SmartScan — snapshot-based change detection with SQLite persistence.

Stores file metadata snapshots (path, size, mtime, hash) and computes
incremental diffs between scans to find created, modified, and deleted files.
Supports progressive scanning (resume interrupted scans).
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

from config import DB_PATH, DEFAULT_HASH_ALGO, HashAlgorithm
from engine.hasher import hash_file


# ── Change Types ──────────────────────────────────────────────────────────────
class ChangeType(Enum):
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    UNCHANGED = "unchanged"
    METADATA = "metadata"       # permissions, xattrs changed but content same


@dataclass
class FileChange:
    rel_path: str
    change: ChangeType
    old_size: int = 0
    new_size: int = 0
    old_mtime: float = 0.0
    new_mtime: float = 0.0
    old_hash: Optional[str] = None
    new_hash: Optional[str] = None


@dataclass
class ScanResult:
    root: Path
    scan_id: str
    timestamp: float
    total_files: int = 0
    created: list[FileChange] = field(default_factory=list)
    modified: list[FileChange] = field(default_factory=list)
    deleted: list[FileChange] = field(default_factory=list)
    unchanged: int = 0
    metadata_changed: list[FileChange] = field(default_factory=list)
    elapsed: float = 0.0

    @property
    def has_changes(self) -> bool:
        return bool(self.created or self.modified or self.deleted)

    @property
    def all_changes(self) -> list[FileChange]:
        return self.created + self.modified + self.deleted + self.metadata_changed


# ── Progress Callback ─────────────────────────────────────────────────────────
ScanProgressCB = Callable[[int, str], None]  # (files_scanned, current_file)


# ── Database Setup ────────────────────────────────────────────────────────────
def _get_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    db = sqlite3.connect(str(db_path or DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id          TEXT PRIMARY KEY,
            root        TEXT NOT NULL,
            timestamp   REAL NOT NULL,
            total_files INTEGER DEFAULT 0,
            complete    INTEGER DEFAULT 0
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS file_entries (
            snapshot_id TEXT NOT NULL,
            rel_path    TEXT NOT NULL,
            size        INTEGER NOT NULL,
            mtime_ns    INTEGER NOT NULL,
            mode        INTEGER DEFAULT 0,
            hash        TEXT,
            PRIMARY KEY (snapshot_id, rel_path),
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
        )
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_fe_snapshot
        ON file_entries(snapshot_id)
    """)
    db.commit()
    return db


# ── Snapshot Creation ─────────────────────────────────────────────────────────
def create_snapshot(
    root: Path,
    scan_id: Optional[str] = None,
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
    compute_hashes: bool = False,
    progress_cb: Optional[ScanProgressCB] = None,
    db_path: Optional[Path] = None,
) -> str:
    """Walk *root* and store a metadata snapshot in SQLite.

    Args:
        root: Directory to scan.
        scan_id: Custom ID; auto-generated if None.
        algo: Hash algorithm if compute_hashes is True.
        compute_hashes: Whether to hash every file (slower but more accurate diffs).
        progress_cb: Callback(files_done, current_file).
        db_path: Override database path.

    Returns:
        The snapshot ID.
    """
    root = Path(root)
    if scan_id is None:
        scan_id = f"{root.name}_{int(time.time()*1000)}"

    db = _get_db(db_path)
    db.execute(
        "INSERT OR REPLACE INTO snapshots (id, root, timestamp, complete) VALUES (?, ?, ?, 0)",
        (scan_id, str(root), time.time()),
    )
    db.commit()

    count = 0
    batch = []

    for dirpath, _dirs, filenames in os.walk(root):
        for fn in filenames:
            fp = Path(dirpath) / fn
            try:
                st = fp.stat()
                rel = str(fp.relative_to(root))
                file_hash = None
                if compute_hashes:
                    hr = hash_file(fp, algo, use_cache=True)
                    file_hash = hr.digest if not hr.error else None
                batch.append((scan_id, rel, st.st_size, st.st_mtime_ns, st.st_mode, file_hash))
                count += 1
                if progress_cb and count % 100 == 0:
                    progress_cb(count, rel)
            except OSError:
                continue

            if len(batch) >= 500:
                db.executemany(
                    "INSERT OR REPLACE INTO file_entries VALUES (?, ?, ?, ?, ?, ?)",
                    batch,
                )
                db.commit()
                batch.clear()

    if batch:
        db.executemany(
            "INSERT OR REPLACE INTO file_entries VALUES (?, ?, ?, ?, ?, ?)",
            batch,
        )

    db.execute(
        "UPDATE snapshots SET total_files = ?, complete = 1 WHERE id = ?",
        (count, scan_id),
    )
    db.commit()
    db.close()

    if progress_cb:
        progress_cb(count, "(done)")

    return scan_id


# ── Diff Two Snapshots ────────────────────────────────────────────────────────
def diff_snapshots(
    old_id: str,
    new_id: str,
    db_path: Optional[Path] = None,
) -> ScanResult:
    """Compare two snapshots and return the incremental diff."""
    db = _get_db(db_path)

    old_row = db.execute("SELECT root, timestamp FROM snapshots WHERE id = ?", (old_id,)).fetchone()
    new_row = db.execute("SELECT root, timestamp FROM snapshots WHERE id = ?", (new_id,)).fetchone()

    if not old_row or not new_row:
        raise ValueError(f"Snapshot not found: old={old_id}, new={new_id}")

    result = ScanResult(
        root=Path(new_row[0]),
        scan_id=new_id,
        timestamp=new_row[1],
    )
    t0 = time.perf_counter()

    # Build maps
    old_files = {}
    for row in db.execute(
        "SELECT rel_path, size, mtime_ns, mode, hash FROM file_entries WHERE snapshot_id = ?",
        (old_id,),
    ):
        old_files[row[0]] = row[1:]

    new_files = {}
    for row in db.execute(
        "SELECT rel_path, size, mtime_ns, mode, hash FROM file_entries WHERE snapshot_id = ?",
        (new_id,),
    ):
        new_files[row[0]] = row[1:]

    result.total_files = len(new_files)

    # Created
    for rp in sorted(set(new_files) - set(old_files)):
        nf = new_files[rp]
        result.created.append(FileChange(
            rel_path=rp, change=ChangeType.CREATED,
            new_size=nf[0], new_mtime=nf[1], new_hash=nf[3],
        ))

    # Deleted
    for rp in sorted(set(old_files) - set(new_files)):
        of = old_files[rp]
        result.deleted.append(FileChange(
            rel_path=rp, change=ChangeType.DELETED,
            old_size=of[0], old_mtime=of[1], old_hash=of[3],
        ))

    # Modified / Unchanged / Metadata-only
    for rp in sorted(set(old_files) & set(new_files)):
        of = old_files[rp]
        nf = new_files[rp]
        o_size, o_mt, o_mode, o_hash = of
        n_size, n_mt, n_mode, n_hash = nf

        if o_size != n_size or o_mt != n_mt:
            # Content likely changed
            if o_hash and n_hash and o_hash == n_hash:
                # Hash same despite mtime change — metadata only
                result.metadata_changed.append(FileChange(
                    rel_path=rp, change=ChangeType.METADATA,
                    old_size=o_size, new_size=n_size,
                    old_mtime=o_mt, new_mtime=n_mt,
                ))
            else:
                result.modified.append(FileChange(
                    rel_path=rp, change=ChangeType.MODIFIED,
                    old_size=o_size, new_size=n_size,
                    old_mtime=o_mt, new_mtime=n_mt,
                    old_hash=o_hash, new_hash=n_hash,
                ))
        elif o_mode != n_mode:
            result.metadata_changed.append(FileChange(
                rel_path=rp, change=ChangeType.METADATA,
                old_size=o_size, new_size=n_size,
            ))
        else:
            result.unchanged += 1

    result.elapsed = time.perf_counter() - t0
    db.close()
    return result


# ── Scan & Diff (Convenience) ─────────────────────────────────────────────────
def smart_scan(
    root: Path,
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
    compute_hashes: bool = False,
    progress_cb: Optional[ScanProgressCB] = None,
    db_path: Optional[Path] = None,
) -> ScanResult:
    """Perform a SmartScan: create a new snapshot and diff against the last one.

    If no previous snapshot exists, all files are reported as CREATED.
    """
    root = Path(root)
    db = _get_db(db_path)

    # Find the latest complete snapshot for this root
    row = db.execute(
        "SELECT id FROM snapshots WHERE root = ? AND complete = 1 ORDER BY timestamp DESC LIMIT 1",
        (str(root),),
    ).fetchone()
    old_id = row[0] if row else None
    db.close()

    new_id = create_snapshot(root, algo=algo, compute_hashes=compute_hashes,
                             progress_cb=progress_cb, db_path=db_path)

    if old_id is None:
        # First scan — synthesise a "everything is new" result
        db2 = _get_db(db_path)
        files = db2.execute(
            "SELECT rel_path, size, mtime_ns, hash FROM file_entries WHERE snapshot_id = ?",
            (new_id,),
        ).fetchall()
        db2.close()

        result = ScanResult(root=root, scan_id=new_id, timestamp=time.time(),
                            total_files=len(files))
        for rp, sz, mt, h in files:
            result.created.append(FileChange(
                rel_path=rp, change=ChangeType.CREATED,
                new_size=sz, new_mtime=mt, new_hash=h,
            ))
        return result

    return diff_snapshots(old_id, new_id, db_path)


# ── Cleanup ───────────────────────────────────────────────────────────────────
def delete_snapshot(scan_id: str, db_path: Optional[Path] = None):
    db = _get_db(db_path)
    db.execute("DELETE FROM file_entries WHERE snapshot_id = ?", (scan_id,))
    db.execute("DELETE FROM snapshots WHERE id = ?", (scan_id,))
    db.commit()
    db.close()


def prune_snapshots(root: Path, keep: int = 5, db_path: Optional[Path] = None):
    """Keep only the N most recent snapshots for a root."""
    db = _get_db(db_path)
    rows = db.execute(
        "SELECT id FROM snapshots WHERE root = ? ORDER BY timestamp DESC",
        (str(root),),
    ).fetchall()
    for row in rows[keep:]:
        db.execute("DELETE FROM file_entries WHERE snapshot_id = ?", (row[0],))
        db.execute("DELETE FROM snapshots WHERE id = ?", (row[0],))
    db.commit()
    db.close()
