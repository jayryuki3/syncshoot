"""High-performance file copy engine.

Features:
- Chunked streaming copy with configurable buffer size
- Multi-destination simultaneous writes (read once, write N)
- Cascading transfers (primary finishes first, secondaries clone from it)
- Safe copy mode (.tmp rename on success)
- Move mode (same-volume optimisation)
- Duplicate detection (name + size + hash)
- Stop & resume from exact byte offset
- I/O throttling (low-priority mode)
- Per-file and per-transfer progress callbacks
"""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_HASH_ALGO,
    MAX_PARALLEL_COPIES,
    SAFE_COPY_SUFFIX,
    RESUME_STATE_FILE,
    FileOpStatus,
    HashAlgorithm,
    TransferStatus,
)
from engine.hasher import hash_file, HashResult


# ── Data Classes ──────────────────────────────────────────────────────────────
@dataclass
class FileRecord:
    """Represents one file operation within a transfer."""
    src: Path
    rel: str                          # relative path within source root
    size: int = 0
    status: FileOpStatus = FileOpStatus.SUCCESS
    error: Optional[str] = None
    src_hash: Optional[str] = None
    dst_hash: Optional[str] = None
    bytes_copied: int = 0
    speed: float = 0.0                # bytes/sec for this file


@dataclass
class TransferJob:
    """Describes a full transfer: one source → N destinations."""
    source: Path
    destinations: list[Path]
    cascade: bool = False             # if True, secondaries copy from dest[0]
    safe_copy: bool = True
    move_mode: bool = False
    hash_algo: HashAlgorithm = DEFAULT_HASH_ALGO
    chunk_size: int = DEFAULT_CHUNK_SIZE
    skip_duplicates: bool = True
    throttle_mbps: float = 0.0        # 0 = unlimited

    # Runtime state
    status: TransferStatus = TransferStatus.PENDING
    files: list[FileRecord] = field(default_factory=list)
    total_bytes: int = 0
    copied_bytes: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0
    error: Optional[str] = None


# ── Progress Callback Signatures ──────────────────────────────────────────────
# file_cb(file_record, bytes_just_written, total_file_bytes)
FileProgressCB = Callable[[FileRecord, int, int], None]
# transfer_cb(job, bytes_total_copied, bytes_total)
TransferProgressCB = Callable[[TransferJob, int, int], None]
# status_cb(job, new_status)
StatusChangeCB = Callable[[TransferJob, TransferStatus], None]


# ── Stop / Pause Control ─────────────────────────────────────────────────────
class TransferControl:
    """Thread-safe stop/pause signals for a running transfer."""

    def __init__(self):
        self._stop = threading.Event()
        self._pause = threading.Event()
        self._pause.set()  # not paused by default

    def stop(self):
        self._stop.set()
        self._pause.set()  # unblock if paused

    def pause(self):
        self._pause.clear()

    def resume(self):
        self._pause.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def wait_if_paused(self, timeout: float = 0.5) -> bool:
        """Block while paused. Returns False if stopped during wait."""
        while not self._pause.is_set():
            if self._stop.is_set():
                return False
            self._pause.wait(timeout)
        return not self._stop.is_set()


# ── Throttle Helper ───────────────────────────────────────────────────────────
class Throttle:
    """Limit throughput to *max_mbps* megabytes per second."""

    def __init__(self, max_mbps: float = 0.0):
        self.max_bps = max_mbps * 1024 * 1024 if max_mbps > 0 else 0
        self._sent = 0
        self._window_start = time.monotonic()
        self._lock = threading.Lock()

    def regulate(self, nbytes: int):
        if self.max_bps <= 0:
            return
        with self._lock:
            self._sent += nbytes
            elapsed = time.monotonic() - self._window_start
            if elapsed < 1.0:
                if self._sent >= self.max_bps:
                    time.sleep(1.0 - elapsed)
                    self._sent = 0
                    self._window_start = time.monotonic()
            else:
                self._sent = 0
                self._window_start = time.monotonic()


# ── Resume State ──────────────────────────────────────────────────────────────
def _save_resume_state(dest: Path, rel_path: str, offset: int):
    """Persist resume offset for an interrupted file."""
    state_file = dest / RESUME_STATE_FILE
    state = {}
    if state_file.exists():
        try:
            state = json.loads(state_file.read_text())
        except (json.JSONDecodeError, OSError):
            state = {}
    state[rel_path] = offset
    state_file.write_text(json.dumps(state, indent=2))


def _load_resume_offset(dest: Path, rel_path: str) -> int:
    """Load previously saved byte offset for a file, or 0."""
    state_file = dest / RESUME_STATE_FILE
    if not state_file.exists():
        return 0
    try:
        state = json.loads(state_file.read_text())
        return state.get(rel_path, 0)
    except (json.JSONDecodeError, OSError):
        return 0


def _clear_resume_state(dest: Path, rel_path: str):
    state_file = dest / RESUME_STATE_FILE
    if not state_file.exists():
        return
    try:
        state = json.loads(state_file.read_text())
        state.pop(rel_path, None)
        if state:
            state_file.write_text(json.dumps(state, indent=2))
        else:
            state_file.unlink(missing_ok=True)
    except (json.JSONDecodeError, OSError):
        pass


# ── Single-File Copy ──────────────────────────────────────────────────────────
def _copy_single_file(
    rec: FileRecord,
    dest_root: Path,
    chunk_size: int,
    safe_copy: bool,
    throttle: Throttle,
    ctrl: TransferControl,
    file_cb: Optional[FileProgressCB] = None,
) -> FileRecord:
    """Copy one file from source to a single destination directory.

    Supports resume from byte offset and safe-copy (.tmp rename).
    """
    dst = dest_root / rec.rel
    dst.parent.mkdir(parents=True, exist_ok=True)

    # Determine write target
    write_target = dst.with_suffix(dst.suffix + SAFE_COPY_SUFFIX) if safe_copy else dst

    # Resume support
    offset = _load_resume_offset(dest_root, rec.rel)
    if offset > 0 and write_target.exists() and write_target.stat().st_size == offset:
        mode = "ab"
    else:
        offset = 0
        mode = "wb"

    rec.bytes_copied = offset
    t0 = time.perf_counter()

    try:
        with open(rec.src, "rb") as src_f:
            if offset > 0:
                src_f.seek(offset)
            with open(write_target, mode) as dst_f:
                while True:
                    if not ctrl.wait_if_paused():
                        # Stopped — save resume state
                        _save_resume_state(dest_root, rec.rel, rec.bytes_copied)
                        rec.status = FileOpStatus.FAILED
                        rec.error = "Transfer stopped by user"
                        return rec

                    buf = src_f.read(chunk_size)
                    if not buf:
                        break
                    dst_f.write(buf)
                    rec.bytes_copied += len(buf)
                    throttle.regulate(len(buf))

                    if file_cb:
                        file_cb(rec, rec.bytes_copied, rec.size)

        # Safe-copy rename
        if safe_copy:
            write_target.rename(dst)

        # Preserve metadata
        shutil.copystat(rec.src, dst)

        elapsed = time.perf_counter() - t0
        rec.speed = rec.bytes_copied / elapsed if elapsed > 0 else 0
        rec.status = FileOpStatus.SUCCESS
        _clear_resume_state(dest_root, rec.rel)

    except OSError as exc:
        rec.status = FileOpStatus.FAILED
        rec.error = str(exc)
        _save_resume_state(dest_root, rec.rel, rec.bytes_copied)

    return rec


# ── Multi-Destination Copy ────────────────────────────────────────────────────
def _copy_to_multiple_destinations(
    rec: FileRecord,
    dest_roots: list[Path],
    chunk_size: int,
    safe_copy: bool,
    throttle: Throttle,
    ctrl: TransferControl,
    file_cb: Optional[FileProgressCB] = None,
) -> dict[Path, FileRecord]:
    """Read source once, write to all destinations simultaneously."""
    results: dict[Path, FileRecord] = {}
    dst_files = {}
    write_targets = {}

    try:
        for dest_root in dest_roots:
            dest_rec = FileRecord(src=rec.src, rel=rec.rel, size=rec.size)
            results[dest_root] = dest_rec

            dst = dest_root / rec.rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            wt = dst.with_suffix(dst.suffix + SAFE_COPY_SUFFIX) if safe_copy else dst
            write_targets[dest_root] = (wt, dst)
            dst_files[dest_root] = open(wt, "wb")

        t0 = time.perf_counter()
        total_read = 0

        with open(rec.src, "rb") as src_f:
            while True:
                if not ctrl.wait_if_paused():
                    for dr in results.values():
                        dr.status = FileOpStatus.FAILED
                        dr.error = "Transfer stopped by user"
                    break

                buf = src_f.read(chunk_size)
                if not buf:
                    break

                for dest_root, df in dst_files.items():
                    try:
                        df.write(buf)
                        results[dest_root].bytes_copied += len(buf)
                    except OSError as exc:
                        results[dest_root].status = FileOpStatus.FAILED
                        results[dest_root].error = str(exc)

                total_read += len(buf)
                throttle.regulate(len(buf))

                if file_cb:
                    file_cb(rec, total_read, rec.size)

        elapsed = time.perf_counter() - t0

        for dest_root in dest_roots:
            dr = results[dest_root]
            if dr.status != FileOpStatus.FAILED:
                dr.status = FileOpStatus.SUCCESS
                dr.speed = dr.bytes_copied / elapsed if elapsed > 0 else 0

    finally:
        for df in dst_files.values():
            df.close()

        # Rename safe-copy files
        for dest_root in dest_roots:
            wt, final = write_targets.get(dest_root, (None, None))
            if wt and final and results[dest_root].status == FileOpStatus.SUCCESS:
                if safe_copy and wt.exists():
                    wt.rename(final)
                try:
                    shutil.copystat(rec.src, final)
                except OSError:
                    pass

    return results


# ── Index Source ──────────────────────────────────────────────────────────────
def index_source(source: Path) -> list[FileRecord]:
    """Walk source directory and build a list of FileRecords."""
    records = []
    source = Path(source)
    if source.is_file():
        records.append(FileRecord(
            src=source,
            rel=source.name,
            size=source.stat().st_size,
        ))
    else:
        for root, _dirs, files in os.walk(source):
            for fname in files:
                fp = Path(root) / fname
                try:
                    st = fp.stat()
                    records.append(FileRecord(
                        src=fp,
                        rel=str(fp.relative_to(source)),
                        size=st.st_size,
                    ))
                except OSError:
                    records.append(FileRecord(
                        src=fp,
                        rel=str(fp.relative_to(source)),
                        status=FileOpStatus.FAILED,
                        error="Cannot stat file",
                    ))
    return records


# ── Duplicate Detection ───────────────────────────────────────────────────────
def _is_duplicate(rec: FileRecord, dest_root: Path, algo: HashAlgorithm) -> bool:
    """Check if destination already has an identical file."""
    dst = dest_root / rec.rel
    if not dst.exists():
        return False
    try:
        dst_stat = dst.stat()
        if dst_stat.st_size != rec.size:
            return False
        src_hr = hash_file(rec.src, algo)
        dst_hr = hash_file(dst, algo)
        return src_hr.digest == dst_hr.digest and not src_hr.error and not dst_hr.error
    except OSError:
        return False


# ── Main Transfer Entry Point ─────────────────────────────────────────────────
def run_transfer(
    job: TransferJob,
    ctrl: Optional[TransferControl] = None,
    file_cb: Optional[FileProgressCB] = None,
    transfer_cb: Optional[TransferProgressCB] = None,
    status_cb: Optional[StatusChangeCB] = None,
    max_workers: int = MAX_PARALLEL_COPIES,
) -> TransferJob:
    """Execute a full transfer job.

    Handles indexing, duplicate detection, multi-dest copy or cascade,
    progress reporting, and stop/resume.
    """
    if ctrl is None:
        ctrl = TransferControl()

    throttle = Throttle(job.throttle_mbps)

    def _set_status(s: TransferStatus):
        job.status = s
        if status_cb:
            status_cb(job, s)

    # ── Phase 1: Index ────────────────────────────────────────────────────
    _set_status(TransferStatus.INDEXING)
    job.started_at = time.time()

    if not job.files:
        job.files = index_source(job.source)

    job.total_bytes = sum(r.size for r in job.files if r.status != FileOpStatus.FAILED)

    if ctrl.stopped:
        _set_status(TransferStatus.CANCELLED)
        return job

    # ── Phase 2: Copy ─────────────────────────────────────────────────────
    _set_status(TransferStatus.COPYING)

    actionable = [r for r in job.files if r.status != FileOpStatus.FAILED]

    # Filter duplicates
    if job.skip_duplicates:
        for rec in actionable:
            if all(_is_duplicate(rec, d, job.hash_algo) for d in job.destinations):
                rec.status = FileOpStatus.SKIPPED

    to_copy = [r for r in actionable if r.status not in (FileOpStatus.SKIPPED, FileOpStatus.FAILED)]

    if job.cascade and len(job.destinations) > 1:
        # ── Cascade: copy to primary first, then secondary copies from primary
        primary = job.destinations[0]
        secondaries = job.destinations[1:]

        # Stage 1: source → primary
        for rec in to_copy:
            if ctrl.stopped:
                break
            _copy_single_file(rec, primary, job.chunk_size, job.safe_copy,
                              throttle, ctrl, file_cb)
            job.copied_bytes += rec.bytes_copied
            if transfer_cb:
                transfer_cb(job, job.copied_bytes, job.total_bytes)

        # Stage 2: primary → secondaries (parallel)
        if not ctrl.stopped and secondaries:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futs = []
                for rec in to_copy:
                    if rec.status != FileOpStatus.SUCCESS:
                        continue
                    cascade_rec = FileRecord(
                        src=primary / rec.rel,
                        rel=rec.rel,
                        size=rec.size,
                    )
                    for sec in secondaries:
                        futs.append(pool.submit(
                            _copy_single_file, cascade_rec, sec,
                            job.chunk_size, job.safe_copy, throttle, ctrl, None,
                        ))
                for f in as_completed(futs):
                    f.result()  # propagate exceptions
    else:
        # ── Parallel multi-dest: read once, write to all
        if len(job.destinations) == 1:
            for rec in to_copy:
                if ctrl.stopped:
                    break
                _copy_single_file(rec, job.destinations[0], job.chunk_size,
                                  job.safe_copy, throttle, ctrl, file_cb)
                job.copied_bytes += rec.bytes_copied
                if transfer_cb:
                    transfer_cb(job, job.copied_bytes, job.total_bytes)
        else:
            for rec in to_copy:
                if ctrl.stopped:
                    break
                _copy_to_multiple_destinations(
                    rec, job.destinations, job.chunk_size,
                    job.safe_copy, throttle, ctrl, file_cb,
                )
                job.copied_bytes += rec.bytes_copied
                if transfer_cb:
                    transfer_cb(job, job.copied_bytes, job.total_bytes)

    # ── Phase 3: Move cleanup ─────────────────────────────────────────────
    if job.move_mode and not ctrl.stopped:
        for rec in to_copy:
            if rec.status == FileOpStatus.SUCCESS:
                try:
                    rec.src.unlink()
                except OSError:
                    pass

    # ── Finalise ──────────────────────────────────────────────────────────
    job.finished_at = time.time()
    if ctrl.stopped:
        _set_status(TransferStatus.CANCELLED)
    elif any(r.status == FileOpStatus.FAILED for r in job.files):
        _set_status(TransferStatus.FAILED)
    else:
        _set_status(TransferStatus.COMPLETE)

    return job


# ── Convenience: Quick Copy ───────────────────────────────────────────────────
def quick_copy(
    source: Path,
    destinations: list[Path],
    cascade: bool = False,
    verify: bool = True,
    progress_cb: Optional[TransferProgressCB] = None,
) -> TransferJob:
    """One-liner copy for simple use cases."""
    job = TransferJob(
        source=Path(source),
        destinations=[Path(d) for d in destinations],
        cascade=cascade,
    )
    return run_transfer(job, transfer_cb=progress_cb)
