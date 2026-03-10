"""Sync engine — 5 modes with dry-run (Trial Sync) support.

Modes:
- BACKUP:        Left -> Right, copy new/modified, leave non-duplicates alone
- BLIND_BACKUP:  Same as backup but no state tracking between runs
- MIRROR:        Make destination identical to source (deletes extras on dest)
- BIDIRECTIONAL: Merge changes from both sides, newest-modified wins
- MOVE:          Relocate files (same-volume optimisation)

All modes support dry_run=True to return planned operations without executing.
"""

from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

from config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_HASH_ALGO,
    SyncMode,
    HashAlgorithm,
    FileOpStatus,
    TransferStatus,
)
from engine.hasher import hash_file
from engine.copier import TransferControl, FileRecord


# ── Planned Operation ─────────────────────────────────────────────────────────
class OpType(Enum):
    COPY = "copy"
    REPLACE = "replace"
    DELETE = "delete"
    SKIP = "skip"
    CONFLICT = "conflict"
    MOVE = "move"


@dataclass
class PlannedOp:
    """A single planned file operation for Trial Sync preview."""
    rel_path: str
    op: OpType
    direction: str = "left_to_right"    # or "right_to_left"
    src_size: int = 0
    dst_size: int = 0
    src_mtime: float = 0.0
    dst_mtime: float = 0.0
    reason: str = ""
    override: Optional[OpType] = None   # user can override in Trial Sync UI

    @property
    def effective_op(self) -> OpType:
        return self.override if self.override is not None else self.op


@dataclass
class SyncPlan:
    """Complete plan of operations for a sync task."""
    mode: SyncMode
    source: Path
    destination: Path
    operations: list[PlannedOp] = field(default_factory=list)
    total_copy_bytes: int = 0
    total_delete_count: int = 0
    elapsed_planning: float = 0.0

    @property
    def copies(self) -> list[PlannedOp]:
        return [o for o in self.operations if o.effective_op in (OpType.COPY, OpType.REPLACE)]

    @property
    def deletes(self) -> list[PlannedOp]:
        return [o for o in self.operations if o.effective_op == OpType.DELETE]

    @property
    def skips(self) -> list[PlannedOp]:
        return [o for o in self.operations if o.effective_op == OpType.SKIP]

    @property
    def conflicts(self) -> list[PlannedOp]:
        return [o for o in self.operations if o.effective_op == OpType.CONFLICT]


@dataclass
class SyncResult:
    """Outcome of executing a sync plan."""
    plan: SyncPlan
    status: TransferStatus = TransferStatus.COMPLETE
    files_copied: int = 0
    files_deleted: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    bytes_transferred: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)  # (rel_path, error_msg)
    elapsed: float = 0.0


# ── Progress Callback ─────────────────────────────────────────────────────────
SyncProgressCB = Callable[[int, int, str, str], None]  # (done, total, rel_path, op_type)


# ── File Comparison Helpers ───────────────────────────────────────────────────
def _file_meta(path: Path) -> tuple[int, float]:
    """Return (size, mtime_ns) or (0, 0) if missing."""
    try:
        st = path.stat()
        return st.st_size, st.st_mtime_ns
    except OSError:
        return 0, 0.0


def _walk_rel(root: Path) -> dict[str, tuple[int, float]]:
    """Walk directory, return {rel_path: (size, mtime_ns)}."""
    out = {}
    root = Path(root)
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            fp = Path(dirpath) / fn
            try:
                st = fp.stat()
                out[str(fp.relative_to(root))] = (st.st_size, st.st_mtime_ns)
            except OSError:
                continue
    return out


# ── Plan Generation ───────────────────────────────────────────────────────────
def plan_sync(
    source: Path,
    destination: Path,
    mode: SyncMode,
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
) -> SyncPlan:
    """Generate a sync plan (dry-run / Trial Sync).

    Compares source and destination, returns planned operations
    without executing anything.
    """
    source, destination = Path(source), Path(destination)
    plan = SyncPlan(mode=mode, source=source, destination=destination)
    t0 = time.perf_counter()

    src_files = _walk_rel(source)
    dst_files = _walk_rel(destination)
    all_paths = sorted(set(src_files) | set(dst_files))

    for rp in all_paths:
        in_src = rp in src_files
        in_dst = rp in dst_files
        s_size, s_mt = src_files.get(rp, (0, 0.0))
        d_size, d_mt = dst_files.get(rp, (0, 0.0))

        if mode == SyncMode.BACKUP or mode == SyncMode.BLIND_BACKUP:
            if in_src and not in_dst:
                plan.operations.append(PlannedOp(
                    rel_path=rp, op=OpType.COPY, src_size=s_size,
                    src_mtime=s_mt, reason="New file",
                ))
                plan.total_copy_bytes += s_size
            elif in_src and in_dst:
                if s_size != d_size or s_mt > d_mt:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.REPLACE, src_size=s_size,
                        dst_size=d_size, src_mtime=s_mt, dst_mtime=d_mt,
                        reason="Modified (size or mtime changed)",
                    ))
                    plan.total_copy_bytes += s_size
                else:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.SKIP, src_size=s_size,
                        dst_size=d_size, reason="Unchanged",
                    ))
            # Backup modes don't delete extra files on dest

        elif mode == SyncMode.MIRROR:
            if in_src and not in_dst:
                plan.operations.append(PlannedOp(
                    rel_path=rp, op=OpType.COPY, src_size=s_size,
                    src_mtime=s_mt, reason="New file",
                ))
                plan.total_copy_bytes += s_size
            elif in_src and in_dst:
                if s_size != d_size or s_mt > d_mt:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.REPLACE, src_size=s_size,
                        dst_size=d_size, src_mtime=s_mt, dst_mtime=d_mt,
                        reason="Modified",
                    ))
                    plan.total_copy_bytes += s_size
                else:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.SKIP, src_size=s_size,
                        dst_size=d_size, reason="Unchanged",
                    ))
            elif not in_src and in_dst:
                plan.operations.append(PlannedOp(
                    rel_path=rp, op=OpType.DELETE, dst_size=d_size,
                    dst_mtime=d_mt, reason="Not in source (mirror cleanup)",
                ))
                plan.total_delete_count += 1

        elif mode == SyncMode.BIDIRECTIONAL:
            if in_src and not in_dst:
                plan.operations.append(PlannedOp(
                    rel_path=rp, op=OpType.COPY, direction="left_to_right",
                    src_size=s_size, src_mtime=s_mt, reason="New on source",
                ))
                plan.total_copy_bytes += s_size
            elif not in_src and in_dst:
                plan.operations.append(PlannedOp(
                    rel_path=rp, op=OpType.COPY, direction="right_to_left",
                    src_size=d_size, src_mtime=d_mt,
                    reason="New on destination",
                ))
                plan.total_copy_bytes += d_size
            elif in_src and in_dst:
                if s_size == d_size and s_mt == d_mt:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.SKIP, reason="Identical",
                    ))
                elif s_mt > d_mt:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.REPLACE, direction="left_to_right",
                        src_size=s_size, dst_size=d_size,
                        src_mtime=s_mt, dst_mtime=d_mt,
                        reason="Source newer",
                    ))
                    plan.total_copy_bytes += s_size
                elif d_mt > s_mt:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.REPLACE, direction="right_to_left",
                        src_size=d_size, dst_size=s_size,
                        src_mtime=d_mt, dst_mtime=s_mt,
                        reason="Destination newer",
                    ))
                    plan.total_copy_bytes += d_size
                else:
                    plan.operations.append(PlannedOp(
                        rel_path=rp, op=OpType.CONFLICT,
                        src_size=s_size, dst_size=d_size,
                        src_mtime=s_mt, dst_mtime=d_mt,
                        reason="Same mtime, different size",
                    ))

        elif mode == SyncMode.MOVE:
            if in_src:
                plan.operations.append(PlannedOp(
                    rel_path=rp, op=OpType.MOVE, src_size=s_size,
                    src_mtime=s_mt, reason="Move to destination",
                ))
                plan.total_copy_bytes += s_size

    plan.elapsed_planning = time.perf_counter() - t0
    return plan


# ── Execute Plan ──────────────────────────────────────────────────────────────
def execute_sync(
    plan: SyncPlan,
    ctrl: Optional[TransferControl] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_cb: Optional[SyncProgressCB] = None,
) -> SyncResult:
    """Execute a previously generated sync plan.

    Respects per-operation overrides set by the Trial Sync UI.
    """
    if ctrl is None:
        ctrl = TransferControl()

    result = SyncResult(plan=plan)
    t0 = time.perf_counter()
    total = len(plan.operations)

    for idx, op in enumerate(plan.operations):
        if ctrl.stopped:
            result.status = TransferStatus.CANCELLED
            break

        ctrl.wait_if_paused()
        effective = op.effective_op

        if progress_cb:
            progress_cb(idx + 1, total, op.rel_path, effective.value)

        if effective == OpType.SKIP:
            result.files_skipped += 1
            continue

        try:
            if effective in (OpType.COPY, OpType.REPLACE):
                if op.direction == "right_to_left":
                    src_path = plan.destination / op.rel_path
                    dst_path = plan.source / op.rel_path
                else:
                    src_path = plan.source / op.rel_path
                    dst_path = plan.destination / op.rel_path

                dst_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_path, dst_path)
                result.files_copied += 1
                result.bytes_transferred += op.src_size

            elif effective == OpType.DELETE:
                target = plan.destination / op.rel_path
                if target.exists():
                    target.unlink()
                    result.files_deleted += 1
                    # Clean empty parent dirs
                    parent = target.parent
                    while parent != plan.destination:
                        try:
                            parent.rmdir()
                            parent = parent.parent
                        except OSError:
                            break

            elif effective == OpType.MOVE:
                src_path = plan.source / op.rel_path
                dst_path = plan.destination / op.rel_path
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                # Try rename first (same-volume), fall back to copy+delete
                try:
                    src_path.rename(dst_path)
                except OSError:
                    shutil.copy2(src_path, dst_path)
                    src_path.unlink()
                result.files_copied += 1
                result.bytes_transferred += op.src_size

            elif effective == OpType.CONFLICT:
                result.files_skipped += 1  # conflicts not auto-resolved

        except OSError as exc:
            result.files_failed += 1
            result.errors.append((op.rel_path, str(exc)))

    result.elapsed = time.perf_counter() - t0
    if result.files_failed > 0 and result.status != TransferStatus.CANCELLED:
        result.status = TransferStatus.FAILED
    elif result.status != TransferStatus.CANCELLED:
        result.status = TransferStatus.COMPLETE

    return result


# ── Convenience: Dry-Run ──────────────────────────────────────────────────────
def trial_sync(
    source: Path,
    destination: Path,
    mode: SyncMode = SyncMode.BACKUP,
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
) -> SyncPlan:
    """Generate a Trial Sync plan (alias for plan_sync)."""
    return plan_sync(source, destination, mode, algo)


# ── Convenience: Full Sync ────────────────────────────────────────────────────
def run_sync(
    source: Path,
    destination: Path,
    mode: SyncMode = SyncMode.BACKUP,
    dry_run: bool = False,
    progress_cb: Optional[SyncProgressCB] = None,
) -> SyncPlan | SyncResult:
    """Plan and optionally execute a sync.

    If dry_run=True, returns the SyncPlan without executing.
    Otherwise, returns the SyncResult.
    """
    plan = plan_sync(source, destination, mode)
    if dry_run:
        return plan
    return execute_sync(plan, progress_cb=progress_cb)
