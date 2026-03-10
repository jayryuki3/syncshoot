"""Post-transfer verification engine.

Verification modes:
- TRANSFER:           file-size comparison only (fastest)
- SOURCE:             compute source checksum, size-check destination
- SOURCE_DESTINATION: independently hash both sides, compare checksums + metadata

Also provides:
- Broken media detection (0-byte / truncated)
- Missing file inventory
- Batch verification against stored checksums
- Integration hooks for MHL generation
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from config import (
    DEFAULT_HASH_ALGO,
    MAX_PARALLEL_HASHES,
    FileOpStatus,
    HashAlgorithm,
    VerifyMode,
)
from engine.hasher import hash_file, compare_hashes, HashResult


# ── Result Containers ─────────────────────────────────────────────────────────
@dataclass
class FileVerifyResult:
    """Verification result for a single file."""
    rel_path: str
    src_path: Path
    dst_path: Path
    status: FileOpStatus = FileOpStatus.SUCCESS
    src_hash: Optional[str] = None
    dst_hash: Optional[str] = None
    src_size: int = 0
    dst_size: int = 0
    error: Optional[str] = None
    elapsed: float = 0.0


@dataclass
class VerifyReport:
    """Aggregate verification report for a transfer."""
    mode: VerifyMode
    algorithm: HashAlgorithm
    total_files: int = 0
    verified_ok: int = 0
    failed: int = 0
    corrupted: int = 0          # checksum mismatch
    truncated: int = 0          # size mismatch or 0-byte
    missing_source: int = 0
    missing_dest: int = 0
    skipped: int = 0
    elapsed: float = 0.0
    results: list[FileVerifyResult] = field(default_factory=list)

    # ── Convenience queries ───────────────────────────────────────────────
    @property
    def success_files(self) -> list[FileVerifyResult]:
        return [r for r in self.results if r.status == FileOpStatus.SUCCESS]

    @property
    def failed_files(self) -> list[FileVerifyResult]:
        return [r for r in self.results if r.status == FileOpStatus.FAILED]

    @property
    def corrupted_files(self) -> list[FileVerifyResult]:
        return [r for r in self.results if r.status == FileOpStatus.CORRUPTED]

    @property
    def truncated_files(self) -> list[FileVerifyResult]:
        return [r for r in self.results if r.status == FileOpStatus.TRUNCATED]

    @property
    def missing_source_files(self) -> list[FileVerifyResult]:
        return [r for r in self.results if r.status == FileOpStatus.MISSING_SOURCE]

    @property
    def missing_dest_files(self) -> list[FileVerifyResult]:
        return [r for r in self.results if r.status == FileOpStatus.MISSING_DEST]

    @property
    def all_passed(self) -> bool:
        return self.failed == 0 and self.corrupted == 0 and self.truncated == 0

    def summary_dict(self) -> dict:
        """Return a dict suitable for JSON serialisation / report generation."""
        return {
            "mode": self.mode.value,
            "algorithm": self.algorithm.value,
            "total_files": self.total_files,
            "verified_ok": self.verified_ok,
            "failed": self.failed,
            "corrupted": self.corrupted,
            "truncated": self.truncated,
            "missing_source": self.missing_source,
            "missing_dest": self.missing_dest,
            "skipped": self.skipped,
            "elapsed_seconds": round(self.elapsed, 3),
            "all_passed": self.all_passed,
        }


# ── Progress Callback ─────────────────────────────────────────────────────────
# verify_cb(files_done, total_files, current_file_rel_path)
VerifyProgressCB = Callable[[int, int, str], None]


# ── Single File Verification ──────────────────────────────────────────────────
def _verify_file(
    rel_path: str,
    src_root: Path,
    dst_root: Path,
    mode: VerifyMode,
    algo: HashAlgorithm,
) -> FileVerifyResult:
    """Verify a single file according to the chosen mode."""
    src = src_root / rel_path
    dst = dst_root / rel_path
    t0 = time.perf_counter()
    res = FileVerifyResult(rel_path=rel_path, src_path=src, dst_path=dst)

    # ── Existence checks ──────────────────────────────────────────────────
    if not src.exists():
        res.status = FileOpStatus.MISSING_SOURCE
        res.error = f"Source missing: {src}"
        res.elapsed = time.perf_counter() - t0
        return res

    if not dst.exists():
        res.status = FileOpStatus.MISSING_DEST
        res.error = f"Destination missing: {dst}"
        res.elapsed = time.perf_counter() - t0
        return res

    try:
        res.src_size = src.stat().st_size
        res.dst_size = dst.stat().st_size
    except OSError as exc:
        res.status = FileOpStatus.FAILED
        res.error = str(exc)
        res.elapsed = time.perf_counter() - t0
        return res

    # ── Broken media detection ────────────────────────────────────────────
    if res.dst_size == 0 and res.src_size > 0:
        res.status = FileOpStatus.TRUNCATED
        res.error = "Destination is 0 bytes (broken/empty copy)"
        res.elapsed = time.perf_counter() - t0
        return res

    if res.dst_size < res.src_size:
        res.status = FileOpStatus.TRUNCATED
        res.error = f"Destination truncated: {res.dst_size} < {res.src_size} bytes"
        res.elapsed = time.perf_counter() - t0
        return res

    # ── TRANSFER mode: size only ──────────────────────────────────────────
    if mode == VerifyMode.TRANSFER:
        if res.src_size != res.dst_size:
            res.status = FileOpStatus.CORRUPTED
            res.error = f"Size mismatch: src={res.src_size}, dst={res.dst_size}"
        else:
            res.status = FileOpStatus.SUCCESS
        res.elapsed = time.perf_counter() - t0
        return res

    # ── SOURCE mode: hash source, size-check dest ─────────────────────────
    src_hr = hash_file(src, algo)
    res.src_hash = src_hr.digest

    if src_hr.error:
        res.status = FileOpStatus.FAILED
        res.error = f"Source hash error: {src_hr.error}"
        res.elapsed = time.perf_counter() - t0
        return res

    if mode == VerifyMode.SOURCE:
        if res.src_size != res.dst_size:
            res.status = FileOpStatus.CORRUPTED
            res.error = f"Size mismatch: src={res.src_size}, dst={res.dst_size}"
        else:
            res.status = FileOpStatus.SUCCESS
        res.elapsed = time.perf_counter() - t0
        return res

    # ── SOURCE_DESTINATION mode: hash both, compare ───────────────────────
    dst_hr = hash_file(dst, algo)
    res.dst_hash = dst_hr.digest

    if dst_hr.error:
        res.status = FileOpStatus.FAILED
        res.error = f"Destination hash error: {dst_hr.error}"
        res.elapsed = time.perf_counter() - t0
        return res

    if not compare_hashes(src_hr, dst_hr):
        res.status = FileOpStatus.CORRUPTED
        res.error = (
            f"Checksum mismatch: src={src_hr.digest[:16]}... "
            f"dst={dst_hr.digest[:16]}..."
        )
    else:
        res.status = FileOpStatus.SUCCESS

    res.elapsed = time.perf_counter() - t0
    return res


# ── Batch Verification ────────────────────────────────────────────────────────
def verify_transfer(
    src_root: Path,
    dst_root: Path,
    rel_paths: list[str],
    mode: VerifyMode = VerifyMode.SOURCE_DESTINATION,
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
    max_workers: int = MAX_PARALLEL_HASHES,
    progress_cb: Optional[VerifyProgressCB] = None,
) -> VerifyReport:
    """Verify an entire transfer batch.

    Args:
        src_root: Source root directory.
        dst_root: Destination root directory.
        rel_paths: Relative file paths to verify.
        mode: Verification mode.
        algo: Hash algorithm for SOURCE / SOURCE_DESTINATION modes.
        max_workers: Parallel verification threads.
        progress_cb: Optional callback(done, total, current_file).

    Returns:
        VerifyReport with per-file results and aggregate counts.
    """
    report = VerifyReport(mode=mode, algorithm=algo, total_files=len(rel_paths))
    t0 = time.perf_counter()

    if mode == VerifyMode.NONE:
        report.skipped = len(rel_paths)
        report.elapsed = 0.0
        return report

    done = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {
            pool.submit(_verify_file, rp, src_root, dst_root, mode, algo): rp
            for rp in rel_paths
        }
        for fut in as_completed(futs):
            res = fut.result()
            report.results.append(res)

            if res.status == FileOpStatus.SUCCESS:
                report.verified_ok += 1
            elif res.status == FileOpStatus.CORRUPTED:
                report.corrupted += 1
            elif res.status == FileOpStatus.TRUNCATED:
                report.truncated += 1
            elif res.status == FileOpStatus.MISSING_SOURCE:
                report.missing_source += 1
            elif res.status == FileOpStatus.MISSING_DEST:
                report.missing_dest += 1
            elif res.status == FileOpStatus.FAILED:
                report.failed += 1
            elif res.status == FileOpStatus.SKIPPED:
                report.skipped += 1

            done += 1
            if progress_cb:
                progress_cb(done, len(rel_paths), res.rel_path)

    report.elapsed = time.perf_counter() - t0
    return report


# ── Inventory Check ───────────────────────────────────────────────────────────
def check_missing_files(
    src_root: Path,
    dst_root: Path,
) -> tuple[list[str], list[str]]:
    """Compare source and destination file inventories.

    Returns:
        (missing_on_dest, extra_on_dest) — lists of relative paths.
    """
    src_root, dst_root = Path(src_root), Path(dst_root)

    def _walk(root: Path) -> set[str]:
        out = set()
        for dirpath, _, filenames in root.walk() if hasattr(root, 'walk') else _os_walk(root):
            for fn in filenames:
                fp = Path(dirpath) / fn
                out.add(str(fp.relative_to(root)))
        return out

    def _os_walk(root):
        import os
        yield from os.walk(root)

    src_files = _walk(src_root)
    dst_files = _walk(dst_root)

    missing = sorted(src_files - dst_files)
    extra = sorted(dst_files - src_files)
    return missing, extra


# ── Batch Volume Verification ─────────────────────────────────────────────────
def verify_volume(
    volume_root: Path,
    checksums: dict[str, str],
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
    max_workers: int = MAX_PARALLEL_HASHES,
    progress_cb: Optional[VerifyProgressCB] = None,
) -> VerifyReport:
    """Verify all files on a volume against a dict of {rel_path: expected_digest}.

    Useful for MHL-based verification or re-checking a previously verified volume.
    """
    report = VerifyReport(
        mode=VerifyMode.SOURCE_DESTINATION,
        algorithm=algo,
        total_files=len(checksums),
    )
    t0 = time.perf_counter()
    done = 0

    def _check_one(rel_path: str, expected: str) -> FileVerifyResult:
        fp = volume_root / rel_path
        res = FileVerifyResult(
            rel_path=rel_path, src_path=fp, dst_path=fp,
        )
        if not fp.exists():
            res.status = FileOpStatus.MISSING_DEST
            res.error = f"File not found: {fp}"
            return res

        try:
            res.dst_size = fp.stat().st_size
        except OSError as exc:
            res.status = FileOpStatus.FAILED
            res.error = str(exc)
            return res

        if res.dst_size == 0:
            res.status = FileOpStatus.TRUNCATED
            res.error = "File is 0 bytes"
            return res

        hr = hash_file(fp, algo)
        res.dst_hash = hr.digest

        if hr.error:
            res.status = FileOpStatus.FAILED
            res.error = hr.error
        elif hr.digest != expected:
            res.status = FileOpStatus.CORRUPTED
            res.error = f"Expected {expected[:16]}..., got {hr.digest[:16]}..."
        else:
            res.status = FileOpStatus.SUCCESS

        return res

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {
            pool.submit(_check_one, rp, digest): rp
            for rp, digest in checksums.items()
        }
        for fut in as_completed(futs):
            res = fut.result()
            report.results.append(res)

            if res.status == FileOpStatus.SUCCESS:
                report.verified_ok += 1
            elif res.status == FileOpStatus.CORRUPTED:
                report.corrupted += 1
            elif res.status == FileOpStatus.TRUNCATED:
                report.truncated += 1
            elif res.status == FileOpStatus.MISSING_DEST:
                report.missing_dest += 1
            elif res.status == FileOpStatus.FAILED:
                report.failed += 1

            done += 1
            if progress_cb:
                progress_cb(done, len(checksums), res.rel_path)

    report.elapsed = time.perf_counter() - t0
    return report
