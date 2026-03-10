"""Streaming checksum engine with concurrent hashing and progress callbacks.

Supports XXH3-64 (default, fastest), XXH64, MD5, SHA1.
Thread-safe, designed for parallel multi-file hashing.
"""

from __future__ import annotations

import hashlib
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import xxhash

from config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_HASH_ALGO,
    MAX_PARALLEL_HASHES,
    HashAlgorithm,
)


# ── Result Container ─────────────────────────────────────────────────────────────────
@dataclass
class HashResult:
    path: Path
    algorithm: HashAlgorithm
    digest: str
    size: int
    elapsed: float = 0.0
    error: Optional[str] = None


# ── Progress Callback Signature ───────────────────────────────────────────────────────
# callback(path, bytes_processed, total_bytes)
ProgressCallback = Callable[[Path, int, int], None]


# ── Hash Cache ────────────────────────────────────────────────────────────────────
class HashCache:
    """Thread-safe LRU-ish cache keyed on (path, mtime, size, algo)."""

    def __init__(self, max_size: int = 4096):
        self._store: dict[tuple, str] = {}
        self._lock = threading.Lock()
        self._max = max_size

    def _key(self, path: Path, algo: HashAlgorithm) -> tuple:
        st = path.stat()
        return (str(path), st.st_mtime_ns, st.st_size, algo.value)

    def get(self, path: Path, algo: HashAlgorithm) -> Optional[str]:
        try:
            k = self._key(path, algo)
        except OSError:
            return None
        with self._lock:
            return self._store.get(k)

    def put(self, path: Path, algo: HashAlgorithm, digest: str) -> None:
        try:
            k = self._key(path, algo)
        except OSError:
            return
        with self._lock:
            if len(self._store) >= self._max:
                oldest = next(iter(self._store))
                del self._store[oldest]
            self._store[k] = digest

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# ── Global Cache Instance ───────────────────────────────────────────────────────────
_cache = HashCache()


# ── Factory ─────────────────────────────────────────────────────────────────────
def _new_hasher(algo: HashAlgorithm):
    """Return a fresh hash object for the given algorithm."""
    if algo == HashAlgorithm.XXH3_64:
        return xxhash.xxh3_64()
    elif algo == HashAlgorithm.XXH64:
        return xxhash.xxh64()
    elif algo == HashAlgorithm.MD5:
        return hashlib.md5()
    elif algo == HashAlgorithm.SHA1:
        return hashlib.sha1()
    raise ValueError(f"Unsupported algorithm: {algo}")


# ── Single File Hash ──────────────────────────────────────────────────────────────────
def hash_file(
    path: Path,
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_cb: Optional[ProgressCallback] = None,
    use_cache: bool = True,
) -> HashResult:
    """Compute checksum for a single file with streaming reads.

    Args:
        path: File to hash.
        algo: Hash algorithm to use.
        chunk_size: Read buffer size in bytes.
        progress_cb: Optional callback(path, bytes_done, total_bytes).
        use_cache: Whether to check/store in the global hash cache.

    Returns:
        HashResult with digest string or error message.
    """
    import time

    path = Path(path)
    if not path.is_file():
        return HashResult(path=path, algorithm=algo, digest="", size=0,
                          error=f"Not a file: {path}")

    total = path.stat().st_size

    # Cache check
    if use_cache:
        cached = _cache.get(path, algo)
        if cached is not None:
            if progress_cb:
                progress_cb(path, total, total)
            return HashResult(path=path, algorithm=algo, digest=cached, size=total)

    h = _new_hasher(algo)
    done = 0
    t0 = time.perf_counter()

    try:
        with open(path, "rb") as f:
            while True:
                buf = f.read(chunk_size)
                if not buf:
                    break
                h.update(buf)
                done += len(buf)
                if progress_cb:
                    progress_cb(path, done, total)
    except OSError as exc:
        return HashResult(path=path, algorithm=algo, digest="", size=total,
                          elapsed=time.perf_counter() - t0,
                          error=str(exc))

    digest = h.hexdigest()
    elapsed = time.perf_counter() - t0

    if use_cache:
        _cache.put(path, algo, digest)

    return HashResult(path=path, algorithm=algo, digest=digest, size=total,
                      elapsed=elapsed)


# ── Batch Parallel Hash ────────────────────────────────────────────────────────────────
def hash_files(
    paths: list[Path],
    algo: HashAlgorithm = DEFAULT_HASH_ALGO,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_workers: int = MAX_PARALLEL_HASHES,
    progress_cb: Optional[ProgressCallback] = None,
    use_cache: bool = True,
) -> list[HashResult]:
    """Hash multiple files concurrently using a thread pool.

    Args:
        paths: List of files to hash.
        algo: Hash algorithm.
        chunk_size: Read buffer size.
        max_workers: Thread pool size.
        progress_cb: Per-file progress callback.
        use_cache: Use the global hash cache.

    Returns:
        List of HashResult in the same order as input paths.
    """
    results: dict[int, HashResult] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(hash_file, p, algo, chunk_size, progress_cb, use_cache): idx
            for idx, p in enumerate(paths)
        }
        for fut in as_completed(future_map):
            idx = future_map[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                results[idx] = HashResult(
                    path=paths[idx], algorithm=algo, digest="",
                    size=0, error=str(exc),
                )

    return [results[i] for i in range(len(paths))]


# ── Convenience ───────────────────────────────────────────────────────────────────
def compare_hashes(result_a: HashResult, result_b: HashResult) -> bool:
    """Return True if two hash results match (same algo, same digest, no errors)."""
    if result_a.error or result_b.error:
        return False
    if result_a.algorithm != result_b.algorithm:
        return False
    return result_a.digest == result_b.digest


def clear_cache() -> None:
    """Clear the global hash cache."""
    _cache.clear()
