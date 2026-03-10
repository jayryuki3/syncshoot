"""Real-time file monitoring via watchdog.

Features:
- Filesystem event observation for source directories
- Grace delay: wait N seconds after last change before triggering sync
- Max delay cap: force sync after N seconds even if changes keep coming
- Fallback interval: periodic sync if filesystem events not detected
- Multi-directory monitoring with independent settings
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from config import (
    WATCHER_GRACE_DELAY,
    WATCHER_MAX_DELAY,
    WATCHER_FALLBACK_INTERVAL,
)

logger = logging.getLogger(__name__)

# Sync trigger callback: (watch_id, root_path) -> None
WatchCallback = Callable[[str, Path], None]


# ── Watch Configuration ───────────────────────────────────────────────────────
@dataclass
class WatchConfig:
    """Configuration for monitoring one directory."""
    watch_id: str
    path: Path
    recursive: bool = True
    grace_delay: float = WATCHER_GRACE_DELAY
    max_delay: float = WATCHER_MAX_DELAY
    fallback_interval: float = WATCHER_FALLBACK_INTERVAL
    enabled: bool = True
    ignore_patterns: list[str] = field(default_factory=lambda: [
        "*.syncshoot_tmp", ".syncshoot_resume.json", ".DS_Store", "Thumbs.db",
    ])


# ── Debounced Trigger ─────────────────────────────────────────────────────────
class DebouncedTrigger:
    """Debounce filesystem events with grace delay and max delay cap."""

    def __init__(
        self,
        watch_id: str,
        grace_delay: float,
        max_delay: float,
        callback: WatchCallback,
        path: Path,
    ):
        self.watch_id = watch_id
        self.grace_delay = grace_delay
        self.max_delay = max_delay
        self.callback = callback
        self.path = path

        self._last_event: float = 0.0
        self._first_event: float = 0.0
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def on_event(self):
        """Called when a filesystem event is detected."""
        with self._lock:
            now = time.monotonic()
            self._last_event = now

            if self._first_event == 0.0:
                self._first_event = now

            # Cancel pending timer
            if self._timer is not None:
                self._timer.cancel()

            # Check max delay cap
            if self.max_delay > 0 and (now - self._first_event) >= self.max_delay:
                self._fire()
                return

            # Set new grace timer
            self._timer = threading.Timer(self.grace_delay, self._grace_expired)
            self._timer.daemon = True
            self._timer.start()

    def _grace_expired(self):
        """Called when grace period expires without new events."""
        with self._lock:
            self._fire()

    def _fire(self):
        """Execute the callback and reset state."""
        self._first_event = 0.0
        self._last_event = 0.0
        self._timer = None

        try:
            self.callback(self.watch_id, self.path)
        except Exception as exc:
            logger.error(f"Watch callback error [{self.watch_id}]: {exc}")

    def cancel(self):
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None


# ── File Watcher Manager ──────────────────────────────────────────────────────
class FileWatcher:
    """Manages real-time file monitoring for multiple directories."""

    def __init__(self):
        self._watches: dict[str, WatchConfig] = {}
        self._triggers: dict[str, DebouncedTrigger] = {}
        self._observers: dict[str, Any] = {}
        self._fallback_threads: dict[str, threading.Thread] = {}
        self._callback: Optional[WatchCallback] = None
        self._running = False

    def set_callback(self, callback: WatchCallback):
        """Set the global callback for all watches."""
        self._callback = callback

    def add_watch(self, config: WatchConfig):
        """Add a directory to monitor."""
        self._watches[config.watch_id] = config
        logger.info(f"Watch added: {config.watch_id} -> {config.path}")

    def remove_watch(self, watch_id: str):
        """Stop and remove a watch."""
        self.stop_watch(watch_id)
        self._watches.pop(watch_id, None)
        logger.info(f"Watch removed: {watch_id}")

    def start_watch(self, watch_id: str):
        """Start monitoring a specific directory."""
        config = self._watches.get(watch_id)
        if not config or not config.enabled or not self._callback:
            return

        # Create debounced trigger
        trigger = DebouncedTrigger(
            watch_id=config.watch_id,
            grace_delay=config.grace_delay,
            max_delay=config.max_delay,
            callback=self._callback,
            path=config.path,
        )
        self._triggers[watch_id] = trigger

        # Try watchdog observer
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            class _Handler(FileSystemEventHandler):
                def __init__(self, trig: DebouncedTrigger, ignores: list[str]):
                    self._trigger = trig
                    self._ignores = ignores

                def _should_ignore(self, path: str) -> bool:
                    import fnmatch
                    name = Path(path).name
                    return any(fnmatch.fnmatch(name, p) for p in self._ignores)

                def on_any_event(self, event):
                    if event.is_directory:
                        return
                    if self._should_ignore(event.src_path):
                        return
                    self._trigger.on_event()

            handler = _Handler(trigger, config.ignore_patterns)
            observer = Observer()
            observer.schedule(handler, str(config.path), recursive=config.recursive)
            observer.daemon = True
            observer.start()
            self._observers[watch_id] = observer
            logger.info(f"Watchdog observer started: {watch_id}")

        except ImportError:
            logger.warning("watchdog not installed, using fallback polling")

        # Fallback interval polling (always runs as safety net)
        if config.fallback_interval > 0:
            self._start_fallback(watch_id, config)

    def stop_watch(self, watch_id: str):
        """Stop monitoring a specific directory."""
        trigger = self._triggers.pop(watch_id, None)
        if trigger:
            trigger.cancel()

        observer = self._observers.pop(watch_id, None)
        if observer:
            observer.stop()
            observer.join(timeout=5)

        # Signal fallback thread to stop
        self._fallback_threads.pop(watch_id, None)

        logger.info(f"Watch stopped: {watch_id}")

    def start_all(self):
        """Start monitoring all configured watches."""
        self._running = True
        for watch_id in self._watches:
            self.start_watch(watch_id)

    def stop_all(self):
        """Stop all monitoring."""
        self._running = False
        for watch_id in list(self._observers):
            self.stop_watch(watch_id)

    def _start_fallback(self, watch_id: str, config: WatchConfig):
        """Start a fallback polling thread."""
        def _poll():
            while self._running and watch_id in self._watches:
                time.sleep(config.fallback_interval)
                if watch_id in self._triggers and self._callback:
                    # Only fire if no recent watchdog events
                    trigger = self._triggers.get(watch_id)
                    if trigger and trigger._first_event == 0.0:
                        logger.debug(f"Fallback poll fire: {watch_id}")
                        self._callback(watch_id, config.path)

        t = threading.Thread(target=_poll, daemon=True, name=f"fallback-{watch_id}")
        t.start()
        self._fallback_threads[watch_id] = t

    @property
    def active_watches(self) -> list[str]:
        return list(self._observers.keys())

    @property
    def watch_configs(self) -> dict[str, WatchConfig]:
        return dict(self._watches)


# Typing import for observer
from typing import Any
