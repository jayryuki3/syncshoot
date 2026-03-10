"""Job scheduler — APScheduler wrapper for SyncShoot.

Trigger types:
- CRON:         Full cron expression (minute, hour, day, month, weekday)
- INTERVAL:     Every N minutes/hours/days
- VOLUME_MOUNT: Run task when a specific volume is connected
- APP_LAUNCH:   Run task on SyncShoot startup (with configurable delay)

Features:
- Schedule persistence in SQLite (survives restarts)
- Missed schedule recovery
- Per-schedule enable/disable
- Notification hooks
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from config import (
    DB_PATH,
    MISSED_SCHEDULE_GRACE,
    ScheduleTrigger,
)

logger = logging.getLogger(__name__)

# Task callback: (schedule_id, trigger_type) -> None
TaskCallback = Callable[[str, ScheduleTrigger], None]


# ── Schedule Definition ───────────────────────────────────────────────────────
@dataclass
class ScheduleConfig:
    """Describes a scheduled job."""
    schedule_id: str
    task_name: str
    trigger: ScheduleTrigger
    enabled: bool = True

    # Cron fields (for CRON trigger)
    cron_expression: str = ""       # "30 2 * * 1-5" = 2:30 AM weekdays

    # Interval fields (for INTERVAL trigger)
    interval_seconds: int = 0

    # Volume mount fields (for VOLUME_MOUNT trigger)
    volume_label: str = ""
    volume_uuid: str = ""

    # App launch fields (for APP_LAUNCH trigger)
    launch_delay_seconds: int = 10

    # Metadata
    last_run: float = 0.0
    next_run: float = 0.0
    run_count: int = 0
    last_status: str = ""
    created_at: float = field(default_factory=time.time)


# ── Database Helpers ──────────────────────────────────────────────────────────
def _get_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    db = sqlite3.connect(str(db_path or DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""
        CREATE TABLE IF NOT EXISTS schedules (
            schedule_id     TEXT PRIMARY KEY,
            task_name       TEXT NOT NULL,
            trigger_type    TEXT NOT NULL,
            enabled         INTEGER DEFAULT 1,
            cron_expression TEXT DEFAULT '',
            interval_seconds INTEGER DEFAULT 0,
            volume_label    TEXT DEFAULT '',
            volume_uuid     TEXT DEFAULT '',
            launch_delay    INTEGER DEFAULT 10,
            last_run        REAL DEFAULT 0,
            next_run        REAL DEFAULT 0,
            run_count       INTEGER DEFAULT 0,
            last_status     TEXT DEFAULT '',
            created_at      REAL DEFAULT 0
        )
    """)
    db.commit()
    return db


def _config_to_row(cfg: ScheduleConfig) -> tuple:
    return (
        cfg.schedule_id, cfg.task_name, cfg.trigger.value, int(cfg.enabled),
        cfg.cron_expression, cfg.interval_seconds,
        cfg.volume_label, cfg.volume_uuid, cfg.launch_delay_seconds,
        cfg.last_run, cfg.next_run, cfg.run_count, cfg.last_status,
        cfg.created_at,
    )


def _row_to_config(row) -> ScheduleConfig:
    return ScheduleConfig(
        schedule_id=row[0], task_name=row[1],
        trigger=ScheduleTrigger(row[2]), enabled=bool(row[3]),
        cron_expression=row[4], interval_seconds=row[5],
        volume_label=row[6], volume_uuid=row[7],
        launch_delay_seconds=row[8],
        last_run=row[9], next_run=row[10], run_count=row[11],
        last_status=row[12], created_at=row[13],
    )


# ── Cron Parser ───────────────────────────────────────────────────────────────
def _parse_cron_field(field_str: str, min_val: int, max_val: int) -> list[int]:
    """Parse a single cron field into a list of matching integers."""
    values = set()
    for part in field_str.split(","):
        part = part.strip()
        if part == "*":
            values.update(range(min_val, max_val + 1))
        elif "/" in part:
            base, step = part.split("/", 1)
            start = min_val if base == "*" else int(base)
            for v in range(start, max_val + 1, int(step)):
                values.add(v)
        elif "-" in part:
            lo, hi = part.split("-", 1)
            values.update(range(int(lo), int(hi) + 1))
        else:
            values.add(int(part))
    return sorted(values)


def cron_matches_now(expression: str) -> bool:
    """Check if a cron expression matches the current time (minute resolution)."""
    parts = expression.strip().split()
    if len(parts) != 5:
        return False

    import datetime
    now = datetime.datetime.now()

    fields = [
        (parts[0], 0, 59, now.minute),
        (parts[1], 0, 23, now.hour),
        (parts[2], 1, 31, now.day),
        (parts[3], 1, 12, now.month),
        (parts[4], 0, 6, now.weekday()),  # 0=Mon in Python, adjust if needed
    ]

    for field_str, min_v, max_v, current in fields:
        allowed = _parse_cron_field(field_str, min_v, max_v)
        if current not in allowed:
            return False
    return True


# ── Scheduler Manager ─────────────────────────────────────────────────────────
class Scheduler:
    """Manages scheduled tasks with persistence."""

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = db_path or DB_PATH
        self._callbacks: dict[str, TaskCallback] = {}
        self._running = False

    # ── CRUD ──────────────────────────────────────────────────────────────
    def add_schedule(self, config: ScheduleConfig) -> ScheduleConfig:
        """Add or update a schedule."""
        db = _get_db(self._db_path)
        db.execute(
            """INSERT OR REPLACE INTO schedules VALUES
               (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            _config_to_row(config),
        )
        db.commit()
        db.close()
        logger.info(f"Schedule saved: {config.schedule_id} ({config.trigger.value})")
        return config

    def remove_schedule(self, schedule_id: str):
        db = _get_db(self._db_path)
        db.execute("DELETE FROM schedules WHERE schedule_id = ?", (schedule_id,))
        db.commit()
        db.close()
        self._callbacks.pop(schedule_id, None)
        logger.info(f"Schedule removed: {schedule_id}")

    def get_schedule(self, schedule_id: str) -> Optional[ScheduleConfig]:
        db = _get_db(self._db_path)
        row = db.execute(
            "SELECT * FROM schedules WHERE schedule_id = ?", (schedule_id,)
        ).fetchone()
        db.close()
        return _row_to_config(row) if row else None

    def list_schedules(self, enabled_only: bool = False) -> list[ScheduleConfig]:
        db = _get_db(self._db_path)
        if enabled_only:
            rows = db.execute("SELECT * FROM schedules WHERE enabled = 1").fetchall()
        else:
            rows = db.execute("SELECT * FROM schedules").fetchall()
        db.close()
        return [_row_to_config(r) for r in rows]

    def toggle_schedule(self, schedule_id: str, enabled: bool):
        db = _get_db(self._db_path)
        db.execute(
            "UPDATE schedules SET enabled = ? WHERE schedule_id = ?",
            (int(enabled), schedule_id),
        )
        db.commit()
        db.close()

    # ── Callback Registration ─────────────────────────────────────────────
    def register_callback(self, schedule_id: str, callback: TaskCallback):
        """Register a function to call when this schedule fires."""
        self._callbacks[schedule_id] = callback

    def register_global_callback(self, callback: TaskCallback):
        """Register a fallback callback for any schedule without a specific one."""
        self._callbacks["__global__"] = callback

    # ── Execution ─────────────────────────────────────────────────────────
    def fire(self, schedule_id: str):
        """Manually trigger a schedule."""
        config = self.get_schedule(schedule_id)
        if config is None:
            logger.warning(f"Schedule not found: {schedule_id}")
            return

        cb = self._callbacks.get(schedule_id) or self._callbacks.get("__global__")
        if cb:
            try:
                cb(schedule_id, config.trigger)
                self._record_run(schedule_id, "success")
            except Exception as exc:
                logger.error(f"Schedule {schedule_id} failed: {exc}")
                self._record_run(schedule_id, f"error: {exc}")
        else:
            logger.warning(f"No callback for schedule: {schedule_id}")

    def _record_run(self, schedule_id: str, status: str):
        db = _get_db(self._db_path)
        db.execute(
            """UPDATE schedules SET
               last_run = ?, run_count = run_count + 1, last_status = ?
               WHERE schedule_id = ?""",
            (time.time(), status, schedule_id),
        )
        db.commit()
        db.close()

    # ── Cron Check (called by timer/loop) ─────────────────────────────────
    def check_cron_schedules(self):
        """Check all cron schedules and fire matching ones. Call once per minute."""
        for config in self.list_schedules(enabled_only=True):
            if config.trigger == ScheduleTrigger.CRON and config.cron_expression:
                if cron_matches_now(config.cron_expression):
                    logger.info(f"Cron match: {config.schedule_id}")
                    self.fire(config.schedule_id)

    # ── Interval Check ────────────────────────────────────────────────────
    def check_interval_schedules(self):
        """Check all interval schedules and fire due ones."""
        now = time.time()
        for config in self.list_schedules(enabled_only=True):
            if config.trigger == ScheduleTrigger.INTERVAL and config.interval_seconds > 0:
                if now - config.last_run >= config.interval_seconds:
                    logger.info(f"Interval due: {config.schedule_id}")
                    self.fire(config.schedule_id)

    # ── Volume Mount Check ────────────────────────────────────────────────
    def check_volume_mount(self, mounted_labels: set[str], mounted_uuids: set[str]):
        """Check if any volume-mount schedules should fire."""
        for config in self.list_schedules(enabled_only=True):
            if config.trigger == ScheduleTrigger.VOLUME_MOUNT:
                label_match = config.volume_label and config.volume_label in mounted_labels
                uuid_match = config.volume_uuid and config.volume_uuid in mounted_uuids
                if label_match or uuid_match:
                    logger.info(f"Volume mount trigger: {config.schedule_id}")
                    self.fire(config.schedule_id)

    # ── App Launch ────────────────────────────────────────────────────────
    def run_app_launch_schedules(self):
        """Fire all APP_LAUNCH schedules (call once at startup)."""
        import threading
        for config in self.list_schedules(enabled_only=True):
            if config.trigger == ScheduleTrigger.APP_LAUNCH:
                delay = config.launch_delay_seconds

                def _delayed_fire(sid=config.schedule_id):
                    time.sleep(delay)
                    self.fire(sid)

                t = threading.Thread(target=_delayed_fire, daemon=True)
                t.start()

    # ── Missed Schedule Recovery ──────────────────────────────────────────
    def recover_missed(self):
        """Fire schedules that were missed while app was closed (within grace period)."""
        now = time.time()
        for config in self.list_schedules(enabled_only=True):
            if config.trigger in (ScheduleTrigger.CRON, ScheduleTrigger.INTERVAL):
                if config.last_run > 0:
                    gap = now - config.last_run
                    expected = config.interval_seconds or 60
                    if gap > expected and gap < MISSED_SCHEDULE_GRACE:
                        logger.info(f"Recovering missed schedule: {config.schedule_id}")
                        self.fire(config.schedule_id)
