"""SyncShoot — App-wide configuration, constants, and enums."""

import os
import enum
from pathlib import Path

# ── App Meta ─────────────────────────────────────────────────────────────────────────
APP_NAME = "SyncShoot"
APP_VERSION = "0.1.0"
APP_ORG = "SyncShoot"

# ── Paths ───────────────────────────────────────────────────────────────────────────
CONFIG_DIR = Path(os.environ.get("SYNCSHOOT_CONFIG", Path.home() / ".syncshoot"))
LOG_DIR = CONFIG_DIR / "logs"
DB_PATH = CONFIG_DIR / "syncshoot.db"
ARCHIVE_DEFAULT_DIR = CONFIG_DIR / "archive"
TASKS_DIR = CONFIG_DIR / "tasks"
SCHEDULES_DIR = CONFIG_DIR / "schedules"
REPORTS_DIR = CONFIG_DIR / "reports"

for _d in (CONFIG_DIR, LOG_DIR, ARCHIVE_DEFAULT_DIR, TASKS_DIR, SCHEDULES_DIR, REPORTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ── Enums ───────────────────────────────────────────────────────────────────────────
class SyncMode(enum.Enum):
    BACKUP = "backup"
    BLIND_BACKUP = "blind_backup"
    MIRROR = "mirror"
    BIDIRECTIONAL = "bidirectional"
    MOVE = "move"


class VerifyMode(enum.Enum):
    NONE = "none"
    TRANSFER = "transfer"
    SOURCE = "source"
    SOURCE_DESTINATION = "source_dest"


class HashAlgorithm(enum.Enum):
    XXH3_64 = "xxh3_64"
    XXH64 = "xxh64"
    MD5 = "md5"
    SHA1 = "sha1"


class TransferStatus(enum.Enum):
    PENDING = "pending"
    INDEXING = "indexing"
    COPYING = "copying"
    VERIFYING = "verifying"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class FileOpStatus(enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CORRUPTED = "corrupted"
    TRUNCATED = "truncated"
    MISSING_SOURCE = "missing_source"
    MISSING_DEST = "missing_dest"


class FilterMode(enum.Enum):
    SIMPLE = "simple"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"


class ScheduleTrigger(enum.Enum):
    CRON = "cron"
    INTERVAL = "interval"
    VOLUME_MOUNT = "volume_mount"
    APP_LAUNCH = "app_launch"
    FILE_CHANGE = "file_change"


class DestinationType(enum.Enum):
    LOCAL = "local"
    S3 = "s3"
    SFTP = "sftp"


class ReportFormat(enum.Enum):
    HTML = "html"
    CSV = "csv"
    JSON = "json"


# ── Copy Engine Defaults ────────────────────────────────────────────────────────────
DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024
MAX_CHUNK_SIZE = 16 * 1024 * 1024
DEFAULT_HASH_ALGO = HashAlgorithm.XXH3_64
DEFAULT_VERIFY_MODE = VerifyMode.SOURCE_DESTINATION
MAX_PARALLEL_COPIES = 4
MAX_PARALLEL_HASHES = 8
SAFE_COPY_SUFFIX = ".syncshoot_tmp"
RESUME_STATE_FILE = ".syncshoot_resume.json"

# ── S3 Defaults ───────────────────────────────────────────────────────────────────
S3_MULTIPART_THRESHOLD = 100 * 1024 * 1024
S3_MULTIPART_CHUNKSIZE = 25 * 1024 * 1024

# ── Scheduler Defaults ──────────────────────────────────────────────────────────────
WATCHER_GRACE_DELAY = 5
WATCHER_MAX_DELAY = 60
WATCHER_FALLBACK_INTERVAL = 300
MISSED_SCHEDULE_GRACE = 3600

# ── Archive Defaults ────────────────────────────────────────────────────────────────
ARCHIVE_MAX_VERSIONS = 10
ARCHIVE_MAX_AGE_DAYS = 90
ARCHIVE_COMPRESS = False

# ── GUI Defaults ──────────────────────────────────────────────────────────────────
SPEED_AVERAGE_WINDOW = 5
PROGRESS_UPDATE_INTERVAL = 250

# ── Theme Colors (hex) ──────────────────────────────────────────────────────────────
COLORS = {
    "indexing": "#9E9E9E",
    "copying": "#2196F3",
    "verifying": "#64B5F6",
    "complete": "#4CAF50",
    "failed": "#F44336",
    "corrupted": "#FF9800",
    "paused": "#FFC107",
    "cancelled": "#795548",
}

# ── Notification Defaults ───────────────────────────────────────────────────────────
NOTIFICATION_SOUND = True
NOTIFICATION_SYSTEM = True
NOTIFICATION_EMAIL = False