# SyncShoot

A high-performance file transfer, sync, and backup application combining the best features of Hedge Offshoot and ChronoSync.

## Features

### From Offshoot
- Blazing-fast multi-destination parallel file copy
- Checksum verification (XXH3-64, XXH64, MD5, SHA1)
- Cascading transfers (copy to fast drive, then clone from there)
- ASC MHL (Media Hash List) generation
- S3 cloud destination support
- Live transfer dashboard with speed/ETA/progress

### From ChronoSync
- 5 sync modes (Backup, Mirror, Bidirectional, Blind Backup, Move)
- Trial Sync dry-run preview with per-file override
- SmartScan change detection (snapshot-based incremental diffing)
- Advanced 3-tier filtering (simple toggles, rule builder, boolean expressions)
- Versioned archiving with retention policies
- Rich scheduling (cron, interval, volume mount, file change, app launch triggers)
- Real-time file monitoring via filesystem events

### SyncShoot Originals
- Post-transfer summary reports (HTML/CSV/JSON) with success, failure, and corruption details
- Cross-platform support (macOS, Windows, Linux)
- CLI mode for headless/automated use

## Setup

```bash
pip install -r requirements.txt
python main.py
```

## Usage

```bash
# GUI mode (default)
python main.py

# Headless CLI mode
python main.py --headless --run-task my_backup_task.json
```

## Architecture

```
syncshoot/
├── main.py                  # Entry point
├── config.py                # App-wide constants, enums, defaults
├── engine/                  # Core engines
│   ├── hasher.py            # Checksum computation
│   ├── copier.py            # File copy engine
│   ├── verifier.py          # Post-copy verification
│   ├── sync.py              # Sync modes
│   ├── scanner.py           # SmartScan change detection
│   ├── filter.py            # Rule-based filtering
│   ├── archiver.py          # Versioned archive manager
│   ├── scheduler.py         # Job scheduling
│   └── watcher.py           # Real-time file monitoring
├── destinations/            # Destination handlers
│   ├── local.py             # Local/external drives
│   ├── s3.py                # S3-compatible cloud
│   └── sftp.py              # SFTP remote
├── gui/                     # PySide6 GUI
│   ├── app.py               # QApplication setup
│   ├── main_window.py       # Main window + sidebar
│   ├── dashboard.py         # Transfer dashboard
│   ├── task_editor.py       # Task creation/editing
│   ├── disk_view.py         # Disk management
│   ├── schedule_panel.py    # Schedule management
│   ├── log_viewer.py        # Transfer logs
│   ├── archive_browser.py   # Archive browsing/restore
│   ├── trial_sync.py        # Dry-run preview
│   └── filter_editor.py     # Filter rule editor
└── utils/                   # Utilities
    ├── mhl.py               # ASC MHL support
    ├── notifications.py     # System/email notifications
    ├── disk_utils.py        # Volume detection
    └── report.py            # Post-transfer reports
```