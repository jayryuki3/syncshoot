"""SyncShoot — Main entry point.

Usage:
    python main.py                          # Launch GUI
    python main.py --headless               # CLI mode (no GUI)
    python main.py --run-task TASK.json     # Run a saved task immediately
    python main.py --verify /path/to/dir    # Verify a volume
    python main.py --generate-mhl /path     # Generate MHL for directory
    python main.py --report TASK.json       # Generate report from last run

Initialises:
- SQLite database for snapshots, schedules, transfer history
- QApplication with theme and system tray
- Main window with all panels registered
- Scheduler, watcher, and mount detection
- Signal/slot connections between engine and GUI
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    APP_NAME,
    APP_VERSION,
    CONFIG_DIR,
    DB_PATH,
    LOG_DIR,
    TASKS_DIR,
    SyncMode,
    VerifyMode,
    HashAlgorithm,
    ReportFormat,
    TransferStatus,
)

# ── Logging Setup ─────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "syncshoot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(APP_NAME)


# ── SQLite Initialisation ─────────────────────────────────────────────────────
def init_database():
    """Ensure all SQLite tables exist."""
    import sqlite3
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")

    # Transfer history
    db.execute("""
        CREATE TABLE IF NOT EXISTS transfer_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name   TEXT NOT NULL,
            source      TEXT NOT NULL,
            destinations TEXT NOT NULL,
            status      TEXT NOT NULL,
            started_at  REAL,
            finished_at REAL,
            total_files INTEGER DEFAULT 0,
            total_bytes INTEGER DEFAULT 0,
            successful  INTEGER DEFAULT 0,
            failed      INTEGER DEFAULT 0,
            corrupted   INTEGER DEFAULT 0,
            report_path TEXT
        )
    """)

    # Snapshots and schedules are created by their respective modules
    # (scanner.py and scheduler.py) on first use

    db.commit()
    db.close()
    logger.info(f"Database initialised: {DB_PATH}")


# ── CLI: Run Task ─────────────────────────────────────────────────────────────
def run_task_cli(task_path: str):
    """Run a saved task in headless mode."""
    from engine.copier import TransferJob, run_transfer, TransferControl
    from engine.verifier import verify_transfer
    from utils.report import build_report_from_job, save_report

    path = Path(task_path)
    if not path.exists():
        logger.error(f"Task file not found: {path}")
        sys.exit(1)

    config = json.loads(path.read_text())
    logger.info(f"Running task: {config.get('task_name', 'Unknown')}")

    # Build transfer job
    job = TransferJob(
        source=Path(config["source"]),
        destinations=[Path(d) for d in config["destinations"]],
        cascade=config.get("cascade", False),
        safe_copy=config.get("safe_copy", True),
        move_mode=config.get("move_mode", False),
        hash_algo=HashAlgorithm(config.get("hash_algorithm", "xxh3_64")),
        skip_duplicates=config.get("skip_duplicates", True),
        throttle_mbps=config.get("throttle_mbps", 0),
    )

    ctrl = TransferControl()

    def _progress(j, copied, total):
        if total > 0:
            pct = copied / total * 100
            print(f"\r  Progress: {pct:.1f}% ({copied}/{total} bytes)", end="", flush=True)

    # Execute
    t0 = time.time()
    job = run_transfer(job, ctrl=ctrl, transfer_cb=_progress)
    print()  # newline after progress

    # Verify
    verify_mode = VerifyMode(config.get("verify_mode", "source_dest"))
    if verify_mode != VerifyMode.NONE and job.destinations:
        logger.info(f"Verifying with mode: {verify_mode.value}")
        rel_paths = [f.rel for f in job.files if f.status.value == "success"]
        from engine.verifier import verify_transfer as vt
        vreport = vt(
            job.source, job.destinations[0], rel_paths,
            mode=verify_mode, algo=job.hash_algo,
        )
        logger.info(f"Verification: {vreport.verified_ok} OK, "
                     f"{vreport.corrupted} corrupted, {vreport.failed} failed")
    else:
        vreport = None

    # Generate report
    report = build_report_from_job(config.get("task_name", "CLI Task"), job, vreport)
    for fmt in (ReportFormat.HTML, ReportFormat.JSON):
        rpath = save_report(report, fmt)
        logger.info(f"Report saved: {rpath}")

    # Record in history
    _record_history(config, job, report)

    # Summary
    elapsed = time.time() - t0
    logger.info(f"Task complete in {elapsed:.1f}s — "
                f"{len(report.successful)} OK, {len(report.failed)} failed, "
                f"{len(report.corrupted)} corrupted")

    if not report.all_passed:
        sys.exit(1)


# ── CLI: Verify Volume ────────────────────────────────────────────────────────
def verify_volume_cli(volume_path: str):
    """Verify all files on a volume using existing MHL files."""
    from utils.mhl import find_mhl_files, extract_checksums
    from engine.verifier import verify_volume

    root = Path(volume_path)
    if not root.is_dir():
        logger.error(f"Not a directory: {root}")
        sys.exit(1)

    mhl_files = find_mhl_files(root)
    if not mhl_files:
        logger.warning("No MHL files found. Running hash-based verification not possible without MHL.")
        sys.exit(1)

    logger.info(f"Found {len(mhl_files)} MHL file(s)")
    all_checksums = {}
    for mhl in mhl_files:
        checksums = extract_checksums(mhl)
        all_checksums.update(checksums)
        logger.info(f"  {mhl.name}: {len(checksums)} entries")

    def _progress(done, total, current):
        print(f"\r  Verifying: {done}/{total} — {current[:60]}", end="", flush=True)

    report = verify_volume(root, all_checksums, progress_cb=_progress)
    print()

    logger.info(f"Verification complete: {report.verified_ok} OK, "
                f"{report.corrupted} corrupted, {report.failed} failed, "
                f"{report.missing_dest} missing")

    if not report.all_passed:
        for r in report.corrupted_files:
            logger.error(f"  CORRUPTED: {r.rel_path} — {r.error}")
        for r in report.failed_files:
            logger.error(f"  FAILED: {r.rel_path} — {r.error}")
        sys.exit(1)


# ── CLI: Generate MHL ─────────────────────────────────────────────────────────
def generate_mhl_cli(dir_path: str):
    """Generate MHL for a directory."""
    from engine.hasher import hash_files
    from utils.mhl import generate_transfer_mhl

    root = Path(dir_path)
    if not root.is_dir():
        logger.error(f"Not a directory: {root}")
        sys.exit(1)

    # Collect files
    files = []
    for dirpath, _, filenames in root.walk() if hasattr(root, 'walk') else _walk(root):
        for fn in filenames:
            if not fn.endswith(".mhl"):
                files.append(Path(dirpath) / fn)

    logger.info(f"Hashing {len(files)} files...")
    from engine.hasher import hash_file, HashAlgorithm as HA
    file_hashes = {}
    for i, fp in enumerate(files):
        hr = hash_file(fp, HA.XXH3_64)
        rel = str(fp.relative_to(root))
        file_hashes[rel] = ("xxh3_64", hr.digest)
        if (i + 1) % 100 == 0:
            print(f"\r  Hashed {i+1}/{len(files)}", end="", flush=True)

    print()
    mhl_path = generate_transfer_mhl(root, root, file_hashes)
    logger.info(f"MHL generated: {mhl_path}")


def _walk(root):
    """Fallback for Python < 3.12 without Path.walk."""
    import os
    yield from os.walk(root)


# ── History Recording ─────────────────────────────────────────────────────────
def _record_history(config: dict, job, report):
    import sqlite3
    db = sqlite3.connect(str(DB_PATH))
    db.execute(
        """INSERT INTO transfer_history
           (task_name, source, destinations, status, started_at, finished_at,
            total_files, total_bytes, successful, failed, corrupted, report_path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            config.get("task_name", ""),
            config.get("source", ""),
            json.dumps(config.get("destinations", [])),
            job.status.value,
            job.started_at, job.finished_at,
            report.total_files, report.total_bytes,
            len(report.successful), len(report.failed), len(report.corrupted),
            "",
        ),
    )
    db.commit()
    db.close()


# ── GUI Launch ────────────────────────────────────────────────────────────────
def launch_gui():
    """Launch the full GUI application."""
    from gui.app import SyncShootApp
    from gui.main_window import MainWindow
    from gui.dashboard import DashboardPanel
    from gui.disk_view import DiskViewPanel
    from gui.task_editor import TaskEditorPanel
    from gui.schedule_panel import SchedulePanel
    from gui.archive_browser import ArchiveBrowserPanel
    from gui.log_viewer import LogViewerPanel

    app = SyncShootApp()

    # Create main window
    window = MainWindow()

    # Create and register panels
    dashboard = DashboardPanel()
    task_editor = TaskEditorPanel()
    disk_view = DiskViewPanel()
    schedule_panel = SchedulePanel()
    archive_browser = ArchiveBrowserPanel()
    log_viewer = LogViewerPanel()

    window.register_panel(0, dashboard)
    window.register_panel(1, task_editor)
    window.register_panel(2, disk_view)
    window.register_panel(3, schedule_panel)
    window.register_panel(4, archive_browser)
    window.register_panel(5, log_viewer)

    # -- Wire Task Editor signals to engine --
    from engine.sync import plan_sync, execute_sync
    from engine.sync import SyncMode as EngSyncMode
    from PySide6.QtCore import QThread, QObject
    from PySide6.QtCore import Signal as QtSignal
    from PySide6.QtWidgets import QMessageBox

    _active_workers = []

    class _SyncWorker(QObject):
        finished = QtSignal(object)
        error = QtSignal(str)
        def __init__(self, cfg, execute=False):
            super().__init__()
            self._cfg = cfg
            self._execute = execute
        def run(self):
            try:
                src = Path(self._cfg["source"])
                dst = Path(self._cfg["destinations"][0])
                mode = EngSyncMode(
                    self._cfg.get("sync_mode", "backup")
                )
                algo = HashAlgorithm(
                    self._cfg.get("hash_algorithm", "xxh3_64")
                )
                plan = plan_sync(src, dst, mode, algo)
                if self._execute:
                    self.finished.emit(execute_sync(plan))
                else:
                    self.finished.emit(plan)
            except Exception as e:
                self.error.emit(str(e))

    def _on_trial_sync(config):
        window.set_status("Running Trial Sync...")
        w = _SyncWorker(config, execute=False)
        t = QThread(window)
        w.moveToThread(t)
        def done(plan):
            ops = []
            for o in plan.operations:
                ops.append({
                    "rel_path": o.rel_path,
                    "op": o.op.value,
                    "src_size": o.src_size,
                    "dst_size": o.dst_size,
                    "reason": o.reason,
                })
            task_editor.show_preview_results(ops)
            nc = len(plan.copies)
            nd = len(plan.deletions)
            ns = len(plan.skips)
            window.set_status(
                f"Trial done: {nc} copy, {nd} del, {ns} skip"
            )
            t.quit()
        def err(msg):
            QMessageBox.critical(
                window, "Trial Sync Error", msg
            )
            window.set_status("Trial Sync failed")
            t.quit()
        w.finished.connect(done)
        w.error.connect(err)
        t.started.connect(w.run)
        t.start()
        _active_workers.append((t, w))

    def _on_run_task(config):
        window.set_status("Running task...")
        w = _SyncWorker(config, execute=True)
        t = QThread(window)
        w.moveToThread(t)
        def done(result):
            c = result.files_copied
            s = result.files_skipped
            f = result.files_failed
            window.set_status(
                f"Done: {c} copied, {s} skipped, {f} failed"
            )
            t.quit()
        def err(msg):
            QMessageBox.critical(
                window, "Task Error", msg
            )
            window.set_status("Task failed")
            t.quit()
        w.finished.connect(done)
        w.error.connect(err)
        t.started.connect(w.run)
        t.start()
        _active_workers.append((t, w))

    def _on_task_saved(path):
        window.set_status(f"Task saved: {path}")
        logger.info(f"Task saved: {path}")

    task_editor.trial_sync_requested.connect(_on_trial_sync)
    task_editor.run_requested.connect(_on_run_task)
    task_editor.task_saved.connect(_on_task_saved)

    def _load_task_file(fpath):
        try:
            cfg = json.loads(Path(fpath).read_text())
            task_editor.load_config(cfg)
            window.set_status(
                f"Loaded: {Path(fpath).name}"
            )
            window._switch_panel(1)
        except Exception as e:
            QMessageBox.critical(
                window, "Load Error", str(e)
            )
    window.task_file_opened.connect(_load_task_file)

    # Setup system tray
    app.setup_tray(window)

    # Start scheduler & mount watcher
    from engine.scheduler import Scheduler
    from utils.disk_utils import MountWatcher

    scheduler = Scheduler()
    scheduler.recover_missed()
    scheduler.run_app_launch_schedules()

    mount_watcher = MountWatcher()
    mount_watcher.add_callback(
        lambda event, disk: scheduler.check_volume_mount(
            {disk.label}, set()
        ) if event == "mounted" else None
    )
    mount_watcher.start()

    # Initial disk refresh
    disk_view.refresh_disks()

    # Show
    window.show()
    logger.info(f"{APP_NAME} v{APP_VERSION} started (GUI mode)")

    exit_code = app.exec()

    # Cleanup
    mount_watcher.stop()
    app.cleanup()

    return exit_code


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        prog=APP_NAME.lower(),
        description=f"{APP_NAME} v{APP_VERSION} — High-performance file transfer, sync & backup",
    )
    parser.add_argument("--headless", action="store_true", help="Run in CLI mode (no GUI)")
    parser.add_argument("--run-task", metavar="TASK.json", help="Run a saved task file")
    parser.add_argument("--verify", metavar="PATH", help="Verify a volume against MHL checksums")
    parser.add_argument("--generate-mhl", metavar="PATH", help="Generate MHL for a directory")
    parser.add_argument("--version", action="version", version=f"{APP_NAME} {APP_VERSION}")

    args = parser.parse_args()

    # Initialise database
    init_database()

    if args.run_task:
        run_task_cli(args.run_task)
    elif args.verify:
        verify_volume_cli(args.verify)
    elif args.generate_mhl:
        generate_mhl_cli(args.generate_mhl)
    elif args.headless:
        logger.info(f"{APP_NAME} running in headless mode. Use --run-task to execute a task.")
        # Could start scheduler loop here for daemon mode
        from engine.scheduler import Scheduler
        scheduler = Scheduler()
        scheduler.recover_missed()
        logger.info("Scheduler running. Press Ctrl+C to stop.")
        try:
            while True:
                scheduler.check_cron_schedules()
                scheduler.check_interval_schedules()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Shutting down.")
    else:
        sys.exit(launch_gui())


if __name__ == "__main__":
    main()
