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

# ── Logging Setup ─────────────────────────────────────────────────────────────────
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


# ── SQLite Initialisation ─────────────────────────────────────────────────────────
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

    db.commit()
    db.close()
    logger.info(f"Database initialised: {DB_PATH}")


# ── CLI: Run Task ─────────────────────────────────────────────────────────────────
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
        for r in report.corrupted_files:
            logger.error(f"  CORRUPTED: {r.rel_path} — {r.error}")
        for r in report.failed_files:
            logger.error(f"  FAILED: {r.rel_path} — {r.error}")
        sys.exit(1)


# ── CLI: Verify Volume ────────────────────────────────────────────────────────────
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


# ── CLI: Generate MHL ─────────────────────────────────────────────────────────────
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


# ── History Recording ─────────────────────────────────────────────────────────────
def _record_history(config: dict, result_or_job, report=None):
    """Record a completed transfer in the SQLite history table.

    Accepts either a SyncResult (GUI path) or a TransferJob (CLI path).
    """
    import sqlite3
    db = sqlite3.connect(str(DB_PATH))

    # Normalise fields from either result type
    if hasattr(result_or_job, "files_copied"):  # SyncResult
        r = result_or_job
        status = r.status.value if hasattr(r.status, "value") else str(r.status)
        total_files = r.files_copied + r.files_skipped + r.files_failed
        total_bytes = r.bytes_transferred
        successful = r.files_copied
        failed = r.files_failed
        started = getattr(r, "started_at", 0)
        finished = getattr(r, "finished_at", 0)
    else:  # TransferJob (CLI path)
        job = result_or_job
        status = job.status.value if hasattr(job.status, "value") else str(job.status)
        total_files = report.total_files if report else 0
        total_bytes = report.total_bytes if report else 0
        successful = len(report.successful) if report else 0
        failed = len(report.failed) if report else 0
        started = getattr(job, "started_at", 0)
        finished = getattr(job, "finished_at", 0)

    db.execute(
        """INSERT INTO transfer_history
           (task_name, source, destinations, status, started_at, finished_at,
            total_files, total_bytes, successful, failed, corrupted, report_path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            config.get("task_name", ""),
            config.get("source", ""),
            json.dumps(config.get("destinations", [])),
            status,
            started, finished,
            total_files, total_bytes,
            successful, failed,
            0,  # corrupted
            "",
        ),
    )
    db.commit()
    db.close()


# ── macOS Permission Check ────────────────────────────────────────────────────────
def check_file_access(source: str, destinations: list[str]) -> tuple[bool, str]:
    """Test read access on source and write access on each destination.

    Returns (ok, error_message).  On macOS the OS may silently deny
    access if Full Disk Access / Files & Folders has not been granted.
    """
    src = Path(source)
    if not src.exists():
        return False, f"Source does not exist:\n{src}"
    # Read test — try listing contents
    try:
        next(src.iterdir(), None)
    except PermissionError:
        return False, (
            f"Cannot read source folder:\n{src}\n\n"
            "On macOS, open System Settings \u2192 Privacy & Security \u2192 "
            "Full Disk Access and add SyncShoot."
        )
    except OSError as e:
        return False, f"Cannot access source:\n{e}"

    for d in destinations:
        dp = Path(d)
        try:
            dp.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            return False, (
                f"Cannot create destination folder:\n{dp}\n\n"
                "On macOS, open System Settings \u2192 Privacy & Security \u2192 "
                "Full Disk Access and add SyncShoot."
            )
        except OSError as e:
            return False, f"Cannot create destination:\n{e}"
        # Write test — try creating a temp file
        try:
            tmp = dp / ".syncshoot_access_test"
            tmp.write_text("test")
            tmp.unlink()
        except PermissionError:
            return False, (
                f"Cannot write to destination:\n{dp}\n\n"
                "On macOS, open System Settings \u2192 Privacy & Security \u2192 "
                "Full Disk Access and add SyncShoot."
            )
        except OSError as e:
            return False, f"Cannot write to destination:\n{e}"

    return True, ""


def open_macos_privacy_settings():
    """Open the macOS Privacy & Security pane (Full Disk Access)."""
    import platform
    import subprocess
    if platform.system() == "Darwin":
        try:
            subprocess.Popen([
                "open",
                "x-apple.systempreferences:"
                "com.apple.preference.security?Privacy_AllFiles",
            ])
        except Exception:
            pass


# ── Formatting Helpers ────────────────────────────────────────────────────────────
def _fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _fmt_elapsed(secs):
    if secs < 60:
        return f"{secs:.1f}s"
    mins, s = divmod(int(secs), 60)
    if mins < 60:
        return f"{mins}m {s}s"
    hrs, m = divmod(mins, 60)
    return f"{hrs}h {m}m {s}s"


# ── GUI Launch ────────────────────────────────────────────────────────────────────
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
    from PySide6.QtCore import Qt, QThread, QObject, QTimer
    from PySide6.QtCore import Signal as QtSignal
    from PySide6.QtWidgets import (
        QMessageBox, QDialog, QVBoxLayout, QLabel,
        QTextEdit, QPushButton, QHBoxLayout,
    )
    from utils.notifications import (
        notify_transfer_complete,
        notify_corruption_detected,
        play_completion_sound,
    )
    from utils.report import TransferReport, FileReportEntry, save_report

    _active_workers = []
    _transfer_counter = [0]
    _stats = {"active": 0, "complete": 0, "failed": 0,
              "bytes": 0, "speed_samples": []}

    def _refresh_dashboard_stats():
        avg_speed = 0.0
        samples = _stats["speed_samples"]
        if samples:
            avg_speed = sum(samples) / len(samples)
        dashboard.update_stats(
            _stats["active"],
            _stats["complete"],
            _stats["failed"],
            _fmt_bytes(_stats["bytes"]),
            f"{avg_speed:.1f} MB/s",
        )
        window.set_active_count(_stats["active"])

    # ---- Transfer Summary Dialog ----
    def _show_transfer_summary(task_name, result, report_path=None):
        """Show a summary dialog after a transfer finishes."""
        dlg = QDialog(window)
        dlg.setWindowTitle(f"Transfer Summary \u2014 {task_name}")
        dlg.setMinimumWidth(500)
        layout = QVBoxLayout(dlg)

        success = result.files_failed == 0
        status_text = "COMPLETED" if success else "COMPLETED WITH ERRORS"
        status_color = "#4CAF50" if success else "#FF5252"
        header = QLabel(f'<h2 style="color:{status_color}">{status_text}</h2>')
        layout.addWidget(header)

        speed = result.bytes_transferred / (1024 * 1024 * max(result.elapsed, 0.01))
        summary_html = "<br>".join([
            f"<b>Task:</b> {task_name}",
            f"<b>Source:</b> {result.plan.source}",
            f"<b>Destination:</b> {result.plan.destination}",
            "",
            f"<b>Files copied:</b> {result.files_copied}",
            f"<b>Files skipped:</b> {result.files_skipped}",
            f"<b>Files failed:</b> {result.files_failed}",
            f"<b>Files deleted:</b> {result.files_deleted}",
            "",
            f"<b>Data transferred:</b> {_fmt_bytes(result.bytes_transferred)}",
            f"<b>Average speed:</b> {speed:.1f} MB/s",
            f"<b>Elapsed:</b> {_fmt_elapsed(result.elapsed)}",
        ])
        summary_label = QLabel(summary_html)
        summary_label.setTextFormat(Qt.RichText)
        summary_label.setWordWrap(True)
        layout.addWidget(summary_label)

        # Error details
        if result.errors:
            err_header = QLabel(f"<b style='color:#FF5252'>Errors ({len(result.errors)}):</b>")
            layout.addWidget(err_header)
            err_text = QTextEdit()
            err_text.setReadOnly(True)
            err_text.setMaximumHeight(150)
            err_lines = [f"{path}: {msg}" for path, msg in result.errors[:50]]
            if len(result.errors) > 50:
                err_lines.append(f"... and {len(result.errors) - 50} more")
            err_text.setPlainText("\n".join(err_lines))
            layout.addWidget(err_text)

        # Buttons row
        btn_layout = QHBoxLayout()
        if report_path:
            open_btn = QPushButton("Open Report")
            _rp = str(report_path)
            def _open_report(checked=False, rp=_rp):
                import subprocess as _sp
                import platform as _pf
                if _pf.system() == "Darwin":
                    _sp.Popen(["open", rp])
                elif _pf.system() == "Windows":
                    _sp.Popen(["start", rp], shell=True)
                else:
                    _sp.Popen(["xdg-open", rp])
            open_btn.clicked.connect(_open_report)
            btn_layout.addWidget(open_btn)

        close_btn = QPushButton("OK")
        close_btn.clicked.connect(dlg.accept)
        btn_layout.addWidget(close_btn)
        layout.addLayout(btn_layout)

        dlg.exec()

    # ---- Permission-checked launch ----
    def _check_access(config):
        """Validate file access before starting a transfer."""
        src = config.get("source", "")
        dsts = config.get("destinations", [])
        ok, err_msg = check_file_access(src, dsts)
        if not ok:
            box = QMessageBox(window)
            box.setIcon(QMessageBox.Critical)
            box.setWindowTitle("Permission Denied")
            box.setText(err_msg)
            box.addButton("Open System Settings", QMessageBox.ActionRole)
            box.addButton(QMessageBox.Cancel)
            result = box.exec()
            # ActionRole button returns 0
            if result == 0:
                open_macos_privacy_settings()
            return False
        return True

    # ---- Sync Worker ----
    class _SyncWorker(QObject):
        finished = QtSignal(object)   # SyncResult or SyncPlan
        error = QtSignal(str)
        progress = QtSignal(int, int, str, str)  # done, total, file, op

        def __init__(self, cfg, execute=False):
            super().__init__()
            self._cfg = cfg
            self._execute = execute

        def run(self):
            try:
                src = Path(self._cfg["source"])
                dst = Path(self._cfg["destinations"][0])

                # Preserve source folder name at destination
                # e.g. source=/Volumes/Card/DCIM, dest=/Backup
                #   -> actual dest becomes /Backup/DCIM/
                if self._cfg.get("preserve_folder_name", True):
                    dst = dst / src.name
                    dst.mkdir(parents=True, exist_ok=True)

                mode = EngSyncMode(
                    self._cfg.get("sync_mode", "backup")
                )
                algo = HashAlgorithm(
                    self._cfg.get("hash_algorithm", "xxh3_64")
                )
                plan = plan_sync(src, dst, mode, algo)
                if self._execute:
                    def _cb(done, total, rel_path, op_type):
                        self.progress.emit(done, total, rel_path, op_type)
                    result = execute_sync(plan, progress_cb=_cb)
                    self.finished.emit(result)
                else:
                    self.finished.emit(plan)
            except Exception as e:
                self.error.emit(str(e))

    # ---- Trial Sync ----
    def _on_trial_sync(config):
        if not _check_access(config):
            return
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

    # ---- Run Single Task (with full dashboard + notifications + report) ----
    def _run_single_task(config):
        """Run a single task with permission check, dashboard card,
        notifications, report generation, and summary dialog."""
        if not _check_access(config):
            return

        import time as _time
        _transfer_counter[0] += 1
        tid = f"transfer_{_transfer_counter[0]}_{int(_time.time())}"
        task_name = config.get("task_name", "Untitled")
        src = config.get("source", "?")
        dst = config.get("destinations", ["?"])[0]
        label = f"{task_name} ({Path(src).name} \u2192 {Path(dst).name})"

        # Create dashboard card immediately
        card = dashboard.add_transfer(tid, label)
        from config import TransferStatus as TS, FileOpStatus
        card.update_status(TS.INDEXING)
        _stats["active"] += 1
        _refresh_dashboard_stats()
        window.set_status(f"Starting: {task_name}...")
        # Switch to dashboard so user sees activity
        window._switch_panel(0)

        w = _SyncWorker(config, execute=True)
        t = QThread(window)
        w.moveToThread(t)

        _start_time = [_time.time()]

        def on_progress(done, total, rel_path, op_type):
            if total == 0:
                pct = 0
            else:
                pct = int(done / total * 100)
            elapsed = _time.time() - _start_time[0]
            # Estimate speed from progress ratio and elapsed
            if elapsed > 0 and done > 0:
                speed = (done / total * config_total_bytes) / (1024 * 1024 * elapsed) if 'config_total_bytes' in dir() else 0.0
            else:
                speed = 0.0
            if total > done:
                remaining = (elapsed / max(done, 1)) * (total - done)
                mins, secs = divmod(int(remaining), 60)
                eta = f"{mins}m {secs}s"
            else:
                eta = "--"
            size_str = f"{done}/{total} files"
            card.update_progress(pct, rel_path, speed, eta, size_str)
            card.update_status(TS.COPYING)
            window.set_speed(speed)
            window.set_status(f"{task_name}: {pct}% \u2014 {rel_path}")

        def on_done(result):
            _stats["active"] = max(0, _stats["active"] - 1)
            _stats["bytes"] += result.bytes_transferred
            elapsed = result.elapsed if result.elapsed > 0 else 0.01
            spd = result.bytes_transferred / (1024 * 1024 * elapsed)
            _stats["speed_samples"].append(spd)
            if len(_stats["speed_samples"]) > 20:
                _stats["speed_samples"] = _stats["speed_samples"][-20:]

            c = result.files_copied
            s = result.files_skipped
            f = result.files_failed
            success = f == 0

            # Update dashboard card
            if f > 0:
                card.update_status(TS.FAILED)
                _stats["failed"] += 1
            else:
                card.update_status(TS.COMPLETE)
                _stats["complete"] += 1
            card.update_progress(
                100,
                f"Done: {c} copied, {s} skipped, {f} failed",
                0.0, "--",
                _fmt_bytes(result.bytes_transferred),
            )
            _refresh_dashboard_stats()
            window.set_status(
                f"{task_name}: {c} copied, {s} skipped, {f} failed"
            )
            window.set_speed(0.0)

            # Generate HTML report
            report_path = None
            try:
                file_results_failed = [
                    FileReportEntry(
                        rel_path=err_path,
                        status=FileOpStatus.FAILED,
                        error=err_msg,
                    )
                    for err_path, err_msg in result.errors
                ]
                file_results_ok = [
                    FileReportEntry(rel_path=f"({c} files copied)", status=FileOpStatus.SUCCESS)
                ] if c > 0 else []
                report = TransferReport(
                    task_name=task_name,
                    source=str(result.plan.source),
                    destinations=[str(result.plan.destination)],
                    started_at=_start_time[0],
                    finished_at=_start_time[0] + result.elapsed,
                    total_files=c + s + f,
                    total_bytes=result.bytes_transferred,
                    successful=file_results_ok,
                    failed=file_results_failed,
                    corrupted=[],
                    all_passed=success,
                )
                rp = save_report(report, ReportFormat.HTML)
                report_path = rp
                logger.info(f"Report saved: {rp}")
            except Exception as e:
                logger.warning(f"Report generation failed: {e}")

            # Record in history DB
            try:
                _record_history(config, result, report if report_path else None)
            except Exception as e:
                logger.warning(f"History recording failed: {e}")

            # System notification + sound
            try:
                notify_transfer_complete(
                    task_name=task_name,
                    total_files=c + s + f,
                    total_size_str=_fmt_bytes(result.bytes_transferred),
                    elapsed_str=_fmt_elapsed(result.elapsed),
                    failed_count=f,
                )
                play_completion_sound(success=success)
            except Exception as e:
                logger.warning(f"Notification failed: {e}")

            # Show summary dialog
            _show_transfer_summary(task_name, result, report_path)

            t.quit()

        def on_err(msg):
            _stats["active"] = max(0, _stats["active"] - 1)
            _stats["failed"] += 1
            card.update_status(TS.FAILED)
            card.update_progress(0, f"Error: {msg}", 0.0, "--", "--")
            _refresh_dashboard_stats()
            window.set_status(f"{task_name} failed: {msg}")
            window.set_speed(0.0)
            # Notify on error
            try:
                notify_transfer_complete(
                    task_name=task_name,
                    total_files=0,
                    total_size_str="0 B",
                    elapsed_str="--",
                    failed_count=1,
                )
                play_completion_sound(success=False)
            except Exception:
                pass
            QMessageBox.critical(window, "Task Error", msg)
            t.quit()

        w.progress.connect(on_progress)
        w.finished.connect(on_done)
        w.error.connect(on_err)
        t.started.connect(w.run)
        t.start()
        _active_workers.append((t, w))

    # ---- Button handlers ----
    def _on_run_task(config):
        """Run Now button handler."""
        _run_single_task(config)

    def _on_task_saved(path):
        window.set_status(f"Task saved: {path}")
        logger.info(f"Task saved: {path}")

    task_editor.trial_sync_requested.connect(_on_trial_sync)
    task_editor.run_requested.connect(_on_run_task)
    task_editor.task_saved.connect(_on_task_saved)

    # +New Task -> reset form
    window.new_task_requested.connect(task_editor.reset_form)

    # Run All -> load all saved tasks and run each
    def _on_run_all():
        from config import TASKS_DIR
        task_files = list(TASKS_DIR.glob("*.json"))
        if not task_files:
            QMessageBox.information(
                window, "No Tasks",
                "No saved tasks found. Create and save a task first."
            )
            window.set_status("No saved tasks to run")
            return
        count = 0
        for tf in task_files:
            try:
                cfg = json.loads(tf.read_text())
                if cfg.get("source") and cfg.get("destinations"):
                    _run_single_task(cfg)
                    count += 1
            except Exception as e:
                logger.error(f"Failed to load task {tf.name}: {e}")
        window.set_status(f"Running {count} task(s)...")

    window.run_all_requested.connect(_on_run_all)

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


# ── Main ──────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        prog=APP_NAME.lower(),
        description=f"{APP_NAME} v{APP_VERSION} \u2014 High-performance file transfer, sync & backup",
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
