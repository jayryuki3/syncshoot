"""Notification system for SyncShoot.

Supports:
- System tray notifications (via Qt)
- Email notifications via SMTP
- Completion sounds
- Configurable per-task notification preferences
"""

from __future__ import annotations

import logging
import smtplib
import threading
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Callable, Optional

from config import (
    NOTIFICATION_SOUND,
    NOTIFICATION_SYSTEM,
    NOTIFICATION_EMAIL,
    APP_NAME,
)

logger = logging.getLogger(__name__)


@dataclass
class NotificationConfig:
    """Per-task notification preferences."""
    system_notify: bool = NOTIFICATION_SYSTEM
    play_sound: bool = NOTIFICATION_SOUND
    email_notify: bool = NOTIFICATION_EMAIL
    email_to: str = ""
    email_from: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""
    smtp_tls: bool = True
    on_success: bool = True
    on_failure: bool = True
    on_corruption: bool = True


# ── Notification Event ────────────────────────────────────────────────────────
@dataclass
class NotificationEvent:
    title: str
    message: str
    level: str = "info"         # "info", "success", "warning", "error"
    task_name: str = ""
    details: str = ""


# ── System Notification ───────────────────────────────────────────────────────
def send_system_notification(event: NotificationEvent):
    """Send an OS-level notification. Uses Qt if available, falls back to platform."""
    try:
        from PySide6.QtWidgets import QSystemTrayIcon, QApplication
        from PySide6.QtGui import QIcon

        app = QApplication.instance()
        if app is None:
            _fallback_notify(event)
            return

        tray = getattr(app, '_syncshoot_tray', None)
        if tray and isinstance(tray, QSystemTrayIcon):
            icon_map = {
                "info": QSystemTrayIcon.MessageIcon.Information,
                "success": QSystemTrayIcon.MessageIcon.Information,
                "warning": QSystemTrayIcon.MessageIcon.Warning,
                "error": QSystemTrayIcon.MessageIcon.Critical,
            }
            tray.showMessage(
                event.title,
                event.message,
                icon_map.get(event.level, QSystemTrayIcon.MessageIcon.Information),
                5000,
            )
        else:
            _fallback_notify(event)

    except ImportError:
        _fallback_notify(event)


def _fallback_notify(event: NotificationEvent):
    """Fallback platform-specific notification."""
    import platform
    system = platform.system()

    try:
        if system == "Darwin":
            import subprocess
            script = f'display notification "{event.message}" with title "{event.title}"'
            subprocess.run(["osascript", "-e", script], timeout=5, capture_output=True)
        elif system == "Linux":
            import subprocess
            subprocess.run(
                ["notify-send", event.title, event.message],
                timeout=5, capture_output=True,
            )
        # Windows: toast notifications require additional libraries
    except Exception as exc:
        logger.debug(f"Fallback notification failed: {exc}")


# ── Sound Notification ────────────────────────────────────────────────────────
def play_completion_sound(success: bool = True):
    """Play a system sound on transfer completion."""
    import platform
    system = platform.system()

    try:
        if system == "Darwin":
            import subprocess
            sound = "Glass" if success else "Basso"
            subprocess.run(
                ["afplay", f"/System/Library/Sounds/{sound}.aiff"],
                timeout=5, capture_output=True,
            )
        elif system == "Linux":
            import subprocess
            # Try paplay (PulseAudio) or aplay (ALSA)
            for cmd in ["paplay", "aplay"]:
                try:
                    subprocess.run(
                        [cmd, "/usr/share/sounds/freedesktop/stereo/complete.oga"],
                        timeout=5, capture_output=True,
                    )
                    break
                except FileNotFoundError:
                    continue
        elif system == "Windows":
            import winsound
            sound = winsound.MB_OK if success else winsound.MB_ICONHAND
            winsound.MessageBeep(sound)
    except Exception as exc:
        logger.debug(f"Sound playback failed: {exc}")


# ── Email Notification ────────────────────────────────────────────────────────
def send_email_notification(event: NotificationEvent, config: NotificationConfig):
    """Send an email notification in a background thread."""
    if not config.email_to or not config.smtp_host:
        logger.warning("Email notification skipped: missing email config")
        return

    def _send():
        try:
            msg = MIMEMultipart()
            msg["From"] = config.email_from or config.smtp_user
            msg["To"] = config.email_to
            msg["Subject"] = f"[{APP_NAME}] {event.title}"

            body = f"{event.message}\n\n"
            if event.details:
                body += f"Details:\n{event.details}\n"
            body += f"\nTask: {event.task_name}" if event.task_name else ""

            msg.attach(MIMEText(body, "plain"))

            if config.smtp_tls:
                server = smtplib.SMTP(config.smtp_host, config.smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP(config.smtp_host, config.smtp_port)

            if config.smtp_user and config.smtp_pass:
                server.login(config.smtp_user, config.smtp_pass)

            server.send_message(msg)
            server.quit()
            logger.info(f"Email notification sent to {config.email_to}")

        except Exception as exc:
            logger.error(f"Email notification failed: {exc}")

    thread = threading.Thread(target=_send, daemon=True)
    thread.start()


# ── Unified Notify ────────────────────────────────────────────────────────────
def notify(
    event: NotificationEvent,
    config: Optional[NotificationConfig] = None,
):
    """Send notifications according to config preferences."""
    if config is None:
        config = NotificationConfig()

    # Filter by event level
    if event.level == "success" and not config.on_success:
        return
    if event.level == "error" and not config.on_failure:
        return
    if event.level == "warning" and not config.on_corruption:
        return

    if config.system_notify:
        send_system_notification(event)

    if config.play_sound:
        play_completion_sound(success=(event.level in ("info", "success")))

    if config.email_notify:
        send_email_notification(event, config)


# ── Convenience Builders ──────────────────────────────────────────────────────
def notify_transfer_complete(
    task_name: str,
    total_files: int,
    total_size_str: str,
    elapsed_str: str,
    all_passed: bool,
    config: Optional[NotificationConfig] = None,
):
    """Send a transfer completion notification."""
    if all_passed:
        event = NotificationEvent(
            title=f"Transfer Complete: {task_name}",
            message=f"{total_files} files ({total_size_str}) in {elapsed_str}",
            level="success",
            task_name=task_name,
        )
    else:
        event = NotificationEvent(
            title=f"Transfer Issues: {task_name}",
            message=f"Completed with errors. {total_files} files processed.",
            level="error",
            task_name=task_name,
        )
    notify(event, config)


def notify_corruption_detected(
    task_name: str,
    corrupted_count: int,
    file_list: str = "",
    config: Optional[NotificationConfig] = None,
):
    """Send an alert about corrupted files."""
    event = NotificationEvent(
        title=f"CORRUPTION DETECTED: {task_name}",
        message=f"{corrupted_count} file(s) failed checksum verification!",
        level="warning",
        task_name=task_name,
        details=file_list,
    )
    notify(event, config)
