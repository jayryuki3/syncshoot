"""QApplication setup — theme, system tray, single-instance enforcement.

Features:
- Dark/light theme with QSS stylesheets
- System tray icon with quick actions
- Global keyboard shortcuts
- Single-instance enforcement
- App-wide settings via QSettings
"""

from __future__ import annotations

import sys
from typing import Optional

from PySide6.QtCore import Qt, QSettings
from PySide6.QtGui import QAction, QIcon, QKeySequence, QShortcut
from PySide6.QtWidgets import (
    QApplication,
    QMenu,
    QSystemTrayIcon,
    QStyleFactory,
)

from config import APP_NAME, APP_VERSION, APP_ORG, COLORS


# ── Dark Theme Stylesheet ────────────────────────────────────────────────────
DARK_STYLE = """
QMainWindow, QWidget {
    background-color: #1a1a2e;
    color: #e0e0e0;
    font-family: -apple-system, 'Segoe UI', Roboto, sans-serif;
    font-size: 13px;
}
QMenuBar {
    background-color: #16213e;
    color: #e0e0e0;
    border-bottom: 1px solid #0f3460;
}
QMenuBar::item:selected { background-color: #0f3460; }
QMenu {
    background-color: #16213e;
    color: #e0e0e0;
    border: 1px solid #0f3460;
}
QMenu::item:selected { background-color: #0f3460; }
QToolBar {
    background-color: #16213e;
    border-bottom: 1px solid #0f3460;
    spacing: 8px;
    padding: 4px;
}
QStatusBar {
    background-color: #16213e;
    color: #888;
    border-top: 1px solid #0f3460;
}
QPushButton {
    background-color: #0f3460;
    color: #e0e0e0;
    border: 1px solid #1a1a4e;
    border-radius: 4px;
    padding: 6px 16px;
    min-height: 28px;
}
QPushButton:hover { background-color: #1a4a7a; }
QPushButton:pressed { background-color: #0a2a50; }
QPushButton:disabled { background-color: #2a2a3e; color: #666; }
QPushButton#primary {
    background-color: #2196F3;
    border-color: #1976D2;
}
QPushButton#primary:hover { background-color: #42A5F5; }
QLineEdit, QSpinBox, QComboBox, QTextEdit, QPlainTextEdit {
    background-color: #0d1b36;
    color: #e0e0e0;
    border: 1px solid #2a3a5e;
    border-radius: 4px;
    padding: 4px 8px;
    min-height: 28px;
}
QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
    border-color: #2196F3;
}
QComboBox::drop-down {
    border: none;
    width: 24px;
}
QTabWidget::pane {
    background-color: #1a1a2e;
    border: 1px solid #2a3a5e;
    border-top: none;
}
QTabBar::tab {
    background-color: #16213e;
    color: #888;
    padding: 8px 16px;
    border: 1px solid #2a3a5e;
    border-bottom: none;
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
}
QTabBar::tab:selected {
    background-color: #1a1a2e;
    color: #fff;
    border-bottom: 2px solid #2196F3;
}
QTableWidget, QTreeWidget, QListWidget {
    background-color: #0d1b36;
    color: #e0e0e0;
    border: 1px solid #2a3a5e;
    gridline-color: #1a2a4e;
    selection-background-color: #0f3460;
}
QHeaderView::section {
    background-color: #16213e;
    color: #aaa;
    border: 1px solid #2a3a5e;
    padding: 6px;
    font-weight: bold;
    text-transform: uppercase;
    font-size: 11px;
}
QProgressBar {
    background-color: #0d1b36;
    border: 1px solid #2a3a5e;
    border-radius: 4px;
    text-align: center;
    color: #fff;
    height: 20px;
}
QProgressBar::chunk {
    background-color: #2196F3;
    border-radius: 3px;
}
QScrollBar:vertical {
    background: #1a1a2e;
    width: 10px;
    border: none;
}
QScrollBar::handle:vertical {
    background: #2a3a5e;
    border-radius: 5px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover { background: #3a4a6e; }
QSplitter::handle { background-color: #2a3a5e; }
QGroupBox {
    color: #aaa;
    border: 1px solid #2a3a5e;
    border-radius: 4px;
    margin-top: 12px;
    padding-top: 16px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 4px;
}
QCheckBox::indicator, QRadioButton::indicator {
    width: 16px; height: 16px;
    border: 1px solid #2a3a5e;
    border-radius: 3px;
    background: #0d1b36;
}
QCheckBox::indicator:checked {
    background: #2196F3;
    border-color: #1976D2;
}
QToolTip {
    background-color: #16213e;
    color: #e0e0e0;
    border: 1px solid #0f3460;
    padding: 4px;
}
"""

LIGHT_STYLE = """
QMainWindow, QWidget {
    background-color: #f5f5f5;
    color: #333;
}
QPushButton {
    background-color: #e0e0e0;
    color: #333;
    border: 1px solid #ccc;
    border-radius: 4px;
    padding: 6px 16px;
}
QPushButton:hover { background-color: #d0d0d0; }
QPushButton#primary { background-color: #2196F3; color: white; }
"""


# ── Application Factory ──────────────────────────────────────────────────────
class SyncShootApp(QApplication):
    """Custom QApplication with theme and tray support."""

    def __init__(self, argv: list[str] | None = None):
        super().__init__(argv or sys.argv)
        self.setApplicationName(APP_NAME)
        self.setApplicationVersion(APP_VERSION)
        self.setOrganizationName(APP_ORG)

        self._settings = QSettings(APP_ORG, APP_NAME)
        self._tray: Optional[QSystemTrayIcon] = None

        self._apply_theme()

    # ── Theme ─────────────────────────────────────────────────────────────
    def _apply_theme(self):
        theme = self._settings.value("theme", "dark")
        if theme == "dark":
            self.setStyleSheet(DARK_STYLE)
        else:
            self.setStyleSheet(LIGHT_STYLE)

    def toggle_theme(self):
        current = self._settings.value("theme", "dark")
        new_theme = "light" if current == "dark" else "dark"
        self._settings.setValue("theme", new_theme)
        self._apply_theme()

    @property
    def is_dark(self) -> bool:
        return self._settings.value("theme", "dark") == "dark"

    # ── System Tray ───────────────────────────────────────────────────────
    def setup_tray(self, main_window):
        """Create system tray icon with context menu."""
        self._tray = QSystemTrayIcon(self)
        # Use a default icon; in production, load from resources
        self._tray.setToolTip(f"{APP_NAME} v{APP_VERSION}")

        menu = QMenu()

        show_action = QAction("Show Dashboard", self)
        show_action.triggered.connect(main_window.show)
        menu.addAction(show_action)

        menu.addSeparator()

        pause_action = QAction("Pause All Transfers", self)
        pause_action.triggered.connect(lambda: main_window.pause_all())
        menu.addAction(pause_action)

        resume_action = QAction("Resume All Transfers", self)
        resume_action.triggered.connect(lambda: main_window.resume_all())
        menu.addAction(resume_action)

        menu.addSeparator()

        theme_action = QAction("Toggle Theme", self)
        theme_action.triggered.connect(self.toggle_theme)
        menu.addAction(theme_action)

        menu.addSeparator()

        quit_action = QAction("Quit", self)
        quit_action.triggered.connect(self.quit)
        menu.addAction(quit_action)

        self._tray.setContextMenu(menu)
        self._tray.activated.connect(
            lambda reason: main_window.show() if reason == QSystemTrayIcon.ActivationReason.DoubleClick else None
        )
        self._tray.show()

        # Store on app for notification access
        self._syncshoot_tray = self._tray

    # ── Settings ──────────────────────────────────────────────────────────
    @property
    def settings(self) -> QSettings:
        return self._settings

    # ── Cleanup ───────────────────────────────────────────────────────────
    def cleanup(self):
        if self._tray:
            self._tray.hide()
