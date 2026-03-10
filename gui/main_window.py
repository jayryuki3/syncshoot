"""Main window with sidebar navigation.

Layout:
- Left sidebar: navigation buttons (Dashboard, Tasks, Disks, Schedules, Archive, Logs)
- Right area: stacked widget switching between panels
- Toolbar: common actions
- Status bar: active transfer count and speed
- Menu bar: File, Edit, View, Tools, Help
"""

from __future__ import annotations

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QHBoxLayout,
    QVBoxLayout,
    QPushButton,
    QStackedWidget,
    QStatusBar,
    QToolBar,
    QLabel,
    QFileDialog,
    QMessageBox,
    QSizePolicy,
)

from config import APP_NAME, APP_VERSION, COLORS


# ── Sidebar Button ────────────────────────────────────────────────────────────
class SidebarButton(QPushButton):
    """Custom styled sidebar navigation button."""

    def __init__(self, text: str, parent=None):
        super().__init__(text, parent)
        self.setCheckable(True)
        self.setFixedHeight(44)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setStyleSheet("""
            QPushButton {
                background: transparent;
                color: #888;
                border: none;
                border-left: 3px solid transparent;
                text-align: left;
                padding-left: 16px;
                font-size: 13px;
                font-weight: 500;
            }
            QPushButton:hover {
                color: #bbb;
                background: #16213e;
            }
            QPushButton:checked {
                color: #fff;
                background: #16213e;
                border-left: 3px solid #2196F3;
            }
        """)


# ── Main Window ───────────────────────────────────────────────────────────────
class MainWindow(QMainWindow):
    """SyncShoot main application window."""

    # Signals
    pause_all_signal = Signal()
    resume_all_signal = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"{APP_NAME} v{APP_VERSION}")
        self.setMinimumSize(1100, 700)
        self.resize(1280, 800)

        self._panels = {}
        self._nav_buttons = []

        self._setup_menu_bar()
        self._setup_toolbar()
        self._setup_central()
        self._setup_status_bar()

        # Status bar update timer
        self._status_timer = QTimer(self)
        self._status_timer.timeout.connect(self._update_status)
        self._status_timer.start(1000)

    # ── Menu Bar ──────────────────────────────────────────────────────────
    def _setup_menu_bar(self):
        mb = self.menuBar()

        # File
        file_menu = mb.addMenu("&File")
        new_task = QAction("&New Task", self)
        new_task.setShortcut(QKeySequence("Ctrl+N"))
        new_task.triggered.connect(self._on_new_task)
        file_menu.addAction(new_task)

        open_task = QAction("&Open Task...", self)
        open_task.setShortcut(QKeySequence("Ctrl+O"))
        open_task.triggered.connect(self._on_open_task)
        file_menu.addAction(open_task)

        file_menu.addSeparator()

        import_action = QAction("&Import Task...", self)
        import_action.triggered.connect(self._on_import_task)
        file_menu.addAction(import_action)

        export_action = QAction("&Export Task...", self)
        export_action.triggered.connect(self._on_export_task)
        file_menu.addAction(export_action)

        file_menu.addSeparator()

        quit_action = QAction("&Quit", self)
        quit_action.setShortcut(QKeySequence("Ctrl+Q"))
        quit_action.triggered.connect(self.close)
        file_menu.addAction(quit_action)

        # Edit
        edit_menu = mb.addMenu("&Edit")
        prefs = QAction("&Preferences...", self)
        prefs.setShortcut(QKeySequence("Ctrl+,"))
        prefs.triggered.connect(self._on_preferences)
        edit_menu.addAction(prefs)

        # View
        view_menu = mb.addMenu("&View")
        toggle_sidebar = QAction("Toggle &Sidebar", self)
        toggle_sidebar.setShortcut(QKeySequence("Ctrl+B"))
        toggle_sidebar.triggered.connect(self._toggle_sidebar)
        view_menu.addAction(toggle_sidebar)

        toggle_theme = QAction("Toggle &Theme", self)
        toggle_theme.setShortcut(QKeySequence("Ctrl+T"))
        toggle_theme.triggered.connect(self._on_toggle_theme)
        view_menu.addAction(toggle_theme)

        # Tools
        tools_menu = mb.addMenu("&Tools")
        verify_vol = QAction("Verify &Volume...", self)
        verify_vol.triggered.connect(self._on_verify_volume)
        tools_menu.addAction(verify_vol)

        gen_mhl = QAction("Generate &MHL...", self)
        gen_mhl.triggered.connect(self._on_generate_mhl)
        tools_menu.addAction(gen_mhl)

        tools_menu.addSeparator()

        gen_report = QAction("Generate &Report...", self)
        gen_report.triggered.connect(self._on_generate_report)
        tools_menu.addAction(gen_report)

        # Help
        help_menu = mb.addMenu("&Help")
        about = QAction("&About", self)
        about.triggered.connect(self._on_about)
        help_menu.addAction(about)

    # ── Toolbar ───────────────────────────────────────────────────────────
    def _setup_toolbar(self):
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        toolbar.setIconSize(toolbar.iconSize())
        self.addToolBar(toolbar)

        self._btn_new = QPushButton("+ New Task")
        self._btn_new.setObjectName("primary")
        self._btn_new.clicked.connect(self._on_new_task)
        toolbar.addWidget(self._btn_new)

        toolbar.addSeparator()

        self._btn_run_all = QPushButton("Run All")
        self._btn_run_all.clicked.connect(self._on_run_all)
        toolbar.addWidget(self._btn_run_all)

        self._btn_stop_all = QPushButton("Stop All")
        self._btn_stop_all.clicked.connect(self._on_stop_all)
        toolbar.addWidget(self._btn_stop_all)

        # Spacer
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(spacer)

        self._speed_label = QLabel("0 MB/s")
        self._speed_label.setStyleSheet("color: #2196F3; font-weight: bold; padding-right: 16px;")
        toolbar.addWidget(self._speed_label)

    # ── Central Widget ────────────────────────────────────────────────────
    def _setup_central(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Sidebar
        self._sidebar = QWidget()
        self._sidebar.setFixedWidth(180)
        self._sidebar.setStyleSheet("background-color: #111128;")
        sidebar_layout = QVBoxLayout(self._sidebar)
        sidebar_layout.setContentsMargins(0, 8, 0, 8)
        sidebar_layout.setSpacing(2)

        # Logo/title
        title = QLabel(APP_NAME)
        title.setStyleSheet("""
            color: #fff;
            font-size: 18px;
            font-weight: 700;
            padding: 12px 16px 16px;
        """)
        sidebar_layout.addWidget(title)

        # Navigation buttons
        nav_items = [
            ("Dashboard", 0),
            ("Tasks", 1),
            ("Disks", 2),
            ("Schedules", 3),
            ("Archive", 4),
            ("Logs", 5),
        ]

        for label, index in nav_items:
            btn = SidebarButton(label)
            btn.clicked.connect(lambda checked, idx=index: self._switch_panel(idx))
            sidebar_layout.addWidget(btn)
            self._nav_buttons.append(btn)

        sidebar_layout.addStretch()

        # Version label at bottom
        ver = QLabel(f"v{APP_VERSION}")
        ver.setStyleSheet("color: #444; font-size: 11px; padding: 8px 16px;")
        sidebar_layout.addWidget(ver)

        layout.addWidget(self._sidebar)

        # Stacked panels
        self._stack = QStackedWidget()
        layout.addWidget(self._stack)

        # Create placeholder panels (replaced when actual panels are registered)
        for label, _ in nav_items:
            placeholder = QLabel(f"{label} panel")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("color: #555; font-size: 16px;")
            self._stack.addWidget(placeholder)

        # Select dashboard by default
        self._switch_panel(0)

    # ── Status Bar ────────────────────────────────────────────────────────
    def _setup_status_bar(self):
        self._status = QStatusBar()
        self.setStatusBar(self._status)
        self._status_label = QLabel("Ready")
        self._status.addWidget(self._status_label)

        self._active_label = QLabel("0 active transfers")
        self._status.addPermanentWidget(self._active_label)

    # ── Panel Management ──────────────────────────────────────────────────
    def register_panel(self, index: int, widget: QWidget):
        """Replace placeholder at index with actual panel widget."""
        old = self._stack.widget(index)
        self._stack.removeWidget(old)
        old.deleteLater()
        self._stack.insertWidget(index, widget)
        self._panels[index] = widget

    def _switch_panel(self, index: int):
        self._stack.setCurrentIndex(index)
        for i, btn in enumerate(self._nav_buttons):
            btn.setChecked(i == index)

    def _toggle_sidebar(self):
        self._sidebar.setVisible(not self._sidebar.isVisible())

    # ── Status Updates ────────────────────────────────────────────────────
    def _update_status(self):
        """Called every second to refresh status bar."""
        # Placeholder — will be connected to engine state
        pass

    def set_status(self, text: str):
        self._status_label.setText(text)

    def set_speed(self, mbps: float):
        self._speed_label.setText(f"{mbps:.1f} MB/s")

    def set_active_count(self, count: int):
        self._active_label.setText(f"{count} active transfer{'s' if count != 1 else ''}")

    # ── Actions ───────────────────────────────────────────────────────────
    def _on_new_task(self):
        self._switch_panel(1)  # Switch to Tasks panel

    def _on_open_task(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open Task", "", "Task Files (*.json);;All Files (*)"
        )
        if path:
            self.set_status(f"Opened: {path}")

    def _on_import_task(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Import Task", "", "Task Files (*.json);;All Files (*)"
        )
        if path:
            self.set_status(f"Imported: {path}")

    def _on_export_task(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Task", "", "Task Files (*.json);;All Files (*)"
        )
        if path:
            self.set_status(f"Exported: {path}")

    def _on_preferences(self):
        self.set_status("Preferences dialog (TODO)")

    def _on_toggle_theme(self):
        from PySide6.QtWidgets import QApplication
        app = QApplication.instance()
        if hasattr(app, 'toggle_theme'):
            app.toggle_theme()

    def _on_verify_volume(self):
        path = QFileDialog.getExistingDirectory(self, "Select Volume to Verify")
        if path:
            self.set_status(f"Verifying: {path}")

    def _on_generate_mhl(self):
        path = QFileDialog.getExistingDirectory(self, "Select Directory for MHL")
        if path:
            self.set_status(f"Generating MHL: {path}")

    def _on_generate_report(self):
        self.set_status("Report generation (TODO)")

    def _on_run_all(self):
        self.set_status("Running all tasks...")

    def _on_stop_all(self):
        self.set_status("Stopping all tasks...")
        self.pause_all_signal.emit()

    def _on_about(self):
        QMessageBox.about(
            self,
            f"About {APP_NAME}",
            f"<h2>{APP_NAME} v{APP_VERSION}</h2>"
            f"<p>High-performance file transfer, sync & backup.</p>"
            f"<p>Combining the best of Offshoot and ChronoSync.</p>"
        )

    def pause_all(self):
        self.pause_all_signal.emit()
        self.set_status("All transfers paused")

    def resume_all(self):
        self.resume_all_signal.emit()
        self.set_status("All transfers resumed")
