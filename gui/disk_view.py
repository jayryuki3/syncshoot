"""Disk/volume management view.

Grid/list view of all connected volumes showing:
- Icon, name, capacity bar, filesystem type
- Drag-and-drop to assign as Source or Destination
- Right-click context menu
- Labels for differentiating same-name volumes
- Auto-refresh on mount/unmount events
"""

from __future__ import annotations

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QFrame,
    QScrollArea,
    QMenu,
    QInputDialog,
    QMessageBox,
    QSizePolicy,
)

from config import COLORS


# ── Disk Card Widget ──────────────────────────────────────────────────────────
class DiskCard(QFrame):
    """Visual card representing one mounted volume."""

    set_source = Signal(str)        # mount_point
    set_destination = Signal(str)   # mount_point
    eject_requested = Signal(str)   # mount_point

    def __init__(self, mount_point: str, label: str, fstype: str,
                 total_gb: float, free_gb: float, is_removable: bool,
                 parent=None):
        super().__init__(parent)
        self.mount_point = mount_point
        self.volume_label = label
        self._custom_label = ""

        self.setFixedSize(240, 160)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_context_menu)

        border_color = "#2196F3" if is_removable else "#2a3a5e"
        self.setStyleSheet(f"""
            QFrame {{
                background-color: #16213e;
                border-radius: 8px;
                border: 1px solid {border_color};
            }}
            QFrame:hover {{
                border-color: #2196F3;
            }}
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(4)

        # Volume name
        self._name_label = QLabel(label)
        self._name_label.setStyleSheet("color: #fff; font-size: 14px; font-weight: 600;")
        layout.addWidget(self._name_label)

        # Custom label
        self._custom_label_widget = QLabel("")
        self._custom_label_widget.setStyleSheet("color: #2196F3; font-size: 11px;")
        self._custom_label_widget.hide()
        layout.addWidget(self._custom_label_widget)

        # Filesystem + removable badge
        info_row = QHBoxLayout()
        fs_label = QLabel(fstype)
        fs_label.setStyleSheet("color: #888; font-size: 11px;")
        info_row.addWidget(fs_label)
        if is_removable:
            badge = QLabel("REMOVABLE")
            badge.setStyleSheet("""
                color: #2196F3;
                background: #2196F322;
                padding: 1px 6px;
                border-radius: 3px;
                font-size: 10px;
                font-weight: 600;
            """)
            info_row.addWidget(badge)
        info_row.addStretch()
        layout.addLayout(info_row)

        layout.addStretch()

        # Capacity bar
        percent = ((total_gb - free_gb) / total_gb * 100) if total_gb > 0 else 0
        self._capacity = QProgressBar()
        self._capacity.setRange(0, 100)
        self._capacity.setValue(int(percent))
        self._capacity.setFixedHeight(8)
        self._capacity.setTextVisible(False)

        bar_color = COLORS["complete"] if percent < 80 else (COLORS["corrupted"] if percent < 95 else COLORS["failed"])
        self._capacity.setStyleSheet(f"""
            QProgressBar {{ background: #0d1b36; border-radius: 4px; border: none; }}
            QProgressBar::chunk {{ background: {bar_color}; border-radius: 4px; }}
        """)
        layout.addWidget(self._capacity)

        # Space label
        self._space_label = QLabel(f"{free_gb:.1f} GB free of {total_gb:.1f} GB")
        self._space_label.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(self._space_label)

    def set_custom_label(self, label: str):
        self._custom_label = label
        if label:
            self._custom_label_widget.setText(label)
            self._custom_label_widget.show()
        else:
            self._custom_label_widget.hide()

    def _show_context_menu(self, pos):
        menu = QMenu(self)

        src_action = menu.addAction("Set as Source")
        src_action.triggered.connect(lambda: self.set_source.emit(self.mount_point))

        dst_action = menu.addAction("Set as Destination")
        dst_action.triggered.connect(lambda: self.set_destination.emit(self.mount_point))

        menu.addSeparator()

        label_action = menu.addAction("Add Label...")
        label_action.triggered.connect(self._on_add_label)

        menu.addSeparator()

        eject_action = menu.addAction("Eject")
        eject_action.triggered.connect(lambda: self.eject_requested.emit(self.mount_point))

        menu.exec(self.mapToGlobal(pos))

    def _on_add_label(self):
        text, ok = QInputDialog.getText(self, "Volume Label", "Enter label:", text=self._custom_label)
        if ok:
            self.set_custom_label(text)


# ── Disk View Panel ───────────────────────────────────────────────────────────
class DiskViewPanel(QWidget):
    """Panel showing all connected volumes in a grid."""

    source_selected = Signal(str)       # mount_point
    destination_selected = Signal(str)  # mount_point

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(16)

        # Header row
        header_row = QHBoxLayout()
        header = QLabel("Disks & Volumes")
        header.setStyleSheet("color: #fff; font-size: 22px; font-weight: 700;")
        header_row.addWidget(header)
        header_row.addStretch()

        self._btn_refresh = QPushButton("Refresh")
        self._btn_refresh.clicked.connect(self.refresh_disks)
        header_row.addWidget(self._btn_refresh)

        layout.addLayout(header_row)

        # Scroll area for disk grid
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")

        self._grid_container = QWidget()
        self._grid_layout = QGridLayout(self._grid_container)
        self._grid_layout.setSpacing(12)
        self._grid_layout.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)

        scroll.setWidget(self._grid_container)
        layout.addWidget(scroll)

        self._disk_cards: list[DiskCard] = []

        # Auto-refresh timer (every 5 seconds)
        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self.refresh_disks)
        self._refresh_timer.start(5000)

    def refresh_disks(self):
        """Re-scan volumes and rebuild card grid."""
        # Clear existing
        for card in self._disk_cards:
            self._grid_layout.removeWidget(card)
            card.deleteLater()
        self._disk_cards.clear()

        try:
            from utils.disk_utils import list_disks
            disks = list_disks()
        except ImportError:
            disks = []

        cols = 4
        for i, disk in enumerate(disks):
            card = DiskCard(
                mount_point=disk.mount_point,
                label=disk.label,
                fstype=disk.fstype,
                total_gb=disk.total_gb,
                free_gb=disk.free_gb,
                is_removable=disk.is_removable,
            )
            card.set_source.connect(self._on_set_source)
            card.set_destination.connect(self._on_set_destination)
            card.eject_requested.connect(self._on_eject)

            row, col = divmod(i, cols)
            self._grid_layout.addWidget(card, row, col)
            self._disk_cards.append(card)

    def _on_set_source(self, mount_point: str):
        self.source_selected.emit(mount_point)

    def _on_set_destination(self, mount_point: str):
        self.destination_selected.emit(mount_point)

    def _on_eject(self, mount_point: str):
        try:
            from destinations.local import eject_volume
            from pathlib import Path
            success, msg = eject_volume(Path(mount_point))
            if success:
                self.refresh_disks()
            else:
                QMessageBox.warning(self, "Eject Failed", msg)
        except ImportError:
            QMessageBox.warning(self, "Error", "Eject functionality not available")
