"""Live transfer dashboard — progress bars, speeds, ETA, color-coded status.

Shows real-time transfer cards with:
- Task name (SOURCE -> DEST)
- Current file being processed
- Speed (moving average) and ETA
- Color-coded progress bar (grey=indexing, blue=copying, light-blue=verifying, green=complete, red=error)
- Aggregate stats
- Pause/resume/cancel per-transfer controls
"""

from __future__ import annotations

from PySide6.QtCore import Qt, QTimer, Signal, Slot
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QFrame,
    QGridLayout,
    QSizePolicy,
)

from config import COLORS, TransferStatus


# ── Stat Card Widget ──────────────────────────────────────────────────────────
class StatCard(QFrame):
    """Small stat display card for aggregate numbers."""

    def __init__(self, label: str, value: str = "0", color: str = "#2196F3", parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"""
            QFrame {{
                background-color: #16213e;
                border-radius: 8px;
                border-left: 4px solid {color};
                padding: 8px;
            }}
        """)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)

        self._label = QLabel(label)
        self._label.setStyleSheet("color: #888; font-size: 11px; text-transform: uppercase;")
        layout.addWidget(self._label)

        self._value = QLabel(value)
        self._value.setStyleSheet("color: #fff; font-size: 20px; font-weight: 700;")
        layout.addWidget(self._value)

    def set_value(self, value: str):
        self._value.setText(value)


# ── Transfer Card Widget ──────────────────────────────────────────────────────
class TransferCard(QFrame):
    """A single transfer's live status card."""

    pause_clicked = Signal(str)     # transfer_id
    cancel_clicked = Signal(str)    # transfer_id

    def __init__(self, transfer_id: str, task_name: str, parent=None):
        super().__init__(parent)
        self.transfer_id = transfer_id
        self.setStyleSheet("""
            QFrame {
                background-color: #16213e;
                border-radius: 8px;
                padding: 4px;
                margin-bottom: 4px;
            }
        """)
        self.setMinimumHeight(100)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(6)

        # Row 1: Task name + controls
        row1 = QHBoxLayout()
        self._task_label = QLabel(task_name)
        self._task_label.setStyleSheet("color: #fff; font-size: 14px; font-weight: 600;")
        row1.addWidget(self._task_label)
        row1.addStretch()

        self._status_badge = QLabel("PENDING")
        self._status_badge.setStyleSheet(f"""
            background-color: {COLORS['indexing']}22;
            color: {COLORS['indexing']};
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
        """)
        row1.addWidget(self._status_badge)

        self._btn_pause = QPushButton("Pause")
        self._btn_pause.setFixedSize(60, 26)
        self._btn_pause.clicked.connect(lambda: self.pause_clicked.emit(self.transfer_id))
        row1.addWidget(self._btn_pause)

        self._btn_cancel = QPushButton("Cancel")
        self._btn_cancel.setFixedSize(60, 26)
        self._btn_cancel.setStyleSheet("QPushButton { color: #F44336; }")
        self._btn_cancel.clicked.connect(lambda: self.cancel_clicked.emit(self.transfer_id))
        row1.addWidget(self._btn_cancel)

        layout.addLayout(row1)

        # Row 2: Current file
        self._file_label = QLabel("Waiting...")
        self._file_label.setStyleSheet("color: #888; font-size: 12px;")
        self._file_label.setWordWrap(True)
        layout.addWidget(self._file_label)

        # Row 3: Progress bar
        self._progress = QProgressBar()
        self._progress.setRange(0, 100)
        self._progress.setValue(0)
        self._progress.setFixedHeight(20)
        layout.addWidget(self._progress)

        # Row 4: Stats
        row4 = QHBoxLayout()
        self._speed_label = QLabel("0 MB/s")
        self._speed_label.setStyleSheet("color: #2196F3; font-size: 12px;")
        row4.addWidget(self._speed_label)

        self._eta_label = QLabel("ETA: --")
        self._eta_label.setStyleSheet("color: #888; font-size: 12px;")
        row4.addWidget(self._eta_label)

        row4.addStretch()

        self._size_label = QLabel("0 / 0")
        self._size_label.setStyleSheet("color: #888; font-size: 12px;")
        row4.addWidget(self._size_label)

        layout.addLayout(row4)

    def update_status(self, status: TransferStatus):
        color = COLORS.get(status.value, COLORS["indexing"])
        self._status_badge.setText(status.value.upper())
        self._status_badge.setStyleSheet(f"""
            background-color: {color}22;
            color: {color};
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
        """)
        self._progress.setStyleSheet(f"QProgressBar::chunk {{ background-color: {color}; border-radius: 3px; }}")

    def update_progress(self, percent: int, current_file: str, speed_mbps: float, eta_str: str, size_str: str):
        self._progress.setValue(percent)
        self._file_label.setText(current_file)
        self._speed_label.setText(f"{speed_mbps:.1f} MB/s")
        self._eta_label.setText(f"ETA: {eta_str}")
        self._size_label.setText(size_str)


# ── Dashboard Panel ───────────────────────────────────────────────────────────
class DashboardPanel(QWidget):
    """Main dashboard showing all active/recent transfers and aggregate stats."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(16)

        # Header
        header = QLabel("Dashboard")
        header.setStyleSheet("color: #fff; font-size: 22px; font-weight: 700;")
        layout.addWidget(header)

        # Stat cards row
        stats_layout = QHBoxLayout()
        stats_layout.setSpacing(12)

        self._stat_active = StatCard("Active", "0", COLORS["copying"])
        self._stat_complete = StatCard("Complete", "0", COLORS["complete"])
        self._stat_failed = StatCard("Failed", "0", COLORS["failed"])
        self._stat_data = StatCard("Data Transferred", "0 B", "#64B5F6")
        self._stat_speed = StatCard("Avg Speed", "0 MB/s", "#2196F3")

        for card in (self._stat_active, self._stat_complete, self._stat_failed, self._stat_data, self._stat_speed):
            stats_layout.addWidget(card)

        layout.addLayout(stats_layout)

        # Transfer cards scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")

        self._cards_container = QWidget()
        self._cards_layout = QVBoxLayout(self._cards_container)
        self._cards_layout.setContentsMargins(0, 0, 0, 0)
        self._cards_layout.setSpacing(8)
        self._cards_layout.addStretch()

        scroll.setWidget(self._cards_container)
        layout.addWidget(scroll)

        self._transfer_cards: dict[str, TransferCard] = {}

        # Empty state
        self._empty_label = QLabel("No active transfers.\nCreate a task or drag files to get started.")
        self._empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._empty_label.setStyleSheet("color: #555; font-size: 14px; padding: 60px;")
        self._cards_layout.insertWidget(0, self._empty_label)

    # ── Transfer Card Management ──────────────────────────────────────────
    def add_transfer(self, transfer_id: str, task_name: str) -> TransferCard:
        self._empty_label.hide()
        card = TransferCard(transfer_id, task_name)
        self._transfer_cards[transfer_id] = card
        self._cards_layout.insertWidget(self._cards_layout.count() - 1, card)
        return card

    def remove_transfer(self, transfer_id: str):
        card = self._transfer_cards.pop(transfer_id, None)
        if card:
            self._cards_layout.removeWidget(card)
            card.deleteLater()
        if not self._transfer_cards:
            self._empty_label.show()

    def get_card(self, transfer_id: str) -> TransferCard | None:
        return self._transfer_cards.get(transfer_id)

    # ── Aggregate Stats ───────────────────────────────────────────────────
    def update_stats(self, active: int, complete: int, failed: int, data_str: str, speed_str: str):
        self._stat_active.set_value(str(active))
        self._stat_complete.set_value(str(complete))
        self._stat_failed.set_value(str(failed))
        self._stat_data.set_value(data_str)
        self._stat_speed.set_value(speed_str)
