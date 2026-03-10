"""Schedule management panel.

Features:
- Schedule list with columns: task name, trigger type, next run, last run, status
- Create/edit dialog: trigger type picker, cron builder, volume selector
- Enable/disable toggle per schedule
- Missed schedule indicator
"""

from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QDialog,
    QFormLayout,
    QComboBox,
    QLineEdit,
    QSpinBox,
    QCheckBox,
    QMessageBox,
)

from config import ScheduleTrigger


class ScheduleEditDialog(QDialog):
    """Dialog for creating/editing a schedule."""

    def __init__(self, parent=None, schedule: dict | None = None):
        super().__init__(parent)
        self.setWindowTitle("Edit Schedule" if schedule else "New Schedule")
        self.setMinimumWidth(450)

        layout = QFormLayout(self)
        layout.setSpacing(12)

        self._task_name = QLineEdit()
        self._task_name.setPlaceholderText("Select a task...")
        layout.addRow("Task:", self._task_name)

        self._trigger_type = QComboBox()
        for t in ScheduleTrigger:
            self._trigger_type.addItem(t.value.replace("_", " ").title(), t.value)
        self._trigger_type.currentIndexChanged.connect(self._on_trigger_changed)
        layout.addRow("Trigger Type:", self._trigger_type)

        # Cron fields
        self._cron_expr = QLineEdit()
        self._cron_expr.setPlaceholderText("30 2 * * 1-5  (min hour day month weekday)")
        layout.addRow("Cron Expression:", self._cron_expr)

        # Interval field
        self._interval = QSpinBox()
        self._interval.setRange(1, 86400)
        self._interval.setValue(60)
        self._interval.setSuffix(" seconds")
        layout.addRow("Interval:", self._interval)

        # Volume mount field
        self._volume_label = QLineEdit()
        self._volume_label.setPlaceholderText("Volume label (e.g. CAMERA_CARD)")
        layout.addRow("Volume Label:", self._volume_label)

        # Launch delay
        self._launch_delay = QSpinBox()
        self._launch_delay.setRange(0, 300)
        self._launch_delay.setValue(10)
        self._launch_delay.setSuffix(" seconds")
        layout.addRow("Launch Delay:", self._launch_delay)

        self._enabled = QCheckBox("Enabled")
        self._enabled.setChecked(True)
        layout.addRow(self._enabled)

        # Buttons
        btn_row = QHBoxLayout()
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        btn_save = QPushButton("Save")
        btn_save.setObjectName("primary")
        btn_save.clicked.connect(self.accept)
        btn_row.addWidget(btn_save)
        layout.addRow(btn_row)

        # Load existing
        if schedule:
            self._task_name.setText(schedule.get("task_name", ""))
            idx = self._trigger_type.findData(schedule.get("trigger", "cron"))
            if idx >= 0:
                self._trigger_type.setCurrentIndex(idx)
            self._cron_expr.setText(schedule.get("cron_expression", ""))
            self._interval.setValue(schedule.get("interval_seconds", 60))
            self._volume_label.setText(schedule.get("volume_label", ""))
            self._launch_delay.setValue(schedule.get("launch_delay", 10))
            self._enabled.setChecked(schedule.get("enabled", True))

        self._on_trigger_changed()

    def _on_trigger_changed(self):
        t = self._trigger_type.currentData()
        self._cron_expr.setVisible(t == "cron")
        self._interval.setVisible(t == "interval")
        self._volume_label.setVisible(t == "volume_mount")
        self._launch_delay.setVisible(t == "app_launch")

    def get_config(self) -> dict:
        return {
            "task_name": self._task_name.text(),
            "trigger": self._trigger_type.currentData(),
            "cron_expression": self._cron_expr.text(),
            "interval_seconds": self._interval.value(),
            "volume_label": self._volume_label.text(),
            "launch_delay": self._launch_delay.value(),
            "enabled": self._enabled.isChecked(),
        }


class SchedulePanel(QWidget):
    """Panel for managing scheduled tasks."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        # Header
        header_row = QHBoxLayout()
        header = QLabel("Schedules")
        header.setStyleSheet("color: #fff; font-size: 22px; font-weight: 700;")
        header_row.addWidget(header)
        header_row.addStretch()

        btn_add = QPushButton("+ New Schedule")
        btn_add.setObjectName("primary")
        btn_add.clicked.connect(self._on_add)
        header_row.addWidget(btn_add)
        layout.addLayout(header_row)

        # Table
        self._table = QTableWidget()
        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels([
            "Task", "Trigger", "Schedule", "Last Run", "Status", "Enabled"
        ])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._on_context_menu)
        layout.addWidget(self._table)

        # Bottom buttons
        btn_row = QHBoxLayout()
        btn_edit = QPushButton("Edit")
        btn_edit.clicked.connect(self._on_edit)
        btn_row.addWidget(btn_edit)
        btn_delete = QPushButton("Delete")
        btn_delete.clicked.connect(self._on_delete)
        btn_row.addWidget(btn_delete)
        btn_run = QPushButton("Run Now")
        btn_run.clicked.connect(self._on_run_now)
        btn_row.addWidget(btn_run)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        self._schedules: list[dict] = []

    def add_schedule_row(self, config: dict):
        self._schedules.append(config)
        self._refresh_table()

    def _refresh_table(self):
        self._table.setRowCount(len(self._schedules))
        for row, s in enumerate(self._schedules):
            self._table.setItem(row, 0, QTableWidgetItem(s.get("task_name", "")))
            self._table.setItem(row, 1, QTableWidgetItem(s.get("trigger", "").replace("_", " ").title()))
            detail = s.get("cron_expression") or f"Every {s.get('interval_seconds', 0)}s" or s.get("volume_label", "")
            self._table.setItem(row, 2, QTableWidgetItem(detail))
            self._table.setItem(row, 3, QTableWidgetItem(s.get("last_run", "Never")))
            self._table.setItem(row, 4, QTableWidgetItem(s.get("last_status", "\u2014")))
            enabled = QTableWidgetItem("Yes" if s.get("enabled", True) else "No")
            self._table.setItem(row, 5, enabled)

    def _on_add(self):
        dlg = ScheduleEditDialog(self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self.add_schedule_row(dlg.get_config())

    def _on_edit(self):
        row = self._table.currentRow()
        if row < 0:
            return
        dlg = ScheduleEditDialog(self, self._schedules[row])
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._schedules[row] = dlg.get_config()
            self._refresh_table()

    def _on_delete(self):
        row = self._table.currentRow()
        if row < 0:
            return
        self._schedules.pop(row)
        self._refresh_table()

    def _on_run_now(self):
        row = self._table.currentRow()
        if row >= 0:
            QMessageBox.information(self, "Run", f"Running: {self._schedules[row].get('task_name', '')}")

    def _on_context_menu(self, pos):
        from PySide6.QtWidgets import QMenu
        menu = QMenu(self)
        menu.addAction("Edit", self._on_edit)
        menu.addAction("Delete", self._on_delete)
        menu.addAction("Run Now", self._on_run_now)
        menu.exec(self._table.viewport().mapToGlobal(pos))
