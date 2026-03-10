"""Trial Sync (dry-run) dialog.

Modal dialog showing all planned file operations before execution:
- Table view: file path, operation, source size, dest size, reason
- Per-file override: skip, force copy, or change operation
- Space requirement estimate
- 'Synchronize' button to commit, 'Cancel' to abort
- Export planned operations to CSV
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QComboBox,
    QFileDialog,
    QMessageBox,
    QSizePolicy,
)

from config import COLORS


# ── Color Map ─────────────────────────────────────────────────────────────────
OP_COLORS = {
    "copy": QColor("#2196F3"),
    "replace": QColor("#FF9800"),
    "delete": QColor("#F44336"),
    "skip": QColor("#9E9E9E"),
    "move": QColor("#64B5F6"),
    "conflict": QColor("#FFC107"),
}


def _fmt_size(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


class TrialSyncDialog(QDialog):
    """Modal dialog showing planned sync operations with override support."""

    execute_requested = Signal(list)  # list of planned operations (with overrides)

    def __init__(self, operations: list[dict], source: str, destination: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Trial Sync — Dry Run Preview")
        self.setMinimumSize(900, 600)
        self.resize(1000, 700)

        self._operations = operations
        self._overrides: dict[int, str] = {}  # row -> override op

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(12)

        # Header
        header = QLabel("Trial Sync Preview")
        header.setStyleSheet("color: #fff; font-size: 20px; font-weight: 700;")
        layout.addWidget(header)

        # Info row
        info_layout = QHBoxLayout()
        info_layout.addWidget(self._info_label("Source:", source))
        info_layout.addWidget(self._info_label("Destination:", destination))
        layout.addLayout(info_layout)

        # Summary stats
        copies = sum(1 for o in operations if o.get("op") in ("copy", "replace"))
        deletes = sum(1 for o in operations if o.get("op") == "delete")
        skips = sum(1 for o in operations if o.get("op") == "skip")
        conflicts = sum(1 for o in operations if o.get("op") == "conflict")
        total_bytes = sum(o.get("src_size", 0) for o in operations if o.get("op") in ("copy", "replace", "move"))

        stats = QLabel(
            f"Copy/Replace: {copies}  |  Delete: {deletes}  |  "
            f"Skip: {skips}  |  Conflicts: {conflicts}  |  "
            f"Space needed: {_fmt_size(total_bytes)}"
        )
        stats.setStyleSheet("color: #aaa; font-size: 12px; padding: 4px 0;")
        layout.addWidget(stats)

        # Table
        self._table = QTableWidget()
        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels([
            "File Path", "Operation", "Source Size", "Dest Size", "Reason", "Override"
        ])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.verticalHeader().setDefaultSectionSize(32)

        self._populate_table()
        layout.addWidget(self._table)

        # Action buttons
        btn_layout = QHBoxLayout()

        btn_export = QPushButton("Export CSV")
        btn_export.clicked.connect(self._on_export)
        btn_layout.addWidget(btn_export)

        btn_layout.addStretch()

        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(btn_cancel)

        btn_sync = QPushButton("Synchronize")
        btn_sync.setObjectName("primary")
        btn_sync.clicked.connect(self._on_synchronize)
        btn_layout.addWidget(btn_sync)

        layout.addLayout(btn_layout)

    def _info_label(self, title: str, value: str) -> QWidget:
        w = QLabel(f"<b>{title}</b> {value}")
        w.setStyleSheet("color: #ccc; font-size: 12px;")
        return w

    def _populate_table(self):
        self._table.setRowCount(len(self._operations))
        for row, op in enumerate(self._operations):
            # File path
            path_item = QTableWidgetItem(op.get("rel_path", ""))
            path_item.setFlags(path_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._table.setItem(row, 0, path_item)

            # Operation
            op_type = op.get("op", "skip")
            op_item = QTableWidgetItem(op_type.upper())
            op_item.setFlags(op_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            op_item.setForeground(OP_COLORS.get(op_type, QColor("#888")))
            self._table.setItem(row, 1, op_item)

            # Source size
            src_size = QTableWidgetItem(_fmt_size(op.get("src_size", 0)))
            src_size.setFlags(src_size.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._table.setItem(row, 2, src_size)

            # Dest size
            dst_size = QTableWidgetItem(_fmt_size(op.get("dst_size", 0)))
            dst_size.setFlags(dst_size.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._table.setItem(row, 3, dst_size)

            # Reason
            reason = QTableWidgetItem(op.get("reason", ""))
            reason.setFlags(reason.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._table.setItem(row, 4, reason)

            # Override dropdown
            combo = QComboBox()
            combo.addItems(["(default)", "Skip", "Copy", "Delete"])
            combo.currentTextChanged.connect(lambda text, r=row: self._set_override(r, text))
            self._table.setCellWidget(row, 5, combo)

    def _set_override(self, row: int, text: str):
        if text == "(default)":
            self._overrides.pop(row, None)
        else:
            self._overrides[row] = text.lower()

    def _on_synchronize(self):
        # Apply overrides
        result = []
        for i, op in enumerate(self._operations):
            op_copy = dict(op)
            if i in self._overrides:
                op_copy["override"] = self._overrides[i]
            result.append(op_copy)

        self.execute_requested.emit(result)
        self.accept()

    def _on_export(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Trial Sync", "trial_sync.csv", "CSV Files (*.csv)"
        )
        if not path:
            return

        import csv
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["File Path", "Operation", "Source Size", "Dest Size", "Reason"])
            for op in self._operations:
                writer.writerow([
                    op.get("rel_path", ""),
                    op.get("op", ""),
                    op.get("src_size", 0),
                    op.get("dst_size", 0),
                    op.get("reason", ""),
                ])

        QMessageBox.information(self, "Exported", f"Trial Sync exported to:\n{path}")
