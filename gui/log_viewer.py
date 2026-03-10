"""Transfer log viewer panel.

Features:
- Searchable, filterable log table
- Detail levels: summary, standard, verbose
- Per-entry drill-down
- Export to CSV/JSON
"""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QLineEdit,
    QComboBox,
    QFileDialog,
    QMessageBox,
    QTextEdit,
    QSplitter,
)

from config import COLORS, FileOpStatus


class LogViewerPanel(QWidget):
    """Panel for viewing and searching transfer logs."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        # Header
        header = QLabel("Transfer Logs")
        header.setStyleSheet("color: #fff; font-size: 22px; font-weight: 700;")
        layout.addWidget(header)

        # Toolbar
        toolbar = QHBoxLayout()

        self._search = QLineEdit()
        self._search.setPlaceholderText("Search logs...")
        self._search.textChanged.connect(self._on_filter)
        toolbar.addWidget(self._search)

        self._level_combo = QComboBox()
        self._level_combo.addItems(["All", "Success", "Failed", "Corrupted", "Skipped"])
        self._level_combo.currentTextChanged.connect(self._on_filter)
        toolbar.addWidget(self._level_combo)

        btn_export = QPushButton("Export")
        btn_export.clicked.connect(self._on_export)
        toolbar.addWidget(btn_export)

        btn_clear = QPushButton("Clear")
        btn_clear.clicked.connect(self._on_clear)
        toolbar.addWidget(btn_clear)

        layout.addLayout(toolbar)

        # Splitter: table + detail
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Log table
        self._table = QTableWidget()
        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels([
            "Timestamp", "Task", "File", "Status", "Size", "Details"
        ])
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.currentCellChanged.connect(self._on_row_selected)
        splitter.addWidget(self._table)

        # Detail view
        self._detail = QTextEdit()
        self._detail.setReadOnly(True)
        self._detail.setMaximumHeight(200)
        self._detail.setPlaceholderText("Select a log entry to view details...")
        splitter.addWidget(self._detail)

        layout.addWidget(splitter)

        self._log_entries: list[dict] = []

    def add_log(self, entry: dict):
        """Add a log entry: {timestamp, task, file, status, size, details, raw}."""
        self._log_entries.append(entry)
        self._add_table_row(entry)

    def _add_table_row(self, entry: dict):
        row = self._table.rowCount()
        self._table.insertRow(row)
        self._table.setItem(row, 0, QTableWidgetItem(entry.get("timestamp", "")))
        self._table.setItem(row, 1, QTableWidgetItem(entry.get("task", "")))
        self._table.setItem(row, 2, QTableWidgetItem(entry.get("file", "")))

        status = entry.get("status", "")
        status_item = QTableWidgetItem(status.upper())
        color_map = {
            "success": COLORS["complete"],
            "failed": COLORS["failed"],
            "corrupted": COLORS["corrupted"],
            "truncated": COLORS["corrupted"],
            "skipped": COLORS["indexing"],
        }
        if status in color_map:
            from PySide6.QtGui import QColor
            status_item.setForeground(QColor(color_map[status]))
        self._table.setItem(row, 3, status_item)

        self._table.setItem(row, 4, QTableWidgetItem(entry.get("size", "")))
        self._table.setItem(row, 5, QTableWidgetItem(entry.get("details", "")[:80]))

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if 0 <= row < len(self._log_entries):
            entry = self._log_entries[row]
            self._detail.setHtml(
                f"<b>Task:</b> {entry.get('task', '')}<br>"
                f"<b>File:</b> {entry.get('file', '')}<br>"
                f"<b>Status:</b> {entry.get('status', '').upper()}<br>"
                f"<b>Size:</b> {entry.get('size', '')}<br>"
                f"<b>Timestamp:</b> {entry.get('timestamp', '')}<br>"
                f"<hr><pre>{entry.get('details', '')}</pre>"
            )

    def _on_filter(self):
        search = self._search.text().lower()
        level = self._level_combo.currentText().lower()
        for row in range(self._table.rowCount()):
            show = True
            if search:
                match = False
                for col in range(self._table.columnCount()):
                    item = self._table.item(row, col)
                    if item and search in item.text().lower():
                        match = True
                        break
                show = match
            if show and level != "all":
                status_item = self._table.item(row, 3)
                if status_item and status_item.text().lower() != level:
                    show = False
            self._table.setRowHidden(row, not show)

    def _on_export(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Logs", "transfer_logs.csv", "CSV (*.csv);;JSON (*.json)"
        )
        if not path:
            return
        if path.endswith(".json"):
            import json
            with open(path, "w") as f:
                json.dump(self._log_entries, f, indent=2)
        else:
            import csv
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp", "Task", "File", "Status", "Size", "Details"])
                for e in self._log_entries:
                    writer.writerow([e.get("timestamp"), e.get("task"), e.get("file"),
                                     e.get("status"), e.get("size"), e.get("details")])
        QMessageBox.information(self, "Exported", f"Logs exported to:\n{path}")

    def _on_clear(self):
        self._log_entries.clear()
        self._table.setRowCount(0)
        self._detail.clear()
