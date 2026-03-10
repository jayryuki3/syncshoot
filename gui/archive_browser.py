"""Archive browser panel — browse, search, and restore archived file versions.

Features:
- Tree/list view of archived files with version history
- One-click restore to original location or custom path
- Archive maintenance: prune by age/count
- Search across all archived files
"""

from __future__ import annotations

from pathlib import Path

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
    QLineEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QFileDialog,
    QMessageBox,
    QSplitter,
    QGroupBox,
    QSpinBox,
    QFormLayout,
)

from config import COLORS


def _fmt_size(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


class ArchiveBrowserPanel(QWidget):
    """Panel for browsing and restoring archived file versions."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        # Header
        header_row = QHBoxLayout()
        header = QLabel("Archive Browser")
        header.setStyleSheet("color: #fff; font-size: 22px; font-weight: 700;")
        header_row.addWidget(header)
        header_row.addStretch()

        self._btn_refresh = QPushButton("Refresh")
        self._btn_refresh.clicked.connect(self.refresh)
        header_row.addWidget(self._btn_refresh)

        self._btn_prune = QPushButton("Prune Old Versions")
        self._btn_prune.clicked.connect(self._on_prune)
        header_row.addWidget(self._btn_prune)

        layout.addLayout(header_row)

        # Search
        search_row = QHBoxLayout()
        self._search = QLineEdit()
        self._search.setPlaceholderText("Search archived files...")
        self._search.textChanged.connect(self._on_search)
        search_row.addWidget(self._search)
        layout.addLayout(search_row)

        # Stats
        self._stats_label = QLabel("No archive loaded")
        self._stats_label.setStyleSheet("color: #888; font-size: 12px;")
        layout.addWidget(self._stats_label)

        # Splitter: tree + version table
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # File tree
        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["Archived Files"])
        self._tree.currentItemChanged.connect(self._on_file_selected)
        splitter.addWidget(self._tree)

        # Version table
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)

        right_layout.addWidget(QLabel("Versions:"))

        self._version_table = QTableWidget()
        self._version_table.setColumnCount(5)
        self._version_table.setHorizontalHeaderLabels([
            "Version", "Archived Date", "Original Size", "Archive Size", "Compressed"
        ])
        self._version_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._version_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        right_layout.addWidget(self._version_table)

        # Restore buttons
        btn_row = QHBoxLayout()
        btn_restore = QPushButton("Restore Selected")
        btn_restore.setObjectName("primary")
        btn_restore.clicked.connect(self._on_restore)
        btn_row.addWidget(btn_restore)

        btn_restore_to = QPushButton("Restore To...")
        btn_restore_to.clicked.connect(self._on_restore_to)
        btn_row.addWidget(btn_restore_to)

        btn_row.addStretch()
        right_layout.addLayout(btn_row)

        splitter.addWidget(right)
        splitter.setSizes([300, 500])
        layout.addWidget(splitter)

        self._archive_data: dict[str, list[dict]] = {}  # rel_path -> versions

    def set_archive_data(self, data: dict[str, list[dict]]):
        """Set archive data: {rel_path: [{version, archived_at, original_size, archived_size, compressed}, ...]}."""
        self._archive_data = data
        self._build_tree()
        total_files = len(data)
        total_versions = sum(len(v) for v in data.values())
        total_size = sum(e.get("archived_size", 0) for versions in data.values() for e in versions)
        self._stats_label.setText(
            f"{total_files} files, {total_versions} versions, {_fmt_size(total_size)} total archive size"
        )

    def _build_tree(self):
        self._tree.clear()
        for rel_path in sorted(self._archive_data.keys()):
            parts = rel_path.split("/")
            parent = self._tree.invisibleRootItem()
            for i, part in enumerate(parts):
                found = None
                for j in range(parent.childCount()):
                    if parent.child(j).text(0) == part:
                        found = parent.child(j)
                        break
                if found:
                    parent = found
                else:
                    item = QTreeWidgetItem(parent, [part])
                    if i == len(parts) - 1:
                        item.setData(0, Qt.ItemDataRole.UserRole, rel_path)
                        count = len(self._archive_data[rel_path])
                        item.setText(0, f"{part} ({count} versions)")
                    parent = item

    def _on_file_selected(self, current, previous):
        if current is None:
            return
        rel_path = current.data(0, Qt.ItemDataRole.UserRole)
        if not rel_path:
            return
        versions = self._archive_data.get(rel_path, [])
        self._version_table.setRowCount(len(versions))
        for row, v in enumerate(versions):
            self._version_table.setItem(row, 0, QTableWidgetItem(str(v.get("version", ""))))
            self._version_table.setItem(row, 1, QTableWidgetItem(v.get("archived_at", "")))
            self._version_table.setItem(row, 2, QTableWidgetItem(_fmt_size(v.get("original_size", 0))))
            self._version_table.setItem(row, 3, QTableWidgetItem(_fmt_size(v.get("archived_size", 0))))
            self._version_table.setItem(row, 4, QTableWidgetItem("Yes" if v.get("compressed") else "No"))

    def _on_search(self, text: str):
        text = text.lower()
        iterator = self._tree.invisibleRootItem()
        self._filter_tree(iterator, text)

    def _filter_tree(self, item, text: str) -> bool:
        visible = False
        for i in range(item.childCount()):
            child = item.child(i)
            child_visible = self._filter_tree(child, text)
            if not child_visible:
                child_visible = text in child.text(0).lower()
            child.setHidden(not child_visible)
            visible = visible or child_visible
        return visible

    def _on_restore(self):
        QMessageBox.information(self, "Restore", "Restore to original location (TODO: connect to ArchiveManager)")

    def _on_restore_to(self):
        path = QFileDialog.getExistingDirectory(self, "Restore To")
        if path:
            QMessageBox.information(self, "Restore", f"Restoring to: {path}")

    def _on_prune(self):
        reply = QMessageBox.question(
            self, "Prune Archive",
            "Remove old versions according to retention policy?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            QMessageBox.information(self, "Pruned", "Archive pruned (TODO: connect to ArchiveManager)")

    def refresh(self):
        """Reload archive data from ArchiveManager."""
        pass  # Will be connected to ArchiveManager in main.py
