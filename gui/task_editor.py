"""Task editor panel — create and configure sync/transfer tasks.

Tabbed interface:
- Setup: sync mode, source/destination pickers, direction
- Options: verification, checksum algo, safe copy, throttling, duplicates, archive
- Rules: embedded filter editor
- Preview: inline Trial Sync results
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QFormLayout,
    QTabWidget,
    QLabel,
    QComboBox,
    QLineEdit,
    QPushButton,
    QCheckBox,
    QSpinBox,
    QDoubleSpinBox,
    QGroupBox,
    QFileDialog,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QSizePolicy,
    QScrollArea,
)

from config import (
    SyncMode,
    VerifyMode,
    HashAlgorithm,
    DEFAULT_CHUNK_SIZE,
    TASKS_DIR,
)


class TaskEditorPanel(QWidget):
    """Full task creation/editing panel with tabbed interface."""

    task_saved = Signal(str)        # task file path
    trial_sync_requested = Signal(dict)  # task config dict
    run_requested = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        # Header
        header_row = QHBoxLayout()
        header = QLabel("Task Editor")
        header.setStyleSheet("color: #fff; font-size: 22px; font-weight: 700;")
        header_row.addWidget(header)
        header_row.addStretch()

        self._btn_save = QPushButton("Save Task")
        self._btn_save.setObjectName("primary")
        self._btn_save.clicked.connect(self._on_save)
        header_row.addWidget(self._btn_save)

        self._btn_run = QPushButton("Run Now")
        self._btn_run.clicked.connect(self._on_run)
        header_row.addWidget(self._btn_run)

        layout.addLayout(header_row)

        # Task name
        name_row = QHBoxLayout()
        name_row.addWidget(QLabel("Task Name:"))
        self._task_name = QLineEdit()
        self._task_name.setPlaceholderText("My Backup Task")
        name_row.addWidget(self._task_name)
        layout.addLayout(name_row)

        # Tab widget
        self._tabs = QTabWidget()
        layout.addWidget(self._tabs)

        self._setup_tab()
        self._options_tab()
        self._rules_tab()
        self._preview_tab()

    # ── Setup Tab ─────────────────────────────────────────────────────────
    def _setup_tab(self):
        tab = QWidget()
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(tab)
        scroll.setStyleSheet("QScrollArea { border: none; }")

        layout = QVBoxLayout(tab)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)

        # Sync Mode
        mode_group = QGroupBox("Sync Mode")
        mode_layout = QFormLayout(mode_group)

        self._sync_mode = QComboBox()
        for mode in SyncMode:
            self._sync_mode.addItem(mode.value.replace("_", " ").title(), mode.value)
        mode_layout.addRow("Mode:", self._sync_mode)

        mode_desc = QLabel("")
        mode_desc.setStyleSheet("color: #888; font-size: 12px;")
        mode_desc.setWordWrap(True)
        mode_layout.addRow(mode_desc)

        self._sync_mode.currentIndexChanged.connect(
            lambda: mode_desc.setText(self._get_mode_description())
        )
        layout.addWidget(mode_group)

        # Source
        src_group = QGroupBox("Source")
        src_layout = QVBoxLayout(src_group)

        src_row = QHBoxLayout()
        self._source_path = QLineEdit()
        self._source_path.setPlaceholderText("/path/to/source")
        src_row.addWidget(self._source_path)

        btn_browse_src = QPushButton("Browse...")
        btn_browse_src.clicked.connect(self._browse_source)
        src_row.addWidget(btn_browse_src)

        src_layout.addLayout(src_row)
        layout.addWidget(src_group)

        # Destinations
        dst_group = QGroupBox("Destinations")
        dst_layout = QVBoxLayout(dst_group)

        self._dest_list = QListWidget()
        self._dest_list.setMaximumHeight(120)
        dst_layout.addWidget(self._dest_list)

        dst_btn_row = QHBoxLayout()
        btn_add_dest = QPushButton("+ Add Destination")
        btn_add_dest.clicked.connect(self._add_destination)
        dst_btn_row.addWidget(btn_add_dest)

        btn_remove_dest = QPushButton("Remove Selected")
        btn_remove_dest.clicked.connect(self._remove_destination)
        dst_btn_row.addWidget(btn_remove_dest)

        dst_btn_row.addStretch()

        self._cascade_check = QCheckBox("Enable Cascading (copy to first dest, then clone)")
        dst_layout.addLayout(dst_btn_row)
        dst_layout.addWidget(self._cascade_check)
        layout.addWidget(dst_group)

        layout.addStretch()
        self._tabs.addTab(scroll, "Setup")

    # ── Options Tab ───────────────────────────────────────────────────────
    def _options_tab(self):
        tab = QWidget()
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(tab)
        scroll.setStyleSheet("QScrollArea { border: none; }")

        layout = QVBoxLayout(tab)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)

        # Verification
        verify_group = QGroupBox("Verification")
        verify_layout = QFormLayout(verify_group)

        self._verify_mode = QComboBox()
        for vm in VerifyMode:
            self._verify_mode.addItem(vm.value.replace("_", " ").title(), vm.value)
        self._verify_mode.setCurrentIndex(3)  # SOURCE_DESTINATION default
        verify_layout.addRow("Verify Mode:", self._verify_mode)

        self._hash_algo = QComboBox()
        for algo in HashAlgorithm:
            self._hash_algo.addItem(algo.value, algo.value)
        verify_layout.addRow("Hash Algorithm:", self._hash_algo)

        layout.addWidget(verify_group)

        # Copy Options
        copy_group = QGroupBox("Copy Options")
        copy_layout = QVBoxLayout(copy_group)

        self._safe_copy = QCheckBox("Safe copy (write to .tmp, rename on success)")
        self._safe_copy.setChecked(True)
        copy_layout.addWidget(self._safe_copy)

        self._skip_dupes = QCheckBox("Skip duplicate files (same name + size + hash)")
        self._skip_dupes.setChecked(True)
        copy_layout.addWidget(self._skip_dupes)

        self._move_mode = QCheckBox("Move mode (delete source after successful copy)")
        copy_layout.addWidget(self._move_mode)

        throttle_row = QHBoxLayout()
        throttle_row.addWidget(QLabel("I/O Throttle:"))
        self._throttle = QDoubleSpinBox()
        self._throttle.setRange(0, 10000)
        self._throttle.setValue(0)
        self._throttle.setSuffix(" MB/s")
        self._throttle.setSpecialValueText("Unlimited")
        throttle_row.addWidget(self._throttle)
        throttle_row.addStretch()
        copy_layout.addLayout(throttle_row)

        layout.addWidget(copy_group)

        # Archive Options
        archive_group = QGroupBox("Archive (for replaced/deleted files)")
        archive_layout = QFormLayout(archive_group)

        self._archive_enabled = QCheckBox("Archive replaced files")
        self._archive_enabled.setChecked(True)
        archive_layout.addRow(self._archive_enabled)

        self._archive_versions = QSpinBox()
        self._archive_versions.setRange(1, 100)
        self._archive_versions.setValue(10)
        archive_layout.addRow("Max versions:", self._archive_versions)

        self._archive_days = QSpinBox()
        self._archive_days.setRange(1, 3650)
        self._archive_days.setValue(90)
        archive_layout.addRow("Max age (days):", self._archive_days)

        self._archive_compress = QCheckBox("Compress archived files (gzip)")
        archive_layout.addRow(self._archive_compress)

        layout.addWidget(archive_group)

        layout.addStretch()
        self._tabs.addTab(scroll, "Options")

    # ── Rules Tab ─────────────────────────────────────────────────────────
    def _rules_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(16, 16, 16, 16)

        info = QLabel("Filter rules determine which files are included or excluded.\n"
                       "Configure rules using the Filter Editor below.")
        info.setStyleSheet("color: #888; font-size: 12px;")
        info.setWordWrap(True)
        layout.addWidget(info)

        # Simple toggles
        self._filter_hidden = QCheckBox("Ignore hidden files (.*)")
        self._filter_hidden.setChecked(True)
        layout.addWidget(self._filter_hidden)

        self._filter_junk = QCheckBox("Ignore system junk (.DS_Store, Thumbs.db)")
        self._filter_junk.setChecked(True)
        layout.addWidget(self._filter_junk)

        self._filter_temp = QCheckBox("Ignore temp files (.tmp, .swp)")
        self._filter_temp.setChecked(True)
        layout.addWidget(self._filter_temp)

        self._filter_media = QCheckBox("Include only media files")
        layout.addWidget(self._filter_media)

        # Custom extensions
        ext_row = QHBoxLayout()
        ext_row.addWidget(QLabel("Custom extensions:"))
        self._custom_ext = QLineEdit()
        self._custom_ext.setPlaceholderText("e.g. mov, mp4, r3d, braw")
        ext_row.addWidget(self._custom_ext)
        layout.addLayout(ext_row)

        layout.addStretch()
        self._tabs.addTab(tab, "Rules")

    # ── Preview Tab ───────────────────────────────────────────────────────
    def _preview_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(16, 16, 16, 16)

        btn_trial = QPushButton("Run Trial Sync (Dry Run)")
        btn_trial.setObjectName("primary")
        btn_trial.clicked.connect(self._on_trial_sync)
        layout.addWidget(btn_trial)

        self._preview_label = QLabel("Click 'Run Trial Sync' to preview planned operations.")
        self._preview_label.setStyleSheet("color: #888; padding: 20px;")
        self._preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._preview_label)

        self._preview_list = QListWidget()
        self._preview_list.hide()
        layout.addWidget(self._preview_list)

        layout.addStretch()
        self._tabs.addTab(tab, "Preview")

    # ── Mode Description ──────────────────────────────────────────────────
    def _get_mode_description(self) -> str:
        descs = {
            "backup": "Copy new and modified files from source to destination. Files only on destination are left alone.",
            "blind_backup": "Same as Backup but without state tracking — always compares files directly.",
            "mirror": "Make destination identical to source. Extra files on destination will be DELETED.",
            "bidirectional": "Merge changes from both sides. Newest modification wins. Conflicts flagged.",
            "move": "Move files from source to destination. Source files are deleted after successful transfer.",
        }
        mode_val = self._sync_mode.currentData()
        return descs.get(mode_val, "")

    # ── Actions ───────────────────────────────────────────────────────────
    def _browse_source(self):
        path = QFileDialog.getExistingDirectory(self, "Select Source Directory")
        if path:
            self._source_path.setText(path)

    def _add_destination(self):
        path = QFileDialog.getExistingDirectory(self, "Select Destination Directory")
        if path:
            self._dest_list.addItem(path)

    def _remove_destination(self):
        for item in self._dest_list.selectedItems():
            self._dest_list.takeItem(self._dest_list.row(item))

    def _build_config(self) -> dict:
        """Build a task config dict from current UI state."""
        destinations = [self._dest_list.item(i).text() for i in range(self._dest_list.count())]
        return {
            "task_name": self._task_name.text() or "Untitled Task",
            "sync_mode": self._sync_mode.currentData(),
            "source": self._source_path.text(),
            "destinations": destinations,
            "cascade": self._cascade_check.isChecked(),
            "verify_mode": self._verify_mode.currentData(),
            "hash_algorithm": self._hash_algo.currentData(),
            "safe_copy": self._safe_copy.isChecked(),
            "skip_duplicates": self._skip_dupes.isChecked(),
            "move_mode": self._move_mode.isChecked(),
            "throttle_mbps": self._throttle.value(),
            "archive_enabled": self._archive_enabled.isChecked(),
            "archive_max_versions": self._archive_versions.value(),
            "archive_max_age_days": self._archive_days.value(),
            "archive_compress": self._archive_compress.isChecked(),
            "filters": {
                "ignore_hidden": self._filter_hidden.isChecked(),
                "ignore_system_junk": self._filter_junk.isChecked(),
                "ignore_temp": self._filter_temp.isChecked(),
                "media_only": self._filter_media.isChecked(),
                "custom_extensions": self._custom_ext.text(),
            },
        }

    def _on_save(self):
        config = self._build_config()
        if not config["source"]:
            QMessageBox.warning(self, "Missing Source", "Please select a source directory.")
            return
        if not config["destinations"]:
            QMessageBox.warning(self, "Missing Destination", "Please add at least one destination.")
            return
        TASKS_DIR.mkdir(parents=True, exist_ok=True)

        name = config["task_name"].replace(" ", "_").replace("/", "_")
        path = TASKS_DIR / f"{name}.json"
        path.write_text(json.dumps(config, indent=2))
        self.task_saved.emit(str(path))
        QMessageBox.information(self, "Saved", f"Task saved to:\n{path}")

    def _on_run(self):
        config = self._build_config()
        if not config["source"]:
            QMessageBox.warning(self, "Missing Source", "Please select a source directory.")
            return
        if not config["destinations"]:
            QMessageBox.warning(self, "Missing Destination", "Please add at least one destination.")
            return
        self.run_requested.emit(config)

    def _on_trial_sync(self):
        config = self._build_config()
        if not config["source"] or not config["destinations"]:
            QMessageBox.warning(self, "Incomplete", "Set source and destination first.")
            return
        self.trial_sync_requested.emit(config)
        self._preview_label.setText("Running Trial Sync...")

    def load_config(self, config: dict):
        """Load a task config dict into the UI."""
        self._task_name.setText(config.get("task_name", ""))
        idx = self._sync_mode.findData(config.get("sync_mode", "backup"))
        if idx >= 0:
            self._sync_mode.setCurrentIndex(idx)
        self._source_path.setText(config.get("source", ""))
        self._dest_list.clear()
        for d in config.get("destinations", []):
            self._dest_list.addItem(d)
        self._cascade_check.setChecked(config.get("cascade", False))

    def show_preview_results(self, operations: list[dict]):
        """Display Trial Sync results in the preview tab."""
        self._preview_list.clear()
        self._preview_list.show()
        self._preview_label.hide()

        for op in operations:
            text = f"[{op.get('op', '?').upper()}] {op.get('rel_path', '')} — {op.get('reason', '')}"
            item = QListWidgetItem(text)
            op_type = op.get("op", "skip")
            if op_type in ("copy", "replace"):
                item.setForeground(Qt.GlobalColor.cyan)
            elif op_type == "delete":
                item.setForeground(Qt.GlobalColor.red)
            elif op_type == "skip":
                item.setForeground(Qt.GlobalColor.gray)
            self._preview_list.addItem(item)

        self._tabs.setCurrentIndex(3)  # Switch to Preview tab
