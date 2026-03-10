"""Filter rule editor — simple/intermediate/advanced modes.

Features:
- Mode switcher: Simple (toggles) -> Intermediate (rule builder) -> Advanced (expression)
- Rule builder: criteria dropdown + operator + value, add/remove rules
- Template save/load
- Live preview showing match/exclude count
"""

from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QComboBox,
    QLineEdit,
    QCheckBox,
    QListWidget,
    QListWidgetItem,
    QGroupBox,
    QStackedWidget,
    QFormLayout,
)

from config import FilterMode


class FilterRuleRow(QWidget):
    """Single filter rule row in intermediate mode."""
    removed = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 2, 0, 2)

        self.action_combo = QComboBox()
        self.action_combo.addItems(["Exclude", "Include"])
        self.action_combo.setFixedWidth(90)
        layout.addWidget(self.action_combo)

        self.criteria_combo = QComboBox()
        self.criteria_combo.addItems([
            "Filename", "Extension", "Path Pattern", "Regex",
            "Size Min (bytes)", "Size Max (bytes)",
            "Date After", "Date Before",
            "Is Hidden", "Is Symlink",
            "Depth Min", "Depth Max",
        ])
        self.criteria_combo.setFixedWidth(140)
        layout.addWidget(self.criteria_combo)

        self.value_input = QLineEdit()
        self.value_input.setPlaceholderText("Value...")
        layout.addWidget(self.value_input)

        self.enabled_check = QCheckBox()
        self.enabled_check.setChecked(True)
        self.enabled_check.setToolTip("Enable/disable this rule")
        layout.addWidget(self.enabled_check)

        btn_remove = QPushButton("X")
        btn_remove.setFixedSize(28, 28)
        btn_remove.setStyleSheet("QPushButton { color: #F44336; }")
        btn_remove.clicked.connect(lambda: self.removed.emit(self))
        layout.addWidget(btn_remove)

    def to_dict(self) -> dict:
        criteria_map = {
            "Filename": "filename", "Extension": "extension",
            "Path Pattern": "path_pattern", "Regex": "regex",
            "Size Min (bytes)": "size_min", "Size Max (bytes)": "size_max",
            "Date After": "date_after", "Date Before": "date_before",
            "Is Hidden": "is_hidden", "Is Symlink": "is_symlink",
            "Depth Min": "depth_min", "Depth Max": "depth_max",
        }
        return {
            "action": self.action_combo.currentText().lower(),
            "criterion": criteria_map.get(self.criteria_combo.currentText(), "filename"),
            "value": self.value_input.text(),
            "enabled": self.enabled_check.isChecked(),
        }


class FilterEditorPanel(QWidget):
    """Filter editor with mode switching."""
    filter_changed = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        header = QLabel("Filter Rules")
        header.setStyleSheet("color: #fff; font-size: 22px; font-weight: 700;")
        layout.addWidget(header)

        # Mode switcher
        mode_row = QHBoxLayout()
        mode_row.addWidget(QLabel("Mode:"))
        self._mode_combo = QComboBox()
        self._mode_combo.addItems(["Simple", "Intermediate", "Advanced"])
        self._mode_combo.currentIndexChanged.connect(self._on_mode_changed)
        mode_row.addWidget(self._mode_combo)
        mode_row.addStretch()
        layout.addLayout(mode_row)

        # Stacked views
        self._stack = QStackedWidget()
        layout.addWidget(self._stack)

        self._setup_simple()
        self._setup_intermediate()
        self._setup_advanced()

        # Preview
        self._preview_label = QLabel("Configure rules above, then test against a directory.")
        self._preview_label.setStyleSheet("color: #888; font-size: 12px; padding: 8px;")
        layout.addWidget(self._preview_label)

    def _setup_simple(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        self._simple_hidden = QCheckBox("Ignore hidden files (.*)")
        self._simple_hidden.setChecked(True)
        layout.addWidget(self._simple_hidden)
        self._simple_junk = QCheckBox("Ignore .DS_Store / Thumbs.db")
        self._simple_junk.setChecked(True)
        layout.addWidget(self._simple_junk)
        self._simple_temp = QCheckBox("Ignore temp files (.tmp, .swp)")
        self._simple_temp.setChecked(True)
        layout.addWidget(self._simple_temp)
        self._simple_media = QCheckBox("Include only media files")
        layout.addWidget(self._simple_media)
        ext_row = QHBoxLayout()
        ext_row.addWidget(QLabel("Custom extensions:"))
        self._simple_ext = QLineEdit()
        self._simple_ext.setPlaceholderText("mov, mp4, r3d, braw")
        ext_row.addWidget(self._simple_ext)
        layout.addLayout(ext_row)
        layout.addStretch()
        self._stack.addWidget(w)

    def _setup_intermediate(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        self._rules_container = QVBoxLayout()
        layout.addLayout(self._rules_container)
        btn_add = QPushButton("+ Add Rule")
        btn_add.clicked.connect(self._add_rule)
        layout.addWidget(btn_add)
        layout.addStretch()
        self._stack.addWidget(w)
        self._rule_rows: list[FilterRuleRow] = []

    def _setup_advanced(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.addWidget(QLabel("Boolean Expression:"))
        self._expr_input = QLineEdit()
        self._expr_input.setPlaceholderText('(ext:mov OR ext:mp4) AND NOT name:*.tmp')
        layout.addWidget(self._expr_input)
        info = QLabel("Use AND, OR, NOT operators with criteria:value pairs.\n"
                       "Criteria: name, ext, path, regex, size_min, size_max, hidden, symlink")
        info.setStyleSheet("color: #888; font-size: 11px;")
        info.setWordWrap(True)
        layout.addWidget(info)
        layout.addStretch()
        self._stack.addWidget(w)

    def _on_mode_changed(self, index: int):
        self._stack.setCurrentIndex(index)

    def _add_rule(self):
        row = FilterRuleRow()
        row.removed.connect(self._remove_rule)
        self._rules_container.addWidget(row)
        self._rule_rows.append(row)

    def _remove_rule(self, row: FilterRuleRow):
        self._rules_container.removeWidget(row)
        self._rule_rows.remove(row)
        row.deleteLater()

    def get_config(self) -> dict:
        mode = self._mode_combo.currentText().lower()
        config = {"mode": mode}
        if mode == "simple":
            config["toggles"] = {
                "ignore_hidden": self._simple_hidden.isChecked(),
                "ignore_system_junk": self._simple_junk.isChecked(),
                "ignore_temp": self._simple_temp.isChecked(),
                "media_only": self._simple_media.isChecked(),
            }
            config["custom_extensions"] = self._simple_ext.text()
        elif mode == "intermediate":
            config["rules"] = [r.to_dict() for r in self._rule_rows]
        elif mode == "advanced":
            config["expression"] = self._expr_input.text()
        return config
