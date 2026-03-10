"""Rule-based file filtering engine — 3-tier system.

Tiers:
- SIMPLE:       5 toggle rules (hidden files, system junk, temp, media-only, extension list)
- INTERMEDIATE: Custom rules from 12+ criteria (name, ext, size, date, path, regex)
- ADVANCED:     Boolean expressions combining rules with AND/OR/NOT

Filter rules are evaluated against file metadata to decide include/exclude.
"""

from __future__ import annotations

import fnmatch
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

from config import FilterMode


# ── Rule Criteria ─────────────────────────────────────────────────────────────
class Criterion(Enum):
    FILENAME = "filename"
    EXTENSION = "extension"
    PATH_PATTERN = "path_pattern"
    REGEX = "regex"
    SIZE_MIN = "size_min"           # bytes
    SIZE_MAX = "size_max"           # bytes
    DATE_AFTER = "date_after"       # mtime > timestamp
    DATE_BEFORE = "date_before"     # mtime < timestamp
    IS_HIDDEN = "is_hidden"
    IS_SYMLINK = "is_symlink"
    DEPTH_MIN = "depth_min"
    DEPTH_MAX = "depth_max"


class RuleAction(Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"


# ── Rule Definition ───────────────────────────────────────────────────────────
@dataclass
class FilterRule:
    """A single filter rule."""
    criterion: Criterion
    value: str                      # interpretation depends on criterion
    action: RuleAction = RuleAction.EXCLUDE
    enabled: bool = True
    label: str = ""                 # human-readable description

    def matches(self, path: Path, rel_path: str, stat: os.stat_result, depth: int) -> bool:
        """Test whether this rule matches a file."""
        if not self.enabled:
            return False

        c = self.criterion
        v = self.value

        if c == Criterion.FILENAME:
            return fnmatch.fnmatch(path.name, v)

        elif c == Criterion.EXTENSION:
            exts = {e.strip().lower().lstrip(".") for e in v.split(",")}
            return path.suffix.lower().lstrip(".") in exts

        elif c == Criterion.PATH_PATTERN:
            return fnmatch.fnmatch(rel_path, v)

        elif c == Criterion.REGEX:
            try:
                return bool(re.search(v, rel_path))
            except re.error:
                return False

        elif c == Criterion.SIZE_MIN:
            try:
                return stat.st_size >= int(v)
            except (ValueError, TypeError):
                return False

        elif c == Criterion.SIZE_MAX:
            try:
                return stat.st_size <= int(v)
            except (ValueError, TypeError):
                return False

        elif c == Criterion.DATE_AFTER:
            try:
                return stat.st_mtime >= float(v)
            except (ValueError, TypeError):
                return False

        elif c == Criterion.DATE_BEFORE:
            try:
                return stat.st_mtime <= float(v)
            except (ValueError, TypeError):
                return False

        elif c == Criterion.IS_HIDDEN:
            return path.name.startswith(".")

        elif c == Criterion.IS_SYMLINK:
            return path.is_symlink()

        elif c == Criterion.DEPTH_MIN:
            try:
                return depth >= int(v)
            except (ValueError, TypeError):
                return False

        elif c == Criterion.DEPTH_MAX:
            try:
                return depth <= int(v)
            except (ValueError, TypeError):
                return False

        return False


# ── Simple Mode Presets ───────────────────────────────────────────────────────
SIMPLE_PRESETS = {
    "ignore_hidden": FilterRule(
        criterion=Criterion.IS_HIDDEN, value="true",
        action=RuleAction.EXCLUDE, label="Ignore hidden files (.*)",
    ),
    "ignore_system_junk": FilterRule(
        criterion=Criterion.FILENAME, value=".DS_Store",
        action=RuleAction.EXCLUDE, label="Ignore .DS_Store / Thumbs.db",
    ),
    "ignore_thumbs": FilterRule(
        criterion=Criterion.FILENAME, value="Thumbs.db",
        action=RuleAction.EXCLUDE, label="Ignore Thumbs.db",
    ),
    "ignore_temp": FilterRule(
        criterion=Criterion.EXTENSION, value="tmp,temp,swp,swo",
        action=RuleAction.EXCLUDE, label="Ignore temp files",
    ),
    "media_only": FilterRule(
        criterion=Criterion.EXTENSION,
        value="mov,mp4,mxf,avi,r3d,braw,ari,dpx,exr,wav,aiff,mp3,aac,jpg,jpeg,png,tiff,tif,cr2,cr3,arw,nef,dng",
        action=RuleAction.INCLUDE, label="Include only media files",
    ),
}


# ── Advanced Boolean Expression ───────────────────────────────────────────────
@dataclass
class BoolExpr:
    """Boolean expression node for advanced filtering.

    Leaf nodes have a rule; branch nodes have an operator and children.
    """
    operator: Optional[str] = None      # "AND", "OR", "NOT", or None for leaf
    rule: Optional[FilterRule] = None
    children: list["BoolExpr"] = field(default_factory=list)

    def evaluate(self, path: Path, rel_path: str, stat: os.stat_result, depth: int) -> bool:
        if self.rule is not None:
            return self.rule.matches(path, rel_path, stat, depth)

        if self.operator == "NOT":
            if self.children:
                return not self.children[0].evaluate(path, rel_path, stat, depth)
            return True

        if self.operator == "AND":
            return all(c.evaluate(path, rel_path, stat, depth) for c in self.children)

        if self.operator == "OR":
            return any(c.evaluate(path, rel_path, stat, depth) for c in self.children)

        return True


# ── Filter Set ────────────────────────────────────────────────────────────────
@dataclass
class FilterSet:
    """A complete filter configuration."""
    name: str = "Default"
    mode: FilterMode = FilterMode.SIMPLE
    simple_toggles: dict[str, bool] = field(default_factory=dict)
    rules: list[FilterRule] = field(default_factory=list)
    expression: Optional[BoolExpr] = None
    custom_extensions: str = ""     # comma-separated for simple mode

    def should_include(self, path: Path, rel_path: str, root: Path) -> bool:
        """Decide whether a file should be included in the transfer/sync."""
        try:
            stat = path.stat()
        except OSError:
            return False

        depth = len(Path(rel_path).parts) - 1

        if self.mode == FilterMode.SIMPLE:
            return self._eval_simple(path, rel_path, stat, depth)
        elif self.mode == FilterMode.INTERMEDIATE:
            return self._eval_intermediate(path, rel_path, stat, depth)
        elif self.mode == FilterMode.ADVANCED:
            return self._eval_advanced(path, rel_path, stat, depth)
        return True

    def _eval_simple(self, path: Path, rel_path: str, stat: os.stat_result, depth: int) -> bool:
        """Simple mode: toggle-based rules."""
        for key, enabled in self.simple_toggles.items():
            if not enabled:
                continue
            preset = SIMPLE_PRESETS.get(key)
            if preset is None:
                continue

            if preset.action == RuleAction.INCLUDE:
                # Include rule: file MUST match to be included
                if not preset.matches(path, rel_path, stat, depth):
                    return False
            else:
                # Exclude rule: if matches, exclude
                if preset.matches(path, rel_path, stat, depth):
                    return False

        # Custom extension filter
        if self.custom_extensions:
            exts = {e.strip().lower().lstrip(".") for e in self.custom_extensions.split(",")}
            if path.suffix.lower().lstrip(".") not in exts:
                return False

        return True

    def _eval_intermediate(self, path: Path, rel_path: str, stat: os.stat_result, depth: int) -> bool:
        """Intermediate mode: custom rule list, all exclude rules applied."""
        for rule in self.rules:
            if not rule.enabled:
                continue
            if rule.matches(path, rel_path, stat, depth):
                if rule.action == RuleAction.EXCLUDE:
                    return False
                # INCLUDE rules: if at least one include rule exists,
                # file must match at least one include rule
        # Check if there are include rules
        include_rules = [r for r in self.rules if r.enabled and r.action == RuleAction.INCLUDE]
        if include_rules:
            return any(r.matches(path, rel_path, stat, depth) for r in include_rules)
        return True

    def _eval_advanced(self, path: Path, rel_path: str, stat: os.stat_result, depth: int) -> bool:
        """Advanced mode: boolean expression tree."""
        if self.expression is None:
            return True
        return self.expression.evaluate(path, rel_path, stat, depth)


# ── Apply Filter to File List ─────────────────────────────────────────────────
def apply_filter(
    root: Path,
    rel_paths: list[str],
    filter_set: FilterSet,
) -> tuple[list[str], list[str]]:
    """Apply a FilterSet to a list of relative paths.

    Returns:
        (included, excluded) — two lists of relative paths.
    """
    root = Path(root)
    included, excluded = [], []
    for rp in rel_paths:
        fp = root / rp
        if filter_set.should_include(fp, rp, root):
            included.append(rp)
        else:
            excluded.append(rp)
    return included, excluded


# ── Template Save/Load ────────────────────────────────────────────────────────
def filter_to_dict(fs: FilterSet) -> dict:
    """Serialise a FilterSet to a JSON-compatible dict."""
    rules_list = []
    for r in fs.rules:
        rules_list.append({
            "criterion": r.criterion.value,
            "value": r.value,
            "action": r.action.value,
            "enabled": r.enabled,
            "label": r.label,
        })
    return {
        "name": fs.name,
        "mode": fs.mode.value,
        "simple_toggles": fs.simple_toggles,
        "rules": rules_list,
        "custom_extensions": fs.custom_extensions,
    }


def filter_from_dict(d: dict) -> FilterSet:
    """Deserialise a FilterSet from a dict."""
    rules = []
    for rd in d.get("rules", []):
        rules.append(FilterRule(
            criterion=Criterion(rd["criterion"]),
            value=rd["value"],
            action=RuleAction(rd.get("action", "exclude")),
            enabled=rd.get("enabled", True),
            label=rd.get("label", ""),
        ))
    return FilterSet(
        name=d.get("name", "Imported"),
        mode=FilterMode(d.get("mode", "simple")),
        simple_toggles=d.get("simple_toggles", {}),
        rules=rules,
        custom_extensions=d.get("custom_extensions", ""),
    )
