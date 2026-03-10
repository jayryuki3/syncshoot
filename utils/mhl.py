"""ASC MHL (Media Hash List) support.

Generates and parses MHL XML files per the ASC MHL standard for
media transfer verification.

Features:
- MHL XML generation per transfer
- MHL parsing and checksum extraction
- MHL-aware transfers: detect existing MHLs, reuse checksums
- Batch MHL verification across volumes
"""

from __future__ import annotations

import os
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from xml.dom import minidom

from config import APP_NAME, APP_VERSION, HashAlgorithm


@dataclass
class MHLEntry:
    """One file entry in an MHL manifest."""
    rel_path: str
    file_size: int
    hash_algo: str          # "xxh3_64", "xxh64", "md5", "sha1"
    hash_value: str
    last_modified: Optional[str] = None


@dataclass
class MHLManifest:
    """A complete MHL manifest."""
    creator: str = APP_NAME
    version: str = APP_VERSION
    created_at: str = ""
    tool: str = f"{APP_NAME} {APP_VERSION}"
    entries: list[MHLEntry] = field(default_factory=list)
    source_path: str = ""
    dest_path: str = ""

    @property
    def total_files(self) -> int:
        return len(self.entries)

    @property
    def total_size(self) -> int:
        return sum(e.file_size for e in self.entries)


# ── MHL Algo Name Mapping ────────────────────────────────────────────────────
_ALGO_TO_MHL = {
    "xxh3_64": "xxh3_64",
    "xxh64": "xxh64",
    "md5": "md5",
    "sha1": "sha1",
}

_MHL_TO_ALGO = {v: k for k, v in _ALGO_TO_MHL.items()}


# ── Generate MHL XML ──────────────────────────────────────────────────────────
def generate_mhl(
    manifest: MHLManifest,
    output_path: Path,
) -> Path:
    """Write an MHL XML file from a manifest.

    Args:
        manifest: The MHLManifest to serialise.
        output_path: Where to write the .mhl file.

    Returns:
        The path to the written MHL file.
    """
    root = ET.Element("hashlist", version="2.0")

    # Creator info
    creator_info = ET.SubElement(root, "creatorinfo")
    ET.SubElement(creator_info, "name").text = manifest.creator
    ET.SubElement(creator_info, "version").text = manifest.version
    ET.SubElement(creator_info, "tool").text = manifest.tool
    creation_date = manifest.created_at or datetime.now(timezone.utc).isoformat()
    ET.SubElement(creator_info, "creationdate").text = creation_date

    # File entries
    for entry in manifest.entries:
        hash_elem = ET.SubElement(root, "hash")
        ET.SubElement(hash_elem, "file").text = entry.rel_path
        ET.SubElement(hash_elem, "size").text = str(entry.file_size)

        algo_name = _ALGO_TO_MHL.get(entry.hash_algo, entry.hash_algo)
        ET.SubElement(hash_elem, algo_name).text = entry.hash_value

        if entry.last_modified:
            ET.SubElement(hash_elem, "lastmodificationdate").text = entry.last_modified

    # Pretty-print
    xml_str = ET.tostring(root, encoding="unicode")
    pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(pretty, encoding="utf-8")
    return output_path


# ── Parse MHL XML ─────────────────────────────────────────────────────────────
def parse_mhl(mhl_path: Path) -> MHLManifest:
    """Parse an MHL file and return a manifest.

    Handles both MHL v1 and v2 structures.
    """
    tree = ET.parse(str(mhl_path))
    root = tree.getroot()

    manifest = MHLManifest()

    # Creator info
    ci = root.find("creatorinfo")
    if ci is not None:
        manifest.creator = (ci.findtext("name") or "").strip()
        manifest.version = (ci.findtext("version") or "").strip()
        manifest.tool = (ci.findtext("tool") or "").strip()
        manifest.created_at = (ci.findtext("creationdate") or "").strip()

    # Hash entries
    for hash_elem in root.findall("hash"):
        file_path = (hash_elem.findtext("file") or "").strip()
        file_size = int(hash_elem.findtext("size") or "0")

        # Find the hash algorithm and value
        hash_algo = ""
        hash_value = ""
        for algo_name in ("xxh3_64", "xxh64", "md5", "sha1", "xxh128", "sha256"):
            val = hash_elem.findtext(algo_name)
            if val:
                hash_algo = _MHL_TO_ALGO.get(algo_name, algo_name)
                hash_value = val.strip()
                break

        last_mod = (hash_elem.findtext("lastmodificationdate") or "").strip()

        if file_path and hash_value:
            manifest.entries.append(MHLEntry(
                rel_path=file_path,
                file_size=file_size,
                hash_algo=hash_algo,
                hash_value=hash_value,
                last_modified=last_mod or None,
            ))

    return manifest


# ── Find MHL Files on Volume ──────────────────────────────────────────────────
def find_mhl_files(root: Path) -> list[Path]:
    """Search for .mhl files under a root directory."""
    root = Path(root)
    results = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith(".mhl"):
                results.append(Path(dirpath) / fn)
    return sorted(results)


# ── Extract Checksums from MHL ────────────────────────────────────────────────
def extract_checksums(mhl_path: Path) -> dict[str, str]:
    """Parse an MHL and return {rel_path: hash_value} dict.

    Useful for verification against an existing MHL.
    """
    manifest = parse_mhl(mhl_path)
    return {e.rel_path: e.hash_value for e in manifest.entries}


# ── Generate MHL from Transfer ────────────────────────────────────────────────
def generate_transfer_mhl(
    src_root: Path,
    dst_root: Path,
    file_hashes: dict[str, tuple[str, str]],  # {rel_path: (algo, digest)}
    output_dir: Optional[Path] = None,
) -> Path:
    """Generate an MHL file for a completed transfer.

    Args:
        src_root: Source root directory.
        dst_root: Destination root directory.
        file_hashes: Dict of {rel_path: (algorithm_name, hex_digest)}.
        output_dir: Where to save MHL. Defaults to dst_root.

    Returns:
        Path to the generated MHL file.
    """
    manifest = MHLManifest(
        source_path=str(src_root),
        dest_path=str(dst_root),
        created_at=datetime.now(timezone.utc).isoformat(),
    )

    for rel_path, (algo, digest) in file_hashes.items():
        fp = dst_root / rel_path
        try:
            size = fp.stat().st_size
            mtime = datetime.fromtimestamp(fp.stat().st_mtime, tz=timezone.utc).isoformat()
        except OSError:
            size = 0
            mtime = None

        manifest.entries.append(MHLEntry(
            rel_path=rel_path,
            file_size=size,
            hash_algo=algo,
            hash_value=digest,
            last_modified=mtime,
        ))

    if output_dir is None:
        output_dir = dst_root

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mhl_name = f"{src_root.name}_{timestamp}.mhl"
    return generate_mhl(manifest, output_dir / mhl_name)
