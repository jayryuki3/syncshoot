"""Post-transfer summary report generator.

Produces comprehensive reports of transfer outcomes:
- All successful transfers (file count, total size, speed, checksums)
- All failed transfers (file, reason, retry status)
- All corrupted files (checksum mismatches, truncated writes, 0-byte copies)

Output formats: HTML, CSV, JSON
"""

from __future__ import annotations

import csv
import io
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import (
    APP_NAME,
    APP_VERSION,
    COLORS,
    REPORTS_DIR,
    FileOpStatus,
    ReportFormat,
    TransferStatus,
)


# ── Report Data Structures ────────────────────────────────────────────────────
@dataclass
class FileReportEntry:
    """One file's transfer outcome."""
    rel_path: str
    status: FileOpStatus
    size: int = 0
    src_hash: Optional[str] = None
    dst_hash: Optional[str] = None
    speed_bps: float = 0.0
    error: Optional[str] = None
    destination: str = ""


@dataclass
class TransferReport:
    """Complete post-transfer summary report."""
    task_name: str
    source: str
    destinations: list[str] = field(default_factory=list)
    started_at: float = 0.0
    finished_at: float = 0.0
    status: TransferStatus = TransferStatus.COMPLETE
    verify_mode: str = ""
    hash_algorithm: str = ""

    # File entries
    files: list[FileReportEntry] = field(default_factory=list)

    # Aggregate counts (computed from files)
    @property
    def total_files(self) -> int:
        return len(self.files)

    @property
    def successful(self) -> list[FileReportEntry]:
        return [f for f in self.files if f.status == FileOpStatus.SUCCESS]

    @property
    def failed(self) -> list[FileReportEntry]:
        return [f for f in self.files if f.status == FileOpStatus.FAILED]

    @property
    def corrupted(self) -> list[FileReportEntry]:
        return [f for f in self.files if f.status == FileOpStatus.CORRUPTED]

    @property
    def truncated(self) -> list[FileReportEntry]:
        return [f for f in self.files if f.status == FileOpStatus.TRUNCATED]

    @property
    def skipped(self) -> list[FileReportEntry]:
        return [f for f in self.files if f.status == FileOpStatus.SKIPPED]

    @property
    def missing_source(self) -> list[FileReportEntry]:
        return [f for f in self.files if f.status == FileOpStatus.MISSING_SOURCE]

    @property
    def missing_dest(self) -> list[FileReportEntry]:
        return [f for f in self.files if f.status == FileOpStatus.MISSING_DEST]

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.successful)

    @property
    def avg_speed_mbps(self) -> float:
        speeds = [f.speed_bps for f in self.successful if f.speed_bps > 0]
        if not speeds:
            return 0.0
        return (sum(speeds) / len(speeds)) / (1024 * 1024)

    @property
    def elapsed_seconds(self) -> float:
        return self.finished_at - self.started_at if self.finished_at > self.started_at else 0.0

    @property
    def all_passed(self) -> bool:
        return len(self.failed) == 0 and len(self.corrupted) == 0 and len(self.truncated) == 0

    def summary_dict(self) -> dict:
        return {
            "task_name": self.task_name,
            "source": self.source,
            "destinations": self.destinations,
            "status": self.status.value,
            "started_at": datetime.fromtimestamp(self.started_at, tz=timezone.utc).isoformat() if self.started_at else "",
            "finished_at": datetime.fromtimestamp(self.finished_at, tz=timezone.utc).isoformat() if self.finished_at else "",
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "verify_mode": self.verify_mode,
            "hash_algorithm": self.hash_algorithm,
            "total_files": self.total_files,
            "successful": len(self.successful),
            "failed": len(self.failed),
            "corrupted": len(self.corrupted),
            "truncated": len(self.truncated),
            "skipped": len(self.skipped),
            "missing_source": len(self.missing_source),
            "missing_dest": len(self.missing_dest),
            "total_bytes": self.total_bytes,
            "avg_speed_mbps": round(self.avg_speed_mbps, 2),
            "all_passed": self.all_passed,
        }


# ── Format Helpers ────────────────────────────────────────────────────────────
def _fmt_size(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def _fmt_speed(bps: float) -> str:
    return _fmt_size(int(bps)) + "/s"


def _fmt_time(ts: float) -> str:
    if ts <= 0:
        return "N/A"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_duration(secs: float) -> str:
    if secs < 60:
        return f"{secs:.1f}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{int(m)}m {int(s)}s"
    h, m = divmod(m, 60)
    return f"{int(h)}h {int(m)}m {int(s)}s"


# ── JSON Report ───────────────────────────────────────────────────────────────
def generate_json_report(report: TransferReport) -> str:
    """Generate a JSON report string."""
    data = report.summary_dict()
    data["files"] = []
    for f in report.files:
        data["files"].append({
            "path": f.rel_path,
            "status": f.status.value,
            "size": f.size,
            "src_hash": f.src_hash,
            "dst_hash": f.dst_hash,
            "speed_bps": round(f.speed_bps, 2),
            "error": f.error,
            "destination": f.destination,
        })
    return json.dumps(data, indent=2)


# ── CSV Report ────────────────────────────────────────────────────────────────
def generate_csv_report(report: TransferReport) -> str:
    """Generate a CSV report string."""
    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([
        "File Path", "Status", "Size (bytes)", "Size (human)",
        "Source Hash", "Dest Hash", "Speed (bytes/s)", "Error", "Destination",
    ])

    for f in report.files:
        writer.writerow([
            f.rel_path,
            f.status.value,
            f.size,
            _fmt_size(f.size),
            f.src_hash or "",
            f.dst_hash or "",
            round(f.speed_bps, 2),
            f.error or "",
            f.destination,
        ])

    return output.getvalue()


# ── HTML Report ───────────────────────────────────────────────────────────────
def generate_html_report(report: TransferReport) -> str:
    """Generate a styled HTML report."""
    status_color = COLORS.get("complete" if report.all_passed else "failed", "#666")
    summary = report.summary_dict()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{APP_NAME} Transfer Report — {report.task_name}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #1a1a2e; color: #e0e0e0; padding: 2rem; }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  h1 {{ color: #fff; margin-bottom: 0.5rem; }}
  .subtitle {{ color: #888; margin-bottom: 2rem; font-size: 0.9rem; }}
  .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
              gap: 1rem; margin-bottom: 2rem; }}
  .stat-card {{ background: #16213e; border-radius: 8px; padding: 1.2rem;
                border-left: 4px solid #2196F3; }}
  .stat-card.success {{ border-left-color: {COLORS['complete']}; }}
  .stat-card.error {{ border-left-color: {COLORS['failed']}; }}
  .stat-card.warning {{ border-left-color: {COLORS['corrupted']}; }}
  .stat-label {{ font-size: 0.8rem; color: #888; text-transform: uppercase; }}
  .stat-value {{ font-size: 1.5rem; font-weight: 700; color: #fff; margin-top: 0.3rem; }}
  h2 {{ color: #fff; margin: 2rem 0 1rem; padding-bottom: 0.5rem;
        border-bottom: 1px solid #333; }}
  table {{ width: 100%; border-collapse: collapse; background: #16213e;
           border-radius: 8px; overflow: hidden; margin-bottom: 2rem; }}
  th {{ background: #0f3460; padding: 0.8rem; text-align: left; font-size: 0.85rem;
        text-transform: uppercase; color: #aaa; }}
  td {{ padding: 0.6rem 0.8rem; border-bottom: 1px solid #1a1a3e; font-size: 0.9rem; }}
  tr:hover {{ background: #1a1a3e; }}
  .status {{ padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }}
  .status-success {{ background: {COLORS['complete']}22; color: {COLORS['complete']}; }}
  .status-failed {{ background: {COLORS['failed']}22; color: {COLORS['failed']}; }}
  .status-corrupted {{ background: {COLORS['corrupted']}22; color: {COLORS['corrupted']}; }}
  .status-truncated {{ background: {COLORS['corrupted']}22; color: {COLORS['corrupted']}; }}
  .status-skipped {{ background: #9E9E9E22; color: #9E9E9E; }}
  .status-missing_source, .status-missing_dest {{ background: {COLORS['failed']}22; color: {COLORS['failed']}; }}
  .overall {{ font-size: 1.2rem; padding: 1rem; border-radius: 8px; margin-bottom: 2rem;
              background: {status_color}22; border: 1px solid {status_color}44; text-align: center; }}
  .footer {{ color: #555; font-size: 0.8rem; text-align: center; margin-top: 3rem; }}
</style>
</head>
<body>
<div class="container">
  <h1>{APP_NAME} Transfer Report</h1>
  <p class="subtitle">Task: {report.task_name} | {_fmt_time(report.started_at)}</p>

  <div class="overall">
    {{"ALL TRANSFERS PASSED" if report.all_passed else "ISSUES DETECTED — Review details below"}}
  </div>

  <div class="summary">
    <div class="stat-card success">
      <div class="stat-label">Successful</div>
      <div class="stat-value">{len(report.successful)}</div>
    </div>
    <div class="stat-card error">
      <div class="stat-label">Failed</div>
      <div class="stat-value">{len(report.failed)}</div>
    </div>
    <div class="stat-card warning">
      <div class="stat-label">Corrupted</div>
      <div class="stat-value">{len(report.corrupted)}</div>
    </div>
    <div class="stat-card warning">
      <div class="stat-label">Truncated</div>
      <div class="stat-value">{len(report.truncated)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total Data</div>
      <div class="stat-value">{_fmt_size(report.total_bytes)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Duration</div>
      <div class="stat-value">{_fmt_duration(report.elapsed_seconds)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Avg Speed</div>
      <div class="stat-value">{report.avg_speed_mbps:.1f} MB/s</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Skipped</div>
      <div class="stat-value">{len(report.skipped)}</div>
    </div>
  </div>

  <h2>Transfer Details</h2>
  <table>
    <tr><th>Source</th><td>{report.source}</td></tr>
    <tr><th>Destinations</th><td>{', '.join(report.destinations)}</td></tr>
    <tr><th>Verification</th><td>{report.verify_mode} ({report.hash_algorithm})</td></tr>
    <tr><th>Overall Status</th><td>{report.status.value.upper()}</td></tr>
  </table>
"""

    # Failed files section
    problems = report.failed + report.corrupted + report.truncated + report.missing_source + report.missing_dest
    if problems:
        html += """  <h2>Issues</h2>
  <table>
    <thead><tr><th>File</th><th>Status</th><th>Size</th><th>Error</th></tr></thead>
    <tbody>
"""
        for f in problems:
            status_cls = f"status-{f.status.value}"
            html += f"""      <tr>
        <td>{f.rel_path}</td>
        <td><span class="status {status_cls}">{f.status.value.upper()}</span></td>
        <td>{_fmt_size(f.size)}</td>
        <td>{f.error or 'N/A'}</td>
      </tr>
"""
        html += "    </tbody>\n  </table>\n"

    # Successful files section
    if report.successful:
        html += """  <h2>Successful Transfers</h2>
  <table>
    <thead><tr><th>File</th><th>Size</th><th>Speed</th><th>Checksum</th></tr></thead>
    <tbody>
"""
        for f in report.successful:
            checksum_display = f.src_hash[:16] + "..." if f.src_hash and len(f.src_hash) > 16 else (f.src_hash or "N/A")
            html += f"""      <tr>
        <td>{f.rel_path}</td>
        <td>{_fmt_size(f.size)}</td>
        <td>{_fmt_speed(f.speed_bps) if f.speed_bps > 0 else 'N/A'}</td>
        <td style="font-family: monospace; font-size: 0.8rem;">{checksum_display}</td>
      </tr>
"""
        html += "    </tbody>\n  </table>\n"

    html += f"""
  <div class="footer">
    Generated by {APP_NAME} v{APP_VERSION} | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
  </div>
</div>
</body>
</html>"""

    return html


# ── Save Report ───────────────────────────────────────────────────────────────
def save_report(
    report: TransferReport,
    fmt: ReportFormat = ReportFormat.HTML,
    output_dir: Optional[Path] = None,
) -> Path:
    """Generate and save a report file.

    Args:
        report: The TransferReport to render.
        fmt: Output format (HTML, CSV, JSON).
        output_dir: Directory to save into. Defaults to REPORTS_DIR.

    Returns:
        Path to the saved report file.
    """
    if output_dir is None:
        output_dir = REPORTS_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = report.task_name.replace(" ", "_").replace("/", "_")

    if fmt == ReportFormat.HTML:
        content = generate_html_report(report)
        ext = ".html"
    elif fmt == ReportFormat.CSV:
        content = generate_csv_report(report)
        ext = ".csv"
    elif fmt == ReportFormat.JSON:
        content = generate_json_report(report)
        ext = ".json"
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    filename = f"{safe_name}_{timestamp}{ext}"
    filepath = output_dir / filename
    filepath.write_text(content, encoding="utf-8")
    return filepath


# ── Build Report from Transfer Job ────────────────────────────────────────────
def build_report_from_job(
    task_name: str,
    job: "TransferJob",
    verify_report: Optional["VerifyReport"] = None,
) -> TransferReport:
    """Build a TransferReport from a completed TransferJob and optional VerifyReport.

    This bridges the copier/verifier outputs into the report system.
    """
    report = TransferReport(
        task_name=task_name,
        source=str(job.source),
        destinations=[str(d) for d in job.destinations],
        started_at=job.started_at,
        finished_at=job.finished_at,
        status=job.status,
        hash_algorithm=job.hash_algo.value,
    )

    # Build file entries from job
    verify_map = {}
    if verify_report:
        report.verify_mode = verify_report.mode.value
        for vr in verify_report.results:
            verify_map[vr.rel_path] = vr

    for fr in job.files:
        vr = verify_map.get(fr.rel)
        entry = FileReportEntry(
            rel_path=fr.rel,
            status=vr.status if vr else fr.status,
            size=fr.size,
            src_hash=vr.src_hash if vr else fr.src_hash,
            dst_hash=vr.dst_hash if vr else fr.dst_hash,
            speed_bps=fr.speed,
            error=vr.error if vr and vr.error else fr.error,
            destination=str(job.destinations[0]) if job.destinations else "",
        )
        report.files.append(entry)

    return report
