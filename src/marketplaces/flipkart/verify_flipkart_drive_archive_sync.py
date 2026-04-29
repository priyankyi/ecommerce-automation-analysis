from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import OUTPUT_DIR, normalize_text

RUNS_DIR = OUTPUT_DIR / "runs"
SYNC_SUMMARY_FILENAME = "drive_sync_summary.json"


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in {429, 500, 502, 503} or attempt == attempts:
                raise
            import time

            time.sleep(delay)
            delay *= 2


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def get_latest_completed_run_dir() -> Path:
    if not RUNS_DIR.exists():
        raise FileNotFoundError(f"Missing Flipkart runs folder: {RUNS_DIR}")
    candidates = [
        path
        for path in RUNS_DIR.iterdir()
        if path.is_dir() and path.name.startswith("FLIPKART_") and (path / "pipeline_run_summary.json").exists()
    ]
    if not candidates:
        raise FileNotFoundError(f"No completed Flipkart runs found in: {RUNS_DIR}")
    return sorted(candidates, key=lambda path: path.name)[-1]


def resolve_run_dir(run_id: str | None) -> Path:
    if run_id:
        run_dir = RUNS_DIR / run_id
        if not run_dir.exists():
            raise FileNotFoundError(f"Missing Flipkart run folder: {run_dir}")
        return run_dir
    return get_latest_completed_run_dir()


def parse_drive_folder_id(url: str) -> str:
    match = re.search(r"/folders/([A-Za-z0-9_-]+)", url)
    return match.group(1) if match else ""


def list_drive_children(drive_service, folder_id: str) -> List[Dict[str, Any]]:
    response = retry(
        lambda: drive_service.files()
        .list(
            q=f"trashed = false and '{folder_id}' in parents",
            fields="files(id, name, mimeType, size)",
            pageSize=1000,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        .execute()
    )
    return list(response.get("files", []))


def build_drive_file_map(drive_service, folder_id: str, relative_prefix: Path | None = None) -> Set[str]:
    relative_prefix = relative_prefix or Path(".")
    available: Set[str] = set()
    for child in list_drive_children(drive_service, folder_id):
        child_name = str(child.get("name", ""))
        mime_type = str(child.get("mimeType", ""))
        child_relative = relative_prefix / child_name
        if mime_type == "application/vnd.google-apps.folder":
            available.update(build_drive_file_map(drive_service, str(child["id"]), child_relative))
        else:
            available.add(str(child_relative).replace("\\", "/"))
    return available


def verify_flipkart_drive_archive_sync(run_id: str | None = None) -> Dict[str, Any]:
    run_dir = resolve_run_dir(run_id)
    summary_path = run_dir / "pipeline_run_summary.json"
    sync_summary_path = run_dir / SYNC_SUMMARY_FILENAME

    if not summary_path.exists():
        raise FileNotFoundError(f"Missing required file: {summary_path}")
    if not sync_summary_path.exists():
        raise FileNotFoundError(f"Missing required file: {sync_summary_path}")

    pipeline_summary = load_json(summary_path)
    sync_summary = load_json(sync_summary_path)
    resolved_run_id = str(sync_summary.get("run_id") or pipeline_summary.get("run_id") or run_dir.name)
    drive_run_folder_url = str(sync_summary.get("drive_run_folder_url", "") or "")
    drive_run_folder_id = str(sync_summary.get("drive_run_folder_id", "") or "") or parse_drive_folder_id(drive_run_folder_url)
    if not drive_run_folder_url:
        raise FileNotFoundError(f"Missing drive run folder URL in sync summary: {sync_summary_path}")
    if not drive_run_folder_id:
        raise FileNotFoundError(f"Unable to parse drive folder ID from URL: {drive_run_folder_url}")

    _, drive_service, _ = build_services()
    folder_metadata = retry(
        lambda: drive_service.files()
        .get(
            fileId=drive_run_folder_id,
            fields="id, name, mimeType, trashed",
            supportsAllDrives=True,
        )
        .execute()
    )

    expected_relative_files = ["pipeline_run_summary.json"]
    for file_name in ["input_manifest.csv", "flipkart_sku_analysis.csv", "backup_before_push.csv"]:
        if (run_dir / file_name).exists():
            expected_relative_files.append(file_name)

    drive_file_paths = build_drive_file_map(drive_service, drive_run_folder_id)
    missing_files = [relative for relative in expected_relative_files if relative.replace("\\", "/") not in drive_file_paths]

    checks = {
        "local_run_folder_exists": run_dir.exists() and run_dir.is_dir(),
        "pipeline_run_summary_exists": summary_path.exists(),
        "drive_sync_summary_exists": sync_summary_path.exists(),
        "drive_run_folder_url_present": bool(drive_run_folder_url),
        "drive_run_folder_found": str(folder_metadata.get("mimeType", "")) == "application/vnd.google-apps.folder",
        "expected_files_present": not missing_files,
        "sync_summary_status_success": normalize_text(sync_summary.get("status", "")).upper() == "SUCCESS",
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "run_id": resolved_run_id,
        "drive_run_folder_url": drive_run_folder_url,
        "expected_files_checked": expected_relative_files,
        "missing_files": missing_files,
        "checks": checks,
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Verify that a Flipkart run archive exists in Google Drive.")
    parser.add_argument("--run-id", default="", help="Verify a specific Flipkart run folder by run_id.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        print(json.dumps(verify_flipkart_drive_archive_sync(run_id=args.run_id or None), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
