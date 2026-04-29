from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, now_iso

RUNS_DIR = OUTPUT_DIR / "runs"
DRIVE_FOLDERS_PATH = PROJECT_ROOT / "data" / "output" / "drive_folders.json"
LOG_PATH = LOG_DIR / "flipkart_drive_archive_sync_log.csv"
SYNC_SUMMARY_HEADERS = [
    "timestamp",
    "run_id",
    "local_run_folder",
    "drive_run_folder_id",
    "drive_run_folder_url",
    "files_uploaded",
    "files_updated",
    "files_skipped",
    "status",
    "message",
]

ROOT_FOLDER_NAME = "ECOM_CONTROL_TOWER"
ARCHIVE_FOLDER_CHAIN = ["03_RUN_ARCHIVES", "FLIPKART"]
SYNC_SUMMARY_FILENAME = "drive_sync_summary.json"
DRIVE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SKIP_FILENAMES = {"credentials.json", "token.json", ".env"}
SKIP_DIRNAMES = {"credentials", ".venv", "__pycache__"}
SKIP_SUFFIXES = {".pyc"}


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in {429, 500, 502, 503} or attempt == attempts:
                raise
            time.sleep(delay)
            delay *= 2


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def escape_drive_query(value: str) -> str:
    return value.replace("'", "\\'")


def drive_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def get_folder_metadata(drive_service, folder_id: str) -> Dict[str, Any]:
    return retry(
        lambda: drive_service.files()
        .get(
            fileId=folder_id,
            fields="id, name, mimeType, trashed, size, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )


def load_drive_folder_map() -> Dict[str, Any]:
    if not DRIVE_FOLDERS_PATH.exists():
        return {}
    return load_json(DRIVE_FOLDERS_PATH)


def find_folder(drive_service, name: str, parent_id: str | None = None) -> str | None:
    query_parts = [
        f"mimeType = '{DRIVE_FOLDER_MIME_TYPE}'",
        f"name = '{escape_drive_query(name)}'",
        "trashed = false",
    ]
    if parent_id:
        query_parts.append(f"'{parent_id}' in parents")
    query = " and ".join(query_parts)
    response = retry(
        lambda: drive_service.files()
        .list(
            q=query,
            fields="files(id, name)",
            pageSize=10,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        .execute()
    )
    files = response.get("files", [])
    return files[0]["id"] if files else None


def create_folder(drive_service, name: str, parent_id: str | None = None) -> str:
    metadata = {"name": name, "mimeType": DRIVE_FOLDER_MIME_TYPE}
    if parent_id:
        metadata["parents"] = [parent_id]
    folder = retry(
        lambda: drive_service.files()
        .create(
            body=metadata,
            fields="id, name",
            supportsAllDrives=True,
        )
        .execute()
    )
    return str(folder["id"])


def ensure_folder(drive_service, name: str, parent_id: str | None = None) -> str:
    folder_id = find_folder(drive_service, name, parent_id=parent_id)
    if folder_id:
        return folder_id
    return create_folder(drive_service, name, parent_id=parent_id)


def resolve_root_folder_id(drive_service) -> str:
    folder_map = load_drive_folder_map()
    root_folder_id = str(folder_map.get("root_folder_id", "") or "")
    root_folder_name = str(folder_map.get("root_folder_name", "") or "")
    if root_folder_id:
        try:
            metadata = get_folder_metadata(drive_service, root_folder_id)
            if metadata.get("mimeType") == DRIVE_FOLDER_MIME_TYPE:
                return root_folder_id
        except Exception:
            pass
    if root_folder_name == ROOT_FOLDER_NAME:
        folder_id = find_folder(drive_service, ROOT_FOLDER_NAME)
        if folder_id:
            return folder_id
    folder_id = find_folder(drive_service, ROOT_FOLDER_NAME)
    if folder_id:
        return folder_id
    return create_folder(drive_service, ROOT_FOLDER_NAME)


def ensure_archive_root(drive_service) -> Tuple[str, str, str, str]:
    root_id = resolve_root_folder_id(drive_service)
    run_archives_id = ensure_folder(drive_service, ARCHIVE_FOLDER_CHAIN[0], parent_id=root_id)
    flipkart_archive_id = ensure_folder(drive_service, ARCHIVE_FOLDER_CHAIN[1], parent_id=run_archives_id)
    return root_id, run_archives_id, flipkart_archive_id, drive_url(flipkart_archive_id)


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
        if not (run_dir / "pipeline_run_summary.json").exists():
            raise FileNotFoundError(f"Run folder is not complete: {run_dir}")
        return run_dir
    return get_latest_completed_run_dir()


def is_unsafe_file(path: Path, run_dir: Path) -> bool:
    try:
        relative = path.relative_to(run_dir)
    except ValueError:
        return True
    parts = relative.parts
    if any(part in SKIP_DIRNAMES for part in parts[:-1]):
        return True
    if path.name in SKIP_FILENAMES:
        return True
    if path.suffix.lower() in SKIP_SUFFIXES:
        return True
    return False


def list_local_files(run_dir: Path) -> List[Path]:
    return sorted(path for path in run_dir.rglob("*") if path.is_file())


def ensure_drive_folder_chain(drive_service, parent_id: str, relative_dir: Path) -> str:
    current_parent = parent_id
    for part in relative_dir.parts:
        current_parent = ensure_folder(drive_service, part, parent_id=current_parent)
    return current_parent


def find_drive_files_by_name(drive_service, parent_id: str, name: str) -> List[Dict[str, Any]]:
    query = (
        "trashed = false and "
        f"'{parent_id}' in parents and "
        f"name = '{escape_drive_query(name)}'"
    )
    response = retry(
        lambda: drive_service.files()
        .list(
            q=query,
            fields="files(id, name, size, mimeType)",
            pageSize=100,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        .execute()
    )
    return list(response.get("files", []))


def upload_or_update_file(drive_service, local_path: Path, parent_id: str) -> Tuple[str, Dict[str, Any]]:
    media = MediaFileUpload(
        str(local_path),
        mimetype=mimetypes.guess_type(local_path.name)[0] or "application/octet-stream",
        resumable=False,
    )
    existing_files = find_drive_files_by_name(drive_service, parent_id, local_path.name)
    local_size = local_path.stat().st_size

    if existing_files:
        existing = existing_files[0]
        existing_size = safe_int(existing.get("size", 0))
        if existing_size == local_size:
            return "skipped", existing
        updated = retry(
            lambda: drive_service.files()
            .update(
                fileId=str(existing["id"]),
                media_body=media,
                fields="id, name, size, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        return "updated", updated

    created = retry(
        lambda: drive_service.files()
        .create(
            body={"name": local_path.name, "parents": [parent_id]},
            media_body=media,
            fields="id, name, size, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    return "uploaded", created


def sync_flipkart_run_archive_to_drive(run_id: str | None = None) -> Dict[str, Any]:
    run_dir = resolve_run_dir(run_id)
    summary_path = run_dir / "pipeline_run_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing required file: {summary_path}")

    summary = load_json(summary_path)
    resolved_run_id = str(summary.get("run_id") or run_dir.name)
    local_run_folder = run_dir

    _, drive_service, _ = build_services()
    _, _, flipkart_archive_id, archive_url = ensure_archive_root(drive_service)
    drive_run_folder_id = ensure_folder(drive_service, resolved_run_id, parent_id=flipkart_archive_id)
    drive_run_folder_url = drive_url(drive_run_folder_id)

    files_uploaded = 0
    files_updated = 0
    files_skipped = 0

    for local_path in list_local_files(run_dir):
        if is_unsafe_file(local_path, run_dir):
            files_skipped += 1
            continue
        relative_path = local_path.relative_to(run_dir)
        parent_relative = relative_path.parent
        drive_parent_id = drive_run_folder_id
        if str(parent_relative) not in {"", "."}:
            drive_parent_id = ensure_drive_folder_chain(drive_service, drive_run_folder_id, parent_relative)
        action, _ = upload_or_update_file(drive_service, local_path, drive_parent_id)
        if action == "uploaded":
            files_uploaded += 1
        elif action == "updated":
            files_updated += 1
        else:
            files_skipped += 1

    synced_at = now_iso()
    sync_summary = {
        "run_id": resolved_run_id,
        "drive_run_folder_id": drive_run_folder_id,
        "drive_run_folder_url": drive_run_folder_url,
        "files_uploaded": files_uploaded,
        "files_updated": files_updated,
        "files_skipped": files_skipped,
        "synced_at": synced_at,
        "status": "SUCCESS",
    }
    sync_summary_path = run_dir / SYNC_SUMMARY_FILENAME
    save_json(sync_summary_path, sync_summary)

    log_row = {
        "timestamp": synced_at,
        "run_id": resolved_run_id,
        "local_run_folder": str(local_run_folder),
        "drive_run_folder_id": drive_run_folder_id,
        "drive_run_folder_url": drive_run_folder_url,
        "files_uploaded": files_uploaded,
        "files_updated": files_updated,
        "files_skipped": files_skipped,
        "status": "SUCCESS",
        "message": f"Synced Flipkart run archive to {archive_url}",
    }
    append_csv_log(LOG_PATH, SYNC_SUMMARY_HEADERS, [log_row])

    result = {
        "status": "SUCCESS",
        "run_id": resolved_run_id,
        "local_run_folder": str(local_run_folder),
        "drive_run_folder_id": drive_run_folder_id,
        "drive_run_folder_url": drive_run_folder_url,
        "files_uploaded": files_uploaded,
        "files_updated": files_updated,
        "files_skipped": files_skipped,
        "log_path": str(LOG_PATH),
        "summary_path": str(sync_summary_path),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Sync a Flipkart run archive folder to Google Drive.")
    parser.add_argument("--run-id", default="", help="Sync a specific Flipkart run folder by run_id.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        sync_flipkart_run_archive_to_drive(run_id=args.run_id or None)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
