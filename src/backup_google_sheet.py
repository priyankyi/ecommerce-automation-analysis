from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
BACKUP_ROOT = PROJECT_ROOT / "data" / "output" / "backups"


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception:
            if attempt == attempts:
                raise
            import time

            time.sleep(delay)
            delay *= 2


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[str]]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
        .get("values", [])
    )


def get_tab_names(sheets_service, spreadsheet_id: str) -> List[str]:
    metadata = retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))")
        .execute()
    )
    return [sheet.get("properties", {}).get("title", "") for sheet in metadata.get("sheets", []) if sheet.get("properties", {}).get("title")]


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_") or "sheet"


def write_csv(path: Path, rows: List[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def backup_google_sheet() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = BACKUP_ROOT / timestamp
    backup_dir.mkdir(parents=True, exist_ok=True)

    exported_files: List[Dict[str, object]] = []
    for tab_name in get_tab_names(sheets_service, spreadsheet_id):
        rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
        output_path = backup_dir / f"{sanitize_filename(tab_name)}.csv"
        write_csv(output_path, rows)
        exported_files.append(
            {
                "tab_name": tab_name,
                "rows_exported": len(rows),
                "file_path": str(output_path),
            }
        )

    result = {
        "spreadsheet_id": spreadsheet_id,
        "backup_dir": str(backup_dir),
        "tabs_backed_up": len(exported_files),
        "files": exported_files,
    }
    print(json.dumps(result, indent=2))
    return result


def main() -> None:
    backup_google_sheet()


if __name__ == "__main__":
    main()
