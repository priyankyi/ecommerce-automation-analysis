from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
DRIVE_FOLDERS_PATH = PROJECT_ROOT / "data" / "output" / "drive_folders.json"
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "image_folder_creation_log.csv"

PARENT_FOLDER_KEY = "02_PRODUCT_IMAGES"
SKU_FOLDER_CHILDREN = [
    "01_HERO",
    "02_FEATURES",
    "03_DIMENSIONS",
    "04_USE_CASE",
    "05_INSTALLATION",
    "06_FINAL_EXPORTS",
]


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be 1 or greater")
    result = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def find_folder(drive_service, name: str, parent_id: str | None = None) -> str | None:
    safe_name = name.replace("'", "\\'")
    query_parts = [
        "mimeType = 'application/vnd.google-apps.folder'",
        f"name = '{safe_name}'",
        "trashed = false",
    ]
    if parent_id:
        query_parts.append(f"'{parent_id}' in parents")
    query = " and ".join(query_parts)
    response = (
        drive_service.files()
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
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]
    folder = drive_service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


def ensure_folder(drive_service, name: str, parent_id: str | None = None) -> Tuple[str, bool]:
    folder_id = find_folder(drive_service, name, parent_id=parent_id)
    if folder_id:
        return folder_id, False
    return create_folder(drive_service, name, parent_id=parent_id), True


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[str]]:
    response = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def find_header_index(headers: List[str], header_name: str) -> int | None:
    for index, header in enumerate(headers):
        if header == header_name:
            return index
    return None


def ensure_image_folder_link_column(
    sheets_service,
    spreadsheet_id: str,
    headers: List[str],
    rows: List[List[str]],
) -> Tuple[List[str], List[List[str]], int, bool]:
    link_header = "Image_Folder_Link"
    link_index = find_header_index(headers, link_header)
    created = False

    if link_index is None:
        headers = headers + [link_header]
        link_index = len(headers) - 1
        created = True
        padded_rows: List[List[str]] = []
        for row in rows:
            padded = list(row)
            while len(padded) < link_index:
                padded.append("")
            padded.append("")
            padded_rows.append(padded)
        rows = padded_rows

        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"MASTER_SKU!A1:{column_index_to_a1(len(headers))}1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

    return headers, rows, link_index, created


def write_folder_links(
    sheets_service,
    spreadsheet_id: str,
    link_index: int,
    row_links: List[Tuple[int, str]],
) -> None:
    if not row_links:
        return

    data = []
    column_letter = column_index_to_a1(link_index + 1)
    for row_number, link in row_links:
        data.append(
            {
                "range": f"MASTER_SKU!{column_letter}{row_number}",
                "values": [[link]],
            }
        )

    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "RAW",
            "data": data,
        },
    ).execute()


def append_log_rows(log_rows: List[Dict[str, object]]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_exists = LOG_PATH.exists()
    with LOG_PATH.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp",
                "sku_id",
                "row_number",
                "sku_folder_id",
                "sku_folder_link",
                "status",
                "message",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_sku_image_structure() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    drive_meta = load_json(DRIVE_FOLDERS_PATH)

    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, drive_service, _ = build_services()

    product_images_parent_id = drive_meta["subfolders"][PARENT_FOLDER_KEY]

    sheet_rows = get_sheet_values(sheets_service, spreadsheet_id, "MASTER_SKU!A1:ZZ")
    if not sheet_rows:
        raise ValueError("MASTER_SKU sheet is empty or missing headers.")

    headers = sheet_rows[0]
    data_rows = sheet_rows[1:]
    headers, data_rows, link_index, created_column = ensure_image_folder_link_column(
        sheets_service,
        spreadsheet_id,
        headers,
        data_rows,
    )

    sku_index = find_header_index(headers, "SKU_ID")
    if sku_index is None:
        raise ValueError("SKU_ID header was not found in MASTER_SKU.")

    folder_cache: Dict[str, str] = {}
    log_rows: List[Dict[str, object]] = []
    link_updates: List[Tuple[int, str]] = []
    processed_count = 0

    for offset, row in enumerate(data_rows, start=2):
        sku_id = row[sku_index].strip() if sku_index < len(row) else ""
        if not sku_id:
            log_rows.append(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "sku_id": "",
                    "row_number": offset,
                    "sku_folder_id": "",
                    "sku_folder_link": "",
                    "status": "skipped",
                    "message": "Blank SKU_ID row",
                }
            )
            continue

        if sku_id in folder_cache:
            sku_folder_id = folder_cache[sku_id]
            folder_created = False
        else:
            sku_folder_id, folder_created = ensure_folder(
                drive_service, sku_id, parent_id=product_images_parent_id
            )
            for child_name in SKU_FOLDER_CHILDREN:
                ensure_folder(drive_service, child_name, parent_id=sku_folder_id)
            folder_cache[sku_id] = sku_folder_id

        sku_folder_link = f"https://drive.google.com/drive/folders/{sku_folder_id}"
        link_updates.append((offset, sku_folder_link))
        processed_count += 1
        log_rows.append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "sku_id": sku_id,
                "row_number": offset,
                "sku_folder_id": sku_folder_id,
                "sku_folder_link": sku_folder_link,
                "status": "created" if folder_created else "found",
                "message": "SKU image folder ready",
            }
        )

    write_folder_links(sheets_service, spreadsheet_id, link_index, link_updates)
    append_log_rows(log_rows)

    return {
        "spreadsheet_id": spreadsheet_id,
        "sheet_name": "MASTER_SKU",
        "image_parent_folder_id": product_images_parent_id,
        "image_parent_folder_name": PARENT_FOLDER_KEY,
        "link_column_created": created_column,
        "processed_rows": processed_count,
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_sku_image_structure()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
