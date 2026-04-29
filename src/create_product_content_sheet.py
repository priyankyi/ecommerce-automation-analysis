from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "product_content_setup_log.csv"

MASTER_SKU_TAB = "MASTER_SKU"
CONTENT_TAB = "PRODUCT_CONTENT"

CONTENT_HEADERS = [
    "SKU_ID",
    "Product_Title",
    "Short_Title",
    "SEO_Title",
    "Description",
    "Bullet_1",
    "Bullet_2",
    "Bullet_3",
    "Bullet_4",
    "Bullet_5",
    "Primary_Keywords",
    "Secondary_Keywords",
    "Image_Text_Hero",
    "Image_Text_Features",
    "Image_Text_Dimensions",
    "Image_Text_Use_Case",
    "Image_Text_Installation",
    "Feature_Claims",
    "Content_Status",
    "QC_Remarks",
    "Created_Date",
    "Last_Updated",
]

CONTENT_STATUS_OPTIONS = [
    "Draft",
    "AI Generated",
    "Human Reviewed",
    "Approved",
    "Rejected",
    "Needs Correction",
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


def zero_based_column_index_to_a1(index: int) -> str:
    return column_index_to_a1(index + 1)


def get_spreadsheet_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, object]:
    return (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def find_tab(metadata: Dict[str, object], tab_name: str) -> Dict[str, object] | None:
    for sheet in metadata.get("sheets", []):
        properties = sheet.get("properties", {})
        if properties.get("title") == tab_name:
            return properties
    return None


def ensure_content_tab(sheets_service, spreadsheet_id: str) -> int:
    metadata = get_spreadsheet_metadata(sheets_service, spreadsheet_id)
    existing = find_tab(metadata, CONTENT_TAB)
    if existing and existing.get("sheetId") is not None:
        return existing["sheetId"]

    response = (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": CONTENT_TAB}}}]},
        )
        .execute()
    )
    return response["replies"][0]["addSheet"]["properties"]["sheetId"]


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[str]]:
    response = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def find_header_index(headers: Sequence[str], header_name: str) -> int | None:
    for index, header in enumerate(headers):
        if header == header_name:
            return index
    return None


def ensure_headers(
    sheets_service,
    spreadsheet_id: str,
    content_rows: List[List[str]],
) -> Tuple[List[str], int, bool]:
    header_row = content_rows[0] if content_rows else []
    if header_row[: len(CONTENT_HEADERS)] != CONTENT_HEADERS:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{CONTENT_TAB}!A1:{zero_based_column_index_to_a1(len(CONTENT_HEADERS) - 1)}1",
            valueInputOption="RAW",
            body={"values": [CONTENT_HEADERS]},
        ).execute()
        return CONTENT_HEADERS, len(CONTENT_HEADERS), True
    return header_row, len(header_row), False


def get_master_skus(sheets_service, spreadsheet_id: str) -> List[Tuple[str, str]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{MASTER_SKU_TAB}!A1:ZZ")
    if not rows:
        return []

    headers = rows[0]
    sku_index = find_header_index(headers, "SKU_ID")
    title_index = find_header_index(headers, "Product_Title")
    if sku_index is None:
        raise ValueError("SKU_ID header was not found in MASTER_SKU.")

    result: List[Tuple[str, str]] = []
    for row in rows[1:]:
        sku_id = row[sku_index].strip() if sku_index < len(row) else ""
        if not sku_id:
            continue
        product_title = row[title_index].strip() if title_index is not None and title_index < len(row) else ""
        result.append((sku_id, product_title))
    return result


def get_existing_content_skus(sheets_service, spreadsheet_id: str) -> Dict[str, int]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{CONTENT_TAB}!A1:ZZ")
    if len(rows) < 2:
        return {}

    headers = rows[0]
    sku_index = find_header_index(headers, "SKU_ID")
    if sku_index is None:
        return {}

    existing: Dict[str, int] = {}
    for row_number, row in enumerate(rows[1:], start=2):
        sku_id = row[sku_index].strip() if sku_index < len(row) else ""
        if sku_id and sku_id not in existing:
            existing[sku_id] = row_number
    return existing


def append_missing_rows(
    sheets_service,
    spreadsheet_id: str,
    master_rows: List[Tuple[str, str]],
    existing_skus: Dict[str, int],
) -> List[Dict[str, object]]:
    appended: List[List[str]] = []
    log_rows: List[Dict[str, object]] = []
    timestamp = datetime.now().isoformat(timespec="seconds")
    seen_master_skus: set[str] = set()

    for sku_id, product_title in master_rows:
        if sku_id in seen_master_skus:
            log_rows.append(
                {
                    "timestamp": timestamp,
                    "sku_id": sku_id,
                    "content_row_number": "",
                    "status": "skipped",
                    "message": "Duplicate SKU_ID in MASTER_SKU",
                }
            )
            continue

        seen_master_skus.add(sku_id)

        if sku_id in existing_skus:
            log_rows.append(
                {
                    "timestamp": timestamp,
                    "sku_id": sku_id,
                    "content_row_number": existing_skus[sku_id],
                    "status": "skipped",
                    "message": "SKU already exists in PRODUCT_CONTENT",
                }
            )
            continue

        row = [""] * len(CONTENT_HEADERS)
        row[0] = sku_id
        row[1] = product_title
        row[18] = "Draft"
        appended.append(row)
        log_rows.append(
            {
                "timestamp": timestamp,
                "sku_id": sku_id,
                "content_row_number": "",
                "status": "created",
                "message": "PRODUCT_CONTENT row seeded",
            }
        )

    if appended:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{CONTENT_TAB}!A:V",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appended},
        ).execute()

    return log_rows


def apply_content_formatting(sheets_service, spreadsheet_id: str, sheet_id: int) -> None:
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 18,
                    "endColumnIndex": 19,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in CONTENT_STATUS_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
    ]

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(CONTENT_HEADERS),
                        }
                    }
                }
            ]
        },
    ).execute()


def append_log_rows(log_rows: List[Dict[str, object]]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_exists = LOG_PATH.exists()
    with LOG_PATH.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["timestamp", "sku_id", "content_row_number", "status", "message"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_product_content_structure() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    content_sheet_id = ensure_content_tab(sheets_service, spreadsheet_id)
    content_rows = get_sheet_values(sheets_service, spreadsheet_id, f"{CONTENT_TAB}!A1:ZZ")
    headers, header_width, headers_written = ensure_headers(sheets_service, spreadsheet_id, content_rows)
    apply_content_formatting(sheets_service, spreadsheet_id, content_sheet_id)

    master_rows = get_master_skus(sheets_service, spreadsheet_id)
    existing_content_skus = get_existing_content_skus(sheets_service, spreadsheet_id)
    log_rows = append_missing_rows(sheets_service, spreadsheet_id, master_rows, existing_content_skus)
    append_log_rows(log_rows)

    return {
        "spreadsheet_id": spreadsheet_id,
        "content_sheet_name": CONTENT_TAB,
        "content_sheet_id": content_sheet_id,
        "headers_written": headers_written,
        "master_sku_count": len(master_rows),
        "existing_content_count": len(existing_content_skus),
        "rows_seeded_this_run": len([row for row in log_rows if row["status"] == "created"]),
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_product_content_structure()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
