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
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "review_rating_tracker_setup_log.csv"

MASTER_SKU_TAB = "MASTER_SKU"
TAB_NAME = "REVIEW_RATING"

MARKETPLACES = [
    "Flipkart",
    "Meesho",
    "Snapdeal",
    "FirstCry",
    "MyStore",
    "Shopify",
]

HEADERS = [
    "SKU_ID",
    "Marketplace",
    "Product_Title",
    "Total_Reviews",
    "Average_Rating",
    "Positive_Reviews",
    "Neutral_Reviews",
    "Negative_Reviews",
    "Latest_Review_Date",
    "Review_Status",
    "Response_Status",
    "Sentiment",
    "Assigned_To",
    "Priority",
    "Action_Required",
    "Last_Checked_Date",
    "Last_Updated",
    "Remarks",
]

REVIEW_STATUS_OPTIONS = [
    "No Reviews",
    "New Review",
    "Needs Response",
    "Responded",
    "Resolved",
]

RESPONSE_STATUS_OPTIONS = [
    "Pending",
    "In Progress",
    "Sent",
    "Closed",
]

SENTIMENT_OPTIONS = [
    "Positive",
    "Neutral",
    "Negative",
    "Mixed",
]

PRIORITY_OPTIONS = [
    "Low",
    "Medium",
    "High",
    "Critical",
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


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, object]:
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


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    existing = find_tab(metadata, tab_name)
    if existing and existing.get("sheetId") is not None:
        return existing["sheetId"]

    response = (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
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


def ensure_headers(sheets_service, spreadsheet_id: str, header_row: List[str]) -> bool:
    if header_row[: len(HEADERS)] != HEADERS:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{TAB_NAME}!A1:{zero_based_column_index_to_a1(len(HEADERS) - 1)}1",
            valueInputOption="RAW",
            body={"values": [HEADERS]},
        ).execute()
        return True
    return False


def get_master_rows(sheets_service, spreadsheet_id: str) -> List[Tuple[str, str]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{MASTER_SKU_TAB}!A1:ZZ")
    if len(rows) < 2:
        return []

    headers = rows[0]
    sku_index = find_header_index(headers, "SKU_ID")
    title_index = find_header_index(headers, "Product_Title")
    if sku_index is None:
        raise ValueError("SKU_ID header was not found in MASTER_SKU.")

    result: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for row in rows[1:]:
        sku_id = row[sku_index].strip() if sku_index < len(row) else ""
        if not sku_id or sku_id in seen:
            continue
        seen.add(sku_id)
        product_title = row[title_index].strip() if title_index is not None and title_index < len(row) else ""
        result.append((sku_id, product_title))
    return result


def get_existing_tracker_keys(sheets_service, spreadsheet_id: str) -> Dict[Tuple[str, str], int]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{TAB_NAME}!A1:R")
    if len(rows) < 2:
        return {}

    headers = rows[0]
    sku_index = find_header_index(headers, "SKU_ID")
    marketplace_index = find_header_index(headers, "Marketplace")
    if sku_index is None or marketplace_index is None:
        return {}

    existing: Dict[Tuple[str, str], int] = {}
    for row_number, row in enumerate(rows[1:], start=2):
        sku_id = row[sku_index].strip() if sku_index < len(row) else ""
        marketplace = row[marketplace_index].strip() if marketplace_index < len(row) else ""
        if sku_id and marketplace and (sku_id, marketplace) not in existing:
            existing[(sku_id, marketplace)] = row_number
    return existing


def map_row(sku_id: str, marketplace: str, product_title: str) -> List[str]:
    row = [""] * len(HEADERS)
    row[0] = sku_id
    row[1] = marketplace
    row[2] = product_title
    row[9] = "No Reviews"
    row[10] = "Pending"
    row[11] = "Neutral"
    row[13] = "Medium"
    row[16] = datetime.now().isoformat(timespec="seconds")
    return row


def append_missing_rows(
    sheets_service,
    spreadsheet_id: str,
    master_rows: List[Tuple[str, str]],
    existing_tracker_keys: Dict[Tuple[str, str], int],
) -> List[Dict[str, object]]:
    rows_to_append: List[List[str]] = []
    log_rows: List[Dict[str, object]] = []
    timestamp = datetime.now().isoformat(timespec="seconds")

    for sku_id, product_title in master_rows:
        for marketplace in MARKETPLACES:
            key = (sku_id, marketplace)
            if key in existing_tracker_keys:
                log_rows.append(
                    {
                        "timestamp": timestamp,
                        "sku_id": sku_id,
                        "marketplace": marketplace,
                        "row_number": existing_tracker_keys[key],
                        "status": "skipped",
                        "message": "Review row already exists",
                    }
                )
                continue

            rows_to_append.append(map_row(sku_id, marketplace, product_title))
            log_rows.append(
                {
                    "timestamp": timestamp,
                    "sku_id": sku_id,
                    "marketplace": marketplace,
                    "row_number": "",
                    "status": "created",
                    "message": "Seeded review tracker row",
                }
            )

    if rows_to_append:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{TAB_NAME}!A:{column_index_to_a1(len(HEADERS))}",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_to_append},
        ).execute()

    return log_rows


def apply_formatting(sheets_service, spreadsheet_id: str, sheet_id: int) -> None:
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
                    "startColumnIndex": 9,
                    "endColumnIndex": 10,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in REVIEW_STATUS_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 10,
                    "endColumnIndex": 11,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in RESPONSE_STATUS_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 11,
                    "endColumnIndex": 12,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in SENTIMENT_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 13,
                    "endColumnIndex": 14,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in PRIORITY_OPTIONS],
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
                            "endIndex": len(HEADERS),
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
            fieldnames=["timestamp", "sku_id", "marketplace", "row_number", "status", "message"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_review_rating_tracker() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    sheet_id = ensure_tab(sheets_service, spreadsheet_id, TAB_NAME)
    existing_headers = get_sheet_values(sheets_service, spreadsheet_id, f"{TAB_NAME}!A1:R1")
    headers_written = ensure_headers(
        sheets_service,
        spreadsheet_id,
        existing_headers[0] if existing_headers else [],
    )

    master_rows = get_master_rows(sheets_service, spreadsheet_id)
    existing_tracker_keys = get_existing_tracker_keys(sheets_service, spreadsheet_id)
    log_rows = append_missing_rows(
        sheets_service,
        spreadsheet_id,
        master_rows,
        existing_tracker_keys,
    )
    append_log_rows(log_rows)
    apply_formatting(sheets_service, spreadsheet_id, sheet_id)

    return {
        "spreadsheet_id": spreadsheet_id,
        "tracker_sheet_name": TAB_NAME,
        "tracker_sheet_id": sheet_id,
        "headers_written": headers_written,
        "master_sku_count": len(master_rows),
        "marketplace_count": len(MARKETPLACES),
        "rows_seeded_this_run": len([row for row in log_rows if row["status"] == "created"]),
        "expected_rows": len(master_rows) * len(MARKETPLACES),
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_review_rating_tracker()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
