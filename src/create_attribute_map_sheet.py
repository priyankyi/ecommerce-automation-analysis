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
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "attribute_map_setup_log.csv"

ATTRIBUTE_TAB = "ATTRIBUTE_MAP"

ATTRIBUTE_HEADERS = [
    "Master_Field",
    "Data_Type",
    "Required",
    "Flipkart_Field",
    "Meesho_Field",
    "Snapdeal_Field",
    "Myntra_Field",
    "POP_Field",
    "FirstCry_Field",
    "MyStore_Field",
    "Shopify_Field",
    "Notes",
]

SEED_FIELDS = [
    "SKU_ID",
    "Product_Title",
    "Category",
    "Sub_Category",
    "Brand",
    "Model_Number",
    "Cost_Price",
    "MRP",
    "Selling_Price",
    "GST_Rate",
    "HSN_Code",
    "Length_cm",
    "Width_cm",
    "Height_cm",
    "Dead_Weight_kg",
    "Volumetric_Weight_kg",
    "Final_Chargeable_Weight",
    "Supplier_Name",
    "Available_Stock",
    "Product_Status",
]

DATA_TYPE_OPTIONS = [
    "Text",
    "Number",
    "Date",
    "Dropdown",
    "URL",
    "Boolean",
    "Formula",
]

REQUIRED_OPTIONS = [
    "Yes",
    "No",
    "Conditional",
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


def ensure_attribute_tab(sheets_service, spreadsheet_id: str) -> int:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    existing = find_tab(metadata, ATTRIBUTE_TAB)
    if existing and existing.get("sheetId") is not None:
        return existing["sheetId"]

    response = (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": ATTRIBUTE_TAB}}}]},
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


def ensure_headers(sheets_service, spreadsheet_id: str, sheet_rows: List[List[str]]) -> bool:
    current_headers = sheet_rows[0] if sheet_rows else []
    if current_headers[: len(ATTRIBUTE_HEADERS)] != ATTRIBUTE_HEADERS:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{ATTRIBUTE_TAB}!A1:{zero_based_column_index_to_a1(len(ATTRIBUTE_HEADERS) - 1)}1",
            valueInputOption="RAW",
            body={"values": [ATTRIBUTE_HEADERS]},
        ).execute()
        return True
    return False


def get_existing_master_fields(sheets_service, spreadsheet_id: str) -> Dict[str, int]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{ATTRIBUTE_TAB}!A1:L")
    if len(rows) < 2:
        return {}

    headers = rows[0]
    master_index = find_header_index(headers, "Master_Field")
    if master_index is None:
        return {}

    existing: Dict[str, int] = {}
    for row_number, row in enumerate(rows[1:], start=2):
        master_field = row[master_index].strip() if master_index < len(row) else ""
        if master_field and master_field not in existing:
            existing[master_field] = row_number
    return existing


def append_missing_seed_rows(
    sheets_service,
    spreadsheet_id: str,
    existing_master_fields: Dict[str, int],
) -> List[Dict[str, object]]:
    rows_to_append: List[List[str]] = []
    log_rows: List[Dict[str, object]] = []
    timestamp = datetime.now().isoformat(timespec="seconds")

    for field in SEED_FIELDS:
        if field in existing_master_fields:
            log_rows.append(
                {
                    "timestamp": timestamp,
                    "master_field": field,
                    "row_number": existing_master_fields[field],
                    "status": "skipped",
                    "message": "Master field already exists",
                }
            )
            continue

        row = [""] * len(ATTRIBUTE_HEADERS)
        row[0] = field
        rows_to_append.append(row)
        log_rows.append(
            {
                "timestamp": timestamp,
                "master_field": field,
                "row_number": "",
                "status": "created",
                "message": "Seeded master field row",
            }
        )

    if rows_to_append:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{ATTRIBUTE_TAB}!A:L",
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
                    "startColumnIndex": 1,
                    "endColumnIndex": 2,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in DATA_TYPE_OPTIONS],
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
                    "startColumnIndex": 2,
                    "endColumnIndex": 3,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in REQUIRED_OPTIONS],
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
                            "endIndex": len(ATTRIBUTE_HEADERS),
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
            fieldnames=["timestamp", "master_field", "row_number", "status", "message"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_attribute_map_structure() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    sheet_id = ensure_attribute_tab(sheets_service, spreadsheet_id)
    sheet_rows = get_sheet_values(sheets_service, spreadsheet_id, f"{ATTRIBUTE_TAB}!A1:L")
    headers_written = ensure_headers(sheets_service, spreadsheet_id, sheet_rows)

    existing_master_fields = get_existing_master_fields(sheets_service, spreadsheet_id)
    log_rows = append_missing_seed_rows(sheets_service, spreadsheet_id, existing_master_fields)
    append_log_rows(log_rows)
    apply_formatting(sheets_service, spreadsheet_id, sheet_id)

    return {
        "spreadsheet_id": spreadsheet_id,
        "attribute_sheet_name": ATTRIBUTE_TAB,
        "attribute_sheet_id": sheet_id,
        "headers_written": headers_written,
        "seed_rows_total": len(SEED_FIELDS),
        "existing_seed_rows": len(existing_master_fields),
        "rows_seeded_this_run": len([row for row in log_rows if row["status"] == "created"]),
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_attribute_map_structure()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
