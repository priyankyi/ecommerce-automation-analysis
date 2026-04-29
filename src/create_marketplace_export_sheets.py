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
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "marketplace_export_setup_log.csv"

MASTER_SKU_TAB = "MASTER_SKU"
CONTENT_TAB = "PRODUCT_CONTENT"
ATTRIBUTE_TAB = "ATTRIBUTE_MAP"

EXPORT_TABS = [
    "FLIPKART_EXPORT",
    "MEESHO_EXPORT",
    "SNAPDEAL_EXPORT",
    "FIRSTCRY_EXPORT",
    "MYSTORE_EXPORT",
    "SHOPIFY_EXPORT",
]

EXPORT_HEADERS = [
    "SKU_ID",
    "Product_Title",
    "Short_Title",
    "Description",
    "MRP",
    "Selling_Price",
    "GST_Rate",
    "HSN_Code",
    "Category",
    "Sub_Category",
    "Brand",
    "Model_Number",
    "Length_cm",
    "Width_cm",
    "Height_cm",
    "Dead_Weight_kg",
    "Image_Folder_Link",
    "Export_Status",
    "QC_Remarks",
    "Last_Updated",
]

EXPORT_STATUS_OPTIONS = [
    "Draft",
    "Ready",
    "Exported",
    "Rejected",
    "Needs Correction",
]

MASTER_FIELDS = [
    "SKU_ID",
    "Product_Title",
    "Category",
    "Sub_Category",
    "Brand",
    "Model_Number",
    "MRP",
    "Selling_Price",
    "GST_Rate",
    "HSN_Code",
    "Length_cm",
    "Width_cm",
    "Height_cm",
    "Dead_Weight_kg",
    "Image_Folder_Link",
    "Last_Updated",
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


def ensure_headers(sheets_service, spreadsheet_id: str, tab_name: str, header_row: List[str]) -> bool:
    if header_row[: len(EXPORT_HEADERS)] != EXPORT_HEADERS:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:{zero_based_column_index_to_a1(len(EXPORT_HEADERS) - 1)}1",
            valueInputOption="RAW",
            body={"values": [EXPORT_HEADERS]},
        ).execute()
        return True
    return False


def build_lookup(rows: List[List[str]], key_header: str) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    if not rows:
        return [], {}

    headers = rows[0]
    key_index = find_header_index(headers, key_header)
    if key_index is None:
        return headers, {}

    lookup: Dict[str, Dict[str, str]] = {}
    for row in rows[1:]:
        key = row[key_index].strip() if key_index < len(row) else ""
        if not key or key in lookup:
            continue
        lookup[key] = {
            headers[i]: row[i] if i < len(row) else ""
            for i in range(len(headers))
        }
    return headers, lookup


def get_source_maps(sheets_service, spreadsheet_id: str) -> Dict[str, Dict[str, str]]:
    master_rows = get_sheet_values(sheets_service, spreadsheet_id, f"{MASTER_SKU_TAB}!A1:ZZ")
    content_rows = get_sheet_values(sheets_service, spreadsheet_id, f"{CONTENT_TAB}!A1:ZZ")
    attribute_rows = get_sheet_values(sheets_service, spreadsheet_id, f"{ATTRIBUTE_TAB}!A1:L")

    _, master_lookup = build_lookup(master_rows, "SKU_ID")
    _, content_lookup = build_lookup(content_rows, "SKU_ID")
    _, attribute_lookup = build_lookup(attribute_rows, "Master_Field")

    return {
        "master": master_lookup,
        "content": content_lookup,
        "attribute": attribute_lookup,
    }


def get_master_skus(sheets_service, spreadsheet_id: str) -> List[str]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{MASTER_SKU_TAB}!A1:ZZ")
    if len(rows) < 2:
        return []

    headers = rows[0]
    sku_index = find_header_index(headers, "SKU_ID")
    if sku_index is None:
        raise ValueError("SKU_ID header was not found in MASTER_SKU.")

    seen: set[str] = set()
    skus: List[str] = []
    for row in rows[1:]:
        sku_id = row[sku_index].strip() if sku_index < len(row) else ""
        if not sku_id or sku_id in seen:
            continue
        seen.add(sku_id)
        skus.append(sku_id)
    return skus


def get_existing_export_skus(sheets_service, spreadsheet_id: str, tab_name: str) -> Dict[str, int]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
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


def map_export_row(
    sku_id: str,
    source_maps: Dict[str, Dict[str, Dict[str, str]]],
) -> List[str]:
    master = source_maps["master"].get(sku_id, {})
    content = source_maps["content"].get(sku_id, {})
    attribute_map = source_maps["attribute"]

    export_row = [""] * len(EXPORT_HEADERS)
    for index, header in enumerate(EXPORT_HEADERS):
        if header == "Export_Status":
            export_row[index] = "Draft"
        elif header == "SKU_ID":
            export_row[index] = sku_id
        elif header in content and content.get(header):
            export_row[index] = content.get(header, "")
        elif header in master and master.get(header):
            export_row[index] = master.get(header, "")
        elif header == "Product_Title" and content.get("Product_Title"):
            export_row[index] = content.get("Product_Title", "")
        elif header == "Description" and content.get("Description"):
            export_row[index] = content.get("Description", "")
        elif header == "Image_Folder_Link" and master.get("Image_Folder_Link"):
            export_row[index] = master.get("Image_Folder_Link", "")
        elif header == "Last_Updated" and content.get("Last_Updated"):
            export_row[index] = content.get("Last_Updated", "")
        elif header == "Last_Updated" and master.get("Last_Updated"):
            export_row[index] = master.get("Last_Updated", "")
        else:
            export_row[index] = ""

    if "SKU_ID" not in attribute_map:
        export_row[1] = export_row[1] or master.get("Product_Title", "")

    return export_row


def append_missing_rows(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    master_skus: List[str],
    source_maps: Dict[str, Dict[str, Dict[str, str]]],
    existing_export_skus: Dict[str, int],
) -> List[Dict[str, object]]:
    rows_to_append: List[List[str]] = []
    log_rows: List[Dict[str, object]] = []
    timestamp = datetime.now().isoformat(timespec="seconds")

    for sku_id in master_skus:
        if sku_id in existing_export_skus:
            log_rows.append(
                {
                    "timestamp": timestamp,
                    "tab_name": tab_name,
                    "sku_id": sku_id,
                    "row_number": existing_export_skus[sku_id],
                    "status": "skipped",
                    "message": "SKU already exists in export tab",
                }
            )
            continue

        rows_to_append.append(map_export_row(sku_id, source_maps))
        log_rows.append(
            {
                "timestamp": timestamp,
                "tab_name": tab_name,
                "sku_id": sku_id,
                "row_number": "",
                "status": "created",
                "message": "Seeded export row",
            }
        )

    if rows_to_append:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A:T",
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
                    "startColumnIndex": 17,
                    "endColumnIndex": 18,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in EXPORT_STATUS_OPTIONS],
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
                            "endIndex": len(EXPORT_HEADERS),
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
            fieldnames=["timestamp", "tab_name", "sku_id", "row_number", "status", "message"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_marketplace_export_sheets() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    source_maps = get_source_maps(sheets_service, spreadsheet_id)
    master_skus = get_master_skus(sheets_service, spreadsheet_id)
    result_tabs: Dict[str, Dict[str, object]] = {}
    all_log_rows: List[Dict[str, object]] = []

    for tab_name in EXPORT_TABS:
        sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
        headers_written = ensure_headers(
            sheets_service,
            spreadsheet_id,
            tab_name,
            get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:Z1")[0]
            if get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:Z1")
            else [],
        )
        existing_export_skus = get_existing_export_skus(sheets_service, spreadsheet_id, tab_name)
        log_rows = append_missing_rows(
            sheets_service,
            spreadsheet_id,
            tab_name,
            master_skus,
            source_maps,
            existing_export_skus,
        )
        all_log_rows.extend(log_rows)
        apply_formatting(sheets_service, spreadsheet_id, sheet_id)
        result_tabs[tab_name] = {
            "sheet_id": sheet_id,
            "headers_written": headers_written,
            "existing_rows": len(existing_export_skus),
            "rows_seeded_this_run": len([row for row in log_rows if row["status"] == "created"]),
        }

    append_log_rows(all_log_rows)

    return {
        "spreadsheet_id": spreadsheet_id,
        "export_tabs": result_tabs,
        "master_sku_count": len(master_skus),
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_marketplace_export_sheets()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
