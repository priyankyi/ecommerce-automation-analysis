from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    FSN_BRIDGE_PATH,
    LOG_DIR,
    TARGET_FSN_PATH,
    append_csv_log,
    build_status_payload,
    clean_fsn,
    ensure_directories,
    format_decimal,
    normalize_text,
    now_iso,
    parse_float,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_cost_master_log.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"

COST_MASTER_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Category",
    "Cost_Price",
    "Packaging_Cost",
    "Other_Cost",
    "Total_Unit_COGS",
    "COGS_Status",
    "Remarks",
    "Last_Updated",
]

COGS_STATUS_OPTIONS = ["Missing", "Entered", "Verified", "Needs Review"]
IDENTITY_FIELDS = ("SKU_ID", "Product_Title", "Category")
MANUAL_COST_FIELDS = ("Cost_Price", "Packaging_Cost", "Other_Cost", "COGS_Status", "Remarks")


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status != 503 or attempt == attempts:
                raise
            import time

            time.sleep(delay)
            delay *= 2


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def find_sheet_id(sheets_service, spreadsheet_id: str, tab_name: str) -> int | None:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return int(props["sheetId"])
    return None


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    sheet_id = find_sheet_id(sheets_service, spreadsheet_id, tab_name)
    if sheet_id is not None:
        return sheet_id
    response = retry(
        lambda: sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        )
        .execute()
    )
    return int(response["replies"][0]["addSheet"]["properties"]["sheetId"])


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    return find_sheet_id(sheets_service, spreadsheet_id, tab_name) is not None


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def clear_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .clear(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:ZZ", body={})
        .execute()
    )


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be at least 1")
    result: List[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def read_sheet_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def write_rows(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    values = [list(headers)] + [[row.get(header, "") for header in headers] for row in rows]
    end_col = column_index_to_a1(len(headers))
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:{end_col}{len(values)}",
            valueInputOption="RAW",
            body={"values": values},
        )
        .execute()
    )


def freeze_bold_resize(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int) -> None:
    requests = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": column_count,
                }
            }
        },
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def set_dropdown_validation(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    column_index: int,
    max_rows: int = 5000,
) -> None:
    requests = [
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": max_rows,
                    "startColumnIndex": column_index,
                    "endColumnIndex": column_index + 1,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in COGS_STATUS_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        }
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def build_sku_lookup(rows: Sequence[Dict[str, str]], fsn_key: str, sku_key: str) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for row in rows:
        fsn = clean_fsn(row.get(fsn_key, ""))
        sku = normalize_text(row.get(sku_key, ""))
        if fsn and sku and sku not in lookup:
            lookup[sku] = fsn
    return lookup


def build_identity_index(rows: Sequence[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    identity: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        identity[fsn] = {
            "SKU_ID": normalize_text(row.get("SKU_ID", "")),
            "Product_Title": normalize_text(row.get("Product_Title", "")),
            "Category": normalize_text(row.get("Category", "")),
        }
    return identity


def resolve_fsn(
    row: Dict[str, str],
    target_by_sku: Dict[str, str],
    bridge_by_sku: Dict[str, str],
) -> Tuple[str, str]:
    fsn = clean_fsn(row.get("FSN", ""))
    if fsn:
        return fsn, "sheet"
    sku_id = normalize_text(row.get("SKU_ID", ""))
    if sku_id and sku_id in target_by_sku:
        return target_by_sku[sku_id], "target_fsns"
    if sku_id and sku_id in bridge_by_sku:
        return bridge_by_sku[sku_id], "bridge"
    return "", ""


def choose_identity(
    fsn: str,
    analysis_row: Dict[str, str],
    existing_row: Dict[str, str],
    target_identity: Dict[str, Dict[str, str]],
    bridge_identity: Dict[str, Dict[str, str]],
) -> Dict[str, str]:
    target_row = target_identity.get(fsn, {})
    bridge_row = bridge_identity.get(fsn, {})
    return {
        "SKU_ID": normalize_text(analysis_row.get("SKU_ID", "")) or normalize_text(bridge_row.get("SKU_ID", "")) or normalize_text(target_row.get("SKU_ID", "")) or normalize_text(existing_row.get("SKU_ID", "")),
        "Product_Title": normalize_text(analysis_row.get("Product_Title", "")) or normalize_text(bridge_row.get("Product_Title", "")) or normalize_text(target_row.get("Product_Title", "")) or normalize_text(existing_row.get("Product_Title", "")),
        "Category": normalize_text(analysis_row.get("Category", "")) or normalize_text(bridge_row.get("Category", "")) or normalize_text(target_row.get("Category", "")) or normalize_text(existing_row.get("Category", "")),
    }


def coerce_status(value: Any, cost_price: str) -> str:
    text = normalize_text(value)
    for option in COGS_STATUS_OPTIONS:
        if text.lower() == option.lower():
            return option
    if not text:
        return "Missing" if not normalize_text(cost_price) else "Entered"
    return text


def total_unit_cogs(cost_price: str, packaging_cost: str, other_cost: str) -> str:
    if not any(normalize_text(value) for value in (cost_price, packaging_cost, other_cost)):
        return ""
    total = parse_float(cost_price) + parse_float(packaging_cost) + parse_float(other_cost)
    return format_decimal(total, 2)


def build_cost_row(
    fsn: str,
    analysis_row: Dict[str, str],
    existing_row: Dict[str, str],
    target_identity: Dict[str, Dict[str, str]],
    bridge_identity: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    identity = choose_identity(fsn, analysis_row, existing_row, target_identity, bridge_identity)
    cost_price = normalize_text(existing_row.get("Cost_Price", ""))
    packaging_cost = normalize_text(existing_row.get("Packaging_Cost", ""))
    other_cost = normalize_text(existing_row.get("Other_Cost", ""))
    cogs_status = coerce_status(existing_row.get("COGS_Status", ""), cost_price)
    return {
        "FSN": fsn,
        "SKU_ID": identity["SKU_ID"],
        "Product_Title": identity["Product_Title"],
        "Category": identity["Category"],
        "Cost_Price": cost_price,
        "Packaging_Cost": packaging_cost,
        "Other_Cost": other_cost,
        "Total_Unit_COGS": total_unit_cogs(cost_price, packaging_cost, other_cost),
        "COGS_Status": cogs_status,
        "Remarks": normalize_text(existing_row.get("Remarks", "")),
        "Last_Updated": now_iso(),
    }


def build_cost_master() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]

    sheets_service, _, _ = build_services()
    if not tab_exists(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {SKU_ANALYSIS_TAB}")

    _, analysis_rows = read_sheet_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    if not analysis_rows:
        raise RuntimeError(f"No rows found in Google Sheet tab: {SKU_ANALYSIS_TAB}")

    _, existing_rows = read_sheet_table(sheets_service, spreadsheet_id, COST_MASTER_TAB) if tab_exists(sheets_service, spreadsheet_id, COST_MASTER_TAB) else ([], [])
    existing_by_fsn = {clean_fsn(row.get("FSN", "")): row for row in existing_rows if clean_fsn(row.get("FSN", ""))}

    target_rows = read_csv_rows(TARGET_FSN_PATH)
    bridge_rows = read_csv_rows(FSN_BRIDGE_PATH)

    target_by_sku = build_sku_lookup(target_rows, "FSN", "SKU_ID")
    bridge_by_sku = build_sku_lookup(bridge_rows, "FSN", "Seller_SKU")
    target_identity = build_identity_index(target_rows)
    bridge_identity = build_identity_index(
        [
            {
                "FSN": row.get("FSN", ""),
                "SKU_ID": row.get("Seller_SKU", ""),
                "Product_Title": row.get("Product_Title", ""),
                "Category": row.get("Category", ""),
            }
            for row in bridge_rows
        ]
    )

    deduped_rows: List[Dict[str, str]] = []
    seen_fsns: set[str] = set()
    duplicates_skipped = 0
    inferred_fsns = 0
    blank_fsn_rows = 0
    for row in analysis_rows:
        fsn, source = resolve_fsn(row, target_by_sku, bridge_by_sku)
        if not fsn:
            blank_fsn_rows += 1
            continue
        if source != "sheet":
            inferred_fsns += 1
        if fsn in seen_fsns:
            duplicates_skipped += 1
            continue
        seen_fsns.add(fsn)
        deduped_rows.append(dict(row))

    cost_rows: List[Dict[str, Any]] = []
    preserved_manual_rows = 0
    default_missing_rows = 0
    default_entered_rows = 0
    for row in deduped_rows:
        fsn, _ = resolve_fsn(row, target_by_sku, bridge_by_sku)
        existing_row = existing_by_fsn.get(fsn, {})
        if any(normalize_text(existing_row.get(field, "")) for field in MANUAL_COST_FIELDS):
            preserved_manual_rows += 1
        cost_row = build_cost_row(fsn, row, existing_row, target_identity, bridge_identity)
        if cost_row["COGS_Status"] == "Missing":
            default_missing_rows += 1
        if cost_row["COGS_Status"] == "Entered" and not normalize_text(existing_row.get("COGS_Status", "")):
            default_entered_rows += 1
        cost_rows.append(cost_row)

    sheet_id = ensure_tab(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    clear_tab(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    write_rows(sheets_service, spreadsheet_id, COST_MASTER_TAB, COST_MASTER_HEADERS, cost_rows)
    freeze_bold_resize(sheets_service, spreadsheet_id, sheet_id, len(COST_MASTER_HEADERS))
    set_dropdown_validation(sheets_service, spreadsheet_id, sheet_id, 8, max_rows=max(len(cost_rows) + 50, 5000))

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "tab_name": COST_MASTER_TAB,
        "rows_read": len(analysis_rows),
        "rows_written": len(cost_rows),
        "duplicates_skipped": duplicates_skipped,
        "blank_fsn_rows": blank_fsn_rows,
        "inferred_fsns": inferred_fsns,
        "preserved_manual_rows": preserved_manual_rows,
        "default_missing_rows": default_missing_rows,
        "default_entered_rows": default_entered_rows,
        "status": "SUCCESS",
        "message": "FLIPKART_COST_MASTER rebuilt from FLIPKART_SKU_ANALYSIS",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "tab_name",
            "rows_read",
            "rows_written",
            "duplicates_skipped",
            "blank_fsn_rows",
            "inferred_fsns",
            "preserved_manual_rows",
            "default_missing_rows",
            "default_entered_rows",
            "status",
            "message",
        ],
        [log_row],
    )

    result = {
        "status": "SUCCESS",
        "generated_at": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "tab_name": COST_MASTER_TAB,
        "rows_read": len(analysis_rows),
        "rows_written": len(cost_rows),
        "duplicates_skipped": duplicates_skipped,
        "blank_fsn_rows": blank_fsn_rows,
        "inferred_fsns": inferred_fsns,
        "preserved_manual_rows": preserved_manual_rows,
        "default_missing_rows": default_missing_rows,
        "default_entered_rows": default_entered_rows,
        "log_path": str(LOG_PATH),
        "source_tabs": [SKU_ANALYSIS_TAB],
        "optional_inputs": [str(TARGET_FSN_PATH), str(FSN_BRIDGE_PATH)],
    }
    print(json.dumps(build_status_payload("SUCCESS", **{k: v for k, v in result.items() if k != "status"}), indent=2))
    return result


def main() -> None:
    try:
        build_cost_master()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "tab_name": COST_MASTER_TAB,
                    "analysis_tab": SKU_ANALYSIS_TAB,
                    "log_path": str(LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
