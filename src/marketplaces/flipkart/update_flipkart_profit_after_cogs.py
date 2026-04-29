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
    LOG_DIR,
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
LOG_PATH = LOG_DIR / "flipkart_profit_after_cogs_log.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"

PROFIT_COLUMNS = [
    "Cost_Price",
    "Packaging_Cost",
    "Other_Cost",
    "Total_Unit_COGS",
    "Total_COGS",
    "Final_Net_Profit",
    "Final_Profit_Per_Order",
    "Final_Profit_Margin",
    "COGS_Status",
]


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


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    sheet_id = find_sheet_id(sheets_service, spreadsheet_id, tab_name)
    if sheet_id is None:
        raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")
    return sheet_id


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


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def build_cost_index(rows: Sequence[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in indexed:
            indexed[fsn] = row
    return indexed


def format_money(value: Any) -> str:
    return format_decimal(value, 2)


def format_margin(value: Any) -> str:
    return format_decimal(value, 4)


def derive_profit_values(analysis_row: Dict[str, str], cost_row: Dict[str, str]) -> Dict[str, Any]:
    cost_price = normalize_text(cost_row.get("Cost_Price", ""))
    packaging_cost = normalize_text(cost_row.get("Packaging_Cost", ""))
    other_cost = normalize_text(cost_row.get("Other_Cost", ""))
    total_unit_cogs = normalize_text(cost_row.get("Total_Unit_COGS", ""))
    cogs_status = normalize_text(cost_row.get("COGS_Status", ""))

    if not cogs_status:
        cogs_status = "Missing" if not cost_price else "Entered"

    if not total_unit_cogs and any(normalize_text(value) for value in (cost_price, packaging_cost, other_cost)):
        total_unit_cogs = format_money(parse_float(cost_price) + parse_float(packaging_cost) + parse_float(other_cost))

    if not total_unit_cogs:
        return {
            "Cost_Price": cost_price,
            "Packaging_Cost": packaging_cost,
            "Other_Cost": other_cost,
            "Total_Unit_COGS": "",
            "Total_COGS": "",
            "Final_Net_Profit": "",
            "Final_Profit_Per_Order": "",
            "Final_Profit_Margin": "",
            "COGS_Status": cogs_status or "Missing",
        }

    units_sold = parse_float(analysis_row.get("Units_Sold", ""))
    orders = parse_float(analysis_row.get("Orders", ""))
    gross_sales = parse_float(analysis_row.get("Gross_Sales", ""))
    net_profit_before_cogs = parse_float(analysis_row.get("Net_Profit_Before_COGS", ""))
    total_cogs = units_sold * parse_float(total_unit_cogs)
    final_net_profit = net_profit_before_cogs - total_cogs
    final_profit_per_order = final_net_profit / orders if orders else ""
    final_profit_margin = final_net_profit / gross_sales if gross_sales else ""

    return {
        "Cost_Price": cost_price,
        "Packaging_Cost": packaging_cost,
        "Other_Cost": other_cost,
        "Total_Unit_COGS": format_money(total_unit_cogs),
        "Total_COGS": format_money(total_cogs),
        "Final_Net_Profit": format_money(final_net_profit),
        "Final_Profit_Per_Order": format_money(final_profit_per_order) if final_profit_per_order != "" else "",
        "Final_Profit_Margin": format_margin(final_profit_margin) if final_profit_margin != "" else "",
        "COGS_Status": cogs_status or "Missing",
    }


def merge_headers(headers: Sequence[str]) -> List[str]:
    merged = list(headers)
    for column in PROFIT_COLUMNS:
        if column not in merged:
            merged.append(column)
    return merged


def update_flipkart_profit_after_cogs() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]

    sheets_service, _, _ = build_services()
    ensure_required_tab_exists(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    ensure_required_tab_exists(sheets_service, spreadsheet_id, COST_MASTER_TAB)

    analysis_headers, analysis_rows = read_sheet_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    if not analysis_rows:
        raise RuntimeError(f"No rows found in Google Sheet tab: {SKU_ANALYSIS_TAB}")
    _, cost_rows = read_sheet_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    cost_index = build_cost_index(cost_rows)

    output_headers = merge_headers(analysis_headers)
    output_rows: List[Dict[str, Any]] = []
    missing_cogs_rows = 0
    missing_cost_rows = 0
    duplicate_fsns_skipped = 0
    seen_fsns: set[str] = set()

    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        if fsn in seen_fsns:
            duplicate_fsns_skipped += 1
            continue
        seen_fsns.add(fsn)
        cost_row = cost_index.get(fsn, {})
        if not cost_row or not normalize_text(cost_row.get("Total_Unit_COGS", "")) and not any(
            normalize_text(cost_row.get(field, "")) for field in ("Cost_Price", "Packaging_Cost", "Other_Cost")
        ):
            missing_cost_rows += 1
        derived = derive_profit_values(row, cost_row)
        if not normalize_text(derived.get("Total_Unit_COGS", "")):
            missing_cogs_rows += 1
        merged_row = dict(row)
        merged_row.update(derived)
        output_rows.append(merged_row)

    sheet_id = ensure_required_tab_exists(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    clear_tab(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    write_rows(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB, output_headers, output_rows)
    freeze_bold_resize(sheets_service, spreadsheet_id, sheet_id, len(output_headers))

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "tab_name": SKU_ANALYSIS_TAB,
        "rows_read": len(analysis_rows),
        "rows_written": len(output_rows),
        "missing_cost_rows": missing_cost_rows,
        "missing_cogs_rows": missing_cogs_rows,
        "duplicate_fsns_skipped": duplicate_fsns_skipped,
        "status": "SUCCESS",
        "message": "FLIPKART_SKU_ANALYSIS updated with COGS-backed profit columns",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "tab_name",
            "rows_read",
            "rows_written",
            "missing_cost_rows",
            "missing_cogs_rows",
            "duplicate_fsns_skipped",
            "status",
            "message",
        ],
        [log_row],
    )

    result = {
        "status": "SUCCESS",
        "generated_at": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "tab_name": SKU_ANALYSIS_TAB,
        "cost_master_tab": COST_MASTER_TAB,
        "rows_read": len(analysis_rows),
        "rows_written": len(output_rows),
        "missing_cost_rows": missing_cost_rows,
        "missing_cogs_rows": missing_cogs_rows,
        "duplicate_fsns_skipped": duplicate_fsns_skipped,
        "log_path": str(LOG_PATH),
        "profit_columns": PROFIT_COLUMNS,
    }
    print(json.dumps(build_status_payload("SUCCESS", **{k: v for k, v in result.items() if k != "status"}), indent=2))
    return result


def main() -> None:
    try:
        update_flipkart_profit_after_cogs()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "tab_name": SKU_ANALYSIS_TAB,
                    "cost_master_tab": COST_MASTER_TAB,
                    "log_path": str(LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
