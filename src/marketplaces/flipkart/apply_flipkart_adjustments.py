from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import (
    add_basic_filter,
    clear_tab,
    ensure_tab,
    freeze_and_format,
    load_json,
    read_table,
    set_list_validation,
    tab_exists,
    write_rows,
)
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    normalize_text,
    now_iso,
    parse_float,
    format_decimal,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_apply_adjustments_log.csv"
SKU_ANALYSIS_SNAPSHOT_PATH = OUTPUT_DIR / "flipkart_adjusted_profit.csv"
LOOKER_SNAPSHOT_PATH = OUTPUT_DIR / "looker_flipkart_adjusted_profit.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"
RUN_HISTORY_TAB = "FLIPKART_RUN_HISTORY"
ADJUSTMENT_LEDGER_TAB = "FLIPKART_ADJUSTMENTS_LEDGER"
ADJUSTED_PROFIT_TAB = "FLIPKART_ADJUSTED_PROFIT"
LOOKER_ADJUSTED_PROFIT_TAB = "LOOKER_FLIPKART_ADJUSTED_PROFIT"

ADJUSTED_PROFIT_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Original_Final_Net_Profit",
    "Total_Adjustment_Additions",
    "Total_Adjustment_Deductions",
    "Net_Adjustment",
    "Adjusted_Final_Net_Profit",
    "Adjustment_Count",
    "Open_Adjustment_Count",
    "Verified_Adjustment_Count",
    "Applied_Adjustment_Count",
    "Adjustment_Status",
    "Last_Updated",
]

SKU_ANALYSIS_APPEND_HEADERS = [
    "Total_Adjustment_Additions",
    "Total_Adjustment_Deductions",
    "Net_Adjustment",
    "Adjusted_Final_Net_Profit",
    "Adjustment_Count",
    "Adjustment_Status",
]

ADJUSTMENT_STATUS_PRIORITY = {
    "Open": 0,
    "Needs Review": 1,
    "Verified": 2,
    "Applied": 3,
    "Ignored": 4,
    "No Adjustments": 5,
}

ORIGINAL_PROFIT_COLUMNS = [
    "Final_Net_Profit",
    "Net_Profit_Before_COGS",
    "Final Net Profit",
    "Net Profit Before COGS",
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


def parse_run_dt(run_id: str) -> datetime:
    try:
        return datetime.strptime(run_id, "FLIPKART_%Y%m%d_%H%M%S")
    except ValueError:
        return datetime.min


def build_latest_run_row(rows: Sequence[Dict[str, str]]) -> Dict[str, str]:
    if not rows:
        return {}
    return max(
        rows,
        key=lambda row: (
            parse_run_dt(normalize_text(row.get("Run_ID", ""))),
            normalize_text(row.get("Last_Updated", "")),
        ),
    )


def build_latest_run_by_fsn(rows: Sequence[Dict[str, str]]) -> Dict[str, str]:
    latest: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        run_id = normalize_text(row.get("Run_ID", ""))
        if not fsn or not run_id:
            continue
        current = latest.get(fsn)
        candidate_key = (
            parse_run_dt(run_id),
            normalize_text(row.get("Last_Updated", "")),
        )
        if current is None:
            latest[fsn] = dict(row)
            continue
        current_key = (
            parse_run_dt(normalize_text(current.get("Run_ID", ""))),
            normalize_text(current.get("Last_Updated", "")),
        )
        if candidate_key > current_key:
            latest[fsn] = dict(row)
    return {fsn: normalize_text(row.get("Run_ID", "")) for fsn, row in latest.items()}


def build_key(row: Dict[str, Any]) -> Tuple[str, str]:
    return normalize_text(row.get("Run_ID", "")), clean_fsn(row.get("FSN", ""))


def build_index(rows: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    indexed: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows:
        run_id, fsn = build_key(row)
        if not run_id or not fsn:
            continue
        current = indexed.get((run_id, fsn))
        if current is None or normalize_text(row.get("Last_Updated", "")) >= normalize_text(current.get("Last_Updated", "")):
            indexed[(run_id, fsn)] = dict(row)
    return indexed


def normalize_status(value: Any) -> str:
    text = normalize_text(value)
    for option in ["Open", "Verified", "Applied", "Ignored", "Needs Review"]:
        if text.lower() == option.lower():
            return option
    return text or "Needs Review"


def normalize_direction(value: Any) -> str:
    text = normalize_text(value)
    for option in ["Deduction", "Addition"]:
        if text.lower() == option.lower():
            return option
    return ""


def valid_adjustment_amount(value: Any) -> float:
    amount = abs(parse_float(value))
    return amount if amount > 0 else 0.0


def target_run_for_row(
    row: Dict[str, Any],
    latest_run_by_fsn: Dict[str, str],
    latest_run_id_global: str,
) -> str:
    related = normalize_text(row.get("Related_Run_ID", ""))
    if related:
        return related
    fsn = clean_fsn(row.get("FSN", ""))
    if fsn and fsn in latest_run_by_fsn:
        return latest_run_by_fsn[fsn]
    return latest_run_id_global


def merge_headers(headers: Sequence[str], extra_headers: Sequence[str]) -> List[str]:
    merged = list(headers)
    for header in extra_headers:
        if header not in merged:
            merged.append(header)
    return merged


def base_output_row(
    base_row: Dict[str, Any],
    analysis_lookup: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    fsn = clean_fsn(base_row.get("FSN", ""))
    analysis_row = analysis_lookup.get(fsn, {})
    return {
        "Run_ID": normalize_text(base_row.get("Run_ID", "")),
        "FSN": fsn,
        "SKU_ID": normalize_text(base_row.get("SKU_ID", "")) or normalize_text(analysis_row.get("SKU_ID", "")),
        "Product_Title": normalize_text(base_row.get("Product_Title", "")) or normalize_text(analysis_row.get("Product_Title", "")),
        "Original_Final_Net_Profit": normalize_text(base_row.get("Final_Net_Profit", "")) or normalize_text(analysis_row.get("Final_Net_Profit", "")),
    }


def determine_status(open_count: int, verified_count: int, applied_count: int, active_count: int) -> str:
    if active_count == 0:
        return "No Adjustments"
    if open_count > 0:
        return "Open"
    if verified_count > 0:
        return "Verified"
    if applied_count > 0:
        return "Applied"
    return "Needs Review"


def read_adjustments(rows: Sequence[Dict[str, str]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        amount = valid_adjustment_amount(row.get("Adjustment_Amount", ""))
        direction = normalize_direction(row.get("Adjustment_Direction", ""))
        if not fsn or amount <= 0 or not direction:
            continue
        output.append(
            {
                "Adjustment_ID": normalize_text(row.get("Adjustment_ID", "")),
                "Adjustment_Date": normalize_text(row.get("Adjustment_Date", "")),
                "Related_Run_ID": normalize_text(row.get("Related_Run_ID", "")),
                "FSN": fsn,
                "SKU_ID": normalize_text(row.get("SKU_ID", "")),
                "Product_Title": normalize_text(row.get("Product_Title", "")),
                "Order_ID": normalize_text(row.get("Order_ID", "")),
                "Order_Item_ID": normalize_text(row.get("Order_Item_ID", "")),
                "Adjustment_Type": normalize_text(row.get("Adjustment_Type", "")),
                "Adjustment_Amount": amount,
                "Adjustment_Direction": direction,
                "Reason": normalize_text(row.get("Reason", "")),
                "Source_Report": normalize_text(row.get("Source_Report", "")),
                "Evidence_Link": normalize_text(row.get("Evidence_Link", "")),
                "Status": normalize_status(row.get("Status", "")),
                "Remarks": normalize_text(row.get("Remarks", "")),
                "Created_By": normalize_text(row.get("Created_By", "")),
                "Last_Updated": normalize_text(row.get("Last_Updated", "")),
            }
        )
    return output


def aggregate_adjustments(
    adjustment_rows: Sequence[Dict[str, Any]],
    latest_run_by_fsn: Dict[str, str],
    latest_run_id_global: str,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    aggregated: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(
        lambda: {
            "additions": 0.0,
            "deductions": 0.0,
            "count": 0,
            "open": 0,
            "verified": 0,
            "applied": 0,
            "ignored": 0,
            "needs_review": 0,
        }
    )
    for row in adjustment_rows:
        status = normalize_status(row.get("Status", ""))
        if status == "Ignored":
            target_run = target_run_for_row(row, latest_run_by_fsn, latest_run_id_global)
            key = (target_run, clean_fsn(row.get("FSN", "")))
            bucket = aggregated[key]
            bucket["ignored"] += 1
            continue
        target_run = target_run_for_row(row, latest_run_by_fsn, latest_run_id_global)
        fsn = clean_fsn(row.get("FSN", ""))
        if not target_run or not fsn:
            continue
        key = (target_run, fsn)
        bucket = aggregated[key]
        amount = float(row["Adjustment_Amount"])
        bucket["count"] += 1
        bucket["additions"] += amount if row["Adjustment_Direction"] == "Addition" else 0.0
        bucket["deductions"] += amount if row["Adjustment_Direction"] == "Deduction" else 0.0
        if status == "Open":
            bucket["open"] += 1
        elif status == "Verified":
            bucket["verified"] += 1
        elif status == "Applied":
            bucket["applied"] += 1
        else:
            bucket["needs_review"] += 1
    return aggregated


def format_money(value: Any) -> str:
    return format_decimal(value, 2)


def first_non_blank(row: Dict[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = normalize_text(row.get(key, ""))
        if value:
            return value
    return ""


def source_profit_column(row: Dict[str, Any]) -> str:
    for key in ORIGINAL_PROFIT_COLUMNS:
        if normalize_text(row.get(key, "")):
            return key
    return ""


def source_profit_column_from_headers(headers: Sequence[str]) -> str:
    for key in ORIGINAL_PROFIT_COLUMNS:
        if key in headers:
            return key
    return ""


def original_profit_value(row: Dict[str, Any]) -> float:
    return parse_float(first_non_blank(row, ORIGINAL_PROFIT_COLUMNS))


def build_outputs() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    required_tabs = [SKU_ANALYSIS_TAB, FSN_HISTORY_TAB, RUN_HISTORY_TAB, ADJUSTMENT_LEDGER_TAB]
    for tab_name in required_tabs:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    ledger_headers, ledger_rows = read_table(sheets_service, spreadsheet_id, ADJUSTMENT_LEDGER_TAB)
    analysis_headers, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    _, fsn_history_rows = read_table(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
    _, run_history_rows = read_table(sheets_service, spreadsheet_id, RUN_HISTORY_TAB)

    latest_run_row = build_latest_run_row(run_history_rows)
    latest_run_id_global = normalize_text(latest_run_row.get("Run_ID", ""))
    latest_run_by_fsn = build_latest_run_by_fsn(fsn_history_rows)

    valid_adjustments = read_adjustments(ledger_rows)
    aggregated = aggregate_adjustments(valid_adjustments, latest_run_by_fsn, latest_run_id_global)

    analysis_lookup = {clean_fsn(row.get("FSN", "")): dict(row) for row in analysis_rows if clean_fsn(row.get("FSN", ""))}
    adjusted_rows: List[Dict[str, Any]] = []
    unique_base_rows: List[Dict[str, Any]] = []
    seen_keys: set[Tuple[str, str]] = set()
    total_additions_all = 0.0
    total_deductions_all = 0.0
    net_adjustment_all = 0.0
    fsns_with_adjustments: set[str] = set()

    for row in fsn_history_rows:
        run_id, fsn = build_key(row)
        if not run_id or not fsn:
            continue
        key = (run_id, fsn)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_base_rows.append(dict(row))

    if not unique_base_rows and analysis_rows:
        for row in analysis_rows:
            fsn = clean_fsn(row.get("FSN", ""))
            if not fsn:
                continue
            unique_base_rows.append(
                {
                    "Run_ID": latest_run_id_global,
                    "FSN": fsn,
                    "SKU_ID": normalize_text(row.get("SKU_ID", "")),
                    "Product_Title": normalize_text(row.get("Product_Title", "")),
                    "Final_Net_Profit": first_non_blank(row, ORIGINAL_PROFIT_COLUMNS),
                }
            )

    for base_row in unique_base_rows:
        run_id, fsn = build_key(base_row)
        key = (run_id, fsn)
        summary = aggregated.get(key, {})
        additions = summary.get("additions", 0.0)
        deductions = summary.get("deductions", 0.0)
        net_adjustment = additions - deductions
        total_additions_all += additions
        total_deductions_all += deductions
        net_adjustment_all += net_adjustment
        if int(summary.get("count", 0)) > 0 or abs(net_adjustment) > 1e-9:
            fsns_with_adjustments.add(fsn)
        original_profit = parse_float(first_non_blank(base_row, ORIGINAL_PROFIT_COLUMNS))
        adjusted_profit = original_profit + net_adjustment
        active_count = int(summary.get("count", 0))
        open_count = int(summary.get("open", 0))
        verified_count = int(summary.get("verified", 0))
        applied_count = int(summary.get("applied", 0))
        status = determine_status(open_count, verified_count, applied_count, active_count)
        adjusted_rows.append(
            {
                "Run_ID": run_id,
                "FSN": fsn,
                "SKU_ID": normalize_text(base_row.get("SKU_ID", "")) or normalize_text(analysis_lookup.get(fsn, {}).get("SKU_ID", "")),
                "Product_Title": normalize_text(base_row.get("Product_Title", "")) or normalize_text(analysis_lookup.get(fsn, {}).get("Product_Title", "")),
                "Original_Final_Net_Profit": format_money(original_profit) if first_non_blank(base_row, ORIGINAL_PROFIT_COLUMNS) else "",
                "Total_Adjustment_Additions": format_money(additions),
                "Total_Adjustment_Deductions": format_money(deductions),
                "Net_Adjustment": format_money(net_adjustment),
                "Adjusted_Final_Net_Profit": format_money(adjusted_profit),
                "Adjustment_Count": str(active_count),
                "Open_Adjustment_Count": str(open_count),
                "Verified_Adjustment_Count": str(verified_count),
                "Applied_Adjustment_Count": str(applied_count),
                "Adjustment_Status": status,
                "Last_Updated": now_iso(),
            }
        )

    adjusted_rows.sort(key=lambda row: (normalize_text(row.get("Run_ID", "")), normalize_text(row.get("FSN", ""))))

    looker_rows = [{header: row.get(header, "") for header in ADJUSTED_PROFIT_HEADERS} for row in adjusted_rows]
    current_analysis_rows: List[Dict[str, Any]] = []
    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        target_run = latest_run_by_fsn.get(fsn, latest_run_id_global)
        summary = aggregated.get((target_run, fsn), {})
        additions = summary.get("additions", 0.0)
        deductions = summary.get("deductions", 0.0)
        net_adjustment = additions - deductions
        original_profit_text = first_non_blank(row, ORIGINAL_PROFIT_COLUMNS)
        original_profit = parse_float(original_profit_text)
        adjusted_profit = original_profit + net_adjustment
        active_count = int(summary.get("count", 0))
        open_count = int(summary.get("open", 0))
        verified_count = int(summary.get("verified", 0))
        applied_count = int(summary.get("applied", 0))
        status = determine_status(open_count, verified_count, applied_count, active_count)
        merged = dict(row)
        merged.update(
            {
                "Total_Adjustment_Additions": format_money(additions),
                "Total_Adjustment_Deductions": format_money(deductions),
                "Net_Adjustment": format_money(net_adjustment),
                "Adjusted_Final_Net_Profit": format_money(adjusted_profit) if (original_profit_text or active_count > 0 or abs(net_adjustment) > 1e-9) else "",
                "Adjustment_Count": str(active_count),
                "Adjustment_Status": status,
            }
        )
        current_analysis_rows.append(merged)

    sku_output_headers = merge_headers(analysis_headers, SKU_ANALYSIS_APPEND_HEADERS)
    analysis_output_rows = [{header: row.get(header, "") for header in sku_output_headers} for row in current_analysis_rows]

    SKU_ANALYSIS_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOOKER_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SKU_ANALYSIS_SNAPSHOT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=sku_output_headers)
        writer.writeheader()
        writer.writerows(analysis_output_rows)
    with LOOKER_SNAPSHOT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ADJUSTED_PROFIT_HEADERS)
        writer.writeheader()
        writer.writerows(looker_rows)

    adjusted_sheet_id = ensure_tab(sheets_service, spreadsheet_id, ADJUSTED_PROFIT_TAB)
    looker_sheet_id = ensure_tab(sheets_service, spreadsheet_id, LOOKER_ADJUSTED_PROFIT_TAB)
    analysis_sheet_id = ensure_tab(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)

    clear_tab(sheets_service, spreadsheet_id, ADJUSTED_PROFIT_TAB)
    clear_tab(sheets_service, spreadsheet_id, LOOKER_ADJUSTED_PROFIT_TAB)
    clear_tab(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    write_rows(sheets_service, spreadsheet_id, ADJUSTED_PROFIT_TAB, ADJUSTED_PROFIT_HEADERS, adjusted_rows)
    write_rows(sheets_service, spreadsheet_id, LOOKER_ADJUSTED_PROFIT_TAB, ADJUSTED_PROFIT_HEADERS, looker_rows)
    write_rows(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB, sku_output_headers, analysis_output_rows)

    freeze_and_format(sheets_service, spreadsheet_id, adjusted_sheet_id, len(ADJUSTED_PROFIT_HEADERS))
    freeze_and_format(sheets_service, spreadsheet_id, looker_sheet_id, len(ADJUSTED_PROFIT_HEADERS))
    freeze_and_format(sheets_service, spreadsheet_id, analysis_sheet_id, len(sku_output_headers))
    add_basic_filter(sheets_service, spreadsheet_id, adjusted_sheet_id, len(ADJUSTED_PROFIT_HEADERS), len(adjusted_rows) + 1)
    add_basic_filter(sheets_service, spreadsheet_id, looker_sheet_id, len(ADJUSTED_PROFIT_HEADERS), len(looker_rows) + 1)
    add_basic_filter(sheets_service, spreadsheet_id, analysis_sheet_id, len(sku_output_headers), len(analysis_output_rows) + 1)

    result = {
        "status": "SUCCESS",
        "ledger_rows_read": len(ledger_rows),
        "valid_adjustment_rows": len(valid_adjustments),
        "adjusted_profit_rows": len(adjusted_rows),
        "fsns_with_adjustments": len(fsns_with_adjustments),
        "total_additions": format_money(total_additions_all),
        "total_deductions": format_money(total_deductions_all),
        "net_adjustment": format_money(net_adjustment_all),
        "source_profit_column_used": source_profit_column_from_headers(analysis_headers),
        "tabs_updated": [ADJUSTED_PROFIT_TAB, LOOKER_ADJUSTED_PROFIT_TAB, SKU_ANALYSIS_TAB],
        "log_path": str(LOG_PATH),
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "ledger_rows_read",
            "valid_adjustment_rows",
            "adjusted_profit_rows",
            "fsns_with_adjustments",
            "total_additions",
            "total_deductions",
            "net_adjustment",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "spreadsheet_id": spreadsheet_id,
                "ledger_rows_read": len(ledger_rows),
                "valid_adjustment_rows": len(valid_adjustments),
                "adjusted_profit_rows": len(adjusted_rows),
                "fsns_with_adjustments": len(fsns_with_adjustments),
                "total_additions": result["total_additions"],
                "total_deductions": result["total_deductions"],
                "net_adjustment": result["net_adjustment"],
                "status": "SUCCESS",
                "message": "Applied Flipkart adjustments to profit views",
            }
        ],
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        build_outputs()
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
