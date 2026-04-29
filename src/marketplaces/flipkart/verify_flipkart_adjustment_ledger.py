from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.apply_flipkart_adjustments import (
    ADJUSTED_PROFIT_HEADERS,
    ADJUSTED_PROFIT_TAB,
    LOOKER_ADJUSTED_PROFIT_TAB,
    ORIGINAL_PROFIT_COLUMNS,
    SKU_ANALYSIS_APPEND_HEADERS,
    SKU_ANALYSIS_TAB,
)
from src.marketplaces.flipkart.create_flipkart_adjustment_ledger import (
    ADJUSTMENT_HEADERS,
    ADJUSTMENT_LEDGER_TAB,
    load_csv_rows,
)
from src.marketplaces.flipkart.flipkart_sheet_helpers import load_json, read_table, tab_exists
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, clean_fsn, normalize_text, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_adjustment_ledger_verify_log.csv"


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


def count_non_empty_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def normalize_direction(value: Any) -> str:
    text = normalize_text(value)
    for option in ["Deduction", "Addition"]:
        if text.lower() == option.lower():
            return option
    return ""


def first_non_blank(row: Dict[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = normalize_text(row.get(key, ""))
        if value:
            return value
    return ""


def source_profit_column_used(headers: Sequence[str], rows: Sequence[Dict[str, str]]) -> str:
    for key in ORIGINAL_PROFIT_COLUMNS:
        if key in headers:
            return key
    for row in rows:
        candidate = first_non_blank(row, ORIGINAL_PROFIT_COLUMNS)
        if candidate:
            for key in ORIGINAL_PROFIT_COLUMNS:
                if normalize_text(row.get(key, "")):
                    return key
    return ""


def adjustment_status_check(rows: Sequence[Dict[str, str]]) -> bool:
    for row in rows:
        count = parse_float(row.get("Adjustment_Count", ""))
        net_adjustment = parse_float(row.get("Net_Adjustment", ""))
        status = normalize_text(row.get("Adjustment_Status", ""))
        if count > 0 or abs(net_adjustment) > 1e-9:
            if status not in {"Open", "Verified", "Applied", "Needs Review", "Ignored"}:
                return False
        else:
            if status not in {"", "No Adjustments"}:
                return False
    return True


def verify_formula(rows: Sequence[Dict[str, str]]) -> bool:
    for row in rows:
        original_text = normalize_text(row.get("Original_Final_Net_Profit", ""))
        original = parse_float(original_text)
        additions = parse_float(row.get("Total_Adjustment_Additions", ""))
        deductions = parse_float(row.get("Total_Adjustment_Deductions", ""))
        net_adjustment = parse_float(row.get("Net_Adjustment", ""))
        adjusted = parse_float(row.get("Adjusted_Final_Net_Profit", ""))
        if abs(net_adjustment - (additions - deductions)) > 1e-6:
            return False
        if original_text or abs(net_adjustment) > 1e-9:
            if abs(adjusted - (original + net_adjustment)) > 1e-6:
                return False
        elif adjusted not in {"", "0", "0.0", "0.00"}:
            return False
    return True


def verify_flipkart_adjustment_ledger() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    required_tabs = [ADJUSTMENT_LEDGER_TAB, ADJUSTED_PROFIT_TAB, LOOKER_ADJUSTED_PROFIT_TAB, SKU_ANALYSIS_TAB]
    tab_presence = {tab_name: tab_exists(sheets_service, spreadsheet_id, tab_name) for tab_name in required_tabs}

    _, ledger_rows = read_table(sheets_service, spreadsheet_id, ADJUSTMENT_LEDGER_TAB)
    _, adjusted_profit_rows = read_table(sheets_service, spreadsheet_id, ADJUSTED_PROFIT_TAB)
    _, looker_rows = read_table(sheets_service, spreadsheet_id, LOOKER_ADJUSTED_PROFIT_TAB)
    analysis_headers, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)

    valid_adjustment_rows = [
        row
        for row in ledger_rows
        if clean_fsn(row.get("FSN", ""))
        and parse_float(row.get("Adjustment_Amount", "")) > 0
        and normalize_direction(row.get("Adjustment_Direction", ""))
    ]
    blank_adjustment_id_count = sum(
        1 for row in valid_adjustment_rows if not normalize_text(row.get("Adjustment_ID", ""))
    )
    fsns_with_adjustments = len(
        {
            clean_fsn(row.get("FSN", ""))
            for row in adjusted_profit_rows
            if clean_fsn(row.get("FSN", ""))
            and (parse_float(row.get("Adjustment_Count", "")) > 0 or abs(parse_float(row.get("Net_Adjustment", ""))) > 1e-9)
        }
    )
    total_additions = sum(parse_float(row.get("Total_Adjustment_Additions", "")) for row in adjusted_profit_rows)
    total_deductions = sum(parse_float(row.get("Total_Adjustment_Deductions", "")) for row in adjusted_profit_rows)
    net_adjustment = sum(parse_float(row.get("Net_Adjustment", "")) for row in adjusted_profit_rows)

    required_ledger_columns = set(ADJUSTMENT_HEADERS)
    required_adjusted_columns = set(ADJUSTED_PROFIT_HEADERS)
    required_analysis_columns = set(SKU_ANALYSIS_APPEND_HEADERS)

    ledger_headers = [str(header) for header in (ledger_rows[0].keys() if ledger_rows else ADJUSTMENT_HEADERS)]
    adjusted_headers = [str(header) for header in (adjusted_profit_rows[0].keys() if adjusted_profit_rows else ADJUSTED_PROFIT_HEADERS)]
    looker_headers = [str(header) for header in (looker_rows[0].keys() if looker_rows else ADJUSTED_PROFIT_HEADERS)]
    source_profit_column = source_profit_column_used(analysis_headers, analysis_rows)

    checks = {
        "ledger_tab_exists": tab_presence[ADJUSTMENT_LEDGER_TAB],
        "adjusted_profit_tab_exists": tab_presence[ADJUSTED_PROFIT_TAB],
        "looker_adjusted_profit_tab_exists": tab_presence[LOOKER_ADJUSTED_PROFIT_TAB],
        "sku_analysis_tab_exists": tab_presence[SKU_ANALYSIS_TAB],
        "ledger_required_columns_present": required_ledger_columns.issubset(set(ledger_headers)),
        "adjusted_profit_required_columns_present": required_adjusted_columns.issubset(set(adjusted_headers)),
        "looker_adjusted_profit_required_columns_present": required_adjusted_columns.issubset(set(looker_headers)),
        "sku_analysis_final_profit_preserved": bool(source_profit_column),
        "sku_analysis_adjustment_columns_present": required_analysis_columns.issubset(set(analysis_headers)),
        "no_blank_adjustment_id_for_amount_rows": blank_adjustment_id_count == 0,
        "adjusted_profit_formula_valid": verify_formula(adjusted_profit_rows),
        "looker_row_count_matches_adjusted_profit": count_non_empty_rows(adjusted_profit_rows) == count_non_empty_rows(looker_rows),
        "adjustment_status_values_valid": adjustment_status_check(adjusted_profit_rows),
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "ledger_rows": count_non_empty_rows(ledger_rows),
        "valid_adjustment_rows": len(valid_adjustment_rows),
        "adjusted_profit_rows": count_non_empty_rows(adjusted_profit_rows),
        "looker_adjusted_profit_rows": count_non_empty_rows(looker_rows),
        "blank_adjustment_id_count": blank_adjustment_id_count,
        "fsns_with_adjustments": fsns_with_adjustments,
        "total_additions": total_additions,
        "total_deductions": total_deductions,
        "net_adjustment": net_adjustment,
        "source_profit_column_used": source_profit_column,
        "sku_analysis_adjustment_columns_found": required_analysis_columns.issubset(set(analysis_headers)),
        "checks": checks,
    }


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_adjustment_ledger(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
