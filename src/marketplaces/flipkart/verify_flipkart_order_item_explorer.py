from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.create_flipkart_order_item_explorer import (
    LOOKER_ORDER_ITEM_MASTER_TAB,
    LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB,
    LOOKER_ORDER_ITEM_TAB,
    LOCAL_LEGACY_ORDER_ITEM_PATH,
    ORDER_ITEM_MASTER_TAB,
    ORDER_ITEM_SOURCE_DETAIL_TAB,
    ORDER_ITEM_TAB,
    SPREADSHEET_META_PATH,
)
from src.marketplaces.flipkart.flipkart_sheet_helpers import read_table, tab_exists
from src.marketplaces.flipkart.flipkart_utils import normalize_text

SOURCE_TABS_TO_PRESERVE = [
    "FLIPKART_ACTION_TRACKER",
    "FLIPKART_COST_MASTER",
    "FLIPKART_ADS_PLANNER",
    "FLIPKART_RETURN_ALL_DETAILS",
    "FLIPKART_CUSTOMER_RETURN_COMMENTS",
    "FLIPKART_COURIER_RETURN_COMMENTS",
    "FLIPKART_RETURN_TYPE_PIVOT",
]

VALID_RETURN_TYPE_TOKENS = {
    "customer_return",
    "courier_return",
    "unknown_return",
}

VALID_YN_VALUES = {"", "yes", "no"}


def load_csv_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def read_sheet_rows(sheets_service: object, spreadsheet_id: str, tab_name: str) -> List[Dict[str, Any]]:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return []
    headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
    if not headers:
        return []
    return [{header: row.get(header, "") for header in headers} for row in rows]


def count_non_blank(df: pd.DataFrame, column: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    return int((df[column].fillna("").astype(str).map(normalize_text) != "").sum())


def count_duplicates(rows: Sequence[Dict[str, Any]], column: str) -> int:
    values = [normalize_text(row.get(column, "")) for row in rows]
    counts = Counter(value for value in values if value)
    return sum(count - 1 for count in counts.values() if count > 1)


def count_blank_fsn(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if not normalize_text(row.get("FSN", "")))


def count_valid_return_type_rows(rows: Sequence[Dict[str, Any]]) -> int:
    valid_count = 0
    for row in rows:
        value = normalize_text(row.get("Return_Type_Final", ""))
        if not value:
            continue
        tokens = [token.strip().lower() for token in value.split("|") if token.strip()]
        if tokens and all(token in VALID_RETURN_TYPE_TOKENS for token in tokens):
            valid_count += 1
    return valid_count


def count_valid_yes_no_rows(rows: Sequence[Dict[str, Any]], column: str) -> int:
    if not rows:
        return 0
    valid = 0
    for row in rows:
        value = normalize_text(row.get(column, "")).lower()
        if value in VALID_YN_VALUES:
            valid += 1
    return valid


def distribution(rows: Sequence[Dict[str, Any]], column: str) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(column, ""))
        if value:
            counter[value] += 1
    return dict(counter)


def verify_flipkart_order_item_explorer() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    legacy_order_source_df = load_csv_table(LOCAL_LEGACY_ORDER_ITEM_PATH)
    legacy_explorer_tab_exists = tab_exists(sheets_service, spreadsheet_id, ORDER_ITEM_TAB)
    looker_legacy_tab_exists = tab_exists(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_TAB)
    master_tab_exists = tab_exists(sheets_service, spreadsheet_id, ORDER_ITEM_MASTER_TAB)
    source_detail_tab_exists = tab_exists(sheets_service, spreadsheet_id, ORDER_ITEM_SOURCE_DETAIL_TAB)
    looker_master_tab_exists = tab_exists(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_MASTER_TAB)
    looker_source_detail_tab_exists = tab_exists(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB)
    source_tabs_preserved = all(tab_exists(sheets_service, spreadsheet_id, tab_name) for tab_name in SOURCE_TABS_TO_PRESERVE)

    legacy_rows = read_sheet_rows(sheets_service, spreadsheet_id, ORDER_ITEM_TAB) if legacy_explorer_tab_exists else []
    master_rows = read_sheet_rows(sheets_service, spreadsheet_id, ORDER_ITEM_MASTER_TAB) if master_tab_exists else []
    source_detail_rows = read_sheet_rows(sheets_service, spreadsheet_id, ORDER_ITEM_SOURCE_DETAIL_TAB) if source_detail_tab_exists else []
    looker_master_rows = read_sheet_rows(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_MASTER_TAB) if looker_master_tab_exists else []
    looker_source_detail_rows = read_sheet_rows(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB) if looker_source_detail_tab_exists else []

    order_id_present_count = count_non_blank(pd.DataFrame(master_rows), "Order_ID")
    order_item_id_present_count = count_non_blank(pd.DataFrame(master_rows), "Order_Item_ID")
    duplicate_order_item_id_count_master = count_duplicates(master_rows, "Order_Item_ID")
    blank_fsn_count_master = count_blank_fsn(master_rows)
    source_detail_blank_fsn_count = count_blank_fsn(source_detail_rows)
    source_row_count_populated = count_non_blank(pd.DataFrame(master_rows), "Source_Row_Count")
    sources_present_populated = count_non_blank(pd.DataFrame(master_rows), "Sources_Present")

    ids_treated_as_text = True
    for rows in (master_rows, source_detail_rows, legacy_rows):
        for row in rows:
            for column in ("Order_ID", "Order_Item_ID"):
                value = normalize_text(row.get(column, ""))
                if value and ("e+" in value.lower() or "e-" in value.lower()):
                    ids_treated_as_text = False

    return_type_valid_count = count_valid_return_type_rows(master_rows)
    return_type_present_count = count_non_blank(pd.DataFrame(master_rows), "Return_Type_Final")
    customer_return_valid_count = count_valid_yes_no_rows(master_rows, "Customer_Return_YN")
    courier_return_valid_count = count_valid_yes_no_rows(master_rows, "Courier_Return_YN")

    data_completeness_distribution = distribution(master_rows, "Data_Completeness_Status")
    return_type_distribution = distribution(master_rows, "Return_Type_Final")

    checks = {
        "legacy_explorer_tab_exists": legacy_explorer_tab_exists,
        "looker_legacy_tab_exists": looker_legacy_tab_exists,
        "master_tab_exists": master_tab_exists,
        "source_detail_tab_exists": source_detail_tab_exists,
        "looker_master_tab_exists": looker_master_tab_exists,
        "looker_source_detail_tab_exists": looker_source_detail_tab_exists,
        "source_tabs_preserved": source_tabs_preserved,
        "master_rows_present": len(master_rows) > 0,
        "source_detail_rows_present": len(source_detail_rows) > 0,
        "source_detail_row_count_ge_master": len(source_detail_rows) >= len(master_rows),
        "no_duplicate_non_blank_order_item_id_in_master": duplicate_order_item_id_count_master == 0,
        "order_id_column_present": "Order_ID" in master_rows[0] if master_rows else False,
        "order_item_id_column_present": "Order_Item_ID" in master_rows[0] if master_rows else False,
        "ids_treated_as_text": ids_treated_as_text,
        "source_row_count_populated": source_row_count_populated > 0,
        "sources_present_populated": sources_present_populated > 0,
        "return_type_values_valid_where_present": return_type_valid_count == return_type_present_count,
        "customer_return_values_valid": customer_return_valid_count == len(master_rows),
        "courier_return_values_valid": courier_return_valid_count == len(master_rows),
        "no_source_manual_tabs_wiped": source_tabs_preserved,
    }

    warnings: List[str] = []
    if blank_fsn_count_master > 0:
        warnings.append(f"{blank_fsn_count_master} master rows are missing FSN")
    if source_detail_blank_fsn_count > 0:
        warnings.append(f"{source_detail_blank_fsn_count} source detail rows are missing FSN")
    if any(not normalize_text(row.get("Order_Item_ID", "")) and normalize_text(row.get("Order_ID", "")) for row in master_rows):
        warnings.append("order-only fallback rows are present")
    if any(not normalize_text(row.get("Net_Profit", "")) for row in master_rows):
        warnings.append("some master rows are missing profit")

    if duplicate_order_item_id_count_master > 0:
        status = "FAIL"
    elif len(master_rows) == 0 or len(source_detail_rows) == 0:
        status = "FAIL"
    elif warnings:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"

    return {
        "status": status,
        "master_rows": len(master_rows),
        "source_detail_rows": len(source_detail_rows),
        "legacy_explorer_rows": len(legacy_rows),
        "order_id_present_count": order_id_present_count,
        "order_item_id_present_count": order_item_id_present_count,
        "duplicate_order_item_id_count_master": duplicate_order_item_id_count_master,
        "blank_fsn_count_master": blank_fsn_count_master,
        "source_detail_blank_fsn_count": source_detail_blank_fsn_count,
        "data_completeness_distribution": data_completeness_distribution,
        "return_type_distribution": return_type_distribution,
        "checks": checks,
        "warnings": warnings,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_order_item_explorer(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
