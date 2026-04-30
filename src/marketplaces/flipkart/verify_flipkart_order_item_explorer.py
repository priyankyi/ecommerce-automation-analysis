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
    LOOKER_ORDER_ITEM_TAB,
    LOCAL_ORDER_ITEM_PATH,
    ORDER_ITEM_TAB,
    SPREADSHEET_META_PATH,
)
from src.marketplaces.flipkart.flipkart_sheet_helpers import tab_exists, read_table
from src.marketplaces.flipkart.flipkart_utils import (
    NORMALIZED_ORDERS_PATH,
    OUTPUT_DIR,
    clean_fsn,
    normalize_text,
)

SOURCE_TABS_TO_PRESERVE = [
    "FLIPKART_SKU_ANALYSIS",
    "FLIPKART_ADJUSTED_PROFIT",
    "FLIPKART_RETURN_COMMENTS",
]


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
    return sum(1 for row in rows if not clean_fsn(row.get("FSN", "")))


def verify_flipkart_order_item_explorer() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    order_source_df = load_csv_table(NORMALIZED_ORDERS_PATH)
    source_order_data_present = not order_source_df.empty

    order_item_tab_exists = tab_exists(sheets_service, spreadsheet_id, ORDER_ITEM_TAB)
    looker_tab_exists = tab_exists(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_TAB)
    source_tabs_preserved = all(tab_exists(sheets_service, spreadsheet_id, tab_name) for tab_name in SOURCE_TABS_TO_PRESERVE)

    order_rows = read_sheet_rows(sheets_service, spreadsheet_id, ORDER_ITEM_TAB) if order_item_tab_exists else []
    looker_rows_data = read_sheet_rows(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_TAB) if looker_tab_exists else []

    order_id_present_count = sum(1 for row in order_rows if normalize_text(row.get("Order_ID", "")))
    order_item_id_present_count = sum(1 for row in order_rows if normalize_text(row.get("Order_Item_ID", "")))
    duplicate_order_item_id_count = count_duplicates(order_rows, "Order_Item_ID")
    blank_fsn_count = count_blank_fsn(order_rows)

    order_id_column_exists = bool(order_rows) and "Order_ID" in order_rows[0]
    order_item_id_column_exists = bool(order_rows) and "Order_Item_ID" in order_rows[0]
    ids_treated_as_text = True
    for row in order_rows:
        for column in ("Order_ID", "Order_Item_ID"):
            value = row.get(column, "")
            if value and not isinstance(value, str):
                ids_treated_as_text = False
            text_value = normalize_text(value)
            if text_value and ("e+" in text_value.lower() or "e-" in text_value.lower()):
                ids_treated_as_text = False

    checks = {
        "order_item_tab_exists": order_item_tab_exists,
        "looker_tab_exists": looker_tab_exists,
        "source_tabs_preserved": source_tabs_preserved,
        "order_id_column_exists": order_id_column_exists,
        "order_item_id_column_exists": order_item_id_column_exists,
        "ids_treated_as_text": ids_treated_as_text,
        "no_duplicate_order_item_id": duplicate_order_item_id_count == 0,
        "no_blank_fsn_when_source_exists": (blank_fsn_count == 0) if source_order_data_present else True,
        "rows_present_when_source_exists": (len(order_rows) > 0) if source_order_data_present else True,
        "looker_rows_present_when_source_exists": (len(looker_rows_data) > 0) if source_order_data_present else True,
    }

    warnings: List[str] = []
    if not source_order_data_present:
        warnings.append("Normalized order source data is missing locally; row count checks were relaxed.")
    if order_item_tab_exists and not order_rows:
        warnings.append("Order item explorer tab exists but has no rows yet.")
    if duplicate_order_item_id_count > 0:
        warnings.append("Duplicate Order_Item_ID values were found.")

    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "order_item_rows": len(order_rows),
        "looker_rows": len(looker_rows_data),
        "order_id_present_count": order_id_present_count,
        "order_item_id_present_count": order_item_id_present_count,
        "duplicate_order_item_id_count": duplicate_order_item_id_count,
        "blank_fsn_count": blank_fsn_count,
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
