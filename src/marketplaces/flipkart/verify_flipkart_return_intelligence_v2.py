from __future__ import annotations

import json
import py_compile
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.create_flipkart_return_intelligence_v2 import (
    COURIER_COMMENTS_TAB,
    COURIER_HEADERS,
    COURIER_SUMMARY_HEADERS,
    COURIER_SUMMARY_TAB,
    CUSTOMER_COMMENTS_TAB,
    CUSTOMER_HEADERS,
    CUSTOMER_SUMMARY_HEADERS,
    CUSTOMER_SUMMARY_TAB,
    DETAIL_HEADERS,
    DETAIL_TAB,
    LOOKER_COURIER_PATH,
    LOOKER_CUSTOMER_PATH,
    LOOKER_DETAIL_PATH,
    LOOKER_PIVOT_PATH,
    LOCAL_COURIER_PATH,
    LOCAL_COURIER_SUMMARY_PATH,
    LOCAL_CUSTOMER_PATH,
    LOCAL_CUSTOMER_SUMMARY_PATH,
    LOCAL_DETAIL_PATH,
    LOCAL_PIVOT_PATH,
    RETURN_DEBUG_PATH,
    PIVOT_HEADERS,
    RAW_CSV_PATH,
    RAW_XLSX_PATH,
    RETURN_TYPE_PIVOT_TAB,
    SPREADSHEET_META_PATH,
    read_table,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text, parse_float

STREAMLIT_APP_PATH = PROJECT_ROOT / "src" / "dashboard" / "flipkart_streamlit_app.py"
LOOKER_TAB_NAMES = [
    "LOOKER_FLIPKART_RETURN_ALL_DETAILS",
    "LOOKER_FLIPKART_CUSTOMER_RETURNS",
    "LOOKER_FLIPKART_COURIER_RETURNS",
    "LOOKER_FLIPKART_RETURN_TYPE_PIVOT",
]


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    response = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    return any(str(sheet.get("properties", {}).get("title", "")) == tab_name for sheet in response.get("sheets", []))


def count_non_blank(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def count_present(rows: Sequence[Dict[str, Any]], field: str) -> int:
    return sum(1 for row in rows if normalize_text(row.get(field, "")))


def load_sheet_rows(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return [], []
    return read_table(sheets_service, spreadsheet_id, tab_name)


def row_totals(rows: Sequence[Dict[str, str]], field: str) -> int:
    return sum(1 for row in rows if normalize_text(row.get(field, "")))


def recompute_rate(detail_rows: Sequence[Dict[str, str]], bucket: str, fsn: str, sold_count: int) -> float | None:
    count = sum(1 for row in detail_rows if clean_fsn(row.get("FSN", "")) == fsn and normalize_text(row.get("Return_Bucket", "")) == bucket)
    if sold_count <= 0:
        return None
    return count / sold_count


def main() -> None:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    detail_headers, detail_rows = load_sheet_rows(sheets_service, spreadsheet_id, DETAIL_TAB)
    customer_headers, customer_rows = load_sheet_rows(sheets_service, spreadsheet_id, CUSTOMER_COMMENTS_TAB)
    courier_headers, courier_rows = load_sheet_rows(sheets_service, spreadsheet_id, COURIER_COMMENTS_TAB)
    customer_summary_headers, customer_summary_rows = load_sheet_rows(sheets_service, spreadsheet_id, CUSTOMER_SUMMARY_TAB)
    courier_summary_headers, courier_summary_rows = load_sheet_rows(sheets_service, spreadsheet_id, COURIER_SUMMARY_TAB)
    pivot_headers, pivot_rows = load_sheet_rows(sheets_service, spreadsheet_id, RETURN_TYPE_PIVOT_TAB)

    order_id_present_count = count_present(detail_rows, "Order_ID")
    order_item_id_present_count = count_present(detail_rows, "Order_Item_ID")
    deduped_return_rows = len(detail_rows)
    duplicate_return_rows_removed = sum(
        max(int(parse_float(row.get("Duplicate_Source_Count", ""))) - 1, 0)
        for row in detail_rows
        if normalize_text(row.get("Duplicate_Source_Count", ""))
    )

    customer_return_rows = sum(1 for row in detail_rows if normalize_text(row.get("Return_Bucket", "")) == "customer_return")
    courier_return_rows = sum(1 for row in detail_rows if normalize_text(row.get("Return_Bucket", "")) == "courier_return")
    unknown_return_rows = sum(1 for row in detail_rows if normalize_text(row.get("Return_Bucket", "")) == "unknown_return")
    total_return_rows = len(detail_rows)

    generated_tabs_exist = all(
        tab_exists(sheets_service, spreadsheet_id, tab_name)
        for tab_name in [DETAIL_TAB, CUSTOMER_COMMENTS_TAB, COURIER_COMMENTS_TAB, CUSTOMER_SUMMARY_TAB, COURIER_SUMMARY_TAB, RETURN_TYPE_PIVOT_TAB]
    )

    customer_summary_by_fsn = {clean_fsn(row.get("FSN", "")): row for row in customer_summary_rows if clean_fsn(row.get("FSN", ""))}
    courier_summary_by_fsn = {clean_fsn(row.get("FSN", "")): row for row in courier_summary_rows if clean_fsn(row.get("FSN", ""))}

    customer_source_safe = True
    courier_source_safe = True
    summary_mismatch_count = 0
    for fsn, row in customer_summary_by_fsn.items():
        sold = int(parse_float(row.get("Sold_Order_Items", "")))
        rate = parse_float(row.get("Customer_Return_Rate", "")) if normalize_text(row.get("Customer_Return_Rate", "")) else 0.0
        recomputed = recompute_rate(detail_rows, "customer_return", fsn, sold)
        if recomputed is None:
            if sold > 0:
                customer_source_safe = False
        elif abs(recomputed - rate) > 0.0001:
            customer_source_safe = False
            summary_mismatch_count += 1
        if any(normalize_text(row.get(field, "")) for field in ["Courier_Return_Count"]):
            customer_source_safe = False
    for fsn, row in courier_summary_by_fsn.items():
        sold = int(parse_float(row.get("Sold_Order_Items", "")))
        rate = parse_float(row.get("Courier_Return_Rate", "")) if normalize_text(row.get("Courier_Return_Rate", "")) else 0.0
        recomputed = recompute_rate(detail_rows, "courier_return", fsn, sold)
        if recomputed is None:
            if sold > 0:
                courier_source_safe = False
        elif abs(recomputed - rate) > 0.0001:
            courier_source_safe = False
            summary_mismatch_count += 1

    detail_bucket_total_matches = customer_return_rows + courier_return_rows + unknown_return_rows == total_return_rows
    order_id_or_order_item_available = order_id_present_count > 0 or order_item_id_present_count > 0
    order_id_warning = order_id_present_count == 0 and order_item_id_present_count > 0
    unknown_rows_reasonable = unknown_return_rows <= max(100, int(total_return_rows * 0.4))

    streamlit_compile_ok = True
    try:
        py_compile.compile(str(STREAMLIT_APP_PATH), doraise=True)
    except Exception:
        streamlit_compile_ok = False

    local_csv_checks = all(
        path.exists()
        for path in [LOCAL_DETAIL_PATH, LOCAL_CUSTOMER_PATH, LOCAL_COURIER_PATH, LOCAL_CUSTOMER_SUMMARY_PATH, LOCAL_COURIER_SUMMARY_PATH, LOCAL_PIVOT_PATH, LOOKER_DETAIL_PATH, LOOKER_CUSTOMER_PATH, LOOKER_COURIER_PATH, LOOKER_PIVOT_PATH, RETURN_DEBUG_PATH]
    )
    raw_files_present = RAW_CSV_PATH.exists() or RAW_XLSX_PATH.exists()

    checks = {
        "return_rows_loaded": total_return_rows > 0,
        "deduped_return_rows_gt_zero": deduped_return_rows > 0,
        "customer_return_rows_gt_zero": customer_return_rows > 0,
        "courier_return_rows_gt_zero": courier_return_rows > 0,
        "bucket_totals_match": detail_bucket_total_matches,
        "customer_return_summary_exists": len(customer_summary_rows) > 0,
        "courier_return_summary_exists": len(courier_summary_rows) > 0,
        "return_type_pivot_exists": len(pivot_rows) > 0,
        "order_id_exists": order_id_present_count > 0,
        "order_item_id_exists": order_item_id_present_count > 0,
        "customer_return_rate_is_customer_only": customer_source_safe,
        "courier_return_rate_is_courier_only": courier_source_safe,
        "unknown_return_rows_reasonable": unknown_rows_reasonable,
        "streamlit_app_compiles": streamlit_compile_ok,
        "generated_tabs_exist": generated_tabs_exist,
        "local_csvs_written": local_csv_checks,
        "raw_inputs_present": raw_files_present,
        "order_id_or_order_item_available": order_id_or_order_item_available,
    }

    warnings: List[str] = []
    if not local_csv_checks:
        warnings.append("one or more local output CSVs are missing")
    if summary_mismatch_count:
        warnings.append("one or more summary rates do not exactly match the source detail rows")
    if not raw_files_present:
        warnings.append("raw return input files are missing")
    if order_id_warning:
        warnings.append("Order_ID is missing in the combined return output, but Order_Item_ID is present so the bridge remains usable")
    if unknown_return_rows > max(50, int(total_return_rows * 0.2)):
        warnings.append("unknown return rows remain noticeable after normalization")
    if duplicate_return_rows_removed < 0:
        warnings.append("duplicate return row count could not be computed")

    hard_checks = dict(checks)
    if order_id_warning:
        hard_checks["order_id_exists"] = True
    status = "PASS" if all(hard_checks.values()) else "FAIL"
    if status == "PASS" and warnings:
        status = "PASS_WITH_WARNINGS"
    payload = {
        "status": status,
        "total_return_rows": total_return_rows,
        "deduped_return_rows": deduped_return_rows,
        "duplicate_return_rows_removed": duplicate_return_rows_removed,
        "customer_return_rows": customer_return_rows,
        "courier_return_rows": courier_return_rows,
        "unknown_return_rows": unknown_return_rows,
        "customer_summary_rows": len(customer_summary_rows),
        "courier_summary_rows": len(courier_summary_rows),
        "return_type_pivot_rows": len(pivot_rows),
        "order_id_present_count": order_id_present_count,
        "order_item_id_present_count": order_item_id_present_count,
        "checks": checks,
        "warnings": warnings,
        "spreadsheet_id": spreadsheet_id,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)
