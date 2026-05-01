from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_cogs_helpers import (
    build_cost_indexes,
    get_usable_cogs,
    is_cogs_available,
    match_cost_row,
    normalize_match_text,
)
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    normalize_text,
    write_csv,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "marketplaces" / "flipkart" / "flipkart_cogs_mapping_diagnostics.csv"
LOG_PATH = LOG_DIR / "flipkart_cogs_mapping_diagnostics_log.csv"

COST_MASTER_TAB = "FLIPKART_COST_MASTER"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"

CSV_HEADERS = [
    "Row_Index",
    "Match_Type",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Analysis_FSN",
    "Analysis_SKU_ID",
    "Analysis_Match_Type",
    "Cost_Price",
    "Packaging_Cost",
    "Other_Cost",
    "Total_Unit_COGS",
    "Derived_Total_Unit_COGS",
    "COGS_Status",
    "COGS_Source",
    "COGS_Missing_Reason",
    "Analysis_Total_Unit_COGS",
    "Analysis_COGS_Status",
    "Analysis_COGS_Source",
    "Analysis_COGS_Missing_Reason",
    "Usable_COGS_Available",
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


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        if normalize_text(sheet.get("properties", {}).get("title", "")) == tab_name:
            return True
    return False


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def first_nonblank(row: Dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = normalize_text(row.get(field, ""))
        if value:
            return value
    return ""


def build_analysis_indexes(rows: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    fsn_index: Dict[str, Dict[str, Any]] = {}
    sku_index: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        sku = normalize_match_text(row.get("SKU_ID", ""))
        if fsn and fsn not in fsn_index:
            fsn_index[fsn] = dict(row)
        if sku and sku not in sku_index:
            sku_index[sku] = dict(row)
    return fsn_index, sku_index


def build_debug_row(
    row_index: int,
    cost_row: Dict[str, Any],
    analysis_row: Dict[str, Any],
    match_type: str,
    analysis_match_type: str,
) -> Dict[str, Any]:
    cogs_snapshot = get_usable_cogs(cost_row)
    analysis_snapshot = get_usable_cogs(analysis_row)
    usable_available = normalize_text(cogs_snapshot.get("Total_Unit_COGS", "")) != ""
    return {
        "Row_Index": row_index,
        "Match_Type": match_type,
        "FSN": clean_fsn(cost_row.get("FSN", "")),
        "SKU_ID": first_nonblank(cost_row, "SKU_ID", "SKU", "Seller_SKU"),
        "Product_Title": first_nonblank(cost_row, "Product_Title", "Title"),
        "Analysis_FSN": clean_fsn(analysis_row.get("FSN", "")),
        "Analysis_SKU_ID": first_nonblank(analysis_row, "SKU_ID", "SKU", "Seller_SKU"),
        "Analysis_Match_Type": analysis_match_type,
        "Cost_Price": cogs_snapshot.get("Cost_Price", ""),
        "Packaging_Cost": cogs_snapshot.get("Packaging_Cost", ""),
        "Other_Cost": cogs_snapshot.get("Other_Cost", ""),
        "Total_Unit_COGS": cogs_snapshot.get("Total_Unit_COGS", ""),
        "Derived_Total_Unit_COGS": cogs_snapshot.get("Derived_Total_Unit_COGS", ""),
        "COGS_Status": cogs_snapshot.get("COGS_Status", ""),
        "COGS_Source": cogs_snapshot.get("COGS_Source", ""),
        "COGS_Missing_Reason": cogs_snapshot.get("COGS_Missing_Reason", ""),
        "Analysis_Total_Unit_COGS": analysis_snapshot.get("Total_Unit_COGS", ""),
        "Analysis_COGS_Status": analysis_snapshot.get("COGS_Status", ""),
        "Analysis_COGS_Source": analysis_snapshot.get("COGS_Source", ""),
        "Analysis_COGS_Missing_Reason": analysis_snapshot.get("COGS_Missing_Reason", ""),
        "Usable_COGS_Available": "Yes" if usable_available else "No",
    }


def diagnose_flipkart_cogs_mapping() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in (COST_MASTER_TAB, SKU_ANALYSIS_TAB):
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    _, cost_rows = read_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    _, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)

    analysis_fsn_index, analysis_sku_index = build_analysis_indexes(analysis_rows)
    cost_fsn_index, cost_sku_index = build_cost_indexes(cost_rows)

    debug_rows: List[Dict[str, Any]] = []
    examples_missing_cogs: List[Dict[str, Any]] = []
    examples_entered_but_not_used: List[Dict[str, Any]] = []
    matched_by_fsn_count = 0
    matched_by_sku_count = 0
    unmatched_cost_rows = 0
    cost_price_numeric_count = 0
    total_unit_cost_numeric_count = 0
    derived_total_unit_cost_count = 0
    cogs_status_entered_count = 0
    rows_with_entered_status_but_no_numeric_cogs = 0
    fsn_present_count = 0
    sku_present_count = 0

    for index, row in enumerate(cost_rows, start=2):
        cogs_snapshot = get_usable_cogs(row)
        usable_total = cogs_snapshot.get("_usable_cogs_value")
        cost_price_value = cogs_snapshot.get("_cost_price_value")
        total_value = cogs_snapshot.get("_total_unit_cogs_value")
        status = normalize_text(cogs_snapshot.get("COGS_Status", ""))
        fsn = clean_fsn(row.get("FSN", ""))
        sku = normalize_match_text(first_nonblank(row, "SKU_ID", "SKU", "Seller_SKU"))

        if cost_price_value is not None and cost_price_value > 0:
            cost_price_numeric_count += 1
        if total_value is not None and total_value > 0:
            total_unit_cost_numeric_count += 1
        if cogs_snapshot.get("COGS_Source") == "Derived_From_Cost_Price":
            derived_total_unit_cost_count += 1
        if status == "Entered":
            cogs_status_entered_count += 1
        if status == "Entered" and not usable_total:
            rows_with_entered_status_but_no_numeric_cogs += 1
            if len(examples_entered_but_not_used) < 10:
                examples_entered_but_not_used.append(
                    {
                        "Row_Index": index,
                        "FSN": fsn,
                        "SKU_ID": first_nonblank(row, "SKU_ID", "SKU", "Seller_SKU"),
                        "Product_Title": first_nonblank(row, "Product_Title", "Title"),
                        "Reason": cogs_snapshot.get("COGS_Missing_Reason", ""),
                        "COGS_Status": status,
                        "Cost_Price": cogs_snapshot.get("Cost_Price", ""),
                        "Packaging_Cost": cogs_snapshot.get("Packaging_Cost", ""),
                        "Other_Cost": cogs_snapshot.get("Other_Cost", ""),
                    }
                )

        if fsn:
            fsn_present_count += 1
        if sku:
            sku_present_count += 1

        analysis_row = {}
        analysis_match_type = ""
        if fsn and fsn in analysis_fsn_index:
            analysis_row = analysis_fsn_index[fsn]
            analysis_match_type = "FSN"
            matched_by_fsn_count += 1
        elif sku and sku in analysis_sku_index:
            analysis_row = analysis_sku_index[sku]
            analysis_match_type = "SKU_ID"
            matched_by_sku_count += 1
        else:
            unmatched_cost_rows += 1

        if not usable_total and len(examples_missing_cogs) < 10:
            examples_missing_cogs.append(
                {
                    "Row_Index": index,
                    "FSN": fsn,
                    "SKU_ID": first_nonblank(row, "SKU_ID", "SKU", "Seller_SKU"),
                    "Product_Title": first_nonblank(row, "Product_Title", "Title"),
                    "Match_Type": analysis_match_type or "Unmatched",
                    "Reason": cogs_snapshot.get("COGS_Missing_Reason", ""),
                    "COGS_Status": status,
                }
            )

        debug_rows.append(build_debug_row(index, row, analysis_row, "FSN" if fsn and fsn in cost_fsn_index else "SKU_ID" if sku and sku in cost_sku_index else "Unmatched", analysis_match_type))

    sku_analysis_cogs_missing_count = sum(1 for row in analysis_rows if clean_fsn(row.get("FSN", "")) and not is_cogs_available(row))

    warnings: List[str] = []
    if rows_with_entered_status_but_no_numeric_cogs > 0:
        warnings.append("entered status rows still lack numeric usable COGS")
    if unmatched_cost_rows > 0:
        warnings.append("some cost rows could not be matched to FLIPKART_SKU_ANALYSIS")
    if fsn_present_count < len(cost_rows):
        warnings.append("some cost rows are missing FSN")
    if sku_present_count < len(cost_rows):
        warnings.append("some cost rows are missing SKU_ID")
    if sku_analysis_cogs_missing_count > 0 and cost_price_numeric_count > 0:
        warnings.append("analysis still has missing usable COGS rows after cost price entries were found")
    if any(row.get("Match_Type") == "Unmatched" for row in debug_rows):
        warnings.append("some cost rows did not match analysis by FSN or SKU_ID")

    checks = {
        "cost_master_has_rows": len(cost_rows) > 0,
        "analysis_has_rows": len(analysis_rows) > 0,
        "cost_price_numeric_rows_present": cost_price_numeric_count > 0,
        "usable_cogs_derived_from_cost_price": derived_total_unit_cost_count > 0,
        "entered_rows_without_numeric_cogs": rows_with_entered_status_but_no_numeric_cogs == 0,
        "analysis_cogs_missing_drops": sku_analysis_cogs_missing_count < len(analysis_rows),
        "matched_rows_exist": (matched_by_fsn_count + matched_by_sku_count) > 0,
    }

    write_csv(OUTPUT_PATH, CSV_HEADERS, debug_rows)
    append_csv_log(
        LOG_PATH,
        [
            "spreadsheet_id",
            "cost_master_rows",
            "cost_price_numeric_count",
            "total_unit_cost_numeric_count",
            "derived_total_unit_cost_count",
            "cogs_status_entered_count",
            "rows_with_entered_status_but_no_numeric_cogs",
            "matched_by_fsn_count",
            "matched_by_sku_count",
            "unmatched_cost_rows",
            "sku_analysis_rows",
            "sku_analysis_cogs_missing_count",
            "status",
            "message",
        ],
        [
            {
                "spreadsheet_id": spreadsheet_id,
                "cost_master_rows": len(cost_rows),
                "cost_price_numeric_count": cost_price_numeric_count,
                "total_unit_cost_numeric_count": total_unit_cost_numeric_count,
                "derived_total_unit_cost_count": derived_total_unit_cost_count,
                "cogs_status_entered_count": cogs_status_entered_count,
                "rows_with_entered_status_but_no_numeric_cogs": rows_with_entered_status_but_no_numeric_cogs,
                "matched_by_fsn_count": matched_by_fsn_count,
                "matched_by_sku_count": matched_by_sku_count,
                "unmatched_cost_rows": unmatched_cost_rows,
                "sku_analysis_rows": len(analysis_rows),
                "sku_analysis_cogs_missing_count": sku_analysis_cogs_missing_count,
                "status": "SUCCESS",
                "message": "Diagnosed Flipkart COGS mapping",
            }
        ],
    )

    return {
        "cost_master_rows": len(cost_rows),
        "cost_price_numeric_count": cost_price_numeric_count,
        "total_unit_cost_numeric_count": total_unit_cost_numeric_count,
        "derived_total_unit_cost_count": derived_total_unit_cost_count,
        "cogs_status_entered_count": cogs_status_entered_count,
        "rows_with_entered_status_but_no_numeric_cogs": rows_with_entered_status_but_no_numeric_cogs,
        "fsn_present_count": fsn_present_count,
        "sku_present_count": sku_present_count,
        "matched_by_fsn_count": matched_by_fsn_count,
        "matched_by_sku_count": matched_by_sku_count,
        "unmatched_cost_rows": unmatched_cost_rows,
        "sku_analysis_rows": len(analysis_rows),
        "sku_analysis_cogs_missing_count": sku_analysis_cogs_missing_count,
        "examples_missing_cogs": examples_missing_cogs,
        "examples_entered_but_not_used": examples_entered_but_not_used,
        "checks": checks,
        "warnings": warnings,
        "debug_csv_path": str(OUTPUT_PATH),
        "log_path": str(LOG_PATH),
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        payload = diagnose_flipkart_cogs_mapping()
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "debug_csv_path": str(OUTPUT_PATH),
                    "log_path": str(LOG_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
