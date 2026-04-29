from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import load_json, read_table, tab_exists
from src.marketplaces.flipkart.flipkart_utils import OUTPUT_DIR, clean_fsn, normalize_text, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
MODULE_CONFIDENCE_TAB = "FLIPKART_MODULE_CONFIDENCE"
LOOKER_MODULE_CONFIDENCE_TAB = "LOOKER_FLIPKART_MODULE_CONFIDENCE"
DATA_GAP_SUMMARY_TAB = "FLIPKART_DATA_GAP_SUMMARY"

LOCAL_MODULE_CONFIDENCE_PATH = OUTPUT_DIR / "flipkart_module_confidence.csv"
LOCAL_LOOKER_CONFIDENCE_PATH = OUTPUT_DIR / "looker_flipkart_module_confidence.csv"
LOCAL_DATA_GAP_SUMMARY_PATH = OUTPUT_DIR / "flipkart_data_gap_summary.csv"

MODULE_COLUMNS = [
    "Listing_Confidence_Score",
    "Listing_Confidence_Status",
    "Listing_Confidence_Reason",
    "Order_Confidence_Score",
    "Order_Confidence_Status",
    "Order_Confidence_Reason",
    "Return_Confidence_Score",
    "Return_Confidence_Status",
    "Return_Confidence_Reason",
    "Settlement_Confidence_Score",
    "Settlement_Confidence_Status",
    "Settlement_Confidence_Reason",
    "PNL_Confidence_Score",
    "PNL_Confidence_Status",
    "PNL_Confidence_Reason",
    "COGS_Confidence_Score",
    "COGS_Confidence_Status",
    "COGS_Confidence_Reason",
    "Ads_Confidence_Score",
    "Ads_Confidence_Status",
    "Ads_Confidence_Reason",
    "Format_Confidence_Score",
    "Format_Confidence_Status",
    "Format_Confidence_Reason",
    "Alert_Risk_Score",
    "Alert_Risk_Status",
    "Alert_Risk_Reason",
    "Overall_Confidence_Score",
    "Overall_Confidence_Status",
    "Overall_Confidence_Reason",
    "Primary_Data_Gap",
    "Suggested_Data_Action",
]

LOOKER_COLUMNS = [
    "Overall_Confidence_Score",
    "Overall_Confidence_Status",
    "Primary_Data_Gap",
    "Suggested_Data_Action",
    "Listing_Confidence_Status",
    "Order_Confidence_Status",
    "Return_Confidence_Status",
    "Settlement_Confidence_Status",
    "PNL_Confidence_Status",
    "COGS_Confidence_Status",
    "Ads_Confidence_Status",
    "Format_Confidence_Status",
    "Alert_Risk_Status",
]

SKU_ANALYSIS_REQUIRED_COLUMNS = [
    "Overall_Confidence_Score",
    "Overall_Confidence_Status",
    "Primary_Data_Gap",
    "Suggested_Data_Action",
]


def latest_non_empty_row(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    for row in reversed(rows):
        if any(normalize_text(value) for value in row.values()):
            return dict(row)
    return {}


def count_non_empty_rows(rows: List[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def count_blank_fsns(rows: List[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if not clean_fsn(row.get("FSN", "")))


def validate_score_range(rows: List[Dict[str, Any]]) -> bool:
    for row in rows:
        score = parse_float(row.get("Overall_Confidence_Score", ""))
        if score < 0 or score > 100:
            return False
    return True


def distribution(rows: List[Dict[str, Any]], field_name: str, preferred_order: List[str]) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(field_name, "")) or "Unknown"
        counter[value] += 1
    ordered: Dict[str, int] = {}
    for key in preferred_order:
        if key in counter:
            ordered[key] = counter.pop(key)
    for key in sorted(counter):
        ordered[key] = counter[key]
    return ordered


def verify_flipkart_module_confidence() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    module_exists = tab_exists(sheets_service, spreadsheet_id, MODULE_CONFIDENCE_TAB)
    looker_exists = tab_exists(sheets_service, spreadsheet_id, LOOKER_MODULE_CONFIDENCE_TAB)
    summary_exists = tab_exists(sheets_service, spreadsheet_id, DATA_GAP_SUMMARY_TAB)
    sku_exists = tab_exists(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)

    module_headers: List[str] = []
    module_rows: List[Dict[str, Any]] = []
    looker_headers: List[str] = []
    looker_rows: List[Dict[str, Any]] = []
    summary_headers: List[str] = []
    summary_rows: List[Dict[str, Any]] = []
    sku_headers: List[str] = []
    sku_rows: List[Dict[str, Any]] = []

    if module_exists:
        module_headers, module_rows = read_table(sheets_service, spreadsheet_id, MODULE_CONFIDENCE_TAB)
    if looker_exists:
        looker_headers, looker_rows = read_table(sheets_service, spreadsheet_id, LOOKER_MODULE_CONFIDENCE_TAB)
    if summary_exists:
        summary_headers, summary_rows = read_table(sheets_service, spreadsheet_id, DATA_GAP_SUMMARY_TAB)
    if sku_exists:
        sku_headers, sku_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)

    latest = latest_non_empty_row(module_rows)
    run_id = normalize_text(latest.get("Run_ID", ""))

    blank_fsn_count = count_blank_fsns(module_rows) + count_blank_fsns(looker_rows)
    score_range_valid = validate_score_range(module_rows) and validate_score_range(looker_rows)
    overall_confidence_distribution = distribution(
        module_rows,
        "Overall_Confidence_Status",
        ["HIGH", "MEDIUM", "LOW", "MISSING", "REVIEW"],
    )
    primary_data_gap_distribution = distribution(
        module_rows,
        "Primary_Data_Gap",
        [
            "COGS Missing",
            "Listing Missing",
            "Settlement Missing",
            "PNL Missing",
            "Ads Mapping Weak",
            "Format Issue",
            "High Alert Risk",
            "No Major Gap",
        ],
    )

    required_module_columns_present = all(column in module_headers for column in MODULE_COLUMNS)
    required_looker_columns_present = all(column in looker_headers for column in LOOKER_COLUMNS)
    sku_required_columns_present = all(column in sku_headers for column in SKU_ANALYSIS_REQUIRED_COLUMNS)

    unique_sku_fsns = len({clean_fsn(row.get("FSN", "")) for row in sku_rows if clean_fsn(row.get("FSN", ""))})
    module_rows_match_sku_count = count_non_empty_rows(module_rows) == unique_sku_fsns

    checks = {
        "module_confidence_tab_exists": module_exists,
        "looker_module_confidence_tab_exists": looker_exists,
        "data_gap_summary_tab_exists": summary_exists,
        "row_count_matches_sku_analysis_fsn_count": module_rows_match_sku_count,
        "no_blank_fsn": blank_fsn_count == 0,
        "score_range_valid": score_range_valid,
        "overall_confidence_status_not_blank": all(normalize_text(row.get("Overall_Confidence_Status", "")) for row in module_rows),
        "primary_data_gap_not_blank": all(normalize_text(row.get("Primary_Data_Gap", "")) for row in module_rows),
        "sku_analysis_has_required_confidence_columns": sku_required_columns_present,
        "required_confidence_modules_present": required_module_columns_present,
        "required_looker_confidence_columns_present": required_looker_columns_present,
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "status": status,
        "run_id": run_id,
        "module_confidence_rows": count_non_empty_rows(module_rows),
        "looker_rows": count_non_empty_rows(looker_rows),
        "data_gap_summary_rows": count_non_empty_rows(summary_rows),
        "blank_fsn_count": blank_fsn_count,
        "overall_confidence_distribution": overall_confidence_distribution,
        "primary_data_gap_distribution": primary_data_gap_distribution,
        "score_range_valid": score_range_valid,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
        "local_outputs": {
            "flipkart_module_confidence": str(LOCAL_MODULE_CONFIDENCE_PATH),
            "looker_flipkart_module_confidence": str(LOCAL_LOOKER_CONFIDENCE_PATH),
            "flipkart_data_gap_summary": str(LOCAL_DATA_GAP_SUMMARY_PATH),
        },
    }
    return payload


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_module_confidence(), indent=2, ensure_ascii=False))
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
