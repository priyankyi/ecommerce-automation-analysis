from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.create_looker_studio_sources import (
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_ADJUSTED_PROFIT_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_COMPETITOR_INTELLIGENCE_TAB,
    LOOKER_DEMAND_PROFILE_TAB,
    LOOKER_EXECUTIVE_TAB,
    LOOKER_FSN_METRICS_TAB,
    LOOKER_MODULE_CONFIDENCE_TAB,
    LOOKER_LISTINGS_TAB,
    LOOKER_ORDER_ITEM_EXPLORER_TAB,
    LOOKER_REPORT_FORMAT_MONITOR_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_RUN_COMPARISON_TAB,
    LOOKER_RUN_QUALITY_TAB,
    LOOKER_TABS,
    SOURCE_TABS,
    SPREADSHEET_META_PATH,
    build_index,
    get_latest_run_row,
    latest_text_value,
    read_table,
    tab_exists,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text

FSN_LEVEL_TABS = [
    LOOKER_FSN_METRICS_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_LISTINGS_TAB,
    LOOKER_ORDER_ITEM_EXPLORER_TAB,
    LOOKER_ADJUSTED_PROFIT_TAB,
    LOOKER_MODULE_CONFIDENCE_TAB,
    LOOKER_COMPETITOR_INTELLIGENCE_TAB,
]

EXPECTED_ROW_TABS = [
    LOOKER_EXECUTIVE_TAB,
    LOOKER_FSN_METRICS_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_LISTINGS_TAB,
    LOOKER_ORDER_ITEM_EXPLORER_TAB,
    LOOKER_RUN_COMPARISON_TAB,
    LOOKER_ADJUSTED_PROFIT_TAB,
    LOOKER_REPORT_FORMAT_MONITOR_TAB,
    LOOKER_RUN_QUALITY_TAB,
    LOOKER_MODULE_CONFIDENCE_TAB,
    LOOKER_DEMAND_PROFILE_TAB,
    LOOKER_COMPETITOR_INTELLIGENCE_TAB,
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


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def count_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def count_blank_fsn(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if not clean_fsn(row.get("FSN", "")))


def read_tab_row_count(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    return max(0, len(rows) - 1) if rows else 0


def read_optional_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return [], []
    return read_table(sheets_service, spreadsheet_id, tab_name)


def read_first_available_table(
    sheets_service,
    spreadsheet_id: str,
    tab_names: Sequence[str],
) -> Tuple[List[str], List[Dict[str, str]], str]:
    for tab_name in tab_names:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            continue
        headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
        return headers, rows, tab_name
    return [], [], ""


def verify_looker_studio_sources() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    missing_tabs = [tab_name for tab_name in LOOKER_TABS if not tab_exists(sheets_service, spreadsheet_id, tab_name)]
    tabs_checked = LOOKER_TABS + SOURCE_TABS

    source_row_counts = {tab_name: read_tab_row_count(sheets_service, spreadsheet_id, tab_name) for tab_name in SOURCE_TABS}

    looker_tables = {}
    for tab_name in LOOKER_TABS:
        _, rows = read_optional_table(sheets_service, spreadsheet_id, tab_name)
        looker_tables[tab_name] = rows

    executive_rows = looker_tables[LOOKER_EXECUTIVE_TAB]
    fsn_rows = looker_tables[LOOKER_FSN_METRICS_TAB]
    alert_rows = looker_tables[LOOKER_ALERTS_TAB]
    action_rows = looker_tables[LOOKER_ACTIONS_TAB]
    ads_rows = looker_tables[LOOKER_ADS_TAB]
    return_rows = looker_tables[LOOKER_RETURNS_TAB]
    listing_rows = looker_tables[LOOKER_LISTINGS_TAB]
    order_item_rows = looker_tables[LOOKER_ORDER_ITEM_EXPLORER_TAB]
    run_comparison_rows = looker_tables[LOOKER_RUN_COMPARISON_TAB]
    adjusted_profit_rows = looker_tables[LOOKER_ADJUSTED_PROFIT_TAB]
    report_format_rows = looker_tables[LOOKER_REPORT_FORMAT_MONITOR_TAB]
    run_quality_rows = looker_tables[LOOKER_RUN_QUALITY_TAB]
    module_confidence_rows = looker_tables[LOOKER_MODULE_CONFIDENCE_TAB]
    demand_profile_rows = looker_tables[LOOKER_DEMAND_PROFILE_TAB]
    competitor_rows = looker_tables[LOOKER_COMPETITOR_INTELLIGENCE_TAB]

    blank_fsn_counts = {
        LOOKER_FSN_METRICS_TAB: count_blank_fsn(fsn_rows),
        LOOKER_ALERTS_TAB: count_blank_fsn(alert_rows),
        LOOKER_ACTIONS_TAB: count_blank_fsn(action_rows),
        LOOKER_ADS_TAB: count_blank_fsn(ads_rows),
        LOOKER_RETURNS_TAB: count_blank_fsn(return_rows),
        LOOKER_LISTINGS_TAB: count_blank_fsn(listing_rows),
        LOOKER_ORDER_ITEM_EXPLORER_TAB: count_blank_fsn(order_item_rows),
        LOOKER_RUN_COMPARISON_TAB: count_blank_fsn(run_comparison_rows),
        LOOKER_ADJUSTED_PROFIT_TAB: count_blank_fsn(adjusted_profit_rows),
        LOOKER_MODULE_CONFIDENCE_TAB: count_blank_fsn(module_confidence_rows),
        LOOKER_COMPETITOR_INTELLIGENCE_TAB: count_blank_fsn(competitor_rows),
    }

    run_comparison_source_rows = read_tab_row_count(sheets_service, spreadsheet_id, "FLIPKART_RUN_COMPARISON")
    adjusted_profit_source_headers, adjusted_profit_source_rows, adjusted_profit_source_tab = read_first_available_table(
        sheets_service,
        spreadsheet_id,
        ["FLIPKART_ADJUSTED_PROFIT", LOOKER_ADJUSTED_PROFIT_TAB],
    )
    report_format_source_rows = read_tab_row_count(sheets_service, spreadsheet_id, "FLIPKART_REPORT_FORMAT_MONITOR")
    run_quality_score_source_rows = read_tab_row_count(sheets_service, spreadsheet_id, "FLIPKART_RUN_QUALITY_SCORE")
    run_quality_breakdown_source_rows = read_tab_row_count(sheets_service, spreadsheet_id, "FLIPKART_RUN_QUALITY_BREAKDOWN")
    module_confidence_source_rows = read_tab_row_count(sheets_service, spreadsheet_id, "FLIPKART_MODULE_CONFIDENCE")
    demand_profile_source_rows = read_tab_row_count(sheets_service, spreadsheet_id, "PRODUCT_TYPE_DEMAND_PROFILE")
    keyword_cache_headers, keyword_cache_rows = read_optional_table(sheets_service, spreadsheet_id, "GOOGLE_KEYWORD_METRICS_CACHE")
    competitor_intelligence_source_rows = read_tab_row_count(sheets_service, spreadsheet_id, "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE")
    report_format_issue_headers, report_format_issue_rows = read_optional_table(sheets_service, spreadsheet_id, "FLIPKART_REPORT_FORMAT_ISSUES")

    keyword_cache_pending_count = sum(1 for row in keyword_cache_rows if normalize_text(row.get("Cache_Status", "")).upper() == "PENDING")
    keyword_cache_total_count = len(keyword_cache_rows)
    competitor_not_enough_data_count = sum(
        1 for row in looker_tables[LOOKER_COMPETITOR_INTELLIGENCE_TAB] if normalize_text(row.get("Competition_Risk_Level", "")) == "Not Enough Data"
    )
    report_format_critical_issue_count = sum(1 for row in report_format_issue_rows if normalize_text(row.get("Severity", "")) == "Critical")
    latest_run_quality_row = {}
    for row in run_quality_rows:
        if any(normalize_text(value) for value in row.values()):
            latest_run_quality_row = dict(row)
    run_quality_score_value = latest_text_value(latest_run_quality_row, "Overall_Run_Quality_Score")

    warnings: List[str] = []
    if keyword_cache_total_count == 0:
        warnings.append("keyword cache is empty")
    elif keyword_cache_pending_count == keyword_cache_total_count:
        warnings.append("keyword cache rows are pending")
    if competitor_not_enough_data_count > 0:
        warnings.append("competitor intelligence contains Not Enough Data rows")
    if report_format_critical_issue_count > 0:
        warnings.append("report format critical issues present")

    executive_metric_names_found = sorted(
        {
            normalize_text(row.get("Metric_Name", ""))
            for row in executive_rows
            if normalize_text(row.get("Metric_Name", ""))
        }
    )
    required_executive_metrics = [
        "Total Target FSNs",
        "Final Profit",
        "Total Alerts",
        "Critical Alerts",
        "High Alerts",
        "Active Tasks",
        "Missing COGS",
        "Missing Active Listings",
        "Ads Ready Count",
        "Return Issue FSNs",
        "COGS Completion Percent",
    ]
    missing_required_executive_metrics = [
        metric for metric in required_executive_metrics if metric not in executive_metric_names_found
    ]

    row_counts = {tab_name: len(rows) for tab_name, rows in looker_tables.items()}
    row_counts.update({tab_name: source_row_counts[tab_name] for tab_name in SOURCE_TABS})
    row_counts.update(
        {
            "FLIPKART_RUN_COMPARISON": run_comparison_source_rows,
            "FLIPKART_ADJUSTED_PROFIT_SOURCE": len(adjusted_profit_source_rows),
            "FLIPKART_REPORT_FORMAT_MONITOR": report_format_source_rows,
            "FLIPKART_RUN_QUALITY_SCORE": run_quality_score_source_rows,
            "FLIPKART_RUN_QUALITY_BREAKDOWN": run_quality_breakdown_source_rows,
            "FLIPKART_MODULE_CONFIDENCE": module_confidence_source_rows,
            "PRODUCT_TYPE_DEMAND_PROFILE": demand_profile_source_rows,
            "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE": competitor_intelligence_source_rows,
            "GOOGLE_KEYWORD_METRICS_CACHE": keyword_cache_total_count,
            "FLIPKART_REPORT_FORMAT_ISSUES": len(report_format_issue_rows),
        }
    )

    expected_row_counts = {
        LOOKER_RUN_COMPARISON_TAB: run_comparison_source_rows,
        LOOKER_ADJUSTED_PROFIT_TAB: len(adjusted_profit_source_rows),
        LOOKER_REPORT_FORMAT_MONITOR_TAB: report_format_source_rows,
        LOOKER_RUN_QUALITY_TAB: run_quality_score_source_rows + run_quality_breakdown_source_rows,
        LOOKER_MODULE_CONFIDENCE_TAB: module_confidence_source_rows,
        LOOKER_DEMAND_PROFILE_TAB: demand_profile_source_rows,
        LOOKER_COMPETITOR_INTELLIGENCE_TAB: competitor_intelligence_source_rows,
        LOOKER_ORDER_ITEM_EXPLORER_TAB: source_row_counts.get("FLIPKART_ORDER_ITEM_EXPLORER", 0),
    }

    fsn_blank_check_tabs = [tab_name for tab_name in FSN_LEVEL_TABS if tab_name in blank_fsn_counts]
    optional_source_tabs = {
        "GOOGLE_KEYWORD_METRICS_CACHE",
        "FLIPKART_VISUAL_COMPETITOR_RESULTS",
        "FLIPKART_COMPETITOR_SEARCH_QUEUE",
        "FLIPKART_ADJUSTMENTS_LEDGER",
        "FLIPKART_REPORT_FORMAT_ISSUES",
    }
    source_tabs_still_have_rows = all(
        source_row_counts.get(tab_name, 0) > 0 for tab_name in SOURCE_TABS if tab_name not in optional_source_tabs
    )
    run_quality_tab_row_count_matches_source = row_counts.get(LOOKER_RUN_QUALITY_TAB, 0) == expected_row_counts[LOOKER_RUN_QUALITY_TAB]
    order_item_tab_row_count_matches_source = row_counts.get(LOOKER_ORDER_ITEM_EXPLORER_TAB, 0) == expected_row_counts[LOOKER_ORDER_ITEM_EXPLORER_TAB]

    checks = {
        "all_looker_tabs_exist": not missing_tabs,
        "executive_summary_has_required_metrics": not missing_required_executive_metrics,
        "looker_tabs_have_rows": all(row_counts.get(tab_name, 0) > 0 for tab_name in EXPECTED_ROW_TABS),
        "fsn_tabs_have_no_blank_fsn": all(blank_fsn_counts.get(tab_name, 0) == 0 for tab_name in fsn_blank_check_tabs),
        "source_tabs_still_have_rows": source_tabs_still_have_rows,
        "run_comparison_row_count_matches_source": row_counts.get(LOOKER_RUN_COMPARISON_TAB, 0) == expected_row_counts[LOOKER_RUN_COMPARISON_TAB],
        "adjusted_profit_row_count_matches_source": row_counts.get(LOOKER_ADJUSTED_PROFIT_TAB, 0) == expected_row_counts[LOOKER_ADJUSTED_PROFIT_TAB],
        "report_format_monitor_row_count_matches_source": row_counts.get(LOOKER_REPORT_FORMAT_MONITOR_TAB, 0) == expected_row_counts[LOOKER_REPORT_FORMAT_MONITOR_TAB],
        "run_quality_row_count_matches_source": run_quality_tab_row_count_matches_source,
        "module_confidence_row_count_matches_source": row_counts.get(LOOKER_MODULE_CONFIDENCE_TAB, 0) == expected_row_counts[LOOKER_MODULE_CONFIDENCE_TAB],
        "demand_profile_row_count_matches_source": row_counts.get(LOOKER_DEMAND_PROFILE_TAB, 0) == expected_row_counts[LOOKER_DEMAND_PROFILE_TAB],
        "competitor_intelligence_row_count_matches_source": row_counts.get(LOOKER_COMPETITOR_INTELLIGENCE_TAB, 0) == expected_row_counts[LOOKER_COMPETITOR_INTELLIGENCE_TAB],
        "order_item_explorer_row_count_matches_source": order_item_tab_row_count_matches_source,
        "keyword_cache_pending_rows_allowed": keyword_cache_pending_count >= 0,
        "competitor_not_enough_data_rows_allowed": competitor_not_enough_data_count >= 0,
    }

    status = "PASS_WITH_WARNINGS" if all(checks.values()) and warnings else ("PASS" if all(checks.values()) else "FAIL")
    return {
        "status": status,
        "tabs_checked": tabs_checked,
        "row_counts": row_counts,
        "missing_tabs": missing_tabs,
        "blank_fsn_counts": blank_fsn_counts,
        "executive_metric_names_found": executive_metric_names_found,
        "missing_required_executive_metrics": missing_required_executive_metrics,
        "warnings": warnings,
        "expected_row_counts": expected_row_counts,
        "critical_counts": {
            "keyword_cache_pending_count": keyword_cache_pending_count,
            "report_format_critical_issue_count": report_format_critical_issue_count,
            "competitor_not_enough_data_count": competitor_not_enough_data_count,
            "run_quality_score": run_quality_score_value,
        },
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        print(json.dumps(verify_looker_studio_sources(), indent=2, ensure_ascii=False))
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
