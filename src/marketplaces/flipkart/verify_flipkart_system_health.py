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
from src.marketplaces.flipkart.create_looker_studio_sources import (
    LOOKER_GROUP_TABS,
    LOOKER_LARGE_TABS,
    LOOKER_REFRESH_MANIFEST_PATH,
)
from src.marketplaces.flipkart.flipkart_cogs_helpers import count_cogs_rows, get_usable_cogs, is_cogs_available
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
ORDER_ITEM_REFRESH_MANIFEST_PATH = PROJECT_ROOT / "data" / "output" / "marketplaces" / "flipkart" / "order_item_refresh_manifest.json"
ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH = PROJECT_ROOT / "data" / "output" / "marketplaces" / "flipkart" / "order_item_looker_refresh_manifest.json"

TABS_TO_CHECK = [
    "FLIPKART_SKU_ANALYSIS",
    "FLIPKART_COST_MASTER",
    "FLIPKART_ALERTS_GENERATED",
    "FLIPKART_ACTION_TRACKER",
    "FLIPKART_ACTIVE_TASKS",
    "FLIPKART_DASHBOARD",
    "FLIPKART_FSN_DRILLDOWN",
    "FLIPKART_RETURN_COMMENTS",
    "FLIPKART_RETURN_ISSUE_SUMMARY",
    "FLIPKART_RETURN_ALL_DETAILS",
    "FLIPKART_CUSTOMER_RETURN_COMMENTS",
    "FLIPKART_COURIER_RETURN_COMMENTS",
    "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY",
    "FLIPKART_COURIER_RETURN_SUMMARY",
    "FLIPKART_RETURN_TYPE_PIVOT",
    "FLIPKART_ADS_PLANNER",
    "FLIPKART_ADS_MASTER",
    "FLIPKART_LISTING_PRESENCE",
    "FLIPKART_MISSING_ACTIVE_LISTINGS",
    "FLIPKART_RUN_HISTORY",
    "FLIPKART_FSN_HISTORY",
    "FLIPKART_ORDER_ITEM_EXPLORER",
    "FLIPKART_ORDER_ITEM_MASTER",
    "FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
    "FLIPKART_ADJUSTMENTS_LEDGER",
    "FLIPKART_ADJUSTED_PROFIT",
    "FLIPKART_RUN_COMPARISON",
    "FLIPKART_FSN_RUN_COMPARISON",
    "FLIPKART_REPORT_FORMAT_MONITOR",
    "FLIPKART_REPORT_FORMAT_ISSUES",
    "FLIPKART_RUN_QUALITY_SCORE",
    "FLIPKART_RUN_QUALITY_BREAKDOWN",
    "FLIPKART_MODULE_CONFIDENCE",
    "FLIPKART_DATA_GAP_SUMMARY",
    "GOOGLE_KEYWORD_METRICS_CACHE",
    "PRODUCT_TYPE_DEMAND_PROFILE",
    "FLIPKART_COMPETITOR_SEARCH_QUEUE",
    "FLIPKART_VISUAL_COMPETITOR_RESULTS",
    "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE",
    "LOOKER_FLIPKART_FSN_METRICS",
    "LOOKER_FLIPKART_ADS",
    "LOOKER_FLIPKART_RETURNS",
    "LOOKER_FLIPKART_RETURN_TYPE_PIVOT",
    "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER",
    "LOOKER_FLIPKART_ORDER_ITEM_MASTER",
    "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
]


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in {429, 500, 502, 503} or attempt == attempts:
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


def ensure_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
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


def get_sheet_values_batch(sheets_service, spreadsheet_id: str, ranges: Sequence[str]) -> Dict[str, List[List[Any]]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=list(ranges))
        .execute()
    )
    tables: Dict[str, List[List[Any]]] = {}
    for value_range in response.get("valueRanges", []):
        range_name = str(value_range.get("range", ""))
        tab_name = range_name.split("!", 1)[0]
        tables[tab_name] = value_range.get("values", [])
    return tables


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def row_count(rows: Sequence[Dict[str, Any]]) -> int:
    return len(rows)


def count_alerts(rows: Sequence[Dict[str, Any]], severity: str) -> int:
    target = normalize_text(severity)
    return sum(1 for row in rows if normalize_text(row.get("Severity", "")) == target)


def count_missing_cogs(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if not is_cogs_available(row))


def count_ads_ready(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if normalize_text(row.get("Ads_Readiness_Status", "")) == "Ready")


def count_active_tasks(rows: Sequence[Dict[str, Any]]) -> int:
    return row_count(rows)


def _load_order_item_looker_manifest() -> Dict[str, Any]:
    if not ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH.exists():
        return {}
    try:
        return json.loads(ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_order_item_refresh_manifest() -> Dict[str, Any]:
    if not ORDER_ITEM_REFRESH_MANIFEST_PATH.exists():
        return {}
    try:
        return json.loads(ORDER_ITEM_REFRESH_MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def verify_flipkart_system_health() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    metadata = get_metadata(sheets_service, spreadsheet_id)
    available_tabs = {
        str(sheet.get("properties", {}).get("title", ""))
        for sheet in metadata.get("sheets", [])
        if str(sheet.get("properties", {}).get("title", ""))
    }
    manifest: Dict[str, Any] = {}
    if LOOKER_REFRESH_MANIFEST_PATH.exists():
        try:
            manifest = load_json(LOOKER_REFRESH_MANIFEST_PATH)
        except Exception:
            manifest = {}
    looker_group = str(manifest.get("last_group", "light") or "light").lower()
    quota_safe_mode = bool(manifest.get("quota_safe_mode", looker_group != "full"))
    order_item_internal_manifest = _load_order_item_refresh_manifest()
    order_item_manifest = _load_order_item_looker_manifest()
    order_item_internal_mode = str(
        order_item_internal_manifest.get("internal_mode", order_item_internal_manifest.get("last_order_item_internal_mode", "master-only"))
        or "master-only"
    ).lower()
    order_item_looker_mode = str(order_item_manifest.get("looker_mode", order_item_manifest.get("last_order_item_looker_mode", "master-only")) or "master-only").lower()
    order_item_large_internal_tabs_optional = order_item_internal_mode != "full"
    order_item_large_looker_tabs_optional = order_item_looker_mode != "full"
    light_tabs = list(LOOKER_GROUP_TABS.get("light", []))
    required_looker_tabs = list(TABS_TO_CHECK if looker_group == "full" else [tab for tab in TABS_TO_CHECK if tab not in LOOKER_LARGE_TABS])
    if order_item_large_looker_tabs_optional:
        required_looker_tabs = [
            tab_name
            for tab_name in required_looker_tabs
            if tab_name not in {
                "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER",
                "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
            }
        ]
    missing_tabs = [tab_name for tab_name in TABS_TO_CHECK if tab_name not in available_tabs]
    missing_required_tabs = [tab_name for tab_name in required_looker_tabs if tab_name not in available_tabs]
    missing_large_tabs = [tab_name for tab_name in LOOKER_LARGE_TABS if tab_name not in available_tabs]

    tables: Dict[str, Tuple[List[str], List[Dict[str, str]]]] = {}
    row_counts: Dict[str, int] = {}
    batch_tabs = [tab_name for tab_name in TABS_TO_CHECK if tab_name not in missing_tabs]
    batch_ranges = [f"{tab_name}!A1:ZZ" for tab_name in batch_tabs]
    batch_values = get_sheet_values_batch(sheets_service, spreadsheet_id, batch_ranges) if batch_ranges else {}

    for tab_name in TABS_TO_CHECK:
        if tab_name in missing_tabs:
            tables[tab_name] = ([], [])
            row_counts[tab_name] = 0
            continue
        rows = batch_values.get(tab_name, [])
        headers = [str(cell) for cell in rows[0]] if rows else []
        rows_data: List[Dict[str, str]] = []
        for row in rows[1:]:
            rows_data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
        tables[tab_name] = (headers, rows_data)
        row_counts[tab_name] = row_count(rows_data)

    sku_rows = tables["FLIPKART_SKU_ANALYSIS"][1]
    cost_rows = tables["FLIPKART_COST_MASTER"][1]
    alerts_rows = tables["FLIPKART_ALERTS_GENERATED"][1]
    tracker_rows = tables["FLIPKART_ACTION_TRACKER"][1]
    active_rows = tables["FLIPKART_ACTIVE_TASKS"][1]
    ads_planner_rows = tables["FLIPKART_ADS_PLANNER"][1]
    missing_listing_rows = tables["FLIPKART_MISSING_ACTIVE_LISTINGS"][1]
    order_item_rows = tables["FLIPKART_ORDER_ITEM_EXPLORER"][1]
    order_item_master_rows = tables["FLIPKART_ORDER_ITEM_MASTER"][1]
    order_item_source_detail_rows = tables["FLIPKART_ORDER_ITEM_SOURCE_DETAIL"][1]
    return_issue_rows = tables["FLIPKART_RETURN_ISSUE_SUMMARY"][1]
    return_all_details_rows = tables["FLIPKART_RETURN_ALL_DETAILS"][1]
    customer_return_rows = tables["FLIPKART_CUSTOMER_RETURN_COMMENTS"][1]
    courier_return_rows = tables["FLIPKART_COURIER_RETURN_COMMENTS"][1]
    customer_summary_rows = tables["FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY"][1]
    courier_summary_rows = tables["FLIPKART_COURIER_RETURN_SUMMARY"][1]
    return_type_pivot_rows = tables["FLIPKART_RETURN_TYPE_PIVOT"][1]
    run_comparison_rows = tables["FLIPKART_RUN_COMPARISON"][1]
    adjusted_profit_rows = tables["FLIPKART_ADJUSTED_PROFIT"][1]
    report_format_monitor_rows = tables["FLIPKART_REPORT_FORMAT_MONITOR"][1]
    report_format_issue_rows = tables["FLIPKART_REPORT_FORMAT_ISSUES"][1]
    run_quality_score_rows = tables["FLIPKART_RUN_QUALITY_SCORE"][1]
    run_quality_breakdown_rows = tables["FLIPKART_RUN_QUALITY_BREAKDOWN"][1]
    module_confidence_rows = tables["FLIPKART_MODULE_CONFIDENCE"][1]
    data_gap_summary_rows = tables["FLIPKART_DATA_GAP_SUMMARY"][1]
    keyword_cache_rows = tables["GOOGLE_KEYWORD_METRICS_CACHE"][1]
    demand_profile_rows = tables["PRODUCT_TYPE_DEMAND_PROFILE"][1]
    competitor_queue_rows = tables["FLIPKART_COMPETITOR_SEARCH_QUEUE"][1]
    competitor_result_rows = tables["FLIPKART_VISUAL_COMPETITOR_RESULTS"][1]
    competitor_price_rows = tables["FLIPKART_COMPETITOR_PRICE_INTELLIGENCE"][1]
    looker_fsn_metrics_rows = tables["LOOKER_FLIPKART_FSN_METRICS"][1]
    looker_returns_rows = tables["LOOKER_FLIPKART_RETURNS"][1]
    looker_ads_rows = tables["LOOKER_FLIPKART_ADS"][1]
    looker_return_type_pivot_rows = tables["LOOKER_FLIPKART_RETURN_TYPE_PIVOT"][1]
    looker_order_item_rows = tables["LOOKER_FLIPKART_ORDER_ITEM_EXPLORER"][1]
    looker_order_item_master_rows = tables["LOOKER_FLIPKART_ORDER_ITEM_MASTER"][1]
    looker_order_item_source_detail_rows = tables["LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL"][1]
    row_counts = {tab_name: row_count(rows_data) for tab_name, (_, rows_data) in tables.items()}
    analysis_cogs_available, analysis_cogs_missing = count_cogs_rows(sku_rows)
    cost_master_cogs_available, cost_master_cogs_missing = count_cogs_rows(cost_rows)
    cost_rows_with_price_no_id = sum(
        1
        for row in cost_rows
        if normalize_text(get_usable_cogs(row).get("Cost_Price", ""))
        and not clean_fsn(row.get("FSN", ""))
        and not normalize_text(row.get("SKU_ID", ""))
    )

    run_quality_score_value = 0.0
    if run_quality_score_rows:
        latest_run_quality_score = next((row for row in reversed(run_quality_score_rows) if any(normalize_text(value) for value in row.values())), {})
        run_quality_score_value = parse_float(latest_run_quality_score.get("Overall_Run_Quality_Score", ""))
    keyword_cache_pending_count = sum(1 for row in keyword_cache_rows if normalize_text(row.get("Cache_Status", "")).upper() == "PENDING")
    keyword_cache_success_count = sum(1 for row in keyword_cache_rows if normalize_text(row.get("Cache_Status", "")).upper() == "SUCCESS")
    keyword_cache_total_count = len(keyword_cache_rows)
    competitor_critical_risk_count = sum(1 for row in competitor_price_rows if normalize_text(row.get("Competition_Risk_Level", "")) == "Critical")
    competitor_medium_risk_count = sum(1 for row in competitor_price_rows if normalize_text(row.get("Competition_Risk_Level", "")) == "Medium")
    competitor_not_enough_data_count = sum(1 for row in competitor_price_rows if normalize_text(row.get("Competition_Risk_Level", "")) == "Not Enough Data")
    report_format_critical_issue_count = sum(1 for row in report_format_issue_rows if normalize_text(row.get("Severity", "")) == "Critical")
    low_confidence_count = sum(1 for row in module_confidence_rows if normalize_text(row.get("Overall_Confidence_Status", "")) == "LOW")
    customer_return_count = sum(1 for row in return_all_details_rows if normalize_text(row.get("Return_Bucket", "")) == "customer_return")
    courier_return_count = sum(1 for row in return_all_details_rows if normalize_text(row.get("Return_Bucket", "")) == "courier_return")
    unknown_return_count = sum(1 for row in return_all_details_rows if normalize_text(row.get("Return_Bucket", "")) == "unknown_return")
    looker_fsn_metrics_has_return_fields = bool(looker_fsn_metrics_rows) and all(
        field in looker_fsn_metrics_rows[0]
        for field in [
            "Customer_Return_Count",
            "Customer_Return_Rate",
            "Courier_Return_Count",
            "Courier_Return_Rate",
            "Total_Return_Count",
            "Total_Return_Rate",
        ]
    )
    looker_returns_has_return_fields = bool(looker_returns_rows) and all(
        field in looker_returns_rows[0]
        for field in [
            "Customer_Return_Count",
            "Courier_Return_Count",
            "Unknown_Return_Count",
            "Customer_Return_Rate",
            "Courier_Return_Rate",
            "Total_Return_Rate",
        ]
    )
    looker_ads_has_return_fields = bool(looker_ads_rows) and all(
        field in looker_ads_rows[0]
        for field in [
            "Customer_Return_Rate",
            "Courier_Return_Rate",
            "Total_Return_Rate",
        ]
    )
    critical_customer_return_fsn_count = sum(1 for row in customer_summary_rows if normalize_text(row.get("Customer_Return_Risk_Level", "")) == "Critical")
    high_courier_return_fsn_count = sum(1 for row in courier_summary_rows if normalize_text(row.get("Courier_Return_Risk_Level", "")) == "High")
    order_item_master_blank_fsn_count = sum(1 for row in order_item_master_rows if not normalize_text(row.get("FSN", "")))
    order_item_source_detail_blank_fsn_count = sum(1 for row in order_item_source_detail_rows if not normalize_text(row.get("FSN", "")))
    order_item_master_missing_profit_count = sum(1 for row in order_item_master_rows if not normalize_text(row.get("Net_Profit", "")))
    order_item_master_order_only_count = sum(1 for row in order_item_master_rows if not normalize_text(row.get("Order_Item_ID", "")) and normalize_text(row.get("Order_ID", "")))

    warnings: List[str] = []
    if keyword_cache_total_count == 0:
        warnings.append("keyword cache rows pending")
    elif keyword_cache_pending_count == keyword_cache_total_count:
        warnings.append("keyword cache rows pending")
    if competitor_not_enough_data_count > 0:
        warnings.append("competitor intelligence contains Not Enough Data rows")
    if report_format_critical_issue_count > 0:
        warnings.append("report format critical issues present")
    if order_item_master_blank_fsn_count > 0:
        warnings.append("order item master has blank FSN rows")
    if order_item_source_detail_blank_fsn_count > 0:
        warnings.append("order item source detail has blank FSN rows")
    if order_item_master_missing_profit_count > 0:
        warnings.append("order item master has missing profit rows")
    if order_item_master_order_only_count > 0:
        warnings.append("order-only fallback rows are present")
    if cost_rows_with_price_no_id > 0:
        warnings.append("some cost rows have Cost_Price but blank FSN/SKU and cannot be mapped")
    if not looker_fsn_metrics_has_return_fields:
        warnings.append("looker fsn metrics is missing explicit return fields")
    if not looker_returns_has_return_fields:
        warnings.append("looker returns is missing explicit return fields")
    if not looker_ads_has_return_fields:
        warnings.append("looker ads is missing explicit return split fields")
    if looker_group == "full":
        if "FLIPKART_ORDER_ITEM_EXPLORER" not in available_tabs:
            warnings.append("order item explorer source tab is missing")
        elif not order_item_rows:
            warnings.append("order item explorer source tab is empty")
        if "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER" not in available_tabs:
            warnings.append("looker order item explorer tab is missing")
        elif not looker_order_item_rows:
            warnings.append("looker order item explorer tab is empty")
        if "FLIPKART_ORDER_ITEM_MASTER" not in available_tabs:
            warnings.append("order item master tab is missing")
        elif not order_item_master_rows:
            warnings.append("order item master tab is empty")
        if "FLIPKART_ORDER_ITEM_SOURCE_DETAIL" not in available_tabs:
            warnings.append("order item source detail tab is missing")
        elif not order_item_source_detail_rows:
            warnings.append("order item source detail tab is empty")
        if "LOOKER_FLIPKART_ORDER_ITEM_MASTER" not in available_tabs:
            warnings.append("looker order item master tab is missing")
        elif not looker_order_item_master_rows:
            warnings.append("looker order item master tab is empty")
        if "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL" not in available_tabs:
            warnings.append("looker order item source detail tab is missing")
        elif not looker_order_item_source_detail_rows:
            warnings.append("looker order item source detail tab is empty")

    optional_zero_row_tabs = {
        "FLIPKART_ADJUSTMENTS_LEDGER",
        "FLIPKART_REPORT_FORMAT_ISSUES",
        "GOOGLE_KEYWORD_METRICS_CACHE",
        "FLIPKART_COMPETITOR_SEARCH_QUEUE",
        "FLIPKART_VISUAL_COMPETITOR_RESULTS",
        "FLIPKART_RETURN_ALL_DETAILS",
    }
    if order_item_large_looker_tabs_optional:
        optional_zero_row_tabs.update(
            {
                "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER",
                "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
            }
        )
    required_row_tabs = [tab_name for tab_name in TABS_TO_CHECK if tab_name not in optional_zero_row_tabs]
    required_missing_tabs = [tab_name for tab_name in missing_tabs if tab_name not in optional_zero_row_tabs]

    critical_counts = {
        "active_tasks": count_active_tasks(active_rows),
        "critical_alerts": count_alerts(alerts_rows, "Critical"),
        "missing_cogs": count_missing_cogs(cost_rows),
        "analysis_cogs_available": analysis_cogs_available,
        "analysis_cogs_missing": analysis_cogs_missing,
        "cost_master_cogs_available": cost_master_cogs_available,
        "cost_master_cogs_missing": cost_master_cogs_missing,
        "missing_active_listings": row_count(missing_listing_rows),
        "ads_ready_count": count_ads_ready(ads_planner_rows),
        "return_issue_summary_rows": row_count(return_issue_rows),
        "return_all_details_rows": row_count(return_all_details_rows),
        "customer_return_rows": row_count(customer_return_rows),
        "courier_return_rows": row_count(courier_return_rows),
        "customer_return_summary_rows": row_count(customer_summary_rows),
        "courier_return_summary_rows": row_count(courier_summary_rows),
        "return_type_pivot_rows": row_count(return_type_pivot_rows),
        "run_quality_score": run_quality_score_value,
        "low_confidence_count": low_confidence_count,
        "critical_competition_risk_count": competitor_critical_risk_count,
        "medium_competition_risk_count": competitor_medium_risk_count,
        "keyword_cache_pending_count": keyword_cache_pending_count,
        "report_format_critical_issue_count": report_format_critical_issue_count,
        "critical_customer_return_fsn_count": critical_customer_return_fsn_count,
        "high_courier_return_fsn_count": high_courier_return_fsn_count,
    }

    customer_only_rows = row_count(customer_return_rows)
    courier_only_rows = row_count(courier_return_rows)
    target_fsn = "OTLGPN7CVFCTRBQF"
    target_planner_row = next((row for row in ads_planner_rows if clean_fsn(row.get("FSN", "")) == target_fsn), {})
    target_customer_return_rate = parse_float(target_planner_row.get("Customer_Return_Rate", ""))
    target_courier_return_rate = parse_float(target_planner_row.get("Courier_Return_Rate", ""))
    target_total_return_rate = parse_float(target_planner_row.get("Total_Return_Rate", ""))
    target_final_decision = normalize_text(target_planner_row.get("Final_Ads_Decision", ""))
    target_reason = normalize_text(target_planner_row.get("Ads_Decision_Reason", ""))
    target_listing_readiness = normalize_text(target_planner_row.get("Listing_Readiness", ""))
    generic_split_reason_count = sum(
        1
        for row in ads_planner_rows
        if normalize_text(row.get("Customer_Return_Rate", "")) and normalize_text(row.get("Courier_Return_Rate", "")) and normalize_text(row.get("Ads_Decision_Reason", "")).lower().startswith("return rate")
    )
    if generic_split_reason_count > 0:
        warnings.append("ads planner still contains generic return-rate reasons")
    if target_listing_readiness != "Bad" and target_final_decision in {"Fix Product First", "Fix Product/Listing First"} and not (
        "customer return rate" in target_reason.lower() or "courier return risk" in target_reason.lower()
    ):
        warnings.append("target fsn still uses a product-blocking decision without split-specific wording")

    checks = {
        "all_required_tabs_present": not required_missing_tabs,
        "sku_analysis_has_rows": row_counts["FLIPKART_SKU_ANALYSIS"] > 0,
        "cost_master_has_rows": row_counts["FLIPKART_COST_MASTER"] > 0,
        "analysis_cogs_available_gt_zero": analysis_cogs_available > 0,
        "analysis_cogs_missing_less_than_total": analysis_cogs_missing <= len(sku_rows),
        "cost_master_cogs_available_gt_zero": cost_master_cogs_available > 0,
        "alerts_generated_has_rows": row_counts["FLIPKART_ALERTS_GENERATED"] > 0,
        "action_tracker_has_rows": row_counts["FLIPKART_ACTION_TRACKER"] > 0,
        "active_tasks_has_rows": row_counts["FLIPKART_ACTIVE_TASKS"] > 0,
        "dashboard_has_rows": row_counts["FLIPKART_DASHBOARD"] > 0,
        "fsn_drilldown_has_rows": row_counts["FLIPKART_FSN_DRILLDOWN"] > 0,
        "return_comments_has_rows": row_counts["FLIPKART_RETURN_COMMENTS"] > 0,
        "return_issue_summary_has_rows": row_counts["FLIPKART_RETURN_ISSUE_SUMMARY"] > 0,
        "return_all_details_has_rows": "FLIPKART_RETURN_ALL_DETAILS" in optional_zero_row_tabs or row_counts["FLIPKART_RETURN_ALL_DETAILS"] > 0,
        "customer_return_comments_has_rows": row_counts["FLIPKART_CUSTOMER_RETURN_COMMENTS"] > 0,
        "courier_return_comments_has_rows": row_counts["FLIPKART_COURIER_RETURN_COMMENTS"] > 0,
        "customer_return_summary_has_rows": row_counts["FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY"] > 0,
        "courier_return_summary_has_rows": row_counts["FLIPKART_COURIER_RETURN_SUMMARY"] > 0,
        "return_type_pivot_has_rows": row_counts["FLIPKART_RETURN_TYPE_PIVOT"] > 0,
        "ads_planner_has_rows": row_counts["FLIPKART_ADS_PLANNER"] > 0,
        "ads_master_has_rows": row_counts["FLIPKART_ADS_MASTER"] > 0,
        "listing_presence_has_rows": row_counts["FLIPKART_LISTING_PRESENCE"] > 0,
        "missing_active_listings_has_rows": row_counts["FLIPKART_MISSING_ACTIVE_LISTINGS"] > 0,
        "run_history_has_rows": row_counts["FLIPKART_RUN_HISTORY"] > 0,
        "fsn_history_has_rows": row_counts["FLIPKART_FSN_HISTORY"] > 0,
        "adjusted_profit_has_rows": row_counts["FLIPKART_ADJUSTED_PROFIT"] > 0,
        "run_comparison_has_rows": row_counts["FLIPKART_RUN_COMPARISON"] > 0,
        "fsn_run_comparison_has_rows": row_counts["FLIPKART_FSN_RUN_COMPARISON"] > 0,
        "report_format_monitor_has_rows": row_counts["FLIPKART_REPORT_FORMAT_MONITOR"] > 0,
        "run_quality_score_has_rows": row_counts["FLIPKART_RUN_QUALITY_SCORE"] > 0,
        "run_quality_breakdown_has_rows": row_counts["FLIPKART_RUN_QUALITY_BREAKDOWN"] > 0,
        "module_confidence_has_rows": row_counts["FLIPKART_MODULE_CONFIDENCE"] > 0,
        "data_gap_summary_has_rows": row_counts["FLIPKART_DATA_GAP_SUMMARY"] > 0,
        "keyword_cache_tab_exists": "GOOGLE_KEYWORD_METRICS_CACHE" not in missing_tabs,
        "demand_profile_has_rows": row_counts["PRODUCT_TYPE_DEMAND_PROFILE"] > 0,
        "competitor_search_queue_tab_exists": "FLIPKART_COMPETITOR_SEARCH_QUEUE" not in missing_tabs,
        "competitor_intelligence_has_rows": row_counts["FLIPKART_COMPETITOR_PRICE_INTELLIGENCE"] > 0,
        "optional_visual_results_tab_exists": "FLIPKART_VISUAL_COMPETITOR_RESULTS" not in missing_tabs,
        "report_format_issues_tab_exists": "FLIPKART_REPORT_FORMAT_ISSUES" not in missing_tabs,
        "adjustments_ledger_tab_exists": "FLIPKART_ADJUSTMENTS_LEDGER" not in missing_tabs,
        "order_item_explorer_has_rows": order_item_large_internal_tabs_optional or row_counts["FLIPKART_ORDER_ITEM_EXPLORER"] > 0,
        "order_item_master_has_rows": row_counts["FLIPKART_ORDER_ITEM_MASTER"] > 0,
        "order_item_source_detail_has_rows": order_item_large_internal_tabs_optional or row_counts["FLIPKART_ORDER_ITEM_SOURCE_DETAIL"] > 0,
        "looker_order_item_explorer_has_rows": order_item_large_looker_tabs_optional or row_counts["LOOKER_FLIPKART_ORDER_ITEM_EXPLORER"] > 0,
        "looker_order_item_master_has_rows": row_counts["LOOKER_FLIPKART_ORDER_ITEM_MASTER"] > 0,
        "looker_order_item_source_detail_has_rows": order_item_large_looker_tabs_optional or row_counts["LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL"] > 0,
        "order_item_master_looker_present": row_counts["LOOKER_FLIPKART_ORDER_ITEM_MASTER"] > 0,
        "order_item_internal_mode": order_item_internal_mode,
        "order_item_quick_mode_quota_safe": order_item_large_internal_tabs_optional and order_item_large_looker_tabs_optional,
        "order_item_large_internal_tabs_optional": order_item_large_internal_tabs_optional,
        "order_item_large_looker_tabs_optional": order_item_large_looker_tabs_optional,
        "order_item_source_detail_skipped_ok": order_item_large_looker_tabs_optional or row_counts["LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL"] > 0,
        "order_item_internal_manifest_exists": ORDER_ITEM_REFRESH_MANIFEST_PATH.exists(),
        "order_item_looker_manifest_exists": ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH.exists(),
        "customer_return_rate_source_is_customer_only": customer_only_rows == row_counts["FLIPKART_CUSTOMER_RETURN_COMMENTS"],
        "courier_return_rate_source_is_courier_only": courier_only_rows == row_counts["FLIPKART_COURIER_RETURN_COMMENTS"],
        "looker_fsn_metrics_has_return_fields": looker_fsn_metrics_has_return_fields,
        "looker_returns_has_return_fields": looker_returns_has_return_fields,
        "looker_ads_has_return_fields": looker_ads_has_return_fields,
        "looker_manifest_exists": LOOKER_REFRESH_MANIFEST_PATH.exists(),
        "looker_light_tabs_exist": all(tab_name in available_tabs for tab_name in light_tabs),
        "looker_large_tabs_optional": looker_group != "full" or all(tab_name in available_tabs for tab_name in LOOKER_LARGE_TABS),
        "looker_refresh_quota_safe_mode": quota_safe_mode,
        "target_fsn_customer_return_rate_ok": abs(target_customer_return_rate - 0.0788) < 0.001,
        "target_fsn_courier_return_rate_ok": abs(target_courier_return_rate - 0.2614) < 0.001,
        "target_fsn_total_return_rate_ok": bool(normalize_text(target_planner_row.get("Total_Return_Rate", ""))),
        "target_fsn_not_product_blocked_by_total_rate": target_final_decision not in {"Fix Product First", "Fix Product/Listing First"} and "customer return rate acceptable; courier return risk elevated" in target_reason.lower(),
    }

    checks["all_required_tabs_present"] = not required_missing_tabs

    status = "PASS_WITH_WARNINGS" if all(checks.values()) and warnings else ("PASS" if all(checks.values()) else "FAIL")
    return {
        "status": status,
        "tabs_checked": TABS_TO_CHECK,
        "missing_tabs": missing_tabs,
        "row_counts": row_counts,
        "critical_counts": critical_counts,
        "warnings": warnings,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
        "order_item_internal_mode": order_item_internal_mode,
        "order_item_looker_mode": order_item_looker_mode,
        "order_item_internal_manifest_path": str(ORDER_ITEM_REFRESH_MANIFEST_PATH),
        "order_item_looker_manifest_path": str(ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH),
    }


def main() -> None:
    try:
        payload = verify_flipkart_system_health()
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        if payload["status"] not in {"PASS", "PASS_WITH_WARNINGS"}:
            raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
