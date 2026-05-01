from __future__ import annotations

import json
import sys
from collections import Counter
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
    write_csv,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_module_confidence_log.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"
ADS_PLANNER_TAB = "FLIPKART_ADS_PLANNER"
ADS_MASTER_TAB = "FLIPKART_ADS_MASTER"
ADS_MAPPING_ISSUES_TAB = "FLIPKART_ADS_MAPPING_ISSUES"
RETURN_COMMENTS_TAB = "FLIPKART_RETURN_COMMENTS"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
LISTING_PRESENCE_TAB = "FLIPKART_LISTING_PRESENCE"
REPORT_FORMAT_MONITOR_TAB = "FLIPKART_REPORT_FORMAT_MONITOR"
REPORT_FORMAT_ISSUES_TAB = "FLIPKART_REPORT_FORMAT_ISSUES"
RUN_QUALITY_SCORE_TAB = "FLIPKART_RUN_QUALITY_SCORE"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"

MODULE_CONFIDENCE_TAB = "FLIPKART_MODULE_CONFIDENCE"
LOOKER_MODULE_CONFIDENCE_TAB = "LOOKER_FLIPKART_MODULE_CONFIDENCE"
DATA_GAP_SUMMARY_TAB = "FLIPKART_DATA_GAP_SUMMARY"

LOCAL_MODULE_CONFIDENCE_PATH = OUTPUT_DIR / "flipkart_module_confidence.csv"
LOCAL_LOOKER_CONFIDENCE_PATH = OUTPUT_DIR / "looker_flipkart_module_confidence.csv"
LOCAL_DATA_GAP_SUMMARY_PATH = OUTPUT_DIR / "flipkart_data_gap_summary.csv"

MODULE_CONFIDENCE_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
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
    "Last_Updated",
]

LOOKER_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
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
    "Last_Updated",
]

DATA_GAP_SUMMARY_HEADERS = [
    "Run_ID",
    "Data_Gap_Type",
    "FSN_Count",
    "High_Priority_Count",
    "Suggested_Action",
    "Last_Updated",
]

REQUIRED_TABS = [
    SKU_ANALYSIS_TAB,
    COST_MASTER_TAB,
    ADS_PLANNER_TAB,
    ADS_MASTER_TAB,
    ADS_MAPPING_ISSUES_TAB,
    RETURN_COMMENTS_TAB,
    RETURN_ISSUE_SUMMARY_TAB,
    LISTING_PRESENCE_TAB,
    REPORT_FORMAT_MONITOR_TAB,
    REPORT_FORMAT_ISSUES_TAB,
    RUN_QUALITY_SCORE_TAB,
    ALERTS_TAB,
    ACTIVE_TASKS_TAB,
]

LOOKER_OUTPUT_TABS = [LOOKER_MODULE_CONFIDENCE_TAB]
OUTPUT_TABS = [MODULE_CONFIDENCE_TAB, LOOKER_MODULE_CONFIDENCE_TAB, DATA_GAP_SUMMARY_TAB]

PRIMARY_GAP_ORDER = [
    "COGS Missing",
    "Listing Missing",
    "Settlement Missing",
    "PNL Missing",
    "Ads Mapping Weak",
    "Format Issue",
    "High Alert Risk",
    "No Major Gap",
]

SUMMARY_ACTIONS = {
    "COGS Missing": "Fill COGS",
    "Listing Missing": "Check Listing Status",
    "Settlement Missing": "Wait for Settlement / Re-run after settlement cycle",
    "PNL Missing": "Check PNL report",
    "Ads Mapping Weak": "Fix Ads Mapping",
    "Format Issue": "Review Report Format",
    "High Alert Risk": "Resolve Critical Alerts",
    "No Major Gap": "Data Looks Usable",
}

LISTING_TRUE_HINTS = ("found", "present", "active listing", "yes", "active")
LISTING_FALSE_HINTS = ("missing", "not found", "inactive", "rejected", "no")
ADS_NOT_EXPECTED_ACTIONS = {
    "Do Not Run Ads",
    "Do Not Run Ads / Improve Economics",
    "Fix Product First",
    "Fix Product/Listing First",
    "Resolve Critical Alert First",
    "Manual Review",
}
ADS_EXPECTED_ACTIONS = {
    "Test Ads",
    "Always-On Test",
    "Seasonal/Event Test",
    "Scale Ads",
}
SEVERITY_ORDER = ["Critical", "High", "Medium", "Low", "Unknown"]


def latest_non_empty_row(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    for row in reversed(list(rows)):
        if any(normalize_text(value) for value in row.values()):
            return dict(row)
    return {}


def format_count(value: Any) -> str:
    number = parse_float(value)
    if number == 0:
        return "0"
    if float(number).is_integer():
        return str(int(number))
    return str(round(number, 2)).rstrip("0").rstrip(".")


def round_score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def status_from_score(score: float) -> str:
    if score <= 0:
        return "MISSING"
    if score >= 85:
        return "HIGH"
    if score >= 65:
        return "MEDIUM"
    if score >= 40:
        return "LOW"
    return "REVIEW"


def overall_status_from_score(score: float) -> str:
    if score >= 85:
        return "HIGH"
    if score >= 65:
        return "MEDIUM"
    if score >= 40:
        return "LOW"
    return "REVIEW"


def score_text(score: float) -> str:
    if float(score).is_integer():
        return str(int(score))
    return str(round(score, 2)).rstrip("0").rstrip(".")


def canonical_severity(value: Any) -> str:
    text = normalize_text(value).strip()
    if not text:
        return "Unknown"
    return text.title()


def first_nonblank(row: Dict[str, Any], *field_names: str) -> str:
    for field_name in field_names:
        value = normalize_text(row.get(field_name, ""))
        if value:
            return value
    return ""


def first_numeric(row: Dict[str, Any], *field_names: str) -> float:
    for field_name in field_names:
        value = normalize_text(row.get(field_name, ""))
        if value:
            return parse_float(value)
    return 0.0


def row_has_truthy_listing(row: Dict[str, Any]) -> bool:
    found_value = normalize_text(row.get("Found_In_Active_Listing", "")).lower()
    status_value = normalize_text(row.get("Listing_Presence_Status", "")).lower()
    if found_value in {"yes", "true", "found", "present", "active"}:
        return True
    return any(hint in status_value for hint in LISTING_TRUE_HINTS) and not any(hint in status_value for hint in LISTING_FALSE_HINTS)


def row_has_missing_listing(row: Dict[str, Any]) -> bool:
    found_value = normalize_text(row.get("Found_In_Active_Listing", "")).lower()
    status_value = normalize_text(row.get("Listing_Presence_Status", "")).lower()
    if found_value in {"no", "false", "missing"}:
        return True
    return any(hint in status_value for hint in LISTING_FALSE_HINTS)


def tab_exists_or_raise(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")


def read_rows(sheets_service, spreadsheet_id: str, tab_name: str) -> List[Dict[str, Any]]:
    _, rows = read_table(sheets_service, spreadsheet_id, tab_name)
    return rows


def read_optional_rows(sheets_service, spreadsheet_id: str, tab_name: str) -> List[Dict[str, Any]]:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return []
    return read_rows(sheets_service, spreadsheet_id, tab_name)


def filter_rows_for_run(rows: Sequence[Dict[str, Any]], run_id: str) -> List[Dict[str, Any]]:
    if not run_id:
        return [dict(row) for row in rows]
    candidate_fields = [field for field in ("Run_ID", "Latest_Run_ID", "First_Seen_Run_ID") if any(normalize_text(row.get(field, "")) for row in rows)]
    if not candidate_fields:
        return [dict(row) for row in rows]
    matched = [
        dict(row)
        for row in rows
        if any(normalize_text(row.get(field, "")) == run_id for field in candidate_fields)
    ]
    return matched or [dict(row) for row in rows]


def index_last_row_by_fsn(rows: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        if fsn not in indexed:
            order.append(fsn)
        indexed[fsn] = dict(row)
    return indexed, order


def group_rows_by_fsn(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        grouped.setdefault(fsn, []).append(dict(row))
    return grouped


def latest_non_blank_fsn_count(rows: Sequence[Dict[str, Any]]) -> int:
    return len({clean_fsn(row.get("FSN", "")) for row in rows if clean_fsn(row.get("FSN", ""))})


def update_sheet_tab(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))
    add_basic_filter(sheets_service, spreadsheet_id, sheet_id, len(headers), len(rows) + 1)


def update_source_tab(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def listing_confidence(listing_rows: Sequence[Dict[str, Any]]) -> Tuple[float, str, str]:
    row = listing_rows[-1] if listing_rows else {}
    status_text = normalize_text(row.get("Listing_Presence_Status", ""))
    found = row_has_truthy_listing(row)
    missing = row_has_missing_listing(row)
    if found:
        score = 100.0
        reason = f"Listing presence status={status_text or 'Found In Active Listing'}."
    elif missing:
        score = 40.0
        reason = f"Listing presence status={status_text or 'Missing From Active Listing File'}."
    elif row:
        score = 60.0
        reason = f"Listing presence status={status_text or 'Unknown'}."
    else:
        score = 60.0
        reason = "Listing presence status unknown."
    return score, status_from_score(score), reason


def order_confidence(analysis_row: Dict[str, Any], listing_score: float, listing_present: bool) -> Tuple[float, str, str]:
    orders = first_numeric(analysis_row, "Orders")
    units_sold = first_numeric(analysis_row, "Units_Sold", "Total_Units_Sold")
    if orders > 0 or units_sold > 0:
        score = 100.0
        reason = f"Orders={format_count(orders)} and Units_Sold={format_count(units_sold)}."
    elif listing_present:
        score = 70.0
        reason = "No orders yet, but the listing exists."
    else:
        score = 40.0
        reason = "No orders and listing is missing."
    return score, status_from_score(score), reason


def return_confidence(
    analysis_row: Dict[str, Any],
    return_summary_rows: Sequence[Dict[str, Any]],
    return_comment_rows: Sequence[Dict[str, Any]],
) -> Tuple[float, str, str]:
    orders = first_numeric(analysis_row, "Orders")
    returns = first_numeric(analysis_row, "Customer_Return_Count") or first_numeric(analysis_row, "Returns")
    return_rate = first_numeric(analysis_row, "Customer_Return_Rate") or first_numeric(analysis_row, "Return_Rate")
    summary_count = len(return_summary_rows)
    comment_count = len(return_comment_rows)
    mapped_rows = summary_count + comment_count
    if mapped_rows > 0:
        score = 100.0
        reason = f"Return issue summary rows={summary_count} and mapped comments={comment_count}."
    elif (returns > 0 or return_rate > 0) and orders > 0:
        score = 40.0
        reason = f"Customer returns expected but not mapped (Customer_Return_Count={format_count(returns)}, Customer_Return_Rate={return_rate:.2f})."
    elif orders > 0:
        score = 80.0
        reason = f"No returns mapped yet and orders exist (Orders={format_count(orders)})."
    else:
        score = 60.0
        reason = "No order data available for return confidence."
    return score, status_from_score(score), reason


def settlement_confidence(analysis_row: Dict[str, Any]) -> Tuple[float, str, str]:
    orders = first_numeric(analysis_row, "Orders")
    settlement_value = first_nonblank(analysis_row, "Net_Settlement", "Settlement_Amount", "Amount_Settled", "Flipkart_Net_Earnings")
    pnl_value = first_nonblank(analysis_row, "Net_Profit_Before_COGS", "Final_Net_Profit", "Flipkart_Net_Earnings")
    if settlement_value:
        score = 100.0
        reason = f"Settlement value present ({settlement_value})."
    elif orders > 0 and pnl_value:
        score = 70.0
        reason = "Orders exist and downstream financial data is present, so settlement may be timing lag."
    elif orders > 0:
        score = 40.0
        reason = "Orders exist but settlement is missing."
    else:
        score = 60.0
        reason = "No order data, so settlement confidence is neutral."
    return score, status_from_score(score), reason


def pnl_confidence(analysis_row: Dict[str, Any]) -> Tuple[float, str, str]:
    orders = first_numeric(analysis_row, "Orders")
    settlement_value = first_nonblank(analysis_row, "Net_Settlement", "Settlement_Amount", "Amount_Settled", "Flipkart_Net_Earnings")
    pnl_value = first_nonblank(analysis_row, "Flipkart_Net_Earnings", "Net_Profit_Before_COGS", "Final_Net_Profit")
    if pnl_value:
        score = 100.0
        reason = f"PNL value present ({pnl_value})."
    elif orders > 0 and settlement_value:
        score = 70.0
        reason = "Orders exist and settlement is present, so PNL may still be catching up."
    elif orders > 0:
        score = 40.0
        reason = "Orders exist but PNL is missing."
    else:
        score = 60.0
        reason = "No order data, so PNL confidence is neutral."
    return score, status_from_score(score), reason


def cogs_confidence(analysis_row: Dict[str, Any], cost_row: Dict[str, Any]) -> Tuple[float, str, str]:
    cogs_status = first_nonblank(analysis_row, "COGS_Status") or first_nonblank(cost_row, "COGS_Status")
    cogs_status_norm = normalize_text(cogs_status)
    if cogs_status_norm == "Verified":
        score = 100.0
        reason = "COGS_Status=Verified."
    elif cogs_status_norm == "Entered":
        score = 85.0
        reason = "COGS_Status=Entered."
    elif cogs_status_norm == "Needs Review":
        score = 50.0
        reason = "COGS_Status=Needs Review."
    else:
        score = 0.0
        reason = "COGS status is Missing or blank."
    return score, status_from_score(score), reason


def ads_confidence(
    fsn: str,
    planner_row: Dict[str, Any],
    ads_master_rows: Sequence[Dict[str, Any]],
    ads_issue_rows: Sequence[Dict[str, Any]],
) -> Tuple[float, str, str]:
    ads_master_count = len(ads_master_rows)
    ads_issue_count = len(ads_issue_rows)
    planner_action = normalize_text(planner_row.get("Suggested_Ad_Action", ""))
    planner_readiness = normalize_text(planner_row.get("Ads_Readiness_Status", ""))
    expected_ads = planner_action in ADS_EXPECTED_ACTIONS or planner_readiness == "Ready"
    not_expected = planner_action in ADS_NOT_EXPECTED_ACTIONS or planner_readiness in {"Review", "Not Ready"}

    mapped_ads_rows = [row for row in ads_master_rows if clean_fsn(row.get("FSN", "")) == fsn]
    issue_count_for_fsn = sum(1 for row in ads_issue_rows if clean_fsn(row.get("FSN", "")) == fsn)

    if mapped_ads_rows and issue_count_for_fsn == 0:
        score = 100.0
        reason = f"Ads mapped rows={len(mapped_ads_rows)} with no mapping issues."
    elif issue_count_for_fsn > 0 and mapped_ads_rows:
        score = 50.0
        reason = f"Ads mapping issues exist for this FSN (issue rows={issue_count_for_fsn})."
    elif issue_count_for_fsn > 0:
        score = 40.0
        reason = f"Ads report exists but FSN is not mapped (issue rows={issue_count_for_fsn})."
    elif not_expected:
        score = 70.0
        reason = "No ads data yet, but ads are not expected for this FSN."
    elif expected_ads:
        score = 40.0
        reason = "Ads are expected, but no mapped data is present."
    else:
        score = 70.0
        reason = "No ads data yet, and the expectation is unclear."
    return score, status_from_score(score), reason


def format_confidence(monitor_rows: Sequence[Dict[str, Any]], issue_rows: Sequence[Dict[str, Any]]) -> Tuple[float, str, str]:
    severity_counts = Counter()
    for row in list(monitor_rows) + list(issue_rows):
        severity = canonical_severity(row.get("Severity", ""))
        severity_counts[severity] += 1
    critical = severity_counts.get("Critical", 0)
    high = severity_counts.get("High", 0)
    medium = severity_counts.get("Medium", 0)
    low = severity_counts.get("Low", 0)
    if critical > 0 or high > 0:
        score = 30.0
        reason = f"Report format drift has critical/high issues (Critical={critical}, High={high})."
    elif medium > 0 or low > 0:
        score = 70.0
        reason = f"Report format drift has medium/minor warnings (Medium={medium}, Low={low})."
    else:
        score = 100.0
        reason = "No critical or high report format issues found."
    return score, status_from_score(score), reason


def alert_risk_confidence(alert_rows: Sequence[Dict[str, Any]], active_rows: Sequence[Dict[str, Any]]) -> Tuple[float, str, str]:
    severity_counts = Counter()
    for row in list(alert_rows) + list(active_rows):
        severity = canonical_severity(row.get("Severity", ""))
        severity_counts[severity] += 1
    critical = severity_counts.get("Critical", 0)
    high = severity_counts.get("High", 0)
    medium = severity_counts.get("Medium", 0)
    low = severity_counts.get("Low", 0)
    if critical > 0:
        score = 20.0
        reason = f"Critical alert risk is active (Critical={critical}, High={high}, Medium={medium}, Low={low})."
    elif high > 0:
        score = 40.0
        reason = f"High alert risk is active (Critical={critical}, High={high}, Medium={medium}, Low={low})."
    elif medium > 0 or low > 0:
        score = 70.0
        reason = f"Only low/medium alerts are present (Critical={critical}, High={high}, Medium={medium}, Low={low})."
    else:
        score = 100.0
        reason = "No active alerts found."
    return score, status_from_score(score), reason


def choose_primary_gap(
    cogs_score: float,
    listing_score: float,
    settlement_score: float,
    pnl_score: float,
    ads_score: float,
    format_score_value: float,
    alert_score: float,
    ads_issue_rows: Sequence[Dict[str, Any]],
    format_rows: Sequence[Dict[str, Any]],
) -> str:
    format_issue_present = any(canonical_severity(row.get("Severity", "")) in {"Critical", "High"} for row in list(format_rows))
    ads_weak = any(True for _ in ads_issue_rows)
    for candidate in PRIMARY_GAP_ORDER:
        if candidate == "COGS Missing" and cogs_score <= 0:
            return candidate
        if candidate == "Listing Missing" and listing_score <= 40:
            return candidate
        if candidate == "Settlement Missing" and settlement_score <= 40:
            return candidate
        if candidate == "PNL Missing" and pnl_score <= 40:
            return candidate
        if candidate == "Ads Mapping Weak" and (ads_score <= 50 or ads_weak):
            return candidate
        if candidate == "Format Issue" and (format_score_value <= 70 or format_issue_present):
            return candidate
        if candidate == "High Alert Risk" and alert_score <= 40:
            return candidate
    return "No Major Gap"


def build_summary_action(primary_gap: str) -> str:
    return SUMMARY_ACTIONS.get(primary_gap, "Data Looks Usable")


def build_module_rows(
    run_id: str,
    analysis_rows: Sequence[Dict[str, Any]],
    cost_rows: Sequence[Dict[str, Any]],
    ads_planner_rows: Sequence[Dict[str, Any]],
    ads_master_rows: Sequence[Dict[str, Any]],
    ads_issue_rows: Sequence[Dict[str, Any]],
    return_comment_rows: Sequence[Dict[str, Any]],
    return_summary_rows: Sequence[Dict[str, Any]],
    listing_rows: Sequence[Dict[str, Any]],
    format_monitor_rows: Sequence[Dict[str, Any]],
    format_issue_rows: Sequence[Dict[str, Any]],
    alert_rows: Sequence[Dict[str, Any]],
    active_rows: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    analysis_rows = filter_rows_for_run(analysis_rows, run_id)
    ads_planner_rows = filter_rows_for_run(ads_planner_rows, run_id)
    ads_master_rows = filter_rows_for_run(ads_master_rows, run_id)
    ads_issue_rows = filter_rows_for_run(ads_issue_rows, run_id)
    return_comment_rows = filter_rows_for_run(return_comment_rows, run_id)
    return_summary_rows = filter_rows_for_run(return_summary_rows, run_id)
    alert_rows = filter_rows_for_run(alert_rows, run_id)
    active_rows = filter_rows_for_run(active_rows, run_id)

    analysis_index, analysis_order = index_last_row_by_fsn(analysis_rows)
    cost_index, _ = index_last_row_by_fsn(cost_rows)
    planner_index, _ = index_last_row_by_fsn(ads_planner_rows)
    listing_index, _ = index_last_row_by_fsn(listing_rows)
    return_comment_index = group_rows_by_fsn(return_comment_rows)
    return_summary_index = group_rows_by_fsn(return_summary_rows)
    ads_master_index = group_rows_by_fsn(ads_master_rows)
    ads_issue_index = group_rows_by_fsn(ads_issue_rows)
    alert_index = group_rows_by_fsn(alert_rows)
    active_index = group_rows_by_fsn(active_rows)

    format_score_value, format_status, format_reason = format_confidence(format_monitor_rows, format_issue_rows)
    alert_score_value, alert_status, alert_reason = alert_risk_confidence(alert_rows, active_rows)
    format_score_value = round_score(format_score_value)
    alert_score_value = round_score(alert_score_value)

    module_rows: List[Dict[str, Any]] = []
    looker_rows: List[Dict[str, Any]] = []
    confidence_breakdown: List[Dict[str, Any]] = []

    for fsn in analysis_order:
        analysis_row = analysis_index.get(fsn, {})
        cost_row = cost_index.get(fsn, {})
        planner_row = planner_index.get(fsn, {})
        listing_row = listing_index.get(fsn, {})
        return_comment_rows_for_fsn = return_comment_index.get(fsn, [])
        return_summary_rows_for_fsn = return_summary_index.get(fsn, [])
        ads_master_rows_for_fsn = ads_master_index.get(fsn, [])
        ads_issue_rows_for_fsn = ads_issue_index.get(fsn, [])
        alert_rows_for_fsn = alert_index.get(fsn, [])
        active_rows_for_fsn = active_index.get(fsn, [])

        sku_id = first_nonblank(analysis_row, "SKU_ID", "Seller_SKU")
        product_title = first_nonblank(analysis_row, "Product_Title", "Title")
        listing_score, listing_status, listing_reason = listing_confidence([listing_row] if listing_row else [])
        listing_present = bool(listing_row)
        order_score, order_status, order_reason = order_confidence(analysis_row, listing_score, listing_present)
        return_score, return_status, return_reason = return_confidence(analysis_row, return_summary_rows_for_fsn, return_comment_rows_for_fsn)
        settlement_score, settlement_status, settlement_reason = settlement_confidence(analysis_row)
        pnl_score, pnl_status, pnl_reason = pnl_confidence(analysis_row)
        cogs_score, cogs_status, cogs_reason = cogs_confidence(analysis_row, cost_row)
        ads_score, ads_status, ads_reason = ads_confidence(fsn, planner_row, ads_master_rows_for_fsn, ads_issue_rows_for_fsn)
        if alert_rows_for_fsn or active_rows_for_fsn:
            alert_score_for_fsn, alert_status_for_fsn, alert_reason_for_fsn = alert_risk_confidence(
                alert_rows_for_fsn,
                active_rows_for_fsn,
            )
        else:
            alert_score_for_fsn = 100.0
            alert_status_for_fsn = status_from_score(alert_score_for_fsn)
            alert_reason_for_fsn = "No active alerts found."
        primary_gap = choose_primary_gap(
            cogs_score,
            listing_score,
            settlement_score,
            pnl_score,
            ads_score,
            format_score_value,
            alert_score_for_fsn,
            ads_issue_rows_for_fsn,
            format_issue_rows,
        )
        suggested_action = build_summary_action(primary_gap)
        module_scores = {
            "Listing": listing_score,
            "Order": order_score,
            "Return": return_score,
            "Settlement": settlement_score,
            "PNL": pnl_score,
            "COGS": cogs_score,
            "Ads": ads_score,
            "Format": format_score_value,
            "Alert": alert_score_for_fsn,
        }
        overall_score = round_score(
            (
                module_scores["Listing"] * 0.10
                + module_scores["Order"] * 0.10
                + module_scores["Return"] * 0.10
                + module_scores["Settlement"] * 0.15
                + module_scores["PNL"] * 0.15
                + module_scores["COGS"] * 0.15
                + module_scores["Ads"] * 0.10
                + module_scores["Format"] * 0.10
                + module_scores["Alert"] * 0.05
            )
        )
        overall_status = overall_status_from_score(overall_score)
        overall_reason = f"Weighted average across 9 modules; primary gap={primary_gap}."

        module_row = {
            "Run_ID": run_id,
            "FSN": fsn,
            "SKU_ID": sku_id,
            "Product_Title": product_title,
            "Listing_Confidence_Score": listing_score,
            "Listing_Confidence_Status": listing_status,
            "Listing_Confidence_Reason": listing_reason,
            "Order_Confidence_Score": order_score,
            "Order_Confidence_Status": order_status,
            "Order_Confidence_Reason": order_reason,
            "Return_Confidence_Score": return_score,
            "Return_Confidence_Status": return_status,
            "Return_Confidence_Reason": return_reason,
            "Settlement_Confidence_Score": settlement_score,
            "Settlement_Confidence_Status": settlement_status,
            "Settlement_Confidence_Reason": settlement_reason,
            "PNL_Confidence_Score": pnl_score,
            "PNL_Confidence_Status": pnl_status,
            "PNL_Confidence_Reason": pnl_reason,
            "COGS_Confidence_Score": cogs_score,
            "COGS_Confidence_Status": cogs_status,
            "COGS_Confidence_Reason": cogs_reason,
            "Ads_Confidence_Score": ads_score,
            "Ads_Confidence_Status": ads_status,
            "Ads_Confidence_Reason": ads_reason,
            "Format_Confidence_Score": format_score_value,
            "Format_Confidence_Status": format_status,
            "Format_Confidence_Reason": format_reason,
            "Alert_Risk_Score": alert_score_for_fsn,
            "Alert_Risk_Status": alert_status_for_fsn,
            "Alert_Risk_Reason": alert_reason_for_fsn,
            "Overall_Confidence_Score": overall_score,
            "Overall_Confidence_Status": overall_status,
            "Overall_Confidence_Reason": overall_reason,
            "Primary_Data_Gap": primary_gap,
            "Suggested_Data_Action": suggested_action,
            "Last_Updated": now_iso(),
        }
        module_rows.append(module_row)
        looker_rows.append(
            {
                "Run_ID": run_id,
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": product_title,
                "Overall_Confidence_Score": overall_score,
                "Overall_Confidence_Status": overall_status,
                "Primary_Data_Gap": primary_gap,
                "Suggested_Data_Action": suggested_action,
                "Listing_Confidence_Status": listing_status,
                "Order_Confidence_Status": order_status,
                "Return_Confidence_Status": return_status,
                "Settlement_Confidence_Status": settlement_status,
                "PNL_Confidence_Status": pnl_status,
                "COGS_Confidence_Status": cogs_status,
                "Ads_Confidence_Status": ads_status,
                "Format_Confidence_Status": format_status,
                "Alert_Risk_Status": alert_status_for_fsn,
                "Last_Updated": module_row["Last_Updated"],
            }
        )

        confidence_breakdown.append(
            {
                "Run_ID": run_id,
                "FSN": fsn,
                "Primary_Data_Gap": primary_gap,
                "Overall_Confidence_Status": overall_status,
                "Overall_Confidence_Score": overall_score,
            }
        )

    gap_counter = Counter(row["Primary_Data_Gap"] for row in module_rows)
    gap_rows: List[Dict[str, Any]] = []
    high_priority_gaps = {"COGS Missing", "Listing Missing", "Settlement Missing", "PNL Missing", "Ads Mapping Weak", "Format Issue", "High Alert Risk"}
    for gap in PRIMARY_GAP_ORDER:
        fsn_count = gap_counter.get(gap, 0)
        if gap == "No Major Gap":
            high_priority_count = 0
        else:
            high_priority_count = sum(
                1
                for row in module_rows
                if row["Primary_Data_Gap"] == gap and row["Overall_Confidence_Status"] in {"LOW", "REVIEW", "MISSING"}
            )
        gap_rows.append(
            {
                "Run_ID": run_id,
                "Data_Gap_Type": gap,
                "FSN_Count": fsn_count,
                "High_Priority_Count": high_priority_count,
                "Suggested_Action": build_summary_action(gap),
                "Last_Updated": now_iso(),
            }
        )

    return module_rows, looker_rows, gap_rows, {"format_score": format_score_value, "alert_score": alert_score_value}, confidence_breakdown


def merge_sku_analysis_rows(
    analysis_rows: Sequence[Dict[str, Any]],
    module_rows: Sequence[Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]]]:
    module_by_fsn = {row["FSN"]: row for row in module_rows}
    headers: List[str] = list(analysis_rows[0].keys()) if analysis_rows else []
    for column in [
        "Overall_Confidence_Score",
        "Overall_Confidence_Status",
        "Primary_Data_Gap",
        "Suggested_Data_Action",
    ]:
        if column not in headers:
            headers.append(column)

    updated_rows: List[Dict[str, Any]] = []
    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        merged = dict(row)
        if fsn and fsn in module_by_fsn:
            confidence_row = module_by_fsn[fsn]
            merged["Overall_Confidence_Score"] = confidence_row["Overall_Confidence_Score"]
            merged["Overall_Confidence_Status"] = confidence_row["Overall_Confidence_Status"]
            merged["Primary_Data_Gap"] = confidence_row["Primary_Data_Gap"]
            merged["Suggested_Data_Action"] = confidence_row["Suggested_Data_Action"]
        else:
            merged.setdefault("Overall_Confidence_Score", "")
            merged.setdefault("Overall_Confidence_Status", "")
            merged.setdefault("Primary_Data_Gap", "")
            merged.setdefault("Suggested_Data_Action", "")
        updated_rows.append(merged)
    return headers, updated_rows


def ordered_distribution(rows: Sequence[Dict[str, Any]], field_name: str, preferred_order: Sequence[str]) -> Dict[str, int]:
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


def create_flipkart_module_confidence() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in REQUIRED_TABS:
        tab_exists_or_raise(sheets_service, spreadsheet_id, tab_name)

    sku_analysis_rows = read_rows(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    cost_rows = read_optional_rows(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    ads_planner_rows = read_optional_rows(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)
    ads_master_rows = read_optional_rows(sheets_service, spreadsheet_id, ADS_MASTER_TAB)
    ads_issue_rows = read_optional_rows(sheets_service, spreadsheet_id, ADS_MAPPING_ISSUES_TAB)
    return_comment_rows = read_optional_rows(sheets_service, spreadsheet_id, RETURN_COMMENTS_TAB)
    return_summary_rows = read_optional_rows(sheets_service, spreadsheet_id, RETURN_ISSUE_SUMMARY_TAB)
    listing_rows = read_optional_rows(sheets_service, spreadsheet_id, LISTING_PRESENCE_TAB)
    format_monitor_rows = read_optional_rows(sheets_service, spreadsheet_id, REPORT_FORMAT_MONITOR_TAB)
    format_issue_rows = read_optional_rows(sheets_service, spreadsheet_id, REPORT_FORMAT_ISSUES_TAB)
    run_quality_rows = read_optional_rows(sheets_service, spreadsheet_id, RUN_QUALITY_SCORE_TAB)
    alert_rows = read_optional_rows(sheets_service, spreadsheet_id, ALERTS_TAB)
    active_rows = read_optional_rows(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)

    if not sku_analysis_rows:
        raise RuntimeError(f"No rows found in {SKU_ANALYSIS_TAB}")

    run_id = (
        first_nonblank(latest_non_empty_row(run_quality_rows), "Run_ID")
        or first_nonblank(latest_non_empty_row(sku_analysis_rows), "Run_ID")
        or first_nonblank(latest_non_empty_row(ads_planner_rows), "Run_ID")
        or first_nonblank(latest_non_empty_row(alert_rows), "Run_ID")
        or f"FLIPKART_{now_iso().replace(':', '').replace('-', '').replace('T', '_')}"
    )

    module_rows, looker_rows, gap_rows, extra_scores, breakdown_rows = build_module_rows(
        run_id,
        sku_analysis_rows,
        cost_rows,
        ads_planner_rows,
        ads_master_rows,
        ads_issue_rows,
        return_comment_rows,
        return_summary_rows,
        listing_rows,
        format_monitor_rows,
        format_issue_rows,
        alert_rows,
        active_rows,
    )

    summary_row = latest_non_empty_row(module_rows)
    overall_distribution = ordered_distribution(module_rows, "Overall_Confidence_Status", ["HIGH", "MEDIUM", "LOW", "MISSING", "REVIEW"])
    primary_gap_distribution = ordered_distribution(module_rows, "Primary_Data_Gap", PRIMARY_GAP_ORDER)
    average_overall_confidence = round_score(
        sum(parse_float(row.get("Overall_Confidence_Score", 0)) for row in module_rows) / len(module_rows)
    )
    low_confidence_count = sum(1 for row in module_rows if normalize_text(row.get("Overall_Confidence_Status", "")) in {"LOW", "MISSING"})
    review_count = sum(1 for row in module_rows if normalize_text(row.get("Overall_Confidence_Status", "")) == "REVIEW")

    module_lookup = {row["FSN"]: row for row in module_rows}
    source_headers, updated_sku_rows = merge_sku_analysis_rows(sku_analysis_rows, module_rows)

    write_csv(LOCAL_MODULE_CONFIDENCE_PATH, MODULE_CONFIDENCE_HEADERS, module_rows)
    write_csv(LOCAL_LOOKER_CONFIDENCE_PATH, LOOKER_HEADERS, looker_rows)
    write_csv(LOCAL_DATA_GAP_SUMMARY_PATH, DATA_GAP_SUMMARY_HEADERS, gap_rows)

    update_sheet_tab(sheets_service, spreadsheet_id, MODULE_CONFIDENCE_TAB, MODULE_CONFIDENCE_HEADERS, module_rows)
    update_sheet_tab(sheets_service, spreadsheet_id, LOOKER_MODULE_CONFIDENCE_TAB, LOOKER_HEADERS, looker_rows)
    update_sheet_tab(sheets_service, spreadsheet_id, DATA_GAP_SUMMARY_TAB, DATA_GAP_SUMMARY_HEADERS, gap_rows)

    merged_headers = source_headers
    update_source_tab(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB, merged_headers, updated_sku_rows)

    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "run_id",
            "fsn_count",
            "overall_confidence_distribution",
            "primary_data_gap_distribution",
            "average_overall_confidence",
            "low_confidence_count",
            "review_count",
            "tabs_updated",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "spreadsheet_id": spreadsheet_id,
                "run_id": run_id,
                "fsn_count": len(module_rows),
                "overall_confidence_distribution": json.dumps(overall_distribution, ensure_ascii=False),
                "primary_data_gap_distribution": json.dumps(primary_gap_distribution, ensure_ascii=False),
                "average_overall_confidence": average_overall_confidence,
                "low_confidence_count": low_confidence_count,
                "review_count": review_count,
                "tabs_updated": " | ".join([MODULE_CONFIDENCE_TAB, LOOKER_MODULE_CONFIDENCE_TAB, DATA_GAP_SUMMARY_TAB, SKU_ANALYSIS_TAB]),
                "status": "SUCCESS",
                "message": "Created Flipkart module confidence tabs",
            }
        ],
    )

    result = {
        "status": "SUCCESS",
        "run_id": run_id,
        "fsn_count": len(module_rows),
        "overall_confidence_distribution": overall_distribution,
        "primary_data_gap_distribution": primary_gap_distribution,
        "average_overall_confidence": average_overall_confidence,
        "low_confidence_count": low_confidence_count,
        "review_count": review_count,
        "tabs_updated": [MODULE_CONFIDENCE_TAB, LOOKER_MODULE_CONFIDENCE_TAB, DATA_GAP_SUMMARY_TAB, SKU_ANALYSIS_TAB],
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_module_confidence()
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
