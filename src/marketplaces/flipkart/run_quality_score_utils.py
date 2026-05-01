from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from src.marketplaces.flipkart.flipkart_cogs_helpers import count_cogs_rows
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text, now_iso, parse_float

RUN_HISTORY_TAB = "FLIPKART_RUN_HISTORY"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"
ADS_MASTER_TAB = "FLIPKART_ADS_MASTER"
ADS_MAPPING_ISSUES_TAB = "FLIPKART_ADS_MAPPING_ISSUES"
RETURN_COMMENTS_TAB = "FLIPKART_RETURN_COMMENTS"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
LISTING_PRESENCE_TAB = "FLIPKART_LISTING_PRESENCE"
REPORT_FORMAT_MONITOR_TAB = "FLIPKART_REPORT_FORMAT_MONITOR"
REPORT_FORMAT_ISSUES_TAB = "FLIPKART_REPORT_FORMAT_ISSUES"
RUN_COMPARISON_TAB = "FLIPKART_RUN_COMPARISON"

RUN_QUALITY_SCORE_TAB = "FLIPKART_RUN_QUALITY_SCORE"
RUN_QUALITY_BREAKDOWN_TAB = "FLIPKART_RUN_QUALITY_BREAKDOWN"
LOOKER_RUN_QUALITY_TAB = "LOOKER_FLIPKART_RUN_QUALITY_SCORE"

RUN_QUALITY_SCORE_HEADERS = [
    "Run_ID",
    "Report_Date",
    "Overall_Run_Quality_Score",
    "Run_Quality_Grade",
    "Decision_Recommendation",
    "Reports_Score",
    "Mapping_Score",
    "COGS_Score",
    "Ads_Score",
    "Returns_Score",
    "Listing_Score",
    "Format_Stability_Score",
    "Alert_Risk_Score",
    "Data_Confidence_Score",
    "Major_Warnings",
    "Critical_Warnings",
    "Suggested_Action",
    "Last_Updated",
]

RUN_QUALITY_BREAKDOWN_HEADERS = [
    "Run_ID",
    "Score_Category",
    "Score_Name",
    "Max_Points",
    "Points_Earned",
    "Score_Percent",
    "Status",
    "Reason",
    "Suggested_Action",
    "Last_Updated",
]

LOOKER_RUN_QUALITY_HEADERS = [
    "Report_Date",
    "Run_ID",
    "Overall_Run_Quality_Score",
    "Run_Quality_Grade",
    "Decision_Recommendation",
    "Score_Category",
    "Score_Name",
    "Max_Points",
    "Points_Earned",
    "Status",
    "Reason",
    "Suggested_Action",
    "Last_Updated",
]

CATEGORY_ORDER = [
    "Reports/Format",
    "Mapping Coverage",
    "COGS",
    "Ads",
    "Returns",
    "Listings",
    "Alerts",
    "Data Confidence",
]

CATEGORY_MAX_POINTS = {
    "Reports/Format": 15.0,
    "Mapping Coverage": 20.0,
    "COGS": 15.0,
    "Ads": 10.0,
    "Returns": 10.0,
    "Listings": 10.0,
    "Alerts": 10.0,
    "Data Confidence": 10.0,
}

TRUTHY_VALUES = {"1", "true", "yes", "y", "found", "present", "active", "found in active listing"}
NEGATIVE_TASK_STATUSES = {"done", "closed", "resolved", "complete", "completed", "fixed"}


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def format_score(value: float, decimals: int = 2) -> str:
    if float(value).is_integer():
        return str(int(round(value)))
    return f"{value:.{decimals}f}".rstrip("0").rstrip(".")


def clean_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [row for row in rows if any(normalize_text(value) for value in row.values())]


def latest_non_empty_row(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    rows = clean_rows(rows)
    return dict(rows[-1]) if rows else {}


def first_non_blank(row: Dict[str, Any], *field_names: str, default: str = "") -> str:
    for field_name in field_names:
        value = normalize_text(row.get(field_name, ""))
        if value:
            return value
    return default


def first_numeric(row: Dict[str, Any], *field_names: str, default: float = 0.0) -> float:
    for field_name in field_names:
        value = normalize_text(row.get(field_name, ""))
        if value:
            return parse_float(value)
    return default


def is_truthy(value: Any) -> bool:
    return normalize_text(value).strip().lower() in TRUTHY_VALUES


def rows_for_run(
    rows: Sequence[Dict[str, Any]],
    run_id: str,
    candidate_fields: Sequence[str],
) -> List[Dict[str, Any]]:
    cleaned = clean_rows(rows)
    if not cleaned or not run_id:
        return cleaned

    fields_present = [field for field in candidate_fields if any(normalize_text(row.get(field, "")) for row in cleaned)]
    if not fields_present:
        return cleaned

    matched = [
        row
        for row in cleaned
        if any(normalize_text(row.get(field, "")) == run_id for field in fields_present)
    ]
    return matched


def count_unique_fsns(rows: Sequence[Dict[str, Any]]) -> int:
    return len({clean_fsn(row.get("FSN", "")) for row in rows if clean_fsn(row.get("FSN", ""))})


def count_truthy_listing_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if is_truthy(row.get("Found_In_Active_Listing", ""))
        or (
            (
                "found" in normalize_text(row.get("Listing_Presence_Status", "")).lower()
                or "present" in normalize_text(row.get("Listing_Presence_Status", "")).lower()
                or "active listing" in normalize_text(row.get("Listing_Presence_Status", "")).lower()
            )
            and "not found" not in normalize_text(row.get("Listing_Presence_Status", "")).lower()
            and "inactive" not in normalize_text(row.get("Listing_Presence_Status", "")).lower()
            and "missing" not in normalize_text(row.get("Listing_Presence_Status", "")).lower()
        )
    )


def count_return_fsns(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if parse_float(row.get("Customer_Return_Count", row.get("Returns", ""))) > 0
        or parse_float(row.get("Customer_Return_Rate", row.get("Return_Rate", ""))) > 0
    )


def score_from_ratio(value: float, max_points: float) -> float:
    return clamp(max_points * clamp(value, 0.0, 1.0), 0.0, max_points)


def grade_from_score(score: float) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Usable With Warnings"
    if score >= 40:
        return "Weak"
    return "Do Not Trust"


def recommendation_from_score(score: float) -> str:
    if score >= 90:
        return "Safe For Business Review"
    if score >= 75:
        return "Usable But Review Warnings"
    if score >= 60:
        return "Use Carefully"
    return "Do Not Make Major Decisions"


def category_status(points: float, max_points: float, has_data: bool = True, warning: bool = False) -> str:
    if not has_data:
        return "Missing"
    if warning:
        return "Warning"
    ratio = (points / max_points) if max_points else 0.0
    if ratio >= 0.8:
        return "Good"
    if ratio >= 0.5:
        return "Warning"
    return "Critical"


def format_warning_text(warnings: Sequence[str]) -> str:
    deduped: List[str] = []
    seen = set()
    for warning in warnings:
        warning_text = normalize_text(warning)
        if warning_text and warning_text not in seen:
            deduped.append(warning_text)
            seen.add(warning_text)
    return " | ".join(deduped)


def make_breakdown_row(
    run_id: str,
    category: str,
    score_name: str,
    max_points: float,
    points_earned: float,
    status: str,
    reason: str,
    suggested_action: str,
    timestamp: str,
) -> Dict[str, Any]:
    score_percent = 0.0 if max_points <= 0 else (points_earned / max_points) * 100.0
    return {
        "Run_ID": run_id,
        "Score_Category": category,
        "Score_Name": score_name,
        "Max_Points": format_score(max_points),
        "Points_Earned": format_score(points_earned),
        "Score_Percent": format_score(score_percent),
        "Status": status,
        "Reason": reason,
        "Suggested_Action": suggested_action,
        "Last_Updated": timestamp,
    }


def _report_format_result(
    report_format_monitor_rows: Sequence[Dict[str, Any]],
    report_format_issue_rows: Sequence[Dict[str, Any]],
    run_comparison_rows: Sequence[Dict[str, Any]],
    run_id: str,
    timestamp: str,
) -> Tuple[Dict[str, Any], str]:
    active_rows = clean_rows(report_format_issue_rows) or clean_rows(report_format_monitor_rows)
    comparison_rows = rows_for_run(run_comparison_rows, run_id, ("Latest_Run_ID", "Run_ID"))

    if not active_rows and not comparison_rows:
        points = 5.0
        reason = "No report format monitor or issue rows were found."
        status = "Warning"
        suggested_action = "Run the report format monitor before making business decisions."
        return (
            make_breakdown_row(run_id, "Reports/Format", "Format Stability and Drift Control", 15.0, points, status, reason, suggested_action, timestamp),
            "No format rows available",
        )

    severity_counts = Counter(normalize_text(row.get("Severity", "")).title() for row in active_rows if normalize_text(row.get("Severity", "")))
    critical_count = int(severity_counts.get("Critical", 0))
    high_count = int(severity_counts.get("High", 0))
    medium_minor_count = int(severity_counts.get("Medium", 0) + severity_counts.get("Low", 0))

    comparison_values = [
        normalize_text(row.get("Direction", "")) or normalize_text(row.get("Comparison_Status", "")) or normalize_text(row.get("Interpretation", ""))
        for row in comparison_rows
    ]
    comparison_values = [value for value in comparison_values if value]
    comparison_bonus = 0.0
    if comparison_values:
        negative_markers = {"worsened", "down", "missing in latest run", "major change"}
        positive_markers = {"no change", "flat", "no major change", "new"}
        if any(value.lower() in negative_markers for value in comparison_values):
            comparison_bonus = -1.0
        elif all(value.lower() in positive_markers for value in comparison_values):
            comparison_bonus = 1.0

    points = 15.0
    if critical_count > 0:
        points -= 10.0
    if high_count > 0:
        points -= 5.0
    if medium_minor_count > 0:
        points -= 2.0
    points += comparison_bonus
    points = clamp(points, 0.0, 15.0)

    status = "Good"
    if critical_count > 0:
        status = "Critical"
    elif high_count > 0 or medium_minor_count > 0 or not comparison_values:
        status = "Warning"

    reason_parts = [
        f"critical={critical_count}",
        f"high={high_count}",
        f"medium_or_low={medium_minor_count}",
    ]
    if comparison_values:
        reason_parts.append(f"run_comparison_rows={len(comparison_values)}")
    reason = ", ".join(reason_parts)
    suggested_action = "Fix format drift before trusting the run."
    return (
        make_breakdown_row(run_id, "Reports/Format", "Format Stability and Drift Control", 15.0, points, status, reason, suggested_action, timestamp),
        reason,
    )


def _mapping_result(
    analysis_rows: Sequence[Dict[str, Any]],
    run_history_row: Dict[str, Any],
    timestamp: str,
    run_id: str,
) -> Tuple[Dict[str, Any], str, int, int, int, int, int]:
    rows = clean_rows(analysis_rows)
    total_fsns = count_unique_fsns(rows)
    if total_fsns <= 0:
        total_fsns = int(round(first_numeric(run_history_row, "Target_FSN_Count", default=0.0))) or 1

    orders_count = sum(1 for row in rows if parse_float(row.get("Orders", "")) > 0)
    settlement_count = sum(1 for row in rows if first_non_blank(row, "Net_Settlement", "Amount_Settled", "Flipkart_Net_Earnings", default=""))
    pnl_count = sum(1 for row in rows if first_non_blank(row, "Net_Profit_Before_COGS", "Final_Net_Profit", default=""))

    high_count = sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "HIGH")
    medium_count = sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "MEDIUM")
    low_count = sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "LOW")

    missing_settlement = sum(1 for row in rows if "SETTLEMENT MISSING" in normalize_text(row.get("Missing_Data", "")).upper())
    missing_pnl = sum(1 for row in rows if "PNL MISSING" in normalize_text(row.get("Missing_Data", "")).upper())

    coverage_ratio = len(rows) / total_fsns if total_fsns else 0.0
    order_ratio = orders_count / total_fsns
    settlement_ratio = settlement_count / total_fsns
    pnl_ratio = pnl_count / total_fsns
    confidence_quality = (
        (high_count * 1.0) + (medium_count * 0.6) + (low_count * 0.2)
    ) / total_fsns

    raw_quality = (
        (0.25 * coverage_ratio)
        + (0.25 * order_ratio)
        + (0.20 * settlement_ratio)
        + (0.15 * pnl_ratio)
        + (0.15 * confidence_quality)
    )
    points = score_from_ratio(raw_quality, 20.0)

    warning = (missing_settlement / total_fsns) > 0.30 or (missing_pnl / total_fsns) > 0.30 or points < 12.0
    status = category_status(points, 20.0, has_data=bool(rows), warning=warning)
    reason = (
        f"fsns={total_fsns}, orders={orders_count}, settlement={settlement_count}, pnl={pnl_count}, "
        f"confidence_high_medium_low={high_count}/{medium_count}/{low_count}, "
        f"missing_settlement={missing_settlement}, missing_pnl={missing_pnl}"
    )
    suggested_action = "Improve settlement and PNL coverage, then lift low-confidence rows."
    row = make_breakdown_row(run_id, "Mapping Coverage", "Orders, Settlement, PNL, and Confidence Coverage", 20.0, points, status, reason, suggested_action, timestamp)
    return row, reason, total_fsns, orders_count, settlement_count, pnl_count, high_count + medium_count + low_count


def _cogs_result(
    cost_master_rows: Sequence[Dict[str, Any]],
    total_fsns: int,
    run_id: str,
    timestamp: str,
) -> Tuple[Dict[str, Any], str, int, int]:
    rows = clean_rows(cost_master_rows)
    if not rows:
        reason = "FLIPKART_COST_MASTER has no rows."
        row = make_breakdown_row(run_id, "COGS", "COGS Completion", 15.0, 0.0, "Critical", reason, "Populate FLIPKART_COST_MASTER with entered COGS values.", timestamp)
        return row, reason, 0, 0

    available_cogs, missing_cogs = count_cogs_rows(rows)
    completion_percent = available_cogs / total_fsns if total_fsns else 0.0
    points = score_from_ratio(completion_percent, 15.0)
    warning = (missing_cogs / total_fsns) > 0.30 if total_fsns else True
    status = category_status(points, 15.0, has_data=True, warning=warning)
    reason = f"available={available_cogs}, missing={missing_cogs}, completion_percent={format_score(completion_percent * 100.0)}"
    suggested_action = "Enter COGS for the missing FSNs before making profit decisions."
    row = make_breakdown_row(run_id, "COGS", "COGS Completion", 15.0, points, status, reason, suggested_action, timestamp)
    return row, reason, available_cogs, missing_cogs


def _ads_result(
    ads_master_rows: Sequence[Dict[str, Any]],
    ads_issue_rows: Sequence[Dict[str, Any]],
    total_fsns: int,
    run_id: str,
    timestamp: str,
) -> Tuple[Dict[str, Any], str, int, int]:
    mapped_rows = clean_rows(ads_master_rows)
    issue_rows = clean_rows(ads_issue_rows)
    mapped_fsns = count_unique_fsns(mapped_rows)
    issue_count = len(issue_rows)
    data_exists = bool(mapped_rows or issue_rows)

    if not data_exists:
        reason = "No ads rows were found in FLIPKART_ADS_MASTER or FLIPKART_ADS_MAPPING_ISSUES."
        row = make_breakdown_row(run_id, "Ads", "Ads Mapping and Data Coverage", 10.0, 5.0, "Warning", reason, "Treat ads as neutral until data is available.", timestamp)
        return row, reason, mapped_fsns, issue_count

    mapped_ratio = mapped_fsns / total_fsns if total_fsns else 0.0
    issue_ratio = issue_count / max(len(mapped_rows) + issue_count, 1)
    quality = clamp((0.70 * mapped_ratio) + (0.30 * (1.0 - issue_ratio)), 0.0, 1.0)
    points = clamp(3.0 + (7.0 * quality), 0.0, 10.0)
    warning = issue_ratio > 0.25 or mapped_ratio < 0.25
    status = category_status(points, 10.0, has_data=True, warning=warning)
    reason = f"mapped_fsns={mapped_fsns}, issue_rows={issue_count}, mapped_ratio={format_score(mapped_ratio * 100.0)}, issue_ratio={format_score(issue_ratio * 100.0)}"
    suggested_action = "Reduce ads mapping issues or hold ads spend until mapping improves."
    row = make_breakdown_row(run_id, "Ads", "Ads Mapping and Data Coverage", 10.0, points, status, reason, suggested_action, timestamp)
    return row, reason, mapped_fsns, issue_count


def _returns_result(
    analysis_rows: Sequence[Dict[str, Any]],
    return_comments_rows: Sequence[Dict[str, Any]],
    return_issue_summary_rows: Sequence[Dict[str, Any]],
    total_fsns: int,
    run_id: str,
    timestamp: str,
) -> Tuple[Dict[str, Any], str, int, int]:
    rows = clean_rows(analysis_rows)
    return_target_fsns = count_return_fsns(rows)
    comment_fsns = count_unique_fsns(return_comments_rows)
    summary_fsns = count_unique_fsns(return_issue_summary_rows)

    if return_target_fsns <= 0:
        reason = "No target FSNs currently show returns."
        row = make_breakdown_row(run_id, "Returns", "Return Comment and Issue Coverage", 10.0, 10.0, "Neutral", reason, "No returns action is needed right now.", timestamp)
        return row, reason, comment_fsns, summary_fsns

    comment_ratio = min(comment_fsns / return_target_fsns, 1.0)
    summary_ratio = min(summary_fsns / return_target_fsns, 1.0)
    points = score_from_ratio((0.55 * comment_ratio) + (0.45 * summary_ratio), 10.0)
    warning = comment_fsns < return_target_fsns or summary_fsns < return_target_fsns or points < 8.0
    status = category_status(points, 10.0, has_data=True, warning=warning)
    reason = f"target_return_fsns={return_target_fsns}, comment_fsns={comment_fsns}, issue_summary_fsns={summary_fsns}"
    suggested_action = "Fill missing return coverage before relying on return-based decisions."
    row = make_breakdown_row(run_id, "Returns", "Return Comment and Issue Coverage", 10.0, points, status, reason, suggested_action, timestamp)
    return row, reason, comment_fsns, summary_fsns


def _listings_result(
    listing_rows: Sequence[Dict[str, Any]],
    total_fsns: int,
    run_id: str,
    timestamp: str,
) -> Tuple[Dict[str, Any], str, int]:
    rows = clean_rows(listing_rows)
    found_fsns = count_truthy_listing_rows(rows)
    if not rows:
        reason = "FLIPKART_LISTING_PRESENCE has no rows."
        row = make_breakdown_row(run_id, "Listings", "Active Listing Coverage", 10.0, 5.0, "Warning", reason, "Refresh the listing presence workflow.", timestamp)
        return row, reason, found_fsns

    found_ratio = found_fsns / total_fsns if total_fsns else 0.0
    points = score_from_ratio(found_ratio, 10.0)
    warning = found_ratio < 0.80 or found_fsns < total_fsns
    status = category_status(points, 10.0, has_data=True, warning=warning)
    reason = f"found_in_active_listing={found_fsns}, total_fsns={total_fsns}"
    suggested_action = "Improve active listing coverage or review missing listings."
    row = make_breakdown_row(run_id, "Listings", "Active Listing Coverage", 10.0, points, status, reason, suggested_action, timestamp)
    return row, reason, found_fsns


def _alerts_result(
    alert_rows: Sequence[Dict[str, Any]],
    active_task_rows: Sequence[Dict[str, Any]],
    run_id: str,
    timestamp: str,
) -> Tuple[Dict[str, Any], str, int, int, int, int]:
    alerts = clean_rows(alert_rows)
    tasks = clean_rows(active_task_rows)

    if not alerts:
        reason = "FLIPKART_ALERTS_GENERATED has no rows."
        row = make_breakdown_row(run_id, "Alerts", "Alert Severity Risk", 10.0, 5.0, "Warning", reason, "Regenerate alerts after the underlying tabs are refreshed.", timestamp)
        return row, reason, 0, 0, 0, 0

    severity_counts = Counter(normalize_text(row.get("Severity", "")).title() for row in alerts if normalize_text(row.get("Severity", "")))
    critical = int(severity_counts.get("Critical", 0))
    high = int(severity_counts.get("High", 0))
    medium = int(severity_counts.get("Medium", 0))
    low = int(severity_counts.get("Low", 0))
    total = len(alerts)

    task_open_count = sum(1 for row in tasks if normalize_text(row.get("Status", "")).lower() not in NEGATIVE_TASK_STATUSES)
    task_ratio = task_open_count / max(len(tasks), 1)
    risk = clamp(
        (critical * 0.45 + high * 0.30 + medium * 0.15) / max(total, 1) + (0.10 * task_ratio),
        0.0,
        1.0,
    )
    points = 10.0 * (1.0 - risk)
    warning = critical > 0 or high > 0 or task_open_count > 0
    status = category_status(points, 10.0, has_data=True, warning=warning)
    reason = f"critical={critical}, high={high}, medium={medium}, low={low}, open_tasks={task_open_count}"
    suggested_action = "Reduce critical and high alerts before making major decisions."
    row = make_breakdown_row(run_id, "Alerts", "Alert Severity Risk", 10.0, points, status, reason, suggested_action, timestamp)
    return row, reason, critical, high, medium, low


def _confidence_result(
    analysis_rows: Sequence[Dict[str, Any]],
    total_fsns: int,
    run_id: str,
    timestamp: str,
) -> Tuple[Dict[str, Any], str, int, int, int]:
    rows = clean_rows(analysis_rows)
    high = sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "HIGH")
    medium = sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "MEDIUM")
    low = sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "LOW")

    if total_fsns <= 0:
        reason = "No analysis rows were found."
        row = make_breakdown_row(run_id, "Data Confidence", "Confidence Mix", 10.0, 0.0, "Critical", reason, "Rebuild FLIPKART_SKU_ANALYSIS before using the run.", timestamp)
        return row, reason, high, medium, low

    quality = ((high * 1.0) + (medium * 0.55) + (low * 0.15)) / total_fsns
    points = score_from_ratio(quality, 10.0)
    warning = (low / total_fsns) > 0.25 or (high / total_fsns) < 0.50 or points < 6.0
    status = category_status(points, 10.0, has_data=True, warning=warning)
    reason = f"high={high}, medium={medium}, low={low}, total_fsns={total_fsns}"
    suggested_action = "Shift low-confidence FSNs toward higher-confidence mappings."
    row = make_breakdown_row(run_id, "Data Confidence", "Confidence Mix", 10.0, points, status, reason, suggested_action, timestamp)
    return row, reason, high, medium, low


def build_run_quality_rows(
    run_history_rows: Sequence[Dict[str, Any]],
    fsn_history_rows: Sequence[Dict[str, Any]],
    analysis_rows: Sequence[Dict[str, Any]],
    alert_rows: Sequence[Dict[str, Any]],
    active_task_rows: Sequence[Dict[str, Any]],
    cost_master_rows: Sequence[Dict[str, Any]],
    ads_master_rows: Sequence[Dict[str, Any]],
    ads_issue_rows: Sequence[Dict[str, Any]],
    return_comment_rows: Sequence[Dict[str, Any]],
    return_issue_summary_rows: Sequence[Dict[str, Any]],
    listing_rows: Sequence[Dict[str, Any]],
    report_format_monitor_rows: Sequence[Dict[str, Any]],
    report_format_issue_rows: Sequence[Dict[str, Any]],
    run_comparison_rows: Sequence[Dict[str, Any]] | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    timestamp = now_iso()
    run_history_rows = clean_rows(run_history_rows)
    latest_run_row = latest_non_empty_row(run_history_rows)
    if not latest_run_row:
        raise RuntimeError("No rows found in FLIPKART_RUN_HISTORY")

    latest_run_id = first_non_blank(latest_run_row, "Run_ID")
    report_date = first_non_blank(latest_run_row, "Report_End_Date", "Run_Date")
    if not report_date:
        report_date = first_non_blank(latest_run_row, "Run_Date")

    run_comparison_rows = run_comparison_rows or []

    report_row, report_reason = _report_format_result(report_format_monitor_rows, report_format_issue_rows, run_comparison_rows, latest_run_id, timestamp)
    mapping_row, mapping_reason, total_fsns, orders_count, settlement_count, pnl_count, confidence_total = _mapping_result(analysis_rows, latest_run_row, timestamp, latest_run_id)
    cogs_row, cogs_reason, cogs_available, cogs_missing = _cogs_result(cost_master_rows, total_fsns, latest_run_id, timestamp)
    ads_row, ads_reason, ads_mapped_fsns, ads_issue_count = _ads_result(ads_master_rows, ads_issue_rows, total_fsns, latest_run_id, timestamp)
    returns_row, returns_reason, return_comment_fsns, return_issue_fsns = _returns_result(
        analysis_rows,
        return_comment_rows,
        return_issue_summary_rows,
        total_fsns,
        latest_run_id,
        timestamp,
    )
    listings_row, listings_reason, found_listing_fsns = _listings_result(listing_rows, total_fsns, latest_run_id, timestamp)
    alerts_row, alerts_reason, critical_alerts, high_alerts, medium_alerts, low_alerts = _alerts_result(alert_rows, active_task_rows, latest_run_id, timestamp)
    confidence_row, confidence_reason, confidence_high, confidence_medium, confidence_low = _confidence_result(analysis_rows, total_fsns, latest_run_id, timestamp)

    breakdown_rows = [
        report_row,
        mapping_row,
        cogs_row,
        ads_row,
        returns_row,
        listings_row,
        alerts_row,
        confidence_row,
    ]
    breakdown_rows.sort(key=lambda row: CATEGORY_ORDER.index(normalize_text(row.get("Score_Category", ""))) if normalize_text(row.get("Score_Category", "")) in CATEGORY_ORDER else len(CATEGORY_ORDER))

    summary_score = round(
        sum(parse_float(row.get("Points_Earned", "")) for row in breakdown_rows),
        2,
    )
    grade = grade_from_score(summary_score)
    recommendation = recommendation_from_score(summary_score)

    critical_warnings: List[str] = []
    major_warnings: List[str] = []

    if not analysis_rows:
        critical_warnings.append("FLIPKART_SKU_ANALYSIS has no rows")
    if not is_truthy(latest_run_row.get("Audit_Passed", "1")):
        critical_warnings.append("Latest run audit did not pass")
    if not is_truthy(latest_run_row.get("Google_Sheet_Pushed", "1")):
        major_warnings.append("Latest run was not pushed to Google Sheets")
    if report_row["Status"] == "Critical":
        critical_warnings.append("Report format issues are critical")
    if (cogs_missing / total_fsns) > 0.30 if total_fsns else True:
        critical_warnings.append("COGS completion is below 70%")
    if alerts_row["Status"] == "Critical":
        critical_warnings.append("Critical alerts are present")
    if parse_float(report_row.get("Points_Earned", "")) < 12.0 and report_row["Status"] != "Good":
        major_warnings.append("Report format stability is below the ideal threshold")
    if parse_float(mapping_row.get("Points_Earned", "")) < 14.0:
        major_warnings.append("Mapping coverage needs review")
    if parse_float(ads_row.get("Points_Earned", "")) < 5.0 and (ads_mapped_fsns or ads_issue_count):
        major_warnings.append("Ads mapping quality is weak")
    if parse_float(returns_row.get("Points_Earned", "")) < 8.0 and count_return_fsns(analysis_rows):
        major_warnings.append("Return coverage is incomplete")
    if parse_float(listings_row.get("Points_Earned", "")) < 8.0:
        major_warnings.append("Active listing coverage is weak")
    if parse_float(confidence_row.get("Points_Earned", "")) < 6.0:
        major_warnings.append("Data confidence is low")
    if critical_alerts > 0:
        major_warnings.append("Critical alerts need immediate attention")

    if run_comparison_rows:
        comparison_rows = rows_for_run(run_comparison_rows, latest_run_id, ("Latest_Run_ID", "Run_ID"))
        if comparison_rows:
            comparison_directions = Counter(normalize_text(row.get("Direction", "")) or normalize_text(row.get("Interpretation", "")) for row in comparison_rows)
            if any(key.lower() in {"worsened", "down", "missing in latest run", "major change"} for key in comparison_directions):
                major_warnings.append("Run comparison shows negative movement")
        else:
            major_warnings.append("Run comparison is available but not aligned to the latest run")
    else:
        major_warnings.append("Run comparison tab is unavailable")

    critical_warnings_text = format_warning_text(critical_warnings)
    major_warnings_text = format_warning_text(major_warnings)
    if critical_warnings_text:
        suggested_action = "Pause major decisions and fix the critical warnings first."
    elif major_warnings_text:
        suggested_action = "Review the weak categories before making major decisions."
    elif summary_score >= 90:
        suggested_action = "Proceed with normal business review."
    elif summary_score >= 75:
        suggested_action = "Use the run, but review the weak spots first."
    else:
        suggested_action = "Use carefully and avoid major decisions until the score improves."

    summary_row = {
        "Run_ID": latest_run_id,
        "Report_Date": report_date,
        "Overall_Run_Quality_Score": format_score(summary_score),
        "Run_Quality_Grade": grade,
        "Decision_Recommendation": recommendation,
        "Reports_Score": report_row["Points_Earned"],
        "Mapping_Score": mapping_row["Points_Earned"],
        "COGS_Score": cogs_row["Points_Earned"],
        "Ads_Score": ads_row["Points_Earned"],
        "Returns_Score": returns_row["Points_Earned"],
        "Listing_Score": listings_row["Points_Earned"],
        "Format_Stability_Score": report_row["Points_Earned"],
        "Alert_Risk_Score": alerts_row["Points_Earned"],
        "Data_Confidence_Score": confidence_row["Points_Earned"],
        "Major_Warnings": major_warnings_text,
        "Critical_Warnings": critical_warnings_text,
        "Suggested_Action": suggested_action,
        "Last_Updated": timestamp,
    }

    looker_rows = [
        {
            "Report_Date": report_date,
            "Run_ID": latest_run_id,
            "Overall_Run_Quality_Score": summary_row["Overall_Run_Quality_Score"],
            "Run_Quality_Grade": grade,
            "Decision_Recommendation": recommendation,
            "Score_Category": row["Score_Category"],
            "Score_Name": row["Score_Name"],
            "Max_Points": row["Max_Points"],
            "Points_Earned": row["Points_Earned"],
            "Status": row["Status"],
            "Reason": row["Reason"],
            "Suggested_Action": row["Suggested_Action"],
            "Last_Updated": timestamp,
        }
        for row in breakdown_rows
    ]

    summary = {
        "run_id": latest_run_id,
        "report_date": report_date,
        "overall_score": summary_score,
        "grade": grade,
        "decision_recommendation": recommendation,
        "critical_warnings": critical_warnings_text,
        "major_warnings": major_warnings_text,
        "breakdown_rows": len(breakdown_rows),
        "tabs_updated": [RUN_QUALITY_SCORE_TAB, RUN_QUALITY_BREAKDOWN_TAB, LOOKER_RUN_QUALITY_TAB],
        "latest_breakdown_statuses": {row["Score_Category"]: row["Status"] for row in breakdown_rows},
        "latest_metrics": {
            "total_fsns": total_fsns,
            "orders_count": orders_count,
            "settlement_count": settlement_count,
            "pnl_count": pnl_count,
            "cogs_available": cogs_available,
            "cogs_missing": cogs_missing,
            "ads_mapped_fsns": ads_mapped_fsns,
            "ads_issue_count": ads_issue_count,
            "return_comment_fsns": return_comment_fsns,
            "return_issue_fsns": return_issue_fsns,
            "found_listing_fsns": found_listing_fsns,
            "critical_alerts": critical_alerts,
            "high_alerts": high_alerts,
            "medium_alerts": medium_alerts,
            "low_alerts": low_alerts,
            "confidence_high": confidence_high,
            "confidence_medium": confidence_medium,
            "confidence_low": confidence_low,
        },
        "report_reason": report_reason,
        "mapping_reason": mapping_reason,
        "cogs_reason": cogs_reason,
        "ads_reason": ads_reason,
        "returns_reason": returns_reason,
        "listings_reason": listings_reason,
        "alerts_reason": alerts_reason,
        "confidence_reason": confidence_reason,
    }
    return [summary_row], breakdown_rows, looker_rows, summary
