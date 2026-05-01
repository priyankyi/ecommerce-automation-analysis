from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_cogs_helpers import hydrate_analysis_rows, is_cogs_available
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    append_csv_log,
    build_status_payload,
    clean_fsn,
    ensure_directories,
    normalize_text,
    now_iso,
    parse_float,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_dashboard_log.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
TRACKER_TAB = "FLIPKART_ACTION_TRACKER"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"
RUN_HISTORY_TAB = "FLIPKART_RUN_HISTORY"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"
RETURN_COMMENTS_TAB = "FLIPKART_RETURN_COMMENTS"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
RETURN_REASON_PIVOT_TAB = "FLIPKART_RETURN_REASON_PIVOT"

DASHBOARD_TAB = "FLIPKART_DASHBOARD"
DASHBOARD_DATA_TAB = "FLIPKART_DASHBOARD_DATA"
TOP_ALERTS_TAB = "FLIPKART_TOP_ALERTS"
TOP_RETURN_ISSUES_TAB = "FLIPKART_TOP_RETURN_ISSUES"
ACTION_SUMMARY_TAB = "FLIPKART_ACTION_SUMMARY"

INPUT_TABS = [
    SKU_ANALYSIS_TAB,
    ALERTS_TAB,
    TRACKER_TAB,
    ACTIVE_TASKS_TAB,
    RUN_HISTORY_TAB,
    FSN_HISTORY_TAB,
    RETURN_COMMENTS_TAB,
    RETURN_ISSUE_SUMMARY_TAB,
    RETURN_REASON_PIVOT_TAB,
]

OUTPUT_TABS = [
    DASHBOARD_TAB,
    DASHBOARD_DATA_TAB,
    TOP_ALERTS_TAB,
    TOP_RETURN_ISSUES_TAB,
    ACTION_SUMMARY_TAB,
]

DASHBOARD_HEADERS = ["Section", "Metric", "Value"]
DASHBOARD_DATA_HEADERS = ["Metric", "Value", "Category", "Last_Updated"]
TOP_ALERT_HEADERS = [
    "Priority_Rank",
    "Severity",
    "Alert_Type",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Suggested_Action",
    "Reason",
    "Trigger_Value",
    "Threshold",
    "Status",
    "Owner",
    "Days_Open",
]
ACTION_SUMMARY_HEADERS = [
    "Status",
    "Task_Count",
    "Critical_Count",
    "High_Count",
    "Medium_Count",
    "Low_Count",
    "Oldest_Days_Open",
    "Latest_Updated",
]

SECTION_TITLES = [
    "Section 1: Run Summary",
    "Section 2: Alert Summary",
    "Section 3: Action Status Summary",
    "Section 4: Data Quality Summary",
    "Section 5: Business Risk Summary",
    "Section 6: Return Issue Summary",
]

SEVERITY_ORDER = {
    "Critical": 0,
    "High": 1,
    "Medium": 2,
    "Low": 3,
}

TOP_ALERT_TYPE_PRIORITY = {
    "Negative Final Profit": 0,
    "Critical Return Rate": 1,
    "Negative Profit Before COGS": 2,
    "Listing Not Active": 3,
    "Low Confidence With Sales": 4,
    "Low Final Profit Margin": 0,
    "High Return Rate": 1,
    "Settlement Missing": 2,
    "PNL Missing": 3,
    "Listing Missing": 4,
    "High ACOS": 5,
    "Low ROAS": 6,
    "COGS Missing For Sold FSN": 0,
    "COGS Missing For High Confidence FSN": 1,
    "No Orders With Stock": 2,
    "Medium Confidence": 3,
    "Ads Data Missing": 4,
    "Low Confidence No Sales": 0,
    "Critical Return Issue": 0,
    "Return Fraud Risk": 1,
    "Repeated Product Issue": 2,
    "Product Issue Cluster": 3,
    "Packaging Damage Issue": 4,
    "Product Not Working Returns": 5,
    "Listing Expectation Mismatch": 6,
    "Logistics Return Cluster": 7,
    "Customer RTO Issue": 8,
}

RETURN_ACTION_PRIORITY_ORDER = {
    "Critical": 0,
    "High": 1,
    "Medium": 2,
    "Low": 3,
}

STATUS_ORDER = [
    "Open",
    "Assigned",
    "In Progress",
    "Done",
    "Waiting For Fresh Data",
    "Resolved",
    "Ignored",
    "Reopened",
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


def find_sheet_id(sheets_service, spreadsheet_id: str, tab_name: str) -> int | None:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return int(props["sheetId"])
    return None


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    sheet_id = find_sheet_id(sheets_service, spreadsheet_id, tab_name)
    if sheet_id is not None:
        return sheet_id
    response = retry(
        lambda: sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        )
        .execute()
    )
    return int(response["replies"][0]["addSheet"]["properties"]["sheetId"])


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    return find_sheet_id(sheets_service, spreadsheet_id, tab_name) is not None


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")


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


def clear_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .clear(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:ZZ", body={})
        .execute()
    )


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be 1 or greater")
    result: List[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def write_rows(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    values = [list(headers)] + [[row.get(header, "") for header in headers] for row in rows]
    end_col = column_index_to_a1(len(headers))
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:{end_col}{len(values)}",
            valueInputOption="RAW",
            body={"values": values},
        )
        .execute()
    )


def add_basic_filter(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int, row_count: int) -> None:
    requests = [
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": max(row_count, 1),
                        "startColumnIndex": 0,
                        "endColumnIndex": column_count,
                    }
                }
            }
        }
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def freeze_and_format(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int, section_rows: Sequence[int] | None = None) -> None:
    requests = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": column_count,
                }
            }
        },
    ]
    for row_index in section_rows or []:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_index,
                        "endRowIndex": row_index + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": column_count,
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            }
        )
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def parse_date_value(value: Any) -> Optional[date]:
    text = normalize_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def parse_datetime_value(value: Any) -> Optional[datetime]:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text[:19] if fmt.endswith("%S") else text[:10], fmt)
                return parsed
            except ValueError:
                continue
    return None


def latest_row_by_run_id(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}

    def sort_key(row: Dict[str, Any]) -> Tuple[datetime, datetime]:
        run_id = normalize_text(row.get("Run_ID", ""))
        try:
            run_dt = datetime.strptime(run_id, "FLIPKART_%Y%m%d_%H%M%S")
        except ValueError:
            run_dt = datetime.min
        updated_dt = parse_datetime_value(row.get("Last_Updated", "")) or datetime.min
        return run_dt, updated_dt

    return max(rows, key=sort_key)


def count_return_rate(rows: Sequence[Dict[str, Any]], threshold: float = 0.20) -> int:
    return sum(1 for row in rows if parse_float(row.get("Return_Rate", "")) > threshold)


def count_by_status(rows: Sequence[Dict[str, Any]], status_field: str = "Status") -> Counter:
    counts: Counter = Counter()
    for row in rows:
        status = normalize_text(row.get(status_field, "")) or "(blank)"
        counts[status] += 1
    return counts


def count_by_severity(rows: Sequence[Dict[str, Any]], severity_field: str = "Severity") -> Counter:
    counts: Counter = Counter()
    for row in rows:
        severity = normalize_text(row.get(severity_field, "")) or "(blank)"
        counts[severity] += 1
    return counts


def count_by_issue_category(rows: Sequence[Dict[str, Any]]) -> Counter:
    counts: Counter = Counter()
    for row in rows:
        category = normalize_text(row.get("Issue_Category", "")) or "Other"
        counts[category] += 1
    return counts


def run_status_text(value: Any) -> str:
    text = normalize_text(value).upper()
    if text in {"TRUE", "YES", "1"}:
        return "PASS"
    if text in {"FALSE", "NO", "0"}:
        return "FAIL"
    return normalize_text(value)


def select_rows_for_latest_run(rows: Sequence[Dict[str, Any]], latest_run_id: str) -> List[Dict[str, Any]]:
    if not rows or not latest_run_id:
        return []
    return [row for row in rows if normalize_text(row.get("Run_ID", "")) == latest_run_id]


def build_dashboard_rows(
    run_row: Dict[str, Any],
    analysis_rows: Sequence[Dict[str, Any]],
    cost_rows: Sequence[Dict[str, Any]],
    alert_rows: Sequence[Dict[str, Any]],
    tracker_rows: Sequence[Dict[str, Any]],
    active_rows: Sequence[Dict[str, Any]],
    fsn_rows: Sequence[Dict[str, Any]],
    return_issue_summary_rows: Sequence[Dict[str, Any]],
    return_comment_rows: Sequence[Dict[str, Any]],
) -> Tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    Dict[str, Any],
    List[int],
]:
    latest_run_id = normalize_text(run_row.get("Run_ID", ""))
    latest_run_date = normalize_text(run_row.get("Run_Date", ""))
    report_start_date = normalize_text(run_row.get("Report_Start_Date", ""))
    report_end_date = normalize_text(run_row.get("Report_End_Date", ""))

    latest_run_fsn_rows = select_rows_for_latest_run(fsn_rows, latest_run_id) if latest_run_id else list(fsn_rows)
    data_source_rows = list(analysis_rows)

    alert_counts = count_by_severity(alert_rows, "Severity")
    cogs_rows = [row for row in data_source_rows if is_cogs_available(row)]
    cogs_missing_rows = [row for row in data_source_rows if clean_fsn(row.get("FSN", "")) and not is_cogs_available(row)]
    final_profit_values = [parse_float(row.get("Final_Net_Profit", "")) for row in cogs_rows if normalize_text(row.get("Final_Net_Profit", ""))]
    final_margin_values = [parse_float(row.get("Final_Profit_Margin", "")) for row in cogs_rows if normalize_text(row.get("Final_Profit_Margin", ""))]
    total_final_net_profit = round(sum(final_profit_values), 2) if final_profit_values else 0.0
    total_cogs = round(sum(parse_float(row.get("Total_COGS", "")) for row in cogs_rows if normalize_text(row.get("Total_COGS", ""))), 2) if cogs_rows else 0.0
    avg_final_profit_margin = round(sum(final_margin_values) / len(final_margin_values), 4) if final_margin_values else 0.0
    cogs_completion_percent = round((len(cogs_rows) / len(data_source_rows)) * 100, 2) if data_source_rows else 0.0
    final_negative_profit_count = sum(1 for row in cogs_rows if parse_float(row.get("Final_Net_Profit", "")) < 0)
    low_final_margin_count = sum(1 for row in cogs_rows if parse_float(row.get("Final_Profit_Margin", "")) < 0.10)

    filtered_active_rows = [
        row
        for row in active_rows
        if normalize_text(row.get("Status", "")) not in {"Resolved", "Ignored"}
    ]
    tracker_status_counts = count_by_status(tracker_rows, "Status")
    active_status_counts = count_by_status(filtered_active_rows, "Status")

    action_summary_rows = build_action_summary_rows(tracker_rows, filtered_active_rows)
    top_alert_rows = build_top_alert_rows(filtered_active_rows)
    top_return_issue_rows = build_top_return_issue_rows(return_issue_summary_rows)

    return_detail_rows = list(return_comment_rows)
    return_summary_rows = list(return_issue_summary_rows)
    return_issue_category_counts = count_by_issue_category(return_detail_rows)
    top_return_issue_category = return_issue_category_counts.most_common(1)[0][0] if return_issue_category_counts else ""
    total_classified_return_comments = sum(
        1
        for row in return_detail_rows
        if normalize_text(row.get("Issue_Category", "")) and normalize_text(row.get("Issue_Category", "")) != "Other"
    )
    other_return_comments_count = sum(1 for row in return_detail_rows if normalize_text(row.get("Issue_Category", "")) in {"", "Other"})
    fsns_with_return_issue_summary = len(return_summary_rows)
    critical_return_issue_fsns = sum(1 for row in return_summary_rows if parse_float(row.get("Critical_Issue_Count", "")) > 0)
    product_issue_fsns = sum(1 for row in return_summary_rows if parse_float(row.get("Product_Issue_Count", "")) > 0)
    logistics_issue_fsns = sum(1 for row in return_summary_rows if parse_float(row.get("Logistics_Issue_Count", "")) > 0)
    customer_rto_issue_fsns = sum(1 for row in return_summary_rows if parse_float(row.get("Customer_RTO_Count", "")) > 0)
    return_fraud_risk_fsns = sum(
        1 for row in return_summary_rows if normalize_text(row.get("Top_Issue_Category", "")) == "Return Fraud / Suspicious"
    )

    dashboard_rows: List[Dict[str, Any]] = []
    dashboard_data_rows: List[Dict[str, Any]] = []
    section_row_indexes: List[int] = []

    sections: List[Tuple[str, List[Tuple[str, Any, str]]]] = [
        (
            SECTION_TITLES[0],
            [
                ("Latest Run ID", latest_run_id, "Run Summary"),
                ("Report Start Date", report_start_date, "Run Summary"),
                ("Report End Date", report_end_date, "Run Summary"),
                ("Last Run Date", latest_run_date, "Run Summary"),
                ("Audit Status", run_status_text(run_row.get("Audit_Passed", "")), "Run Summary"),
                ("Google Sheet Push Status", run_status_text(run_row.get("Google_Sheet_Pushed", "")), "Run Summary"),
                ("Total Target FSNs", run_row.get("Target_FSN_Count", ""), "Run Summary"),
                ("Rows Written", run_row.get("Rows_Written", ""), "Run Summary"),
                ("High Confidence Count", run_row.get("High_Confidence_Count", ""), "Run Summary"),
                ("Medium Confidence Count", run_row.get("Medium_Confidence_Count", ""), "Run Summary"),
                ("Low Confidence Count", run_row.get("Low_Confidence_Count", ""), "Run Summary"),
            ],
        ),
        (
            SECTION_TITLES[1],
            [
                ("Total Alerts", len(alert_rows), "Alert Summary"),
                ("Critical Alerts", alert_counts.get("Critical", 0), "Alert Summary"),
                ("High Alerts", alert_counts.get("High", 0), "Alert Summary"),
                ("Medium Alerts", alert_counts.get("Medium", 0), "Alert Summary"),
                ("Low Alerts", alert_counts.get("Low", 0), "Alert Summary"),
            ],
        ),
        (
            SECTION_TITLES[2],
            [
                ("Open Tasks", tracker_status_counts.get("Open", 0), "Action Status Summary"),
                ("In Progress Tasks", tracker_status_counts.get("In Progress", 0), "Action Status Summary"),
                ("Done Tasks", tracker_status_counts.get("Done", 0), "Action Status Summary"),
                ("Waiting For Fresh Data Tasks", tracker_status_counts.get("Waiting For Fresh Data", 0), "Action Status Summary"),
                ("Resolved Tasks", tracker_status_counts.get("Resolved", 0), "Action Status Summary"),
                ("Ignored Tasks", tracker_status_counts.get("Ignored", 0), "Action Status Summary"),
            ],
        ),
        (
            SECTION_TITLES[3],
            [
                ("FSNs With Orders", sum(1 for row in data_source_rows if "Orders Missing" not in normalize_text(row.get("Missing_Data", ""))), "Data Quality Summary"),
                ("FSNs With Returns", sum(1 for row in data_source_rows if "Returns Missing" not in normalize_text(row.get("Missing_Data", ""))), "Data Quality Summary"),
                ("FSNs With Settlement", sum(1 for row in data_source_rows if "Settlement Missing" not in normalize_text(row.get("Missing_Data", ""))), "Data Quality Summary"),
                ("FSNs With PNL", sum(1 for row in data_source_rows if "PNL Missing" not in normalize_text(row.get("Missing_Data", ""))), "Data Quality Summary"),
                ("FSNs With COGS", len(cogs_rows), "COGS Summary"),
                ("FSNs Missing COGS", len(cogs_missing_rows), "COGS Summary"),
                ("COGS Completion Percent", cogs_completion_percent, "COGS Summary"),
            ],
        ),
        (
            SECTION_TITLES[4],
            [
                ("High Return Rate Count", count_return_rate(data_source_rows), "Business Risk Summary"),
                ("Missing Settlement Count", sum(1 for row in data_source_rows if "Settlement Missing" in normalize_text(row.get("Missing_Data", ""))), "Business Risk Summary"),
                ("Missing PNL Count", sum(1 for row in data_source_rows if "PNL Missing" in normalize_text(row.get("Missing_Data", ""))), "Business Risk Summary"),
                ("Final Negative Profit FSNs", final_negative_profit_count, "Business Risk Summary"),
                ("Low Final Margin FSNs", low_final_margin_count, "Business Risk Summary"),
                ("Total Final Net Profit", total_final_net_profit, "Business Risk Summary"),
                ("Total COGS", total_cogs, "Business Risk Summary"),
                ("Average Final Profit Margin", avg_final_profit_margin, "Business Risk Summary"),
            ],
        ),
        (
            SECTION_TITLES[5],
            [
                ("FSNs With Return Issue Summary", fsns_with_return_issue_summary, "Return Issue Summary"),
                ("Critical Return Issue FSNs", critical_return_issue_fsns, "Return Issue Summary"),
                ("Product Issue FSNs", product_issue_fsns, "Return Issue Summary"),
                ("Logistics Issue FSNs", logistics_issue_fsns, "Return Issue Summary"),
                ("Customer RTO Issue FSNs", customer_rto_issue_fsns, "Return Issue Summary"),
                ("Return Fraud Risk FSNs", return_fraud_risk_fsns, "Return Issue Summary"),
                ("Top Return Issue Category", top_return_issue_category, "Return Issue Summary"),
                ("Total Classified Return Comments", total_classified_return_comments, "Return Issue Summary"),
                ("Other Return Comments Count", other_return_comments_count, "Return Issue Summary"),
            ],
        ),
    ]

    for section_title, section_metrics in sections:
        section_row_indexes.append(len(dashboard_rows) + 1)
        dashboard_rows.append({"Section": section_title, "Metric": "", "Value": ""})
        for metric_name, metric_value, category in section_metrics:
            dashboard_rows.append({"Section": "", "Metric": metric_name, "Value": metric_value})
            dashboard_data_rows.append(
                {
                    "Metric": metric_name,
                    "Value": metric_value,
                    "Category": category,
                    "Last_Updated": now_iso(),
                }
            )

    dashboard_payload = {
        "latest_run_id": latest_run_id,
        "total_alerts": len(alert_rows),
        "critical_alerts": alert_counts.get("Critical", 0),
        "high_alerts": alert_counts.get("High", 0),
        "medium_alerts": alert_counts.get("Medium", 0),
        "low_alerts": alert_counts.get("Low", 0),
        "active_tasks": len(filtered_active_rows),
        "tracker_open_tasks": tracker_status_counts.get("Open", 0),
        "tracker_in_progress_tasks": tracker_status_counts.get("In Progress", 0),
        "tracker_done_tasks": tracker_status_counts.get("Done", 0),
        "tracker_waiting_tasks": tracker_status_counts.get("Waiting For Fresh Data", 0),
        "tracker_resolved_tasks": tracker_status_counts.get("Resolved", 0),
        "tracker_ignored_tasks": tracker_status_counts.get("Ignored", 0),
        "dashboard_tabs_updated": OUTPUT_TABS,
        "latest_run_fsn_rows": len(latest_run_fsn_rows),
        "analysis_rows": len(analysis_rows),
        "cost_master_rows": len(cost_rows),
        "tracker_rows": len(tracker_rows),
        "active_rows": len(active_rows),
        "fsns_with_cogs": len(cogs_rows),
        "fsns_missing_cogs": len(cogs_missing_rows),
        "final_negative_profit_fsns": final_negative_profit_count,
        "low_final_margin_fsns": low_final_margin_count,
        "total_final_net_profit": total_final_net_profit,
        "total_cogs": total_cogs,
        "average_final_profit_margin": avg_final_profit_margin,
        "cogs_completion_percent": cogs_completion_percent,
        "fsns_with_return_issue_summary": fsns_with_return_issue_summary,
        "critical_return_issue_fsns": critical_return_issue_fsns,
        "product_issue_fsns": product_issue_fsns,
        "logistics_issue_fsns": logistics_issue_fsns,
        "customer_rto_issue_fsns": customer_rto_issue_fsns,
        "return_fraud_risk_fsns": return_fraud_risk_fsns,
        "top_return_issue_category": top_return_issue_category,
        "total_classified_return_comments": total_classified_return_comments,
        "other_return_comments_count": other_return_comments_count,
    }

    return (
        dashboard_rows,
        dashboard_data_rows,
        top_alert_rows,
        top_return_issue_rows,
        action_summary_rows,
        dashboard_payload,
        section_row_indexes,
    )


def build_top_alert_rows(active_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered_rows = [
        row
        for row in active_rows
        if normalize_text(row.get("Status", "")) not in {"Resolved", "Ignored"}
    ]

    def sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, float, str, str]:
        severity = normalize_text(row.get("Severity", ""))
        severity_rank = SEVERITY_ORDER.get(severity, len(SEVERITY_ORDER))
        alert_type = normalize_text(row.get("Alert_Type", ""))
        alert_rank = TOP_ALERT_TYPE_PRIORITY.get(alert_type, 99)
        days_open_text = normalize_text(row.get("Days_Open", ""))
        days_open = parse_float(days_open_text) if days_open_text else 0.0
        blank_flag = 1 if days_open_text == "" else 0
        alert_id = normalize_text(row.get("Alert_ID", ""))
        fsn = clean_fsn(row.get("FSN", ""))
        return severity_rank, alert_rank, blank_flag, -days_open, alert_id, fsn

    sorted_rows = sorted(filtered_rows, key=sort_key)[:50]
    output: List[Dict[str, Any]] = []
    for index, row in enumerate(sorted_rows, start=1):
        output.append(
            {
                "Priority_Rank": index,
                "Severity": normalize_text(row.get("Severity", "")),
                "Alert_Type": normalize_text(row.get("Alert_Type", "")),
                "FSN": clean_fsn(row.get("FSN", "")),
                "SKU_ID": normalize_text(row.get("SKU_ID", "")),
                "Product_Title": normalize_text(row.get("Product_Title", "")),
                "Suggested_Action": normalize_text(row.get("Suggested_Action", "")),
                "Reason": normalize_text(row.get("Reason", "")),
                "Trigger_Value": normalize_text(row.get("Trigger_Value", "")),
                "Threshold": normalize_text(row.get("Threshold", "")),
                "Status": normalize_text(row.get("Status", "")),
                "Owner": normalize_text(row.get("Owner", "")),
                "Days_Open": normalize_text(row.get("Days_Open", "")),
            }
        )
    return output


def build_top_return_issue_rows(summary_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(row: Dict[str, Any]) -> Tuple[int, float, float, float, str]:
        priority = normalize_text(row.get("Return_Action_Priority", ""))
        priority_rank = RETURN_ACTION_PRIORITY_ORDER.get(priority, len(RETURN_ACTION_PRIORITY_ORDER))
        total_returns = parse_float(row.get("Total_Returns_In_Detailed_Report", ""))
        critical_count = parse_float(row.get("Critical_Issue_Count", ""))
        high_count = parse_float(row.get("High_Issue_Count", ""))
        fsn = clean_fsn(row.get("FSN", ""))
        return priority_rank, -total_returns, -critical_count, -high_count, fsn

    sorted_rows = sorted(summary_rows, key=sort_key)[:50]
    output: List[Dict[str, Any]] = []
    for index, row in enumerate(sorted_rows, start=1):
        output.append(
            {
                "Priority_Rank": index,
                "FSN": clean_fsn(row.get("FSN", "")),
                "SKU_ID": normalize_text(row.get("SKU_ID", "")),
                "Product_Title": normalize_text(row.get("Product_Title", "")),
                "Total_Returns_In_Detailed_Report": normalize_text(row.get("Total_Returns_In_Detailed_Report", "")),
                "Customer_Return_Count": normalize_text(row.get("Customer_Return_Count", "")),
                "Courier_Return_Count": normalize_text(row.get("Courier_Return_Count", "")),
                "Unknown_Return_Count": normalize_text(row.get("Unknown_Return_Count", "")),
                "Top_Issue_Category": normalize_text(row.get("Top_Issue_Category", "")),
                "Top_Return_Reason": normalize_text(row.get("Top_Return_Reason", "")),
                "Top_Return_Sub_Reason": normalize_text(row.get("Top_Return_Sub_Reason", "")),
                "Critical_Issue_Count": normalize_text(row.get("Critical_Issue_Count", "")),
                "High_Issue_Count": normalize_text(row.get("High_Issue_Count", "")),
                "Product_Issue_Count": normalize_text(row.get("Product_Issue_Count", "")),
                "Logistics_Issue_Count": normalize_text(row.get("Logistics_Issue_Count", "")),
                "Customer_RTO_Count": normalize_text(row.get("Customer_RTO_Count", "")),
                "Suggested_Return_Action": normalize_text(row.get("Suggested_Return_Action", "")),
                "Return_Action_Priority": normalize_text(row.get("Return_Action_Priority", "")),
            }
        )
    return output


def build_action_summary_rows(tracker_rows: Sequence[Dict[str, Any]], active_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tracker_by_status: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    active_by_status: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in tracker_rows:
        status = normalize_text(row.get("Status", ""))
        if status:
            tracker_by_status[status].append(row)
    for row in active_rows:
        status = normalize_text(row.get("Status", ""))
        if status:
            active_by_status[status].append(row)

    statuses = list(STATUS_ORDER)
    for status in sorted(set(tracker_by_status) | set(active_by_status)):
        if status not in statuses:
            statuses.append(status)

    def status_sort_key(status: str) -> Tuple[int, str]:
        if status in STATUS_ORDER:
            return STATUS_ORDER.index(status), status
        return len(STATUS_ORDER), status

    today = date.today()
    output: List[Dict[str, Any]] = []
    for status in sorted(statuses, key=status_sort_key):
        tracker_group = tracker_by_status.get(status, [])
        active_group = active_by_status.get(status, [])
        if not tracker_group and not active_group:
            continue
        latest_updated = ""
        latest_dt: Optional[datetime] = None
        for row in tracker_group + active_group:
            updated = parse_datetime_value(row.get("Last_Updated", ""))
            if updated and (latest_dt is None or updated > latest_dt):
                latest_dt = updated
                latest_updated = updated.isoformat(timespec="seconds")
        oldest_days_open = ""
        days_open_values: List[float] = []
        for row in active_group:
            parsed = parse_float(row.get("Days_Open", ""))
            if parsed is not None:
                days_open_values.append(parsed)
        if not days_open_values and tracker_group:
            for row in tracker_group:
                for field in ("Action_Date", "Review_After_Date", "Last_Updated"):
                    parsed_date = parse_date_value(row.get(field, ""))
                    if parsed_date:
                        days_open_values.append(float((today - parsed_date).days))
                        break
        if days_open_values:
            oldest_days_open = str(int(max(days_open_values)))

        severity_counts = Counter(normalize_text(row.get("Severity", "")) or "(blank)" for row in active_group)
        output.append(
            {
                "Status": status,
                "Task_Count": len(tracker_group),
                "Critical_Count": severity_counts.get("Critical", 0),
                "High_Count": severity_counts.get("High", 0),
                "Medium_Count": severity_counts.get("Medium", 0),
                "Low_Count": severity_counts.get("Low", 0),
                "Oldest_Days_Open": oldest_days_open,
                "Latest_Updated": latest_updated,
            }
        )
    return output


def update_dashboard_tabs(
    sheets_service,
    spreadsheet_id: str,
    dashboard_rows: Sequence[Dict[str, Any]],
    dashboard_data_rows: Sequence[Dict[str, Any]],
    top_alert_rows: Sequence[Dict[str, Any]],
    top_return_issue_rows: Sequence[Dict[str, Any]],
    action_summary_rows: Sequence[Dict[str, Any]],
    section_row_indexes: Sequence[int],
) -> None:
    payloads = [
        (DASHBOARD_TAB, DASHBOARD_HEADERS, dashboard_rows, 3, section_row_indexes),
        (DASHBOARD_DATA_TAB, DASHBOARD_DATA_HEADERS, dashboard_data_rows, 4, []),
        (TOP_ALERTS_TAB, TOP_ALERT_HEADERS, top_alert_rows, 13, []),
        (
            TOP_RETURN_ISSUES_TAB,
            [
                "Priority_Rank",
                "FSN",
                "SKU_ID",
                "Product_Title",
                "Total_Returns_In_Detailed_Report",
                "Customer_Return_Count",
                "Courier_Return_Count",
                "Unknown_Return_Count",
                "Top_Issue_Category",
                "Top_Return_Reason",
                "Top_Return_Sub_Reason",
                "Critical_Issue_Count",
                "High_Issue_Count",
                "Product_Issue_Count",
                "Logistics_Issue_Count",
                "Customer_RTO_Count",
                "Suggested_Return_Action",
                "Return_Action_Priority",
            ],
            top_return_issue_rows,
            15,
            [],
        ),
        (ACTION_SUMMARY_TAB, ACTION_SUMMARY_HEADERS, action_summary_rows, 8, []),
    ]

    for tab_name, headers, rows, column_count, section_rows in payloads:
        sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
        clear_tab(sheets_service, spreadsheet_id, tab_name)
        write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
        freeze_and_format(
            sheets_service,
            spreadsheet_id,
            sheet_id,
            column_count,
            section_rows=section_rows if tab_name == DASHBOARD_TAB else None,
        )
        add_basic_filter(sheets_service, spreadsheet_id, sheet_id, column_count, len(rows) + 1)


def create_flipkart_dashboard() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in INPUT_TABS:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)
    ensure_required_tab_exists(sheets_service, spreadsheet_id, COST_MASTER_TAB)

    _, run_rows = read_table(sheets_service, spreadsheet_id, RUN_HISTORY_TAB)
    _, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    _, cost_rows = read_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    _, alert_rows = read_table(sheets_service, spreadsheet_id, ALERTS_TAB)
    _, tracker_rows = read_table(sheets_service, spreadsheet_id, TRACKER_TAB)
    _, active_rows = read_table(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)
    _, fsn_rows = read_table(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
    _, return_comment_rows = read_table(sheets_service, spreadsheet_id, RETURN_COMMENTS_TAB)
    _, return_issue_summary_rows = read_table(sheets_service, spreadsheet_id, RETURN_ISSUE_SUMMARY_TAB)

    if not run_rows:
        raise RuntimeError(f"No rows found in {RUN_HISTORY_TAB}")
    if not analysis_rows:
        raise RuntimeError(f"No rows found in {SKU_ANALYSIS_TAB}")

    live_analysis_rows = hydrate_analysis_rows(analysis_rows, cost_rows)
    latest_run_row = latest_row_by_run_id(run_rows)
    if not latest_run_row and fsn_rows:
        fsn_run_ids = [normalize_text(row.get("Run_ID", "")) for row in fsn_rows if normalize_text(row.get("Run_ID", ""))]
        latest_run_id = sorted(fsn_run_ids)[-1] if fsn_run_ids else ""
        latest_run_row = next((row for row in run_rows if normalize_text(row.get("Run_ID", "")) == latest_run_id), run_rows[-1])

    dashboard_rows, dashboard_data_rows, top_alert_rows, top_return_issue_rows, action_summary_rows, dashboard_payload, section_row_indexes = build_dashboard_rows(
        latest_run_row,
        live_analysis_rows,
        cost_rows,
        alert_rows,
        tracker_rows,
        active_rows,
        fsn_rows,
        return_issue_summary_rows,
        return_comment_rows,
    )

    update_dashboard_tabs(
        sheets_service,
        spreadsheet_id,
        dashboard_rows,
        dashboard_data_rows,
        top_alert_rows,
        top_return_issue_rows,
        action_summary_rows,
        section_row_indexes,
    )

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "latest_run_id": dashboard_payload.get("latest_run_id", ""),
        "total_alerts": dashboard_payload.get("total_alerts", 0),
        "critical_alerts": dashboard_payload.get("critical_alerts", 0),
        "high_alerts": dashboard_payload.get("high_alerts", 0),
        "medium_alerts": dashboard_payload.get("medium_alerts", 0),
        "low_alerts": dashboard_payload.get("low_alerts", 0),
        "active_tasks": dashboard_payload.get("active_tasks", 0),
        "fsns_with_return_issue_summary": dashboard_payload.get("fsns_with_return_issue_summary", 0),
        "critical_return_issue_fsns": dashboard_payload.get("critical_return_issue_fsns", 0),
        "product_issue_fsns": dashboard_payload.get("product_issue_fsns", 0),
        "logistics_issue_fsns": dashboard_payload.get("logistics_issue_fsns", 0),
        "customer_rto_issue_fsns": dashboard_payload.get("customer_rto_issue_fsns", 0),
        "return_fraud_risk_fsns": dashboard_payload.get("return_fraud_risk_fsns", 0),
        "top_return_issue_category": dashboard_payload.get("top_return_issue_category", ""),
        "total_classified_return_comments": dashboard_payload.get("total_classified_return_comments", 0),
        "other_return_comments_count": dashboard_payload.get("other_return_comments_count", 0),
        "dashboard_tabs_updated": json.dumps(dashboard_payload.get("dashboard_tabs_updated", []), ensure_ascii=False),
        "status": "SUCCESS",
        "message": "Flipkart dashboard tabs refreshed",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "latest_run_id",
            "total_alerts",
            "critical_alerts",
            "high_alerts",
            "medium_alerts",
            "low_alerts",
            "active_tasks",
            "dashboard_tabs_updated",
            "status",
            "message",
        ],
        [log_row],
    )

    result = {
        "status": "SUCCESS",
        "latest_run_id": dashboard_payload.get("latest_run_id", ""),
        "total_alerts": dashboard_payload.get("total_alerts", 0),
        "critical_alerts": dashboard_payload.get("critical_alerts", 0),
        "high_alerts": dashboard_payload.get("high_alerts", 0),
        "medium_alerts": dashboard_payload.get("medium_alerts", 0),
        "low_alerts": dashboard_payload.get("low_alerts", 0),
        "active_tasks": dashboard_payload.get("active_tasks", 0),
        "fsns_with_cogs": dashboard_payload.get("fsns_with_cogs", 0),
        "fsns_missing_cogs": dashboard_payload.get("fsns_missing_cogs", 0),
        "final_negative_profit_fsns": dashboard_payload.get("final_negative_profit_fsns", 0),
        "low_final_margin_fsns": dashboard_payload.get("low_final_margin_fsns", 0),
        "total_final_net_profit": dashboard_payload.get("total_final_net_profit", 0),
        "total_cogs": dashboard_payload.get("total_cogs", 0),
        "average_final_profit_margin": dashboard_payload.get("average_final_profit_margin", 0),
        "cogs_completion_percent": dashboard_payload.get("cogs_completion_percent", 0),
        "fsns_with_return_issue_summary": dashboard_payload.get("fsns_with_return_issue_summary", 0),
        "critical_return_issue_fsns": dashboard_payload.get("critical_return_issue_fsns", 0),
        "product_issue_fsns": dashboard_payload.get("product_issue_fsns", 0),
        "logistics_issue_fsns": dashboard_payload.get("logistics_issue_fsns", 0),
        "customer_rto_issue_fsns": dashboard_payload.get("customer_rto_issue_fsns", 0),
        "return_fraud_risk_fsns": dashboard_payload.get("return_fraud_risk_fsns", 0),
        "top_return_issue_category": dashboard_payload.get("top_return_issue_category", ""),
        "total_classified_return_comments": dashboard_payload.get("total_classified_return_comments", 0),
        "other_return_comments_count": dashboard_payload.get("other_return_comments_count", 0),
        "dashboard_tabs_updated": dashboard_payload.get("dashboard_tabs_updated", []),
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(build_status_payload("SUCCESS", **{k: v for k, v in result.items() if k != "status"}), indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_dashboard()
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
