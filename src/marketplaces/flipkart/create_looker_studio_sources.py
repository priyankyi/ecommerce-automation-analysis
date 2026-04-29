from __future__ import annotations

import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_cogs_helpers import COGS_AVAILABLE_STATUSES, count_cogs_rows, is_cogs_available
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
LOG_PATH = LOG_DIR / "flipkart_looker_studio_sources_log.csv"

SOURCE_TABS = [
    "FLIPKART_DASHBOARD_DATA",
    "FLIPKART_SKU_ANALYSIS",
    "FLIPKART_ALERTS_GENERATED",
    "FLIPKART_ACTIVE_TASKS",
    "FLIPKART_ACTION_TRACKER",
    "FLIPKART_ADS_PLANNER",
    "FLIPKART_RETURN_ISSUE_SUMMARY",
    "FLIPKART_LISTING_PRESENCE",
    "FLIPKART_MISSING_ACTIVE_LISTINGS",
    "FLIPKART_RUN_HISTORY",
    "FLIPKART_FSN_HISTORY",
]

LOOKER_EXECUTIVE_TAB = "LOOKER_FLIPKART_EXECUTIVE_SUMMARY"
LOOKER_FSN_METRICS_TAB = "LOOKER_FLIPKART_FSN_METRICS"
LOOKER_ALERTS_TAB = "LOOKER_FLIPKART_ALERTS"
LOOKER_ACTIONS_TAB = "LOOKER_FLIPKART_ACTIONS"
LOOKER_ADS_TAB = "LOOKER_FLIPKART_ADS"
LOOKER_RETURNS_TAB = "LOOKER_FLIPKART_RETURNS"
LOOKER_LISTINGS_TAB = "LOOKER_FLIPKART_LISTINGS"

LOOKER_TABS = [
    LOOKER_EXECUTIVE_TAB,
    LOOKER_FSN_METRICS_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_LISTINGS_TAB,
]

LOOKER_HEADERS = {
    LOOKER_EXECUTIVE_TAB: [
        "Report_Date",
        "Run_ID",
        "Metric_Category",
        "Metric_Name",
        "Metric_Value",
        "Metric_Display_Value",
        "Sort_Order",
        "Last_Updated",
    ],
    LOOKER_FSN_METRICS_TAB: [
        "Run_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Category",
        "Listing_Presence_Status",
        "Found_In_Active_Listing",
        "Orders",
        "Units_Sold",
        "Gross_Sales",
        "Returns",
        "Return_Rate",
        "Net_Settlement",
        "Flipkart_Net_Earnings",
        "Net_Profit_Before_COGS",
        "Cost_Price",
        "Total_Unit_COGS",
        "Total_COGS",
        "Final_Net_Profit",
        "Final_Profit_Margin",
        "COGS_Status",
        "Data_Confidence",
        "Final_Action",
        "Final_Ads_Decision",
        "Final_Budget_Recommendation",
        "Ads_Risk_Level",
        "Ads_Opportunity_Level",
        "Last_Updated",
    ],
    LOOKER_ALERTS_TAB: [
        "Run_ID",
        "Alert_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Alert_Type",
        "Severity",
        "Suggested_Action",
        "Reason",
        "Data_Confidence",
        "Status_Default",
        "Last_Updated",
    ],
    LOOKER_ACTIONS_TAB: [
        "Action_ID",
        "Alert_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Alert_Type",
        "Severity",
        "Owner",
        "Status",
        "Action_Taken",
        "Action_Date",
        "Expected_Impact",
        "Review_After_Date",
        "Resolution_Notes",
        "Last_Updated",
    ],
    LOOKER_ADS_TAB: [
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Final_Product_Type",
        "Final_Seasonality_Tag",
        "Ad_Run_Type",
        "Current_Ad_Status",
        "Ad_ROAS",
        "Ad_ACOS",
        "Ad_Revenue",
        "Estimated_Ad_Spend",
        "Final_Ads_Decision",
        "Final_Budget_Recommendation",
        "Ads_Risk_Level",
        "Ads_Opportunity_Level",
        "Ads_Decision_Reason",
        "Last_Updated",
    ],
    LOOKER_RETURNS_TAB: [
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Total_Returns_In_Detailed_Report",
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
        "Last_Updated",
    ],
    LOOKER_LISTINGS_TAB: [
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Found_In_Active_Listing",
        "Listing_Presence_Status",
        "Possible_Issue",
        "Suggested_Action",
        "Priority",
        "Last_Updated",
    ],
}

METRIC_ROWS = [
    ("Overview", "Total Target FSNs", 10, "integer"),
    ("Profit", "Final Profit", 20, "money"),
    ("Alerts", "Total Alerts", 30, "integer"),
    ("Alerts", "Critical Alerts", 40, "integer"),
    ("Alerts", "High Alerts", 50, "integer"),
    ("Operations", "Active Tasks", 60, "integer"),
    ("COGS", "Missing COGS", 70, "integer"),
    ("Listings", "Missing Active Listings", 80, "integer"),
    ("Ads", "Ads Ready Count", 90, "integer"),
    ("Returns", "Return Issue FSNs", 100, "integer"),
    ("COGS", "COGS Completion Percent", 110, "percent"),
]


def retry(func: Callable[[], Any], attempts: int = 4) -> Any:
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


def find_sheet_id(sheets_service, spreadsheet_id: str, tab_name: str) -> Optional[int]:
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
    retry(
        lambda: sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "setBasicFilter": {
                            "filter": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": max(row_count, 2),
                                    "startColumnIndex": 0,
                                    "endColumnIndex": column_count,
                                }
                            }
                        }
                    }
                ]
            },
        )
        .execute()
    )


def freeze_and_format(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int) -> None:
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
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def normalize_number_text(value: Any, decimals: int = 2) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    number = parse_float(text)
    if decimals <= 0:
        return str(int(round(number)))
    if float(number).is_integer():
        return str(int(round(number)))
    return f"{number:.{decimals}f}".rstrip("0").rstrip(".")


def format_display_value(metric_name: str, metric_value: str, kind: str) -> str:
    if not metric_value:
        return ""
    if kind == "percent":
        return f"{normalize_number_text(metric_value, 2)}%"
    if kind == "money":
        return f"{parse_float(metric_value):,.2f}"
    if kind == "integer":
        return f"{int(round(parse_float(metric_value))):,}"
    return metric_value


def get_latest_run_row(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}

    def sort_key(row: Dict[str, Any]) -> Tuple[datetime, datetime]:
        run_id = normalize_text(row.get("Run_ID", ""))
        try:
            run_dt = datetime.strptime(run_id, "FLIPKART_%Y%m%d_%H%M%S")
        except ValueError:
            run_dt = datetime.min
        updated_text = normalize_text(row.get("Last_Updated", ""))
        try:
            updated_dt = datetime.fromisoformat(updated_text)
        except ValueError:
            updated_dt = datetime.min
        return run_dt, updated_dt

    return max(rows, key=sort_key)


def latest_text_value(row: Dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = normalize_text(row.get(field, ""))
        if value:
            return value
    return ""


def build_index(
    rows: Sequence[Dict[str, Any]],
    key_field: str = "FSN",
    latest_by_updated: bool = False,
) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for row in rows:
        key = clean_fsn(row.get(key_field, "")) if key_field == "FSN" else normalize_text(row.get(key_field, ""))
        if not key:
            continue
        if key not in indexed:
            indexed[key] = dict(row)
            order.append(key)
        elif latest_by_updated:
            existing = indexed[key]
            existing_updated = normalize_text(existing.get("Last_Updated", ""))
            new_updated = normalize_text(row.get("Last_Updated", ""))
            if new_updated > existing_updated:
                indexed[key] = dict(row)
    if latest_by_updated:
        return indexed
    return {key: indexed[key] for key in order}


def build_dashboard_lookup(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        metric = normalize_text(row.get("Metric", ""))
        if not metric:
            continue
        lookup[metric] = dict(row)
    return lookup


def lookup_dashboard_metric(lookup: Dict[str, Dict[str, Any]], *metric_names: str) -> str:
    for metric_name in metric_names:
        row = lookup.get(metric_name)
        if row is not None:
            value = normalize_text(row.get("Value", ""))
            if value:
                return value
    return ""


def count_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def count_non_blank_fsns(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if clean_fsn(row.get("FSN", "")))


def count_severity(rows: Sequence[Dict[str, Any]], severity: str) -> int:
    return sum(1 for row in rows if normalize_text(row.get("Severity", "")) == severity)


def count_ads_ready(rows: Sequence[Dict[str, Any]]) -> int:
    ready_decisions = {"Test Ads", "Always-On Test", "Seasonal/Event Test", "Scale Ads", "Continue / Optimize Ads"}
    ready_statuses = {"READY", "PREPARE"}
    count = 0
    for row in rows:
        status = normalize_text(row.get("Ads_Readiness_Status", "")).upper()
        decision = normalize_text(row.get("Final_Ads_Decision", ""))
        if status in ready_statuses or decision in ready_decisions:
            count += 1
    return count


def parse_metric_number(metric_value: str, kind: str) -> float:
    if not metric_value:
        return 0.0
    if kind == "percent" and metric_value.endswith("%"):
        return parse_float(metric_value[:-1]) / 100.0
    return parse_float(metric_value)


def build_executive_summary_rows(
    context: Dict[str, Any],
    metric_source_lookup: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    report_date = context["report_date"]
    run_id = context["run_id"]
    last_updated = now_iso()

    computed_metrics = {
        "Total Target FSNs": str(context["total_target_fsns"]),
        "Final Profit": normalize_number_text(context["final_profit"], 2),
        "Total Alerts": str(context["total_alerts"]),
        "Critical Alerts": str(context["critical_alerts"]),
        "High Alerts": str(context["high_alerts"]),
        "Active Tasks": str(context["active_tasks"]),
        "Missing COGS": str(context["missing_cogs"]),
        "Missing Active Listings": str(context["missing_active_listings"]),
        "Ads Ready Count": str(context["ads_ready_count"]),
        "Return Issue FSNs": str(context["return_issue_fsns"]),
        "COGS Completion Percent": normalize_number_text(context["cogs_completion_percent"], 2),
    }

    source_metric_aliases = {
        "Total Target FSNs": ("Total Target FSNs", "Target FSN Count", "Target_FSN_Count"),
        "Final Profit": ("Final Profit", "Total Final Net Profit", "Final Net Profit"),
        "Total Alerts": ("Total Alerts",),
        "Critical Alerts": ("Critical Alerts",),
        "High Alerts": ("High Alerts",),
        "Active Tasks": ("Active Tasks",),
        "Missing COGS": ("Missing COGS", "FSNs Missing COGS"),
        "Missing Active Listings": ("Missing Active Listings",),
        "Ads Ready Count": ("Ads Ready Count",),
        "Return Issue FSNs": ("Return Issue FSNs", "FSNs With Return Issue Summary"),
        "COGS Completion Percent": ("COGS Completion Percent",),
    }

    for metric_category, metric_name, sort_order, kind in METRIC_ROWS:
        metric_row = metric_source_lookup.get(metric_name, {})
        value = lookup_dashboard_metric(metric_source_lookup, *source_metric_aliases.get(metric_name, (metric_name,)))
        if not value:
            value = computed_metrics.get(metric_name, "")
        display_value = format_display_value(metric_name, value, kind)
        rows.append(
            {
                "Report_Date": report_date,
                "Run_ID": run_id,
                "Metric_Category": metric_category,
                "Metric_Name": metric_name,
                "Metric_Value": value,
                "Metric_Display_Value": display_value,
                "Sort_Order": sort_order,
                "Last_Updated": normalize_text(metric_row.get("Last_Updated", "")) or last_updated,
            }
        )

    return rows


def safe_metric_text(row: Dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = normalize_text(row.get(field, ""))
        if value:
            return value
    return ""


def resolve_cogs_fields(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    cost_price = safe_metric_text(row, "Cost_Price")
    total_unit_cogs = safe_metric_text(row, "Total_Unit_COGS")
    total_cogs = safe_metric_text(row, "Total_COGS")
    final_net_profit = safe_metric_text(row, "Final_Net_Profit")

    if not total_unit_cogs and any(safe_metric_text(row, field) for field in ("Cost_Price", "Packaging_Cost", "Other_Cost")):
        total_unit_cogs = normalize_number_text(
            parse_float(row.get("Cost_Price", "")) + parse_float(row.get("Packaging_Cost", "")) + parse_float(row.get("Other_Cost", "")),
            2,
        )

    if not total_cogs and total_unit_cogs:
        total_cogs = normalize_number_text(parse_float(total_unit_cogs) * parse_float(row.get("Units_Sold", "")), 2)

    if not final_net_profit and total_cogs:
        final_net_profit = normalize_number_text(parse_float(row.get("Net_Profit_Before_COGS", "")) - parse_float(total_cogs), 2)

    return cost_price, total_unit_cogs, total_cogs, final_net_profit


def build_fsn_metrics_rows(
    analysis_rows: Sequence[Dict[str, Any]],
    listing_lookup: Dict[str, Dict[str, Any]],
    missing_listing_lookup: Dict[str, Dict[str, Any]],
    ads_lookup: Dict[str, Dict[str, Any]],
    return_lookup: Dict[str, Dict[str, Any]],
    latest_run_id: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_fsns: set[str] = set()
    ordered_fsns: List[str] = []
    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in seen_fsns:
            seen_fsns.add(fsn)
            ordered_fsns.append(fsn)

    for fsn in ordered_fsns:
        analysis_row = next((row for row in analysis_rows if clean_fsn(row.get("FSN", "")) == fsn), {})
        listing_row = listing_lookup.get(fsn, {})
        missing_listing_row = missing_listing_lookup.get(fsn, {})
        ads_row = ads_lookup.get(fsn, {})
        return_row = return_lookup.get(fsn, {})

        cost_price, total_unit_cogs, total_cogs, final_net_profit = resolve_cogs_fields(analysis_row)
        final_profit_margin = safe_metric_text(analysis_row, "Final_Profit_Margin")
        if not final_profit_margin and final_net_profit and safe_metric_text(analysis_row, "Gross_Sales"):
            final_profit_margin = normalize_number_text(parse_float(final_net_profit) / parse_float(analysis_row.get("Gross_Sales", "")), 4)

        rows.append(
            {
                "Run_ID": latest_run_id,
                "FSN": fsn,
                "SKU_ID": safe_metric_text(analysis_row, "SKU_ID", "SKU", "Seller_SKU"),
                "Product_Title": safe_metric_text(analysis_row, "Product_Title", "Title"),
                "Category": safe_metric_text(analysis_row, "Category"),
                "Listing_Presence_Status": safe_metric_text(
                    listing_row,
                    "Listing_Presence_Status",
                    "Target_FSN_Status",
                    "Detected_Status",
                )
                or safe_metric_text(missing_listing_row, "Listing_Presence_Status", "Status")
                or ("Found" if safe_metric_text(listing_row, "Found_In_Active_Listing") in {"Yes", "TRUE"} else "Missing"),
                "Found_In_Active_Listing": safe_metric_text(listing_row, "Found_In_Active_Listing")
                or ("Yes" if safe_metric_text(listing_row, "Found_In_Active_Listing").upper() in {"YES", "TRUE"} else "No"),
                "Orders": safe_metric_text(analysis_row, "Orders"),
                "Units_Sold": safe_metric_text(analysis_row, "Units_Sold"),
                "Gross_Sales": safe_metric_text(analysis_row, "Gross_Sales"),
                "Returns": safe_metric_text(analysis_row, "Returns"),
                "Return_Rate": safe_metric_text(analysis_row, "Return_Rate"),
                "Net_Settlement": safe_metric_text(analysis_row, "Net_Settlement"),
                "Flipkart_Net_Earnings": safe_metric_text(analysis_row, "Flipkart_Net_Earnings"),
                "Net_Profit_Before_COGS": safe_metric_text(analysis_row, "Net_Profit_Before_COGS"),
                "Cost_Price": cost_price,
                "Total_Unit_COGS": total_unit_cogs,
                "Total_COGS": total_cogs,
                "Final_Net_Profit": final_net_profit,
                "Final_Profit_Margin": final_profit_margin,
                "COGS_Status": safe_metric_text(analysis_row, "COGS_Status")
                or ("Entered" if normalize_text(total_unit_cogs) else "Missing"),
                "Data_Confidence": safe_metric_text(analysis_row, "Data_Confidence"),
                "Final_Action": safe_metric_text(analysis_row, "Final_Action", "Suggested_Action"),
                "Final_Ads_Decision": safe_metric_text(analysis_row, "Final_Ads_Decision", "Suggested_Ad_Action", "Final_Action"),
                "Final_Budget_Recommendation": safe_metric_text(
                    analysis_row,
                    "Final_Budget_Recommendation",
                    "Suggested_Budget_Level",
                )
                or safe_metric_text(ads_row, "Final_Budget_Recommendation"),
                "Ads_Risk_Level": safe_metric_text(ads_row, "Ads_Risk_Level", "Risk_Level"),
                "Ads_Opportunity_Level": safe_metric_text(ads_row, "Ads_Opportunity_Level", "Opportunity_Level"),
                "Last_Updated": safe_metric_text(analysis_row, "Last_Updated", "Updated_At") or safe_metric_text(ads_row, "Last_Updated") or now_iso(),
            }
        )

    return rows


def stable_alert_id(row: Dict[str, Any]) -> str:
    key = "|".join(
        [
            clean_fsn(row.get("FSN", "")),
            normalize_text(row.get("Alert_Type", "")).upper(),
            normalize_text(row.get("Reason", "")).upper(),
        ]
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12].upper()
    return f"FKA-{digest}"


def build_alert_rows(rows: Sequence[Dict[str, Any]], latest_run_id: str) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen_alert_ids: set[str] = set()
    for row in rows:
        alert_id = normalize_text(row.get("Alert_ID", "")) or stable_alert_id(row)
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        if alert_id in seen_alert_ids:
            continue
        seen_alert_ids.add(alert_id)
        output.append(
            {
                "Run_ID": normalize_text(row.get("Run_ID", "")) or latest_run_id,
                "Alert_ID": alert_id,
                "FSN": fsn,
                "SKU_ID": safe_metric_text(row, "SKU_ID"),
                "Product_Title": safe_metric_text(row, "Product_Title"),
                "Alert_Type": safe_metric_text(row, "Alert_Type"),
                "Severity": safe_metric_text(row, "Severity"),
                "Suggested_Action": safe_metric_text(row, "Suggested_Action"),
                "Reason": safe_metric_text(row, "Reason"),
                "Data_Confidence": safe_metric_text(row, "Data_Confidence"),
                "Status_Default": safe_metric_text(row, "Status_Default") or "Open",
                "Last_Updated": safe_metric_text(row, "Last_Updated") or now_iso(),
            }
        )
    return output


def stable_action_id(alert_id: str) -> str:
    digest = hashlib.sha1(normalize_text(alert_id).encode("utf-8")).hexdigest()[:12].upper()
    return f"FKACTION-{digest}"


def build_action_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen_action_ids: set[str] = set()
    for row in rows:
        action_id = normalize_text(row.get("Action_ID", ""))
        alert_id = normalize_text(row.get("Alert_ID", ""))
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        if not action_id:
            action_id = stable_action_id(alert_id or fsn)
        if action_id in seen_action_ids:
            continue
        seen_action_ids.add(action_id)
        output.append(
            {
                "Action_ID": action_id,
                "Alert_ID": alert_id,
                "FSN": fsn,
                "SKU_ID": safe_metric_text(row, "SKU_ID"),
                "Product_Title": safe_metric_text(row, "Product_Title"),
                "Alert_Type": safe_metric_text(row, "Alert_Type"),
                "Severity": safe_metric_text(row, "Severity"),
                "Owner": safe_metric_text(row, "Owner"),
                "Status": safe_metric_text(row, "Status"),
                "Action_Taken": safe_metric_text(row, "Action_Taken"),
                "Action_Date": safe_metric_text(row, "Action_Date"),
                "Expected_Impact": safe_metric_text(row, "Expected_Impact"),
                "Review_After_Date": safe_metric_text(row, "Review_After_Date"),
                "Resolution_Notes": safe_metric_text(row, "Resolution_Notes"),
                "Last_Updated": safe_metric_text(row, "Last_Updated") or now_iso(),
            }
        )
    return output


def build_ads_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen_fsns: set[str] = set()
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in seen_fsns:
            continue
        seen_fsns.add(fsn)
        output.append(
            {
                "FSN": fsn,
                "SKU_ID": safe_metric_text(row, "SKU_ID"),
                "Product_Title": safe_metric_text(row, "Product_Title"),
                "Final_Product_Type": safe_metric_text(row, "Final_Product_Type", "Detected_Product_Type"),
                "Final_Seasonality_Tag": safe_metric_text(row, "Final_Seasonality_Tag", "Detected_Seasonality_Tag"),
                "Ad_Run_Type": safe_metric_text(row, "Ad_Run_Type"),
                "Current_Ad_Status": safe_metric_text(row, "Current_Ad_Status"),
                "Ad_ROAS": safe_metric_text(row, "Ad_ROAS", "ROAS"),
                "Ad_ACOS": safe_metric_text(row, "Ad_ACOS", "ACOS"),
                "Ad_Revenue": safe_metric_text(row, "Ad_Revenue", "Ads_Revenue"),
                "Estimated_Ad_Spend": safe_metric_text(row, "Estimated_Ad_Spend"),
                "Final_Ads_Decision": safe_metric_text(row, "Final_Ads_Decision", "Suggested_Ad_Action"),
                "Final_Budget_Recommendation": safe_metric_text(row, "Final_Budget_Recommendation", "Suggested_Budget_Level"),
                "Ads_Risk_Level": safe_metric_text(row, "Ads_Risk_Level"),
                "Ads_Opportunity_Level": safe_metric_text(row, "Ads_Opportunity_Level"),
                "Ads_Decision_Reason": safe_metric_text(row, "Ads_Decision_Reason", "Reason"),
                "Last_Updated": safe_metric_text(row, "Last_Updated") or now_iso(),
            }
        )
    return output


def build_returns_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen_fsns: set[str] = set()
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in seen_fsns:
            continue
        seen_fsns.add(fsn)
        output.append(
            {
                "FSN": fsn,
                "SKU_ID": safe_metric_text(row, "SKU_ID"),
                "Product_Title": safe_metric_text(row, "Product_Title"),
                "Total_Returns_In_Detailed_Report": safe_metric_text(row, "Total_Returns_In_Detailed_Report"),
                "Top_Issue_Category": safe_metric_text(row, "Top_Issue_Category"),
                "Top_Return_Reason": safe_metric_text(row, "Top_Return_Reason"),
                "Top_Return_Sub_Reason": safe_metric_text(row, "Top_Return_Sub_Reason"),
                "Critical_Issue_Count": safe_metric_text(row, "Critical_Issue_Count"),
                "High_Issue_Count": safe_metric_text(row, "High_Issue_Count"),
                "Product_Issue_Count": safe_metric_text(row, "Product_Issue_Count"),
                "Logistics_Issue_Count": safe_metric_text(row, "Logistics_Issue_Count"),
                "Customer_RTO_Count": safe_metric_text(row, "Customer_RTO_Count"),
                "Suggested_Return_Action": safe_metric_text(row, "Suggested_Return_Action"),
                "Return_Action_Priority": safe_metric_text(row, "Return_Action_Priority"),
                "Last_Updated": safe_metric_text(row, "Last_Updated") or now_iso(),
            }
        )
    return output


def build_listings_rows(
    presence_rows: Sequence[Dict[str, Any]],
    missing_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    missing_lookup = build_index(missing_rows, key_field="FSN")
    output: List[Dict[str, Any]] = []
    seen_fsns: set[str] = set()
    for row in presence_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in seen_fsns:
            continue
        seen_fsns.add(fsn)
        missing_row = missing_lookup.get(fsn, {})
        output.append(
            {
                "FSN": fsn,
                "SKU_ID": safe_metric_text(row, "SKU_ID"),
                "Product_Title": safe_metric_text(row, "Product_Title"),
                "Found_In_Active_Listing": safe_metric_text(row, "Found_In_Active_Listing"),
                "Listing_Presence_Status": safe_metric_text(row, "Listing_Presence_Status"),
                "Possible_Issue": safe_metric_text(row, "Possible_Issue"),
                "Suggested_Action": safe_metric_text(row, "Suggested_Action")
                or safe_metric_text(missing_row, "Suggested_Action"),
                "Priority": safe_metric_text(missing_row, "Priority") or ("Low" if safe_metric_text(row, "Found_In_Active_Listing") in {"Yes", "TRUE"} else "Medium"),
                "Last_Updated": safe_metric_text(row, "Last_Updated") or safe_metric_text(missing_row, "Last_Updated") or now_iso(),
            }
        )
    return output


def write_output_tab(
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


def create_looker_studio_sources() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in SOURCE_TABS:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)

    dashboard_data_headers, dashboard_data_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_DASHBOARD_DATA")
    analysis_headers, analysis_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_SKU_ANALYSIS")
    alerts_headers, alerts_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_ALERTS_GENERATED")
    active_tasks_headers, active_tasks_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_ACTIVE_TASKS")
    tracker_headers, tracker_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_ACTION_TRACKER")
    ads_headers, ads_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_ADS_PLANNER")
    return_headers, return_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_RETURN_ISSUE_SUMMARY")
    listing_headers, listing_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_LISTING_PRESENCE")
    missing_listing_headers, missing_listing_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_MISSING_ACTIVE_LISTINGS")
    run_history_headers, run_history_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_RUN_HISTORY")
    fsn_history_headers, fsn_history_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_FSN_HISTORY")

    latest_run_row = get_latest_run_row(run_history_rows)
    latest_run_id = latest_text_value(latest_run_row, "Run_ID")
    report_date = latest_text_value(latest_run_row, "Report_End_Date", "Run_Date") or datetime.now().date().isoformat()

    analysis_lookup = build_index(analysis_rows, key_field="FSN")
    listing_lookup = build_index(listing_rows, key_field="FSN")
    missing_listing_lookup = build_index(missing_listing_rows, key_field="FSN")
    ads_lookup = build_index(ads_rows, key_field="FSN")
    return_lookup = build_index(return_rows, key_field="FSN")
    dashboard_lookup = build_dashboard_lookup(dashboard_data_rows)

    analysis_fsns = [clean_fsn(row.get("FSN", "")) for row in analysis_rows if clean_fsn(row.get("FSN", ""))]
    total_target_fsns = len(dict.fromkeys(analysis_fsns))
    total_alerts = len([row for row in alerts_rows if normalize_text(row.get("Alert_ID", "")) or clean_fsn(row.get("FSN", ""))])
    critical_alerts = count_severity(alerts_rows, "Critical")
    high_alerts = count_severity(alerts_rows, "High")
    active_tasks = len([row for row in active_tasks_rows if normalize_text(row.get("Alert_ID", "")) or clean_fsn(row.get("FSN", ""))])
    missing_cogs = sum(1 for row in analysis_rows if clean_fsn(row.get("FSN", "")) and not is_cogs_available(row))
    missing_active_listings = len([row for row in missing_listing_rows if clean_fsn(row.get("FSN", ""))])
    ads_ready_count = count_ads_ready(ads_rows)
    return_issue_fsns = len([row for row in return_rows if clean_fsn(row.get("FSN", ""))])
    cogs_available, cogs_missing = count_cogs_rows(analysis_rows)
    cogs_completion_percent = round((cogs_available / (cogs_available + cogs_missing)) * 100, 2) if (cogs_available + cogs_missing) else 0.0

    final_profit = 0.0
    for row in analysis_rows:
        if not clean_fsn(row.get("FSN", "")):
            continue
        value = normalize_text(row.get("Final_Net_Profit", ""))
        if value:
            final_profit += parse_float(value)
            continue
        total_cogs_value = normalize_text(row.get("Total_COGS", ""))
        if total_cogs_value:
            final_profit += parse_float(row.get("Net_Profit_Before_COGS", "")) - parse_float(total_cogs_value)

    context = {
        "run_id": latest_run_id,
        "report_date": report_date,
        "total_target_fsns": total_target_fsns or len(analysis_lookup),
        "final_profit": final_profit,
        "total_alerts": total_alerts,
        "critical_alerts": critical_alerts,
        "high_alerts": high_alerts,
        "active_tasks": active_tasks,
        "missing_cogs": missing_cogs,
        "missing_active_listings": missing_active_listings,
        "ads_ready_count": ads_ready_count,
        "return_issue_fsns": return_issue_fsns,
        "cogs_completion_percent": cogs_completion_percent,
    }

    executive_rows = build_executive_summary_rows(context, dashboard_lookup)
    fsn_metric_rows = build_fsn_metrics_rows(
        analysis_rows,
        listing_lookup,
        missing_listing_lookup,
        ads_lookup,
        return_lookup,
        latest_run_id,
    )
    alert_rows = build_alert_rows(alerts_rows, latest_run_id)
    action_rows = build_action_rows(tracker_rows)
    ads_source_rows = build_ads_rows(ads_rows)
    return_source_rows = build_returns_rows(return_rows)
    listing_source_rows = build_listings_rows(listing_rows, missing_listing_rows)

    output_payloads = {
        LOOKER_EXECUTIVE_TAB: executive_rows,
        LOOKER_FSN_METRICS_TAB: fsn_metric_rows,
        LOOKER_ALERTS_TAB: alert_rows,
        LOOKER_ACTIONS_TAB: action_rows,
        LOOKER_ADS_TAB: ads_source_rows,
        LOOKER_RETURNS_TAB: return_source_rows,
        LOOKER_LISTINGS_TAB: listing_source_rows,
    }

    for tab_name, rows in output_payloads.items():
        write_output_tab(sheets_service, spreadsheet_id, tab_name, LOOKER_HEADERS[tab_name], rows)

    result = {
        "status": "SUCCESS",
        "spreadsheet_id": spreadsheet_id,
        "run_id": latest_run_id,
        "report_date": report_date,
        "tabs_updated": LOOKER_TABS,
        "row_counts": {tab_name: len(rows) for tab_name, rows in output_payloads.items()},
        "source_tabs_checked": SOURCE_TABS,
        "log_path": str(LOG_PATH),
    }

    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "run_id",
            "report_date",
            "executive_rows",
            "fsn_metric_rows",
            "alert_rows",
            "action_rows",
            "ads_rows",
            "return_rows",
            "listing_rows",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "spreadsheet_id": spreadsheet_id,
                "run_id": latest_run_id,
                "report_date": report_date,
                "executive_rows": len(executive_rows),
                "fsn_metric_rows": len(fsn_metric_rows),
                "alert_rows": len(alert_rows),
                "action_rows": len(action_rows),
                "ads_rows": len(ads_source_rows),
                "return_rows": len(return_source_rows),
                "listing_rows": len(listing_source_rows),
                "status": "SUCCESS",
                "message": "Rebuilt Looker Studio source tabs for Flipkart",
            }
        ],
    )

    print(json.dumps(build_status_payload("SUCCESS", **{k: v for k, v in result.items() if k != "status"}), indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_looker_studio_sources()
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
