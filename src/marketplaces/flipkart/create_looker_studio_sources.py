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
from src.marketplaces.flipkart.flipkart_cogs_helpers import COGS_AVAILABLE_STATUSES, count_cogs_rows, get_usable_cogs, is_cogs_available
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
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
    "FLIPKART_RETURN_ALL_DETAILS",
    "FLIPKART_CUSTOMER_RETURN_COMMENTS",
    "FLIPKART_COURIER_RETURN_COMMENTS",
    "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY",
    "FLIPKART_COURIER_RETURN_SUMMARY",
    "FLIPKART_RETURN_TYPE_PIVOT",
    "FLIPKART_LISTING_PRESENCE",
    "FLIPKART_MISSING_ACTIVE_LISTINGS",
    "FLIPKART_RUN_HISTORY",
    "FLIPKART_FSN_HISTORY",
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
    "FLIPKART_ORDER_ITEM_EXPLORER",
    "FLIPKART_ORDER_ITEM_MASTER",
    "FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
]

LOOKER_EXECUTIVE_TAB = "LOOKER_FLIPKART_EXECUTIVE_SUMMARY"
LOOKER_FSN_METRICS_TAB = "LOOKER_FLIPKART_FSN_METRICS"
LOOKER_ALERTS_TAB = "LOOKER_FLIPKART_ALERTS"
LOOKER_ACTIONS_TAB = "LOOKER_FLIPKART_ACTIONS"
LOOKER_ADS_TAB = "LOOKER_FLIPKART_ADS"
LOOKER_RETURNS_TAB = "LOOKER_FLIPKART_RETURNS"
LOOKER_RETURN_ALL_DETAILS_TAB = "LOOKER_FLIPKART_RETURN_ALL_DETAILS"
LOOKER_CUSTOMER_RETURNS_TAB = "LOOKER_FLIPKART_CUSTOMER_RETURNS"
LOOKER_COURIER_RETURNS_TAB = "LOOKER_FLIPKART_COURIER_RETURNS"
LOOKER_RETURN_TYPE_PIVOT_TAB = "LOOKER_FLIPKART_RETURN_TYPE_PIVOT"
LOOKER_LISTINGS_TAB = "LOOKER_FLIPKART_LISTINGS"
LOOKER_ORDER_ITEM_EXPLORER_TAB = "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER"
LOOKER_ORDER_ITEM_MASTER_TAB = "LOOKER_FLIPKART_ORDER_ITEM_MASTER"
LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB = "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL"
LOOKER_ADJUSTED_PROFIT_TAB = "LOOKER_FLIPKART_ADJUSTED_PROFIT"
LOOKER_REPORT_FORMAT_MONITOR_TAB = "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR"
LOOKER_RUN_QUALITY_TAB = "LOOKER_FLIPKART_RUN_QUALITY_SCORE"
LOOKER_MODULE_CONFIDENCE_TAB = "LOOKER_FLIPKART_MODULE_CONFIDENCE"
LOOKER_DEMAND_PROFILE_TAB = "LOOKER_FLIPKART_DEMAND_PROFILE"
LOOKER_COMPETITOR_INTELLIGENCE_TAB = "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE"
LOOKER_RUN_COMPARISON_TAB = "LOOKER_FLIPKART_RUN_COMPARISON"

LOOKER_TABS = [
    LOOKER_EXECUTIVE_TAB,
    LOOKER_FSN_METRICS_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_RETURN_ALL_DETAILS_TAB,
    LOOKER_CUSTOMER_RETURNS_TAB,
    LOOKER_COURIER_RETURNS_TAB,
    LOOKER_RETURN_TYPE_PIVOT_TAB,
    LOOKER_LISTINGS_TAB,
    LOOKER_ORDER_ITEM_EXPLORER_TAB,
    LOOKER_ORDER_ITEM_MASTER_TAB,
    LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB,
    LOOKER_RUN_COMPARISON_TAB,
    LOOKER_ADJUSTED_PROFIT_TAB,
    LOOKER_REPORT_FORMAT_MONITOR_TAB,
    LOOKER_RUN_QUALITY_TAB,
    LOOKER_MODULE_CONFIDENCE_TAB,
    LOOKER_DEMAND_PROFILE_TAB,
    LOOKER_COMPETITOR_INTELLIGENCE_TAB,
]

LOOKER_LARGE_TABS = [
    "LOOKER_FLIPKART_RETURN_ALL_DETAILS",
    "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER",
    "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
]

LOOKER_GROUP_TABS = {
    "core": [
        LOOKER_EXECUTIVE_TAB,
        LOOKER_FSN_METRICS_TAB,
        LOOKER_ALERTS_TAB,
        LOOKER_ACTIONS_TAB,
        LOOKER_ADS_TAB,
        LOOKER_RETURNS_TAB,
        LOOKER_LISTINGS_TAB,
    ],
    "ads": [
        LOOKER_ADS_TAB,
        LOOKER_EXECUTIVE_TAB,
        LOOKER_FSN_METRICS_TAB,
    ],
    "returns": [
        LOOKER_RETURNS_TAB,
        LOOKER_RETURN_TYPE_PIVOT_TAB,
        LOOKER_CUSTOMER_RETURNS_TAB,
        LOOKER_COURIER_RETURNS_TAB,
        LOOKER_EXECUTIVE_TAB,
        LOOKER_FSN_METRICS_TAB,
    ],
    "orders": [
        LOOKER_ORDER_ITEM_MASTER_TAB,
        LOOKER_EXECUTIVE_TAB,
    ],
    "cogs": [
        LOOKER_ADJUSTED_PROFIT_TAB,
        LOOKER_FSN_METRICS_TAB,
        LOOKER_ADS_TAB,
        LOOKER_EXECUTIVE_TAB,
    ],
    "quality": [
        LOOKER_RUN_QUALITY_TAB,
        LOOKER_MODULE_CONFIDENCE_TAB,
        LOOKER_REPORT_FORMAT_MONITOR_TAB,
        LOOKER_EXECUTIVE_TAB,
    ],
    "competitor": [
        LOOKER_COMPETITOR_INTELLIGENCE_TAB,
        LOOKER_ADS_TAB,
        LOOKER_EXECUTIVE_TAB,
    ],
}

LOOKER_GROUP_TABS["light"] = [
    LOOKER_EXECUTIVE_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_LISTINGS_TAB,
]
LOOKER_GROUP_TABS["full"] = list(LOOKER_TABS)

LOOKER_REFRESH_MANIFEST_PATH = OUTPUT_DIR / "looker_refresh_manifest.json"

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
        "Customer_Return_Count",
        "Courier_Return_Count",
        "Unknown_Return_Count",
        "Total_Return_Count",
        "Customer_Return_Rate",
        "Courier_Return_Rate",
        "Total_Return_Rate",
        "Net_Settlement",
        "Flipkart_Net_Earnings",
        "Net_Profit_Before_COGS",
        "Cost_Price",
        "Total_Unit_COGS",
        "Derived_Total_Unit_COGS",
        "Total_COGS",
        "Final_Net_Profit",
        "Final_Profit_Margin",
        "COGS_Status",
        "COGS_Source",
        "COGS_Data_Source",
        "COGS_Missing_Reason",
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
        "Customer_Return_Rate",
        "Courier_Return_Rate",
        "Total_Return_Rate",
        "Last_Updated",
    ],
    LOOKER_RETURNS_TAB: [
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Total_Returns_In_Detailed_Report",
        "Customer_Return_Count",
        "Courier_Return_Count",
        "Unknown_Return_Count",
        "Customer_Return_Rate",
        "Courier_Return_Rate",
        "Total_Return_Rate",
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
    LOOKER_RETURN_ALL_DETAILS_TAB: [
        "Run_ID",
        "Return_ID",
        "Order_ID",
        "Order_Item_ID",
        "Return_Type",
        "Return_Bucket",
        "SKU_ID",
        "FSN",
        "Product_Title",
        "Return_Status",
        "Return_Result",
        "Return_Reason",
        "Return_Sub_Reason",
        "Comments",
        "Tracking_ID",
        "Reverse_Logistics_Tracking_ID",
        "Return_Requested_Date",
        "Return_Approval_Date",
        "Return_Completion_Date",
        "Quantity",
        "Source_File",
        "Last_Updated",
    ],
    LOOKER_CUSTOMER_RETURNS_TAB: [
        "Run_ID",
        "Return_ID",
        "Order_ID",
        "Order_Item_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Return_Reason",
        "Return_Sub_Reason",
        "Comments",
        "Customer_Issue_Category",
        "Customer_Issue_Severity",
        "Priority",
        "Suggested_Action",
        "Source_File",
        "Last_Updated",
    ],
    LOOKER_COURIER_RETURNS_TAB: [
        "Run_ID",
        "Return_ID",
        "Order_ID",
        "Order_Item_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Return_Reason",
        "Return_Sub_Reason",
        "Comments",
        "Courier_Issue_Category",
        "Courier_Issue_Severity",
        "Priority",
        "Suggested_Action",
        "Source_File",
        "Last_Updated",
    ],
    LOOKER_RETURN_TYPE_PIVOT_TAB: [
        "Run_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Sold_Order_Items",
        "Customer_Return_Count",
        "Courier_Return_Count",
        "Unknown_Return_Count",
        "Total_Return_Count",
        "Customer_Return_Rate",
        "Courier_Return_Rate",
        "Total_Return_Rate",
        "Customer_vs_Courier_Mix",
        "Dominant_Return_Type",
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
    LOOKER_ORDER_ITEM_EXPLORER_TAB: [
        "Run_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Order_ID",
        "Order_Item_ID",
        "Order_Date",
        "Dispatch_Date",
        "Delivery_Date",
        "Quantity",
        "Selling_Price",
        "Settlement_Amount",
        "Commission",
        "Shipping_Fee",
        "Other_Fees",
        "Total_Deductions",
        "Cost_Price",
        "COGS",
        "Net_Profit",
        "Profit_Margin",
        "Return_Status",
        "Return_ID",
        "Return_Date",
        "Return_Reason",
        "Return_Sub_Reason",
        "Return_Issue_Category",
        "Alert_Count",
        "Critical_Alert_Count",
        "Final_Ads_Decision",
        "Competition_Risk_Level",
        "Data_Gap_Reason",
        "Source_File",
        "Last_Updated",
    ],
    LOOKER_ORDER_ITEM_MASTER_TAB: [
        "Run_ID",
        "Order_ID",
        "Order_Item_ID",
        "Master_Order_Key",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Order_Date",
        "Latest_Event_Date",
        "Quantity",
        "Selling_Price",
        "Settlement_Amount",
        "Total_Deductions",
        "Cost_Price",
        "COGS",
        "Net_Profit",
        "Profit_Margin",
        "Return_YN",
        "Return_IDs",
        "Return_Type_Final",
        "Customer_Return_YN",
        "Courier_Return_YN",
        "Return_Status_Final",
        "Return_Reason_Final",
        "Return_Sub_Reason_Final",
        "Customer_Issue_Category",
        "Courier_Issue_Category",
        "Customer_Return_Risk_Level",
        "Courier_Return_Risk_Level",
        "Alert_Count",
        "Critical_Alert_Count",
        "Final_Ads_Decision",
        "Competition_Risk_Level",
        "Source_Row_Count",
        "Sources_Present",
        "Data_Completeness_Status",
        "Data_Gap_Reason",
        "Last_Updated",
    ],
    LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB: [
        "Run_ID",
        "Source_File",
        "Source_Tab",
        "Source_Row_Type",
        "Order_ID",
        "Order_Item_ID",
        "Return_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Order_Date",
        "Dispatch_Date",
        "Delivery_Date",
        "Settlement_Date",
        "Return_Date",
        "Quantity",
        "Selling_Price",
        "Settlement_Amount",
        "Commission",
        "Shipping_Fee",
        "Other_Fees",
        "Total_Deductions",
        "Cost_Price",
        "COGS",
        "Net_Profit",
        "Profit_Margin",
        "Return_Type",
        "Customer_Return_YN",
        "Courier_Return_YN",
        "Return_Status",
        "Return_Reason",
        "Return_Sub_Reason",
        "Customer_Issue_Category",
        "Courier_Issue_Category",
        "Customer_Return_Risk_Level",
        "Courier_Return_Risk_Level",
        "Alert_Count",
        "Critical_Alert_Count",
        "Final_Ads_Decision",
        "Competition_Risk_Level",
        "Data_Gap_Reason",
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


def retry(func: Callable[[], Any], attempts: int = 3) -> Any:
    delays = (5, 15, 30)
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in {429, 500, 502, 503} or attempt == attempts:
                raise
            import time

            delay = delays[min(attempt - 1, len(delays) - 1)]
            time.sleep(delay)


def _is_quota_limited_http_error(exc: HttpError) -> bool:
    message = str(exc).lower()
    status = getattr(exc.resp, "status", None)
    return status == 429 or "quota" in message or "rate_limit" in message or "rate limit" in message


def _normalize_manifest_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_normalize_manifest_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_manifest_value(item) for key, item in value.items()}
    return value


def _load_looker_refresh_manifest() -> Dict[str, Any]:
    if not LOOKER_REFRESH_MANIFEST_PATH.exists():
        return {}
    try:
        return json.loads(LOOKER_REFRESH_MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_looker_refresh_manifest(payload: Dict[str, Any]) -> None:
    LOOKER_REFRESH_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOOKER_REFRESH_MANIFEST_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _hash_looker_rows(headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> str:
    normalized_rows = [[normalize_text(row.get(header, "")) for header in headers] for row in rows]
    payload = {"headers": list(headers), "rows": normalized_rows}
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()


def _build_looker_refresh_manifest_entry(
    *,
    tab_name: str,
    source_tab_names: Sequence[str],
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "tab_name": tab_name,
        "source_tab_names": list(dict.fromkeys(source_tab_names)),
        "row_count": len(rows),
        "column_count": len(headers),
        "content_hash": _hash_looker_rows(headers, rows),
        "last_written_at": now_iso(),
    }


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


def resolve_cogs_fields(row: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, str, str]:
    cogs_snapshot = get_usable_cogs(row)
    cost_price = safe_metric_text(cogs_snapshot, "Cost_Price")
    total_unit_cogs = safe_metric_text(cogs_snapshot, "Total_Unit_COGS")
    derived_total_unit_cogs = safe_metric_text(cogs_snapshot, "Derived_Total_Unit_COGS")
    total_cogs = safe_metric_text(row, "Total_COGS")
    final_net_profit = safe_metric_text(row, "Final_Net_Profit")
    cogs_status = safe_metric_text(cogs_snapshot, "COGS_Status")
    cogs_source = safe_metric_text(cogs_snapshot, "COGS_Source")
    cogs_data_source = safe_metric_text(cogs_snapshot, "COGS_Data_Source")
    cogs_missing_reason = safe_metric_text(cogs_snapshot, "COGS_Missing_Reason")

    if not total_cogs and total_unit_cogs:
        total_cogs = normalize_number_text(parse_float(total_unit_cogs) * parse_float(row.get("Units_Sold", "")), 2)

    if not final_net_profit and total_cogs:
        final_net_profit = normalize_number_text(parse_float(row.get("Net_Profit_Before_COGS", "")) - parse_float(total_cogs), 2)

    return cost_price, total_unit_cogs, derived_total_unit_cogs, total_cogs, final_net_profit, cogs_status, cogs_source or cogs_data_source, cogs_missing_reason


def build_fsn_metrics_rows(
    analysis_rows: Sequence[Dict[str, Any]],
    listing_lookup: Dict[str, Dict[str, Any]],
    missing_listing_lookup: Dict[str, Dict[str, Any]],
    ads_lookup: Dict[str, Dict[str, Any]],
    return_lookup: Dict[str, Dict[str, Any]],
    customer_return_lookup: Dict[str, Dict[str, Any]],
    courier_return_lookup: Dict[str, Dict[str, Any]],
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
        customer_return_row = customer_return_lookup.get(fsn, {})
        courier_return_row = courier_return_lookup.get(fsn, {})

        cost_price, total_unit_cogs, derived_total_unit_cogs, total_cogs, final_net_profit, cogs_status, cogs_source_summary, cogs_missing_reason = resolve_cogs_fields(analysis_row)
        final_profit_margin = safe_metric_text(analysis_row, "Final_Profit_Margin")
        gross_sales_value = parse_float(analysis_row.get("Gross_Sales", ""))
        if not final_profit_margin and final_net_profit and gross_sales_value > 0:
            final_profit_margin = normalize_number_text(parse_float(final_net_profit) / gross_sales_value, 4)

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
                "Customer_Return_Count": safe_metric_text(customer_return_row, "Customer_Return_Count"),
                "Courier_Return_Count": safe_metric_text(courier_return_row, "Courier_Return_Count"),
                "Unknown_Return_Count": safe_metric_text(return_row, "Unknown_Return_Count") or safe_metric_text(analysis_row, "Unknown_Return_Count"),
                "Total_Return_Count": safe_metric_text(return_row, "Total_Return_Count") or safe_metric_text(analysis_row, "Total_Return_Count"),
                "Customer_Return_Rate": safe_metric_text(customer_return_row, "Customer_Return_Rate"),
                "Courier_Return_Rate": safe_metric_text(courier_return_row, "Courier_Return_Rate"),
                "Total_Return_Rate": safe_metric_text(return_row, "Total_Return_Rate") or safe_metric_text(analysis_row, "Total_Return_Rate"),
                "Net_Settlement": safe_metric_text(analysis_row, "Net_Settlement"),
                "Flipkart_Net_Earnings": safe_metric_text(analysis_row, "Flipkart_Net_Earnings"),
                "Net_Profit_Before_COGS": safe_metric_text(analysis_row, "Net_Profit_Before_COGS"),
                "Cost_Price": cost_price,
                "Total_Unit_COGS": total_unit_cogs,
                "Derived_Total_Unit_COGS": safe_metric_text(analysis_row, "Derived_Total_Unit_COGS") or derived_total_unit_cogs,
                "Total_COGS": total_cogs,
                "Final_Net_Profit": final_net_profit,
                "Final_Profit_Margin": final_profit_margin,
                "COGS_Status": cogs_status or ("Entered" if normalize_text(total_unit_cogs) else "Missing"),
                "COGS_Source": safe_metric_text(analysis_row, "COGS_Source") or cogs_source_summary,
                "COGS_Data_Source": safe_metric_text(analysis_row, "COGS_Data_Source") or cogs_source_summary,
                "COGS_Missing_Reason": safe_metric_text(analysis_row, "COGS_Missing_Reason") or cogs_missing_reason,
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
                "Customer_Return_Rate": safe_metric_text(row, "Customer_Return_Rate"),
                "Courier_Return_Rate": safe_metric_text(row, "Courier_Return_Rate"),
                "Total_Return_Rate": safe_metric_text(row, "Total_Return_Rate"),
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
                "Customer_Return_Count": safe_metric_text(row, "Customer_Return_Count"),
                "Courier_Return_Count": safe_metric_text(row, "Courier_Return_Count"),
                "Unknown_Return_Count": safe_metric_text(row, "Unknown_Return_Count"),
                "Customer_Return_Rate": safe_metric_text(row, "Customer_Return_Rate"),
                "Courier_Return_Rate": safe_metric_text(row, "Courier_Return_Rate"),
                "Total_Return_Rate": safe_metric_text(row, "Total_Return_Rate"),
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


def _copy_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows if any(normalize_text(value) for value in row.values())]


def _headers_union(*header_groups: Sequence[str]) -> List[str]:
    headers: List[str] = []
    for group in header_groups:
        for header in group:
            if header not in headers:
                headers.append(header)
    return headers


def _cache_status_summary(cache_rows: Sequence[Dict[str, Any]]) -> Tuple[str, int, int, int, str]:
    counter = Counter()
    latest_refreshed = ""
    for row in cache_rows:
        status = normalize_text(row.get("Cache_Status", "")).upper() or "UNKNOWN"
        counter[status] += 1
        refreshed = normalize_text(row.get("Last_Refreshed", ""))
        if refreshed and refreshed > latest_refreshed:
            latest_refreshed = refreshed
    pending_count = counter.get("PENDING", 0)
    success_count = counter.get("SUCCESS", 0)
    failed_count = sum(counter.get(status, 0) for status in counter if status not in {"PENDING", "SUCCESS"})
    if cache_rows:
        parts = [f"{status}:{count}" for status, count in sorted(counter.items())]
        summary = " | ".join(parts)
    else:
        summary = "No keyword cache rows yet"
    return summary, pending_count, success_count, failed_count, latest_refreshed


def build_demand_profile_looker_rows(
    demand_headers: Sequence[str],
    demand_rows: Sequence[Dict[str, Any]],
    cache_rows: Sequence[Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]]]:
    headers = _headers_union(
        demand_headers or [
            "Product_Type",
            "Seasonality_Tag",
            "Peak_Months",
            "Prep_Start_Days_Before_Peak",
            "Ads_Start_Days_Before_Peak",
            "Total_Avg_Monthly_Searches",
            "Demand_Stability",
            "Seasonality_Score",
            "Current_Month_Demand_Index",
            "Next_45_Days_Demand_Status",
            "Demand_Confidence",
            "Demand_Source",
            "Recommended_Ad_Window",
            "Remarks",
            "Last_Updated",
        ],
        [
            "Keyword_Count",
            "Cache_Status_Summary",
            "Cache_Pending_Count",
            "Cache_Success_Count",
            "Cache_Failed_Count",
            "Cache_Last_Refreshed",
        ],
    )
    grouped_cache: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in cache_rows:
        product_type = normalize_text(row.get("Product_Type", ""))
        if product_type:
            grouped_cache[product_type].append(dict(row))

    output_rows: List[Dict[str, Any]] = []
    for row in demand_rows:
        merged = dict(row)
        product_type = normalize_text(row.get("Product_Type", ""))
        related_cache = grouped_cache.get(product_type, [])
        summary, pending_count, success_count, failed_count, latest_refreshed = _cache_status_summary(related_cache)
        merged.update(
            {
                "Keyword_Count": str(len(related_cache)),
                "Cache_Status_Summary": summary,
                "Cache_Pending_Count": str(pending_count),
                "Cache_Success_Count": str(success_count),
                "Cache_Failed_Count": str(failed_count),
                "Cache_Last_Refreshed": latest_refreshed,
            }
        )
        output_rows.append(merged)

    return headers, output_rows


def build_run_quality_looker_rows(
    score_headers: Sequence[str],
    score_rows: Sequence[Dict[str, Any]],
    breakdown_headers: Sequence[str],
    breakdown_rows: Sequence[Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]]]:
    headers = _headers_union(
        [
            "Record_Type",
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
        ],
        score_headers,
        breakdown_headers,
    )
    summary_rows = [dict(row) for row in score_rows if any(normalize_text(value) for value in row.values())]
    breakdown_source_rows = [dict(row) for row in breakdown_rows if any(normalize_text(value) for value in row.values())]
    latest_summary = dict(summary_rows[-1]) if summary_rows else {}

    output_rows: List[Dict[str, Any]] = []
    for row in summary_rows:
        output_rows.append(
            {
                "Record_Type": "Summary",
                "Report_Date": normalize_text(row.get("Report_Date", "")),
                "Run_ID": normalize_text(row.get("Run_ID", "")),
                "Overall_Run_Quality_Score": normalize_text(row.get("Overall_Run_Quality_Score", "")),
                "Run_Quality_Grade": normalize_text(row.get("Run_Quality_Grade", "")),
                "Decision_Recommendation": normalize_text(row.get("Decision_Recommendation", "")),
                "Suggested_Action": normalize_text(row.get("Suggested_Action", "")),
                "Last_Updated": normalize_text(row.get("Last_Updated", "")),
            }
        )

    for row in breakdown_source_rows:
        output_rows.append(
            {
                "Record_Type": "Breakdown",
                "Report_Date": normalize_text(latest_summary.get("Report_Date", "")),
                "Run_ID": normalize_text(row.get("Run_ID", "")) or normalize_text(latest_summary.get("Run_ID", "")),
                "Overall_Run_Quality_Score": normalize_text(latest_summary.get("Overall_Run_Quality_Score", "")),
                "Run_Quality_Grade": normalize_text(latest_summary.get("Run_Quality_Grade", "")),
                "Decision_Recommendation": normalize_text(latest_summary.get("Decision_Recommendation", "")),
                "Score_Category": normalize_text(row.get("Score_Category", "")),
                "Score_Name": normalize_text(row.get("Score_Name", "")),
                "Max_Points": normalize_text(row.get("Max_Points", "")),
                "Points_Earned": normalize_text(row.get("Points_Earned", "")),
                "Status": normalize_text(row.get("Status", "")),
                "Reason": normalize_text(row.get("Reason", "")),
                "Suggested_Action": normalize_text(row.get("Suggested_Action", "")),
                "Last_Updated": normalize_text(row.get("Last_Updated", "")),
            }
        )

    return headers, output_rows


def build_copy_rows(
    primary_tab_name: str,
    fallback_tab_names: Sequence[str],
    sheets_service,
    spreadsheet_id: str,
) -> Tuple[List[str], List[Dict[str, Any]], str]:
    headers, rows, source_tab = read_first_available_table(sheets_service, spreadsheet_id, (primary_tab_name, *fallback_tab_names))
    return headers, _copy_rows(rows), source_tab


def _quota_limited_warning_result(spreadsheet_id: str) -> Dict[str, Any]:
    return {
        "status": "WARNING",
        "spreadsheet_id": spreadsheet_id,
        "message": "Sheets quota exceeded; wait 5 minutes and rerun",
        "next_action": "wait 5 minutes and rerun",
        "warnings": ["Sheets quota exceeded"],
        "tabs_updated": [],
        "row_counts": {},
        "source_tabs_checked": SOURCE_TABS,
        "log_path": str(LOG_PATH),
    }


def create_looker_studio_sources(*, group: str = "light", include_large_tabs: bool = False, force: bool = False) -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    selected_group = normalize_text(group).lower() or "light"
    if selected_group not in LOOKER_GROUP_TABS:
        raise ValueError(f"Unsupported Looker refresh group: {group}")

    requested_tabs = list(LOOKER_GROUP_TABS[selected_group])
    refresh_tabs = list(LOOKER_TABS if selected_group == "full" else requested_tabs)
    if include_large_tabs and selected_group != "full":
        for tab_name in LOOKER_LARGE_TABS:
            if tab_name not in refresh_tabs:
                refresh_tabs.append(tab_name)
    large_tabs_skipped = [tab_name for tab_name in LOOKER_LARGE_TABS if tab_name not in refresh_tabs]
    quota_safe_mode = selected_group != "full" and not force
    needs_return_all_details = LOOKER_RETURN_ALL_DETAILS_TAB in refresh_tabs
    needs_customer_returns = LOOKER_CUSTOMER_RETURNS_TAB in refresh_tabs
    needs_courier_returns = LOOKER_COURIER_RETURNS_TAB in refresh_tabs
    needs_return_type_pivot = LOOKER_RETURN_TYPE_PIVOT_TAB in refresh_tabs
    needs_order_item_tabs = any(
        tab_name in refresh_tabs
        for tab_name in (
            LOOKER_ORDER_ITEM_EXPLORER_TAB,
            LOOKER_ORDER_ITEM_MASTER_TAB,
            LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB,
        )
    )
    needs_adjusted_profit = LOOKER_ADJUSTED_PROFIT_TAB in refresh_tabs
    needs_run_comparison = LOOKER_RUN_COMPARISON_TAB in refresh_tabs
    needs_report_format = LOOKER_REPORT_FORMAT_MONITOR_TAB in refresh_tabs
    needs_module_confidence = LOOKER_MODULE_CONFIDENCE_TAB in refresh_tabs
    needs_competitor = LOOKER_COMPETITOR_INTELLIGENCE_TAB in refresh_tabs
    needs_run_quality = LOOKER_RUN_QUALITY_TAB in refresh_tabs
    needs_demand_profile = LOOKER_DEMAND_PROFILE_TAB in refresh_tabs
    needs_fsn_metrics = LOOKER_FSN_METRICS_TAB in refresh_tabs
    source_tab_cache: Dict[str, Tuple[List[str], List[Dict[str, str]]]] = {}
    source_tabs_read_count = 0
    source_tabs_cache_hits = 0

    def read_table_cached(tab_name: str, *, optional: bool = False) -> Tuple[List[str], List[Dict[str, str]]]:
        nonlocal source_tabs_read_count, source_tabs_cache_hits
        if tab_name in source_tab_cache:
            source_tabs_cache_hits += 1
            return source_tab_cache[tab_name]
        if optional and not tab_exists(sheets_service, spreadsheet_id, tab_name):
            source_tab_cache[tab_name] = ([], [])
            return source_tab_cache[tab_name]
        headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
        source_tab_cache[tab_name] = (headers, rows)
        source_tabs_read_count += 1
        return headers, rows

    def read_first_available_table_cached(tab_names: Sequence[str]) -> Tuple[List[str], List[Dict[str, str]], str]:
        for tab_name in tab_names:
            if not tab_exists(sheets_service, spreadsheet_id, tab_name):
                continue
            headers, rows = read_table_cached(tab_name)
            return headers, rows, tab_name
        return [], [], ""

    def build_copy_rows_cached(primary_tab_name: str, fallback_tab_names: Sequence[str]) -> Tuple[List[str], List[Dict[str, Any]], str]:
        headers, rows, source_tab = read_first_available_table_cached((primary_tab_name, *fallback_tab_names))
        return headers, _copy_rows(rows), source_tab

    def source_ref(*tab_names: str) -> List[str]:
        return [tab_name for tab_name in tab_names if tab_name]

    try:
        for tab_name in SOURCE_TABS:
            ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)

        dashboard_data_headers, dashboard_data_rows = read_table_cached("FLIPKART_DASHBOARD_DATA")
        analysis_headers, analysis_rows = read_table_cached("FLIPKART_SKU_ANALYSIS")
        alerts_headers, alerts_rows = read_table_cached("FLIPKART_ALERTS_GENERATED")
        active_tasks_headers, active_tasks_rows = read_table_cached("FLIPKART_ACTIVE_TASKS")
        tracker_headers, tracker_rows = read_table_cached("FLIPKART_ACTION_TRACKER")
        ads_headers, ads_rows = read_table_cached("FLIPKART_ADS_PLANNER")
        return_headers, return_rows = read_table_cached("FLIPKART_RETURN_ISSUE_SUMMARY")
        if needs_return_all_details:
            return_all_details_headers, return_all_details_rows = read_table_cached("FLIPKART_RETURN_ALL_DETAILS", optional=True)
        else:
            return_all_details_headers, return_all_details_rows = [], []
        if needs_customer_returns:
            customer_return_headers, customer_return_rows = read_table_cached("FLIPKART_CUSTOMER_RETURN_COMMENTS", optional=True)
        else:
            customer_return_headers, customer_return_rows = [], []
        if needs_courier_returns:
            courier_return_headers, courier_return_rows = read_table_cached("FLIPKART_COURIER_RETURN_COMMENTS", optional=True)
        else:
            courier_return_headers, courier_return_rows = [], []
        if needs_fsn_metrics:
            customer_issue_headers, customer_issue_rows = read_table_cached("FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY", optional=True)
            courier_issue_headers, courier_issue_rows = read_table_cached("FLIPKART_COURIER_RETURN_SUMMARY", optional=True)
            return_type_pivot_headers, return_type_pivot_rows = read_table_cached("FLIPKART_RETURN_TYPE_PIVOT", optional=True)
        else:
            customer_issue_headers, customer_issue_rows = [], []
            courier_issue_headers, courier_issue_rows = [], []
            return_type_pivot_headers, return_type_pivot_rows = [], []
        listing_headers, listing_rows = read_table_cached("FLIPKART_LISTING_PRESENCE")
        missing_listing_headers, missing_listing_rows = read_table_cached("FLIPKART_MISSING_ACTIVE_LISTINGS")
        run_history_headers, run_history_rows = read_table_cached("FLIPKART_RUN_HISTORY")
        if needs_order_item_tabs:
            order_item_headers, order_item_rows = read_table_cached("FLIPKART_ORDER_ITEM_EXPLORER")
            order_item_master_headers, order_item_master_rows = read_table_cached("FLIPKART_ORDER_ITEM_MASTER")
            order_item_source_detail_headers, order_item_source_detail_rows = read_table_cached("FLIPKART_ORDER_ITEM_SOURCE_DETAIL")
        else:
            order_item_headers, order_item_rows = [], []
            order_item_master_headers, order_item_master_rows = [], []
            order_item_source_detail_headers, order_item_source_detail_rows = [], []

        latest_run_row = get_latest_run_row(run_history_rows)
        latest_run_id = latest_text_value(latest_run_row, "Run_ID")
        report_date = latest_text_value(latest_run_row, "Report_End_Date", "Run_Date") or datetime.now().date().isoformat()

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
        if needs_fsn_metrics:
            analysis_lookup = build_index(analysis_rows, key_field="FSN")
            listing_lookup = build_index(listing_rows, key_field="FSN")
            missing_listing_lookup = build_index(missing_listing_rows, key_field="FSN")
            ads_lookup = build_index(ads_rows, key_field="FSN")
            return_lookup = build_index(return_type_pivot_rows, key_field="FSN")
            customer_return_lookup = build_index(customer_issue_rows, key_field="FSN")
            courier_return_lookup = build_index(courier_issue_rows, key_field="FSN")
            fsn_metric_rows = build_fsn_metrics_rows(
                analysis_rows,
                listing_lookup,
                missing_listing_lookup,
                ads_lookup,
                return_lookup,
                customer_return_lookup,
                courier_return_lookup,
                latest_run_id,
            )
        else:
            fsn_metric_rows = []
        alert_rows = build_alert_rows(alerts_rows, latest_run_id)
        action_rows = build_action_rows(tracker_rows)
        ads_source_rows = build_ads_rows(ads_rows)
        return_source_rows = build_returns_rows(return_rows)
        return_all_details_source_rows = _copy_rows(return_all_details_rows)
        customer_return_source_rows = _copy_rows(customer_return_rows)
        courier_return_source_rows = _copy_rows(courier_return_rows)
        customer_issue_source_rows = _copy_rows(customer_issue_rows)
        courier_issue_source_rows = _copy_rows(courier_issue_rows)
        return_type_pivot_source_rows = _copy_rows(return_type_pivot_rows)
        listing_source_rows = build_listings_rows(listing_rows, missing_listing_rows)
        if needs_adjusted_profit:
            adjusted_profit_headers, adjusted_profit_rows, adjusted_profit_source_tab = build_copy_rows_cached(
                "FLIPKART_ADJUSTED_PROFIT",
                [LOOKER_ADJUSTED_PROFIT_TAB],
            )
        else:
            adjusted_profit_headers, adjusted_profit_rows, adjusted_profit_source_tab = [], [], ""
        if needs_run_comparison:
            run_comparison_headers, run_comparison_rows, run_comparison_source_tab = build_copy_rows_cached(
                "FLIPKART_RUN_COMPARISON",
                [],
            )
        else:
            run_comparison_headers, run_comparison_rows, run_comparison_source_tab = [], [], ""
        if needs_report_format:
            report_format_headers, report_format_rows, report_format_source_tab = build_copy_rows_cached(
                "FLIPKART_REPORT_FORMAT_MONITOR",
                [LOOKER_REPORT_FORMAT_MONITOR_TAB],
            )
        else:
            report_format_headers, report_format_rows, report_format_source_tab = [], [], ""
        if needs_module_confidence:
            module_confidence_headers, module_confidence_rows, module_confidence_source_tab = build_copy_rows_cached(
                "FLIPKART_MODULE_CONFIDENCE",
                [],
            )
        else:
            module_confidence_headers, module_confidence_rows, module_confidence_source_tab = [], [], ""
        if needs_competitor:
            competitor_headers, competitor_rows, competitor_source_tab = build_copy_rows_cached(
                "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE",
                [],
            )
        else:
            competitor_headers, competitor_rows, competitor_source_tab = [], [], ""
        if needs_demand_profile:
            demand_headers, demand_rows = read_table_cached("PRODUCT_TYPE_DEMAND_PROFILE")
            keyword_cache_headers, keyword_cache_rows = read_table_cached("GOOGLE_KEYWORD_METRICS_CACHE", optional=True)
        else:
            demand_headers, demand_rows = [], []
            keyword_cache_headers, keyword_cache_rows = [], []
        if needs_run_quality:
            run_quality_score_headers, run_quality_score_rows = read_table_cached("FLIPKART_RUN_QUALITY_SCORE", optional=True)
            run_quality_breakdown_headers, run_quality_breakdown_rows = read_table_cached("FLIPKART_RUN_QUALITY_BREAKDOWN", optional=True)
        else:
            run_quality_score_headers, run_quality_score_rows = [], []
            run_quality_breakdown_headers, run_quality_breakdown_rows = [], []
        looker_run_quality_headers, looker_run_quality_rows = build_run_quality_looker_rows(
            run_quality_score_headers,
            run_quality_score_rows,
            run_quality_breakdown_headers,
            run_quality_breakdown_rows,
        )
        demand_looker_headers, demand_looker_rows = build_demand_profile_looker_rows(
            demand_headers,
            demand_rows,
            keyword_cache_rows,
        )

        tab_payloads: Dict[str, Dict[str, Any]] = {
            LOOKER_EXECUTIVE_TAB: {
                "headers": LOOKER_HEADERS[LOOKER_EXECUTIVE_TAB],
                "rows": executive_rows,
                "source_tab_names": source_ref("FLIPKART_DASHBOARD_DATA", "FLIPKART_SKU_ANALYSIS", "FLIPKART_ALERTS_GENERATED", "FLIPKART_ACTIVE_TASKS", "FLIPKART_ACTION_TRACKER", "FLIPKART_ADS_PLANNER", "FLIPKART_RETURN_ISSUE_SUMMARY"),
            },
            LOOKER_FSN_METRICS_TAB: {
                "headers": LOOKER_HEADERS[LOOKER_FSN_METRICS_TAB],
                "rows": fsn_metric_rows,
                "source_tab_names": source_ref("FLIPKART_SKU_ANALYSIS", "FLIPKART_LISTING_PRESENCE", "FLIPKART_MISSING_ACTIVE_LISTINGS", "FLIPKART_ADS_PLANNER", "FLIPKART_RETURN_ISSUE_SUMMARY", "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY", "FLIPKART_COURIER_RETURN_SUMMARY"),
            },
            LOOKER_ALERTS_TAB: {
                "headers": LOOKER_HEADERS[LOOKER_ALERTS_TAB],
                "rows": alert_rows,
                "source_tab_names": source_ref("FLIPKART_ALERTS_GENERATED"),
            },
            LOOKER_ACTIONS_TAB: {
                "headers": LOOKER_HEADERS[LOOKER_ACTIONS_TAB],
                "rows": action_rows,
                "source_tab_names": source_ref("FLIPKART_ACTION_TRACKER"),
            },
            LOOKER_ADS_TAB: {
                "headers": LOOKER_HEADERS[LOOKER_ADS_TAB],
                "rows": ads_source_rows,
                "source_tab_names": source_ref("FLIPKART_ADS_PLANNER"),
            },
            LOOKER_RETURNS_TAB: {
                "headers": LOOKER_HEADERS[LOOKER_RETURNS_TAB],
                "rows": return_source_rows,
                "source_tab_names": source_ref("FLIPKART_RETURN_ISSUE_SUMMARY"),
            },
            LOOKER_LISTINGS_TAB: {
                "headers": LOOKER_HEADERS[LOOKER_LISTINGS_TAB],
                "rows": listing_source_rows,
                "source_tab_names": source_ref("FLIPKART_LISTING_PRESENCE", "FLIPKART_MISSING_ACTIVE_LISTINGS"),
            },
            LOOKER_RUN_COMPARISON_TAB: {
                "headers": run_comparison_headers,
                "rows": run_comparison_rows,
                "source_tab_names": source_ref(run_comparison_source_tab, "FLIPKART_RUN_COMPARISON"),
            },
            LOOKER_ADJUSTED_PROFIT_TAB: {
                "headers": adjusted_profit_headers,
                "rows": adjusted_profit_rows,
                "source_tab_names": source_ref(adjusted_profit_source_tab, "FLIPKART_ADJUSTED_PROFIT"),
            },
            LOOKER_REPORT_FORMAT_MONITOR_TAB: {
                "headers": report_format_headers,
                "rows": report_format_rows,
                "source_tab_names": source_ref(report_format_source_tab, "FLIPKART_REPORT_FORMAT_MONITOR"),
            },
            LOOKER_RUN_QUALITY_TAB: {
                "headers": looker_run_quality_headers,
                "rows": looker_run_quality_rows,
                "source_tab_names": source_ref("FLIPKART_RUN_QUALITY_SCORE", "FLIPKART_RUN_QUALITY_BREAKDOWN"),
            },
            LOOKER_MODULE_CONFIDENCE_TAB: {
                "headers": module_confidence_headers,
                "rows": module_confidence_rows,
                "source_tab_names": source_ref(module_confidence_source_tab, "FLIPKART_MODULE_CONFIDENCE"),
            },
            LOOKER_DEMAND_PROFILE_TAB: {
                "headers": demand_looker_headers,
                "rows": demand_looker_rows,
                "source_tab_names": source_ref("PRODUCT_TYPE_DEMAND_PROFILE", "GOOGLE_KEYWORD_METRICS_CACHE"),
            },
            LOOKER_COMPETITOR_INTELLIGENCE_TAB: {
                "headers": competitor_headers,
                "rows": competitor_rows,
                "source_tab_names": source_ref(competitor_source_tab, "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE"),
            },
        }

        if LOOKER_RETURN_ALL_DETAILS_TAB in refresh_tabs:
            tab_payloads[LOOKER_RETURN_ALL_DETAILS_TAB] = {
                "headers": LOOKER_HEADERS[LOOKER_RETURN_ALL_DETAILS_TAB],
                "rows": return_all_details_source_rows,
                "source_tab_names": source_ref("FLIPKART_RETURN_ALL_DETAILS"),
            }
        if LOOKER_CUSTOMER_RETURNS_TAB in refresh_tabs:
            tab_payloads[LOOKER_CUSTOMER_RETURNS_TAB] = {
                "headers": LOOKER_HEADERS[LOOKER_CUSTOMER_RETURNS_TAB],
                "rows": customer_return_source_rows,
                "source_tab_names": source_ref("FLIPKART_CUSTOMER_RETURN_COMMENTS"),
            }
        if LOOKER_COURIER_RETURNS_TAB in refresh_tabs:
            tab_payloads[LOOKER_COURIER_RETURNS_TAB] = {
                "headers": LOOKER_HEADERS[LOOKER_COURIER_RETURNS_TAB],
                "rows": courier_return_source_rows,
                "source_tab_names": source_ref("FLIPKART_COURIER_RETURN_COMMENTS"),
            }
        if LOOKER_RETURN_TYPE_PIVOT_TAB in refresh_tabs:
            tab_payloads[LOOKER_RETURN_TYPE_PIVOT_TAB] = {
                "headers": LOOKER_HEADERS[LOOKER_RETURN_TYPE_PIVOT_TAB],
                "rows": return_type_pivot_source_rows,
                "source_tab_names": source_ref("FLIPKART_RETURN_TYPE_PIVOT", "FLIPKART_RETURN_ISSUE_SUMMARY"),
            }
        if LOOKER_ORDER_ITEM_EXPLORER_TAB in refresh_tabs:
            tab_payloads[LOOKER_ORDER_ITEM_EXPLORER_TAB] = {
                "headers": LOOKER_HEADERS[LOOKER_ORDER_ITEM_EXPLORER_TAB],
                "rows": order_item_rows,
                "source_tab_names": source_ref("FLIPKART_ORDER_ITEM_EXPLORER"),
            }
        if LOOKER_ORDER_ITEM_MASTER_TAB in refresh_tabs:
            tab_payloads[LOOKER_ORDER_ITEM_MASTER_TAB] = {
                "headers": LOOKER_HEADERS[LOOKER_ORDER_ITEM_MASTER_TAB],
                "rows": order_item_master_rows,
                "source_tab_names": source_ref("FLIPKART_ORDER_ITEM_MASTER", "FLIPKART_ORDER_ITEM_EXPLORER"),
            }
        if LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB in refresh_tabs:
            tab_payloads[LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB] = {
                "headers": LOOKER_HEADERS[LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB],
                "rows": order_item_source_detail_rows,
                "source_tab_names": source_ref("FLIPKART_ORDER_ITEM_SOURCE_DETAIL", "FLIPKART_ORDER_ITEM_EXPLORER", "FLIPKART_ORDER_ITEM_MASTER"),
            }

        selected_output_tabs = [tab_name for tab_name in refresh_tabs if tab_name in tab_payloads]
        tabs_written: List[str] = []
        tabs_skipped_unchanged: List[str] = []
        manifest = _load_looker_refresh_manifest()
        manifest_tabs = {
            str(item.get("tab_name", "")): dict(item)
            for item in manifest.get("tabs", [])
            if isinstance(item, dict) and str(item.get("tab_name", ""))
        }

        def write_tab_if_needed(tab_name: str) -> None:
            payload = tab_payloads[tab_name]
            headers = payload["headers"]
            rows = payload["rows"]
            source_tab_names = payload.get("source_tab_names", [])
            content_hash = _hash_looker_rows(headers, rows)
            existing = manifest_tabs.get(tab_name, {})
            sheet_exists = tab_exists(sheets_service, spreadsheet_id, tab_name)
            if not force and sheet_exists and existing.get("content_hash") == content_hash:
                tabs_skipped_unchanged.append(tab_name)
                return
            write_output_tab(sheets_service, spreadsheet_id, tab_name, headers, rows)
            tabs_written.append(tab_name)
            manifest_tabs[tab_name] = _build_looker_refresh_manifest_entry(
                tab_name=tab_name,
                source_tab_names=source_tab_names,
                headers=headers,
                rows=rows,
            )

        for tab_name in selected_output_tabs:
            try:
                write_tab_if_needed(tab_name)
            except HttpError as exc:
                if not _is_quota_limited_http_error(exc):
                    raise
                quota_result = {
                    "status": "WARNING",
                    "spreadsheet_id": spreadsheet_id,
                    "group": selected_group,
                    "quota_safe_mode": quota_safe_mode,
                    "message": f"Sheets quota exceeded while writing {tab_name}; wait 5 minutes and rerun",
                    "failed_tab_name": tab_name,
                    "tabs_requested": selected_output_tabs,
                    "tabs_written": tabs_written,
                    "tabs_skipped_unchanged": tabs_skipped_unchanged,
                    "large_tabs_skipped": large_tabs_skipped,
                    "source_tabs_read_count": source_tabs_read_count,
                    "source_tabs_cache_hits": source_tabs_cache_hits,
                    "source_tabs_checked": SOURCE_TABS,
                    "skipped_unchanged_tabs": tabs_skipped_unchanged,
                    "tabs_updated": tabs_written,
                    "log_path": str(LOG_PATH),
                }
                _save_looker_refresh_manifest({"tabs": list(manifest_tabs.values()), "last_group": selected_group, "quota_safe_mode": quota_safe_mode, "last_written_at": now_iso()})
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
                            "status": "WARNING",
                            "message": quota_result["message"],
                        }
                    ],
                )
                return quota_result

        _save_looker_refresh_manifest(
            {
                "last_group": selected_group,
                "quota_safe_mode": quota_safe_mode,
                "last_written_at": now_iso(),
                "tabs": list(manifest_tabs.values()),
            }
        )

        result = {
            "status": "SUCCESS",
            "spreadsheet_id": spreadsheet_id,
            "run_id": latest_run_id,
            "report_date": report_date,
            "group": selected_group,
            "quota_safe_mode": quota_safe_mode,
            "tabs_requested": selected_output_tabs,
            "tabs_written": tabs_written,
            "tabs_skipped_unchanged": tabs_skipped_unchanged,
            "skipped_unchanged_tabs": tabs_skipped_unchanged,
            "large_tabs_skipped": large_tabs_skipped,
            "source_tabs_read_count": source_tabs_read_count,
            "source_tabs_cache_hits": source_tabs_cache_hits,
            "tabs_updated": tabs_written,
            "row_counts": {tab_name: len(tab_payloads[tab_name]["rows"]) for tab_name in selected_output_tabs},
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
                    "message": f"Rebuilt Looker Studio source tabs for Flipkart ({selected_group})",
                }
            ],
        )

        return result
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        message = str(exc)
        if status == 429 or "quota" in message.lower() or "rate_limit" in message.lower():
            warning_result = _quota_limited_warning_result(spreadsheet_id)
            append_csv_log(
                LOG_PATH,
                ["timestamp", "spreadsheet_id", "run_id", "report_date", "executive_rows", "fsn_metric_rows", "alert_rows", "action_rows", "ads_rows", "return_rows", "listing_rows", "status", "message"],
                [
                    {
                        "timestamp": now_iso(),
                        "spreadsheet_id": spreadsheet_id,
                        "run_id": "",
                        "report_date": "",
                        "executive_rows": 0,
                        "fsn_metric_rows": 0,
                        "alert_rows": 0,
                        "action_rows": 0,
                        "ads_rows": 0,
                        "return_rows": 0,
                        "listing_rows": 0,
                        "status": "WARNING",
                        "message": warning_result["message"],
                    }
                ],
            )
            return warning_result
        raise


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Build Flipkart Looker Studio source tabs.")
    parser.add_argument("--group", choices=sorted(LOOKER_GROUP_TABS), default="light", help="Looker refresh group to build. Default: light.")
    parser.add_argument("--include-large-tabs", action="store_true", help="Include the large audit tabs for the selected group.")
    parser.add_argument("--force", action="store_true", help="Rewrite tabs even when the content hash matches the last manifest entry.")
    args = parser.parse_args()
    try:
        result = create_looker_studio_sources(group=args.group, include_large_tabs=args.include_large_tabs, force=args.force)
        print(json.dumps(result, indent=2, ensure_ascii=False))
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
