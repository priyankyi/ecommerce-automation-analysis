from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_cogs_helpers import count_cogs_rows, is_cogs_available
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    append_csv_log,
    clean_fsn,
    normalize_text,
    now_iso,
    parse_float,
    write_csv,
)
from src.marketplaces.flipkart.create_looker_studio_sources import build_index, get_latest_run_row, latest_text_value

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_run_comparison_log.csv"

RUN_HISTORY_TAB = "FLIPKART_RUN_HISTORY"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"

RUN_COMPARISON_TAB = "FLIPKART_RUN_COMPARISON"
FSN_COMPARISON_TAB = "FLIPKART_FSN_RUN_COMPARISON"
LOOKER_RUN_COMPARISON_TAB = "LOOKER_FLIPKART_RUN_COMPARISON"

RUN_COMPARISON_HEADERS = [
    "Metric_Category",
    "Metric_Name",
    "Previous_Run_ID",
    "Previous_Value",
    "Latest_Run_ID",
    "Latest_Value",
    "Change_Value",
    "Change_Percent",
    "Direction",
    "Interpretation",
    "Last_Updated",
]

FSN_COMPARISON_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Previous_Run_ID",
    "Latest_Run_ID",
    "Previous_Orders",
    "Latest_Orders",
    "Orders_Change",
    "Previous_Return_Rate",
    "Latest_Return_Rate",
    "Return_Rate_Change",
    "Previous_Final_Net_Profit",
    "Latest_Final_Net_Profit",
    "Profit_Change",
    "Previous_Data_Confidence",
    "Latest_Data_Confidence",
    "Previous_Final_Action",
    "Latest_Final_Action",
    "Comparison_Status",
    "Business_Interpretation",
    "Suggested_Review_Action",
    "Last_Updated",
]

LOOKER_RUN_COMPARISON_HEADERS = [
    "Report_Date",
    "Metric_Category",
    "Metric_Name",
    "Previous_Run_ID",
    "Previous_Value",
    "Latest_Run_ID",
    "Latest_Value",
    "Change_Value",
    "Change_Percent",
    "Direction",
    "Interpretation",
    "Last_Updated",
]

RUN_METRICS: List[Dict[str, Any]] = [
    {"category": "Run Summary", "name": "Total Target FSNs", "higher_is_better": True, "source": "run"},
    {"category": "Run Summary", "name": "Rows Written", "higher_is_better": True, "source": "run"},
    {"category": "Confidence", "name": "High Confidence Count", "higher_is_better": True, "source": "run"},
    {"category": "Confidence", "name": "Medium Confidence Count", "higher_is_better": False, "source": "run"},
    {"category": "Confidence", "name": "Low Confidence Count", "higher_is_better": False, "source": "run"},
    {"category": "Alerts", "name": "Total Alerts", "higher_is_better": False, "source": "current_only"},
    {"category": "Alerts", "name": "Critical Alerts", "higher_is_better": False, "source": "current_only"},
    {"category": "Alerts", "name": "High Alerts", "higher_is_better": False, "source": "current_only"},
    {"category": "Operations", "name": "Active Tasks", "higher_is_better": False, "source": "current_only"},
    {"category": "COGS", "name": "Missing COGS", "higher_is_better": False, "source": "current_only"},
    {"category": "Listings", "name": "Missing Active Listings", "higher_is_better": False, "source": "current_only"},
    {"category": "Ads", "name": "Ads Ready Count", "higher_is_better": True, "source": "current_only"},
    {"category": "Sales", "name": "FSNs With Orders", "higher_is_better": True, "source": "current_or_history"},
    {"category": "Sales", "name": "FSNs With Returns", "higher_is_better": False, "source": "current_or_history"},
    {"category": "Sales", "name": "FSNs With Settlement", "higher_is_better": True, "source": "current_or_history"},
    {"category": "Sales", "name": "FSNs With PNL", "higher_is_better": True, "source": "current_or_history"},
    {"category": "Returns", "name": "High Return Rate Count", "higher_is_better": False, "source": "current_or_history"},
    {"category": "Accounting", "name": "Missing Settlement Count", "higher_is_better": False, "source": "current_or_history"},
    {"category": "Accounting", "name": "Missing PNL Count", "higher_is_better": False, "source": "current_or_history"},
    {"category": "Profit", "name": "Total Final Net Profit", "higher_is_better": True, "source": "current_only"},
    {"category": "Profit", "name": "Total COGS", "higher_is_better": False, "source": "current_only"},
    {"category": "COGS", "name": "COGS Completion Percent", "higher_is_better": True, "source": "current_only"},
]

CONFIDENCE_RANK = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "": 0}
STATUS_ORDER = {
    "Improved": 0,
    "Worsened": 1,
    "No Major Change": 2,
    "New In Latest Run": 3,
    "Missing In Latest Run": 4,
    "Not Enough History": 5,
}


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


def format_number(value: Any, decimals: int = 2) -> str:
    number = parse_float(value)
    if decimals <= 0:
        return str(int(round(number)))
    if float(number).is_integer():
        return str(int(round(number)))
    return f"{number:.{decimals}f}".rstrip("0").rstrip(".")


def format_percent_change(previous_value: str, latest_value: str) -> str:
    previous = parse_float(previous_value)
    latest = parse_float(latest_value)
    if previous == 0:
        return ""
    return format_number(((latest - previous) / abs(previous)) * 100, 2)


def safe_text(value: Any) -> str:
    return normalize_text(value)


def unique_rows(rows: Sequence[Dict[str, Any]], key_field: str = "FSN") -> List[Dict[str, Any]]:
    indexed = build_index(rows, key_field=key_field, latest_by_updated=True)
    return list(indexed.values())


def count_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def row_has_token(row: Dict[str, Any], *tokens: str) -> bool:
    haystack = " ".join(safe_text(value).lower() for value in row.values())
    return any(token.lower() in haystack for token in tokens)


def row_missing_cogs(row: Dict[str, Any]) -> bool:
    if safe_text(row.get("FSN", "")) and not is_cogs_available(row):
        return True
    missing_data = safe_text(row.get("Missing_Data", ""))
    return "cogs missing" in missing_data.lower()


def row_missing_listing(row: Dict[str, Any]) -> bool:
    listing_text = " ".join(
        [
            safe_text(row.get("Listing_Presence_Status", "")),
            safe_text(row.get("Listing_Status", "")),
            safe_text(row.get("Found_In_Active_Listing", "")),
            safe_text(row.get("Missing_Data", "")),
        ]
    ).lower()
    if "found" in listing_text and "missing" not in listing_text:
        return False
    blocked_tokens = ["missing", "not found", "inactive", "blocked", "rejected", "unlisted", "paused", "disabled"]
    return any(token in listing_text for token in blocked_tokens)


def row_missing_settlement(row: Dict[str, Any]) -> bool:
    missing_data = safe_text(row.get("Missing_Data", "")).lower()
    if "settlement missing" in missing_data:
        return True
    settlement = safe_text(row.get("Net_Settlement", ""))
    return not settlement or settlement in {"0", "0.0", "0.00"}


def row_missing_pnl(row: Dict[str, Any]) -> bool:
    missing_data = safe_text(row.get("Missing_Data", "")).lower()
    if "pnl missing" in missing_data:
        return True
    pnl = safe_text(row.get("Net_Profit_Before_COGS", ""))
    return not pnl


def confidence_rank(value: Any) -> int:
    return CONFIDENCE_RANK.get(safe_text(value).upper(), 0)


def build_current_snapshot(
    latest_run_row: Dict[str, Any],
    analysis_rows: Sequence[Dict[str, Any]],
    alert_rows: Sequence[Dict[str, Any]],
    active_task_rows: Sequence[Dict[str, Any]],
) -> Dict[str, str]:
    analysis_unique_rows = unique_rows(analysis_rows, key_field="FSN")
    cogs_available_count, cogs_missing_count = count_cogs_rows(analysis_unique_rows)

    snapshot: Dict[str, str] = {}
    snapshot["Total Target FSNs"] = safe_text(latest_run_row.get("Target_FSN_Count", "")) or str(len(analysis_unique_rows))
    snapshot["Rows Written"] = safe_text(latest_run_row.get("Rows_Written", "")) or str(len(analysis_unique_rows))
    snapshot["High Confidence Count"] = safe_text(latest_run_row.get("High_Confidence_Count", ""))
    snapshot["Medium Confidence Count"] = safe_text(latest_run_row.get("Medium_Confidence_Count", ""))
    snapshot["Low Confidence Count"] = safe_text(latest_run_row.get("Low_Confidence_Count", ""))
    snapshot["Total Alerts"] = str(count_rows(alert_rows))
    snapshot["Critical Alerts"] = str(sum(1 for row in alert_rows if safe_text(row.get("Severity", "")) == "Critical"))
    snapshot["High Alerts"] = str(sum(1 for row in alert_rows if safe_text(row.get("Severity", "")) == "High"))
    snapshot["Active Tasks"] = str(count_rows(active_task_rows))
    snapshot["Missing COGS"] = str(cogs_missing_count)
    snapshot["Missing Active Listings"] = str(sum(1 for row in analysis_unique_rows if row_missing_listing(row)))
    snapshot["Ads Ready Count"] = str(sum(1 for row in analysis_unique_rows if is_ads_ready(row)))
    snapshot["FSNs With Orders"] = str(sum(1 for row in analysis_unique_rows if parse_float(row.get("Orders", "")) > 0))
    snapshot["FSNs With Returns"] = str(sum(1 for row in analysis_unique_rows if parse_float(row.get("Customer_Return_Count", row.get("Returns", ""))) > 0))
    snapshot["FSNs With Settlement"] = str(sum(1 for row in analysis_unique_rows if not row_missing_settlement(row)))
    snapshot["FSNs With PNL"] = str(sum(1 for row in analysis_unique_rows if not row_missing_pnl(row)))
    snapshot["High Return Rate Count"] = str(sum(1 for row in analysis_unique_rows if parse_float(row.get("Customer_Return_Rate", row.get("Return_Rate", ""))) > 0.20))
    snapshot["Missing Settlement Count"] = str(sum(1 for row in analysis_unique_rows if row_missing_settlement(row)))
    snapshot["Missing PNL Count"] = str(sum(1 for row in analysis_unique_rows if row_missing_pnl(row)))
    snapshot["Total Final Net Profit"] = format_number(
        sum(parse_float(row.get("Final_Net_Profit", "")) for row in analysis_unique_rows),
        2,
    )
    snapshot["Total COGS"] = format_number(sum(parse_float(row.get("Total_COGS", "")) for row in analysis_unique_rows), 2)
    snapshot["COGS Completion Percent"] = format_number(
        round((cogs_available_count / (cogs_available_count + cogs_missing_count)) * 100, 2)
        if (cogs_available_count + cogs_missing_count)
        else 0.0,
        2,
    )
    return snapshot


def build_previous_snapshot(
    previous_run_row: Dict[str, Any],
    previous_fsn_rows: Sequence[Dict[str, Any]],
) -> Dict[str, str]:
    previous_unique_rows = unique_rows(previous_fsn_rows, key_field="FSN")
    snapshot: Dict[str, str] = {}
    snapshot["Total Target FSNs"] = safe_text(previous_run_row.get("Target_FSN_Count", ""))
    snapshot["Rows Written"] = safe_text(previous_run_row.get("Rows_Written", ""))
    snapshot["High Confidence Count"] = safe_text(previous_run_row.get("High_Confidence_Count", ""))
    snapshot["Medium Confidence Count"] = safe_text(previous_run_row.get("Medium_Confidence_Count", ""))
    snapshot["Low Confidence Count"] = safe_text(previous_run_row.get("Low_Confidence_Count", ""))
    snapshot["Total Alerts"] = ""
    snapshot["Critical Alerts"] = ""
    snapshot["High Alerts"] = ""
    snapshot["Active Tasks"] = ""
    snapshot["Missing COGS"] = str(sum(1 for row in previous_unique_rows if row_missing_cogs(row)))
    snapshot["Missing Active Listings"] = str(sum(1 for row in previous_unique_rows if row_missing_listing(row)))
    snapshot["Ads Ready Count"] = ""
    snapshot["FSNs With Orders"] = str(sum(1 for row in previous_unique_rows if parse_float(row.get("Orders", "")) > 0))
    snapshot["FSNs With Returns"] = str(sum(1 for row in previous_unique_rows if parse_float(row.get("Customer_Return_Count", row.get("Returns", ""))) > 0))
    snapshot["FSNs With Settlement"] = str(sum(1 for row in previous_unique_rows if not row_missing_settlement(row)))
    snapshot["FSNs With PNL"] = str(sum(1 for row in previous_unique_rows if not row_missing_pnl(row)))
    snapshot["High Return Rate Count"] = str(sum(1 for row in previous_unique_rows if parse_float(row.get("Customer_Return_Rate", row.get("Return_Rate", ""))) > 0.20))
    snapshot["Missing Settlement Count"] = str(sum(1 for row in previous_unique_rows if row_missing_settlement(row)))
    snapshot["Missing PNL Count"] = str(sum(1 for row in previous_unique_rows if row_missing_pnl(row)))
    snapshot["Total Final Net Profit"] = ""
    snapshot["Total COGS"] = ""
    snapshot["COGS Completion Percent"] = ""
    return snapshot


def metric_direction(metric_name: str, latest_value: str, previous_value: str, higher_is_better: bool) -> str:
    latest_present = safe_text(latest_value) != ""
    previous_present = safe_text(previous_value) != ""
    if not latest_present and not previous_present:
        return "Not Comparable"
    if latest_present and not previous_present:
        return "New"
    if previous_present and not latest_present:
        return "Not Comparable"

    latest_number = parse_float(latest_value)
    previous_number = parse_float(previous_value)
    delta = latest_number - previous_number
    if abs(delta) < 1e-9:
        return "No Change"
    improved = delta > 0 if higher_is_better else delta < 0
    return "Improved" if improved else "Worsened"


def metric_interpretation(metric_name: str, direction: str, latest_value: str, previous_value: str) -> str:
    if direction == "Not Comparable":
        return "Not enough history to compare this metric."
    if direction == "New":
        return f"{metric_name} appears in the latest run and was not available previously."
    if direction == "No Change":
        return f"No measurable change in {metric_name}."

    metric_lower = metric_name.lower()
    if direction == "Improved":
        if "alerts" in metric_lower or "missing" in metric_lower or "returns" in metric_lower:
            return f"{metric_name} decreased, which is favorable."
        if "profit" in metric_lower or "completion" in metric_lower or "confidence" in metric_lower or "orders" in metric_lower or "settlement" in metric_lower or metric_name == "Ads Ready Count":
            return f"{metric_name} increased, which is favorable."
    if direction == "Worsened":
        if "alerts" in metric_lower or "missing" in metric_lower or "returns" in metric_lower:
            return f"{metric_name} increased, which is unfavorable."
        if "profit" in metric_lower or "completion" in metric_lower or "confidence" in metric_lower or "orders" in metric_lower or "settlement" in metric_lower or metric_name == "Ads Ready Count":
            return f"{metric_name} decreased, which is unfavorable."
    return f"Latest value changed from {previous_value or 'blank'} to {latest_value or 'blank'}."


def build_run_comparison_rows(
    latest_run_id: str,
    previous_run_id: str,
    latest_snapshot: Dict[str, str],
    previous_snapshot: Dict[str, str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    last_updated = now_iso()
    for metric in RUN_METRICS:
        metric_name = metric["name"]
        latest_value = latest_snapshot.get(metric_name, "")
        previous_value = previous_snapshot.get(metric_name, "")
        if not previous_run_id:
            direction = "Not Comparable"
            interpretation = "Not enough history to compare this metric."
        else:
            direction = metric_direction(metric_name, latest_value, previous_value, bool(metric["higher_is_better"]))
            interpretation = metric_interpretation(metric_name, direction, latest_value, previous_value)
        change_value = ""
        change_percent = ""
        if direction not in {"Not Comparable", "New"} and safe_text(previous_value) and safe_text(latest_value):
            delta = parse_float(latest_value) - parse_float(previous_value)
            change_value = format_number(delta, 2)
            change_percent = format_percent_change(previous_value, latest_value)
        rows.append(
            {
                "Metric_Category": metric["category"],
                "Metric_Name": metric_name,
                "Previous_Run_ID": previous_run_id,
                "Previous_Value": previous_value,
                "Latest_Run_ID": latest_run_id,
                "Latest_Value": latest_value,
                "Change_Value": change_value,
                "Change_Percent": change_percent,
                "Direction": direction,
                "Interpretation": interpretation,
                "Last_Updated": last_updated,
            }
        )
    return rows


def confidence_label(value: Any) -> str:
    label = safe_text(value).upper()
    return label if label else ""


def compare_confidence(previous_value: str, latest_value: str) -> str:
    prev_rank = CONFIDENCE_RANK.get(confidence_label(previous_value), 0)
    latest_rank = CONFIDENCE_RANK.get(confidence_label(latest_value), 0)
    if prev_rank == 0 and latest_rank == 0:
        return "No Major Change"
    if latest_rank > prev_rank:
        return "Improved"
    if latest_rank < prev_rank:
        return "Worsened"
    return "No Major Change"


def compare_orders(previous_value: str, latest_value: str) -> str:
    prev = parse_float(previous_value)
    latest = parse_float(latest_value)
    if abs(latest - prev) < 1e-9:
        return "No Major Change"
    return "Improved" if latest > prev else "Worsened"


def compare_return_rate(previous_value: str, latest_value: str) -> str:
    prev = parse_float(previous_value)
    latest = parse_float(latest_value)
    if abs(latest - prev) < 1e-9:
        return "No Major Change"
    return "Improved" if latest < prev else "Worsened"


def compare_text_action(previous_value: str, latest_value: str) -> str:
    prev = safe_text(previous_value).lower()
    latest = safe_text(latest_value).lower()
    if latest == prev:
        return "No Major Change"
    positive_tokens = ["scale", "continue", "monitor", "optimize", "good"]
    negative_tokens = ["fix", "review", "missing", "stop", "do not", "investigate", "wait"]
    prev_positive = any(token in prev for token in positive_tokens)
    latest_positive = any(token in latest for token in positive_tokens)
    prev_negative = any(token in prev for token in negative_tokens)
    latest_negative = any(token in latest for token in negative_tokens)
    if latest_positive and not prev_positive:
        return "Improved"
    if latest_negative and not prev_negative:
        return "Worsened"
    return "No Major Change"


def build_business_interpretation(
    comparison_status: str,
    orders_change: str,
    return_rate_change: str,
    profit_change: str,
    previous_confidence: str,
    latest_confidence: str,
    previous_action: str,
    latest_action: str,
) -> str:
    if comparison_status == "Not Enough History":
        return "Previous-run comparison data is not available yet."
    if comparison_status == "New In Latest Run":
        return "FSN appears in the latest run but was not present in the previous run."
    if comparison_status == "Missing In Latest Run":
        return "FSN was present in the previous run but is missing from the latest run."

    details: List[str] = []
    if return_rate_change:
        if parse_float(return_rate_change) < 0:
            details.append("return rate improved")
        elif parse_float(return_rate_change) > 0:
            details.append("return rate worsened")
    if orders_change and parse_float(orders_change) != 0:
        details.append("orders changed")
    if profit_change:
        if parse_float(profit_change) > 0:
            details.append("profit improved")
        elif parse_float(profit_change) < 0:
            details.append("profit declined")
    confidence_change = compare_confidence(previous_confidence, latest_confidence)
    if confidence_change:
        if confidence_change == "Improved":
            details.append("confidence improved")
        elif confidence_change == "Worsened":
            details.append("confidence worsened")

    action_change = compare_text_action(previous_action, latest_action)
    if action_change == "Improved":
        details.append("final action improved")
    elif action_change == "Worsened":
        details.append("final action worsened")

    if details:
        return "; ".join(details).capitalize() + "."
    if comparison_status == "Improved":
        return "Latest run looks healthier than the previous run."
    if comparison_status == "Worsened":
        return "Latest run shows regression versus the previous run."
    return "No major operational shift detected."


def build_suggested_review_action(
    comparison_status: str,
    latest_row: Dict[str, Any],
    previous_row: Dict[str, Any],
    orders_change: str,
    return_rate_change: str,
    profit_change: str,
) -> str:
    if comparison_status == "Not Enough History":
        return "Data Review Required"
    if comparison_status == "Missing In Latest Run":
        return "Review Newly Missing FSN"
    if comparison_status == "New In Latest Run":
        if confidence_rank(latest_row.get("Data_Confidence", "")) <= 1:
            return "Data Review Required"
        return "Continue Monitoring"
    if comparison_status == "Improved":
        return "Good Improvement"
    if comparison_status == "Worsened":
        if parse_float(return_rate_change) > 0:
            return "Review Worsened Return Rate"
        if profit_change and parse_float(profit_change) < 0:
            return "Review Profit Drop"
        if confidence_rank(latest_row.get("Data_Confidence", "")) < confidence_rank(previous_row.get("Data_Confidence", "")):
            return "Data Review Required"
        return "Continue Monitoring"
    return "Continue Monitoring"


def build_fsn_comparison_rows(
    latest_run_id: str,
    previous_run_id: str,
    latest_rows: Sequence[Dict[str, Any]],
    previous_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    latest_lookup = build_index(latest_rows, key_field="FSN", latest_by_updated=True)
    previous_lookup = build_index(previous_rows, key_field="FSN", latest_by_updated=True)
    all_fsns = list(dict.fromkeys(list(previous_lookup.keys()) + list(latest_lookup.keys())))
    last_updated = now_iso()
    rows: List[Dict[str, Any]] = []

    if not previous_run_id:
        for fsn in sorted(latest_lookup.keys()):
            latest_row = latest_lookup[fsn]
            rows.append(
                {
                    "FSN": fsn,
                    "SKU_ID": safe_text(latest_row.get("SKU_ID", "")),
                    "Product_Title": safe_text(latest_row.get("Product_Title", "")),
                    "Previous_Run_ID": "",
                    "Latest_Run_ID": latest_run_id,
                    "Previous_Orders": "",
                    "Latest_Orders": safe_text(latest_row.get("Orders", "")),
                    "Orders_Change": "",
                    "Previous_Return_Rate": "",
                    "Latest_Return_Rate": safe_text(latest_row.get("Return_Rate", "")),
                    "Return_Rate_Change": "",
                    "Previous_Final_Net_Profit": "",
                    "Latest_Final_Net_Profit": safe_text(latest_row.get("Final_Net_Profit", "")),
                    "Profit_Change": "",
                    "Previous_Data_Confidence": "",
                    "Latest_Data_Confidence": safe_text(latest_row.get("Data_Confidence", "")),
                    "Previous_Final_Action": "",
                    "Latest_Final_Action": safe_text(latest_row.get("Final_Action", "")),
                    "Comparison_Status": "Not Enough History",
                    "Business_Interpretation": "Previous-run comparison data is not available yet.",
                    "Suggested_Review_Action": "Data Review Required",
                    "Last_Updated": last_updated,
                }
            )
        return rows

    for fsn in sorted(all_fsns):
        latest_row = latest_lookup.get(fsn, {})
        previous_row = previous_lookup.get(fsn, {})
        latest_present = bool(latest_row)
        previous_present = bool(previous_row)

        if latest_present and not previous_present:
            status = "New In Latest Run"
        elif previous_present and not latest_present:
            status = "Missing In Latest Run"
        elif latest_present and previous_present:
            orders_change_value = parse_float(latest_row.get("Orders", "")) - parse_float(previous_row.get("Orders", ""))
            return_rate_change_value = parse_float(latest_row.get("Return_Rate", "")) - parse_float(previous_row.get("Return_Rate", ""))
            profit_change_value = parse_float(latest_row.get("Final_Net_Profit", "")) - parse_float(previous_row.get("Final_Net_Profit", ""))

            score = 0.0
            if abs(orders_change_value) >= 1e-9:
                score += 0.5 if orders_change_value > 0 else -0.5
            if abs(return_rate_change_value) >= 1e-9:
                score += 2.0 if return_rate_change_value < 0 else -2.0
            if safe_text(latest_row.get("Final_Net_Profit", "")) and safe_text(previous_row.get("Final_Net_Profit", "")):
                score += 2.0 if profit_change_value > 0 else -2.0 if profit_change_value < 0 else 0.0
            confidence_delta = confidence_rank(latest_row.get("Data_Confidence", "")) - confidence_rank(previous_row.get("Data_Confidence", ""))
            if confidence_delta:
                score += 1.0 if confidence_delta > 0 else -1.0

            if score >= 1.5:
                status = "Improved"
            elif score <= -1.5:
                status = "Worsened"
            else:
                status = "No Major Change"
        else:
            status = "Not Enough History"

        orders_change_text = ""
        return_rate_change_text = ""
        profit_change_text = ""
        if latest_present and previous_present:
            orders_change_text = format_number(parse_float(latest_row.get("Orders", "")) - parse_float(previous_row.get("Orders", "")), 2)
            return_rate_change_text = format_number(
                parse_float(latest_row.get("Return_Rate", "")) - parse_float(previous_row.get("Return_Rate", "")),
                4,
            )
            if safe_text(latest_row.get("Final_Net_Profit", "")) and safe_text(previous_row.get("Final_Net_Profit", "")):
                profit_change_text = format_number(
                    parse_float(latest_row.get("Final_Net_Profit", "")) - parse_float(previous_row.get("Final_Net_Profit", "")),
                    2,
                )

        business_interpretation = build_business_interpretation(
            status,
            orders_change_text,
            return_rate_change_text,
            profit_change_text,
            safe_text(previous_row.get("Data_Confidence", "")),
            safe_text(latest_row.get("Data_Confidence", "")),
            safe_text(previous_row.get("Final_Action", "")),
            safe_text(latest_row.get("Final_Action", "")),
        )
        suggested_review_action = build_suggested_review_action(
            status,
            latest_row,
            previous_row,
            orders_change_text,
            return_rate_change_text,
            profit_change_text,
        )

        rows.append(
            {
                "FSN": fsn,
                "SKU_ID": safe_text(latest_row.get("SKU_ID", "")) or safe_text(previous_row.get("SKU_ID", "")),
                "Product_Title": safe_text(latest_row.get("Product_Title", "")) or safe_text(previous_row.get("Product_Title", "")),
                "Previous_Run_ID": previous_run_id,
                "Latest_Run_ID": latest_run_id,
                "Previous_Orders": safe_text(previous_row.get("Orders", "")),
                "Latest_Orders": safe_text(latest_row.get("Orders", "")),
                "Orders_Change": orders_change_text,
                "Previous_Return_Rate": safe_text(previous_row.get("Return_Rate", "")),
                "Latest_Return_Rate": safe_text(latest_row.get("Return_Rate", "")),
                "Return_Rate_Change": return_rate_change_text,
                "Previous_Final_Net_Profit": safe_text(previous_row.get("Final_Net_Profit", "")),
                "Latest_Final_Net_Profit": safe_text(latest_row.get("Final_Net_Profit", "")),
                "Profit_Change": profit_change_text,
                "Previous_Data_Confidence": safe_text(previous_row.get("Data_Confidence", "")),
                "Latest_Data_Confidence": safe_text(latest_row.get("Data_Confidence", "")),
                "Previous_Final_Action": safe_text(previous_row.get("Final_Action", "")),
                "Latest_Final_Action": safe_text(latest_row.get("Final_Action", "")),
                "Comparison_Status": status,
                "Business_Interpretation": business_interpretation,
                "Suggested_Review_Action": suggested_review_action,
                "Last_Updated": last_updated,
            }
        )

    rows.sort(
        key=lambda row: (
            STATUS_ORDER.get(safe_text(row.get("Comparison_Status", "")), 99),
            safe_text(row.get("FSN", "")),
        )
    )
    return rows


def is_ads_ready(row: Dict[str, Any]) -> bool:
    ready_decisions = {
        "Test Ads",
        "Always-On Test",
        "Seasonal/Event Test",
        "Scale Ads",
        "Continue / Optimize Ads",
    }
    ready_statuses = {"READY", "PREPARE"}
    decision = safe_text(row.get("Final_Ads_Decision", ""))
    status = safe_text(row.get("Ads_Readiness_Status", "")).upper()
    return decision in ready_decisions or status in ready_statuses


def build_previous_run_candidates(run_history_rows: Sequence[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    unique_runs: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in run_history_rows:
        run_id = safe_text(row.get("Run_ID", ""))
        if not run_id or run_id in seen:
            continue
        seen.add(run_id)
        unique_runs.append(dict(row))

    def sort_key(row: Dict[str, Any]) -> Tuple[datetime, datetime]:
        run_id = safe_text(row.get("Run_ID", ""))
        try:
            run_dt = datetime.strptime(run_id, "FLIPKART_%Y%m%d_%H%M%S")
        except ValueError:
            run_dt = datetime.min
        updated_text = safe_text(row.get("Last_Updated", ""))
        try:
            updated_dt = datetime.fromisoformat(updated_text)
        except ValueError:
            updated_dt = datetime.min
        return run_dt, updated_dt

    unique_runs.sort(key=sort_key)
    if len(unique_runs) < 2:
        return "", {}
    return safe_text(unique_runs[-2].get("Run_ID", "")), unique_runs[-2]


def build_latest_run_comparison() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [RUN_HISTORY_TAB, FSN_HISTORY_TAB, SKU_ANALYSIS_TAB, ALERTS_TAB, ACTIVE_TASKS_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    _, run_history_rows = read_table(sheets_service, spreadsheet_id, RUN_HISTORY_TAB)
    _, fsn_history_rows = read_table(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
    _, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    _, alert_rows = read_table(sheets_service, spreadsheet_id, ALERTS_TAB)
    _, active_task_rows = read_table(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)

    latest_run_row = get_latest_run_row(run_history_rows)
    latest_run_id = latest_text_value(latest_run_row, "Run_ID")
    previous_run_id, previous_run_row = build_previous_run_candidates(run_history_rows)

    latest_fsn_rows = unique_rows(analysis_rows, key_field="FSN")
    previous_fsn_rows = [row for row in fsn_history_rows if safe_text(row.get("Run_ID", "")) == previous_run_id] if previous_run_id else []

    latest_snapshot = build_current_snapshot(latest_run_row, latest_fsn_rows, alert_rows, active_task_rows)
    previous_snapshot = build_previous_snapshot(previous_run_row, previous_fsn_rows) if previous_run_id else {}

    run_comparison_rows = build_run_comparison_rows(latest_run_id, previous_run_id, latest_snapshot, previous_snapshot)
    fsn_comparison_rows = build_fsn_comparison_rows(latest_run_id, previous_run_id, latest_fsn_rows, previous_fsn_rows)

    report_date = safe_text(latest_run_row.get("Report_End_Date", "")) or safe_text(latest_run_row.get("Run_Date", "")) or now_iso()[:10]
    looker_rows = [
        {
            "Report_Date": report_date,
            **{header: row.get(header, "") for header in RUN_COMPARISON_HEADERS},
        }
        for row in run_comparison_rows
    ]

    local_run_comparison_path = OUTPUT_DIR / "flipkart_run_comparison.csv"
    local_fsn_comparison_path = OUTPUT_DIR / "flipkart_fsn_run_comparison.csv"
    local_looker_comparison_path = OUTPUT_DIR / "looker_flipkart_run_comparison.csv"

    write_csv(local_run_comparison_path, RUN_COMPARISON_HEADERS, run_comparison_rows)
    write_csv(local_fsn_comparison_path, FSN_COMPARISON_HEADERS, fsn_comparison_rows)
    write_csv(local_looker_comparison_path, LOOKER_RUN_COMPARISON_HEADERS, looker_rows)

    run_sheet_id = ensure_tab(sheets_service, spreadsheet_id, RUN_COMPARISON_TAB)
    fsn_sheet_id = ensure_tab(sheets_service, spreadsheet_id, FSN_COMPARISON_TAB)
    looker_sheet_id = ensure_tab(sheets_service, spreadsheet_id, LOOKER_RUN_COMPARISON_TAB)

    write_output_tab(sheets_service, spreadsheet_id, RUN_COMPARISON_TAB, RUN_COMPARISON_HEADERS, run_comparison_rows)
    write_output_tab(sheets_service, spreadsheet_id, FSN_COMPARISON_TAB, FSN_COMPARISON_HEADERS, fsn_comparison_rows)
    write_output_tab(sheets_service, spreadsheet_id, LOOKER_RUN_COMPARISON_TAB, LOOKER_RUN_COMPARISON_HEADERS, looker_rows)

    run_status_counts = Counter(normalize_text(row.get("Direction", "")) for row in run_comparison_rows)
    fsn_status_counts = Counter(normalize_text(row.get("Comparison_Status", "")) for row in fsn_comparison_rows)
    improved_count = sum(1 for row in run_comparison_rows if normalize_text(row.get("Direction", "")) == "Improved")
    worsened_count = sum(1 for row in run_comparison_rows if normalize_text(row.get("Direction", "")) == "Worsened")
    no_change_count = sum(1 for row in run_comparison_rows if normalize_text(row.get("Direction", "")) == "No Change")
    not_enough_history = sum(
        1
        for row in run_comparison_rows
        if normalize_text(row.get("Direction", "")) == "Not Comparable"
        or "Not enough history" in normalize_text(row.get("Interpretation", ""))
    )

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "latest_run_id": latest_run_id,
        "previous_run_id": previous_run_id,
        "run_comparison_rows": len(run_comparison_rows),
        "fsn_comparison_rows": len(fsn_comparison_rows),
        "status": "SUCCESS",
        "message": "Built Flipkart run comparison tabs",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "latest_run_id",
            "previous_run_id",
            "run_comparison_rows",
            "fsn_comparison_rows",
            "status",
            "message",
        ],
        [log_row],
    )

    result = {
        "status": "SUCCESS",
        "latest_run_id": latest_run_id,
        "previous_run_id": previous_run_id,
        "run_comparison_rows": len(run_comparison_rows),
        "fsn_comparison_rows": len(fsn_comparison_rows),
        "improved_count": improved_count,
        "worsened_count": worsened_count,
        "no_change_count": no_change_count,
        "not_enough_history": not_enough_history,
        "tabs_updated": [RUN_COMPARISON_TAB, FSN_COMPARISON_TAB, LOOKER_RUN_COMPARISON_TAB],
        "log_path": str(LOG_PATH),
        "local_outputs": {
            "flipkart_run_comparison": str(local_run_comparison_path),
            "flipkart_fsn_run_comparison": str(local_fsn_comparison_path),
            "looker_flipkart_run_comparison": str(local_looker_comparison_path),
        },
        "run_status_distribution": dict(run_status_counts),
        "fsn_status_distribution": dict(fsn_status_counts),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        build_latest_run_comparison()
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
