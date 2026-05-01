from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_cogs_helpers import (
    COGS_AVAILABLE_STATUSES,
    count_cogs_rows,
    hydrate_analysis_rows,
    is_cogs_available,
)
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    append_csv_log,
    clean_fsn,
    normalize_text,
    now_iso,
    parse_float,
)

RUNS_DIR = OUTPUT_DIR / "runs"
SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_alerts_tasks_log.csv"

ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
CUSTOMER_RETURN_SUMMARY_TAB = "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY"
COURIER_RETURN_SUMMARY_TAB = "FLIPKART_COURIER_RETURN_SUMMARY"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
TRACKER_TAB = "FLIPKART_ACTION_TRACKER"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"

ALERT_HEADERS = [
    "Alert_ID",
    "Run_ID",
    "Report_Start_Date",
    "Report_End_Date",
    "Alert_Date",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Alert_Type",
    "Severity",
    "Trigger_Value",
    "Threshold",
    "Suggested_Action",
    "Reason",
    "Source_Field",
    "Data_Confidence",
    "Status_Default",
    "Last_Updated",
]

TRACKER_HEADERS = [
    "Action_ID",
    "Alert_ID",
    "First_Seen_Run_ID",
    "Latest_Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Alert_Type",
    "Severity",
    "Suggested_Action",
    "Owner",
    "Status",
    "Action_Taken",
    "Action_Date",
    "Expected_Impact",
    "Review_After_Date",
    "Review_After_Run_ID",
    "Evidence_Link",
    "Resolution_Notes",
    "Last_Updated",
]

ACTIVE_TASK_HEADERS = [
    "Alert_ID",
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Alert_Type",
    "Severity",
    "Suggested_Action",
    "Reason",
    "Owner",
    "Status",
    "Action_Taken",
    "Action_Date",
    "Review_After_Date",
    "Days_Open",
    "Data_Confidence",
    "Trigger_Value",
    "Threshold",
    "Last_Updated",
]

STATUS_OPTIONS = [
    "Open",
    "Assigned",
    "In Progress",
    "Done",
    "Waiting For Fresh Data",
    "Resolved",
    "Ignored",
    "Reopened",
]

SEVERITY_ORDER = {
    "Critical": 0,
    "High": 1,
    "Medium": 2,
    "Low": 3,
}

TOP_ALERT_TYPE_PRIORITY = {
    "Negative Final Profit": 0,
    "Critical Customer Return Rate": 1,
    "Negative Profit Before COGS": 2,
    "Listing Not Active": 3,
    "Low Confidence With Sales": 4,
    "Low Final Profit Margin": 0,
    "High Customer Return Rate": 1,
    "High Courier Return Rate": 2,
    "High Cancellation / RTO": 3,
    "High Attempts Exhausted": 4,
    "High Shipment Ageing": 5,
    "High Not Serviceable": 6,
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


def get_latest_run_dir() -> Path:
    if not RUNS_DIR.exists():
        raise FileNotFoundError(f"Missing Flipkart runs folder: {RUNS_DIR}")
    run_dirs = [
        path
        for path in RUNS_DIR.iterdir()
        if path.is_dir() and path.name.startswith("FLIPKART_") and (path / "pipeline_run_summary.json").exists()
    ]
    if not run_dirs:
        raise FileNotFoundError(f"No completed Flipkart runs found in: {RUNS_DIR}")
    return sorted(run_dirs, key=lambda path: path.name)[-1]


def parse_run_date(run_id: str) -> date:
    try:
        return datetime.strptime(run_id, "FLIPKART_%Y%m%d_%H%M%S").date()
    except ValueError:
        return datetime.now().date()


def parse_iso_date(value: Any) -> Optional[date]:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be 1 or greater")
    result: List[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return
    raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return int(props["sheetId"])
    response = retry(
        lambda: sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        )
        .execute()
    )
    return int(response["replies"][0]["addSheet"]["properties"]["sheetId"])


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
        .get("values", [])
    )


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
        .clear(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A:ZZ",
            body={},
        )
        .execute()
    )


def write_rows(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
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


def freeze_bold_resize(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int) -> None:
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


def set_status_validation(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    column_index: int,
    max_rows: int = 5000,
) -> None:
    requests = [
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": max_rows,
                    "startColumnIndex": column_index,
                    "endColumnIndex": column_index + 1,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in STATUS_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        }
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def normalize_status(value: Any) -> str:
    text = normalize_text(value)
    for option in STATUS_OPTIONS:
        if text.lower() == option.lower():
            return option
    return text


def stable_alert_id(fsn: str, alert_type: str, source_field: str) -> str:
    key = "|".join([normalize_text(fsn).upper(), normalize_text(alert_type).upper(), normalize_text(source_field).upper()])
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12].upper()
    return f"FKA-{digest}"


def stable_action_id(alert_id: str) -> str:
    digest = hashlib.sha1(normalize_text(alert_id).encode("utf-8")).hexdigest()[:12].upper()
    return f"FKACTION-{digest}"


def format_number(value: Any, decimals: int = 2) -> str:
    number = parse_float(value)
    if decimals <= 0:
        return str(int(round(number)))
    if float(number).is_integer():
        return str(int(round(number)))
    return f"{number:.{decimals}f}".rstrip("0").rstrip(".")


def is_listing_not_active(listing_status: str) -> bool:
    text = normalize_text(listing_status).lower()
    if not text:
        return False
    blocked_tokens = ["blocked", "rejected", "inactive", "paused", "disabled", "unlisted", "archived"]
    return any(token in text for token in blocked_tokens)


def build_reason(row: Dict[str, str], fallback: str) -> str:
    reason = normalize_text(row.get("Reason", ""))
    return reason or fallback


def build_alert_row(
    *,
    summary: Dict[str, Any],
    row: Dict[str, str],
    alert_type: str,
    severity: str,
    trigger_value: str,
    threshold: str,
    suggested_action: str,
    reason: str,
    source_field: str,
) -> Dict[str, Any]:
    fsn = clean_fsn(row.get("FSN", ""))
    sku_id = normalize_text(row.get("SKU_ID", ""))
    alert_id = stable_alert_id(fsn, alert_type, source_field)
    run_id = str(summary.get("run_id", ""))
    report_start_date = str(summary.get("report_start_date", ""))
    report_end_date = str(summary.get("report_end_date", ""))
    run_date = parse_run_date(run_id).isoformat()
    data_confidence = normalize_text(row.get("Data_Confidence", "")).upper()

    return {
        "Alert_ID": alert_id,
        "Run_ID": run_id,
        "Report_Start_Date": report_start_date,
        "Report_End_Date": report_end_date,
        "Alert_Date": run_date,
        "FSN": fsn,
        "SKU_ID": sku_id,
        "Product_Title": normalize_text(row.get("Product_Title", "")),
        "Alert_Type": alert_type,
        "Severity": severity,
        "Trigger_Value": trigger_value,
        "Threshold": threshold,
        "Suggested_Action": suggested_action,
        "Reason": reason,
        "Source_Field": source_field,
        "Data_Confidence": data_confidence,
        "Status_Default": "Open",
        "Last_Updated": now_iso(),
    }


def build_return_issue_reason(row: Dict[str, str]) -> str:
    top_issue_category = normalize_text(row.get("Top_Issue_Category", "")) or "Other"
    top_reason = normalize_text(row.get("Top_Return_Reason", ""))
    top_sub_reason = normalize_text(row.get("Top_Return_Sub_Reason", ""))
    parts = [f"Top issue: {top_issue_category}"]
    if top_reason:
        parts.append(f"Reason: {top_reason}")
    if top_sub_reason:
        parts.append(f"Sub reason: {top_sub_reason}")
    return " | ".join(parts)


def build_alerts(
    summary: Dict[str, Any],
    analysis_rows: Sequence[Dict[str, str]],
    customer_summary_rows: Sequence[Dict[str, str]],
    courier_summary_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    seen_alert_ids: set[str] = set()
    customer_lookup = {clean_fsn(row.get("FSN", "")): dict(row) for row in customer_summary_rows if clean_fsn(row.get("FSN", ""))}
    courier_lookup = {clean_fsn(row.get("FSN", "")): dict(row) for row in courier_summary_rows if clean_fsn(row.get("FSN", ""))}

    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue

        orders = parse_float(row.get("Orders", ""))
        stock = parse_float(row.get("Stock", ""))
        return_rate = parse_float(row.get("Return_Rate", ""))
        net_profit_before_cogs = parse_float(row.get("Net_Profit_Before_COGS", ""))
        final_net_profit_raw = normalize_text(row.get("Final_Net_Profit", ""))
        final_profit_margin_raw = normalize_text(row.get("Final_Profit_Margin", ""))
        cogs_status_raw = normalize_text(row.get("COGS_Status", "")).upper()
        cogs_available = cogs_status_raw in COGS_AVAILABLE_STATUSES and final_net_profit_raw != ""
        final_net_profit = parse_float(final_net_profit_raw) if final_net_profit_raw else 0.0
        final_profit_margin = parse_float(final_profit_margin_raw) if final_profit_margin_raw else 0.0
        acos_raw = normalize_text(row.get("ACOS", ""))
        roas_raw = normalize_text(row.get("ROAS", ""))
        acos = parse_float(acos_raw)
        roas = parse_float(roas_raw)
        estimated_ad_spend_raw = normalize_text(row.get("Estimated_Ad_Spend", ""))
        estimated_ad_spend = parse_float(estimated_ad_spend_raw)
        data_confidence = normalize_text(row.get("Data_Confidence", "")).upper()
        missing_data = normalize_text(row.get("Missing_Data", ""))
        listing_status = normalize_text(row.get("Listing_Status", ""))
        customer_row = customer_lookup.get(fsn, {})
        courier_row = courier_lookup.get(fsn, {})
        customer_return_rate = parse_float(customer_row.get("Customer_Return_Rate", ""))
        courier_return_rate = parse_float(courier_row.get("Courier_Return_Rate", ""))
        customer_return_count = int(parse_float(customer_row.get("Customer_Return_Count", "")))
        courier_return_count = int(parse_float(courier_row.get("Courier_Return_Count", "")))
        defective_count = int(parse_float(customer_row.get("Defective_Product_Count", "")))
        damaged_count = int(parse_float(customer_row.get("Damaged_Product_Count", "")))
        missing_item_count = int(parse_float(customer_row.get("Missing_Item_Count", "")))
        wrong_product_count = int(parse_float(customer_row.get("Wrong_Product_Count", "")))
        remorse_count = int(parse_float(customer_row.get("Customer_Remorse_Count", "")))
        quality_issue_count = int(parse_float(customer_row.get("Quality_Issue_Count", "")))
        order_cancelled_count = int(parse_float(courier_row.get("Order_Cancelled_Count", "")))
        attempts_exhausted_count = int(parse_float(courier_row.get("Attempts_Exhausted_Count", "")))
        shipment_ageing_count = int(parse_float(courier_row.get("Shipment_Ageing_Count", "")))
        not_serviceable_count = int(parse_float(courier_row.get("Not_Serviceable_Count", "")))
        orc_validated_count = int(parse_float(courier_row.get("ORC_Validated_Count", "")))
        delivery_failed_count = int(parse_float(courier_row.get("Delivery_Failed_Count", "")))
        base_reason = build_reason(row, "Triggered from FLIPKART_SKU_ANALYSIS")

        def add_alert(
            *,
            alert_type: str,
            severity: str,
            trigger_value: str,
            threshold: str,
            suggested_action: str,
            reason: str,
            source_field: str,
        ) -> None:
            alert_id = stable_alert_id(fsn, alert_type, source_field)
            if alert_id in seen_alert_ids:
                return
            seen_alert_ids.add(alert_id)
            alerts.append(
                build_alert_row(
                    summary=summary,
                    row=row,
                    alert_type=alert_type,
                    severity=severity,
                    trigger_value=trigger_value,
                    threshold=threshold,
                    suggested_action=suggested_action,
                    reason=reason,
                    source_field=source_field,
                )
            )

        if data_confidence == "LOW" and orders > 0:
            add_alert(
                alert_type="Low Confidence With Sales",
                severity="Critical",
                trigger_value="LOW",
                threshold="Orders > 0",
                suggested_action="Data Check Required",
                reason=base_reason,
                source_field="Data_Confidence",
            )

        if cogs_available and final_net_profit < 0:
            add_alert(
                alert_type="Negative Final Profit",
                severity="Critical",
                trigger_value=format_number(final_net_profit, 2),
                threshold="< 0",
                suggested_action="Stop/Review Product Economics",
                reason=f"{base_reason} | Final profit after COGS is negative",
                source_field="Final_Net_Profit",
            )

        if net_profit_before_cogs < 0:
            add_alert(
                alert_type="Negative Profit Before COGS",
                severity="Critical",
                trigger_value=format_number(net_profit_before_cogs, 2),
                threshold=">= 0",
                suggested_action="Investigate Profit",
                reason=base_reason,
                source_field="Net_Profit_Before_COGS",
            )

        if not cogs_available and orders > 0:
            add_alert(
                alert_type="COGS Missing For Sold FSN",
                severity="Medium",
                trigger_value=cogs_status_raw or "Missing",
                threshold="Orders > 0",
                suggested_action="Fill Cost in FLIPKART_COST_MASTER",
                reason=f"{base_reason} | COGS missing for sold FSN",
                source_field="COGS_Status",
            )

        if not cogs_available and data_confidence == "HIGH":
            add_alert(
                alert_type="COGS Missing For High Confidence FSN",
                severity="Medium",
                trigger_value=cogs_status_raw or "Missing",
                threshold="Data_Confidence = HIGH",
                suggested_action="Fill Cost First",
                reason=f"{base_reason} | COGS missing for high confidence FSN",
                source_field="COGS_Status",
            )

        if cogs_available and final_profit_margin < 0.10:
            add_alert(
                alert_type="Low Final Profit Margin",
                severity="High",
                trigger_value=final_profit_margin_raw or format_number(final_profit_margin, 4),
                threshold="< 0.10",
                suggested_action="Improve Price/Cost/Ads",
                reason=f"{base_reason} | Final profit margin is below 10%",
                source_field="Final_Profit_Margin",
            )

        if customer_row and customer_return_rate >= 0.50 and orders >= 2:
            add_alert(
                alert_type="Critical Customer Return Rate",
                severity="Critical",
                trigger_value=f"{customer_return_rate:.2f}",
                threshold=">= 0.50 and Orders >= 2",
                suggested_action="Fix Product/Listing",
                reason=f"{base_reason} | Customer return rate is critical",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

        if is_listing_not_active(listing_status):
            add_alert(
                alert_type="Listing Not Active",
                severity="Critical",
                trigger_value=listing_status or "Inactive",
                threshold="blocked/rejected/inactive",
                suggested_action="Fix Listing",
                reason=base_reason,
                source_field="Listing_Status",
            )

        if customer_row and customer_return_rate >= 0.20 and orders >= 2:
            add_alert(
                alert_type="High Customer Return Rate",
                severity="High",
                trigger_value=f"{customer_return_rate:.2f}",
                threshold=">= 0.20 and Orders >= 2",
                suggested_action="Fix Product/Listing",
                reason=f"{base_reason} | Customer return rate is elevated",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

        if courier_row and courier_return_rate >= 0.20 and orders >= 2:
            add_alert(
                alert_type="High Courier Return Rate",
                severity="High",
                trigger_value=f"{courier_return_rate:.2f}",
                threshold=">= 0.20 and Orders >= 2",
                suggested_action="Check Logistics",
                reason=f"{base_reason} | Courier return rate is elevated",
                source_field=COURIER_RETURN_SUMMARY_TAB,
            )

        if courier_row and (order_cancelled_count + attempts_exhausted_count) >= 2:
            add_alert(
                alert_type="High Cancellation / RTO",
                severity="High",
                trigger_value=str(order_cancelled_count + attempts_exhausted_count),
                threshold=">= 2",
                suggested_action="Check Cancellation / RTO Process",
                reason=f"{base_reason} | Cancellation/RTO count: {order_cancelled_count + attempts_exhausted_count}",
                source_field=COURIER_RETURN_SUMMARY_TAB,
            )

        if courier_row and attempts_exhausted_count >= 2:
            add_alert(
                alert_type="High Attempts Exhausted",
                severity="High",
                trigger_value=str(attempts_exhausted_count),
                threshold=">= 2",
                suggested_action="Check Delivery Attempts / Contactability",
                reason=f"{base_reason} | Attempts exhausted count: {attempts_exhausted_count}",
                source_field=COURIER_RETURN_SUMMARY_TAB,
            )

        if courier_row and shipment_ageing_count >= 2:
            add_alert(
                alert_type="High Shipment Ageing",
                severity="Medium",
                trigger_value=str(shipment_ageing_count),
                threshold=">= 2",
                suggested_action="Check Shipment Aging / TAT",
                reason=f"{base_reason} | Shipment ageing count: {shipment_ageing_count}",
                source_field=COURIER_RETURN_SUMMARY_TAB,
            )

        if courier_row and not_serviceable_count > 0:
            add_alert(
                alert_type="High Not Serviceable",
                severity="High",
                trigger_value=str(not_serviceable_count),
                threshold="> 0",
                suggested_action="Check Serviceability / Pincode Coverage",
                reason=f"{base_reason} | Not serviceable count: {not_serviceable_count}",
                source_field=COURIER_RETURN_SUMMARY_TAB,
            )

        if customer_row and defective_count > 0:
            add_alert(
                alert_type="Product Not Working Returns",
                severity="High",
                trigger_value=str(defective_count),
                threshold="> 0",
                suggested_action="Check QC / Supplier / Product Defect",
                reason=f"{base_reason} | Defective customer returns: {defective_count}",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

        if customer_row and damaged_count > 0:
            add_alert(
                alert_type="Packaging Damage Issue",
                severity="High",
                trigger_value=str(damaged_count),
                threshold="> 0",
                suggested_action="Improve Packaging / Courier Handling",
                reason=f"{base_reason} | Damaged customer returns: {damaged_count}",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

        if customer_row and wrong_product_count > 0:
            add_alert(
                alert_type="Listing Expectation Mismatch",
                severity="High",
                trigger_value=str(wrong_product_count),
                threshold="> 0",
                suggested_action="Improve Picking / Listing Claims",
                reason=f"{base_reason} | Wrong product customer returns: {wrong_product_count}",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

        if customer_row and quality_issue_count >= 2:
            add_alert(
                alert_type="Product Issue Cluster",
                severity="High",
                trigger_value=str(quality_issue_count),
                threshold=">= 2",
                suggested_action="Fix Product / Listing First",
                reason=f"{base_reason} | Customer product issue cluster: {quality_issue_count}",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

        if customer_row and remorse_count >= 2:
            add_alert(
                alert_type="Customer Return Cluster",
                severity="Low",
                trigger_value=str(remorse_count),
                threshold=">= 2",
                suggested_action="Monitor Demand / Positioning",
                reason=f"{base_reason} | Customer remorse count: {remorse_count}",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

        if "Settlement Missing" in missing_data and orders > 0:
            add_alert(
                alert_type="Settlement Missing",
                severity="High",
                trigger_value="Settlement Missing",
                threshold="Orders > 0",
                suggested_action="Check Settlement Mapping/Data",
                reason=base_reason,
                source_field="Missing_Data",
            )

        if "PNL Missing" in missing_data and orders > 0:
            add_alert(
                alert_type="PNL Missing",
                severity="High",
                trigger_value="PNL Missing",
                threshold="Orders > 0",
                suggested_action="Check PNL Mapping/Data",
                reason=base_reason,
                source_field="Missing_Data",
            )

        if "Listing Missing" in missing_data:
            add_alert(
                alert_type="Listing Missing",
                severity="High",
                trigger_value="Listing Missing",
                threshold="Present in Missing_Data",
                suggested_action="Check Listing Status",
                reason=base_reason,
                source_field="Missing_Data",
            )

        if acos_raw and acos > 0.35:
            add_alert(
                alert_type="High ACOS",
                severity="High",
                trigger_value=acos_raw,
                threshold="> 0.35",
                suggested_action="Reduce Ads",
                reason=base_reason,
                source_field="ACOS",
            )

        if roas_raw and roas < 1.5 and estimated_ad_spend > 0:
            add_alert(
                alert_type="Low ROAS",
                severity="High",
                trigger_value=roas_raw,
                threshold="< 1.5 and Estimated_Ad_Spend > 0",
                suggested_action="Review Ads",
                reason=base_reason,
                source_field="ROAS",
            )

        if orders == 0 and stock > 0:
            add_alert(
                alert_type="No Orders With Stock",
                severity="Medium",
                trigger_value=f"Orders={int(round(orders))}, Stock={format_number(stock, 0)}",
                threshold="Orders = 0 and Stock > 0",
                suggested_action="Improve Listing/Traffic",
                reason=base_reason,
                source_field="Orders",
            )

        if data_confidence == "MEDIUM":
            add_alert(
                alert_type="Medium Confidence",
                severity="Medium",
                trigger_value="MEDIUM",
                threshold="Data_Confidence = MEDIUM",
                suggested_action="Review Data",
                reason=base_reason,
                source_field="Data_Confidence",
            )

        if "Ads Missing" in missing_data and orders > 0:
            add_alert(
                alert_type="Ads Data Missing",
                severity="Medium",
                trigger_value="Ads Missing",
                threshold="Orders > 0",
                suggested_action="Check Ads Mapping",
                reason=base_reason,
                source_field="Missing_Data",
            )

        if data_confidence == "LOW" and orders == 0:
            add_alert(
                alert_type="Low Confidence No Sales",
                severity="Low",
                trigger_value="LOW",
                threshold="Orders = 0",
                suggested_action="Monitor/Data Check",
                reason=base_reason,
                source_field="Data_Confidence",
            )

        if customer_row and customer_return_rate >= 0.50 and final_profit_margin < 0.10:
            add_alert(
                alert_type="Critical Customer Return Rate",
                severity="Critical",
                trigger_value=f"{customer_return_rate:.2f}",
                threshold=">= 0.50 and Orders >= 2",
                suggested_action="Fix Product First",
                reason=f"{base_reason} | High customer return pressure and weak profit",
                source_field=CUSTOMER_RETURN_SUMMARY_TAB,
            )

    alerts.sort(
        key=lambda alert: (
            SEVERITY_ORDER.get(alert["Severity"], 99),
            TOP_ALERT_TYPE_PRIORITY.get(normalize_text(alert.get("Alert_Type", "")), 99),
            alert["FSN"],
            alert["Alert_Type"],
        )
    )
    return alerts


def build_return_issue_alerts(
    summary: Dict[str, Any],
    customer_summary_rows: Sequence[Dict[str, str]],
    courier_summary_rows: Sequence[Dict[str, str]],
    existing_alert_ids: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    return []


def build_tracker_lookup(rows: Sequence[Dict[str, str]]) -> Dict[str, Tuple[int, Dict[str, str]]]:
    lookup: Dict[str, Tuple[int, Dict[str, str]]] = {}
    for index, row in enumerate(rows):
        alert_id = normalize_text(row.get("Alert_ID", ""))
        if alert_id and alert_id not in lookup:
            lookup[alert_id] = (index, dict(row))
    return lookup


def build_tracker_row(
    *,
    alert: Dict[str, Any],
    existing_row: Optional[Dict[str, str]],
    current_run_id: str,
    current_run_date: date,
    reopen: bool = False,
) -> Dict[str, Any]:
    owner = normalize_text(existing_row.get("Owner")) if existing_row else ""
    status = normalize_status(existing_row.get("Status")) if existing_row else "Open"
    if not status:
        status = "Open"
    if reopen:
        status = "Reopened"

    first_seen_run_id = normalize_text(existing_row.get("First_Seen_Run_ID")) if existing_row else ""
    if not first_seen_run_id:
        first_seen_run_id = current_run_id

    action_id = normalize_text(existing_row.get("Action_ID")) if existing_row else ""
    if not action_id:
        action_id = stable_action_id(alert["Alert_ID"])

    return {
        "Action_ID": action_id,
        "Alert_ID": alert["Alert_ID"],
        "First_Seen_Run_ID": first_seen_run_id,
        "Latest_Run_ID": current_run_id,
        "FSN": alert["FSN"],
        "SKU_ID": alert["SKU_ID"],
        "Product_Title": alert["Product_Title"],
        "Alert_Type": alert["Alert_Type"],
        "Severity": alert["Severity"],
        "Suggested_Action": alert["Suggested_Action"],
        "Owner": owner,
        "Status": status,
        "Action_Taken": normalize_text(existing_row.get("Action_Taken")) if existing_row else "",
        "Action_Date": normalize_text(existing_row.get("Action_Date")) if existing_row else "",
        "Expected_Impact": normalize_text(existing_row.get("Expected_Impact")) if existing_row else "",
        "Review_After_Date": normalize_text(existing_row.get("Review_After_Date")) if existing_row else "",
        "Review_After_Run_ID": normalize_text(existing_row.get("Review_After_Run_ID")) if existing_row else "",
        "Evidence_Link": normalize_text(existing_row.get("Evidence_Link")) if existing_row else "",
        "Resolution_Notes": normalize_text(existing_row.get("Resolution_Notes")) if existing_row else "",
        "Last_Updated": now_iso(),
    }


def should_reopen(existing_row: Dict[str, str], current_run_date: date) -> bool:
    review_after_date = parse_iso_date(existing_row.get("Review_After_Date"))
    if review_after_date is None:
        return False
    return current_run_date > review_after_date


def merge_tracker_rows(
    summary: Dict[str, Any],
    alerts: Sequence[Dict[str, Any]],
    tracker_rows: Sequence[Dict[str, str]],
) -> Tuple[List[Dict[str, Any]], int, int]:
    current_run_id = str(summary.get("run_id", ""))
    current_run_date = parse_run_date(current_run_id)
    merged_rows = [dict(row) for row in tracker_rows]
    tracker_lookup = build_tracker_lookup(merged_rows)
    created = 0
    updated = 0

    for alert in alerts:
        existing = tracker_lookup.get(alert["Alert_ID"])
        if existing is None:
            new_row = build_tracker_row(
                alert=alert,
                existing_row=None,
                current_run_id=current_run_id,
                current_run_date=current_run_date,
            )
            merged_rows.append(new_row)
            tracker_lookup[alert["Alert_ID"]] = (len(merged_rows) - 1, new_row)
            created += 1
            continue

        row_index, existing_row = existing
        status = normalize_status(existing_row.get("Status"))
        if status in {"Resolved", "Ignored"}:
            continue

        if status in {"Done", "Waiting For Fresh Data"}:
            if should_reopen(existing_row, current_run_date):
                merged_rows[row_index] = build_tracker_row(
                    alert=alert,
                    existing_row=existing_row,
                    current_run_id=current_run_id,
                    current_run_date=current_run_date,
                    reopen=True,
                )
                updated += 1
            continue

        merged_rows[row_index] = build_tracker_row(
            alert=alert,
            existing_row=existing_row,
            current_run_id=current_run_id,
            current_run_date=current_run_date,
        )
        updated += 1

    return merged_rows, created, updated


def build_active_task_rows(
    summary: Dict[str, Any],
    alerts: Sequence[Dict[str, Any]],
    tracker_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    current_run_id = str(summary.get("run_id", ""))
    current_run_date = parse_run_date(current_run_id)
    tracker_lookup = {normalize_text(row.get("Alert_ID", "")): row for row in tracker_rows if normalize_text(row.get("Alert_ID", ""))}
    active_rows: List[Dict[str, Any]] = []

    for alert in alerts:
        tracker_row = tracker_lookup.get(alert["Alert_ID"], {})
        status = normalize_status(tracker_row.get("Status")) or "Open"
        if status in {"Resolved", "Ignored"}:
            continue

        first_seen_run_id = normalize_text(tracker_row.get("First_Seen_Run_ID")) or current_run_id
        days_open = 0
        first_seen_date = parse_run_date(first_seen_run_id) if first_seen_run_id else current_run_date
        if first_seen_date <= current_run_date:
            days_open = max(0, (current_run_date - first_seen_date).days)

        active_rows.append(
            {
                "Alert_ID": alert["Alert_ID"],
                "Run_ID": current_run_id,
                "FSN": alert["FSN"],
                "SKU_ID": alert["SKU_ID"],
                "Product_Title": alert["Product_Title"],
                "Alert_Type": alert["Alert_Type"],
                "Severity": alert["Severity"],
                "Suggested_Action": alert["Suggested_Action"],
                "Reason": alert["Reason"],
                "Owner": normalize_text(tracker_row.get("Owner")),
                "Status": status,
                "Action_Taken": normalize_text(tracker_row.get("Action_Taken")),
                "Action_Date": normalize_text(tracker_row.get("Action_Date")),
                "Review_After_Date": normalize_text(tracker_row.get("Review_After_Date")),
                "Days_Open": days_open,
                "Data_Confidence": alert["Data_Confidence"],
                "Trigger_Value": alert["Trigger_Value"],
                "Threshold": alert["Threshold"],
                "Last_Updated": normalize_text(tracker_row.get("Last_Updated")) or alert["Last_Updated"],
                "_sort_severity": SEVERITY_ORDER.get(alert["Severity"], 99),
            }
        )

    active_rows.sort(key=lambda row: (row["_sort_severity"], -int(row["Days_Open"]), row["Alert_ID"]))
    for row in active_rows:
        row.pop("_sort_severity", None)
    return active_rows


def print_json(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def count_alert_types(alerts: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    counter = Counter(normalize_text(alert.get("Alert_Type", "")) for alert in alerts)
    return {key: value for key, value in counter.items() if key}


def append_log_row(payload: Dict[str, Any]) -> None:
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "run_id",
            "generated_alert_count",
            "tracker_rows_created",
            "tracker_rows_updated",
            "active_tasks_count",
            "status",
            "message",
        ],
        [payload],
    )


def create_flipkart_alerts_and_tasks() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    latest_run_dir = get_latest_run_dir()
    summary_path = latest_run_dir / "pipeline_run_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing required file: {summary_path}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    ensure_required_tab_exists(sheets_service, spreadsheet_id, ANALYSIS_TAB)
    ensure_required_tab_exists(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    ensure_required_tab_exists(sheets_service, spreadsheet_id, CUSTOMER_RETURN_SUMMARY_TAB)
    ensure_required_tab_exists(sheets_service, spreadsheet_id, COURIER_RETURN_SUMMARY_TAB)

    alerts_sheet_id = ensure_tab(sheets_service, spreadsheet_id, ALERTS_TAB)
    tracker_sheet_id = ensure_tab(sheets_service, spreadsheet_id, TRACKER_TAB)
    active_tasks_sheet_id = ensure_tab(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)

    summary = load_json(summary_path)
    _, analysis_rows = read_table(sheets_service, spreadsheet_id, ANALYSIS_TAB)
    _, cost_rows = read_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    _, customer_return_rows = read_table(sheets_service, spreadsheet_id, CUSTOMER_RETURN_SUMMARY_TAB)
    _, courier_return_rows = read_table(sheets_service, spreadsheet_id, COURIER_RETURN_SUMMARY_TAB)
    live_analysis_rows = hydrate_analysis_rows(analysis_rows, cost_rows)
    alerts = build_alerts(summary, live_analysis_rows, customer_return_rows, courier_return_rows)
    return_issue_alerts = build_return_issue_alerts(
        summary,
        customer_return_rows,
        courier_return_rows,
        existing_alert_ids={alert["Alert_ID"] for alert in alerts},
    )
    alerts.extend(return_issue_alerts)
    alerts.sort(
        key=lambda alert: (
            SEVERITY_ORDER.get(alert["Severity"], 99),
            TOP_ALERT_TYPE_PRIORITY.get(normalize_text(alert.get("Alert_Type", "")), 99),
            alert["FSN"],
            alert["Alert_Type"],
        )
    )
    alert_type_counts = count_alert_types(alerts)
    cogs_available_count, cogs_missing_count = count_cogs_rows(live_analysis_rows)

    _, existing_tracker_rows = read_table(sheets_service, spreadsheet_id, TRACKER_TAB)
    merged_tracker_rows, tracker_rows_created, tracker_rows_updated = merge_tracker_rows(summary, alerts, existing_tracker_rows)
    active_tasks_rows = build_active_task_rows(summary, alerts, merged_tracker_rows)

    clear_tab(sheets_service, spreadsheet_id, ALERTS_TAB)
    write_rows(sheets_service, spreadsheet_id, ALERTS_TAB, ALERT_HEADERS, alerts)

    tracker_values = [TRACKER_HEADERS] + [[row.get(header, "") for header in TRACKER_HEADERS] for row in merged_tracker_rows]
    tracker_end_col = column_index_to_a1(len(TRACKER_HEADERS))
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{TRACKER_TAB}!A1:{tracker_end_col}{len(tracker_values)}",
            valueInputOption="RAW",
            body={"values": tracker_values},
        )
        .execute()
    )

    clear_tab(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)
    write_rows(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB, ACTIVE_TASK_HEADERS, active_tasks_rows)

    freeze_bold_resize(sheets_service, spreadsheet_id, alerts_sheet_id, len(ALERT_HEADERS))
    freeze_bold_resize(sheets_service, spreadsheet_id, tracker_sheet_id, len(TRACKER_HEADERS))
    freeze_bold_resize(sheets_service, spreadsheet_id, active_tasks_sheet_id, len(ACTIVE_TASK_HEADERS))
    set_status_validation(sheets_service, spreadsheet_id, tracker_sheet_id, TRACKER_HEADERS.index("Status"))
    set_status_validation(sheets_service, spreadsheet_id, active_tasks_sheet_id, ACTIVE_TASK_HEADERS.index("Status"))

    generated_alert_count = len(alerts)
    critical_count = sum(1 for alert in alerts if alert["Severity"] == "Critical")
    high_count = sum(1 for alert in alerts if alert["Severity"] == "High")
    medium_count = sum(1 for alert in alerts if alert["Severity"] == "Medium")
    low_count = sum(1 for alert in alerts if alert["Severity"] == "Low")
    return_issue_alert_count = sum(
        1
        for alert in alerts
        if normalize_text(alert.get("Source_Field", "")) in {CUSTOMER_RETURN_SUMMARY_TAB, COURIER_RETURN_SUMMARY_TAB}
    )
    critical_return_issue_alert_count = sum(
        1
        for alert in alerts
        if normalize_text(alert.get("Source_Field", "")) in {CUSTOMER_RETURN_SUMMARY_TAB, COURIER_RETURN_SUMMARY_TAB}
        and alert["Severity"] == "Critical"
    )
    product_issue_cluster_alert_count = sum(
        1
        for alert in alerts
        if normalize_text(alert.get("Source_Field", "")) == CUSTOMER_RETURN_SUMMARY_TAB
        and normalize_text(alert.get("Alert_Type", "")) in {"Product Issue Cluster", "Product Not Working Returns", "Packaging Damage Issue", "Listing Expectation Mismatch"}
    )
    logistics_return_alert_count = sum(
        1
        for alert in alerts
        if normalize_text(alert.get("Source_Field", "")) == COURIER_RETURN_SUMMARY_TAB
        and normalize_text(alert.get("Alert_Type", "")) in {"High Courier Return Rate", "High Cancellation / RTO", "High Attempts Exhausted", "High Shipment Ageing", "High Not Serviceable"}
    )
    active_tasks_count = len(active_tasks_rows)

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "run_id": summary.get("run_id", ""),
        "generated_alert_count": generated_alert_count,
        "tracker_rows_created": tracker_rows_created,
        "tracker_rows_updated": tracker_rows_updated,
        "active_tasks_count": active_tasks_count,
        "status": "SUCCESS",
        "message": "Updated Flipkart alerts, tracker, and active tasks",
    }
    append_log_row(log_row)

    result = {
        "status": "SUCCESS",
        "run_id": summary.get("run_id", ""),
        "generated_alert_count": generated_alert_count,
        "critical_alert_count": critical_count,
        "high_alert_count": high_count,
        "medium_alert_count": medium_count,
        "low_alert_count": low_count,
        "return_issue_alert_count": return_issue_alert_count,
        "critical_return_issue_alert_count": critical_return_issue_alert_count,
        "product_issue_cluster_alert_count": product_issue_cluster_alert_count,
        "logistics_return_alert_count": logistics_return_alert_count,
        "negative_final_profit_alert_count": alert_type_counts.get("Negative Final Profit", 0),
        "low_final_profit_margin_alert_count": alert_type_counts.get("Low Final Profit Margin", 0),
        "cogs_missing_sold_alert_count": alert_type_counts.get("COGS Missing For Sold FSN", 0),
        "cogs_missing_high_confidence_alert_count": alert_type_counts.get("COGS Missing For High Confidence FSN", 0),
        "tracker_rows_created": tracker_rows_created,
        "tracker_rows_updated": tracker_rows_updated,
        "active_tasks_count": active_tasks_count,
        "cogs_available_fsn_count": cogs_available_count,
        "cogs_missing_fsn_count": cogs_missing_count,
        "alert_type_counts": alert_type_counts,
        "tabs_updated": [ALERTS_TAB, TRACKER_TAB, ACTIVE_TASKS_TAB],
        "spreadsheet_id": spreadsheet_id,
        "latest_run_dir": str(latest_run_dir),
        "summary_path": str(summary_path),
        "analysis_tab": ANALYSIS_TAB,
        "cost_master_tab": COST_MASTER_TAB,
        "log_path": str(LOG_PATH),
    }
    print_json(result)
    return result


def main() -> None:
    try:
        create_flipkart_alerts_and_tasks()
    except Exception as exc:
        error_payload = {
            "status": "ERROR",
            "error_type": exc.__class__.__name__,
            "message": str(exc),
            "log_path": str(LOG_PATH),
        }
        print_json(error_payload)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
