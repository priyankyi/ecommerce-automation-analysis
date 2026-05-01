from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pandas as pd
from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import clear_tab, ensure_tab, freeze_and_format, load_json, write_rows
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    NORMALIZED_ORDERS_PATH,
    OUTPUT_DIR,
    SKU_ANALYSIS_PATH,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    normalize_text,
    now_iso,
    parse_float,
    write_csv,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_return_intelligence_v2_log.csv"
RAW_CSV_PATH = PROJECT_ROOT / "data" / "input" / "marketplaces" / "flipkart" / "raw" / "Returns Report.csv"
RAW_XLSX_PATH = PROJECT_ROOT / "data" / "input" / "marketplaces" / "flipkart" / "raw" / "Returns.xlsx"

DETAIL_TAB = "FLIPKART_RETURN_ALL_DETAILS"
CUSTOMER_COMMENTS_TAB = "FLIPKART_CUSTOMER_RETURN_COMMENTS"
COURIER_COMMENTS_TAB = "FLIPKART_COURIER_RETURN_COMMENTS"
CUSTOMER_SUMMARY_TAB = "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY"
COURIER_SUMMARY_TAB = "FLIPKART_COURIER_RETURN_SUMMARY"
RETURN_TYPE_PIVOT_TAB = "FLIPKART_RETURN_TYPE_PIVOT"

LOOKER_DETAIL_PATH = OUTPUT_DIR / "looker_flipkart_return_all_details.csv"
LOOKER_CUSTOMER_PATH = OUTPUT_DIR / "looker_flipkart_customer_returns.csv"
LOOKER_COURIER_PATH = OUTPUT_DIR / "looker_flipkart_courier_returns.csv"
LOOKER_PIVOT_PATH = OUTPUT_DIR / "looker_flipkart_return_type_pivot.csv"
LOCAL_DETAIL_PATH = OUTPUT_DIR / "flipkart_return_all_details.csv"
LOCAL_CUSTOMER_PATH = OUTPUT_DIR / "flipkart_customer_return_comments.csv"
LOCAL_COURIER_PATH = OUTPUT_DIR / "flipkart_courier_return_comments.csv"
LOCAL_CUSTOMER_SUMMARY_PATH = OUTPUT_DIR / "flipkart_customer_return_issue_summary.csv"
LOCAL_COURIER_SUMMARY_PATH = OUTPUT_DIR / "flipkart_courier_return_summary.csv"
LOCAL_PIVOT_PATH = OUTPUT_DIR / "flipkart_return_type_pivot.csv"
RETURN_DEBUG_PATH = OUTPUT_DIR / "flipkart_return_v2_column_mapping_debug.csv"

SOURCE_ROWS_DEBUG_HEADERS = [
    "Source_File",
    "Original_Column",
    "Mapped_Field",
    "Mapping_Confidence",
    "Sample_Value",
]

FIELD_ALIASES = {
    "Return_ID": ["Return ID", "return_id", "Return_Id", "RI"],
    "Order_ID": ["Order ID", "order_id", "order id", "orderId", "Order_Id", "Order Number", "order number"],
    "Order_Item_ID": ["Order Item ID", "order_item_id", "order item id", "orderItemId", "order_item", "item id", "OI", "OI:", "orderItem", "Replacement Order Item ID"],
    "SKU_ID": ["SKU", "sku", "Seller SKU", "seller_sku", "SKU ID", "sku_id"],
    "FSN": ["FSN", "fsn"],
    "Product_Title": ["Product", "product_title", "Product Title", "title"],
    "Return_Type": ["Return Type", "return_type", "Return_Type", "return type", "type", "Return Category", "return category"],
    "Return_Reason": ["Return Reason", "return_reason", "Return_Reason", "return reason"],
    "Return_Sub_Reason": ["Return Sub-reason", "return_sub_reason", "Return_Sub_Reason", "return sub reason", "Return Sub Reason", "sub reason"],
    "Comments": ["Comments", "comments", "Comment", "comment"],
    "Return_Status": ["Return Status", "return_status", "Return_Status", "return status", "Completion Status", "completion status", "Status", "status"],
    "Return_Result": ["Return Result", "return_result", "Return_Result", "return result", "return completion type", "Return Completion Type"],
    "Tracking_ID": ["Tracking ID", "tracking_id", "Tracking_Id"],
    "Reverse_Logistics_Tracking_ID": ["Reverse Logistics Tracking ID", "reverse_logistics_tracking_id", "reverse logistics tracking id", "Shipment ID", "shipment id"],
    "Return_Requested_Date": ["Return Requested Date", "return_requested_date", "return requested date"],
    "Return_Approval_Date": ["Return Approval Date", "return_approval_date", "return approval date"],
    "Return_Completion_Date": ["Return Completion Date", "return_completion_date", "return completion date", "Completed Date", "completed date"],
    "Quantity": ["Quantity", "quantity"],
}

RETURN_TYPE_CUSTOMER_VALUES = {"customer_return", "customer return", "customer", "customer_returned"}
RETURN_TYPE_COURIER_VALUES = {"courier_return", "courier return", "courier", "rto", "return_to_origin", "return to origin", "logistics return"}
RETURN_TYPE_CUSTOMER_KEYWORDS = (
    "defective",
    "damaged",
    "wrong product",
    "missing",
    "quality",
    "not as described",
    "customer return",
    "size",
    "color",
    "received",
    "broken",
    "does not work",
    "not working",
)
RETURN_TYPE_COURIER_KEYWORDS = (
    "cancelled",
    "courier",
    "rto",
    "return to origin",
    "attempts exhausted",
    "shipment ageing",
    "not serviceable",
    "orc",
    "delivery failed",
    "undelivered",
    "rejected at doorstep",
    "logistics",
)
KNOWN_PREFIX_RE = re.compile(
    r"^(?:(?:order item id|order id|order number|orderitemid|orderid|oi|ri|rtr|sku|fsn)\s*[:\-]?\s*)+",
    flags=re.IGNORECASE,
)

DETAIL_HEADERS = [
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
    "Dedupe_Key",
    "Duplicate_Source_Count",
    "Data_Gap_Reason",
    "Last_Updated",
]

CUSTOMER_HEADERS = [
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
]

COURIER_HEADERS = [
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
]

CUSTOMER_SUMMARY_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Sold_Order_Items",
    "Customer_Return_Count",
    "Customer_Return_Rate",
    "Quality_Issue_Count",
    "Defective_Product_Count",
    "Damaged_Product_Count",
    "Missing_Item_Count",
    "Wrong_Product_Count",
    "Customer_Remorse_Count",
    "Top_Customer_Return_Reason",
    "Top_Customer_Return_Sub_Reason",
    "Customer_Return_Risk_Level",
    "Suggested_Action",
    "Data_Gap_Reason",
    "Last_Updated",
]

COURIER_SUMMARY_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Sold_Order_Items",
    "Courier_Return_Count",
    "Courier_Return_Rate",
    "Order_Cancelled_Count",
    "Attempts_Exhausted_Count",
    "Shipment_Ageing_Count",
    "Not_Serviceable_Count",
    "ORC_Validated_Count",
    "Delivery_Failed_Count",
    "Top_Courier_Return_Reason",
    "Top_Courier_Return_Sub_Reason",
    "Courier_Return_Risk_Level",
    "Suggested_Action",
    "Data_Gap_Reason",
    "Last_Updated",
]

PIVOT_HEADERS = [
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


def text_value(value: Any) -> str:
    return normalize_text(value)


def strip_common_prefix(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    cleaned = text.strip()
    while True:
        stripped = KNOWN_PREFIX_RE.sub("", cleaned).strip()
        if stripped == cleaned:
            break
        cleaned = stripped
    return cleaned.strip()


def normalize_identifier(value: Any) -> str:
    return strip_common_prefix(value)


def normalize_header_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_text(value).lower())


def short_sample_value(value: Any, limit: int = 40) -> str:
    text = normalize_text(value)
    if len(text) <= limit:
        return text
    return text[:limit]


def normalize_return_type_value(value: Any) -> str:
    text = normalize_text(value).lower().replace("-", " ").replace("_", " ")
    if not text:
        return ""
    if text in RETURN_TYPE_CUSTOMER_VALUES or "customer" in text:
        return "customer_return"
    if text in RETURN_TYPE_COURIER_VALUES or any(token in text for token in ("courier", "logistics", "rto", "cancel")):
        return "courier_return"
    return ""


def normalize_return_type(fields: Dict[str, str]) -> str:
    explicit = normalize_return_type_value(fields.get("Return_Type", ""))
    if explicit:
        return explicit
    haystack = " ".join(
        normalize_text(fields.get(field, ""))
        for field in ("Return_Reason", "Return_Sub_Reason", "Comments", "Return_Result", "Return_Status")
    ).lower()
    customer_hits = any(token in haystack for token in RETURN_TYPE_CUSTOMER_KEYWORDS)
    courier_hits = any(token in haystack for token in RETURN_TYPE_COURIER_KEYWORDS)
    if customer_hits and not courier_hits:
        return "customer_return"
    if courier_hits and not customer_hits:
        return "courier_return"
    return "unknown_return"


def classify_customer_issue(fields: Dict[str, str]) -> Tuple[str, str, str, str]:
    haystack = " ".join(fields.values()).lower()
    if any(token in haystack for token in ("defect", "defective", "faulty", "broken", "not working", "dead", "malfunction")):
        return "Defective Product", "Critical", "High", "Check QC / Supplier / Product Defect"
    if any(token in haystack for token in ("damag", "crack", "dent", "broken", "broken in transit")):
        return "Damaged Product", "Critical", "High", "Check Packaging / Courier Handling"
    if any(token in haystack for token in ("missing item", "missing accessory", "missing part", "incomplete", "accessory missing")):
        return "Missing Item / Accessory", "High", "High", "Check Packing / Item Completeness"
    if any(token in haystack for token in ("wrong product", "incorrect", "mismatch", "different product", "wrong item")):
        return "Wrong Product", "Critical", "High", "Check Picking / Packing"
    if any(token in haystack for token in ("quality", "poor quality", "bad quality", "low quality", "build quality", "material")):
        return "Quality Issue", "High", "High", "Improve Product Quality / Listing Claims"
    if any(token in haystack for token in ("as described", "expectation", "description", "photos", "image mismatch", "not as described")):
        return "Not As Described", "High", "High", "Improve Photos / Description / Specs"
    if any(token in haystack for token in ("remorse", "changed mind", "unwanted", "not required", "no longer needed", "mistake")):
        return "Customer Remorse", "Medium", "Medium", "Monitor Demand / Retention Signals"
    return "Other Customer Return", "Low", "Low", "Review Manually"


def classify_courier_issue(fields: Dict[str, str]) -> Tuple[str, str, str, str]:
    haystack = " ".join(fields.values()).lower()
    if any(token in haystack for token in ("cancel", "order cancelled", "customer cancelled")):
        return "Order Cancelled", "High", "High", "Check Cancellation / Order Journey"
    if any(token in haystack for token in ("rto", "return to origin", "courier return", "reverse logistics")):
        return "RTO / Courier Return", "High", "High", "Review Delivery / RTO Process"
    if any(token in haystack for token in ("attempts exhausted", "multiple attempts", "attempts")):
        return "Attempts Exhausted", "High", "High", "Check Delivery Attempts / Contactability"
    if any(token in haystack for token in ("shipment ageing", "shipment ageing", "ageing", "delayed")):
        return "Shipment Ageing", "Medium", "Medium", "Check Shipment Aging / TAT"
    if any(token in haystack for token in ("not serviceable", "non serviceable", "serviceability")):
        return "Not Serviceable", "High", "High", "Check Serviceability / Pincode Coverage"
    if any(token in haystack for token in ("orc", "validated with customer", "orc validated")):
        return "ORC Validated With Customer", "High", "High", "Review ORC / Confirmation Flow"
    if any(token in haystack for token in ("delivery failed", "undelivered", "failed delivery")):
        return "Delivery Failed", "High", "High", "Check Delivery Failure Reasons"
    return "Other Courier Return", "Low", "Low", "Review Manually"


def classify_return_bucket(return_type: str) -> str:
    normalized = normalize_text(return_type).lower()
    if normalized == "customer_return":
        return "customer_return"
    if normalized == "courier_return":
        return "courier_return"
    return "unknown_return"


def safe_float_text(value: Any, decimals: int = 2) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    number = parse_float(text)
    if decimals <= 0 or float(number).is_integer():
        return str(int(round(number)))
    return f"{number:.{decimals}f}".rstrip("0").rstrip(".")


def canonicalize_source_file(source_name: str) -> str:
    return normalize_text(source_name) or "Returns"


def get_value_by_header(row: Dict[str, Any], header_map: Dict[str, str], field_name: str) -> str:
    header = header_map.get(field_name, "")
    if header and header in row:
        return normalize_text(row.get(header, ""))
    alias_values = FIELD_ALIASES.get(field_name, [])
    for alias in alias_values:
        for key in row.keys():
            if normalize_header_key(key) == normalize_header_key(alias):
                return normalize_text(row.get(key, ""))
    return ""


def detect_header_map(headers: Sequence[str], rows: Sequence[Dict[str, Any]], source_file: str) -> Tuple[Dict[str, str], List[Dict[str, str]], Dict[str, List[str]]]:
    normalized_lookup = {normalize_header_key(header): header for header in headers}
    header_map: Dict[str, str] = {}
    debug_rows: List[Dict[str, str]] = []
    detected_columns: Dict[str, List[str]] = {"headers": [str(header) for header in headers]}

    for field_name, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            alias_key = normalize_header_key(alias)
            if alias_key in normalized_lookup:
                header_map[field_name] = normalized_lookup[alias_key]
                break

    for header in headers:
        mapped_field = ""
        mapped_confidence = "unmapped"
        header_key = normalize_header_key(header)
        for field_name, aliases in FIELD_ALIASES.items():
            alias_keys = {normalize_header_key(alias) for alias in aliases}
            if header_key in alias_keys:
                mapped_field = field_name
                mapped_confidence = "exact"
                break
        if not mapped_field and rows:
            sample_values = [normalize_text(row.get(header, "")) for row in rows if normalize_text(row.get(header, ""))]
            sample_value = sample_values[0] if sample_values else ""
            sample_normalized = sample_value.lower().replace("-", " ").replace("_", " ")
            if sample_value and any(token in sample_normalized for token in RETURN_TYPE_CUSTOMER_VALUES | RETURN_TYPE_COURIER_VALUES):
                mapped_field = "Return_Type"
                mapped_confidence = "inferred"
        else:
            sample_value = ""
        debug_rows.append(
            {
                "Source_File": source_file,
                "Original_Column": str(header),
                "Mapped_Field": mapped_field,
                "Mapping_Confidence": mapped_confidence,
                "Sample_Value": short_sample_value(sample_value),
            }
        )

    return header_map, debug_rows, detected_columns


def load_csv_source(path: Path, source_file: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]], Dict[str, List[str]]]:
    if not path.exists():
        return [], [], {"headers": []}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
        headers = list(reader.fieldnames or [])
    header_map, debug_rows, detected_columns = detect_header_map(headers, rows, source_file)
    mapped_rows: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        mapped_rows.append(normalize_return_source_row(row, header_map, source_file, source_file, index))
    return mapped_rows, debug_rows, detected_columns


def load_excel_source(path: Path, source_file: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]], Dict[str, List[str]]]:
    if not path.exists():
        return [], [], {"headers": []}
    try:
        excel = pd.ExcelFile(path)
    except Exception:
        return [], [], {"headers": []}
    sheet_name = "Returns" if "Returns" in excel.sheet_names else (excel.sheet_names[0] if excel.sheet_names else "")
    if not sheet_name:
        return [], [], {"headers": []}
    try:
        frame = pd.read_excel(path, sheet_name=sheet_name, dtype=str, keep_default_na=False).fillna("")
    except Exception:
        return [], [], {"headers": []}
    rows = frame.to_dict(orient="records")
    headers = [str(column) for column in frame.columns]
    header_map, debug_rows, detected_columns = detect_header_map(headers, rows, source_file)
    detected_columns["sheet_name"] = [sheet_name]
    mapped_rows: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        mapped_rows.append(normalize_return_source_row(row, header_map, source_file, sheet_name, index))
    return mapped_rows, debug_rows, detected_columns


def normalize_return_source_row(
    row: Dict[str, Any],
    header_map: Dict[str, str],
    source_file: str,
    source_sheet: str,
    index: int,
) -> Dict[str, str]:
    mapped: Dict[str, str] = {field: "" for field in FIELD_ALIASES}
    mapped["Source_File"] = canonicalize_source_file(source_file)
    mapped["Source_Sheet"] = normalize_text(source_sheet)
    mapped["Source_Row_Number"] = str(index + 2)

    for field_name in FIELD_ALIASES:
        mapped[field_name] = get_value_by_header(row, header_map, field_name)

    mapped["Return_ID"] = normalize_identifier(mapped.get("Return_ID", ""))
    mapped["Order_ID"] = normalize_identifier(mapped.get("Order_ID", ""))
    mapped["Order_Item_ID"] = normalize_identifier(mapped.get("Order_Item_ID", ""))
    mapped["SKU_ID"] = normalize_identifier(mapped.get("SKU_ID", ""))
    mapped["FSN"] = clean_fsn(mapped.get("FSN", ""))
    mapped["Product_Title"] = normalize_text(mapped.get("Product_Title", ""))
    mapped["Tracking_ID"] = normalize_identifier(mapped.get("Tracking_ID", ""))
    mapped["Reverse_Logistics_Tracking_ID"] = normalize_identifier(mapped.get("Reverse_Logistics_Tracking_ID", ""))
    mapped["Return_Requested_Date"] = normalize_text(mapped.get("Return_Requested_Date", ""))
    mapped["Return_Approval_Date"] = normalize_text(mapped.get("Return_Approval_Date", ""))
    mapped["Return_Completion_Date"] = normalize_text(mapped.get("Return_Completion_Date", ""))
    mapped["Quantity"] = normalize_text(mapped.get("Quantity", ""))
    mapped["Comments"] = normalize_text(mapped.get("Comments", ""))
    mapped["Return_Reason"] = normalize_text(mapped.get("Return_Reason", ""))
    mapped["Return_Sub_Reason"] = normalize_text(mapped.get("Return_Sub_Reason", ""))
    mapped["Return_Status"] = normalize_text(mapped.get("Return_Status", ""))
    mapped["Return_Result"] = normalize_text(mapped.get("Return_Result", ""))
    mapped["Return_Type"] = normalize_return_type_value(mapped.get("Return_Type", ""))
    if not mapped["Return_Type"]:
        mapped["Return_Type"] = normalize_return_type(mapped)
    if not mapped["Return_Completion_Date"]:
        mapped["Return_Completion_Date"] = normalize_text(row.get("Completed Date", "")) or normalize_text(row.get("return_completion_date", ""))
    return mapped


def load_raw_return_rows() -> Tuple[List[Dict[str, Any]], List[Dict[str, str]], Dict[str, Dict[str, List[str]]]]:
    rows: List[Dict[str, Any]] = []
    debug_rows: List[Dict[str, str]] = []
    detected_columns: Dict[str, Dict[str, List[str]]] = {}

    for source_name, loader, path in [
        ("Returns Report.csv", load_csv_source, RAW_CSV_PATH),
        ("Returns.xlsx", load_excel_source, RAW_XLSX_PATH),
    ]:
        source_rows, source_debug_rows, source_detected_columns = loader(path, source_name)
        rows.extend(source_rows)
        debug_rows.extend(source_debug_rows)
        detected_columns[source_name] = source_detected_columns
    return rows, debug_rows, detected_columns


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    return any(str(sheet.get("properties", {}).get("title", "")) == tab_name for sheet in metadata.get("sheets", []))


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A1:ZZ")
        .execute()
    )
    rows = response.get("values", [])
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def build_identity_lookup() -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    if not SKU_ANALYSIS_PATH.exists():
        return lookup
    try:
        frame = pd.read_csv(SKU_ANALYSIS_PATH, dtype=str, keep_default_na=False).fillna("")
    except Exception:
        return lookup
    for _, row in frame.iterrows():
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in lookup:
            continue
        lookup[fsn] = {
            "SKU_ID": normalize_identifier(row.get("SKU_ID", "")),
            "Product_Title": normalize_text(row.get("Product_Title", "")),
        }
    return lookup


def build_order_item_lookup() -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    if not NORMALIZED_ORDERS_PATH.exists():
        return lookup
    try:
        frame = pd.read_csv(NORMALIZED_ORDERS_PATH, dtype=str, keep_default_na=False).fillna("")
    except Exception:
        return lookup
    for _, row in frame.iterrows():
        fsn = clean_fsn(row.get("FSN", ""))
        order_item_id = normalize_identifier(row.get("Order_Item_ID", ""))
        if not fsn or not order_item_id:
            continue
        lookup.setdefault(order_item_id, fsn)
        lookup.setdefault(normalize_text(row.get("Order_Item_ID", "")), fsn)
    return lookup


def build_dedupe_key(row: Dict[str, str], index: int) -> Tuple[str, str]:
    return_id = normalize_identifier(row.get("Return_ID", ""))
    if return_id:
        return f"RETURN::{return_id}", ""

    order_item_id = normalize_identifier(row.get("Order_Item_ID", ""))
    return_type = normalize_return_type_value(row.get("Return_Type", ""))
    if not return_type:
        return_type = normalize_return_type(row)
    return_reason = normalize_identifier(row.get("Return_Reason", ""))
    if order_item_id and return_type and return_reason:
        return f"ORDER_ITEM::{order_item_id}|{return_type}|{return_reason}", ""

    order_id = normalize_identifier(row.get("Order_ID", ""))
    fsn = clean_fsn(row.get("FSN", ""))
    sku_id = normalize_identifier(row.get("SKU_ID", ""))
    return_requested_date = normalize_text(row.get("Return_Requested_Date", ""))
    if order_id and fsn and sku_id and return_reason and return_requested_date:
        return f"ORDER::{order_id}|{fsn}|{sku_id}|{return_reason}|{return_requested_date}", ""

    data_gap_parts = []
    if not order_id:
        data_gap_parts.append("Order_ID missing")
    if not order_item_id:
        data_gap_parts.append("Order_Item_ID missing")
    if not return_reason:
        data_gap_parts.append("Return_Reason missing")
    if not return_requested_date:
        data_gap_parts.append("Return_Requested_Date missing")
    return f"ROW::{index}", "; ".join(data_gap_parts) if data_gap_parts else "Missing stable dedupe key inputs"


def merge_value(record: Dict[str, Any], field: str, value: Any) -> None:
    text = normalize_text(value)
    if not text:
        return
    if not normalize_text(record.get(field, "")):
        record[field] = text


def append_source(record: Dict[str, Any], source_name: str) -> None:
    existing = normalize_text(record.get("Source_File", ""))
    values = [item for item in [existing, source_name] if item]
    record["Source_File"] = "; ".join(dict.fromkeys(values))


def build_detail_rows(raw_rows: Sequence[Dict[str, str]], identity_lookup: Dict[str, Dict[str, str]], order_item_lookup: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    run_id = f"FLIPKART_RETURN_INTELLIGENCE_V2_{now_iso().replace(':', '').replace('-', '').replace('T', '_')}"
    timestamp = now_iso()
    records: Dict[str, Dict[str, Any]] = {}
    source_counts = Counter()
    duplicate_rows_removed = 0

    for index, row in enumerate(raw_rows):
        if not any(normalize_text(value) for value in row.values()):
            continue
        source_name = normalize_text(row.get("Source_File", "")) or "Returns"
        source_counts[source_name] += 1
        key, gap_reason = build_dedupe_key(row, index)
        if key in records:
            duplicate_rows_removed += 1
        record = records.setdefault(key, {header: "" for header in DETAIL_HEADERS})
        append_source(record, source_name)
        record["Dedupe_Key"] = key
        record["Duplicate_Source_Count"] = str(int(parse_float(record.get("Duplicate_Source_Count", ""))) + 1 if normalize_text(record.get("Duplicate_Source_Count", "")) else 1)
        if gap_reason and not normalize_text(record.get("Data_Gap_Reason", "")):
            record["Data_Gap_Reason"] = gap_reason

        order_id = normalize_identifier(row.get("Order_ID", ""))
        order_item_id = normalize_identifier(row.get("Order_Item_ID", ""))
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn and order_item_id:
            fsn = clean_fsn(order_item_lookup.get(order_item_id, ""))
        identity = identity_lookup.get(fsn, {})
        merged_fields = {
            "Return_ID": normalize_identifier(row.get("Return_ID", "")),
            "Order_ID": order_id,
            "Order_Item_ID": order_item_id,
            "SKU_ID": normalize_identifier(row.get("SKU_ID", "")) or normalize_identifier(row.get("Seller_SKU", "")) or identity.get("SKU_ID", ""),
            "FSN": fsn,
            "Product_Title": normalize_text(row.get("Product_Title", "")) or identity.get("Product_Title", ""),
            "Return_Status": normalize_text(row.get("Return_Status", "")),
            "Return_Result": normalize_text(row.get("Return_Result", "")),
            "Return_Reason": normalize_identifier(row.get("Return_Reason", "")),
            "Return_Sub_Reason": normalize_identifier(row.get("Return_Sub_Reason", "")),
            "Comments": normalize_text(row.get("Comments", "")),
            "Tracking_ID": normalize_identifier(row.get("Tracking_ID", "")),
            "Reverse_Logistics_Tracking_ID": normalize_identifier(row.get("Reverse_Logistics_Tracking_ID", "")),
            "Return_Requested_Date": normalize_text(row.get("Return_Requested_Date", "")),
            "Return_Approval_Date": normalize_text(row.get("Return_Approval_Date", "")),
            "Return_Completion_Date": normalize_text(row.get("Return_Completion_Date", "")) or normalize_text(row.get("Completed_Date", "")),
            "Quantity": normalize_identifier(row.get("Quantity", "")),
        }
        normalized_type = normalize_return_type(merged_fields)
        merged_fields["Return_Type"] = normalized_type
        merged_fields["Return_Bucket"] = classify_return_bucket(normalized_type)
        if not normalize_text(record.get("Data_Gap_Reason", "")) and not normalize_text(merged_fields.get("Order_ID", "")) and not normalize_text(merged_fields.get("Order_Item_ID", "")):
            record["Data_Gap_Reason"] = "Order_ID and Order_Item_ID missing"

        for field, value in merged_fields.items():
            if field == "Return_Type":
                if not normalize_text(record.get(field, "")) or normalize_text(record.get(field, "")) == "unknown_return":
                    record[field] = value
                continue
            if field == "Return_Bucket":
                record[field] = value
                continue
            merge_value(record, field, value)

        if not normalize_text(record.get("Return_Type", "")):
            record["Return_Type"] = normalized_type
        if not normalize_text(record.get("Return_Bucket", "")):
            record["Return_Bucket"] = classify_return_bucket(record.get("Return_Type", ""))
        record["Run_ID"] = run_id
        record["Last_Updated"] = timestamp

    detail_rows = []
    for record in records.values():
        row = {header: normalize_text(record.get(header, "")) for header in DETAIL_HEADERS}
        detail_rows.append(row)

    detail_rows.sort(key=lambda row: (row.get("FSN", ""), row.get("Order_Item_ID", ""), row.get("Return_ID", "")))
    summary = {
        "run_id": run_id,
        "source_counts": dict(source_counts),
        "deduped_return_rows": len(detail_rows),
        "duplicate_return_rows_removed": duplicate_rows_removed,
        "raw_return_rows": len(raw_rows),
        "detail_rows": len(detail_rows),
    }
    return detail_rows, summary


def sold_order_item_counts() -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    if not NORMALIZED_ORDERS_PATH.exists():
        return counts
    try:
        frame = pd.read_csv(NORMALIZED_ORDERS_PATH, dtype=str, keep_default_na=False).fillna("")
    except Exception:
        return counts
    for _, row in frame.iterrows():
        fsn = clean_fsn(row.get("FSN", ""))
        order_item_id = normalize_identifier(row.get("Order_Item_ID", ""))
        if not fsn:
            continue
        key = f"{fsn}|{order_item_id}" if order_item_id else fsn
        counts[key] += 1
    fsn_counts: Dict[str, int] = defaultdict(int)
    for key, value in counts.items():
        fsn = key.split("|", 1)[0]
        fsn_counts[fsn] += 1
    return dict(fsn_counts)


def build_summary_index(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        index[fsn] = dict(row)
    return index


def top_value(rows: Sequence[Dict[str, Any]], field: str) -> str:
    counts = Counter(normalize_text(row.get(field, "")) for row in rows if normalize_text(row.get(field, "")))
    if not counts:
        return ""
    return counts.most_common(1)[0][0]


def risk_level(rate: float | None, count: int) -> str:
    if rate is None:
        return "Not Enough Data"
    if rate >= 0.50 or count >= 15:
        return "Critical"
    if rate >= 0.20 or count >= 8:
        return "High"
    if rate >= 0.10 or count >= 3:
        return "Medium"
    return "Low"


def customer_action_for_category(category: str, risk: str) -> str:
    mapping = {
        "Defective Product": "Fix QC / Supplier / Product Defect",
        "Damaged Product": "Check Packaging / Courier Handling",
        "Missing Item / Accessory": "Fix Packing Completeness",
        "Wrong Product": "Check Picking / Packing",
        "Quality Issue": "Improve Product Quality / Listing Claims",
        "Not As Described": "Improve Listing Accuracy",
        "Customer Remorse": "Monitor / Improve Positioning",
        "Other Customer Return": "Review Manually",
    }
    if risk == "Critical":
        return mapping.get(category, "Fix Product First")
    return mapping.get(category, "Review Manually")


def courier_action_for_category(category: str, risk: str) -> str:
    mapping = {
        "Order Cancelled": "Check Cancellation / Order Journey",
        "RTO / Courier Return": "Review Delivery / RTO Process",
        "Attempts Exhausted": "Check Delivery Attempts / Contactability",
        "Shipment Ageing": "Check Shipment Aging / TAT",
        "Not Serviceable": "Check Serviceability / Pincode Coverage",
        "ORC Validated With Customer": "Review ORC / Confirmation Flow",
        "Delivery Failed": "Check Delivery Failure Reasons",
        "Other Courier Return": "Review Manually",
    }
    if risk == "Critical":
        return mapping.get(category, "Check Logistics First")
    return mapping.get(category, "Review Manually")


def build_customer_summary_rows(detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        if normalize_text(row.get("Return_Bucket", "")) != "customer_return":
            continue
        grouped[clean_fsn(row.get("FSN", ""))].append(dict(row))

    sold_counts = sold_order_item_counts()
    identity_lookup = build_identity_lookup()
    rows: List[Dict[str, Any]] = []
    for fsn, items in grouped.items():
        identity = identity_lookup.get(fsn, {})
        sku_id = normalize_text(items[0].get("SKU_ID", "")) or identity.get("SKU_ID", "")
        title = normalize_text(items[0].get("Product_Title", "")) or identity.get("Product_Title", "")
        sold = sold_counts.get(fsn, 0)
        return_count = len(items)
        rate = (return_count / sold) if sold else None
        category_counts = Counter(normalize_text(item.get("Customer_Issue_Category", "")) for item in items)
        reason_counts = Counter(normalize_text(item.get("Return_Reason", "")) for item in items)
        sub_reason_counts = Counter(normalize_text(item.get("Return_Sub_Reason", "")) for item in items)

        quality_issue_count = sum(1 for item in items if normalize_text(item.get("Customer_Issue_Category", "")) in {"Defective Product", "Damaged Product", "Missing Item / Accessory", "Wrong Product", "Quality Issue", "Not As Described"})
        defective_count = category_counts.get("Defective Product", 0)
        damaged_count = category_counts.get("Damaged Product", 0)
        missing_count = category_counts.get("Missing Item / Accessory", 0)
        wrong_count = category_counts.get("Wrong Product", 0)
        remorse_count = category_counts.get("Customer Remorse", 0)
        top_reason = reason_counts.most_common(1)[0][0] if reason_counts else ""
        top_sub_reason = sub_reason_counts.most_common(1)[0][0] if sub_reason_counts else ""
        risk = risk_level(rate, return_count)
        if risk == "Critical" and defective_count == 0 and damaged_count == 0 and wrong_count == 0:
            suggested = "Fix Product First"
        else:
            suggested = customer_action_for_category(top_category(items), risk)
        data_gaps = []
        if not sold:
            data_gaps.append("Sold order items unavailable")
        rows.append(
            {
                "Run_ID": items[0].get("Run_ID", ""),
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": title,
                "Sold_Order_Items": str(sold) if sold else "",
                "Customer_Return_Count": str(return_count),
                "Customer_Return_Rate": f"{rate:.4f}" if rate is not None else "",
                "Quality_Issue_Count": str(quality_issue_count),
                "Defective_Product_Count": str(defective_count),
                "Damaged_Product_Count": str(damaged_count),
                "Missing_Item_Count": str(missing_count),
                "Wrong_Product_Count": str(wrong_count),
                "Customer_Remorse_Count": str(remorse_count),
                "Top_Customer_Return_Reason": top_reason,
                "Top_Customer_Return_Sub_Reason": top_sub_reason,
                "Customer_Return_Risk_Level": risk,
                "Suggested_Action": suggested,
                "Data_Gap_Reason": "; ".join(data_gaps),
                "Last_Updated": now_iso(),
            }
        )
    rows.sort(key=lambda row: (row.get("Customer_Return_Risk_Level", ""), row.get("Customer_Return_Count", ""), row.get("FSN", "")), reverse=True)
    return rows


def top_category(rows: Sequence[Dict[str, Any]]) -> str:
    counts = Counter(normalize_text(row.get("Customer_Issue_Category", "")) for row in rows if normalize_text(row.get("Customer_Issue_Category", "")))
    if not counts:
        return "Other Customer Return"
    return counts.most_common(1)[0][0]


def build_courier_summary_rows(detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        if normalize_text(row.get("Return_Bucket", "")) != "courier_return":
            continue
        grouped[clean_fsn(row.get("FSN", ""))].append(dict(row))

    sold_counts = sold_order_item_counts()
    identity_lookup = build_identity_lookup()
    rows: List[Dict[str, Any]] = []
    for fsn, items in grouped.items():
        identity = identity_lookup.get(fsn, {})
        sku_id = normalize_text(items[0].get("SKU_ID", "")) or identity.get("SKU_ID", "")
        title = normalize_text(items[0].get("Product_Title", "")) or identity.get("Product_Title", "")
        sold = sold_counts.get(fsn, 0)
        return_count = len(items)
        rate = (return_count / sold) if sold else None
        category_counts = Counter(normalize_text(item.get("Courier_Issue_Category", "")) for item in items)
        reason_counts = Counter(normalize_text(item.get("Return_Reason", "")) for item in items)
        sub_reason_counts = Counter(normalize_text(item.get("Return_Sub_Reason", "")) for item in items)
        order_cancelled = category_counts.get("Order Cancelled", 0)
        attempts_exhausted = category_counts.get("Attempts Exhausted", 0)
        shipment_ageing = category_counts.get("Shipment Ageing", 0)
        not_serviceable = category_counts.get("Not Serviceable", 0)
        orc_validated = category_counts.get("ORC Validated With Customer", 0)
        delivery_failed = category_counts.get("Delivery Failed", 0)
        top_reason = reason_counts.most_common(1)[0][0] if reason_counts else ""
        top_sub_reason = sub_reason_counts.most_common(1)[0][0] if sub_reason_counts else ""
        risk = risk_level(rate, return_count)
        suggested = courier_action_for_category(top_category_courier(items), risk)
        data_gaps = []
        if not sold:
            data_gaps.append("Sold order items unavailable")
        rows.append(
            {
                "Run_ID": items[0].get("Run_ID", ""),
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": title,
                "Sold_Order_Items": str(sold) if sold else "",
                "Courier_Return_Count": str(return_count),
                "Courier_Return_Rate": f"{rate:.4f}" if rate is not None else "",
                "Order_Cancelled_Count": str(order_cancelled),
                "Attempts_Exhausted_Count": str(attempts_exhausted),
                "Shipment_Ageing_Count": str(shipment_ageing),
                "Not_Serviceable_Count": str(not_serviceable),
                "ORC_Validated_Count": str(orc_validated),
                "Delivery_Failed_Count": str(delivery_failed),
                "Top_Courier_Return_Reason": top_reason,
                "Top_Courier_Return_Sub_Reason": top_sub_reason,
                "Courier_Return_Risk_Level": risk,
                "Suggested_Action": suggested,
                "Data_Gap_Reason": "; ".join(data_gaps),
                "Last_Updated": now_iso(),
            }
        )
    rows.sort(key=lambda row: (row.get("Courier_Return_Risk_Level", ""), row.get("Courier_Return_Count", ""), row.get("FSN", "")), reverse=True)
    return rows


def top_category_courier(rows: Sequence[Dict[str, Any]]) -> str:
    counts = Counter(normalize_text(row.get("Courier_Issue_Category", "")) for row in rows if normalize_text(row.get("Courier_Issue_Category", "")))
    if not counts:
        return "Other Courier Return"
    return counts.most_common(1)[0][0]


def build_return_type_pivot_rows(detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sold_counts = sold_order_item_counts()
    identity_lookup = build_identity_lookup()
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn:
            grouped[fsn].append(dict(row))

    rows: List[Dict[str, Any]] = []
    for fsn, items in grouped.items():
        identity = identity_lookup.get(fsn, {})
        sku_id = normalize_text(items[0].get("SKU_ID", "")) or identity.get("SKU_ID", "")
        title = normalize_text(items[0].get("Product_Title", "")) or identity.get("Product_Title", "")
        sold = sold_counts.get(fsn, 0)
        customer_count = sum(1 for item in items if normalize_text(item.get("Return_Bucket", "")) == "customer_return")
        courier_count = sum(1 for item in items if normalize_text(item.get("Return_Bucket", "")) == "courier_return")
        unknown_count = sum(1 for item in items if normalize_text(item.get("Return_Bucket", "")) == "unknown_return")
        total_count = customer_count + courier_count + unknown_count
        customer_rate = (customer_count / sold) if sold else None
        courier_rate = (courier_count / sold) if sold else None
        total_rate = (total_count / sold) if sold else None
        if customer_count > courier_count and customer_count >= unknown_count:
            dominant = "customer_return"
        elif courier_count > customer_count and courier_count >= unknown_count:
            dominant = "courier_return"
        elif unknown_count > 0:
            dominant = "unknown_return"
        else:
            dominant = "mixed"
        if customer_count == 0 and courier_count == 0:
            mix = "Unknown Only"
        elif customer_count and not courier_count:
            mix = "Customer Heavy"
        elif courier_count and not customer_count:
            mix = "Courier Heavy"
        else:
            mix = "Mixed"
        rows.append(
            {
                "Run_ID": items[0].get("Run_ID", ""),
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": title,
                "Sold_Order_Items": str(sold) if sold else "",
                "Customer_Return_Count": str(customer_count),
                "Courier_Return_Count": str(courier_count),
                "Unknown_Return_Count": str(unknown_count),
                "Total_Return_Count": str(total_count),
                "Customer_Return_Rate": f"{customer_rate:.4f}" if customer_rate is not None else "",
                "Courier_Return_Rate": f"{courier_rate:.4f}" if courier_rate is not None else "",
                "Total_Return_Rate": f"{total_rate:.4f}" if total_rate is not None else "",
                "Customer_vs_Courier_Mix": mix,
                "Dominant_Return_Type": dominant,
                "Last_Updated": now_iso(),
            }
        )
    rows.sort(key=lambda row: (row.get("Total_Return_Count", ""), row.get("FSN", "")), reverse=True)
    return rows


def split_rows(rows: Sequence[Dict[str, Any]], bucket: str) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows if normalize_text(row.get("Return_Bucket", "")) == bucket]


def build_local_outputs(detail_rows: Sequence[Dict[str, Any]], customer_rows: Sequence[Dict[str, Any]], courier_rows: Sequence[Dict[str, Any]], customer_summary_rows: Sequence[Dict[str, Any]], courier_summary_rows: Sequence[Dict[str, Any]], pivot_rows: Sequence[Dict[str, Any]]) -> None:
    write_csv(LOCAL_DETAIL_PATH, DETAIL_HEADERS, detail_rows)
    write_csv(LOCAL_CUSTOMER_PATH, CUSTOMER_HEADERS, customer_rows)
    write_csv(LOCAL_COURIER_PATH, COURIER_HEADERS, courier_rows)
    write_csv(LOCAL_CUSTOMER_SUMMARY_PATH, CUSTOMER_SUMMARY_HEADERS, customer_summary_rows)
    write_csv(LOCAL_COURIER_SUMMARY_PATH, COURIER_SUMMARY_HEADERS, courier_summary_rows)
    write_csv(LOCAL_PIVOT_PATH, PIVOT_HEADERS, pivot_rows)
    write_csv(LOOKER_DETAIL_PATH, DETAIL_HEADERS, detail_rows)
    write_csv(LOOKER_CUSTOMER_PATH, CUSTOMER_HEADERS, customer_rows)
    write_csv(LOOKER_COURIER_PATH, COURIER_HEADERS, courier_rows)
    write_csv(LOOKER_PIVOT_PATH, PIVOT_HEADERS, pivot_rows)


def write_sheet_tab(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def create_flipkart_return_intelligence_v2() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    raw_rows, debug_rows, detected_columns = load_raw_return_rows()
    identity_lookup = build_identity_lookup()
    order_item_lookup = build_order_item_lookup()
    detail_rows, summary = build_detail_rows(raw_rows, identity_lookup, order_item_lookup)

    customer_rows = split_rows(detail_rows, "customer_return")
    courier_rows = split_rows(detail_rows, "courier_return")
    unknown_rows = split_rows(detail_rows, "unknown_return")

    customer_summary_rows = build_customer_summary_rows(customer_rows)
    courier_summary_rows = build_courier_summary_rows(courier_rows)
    pivot_rows = build_return_type_pivot_rows(detail_rows)

    build_local_outputs(detail_rows, customer_rows, courier_rows, customer_summary_rows, courier_summary_rows, pivot_rows)
    write_csv(RETURN_DEBUG_PATH, SOURCE_ROWS_DEBUG_HEADERS, debug_rows)

    for tab_name, headers, rows in [
        (DETAIL_TAB, DETAIL_HEADERS, detail_rows),
        (CUSTOMER_COMMENTS_TAB, CUSTOMER_HEADERS, customer_rows),
        (COURIER_COMMENTS_TAB, COURIER_HEADERS, courier_rows),
        (CUSTOMER_SUMMARY_TAB, CUSTOMER_SUMMARY_HEADERS, customer_summary_rows),
        (COURIER_SUMMARY_TAB, COURIER_SUMMARY_HEADERS, courier_summary_rows),
        (RETURN_TYPE_PIVOT_TAB, PIVOT_HEADERS, pivot_rows),
    ]:
        write_sheet_tab(sheets_service, spreadsheet_id, tab_name, headers, rows)

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "run_id": summary["run_id"],
        "raw_return_rows": summary["raw_return_rows"],
        "deduped_return_rows": summary["deduped_return_rows"],
        "duplicate_return_rows_removed": summary["duplicate_return_rows_removed"],
        "detail_rows": len(detail_rows),
        "customer_return_rows": len(customer_rows),
        "courier_return_rows": len(courier_rows),
        "unknown_return_rows": len(unknown_rows),
        "customer_summary_rows": len(customer_summary_rows),
        "courier_summary_rows": len(courier_summary_rows),
        "return_type_pivot_rows": len(pivot_rows),
        "status": "SUCCESS",
        "message": "Flipkart return intelligence v2 created",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "run_id",
            "raw_return_rows",
            "deduped_return_rows",
            "duplicate_return_rows_removed",
            "detail_rows",
            "customer_return_rows",
            "courier_return_rows",
            "unknown_return_rows",
            "customer_summary_rows",
            "courier_summary_rows",
            "return_type_pivot_rows",
            "status",
            "message",
        ],
        [log_row],
    )

    result = {
        "status": "SUCCESS",
        "spreadsheet_id": spreadsheet_id,
        "run_id": summary["run_id"],
        "raw_return_rows": summary["raw_return_rows"],
        "deduped_return_rows": summary["deduped_return_rows"],
        "duplicate_return_rows_removed": summary["duplicate_return_rows_removed"],
        "detail_rows": len(detail_rows),
        "customer_return_rows": len(customer_rows),
        "courier_return_rows": len(courier_rows),
        "unknown_return_rows": len(unknown_rows),
        "customer_summary_rows": len(customer_summary_rows),
        "courier_summary_rows": len(courier_summary_rows),
        "return_type_pivot_rows": len(pivot_rows),
        "source_counts": summary["source_counts"],
        "detected_columns": detected_columns,
        "debug_csv_path": str(RETURN_DEBUG_PATH),
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_return_intelligence_v2()
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
