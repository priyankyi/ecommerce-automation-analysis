from __future__ import annotations

import argparse
import hashlib
import csv
import json
import sys
from collections import Counter, OrderedDict, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd
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
    NORMALIZED_ORDERS_PATH,
    NORMALIZED_PNL_PATH,
    NORMALIZED_RETURNS_PATH,
    NORMALIZED_SETTLEMENTS_PATH,
    SKU_ANALYSIS_PATH,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    now_iso,
    parse_float,
    write_csv,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_order_item_explorer_log.csv"
LOCAL_LEGACY_ORDER_ITEM_PATH = OUTPUT_DIR / "flipkart_order_item_explorer.csv"
LOCAL_LEGACY_LOOKER_PATH = OUTPUT_DIR / "looker_flipkart_order_item_explorer.csv"
LOCAL_MASTER_PATH = OUTPUT_DIR / "flipkart_order_item_master.csv"
LOCAL_SOURCE_DETAIL_PATH = OUTPUT_DIR / "flipkart_order_item_source_detail.csv"
LOCAL_LOOKER_MASTER_PATH = OUTPUT_DIR / "looker_flipkart_order_item_master.csv"
LOCAL_LOOKER_SOURCE_DETAIL_PATH = OUTPUT_DIR / "looker_flipkart_order_item_source_detail.csv"
ORDER_ITEM_TAB = "FLIPKART_ORDER_ITEM_EXPLORER"
LOOKER_ORDER_ITEM_TAB = "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER"
ORDER_ITEM_MASTER_TAB = "FLIPKART_ORDER_ITEM_MASTER"
ORDER_ITEM_SOURCE_DETAIL_TAB = "FLIPKART_ORDER_ITEM_SOURCE_DETAIL"
LOOKER_ORDER_ITEM_MASTER_TAB = "LOOKER_FLIPKART_ORDER_ITEM_MASTER"
LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB = "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL"
RETURN_TYPE_PIVOT_TAB = "FLIPKART_RETURN_TYPE_PIVOT"
ORDER_ITEM_REFRESH_MANIFEST_PATH = OUTPUT_DIR / "order_item_refresh_manifest.json"
ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH = OUTPUT_DIR / "order_item_looker_refresh_manifest.json"
INTERNAL_MODE_VALUES = {"master-only", "light", "full"}
LOOKER_MODE_VALUES = {"none", "master-only", "light", "full"}
INTERNAL_LIGHT_EXPLORER_ROW_THRESHOLD = 8000
LOOKER_LIGHT_EXPLORER_ROW_THRESHOLD = 8000

LOCAL_CSV_SOURCES: Dict[str, Path] = {
    "orders": NORMALIZED_ORDERS_PATH,
    "returns": NORMALIZED_RETURNS_PATH,
    "settlements": NORMALIZED_SETTLEMENTS_PATH,
    "pnl": NORMALIZED_PNL_PATH,
    "sku_analysis": SKU_ANALYSIS_PATH,
    "adjusted_profit": OUTPUT_DIR / "flipkart_adjusted_profit.csv",
    "return_comments": OUTPUT_DIR / "flipkart_return_comments.csv",
    "return_all_details": OUTPUT_DIR / "flipkart_return_all_details.csv",
    "customer_return_comments": OUTPUT_DIR / "flipkart_customer_return_comments.csv",
    "courier_return_comments": OUTPUT_DIR / "flipkart_courier_return_comments.csv",
    "return_type_pivot": OUTPUT_DIR / "flipkart_return_type_pivot.csv",
    "ads_recommendations": OUTPUT_DIR / "flipkart_ads_final_recommendations.csv",
    "ads_planner": OUTPUT_DIR / "flipkart_ads_planner.csv",
    "competitor_price": OUTPUT_DIR / "flipkart_competitor_price_intelligence.csv",
}

SHEET_FALLBACK_TABS = {
    "alerts": "FLIPKART_ALERTS_GENERATED",
    "sku_analysis": "FLIPKART_SKU_ANALYSIS",
    "adjusted_profit": "FLIPKART_ADJUSTED_PROFIT",
    "return_comments": "FLIPKART_RETURN_COMMENTS",
    "return_all_details": "FLIPKART_RETURN_ALL_DETAILS",
    "customer_return_comments": "FLIPKART_CUSTOMER_RETURN_COMMENTS",
    "courier_return_comments": "FLIPKART_COURIER_RETURN_COMMENTS",
    "return_type_pivot": RETURN_TYPE_PIVOT_TAB,
    "ads_planner": "FLIPKART_ADS_PLANNER",
}

OUTPUT_HEADERS = [
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
    "Return_Type",
    "Return_Reason",
    "Return_Sub_Reason",
    "Return_Issue_Category",
    "Customer_Return_YN",
    "Courier_Return_YN",
    "Customer_Issue_Category",
    "Courier_Issue_Category",
    "Customer_Return_Risk_Level",
    "Courier_Return_Risk_Level",
    "Alert_Count",
    "Critical_Alert_Count",
    "Final_Ads_Decision",
    "Competition_Risk_Level",
    "Data_Gap_Reason",
    "Source_File",
    "Last_Updated",
]

ORDER_ITEM_SOURCE_DETAIL_HEADERS = [
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
]

ORDER_ITEM_MASTER_HEADERS = [
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
]

SOURCE_PRIORITY = {
    "Orders.xlsx": 1,
    "Returns Report.csv": 2,
    "Returns.xlsx": 3,
    "Settled Transactions.xlsx": 4,
    "PNL.xlsx": 5,
    "flipkart_return_all_details.csv": 6,
    "flipkart_customer_return_comments.csv": 6,
    "flipkart_courier_return_comments.csv": 6,
    "flipkart_customer_return_issue_summary.csv": 6,
    "flipkart_courier_return_summary.csv": 6,
    "flipkart_return_type_pivot.csv": 6,
    "flipkart_return_comments.csv": 6,
    "flipkart_ads_final_recommendations.csv": 6,
    "flipkart_ads_planner.csv": 6,
    "flipkart_competitor_price_intelligence.csv": 6,
    "alert": 7,
    "unknown": 9,
}


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


def load_csv_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def load_sheet_table_if_available(
    sheets_service: object,
    spreadsheet_id: str,
    tab_name: str,
) -> pd.DataFrame:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return pd.DataFrame()
    headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
    if not headers:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=headers).fillna("")


def _load_order_item_looker_manifest() -> Dict[str, Any]:
    if not ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH.exists():
        return {}
    try:
        return json.loads(ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_order_item_looker_manifest(payload: Dict[str, Any]) -> None:
    ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_order_item_refresh_manifest() -> Dict[str, Any]:
    if not ORDER_ITEM_REFRESH_MANIFEST_PATH.exists():
        return {}
    try:
        return json.loads(ORDER_ITEM_REFRESH_MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_order_item_refresh_manifest(payload: Dict[str, Any]) -> None:
    ORDER_ITEM_REFRESH_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    ORDER_ITEM_REFRESH_MANIFEST_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _content_hash(headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> str:
    normalized_rows = [[text_value(row.get(header, "")) for header in headers] for row in rows]
    payload = {"headers": list(headers), "rows": normalized_rows}
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()


def _current_sheet_content_hash(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
) -> str:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return ""
    headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
    if not headers:
        return ""
    return _content_hash(headers, rows)


def _normalize_internal_mode(internal_mode: str | None) -> str:
    mode = text_value(internal_mode).lower() or "master-only"
    if mode not in INTERNAL_MODE_VALUES:
        raise ValueError(f"Unsupported order-item internal mode: {internal_mode}")
    return mode


def load_latest_run_id(frames: Sequence[pd.DataFrame], candidates: Sequence[str]) -> str:
    for df in frames:
        if df.empty or "Run_ID" not in df.columns:
            continue
        for value in reversed(df["Run_ID"].fillna("").astype(str).tolist()):
            text = value.strip()
            if text:
                return text
    return f"FLIPKART_ORDER_ITEM_EXPLORER_{now_iso().replace(':', '').replace('-', '').replace('T', '_')}"


def text_value(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def normalize_text(value: Any) -> str:
    return text_value(value)


def format_number_text(value: Any, decimals: int = 2) -> str:
    text = text_value(value)
    if not text:
        return ""
    number = parse_float(text)
    if float(number).is_integer() or decimals <= 0:
        return str(int(round(number)))
    return f"{number:.{decimals}f}".rstrip("0").rstrip(".")


def append_source(existing: str, new_value: str) -> str:
    items = [item for item in (text_value(existing), text_value(new_value)) if item]
    if not items:
        return ""
    return "; ".join(dict.fromkeys(items))


def set_first(record: Dict[str, Any], field: str, value: Any) -> None:
    text = text_value(value)
    if text and not text_value(record.get(field, "")):
        record[field] = text


def add_gap(record: Dict[str, Any], reason: str) -> None:
    gaps = record.setdefault("__data_gaps", [])
    if reason and reason not in gaps:
        gaps.append(reason)


def add_source(record: Dict[str, Any], source_text: str) -> None:
    record["Source_File"] = append_source(record.get("Source_File", ""), source_text)


def row_key(row: Dict[str, Any], source_name: str, index: int) -> str:
    return_id = text_value(row.get("Return_ID", ""))
    order_item_id = text_value(row.get("Order_Item_ID", ""))
    order_id = text_value(row.get("Order_ID", ""))
    fsn = clean_fsn(row.get("FSN", ""))
    sku = text_value(row.get("SKU_ID", "")) or text_value(row.get("Seller_SKU", ""))
    if return_id:
        return f"RETURN::{return_id}"
    if order_item_id:
        return f"ORDER_ITEM::{order_item_id}"
    composite = [part for part in [order_id, fsn, sku] if part]
    if composite:
        return "ORDER_FALLBACK::" + "|".join(composite)
    return f"ROW::{source_name}::{index}"


def blank_record(run_id: str) -> Dict[str, Any]:
    record = {header: "" for header in OUTPUT_HEADERS}
    record["Run_ID"] = run_id
    record["__data_gaps"] = []
    return record


def merge_order_row(record: Dict[str, Any], row: Dict[str, Any], source_name: str) -> None:
    source_file = text_value(row.get("Source_File", "")) or source_name
    add_source(record, source_file)
    set_first(record, "FSN", clean_fsn(row.get("FSN", "")))
    set_first(record, "Order_ID", row.get("Order_ID", ""))
    set_first(record, "Order_Item_ID", row.get("Order_Item_ID", ""))
    set_first(record, "SKU_ID", row.get("Seller_SKU", ""))
    set_first(record, "Product_Title", row.get("Product_Title", ""))
    set_first(record, "Order_Date", row.get("Order_Date", ""))
    set_first(record, "Dispatch_Date", row.get("Dispatch_Date", ""))
    set_first(record, "Delivery_Date", row.get("Delivery_Date", ""))
    set_first(record, "Quantity", text_value(row.get("Quantity", "")))
    set_first(record, "Selling_Price", text_value(row.get("Selling_Price", "")))


def merge_return_row(record: Dict[str, Any], row: Dict[str, Any], source_name: str) -> None:
    source_file = text_value(row.get("Source_File", "")) or source_name
    add_source(record, source_file)
    set_first(record, "FSN", clean_fsn(row.get("FSN", "")))
    set_first(record, "Order_ID", row.get("Order_ID", ""))
    set_first(record, "Order_Item_ID", row.get("Order_Item_ID", ""))
    set_first(record, "SKU_ID", row.get("SKU_ID", ""))
    set_first(record, "Product_Title", row.get("Product_Title", ""))
    set_first(record, "Return_Status", row.get("Return_Status", ""))
    set_first(record, "Return_ID", row.get("Return_ID", ""))
    set_first(record, "Return_Date", row.get("Return_Date", ""))
    set_first(record, "Return_Reason", row.get("Return_Reason", ""))
    set_first(record, "Return_Sub_Reason", row.get("Return_Sub_Reason", ""))
    set_first(record, "Return_Issue_Category", row.get("Issue_Category", ""))
    return_type = infer_return_type(row)
    set_first(record, "Return_Type", return_type)
    set_first(record, "Customer_Return_YN", "Yes" if return_type == "customer_return" else "No")
    set_first(record, "Courier_Return_YN", "Yes" if return_type == "courier_return" else "No")
    customer_category = normalize_text(row.get("Customer_Issue_Category", "")) or infer_customer_issue_category(row)
    courier_category = normalize_text(row.get("Courier_Issue_Category", "")) or infer_courier_issue_category(row)
    if return_type != "customer_return":
        customer_category = normalize_text(row.get("Customer_Issue_Category", ""))
    if return_type != "courier_return":
        courier_category = normalize_text(row.get("Courier_Issue_Category", ""))
    set_first(record, "Customer_Issue_Category", customer_category)
    set_first(record, "Courier_Issue_Category", courier_category)
    set_first(record, "Customer_Return_Risk_Level", normalize_text(row.get("Customer_Return_Risk_Level", "")) or infer_customer_risk(customer_category))
    set_first(record, "Courier_Return_Risk_Level", normalize_text(row.get("Courier_Return_Risk_Level", "")) or infer_courier_risk(courier_category))


def merge_return_type_pivot_row(record: Dict[str, Any], row: Dict[str, Any], source_name: str) -> None:
    source_file = text_value(row.get("Source_File", "")) or source_name
    add_source(record, source_file)
    set_first(record, "FSN", clean_fsn(row.get("FSN", "")))
    set_first(record, "SKU_ID", row.get("SKU_ID", ""))
    set_first(record, "Product_Title", row.get("Product_Title", ""))
    customer_count = text_value(row.get("Customer_Return_Count", ""))
    courier_count = text_value(row.get("Courier_Return_Count", ""))
    unknown_count = text_value(row.get("Unknown_Return_Count", ""))
    dominant_type = normalize_text(row.get("Dominant_Return_Type", ""))
    total_count = text_value(row.get("Total_Return_Count", ""))
    if dominant_type:
        set_first(record, "Return_Type", dominant_type)
    if customer_count and customer_count != "0":
        set_first(record, "Customer_Return_YN", "Yes")
    if courier_count and courier_count != "0":
        set_first(record, "Courier_Return_YN", "Yes")
    if dominant_type == "customer_return":
        set_first(record, "Customer_Return_YN", "Yes")
    elif dominant_type == "courier_return":
        set_first(record, "Courier_Return_YN", "Yes")
    if total_count and total_count != "0":
        set_first(record, "Return_Issue_Category", f"Pivot total {total_count}")
    if customer_count and not text_value(record.get("Customer_Issue_Category", "")):
        set_first(record, "Customer_Issue_Category", "Customer Returns Present")
    if courier_count and not text_value(record.get("Courier_Issue_Category", "")):
        set_first(record, "Courier_Issue_Category", "Courier Returns Present")
    if unknown_count and not text_value(record.get("Return_Status", "")):
        set_first(record, "Return_Status", "Unknown Return Types Present")


def merge_settlement_row(record: Dict[str, Any], row: Dict[str, Any], source_name: str) -> None:
    source_file = text_value(row.get("Source_File", "")) or source_name
    add_source(record, source_file)
    set_first(record, "FSN", clean_fsn(row.get("FSN", "")))
    set_first(record, "Order_ID", row.get("Order_ID", ""))
    set_first(record, "Order_Item_ID", row.get("Order_Item_ID", ""))
    set_first(record, "SKU_ID", row.get("Seller_SKU", ""))
    set_first(record, "Settlement_Amount", row.get("Net_Settlement", ""))
    set_first(record, "Commission", row.get("Commission", ""))
    set_first(record, "Shipping_Fee", row.get("Shipping_Fee", ""))
    other_fee_total = 0.0
    for field in [
        "Fixed_Fee",
        "Collection_Fee",
        "Reverse_Shipping_Fee",
        "GST_On_Fees",
        "TCS",
        "TDS",
        "Refund",
        "Protection_Fund",
        "Adjustments",
    ]:
        other_fee_total += parse_float(row.get(field, ""))
    if not text_value(record.get("Other_Fees", "")) and other_fee_total:
        record["Other_Fees"] = format_number_text(other_fee_total)
    if not text_value(record.get("Total_Deductions", "")):
        deductions = parse_float(record.get("Commission", "")) + parse_float(record.get("Shipping_Fee", "")) + parse_float(record.get("Other_Fees", ""))
        if deductions:
            record["Total_Deductions"] = format_number_text(deductions)


def merge_pnl_row(record: Dict[str, Any], row: Dict[str, Any], source_name: str) -> None:
    source_file = text_value(row.get("Source_File", "")) or source_name
    add_source(record, source_file)
    set_first(record, "FSN", clean_fsn(row.get("FSN", "")))
    set_first(record, "Order_ID", row.get("Order_ID", ""))
    set_first(record, "Order_Item_ID", row.get("Order_Item_ID", ""))
    set_first(record, "SKU_ID", row.get("SKU_ID", ""))
    if not text_value(record.get("Settlement_Amount", "")):
        set_first(record, "Settlement_Amount", row.get("Amount_Settled", ""))
    set_first(record, "Net_Profit", row.get("Flipkart_Net_Earnings", ""))
    if not text_value(record.get("Net_Profit", "")):
        set_first(record, "Net_Profit", row.get("Amount_Pending", ""))


def infer_return_type(row: Dict[str, Any]) -> str:
    return_type = normalize_text(row.get("Return_Type", "")).lower()
    if "customer" in return_type:
        return "customer_return"
    if any(token in return_type for token in ("courier", "logistics", "rto", "cancel")):
        return "courier_return"
    haystack = " ".join(
        normalize_text(row.get(field, "")).lower()
        for field in ["Return_Reason", "Return_Sub_Reason", "Comments", "Issue_Category", "Return_Status", "Return_Result"]
    )
    if any(token in haystack for token in ("defect", "damag", "wrong", "quality", "as described", "remorse", "missing item", "missing accessory", "faulty", "broken", "not working")):
        return "customer_return"
    if any(token in haystack for token in ("courier", "logistics", "shipment ageing", "attempts exhausted", "not serviceable", "orc", "delivery failed", "rto", "cancel")):
        return "courier_return"
    return "unknown_return"


def infer_customer_issue_category(row: Dict[str, Any]) -> str:
    haystack = " ".join(
        normalize_text(row.get(field, "")).lower()
        for field in ["Return_Reason", "Return_Sub_Reason", "Comments", "Issue_Category", "Return_Status", "Return_Result"]
    )
    if any(token in haystack for token in ("defect", "defective", "faulty", "broken", "not working", "dead", "malfunction")):
        return "Defective Product"
    if any(token in haystack for token in ("damag", "crack", "dent", "broken in transit")):
        return "Damaged Product"
    if any(token in haystack for token in ("missing item", "missing accessory", "missing part", "incomplete")):
        return "Missing Item / Accessory"
    if any(token in haystack for token in ("wrong product", "incorrect", "mismatch", "different product", "wrong item")):
        return "Wrong Product"
    if any(token in haystack for token in ("quality", "poor quality", "bad quality", "low quality", "build quality")):
        return "Quality Issue"
    if any(token in haystack for token in ("as described", "description", "photos", "expectation", "not as described")):
        return "Not As Described"
    if any(token in haystack for token in ("remorse", "changed mind", "unwanted", "not required", "mistake")):
        return "Customer Remorse"
    return "Other Customer Return"


def infer_courier_issue_category(row: Dict[str, Any]) -> str:
    haystack = " ".join(
        normalize_text(row.get(field, "")).lower()
        for field in ["Return_Reason", "Return_Sub_Reason", "Comments", "Issue_Category", "Return_Status", "Return_Result"]
    )
    if any(token in haystack for token in ("cancel", "order cancelled", "customer cancelled")):
        return "Order Cancelled"
    if any(token in haystack for token in ("rto", "return to origin", "courier return", "reverse logistics")):
        return "RTO / Courier Return"
    if any(token in haystack for token in ("attempts exhausted", "multiple attempts", "attempts")):
        return "Attempts Exhausted"
    if any(token in haystack for token in ("shipment ageing", "ageing", "delayed")):
        return "Shipment Ageing"
    if any(token in haystack for token in ("not serviceable", "non serviceable", "serviceability")):
        return "Not Serviceable"
    if any(token in haystack for token in ("orc", "validated with customer", "orc validated")):
        return "ORC Validated With Customer"
    if any(token in haystack for token in ("delivery failed", "undelivered", "failed delivery")):
        return "Delivery Failed"
    return "Other Courier Return"


def infer_customer_risk(category: str) -> str:
    if category in {"Defective Product", "Damaged Product", "Wrong Product"}:
        return "Critical"
    if category in {"Missing Item / Accessory", "Quality Issue", "Not As Described"}:
        return "High"
    if category == "Customer Remorse":
        return "Medium"
    return "Low"


def infer_courier_risk(category: str) -> str:
    if category in {"Order Cancelled", "RTO / Courier Return", "Attempts Exhausted", "Not Serviceable", "ORC Validated With Customer", "Delivery Failed"}:
        return "High"
    if category == "Shipment Ageing":
        return "Medium"
    return "Low"


def merge_alert_counts(record: Dict[str, Any], alert_counts: Dict[str, Dict[str, int]]) -> None:
    fsn = clean_fsn(record.get("FSN", ""))
    if not fsn or fsn not in alert_counts:
        return
    counts = alert_counts[fsn]
    if not text_value(record.get("Alert_Count", "")):
        record["Alert_Count"] = str(counts.get("Alert_Count", 0))
    if not text_value(record.get("Critical_Alert_Count", "")):
        record["Critical_Alert_Count"] = str(counts.get("Critical_Alert_Count", 0))


def merge_fsn_summary(record: Dict[str, Any], row: Dict[str, Any], source_name: str) -> None:
    source_file = text_value(row.get("Source_File", "")) or source_name
    add_source(record, source_file)
    set_first(record, "FSN", clean_fsn(row.get("FSN", "")))
    set_first(record, "SKU_ID", row.get("SKU_ID", ""))
    set_first(record, "Product_Title", row.get("Product_Title", ""))
    set_first(record, "Cost_Price", row.get("Cost_Price", ""))
    if not text_value(record.get("COGS", "")):
        set_first(record, "COGS", row.get("Total_COGS", ""))
    if not text_value(record.get("Net_Profit", "")):
        set_first(record, "Net_Profit", row.get("Adjusted_Final_Net_Profit", ""))
    if not text_value(record.get("Net_Profit", "")):
        set_first(record, "Net_Profit", row.get("Final_Net_Profit", ""))
    if not text_value(record.get("Profit_Margin", "")):
        set_first(record, "Profit_Margin", row.get("Final_Profit_Margin", ""))
    if not text_value(record.get("Return_Status", "")) and text_value(row.get("Return_Status", "")):
        set_first(record, "Return_Status", row.get("Return_Status", ""))
    set_first(record, "Final_Ads_Decision", row.get("Final_Ads_Decision", ""))
    set_first(record, "Competition_Risk_Level", row.get("Competition_Risk_Level", ""))


def build_alert_counts(alert_rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"Alert_Count": 0, "Critical_Alert_Count": 0})
    for row in alert_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        counts[fsn]["Alert_Count"] += 1
        if text_value(row.get("Severity", "")).lower() == "critical":
            counts[fsn]["Critical_Alert_Count"] += 1
    return counts


def source_rank(source_file: str, row_type: str) -> int:
    if row_type == "alert":
        return SOURCE_PRIORITY["alert"]
    return SOURCE_PRIORITY.get(source_file, SOURCE_PRIORITY["unknown"])


def pick_text(row: Dict[str, Any], *fields: str) -> str:
    for field in fields:
        value = text_value(row.get(field, ""))
        if value:
            return value
    return ""


def order_item_master_key(row: Dict[str, Any]) -> str:
    order_item_id = text_value(row.get("Order_Item_ID", ""))
    if order_item_id:
        return order_item_id
    order_id = text_value(row.get("Order_ID", ""))
    fsn = text_value(row.get("FSN", ""))
    sku_id = text_value(row.get("SKU_ID", ""))
    if order_id:
        return f"ORDER_ONLY::{order_id}::{fsn}::{sku_id}"
    return ""


def build_source_detail_row(
    row: Dict[str, Any],
    *,
    run_id: str,
    source_file: str,
    source_tab: str,
    row_type: str,
    alert_counts: Dict[str, Dict[str, int]],
) -> Dict[str, Any]:
    source_row: Dict[str, Any] = {header: "" for header in ORDER_ITEM_SOURCE_DETAIL_HEADERS}
    source_row["Run_ID"] = run_id
    source_row["Source_File"] = source_file
    source_row["Source_Tab"] = source_tab
    source_row["Source_Row_Type"] = row_type
    source_row["Order_ID"] = pick_text(row, "Order_ID", "order_id")
    source_row["Order_Item_ID"] = pick_text(row, "Order_Item_ID", "order_item_id")
    source_row["Return_ID"] = pick_text(row, "Return_ID", "return_id", "ReturnId")
    source_row["FSN"] = clean_fsn(row.get("FSN", ""))
    source_row["SKU_ID"] = pick_text(row, "SKU_ID", "Seller_SKU", "SellerSku", "SKU", "Sku Id")
    source_row["Product_Title"] = pick_text(row, "Product_Title", "Product Name", "Title", "Product")
    source_row["Order_Date"] = pick_text(row, "Order_Date", "Order Date")
    source_row["Dispatch_Date"] = pick_text(row, "Dispatch_Date", "Dispatch Date")
    source_row["Delivery_Date"] = pick_text(row, "Delivery_Date", "Delivery Date")
    source_row["Settlement_Date"] = pick_text(row, "Settlement_Date", "Settlement Date")
    source_row["Return_Date"] = pick_text(row, "Return_Date", "Return Date")
    source_row["Quantity"] = pick_text(row, "Quantity", "Qty")
    source_row["Selling_Price"] = pick_text(row, "Selling_Price", "Selling Price", "Sale Price")
    source_row["Settlement_Amount"] = pick_text(row, "Settlement_Amount", "Net_Settlement", "Net Settlement", "Amount_Settled", "Amount Settled")
    source_row["Commission"] = pick_text(row, "Commission", "Commission Fee")
    source_row["Shipping_Fee"] = pick_text(row, "Shipping_Fee", "Shipping Fee")
    source_row["Other_Fees"] = pick_text(row, "Other_Fees", "Other Fees")
    source_row["Total_Deductions"] = pick_text(row, "Total_Deductions", "Total Deductions")
    source_row["Cost_Price"] = pick_text(row, "Cost_Price", "Unit Cost")
    source_row["COGS"] = pick_text(row, "COGS", "Total_COGS", "Total COGS")
    source_row["Net_Profit"] = pick_text(row, "Net_Profit", "Final_Net_Profit", "Flipkart_Net_Earnings", "Net Profit")
    source_row["Profit_Margin"] = pick_text(row, "Profit_Margin", "Final_Profit_Margin", "Profit Margin")
    source_row["Return_Type"] = pick_text(row, "Return_Type", "Return_Bucket", "Dominant_Return_Type")
    source_row["Customer_Return_YN"] = pick_text(row, "Customer_Return_YN")
    source_row["Courier_Return_YN"] = pick_text(row, "Courier_Return_YN")
    source_row["Return_Status"] = pick_text(row, "Return_Status", "Status")
    source_row["Return_Reason"] = pick_text(row, "Return_Reason", "Reason")
    source_row["Return_Sub_Reason"] = pick_text(row, "Return_Sub_Reason", "Sub_Reason")
    source_row["Customer_Issue_Category"] = pick_text(row, "Customer_Issue_Category")
    source_row["Courier_Issue_Category"] = pick_text(row, "Courier_Issue_Category")
    source_row["Customer_Return_Risk_Level"] = pick_text(row, "Customer_Return_Risk_Level")
    source_row["Courier_Return_Risk_Level"] = pick_text(row, "Courier_Return_Risk_Level")
    source_row["Final_Ads_Decision"] = pick_text(row, "Final_Ads_Decision", "Suggested_Ad_Action")
    source_row["Competition_Risk_Level"] = pick_text(row, "Competition_Risk_Level")
    source_row["Alert_Count"] = pick_text(row, "Alert_Count")
    source_row["Critical_Alert_Count"] = pick_text(row, "Critical_Alert_Count")
    if row_type == "alert":
        source_row["Alert_Count"] = source_row["Alert_Count"] or "1"
        source_row["Critical_Alert_Count"] = source_row["Critical_Alert_Count"] or ("1" if pick_text(row, "Severity").lower() == "critical" else "0")
    if row_type in {"return", "return_v2"}:
        return_type = infer_return_type(row)
        if return_type == "unknown_return" and source_row["Return_Type"] in {"customer_return", "courier_return", "unknown_return"}:
            return_type = source_row["Return_Type"]
        source_row["Return_Type"] = source_row["Return_Type"] or return_type
        if return_type == "customer_return":
            source_row["Customer_Return_YN"] = source_row["Customer_Return_YN"] or "Yes"
            source_row["Customer_Issue_Category"] = source_row["Customer_Issue_Category"] or infer_customer_issue_category(row)
            source_row["Customer_Return_Risk_Level"] = source_row["Customer_Return_Risk_Level"] or infer_customer_risk(source_row["Customer_Issue_Category"])
        elif return_type == "courier_return":
            source_row["Courier_Return_YN"] = source_row["Courier_Return_YN"] or "Yes"
            source_row["Courier_Issue_Category"] = source_row["Courier_Issue_Category"] or infer_courier_issue_category(row)
            source_row["Courier_Return_Risk_Level"] = source_row["Courier_Return_Risk_Level"] or infer_courier_risk(source_row["Courier_Issue_Category"])
        if pick_text(row, "Customer_Return_Count", "Customer_Returns_Count") and text_value(row.get("Customer_Return_Count", row.get("Customer_Returns_Count", ""))) != "0":
            source_row["Customer_Return_YN"] = source_row["Customer_Return_YN"] or "Yes"
        if pick_text(row, "Courier_Return_Count", "Courier_Returns_Count") and text_value(row.get("Courier_Return_Count", row.get("Courier_Returns_Count", ""))) != "0":
            source_row["Courier_Return_YN"] = source_row["Courier_Return_YN"] or "Yes"
    if not source_row["Customer_Return_YN"] and source_row["Return_Type"] == "customer_return":
        source_row["Customer_Return_YN"] = "Yes"
    if not source_row["Courier_Return_YN"] and source_row["Return_Type"] == "courier_return":
        source_row["Courier_Return_YN"] = "Yes"

    fsn = clean_fsn(source_row["FSN"])
    if fsn and fsn in alert_counts:
        source_row["Alert_Count"] = source_row["Alert_Count"] or str(alert_counts[fsn]["Alert_Count"])
        source_row["Critical_Alert_Count"] = source_row["Critical_Alert_Count"] or str(alert_counts[fsn]["Critical_Alert_Count"])

    if not source_row["Data_Gap_Reason"]:
        gaps: List[str] = []
        if not source_row["Order_ID"]:
            gaps.append("Order_ID missing")
        if not source_row["Order_Item_ID"]:
            gaps.append("Order_Item_ID missing")
        if not source_row["FSN"]:
            gaps.append("FSN missing")
        if not source_row["Return_Type"] and not source_row["Return_ID"]:
            gaps.append("Return classification missing")
        source_row["Data_Gap_Reason"] = "; ".join(gaps)
    source_row["Last_Updated"] = pick_text(row, "Last_Updated", "Updated_At") or now_iso()
    return source_row


def build_source_detail_rows(source: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    frames: Dict[str, pd.DataFrame] = source["frames"]
    alert_rows: List[Dict[str, Any]] = source["alert_rows"]
    run_id = load_latest_run_id(
        [
            frames.get("return_all_details", pd.DataFrame()),
            frames.get("customer_return_comments", pd.DataFrame()),
            frames.get("courier_return_comments", pd.DataFrame()),
            frames.get("return_type_pivot", pd.DataFrame()),
            frames.get("return_comments", pd.DataFrame()),
            frames.get("adjusted_profit", pd.DataFrame()),
            frames.get("sku_analysis", pd.DataFrame()),
        ],
        [],
    )
    alert_counts = build_alert_counts(alert_rows)
    ordered_sources: List[tuple[str, str, str, Sequence[Dict[str, Any]]]] = []

    def add_source_rows(source_name: str, source_tab: str, row_type: str, rows: Sequence[Dict[str, Any]]) -> None:
        ordered_sources.append((source_name, source_tab, row_type, rows))

    add_source_rows("Orders.xlsx", "orders", "order", frames.get("orders", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("Returns Report.csv", "returns_report", "return", frames.get("return_comments", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("Returns.xlsx", "returns", "return", frames.get("returns", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("Settled Transactions.xlsx", "settlements", "settlement", frames.get("settlements", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("PNL.xlsx", "pnl", "pnl", frames.get("pnl", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_return_all_details.csv", "return_v2", "return_v2", frames.get("return_all_details", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_customer_return_comments.csv", "return_v2", "return_v2", frames.get("customer_return_comments", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_courier_return_comments.csv", "return_v2", "return_v2", frames.get("courier_return_comments", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_customer_return_issue_summary.csv", "return_v2", "return_v2", frames.get("customer_issue_summary", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_courier_return_summary.csv", "return_v2", "return_v2", frames.get("courier_issue_summary", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_return_type_pivot.csv", "return_v2", "return_v2", frames.get("return_type_pivot", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_return_comments.csv", "return", "return", frames.get("return_comments", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("FLIPKART_ALERTS_GENERATED", "alerts", "alert", alert_rows)
    add_source_rows("flipkart_ads_final_recommendations.csv", "ads_recommendations", "unknown", frames.get("ads_planner", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_competitor_price_intelligence.csv", "competitor_price", "unknown", load_csv_table(OUTPUT_DIR / "flipkart_competitor_price_intelligence.csv").fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_sku_analysis.csv", "sku_analysis", "unknown", frames.get("sku_analysis", pd.DataFrame()).fillna("").to_dict(orient="records"))
    add_source_rows("flipkart_adjusted_profit.csv", "adjusted_profit", "unknown", frames.get("adjusted_profit", pd.DataFrame()).fillna("").to_dict(orient="records"))

    source_detail_rows: List[Dict[str, Any]] = []
    source_counts: Dict[str, int] = {}
    for source_name, source_tab, row_type, rows in ordered_sources:
        source_counts[source_name] = 0
        for row in rows:
            if not any(text_value(value) for value in row.values()):
                continue
            source_detail_rows.append(
                build_source_detail_row(
                    row,
                    run_id=run_id,
                    source_file=source_name,
                    source_tab=source_tab,
                    row_type=row_type,
                    alert_counts=alert_counts,
                )
            )
            source_counts[source_name] += 1

    return source_detail_rows, {
        "run_id": run_id,
        "source_counts": source_counts,
        "alert_rows": len(alert_rows),
        "source_row_total": sum(source_counts.values()),
    }


def build_master_row_from_group(
    group_rows: Sequence[Dict[str, Any]],
    run_id: str,
) -> Dict[str, Any]:
    ordered_rows = sorted(group_rows, key=lambda row: (source_rank(text_value(row.get("Source_File", "")), text_value(row.get("Source_Row_Type", ""))), normalize_text(row.get("Last_Updated", ""))), reverse=False)
    master_row: Dict[str, Any] = {header: "" for header in ORDER_ITEM_MASTER_HEADERS}
    master_row["Run_ID"] = run_id
    master_row["Source_Row_Count"] = str(len(group_rows))
    master_row["Sources_Present"] = " | ".join(dict.fromkeys(text_value(row.get("Source_File", "")) for row in ordered_rows if text_value(row.get("Source_File", ""))))
    master_row["Last_Updated"] = max((text_value(row.get("Last_Updated", "")) for row in ordered_rows if text_value(row.get("Last_Updated", ""))), default=now_iso())

    def first_non_blank(*fields: str) -> str:
        for row in ordered_rows:
            for field in fields:
                value = text_value(row.get(field, ""))
                if value:
                    return value
        return ""

    master_row["Order_ID"] = first_non_blank("Order_ID")
    master_row["Order_Item_ID"] = first_non_blank("Order_Item_ID")
    master_row["Master_Order_Key"] = first_non_blank("Order_Item_ID") or order_item_master_key(ordered_rows[0])
    master_row["FSN"] = first_non_blank("FSN")
    master_row["SKU_ID"] = first_non_blank("SKU_ID")
    master_row["Product_Title"] = first_non_blank("Product_Title")
    master_row["Order_Date"] = first_non_blank("Order_Date")
    master_row["Quantity"] = first_non_blank("Quantity")
    master_row["Selling_Price"] = first_non_blank("Selling_Price")
    master_row["Settlement_Amount"] = first_non_blank("Settlement_Amount")
    master_row["Total_Deductions"] = first_non_blank("Total_Deductions")
    master_row["Cost_Price"] = first_non_blank("Cost_Price")
    master_row["COGS"] = first_non_blank("COGS")
    master_row["Net_Profit"] = first_non_blank("Net_Profit")
    master_row["Profit_Margin"] = first_non_blank("Profit_Margin")
    return_ids = []
    customer_return_seen = False
    courier_return_seen = False
    alert_count_total = 0
    critical_alert_count_total = 0
    return_types = []
    customer_issue = ""
    courier_issue = ""
    customer_risk = ""
    courier_risk = ""
    final_ads_decision = ""
    competition_risk = ""
    latest_event_date = ""
    return_status = ""
    return_reason = ""
    return_sub_reason = ""
    for row in ordered_rows:
        return_id = text_value(row.get("Return_ID", ""))
        if return_id and return_id not in return_ids:
            return_ids.append(return_id)
        if text_value(row.get("Customer_Return_YN", "")).lower() == "yes":
            customer_return_seen = True
        if text_value(row.get("Courier_Return_YN", "")).lower() == "yes":
            courier_return_seen = True
        alert_count_total += int(parse_float(row.get("Alert_Count", "")))
        critical_alert_count_total += int(parse_float(row.get("Critical_Alert_Count", "")))
        return_type = text_value(row.get("Return_Type", ""))
        if return_type and return_type not in return_types:
            return_types.append(return_type)
        customer_issue = customer_issue or text_value(row.get("Customer_Issue_Category", ""))
        courier_issue = courier_issue or text_value(row.get("Courier_Issue_Category", ""))
        customer_risk = customer_risk or text_value(row.get("Customer_Return_Risk_Level", ""))
        courier_risk = courier_risk or text_value(row.get("Courier_Return_Risk_Level", ""))
        final_ads_decision = final_ads_decision or text_value(row.get("Final_Ads_Decision", ""))
        competition_risk = competition_risk or text_value(row.get("Competition_Risk_Level", ""))
        latest_event_date = max(latest_event_date, text_value(row.get("Return_Date", "")), text_value(row.get("Settlement_Date", "")), text_value(row.get("Delivery_Date", "")), text_value(row.get("Dispatch_Date", "")), text_value(row.get("Order_Date", "")))
        return_status = return_status or text_value(row.get("Return_Status", ""))
        return_reason = return_reason or text_value(row.get("Return_Reason", ""))
        return_sub_reason = return_sub_reason or text_value(row.get("Return_Sub_Reason", ""))

    master_row["Latest_Event_Date"] = latest_event_date
    master_row["Return_YN"] = "Yes" if (return_ids or return_types or customer_return_seen or courier_return_seen) else "No"
    master_row["Return_IDs"] = " | ".join(return_ids)
    master_row["Return_Type_Final"] = " | ".join(dict.fromkeys(return_types))
    master_row["Customer_Return_YN"] = "Yes" if customer_return_seen else ""
    master_row["Courier_Return_YN"] = "Yes" if courier_return_seen else ""
    master_row["Return_Status_Final"] = return_status
    master_row["Return_Reason_Final"] = return_reason
    master_row["Return_Sub_Reason_Final"] = return_sub_reason
    master_row["Customer_Issue_Category"] = customer_issue
    master_row["Courier_Issue_Category"] = courier_issue
    master_row["Customer_Return_Risk_Level"] = customer_risk
    master_row["Courier_Return_Risk_Level"] = courier_risk
    master_row["Alert_Count"] = str(alert_count_total) if alert_count_total else ""
    master_row["Critical_Alert_Count"] = str(critical_alert_count_total) if critical_alert_count_total else ""
    master_row["Final_Ads_Decision"] = final_ads_decision
    master_row["Competition_Risk_Level"] = competition_risk
    master_row["Data_Completeness_Status"] = "Partial"
    return master_row


def build_master_rows(source_detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()
    alert_rows_by_fsn: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in source_detail_rows:
        if text_value(row.get("Source_Row_Type", "")) == "alert":
            fsn = clean_fsn(row.get("FSN", ""))
            if fsn:
                alert_rows_by_fsn[fsn].append(row)
            continue
        key = order_item_master_key(row)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)

    master_rows: List[Dict[str, Any]] = []
    for key, rows in grouped.items():
        fsn = clean_fsn(rows[0].get("FSN", ""))
        alert_rows = alert_rows_by_fsn.get(fsn, [])
        combined_rows = list(rows) + list(alert_rows)
        master_row = build_master_row_from_group(combined_rows, text_value(rows[0].get("Run_ID", "")))
        master_row["Master_Order_Key"] = key
        order_id = text_value(master_row.get("Order_ID", ""))
        fsn = text_value(master_row.get("FSN", ""))
        net_profit = text_value(master_row.get("Net_Profit", ""))
        return_type = text_value(master_row.get("Return_Type_Final", ""))
        data_gap_reason = []
        if not order_id:
            data_gap_reason.append("Missing Order ID")
        if not fsn:
            data_gap_reason.append("Missing FSN")
        if not net_profit:
            data_gap_reason.append("Missing Profit")
        if not return_type:
            data_gap_reason.append("Missing Return Classification")
        master_row["Data_Gap_Reason"] = "; ".join(data_gap_reason)
        if not data_gap_reason:
            master_row["Data_Completeness_Status"] = "Complete"
        elif len(data_gap_reason) == 1:
            master_row["Data_Completeness_Status"] = data_gap_reason[0]
        else:
            master_row["Data_Completeness_Status"] = "Partial"
        if not master_row["Customer_Return_YN"] and master_row["Return_Type_Final"] == "customer_return":
            master_row["Customer_Return_YN"] = "Yes"
        if not master_row["Courier_Return_YN"] and master_row["Return_Type_Final"] == "courier_return":
            master_row["Courier_Return_YN"] = "Yes"
        master_row["Source_Row_Count"] = str(len(combined_rows))
        sources_present = " | ".join(dict.fromkeys(text_value(row.get("Source_File", "")) for row in combined_rows if text_value(row.get("Source_File", ""))))
        master_row["Sources_Present"] = sources_present
        alert_rows = alert_rows_by_fsn.get(fsn, [])
        alert_count = len(alert_rows)
        critical_alert_count = sum(1 for row in alert_rows if normalize_text(row.get("Severity", "")).lower() == "critical")
        master_row["Alert_Count"] = str(alert_count) if alert_count else ""
        master_row["Critical_Alert_Count"] = str(critical_alert_count) if critical_alert_count else ""
        master_rows.append(master_row)
    return master_rows


def build_data_gap_reason(record: Dict[str, Any]) -> str:
    gaps = list(record.get("__data_gaps", []))
    if not text_value(record.get("Order_ID", "")):
        gaps.append("Order_ID missing")
    if not text_value(record.get("Order_Item_ID", "")):
        gaps.append("Order_Item_ID missing; duplicate check uses Order_ID + FSN + SKU_ID")
    if not text_value(record.get("FSN", "")):
        gaps.append("FSN missing")
    if not text_value(record.get("SKU_ID", "")):
        gaps.append("SKU_ID missing")
    if not text_value(record.get("Return_Status", "")) and not text_value(record.get("Return_ID", "")):
        gaps.append("No return record found")
    if not text_value(record.get("Settlement_Amount", "")) and not text_value(record.get("COGS", "")) and not text_value(record.get("Net_Profit", "")):
        gaps.append("No settlement or profit layer found")
    return "; ".join(dict.fromkeys(gaps))


def finalize_record(record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    record["Run_ID"] = run_id
    record["Last_Updated"] = now_iso()
    if not text_value(record.get("COGS", "")) and text_value(record.get("Cost_Price", "")) and text_value(record.get("Quantity", "")):
        record["COGS"] = format_number_text(parse_float(record["Cost_Price"]) * parse_float(record["Quantity"]))
    if not text_value(record.get("Total_Deductions", "")):
        deductions = parse_float(record.get("Commission", "")) + parse_float(record.get("Shipping_Fee", "")) + parse_float(record.get("Other_Fees", ""))
        if deductions:
            record["Total_Deductions"] = format_number_text(deductions)
    record["Data_Gap_Reason"] = build_data_gap_reason(record)
    record.pop("__data_gaps", None)
    final_row = {header: text_value(record.get(header, "")) for header in OUTPUT_HEADERS}
    return final_row


def read_sheet_rows_if_present(
    sheets_service: object,
    spreadsheet_id: str,
    tab_name: str,
) -> List[Dict[str, Any]]:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return []
    headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
    if not headers:
        return []
    return [{header: row.get(header, "") for header in headers} for row in rows]


def write_sheet_tab(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> int:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))
    add_basic_filter(sheets_service, spreadsheet_id, sheet_id, len(headers), len(rows) + 1)
    return sheet_id


def load_source_data(sheets_service: object, spreadsheet_id: str) -> Dict[str, Any]:
    frames: Dict[str, pd.DataFrame] = {}
    for key, path in LOCAL_CSV_SOURCES.items():
        frames[key] = load_csv_table(path)

    if frames["return_all_details"].empty:
        frames["return_all_details"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["return_all_details"])
    if frames["customer_return_comments"].empty:
        frames["customer_return_comments"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["customer_return_comments"])
    if frames["courier_return_comments"].empty:
        frames["courier_return_comments"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["courier_return_comments"])
    if frames["return_type_pivot"].empty:
        frames["return_type_pivot"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["return_type_pivot"])
    if frames["return_comments"].empty:
        frames["return_comments"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["return_comments"])
    if frames["sku_analysis"].empty:
        frames["sku_analysis"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["sku_analysis"])
    if frames["adjusted_profit"].empty:
        frames["adjusted_profit"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["adjusted_profit"])
    if frames.get("customer_issue_summary", pd.DataFrame()).empty and tab_exists(sheets_service, spreadsheet_id, "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY"):
        frames["customer_issue_summary"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY")
    if frames.get("courier_issue_summary", pd.DataFrame()).empty and tab_exists(sheets_service, spreadsheet_id, "FLIPKART_COURIER_RETURN_SUMMARY"):
        frames["courier_issue_summary"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, "FLIPKART_COURIER_RETURN_SUMMARY")
    if not frames["ads_recommendations"].empty:
        frames["ads_planner"] = frames["ads_recommendations"]
    elif frames["ads_planner"].empty:
        frames["ads_planner"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["ads_planner"])

    alert_rows = read_sheet_rows_if_present(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["alerts"])
    return {
        "frames": frames,
        "alert_rows": alert_rows,
    }


def build_legacy_order_item_explorer_rows(source: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    frames: Dict[str, pd.DataFrame] = source["frames"]
    alert_rows: List[Dict[str, Any]] = source["alert_rows"]
    run_id = load_latest_run_id(
        [
            frames.get("return_all_details", pd.DataFrame()),
            frames.get("customer_return_comments", pd.DataFrame()),
            frames.get("courier_return_comments", pd.DataFrame()),
            frames.get("return_type_pivot", pd.DataFrame()),
            frames.get("return_comments", pd.DataFrame()),
            frames.get("adjusted_profit", pd.DataFrame()),
            frames.get("sku_analysis", pd.DataFrame()),
        ],
        [],
    )

    records: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    source_counts: Dict[str, int] = {}

    def ingest_rows(rows: Sequence[Dict[str, Any]], source_name: str, merger) -> None:
        source_counts[source_name] = 0
        for index, row in enumerate(rows):
            if not any(text_value(value) for value in row.values()):
                continue
            key = row_key(row, source_name, index)
            record = records.setdefault(key, blank_record(run_id))
            merger(record, row, source_name)
            source_counts[source_name] += 1

    ingest_rows(frames.get("orders", pd.DataFrame()).fillna("").to_dict(orient="records"), "Orders.xlsx", merge_order_row)
    ingest_rows(frames.get("returns", pd.DataFrame()).fillna("").to_dict(orient="records"), "Returns.xlsx", merge_return_row)
    ingest_rows(frames.get("settlements", pd.DataFrame()).fillna("").to_dict(orient="records"), "Settled Transactions.xlsx", merge_settlement_row)
    ingest_rows(frames.get("pnl", pd.DataFrame()).fillna("").to_dict(orient="records"), "PNL.xlsx", merge_pnl_row)
    ingest_rows(frames.get("return_comments", pd.DataFrame()).fillna("").to_dict(orient="records"), "Returns Report.csv", merge_return_row)
    ingest_rows(frames.get("return_all_details", pd.DataFrame()).fillna("").to_dict(orient="records"), "flipkart_return_all_details.csv", merge_return_row)
    ingest_rows(frames.get("customer_return_comments", pd.DataFrame()).fillna("").to_dict(orient="records"), "flipkart_customer_return_comments.csv", merge_return_row)
    ingest_rows(frames.get("courier_return_comments", pd.DataFrame()).fillna("").to_dict(orient="records"), "flipkart_courier_return_comments.csv", merge_return_row)
    ingest_rows(frames.get("return_type_pivot", pd.DataFrame()).fillna("").to_dict(orient="records"), "flipkart_return_type_pivot.csv", merge_return_type_pivot_row)

    sku_frame = frames.get("sku_analysis", pd.DataFrame())
    if not sku_frame.empty and "FSN" in sku_frame.columns:
        for _, row in sku_frame.iterrows():
            fsn = clean_fsn(row.get("FSN", ""))
            if not fsn:
                continue
            for record in records.values():
                if clean_fsn(record.get("FSN", "")) == fsn:
                    merge_fsn_summary(record, row.to_dict(), "flipkart_sku_analysis.csv")

    adjusted_frame = frames.get("adjusted_profit", pd.DataFrame())
    if not adjusted_frame.empty and "FSN" in adjusted_frame.columns:
        for _, row in adjusted_frame.iterrows():
            fsn = clean_fsn(row.get("FSN", ""))
            if not fsn:
                continue
            for record in records.values():
                if clean_fsn(record.get("FSN", "")) == fsn:
                    merge_fsn_summary(record, row.to_dict(), "flipkart_adjusted_profit.csv")

    ads_frame = frames.get("ads_planner", pd.DataFrame())
    if not ads_frame.empty and "FSN" in ads_frame.columns:
        for _, row in ads_frame.iterrows():
            fsn = clean_fsn(row.get("FSN", ""))
            if not fsn:
                continue
            for record in records.values():
                if clean_fsn(record.get("FSN", "")) == fsn:
                    merge_fsn_summary(record, row.to_dict(), "flipkart_ads_final_recommendations.csv")

    competitor_frame = load_csv_table(OUTPUT_DIR / "flipkart_competitor_price_intelligence.csv")
    if not competitor_frame.empty and "FSN" in competitor_frame.columns:
        for _, row in competitor_frame.iterrows():
            fsn = clean_fsn(row.get("FSN", ""))
            if not fsn:
                continue
            for record in records.values():
                if clean_fsn(record.get("FSN", "")) == fsn:
                    merge_fsn_summary(record, row.to_dict(), "flipkart_competitor_price_intelligence.csv")

    alert_counts = build_alert_counts(alert_rows)
    for record in records.values():
        merge_alert_counts(record, alert_counts)

    final_rows = [finalize_record(record, run_id) for record in records.values()]
    return final_rows, {
        "run_id": run_id,
        "source_counts": source_counts,
        "alert_rows": len(alert_rows),
        "source_row_total": sum(source_counts.values()),
    }


def write_local_outputs(
    legacy_rows: Sequence[Dict[str, Any]],
    master_rows: Sequence[Dict[str, Any]],
    source_detail_rows: Sequence[Dict[str, Any]],
) -> None:
    write_csv(LOCAL_LEGACY_ORDER_ITEM_PATH, OUTPUT_HEADERS, legacy_rows)
    write_csv(LOCAL_LEGACY_LOOKER_PATH, OUTPUT_HEADERS, legacy_rows)
    write_csv(LOCAL_MASTER_PATH, ORDER_ITEM_MASTER_HEADERS, master_rows)
    write_csv(LOCAL_SOURCE_DETAIL_PATH, ORDER_ITEM_SOURCE_DETAIL_HEADERS, source_detail_rows)
    write_csv(LOCAL_LOOKER_MASTER_PATH, ORDER_ITEM_MASTER_HEADERS, master_rows)
    write_csv(LOCAL_LOOKER_SOURCE_DETAIL_PATH, ORDER_ITEM_SOURCE_DETAIL_HEADERS, source_detail_rows)


def _normalize_looker_mode(looker_mode: str | None) -> str:
    mode = text_value(looker_mode).lower() or "master-only"
    if mode not in LOOKER_MODE_VALUES:
        raise ValueError(f"Unsupported order-item Looker mode: {looker_mode}")
    return mode


def write_outputs_to_sheet(
    sheets_service,
    spreadsheet_id: str,
    legacy_rows: Sequence[Dict[str, Any]],
    master_rows: Sequence[Dict[str, Any]],
    source_detail_rows: Sequence[Dict[str, Any]],
    *,
    internal_mode: str = "master-only",
    looker_mode: str = "master-only",
    force_write: bool = False,
    force_looker_write: bool = False,
) -> Dict[str, Any]:
    normalized_internal_mode = _normalize_internal_mode(internal_mode)
    normalized_mode = _normalize_looker_mode(looker_mode)
    internal_manifest = _load_order_item_refresh_manifest()
    manifest = _load_order_item_looker_manifest()
    existing_internal_entries = {
        str(entry.get("tab_name", "")): dict(entry)
        for entry in internal_manifest.get("tabs", [])
        if isinstance(entry, dict) and str(entry.get("tab_name", "")).strip()
    }
    existing_entries = {
        str(entry.get("tab_name", "")): dict(entry)
        for entry in manifest.get("tabs", [])
        if isinstance(entry, dict) and str(entry.get("tab_name", "")).strip()
    }

    internal_tabs_written: List[str] = []
    internal_tabs_skipped: List[str] = []
    skipped_unchanged_internal_tabs: List[str] = []
    large_internal_tabs_skipped: List[str] = []
    looker_tabs_written: List[str] = []
    looker_tabs_skipped: List[str] = []
    skipped_unchanged_looker_tabs: List[str] = []
    large_looker_tabs_skipped: List[str] = []
    internal_manifest_entries: List[Dict[str, Any]] = []
    manifest_entries: List[Dict[str, Any]] = []

    for tab_name, headers, rows in [
        (ORDER_ITEM_TAB, OUTPUT_HEADERS, legacy_rows),
        (ORDER_ITEM_MASTER_TAB, ORDER_ITEM_MASTER_HEADERS, master_rows),
        (ORDER_ITEM_SOURCE_DETAIL_TAB, ORDER_ITEM_SOURCE_DETAIL_HEADERS, source_detail_rows),
    ]:
        row_count = len(rows)
        column_count = len(headers)
        content_hash = _content_hash(headers, rows)
        existing_entry = existing_internal_entries.get(tab_name, {})
        last_written_at = text_value(existing_entry.get("last_written_at", ""))
        current_sheet_hash = ""
        skip_reason = ""
        should_write = False

        if normalized_internal_mode == "master-only":
            if tab_name == ORDER_ITEM_MASTER_TAB:
                should_write = True
            else:
                skip_reason = "mode"
        elif normalized_internal_mode == "light":
            if tab_name == ORDER_ITEM_MASTER_TAB:
                should_write = True
            elif tab_name == ORDER_ITEM_TAB:
                if row_count > INTERNAL_LIGHT_EXPLORER_ROW_THRESHOLD:
                    skip_reason = "row_threshold"
                    large_internal_tabs_skipped.append(tab_name)
                else:
                    should_write = True
            else:
                skip_reason = "mode"
        elif normalized_internal_mode == "full":
            should_write = True

        if should_write and not force_write:
            stored_hash = text_value(existing_entry.get("content_hash", ""))
            if not stored_hash:
                current_sheet_hash = _current_sheet_content_hash(sheets_service, spreadsheet_id, tab_name)
            if stored_hash == content_hash or current_sheet_hash == content_hash:
                should_write = False
                skip_reason = "unchanged_hash"
                skipped_unchanged_internal_tabs.append(tab_name)

        if should_write:
            write_sheet_tab(sheets_service, spreadsheet_id, tab_name, headers, rows)
            last_written_at = now_iso()
            internal_tabs_written.append(tab_name)
        else:
            internal_tabs_skipped.append(tab_name)
            if skip_reason == "row_threshold" and tab_name not in large_internal_tabs_skipped:
                large_internal_tabs_skipped.append(tab_name)

        internal_manifest_entries.append(
            {
                "tab_name": tab_name,
                "row_count": row_count,
                "column_count": column_count,
                "content_hash": content_hash,
                "last_written_at": last_written_at,
                "written": should_write,
                "skip_reason": skip_reason,
            }
        )

    looker_tab_specs = [
        (LOOKER_ORDER_ITEM_TAB, OUTPUT_HEADERS, legacy_rows),
        (LOOKER_ORDER_ITEM_MASTER_TAB, ORDER_ITEM_MASTER_HEADERS, master_rows),
        (LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB, ORDER_ITEM_SOURCE_DETAIL_HEADERS, source_detail_rows),
    ]

    for tab_name, headers, rows in looker_tab_specs:
        row_count = len(rows)
        column_count = len(headers)
        content_hash = _content_hash(headers, rows)
        existing_entry = existing_entries.get(tab_name, {})
        last_written_at = text_value(existing_entry.get("last_written_at", ""))
        current_sheet_hash = ""
        skip_reason = ""
        should_write = False

        if normalized_mode == "none":
            skip_reason = "mode"
        elif normalized_mode == "master-only":
            if tab_name == LOOKER_ORDER_ITEM_MASTER_TAB:
                should_write = True
            else:
                skip_reason = "mode"
        elif normalized_mode == "light":
            if tab_name == LOOKER_ORDER_ITEM_MASTER_TAB:
                should_write = True
            elif tab_name == LOOKER_ORDER_ITEM_TAB:
                if row_count > LOOKER_LIGHT_EXPLORER_ROW_THRESHOLD:
                    skip_reason = "row_threshold"
                    large_looker_tabs_skipped.append(tab_name)
                else:
                    should_write = True
            else:
                skip_reason = "mode"
                large_looker_tabs_skipped.append(tab_name)
        elif normalized_mode == "full":
            should_write = True

        if should_write and not force_looker_write:
            stored_hash = text_value(existing_entry.get("content_hash", ""))
            if not stored_hash:
                current_sheet_hash = _current_sheet_content_hash(sheets_service, spreadsheet_id, tab_name)
            if stored_hash == content_hash or current_sheet_hash == content_hash:
                should_write = False
                skip_reason = "unchanged_hash"
                skipped_unchanged_looker_tabs.append(tab_name)

        if should_write:
            write_sheet_tab(sheets_service, spreadsheet_id, tab_name, headers, rows)
            last_written_at = now_iso()
            looker_tabs_written.append(tab_name)
        else:
            looker_tabs_skipped.append(tab_name)
            if skip_reason == "row_threshold" and tab_name not in large_looker_tabs_skipped:
                large_looker_tabs_skipped.append(tab_name)
            if skip_reason == "mode" and tab_name == LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB and normalized_mode != "full":
                large_looker_tabs_skipped.append(tab_name)

        manifest_entries.append(
            {
                "tab_name": tab_name,
                "row_count": row_count,
                "column_count": column_count,
                "content_hash": content_hash,
                "last_written_at": last_written_at,
                "written": should_write,
                "skip_reason": skip_reason,
            }
        )

    manifest_payload = {
        **{key: value for key, value in manifest.items() if key != "tabs"},
        "spreadsheet_id": spreadsheet_id,
        "last_written_at": now_iso(),
        "last_order_item_looker_mode": normalized_mode,
        "looker_mode": normalized_mode,
        "quota_safe_mode": normalized_mode != "full",
        "force_looker_write": bool(force_looker_write),
        "skipped_unchanged_looker_tabs": skipped_unchanged_looker_tabs,
        "large_looker_tabs_skipped": large_looker_tabs_skipped,
        "looker_tabs_written": looker_tabs_written,
        "looker_tabs_skipped": looker_tabs_skipped,
        "tabs": sorted(manifest_entries, key=lambda entry: entry["tab_name"]),
    }
    internal_manifest_payload = {
        **{key: value for key, value in internal_manifest.items() if key != "tabs"},
        "spreadsheet_id": spreadsheet_id,
        "last_written_at": now_iso(),
        "last_order_item_internal_mode": normalized_internal_mode,
        "internal_mode": normalized_internal_mode,
        "quota_safe_mode": normalized_internal_mode != "full" and normalized_mode != "full",
        "force_write": bool(force_write),
        "skipped_unchanged_internal_tabs": skipped_unchanged_internal_tabs,
        "large_internal_tabs_skipped": large_internal_tabs_skipped,
        "internal_tabs_written": internal_tabs_written,
        "internal_tabs_skipped": internal_tabs_skipped,
        "tabs": sorted(internal_manifest_entries, key=lambda entry: entry["tab_name"]),
    }
    _save_order_item_refresh_manifest(internal_manifest_payload)
    _save_order_item_looker_manifest(manifest_payload)
    tabs_written = internal_tabs_written + looker_tabs_written
    return {
        "internal_mode": normalized_internal_mode,
        "internal_tabs_written": internal_tabs_written,
        "internal_tabs_skipped": internal_tabs_skipped,
        "looker_tabs_written": looker_tabs_written,
        "looker_tabs_skipped": looker_tabs_skipped,
        "skipped_unchanged_internal_tabs": skipped_unchanged_internal_tabs,
        "skipped_unchanged_looker_tabs": skipped_unchanged_looker_tabs,
        "large_internal_tabs_skipped": large_internal_tabs_skipped,
        "large_looker_tabs_skipped": large_looker_tabs_skipped,
        "internal_manifest_path": str(ORDER_ITEM_REFRESH_MANIFEST_PATH),
        "looker_manifest_path": str(ORDER_ITEM_LOOKER_REFRESH_MANIFEST_PATH),
        "quota_safe_mode": normalized_internal_mode != "full" and normalized_mode != "full",
        "looker_mode": normalized_mode,
        "tabs_written": tabs_written,
        "tabs_updated": tabs_written,
    }


def create_flipkart_order_item_explorer(
    *,
    internal_mode: str = "master-only",
    looker_mode: str = "master-only",
    force_write: bool = False,
    force_looker_write: bool = False,
) -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    source = load_source_data(sheets_service, spreadsheet_id)
    source_detail_rows, detail_summary = build_source_detail_rows(source)
    master_rows = build_master_rows(source_detail_rows)
    normalized_internal_mode = _normalize_internal_mode(internal_mode)
    normalized_looker_mode = _normalize_looker_mode(looker_mode)
    fast_master_only_path = normalized_internal_mode == "master-only" and normalized_looker_mode == "master-only"
    if fast_master_only_path:
        legacy_rows: List[Dict[str, Any]] = []
        summary = {
            "run_id": detail_summary["run_id"],
            "source_counts": {},
            "alert_rows": detail_summary["alert_rows"],
            "source_row_total": detail_summary["source_row_total"],
        }
    else:
        legacy_rows, summary = build_legacy_order_item_explorer_rows(source)
    if fast_master_only_path:
        write_csv(LOCAL_MASTER_PATH, ORDER_ITEM_MASTER_HEADERS, master_rows)
        write_csv(LOCAL_LOOKER_MASTER_PATH, ORDER_ITEM_MASTER_HEADERS, master_rows)
    else:
        write_local_outputs(legacy_rows, master_rows, source_detail_rows)
    sheet_write_summary = write_outputs_to_sheet(
        sheets_service,
        spreadsheet_id,
        legacy_rows,
        master_rows,
        source_detail_rows,
        internal_mode=internal_mode,
        looker_mode=looker_mode,
        force_write=force_write,
        force_looker_write=force_looker_write,
    )

    order_id_present_count = sum(1 for row in master_rows if text_value(row.get("Order_ID", "")))
    order_item_id_present_count = sum(1 for row in master_rows if text_value(row.get("Order_Item_ID", "")))
    blank_fsn_count = sum(1 for row in master_rows if not clean_fsn(row.get("FSN", "")))
    order_only_fallback_count = sum(1 for row in master_rows if not text_value(row.get("Order_Item_ID", "")) and text_value(row.get("Order_ID", "")))
    order_item_counts = Counter(text_value(row.get("Order_Item_ID", "")) for row in master_rows if text_value(row.get("Order_Item_ID", "")))
    duplicate_order_item_id_count = sum(count - 1 for count in order_item_counts.values() if count > 1)
    source_detail_blank_fsn_count = sum(1 for row in source_detail_rows if not clean_fsn(row.get("FSN", "")))

    warnings: List[str] = []
    if summary["source_row_total"] == 0:
        warnings.append("No local order-level source rows were available")
    if order_item_id_present_count == 0 and order_id_present_count > 0:
        warnings.append("Order_Item_ID values are still missing for the available order rows")
    if blank_fsn_count > 0:
        warnings.append(f"{blank_fsn_count} master rows are missing FSN")
    if source_detail_blank_fsn_count > 0:
        warnings.append(f"{source_detail_blank_fsn_count} source detail rows are missing FSN")
    if order_only_fallback_count > 0:
        warnings.append(f"{order_only_fallback_count} master rows use the order-only fallback key")

    status = "PASS"
    if duplicate_order_item_id_count > 0:
        status = "FAIL"
    elif warnings:
        status = "PASS_WITH_WARNINGS"

    result = {
        "status": status,
        "spreadsheet_id": spreadsheet_id,
        "run_id": summary["run_id"],
        "internal_mode": sheet_write_summary["internal_mode"],
        "looker_mode": sheet_write_summary["looker_mode"],
        "internal_tabs_written": sheet_write_summary["internal_tabs_written"],
        "internal_tabs_skipped": sheet_write_summary["internal_tabs_skipped"],
        "looker_tabs_written": sheet_write_summary["looker_tabs_written"],
        "looker_tabs_skipped": sheet_write_summary["looker_tabs_skipped"],
        "skipped_unchanged_internal_tabs": sheet_write_summary["skipped_unchanged_internal_tabs"],
        "skipped_unchanged_looker_tabs": sheet_write_summary["skipped_unchanged_looker_tabs"],
        "large_internal_tabs_skipped": sheet_write_summary["large_internal_tabs_skipped"],
        "large_looker_tabs_skipped": sheet_write_summary["large_looker_tabs_skipped"],
        "quota_safe_mode": sheet_write_summary["quota_safe_mode"],
        "internal_manifest_path": sheet_write_summary["internal_manifest_path"],
        "looker_manifest_path": sheet_write_summary["looker_manifest_path"],
        "legacy_explorer_rows": len(legacy_rows),
        "master_rows": len(master_rows),
        "source_detail_rows": len(source_detail_rows),
        "looker_rows": len(legacy_rows),
        "order_id_present_count": order_id_present_count,
        "order_item_id_present_count": order_item_id_present_count,
        "duplicate_order_item_id_count": duplicate_order_item_id_count,
        "blank_fsn_count_master": blank_fsn_count,
        "source_detail_blank_fsn_count": source_detail_blank_fsn_count,
        "order_only_fallback_count": order_only_fallback_count,
        "warnings": warnings,
        "tabs_written": sheet_write_summary["tabs_written"],
        "tabs_updated": sheet_write_summary["tabs_updated"],
        "local_outputs": {
            "order_item_explorer": str(LOCAL_LEGACY_ORDER_ITEM_PATH),
            "looker_order_item_explorer": str(LOCAL_LEGACY_LOOKER_PATH),
            "order_item_master": str(LOCAL_MASTER_PATH),
            "order_item_source_detail": str(LOCAL_SOURCE_DETAIL_PATH),
            "looker_order_item_master": str(LOCAL_LOOKER_MASTER_PATH),
            "looker_order_item_source_detail": str(LOCAL_LOOKER_SOURCE_DETAIL_PATH),
        },
        "source_counts": summary["source_counts"],
        "source_detail_source_counts": detail_summary["source_counts"],
        "alert_rows": summary["alert_rows"],
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "run_id",
            "legacy_explorer_rows",
            "master_rows",
            "source_detail_rows",
            "looker_rows",
            "order_id_present_count",
            "order_item_id_present_count",
            "duplicate_order_item_id_count",
            "blank_fsn_count_master",
            "source_detail_blank_fsn_count",
            "order_only_fallback_count",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "spreadsheet_id": spreadsheet_id,
                "run_id": summary["run_id"],
                "legacy_explorer_rows": len(legacy_rows),
                "master_rows": len(master_rows),
                "source_detail_rows": len(source_detail_rows),
                "looker_rows": len(legacy_rows),
                "order_id_present_count": order_id_present_count,
                "order_item_id_present_count": order_item_id_present_count,
                "duplicate_order_item_id_count": duplicate_order_item_id_count,
                "blank_fsn_count_master": blank_fsn_count,
                "source_detail_blank_fsn_count": source_detail_blank_fsn_count,
                "order_only_fallback_count": order_only_fallback_count,
                "status": status,
                "message": f"Rebuilt Flipkart order-item tabs with internal_mode={sheet_write_summary['internal_mode']} looker_mode={sheet_write_summary['looker_mode']}",
            }
        ],
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the Flipkart order-item explorer and master tabs.")
    parser.add_argument("--internal-mode", choices=sorted(INTERNAL_MODE_VALUES), default="master-only", help="Order-item internal write mode. Default: master-only.")
    parser.add_argument("--looker-mode", choices=sorted(LOOKER_MODE_VALUES), default="master-only", help="Order-item Looker write mode. Default: master-only.")
    parser.add_argument("--force-write", action="store_true", help="Rewrite internal order-item tabs even when the content hash is unchanged.")
    parser.add_argument("--force-looker-write", action="store_true", help="Rewrite Looker order-item tabs even when the content hash is unchanged.")
    args = parser.parse_args()

    try:
        create_flipkart_order_item_explorer(
            internal_mode=args.internal_mode,
            looker_mode=args.looker_mode,
            force_write=args.force_write,
            force_looker_write=args.force_looker_write,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "spreadsheet_id": load_json(SPREADSHEET_META_PATH)["spreadsheet_id"] if SPREADSHEET_META_PATH.exists() else "",
                    "log_path": str(LOG_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
