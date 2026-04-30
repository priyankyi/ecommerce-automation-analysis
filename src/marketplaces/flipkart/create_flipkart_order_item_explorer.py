from __future__ import annotations

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
LOCAL_ORDER_ITEM_PATH = OUTPUT_DIR / "flipkart_order_item_explorer.csv"
LOCAL_LOOKER_PATH = OUTPUT_DIR / "looker_flipkart_order_item_explorer.csv"
ORDER_ITEM_TAB = "FLIPKART_ORDER_ITEM_EXPLORER"
LOOKER_ORDER_ITEM_TAB = "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER"

LOCAL_CSV_SOURCES: Dict[str, Path] = {
    "orders": NORMALIZED_ORDERS_PATH,
    "returns": NORMALIZED_RETURNS_PATH,
    "settlements": NORMALIZED_SETTLEMENTS_PATH,
    "pnl": NORMALIZED_PNL_PATH,
    "sku_analysis": SKU_ANALYSIS_PATH,
    "adjusted_profit": OUTPUT_DIR / "flipkart_adjusted_profit.csv",
    "return_comments": OUTPUT_DIR / "flipkart_return_comments.csv",
    "ads_recommendations": OUTPUT_DIR / "flipkart_ads_final_recommendations.csv",
    "ads_planner": OUTPUT_DIR / "flipkart_ads_planner.csv",
    "competitor_price": OUTPUT_DIR / "flipkart_competitor_price_intelligence.csv",
}

SHEET_FALLBACK_TABS = {
    "alerts": "FLIPKART_ALERTS_GENERATED",
    "sku_analysis": "FLIPKART_SKU_ANALYSIS",
    "adjusted_profit": "FLIPKART_ADJUSTED_PROFIT",
    "return_comments": "FLIPKART_RETURN_COMMENTS",
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
    order_item_id = text_value(row.get("Order_Item_ID", ""))
    order_id = text_value(row.get("Order_ID", ""))
    fsn = clean_fsn(row.get("FSN", ""))
    sku = text_value(row.get("SKU_ID", "")) or text_value(row.get("Seller_SKU", ""))
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

    if frames["return_comments"].empty:
        frames["return_comments"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["return_comments"])
    if frames["sku_analysis"].empty:
        frames["sku_analysis"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["sku_analysis"])
    if frames["adjusted_profit"].empty:
        frames["adjusted_profit"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["adjusted_profit"])
    if not frames["ads_recommendations"].empty:
        frames["ads_planner"] = frames["ads_recommendations"]
    elif frames["ads_planner"].empty:
        frames["ads_planner"] = load_sheet_table_if_available(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["ads_planner"])

    alert_rows = read_sheet_rows_if_present(sheets_service, spreadsheet_id, SHEET_FALLBACK_TABS["alerts"])
    return {
        "frames": frames,
        "alert_rows": alert_rows,
    }


def build_order_item_explorer_rows(source: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    frames: Dict[str, pd.DataFrame] = source["frames"]
    alert_rows: List[Dict[str, Any]] = source["alert_rows"]
    run_id = load_latest_run_id(
        [
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


def write_local_outputs(rows: Sequence[Dict[str, Any]]) -> None:
    write_csv(LOCAL_ORDER_ITEM_PATH, OUTPUT_HEADERS, rows)
    write_csv(LOCAL_LOOKER_PATH, OUTPUT_HEADERS, rows)


def write_outputs_to_sheet(sheets_service, spreadsheet_id: str, rows: Sequence[Dict[str, Any]]) -> List[str]:
    write_sheet_tab(sheets_service, spreadsheet_id, ORDER_ITEM_TAB, OUTPUT_HEADERS, rows)
    write_sheet_tab(sheets_service, spreadsheet_id, LOOKER_ORDER_ITEM_TAB, OUTPUT_HEADERS, rows)
    return [ORDER_ITEM_TAB, LOOKER_ORDER_ITEM_TAB]


def create_flipkart_order_item_explorer() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    source = load_source_data(sheets_service, spreadsheet_id)
    rows, summary = build_order_item_explorer_rows(source)
    write_local_outputs(rows)
    tabs_updated = write_outputs_to_sheet(sheets_service, spreadsheet_id, rows)

    order_id_present_count = sum(1 for row in rows if text_value(row.get("Order_ID", "")))
    order_item_id_present_count = sum(1 for row in rows if text_value(row.get("Order_Item_ID", "")))
    blank_fsn_count = sum(1 for row in rows if not clean_fsn(row.get("FSN", "")))
    duplicate_order_item_id_count = 0
    order_item_counts = Counter(text_value(row.get("Order_Item_ID", "")) for row in rows if text_value(row.get("Order_Item_ID", "")))
    duplicate_order_item_id_count = sum(count - 1 for count in order_item_counts.values() if count > 1)

    warnings: List[str] = []
    if summary["source_row_total"] == 0:
        warnings.append("No local order-level source rows were available")
    if order_item_id_present_count == 0 and order_id_present_count > 0:
        warnings.append("Order_Item_ID values are still missing for the available order rows")

    result = {
        "status": "SUCCESS",
        "spreadsheet_id": spreadsheet_id,
        "run_id": summary["run_id"],
        "order_item_rows": len(rows),
        "looker_rows": len(rows),
        "order_id_present_count": order_id_present_count,
        "order_item_id_present_count": order_item_id_present_count,
        "duplicate_order_item_id_count": duplicate_order_item_id_count,
        "blank_fsn_count": blank_fsn_count,
        "warnings": warnings,
        "tabs_updated": tabs_updated,
        "local_outputs": {
            "order_item_explorer": str(LOCAL_ORDER_ITEM_PATH),
            "looker_order_item_explorer": str(LOCAL_LOOKER_PATH),
        },
        "source_counts": summary["source_counts"],
        "alert_rows": summary["alert_rows"],
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "run_id",
            "order_item_rows",
            "looker_rows",
            "order_id_present_count",
            "order_item_id_present_count",
            "duplicate_order_item_id_count",
            "blank_fsn_count",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "spreadsheet_id": spreadsheet_id,
                "run_id": summary["run_id"],
                "order_item_rows": len(rows),
                "looker_rows": len(rows),
                "order_id_present_count": order_id_present_count,
                "order_item_id_present_count": order_item_id_present_count,
                "duplicate_order_item_id_count": duplicate_order_item_id_count,
                "blank_fsn_count": blank_fsn_count,
                "status": "SUCCESS",
                "message": "Rebuilt Flipkart order-item explorer tabs",
            }
        ],
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_order_item_explorer()
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
