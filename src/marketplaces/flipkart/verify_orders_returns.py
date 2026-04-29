from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_utils import (
    NORMALIZED_ORDERS_PATH,
    NORMALIZED_RETURNS_PATH,
    RAW_INPUT_DIR,
    TARGET_FSN_PATH,
    build_status_payload,
    clean_fsn,
    dedupe_dict_rows,
    ensure_directories,
    get_cell,
    load_report_patterns,
    load_synonyms,
    normalize_text,
    read_workbook_rows,
    select_best_sheet_across_files,
    write_csv,
)

ORDER_HEADERS = [
    "FSN",
    "Order_ID",
    "Order_Item_ID",
    "Seller_SKU",
    "Product_Title",
    "Order_Date",
    "Quantity",
    "Selling_Price",
    "Order_Status",
    "Dispatch_Date",
    "Delivery_Date",
    "Cancellation_Status",
    "Source_File",
    "Mapping_Confidence",
]

RETURN_HEADERS = [
    "FSN",
    "Order_ID",
    "Order_Item_ID",
    "Seller_SKU",
    "Return_ID",
    "Return_Date",
    "Return_Type",
    "Return_Reason",
    "Return_Status",
    "Reverse_Shipment_Status",
    "Source_File",
    "Mapping_Confidence",
]

ORDER_REQUIRED_COLUMNS = [
    "fsn",
    "order_item_id",
    "order_id",
    "sku_id",
    "product_title",
    "quantity",
    "order_date",
    "order_status",
]

RETURN_REQUIRED_COLUMNS = [
    "return_id",
    "order_item_id",
    "return_status",
    "return_reason",
    "return_date",
]


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    import csv

    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_target_fsns() -> Dict[str, Dict[str, Any]]:
    if not TARGET_FSN_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {TARGET_FSN_PATH}")
    rows = read_csv_rows(TARGET_FSN_PATH)
    return {clean_fsn(row.get("FSN", "")): row for row in rows if clean_fsn(row.get("FSN", ""))}


def value(row: Sequence[Any], columns: Dict[str, Dict[str, Any]], key: str) -> str:
    column = columns.get(key)
    if not column:
        return ""
    return normalize_text(get_cell(row, int(column["index"])))


def selected_sheet_details(selection: Dict[str, Any]) -> Dict[str, Any]:
    selected = selection.get("selected_sheet") or {}
    sheet_name = selected.get("sheet_name", "")
    return {
        "file_name": selected.get("file_name", ""),
        "file_path": selected.get("file_path", ""),
        "sheet_name": sheet_name,
        "header_row_index": int(selected.get("header_row_index", 0) or 0),
        "selection_score": float(selected.get("selection_score", 0.0) or 0.0),
        "combined_score": float(selected.get("combined_score", 0.0) or 0.0),
        "raw_row_count": int(selected.get("raw_row_count", 0) or 0),
        "non_empty_row_count": int(selected.get("non_empty_row_count", 0) or 0),
        "detected_columns": selected.get("detected_columns", {}) or {},
        "exclusion_reason": selected.get("exclusion_reason", ""),
        "selected_sheet_is_help": "help" in normalize_text(sheet_name).lower(),
    }


def normalize_orders(
    file_path: Path,
    sheet_name: str,
    header_row_index: int,
    columns: Dict[str, Dict[str, Any]],
    data_rows: Sequence[Sequence[Any]],
    target_fsns: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    target_set = set(target_fsns)
    output_rows: List[Dict[str, Any]] = []
    all_order_fsns: List[str] = []
    matched_target_fsns: List[str] = []

    for row in data_rows:
        fsn = clean_fsn(value(row, columns, "fsn"))
        if fsn:
            all_order_fsns.append(fsn)
        if not fsn or fsn not in target_set:
            continue
        matched_target_fsns.append(fsn)
        output_rows.append(
            {
                "FSN": fsn,
                "Order_ID": clean_fsn(value(row, columns, "order_id")),
                "Order_Item_ID": clean_fsn(value(row, columns, "order_item_id")),
                "Seller_SKU": value(row, columns, "sku_id"),
                "Product_Title": value(row, columns, "product_title"),
                "Order_Date": value(row, columns, "order_date"),
                "Quantity": value(row, columns, "quantity"),
                "Selling_Price": value(row, columns, "selling_price"),
                "Order_Status": value(row, columns, "order_status"),
                "Dispatch_Date": value(row, columns, "dispatch_date"),
                "Delivery_Date": value(row, columns, "delivery_date"),
                "Cancellation_Status": value(row, columns, "cancellation_status"),
                "Source_File": file_path.name,
                "Mapping_Confidence": "HIGH",
            }
        )

    normalized_rows = dedupe_dict_rows(output_rows, "Order_Item_ID")
    if not normalized_rows:
        normalized_rows = dedupe_dict_rows(output_rows, "Order_ID")

    order_lookup: Dict[str, str] = {}
    for row in normalized_rows:
        order_item_id = clean_fsn(row.get("Order_Item_ID", ""))
        order_id = clean_fsn(row.get("Order_ID", ""))
        fsn = clean_fsn(row.get("FSN", ""))
        if order_item_id and fsn:
            order_lookup.setdefault(order_item_id, fsn)
        if order_id and fsn:
            order_lookup.setdefault(order_id, fsn)

    summary = {
        "file_name": file_path.name,
        "sheet_name": sheet_name,
        "header_row_index": header_row_index,
        "raw_rows": len(data_rows),
        "orders_fsn_count": len(set(all_order_fsns)),
        "orders_matched_target_fsn_count": len(set(matched_target_fsns)),
        "normalized_orders_rows": len(normalized_rows),
        "selected_sheet_is_help": "help" in normalize_text(sheet_name).lower(),
        "column_presence": {
            key: key in columns for key in ORDER_REQUIRED_COLUMNS
        },
        "columns_detected": sorted(columns.keys()),
    }
    return normalized_rows, {"summary": summary, "order_lookup": order_lookup}


def normalize_returns(
    file_path: Path,
    sheet_name: str,
    header_row_index: int,
    columns: Dict[str, Dict[str, Any]],
    data_rows: Sequence[Sequence[Any]],
    target_fsns: Dict[str, Dict[str, Any]],
    order_lookup: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    target_set = set(target_fsns)
    output_rows: List[Dict[str, Any]] = []
    mapped_via_order_item_id = 0
    direct_fsn_hits = 0
    matched_target_fsns: List[str] = []

    for row in data_rows:
        order_item_id = clean_fsn(value(row, columns, "order_item_id"))
        direct_fsn = clean_fsn(value(row, columns, "fsn"))
        fsn = ""
        mapping_confidence = "LOW"

        if order_item_id and order_item_id in order_lookup:
            fsn = clean_fsn(order_lookup[order_item_id])
            mapping_confidence = "HIGH"
            mapped_via_order_item_id += 1
        elif direct_fsn and direct_fsn in target_set:
            fsn = direct_fsn
            mapping_confidence = "HIGH"
            direct_fsn_hits += 1

        if not fsn or fsn not in target_set:
            continue

        matched_target_fsns.append(fsn)
        output_rows.append(
            {
                "FSN": fsn,
                "Order_ID": clean_fsn(value(row, columns, "order_id")),
                "Order_Item_ID": order_item_id,
                "Seller_SKU": value(row, columns, "sku_id"),
                "Return_ID": clean_fsn(value(row, columns, "return_id")),
                "Return_Date": value(row, columns, "return_date"),
                "Return_Type": value(row, columns, "return_type"),
                "Return_Reason": value(row, columns, "return_reason"),
                "Return_Status": value(row, columns, "return_status"),
                "Reverse_Shipment_Status": value(row, columns, "reverse_shipment_status"),
                "Source_File": file_path.name,
                "Mapping_Confidence": mapping_confidence,
            }
        )

    summary = {
        "file_name": file_path.name,
        "sheet_name": sheet_name,
        "header_row_index": header_row_index,
        "raw_rows": len(data_rows),
        "returns_mapped_via_order_item_id_count": mapped_via_order_item_id,
        "returns_direct_fsn_hits": direct_fsn_hits,
        "returns_matched_target_fsn_count": len(set(matched_target_fsns)),
        "normalized_returns_rows": len(output_rows),
        "selected_sheet_is_help": "help" in normalize_text(sheet_name).lower(),
        "column_presence": {
            key: key in columns for key in RETURN_REQUIRED_COLUMNS
        },
        "columns_detected": sorted(columns.keys()),
    }
    return output_rows, {"summary": summary}


def filter_report_files(raw_files: Sequence[Path], report_type: str, patterns: Dict[str, Any]) -> List[Path]:
    report_spec = patterns.get("report_types", {}).get(report_type, {})
    filename_keywords = [normalize_text(keyword).lower() for keyword in report_spec.get("filename_keywords", []) if normalize_text(keyword)]
    if not filename_keywords:
        return list(raw_files)

    filtered = []
    for file_path in raw_files:
        file_name = normalize_text(file_path.name).lower()
        if any(keyword in file_name for keyword in filename_keywords):
            filtered.append(file_path)
    return filtered or list(raw_files)


def verify_orders_returns() -> Dict[str, Any]:
    ensure_directories()
    target_fsns = load_target_fsns()
    synonyms = load_synonyms()
    patterns = load_report_patterns()
    raw_files = sorted(path for path in RAW_INPUT_DIR.iterdir() if path.suffix.lower() in {".csv", ".xls", ".xlsx", ".xlsm"}) if RAW_INPUT_DIR.exists() else []
    order_files = filter_report_files(raw_files, "orders", patterns)
    return_files = filter_report_files(raw_files, "returns", patterns)

    orders_selection = select_best_sheet_across_files(order_files, "orders", synonyms, patterns)
    returns_selection = select_best_sheet_across_files(return_files, "returns", synonyms, patterns)

    orders_details = selected_sheet_details(orders_selection)
    returns_details = selected_sheet_details(returns_selection)

    orders_rows: List[Dict[str, Any]] = []
    returns_rows: List[Dict[str, Any]] = []
    orders_payload: Dict[str, Any] = {}
    returns_payload: Dict[str, Any] = {}

    if orders_details["file_path"] and orders_details["sheet_name"]:
        workbook_rows = read_workbook_rows(Path(orders_details["file_path"]))
        rows = workbook_rows.get(orders_details["sheet_name"], [])
        data_rows = rows[orders_details["header_row_index"] + 1 :] if rows else []
        orders_rows, orders_meta = normalize_orders(
            Path(orders_details["file_path"]),
            orders_details["sheet_name"],
            orders_details["header_row_index"],
            orders_details["detected_columns"],
            data_rows,
            target_fsns,
        )
        orders_payload = orders_meta["summary"]
        order_lookup = orders_meta["order_lookup"]
    else:
        order_lookup = {}
        orders_payload = {
            "file_name": "",
            "sheet_name": "",
            "header_row_index": 0,
            "raw_rows": 0,
            "orders_fsn_count": 0,
            "orders_matched_target_fsn_count": 0,
            "normalized_orders_rows": 0,
            "selected_sheet_is_help": False,
            "column_presence": {key: False for key in ORDER_REQUIRED_COLUMNS},
            "columns_detected": [],
        }

    if returns_details["file_path"] and returns_details["sheet_name"]:
        workbook_rows = read_workbook_rows(Path(returns_details["file_path"]))
        rows = workbook_rows.get(returns_details["sheet_name"], [])
        data_rows = rows[returns_details["header_row_index"] + 1 :] if rows else []
        returns_rows, returns_meta = normalize_returns(
            Path(returns_details["file_path"]),
            returns_details["sheet_name"],
            returns_details["header_row_index"],
            returns_details["detected_columns"],
            data_rows,
            target_fsns,
            order_lookup,
        )
        returns_payload = returns_meta["summary"]
    else:
        returns_payload = {
            "file_name": "",
            "sheet_name": "",
            "header_row_index": 0,
            "raw_rows": 0,
            "returns_mapped_via_order_item_id_count": 0,
            "returns_direct_fsn_hits": 0,
            "returns_matched_target_fsn_count": 0,
            "normalized_returns_rows": 0,
            "selected_sheet_is_help": False,
            "column_presence": {key: False for key in RETURN_REQUIRED_COLUMNS},
            "columns_detected": [],
        }

    write_csv(NORMALIZED_ORDERS_PATH, ORDER_HEADERS, orders_rows)
    write_csv(NORMALIZED_RETURNS_PATH, RETURN_HEADERS, returns_rows)

    result = {
        "status": "SUCCESS",
        "generated_at": datetime.now().isoformat(timespec="milliseconds"),
        "scope": "flipkart_orders_returns_only",
        "target_fsn_count": len(target_fsns),
        "normalized_orders_path": str(NORMALIZED_ORDERS_PATH),
        "normalized_returns_path": str(NORMALIZED_RETURNS_PATH),
        "normalized_orders": orders_payload,
        "normalized_returns": returns_payload,
        "selected_sheets": {
            "orders": {
                "file_name": orders_details["file_name"],
                "sheet_name": orders_details["sheet_name"],
                "selected_score": orders_details["selection_score"],
                "raw_row_count": orders_details["raw_row_count"],
                "sheet_is_help": orders_details["selected_sheet_is_help"],
            },
            "returns": {
                "file_name": returns_details["file_name"],
                "sheet_name": returns_details["sheet_name"],
                "selected_score": returns_details["selection_score"],
                "raw_row_count": returns_details["raw_row_count"],
                "sheet_is_help": returns_details["selected_sheet_is_help"],
            },
        },
    }
    return result


def main() -> None:
    try:
        print(json.dumps(verify_orders_returns(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "normalized_orders_path": str(NORMALIZED_ORDERS_PATH),
                    "normalized_returns_path": str(NORMALIZED_RETURNS_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
