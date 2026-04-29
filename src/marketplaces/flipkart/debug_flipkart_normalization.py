from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_utils import (
    ANALYSIS_JSON_PATH,
    LOG_DIR,
    NORMALIZED_ORDERS_PATH,
    NORMALIZATION_STATE_PATH,
    OUTPUT_DIR,
    RAW_INPUT_DIR,
    TARGET_FSN_PATH,
    append_csv_log,
    build_status_payload,
    csv_data_row_count,
    ensure_directories,
    load_json,
    load_synonyms,
    normalize_text,
    load_report_patterns,
    now_iso,
    list_input_files,
    read_workbook_rows,
    save_json,
    select_best_sheet_across_files,
)

DEBUG_REPORT_PATH = OUTPUT_DIR / "debug_normalization_report.json"
DEBUG_LOG_PATH = LOG_DIR / "debug_normalization_log.csv"
DEBUG_LOG_HEADERS = [
    "timestamp",
    "report_type",
    "file_name",
    "sheet_name",
    "status",
    "raw_rows",
    "rows_with_fsn",
    "rows_with_order_item_id",
    "rows_with_order_id",
    "rows_with_sku",
    "rows_after_target_filter",
    "reason_if_zero",
    "message",
]

TARGET_TYPES = ["orders", "returns", "settlements", "pnl", "sales_tax"]


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_target_fsns() -> List[str]:
    if not TARGET_FSN_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {TARGET_FSN_PATH}")
    rows = read_csv_rows(TARGET_FSN_PATH)
    return [normalize_text(row.get("FSN", "")) for row in rows if normalize_text(row.get("FSN", ""))]


def load_normalized_orders_lookup() -> Dict[str, str]:
    if not NORMALIZED_ORDERS_PATH.exists():
        return {}
    rows = read_csv_rows(NORMALIZED_ORDERS_PATH)
    lookup: Dict[str, str] = {}
    for row in rows:
        fsn = normalize_text(row.get("FSN", ""))
        order_item_id = normalize_text(row.get("Order_Item_ID", ""))
        order_id = normalize_text(row.get("Order_ID", ""))
        if fsn and order_item_id:
            lookup.setdefault(order_item_id, fsn)
        if fsn and order_id:
            lookup.setdefault(order_id, fsn)
    return lookup


def find_analysis_files(analysis: Dict[str, Any], report_type: str) -> List[Dict[str, Any]]:
    return [file_info for file_info in analysis.get("files", []) if file_info.get("report_type") == report_type]


def report_file_names(analysis: Dict[str, Any], report_type: str) -> List[str]:
    return sorted({file_info.get("file_name", "") for file_info in find_analysis_files(analysis, report_type) if file_info.get("file_name")})


def row_non_empty_values(row: Sequence[Any]) -> List[str]:
    return [normalize_text(cell) for cell in row if normalize_text(cell)]


def sample_non_empty_rows(rows: Sequence[Sequence[Any]], limit: int = 5) -> List[List[str]]:
    samples: List[List[str]] = []
    for row in rows:
        values = row_non_empty_values(row)
        if values:
            samples.append(values)
        if len(samples) >= limit:
            break
    return samples


def count_column_values(rows: Sequence[Sequence[Any]], columns: Dict[str, Dict[str, Any]], key: str) -> int:
    column = columns.get(key)
    if not column:
        return 0
    index = int(column["index"])
    total = 0
    for row in rows:
        if index < len(row) and normalize_text(row[index]):
            total += 1
    return total


def collect_column_values(rows: Sequence[Sequence[Any]], columns: Dict[str, Dict[str, Any]], key: str, limit: int = 5) -> List[str]:
    column = columns.get(key)
    if not column:
        return []
    index = int(column["index"])
    values: List[str] = []
    for row in rows:
        if index < len(row):
            value = normalize_text(row[index])
            if value:
                values.append(value)
        if len(values) >= limit:
            break
    return values


def print_selection(report_type: str, selection: Dict[str, Any]) -> None:
    selected = selection.get("selected_sheet")
    payload: Dict[str, Any] = {
        "report_type": report_type,
        "selected_sheet": selected.get("sheet_name", "") if selected else "",
        "selected_score": selected.get("selection_score", 0.0) if selected else 0.0,
        "header_row_index": selected.get("header_row_index", "") if selected else "",
        "columns": sorted(selected.get("detected_columns", {}).keys()) if selected else [],
        "raw_row_count": selected.get("raw_row_count", 0) if selected else 0,
        "normalized_row_count": selected.get("non_empty_row_count", 0) if selected else 0,
        "rejected_sheets": [
            {
                "sheet_name": item.get("sheet_name", ""),
                "reason": item.get("exclusion_reason", ""),
                "score": item.get("selection_score", 0.0),
            }
            for item in selection.get("rejected_sheets", [])
        ],
    }
    print(json.dumps(payload, ensure_ascii=False))


def filter_target_rows(rows: Sequence[Sequence[Any]], columns: Dict[str, Dict[str, Any]], target_fsns: Iterable[str]) -> List[Sequence[Any]]:
    fsn_column = columns.get("fsn")
    if not fsn_column:
        return []
    index = int(fsn_column["index"])
    target_set = set(target_fsns)
    return [row for row in rows if index < len(row) and normalize_text(row[index]) in target_set]


def build_reason(report_type: str, raw_rows: int, rows_with_fsn: int, rows_after_target_filter: int, has_normalized_orders: bool) -> str:
    if raw_rows == 0:
        return "no data rows after the header"
    if rows_after_target_filter > 0:
        return ""
    if report_type == "returns" and not has_normalized_orders:
        return "normalized_orders is empty, so return mapping cannot run yet"
    if rows_with_fsn == 0:
        return "FSN column is blank or missing in the detected data rows"
    return "FSNs were found, but none matched the target FSN list"


def diagnose_sheet(
    report_type: str,
    file_name: str,
    selection: Dict[str, Any],
    target_fsns: List[str],
    normalized_order_lookup: Dict[str, str],
) -> Dict[str, Any]:
    selected = selection.get("selected_sheet")
    if not selected:
        return {
            "report_type": report_type,
            "file_name": file_name,
            "sheet_name": "",
            "header_row_index": 0,
            "raw_rows": 0,
            "rows_with_fsn": 0,
            "rows_with_order_item_id": 0,
            "rows_with_order_id": 0,
            "rows_with_sku": 0,
            "rows_after_target_filter": 0,
            "reason_if_zero": "No usable sheet selected",
            "columns": {"all_detected": {}, "fsn": {}, "sku_id": {}, "order_id": {}, "order_item_id": {}},
            "sample_rows": [],
            "sample_detected_values": {"fsn": [], "sku_id": [], "order_id": [], "order_item_id": []},
            "status": "ZERO",
        }

    file_path = RAW_INPUT_DIR / file_name
    workbook_rows = read_workbook_rows(file_path)
    sheet_name = selected.get("sheet_name", "")
    rows = workbook_rows.get(sheet_name, [])
    header_row_index = int(selected.get("header_row_index", 0))
    detected_columns = selected.get("detected_columns", {})
    data_rows = rows[header_row_index + 1 :] if rows else []

    rows_with_fsn = count_column_values(data_rows, detected_columns, "fsn")
    rows_with_order_item_id = count_column_values(data_rows, detected_columns, "order_item_id")
    rows_with_order_id = count_column_values(data_rows, detected_columns, "order_id")
    rows_with_sku = count_column_values(data_rows, detected_columns, "sku_id")

    if report_type == "orders":
        filtered_rows = filter_target_rows(data_rows, detected_columns, target_fsns)
        reason = build_reason(report_type, len(data_rows), rows_with_fsn, len(filtered_rows), bool(normalized_order_lookup))
    elif report_type == "returns":
        if not normalized_order_lookup:
            filtered_rows = []
            reason = "normalized_orders is empty, so return mapping cannot run yet"
        else:
            filtered_rows = []
            order_item_col = detected_columns.get("order_item_id")
            order_id_col = detected_columns.get("order_id")
            for row in data_rows:
                candidates: List[str] = []
                if order_item_col and int(order_item_col["index"]) < len(row):
                    value = normalize_text(row[int(order_item_col["index"])])
                    if value and value in normalized_order_lookup:
                        candidates.append(normalized_order_lookup[value])
                if order_id_col and int(order_id_col["index"]) < len(row):
                    value = normalize_text(row[int(order_id_col["index"])])
                    if value and value in normalized_order_lookup:
                        candidates.append(normalized_order_lookup[value])
                if candidates:
                    fsn = candidates[0]
                    if fsn in target_fsns:
                        filtered_rows.append(row)
            reason = build_reason(report_type, len(data_rows), rows_with_fsn, len(filtered_rows), bool(normalized_order_lookup))
    elif report_type in {"settlements", "pnl", "sales_tax"}:
        filtered_rows = filter_target_rows(data_rows, detected_columns, target_fsns)
        reason = build_reason(report_type, len(data_rows), rows_with_fsn, len(filtered_rows), bool(normalized_order_lookup))
    else:
        filtered_rows = []
        reason = "unsupported report type for debug"

    return {
        "report_type": report_type,
        "file_name": file_name,
        "sheet_name": sheet_name,
        "header_row_index": header_row_index,
        "raw_rows": len(data_rows),
        "rows_with_fsn": rows_with_fsn,
        "rows_with_order_item_id": rows_with_order_item_id,
        "rows_with_order_id": rows_with_order_id,
        "rows_with_sku": rows_with_sku,
        "rows_after_target_filter": len(filtered_rows),
        "reason_if_zero": reason,
        "columns": {
            "all_detected": detected_columns,
            "fsn": selected.get("detected_columns", {}).get("fsn", {}),
            "sku_id": selected.get("detected_columns", {}).get("sku_id", {}),
            "order_id": selected.get("detected_columns", {}).get("order_id", {}),
            "order_item_id": selected.get("detected_columns", {}).get("order_item_id", {}),
        },
        "sample_rows": sample_non_empty_rows(data_rows),
        "sample_detected_values": {
            "fsn": collect_column_values(data_rows, detected_columns, "fsn"),
            "sku_id": collect_column_values(data_rows, detected_columns, "sku_id"),
            "order_id": collect_column_values(data_rows, detected_columns, "order_id"),
            "order_item_id": collect_column_values(data_rows, detected_columns, "order_item_id"),
        },
        "status": "ZERO" if len(filtered_rows) == 0 else "OK",
    }


def aggregate_health(report_type: str, sheet_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    raw_rows = sum(row["raw_rows"] for row in sheet_rows)
    rows_with_fsn = sum(row["rows_with_fsn"] for row in sheet_rows)
    rows_with_order_item_id = sum(row["rows_with_order_item_id"] for row in sheet_rows)
    rows_with_order_id = sum(row["rows_with_order_id"] for row in sheet_rows)
    rows_with_sku = sum(row["rows_with_sku"] for row in sheet_rows)
    rows_after_target_filter = sum(row["rows_after_target_filter"] for row in sheet_rows)
    reasons = sorted({row["reason_if_zero"] for row in sheet_rows if row["reason_if_zero"]})
    return {
        "report_type": report_type,
        "raw_rows": raw_rows,
        "rows_with_fsn": rows_with_fsn,
        "rows_with_order_item_id": rows_with_order_item_id,
        "rows_with_order_id": rows_with_order_id,
        "rows_with_sku": rows_with_sku,
        "rows_after_target_filter": rows_after_target_filter,
        "reason_if_zero": "; ".join(reasons),
    }


def debug_flipkart_normalization() -> Dict[str, Any]:
    ensure_directories()
    if not ANALYSIS_JSON_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {ANALYSIS_JSON_PATH}")

    analysis = load_json(ANALYSIS_JSON_PATH)
    synonyms = load_synonyms()
    patterns = load_report_patterns()
    target_fsns = load_target_fsns()
    normalized_order_lookup = load_normalized_orders_lookup()
    report_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    log_rows: List[Dict[str, Any]] = []
    raw_files = list_input_files(RAW_INPUT_DIR)

    for report_type in TARGET_TYPES:
        selection = select_best_sheet_across_files(raw_files, report_type, synonyms, patterns)
        print_selection(report_type, selection)
        selected = selection.get("selected_sheet")
        if not selected:
            log_rows.append(
                {
                    "timestamp": now_iso(),
                    "report_type": report_type,
                    "file_name": "",
                    "sheet_name": "",
                    "status": "EMPTY",
                    "raw_rows": 0,
                    "rows_with_fsn": 0,
                    "rows_with_order_item_id": 0,
                    "rows_with_order_id": 0,
                    "rows_with_sku": 0,
                    "rows_after_target_filter": 0,
                    "reason_if_zero": "no usable sheet selected",
                    "message": "missing classification",
                }
            )
            continue

        diagnosis = diagnose_sheet(report_type, selected.get("file_name", ""), selection, target_fsns, normalized_order_lookup)
        report_rows[report_type].append(diagnosis)
        log_rows.append(
            {
                "timestamp": now_iso(),
                "report_type": report_type,
                "file_name": diagnosis["file_name"],
                "sheet_name": diagnosis["sheet_name"],
                "status": diagnosis["status"],
                "raw_rows": diagnosis["raw_rows"],
                "rows_with_fsn": diagnosis["rows_with_fsn"],
                "rows_with_order_item_id": diagnosis["rows_with_order_item_id"],
                "rows_with_order_id": diagnosis["rows_with_order_id"],
                "rows_with_sku": diagnosis["rows_with_sku"],
                "rows_after_target_filter": diagnosis["rows_after_target_filter"],
                "reason_if_zero": diagnosis["reason_if_zero"],
                "message": "diagnosed",
            }
        )

    health_summary = [aggregate_health(report_type, report_rows.get(report_type, [])) for report_type in TARGET_TYPES]
    report = {
        "generated_at": now_iso(),
        "analysis_path": str(ANALYSIS_JSON_PATH),
        "normalization_state_path": str(NORMALIZATION_STATE_PATH),
        "target_fsn_count": len(target_fsns),
        "normalized_orders_exists": NORMALIZED_ORDERS_PATH.exists(),
        "normalized_orders_rows": csv_data_row_count(NORMALIZED_ORDERS_PATH) if NORMALIZED_ORDERS_PATH.exists() else 0,
        "report_type_diagnostics": report_rows,
        "health_summary": health_summary,
    }

    save_json(DEBUG_REPORT_PATH, report)
    append_csv_log(DEBUG_LOG_PATH, DEBUG_LOG_HEADERS, log_rows)

    print(
        json.dumps(
            build_status_payload(
                "SUCCESS",
                debug_report_path=str(DEBUG_REPORT_PATH),
                debug_log_path=str(DEBUG_LOG_PATH),
                target_fsn_count=len(target_fsns),
                report_types=len(TARGET_TYPES),
                health_summary=health_summary,
            ),
            indent=2,
        )
    )
    return report


def main() -> None:
    try:
        debug_flipkart_normalization()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "debug_report_path": str(DEBUG_REPORT_PATH),
                    "debug_log_path": str(DEBUG_LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
