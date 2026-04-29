from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_utils import (
    ANALYSIS_JSON_PATH,
    FSN_BRIDGE_LOG_PATH,
    FSN_BRIDGE_PATH,
    RAW_INPUT_DIR,
    OUTPUT_DIR,
    TARGET_FSN_PATH,
    TARGET_MASTER_FSN_FILENAME,
    PIPELINE_STATUS_PATH,
    ensure_directories,
    format_decimal,
    highest_priority_fsn,
    is_blank,
    list_input_files,
    load_json,
    load_report_patterns,
    load_synonyms,
    merge_non_blank,
    clean_fsn,
    normalize_text,
    now_iso,
    read_workbook_rows,
    save_json,
    save_run_state,
    list_input_files,
    select_best_sheet_across_files,
    write_csv,
    append_csv_log,
    dedupe_dict_rows,
    build_status_payload,
)

TARGET_HEADERS = ["FSN", "SKU_ID", "Product_Title", "Priority", "Remarks", "Source_File"]
BRIDGE_HEADERS = ["FSN", "Seller_SKU", "Product_Title", "Listing_ID", "Category", "Source_Files", "Mapping_Confidence", "Mapping_Issue"]
BRIDGE_LOG_HEADERS = ["timestamp", "stage", "source_file", "sheet_name", "rows_read", "rows_written", "status", "message"]


def find_master_file() -> Path:
    candidates = list_input_files(RAW_INPUT_DIR)
    for path in candidates:
        if path.name == TARGET_MASTER_FSN_FILENAME:
            return path
    raise FileNotFoundError(f"Missing required file: {RAW_INPUT_DIR / TARGET_MASTER_FSN_FILENAME}")


def choose_best_header(rows: List[List[Any]], synonyms: Dict[str, List[str]]) -> Tuple[int, Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    from src.marketplaces.flipkart.flipkart_utils import detect_header_row

    return detect_header_row(rows, synonyms)


def report_file_names(analysis: Dict[str, Any], report_type: str) -> List[str]:
    return sorted({file_info.get("file_name", "") for file_info in analysis.get("files", []) if file_info.get("report_type") == report_type and file_info.get("file_name")})


def extract_target_fsns(master_file: Path, synonyms: Dict[str, List[str]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    workbook_rows = read_workbook_rows(master_file)
    if not workbook_rows:
        raise RuntimeError(f"Unable to read workbook sheets from {master_file}")

    target_rows: List[Dict[str, Any]] = []
    logs: List[Dict[str, Any]] = []
    seen_fsns: set[str] = set()
    fsn_detected_any = False

    for sheet_name, rows in workbook_rows.items():
        if not rows:
            continue
        header_row_index, detected_columns, candidates = choose_best_header(rows, synonyms)
        fsn_column = detected_columns.get("fsn")
        if not fsn_column:
            logs.append(
                {
                    "timestamp": now_iso(),
                    "stage": "target_fsn_extract",
                    "source_file": master_file.name,
                    "sheet_name": sheet_name,
                    "rows_read": max(0, len(rows) - (header_row_index + 1)),
                    "rows_written": 0,
                    "status": "WARNING",
                    "message": f"FSN column not detected. Columns found: {', '.join([str(c) for c in rows[header_row_index][:20]])}",
                }
            )
            continue
        fsn_detected_any = True
        sku_col = detected_columns.get("sku_id")
        title_col = detected_columns.get("product_title")
        priority_col = detected_columns.get("priority")
        remarks_col = detected_columns.get("remarks")

        data_rows = rows[header_row_index + 1 :]
        written = 0
        for row in data_rows:
            fsn = clean_fsn(row[fsn_column["index"]]) if fsn_column["index"] < len(row) else ""
            if not fsn or fsn in seen_fsns:
                continue
            seen_fsns.add(fsn)
            target_rows.append(
                {
                    "FSN": fsn,
                    "SKU_ID": normalize_text(row[sku_col["index"]]) if sku_col and sku_col["index"] < len(row) else "",
                    "Product_Title": normalize_text(row[title_col["index"]]) if title_col and title_col["index"] < len(row) else "",
                    "Priority": normalize_text(row[priority_col["index"]]) if priority_col and priority_col["index"] < len(row) else "",
                    "Remarks": normalize_text(row[remarks_col["index"]]) if remarks_col and remarks_col["index"] < len(row) else "",
                    "Source_File": master_file.name,
                }
            )
            written += 1

        logs.append(
            {
                "timestamp": now_iso(),
                "stage": "target_fsn_extract",
                "source_file": master_file.name,
                "sheet_name": sheet_name,
                "rows_read": len(data_rows),
                "rows_written": written,
                "status": "SUCCESS",
                "message": f"Extracted target FSNs from header row {header_row_index}",
            }
        )

    if not fsn_detected_any:
        raise RuntimeError(f"FSN column not detected in {master_file.name}. Sheets scanned: {list(workbook_rows.keys())}")

    target_rows = dedupe_dict_rows(target_rows, "FSN")
    return target_rows, logs


def build_bridge(master_targets: List[Dict[str, Any]], analysis: Dict[str, Any], synonyms: Dict[str, List[str]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fsn_map: Dict[str, Dict[str, Any]] = {clean_fsn(row["FSN"]): dict(row) for row in master_targets if clean_fsn(row.get("FSN"))}
    priority_map = {row["FSN"]: row for row in master_targets}
    bridge: Dict[str, Dict[str, Any]] = {
        clean_fsn(row["FSN"]): {
            "FSN": clean_fsn(row["FSN"]),
            "Seller_SKU": row.get("SKU_ID", ""),
            "Product_Title": row.get("Product_Title", ""),
            "Listing_ID": "",
            "Category": "",
            "Source_Files": row.get("Source_File", ""),
            "Mapping_Confidence": "REQUIRED",
            "Mapping_Issue": "",
        }
        for row in master_targets
        if clean_fsn(row.get("FSN"))
    }
    source_files: Dict[str, set[str]] = defaultdict(set)
    logs: List[Dict[str, Any]] = []
    patterns = load_report_patterns()
    raw_files = list_input_files(RAW_INPUT_DIR)

    for report_type in ["listing", "orders", "sales_tax"]:
        selection = select_best_sheet_across_files(raw_files, report_type, synonyms, patterns)
        selected = selection.get("selected_sheet")
        if not selected:
            logs.append(
                {
                    "timestamp": now_iso(),
                    "stage": "bridge",
                    "source_file": "",
                    "sheet_name": "",
                    "rows_read": 0,
                    "rows_written": 0,
                    "status": "EMPTY",
                    "message": f"No usable {report_type} sheet found",
                }
            )
            continue

        file_name = selected.get("file_name", "")
        file_path = Path(selected.get("file_path", RAW_INPUT_DIR / file_name))
        sheet_name = selected.get("sheet_name", "")
        workbook_rows = read_workbook_rows(file_path)
        rows = workbook_rows.get(sheet_name, [])
        header_row_index = int(selected.get("header_row_index", 0))
        detected_columns = selected.get("detected_columns", {})
        fsn_col = detected_columns.get("fsn")
        sku_col = detected_columns.get("sku_id")
        title_col = detected_columns.get("product_title")
        listing_id_col = detected_columns.get("listing_id")
        category_col = detected_columns.get("category")

        source_files[file_name].add(sheet_name)
        data_rows = rows[header_row_index + 1 :]
        written = 0
        for row in data_rows:
            fsn = clean_fsn(row[fsn_col["index"]]) if fsn_col and fsn_col["index"] < len(row) else ""
            sku = normalize_text(row[sku_col["index"]]) if sku_col and sku_col["index"] < len(row) else ""
            title = normalize_text(row[title_col["index"]]) if title_col and title_col["index"] < len(row) else ""
            listing_id = normalize_text(row[listing_id_col["index"]]) if listing_id_col and listing_id_col["index"] < len(row) else ""
            category = normalize_text(row[category_col["index"]]) if category_col and category_col["index"] < len(row) else ""

            if fsn and fsn in bridge:
                existing = bridge[fsn]
                merge_non_blank(existing, {"Seller_SKU": sku, "Product_Title": title, "Listing_ID": listing_id, "Category": category}, ["Seller_SKU", "Product_Title", "Listing_ID", "Category"])
                existing["Source_Files"] = ", ".join(sorted(set(filter(None, [existing.get("Source_Files", ""), file_name]))))
                existing["Mapping_Confidence"] = "HIGH"
                written += 1
                continue

            if not sku:
                continue
            matching_fsns = [clean_fsn(target["FSN"]) for target in master_targets if normalize_text(target.get("SKU_ID")) == sku and clean_fsn(target.get("FSN"))]
            if not matching_fsns:
                continue
            if len(matching_fsns) == 1:
                fsn = matching_fsns[0]
                existing = bridge[fsn]
                merge_non_blank(existing, {"Seller_SKU": sku, "Product_Title": title, "Listing_ID": listing_id, "Category": category}, ["Seller_SKU", "Product_Title", "Listing_ID", "Category"])
                existing["Source_Files"] = ", ".join(sorted(set(filter(None, [existing.get("Source_Files", ""), file_name]))))
                existing["Mapping_Confidence"] = "MEDIUM"
                existing["Mapping_Issue"] = "SKU fallback"
                written += 1
            else:
                chosen = highest_priority_fsn(matching_fsns, priority_map)
                if chosen and chosen in bridge:
                    existing = bridge[chosen]
                    merge_non_blank(existing, {"Seller_SKU": sku, "Product_Title": title, "Listing_ID": listing_id, "Category": category}, ["Seller_SKU", "Product_Title", "Listing_ID", "Category"])
                    existing["Source_Files"] = ", ".join(sorted(set(filter(None, [existing.get("Source_Files", ""), file_name]))))
                    existing["Mapping_Confidence"] = "LOW"
                    existing["Mapping_Issue"] = "SKU maps to multiple FSNs"
                    written += 1

        logs.append(
            {
                "timestamp": now_iso(),
                "stage": "bridge",
                "source_file": file_name,
                "sheet_name": sheet_name,
                "rows_read": len(data_rows),
                "rows_written": written,
                "status": "SUCCESS",
                "message": report_type,
            }
        )

    bridge_rows = [bridge[fsn] for fsn in sorted(bridge)]
    return bridge_rows, logs


def build_flipkart_fsn_bridge() -> Dict[str, Any]:
    ensure_directories()
    synonyms = load_synonyms()
    analysis = load_json(ANALYSIS_JSON_PATH) if ANALYSIS_JSON_PATH.exists() else {"files": []}

    master_file = find_master_file()
    target_rows, target_logs = extract_target_fsns(master_file, synonyms)
    write_csv(TARGET_FSN_PATH, TARGET_HEADERS, target_rows)

    bridge_rows, bridge_logs = build_bridge(target_rows, analysis, synonyms)
    write_csv(FSN_BRIDGE_PATH, BRIDGE_HEADERS, bridge_rows)

    append_csv_log(FSN_BRIDGE_LOG_PATH, BRIDGE_LOG_HEADERS, target_logs + bridge_logs)

    result = {
        "status": "ok",
        "generated_at": now_iso(),
        "target_master_file": str(master_file),
        "target_fsns_path": str(TARGET_FSN_PATH),
        "bridge_path": str(FSN_BRIDGE_PATH),
        "target_fsn_count": len(target_rows),
        "bridge_row_count": len(bridge_rows),
        "logs_path": str(FSN_BRIDGE_LOG_PATH),
    }
    save_json(OUTPUT_DIR / "flipkart_target_fsn_summary.json", result)
    save_run_state(
        PIPELINE_STATUS_PATH,
        {
            "status": "SUCCESS",
            "stage": "bridge",
            "bridge_generated_at": result["generated_at"],
            "bridge_mtime": FSN_BRIDGE_PATH.stat().st_mtime,
            "target_fsns_mtime": TARGET_FSN_PATH.stat().st_mtime,
        },
    )
    payload = dict(result)
    payload.pop("status", None)
    print(json.dumps(build_status_payload("SUCCESS", **payload), indent=2))
    return result


def main() -> None:
    try:
        build_flipkart_fsn_bridge()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "target_fsns_path": str(TARGET_FSN_PATH),
                    "bridge_path": str(FSN_BRIDGE_PATH),
                    "log_path": str(FSN_BRIDGE_LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
