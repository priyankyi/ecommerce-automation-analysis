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
    FSN_BRIDGE_PATH,
    LOG_DIR,
    OUTPUT_DIR,
    RAW_INPUT_DIR,
    TARGET_FSN_PATH,
    append_csv_log,
    build_status_payload,
    clean_fsn,
    ensure_directories,
    load_json,
    load_report_patterns,
    load_synonyms,
    list_input_files,
    normalize_text,
    now_iso,
    read_workbook_rows,
    select_best_sheet_across_files,
    write_csv,
)

COVERAGE_REPORT_PATH = OUTPUT_DIR / "fsn_coverage_report.csv"
COVERAGE_JSON_PATH = OUTPUT_DIR / "fsn_mismatch_samples.json"
COVERAGE_LOG_PATH = LOG_DIR / "fsn_coverage_log.csv"

COVERAGE_REPORT_HEADERS = [
    "generated_at",
    "target_fsn_count",
    "listing_fsn_count",
    "listing_match_count",
    "order_fsn_count",
    "order_target_match_count",
    "return_fsn_count",
    "return_target_match_count",
    "ads_match_count",
    "sample_target_fsns",
    "sample_listing_fsns",
    "sample_order_fsns",
    "sample_unmatched_order_fsns",
    "sample_target_fsns_missing_from_orders",
]

COVERAGE_LOG_HEADERS = [
    "timestamp",
    "report_type",
    "file_name",
    "sheet_name",
    "header_row_index",
    "raw_rows",
    "fsn_count",
    "target_match_count",
    "status",
    "columns",
    "message",
]


def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_target_fsns() -> List[str]:
    if not TARGET_FSN_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {TARGET_FSN_PATH}")
    rows = read_csv_dicts(TARGET_FSN_PATH)
    return sorted({clean_fsn(row.get("FSN", "")) for row in rows if clean_fsn(row.get("FSN", ""))})


def load_bridge_lookup() -> Dict[str, List[str]]:
    if not FSN_BRIDGE_PATH.exists():
        return {}
    rows = read_csv_dicts(FSN_BRIDGE_PATH)
    lookup: Dict[str, List[str]] = defaultdict(list)
    for row in rows:
        sku = normalize_text(row.get("Seller_SKU", ""))
        fsn = clean_fsn(row.get("FSN", ""))
        if sku and fsn:
            lookup[sku].append(fsn)
    return {sku: sorted(set(fsns)) for sku, fsns in lookup.items()}


def load_analysis() -> Dict[str, Any]:
    if not ANALYSIS_JSON_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {ANALYSIS_JSON_PATH}")
    return load_json(ANALYSIS_JSON_PATH)


def report_file_names(analysis: Dict[str, Any], report_type: str) -> List[str]:
    return sorted({file_info.get("file_name", "") for file_info in analysis.get("files", []) if file_info.get("report_type") == report_type and file_info.get("file_name")})


def unique_clean_values(values: Iterable[str], limit: int | None = None) -> List[str]:
    seen: List[str] = []
    seen_set = set()
    for value in values:
        cleaned = clean_fsn(value)
        if not cleaned or cleaned in seen_set:
            continue
        seen.append(cleaned)
        seen_set.add(cleaned)
        if limit is not None and len(seen) >= limit:
            break
    return seen


def sheet_values(
    rows: Sequence[Sequence[Any]],
    columns: Dict[str, Dict[str, Any]],
    key: str,
) -> List[str]:
    column = columns.get(key)
    if not column:
        return []
    index = int(column["index"])
    values: List[str] = []
    for row in rows:
        if index < len(row):
            value = clean_fsn(row[index])
            if value:
                values.append(value)
    return values


def collect_report_rows(
    analysis: Dict[str, Any],
    report_type: str,
    target_fsns: List[str],
    bridge_lookup: Dict[str, List[str]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    target_set = set(target_fsns)
    raw_fsns: List[str] = []
    matched_fsns: List[str] = []
    sheet_logs: List[Dict[str, Any]] = []
    match_count = 0
    synonyms = load_synonyms()
    patterns = load_report_patterns()
    raw_files = list_input_files(RAW_INPUT_DIR)
    selection = select_best_sheet_across_files(raw_files, report_type, synonyms, patterns)
    selected = selection.get("selected_sheet")
    if not selected:
        return {
            "report_type": report_type,
            "raw_fsn_count": 0,
            "target_match_count": 0,
            "mapped_match_count": 0,
            "all_raw_fsns": [],
            "sample_raw_fsns": [],
            "sample_matched_fsns": [],
        }, [], []

    file_name = selected.get("file_name", "")
    file_path = Path(selected.get("file_path", RAW_INPUT_DIR / file_name))
    workbook_rows = read_workbook_rows(file_path)
    sheet_name = selected.get("sheet_name", "")
    rows = workbook_rows.get(sheet_name, [])
    header_row_index = int(selected.get("header_row_index", 0))
    columns = selected.get("detected_columns", {})
    data_rows = rows[header_row_index + 1 :]
    fsn_column = columns.get("fsn")
    sku_column = columns.get("sku_id")
    fsn_count = 0
    target_match_count = 0
    if fsn_column:
        fsn_index = int(fsn_column["index"])
        for row in data_rows:
            fsn_value = clean_fsn(row[fsn_index]) if fsn_index < len(row) else ""
            if not fsn_value:
                continue
            fsn_count += 1
            raw_fsns.append(fsn_value)
            if fsn_value in target_set:
                target_match_count += 1
                match_count += 1
                matched_fsns.append(fsn_value)
            elif report_type == "ads" and sku_column:
                sku_index = int(sku_column["index"])
                sku_value = normalize_text(row[sku_index]) if sku_index < len(row) else ""
                mapped_candidates = [clean_fsn(fsn) for fsn in bridge_lookup.get(sku_value, []) if clean_fsn(fsn) in target_set]
                if mapped_candidates:
                    target_match_count += 1
                    match_count += 1
                    matched_fsns.append(mapped_candidates[0])
    elif report_type == "ads" and sku_column:
        sku_index = int(sku_column["index"])
        for row in data_rows:
            sku_value = normalize_text(row[sku_index]) if sku_index < len(row) else ""
            if not sku_value:
                continue
            raw_fsns.append("")
            mapped_candidates = [clean_fsn(fsn) for fsn in bridge_lookup.get(sku_value, []) if clean_fsn(fsn) in target_set]
            if mapped_candidates:
                target_match_count += 1
                match_count += 1
                matched_fsns.append(mapped_candidates[0])

    sheet_logs.append(
        {
            "timestamp": now_iso(),
            "report_type": report_type,
            "file_name": file_name,
            "sheet_name": sheet_name,
            "header_row_index": header_row_index,
            "raw_rows": len(data_rows),
            "fsn_count": fsn_count,
            "target_match_count": target_match_count,
            "status": "SUCCESS" if data_rows else "EMPTY",
            "columns": json.dumps(sorted(columns.keys()), ensure_ascii=False),
            "message": "scanned",
        }
    )

    summary = {
        "report_type": report_type,
        "raw_fsn_count": len(raw_fsns),
        "target_match_count": match_count,
        "mapped_match_count": match_count,
        "all_raw_fsns": unique_clean_values(raw_fsns),
        "sample_raw_fsns": unique_clean_values(raw_fsns, limit=10),
        "sample_matched_fsns": unique_clean_values(matched_fsns, limit=10),
    }
    return summary, sheet_logs, matched_fsns


def build_fsn_coverage_report() -> Dict[str, Any]:
    ensure_directories()
    analysis = load_analysis()
    target_fsns = load_target_fsns()
    bridge_lookup = load_bridge_lookup()

    listing_summary, listing_logs, _ = collect_report_rows(analysis, "listing", target_fsns, bridge_lookup)
    order_summary, order_logs, _ = collect_report_rows(analysis, "orders", target_fsns, bridge_lookup)
    return_summary, return_logs, _ = collect_report_rows(analysis, "returns", target_fsns, bridge_lookup)
    ads_summary, ads_logs, ads_mapped = collect_report_rows(analysis, "ads", target_fsns, bridge_lookup)

    target_set = set(target_fsns)
    order_fsns = order_summary["all_raw_fsns"]
    unmatched_order_fsns = [fsn for fsn in order_fsns if fsn not in target_set]
    target_missing_from_orders = [fsn for fsn in target_fsns if fsn not in set(order_fsns)]

    summary_row = {
        "generated_at": now_iso(),
        "target_fsn_count": len(target_fsns),
        "listing_fsn_count": listing_summary["raw_fsn_count"],
        "listing_match_count": listing_summary["target_match_count"],
        "order_fsn_count": order_summary["raw_fsn_count"],
        "order_target_match_count": order_summary["target_match_count"],
        "return_fsn_count": return_summary["raw_fsn_count"],
        "return_target_match_count": return_summary["target_match_count"],
        "ads_match_count": ads_summary["mapped_match_count"],
        "sample_target_fsns": json.dumps(target_fsns[:10], ensure_ascii=False),
        "sample_listing_fsns": json.dumps(listing_summary["sample_raw_fsns"][:10], ensure_ascii=False),
        "sample_order_fsns": json.dumps(order_fsns[:10], ensure_ascii=False),
        "sample_unmatched_order_fsns": json.dumps(unmatched_order_fsns[:10], ensure_ascii=False),
        "sample_target_fsns_missing_from_orders": json.dumps(target_missing_from_orders[:10], ensure_ascii=False),
    }

    report_path = COVERAGE_REPORT_PATH
    try:
        write_csv(report_path, COVERAGE_REPORT_HEADERS, [summary_row])
    except PermissionError:
        fallback_stamp = now_iso().replace(":", "").replace("-", "").replace("T", "_")
        report_path = COVERAGE_REPORT_PATH.with_name(f"{COVERAGE_REPORT_PATH.stem}_{fallback_stamp}{COVERAGE_REPORT_PATH.suffix}")
        write_csv(report_path, COVERAGE_REPORT_HEADERS, [summary_row])
        print("Close the existing CSV if open in Excel.")
        summary_row["report_path"] = str(report_path)

    mismatch_payload = {
        "generated_at": summary_row["generated_at"],
        "target_fsn_count": len(target_fsns),
        "target_fsns_sample": target_fsns[:10],
        "listing": {
            "raw_fsn_count": listing_summary["raw_fsn_count"],
            "match_count": listing_summary["target_match_count"],
            "sample_raw_fsns": listing_summary["sample_raw_fsns"],
        },
        "orders": {
            "raw_fsn_count": order_summary["raw_fsn_count"],
            "match_count": order_summary["target_match_count"],
            "sample_raw_fsns": order_fsns[:10],
            "sample_unmatched_fsns": unmatched_order_fsns[:10],
            "missing_from_orders": target_missing_from_orders[:10],
        },
        "returns": {
            "raw_fsn_count": return_summary["raw_fsn_count"],
            "match_count": return_summary["target_match_count"],
            "sample_raw_fsns": return_summary["sample_raw_fsns"],
        },
        "ads": {
            "mapped_match_count": ads_summary["mapped_match_count"],
            "sample_mapped_fsns": unique_clean_values(ads_mapped, limit=10),
        },
    }
    COVERAGE_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    COVERAGE_JSON_PATH.write_text(json.dumps(mismatch_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    log_rows = []
    log_rows.extend(listing_logs)
    log_rows.extend(order_logs)
    log_rows.extend(return_logs)
    log_rows.extend(ads_logs)
    append_csv_log(COVERAGE_LOG_PATH, COVERAGE_LOG_HEADERS, log_rows)

    result = {
        "status": "SUCCESS",
        "generated_at": summary_row["generated_at"],
        "report_path": str(report_path),
        "json_path": str(COVERAGE_JSON_PATH),
        "log_path": str(COVERAGE_LOG_PATH),
        "target_fsn_count": len(target_fsns),
        "listing_match_count": listing_summary["target_match_count"],
        "order_target_match_count": order_summary["target_match_count"],
        "return_target_match_count": return_summary["target_match_count"],
        "ads_match_count": ads_summary["mapped_match_count"],
    }
    payload = dict(result)
    payload.pop("status", None)
    print(json.dumps(build_status_payload("SUCCESS", **payload), indent=2))
    return mismatch_payload


def main() -> None:
    try:
        build_fsn_coverage_report()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "report_path": str(COVERAGE_REPORT_PATH),
                    "json_path": str(COVERAGE_JSON_PATH),
                    "log_path": str(COVERAGE_LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
