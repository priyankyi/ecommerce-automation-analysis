from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_utils import (
    ANALYSIS_JSON_PATH,
    CONFIG_DIR,
    REPORT_ANALYSIS_LOG_PATH,
    ensure_directories,
    infer_report_type,
    likely_columns,
    list_input_files,
    load_json,
    load_report_patterns,
    load_synonyms,
    now_iso,
    read_workbook_rows,
    save_json,
    save_run_state,
    write_csv,
    PIPELINE_STATUS_PATH,
    build_status_payload,
)

ANALYSIS_LOG_HEADERS = [
    "timestamp",
    "file_name",
    "sheet_name",
    "header_row_index",
    "row_count",
    "report_type",
    "fsn_column",
    "sku_column",
    "order_id_column",
    "order_item_id_column",
    "price_columns",
    "settlement_columns",
    "fee_columns",
    "ad_columns",
    "date_columns",
    "status",
    "message",
]


def analyze_file_sheet(file_path: Path, sheet_name: str, rows: List[List[Any]], synonyms: Dict[str, List[str]], patterns: Dict[str, Any]) -> Dict[str, Any]:
    header_row_index, detected_columns, candidates = likely_header_detection(rows, synonyms)
    report_type, type_scores = infer_report_type(file_path.name, sheet_name, detected_columns, patterns)
    column_keys = list(detected_columns.keys())
    fsn_column = detected_columns.get("fsn", {})
    sku_column = detected_columns.get("sku_id", {})
    order_id_column = detected_columns.get("order_id", {})
    order_item_column = detected_columns.get("order_item_id", {})
    price_keys = [key for key in ["mrp", "selling_price", "gross_amount", "net_settlement", "taxable_value", "total_revenue", "estimated_ad_spend"] if key in detected_columns]
    settlement_keys = [key for key in ["settlement_id", "settlement_date", "gross_amount", "commission", "fixed_fee", "collection_fee", "shipping_fee", "reverse_shipping_fee", "gst_on_fees", "tcs", "tds", "refund", "protection_fund", "adjustments", "net_settlement"] if key in detected_columns]
    fee_keys = [key for key in ["commission", "fixed_fee", "collection_fee", "shipping_fee", "reverse_shipping_fee", "gst_on_fees"] if key in detected_columns]
    ad_keys = [key for key in ["campaign_id", "campaign_name", "adgroup_id", "adgroup_name", "views", "clicks", "direct_units_sold", "indirect_units_sold", "total_revenue", "roi"] if key in detected_columns]
    date_keys = [key for key in ["order_date", "dispatch_date", "delivery_date", "return_date", "settlement_date", "invoice_date"] if key in detected_columns]

    return {
        "file_name": file_path.name,
        "sheet_name": sheet_name,
        "row_count": max(0, len(rows) - (header_row_index + 1)),
        "header_row_index": header_row_index,
        "header_row_values": ["" if cell is None else str(cell).strip() for cell in rows[header_row_index]] if rows else [],
        "report_type": report_type,
        "type_scores": type_scores,
        "detected_columns": detected_columns,
        "likely_columns": {
            "fsn": fsn_column,
            "sku_id": sku_column,
            "order_id": order_id_column,
            "order_item_id": order_item_column,
            "price_columns": price_keys,
            "settlement_columns": settlement_keys,
            "fee_columns": fee_keys,
            "ad_columns": ad_keys,
            "date_columns": date_keys,
        },
        "column_count": len(rows[header_row_index]) if rows else 0,
        "header_candidates": candidates,
        "warnings": [] if fsn_column else ["FSN column not detected"],
    }


def likely_header_detection(rows: List[List[Any]], synonyms: Dict[str, List[str]]) -> tuple[int, Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    from src.marketplaces.flipkart.flipkart_utils import detect_header_row

    return detect_header_row(rows, synonyms)


def analyze_flipkart_reports() -> Dict[str, Any]:
    ensure_directories()
    input_dir = Path(PROJECT_ROOT / "data" / "input" / "marketplaces" / "flipkart" / "raw")
    input_dir.mkdir(parents=True, exist_ok=True)

    synonyms = load_synonyms()
    patterns = load_report_patterns()
    files = list_input_files(input_dir)

    file_sheets: List[Dict[str, Any]] = []
    log_rows: List[Dict[str, Any]] = []
    report_counts: Counter[str] = Counter()

    for file_path in files:
        workbook_rows = read_workbook_rows(file_path)
        if not workbook_rows:
            message = f"No readable sheets found in {file_path.name}"
            print(message)
            log_rows.append(
                {
                    "timestamp": now_iso(),
                    "file_name": file_path.name,
                    "sheet_name": "",
                    "header_row_index": "",
                    "row_count": 0,
                    "report_type": "unknown",
                    "fsn_column": "",
                    "sku_column": "",
                    "order_id_column": "",
                    "order_item_id_column": "",
                    "price_columns": "",
                    "settlement_columns": "",
                    "fee_columns": "",
                    "ad_columns": "",
                    "date_columns": "",
                    "status": "ERROR",
                    "message": message,
                }
            )
            continue

        for sheet_name, rows in workbook_rows.items():
            if not rows:
                continue
            sheet_result = analyze_file_sheet(file_path, sheet_name, rows, synonyms, patterns)
            file_sheets.append(sheet_result)
            report_counts[sheet_result["report_type"]] += 1

            log_rows.append(
                {
                    "timestamp": now_iso(),
                    "file_name": file_path.name,
                    "sheet_name": sheet_name,
                    "header_row_index": sheet_result["header_row_index"],
                    "row_count": sheet_result["row_count"],
                    "report_type": sheet_result["report_type"],
                    "fsn_column": json.dumps(sheet_result["likely_columns"]["fsn"], ensure_ascii=False),
                    "sku_column": json.dumps(sheet_result["likely_columns"]["sku_id"], ensure_ascii=False),
                    "order_id_column": json.dumps(sheet_result["likely_columns"]["order_id"], ensure_ascii=False),
                    "order_item_id_column": json.dumps(sheet_result["likely_columns"]["order_item_id"], ensure_ascii=False),
                    "price_columns": json.dumps(sheet_result["likely_columns"]["price_columns"], ensure_ascii=False),
                    "settlement_columns": json.dumps(sheet_result["likely_columns"]["settlement_columns"], ensure_ascii=False),
                    "fee_columns": json.dumps(sheet_result["likely_columns"]["fee_columns"], ensure_ascii=False),
                    "ad_columns": json.dumps(sheet_result["likely_columns"]["ad_columns"], ensure_ascii=False),
                    "date_columns": json.dumps(sheet_result["likely_columns"]["date_columns"], ensure_ascii=False),
                    "status": "SUCCESS",
                    "message": "Analyzed",
                }
            )

    summary = {
        "generated_at": now_iso(),
        "input_dir": str(input_dir),
        "files_scanned": len(files),
        "sheets_scanned": len(file_sheets),
        "report_type_counts": dict(report_counts),
        "files": file_sheets,
    }

    save_json(ANALYSIS_JSON_PATH, summary)
    save_run_state(
        PIPELINE_STATUS_PATH,
        {
            "status": "SUCCESS",
            "stage": "analysis",
            "analysis_generated_at": summary["generated_at"],
            "analysis_mtime": ANALYSIS_JSON_PATH.stat().st_mtime,
        },
    )
    write_csv(REPORT_ANALYSIS_LOG_PATH, ANALYSIS_LOG_HEADERS, log_rows)

    print(json.dumps(build_status_payload("SUCCESS", input_dir=str(input_dir), files_scanned=len(files), sheets_scanned=len(file_sheets), analysis_path=str(ANALYSIS_JSON_PATH), log_path=str(REPORT_ANALYSIS_LOG_PATH), report_type_counts=dict(report_counts), row_counts=[item["row_count"] for item in file_sheets]), indent=2))
    return summary


def main() -> None:
    try:
        analyze_flipkart_reports()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "analysis_path": str(ANALYSIS_JSON_PATH),
                    "log_path": str(REPORT_ANALYSIS_LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
