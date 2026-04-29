from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

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
    write_rows,
)
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, RAW_INPUT_DIR, OUTPUT_DIR, append_csv_log, ensure_directories, now_iso, save_json
from src.marketplaces.flipkart.report_format_monitor_utils import scan_raw_report_files

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
BASELINE_PATH = OUTPUT_DIR / "flipkart_report_format_baseline.json"
BASELINE_TAB = "FLIPKART_REPORT_FORMAT_BASELINE"
LOG_PATH = LOG_DIR / "flipkart_report_format_baseline_log.csv"

BASELINE_HEADERS = [
    "File_Name",
    "File_Extension",
    "Sheet_Name",
    "Detected_Report_Type",
    "Sheet_Class",
    "Effective_Data_Rows",
    "Header_Detection_Status",
    "Required_Business_Headers_Present",
    "Row_Count",
    "Column_Count",
    "Header_Row_Index",
    "Headers",
    "Normalized_Headers",
    "Sample_First_Data_Row_Hash",
    "File_Modified_Time",
    "Baseline_Created_At",
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


def write_baseline_tab(sheets_service, spreadsheet_id: str, rows: List[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, BASELINE_TAB)
    clear_tab(sheets_service, spreadsheet_id, BASELINE_TAB)
    write_rows(sheets_service, spreadsheet_id, BASELINE_TAB, BASELINE_HEADERS, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(BASELINE_HEADERS))
    add_basic_filter(sheets_service, spreadsheet_id, sheet_id, len(BASELINE_HEADERS), len(rows) + 1)


def create_flipkart_report_format_baseline() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    baseline_created_at = now_iso()
    baseline_entries, files_scanned, sheets_scanned = scan_raw_report_files(RAW_INPUT_DIR, baseline_created_at=baseline_created_at)
    tab_rows = [
        {
            "File_Name": entry.get("file_name", ""),
            "File_Extension": entry.get("file_extension", ""),
            "Sheet_Name": entry.get("sheet_name", ""),
            "Detected_Report_Type": entry.get("detected_report_type", ""),
            "Sheet_Class": entry.get("sheet_class", ""),
            "Effective_Data_Rows": entry.get("effective_data_rows", ""),
            "Header_Detection_Status": entry.get("header_detection_status", ""),
            "Required_Business_Headers_Present": entry.get("required_business_headers_present", ""),
            "Row_Count": entry.get("row_count", ""),
            "Column_Count": entry.get("column_count", ""),
            "Header_Row_Index": entry.get("header_row_index", ""),
            "Headers": " | ".join(entry.get("headers", [])),
            "Normalized_Headers": " | ".join(entry.get("normalized_headers", [])),
            "Sample_First_Data_Row_Hash": entry.get("sample_first_data_row_hash", ""),
            "File_Modified_Time": entry.get("file_modified_time", ""),
            "Baseline_Created_At": entry.get("baseline_created_at", ""),
        }
        for entry in baseline_entries
    ]

    save_json(
        BASELINE_PATH,
        {
            "status": "SUCCESS",
            "baseline_created_at": baseline_created_at,
            "raw_input_dir": str(RAW_INPUT_DIR),
            "files_scanned": files_scanned,
            "sheets_scanned": sheets_scanned,
            "baseline_entries": len(baseline_entries),
            "entries": baseline_entries,
        },
    )

    sheets_service, _, _ = build_services()
    write_baseline_tab(sheets_service, spreadsheet_id, tab_rows)

    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "files_scanned",
            "sheets_scanned",
            "baseline_entries",
            "baseline_path",
            "tab_updated",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "files_scanned": files_scanned,
                "sheets_scanned": sheets_scanned,
                "baseline_entries": len(baseline_entries),
                "baseline_path": str(BASELINE_PATH),
                "tab_updated": BASELINE_TAB,
                "status": "SUCCESS",
                "message": "Created or refreshed the Flipkart report format baseline",
            }
        ],
    )

    result = {
        "status": "SUCCESS",
        "files_scanned": files_scanned,
        "sheets_scanned": sheets_scanned,
        "baseline_entries": len(baseline_entries),
        "baseline_path": str(BASELINE_PATH),
        "tab_updated": BASELINE_TAB,
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_report_format_baseline()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "baseline_path": str(BASELINE_PATH),
                    "log_path": str(LOG_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
