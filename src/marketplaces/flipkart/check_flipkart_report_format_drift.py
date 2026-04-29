from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence

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
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, RAW_INPUT_DIR, append_csv_log, ensure_directories, now_iso, write_csv
from src.marketplaces.flipkart.report_format_monitor_utils import compare_entries, scan_raw_report_files, VALID_DRIFT_STATUSES, VALID_SEVERITIES

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
BASELINE_PATH = OUTPUT_DIR / "flipkart_report_format_baseline.json"
MONITOR_PATH = OUTPUT_DIR / "flipkart_report_format_monitor.csv"
ISSUES_PATH = OUTPUT_DIR / "flipkart_report_format_issues.csv"
LOOKER_PATH = OUTPUT_DIR / "looker_flipkart_report_format_monitor.csv"

MONITOR_TAB = "FLIPKART_REPORT_FORMAT_MONITOR"
ISSUES_TAB = "FLIPKART_REPORT_FORMAT_ISSUES"
LOOKER_TAB = "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR"

LOG_PATH = LOG_DIR / "flipkart_report_format_drift_log.csv"

MONITOR_HEADERS = [
    "Check_Date",
    "File_Name",
    "Sheet_Name",
    "Detected_Report_Type",
    "Sheet_Class",
    "Effective_Data_Rows",
    "Header_Detection_Status",
    "Baseline_Status",
    "Current_Row_Count",
    "Baseline_Row_Count",
    "Row_Count_Change",
    "Current_Column_Count",
    "Baseline_Column_Count",
    "Missing_Headers",
    "New_Headers",
    "Header_Change_Count",
    "Severity",
    "Drift_Status",
    "Suggested_Action",
    "Last_Updated",
]

ISSUE_HEADERS = [
    "Check_Date",
    "File_Name",
    "Sheet_Name",
    "Issue_Type",
    "Severity",
    "Issue_Detail",
    "Baseline_Value",
    "Current_Value",
    "Suggested_Action",
    "Status",
    "Owner",
    "Remarks",
    "Last_Updated",
]

LOOKER_HEADERS = [
    "Check_Date",
    "File_Name",
    "Sheet_Name",
    "Detected_Report_Type",
    "Severity",
    "Drift_Status",
    "Issue_Count",
    "Suggested_Action",
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


def write_tab(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))
    add_basic_filter(sheets_service, spreadsheet_id, sheet_id, len(headers), len(rows) + 1)


def issue_key(row: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("File_Name", "")).strip().lower(),
            str(row.get("Sheet_Name", "")).strip().lower(),
            str(row.get("Issue_Type", "")).strip().lower(),
        ]
    )


def preserve_manual_issue_fields(
    sheets_service,
    spreadsheet_id: str,
    issue_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not tab_exists(sheets_service, spreadsheet_id, ISSUES_TAB):
        for row in issue_rows:
            row.setdefault("Status", "Open")
            row.setdefault("Owner", "")
            row.setdefault("Remarks", "")
        return issue_rows

    _, existing_rows = read_table(sheets_service, spreadsheet_id, ISSUES_TAB)
    lookup = {issue_key(row): row for row in existing_rows}

    merged_rows: List[Dict[str, Any]] = []
    for row in issue_rows:
        merged = dict(row)
        existing = lookup.get(issue_key(row), {})
        for field in ("Status", "Owner", "Remarks"):
            existing_value = str(existing.get(field, "")).strip()
            if existing_value:
                merged[field] = existing_value
            elif field == "Status":
                merged.setdefault(field, "Open")
            else:
                merged.setdefault(field, "")
        merged_rows.append(merged)
    return merged_rows


def write_outputs(rows_path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    write_csv(rows_path, headers, rows)


def create_flipkart_report_format_drift() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    if not BASELINE_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {BASELINE_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    baseline_payload = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    baseline_entries = baseline_payload.get("entries", [])

    current_entries, files_checked, sheets_checked = scan_raw_report_files(RAW_INPUT_DIR)
    monitor_rows, issue_rows, looker_rows, summary = compare_entries(baseline_entries, current_entries)

    sheets_service, _, _ = build_services()
    issue_rows = preserve_manual_issue_fields(sheets_service, spreadsheet_id, issue_rows)

    write_outputs(MONITOR_PATH, MONITOR_HEADERS, monitor_rows)
    write_outputs(ISSUES_PATH, ISSUE_HEADERS, issue_rows)
    write_outputs(LOOKER_PATH, LOOKER_HEADERS, looker_rows)

    write_tab(sheets_service, spreadsheet_id, MONITOR_TAB, MONITOR_HEADERS, monitor_rows)
    write_tab(sheets_service, spreadsheet_id, ISSUES_TAB, ISSUE_HEADERS, issue_rows)
    write_tab(sheets_service, spreadsheet_id, LOOKER_TAB, LOOKER_HEADERS, looker_rows)

    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "files_checked",
            "sheets_checked",
            "ok_count",
            "minor_change_count",
            "major_change_count",
            "critical_issue_count",
            "issue_count",
            "empty_helper_ok_count",
            "data_sheet_ok_count",
            "false_positive_prevented_count",
            "baseline_path",
            "tabs_updated",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "files_checked": files_checked,
                "sheets_checked": sheets_checked,
                "ok_count": summary.get("ok_count", 0),
                "minor_change_count": summary.get("minor_change_count", 0),
                "major_change_count": summary.get("major_change_count", 0),
                "critical_issue_count": summary.get("critical_issue_count", 0),
                "issue_count": len(issue_rows),
                "empty_helper_ok_count": summary.get("empty_helper_ok_count", 0),
                "data_sheet_ok_count": summary.get("data_sheet_ok_count", 0),
                "false_positive_prevented_count": summary.get("false_positive_prevented_count", 0),
                "baseline_path": str(BASELINE_PATH),
                "tabs_updated": " | ".join([MONITOR_TAB, ISSUES_TAB, LOOKER_TAB]),
                "status": "SUCCESS",
                "message": "Completed Flipkart report format drift comparison",
            }
        ],
    )

    result = {
        "status": "SUCCESS",
        "files_checked": files_checked,
        "sheets_checked": sheets_checked,
        "ok_count": summary.get("ok_count", 0),
        "minor_change_count": summary.get("minor_change_count", 0),
        "major_change_count": summary.get("major_change_count", 0),
        "critical_issue_count": summary.get("critical_issue_count", 0),
        "issue_count": len(issue_rows),
        "empty_helper_ok_count": summary.get("empty_helper_ok_count", 0),
        "data_sheet_ok_count": summary.get("data_sheet_ok_count", 0),
        "false_positive_prevented_count": summary.get("false_positive_prevented_count", 0),
        "tabs_updated": [MONITOR_TAB, ISSUES_TAB, LOOKER_TAB],
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_report_format_drift()
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
