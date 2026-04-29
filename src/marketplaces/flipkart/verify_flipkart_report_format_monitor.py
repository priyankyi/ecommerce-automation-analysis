from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import load_json, read_table, tab_exists
from src.marketplaces.flipkart.flipkart_utils import OUTPUT_DIR, normalize_text
from src.marketplaces.flipkart.report_format_monitor_utils import VALID_DRIFT_STATUSES, VALID_SEVERITIES

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
BASELINE_PATH = OUTPUT_DIR / "flipkart_report_format_baseline.json"
MONITOR_TAB = "FLIPKART_REPORT_FORMAT_MONITOR"
ISSUES_TAB = "FLIPKART_REPORT_FORMAT_ISSUES"
LOOKER_TAB = "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR"


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


def count_non_empty_rows(rows: List[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def get_distribution(rows: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(field, ""))
        if value:
            counter[value] += 1
    return dict(counter)


def verify_flipkart_report_format_monitor() -> Dict[str, Any]:
    baseline_exists = BASELINE_PATH.exists()
    baseline_entries = 0
    if baseline_exists:
        try:
            baseline_payload = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
            baseline_entries = int(baseline_payload.get("baseline_entries", len(baseline_payload.get("entries", []))))
        except Exception:
            baseline_entries = 0
            baseline_exists = False

    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    monitor_tab_exists = tab_exists(sheets_service, spreadsheet_id, MONITOR_TAB)
    issues_tab_exists = tab_exists(sheets_service, spreadsheet_id, ISSUES_TAB)
    looker_tab_exists = tab_exists(sheets_service, spreadsheet_id, LOOKER_TAB)

    monitor_headers: List[str] = []
    monitor_rows: List[Dict[str, Any]] = []
    issue_headers: List[str] = []
    issue_rows: List[Dict[str, Any]] = []
    looker_headers: List[str] = []
    looker_rows: List[Dict[str, Any]] = []

    if monitor_tab_exists:
        monitor_headers, monitor_rows = read_table(sheets_service, spreadsheet_id, MONITOR_TAB)
    if issues_tab_exists:
        issue_headers, issue_rows = read_table(sheets_service, spreadsheet_id, ISSUES_TAB)
    if looker_tab_exists:
        looker_headers, looker_rows = read_table(sheets_service, spreadsheet_id, LOOKER_TAB)

    severity_distribution = get_distribution(monitor_rows, "Severity")
    drift_status_distribution = get_distribution(monitor_rows, "Drift_Status")

    blank_file_name_count = sum(1 for row in monitor_rows if not normalize_text(row.get("File_Name", "")))
    invalid_severity_count = sum(
        1 for row in monitor_rows if normalize_text(row.get("Severity", "")) and normalize_text(row.get("Severity", "")) not in VALID_SEVERITIES
    )
    invalid_drift_status_count = sum(
        1
        for row in monitor_rows
        if normalize_text(row.get("Drift_Status", "")) and normalize_text(row.get("Drift_Status", "")) not in VALID_DRIFT_STATUSES
    )

    issue_manual_fields_present = all(field in issue_headers for field in ("Status", "Owner", "Remarks"))
    issue_manual_fields_populated = all(
        field in row for row in issue_rows for field in ("Status", "Owner", "Remarks")
    )

    critical_issue_count = sum(1 for row in issue_rows if normalize_text(row.get("Severity", "")) == "Critical")
    high_issue_count = sum(1 for row in issue_rows if normalize_text(row.get("Severity", "")) == "High")

    checks = {
        "baseline_exists": baseline_exists,
        "monitor_tab_exists": monitor_tab_exists,
        "issues_tab_exists": issues_tab_exists,
        "looker_tab_exists": looker_tab_exists,
        "monitor_rows_positive": count_non_empty_rows(monitor_rows) > 0,
        "no_blank_file_names": blank_file_name_count == 0,
        "severity_values_valid": invalid_severity_count == 0,
        "drift_status_values_valid": invalid_drift_status_count == 0,
        "issue_manual_fields_present": issue_manual_fields_present,
        "issue_manual_fields_populated": issue_manual_fields_populated,
    }

    hard_fail = not all(checks.values())
    if hard_fail:
        status = "FAIL"
    elif critical_issue_count > 0:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"

    payload = {
        "status": status,
        "baseline_exists": baseline_exists,
        "baseline_entries": baseline_entries,
        "monitor_rows": count_non_empty_rows(monitor_rows),
        "issue_rows": count_non_empty_rows(issue_rows),
        "critical_issue_count": critical_issue_count,
        "high_issue_count": high_issue_count,
        "severity_distribution": severity_distribution,
        "drift_status_distribution": drift_status_distribution,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }
    return payload


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_report_format_monitor(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
