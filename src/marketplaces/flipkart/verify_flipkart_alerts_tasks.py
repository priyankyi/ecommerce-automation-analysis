from __future__ import annotations

import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import normalize_text

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
TRACKER_TAB = "FLIPKART_ACTION_TRACKER"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"

TAB_NAMES = [ALERTS_TAB, TRACKER_TAB, ACTIVE_TASKS_TAB]
DISALLOWED_ACTIVE_STATUSES = {"Resolved", "Ignored"}
SEVERITY_ORDER = ("Critical", "High", "Medium", "Low")


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status != 503 or attempt == attempts:
                raise
            time.sleep(delay)
            delay *= 2


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def ensure_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return
    raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def pick_field(headers: Sequence[str], desired: str) -> Optional[str]:
    if desired in headers:
        return desired
    desired_norm = desired.lower()
    for header in headers:
        if normalize_text(header).lower() == desired_norm:
            return header
    return None


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: ("" if value is None else value) for key, value in row.items()}


def count_blank_and_duplicates(rows: Sequence[Dict[str, Any]], field_name: Optional[str]) -> Tuple[int, int]:
    if not field_name:
        return len(rows), 0
    values = [normalize_text(row.get(field_name, "")) for row in rows]
    missing_count = sum(1 for value in values if not value)
    counts = Counter(value for value in values if value)
    duplicate_count = sum(count - 1 for count in counts.values() if count > 1)
    return missing_count, duplicate_count


def ordered_distribution(rows: Sequence[Dict[str, Any]], field_name: Optional[str], preferred_order: Sequence[str]) -> Dict[str, int]:
    if not field_name:
        return {}
    counts = Counter()
    for row in rows:
        value = normalize_text(row.get(field_name, "")) or "(blank)"
        counts[value] += 1

    ordered: Dict[str, int] = {}
    for key in preferred_order:
        if key in counts:
            ordered[key] = counts.pop(key)
    for key in sorted(counts):
        ordered[key] = counts[key]
    return ordered


def sample_rows(rows: Sequence[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    return [normalize_row(dict(row)) for row in rows[:limit]]


def build_generated_sample(rows: Sequence[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    sample_fields = [
        "Alert_ID",
        "Severity",
        "Alert_Type",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Trigger_Value",
        "Threshold",
        "Suggested_Action",
        "Reason",
        "Status_Default",
    ]
    output: List[Dict[str, Any]] = []
    for row in rows:
        if normalize_text(row.get("Severity", "")) != "Critical":
            continue
        output.append({field: normalize_text(row.get(field, "")) for field in sample_fields})
        if len(output) >= limit:
            break
    return output


def build_active_sample(rows: Sequence[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    sample_fields = [
        "Alert_ID",
        "Severity",
        "Status",
        "Alert_Type",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Suggested_Action",
        "Reason",
        "Days_Open",
    ]
    return [{field: normalize_text(row.get(field, "")) for field in sample_fields} for row in rows[:limit]]


def summarize_tabs() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in TAB_NAMES:
        ensure_tab_exists(sheets_service, spreadsheet_id, tab_name)

    alert_headers, alert_rows = read_table(sheets_service, spreadsheet_id, ALERTS_TAB)
    tracker_headers, tracker_rows = read_table(sheets_service, spreadsheet_id, TRACKER_TAB)
    active_headers, active_rows = read_table(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)

    alert_id_field_alerts = pick_field(alert_headers, "Alert_ID")
    alert_id_field_tracker = pick_field(tracker_headers, "Alert_ID")
    alert_id_field_active = pick_field(active_headers, "Alert_ID")
    status_field_tracker = pick_field(tracker_headers, "Status")
    status_field_active = pick_field(active_headers, "Status")
    severity_field_alerts = pick_field(alert_headers, "Severity")
    severity_field_active = pick_field(active_headers, "Severity")

    alerts_missing, alerts_duplicates = count_blank_and_duplicates(alert_rows, alert_id_field_alerts)
    tracker_missing, tracker_duplicates = count_blank_and_duplicates(tracker_rows, alert_id_field_tracker)
    active_missing, active_duplicates = count_blank_and_duplicates(active_rows, alert_id_field_active)

    tracker_status_distribution = ordered_distribution(tracker_rows, status_field_tracker, [])
    alert_severity_distribution = ordered_distribution(alert_rows, severity_field_alerts, SEVERITY_ORDER)
    active_severity_distribution = ordered_distribution(active_rows, severity_field_active, SEVERITY_ORDER)

    active_status_values = [normalize_text(row.get(status_field_active, "")) if status_field_active else "" for row in active_rows]
    active_status_has_disallowed = any(status in DISALLOWED_ACTIVE_STATUSES for status in active_status_values)
    active_status_distribution = Counter(status_field_active and normalize_text(row.get(status_field_active, "")) or "(blank)" for row in active_rows)
    active_status_distribution = dict(active_status_distribution)

    active_rows_without_disallowed = [row for row in active_rows if normalize_text(row.get(status_field_active, "")) not in DISALLOWED_ACTIVE_STATUSES] if status_field_active else list(active_rows)
    active_tasks_match = len(active_rows) == len(active_rows_without_disallowed) and not active_status_has_disallowed

    generated_count = len(alert_rows)
    tracker_count = len(tracker_rows)
    active_count = len(active_rows)

    checks = {
        "generated_rows_present": generated_count > 0,
        "tracker_rows_at_least_generated": tracker_count >= generated_count,
        "active_tasks_exclude_resolved_ignored": active_tasks_match,
        "alert_ids_present_everywhere": (alerts_missing + tracker_missing + active_missing) == 0,
        "generated_alert_duplicates_zero": alerts_duplicates == 0,
        "no_duplicate_alert_ids_in_tracker": tracker_duplicates == 0,
        "no_duplicate_alert_ids_in_active_tasks": active_duplicates == 0,
    }

    status = "PASS" if all(checks.values()) else "FAIL"

    return {
        "status": status,
        "alerts_generated_rows": generated_count,
        "action_tracker_rows": tracker_count,
        "active_tasks_rows": active_count,
        "alert_severity_distribution": alert_severity_distribution,
        "active_task_severity_distribution": active_severity_distribution,
        "action_tracker_status_distribution": active_status_distribution,
        "duplicate_alert_id_count": {
            ALERTS_TAB: alerts_duplicates,
            TRACKER_TAB: tracker_duplicates,
            ACTIVE_TASKS_TAB: active_duplicates,
        },
        "missing_alert_id_count": alerts_missing + tracker_missing + active_missing,
        "sample_critical_alerts": build_generated_sample(alert_rows, limit=5),
        "sample_active_tasks": build_active_sample(active_rows, limit=5),
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        print(json.dumps(summarize_tabs(), indent=2, ensure_ascii=False))
    except Exception as exc:
        error_payload = {
            "status": "ERROR",
            "error_type": exc.__class__.__name__,
            "message": str(exc),
        }
        print(json.dumps(error_payload, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
