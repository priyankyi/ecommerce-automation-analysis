from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import normalize_text

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"
DASHBOARD_DATA_TAB = "FLIPKART_DASHBOARD_DATA"
TOP_RETURN_ISSUES_TAB = "FLIPKART_TOP_RETURN_ISSUES"
DRILLDOWN_TAB = "FLIPKART_FSN_DRILLDOWN"

RETURN_ISSUE_ALERT_TYPES = {
    "Critical Return Issue",
    "Repeated Product Issue",
    "Return Fraud Risk",
    "Product Issue Cluster",
    "Packaging Damage Issue",
    "Product Not Working Returns",
    "Listing Expectation Mismatch",
    "Logistics Return Cluster",
    "Customer RTO Issue",
}

REQUIRED_DASHBOARD_METRICS = [
    "FSNs With Return Issue Summary",
    "Critical Return Issue FSNs",
    "Product Issue FSNs",
    "Logistics Issue FSNs",
    "Customer RTO Issue FSNs",
    "Return Fraud Risk FSNs",
    "Top Return Issue Category",
    "Total Classified Return Comments",
    "Other Return Comments Count",
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


def count_duplicates(rows: Sequence[Dict[str, Any]], field_name: Optional[str]) -> int:
    if not field_name:
        return len(rows)
    values = [normalize_text(row.get(field_name, "")) for row in rows]
    counts = Counter(value for value in values if value)
    return sum(count - 1 for count in counts.values() if count > 1)


def pick_field(headers: Sequence[str], desired: str) -> Optional[str]:
    if desired in headers:
        return desired
    desired_norm = normalize_text(desired).lower()
    for header in headers:
        if normalize_text(header).lower() == desired_norm:
            return header
    return None


def dashboard_metric_map(rows: Sequence[Dict[str, str]]) -> Dict[str, str]:
    metrics: Dict[str, str] = {}
    for row in rows:
        metric = normalize_text(row.get("Metric", ""))
        if metric:
            metrics[metric] = normalize_text(row.get("Value", ""))
    return metrics


def flattened_values(rows: Sequence[Sequence[Any]]) -> List[str]:
    values: List[str] = []
    for row in rows:
        values.extend(normalize_text(cell) for cell in row)
    return values


def main() -> None:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [ALERTS_TAB, ACTIVE_TASKS_TAB, DASHBOARD_DATA_TAB, TOP_RETURN_ISSUES_TAB, DRILLDOWN_TAB]:
        ensure_tab_exists(sheets_service, spreadsheet_id, tab_name)

    alert_headers, alert_rows = read_table(sheets_service, spreadsheet_id, ALERTS_TAB)
    active_headers, active_rows = read_table(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)
    _, dashboard_data_rows = read_table(sheets_service, spreadsheet_id, DASHBOARD_DATA_TAB)
    _, top_return_issue_rows = read_table(sheets_service, spreadsheet_id, TOP_RETURN_ISSUES_TAB)

    drilldown_values = get_sheet_values(sheets_service, spreadsheet_id, f"{DRILLDOWN_TAB}!A1:Q140")
    drilldown_flat = flattened_values(drilldown_values)

    alert_id_field = pick_field(alert_headers, "Alert_ID")
    duplicate_alert_id_count = count_duplicates(alert_rows, alert_id_field)

    return_issue_alert_count = sum(
        1
        for row in alert_rows
        if normalize_text(row.get("Source_Field", "")) == "FLIPKART_RETURN_ISSUE_SUMMARY"
        or normalize_text(row.get("Alert_Type", "")) in RETURN_ISSUE_ALERT_TYPES
    )

    dashboard_metrics = dashboard_metric_map(dashboard_data_rows)
    dashboard_return_metrics_present = all(metric in dashboard_metrics for metric in REQUIRED_DASHBOARD_METRICS)

    drilldown_return_issue_section_present = (
        "Section 8: Return Issue Intelligence" in drilldown_flat
        and "Recent Return Comments for Selected FSN" in drilldown_flat
        and "Top_Issue_Category" in drilldown_flat
        and "Suggested_Return_Action" in drilldown_flat
        and "Issue_Severity" in drilldown_flat
    )

    checks = {
        "alerts_rows_present": len(alert_rows) > 0,
        "active_tasks_rows_present": len(active_rows) > 0,
        "top_return_issues_rows_present": len(top_return_issue_rows) > 0,
        "dashboard_return_metrics_present": dashboard_return_metrics_present,
        "drilldown_return_issue_section_present": drilldown_return_issue_section_present,
        "duplicate_alert_id_count_zero": duplicate_alert_id_count == 0,
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "status": status,
        "return_issue_alert_count": return_issue_alert_count,
        "top_return_issues_rows": len(top_return_issue_rows),
        "dashboard_return_metrics_present": dashboard_return_metrics_present,
        "drilldown_return_issue_section_present": drilldown_return_issue_section_present,
        "duplicate_alert_id_count": duplicate_alert_id_count,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)
