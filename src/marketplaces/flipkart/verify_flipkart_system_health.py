from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import normalize_text

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

TABS_TO_CHECK = [
    "FLIPKART_SKU_ANALYSIS",
    "FLIPKART_COST_MASTER",
    "FLIPKART_ALERTS_GENERATED",
    "FLIPKART_ACTION_TRACKER",
    "FLIPKART_ACTIVE_TASKS",
    "FLIPKART_DASHBOARD",
    "FLIPKART_FSN_DRILLDOWN",
    "FLIPKART_RETURN_COMMENTS",
    "FLIPKART_RETURN_ISSUE_SUMMARY",
    "FLIPKART_ADS_PLANNER",
    "FLIPKART_ADS_MASTER",
    "FLIPKART_LISTING_PRESENCE",
    "FLIPKART_MISSING_ACTIVE_LISTINGS",
    "FLIPKART_RUN_HISTORY",
    "FLIPKART_FSN_HISTORY",
]


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in {429, 500, 502, 503} or attempt == attempts:
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


def ensure_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return True
    return False


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def get_sheet_values_batch(sheets_service, spreadsheet_id: str, ranges: Sequence[str]) -> Dict[str, List[List[Any]]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=list(ranges))
        .execute()
    )
    tables: Dict[str, List[List[Any]]] = {}
    for value_range in response.get("valueRanges", []):
        range_name = str(value_range.get("range", ""))
        tab_name = range_name.split("!", 1)[0]
        tables[tab_name] = value_range.get("values", [])
    return tables


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def row_count(rows: Sequence[Dict[str, Any]]) -> int:
    return len(rows)


def count_alerts(rows: Sequence[Dict[str, Any]], severity: str) -> int:
    target = normalize_text(severity)
    return sum(1 for row in rows if normalize_text(row.get("Severity", "")) == target)


def count_missing_cogs(rows: Sequence[Dict[str, Any]]) -> int:
    missing_statuses = {"", "Missing", "Needs Review"}
    return sum(1 for row in rows if normalize_text(row.get("COGS_Status", "")) in missing_statuses)


def count_ads_ready(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if normalize_text(row.get("Ads_Readiness_Status", "")) == "Ready")


def count_active_tasks(rows: Sequence[Dict[str, Any]]) -> int:
    return row_count(rows)


def verify_flipkart_system_health() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    metadata = get_metadata(sheets_service, spreadsheet_id)
    available_tabs = {
        str(sheet.get("properties", {}).get("title", ""))
        for sheet in metadata.get("sheets", [])
        if str(sheet.get("properties", {}).get("title", ""))
    }
    missing_tabs = [tab_name for tab_name in TABS_TO_CHECK if tab_name not in available_tabs]

    tables: Dict[str, Tuple[List[str], List[Dict[str, str]]]] = {}
    row_counts: Dict[str, int] = {}
    batch_tabs = [tab_name for tab_name in TABS_TO_CHECK if tab_name not in missing_tabs]
    batch_ranges = [f"{tab_name}!A1:ZZ" for tab_name in batch_tabs]
    batch_values = get_sheet_values_batch(sheets_service, spreadsheet_id, batch_ranges) if batch_ranges else {}

    for tab_name in TABS_TO_CHECK:
        if tab_name in missing_tabs:
            tables[tab_name] = ([], [])
            row_counts[tab_name] = 0
            continue
        rows = batch_values.get(tab_name, [])
        headers = [str(cell) for cell in rows[0]] if rows else []
        rows_data: List[Dict[str, str]] = []
        for row in rows[1:]:
            rows_data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
        tables[tab_name] = (headers, rows_data)
        row_counts[tab_name] = row_count(rows_data)

    sku_rows = tables["FLIPKART_SKU_ANALYSIS"][1]
    cost_rows = tables["FLIPKART_COST_MASTER"][1]
    alerts_rows = tables["FLIPKART_ALERTS_GENERATED"][1]
    tracker_rows = tables["FLIPKART_ACTION_TRACKER"][1]
    active_rows = tables["FLIPKART_ACTIVE_TASKS"][1]
    ads_planner_rows = tables["FLIPKART_ADS_PLANNER"][1]
    missing_listing_rows = tables["FLIPKART_MISSING_ACTIVE_LISTINGS"][1]
    return_issue_rows = tables["FLIPKART_RETURN_ISSUE_SUMMARY"][1]

    critical_counts = {
        "active_tasks": count_active_tasks(active_rows),
        "critical_alerts": count_alerts(alerts_rows, "Critical"),
        "missing_cogs": count_missing_cogs(cost_rows),
        "missing_active_listings": row_count(missing_listing_rows),
        "ads_ready_count": count_ads_ready(ads_planner_rows),
        "return_issue_summary_rows": row_count(return_issue_rows),
    }

    checks = {
        "all_required_tabs_present": not missing_tabs,
        "sku_analysis_has_rows": row_counts["FLIPKART_SKU_ANALYSIS"] > 0,
        "cost_master_has_rows": row_counts["FLIPKART_COST_MASTER"] > 0,
        "alerts_generated_has_rows": row_counts["FLIPKART_ALERTS_GENERATED"] > 0,
        "action_tracker_has_rows": row_counts["FLIPKART_ACTION_TRACKER"] > 0,
        "active_tasks_has_rows": row_counts["FLIPKART_ACTIVE_TASKS"] > 0,
        "dashboard_has_rows": row_counts["FLIPKART_DASHBOARD"] > 0,
        "fsn_drilldown_has_rows": row_counts["FLIPKART_FSN_DRILLDOWN"] > 0,
        "return_comments_has_rows": row_counts["FLIPKART_RETURN_COMMENTS"] > 0,
        "return_issue_summary_has_rows": row_counts["FLIPKART_RETURN_ISSUE_SUMMARY"] > 0,
        "ads_planner_has_rows": row_counts["FLIPKART_ADS_PLANNER"] > 0,
        "ads_master_has_rows": row_counts["FLIPKART_ADS_MASTER"] > 0,
        "listing_presence_has_rows": row_counts["FLIPKART_LISTING_PRESENCE"] > 0,
        "missing_active_listings_has_rows": row_counts["FLIPKART_MISSING_ACTIVE_LISTINGS"] > 0,
        "run_history_has_rows": row_counts["FLIPKART_RUN_HISTORY"] > 0,
        "fsn_history_has_rows": row_counts["FLIPKART_FSN_HISTORY"] > 0,
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "tabs_checked": TABS_TO_CHECK,
        "missing_tabs": missing_tabs,
        "row_counts": row_counts,
        "critical_counts": critical_counts,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        payload = verify_flipkart_system_health()
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        if payload["status"] != "PASS":
            raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
