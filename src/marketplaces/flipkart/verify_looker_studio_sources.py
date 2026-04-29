from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.create_looker_studio_sources import (
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_EXECUTIVE_TAB,
    LOOKER_FSN_METRICS_TAB,
    LOOKER_LISTINGS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_TABS,
    SOURCE_TABS,
    SPREADSHEET_META_PATH,
    build_index,
    get_latest_run_row,
    latest_text_value,
    read_table,
    tab_exists,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text

FSN_LEVEL_TABS = [
    LOOKER_FSN_METRICS_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_LISTINGS_TAB,
]

EXPECTED_ROW_TABS = [
    LOOKER_EXECUTIVE_TAB,
    LOOKER_FSN_METRICS_TAB,
    LOOKER_ALERTS_TAB,
    LOOKER_ACTIONS_TAB,
    LOOKER_ADS_TAB,
    LOOKER_RETURNS_TAB,
    LOOKER_LISTINGS_TAB,
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


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def count_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def count_blank_fsn(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if not clean_fsn(row.get("FSN", "")))


def read_tab_row_count(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    return max(0, len(rows) - 1) if rows else 0


def verify_looker_studio_sources() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    missing_tabs = [tab_name for tab_name in LOOKER_TABS if not tab_exists(sheets_service, spreadsheet_id, tab_name)]
    tabs_checked = LOOKER_TABS + SOURCE_TABS

    source_data = {}
    for tab_name in SOURCE_TABS:
        source_data[tab_name] = read_tab_row_count(sheets_service, spreadsheet_id, tab_name)

    looker_tables = {}
    for tab_name in LOOKER_TABS:
        _, rows = read_table(sheets_service, spreadsheet_id, tab_name)
        looker_tables[tab_name] = rows

    executive_rows = looker_tables[LOOKER_EXECUTIVE_TAB]
    fsn_rows = looker_tables[LOOKER_FSN_METRICS_TAB]
    alert_rows = looker_tables[LOOKER_ALERTS_TAB]
    action_rows = looker_tables[LOOKER_ACTIONS_TAB]
    ads_rows = looker_tables[LOOKER_ADS_TAB]
    return_rows = looker_tables[LOOKER_RETURNS_TAB]
    listing_rows = looker_tables[LOOKER_LISTINGS_TAB]

    blank_fsn_counts = {
        LOOKER_FSN_METRICS_TAB: count_blank_fsn(fsn_rows),
        LOOKER_ALERTS_TAB: count_blank_fsn(alert_rows),
        LOOKER_ACTIONS_TAB: count_blank_fsn(action_rows),
        LOOKER_ADS_TAB: count_blank_fsn(ads_rows),
        LOOKER_RETURNS_TAB: count_blank_fsn(return_rows),
        LOOKER_LISTINGS_TAB: count_blank_fsn(listing_rows),
    }

    executive_metric_names_found = sorted(
        {
            normalize_text(row.get("Metric_Name", ""))
            for row in executive_rows
            if normalize_text(row.get("Metric_Name", ""))
        }
    )
    required_executive_metrics = [
        "Total Target FSNs",
        "Final Profit",
        "Total Alerts",
        "Critical Alerts",
        "High Alerts",
        "Active Tasks",
        "Missing COGS",
        "Missing Active Listings",
        "Ads Ready Count",
        "Return Issue FSNs",
        "COGS Completion Percent",
    ]
    missing_required_executive_metrics = [
        metric for metric in required_executive_metrics if metric not in executive_metric_names_found
    ]

    row_counts = {tab_name: len(rows) for tab_name, rows in looker_tables.items()}
    row_counts.update({tab_name: source_data[tab_name] for tab_name in SOURCE_TABS})

    checks = {
        "all_looker_tabs_exist": not missing_tabs,
        "executive_summary_has_required_metrics": not missing_required_executive_metrics,
        "looker_tabs_have_rows": all(row_counts.get(tab_name, 0) > 0 for tab_name in EXPECTED_ROW_TABS),
        "fsn_tabs_have_no_blank_fsn": all(count == 0 for count in blank_fsn_counts.values()),
        "source_tabs_still_have_rows": all(source_data.get(tab_name, 0) > 0 for tab_name in SOURCE_TABS),
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "tabs_checked": tabs_checked,
        "row_counts": row_counts,
        "missing_tabs": missing_tabs,
        "blank_fsn_counts": blank_fsn_counts,
        "executive_metric_names_found": executive_metric_names_found,
        "missing_required_executive_metrics": missing_required_executive_metrics,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        print(json.dumps(verify_looker_studio_sources(), indent=2, ensure_ascii=False))
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
