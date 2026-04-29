from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_cogs_helpers import count_cogs_rows, hydrate_analysis_rows
from src.marketplaces.flipkart.flipkart_utils import normalize_text, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

COST_MASTER_TAB = "FLIPKART_COST_MASTER"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
DASHBOARD_DATA_TAB = "FLIPKART_DASHBOARD_DATA"


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


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
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


def dashboard_metric_map(rows: List[Dict[str, str]]) -> Dict[str, str]:
    return {normalize_text(row.get("Metric", "")): normalize_text(row.get("Value", "")) for row in rows if normalize_text(row.get("Metric", ""))}


def count_alert_type(rows: List[Dict[str, str]], alert_type: str) -> int:
    target = normalize_text(alert_type)
    return sum(1 for row in rows if normalize_text(row.get("Alert_Type", "")) == target)


def main() -> None:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [COST_MASTER_TAB, SKU_ANALYSIS_TAB, ALERTS_TAB, DASHBOARD_DATA_TAB]:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)

    _, cost_rows = read_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    _, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    _, alert_rows = read_table(sheets_service, spreadsheet_id, ALERTS_TAB)
    _, dashboard_data_rows = read_table(sheets_service, spreadsheet_id, DASHBOARD_DATA_TAB)

    hydrated_rows = hydrate_analysis_rows(analysis_rows, cost_rows)
    cost_master_cogs_entered_count, cost_master_missing_cogs_count = count_cogs_rows(cost_rows)
    sku_analysis_cogs_entered_count, sku_analysis_missing_cogs_count = count_cogs_rows(hydrated_rows)

    alerts_cogs_missing_sold_count = count_alert_type(alert_rows, "COGS Missing For Sold FSN")
    alerts_cogs_missing_high_confidence_count = count_alert_type(alert_rows, "COGS Missing For High Confidence FSN")

    dashboard_metrics = dashboard_metric_map(dashboard_data_rows)
    dashboard_fsns_with_cogs = int(parse_float(dashboard_metrics.get("FSNs With COGS", "0")))
    dashboard_fsns_missing_cogs = int(parse_float(dashboard_metrics.get("FSNs Missing COGS", "0")))
    dashboard_cogs_completion_percent = float(parse_float(dashboard_metrics.get("COGS Completion Percent", "0")))

    expected_completion = round(
        (sku_analysis_cogs_entered_count / len(hydrated_rows)) * 100,
        2,
    ) if hydrated_rows else 0.0

    checks = {
        "cost_master_has_cogs": cost_master_cogs_entered_count > 0,
        "sku_analysis_has_cogs": sku_analysis_cogs_entered_count > 0,
        "dashboard_matches_live_cogs_count": dashboard_fsns_with_cogs == sku_analysis_cogs_entered_count,
        "dashboard_matches_live_missing_cogs_count": dashboard_fsns_missing_cogs == sku_analysis_missing_cogs_count,
        "dashboard_completion_matches_live_analysis": round(dashboard_cogs_completion_percent, 2) == expected_completion,
        "alerts_cogs_missing_sold_within_bounds": 0 <= alerts_cogs_missing_sold_count <= sku_analysis_missing_cogs_count,
        "alerts_cogs_missing_high_confidence_within_bounds": 0 <= alerts_cogs_missing_high_confidence_count <= alerts_cogs_missing_sold_count,
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "status": status,
        "cost_master_rows": len(cost_rows),
        "cost_master_cogs_entered_count": cost_master_cogs_entered_count,
        "cost_master_missing_cogs_count": cost_master_missing_cogs_count,
        "sku_analysis_rows": len(hydrated_rows),
        "sku_analysis_cogs_entered_count": sku_analysis_cogs_entered_count,
        "sku_analysis_missing_cogs_count": sku_analysis_missing_cogs_count,
        "alerts_cogs_missing_sold_count": alerts_cogs_missing_sold_count,
        "alerts_cogs_missing_high_confidence_count": alerts_cogs_missing_high_confidence_count,
        "dashboard_fsns_with_cogs": dashboard_fsns_with_cogs,
        "dashboard_fsns_missing_cogs": dashboard_fsns_missing_cogs,
        "dashboard_cogs_completion_percent": round(dashboard_cogs_completion_percent, 2),
        "checks": checks,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
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
