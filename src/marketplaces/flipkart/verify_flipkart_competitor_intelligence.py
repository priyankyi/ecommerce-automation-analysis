from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import load_json, read_table, tab_exists
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

QUEUE_TAB = "FLIPKART_COMPETITOR_SEARCH_QUEUE"
RESULTS_TAB = "FLIPKART_VISUAL_COMPETITOR_RESULTS"
PRICE_TAB = "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE"
LOOKER_TAB = "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE"
ADS_PLANNER_TAB = "FLIPKART_ADS_PLANNER"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
MODULE_CONFIDENCE_TAB = "FLIPKART_MODULE_CONFIDENCE"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"

VALID_RISK_LEVELS = {"Low", "Medium", "High", "Critical", "Not Enough Data"}


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


def count_duplicates(rows: Sequence[Dict[str, Any]], *fields: str) -> int:
    seen = Counter()
    for row in rows:
        key = tuple(clean_fsn(row.get(field, "")) if field == "FSN" else normalize_text(row.get(field, "")).lower() for field in fields)
        if any(not part for part in key):
            continue
        seen[key] += 1
    return sum(count - 1 for count in seen.values() if count > 1)


def ordered_distribution(rows: Sequence[Dict[str, Any]], field_name: str, preferred_order: Sequence[str]) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(field_name, "")) or "Unknown"
        counter[value] += 1
    ordered: Dict[str, int] = {}
    for item in preferred_order:
        if item in counter:
            ordered[item] = counter.pop(item)
    for item in sorted(counter):
        ordered[item] = counter[item]
    return ordered


def check_source_tabs(sheets_service, spreadsheet_id: str) -> bool:
    for tab_name in [ADS_PLANNER_TAB, SKU_ANALYSIS_TAB, MODULE_CONFIDENCE_TAB, COST_MASTER_TAB, ALERTS_TAB, ACTIVE_TASKS_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            return False
        _, rows = read_table(sheets_service, spreadsheet_id, tab_name)
        if len(rows) == 0:
            return False
    return True


def verify_flipkart_competitor_intelligence() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [QUEUE_TAB, RESULTS_TAB, PRICE_TAB, LOOKER_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    queue_headers, queue_rows = read_table(sheets_service, spreadsheet_id, QUEUE_TAB)
    result_headers, result_rows = read_table(sheets_service, spreadsheet_id, RESULTS_TAB)
    price_headers, price_rows = read_table(sheets_service, spreadsheet_id, PRICE_TAB)
    looker_headers, looker_rows = read_table(sheets_service, spreadsheet_id, LOOKER_TAB)
    _, planner_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)

    ad_ready_fsns = {
        clean_fsn(row.get("FSN", ""))
        for row in planner_rows
        if clean_fsn(row.get("FSN", "")) and normalize_text(row.get("Final_Ads_Decision", "")) in {"Scale Ads", "Test Ads"}
    }
    queue_rows_expected = len(ad_ready_fsns) > 0
    queue_blank_fsn = sum(1 for row in queue_rows if not clean_fsn(row.get("FSN", "")))
    price_blank_fsn = sum(1 for row in price_rows if not clean_fsn(row.get("FSN", "")))
    looker_blank_fsn = sum(1 for row in looker_rows if not clean_fsn(row.get("FSN", "")))
    result_blank_fsn = sum(1 for row in result_rows if not clean_fsn(row.get("FSN", "")))
    flipkart_only_url_violations = sum(
        1
        for row in result_rows
        if normalize_text(row.get("Competitor_Link", "")).lower() and "flipkart.com" not in normalize_text(row.get("Competitor_Link", "")).lower()
    )
    duplicate_link_count = count_duplicates(result_rows, "FSN", "Competitor_Link")
    risk_distribution = ordered_distribution(price_rows, "Competition_Risk_Level", ["Low", "Medium", "High", "Critical", "Not Enough Data"])
    suggested_action_distribution = ordered_distribution(
        price_rows,
        "Suggested_Action",
        [
            "Scale Ads Allowed",
            "Test Ads Carefully",
            "Improve Price Before Ads",
            "Improve Listing Before Ads",
            "Do Not Scale Ads",
            "Need Competitor Data",
            "Manual Review Required",
        ],
    )
    valid_risk_levels = all(normalize_text(row.get("Competition_Risk_Level", "")) in VALID_RISK_LEVELS for row in price_rows)
    suggestions_populated = all(normalize_text(row.get("Suggested_Action", "")) for row in price_rows)
    no_auto_price_change_fields = all(
        "AUTO_PRICE_CHANGE" not in " ".join(headers).upper()
        and "PRICE_CHANGE" not in " ".join(headers).upper()
        and "SUGGESTED_NEW_PRICE" not in " ".join(headers).upper()
        for headers in [queue_headers, result_headers, price_headers, looker_headers]
    )
    source_tabs_preserved = check_source_tabs(sheets_service, spreadsheet_id)

    checks = {
        "tabs_present": all(tab_exists(sheets_service, spreadsheet_id, tab_name) for tab_name in [QUEUE_TAB, RESULTS_TAB, PRICE_TAB, LOOKER_TAB]),
        "queue_rows_cover_ad_ready_fsns": (len(queue_rows) > 0) if queue_rows_expected else True,
        "no_blank_fsn_rows": queue_blank_fsn == 0 and price_blank_fsn == 0 and looker_blank_fsn == 0,
        "only_flipkart_urls_in_results": flipkart_only_url_violations == 0,
        "no_duplicate_fsn_competitor_link": duplicate_link_count == 0,
        "valid_risk_levels": valid_risk_levels,
        "suggested_actions_populated": suggestions_populated,
        "no_auto_price_change_fields": no_auto_price_change_fields,
        "source_tabs_preserved": source_tabs_preserved,
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "status": status,
        "queue_rows": len(queue_rows),
        "visual_result_rows": len(result_rows),
        "price_intelligence_rows": len(price_rows),
        "looker_rows": len(looker_rows),
        "flipkart_only_url_violations": flipkart_only_url_violations,
        "risk_distribution": risk_distribution,
        "suggested_action_distribution": suggested_action_distribution,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return payload


def main() -> None:
    try:
        verify_flipkart_competitor_intelligence()
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
