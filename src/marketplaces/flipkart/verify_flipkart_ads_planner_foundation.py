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
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

PRODUCT_AD_PROFILE_TAB = "FLIPKART_PRODUCT_AD_PROFILE"
KEYWORD_SEEDS_TAB = "GOOGLE_ADS_KEYWORD_SEEDS"
KEYWORD_CACHE_TAB = "GOOGLE_KEYWORD_METRICS_CACHE"
DEMAND_PROFILE_TAB = "PRODUCT_TYPE_DEMAND_PROFILE"
ADS_PLANNER_TAB = "FLIPKART_ADS_PLANNER"

TAB_NAMES = [PRODUCT_AD_PROFILE_TAB, KEYWORD_SEEDS_TAB, KEYWORD_CACHE_TAB, DEMAND_PROFILE_TAB, ADS_PLANNER_TAB]
PRODUCT_TYPE_ORDER = [
    "Rice/Fairy/Jhalar Light",
    "String Light",
    "Rope Light",
    "Strip Light",
    "Flood Light",
    "Gate/Wall/Post Light",
    "DJ/Event Light",
    "Unknown",
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


def blank_and_duplicate_counts(rows: Sequence[Dict[str, Any]], field_name: str) -> Tuple[int, int]:
    blanks = sum(1 for row in rows if not clean_fsn(row.get(field_name, "")))
    counts = Counter(clean_fsn(row.get(field_name, "")) for row in rows if clean_fsn(row.get(field_name, "")))
    duplicates = sum(count - 1 for count in counts.values() if count > 1)
    return blanks, duplicates


def ordered_distribution(rows: Sequence[Dict[str, Any]], field_name: str, preferred_order: Sequence[str]) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(field_name, "")) or "Unknown"
        counter[value] += 1
    ordered: Dict[str, int] = {}
    for key in preferred_order:
        if key in counter:
            ordered[key] = counter.pop(key)
    for key in sorted(counter):
        ordered[key] = counter[key]
    return ordered


def count_actions(rows: Sequence[Dict[str, Any]], values: Sequence[str]) -> int:
    target = {normalize_text(value) for value in values}
    return sum(1 for row in rows if normalize_text(row.get("Suggested_Ad_Action", "")) in target)


def verify_flipkart_ads_planner_foundation() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in TAB_NAMES:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)

    product_headers, product_rows = read_table(sheets_service, spreadsheet_id, PRODUCT_AD_PROFILE_TAB)
    seed_headers, seed_rows = read_table(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB)
    cache_headers, cache_rows = read_table(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)
    demand_headers, demand_rows = read_table(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)
    ads_headers, ads_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)

    product_blank, product_duplicate = blank_and_duplicate_counts(product_rows, "FSN")
    ads_blank, ads_duplicate = blank_and_duplicate_counts(ads_rows, "FSN")

    product_type_distribution = ordered_distribution(product_rows, "Final_Product_Type", PRODUCT_TYPE_ORDER)
    seasonality_distribution = ordered_distribution(
        product_rows,
        "Final_Seasonality_Tag",
        [
            "Diwali Heavy",
            "Festive Boost",
            "Year-Round",
            "Year-Round + Festive Boost",
            "Year-Round Home Exterior",
            "Utility / Outdoor",
            "Wedding / Event",
            "Unknown",
        ],
    )
    ads_action_distribution = ordered_distribution(
        ads_rows,
        "Suggested_Ad_Action",
        [
            "Fill COGS First",
            "Do Not Run Ads",
            "Do Not Run Ads / Improve Economics",
            "Fix Product First",
            "Fix Product/Listing First",
            "Fix Listing First",
            "Resolve Critical Alert First",
            "Seasonal Ads Later / Prepare Listing First",
            "Test Ads",
            "Always-On Test",
            "Seasonal/Event Test",
            "Manual Review",
            "Monitor",
        ],
    )

    fsn_counts = Counter(clean_fsn(row.get("FSN", "")) for row in ads_rows if clean_fsn(row.get("FSN", "")))
    duplicate_fsn_count = product_duplicate + ads_duplicate
    blank_fsn_count = product_blank + ads_blank
    checks = {
        "tabs_present": len([name for name in TAB_NAMES if name]) == len(TAB_NAMES),
        "product_profile_rows_match_planner_rows": len(product_rows) == len(ads_rows),
        "product_profile_fsns_unique": product_duplicate == 0,
        "ads_planner_fsns_unique": ads_duplicate == 0,
        "no_blank_fsn_rows": blank_fsn_count == 0,
        "keyword_seed_rows_present": len(seed_rows) > 0,
        "keyword_cache_rows_present": len(cache_rows) > 0,
        "demand_profile_rows_present": len(demand_rows) > 0,
        "product_types_classified": len(product_type_distribution) > 0,
        "planner_actions_classified": len(ads_action_distribution) > 0,
        "ads_planner_fsn_count_matches_unique_fsn": len(fsn_counts) == len(ads_rows),
    }
    status = "PASS" if all(checks.values()) else "FAIL"

    return {
        "status": status,
        "product_ad_profile_rows": len(product_rows),
        "keyword_seed_rows": len(seed_rows),
        "keyword_cache_rows": len(cache_rows),
        "demand_profile_rows": len(demand_rows),
        "ads_planner_rows": len(ads_rows),
        "product_type_distribution": product_type_distribution,
        "seasonality_distribution": seasonality_distribution,
        "ads_action_distribution": ads_action_distribution,
        "blank_fsn_count": blank_fsn_count,
        "duplicate_fsn_count": duplicate_fsn_count,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_ads_planner_foundation(), indent=2, ensure_ascii=False))
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
