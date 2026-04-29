from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.integrations.google_ads.google_ads_config import load_google_ads_config
from src.marketplaces.flipkart.flipkart_sheet_helpers import read_table, tab_exists
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

KEYWORD_SEEDS_TAB = "GOOGLE_ADS_KEYWORD_SEEDS"
KEYWORD_CACHE_TAB = "GOOGLE_KEYWORD_METRICS_CACHE"
DEMAND_PROFILE_TAB = "PRODUCT_TYPE_DEMAND_PROFILE"


def _load_spreadsheet_id() -> str:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    return json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]


def _duplicate_key_count(rows: Sequence[Dict[str, str]]) -> int:
    counts = Counter(
        (
            normalize_text(row.get("Keyword", "")).lower(),
            normalize_text(row.get("Product_Type", "")).lower(),
            normalize_text(row.get("Geo", "")).lower(),
            normalize_text(row.get("Language", "")).lower(),
        )
        for row in rows
        if normalize_text(row.get("Keyword", ""))
    )
    return sum(count - 1 for count in counts.values() if count > 1)


def _cache_status_distribution(rows: Sequence[Dict[str, str]]) -> Dict[str, int]:
    counter = Counter(normalize_text(row.get("Cache_Status", "")) or "Unknown" for row in rows)
    return dict(sorted(counter.items()))


def _numeric_fields_valid(rows: Sequence[Dict[str, str]]) -> bool:
    numeric_fields = [
        "Avg_Monthly_Searches",
        "Competition_Index",
        "Low_Top_Page_Bid",
        "High_Top_Page_Bid",
        "Monthly_Search_Jan",
        "Monthly_Search_Feb",
        "Monthly_Search_Mar",
        "Monthly_Search_Apr",
        "Monthly_Search_May",
        "Monthly_Search_Jun",
        "Monthly_Search_Jul",
        "Monthly_Search_Aug",
        "Monthly_Search_Sep",
        "Monthly_Search_Oct",
        "Monthly_Search_Nov",
        "Monthly_Search_Dec",
    ]
    for row in rows:
        if normalize_text(row.get("Cache_Status", "")).upper() != "SUCCESS":
            continue
        for field in numeric_fields:
            value = normalize_text(row.get(field, ""))
            if not value:
                continue
            try:
                parse_float(value)
            except Exception:
                return False
    return True


def verify_google_keyword_metrics_cache() -> Dict[str, Any]:
    spreadsheet_id = _load_spreadsheet_id()
    google_ads_config = load_google_ads_config()

    from src.auth_google import build_services

    sheets_service, _, _ = build_services()

    seeds_present = tab_exists(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB)
    cache_present = tab_exists(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)
    demand_present = tab_exists(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)

    seed_rows: List[Dict[str, str]] = []
    cache_rows: List[Dict[str, str]] = []
    demand_rows: List[Dict[str, str]] = []

    if seeds_present:
        _, seed_rows = read_table(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB)
    if cache_present:
        _, cache_rows = read_table(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)
    if demand_present:
        _, demand_rows = read_table(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)

    success_cache_rows = [row for row in cache_rows if normalize_text(row.get("Cache_Status", "")).upper() == "SUCCESS"]
    cache_empty = len(success_cache_rows) == 0
    product_types_with_keyword_data = len({normalize_text(row.get("Product_Type", "")) for row in success_cache_rows if normalize_text(row.get("Product_Type", ""))})
    cache_status_distribution = _cache_status_distribution(cache_rows)
    duplicate_cache_key_count = _duplicate_key_count(cache_rows)

    demand_source_ok = True
    if cache_empty:
        demand_source_ok = all(
            normalize_text(row.get("Demand_Source", "")).startswith("Manual Default")
            or "Cache Empty" in normalize_text(row.get("Demand_Source", ""))
            for row in demand_rows
        )
    checks = {
        "keyword_seed_tab_exists": seeds_present,
        "keyword_cache_tab_exists": cache_present,
        "demand_profile_tab_exists": demand_present,
        "cache_rows_have_valid_numeric_fields": _numeric_fields_valid(cache_rows),
        "no_duplicate_cache_keys": duplicate_cache_key_count == 0,
        "cache_empty_has_manual_defaults": demand_source_ok,
        "demand_profile_has_rows": len(demand_rows) > 0,
        "cache_or_success_rows_present": bool(cache_rows) or cache_empty,
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    warnings: List[str] = []
    if google_ads_config["status"] != "SUCCESS":
        warnings.append(google_ads_config["status"])
    if cache_empty:
        warnings.append("CACHE_EMPTY")
    if warnings and status == "PASS":
        status = "PASS_WITH_WARNINGS"

    return {
        "status": status,
        "keyword_seed_rows": len(seed_rows),
        "keyword_cache_rows": len(cache_rows),
        "product_type_demand_rows": len(demand_rows),
        "cache_status_distribution": cache_status_distribution,
        "product_types_with_keyword_data": product_types_with_keyword_data,
        "duplicate_cache_key_count": duplicate_cache_key_count,
        "checks": checks,
        "warnings": warnings,
    }


def main() -> None:
    try:
        print(json.dumps(verify_google_keyword_metrics_cache(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "keyword_seed_rows": 0,
                    "keyword_cache_rows": 0,
                    "product_type_demand_rows": 0,
                    "cache_status_distribution": {},
                    "product_types_with_keyword_data": 0,
                    "duplicate_cache_key_count": 0,
                    "checks": {},
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

