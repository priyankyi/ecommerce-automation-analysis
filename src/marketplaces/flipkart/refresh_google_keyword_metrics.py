from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter, OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.integrations.google_ads.fetch_keyword_historical_metrics import fetch_keyword_historical_metrics
from src.integrations.google_ads.google_ads_config import load_google_ads_config, normalize_customer_id
from src.marketplaces.flipkart.flipkart_sheet_helpers import clear_tab, ensure_tab, freeze_and_format, read_table, tab_exists, write_rows
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, ensure_directories, normalize_text, now_iso

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "google_keyword_metrics_refresh_log.csv"

KEYWORD_SEEDS_TAB = "GOOGLE_ADS_KEYWORD_SEEDS"
KEYWORD_CACHE_TAB = "GOOGLE_KEYWORD_METRICS_CACHE"

LOCAL_CACHE_PATH = OUTPUT_DIR / "google_keyword_metrics_cache.csv"

CACHE_HEADERS = [
    "Keyword",
    "Product_Type",
    "Geo",
    "Geo_Target_ID",
    "Language",
    "Language_ID",
    "Avg_Monthly_Searches",
    "Competition",
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
    "Last_Refreshed",
    "Source",
    "Cache_Status",
    "Error_Message",
    "Remarks",
]

DEFAULT_GEO_TARGET_ID = "2356"
DEFAULT_LANGUAGE_ID = "1000"


def _row_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        normalize_text(row.get("Keyword", "")).lower(),
        normalize_text(row.get("Product_Type", "")).lower(),
        normalize_text(row.get("Geo", "")).lower(),
        normalize_text(row.get("Language", "")).lower(),
    )


def _dedupe_seed_rows(seed_rows: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    deduped: List[Dict[str, str]] = []
    seen = set()
    for row in seed_rows:
        keyword = normalize_text(row.get("Seed_Keyword", ""))
        if not keyword:
            continue
        candidate = {
            "Product_Type": normalize_text(row.get("Product_Type", "")),
            "Seed_Keyword": keyword,
            "Geo": normalize_text(row.get("Geo", "")) or "India",
            "Language": normalize_text(row.get("Language", "")) or "English",
            "Intent_Type": normalize_text(row.get("Intent_Type", "")),
            "Priority": normalize_text(row.get("Priority", "")),
            "Manual_Status": normalize_text(row.get("Manual_Status", "")),
            "Last_Updated": normalize_text(row.get("Last_Updated", "")),
        }
        key = _row_key(
            {
                "Keyword": candidate["Seed_Keyword"],
                "Product_Type": candidate["Product_Type"],
                "Geo": candidate["Geo"],
                "Language": candidate["Language"],
            }
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _normalize_geo_target_id(value: Any, geo_text: str) -> str:
    text = normalize_customer_id(value)
    if text:
        return text
    geo_norm = normalize_text(geo_text).lower()
    if "india" in geo_norm or geo_norm == "in":
        return DEFAULT_GEO_TARGET_ID
    return DEFAULT_GEO_TARGET_ID


def _normalize_language_id(value: Any, language_text: str) -> str:
    text = normalize_customer_id(value)
    if text:
        return text
    language_norm = normalize_text(language_text).lower()
    if "english" in language_norm:
        return DEFAULT_LANGUAGE_ID
    return DEFAULT_LANGUAGE_ID


def _is_fresh(last_refreshed: str) -> bool:
    text = normalize_text(last_refreshed)
    if not text:
        return False
    try:
        refreshed_at = datetime.fromisoformat(text)
    except ValueError:
        return False
    return refreshed_at >= datetime.now() - timedelta(days=30)


def _index_cache_rows(cache_rows: Sequence[Dict[str, str]]) -> Dict[Tuple[str, str, str, str], Dict[str, str]]:
    indexed: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    for row in cache_rows:
        key = _row_key(row)
        if any(not part for part in key):
            continue
        indexed[key] = dict(row)
    return indexed


def _resolve_config_status() -> Dict[str, Any]:
    return load_google_ads_config()


def _write_cache_outputs(sheets_service, spreadsheet_id: str, rows: Sequence[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)
    clear_tab(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)
    write_rows(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB, CACHE_HEADERS, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(CACHE_HEADERS))


def _sort_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            normalize_text(row.get("Product_Type", "")),
            normalize_text(row.get("Keyword", "")),
            normalize_text(row.get("Geo", "")),
            normalize_text(row.get("Language", "")),
        ),
    )


def _merge_existing_row(existing_row: Dict[str, str], refreshed_row: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing_row)
    merged.update({key: value for key, value in refreshed_row.items() if value != ""})
    if normalize_text(existing_row.get("Remarks", "")) and not normalize_text(refreshed_row.get("Remarks", "")):
        merged["Remarks"] = existing_row.get("Remarks", "")
    return merged


def _close_variant_lookup(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        api_keyword = normalize_text(row.get("Keyword", "")).lower()
        if api_keyword and api_keyword not in lookup:
            lookup[api_keyword] = dict(row)
        for close_variant in normalize_text(row.get("Close_Variants", "")).split(";"):
            variant_key = normalize_text(close_variant).lower()
            if variant_key and variant_key not in lookup:
                lookup[variant_key] = dict(row)
    return lookup


def _is_access_not_ready_message(message: str) -> bool:
    message_lower = normalize_text(message).lower()
    return any(
        token in message_lower
        for token in (
            "developer token",
            "basic access",
            "standard access",
            "user_permission_denied",
            "doesn't have permission",
            "does not have permission",
            "caller does not have permission",
            "access token scope insufficient",
        )
    )


def refresh_google_keyword_metrics(
    force: bool = False,
    batch_size: int = 20,
    sleep_seconds: int = 2,
    max_keywords: int | None = None,
) -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    if not tab_exists(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {KEYWORD_SEEDS_TAB}")
    if not tab_exists(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB):
        ensure_tab(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)

    _, seed_rows_raw = read_table(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB)
    cache_headers, cache_rows_raw = read_table(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)

    seed_rows = _dedupe_seed_rows(seed_rows_raw)
    cache_index = _index_cache_rows(cache_rows_raw)
    config_payload = _resolve_config_status()

    keywords_total = len(seed_rows)
    keywords_already_fresh = 0
    refresh_candidates: List[Dict[str, str]] = []

    for seed_row in seed_rows:
        key = (
            normalize_text(seed_row["Seed_Keyword"]).lower(),
            normalize_text(seed_row["Product_Type"]).lower(),
            normalize_text(seed_row["Geo"]).lower(),
            normalize_text(seed_row["Language"]).lower(),
        )
        existing_row = cache_index.get(key)
        if existing_row and not force and normalize_text(existing_row.get("Cache_Status", "")).upper() != "ERROR" and _is_fresh(existing_row.get("Last_Refreshed", "")):
            keywords_already_fresh += 1
            continue
        refresh_candidates.append(seed_row)

    if max_keywords is not None:
        refresh_candidates = refresh_candidates[: max(0, int(max_keywords))]

    if config_payload["status"] == "NEEDS_CREDENTIALS":
        log_row = {
            "timestamp": now_iso(),
            "spreadsheet_id": spreadsheet_id,
            "status": "NEEDS_CREDENTIALS",
            "keywords_total": keywords_total,
            "keywords_already_fresh": keywords_already_fresh,
            "keywords_to_refresh": len(refresh_candidates),
            "keywords_refreshed": 0,
            "keywords_failed": 0,
            "cache_rows_written": len(cache_rows_raw),
            "api_called": False,
            "log_path": str(LOG_PATH),
            "message": "Google Ads credentials file is missing",
        }
        append_csv_log(
            LOG_PATH,
            list(log_row.keys()),
            [log_row],
        )
        return {
            "status": "NEEDS_CREDENTIALS",
            "keywords_total": keywords_total,
            "keywords_already_fresh": keywords_already_fresh,
            "keywords_to_refresh": len(refresh_candidates),
            "keywords_refreshed": 0,
            "keywords_failed": 0,
            "cache_rows_written": len(cache_rows_raw),
            "api_called": False,
            "log_path": str(LOG_PATH),
        }

    if config_payload["status"] == "INVALID_CONFIG":
        log_row = {
            "timestamp": now_iso(),
            "spreadsheet_id": spreadsheet_id,
            "status": "API_ACCESS_NOT_READY",
            "keywords_total": keywords_total,
            "keywords_already_fresh": keywords_already_fresh,
            "keywords_to_refresh": len(refresh_candidates),
            "keywords_refreshed": 0,
            "keywords_failed": 0,
            "cache_rows_written": len(cache_rows_raw),
            "api_called": False,
            "log_path": str(LOG_PATH),
            "message": f"Invalid Google Ads config at {config_payload['config_path']}",
        }
        append_csv_log(LOG_PATH, list(log_row.keys()), [log_row])
        return {
            "status": "API_ACCESS_NOT_READY",
            "keywords_total": keywords_total,
            "keywords_already_fresh": keywords_already_fresh,
            "keywords_to_refresh": len(refresh_candidates),
            "keywords_refreshed": 0,
            "keywords_failed": 0,
            "cache_rows_written": len(cache_rows_raw),
            "api_called": False,
            "log_path": str(LOG_PATH),
        }

    customer_id = normalize_customer_id(config_payload["config"]["customer_id"])

    grouped: "OrderedDict[Tuple[str, str], List[Dict[str, str]]]" = OrderedDict()
    for seed_row in refresh_candidates:
        geo_target_id = _normalize_geo_target_id(seed_row.get("Geo_Target_ID", ""), seed_row.get("Geo", ""))
        language_id = _normalize_language_id(seed_row.get("Language_ID", ""), seed_row.get("Language", ""))
        grouped.setdefault((geo_target_id, language_id), []).append(seed_row)

    refreshed_rows: List[Dict[str, Any]] = []
    api_called = False
    keywords_refreshed = 0
    keywords_failed = 0
    batch_errors = 0
    access_not_ready_message = ""

    for (geo_target_id, language_id), grouped_rows in grouped.items():
        group_keywords = [row["Seed_Keyword"] for row in grouped_rows]
        if not group_keywords:
            continue
        api_called = True
        try:
            fetched_rows = fetch_keyword_historical_metrics(
                group_keywords,
                geo_target_id=geo_target_id,
                language_id=language_id,
                customer_id=customer_id,
                batch_size=batch_size,
            )
            lookup = _close_variant_lookup(fetched_rows)
            for seed_row in grouped_rows:
                keyword_key = normalize_text(seed_row["Seed_Keyword"]).lower()
                seed_key = (
                    keyword_key,
                    normalize_text(seed_row["Product_Type"]).lower(),
                    normalize_text(seed_row["Geo"]).lower(),
                    normalize_text(seed_row["Language"]).lower(),
                )
                api_row = lookup.get(keyword_key)
                if api_row is None:
                    api_row = {
                        "Keyword": seed_row["Seed_Keyword"],
                        "Close_Variants": "",
                        "Avg_Monthly_Searches": "",
                        "Competition": "",
                        "Competition_Index": "",
                        "Low_Top_Page_Bid": "",
                        "High_Top_Page_Bid": "",
                        "Monthly_Search_Jan": "",
                        "Monthly_Search_Feb": "",
                        "Monthly_Search_Mar": "",
                        "Monthly_Search_Apr": "",
                        "Monthly_Search_May": "",
                        "Monthly_Search_Jun": "",
                        "Monthly_Search_Jul": "",
                        "Monthly_Search_Aug": "",
                        "Monthly_Search_Sep": "",
                        "Monthly_Search_Oct": "",
                        "Monthly_Search_Nov": "",
                        "Monthly_Search_Dec": "",
                        "Source": "Google Ads API",
                        "Last_Refreshed": now_iso(),
                        "Cache_Status": "ERROR",
                        "Error_Message": "No matching keyword result returned",
                    }
                    keywords_failed += 1
                elif normalize_text(api_row.get("Cache_Status", "")).upper() == "ERROR":
                    error_message = normalize_text(api_row.get("Error_Message", ""))
                    if _is_access_not_ready_message(error_message):
                        access_not_ready_message = error_message or "Google Ads Keyword Planner access is not ready"
                        break
                    keywords_failed += 1
                else:
                    keywords_refreshed += 1
                refreshed_rows.append(
                    {
                        "Keyword": seed_row["Seed_Keyword"],
                        "Product_Type": seed_row["Product_Type"],
                        "Geo": seed_row["Geo"],
                        "Geo_Target_ID": geo_target_id,
                        "Language": seed_row["Language"],
                        "Language_ID": language_id,
                        "Avg_Monthly_Searches": api_row.get("Avg_Monthly_Searches", ""),
                        "Competition": api_row.get("Competition", ""),
                        "Competition_Index": api_row.get("Competition_Index", ""),
                        "Low_Top_Page_Bid": api_row.get("Low_Top_Page_Bid", ""),
                        "High_Top_Page_Bid": api_row.get("High_Top_Page_Bid", ""),
                        "Monthly_Search_Jan": api_row.get("Monthly_Search_Jan", ""),
                        "Monthly_Search_Feb": api_row.get("Monthly_Search_Feb", ""),
                        "Monthly_Search_Mar": api_row.get("Monthly_Search_Mar", ""),
                        "Monthly_Search_Apr": api_row.get("Monthly_Search_Apr", ""),
                        "Monthly_Search_May": api_row.get("Monthly_Search_May", ""),
                        "Monthly_Search_Jun": api_row.get("Monthly_Search_Jun", ""),
                        "Monthly_Search_Jul": api_row.get("Monthly_Search_Jul", ""),
                        "Monthly_Search_Aug": api_row.get("Monthly_Search_Aug", ""),
                        "Monthly_Search_Sep": api_row.get("Monthly_Search_Sep", ""),
                        "Monthly_Search_Oct": api_row.get("Monthly_Search_Oct", ""),
                        "Monthly_Search_Nov": api_row.get("Monthly_Search_Nov", ""),
                        "Monthly_Search_Dec": api_row.get("Monthly_Search_Dec", ""),
                        "Last_Refreshed": api_row.get("Last_Refreshed", now_iso()),
                        "Source": api_row.get("Source", "Google Ads API"),
                        "Cache_Status": api_row.get("Cache_Status", "SUCCESS"),
                        "Error_Message": api_row.get("Error_Message", ""),
                        "Remarks": normalize_text(api_row.get("Remarks", "")) or normalize_text(cache_index.get(seed_key, {}).get("Remarks", "")),
                    }
                )
                if access_not_ready_message:
                    break
        except Exception as exc:
            batch_errors += 1
            message = normalize_text(str(exc)) or exc.__class__.__name__
            if _is_access_not_ready_message(message):
                access_not_ready_message = message
                break
            for seed_row in grouped_rows:
                refreshed_rows.append(
                    {
                        "Keyword": seed_row["Seed_Keyword"],
                        "Product_Type": seed_row["Product_Type"],
                        "Geo": seed_row["Geo"],
                        "Geo_Target_ID": geo_target_id,
                        "Language": seed_row["Language"],
                        "Language_ID": language_id,
                        "Avg_Monthly_Searches": "",
                        "Competition": "",
                        "Competition_Index": "",
                        "Low_Top_Page_Bid": "",
                        "High_Top_Page_Bid": "",
                        "Monthly_Search_Jan": "",
                        "Monthly_Search_Feb": "",
                        "Monthly_Search_Mar": "",
                        "Monthly_Search_Apr": "",
                        "Monthly_Search_May": "",
                        "Monthly_Search_Jun": "",
                        "Monthly_Search_Jul": "",
                        "Monthly_Search_Aug": "",
                        "Monthly_Search_Sep": "",
                        "Monthly_Search_Oct": "",
                        "Monthly_Search_Nov": "",
                        "Monthly_Search_Dec": "",
                        "Last_Refreshed": now_iso(),
                        "Source": "Google Ads API",
                        "Cache_Status": "ERROR",
                        "Error_Message": message,
                        "Remarks": "",
                    }
                )
                keywords_failed += 1
        if access_not_ready_message:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    if access_not_ready_message:
        log_row = {
            "timestamp": now_iso(),
            "spreadsheet_id": spreadsheet_id,
            "status": "API_ACCESS_NOT_READY",
            "keywords_total": keywords_total,
            "keywords_already_fresh": keywords_already_fresh,
            "keywords_to_refresh": len(refresh_candidates),
            "keywords_refreshed": 0,
            "keywords_failed": 0,
            "cache_rows_written": len(cache_rows_raw),
            "api_called": True,
            "log_path": str(LOG_PATH),
            "message": access_not_ready_message,
        }
        append_csv_log(LOG_PATH, list(log_row.keys()), [log_row])
        return {
            "status": "API_ACCESS_NOT_READY",
            "keywords_total": keywords_total,
            "keywords_already_fresh": keywords_already_fresh,
            "keywords_to_refresh": len(refresh_candidates),
            "keywords_refreshed": 0,
            "keywords_failed": 0,
            "cache_rows_written": len(cache_rows_raw),
            "api_called": True,
            "log_path": str(LOG_PATH),
        }

    refreshed_index = _index_cache_rows(refreshed_rows)
    merged_rows: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for row in cache_rows_raw:
        key = _row_key(row)
        if any(not part for part in key):
            continue
        merged_rows[key] = dict(row)
    for row in refreshed_rows:
        key = _row_key(row)
        existing = merged_rows.get(key, {})
        merged_rows[key] = _merge_existing_row(existing, row)

    final_rows = _sort_rows(list(merged_rows.values()))
    for row in final_rows:
        row.setdefault("Geo_Target_ID", _normalize_geo_target_id(row.get("Geo_Target_ID", ""), row.get("Geo", "")))
        row.setdefault("Language_ID", _normalize_language_id(row.get("Language_ID", ""), row.get("Language", "")))
        row.setdefault("Remarks", "")

    _write_cache_outputs(sheets_service, spreadsheet_id, final_rows)
    with LOCAL_CACHE_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CACHE_HEADERS)
        writer.writeheader()
        for row in final_rows:
            writer.writerow({header: row.get(header, "") for header in CACHE_HEADERS})

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": "SUCCESS",
        "keywords_total": keywords_total,
        "keywords_already_fresh": keywords_already_fresh,
        "keywords_to_refresh": len(refresh_candidates),
        "keywords_refreshed": keywords_refreshed,
        "keywords_failed": keywords_failed,
        "cache_rows_written": len(final_rows),
        "api_called": api_called,
        "log_path": str(LOG_PATH),
        "message": "Refreshed Google Keyword Metrics cache",
    }
    append_csv_log(LOG_PATH, list(log_row.keys()), [log_row])

    return {
        "status": "SUCCESS",
        "keywords_total": keywords_total,
        "keywords_already_fresh": keywords_already_fresh,
        "keywords_to_refresh": len(refresh_candidates),
        "keywords_refreshed": keywords_refreshed,
        "keywords_failed": keywords_failed,
        "cache_rows_written": len(final_rows),
        "api_called": api_called,
        "log_path": str(LOG_PATH),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Flipkart Google Keyword Metrics cache from Google Ads API.")
    parser.add_argument("--force", action="store_true", help="Refresh all keywords even if the cache entry is still fresh.")
    parser.add_argument("--batch-size", type=int, default=20, help="Batch size sent to the Google Ads API.")
    parser.add_argument("--sleep-seconds", type=int, default=2, help="Sleep between keyword batches.")
    parser.add_argument("--max-keywords", type=int, default=None, help="Limit the number of keywords refreshed for testing.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        print(
            json.dumps(
                refresh_google_keyword_metrics(
                    force=args.force,
                    batch_size=args.batch_size,
                    sleep_seconds=args.sleep_seconds,
                    max_keywords=args.max_keywords,
                ),
                indent=2,
                ensure_ascii=False,
            )
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "keywords_total": 0,
                    "keywords_already_fresh": 0,
                    "keywords_to_refresh": 0,
                    "keywords_refreshed": 0,
                    "keywords_failed": 0,
                    "cache_rows_written": 0,
                    "api_called": False,
                    "log_path": str(LOG_PATH),
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
