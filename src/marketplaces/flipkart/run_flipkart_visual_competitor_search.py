from __future__ import annotations

import argparse
import csv
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
from src.integrations.visual_search.search_google_lens_flipkart_only import (
    append_usage_log_row,
    count_usage_calls_for_month,
    current_month_key,
    hash_image_url,
    load_usage_log,
    month_image_hash_seen,
    search_flipkart_only,
    USAGE_LOG_PATH,
)
from src.integrations.visual_search.visual_search_config import extract_visual_search_limits, load_visual_search_config, resolve_visual_search_config_path
from src.marketplaces.flipkart.flipkart_sheet_helpers import clear_tab, ensure_tab, freeze_and_format, load_json, read_table, tab_exists, write_rows
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, clean_fsn, ensure_directories, normalize_text, now_iso, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_visual_competitor_search_log.csv"
LOCAL_QUEUE_PATH = OUTPUT_DIR / "flipkart_competitor_search_queue.csv"
LOCAL_RESULTS_PATH = OUTPUT_DIR / "flipkart_visual_competitor_results.csv"
RESULT_CACHE_PATH = OUTPUT_DIR / "flipkart_visual_search_cache.json"
USAGE_LOG_PATH_LOCAL = USAGE_LOG_PATH

QUEUE_TAB = "FLIPKART_COMPETITOR_SEARCH_QUEUE"
RESULTS_TAB = "FLIPKART_VISUAL_COMPETITOR_RESULTS"

RESULT_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Our_Selling_Price",
    "Our_Pack_Count",
    "Our_Unit_Price",
    "Competitor_Title",
    "Competitor_Link",
    "Competitor_Image",
    "Competitor_Price",
    "Competitor_Pack_Count",
    "Competitor_Unit_Price",
    "Competitor_Rating",
    "Competitor_Reviews",
    "Competitor_In_Stock",
    "Visual_Search_Source",
    "Comparable_YN",
    "Comparison_Confidence",
    "Raw_Position",
    "Last_Checked",
]

QUEUE_PROCESS_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Product_Type",
    "Final_Ads_Decision",
    "Final_Budget_Recommendation",
    "Our_Selling_Price",
    "Our_Pack_Count",
    "Our_Unit_Price",
    "Our_Final_Profit_Margin",
    "Our_Return_Rate",
    "Product_Image_URL",
    "Search_Method",
    "Search_Status",
    "Manual_Review_Status",
    "Priority",
    "Remarks",
    "Last_Updated",
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


def write_local_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def key_fsn_link(row: Dict[str, Any]) -> Tuple[str, str]:
    return clean_fsn(row.get("FSN", "")), normalize_text(row.get("Competitor_Link", ""))


def build_index(rows: Sequence[Dict[str, str]], field_name: str = "FSN") -> Dict[str, Dict[str, str]]:
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get(field_name, ""))
        if fsn and fsn not in indexed:
            indexed[fsn] = dict(row)
    return indexed


def build_grouped_results(rows: Sequence[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        grouped.setdefault(fsn, []).append(dict(row))
    return grouped


def make_result_row(queue_row: Dict[str, str], raw_result: Dict[str, Any]) -> Dict[str, Any]:
    competitor_price = normalize_text(raw_result.get("Competitor_Price", ""))
    competitor_pack_count = normalize_text(raw_result.get("Competitor_Pack_Count", ""))
    competitor_unit_price = competitor_price
    if competitor_price:
        try:
            price_value = parse_float(competitor_price)
            pack_value = parse_float(competitor_pack_count)
            if pack_value > 0:
                competitor_unit_price = f"{price_value / pack_value:.2f}"
            else:
                competitor_unit_price = f"{price_value:.2f}"
        except Exception:
            competitor_unit_price = competitor_price
    pack_count = normalize_text(queue_row.get("Our_Pack_Count", ""))
    our_unit_price = normalize_text(queue_row.get("Our_Unit_Price", ""))
    comparable = "Yes" if normalize_text(raw_result.get("Comparable_YN", "")) == "Yes" and normalize_text(raw_result.get("Competitor_Link", "")) else "No"
    confidence = normalize_text(raw_result.get("Comparison_Confidence", ""))
    if comparable == "Yes" and pack_count and competitor_pack_count:
        confidence = "High"
    elif comparable == "Yes" and (pack_count or competitor_pack_count):
        confidence = confidence or "Medium"
    elif comparable == "Yes":
        confidence = confidence or "Low"
    return {
        "Run_ID": normalize_text(queue_row.get("Run_ID", "")),
        "FSN": clean_fsn(queue_row.get("FSN", "")),
        "SKU_ID": normalize_text(queue_row.get("SKU_ID", "")),
        "Product_Title": normalize_text(queue_row.get("Product_Title", "")),
        "Our_Selling_Price": normalize_text(queue_row.get("Our_Selling_Price", "")),
        "Our_Pack_Count": pack_count,
        "Our_Unit_Price": our_unit_price,
        "Competitor_Title": normalize_text(raw_result.get("Competitor_Title", "")),
        "Competitor_Link": normalize_text(raw_result.get("Competitor_Link", "")),
        "Competitor_Image": normalize_text(raw_result.get("Competitor_Image", "")),
        "Competitor_Price": competitor_price,
        "Competitor_Pack_Count": competitor_pack_count,
        "Competitor_Unit_Price": competitor_unit_price,
        "Competitor_Rating": normalize_text(raw_result.get("Competitor_Rating", "")),
        "Competitor_Reviews": normalize_text(raw_result.get("Competitor_Reviews", "")),
        "Competitor_In_Stock": normalize_text(raw_result.get("Competitor_In_Stock", "")),
        "Visual_Search_Source": normalize_text(raw_result.get("Visual_Search_Source", "")),
        "Comparable_YN": comparable,
        "Comparison_Confidence": confidence or "Low",
        "Raw_Position": normalize_text(raw_result.get("Raw_Position", "")),
        "Last_Checked": now_iso(),
    }


def merge_result_rows(existing_rows: Sequence[Dict[str, str]], new_rows: Sequence[Dict[str, Any]], force_fsns: Sequence[str]) -> List[Dict[str, Any]]:
    force_set = {clean_fsn(fsn) for fsn in force_fsns if clean_fsn(fsn)}
    merged: List[Dict[str, Any]] = []
    if force_set:
        for row in existing_rows:
            if clean_fsn(row.get("FSN", "")) not in force_set:
                merged.append(dict(row))
    else:
        merged.extend(dict(row) for row in existing_rows)
    seen = {key_fsn_link(row) for row in merged}
    for row in new_rows:
        key = key_fsn_link(row)
        if key in seen:
            continue
        merged.append(dict(row))
        seen.add(key)
    return merged


def write_output_tab(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def load_queue_rows(sheets_service, spreadsheet_id: str) -> Tuple[List[str], List[Dict[str, str]]]:
    if not tab_exists(sheets_service, spreadsheet_id, QUEUE_TAB):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {QUEUE_TAB}")
    return read_table(sheets_service, spreadsheet_id, QUEUE_TAB)


def load_results_rows(sheets_service, spreadsheet_id: str) -> Tuple[List[str], List[Dict[str, str]]]:
    if not tab_exists(sheets_service, spreadsheet_id, RESULTS_TAB):
        return RESULT_HEADERS, []
    return read_table(sheets_service, spreadsheet_id, RESULTS_TAB)


def parse_int(value: Any, default: int) -> int:
    text = normalize_text(value)
    if not text:
        return default
    try:
        return max(0, int(float(text)))
    except ValueError:
        return default


def run_flipkart_visual_competitor_search(max_fsns: int = 5, force: bool = False, sleep_seconds: int = 3) -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    config_payload = load_visual_search_config()
    provider_config = config_payload.get("config") or {}
    limits = extract_visual_search_limits(provider_config)
    monthly_limit = limits["monthly_limit"]
    safe_monthly_limit = limits["safe_monthly_limit"]
    provider = normalize_text(provider_config.get("VISUAL_SEARCH_PROVIDER", "SERPAPI_GOOGLE_LENS")).upper() or "SERPAPI_GOOGLE_LENS"
    current_month = current_month_key()
    usage_rows = load_usage_log()
    month_calls_used = count_usage_calls_for_month(usage_rows, current_month, provider)
    remaining_safe_calls = max(0, safe_monthly_limit - month_calls_used)

    if not tab_exists(sheets_service, spreadsheet_id, QUEUE_TAB):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {QUEUE_TAB}")
    if not tab_exists(sheets_service, spreadsheet_id, RESULTS_TAB):
        ensure_tab(sheets_service, spreadsheet_id, RESULTS_TAB)

    _, queue_rows = load_queue_rows(sheets_service, spreadsheet_id)
    _, results_rows = load_results_rows(sheets_service, spreadsheet_id)
    existing_results_rows = list(results_rows)

    pending_rows = []
    for row in queue_rows:
        status = normalize_text(row.get("Search_Status", ""))
        if force or status == "Pending":
            pending_rows.append(dict(row))
    pending_rows = pending_rows[: max_fsns if max_fsns > 0 else len(pending_rows)]

    updated_queue_rows = [dict(row) for row in queue_rows]
    new_results: List[Dict[str, Any]] = []
    processed_fsns: List[str] = []
    skipped_already_searched = 0
    skipped_missing_image_url = 0
    api_called_count = 0
    no_match_count = 0
    needs_credentials_count = 0
    status_counter = Counter()
    message_notes: List[str] = []
    quota_guard_stopped = False
    force_warning = bool(force)

    for row in pending_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        processed_fsns.append(fsn)
        image_url = normalize_text(row.get("Product_Image_URL", ""))
        query = normalize_text(row.get("Product_Title", "")) or normalize_text(row.get("SKU_ID", ""))
        image_url_hash = hash_image_url(image_url)

        if not image_url:
            skipped_missing_image_url += 1
            message_notes.append(f"{fsn}: skipped missing Product_Image_URL")
            for current in updated_queue_rows:
                if clean_fsn(current.get("FSN", "")) == fsn:
                    current["Search_Status"] = "Needs Image URL"
                    current["Last_Updated"] = now_iso()
                    break
            continue

        if not force and month_image_hash_seen(usage_rows, current_month, provider, fsn, image_url_hash):
            skipped_already_searched += 1
            message_notes.append(f"{fsn}: skipped already searched this month")
            for current in updated_queue_rows:
                if clean_fsn(current.get("FSN", "")) == fsn:
                    current["Search_Status"] = "Already Searched This Month"
                    current["Last_Updated"] = now_iso()
                    break
            continue

        month_calls_used = count_usage_calls_for_month(usage_rows, current_month, provider)
        remaining_safe_calls = max(0, safe_monthly_limit - month_calls_used)
        if month_calls_used >= safe_monthly_limit:
            quota_guard_stopped = True
            message_notes.append("monthly safe limit reached")
            break

        search_payload = search_flipkart_only(image_url=image_url, query=query, config_path=resolve_visual_search_config_path(), use_cache=True)
        api_called = bool(search_payload.get("api_called"))
        if api_called:
            api_called_count += 1

        raw_results = search_payload.get("results", [])
        result_rows = [make_result_row(row, raw_result) for raw_result in raw_results if normalize_text(raw_result.get("Competitor_Link", ""))]
        search_status = "Completed"
        if search_payload["status"] == "NEEDS_CREDENTIALS":
            search_status = "API Pending"
            needs_credentials_count += 1
        elif search_payload["status"] == "ERROR":
            search_status = "Search Error"
        elif not result_rows:
            search_status = "No Flipkart Match Found"
            no_match_count += 1

        if api_called:
            append_usage_log_row(
                {
                    "timestamp": now_iso(),
                    "month": current_month,
                    "provider": provider,
                    "fsn": fsn,
                    "image_url_hash": image_url_hash,
                    "api_called": True,
                    "status": search_payload.get("status", ""),
                    "results_returned": len(result_rows),
                }
            )
            usage_rows.append(
                {
                    "timestamp": now_iso(),
                    "month": current_month,
                    "provider": provider,
                    "fsn": fsn,
                    "image_url_hash": image_url_hash,
                    "api_called": "true",
                    "status": search_payload.get("status", ""),
                    "results_returned": str(len(result_rows)),
                }
            )

        for current in updated_queue_rows:
            if clean_fsn(current.get("FSN", "")) == fsn:
                current["Search_Status"] = search_status
                current["Last_Updated"] = now_iso()
                break
        if result_rows:
            new_results.extend(result_rows)

        status_counter[search_status] += 1
        message_notes.append(f"{fsn}: {search_status}")

        if sleep_seconds > 0 and api_called:
            import time

            time.sleep(sleep_seconds)

    month_calls_used = count_usage_calls_for_month(usage_rows, current_month, provider)
    remaining_safe_calls = max(0, safe_monthly_limit - month_calls_used)
    merged_results = merge_result_rows(existing_results_rows, new_results, processed_fsns if force else [])

    write_local_csv(LOCAL_QUEUE_PATH, QUEUE_PROCESS_HEADERS, updated_queue_rows)
    write_local_csv(LOCAL_RESULTS_PATH, RESULT_HEADERS, merged_results)

    if quota_guard_stopped:
        local_status = "QUOTA_GUARD_STOPPED"
    elif needs_credentials_count == len(processed_fsns) and processed_fsns:
        local_status = "NEEDS_CREDENTIALS"
    elif processed_fsns:
        local_status = "SUCCESS"
    else:
        local_status = "NO_PENDING_ROWS"

    write_local_csv(LOCAL_QUEUE_PATH, QUEUE_PROCESS_HEADERS, updated_queue_rows)
    write_local_csv(LOCAL_RESULTS_PATH, RESULT_HEADERS, merged_results)
    write_output_tab(sheets_service, spreadsheet_id, QUEUE_TAB, QUEUE_PROCESS_HEADERS, updated_queue_rows)
    write_output_tab(sheets_service, spreadsheet_id, RESULTS_TAB, RESULT_HEADERS, merged_results)

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": local_status,
        "processed_fsns": len(processed_fsns),
        "api_called_count": api_called_count,
        "no_match_count": no_match_count,
        "needs_credentials_count": needs_credentials_count,
        "results_rows": len(merged_results),
        "queue_rows": len(updated_queue_rows),
        "message": " | ".join(message_notes)[:1000],
        "config_path": str(resolve_visual_search_config_path()),
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "status",
            "processed_fsns",
            "api_called_count",
            "no_match_count",
            "needs_credentials_count",
            "results_rows",
            "queue_rows",
            "message",
            "config_path",
        ],
        [log_row],
    )

    payload = {
        "status": local_status,
        "monthly_limit": monthly_limit,
        "safe_monthly_limit": safe_monthly_limit,
        "month_calls_used": month_calls_used,
        "remaining_safe_calls": remaining_safe_calls,
        "processed_fsns": len(processed_fsns),
        "api_called_count": api_called_count,
        "visual_result_rows": len(merged_results),
        "skipped_already_searched": skipped_already_searched,
        "skipped_missing_image_url": skipped_missing_image_url,
        "quota_guard_stopped": quota_guard_stopped,
        "log_path": str(LOG_PATH),
        "force_warning": force_warning,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return payload
def main() -> None:
    parser = argparse.ArgumentParser(description="Run Flipkart visual competitor search.")
    parser.add_argument("--max-fsns", type=int, default=5)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--sleep-seconds", type=int, default=3)
    args = parser.parse_args()
    try:
        run_flipkart_visual_competitor_search(max_fsns=args.max_fsns, force=args.force, sleep_seconds=args.sleep_seconds)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                    "tabs_updated": [QUEUE_TAB, RESULTS_TAB],
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
