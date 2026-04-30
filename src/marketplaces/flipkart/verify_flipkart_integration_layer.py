from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.create_looker_studio_sources import (
    LOOKER_ADJUSTED_PROFIT_TAB,
    LOOKER_COMPETITOR_INTELLIGENCE_TAB,
    LOOKER_DEMAND_PROFILE_TAB,
    LOOKER_MODULE_CONFIDENCE_TAB,
    LOOKER_ORDER_ITEM_EXPLORER_TAB,
    LOOKER_REPORT_FORMAT_MONITOR_TAB,
    LOOKER_RUN_COMPARISON_TAB,
    LOOKER_RUN_QUALITY_TAB,
    LOOKER_TABS,
    SPREADSHEET_META_PATH,
)
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, clean_fsn, normalize_text

POST_REFRESH_LOG_PATH = LOG_DIR / "flipkart_post_analysis_refresh_log.csv"
LOOKER_SOURCE_PATH = PROJECT_ROOT / "src" / "marketplaces" / "flipkart" / "create_looker_studio_sources.py"
SYSTEM_HEALTH_SOURCE_PATH = PROJECT_ROOT / "src" / "marketplaces" / "flipkart" / "verify_flipkart_system_health.py"

MANUAL_TABS = [
    "FLIPKART_ACTION_TRACKER",
    "FLIPKART_COST_MASTER",
    "FLIPKART_PRODUCT_AD_PROFILE",
    "FLIPKART_ADS_PLANNER",
    "FLIPKART_MISSING_ACTIVE_LISTINGS",
    "FLIPKART_LISTING_STATUS_ISSUES",
]

KEY_GENERATED_TABS = [
    "FLIPKART_ADJUSTMENTS_LEDGER",
    "FLIPKART_ADJUSTED_PROFIT",
    "FLIPKART_RUN_COMPARISON",
    "FLIPKART_FSN_RUN_COMPARISON",
    "FLIPKART_REPORT_FORMAT_MONITOR",
    "FLIPKART_REPORT_FORMAT_ISSUES",
    "FLIPKART_RUN_QUALITY_SCORE",
    "FLIPKART_RUN_QUALITY_BREAKDOWN",
    "FLIPKART_MODULE_CONFIDENCE",
    "FLIPKART_DATA_GAP_SUMMARY",
    "GOOGLE_KEYWORD_METRICS_CACHE",
    "PRODUCT_TYPE_DEMAND_PROFILE",
    "FLIPKART_COMPETITOR_SEARCH_QUEUE",
    "FLIPKART_VISUAL_COMPETITOR_RESULTS",
    "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE",
    "FLIPKART_ORDER_ITEM_EXPLORER",
]

LOOKER_TABS_FOR_INTEGRATION = [
    LOOKER_RUN_COMPARISON_TAB,
    LOOKER_ADJUSTED_PROFIT_TAB,
    LOOKER_REPORT_FORMAT_MONITOR_TAB,
    LOOKER_RUN_QUALITY_TAB,
    LOOKER_MODULE_CONFIDENCE_TAB,
    LOOKER_DEMAND_PROFILE_TAB,
    LOOKER_COMPETITOR_INTELLIGENCE_TAB,
    LOOKER_ORDER_ITEM_EXPLORER_TAB,
]


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> List[Dict[str, Any]]:
    def _fetch() -> Any:
        return sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A1:ZZ").execute()

    try:
        response = retry(_fetch)
    except HttpError as exc:
        message = str(exc)
        if getattr(exc.resp, "status", None) in {400, 404} or "Unable to parse range" in message:
            return []
        raise
    rows = response.get("values", [])
    if not rows:
        return []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, Any]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return data


def count_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


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


def read_tables_batch(sheets_service, spreadsheet_id: str, tab_names: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not tab_names:
        return {}

    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=[f"{tab}!A1:ZZ" for tab in tab_names])
        .execute()
    )
    tables: Dict[str, List[Dict[str, Any]]] = {}
    for value_range in response.get("valueRanges", []):
        range_name = str(value_range.get("range", ""))
        tab_name = range_name.split("!", 1)[0]
        rows = value_range.get("values", [])
        if not rows:
            tables[tab_name] = []
            continue
        headers = [str(cell) for cell in rows[0]]
        tables[tab_name] = [
            {headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))}
            for row in rows[1:]
        ]
    return tables


def read_source_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def load_latest_runner_summary() -> Dict[str, Any]:
    if not POST_REFRESH_LOG_PATH.exists():
        return {}
    try:
        with POST_REFRESH_LOG_PATH.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
    except Exception:
        return {}
    if not rows:
        return {}
    latest = rows[-1]
    details_text = normalize_text(latest.get("details", ""))
    if not details_text:
        return dict(latest)
    try:
        details = json.loads(details_text)
    except json.JSONDecodeError:
        details = {}
    summary = dict(latest)
    if isinstance(details, dict):
        summary.update(details)
    return summary


def _load_spreadsheet_id() -> str:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    return json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]


def verify_flipkart_integration_layer() -> Dict[str, Any]:
    spreadsheet_id = _load_spreadsheet_id()
    sheets_service, _, _ = build_services()
    metadata = retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))")
        .execute()
    )
    available_tabs = {
        str(sheet.get("properties", {}).get("title", ""))
        for sheet in metadata.get("sheets", [])
        if str(sheet.get("properties", {}).get("title", ""))
    }

    runner_summary = load_latest_runner_summary()
    looker_source_text = read_source_text(LOOKER_SOURCE_PATH)
    system_health_source_text = read_source_text(SYSTEM_HEALTH_SOURCE_PATH)

    requested_tabs = sorted(set(MANUAL_TABS + KEY_GENERATED_TABS + LOOKER_TABS_FOR_INTEGRATION + ["FLIPKART_SKU_ANALYSIS"]))
    tabs_to_read = [tab for tab in requested_tabs if tab in available_tabs]
    tables = read_tables_batch(sheets_service, spreadsheet_id, tabs_to_read)

    def table(tab_name: str) -> List[Dict[str, Any]]:
        return tables.get(tab_name, [])

    sku_analysis_rows = table("FLIPKART_SKU_ANALYSIS")
    module_confidence_rows = table("FLIPKART_MODULE_CONFIDENCE")
    adjusted_profit_rows = table("FLIPKART_ADJUSTED_PROFIT")
    report_format_rows = table("FLIPKART_REPORT_FORMAT_MONITOR")
    report_format_issue_rows = table("FLIPKART_REPORT_FORMAT_ISSUES")
    run_quality_rows = table("FLIPKART_RUN_QUALITY_SCORE")
    run_quality_breakdown_rows = table("FLIPKART_RUN_QUALITY_BREAKDOWN")
    demand_profile_rows = table("PRODUCT_TYPE_DEMAND_PROFILE")
    competitor_price_rows = table("FLIPKART_COMPETITOR_PRICE_INTELLIGENCE")
    competitor_queue_rows = table("FLIPKART_COMPETITOR_SEARCH_QUEUE")
    visual_results_rows = table("FLIPKART_VISUAL_COMPETITOR_RESULTS")
    keyword_cache_rows = table("GOOGLE_KEYWORD_METRICS_CACHE")

    sku_fsns = {clean_fsn(row.get("FSN", "")) for row in sku_analysis_rows if clean_fsn(row.get("FSN", ""))}
    module_confidence_fsns = {clean_fsn(row.get("FSN", "")) for row in module_confidence_rows if clean_fsn(row.get("FSN", ""))}

    runner_external_google_ads_called = bool(runner_summary.get("external_google_ads_called", False))
    runner_external_visual_search_called = bool(runner_summary.get("external_visual_search_called", False))
    runner_steps_run = [str(step) for step in runner_summary.get("steps_run", [])]
    runner_default_safe = (
        not runner_external_google_ads_called
        and not runner_external_visual_search_called
        and "refresh_google_keyword_metrics" not in runner_steps_run
        and "run_flipkart_visual_competitor_search" not in runner_steps_run
        and "sync_flipkart_run_archive_to_drive" not in runner_steps_run
    )

    required_health_keys = [
        "adjusted_profit_has_rows",
        "run_comparison_has_rows",
        "fsn_run_comparison_has_rows",
        "report_format_monitor_has_rows",
        "run_quality_score_has_rows",
        "run_quality_breakdown_has_rows",
        "module_confidence_has_rows",
        "data_gap_summary_has_rows",
        "keyword_cache_tab_exists",
        "demand_profile_has_rows",
        "competitor_intelligence_has_rows",
        "order_item_explorer_has_rows",
        "looker_order_item_explorer_has_rows",
    ]
    looker_required_tabs = LOOKER_TABS_FOR_INTEGRATION
    looker_source_has_new_tabs = all(tab_name in looker_source_text for tab_name in looker_required_tabs)
    system_health_source_has_new_checks = all(key in system_health_source_text for key in required_health_keys)

    warnings: List[str] = []
    if not keyword_cache_rows:
        warnings.append("keyword cache rows are pending")
    elif all(normalize_text(row.get("Cache_Status", "")).upper() == "PENDING" for row in keyword_cache_rows):
        warnings.append("keyword cache rows are pending")
    if competitor_price_rows and all(normalize_text(row.get("Competition_Risk_Level", "")) == "Not Enough Data" for row in competitor_price_rows):
        warnings.append("competitor intelligence rows are Not Enough Data")
    if report_format_issue_rows and any(normalize_text(row.get("Severity", "")) == "Critical" for row in report_format_issue_rows):
        warnings.append("report format critical issues present")

    row_counts = {
        "FLIPKART_SKU_ANALYSIS": count_rows(sku_analysis_rows),
        "FLIPKART_MODULE_CONFIDENCE": count_rows(module_confidence_rows),
        "FLIPKART_ADJUSTED_PROFIT": count_rows(adjusted_profit_rows),
        "FLIPKART_REPORT_FORMAT_MONITOR": count_rows(report_format_rows),
        "FLIPKART_REPORT_FORMAT_ISSUES": count_rows(report_format_issue_rows),
        "FLIPKART_RUN_QUALITY_SCORE": count_rows(run_quality_rows),
        "FLIPKART_RUN_QUALITY_BREAKDOWN": count_rows(run_quality_breakdown_rows),
        "PRODUCT_TYPE_DEMAND_PROFILE": count_rows(demand_profile_rows),
        "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE": count_rows(competitor_price_rows),
        "FLIPKART_COMPETITOR_SEARCH_QUEUE": count_rows(competitor_queue_rows),
        "FLIPKART_VISUAL_COMPETITOR_RESULTS": count_rows(visual_results_rows),
        "GOOGLE_KEYWORD_METRICS_CACHE": count_rows(keyword_cache_rows),
        LOOKER_RUN_COMPARISON_TAB: count_rows(table(LOOKER_RUN_COMPARISON_TAB)),
        LOOKER_ADJUSTED_PROFIT_TAB: count_rows(table(LOOKER_ADJUSTED_PROFIT_TAB)),
        LOOKER_REPORT_FORMAT_MONITOR_TAB: count_rows(table(LOOKER_REPORT_FORMAT_MONITOR_TAB)),
        LOOKER_RUN_QUALITY_TAB: count_rows(table(LOOKER_RUN_QUALITY_TAB)),
        LOOKER_MODULE_CONFIDENCE_TAB: count_rows(table(LOOKER_MODULE_CONFIDENCE_TAB)),
        LOOKER_DEMAND_PROFILE_TAB: count_rows(table(LOOKER_DEMAND_PROFILE_TAB)),
        LOOKER_COMPETITOR_INTELLIGENCE_TAB: count_rows(table(LOOKER_COMPETITOR_INTELLIGENCE_TAB)),
    }

    looker_tabs_exist = all(tab in available_tabs for tab in looker_required_tabs)
    manual_tabs_exist = all(tab in available_tabs for tab in MANUAL_TABS)
    key_generated_tabs_exist = all(tab in available_tabs for tab in KEY_GENERATED_TABS)
    module_confidence_matches_sku = module_confidence_fsns == sku_fsns and bool(module_confidence_fsns)
    run_quality_has_one_row = row_counts["FLIPKART_RUN_QUALITY_SCORE"] == 1 and row_counts["FLIPKART_RUN_QUALITY_BREAKDOWN"] > 0
    competitor_intelligence_has_rows = row_counts["FLIPKART_COMPETITOR_PRICE_INTELLIGENCE"] > 0 and row_counts[LOOKER_COMPETITOR_INTELLIGENCE_TAB] > 0
    adjusted_profit_has_rows = row_counts["FLIPKART_ADJUSTED_PROFIT"] > 0 and row_counts[LOOKER_ADJUSTED_PROFIT_TAB] > 0
    report_format_monitor_has_rows = row_counts["FLIPKART_REPORT_FORMAT_MONITOR"] > 0 and row_counts[LOOKER_REPORT_FORMAT_MONITOR_TAB] > 0
    demand_profile_has_rows = row_counts["PRODUCT_TYPE_DEMAND_PROFILE"] > 0 and row_counts[LOOKER_DEMAND_PROFILE_TAB] > 0
    default_refresh_safe = runner_default_safe and not runner_summary.get("drive_archive_synced", False)

    checks = {
        "runner_script_exists": (PROJECT_ROOT / "src" / "marketplaces" / "flipkart" / "run_flipkart_post_analysis_refresh.py").exists(),
        "runner_default_safe": default_refresh_safe,
        "looker_source_has_new_tabs": looker_source_has_new_tabs,
        "system_health_source_has_new_checks": system_health_source_has_new_checks,
        "new_looker_tabs_exist": looker_tabs_exist,
        "manual_tabs_exist": manual_tabs_exist,
        "key_generated_tabs_exist": key_generated_tabs_exist,
        "module_confidence_matches_sku_analysis": module_confidence_matches_sku,
        "competitor_intelligence_has_rows": competitor_intelligence_has_rows,
        "run_quality_has_one_row": run_quality_has_one_row,
        "adjusted_profit_has_rows": adjusted_profit_has_rows,
        "report_format_monitor_has_rows": report_format_monitor_has_rows,
        "demand_profile_has_rows": demand_profile_has_rows,
    }

    status = "PASS_WITH_WARNINGS" if all(checks.values()) and warnings else ("PASS" if all(checks.values()) else "FAIL")
    tabs_checked = sorted(set(MANUAL_TABS + KEY_GENERATED_TABS + LOOKER_TABS_FOR_INTEGRATION + ["FLIPKART_SKU_ANALYSIS"]))

    return {
        "status": status,
        "tabs_checked": tabs_checked,
        "row_counts": row_counts,
        "warnings": warnings,
        "checks": checks,
        "runner_summary": {
            "steps_run": runner_steps_run,
            "external_google_ads_called": runner_summary.get("external_google_ads_called", False),
            "external_visual_search_called": runner_summary.get("external_visual_search_called", False),
            "drive_archive_synced": runner_summary.get("drive_archive_synced", False),
            "tabs_refreshed": runner_summary.get("tabs_refreshed", []),
            "status": runner_summary.get("status", ""),
        },
        "looker_summary": {
            "status": "NOT_RUN",
            "tabs_checked": LOOKER_TABS_FOR_INTEGRATION,
        },
        "system_health": {
            "status": "NOT_RUN",
            "checks": {key: key in system_health_source_text for key in required_health_keys},
        },
    }


def main() -> None:
    try:
        payload = verify_flipkart_integration_layer()
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        if payload["status"] not in {"PASS", "PASS_WITH_WARNINGS"}:
            raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
