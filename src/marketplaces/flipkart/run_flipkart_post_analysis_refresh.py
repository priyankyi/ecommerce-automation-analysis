from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Sequence

from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, append_csv_log, ensure_directories, now_iso

LOG_PATH = LOG_DIR / "flipkart_post_analysis_refresh_log.csv"

LOG_HEADERS = [
    "timestamp",
    "status",
    "step_name",
    "details",
    "log_path",
]

_RUNNER_VISUAL_MAX_FSNS = 5
RUN_MODES = ("full", "quick", "looker-only", "competitor-only", "cogs-only", "actions-only", "health-only")
IN_PROCESS_STEPS = {"create_looker_studio_sources"}

STEP_TAB_MAP: Dict[str, List[str]] = {
    "update_flipkart_profit_after_cogs": ["FLIPKART_SKU_ANALYSIS"],
    "create_flipkart_return_comments_analysis": ["FLIPKART_RETURN_COMMENTS", "FLIPKART_RETURN_ISSUE_SUMMARY", "FLIPKART_RETURN_REASON_PIVOT"],
    "create_flipkart_return_intelligence_v2": [
        "FLIPKART_RETURN_ALL_DETAILS",
        "FLIPKART_CUSTOMER_RETURN_COMMENTS",
        "FLIPKART_COURIER_RETURN_COMMENTS",
        "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY",
        "FLIPKART_COURIER_RETURN_SUMMARY",
        "FLIPKART_RETURN_TYPE_PIVOT",
    ],
    "create_flipkart_ads_planner_foundation": ["FLIPKART_PRODUCT_AD_PROFILE", "GOOGLE_ADS_KEYWORD_SEEDS", "GOOGLE_KEYWORD_METRICS_CACHE", "PRODUCT_TYPE_DEMAND_PROFILE", "FLIPKART_ADS_PLANNER"],
    "create_flipkart_ads_mapping": ["FLIPKART_ADS_MASTER", "FLIPKART_ADS_MAPPING_ISSUES", "FLIPKART_ADS_SUMMARY_BY_FSN", "FLIPKART_ADS_PLANNER"],
    "update_flipkart_ads_recommendations": ["FLIPKART_ADS_PLANNER"],
    "create_flipkart_listing_presence_workflow": ["FLIPKART_LISTING_PRESENCE", "FLIPKART_MISSING_ACTIVE_LISTINGS", "FLIPKART_LISTING_STATUS_ISSUES", "FLIPKART_SKU_ANALYSIS"],
    "create_flipkart_adjustment_ledger": ["FLIPKART_ADJUSTMENTS_LEDGER"],
    "apply_flipkart_adjustments": ["FLIPKART_ADJUSTMENTS_LEDGER", "FLIPKART_ADJUSTED_PROFIT", "LOOKER_FLIPKART_ADJUSTED_PROFIT", "FLIPKART_SKU_ANALYSIS"],
    "check_flipkart_report_format_drift": ["FLIPKART_REPORT_FORMAT_MONITOR", "FLIPKART_REPORT_FORMAT_ISSUES", "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR"],
    "create_flipkart_alerts_and_tasks": ["FLIPKART_ALERTS_GENERATED", "FLIPKART_ACTION_TRACKER", "FLIPKART_ACTIVE_TASKS"],
    "create_flipkart_dashboard": ["FLIPKART_DASHBOARD", "FLIPKART_DASHBOARD_DATA", "FLIPKART_TOP_ALERTS", "FLIPKART_TOP_RETURN_ISSUES", "FLIPKART_ACTION_SUMMARY"],
    "create_flipkart_fsn_drilldown": ["FLIPKART_FSN_DRILLDOWN"],
    "create_flipkart_run_comparison": ["FLIPKART_RUN_COMPARISON", "FLIPKART_FSN_RUN_COMPARISON", "LOOKER_FLIPKART_RUN_COMPARISON"],
    "create_flipkart_run_quality_score": ["FLIPKART_RUN_QUALITY_SCORE", "FLIPKART_RUN_QUALITY_BREAKDOWN", "LOOKER_FLIPKART_RUN_QUALITY_SCORE"],
    "create_flipkart_module_confidence": ["FLIPKART_MODULE_CONFIDENCE", "FLIPKART_DATA_GAP_SUMMARY", "LOOKER_FLIPKART_MODULE_CONFIDENCE"],
    "refresh_google_keyword_metrics": ["GOOGLE_ADS_KEYWORD_SEEDS", "GOOGLE_KEYWORD_METRICS_CACHE", "PRODUCT_TYPE_DEMAND_PROFILE"],
    "update_product_type_demand_profile": ["PRODUCT_TYPE_DEMAND_PROFILE"],
    "run_flipkart_visual_competitor_search": ["FLIPKART_COMPETITOR_SEARCH_QUEUE", "FLIPKART_VISUAL_COMPETITOR_RESULTS"],
    "create_flipkart_competitor_price_intelligence": ["FLIPKART_COMPETITOR_PRICE_INTELLIGENCE", "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE"],
    "create_flipkart_order_item_explorer": [
        "FLIPKART_ORDER_ITEM_EXPLORER",
        "FLIPKART_ORDER_ITEM_MASTER",
        "FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
        "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER",
        "LOOKER_FLIPKART_ORDER_ITEM_MASTER",
        "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
    ],
    "create_looker_studio_sources": [
        "LOOKER_FLIPKART_EXECUTIVE_SUMMARY",
        "LOOKER_FLIPKART_FSN_METRICS",
        "LOOKER_FLIPKART_ALERTS",
        "LOOKER_FLIPKART_ACTIONS",
        "LOOKER_FLIPKART_ADS",
        "LOOKER_FLIPKART_RETURNS",
        "LOOKER_FLIPKART_LISTINGS",
        "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER",
        "LOOKER_FLIPKART_RUN_COMPARISON",
        "LOOKER_FLIPKART_ADJUSTED_PROFIT",
        "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR",
        "LOOKER_FLIPKART_RUN_QUALITY_SCORE",
        "LOOKER_FLIPKART_MODULE_CONFIDENCE",
        "LOOKER_FLIPKART_DEMAND_PROFILE",
        "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE",
        "LOOKER_FLIPKART_ORDER_ITEM_MASTER",
        "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
    ],
}

STEP_MODULES: Dict[str, str] = {
    "update_flipkart_profit_after_cogs": "src.marketplaces.flipkart.update_flipkart_profit_after_cogs",
    "create_flipkart_return_comments_analysis": "src.marketplaces.flipkart.create_flipkart_return_comments_analysis",
    "create_flipkart_return_intelligence_v2": "src.marketplaces.flipkart.create_flipkart_return_intelligence_v2",
    "create_flipkart_ads_planner_foundation": "src.marketplaces.flipkart.create_flipkart_ads_planner_foundation",
    "create_flipkart_ads_mapping": "src.marketplaces.flipkart.create_flipkart_ads_mapping",
    "update_flipkart_ads_recommendations": "src.marketplaces.flipkart.update_flipkart_ads_recommendations",
    "create_flipkart_listing_presence_workflow": "src.marketplaces.flipkart.create_flipkart_listing_presence_workflow",
    "create_flipkart_adjustment_ledger": "src.marketplaces.flipkart.create_flipkart_adjustment_ledger",
    "apply_flipkart_adjustments": "src.marketplaces.flipkart.apply_flipkart_adjustments",
    "check_flipkart_report_format_drift": "src.marketplaces.flipkart.check_flipkart_report_format_drift",
    "create_flipkart_alerts_and_tasks": "src.marketplaces.flipkart.create_flipkart_alerts_and_tasks",
    "create_flipkart_dashboard": "src.marketplaces.flipkart.create_flipkart_dashboard",
    "create_flipkart_fsn_drilldown": "src.marketplaces.flipkart.create_flipkart_fsn_drilldown",
    "create_flipkart_run_comparison": "src.marketplaces.flipkart.create_flipkart_run_comparison",
    "create_flipkart_run_quality_score": "src.marketplaces.flipkart.create_flipkart_run_quality_score",
    "create_flipkart_module_confidence": "src.marketplaces.flipkart.create_flipkart_module_confidence",
    "refresh_google_keyword_metrics": "src.marketplaces.flipkart.refresh_google_keyword_metrics",
    "update_product_type_demand_profile": "src.marketplaces.flipkart.update_product_type_demand_profile",
    "run_flipkart_visual_competitor_search": "src.marketplaces.flipkart.run_flipkart_visual_competitor_search",
    "create_flipkart_competitor_price_intelligence": "src.marketplaces.flipkart.create_flipkart_competitor_price_intelligence",
    "create_flipkart_order_item_explorer": "src.marketplaces.flipkart.create_flipkart_order_item_explorer",
    "create_looker_studio_sources": "src.marketplaces.flipkart.create_looker_studio_sources",
    "sync_flipkart_run_archive_to_drive": "src.marketplaces.flipkart.sync_flipkart_run_archive_to_drive",
    "verify_flipkart_cogs_layer": "src.marketplaces.flipkart.verify_flipkart_cogs_layer",
    "verify_flipkart_alerts_tasks": "src.marketplaces.flipkart.verify_flipkart_alerts_tasks",
    "verify_flipkart_return_comments_analysis": "src.marketplaces.flipkart.verify_flipkart_return_comments_analysis",
    "verify_flipkart_ads_planner_foundation": "src.marketplaces.flipkart.verify_flipkart_ads_planner_foundation",
    "verify_flipkart_ads_mapping": "src.marketplaces.flipkart.verify_flipkart_ads_mapping",
    "verify_flipkart_ads_recommendations": "src.marketplaces.flipkart.verify_flipkart_ads_recommendations",
    "verify_flipkart_listing_presence_workflow": "src.marketplaces.flipkart.verify_flipkart_listing_presence_workflow",
    "verify_flipkart_adjustment_ledger": "src.marketplaces.flipkart.verify_flipkart_adjustment_ledger",
    "verify_flipkart_report_format_monitor": "src.marketplaces.flipkart.verify_flipkart_report_format_monitor",
    "verify_flipkart_run_comparison": "src.marketplaces.flipkart.verify_flipkart_run_comparison",
    "verify_flipkart_run_quality_score": "src.marketplaces.flipkart.verify_flipkart_run_quality_score",
    "verify_flipkart_module_confidence": "src.marketplaces.flipkart.verify_flipkart_module_confidence",
    "verify_google_keyword_metrics_cache": "src.marketplaces.flipkart.verify_google_keyword_metrics_cache",
    "verify_flipkart_competitor_intelligence": "src.marketplaces.flipkart.verify_flipkart_competitor_intelligence",
    "verify_looker_studio_sources": "src.marketplaces.flipkart.verify_looker_studio_sources",
    "verify_flipkart_integration_layer": "src.marketplaces.flipkart.verify_flipkart_integration_layer",
}
HEALTH_CHECK_MODULE = "src.marketplaces.flipkart.verify_flipkart_system_health"

STEP_ORDER: List[str] = [
    "update_flipkart_profit_after_cogs",
    "create_flipkart_return_comments_analysis",
    "create_flipkart_return_intelligence_v2",
    "create_flipkart_ads_planner_foundation",
    "create_flipkart_ads_mapping",
    "update_flipkart_ads_recommendations",
    "create_flipkart_listing_presence_workflow",
    "create_flipkart_adjustment_ledger",
    "apply_flipkart_adjustments",
    "check_flipkart_report_format_drift",
    "create_flipkart_alerts_and_tasks",
    "create_flipkart_dashboard",
    "create_flipkart_fsn_drilldown",
    "create_flipkart_run_comparison",
    "create_flipkart_run_quality_score",
    "create_flipkart_module_confidence",
    "update_product_type_demand_profile",
    "create_flipkart_competitor_price_intelligence",
    "create_flipkart_order_item_explorer",
    "create_looker_studio_sources",
]

MODE_STEP_ORDER: Dict[str, List[str]] = {
    "quick": [
        "create_flipkart_return_comments_analysis",
        "create_flipkart_return_intelligence_v2",
        "update_product_type_demand_profile",
        "create_flipkart_competitor_price_intelligence",
        "create_flipkart_order_item_explorer",
        "create_looker_studio_sources",
        "verify_flipkart_integration_layer",
        "verify_flipkart_system_health",
    ],
    "looker-only": [
        "create_looker_studio_sources",
        "verify_flipkart_integration_layer",
    ],
    "competitor-only": [
        "create_flipkart_competitor_price_intelligence",
        "create_looker_studio_sources",
        "verify_flipkart_competitor_intelligence",
        "verify_flipkart_integration_layer",
    ],
    "cogs-only": [
        "update_flipkart_profit_after_cogs",
        "apply_flipkart_adjustments",
        "create_flipkart_alerts_and_tasks",
        "create_flipkart_dashboard",
        "create_flipkart_run_quality_score",
        "create_flipkart_module_confidence",
        "create_looker_studio_sources",
        "verify_flipkart_system_health",
    ],
    "actions-only": [
        "create_flipkart_alerts_and_tasks",
        "create_flipkart_dashboard",
        "create_looker_studio_sources",
        "verify_flipkart_integration_layer",
    ],
    "health-only": [
        "verify_flipkart_system_health",
    ],
}

HEALTH_CHECK_STEP = "verify_flipkart_system_health"
DETAILED_VERIFY_STEPS: List[str] = [
    "verify_flipkart_cogs_layer",
    "verify_flipkart_alerts_tasks",
    "verify_flipkart_return_comments_analysis",
    "verify_flipkart_ads_planner_foundation",
    "verify_flipkart_ads_mapping",
    "verify_flipkart_ads_recommendations",
    "verify_flipkart_listing_presence_workflow",
    "verify_flipkart_adjustment_ledger",
    "verify_flipkart_report_format_monitor",
    "verify_flipkart_run_comparison",
    "verify_flipkart_run_quality_score",
    "verify_flipkart_module_confidence",
    "verify_google_keyword_metrics_cache",
    "verify_flipkart_competitor_intelligence",
    "verify_looker_studio_sources",
    "verify_flipkart_integration_layer",
]
OPTIONAL_DETAILED_VERIFY_STEPS: List[str] = [
    "verify_flipkart_return_issue_integration",
]

OPTIONAL_EXTERNAL_STEPS = {
    "refresh_google_keyword_metrics",
    "run_flipkart_visual_competitor_search",
}

WARNING_STATUSES = {
    "WARNING",
    "PASS_WITH_WARNINGS",
    "SUCCESS_WITH_WARNINGS",
    "API_ACCESS_NOT_READY",
    "NEEDS_CREDENTIALS",
    "QUOTA_GUARD_STOPPED",
    "RETRY_LATER",
}

MANUAL_COLUMNS: Dict[str, Sequence[str]] = {
    "FLIPKART_ACTION_TRACKER": (
        "Owner",
        "Status",
        "Action_Taken",
        "Action_Date",
        "Expected_Impact",
        "Review_After_Date",
        "Review_After_Run_ID",
        "Evidence_Link",
        "Resolution_Notes",
    ),
    "FLIPKART_COST_MASTER": ("Cost_Price", "Packaging_Cost", "Other_Cost", "COGS_Status", "Remarks"),
    "FLIPKART_PRODUCT_AD_PROFILE": ("Manual_Product_Type", "Manual_Seasonality_Tag", "Manual_Override_Remarks"),
    "FLIPKART_ADS_PLANNER": ("Manual_Final_Ads_Decision", "Manual_Ads_Remarks"),
    "FLIPKART_MISSING_ACTIVE_LISTINGS": ("Owner", "Status", "Remarks"),
    "FLIPKART_LISTING_STATUS_ISSUES": ("Owner", "Status", "Remarks"),
}


def _json_text(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False)


def _append_log_row(payload: Dict[str, Any]) -> None:
    append_csv_log(LOG_PATH, LOG_HEADERS, [payload])


def _step_expected_status(step_name: str) -> Sequence[str]:
    if step_name == HEALTH_CHECK_STEP or step_name.startswith("verify_"):
        return ("PASS", "PASS_WITH_WARNINGS", "WARNING", "RETRY_LATER")
    if step_name in OPTIONAL_EXTERNAL_STEPS:
        return ("SUCCESS", "SUCCESS_WITH_WARNINGS", "WARNING", "API_ACCESS_NOT_READY", "NEEDS_CREDENTIALS", "QUOTA_GUARD_STOPPED", "NO_PENDING_ROWS")
    return ("SUCCESS", "SUCCESS_WITH_WARNINGS")


def _is_google_sheets_429(message: str) -> bool:
    normalized = message.lower()
    return "429" in normalized and "google" in normalized and "sheet" in normalized


def _is_quota_limited_message(message: str) -> bool:
    normalized = message.lower()
    return any(
        token in normalized
        for token in (
            "quota exceeded",
            "rate_limit_exceeded",
            "read requests per minute",
            "quota_limited",
        )
    ) or _is_google_sheets_429(message)


def _verification_warning_payload(step_name: str, payload: Dict[str, Any]) -> Dict[str, Any] | None:
    if step_name == "refresh_google_keyword_metrics":
        status_value = str(payload.get("status", "")).upper()
        if status_value == "API_ACCESS_NOT_READY":
            warning_payload = dict(payload)
            warning_payload["status"] = "WARNING"
            warning_payload["message"] = "Google Ads API access pending"
            warning_payload["warning_type"] = "external_api_pending"
            return warning_payload
    if step_name == "run_flipkart_visual_competitor_search":
        status_value = str(payload.get("status", "")).upper()
        if status_value == "NEEDS_CREDENTIALS":
            warning_payload = dict(payload)
            warning_payload["status"] = "WARNING"
            warning_payload["message"] = "visual search credentials/image URLs missing"
            warning_payload["warning_type"] = "external_api_pending"
            return warning_payload
    if step_name != HEALTH_CHECK_STEP and not step_name.startswith("verify_"):
        return None
    if str(payload.get("status", "")).upper() == "RETRY_LATER":
        warning_payload = dict(payload)
        warning_payload["status"] = "WARNING"
        warning_payload["message"] = "wait 5 minutes and rerun health check"
        warning_payload["warning_type"] = "quota_limited_verification"
        return warning_payload
    if str(payload.get("status", "")).upper() == "WARNING":
        return payload
    message = str(payload.get("message", ""))
    error_type = str(payload.get("error_type", ""))
    if _is_google_sheets_429(message) or (error_type == "HttpError" and "429" in message):
        warning_payload = dict(payload)
        warning_payload["status"] = "WARNING"
        warning_payload["message"] = "wait 5 minutes and rerun health check"
        warning_payload["warning_type"] = "quota_limited_verification"
        return warning_payload
    return None


def _build_detailed_verify_steps() -> List[str]:
    detailed_steps = list(DETAILED_VERIFY_STEPS)
    for step_name in OPTIONAL_DETAILED_VERIFY_STEPS:
        if step_name in STEP_MODULES:
            detailed_steps.append(step_name)
    return detailed_steps


def _build_step_order(
    *,
    refresh_keywords: bool,
    run_visual_search: bool,
    sync_drive_archive: bool,
) -> List[str]:
    step_names = list(STEP_ORDER)
    if refresh_keywords:
        insert_at = step_names.index("update_product_type_demand_profile")
        step_names.insert(insert_at, "refresh_google_keyword_metrics")
    if run_visual_search:
        insert_at = step_names.index("create_flipkart_competitor_price_intelligence")
        step_names.insert(insert_at, "run_flipkart_visual_competitor_search")
    if sync_drive_archive:
        step_names.append("sync_flipkart_run_archive_to_drive")
    return step_names


def _build_execution_schedule(
    *,
    mode: str,
    refresh_keywords: bool,
    run_visual_search: bool,
    sync_drive_archive: bool,
    verify_all: bool,
    skip_verification: bool,
    health_delay_seconds: float,
) -> List[str]:
    if mode == "full":
        step_names = _build_step_order(
            refresh_keywords=refresh_keywords,
            run_visual_search=run_visual_search,
            sync_drive_archive=sync_drive_archive,
        )
        if not skip_verification:
            if health_delay_seconds > 0:
                step_names.append("__health_delay__")
            if verify_all:
                step_names.extend(_build_detailed_verify_steps())
            step_names.append(HEALTH_CHECK_STEP)
        return step_names
    step_names = list(MODE_STEP_ORDER[mode])
    if mode == "competitor-only" and run_visual_search:
        insert_at = step_names.index("create_flipkart_competitor_price_intelligence")
        step_names.insert(insert_at, "run_flipkart_visual_competitor_search")
    return step_names


def _is_warning_payload(step_name: str, payload: Dict[str, Any]) -> bool:
    status_value = str(payload.get("status", "")).upper()
    if status_value in {"WARNING", "PASS_WITH_WARNINGS", "SUCCESS_WITH_WARNINGS"}:
        return True
    if step_name == HEALTH_CHECK_STEP and status_value == "PASS":
        warnings = payload.get("warnings", [])
        return bool(warnings)
    return False


def _warning_message(step_name: str, payload: Dict[str, Any]) -> str:
    if step_name == "refresh_google_keyword_metrics":
        return str(payload.get("message", "")) or "Google Ads API access pending"
    if step_name == "run_flipkart_visual_competitor_search":
        return str(payload.get("message", "")) or "visual search credentials/image URLs missing"
    if step_name == HEALTH_CHECK_STEP:
        warnings = payload.get("warnings", [])
        if isinstance(warnings, list) and warnings:
            return " | ".join(str(item) for item in warnings if str(item))
    warnings = payload.get("warnings", [])
    if isinstance(warnings, list) and warnings:
        return " | ".join(str(item) for item in warnings if str(item))
    return str(payload.get("message", "")) or f"{step_name} completed with warnings"


def _allowed_warning_step(step_name: str) -> bool:
    return step_name in OPTIONAL_EXTERNAL_STEPS or step_name == HEALTH_CHECK_STEP


def _resolve_step_module_name(step_name: str) -> str:
    if step_name == HEALTH_CHECK_STEP:
        return HEALTH_CHECK_MODULE
    module_name = STEP_MODULES.get(step_name)
    if module_name is None:
        raise ValueError(f"Unknown Flipkart step: {step_name}")
    return module_name


def _ordered_tabs(step_names: Sequence[str], step_payloads: Dict[str, Dict[str, Any]]) -> List[str]:
    ordered: List[str] = []
    for step_name in step_names:
        tabs = step_payloads.get(step_name, {}).get("tabs_updated")
        if tabs is None:
            tabs = step_payloads.get(step_name, {}).get("dashboard_tabs_updated")
        if not tabs:
            tabs = STEP_TAB_MAP.get(step_name, [])
        for tab_name in tabs:
            if tab_name not in ordered:
                ordered.append(tab_name)
    return ordered


def _get_sheet_headers_batch(spreadsheet_id: str, tab_names: Sequence[str]) -> Dict[str, List[str]]:
    from src.auth_google import build_services

    sheets_service, _, _ = build_services()
    ranges = [f"{tab_name}!A1:ZZ" for tab_name in tab_names]
    response = sheets_service.spreadsheets().values().batchGet(spreadsheetId=spreadsheet_id, ranges=ranges).execute()
    headers_by_tab: Dict[str, List[str]] = {}
    for value_range in response.get("valueRanges", []):
        range_name = str(value_range.get("range", ""))
        tab_name = range_name.split("!", 1)[0]
        values = value_range.get("values", [])
        headers_by_tab[tab_name] = [str(cell) for cell in values[0]] if values else []
    return headers_by_tab


def _run_step_subprocess(step_name: str) -> Dict[str, Any]:
    module_name = _resolve_step_module_name(step_name)
    if step_name in IN_PROCESS_STEPS:
        module = importlib.import_module(module_name)
        step_func = getattr(module, step_name, None)
        if step_func is None:
            raise AttributeError(f"{module_name} does not expose {step_name}()")
        payload = step_func()
        if not isinstance(payload, dict):
            raise RuntimeError(f"{step_name} returned a non-dict payload")
        payload = dict(payload)
        payload["__returncode"] = 0
        payload["__stderr"] = ""
        warning_payload = _verification_warning_payload(step_name, payload)
        if warning_payload is not None:
            warning_payload["__returncode"] = 0
            warning_payload["__stderr"] = ""
            return warning_payload
        return payload
    repo_root = Path(__file__).resolve().parents[3]
    extra_args: List[str] = []
    if step_name == "run_flipkart_visual_competitor_search":
        extra_args = ["--max-fsns", str(_RUNNER_VISUAL_MAX_FSNS)]
    with TemporaryDirectory(prefix=f"{step_name}_") as temp_dir:
        temp_path = Path(temp_dir)
        stdout_path = temp_path / "stdout.txt"
        stderr_path = temp_path / "stderr.txt"
        with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
            completed = subprocess.run(
                [sys.executable, "-m", module_name, *extra_args],
                cwd=str(repo_root),
                stdout=stdout_handle,
                stderr=stderr_handle,
                check=False,
            )
        stdout_text = stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else ""
        stderr_text = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else ""

    payload: Dict[str, Any] = {}
    if stdout_text.strip():
        try:
            payload = json.loads(stdout_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{step_name} produced non-JSON output") from exc
    if not payload:
        payload = {"status": "ERROR", "message": "No JSON output produced"}

    payload["__returncode"] = completed.returncode
    payload["__stderr"] = stderr_text.strip()
    warning_payload = _verification_warning_payload(step_name, payload)
    if warning_payload is not None:
        warning_payload["__returncode"] = 0
        warning_payload["__stderr"] = stderr_text.strip()
        return warning_payload
    return payload


def _check_manual_columns(spreadsheet_id: str, tab_name: str, required_columns: Sequence[str]) -> bool:
    headers = _get_sheet_headers_batch(spreadsheet_id, [tab_name]).get(tab_name, [])
    return bool(headers) and all(column in headers for column in required_columns)


def _validate_manual_tabs_preserved(spreadsheet_id: str) -> bool:
    headers_by_tab = _get_sheet_headers_batch(spreadsheet_id, list(MANUAL_COLUMNS))
    for tab_name, required_columns in MANUAL_COLUMNS.items():
        headers = headers_by_tab.get(tab_name, [])
        if not headers or not all(column in headers for column in required_columns):
            return False
    return True


def _load_spreadsheet_id() -> str:
    meta_path = Path(__file__).resolve().parents[3] / "data" / "output" / "master_sku_sheet.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"Missing required file: {meta_path}")
    return json.loads(meta_path.read_text(encoding="utf-8"))["spreadsheet_id"]


def _run_raw_input_safety_check() -> Dict[str, Any]:
    module = importlib.import_module("src.marketplaces.flipkart.check_flipkart_raw_input_safety")
    checker = getattr(module, "check_flipkart_raw_input_safety", None)
    if checker is None:
        raise AttributeError("src.marketplaces.flipkart.check_flipkart_raw_input_safety does not expose check_flipkart_raw_input_safety()")
    result = checker()
    if not isinstance(result, dict):
        raise RuntimeError("check_flipkart_raw_input_safety() returned a non-dict payload")
    return result


def run_flipkart_post_analysis_refresh(
    *,
    mode: str = "quick",
    refresh_keywords: bool = False,
    run_visual_search: bool = False,
    visual_max_fsns: int = 5,
    sync_drive_archive: bool = False,
    verify_all: bool = False,
    skip_verification: bool = False,
    sleep_seconds: float = 0.0,
    health_delay_seconds: float = 0.0,
    force_raw_refresh: bool = False,
) -> Dict[str, Any]:
    global _RUNNER_VISUAL_MAX_FSNS
    _RUNNER_VISUAL_MAX_FSNS = max(0, int(visual_max_fsns))

    raw_input_safety_result: Dict[str, Any] | None = None
    raw_input_safety_warning: str | None = None
    if mode == "full":
        raw_input_safety_result = _run_raw_input_safety_check()
        safe_to_run_full_refresh = bool(raw_input_safety_result.get("safe_to_run_full_refresh", False))
        if not safe_to_run_full_refresh and not force_raw_refresh:
            blocked_summary = {
                "timestamp": now_iso(),
                "mode": mode,
                "status": "BLOCKED",
                "steps_run": [],
                "failed_step": "check_flipkart_raw_input_safety",
                "safe_to_run_full_refresh": False,
                "warnings": list(raw_input_safety_result.get("warnings", [])),
                "next_action": raw_input_safety_result.get("next_action", ""),
                "external_google_ads_called": False,
                "external_visual_search_called": False,
                "manual_tabs_preserved": True,
                "verification_passed": True,
                "verification_skipped": True,
                "tabs_refreshed": [],
                "drive_archive_synced": False,
                "raw_input_safety_result": raw_input_safety_result,
                "log_path": str(LOG_PATH),
            }
            if raw_input_safety_result.get("same_manifest_as_previous_run") or raw_input_safety_result.get("same_manifest_as_latest_run"):
                blocked_summary["warnings"].append("Current raw manifest matches a previously used manifest.")
            _append_log_row(
                {
                    "timestamp": blocked_summary["timestamp"],
                    "status": blocked_summary["status"],
                    "step_name": blocked_summary["failed_step"],
                    "details": json.dumps({k: v for k, v in blocked_summary.items() if k != "timestamp"}, ensure_ascii=False),
                    "log_path": str(LOG_PATH),
                }
            )
            return blocked_summary
        if not safe_to_run_full_refresh and force_raw_refresh:
            raw_input_safety_warning = "Raw input safety was bypassed by --force-raw-refresh"
        ensure_directories()
        spreadsheet_id = _load_spreadsheet_id()
    else:
        ensure_directories()
        spreadsheet_id = _load_spreadsheet_id()

    step_names = _build_execution_schedule(
        mode=mode,
        refresh_keywords=refresh_keywords,
        run_visual_search=run_visual_search,
        sync_drive_archive=sync_drive_archive,
        verify_all=verify_all,
        skip_verification=skip_verification,
        health_delay_seconds=health_delay_seconds,
    )
    detailed_verify_steps = _build_detailed_verify_steps()

    step_payloads: Dict[str, Dict[str, Any]] = {}
    steps_run: List[str] = []
    failed_step: str | None = None
    failure_exc: BaseException | None = None
    health_payload: Dict[str, Any] | None = None
    warning_messages: List[str] = []
    quota_warning_present = False

    if raw_input_safety_result is not None:
        raw_input_safety_warnings = [str(item) for item in raw_input_safety_result.get("warnings", []) if str(item)]
        warning_messages.extend(raw_input_safety_warnings)
        if raw_input_safety_warning:
            warning_messages.append(raw_input_safety_warning)

    for index, step_name in enumerate(step_names):
        if step_name == "__health_delay__":
            time.sleep(health_delay_seconds)
            continue
        try:
            payload = _run_step_subprocess(step_name)
            step_payloads[step_name] = payload
            steps_run.append(step_name)
            status_value = str(payload.get("status", "")).upper()
            returncode = int(payload.get("__returncode", 0) or 0)
            if step_name == HEALTH_CHECK_STEP:
                health_payload = payload
                if status_value in WARNING_STATUSES or _is_google_sheets_429(str(payload.get("message", ""))):
                    warning_messages.append(_warning_message(step_name, payload))
                    quota_warning_present = quota_warning_present or _is_google_sheets_429(str(payload.get("message", "")))
                    continue
                if status_value != "PASS" or returncode != 0:
                    failed_step = HEALTH_CHECK_STEP
                    if status_value and status_value != "PASS":
                        failure_exc = RuntimeError(payload.get("message", f"{step_name} failed"))
                    elif returncode != 0:
                        failure_exc = RuntimeError(payload.get("__stderr", f"{step_name} returned non-zero exit code"))
                    break
                continue
            if status_value in WARNING_STATUSES:
                warning_messages.append(_warning_message(step_name, payload))
                quota_warning_present = quota_warning_present or _is_quota_limited_message(str(payload.get("message", "")))
                continue
            if _is_quota_limited_message(str(payload.get("message", ""))):
                warning_messages.append("wait 5 minutes and rerun health check")
                quota_warning_present = True
                continue
            if status_value not in {value.upper() for value in _step_expected_status(step_name)} or returncode != 0:
                if _is_quota_limited_message(str(payload.get("message", ""))):
                    warning_messages.append("wait 5 minutes and rerun health check")
                    quota_warning_present = True
                    continue
                failed_step = step_name
                if status_value and status_value != "SUCCESS" and status_value != "PASS":
                    failure_exc = RuntimeError(payload.get("message", f"{step_name} failed"))
                elif returncode != 0:
                    failure_exc = RuntimeError(payload.get("__stderr", f"{step_name} returned non-zero exit code"))
                break
        except BaseException as exc:  # noqa: BLE001 - capture step failure for JSON summary
            failed_step = step_name
            failure_exc = exc
            break
        if sleep_seconds > 0 and index < len(step_names) - 1:
            time.sleep(sleep_seconds)

    verification_steps: List[str] = []
    verification_steps = [step for step in step_names if step.startswith("verify_")]
    verification_passed = not verification_steps or all(
        str(step_payloads.get(step, {}).get("status", "")).upper() not in {"ERROR", "FAIL"} for step in verification_steps
    )
    manual_tabs_preserved = True
    try:
        manual_tabs_preserved = _validate_manual_tabs_preserved(spreadsheet_id)
    except BaseException as exc:  # noqa: BLE001 - quota pressure should downgrade to warning
        if _is_google_sheets_429(str(exc)):
            warning_messages.append("wait 5 minutes and rerun health check")
            quota_warning_present = True
        else:
            raise
    tabs_refreshed = _ordered_tabs(step_names, step_payloads)

    status = "SUCCESS"
    error_type = ""
    message = ""
    if failure_exc is not None:
        status = "ERROR"
        if failed_step == HEALTH_CHECK_STEP and health_payload is not None:
            error_type = str(health_payload.get("error_type", "")) or failure_exc.__class__.__name__
            message = str(health_payload.get("message", "")) or str(failure_exc)
        else:
            error_type = failure_exc.__class__.__name__
            message = str(failure_exc)
    elif not verification_passed or not manual_tabs_preserved:
        status = "FAIL"
        if not verification_passed and not failed_step:
            failed_step = "verification_passed"
        elif not manual_tabs_preserved and not failed_step:
            failed_step = "manual_tabs_preserved"
    elif warning_messages:
        status = "WARNING" if quota_warning_present else "SUCCESS_WITH_WARNINGS"
        message = warning_messages[0]

    external_google_ads_called = bool(step_payloads.get("refresh_google_keyword_metrics", {}).get("api_called", False))
    external_visual_search_called = int(step_payloads.get("run_flipkart_visual_competitor_search", {}).get("api_called_count", 0) or 0) > 0
    drive_archive_synced = step_payloads.get("sync_flipkart_run_archive_to_drive", {}).get("status", "") in {"SUCCESS", "SUCCESS_WITH_WARNINGS"}

    summary = {
        "timestamp": now_iso(),
        "mode": mode,
        "status": status,
        "steps_run": steps_run,
        "failed_step": failed_step,
        "safe_to_run_full_refresh": None if raw_input_safety_result is None else bool(raw_input_safety_result.get("safe_to_run_full_refresh", False)),
        "verification_passed": verification_passed,
        "verification_skipped": skip_verification,
        "warnings": warning_messages,
        "external_google_ads_called": external_google_ads_called,
        "external_visual_search_called": external_visual_search_called,
        "drive_archive_synced": drive_archive_synced,
        "tabs_refreshed": tabs_refreshed,
        "manual_tabs_preserved": manual_tabs_preserved,
        "raw_input_safety_result": raw_input_safety_result,
        "log_path": str(LOG_PATH),
    }
    if error_type:
        summary["error_type"] = error_type
    if message:
        summary["message"] = message
    _append_log_row(
        {
            "timestamp": summary["timestamp"],
            "status": summary["status"],
            "step_name": "run_flipkart_post_analysis_refresh",
            "details": json.dumps({k: v for k, v in summary.items() if k != "timestamp"}, ensure_ascii=False),
            "log_path": str(LOG_PATH),
        }
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Flipkart post-analysis refresh steps.")
    parser.add_argument("--mode", choices=RUN_MODES, default="quick", help="Refresh mode to run. Default: quick.")
    parser.add_argument("--refresh-keywords", action="store_true", help="Refresh Google Keyword Planner cache before rebuilding the demand profile.")
    parser.add_argument("--run-visual-search", action="store_true", help="Run Flipkart visual competitor search before competitor intelligence.")
    parser.add_argument("--visual-max-fsns", type=int, default=5, help="Maximum FSNs to send through visual competitor search.")
    parser.add_argument("--sync-drive-archive", action="store_true", help="Sync the latest Flipkart run archive to Google Drive after refresh steps.")
    parser.add_argument("--verify-all", action="store_true", help="Run detailed Flipkart verification scripts after the default refresh flow.")
    parser.add_argument("--skip-verification", action="store_true", help="Skip post-refresh verification and run refresh modules only.")
    parser.add_argument("--force-raw-refresh", action="store_true", help="Bypass the raw input safety guard for full refresh mode only.")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Pause between subprocess steps to reduce Google Sheets quota pressure.")
    parser.add_argument("--health-delay-seconds", type=float, default=0.0, help="Pause before the health check or detailed verification steps.")
    args = parser.parse_args()

    try:
        summary = run_flipkart_post_analysis_refresh(
            mode=args.mode,
            refresh_keywords=args.refresh_keywords,
            run_visual_search=args.run_visual_search,
            visual_max_fsns=max(0, args.visual_max_fsns),
            sync_drive_archive=args.sync_drive_archive,
            verify_all=args.verify_all,
            skip_verification=args.skip_verification,
            sleep_seconds=max(0.0, args.sleep_seconds),
            health_delay_seconds=max(0.0, args.health_delay_seconds),
            force_raw_refresh=args.force_raw_refresh,
        )
        print(_json_text(summary))
        if summary["status"] not in {"SUCCESS", "WARNING", "SUCCESS_WITH_WARNINGS"}:
            raise SystemExit(1)
    except Exception as exc:
        error_payload = {
            "timestamp": now_iso(),
            "status": "ERROR",
            "error_type": exc.__class__.__name__,
            "message": str(exc),
            "failed_step": "run_flipkart_post_analysis_refresh",
            "log_path": str(LOG_PATH),
        }
        _append_log_row(
            {
                "timestamp": error_payload["timestamp"],
                "status": error_payload["status"],
                "step_name": error_payload["failed_step"],
                "details": json.dumps(error_payload, ensure_ascii=False),
                "log_path": str(LOG_PATH),
            }
        )
        print(_json_text(error_payload))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
