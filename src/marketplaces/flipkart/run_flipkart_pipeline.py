from __future__ import annotations

import argparse
import json
import shutil
import sys
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from pathlib import Path
from traceback import format_exception
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from src.marketplaces.flipkart.analyze_flipkart_reports import analyze_flipkart_reports
from src.marketplaces.flipkart.audit_flipkart_sku_analysis import AUDIT_LOG_PATH, audit_flipkart_sku_analysis
from src.marketplaces.flipkart.build_flipkart_fsn_bridge import build_flipkart_fsn_bridge
from src.marketplaces.flipkart.build_flipkart_sku_analysis import build_flipkart_sku_analysis
from src.marketplaces.flipkart.create_flipkart_alerts_and_tasks import (
    LOG_PATH as ALERTS_TASKS_LOG_PATH,
    create_flipkart_alerts_and_tasks,
)
from src.marketplaces.flipkart.flipkart_utils import (
    CONFIG_DIR,
    ANALYSIS_JSON_PATH,
    FSN_BRIDGE_LOG_PATH,
    FSN_BRIDGE_PATH,
    LOG_DIR,
    NORMALIZATION_LOG_PATH,
    NORMALIZED_ADS_PATH,
    NORMALIZED_LISTINGS_PATH,
    NORMALIZED_ORDERS_PATH,
    NORMALIZED_PNL_PATH,
    NORMALIZED_RETURNS_PATH,
    NORMALIZED_SETTLEMENTS_PATH,
    PUSH_LOG_PATH,
    RAW_INPUT_DIR,
    REPORT_ANALYSIS_LOG_PATH,
    SKU_ANALYSIS_LOG_PATH,
    SKU_ANALYSIS_PATH,
    TARGET_FSN_PATH,
    append_csv_log,
    csv_data_row_count,
    detect_header_row,
    file_mtime_iso,
    infer_report_type,
    list_input_files,
    load_json,
    load_report_patterns,
    load_synonyms,
    now_iso,
    read_workbook_rows,
    report_type_scores,
    save_json,
    write_csv,
    OUTPUT_DIR,
)
from src.marketplaces.flipkart.normalize_flipkart_reports import normalize_flipkart_reports
from src.marketplaces.flipkart.push_flipkart_analysis_to_sheet import push_flipkart_analysis_to_sheet
from src.marketplaces.flipkart.update_flipkart_run_history import update_flipkart_run_history

PIPELINE_RUN_LOG_PATH = LOG_DIR / "flipkart_pipeline_run_log.csv"
RUNS_DIR = OUTPUT_DIR / "runs"
FLIPKART_RUN_CONFIG_PATH = CONFIG_DIR / "flipkart_run_config.json"
GOOGLE_SHEET_TAB = "FLIPKART_SKU_ANALYSIS"
PIPELINE_RUN_LOG_HEADERS = [
    "run_id",
    "run_folder",
    "timestamp",
    "status",
    "steps_run",
    "failed_step",
    "error_type",
    "message",
    "traceback_tail",
    "audit_passed",
    "pushed_to_google_sheet",
    "target_fsn_count",
    "rows_written",
    "high_confidence_count",
    "medium_confidence_count",
    "low_confidence_count",
    "output_csv_path",
    "google_sheet_tab",
]

RUN_HISTORY_UPDATE_DEFAULTS = {
    "run_history_updated": False,
    "run_history_rows_added": 0,
    "fsn_history_rows_added": 0,
}

ALERT_TASKS_DEFAULTS = {
    "alerts_generated": 0,
    "critical_alert_count": 0,
    "high_alert_count": 0,
    "medium_alert_count": 0,
    "low_alert_count": 0,
    "tracker_rows_created": 0,
    "tracker_rows_updated": 0,
    "active_tasks_count": 0,
    "tabs_updated": [],
}

StageFn = Callable[[], Dict[str, Any]]
PipelineStep = Tuple[str, StageFn]


def _capture_stage(func: StageFn) -> Dict[str, Any]:
    buffer = StringIO()
    with redirect_stdout(buffer):
        return func()


def _validate_raw_input_folder() -> None:
    if not RAW_INPUT_DIR.exists():
        raise FileNotFoundError(f"Missing required Flipkart raw report folder: {RAW_INPUT_DIR}")
    if not RAW_INPUT_DIR.is_dir():
        raise NotADirectoryError(f"Expected a folder for Flipkart raw reports: {RAW_INPUT_DIR}")

    report_files = [
        path
        for path in sorted(RAW_INPUT_DIR.iterdir())
        if path.is_file() and not path.name.startswith("~$") and path.suffix.lower() in {".csv", ".xls", ".xlsx", ".xlsm"}
    ]
    if not report_files:
        raise RuntimeError(f"No raw Flipkart report files found in: {RAW_INPUT_DIR}")


def _validate_target_fsn_file() -> None:
    if not TARGET_FSN_PATH.exists():
        raise FileNotFoundError(f"Missing required Flipkart target FSN file: {TARGET_FSN_PATH}")
    if csv_data_row_count(TARGET_FSN_PATH) <= 0:
        raise RuntimeError(f"Flipkart target FSN file is empty: {TARGET_FSN_PATH}")


def _audit_passed(summary: Dict[str, Any]) -> bool:
    return (
        int(summary.get("confidence_mismatch_count", 0)) == 0
        and int(summary.get("profit_mismatch_count", 0)) == 0
        and int(summary.get("return_rate_mismatch_count", 0)) == 0
        and int(summary.get("numeric_parse_issue_count", 0)) == 0
        and int(summary.get("settlement_present_but_zero_net_settlement_count", 0)) == 0
        and int(summary.get("orders_present_but_zero_sales_count", 0)) <= 2
        and int(summary.get("pnl_present_but_zero_net_earnings_count", 0)) <= 2
    )


def _append_pipeline_run_log(payload: Dict[str, Any]) -> None:
    append_csv_log(PIPELINE_RUN_LOG_PATH, PIPELINE_RUN_LOG_HEADERS, [payload])


def _generate_run_id() -> str:
    return f"FLIPKART_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _load_run_config() -> Dict[str, str]:
    if not FLIPKART_RUN_CONFIG_PATH.exists():
        return {"report_start_date": "", "report_end_date": "", "notes": ""}
    payload = load_json(FLIPKART_RUN_CONFIG_PATH)
    return {
        "report_start_date": str(payload.get("report_start_date", "") or ""),
        "report_end_date": str(payload.get("report_end_date", "") or ""),
        "notes": str(payload.get("notes", "") or ""),
    }


def _generate_fallback_run_id() -> str:
    return f"FLIPKART_ERROR_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _traceback_tail(exc: BaseException, lines: int = 12) -> str:
    tail = "".join(format_exception(type(exc), exc, exc.__traceback__)).strip().splitlines()
    if not tail:
        return ""
    return "\n".join(tail[-lines:])


def _print_json(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _finalize_failure(
    *,
    run_id: Optional[str],
    run_dir: Optional[Path],
    run_config: Dict[str, str],
    manifest_info: Optional[Dict[str, Any]],
    failed_step: str,
    exc: BaseException,
    steps_run: List[str],
    audit_passed: bool,
    pushed_to_google_sheet: bool,
    google_sheet_tab: str,
    target_fsn_count: int,
    rows_written: int,
    high_confidence_count: int,
    medium_confidence_count: int,
    low_confidence_count: int,
    run_history_updated: bool,
    run_history_rows_added: int,
    fsn_history_rows_added: int,
    alerts_generated: int,
    critical_alert_count: int,
    high_alert_count: int,
    medium_alert_count: int,
    low_alert_count: int,
    tracker_rows_created: int,
    tracker_rows_updated: int,
    active_tasks_count: int,
    tabs_updated: List[str],
    log_entries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    final_run_id = run_id or _generate_fallback_run_id()
    final_run_dir = run_dir or (RUNS_DIR / final_run_id)
    final_run_dir.mkdir(parents=True, exist_ok=True)
    traceback_tail = _traceback_tail(exc)
    summary = {
        "run_id": final_run_id,
        "run_folder": str(final_run_dir),
        "report_start_date": run_config.get("report_start_date", ""),
        "report_end_date": run_config.get("report_end_date", ""),
        "input_file_count": int((manifest_info or {}).get("input_file_count", 0) or 0),
        "input_files_found": list((manifest_info or {}).get("input_files_found", [])),
        "manifest_path": str((manifest_info or {}).get("manifest_path", "")),
        "archive_folder_path": str(final_run_dir),
        "backup_before_push_path": str(final_run_dir / "backup_before_push.csv"),
        "run_summary_path": str(final_run_dir / "pipeline_run_summary.json"),
        "status": "ERROR",
        "steps_run": steps_run,
        "failed_step": failed_step,
        "error_type": exc.__class__.__name__,
        "message": str(exc),
        "traceback_tail": traceback_tail,
        "target_fsn_count": target_fsn_count,
        "rows_written": rows_written,
        "high_confidence_count": high_confidence_count,
        "medium_confidence_count": medium_confidence_count,
        "low_confidence_count": low_confidence_count,
        "audit_passed": audit_passed,
        "pushed_to_google_sheet": pushed_to_google_sheet,
        "run_history_updated": run_history_updated,
        "run_history_rows_added": run_history_rows_added,
        "fsn_history_rows_added": fsn_history_rows_added,
        "alerts_generated": alerts_generated,
        "critical_alert_count": critical_alert_count,
        "high_alert_count": high_alert_count,
        "medium_alert_count": medium_alert_count,
        "low_alert_count": low_alert_count,
        "tracker_rows_created": tracker_rows_created,
        "tracker_rows_updated": tracker_rows_updated,
        "active_tasks_count": active_tasks_count,
        "tabs_updated": tabs_updated,
        "output_csv_path": str(SKU_ANALYSIS_PATH),
        "google_sheet_tab": google_sheet_tab,
        "logs": log_entries
        + [
            {
                "step": failed_step,
                "status": "ERROR",
                "log_path": "",
                "message": str(exc),
            },
            {
                "step": "flipkart_pipeline_run_log",
                "status": "ERROR",
                "log_path": str(PIPELINE_RUN_LOG_PATH),
            },
        ],
    }
    archive_files = _archive_run_outputs(final_run_dir)
    summary["archive_files"] = archive_files
    save_json(Path(summary["run_summary_path"]), summary)
    _append_pipeline_run_log(
        {
            "timestamp": now_iso(),
            "run_id": summary["run_id"],
            "run_folder": summary["run_folder"],
            "status": summary["status"],
            "steps_run": json.dumps(summary["steps_run"], ensure_ascii=False),
            "failed_step": summary["failed_step"] or "",
            "error_type": summary["error_type"],
            "message": summary["message"],
            "traceback_tail": summary["traceback_tail"],
            "audit_passed": str(summary["audit_passed"]),
            "pushed_to_google_sheet": str(summary["pushed_to_google_sheet"]),
            "target_fsn_count": summary["target_fsn_count"],
            "rows_written": summary["rows_written"],
            "high_confidence_count": summary["high_confidence_count"],
            "medium_confidence_count": summary["medium_confidence_count"],
            "low_confidence_count": summary["low_confidence_count"],
            "output_csv_path": summary["output_csv_path"],
            "google_sheet_tab": summary["google_sheet_tab"],
            "message": summary["message"],
        }
    )
    _print_json(summary)
    return summary


def _detect_manifest_report_type(file_path: Any, synonyms: Dict[str, List[str]], patterns: Dict[str, Any]) -> Tuple[str, int]:
    workbook_rows = read_workbook_rows(file_path)
    best_report_type = "unknown"
    best_score = 0.0
    best_row_count = 0
    filename_scores = report_type_scores(file_path.name, "", {}, patterns)
    filename_best_type = max(filename_scores, key=filename_scores.get) if filename_scores else "unknown"
    filename_best_score = float(filename_scores.get(filename_best_type, 0.0)) if filename_scores else 0.0

    if filename_best_score > 0:
        for sheet_name, rows in workbook_rows.items():
            if not rows:
                continue
            header_row_index, detected_columns, _ = detect_header_row(rows, synonyms)
            report_type, scores = infer_report_type(file_path.name, sheet_name, detected_columns, patterns)
            if report_type != filename_best_type:
                continue
            score = float(scores.get(report_type, 0.0)) if scores else 0.0
            row_count = max(0, len(rows) - (header_row_index + 1))
            if score >= best_score:
                best_report_type = filename_best_type
                best_score = score
                best_row_count = row_count
        if best_score > 0.0:
            return best_report_type, best_row_count
        return filename_best_type, 0

    for sheet_name, rows in workbook_rows.items():
        if not rows:
            continue
        header_row_index, detected_columns, _ = detect_header_row(rows, synonyms)
        report_type, scores = infer_report_type(file_path.name, sheet_name, detected_columns, patterns)
        score = float(scores.get(report_type, 0.0)) if scores else 0.0
        row_count = max(0, len(rows) - (header_row_index + 1))
        if score > best_score:
            best_report_type = report_type
            best_score = score
            best_row_count = row_count

    if best_score <= 0.0:
        if file_path.suffix.lower() == ".csv":
            rows = workbook_rows.get(file_path.name, [])
            best_row_count = csv_data_row_count(file_path) if rows else 0
        return "unknown", best_row_count

    return best_report_type, best_row_count


def _build_input_manifest(run_dir: Path, run_id: str) -> Dict[str, Any]:
    synonyms = load_synonyms()
    patterns = load_report_patterns()
    files = list_input_files(RAW_INPUT_DIR)
    manifest_rows: List[Dict[str, Any]] = []

    for file_path in files:
        suffix = file_path.suffix.lower()
        sheet_count = 1
        row_count: Any = ""
        detected_report_type = "unknown"

        if suffix in {".xls", ".xlsx", ".xlsm", ".csv"}:
            workbook_rows = read_workbook_rows(file_path)
            sheet_count = len(workbook_rows) if suffix in {".xls", ".xlsx", ".xlsm"} else 1
            detected_report_type, detected_row_count = _detect_manifest_report_type(file_path, synonyms, patterns)
            row_count = detected_row_count
            if suffix == ".csv" and row_count == 0:
                row_count = csv_data_row_count(file_path)

        manifest_rows.append(
            {
                "run_id": run_id,
                "timestamp": now_iso(),
                "file_name": file_path.name,
                "file_path": str(file_path),
                "file_size_bytes": file_path.stat().st_size,
                "modified_time": file_mtime_iso(file_path),
                "detected_report_type": detected_report_type,
                "sheet_count": sheet_count if suffix in {".xls", ".xlsx", ".xlsm"} else "",
                "row_count": row_count,
            }
        )

    manifest_path = run_dir / "input_manifest.csv"
    write_csv(
        manifest_path,
        [
            "run_id",
            "timestamp",
            "file_name",
            "file_path",
            "file_size_bytes",
            "modified_time",
            "detected_report_type",
            "sheet_count",
            "row_count",
        ],
        manifest_rows,
    )
    return {
        "manifest_path": str(manifest_path),
        "input_file_count": len(files),
        "input_files_found": [file_path.name for file_path in files],
    }


def _archive_run_outputs(run_dir: Path) -> List[str]:
    archive_files = [
        SKU_ANALYSIS_PATH,
        ANALYSIS_JSON_PATH,
        FSN_BRIDGE_PATH,
        NORMALIZED_LISTINGS_PATH,
        NORMALIZED_ORDERS_PATH,
        NORMALIZED_RETURNS_PATH,
        NORMALIZED_SETTLEMENTS_PATH,
        NORMALIZED_PNL_PATH,
        NORMALIZED_ADS_PATH,
        OUTPUT_DIR / "normalized_sales_tax.csv",
        OUTPUT_DIR / "flipkart_target_fsns.csv",
        OUTPUT_DIR / "flipkart_target_fsn_summary.json",
        OUTPUT_DIR / "fsn_mismatch_samples.json",
        OUTPUT_DIR / "fsn_coverage_report.csv",
        OUTPUT_DIR / "flipkart_analysis_audit_summary.json",
        OUTPUT_DIR / "flipkart_analysis_audit.csv",
        OUTPUT_DIR / "flipkart_analysis_audit_state.json",
        OUTPUT_DIR / "flipkart_pipeline_status.json",
        OUTPUT_DIR / "flipkart_normalization_state.json",
        OUTPUT_DIR / "flipkart_sku_analysis_state.json",
        OUTPUT_DIR / "flipkart_push_state.json",
    ]

    copied_files: List[str] = []
    for source_path in archive_files:
        if not source_path.exists():
            continue
        destination_path = run_dir / source_path.name
        shutil.copy2(source_path, destination_path)
        copied_files.append(str(destination_path))
    return copied_files


def _log_entry(step: str, status: str, log_path: str, message: str = "") -> Dict[str, Any]:
    entry = {"step": step, "status": status, "log_path": log_path}
    if message:
        entry["message"] = message
    return entry


def _build_summary(
    *,
    run_id: str,
    run_folder: str,
    report_start_date: str,
    report_end_date: str,
    input_file_count: int,
    input_files_found: List[str],
    manifest_path: str,
    archive_folder_path: str,
    backup_before_push_path: str,
    run_summary_path: str,
    status: str,
    steps_run: List[str],
    failed_step: Optional[str],
    target_fsn_count: int,
    rows_written: int,
    high_confidence_count: int,
    medium_confidence_count: int,
    low_confidence_count: int,
    audit_passed: bool,
    pushed_to_google_sheet: bool,
    google_sheet_tab: str,
    run_history_updated: bool,
    run_history_rows_added: int,
    fsn_history_rows_added: int,
    alerts_generated: int,
    critical_alert_count: int,
    high_alert_count: int,
    medium_alert_count: int,
    low_alert_count: int,
    tracker_rows_created: int,
    tracker_rows_updated: int,
    active_tasks_count: int,
    tabs_updated: List[str],
    log_entries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "run_folder": run_folder,
        "report_start_date": report_start_date,
        "report_end_date": report_end_date,
        "input_file_count": input_file_count,
        "input_files_found": input_files_found,
        "manifest_path": manifest_path,
        "archive_folder_path": archive_folder_path,
        "backup_before_push_path": backup_before_push_path,
        "run_summary_path": run_summary_path,
        "status": status,
        "steps_run": steps_run,
        "failed_step": failed_step,
        "target_fsn_count": target_fsn_count,
        "rows_written": rows_written,
        "high_confidence_count": high_confidence_count,
        "medium_confidence_count": medium_confidence_count,
        "low_confidence_count": low_confidence_count,
        "audit_passed": audit_passed,
        "pushed_to_google_sheet": pushed_to_google_sheet,
        "run_history_updated": run_history_updated,
        "run_history_rows_added": run_history_rows_added,
        "fsn_history_rows_added": fsn_history_rows_added,
        "alerts_generated": alerts_generated,
        "critical_alert_count": critical_alert_count,
        "high_alert_count": high_alert_count,
        "medium_alert_count": medium_alert_count,
        "low_alert_count": low_alert_count,
        "tracker_rows_created": tracker_rows_created,
        "tracker_rows_updated": tracker_rows_updated,
        "active_tasks_count": active_tasks_count,
        "tabs_updated": tabs_updated,
        "output_csv_path": str(SKU_ANALYSIS_PATH),
        "google_sheet_tab": google_sheet_tab,
        "logs": log_entries
        + [
            {
                "step": "flipkart_pipeline_run_log",
                "status": status,
                "log_path": str(PIPELINE_RUN_LOG_PATH),
            }
        ],
    }


def run_flipkart_pipeline(debug: bool = False) -> Dict[str, Any]:
    run_id: Optional[str] = None
    run_dir: Optional[Path] = None
    run_config: Dict[str, str] = {}
    manifest_info: Optional[Dict[str, Any]] = None
    steps: List[PipelineStep] = [
        ("analyze_flipkart_reports", analyze_flipkart_reports),
        ("build_flipkart_fsn_bridge", build_flipkart_fsn_bridge),
        ("normalize_flipkart_reports", normalize_flipkart_reports),
        ("build_flipkart_sku_analysis", build_flipkart_sku_analysis),
        ("audit_flipkart_sku_analysis", audit_flipkart_sku_analysis),
    ]
    steps_run: List[str] = []
    log_entries: List[Dict[str, Any]] = []
    failed_step: Optional[str] = None
    status = "SUCCESS"
    pushed_to_google_sheet = False
    audit_passed = False
    target_fsn_count = 0
    rows_written = 0
    high_confidence_count = 0
    medium_confidence_count = 0
    low_confidence_count = 0
    google_sheet_tab = GOOGLE_SHEET_TAB
    run_history_updated = RUN_HISTORY_UPDATE_DEFAULTS["run_history_updated"]
    run_history_rows_added = RUN_HISTORY_UPDATE_DEFAULTS["run_history_rows_added"]
    fsn_history_rows_added = RUN_HISTORY_UPDATE_DEFAULTS["fsn_history_rows_added"]
    alerts_generated = ALERT_TASKS_DEFAULTS["alerts_generated"]
    critical_alert_count = ALERT_TASKS_DEFAULTS["critical_alert_count"]
    high_alert_count = ALERT_TASKS_DEFAULTS["high_alert_count"]
    medium_alert_count = ALERT_TASKS_DEFAULTS["medium_alert_count"]
    low_alert_count = ALERT_TASKS_DEFAULTS["low_alert_count"]
    tracker_rows_created = ALERT_TASKS_DEFAULTS["tracker_rows_created"]
    tracker_rows_updated = ALERT_TASKS_DEFAULTS["tracker_rows_updated"]
    active_tasks_count = ALERT_TASKS_DEFAULTS["active_tasks_count"]
    tabs_updated = list(ALERT_TASKS_DEFAULTS["tabs_updated"])
    archive_folder_path = ""
    run_summary_path = ""
    backup_before_push_path = ""
    current_step = "precheck"

    try:
        run_config = _load_run_config()
        _validate_raw_input_folder()
        run_id = _generate_run_id()
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        archive_folder_path = str(run_dir)
        run_summary_path = str(run_dir / "pipeline_run_summary.json")
        backup_before_push_path = str(run_dir / "backup_before_push.csv")

        if debug:
            _print_json({"status": "DEBUG", "run_id": run_id, "step": "pipeline_start", "message": "Starting Flipkart pipeline"})

        manifest_info = _build_input_manifest(run_dir, run_id)

        for step_name, func in steps:
            current_step = step_name
            steps_run.append(step_name)
            if debug:
                _print_json({"status": "DEBUG", "run_id": run_id, "step": step_name, "message": "Starting step"})
            result = _capture_stage(func)

            if step_name == "analyze_flipkart_reports":
                log_entries.append(_log_entry(step_name, "SUCCESS", str(REPORT_ANALYSIS_LOG_PATH)))
                continue

            if step_name == "build_flipkart_fsn_bridge":
                _validate_target_fsn_file()
                log_entries.append(_log_entry(step_name, "SUCCESS", str(FSN_BRIDGE_LOG_PATH)))
                continue

            if step_name == "normalize_flipkart_reports":
                log_entries.append(_log_entry(step_name, "SUCCESS", str(NORMALIZATION_LOG_PATH)))
                continue

            if step_name == "build_flipkart_sku_analysis":
                build_summary = dict(result.get("summary", {}))
                rows_written = int(result.get("rows_written", build_summary.get("rows_written", 0)) or 0)
                high_confidence_count = int(build_summary.get("high_confidence_count", 0))
                medium_confidence_count = int(build_summary.get("medium_confidence_count", 0))
                low_confidence_count = int(build_summary.get("low_confidence_count", 0))
                target_fsn_count = int(build_summary.get("total_target_fsns", 0))
                log_entries.append(_log_entry(step_name, "SUCCESS", str(SKU_ANALYSIS_LOG_PATH)))
                continue

            if step_name == "audit_flipkart_sku_analysis":
                audit_passed = _audit_passed(result)
                if not audit_passed:
                    status = "AUDIT_FAILED"
                    failed_step = step_name
                log_entries.append(_log_entry(step_name, "SUCCESS", str(AUDIT_LOG_PATH)))

        if audit_passed:
            current_step = "push_flipkart_analysis_to_sheet"
            steps_run.append(current_step)
            if debug:
                _print_json({"status": "DEBUG", "run_id": run_id, "step": current_step, "message": "Starting step"})
            result = _capture_stage(lambda: push_flipkart_analysis_to_sheet(Path(backup_before_push_path)))
            pushed_to_google_sheet = True
            google_sheet_tab = str(result.get("tab_name", GOOGLE_SHEET_TAB))
            log_entries.append(_log_entry(current_step, "SUCCESS", str(PUSH_LOG_PATH)))

            provisional_summary = _build_summary(
                run_id=run_id,
                run_folder=str(run_dir),
                report_start_date=run_config["report_start_date"],
                report_end_date=run_config["report_end_date"],
                input_file_count=int((manifest_info or {}).get("input_file_count", 0) or 0),
                input_files_found=list((manifest_info or {}).get("input_files_found", [])),
                manifest_path=str((manifest_info or {}).get("manifest_path", "")),
                archive_folder_path=archive_folder_path,
                backup_before_push_path=backup_before_push_path,
                run_summary_path=run_summary_path,
                status=status,
                steps_run=steps_run,
                failed_step=failed_step,
                target_fsn_count=target_fsn_count,
                rows_written=rows_written,
                high_confidence_count=high_confidence_count,
                medium_confidence_count=medium_confidence_count,
                low_confidence_count=low_confidence_count,
                audit_passed=audit_passed,
                pushed_to_google_sheet=pushed_to_google_sheet,
                google_sheet_tab=google_sheet_tab,
                run_history_updated=run_history_updated,
                run_history_rows_added=run_history_rows_added,
                fsn_history_rows_added=fsn_history_rows_added,
                alerts_generated=alerts_generated,
                critical_alert_count=critical_alert_count,
                high_alert_count=high_alert_count,
                medium_alert_count=medium_alert_count,
                low_alert_count=low_alert_count,
                tracker_rows_created=tracker_rows_created,
                tracker_rows_updated=tracker_rows_updated,
                active_tasks_count=active_tasks_count,
                tabs_updated=tabs_updated,
                log_entries=log_entries,
            )
            save_json(Path(run_summary_path), provisional_summary)

            current_step = "update_flipkart_run_history"
            if debug:
                _print_json({"status": "DEBUG", "run_id": run_id, "step": current_step, "message": "Starting step"})
            history_result = _capture_stage(update_flipkart_run_history)
            run_history_updated = bool(history_result.get("run_history_updated", False))
            run_history_rows_added = int(history_result.get("run_history_rows_added", 0) or 0)
            fsn_history_rows_added = int(history_result.get("fsn_history_rows_added", 0) or 0)

            current_step = "create_flipkart_alerts_and_tasks"
            steps_run.append(current_step)
            if debug:
                _print_json({"status": "DEBUG", "run_id": run_id, "step": current_step, "message": "Starting step"})
            alerts_result = _capture_stage(create_flipkart_alerts_and_tasks)
            alerts_generated = int(alerts_result.get("generated_alert_count", 0) or 0)
            critical_alert_count = int(alerts_result.get("critical_alert_count", 0) or 0)
            high_alert_count = int(alerts_result.get("high_alert_count", 0) or 0)
            medium_alert_count = int(alerts_result.get("medium_alert_count", 0) or 0)
            low_alert_count = int(alerts_result.get("low_alert_count", 0) or 0)
            tracker_rows_created = int(alerts_result.get("tracker_rows_created", 0) or 0)
            tracker_rows_updated = int(alerts_result.get("tracker_rows_updated", 0) or 0)
            active_tasks_count = int(alerts_result.get("active_tasks_count", 0) or 0)
            tabs_updated = list(alerts_result.get("tabs_updated", []))
            log_entries.append(_log_entry(current_step, "SUCCESS", str(ALERTS_TASKS_LOG_PATH)))
        else:
            status = "AUDIT_FAILED"
            if failed_step is None:
                failed_step = "audit_flipkart_sku_analysis"

        if run_dir is None:
            raise RuntimeError("Run directory was not initialized")
        archive_files = _archive_run_outputs(run_dir)
        summary = _build_summary(
            run_id=run_id or _generate_fallback_run_id(),
            run_folder=str(run_dir),
            report_start_date=run_config["report_start_date"],
            report_end_date=run_config["report_end_date"],
            input_file_count=int((manifest_info or {}).get("input_file_count", 0) or 0),
            input_files_found=list((manifest_info or {}).get("input_files_found", [])),
            manifest_path=str((manifest_info or {}).get("manifest_path", "")),
            archive_folder_path=archive_folder_path or str(run_dir),
            backup_before_push_path=backup_before_push_path or str(run_dir / "backup_before_push.csv"),
            run_summary_path=run_summary_path or str(run_dir / "pipeline_run_summary.json"),
            status=status,
            steps_run=steps_run,
            failed_step=failed_step,
            target_fsn_count=target_fsn_count,
            rows_written=rows_written,
            high_confidence_count=high_confidence_count,
            medium_confidence_count=medium_confidence_count,
            low_confidence_count=low_confidence_count,
            audit_passed=audit_passed,
            pushed_to_google_sheet=pushed_to_google_sheet,
            google_sheet_tab=google_sheet_tab,
            run_history_updated=run_history_updated,
            run_history_rows_added=run_history_rows_added,
            fsn_history_rows_added=fsn_history_rows_added,
            alerts_generated=alerts_generated,
            critical_alert_count=critical_alert_count,
            high_alert_count=high_alert_count,
            medium_alert_count=medium_alert_count,
            low_alert_count=low_alert_count,
            tracker_rows_created=tracker_rows_created,
            tracker_rows_updated=tracker_rows_updated,
            active_tasks_count=active_tasks_count,
            tabs_updated=tabs_updated,
            log_entries=log_entries,
        )
        summary["archive_files"] = archive_files
        save_json(Path(summary["run_summary_path"]), summary)

        _append_pipeline_run_log(
            {
                "run_id": summary["run_id"],
                "run_folder": summary["run_folder"],
                "timestamp": now_iso(),
                "status": summary["status"],
                "steps_run": json.dumps(summary["steps_run"], ensure_ascii=False),
                "failed_step": summary["failed_step"] or "",
                "error_type": "",
                "message": "Pipeline completed successfully." if status == "SUCCESS" else "Audit failed; push skipped.",
                "traceback_tail": "",
                "audit_passed": str(summary["audit_passed"]),
                "pushed_to_google_sheet": str(summary["pushed_to_google_sheet"]),
                "target_fsn_count": summary["target_fsn_count"],
                "rows_written": summary["rows_written"],
                "high_confidence_count": summary["high_confidence_count"],
                "medium_confidence_count": summary["medium_confidence_count"],
                "low_confidence_count": summary["low_confidence_count"],
                "output_csv_path": summary["output_csv_path"],
                "google_sheet_tab": summary["google_sheet_tab"],
            }
        )
        print(f"run_id: {summary['run_id']}")
        print(f"report_start_date: {summary['report_start_date']}")
        print(f"report_end_date: {summary['report_end_date']}")
        print(f"input files found: {', '.join(summary['input_files_found'])}")
        print(f"audit passed: {summary['audit_passed']}")
        print(f"pushed_to_google_sheet: {summary['pushed_to_google_sheet']}")
        print(f"archive folder path: {summary['archive_folder_path']}")
        _print_json(summary)
        return summary
    except Exception as exc:
        failed_step = failed_step or current_step or (steps_run[-1] if steps_run else "precheck")
        log_entries.append(_log_entry(failed_step, "ERROR", "", str(exc)))
        _finalize_failure(
            run_id=run_id,
            run_dir=run_dir,
            run_config=run_config,
            manifest_info=manifest_info,
            failed_step=failed_step,
            exc=exc,
            steps_run=steps_run,
            audit_passed=audit_passed,
            pushed_to_google_sheet=pushed_to_google_sheet,
            google_sheet_tab=google_sheet_tab,
            target_fsn_count=target_fsn_count,
            rows_written=rows_written,
            high_confidence_count=high_confidence_count,
            medium_confidence_count=medium_confidence_count,
            low_confidence_count=low_confidence_count,
            run_history_updated=run_history_updated,
            run_history_rows_added=run_history_rows_added,
            fsn_history_rows_added=fsn_history_rows_added,
            alerts_generated=alerts_generated,
            critical_alert_count=critical_alert_count,
            high_alert_count=high_alert_count,
            medium_alert_count=medium_alert_count,
            low_alert_count=low_alert_count,
            tracker_rows_created=tracker_rows_created,
            tracker_rows_updated=tracker_rows_updated,
            active_tasks_count=active_tasks_count,
            tabs_updated=tabs_updated,
            log_entries=log_entries,
        )
        raise


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Run the Flipkart production pipeline.")
    parser.add_argument("--debug", action="store_true", help="Print progress before each pipeline step.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        run_flipkart_pipeline(debug=bool(args.debug))
    except Exception:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
