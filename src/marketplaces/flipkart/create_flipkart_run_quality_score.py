from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import (
    add_basic_filter,
    clear_tab,
    ensure_tab,
    freeze_and_format,
    load_json,
    read_table,
    tab_exists,
    write_rows,
)
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, ensure_directories, now_iso, write_csv
from src.marketplaces.flipkart.run_quality_score_utils import (
    ADS_MASTER_TAB,
    ADS_MAPPING_ISSUES_TAB,
    ACTIVE_TASKS_TAB,
    ALERTS_TAB,
    COST_MASTER_TAB,
    FSN_HISTORY_TAB,
    LISTING_PRESENCE_TAB,
    LOOKER_RUN_QUALITY_HEADERS,
    LOOKER_RUN_QUALITY_TAB,
    REPORT_FORMAT_ISSUES_TAB,
    REPORT_FORMAT_MONITOR_TAB,
    RETURN_COMMENTS_TAB,
    RETURN_ISSUE_SUMMARY_TAB,
    RUN_COMPARISON_TAB,
    RUN_HISTORY_TAB,
    RUN_QUALITY_BREAKDOWN_HEADERS,
    RUN_QUALITY_BREAKDOWN_TAB,
    RUN_QUALITY_SCORE_HEADERS,
    RUN_QUALITY_SCORE_TAB,
    SKU_ANALYSIS_TAB,
    build_run_quality_rows,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_run_quality_score_log.csv"
LOCAL_SCORE_PATH = OUTPUT_DIR / "flipkart_run_quality_score.csv"
LOCAL_BREAKDOWN_PATH = OUTPUT_DIR / "flipkart_run_quality_breakdown.csv"
LOCAL_LOOKER_PATH = OUTPUT_DIR / "looker_flipkart_run_quality_score.csv"

SOURCE_TABS = [
    RUN_HISTORY_TAB,
    FSN_HISTORY_TAB,
    SKU_ANALYSIS_TAB,
    ALERTS_TAB,
    ACTIVE_TASKS_TAB,
    COST_MASTER_TAB,
    ADS_MASTER_TAB,
    ADS_MAPPING_ISSUES_TAB,
    RETURN_COMMENTS_TAB,
    RETURN_ISSUE_SUMMARY_TAB,
    LISTING_PRESENCE_TAB,
    REPORT_FORMAT_MONITOR_TAB,
    REPORT_FORMAT_ISSUES_TAB,
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


def write_tab(sheets_service, spreadsheet_id: str, tab_name: str, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))
    add_basic_filter(sheets_service, spreadsheet_id, sheet_id, len(headers), len(rows) + 1)


def clear_output_tabs(sheets_service, spreadsheet_id: str) -> None:
    for tab_name in (RUN_QUALITY_SCORE_TAB, RUN_QUALITY_BREAKDOWN_TAB, LOOKER_RUN_QUALITY_TAB):
        ensure_tab(sheets_service, spreadsheet_id, tab_name)
        clear_tab(sheets_service, spreadsheet_id, tab_name)


def read_optional_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Sequence[Dict[str, Any]] | None:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        return None
    _, rows = read_table(sheets_service, spreadsheet_id, tab_name)
    return rows


def create_flipkart_run_quality_score() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    clear_output_tabs(sheets_service, spreadsheet_id)

    run_history_headers, run_history_rows = read_table(sheets_service, spreadsheet_id, RUN_HISTORY_TAB)
    fsn_history_rows = read_optional_table(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
    analysis_rows = read_optional_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    alert_rows = read_optional_table(sheets_service, spreadsheet_id, ALERTS_TAB)
    active_task_rows = read_optional_table(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)
    cost_master_rows = read_optional_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    ads_master_rows = read_optional_table(sheets_service, spreadsheet_id, ADS_MASTER_TAB)
    ads_issue_rows = read_optional_table(sheets_service, spreadsheet_id, ADS_MAPPING_ISSUES_TAB)
    return_comment_rows = read_optional_table(sheets_service, spreadsheet_id, RETURN_COMMENTS_TAB)
    return_issue_summary_rows = read_optional_table(sheets_service, spreadsheet_id, RETURN_ISSUE_SUMMARY_TAB)
    listing_rows = read_optional_table(sheets_service, spreadsheet_id, LISTING_PRESENCE_TAB)
    report_format_monitor_rows = read_optional_table(sheets_service, spreadsheet_id, REPORT_FORMAT_MONITOR_TAB)
    report_format_issue_rows = read_optional_table(sheets_service, spreadsheet_id, REPORT_FORMAT_ISSUES_TAB)
    run_comparison_rows = read_optional_table(sheets_service, spreadsheet_id, RUN_COMPARISON_TAB)

    if not run_history_rows:
        raise RuntimeError(f"No rows found in {RUN_HISTORY_TAB}")

    score_rows, breakdown_rows, looker_rows, summary = build_run_quality_rows(
        run_history_rows,
        fsn_history_rows,
        analysis_rows,
        alert_rows,
        active_task_rows,
        cost_master_rows,
        ads_master_rows,
        ads_issue_rows,
        return_comment_rows,
        return_issue_summary_rows,
        listing_rows,
        report_format_monitor_rows,
        report_format_issue_rows,
        run_comparison_rows,
    )

    write_csv(LOCAL_SCORE_PATH, RUN_QUALITY_SCORE_HEADERS, score_rows)
    write_csv(LOCAL_BREAKDOWN_PATH, RUN_QUALITY_BREAKDOWN_HEADERS, breakdown_rows)
    write_csv(LOCAL_LOOKER_PATH, LOOKER_RUN_QUALITY_HEADERS, looker_rows)

    write_tab(sheets_service, spreadsheet_id, RUN_QUALITY_SCORE_TAB, RUN_QUALITY_SCORE_HEADERS, score_rows)
    write_tab(sheets_service, spreadsheet_id, RUN_QUALITY_BREAKDOWN_TAB, RUN_QUALITY_BREAKDOWN_HEADERS, breakdown_rows)
    write_tab(sheets_service, spreadsheet_id, LOOKER_RUN_QUALITY_TAB, LOOKER_RUN_QUALITY_HEADERS, looker_rows)

    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "run_id",
            "report_date",
            "overall_score",
            "grade",
            "decision_recommendation",
            "breakdown_rows",
            "status",
            "message",
        ],
        [
            {
                "timestamp": now_iso(),
                "spreadsheet_id": spreadsheet_id,
                "run_id": summary["run_id"],
                "report_date": summary["report_date"],
                "overall_score": summary["overall_score"],
                "grade": summary["grade"],
                "decision_recommendation": summary["decision_recommendation"],
                "breakdown_rows": summary["breakdown_rows"],
                "status": "SUCCESS",
                "message": "Created Flipkart run quality score tabs",
            }
        ],
    )

    result = {
        "status": "SUCCESS",
        "run_id": summary["run_id"],
        "overall_score": summary["overall_score"],
        "grade": summary["grade"],
        "decision_recommendation": summary["decision_recommendation"],
        "critical_warnings": summary["critical_warnings"],
        "major_warnings": summary["major_warnings"],
        "breakdown_rows": summary["breakdown_rows"],
        "tabs_updated": summary["tabs_updated"],
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_run_quality_score()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
