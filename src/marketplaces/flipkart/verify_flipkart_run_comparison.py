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
from src.marketplaces.flipkart.create_flipkart_run_comparison import (
    FSN_COMPARISON_TAB,
    FSN_HISTORY_TAB,
    LOOKER_RUN_COMPARISON_TAB,
    RUN_COMPARISON_TAB,
    RUN_HISTORY_TAB,
    SPREADSHEET_META_PATH,
    build_previous_run_candidates,
    load_json,
    read_table,
    tab_exists,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text


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


def count_non_empty_rows(rows: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def verify_flipkart_run_comparison() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    required_tabs = [RUN_COMPARISON_TAB, FSN_COMPARISON_TAB, LOOKER_RUN_COMPARISON_TAB, RUN_HISTORY_TAB, FSN_HISTORY_TAB]
    tab_presence = {tab_name: tab_exists(sheets_service, spreadsheet_id, tab_name) for tab_name in required_tabs}

    _, run_history_rows = read_table(sheets_service, spreadsheet_id, RUN_HISTORY_TAB)
    _, fsn_history_rows = read_table(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
    _, run_comparison_rows = read_table(sheets_service, spreadsheet_id, RUN_COMPARISON_TAB)
    _, fsn_comparison_rows = read_table(sheets_service, spreadsheet_id, FSN_COMPARISON_TAB)
    _, looker_rows = read_table(sheets_service, spreadsheet_id, LOOKER_RUN_COMPARISON_TAB)

    latest_run_row = {}
    latest_run_id = ""
    if run_history_rows:
        from src.marketplaces.flipkart.create_looker_studio_sources import get_latest_run_row, latest_text_value

        latest_run_row = get_latest_run_row(run_history_rows)
        latest_run_id = latest_text_value(latest_run_row, "Run_ID")

    previous_run_id, _ = build_previous_run_candidates(run_history_rows)
    history_run_ids = {normalize_text(row.get("Run_ID", "")) for row in run_history_rows if normalize_text(row.get("Run_ID", ""))}
    history_has_2plus_runs = len(history_run_ids) >= 2

    row_counts = {
        RUN_COMPARISON_TAB: count_non_empty_rows(run_comparison_rows),
        FSN_COMPARISON_TAB: count_non_empty_rows(fsn_comparison_rows),
        LOOKER_RUN_COMPARISON_TAB: count_non_empty_rows(looker_rows),
    }
    blank_fsn_count = sum(1 for row in fsn_comparison_rows if not clean_fsn(row.get("FSN", "")))
    comparison_status_distribution = Counter(
        normalize_text(row.get("Comparison_Status", "")) for row in fsn_comparison_rows if normalize_text(row.get("Comparison_Status", ""))
    )

    checks = {
        "run_comparison_tab_exists": tab_presence[RUN_COMPARISON_TAB],
        "fsn_comparison_tab_exists": tab_presence[FSN_COMPARISON_TAB],
        "looker_run_comparison_tab_exists": tab_presence[LOOKER_RUN_COMPARISON_TAB],
        "run_comparison_has_rows": row_counts[RUN_COMPARISON_TAB] > 0,
        "fsn_comparison_has_rows": row_counts[FSN_COMPARISON_TAB] > 0,
        "looker_run_comparison_has_rows": row_counts[LOOKER_RUN_COMPARISON_TAB] > 0,
        "latest_run_id_exists": bool(latest_run_id),
        "previous_run_id_exists_if_history_2plus": (not history_has_2plus_runs) or bool(previous_run_id),
        "no_blank_fsn_in_fsn_comparison": blank_fsn_count == 0,
        "comparison_statuses_populated": all(
            normalize_text(row.get("Comparison_Status", "")) for row in fsn_comparison_rows
        ),
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "latest_run_id": latest_run_id,
        "previous_run_id": previous_run_id,
        "row_counts": row_counts,
        "blank_fsn_count": blank_fsn_count,
        "comparison_status_distribution": dict(comparison_status_distribution),
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
        "history_run_count": len(history_run_ids),
        "fsn_history_rows": len(fsn_history_rows),
    }


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_run_comparison(), indent=2, ensure_ascii=False))
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
