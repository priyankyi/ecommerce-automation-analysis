from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import load_json, read_table, tab_exists
from src.marketplaces.flipkart.flipkart_utils import OUTPUT_DIR, normalize_text, parse_float
from src.marketplaces.flipkart.run_quality_score_utils import (
    LOOKER_RUN_QUALITY_TAB,
    RUN_QUALITY_BREAKDOWN_TAB,
    RUN_QUALITY_SCORE_TAB,
    CATEGORY_ORDER,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOCAL_SCORE_PATH = OUTPUT_DIR / "flipkart_run_quality_score.csv"
LOCAL_BREAKDOWN_PATH = OUTPUT_DIR / "flipkart_run_quality_breakdown.csv"
LOCAL_LOOKER_PATH = OUTPUT_DIR / "looker_flipkart_run_quality_score.csv"


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


def count_non_empty_rows(rows: List[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if any(normalize_text(value) for value in row.values()))


def latest_row(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = [row for row in rows if any(normalize_text(value) for value in row.values())]
    return rows[-1] if rows else {}


def verify_flipkart_run_quality_score() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    score_tab_exists = tab_exists(sheets_service, spreadsheet_id, RUN_QUALITY_SCORE_TAB)
    breakdown_tab_exists = tab_exists(sheets_service, spreadsheet_id, RUN_QUALITY_BREAKDOWN_TAB)
    looker_tab_exists = tab_exists(sheets_service, spreadsheet_id, LOOKER_RUN_QUALITY_TAB)

    score_headers: List[str] = []
    score_rows: List[Dict[str, Any]] = []
    breakdown_headers: List[str] = []
    breakdown_rows: List[Dict[str, Any]] = []
    looker_headers: List[str] = []
    looker_rows: List[Dict[str, Any]] = []

    if score_tab_exists:
        score_headers, score_rows = read_table(sheets_service, spreadsheet_id, RUN_QUALITY_SCORE_TAB)
    if breakdown_tab_exists:
        breakdown_headers, breakdown_rows = read_table(sheets_service, spreadsheet_id, RUN_QUALITY_BREAKDOWN_TAB)
    if looker_tab_exists:
        looker_headers, looker_rows = read_table(sheets_service, spreadsheet_id, LOOKER_RUN_QUALITY_TAB)

    latest = latest_row(score_rows)
    overall_score = parse_float(latest.get("Overall_Run_Quality_Score", "")) if latest else 0.0
    grade = normalize_text(latest.get("Run_Quality_Grade", "")) if latest else ""
    decision_recommendation = normalize_text(latest.get("Decision_Recommendation", "")) if latest else ""
    run_id = normalize_text(latest.get("Run_ID", "")) if latest else ""

    score_categories_found = sorted(
        {normalize_text(row.get("Score_Category", "")) for row in breakdown_rows if normalize_text(row.get("Score_Category", ""))},
        key=lambda value: CATEGORY_ORDER.index(value) if value in CATEGORY_ORDER else len(CATEGORY_ORDER),
    )
    missing_score_categories = [category for category in CATEGORY_ORDER if category not in score_categories_found]

    checks = {
        "score_tab_exists": score_tab_exists,
        "breakdown_tab_exists": breakdown_tab_exists,
        "looker_tab_exists": looker_tab_exists,
        "summary_row_count_is_1": count_non_empty_rows(score_rows) == 1,
        "overall_score_in_range": 0.0 <= overall_score <= 100.0,
        "grade_not_blank": bool(grade),
        "decision_recommendation_not_blank": bool(decision_recommendation),
        "breakdown_rows_exist": count_non_empty_rows(breakdown_rows) > 0,
        "looker_rows_exist": count_non_empty_rows(looker_rows) > 0,
        "required_score_categories_present": not missing_score_categories,
    }

    if not all(checks.values()):
        status = "FAIL"
    elif grade in {"Weak", "Do Not Trust"}:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"

    row_counts = {
        RUN_QUALITY_SCORE_TAB: count_non_empty_rows(score_rows),
        RUN_QUALITY_BREAKDOWN_TAB: count_non_empty_rows(breakdown_rows),
        LOOKER_RUN_QUALITY_TAB: count_non_empty_rows(looker_rows),
    }

    return {
        "status": status,
        "run_id": run_id,
        "overall_score": overall_score,
        "grade": grade,
        "decision_recommendation": decision_recommendation,
        "row_counts": row_counts,
        "score_categories_found": score_categories_found,
        "missing_score_categories": missing_score_categories,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
        "local_outputs": {
            "flipkart_run_quality_score": str(LOCAL_SCORE_PATH),
            "flipkart_run_quality_breakdown": str(LOCAL_BREAKDOWN_PATH),
            "looker_flipkart_run_quality_score": str(LOCAL_LOOKER_PATH),
        },
    }


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_run_quality_score(), indent=2, ensure_ascii=False))
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
