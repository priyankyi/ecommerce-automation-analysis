from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.create_flipkart_return_comments_analysis import (
    DETAIL_TAB,
    OUTPUT_TABS,
    PIVOT_TAB,
    SPREADSHEET_META_PATH,
    SUMMARY_TAB,
    ensure_required_tab_exists,
    normalize_text,
    read_table,
)

SEVERITY_ORDER = ["Critical", "High", "Medium", "Low"]
TAB_LABELS = [DETAIL_TAB, SUMMARY_TAB, PIVOT_TAB]


def pick_field(headers: Sequence[str], desired: str) -> str | None:
    if desired in headers:
        return desired
    desired_norm = normalize_text(desired).lower()
    for header in headers:
        if normalize_text(header).lower() == desired_norm:
            return header
    return None


def count_duplicates(rows: Sequence[Dict[str, str]], field_name: str | None) -> int:
    if not field_name:
        return len(rows)
    values = [normalize_text(row.get(field_name, "")) for row in rows]
    counts = Counter(value for value in values if value)
    return sum(count - 1 for count in counts.values() if count > 1)


def count_blank(rows: Sequence[Dict[str, str]], field_name: str | None) -> int:
    if not field_name:
        return len(rows)
    return sum(1 for row in rows if not normalize_text(row.get(field_name, "")))


def ordered_distribution(rows: Sequence[Dict[str, str]], field_name: str | None, preferred_order: Sequence[str]) -> Dict[str, int]:
    if not field_name:
        return {}
    counts = Counter(normalize_text(row.get(field_name, "")) or "(blank)" for row in rows)
    ordered: Dict[str, int] = {}
    for key in preferred_order:
        if key in counts:
            ordered[key] = counts.pop(key)
    for key in sorted(counts):
        ordered[key] = counts[key]
    return ordered


def sample_critical_issues(rows: Sequence[Dict[str, str]], limit: int = 5) -> List[Dict[str, str]]:
    fields = [
        "Run_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Order_ID",
        "Order_Item_ID",
        "Return_ID",
        "Return_Reason",
        "Return_Sub_Reason",
        "Comments",
        "Issue_Category",
        "Issue_Severity",
        "Issue_Source",
        "Suggested_Action",
    ]
    output: List[Dict[str, str]] = []
    for row in rows:
        if normalize_text(row.get("Issue_Severity", "")) != "Critical":
            continue
        output.append({field: normalize_text(row.get(field, "")) for field in fields})
        if len(output) >= limit:
            break
    return output


def summarize_tabs() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in OUTPUT_TABS:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)

    detail_headers, detail_rows = read_table(sheets_service, spreadsheet_id, DETAIL_TAB)
    summary_headers, summary_rows = read_table(sheets_service, spreadsheet_id, SUMMARY_TAB)
    pivot_headers, pivot_rows = read_table(sheets_service, spreadsheet_id, PIVOT_TAB)

    return_id_field = pick_field(detail_headers, "Return_ID")
    fsn_field = pick_field(detail_headers, "FSN")
    issue_category_field = pick_field(detail_headers, "Issue_Category")
    severity_field = pick_field(detail_headers, "Issue_Severity")

    duplicate_return_id_count = count_duplicates(detail_rows, return_id_field)
    blank_fsn_count = count_blank(detail_rows, fsn_field)
    issue_category_distribution = ordered_distribution(detail_rows, issue_category_field, [])
    severity_distribution = ordered_distribution(detail_rows, severity_field, SEVERITY_ORDER)
    sample_critical = sample_critical_issues(detail_rows, limit=5)

    checks = {
        "detail_rows_present": len(detail_rows) > 0,
        "summary_rows_present": len(summary_rows) > 0,
        "pivot_rows_present": len(pivot_rows) > 0,
        "no_blank_fsns": blank_fsn_count == 0,
        "no_duplicate_return_ids": duplicate_return_id_count == 0,
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "return_comments_rows": len(detail_rows),
        "return_issue_summary_rows": len(summary_rows),
        "return_reason_pivot_rows": len(pivot_rows),
        "issue_category_distribution": issue_category_distribution,
        "severity_distribution": severity_distribution,
        "duplicate_return_id_count": duplicate_return_id_count,
        "blank_fsn_count": blank_fsn_count,
        "sample_critical_issues": sample_critical,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        print(json.dumps(summarize_tabs(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
