from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import normalize_text

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

PRESENCE_TAB = "FLIPKART_LISTING_PRESENCE"
MISSING_TAB = "FLIPKART_MISSING_ACTIVE_LISTINGS"
ISSUES_TAB = "FLIPKART_LISTING_STATUS_ISSUES"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"

SKU_ANALYSIS_COLUMNS = {
    "Listing_Presence_Status",
    "Found_In_Active_Listing",
    "Listing_Check_Action",
}


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


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return
    raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def count_duplicates(rows: Sequence[Dict[str, Any]], field_name: Optional[str]) -> int:
    if not field_name:
        return len(rows)
    values = [normalize_text(row.get(field_name, "")) for row in rows]
    counts = Counter(value for value in values if value)
    return sum(count - 1 for count in counts.values() if count > 1)


def pick_field(headers: Sequence[str], desired: str) -> Optional[str]:
    if desired in headers:
        return desired
    desired_norm = normalize_text(desired).lower()
    for header in headers:
        if normalize_text(header).lower() == desired_norm:
            return header
    return None


def main() -> None:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [PRESENCE_TAB, MISSING_TAB, ISSUES_TAB, SKU_ANALYSIS_TAB]:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)

    presence_headers, presence_rows = read_table(sheets_service, spreadsheet_id, PRESENCE_TAB)
    missing_headers, missing_rows = read_table(sheets_service, spreadsheet_id, MISSING_TAB)
    issue_headers, issue_rows = read_table(sheets_service, spreadsheet_id, ISSUES_TAB)
    sku_headers, sku_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)

    fsn_field_presence = pick_field(presence_headers, "FSN")
    fsn_field_missing = pick_field(missing_headers, "FSN")
    fsn_field_issues = pick_field(issue_headers, "FSN")
    fsn_field_sku = pick_field(sku_headers, "FSN")

    found_count = sum(1 for row in presence_rows if normalize_text(row.get("Found_In_Active_Listing", "")) == "Yes")
    missing_count = sum(1 for row in presence_rows if normalize_text(row.get("Found_In_Active_Listing", "")) == "No")
    blank_fsn_count = sum(1 for row in presence_rows if not normalize_text(row.get(fsn_field_presence or "FSN", "")))
    duplicate_fsn_count = count_duplicates(presence_rows, fsn_field_presence)

    no_fabricated_blocked_reasons = all(
        not normalize_text(row.get("Blocked_Reason", ""))
        and not normalize_text(row.get("Inactive_Reason", ""))
        and not normalize_text(row.get("Rejected_Reason", ""))
        for row in issue_rows
    )
    sku_analysis_listing_presence_columns_found = all(column in sku_headers for column in SKU_ANALYSIS_COLUMNS)

    checks = {
        "one_row_per_target_fsn": len(presence_rows) == found_count + missing_count,
        "missing_count_matches_missing_table_rows": missing_count == len(missing_rows),
        "issue_count_matches_missing_count": len(issue_rows) == missing_count,
        "no_blank_fsns": blank_fsn_count == 0,
        "no_duplicate_fsns": duplicate_fsn_count == 0,
        "no_fabricated_blocked_reasons": no_fabricated_blocked_reasons,
        "sku_analysis_has_listing_presence_columns": sku_analysis_listing_presence_columns_found,
    }

    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "status": status,
        "listing_presence_rows": len(presence_rows),
        "missing_active_listing_rows": len(missing_rows),
        "listing_status_issue_rows": len(issue_rows),
        "found_count": found_count,
        "missing_count": missing_count,
        "blank_fsn_count": blank_fsn_count,
        "duplicate_fsn_count": duplicate_fsn_count,
        "sku_analysis_listing_presence_columns_found": sku_analysis_listing_presence_columns_found,
        "checks": checks,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)
