from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_ads_mapping_helpers import (
    ADS_MASTER_TAB,
    ADS_MAPPING_ISSUES_TAB,
    ADS_PLANNER_AD_COLUMNS,
    ADS_PLANNER_TAB,
    ADS_SUMMARY_TAB,
    SPREADSHEET_META_PATH,
    build_sheets_service,
    load_json_file,
    read_table,
    tab_exists,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text


def duplicate_key_count(rows: List[Dict[str, str]], fields: List[str]) -> int:
    counts = Counter()
    for row in rows:
        key = tuple(normalize_text(row.get(field, "")) for field in fields)
        if all(not part for part in key):
            continue
        counts[key] += 1
    return sum(count - 1 for count in counts.values() if count > 1)


def issue_distribution(rows: List[Dict[str, str]]) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        issue_type = normalize_text(row.get("Issue_Type", "")) or "Unknown"
        counter[issue_type] += 1
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def main() -> None:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    spreadsheet_id = load_json_file(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_sheets_service()

    for tab_name in [ADS_MASTER_TAB, ADS_MAPPING_ISSUES_TAB, ADS_SUMMARY_TAB, ADS_PLANNER_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    _, master_rows = read_table(sheets_service, spreadsheet_id, ADS_MASTER_TAB)
    _, issue_rows = read_table(sheets_service, spreadsheet_id, ADS_MAPPING_ISSUES_TAB)
    _, summary_rows = read_table(sheets_service, spreadsheet_id, ADS_SUMMARY_TAB)
    planner_headers, planner_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)

    blank_fsn_in_master_count = sum(1 for row in master_rows if not clean_fsn(row.get("FSN", "")))
    duplicate_ad_master_key_count = duplicate_key_count(master_rows, ["FSN", "Campaign_ID", "Campaign_Name", "AdGroup_ID", "AdGroup_Name"])
    master_fsns = {clean_fsn(row.get("FSN", "")) for row in master_rows if clean_fsn(row.get("FSN", ""))}
    summary_fsns = {clean_fsn(row.get("FSN", "")) for row in summary_rows if clean_fsn(row.get("FSN", ""))}
    planner_status_columns_present = all(column in planner_headers for column in ADS_PLANNER_AD_COLUMNS)
    planner_fsns_with_ads_data = sum(
        1
        for row in planner_rows
        if normalize_text(row.get("Current_Ad_Status", "")) in {"Ads Data Available", "Ads Mapping Issue"}
    )
    issue_types = issue_distribution(issue_rows)
    mapping_issue_present = any(issue_types.get(name, 0) > 0 for name in ["SKU Maps To Multiple FSNs", "No Matching FSN", "No Mapping Key"])

    checks = {
        "no_blank_fsn_in_ads_master": blank_fsn_in_master_count == 0,
        "no_duplicate_silent_ad_master_keys": duplicate_ad_master_key_count == 0,
        "ads_summary_rows_match_unique_fsns_in_ads_master": len(summary_fsns) == len(master_fsns),
        "planner_updated_with_ads_status_columns": planner_status_columns_present,
        "mapping_issues_present_if_ambiguity_or_no_match_exists": (not mapping_issue_present) or len(issue_rows) > 0,
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    payload = {
        "status": status,
        "ads_master_rows": len(master_rows),
        "mapping_issue_rows": len(issue_rows),
        "ads_summary_fsn_rows": len(summary_rows),
        "planner_rows": len(planner_rows),
        "planner_fsns_with_ads_data": planner_fsns_with_ads_data,
        "duplicate_ad_master_key_count": duplicate_ad_master_key_count,
        "blank_fsn_in_ads_master_count": blank_fsn_in_master_count,
        "issue_type_distribution": issue_types,
        "checks": checks,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
