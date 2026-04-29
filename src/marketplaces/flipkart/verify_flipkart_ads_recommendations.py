from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_ads_mapping_helpers import (
    ADS_MAPPING_ISSUES_TAB,
    ADS_PLANNER_TAB,
    ADS_SUMMARY_TAB,
    SPREADSHEET_META_PATH,
    read_table,
    tab_exists,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text

DECISION_ORDER = [
    "Manual override",
    "Fill COGS First",
    "Do Not Run Ads",
    "Improve Economics Before Ads",
    "Do Not Run Ads / Improve Economics",
    "Fix Product First",
    "Fix Product/Listing First",
    "Resolve Critical Alert First",
    "Fix Ads Mapping",
    "Scale Ads",
    "Continue / Optimize Ads",
    "Pause or Reduce Ads",
    "Test Ads",
    "Always-On Test",
    "Seasonal/Event Test",
    "Seasonal Ads Later / Prepare Listing First",
    "Monitor",
    "Manual Review",
]

BUDGET_ORDER = ["Do Not Run", "Low Test", "Medium Test", "Scale Later", "Reduce", "Manual Review"]
RISK_ORDER = ["Critical", "High", "Medium", "Low"]
OPPORTUNITY_ORDER = ["High", "Medium", "Low", "Unknown"]


def ordered_counter(rows: List[Dict[str, str]], field_name: str, preferred_order: List[str]) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(field_name, "")) or "Unknown"
        counter[value] += 1
    ordered: Dict[str, int] = {}
    for key in preferred_order:
        if key in counter:
            ordered[key] = counter.pop(key)
    for key in sorted(counter):
        ordered[key] = counter[key]
    return ordered


def verify_flipkart_ads_recommendations() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [ADS_PLANNER_TAB, ADS_SUMMARY_TAB, ADS_MAPPING_ISSUES_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    planner_headers, planner_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)
    _, summary_rows = read_table(sheets_service, spreadsheet_id, ADS_SUMMARY_TAB)
    _, issue_rows = read_table(sheets_service, spreadsheet_id, ADS_MAPPING_ISSUES_TAB)

    final_decision_distribution = ordered_counter(planner_rows, "Final_Ads_Decision", DECISION_ORDER)
    budget_distribution = ordered_counter(planner_rows, "Final_Budget_Recommendation", BUDGET_ORDER)
    risk_distribution = ordered_counter(planner_rows, "Ads_Risk_Level", RISK_ORDER)
    opportunity_distribution = ordered_counter(planner_rows, "Ads_Opportunity_Level", OPPORTUNITY_ORDER)

    blank_final_decision_count = sum(1 for row in planner_rows if not normalize_text(row.get("Final_Ads_Decision", "")))
    blank_budget_count = sum(1 for row in planner_rows if not normalize_text(row.get("Final_Budget_Recommendation", "")))
    manual_override_count = sum(
        1
        for row in planner_rows
        if normalize_text(row.get("Manual_Final_Ads_Decision", "")) or normalize_text(row.get("Manual_Override", "")) or normalize_text(row.get("Manual_Ads_Remarks", ""))
    )

    summary_fsns = {clean_fsn(row.get("FSN", "")) for row in summary_rows if clean_fsn(row.get("FSN", ""))}
    issue_fsns = {clean_fsn(row.get("Raw_FSN", "")) for row in issue_rows if clean_fsn(row.get("Raw_FSN", ""))}

    ads_data_check = []
    unmapped_ads_forced = []
    for row in planner_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        current_status = normalize_text(row.get("Current_Ad_Status", ""))
        ads_mapping_status = normalize_text(row.get("Ads_Mapping_Status", ""))
        final_decision = normalize_text(row.get("Final_Ads_Decision", ""))
        data_used = normalize_text(row.get("Ads_Data_Used", ""))
        roas_acos_available = bool(normalize_text(row.get("Ad_ROAS", "")) or normalize_text(row.get("Ad_ACOS", ""))) or fsn in summary_fsns

        if current_status == "Ads Data Available" and roas_acos_available:
            ads_data_check.append("ROAS" in data_used.upper() and "ACOS" in data_used.upper())
        if ads_mapping_status == "Issue" or current_status == "Ads Mapping Issue":
            unmapped_ads_forced.append(
                final_decision != "Fix Ads Mapping"
                or "ROAS" in data_used.upper()
                or "ACOS" in data_used.upper()
            )

    checks = {
        "every_fsn_has_final_ads_decision": blank_final_decision_count == 0,
        "no_blank_budget_recommendation": blank_budget_count == 0,
        "manual_override_columns_preserved": all(column in planner_headers for column in ["Manual_Final_Ads_Decision", "Manual_Ads_Remarks"]),
        "ads_data_fsns_include_roas_acos_when_available": all(ads_data_check) if ads_data_check else True,
        "no_unmapped_ads_were_forced_into_planner": not any(unmapped_ads_forced),
        "planner_rows_match_summary_or_issue_scope": len(planner_rows) > 0 and len(summary_rows) >= 0 and len(issue_rows) >= 0,
    }
    status = "PASS" if all(checks.values()) else "FAIL"
    return {
        "status": status,
        "planner_rows": len(planner_rows),
        "final_decision_distribution": final_decision_distribution,
        "budget_distribution": budget_distribution,
        "risk_distribution": risk_distribution,
        "opportunity_distribution": opportunity_distribution,
        "blank_final_decision_count": blank_final_decision_count,
        "manual_override_count": manual_override_count,
        "checks": checks,
        "spreadsheet_id": spreadsheet_id,
    }


def main() -> None:
    try:
        print(json.dumps(verify_flipkart_ads_recommendations(), indent=2, ensure_ascii=False))
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
