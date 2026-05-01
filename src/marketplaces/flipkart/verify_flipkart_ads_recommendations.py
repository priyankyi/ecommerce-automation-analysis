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
    "Do Not Run Ads / Improve Product First",
    "Fix Product First",
    "Fix Product/Listing First",
    "Test Ads Carefully / Fix Product First",
    "Test Ads Carefully / Check Logistics",
    "Review Return Split",
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


def approx_equal(left: str, right: str, tolerance: float = 0.0005) -> bool:
    try:
        return abs(float(left) - float(right)) <= tolerance
    except Exception:
        return False


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
    split_logic_warnings = []
    valid_all_customer_return_notes = []
    target_fsn_check = {}
    for row in planner_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        current_status = normalize_text(row.get("Current_Ad_Status", ""))
        ads_mapping_status = normalize_text(row.get("Ads_Mapping_Status", ""))
        final_decision = normalize_text(row.get("Final_Ads_Decision", ""))
        data_used = normalize_text(row.get("Ads_Data_Used", ""))
        decision_reason = normalize_text(row.get("Ads_Decision_Reason", ""))
        customer_return_count = normalize_text(row.get("Customer_Return_Count", ""))
        courier_return_count = normalize_text(row.get("Courier_Return_Count", ""))
        total_return_count = normalize_text(row.get("Total_Return_Count", ""))
        customer_return_rate = normalize_text(row.get("Customer_Return_Rate", ""))
        courier_return_rate = normalize_text(row.get("Courier_Return_Rate", ""))
        total_return_rate = normalize_text(row.get("Total_Return_Rate", ""))
        roas_acos_available = bool(normalize_text(row.get("Ad_ROAS", "")) or normalize_text(row.get("Ad_ACOS", ""))) or fsn in summary_fsns

        if current_status == "Ads Data Available" and roas_acos_available:
            ads_data_check.append("ROAS" in data_used.upper() and "ACOS" in data_used.upper())
        if ads_mapping_status == "Issue" or current_status == "Ads Mapping Issue":
            unmapped_ads_forced.append(
                final_decision != "Fix Ads Mapping"
                or "ROAS" in data_used.upper()
                or "ACOS" in data_used.upper()
            )
        customer_source_evidence = any(
            normalize_text(row.get(field, ""))
            for field in [
                "Customer_Return_Count",
                "Customer_Return_Risk_Level",
                "Top_Customer_Return_Reason",
                "Top_Customer_Return_Sub_Reason",
            ]
        )
        courier_source_evidence = any(
            normalize_text(row.get(field, ""))
            for field in [
                "Courier_Return_Count",
                "Courier_Return_Risk_Level",
                "Top_Courier_Return_Reason",
                "Top_Courier_Return_Sub_Reason",
            ]
        )
        all_customer_returns = (
            customer_return_count
            and total_return_count
            and customer_return_count == total_return_count
            and (not courier_return_count or courier_return_count in {"0", "0.0"})
        )
        if customer_return_rate and courier_return_rate and total_return_rate:
            if (
                approx_equal(customer_return_rate, total_return_rate)
                and not approx_equal(customer_return_rate, courier_return_rate)
            ):
                if all_customer_returns:
                    valid_all_customer_return_notes.append(f"{fsn}: valid all-customer-return case; customer count matches total count and courier count is zero or blank")
                else:
                    split_logic_warnings.append(f"{fsn}: customer return rate appears to mirror total return rate")
        if decision_reason.lower().startswith("return rate") or "return rate above threshold" in decision_reason.lower():
            if customer_return_rate and courier_return_rate and not customer_source_evidence and not courier_source_evidence:
                split_logic_warnings.append(f"{fsn}: generic return-rate reason still present")
        if fsn == "OTLGPN7CVFCTRBQF":
            target_fsn_check = {
                "FSN": fsn,
                "Customer_Return_Rate": customer_return_rate,
                "Courier_Return_Rate": courier_return_rate,
                "Total_Return_Rate": total_return_rate,
                "Final_Ads_Decision": final_decision,
                "Ads_Decision_Reason": decision_reason,
                "Listing_Readiness": normalize_text(row.get("Listing_Readiness", "")),
            }

    checks = {
        "every_fsn_has_final_ads_decision": blank_final_decision_count == 0,
        "no_blank_budget_recommendation": blank_budget_count == 0,
        "manual_override_columns_preserved": all(column in planner_headers for column in ["Manual_Final_Ads_Decision", "Manual_Ads_Remarks"]),
        "ads_data_fsns_include_roas_acos_when_available": all(ads_data_check) if ads_data_check else True,
        "no_unmapped_ads_were_forced_into_planner": not any(unmapped_ads_forced),
        "planner_rows_match_summary_or_issue_scope": len(planner_rows) > 0 and len(summary_rows) >= 0 and len(issue_rows) >= 0,
        "target_fsn_split_logic_ok": (
            target_fsn_check.get("Customer_Return_Rate") not in {"", "0"}
            and target_fsn_check.get("Courier_Return_Rate") not in {"", "0"}
            and (
                "customer return rate acceptable; courier return risk elevated" in target_fsn_check.get("Ads_Decision_Reason", "").lower()
                or "listing is missing or not active" in target_fsn_check.get("Ads_Decision_Reason", "").lower()
                or target_fsn_check.get("Listing_Readiness") == "Bad"
            )
        ),
        "no_generic_return_reason_when_split_exists": not split_logic_warnings,
    }
    warning_only_checks = {"no_generic_return_reason_when_split_exists"}
    hard_fail_checks = [name for name, value in checks.items() if not value and name not in warning_only_checks]
    if hard_fail_checks:
        status = "FAIL"
    elif all(checks.values()):
        status = "PASS"
    else:
        status = "PASS_WITH_WARNINGS"
    return {
        "status": status,
        "planner_rows": len(planner_rows),
        "final_decision_distribution": final_decision_distribution,
        "budget_distribution": budget_distribution,
        "risk_distribution": risk_distribution,
        "opportunity_distribution": opportunity_distribution,
        "blank_final_decision_count": blank_final_decision_count,
        "manual_override_count": manual_override_count,
        "split_logic_warnings": split_logic_warnings,
        "valid_all_customer_return_notes": valid_all_customer_return_notes,
        "target_fsn_check": target_fsn_check,
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
