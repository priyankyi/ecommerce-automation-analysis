from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.create_flipkart_ads_planner_foundation import (
    DEFAULT_DEMAND_PROFILES,
    compute_ad_dates,
    demand_profile_lookup,
    pick_active_task_row,
)
from src.marketplaces.flipkart.flipkart_ads_mapping_helpers import (
    ADS_MAPPING_ISSUES_TAB,
    ADS_PLANNER_TAB,
    ADS_SUMMARY_TAB,
    LOCAL_ADS_PLANNER_PATH,
    SPREADSHEET_META_PATH,
    build_sheets_service,
    ensure_tab,
    read_table,
    tab_exists,
    write_output_tab,
)
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    format_decimal,
    normalize_text,
    now_iso,
    parse_float,
)

LOG_PATH = LOG_DIR / "flipkart_ads_recommendations_log.csv"
LOCAL_OUTPUT_PATH = OUTPUT_DIR / "flipkart_ads_final_recommendations.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
CUSTOMER_RETURN_SUMMARY_TAB = "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY"
COURIER_RETURN_SUMMARY_TAB = "FLIPKART_COURIER_RETURN_SUMMARY"
RETURN_TYPE_PIVOT_TAB = "FLIPKART_RETURN_TYPE_PIVOT"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"
PRODUCT_AD_PROFILE_TAB = "FLIPKART_PRODUCT_AD_PROFILE"
DEMAND_PROFILE_TAB = "PRODUCT_TYPE_DEMAND_PROFILE"

REQUIRED_TABS = [
    ADS_PLANNER_TAB,
    ADS_SUMMARY_TAB,
    ADS_MAPPING_ISSUES_TAB,
    SKU_ANALYSIS_TAB,
    RETURN_ISSUE_SUMMARY_TAB,
    CUSTOMER_RETURN_SUMMARY_TAB,
    COURIER_RETURN_SUMMARY_TAB,
    ACTIVE_TASKS_TAB,
    PRODUCT_AD_PROFILE_TAB,
    DEMAND_PROFILE_TAB,
]

DECISION_PRIORITY = [
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

RISK_ORDER = ["Critical", "High", "Medium", "Low"]
OPPORTUNITY_ORDER = ["High", "Medium", "Low", "Unknown"]
BUDGET_ORDER = ["Do Not Run", "Low Test", "Medium Test", "Scale Later", "Reduce", "Manual Review"]

DECISION_METADATA: Dict[str, Dict[str, str]] = {
    "Fill COGS First": {
        "budget": "Do Not Run",
        "risk": "High",
        "opportunity": "Low",
    },
    "Do Not Run Ads": {
        "budget": "Do Not Run",
        "risk": "Critical",
        "opportunity": "Low",
    },
    "Do Not Run Ads / Improve Economics": {
        "budget": "Do Not Run",
        "risk": "High",
        "opportunity": "Low",
    },
    "Do Not Run Ads / Improve Product First": {
        "budget": "Do Not Run",
        "risk": "Critical",
        "opportunity": "Low",
    },
    "Improve Economics Before Ads": {
        "budget": "Do Not Run",
        "risk": "High",
        "opportunity": "Low",
    },
    "Fix Product First": {
        "budget": "Do Not Run",
        "risk": "Critical",
        "opportunity": "Low",
    },
    "Fix Product/Listing First": {
        "budget": "Do Not Run",
        "risk": "High",
        "opportunity": "Low",
    },
    "Test Ads Carefully / Fix Product First": {
        "budget": "Low Test",
        "risk": "High",
        "opportunity": "Medium",
    },
    "Test Ads Carefully / Check Logistics": {
        "budget": "Low Test",
        "risk": "Medium",
        "opportunity": "Medium",
    },
    "Resolve Critical Alert First": {
        "budget": "Do Not Run",
        "risk": "High",
        "opportunity": "Low",
    },
    "Fix Ads Mapping": {
        "budget": "Do Not Run",
        "risk": "Medium",
        "opportunity": "Low",
    },
    "Scale Ads": {
        "budget": "Medium Test",
        "risk": "Low",
        "opportunity": "High",
    },
    "Continue / Optimize Ads": {
        "budget": "Medium Test",
        "risk": "Medium",
        "opportunity": "Medium",
    },
    "Pause or Reduce Ads": {
        "budget": "Reduce",
        "risk": "High",
        "opportunity": "Low",
    },
    "Test Ads": {
        "budget": "Low Test",
        "risk": "Low",
        "opportunity": "Medium",
    },
    "Always-On Test": {
        "budget": "Medium Test",
        "risk": "Low",
        "opportunity": "Medium",
    },
    "Seasonal/Event Test": {
        "budget": "Low Test",
        "risk": "Low",
        "opportunity": "Medium",
    },
    "Seasonal Ads Later / Prepare Listing First": {
        "budget": "Scale Later",
        "risk": "Medium",
        "opportunity": "Medium",
    },
    "Monitor": {
        "budget": "Manual Review",
        "risk": "Medium",
        "opportunity": "Unknown",
    },
    "Manual Review": {
        "budget": "Manual Review",
        "risk": "Medium",
        "opportunity": "Unknown",
    },
}

SEASONAL_AD_DECISIONS = {
    "Diwali Heavy": ("Seasonal Ads Later / Prepare Listing First", "Scale Later"),
    "Festive Boost": ("Seasonal Ads Later / Prepare Listing First", "Scale Later"),
}

READY_SEASONALITY_TEST = {
    "Year-Round",
    "Year-Round Home Exterior",
    "Utility / Outdoor",
    "Year-Round + Festive Boost",
}

ACTIVE_DECISIONS = {"Scale Ads", "Continue / Optimize Ads", "Test Ads", "Always-On Test", "Seasonal/Event Test", "Test Ads Carefully / Fix Product First", "Test Ads Carefully / Check Logistics"}
BLOCKED_DECISIONS = {
    "Fill COGS First",
    "Do Not Run Ads",
    "Do Not Run Ads / Improve Economics",
    "Improve Economics Before Ads",
    "Fix Product First",
    "Fix Product/Listing First",
    "Do Not Run Ads / Improve Product First",
    "Test Ads Carefully / Fix Product First",
    "Test Ads Carefully / Check Logistics",
    "Resolve Critical Alert First",
    "Fix Ads Mapping",
    "Review Return Split",
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def read_sheet_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def ordered_counter(rows: Sequence[Dict[str, Any]], field_name: str, preferred_order: Sequence[str]) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(field_name, "")) or "Unknown"
        counter[value] += 1
    ordered: Dict[str, int] = {}
    for item in preferred_order:
        if item in counter:
            ordered[item] = counter.pop(item)
    for item in sorted(counter):
        ordered[item] = counter[item]
    return ordered


def build_index(rows: Sequence[Dict[str, str]], field_name: str = "FSN") -> Dict[str, Dict[str, str]]:
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get(field_name, ""))
        if fsn and fsn not in indexed:
            indexed[fsn] = dict(row)
    return indexed


def build_grouped_tasks(rows: Sequence[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        grouped.setdefault(fsn, []).append(dict(row))
    return grouped


def highest_severity_task(rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    return pick_active_task_row(rows)


def safe_float(value: Any) -> float:
    return parse_float(value)


def safe_date_iso(value: str) -> str:
    if not normalize_text(value):
        return ""
    try:
        return date.fromisoformat(normalize_text(value)).isoformat()
    except ValueError:
        return ""


def clamp_future_date(value: str, fallback_days: int) -> str:
    today = date.today()
    parsed = safe_date_iso(value)
    if parsed:
        try:
            parsed_date = date.fromisoformat(parsed)
        except ValueError:
            parsed_date = today
        return max(parsed_date, today).isoformat()
    return (today + timedelta(days=fallback_days)).isoformat()


def pick_first_nonblank(*values: Any) -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return ""


def resolve_product_context(
    fsn: str,
    planner_row: Dict[str, str],
    analysis_row: Dict[str, str],
    product_profile_row: Dict[str, str],
    demand_row: Dict[str, Any],
) -> Tuple[str, str, str]:
    final_product_type = pick_first_nonblank(
        planner_row.get("Final_Product_Type", ""),
        product_profile_row.get("Final_Product_Type", ""),
        product_profile_row.get("Detected_Product_Type", ""),
        analysis_row.get("Detected_Product_Type", ""),
        "Unknown",
    )
    final_seasonality_tag = pick_first_nonblank(
        planner_row.get("Final_Seasonality_Tag", ""),
        product_profile_row.get("Final_Seasonality_Tag", ""),
        product_profile_row.get("Detected_Seasonality_Tag", ""),
        analysis_row.get("Detected_Seasonality_Tag", ""),
        "Unknown",
    )
    ad_run_type = pick_first_nonblank(
        planner_row.get("Ad_Run_Type", ""),
        product_profile_row.get("Ad_Run_Type", ""),
        "",
    )
    if not ad_run_type and final_seasonality_tag:
        ad_run_type = pick_first_nonblank(demand_row.get("Recommended_Ad_Window", ""), "Manual Review")
    if not ad_run_type:
        ad_run_type = "Manual Review"
    return final_product_type, final_seasonality_tag, ad_run_type


def resolve_readiness(
    planner_row: Dict[str, str],
    analysis_row: Dict[str, str],
    active_task_row: Dict[str, Any],
) -> Dict[str, str]:
    cogs_status = pick_first_nonblank(planner_row.get("COGS_Readiness", ""), analysis_row.get("COGS_Status", ""))
    if not cogs_status:
        cogs_status = "Missing"
    cogs_ready = "Ready" if normalize_text(cogs_status).upper() in {"READY", "ENTERED", "VERIFIED"} else "Missing"

    profit_readiness = pick_first_nonblank(planner_row.get("Profit_Readiness", ""), "")
    if not profit_readiness:
        final_profit_margin = safe_float(analysis_row.get("Final_Profit_Margin", ""))
        if cogs_ready != "Ready":
            profit_readiness = "Unknown"
        elif final_profit_margin >= 0.20:
            profit_readiness = "Strong"
        elif final_profit_margin < 0.10:
            profit_readiness = "Weak"
        else:
            profit_readiness = "Moderate"

    customer_return_rate = safe_float(planner_row.get("Customer_Return_Rate", ""))
    total_return_rate = safe_float(planner_row.get("Total_Return_Rate", ""))
    split_available = bool(
        normalize_text(planner_row.get("Customer_Return_Rate", ""))
        or normalize_text(planner_row.get("Courier_Return_Rate", ""))
    )
    return_readiness = pick_first_nonblank(planner_row.get("Return_Readiness", ""), "")
    if not return_readiness:
        if split_available:
            if customer_return_rate >= 0.50:
                return_readiness = "Critical"
            elif customer_return_rate >= 0.20:
                return_readiness = "Bad"
            elif customer_return_rate < 0.15:
                return_readiness = "Good"
            else:
                return_readiness = "Review"
        else:
            return_readiness = "Review"

    listing_readiness = pick_first_nonblank(planner_row.get("Listing_Readiness", ""), "")
    if not listing_readiness:
        listing_status = normalize_text(analysis_row.get("Listing_Status", ""))
        listing_readiness = "Bad" if any(token in listing_status.lower() for token in ("missing", "inactive", "blocked", "not active", "unlisted", "paused")) else "Good"

    data_readiness = pick_first_nonblank(planner_row.get("Data_Readiness", ""), "")
    if not data_readiness:
        data_confidence = normalize_text(analysis_row.get("Data_Confidence", "")).upper()
        data_readiness = "Good" if data_confidence == "HIGH" else "Review"

    alert_readiness = pick_first_nonblank(planner_row.get("Alert_Readiness", ""), "")
    if not alert_readiness:
        active_severity = normalize_text(active_task_row.get("Severity", ""))
        if active_severity == "Critical":
            alert_readiness = "Bad"
        elif active_severity == "High":
            alert_readiness = "Review"
        else:
            alert_readiness = "Good"

    return {
        "COGS_Readiness": cogs_ready,
        "Profit_Readiness": profit_readiness,
        "Return_Readiness": return_readiness,
        "Listing_Readiness": listing_readiness,
        "Data_Readiness": data_readiness,
        "Alert_Readiness": alert_readiness,
        "Final_Net_Profit": format_decimal(analysis_row.get("Final_Net_Profit", ""), 2) if normalize_text(analysis_row.get("Final_Net_Profit", "")) else "",
        "Final_Profit_Margin": format_decimal(analysis_row.get("Final_Profit_Margin", ""), 4) if normalize_text(analysis_row.get("Final_Profit_Margin", "")) else "",
        "Return_Rate": format_decimal(planner_row.get("Customer_Return_Rate", ""), 4) if normalize_text(planner_row.get("Customer_Return_Rate", "")) else "",
        "Customer_Return_Rate": format_decimal(planner_row.get("Customer_Return_Rate", ""), 4) if normalize_text(planner_row.get("Customer_Return_Rate", "")) else "",
    }


def resolve_ads_metrics(
    planner_row: Dict[str, str],
    summary_row: Dict[str, str],
    mapping_issue_present: bool,
) -> Dict[str, Any]:
    current_status = pick_first_nonblank(planner_row.get("Current_Ad_Status", ""), "")
    ads_mapping_status = pick_first_nonblank(planner_row.get("Ads_Mapping_Status", ""), "")
    ad_rows = pick_first_nonblank(planner_row.get("Ad_Rows", ""), summary_row.get("Ad_Rows", ""))
    ad_roas = pick_first_nonblank(summary_row.get("ROAS", ""), planner_row.get("Ad_ROAS", ""))
    ad_acos = pick_first_nonblank(summary_row.get("ACOS", ""), planner_row.get("Ad_ACOS", ""))
    estimated_spend = pick_first_nonblank(summary_row.get("Estimated_Ad_Spend", ""), planner_row.get("Estimated_Ad_Spend", ""))
    ad_views = pick_first_nonblank(summary_row.get("Views", ""), planner_row.get("Ad_Views", ""))
    ad_clicks = pick_first_nonblank(summary_row.get("Clicks", ""), planner_row.get("Ad_Clicks", ""))

    if mapping_issue_present or normalize_text(ads_mapping_status) == "Issue" or normalize_text(current_status) == "Ads Mapping Issue":
        return {
            "current_status": "Ads Mapping Issue",
            "ads_mapping_status": "Issue",
            "ad_rows": ad_rows,
            "ad_roas": "",
            "ad_acos": "",
            "estimated_spend": "",
            "ad_views": ad_views,
            "ad_clicks": ad_clicks,
            "ads_data_available": False,
        }

    ads_data_available = bool(normalize_text(ad_rows)) or bool(normalize_text(ad_roas)) or bool(normalize_text(ad_acos))
    if ads_data_available:
        current_status = "Ads Data Available"
        ads_mapping_status = "Mapped"
    else:
        current_status = "No Ads Data"
        ads_mapping_status = ""

    return {
        "current_status": current_status,
        "ads_mapping_status": ads_mapping_status,
        "ad_rows": ad_rows,
        "ad_roas": ad_roas,
        "ad_acos": ad_acos,
        "estimated_spend": estimated_spend,
        "ad_views": ad_views,
        "ad_clicks": ad_clicks,
        "ads_data_available": ads_data_available,
    }


def is_healthy_margin(analysis_row: Dict[str, str]) -> bool:
    return safe_float(analysis_row.get("Final_Profit_Margin", "")) >= 0.20


def has_negative_profit(analysis_row: Dict[str, str]) -> bool:
    return safe_float(analysis_row.get("Final_Net_Profit", "")) < 0


def has_cogs_missing(readiness: Dict[str, str]) -> bool:
    return normalize_text(readiness.get("COGS_Readiness", "")) != "Ready"


def build_ads_data_used(
    readiness: Dict[str, str],
    product_type: str,
    seasonality_tag: str,
    ads_metrics: Dict[str, Any],
    analysis_row: Dict[str, str],
    return_row: Dict[str, str],
    courier_row: Dict[str, str],
    active_task_row: Dict[str, Any],
) -> str:
    parts = [
        "Internal Profit",
        "COGS",
        "Customer Return Rate",
        "Courier Return Rate",
        "Active Alerts",
        "Product Type",
        "Seasonality",
    ]
    if ads_metrics["ads_data_available"]:
        parts.append("Ads ROAS/ACOS")
    else:
        parts.append("Ads ROAS/ACOS unavailable")
    if normalize_text(return_row.get("Customer_Return_Risk_Level", "")) or normalize_text(courier_row.get("Courier_Return_Risk_Level", "")):
        parts.append("Return Intelligence v2")
    if normalize_text(active_task_row.get("Severity", "")):
        parts.append("Active Alert Severity")
    if normalize_text(readiness.get("Final_Net_Profit", "")):
        parts.append("Final Net Profit")
    return "; ".join(parts)


def build_reason(
    final_decision: str,
    manual_override: bool,
    readiness: Dict[str, str],
    analysis_row: Dict[str, str],
    return_row: Dict[str, str],
    courier_row: Dict[str, str],
    active_task_row: Dict[str, Any],
    product_type: str,
    seasonality_tag: str,
    ads_metrics: Dict[str, Any],
) -> str:
    if manual_override:
        return "Manual override"
    if final_decision == "Fill COGS First":
        return "COGS missing"
    if final_decision == "Do Not Run Ads":
        return "Final net profit is negative"
    if final_decision in {"Do Not Run Ads / Improve Economics", "Improve Economics Before Ads"}:
        return "Final profit margin is below 10%"
    if final_decision == "Fix Product First":
        return "Customer return rate is critical"
    if final_decision == "Fix Product/Listing First":
        listing_status = normalize_text(analysis_row.get("Listing_Status", ""))
        if any(token in listing_status.lower() for token in ("missing", "inactive", "blocked", "not active", "unlisted", "paused")):
            return "Listing is missing or not active"
        return "Customer return rate is elevated"
    if final_decision == "Do Not Run Ads / Improve Product First":
        return "Customer return rate is critical and margin is weak"
    if final_decision == "Test Ads Carefully / Fix Product First":
        return "Customer return rate is elevated"
    if final_decision == "Test Ads Carefully / Check Logistics":
        return "Customer return rate acceptable; courier return risk elevated"
    if final_decision == "Review Return Split":
        return "Return split missing; review manually"
    if final_decision == "Resolve Critical Alert First":
        return "Critical active alert exists"
    if final_decision == "Fix Ads Mapping":
        return "Ads mapping issue; unmapped ads were not forced into planner"
    if final_decision == "Scale Ads":
        return f"Healthy margin with strong mapped ads performance (ROAS {ads_metrics['ad_roas'] or 'n/a'}, ACOS {ads_metrics['ad_acos'] or 'n/a'})"
    if final_decision == "Continue / Optimize Ads":
        return f"Mapped ads are acceptable but can be improved (ROAS {ads_metrics['ad_roas'] or 'n/a'}, ACOS {ads_metrics['ad_acos'] or 'n/a'})"
    if final_decision == "Pause or Reduce Ads":
        return f"Mapped ads ACOS is too high ({ads_metrics['ad_acos'] or 'n/a'})"
    if final_decision == "Test Ads":
        return f"No mapped ads data; {seasonality_tag or product_type} is ready for low-risk testing"
    if final_decision == "Always-On Test":
        return f"No mapped ads data; {seasonality_tag or product_type} supports an always-on test"
    if final_decision == "Seasonal/Event Test":
        return f"No mapped ads data; {seasonality_tag or product_type} supports a seasonal test"
    if final_decision == "Seasonal Ads Later / Prepare Listing First":
        return f"No mapped ads data; {seasonality_tag or product_type} is seasonal and should be prepared first"
    if final_decision == "Monitor":
        return "No mapped ads data and internal readiness is incomplete"
    if final_decision == "Manual Review":
        return "Product or ads state needs human review"
    return "Internal readiness and mapped ads performance reviewed"


def choose_decision(
    planner_row: Dict[str, str],
    analysis_row: Dict[str, str],
    return_row: Dict[str, str],
    courier_row: Dict[str, str],
    active_task_row: Dict[str, Any],
    product_type: str,
    seasonality_tag: str,
    ads_metrics: Dict[str, Any],
) -> Tuple[str, str, str, str, bool]:
    manual_final = pick_first_nonblank(planner_row.get("Manual_Final_Ads_Decision", ""), planner_row.get("Manual_Override", ""))
    manual_remarks = normalize_text(planner_row.get("Manual_Ads_Remarks", ""))
    manual_override = bool(manual_final)

    if manual_override:
        final_decision = manual_final
    else:
        readiness = resolve_readiness(planner_row, analysis_row, active_task_row)
        customer_return_rate = safe_float(return_row.get("Customer_Return_Rate", ""))
        courier_return_rate = safe_float(courier_row.get("Courier_Return_Rate", ""))
        split_available = bool(
            normalize_text(return_row.get("Customer_Return_Rate", ""))
            or normalize_text(courier_row.get("Courier_Return_Rate", ""))
        )
        profit_margin = safe_float(analysis_row.get("Final_Profit_Margin", ""))
        if has_cogs_missing(readiness):
            final_decision = "Fill COGS First"
        elif has_negative_profit(analysis_row):
            final_decision = "Do Not Run Ads"
        elif split_available and customer_return_rate >= 0.20 and profit_margin < 0.10:
            final_decision = "Do Not Run Ads / Improve Product First"
        elif split_available and customer_return_rate >= 0.50:
            final_decision = "Fix Product First"
        elif split_available and customer_return_rate >= 0.20 and profit_margin < 0.20:
            final_decision = "Test Ads Carefully / Fix Product First"
        elif split_available and customer_return_rate >= 0.20:
            final_decision = "Test Ads Carefully / Fix Product First"
        elif not split_available:
            final_decision = "Review Return Split"
        elif normalize_text(active_task_row.get("Severity", "")) == "Critical":
            final_decision = "Resolve Critical Alert First"
        elif normalize_text(ads_metrics["ads_mapping_status"]) == "Issue" or normalize_text(planner_row.get("Current_Ad_Status", "")) == "Ads Mapping Issue":
            final_decision = "Fix Ads Mapping"
        elif any(token in normalize_text(analysis_row.get("Listing_Status", "")).lower() for token in ("missing", "inactive", "blocked", "not active", "unlisted", "paused")):
            final_decision = "Fix Product/Listing First"
        elif split_available and courier_return_rate >= 0.20 and customer_return_rate < 0.20:
            final_decision = "Test Ads Carefully / Check Logistics"
        elif split_available and customer_return_rate < 0.15 and profit_margin >= 0.20 and ads_metrics["ads_data_available"] and safe_float(ads_metrics["ad_acos"]) <= 0.20 and safe_float(ads_metrics["ad_roas"]) >= 5:
            final_decision = "Scale Ads"
        elif split_available and customer_return_rate < 0.15 and profit_margin >= 0.20 and ads_metrics["ads_data_available"]:
            final_decision = "Continue / Optimize Ads"
        elif ads_metrics["ads_data_available"] and safe_float(ads_metrics["ad_acos"]) <= 0.20 and safe_float(ads_metrics["ad_roas"]) >= 5 and is_healthy_margin(analysis_row):
            final_decision = "Scale Ads"
        elif ads_metrics["ads_data_available"] and safe_float(ads_metrics["ad_acos"]) <= 0.35 and safe_float(ads_metrics["ad_roas"]) >= 3:
            final_decision = "Continue / Optimize Ads"
        elif ads_metrics["ads_data_available"] and safe_float(ads_metrics["ad_acos"]) > 0.35:
            final_decision = "Pause or Reduce Ads"
        elif not ads_metrics["ads_data_available"]:
            if seasonality_tag in SEASONAL_AD_DECISIONS:
                final_decision = SEASONAL_AD_DECISIONS[seasonality_tag][0]
            elif seasonality_tag in READY_SEASONALITY_TEST:
                final_decision = "Test Ads"
            elif normalize_text(product_type) == "Unknown":
                final_decision = "Manual Review"
            else:
                final_decision = "Monitor"
        else:
            final_decision = "Monitor"

    metadata = DECISION_METADATA.get(final_decision, {"budget": "Manual Review", "risk": "Medium", "opportunity": "Unknown"})
    return final_decision, metadata["budget"], metadata["risk"], metadata["opportunity"], manual_override


def compute_action_dates(
    final_decision: str,
    planner_row: Dict[str, str],
    demand_row: Dict[str, Any],
    product_type: str,
    seasonality_tag: str,
) -> Tuple[str, str]:
    today = date.today()
    start_preparation_date = safe_date_iso(planner_row.get("Start_Preparation_Date", ""))
    start_ads_date = safe_date_iso(planner_row.get("Start_Ads_Date", ""))

    if final_decision == "Seasonal Ads Later / Prepare Listing First":
        next_action = clamp_future_date(start_preparation_date, 30)
        review_date = clamp_future_date(start_ads_date or next_action, 14)
        return next_action, review_date

    if final_decision in ACTIVE_DECISIONS:
        return today.isoformat(), (today + timedelta(days=14)).isoformat()

    if final_decision == "Scale Ads":
        return today.isoformat(), (today + timedelta(days=14)).isoformat()

    if final_decision in {
        "Fill COGS First",
        "Do Not Run Ads",
        "Do Not Run Ads / Improve Economics",
        "Improve Economics Before Ads",
        "Fix Product First",
        "Fix Product/Listing First",
        "Resolve Critical Alert First",
        "Fix Ads Mapping",
        "Pause or Reduce Ads",
    }:
        return (today + timedelta(days=7)).isoformat(), (today + timedelta(days=14)).isoformat()

    if final_decision in {"Monitor", "Manual Review"}:
        return (today + timedelta(days=30)).isoformat(), (today + timedelta(days=30)).isoformat()

    if product_type in DEFAULT_DEMAND_PROFILES and seasonality_tag:
        start_prep, start_ads, _ = compute_ad_dates(product_type, seasonality_tag, demand_row, today)
        if start_prep or start_ads:
            next_action = clamp_future_date(start_prep or start_ads, 0)
            review_date = clamp_future_date(start_ads or start_prep or next_action, 14)
            return next_action, review_date

    return today.isoformat(), (today + timedelta(days=14)).isoformat()


def append_new_columns(headers: Sequence[str]) -> List[str]:
    new_headers = list(headers)
    for column in [
        "Final_Ads_Decision",
        "Final_Budget_Recommendation",
        "Ads_Decision_Reason",
        "Ads_Risk_Level",
        "Ads_Opportunity_Level",
        "Customer_Return_Count",
        "Courier_Return_Count",
        "Unknown_Return_Count",
        "Total_Return_Count",
        "Customer_Return_Rate",
        "Customer_Return_Risk_Level",
        "Courier_Return_Rate",
        "Courier_Return_Risk_Level",
        "Total_Return_Rate",
        "Next_Ad_Action_Date",
        "Ads_Review_Date",
        "Ads_Data_Used",
        "Manual_Final_Ads_Decision",
        "Manual_Ads_Remarks",
    ]:
        if column not in new_headers:
            new_headers.append(column)
    return new_headers


def write_local_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def build_final_rows(
    planner_headers: Sequence[str],
    planner_rows: Sequence[Dict[str, str]],
    analysis_rows: Sequence[Dict[str, str]],
    summary_rows: Sequence[Dict[str, str]],
    customer_summary_rows: Sequence[Dict[str, str]],
    courier_summary_rows: Sequence[Dict[str, str]],
    return_type_rows: Sequence[Dict[str, str]],
    issue_rows: Sequence[Dict[str, str]],
    active_task_rows: Sequence[Dict[str, str]],
    product_profile_rows: Sequence[Dict[str, str]],
    demand_profile_rows: Sequence[Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]]]:
    analysis_index = build_index(analysis_rows)
    summary_index = build_index(summary_rows)
    customer_summary_index = build_index(customer_summary_rows)
    courier_summary_index = build_index(courier_summary_rows)
    return_type_index = build_index(return_type_rows)
    product_profile_index = build_index(product_profile_rows)
    demand_lookup = demand_profile_lookup(demand_profile_rows)
    active_task_group = build_grouped_tasks(active_task_rows)
    issue_fsns = {clean_fsn(row.get("Raw_FSN", "")) for row in issue_rows if clean_fsn(row.get("Raw_FSN", ""))}

    final_headers = append_new_columns(planner_headers)
    final_rows: List[Dict[str, Any]] = []

    for planner_row in planner_rows:
        fsn = clean_fsn(planner_row.get("FSN", ""))
        analysis_row = analysis_index.get(fsn, {})
        summary_row = summary_index.get(fsn, {})
        customer_row = customer_summary_index.get(fsn, {})
        courier_row = courier_summary_index.get(fsn, {})
        return_type_row = return_type_index.get(fsn, {})
        product_profile_row = product_profile_index.get(fsn, {})
        active_row = highest_severity_task(active_task_group.get(fsn, []))
        demand_row = demand_lookup.get(
            pick_first_nonblank(
                planner_row.get("Final_Product_Type", ""),
                product_profile_row.get("Final_Product_Type", ""),
                analysis_row.get("Detected_Product_Type", ""),
                "Unknown",
            ),
            {},
        )
        final_product_type, final_seasonality_tag, ad_run_type = resolve_product_context(
            fsn,
            planner_row,
            analysis_row,
            product_profile_row,
            demand_row,
        )
        readiness = resolve_readiness(planner_row, analysis_row, active_row)
        ads_metrics = resolve_ads_metrics(planner_row, summary_row, fsn in issue_fsns)
        final_decision, budget, risk, opportunity, manual_override = choose_decision(
            planner_row,
            analysis_row,
            customer_row,
            courier_row,
            active_row,
            final_product_type,
            final_seasonality_tag,
            ads_metrics,
        )
        decision_reason = build_reason(
            final_decision,
            manual_override,
            readiness,
            analysis_row,
            customer_row,
            courier_row,
            active_row,
            final_product_type,
            final_seasonality_tag,
            ads_metrics,
        )
        manual_final_decision = pick_first_nonblank(planner_row.get("Manual_Final_Ads_Decision", ""), planner_row.get("Manual_Override", ""))
        manual_remarks = normalize_text(planner_row.get("Manual_Ads_Remarks", ""))
        if not manual_remarks and normalize_text(planner_row.get("Manual_Override", "")) and normalize_text(planner_row.get("Manual_Override", "")) != manual_final_decision:
            manual_remarks = normalize_text(planner_row.get("Manual_Override", ""))

        next_action_date, review_date = compute_action_dates(final_decision, planner_row, demand_row, final_product_type, final_seasonality_tag)
        ads_data_used = build_ads_data_used(readiness, final_product_type, final_seasonality_tag, ads_metrics, analysis_row, customer_row, courier_row, active_row)

        updated = dict(planner_row)
        updated["Final_Ads_Decision"] = final_decision
        updated["Final_Budget_Recommendation"] = budget
        updated["Ads_Decision_Reason"] = decision_reason
        updated["Ads_Risk_Level"] = risk
        updated["Ads_Opportunity_Level"] = opportunity
        updated["Customer_Return_Count"] = normalize_text(pick_first_nonblank(customer_row.get("Customer_Return_Count", ""), analysis_row.get("Customer_Return_Count", "")))
        updated["Customer_Return_Rate"] = normalize_text(customer_row.get("Customer_Return_Rate", ""))
        updated["Customer_Return_Risk_Level"] = normalize_text(customer_row.get("Customer_Return_Risk_Level", ""))
        updated["Courier_Return_Count"] = normalize_text(pick_first_nonblank(courier_row.get("Courier_Return_Count", ""), analysis_row.get("Courier_Return_Count", "")))
        updated["Courier_Return_Rate"] = normalize_text(courier_row.get("Courier_Return_Rate", ""))
        updated["Courier_Return_Risk_Level"] = normalize_text(courier_row.get("Courier_Return_Risk_Level", ""))
        updated["Unknown_Return_Count"] = normalize_text(pick_first_nonblank(return_type_row.get("Unknown_Return_Count", ""), analysis_row.get("Unknown_Return_Count", "")))
        updated["Total_Return_Count"] = normalize_text(pick_first_nonblank(return_type_row.get("Total_Return_Count", ""), analysis_row.get("Total_Return_Count", "")))
        updated["Total_Return_Rate"] = normalize_text(pick_first_nonblank(return_type_row.get("Total_Return_Rate", ""), analysis_row.get("Total_Return_Rate", ""), analysis_row.get("Return_Rate", "")))
        updated["Next_Ad_Action_Date"] = next_action_date
        updated["Ads_Review_Date"] = review_date
        updated["Ads_Data_Used"] = ads_data_used
        updated["Manual_Final_Ads_Decision"] = manual_final_decision
        updated["Manual_Ads_Remarks"] = manual_remarks
        updated["Final_Product_Type"] = final_product_type or planner_row.get("Final_Product_Type", "")
        updated["Final_Seasonality_Tag"] = final_seasonality_tag or planner_row.get("Final_Seasonality_Tag", "")
        updated["Ad_Run_Type"] = ad_run_type or planner_row.get("Ad_Run_Type", "")
        final_rows.append(updated)

    return final_headers, final_rows


def update_flipkart_ads_recommendations() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in REQUIRED_TABS:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    planner_headers, planner_rows = read_sheet_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)
    _, summary_rows = read_sheet_table(sheets_service, spreadsheet_id, ADS_SUMMARY_TAB)
    _, issue_rows = read_sheet_table(sheets_service, spreadsheet_id, ADS_MAPPING_ISSUES_TAB)
    _, analysis_rows = read_sheet_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    _, customer_return_rows = read_sheet_table(sheets_service, spreadsheet_id, CUSTOMER_RETURN_SUMMARY_TAB)
    _, courier_return_rows = read_sheet_table(sheets_service, spreadsheet_id, COURIER_RETURN_SUMMARY_TAB)
    return_type_rows = read_sheet_table(sheets_service, spreadsheet_id, RETURN_TYPE_PIVOT_TAB)[1] if tab_exists(sheets_service, spreadsheet_id, RETURN_TYPE_PIVOT_TAB) else []
    _, active_task_rows = read_sheet_table(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)
    _, product_profile_rows = read_sheet_table(sheets_service, spreadsheet_id, PRODUCT_AD_PROFILE_TAB)
    _, demand_profile_rows = read_sheet_table(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)

    final_headers, final_rows = build_final_rows(
        planner_headers,
        planner_rows,
        analysis_rows,
        summary_rows,
        customer_return_rows,
        courier_return_rows,
        return_type_rows,
        issue_rows,
        active_task_rows,
        product_profile_rows,
        demand_profile_rows,
    )

    write_local_csv(LOCAL_OUTPUT_PATH, final_headers, final_rows)
    write_local_csv(LOCAL_ADS_PLANNER_PATH, final_headers, final_rows)

    planner_sheet_id = ensure_tab(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)
    write_output_tab(sheets_service, spreadsheet_id, ADS_PLANNER_TAB, final_headers, final_rows, planner_sheet_id)

    final_decision_distribution = ordered_counter(
        final_rows,
        "Final_Ads_Decision",
        DECISION_PRIORITY,
    )
    budget_distribution = ordered_counter(final_rows, "Final_Budget_Recommendation", BUDGET_ORDER)
    risk_distribution = ordered_counter(final_rows, "Ads_Risk_Level", RISK_ORDER)
    opportunity_distribution = ordered_counter(final_rows, "Ads_Opportunity_Level", OPPORTUNITY_ORDER)

    scale_ads_count = sum(1 for row in final_rows if normalize_text(row.get("Final_Ads_Decision", "")) == "Scale Ads")
    test_ads_count = sum(
        1
        for row in final_rows
        if normalize_text(row.get("Final_Ads_Decision", "")) in {"Test Ads", "Always-On Test", "Seasonal/Event Test"}
    )
    fix_before_ads_count = sum(
        1
        for row in final_rows
        if normalize_text(row.get("Final_Ads_Decision", "")) in {
            "Fill COGS First",
            "Do Not Run Ads",
            "Do Not Run Ads / Improve Economics",
            "Improve Economics Before Ads",
            "Fix Product First",
            "Fix Product/Listing First",
            "Resolve Critical Alert First",
            "Fix Ads Mapping",
        }
    )
    do_not_run_count = sum(1 for row in final_rows if normalize_text(row.get("Final_Budget_Recommendation", "")) == "Do Not Run")
    manual_review_count = sum(1 for row in final_rows if normalize_text(row.get("Final_Ads_Decision", "")) in {"Manual Review", "Monitor"})

    tabs_updated = [ADS_PLANNER_TAB]
    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": "SUCCESS",
        "planner_rows": len(final_rows),
        "final_decision_distribution": json.dumps(final_decision_distribution, ensure_ascii=False),
        "budget_distribution": json.dumps(budget_distribution, ensure_ascii=False),
        "risk_distribution": json.dumps(risk_distribution, ensure_ascii=False),
        "opportunity_distribution": json.dumps(opportunity_distribution, ensure_ascii=False),
        "scale_ads_count": scale_ads_count,
        "test_ads_count": test_ads_count,
        "fix_before_ads_count": fix_before_ads_count,
        "do_not_run_count": do_not_run_count,
        "manual_review_count": manual_review_count,
        "tabs_updated": json.dumps(tabs_updated, ensure_ascii=False),
        "log_path": str(LOG_PATH),
        "message": "Built final Flipkart ads recommendations from planner, readiness, and mapped ads data",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "status",
            "planner_rows",
            "final_decision_distribution",
            "budget_distribution",
            "risk_distribution",
            "opportunity_distribution",
            "scale_ads_count",
            "test_ads_count",
            "fix_before_ads_count",
            "do_not_run_count",
            "manual_review_count",
            "tabs_updated",
            "log_path",
            "message",
        ],
        [log_row],
    )

    payload = {
        "status": "SUCCESS",
        "planner_rows": len(final_rows),
        "final_decision_distribution": final_decision_distribution,
        "budget_distribution": budget_distribution,
        "risk_distribution": risk_distribution,
        "opportunity_distribution": opportunity_distribution,
        "scale_ads_count": scale_ads_count,
        "test_ads_count": test_ads_count,
        "fix_before_ads_count": fix_before_ads_count,
        "do_not_run_count": do_not_run_count,
        "manual_review_count": manual_review_count,
        "tabs_updated": tabs_updated,
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return payload


def main() -> None:
    try:
        update_flipkart_ads_recommendations()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                    "tabs_updated": [ADS_PLANNER_TAB],
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
