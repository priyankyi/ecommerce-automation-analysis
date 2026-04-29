from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.integrations.google_ads.google_ads_config import load_google_ads_config
from src.marketplaces.flipkart.create_flipkart_ads_planner_foundation import DEFAULT_DEMAND_PROFILES, compute_ad_dates, parse_peak_months
from src.marketplaces.flipkart.flipkart_sheet_helpers import clear_tab, ensure_tab, freeze_and_format, read_table, tab_exists, write_rows
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, ensure_directories, normalize_text, now_iso, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "product_type_demand_profile_log.csv"

KEYWORD_CACHE_TAB = "GOOGLE_KEYWORD_METRICS_CACHE"
DEMAND_PROFILE_TAB = "PRODUCT_TYPE_DEMAND_PROFILE"

LOCAL_DEMAND_PROFILE_PATH = OUTPUT_DIR / "product_type_demand_profile.csv"

DEMAND_HEADERS = [
    "Product_Type",
    "Seasonality_Tag",
    "Peak_Months",
    "Prep_Start_Days_Before_Peak",
    "Ads_Start_Days_Before_Peak",
    "Total_Avg_Monthly_Searches",
    "Demand_Stability",
    "Seasonality_Score",
    "Current_Month_Demand_Index",
    "Next_45_Days_Demand_Status",
    "Demand_Confidence",
    "Demand_Source",
    "Recommended_Ad_Window",
    "Remarks",
    "Last_Updated",
]

MONTH_ORDER = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_TO_INDEX = {month: index for index, month in enumerate(MONTH_ORDER, start=1)}


def _headers_union(existing_headers: Sequence[str]) -> List[str]:
    headers = list(existing_headers)
    for header in DEMAND_HEADERS:
        if header not in headers:
            headers.append(header)
    return headers


def _load_cache_rows(sheets_service, spreadsheet_id: str) -> List[Dict[str, str]]:
    if not tab_exists(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB):
        return []
    _, rows = read_table(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)
    return rows


def _default_demand_profile(product_type: str) -> Dict[str, Any]:
    default = DEFAULT_DEMAND_PROFILES.get(product_type, {})
    return {
        "Product_Type": product_type,
        "Seasonality_Tag": default.get("Seasonality_Tag", "Unknown"),
        "Peak_Months": default.get("Peak_Months", ""),
        "Prep_Start_Days_Before_Peak": str(default.get("Prep_Start_Days_Before_Peak", "")),
        "Ads_Start_Days_Before_Peak": str(default.get("Ads_Start_Days_Before_Peak", "")),
        "Total_Avg_Monthly_Searches": "",
        "Demand_Stability": "Unknown",
        "Seasonality_Score": "",
        "Current_Month_Demand_Index": "",
        "Next_45_Days_Demand_Status": "Unknown",
        "Demand_Confidence": default.get("Demand_Confidence", "Medium"),
        "Demand_Source": "Manual Default / Cache Empty",
        "Recommended_Ad_Window": default.get("Recommended_Ad_Window", ""),
        "Remarks": default.get("Remarks", ""),
        "Last_Updated": now_iso(),
    }


def _monthly_volume_sum(rows: Sequence[Dict[str, str]], month_name: str) -> float:
    return sum(parse_float(row.get(f"Monthly_Search_{month_name}", "")) for row in rows)


def _monthly_totals(rows: Sequence[Dict[str, str]]) -> Dict[str, float]:
    return {month: _monthly_volume_sum(rows, month) for month in MONTH_ORDER}


def _peak_months(monthly_totals: Dict[str, float], top_n: int = 3) -> List[str]:
    ranked = sorted(MONTH_ORDER, key=lambda month: (monthly_totals.get(month, 0.0), -MONTH_TO_INDEX[month]), reverse=True)
    ranked = [month for month in ranked if monthly_totals.get(month, 0.0) > 0]
    return ranked[:top_n]


def _lowest_months(monthly_totals: Dict[str, float], bottom_n: int = 3) -> List[str]:
    ranked = sorted(MONTH_ORDER, key=lambda month: (monthly_totals.get(month, 0.0), MONTH_TO_INDEX[month]))
    ranked = [month for month in ranked if monthly_totals.get(month, 0.0) > 0]
    return ranked[:bottom_n]


def _seasonality_score(monthly_totals: Dict[str, float]) -> float:
    values = [monthly_totals.get(month, 0.0) for month in MONTH_ORDER]
    total = sum(values)
    if total <= 0:
        return 0.0
    top_three = sorted(values, reverse=True)[:3]
    return round((sum(top_three) / total) * 100.0, 2)


def _demand_stability(monthly_totals: Dict[str, float]) -> str:
    values = [monthly_totals.get(month, 0.0) for month in MONTH_ORDER if monthly_totals.get(month, 0.0) > 0]
    if len(values) < 2:
        return "Unknown"
    mean_value = statistics.mean(values)
    if mean_value <= 0:
        return "Unknown"
    stdev = statistics.pstdev(values)
    coefficient = stdev / mean_value if mean_value else 0.0
    if coefficient <= 0.30:
        return "High"
    if coefficient <= 0.60:
        return "Medium"
    return "Low"


def _current_month_index(monthly_totals: Dict[str, float]) -> float:
    month_name = date.today().strftime("%b")
    current_value = monthly_totals.get(month_name, 0.0)
    non_zero_values = [value for value in monthly_totals.values() if value > 0]
    if not non_zero_values:
        return 0.0
    average_value = statistics.mean(non_zero_values)
    if average_value <= 0:
        return 0.0
    return round((current_value / average_value) * 100.0, 2)


def _next_45_days_status(monthly_totals: Dict[str, float]) -> str:
    today = date.today()
    current_index = today.month - 1
    next_month = ((current_index + 1) % 12) + 1
    month_after = ((current_index + 2) % 12) + 1
    selected_months = [MONTH_ORDER[current_index], MONTH_ORDER[next_month - 1], MONTH_ORDER[month_after - 1]]
    selected_total = sum(monthly_totals.get(month, 0.0) for month in selected_months)
    yearly_total = sum(monthly_totals.values())
    if yearly_total <= 0:
        return "Unknown"
    share = selected_total / yearly_total
    if share >= 0.30:
        return "High"
    if share >= 0.15:
        return "Medium"
    return "Low"


def _confidence_from_rows(rows: Sequence[Dict[str, str]], keyword_count: int) -> str:
    success_rows = [row for row in rows if normalize_text(row.get("Cache_Status", "")).upper() == "SUCCESS"]
    if not success_rows:
        return "Low"
    if keyword_count >= 4:
        return "High"
    if keyword_count >= 2:
        return "Medium"
    return "Low"


def _build_profile_row(product_type: str, cache_rows: Sequence[Dict[str, str]], existing_row: Dict[str, str]) -> Dict[str, Any]:
    matching_rows = [row for row in cache_rows if normalize_text(row.get("Product_Type", "")) == product_type and normalize_text(row.get("Cache_Status", "")).upper() == "SUCCESS"]
    if not matching_rows:
        default_row = _default_demand_profile(product_type)
        merged = dict(default_row)
        if existing_row:
            for key, value in existing_row.items():
                if normalize_text(value):
                    merged[key] = value
        merged["Demand_Source"] = "Manual Default / Cache Empty"
        merged["Last_Updated"] = now_iso()
        return merged

    monthly_totals = _monthly_totals(matching_rows)
    peak_months = _peak_months(monthly_totals)
    lowest_months = _lowest_months(monthly_totals)
    total_avg_monthly_searches = sum(parse_float(row.get("Avg_Monthly_Searches", "")) for row in matching_rows)
    seasonality_score = _seasonality_score(monthly_totals)
    demand_stability = _demand_stability(monthly_totals)
    current_month_index = _current_month_index(monthly_totals)
    next_45_days_status = _next_45_days_status(monthly_totals)

    default_row = _default_demand_profile(product_type)
    base_row = {
        **default_row,
        "Peak_Months": ", ".join(peak_months) if peak_months else default_row.get("Peak_Months", ""),
        "Total_Avg_Monthly_Searches": str(int(round(total_avg_monthly_searches))) if total_avg_monthly_searches else "",
        "Demand_Stability": demand_stability,
        "Seasonality_Score": f"{seasonality_score:.2f}" if seasonality_score else "",
        "Current_Month_Demand_Index": f"{current_month_index:.2f}" if current_month_index else "",
        "Next_45_Days_Demand_Status": next_45_days_status,
        "Demand_Confidence": _confidence_from_rows(matching_rows, len(matching_rows)),
        "Demand_Source": "Google Keyword Planner Cache",
        "Recommended_Ad_Window": default_row.get("Recommended_Ad_Window", ""),
        "Remarks": "; ".join(
            part
            for part in [
                f"Peak months: {', '.join(peak_months)}" if peak_months else "",
                f"Lowest months: {', '.join(lowest_months)}" if lowest_months else "",
            ]
            if part
        ),
        "Last_Updated": now_iso(),
    }

    if existing_row:
        for key, value in existing_row.items():
            if key not in base_row or not normalize_text(base_row.get(key, "")):
                if normalize_text(value):
                    base_row[key] = value
        for key in ["Remarks"]:
            if normalize_text(existing_row.get(key, "")) and not normalize_text(base_row.get(key, "")):
                base_row[key] = existing_row.get(key, "")

    return base_row


def _write_outputs(sheets_service, spreadsheet_id: str, rows: Sequence[Dict[str, Any]], headers: Sequence[str]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)
    clear_tab(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)
    write_rows(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def update_product_type_demand_profile() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = json.loads(SPREADSHEET_META_PATH.read_text(encoding="utf-8"))["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    if not tab_exists(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB):
        ensure_tab(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)

    cache_rows = _load_cache_rows(sheets_service, spreadsheet_id)
    _, existing_rows = read_table(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)
    existing_by_type = {normalize_text(row.get("Product_Type", "")): dict(row) for row in existing_rows if normalize_text(row.get("Product_Type", ""))}

    product_types = set(DEFAULT_DEMAND_PROFILES.keys())
    product_types.update(normalize_text(row.get("Product_Type", "")) for row in cache_rows if normalize_text(row.get("Product_Type", "")))
    product_types.update(existing_by_type.keys())

    final_rows: List[Dict[str, Any]] = []
    cache_product_types = {
        normalize_text(row.get("Product_Type", ""))
        for row in cache_rows
        if normalize_text(row.get("Product_Type", "")) and normalize_text(row.get("Cache_Status", "")).upper() == "SUCCESS"
    }

    for product_type in sorted(product_types):
        existing_row = existing_by_type.get(product_type, {})
        final_rows.append(_build_profile_row(product_type, cache_rows, existing_row))

    existing_headers: List[str] = []
    for row in existing_rows:
        for header in row.keys():
            if header not in existing_headers:
                existing_headers.append(header)
    headers = _headers_union(existing_headers or DEMAND_HEADERS)

    with LOCAL_DEMAND_PROFILE_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in final_rows:
            writer.writerow({header: row.get(header, "") for header in headers})

    _write_outputs(sheets_service, spreadsheet_id, final_rows, headers)

    cache_empty = not any(normalize_text(row.get("Cache_Status", "")).upper() == "SUCCESS" for row in cache_rows)
    status = "SUCCESS_WITH_WARNINGS" if cache_empty else "SUCCESS"
    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": status,
        "product_types_processed": len(final_rows),
        "product_types_with_keyword_data": len(cache_product_types),
        "cache_empty": cache_empty,
        "tabs_updated": json.dumps([DEMAND_PROFILE_TAB], ensure_ascii=False),
        "log_path": str(LOG_PATH),
        "message": "Updated product type demand profile from Google Keyword Metrics cache",
    }
    append_csv_log(LOG_PATH, list(log_row.keys()), [log_row])

    return {
        "status": status,
        "product_types_processed": len(final_rows),
        "product_types_with_keyword_data": len(cache_product_types),
        "cache_empty": cache_empty,
        "tabs_updated": [DEMAND_PROFILE_TAB],
        "log_path": str(LOG_PATH),
        "warnings": ["CACHE_EMPTY"] if cache_empty else [],
    }


def main() -> None:
    try:
        print(json.dumps(update_product_type_demand_profile(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "product_types_processed": 0,
                    "product_types_with_keyword_data": 0,
                    "cache_empty": True,
                    "tabs_updated": [DEMAND_PROFILE_TAB],
                    "log_path": str(LOG_PATH),
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
