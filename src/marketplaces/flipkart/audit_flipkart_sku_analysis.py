from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_utils import (
    NORMALIZED_LISTINGS_PATH,
    NORMALIZED_ORDERS_PATH,
    NORMALIZED_PNL_PATH,
    NORMALIZED_RETURNS_PATH,
    NORMALIZED_SETTLEMENTS_PATH,
    NORMALIZED_ADS_PATH,
    OUTPUT_DIR,
    LOG_DIR,
    SKU_ANALYSIS_PATH,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    format_decimal,
    now_iso,
    parse_float,
    save_run_state,
    write_csv,
)

AUDIT_OUTPUT_PATH = OUTPUT_DIR / "flipkart_analysis_audit.csv"
AUDIT_SUMMARY_PATH = OUTPUT_DIR / "flipkart_analysis_audit_summary.json"
AUDIT_LOG_PATH = LOG_DIR / "flipkart_analysis_audit_log.csv"

AUDIT_HEADERS = [
    "FSN",
    "current_data_confidence",
    "calculated_data_confidence_expected",
    "confidence_mismatch",
    "has_listing",
    "has_orders",
    "has_returns",
    "has_settlement",
    "has_pnl",
    "has_ads",
    "low_confidence_mapping_used",
    "fallback_mapping_used",
    "numeric_parse_issue",
    "profit_mismatch",
    "return_rate_mismatch",
    "pnl_difference_mismatch",
    "orders_present_but_zero_sales",
    "settlement_present_but_zero_net_settlement",
    "pnl_present_but_zero_net_earnings",
    "Orders",
    "Units_Sold",
    "Gross_Sales",
    "Returns",
    "Return_Rate",
    "Net_Settlement",
    "Marketplace_Fees",
    "Flipkart_Net_Earnings",
    "Flipkart_Expenses",
    "Estimated_Ad_Spend",
    "Net_Profit_Before_COGS",
    "Profit_Per_Order_Before_COGS",
    "Profit_Margin_Before_COGS",
    "Pnl_Difference",
    "recalculated_return_rate",
    "recalculated_net_profit_before_cogs",
    "recalculated_profit_per_order_before_cogs",
    "recalculated_profit_margin_before_cogs",
    "recalculated_pnl_difference",
    "Missing_Data",
]

LOG_HEADERS = [
    "timestamp",
    "fsn",
    "current_data_confidence",
    "expected_data_confidence",
    "confidence_mismatch",
    "profit_mismatch",
    "return_rate_mismatch",
    "pnl_difference_mismatch",
    "numeric_parse_issue",
    "status",
    "message",
]

NUMERIC_COLUMNS = [
    "Orders",
    "Units_Sold",
    "Gross_Sales",
    "Returns",
    "Return_Rate",
    "Net_Settlement",
    "Marketplace_Fees",
    "Flipkart_Net_Earnings",
    "Flipkart_Expenses",
    "Estimated_Ad_Spend",
    "Net_Profit_Before_COGS",
    "Profit_Margin_Before_COGS",
]


def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    return read_csv_dicts(path)


def group_by_fsn(rows: Sequence[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn:
            grouped[fsn].append(row)
    return grouped


def first_non_blank(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "null", "nan"}:
            return text
    return ""


def unique_count(rows: Sequence[Dict[str, str]], preferred_keys: Sequence[str]) -> int:
    seen = set()
    for row in rows:
        for key in preferred_keys:
            value = clean_fsn(row.get(key, "")) if "FSN" in key.upper() else first_non_blank(row.get(key, ""))
            if value:
                seen.add(value)
                break
    return len(seen)


def sum_field(rows: Sequence[Dict[str, str]], field: str) -> float:
    return sum(parse_float(row.get(field, "")) for row in rows)


def strict_numeric_ok(value: Any) -> bool:
    text = first_non_blank(value)
    if not text:
        return True
    cleaned = text.replace(",", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    return bool(re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", cleaned))


def any_low_confidence_mapping(rows: Sequence[Dict[str, str]]) -> bool:
    for row in rows:
        confidence = first_non_blank(row.get("Mapping_Confidence", "")).upper()
        issue = first_non_blank(row.get("Mapping_Issue", "")).lower()
        if confidence == "LOW" or "low confidence" in issue:
            return True
    return False


def any_fallback_mapping(rows: Sequence[Dict[str, str]]) -> bool:
    for row in rows:
        issue = first_non_blank(row.get("Mapping_Issue", "")).lower()
        if "fallback" in issue:
            return True
    return False


def calculate_expected_confidence(
    has_listing: bool,
    has_orders: bool,
    has_settlement: bool,
    has_pnl: bool,
    low_confidence_mapping_used: bool,
) -> str:
    if not has_listing or not has_orders:
        return "LOW"
    if has_listing and has_orders and has_settlement and has_pnl:
        return "LOW" if low_confidence_mapping_used else "HIGH"
    return "LOW" if low_confidence_mapping_used else "MEDIUM"


def audit_flipkart_sku_analysis() -> Dict[str, Any]:
    ensure_directories()

    if not SKU_ANALYSIS_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SKU_ANALYSIS_PATH}")

    final_rows = load_rows(SKU_ANALYSIS_PATH)
    listing_rows = load_rows(NORMALIZED_LISTINGS_PATH)
    order_rows = load_rows(NORMALIZED_ORDERS_PATH)
    return_rows = load_rows(NORMALIZED_RETURNS_PATH)
    settlement_rows = load_rows(NORMALIZED_SETTLEMENTS_PATH)
    pnl_rows = load_rows(NORMALIZED_PNL_PATH)
    ads_rows = load_rows(NORMALIZED_ADS_PATH)

    listings_by_fsn = group_by_fsn(listing_rows)
    orders_by_fsn = group_by_fsn(order_rows)
    returns_by_fsn = group_by_fsn(return_rows)
    settlements_by_fsn = group_by_fsn(settlement_rows)
    pnl_by_fsn = group_by_fsn(pnl_rows)
    ads_by_fsn = group_by_fsn(ads_rows)

    audit_rows: List[Dict[str, Any]] = []
    log_rows: List[Dict[str, Any]] = []

    summary_buckets = Counter()
    negative_profit_rows: List[Tuple[str, float, float, float]] = []
    high_return_rows: List[Tuple[str, float, int, int]] = []
    missing_settlement_rows: List[Tuple[str, float, float]] = []

    for row in final_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue

        listing_matches = listings_by_fsn.get(fsn, [])
        order_matches = orders_by_fsn.get(fsn, [])
        return_matches = returns_by_fsn.get(fsn, [])
        settlement_matches = settlements_by_fsn.get(fsn, [])
        pnl_matches = pnl_by_fsn.get(fsn, [])
        ads_matches = ads_by_fsn.get(fsn, [])

        has_listing = bool(listing_matches)
        has_orders = bool(order_matches)
        has_returns = bool(return_matches)
        has_settlement = bool(settlement_matches)
        has_pnl = bool(pnl_matches)
        has_ads = bool(ads_matches)

        low_confidence_mapping_used = any_low_confidence_mapping(
            listing_matches + order_matches + settlement_matches + pnl_matches
        )
        fallback_mapping_used = any_fallback_mapping(listing_matches + order_matches + settlement_matches + pnl_matches)

        current_data_confidence = first_non_blank(row.get("Data_Confidence", "")).upper()
        calculated_data_confidence_expected = calculate_expected_confidence(
            has_listing=has_listing,
            has_orders=has_orders,
            has_settlement=has_settlement,
            has_pnl=has_pnl,
            low_confidence_mapping_used=low_confidence_mapping_used,
        )
        confidence_mismatch = current_data_confidence != calculated_data_confidence_expected

        current_orders = parse_float(row.get("Orders", ""))
        current_units_sold = parse_float(row.get("Units_Sold", ""))
        current_gross_sales = parse_float(row.get("Gross_Sales", ""))
        current_returns = parse_float(row.get("Returns", ""))
        current_return_rate = parse_float(row.get("Return_Rate", ""))
        current_net_settlement = parse_float(row.get("Net_Settlement", ""))
        current_marketplace_fees = parse_float(row.get("Marketplace_Fees", ""))
        current_flipkart_net_earnings = parse_float(row.get("Flipkart_Net_Earnings", ""))
        current_flipkart_expenses = parse_float(row.get("Flipkart_Expenses", ""))
        current_estimated_ad_spend = parse_float(row.get("Estimated_Ad_Spend", ""))
        current_net_profit = parse_float(row.get("Net_Profit_Before_COGS", ""))
        current_profit_per_order = parse_float(row.get("Profit_Per_Order_Before_COGS", ""))
        current_profit_margin = parse_float(row.get("Profit_Margin_Before_COGS", ""))
        current_pnl_difference = parse_float(row.get("Pnl_Difference", ""))

        recalculated_return_rate = (current_returns / current_orders) if current_orders else 0.0
        recalculated_net_profit = current_net_settlement - current_estimated_ad_spend
        recalculated_profit_per_order = (recalculated_net_profit / current_orders) if current_orders else 0.0
        recalculated_profit_margin = (recalculated_net_profit / current_gross_sales) if current_gross_sales else 0.0
        recalculated_pnl_difference = recalculated_net_profit - current_flipkart_net_earnings

        profit_mismatch = abs(current_net_profit - recalculated_net_profit) > 0.01 or abs(current_profit_per_order - recalculated_profit_per_order) > 0.01 or abs(current_profit_margin - recalculated_profit_margin) > 0.0001
        return_rate_mismatch = abs(current_return_rate - recalculated_return_rate) > 0.0001
        pnl_difference_mismatch = abs(current_pnl_difference - recalculated_pnl_difference) > 0.01

        numeric_values = {field: row.get(field, "") for field in NUMERIC_COLUMNS}
        numeric_parse_issue = any(not strict_numeric_ok(value) for value in numeric_values.values())

        orders_present_but_zero_sales = has_orders and current_orders > 0 and abs(current_gross_sales) <= 0.0001
        settlement_present_but_zero_net_settlement = has_settlement and abs(current_net_settlement) <= 0.0001
        pnl_present_but_zero_net_earnings = has_pnl and abs(current_flipkart_net_earnings) <= 0.0001

        if recalculated_net_profit < 0:
            negative_profit_rows.append((fsn, recalculated_net_profit, current_net_settlement, current_estimated_ad_spend))
        if recalculated_return_rate > 0.20:
            high_return_rows.append((fsn, recalculated_return_rate, int(current_returns), int(current_orders)))
        if settlement_present_but_zero_net_settlement:
            missing_settlement_rows.append((fsn, current_net_settlement, current_flipkart_net_earnings))

        audit_row = {
            "FSN": fsn,
            "current_data_confidence": current_data_confidence,
            "calculated_data_confidence_expected": calculated_data_confidence_expected,
            "confidence_mismatch": str(confidence_mismatch),
            "has_listing": str(has_listing),
            "has_orders": str(has_orders),
            "has_returns": str(has_returns),
            "has_settlement": str(has_settlement),
            "has_pnl": str(has_pnl),
            "has_ads": str(has_ads),
            "low_confidence_mapping_used": str(low_confidence_mapping_used),
            "fallback_mapping_used": str(fallback_mapping_used),
            "numeric_parse_issue": str(numeric_parse_issue),
            "profit_mismatch": str(profit_mismatch),
            "return_rate_mismatch": str(return_rate_mismatch),
            "pnl_difference_mismatch": str(pnl_difference_mismatch),
            "orders_present_but_zero_sales": str(orders_present_but_zero_sales),
            "settlement_present_but_zero_net_settlement": str(settlement_present_but_zero_net_settlement),
            "pnl_present_but_zero_net_earnings": str(pnl_present_but_zero_net_earnings),
            "Orders": row.get("Orders", ""),
            "Units_Sold": row.get("Units_Sold", ""),
            "Gross_Sales": row.get("Gross_Sales", ""),
            "Returns": row.get("Returns", ""),
            "Return_Rate": row.get("Return_Rate", ""),
            "Net_Settlement": row.get("Net_Settlement", ""),
            "Marketplace_Fees": row.get("Marketplace_Fees", ""),
            "Flipkart_Net_Earnings": row.get("Flipkart_Net_Earnings", ""),
            "Flipkart_Expenses": row.get("Flipkart_Expenses", ""),
            "Estimated_Ad_Spend": row.get("Estimated_Ad_Spend", ""),
            "Net_Profit_Before_COGS": row.get("Net_Profit_Before_COGS", ""),
            "Profit_Per_Order_Before_COGS": row.get("Profit_Per_Order_Before_COGS", ""),
            "Profit_Margin_Before_COGS": row.get("Profit_Margin_Before_COGS", ""),
            "Pnl_Difference": row.get("Pnl_Difference", ""),
            "recalculated_return_rate": format_decimal(recalculated_return_rate, 4),
            "recalculated_net_profit_before_cogs": format_decimal(recalculated_net_profit, 2),
            "recalculated_profit_per_order_before_cogs": format_decimal(recalculated_profit_per_order, 2),
            "recalculated_profit_margin_before_cogs": format_decimal(recalculated_profit_margin, 4),
            "recalculated_pnl_difference": format_decimal(recalculated_pnl_difference, 2),
            "Missing_Data": row.get("Missing_Data", ""),
        }
        audit_rows.append(audit_row)

        summary_buckets["total_rows"] += 1
        if calculated_data_confidence_expected == "HIGH":
            summary_buckets["expected_high_confidence_count"] += 1
        elif calculated_data_confidence_expected == "MEDIUM":
            summary_buckets["expected_medium_confidence_count"] += 1
        else:
            summary_buckets["expected_low_confidence_count"] += 1

        if current_data_confidence == "HIGH":
            summary_buckets["current_high_confidence_count"] += 1
        elif current_data_confidence == "MEDIUM":
            summary_buckets["current_medium_confidence_count"] += 1
        else:
            summary_buckets["current_low_confidence_count"] += 1

        if confidence_mismatch:
            summary_buckets["confidence_mismatch_count"] += 1
        if recalculated_net_profit < 0:
            summary_buckets["negative_profit_count_recalculated"] += 1
        if profit_mismatch:
            summary_buckets["profit_mismatch_count"] += 1
        if return_rate_mismatch:
            summary_buckets["return_rate_mismatch_count"] += 1
        if pnl_difference_mismatch:
            summary_buckets["pnl_difference_mismatch_count"] += 1
        if numeric_parse_issue:
            summary_buckets["numeric_parse_issue_count"] += 1
        if orders_present_but_zero_sales:
            summary_buckets["orders_present_but_zero_sales_count"] += 1
        if settlement_present_but_zero_net_settlement:
            summary_buckets["settlement_present_but_zero_net_settlement_count"] += 1
        if pnl_present_but_zero_net_earnings:
            summary_buckets["pnl_present_but_zero_net_earnings_count"] += 1

        log_rows.append(
            {
                "timestamp": now_iso(),
                "fsn": fsn,
                "current_data_confidence": current_data_confidence,
                "expected_data_confidence": calculated_data_confidence_expected,
                "confidence_mismatch": str(confidence_mismatch),
                "profit_mismatch": str(profit_mismatch),
                "return_rate_mismatch": str(return_rate_mismatch),
                "pnl_difference_mismatch": str(pnl_difference_mismatch),
                "numeric_parse_issue": str(numeric_parse_issue),
                "status": "ok",
                "message": "audited",
            }
        )

    write_csv(AUDIT_OUTPUT_PATH, AUDIT_HEADERS, audit_rows)
    append_csv_log(AUDIT_LOG_PATH, LOG_HEADERS, log_rows)

    current_rows_by_confidence = Counter(first_non_blank(row.get("Data_Confidence", "")).upper() for row in final_rows)
    current_rows_by_confidence.pop("", None)

    top_10_negative_profit_fsns = [
        {
            "fsn": fsn,
            "net_profit_before_cogs": format_decimal(net_profit, 2),
            "net_settlement": format_decimal(net_settlement, 2),
            "estimated_ad_spend": format_decimal(ad_spend, 2),
        }
        for fsn, net_profit, net_settlement, ad_spend in sorted(negative_profit_rows, key=lambda item: item[1])[:10]
    ]
    top_10_high_return_rate_fsns = [
        {
            "fsn": fsn,
            "return_rate": format_decimal(return_rate, 4),
            "returns": returns,
            "orders": orders,
        }
        for fsn, return_rate, returns, orders in sorted(high_return_rows, key=lambda item: item[1], reverse=True)[:10]
    ]
    top_10_missing_settlement_fsns = [
        {
            "fsn": fsn,
            "net_settlement": format_decimal(net_settlement, 2),
            "flipkart_net_earnings": format_decimal(net_earnings, 2),
        }
        for fsn, net_settlement, net_earnings in sorted(missing_settlement_rows, key=lambda item: (abs(item[1]), item[0]))[:10]
    ]

    recommendation = "Fix build_flipkart_sku_analysis.py confidence logic before pushing to Google Sheets."
    if summary_buckets["profit_mismatch_count"] or summary_buckets["numeric_parse_issue_count"]:
        recommendation = "Fix profit/parse logic in build_flipkart_sku_analysis.py before pushing to Google Sheets."
    elif summary_buckets["confidence_mismatch_count"] == 0:
        recommendation = "Confidence logic looks consistent; profit and numeric checks should still be reviewed before push."

    summary: Dict[str, Any] = {
        "total_rows": summary_buckets["total_rows"],
        "expected_high_confidence_count": summary_buckets["expected_high_confidence_count"],
        "expected_medium_confidence_count": summary_buckets["expected_medium_confidence_count"],
        "expected_low_confidence_count": summary_buckets["expected_low_confidence_count"],
        "current_high_confidence_count": summary_buckets["current_high_confidence_count"],
        "current_medium_confidence_count": summary_buckets["current_medium_confidence_count"],
        "current_low_confidence_count": summary_buckets["current_low_confidence_count"],
        "confidence_mismatch_count": summary_buckets["confidence_mismatch_count"],
        "negative_profit_count_recalculated": summary_buckets["negative_profit_count_recalculated"],
        "profit_mismatch_count": summary_buckets["profit_mismatch_count"],
        "return_rate_mismatch_count": summary_buckets["return_rate_mismatch_count"],
        "pnl_difference_mismatch_count": summary_buckets["pnl_difference_mismatch_count"],
        "numeric_parse_issue_count": summary_buckets["numeric_parse_issue_count"],
        "orders_present_but_zero_sales_count": summary_buckets["orders_present_but_zero_sales_count"],
        "settlement_present_but_zero_net_settlement_count": summary_buckets["settlement_present_but_zero_net_settlement_count"],
        "pnl_present_but_zero_net_earnings_count": summary_buckets["pnl_present_but_zero_net_earnings_count"],
        "current_data_confidence_distribution": dict(current_rows_by_confidence),
        "top_10_negative_profit_fsns": top_10_negative_profit_fsns,
        "top_10_high_return_rate_fsns": top_10_high_return_rate_fsns,
        "top_10_missing_settlement_fsns": top_10_missing_settlement_fsns,
        "recommendation": recommendation,
    }

    AUDIT_SUMMARY_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    save_run_state(
        OUTPUT_DIR / "flipkart_analysis_audit_state.json",
        {
            "status": "SUCCESS",
            "stage": "flipkart_analysis_audit",
            "generated_at": now_iso(),
            "summary_path": str(AUDIT_SUMMARY_PATH),
            "audit_path": str(AUDIT_OUTPUT_PATH),
            "log_path": str(AUDIT_LOG_PATH),
            "summary": summary,
        },
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


def main() -> None:
    try:
        audit_flipkart_sku_analysis()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "analysis_path": str(SKU_ANALYSIS_PATH),
                    "audit_path": str(AUDIT_OUTPUT_PATH),
                    "summary_path": str(AUDIT_SUMMARY_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
