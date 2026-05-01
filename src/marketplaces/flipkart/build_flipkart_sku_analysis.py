from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_utils import (
    NORMALIZED_ADS_PATH,
    NORMALIZED_LISTINGS_PATH,
    NORMALIZED_ORDERS_PATH,
    NORMALIZED_PNL_PATH,
    NORMALIZED_RETURNS_PATH,
    NORMALIZED_SALES_TAX_PATH,
    NORMALIZED_SETTLEMENTS_PATH,
    NORMALIZATION_STATE_PATH,
    SKU_ANALYSIS_LOG_PATH,
    SKU_ANALYSIS_PATH,
    SKU_ANALYSIS_STATE_PATH,
    FSN_BRIDGE_PATH,
    TARGET_FSN_PATH,
    ensure_directories,
    csv_data_row_count,
    load_run_state,
    format_decimal,
    now_iso,
    parse_float,
    path_mtime,
    clean_fsn,
    normalize_text,
    write_csv,
    append_csv_log,
    save_run_state,
    build_status_payload,
)

SKU_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Category",
    "Listing_Status",
    "Inactive_Reason",
    "MRP",
    "Selling_Price",
    "Stock",
    "Bank_Settlement",
    "Orders",
    "Units_Sold",
    "Gross_Sales",
    "Returns",
    "Return_Rate",
    "Customer_Return_Count",
    "Courier_Return_Count",
    "Unknown_Return_Count",
    "Total_Return_Count",
    "Customer_Return_Rate",
    "Courier_Return_Rate",
    "Total_Return_Rate",
    "Cancellations",
    "Net_Settlement",
    "Marketplace_Fees",
    "Commission",
    "Fixed_Fee",
    "Collection_Fee",
    "Shipping_Fees",
    "Reverse_Shipping_Fees",
    "GST_On_Fees",
    "TCS",
    "TDS",
    "Refund",
    "Protection_Fund",
    "Adjustments",
    "Flipkart_Net_Earnings",
    "Flipkart_Expenses",
    "Amount_Settled",
    "Amount_Pending",
    "Ads_Revenue",
    "Estimated_Ad_Spend",
    "Ad_Orders",
    "Views",
    "Clicks",
    "CTR",
    "ROAS",
    "ACOS",
    "Net_Profit_Before_COGS",
    "Profit_Per_Order_Before_COGS",
    "Profit_Margin_Before_COGS",
    "Pnl_Difference",
    "Missing_Data",
    "Data_Confidence",
    "Final_Action",
    "Reason",
]
LOG_HEADERS = ["timestamp", "fsn", "orders", "units_sold", "gross_sales", "returns", "net_settlement", "ads_revenue", "estimated_ad_spend", "data_confidence", "final_action", "status", "message"]


def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def load_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    return read_csv_dicts(path)


def ensure_fresh_normalized_inputs() -> Dict[str, Any]:
    state = load_run_state(NORMALIZATION_STATE_PATH)
    if normalize_text(state.get("status", "")).upper() != "SUCCESS":
        raise RuntimeError(f"Normalization state is not successful: {NORMALIZATION_STATE_PATH}")

    files_to_check = [
        TARGET_FSN_PATH,
        FSN_BRIDGE_PATH,
        NORMALIZED_LISTINGS_PATH,
        NORMALIZED_ORDERS_PATH,
        NORMALIZED_RETURNS_PATH,
        NORMALIZED_SETTLEMENTS_PATH,
        NORMALIZED_PNL_PATH,
        NORMALIZED_SALES_TAX_PATH,
        NORMALIZED_ADS_PATH,
    ]
    freshness: Dict[str, Any] = {}
    for path in files_to_check:
        if not path.exists():
            freshness[str(path)] = {"rows": 0, "mtime": 0.0}
            continue
        data_rows = csv_data_row_count(path) if path.suffix.lower() == ".csv" else 0
        freshness[str(path)] = {"rows": data_rows, "mtime": path_mtime(path)}
    return {"state": state, "freshness": freshness}


def group_by_fsn(rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn:
            grouped[fsn].append(row)
    return grouped


def load_normalization_diagnostics() -> Dict[str, Any]:
    if not NORMALIZATION_STATE_PATH.exists():
        return {}
    state = load_run_state(NORMALIZATION_STATE_PATH)
    return state.get("report_diagnostics", {}) if isinstance(state, dict) else {}


def sum_field(rows: List[Dict[str, str]], field: str) -> float:
    total = 0.0
    for row in rows:
        total += parse_float(row.get(field, ""))
    return total


def count_unique(rows: List[Dict[str, str]], keys: List[str]) -> int:
    seen = set()
    for row in rows:
        for key in keys:
            value = row.get(key, "")
            if value:
                seen.add(value)
                break
    return len(seen)


def build_lookup(rows: List[Dict[str, str]], key_field: str) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = row.get(key_field, "")
        if key:
            grouped[key].append(row)
    return grouped


def first_non_blank(*values: Any) -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return ""


def unique_count(rows: List[Dict[str, str]], preferred_keys: List[str]) -> int:
    seen = set()
    for row in rows:
        for key in preferred_keys:
            value = normalize_text(row.get(key, ""))
            if value:
                seen.add(value)
                break
    return len(seen)


def any_low_mapping(rows: List[Dict[str, str]]) -> bool:
    for row in rows:
        if normalize_text(row.get("Mapping_Confidence", "")).upper() == "LOW":
            return True
        if "fallback" in normalize_text(row.get("Mapping_Issue", "")).lower():
            return True
    return False


def choose_data_confidence(listing_rows: List[Dict[str, str]], order_rows: List[Dict[str, str]], settlement_rows: List[Dict[str, str]], pnl_rows: List[Dict[str, str]]) -> str:
    has_listing = bool(listing_rows)
    has_orders = bool(order_rows)
    has_settlement = bool(settlement_rows)
    has_pnl = bool(pnl_rows)
    if has_listing and has_orders and has_settlement and has_pnl:
        return "HIGH"
    if has_listing and has_orders:
        return "MEDIUM"
    return "LOW"


def field_has_value(rows: List[Dict[str, str]], field: str) -> bool:
    return any(normalize_text(row.get(field, "")) for row in rows)


def derive_gross_sales(order_rows: List[Dict[str, str]], settlement_rows: List[Dict[str, str]]) -> float:
    settlement_gross = sum_field(settlement_rows, "Gross_Amount")
    if field_has_value(settlement_rows, "Gross_Amount"):
        return settlement_gross
    order_sales = 0.0
    for row in order_rows:
        order_sales += parse_float(row.get("Selling_Price", "0")) * parse_float(row.get("Quantity", "0"))
    return order_sales


def derive_net_settlement(
    settlement_rows: List[Dict[str, str]],
    gross_sales: float,
    current_net_settlement: float,
) -> float:
    if field_has_value(settlement_rows, "Net_Settlement"):
        return current_net_settlement
    if not settlement_rows:
        return 0.0
    settlement_charges = (
        sum_field(settlement_rows, "Commission")
        + sum_field(settlement_rows, "Fixed_Fee")
        + sum_field(settlement_rows, "Collection_Fee")
        + sum_field(settlement_rows, "Shipping_Fee")
        + sum_field(settlement_rows, "Reverse_Shipping_Fee")
        + sum_field(settlement_rows, "GST_On_Fees")
        + sum_field(settlement_rows, "TCS")
        + sum_field(settlement_rows, "TDS")
        + sum_field(settlement_rows, "Refund")
        + sum_field(settlement_rows, "Protection_Fund")
        + sum_field(settlement_rows, "Adjustments")
    )
    return gross_sales - settlement_charges


def derive_final_action(listing_status: str, orders: int, stock: float, return_rate: float, acos: float, net_profit: float, data_confidence: str) -> Tuple[str, str]:
    listing_status_norm = listing_status.lower()
    if any(flag in listing_status_norm for flag in ["inactive", "blocked", "rejected"]):
        return "Fix Listing", "Listing status is inactive/blocked/rejected"
    if orders == 0 and stock > 0:
        return "Improve Traffic", "Stock exists but no orders"
    if return_rate > 0.20:
        return "Fix Product/Listing", "Return rate above threshold"
    if acos > 0.35:
        return "Reduce Ads", "ACOS above threshold"
    if net_profit < 0:
        return "Investigate Profit", "Net profit before COGS is negative"
    if data_confidence == "LOW":
        return "Data Check Required", "Low mapping confidence or missing key reports"
    return "Monitor / Scale Candidate", "Healthy target FSN"


def build_flipkart_sku_analysis() -> Dict[str, Any]:
    ensure_directories()
    freshness = ensure_fresh_normalized_inputs()
    target_rows = load_rows(TARGET_FSN_PATH)
    bridge_rows = load_rows(FSN_BRIDGE_PATH)
    listings = load_rows(NORMALIZED_LISTINGS_PATH)
    orders = load_rows(NORMALIZED_ORDERS_PATH)
    returns = load_rows(NORMALIZED_RETURNS_PATH)
    settlements = load_rows(NORMALIZED_SETTLEMENTS_PATH)
    pnl_rows = load_rows(NORMALIZED_PNL_PATH)
    sales_tax_rows = load_rows(NORMALIZED_SALES_TAX_PATH)
    ads_rows = load_rows(NORMALIZED_ADS_PATH)

    if not target_rows:
        raise FileNotFoundError(f"Missing required file: {TARGET_FSN_PATH}")

    target_fsns = sorted({clean_fsn(row.get("FSN", "")) for row in target_rows if clean_fsn(row.get("FSN", ""))})
    target_info = {clean_fsn(row.get("FSN", "")): row for row in target_rows if clean_fsn(row.get("FSN", ""))}
    bridge_info = {clean_fsn(row.get("FSN", "")): row for row in bridge_rows if clean_fsn(row.get("FSN", ""))}

    listings_by_fsn = group_by_fsn(listings)
    orders_by_fsn = group_by_fsn(orders)
    returns_by_fsn = group_by_fsn(returns)
    settlements_by_fsn = group_by_fsn(settlements)
    pnl_by_fsn = group_by_fsn(pnl_rows)
    sales_tax_by_fsn = group_by_fsn(sales_tax_rows)
    ads_by_fsn = group_by_fsn(ads_rows)

    rows: List[Dict[str, Any]] = []
    logs: List[Dict[str, Any]] = []

    for fsn in target_fsns:
        listing_rows = listings_by_fsn.get(fsn, [])
        order_rows = orders_by_fsn.get(fsn, [])
        return_rows = returns_by_fsn.get(fsn, [])
        settlement_rows = settlements_by_fsn.get(fsn, [])
        pnl_fsn_rows = pnl_by_fsn.get(fsn, [])
        ads_fsn_rows = ads_by_fsn.get(fsn, [])

        listing = listing_rows[0] if listing_rows else {}
        target_row = target_info.get(fsn, {})
        bridge_row = bridge_info.get(fsn, {})
        product_title = first_non_blank(target_row.get("Product_Title", ""), bridge_row.get("Product_Title", ""), listing.get("Product_Title", ""), order_rows[0].get("Product_Title", "") if order_rows else "")
        sku_id = first_non_blank(target_row.get("SKU_ID", ""), bridge_row.get("Seller_SKU", ""), bridge_row.get("SKU_ID", ""), listing.get("Seller_SKU", ""), order_rows[0].get("Seller_SKU", "") if order_rows else "")
        category = first_non_blank(listing.get("Category", ""), bridge_row.get("Category", ""))
        listing_status = first_non_blank(listing.get("Listing_Status", ""), bridge_row.get("Listing_Status", ""))
        inactive_reason = first_non_blank(listing.get("Inactive_Reason", ""), bridge_row.get("Inactive_Reason", ""))
        mrp = first_non_blank(listing.get("MRP", ""), bridge_row.get("MRP", ""))
        selling_price = first_non_blank(listing.get("Selling_Price", ""), bridge_row.get("Selling_Price", ""))
        stock = parse_float(listing.get("Stock", ""))
        bank_settlement = first_non_blank(listing.get("Bank_Settlement", ""), bridge_row.get("Bank_Settlement", ""))

        orders_count = unique_count(order_rows, ["Order_Item_ID", "Order_ID"])
        units_sold = sum(parse_float(row.get("Quantity", "0")) for row in order_rows) if order_rows else 0.0
        gross_sales = derive_gross_sales(order_rows, settlement_rows)
        customer_return_rows = [row for row in return_rows if normalize_text(row.get("Return_Bucket", "")) == "customer_return"]
        courier_return_rows = [row for row in return_rows if normalize_text(row.get("Return_Bucket", "")) == "courier_return"]
        unknown_return_rows = [row for row in return_rows if normalize_text(row.get("Return_Bucket", "")) == "unknown_return"]
        customer_returns_count = unique_count(customer_return_rows, ["Order_Item_ID", "Return_ID", "Order_ID"])
        courier_returns_count = unique_count(courier_return_rows, ["Order_Item_ID", "Return_ID", "Order_ID"])
        unknown_returns_count = unique_count(unknown_return_rows, ["Order_Item_ID", "Return_ID", "Order_ID"])
        total_returns_count = customer_returns_count + courier_returns_count + unknown_returns_count
        returns_count = customer_returns_count
        customer_return_rate = (customer_returns_count / orders_count) if orders_count else 0.0
        courier_return_rate = (courier_returns_count / orders_count) if orders_count else 0.0
        total_return_rate = (total_returns_count / orders_count) if orders_count else 0.0
        cancellations = len([row for row in order_rows if row.get("Cancellation_Status", "").lower() in {"cancelled", "canceled", "cancel", "cancelled by customer"}])
        raw_net_settlement = sum_field(settlement_rows, "Net_Settlement")
        net_settlement = derive_net_settlement(settlement_rows, gross_sales, raw_net_settlement)
        commission = sum_field(settlement_rows, "Commission")
        fixed_fee = sum_field(settlement_rows, "Fixed_Fee")
        collection_fee = sum_field(settlement_rows, "Collection_Fee")
        shipping_fee = sum_field(settlement_rows, "Shipping_Fee")
        reverse_shipping_fee = sum_field(settlement_rows, "Reverse_Shipping_Fee")
        gst_on_fees = sum_field(settlement_rows, "GST_On_Fees")
        tcs = sum_field(settlement_rows, "TCS")
        tds = sum_field(settlement_rows, "TDS")
        refund = sum_field(settlement_rows, "Refund")
        protection_fund = sum_field(settlement_rows, "Protection_Fund")
        adjustments = sum_field(settlement_rows, "Adjustments")
        marketplace_fees = commission + fixed_fee + collection_fee + shipping_fee + reverse_shipping_fee + gst_on_fees
        flipkart_net_earnings = sum_field(pnl_fsn_rows, "Flipkart_Net_Earnings")
        flipkart_expenses = sum_field(pnl_fsn_rows, "Flipkart_Expenses")
        amount_settled = sum_field(pnl_fsn_rows, "Amount_Settled")
        amount_pending = sum_field(pnl_fsn_rows, "Amount_Pending")
        ads_revenue = sum_field(ads_fsn_rows, "Total_Revenue")
        estimated_ad_spend = sum_field(ads_fsn_rows, "Estimated_Ad_Spend")
        ad_orders = sum(parse_float(row.get("Direct_Units_Sold", "0")) + parse_float(row.get("Indirect_Units_Sold", "0")) for row in ads_fsn_rows)
        views = sum(parse_float(row.get("Views", "0")) for row in ads_fsn_rows)
        clicks = sum(parse_float(row.get("Clicks", "0")) for row in ads_fsn_rows)
        ctr = (clicks / views) if views > 0 else 0.0
        roas = (ads_revenue / estimated_ad_spend) if estimated_ad_spend > 0 else 0.0
        acos = (estimated_ad_spend / ads_revenue) if ads_revenue > 0 else 0.0
        net_profit_before_cogs = net_settlement - estimated_ad_spend
        profit_per_order_before_cogs = net_profit_before_cogs / orders_count if orders_count else 0.0
        profit_margin_before_cogs = net_profit_before_cogs / gross_sales if gross_sales else 0.0
        pnl_difference = net_profit_before_cogs - flipkart_net_earnings
        mapping_confidence = choose_data_confidence(listing_rows, order_rows, settlement_rows, pnl_fsn_rows)

        missing = []
        if not listing_rows:
            missing.append("Listing Missing")
        if not order_rows:
            missing.append("Orders Missing")
        if not return_rows:
            missing.append("Returns Missing")
        if not settlement_rows:
            missing.append("Settlement Missing")
        if not ads_fsn_rows:
            missing.append("Ads Missing")
        if not pnl_fsn_rows:
            missing.append("PNL Missing")
        if not sales_tax_by_fsn.get(fsn):
            missing.append("Tax Missing")

        final_action, reason = derive_final_action(listing_status, orders_count, stock, returns_count / orders_count if orders_count else 0.0, acos, net_profit_before_cogs, mapping_confidence)

        row = {
            "FSN": fsn,
            "SKU_ID": sku_id,
            "Product_Title": product_title,
            "Category": category,
            "Listing_Status": listing_status,
            "Inactive_Reason": inactive_reason,
            "MRP": mrp,
            "Selling_Price": selling_price,
            "Stock": format_decimal(stock, 0),
            "Bank_Settlement": bank_settlement,
            "Orders": str(orders_count),
            "Units_Sold": format_decimal(units_sold, 0),
            "Gross_Sales": format_decimal(gross_sales, 2),
            "Returns": str(returns_count),
            "Return_Rate": f"{(returns_count / orders_count):.4f}" if orders_count else "0",
            "Customer_Return_Count": str(customer_returns_count),
            "Courier_Return_Count": str(courier_returns_count),
            "Unknown_Return_Count": str(unknown_returns_count),
            "Total_Return_Count": str(total_returns_count),
            "Customer_Return_Rate": f"{customer_return_rate:.4f}" if orders_count else "0",
            "Courier_Return_Rate": f"{courier_return_rate:.4f}" if orders_count else "0",
            "Total_Return_Rate": f"{total_return_rate:.4f}" if orders_count else "0",
            "Cancellations": str(cancellations),
            "Net_Settlement": format_decimal(net_settlement, 2),
            "Marketplace_Fees": format_decimal(marketplace_fees, 2),
            "Commission": format_decimal(commission, 2),
            "Fixed_Fee": format_decimal(fixed_fee, 2),
            "Collection_Fee": format_decimal(collection_fee, 2),
            "Shipping_Fees": format_decimal(shipping_fee, 2),
            "Reverse_Shipping_Fees": format_decimal(reverse_shipping_fee, 2),
            "GST_On_Fees": format_decimal(gst_on_fees, 2),
            "TCS": format_decimal(tcs, 2),
            "TDS": format_decimal(tds, 2),
            "Refund": format_decimal(refund, 2),
            "Protection_Fund": format_decimal(protection_fund, 2),
            "Adjustments": format_decimal(adjustments, 2),
            "Flipkart_Net_Earnings": format_decimal(flipkart_net_earnings, 2),
            "Flipkart_Expenses": format_decimal(flipkart_expenses, 2),
            "Amount_Settled": format_decimal(amount_settled, 2),
            "Amount_Pending": format_decimal(amount_pending, 2),
            "Ads_Revenue": format_decimal(ads_revenue, 2),
            "Estimated_Ad_Spend": format_decimal(estimated_ad_spend, 2),
            "Ad_Orders": format_decimal(ad_orders, 0),
            "Views": format_decimal(views, 0),
            "Clicks": format_decimal(clicks, 0),
            "CTR": f"{ctr:.4f}" if ctr else "0",
            "ROAS": f"{roas:.4f}" if roas else "0",
            "ACOS": f"{acos:.4f}" if acos else "0",
            "Net_Profit_Before_COGS": format_decimal(net_profit_before_cogs, 2),
            "Profit_Per_Order_Before_COGS": format_decimal(profit_per_order_before_cogs, 2),
            "Profit_Margin_Before_COGS": f"{profit_margin_before_cogs:.4f}" if gross_sales else "0",
            "Pnl_Difference": format_decimal(pnl_difference, 2) if pnl_difference != "" else "",
            "Missing_Data": ", ".join(missing),
            "Data_Confidence": mapping_confidence,
            "Final_Action": final_action,
            "Reason": reason,
        }
        rows.append(row)
        logs.append(
            {
                "timestamp": now_iso(),
                "fsn": fsn,
                "orders": orders_count,
                "units_sold": format_decimal(units_sold, 0),
                "gross_sales": format_decimal(gross_sales, 2),
                "returns": returns_count,
                "net_settlement": format_decimal(net_settlement, 2),
                "ads_revenue": format_decimal(ads_revenue, 2),
                "estimated_ad_spend": format_decimal(estimated_ad_spend, 2),
                "data_confidence": mapping_confidence,
                "final_action": final_action,
                "status": "ok",
                "message": "aggregated",
            }
        )

    write_csv(SKU_ANALYSIS_PATH, SKU_HEADERS, rows)
    append_csv_log(SKU_ANALYSIS_LOG_PATH, LOG_HEADERS, logs)

    total_target_fsns = len(target_fsns)
    summary = {
        "total_target_fsns": total_target_fsns,
        "rows_written": len(rows),
        "fsns_with_listing": sum(1 for fsn in target_fsns if listings_by_fsn.get(fsn)),
        "fsns_with_orders": sum(1 for fsn in target_fsns if orders_by_fsn.get(fsn)),
        "fsns_with_returns": sum(1 for fsn in target_fsns if returns_by_fsn.get(fsn)),
        "fsns_with_settlement": sum(1 for fsn in target_fsns if settlements_by_fsn.get(fsn)),
        "fsns_with_pnl": sum(1 for fsn in target_fsns if pnl_by_fsn.get(fsn)),
        "fsns_with_ads": sum(1 for fsn in target_fsns if ads_by_fsn.get(fsn)),
        "high_confidence_count": sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "HIGH"),
        "medium_confidence_count": sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "MEDIUM"),
        "low_confidence_count": sum(1 for row in rows if normalize_text(row.get("Data_Confidence", "")).upper() == "LOW"),
        "negative_profit_count": sum(1 for row in rows if parse_float(row.get("Net_Profit_Before_COGS", "")) < 0),
        "high_return_rate_count": sum(1 for row in rows if parse_float(row.get("Customer_Return_Rate", row.get("Return_Rate", ""))) > 0.20),
        "high_customer_return_rate_count": sum(1 for row in rows if parse_float(row.get("Customer_Return_Rate", row.get("Return_Rate", ""))) > 0.20),
        "missing_settlement_count": sum(1 for row in rows if "Settlement Missing" in normalize_text(row.get("Missing_Data", ""))),
        "missing_pnl_count": sum(1 for row in rows if "PNL Missing" in normalize_text(row.get("Missing_Data", ""))),
    }

    result = {
        "status": "SUCCESS",
        "generated_at": now_iso(),
        "rows_written": len(rows),
        "output_path": str(SKU_ANALYSIS_PATH),
        "log_path": str(SKU_ANALYSIS_LOG_PATH),
        "normalized_input_state": str(NORMALIZATION_STATE_PATH),
        "summary": summary,
    }
    save_run_state(
        SKU_ANALYSIS_STATE_PATH,
        {
            "status": "SUCCESS",
            "stage": "sku_analysis",
            "generated_at": result["generated_at"],
            "sku_analysis_mtime": SKU_ANALYSIS_PATH.stat().st_mtime,
            "normalization_state_mtime": NORMALIZATION_STATE_PATH.stat().st_mtime,
            "normalized_input_rows": freshness["freshness"],
            "summary": summary,
        },
    )
    payload = dict(result)
    payload.pop("status", None)
    print(json.dumps(build_status_payload("SUCCESS", **payload), indent=2))
    return result


def main() -> None:
    try:
        build_flipkart_sku_analysis()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "output_path": str(SKU_ANALYSIS_PATH),
                    "log_path": str(SKU_ANALYSIS_LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
