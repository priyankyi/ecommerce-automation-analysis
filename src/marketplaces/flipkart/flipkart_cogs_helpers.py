from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from src.marketplaces.flipkart.flipkart_utils import format_decimal, normalize_text, parse_float

COGS_COLUMNS = [
    "Cost_Price",
    "Packaging_Cost",
    "Other_Cost",
    "Total_Unit_COGS",
    "Total_COGS",
    "Final_Net_Profit",
    "Final_Profit_Per_Order",
    "Final_Profit_Margin",
    "COGS_Status",
]

COGS_AVAILABLE_STATUSES = {"ENTERED", "VERIFIED"}


def build_cost_index(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    from src.marketplaces.flipkart.flipkart_utils import clean_fsn

    indexed: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in indexed:
            indexed[fsn] = dict(row)
    return indexed


def format_money(value: Any) -> str:
    return format_decimal(value, 2)


def format_margin(value: Any) -> str:
    return format_decimal(value, 4)


def _is_cost_row_populated(cost_row: Dict[str, Any]) -> bool:
    return any(
        normalize_text(cost_row.get(field, ""))
        for field in ("Cost_Price", "Packaging_Cost", "Other_Cost", "Total_Unit_COGS")
    )


def derive_cogs_row(analysis_row: Dict[str, Any], cost_row: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(analysis_row)

    for field in COGS_COLUMNS:
        if normalize_text(merged.get(field, "")):
            continue
        if normalize_text(cost_row.get(field, "")):
            merged[field] = cost_row.get(field, "")

    cost_price = normalize_text(merged.get("Cost_Price", ""))
    packaging_cost = normalize_text(merged.get("Packaging_Cost", ""))
    other_cost = normalize_text(merged.get("Other_Cost", ""))
    total_unit_cogs = normalize_text(merged.get("Total_Unit_COGS", ""))
    cogs_status = normalize_text(merged.get("COGS_Status", ""))

    if not cogs_status:
        cogs_status = normalize_text(cost_row.get("COGS_Status", ""))
    if not cogs_status:
        cogs_status = "Entered" if _is_cost_row_populated(cost_row) else "Missing"

    if not total_unit_cogs and any((cost_price, packaging_cost, other_cost)):
        total_unit_cogs = format_money(parse_float(cost_price) + parse_float(packaging_cost) + parse_float(other_cost))

    if total_unit_cogs:
        units_sold = parse_float(analysis_row.get("Units_Sold", ""))
        orders = parse_float(analysis_row.get("Orders", ""))
        gross_sales = parse_float(analysis_row.get("Gross_Sales", ""))
        net_profit_before_cogs = parse_float(analysis_row.get("Net_Profit_Before_COGS", ""))
        total_cogs = units_sold * parse_float(total_unit_cogs)
        final_net_profit = net_profit_before_cogs - total_cogs
        final_profit_per_order = final_net_profit / orders if orders else ""
        final_profit_margin = final_net_profit / gross_sales if gross_sales else ""

        merged.update(
            {
                "Total_Unit_COGS": format_money(total_unit_cogs),
                "Total_COGS": format_money(total_cogs),
                "Final_Net_Profit": format_money(final_net_profit),
                "Final_Profit_Per_Order": format_money(final_profit_per_order) if final_profit_per_order != "" else "",
                "Final_Profit_Margin": format_margin(final_profit_margin) if final_profit_margin != "" else "",
            }
        )
    else:
        for field in ("Total_Unit_COGS", "Total_COGS", "Final_Net_Profit", "Final_Profit_Per_Order", "Final_Profit_Margin"):
            merged[field] = normalize_text(merged.get(field, ""))

    merged["Cost_Price"] = cost_price
    merged["Packaging_Cost"] = packaging_cost
    merged["Other_Cost"] = other_cost
    merged["COGS_Status"] = cogs_status or "Missing"
    return merged


def hydrate_analysis_rows(
    analysis_rows: Sequence[Dict[str, Any]],
    cost_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cost_index = build_cost_index(cost_rows)
    hydrated_rows: List[Dict[str, Any]] = []
    seen_fsns: set[str] = set()

    from src.marketplaces.flipkart.flipkart_utils import clean_fsn

    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in seen_fsns:
            continue
        seen_fsns.add(fsn)
        hydrated_rows.append(derive_cogs_row(row, cost_index.get(fsn, {})))
    return hydrated_rows


def is_cogs_available(row: Dict[str, Any]) -> bool:
    cogs_status = normalize_text(row.get("COGS_Status", "")).upper()
    has_cost_inputs = any(
        normalize_text(row.get(field, ""))
        for field in ("Cost_Price", "Packaging_Cost", "Other_Cost", "Total_Unit_COGS")
    )
    final_net_profit = normalize_text(row.get("Final_Net_Profit", ""))
    total_unit_cogs = normalize_text(row.get("Total_Unit_COGS", ""))
    if cogs_status not in COGS_AVAILABLE_STATUSES:
        return False
    if has_cost_inputs:
        return True
    return final_net_profit != "" or total_unit_cogs != ""


def count_cogs_rows(rows: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    available = sum(1 for row in rows if normalize_text(row.get("FSN", "")) and is_cogs_available(row))
    missing = sum(1 for row in rows if normalize_text(row.get("FSN", "")) and not is_cogs_available(row))
    return available, missing
