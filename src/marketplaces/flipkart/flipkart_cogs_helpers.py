from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence, Tuple

from src.marketplaces.flipkart.flipkart_utils import clean_fsn, format_decimal, normalize_key, normalize_text, parse_float, to_number

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

COGS_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "FSN": ("FSN",),
    "SKU_ID": ("SKU_ID", "SKU", "Seller_SKU", "Seller SKU"),
    "Cost_Price": ("Cost_Price", "Cost Price", "Cost_P", "Cost", "Product_Cost", "Product Cost"),
    "Packaging_Cost": ("Packaging_Cost", "Packaging Cost", "Packaging_C"),
    "Other_Cost": ("Other_Cost", "Other Cost", "Other_C"),
    "Total_Unit_COGS": ("Total_Unit_COGS", "Total Unit Cost", "Total_Unit_Cost", "Total_Unit_C", "COGS", "Total COGS", "Total Unit COGS"),
    "COGS_Status": ("COGS_Status", "COGS Status", "Status", "COGS"),
}


def normalize_match_text(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return re.sub(r"\s+", "", text).casefold()


def _row_lookup(row: Dict[str, Any]) -> Dict[str, Any]:
    return {normalize_key(key): value for key, value in row.items()}


def first_alias_value(row: Dict[str, Any], field_name: str) -> str:
    lookup = _row_lookup(row)
    for alias in COGS_FIELD_ALIASES.get(field_name, (field_name,)):
        value = lookup.get(normalize_key(alias), "")
        if normalize_text(value):
            return normalize_text(value)
    return ""


def first_alias_numeric(row: Dict[str, Any], field_name: str) -> float | None:
    text = first_alias_value(row, field_name)
    if not text:
        return None
    value = to_number(text)
    if value is None or value <= 0:
        return None
    return value


def build_cost_indexes(rows: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    fsn_index: Dict[str, Dict[str, Any]] = {}
    sku_index: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        row_copy = dict(row)
        fsn = clean_fsn(first_alias_value(row_copy, "FSN"))
        sku = normalize_match_text(first_alias_value(row_copy, "SKU_ID"))
        if fsn and fsn not in fsn_index:
            fsn_index[fsn] = row_copy
        if sku and sku not in sku_index:
            sku_index[sku] = row_copy
    return fsn_index, sku_index


def build_cost_index(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    fsn_index, _ = build_cost_indexes(rows)
    return fsn_index


def match_cost_row(
    analysis_row: Dict[str, Any],
    fsn_index: Dict[str, Dict[str, Any]],
    sku_index: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Any], str]:
    fsn = clean_fsn(first_alias_value(analysis_row, "FSN"))
    if fsn and fsn in fsn_index:
        return fsn_index[fsn], "FSN"
    sku = normalize_match_text(first_alias_value(analysis_row, "SKU_ID"))
    if sku and sku in sku_index:
        return sku_index[sku], "SKU_ID"
    return {}, ""


def format_money(value: Any) -> str:
    return format_decimal(value, 2)


def format_margin(value: Any) -> str:
    return format_decimal(value, 4)


def get_usable_cogs(row: Dict[str, Any]) -> Dict[str, Any]:
    cost_price_text = first_alias_value(row, "Cost_Price")
    packaging_cost_text = first_alias_value(row, "Packaging_Cost")
    other_cost_text = first_alias_value(row, "Other_Cost")
    total_unit_cogs_text = first_alias_value(row, "Total_Unit_COGS")
    cogs_status = first_alias_value(row, "COGS_Status")

    cost_price_value = to_number(cost_price_text)
    packaging_value = to_number(packaging_cost_text) or 0.0
    other_value = to_number(other_cost_text) or 0.0
    total_unit_cogs_value = to_number(total_unit_cogs_text)

    usable_total = None
    cogs_source = ""
    derived_total = ""
    missing_reason = ""

    if total_unit_cogs_value is not None and total_unit_cogs_value > 0:
        usable_total = total_unit_cogs_value
        cogs_source = "Total_Unit_COGS"
    elif cost_price_value is not None and cost_price_value > 0:
        usable_total = cost_price_value + packaging_value + other_value
        cogs_source = "Derived_From_Cost_Price"
        derived_total = format_money(usable_total)
    else:
        if normalize_text(cogs_status).upper() == "ENTERED":
            missing_reason = "COGS_Status=Entered but no numeric usable COGS found"
        else:
            missing_reason = "No numeric Total_Unit_COGS or Cost_Price found"

    resolved_status = normalize_text(cogs_status)
    if not resolved_status:
        resolved_status = "Entered" if usable_total is not None else "Missing"

    return {
        "Cost_Price": cost_price_text,
        "Packaging_Cost": packaging_cost_text,
        "Other_Cost": other_cost_text,
        "Total_Unit_COGS": format_money(usable_total) if usable_total is not None else "",
        "Derived_Total_Unit_COGS": derived_total if derived_total else (format_money(usable_total) if cogs_source == "Derived_From_Cost_Price" and usable_total is not None else ""),
        "COGS_Status": resolved_status or "Missing",
        "COGS_Source": cogs_source,
        "COGS_Data_Source": cogs_source,
        "COGS_Missing_Reason": missing_reason,
        "_usable_cogs_value": usable_total,
        "_total_unit_cogs_value": total_unit_cogs_value,
        "_cost_price_value": cost_price_value,
    }


def derive_cogs_row(analysis_row: Dict[str, Any], cost_row: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(analysis_row)

    usable_snapshot = get_usable_cogs(cost_row)
    for field in COGS_COLUMNS:
        if normalize_text(merged.get(field, "")):
            continue
        value = usable_snapshot.get(field, "")
        if normalize_text(value):
            merged[field] = value

    cost_price = normalize_text(usable_snapshot.get("Cost_Price", ""))
    packaging_cost = normalize_text(usable_snapshot.get("Packaging_Cost", ""))
    other_cost = normalize_text(usable_snapshot.get("Other_Cost", ""))
    total_unit_cogs = normalize_text(usable_snapshot.get("Total_Unit_COGS", ""))
    cogs_status = normalize_text(usable_snapshot.get("COGS_Status", ""))

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
                "Derived_Total_Unit_COGS": normalize_text(usable_snapshot.get("Derived_Total_Unit_COGS", "")),
                "Total_COGS": format_money(total_cogs),
                "Final_Net_Profit": format_money(final_net_profit),
                "Final_Profit_Per_Order": format_money(final_profit_per_order) if final_profit_per_order != "" else "",
                "Final_Profit_Margin": format_margin(final_profit_margin) if final_profit_margin != "" else "",
                "COGS_Source": normalize_text(usable_snapshot.get("COGS_Source", "")),
                "COGS_Data_Source": normalize_text(usable_snapshot.get("COGS_Data_Source", "")),
                "COGS_Missing_Reason": normalize_text(usable_snapshot.get("COGS_Missing_Reason", "")),
            }
        )
    else:
        for field in ("Total_Unit_COGS", "Derived_Total_Unit_COGS", "Total_COGS", "Final_Net_Profit", "Final_Profit_Per_Order", "Final_Profit_Margin", "COGS_Source", "COGS_Data_Source", "COGS_Missing_Reason"):
            merged[field] = normalize_text(merged.get(field, "")) or normalize_text(usable_snapshot.get(field, ""))

    merged["Cost_Price"] = cost_price
    merged["Packaging_Cost"] = packaging_cost
    merged["Other_Cost"] = other_cost
    merged["COGS_Status"] = cogs_status or "Missing"
    return merged


def hydrate_analysis_rows(
    analysis_rows: Sequence[Dict[str, Any]],
    cost_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cost_fsn_index, cost_sku_index = build_cost_indexes(cost_rows)
    hydrated_rows: List[Dict[str, Any]] = []
    seen_fsns: set[str] = set()

    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in seen_fsns:
            continue
        seen_fsns.add(fsn)
        cost_row, _ = match_cost_row(row, cost_fsn_index, cost_sku_index)
        hydrated_rows.append(derive_cogs_row(row, cost_row))
    return hydrated_rows


def is_cogs_available(row: Dict[str, Any]) -> bool:
    usable_total = get_usable_cogs(row).get("_usable_cogs_value")
    return usable_total is not None and usable_total > 0


def count_cogs_rows(rows: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    available = sum(1 for row in rows if normalize_text(row.get("FSN", "")) and is_cogs_available(row))
    missing = sum(1 for row in rows if normalize_text(row.get("FSN", "")) and not is_cogs_available(row))
    return available, missing
