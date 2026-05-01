from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard.dashboard_google_sheets import load_dashboard_payload as load_dashboard_payload_from_sheet
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_key, normalize_text, parse_float

SOURCE_TABS = [
    "LOOKER_FLIPKART_EXECUTIVE_SUMMARY",
    "LOOKER_FLIPKART_FSN_METRICS",
    "LOOKER_FLIPKART_ALERTS",
    "LOOKER_FLIPKART_ACTIONS",
    "LOOKER_FLIPKART_ADS",
    "LOOKER_FLIPKART_RETURNS",
    "LOOKER_FLIPKART_RETURN_ALL_DETAILS",
    "LOOKER_FLIPKART_CUSTOMER_RETURNS",
    "LOOKER_FLIPKART_COURIER_RETURNS",
    "LOOKER_FLIPKART_RETURN_TYPE_PIVOT",
    "LOOKER_FLIPKART_LISTINGS",
    "LOOKER_FLIPKART_RUN_COMPARISON",
    "LOOKER_FLIPKART_ADJUSTED_PROFIT",
    "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR",
    "LOOKER_FLIPKART_RUN_QUALITY_SCORE",
    "LOOKER_FLIPKART_MODULE_CONFIDENCE",
    "LOOKER_FLIPKART_DEMAND_PROFILE",
    "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE",
    "FLIPKART_RETURN_COMMENTS",
    "FLIPKART_RETURN_ISSUE_SUMMARY",
    "FLIPKART_RETURN_REASON_PIVOT",
    "FLIPKART_RETURN_ALL_DETAILS",
    "FLIPKART_CUSTOMER_RETURN_COMMENTS",
    "FLIPKART_COURIER_RETURN_COMMENTS",
    "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY",
    "FLIPKART_COURIER_RETURN_SUMMARY",
    "FLIPKART_RETURN_TYPE_PIVOT",
    "FLIPKART_MISSING_ACTIVE_LISTINGS",
    "FLIPKART_FSN_RUN_COMPARISON",
    "FLIPKART_VISUAL_COMPETITOR_RESULTS",
    "FLIPKART_ORDER_ITEM_EXPLORER",
    "FLIPKART_ORDER_ITEM_MASTER",
    "FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
    "LOOKER_FLIPKART_ORDER_ITEM_MASTER",
    "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
]

EXECUTIVE_TAB = "LOOKER_FLIPKART_EXECUTIVE_SUMMARY"
FSN_METRICS_TAB = "LOOKER_FLIPKART_FSN_METRICS"
ALERTS_TAB = "LOOKER_FLIPKART_ALERTS"
ACTIONS_TAB = "LOOKER_FLIPKART_ACTIONS"
ADS_TAB = "LOOKER_FLIPKART_ADS"
RETURNS_TAB = "LOOKER_FLIPKART_RETURNS"
RETURN_ALL_DETAILS_TAB = "FLIPKART_RETURN_ALL_DETAILS"
CUSTOMER_RETURN_COMMENTS_TAB = "FLIPKART_CUSTOMER_RETURN_COMMENTS"
COURIER_RETURN_COMMENTS_TAB = "FLIPKART_COURIER_RETURN_COMMENTS"
CUSTOMER_RETURN_SUMMARY_TAB = "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY"
COURIER_RETURN_SUMMARY_TAB = "FLIPKART_COURIER_RETURN_SUMMARY"
RETURN_TYPE_PIVOT_TAB = "FLIPKART_RETURN_TYPE_PIVOT"
LISTINGS_TAB = "LOOKER_FLIPKART_LISTINGS"
RUN_COMPARISON_TAB = "LOOKER_FLIPKART_RUN_COMPARISON"
ADJUSTED_PROFIT_TAB = "LOOKER_FLIPKART_ADJUSTED_PROFIT"
REPORT_FORMAT_TAB = "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR"
RUN_QUALITY_TAB = "LOOKER_FLIPKART_RUN_QUALITY_SCORE"
MODULE_CONFIDENCE_TAB = "LOOKER_FLIPKART_MODULE_CONFIDENCE"
DEMAND_PROFILE_TAB = "LOOKER_FLIPKART_DEMAND_PROFILE"
COMPETITOR_TAB = "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE"
RETURN_COMMENTS_TAB = "FLIPKART_RETURN_COMMENTS"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
RETURN_REASON_PIVOT_TAB = "FLIPKART_RETURN_REASON_PIVOT"
MISSING_ACTIVE_LISTINGS_TAB = "FLIPKART_MISSING_ACTIVE_LISTINGS"
FSN_RUN_COMPARISON_TAB = "FLIPKART_FSN_RUN_COMPARISON"
VISUAL_COMPETITOR_RESULTS_TAB = "FLIPKART_VISUAL_COMPETITOR_RESULTS"
ORDER_ITEM_EXPLORER_TAB = "FLIPKART_ORDER_ITEM_EXPLORER"
LOOKER_ORDER_ITEM_EXPLORER_TAB = "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER"
ORDER_ITEM_MASTER_TAB = "FLIPKART_ORDER_ITEM_MASTER"
ORDER_ITEM_SOURCE_DETAIL_TAB = "FLIPKART_ORDER_ITEM_SOURCE_DETAIL"
LOOKER_ORDER_ITEM_MASTER_TAB = "LOOKER_FLIPKART_ORDER_ITEM_MASTER"
LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB = "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL"

PAGE_ORDER = [
    "Executive Overview",
    "Alerts & Actions",
    "Profit & COGS",
    "Ads Planner",
    "Competitor Risk",
    "Data Quality",
    "Returns Intelligence",
    "Return Comments Explorer",
    "Order ID Explorer",
    "FSN Deep Dive",
    "Listing Issues",
    "Run History & Comparison",
    "Raw Data Explorer / Downloads",
]

DEFAULT_SEARCH_COLUMNS = ["FSN", "SKU_ID", "Product_Title"]
DISPLAY_LIMIT = 500

SEVERITY_PALETTE = {
    "critical": "#fecaca",
    "high": "#fde68a",
    "medium": "#fef3c7",
    "low": "#dcfce7",
}

STATUS_PALETTE = {
    "open": "#fee2e2",
    "assigned": "#e0f2fe",
    "in progress": "#dbeafe",
    "waiting for fresh data": "#e2e8f0",
    "done": "#dcfce7",
    "resolved": "#dcfce7",
    "closed": "#dcfce7",
    "ignored": "#f1f5f9",
    "reopened": "#fae8ff",
}

RISK_PALETTE = {
    "critical": "#fecaca",
    "high": "#fde68a",
    "medium": "#fef3c7",
    "low": "#dcfce7",
    "not enough data": "#e2e8f0",
}

DECISION_PALETTE = {
    "do not run": "#fee2e2",
    "do not run ads / improve economics": "#fee2e2",
    "improve price before ads": "#fef3c7",
    "improve economics before ads": "#fef3c7",
    "fix product first": "#fee2e2",
    "fix product/listing first": "#fee2e2",
    "resolve critical alert first": "#fee2e2",
    "test ads": "#dbeafe",
    "always-on test": "#dbeafe",
    "scale ads": "#dcfce7",
    "continue / optimize ads": "#dcfce7",
}

CONFIDENCE_PALETTE = {
    "high": "#dcfce7",
    "medium": "#fef3c7",
    "low": "#fee2e2",
}

GREY_PALETTE = {
    "pending": "#e2e8f0",
    "not enough data": "#e2e8f0",
    "waiting": "#e2e8f0",
    "unknown": "#e2e8f0",
    "na": "#e2e8f0",
    "n/a": "#e2e8f0",
}

QUALITY_PALETTE = {
    "pass": "#dcfce7",
    "pass with warnings": "#fef3c7",
    "pass_with_warnings": "#fef3c7",
    "warning": "#fef3c7",
    "fail": "#fee2e2",
    "success": "#dcfce7",
    "usable with warnings": "#fef3c7",
}

RETURN_CATEGORY_PALETTE = {
    "product not working": "#fecaca",
    "damaged product": "#fecaca",
    "wrong product": "#fef3c7",
    "quality issue": "#fef3c7",
    "size / expectation mismatch": "#fef3c7",
    "logistics / courier": "#dbeafe",
    "customer refused / rto": "#e2e8f0",
    "return fraud / suspicious": "#fecaca",
    "other": "#e2e8f0",
}

CUSTOMER_RETURN_CATEGORY_PALETTE = {
    "defective product": "#fecaca",
    "damaged product": "#fecaca",
    "missing item / accessory": "#fef3c7",
    "wrong product": "#fecaca",
    "quality issue": "#fef3c7",
    "not as described": "#fef3c7",
    "customer remorse": "#e2e8f0",
    "other customer return": "#e2e8f0",
}

COURIER_RETURN_CATEGORY_PALETTE = {
    "order cancelled": "#e2e8f0",
    "rto / courier return": "#dbeafe",
    "attempts exhausted": "#dbeafe",
    "shipment ageing": "#dbeafe",
    "not serviceable": "#dbeafe",
    "orc validated with customer": "#dbeafe",
    "delivery failed": "#dbeafe",
    "other courier return": "#e2e8f0",
}

COMPARISON_PALETTE = {
    "improved": "#dcfce7",
    "better": "#dcfce7",
    "worsened": "#fee2e2",
    "worse": "#fee2e2",
    "no change": "#e2e8f0",
    "new": "#dbeafe",
}


def values_to_dataframe(values: Sequence[Sequence[Any]]) -> pd.DataFrame:
    if not values:
        return pd.DataFrame()
    headers = [str(cell) for cell in values[0]]
    rows: List[Dict[str, Any]] = []
    for row in values[1:]:
        row_dict = {headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))}
        if any(normalize_text(value) for value in row_dict.values()):
            rows.append(row_dict)
    return pd.DataFrame(rows, columns=headers)


def dataframe_or_empty(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    return df.copy()


def first_available_frame(frames: Dict[str, pd.DataFrame], tab_names: Sequence[str]) -> tuple[pd.DataFrame, str]:
    for tab_name in tab_names:
        df = dataframe_or_empty(frames.get(tab_name, pd.DataFrame()))
        if not df.empty:
            return df, tab_name
    if tab_names:
        return dataframe_or_empty(frames.get(tab_names[0], pd.DataFrame())), tab_names[0]
    return pd.DataFrame(), ""


def unique_text_values(df: pd.DataFrame, column: str) -> List[str]:
    if column not in df.columns:
        return []
    values = []
    seen = set()
    for raw in df[column].tolist():
        value = normalize_text(raw)
        if value and value not in seen:
            seen.add(value)
            values.append(value)
    return sorted(values, key=lambda item: item.lower())


def count_unique_non_blank(df: pd.DataFrame, column: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    return len({normalize_text(value) for value in df[column].fillna("").astype(str).tolist() if normalize_text(value)})


def resolve_column(df: pd.DataFrame, candidates: Sequence[str]) -> str:
    if df.empty or not len(df.columns):
        return ""
    normalized_lookup = {normalize_key(column): column for column in df.columns}
    for candidate in candidates:
        normalized = normalize_key(candidate)
        if normalized in normalized_lookup:
            return normalized_lookup[normalized]
    for candidate in candidates:
        normalized = normalize_key(candidate)
        for key, actual in normalized_lookup.items():
            if normalized and (normalized in key or key in normalized):
                return actual
    return ""


def column_series(df: pd.DataFrame, candidates: Sequence[str]) -> pd.Series:
    column = resolve_column(df, candidates)
    if not column:
        return pd.Series(dtype="object")
    return df[column].fillna("").astype(str)


def filter_by_query(df: pd.DataFrame, query: str, columns: Sequence[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    query_text = normalize_text(query).lower()
    if not query_text:
        return df.copy()
    search_columns = [column for column in (columns or df.columns) if column in df.columns]
    if not search_columns:
        return df.iloc[0:0].copy()
    mask = pd.Series(False, index=df.index)
    for column in search_columns:
        series = df[column].fillna("").astype(str).str.lower()
        mask = mask | series.str.contains(re.escape(query_text), regex=True, na=False)
    return df.loc[mask].copy()


def filter_by_selected_values(df: pd.DataFrame, column: str, selected_values: Sequence[str]) -> pd.DataFrame:
    if df.empty or not column or column not in df.columns or not selected_values:
        return df.copy()
    selected = {normalize_text(value) for value in selected_values if normalize_text(value)}
    if not selected:
        return df.copy()
    return df[df[column].fillna("").astype(str).map(normalize_text).isin(selected)].copy()


def parse_percent(text: Any) -> float:
    value = parse_float(text)
    raw = normalize_text(text)
    if "%" in raw:
        return value
    if abs(value) <= 1.5:
        return value * 100.0
    return value


def format_count(value: Any) -> str:
    number = parse_float(value)
    if number == 0 and normalize_text(value) == "":
        return "0"
    return f"{int(round(number)):,}"


def format_money(value: Any) -> str:
    number = parse_float(value)
    return f"{number:,.2f}"


def format_percent(value: Any) -> str:
    number = parse_percent(value)
    return f"{number:.2f}%"


def format_text_or_dash(value: Any) -> str:
    text = normalize_text(value)
    return text if text else "-"


def metric_card(label: str, value: Any, help_text: str | None = None) -> str:
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{format_text_or_dash(value)}</div>
        <div class="metric-note">{format_text_or_dash(help_text) if help_text else ""}</div>
    </div>
    """


def status_badge(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return '<span class="status-badge status-grey">-</span>'
    key = normalize_key(text)
    if key in {"critical", "high"}:
        palette_class = f"status-{key}"
    elif key in {"medium"}:
        palette_class = "status-medium"
    elif key in {"low", "good", "done", "resolved", "closed", "scale ads", "test ads", "always-on test"}:
        palette_class = "status-low"
    elif key in {"pending", "not enough data", "waiting", "na", "n/a", "unknown"}:
        palette_class = "status-grey"
    else:
        palette_class = "status-grey"
    return f'<span class="status-badge {palette_class}">{text}</span>'


def load_sheet_tab(tab_name: str, payload: Dict[str, Any] | None = None) -> pd.DataFrame:
    source = payload or load_dashboard_payload()
    return dataframe_or_empty(source["frames"].get(tab_name, pd.DataFrame()))


def safe_dataframe(tab_name: str, payload: Dict[str, Any] | None = None) -> pd.DataFrame:
    try:
        return load_sheet_tab(tab_name, payload)
    except Exception:
        return pd.DataFrame()


def filter_dataframe(
    df: pd.DataFrame,
    search_cols: Sequence[str] | None = None,
    filters: Dict[str, Sequence[str] | str | None] | None = None,
) -> pd.DataFrame:
    filtered = dataframe_or_empty(df)
    if filtered.empty:
        return filtered
    for column, selected in (filters or {}).items():
        if column not in filtered.columns or selected is None:
            continue
        if isinstance(selected, str):
            selected_values = [selected]
        else:
            selected_values = [value for value in selected if normalize_text(value)]
        if not selected_values:
            continue
        normalized_selected = {normalize_text(value) for value in selected_values}
        filtered = filtered[filtered[column].fillna("").astype(str).map(normalize_text).isin(normalized_selected)].copy()
        if filtered.empty:
            return filtered
    return filtered


def apply_global_search(df: pd.DataFrame, search_filters: Dict[str, str] | None, search_cols: Sequence[str]) -> pd.DataFrame:
    filtered = dataframe_or_empty(df)
    if filtered.empty or not search_filters:
        return filtered
    if search_filters.get("fsn"):
        filtered = filter_by_query(filtered, search_filters["fsn"], [column for column in search_cols if "FSN" in column.upper() or "ID" in column.upper()])
    if search_filters.get("sku"):
        filtered = filter_by_query(filtered, search_filters["sku"], [column for column in search_cols if "SKU" in column.upper() or "ITEM" in column.upper()])
    if search_filters.get("product"):
        filtered = filter_by_query(filtered, search_filters["product"], [column for column in search_cols if any(token in column.upper() for token in ["TITLE", "PRODUCT", "NAME", "DESC"])])
    return filtered


def download_button(df: pd.DataFrame, file_name: str, label: str = "Download CSV", key: str | None = None) -> None:
    render_download_button(df, file_name, label, key=key)


def style_status_value(value: Any) -> str:
    return style_cell_value(value, GREY_PALETTE)


def load_dashboard_payload() -> Dict[str, Any]:
    return load_dashboard_payload_from_sheet()


def build_fsn_index(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "FSN" not in df.columns:
        return pd.DataFrame(columns=["FSN", "SKU_ID", "Product_Title"])
    columns = [column for column in ["FSN", "SKU_ID", "Product_Title", "Category", "Last_Updated"] if column in df.columns]
    indexed = df.loc[:, columns].copy()
    indexed["FSN"] = indexed["FSN"].map(clean_fsn)
    indexed = indexed[indexed["FSN"].map(bool)]
    if "Product_Title" not in indexed.columns:
        indexed["Product_Title"] = ""
    if "SKU_ID" not in indexed.columns:
        indexed["SKU_ID"] = ""
    indexed = indexed.drop_duplicates(subset=["FSN"], keep="first")
    return indexed.sort_values(by=[column for column in ["Product_Title", "FSN"] if column in indexed.columns], kind="stable")


def build_metric_lookup(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    if df.empty or "Metric_Name" not in df.columns:
        return lookup
    for _, row in df.iterrows():
        metric_name = normalize_text(row.get("Metric_Name", ""))
        if metric_name:
            lookup[metric_name] = row.to_dict()
    return lookup


def metric_lookup_value(lookup: Dict[str, Dict[str, Any]], *names: str) -> str:
    for name in names:
        row = lookup.get(name)
        if not row:
            continue
        value = normalize_text(row.get("Metric_Display_Value", "")) or normalize_text(row.get("Metric_Value", ""))
        if value:
            return value
    return ""


def metric_lookup_numeric(lookup: Dict[str, Dict[str, Any]], *names: str) -> float:
    value = metric_lookup_value(lookup, *names)
    return parse_float(value)


def latest_non_blank_value(df: pd.DataFrame, candidates: Sequence[str]) -> str:
    for column in candidates:
        if column in df.columns:
            for value in reversed(df[column].fillna("").astype(str).tolist()):
                text = normalize_text(value)
                if text:
                    return text
    return ""


def count_non_blank_rows(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    return int(sum(1 for _, row in df.iterrows() if any(normalize_text(value) for value in row.values)))


def count_matching_values(df: pd.DataFrame, column: str, target: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    normalized_target = normalize_text(target).lower()
    return int((df[column].fillna("").astype(str).str.lower() == normalized_target).sum())


def count_contains(df: pd.DataFrame, column: str, needle: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    needle_text = normalize_text(needle).lower()
    if not needle_text:
        return 0
    return int(df[column].fillna("").astype(str).str.lower().str.contains(re.escape(needle_text), regex=True, na=False).sum())


def positive_negative_split(df: pd.DataFrame, column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty or column not in df.columns:
        return df.iloc[0:0].copy(), df.iloc[0:0].copy()
    numeric = df[column].map(parse_float)
    positive = df.loc[numeric >= 0].copy()
    negative = df.loc[numeric < 0].copy()
    return positive, negative


def style_cell_value(value: Any, palette: Dict[str, str]) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    color = palette.get(text.lower())
    if not color:
        color = palette.get(normalize_key(text), "#e2e8f0")
    return f"background-color: {color}; color: #0f172a; font-weight: 700;"


def style_profit_cell(value: Any) -> str:
    number = parse_float(value)
    if number < 0:
        return "background-color: #fee2e2; color: #991b1b; font-weight: 700;"
    if number > 0:
        return "background-color: #dcfce7; color: #166534; font-weight: 700;"
    return ""


def apply_table_styles(df: pd.DataFrame, style_columns: Dict[str, Dict[str, str]] | None = None) -> pd.io.formats.style.Styler:
    styler = df.style.hide(axis="index")
    for column, palette in (style_columns or {}).items():
        if column in df.columns:
            styler = styler.map(lambda value, p=palette: style_cell_value(value, p), subset=[column])
    for column in [
        "Final_Net_Profit",
        "Adjusted_Final_Net_Profit",
        "Net_Adjustment",
        "Price_Gap_Percent",
        "Competition_Risk_Score",
        "Overall_Confidence_Score",
        "Overall_Run_Quality_Score",
        "Delta_Value",
    ]:
        if column in df.columns:
            styler = styler.map(style_profit_cell if column in {"Final_Net_Profit", "Adjusted_Final_Net_Profit", "Net_Adjustment", "Delta_Value"} else lambda value: "", subset=[column])
    return styler


def section_card(title: str, body: str) -> str:
    return f"""
    <div class="section-card">
        <div class="section-title">{title}</div>
        <div class="section-body">{body}</div>
    </div>
    """


def render_metric_cards(metrics: Sequence[Dict[str, str]], columns: int = 4) -> None:
    if not metrics:
        return
    for start in range(0, len(metrics), columns):
        row = metrics[start : start + columns]
        cols = st.columns(len(row))
        for index, metric in enumerate(row):
            with cols[index]:
                st.markdown(
                    f"""
                    <div class="metric-card">
                        <div class="metric-label">{metric.get("label", "")}</div>
                        <div class="metric-value">{metric.get("value", "")}</div>
                        <div class="metric-note">{metric.get("note", "")}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def render_download_button(df: pd.DataFrame, file_name: str, label: str = "Download CSV", key: str | None = None) -> None:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    button_key = key or f"download_{normalize_key(file_name)}_{normalize_key(label)}"
    st.download_button(label, data=csv_bytes, file_name=file_name, mime="text/csv", use_container_width=True, key=button_key)


def render_dataframe_section(
    title: str,
    df: pd.DataFrame,
    file_name: str,
    *,
    caption: str | None = None,
    preferred_columns: Sequence[str] | None = None,
    style_columns: Dict[str, Dict[str, str]] | None = None,
    max_rows: int = DISPLAY_LIMIT,
) -> None:
    st.markdown(f"### {title}")
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No rows matched the current filters.")
        return
    view_df = df.copy()
    if preferred_columns:
        selected = [column for column in preferred_columns if column in view_df.columns]
        if selected:
            view_df = view_df.loc[:, selected]
    if len(view_df) > max_rows:
        view_df = view_df.head(max_rows).copy()
    top_bar = st.columns([3, 1])
    with top_bar[1]:
        render_download_button(view_df, file_name)
    styled = apply_table_styles(view_df, style_columns=style_columns)
    st.dataframe(styled, use_container_width=True, height=min(650, 36 + 34 * max(4, min(len(view_df), 14))))


def render_warning_banner(message: str) -> None:
    st.warning(message)


def display_status_strip(data: Dict[str, Any]) -> None:
    loaded_tabs = len(SOURCE_TABS) - len(data.get("missing_tabs", []))
    status_bits = [
        f"Dashboard: `{'Online' if data.get('spreadsheet_connected') else 'Degraded'}`",
        f"Spreadsheet connected: `{'Yes' if data.get('spreadsheet_connected') else 'No'}`",
        f"Last load: `{data.get('last_data_load_timestamp', '-')}`",
        f"Tabs loaded: `{loaded_tabs}/{len(SOURCE_TABS)}`",
    ]
    if data.get("missing_tabs"):
        status_bits.append(f"Missing: `{', '.join(data['missing_tabs'])}`")
    st.caption(" | ".join(status_bits))


def build_overview_metrics(frames: Dict[str, pd.DataFrame]) -> tuple[List[Dict[str, str]], Dict[str, Dict[str, Any]]]:
    executive_df = dataframe_or_empty(frames[EXECUTIVE_TAB])
    fsn_df = dataframe_or_empty(frames[FSN_METRICS_TAB])
    alerts_df = dataframe_or_empty(frames[ALERTS_TAB])
    actions_df = dataframe_or_empty(frames[ACTIONS_TAB])
    ads_df = dataframe_or_empty(frames[ADS_TAB])
    returns_df = dataframe_or_empty(frames[RETURNS_TAB])
    listings_df = dataframe_or_empty(frames[LISTINGS_TAB])
    profit_df = dataframe_or_empty(frames[ADJUSTED_PROFIT_TAB])
    run_quality_df = dataframe_or_empty(frames[RUN_QUALITY_TAB])
    metric_lookup = build_metric_lookup(executive_df)

    quality_lookup = build_metric_lookup(run_quality_df)
    run_quality_score = metric_lookup_value(quality_lookup, "Overall Run Quality Score")
    if not run_quality_score:
        run_quality_score = metric_lookup_value(metric_lookup, "Overall Run Quality Score")

    total_target_fsns = metric_lookup_numeric(metric_lookup, "Total Target FSNs")
    if not total_target_fsns:
        total_target_fsns = float(len(build_fsn_index(fsn_df)))
    final_profit = metric_lookup_value(metric_lookup, "Final Profit")
    if not final_profit:
        if "Adjusted_Final_Net_Profit" in profit_df.columns and not profit_df.empty:
            final_profit = format_money(profit_df["Adjusted_Final_Net_Profit"].map(parse_float).sum())
        elif "Adjusted_Final_Net_Profit" in fsn_df.columns and not fsn_df.empty:
            final_profit = format_money(fsn_df["Adjusted_Final_Net_Profit"].map(parse_float).sum())
        elif "Final_Net_Profit" in fsn_df.columns and not fsn_df.empty:
            final_profit = format_money(fsn_df["Final_Net_Profit"].map(parse_float).sum())
    total_adjusted_profit = format_money(profit_df["Adjusted_Final_Net_Profit"].map(parse_float).sum()) if "Adjusted_Final_Net_Profit" in profit_df.columns and not profit_df.empty else ""
    total_cogs = ""
    if "Total_COGS" in fsn_df.columns and not fsn_df.empty:
        total_cogs = format_money(fsn_df["Total_COGS"].map(parse_float).sum())

    total_alerts = metric_lookup_numeric(metric_lookup, "Total Alerts")
    if not total_alerts:
        total_alerts = float(count_non_blank_rows(alerts_df))
    critical_alerts = metric_lookup_numeric(metric_lookup, "Critical Alerts")
    if not critical_alerts:
        critical_alerts = float(count_matching_values(alerts_df, "Severity", "Critical"))
    high_alerts = metric_lookup_numeric(metric_lookup, "High Alerts")
    if not high_alerts:
        high_alerts = float(count_matching_values(alerts_df, "Severity", "High"))
    active_tasks = metric_lookup_numeric(metric_lookup, "Active Tasks")
    if not active_tasks:
        active_tasks = float(count_non_blank_rows(actions_df))
    missing_cogs = metric_lookup_numeric(metric_lookup, "Missing COGS")
    if not missing_cogs and "COGS_Status" in fsn_df.columns:
        missing_cogs = float(count_contains(fsn_df, "COGS_Status", "missing"))
    missing_listings = metric_lookup_numeric(metric_lookup, "Missing Active Listings")
    if not missing_listings and "Listing_Presence_Status" in listings_df.columns:
        missing_listings = float(count_contains(listings_df, "Listing_Presence_Status", "missing"))
    ads_ready = metric_lookup_numeric(metric_lookup, "Ads Ready Count")
    if not ads_ready and "Final_Ads_Decision" in ads_df.columns:
        ads_ready = float(
            sum(
                count_matching_values(ads_df, "Final_Ads_Decision", decision)
                for decision in ["Test Ads", "Always-On Test", "Seasonal/Event Test", "Scale Ads", "Continue / Optimize Ads"]
            )
        )
    return_issue_fsns = metric_lookup_numeric(metric_lookup, "Return Issue FSNs")
    if not return_issue_fsns:
        return_issue_fsns = float(len(build_fsn_index(returns_df)))
    cogs_completion = metric_lookup_value(metric_lookup, "COGS Completion Percent")
    if not cogs_completion and "COGS_Status" in fsn_df.columns:
        cogs_available = int((~fsn_df["COGS_Status"].fillna("").astype(str).str.contains("missing", case=False, na=False)).sum())
        cogs_completion = format_percent((cogs_available / len(fsn_df) * 100.0) if len(fsn_df) else 0.0)
    low_margin_count = 0
    if "Final_Profit_Margin" in fsn_df.columns and not fsn_df.empty:
        low_margin_count = int((fsn_df["Final_Profit_Margin"].map(parse_percent) < 10).sum())
    negative_profit_count = 0
    if "Final_Net_Profit" in fsn_df.columns and not fsn_df.empty:
        negative_profit_count = int((fsn_df["Final_Net_Profit"].map(parse_float) < 0).sum())

    metrics = [
        {"label": "Total Target FSNs", "value": f"{int(total_target_fsns):,}", "note": "Unique FSNs in the control tower"},
        {"label": "Final Profit", "value": final_profit, "note": "From the executive summary"},
        {"label": "Adjusted Profit", "value": total_adjusted_profit or "-", "note": "Adjustment-aware total"},
        {"label": "Total COGS", "value": total_cogs or "-", "note": "From the current FSN metrics"},
        {"label": "Run Quality", "value": run_quality_score or "-", "note": "Latest scorecard result"},
        {"label": "Total Alerts", "value": f"{int(total_alerts):,}", "note": "All generated alerts"},
        {"label": "Critical Alerts", "value": f"{int(critical_alerts):,}", "note": "Immediate attention"},
        {"label": "High Alerts", "value": f"{int(high_alerts):,}", "note": "Needs fast follow-up"},
        {"label": "Active Tasks", "value": f"{int(active_tasks):,}", "note": "Open action rows"},
        {"label": "Missing COGS", "value": f"{int(missing_cogs):,}", "note": "FSNs still waiting on cost"},
        {"label": "Missing Listings", "value": f"{int(missing_listings):,}", "note": "Not present in active listing"},
        {"label": "Ads Ready", "value": f"{int(ads_ready):,}", "note": "Safe for test or scale"},
        {"label": "Return Issue FSNs", "value": f"{int(return_issue_fsns):,}", "note": "FSNs with issue summaries"},
        {"label": "Low Margin FSNs", "value": f"{low_margin_count:,}", "note": "Margin below 10%"},
        {"label": "Negative Profit FSNs", "value": f"{negative_profit_count:,}", "note": "Needs immediate review"},
        {"label": "COGS Completion", "value": cogs_completion or "-", "note": "Coverage from the latest source tabs"},
    ]
    return metrics, metric_lookup


def render_page_header(title: str, subtitle: str, run_id: str = "") -> None:
    st.markdown(
        f"""
        <div class="hero-card">
            <div class="hero-kicker">Flipkart Control Tower</div>
            <h1>{title}</h1>
            <p>{subtitle}</p>
            <div class="hero-meta">{f"Latest run: {run_id}" if run_id else "Read-only Streamlit view over Flipkart Looker source tabs."}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_chart_from_counts(df: pd.DataFrame, x_column: str, y_title: str, color: str = "") -> None:
    if df.empty or x_column not in df.columns:
        return
    counts = df[x_column].fillna("").astype(str)
    counts = counts[counts.map(bool)].value_counts().reset_index()
    counts.columns = [x_column, y_title]
    if counts.empty:
        return
    fig = px.bar(
        counts,
        x=x_column,
        y=y_title,
        text=y_title,
        color=x_column if color else None,
        color_discrete_sequence=["#0f766e", "#2563eb", "#7c3aed", "#d97706", "#dc2626", "#0891b2"],
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=320,
        showlegend=False,
        xaxis_title="",
        yaxis_title="",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_executive_overview(frames: Dict[str, pd.DataFrame], metric_lookup: Dict[str, Dict[str, Any]], search_filters: Dict[str, str]) -> None:
    executive_df = dataframe_or_empty(frames[EXECUTIVE_TAB])
    run_quality_df = dataframe_or_empty(frames[RUN_QUALITY_TAB])
    demand_df = dataframe_or_empty(frames[DEMAND_PROFILE_TAB])
    alerts_df = dataframe_or_empty(frames[ALERTS_TAB])
    actions_df = dataframe_or_empty(frames[ACTIONS_TAB])
    ads_df = dataframe_or_empty(frames[ADS_TAB])
    returns_df = dataframe_or_empty(frames[RETURNS_TAB])
    listings_df = dataframe_or_empty(frames[LISTINGS_TAB])
    adjusted_profit_df = dataframe_or_empty(frames[ADJUSTED_PROFIT_TAB])

    filtered_exec = apply_global_search(executive_df, search_filters, ["Metric_Category", "Metric_Name", "Metric_Value", "Metric_Display_Value", "Run_ID"])
    latest_run_id = latest_non_blank_value(executive_df, ["Run_ID"]) or latest_non_blank_value(run_quality_df, ["Run_ID"])
    render_page_header(
        "Executive Overview",
        "Mission-control summary for the latest Flipkart Control Tower tabs. This view is read-only and meant to replace Looker Studio for daily operations.",
        latest_run_id,
    )
    render_metric_cards(build_overview_metrics(frames)[0], columns=4)
    st.markdown("### Main Warnings")
    demand_status_col = resolve_column(demand_df, ["Cache_Status_Summary", "Demand_Status", "Status"])
    competitor_df = dataframe_or_empty(frames[COMPETITOR_TAB])
    competitor_risk_col = resolve_column(competitor_df, ["Competition_Risk_Level"])
    keyword_pending = demand_status_col and count_contains(demand_df, demand_status_col, "pending") > 0
    competitor_not_enough = competitor_risk_col and count_matching_values(competitor_df, competitor_risk_col, "Not Enough Data") > 0
    google_ads_pending = False
    if not demand_df.empty:
        combined_text = " ".join(
            normalize_text(value).lower()
            for value in demand_df.fillna("").astype(str).head(40).values.flatten().tolist()
        )
        google_ads_pending = "basic access pending" in combined_text or "google ads basic access pending" in combined_text or keyword_pending
    warning_specs = [
        ("Keyword cache pending", "Pending" if keyword_pending else "Ready", "Low/grey" if keyword_pending else "Resolved"),
        ("Competitor Not Enough Data", "Present" if competitor_not_enough else "Clear", "Needs more image/search context" if competitor_not_enough else "No current gaps"),
        ("Google Ads Basic Access", "Pending" if google_ads_pending else "Ready", "Manual approval still needed" if google_ads_pending else "Cached planning available"),
    ]
    warning_cols = st.columns(len(warning_specs))
    for index, (label, value, help_text) in enumerate(warning_specs):
        with warning_cols[index]:
            st.markdown(metric_card(label, value, help_text), unsafe_allow_html=True)

    st.markdown("### Executive Summary Table")
    if filtered_exec.empty:
        st.info("No executive summary rows matched the current search.")
    else:
        preferred = ["Report_Date", "Run_ID", "Metric_Category", "Metric_Name", "Metric_Value", "Metric_Display_Value", "Sort_Order", "Last_Updated"]
        render_download_button(filtered_exec.loc[:, [column for column in preferred if column in filtered_exec.columns]], "flipkart_executive_summary_filtered.csv")
        st.dataframe(
            apply_table_styles(
                filtered_exec.loc[:, [column for column in preferred if column in filtered_exec.columns]],
            ),
            use_container_width=True,
            height=min(600, 36 + 34 * max(6, min(len(filtered_exec), 16))),
        )
    category_col = resolve_column(executive_df, ["Metric_Category"])
    if category_col:
        st.markdown("### Metric Coverage")
        render_chart_from_counts(executive_df, category_col, "Metric_Count")

    run_quality_filtered = apply_global_search(run_quality_df, search_filters, ["Run_ID", "Score_Category", "Score_Name", "Reason", "Suggested_Action"])
    summary_rows = run_quality_filtered[run_quality_filtered.get("Record_Type", pd.Series(dtype="object")).fillna("").astype(str).str.lower() == "summary"] if "Record_Type" in run_quality_filtered.columns else run_quality_filtered
    breakdown_rows = run_quality_filtered[run_quality_filtered.get("Record_Type", pd.Series(dtype="object")).fillna("").astype(str).str.lower() == "breakdown"] if "Record_Type" in run_quality_filtered.columns else run_quality_filtered
    st.markdown("### Run Quality Breakdown")
    if breakdown_rows.empty:
        st.info("No run quality breakdown rows matched the current search.")
    else:
        breakdown_cols = ["Score_Category", "Score_Name", "Max_Points", "Points_Earned", "Status", "Reason", "Suggested_Action"]
        render_dataframe_section(
            "Run Quality Breakdown Table",
            breakdown_rows,
            "flipkart_run_quality_breakdown.csv",
            preferred_columns=breakdown_cols,
            style_columns={"Status": STATUS_PALETTE, "Score_Category": GREY_PALETTE},
        )
    render_dataframe_section(
        "Run Quality Summary",
        summary_rows,
        "flipkart_run_quality_summary.csv",
        preferred_columns=["Run_ID", "Report_Date", "Overall_Run_Quality_Score", "Run_Quality_Grade", "Decision_Recommendation", "Critical_Warnings", "Major_Warnings", "Last_Updated"],
        style_columns={"Run_Quality_Grade": QUALITY_PALETTE, "Decision_Recommendation": DECISION_PALETTE},
    )


def render_alerts_actions(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    alerts_df = dataframe_or_empty(frames[ALERTS_TAB])
    actions_df = dataframe_or_empty(frames[ACTIONS_TAB])
    severity_col = resolve_column(alerts_df, ["Severity"])
    owner_col = resolve_column(actions_df, ["Owner"])
    status_col = resolve_column(actions_df, ["Status"])
    alert_type_col = resolve_column(alerts_df, ["Alert_Type"])
    action_type_col = resolve_column(actions_df, ["Alert_Type", "Action_Type"])
    alerts_filtered = apply_global_search(alerts_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Alert_Type", "Suggested_Action", "Reason", "Severity", "Status_Default"])
    actions_filtered = apply_global_search(actions_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Alert_Type", "Action_Taken", "Owner", "Status", "Resolution_Notes"])
    render_page_header(
        "Alerts & Actions",
        "Track alert severity and the matching operational owner/status workflow without jumping into the pipeline.",
        latest_non_blank_value(alerts_df, ["Run_ID"]),
    )

    filter_cols = st.columns(4)
    selected_severities = unique_text_values(alerts_filtered, severity_col) if severity_col else []
    selected_alert_types = unique_text_values(alerts_filtered, alert_type_col) if alert_type_col else []
    selected_owners = unique_text_values(actions_filtered, owner_col) if owner_col else []
    selected_statuses = unique_text_values(actions_filtered, status_col) if status_col else []
    with filter_cols[0]:
        severity_pick = st.multiselect("Severity", selected_severities, default=selected_severities, key="alerts_page_severity")
    with filter_cols[1]:
        alert_type_pick = st.multiselect("Alert type", selected_alert_types, default=selected_alert_types, key="alerts_page_type")
    with filter_cols[2]:
        owner_pick = st.multiselect("Owner", selected_owners, default=selected_owners, key="alerts_page_owner")
    with filter_cols[3]:
        status_pick = st.multiselect("Status", selected_statuses, default=selected_statuses, key="alerts_page_status")

    if severity_col:
        alerts_filtered = filter_by_selected_values(alerts_filtered, severity_col, severity_pick)
    if alert_type_col:
        alerts_filtered = filter_by_selected_values(alerts_filtered, alert_type_col, alert_type_pick)
    if owner_col:
        actions_filtered = filter_by_selected_values(actions_filtered, owner_col, owner_pick)
    if status_col:
        actions_filtered = filter_by_selected_values(actions_filtered, status_col, status_pick)
    if action_type_col and alert_type_pick:
        actions_filtered = filter_by_selected_values(actions_filtered, action_type_col, alert_type_pick)

    critical_alerts = count_matching_values(alerts_filtered, severity_col, "Critical") if severity_col else 0
    high_alerts = count_matching_values(alerts_filtered, severity_col, "High") if severity_col else 0
    open_actions = count_matching_values(actions_filtered, status_col, "Open") if status_col else 0
    in_progress_actions = count_matching_values(actions_filtered, status_col, "In Progress") if status_col else 0
    render_metric_cards(
        [
            {"label": "Filtered Alerts", "value": f"{len(alerts_filtered):,}", "note": "Current search and severity filters"},
            {"label": "Critical Alerts", "value": f"{critical_alerts:,}", "note": "Immediate attention"},
            {"label": "High Alerts", "value": f"{high_alerts:,}", "note": "Needs fast follow-up"},
            {"label": "Open Actions", "value": f"{open_actions:,}", "note": "Needs assignment"},
            {"label": "In Progress", "value": f"{in_progress_actions:,}", "note": "Already moving"},
        ],
        columns=5,
    )
    if severity_col and not alerts_filtered.empty:
        st.markdown("### Alert Severity Mix")
        render_chart_from_counts(alerts_filtered, severity_col, "Alert_Count")
    st.markdown("### Main Action Table")
    action_cols = ["Action_ID", "Alert_ID", "FSN", "SKU_ID", "Product_Title", "Alert_Type", "Severity", "Owner", "Status", "Action_Taken", "Expected_Impact", "Review_After_Date", "Last_Updated"]
    if actions_filtered.empty:
        st.info("No action rows matched the current filters.")
    else:
        top_controls = st.columns([4, 1])
        with top_controls[1]:
            download_button(actions_filtered.loc[:, [column for column in action_cols if column in actions_filtered.columns]], "flipkart_actions_filtered.csv")
        st.dataframe(
            apply_table_styles(
                actions_filtered.loc[:, [column for column in action_cols if column in actions_filtered.columns]],
                style_columns={"Status": STATUS_PALETTE, "Severity": SEVERITY_PALETTE},
            ),
            use_container_width=True,
            height=min(650, 36 + 34 * max(6, min(len(actions_filtered), 16))),
        )
    with st.expander("Filtered alerts table", expanded=False):
        render_dataframe_section(
            "Alerts Table",
            alerts_filtered,
            "flipkart_alerts_filtered.csv",
            caption="Colored by alert severity so the urgent rows stand out immediately.",
            preferred_columns=["Run_ID", "Alert_ID", "FSN", "SKU_ID", "Product_Title", "Alert_Type", "Severity", "Suggested_Action", "Reason", "Data_Confidence", "Status_Default", "Last_Updated"],
            style_columns={"Severity": SEVERITY_PALETTE},
        )


def render_profit_cogs(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    fsn_df = dataframe_or_empty(frames[FSN_METRICS_TAB])
    profit_df = dataframe_or_empty(frames[ADJUSTED_PROFIT_TAB])
    fsn_filtered = apply_global_search(fsn_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Category", "COGS_Status"])
    profit_filtered = apply_global_search(profit_df, search_filters, ["FSN", "SKU_ID", "Product_Title"])
    render_page_header(
        "Profit & COGS",
        "Use this page to inspect margin, cost coverage, and the adjustment ledger without touching MASTER_SKU or the production pipeline.",
        latest_non_blank_value(fsn_df, ["Run_ID"]),
    )
    final_profit_col = resolve_column(fsn_filtered, ["Final_Net_Profit", "Adjusted_Final_Net_Profit"])
    cogs_col = resolve_column(fsn_filtered, ["COGS_Status"])
    margin_col = resolve_column(fsn_filtered, ["Final_Profit_Margin"])
    adjusted_profit_col = resolve_column(profit_filtered, ["Adjusted_Final_Net_Profit"])
    total_adjusted_profit = format_money(profit_filtered[adjusted_profit_col].map(parse_float).sum()) if adjusted_profit_col and not profit_filtered.empty else "-"
    total_cogs = format_money(fsn_filtered["Total_COGS"].map(parse_float).sum()) if "Total_COGS" in fsn_filtered.columns and not fsn_filtered.empty else "-"
    negative_profit_rows = pd.DataFrame()
    low_margin_rows = pd.DataFrame()
    if final_profit_col and not fsn_filtered.empty:
        numeric = fsn_filtered[final_profit_col].map(parse_float)
        negative_profit_rows = fsn_filtered.loc[numeric < 0].copy()
    if margin_col and not fsn_filtered.empty:
        margin_numeric = fsn_filtered[margin_col].map(parse_percent)
        low_margin_rows = fsn_filtered.loc[margin_numeric < 10].copy()
    missing_cogs_count = count_contains(fsn_filtered, cogs_col, "missing") if cogs_col else 0
    avg_margin = format_percent(fsn_filtered[margin_col].map(parse_percent).mean()) if margin_col and not fsn_filtered.empty else "-"
    cogs_status_values = unique_text_values(fsn_filtered, cogs_col) if cogs_col else []
    selected_cogs_statuses = st.multiselect("COGS status", cogs_status_values, default=cogs_status_values, key="profit_cogs_status_filter")
    if cogs_col and selected_cogs_statuses:
        fsn_filtered = filter_by_selected_values(fsn_filtered, cogs_col, selected_cogs_statuses)
    render_metric_cards(
        [
            {"label": "Total Adjusted Profit", "value": total_adjusted_profit, "note": "Adjustment-aware total"},
            {"label": "Total COGS", "value": total_cogs, "note": "Current FSN metrics"},
            {"label": "Missing COGS", "value": f"{missing_cogs_count:,}", "note": "Still waiting on cost"},
            {"label": "Negative Profit FSNs", "value": f"{len(negative_profit_rows):,}", "note": "Needs review"},
            {"label": "Low Margin FSNs", "value": f"{len(low_margin_rows):,}", "note": "Below 10% margin"},
            {"label": "Avg Margin", "value": avg_margin, "note": "Final profit margin"},
        ],
        columns=3,
    )
    if final_profit_col and not fsn_filtered.empty:
        st.markdown("### Profit Distribution")
        sorted_profit = fsn_filtered.loc[:, [column for column in ["FSN", "SKU_ID", "Product_Title", final_profit_col] if column in fsn_filtered.columns]].copy()
        sorted_profit[final_profit_col] = sorted_profit[final_profit_col].map(parse_float)
        top_profit = sorted_profit.sort_values(by=final_profit_col, ascending=False).head(8)
        bottom_profit = sorted_profit.sort_values(by=final_profit_col, ascending=True).head(8)
        chart_df = pd.concat([top_profit, bottom_profit], ignore_index=True)
        chart_df["Bucket"] = ["Top Profit"] * len(top_profit) + ["Lowest Profit"] * len(bottom_profit)
        fig = px.bar(
            chart_df,
            x="FSN",
            y=final_profit_col,
            color="Bucket",
            barmode="group",
            hover_data=[column for column in ["SKU_ID", "Product_Title"] if column in chart_df.columns],
            color_discrete_map={"Top Profit": "#0f766e", "Lowest Profit": "#dc2626"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=320, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    fsn_cols = ["Run_ID", "FSN", "SKU_ID", "Product_Title", "Gross_Sales", "Net_Profit_Before_COGS", "Total_COGS", "Final_Net_Profit", "Final_Profit_Margin", "COGS_Status", "Final_Action", "Last_Updated"]
    adjusted_cols = ["Run_ID", "FSN", "SKU_ID", "Product_Title", "Original_Final_Net_Profit", "Total_Adjustment_Additions", "Total_Adjustment_Deductions", "Net_Adjustment", "Adjusted_Final_Net_Profit", "Adjustment_Count", "Adjustment_Status", "Last_Updated"]
    render_dataframe_section(
        "FSN Profit Snapshot",
        fsn_filtered,
        "flipkart_profit_snapshot_filtered.csv",
        caption="The main FSN financial table with COGS and margin context.",
        preferred_columns=fsn_cols,
        style_columns={
            "Final_Profit_Margin": CONFIDENCE_PALETTE,
            "COGS_Status": {"missing": "#fee2e2", "entered": "#dcfce7", "available": "#dcfce7", "verified": "#dcfce7"},
        },
    )
    render_dataframe_section(
        "Adjusted Profit Ledger",
        profit_filtered,
        "flipkart_adjusted_profit_filtered.csv",
        caption="The adjustment-aware profit layer. Positive and negative monetary cells are highlighted automatically.",
        preferred_columns=adjusted_cols,
        style_columns={"Adjustment_Status": STATUS_PALETTE},
    )


def render_ads_planner(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    ads_df = dataframe_or_empty(frames[ADS_TAB])
    demand_df = dataframe_or_empty(frames[DEMAND_PROFILE_TAB])
    decision_col = resolve_column(ads_df, ["Final_Ads_Decision"])
    budget_col = resolve_column(ads_df, ["Final_Budget_Recommendation"])
    product_type_col = resolve_column(ads_df, ["Final_Product_Type", "Product_Type"])
    demand_status_col = resolve_column(demand_df, ["Cache_Status_Summary", "Demand_Confidence", "Next_45_Days_Demand_Status"])
    ads_filtered = apply_global_search(ads_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Final_Ads_Decision", "Final_Product_Type", "Ads_Risk_Level", "Ads_Opportunity_Level", "Final_Budget_Recommendation"])
    demand_filtered = apply_global_search(demand_df, search_filters, ["Product_Type", "Seasonality_Tag", "Demand_Source", "Cache_Status_Summary", "Remarks", "Next_45_Days_Demand_Status"])

    render_page_header(
        "Ads Planner",
        "Use the cached Google Keyword Planner layer and product demand profile without making live Ads API calls.",
        latest_non_blank_value(ads_df, ["Run_ID"]),
    )
    if demand_status_col and count_contains(demand_filtered, demand_status_col, "pending") > 0:
        render_warning_banner("Keyword cache pending: the demand profile still contains pending keyword cache rows, which is normal until Google Ads access is approved.")

    decision_values = unique_text_values(ads_filtered, decision_col) if decision_col else []
    product_values = unique_text_values(ads_filtered, product_type_col) if product_type_col else []
    budget_values = unique_text_values(ads_filtered, budget_col) if budget_col else []
    filter_cols = st.columns(3)
    with filter_cols[0]:
        decision_pick = st.multiselect("Ads decision", decision_values, default=decision_values, key="ads_decision_filter")
    with filter_cols[1]:
        product_pick = st.multiselect("Product type", product_values, default=product_values, key="ads_product_type_filter")
    with filter_cols[2]:
        budget_pick = st.multiselect("Budget recommendation", budget_values, default=budget_values, key="ads_budget_filter")
    if decision_col:
        ads_filtered = filter_by_selected_values(ads_filtered, decision_col, decision_pick)
    if product_type_col:
        ads_filtered = filter_by_selected_values(ads_filtered, product_type_col, product_pick)
    if budget_col:
        ads_filtered = filter_by_selected_values(ads_filtered, budget_col, budget_pick)

    ads_ready = 0
    if decision_col and not ads_filtered.empty:
        ads_ready = sum(
            count_matching_values(ads_filtered, decision_col, decision)
            for decision in ["Scale Ads", "Test Ads", "Always-On Test", "Continue / Optimize Ads"]
        )
    else:
        ads_ready = len(ads_filtered)

    render_metric_cards(
        [
            {"label": "Scale Ads", "value": f"{count_matching_values(ads_filtered, decision_col, 'Scale Ads') if decision_col else 0:,}", "note": "Higher confidence rows"},
            {"label": "Test Ads", "value": f"{count_matching_values(ads_filtered, decision_col, 'Test Ads') if decision_col else 0:,}", "note": "Controlled experiments"},
            {"label": "Do Not Run", "value": f"{count_matching_values(ads_filtered, decision_col, 'Do Not Run Ads / Improve Economics') if decision_col else 0:,}", "note": "Do not spend yet"},
            {"label": "Fill COGS First", "value": f"{count_matching_values(ads_filtered, decision_col, 'Fill COGS First') if decision_col else 0:,}", "note": "Wait on cost coverage"},
            {"label": "Ads Ready", "value": f"{ads_ready:,}", "note": "Safe to test or scale"},
        ],
        columns=5,
    )
    if decision_col and not ads_filtered.empty:
        st.markdown("### Ads Decision Mix")
        render_chart_from_counts(ads_filtered, decision_col, "Ads_Count")
    ads_cols = ["FSN", "SKU_ID", "Product_Title", "Final_Product_Type", "Final_Seasonality_Tag", "Ad_Run_Type", "Current_Ad_Status", "Ad_ROAS", "Ad_ACOS", "Ad_Revenue", "Estimated_Ad_Spend", "Final_Ads_Decision", "Final_Budget_Recommendation", "Ads_Risk_Level", "Ads_Opportunity_Level", "Ads_Decision_Reason", "Last_Updated"]
    demand_cols = ["Product_Type", "Seasonality_Tag", "Peak_Months", "Prep_Start_Days_Before_Peak", "Ads_Start_Days_Before_Peak", "Total_Avg_Monthly_Searches", "Demand_Stability", "Seasonality_Score", "Current_Month_Demand_Index", "Next_45_Days_Demand_Status", "Demand_Confidence", "Demand_Source", "Recommended_Ad_Window", "Keyword_Count", "Cache_Status_Summary", "Cache_Pending_Count", "Cache_Success_Count", "Cache_Failed_Count", "Cache_Last_Refreshed", "Remarks", "Last_Updated"]
    render_dataframe_section(
        "Ads Planner Table",
        ads_filtered,
        "flipkart_ads_planner_filtered.csv",
        caption="Colored by risk and decision so the safest actions are obvious.",
        preferred_columns=ads_cols,
        style_columns={"Ads_Risk_Level": RISK_PALETTE, "Ads_Opportunity_Level": CONFIDENCE_PALETTE, "Final_Ads_Decision": DECISION_PALETTE},
    )
    st.markdown("### Demand Source / Status")
    render_dataframe_section(
        "Demand Profile & Keyword Cache",
        demand_filtered,
        "flipkart_demand_profile_filtered.csv",
        caption="This page surfaces the cached keyword planning context and shows when the cache is still pending.",
        preferred_columns=demand_cols,
        style_columns={"Cache_Status_Summary": STATUS_PALETTE, "Demand_Confidence": CONFIDENCE_PALETTE},
    )


def render_returns_intelligence(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    customer_detail_df = dataframe_or_empty(frames[CUSTOMER_RETURN_COMMENTS_TAB])
    courier_detail_df = dataframe_or_empty(frames[COURIER_RETURN_COMMENTS_TAB])
    all_details_df = dataframe_or_empty(frames[RETURN_ALL_DETAILS_TAB])
    customer_summary_df = dataframe_or_empty(frames[CUSTOMER_RETURN_SUMMARY_TAB])
    courier_summary_df = dataframe_or_empty(frames[COURIER_RETURN_SUMMARY_TAB])
    pivot_df = dataframe_or_empty(frames[RETURN_TYPE_PIVOT_TAB])
    bucket_col = resolve_column(all_details_df, ["Return_Bucket"])
    if customer_detail_df.empty and not all_details_df.empty and bucket_col:
        customer_detail_df = all_details_df[all_details_df[bucket_col].fillna("").astype(str).map(normalize_text) == "customer_return"].copy()
    if courier_detail_df.empty and not all_details_df.empty and bucket_col:
        courier_detail_df = all_details_df[all_details_df[bucket_col].fillna("").astype(str).map(normalize_text) == "courier_return"].copy()
    return_type_focus = st.selectbox(
        "Return Type",
        ["All", "customer_return", "courier_return", "unknown_return"],
        index=0,
        key="returns_intelligence_return_type_focus",
    )
    all_details_filtered = apply_global_search(all_details_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Return_Reason", "Return_Sub_Reason", "Comments", "Order_ID", "Order_Item_ID", "Return_ID"])
    customer_filtered = apply_global_search(customer_summary_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Top_Customer_Return_Reason", "Top_Customer_Return_Sub_Reason", "Suggested_Action"])
    courier_filtered = apply_global_search(courier_summary_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Top_Courier_Return_Reason", "Top_Courier_Return_Sub_Reason", "Suggested_Action"])
    pivot_filtered = apply_global_search(pivot_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Customer_vs_Courier_Mix", "Dominant_Return_Type"])
    if return_type_focus != "All" and bucket_col:
        all_details_filtered = all_details_filtered[all_details_filtered[bucket_col].fillna("").astype(str).map(normalize_text) == return_type_focus].copy()
        pivot_bucket_col = resolve_column(pivot_filtered, ["Dominant_Return_Type"])
        if pivot_bucket_col:
            pivot_filtered = pivot_filtered[pivot_filtered[pivot_bucket_col].fillna("").astype(str).map(normalize_text) == return_type_focus].copy()
    render_page_header(
        "Returns Intelligence",
        "Customer returns drive product-quality and ads risk. Courier returns stay separate as logistics intelligence.",
        latest_non_blank_value(all_details_df, ["Run_ID"]),
    )
    customer_count = int(parse_float(latest_non_blank_value(customer_summary_df, ["Customer_Return_Count"]) or 0))
    courier_count = int(parse_float(latest_non_blank_value(courier_summary_df, ["Courier_Return_Count"]) or 0))
    customer_rate = latest_non_blank_value(customer_summary_df, ["Customer_Return_Rate"])
    courier_rate = latest_non_blank_value(courier_summary_df, ["Courier_Return_Rate"])
    total_return_count = int(parse_float((customer_count + courier_count) if (customer_count or courier_count) else latest_non_blank_value(all_details_df, ["Total_Return_Count"]) or 0))
    critical_customer_fsns = 0
    if not customer_summary_df.empty and "Customer_Return_Risk_Level" in customer_summary_df.columns:
        critical_customer_fsns = int((customer_summary_df["Customer_Return_Risk_Level"].fillna("").astype(str).map(normalize_text) == "Critical").sum())
    high_courier_fsns = 0
    if not courier_summary_df.empty and "Courier_Return_Risk_Level" in courier_summary_df.columns:
        high_courier_fsns = int((courier_summary_df["Courier_Return_Risk_Level"].fillna("").astype(str).map(normalize_text) == "High").sum())
    quality_issue_count = int(customer_summary_df["Quality_Issue_Count"].map(parse_float).sum()) if "Quality_Issue_Count" in customer_summary_df.columns else 0
    defective_count = int(customer_summary_df["Defective_Product_Count"].map(parse_float).sum()) if "Defective_Product_Count" in customer_summary_df.columns else 0
    damaged_count = int(customer_summary_df["Damaged_Product_Count"].map(parse_float).sum()) if "Damaged_Product_Count" in customer_summary_df.columns else 0
    missing_count = int(customer_summary_df["Missing_Item_Count"].map(parse_float).sum()) if "Missing_Item_Count" in customer_summary_df.columns else 0
    top_customer_reason = latest_non_blank_value(customer_summary_df, ["Top_Customer_Return_Reason"])
    top_courier_reason = latest_non_blank_value(courier_summary_df, ["Top_Courier_Return_Reason"])

    render_metric_cards(
        [
            {"label": "Customer Returns", "value": f"{customer_count:,}", "note": f"Rate {customer_rate or '-'}"},
            {"label": "Courier Returns", "value": f"{courier_count:,}", "note": f"Rate {courier_rate or '-'}"},
            {"label": "Total Returns", "value": f"{total_return_count:,}", "note": "Customer + courier"},
            {"label": "Critical Customer FSNs", "value": f"{critical_customer_fsns:,}", "note": "Product-quality risk"},
            {"label": "High Courier FSNs", "value": f"{high_courier_fsns:,}", "note": "Operational/logistics risk"},
            {"label": "Quality Issue Count", "value": f"{quality_issue_count:,}", "note": f"Defective {defective_count:,} | Damaged {damaged_count:,} | Missing {missing_count:,}"},
        ],
        columns=3,
    )

    section_cols = st.columns(3)
    with section_cols[0]:
        st.markdown("### Customer Returns")
        if not customer_summary_df.empty:
            render_dataframe_section(
                "Customer Return Summary",
                customer_filtered,
                "flipkart_customer_return_issue_summary_filtered.csv",
                preferred_columns=[
                    "FSN",
                    "SKU_ID",
                    "Product_Title",
                    "Sold_Order_Items",
                    "Customer_Return_Count",
                    "Customer_Return_Rate",
                    "Quality_Issue_Count",
                    "Defective_Product_Count",
                    "Damaged_Product_Count",
                    "Missing_Item_Count",
                    "Wrong_Product_Count",
                    "Customer_Remorse_Count",
                    "Top_Customer_Return_Reason",
                    "Top_Customer_Return_Sub_Reason",
                    "Customer_Return_Risk_Level",
                    "Suggested_Action",
                    "Data_Gap_Reason",
                    "Last_Updated",
                ],
                style_columns={"Customer_Return_Risk_Level": RISK_PALETTE, "Suggested_Action": DECISION_PALETTE},
            )
    with section_cols[1]:
        st.markdown("### Courier Returns")
        if not courier_summary_df.empty:
            render_dataframe_section(
                "Courier Return Summary",
                courier_filtered,
                "flipkart_courier_return_summary_filtered.csv",
                preferred_columns=[
                    "FSN",
                    "SKU_ID",
                    "Product_Title",
                    "Sold_Order_Items",
                    "Courier_Return_Count",
                    "Courier_Return_Rate",
                    "Order_Cancelled_Count",
                    "Attempts_Exhausted_Count",
                    "Shipment_Ageing_Count",
                    "Not_Serviceable_Count",
                    "ORC_Validated_Count",
                    "Delivery_Failed_Count",
                    "Top_Courier_Return_Reason",
                    "Top_Courier_Return_Sub_Reason",
                    "Courier_Return_Risk_Level",
                    "Suggested_Action",
                    "Data_Gap_Reason",
                    "Last_Updated",
                ],
                style_columns={"Courier_Return_Risk_Level": RISK_PALETTE, "Suggested_Action": DECISION_PALETTE},
            )
    with section_cols[2]:
        st.markdown("### Return Type Mix")
        if not pivot_df.empty:
            render_dataframe_section(
                "Return Type Pivot",
                pivot_filtered,
                "flipkart_return_type_pivot_filtered.csv",
                preferred_columns=[
                    "FSN",
                    "SKU_ID",
                    "Product_Title",
                    "Sold_Order_Items",
                    "Customer_Return_Count",
                    "Courier_Return_Count",
                    "Unknown_Return_Count",
                    "Total_Return_Count",
                    "Customer_Return_Rate",
                    "Courier_Return_Rate",
                    "Total_Return_Rate",
                    "Customer_vs_Courier_Mix",
                    "Dominant_Return_Type",
                    "Last_Updated",
                ],
                style_columns={"Customer_vs_Courier_Mix": STATUS_PALETTE, "Dominant_Return_Type": STATUS_PALETTE},
            )

    if not all_details_filtered.empty:
        st.markdown("### Return Detail Copy View")
        render_dataframe_section(
            "All Return Details",
            all_details_filtered,
            "flipkart_return_all_details_filtered.csv",
            preferred_columns=[
                "Run_ID",
                "Return_ID",
                "Order_ID",
                "Order_Item_ID",
                "Return_Type",
                "Return_Bucket",
                "FSN",
                "SKU_ID",
                "Product_Title",
                "Return_Reason",
                "Return_Sub_Reason",
                "Comments",
                "Customer_Issue_Category",
                "Courier_Issue_Category",
                "Return_Status",
                "Return_Result",
                "Suggested_Action",
                "Source_File",
                "Last_Updated",
            ],
            style_columns={"Return_Type": STATUS_PALETTE, "Return_Bucket": STATUS_PALETTE},
        )


def render_return_comments_explorer(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    detail_df = dataframe_or_empty(frames[RETURN_ALL_DETAILS_TAB])
    if detail_df.empty:
        detail_df = dataframe_or_empty(frames[RETURN_COMMENTS_TAB])
    render_page_header(
        "Return Comments Explorer",
        "Search customer and courier return rows by FSN, SKU, order, return ID, and free-text notes.",
        latest_non_blank_value(detail_df, ["Run_ID"]),
    )
    if detail_df.empty:
        st.info("No return comment rows are available yet.")
        return
    fsn_col = resolve_column(detail_df, ["FSN"])
    sku_col = resolve_column(detail_df, ["SKU_ID"])
    order_col = resolve_column(detail_df, ["Order_ID", "Order Item ID", "Order_Item_ID"])
    order_item_col = resolve_column(detail_df, ["Order_Item_ID"])
    return_col = resolve_column(detail_df, ["Return_ID"])
    return_type_col = resolve_column(detail_df, ["Return_Type"])
    customer_issue_col = resolve_column(detail_df, ["Customer_Issue_Category"])
    courier_issue_col = resolve_column(detail_df, ["Courier_Issue_Category"])
    reason_col = resolve_column(detail_df, ["Return_Reason"])
    sub_reason_col = resolve_column(detail_df, ["Return_Sub_Reason"])
    comments_col = resolve_column(detail_df, ["Comments"])

    filter_cols = st.columns(4)
    with filter_cols[0]:
        fsn_search = st.text_input("FSN search", value=search_filters.get("fsn", ""), key="return_comments_fsn_search")
    with filter_cols[1]:
        sku_search = st.text_input("SKU search", value=search_filters.get("sku", ""), key="return_comments_sku_search")
    with filter_cols[2]:
        order_search = st.text_input("Order / Order Item ID search", value="", key="return_comments_order_search")
    with filter_cols[3]:
        comment_search = st.text_input("Comment / reason text", value=search_filters.get("product", ""), key="return_comments_text_search")

    filtered = detail_df.copy()
    if fsn_search and fsn_col:
        filtered = filter_by_query(filtered, fsn_search, [fsn_col])
    if sku_search and sku_col:
        filtered = filter_by_query(filtered, sku_search, [sku_col])
    if order_search:
        filtered = filter_by_query(filtered, order_search, [column for column in [order_col, order_item_col, return_col] if column])
    if comment_search:
        filtered = filter_by_query(filtered, comment_search, [column for column in [comments_col, reason_col, sub_reason_col] if column])

    return_type_values = unique_text_values(filtered, return_type_col) if return_type_col else []
    customer_issue_values = unique_text_values(filtered, customer_issue_col) if customer_issue_col else []
    courier_issue_values = unique_text_values(filtered, courier_issue_col) if courier_issue_col else []
    filter_cols_2 = st.columns(3)
    with filter_cols_2[0]:
        return_type_pick = st.multiselect("Return type", return_type_values or ["customer_return", "courier_return", "unknown_return"], default=return_type_values or ["customer_return", "courier_return", "unknown_return"], key="return_comments_type_filter")
    with filter_cols_2[1]:
        customer_issue_pick = st.multiselect("Customer issue category", customer_issue_values, default=customer_issue_values, key="return_comments_customer_issue_filter")
    with filter_cols_2[2]:
        courier_issue_pick = st.multiselect("Courier issue category", courier_issue_values, default=courier_issue_values, key="return_comments_courier_issue_filter")
    if return_type_col:
        filtered = filter_by_selected_values(filtered, return_type_col, return_type_pick)
    if customer_issue_col:
        filtered = filter_by_selected_values(filtered, customer_issue_col, customer_issue_pick)
    if courier_issue_col:
        filtered = filter_by_selected_values(filtered, courier_issue_col, courier_issue_pick)

    render_metric_cards(
        [
            {"label": "Filtered Rows", "value": f"{len(filtered):,}", "note": "Return rows in view"},
            {"label": "Unique FSNs", "value": f"{len(build_fsn_index(filtered)):,}", "note": "Return-linked FSNs"},
            {"label": "Copy-Friendly IDs", "value": f"{count_unique_non_blank(filtered, return_col) + count_unique_non_blank(filtered, order_col):,}", "note": "Return / order lookup"},
        ],
        columns=3,
    )
    copy_order_item_ids = "\n".join(sorted({normalize_text(value) for value in filtered[order_item_col].fillna("").astype(str).tolist() if normalize_text(value)})) if order_item_col and not filtered.empty else ""
    copy_order_ids = "\n".join(sorted({normalize_text(value) for value in filtered[order_col].fillna("").astype(str).tolist() if normalize_text(value)})) if order_col and not filtered.empty else ""
    copy_cols = st.columns(2)
    with copy_cols[0]:
        st.text_area("Copy Order_Item_ID list", value=copy_order_item_ids, height=180, key="return_comments_copy_order_item_ids")
    with copy_cols[1]:
        st.text_area("Copy Order_ID list", value=copy_order_ids, height=180, key="return_comments_copy_order_ids")
    render_dataframe_section(
        "Return Comments Detail",
        filtered,
        "flipkart_return_comments_filtered.csv",
        preferred_columns=[
            column
            for column in [
                "Order_ID",
                "Order_Item_ID",
                "Return_ID",
                "Return_Type",
                "FSN",
                "SKU_ID",
                "Product_Title",
                "Return_Reason",
                "Return_Sub_Reason",
                "Comments",
                "Customer_Issue_Category",
                "Courier_Issue_Category",
                "Suggested_Action",
                "Source_File",
                "Last_Updated",
            ]
            if column in filtered.columns
        ],
        style_columns={"Return_Type": STATUS_PALETTE, "Customer_Issue_Category": CUSTOMER_RETURN_CATEGORY_PALETTE, "Courier_Issue_Category": COURIER_RETURN_CATEGORY_PALETTE, "Suggested_Action": DECISION_PALETTE},
    )


def render_order_item_explorer(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    order_df, source_tab = first_available_frame(
        frames,
        [LOOKER_ORDER_ITEM_MASTER_TAB, ORDER_ITEM_MASTER_TAB, LOOKER_ORDER_ITEM_EXPLORER_TAB, ORDER_ITEM_EXPLORER_TAB],
    )
    source_detail_df, source_detail_tab = first_available_frame(
        frames,
        [LOOKER_ORDER_ITEM_SOURCE_DETAIL_TAB, ORDER_ITEM_SOURCE_DETAIL_TAB],
    )
    render_page_header(
        "Order ID Explorer",
        "Master view first, with source detail below for audit. Use the master table for daily work and the source detail only when you need to trace a value back to its report row.",
        latest_non_blank_value(order_df, ["Run_ID"]),
    )
    if order_df.empty:
        st.info("No order-item master rows are available yet.")
        return

    order_id_col = resolve_column(order_df, ["Order_ID", "Order ID"])
    order_item_col = resolve_column(order_df, ["Order_Item_ID", "Order Item ID"])
    fsn_col = resolve_column(order_df, ["FSN"])
    sku_col = resolve_column(order_df, ["SKU_ID", "Seller_SKU"])
    title_col = resolve_column(order_df, ["Product_Title", "Product Name", "Title"])
    return_type_col = resolve_column(order_df, ["Return_Type_Final", "Return_Type"])
    customer_return_col = resolve_column(order_df, ["Customer_Return_YN"])
    courier_return_col = resolve_column(order_df, ["Courier_Return_YN"])
    decision_col = resolve_column(order_df, ["Final_Ads_Decision"])
    risk_col = resolve_column(order_df, ["Competition_Risk_Level"])
    completeness_col = resolve_column(order_df, ["Data_Completeness_Status"])

    st.caption(f"Primary source tab: `{source_tab}` | Audit tab: `{source_detail_tab}`")

    search_row_1 = st.columns(3)
    with search_row_1[0]:
        order_id_search = st.text_input("Search Order ID", value="", key="order_item_order_id_search", placeholder="Type an Order ID")
    with search_row_1[1]:
        order_item_search = st.text_input("Search Order Item ID", value="", key="order_item_order_item_search", placeholder="Type an Order Item ID")
    with search_row_1[2]:
        fsn_search = st.text_input("Search FSN", value=search_filters.get("fsn", ""), key="order_item_fsn_search", placeholder="Type an FSN")

    search_row_2 = st.columns(2)
    with search_row_2[0]:
        sku_search = st.text_input("Search SKU_ID", value=search_filters.get("sku", ""), key="order_item_sku_search", placeholder="Type a SKU ID")
    with search_row_2[1]:
        title_search = st.text_input("Search Product Title", value=search_filters.get("product", ""), key="order_item_title_search", placeholder="Type a product title")

    filtered = order_df.copy()
    if order_id_search and order_id_col:
        filtered = filter_by_query(filtered, order_id_search, [order_id_col])
    if order_item_search and order_item_col:
        filtered = filter_by_query(filtered, order_item_search, [order_item_col])
    if fsn_search and fsn_col:
        filtered = filter_by_query(filtered, fsn_search, [fsn_col])
    if sku_search and sku_col:
        filtered = filter_by_query(filtered, sku_search, [sku_col])
    if title_search and title_col:
        filtered = filter_by_query(filtered, title_search, [title_col])

    filter_row_1 = st.columns(3)
    return_type_values = unique_text_values(filtered, return_type_col) if return_type_col else []
    customer_return_values = unique_text_values(filtered, customer_return_col) if customer_return_col else []
    courier_return_values = unique_text_values(filtered, courier_return_col) if courier_return_col else []
    with filter_row_1[0]:
        return_type_pick = st.multiselect("Return_Type_Final", return_type_values or ["customer_return", "courier_return", "unknown_return"], default=return_type_values or ["customer_return", "courier_return", "unknown_return"], key="order_item_return_type_filter")
    with filter_row_1[1]:
        customer_return_pick = st.multiselect("Customer_Return_YN", customer_return_values or ["Yes", "No"], default=customer_return_values or ["Yes", "No"], key="order_item_customer_return_filter")
    with filter_row_1[2]:
        courier_return_pick = st.multiselect("Courier_Return_YN", courier_return_values or ["Yes", "No"], default=courier_return_values or ["Yes", "No"], key="order_item_courier_return_filter")

    filter_row_2 = st.columns(3)
    decision_values = unique_text_values(filtered, decision_col) if decision_col else []
    risk_values = unique_text_values(filtered, risk_col) if risk_col else []
    completeness_values = unique_text_values(filtered, completeness_col) if completeness_col else []
    with filter_row_2[0]:
        decision_pick = st.multiselect("Final_Ads_Decision", decision_values, default=decision_values, key="order_item_decision_filter")
    with filter_row_2[1]:
        risk_pick = st.multiselect("Competition_Risk_Level", risk_values, default=risk_values, key="order_item_risk_filter")
    with filter_row_2[2]:
        completeness_pick = st.multiselect("Data_Completeness_Status", completeness_values, default=completeness_values, key="order_item_completeness_filter")

    if return_type_col:
        filtered = filter_by_selected_values(filtered, return_type_col, return_type_pick)
    if customer_return_col:
        filtered = filter_by_selected_values(filtered, customer_return_col, customer_return_pick)
    if courier_return_col:
        filtered = filter_by_selected_values(filtered, courier_return_col, courier_return_pick)
    if decision_col:
        filtered = filter_by_selected_values(filtered, decision_col, decision_pick)
    if risk_col:
        filtered = filter_by_selected_values(filtered, risk_col, risk_pick)
    if completeness_col:
        filtered = filter_by_selected_values(filtered, completeness_col, completeness_pick)

    order_id_count = count_unique_non_blank(filtered, order_id_col) if order_id_col else 0
    order_item_count = count_unique_non_blank(filtered, order_item_col) if order_item_col else 0
    returned_rows = 0
    if not filtered.empty:
        status_match = pd.Series(False, index=filtered.index)
        if return_type_col:
            status_match = status_match | (filtered[return_type_col].fillna("").astype(str).map(normalize_text) != "")
        if customer_return_col:
            status_match = status_match | (filtered[customer_return_col].fillna("").astype(str).map(normalize_text).str.lower() == "yes")
        if courier_return_col:
            status_match = status_match | (filtered[courier_return_col].fillna("").astype(str).map(normalize_text).str.lower() == "yes")
        returned_rows = int(status_match.sum())
    missing_fsn_count = int((filtered[fsn_col].fillna("").astype(str).map(normalize_text) == "").sum()) if fsn_col and not filtered.empty else 0

    render_metric_cards(
        [
            {"label": "Master Rows", "value": f"{len(filtered):,}", "note": "Current master view"},
            {"label": "Unique Order IDs", "value": f"{order_id_count:,}", "note": "Distinct Order_ID values"},
            {"label": "Unique Order Item IDs", "value": f"{order_item_count:,}", "note": "Distinct Order_Item_ID values"},
            {"label": "Returned Order Items", "value": f"{returned_rows:,}", "note": "Rows with return signals"},
            {"label": "Rows Missing FSN", "value": f"{missing_fsn_count:,}", "note": "Needs source follow-up"},
        ],
        columns=5,
    )

    master_preferred_columns = [
        "Order_ID",
        "Order_Item_ID",
        "FSN",
        "SKU_ID",
        "Product_Title",
        "Order_Date",
        "Selling_Price",
        "Net_Profit",
        "Return_YN",
        "Return_Type_Final",
        "Return_Reason_Final",
        "Customer_Issue_Category",
        "Courier_Issue_Category",
        "Final_Ads_Decision",
        "Competition_Risk_Level",
        "Data_Completeness_Status",
    ]
    render_dataframe_section(
        "Order Item Master",
        filtered,
        "flipkart_order_item_master_filtered.csv",
        caption="Copy from this table for daily team work.",
        preferred_columns=[column for column in master_preferred_columns if column in filtered.columns],
        style_columns={
            "Return_YN": STATUS_PALETTE,
            "Customer_Return_YN": STATUS_PALETTE,
            "Courier_Return_YN": STATUS_PALETTE,
            "Customer_Issue_Category": CUSTOMER_RETURN_CATEGORY_PALETTE,
            "Courier_Issue_Category": COURIER_RETURN_CATEGORY_PALETTE,
            "Competition_Risk_Level": RISK_PALETTE,
            "Final_Ads_Decision": DECISION_PALETTE,
        },
    )

    if not filtered.empty:
        order_item_values = "\n".join(dict.fromkeys(value for value in filtered[order_item_col].fillna("").astype(str).tolist() if normalize_text(value))) if order_item_col else ""
        order_values = "\n".join(dict.fromkeys(value for value in filtered[order_id_col].fillna("").astype(str).tolist() if normalize_text(value))) if order_id_col else ""
        fsn_values = "\n".join(dict.fromkeys(value for value in filtered[fsn_col].fillna("").astype(str).tolist() if normalize_text(value))) if fsn_col else ""
        copy_cols = st.columns(3)
        with copy_cols[0]:
            st.text_area("Filtered Order_Item_ID list", value=order_item_values, height=180, help="Copy this list into Flipkart checks.", key="order_item_copy_list")
        with copy_cols[1]:
            st.text_area("Filtered Order_ID list", value=order_values, height=180, help="Copy this list into Flipkart checks.", key="order_copy_list")
        with copy_cols[2]:
            st.text_area("Filtered FSN list", value=fsn_values, height=180, help="Optional FSN copy list for the same filtered rows.", key="order_fsn_copy_list")

    with st.expander("Source detail audit", expanded=False):
        source_filtered = source_detail_df.copy()
        selected_order_ids = {normalize_text(value) for value in filtered[order_id_col].fillna("").astype(str).tolist() if order_id_col and normalize_text(value)} if order_id_col else set()
        selected_order_item_ids = {normalize_text(value) for value in filtered[order_item_col].fillna("").astype(str).tolist() if order_item_col and normalize_text(value)} if order_item_col else set()
        selected_fsns = {clean_fsn(value) for value in filtered[fsn_col].fillna("").astype(str).tolist() if fsn_col and clean_fsn(value)} if fsn_col else set()
        if not source_filtered.empty:
            if "Order_ID" in source_filtered.columns and selected_order_ids:
                source_filtered = source_filtered[source_filtered["Order_ID"].fillna("").astype(str).map(normalize_text).isin(selected_order_ids)].copy()
            if "Order_Item_ID" in source_filtered.columns and selected_order_item_ids:
                source_filtered = pd.concat(
                    [
                        source_filtered,
                        source_detail_df[source_detail_df["Order_Item_ID"].fillna("").astype(str).map(normalize_text).isin(selected_order_item_ids)].copy()
                    ],
                    ignore_index=True,
                ).drop_duplicates()
            if "FSN" in source_filtered.columns and selected_fsns:
                source_filtered = pd.concat(
                    [
                        source_filtered,
                        source_detail_df[source_detail_df["FSN"].fillna("").astype(str).map(clean_fsn).isin(selected_fsns)].copy()
                    ],
                    ignore_index=True,
                ).drop_duplicates()
        if source_filtered.empty:
            st.info("No source detail rows matched the current filtered master rows.")
        else:
            render_dataframe_section(
                "Order Item Source Detail",
                source_filtered,
                "flipkart_order_item_source_detail_filtered.csv",
                caption="Use this only to trace why a master value exists or differs across reports.",
                preferred_columns=[
                    column
                    for column in [
                        "Source_File",
                        "Source_Tab",
                        "Source_Row_Type",
                        "Order_ID",
                        "Order_Item_ID",
                        "Return_ID",
                        "FSN",
                        "SKU_ID",
                        "Product_Title",
                        "Order_Date",
                        "Settlement_Date",
                        "Return_Date",
                        "Quantity",
                        "Selling_Price",
                        "Settlement_Amount",
                        "Net_Profit",
                        "Return_Type",
                        "Customer_Return_YN",
                        "Courier_Return_YN",
                        "Return_Status",
                        "Return_Reason",
                        "Return_Sub_Reason",
                        "Customer_Issue_Category",
                        "Courier_Issue_Category",
                        "Alert_Count",
                        "Critical_Alert_Count",
                        "Final_Ads_Decision",
                        "Competition_Risk_Level",
                        "Data_Gap_Reason",
                        "Last_Updated",
                    ]
                    if column in source_filtered.columns
                ],
                style_columns={
                    "Source_Row_Type": STATUS_PALETTE,
                    "Return_Type": STATUS_PALETTE,
                    "Customer_Return_YN": STATUS_PALETTE,
                    "Courier_Return_YN": STATUS_PALETTE,
                    "Customer_Issue_Category": CUSTOMER_RETURN_CATEGORY_PALETTE,
                    "Courier_Issue_Category": COURIER_RETURN_CATEGORY_PALETTE,
                    "Competition_Risk_Level": RISK_PALETTE,
                    "Final_Ads_Decision": DECISION_PALETTE,
                },
            )

    with st.expander("Legacy compatibility view", expanded=False):
        legacy_df = dataframe_or_empty(frames.get(ORDER_ITEM_EXPLORER_TAB, pd.DataFrame()))
        if legacy_df.empty:
            legacy_df = dataframe_or_empty(frames.get(LOOKER_ORDER_ITEM_EXPLORER_TAB, pd.DataFrame()))
        if legacy_df.empty:
            st.info("Legacy explorer rows are not loaded.")
        else:
            render_dataframe_section(
                "Legacy Order Item Explorer",
                legacy_df,
                "flipkart_order_item_explorer_legacy_filtered.csv",
                preferred_columns=[column for column in ["Order_ID", "Order_Item_ID", "FSN", "SKU_ID", "Product_Title", "Return_Type", "Customer_Return_YN", "Courier_Return_YN", "Net_Profit", "Final_Ads_Decision", "Competition_Risk_Level", "Data_Gap_Reason", "Last_Updated"] if column in legacy_df.columns],
            )
def render_listing_issues(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    listings_df = dataframe_or_empty(frames[LISTINGS_TAB])
    missing_df = dataframe_or_empty(frames[MISSING_ACTIVE_LISTINGS_TAB])
    listings_filtered = apply_global_search(listings_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Listing_Presence_Status", "Possible_Issue", "Suggested_Action", "Priority"])
    missing_filtered = apply_global_search(missing_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Suggested_Action", "Priority", "Status"])
    render_page_header(
        "Listing Issues",
        "Review missing active listings and other listing presence problems without editing the live sheet.",
        latest_non_blank_value(listings_df, ["Run_ID"]),
    )
    priority_col = resolve_column(missing_filtered, ["Priority"])
    status_col = resolve_column(missing_filtered, ["Status", "Listing_Presence_Status"])
    priority_values = unique_text_values(missing_filtered, priority_col) if priority_col else []
    status_values = unique_text_values(missing_filtered, status_col) if status_col else []
    filter_cols = st.columns(2)
    with filter_cols[0]:
        priority_pick = st.multiselect("Priority", priority_values, default=priority_values, key="listing_priority_filter")
    with filter_cols[1]:
        status_pick = st.multiselect("Status", status_values, default=status_values, key="listing_status_filter")
    if priority_col:
        missing_filtered = filter_by_selected_values(missing_filtered, priority_col, priority_pick)
    if status_col:
        missing_filtered = filter_by_selected_values(missing_filtered, status_col, status_pick)

    missing_count = len(build_fsn_index(missing_filtered)) if not missing_filtered.empty else 0
    render_metric_cards(
        [
            {"label": "Missing Active Listings", "value": f"{missing_count:,}", "note": "FSNs not found in the active listing file"},
            {"label": "Listing Source Rows", "value": f"{len(listings_filtered):,}", "note": "General listing source"},
            {"label": "Priority Filters", "value": f"{len(priority_pick):,}", "note": "Current priority selection"},
        ],
        columns=3,
    )
    if priority_col and not missing_filtered.empty:
        st.markdown("### Priority Distribution")
        render_chart_from_counts(missing_filtered, priority_col, "Listing_Count")
    render_dataframe_section(
        "Missing Active Listings",
        missing_filtered,
        "flipkart_missing_active_listings_filtered.csv",
        preferred_columns=[column for column in ["FSN", "SKU_ID", "Product_Title", "Priority", "Status", "Suggested_Action", "Owner", "Remarks", "Last_Updated"] if column in missing_filtered.columns],
        style_columns={"Priority": SEVERITY_PALETTE, "Status": STATUS_PALETTE, "Suggested_Action": DECISION_PALETTE},
    )
    with st.expander("Listing source table", expanded=False):
        render_dataframe_section(
            "Listing Source",
            listings_filtered,
            "flipkart_listings_filtered.csv",
            preferred_columns=[column for column in ["FSN", "SKU_ID", "Product_Title", "Listing_Presence_Status", "Possible_Issue", "Suggested_Action", "Priority", "Last_Updated"] if column in listings_filtered.columns],
            style_columns={"Listing_Presence_Status": STATUS_PALETTE, "Priority": SEVERITY_PALETTE},
        )


def render_run_history_comparison(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    run_df = dataframe_or_empty(frames[RUN_COMPARISON_TAB])
    fsn_df = dataframe_or_empty(frames[FSN_RUN_COMPARISON_TAB])
    run_filtered = apply_global_search(run_df, search_filters, ["Run_ID", "Previous_Run_ID", "Comparison_Status", "Change_Type", "Reason"])
    fsn_filtered = apply_global_search(fsn_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Comparison_Status", "Change_Type", "Reason"])
    render_page_header(
        "Run History & Comparison",
        "Compare the latest Flipkart run against the previous run and spot FSNs that got worse or improved.",
        latest_non_blank_value(run_df, ["Run_ID"]),
    )
    status_col = resolve_column(run_filtered, ["Comparison_Status", "Run_Status"])
    change_col = resolve_column(fsn_filtered, ["Change_Type"])
    status_values = unique_text_values(run_filtered, status_col) if status_col else []
    change_values = unique_text_values(fsn_filtered, change_col) if change_col else []
    filter_cols = st.columns(2)
    with filter_cols[0]:
        status_pick = st.multiselect("Comparison status", status_values, default=status_values, key="run_history_status_filter")
    with filter_cols[1]:
        change_pick = st.multiselect("Change type", change_values, default=change_values, key="run_history_change_filter")
    if status_col:
        run_filtered = filter_by_selected_values(run_filtered, status_col, status_pick)
    if change_col:
        fsn_filtered = filter_by_selected_values(fsn_filtered, change_col, change_pick)

    improved_count = count_matching_values(run_filtered, status_col, "Improved") if status_col else 0
    worsened_count = count_matching_values(run_filtered, status_col, "Worsened") if status_col else 0
    no_change_count = count_matching_values(run_filtered, status_col, "No Change") if status_col else 0
    render_metric_cards(
        [
            {"label": "Improved", "value": f"{improved_count:,}", "note": "Improved vs previous run"},
            {"label": "Worsened", "value": f"{worsened_count:,}", "note": "Needs attention"},
            {"label": "No Change", "value": f"{no_change_count:,}", "note": "Stable rows"},
            {"label": "Comparison Rows", "value": f"{len(run_filtered):,}", "note": "Run-level comparison"},
            {"label": "FSN Rows", "value": f"{len(fsn_filtered):,}", "note": "FSN-level comparison"},
        ],
        columns=5,
    )
    render_dataframe_section(
        "Latest vs Previous Run Summary",
        run_filtered,
        "flipkart_run_comparison_filtered.csv",
        preferred_columns=[column for column in ["Run_ID", "Previous_Run_ID", "Comparison_Status", "Improved_Count", "Worsened_Count", "No_Change_Count", "New_Count", "Reason", "Last_Updated"] if column in run_filtered.columns],
        style_columns={"Comparison_Status": COMPARISON_PALETTE},
    )
    render_dataframe_section(
        "FSN-Level Comparison",
        fsn_filtered,
        "flipkart_fsn_run_comparison_filtered.csv",
        preferred_columns=[column for column in ["Run_ID", "Previous_Run_ID", "FSN", "SKU_ID", "Product_Title", "Comparison_Status", "Change_Type", "Current_Value", "Previous_Value", "Delta_Value", "Reason", "Last_Updated"] if column in fsn_filtered.columns],
        style_columns={"Comparison_Status": COMPARISON_PALETTE, "Change_Type": COMPARISON_PALETTE},
    )


def render_raw_data_explorer(frames: Dict[str, pd.DataFrame], data: Dict[str, Any]) -> None:
    render_page_header(
        "Raw Data Explorer / Downloads",
        "Power-user view for inspecting any loaded source tab and downloading filtered CSVs.",
        latest_non_blank_value(dataframe_or_empty(frames.get(EXECUTIVE_TAB, pd.DataFrame())), ["Run_ID"]),
    )
    available_tabs = data.get("available_tabs", [])
    if not available_tabs:
        st.info("No source tables are currently available.")
        return
    selected_tab = st.selectbox("Source table", available_tabs, index=0)
    selected_df = dataframe_or_empty(frames.get(selected_tab, pd.DataFrame()))
    if selected_df.empty:
        st.warning(f"Tab `{selected_tab}` is missing or empty.")
        return
    search_text = st.text_input("Search rows", value="", key="raw_data_search")
    filtered = filter_by_query(selected_df, search_text, list(selected_df.columns)) if search_text else selected_df.copy()
    st.caption(f"Rows: {len(filtered):,} | Columns: {len(filtered.columns):,}")
    render_dataframe_section(
        selected_tab,
        filtered,
        f"{normalize_key(selected_tab)}_filtered.csv",
        preferred_columns=[column for column in filtered.columns],
    )


def render_competitor_risk(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    competitor_df = dataframe_or_empty(frames[COMPETITOR_TAB])
    visual_df = dataframe_or_empty(frames[VISUAL_COMPETITOR_RESULTS_TAB])
    risk_col = resolve_column(competitor_df, ["Competition_Risk_Level"])
    action_col = resolve_column(competitor_df, ["Suggested_Action"])
    competitor_filtered = apply_global_search(competitor_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Competition_Risk_Level", "Competitor_Insight", "Suggested_Action", "Price_Gap_Percent"])
    render_page_header(
        "Competitor Risk",
        "Flipkart-only competitor intelligence with explicit Not Enough Data warnings for rows that still need image or search context.",
        latest_non_blank_value(competitor_df, ["Run_ID"]),
    )
    risk_values = unique_text_values(competitor_filtered, risk_col) if risk_col else []
    action_values = unique_text_values(competitor_filtered, action_col) if action_col else []
    filter_cols = st.columns(2)
    with filter_cols[0]:
        selected_risks = st.multiselect("Competition risk", risk_values, default=risk_values, key="competitor_risk_filter")
    with filter_cols[1]:
        selected_actions = st.multiselect("Suggested action", action_values, default=action_values, key="competitor_action_filter")
    if risk_col:
        competitor_filtered = filter_by_selected_values(competitor_filtered, risk_col, selected_risks)
    if action_col:
        competitor_filtered = filter_by_selected_values(competitor_filtered, action_col, selected_actions)

    not_enough_data_count = count_matching_values(competitor_filtered, risk_col, "Not Enough Data") if risk_col else 0
    if not_enough_data_count > 0:
        render_warning_banner(
            f"Competitor Not Enough Data warning: {not_enough_data_count} filtered row(s) still do not have enough comparable competitor context."
        )
    render_metric_cards(
        [
            {"label": "Critical Risk", "value": f"{count_matching_values(competitor_filtered, risk_col, 'Critical') if risk_col else 0:,}", "note": "Highest risk"},
            {"label": "High Risk", "value": f"{count_matching_values(competitor_filtered, risk_col, 'High') if risk_col else 0:,}", "note": "Needs attention"},
            {"label": "Medium Risk", "value": f"{count_matching_values(competitor_filtered, risk_col, 'Medium') if risk_col else 0:,}", "note": "Watch closely"},
            {"label": "Not Enough Data", "value": f"{not_enough_data_count:,}", "note": "Missing comparable signals"},
            {"label": "Competitor Rows", "value": f"{len(competitor_filtered):,}", "note": "Current search and risk filters"},
        ],
        columns=5,
    )
    if risk_col and not competitor_filtered.empty:
        st.markdown("### Competition Risk Mix")
        render_chart_from_counts(competitor_filtered, risk_col, "Risk_Count")
    competitor_cols = ["FSN", "Product_Title", "Our_Unit_Price", "Median_Comparable_Competitor_Unit_Price", "Price_Gap_Percent", "Competition_Risk_Level", "Competitor_Insight", "Suggested_Action", "Confidence", "Last_Updated"]
    render_dataframe_section(
        "Competitor Intelligence",
        competitor_filtered,
        "flipkart_competitor_intelligence_filtered.csv",
        caption="Rows are color-coded by risk level and action recommendation.",
        preferred_columns=competitor_cols,
        style_columns={"Competition_Risk_Level": RISK_PALETTE, "Suggested_Action": DECISION_PALETTE, "Confidence": CONFIDENCE_PALETTE},
    )
    with st.expander("Raw visual competitor results", expanded=False):
        render_dataframe_section(
            "Visual Competitor Results",
            visual_df,
            "flipkart_visual_competitor_results.csv",
            caption="Unfiltered raw visual competitor search output for power users.",
            preferred_columns=[column for column in ["FSN", "Product_Title", "Competitor_Url", "Competitor_Title", "Comparable_Competitor_Count", "Price_Gap_Percent", "Competition_Risk_Level", "Suggested_Action", "Last_Updated"] if column in visual_df.columns],
            style_columns={"Competition_Risk_Level": RISK_PALETTE, "Suggested_Action": DECISION_PALETTE},
        )


def render_data_quality(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    run_quality_df = dataframe_or_empty(frames[RUN_QUALITY_TAB])
    module_confidence_df = dataframe_or_empty(frames[MODULE_CONFIDENCE_TAB])
    report_format_df = dataframe_or_empty(frames[REPORT_FORMAT_TAB])
    run_quality_filtered = apply_global_search(run_quality_df, search_filters, ["Run_ID", "Score_Category", "Score_Name", "Reason", "Suggested_Action"])
    module_confidence_filtered = apply_global_search(module_confidence_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Overall_Confidence_Status", "Primary_Data_Gap", "Suggested_Data_Action", "Overall_Confidence_Score"])
    report_format_filtered = apply_global_search(report_format_df, search_filters, ["File_Name", "Detected_Report_Type", "Sheet_Name", "Header_Row_Index", "Header_Detection_Status", "Required_Business_Headers_Present"])
    summary_rows = run_quality_filtered[run_quality_filtered.get("Record_Type", pd.Series(dtype="object")).fillna("").astype(str).str.lower() == "summary"] if "Record_Type" in run_quality_filtered.columns else run_quality_filtered
    breakdown_rows = run_quality_filtered[run_quality_filtered.get("Record_Type", pd.Series(dtype="object")).fillna("").astype(str).str.lower() == "breakdown"] if "Record_Type" in run_quality_filtered.columns else run_quality_filtered

    latest_summary = summary_rows.tail(1)
    overall_score = latest_non_blank_value(latest_summary, ["Overall_Run_Quality_Score"])
    grade = latest_non_blank_value(latest_summary, ["Run_Quality_Grade"])
    recommendation = latest_non_blank_value(latest_summary, ["Decision_Recommendation"])
    critical_warnings = latest_non_blank_value(latest_summary, ["Critical_Warnings"])
    major_warnings = latest_non_blank_value(latest_summary, ["Major_Warnings"])
    avg_confidence = "-"
    if not module_confidence_filtered.empty and "Overall_Confidence_Score" in module_confidence_filtered.columns:
        avg_confidence = format_percent(module_confidence_filtered["Overall_Confidence_Score"].map(parse_percent).mean())

    confidence_col = resolve_column(module_confidence_filtered, ["Overall_Confidence_Status"])
    gap_col = resolve_column(module_confidence_filtered, ["Primary_Data_Gap"])
    confidence_values = unique_text_values(module_confidence_filtered, confidence_col) if confidence_col else []
    gap_values = unique_text_values(module_confidence_filtered, gap_col) if gap_col else []
    filter_cols = st.columns(2)
    with filter_cols[0]:
        confidence_pick = st.multiselect("Confidence status", confidence_values, default=confidence_values, key="quality_confidence_filter")
    with filter_cols[1]:
        gap_pick = st.multiselect("Primary data gap", gap_values, default=gap_values, key="quality_gap_filter")
    if confidence_col:
        module_confidence_filtered = filter_by_selected_values(module_confidence_filtered, confidence_col, confidence_pick)
    if gap_col:
        module_confidence_filtered = filter_by_selected_values(module_confidence_filtered, gap_col, gap_pick)

    report_severity_col = resolve_column(report_format_filtered, ["Severity"])
    report_critical = count_matching_values(report_format_filtered, report_severity_col, "Critical") if report_severity_col else 0
    low_confidence_count = count_matching_values(module_confidence_filtered, confidence_col, "Low") if confidence_col else 0

    render_page_header(
        "Data Quality",
        "Review run health, report-format drift, and module confidence before a new refresh is published or handed off.",
        latest_non_blank_value(run_quality_df, ["Run_ID"]),
    )
    render_metric_cards(
        [
            {"label": "Overall Score", "value": overall_score or "-", "note": "Latest run quality score"},
            {"label": "Grade", "value": grade or "-", "note": "Operational quality label"},
            {"label": "Recommendation", "value": recommendation or "-", "note": "What the team should do next"},
            {"label": "Avg Confidence", "value": avg_confidence, "note": "Module confidence across FSNs"},
            {"label": "LOW Confidence FSNs", "value": f"{low_confidence_count:,}", "note": "Needs review"},
            {"label": "Format Critical Issues", "value": f"{report_critical:,}", "note": "Critical format drift rows"},
        ],
        columns=3,
    )
    if not breakdown_rows.empty and "Points_Earned" in breakdown_rows.columns:
        st.markdown("### Run Quality Breakdown")
        chart_df = breakdown_rows.loc[:, [column for column in ["Score_Category", "Score_Name", "Points_Earned", "Status"] if column in breakdown_rows.columns]].copy()
        chart_df["Points_Earned"] = chart_df["Points_Earned"].map(parse_float)
        fig = px.bar(chart_df, x="Score_Category", y="Points_Earned", color="Status" if "Status" in chart_df.columns else None, barmode="group")
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=320, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    if gap_col and not module_confidence_filtered.empty:
        st.markdown("### Primary Data Gap Distribution")
        render_chart_from_counts(module_confidence_filtered, gap_col, "FSN_Count")

    run_quality_cols = ["Run_ID", "Report_Date", "Overall_Run_Quality_Score", "Run_Quality_Grade", "Decision_Recommendation", "Score_Category", "Score_Name", "Max_Points", "Points_Earned", "Status", "Reason", "Suggested_Action", "Last_Updated"]
    module_confidence_cols = ["Run_ID", "FSN", "SKU_ID", "Product_Title", "Overall_Confidence_Score", "Overall_Confidence_Status", "Primary_Data_Gap", "Suggested_Data_Action", "Listing_Confidence_Status", "Order_Confidence_Status", "Return_Confidence_Status", "Settlement_Confidence_Status", "PNL_Confidence_Status", "COGS_Confidence_Status", "Ads_Confidence_Status", "Format_Confidence_Status", "Alert_Risk_Status", "Last_Updated"]
    report_format_cols = ["File_Name", "Sheet_Name", "Detected_Report_Type", "Sheet_Class", "Effective_Data_Rows", "Header_Detection_Status", "Required_Business_Headers_Present", "Row_Count", "Column_Count", "Header_Row_Index", "Baseline_Created_At", "Last_Updated"]
    render_dataframe_section(
        "Run Quality Score",
        run_quality_filtered,
        "flipkart_run_quality_score_filtered.csv",
        preferred_columns=run_quality_cols,
        style_columns={"Run_Quality_Grade": QUALITY_PALETTE, "Decision_Recommendation": DECISION_PALETTE, "Status": STATUS_PALETTE},
    )
    render_dataframe_section(
        "Module Confidence",
        module_confidence_filtered,
        "flipkart_module_confidence_filtered.csv",
        preferred_columns=module_confidence_cols,
        style_columns={"Overall_Confidence_Status": CONFIDENCE_PALETTE, "Primary_Data_Gap": STATUS_PALETTE, "Suggested_Data_Action": DECISION_PALETTE},
    )
    render_dataframe_section(
        "Report Format Monitor",
        report_format_filtered,
        "flipkart_report_format_monitor_filtered.csv",
        preferred_columns=report_format_cols,
        style_columns={"Header_Detection_Status": STATUS_PALETTE, "Required_Business_Headers_Present": STATUS_PALETTE},
    )


def build_fsn_candidates(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> pd.DataFrame:
    base_df = build_fsn_index(dataframe_or_empty(frames[FSN_METRICS_TAB]))
    if base_df.empty:
        return base_df
    base_df = apply_global_search(base_df, search_filters, ["FSN", "SKU_ID", "Product_Title", "Category"])
    display_df = base_df.copy()
    display_df["Display_Label"] = display_df.apply(
        lambda row: " | ".join(
            part for part in [normalize_text(row.get("FSN", "")), normalize_text(row.get("SKU_ID", "")), normalize_text(row.get("Product_Title", ""))] if part
        ),
        axis=1,
    )
    display_df = display_df[display_df["Display_Label"].map(bool)]
    return display_df.sort_values(by=[column for column in ["Product_Title", "FSN"] if column in display_df.columns], kind="stable")


def selected_rows_for_fsn(df: pd.DataFrame, fsn: str) -> pd.DataFrame:
    if df.empty or "FSN" not in df.columns or not clean_fsn(fsn):
        return df.iloc[0:0].copy()
    return df[df["FSN"].fillna("").map(clean_fsn) == clean_fsn(fsn)].copy()


def render_fsn_drilldown(frames: Dict[str, pd.DataFrame], search_filters: Dict[str, str]) -> None:
    candidates = build_fsn_candidates(frames, search_filters)
    render_page_header(
        "FSN Deep Dive",
        "A single-FSN operating view that pulls together the matching metrics, alerts, actions, ads, returns, listings, and quality signals.",
        latest_non_blank_value(dataframe_or_empty(frames[FSN_METRICS_TAB]), ["Run_ID"]),
    )
    if candidates.empty:
        st.info("No FSNs matched the current search.")
        return
    selected_label = st.selectbox("Select an FSN", candidates["Display_Label"].tolist(), index=0)
    selected_row = candidates[candidates["Display_Label"] == selected_label].iloc[0]
    selected_fsn = clean_fsn(selected_row.get("FSN", ""))

    fsn_metrics_df = selected_rows_for_fsn(dataframe_or_empty(frames[FSN_METRICS_TAB]), selected_fsn)
    alerts_df = selected_rows_for_fsn(dataframe_or_empty(frames[ALERTS_TAB]), selected_fsn)
    actions_df = selected_rows_for_fsn(dataframe_or_empty(frames[ACTIONS_TAB]), selected_fsn)
    ads_df = selected_rows_for_fsn(dataframe_or_empty(frames[ADS_TAB]), selected_fsn)
    order_item_master_df = selected_rows_for_fsn(dataframe_or_empty(frames[ORDER_ITEM_MASTER_TAB]), selected_fsn)
    order_item_source_detail_df = selected_rows_for_fsn(dataframe_or_empty(frames[ORDER_ITEM_SOURCE_DETAIL_TAB]), selected_fsn)
    all_returns_df = selected_rows_for_fsn(dataframe_or_empty(frames[RETURN_ALL_DETAILS_TAB]), selected_fsn)
    customer_summary_df = selected_rows_for_fsn(dataframe_or_empty(frames[CUSTOMER_RETURN_SUMMARY_TAB]), selected_fsn)
    courier_summary_df = selected_rows_for_fsn(dataframe_or_empty(frames[COURIER_RETURN_SUMMARY_TAB]), selected_fsn)
    return_type_pivot_df = selected_rows_for_fsn(dataframe_or_empty(frames[RETURN_TYPE_PIVOT_TAB]), selected_fsn)
    customer_returns_df = selected_rows_for_fsn(dataframe_or_empty(frames[CUSTOMER_RETURN_COMMENTS_TAB]), selected_fsn)
    courier_returns_df = selected_rows_for_fsn(dataframe_or_empty(frames[COURIER_RETURN_COMMENTS_TAB]), selected_fsn)
    listings_df = selected_rows_for_fsn(dataframe_or_empty(frames[LISTINGS_TAB]), selected_fsn)
    profit_df = selected_rows_for_fsn(dataframe_or_empty(frames[ADJUSTED_PROFIT_TAB]), selected_fsn)
    confidence_df = selected_rows_for_fsn(dataframe_or_empty(frames[MODULE_CONFIDENCE_TAB]), selected_fsn)
    competitor_df = selected_rows_for_fsn(dataframe_or_empty(frames[COMPETITOR_TAB]), selected_fsn)

    summary_source = fsn_metrics_df if not fsn_metrics_df.empty else candidates.loc[candidates["FSN"] == selected_fsn].copy()
    summary_row = summary_source.iloc[0] if not summary_source.empty else pd.Series(dtype="object")
    title = normalize_text(summary_row.get("Product_Title", "")) or normalize_text(selected_row.get("Product_Title", "")) or "-"
    sku = normalize_text(summary_row.get("SKU_ID", "")) or normalize_text(selected_row.get("SKU_ID", "")) or "-"
    category = normalize_text(summary_row.get("Category", "")) or "-"
    customer_return_count = normalize_text(summary_row.get("Customer_Return_Count", "")) or normalize_text(summary_row.get("Returns", "")) or "0"
    customer_return_rate = normalize_text(summary_row.get("Customer_Return_Rate", "")) or normalize_text(summary_row.get("Return_Rate", "")) or "0"
    courier_return_count = normalize_text(summary_row.get("Courier_Return_Count", "")) or "0"
    courier_return_rate = normalize_text(summary_row.get("Courier_Return_Rate", "")) or "0"
    total_return_count = normalize_text(summary_row.get("Total_Return_Count", "")) or normalize_text(summary_row.get("Returns", "")) or "0"
    total_return_rate = normalize_text(summary_row.get("Total_Return_Rate", "")) or normalize_text(summary_row.get("Return_Rate", "")) or "0"
    render_metric_cards(
        [
            {"label": "FSN", "value": selected_fsn, "note": title},
            {"label": "SKU", "value": sku, "note": category},
            {"label": "Alerts", "value": f"{len(alerts_df):,}", "note": "Matching alert rows"},
            {"label": "Actions", "value": f"{len(actions_df):,}", "note": "Matching action rows"},
            {"label": "Profit Rows", "value": f"{len(profit_df):,}", "note": "Adjustment-aware profit"},
            {"label": "Order Item Master Rows", "value": f"{len(order_item_master_df):,}", "note": "Master rows for this FSN"},
            {"label": "Customer Return Count", "value": customer_return_count, "note": f"Rate {customer_return_rate}"},
            {"label": "Courier Return Count", "value": courier_return_count, "note": f"Rate {courier_return_rate}"},
            {"label": "Total Return Count", "value": total_return_count, "note": f"Rate {total_return_rate}"},
            {"label": "Listings", "value": f"{len(listings_df):,}", "note": "Listing presence rows"},
            {"label": "Competitor Risk", "value": format_text_or_dash(latest_non_blank_value(competitor_df, ["Competition_Risk_Level"])), "note": "Comparable competitor view"},
        ],
        columns=4,
    )

    core_summary_rows: List[Dict[str, Any]] = []
    for source_name, df, preferred_columns in [
        ("FSN Metrics", fsn_metrics_df, ["FSN", "SKU_ID", "Product_Title", "Category", "Listing_Presence_Status", "Orders", "Units_Sold", "Gross_Sales", "Customer_Return_Count", "Customer_Return_Rate", "Courier_Return_Count", "Courier_Return_Rate", "Total_Return_Count", "Total_Return_Rate", "Returns", "Return_Rate", "Net_Settlement", "Final_Net_Profit", "Final_Profit_Margin", "COGS_Status", "Final_Action", "Final_Ads_Decision", "Final_Budget_Recommendation", "Ads_Risk_Level", "Ads_Opportunity_Level", "Last_Updated"]),
        ("Adjusted Profit", profit_df, ["FSN", "SKU_ID", "Product_Title", "Original_Final_Net_Profit", "Total_Adjustment_Additions", "Total_Adjustment_Deductions", "Net_Adjustment", "Adjusted_Final_Net_Profit", "Adjustment_Count", "Adjustment_Status", "Last_Updated"]),
        ("Ads", ads_df, ["FSN", "SKU_ID", "Product_Title", "Final_Product_Type", "Final_Seasonality_Tag", "Ad_Run_Type", "Current_Ad_Status", "Ad_ROAS", "Ad_ACOS", "Final_Ads_Decision", "Final_Budget_Recommendation", "Ads_Risk_Level", "Ads_Opportunity_Level", "Last_Updated"]),
        ("Customer Returns", customer_summary_df, ["FSN", "SKU_ID", "Product_Title", "Sold_Order_Items", "Customer_Return_Count", "Customer_Return_Rate", "Quality_Issue_Count", "Defective_Product_Count", "Damaged_Product_Count", "Missing_Item_Count", "Wrong_Product_Count", "Customer_Remorse_Count", "Top_Customer_Return_Reason", "Top_Customer_Return_Sub_Reason", "Customer_Return_Risk_Level", "Suggested_Action", "Data_Gap_Reason", "Last_Updated"]),
        ("Courier Returns", courier_summary_df, ["FSN", "SKU_ID", "Product_Title", "Sold_Order_Items", "Courier_Return_Count", "Courier_Return_Rate", "Order_Cancelled_Count", "Attempts_Exhausted_Count", "Shipment_Ageing_Count", "Not_Serviceable_Count", "ORC_Validated_Count", "Delivery_Failed_Count", "Top_Courier_Return_Reason", "Top_Courier_Return_Sub_Reason", "Courier_Return_Risk_Level", "Suggested_Action", "Data_Gap_Reason", "Last_Updated"]),
        ("Order Item Master", order_item_master_df, ["Run_ID", "Order_ID", "Order_Item_ID", "Master_Order_Key", "FSN", "SKU_ID", "Product_Title", "Order_Date", "Latest_Event_Date", "Selling_Price", "Settlement_Amount", "Net_Profit", "Return_YN", "Return_Type_Final", "Customer_Return_YN", "Courier_Return_YN", "Data_Completeness_Status", "Data_Gap_Reason", "Last_Updated"]),
        ("Listings", listings_df, ["FSN", "SKU_ID", "Product_Title", "Found_In_Active_Listing", "Listing_Presence_Status", "Possible_Issue", "Suggested_Action", "Priority", "Last_Updated"]),
        ("Confidence", confidence_df, ["FSN", "SKU_ID", "Product_Title", "Overall_Confidence_Score", "Overall_Confidence_Status", "Primary_Data_Gap", "Suggested_Data_Action", "COGS_Confidence_Status", "Ads_Confidence_Status", "Format_Confidence_Status", "Alert_Risk_Status", "Last_Updated"]),
        ("Competitor", competitor_df, ["FSN", "SKU_ID", "Product_Title", "Comparable_Competitor_Count", "Median_Comparable_Competitor_Unit_Price", "Price_Gap_Percent", "Competition_Risk_Score", "Competition_Risk_Level", "Suggested_Action", "Confidence", "Last_Updated"]),
    ]:
        if df.empty:
            continue
        row = df.iloc[0]
        row_payload = {"Source": source_name}
        for column in preferred_columns:
            if column in df.columns:
                row_payload[column] = row.get(column, "")
        core_summary_rows.append(row_payload)

    core_summary_df = pd.DataFrame(core_summary_rows)
    render_dataframe_section(
        "Core Snapshot",
        core_summary_df,
        "flipkart_fsn_core_snapshot.csv",
        caption="One row per source tab so the current FSN can be reviewed at a glance.",
        preferred_columns=[column for column in core_summary_df.columns if column != "Source"],
        style_columns={"Overall_Confidence_Status": CONFIDENCE_PALETTE, "Competition_Risk_Level": RISK_PALETTE, "Final_Ads_Decision": DECISION_PALETTE, "Adjustment_Status": STATUS_PALETTE},
    )
    render_dataframe_section(
        "Alerts for Selected FSN",
        alerts_df,
        "flipkart_fsn_alerts.csv",
        preferred_columns=["Alert_ID", "FSN", "SKU_ID", "Product_Title", "Alert_Type", "Severity", "Suggested_Action", "Reason", "Data_Confidence", "Status_Default", "Last_Updated"],
        style_columns={"Severity": SEVERITY_PALETTE},
    )
    render_dataframe_section(
        "Actions for Selected FSN",
        actions_df,
        "flipkart_fsn_actions.csv",
        preferred_columns=["Action_ID", "Alert_ID", "FSN", "SKU_ID", "Product_Title", "Owner", "Status", "Action_Taken", "Expected_Impact", "Review_After_Date", "Last_Updated"],
        style_columns={"Status": STATUS_PALETTE},
    )
    render_dataframe_section(
        "Returns Summary for Selected FSN",
        customer_summary_df,
        "flipkart_fsn_customer_return_summary.csv",
        preferred_columns=["FSN", "SKU_ID", "Product_Title", "Sold_Order_Items", "Customer_Return_Count", "Customer_Return_Rate", "Quality_Issue_Count", "Defective_Product_Count", "Damaged_Product_Count", "Missing_Item_Count", "Wrong_Product_Count", "Customer_Remorse_Count", "Top_Customer_Return_Reason", "Top_Customer_Return_Sub_Reason", "Customer_Return_Risk_Level", "Suggested_Action", "Data_Gap_Reason", "Last_Updated"],
        style_columns={"Customer_Return_Risk_Level": RISK_PALETTE, "Suggested_Action": DECISION_PALETTE},
    )
    render_dataframe_section(
        "Courier Returns for Selected FSN",
        courier_summary_df,
        "flipkart_fsn_courier_return_summary.csv",
        preferred_columns=["FSN", "SKU_ID", "Product_Title", "Sold_Order_Items", "Courier_Return_Count", "Courier_Return_Rate", "Order_Cancelled_Count", "Attempts_Exhausted_Count", "Shipment_Ageing_Count", "Not_Serviceable_Count", "ORC_Validated_Count", "Delivery_Failed_Count", "Top_Courier_Return_Reason", "Top_Courier_Return_Sub_Reason", "Courier_Return_Risk_Level", "Suggested_Action", "Data_Gap_Reason", "Last_Updated"],
        style_columns={"Courier_Return_Risk_Level": RISK_PALETTE, "Suggested_Action": DECISION_PALETTE},
    )
    with st.expander("Additional source tabs", expanded=False):
        render_dataframe_section(
            "Profit",
            profit_df,
            "flipkart_fsn_profit.csv",
            preferred_columns=["Run_ID", "FSN", "SKU_ID", "Product_Title", "Original_Final_Net_Profit", "Total_Adjustment_Additions", "Total_Adjustment_Deductions", "Net_Adjustment", "Adjusted_Final_Net_Profit", "Adjustment_Status", "Last_Updated"],
            style_columns={"Adjustment_Status": STATUS_PALETTE},
        )
        render_dataframe_section(
            "Ads",
            ads_df,
            "flipkart_fsn_ads.csv",
            preferred_columns=["FSN", "SKU_ID", "Product_Title", "Final_Product_Type", "Final_Seasonality_Tag", "Ad_Run_Type", "Current_Ad_Status", "Ad_ROAS", "Ad_ACOS", "Final_Ads_Decision", "Final_Budget_Recommendation", "Ads_Risk_Level", "Ads_Opportunity_Level", "Last_Updated"],
            style_columns={"Ads_Risk_Level": RISK_PALETTE, "Final_Ads_Decision": DECISION_PALETTE},
        )
        render_dataframe_section(
            "Return Details",
            all_returns_df,
            "flipkart_fsn_return_details.csv",
            preferred_columns=["Run_ID", "Return_ID", "Order_ID", "Order_Item_ID", "Return_Type", "Return_Bucket", "FSN", "SKU_ID", "Product_Title", "Return_Reason", "Return_Sub_Reason", "Comments", "Customer_Issue_Category", "Courier_Issue_Category", "Return_Status", "Return_Result", "Last_Updated"],
            style_columns={"Return_Type": STATUS_PALETTE, "Return_Bucket": STATUS_PALETTE},
        )
        render_dataframe_section(
            "Listings",
            listings_df,
            "flipkart_fsn_listings.csv",
            preferred_columns=["FSN", "SKU_ID", "Product_Title", "Found_In_Active_Listing", "Listing_Presence_Status", "Possible_Issue", "Suggested_Action", "Priority", "Last_Updated"],
            style_columns={"Listing_Presence_Status": STATUS_PALETTE, "Priority": STATUS_PALETTE},
        )
        render_dataframe_section(
            "Module Confidence",
            confidence_df,
            "flipkart_fsn_confidence.csv",
            preferred_columns=["FSN", "SKU_ID", "Product_Title", "Overall_Confidence_Score", "Overall_Confidence_Status", "Primary_Data_Gap", "Suggested_Data_Action", "Last_Updated"],
            style_columns={"Overall_Confidence_Status": CONFIDENCE_PALETTE, "Suggested_Data_Action": DECISION_PALETTE},
        )
        render_dataframe_section(
            "Competitor Intelligence",
            competitor_df,
            "flipkart_fsn_competitor.csv",
            preferred_columns=["FSN", "SKU_ID", "Product_Title", "Comparable_Competitor_Count", "Median_Comparable_Competitor_Unit_Price", "Price_Gap_Percent", "Competition_Risk_Score", "Competition_Risk_Level", "Competitor_Insight", "Suggested_Action", "Confidence", "Last_Updated"],
            style_columns={"Competition_Risk_Level": RISK_PALETTE, "Confidence": CONFIDENCE_PALETTE},
        )
    render_dataframe_section(
        "Return Type Pivot",
        return_type_pivot_df,
        "flipkart_fsn_return_type_pivot.csv",
        preferred_columns=["FSN", "SKU_ID", "Product_Title", "Sold_Order_Items", "Customer_Return_Count", "Courier_Return_Count", "Unknown_Return_Count", "Total_Return_Count", "Customer_Return_Rate", "Courier_Return_Rate", "Total_Return_Rate", "Customer_vs_Courier_Mix", "Dominant_Return_Type", "Last_Updated"],
        style_columns={"Customer_vs_Courier_Mix": STATUS_PALETTE, "Dominant_Return_Type": STATUS_PALETTE},
    )
    render_dataframe_section(
        "Order Item Master for Selected FSN",
        order_item_master_df,
        "flipkart_fsn_order_item_master.csv",
        preferred_columns=["Run_ID", "Order_ID", "Order_Item_ID", "Master_Order_Key", "FSN", "SKU_ID", "Product_Title", "Order_Date", "Latest_Event_Date", "Selling_Price", "Settlement_Amount", "Net_Profit", "Return_YN", "Return_Type_Final", "Customer_Return_YN", "Courier_Return_YN", "Data_Completeness_Status", "Data_Gap_Reason", "Last_Updated"],
        style_columns={"Return_YN": STATUS_PALETTE, "Customer_Return_YN": STATUS_PALETTE, "Courier_Return_YN": STATUS_PALETTE},
    )
    with st.expander("Order Item source detail for selected FSN", expanded=False):
        render_dataframe_section(
            "Order Item Source Detail",
            order_item_source_detail_df,
            "flipkart_fsn_order_item_source_detail.csv",
            preferred_columns=["Source_File", "Source_Tab", "Source_Row_Type", "Order_ID", "Order_Item_ID", "Return_ID", "FSN", "SKU_ID", "Product_Title", "Order_Date", "Settlement_Date", "Return_Date", "Quantity", "Selling_Price", "Settlement_Amount", "Net_Profit", "Return_Type", "Customer_Return_YN", "Courier_Return_YN", "Return_Status", "Return_Reason", "Return_Sub_Reason", "Alert_Count", "Critical_Alert_Count", "Final_Ads_Decision", "Competition_Risk_Level", "Data_Gap_Reason", "Last_Updated"],
            style_columns={"Source_Row_Type": STATUS_PALETTE, "Return_Type": STATUS_PALETTE, "Customer_Return_YN": STATUS_PALETTE, "Courier_Return_YN": STATUS_PALETTE},
        )
    if not order_item_master_df.empty:
        copy_cols = st.columns(2)
        with copy_cols[0]:
            st.text_area("Order Item IDs", value="\n".join(dict.fromkeys(value for value in order_item_master_df.get("Order_Item_ID", pd.Series(dtype="object")).fillna("").astype(str).tolist() if normalize_text(value))), height=180, key="fsn_deep_dive_order_item_ids")
        with copy_cols[1]:
            st.text_area("Return IDs", value="\n".join(dict.fromkeys(value for value in order_item_master_df.get("Return_IDs", pd.Series(dtype="object")).fillna("").astype(str).tolist() if normalize_text(value))), height=180, key="fsn_deep_dive_return_ids")


def render_sidebar(data: Dict[str, Any], default_page: str) -> tuple[str, Dict[str, str]]:
    st.sidebar.title("Flipkart Control Tower")
    if st.sidebar.button("Refresh data cache", use_container_width=True):
        load_dashboard_payload_from_sheet.clear()
        st.rerun()
    st.sidebar.markdown("### Status")
    st.sidebar.write(f"Dashboard status: {'Online' if data.get('spreadsheet_connected') else 'Degraded'}")
    st.sidebar.write(f"Spreadsheet connected: {'Yes' if data.get('spreadsheet_connected') else 'No'}")
    st.sidebar.write(f"Last data load: {data.get('last_data_load_timestamp', '-')}")
    if data.get("load_status") == "quota_limited" and data.get("load_message"):
        st.sidebar.warning(data["load_message"])
    page = st.sidebar.selectbox("Page", PAGE_ORDER, index=PAGE_ORDER.index(default_page))
    st.sidebar.markdown("### Filters")
    fsn_search = st.sidebar.text_input("FSN search", value="", placeholder="Type an FSN")
    sku_search = st.sidebar.text_input("SKU search", value="", placeholder="Type a SKU")
    product_search = st.sidebar.text_input("Product title search", value="", placeholder="Type a product title")
    return page, {"fsn": fsn_search, "sku": sku_search, "product": product_search}


def render_global_notices(data: Dict[str, Any]) -> None:
    if data["missing_tabs"]:
        st.warning(f"Missing source tabs: {', '.join(data['missing_tabs'])}")


def inject_dashboard_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            color-scheme: light;
            --dashboard-text: #0f172a;
            --dashboard-muted: #475569;
            --dashboard-sidebar-bg: #0f172a;
            --dashboard-sidebar-surface: #ffffff;
            --dashboard-sidebar-text: #e2e8f0;
            --dashboard-sidebar-border: #cbd5e1;
            --dashboard-accent: #0f766e;
            --dashboard-accent-strong: #2563eb;
        }
        ::selection {
            background: rgba(15, 118, 110, 0.18);
            color: #0f172a;
        }
        html, body {
            background: #f8fafc !important;
            color: var(--dashboard-text) !important;
        }
        [data-testid="stAppViewContainer"],
        [data-testid="stHeader"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"] {
            background: transparent !important;
        }
        .stApp,
        .stAppViewContainer,
        section.main,
        .main,
        .block-container {
            background:
                radial-gradient(circle at top right, rgba(15, 118, 110, 0.10), transparent 28%),
                radial-gradient(circle at bottom left, rgba(37, 99, 235, 0.08), transparent 24%),
                linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%) !important;
            color: var(--dashboard-text) !important;
        }
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(15, 118, 110, 0.10), transparent 28%),
                radial-gradient(circle at bottom left, rgba(37, 99, 235, 0.08), transparent 24%),
                linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
        }
        .block-container {
            padding-top: 1.15rem;
            padding-bottom: 1.75rem;
            max-width: 1600px;
        }
        .hero-card {
            padding: 1.1rem 1.2rem 1.2rem;
            border-radius: 22px;
            border: 1px solid rgba(15, 23, 42, 0.08);
            background: rgba(255, 255, 255, 0.78);
            box-shadow: 0 18px 48px rgba(15, 23, 42, 0.08);
            margin-bottom: 1rem;
        }
        .hero-card h1 {
            margin: 0.15rem 0 0.35rem 0;
            font-size: 2.25rem;
            line-height: 1.05;
            color: #0f172a;
        }
        .hero-card p {
            margin: 0;
            color: #334155;
            font-size: 0.98rem;
            max-width: 72ch;
        }
        .hero-kicker {
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.72rem;
            font-weight: 700;
            color: #0f766e;
        }
        .hero-meta {
            margin-top: 0.75rem;
            font-size: 0.82rem;
            color: #475569;
        }
        .metric-card {
            padding: 0.95rem 1rem 0.9rem;
            border-radius: 18px;
            border: 1px solid rgba(148, 163, 184, 0.22);
            background: rgba(255, 255, 255, 0.84);
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
            min-height: 110px;
            margin-bottom: 0.65rem;
        }
        .metric-label {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #64748b;
            margin-bottom: 0.25rem;
        }
        .metric-value {
            font-size: 1.75rem;
            font-weight: 800;
            color: #0f172a;
            line-height: 1.1;
            margin-bottom: 0.18rem;
        }
        .metric-note {
            font-size: 0.8rem;
            color: #475569;
        }
        .section-card {
            padding: 1rem 1rem 0.85rem;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.82);
            border: 1px solid rgba(148, 163, 184, 0.16);
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
            margin-bottom: 1rem;
        }
        .section-title {
            font-size: 0.85rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #0f172a;
            margin-bottom: 0.35rem;
        }
        .section-body {
            color: #334155;
            font-size: 0.93rem;
        }
        .status-badge {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 700;
            line-height: 1.2;
            border: 1px solid rgba(15, 23, 42, 0.08);
        }
        .status-critical { background: #fecaca; color: #7f1d1d; }
        .status-high { background: #fde68a; color: #78350f; }
        .status-medium { background: #fef3c7; color: #78350f; }
        .status-low { background: #dcfce7; color: #166534; }
        .status-grey { background: #e2e8f0; color: #334155; }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(15, 23, 42, 0.96) 0%, rgba(30, 41, 59, 0.98) 100%);
            color: var(--dashboard-sidebar-text) !important;
            border-right: 1px solid rgba(148, 163, 184, 0.20);
        }
        [data-testid="stSidebar"] *,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] div {
            color: var(--dashboard-sidebar-text) !important;
        }
        [data-testid="stSidebar"] .stButton button {
            background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
            color: #0f172a !important;
            border: 1px solid rgba(148, 163, 184, 0.45);
            font-weight: 700;
        }
        [data-testid="stSidebar"] .stButton button:hover {
            border-color: rgba(148, 163, 184, 0.75);
        }
        [data-testid="stSidebar"] input,
        [data-testid="stSidebar"] textarea,
        [data-testid="stSidebar"] select,
        [data-testid="stSidebar"] [data-baseweb="select"] *,
        [data-testid="stSidebar"] [data-baseweb="input"] * {
            color: #0f172a !important;
        }
        [data-testid="stSidebar"] input,
        [data-testid="stSidebar"] textarea {
            background: var(--dashboard-sidebar-surface) !important;
            border-color: var(--dashboard-sidebar-border) !important;
        }
        [data-testid="stSidebar"] [data-baseweb="select"] > div,
        [data-testid="stSidebar"] [data-baseweb="input"] > div {
            background: var(--dashboard-sidebar-surface) !important;
            border-color: var(--dashboard-sidebar-border) !important;
        }
        [data-testid="stSidebar"] ::placeholder {
            color: #64748b !important;
            opacity: 1;
        }
        [data-testid="stSidebar"] .stSelectbox,
        [data-testid="stSidebar"] .stTextInput,
        [data-testid="stSidebar"] .stMultiSelect {
            color: #e2e8f0 !important;
        }
        .stMarkdown,
        .stMarkdown p,
        .stMarkdown span,
        .stCaption,
        label,
        input,
        textarea,
        select,
        .stSelectbox,
        .stTextInput,
        .stMultiSelect {
            color: var(--dashboard-text) !important;
        }
        .stDataFrame, .stDataFrame * {
            color: var(--dashboard-text) !important;
        }
        div[data-testid="stDataFrame"] {
            color: var(--dashboard-text) !important;
        }
        div[data-testid="stDataFrame"] * {
            color: var(--dashboard-text) !important;
        }
        div[data-baseweb="select"] > div {
            color: var(--dashboard-text) !important;
        }
        div[data-baseweb="select"] input,
        div[data-baseweb="select"] span {
            color: var(--dashboard-text) !important;
        }
        input,
        textarea,
        select {
            background-color: #ffffff !important;
            color: var(--dashboard-text) !important;
        }
        input::placeholder,
        textarea::placeholder {
            color: #94a3b8 !important;
            opacity: 1;
        }
        .stButton button {
            border-radius: 999px;
            border: 1px solid rgba(15, 118, 110, 0.20);
            background: linear-gradient(135deg, rgba(15, 118, 110, 0.10), rgba(37, 99, 235, 0.10));
            color: var(--dashboard-text) !important;
            font-weight: 700;
        }
        .stButton button:hover {
            border-color: rgba(15, 118, 110, 0.42);
        }
        .stSelectbox div[data-baseweb="select"],
        .stMultiSelect div[data-baseweb="select"],
        .stTextInput div[data-baseweb="input"],
        .stTextArea div[data-baseweb="textarea"] {
            color: var(--dashboard-text) !important;
        }
        .stSelectbox div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="select"] > div,
        .stTextInput div[data-baseweb="input"] > div,
        .stTextArea div[data-baseweb="textarea"] > div {
            background-color: #ffffff !important;
            color: var(--dashboard-text) !important;
        }
        .stSelectbox [data-baseweb="select"] [role="combobox"],
        .stMultiSelect [data-baseweb="select"] [role="combobox"] {
            background-color: #ffffff !important;
            color: var(--dashboard-text) !important;
        }
        @media (prefers-color-scheme: dark) {
            html, body, .stApp, .stAppViewContainer, section.main, .main, .block-container {
                background:
                    radial-gradient(circle at top right, rgba(15, 118, 110, 0.10), transparent 28%),
                    radial-gradient(circle at bottom left, rgba(37, 99, 235, 0.08), transparent 24%),
                    linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%) !important;
                color: var(--dashboard-text) !important;
            }
            [data-testid="stSidebar"], [data-testid="stSidebar"] * {
                color: var(--dashboard-sidebar-text) !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_css = inject_dashboard_css


def main() -> None:
    st.set_page_config(page_title="Flipkart Control Tower", page_icon="📊", layout="wide", initial_sidebar_state="expanded")
    inject_css()
    try:
        data = load_dashboard_payload()
    except Exception as exc:
        st.error(
            "Unable to load the Flipkart Google Sheet. "
            f"{exc.__class__.__name__}: {exc}"
        )
        st.stop()

    page, search_query = render_sidebar(data, PAGE_ORDER[0])
    display_status_strip(data)
    render_global_notices(data)
    frames = data["frames"]
    metrics, metric_lookup = build_overview_metrics(frames)

    if page == "Executive Overview":
        render_executive_overview(frames, metric_lookup, search_query)
    elif page == "Alerts & Actions":
        render_alerts_actions(frames, search_query)
    elif page == "Profit & COGS":
        render_profit_cogs(frames, search_query)
    elif page == "Ads Planner":
        render_ads_planner(frames, search_query)
    elif page == "Competitor Risk":
        render_competitor_risk(frames, search_query)
    elif page == "Data Quality":
        render_data_quality(frames, search_query)
    elif page == "FSN Drilldown":
        render_fsn_drilldown(frames, search_query)
    else:
        render_page_header("Executive Overview", "Default view")
        render_metric_cards(metrics, columns=4)


def run_app() -> None:
    st.set_page_config(page_title="Flipkart Control Tower", page_icon="F", layout="wide", initial_sidebar_state="expanded")
    inject_css()
    try:
        data = load_dashboard_payload()
    except Exception as exc:
        st.error(
            "Unable to load the Flipkart Google Sheet. "
            f"{exc.__class__.__name__}: {exc}"
        )
        st.stop()

    page, search_filters = render_sidebar(data, PAGE_ORDER[0])
    display_status_strip(data)
    load_status = normalize_text(data.get("load_status")).lower()
    if load_status == "missing_spreadsheet_id":
        st.error("MASTER_SPREADSHEET_ID is missing. Add it in Streamlit Cloud Secrets.")
        st.stop()
    if load_status == "auth_error":
        if data.get("gcp_service_account_found"):
            st.error(data.get("auth_error_message_safe") or "Service account secrets found but Google auth failed. Check private_key formatting and Google Sheet sharing.")
        else:
            st.error("gcp_service_account block not found in Streamlit Secrets.")
        st.stop()
    if load_status in {"missing_secrets", "sheet_error"}:
        st.warning(data.get("load_message") or "Unable to load dashboard data.")
        st.stop()
    if load_status == "quota_limited":
        st.warning(data.get("load_message") or "Google Sheets quota limit reached. Wait 5 minutes and refresh.")
    render_global_notices(data)
    frames = data["frames"]
    metrics, metric_lookup = build_overview_metrics(frames)

    if page == "Executive Overview":
        render_executive_overview(frames, metric_lookup, search_filters)
    elif page == "Alerts & Actions":
        render_alerts_actions(frames, search_filters)
    elif page == "Profit & COGS":
        render_profit_cogs(frames, search_filters)
    elif page == "Ads Planner":
        render_ads_planner(frames, search_filters)
    elif page == "Competitor Risk":
        render_competitor_risk(frames, search_filters)
    elif page == "Data Quality":
        render_data_quality(frames, search_filters)
    elif page == "Returns Intelligence":
        render_returns_intelligence(frames, search_filters)
    elif page == "Return Comments Explorer":
        render_return_comments_explorer(frames, search_filters)
    elif page == "Order ID Explorer":
        render_order_item_explorer(frames, search_filters)
    elif page == "FSN Deep Dive":
        render_fsn_drilldown(frames, search_filters)
    elif page == "Listing Issues":
        render_listing_issues(frames, search_filters)
    elif page == "Run History & Comparison":
        render_run_history_comparison(frames, search_filters)
    elif page == "Raw Data Explorer / Downloads":
        render_raw_data_explorer(frames, data)
    else:
        render_page_header("Executive Overview", "Default view")
        render_metric_cards(metrics, columns=4)


main = run_app


if __name__ == "__main__":
    main()
