from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_ads_mapping_helpers import (
    ADS_MASTER_HEADERS,
    ADS_MAPPING_ISSUES_HEADERS,
    ADS_MAPPING_LOG_PATH,
    ADS_MASTER_TAB,
    ADS_MAPPING_ISSUES_TAB,
    ADS_PLANNER_AD_COLUMNS,
    ADS_PLANNER_TAB,
    FSN_BRIDGE_PATH,
    ADS_RAW_PATH,
    ADS_SUMMARY_HEADERS,
    ADS_SUMMARY_TAB,
    LOCAL_ADS_MASTER_PATH,
    LOCAL_ADS_MAPPING_ISSUES_PATH,
    LOCAL_ADS_PLANNER_PATH,
    LOCAL_ADS_SUMMARY_PATH,
    SPREADSHEET_META_PATH,
    SKU_ANALYSIS_TAB,
    append_log,
    build_bridge_indexes,
    build_sheets_service,
    compute_row_metrics,
    detect_ads_columns,
    detected_header_name,
    ensure_directories,
    ensure_tab,
    format_int,
    format_metric,
    load_csv_table,
    load_json_file,
    now_iso,
    ordered_counter,
    resolve_sku_candidates,
    row_to_json,
    run_id,
    read_table,
    tab_exists,
    unique_fsns_from_rows,
    write_local_csv,
    write_output_tab,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, normalize_text


def choose_best_confidence(confidences: Sequence[str]) -> str:
    priority = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}
    best = "UNKNOWN"
    for confidence in confidences:
        confidence_norm = normalize_text(confidence).upper() or "UNKNOWN"
        if priority.get(confidence_norm, 0) > priority.get(best, 0):
            best = confidence_norm
    return best


def summarize_spend_source(sources: Sequence[str]) -> str:
    priorities = {"Source Spend": 3, "Estimated From ROI": 2, "": 0}
    best = ""
    for source in sources:
        if priorities.get(source, 0) > priorities.get(best, 0):
            best = source
    return best


def performance_note(current_status: str, acos: str, roas: str) -> str:
    if normalize_text(current_status) == "Ads Mapping Issue":
        return "Fix Ads Mapping"
    if not normalize_text(acos) or not normalize_text(roas):
        return "Insufficient Ads Data"
    acos_value = float(acos)
    roas_value = float(roas)
    if acos_value <= 0.20 and roas_value >= 5:
        return "Strong Ads Performance"
    if acos_value <= 0.35 and roas_value >= 3:
        return "Acceptable Ads Performance"
    if acos_value > 0.35:
        return "High ACOS"
    return "Insufficient Ads Data"


def build_master_and_issues_rows(
    ads_rows: Sequence[Dict[str, str]],
    detected: Dict[str, Dict[str, Any]],
    bridge_indexes: Dict[str, Any],
    source_file: str,
    run: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    master_rows: List[Dict[str, Any]] = []
    issue_rows: List[Dict[str, Any]] = []

    fsn_index = bridge_indexes["fsn_index"]
    for row in ads_rows:
        raw_fsn = clean_fsn(row.get(detected_header_name(detected, "fsn"), "")) if detected_header_name(detected, "fsn") else ""
        raw_sku = normalize_text(row.get(detected_header_name(detected, "sku_id"), "")) if detected_header_name(detected, "sku_id") else ""
        raw_product_name = normalize_text(row.get(detected_header_name(detected, "product_name"), "")) if detected_header_name(detected, "product_name") else ""
        campaign_id = normalize_text(row.get(detected_header_name(detected, "campaign_id"), "")) if detected_header_name(detected, "campaign_id") else ""
        campaign_name = normalize_text(row.get(detected_header_name(detected, "campaign_name"), "")) if detected_header_name(detected, "campaign_name") else ""
        adgroup_id = normalize_text(row.get(detected_header_name(detected, "adgroup_id"), "")) if detected_header_name(detected, "adgroup_id") else ""
        adgroup_name = normalize_text(row.get(detected_header_name(detected, "adgroup_name"), "")) if detected_header_name(detected, "adgroup_name") else ""
        metrics = compute_row_metrics(row, detected)

        mapped_row: Dict[str, Any] | None = None
        mapping_method = ""
        mapping_confidence = ""
        possible_fsns: List[str] = []
        possible_skus: List[str] = []

        if raw_fsn:
            bridge_row = fsn_index.get(raw_fsn)
            if bridge_row:
                mapped_row = bridge_row
                mapping_method = "Direct FSN"
                mapping_confidence = "HIGH"
            else:
                issue_rows.append(
                    {
                        "Run_ID": run,
                        "Raw_SKU": raw_sku,
                        "Raw_FSN": raw_fsn,
                        "Raw_Product_Name": raw_product_name,
                        "Campaign_ID": campaign_id,
                        "Campaign_Name": campaign_name,
                        "Issue_Type": "No Matching FSN",
                        "Issue_Detail": "Direct FSN was present but did not resolve in the bridge",
                        "Possible_FSNs": "",
                        "Possible_SKUs": "",
                        "Raw_Row_JSON": row_to_json(row),
                        "Source_File": source_file,
                        "Last_Updated": now_iso(),
                    }
                )
                continue
        elif raw_sku:
            candidates, match_type = resolve_sku_candidates(raw_sku, bridge_indexes)
            possible_fsns = unique_fsns_from_rows(candidates)
            possible_skus = [normalize_text(candidate.get("Seller_SKU", "")) for candidate in candidates if normalize_text(candidate.get("Seller_SKU", ""))]
            if not candidates:
                issue_rows.append(
                    {
                        "Run_ID": run,
                        "Raw_SKU": raw_sku,
                        "Raw_FSN": raw_fsn,
                        "Raw_Product_Name": raw_product_name,
                        "Campaign_ID": campaign_id,
                        "Campaign_Name": campaign_name,
                        "Issue_Type": "No Matching FSN",
                        "Issue_Detail": "SKU did not resolve to any FSN in the bridge",
                        "Possible_FSNs": "",
                        "Possible_SKUs": "",
                        "Raw_Row_JSON": row_to_json(row),
                        "Source_File": source_file,
                        "Last_Updated": now_iso(),
                    }
                )
                continue
            if len(possible_fsns) > 1:
                issue_rows.append(
                    {
                        "Run_ID": run,
                        "Raw_SKU": raw_sku,
                        "Raw_FSN": raw_fsn,
                        "Raw_Product_Name": raw_product_name,
                        "Campaign_ID": campaign_id,
                        "Campaign_Name": campaign_name,
                        "Issue_Type": "SKU Maps To Multiple FSNs",
                        "Issue_Detail": f"SKU resolved via {match_type} match to multiple FSNs",
                        "Possible_FSNs": "; ".join(possible_fsns),
                        "Possible_SKUs": "; ".join(sorted(dict.fromkeys(possible_skus))),
                        "Raw_Row_JSON": row_to_json(row),
                        "Source_File": source_file,
                        "Last_Updated": now_iso(),
                    }
                )
                continue
            mapped_row = candidates[0]
            mapping_method = "Unique SKU to FSN"
            mapping_confidence = "MEDIUM"
        else:
            issue_rows.append(
                {
                    "Run_ID": run,
                    "Raw_SKU": "",
                    "Raw_FSN": "",
                    "Raw_Product_Name": raw_product_name,
                    "Campaign_ID": campaign_id,
                    "Campaign_Name": campaign_name,
                    "Issue_Type": "No Mapping Key",
                    "Issue_Detail": "Neither SKU nor FSN was present in the row",
                    "Possible_FSNs": "",
                    "Possible_SKUs": "",
                    "Raw_Row_JSON": row_to_json(row),
                    "Source_File": source_file,
                    "Last_Updated": now_iso(),
                }
            )
            continue

        assert mapped_row is not None
        bridge_fsn = clean_fsn(mapped_row.get("FSN", ""))
        bridge_sku = normalize_text(mapped_row.get("Seller_SKU", "")) or raw_sku
        bridge_title = normalize_text(mapped_row.get("Product_Title", "")) or raw_product_name

        master_rows.append(
            {
                "Run_ID": run,
                "FSN": bridge_fsn or raw_fsn,
                "SKU_ID": bridge_sku,
                "Product_Title": bridge_title,
                "Campaign_ID": campaign_id,
                "Campaign_Name": campaign_name,
                "AdGroup_ID": adgroup_id,
                "AdGroup_Name": adgroup_name,
                "Views": format_int(metrics["views"]),
                "Clicks": format_int(metrics["clicks"]),
                "Direct_Units_Sold": format_int(metrics["direct_units"]),
                "Indirect_Units_Sold": format_int(metrics["indirect_units"]),
                "Total_Units_Sold": format_int(metrics["total_units"]),
                "Total_Revenue": format_metric(metrics["revenue"], 2),
                "ROI": format_metric(metrics["roi"], 4),
                "Estimated_Ad_Spend": format_metric(metrics["spend"], 2) if metrics["spend_source"] else "",
                "ROAS": format_metric(metrics["roas"], 4),
                "ACOS": format_metric(metrics["acos"], 4),
                "Mapping_Method": mapping_method,
                "Mapping_Confidence": mapping_confidence,
                "Source_File": source_file,
                "Last_Updated": now_iso(),
                "Spend_Source": metrics["spend_source"],
            }
        )

    return master_rows, issue_rows


def build_summary_rows(master_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in master_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn:
            grouped[fsn].append(dict(row))

    summary_rows: List[Dict[str, Any]] = []
    for fsn, rows in grouped.items():
        campaign_keys = []
        mapping_confidences = []
        spend_sources = []
        views = 0.0
        clicks = 0.0
        total_units = 0.0
        revenue = 0.0
        spend = 0.0
        for row in rows:
            campaign_key = normalize_text(row.get("Campaign_ID", "")) or normalize_text(row.get("Campaign_Name", ""))
            if campaign_key and campaign_key not in campaign_keys:
                campaign_keys.append(campaign_key)
            mapping_confidences.append(normalize_text(row.get("Mapping_Confidence", "")))
            views += float(normalize_text(row.get("Views", "0")) or 0)
            clicks += float(normalize_text(row.get("Clicks", "0")) or 0)
            total_units += float(normalize_text(row.get("Total_Units_Sold", "0")) or 0)
            revenue += float(normalize_text(row.get("Total_Revenue", "0")) or 0)
            spend += float(normalize_text(row.get("Estimated_Ad_Spend", "0")) or 0)
            spend_source = normalize_text(row.get("Spend_Source", ""))
            if spend_source:
                spend_sources.append(spend_source)
        ctr = (clicks / views) if views > 0 else 0.0
        roas = (revenue / spend) if spend > 0 else 0.0
        acos = (spend / revenue) if revenue > 0 and spend > 0 else 0.0
        summary_rows.append(
            {
                "FSN": fsn,
                "SKU_ID": next((normalize_text(row.get("SKU_ID", "")) for row in rows if normalize_text(row.get("SKU_ID", ""))), ""),
                "Product_Title": next((normalize_text(row.get("Product_Title", "")) for row in rows if normalize_text(row.get("Product_Title", ""))), ""),
                "Ad_Rows": str(len(rows)),
                "Campaign_Count": str(len(campaign_keys)),
                "Views": format_metric(views, 0) if views else "0",
                "Clicks": format_metric(clicks, 0) if clicks else "0",
                "CTR": format_metric(ctr, 4) if views > 0 else "",
                "Total_Units_Sold": format_metric(total_units, 0) if total_units else "0",
                "Total_Revenue": format_metric(revenue, 2) if revenue else "0",
                "Estimated_Ad_Spend": format_metric(spend, 2) if spend else "",
                "ROAS": format_metric(roas, 4) if roas else "",
                "ACOS": format_metric(acos, 4) if acos else "",
                "Mapping_Confidence": choose_best_confidence(mapping_confidences),
                "Spend_Source": summarize_spend_source(spend_sources),
                "Last_Updated": now_iso(),
            }
        )

    summary_rows.sort(key=lambda row: row.get("FSN", ""))
    return summary_rows


def choose_best_confidence(confidences: Sequence[str]) -> str:
    priority = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}
    best = "UNKNOWN"
    for confidence in confidences:
        confidence_norm = normalize_text(confidence).upper() or "UNKNOWN"
        if priority.get(confidence_norm, 0) > priority.get(best, 0):
            best = confidence_norm
    return best


def summarize_spend_source(sources: Sequence[str]) -> str:
    if not sources:
        return ""
    if "Source Spend" in sources:
        return "Source Spend"
    if "Estimated From ROI" in sources:
        return "Estimated From ROI"
    return sources[0]


def merge_planner_rows(
    planner_headers: Sequence[str],
    planner_rows: Sequence[Dict[str, str]],
    summary_rows: Sequence[Dict[str, Any]],
    issue_rows: Sequence[Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]]]:
    new_headers = list(planner_headers)
    for column in ADS_PLANNER_AD_COLUMNS:
        if column not in new_headers:
            new_headers.append(column)

    summary_lookup = {clean_fsn(row.get("FSN", "")): dict(row) for row in summary_rows if clean_fsn(row.get("FSN", ""))}
    issue_fsns = {clean_fsn(row.get("Raw_FSN", "")) for row in issue_rows if clean_fsn(row.get("Raw_FSN", ""))}

    updated_rows: List[Dict[str, Any]] = []
    for row in planner_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        summary = summary_lookup.get(fsn, {})
        has_issue = fsn in issue_fsns
        current_status = "No Ads Data"
        mapping_status = "No Ads Data"
        confidence = ""
        ad_rows = summary.get("Ad_Rows", "")
        campaign_count = summary.get("Campaign_Count", "")
        views = summary.get("Views", "")
        clicks = summary.get("Clicks", "")
        ctr = summary.get("CTR", "")
        revenue = summary.get("Total_Revenue", "")
        estimated_spend = summary.get("Estimated_Ad_Spend", "")
        roas = summary.get("ROAS", "")
        acos = summary.get("ACOS", "")
        if fsn in summary_lookup:
            current_status = "Ads Data Available"
            mapping_status = "Mapped"
            confidence = summary.get("Mapping_Confidence", "")
        if has_issue:
            current_status = "Ads Mapping Issue"
            mapping_status = "Issue"
            if not confidence:
                confidence = "LOW"
        performance_note = performance_note_for_row(current_status, acos, roas)
        updated = dict(row)
        updated["Current_Ad_Status"] = current_status
        updated["Ad_Rows"] = ad_rows
        updated["Ad_Campaign_Count"] = campaign_count
        updated["Ad_Views"] = views
        updated["Ad_Clicks"] = clicks
        updated["Ad_CTR"] = ctr
        updated["Ad_Revenue"] = revenue
        updated["Estimated_Ad_Spend"] = estimated_spend
        updated["Ad_ROAS"] = roas
        updated["Ad_ACOS"] = acos
        updated["Ads_Mapping_Status"] = mapping_status
        updated["Ads_Data_Confidence"] = confidence
        updated["Ads_Performance_Note"] = performance_note
        updated_rows.append(updated)

    return new_headers, updated_rows


def performance_note_for_row(current_status: str, acos: str, roas: str) -> str:
    if normalize_text(current_status) == "Ads Mapping Issue":
        return "Fix Ads Mapping"
    if not normalize_text(acos) or not normalize_text(roas):
        return "Insufficient Ads Data"
    acos_value = float(acos)
    roas_value = float(roas)
    if acos_value <= 0.20 and roas_value >= 5:
        return "Strong Ads Performance"
    if acos_value <= 0.35 and roas_value >= 3:
        return "Acceptable Ads Performance"
    if acos_value > 0.35:
        return "High ACOS"
    return "Insufficient Ads Data"


def main() -> None:
    ensure_directories()
    if not ADS_RAW_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {ADS_RAW_PATH}")
    if not FSN_BRIDGE_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {FSN_BRIDGE_PATH}")
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    run = run_id()
    spreadsheet_id = load_json_file(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_sheets_service()

    ads_headers, ads_rows = load_csv_table(ADS_RAW_PATH)
    bridge_headers, bridge_rows = load_csv_table(FSN_BRIDGE_PATH)
    detected = detect_ads_columns(ads_headers)
    bridge_indexes = build_bridge_indexes(bridge_rows)

    master_rows, issue_rows = build_master_and_issues_rows(ads_rows, detected, bridge_indexes, ADS_RAW_PATH.name, run)
    summary_rows = build_summary_rows(master_rows)

    for tab_name in [ADS_MASTER_TAB, ADS_MAPPING_ISSUES_TAB, ADS_SUMMARY_TAB]:
        ensure_tab(sheets_service, spreadsheet_id, tab_name)
    planner_sheet_id = ensure_tab(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)
    if not tab_exists(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {SKU_ANALYSIS_TAB}")

    sheets_planner_headers, sheets_planner_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)

    updated_planner_headers, updated_planner_rows = merge_planner_rows(
        sheets_planner_headers,
        sheets_planner_rows,
        summary_rows,
        issue_rows,
    )

    write_local_csv(LOCAL_ADS_MASTER_PATH, ADS_MASTER_HEADERS, master_rows)
    write_local_csv(LOCAL_ADS_MAPPING_ISSUES_PATH, ADS_MAPPING_ISSUES_HEADERS, issue_rows)
    write_local_csv(LOCAL_ADS_SUMMARY_PATH, ADS_SUMMARY_HEADERS, summary_rows)
    write_local_csv(LOCAL_ADS_PLANNER_PATH, updated_planner_headers, updated_planner_rows)

    write_output_tab(sheets_service, spreadsheet_id, ADS_MASTER_TAB, ADS_MASTER_HEADERS, master_rows, ensure_tab(sheets_service, spreadsheet_id, ADS_MASTER_TAB))
    write_output_tab(sheets_service, spreadsheet_id, ADS_MAPPING_ISSUES_TAB, ADS_MAPPING_ISSUES_HEADERS, issue_rows, ensure_tab(sheets_service, spreadsheet_id, ADS_MAPPING_ISSUES_TAB))
    write_output_tab(sheets_service, spreadsheet_id, ADS_SUMMARY_TAB, ADS_SUMMARY_HEADERS, summary_rows, ensure_tab(sheets_service, spreadsheet_id, ADS_SUMMARY_TAB))
    write_output_tab(sheets_service, spreadsheet_id, ADS_PLANNER_TAB, updated_planner_headers, updated_planner_rows, planner_sheet_id)

    unique_fsns = sorted({clean_fsn(row.get("FSN", "")) for row in master_rows if clean_fsn(row.get("FSN", ""))})
    total_views = sum(float(normalize_text(row.get("Views", "0")) or 0) for row in master_rows)
    total_clicks = sum(float(normalize_text(row.get("Clicks", "0")) or 0) for row in master_rows)
    total_revenue = sum(float(normalize_text(row.get("Total_Revenue", "0")) or 0) for row in master_rows)
    total_spend = sum(float(normalize_text(row.get("Estimated_Ad_Spend", "0")) or 0) for row in master_rows)
    roas_values = [float(row["ROAS"]) for row in master_rows if normalize_text(row.get("ROAS", ""))]
    acos_values = [float(row["ACOS"]) for row in master_rows if normalize_text(row.get("ACOS", ""))]

    issue_distribution = ordered_counter(issue_rows, "Issue_Type")
    tabs_updated = [ADS_MASTER_TAB, ADS_MAPPING_ISSUES_TAB, ADS_SUMMARY_TAB, ADS_PLANNER_TAB]

    append_log(
        ADS_MAPPING_LOG_PATH,
        [
            "timestamp",
            "run_id",
            "status",
            "raw_ads_rows",
            "mapped_ads_rows",
            "mapping_issue_rows",
            "fsns_with_ads",
            "total_views",
            "total_clicks",
            "total_revenue",
            "total_estimated_ad_spend",
            "average_roas",
            "average_acos",
            "issue_type_distribution",
            "tabs_updated",
            "log_path",
            "message",
        ],
        {
            "timestamp": now_iso(),
            "run_id": run,
            "status": "SUCCESS",
            "raw_ads_rows": len(ads_rows),
            "mapped_ads_rows": len(master_rows),
            "mapping_issue_rows": len(issue_rows),
            "fsns_with_ads": len(unique_fsns),
            "total_views": int(total_views),
            "total_clicks": int(total_clicks),
            "total_revenue": format_metric(total_revenue, 2),
            "total_estimated_ad_spend": format_metric(total_spend, 2),
            "average_roas": format_metric(sum(roas_values) / len(roas_values), 4) if roas_values else "",
            "average_acos": format_metric(sum(acos_values) / len(acos_values), 4) if acos_values else "",
            "issue_type_distribution": json.dumps(issue_distribution, ensure_ascii=False),
            "tabs_updated": json.dumps(tabs_updated, ensure_ascii=False),
            "log_path": str(ADS_MAPPING_LOG_PATH),
            "message": "Built Flipkart ads mapping from raw ADS.csv",
        },
    )

    payload = {
        "status": "SUCCESS",
        "raw_ads_rows": len(ads_rows),
        "mapped_ads_rows": len(master_rows),
        "mapping_issue_rows": len(issue_rows),
        "fsns_with_ads": len(unique_fsns),
        "total_views": int(total_views),
        "total_clicks": int(total_clicks),
        "total_revenue": format_metric(total_revenue, 2),
        "total_estimated_ad_spend": format_metric(total_spend, 2),
        "average_roas": format_metric(sum(roas_values) / len(roas_values), 4) if roas_values else "",
        "average_acos": format_metric(sum(acos_values) / len(acos_values), 4) if acos_values else "",
        "issue_type_distribution": issue_distribution,
        "tabs_updated": tabs_updated,
        "log_path": str(ADS_MAPPING_LOG_PATH),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
