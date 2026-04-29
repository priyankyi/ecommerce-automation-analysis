from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.marketplaces.flipkart.flipkart_ads_mapping_helpers import (
    ADS_DIAGNOSTIC_JSON_PATH,
    ADS_DIAGNOSTIC_LOG_PATH,
    ADS_RAW_PATH,
    build_bridge_indexes,
    detect_ads_columns,
    load_csv_table,
    append_log,
    now_iso,
    resolve_sku_candidates,
    unique_fsns_from_rows,
)
from src.marketplaces.flipkart.flipkart_utils import clean_fsn, ensure_directories, normalize_text, parse_float


def header_or_blank(detected: Dict[str, Dict[str, Any]], key: str) -> str:
    return normalize_text(detected.get(key, {}).get("header", ""))


def sample_rows(rows: List[Dict[str, str]], limit: int = 5) -> List[Dict[str, str]]:
    return [{key: row.get(key, "") for key in row.keys()} for row in rows[:limit]]


def main() -> None:
    ensure_directories()
    run_id = f"FLIPKART_ADS_DIAGNOSTIC_{Path(ADS_RAW_PATH).stem.upper()}"
    status = "SUCCESS"
    ads_file_found = ADS_RAW_PATH.exists()
    raw_rows = 0
    columns: List[str] = []
    detected: Dict[str, Dict[str, Any]] = {}
    detected_sku_column = ""
    detected_fsn_column = ""
    detected_campaign_columns: Dict[str, str] = {}
    detected_views_column = ""
    detected_clicks_column = ""
    detected_units_column: Dict[str, str] = {}
    detected_revenue_column = ""
    detected_roi_column = ""
    detected_spend_column = ""
    sample: List[Dict[str, str]] = []
    mapping_possible_by_fsn = False
    mapping_possible_by_sku = False
    suspected_risks: List[str] = []

    if ads_file_found:
        columns, rows = load_csv_table(ADS_RAW_PATH)
        raw_rows = len(rows)
        detected = detect_ads_columns(columns)
        detected_sku_column = header_or_blank(detected, "sku_id")
        detected_fsn_column = header_or_blank(detected, "fsn")
        detected_campaign_columns = {
            "campaign_id": header_or_blank(detected, "campaign_id"),
            "campaign_name": header_or_blank(detected, "campaign_name"),
            "adgroup_id": header_or_blank(detected, "adgroup_id"),
            "adgroup_name": header_or_blank(detected, "adgroup_name"),
        }
        detected_views_column = header_or_blank(detected, "views")
        detected_clicks_column = header_or_blank(detected, "clicks")
        detected_units_column = {
            "direct_units_sold": header_or_blank(detected, "direct_units_sold"),
            "indirect_units_sold": header_or_blank(detected, "indirect_units_sold"),
        }
        detected_revenue_column = header_or_blank(detected, "total_revenue")
        detected_roi_column = header_or_blank(detected, "roi")
        detected_spend_column = header_or_blank(detected, "estimated_ad_spend")
        sample = sample_rows(rows, 5)

        bridge_headers, bridge_rows = load_csv_table(Path(PROJECT_ROOT / "data" / "output" / "marketplaces" / "flipkart" / "flipkart_fsn_bridge.csv"))
        bridge_indexes = build_bridge_indexes(bridge_rows)
        fsn_index = bridge_indexes["fsn_index"]

        if detected_fsn_column:
            mapping_possible_by_fsn = any(clean_fsn(row.get(detected_fsn_column, "")) in fsn_index for row in rows)
        if detected_sku_column:
            mapping_possible_by_sku = any(resolve_sku_candidates(row.get(detected_sku_column, ""), bridge_indexes)[0] for row in rows)

        if not detected_fsn_column:
            suspected_risks.append("No direct FSN column detected; mapping will rely on SKU bridge only")
        if not detected_sku_column:
            suspected_risks.append("No SKU column detected; rows cannot be bridged safely")
        if not detected_spend_column:
            suspected_risks.append("No explicit ad spend column detected; spend will need ROI-based estimation when available")
        if not mapping_possible_by_sku and not mapping_possible_by_fsn:
            suspected_risks.append("No rows appear to resolve to the current FSN bridge")

        unmatched_rows = 0
        ambiguous_rows = 0
        if detected_sku_column:
            for row in rows:
                candidates, _ = resolve_sku_candidates(row.get(detected_sku_column, ""), bridge_indexes)
                if not candidates:
                    unmatched_rows += 1
                else:
                    fsns = unique_fsns_from_rows(candidates)
                    if len(fsns) > 1:
                        ambiguous_rows += 1
        if unmatched_rows:
            suspected_risks.append(f"{unmatched_rows} rows do not resolve to the current FSN bridge by SKU")
        if ambiguous_rows:
            suspected_risks.append(f"{ambiguous_rows} rows resolve to multiple FSNs by SKU")
        if raw_rows == 0:
            suspected_risks.append("ADS.csv has no data rows after header detection")
    else:
        status = "ERROR"
        suspected_risks.append("ADS.csv not found")

    payload = {
        "status": status,
        "ads_file_found": ads_file_found,
        "raw_rows": raw_rows,
        "columns": columns,
        "detected_sku_column": detected_sku_column,
        "detected_fsn_column": detected_fsn_column,
        "detected_campaign_columns": detected_campaign_columns,
        "detected_views_column": detected_views_column,
        "detected_clicks_column": detected_clicks_column,
        "detected_units_column": detected_units_column,
        "detected_revenue_column": detected_revenue_column,
        "detected_roi_column": detected_roi_column,
        "detected_spend_column": detected_spend_column,
        "sample_rows": sample,
        "mapping_possible_by_fsn": mapping_possible_by_fsn,
        "mapping_possible_by_sku": mapping_possible_by_sku,
        "suspected_risks": suspected_risks,
    }

    ADS_DIAGNOSTIC_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    ADS_DIAGNOSTIC_JSON_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    append_log(
        ADS_DIAGNOSTIC_LOG_PATH,
        [
            "timestamp",
            "run_id",
            "status",
            "ads_file_found",
            "raw_rows",
            "detected_sku_column",
            "detected_fsn_column",
            "mapping_possible_by_fsn",
            "mapping_possible_by_sku",
            "risk_count",
            "log_path",
            "message",
        ],
        {
            "timestamp": now_iso(),
            "run_id": run_id,
            "status": status,
            "ads_file_found": ads_file_found,
            "raw_rows": raw_rows,
            "detected_sku_column": detected_sku_column,
            "detected_fsn_column": detected_fsn_column,
            "mapping_possible_by_fsn": mapping_possible_by_fsn,
            "mapping_possible_by_sku": mapping_possible_by_sku,
            "risk_count": len(suspected_risks),
            "log_path": str(ADS_DIAGNOSTIC_LOG_PATH),
            "message": "Diagnosed Flipkart ADS.csv structure" if ads_file_found else "ADS.csv not found",
        },
    )

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
