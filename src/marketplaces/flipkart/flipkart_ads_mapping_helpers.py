from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    RAW_INPUT_DIR,
    append_csv_log,
    clean_fsn,
    detect_header_row,
    ensure_directories,
    format_decimal,
    likely_columns,
    load_json,
    load_synonyms,
    normalize_key,
    normalize_text,
    parse_float,
    read_csv_rows,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ADS_RAW_PATH = RAW_INPUT_DIR / "ADS.csv"
FSN_BRIDGE_PATH = OUTPUT_DIR / "flipkart_fsn_bridge.csv"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
ADS_PLANNER_TAB = "FLIPKART_ADS_PLANNER"
ADS_MASTER_TAB = "FLIPKART_ADS_MASTER"
ADS_MAPPING_ISSUES_TAB = "FLIPKART_ADS_MAPPING_ISSUES"
ADS_SUMMARY_TAB = "FLIPKART_ADS_SUMMARY_BY_FSN"

ADS_DIAGNOSTIC_JSON_PATH = OUTPUT_DIR / "flipkart_ads_report_diagnostic.json"
ADS_DIAGNOSTIC_LOG_PATH = LOG_DIR / "flipkart_ads_report_diagnostic_log.csv"
ADS_MAPPING_LOG_PATH = LOG_DIR / "flipkart_ads_mapping_log.csv"
LOCAL_ADS_MASTER_PATH = OUTPUT_DIR / "flipkart_ads_master.csv"
LOCAL_ADS_MAPPING_ISSUES_PATH = OUTPUT_DIR / "flipkart_ads_mapping_issues.csv"
LOCAL_ADS_SUMMARY_PATH = OUTPUT_DIR / "flipkart_ads_summary_by_fsn.csv"
LOCAL_ADS_PLANNER_PATH = OUTPUT_DIR / "flipkart_ads_planner.csv"
SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

ADS_MASTER_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Campaign_ID",
    "Campaign_Name",
    "AdGroup_ID",
    "AdGroup_Name",
    "Views",
    "Clicks",
    "Direct_Units_Sold",
    "Indirect_Units_Sold",
    "Total_Units_Sold",
    "Total_Revenue",
    "ROI",
    "Estimated_Ad_Spend",
    "ROAS",
    "ACOS",
    "Mapping_Method",
    "Mapping_Confidence",
    "Source_File",
    "Last_Updated",
]

ADS_MAPPING_ISSUES_HEADERS = [
    "Run_ID",
    "Raw_SKU",
    "Raw_FSN",
    "Raw_Product_Name",
    "Campaign_ID",
    "Campaign_Name",
    "Issue_Type",
    "Issue_Detail",
    "Possible_FSNs",
    "Possible_SKUs",
    "Raw_Row_JSON",
    "Source_File",
    "Last_Updated",
]

ADS_SUMMARY_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Ad_Rows",
    "Campaign_Count",
    "Views",
    "Clicks",
    "CTR",
    "Total_Units_Sold",
    "Total_Revenue",
    "Estimated_Ad_Spend",
    "ROAS",
    "ACOS",
    "Mapping_Confidence",
    "Spend_Source",
    "Last_Updated",
]

ADS_PLANNER_AD_COLUMNS = [
    "Current_Ad_Status",
    "Ad_Rows",
    "Ad_Campaign_Count",
    "Ad_Views",
    "Ad_Clicks",
    "Ad_CTR",
    "Ad_Revenue",
    "Estimated_Ad_Spend",
    "Ad_ROAS",
    "Ad_ACOS",
    "Ads_Mapping_Status",
    "Ads_Data_Confidence",
    "Ads_Performance_Note",
]

ADS_FIELD_KEYS = [
    "fsn",
    "sku_id",
    "campaign_id",
    "campaign_name",
    "adgroup_id",
    "adgroup_name",
    "views",
    "clicks",
    "direct_units_sold",
    "indirect_units_sold",
    "total_revenue",
    "roi",
    "estimated_ad_spend",
    "roas",
    "acos",
    "product_name",
]


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


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def run_id() -> str:
    return f"FLIPKART_ADS_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return load_json(path)


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    return any(normalize_text(sheet.get("properties", {}).get("title", "")) == tab_name for sheet in metadata.get("sheets", []))


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return int(props["sheetId"])
    response = retry(
        lambda: sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        )
        .execute()
    )
    return int(response["replies"][0]["addSheet"]["properties"]["sheetId"])


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def clear_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .clear(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:ZZ", body={})
        .execute()
    )


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be at least 1")
    result: List[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def write_rows(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    values = [list(headers)] + [[row.get(header, "") for header in headers] for row in rows]
    end_col = column_index_to_a1(len(headers))
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:{end_col}{len(values)}",
            valueInputOption="RAW",
            body={"values": values},
        )
        .execute()
    )


def freeze_bold_resize(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int) -> None:
    requests = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": column_count,
                }
            }
        },
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def write_output_tab(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
    sheet_id: int,
) -> None:
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_bold_resize(sheets_service, spreadsheet_id, sheet_id, len(headers))


def write_local_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def load_csv_table(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        parsed_rows = [row for row in csv.reader(handle) if any(normalize_text(cell) for cell in row)]
    if not parsed_rows:
        return [], []
    header_row_index, _, _ = detect_header_row(parsed_rows, load_synonyms(), max_scan_rows=min(25, len(parsed_rows)))
    headers = [normalize_text(cell) for cell in parsed_rows[header_row_index]]
    data: List[Dict[str, str]] = []
    for row in parsed_rows[header_row_index + 1 :]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def detect_ads_columns(headers: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    synonyms = load_synonyms()
    return likely_columns(headers, synonyms, ADS_FIELD_KEYS)


def detected_header_name(detected_columns: Dict[str, Dict[str, Any]], key: str) -> str:
    return normalize_text(detected_columns.get(key, {}).get("header", ""))


def row_value(row: Dict[str, Any], column_name: str) -> str:
    if not column_name:
        return ""
    return normalize_text(row.get(column_name, ""))


def row_to_json(row: Dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False)


def normalize_sku_key(value: Any) -> str:
    text = normalize_text(value).lower()
    if not text:
        return ""
    return text


def build_bridge_indexes(rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    fsn_index: Dict[str, Dict[str, str]] = {}
    sku_exact_index: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    sku_key_index: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        sku = normalize_text(row.get("Seller_SKU", ""))
        if fsn:
            fsn_index[fsn] = dict(row)
        if sku:
            sku_exact_index[sku.lower()].append(dict(row))
            sku_key_index[normalize_key(sku)].append(dict(row))

    return {
        "fsn_index": fsn_index,
        "sku_exact_index": sku_exact_index,
        "sku_key_index": sku_key_index,
    }


def resolve_sku_candidates(sku_value: Any, bridge_indexes: Dict[str, Any]) -> Tuple[List[Dict[str, str]], str]:
    sku_text = normalize_text(sku_value)
    if not sku_text:
        return [], ""
    exact_matches = bridge_indexes["sku_exact_index"].get(sku_text.lower(), [])
    if exact_matches:
        return list(exact_matches), "exact"
    key_matches = bridge_indexes["sku_key_index"].get(normalize_key(sku_text), [])
    if key_matches:
        return list(key_matches), "key"
    return [], ""


def unique_fsns_from_rows(rows: Sequence[Dict[str, str]]) -> List[str]:
    seen: List[str] = []
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in seen:
            seen.append(fsn)
    return seen


def format_int(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    number = parse_float(text)
    if number == 0 and text not in {"0", "0.0", "0.00", "0.0000"}:
        return text
    return str(int(round(number)))


def format_metric(value: Any, decimals: int = 2) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return format_decimal(value, decimals)


def compute_row_metrics(row: Dict[str, Any], detected: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    views_header = detected_header_name(detected, "views")
    clicks_header = detected_header_name(detected, "clicks")
    direct_units_header = detected_header_name(detected, "direct_units_sold")
    indirect_units_header = detected_header_name(detected, "indirect_units_sold")
    revenue_header = detected_header_name(detected, "total_revenue")
    roi_header = detected_header_name(detected, "roi")
    spend_header = detected_header_name(detected, "estimated_ad_spend")
    roas_header = detected_header_name(detected, "roas")
    acos_header = detected_header_name(detected, "acos")

    views = parse_float(row.get(views_header, "")) if views_header else 0.0
    clicks = parse_float(row.get(clicks_header, "")) if clicks_header else 0.0
    direct_units = parse_float(row.get(direct_units_header, "")) if direct_units_header else 0.0
    indirect_units = parse_float(row.get(indirect_units_header, "")) if indirect_units_header else 0.0
    revenue = parse_float(row.get(revenue_header, "")) if revenue_header else 0.0
    roi = parse_float(row.get(roi_header, "")) if roi_header else 0.0

    source_spend_text = normalize_text(row.get(spend_header, "")) if spend_header else ""
    spend = parse_float(source_spend_text) if source_spend_text else 0.0
    spend_source = ""
    if source_spend_text:
        spend_source = "Source Spend"
    elif revenue > 0 and roi > 0:
        spend = revenue / roi
        spend_source = "Estimated From ROI"

    roas = 0.0
    if spend > 0 and revenue > 0:
        roas = revenue / spend
    elif roi > 0:
        roas = roi
    elif roas_header:
        roas = parse_float(row.get(roas_header, ""))

    acos = 0.0
    if spend > 0 and revenue > 0:
        acos = spend / revenue
    elif acos_header:
        acos = parse_float(row.get(acos_header, ""))

    return {
        "views": views,
        "clicks": clicks,
        "direct_units": direct_units,
        "indirect_units": indirect_units,
        "total_units": direct_units + indirect_units,
        "revenue": revenue,
        "roi": roi,
        "spend": spend,
        "spend_source": spend_source,
        "roas": roas,
        "acos": acos,
    }


def format_raw_ads_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: row.get(key, "") for key in row.keys()}


def load_spreadsheet_id() -> str:
    meta = load_json_file(SPREADSHEET_META_PATH)
    return meta["spreadsheet_id"]


def build_sheets_service():
    return build_services()


def issue_sort_key(issue_type: str) -> Tuple[int, str]:
    priority = {
        "SKU Maps To Multiple FSNs": 0,
        "No Matching FSN": 1,
        "No Mapping Key": 2,
    }
    return priority.get(issue_type, 99), issue_type


def ordered_counter(rows: Sequence[Dict[str, Any]], field_name: str) -> Dict[str, int]:
    counter = Counter()
    for row in rows:
        value = normalize_text(row.get(field_name, "")) or "Unknown"
        counter[value] += 1
    ordered: Dict[str, int] = {}
    for key in sorted(counter, key=lambda item: (-counter[item], item)):
        ordered[key] = counter[key]
    return ordered


def append_log(path: Path, headers: Sequence[str], row: Dict[str, Any]) -> None:
    append_csv_log(path, headers, [row])
