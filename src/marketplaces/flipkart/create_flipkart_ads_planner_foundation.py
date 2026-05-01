from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_cogs_helpers import count_cogs_rows, hydrate_analysis_rows
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    append_csv_log,
    build_status_payload,
    clean_fsn,
    ensure_directories,
    format_decimal,
    normalize_text,
    now_iso,
    parse_float,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_ads_planner_foundation_log.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
RETURN_TYPE_PIVOT_TAB = "FLIPKART_RETURN_TYPE_PIVOT"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"

PRODUCT_AD_PROFILE_TAB = "FLIPKART_PRODUCT_AD_PROFILE"
KEYWORD_SEEDS_TAB = "GOOGLE_ADS_KEYWORD_SEEDS"
KEYWORD_CACHE_TAB = "GOOGLE_KEYWORD_METRICS_CACHE"
DEMAND_PROFILE_TAB = "PRODUCT_TYPE_DEMAND_PROFILE"
ADS_PLANNER_TAB = "FLIPKART_ADS_PLANNER"

LOCAL_PRODUCT_AD_PROFILE_PATH = OUTPUT_DIR / "flipkart_product_ad_profile.csv"
LOCAL_KEYWORD_SEEDS_PATH = OUTPUT_DIR / "google_ads_keyword_seeds.csv"
LOCAL_KEYWORD_CACHE_PATH = OUTPUT_DIR / "google_keyword_metrics_cache.csv"
LOCAL_DEMAND_PROFILE_PATH = OUTPUT_DIR / "product_type_demand_profile.csv"
LOCAL_ADS_PLANNER_PATH = OUTPUT_DIR / "flipkart_ads_planner.csv"

PRODUCT_AD_PROFILE_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Category",
    "Detected_Product_Type",
    "Manual_Product_Type",
    "Final_Product_Type",
    "Detected_Seasonality_Tag",
    "Manual_Seasonality_Tag",
    "Final_Seasonality_Tag",
    "Ad_Run_Type",
    "Classification_Confidence",
    "Classification_Source",
    "Manual_Override_Remarks",
    "Last_Updated",
]

KEYWORD_SEEDS_HEADERS = [
    "Product_Type",
    "Seed_Keyword",
    "Geo",
    "Language",
    "Intent_Type",
    "Priority",
    "Manual_Status",
    "Last_Updated",
]

KEYWORD_CACHE_HEADERS = [
    "Keyword",
    "Product_Type",
    "Geo",
    "Language",
    "Avg_Monthly_Searches",
    "Competition",
    "Competition_Index",
    "Low_Top_Page_Bid",
    "High_Top_Page_Bid",
    "Monthly_Search_Jan",
    "Monthly_Search_Feb",
    "Monthly_Search_Mar",
    "Monthly_Search_Apr",
    "Monthly_Search_May",
    "Monthly_Search_Jun",
    "Monthly_Search_Jul",
    "Monthly_Search_Aug",
    "Monthly_Search_Sep",
    "Monthly_Search_Oct",
    "Monthly_Search_Nov",
    "Monthly_Search_Dec",
    "Last_Refreshed",
    "Source",
    "Cache_Status",
    "Remarks",
]

DEMAND_PROFILE_HEADERS = [
    "Product_Type",
    "Seasonality_Tag",
    "Peak_Months",
    "Prep_Start_Days_Before_Peak",
    "Ads_Start_Days_Before_Peak",
    "Demand_Confidence",
    "Demand_Source",
    "Recommended_Ad_Window",
    "Remarks",
    "Last_Updated",
]

ADS_PLANNER_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Final_Product_Type",
    "Final_Seasonality_Tag",
    "Ad_Run_Type",
    "COGS_Readiness",
    "Profit_Readiness",
    "Return_Readiness",
    "Listing_Readiness",
    "Data_Readiness",
    "Alert_Readiness",
    "Ads_Readiness_Status",
    "Suggested_Ad_Action",
    "Suggested_Budget_Level",
    "Start_Preparation_Date",
    "Start_Ads_Date",
    "Stop_Ads_Date",
    "Reason",
    "Confidence",
    "Manual_Override",
    "Last_Updated",
]

TAB_OUTPUTS = {
    PRODUCT_AD_PROFILE_TAB: LOCAL_PRODUCT_AD_PROFILE_PATH,
    KEYWORD_SEEDS_TAB: LOCAL_KEYWORD_SEEDS_PATH,
    KEYWORD_CACHE_TAB: LOCAL_KEYWORD_CACHE_PATH,
    DEMAND_PROFILE_TAB: LOCAL_DEMAND_PROFILE_PATH,
    ADS_PLANNER_TAB: LOCAL_ADS_PLANNER_PATH,
}

PRODUCT_TYPE_ORDER = [
    "Rice/Fairy/Jhalar Light",
    "String Light",
    "Rope Light",
    "Strip Light",
    "Flood Light",
    "Gate/Wall/Post Light",
    "DJ/Event Light",
    "Unknown",
]

PRODUCT_TYPE_RULES: List[Tuple[str, Sequence[str]]] = [
    ("Rice/Fairy/Jhalar Light", ("rice", "fairy", "jhalar", "curtain", "diwali", "serial light")),
    ("String Light", ("string light", "decorative string")),
    ("Rope Light", ("rope light", "tube rope")),
    ("Strip Light", ("strip light", "led strip", "rgb strip")),
    ("Flood Light", ("flood light", "lens flood", "50w flood", "100w flood", "150w flood")),
    ("Gate/Wall/Post Light", ("gate light", "post light", "wall light", "outdoor lamp", "cube", "brick")),
    ("DJ/Event Light", ("dj", "disco", "party", "par light", "stage")),
]

SEASONALITY_BY_PRODUCT_TYPE = {
    "Rice/Fairy/Jhalar Light": "Diwali Heavy",
    "String Light": "Festive Boost",
    "Rope Light": "Year-Round + Festive Boost",
    "Strip Light": "Year-Round",
    "Flood Light": "Utility / Outdoor",
    "Gate/Wall/Post Light": "Year-Round Home Exterior",
    "DJ/Event Light": "Wedding / Event",
    "Unknown": "Unknown",
}

AD_RUN_TYPE_BY_SEASONALITY = {
    "Diwali Heavy": "Seasonal",
    "Festive Boost": "Seasonal + Test",
    "Year-Round": "Always-On Eligible",
    "Utility / Outdoor": "Always-On Test",
    "Year-Round Home Exterior": "Always-On Eligible",
    "Year-Round + Festive Boost": "Always-On Eligible",
    "Wedding / Event": "Seasonal/Event",
    "Unknown": "Manual Review",
}

DEFAULT_KEYWORD_SEEDS: Dict[str, List[str]] = {
    "Rice/Fairy/Jhalar Light": ["rice light", "diwali light", "jhalar light", "fairy light", "curtain light"],
    "String Light": ["string light", "led string light", "decorative string light"],
    "Rope Light": ["rope light", "led rope light", "outdoor rope light"],
    "Strip Light": ["strip light", "led strip light", "rgb strip light"],
    "Flood Light": ["led flood light", "outdoor flood light", "50w flood light", "100w flood light"],
    "Gate/Wall/Post Light": ["gate light", "outdoor wall light", "post light", "garden wall light"],
    "DJ/Event Light": ["dj light", "disco light", "party light", "stage light"],
}

DEFAULT_DEMAND_PROFILES: Dict[str, Dict[str, Any]] = {
    "Rice/Fairy/Jhalar Light": {
        "Seasonality_Tag": "Diwali Heavy",
        "Peak_Months": "Sep, Oct, Nov",
        "Prep_Start_Days_Before_Peak": 45,
        "Ads_Start_Days_Before_Peak": 25,
        "Demand_Confidence": "Medium",
        "Demand_Source": "Manual Default",
        "Recommended_Ad_Window": "Prep 45 days before Sep-Oct-Nov peak; start ads 25 days before peak",
        "Remarks": "",
    },
    "String Light": {
        "Seasonality_Tag": "Festive Boost",
        "Peak_Months": "Sep, Oct, Nov, Dec",
        "Prep_Start_Days_Before_Peak": 30,
        "Ads_Start_Days_Before_Peak": 20,
        "Demand_Confidence": "Medium",
        "Demand_Source": "Manual Default",
        "Recommended_Ad_Window": "Prep 30 days before festive peak; start ads 20 days before peak",
        "Remarks": "",
    },
    "Rope Light": {
        "Seasonality_Tag": "Year-Round + Festive Boost",
        "Peak_Months": "All Year + Sep-Nov Boost",
        "Prep_Start_Days_Before_Peak": 20,
        "Ads_Start_Days_Before_Peak": 15,
        "Demand_Confidence": "Medium",
        "Demand_Source": "Manual Default",
        "Recommended_Ad_Window": "Always-on base with Sep-Nov boost; prep 20 days before boost",
        "Remarks": "",
    },
    "Strip Light": {
        "Seasonality_Tag": "Year-Round",
        "Peak_Months": "All Year",
        "Prep_Start_Days_Before_Peak": 15,
        "Ads_Start_Days_Before_Peak": 10,
        "Demand_Confidence": "Medium",
        "Demand_Source": "Manual Default",
        "Recommended_Ad_Window": "Always-on year-round; prep 15 days before any promo",
        "Remarks": "",
    },
    "Flood Light": {
        "Seasonality_Tag": "Utility / Outdoor",
        "Peak_Months": "All Year",
        "Prep_Start_Days_Before_Peak": 15,
        "Ads_Start_Days_Before_Peak": 10,
        "Demand_Confidence": "Medium",
        "Demand_Source": "Manual Default",
        "Recommended_Ad_Window": "Always-on utility demand; prep 15 days before promo",
        "Remarks": "",
    },
    "Gate/Wall/Post Light": {
        "Seasonality_Tag": "Year-Round Home Exterior",
        "Peak_Months": "All Year",
        "Prep_Start_Days_Before_Peak": 15,
        "Ads_Start_Days_Before_Peak": 10,
        "Demand_Confidence": "Medium",
        "Demand_Source": "Manual Default",
        "Recommended_Ad_Window": "Always-on home exterior demand; prep 15 days before promo",
        "Remarks": "",
    },
    "DJ/Event Light": {
        "Seasonality_Tag": "Wedding / Event",
        "Peak_Months": "Oct, Nov, Dec, Jan, Feb",
        "Prep_Start_Days_Before_Peak": 30,
        "Ads_Start_Days_Before_Peak": 20,
        "Demand_Confidence": "Medium",
        "Demand_Source": "Manual Default",
        "Recommended_Ad_Window": "Prep 30 days before wedding/event peak; start ads 20 days before peak",
        "Remarks": "",
    },
}

MONTH_ABBRS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

MONTH_PATTERN = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b", re.IGNORECASE)
SEVERITY_ORDER = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}


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


def pick_first_nonblank(*values: Any) -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return ""


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def find_sheet_id(sheets_service, spreadsheet_id: str, tab_name: str) -> Optional[int]:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return int(props["sheetId"])
    return None


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    sheet_id = find_sheet_id(sheets_service, spreadsheet_id, tab_name)
    if sheet_id is not None:
        return sheet_id
    response = retry(
        lambda: sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        )
        .execute()
    )
    return int(response["replies"][0]["addSheet"]["properties"]["sheetId"])


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    return find_sheet_id(sheets_service, spreadsheet_id, tab_name) is not None


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


def merge_blank_values(existing_row: Dict[str, Any], default_row: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(default_row)
    for key, value in existing_row.items():
        if normalize_text(value):
            merged[key] = value
    return merged


def row_key(row: Dict[str, Any], fields: Sequence[str]) -> Tuple[str, ...]:
    return tuple(normalize_text(row.get(field, "")) for field in fields)


def build_analysis_index(rows: Sequence[Dict[str, str]]) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    ordered_fsns: List[str] = []
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in indexed:
            continue
        indexed[fsn] = dict(row)
        ordered_fsns.append(fsn)
    return ordered_fsns, indexed


def detect_product_type(product_title: str, sku_id: str) -> Tuple[str, List[str]]:
    haystack = f"{normalize_text(product_title)} {normalize_text(sku_id)}".lower()
    for product_type, keywords in PRODUCT_TYPE_RULES:
        matches = [keyword for keyword in keywords if keyword in haystack]
        if matches:
            return product_type, matches
    return "Unknown", []


def classification_confidence(detected_type: str, matches: Sequence[str]) -> str:
    if detected_type == "Unknown":
        return "Low"
    if len(matches) >= 2:
        return "High"
    return "Medium"


def build_product_profile_rows(
    ordered_fsns: Sequence[str],
    analysis_rows: Dict[str, Dict[str, str]],
    existing_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, Any]]:
    existing_index = {clean_fsn(row.get("FSN", "")): row for row in existing_rows if clean_fsn(row.get("FSN", ""))}
    profile_rows: List[Dict[str, Any]] = []
    for fsn in ordered_fsns:
        analysis_row = analysis_rows.get(fsn, {})
        existing_row = existing_index.get(fsn, {})
        product_title = normalize_text(analysis_row.get("Product_Title", ""))
        sku_id = normalize_text(analysis_row.get("SKU_ID", ""))
        category = normalize_text(analysis_row.get("Category", ""))
        detected_type, matches = detect_product_type(product_title, sku_id)
        detected_seasonality = SEASONALITY_BY_PRODUCT_TYPE.get(detected_type, "Unknown")
        manual_product_type = normalize_text(existing_row.get("Manual_Product_Type", ""))
        manual_seasonality_tag = normalize_text(existing_row.get("Manual_Seasonality_Tag", ""))
        manual_override_remarks = normalize_text(existing_row.get("Manual_Override_Remarks", ""))
        final_product_type = manual_product_type or detected_type
        final_seasonality_tag = manual_seasonality_tag or detected_seasonality
        ad_run_type = AD_RUN_TYPE_BY_SEASONALITY.get(final_seasonality_tag, "Manual Review")
        confidence = classification_confidence(detected_type, matches)
        source_parts = ["Title/SKU Keywords" if matches else "Rules Engine"]
        if manual_product_type or manual_seasonality_tag or manual_override_remarks:
            source_parts.insert(0, "Manual Override")
        profile_rows.append(
            {
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": product_title,
                "Category": category,
                "Detected_Product_Type": detected_type,
                "Manual_Product_Type": manual_product_type,
                "Final_Product_Type": final_product_type,
                "Detected_Seasonality_Tag": detected_seasonality,
                "Manual_Seasonality_Tag": manual_seasonality_tag,
                "Final_Seasonality_Tag": final_seasonality_tag,
                "Ad_Run_Type": ad_run_type,
                "Classification_Confidence": confidence,
                "Classification_Source": " + ".join(source_parts),
                "Manual_Override_Remarks": manual_override_remarks,
                "Last_Updated": now_iso(),
            }
        )
    return profile_rows


def build_default_keyword_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for product_type in PRODUCT_TYPE_ORDER:
        if product_type == "Unknown":
            continue
        keywords = DEFAULT_KEYWORD_SEEDS.get(product_type, [])
        for priority, keyword in enumerate(keywords, start=1):
            rows.append(
                {
                    "Product_Type": product_type,
                    "Seed_Keyword": keyword,
                    "Geo": "India",
                    "Language": "English/Hindi",
                    "Intent_Type": "Commercial",
                    "Priority": str(priority),
                    "Manual_Status": "Seeded",
                    "Last_Updated": now_iso(),
                }
            )
    return rows


def build_keyword_seed_rows(existing_rows: Sequence[Dict[str, str]]) -> List[Dict[str, Any]]:
    key_fields = ("Product_Type", "Seed_Keyword", "Geo", "Language")
    existing_index: Dict[Tuple[str, ...], Dict[str, str]] = {}
    for row in existing_rows:
        key = row_key(row, key_fields)
        if any(not value for value in key):
            continue
        existing_index.setdefault(key, dict(row))

    final_map: Dict[Tuple[str, ...], Dict[str, Any]] = dict(existing_index)
    for default_row in build_default_keyword_rows():
        key = row_key(default_row, key_fields)
        if key in final_map:
            final_map[key] = merge_blank_values(final_map[key], default_row)
        else:
            final_map[key] = default_row

    output_rows = list(final_map.values())
    output_rows.sort(
        key=lambda row: (
            PRODUCT_TYPE_ORDER.index(normalize_text(row.get("Product_Type", ""))) if normalize_text(row.get("Product_Type", "")) in PRODUCT_TYPE_ORDER else len(PRODUCT_TYPE_ORDER),
            parse_float(row.get("Priority", "")),
            normalize_text(row.get("Seed_Keyword", "")),
            normalize_text(row.get("Geo", "")),
            normalize_text(row.get("Language", "")),
        )
    )
    return output_rows


def build_keyword_cache_rows(
    keyword_seed_rows: Sequence[Dict[str, Any]],
    existing_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, Any]]:
    key_fields = ("Keyword", "Product_Type", "Geo", "Language")
    existing_index: Dict[Tuple[str, ...], Dict[str, str]] = {}
    for row in existing_rows:
        key = row_key(row, key_fields)
        if any(not value for value in key):
            continue
        existing_index.setdefault(key, dict(row))

    for seed_row in keyword_seed_rows:
        cache_row = {
            "Keyword": normalize_text(seed_row.get("Seed_Keyword", "")),
            "Product_Type": normalize_text(seed_row.get("Product_Type", "")),
            "Geo": normalize_text(seed_row.get("Geo", "")) or "India",
            "Language": normalize_text(seed_row.get("Language", "")) or "English/Hindi",
            "Avg_Monthly_Searches": "",
            "Competition": "",
            "Competition_Index": "",
            "Low_Top_Page_Bid": "",
            "High_Top_Page_Bid": "",
            "Monthly_Search_Jan": "",
            "Monthly_Search_Feb": "",
            "Monthly_Search_Mar": "",
            "Monthly_Search_Apr": "",
            "Monthly_Search_May": "",
            "Monthly_Search_Jun": "",
            "Monthly_Search_Jul": "",
            "Monthly_Search_Aug": "",
            "Monthly_Search_Sep": "",
            "Monthly_Search_Oct": "",
            "Monthly_Search_Nov": "",
            "Monthly_Search_Dec": "",
            "Last_Refreshed": "",
            "Source": "Seed Library",
            "Cache_Status": "Pending",
            "Remarks": "Awaiting Google Ads API or CSV import",
        }
        key = row_key(cache_row, key_fields)
        if key in existing_index:
            existing_index[key] = merge_blank_values(existing_index[key], cache_row)
        else:
            existing_index[key] = cache_row

    output_rows = list(existing_index.values())
    output_rows.sort(
        key=lambda row: (
            PRODUCT_TYPE_ORDER.index(normalize_text(row.get("Product_Type", ""))) if normalize_text(row.get("Product_Type", "")) in PRODUCT_TYPE_ORDER else len(PRODUCT_TYPE_ORDER),
            normalize_text(row.get("Keyword", "")),
            normalize_text(row.get("Geo", "")),
            normalize_text(row.get("Language", "")),
        )
    )
    return output_rows


def build_demand_profile_rows(existing_rows: Sequence[Dict[str, str]]) -> List[Dict[str, Any]]:
    existing_index: Dict[str, Dict[str, str]] = {}
    for row in existing_rows:
        product_type = normalize_text(row.get("Product_Type", ""))
        if not product_type:
            continue
        existing_index.setdefault(product_type, dict(row))

    final_map: Dict[str, Dict[str, Any]] = dict(existing_index)
    for product_type, default_row in DEFAULT_DEMAND_PROFILES.items():
        row = {
            "Product_Type": product_type,
            "Seasonality_Tag": default_row["Seasonality_Tag"],
            "Peak_Months": default_row["Peak_Months"],
            "Prep_Start_Days_Before_Peak": str(default_row["Prep_Start_Days_Before_Peak"]),
            "Ads_Start_Days_Before_Peak": str(default_row["Ads_Start_Days_Before_Peak"]),
            "Demand_Confidence": default_row["Demand_Confidence"],
            "Demand_Source": default_row["Demand_Source"],
            "Recommended_Ad_Window": default_row["Recommended_Ad_Window"],
            "Remarks": default_row["Remarks"],
            "Last_Updated": now_iso(),
        }
        if product_type in final_map:
            final_map[product_type] = merge_blank_values(final_map[product_type], row)
        else:
            final_map[product_type] = row

    output_rows = list(final_map.values())
    output_rows.sort(
        key=lambda row: (
            PRODUCT_TYPE_ORDER.index(normalize_text(row.get("Product_Type", ""))) if normalize_text(row.get("Product_Type", "")) in PRODUCT_TYPE_ORDER else len(PRODUCT_TYPE_ORDER),
            normalize_text(row.get("Product_Type", "")),
        )
    )
    return output_rows


def demand_profile_lookup(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        product_type = normalize_text(row.get("Product_Type", ""))
        if product_type and product_type not in lookup:
            lookup[product_type] = dict(row)
    return lookup


def parse_peak_months(text: str) -> List[str]:
    months: List[str] = []
    for match in MONTH_PATTERN.finditer(text or ""):
        month = match.group(1).title()
        if month not in months:
            months.append(month)
    return months


def month_start_for_year(month_abbr: str, year: int) -> date:
    return date(year, MONTH_ABBRS[month_abbr], 1)


def month_end_for_year(month_abbr: str, year: int) -> date:
    month = MONTH_ABBRS[month_abbr]
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def compute_ad_dates(product_type: str, seasonality_tag: str, demand_row: Dict[str, Any], today: date) -> Tuple[str, str, str]:
    product_type_norm = normalize_text(product_type)
    seasonality_norm = normalize_text(seasonality_tag)
    if not product_type_norm or product_type_norm not in DEFAULT_DEMAND_PROFILES:
        return "", "", ""

    if "Year-Round" in seasonality_norm or seasonality_norm in {"Utility / Outdoor", "Unknown"}:
        return today.isoformat(), today.isoformat(), ""

    peak_months = parse_peak_months(normalize_text(demand_row.get("Peak_Months", "")))
    if not peak_months:
        return "", "", ""

    prep_days = int(parse_float(demand_row.get("Prep_Start_Days_Before_Peak", "")))
    ads_days = int(parse_float(demand_row.get("Ads_Start_Days_Before_Peak", "")))
    first_month = peak_months[0]
    last_month = peak_months[-1]

    anchor_year = today.year if MONTH_ABBRS[first_month] >= today.month else today.year + 1
    anchor_date = month_start_for_year(first_month, anchor_year)
    start_prep_date = anchor_date - timedelta(days=prep_days)
    start_ads_date = anchor_date - timedelta(days=ads_days)
    stop_ads_date = month_end_for_year(last_month, anchor_year) + timedelta(days=7)
    return start_prep_date.isoformat(), start_ads_date.isoformat(), stop_ads_date.isoformat()


def classify_readiness(
    analysis_row: Dict[str, Any],
    return_row: Dict[str, Any],
    active_task_row: Dict[str, Any],
) -> Dict[str, str]:
    cogs_status = normalize_text(analysis_row.get("COGS_Status", ""))
    cogs_ready = "Ready" if cogs_status.upper() in {"ENTERED", "VERIFIED"} else "Missing"

    final_profit_margin_raw = normalize_text(analysis_row.get("Final_Profit_Margin", ""))
    final_net_profit = parse_float(analysis_row.get("Final_Net_Profit", ""))
    if not cogs_ready == "Ready":
        profit_readiness = "Unknown"
    else:
        final_profit_margin = parse_float(final_profit_margin_raw)
        if final_profit_margin >= 0.20:
            profit_readiness = "Strong"
        elif final_profit_margin < 0.10:
            profit_readiness = "Weak"
        else:
            profit_readiness = "Moderate"

    customer_return_rate = parse_float(return_row.get("Customer_Return_Rate", ""))
    courier_return_rate = parse_float(return_row.get("Courier_Return_Rate", ""))
    total_return_rate = parse_float(
        pick_first_nonblank(
            return_row.get("Total_Return_Rate", ""),
            analysis_row.get("Total_Return_Rate", ""),
            analysis_row.get("Return_Rate", ""),
        )
    )
    split_available = bool(
        normalize_text(return_row.get("Customer_Return_Rate", ""))
        or normalize_text(return_row.get("Courier_Return_Rate", ""))
    )
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

    listing_status = normalize_text(analysis_row.get("Listing_Status", ""))
    listing_readiness = "Bad" if any(token in listing_status.lower() for token in ("missing", "inactive", "blocked", "not active", "unlisted", "paused")) else "Good"

    data_confidence = normalize_text(analysis_row.get("Data_Confidence", "")).upper()
    data_readiness = "Good" if data_confidence == "HIGH" else "Review"

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
        "Final_Net_Profit": format_decimal(final_net_profit, 2) if normalize_text(analysis_row.get("Final_Net_Profit", "")) else "",
    }


def pick_active_task_row(rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    if not rows:
        return {}
    return max(
        (dict(row) for row in rows),
        key=lambda row: (
            -SEVERITY_ORDER.get(normalize_text(row.get("Severity", "")), 99),
            normalize_text(row.get("Last_Updated", "")),
        ),
    )


def build_ads_action(
    final_product_type: str,
    final_seasonality_tag: str,
    readiness: Dict[str, str],
    analysis_row: Dict[str, Any],
    return_row: Dict[str, Any],
    active_task_row: Dict[str, Any],
) -> Tuple[str, str, str]:
    cogs_missing = readiness["COGS_Readiness"] != "Ready"
    final_net_profit = parse_float(analysis_row.get("Final_Net_Profit", ""))
    final_profit_margin = parse_float(analysis_row.get("Final_Profit_Margin", ""))
    customer_return_rate = parse_float(return_row.get("Customer_Return_Rate", ""))
    courier_return_rate = parse_float(return_row.get("Courier_Return_Rate", ""))
    total_return_rate = parse_float(
        pick_first_nonblank(
            return_row.get("Total_Return_Rate", ""),
            analysis_row.get("Total_Return_Rate", ""),
            analysis_row.get("Return_Rate", ""),
        )
    )
    split_available = bool(
        normalize_text(return_row.get("Customer_Return_Rate", ""))
        or normalize_text(return_row.get("Courier_Return_Rate", ""))
    )
    if not split_available:
        return "Review Return Split", "Manual Review", "Return split missing; review manually"
    listing_status = normalize_text(analysis_row.get("Listing_Status", ""))
    active_severity = normalize_text(active_task_row.get("Severity", ""))
    product_type_norm = normalize_text(final_product_type)
    seasonality_norm = normalize_text(final_seasonality_tag)

    reasons: List[str] = []
    if cogs_missing:
        return "Fill COGS First", "Manual Review", "COGS missing"
    if final_net_profit < 0:
        return "Do Not Run Ads", "Do Not Run", "Final net profit is negative"
    if final_profit_margin < 0.10 and readiness["COGS_Readiness"] == "Ready" and not split_available:
        return "Do Not Run Ads / Improve Economics", "Do Not Run", "Final profit margin is below 10%"
    if split_available and customer_return_rate >= 0.20 and final_profit_margin < 0.10:
        return "Do Not Run Ads / Improve Product First", "Do Not Run", "Customer return rate is critical and margin is weak"
    if split_available and customer_return_rate >= 0.50:
        return "Fix Product First", "Do Not Run", "Customer return rate is critical"
    if split_available and customer_return_rate >= 0.20:
        return "Test Ads Carefully / Fix Product First", "Low Test", "Customer return rate is elevated"
    if split_available and customer_return_rate < 0.20 and courier_return_rate >= 0.20:
        return "Test Ads Carefully / Check Logistics", "Low Test", "Customer return rate acceptable; courier return risk elevated"
    if any(token in listing_status.lower() for token in ("missing", "inactive", "blocked", "not active", "unlisted", "paused")):
        return "Fix Product/Listing First", "Do Not Run", "Listing is missing or not active"
    if active_severity == "Critical":
        return "Resolve Critical Alert First", "Manual Review", "Critical active alert exists"

    if product_type_norm == "Unknown":
        return "Manual Review", "Manual Review", "Product type could not be classified"
    if seasonality_norm == "Diwali Heavy":
        return "Seasonal Ads Later / Prepare Listing First", "Scale Later", "Seasonal Diwali-heavy demand"
    if seasonality_norm in {"Year-Round", "Year-Round Home Exterior"}:
        return "Test Ads", "Low Test", "Year-round product and otherwise ready"
    if seasonality_norm == "Utility / Outdoor":
        return "Always-On Test", "Medium Test", "Utility or outdoor demand and otherwise ready"
    if seasonality_norm == "Wedding / Event":
        return "Seasonal/Event Test", "Low Test", "Wedding or event demand and otherwise ready"
    if seasonality_norm == "Festive Boost":
        return "Seasonal Ads Later / Prepare Listing First", "Medium Test", "Festive boost demand and otherwise ready"
    if "Year-Round" in seasonality_norm and "Festive Boost" in seasonality_norm:
        return "Test Ads", "Medium Test", "Year-round base with festive boost"

    if split_available and customer_return_rate < 0.15 and readiness["Profit_Readiness"] == "Strong" and ads_metrics["ads_data_available"] and safe_float(ads_metrics["ad_acos"]) <= 0.20 and safe_float(ads_metrics["ad_roas"]) >= 5:
        return "Scale Ads", "Medium Test", "Customer return rate is low and mapped ads performance is strong"
    if split_available and customer_return_rate < 0.15 and readiness["Profit_Readiness"] == "Strong" and ads_metrics["ads_data_available"]:
        return "Continue / Optimize Ads", "Medium Test", "Customer return rate is low and ads are healthy"
    if readiness["Alert_Readiness"] == "Review":
        reasons.append("High active alert exists")
    if readiness["Data_Readiness"] == "Review":
        reasons.append("Data confidence is not HIGH")
    if not reasons:
        reasons.append("Monitor internal readiness")
    if split_available and courier_return_rate >= 0.20:
        reasons.insert(0, "Customer return rate acceptable; courier return risk elevated")
    elif split_available and customer_return_rate >= 0.20:
        reasons.insert(0, "Customer return rate elevated")
    return "Monitor", "Manual Review", "; ".join(reasons)


def readiness_status(suggested_action: str, readiness: Dict[str, str]) -> str:
    if suggested_action in {
        "Fill COGS First",
        "Do Not Run Ads",
        "Do Not Run Ads / Improve Economics",
        "Do Not Run Ads / Improve Product First",
        "Fix Product First",
        "Fix Product/Listing First",
        "Fix Listing First",
        "Resolve Critical Alert First",
        "Review Return Split",
    }:
        return "Blocked"
    if suggested_action in {"Seasonal Ads Later / Prepare Listing First"}:
        return "Prepare"
    if suggested_action in {"Test Ads", "Always-On Test", "Seasonal/Event Test"}:
        return "Ready"
    if suggested_action == "Manual Review":
        return "Review"
    if readiness["Alert_Readiness"] == "Review" or readiness["Data_Readiness"] == "Review":
        return "Review"
    return "Monitor"


def recommendation_confidence(
    final_product_type: str,
    readiness: Dict[str, str],
    suggested_action: str,
    manual_override: str,
) -> str:
    if manual_override:
        return "Manual"
    if final_product_type == "Unknown":
        return "Low"
    if suggested_action in {"Do Not Run Ads", "Do Not Run Ads / Improve Economics", "Do Not Run Ads / Improve Product First", "Fix Product First", "Fix Product/Listing First", "Fix Listing First", "Resolve Critical Alert First", "Review Return Split"}:
        return "Low"
    if readiness["COGS_Readiness"] == "Ready" and readiness["Profit_Readiness"] == "Strong" and readiness["Return_Readiness"] == "Good" and readiness["Listing_Readiness"] == "Good" and readiness["Alert_Readiness"] == "Good" and readiness["Data_Readiness"] == "Good":
        return "High"
    if readiness["COGS_Readiness"] == "Ready":
        return "Medium"
    return "Low"


def build_ads_planner_rows(
    ordered_fsns: Sequence[str],
    analysis_rows: Dict[str, Dict[str, str]],
    product_profile_rows: Sequence[Dict[str, Any]],
    return_summary_rows: Sequence[Dict[str, str]],
    active_tasks_rows: Sequence[Dict[str, str]],
    existing_rows: Sequence[Dict[str, str]],
    demand_profile_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return_lookup = {clean_fsn(row.get("FSN", "")): dict(row) for row in return_summary_rows if clean_fsn(row.get("FSN", ""))}
    profile_lookup = {clean_fsn(row.get("FSN", "")): dict(row) for row in product_profile_rows if clean_fsn(row.get("FSN", ""))}
    active_task_group: Dict[str, List[Dict[str, str]]] = {}
    for row in active_tasks_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        active_task_group.setdefault(fsn, []).append(dict(row))
    existing_index = {clean_fsn(row.get("FSN", "")): row for row in existing_rows if clean_fsn(row.get("FSN", ""))}
    demand_lookup = demand_profile_lookup(demand_profile_rows)

    planner_rows: List[Dict[str, Any]] = []
    for fsn in ordered_fsns:
        analysis_row = analysis_rows.get(fsn, {})
        profile_row = profile_lookup.get(fsn, {})
        return_row = return_lookup.get(fsn, {})
        active_row = pick_active_task_row(active_task_group.get(fsn, []))
        final_product_type = normalize_text(profile_row.get("Final_Product_Type", "")) or normalize_text(profile_row.get("Detected_Product_Type", ""))
        if not final_product_type:
            final_product_type = normalize_text(analysis_row.get("Detected_Product_Type", "")) or "Unknown"
        final_seasonality_tag = normalize_text(profile_row.get("Final_Seasonality_Tag", "")) or normalize_text(profile_row.get("Detected_Seasonality_Tag", ""))
        demand_row = demand_lookup.get(final_product_type, {})
        readiness = classify_readiness(analysis_row, return_row, active_row)
        manual_override = normalize_text(existing_index.get(fsn, {}).get("Manual_Override", ""))
        suggested_action, suggested_budget_level, reason = build_ads_action(
            final_product_type,
            final_seasonality_tag,
            readiness,
            analysis_row,
            return_row,
            active_row,
        )
        if manual_override:
            suggested_action = manual_override
        start_preparation_date, start_ads_date, stop_ads_date = compute_ad_dates(
            final_product_type,
            final_seasonality_tag,
            demand_row,
            date.today(),
        )
        if suggested_action == "Manual Review" and not manual_override:
            suggested_budget_level = "Manual Review"
        ads_readiness = readiness_status(suggested_action, readiness)
        if manual_override:
            reason = f"{reason} | Manual override preserved: {manual_override}"
        planner_row = {
            "FSN": fsn,
            "SKU_ID": normalize_text(analysis_row.get("SKU_ID", "")),
            "Product_Title": normalize_text(analysis_row.get("Product_Title", "")),
            "Final_Product_Type": final_product_type,
            "Final_Seasonality_Tag": final_seasonality_tag,
            "Ad_Run_Type": normalize_text(analysis_row.get("Ad_Run_Type", "")) or AD_RUN_TYPE_BY_SEASONALITY.get(final_seasonality_tag, "Manual Review"),
            "COGS_Readiness": readiness["COGS_Readiness"],
            "Profit_Readiness": readiness["Profit_Readiness"],
            "Return_Readiness": readiness["Return_Readiness"],
            "Listing_Readiness": readiness["Listing_Readiness"],
            "Data_Readiness": readiness["Data_Readiness"],
            "Alert_Readiness": readiness["Alert_Readiness"],
            "Ads_Readiness_Status": ads_readiness,
            "Suggested_Ad_Action": suggested_action,
            "Suggested_Budget_Level": suggested_budget_level,
            "Start_Preparation_Date": start_preparation_date,
            "Start_Ads_Date": start_ads_date,
            "Stop_Ads_Date": stop_ads_date,
            "Reason": reason,
            "Confidence": recommendation_confidence(final_product_type, readiness, suggested_action, manual_override),
            "Manual_Override": manual_override,
            "Last_Updated": now_iso(),
        }
        planner_rows.append(planner_row)
    return planner_rows


def write_local_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def count_fsn_issues(rows: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    blank = sum(1 for row in rows if not clean_fsn(row.get("FSN", "")))
    counts = Counter(clean_fsn(row.get("FSN", "")) for row in rows if clean_fsn(row.get("FSN", "")))
    duplicates = sum(count - 1 for count in counts.values() if count > 1)
    return blank, duplicates


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


def load_required_tabs(sheets_service, spreadsheet_id: str) -> Dict[str, Dict[str, Any]]:
    payload: Dict[str, Dict[str, Any]] = {}
    for tab_name in [SKU_ANALYSIS_TAB, COST_MASTER_TAB, RETURN_ISSUE_SUMMARY_TAB, ALERTS_TAB, ACTIVE_TASKS_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")
        headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
        payload[tab_name] = {"headers": headers, "rows": rows}
    if tab_exists(sheets_service, spreadsheet_id, RETURN_TYPE_PIVOT_TAB):
        headers, rows = read_table(sheets_service, spreadsheet_id, RETURN_TYPE_PIVOT_TAB)
        payload[RETURN_TYPE_PIVOT_TAB] = {"headers": headers, "rows": rows}
    else:
        payload[RETURN_TYPE_PIVOT_TAB] = {"headers": [], "rows": []}
    if tab_exists(sheets_service, spreadsheet_id, FSN_HISTORY_TAB):
        headers, rows = read_table(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
        payload[FSN_HISTORY_TAB] = {"headers": headers, "rows": rows}
    else:
        payload[FSN_HISTORY_TAB] = {"headers": [], "rows": []}
    return payload


def ensure_output_tabs(sheets_service, spreadsheet_id: str) -> Dict[str, int]:
    return {
        PRODUCT_AD_PROFILE_TAB: ensure_tab(sheets_service, spreadsheet_id, PRODUCT_AD_PROFILE_TAB),
        KEYWORD_SEEDS_TAB: ensure_tab(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB),
        KEYWORD_CACHE_TAB: ensure_tab(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB),
        DEMAND_PROFILE_TAB: ensure_tab(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB),
        ADS_PLANNER_TAB: ensure_tab(sheets_service, spreadsheet_id, ADS_PLANNER_TAB),
    }


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


def create_flipkart_ads_planner_foundation() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    source_tabs = load_required_tabs(sheets_service, spreadsheet_id)
    output_sheet_ids = ensure_output_tabs(sheets_service, spreadsheet_id)

    analysis_headers, analysis_rows = source_tabs[SKU_ANALYSIS_TAB]["headers"], source_tabs[SKU_ANALYSIS_TAB]["rows"]
    cost_rows = source_tabs[COST_MASTER_TAB]["rows"]
    return_summary_rows = source_tabs[RETURN_TYPE_PIVOT_TAB]["rows"] or source_tabs[RETURN_ISSUE_SUMMARY_TAB]["rows"]
    alerts_rows = source_tabs[ALERTS_TAB]["rows"]
    active_tasks_rows = source_tabs[ACTIVE_TASKS_TAB]["rows"]
    fsn_history_rows = source_tabs[FSN_HISTORY_TAB]["rows"]

    hydrated_analysis_rows = hydrate_analysis_rows(analysis_rows, cost_rows)
    if not hydrated_analysis_rows:
        raise RuntimeError(f"No rows found in Google Sheet tab: {SKU_ANALYSIS_TAB}")

    ordered_fsns, analysis_index = build_analysis_index(hydrated_analysis_rows)
    existing_product_profile_rows = read_table(sheets_service, spreadsheet_id, PRODUCT_AD_PROFILE_TAB)[1] if tab_exists(sheets_service, spreadsheet_id, PRODUCT_AD_PROFILE_TAB) else []
    existing_keyword_seed_rows = read_table(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB)[1] if tab_exists(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB) else []
    existing_keyword_cache_rows = read_table(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB)[1] if tab_exists(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB) else []
    existing_demand_profile_rows = read_table(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB)[1] if tab_exists(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB) else []
    existing_ads_planner_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)[1] if tab_exists(sheets_service, spreadsheet_id, ADS_PLANNER_TAB) else []

    product_profile_rows = build_product_profile_rows(ordered_fsns, analysis_index, existing_product_profile_rows)
    keyword_seed_rows = build_keyword_seed_rows(existing_keyword_seed_rows)
    keyword_cache_rows = build_keyword_cache_rows(keyword_seed_rows, existing_keyword_cache_rows)
    demand_profile_rows = build_demand_profile_rows(existing_demand_profile_rows)
    ads_planner_rows = build_ads_planner_rows(
        ordered_fsns,
        analysis_index,
        product_profile_rows,
        return_summary_rows,
        active_tasks_rows,
        existing_ads_planner_rows,
        demand_profile_rows,
    )

    write_local_csv(LOCAL_PRODUCT_AD_PROFILE_PATH, PRODUCT_AD_PROFILE_HEADERS, product_profile_rows)
    write_local_csv(LOCAL_KEYWORD_SEEDS_PATH, KEYWORD_SEEDS_HEADERS, keyword_seed_rows)
    write_local_csv(LOCAL_KEYWORD_CACHE_PATH, KEYWORD_CACHE_HEADERS, keyword_cache_rows)
    write_local_csv(LOCAL_DEMAND_PROFILE_PATH, DEMAND_PROFILE_HEADERS, demand_profile_rows)
    write_local_csv(LOCAL_ADS_PLANNER_PATH, ADS_PLANNER_HEADERS, ads_planner_rows)

    write_output_tab(sheets_service, spreadsheet_id, PRODUCT_AD_PROFILE_TAB, PRODUCT_AD_PROFILE_HEADERS, product_profile_rows, output_sheet_ids[PRODUCT_AD_PROFILE_TAB])
    write_output_tab(sheets_service, spreadsheet_id, KEYWORD_SEEDS_TAB, KEYWORD_SEEDS_HEADERS, keyword_seed_rows, output_sheet_ids[KEYWORD_SEEDS_TAB])
    write_output_tab(sheets_service, spreadsheet_id, KEYWORD_CACHE_TAB, KEYWORD_CACHE_HEADERS, keyword_cache_rows, output_sheet_ids[KEYWORD_CACHE_TAB])
    write_output_tab(sheets_service, spreadsheet_id, DEMAND_PROFILE_TAB, DEMAND_PROFILE_HEADERS, demand_profile_rows, output_sheet_ids[DEMAND_PROFILE_TAB])
    write_output_tab(sheets_service, spreadsheet_id, ADS_PLANNER_TAB, ADS_PLANNER_HEADERS, ads_planner_rows, output_sheet_ids[ADS_PLANNER_TAB])

    product_type_distribution = ordered_counter(product_profile_rows, "Final_Product_Type", PRODUCT_TYPE_ORDER)
    seasonality_distribution = ordered_counter(product_profile_rows, "Final_Seasonality_Tag", [
        "Diwali Heavy",
        "Festive Boost",
        "Year-Round",
        "Year-Round + Festive Boost",
        "Year-Round Home Exterior",
        "Utility / Outdoor",
        "Wedding / Event",
        "Unknown",
    ])
    ads_action_distribution = ordered_counter(ads_planner_rows, "Suggested_Ad_Action", [
        "Fill COGS First",
        "Do Not Run Ads",
        "Do Not Run Ads / Improve Economics",
        "Do Not Run Ads / Improve Product First",
        "Fix Product First",
        "Fix Product/Listing First",
        "Test Ads Carefully / Fix Product First",
        "Test Ads Carefully / Check Logistics",
        "Review Return Split",
        "Fix Listing First",
        "Resolve Critical Alert First",
        "Seasonal Ads Later / Prepare Listing First",
        "Test Ads",
        "Always-On Test",
        "Seasonal/Event Test",
        "Continue / Optimize Ads",
        "Scale Ads",
        "Manual Review",
        "Monitor",
    ])
    cogs_available_count, cogs_missing_count = count_cogs_rows(hydrated_analysis_rows)
    blank_fsn_count, duplicate_fsn_count = count_fsn_issues(ads_planner_rows)
    ready_for_test_ads_count = sum(1 for row in ads_planner_rows if normalize_text(row.get("Ads_Readiness_Status", "")) == "Ready" and normalize_text(row.get("Suggested_Ad_Action", "")) in {"Test Ads", "Always-On Test", "Seasonal/Event Test"})
    do_not_run_ads_count = sum(
        1
        for row in ads_planner_rows
        if normalize_text(row.get("Suggested_Budget_Level", "")) == "Do Not Run"
        or normalize_text(row.get("Suggested_Ad_Action", "")).startswith("Do Not Run Ads")
        or normalize_text(row.get("Suggested_Ad_Action", "")) in {
            "Fix Product First",
            "Fix Product/Listing First",
            "Fix Listing First",
            "Resolve Critical Alert First",
            "Do Not Run Ads / Improve Product First",
            "Review Return Split",
        }
    )
    manual_review_count = sum(1 for row in ads_planner_rows if normalize_text(row.get("Suggested_Ad_Action", "")) == "Manual Review" or normalize_text(row.get("Ads_Readiness_Status", "")) == "Review")

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": "SUCCESS",
        "fsn_count": len(product_profile_rows),
        "product_type_distribution": json.dumps(product_type_distribution, ensure_ascii=False),
        "seasonality_distribution": json.dumps(seasonality_distribution, ensure_ascii=False),
        "ads_action_distribution": json.dumps(ads_action_distribution, ensure_ascii=False),
        "cogs_missing_count": cogs_missing_count,
        "ready_for_test_ads_count": ready_for_test_ads_count,
        "do_not_run_ads_count": do_not_run_ads_count,
        "manual_review_count": manual_review_count,
        "tabs_updated": json.dumps([PRODUCT_AD_PROFILE_TAB, KEYWORD_SEEDS_TAB, KEYWORD_CACHE_TAB, DEMAND_PROFILE_TAB, ADS_PLANNER_TAB], ensure_ascii=False),
        "log_path": str(LOG_PATH),
        "analysis_rows": len(analysis_rows),
        "return_issue_summary_rows": len(return_summary_rows),
        "alerts_generated_rows": len(alerts_rows),
        "active_tasks_rows": len(active_tasks_rows),
        "fsn_history_rows": len(fsn_history_rows),
        "message": "Built Flipkart ads planner foundation from sheet data",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "status",
            "fsn_count",
            "product_type_distribution",
            "seasonality_distribution",
            "ads_action_distribution",
            "cogs_missing_count",
            "ready_for_test_ads_count",
            "do_not_run_ads_count",
            "manual_review_count",
            "tabs_updated",
            "log_path",
            "analysis_rows",
            "return_issue_summary_rows",
            "alerts_generated_rows",
            "active_tasks_rows",
            "fsn_history_rows",
            "message",
        ],
        [log_row],
    )

    result = {
        "status": "SUCCESS",
        "generated_at": now_iso(),
        "fsn_count": len(product_profile_rows),
        "product_type_distribution": product_type_distribution,
        "seasonality_distribution": seasonality_distribution,
        "ads_action_distribution": ads_action_distribution,
        "cogs_missing_count": cogs_missing_count,
        "ready_for_test_ads_count": ready_for_test_ads_count,
        "do_not_run_ads_count": do_not_run_ads_count,
        "manual_review_count": manual_review_count,
        "tabs_updated": [PRODUCT_AD_PROFILE_TAB, KEYWORD_SEEDS_TAB, KEYWORD_CACHE_TAB, DEMAND_PROFILE_TAB, ADS_PLANNER_TAB],
        "log_path": str(LOG_PATH),
        "local_outputs": {tab_name: str(path) for tab_name, path in TAB_OUTPUTS.items()},
        "source_tabs": {
            SKU_ANALYSIS_TAB: len(analysis_rows),
            COST_MASTER_TAB: len(cost_rows),
            RETURN_ISSUE_SUMMARY_TAB: len(return_summary_rows),
            RETURN_TYPE_PIVOT_TAB: len(source_tabs[RETURN_TYPE_PIVOT_TAB]["rows"]),
            ALERTS_TAB: len(alerts_rows),
            ACTIVE_TASKS_TAB: len(active_tasks_rows),
            FSN_HISTORY_TAB: len(fsn_history_rows),
        },
    }
    print(json.dumps(build_status_payload("SUCCESS", **{k: v for k, v in result.items() if k != "status"}), indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_ads_planner_foundation()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                    "tabs": [PRODUCT_AD_PROFILE_TAB, KEYWORD_SEEDS_TAB, KEYWORD_CACHE_TAB, DEMAND_PROFILE_TAB, ADS_PLANNER_TAB],
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
