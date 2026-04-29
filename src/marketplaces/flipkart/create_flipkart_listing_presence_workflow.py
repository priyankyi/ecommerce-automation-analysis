from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    TARGET_FSN_PATH,
    FSN_BRIDGE_PATH,
    SKU_ANALYSIS_PATH,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    load_json,
    normalize_text,
    now_iso,
    parse_float,
    parse_int,
    write_csv,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
RAW_LISTING_PATH = PROJECT_ROOT / "data" / "input" / "marketplaces" / "flipkart" / "raw" / "Listing.xls"

LOG_PATH = LOG_DIR / "flipkart_listing_presence_workflow_log.csv"

PRESENCE_TAB = "FLIPKART_LISTING_PRESENCE"
MISSING_TAB = "FLIPKART_MISSING_ACTIVE_LISTINGS"
ISSUES_TAB = "FLIPKART_LISTING_STATUS_ISSUES"

SOURCE_TABS = [
    "FLIPKART_SKU_ANALYSIS",
    "FLIPKART_ALERTS_GENERATED",
    "FLIPKART_ACTIVE_TASKS",
    "FLIPKART_ACTION_TRACKER",
]

PRESENCE_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Target_FSN_Status",
    "Found_In_Active_Listing",
    "Listing_Status_From_File",
    "Listing_Presence_Status",
    "Possible_Issue",
    "Suggested_Action",
    "Data_Source",
    "Last_Updated",
]

MISSING_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Orders",
    "Returns",
    "Return_Rate",
    "Data_Confidence",
    "Final_Action",
    "Listing_Presence_Status",
    "Possible_Issue",
    "Suggested_Action",
    "Priority",
    "Owner",
    "Status",
    "Remarks",
    "Last_Updated",
]

ISSUES_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Detected_Status",
    "Blocked_Reason",
    "Inactive_Reason",
    "Rejected_Reason",
    "Correction_Required",
    "Source_Report",
    "Suggested_Action",
    "Priority",
    "Owner",
    "Status",
    "Remarks",
    "Last_Updated",
]

SKU_ANALYSIS_LISTING_COLUMNS = [
    "Listing_Presence_Status",
    "Found_In_Active_Listing",
    "Listing_Check_Action",
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


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    if not tab_exists(sheets_service, spreadsheet_id, tab_name):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be 1 or greater")
    result: List[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


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


def clear_and_write_tab(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    values = [list(headers)] + [[row.get(header, "") for header in headers] for row in rows]
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A:ZZ",
        body={},
    ).execute()
    if values:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:{column_index_to_a1(len(headers))}{len(values)}",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def freeze_and_format(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int) -> None:
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
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def pick_field(headers: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    normalized = {normalize_text(header).lower(): header for header in headers}
    for candidate in candidates:
        candidate_norm = normalize_text(candidate).lower()
        if candidate_norm in normalized:
            return normalized[candidate_norm]
    return None


def first_non_blank(row: Dict[str, Any], candidates: Sequence[str]) -> str:
    for candidate in candidates:
        value = normalize_text(row.get(candidate, ""))
        if value:
            return value
    return ""


def is_likely_fsn(value: Any) -> bool:
    fsn = clean_fsn(value)
    return bool(fsn) and bool(re.fullmatch(r"[A-Z0-9]{16}", fsn))


def safe_number_text(value: Any, integer: bool = False) -> str:
    if integer:
        return str(parse_int(value))
    number = parse_float(value)
    if number == 0 and normalize_text(value) == "":
        return "0"
    if float(number).is_integer():
        return str(int(number))
    text = f"{number:.4f}".rstrip("0").rstrip(".")
    return text or "0"


def load_listing_workbook(path: Path) -> Tuple[str, List[Dict[str, Any]]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    workbook = pd.ExcelFile(path)
    required = {"Flipkart Serial Number", "Listing Status"}
    for sheet_name in workbook.sheet_names:
        frame = pd.read_excel(workbook, sheet_name=sheet_name, dtype=object)
        if required.issubset({str(column) for column in frame.columns}):
            rows = frame.fillna("").to_dict(orient="records")
            return sheet_name, rows
    raise FileNotFoundError(f"Could not find a listing sheet with required columns in: {path}")


def build_listing_lookups(rows: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    active_lookup: Dict[str, Dict[str, Any]] = {}
    all_lookup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("Flipkart Serial Number", ""))
        if not is_likely_fsn(fsn):
            continue
        status = normalize_text(row.get("Listing Status", "")).upper()
        cleaned = {
            "FSN": fsn,
            "SKU_ID": normalize_text(row.get("Seller SKU Id", "")),
            "Product_Title": normalize_text(row.get("Product Title", "")),
            "Listing_Status": status,
            "Inactive_Reason": normalize_text(row.get("Inactive Reason", "")),
        }
        existing = all_lookup.get(fsn)
        if existing is None:
            all_lookup[fsn] = cleaned
        elif existing.get("Listing_Status") != "ACTIVE" and status == "ACTIVE":
            all_lookup[fsn] = cleaned
        if status == "ACTIVE":
            active_existing = active_lookup.get(fsn)
            if active_existing is None or active_existing.get("Listing_Status") != "ACTIVE":
                active_lookup[fsn] = cleaned
    return active_lookup, all_lookup


def build_source_maps() -> Dict[str, Dict[str, Any]]:
    target_rows = read_csv_rows(TARGET_FSN_PATH)
    bridge_rows = read_csv_rows(FSN_BRIDGE_PATH)
    sku_rows = read_csv_rows(SKU_ANALYSIS_PATH)

    target_map: Dict[str, Dict[str, Any]] = {}
    for row in target_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in target_map:
            target_map[fsn] = row

    bridge_map: Dict[str, Dict[str, Any]] = {}
    for row in bridge_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in bridge_map:
            bridge_map[fsn] = row

    sku_map: Dict[str, Dict[str, Any]] = {}
    for row in sku_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in sku_map:
            sku_map[fsn] = row

    return {"target": target_map, "bridge": bridge_map, "sku": sku_map}


def build_manual_lookup(
    existing_rows: Sequence[Dict[str, str]],
    tracker_rows: Sequence[Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    for rows in [existing_rows, tracker_rows]:
        for row in rows:
            fsn = clean_fsn(row.get("FSN", ""))
            if not fsn:
                continue
            entry = lookup.setdefault(fsn, {"Owner": "", "Status": "", "Remarks": ""})
            owner = first_non_blank(row, ["Owner"])
            status = first_non_blank(row, ["Status"])
            remarks = first_non_blank(row, ["Remarks", "Resolution_Notes", "Action_Taken"])
            if owner:
                entry["Owner"] = owner
            if status:
                entry["Status"] = status
            if remarks:
                entry["Remarks"] = remarks
    return lookup


def build_presence_rows(
    target_order: Sequence[str],
    target_map: Dict[str, Dict[str, Any]],
    bridge_map: Dict[str, Dict[str, Any]],
    sku_map: Dict[str, Dict[str, Any]],
    active_listing_lookup: Dict[str, Dict[str, Any]],
    all_listing_lookup: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    presence_rows: List[Dict[str, Any]] = []
    missing_rows: List[Dict[str, Any]] = []
    issue_rows: List[Dict[str, Any]] = []
    counts = Counter()
    updated_at = now_iso()

    for fsn in target_order:
        target_row = target_map.get(fsn, {})
        bridge_row = bridge_map.get(fsn, {})
        sku_row = sku_map.get(fsn, {})
        listing_row = active_listing_lookup.get(fsn) or all_listing_lookup.get(fsn, {})

        sku_id = first_non_blank(
            target_row,
            ["SKU_ID"],
        ) or first_non_blank(
            bridge_row,
            ["Seller_SKU", "SKU_ID"],
        ) or first_non_blank(
            sku_row,
            ["SKU_ID"],
        ) or first_non_blank(
            listing_row,
            ["SKU_ID"],
        )
        product_title = first_non_blank(
            sku_row,
            ["Product_Title"],
        ) or first_non_blank(
            bridge_row,
            ["Product_Title"],
        ) or first_non_blank(
            target_row,
            ["Product_Title"],
        ) or first_non_blank(
            listing_row,
            ["Product_Title"],
        )
        data_confidence = normalize_text(sku_row.get("Data_Confidence", "")) or "UNKNOWN"
        orders = safe_number_text(sku_row.get("Orders", ""), integer=True)
        returns = safe_number_text(sku_row.get("Returns", ""), integer=True)
        return_rate = safe_number_text(sku_row.get("Return_Rate", ""))

        found = fsn in active_listing_lookup
        listing_status_from_file = normalize_text(listing_row.get("Listing_Status", "")) if listing_row else ""
        if found:
            listing_presence_status = "Found In Active Listing File"
            possible_issue = ""
            suggested_action = "Monitor"
        else:
            listing_presence_status = "Missing From Active Listing File"
            possible_issue = "Blocked / Inactive / Archived / Not Listed / Ready For Activation / Data Mismatch"
            suggested_action = "Check Listing Status in Flipkart Panel"

        presence_rows.append(
            {
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": product_title,
                "Target_FSN_Status": "Target FSN",
                "Found_In_Active_Listing": "Yes" if found else "No",
                "Listing_Status_From_File": listing_status_from_file,
                "Listing_Presence_Status": listing_presence_status,
                "Possible_Issue": possible_issue,
                "Suggested_Action": suggested_action,
                "Data_Source": "Listing.xls + flipkart_target_fsns.csv + flipkart_fsn_bridge.csv + flipkart_sku_analysis.csv",
                "Last_Updated": updated_at,
            }
        )

        if found:
            counts["found"] += 1
            continue

        priority = "Medium"
        if parse_int(orders) > 0:
            priority = "Critical"
        elif data_confidence.upper() == "HIGH":
            priority = "High"

        missing_row = {
            "FSN": fsn,
            "SKU_ID": sku_id,
            "Product_Title": product_title,
            "Orders": orders,
            "Returns": returns,
            "Return_Rate": return_rate,
            "Data_Confidence": data_confidence,
            "Final_Action": suggested_action,
            "Listing_Presence_Status": listing_presence_status,
            "Possible_Issue": possible_issue,
            "Suggested_Action": suggested_action,
            "Priority": priority,
            "Owner": "",
            "Status": "",
            "Remarks": "",
            "Last_Updated": updated_at,
        }
        issue_row = {
            "FSN": fsn,
            "SKU_ID": sku_id,
            "Product_Title": product_title,
            "Detected_Status": "Unknown / Missing From Active Listing File",
            "Blocked_Reason": "",
            "Inactive_Reason": "",
            "Rejected_Reason": "",
            "Correction_Required": suggested_action,
            "Source_Report": "Listing.xls active report only",
            "Suggested_Action": suggested_action,
            "Priority": priority,
            "Owner": "",
            "Status": "",
            "Remarks": "",
            "Last_Updated": updated_at,
        }
        missing_rows.append(missing_row)
        issue_rows.append(issue_row)
        if priority == "Critical":
            counts["critical"] += 1
        elif priority == "High":
            counts["high"] += 1
        else:
            counts["medium"] += 1

    return presence_rows, missing_rows, issue_rows, counts


def augment_sku_analysis_rows(
    sku_rows: Sequence[Dict[str, Any]],
    presence_lookup: Dict[str, Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not sku_rows:
        return [], []
    base_headers = list(sku_rows[0].keys())
    headers = list(base_headers)
    for column in SKU_ANALYSIS_LISTING_COLUMNS:
        if column not in headers:
            headers.append(column)

    rows: List[Dict[str, Any]] = []
    for row in sku_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        presence = presence_lookup.get(fsn, {})
        new_row = dict(row)
        new_row["Listing_Presence_Status"] = presence.get("Listing_Presence_Status", "")
        new_row["Found_In_Active_Listing"] = presence.get("Found_In_Active_Listing", "")
        new_row["Listing_Check_Action"] = presence.get("Suggested_Action", "")
        rows.append(new_row)
    return headers, rows


def update_sheet_tab(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    values = [list(headers)] + [[row.get(header, "") for header in headers] for row in rows]
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A:ZZ",
        body={},
    ).execute()
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1:{column_index_to_a1(len(headers))}{len(values)}",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def run_workflow() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    if not RAW_LISTING_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {RAW_LISTING_PATH}")
    for path in [TARGET_FSN_PATH, FSN_BRIDGE_PATH, SKU_ANALYSIS_PATH]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in SOURCE_TABS:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)
    ensure_required_tab_exists(sheets_service, spreadsheet_id, "FLIPKART_SKU_ANALYSIS")
    ensure_required_tab_exists(sheets_service, spreadsheet_id, "FLIPKART_ACTION_TRACKER")

    existing_presence_headers, existing_presence_rows = read_table(sheets_service, spreadsheet_id, PRESENCE_TAB) if tab_exists(sheets_service, spreadsheet_id, PRESENCE_TAB) else ([], [])
    existing_missing_headers, existing_missing_rows = read_table(sheets_service, spreadsheet_id, MISSING_TAB) if tab_exists(sheets_service, spreadsheet_id, MISSING_TAB) else ([], [])
    existing_issue_headers, existing_issue_rows = read_table(sheets_service, spreadsheet_id, ISSUES_TAB) if tab_exists(sheets_service, spreadsheet_id, ISSUES_TAB) else ([], [])
    tracker_headers, tracker_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_ACTION_TRACKER")
    sku_headers, sku_rows = read_table(sheets_service, spreadsheet_id, "FLIPKART_SKU_ANALYSIS")

    listing_sheet_name, listing_rows = load_listing_workbook(RAW_LISTING_PATH)
    active_lookup, all_lookup = build_listing_lookups(listing_rows)
    source_maps = build_source_maps()
    target_map = source_maps["target"]
    bridge_map = source_maps["bridge"]
    sku_map = source_maps["sku"]

    target_order: List[str] = []
    target_csv_rows = read_csv_rows(TARGET_FSN_PATH)
    for row in target_csv_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if fsn and fsn not in target_order:
            target_order.append(fsn)

    presence_rows, missing_rows, issue_rows, counts = build_presence_rows(
        target_order=target_order,
        target_map=target_map,
        bridge_map=bridge_map,
        sku_map=sku_map,
        active_listing_lookup=active_lookup,
        all_listing_lookup=all_lookup,
    )

    manual_lookup = build_manual_lookup(existing_missing_rows, tracker_rows)
    issue_manual_lookup = build_manual_lookup(existing_issue_rows, tracker_rows)

    for row in missing_rows:
        manual = manual_lookup.get(clean_fsn(row.get("FSN", "")), {})
        if manual.get("Owner", ""):
            row["Owner"] = manual["Owner"]
        if manual.get("Status", ""):
            row["Status"] = manual["Status"]
        if manual.get("Remarks", ""):
            row["Remarks"] = manual["Remarks"]
        if not normalize_text(row.get("Status", "")):
            row["Status"] = "Open"

    for row in issue_rows:
        manual = issue_manual_lookup.get(clean_fsn(row.get("FSN", "")), {})
        if manual.get("Owner", ""):
            row["Owner"] = manual["Owner"]
        if manual.get("Status", ""):
            row["Status"] = manual["Status"]
        if manual.get("Remarks", ""):
            row["Remarks"] = manual["Remarks"]
        if not normalize_text(row.get("Status", "")):
            row["Status"] = "Open"

    presence_lookup = {clean_fsn(row.get("FSN", "")): row for row in presence_rows}
    sku_analysis_headers, sku_analysis_rows = augment_sku_analysis_rows(sku_rows, presence_lookup)

    write_csv(OUTPUT_DIR / "flipkart_listing_presence.csv", PRESENCE_HEADERS, presence_rows)
    write_csv(OUTPUT_DIR / "flipkart_missing_active_listings.csv", MISSING_HEADERS, missing_rows)
    write_csv(OUTPUT_DIR / "flipkart_listing_status_issues.csv", ISSUES_HEADERS, issue_rows)

    update_sheet_tab(sheets_service, spreadsheet_id, PRESENCE_TAB, PRESENCE_HEADERS, presence_rows)
    update_sheet_tab(sheets_service, spreadsheet_id, MISSING_TAB, MISSING_HEADERS, missing_rows)
    update_sheet_tab(sheets_service, spreadsheet_id, ISSUES_TAB, ISSUES_HEADERS, issue_rows)
    update_sheet_tab(sheets_service, spreadsheet_id, "FLIPKART_SKU_ANALYSIS", sku_analysis_headers, sku_analysis_rows)

    active_listing_fsn_count = len(active_lookup)
    found_in_active_listing_count = len(presence_rows) - len(missing_rows)
    missing_from_active_listing_count = len(missing_rows)
    summary = {
        "status": "SUCCESS",
        "target_fsn_count": len(target_order),
        "active_listing_fsn_count": active_listing_fsn_count,
        "found_in_active_listing_count": found_in_active_listing_count,
        "missing_from_active_listing_count": missing_from_active_listing_count,
        "critical_missing_count": counts["critical"],
        "high_missing_count": counts["high"],
        "medium_missing_count": counts["medium"],
        "tabs_updated": [PRESENCE_TAB, MISSING_TAB, ISSUES_TAB, "FLIPKART_SKU_ANALYSIS"],
        "log_path": str(LOG_PATH),
    }

    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "status",
            "target_fsn_count",
            "active_listing_fsn_count",
            "found_in_active_listing_count",
            "missing_from_active_listing_count",
            "critical_missing_count",
            "high_missing_count",
            "medium_missing_count",
            "tabs_updated",
            "listing_sheet_name",
        ],
        [
            {
                "timestamp": now_iso(),
                "status": summary["status"],
                "target_fsn_count": summary["target_fsn_count"],
                "active_listing_fsn_count": summary["active_listing_fsn_count"],
                "found_in_active_listing_count": summary["found_in_active_listing_count"],
                "missing_from_active_listing_count": summary["missing_from_active_listing_count"],
                "critical_missing_count": summary["critical_missing_count"],
                "high_missing_count": summary["high_missing_count"],
                "medium_missing_count": summary["medium_missing_count"],
                "tabs_updated": ";".join(summary["tabs_updated"]),
                "listing_sheet_name": listing_sheet_name,
            }
        ],
    )
    return summary


def main() -> None:
    try:
        print(json.dumps(run_workflow(), indent=2, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
