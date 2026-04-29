from __future__ import annotations

import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "daily_alerts_log.csv"

MASTER_SKU_TAB = "MASTER_SKU"
PNL_TAB = "SKU_PNL"
REVIEW_TAB = "REVIEW_RATING"
LISTING_TAB = "LISTING_STATUS"
SCORECARD_TAB = "SKU_SCORECARD"
DECISION_TAB = "DECISION_TAGS"
TAB_NAME = "DAILY_ALERTS"

HEADERS = [
    "Alert_ID",
    "Alert_Date",
    "SKU_ID",
    "Marketplace",
    "Alert_Type",
    "Severity",
    "Trigger_Value",
    "Recommended_Action",
    "Owner",
    "Status",
    "Source_Tab",
    "Last_Updated",
    "Remarks",
]

ALERT_TYPES = [
    "Negative Profit",
    "Low Score",
    "Poor Rating",
    "Blocked Listing",
    "Rejected Listing",
    "Needs Correction",
    "High ACOS",
    "Low ROAS",
    "Low Stock",
    "Kill Candidate",
    "Liquidation Candidate",
]

SEVERITY_OPTIONS = ["Low", "Medium", "High", "Critical"]
STATUS_OPTIONS = ["Open", "In Progress", "Resolved", "Ignored"]


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be 1 or greater")
    result = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def zero_based_column_index_to_a1(index: int) -> str:
    return column_index_to_a1(index + 1)


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, object]:
    return (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def find_tab(metadata: Dict[str, object], tab_name: str) -> Dict[str, object] | None:
    for sheet in metadata.get("sheets", []):
        properties = sheet.get("properties", {})
        if properties.get("title") == tab_name:
            return properties
    return None


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    existing = find_tab(metadata, tab_name)
    if existing and existing.get("sheetId") is not None:
        return existing["sheetId"]

    response = (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        )
        .execute()
    )
    return response["replies"][0]["addSheet"]["properties"]["sheetId"]


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[str]]:
    return _retry_google_call(
        lambda: (
            sheets_service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
            .get("values", [])
        )
    )


def _retry_google_call(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status != 503 or attempt == attempts:
                raise
            time.sleep(delay)
            delay *= 2


def find_header_index(headers: Sequence[str], header_name: str) -> int | None:
    for index, header in enumerate(headers):
        if header == header_name:
            return index
    return None


def clean_text(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"none", "null"}:
        return ""
    return text


def to_float(value: object) -> float:
    text = clean_text(value)
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def to_int(value: object) -> int:
    return int(round(to_float(value)))


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if len(rows) < 2:
        return [], []

    headers = rows[0]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))})
    return headers, data


def build_lookup(rows: List[Dict[str, str]], key_fields: Tuple[str, ...]) -> Dict[Tuple[str, ...], Dict[str, str]]:
    lookup: Dict[Tuple[str, ...], Dict[str, str]] = {}
    for row in rows:
        key = tuple(clean_text(row.get(field)) for field in key_fields)
        if any(not part for part in key) or key in lookup:
            continue
        lookup[key] = row
    return lookup


def read_master_skus(sheets_service, spreadsheet_id: str) -> List[str]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{MASTER_SKU_TAB}!A1:ZZ")
    if len(rows) < 2:
        return []

    headers = rows[0]
    sku_index = find_header_index(headers, "SKU_ID")
    if sku_index is None:
        raise ValueError("SKU_ID header was not found in MASTER_SKU.")

    seen: set[str] = set()
    skus: List[str] = []
    for row in rows[1:]:
        sku_id = row[sku_index].strip() if sku_index < len(row) else ""
        if sku_id and sku_id not in seen:
            seen.add(sku_id)
            skus.append(sku_id)
    return skus


def ensure_headers(sheets_service, spreadsheet_id: str, header_row: List[str]) -> bool:
    if header_row[: len(HEADERS)] != HEADERS:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{TAB_NAME}!A1:{zero_based_column_index_to_a1(len(HEADERS) - 1)}1",
            valueInputOption="RAW",
            body={"values": [HEADERS]},
        ).execute()
        return True
    return False


def find_alert_row_index(rows: List[Dict[str, str]], key: Tuple[str, str, str, str]) -> int | None:
    for index, row in enumerate(rows):
        existing_key = (
            clean_text(row.get("SKU_ID")),
            clean_text(row.get("Marketplace")),
            clean_text(row.get("Alert_Type")),
            clean_text(row.get("Source_Tab")),
        )
        if existing_key == key and clean_text(row.get("Status")) in {"Open", "In Progress"}:
            return index
    return None


def normalize_severity(value: str) -> str:
    text = clean_text(value)
    return text if text in SEVERITY_OPTIONS else "Low"


def choose_recommended_action(alert_type: str, fallback: str) -> str:
    mapping = {
        "Negative Profit": fallback,
        "Low Score": fallback,
        "Poor Rating": "Fix",
        "Blocked Listing": "Fix",
        "Rejected Listing": "Fix",
        "Needs Correction": "Fix",
        "High ACOS": fallback,
        "Low ROAS": fallback,
        "Low Stock": "Reorder",
        "Kill Candidate": "Kill",
        "Liquidation Candidate": "Liquidate",
    }
    return mapping.get(alert_type, fallback)


def build_alerts(
    master_lookup: Dict[Tuple[str], Dict[str, str]],
    scorecard_lookup: Dict[Tuple[str, str], Dict[str, str]],
    review_lookup: Dict[Tuple[str, str], Dict[str, str]],
    listing_lookup: Dict[Tuple[str, str], Dict[str, str]],
    decision_lookup: Dict[Tuple[str, str], Dict[str, str]],
    scorecard_rows: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    alerts: List[Dict[str, str]] = []
    seen: set[Tuple[str, str, str, str]] = set()
    alert_date = datetime.now().date().isoformat()
    timestamp = datetime.now().isoformat(timespec="seconds")

    ordered_pairs = []
    for row in scorecard_rows:
        key = (clean_text(row.get("SKU_ID")), clean_text(row.get("Marketplace")))
        if key[0] and key[1] and key not in ordered_pairs:
            ordered_pairs.append(key)

    for sku_id, marketplace in ordered_pairs:
        scorecard_row = scorecard_lookup.get((sku_id, marketplace), {})
        review_row = review_lookup.get((sku_id, marketplace), {})
        listing_row = listing_lookup.get((sku_id, marketplace), {})
        decision_row = decision_lookup.get((sku_id, marketplace), {})
        master_row = master_lookup.get((sku_id,), {})

        net_profit = to_float(scorecard_row.get("Net_Profit"))
        total_score = to_float(scorecard_row.get("Total_Score"))
        average_rating = to_float(review_row.get("Average_Rating"))
        listing_status = clean_text(listing_row.get("Listing_Status"))
        acos = to_float(scorecard_row.get("ACOS"))
        roas = to_float(scorecard_row.get("ROAS"))
        ad_spend = to_float(scorecard_row.get("Ad_Spend"))
        available_stock = to_float(master_row.get("Available_Stock"))
        reorder_level = to_float(master_row.get("Reorder_Level"))
        final_decision_tag = clean_text(decision_row.get("Final_Decision_Tag"))
        recommended_action = clean_text(scorecard_row.get("Recommended_Action"))

        def add_alert(alert_type: str, severity: str, trigger_value: str, source_tab: str, action: str) -> None:
            key = (sku_id, marketplace, alert_type, source_tab)
            if key in seen:
                return
            seen.add(key)
            alerts.append(
                {
                    "Alert_Date": alert_date,
                    "SKU_ID": sku_id,
                    "Marketplace": marketplace,
                    "Alert_Type": alert_type,
                    "Severity": severity,
                    "Trigger_Value": trigger_value,
                    "Recommended_Action": action,
                    "Owner": "",
                    "Status": "Open",
                    "Source_Tab": source_tab,
                    "Last_Updated": timestamp,
                    "Remarks": "",
                }
            )

        if net_profit < 0:
            add_alert("Negative Profit", "High", f"{net_profit:.2f}", SCORECARD_TAB, choose_recommended_action("Negative Profit", recommended_action))

        if total_score < 40:
            severity = "Critical" if total_score < 20 else "High"
            add_alert("Low Score", severity, f"{total_score:.0f}", SCORECARD_TAB, choose_recommended_action("Low Score", recommended_action))

        if average_rating < 3:
            severity = "Critical" if average_rating < 2 else "High"
            add_alert("Poor Rating", severity, f"{average_rating:.2f}", REVIEW_TAB, choose_recommended_action("Poor Rating", recommended_action))

        if listing_status == "Blocked":
            add_alert("Blocked Listing", "Critical", listing_status, LISTING_TAB, "Fix")
        if listing_status == "Rejected":
            add_alert("Rejected Listing", "High", listing_status, LISTING_TAB, "Fix")
        if listing_status == "Needs Correction":
            add_alert("Needs Correction", "Medium", listing_status, LISTING_TAB, "Fix")

        if acos > 0.35:
            severity = "Critical" if acos > 0.50 else "High"
            add_alert("High ACOS", severity, f"{acos:.4f}", SCORECARD_TAB, choose_recommended_action("High ACOS", recommended_action))

        if roas < 1.5 and ad_spend > 0:
            add_alert("Low ROAS", "High", f"{roas:.4f}", SCORECARD_TAB, choose_recommended_action("Low ROAS", recommended_action))

        if available_stock <= reorder_level:
            add_alert(
                "Low Stock",
                "Medium",
                f"{available_stock:.0f} <= {reorder_level:.0f}",
                MASTER_SKU_TAB,
                "Reorder",
            )

        if final_decision_tag == "Kill":
            add_alert("Kill Candidate", "Critical", final_decision_tag, DECISION_TAB, "Kill")
        if final_decision_tag == "Liquidate":
            add_alert("Liquidation Candidate", "High", final_decision_tag, DECISION_TAB, "Liquidate")

    return alerts


def build_row_from_alert(alert: Dict[str, str], alert_id: str, existing_row: Dict[str, str] | None) -> List[str]:
    owner = clean_text(existing_row.get("Owner")) if existing_row else ""
    status = clean_text(existing_row.get("Status")) if existing_row else "Open"
    if status not in STATUS_OPTIONS:
        status = "Open"
    remarks = clean_text(existing_row.get("Remarks")) if existing_row else ""

    return [
        alert_id,
        alert["Alert_Date"],
        alert["SKU_ID"],
        alert["Marketplace"],
        alert["Alert_Type"],
        alert["Severity"],
        alert["Trigger_Value"],
        alert["Recommended_Action"],
        owner,
        status,
        alert["Source_Tab"],
        alert["Last_Updated"],
        remarks,
    ]


def existing_rows_by_key(rows: List[Dict[str, str]]) -> Dict[Tuple[str, str, str, str], Tuple[int, Dict[str, str]]]:
    lookup: Dict[Tuple[str, str, str, str], Tuple[int, Dict[str, str]]] = {}
    for row_number, row in enumerate(rows, start=2):
        key = (
            clean_text(row.get("SKU_ID")),
            clean_text(row.get("Marketplace")),
            clean_text(row.get("Alert_Type")),
            clean_text(row.get("Source_Tab")),
        )
        if all(key) and key not in lookup:
            lookup[key] = (row_number, row)
    return lookup


def apply_formatting(sheets_service, spreadsheet_id: str, sheet_id: int) -> None:
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
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
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 9,
                    "endColumnIndex": 10,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in STATUS_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
    ]
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(HEADERS),
                        }
                    }
                }
            ]
        },
    ).execute()


def append_log_rows(log_rows: List[Dict[str, object]]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_exists = LOG_PATH.exists()
    with LOG_PATH.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["timestamp", "alert_id", "sku_id", "marketplace", "alert_type", "status", "message"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_daily_alerts() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    sheet_id = ensure_tab(sheets_service, spreadsheet_id, TAB_NAME)
    existing_headers = get_sheet_values(sheets_service, spreadsheet_id, f"{TAB_NAME}!A1:M1")
    headers_written = ensure_headers(sheets_service, spreadsheet_id, existing_headers[0] if existing_headers else [])

    _, scorecard_rows = read_table(sheets_service, spreadsheet_id, SCORECARD_TAB)
    _, decision_rows = read_table(sheets_service, spreadsheet_id, DECISION_TAB)
    _, review_rows = read_table(sheets_service, spreadsheet_id, REVIEW_TAB)
    _, listing_rows = read_table(sheets_service, spreadsheet_id, LISTING_TAB)
    _, pnl_rows = read_table(sheets_service, spreadsheet_id, PNL_TAB)
    _, master_rows = read_table(sheets_service, spreadsheet_id, MASTER_SKU_TAB)
    _, existing_alert_rows = read_table(sheets_service, spreadsheet_id, TAB_NAME)

    scorecard_lookup = build_lookup(scorecard_rows, ("SKU_ID", "Marketplace"))
    review_lookup = build_lookup(review_rows, ("SKU_ID", "Marketplace"))
    listing_lookup = build_lookup(listing_rows, ("SKU_ID", "Marketplace"))
    decision_lookup = build_lookup(decision_rows, ("SKU_ID", "Marketplace"))
    master_lookup = build_lookup(master_rows, ("SKU_ID",))
    existing_lookup = existing_rows_by_key(existing_alert_rows)

    alerts = build_alerts(master_lookup, scorecard_lookup, review_lookup, listing_lookup, decision_lookup, scorecard_rows)

    rows_to_write: List[List[str]] = []
    log_rows: List[Dict[str, object]] = []
    now = datetime.now().isoformat(timespec="seconds")

    for alert in alerts:
        key = (alert["SKU_ID"], alert["Marketplace"], alert["Alert_Type"], alert["Source_Tab"])
        existing = existing_lookup.get(key)
        if existing and clean_text(existing[1].get("Status")) in {"Open", "In Progress"}:
            alert_id = clean_text(existing[1].get("Alert_ID")) or f"ALERT-{now.replace(':', '').replace('-', '')}"
            row = build_row_from_alert(alert, alert_id, existing[1])
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{TAB_NAME}!A{existing[0]}:M{existing[0]}",
                valueInputOption="RAW",
                body={"values": [row]},
            ).execute()
            log_rows.append(
                {
                    "timestamp": now,
                    "alert_id": alert_id,
                    "sku_id": alert["SKU_ID"],
                    "marketplace": alert["Marketplace"],
                    "alert_type": alert["Alert_Type"],
                    "status": "updated",
                    "message": "Updated active alert",
                }
            )
            continue

        alert_id = f"AL-{datetime.now().strftime('%Y%m%d%H%M%S')}-{alert['SKU_ID']}-{alert['Marketplace']}-{alert['Alert_Type'].replace(' ', '')[:12]}"
        row = build_row_from_alert(alert, alert_id, None)
        rows_to_write.append(row)
        log_rows.append(
            {
                "timestamp": now,
                "alert_id": alert_id,
                "sku_id": alert["SKU_ID"],
                "marketplace": alert["Marketplace"],
                "alert_type": alert["Alert_Type"],
                "status": "created",
                "message": "Created alert row",
            }
        )

    if rows_to_write:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{TAB_NAME}!A:M",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_to_write},
        ).execute()

    apply_formatting(sheets_service, spreadsheet_id, sheet_id)
    append_log_rows(log_rows)

    print("Daily alerts summary:")
    print(f"  Rows read from SKU_PNL: {len(pnl_rows)}")
    print(f"  Rows read from SKU_SCORECARD: {len(scorecard_rows)}")
    print(f"  Rows read from DECISION_TAGS: {len(decision_rows)}")
    print(f"  Rows read from LISTING_STATUS: {len(listing_rows)}")
    print(f"  Rows read from REVIEW_RATING: {len(review_rows)}")
    print(f"  Rows read from MASTER_SKU: {len(master_rows)}")
    print(f"  Rows written to DAILY_ALERTS: {len(rows_to_write)}")

    return {
        "spreadsheet_id": spreadsheet_id,
        "daily_alerts_sheet_name": TAB_NAME,
        "daily_alerts_sheet_id": sheet_id,
        "headers_written": headers_written,
        "pnl_rows_read": len(pnl_rows),
        "scorecard_rows_read": len(scorecard_rows),
        "decision_rows_read": len(decision_rows),
        "listing_rows_read": len(listing_rows),
        "review_rows_read": len(review_rows),
        "master_rows_read": len(master_rows),
        "rows_written": len(rows_to_write),
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_daily_alerts()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
