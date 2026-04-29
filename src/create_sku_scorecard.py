from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "sku_scorecard_log.csv"

MASTER_SKU_TAB = "MASTER_SKU"
PNL_TAB = "SKU_PNL"
REVIEW_TAB = "REVIEW_RATING"
LISTING_TAB = "LISTING_STATUS"
TAB_NAME = "SKU_SCORECARD"

HEADERS = [
    "SKU_ID",
    "Marketplace",
    "Net_Profit",
    "Profit_Margin_Percent",
    "Orders",
    "Units_Sold",
    "ROAS",
    "ACOS",
    "Average_Rating",
    "Low_Rating_Count",
    "Return_Count",
    "Cancelled_Count",
    "Listing_Status",
    "Profit_Score",
    "Sales_Score",
    "Rating_Score",
    "Return_Score",
    "Ad_Score",
    "Listing_Stability_Score",
    "Total_Score",
    "Recommended_Action",
    "Reason",
    "Last_Updated",
]


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
    response = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if len(rows) < 2:
        return [], []

    headers = rows[0]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))})
    return headers, data


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


def format_number(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return f"{value:.2f}"


def format_percent(value: float) -> str:
    return f"{value:.4f}" if value else "0"


def build_lookup(rows: List[Dict[str, str]], key_fields: Tuple[str, str]) -> Dict[Tuple[str, str], Dict[str, str]]:
    lookup: Dict[Tuple[str, str], Dict[str, str]] = {}
    first_key, second_key = key_fields
    for row in rows:
        key = (clean_text(row.get(first_key)), clean_text(row.get(second_key)))
        if not key[0] or not key[1] or key in lookup:
            continue
        lookup[key] = row
    return lookup


def get_master_skus(sheets_service, spreadsheet_id: str) -> List[str]:
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
        if not sku_id or sku_id in seen:
            continue
        seen.add(sku_id)
        skus.append(sku_id)
    return skus


def get_marketplaces(*lookups: Dict[Tuple[str, str], Dict[str, str]]) -> List[str]:
    marketplaces: List[str] = []
    seen: set[str] = set()
    for lookup in lookups:
        for _, marketplace in lookup:
            if marketplace and marketplace not in seen:
                seen.add(marketplace)
                marketplaces.append(marketplace)
    return marketplaces


def normalize_listing_status(status: str) -> str:
    text = clean_text(status)
    return text or "Not Started"


def listing_stability_score(status: str) -> int:
    normalized = normalize_listing_status(status)
    if normalized == "Live":
        return 10
    if normalized in {"Uploaded", "Not Started"}:
        return 6
    if normalized == "Needs Correction":
        return 4
    if normalized == "Rejected":
        return 2
    if normalized == "Blocked":
        return 0
    return 6


def profit_score(net_profit: float, profit_margin: float) -> int:
    if net_profit > 0 and profit_margin >= 0.15:
        return 30
    if net_profit > 0:
        return 20
    if net_profit == 0:
        return 10
    return 0


def sales_score(orders: int) -> int:
    if orders >= 10:
        return 20
    if orders >= 5:
        return 15
    if orders >= 1:
        return 10
    return 0


def rating_score(average_rating: float, has_rating: bool) -> int:
    if not has_rating:
        return 7
    if average_rating >= 4.2:
        return 15
    if average_rating >= 3.5:
        return 10
    if average_rating >= 3.0:
        return 5
    return 0


def return_score(return_count: int) -> int:
    if return_count == 0:
        return 15
    if return_count <= 2:
        return 10
    if return_count <= 5:
        return 5
    return 0


def ad_score(roas: float, has_ad_data: bool) -> int:
    if not has_ad_data:
        return 5
    if roas >= 3:
        return 10
    if roas >= 2:
        return 7
    if roas >= 1:
        return 4
    return 0


def recommended_action(total_score: int) -> str:
    if total_score >= 80:
        return "Scale"
    if total_score >= 60:
        return "Maintain"
    if total_score >= 40:
        return "Fix"
    if total_score >= 20:
        return "Reduce Ads / Liquidate"
    return "Kill"


def build_reason(parts: List[str]) -> str:
    cleaned = [part for part in parts if part]
    return "; ".join(cleaned)


def build_scorecard_rows(
    master_skus: List[str],
    pnl_lookup: Dict[Tuple[str, str], Dict[str, str]],
    review_lookup: Dict[Tuple[str, str], Dict[str, str]],
    listing_lookup: Dict[Tuple[str, str], Dict[str, str]],
) -> List[List[str]]:
    marketplaces = get_marketplaces(pnl_lookup, review_lookup, listing_lookup)
    if not marketplaces and review_lookup:
        marketplaces = sorted({marketplace for _, marketplace in review_lookup})
    elif not marketplaces and listing_lookup:
        marketplaces = sorted({marketplace for _, marketplace in listing_lookup})
    elif not marketplaces and pnl_lookup:
        marketplaces = sorted({marketplace for _, marketplace in pnl_lookup})

    timestamp = datetime.now().isoformat(timespec="seconds")
    rows: List[List[str]] = []

    for sku_id in master_skus:
        for marketplace in marketplaces:
            pnl_row = pnl_lookup.get((sku_id, marketplace), {})
            review_row = review_lookup.get((sku_id, marketplace), {})
            listing_row = listing_lookup.get((sku_id, marketplace), {})

            net_profit = to_float(pnl_row.get("Net_Profit"))
            profit_margin = to_float(pnl_row.get("Profit_Margin_Percent"))
            orders = int(round(to_float(pnl_row.get("Orders"))))
            units_sold = to_float(pnl_row.get("Units_Sold"))
            roas = to_float(pnl_row.get("ROAS"))
            acos = to_float(pnl_row.get("ACOS"))
            average_rating = to_float(review_row.get("Average_Rating"))
            has_rating = clean_text(review_row.get("Average_Rating")) != ""
            low_rating_count = int(round(to_float(review_row.get("Negative_Reviews"))))
            return_count = int(round(to_float(pnl_row.get("Return_Count"))))
            cancelled_count = int(round(to_float(pnl_row.get("Cancelled_Count"))))
            listing_status = normalize_listing_status(listing_row.get("Listing_Status"))
            ad_spend = to_float(pnl_row.get("Ad_Spend"))
            ad_sales = to_float(pnl_row.get("Ad_Sales"))

            profit = profit_score(net_profit, profit_margin)
            sales = sales_score(orders)
            rating = rating_score(average_rating, has_rating)
            returns = return_score(return_count)
            ads = ad_score(roas, (ad_spend > 0 or ad_sales > 0))
            listing = listing_stability_score(listing_status)
            total_score = profit + sales + rating + returns + ads + listing

            reason = build_reason(
                [
                    f"Profit {profit}/30",
                    f"Sales {sales}/20",
                    f"Rating {rating}/15",
                    f"Returns {returns}/15",
                    f"Ads {ads}/10",
                    f"Listing {listing}/10",
                ]
            )

            rows.append(
                [
                    sku_id,
                    marketplace,
                    format_number(net_profit),
                    format_percent(profit_margin),
                    str(orders),
                    format_number(units_sold),
                    format_percent(roas),
                    format_percent(acos),
                    format_percent(average_rating),
                    str(low_rating_count),
                    str(return_count),
                    str(cancelled_count),
                    listing_status,
                    str(profit),
                    str(sales),
                    str(rating),
                    str(returns),
                    str(ads),
                    str(listing),
                    str(total_score),
                    recommended_action(total_score),
                    reason,
                    timestamp,
                ]
            )

    return rows


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


def clear_data_rows(sheets_service, spreadsheet_id: str) -> None:
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{TAB_NAME}!A2:{zero_based_column_index_to_a1(len(HEADERS) - 1)}",
        body={},
    ).execute()


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
            fieldnames=["timestamp", "sku_id", "marketplace", "total_score", "recommended_action", "status", "message"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_sku_scorecard() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    sheet_id = ensure_tab(sheets_service, spreadsheet_id, TAB_NAME)
    existing_headers = get_sheet_values(sheets_service, spreadsheet_id, f"{TAB_NAME}!A1:W1")
    headers_written = ensure_headers(
        sheets_service,
        spreadsheet_id,
        existing_headers[0] if existing_headers else [],
    )

    master_skus = get_master_skus(sheets_service, spreadsheet_id)
    _, pnl_rows = read_table(sheets_service, spreadsheet_id, PNL_TAB)
    _, review_rows = read_table(sheets_service, spreadsheet_id, REVIEW_TAB)
    _, listing_rows = read_table(sheets_service, spreadsheet_id, LISTING_TAB)

    pnl_lookup = build_lookup(pnl_rows, ("SKU_ID", "Marketplace"))
    review_lookup = build_lookup(review_rows, ("SKU_ID", "Marketplace"))
    listing_lookup = build_lookup(listing_rows, ("SKU_ID", "Marketplace"))

    scorecard_rows = build_scorecard_rows(master_skus, pnl_lookup, review_lookup, listing_lookup)

    clear_data_rows(sheets_service, spreadsheet_id)
    if scorecard_rows:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{TAB_NAME}!A2:{zero_based_column_index_to_a1(len(HEADERS) - 1)}{len(scorecard_rows) + 1}",
            valueInputOption="RAW",
            body={"values": scorecard_rows},
        ).execute()

    apply_formatting(sheets_service, spreadsheet_id, sheet_id)

    log_rows = [
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "sku_id": row[0],
            "marketplace": row[1],
            "total_score": row[19],
            "recommended_action": row[20],
            "status": "calculated",
            "message": "SKU scorecard row calculated",
        }
        for row in scorecard_rows
    ]
    append_log_rows(log_rows)

    print("SKU scorecard summary:")
    print(f"  Rows read from SKU_PNL: {len(pnl_rows)}")
    print(f"  Rows read from REVIEW_RATING: {len(review_rows)}")
    print(f"  Rows read from LISTING_STATUS: {len(listing_rows)}")
    print(f"  Rows written to SKU_SCORECARD: {len(scorecard_rows)}")

    return {
        "spreadsheet_id": spreadsheet_id,
        "scorecard_sheet_name": TAB_NAME,
        "scorecard_sheet_id": sheet_id,
        "headers_written": headers_written,
        "master_sku_count": len(master_skus),
        "pnl_rows_read": len(pnl_rows),
        "review_rows_read": len(review_rows),
        "listing_rows_read": len(listing_rows),
        "rows_written": len(scorecard_rows),
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_sku_scorecard()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
