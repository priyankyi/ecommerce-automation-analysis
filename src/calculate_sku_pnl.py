from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "sku_pnl_log.csv"

MASTER_SKU_TAB = "MASTER_SKU"
ORDER_MASTER_TAB = "ORDER_MASTER"
SETTLEMENT_MASTER_TAB = "SETTLEMENT_MASTER"
ADS_MASTER_TAB = "ADS_MASTER"
PNL_TAB = "SKU_PNL"

HEADERS = [
    "SKU_ID",
    "Marketplace",
    "Orders",
    "Units_Sold",
    "Gross_Sales",
    "Net_Settlement",
    "Product_Cost",
    "Total_COGS",
    "Ad_Spend",
    "Ad_Sales",
    "Marketplace_Fee",
    "Shipping_Fee",
    "GST",
    "TCS",
    "TDS",
    "Adjustment",
    "Net_Profit",
    "Profit_Per_Order",
    "Profit_Margin_Percent",
    "ROAS",
    "ACOS",
    "Return_Count",
    "Cancelled_Count",
    "Last_Updated",
    "Decision_Basis",
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


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[str]]:
    response = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


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


def format_ratio(numerator: float, denominator: float) -> str:
    if denominator == 0:
        return "0"
    return f"{numerator / denominator:.4f}"


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if len(rows) < 2:
        return [], []

    headers = rows[0]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        entry = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
        data.append(entry)
    return headers, data


def get_master_costs(master_rows: List[Dict[str, str]]) -> Dict[str, float]:
    costs: Dict[str, float] = {}
    for row in master_rows:
        sku_id = clean_text(row.get("SKU_ID"))
        if not sku_id:
            continue
        if sku_id in costs:
            continue
        costs[sku_id] = to_float(row.get("Cost_Price"))
    return costs


def aggregate_order_data(order_rows: List[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, object]]:
    grouped: Dict[Tuple[str, str], Dict[str, object]] = {}
    for row in order_rows:
        sku_id = clean_text(row.get("SKU_ID"))
        marketplace = clean_text(row.get("Marketplace"))
        order_id = clean_text(row.get("Order_ID"))
        if not sku_id or not marketplace:
            continue
        key = (sku_id, marketplace)
        bucket = grouped.setdefault(
            key,
            {
                "order_ids": set(),
                "units_sold": 0.0,
                "gross_sales": 0.0,
                "return_count": 0,
                "cancelled_count": 0,
            },
        )
        if order_id:
            bucket["order_ids"].add(order_id)
        quantity = to_float(row.get("Quantity"))
        selling_price = to_float(row.get("Selling_Price"))
        bucket["units_sold"] += quantity
        bucket["gross_sales"] += selling_price * quantity
        if clean_text(row.get("Return_Status")):
            bucket["return_count"] += 1
        if clean_text(row.get("Order_Status")).lower() == "cancelled":
            bucket["cancelled_count"] += 1
    return grouped


def aggregate_settlement_data(settlement_rows: List[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, float]]:
    grouped: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
        lambda: {
            "net_settlement": 0.0,
            "marketplace_fee": 0.0,
            "shipping_fee": 0.0,
            "gst": 0.0,
            "tcs": 0.0,
            "tds": 0.0,
            "adjustment": 0.0,
        }
    )
    for row in settlement_rows:
        sku_id = clean_text(row.get("SKU_ID"))
        marketplace = clean_text(row.get("Marketplace"))
        if not sku_id or not marketplace:
            continue
        bucket = grouped[(sku_id, marketplace)]
        bucket["net_settlement"] += to_float(row.get("Net_Settlement"))
        bucket["marketplace_fee"] += to_float(row.get("Marketplace_Fee"))
        bucket["shipping_fee"] += to_float(row.get("Shipping_Fee"))
        bucket["gst"] += to_float(row.get("GST"))
        bucket["tcs"] += to_float(row.get("TCS"))
        bucket["tds"] += to_float(row.get("TDS"))
        bucket["adjustment"] += to_float(row.get("Adjustment"))
    return grouped


def aggregate_ads_data(ads_rows: List[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, float]]:
    grouped: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
        lambda: {
            "ad_spend": 0.0,
            "ad_sales": 0.0,
        }
    )
    for row in ads_rows:
        sku_id = clean_text(row.get("SKU_ID"))
        marketplace = clean_text(row.get("Marketplace"))
        if not sku_id or not marketplace:
            continue
        bucket = grouped[(sku_id, marketplace)]
        bucket["ad_spend"] += to_float(row.get("Ad_Spend"))
        bucket["ad_sales"] += to_float(row.get("Ad_Sales"))
    return grouped


def build_pnl_rows(
    master_costs: Dict[str, float],
    order_data: Dict[Tuple[str, str], Dict[str, object]],
    settlement_data: Dict[Tuple[str, str], Dict[str, float]],
    ads_data: Dict[Tuple[str, str], Dict[str, float]],
) -> List[List[str]]:
    keys = set(order_data) | set(settlement_data) | set(ads_data)
    rows: List[List[str]] = []
    timestamp = datetime.now().isoformat(timespec="seconds")

    for sku_id, marketplace in sorted(keys):
        orders_bucket = order_data.get((sku_id, marketplace), {})
        settlement_bucket = settlement_data.get((sku_id, marketplace), {})
        ads_bucket = ads_data.get((sku_id, marketplace), {})

        orders = len(orders_bucket.get("order_ids", set()))
        units_sold = float(orders_bucket.get("units_sold", 0.0))
        gross_sales = float(orders_bucket.get("gross_sales", 0.0))
        net_settlement = settlement_bucket.get("net_settlement", 0.0)
        product_cost = master_costs.get(sku_id, 0.0)
        total_cogs = product_cost * units_sold
        ad_spend = ads_bucket.get("ad_spend", 0.0)
        ad_sales = ads_bucket.get("ad_sales", 0.0)
        marketplace_fee = settlement_bucket.get("marketplace_fee", 0.0)
        shipping_fee = settlement_bucket.get("shipping_fee", 0.0)
        gst = settlement_bucket.get("gst", 0.0)
        tcs = settlement_bucket.get("tcs", 0.0)
        tds = settlement_bucket.get("tds", 0.0)
        adjustment = settlement_bucket.get("adjustment", 0.0)
        net_profit = net_settlement - total_cogs - ad_spend
        profit_per_order = net_profit / orders if orders else 0.0
        profit_margin_percent = net_profit / gross_sales if gross_sales else 0.0
        roas = ad_sales / ad_spend if ad_spend else 0.0
        acos = ad_spend / ad_sales if ad_sales else 0.0
        return_count = int(orders_bucket.get("return_count", 0))
        cancelled_count = int(orders_bucket.get("cancelled_count", 0))

        rows.append(
            [
                sku_id,
                marketplace,
                str(orders),
                format_number(units_sold),
                format_number(gross_sales),
                format_number(net_settlement),
                format_number(product_cost),
                format_number(total_cogs),
                format_number(ad_spend),
                format_number(ad_sales),
                format_number(marketplace_fee),
                format_number(shipping_fee),
                format_number(gst),
                format_number(tcs),
                format_number(tds),
                format_number(adjustment),
                format_number(net_profit),
                format_ratio(net_profit, orders) if orders else "0",
                format_ratio(net_profit, gross_sales) if gross_sales else "0",
                format_ratio(ad_sales, ad_spend) if ad_spend else "0",
                format_ratio(ad_spend, ad_sales) if ad_sales else "0",
                str(return_count),
                str(cancelled_count),
                timestamp,
                "Python aggregated from MASTER_SKU, ORDER_MASTER, SETTLEMENT_MASTER, ADS_MASTER",
            ]
        )

    return rows


def ensure_pnl_tab(sheets_service, spreadsheet_id: str) -> int:
    sheet_meta = (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )
    for sheet in sheet_meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == PNL_TAB:
            return props["sheetId"]

    response = (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": PNL_TAB}}}]},
        )
        .execute()
    )
    return response["replies"][0]["addSheet"]["properties"]["sheetId"]


def clear_data_rows(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int) -> None:
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": column_count,
                        },
                        "fields": "userEnteredValue",
                    }
                }
            ]
        },
    ).execute()


def apply_formatting(sheets_service, spreadsheet_id: str, sheet_id: int, column_count: int) -> None:
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
                            "endIndex": column_count,
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
            fieldnames=[
                "timestamp",
                "sku_id",
                "marketplace",
                "orders",
                "units_sold",
                "net_profit",
                "status",
                "message",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_sku_pnl() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    pnl_sheet_id = ensure_pnl_tab(sheets_service, spreadsheet_id)
    headers_written = ensure_pnl_tab_headers(sheets_service, spreadsheet_id)

    master_headers, master_rows = read_table(sheets_service, spreadsheet_id, MASTER_SKU_TAB)
    order_headers, order_rows = read_table(sheets_service, spreadsheet_id, ORDER_MASTER_TAB)
    settlement_headers, settlement_rows = read_table(sheets_service, spreadsheet_id, SETTLEMENT_MASTER_TAB)
    ads_headers, ads_rows = read_table(sheets_service, spreadsheet_id, ADS_MASTER_TAB)

    master_costs = get_master_costs(master_rows)
    order_data = aggregate_order_data(order_rows)
    settlement_data = aggregate_settlement_data(settlement_rows)
    ads_data = aggregate_ads_data(ads_rows)
    pnl_rows = build_pnl_rows(master_costs, order_data, settlement_data, ads_data)

    clear_data_rows(sheets_service, spreadsheet_id, pnl_sheet_id, len(HEADERS))
    if pnl_rows:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{PNL_TAB}!A2:{zero_based_column_index_to_a1(len(HEADERS) - 1)}{len(pnl_rows) + 1}",
            valueInputOption="RAW",
            body={"values": pnl_rows},
        ).execute()

    apply_formatting(sheets_service, spreadsheet_id, pnl_sheet_id, len(HEADERS))

    log_rows = [
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "sku_id": row[0],
            "marketplace": row[1],
            "orders": row[2],
            "units_sold": row[3],
            "net_profit": row[16],
            "status": "calculated",
            "message": "SKU P&L row calculated",
        }
        for row in pnl_rows
    ]
    append_log_rows(log_rows)

    print("P&L summary:")
    print(f"  Rows read from MASTER_SKU: {len(master_rows)}")
    print(f"  Rows read from ORDER_MASTER: {len(order_rows)}")
    print(f"  Rows read from SETTLEMENT_MASTER: {len(settlement_rows)}")
    print(f"  Rows read from ADS_MASTER: {len(ads_rows)}")
    print(f"  SKU-marketplace rows calculated: {len(pnl_rows)}")
    print(f"  Rows written to SKU_PNL: {len(pnl_rows)}")

    return {
        "spreadsheet_id": spreadsheet_id,
        "pnl_tab": PNL_TAB,
        "pnl_sheet_id": pnl_sheet_id,
        "headers_written": headers_written,
        "rows_written": len(pnl_rows),
        "log_path": str(LOG_PATH),
    }


def ensure_pnl_tab_headers(sheets_service, spreadsheet_id: str) -> bool:
    current = get_sheet_values(sheets_service, spreadsheet_id, f"{PNL_TAB}!A1:Y1")
    current_headers = current[0] if current else []
    if current_headers[: len(HEADERS)] != HEADERS:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{PNL_TAB}!A1:{zero_based_column_index_to_a1(len(HEADERS) - 1)}1",
            valueInputOption="RAW",
            body={"values": [HEADERS]},
        ).execute()
        return True
    return False


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if len(rows) < 2:
        return [], []

    headers = rows[0]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))})
    return headers, data


def main() -> None:
    result = ensure_sku_pnl()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
