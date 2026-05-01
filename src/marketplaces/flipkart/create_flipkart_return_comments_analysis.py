from __future__ import annotations

import csv
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    NORMALIZED_ORDERS_PATH,
    OUTPUT_DIR,
    append_csv_log,
    build_status_payload,
    clean_fsn,
    ensure_directories,
    load_json,
    normalize_text,
    now_iso,
    parse_int,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
RAW_RETURNS_PATH = PROJECT_ROOT / "data" / "input" / "marketplaces" / "flipkart" / "raw" / "Returns Report.csv"

LOG_PATH = LOG_DIR / "flipkart_return_comments_analysis_log.csv"

DETAIL_TAB = "FLIPKART_RETURN_COMMENTS"
SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
PIVOT_TAB = "FLIPKART_RETURN_REASON_PIVOT"

SOURCE_TABS = ["FLIPKART_SKU_ANALYSIS", "FLIPKART_ACTIVE_TASKS", "FLIPKART_ACTION_TRACKER"]
OUTPUT_TABS = [DETAIL_TAB, SUMMARY_TAB, PIVOT_TAB]

LOCAL_DETAIL_PATH = OUTPUT_DIR / "flipkart_return_comments.csv"
LOCAL_SUMMARY_PATH = OUTPUT_DIR / "flipkart_return_issue_summary.csv"
LOCAL_PIVOT_PATH = OUTPUT_DIR / "flipkart_return_reason_pivot.csv"

DETAIL_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Order_ID",
    "Order_Item_ID",
    "Return_ID",
    "Total_Price",
    "Quantity",
    "Return_Requested_Date",
    "Return_Approval_Date",
    "Completed_Date",
    "Return_Status",
    "Completion_Status",
    "Return_Type",
    "Return_Reason",
    "Return_Sub_Reason",
    "Comments",
    "Issue_Category",
    "Issue_Severity",
    "Issue_Source",
    "Suggested_Action",
    "Source_File",
    "Last_Updated",
]

SUMMARY_HEADERS = [
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Total_Returns_In_Detailed_Report",
    "Customer_Return_Count",
    "Courier_Return_Count",
    "Unknown_Return_Count",
    "Top_Issue_Category",
    "Top_Return_Reason",
    "Top_Return_Sub_Reason",
    "Comments_Count",
    "Critical_Issue_Count",
    "High_Issue_Count",
    "Medium_Issue_Count",
    "Low_Issue_Count",
    "Product_Issue_Count",
    "Logistics_Issue_Count",
    "Customer_RTO_Count",
    "Suggested_Return_Action",
    "Return_Action_Priority",
    "Last_Updated",
]

PIVOT_HEADERS = [
    "Issue_Category",
    "Return_Reason",
    "Return_Sub_Reason",
    "Return_Count",
    "FSN_Count",
    "Top_FSNs",
    "Suggested_Action",
]

SOURCE_FIELD_MAP = {
    "Reason": "Reason",
    "Sub Reason": "Sub Reason",
    "Comments": "Comments",
}

ISSUE_RULES: List[Tuple[str, Sequence[str], str]] = [
    (
        "Return Fraud / Suspicious",
        ("empty box", "used product", "missing item", "fake", "fraud"),
        "Raise Claim / Investigate",
    ),
    (
        "Wrong Product",
        ("wrong", "mismatch", "different product", "incorrect item"),
        "Check Picking/Packing",
    ),
    (
        "Damaged Product",
        ("damaged", "broken", "crack", "dent", "physical damage"),
        "Improve Packaging / Courier Handling",
    ),
    (
        "Product Not Working",
        ("not working", "dead", "defective", "stopped working", "no light", "not lighting", "faulty"),
        "Check QC / Supplier / Product Defect",
    ),
    (
        "Quality Issue",
        ("poor quality", "cheap", "bad quality", "low quality"),
        "Improve Product Quality / Listing Claims",
    ),
    (
        "Size / Expectation Mismatch",
        ("size", "small", "big", "not as expected", "expectation", "color mismatch", "brightness", "low light"),
        "Improve Photos, Dimensions, Description",
    ),
    (
        "Customer Refused / RTO",
        ("customer refused", "refused", "customer not available", "address", "door locked", "undelivered", "rto"),
        "Check COD / Delivery Confirmation",
    ),
    (
        "Logistics / Courier",
        ("hub", "eob", "courier", "delivery", "shipment", "logistics", "pickup", "delivered", "non functional", "ageing"),
        "Logistics Issue, Monitor Separately",
    ),
]

ISSUE_PRIORITY = [rule[0] for rule in ISSUE_RULES] + ["Other"]
SEVERITY_BY_CATEGORY = {
    "Return Fraud / Suspicious": "Critical",
    "Product Not Working": "Critical",
    "Damaged Product": "Critical",
    "Wrong Product": "Critical",
    "Quality Issue": "High",
    "Size / Expectation Mismatch": "High",
    "Customer Refused / RTO": "High",
    "Logistics / Courier": "Medium",
    "Other": "Low",
}
CATEGORY_TO_ACTION = {category: action for category, _, action in ISSUE_RULES}
CATEGORY_TO_ACTION["Other"] = "Review Manually"
PRODUCT_ISSUE_CATEGORIES = {
    "Product Not Working",
    "Damaged Product",
    "Wrong Product",
    "Quality Issue",
    "Size / Expectation Mismatch",
}
LOGISTICS_ISSUE_CATEGORIES = {"Logistics / Courier"}
CUSTOMER_RTO_CATEGORIES = {"Customer Refused / RTO"}
SEVERITY_ORDER = ["Critical", "High", "Medium", "Low"]
SOURCE_PRIORITY = ["Reason", "Sub Reason", "Comments"]


def retry(func, attempts: int = 4):
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


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def find_sheet_id(sheets_service, spreadsheet_id: str, tab_name: str) -> int | None:
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
        raise ValueError("Column index must be 1 or greater")
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


def freeze_bold_resize_and_filter(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    column_count: int,
    row_count: int,
) -> None:
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
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": max(row_count, 1),
                        "startColumnIndex": 0,
                        "endColumnIndex": column_count,
                    }
                }
            }
        },
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def read_csv_table(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = read_csv_rows(path)
    if not rows:
        return [], []
    headers = list(rows[0].keys())
    return headers, rows


def pick_first(*values: Any) -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return ""


def build_identity_lookup(rows: Sequence[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in lookup:
            continue
        lookup[fsn] = {
            "SKU_ID": normalize_text(row.get("SKU_ID", "")),
            "Product_Title": normalize_text(row.get("Product_Title", "")),
        }
    return lookup


def order_item_variants(value: Any) -> List[str]:
    text = normalize_text(value).upper()
    if not text:
        return []
    variants = [text]
    stripped = text.removeprefix("OI:").removeprefix("OI").lstrip(":-_ ")
    if stripped and stripped not in variants:
        variants.append(stripped)
    if not text.startswith("OI:"):
        prefixed = f"OI:{stripped}" if stripped else f"OI:{text}"
        if prefixed not in variants:
            variants.append(prefixed)
    return variants


def build_order_item_lookup(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    rows = read_csv_rows(path)
    lookup: Dict[str, str] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        order_item_id = normalize_text(row.get("Order_Item_ID", ""))
        if not fsn or not order_item_id:
            continue
        for variant in order_item_variants(order_item_id):
            lookup.setdefault(variant, fsn)
    return lookup


def keyword_hits(text: str, keywords: Sequence[str]) -> int:
    if not text:
        return 0
    text_lower = text.lower()
    return sum(1 for keyword in keywords if keyword in text_lower)


def classify_issue(fields: Dict[str, str]) -> Tuple[str, str, str, str]:
    field_hits: Dict[str, List[str]] = {}
    for field_name, text in fields.items():
        field_hits[field_name] = []
        for category, keywords, _ in ISSUE_RULES:
            if keyword_hits(text, keywords):
                field_hits[field_name].append(category)

    category_scores: List[Dict[str, Any]] = []
    for priority, (category, keywords, action) in enumerate(ISSUE_RULES):
        hit_count = 0
        matching_fields: List[str] = []
        for field_name, text in fields.items():
            matches = keyword_hits(text, keywords)
            if matches:
                hit_count += matches
                matching_fields.append(field_name)
        if hit_count > 0:
            category_scores.append(
                {
                    "category": category,
                    "priority": priority,
                    "hits": hit_count,
                    "fields": matching_fields,
                    "action": action,
                }
            )

    if category_scores:
        chosen = max(category_scores, key=lambda item: (item["hits"], -item["priority"]))
        category = chosen["category"]
        action = chosen["action"]
        matching_fields = chosen["fields"]
    else:
        category = "Other"
        action = CATEGORY_TO_ACTION["Other"]
        matching_fields = []

    if category == "Other":
        source_fields = [field for field, text in fields.items() if normalize_text(text)]
        if len(source_fields) > 1:
            source = "Mixed"
        elif source_fields:
            source = source_fields[0]
        else:
            source = "Reason"
    else:
        if len(matching_fields) > 1:
            source = "Mixed"
        elif matching_fields:
            source = matching_fields[0]
        else:
            source = "Reason"

    if source in SOURCE_FIELD_MAP:
        source = SOURCE_FIELD_MAP[source]

    severity = SEVERITY_BY_CATEGORY.get(category, "Low")
    return category, severity, source, action


def build_detail_rows(
    raw_rows: Sequence[Dict[str, str]],
    sku_lookup: Dict[str, Dict[str, str]],
    order_item_lookup: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], int]:
    detail_rows: List[Dict[str, Any]] = []
    unmapped_rows = 0
    run_id = f"FLIPKART_RETURN_COMMENTS_{now_iso().replace(':', '').replace('-', '').replace('T', '_')}"
    timestamp = now_iso()

    for row in raw_rows:
        raw_fsn = clean_fsn(row.get("FSN", ""))
        order_item_id = pick_first(row.get("Order Item ID"), row.get("Order_Item_ID"))
        mapped_fsn = ""
        if raw_fsn and raw_fsn in sku_lookup:
            mapped_fsn = raw_fsn
        elif not raw_fsn and order_item_id:
            for variant in order_item_variants(order_item_id):
                candidate = order_item_lookup.get(variant, "")
                if candidate and candidate in sku_lookup:
                    mapped_fsn = candidate
                    break

        if not mapped_fsn:
            unmapped_rows += 1
            continue

        identity = sku_lookup.get(mapped_fsn, {})
        row_fields = {
            "Reason": pick_first(row.get("Return Reason"), row.get("Return_Reason")),
            "Sub Reason": pick_first(row.get("Return Sub-reason"), row.get("Return_Sub_Reason"), row.get("Return Sub Reason")),
            "Comments": pick_first(row.get("Comments"), row.get("Comment")),
        }
        issue_category, issue_severity, issue_source, suggested_action = classify_issue(row_fields)

        detail_rows.append(
            {
                "Run_ID": run_id,
                "FSN": mapped_fsn,
                "SKU_ID": pick_first(row.get("SKU"), identity.get("SKU_ID")),
                "Product_Title": pick_first(row.get("Product"), identity.get("Product_Title")),
                "Order_ID": pick_first(row.get("Order ID"), row.get("Order_ID")),
                "Order_Item_ID": order_item_id,
                "Return_ID": pick_first(row.get("Return ID"), row.get("Return_ID")),
                "Total_Price": normalize_text(row.get("Total Price", "")),
                "Quantity": str(parse_int(row.get("Quantity", ""))) if normalize_text(row.get("Quantity", "")) else "",
                "Return_Requested_Date": pick_first(row.get("Return Requested Date"), row.get("Return_Requested_Date")),
                "Return_Approval_Date": pick_first(row.get("Return Approval Date"), row.get("Return_Approval_Date")),
                "Completed_Date": pick_first(row.get("Completed Date"), row.get("Completed_Date")),
                "Return_Status": pick_first(row.get("Return Status"), row.get("Return_Status")),
                "Completion_Status": pick_first(row.get("Completion Status"), row.get("Completion_Status")),
                "Return_Type": pick_first(row.get("Return Type"), row.get("Return_Type")),
                "Return_Reason": row_fields["Reason"],
                "Return_Sub_Reason": row_fields["Sub Reason"],
                "Comments": row_fields["Comments"],
                "Issue_Category": issue_category,
                "Issue_Severity": issue_severity,
                "Issue_Source": issue_source,
                "Suggested_Action": suggested_action,
                "Source_File": RAW_RETURNS_PATH.name,
                "Last_Updated": timestamp,
            }
        )

    return detail_rows, unmapped_rows


def build_summary_rows(detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def normalize_return_type_value(value: Any) -> str:
        normalized = normalize_text(value).lower()
        if normalized in {"customer_return", "customer return", "customer"}:
            return "customer_return"
        if normalized in {"courier_return", "courier return", "courier", "rto", "return to origin", "return_to_origin", "logistics return"}:
            return "courier_return"
        return "unknown_return"

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        grouped[clean_fsn(row.get("FSN", ""))].append(row)

    summary_rows: List[Dict[str, Any]] = []
    for fsn, rows in grouped.items():
        sku_id = pick_first(*(row.get("SKU_ID", "") for row in rows))
        product_title = pick_first(*(row.get("Product_Title", "") for row in rows))
        issue_counter = Counter(normalize_text(row.get("Issue_Category", "")) or "Other" for row in rows)
        severity_counter = Counter(normalize_text(row.get("Issue_Severity", "")) or "Low" for row in rows)
        reason_counter = Counter(normalize_text(row.get("Return_Reason", "")) for row in rows if normalize_text(row.get("Return_Reason", "")))
        sub_reason_counter = Counter(normalize_text(row.get("Return_Sub_Reason", "")) for row in rows if normalize_text(row.get("Return_Sub_Reason", "")))
        comments_count = sum(1 for row in rows if normalize_text(row.get("Comments", "")))
        customer_return_count = sum(1 for row in rows if normalize_return_type_value(row.get("Return_Type", "")) == "customer_return")
        courier_return_count = sum(1 for row in rows if normalize_return_type_value(row.get("Return_Type", "")) == "courier_return")
        unknown_return_count = max(len(rows) - customer_return_count - courier_return_count, 0)

        top_issue_category = max(
            ISSUE_PRIORITY,
            key=lambda category: (
                issue_counter.get(category, 0),
                -ISSUE_PRIORITY.index(category),
            ),
        )
        if issue_counter.get(top_issue_category, 0) == 0:
            top_issue_category = "Other"

        top_reason = reason_counter.most_common(1)[0][0] if reason_counter else ""
        top_sub_reason = sub_reason_counter.most_common(1)[0][0] if sub_reason_counter else ""
        critical_count = severity_counter.get("Critical", 0)
        high_count = severity_counter.get("High", 0)
        medium_count = severity_counter.get("Medium", 0)
        low_count = severity_counter.get("Low", 0)

        product_issue_count = sum(issue_counter.get(category, 0) for category in PRODUCT_ISSUE_CATEGORIES)
        logistics_issue_count = sum(issue_counter.get(category, 0) for category in LOGISTICS_ISSUE_CATEGORIES)
        customer_rto_count = sum(issue_counter.get(category, 0) for category in CUSTOMER_RTO_CATEGORIES)
        suggested_action = CATEGORY_TO_ACTION.get(top_issue_category, "Review Manually")

        if critical_count > 0:
            action_priority = "Critical"
        elif product_issue_count >= 2 or high_count >= 2:
            action_priority = "High"
        elif logistics_issue_count >= 2:
            action_priority = "Medium"
        else:
            action_priority = "Low"

        summary_rows.append(
            {
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": product_title,
                "Total_Returns_In_Detailed_Report": len(rows),
                "Customer_Return_Count": customer_return_count,
                "Courier_Return_Count": courier_return_count,
                "Unknown_Return_Count": unknown_return_count,
                "Top_Issue_Category": top_issue_category,
                "Top_Return_Reason": top_reason,
                "Top_Return_Sub_Reason": top_sub_reason,
                "Comments_Count": comments_count,
                "Critical_Issue_Count": critical_count,
                "High_Issue_Count": high_count,
                "Medium_Issue_Count": medium_count,
                "Low_Issue_Count": low_count,
                "Product_Issue_Count": product_issue_count,
                "Logistics_Issue_Count": logistics_issue_count,
                "Customer_RTO_Count": customer_rto_count,
                "Suggested_Return_Action": suggested_action,
                "Return_Action_Priority": action_priority,
                "Last_Updated": now_iso(),
            }
        )

    summary_rows.sort(
        key=lambda row: (
            -int(parse_int(row.get("Total_Returns_In_Detailed_Report", 0))),
            -int(parse_int(row.get("Critical_Issue_Count", 0))),
            -int(parse_int(row.get("High_Issue_Count", 0))),
            normalize_text(row.get("FSN", "")),
        )
    )
    return summary_rows


def build_pivot_rows(detail_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        key = (
            normalize_text(row.get("Issue_Category", "")) or "Other",
            normalize_text(row.get("Return_Reason", "")),
            normalize_text(row.get("Return_Sub_Reason", "")),
        )
        grouped[key].append(row)

    pivot_rows: List[Dict[str, Any]] = []
    for (category, reason, sub_reason), rows in grouped.items():
        fsn_counter = Counter(clean_fsn(row.get("FSN", "")) for row in rows if clean_fsn(row.get("FSN", "")))
        top_fsns = [fsn for fsn, _ in fsn_counter.most_common(5)]
        pivot_rows.append(
            {
                "Issue_Category": category,
                "Return_Reason": reason,
                "Return_Sub_Reason": sub_reason,
                "Return_Count": len(rows),
                "FSN_Count": len(fsn_counter),
                "Top_FSNs": "; ".join(top_fsns),
                "Suggested_Action": CATEGORY_TO_ACTION.get(category, "Review Manually"),
            }
        )

    pivot_rows.sort(
        key=lambda row: (
            -int(parse_int(row.get("Return_Count", 0))),
            ISSUE_PRIORITY.index(normalize_text(row.get("Issue_Category", ""))) if normalize_text(row.get("Issue_Category", "")) in ISSUE_PRIORITY else len(ISSUE_PRIORITY),
            normalize_text(row.get("Return_Reason", "")),
            normalize_text(row.get("Return_Sub_Reason", "")),
        )
    )
    return pivot_rows


def write_local_outputs(detail_rows: Sequence[Dict[str, Any]], summary_rows: Sequence[Dict[str, Any]], pivot_rows: Sequence[Dict[str, Any]]) -> None:
    for path in [LOCAL_DETAIL_PATH, LOCAL_SUMMARY_PATH, LOCAL_PIVOT_PATH]:
        path.parent.mkdir(parents=True, exist_ok=True)
    with LOCAL_DETAIL_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=DETAIL_HEADERS)
        writer.writeheader()
        writer.writerows([{header: row.get(header, "") for header in DETAIL_HEADERS} for row in detail_rows])
    with LOCAL_SUMMARY_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_HEADERS)
        writer.writeheader()
        writer.writerows([{header: row.get(header, "") for header in SUMMARY_HEADERS} for row in summary_rows])
    with LOCAL_PIVOT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PIVOT_HEADERS)
        writer.writeheader()
        writer.writerows([{header: row.get(header, "") for header in PIVOT_HEADERS} for row in pivot_rows])


def write_sheet_tab(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> int:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_bold_resize_and_filter(sheets_service, spreadsheet_id, sheet_id, len(headers), len(rows) + 1)
    return sheet_id


def load_source_tabs(sheets_service, spreadsheet_id: str) -> Dict[str, Dict[str, Any]]:
    tab_payload: Dict[str, Dict[str, Any]] = {}
    for tab_name in SOURCE_TABS:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)
        headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
        tab_payload[tab_name] = {"headers": headers, "rows": rows}
    return tab_payload


def create_flipkart_return_comments_analysis() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    if not RAW_RETURNS_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {RAW_RETURNS_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    source_tabs = load_source_tabs(sheets_service, spreadsheet_id)
    sku_analysis_rows = source_tabs["FLIPKART_SKU_ANALYSIS"]["rows"]
    sku_lookup = build_identity_lookup(sku_analysis_rows)
    order_item_lookup = build_order_item_lookup(NORMALIZED_ORDERS_PATH)

    raw_headers, raw_rows = read_csv_table(RAW_RETURNS_PATH)
    detail_rows, unmapped_rows = build_detail_rows(raw_rows, sku_lookup, order_item_lookup)
    summary_rows = build_summary_rows(detail_rows)
    pivot_rows = build_pivot_rows(detail_rows)

    write_local_outputs(detail_rows, summary_rows, pivot_rows)

    write_sheet_tab(sheets_service, spreadsheet_id, DETAIL_TAB, DETAIL_HEADERS, detail_rows)
    write_sheet_tab(sheets_service, spreadsheet_id, SUMMARY_TAB, SUMMARY_HEADERS, summary_rows)
    write_sheet_tab(sheets_service, spreadsheet_id, PIVOT_TAB, PIVOT_HEADERS, pivot_rows)

    issue_distribution = Counter(row.get("Issue_Category", "") or "Other" for row in detail_rows)
    severity_distribution = Counter(row.get("Issue_Severity", "") or "Low" for row in detail_rows)
    fsn_counter: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in detail_rows:
        fsn_counter[clean_fsn(row.get("FSN", ""))].append(row)
    top_10_issue_fsns = [
        {
            "FSN": fsn,
            "return_count": len(rows),
            "top_issue_category": Counter(normalize_text(row.get("Issue_Category", "")) or "Other" for row in rows).most_common(1)[0][0],
        }
        for fsn, rows in sorted(fsn_counter.items(), key=lambda item: (-len(item[1]), item[0]))[:10]
    ]

    log_row = {
        "timestamp": now_iso(),
        "status": "SUCCESS",
        "run_id": detail_rows[0]["Run_ID"] if detail_rows else "",
        "raw_return_rows": len(raw_rows),
        "target_fsn_return_rows": len(detail_rows),
        "unmapped_rows": unmapped_rows,
        "return_comments_rows_written": len(detail_rows),
        "return_issue_summary_rows": len(summary_rows),
        "return_reason_pivot_rows": len(pivot_rows),
        "issue_category_distribution": json.dumps(dict(issue_distribution), ensure_ascii=False),
        "severity_distribution": json.dumps(dict(severity_distribution), ensure_ascii=False),
        "top_10_issue_fsns": json.dumps(top_10_issue_fsns, ensure_ascii=False),
        "tabs_updated": json.dumps(OUTPUT_TABS, ensure_ascii=False),
        "log_path": str(LOG_PATH),
        "source_tabs": json.dumps({tab: len(payload["rows"]) for tab, payload in source_tabs.items()}, ensure_ascii=False),
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "status",
            "run_id",
            "raw_return_rows",
            "target_fsn_return_rows",
            "unmapped_rows",
            "return_comments_rows_written",
            "return_issue_summary_rows",
            "return_reason_pivot_rows",
            "issue_category_distribution",
            "severity_distribution",
            "top_10_issue_fsns",
            "tabs_updated",
            "log_path",
            "source_tabs",
        ],
        [log_row],
    )

    summary = {
        "status": "SUCCESS",
        "generated_at": now_iso(),
        "run_id": log_row["run_id"],
        "raw_return_rows": len(raw_rows),
        "target_fsn_return_rows": len(detail_rows),
        "unmapped_rows": unmapped_rows,
        "return_comments_rows_written": len(detail_rows),
        "return_issue_summary_rows": len(summary_rows),
        "return_reason_pivot_rows": len(pivot_rows),
        "issue_category_distribution": dict(issue_distribution),
        "severity_distribution": dict(severity_distribution),
        "top_10_issue_fsns": top_10_issue_fsns,
        "tabs_updated": OUTPUT_TABS,
        "log_path": str(LOG_PATH),
        "local_outputs": {
            "return_comments": str(LOCAL_DETAIL_PATH),
            "return_issue_summary": str(LOCAL_SUMMARY_PATH),
            "return_reason_pivot": str(LOCAL_PIVOT_PATH),
        },
        "source_tabs": {tab: len(payload["rows"]) for tab, payload in source_tabs.items()},
    }
    payload = dict(summary)
    payload.pop("status", None)
    print(json.dumps(build_status_payload("SUCCESS", **payload), indent=2, ensure_ascii=False))
    return summary


def main() -> None:
    try:
        create_flipkart_return_comments_analysis()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "raw_returns_path": str(RAW_RETURNS_PATH),
                    "log_path": str(LOG_PATH),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
