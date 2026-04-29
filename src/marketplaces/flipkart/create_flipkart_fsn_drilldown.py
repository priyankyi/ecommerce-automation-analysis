from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    normalize_text,
    now_iso,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_fsn_drilldown_log.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
TRACKER_TAB = "FLIPKART_ACTION_TRACKER"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"
RETURN_COMMENTS_TAB = "FLIPKART_RETURN_COMMENTS"
RETURN_ISSUE_SUMMARY_TAB = "FLIPKART_RETURN_ISSUE_SUMMARY"
RETURN_REASON_PIVOT_TAB = "FLIPKART_RETURN_REASON_PIVOT"

DRILLDOWN_TAB = "FLIPKART_FSN_DRILLDOWN"

LOCAL_RETURNS_PATH = OUTPUT_DIR / "normalized_returns.csv"

VISIBLE_HEADERS = 17
HELPER_START_COLUMN = 29  # AC
HELPER_END_COLUMN = 38  # AL

SECTION_TITLES = {
    2: "Section 2: Identity",
    3: "Section 3: Business Metrics",
    4: "Section 4: Alert Summary",
    5: "Section 5: Active Alerts for Selected FSN",
    6: "Section 6: Return Details for Selected FSN",
    7: "Section 7: Historical Trend for Selected FSN",
    8: "Section 8: Return Issue Intelligence",
}

IDENTITY_FIELDS = [
    ("FSN", "A", "B"),
    ("SKU_ID", "B", "B"),
    ("Product_Title", "C", "C"),
    ("Category", "D", "D"),
    ("Listing_Status", "E", "E"),
    ("Data_Confidence", "AU", "AU"),
    ("Final_Action", "AV", "AV"),
    ("Reason", "AW", "AW"),
]

BUSINESS_FIELDS = [
    ("Orders", "K"),
    ("Units_Sold", "L"),
    ("Gross_Sales", "M"),
    ("Returns", "N"),
    ("Return_Rate", "O"),
    ("Net_Settlement", "Q"),
    ("Flipkart_Net_Earnings", "AD"),
    ("Net_Profit_Before_COGS", "AP"),
    ("Profit_Per_Order_Before_COGS", "AQ"),
    ("Profit_Margin_Before_COGS", "AR"),
    ("Cost_Price", "AX"),
    ("Total_Unit_COGS", "BA"),
    ("Total_COGS", "BB"),
    ("Final_Net_Profit", "BC"),
    ("Final_Profit_Per_Order", "BD"),
    ("Final_Profit_Margin", "BE"),
    ("COGS_Status", "BF"),
]

ALERT_SUMMARY_FIELDS = [
    ("Critical Alerts", "Critical"),
    ("High Alerts", "High"),
    ("Medium Alerts", "Medium"),
    ("Low Alerts", "Low"),
    ("Open Tasks", "Open"),
    ("In Progress Tasks", "In Progress"),
    ("Resolved Tasks", "Resolved"),
]

ACTIVE_ALERT_HEADERS = [
    "Alert_ID",
    "Severity",
    "Alert_Type",
    "Suggested_Action",
    "Reason",
    "Trigger_Value",
    "Status",
    "Owner",
    "Days_Open",
]

RETURN_HEADERS = [
    "Order_Item_ID",
    "Return_ID",
    "Return_Status",
    "Return_Reason",
    "Return_Sub_Reason",
    "Comments",
    "Return_Date",
]

HISTORY_HEADERS = [
    "Run_ID",
    "Report_Start_Date",
    "Report_End_Date",
    "Orders",
    "Returns",
    "Return_Rate",
    "Net_Settlement",
    "Net_Profit_Before_COGS",
    "Data_Confidence",
    "Final_Action",
]

LOG_HEADERS = [
    "timestamp",
    "spreadsheet_id",
    "fsn_count",
    "default_selected_fsn",
    "sections_created",
    "tab_updated",
    "status",
    "message",
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


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


def ensure_required_tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    if find_sheet_id(sheets_service, spreadsheet_id, tab_name) is None:
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
        .clear(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:AL", body={})
        .execute()
    )


def ensure_sheet_grid(sheets_service, spreadsheet_id: str, sheet_id: int, *, row_count: int = 1000, column_count: int = 40) -> None:
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "rowCount": row_count,
                        "columnCount": column_count,
                    },
                },
                "fields": "gridProperties.rowCount,gridProperties.columnCount",
            }
        }
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def column_index_to_a1(index: int) -> str:
    if index < 1:
        raise ValueError("Column index must be 1 or greater")
    result: List[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def write_values(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    start_cell: str,
    values: Sequence[Sequence[Any]],
    value_input_option: str = "RAW",
) -> None:
    if not values:
        return
    end_col = column_index_to_a1(len(values[0]))
    end_row = len(values)
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!{start_cell}:{end_col}{int(start_cell[1:]) + end_row - 1}",
            valueInputOption=value_input_option,
            body={"values": [list(row) for row in values]},
        )
        .execute()
    )


def freeze_and_format(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    visible_columns: int,
    section_rows: Sequence[int] | None = None,
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
                    "endIndex": visible_columns,
                }
            }
        },
    ]
    for row_index in section_rows or [3, 13, 37, 61]:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_index,
                        "endRowIndex": row_index + 1,
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            }
        )
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def hide_helper_columns(sheets_service, spreadsheet_id: str, sheet_id: int) -> None:
    requests = [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": HELPER_START_COLUMN - 1,
                    "endIndex": HELPER_END_COLUMN,
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        }
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def set_dropdown_validation(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    helper_end_row: int,
) -> None:
    if helper_end_row < 2:
        return
    requests = [
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 2,
                    "startColumnIndex": 1,
                    "endColumnIndex": 2,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_RANGE",
                        "values": [
                            {
                                "userEnteredValue": f"={DRILLDOWN_TAB}!$AC$2:$AC${helper_end_row}",
                            }
                        ],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        }
    ]
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def unique_fsns(analysis_rows: Sequence[Dict[str, str]]) -> List[str]:
    seen = set()
    fsns: List[str] = []
    for row in analysis_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn or fsn in seen:
            continue
        seen.add(fsn)
        fsns.append(fsn)
    return fsns


def build_return_helper_rows() -> List[List[Any]]:
    rows = read_csv_rows(LOCAL_RETURNS_PATH)
    output: List[List[Any]] = []
    for row in rows:
        output.append(
            [
                clean_fsn(row.get("FSN", "")),
                normalize_text(row.get("Order_Item_ID", "")),
                normalize_text(row.get("Return_ID", "")),
                normalize_text(row.get("Return_Status", "")),
                normalize_text(row.get("Return_Reason", "")),
                normalize_text(row.get("Return_Sub_Reason", "")),
                normalize_text(row.get("Comments", "")),
                normalize_text(row.get("Return_Date", "")),
            ]
        )
    output.sort(key=lambda item: (item[7] or "", item[1] or "", item[2] or ""), reverse=True)
    return output


def formula_lookup(column_letter: str) -> str:
    return (
        f'=IF($B$2="","",IFERROR('
        f'INDEX(FLIPKART_SKU_ANALYSIS!${column_letter}:${column_letter}, '
        f'MATCH($B$2, FLIPKART_SKU_ANALYSIS!$A:$A, 0)), ""))'
    )


def formula_countifs(severity_or_status: str, column_letter_fsn: str, column_letter_match: str, tab_name: str, match_value: str) -> str:
    return (
        f'=IF($B$2="","",COUNTIFS({tab_name}!${column_letter_fsn}:${column_letter_fsn},$B$2,'
        f'{tab_name}!${column_letter_match}:${column_letter_match},"{match_value}"))'
    )


def header_letter_map(headers: Sequence[str]) -> Dict[str, str]:
    return {str(header): column_index_to_a1(index + 1) for index, header in enumerate(headers)}


def lookup_formula(tab_name: str, column_letter: str, match_column: str = "A") -> str:
    return (
        f'=IF($B$2="","",IFERROR('
        f'INDEX({tab_name}!${column_letter}:${column_letter}, '
        f'MATCH($B$2, {tab_name}!${match_column}:${match_column}, 0)), ""))'
    )


def build_recent_return_comments_formula(detail_columns: Dict[str, str]) -> str:
    required_columns = [
        "Return_ID",
        "Return_Reason",
        "Return_Sub_Reason",
        "Comments",
        "Issue_Category",
        "Issue_Severity",
        "Suggested_Action",
        "FSN",
        "Return_Requested_Date",
    ]
    missing = [column for column in required_columns if column not in detail_columns]
    if missing:
        return '=""'
    return (
        '=IFERROR(QUERY({'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Return_ID"]}:${detail_columns["Return_ID"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Return_Reason"]}:${detail_columns["Return_Reason"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Return_Sub_Reason"]}:${detail_columns["Return_Sub_Reason"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Comments"]}:${detail_columns["Comments"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Issue_Category"]}:${detail_columns["Issue_Category"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Issue_Severity"]}:${detail_columns["Issue_Severity"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Suggested_Action"]}:${detail_columns["Suggested_Action"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["FSN"]}:${detail_columns["FSN"]},'
        f'FLIPKART_RETURN_COMMENTS!${detail_columns["Return_Requested_Date"]}:${detail_columns["Return_Requested_Date"]}'
        '}, "select Col1,Col2,Col3,Col4,Col5,Col6,Col7 where Col8 = \'"&$B$2&"\' order by Col9 desc limit 10", 0), "")'
    )


def build_visible_grid(
    analysis_headers: Sequence[str],
    return_summary_headers: Sequence[str],
    return_comment_headers: Sequence[str],
) -> List[List[Any]]:
    analysis_columns = header_letter_map(analysis_headers)
    return_summary_columns = header_letter_map(return_summary_headers)
    return_comment_columns = header_letter_map(return_comment_headers)

    def analysis_lookup(header_name: str) -> str:
        column_letter = analysis_columns.get(header_name, "")
        if not column_letter:
            return '=""'
        return lookup_formula("FLIPKART_SKU_ANALYSIS", column_letter)

    def return_summary_lookup(header_name: str) -> str:
        column_letter = return_summary_columns.get(header_name, "")
        if not column_letter:
            return '=""'
        return lookup_formula(RETURN_ISSUE_SUMMARY_TAB, column_letter)

    rows = 125
    cols = VISIBLE_HEADERS
    grid: List[List[Any]] = [["" for _ in range(cols)] for _ in range(rows)]

    def set_cell(row: int, col: int, value: Any) -> None:
        grid[row - 1][col - 1] = value

    set_cell(1, 1, "FLIPKART_FSN_DRILLDOWN")
    set_cell(2, 1, "Select FSN")

    set_cell(4, 1, SECTION_TITLES[2])
    set_cell(4, 4, SECTION_TITLES[3])
    set_cell(4, 7, SECTION_TITLES[4])

    identity_rows = {
        5: ("FSN", "=$B$2"),
        6: ("SKU_ID", formula_lookup("B")),
        7: ("Product_Title", formula_lookup("C")),
        8: ("Category", formula_lookup("D")),
        9: ("Listing_Status", formula_lookup("E")),
        10: ("Data_Confidence", formula_lookup("AU")),
        11: ("Final_Action", formula_lookup("AV")),
        12: ("Reason", formula_lookup("AW")),
    }
    for row_index, (label, formula) in identity_rows.items():
        set_cell(row_index, 1, label)
        set_cell(row_index, 2, formula)

    business_rows = {
        5: ("Orders", analysis_lookup("Orders")),
        6: ("Units_Sold", analysis_lookup("Units_Sold")),
        7: ("Gross_Sales", analysis_lookup("Gross_Sales")),
        8: ("Returns", analysis_lookup("Returns")),
        9: ("Return_Rate", analysis_lookup("Return_Rate")),
        10: ("Net_Settlement", analysis_lookup("Net_Settlement")),
        11: ("Flipkart_Net_Earnings", analysis_lookup("Flipkart_Net_Earnings")),
        12: ("Net_Profit_Before_COGS", analysis_lookup("Net_Profit_Before_COGS")),
        13: ("Profit_Per_Order_Before_COGS", analysis_lookup("Profit_Per_Order_Before_COGS")),
        14: ("Profit_Margin_Before_COGS", analysis_lookup("Profit_Margin_Before_COGS")),
        15: ("Cost_Price", analysis_lookup("Cost_Price")),
        16: ("Total_Unit_COGS", analysis_lookup("Total_Unit_COGS")),
        17: ("Total_COGS", analysis_lookup("Total_COGS")),
        18: ("Final_Net_Profit", analysis_lookup("Final_Net_Profit")),
        19: ("Final_Profit_Per_Order", analysis_lookup("Final_Profit_Per_Order")),
        20: ("Final_Profit_Margin", analysis_lookup("Final_Profit_Margin")),
        21: ("COGS_Status", analysis_lookup("COGS_Status")),
    }
    for row_index, (label, formula) in business_rows.items():
        set_cell(row_index, 4, label)
        set_cell(row_index, 5, formula)

    alert_rows = {
        5: ("Critical Alerts", formula_countifs("fsn", "F", "J", ALERTS_TAB, "Critical")),
        6: ("High Alerts", formula_countifs("fsn", "F", "J", ALERTS_TAB, "High")),
        7: ("Medium Alerts", formula_countifs("fsn", "F", "J", ALERTS_TAB, "Medium")),
        8: ("Low Alerts", formula_countifs("fsn", "F", "J", ALERTS_TAB, "Low")),
        9: ("Open Tasks", formula_countifs("fsn", "E", "K", TRACKER_TAB, "Open")),
        10: ("In Progress Tasks", formula_countifs("fsn", "E", "K", TRACKER_TAB, "In Progress")),
        11: ("Resolved Tasks", formula_countifs("fsn", "E", "K", TRACKER_TAB, "Resolved")),
    }
    for row_index, (label, formula) in alert_rows.items():
        set_cell(row_index, 7, label)
        set_cell(row_index, 8, formula)

    set_cell(24, 1, SECTION_TITLES[5])
    for col_index, header in enumerate(ACTIVE_ALERT_HEADERS, start=1):
        set_cell(25, col_index, header)
    set_cell(
        26,
        1,
        '=IFERROR(SORTN(FILTER({FLIPKART_ACTIVE_TASKS!A2:A,FLIPKART_ACTIVE_TASKS!G2:G,FLIPKART_ACTIVE_TASKS!F2:F,'
        'FLIPKART_ACTIVE_TASKS!H2:H,FLIPKART_ACTIVE_TASKS!I2:I,FLIPKART_ACTIVE_TASKS!Q2:Q,'
        'FLIPKART_ACTIVE_TASKS!K2:K,FLIPKART_ACTIVE_TASKS!J2:J,FLIPKART_ACTIVE_TASKS!O2:O}, '
        'FLIPKART_ACTIVE_TASKS!C2:C=$B$2,FLIPKART_ACTIVE_TASKS!K2:K<>"Resolved",'
        'FLIPKART_ACTIVE_TASKS!K2:K<>"Ignored"),20,0,9,FALSE),"")'
    )

    set_cell(48, 1, SECTION_TITLES[6])
    for col_index, header in enumerate(RETURN_HEADERS, start=1):
        set_cell(49, col_index, header)
    set_cell(
        50,
        1,
        '=IFERROR(SORTN(FILTER($AE$2:$AL, $AE$2:$AE=$B$2), 20, 0, 8, FALSE), "")'
    )

    set_cell(72, 1, SECTION_TITLES[7])
    for col_index, header in enumerate(HISTORY_HEADERS, start=1):
        set_cell(73, col_index, header)
    set_cell(
        74,
        1,
        '=IFERROR(SORTN(FILTER({FLIPKART_FSN_HISTORY!A2:A,FLIPKART_FSN_HISTORY!B2:B,FLIPKART_FSN_HISTORY!C2:C,'
        'FLIPKART_FSN_HISTORY!I2:I,FLIPKART_FSN_HISTORY!L2:L,FLIPKART_FSN_HISTORY!M2:M,'
        'FLIPKART_FSN_HISTORY!N2:N,FLIPKART_FSN_HISTORY!P2:P,FLIPKART_FSN_HISTORY!Q2:Q,'
        'FLIPKART_FSN_HISTORY!R2:R}, FLIPKART_FSN_HISTORY!D2:D=$B$2), 12, 0, 1, FALSE), "")'
    )

    set_cell(96, 1, SECTION_TITLES[8])
    return_issue_rows = {
        97: ("Top_Issue_Category", return_summary_lookup("Top_Issue_Category")),
        98: ("Top_Return_Reason", return_summary_lookup("Top_Return_Reason")),
        99: ("Top_Return_Sub_Reason", return_summary_lookup("Top_Return_Sub_Reason")),
        100: ("Total_Returns_In_Detailed_Report", return_summary_lookup("Total_Returns_In_Detailed_Report")),
        101: ("Critical_Issue_Count", return_summary_lookup("Critical_Issue_Count")),
        102: ("High_Issue_Count", return_summary_lookup("High_Issue_Count")),
        103: ("Product_Issue_Count", return_summary_lookup("Product_Issue_Count")),
        104: ("Logistics_Issue_Count", return_summary_lookup("Logistics_Issue_Count")),
        105: ("Customer_RTO_Count", return_summary_lookup("Customer_RTO_Count")),
        106: ("Suggested_Return_Action", return_summary_lookup("Suggested_Return_Action")),
        107: ("Return_Action_Priority", return_summary_lookup("Return_Action_Priority")),
    }
    for row_index, (label, formula) in return_issue_rows.items():
        set_cell(row_index, 1, label)
        set_cell(row_index, 2, formula)

    set_cell(109, 1, "Recent Return Comments for Selected FSN")
    recent_return_comments_headers = [
        "Return_ID",
        "Return_Reason",
        "Return_Sub_Reason",
        "Comments",
        "Issue_Category",
        "Issue_Severity",
        "Suggested_Action",
    ]
    for col_index, header in enumerate(recent_return_comments_headers, start=1):
        set_cell(110, col_index, header)
    set_cell(111, 1, build_recent_return_comments_formula(return_comment_columns))

    return grid


def set_cell_range(
    grid: List[List[Any]],
    start_row: int,
    start_col: int,
    values: Sequence[Sequence[Any]],
) -> None:
    for row_offset, row in enumerate(values):
        for col_offset, value in enumerate(row):
            grid[start_row - 1 + row_offset][start_col - 1 + col_offset] = value


def update_drilldown_tab(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    visible_grid: Sequence[Sequence[Any]],
    unique_fsns_list: Sequence[str],
    return_rows: Sequence[Sequence[Any]],
) -> None:
    ensure_sheet_grid(sheets_service, spreadsheet_id, sheet_id)
    clear_tab(sheets_service, spreadsheet_id, DRILLDOWN_TAB)
    visible_end_col = column_index_to_a1(VISIBLE_HEADERS)
    visible_end_row = len(visible_grid)
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{DRILLDOWN_TAB}!A1:{visible_end_col}{visible_end_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [list(row) for row in visible_grid]},
        )
        .execute()
    )

    fsn_values = [["FSN_LIST"]] + [[fsn] for fsn in unique_fsns_list]
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{DRILLDOWN_TAB}!AC1:AC{len(fsn_values)}",
            valueInputOption="RAW",
            body={"values": fsn_values},
        )
        .execute()
    )

    return_headers = [["FSN", "Order_Item_ID", "Return_ID", "Return_Status", "Return_Reason", "Return_Sub_Reason", "Comments", "Return_Date"]]
    return_values = return_headers + [list(row) for row in return_rows]
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{DRILLDOWN_TAB}!AE1:AL{len(return_values)}",
            valueInputOption="RAW",
            body={"values": return_values},
        )
        .execute()
    )

    set_dropdown_validation(sheets_service, spreadsheet_id, sheet_id, len(fsn_values))
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, VISIBLE_HEADERS, section_rows=[3, 23, 47, 71, 95, 108])
    hide_helper_columns(sheets_service, spreadsheet_id, sheet_id)


def create_flipkart_fsn_drilldown() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [
        SKU_ANALYSIS_TAB,
        ALERTS_TAB,
        TRACKER_TAB,
        ACTIVE_TASKS_TAB,
        FSN_HISTORY_TAB,
        RETURN_COMMENTS_TAB,
        RETURN_ISSUE_SUMMARY_TAB,
        RETURN_REASON_PIVOT_TAB,
    ]:
        ensure_required_tab_exists(sheets_service, spreadsheet_id, tab_name)

    analysis_headers, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    return_summary_headers, return_summary_rows = read_table(sheets_service, spreadsheet_id, RETURN_ISSUE_SUMMARY_TAB)
    return_comment_headers, return_comment_rows = read_table(sheets_service, spreadsheet_id, RETURN_COMMENTS_TAB)
    if not analysis_rows:
        raise RuntimeError(f"No rows found in {SKU_ANALYSIS_TAB}")
    if not return_summary_rows:
        raise RuntimeError(f"No rows found in {RETURN_ISSUE_SUMMARY_TAB}")
    if not return_comment_rows:
        raise RuntimeError(f"No rows found in {RETURN_COMMENTS_TAB}")

    unique_fsns_list = unique_fsns(analysis_rows)
    if not unique_fsns_list:
        raise RuntimeError(f"No FSNs found in {SKU_ANALYSIS_TAB}")
    default_selected_fsn = unique_fsns_list[0]

    return_rows = build_return_helper_rows()
    visible_grid = build_visible_grid(analysis_headers, return_summary_headers, return_comment_headers)
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, DRILLDOWN_TAB)
    update_drilldown_tab(
        sheets_service,
        spreadsheet_id,
        sheet_id,
        visible_grid,
        unique_fsns_list,
        return_rows,
    )

    sections_created = list(SECTION_TITLES.values())
    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "fsn_count": len(unique_fsns_list),
        "default_selected_fsn": default_selected_fsn,
        "sections_created": json.dumps(sections_created, ensure_ascii=False),
        "tab_updated": "TRUE",
        "status": "SUCCESS",
        "message": "Flipkart FSN drilldown tab refreshed",
    }
    append_csv_log(LOG_PATH, LOG_HEADERS, [log_row])

    result = {
        "status": "SUCCESS",
        "fsn_count": len(unique_fsns_list),
        "default_selected_fsn": default_selected_fsn,
        "sections_created": sections_created,
        "tab_updated": True,
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        create_flipkart_fsn_drilldown()
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
