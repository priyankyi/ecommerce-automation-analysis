from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    OUTPUT_DIR,
    append_csv_log,
    clean_fsn,
    normalize_text,
    now_iso,
    parse_float,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
RUNS_DIR = OUTPUT_DIR / "runs"
RUN_HISTORY_TAB = "FLIPKART_RUN_HISTORY"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"
LOG_PATH = LOG_DIR / "flipkart_run_history_log.csv"

RUN_HISTORY_HEADERS = [
    "Run_ID",
    "Run_Date",
    "Report_Start_Date",
    "Report_End_Date",
    "Target_FSN_Count",
    "Rows_Written",
    "High_Confidence_Count",
    "Medium_Confidence_Count",
    "Low_Confidence_Count",
    "FSNs_With_Listing",
    "FSNs_With_Orders",
    "FSNs_With_Returns",
    "FSNs_With_Settlement",
    "FSNs_With_PNL",
    "FSNs_With_Ads",
    "High_Return_Rate_Count",
    "Missing_Settlement_Count",
    "Missing_PNL_Count",
    "Audit_Passed",
    "Google_Sheet_Pushed",
    "Output_CSV_Path",
    "Archive_Folder",
    "Last_Updated",
]

FSN_HISTORY_HEADERS = [
    "Run_ID",
    "Report_Start_Date",
    "Report_End_Date",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Category",
    "Listing_Status",
    "Orders",
    "Units_Sold",
    "Gross_Sales",
    "Returns",
    "Return_Rate",
    "Customer_Return_Count",
    "Courier_Return_Count",
    "Unknown_Return_Count",
    "Total_Return_Count",
    "Customer_Return_Rate",
    "Courier_Return_Rate",
    "Total_Return_Rate",
    "Net_Settlement",
    "Flipkart_Net_Earnings",
    "Net_Profit_Before_COGS",
    "Data_Confidence",
    "Final_Action",
    "Reason",
    "Missing_Data",
    "Last_Updated",
]

LOG_HEADERS = [
    "timestamp",
    "spreadsheet_id",
    "run_id",
    "run_history_rows_added",
    "fsn_history_rows_added",
    "status",
    "message",
]


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception:
            if attempt == attempts:
                raise
            import time

            time.sleep(delay)
            delay *= 2


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


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


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
        .get("values", [])
    )


def column_index_to_a1(index: int) -> str:
    result = []
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


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
    retry(lambda: sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute())


def ensure_headers(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str]) -> bool:
    current = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:{column_index_to_a1(len(headers) - 1)}1")
    if current and any(normalize_text(cell) for cell in current[0]):
        return False
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:{column_index_to_a1(len(headers) - 1)}1",
            valueInputOption="RAW",
            body={"values": [list(headers)]},
        )
        .execute()
    )
    return True


def read_table(sheets_service, spreadsheet_id: str, tab_name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    if not rows:
        return [], []
    headers = [str(cell) for cell in rows[0]]
    data: List[Dict[str, str]] = []
    for row in rows[1:]:
        data.append({headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))})
    return headers, data


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def get_latest_run_dir() -> Path:
    if not RUNS_DIR.exists():
        raise FileNotFoundError(f"Missing Flipkart runs folder: {RUNS_DIR}")
    run_dirs = [path for path in RUNS_DIR.iterdir() if path.is_dir() and path.name.startswith("FLIPKART_")]
    if not run_dirs:
        raise FileNotFoundError(f"No Flipkart runs found in: {RUNS_DIR}")
    return sorted(run_dirs, key=lambda path: path.name)[-1]


def parse_run_date(run_id: str) -> str:
    try:
        return datetime.strptime(run_id, "FLIPKART_%Y%m%d_%H%M%S").date().isoformat()
    except ValueError:
        return datetime.now().date().isoformat()


def bool_text(value: Any) -> str:
    return "TRUE" if bool(value) else "FALSE"


def count_present_fsns(rows: Sequence[Dict[str, str]], marker: str) -> int:
    return sum(1 for row in rows if marker not in normalize_text(row.get("Missing_Data", "")))


def count_high_return_rate(rows: Sequence[Dict[str, str]]) -> int:
    return sum(1 for row in rows if parse_float(row.get("Customer_Return_Rate", row.get("Return_Rate", ""))) > 0.20)


def build_run_history_row(summary: Dict[str, Any], analysis_rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    return {
        "Run_ID": summary.get("run_id", ""),
        "Run_Date": parse_run_date(str(summary.get("run_id", ""))),
        "Report_Start_Date": summary.get("report_start_date", ""),
        "Report_End_Date": summary.get("report_end_date", ""),
        "Target_FSN_Count": summary.get("target_fsn_count", 0),
        "Rows_Written": summary.get("rows_written", 0),
        "High_Confidence_Count": summary.get("high_confidence_count", 0),
        "Medium_Confidence_Count": summary.get("medium_confidence_count", 0),
        "Low_Confidence_Count": summary.get("low_confidence_count", 0),
        "FSNs_With_Listing": count_present_fsns(analysis_rows, "Listing Missing"),
        "FSNs_With_Orders": count_present_fsns(analysis_rows, "Orders Missing"),
        "FSNs_With_Returns": count_present_fsns(analysis_rows, "Returns Missing"),
        "FSNs_With_Settlement": count_present_fsns(analysis_rows, "Settlement Missing"),
        "FSNs_With_PNL": count_present_fsns(analysis_rows, "PNL Missing"),
        "FSNs_With_Ads": count_present_fsns(analysis_rows, "Ads Missing"),
        "High_Return_Rate_Count": count_high_return_rate(analysis_rows),
        "Missing_Settlement_Count": sum(1 for row in analysis_rows if "Settlement Missing" in normalize_text(row.get("Missing_Data", ""))),
        "Missing_PNL_Count": sum(1 for row in analysis_rows if "PNL Missing" in normalize_text(row.get("Missing_Data", ""))),
        "Audit_Passed": bool_text(summary.get("audit_passed", False)),
        "Google_Sheet_Pushed": bool_text(summary.get("pushed_to_google_sheet", False)),
        "Output_CSV_Path": summary.get("output_csv_path", ""),
        "Archive_Folder": summary.get("archive_folder_path", ""),
        "Last_Updated": now_iso(),
    }


def build_fsn_history_rows(summary: Dict[str, Any], analysis_rows: Sequence[Dict[str, str]]) -> List[Dict[str, Any]]:
    run_id = str(summary.get("run_id", ""))
    report_start_date = str(summary.get("report_start_date", ""))
    report_end_date = str(summary.get("report_end_date", ""))
    rows: List[Dict[str, Any]] = []
    for row in analysis_rows:
        rows.append(
            {
                "Run_ID": run_id,
                "Report_Start_Date": report_start_date,
                "Report_End_Date": report_end_date,
                "FSN": clean_fsn(row.get("FSN", "")),
                "SKU_ID": row.get("SKU_ID", ""),
                "Product_Title": row.get("Product_Title", ""),
                "Category": row.get("Category", ""),
                "Listing_Status": row.get("Listing_Status", ""),
                "Orders": row.get("Orders", ""),
                "Units_Sold": row.get("Units_Sold", ""),
                "Gross_Sales": row.get("Gross_Sales", ""),
                "Returns": row.get("Returns", ""),
                "Return_Rate": row.get("Return_Rate", ""),
                "Customer_Return_Count": row.get("Customer_Return_Count", ""),
                "Courier_Return_Count": row.get("Courier_Return_Count", ""),
                "Unknown_Return_Count": row.get("Unknown_Return_Count", ""),
                "Total_Return_Count": row.get("Total_Return_Count", ""),
                "Customer_Return_Rate": row.get("Customer_Return_Rate", ""),
                "Courier_Return_Rate": row.get("Courier_Return_Rate", ""),
                "Total_Return_Rate": row.get("Total_Return_Rate", ""),
                "Net_Settlement": row.get("Net_Settlement", ""),
                "Flipkart_Net_Earnings": row.get("Flipkart_Net_Earnings", ""),
                "Net_Profit_Before_COGS": row.get("Net_Profit_Before_COGS", ""),
                "Data_Confidence": row.get("Data_Confidence", ""),
                "Final_Action": row.get("Final_Action", ""),
                "Reason": row.get("Reason", ""),
                "Missing_Data": row.get("Missing_Data", ""),
                "Last_Updated": now_iso(),
            }
        )
    return rows


def append_rows(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    headers: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> int:
    if not rows:
        return 0
    values = [[row.get(header, "") for header in headers] for row in rows]
    retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A:{column_index_to_a1(len(headers) - 1)}",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        )
        .execute()
    )
    return len(rows)


def update_flipkart_run_history() -> Dict[str, Any]:
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    latest_run_dir = get_latest_run_dir()
    summary_path = latest_run_dir / "pipeline_run_summary.json"
    analysis_path = OUTPUT_DIR / "flipkart_sku_analysis.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing required file: {summary_path}")
    if not analysis_path.exists():
        raise FileNotFoundError(f"Missing required file: {analysis_path}")

    summary = load_json(summary_path)
    analysis_rows = read_csv_rows(analysis_path)
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    run_sheet_id = ensure_tab(sheets_service, spreadsheet_id, RUN_HISTORY_TAB)
    fsn_sheet_id = ensure_tab(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
    ensure_headers(sheets_service, spreadsheet_id, RUN_HISTORY_TAB, RUN_HISTORY_HEADERS)
    ensure_headers(sheets_service, spreadsheet_id, FSN_HISTORY_TAB, FSN_HISTORY_HEADERS)

    run_headers, run_existing_rows = read_table(sheets_service, spreadsheet_id, RUN_HISTORY_TAB)
    fsn_headers, fsn_existing_rows = read_table(sheets_service, spreadsheet_id, FSN_HISTORY_TAB)
    run_headers = run_headers or list(RUN_HISTORY_HEADERS)
    fsn_headers = fsn_headers or list(FSN_HISTORY_HEADERS)

    run_key_set = {normalize_text(row.get("Run_ID", "")) for row in run_existing_rows if normalize_text(row.get("Run_ID", ""))}
    fsn_key_set = {
        (normalize_text(row.get("Run_ID", "")), normalize_text(row.get("FSN", "")))
        for row in fsn_existing_rows
        if normalize_text(row.get("Run_ID", "")) and normalize_text(row.get("FSN", ""))
    }

    run_row = build_run_history_row(summary, analysis_rows)
    run_rows_to_add = [] if normalize_text(run_row["Run_ID"]) in run_key_set else [run_row]
    fsn_rows_to_add = []
    for row in build_fsn_history_rows(summary, analysis_rows):
        key = (normalize_text(row.get("Run_ID", "")), normalize_text(row.get("FSN", "")))
        if key in fsn_key_set:
            continue
        if not all(key):
            continue
        fsn_rows_to_add.append(row)
        fsn_key_set.add(key)

    run_rows_added = append_rows(sheets_service, spreadsheet_id, RUN_HISTORY_TAB, RUN_HISTORY_HEADERS, run_rows_to_add)
    fsn_rows_added = append_rows(sheets_service, spreadsheet_id, FSN_HISTORY_TAB, FSN_HISTORY_HEADERS, fsn_rows_to_add)

    freeze_and_format(sheets_service, spreadsheet_id, run_sheet_id, len(RUN_HISTORY_HEADERS))
    freeze_and_format(sheets_service, spreadsheet_id, fsn_sheet_id, len(FSN_HISTORY_HEADERS))

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "run_id": summary.get("run_id", ""),
        "run_history_rows_added": run_rows_added,
        "fsn_history_rows_added": fsn_rows_added,
        "status": "SUCCESS",
        "message": f"Updated {RUN_HISTORY_TAB} and {FSN_HISTORY_TAB}",
    }
    append_csv_log(LOG_PATH, LOG_HEADERS, [log_row])

    result = {
        "status": "SUCCESS",
        "spreadsheet_id": spreadsheet_id,
        "run_id": summary.get("run_id", ""),
        "run_history_updated": True,
        "run_history_rows_added": run_rows_added,
        "fsn_history_rows_added": fsn_rows_added,
        "run_history_tab": RUN_HISTORY_TAB,
        "fsn_history_tab": FSN_HISTORY_TAB,
        "log_path": str(LOG_PATH),
        "latest_run_dir": str(latest_run_dir),
        "summary_path": str(summary_path),
        "analysis_path": str(analysis_path),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        update_flipkart_run_history()
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
