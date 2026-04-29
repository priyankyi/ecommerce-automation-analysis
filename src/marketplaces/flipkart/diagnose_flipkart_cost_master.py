from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    LOG_DIR,
    append_csv_log,
    clean_fsn,
    ensure_directories,
    normalize_key,
    normalize_text,
    parse_float,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "output" / "marketplaces" / "flipkart" / "flipkart_cost_master_diagnostic.csv"
LOG_PATH = LOG_DIR / "flipkart_cost_master_diagnostic_log.csv"

COST_MASTER_TAB = "FLIPKART_COST_MASTER"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
MASTER_SKU_TAB = "MASTER_SKU"

DIAGNOSTIC_HEADERS = [
    "Row_Index",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Raw_Cost_Price",
    "Parsed_Cost_Price",
    "Cost_Price_Class",
    "Raw_Packaging_Cost",
    "Raw_Other_Cost",
    "Raw_Total_Unit_COGS",
    "Raw_COGS_Status",
    "Notes",
]

ALIASES: Dict[str, Tuple[str, ...]] = {
    "cost_price": ("Cost_Price", "Cost Price", "cost price", "Cost", "COGS", "Product_Cost", "Product Cost"),
    "packaging_cost": ("Packaging_Cost", "Packaging Cost", "packaging cost", "Packaging"),
    "other_cost": ("Other_Cost", "Other Cost", "other cost"),
    "total_unit_cogs": ("Total_Unit_COGS", "Total Unit COGS", "Total COGS", "Unit COGS", "COGS"),
    "cogs_status": ("COGS_Status", "COGS Status", "cogs status", "Status", "COGS"),
}


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


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    return any(normalize_text(sheet.get("properties", {}).get("title", "")) == tab_name for sheet in metadata.get("sheets", []))


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


def write_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in headers})


def normalize_header(header: Any) -> str:
    return normalize_key(normalize_text(header))


def find_header(headers: Sequence[str], aliases: Sequence[str]) -> Optional[str]:
    best_header = ""
    best_score = 0.0
    for header in headers:
        header_norm = normalize_header(header)
        for alias in aliases:
            alias_norm = normalize_header(alias)
            if not header_norm or not alias_norm:
                continue
            score = 0.0
            if header_norm == alias_norm:
                score = 1.0
            elif alias_norm in header_norm or header_norm in alias_norm:
                score = 0.94
            else:
                from difflib import SequenceMatcher

                score = SequenceMatcher(None, header_norm, alias_norm).ratio()
            if score > best_score:
                best_score = score
                best_header = normalize_text(header)
    return best_header if best_score >= 0.82 else None


def detect_headers(headers: Sequence[str]) -> Dict[str, Optional[str]]:
    return {canonical: find_header(headers, aliases) for canonical, aliases in ALIASES.items()}


def classify_cost_value(raw_value: Any) -> Tuple[str, str]:
    text = normalize_text(raw_value)
    if not text:
        return "blank", ""
    if text.startswith("="):
        return "formula", text
    parsed = parse_float(text)
    digits_present = bool(re.search(r"\d", text))
    if digits_present:
        return "numeric", f"{parsed:.2f}"
    return "text", text


def count_master_rows(rows: Sequence[Dict[str, str]]) -> Tuple[int, int]:
    real_rows = 0
    test_rows = 0
    for row in rows:
        sku_id = normalize_text(row.get("SKU_ID", ""))
        if sku_id.upper().startswith("LED-TEST-"):
            test_rows += 1
        elif sku_id:
            real_rows += 1
    return real_rows, test_rows


def sample_rows(rows: Sequence[Dict[str, Any]], headers: Dict[str, Optional[str]], limit: int = 10) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    cost_key = headers.get("cost_price")
    packaging_key = headers.get("packaging_cost")
    other_key = headers.get("other_cost")
    total_key = headers.get("total_unit_cogs")
    status_key = headers.get("cogs_status")

    for index, row in enumerate(rows, start=2):
        raw_cost = row.get(cost_key, "") if cost_key else ""
        raw_cost_text = normalize_text(raw_cost)
        if not raw_cost_text:
            continue
        cost_class, parsed_cost = classify_cost_value(raw_cost)
        notes = []
        if cost_class == "formula":
            notes.append("formula")
        elif cost_class == "text":
            notes.append("text")
        if raw_cost_text and re.search(r"[₹,\s]", raw_cost_text):
            notes.append("formatted")
        output.append(
            {
                "Row_Index": index,
                "FSN": clean_fsn(row.get("FSN", "")),
                "SKU_ID": normalize_text(row.get("SKU_ID", "")),
                "Product_Title": normalize_text(row.get("Product_Title", "")),
                "Raw_Cost_Price": raw_cost_text,
                "Parsed_Cost_Price": parsed_cost,
                "Cost_Price_Class": cost_class,
                "Raw_Packaging_Cost": normalize_text(row.get(packaging_key, "")) if packaging_key else "",
                "Raw_Other_Cost": normalize_text(row.get(other_key, "")) if other_key else "",
                "Raw_Total_Unit_COGS": normalize_text(row.get(total_key, "")) if total_key else "",
                "Raw_COGS_Status": normalize_text(row.get(status_key, "")) if status_key else "",
                "Notes": "; ".join(notes),
            }
        )
        if len(output) >= limit:
            break
    return output


def sample_non_numeric_rows(rows: Sequence[Dict[str, str]], headers: Dict[str, Optional[str]], limit: int = 10) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    cost_key = headers.get("cost_price")
    packaging_key = headers.get("packaging_cost")
    other_key = headers.get("other_cost")
    total_key = headers.get("total_unit_cogs")
    status_key = headers.get("cogs_status")

    for index, row in enumerate(rows, start=2):
        raw_cost = row.get(cost_key, "") if cost_key else ""
        raw_cost_text = normalize_text(raw_cost)
        if not raw_cost_text:
            continue
        cost_class, parsed_cost = classify_cost_value(raw_cost)
        if cost_class == "numeric":
            continue
        output.append(
            {
                "Row_Index": index,
                "FSN": clean_fsn(row.get("FSN", "")),
                "SKU_ID": normalize_text(row.get("SKU_ID", "")),
                "Product_Title": normalize_text(row.get("Product_Title", "")),
                "Raw_Cost_Price": raw_cost_text,
                "Parsed_Cost_Price": parsed_cost,
                "Cost_Price_Class": cost_class,
                "Raw_Packaging_Cost": normalize_text(row.get(packaging_key, "")) if packaging_key else "",
                "Raw_Other_Cost": normalize_text(row.get(other_key, "")) if other_key else "",
                "Raw_Total_Unit_COGS": normalize_text(row.get(total_key, "")) if total_key else "",
                "Raw_COGS_Status": normalize_text(row.get(status_key, "")) if status_key else "",
                "Notes": "non-numeric",
            }
        )
        if len(output) >= limit:
            break
    return output


def build_row_diagnostics(rows: Sequence[Dict[str, str]], headers: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    cost_key = headers.get("cost_price")
    packaging_key = headers.get("packaging_cost")
    other_key = headers.get("other_cost")
    total_key = headers.get("total_unit_cogs")
    status_key = headers.get("cogs_status")

    for index, row in enumerate(rows, start=2):
        raw_cost = row.get(cost_key, "") if cost_key else ""
        raw_cost_text = normalize_text(raw_cost)
        cost_class, parsed_cost = classify_cost_value(raw_cost)
        output.append(
            {
                "Row_Index": index,
                "FSN": clean_fsn(row.get("FSN", "")),
                "SKU_ID": normalize_text(row.get("SKU_ID", "")),
                "Product_Title": normalize_text(row.get("Product_Title", "")),
                "Raw_Cost_Price": raw_cost_text,
                "Parsed_Cost_Price": parsed_cost,
                "Cost_Price_Class": cost_class,
                "Raw_Packaging_Cost": normalize_text(row.get(packaging_key, "")) if packaging_key else "",
                "Raw_Other_Cost": normalize_text(row.get(other_key, "")) if other_key else "",
                "Raw_Total_Unit_COGS": normalize_text(row.get(total_key, "")) if total_key else "",
                "Raw_COGS_Status": normalize_text(row.get(status_key, "")) if status_key else "",
                "Notes": "blank" if not raw_cost_text else cost_class,
            }
        )
    return output


def summarize_cost_master(rows: Sequence[Dict[str, str]], headers: Dict[str, Optional[str]]) -> Dict[str, Any]:
    cost_key = headers.get("cost_price")
    packaging_key = headers.get("packaging_cost")
    other_key = headers.get("other_cost")
    total_key = headers.get("total_unit_cogs")
    status_key = headers.get("cogs_status")

    non_blank_rows = 0
    numeric_rows = 0
    non_numeric_rows = 0
    status_blank_numeric_rows = 0
    manual_cogs_rows = 0

    for row in rows:
        raw_cost = row.get(cost_key, "") if cost_key else ""
        raw_cost_text = normalize_text(raw_cost)
        if not raw_cost_text:
            continue
        non_blank_rows += 1
        cost_class, _ = classify_cost_value(raw_cost)
        if cost_class == "numeric":
            numeric_rows += 1
        else:
            non_numeric_rows += 1
        raw_status = normalize_text(row.get(status_key, "")) if status_key else ""
        total_value = normalize_text(row.get(total_key, "")) if total_key else ""
        packaging_value = normalize_text(row.get(packaging_key, "")) if packaging_key else ""
        other_value = normalize_text(row.get(other_key, "")) if other_key else ""
        if cost_class == "numeric" and not raw_status:
            status_blank_numeric_rows += 1
        if any([raw_cost_text, packaging_value, other_value, total_value]):
            manual_cogs_rows += 1

    return {
        "non_blank_cost_price_rows": non_blank_rows,
        "numeric_cost_price_rows": numeric_rows,
        "non_numeric_cost_price_rows": non_numeric_rows,
        "status_blank_numeric_cost_rows": status_blank_numeric_rows,
        "manual_cogs_rows": manual_cogs_rows,
    }


def build_issue(summary: Dict[str, Any]) -> Tuple[str, str]:
    if not summary["cost_master_exists"]:
        return "FLIPKART_COST_MASTER is missing or unreadable.", "Restore the tab before running the Stage 6 COGS update."

    if not summary["cost_price_column_found"]:
        if summary["cost_price_header_candidates"]:
            return (
                "Cost values appear to live in a similar-looking column name instead of Cost_Price.",
                "Rename or remap the detected source column to Cost_Price, then rerun the COGS refresh.",
            )
        return (
            "Cost_Price could not be found in FLIPKART_COST_MASTER.",
            "Add or restore a Cost_Price column in FLIPKART_COST_MASTER before rerunning the profit refresh.",
        )

    if summary["numeric_cost_price_rows"] == 0 and summary["non_blank_cost_price_rows"] > 0:
        return (
            "Cost_Price contains populated cells, but they are being read as text/formulas instead of clean numeric values.",
            "Normalize the values to numeric text such as 120, 120.00, or ₹120 and rerun the COGS refresh.",
        )

    if summary["numeric_cost_price_rows"] > 0 and summary["cost_master_cogs_entered_like_rows"] == 0:
        return (
            "Numeric Cost_Price values exist, but COGS_Status is not being recognized as Entered in downstream sheets.",
            "Update the profit refresh to auto-set COGS_Status=Entered whenever Cost_Price is populated, then refresh FLIPKART_SKU_ANALYSIS.",
        )

    if summary["numeric_cost_price_rows"] > 0 and summary["sku_analysis_cogs_entered_count"] == 0:
        return (
            "FLIPKART_SKU_ANALYSIS is not reflecting the populated COGS values from FLIPKART_COST_MASTER.",
            "Rerun the profit refresh so analysis rows inherit the live COGS fields from FLIPKART_COST_MASTER.",
        )

    return (
        "No obvious tab or parsing issue was found; the remaining problem is likely stale analysis output.",
        "Rerun the profit refresh and then the Stage 6 dashboard/alerts builders without changing MASTER_SKU.",
    )


def write_diagnostic_csv(rows: Sequence[Dict[str, Any]]) -> None:
    write_csv(OUTPUT_PATH, DIAGNOSTIC_HEADERS, rows)


def main() -> None:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    summary: Dict[str, Any] = {
        "spreadsheet_id": spreadsheet_id,
        "cost_master_exists": tab_exists(sheets_service, spreadsheet_id, COST_MASTER_TAB),
        "analysis_exists": tab_exists(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB),
        "master_sku_exists": tab_exists(sheets_service, spreadsheet_id, MASTER_SKU_TAB),
        "cost_master_rows": 0,
        "cost_master_headers": [],
        "cost_price_column_found": False,
        "packaging_cost_column_found": False,
        "other_cost_column_found": False,
        "total_unit_cogs_column_found": False,
        "cogs_status_column_found": False,
        "non_blank_cost_price_rows": 0,
        "numeric_cost_price_rows": 0,
        "non_numeric_cost_price_rows": 0,
        "sample_non_blank_cost_rows": [],
        "sample_non_numeric_cost_rows": [],
        "sku_analysis_cogs_columns_found": {},
        "master_sku_real_rows_count": 0,
        "master_sku_test_rows_count": 0,
        "diagnostic_rows_written": 0,
        "suspected_issue": "",
        "recommended_fix": "",
    }

    cost_rows: List[Dict[str, str]] = []
    cost_header_map: Dict[str, Optional[str]] = {key: None for key in ALIASES}
    if summary["cost_master_exists"]:
        cost_headers, cost_rows = read_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
        summary["cost_master_rows"] = len(cost_rows)
        summary["cost_master_headers"] = cost_headers
        cost_header_map = detect_headers(cost_headers)
        summary["cost_price_column_found"] = bool(cost_header_map["cost_price"])
        summary["packaging_cost_column_found"] = bool(cost_header_map["packaging_cost"])
        summary["other_cost_column_found"] = bool(cost_header_map["other_cost"])
        summary["total_unit_cogs_column_found"] = bool(cost_header_map["total_unit_cogs"])
        summary["cogs_status_column_found"] = bool(cost_header_map["cogs_status"])
        master_stats = summarize_cost_master(cost_rows, cost_header_map)
        summary["non_blank_cost_price_rows"] = master_stats["non_blank_cost_price_rows"]
        summary["numeric_cost_price_rows"] = master_stats["numeric_cost_price_rows"]
        summary["non_numeric_cost_price_rows"] = master_stats["non_numeric_cost_price_rows"]
        summary["cost_master_cogs_entered_like_rows"] = master_stats["status_blank_numeric_cost_rows"]
        summary["sample_non_blank_cost_rows"] = sample_rows(cost_rows, cost_header_map, limit=10)
        summary["sample_non_numeric_cost_rows"] = sample_non_numeric_rows(cost_rows, cost_header_map, limit=10)
    else:
        summary["cost_master_cogs_entered_like_rows"] = 0

    if summary["analysis_exists"]:
        analysis_headers, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
        analysis_header_map = detect_headers(analysis_headers)
        summary["sku_analysis_cogs_columns_found"] = {
            canonical: header for canonical, header in analysis_header_map.items() if header
        }
    else:
        summary["sku_analysis_cogs_columns_found"] = {}
        analysis_rows = []

    if summary["master_sku_exists"]:
        _, master_rows = read_table(sheets_service, spreadsheet_id, MASTER_SKU_TAB)
        real_rows = []
        test_rows = []
        for row in master_rows:
            sku_id = normalize_text(row.get("SKU_ID", ""))
            if sku_id.upper().startswith("LED-TEST-"):
                test_rows.append(row)
            elif sku_id:
                real_rows.append(row)
        summary["master_sku_real_rows_count"] = len(real_rows)
        summary["master_sku_test_rows_count"] = len(test_rows)

    suspected_issue, recommended_fix = build_issue(summary)
    summary["suspected_issue"] = suspected_issue
    summary["recommended_fix"] = recommended_fix

    csv_rows: List[Dict[str, Any]] = build_row_diagnostics(cost_rows, cost_header_map) if cost_rows else []
    summary["diagnostic_rows_written"] = len(csv_rows)
    write_diagnostic_csv(csv_rows)

    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "cost_master_exists",
            "cost_master_rows",
            "non_blank_cost_price_rows",
            "numeric_cost_price_rows",
            "non_numeric_cost_price_rows",
            "master_sku_real_rows_count",
            "master_sku_test_rows_count",
            "suspected_issue",
        ],
        [
            {
                "timestamp": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
                "spreadsheet_id": spreadsheet_id,
                "cost_master_exists": summary["cost_master_exists"],
                "cost_master_rows": summary["cost_master_rows"],
                "non_blank_cost_price_rows": summary["non_blank_cost_price_rows"],
                "numeric_cost_price_rows": summary["numeric_cost_price_rows"],
                "non_numeric_cost_price_rows": summary["non_numeric_cost_price_rows"],
                "master_sku_real_rows_count": summary["master_sku_real_rows_count"],
                "master_sku_test_rows_count": summary["master_sku_test_rows_count"],
                "suspected_issue": summary["suspected_issue"],
            }
        ],
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)
