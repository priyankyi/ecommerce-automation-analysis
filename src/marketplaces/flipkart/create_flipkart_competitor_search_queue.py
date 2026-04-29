from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import clear_tab, ensure_tab, freeze_and_format, load_json, read_table, tab_exists, write_rows
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, clean_fsn, ensure_directories, format_decimal, normalize_text, now_iso, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_competitor_search_queue_log.csv"
LOCAL_OUTPUT_PATH = OUTPUT_DIR / "flipkart_competitor_search_queue.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
MODULE_CONFIDENCE_TAB = "FLIPKART_MODULE_CONFIDENCE"
COST_MASTER_TAB = "FLIPKART_COST_MASTER"
ADS_PLANNER_TAB = "FLIPKART_ADS_PLANNER"
ALERTS_TAB = "FLIPKART_ALERTS_GENERATED"
ACTIVE_TASKS_TAB = "FLIPKART_ACTIVE_TASKS"
QUEUE_TAB = "FLIPKART_COMPETITOR_SEARCH_QUEUE"

QUEUE_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Product_Type",
    "Final_Ads_Decision",
    "Final_Budget_Recommendation",
    "Our_Selling_Price",
    "Our_Pack_Count",
    "Our_Unit_Price",
    "Our_Final_Profit_Margin",
    "Our_Return_Rate",
    "Product_Image_URL",
    "Search_Method",
    "Search_Status",
    "Manual_Review_Status",
    "Priority",
    "Remarks",
    "Last_Updated",
]

AD_READY_DECISIONS = {"Scale Ads", "Test Ads"}
PRIORITY_ORDER = {"High": 0, "Medium": 1, "Low": 2}


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


def latest_non_empty_row(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    for row in reversed(list(rows)):
        if any(normalize_text(value) for value in row.values()):
            return dict(row)
    return {}


def build_index(rows: Sequence[Dict[str, str]], field_name: str = "FSN") -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    ordered: List[str] = []
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get(field_name, ""))
        if not fsn or fsn in indexed:
            continue
        ordered.append(fsn)
        indexed[fsn] = dict(row)
    return ordered, indexed


def infer_pack_count(text: str) -> str:
    text_norm = normalize_text(text).lower()
    if not text_norm:
        return ""
    patterns = [
        r"\bpack\s*(?:of)?\s*(\d+)\b",
        r"\bset\s*(?:of)?\s*(\d+)\b",
        r"\bcombo\s*(?:of)?\s*(\d+)\b",
        r"\bx\s*(\d+)\b",
        r"\b(\d+)\s*(?:pack|pcs|pieces|pc|unit|lights)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text_norm)
        if match:
            try:
                count = int(match.group(1))
                if count > 0:
                    return str(count)
            except ValueError:
                continue
    return ""


def safe_float_text(value: Any) -> float:
    try:
        return parse_float(value)
    except Exception:
        return 0.0


def derive_unit_price(listing_price: Any, pack_count: str) -> str:
    listing_value = safe_float_text(listing_price)
    pack_value = safe_float_text(pack_count)
    if listing_value <= 0:
        return ""
    if pack_value > 0:
        return format_decimal(listing_value / pack_value, 2)
    return format_decimal(listing_value, 2)


def derive_priority(decision: str, final_profit_margin: Any, return_rate: Any, alert_summary: Dict[str, Any], module_status: str) -> str:
    decision_norm = normalize_text(decision)
    priority = "Medium" if decision_norm == "Test Ads" else "High"
    if decision_norm == "Scale Ads":
        priority = "High"
    if normalize_text(alert_summary.get("highest_severity", "")) in {"Critical", "High"}:
        priority = "High"
    if safe_float_text(final_profit_margin) < 0.10 or safe_float_text(return_rate) >= 0.20:
        priority = "High"
    if normalize_text(module_status).upper() in {"LOW", "MISSING", "REVIEW"} and priority != "High":
        priority = "Medium"
    return priority


def build_remark(existing_remark: str, module_status: str, alert_summary: Dict[str, Any], cost_status: str, pack_count: str) -> str:
    if normalize_text(existing_remark):
        return normalize_text(existing_remark)
    pieces = []
    if normalize_text(module_status):
        pieces.append(f"Module confidence {normalize_text(module_status)}")
    if normalize_text(cost_status):
        pieces.append(f"COGS {normalize_text(cost_status)}")
    if alert_summary.get("critical_count", 0):
        pieces.append(f"Critical alerts {alert_summary['critical_count']}")
    if not pack_count:
        pieces.append("Pack count not inferred")
    return "; ".join(pieces)


def summarize_alerts(alert_rows: Sequence[Dict[str, str]], fsn: str, active_rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    highest = ""
    critical = 0
    high = 0
    medium = 0
    for row in list(alert_rows) + list(active_rows):
        if clean_fsn(row.get("FSN", "")) != fsn:
            continue
        severity = normalize_text(row.get("Severity", ""))
        if severity == "Critical":
            critical += 1
        elif severity == "High":
            high += 1
        elif severity == "Medium":
            medium += 1
        if severity and (highest == "" or PRIORITY_ORDER.get(severity, 99) < PRIORITY_ORDER.get(highest, 99)):
            highest = severity
    return {"critical_count": critical, "high_count": high, "medium_count": medium, "highest_severity": highest}


def build_queue_rows(
    ordered_fsns: Sequence[str],
    planner_rows: Sequence[Dict[str, str]],
    analysis_rows: Sequence[Dict[str, str]],
    module_rows: Sequence[Dict[str, str]],
    cost_rows: Sequence[Dict[str, str]],
    alert_rows: Sequence[Dict[str, str]],
    active_rows: Sequence[Dict[str, str]],
    existing_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, Any]]:
    planner_index = {clean_fsn(row.get("FSN", "")): dict(row) for row in planner_rows if clean_fsn(row.get("FSN", ""))}
    analysis_index = {clean_fsn(row.get("FSN", "")): dict(row) for row in analysis_rows if clean_fsn(row.get("FSN", ""))}
    module_index = {clean_fsn(row.get("FSN", "")): dict(row) for row in module_rows if clean_fsn(row.get("FSN", ""))}
    cost_index = {clean_fsn(row.get("FSN", "")): dict(row) for row in cost_rows if clean_fsn(row.get("FSN", ""))}
    existing_index = {clean_fsn(row.get("FSN", "")): dict(row) for row in existing_rows if clean_fsn(row.get("FSN", ""))}

    run_id = (
        latest_non_empty_row(module_rows).get("Run_ID")
        or latest_non_empty_row(planner_rows).get("Run_ID")
        or latest_non_empty_row(analysis_rows).get("Run_ID")
        or latest_non_empty_row(alert_rows).get("Run_ID")
        or f"FLIPKART_VISUAL_{now_iso().replace(':', '').replace('-', '').replace('T', '_')}"
    )

    queue_rows: List[Dict[str, Any]] = []
    for fsn in ordered_fsns:
        planner_row = planner_index.get(fsn, {})
        if normalize_text(planner_row.get("Final_Ads_Decision", "")) not in AD_READY_DECISIONS:
            continue
        analysis_row = analysis_index.get(fsn, {})
        module_row = module_index.get(fsn, {})
        cost_row = cost_index.get(fsn, {})
        alert_summary = summarize_alerts(alert_rows, fsn, active_rows)
        existing_row = existing_index.get(fsn, {})
        sku_id = normalize_text(analysis_row.get("SKU_ID", "")) or normalize_text(planner_row.get("SKU_ID", ""))
        product_title = normalize_text(analysis_row.get("Product_Title", "")) or normalize_text(planner_row.get("Product_Title", ""))
        product_type = normalize_text(planner_row.get("Final_Product_Type", "")) or normalize_text(analysis_row.get("Detected_Product_Type", "")) or "Unknown"
        selling_price = normalize_text(analysis_row.get("Selling_Price", ""))
        pack_count = infer_pack_count(product_title)
        unit_price = derive_unit_price(selling_price, pack_count)
        module_status = normalize_text(module_row.get("Overall_Confidence_Status", ""))
        priority = derive_priority(planner_row.get("Final_Ads_Decision", ""), analysis_row.get("Final_Profit_Margin", ""), analysis_row.get("Return_Rate", ""), alert_summary, module_status)
        queue_rows.append(
            {
                "Run_ID": run_id,
                "FSN": fsn,
                "SKU_ID": sku_id,
                "Product_Title": product_title,
                "Product_Type": product_type,
                "Final_Ads_Decision": normalize_text(planner_row.get("Final_Ads_Decision", "")),
                "Final_Budget_Recommendation": normalize_text(planner_row.get("Final_Budget_Recommendation", "")),
                "Our_Selling_Price": selling_price,
                "Our_Pack_Count": pack_count,
                "Our_Unit_Price": unit_price,
                "Our_Final_Profit_Margin": format_decimal(analysis_row.get("Final_Profit_Margin", ""), 4) if normalize_text(analysis_row.get("Final_Profit_Margin", "")) else "",
                "Our_Return_Rate": format_decimal(analysis_row.get("Return_Rate", ""), 4) if normalize_text(analysis_row.get("Return_Rate", "")) else "",
                "Product_Image_URL": normalize_text(existing_row.get("Product_Image_URL", "")),
                "Search_Method": normalize_text(existing_row.get("Search_Method", "")) or "Visual Search",
                "Search_Status": normalize_text(existing_row.get("Search_Status", "")) or "Pending",
                "Manual_Review_Status": normalize_text(existing_row.get("Manual_Review_Status", "")) or "Not Reviewed",
                "Priority": priority,
                "Remarks": build_remark(normalize_text(existing_row.get("Remarks", "")), module_status, alert_summary, normalize_text(cost_row.get("COGS_Status", "")), pack_count),
                "Last_Updated": now_iso(),
            }
        )
    queue_rows.sort(key=lambda row: (PRIORITY_ORDER.get(normalize_text(row.get("Priority", "")), 9), normalize_text(row.get("FSN", ""))))
    return queue_rows


def write_local_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def write_output_tab(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def create_flipkart_competitor_search_queue() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [SKU_ANALYSIS_TAB, MODULE_CONFIDENCE_TAB, COST_MASTER_TAB, ADS_PLANNER_TAB, ALERTS_TAB, ACTIVE_TASKS_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    _, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)
    _, module_rows = read_table(sheets_service, spreadsheet_id, MODULE_CONFIDENCE_TAB)
    _, cost_rows = read_table(sheets_service, spreadsheet_id, COST_MASTER_TAB)
    _, planner_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)
    _, alert_rows = read_table(sheets_service, spreadsheet_id, ALERTS_TAB)
    _, active_rows = read_table(sheets_service, spreadsheet_id, ACTIVE_TASKS_TAB)
    existing_headers, existing_rows = read_table(sheets_service, spreadsheet_id, QUEUE_TAB) if tab_exists(sheets_service, spreadsheet_id, QUEUE_TAB) else ([], [])
    existing_index = {clean_fsn(row.get("FSN", "")): dict(row) for row in existing_rows if clean_fsn(row.get("FSN", ""))}

    ordered_fsns, _ = build_index(planner_rows)
    queue_rows = build_queue_rows(ordered_fsns, planner_rows, analysis_rows, module_rows, cost_rows, alert_rows, active_rows, existing_rows)

    write_local_csv(LOCAL_OUTPUT_PATH, QUEUE_HEADERS, queue_rows)
    write_output_tab(sheets_service, spreadsheet_id, QUEUE_TAB, QUEUE_HEADERS, queue_rows)

    ad_ready_count = sum(1 for row in planner_rows if clean_fsn(row.get("FSN", "")) and normalize_text(row.get("Final_Ads_Decision", "")) in AD_READY_DECISIONS)
    scale_ads_count = sum(1 for row in queue_rows if normalize_text(row.get("Final_Ads_Decision", "")) == "Scale Ads")
    test_ads_count = sum(1 for row in queue_rows if normalize_text(row.get("Final_Ads_Decision", "")) == "Test Ads")
    preserved_manual_rows = sum(
        1
        for row in queue_rows
        if any(
            normalize_text(existing_index.get(clean_fsn(row.get("FSN", "")), {}).get(field, ""))
            for field in ("Product_Image_URL", "Search_Method", "Search_Status", "Manual_Review_Status", "Remarks")
        )
    )
    blank_image_url_rows = sum(1 for row in queue_rows if not normalize_text(row.get("Product_Image_URL", "")))

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": "SUCCESS",
        "run_id": queue_rows[0]["Run_ID"] if queue_rows else "",
        "queue_rows": len(queue_rows),
        "ad_ready_rows": ad_ready_count,
        "scale_ads_rows": scale_ads_count,
        "test_ads_rows": test_ads_count,
        "blank_image_url_rows": blank_image_url_rows,
        "tabs_updated": json.dumps([QUEUE_TAB], ensure_ascii=False),
        "message": "Built Flipkart competitor search queue for ad-ready FSNs",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "status",
            "run_id",
            "queue_rows",
            "ad_ready_rows",
            "scale_ads_rows",
            "test_ads_rows",
            "blank_image_url_rows",
            "tabs_updated",
            "message",
        ],
        [log_row],
    )

    payload = {
        "status": "SUCCESS",
        "run_id": log_row["run_id"],
        "queue_rows": len(queue_rows),
        "ad_ready_rows": ad_ready_count,
        "scale_ads_rows": scale_ads_count,
        "test_ads_rows": test_ads_count,
        "blank_image_url_rows": blank_image_url_rows,
        "tabs_updated": [QUEUE_TAB],
        "local_output": str(LOCAL_OUTPUT_PATH),
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return payload


def main() -> None:
    try:
        create_flipkart_competitor_search_queue()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                    "tabs_updated": [QUEUE_TAB],
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
