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
from src.marketplaces.flipkart.flipkart_sheet_helpers import clear_tab, ensure_tab, freeze_and_format, load_json, read_table, tab_exists, write_rows
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, clean_fsn, ensure_directories, normalize_text, now_iso

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_competitor_image_url_populate_log.csv"
LOCAL_OUTPUT_PATH = OUTPUT_DIR / "flipkart_competitor_search_queue.csv"

QUEUE_TAB = "FLIPKART_COMPETITOR_SEARCH_QUEUE"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
LISTING_PRESENCE_TAB = "FLIPKART_LISTING_PRESENCE"

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


def write_local_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def index_rows(rows: Sequence[Dict[str, str]], key_field: str = "FSN") -> Dict[str, Dict[str, str]]:
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get(key_field, ""))
        if fsn and fsn not in indexed:
            indexed[fsn] = dict(row)
    return indexed


def first_nonblank(*values: Any) -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return ""


def build_image_url(row: Dict[str, str], analysis_row: Dict[str, str], listing_row: Dict[str, str]) -> str:
    candidates = [
        row.get("Product_Image_URL", ""),
        analysis_row.get("Product_Image_URL", ""),
        analysis_row.get("Image_URL", ""),
        analysis_row.get("Thumbnail_URL", ""),
        listing_row.get("Product_Image_URL", ""),
        listing_row.get("Image_URL", ""),
        listing_row.get("Thumbnail_URL", ""),
    ]
    return first_nonblank(*candidates)


def populate_competitor_product_image_urls() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    if not tab_exists(sheets_service, spreadsheet_id, QUEUE_TAB):
        raise FileNotFoundError(f"Missing required Google Sheet tab: {QUEUE_TAB}")

    _, queue_rows = read_table(sheets_service, spreadsheet_id, QUEUE_TAB)
    _, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB) if tab_exists(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB) else ([], [])
    _, listing_rows = read_table(sheets_service, spreadsheet_id, LISTING_PRESENCE_TAB) if tab_exists(sheets_service, spreadsheet_id, LISTING_PRESENCE_TAB) else ([], [])

    analysis_index = index_rows(analysis_rows)
    listing_index = index_rows(listing_rows)
    updated_rows: List[Dict[str, Any]] = []
    filled_count = 0
    blank_before_count = 0

    for row in queue_rows:
        fsn = clean_fsn(row.get("FSN", ""))
        analysis_row = analysis_index.get(fsn, {})
        listing_row = listing_index.get(fsn, {})
        current_image_url = normalize_text(row.get("Product_Image_URL", ""))
        if not current_image_url:
            blank_before_count += 1
        image_url = build_image_url(row, analysis_row, listing_row)
        updated = dict(row)
        if image_url and not current_image_url:
            updated["Product_Image_URL"] = image_url
            filled_count += 1
        updated_rows.append(updated)

    blank_after_count = sum(1 for row in updated_rows if not normalize_text(row.get("Product_Image_URL", "")))
    write_local_csv(LOCAL_OUTPUT_PATH, QUEUE_HEADERS, updated_rows)

    if tab_exists(sheets_service, spreadsheet_id, QUEUE_TAB):
        sheet_id = ensure_tab(sheets_service, spreadsheet_id, QUEUE_TAB)
        clear_tab(sheets_service, spreadsheet_id, QUEUE_TAB)
        write_rows(sheets_service, spreadsheet_id, QUEUE_TAB, QUEUE_HEADERS, updated_rows)
        freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(QUEUE_HEADERS))

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": "SUCCESS",
        "queue_rows": len(queue_rows),
        "filled_count": filled_count,
        "image_urls_still_blank": blank_after_count,
        "message": "Populated competitor image URLs only from safe existing sheet data; no fabricated URLs were added.",
    }
    append_csv_log(
        LOG_PATH,
        ["timestamp", "spreadsheet_id", "status", "queue_rows", "filled_count", "image_urls_still_blank", "message"],
        [log_row],
    )

    payload = {
        "status": "SUCCESS",
        "queue_rows": len(queue_rows),
        "filled_count": filled_count,
        "image_urls_still_blank": blank_after_count,
        "blank_before_count": blank_before_count,
        "tabs_updated": [QUEUE_TAB],
        "local_output": str(LOCAL_OUTPUT_PATH),
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return payload


def main() -> None:
    try:
        populate_competitor_product_image_urls()
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
