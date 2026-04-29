from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import (
    add_basic_filter,
    clear_tab,
    column_index_to_a1,
    ensure_tab,
    freeze_and_format,
    load_json,
    read_table,
    set_list_validation,
    tab_exists,
    write_cells,
    write_rows,
)
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, clean_fsn, ensure_directories, normalize_text, now_iso, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_adjustment_ledger_log.csv"
SNAPSHOT_PATH = OUTPUT_DIR / "flipkart_adjustments_ledger_snapshot.csv"

SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
FSN_HISTORY_TAB = "FLIPKART_FSN_HISTORY"
RUN_HISTORY_TAB = "FLIPKART_RUN_HISTORY"
ADJUSTMENT_LEDGER_TAB = "FLIPKART_ADJUSTMENTS_LEDGER"

ADJUSTMENT_HEADERS = [
    "Adjustment_ID",
    "Adjustment_Date",
    "Related_Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Order_ID",
    "Order_Item_ID",
    "Adjustment_Type",
    "Adjustment_Amount",
    "Adjustment_Direction",
    "Reason",
    "Source_Report",
    "Evidence_Link",
    "Status",
    "Remarks",
    "Created_By",
    "Last_Updated",
]

ADJUSTMENT_TYPE_OPTIONS = [
    "Late Return Deduction",
    "Reverse Shipping",
    "SPF Adjustment",
    "Commission Correction",
    "Penalty",
    "Payment Hold",
    "Claim Credit",
    "TCS/TDS Correction",
    "Other",
]

ADJUSTMENT_DIRECTION_OPTIONS = ["Deduction", "Addition"]
ADJUSTMENT_STATUS_OPTIONS = ["Open", "Verified", "Applied", "Ignored", "Needs Review"]


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


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def normalize_adjustment_type(value: Any) -> str:
    text = normalize_text(value)
    for option in ADJUSTMENT_TYPE_OPTIONS:
        if text.lower() == option.lower():
            return option
    return text or "Other"


def normalize_direction(value: Any) -> str:
    text = normalize_text(value)
    for option in ADJUSTMENT_DIRECTION_OPTIONS:
        if text.lower() == option.lower():
            return option
    return ""


def normalize_status(value: Any) -> str:
    text = normalize_text(value)
    for option in ADJUSTMENT_STATUS_OPTIONS:
        if text.lower() == option.lower():
            return option
    return text or "Needs Review"


def stable_adjustment_id(row: Dict[str, Any]) -> str:
    key = "|".join(
        [
            clean_fsn(row.get("FSN", "")),
            normalize_text(row.get("Order_Item_ID", "")).upper(),
            normalize_adjustment_type(row.get("Adjustment_Type", "")).upper(),
            normalize_text(row.get("Adjustment_Date", "")).upper(),
            normalize_text(row.get("Adjustment_Amount", "")).replace(",", ""),
        ]
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12].upper()
    return f"FKADJ-{digest}"


def ensure_headers(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
) -> Tuple[List[str], List[Dict[str, str]], bool, int]:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    headers, rows = read_table(sheets_service, spreadsheet_id, tab_name)
    headers_written = False
    if not headers:
        write_rows(sheets_service, spreadsheet_id, tab_name, ADJUSTMENT_HEADERS, [])
        headers = list(ADJUSTMENT_HEADERS)
        headers_written = True
    else:
        merged_headers = list(headers)
        for header in ADJUSTMENT_HEADERS:
            if header not in merged_headers:
                merged_headers.append(header)
        if merged_headers != headers:
            write_rows(sheets_service, spreadsheet_id, tab_name, merged_headers, rows)
            headers = merged_headers
            headers_written = True
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))
    add_basic_filter(sheets_service, spreadsheet_id, sheet_id, len(headers), len(rows) + 1)
    return headers, rows, headers_written, sheet_id


def build_adjustment_ledger() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    required_tabs = [SKU_ANALYSIS_TAB, FSN_HISTORY_TAB, RUN_HISTORY_TAB]
    for tab_name in required_tabs:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    ledger_headers, ledger_rows, headers_written, sheet_id = ensure_headers(
        sheets_service,
        spreadsheet_id,
        ADJUSTMENT_LEDGER_TAB,
    )

    updated_rows: List[Dict[str, Any]] = []
    id_updates: List[Tuple[str, Any]] = []
    adjustment_id_index = ledger_headers.index("Adjustment_ID") if "Adjustment_ID" in ledger_headers else -1
    adjustment_amount_index = ledger_headers.index("Adjustment_Amount") if "Adjustment_Amount" in ledger_headers else -1

    for row_index, row in enumerate(ledger_rows, start=2):
        updated_row = dict(row)
        amount = abs(parse_float(updated_row.get("Adjustment_Amount", "")))
        adjustment_id = normalize_text(updated_row.get("Adjustment_ID", ""))
        if adjustment_id_index >= 0 and not adjustment_id and amount > 0:
            adjustment_id = stable_adjustment_id(updated_row)
            id_updates.append((f"{ADJUSTMENT_LEDGER_TAB}!{column_index_to_a1(adjustment_id_index + 1)}{row_index}", adjustment_id))
            updated_row["Adjustment_ID"] = adjustment_id
        if adjustment_amount_index >= 0 and normalize_text(updated_row.get("Adjustment_Amount", "")):
            updated_row["Adjustment_Amount"] = normalize_text(updated_row.get("Adjustment_Amount", ""))
        updated_rows.append(updated_row)

    if id_updates:
        write_cells(sheets_service, spreadsheet_id, id_updates)

    snapshot_headers = list(ledger_headers)
    snapshot_rows = [{header: row.get(header, "") for header in snapshot_headers} for row in updated_rows]
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SNAPSHOT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=snapshot_headers)
        writer.writeheader()
        for row in snapshot_rows:
            writer.writerow(row)

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "ledger_rows": len(updated_rows),
        "headers_written": str(bool(headers_written)),
        "manual_rows_preserved": len(updated_rows),
        "tab_updated": ADJUSTMENT_LEDGER_TAB,
        "status": "SUCCESS",
        "message": "Prepared Flipkart adjustment ledger structure without clearing manual rows",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "ledger_rows",
            "headers_written",
            "manual_rows_preserved",
            "tab_updated",
            "status",
            "message",
        ],
        [log_row],
    )

    result = {
        "status": "SUCCESS",
        "ledger_rows": len(updated_rows),
        "headers_written": bool(headers_written),
        "manual_rows_preserved": len(updated_rows),
        "tab_updated": ADJUSTMENT_LEDGER_TAB,
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main() -> None:
    try:
        build_adjustment_ledger()
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
