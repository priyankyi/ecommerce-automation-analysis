from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_utils import (
    SKU_ANALYSIS_STATE_PATH,
    PUSH_LOG_PATH,
    SKU_ANALYSIS_PATH,
    ensure_directories,
    csv_data_row_count,
    load_json,
    load_run_state,
    now_iso,
    path_mtime,
    save_run_state,
    write_csv,
    build_status_payload,
)

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
TAB_NAME = "FLIPKART_SKU_ANALYSIS"


def load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def column_index_to_a1(index: int) -> str:
    result = []
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def get_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, Any]:
    return (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def tab_exists(sheets_service, spreadsheet_id: str, tab_name: str) -> bool:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return True
    return False


def ensure_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    metadata = get_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return int(props["sheetId"])
    response = (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        )
        .execute()
    )
    return int(response["replies"][0]["addSheet"]["properties"]["sheetId"])


def read_sheet_values(sheets_service, spreadsheet_id: str, tab_name: str) -> List[List[Any]]:
    response = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:ZZ")
        .execute()
    )
    return response.get("values", [])


def backup_current_tab_before_push(
    sheets_service,
    spreadsheet_id: str,
    backup_path: Path,
) -> Dict[str, Any]:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    local_rows = load_rows(SKU_ANALYSIS_PATH) if SKU_ANALYSIS_PATH.exists() else []
    local_headers = list(local_rows[0].keys()) if local_rows else []
    if not tab_exists(sheets_service, spreadsheet_id, TAB_NAME):
        write_csv(backup_path, local_headers, [])
        return {
            "status": "EMPTY",
            "backup_path": str(backup_path),
            "rows_backed_up": 0,
            "message": "Target tab does not yet exist",
        }

    values = read_sheet_values(sheets_service, spreadsheet_id, TAB_NAME)
    if not values:
        write_csv(backup_path, local_headers, [])
        return {
            "status": "EMPTY",
            "backup_path": str(backup_path),
            "rows_backed_up": 0,
            "message": "Target tab is empty",
        }

    headers = [str(cell) for cell in values[0]] if values[0] else local_headers
    backup_rows: List[Dict[str, Any]] = []
    for row in values[1:]:
        backup_rows.append({header: row[index] if index < len(row) else "" for index, header in enumerate(headers)})
    write_csv(backup_path, headers, backup_rows)
    return {
        "status": "SUCCESS",
        "backup_path": str(backup_path),
        "rows_backed_up": len(backup_rows),
        "message": "FLIPKART_SKU_ANALYSIS backed up before push",
    }


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
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()


def push_flipkart_analysis_to_sheet(backup_path: Path | None = None) -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")
    if not SKU_ANALYSIS_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SKU_ANALYSIS_PATH}")
    sku_state = load_run_state(SKU_ANALYSIS_STATE_PATH)
    if str(sku_state.get("status", "")).upper() != "SUCCESS":
        raise RuntimeError(f"SKU analysis state is not successful: {SKU_ANALYSIS_STATE_PATH}")
    sku_state_mtime = path_mtime(SKU_ANALYSIS_STATE_PATH)
    if csv_data_row_count(SKU_ANALYSIS_PATH) <= 0:
        raise RuntimeError(f"Stale or empty SKU analysis file: {SKU_ANALYSIS_PATH}")
    if path_mtime(SKU_ANALYSIS_PATH) > sku_state_mtime + 1e-6:
        raise RuntimeError(f"Stale SKU analysis file newer than latest successful state: {SKU_ANALYSIS_PATH}")

    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()
    backup_result = {"backup_path": "", "rows_backed_up": 0, "status": "SKIPPED", "message": "No backup path provided"}
    if backup_path is not None:
        backup_result = backup_current_tab_before_push(sheets_service, spreadsheet_id, backup_path)
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, TAB_NAME)

    rows = load_rows(SKU_ANALYSIS_PATH)
    if not rows:
        raise RuntimeError(f"No rows found in {SKU_ANALYSIS_PATH}")
    headers = list(rows[0].keys())
    values = [headers] + [[row.get(header, "") for header in headers] for row in rows]

    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{TAB_NAME}!A:ZZ",
        body={},
    ).execute()
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{TAB_NAME}!A1:{column_index_to_a1(len(headers) - 1)}{len(values)}",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "tab_name": TAB_NAME,
        "rows_written": len(rows),
        "status": "SUCCESS",
        "message": "FLIPKART_SKU_ANALYSIS updated",
        "backup_before_push_path": backup_result.get("backup_path", ""),
        "backup_rows": backup_result.get("rows_backed_up", 0),
    }
    write_csv(PUSH_LOG_PATH, ["timestamp", "spreadsheet_id", "tab_name", "rows_written", "status", "message"], [log_row])
    save_run_state(
        PROJECT_ROOT / "data" / "output" / "marketplaces" / "flipkart" / "flipkart_push_state.json",
        {
            "status": "SUCCESS",
            "stage": "push",
            "generated_at": now_iso(),
            "sku_analysis_state_mtime": sku_state_mtime,
            "sku_analysis_mtime": path_mtime(SKU_ANALYSIS_PATH),
        },
    )

    result = {
        "status": "SUCCESS",
        "generated_at": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "tab_name": TAB_NAME,
        "rows_written": len(rows),
        "headers_written": len(headers),
        "log_path": str(PUSH_LOG_PATH),
        "backup_before_push_path": backup_result.get("backup_path", ""),
        "backup_rows": backup_result.get("rows_backed_up", 0),
    }
    payload = dict(result)
    payload.pop("status", None)
    print(json.dumps(build_status_payload("SUCCESS", **payload), indent=2))
    return result


def main() -> None:
    try:
        push_flipkart_analysis_to_sheet()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "analysis_path": str(SKU_ANALYSIS_PATH),
                    "log_path": str(PUSH_LOG_PATH),
                },
                indent=2,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
