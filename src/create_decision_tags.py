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
LOG_PATH = PROJECT_ROOT / "data" / "logs" / "decision_tags_log.csv"

SCORECARD_TAB = "SKU_SCORECARD"
TAB_NAME = "DECISION_TAGS"

HEADERS = [
    "SKU_ID",
    "Marketplace",
    "Total_Score",
    "Recommended_Action",
    "Final_Decision_Tag",
    "Decision_Reason",
    "Owner",
    "Review_Status",
    "Action_Deadline",
    "Last_Updated",
    "Remarks",
]

REVIEW_STATUS_OPTIONS = [
    "Pending",
    "Approved",
    "Rejected",
    "Needs Review",
]

FINAL_DECISION_TAG_OPTIONS = [
    "Scale",
    "Maintain",
    "Fix",
    "Relist",
    "Liquidate",
    "Kill",
    "Hold",
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


def to_int(value: object) -> int:
    text = clean_text(value)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def map_final_decision_tag(recommended_action: str) -> str:
    mapping = {
        "Scale": "Scale",
        "Maintain": "Maintain",
        "Fix": "Fix",
        "Reduce Ads / Liquidate": "Liquidate",
        "Kill": "Kill",
    }
    return mapping.get(clean_text(recommended_action), "Hold")


def normalize_review_status(status: str) -> str:
    text = clean_text(status)
    return text if text in REVIEW_STATUS_OPTIONS else "Pending"


def build_lookup(rows: List[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, str]]:
    lookup: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in rows:
        sku_id = clean_text(row.get("SKU_ID"))
        marketplace = clean_text(row.get("Marketplace"))
        if not sku_id or not marketplace or (sku_id, marketplace) in lookup:
            continue
        lookup[(sku_id, marketplace)] = row
    return lookup


def build_ordered_pairs(rows: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    ordered: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for row in rows:
        key = (clean_text(row.get("SKU_ID")), clean_text(row.get("Marketplace")))
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def build_row(
    sku_id: str,
    marketplace: str,
    scorecard_row: Dict[str, str],
    existing_row: Dict[str, str] | None,
) -> List[str]:
    total_score = clean_text(scorecard_row.get("Total_Score"))
    recommended_action = clean_text(scorecard_row.get("Recommended_Action"))
    decision_reason = clean_text(scorecard_row.get("Reason"))
    final_decision_tag = map_final_decision_tag(recommended_action)
    timestamp = datetime.now().isoformat(timespec="seconds")

    owner = clean_text(existing_row.get("Owner")) if existing_row else ""
    review_status = normalize_review_status(existing_row.get("Review_Status")) if existing_row else "Pending"
    action_deadline = clean_text(existing_row.get("Action_Deadline")) if existing_row else ""
    remarks = clean_text(existing_row.get("Remarks")) if existing_row else ""

    return [
        sku_id,
        marketplace,
        total_score,
        recommended_action,
        final_decision_tag,
        decision_reason,
        owner,
        review_status,
        action_deadline,
        timestamp,
        remarks,
    ]


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
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 7,
                    "endColumnIndex": 8,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in REVIEW_STATUS_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 4,
                    "endColumnIndex": 5,
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": value} for value in FINAL_DECISION_TAG_OPTIONS],
                    },
                    "showCustomUi": True,
                    "strict": True,
                },
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
            fieldnames=["timestamp", "sku_id", "marketplace", "total_score", "final_decision_tag", "status", "message"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(log_rows)


def ensure_decision_tags() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    sheet_id = ensure_tab(sheets_service, spreadsheet_id, TAB_NAME)
    existing_headers = get_sheet_values(sheets_service, spreadsheet_id, f"{TAB_NAME}!A1:K1")
    headers_written = ensure_headers(
        sheets_service,
        spreadsheet_id,
        existing_headers[0] if existing_headers else [],
    )

    _, scorecard_rows = read_table(sheets_service, spreadsheet_id, SCORECARD_TAB)
    _, existing_rows = read_table(sheets_service, spreadsheet_id, TAB_NAME)

    scorecard_lookup = build_lookup(scorecard_rows)
    existing_lookup = build_lookup(existing_rows)
    ordered_pairs = build_ordered_pairs(scorecard_rows)

    rows_to_write: List[List[str]] = []
    log_rows: List[Dict[str, object]] = []
    for sku_id, marketplace in ordered_pairs:
        scorecard_row = scorecard_lookup[(sku_id, marketplace)]
        existing_row = existing_lookup.get((sku_id, marketplace))
        row = build_row(sku_id, marketplace, scorecard_row, existing_row)
        rows_to_write.append(row)
        log_rows.append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "sku_id": sku_id,
                "marketplace": marketplace,
                "total_score": row[2],
                "final_decision_tag": row[4],
                "status": "updated" if existing_row else "created",
                "message": "Decision tag row built from SKU_SCORECARD",
            }
        )

    clear_data_rows(sheets_service, spreadsheet_id)
    if rows_to_write:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{TAB_NAME}!A2:{zero_based_column_index_to_a1(len(HEADERS) - 1)}{len(rows_to_write) + 1}",
            valueInputOption="RAW",
            body={"values": rows_to_write},
        ).execute()

    apply_formatting(sheets_service, spreadsheet_id, sheet_id)
    append_log_rows(log_rows)

    print("Decision tags summary:")
    print(f"  Rows read from SKU_SCORECARD: {len(scorecard_rows)}")
    print(f"  Existing DECISION_TAGS rows read: {len(existing_rows)}")
    print(f"  Rows written to DECISION_TAGS: {len(rows_to_write)}")

    return {
        "spreadsheet_id": spreadsheet_id,
        "decision_tags_sheet_name": TAB_NAME,
        "decision_tags_sheet_id": sheet_id,
        "headers_written": headers_written,
        "scorecard_rows_read": len(scorecard_rows),
        "existing_rows_read": len(existing_rows),
        "rows_written": len(rows_to_write),
        "log_path": str(LOG_PATH),
    }


def main() -> None:
    result = ensure_decision_tags()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
