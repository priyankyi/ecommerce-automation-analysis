from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from googleapiclient.errors import HttpError

from src.auth_google import build_services, project_root

TEMPLATE_PATH = project_root() / "templates" / "master_sku_columns.json"


def load_template() -> Dict[str, object]:
    return json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))


def escape_query_value(value: str) -> str:
    return value.replace("'", "\\'")


def find_spreadsheet(drive_service, spreadsheet_name: str) -> str | None:
    query = (
        "mimeType = 'application/vnd.google-apps.spreadsheet' and "
        f"name = '{escape_query_value(spreadsheet_name)}' and trashed = false"
    )
    response = (
        drive_service.files()
        .list(q=query, fields="files(id, name)", pageSize=10, includeItemsFromAllDrives=True, supportsAllDrives=True)
        .execute()
    )
    files = response.get("files", [])
    return files[0]["id"] if files else None


def create_spreadsheet(sheets_service, spreadsheet_name: str) -> str:
    body = {
        "properties": {
            "title": spreadsheet_name,
        }
    }
    response = sheets_service.spreadsheets().create(body=body, fields="spreadsheetId").execute()
    return response["spreadsheetId"]


def get_sheet_metadata(sheets_service, spreadsheet_id: str) -> Dict[str, object]:
    return (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )


def ensure_sheet_tab(sheets_service, spreadsheet_id: str, sheet_name: str) -> int:
    metadata = get_sheet_metadata(sheets_service, spreadsheet_id)
    for sheet in metadata.get("sheets", []):
        properties = sheet.get("properties", {})
        if properties.get("title") == sheet_name:
            return properties["sheetId"]

    sheets = metadata.get("sheets", [])
    if len(sheets) == 1:
        existing_sheet = sheets[0].get("properties", {})
        existing_title = existing_sheet.get("title")
        existing_sheet_id = existing_sheet.get("sheetId")
        if existing_sheet_id is not None and existing_title:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": existing_sheet_id,
                                    "title": sheet_name,
                                },
                                "fields": "title",
                            }
                        }
                    ]
                },
            ).execute()
            return existing_sheet_id

    response = (
        sheets_service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": sheet_name,
                            }
                        }
                    }
                ]
            },
        )
        .execute()
    )
    add_sheet_response = response["replies"][0]["addSheet"]["properties"]
    return add_sheet_response["sheetId"]


def update_values(sheets_service, spreadsheet_id: str, range_name: str, values: List[List[str]]) -> None:
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def build_dropdown_request(sheet_id: int, a1_range: str, options: List[str]) -> Dict[str, object]:
    return {
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 1000,
                "startColumnIndex": a1_range_to_zero_based_column(a1_range),
                "endColumnIndex": a1_range_to_zero_based_column(a1_range) + 1,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": value} for value in options],
                },
                "showCustomUi": True,
                "strict": True,
            },
        }
    }


def a1_range_to_zero_based_column(a1_range: str) -> int:
    column = a1_range.split(":")[0]
    letters = "".join(ch for ch in column if ch.isalpha()).upper()
    result = 0
    for char in letters:
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def configure_master_sheet(sheets_service, spreadsheet_id: str, sheet_id: int, template: Dict[str, object]) -> None:
    headers = template["headers"]
    formulas = template["formulas"]
    dropdowns = template["dropdowns"]

    requests: List[Dict[str, object]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 1,
                    },
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
    ]

    for dropdown in dropdowns:
        requests.append(
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 1000,
                        "startColumnIndex": a1_range_to_zero_based_column(dropdown["range"]),
                        "endColumnIndex": a1_range_to_zero_based_column(dropdown["range"]) + 1,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": value} for value in dropdown["values"]],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            }
        )

    for formula in formulas:
        requests.append(
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 2,
                        "startColumnIndex": a1_range_to_zero_based_column(formula["cell"]),
                        "endColumnIndex": a1_range_to_zero_based_column(formula["cell"]) + 1,
                    },
                    "rows": [
                        {
                            "values": [
                                {
                                    "userEnteredValue": {
                                        "formulaValue": formula["formula"]
                                    }
                                }
                            ]
                        }
                    ],
                    "fields": "userEnteredValue.formulaValue",
                }
            }
        )

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()

    update_values(sheets_service, spreadsheet_id, "MASTER_SKU!A1:Y1", [headers])


def ensure_master_sku_sheet() -> Dict[str, object]:
    template = load_template()
    spreadsheet_name = template["spreadsheet_name"]
    sheet_name = template["sheet_name"]

    sheets_service, drive_service, _ = build_services()
    spreadsheet_id = find_spreadsheet(drive_service, spreadsheet_name)
    if not spreadsheet_id:
        spreadsheet_id = create_spreadsheet(sheets_service, spreadsheet_name)

    sheet_id = ensure_sheet_tab(sheets_service, spreadsheet_id, sheet_name)
    configure_master_sheet(sheets_service, spreadsheet_id, sheet_id, template)

    return {
        "spreadsheet_name": spreadsheet_name,
        "spreadsheet_id": spreadsheet_id,
        "sheet_name": sheet_name,
        "sheet_id": sheet_id,
    }


def main() -> None:
    result = ensure_master_sku_sheet()
    output_path = project_root() / "data" / "output" / "master_sku_sheet.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"Saved sheet info to: {output_path}")


if __name__ == "__main__":
    main()
