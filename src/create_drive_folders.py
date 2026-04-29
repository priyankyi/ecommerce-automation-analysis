from __future__ import annotations

import json
from typing import Dict, List

from src.auth_google import build_services, project_root

ROOT_FOLDER_NAME = "ECOM_CONTROL_TOWER"
SUBFOLDERS = [
    "01_MASTER_SKU",
    "02_PRODUCT_IMAGES",
    "03_MARKETPLACE_EXPORTS",
    "04_ORDERS_RAW",
    "05_SETTLEMENT_RAW",
    "06_ADS_RAW",
    "07_REVIEWS_RAW",
    "08_OUTPUT_REPORTS",
    "09_LOGS",
]


def find_folder(drive_service, name: str, parent_id: str | None = None) -> str | None:
    safe_name = name.replace("'", "\\'")
    query_parts = [
        "mimeType = 'application/vnd.google-apps.folder'",
        f"name = '{safe_name}'",
        "trashed = false",
    ]
    if parent_id:
        query_parts.append(f"'{parent_id}' in parents")
    query = " and ".join(query_parts)
    response = (
        drive_service.files()
        .list(q=query, fields="files(id, name)", pageSize=10, includeItemsFromAllDrives=True, supportsAllDrives=True)
        .execute()
    )
    files = response.get("files", [])
    return files[0]["id"] if files else None


def create_folder(drive_service, name: str, parent_id: str | None = None) -> str:
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]
    folder = drive_service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


def ensure_folder(drive_service, name: str, parent_id: str | None = None) -> str:
    folder_id = find_folder(drive_service, name, parent_id=parent_id)
    if folder_id:
        return folder_id
    return create_folder(drive_service, name, parent_id=parent_id)


def ensure_drive_structure() -> Dict[str, object]:
    _, drive_service, _ = build_services()

    root_id = ensure_folder(drive_service, ROOT_FOLDER_NAME)
    subfolder_ids: Dict[str, str] = {}
    for folder_name in SUBFOLDERS:
        subfolder_ids[folder_name] = ensure_folder(drive_service, folder_name, parent_id=root_id)

    result = {
        "root_folder_name": ROOT_FOLDER_NAME,
        "root_folder_id": root_id,
        "subfolders": subfolder_ids,
    }
    return result


def main() -> None:
    structure = ensure_drive_structure()
    output_path = project_root() / "data" / "output" / "drive_folders.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(structure, indent=2), encoding="utf-8")
    print(json.dumps(structure, indent=2))
    print(f"Saved folder map to: {output_path}")


if __name__ == "__main__":
    main()
