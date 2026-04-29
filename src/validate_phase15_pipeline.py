from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"

MASTER_SKU_TAB = "MASTER_SKU"
PRODUCT_CONTENT_TAB = "PRODUCT_CONTENT"
LISTING_STATUS_TAB = "LISTING_STATUS"
REVIEW_RATING_TAB = "REVIEW_RATING"

MARKETPLACES = [
    "Flipkart",
    "Meesho",
    "Snapdeal",
    "FirstCry",
    "MyStore",
    "Shopify",
]

EXPORT_TABS = [
    "FLIPKART_EXPORT",
    "MEESHO_EXPORT",
    "SNAPDEAL_EXPORT",
    "FIRSTCRY_EXPORT",
    "MYSTORE_EXPORT",
    "SHOPIFY_EXPORT",
]

REQUIRED_MASTER_FIELDS = [
    "SKU_ID",
    "Product_Title",
    "Category",
    "Cost_Price",
    "MRP",
    "Selling_Price",
    "GST_Rate",
    "HSN_Code",
    "Length_cm",
    "Width_cm",
    "Height_cm",
    "Dead_Weight_kg",
    "Available_Stock",
    "Reorder_Level",
    "Product_Status",
]


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


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


def get_sheet_values(sheets_service, spreadsheet_id: str, range_name: str) -> List[List[str]]:
    return retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
        .get("values", [])
    )


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


def build_table(rows: List[List[str]]) -> Tuple[List[str], List[Dict[str, str]]]:
    if len(rows) < 2:
        return [], []
    headers = rows[0]
    data = [{headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))} for row in rows[1:]]
    return headers, data


def count_real_skus(master_rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    real_rows: List[Dict[str, str]] = []
    sample_rows: List[Dict[str, str]] = []
    for row in master_rows:
        sku_id = clean_text(row.get("SKU_ID"))
        if sku_id.upper().startswith("LED-TEST-"):
            sample_rows.append(row)
        elif sku_id:
            real_rows.append(row)
    return real_rows, sample_rows


def build_lookup(rows: List[Dict[str, str]], key_fields: Tuple[str, ...]) -> Dict[Tuple[str, ...], Dict[str, str]]:
    lookup: Dict[Tuple[str, ...], Dict[str, str]] = {}
    for row in rows:
        key = tuple(clean_text(row.get(field)) for field in key_fields)
        if any(not part for part in key) or key in lookup:
            continue
        lookup[key] = row
    return lookup


def parse_drive_folder_id(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    match = re.search(r"/folders/([A-Za-z0-9_-]+)", text)
    if match:
        return match.group(1)
    return text


def validate_folder_exists(drive_service, folder_id: str) -> bool:
    if not folder_id:
        return False
    try:
        retry(
            lambda: drive_service.files()
            .get(fileId=folder_id, fields="id", supportsAllDrives=True)
            .execute()
        )
        return True
    except Exception:
        return False


def validate_phase15_pipeline() -> Dict[str, object]:
    meta = load_json(SPREADSHEET_META_PATH)
    spreadsheet_id = meta["spreadsheet_id"]
    sheets_service, drive_service, _ = build_services()

    master_rows = build_table(get_sheet_values(sheets_service, spreadsheet_id, f"{MASTER_SKU_TAB}!A1:ZZ"))[1]
    product_content_rows = build_table(get_sheet_values(sheets_service, spreadsheet_id, f"{PRODUCT_CONTENT_TAB}!A1:ZZ"))[1]
    listing_rows = build_table(get_sheet_values(sheets_service, spreadsheet_id, f"{LISTING_STATUS_TAB}!A1:ZZ"))[1]
    review_rows = build_table(get_sheet_values(sheets_service, spreadsheet_id, f"{REVIEW_RATING_TAB}!A1:ZZ"))[1]

    export_rows: Dict[str, List[Dict[str, str]]] = {}
    for tab_name in EXPORT_TABS:
        export_rows[tab_name] = build_table(get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ"))[1]

    real_rows, sample_rows = count_real_skus(master_rows)
    real_sku_ids = [clean_text(row.get("SKU_ID")) for row in real_rows]
    real_sku_ids = [sku for sku in real_sku_ids if sku]

    errors: List[str] = []
    warnings: List[str] = []

    if len(real_sku_ids) < 50:
        errors.append(f"MASTER_SKU has {len(real_sku_ids)} real SKUs; expected at least 50.")

    seen = set()
    duplicate_skus = []
    for row in master_rows:
        sku_id = clean_text(row.get("SKU_ID"))
        if not sku_id:
            continue
        if sku_id in seen:
            duplicate_skus.append(sku_id)
        seen.add(sku_id)
    if duplicate_skus:
        errors.append(f"Duplicate SKU_ID values found in MASTER_SKU: {', '.join(sorted(set(duplicate_skus)))}")

    missing_required = []
    real_master_lookup = {clean_text(row.get("SKU_ID")): row for row in real_rows}
    for sku_id, row in real_master_lookup.items():
        for field in REQUIRED_MASTER_FIELDS:
            if not clean_text(row.get(field)):
                missing_required.append(f"{sku_id}: {field}")
    if missing_required:
        errors.append(f"Required MASTER_SKU fields blank: {len(missing_required)} issues.")

    product_lookup = build_lookup(product_content_rows, ("SKU_ID",))
    missing_product_content = [sku for sku in real_sku_ids if (sku,) not in product_lookup]
    if missing_product_content:
        errors.append(f"Missing PRODUCT_CONTENT rows for {len(missing_product_content)} real SKUs.")

    master_folder_links = {clean_text(row.get("SKU_ID")): clean_text(row.get("Image_Folder_Link")) for row in master_rows}
    missing_folders = []
    for sku_id in real_sku_ids:
        folder_id = parse_drive_folder_id(master_folder_links.get(sku_id, ""))
        if not validate_folder_exists(drive_service, folder_id):
            missing_folders.append(sku_id)
    if missing_folders:
        errors.append(f"Missing image folders for {len(missing_folders)} real SKUs.")

    export_missing = []
    for tab_name, rows in export_rows.items():
        lookup = build_lookup(rows, ("SKU_ID",))
        missing = [sku for sku in real_sku_ids if (sku,) not in lookup]
        if missing:
            export_missing.append(f"{tab_name}: {len(missing)} missing")
    if export_missing:
        errors.append("Marketplace export tabs missing rows: " + "; ".join(export_missing))

    listing_lookup = build_lookup(listing_rows, ("SKU_ID", "Marketplace"))
    review_lookup = build_lookup(review_rows, ("SKU_ID", "Marketplace"))
    listing_missing = []
    review_missing = []
    for sku_id in real_sku_ids:
        for marketplace in MARKETPLACES:
            if (sku_id, marketplace) not in listing_lookup:
                listing_missing.append(f"{sku_id}:{marketplace}")
            if (sku_id, marketplace) not in review_lookup:
                review_missing.append(f"{sku_id}:{marketplace}")
    if listing_missing:
        errors.append(f"LISTING_STATUS missing {len(listing_missing)} SKU x marketplace rows.")
    if review_missing:
        errors.append(f"REVIEW_RATING missing {len(review_missing)} SKU x marketplace rows.")

    result = {
        "real_sku_count": len(real_sku_ids),
        "sample_sku_count": len(sample_rows),
        "product_content_rows": len(product_content_rows),
        "listing_status_rows": len(listing_rows),
        "review_rating_rows": len(review_rows),
        "validation_errors": errors,
        "warnings": warnings,
        "passed": not errors,
    }

    print(json.dumps(result, indent=2))
    return result


def main() -> None:
    validate_phase15_pipeline()


if __name__ == "__main__":
    main()
