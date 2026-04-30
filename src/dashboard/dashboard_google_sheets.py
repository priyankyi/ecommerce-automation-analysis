from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence

import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.auth_google import build_services, load_service_account_credentials, project_root
from src.marketplaces.flipkart.flipkart_sheet_helpers import load_json

PROJECT_ROOT = project_root()
SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
READONLY_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
SERVICE_ACCOUNT_SECRET_KEY = "gcp_service_account"
SPREADSHEET_ID_SECRET_KEY = "MASTER_SPREADSHEET_ID"


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in {429, 500, 502, 503} or attempt == attempts:
                raise
            import time

            time.sleep(delay)
            delay *= 2


def _safe_secrets() -> Dict[str, Any]:
    try:
        return dict(st.secrets)
    except Exception:
        return {}


def _normalize_secret_info(secret_value: Any) -> Dict[str, Any] | None:
    if isinstance(secret_value, dict) and secret_value:
        return dict(secret_value)
    if isinstance(secret_value, str):
        text = secret_value.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return None
            if isinstance(parsed, dict) and parsed:
                return parsed
    return None


def resolve_spreadsheet_id() -> tuple[str, str]:
    secrets = _safe_secrets()
    secret_spreadsheet_id = str(secrets.get(SPREADSHEET_ID_SECRET_KEY, "")).strip()
    if secret_spreadsheet_id:
        return secret_spreadsheet_id, "Streamlit Secrets"

    env_spreadsheet_id = os.environ.get(SPREADSHEET_ID_SECRET_KEY, "").strip()
    if env_spreadsheet_id:
        return env_spreadsheet_id, "ENV"

    if SPREADSHEET_META_PATH.exists():
        return str(load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]).strip(), "Local JSON"

    raise FileNotFoundError("MASTER_SPREADSHEET_ID is missing. Add it in Streamlit Cloud Secrets.")


def build_dashboard_services() -> tuple[object, str]:
    secrets = _safe_secrets()
    service_account_info = _normalize_secret_info(secrets.get(SERVICE_ACCOUNT_SECRET_KEY))
    if service_account_info:
        creds = load_service_account_credentials(service_account_info, scopes=READONLY_SCOPES)
        sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False, static_discovery=False)
        return sheets_service, "Streamlit Secrets"

    sheets_service, _, _ = build_services()
    return sheets_service, "Local"


def values_to_dataframe(values: Sequence[Sequence[Any]]) -> pd.DataFrame:
    if not values:
        return pd.DataFrame()
    headers = [str(cell) for cell in values[0]]
    rows: list[Dict[str, Any]] = []
    for row in values[1:]:
        row_dict = {headers[index]: row[index] if index < len(row) else "" for index in range(len(headers))}
        if any(str(value).strip() for value in row_dict.values()):
            rows.append(row_dict)
    return pd.DataFrame(rows, columns=headers)


def get_tab_names(sheets_service: object, spreadsheet_id: str) -> list[str]:
    metadata = retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))")
        .execute()
    )
    tab_names: list[str] = []
    for sheet in metadata.get("sheets", []):
        title = sheet.get("properties", {}).get("title")
        if title:
            tab_names.append(str(title))
    return tab_names


def get_sheet_values(sheets_service: object, spreadsheet_id: str, range_name: str) -> list[list[Any]]:
    response = retry(
        lambda: sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def read_tab_dataframe(sheets_service: object, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    rows = get_sheet_values(sheets_service, spreadsheet_id, f"{tab_name}!A1:ZZ")
    return values_to_dataframe(rows)


@st.cache_data(ttl=300, show_spinner=False)
def load_dashboard_payload() -> Dict[str, Any]:
    load_timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    secrets = _safe_secrets()
    service_account_info = _normalize_secret_info(secrets.get(SERVICE_ACCOUNT_SECRET_KEY))
    payload: Dict[str, Any] = {
        "spreadsheet_id": "",
        "spreadsheet_id_source": "",
        "auth_mode": "Local",
        "spreadsheet_connected": False,
        "last_data_load_timestamp": load_timestamp,
        "load_status": "initial",
        "load_message": "",
        "available_tabs": [],
        "missing_tabs": [],
        "frames": {},
        "row_counts": {},
    }

    try:
        spreadsheet_id, spreadsheet_id_source = resolve_spreadsheet_id()
    except FileNotFoundError as exc:
        return {
            **payload,
            "spreadsheet_id_source": "",
            "load_status": "missing_spreadsheet_id",
            "load_message": str(exc),
        }
    except Exception as exc:
        return {
            **payload,
            "load_status": "sheet_error",
            "load_message": f"Unable to resolve spreadsheet id. {exc.__class__.__name__}: {exc}",
        }

    try:
        sheets_service, auth_mode = build_dashboard_services()
    except FileNotFoundError as exc:
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": "Streamlit Secrets" if service_account_info else "Local",
            "load_status": "missing_secrets",
            "load_message": (
                "No usable Google auth credentials were found. "
                "If you're on Streamlit Cloud, add the Google service-account secrets in Advanced settings. "
                "If you're running locally, keep the OAuth credentials/token files available. "
                f"({exc})"
            ),
        }
    except Exception as exc:
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": "Streamlit Secrets" if service_account_info else "Local",
            "load_status": "auth_error",
            "load_message": f"Unable to initialize Google Sheets access. {exc.__class__.__name__}: {exc}",
        }

    try:
        available_tabs = get_tab_names(sheets_service, spreadsheet_id)
        frames: Dict[str, pd.DataFrame] = {}
        missing_tabs: list[str] = []
        for tab_name in [
            "LOOKER_FLIPKART_EXECUTIVE_SUMMARY",
            "LOOKER_FLIPKART_FSN_METRICS",
            "LOOKER_FLIPKART_ALERTS",
            "LOOKER_FLIPKART_ACTIONS",
            "LOOKER_FLIPKART_ADS",
            "LOOKER_FLIPKART_RETURNS",
            "LOOKER_FLIPKART_LISTINGS",
            "LOOKER_FLIPKART_RUN_COMPARISON",
            "LOOKER_FLIPKART_ADJUSTED_PROFIT",
            "LOOKER_FLIPKART_REPORT_FORMAT_MONITOR",
            "LOOKER_FLIPKART_RUN_QUALITY_SCORE",
            "LOOKER_FLIPKART_MODULE_CONFIDENCE",
            "LOOKER_FLIPKART_DEMAND_PROFILE",
            "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE",
            "FLIPKART_RETURN_COMMENTS",
            "FLIPKART_RETURN_ISSUE_SUMMARY",
            "FLIPKART_RETURN_REASON_PIVOT",
            "FLIPKART_MISSING_ACTIVE_LISTINGS",
            "FLIPKART_FSN_RUN_COMPARISON",
            "FLIPKART_VISUAL_COMPETITOR_RESULTS",
        ]:
            if tab_name in available_tabs:
                frames[tab_name] = read_tab_dataframe(sheets_service, spreadsheet_id, tab_name)
            else:
                frames[tab_name] = pd.DataFrame()
                missing_tabs.append(tab_name)

        row_counts = {tab_name: int(len(df)) for tab_name, df in frames.items()}
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            "spreadsheet_connected": True,
            "load_status": "ok",
            "load_message": "",
            "available_tabs": sorted(available_tabs),
            "missing_tabs": missing_tabs,
            "frames": frames,
            "row_counts": row_counts,
        }
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        if status == 429:
            return {
                **payload,
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_id_source": spreadsheet_id_source,
                "auth_mode": auth_mode,
                "load_status": "quota_limited",
                "load_message": "Google Sheets quota limit reached. Wait 5 minutes and refresh the dashboard.",
            }
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            "load_status": "sheet_error",
            "load_message": f"Unable to read Google Sheets. {exc.__class__.__name__}: {exc}",
        }
    except Exception as exc:
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            "load_status": "sheet_error",
            "load_message": f"Unable to load dashboard data. {exc.__class__.__name__}: {exc}",
        }
