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
DEFAULT_MASTER_SPREADSHEET_ID = "1E9xtLqrMtaio5O0jA0ypx6fsAV2ufKb121G3q8qB_Cg"
READONLY_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
SERVICE_ACCOUNT_SECRET_KEY = "gcp_service_account"
SPREADSHEET_ID_SECRET_KEY = "MASTER_SPREADSHEET_ID"
REQUIRED_SERVICE_ACCOUNT_KEYS = {
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "auth_uri",
    "token_uri",
    "auth_provider_x509_cert_url",
    "client_x509_cert_url",
}


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
        normalized = dict(secret_value)
    elif isinstance(secret_value, str):
        text = secret_value.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return None
            if isinstance(parsed, dict) and parsed:
                normalized = dict(parsed)
            else:
                return None
        else:
            return None
    else:
        return None

    if "private_key" in normalized and normalized["private_key"] is not None:
        private_key = str(normalized["private_key"]).strip()
        private_key = private_key.replace("\\n", "\n")
        normalized["private_key"] = private_key
    return normalized


def _safe_secret_container() -> Any:
    try:
        return st.secrets
    except Exception:
        return {}


def _has_secret_key(container: Any, key: str) -> bool:
    try:
        return key in container
    except Exception:
        return False


def _get_secret_value(container: Any, key: str) -> Any:
    if isinstance(container, dict):
        return container.get(key)
    if _has_secret_key(container, key):
        try:
            return container[key]
        except Exception:
            pass
    try:
        return container.get(key)
    except Exception:
        return None


def _load_service_account_secret() -> tuple[Dict[str, Any] | None, bool, str]:
    container = _safe_secret_container()
    if not _has_secret_key(container, SERVICE_ACCOUNT_SECRET_KEY):
        return None, False, ""
    secret_value = _get_secret_value(container, SERVICE_ACCOUNT_SECRET_KEY)
    info = _normalize_secret_info(secret_value)
    email = ""
    if info:
        email = str(info.get("client_email", "")).strip()
    return info, True, email


def _validate_service_account_info(service_account_info: Dict[str, Any]) -> list[str]:
    missing = []
    for key in sorted(REQUIRED_SERVICE_ACCOUNT_KEYS):
        value = service_account_info.get(key, "")
        if value is None or not str(value).strip():
            missing.append(key)
    return missing


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

    return DEFAULT_MASTER_SPREADSHEET_ID, "Default"


def build_dashboard_services() -> tuple[object, str, Dict[str, Any]]:
    service_account_info, service_account_found, service_account_email = _load_service_account_secret()
    meta: Dict[str, Any] = {
        "streamlit_secrets_available": service_account_found,
        "gcp_service_account_found": service_account_found,
        "service_account_email": service_account_email,
        "private_key_present": bool(str(service_account_info.get("private_key", "")).strip()) if service_account_info else False,
    }

    if service_account_found:
        if not service_account_info:
            raise ValueError("gcp_service_account block found but could not be parsed as a mapping.")
        missing_keys = _validate_service_account_info(service_account_info)
        if missing_keys:
            raise ValueError(
                "gcp_service_account block found but is missing required keys: "
                + ", ".join(missing_keys)
            )
        creds = load_service_account_credentials(service_account_info, scopes=READONLY_SCOPES)
        sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False, static_discovery=False)
        meta["auth_mode"] = "Streamlit Secrets"
        return sheets_service, "Streamlit Secrets", meta

    sheets_service, _, _ = build_services()
    meta["auth_mode"] = "Local"
    return sheets_service, "Local", meta


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
    service_account_info, service_account_found, service_account_email = _load_service_account_secret()
    payload: Dict[str, Any] = {
        "spreadsheet_id": "",
        "spreadsheet_id_source": "",
        "auth_mode": "Local",
        "streamlit_secrets_available": bool(secrets),
        "gcp_service_account_found": service_account_found,
        "service_account_email": service_account_email,
        "private_key_present": bool(str(service_account_info.get("private_key", "")).strip()) if service_account_info else False,
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
        sheets_service, auth_mode, auth_meta = build_dashboard_services()
    except ValueError as exc:
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": "Streamlit Secrets" if service_account_found else "Local",
            "streamlit_secrets_available": bool(secrets),
            "gcp_service_account_found": service_account_found,
            "service_account_email": service_account_email,
            "private_key_present": bool(str(service_account_info.get("private_key", "")).strip()) if service_account_info else False,
            "load_status": "auth_error",
            "load_message": (
                "Service account secrets found but Google auth failed. "
                "Check private_key formatting and Google Sheet sharing."
                if service_account_found
                else "gcp_service_account block not found in Streamlit Secrets."
            ),
        }
    except Exception as exc:
        if service_account_found:
            message = "Service account secrets found but Google auth failed. Check private_key formatting and Google Sheet sharing."
            load_status = "auth_error"
        else:
            message = f"Unable to initialize Google Sheets access. {exc.__class__.__name__}: {exc}"
            load_status = "missing_secrets"
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": "Streamlit Secrets" if service_account_found else "Local",
            "streamlit_secrets_available": bool(secrets),
            "gcp_service_account_found": service_account_found,
            "service_account_email": service_account_email,
            "private_key_present": bool(str(service_account_info.get("private_key", "")).strip()) if service_account_info else False,
            "load_status": load_status,
            "load_message": message,
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
            "streamlit_secrets_available": auth_meta["streamlit_secrets_available"],
            "gcp_service_account_found": auth_meta["gcp_service_account_found"],
            "service_account_email": auth_meta["service_account_email"],
            "private_key_present": auth_meta["private_key_present"],
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
                "streamlit_secrets_available": auth_meta["streamlit_secrets_available"],
                "gcp_service_account_found": auth_meta["gcp_service_account_found"],
                "service_account_email": auth_meta["service_account_email"],
                "private_key_present": auth_meta["private_key_present"],
                "load_status": "quota_limited",
                "load_message": "Google Sheets quota limit reached. Wait 5 minutes and refresh the dashboard.",
            }
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            "streamlit_secrets_available": auth_meta["streamlit_secrets_available"],
            "gcp_service_account_found": auth_meta["gcp_service_account_found"],
            "service_account_email": auth_meta["service_account_email"],
            "private_key_present": auth_meta["private_key_present"],
            "load_status": "sheet_error",
            "load_message": f"Unable to read Google Sheets. {exc.__class__.__name__}: {exc}",
        }
    except Exception as exc:
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            "streamlit_secrets_available": auth_meta["streamlit_secrets_available"],
            "gcp_service_account_found": auth_meta["gcp_service_account_found"],
            "service_account_email": auth_meta["service_account_email"],
            "private_key_present": auth_meta["private_key_present"],
            "load_status": "sheet_error",
            "load_message": f"Unable to load dashboard data. {exc.__class__.__name__}: {exc}",
        }
