from __future__ import annotations

from datetime import datetime
from collections.abc import Mapping
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence

import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

from src.auth_google import build_services, project_root
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
DASHBOARD_DEBUG_KEY = "DASHBOARD_DEBUG"
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

AUTH_STAGE_PARSE = "parse_streamlit_secrets"
AUTH_STAGE_CREDENTIALS = "create_service_account_credentials"
AUTH_STAGE_BUILD_SERVICE = "build_sheets_service"
AUTH_STAGE_TEST_READ = "test_sheet_read"


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


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def resolve_dashboard_debug_mode() -> bool:
    secrets = _safe_secrets()
    secret_value = secrets.get(DASHBOARD_DEBUG_KEY)
    env_value = os.environ.get(DASHBOARD_DEBUG_KEY)
    return _is_truthy(env_value) or _is_truthy(secret_value)


def _to_plain_dict(secret_value: Any) -> Dict[str, Any] | None:
    if isinstance(secret_value, Mapping):
        try:
            return dict(secret_value)
        except Exception:
            return None
    if isinstance(secret_value, str):
        text = secret_value.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return None
            if isinstance(parsed, Mapping):
                try:
                    return dict(parsed)
                except Exception:
                    return None
        return None
    try:
        parsed = dict(secret_value)
    except Exception:
        return None
    return parsed if parsed else None


def _normalize_secret_info(secret_value: Any) -> Dict[str, Any] | None:
    normalized = _to_plain_dict(secret_value)
    if not normalized:
        return None

    if "private_key" in normalized and normalized["private_key"] is not None:
        private_key_text = str(normalized["private_key"]).strip()
        private_key_text = private_key_text.replace("\\n", "\n")
        if private_key_text and not private_key_text.endswith("\n"):
            private_key_text += "\n"
        normalized["private_key"] = private_key_text
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


def _safe_auth_error_message(exc: Exception) -> str:
    text = f"{exc.__class__.__name__}: {exc}".lower()
    if "private key" in text and ("invalid" in text or "parse" in text):
        return "Private key format invalid. Recreate service account JSON key and paste it into Streamlit secrets."
    if "forbidden" in text or "permission" in text or "403" in text:
        return "Service account authenticated but cannot access sheet. Share the Google Sheet with client_email as Viewer."
    if "api has not been used" in text or "disabled" in text:
        return "Google Sheets API/Drive API may not be enabled for this Google Cloud project."
    return "Google auth failed. Check private_key formatting and Google Sheet sharing."


def _infer_auth_stage_from_message(message: str) -> str:
    lowered = message.lower()
    if "missing required keys" in lowered or "not found in streamlit secrets" in lowered:
        return AUTH_STAGE_PARSE
    if "private key format invalid" in lowered:
        return AUTH_STAGE_CREDENTIALS
    if "authenticated but cannot access sheet" in lowered or "may not be enabled" in lowered:
        return AUTH_STAGE_TEST_READ
    return AUTH_STAGE_BUILD_SERVICE


def _service_account_diagnostics(service_account_info: Dict[str, Any] | None, service_account_block_found: bool) -> Dict[str, Any]:
    info = service_account_info or {}
    private_key_text = str(info.get("private_key", "")).strip().replace("\\n", "\n")
    return {
        "service_account_block_found": service_account_block_found,
        "gcp_service_account_found": service_account_block_found,
        "client_email_present": bool(str(info.get("client_email", "")).strip()),
        "private_key_present": bool(private_key_text),
        "private_key_starts_with_begin": private_key_text.startswith("-----BEGIN PRIVATE KEY-----"),
        "private_key_ends_with_end": private_key_text.strip().endswith("-----END PRIVATE KEY-----"),
        "service_account_email": str(info.get("client_email", "")).strip(),
    }


def _build_service_account_credentials(service_account_info: Dict[str, Any]) -> ServiceAccountCredentials:
    normalized = dict(service_account_info)
    if "private_key" in normalized and normalized["private_key"] is not None:
        private_key_text = str(normalized["private_key"]).strip().replace("\\n", "\n")
        if private_key_text and not private_key_text.endswith("\n"):
            private_key_text += "\n"
        normalized["private_key"] = private_key_text
    return ServiceAccountCredentials.from_service_account_info(normalized, scopes=list(READONLY_SCOPES))


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
    diagnostics: Dict[str, Any] = {
        "streamlit_secrets_available": bool(_safe_secrets()),
        **_service_account_diagnostics(service_account_info if service_account_found else None, service_account_found),
        "auth_error_type": "",
        "auth_error_message_safe": "",
        "auth_stage_failed": "",
    }

    if service_account_found:
        if not service_account_info:
            raise ValueError("gcp_service_account block not found in Streamlit Secrets.")
        missing_keys = _validate_service_account_info(service_account_info)
        if missing_keys:
            diagnostics["auth_error_type"] = "ValueError"
            diagnostics["auth_error_message_safe"] = (
                "gcp_service_account block found but is missing required keys: " + ", ".join(missing_keys)
            )
            diagnostics["auth_stage_failed"] = AUTH_STAGE_PARSE
            raise ValueError(diagnostics["auth_error_message_safe"])
        try:
            creds = _build_service_account_credentials(service_account_info)
        except Exception as exc:
            diagnostics["auth_error_type"] = exc.__class__.__name__
            diagnostics["auth_error_message_safe"] = "Private key format invalid. Recreate service account JSON key and paste it into Streamlit secrets."
            diagnostics["auth_stage_failed"] = AUTH_STAGE_CREDENTIALS
            raise ValueError(diagnostics["auth_error_message_safe"]) from exc
        try:
            sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False, static_discovery=False)
        except Exception as exc:
            diagnostics["auth_error_type"] = exc.__class__.__name__
            diagnostics["auth_error_message_safe"] = _safe_auth_error_message(exc)
            diagnostics["auth_stage_failed"] = AUTH_STAGE_BUILD_SERVICE
            raise ValueError(diagnostics["auth_error_message_safe"]) from exc
        diagnostics["auth_mode"] = "Streamlit Secrets"
        return sheets_service, "Streamlit Secrets", diagnostics

    sheets_service, _, _ = build_services()
    diagnostics["auth_mode"] = "Local"
    diagnostics["auth_stage_failed"] = ""
    return sheets_service, "Local", diagnostics


def _read_test_range(sheets_service: object, spreadsheet_id: str) -> None:
    get_sheet_values(sheets_service, spreadsheet_id, "MASTER_SKU!A1:A1")


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
    base_auth_meta = _service_account_diagnostics(service_account_info if service_account_found else None, service_account_found)
    base_auth_meta.update(
        {
            "streamlit_secrets_available": bool(secrets),
            "auth_error_type": "",
            "auth_error_message_safe": "",
            "auth_stage_failed": "",
        }
    )
    payload: Dict[str, Any] = {
        "spreadsheet_id": "",
        "spreadsheet_id_source": "",
        "auth_mode": "Local",
        "dashboard_debug": resolve_dashboard_debug_mode(),
        "streamlit_secrets_available": bool(secrets),
        "service_account_block_found": service_account_found,
        "gcp_service_account_found": service_account_found,
        "client_email_present": bool(service_account_email),
        "service_account_email": service_account_email,
        "private_key_present": bool(str(service_account_info.get("private_key", "")).strip()) if service_account_info else False,
        "private_key_starts_with_begin": bool(str(service_account_info.get("private_key", "")).strip().replace("\\n", "\n").startswith("-----BEGIN PRIVATE KEY-----")) if service_account_info else False,
        "private_key_ends_with_end": bool(str(service_account_info.get("private_key", "")).strip().replace("\\n", "\n").strip().endswith("-----END PRIVATE KEY-----")) if service_account_info else False,
        "auth_error_type": "",
        "auth_error_message_safe": "",
        "auth_stage_failed": "",
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
        message = str(exc)
        stage_failed = _infer_auth_stage_from_message(message)
        auth_meta = {
            **base_auth_meta,
            "auth_error_type": exc.__class__.__name__,
            "auth_error_message_safe": message if stage_failed in {AUTH_STAGE_PARSE, AUTH_STAGE_CREDENTIALS} else _safe_auth_error_message(exc),
            "auth_stage_failed": stage_failed,
        }
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": "Streamlit Secrets" if service_account_found else "Local",
            **auth_meta,
            "load_status": "auth_error",
            "load_message": auth_meta["auth_error_message_safe"],
        }
    except Exception as exc:
        message = _safe_auth_error_message(exc) if service_account_found else "Unable to initialize Google Sheets access."
        load_status = "auth_error" if service_account_found else "missing_secrets"
        auth_meta = {
            **base_auth_meta,
            "auth_error_type": exc.__class__.__name__,
            "auth_error_message_safe": message,
            "auth_stage_failed": AUTH_STAGE_BUILD_SERVICE if service_account_found else "",
        }
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": "Streamlit Secrets" if service_account_found else "Local",
            **auth_meta,
            "load_status": load_status,
            "load_message": message,
        }

    try:
        _read_test_range(sheets_service, spreadsheet_id)
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        auth_meta = {**auth_meta, "auth_stage_failed": AUTH_STAGE_TEST_READ, "auth_error_type": exc.__class__.__name__}
        if status == 403:
            auth_meta["auth_error_message_safe"] = "Service account authenticated but cannot access sheet. Share the Google Sheet with client_email as Viewer."
        elif status in {400, 404}:
            auth_meta["auth_error_message_safe"] = "Google Sheets API/Drive API may not be enabled for this Google Cloud project."
        else:
            auth_meta["auth_error_message_safe"] = _safe_auth_error_message(exc)
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            **auth_meta,
            "load_status": "auth_error",
            "load_message": auth_meta["auth_error_message_safe"],
        }
    except Exception as exc:
        auth_meta = {
            **auth_meta,
            "auth_stage_failed": AUTH_STAGE_TEST_READ,
            "auth_error_type": exc.__class__.__name__,
            "auth_error_message_safe": _safe_auth_error_message(exc),
        }
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            **auth_meta,
            "load_status": "auth_error",
            "load_message": auth_meta["auth_error_message_safe"],
        }

    try:
        available_tabs = get_tab_names(sheets_service, spreadsheet_id)
        frames: Dict[str, pd.DataFrame] = {}
        missing_tabs: list[str] = []
        for tab_name in [
            "LOOKER_FLIPKART_EXECUTIVE_SUMMARY",
            "LOOKER_FLIPKART_FSN_METRICS",
            "LOOKER_FLIPKART_ALERTS",
            "FLIPKART_ACTIVE_TASKS",
            "LOOKER_FLIPKART_ACTIONS",
            "LOOKER_FLIPKART_ADS",
            "LOOKER_FLIPKART_RETURNS",
            "LOOKER_FLIPKART_RETURN_ALL_DETAILS",
            "LOOKER_FLIPKART_CUSTOMER_RETURNS",
            "LOOKER_FLIPKART_COURIER_RETURNS",
            "LOOKER_FLIPKART_RETURN_TYPE_PIVOT",
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
            "FLIPKART_RETURN_ALL_DETAILS",
            "FLIPKART_CUSTOMER_RETURN_COMMENTS",
            "FLIPKART_COURIER_RETURN_COMMENTS",
            "FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY",
            "FLIPKART_COURIER_RETURN_SUMMARY",
            "FLIPKART_RETURN_TYPE_PIVOT",
            "FLIPKART_MISSING_ACTIVE_LISTINGS",
            "FLIPKART_FSN_RUN_COMPARISON",
            "FLIPKART_VISUAL_COMPETITOR_RESULTS",
            "FLIPKART_ORDER_ITEM_EXPLORER",
            "FLIPKART_ORDER_ITEM_MASTER",
            "FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
            "LOOKER_FLIPKART_ORDER_ITEM_EXPLORER",
            "LOOKER_FLIPKART_ORDER_ITEM_MASTER",
            "LOOKER_FLIPKART_ORDER_ITEM_SOURCE_DETAIL",
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
            **auth_meta,
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
                **auth_meta,
                "load_status": "quota_limited",
                "load_message": "Google Sheets quota limit reached. Wait 5 minutes and refresh the dashboard.",
            }
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            **auth_meta,
            "load_status": "sheet_error",
            "load_message": f"Unable to read Google Sheets. {exc.__class__.__name__}: {exc}",
        }
    except Exception as exc:
        return {
            **payload,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_id_source": spreadsheet_id_source,
            "auth_mode": auth_mode,
            **auth_meta,
            "load_status": "sheet_error",
            "load_message": f"Unable to load dashboard data. {exc.__class__.__name__}: {exc}",
        }
