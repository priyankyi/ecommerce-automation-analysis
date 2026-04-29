from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "credentials" / "google_ads.yaml"
CONFIG_ENV_VAR = "GOOGLE_ADS_CONFIG_PATH"
REQUIRED_KEYS = ("developer_token", "client_id", "client_secret", "refresh_token", "customer_id")
PLACEHOLDER_HINTS = ("YOUR_", "OPTIONAL_", "REPLACE_", "TODO", "CHANGE_ME")


def resolve_google_ads_config_path(config_path: str | Path | None = None) -> Path:
    if config_path is not None:
        return Path(config_path).expanduser().resolve()
    env_path = os.environ.get(CONFIG_ENV_VAR, "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    return DEFAULT_CONFIG_PATH.resolve()


def _strip_inline_comment(line: str) -> str:
    in_single = False
    in_double = False
    for index, char in enumerate(line):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            return line[:index].rstrip()
    return line.rstrip()


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and ((value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'"))):
        return value[1:-1]
    return value


def parse_simple_yaml(text: str) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = _strip_inline_comment(raw_line).strip()
        if not line or line.startswith("---"):
            continue
        match = re.match(r"^([A-Za-z0-9_]+)\s*:\s*(.*)$", line)
        if not match:
            continue
        key = match.group(1).strip()
        value = _unquote(match.group(2).strip())
        if value.lower() in {"null", "none", "~"}:
            value = ""
        parsed[key] = value
    return parsed


def normalize_customer_id(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def looks_like_placeholder(value: Any) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return True
    return any(hint in text for hint in PLACEHOLDER_HINTS)


def load_google_ads_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    path = resolve_google_ads_config_path(config_path)
    payload: Dict[str, Any] = {
        "status": "NEEDS_CREDENTIALS",
        "config_path": str(path),
        "config_exists": path.exists(),
        "missing_keys": list(REQUIRED_KEYS),
        "config": None,
    }

    if not path.exists():
        return payload

    raw_config = parse_simple_yaml(path.read_text(encoding="utf-8"))
    sanitized: Dict[str, str] = {}
    missing_keys = []

    for key in REQUIRED_KEYS:
        value = raw_config.get(key, "")
        if key == "customer_id":
            value = normalize_customer_id(value)
        value = str(value).strip()
        if not value or looks_like_placeholder(value):
            missing_keys.append(key)
            continue
        sanitized[key] = value

    login_customer_id = normalize_customer_id(raw_config.get("login_customer_id", ""))
    if login_customer_id and not looks_like_placeholder(login_customer_id):
        sanitized["login_customer_id"] = login_customer_id

    payload["config_exists"] = True
    payload["missing_keys"] = missing_keys
    if missing_keys:
        payload["status"] = "INVALID_CONFIG"
        return payload

    payload["status"] = "SUCCESS"
    payload["config"] = sanitized
    return payload


def build_google_ads_client(config_path: str | Path | None = None) -> Tuple[Optional[Any], Dict[str, Any]]:
    config_payload = load_google_ads_config(config_path)
    if config_payload["status"] != "SUCCESS":
        return None, config_payload

    try:
        from google.ads.googleads.client import GoogleAdsClient
    except Exception as exc:  # pragma: no cover - dependency failure
        return None, {
            "status": "ERROR",
            "config_path": config_payload["config_path"],
            "config_exists": True,
            "missing_keys": [],
            "config": None,
            "error_type": exc.__class__.__name__,
            "message": str(exc),
        }

    client_config = dict(config_payload["config"])
    client_config.setdefault("use_proto_plus", True)
    client = GoogleAdsClient.load_from_dict(client_config)
    return client, config_payload
