from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "credentials" / "visual_search.env"
CONFIG_ENV_VAR = "VISUAL_SEARCH_CONFIG_PATH"
REQUIRED_KEYS = ("VISUAL_SEARCH_PROVIDER", "SERPAPI_API_KEY", "VISUAL_SEARCH_COUNTRY", "VISUAL_SEARCH_LANGUAGE")
PLACEHOLDER_HINTS = ("YOUR_", "OPTIONAL_", "REPLACE_", "TODO", "CHANGE_ME")


def resolve_visual_search_config_path(config_path: str | Path | None = None) -> Path:
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


def parse_simple_env(text: str) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = _strip_inline_comment(raw_line).strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([A-Za-z0-9_]+)\s*=\s*(.*)$", line)
        if not match:
            continue
        key = match.group(1).strip()
        value = _unquote(match.group(2).strip())
        if value.lower() in {"null", "none", "~"}:
            value = ""
        parsed[key] = value
    return parsed


def looks_like_placeholder(value: Any) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return True
    return any(hint in text for hint in PLACEHOLDER_HINTS)


def load_visual_search_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    path = resolve_visual_search_config_path(config_path)
    payload: Dict[str, Any] = {
        "status": "NEEDS_CREDENTIALS",
        "config_path": str(path),
        "config_exists": path.exists(),
        "missing_keys": list(REQUIRED_KEYS),
        "config": None,
    }

    if not path.exists():
        return payload

    raw_config = parse_simple_env(path.read_text(encoding="utf-8"))
    sanitized: Dict[str, str] = {}
    missing_keys = []

    for key in REQUIRED_KEYS:
        value = str(raw_config.get(key, "")).strip()
        if not value or looks_like_placeholder(value):
            missing_keys.append(key)
            continue
        sanitized[key] = value

    payload["config_exists"] = True
    payload["missing_keys"] = missing_keys
    if missing_keys:
        payload["status"] = "NEEDS_CREDENTIALS" if "SERPAPI_API_KEY" in missing_keys else "INVALID_CONFIG"
        return payload

    payload["status"] = "SUCCESS"
    payload["config"] = sanitized
    return payload
