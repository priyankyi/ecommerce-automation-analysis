from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.integrations.visual_search.visual_search_config import load_visual_search_config


def test_visual_search_access() -> Dict[str, Any]:
    config_payload = load_visual_search_config()
    config = config_payload.get("config") or {}
    api_key_present = bool(str(config.get("SERPAPI_API_KEY", "")).strip())
    status = "READY" if api_key_present else "NEEDS_CREDENTIALS"
    payload = {
        "status": status,
        "api_key_present": api_key_present,
        "live_test_called": False,
        "config_path": config_payload["config_path"],
        "config_exists": config_payload["config_exists"],
        "missing_keys": config_payload.get("missing_keys", []),
        "provider": config.get("VISUAL_SEARCH_PROVIDER", ""),
        "country": config.get("VISUAL_SEARCH_COUNTRY", ""),
        "language": config.get("VISUAL_SEARCH_LANGUAGE", ""),
        "monthly_limit": config.get("VISUAL_SEARCH_MONTHLY_LIMIT", ""),
        "safe_monthly_limit": config.get("VISUAL_SEARCH_SAFE_MONTHLY_LIMIT", ""),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return payload


def main() -> None:
    try:
        test_visual_search_access()
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error_type": exc.__class__.__name__, "message": str(exc)}, indent=2, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
