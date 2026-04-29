from __future__ import annotations

import json
from typing import Any, Dict, List

from src.integrations.google_ads.google_ads_config import build_google_ads_client, load_google_ads_config, normalize_customer_id

TEST_KEYWORDS = [
    "rice light",
    "led flood light",
    "rope light",
    "strip light",
    "gate light",
]

DEFAULT_GEO_TARGET_ID = "2356"
DEFAULT_LANGUAGE_ID = "1000"
MASKED_TOKEN = "************"


def _collect_error_text(exc: Exception) -> str:
    parts: List[str] = []
    failure = getattr(exc, "failure", None)
    if failure is not None:
        for error in getattr(failure, "errors", []) or []:
            parts.append(str(getattr(error, "message", "")))
            error_code = getattr(error, "error_code", None)
            if error_code is not None:
                parts.append(str(getattr(error_code, "WhichOneof", lambda *_: "")("error_code")))
                parts.append(str(error_code))
    parts.append(str(exc))
    return " | ".join(part for part in parts if part)


def _classify_error(exc: Exception) -> tuple[str, str]:
    message = _collect_error_text(exc)
    message_lower = message.lower()
    if any(token in message_lower for token in ("too many requests", "quota", "rate limit", "resource_exhausted", "exceeded")):
        return "RATE_LIMITED", message
    if any(token in message_lower for token in ("account has been canceled", "account is closed", "customer account closed", "canceled account", "closed account", "account disabled", "account deactivated")):
        return "CUSTOMER_ACCOUNT_CLOSED", message
    if any(token in message_lower for token in ("developer token", "keyword planner", "access level", "basic access", "not approved", "planning service", "developer_token_not_approved")):
        return "API_ACCESS_NOT_READY", message
    if any(token in message_lower for token in ("user_permission_denied", "caller does not have permission", "doesn't have permission", "does not have permission", "login-customer-id", "hierarchy", "linked under manager")):
        return "CUSTOMER_HIERARCHY_ERROR", message
    if any(token in message_lower for token in ("invalid_grant", "unauthenticated", "authentication", "refresh token", "client secret", "client id")):
        return "AUTH_ERROR", message
    return "ERROR", message


def _next_action_for_status(status: str) -> str:
    if status == "CUSTOMER_ACCOUNT_CLOSED":
        return "Use active account 9985985021"
    if status == "API_ACCESS_NOT_READY":
        return "Apply for Basic Access in API Center. Test Account Access cannot access production accounts."
    if status == "CUSTOMER_HIERARCHY_ERROR":
        return "Confirm 9985985021 is linked and accessible under manager 3821874145, and OAuth user has access."
    if status == "AUTH_ERROR":
        return "Regenerate the refresh token with Google Ads scope."
    if status == "RATE_LIMITED":
        return "Wait and retry later with smaller batches or cached refreshes."
    return "Review the API error details and fix the Google Ads config or account permissions."


def _resolve_geo_target_id(config: Dict[str, Any]) -> str:
    raw = normalize_customer_id(config.get("geo_target_id", "")) or normalize_customer_id(config.get("default_geo_target_id", ""))
    return raw or DEFAULT_GEO_TARGET_ID


def _resolve_language_id(config: Dict[str, Any]) -> str:
    raw = normalize_customer_id(config.get("language_id", "")) or normalize_customer_id(config.get("default_language_id", ""))
    return raw or DEFAULT_LANGUAGE_ID


def run_google_ads_api_access_test() -> Dict[str, Any]:
    config_payload = load_google_ads_config()
    config_path = config_payload.get("config_path", "")
    config_exists = bool(config_payload.get("config_exists"))
    oauth_loaded = False
    developer_token_present = False
    developer_token_masked = ""
    customer_id = ""
    login_customer_id = ""

    if config_payload["status"] == "NEEDS_CREDENTIALS":
        return {
            "status": "NEEDS_CREDENTIALS",
            "access_ready": False,
            "config_file_found": False,
            "config_path": config_path,
            "oauth_loaded": False,
            "customer_id_used": "",
            "login_customer_id_used": "",
            "developer_token_present": False,
            "developer_token_masked": "",
            "test_keywords_sent": TEST_KEYWORDS,
            "results_returned": 0,
            "error_type": "MISSING_CREDENTIALS",
            "message": f"Missing Google Ads credentials file: {config_payload['config_path']}",
            "next_action": "Create credentials/google_ads.yaml from config/google_ads_template.yaml",
        }
    if config_payload["status"] == "INVALID_CONFIG":
        return {
            "status": "ERROR",
            "access_ready": False,
            "config_file_found": config_exists,
            "config_path": config_path,
            "oauth_loaded": False,
            "customer_id_used": "",
            "login_customer_id_used": "",
            "developer_token_present": False,
            "developer_token_masked": "",
            "test_keywords_sent": TEST_KEYWORDS,
            "results_returned": 0,
            "error_type": "INVALID_CONFIG",
            "message": f"Invalid Google Ads config at {config_payload['config_path']}. Missing keys: {', '.join(config_payload.get('missing_keys', []))}",
            "next_action": "Fill in all required Google Ads credentials and rerun the test",
        }

    client, client_payload = build_google_ads_client()
    if client is None:
        return {
            "status": "ERROR",
            "access_ready": False,
            "config_file_found": config_exists,
            "config_path": config_path,
            "oauth_loaded": False,
            "customer_id_used": "",
            "login_customer_id_used": "",
            "developer_token_present": False,
            "developer_token_masked": "",
            "test_keywords_sent": TEST_KEYWORDS,
            "results_returned": 0,
            "error_type": client_payload.get("error_type", "CONFIG_ERROR"),
            "message": client_payload.get("message", "Unable to initialize Google Ads client"),
            "next_action": "Fix the Google Ads client configuration and rerun the test",
        }

    oauth_loaded = True
    customer_id = normalize_customer_id(client_payload["config"]["customer_id"])
    login_customer_id = normalize_customer_id(client_payload["config"].get("login_customer_id", ""))
    developer_token_present = bool(normalize_customer_id(client_payload["config"].get("developer_token", "")) or client_payload["config"].get("developer_token"))
    developer_token_masked = MASKED_TOKEN if developer_token_present else ""
    geo_target_id = _resolve_geo_target_id(client_payload["config"])
    language_id = _resolve_language_id(client_payload["config"])

    google_ads_service = client.get_service("GoogleAdsService")
    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

    def run_request(target_customer_id: str) -> Any:
        request = client.get_type("GenerateKeywordHistoricalMetricsRequest")
        request.customer_id = target_customer_id
        request.keywords.extend(TEST_KEYWORDS)
        request.geo_target_constants.append(google_ads_service.geo_target_constant_path(geo_target_id))
        request.language = google_ads_service.language_constant_path(language_id)
        request.keyword_plan_network = client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
        return keyword_plan_idea_service.generate_keyword_historical_metrics(request=request)

    attempted_customer_id = customer_id
    try:
        response = run_request(attempted_customer_id)
        results = list(getattr(response, "results", []) or [])
        return {
            "status": "SUCCESS",
            "access_ready": True,
            "config_file_found": config_exists,
            "config_path": config_path,
            "oauth_loaded": oauth_loaded,
            "customer_id_used": attempted_customer_id,
            "login_customer_id_used": login_customer_id,
            "developer_token_present": developer_token_present,
            "developer_token_masked": developer_token_masked,
            "test_keywords_sent": TEST_KEYWORDS,
            "results_returned": len(results),
            "error_type": "",
            "message": "Google Ads Keyword Planner access is working",
            "next_action": "Run the keyword metrics refresh for a small cache test",
        }
    except Exception as exc:
        message = _collect_error_text(exc)
        status, message = _classify_error(exc)
        return {
            "status": status,
            "access_ready": False,
            "config_file_found": config_exists,
            "config_path": config_path,
            "oauth_loaded": oauth_loaded,
            "customer_id_used": attempted_customer_id,
            "login_customer_id_used": login_customer_id,
            "developer_token_present": developer_token_present,
            "developer_token_masked": developer_token_masked,
            "test_keywords_sent": TEST_KEYWORDS,
            "results_returned": 0,
            "error_type": status if status != "ERROR" else exc.__class__.__name__,
            "message": message,
            "next_action": _next_action_for_status(status),
        }


def main() -> None:
    try:
        print(json.dumps(run_google_ads_api_access_test(), indent=2, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover - defensive fallback
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "access_ready": False,
                    "config_file_found": config_exists,
                    "config_path": config_path,
                    "oauth_loaded": oauth_loaded,
                    "customer_id_used": "",
                    "login_customer_id_used": "",
                    "developer_token_present": developer_token_present,
                    "developer_token_masked": developer_token_masked,
                    "test_keywords_sent": TEST_KEYWORDS,
                    "results_returned": 0,
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "next_action": "Fix the unexpected error and rerun the test",
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
