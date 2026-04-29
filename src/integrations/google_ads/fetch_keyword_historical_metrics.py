from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from src.integrations.google_ads.google_ads_config import build_google_ads_client
from src.marketplaces.flipkart.flipkart_utils import format_decimal, normalize_text

GOOGLE_ADS_SOURCE = "Google Ads API"


def _keyword_chunks(keywords: Sequence[str], batch_size: int) -> Iterable[List[str]]:
    deduped: List[str] = []
    seen = set()
    for keyword in keywords:
        keyword_text = normalize_text(keyword)
        if not keyword_text:
            continue
        keyword_key = keyword_text.lower()
        if keyword_key in seen:
            continue
        seen.add(keyword_key)
        deduped.append(keyword_text)
    for index in range(0, len(deduped), max(1, batch_size)):
        yield deduped[index : index + max(1, batch_size)]


def _month_key_from_value(month_value: Any) -> str:
    month_text = normalize_text(getattr(month_value, "name", month_value)).upper()
    month_map = {
        "JANUARY": "Jan",
        "FEBRUARY": "Feb",
        "MARCH": "Mar",
        "APRIL": "Apr",
        "MAY": "May",
        "JUNE": "Jun",
        "JULY": "Jul",
        "AUGUST": "Aug",
        "SEPTEMBER": "Sep",
        "OCTOBER": "Oct",
        "NOVEMBER": "Nov",
        "DECEMBER": "Dec",
    }
    return month_map.get(month_text, "")


def _build_error_row(keyword: str, error_message: str) -> Dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    row = {
        "Keyword": normalize_text(keyword),
        "Close_Variants": "",
        "Avg_Monthly_Searches": "",
        "Competition": "",
        "Competition_Index": "",
        "Low_Top_Page_Bid": "",
        "High_Top_Page_Bid": "",
        "Monthly_Search_Jan": "",
        "Monthly_Search_Feb": "",
        "Monthly_Search_Mar": "",
        "Monthly_Search_Apr": "",
        "Monthly_Search_May": "",
        "Monthly_Search_Jun": "",
        "Monthly_Search_Jul": "",
        "Monthly_Search_Aug": "",
        "Monthly_Search_Sep": "",
        "Monthly_Search_Oct": "",
        "Monthly_Search_Nov": "",
        "Monthly_Search_Dec": "",
        "Source": GOOGLE_ADS_SOURCE,
        "Last_Refreshed": now,
        "Cache_Status": "ERROR",
        "Error_Message": normalize_text(error_message),
    }
    return row


def _extract_metric_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        if float(value).is_integer():
            return str(int(round(float(value))))
        return format_decimal(value, 2)
    text = normalize_text(value)
    return text


def _result_to_row(result: Any) -> Dict[str, Any]:
    metrics = getattr(result, "keyword_metrics", None)
    close_variants = getattr(result, "close_variants", []) or []
    monthly_values = {
        "Jan": "",
        "Feb": "",
        "Mar": "",
        "Apr": "",
        "May": "",
        "Jun": "",
        "Jul": "",
        "Aug": "",
        "Sep": "",
        "Oct": "",
        "Nov": "",
        "Dec": "",
    }
    if metrics is not None:
        for month in getattr(metrics, "monthly_search_volumes", []) or []:
            month_key = _month_key_from_value(getattr(month, "month", ""))
            if month_key:
                monthly_values[month_key] = _extract_metric_value(getattr(month, "monthly_searches", ""))

        competition = getattr(metrics, "competition", "")
        competition_text = normalize_text(getattr(competition, "name", competition))
        low_bid = getattr(metrics, "low_top_of_page_bid_micros", "")
        high_bid = getattr(metrics, "high_top_of_page_bid_micros", "")
        row = {
            "Keyword": normalize_text(getattr(result, "text", "")),
            "Close_Variants": "; ".join(sorted({normalize_text(value) for value in close_variants if normalize_text(value)})),
            "Avg_Monthly_Searches": _extract_metric_value(getattr(metrics, "avg_monthly_searches", "")),
            "Competition": competition_text,
            "Competition_Index": _extract_metric_value(getattr(metrics, "competition_index", "")),
            "Low_Top_Page_Bid": format_decimal(float(low_bid) / 1_000_000.0, 2) if low_bid not in {"", None} else "",
            "High_Top_Page_Bid": format_decimal(float(high_bid) / 1_000_000.0, 2) if high_bid not in {"", None} else "",
            "Monthly_Search_Jan": monthly_values["Jan"],
            "Monthly_Search_Feb": monthly_values["Feb"],
            "Monthly_Search_Mar": monthly_values["Mar"],
            "Monthly_Search_Apr": monthly_values["Apr"],
            "Monthly_Search_May": monthly_values["May"],
            "Monthly_Search_Jun": monthly_values["Jun"],
            "Monthly_Search_Jul": monthly_values["Jul"],
            "Monthly_Search_Aug": monthly_values["Aug"],
            "Monthly_Search_Sep": monthly_values["Sep"],
            "Monthly_Search_Oct": monthly_values["Oct"],
            "Monthly_Search_Nov": monthly_values["Nov"],
            "Monthly_Search_Dec": monthly_values["Dec"],
            "Source": GOOGLE_ADS_SOURCE,
            "Last_Refreshed": datetime.now().isoformat(timespec="seconds"),
            "Cache_Status": "SUCCESS",
            "Error_Message": "",
        }
        return row
    return _build_error_row(getattr(result, "text", ""), "No historical metrics returned")


def fetch_keyword_historical_metrics(
    keywords: Sequence[str],
    geo_target_id: str,
    language_id: str,
    customer_id: str,
    batch_size: int = 20,
) -> List[Dict[str, Any]]:
    client, config_payload = build_google_ads_client()
    if client is None:
        raise RuntimeError(config_payload.get("message", f"Google Ads config status: {config_payload.get('status', 'ERROR')}"))

    google_ads_service = client.get_service("GoogleAdsService")
    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

    results: List[Dict[str, Any]] = []
    batch_delay = 2

    for batch in _keyword_chunks(keywords, batch_size):
        request = client.get_type("GenerateKeywordHistoricalMetricsRequest")
        request.customer_id = str(customer_id)
        request.keywords.extend(batch)
        request.geo_target_constants.append(google_ads_service.geo_target_constant_path(str(geo_target_id)))
        request.language = google_ads_service.language_constant_path(str(language_id))
        request.keyword_plan_network = client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH

        try:
            response = keyword_plan_idea_service.generate_keyword_historical_metrics(request=request)
            batch_rows = [_result_to_row(result) for result in getattr(response, "results", []) or []]
            if not batch_rows:
                batch_rows = [_build_error_row(keyword, "No historical metrics returned") for keyword in batch]
            results.extend(batch_rows)
        except Exception as exc:
            message = normalize_text(str(exc)) or exc.__class__.__name__
            for keyword in batch:
                results.append(_build_error_row(keyword, message))
        time.sleep(batch_delay)

    return results

