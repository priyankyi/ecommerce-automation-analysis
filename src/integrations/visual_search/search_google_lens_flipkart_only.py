from __future__ import annotations

import csv
import hashlib
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .visual_search_config import load_visual_search_config

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "marketplaces" / "flipkart"
CACHE_PATH = OUTPUT_DIR / "flipkart_visual_search_cache.json"
USAGE_LOG_PATH = PROJECT_ROOT / "data" / "logs" / "marketplaces" / "flipkart" / "visual_search_usage_log.csv"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PROVIDER = "SERPAPI_GOOGLE_LENS"
FLIPKART_DOMAIN_HINT = "flipkart.com"


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def clean_number_text(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return ""
    return match.group(0)


def parse_float(value: Any) -> float:
    number_text = clean_number_text(value)
    try:
        return float(number_text) if number_text else 0.0
    except ValueError:
        return 0.0


def load_cache(path: Path = CACHE_PATH) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def load_usage_log(path: Path = USAGE_LOG_PATH) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def append_usage_log_row(row: Dict[str, Any], path: Path = USAGE_LOG_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    headers = ["timestamp", "month", "provider", "fsn", "image_url_hash", "api_called", "status", "results_returned"]
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow({header: row.get(header, "") for header in headers})


def current_month_key(moment: Optional[datetime] = None) -> str:
    return (moment or datetime.now()).strftime("%Y-%m")


def count_usage_calls_for_month(rows: List[Dict[str, str]], month: str, provider: str) -> int:
    provider_norm = normalize_text(provider).upper()
    return sum(
        1
        for row in rows
        if normalize_text(row.get("month", "")) == month
        and normalize_text(row.get("provider", "")).upper() == provider_norm
        and normalize_text(row.get("api_called", "")).lower() in {"true", "1", "yes", "y"}
    )


def month_image_hash_seen(rows: List[Dict[str, str]], month: str, provider: str, fsn: str, image_url_hash: str) -> bool:
    provider_norm = normalize_text(provider).upper()
    fsn_norm = normalize_text(fsn).upper()
    hash_norm = normalize_text(image_url_hash)
    if not fsn_norm or not hash_norm:
        return False
    return any(
        normalize_text(row.get("month", "")) == month
        and normalize_text(row.get("provider", "")).upper() == provider_norm
        and normalize_text(row.get("fsn", "")).upper() == fsn_norm
        and normalize_text(row.get("image_url_hash", "")) == hash_norm
        and normalize_text(row.get("api_called", "")).lower() in {"true", "1", "yes", "y"}
        for row in rows
    )


def save_cache(cache: Dict[str, Any], path: Path = CACHE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")


def build_cache_key(provider: str, image_url: str, query: str, country: str, language: str) -> str:
    raw = "|".join([provider, normalize_text(image_url), normalize_text(query), normalize_text(country).lower(), normalize_text(language).lower()])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def hash_image_url(image_url: str) -> str:
    normalized = normalize_text(image_url)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def competitor_link_is_flipkart(link: str) -> bool:
    return FLIPKART_DOMAIN_HINT in normalize_text(link).lower()


def _first_nonblank(*values: Any) -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return ""


def _extract_competitor_price(entry: Dict[str, Any]) -> str:
    for key in ("price", "extracted_price", "current_price", "listed_price"):
        candidate = clean_number_text(entry.get(key, ""))
        if candidate:
            return candidate
    return ""


def _extract_competitor_rating(entry: Dict[str, Any]) -> str:
    for key in ("rating", "stars", "score"):
        candidate = clean_number_text(entry.get(key, ""))
        if candidate:
            return candidate
    return ""


def _extract_competitor_reviews(entry: Dict[str, Any]) -> str:
    for key in ("reviews", "reviews_count", "review_count", "total_reviews"):
        candidate = clean_number_text(entry.get(key, ""))
        if candidate:
            return candidate
    return ""


def _extract_stock(entry: Dict[str, Any]) -> str:
    for key in ("in_stock", "availability", "stock_status", "is_in_stock"):
        value = normalize_text(entry.get(key, ""))
        if value:
            normalized = value.lower()
            if normalized in {"true", "yes", "available", "in stock", "instock"}:
                return "Yes"
            if normalized in {"false", "no", "unavailable", "out of stock", "sold out"}:
                return "No"
            return value
    if _extract_competitor_price(entry):
        return "Yes"
    return ""


def _normalize_result(entry: Dict[str, Any], raw_position: int, source: str) -> Dict[str, Any]:
    link = _first_nonblank(entry.get("link"), entry.get("url"), entry.get("product_link"), entry.get("source"))
    if not competitor_link_is_flipkart(link):
        return {}
    title = _first_nonblank(entry.get("title"), entry.get("product_title"), entry.get("name"), entry.get("snippet"))
    image = _first_nonblank(entry.get("thumbnail"), entry.get("image"), entry.get("thumbnail_link"), entry.get("product_image"))
    price = _extract_competitor_price(entry)
    rating = _extract_competitor_rating(entry)
    reviews = _extract_competitor_reviews(entry)
    in_stock = _extract_stock(entry)
    comparison_confidence = "Low"
    if title and price and image:
        comparison_confidence = "High"
    elif title and price:
        comparison_confidence = "Medium"
    return {
        "Competitor_Title": title,
        "Competitor_Link": link,
        "Competitor_Source": _first_nonblank(entry.get("source"), entry.get("displayed_link"), entry.get("domain"), "Flipkart"),
        "Competitor_Image": image,
        "Competitor_Price": price,
        "Competitor_Rating": rating,
        "Competitor_Reviews": reviews,
        "Competitor_In_Stock": in_stock,
        "Visual_Search_Source": source,
        "Raw_Position": str(raw_position),
        "Comparable_YN": "Yes" if title and price else "No",
        "Comparison_Confidence": comparison_confidence,
    }


def _collect_entries(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    for key in ("visual_matches", "shopping_results", "organic_results", "inline_images", "products"):
        entries = payload.get(key, [])
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, dict):
                    collected.append(entry)
    return collected


def _build_request_url(provider_config: Dict[str, str], image_url: str, query: str) -> str:
    provider = normalize_text(provider_config.get("VISUAL_SEARCH_PROVIDER", DEFAULT_PROVIDER)).upper()
    api_key = normalize_text(provider_config.get("SERPAPI_API_KEY", ""))
    country = normalize_text(provider_config.get("VISUAL_SEARCH_COUNTRY", "in")).lower() or "in"
    language = normalize_text(provider_config.get("VISUAL_SEARCH_LANGUAGE", "en")).lower() or "en"
    params: Dict[str, str] = {
        "api_key": api_key,
        "country": country,
        "hl": language,
    }
    if image_url:
        params["engine"] = "google_lens"
        params["url"] = image_url
        source = provider
    else:
        params["engine"] = "google"
        params["q"] = query
        source = f"{provider}_QUERY_FALLBACK"
    return f"https://serpapi.com/search.json?{urllib.parse.urlencode(params)}", source


def _fetch_json(url: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def search_flipkart_only(
    image_url: str = "",
    query: str = "",
    config_path: str | Path | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    use_cache: bool = True,
) -> Dict[str, Any]:
    image_url = normalize_text(image_url)
    query = normalize_text(query)

    if not image_url and not query:
        return {
            "status": "NO_SEARCH_INPUT",
            "api_called": False,
            "cache_hit": False,
            "results": [],
            "message": "No image_url or query was provided.",
            "provider": DEFAULT_PROVIDER,
            "image_url": "",
            "query": "",
        }

    config_payload = load_visual_search_config(config_path)
    provider_config = config_payload.get("config") or {}
    provider = normalize_text(provider_config.get("VISUAL_SEARCH_PROVIDER", DEFAULT_PROVIDER)).upper() or DEFAULT_PROVIDER
    country = normalize_text(provider_config.get("VISUAL_SEARCH_COUNTRY", "in")).lower() or "in"
    language = normalize_text(provider_config.get("VISUAL_SEARCH_LANGUAGE", "en")).lower() or "en"
    cache_key = build_cache_key(provider, image_url, query, country, language)
    cache = load_cache() if use_cache else {}
    if use_cache and cache_key in cache:
        cached_payload = dict(cache[cache_key])
        cached_payload.update(
            {
                "status": cached_payload.get("status", "SUCCESS"),
                "api_called": False,
                "cache_hit": True,
                "provider": provider,
                "image_url": image_url,
                "query": query,
                "cache_key": cache_key,
            }
        )
        return cached_payload

    if config_payload["status"] != "SUCCESS":
        return {
            "status": "NEEDS_CREDENTIALS",
            "api_called": False,
            "cache_hit": False,
            "results": [],
            "provider": provider,
            "image_url": image_url,
            "query": query,
            "config_path": config_payload["config_path"],
            "missing_keys": config_payload.get("missing_keys", []),
            "message": "Visual search credentials are missing.",
        }

    request_url, source = _build_request_url(provider_config, image_url, query)
    try:
        api_payload = _fetch_json(request_url, timeout_seconds=timeout_seconds)
    except urllib.error.HTTPError as exc:
        return {
            "status": "ERROR",
            "api_called": True,
            "cache_hit": False,
            "results": [],
            "provider": provider,
            "image_url": image_url,
            "query": query,
            "error_type": exc.__class__.__name__,
            "message": f"HTTP {getattr(exc, 'code', '')}",
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "api_called": True,
            "cache_hit": False,
            "results": [],
            "provider": provider,
            "image_url": image_url,
            "query": query,
            "error_type": exc.__class__.__name__,
            "message": str(exc),
        }

    normalized_results: List[Dict[str, Any]] = []
    for raw_position, entry in enumerate(_collect_entries(api_payload), start=1):
        normalized = _normalize_result(entry, raw_position, source)
        if normalized:
            normalized_results.append(normalized)

    status = "SUCCESS" if normalized_results else "NO_FLIPKART_MATCH_FOUND"
    result_payload = {
        "status": status,
        "api_called": True,
        "cache_hit": False,
        "results": normalized_results,
        "provider": provider,
        "image_url": image_url,
        "query": query,
        "cache_key": cache_key,
        "message": "Flipkart-only results filtered from SerpApi payload." if normalized_results else "No Flipkart results found.",
        "raw_result_count": len(_collect_entries(api_payload)),
    }
    if use_cache:
        cache[cache_key] = result_payload
        save_cache(cache)
    return result_payload
