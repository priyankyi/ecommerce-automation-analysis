from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.auth_google import build_services
from src.marketplaces.flipkart.flipkart_sheet_helpers import clear_tab, ensure_tab, freeze_and_format, load_json, read_table, tab_exists, write_rows
from src.marketplaces.flipkart.flipkart_utils import LOG_DIR, OUTPUT_DIR, append_csv_log, clean_fsn, ensure_directories, format_decimal, normalize_text, now_iso, parse_float

SPREADSHEET_META_PATH = PROJECT_ROOT / "data" / "output" / "master_sku_sheet.json"
LOG_PATH = LOG_DIR / "flipkart_competitor_price_intelligence_log.csv"
LOCAL_PRICE_INTELLIGENCE_PATH = OUTPUT_DIR / "flipkart_competitor_price_intelligence.csv"
LOCAL_LOOKER_PATH = OUTPUT_DIR / "looker_flipkart_competitor_intelligence.csv"

QUEUE_TAB = "FLIPKART_COMPETITOR_SEARCH_QUEUE"
RESULTS_TAB = "FLIPKART_VISUAL_COMPETITOR_RESULTS"
ADS_PLANNER_TAB = "FLIPKART_ADS_PLANNER"
SKU_ANALYSIS_TAB = "FLIPKART_SKU_ANALYSIS"
PRICE_INTELLIGENCE_TAB = "FLIPKART_COMPETITOR_PRICE_INTELLIGENCE"
LOOKER_TAB = "LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE"

OUTPUT_HEADERS = [
    "Run_ID",
    "FSN",
    "SKU_ID",
    "Product_Title",
    "Final_Ads_Decision",
    "Our_Selling_Price",
    "Our_Pack_Count",
    "Our_Unit_Price",
    "Our_Final_Profit_Margin",
    "Our_Return_Rate",
    "Comparable_Competitor_Count",
    "Lowest_Comparable_Competitor_Price",
    "Median_Comparable_Competitor_Price",
    "Lowest_Comparable_Competitor_Unit_Price",
    "Median_Comparable_Competitor_Unit_Price",
    "Price_Gap_Percent",
    "Best_Competitor_Rating",
    "Best_Competitor_Reviews",
    "Competition_Risk_Score",
    "Competition_Risk_Level",
    "Competitor_Insight",
    "Suggested_Action",
    "Confidence",
    "Last_Updated",
]

LOOKER_HEADERS = OUTPUT_HEADERS
VALID_RISK_LEVELS = {"Low", "Medium", "High", "Critical", "Not Enough Data"}


def retry(func, attempts: int = 4):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status != 503 or attempt == attempts:
                raise
            import time

            time.sleep(delay)
            delay *= 2


def write_local_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def build_index(rows: Sequence[Dict[str, str]], field_name: str = "FSN") -> Dict[str, Dict[str, str]]:
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fsn = clean_fsn(row.get(field_name, ""))
        if fsn and fsn not in indexed:
            indexed[fsn] = dict(row)
    return indexed


def build_grouped(rows: Sequence[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        fsn = clean_fsn(row.get("FSN", ""))
        if not fsn:
            continue
        grouped.setdefault(fsn, []).append(dict(row))
    return grouped


def median_or_empty(values: Sequence[float]) -> str:
    clean_values = [value for value in values if value > 0]
    if not clean_values:
        return ""
    return format_decimal(statistics.median(clean_values), 2)


def min_or_empty(values: Sequence[float]) -> str:
    clean_values = [value for value in values if value > 0]
    if not clean_values:
        return ""
    return format_decimal(min(clean_values), 2)


def level_from_gap(gap: float, profit_margin: float, competitor_cheaper: bool, return_rate: float) -> Tuple[str, float]:
    risk_score = 0.0
    if competitor_cheaper:
        if gap >= 0.35:
            level = "Critical"
            risk_score += 80
        elif gap >= 0.25:
            level = "High"
            risk_score += 65
        elif gap >= 0.10:
            level = "Medium"
            risk_score += 40
        else:
            level = "Low"
            risk_score += 20
    else:
        level = "Low" if gap <= 0 else "Medium"
        risk_score += 15 if gap <= 0 else 25

    if profit_margin < 0.10 and competitor_cheaper:
        level = "Critical"
        risk_score += 20
    elif profit_margin < 0.15 and level in {"Low", "Medium"}:
        level = "Medium"
        risk_score += 10

    if return_rate >= 0.20 and level in {"Low", "Medium"}:
        level = "High"
        risk_score += 15
    if return_rate >= 0.35:
        level = "Critical"
        risk_score += 25

    risk_score = max(0.0, min(100.0, risk_score))
    return level, risk_score


def action_from_level(level: str, competitor_cheaper: bool, final_decision: str, no_data: bool) -> str:
    decision_norm = normalize_text(final_decision)
    if no_data:
        return "Need Competitor Data"
    if level == "Critical":
        return "Do Not Scale Ads"
    if level == "High":
        return "Improve Price Before Ads" if competitor_cheaper else "Manual Review Required"
    if level == "Medium":
        return "Test Ads Carefully" if decision_norm == "Test Ads" else "Improve Price Before Ads"
    if decision_norm == "Scale Ads":
        return "Scale Ads Allowed"
    if decision_norm == "Test Ads":
        return "Test Ads Carefully"
    return "Manual Review Required"


def confidence_from_data(comp_count: int, has_pack_counts: bool, has_unit_prices: bool) -> str:
    if comp_count <= 0:
        return "Low"
    if comp_count >= 2 and has_pack_counts and has_unit_prices:
        return "High"
    if has_pack_counts or has_unit_prices:
        return "Medium"
    return "Low"


def build_price_intelligence_rows(
    queue_rows: Sequence[Dict[str, str]],
    results_rows: Sequence[Dict[str, str]],
    planner_rows: Sequence[Dict[str, str]],
    analysis_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, Any]]:
    queue_index = build_index(queue_rows)
    planner_index = build_index(planner_rows)
    analysis_index = build_index(analysis_rows)
    results_grouped = build_grouped(results_rows)

    intelligence_rows: List[Dict[str, Any]] = []
    for fsn, queue_row in queue_index.items():
        planner_row = planner_index.get(fsn, {})
        analysis_row = analysis_index.get(fsn, {})
        competitor_rows = [row for row in results_grouped.get(fsn, []) if normalize_text(row.get("Competitor_Link", "")).lower().find("flipkart.com") >= 0 and normalize_text(row.get("Comparable_YN", "")).upper() == "YES"]
        competitor_prices = [parse_float(row.get("Competitor_Price", "")) for row in competitor_rows if parse_float(row.get("Competitor_Price", "")) > 0]
        competitor_unit_prices = [parse_float(row.get("Competitor_Unit_Price", "")) for row in competitor_rows if parse_float(row.get("Competitor_Unit_Price", "")) > 0]
        competitor_ratings = [parse_float(row.get("Competitor_Rating", "")) for row in competitor_rows if parse_float(row.get("Competitor_Rating", "")) > 0]
        competitor_reviews = [parse_float(row.get("Competitor_Reviews", "")) for row in competitor_rows if parse_float(row.get("Competitor_Reviews", "")) > 0]
        comparable_count = len(competitor_rows)

        our_pack_count = normalize_text(queue_row.get("Our_Pack_Count", ""))
        our_unit_price_text = normalize_text(queue_row.get("Our_Unit_Price", ""))
        our_unit_price = parse_float(our_unit_price_text)
        median_comp_price = parse_float(median_or_empty(competitor_prices))
        median_comp_unit = parse_float(median_or_empty(competitor_unit_prices))
        lowest_comp_price = parse_float(min_or_empty(competitor_prices))
        lowest_comp_unit = parse_float(min_or_empty(competitor_unit_prices))
        our_margin = parse_float(queue_row.get("Our_Final_Profit_Margin", "") or analysis_row.get("Final_Profit_Margin", ""))
        our_return_rate = parse_float(queue_row.get("Our_Return_Rate", "") or analysis_row.get("Return_Rate", ""))

        comparable_no_data = comparable_count == 0 or median_comp_unit <= 0 or our_unit_price <= 0
        price_gap = ""
        competitor_cheaper = False
        if not comparable_no_data:
            price_gap_value = (our_unit_price - median_comp_unit) / median_comp_unit if median_comp_unit > 0 else 0.0
            price_gap = format_decimal(price_gap_value, 4)
            competitor_cheaper = median_comp_unit < our_unit_price
        level, risk_score = level_from_gap(parse_float(price_gap), our_margin, competitor_cheaper, our_return_rate) if not comparable_no_data else ("Not Enough Data", 0.0)
        if comparable_no_data:
            action = action_from_level("Not Enough Data", False, planner_row.get("Final_Ads_Decision", ""), True)
        else:
            action = action_from_level(level, competitor_cheaper, planner_row.get("Final_Ads_Decision", ""), False)

        if comparable_no_data:
            insight = "No comparable Flipkart competitor data found."
        else:
            insight = (
                f"{comparable_count} comparable Flipkart results; median competitor unit price {median_comp_unit:.2f}; "
                f"our unit price {our_unit_price:.2f}; gap {parse_float(price_gap) * 100:.1f}%."
            )
            if our_return_rate >= 0.20:
                insight += " Return rate is elevated."
            if our_margin < 0.10:
                insight += " Profit margin is tight."

        has_pack_counts = any(normalize_text(row.get("Competitor_Pack_Count", "")) for row in competitor_rows) and bool(our_pack_count)
        has_unit_prices = any(normalize_text(row.get("Competitor_Unit_Price", "")) for row in competitor_rows) and bool(our_unit_price_text)
        confidence = confidence_from_data(comparable_count, has_pack_counts, has_unit_prices)
        best_rating = ""
        if competitor_ratings:
            best_rating = format_decimal(max(competitor_ratings), 2)
        best_reviews = ""
        if competitor_reviews:
            best_reviews = format_decimal(max(competitor_reviews), 0)

        intelligence_rows.append(
            {
                "Run_ID": normalize_text(queue_row.get("Run_ID", "")),
                "FSN": fsn,
                "SKU_ID": normalize_text(queue_row.get("SKU_ID", "")),
                "Product_Title": normalize_text(queue_row.get("Product_Title", "")),
                "Final_Ads_Decision": normalize_text(planner_row.get("Final_Ads_Decision", "")) or normalize_text(queue_row.get("Final_Ads_Decision", "")),
                "Our_Selling_Price": normalize_text(queue_row.get("Our_Selling_Price", "")),
                "Our_Pack_Count": our_pack_count,
                "Our_Unit_Price": our_unit_price_text,
                "Our_Final_Profit_Margin": normalize_text(queue_row.get("Our_Final_Profit_Margin", "")),
                "Our_Return_Rate": normalize_text(queue_row.get("Our_Return_Rate", "")),
                "Comparable_Competitor_Count": str(comparable_count),
                "Lowest_Comparable_Competitor_Price": format_decimal(lowest_comp_price, 2) if lowest_comp_price else "",
                "Median_Comparable_Competitor_Price": format_decimal(median_comp_price, 2) if median_comp_price else "",
                "Lowest_Comparable_Competitor_Unit_Price": format_decimal(lowest_comp_unit, 2) if lowest_comp_unit else "",
                "Median_Comparable_Competitor_Unit_Price": format_decimal(median_comp_unit, 2) if median_comp_unit else "",
                "Price_Gap_Percent": price_gap,
                "Best_Competitor_Rating": best_rating,
                "Best_Competitor_Reviews": best_reviews,
                "Competition_Risk_Score": format_decimal(risk_score, 2),
                "Competition_Risk_Level": level,
                "Competitor_Insight": insight,
                "Suggested_Action": action,
                "Confidence": confidence,
                "Last_Updated": now_iso(),
            }
        )
    return intelligence_rows


def write_output_tab(sheets_service, spreadsheet_id: str, tab_name: str, headers: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    sheet_id = ensure_tab(sheets_service, spreadsheet_id, tab_name)
    clear_tab(sheets_service, spreadsheet_id, tab_name)
    write_rows(sheets_service, spreadsheet_id, tab_name, headers, rows)
    freeze_and_format(sheets_service, spreadsheet_id, sheet_id, len(headers))


def create_flipkart_competitor_price_intelligence() -> Dict[str, Any]:
    ensure_directories()
    if not SPREADSHEET_META_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SPREADSHEET_META_PATH}")

    spreadsheet_id = load_json(SPREADSHEET_META_PATH)["spreadsheet_id"]
    sheets_service, _, _ = build_services()

    for tab_name in [QUEUE_TAB, RESULTS_TAB, ADS_PLANNER_TAB, SKU_ANALYSIS_TAB]:
        if not tab_exists(sheets_service, spreadsheet_id, tab_name):
            raise FileNotFoundError(f"Missing required Google Sheet tab: {tab_name}")

    _, queue_rows = read_table(sheets_service, spreadsheet_id, QUEUE_TAB)
    _, results_rows = read_table(sheets_service, spreadsheet_id, RESULTS_TAB) if tab_exists(sheets_service, spreadsheet_id, RESULTS_TAB) else ([], [])
    _, planner_rows = read_table(sheets_service, spreadsheet_id, ADS_PLANNER_TAB)
    _, analysis_rows = read_table(sheets_service, spreadsheet_id, SKU_ANALYSIS_TAB)

    intelligence_rows = build_price_intelligence_rows(queue_rows, results_rows, planner_rows, analysis_rows)
    looper_rows = [dict(row) for row in intelligence_rows]

    write_local_csv(LOCAL_PRICE_INTELLIGENCE_PATH, OUTPUT_HEADERS, intelligence_rows)
    write_local_csv(LOCAL_LOOKER_PATH, LOOKER_HEADERS, looper_rows)

    write_output_tab(sheets_service, spreadsheet_id, PRICE_INTELLIGENCE_TAB, OUTPUT_HEADERS, intelligence_rows)
    write_output_tab(sheets_service, spreadsheet_id, LOOKER_TAB, LOOKER_HEADERS, looper_rows)

    risk_distribution = Counter(normalize_text(row.get("Competition_Risk_Level", "")) for row in intelligence_rows)
    action_distribution = Counter(normalize_text(row.get("Suggested_Action", "")) for row in intelligence_rows)
    run_id = intelligence_rows[0]["Run_ID"] if intelligence_rows else ""

    log_row = {
        "timestamp": now_iso(),
        "spreadsheet_id": spreadsheet_id,
        "status": "SUCCESS",
        "run_id": run_id,
        "queue_rows": len(queue_rows),
        "visual_result_rows": len(results_rows),
        "price_intelligence_rows": len(intelligence_rows),
        "looker_rows": len(looper_rows),
        "risk_distribution": json.dumps(dict(risk_distribution), ensure_ascii=False),
        "suggested_action_distribution": json.dumps(dict(action_distribution), ensure_ascii=False),
        "message": "Built Flipkart competitor price intelligence",
    }
    append_csv_log(
        LOG_PATH,
        [
            "timestamp",
            "spreadsheet_id",
            "status",
            "run_id",
            "queue_rows",
            "visual_result_rows",
            "price_intelligence_rows",
            "looker_rows",
            "risk_distribution",
            "suggested_action_distribution",
            "message",
        ],
        [log_row],
    )

    payload = {
        "status": "SUCCESS",
        "run_id": run_id,
        "queue_rows": len(queue_rows),
        "visual_result_rows": len(results_rows),
        "price_intelligence_rows": len(intelligence_rows),
        "looker_rows": len(looper_rows),
        "risk_distribution": dict(risk_distribution),
        "suggested_action_distribution": dict(action_distribution),
        "tabs_updated": [PRICE_INTELLIGENCE_TAB, LOOKER_TAB],
        "local_outputs": {
            "price_intelligence": str(LOCAL_PRICE_INTELLIGENCE_PATH),
            "looker": str(LOCAL_LOOKER_PATH),
        },
        "log_path": str(LOG_PATH),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return payload


def main() -> None:
    try:
        create_flipkart_competitor_price_intelligence()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "log_path": str(LOG_PATH),
                    "tabs_updated": [PRICE_INTELLIGENCE_TAB, LOOKER_TAB],
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
