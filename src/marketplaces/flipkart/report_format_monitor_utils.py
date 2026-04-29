from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from src.marketplaces.flipkart.flipkart_utils import (
    best_header_match,
    detect_header_row,
    count_detected_columns,
    file_mtime_iso,
    list_input_files,
    load_report_patterns,
    load_synonyms,
    normalize_key,
    normalize_text,
    now_iso,
    read_workbook_rows,
    report_type_scores,
    ALL_USEFUL_COLUMN_KEYS,
    REPORT_TYPE_COLUMN_KEYS,
)

HEADER_TERMS = (
    "fsn",
    "sku",
    "order",
    "return",
    "settlement",
    "pnl",
    "campaign",
    "views",
    "clicks",
    "listing",
)

VALID_SEVERITIES = ("Low", "Medium", "High", "Critical")
VALID_DRIFT_STATUSES = (
    "OK",
    "Minor Change",
    "Major Change",
    "Missing File",
    "New File",
    "Missing Sheet",
    "Header Changed",
    "Row Count Warning",
    "Human Review Required",
)

BUSINESS_REPORT_TYPES = {"orders", "returns", "settlements", "pnl", "sales_tax", "listing", "ads", "master_fsn"}
HELPER_HINTS = ("help", "summary", "instructions", "readme", "dropdown", "template", "overview", "index", "notes")

FILE_SHEET_KEY_SEPARATOR = "::"


def build_file_sheet_key(file_name: str, sheet_name: str) -> str:
    return f"{normalize_text(file_name)}{FILE_SHEET_KEY_SEPARATOR}{normalize_text(sheet_name)}"


def normalize_header_value(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()
    return text


def row_texts(row: Sequence[Any]) -> List[str]:
    return [normalize_text(cell) for cell in row]


def row_non_empty_count(row: Sequence[Any]) -> int:
    return sum(1 for cell in row if normalize_text(cell))


def row_term_hits(row: Sequence[Any]) -> int:
    joined = " ".join(value.lower() for value in row_texts(row) if value)
    return sum(1 for term in HEADER_TERMS if term in joined)


def row_score(row: Sequence[Any]) -> Tuple[int, int, int]:
    non_empty = row_non_empty_count(row)
    term_hits = row_term_hits(row)
    detected = 0
    for cell in row:
        canonical, score = best_header_match(cell, load_synonyms())
        if canonical and score >= 0.82:
            detected += 1
    return non_empty, term_hits, detected


def detect_report_header(rows: Sequence[Sequence[Any]]) -> Tuple[Optional[int], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    synonyms = load_synonyms()
    header_row_index, detected_columns, candidates = detect_header_row(rows, synonyms)
    best_candidate = None
    if candidates:
        best_candidate = max(
            candidates,
            key=lambda item: (
                int(item.get("matched_columns", 0)),
                float(item.get("score", 0.0)),
                len([cell for cell in item.get("headers", []) if normalize_text(cell)]),
                -int(item.get("row_index", 0)),
            ),
        )

    if not rows or best_candidate is None:
        return None, {}, candidates

    header_like = (
        int(best_candidate.get("matched_columns", 0)) > 0
        or int(best_candidate.get("score", 0.0)) >= 3
        or row_term_hits(rows[int(best_candidate.get("row_index", 0))]) > 0
    )
    if not header_like:
        return None, {}, candidates

    return int(best_candidate.get("row_index", 0)), detected_columns, candidates


def build_detected_columns(headers: Sequence[Any]) -> Dict[str, Dict[str, Any]]:
    synonyms = load_synonyms()
    detected: Dict[str, Dict[str, Any]] = {}
    for index, header in enumerate(headers):
        canonical, score = best_header_match(header, synonyms)
        if canonical and score >= 0.82:
            current = detected.get(canonical)
            if current is None or score > float(current.get("score", 0.0)):
                detected[canonical] = {
                    "header": normalize_text(header),
                    "index": index,
                    "score": round(float(score), 4),
                }
    return detected


def infer_report_type(file_name: str, sheet_name: str, headers: Sequence[Any]) -> str:
    patterns = load_report_patterns()
    detected_columns = build_detected_columns(headers)
    scores = report_type_scores(file_name, sheet_name, detected_columns, patterns)
    if not scores:
        return "unknown"
    best_type = max(scores, key=scores.get)
    return best_type if scores.get(best_type, 0.0) > 0 else "unknown"


def select_header_row_values(rows: Sequence[Sequence[Any]], header_row_index: Optional[int]) -> List[str]:
    if header_row_index is None or header_row_index < 0 or header_row_index >= len(rows):
        return []
    return [normalize_text(cell) for cell in rows[header_row_index]]


def count_data_rows(rows: Sequence[Sequence[Any]], header_row_index: Optional[int]) -> int:
    if header_row_index is None:
        return max(0, sum(1 for row in rows if row_non_empty_count(row) > 0) - 1)
    return max(0, sum(1 for row in rows[header_row_index + 1 :] if row_non_empty_count(row) > 0))


def count_columns(rows: Sequence[Sequence[Any]], header_row_index: Optional[int], headers: Sequence[Any]) -> int:
    if header_row_index is not None and header_row_index < len(rows):
        return len(rows[header_row_index])
    return max([len(row) for row in rows if row] + [len(list(headers))])


def first_data_row(rows: Sequence[Sequence[Any]], header_row_index: Optional[int]) -> List[Any]:
    if not rows:
        return []
    start_index = 0 if header_row_index is None else header_row_index + 1
    for row in rows[start_index:]:
        if row_non_empty_count(row) > 0:
            return list(row)
    return []


def sample_row_hash(row: Sequence[Any]) -> str:
    if not row:
        return ""
    payload = "|".join(normalize_text(cell) for cell in row)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest().upper()


def file_extension_text(file_path: Path) -> str:
    return file_path.suffix.lower()


def header_detection_status_for(rows: Sequence[Sequence[Any]], header_row_index: Optional[int]) -> str:
    if not rows:
        return "Not Applicable"
    if header_row_index is None:
        return "Not Detected"
    return "Detected"


def helper_sheet_hint(sheet_name: str) -> bool:
    sheet_norm = normalize_key(sheet_name)
    return any(token in sheet_norm for token in HELPER_HINTS)


def count_rows_with_values(rows: Sequence[Sequence[Any]]) -> int:
    return sum(1 for row in rows if row_non_empty_count(row) > 0)


def count_business_header_hits(report_type: str, detected_columns: Dict[str, Dict[str, Any]]) -> int:
    if report_type in BUSINESS_REPORT_TYPES:
        return count_detected_columns(detected_columns, REPORT_TYPE_COLUMN_KEYS.get(report_type, ()))
    return 0


def classify_sheet(
    file_name: str,
    sheet_name: str,
    rows: Sequence[Sequence[Any]],
    header_row_index: Optional[int],
    detected_columns: Dict[str, Dict[str, Any]],
    report_type: str,
) -> Dict[str, Any]:
    row_count = count_rows_with_values(rows)
    effective_data_rows = count_data_rows(rows, header_row_index) if header_row_index is not None else 0
    headers = select_header_row_values(rows, header_row_index)
    normalized_headers = [normalize_header_value(header) for header in headers]
    business_header_hits = count_business_header_hits(report_type, detected_columns)
    useful_header_hits = count_detected_columns(detected_columns, ALL_USEFUL_COLUMN_KEYS)
    helper_hint = helper_sheet_hint(sheet_name)
    header_detection_status = header_detection_status_for(rows, header_row_index)
    requires_business_headers = report_type in BUSINESS_REPORT_TYPES
    required_business_headers_present = bool(business_header_hits > 0) if requires_business_headers else None

    if row_count == 0:
        sheet_class = "Empty Sheet"
    elif effective_data_rows == 0 and business_header_hits == 0:
        sheet_class = "Helper Sheet" if helper_hint or header_detection_status != "Detected" else "Unknown / Needs Review"
    elif business_header_hits > 0 and (effective_data_rows > 0 or header_detection_status == "Detected"):
        sheet_class = "Data Sheet"
    elif helper_hint and useful_header_hits == 0:
        sheet_class = "Helper Sheet"
    else:
        sheet_class = "Unknown / Needs Review"

    if sheet_class == "Unknown / Needs Review" and (row_count == 0 or effective_data_rows == 0):
        sheet_class = "Helper Sheet" if helper_hint or useful_header_hits == 0 else "Empty Sheet"

    return {
        "sheet_class": sheet_class,
        "effective_data_rows": effective_data_rows if sheet_class == "Data Sheet" else 0,
        "header_detection_status": header_detection_status,
        "required_business_headers_present": required_business_headers_present,
        "headers": headers,
        "normalized_headers": normalized_headers,
        "row_count": row_count,
        "business_header_hits": business_header_hits,
        "useful_header_hits": useful_header_hits,
    }


def scan_raw_report_files(raw_dir: Path, baseline_created_at: Optional[str] = None) -> Tuple[List[Dict[str, Any]], int, int]:
    entries: List[Dict[str, Any]] = []
    files = list_input_files(raw_dir)
    created_at = baseline_created_at or now_iso()

    for file_path in files:
        workbook_rows = read_workbook_rows(file_path)
        file_modified_time = file_mtime_iso(file_path) if file_path.exists() else ""
        suffix = file_extension_text(file_path)
        for sheet_name, rows in workbook_rows.items():
            header_row_index, _, _ = detect_report_header(rows)
            headers = select_header_row_values(rows, header_row_index)
            normalized_headers = [normalize_header_value(header) for header in headers]
            data_row = first_data_row(rows, header_row_index)
            report_type = infer_report_type(file_path.name, sheet_name, headers)
            classification = classify_sheet(file_path.name, sheet_name, rows, header_row_index, build_detected_columns(headers), report_type)
            entry = {
                "file_name": file_path.name,
                "file_extension": suffix,
                "sheet_name": "" if suffix == ".csv" else sheet_name,
                "detected_report_type": report_type,
                "sheet_class": classification["sheet_class"],
                "effective_data_rows": classification["effective_data_rows"],
                "header_detection_status": classification["header_detection_status"],
                "required_business_headers_present": classification["required_business_headers_present"],
                "row_count": classification["row_count"],
                "column_count": count_columns(rows, header_row_index, headers),
                "header_row_index": header_row_index if header_row_index is not None else "",
                "headers": headers,
                "normalized_headers": normalized_headers,
                "sample_first_data_row_hash": sample_row_hash(data_row),
                "file_modified_time": file_modified_time,
                "baseline_created_at": created_at,
            }
            entries.append(entry)

    return entries, len(files), len(entries)


def entry_key(entry: Dict[str, Any]) -> str:
    return build_file_sheet_key(entry.get("file_name", ""), entry.get("sheet_name", ""))


def as_header_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [normalize_text(item) for item in value]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:
            return [part.strip() for part in value.split("|") if part.strip()]
        if isinstance(parsed, list):
            return [normalize_text(item) for item in parsed]
    return []


def compare_entries(
    baseline_entries: Sequence[Dict[str, Any]],
    current_entries: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    baseline_map = {entry_key(entry): dict(entry) for entry in baseline_entries}
    current_map = {entry_key(entry): dict(entry) for entry in current_entries}
    baseline_files = {normalize_text(entry.get("file_name", "")) for entry in baseline_entries}
    current_files = {normalize_text(entry.get("file_name", "")) for entry in current_entries}

    monitor_rows: List[Dict[str, Any]] = []
    issue_rows: List[Dict[str, Any]] = []
    looker_rows: List[Dict[str, Any]] = []

    def join_headers(headers: Sequence[str]) -> str:
        return " | ".join(header for header in headers if header)

    def issue_row(
        file_name: str,
        sheet_name: str,
        issue_type: str,
        severity: str,
        issue_detail: str,
        baseline_value: str,
        current_value: str,
        suggested_action: str,
        status: str = "",
        owner: str = "",
        remarks: str = "",
        last_updated: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "Check_Date": now_iso(),
            "File_Name": file_name,
            "Sheet_Name": sheet_name,
            "Issue_Type": issue_type,
            "Severity": severity,
            "Issue_Detail": issue_detail,
            "Baseline_Value": baseline_value,
            "Current_Value": current_value,
            "Suggested_Action": suggested_action,
            "Status": status,
            "Owner": owner,
            "Remarks": remarks,
            "Last_Updated": last_updated or now_iso(),
        }

    def monitor_row(
        file_name: str,
        sheet_name: str,
        detected_report_type: str,
        sheet_class: str,
        effective_data_rows: int,
        header_detection_status: str,
        baseline_status: str,
        current_row_count: str,
        baseline_row_count: str,
        current_column_count: str,
        baseline_column_count: str,
        missing_headers: Sequence[str],
        new_headers: Sequence[str],
        header_change_count: int,
        severity: str,
        drift_status: str,
        suggested_action: str,
        last_updated: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "Check_Date": now_iso(),
            "File_Name": file_name,
            "Sheet_Name": sheet_name,
            "Detected_Report_Type": detected_report_type,
            "Sheet_Class": sheet_class,
            "Effective_Data_Rows": effective_data_rows,
            "Header_Detection_Status": header_detection_status,
            "Baseline_Status": baseline_status,
            "Current_Row_Count": current_row_count,
            "Baseline_Row_Count": baseline_row_count,
            "Row_Count_Change": str(int(current_row_count or 0) - int(baseline_row_count or 0)) if current_row_count != "" and baseline_row_count != "" else "",
            "Current_Column_Count": current_column_count,
            "Baseline_Column_Count": baseline_column_count,
            "Missing_Headers": join_headers(missing_headers),
            "New_Headers": join_headers(new_headers),
            "Header_Change_Count": header_change_count,
            "Severity": severity,
            "Drift_Status": drift_status,
            "Suggested_Action": suggested_action,
            "Last_Updated": last_updated or now_iso(),
        }

    def compare_pair(curr: Dict[str, Any], base: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        current_headers = as_header_list(curr.get("normalized_headers", []))
        baseline_headers = as_header_list(base.get("normalized_headers", []))
        current_row_count = int(curr.get("row_count", 0) or 0)
        baseline_row_count = int(base.get("row_count", 0) or 0)
        current_column_count = int(curr.get("column_count", 0) or 0)
        baseline_column_count = int(base.get("column_count", 0) or 0)
        current_class = normalize_text(curr.get("sheet_class", "Unknown / Needs Review")) or "Unknown / Needs Review"
        baseline_class = normalize_text(base.get("sheet_class", "Unknown / Needs Review")) or "Unknown / Needs Review"
        current_effective_rows = int(curr.get("effective_data_rows", 0) or 0)
        baseline_effective_rows = int(base.get("effective_data_rows", 0) or 0)
        current_header_status = normalize_text(curr.get("header_detection_status", "Not Detected")) or "Not Detected"
        baseline_header_status = normalize_text(base.get("header_detection_status", "Not Detected")) or "Not Detected"
        missing_headers = [header for header in baseline_headers if header and header not in current_headers]
        new_headers = [header for header in current_headers if header and header not in baseline_headers]
        header_change_count = len(missing_headers) + len(new_headers)
        row_change = current_row_count - baseline_row_count
        row_change_ratio = abs(row_change) / max(baseline_row_count, 1)
        row_drop_ratio = (baseline_row_count - current_row_count) / max(baseline_row_count, 1)
        column_change_ratio = abs(current_column_count - baseline_column_count) / max(baseline_column_count, 1)
        sheet_is_data = current_class == "Data Sheet" or baseline_class == "Data Sheet"
        same_helper_or_empty = current_class in {"Empty Sheet", "Helper Sheet"} and baseline_class in {"Empty Sheet", "Helper Sheet"}
        helper_baseline_prevented = same_helper_or_empty

        severity = "Low"
        drift_status = "OK"
        issue_rows_local: List[Dict[str, Any]] = []
        suggested_action = "No action needed"

        if same_helper_or_empty:
            severity = "Low"
            drift_status = "OK"
            suggested_action = "No action needed"
        elif baseline_class in {"Empty Sheet", "Helper Sheet"} and current_class == "Data Sheet":
            severity = "Medium"
            drift_status = "Minor Change"
            suggested_action = "Review the newly detected data sheet and add it to the baseline if intentional."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Data Sheet Appeared",
                    severity,
                    "A helper or empty baseline entry now contains data-sheet structure.",
                    baseline_class,
                    current_class,
                    suggested_action,
                )
            )
        elif baseline_class == "Data Sheet" and current_class in {"Empty Sheet", "Helper Sheet", "Unknown / Needs Review"}:
            severity = "Critical"
            drift_status = "Human Review Required"
            suggested_action = "Recheck the raw file; the expected data sheet is now empty, helper-like, or unreadable."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Data Sheet Missing Structure",
                    severity,
                    f"Baseline data sheet became {current_class.lower()}.",
                    baseline_class,
                    current_class,
                    suggested_action,
                )
            )
        elif sheet_is_data and current_header_status == "Not Detected":
            severity = "Critical"
            drift_status = "Human Review Required"
            suggested_action = "Inspect the raw report manually and re-confirm the header row."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Header Row Undetected",
                    severity,
                    "Could not detect a reliable header row for a data sheet.",
                    join_headers(baseline_headers),
                    join_headers(current_headers),
                    suggested_action,
                )
            )
        elif sheet_is_data and base.get("required_business_headers_present") is True and curr.get("required_business_headers_present") is False:
            severity = "Critical"
            drift_status = "Header Changed"
            suggested_action = "Update the baseline and parser only after confirming the new header layout."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Required Headers Missing",
                    severity,
                    "One or more required business headers are missing from the current report.",
                    join_headers(baseline_headers),
                    join_headers(current_headers),
                    suggested_action,
                )
            )
        elif sheet_is_data and row_change < 0 and row_drop_ratio >= 0.7:
            severity = "Critical"
            drift_status = "Row Count Warning"
            suggested_action = "Check source completeness and confirm the raw report cycle."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Row Count Drop",
                    severity,
                    f"Current row count dropped by {row_drop_ratio:.0%} versus the baseline.",
                    str(baseline_row_count),
                    str(current_row_count),
                    suggested_action,
                )
            )
        elif sheet_is_data and row_change < 0 and row_drop_ratio >= 0.4:
            severity = "High"
            drift_status = "Row Count Warning"
            suggested_action = "Review the raw report cycle; the row count drop is significant."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Row Count Drop",
                    severity,
                    f"Current row count dropped by {row_drop_ratio:.0%} versus the baseline.",
                    str(baseline_row_count),
                    str(current_row_count),
                    suggested_action,
                )
            )
        elif sheet_is_data and row_change < 0 and row_drop_ratio >= 0.2:
            severity = "Medium"
            drift_status = "Row Count Warning"
            suggested_action = "Check the raw report cycle for a moderate row count drop."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Row Count Drop",
                    severity,
                    f"Current row count dropped by {row_drop_ratio:.0%} versus the baseline.",
                    str(baseline_row_count),
                    str(current_row_count),
                    suggested_action,
                )
            )
        elif sheet_is_data and row_change > 0 and row_change_ratio > 0.4:
            severity = "Medium"
            drift_status = "Row Count Warning"
            suggested_action = "Review the raw report cycle; the row count increase is large."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Row Count Increase",
                    severity,
                    f"Current row count increased by {row_change_ratio:.0%} versus the baseline.",
                    str(baseline_row_count),
                    str(current_row_count),
                    suggested_action,
                )
            )
        elif sheet_is_data and row_change > 0 and row_change_ratio > 0.2:
            severity = "Low"
            drift_status = "Minor Change"
            suggested_action = "Check whether the larger row count is expected for this cycle."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Row Count Increase",
                    severity,
                    f"Current row count increased by {row_change_ratio:.0%} versus the baseline.",
                    str(baseline_row_count),
                    str(current_row_count),
                    suggested_action,
                )
            )
        elif sheet_is_data and column_change_ratio >= 0.3:
            severity = "High"
            drift_status = "Major Change"
            suggested_action = "Review the column layout and refresh the baseline only if the change is intentional."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Column Count Change",
                    severity,
                    f"Column count changed from {baseline_column_count} to {current_column_count}.",
                    str(baseline_column_count),
                    str(current_column_count),
                    suggested_action,
                )
            )
        elif sheet_is_data and column_change_ratio >= 0.15:
            severity = "Medium"
            drift_status = "Minor Change"
            suggested_action = "Check the column layout before the next analysis run."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Column Count Change",
                    severity,
                    f"Column count changed from {baseline_column_count} to {current_column_count}.",
                    str(baseline_column_count),
                    str(current_column_count),
                    suggested_action,
                )
            )
        elif sheet_is_data and new_headers:
            severity = "Medium"
            drift_status = "Minor Change"
            suggested_action = "Confirm the new headers before refreshing the baseline."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "New Headers Added",
                    severity,
                    "One or more headers were added to the report.",
                    join_headers(baseline_headers),
                    join_headers(current_headers),
                    suggested_action,
                )
            )
        elif sheet_is_data and row_change_ratio > 0.05:
            severity = "Low"
            drift_status = "Minor Change"
            suggested_action = "Row count changed slightly; confirm that the cycle is expected."
            issue_rows_local.append(
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    "Row Count Change",
                    severity,
                    f"Row count changed from {baseline_row_count} to {current_row_count}.",
                    str(baseline_row_count),
                    str(current_row_count),
                    suggested_action,
                )
            )

        monitor = monitor_row(
            curr.get("file_name", ""),
            curr.get("sheet_name", ""),
            curr.get("detected_report_type", base.get("detected_report_type", "unknown")),
            current_class,
            current_effective_rows,
            current_header_status,
            baseline_class,
            str(current_row_count),
            str(baseline_row_count),
            str(current_column_count),
            str(baseline_column_count),
            missing_headers if sheet_is_data else [],
            new_headers if sheet_is_data else [],
            header_change_count if sheet_is_data else 0,
            severity,
            drift_status,
            suggested_action,
        )
        looker = {
            "Check_Date": monitor["Check_Date"],
            "File_Name": monitor["File_Name"],
            "Sheet_Name": monitor["Sheet_Name"],
            "Detected_Report_Type": monitor["Detected_Report_Type"],
            "Severity": monitor["Severity"],
            "Drift_Status": monitor["Drift_Status"],
            "Issue_Count": str(len(issue_rows_local)),
            "Suggested_Action": monitor["Suggested_Action"],
            "Last_Updated": monitor["Last_Updated"],
        }
        return monitor, issue_rows_local, looker

    def compare_missing_base(base: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
        current_sheet_exists = any(
            normalize_text(entry.get("file_name", "")) == normalize_text(base.get("file_name", ""))
            for entry in current_entries
        )
        baseline_class = normalize_text(base.get("sheet_class", "Unknown / Needs Review")) or "Unknown / Needs Review"
        if baseline_class in {"Empty Sheet", "Helper Sheet"}:
            severity = "Low"
            drift_status = "OK"
            suggested_action = "No action needed"
            issue_rows_local: List[Dict[str, Any]] = []
        else:
            severity = "Critical"
            drift_status = "Missing File" if not current_sheet_exists else "Missing Sheet"
            issue_type = "Missing File" if not current_sheet_exists else "Missing Sheet"
            suggested_action = "Restore the missing raw file before the next analysis run." if not current_sheet_exists else "Restore the missing sheet or refresh the baseline if the workbook changed intentionally."
            issue_rows_local = [
                issue_row(
                    base.get("file_name", ""),
                    base.get("sheet_name", ""),
                    issue_type,
                    severity,
                    "The baseline entry is not present in the current raw folder scan.",
                    join_headers(as_header_list(base.get("normalized_headers", []))),
                    "",
                    suggested_action,
                )
            ]
        monitor = monitor_row(
            base.get("file_name", ""),
            base.get("sheet_name", ""),
            base.get("detected_report_type", "unknown"),
            baseline_class,
            int(base.get("effective_data_rows", 0) or 0),
            normalize_text(base.get("header_detection_status", "Not Detected")) or "Not Detected",
            "Baseline Only",
            "",
            str(base.get("row_count", "")),
            "",
            str(base.get("column_count", "")),
            as_header_list(base.get("normalized_headers", [])) if baseline_class == "Data Sheet" else [],
            [],
            len(as_header_list(base.get("normalized_headers", []))) if baseline_class == "Data Sheet" else 0,
            severity,
            drift_status,
            suggested_action,
        )
        looker = {
            "Check_Date": monitor["Check_Date"],
            "File_Name": monitor["File_Name"],
            "Sheet_Name": monitor["Sheet_Name"],
            "Detected_Report_Type": monitor["Detected_Report_Type"],
            "Severity": monitor["Severity"],
            "Drift_Status": monitor["Drift_Status"],
            "Issue_Count": "1",
            "Suggested_Action": monitor["Suggested_Action"],
            "Last_Updated": monitor["Last_Updated"],
        }
        return monitor, issue_rows_local, looker

    def compare_new_current(curr: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
        file_known = normalize_text(curr.get("file_name", "")) in baseline_files
        current_class = normalize_text(curr.get("sheet_class", "Unknown / Needs Review")) or "Unknown / Needs Review"
        if current_class in {"Empty Sheet", "Helper Sheet"}:
            severity = "Low"
            drift_status = "OK"
            suggested_action = "No action needed"
            issue_rows_local: List[Dict[str, Any]] = []
        else:
            severity = "Medium"
            drift_status = "Minor Change" if file_known else "New File"
            issue_type = "New Sheet" if file_known else "New File"
            suggested_action = "Review the new sheet and add it to the baseline if it is intentional." if file_known else "Review the new raw file and add it to the baseline if it is intentional."
            issue_rows_local = [
                issue_row(
                    curr.get("file_name", ""),
                    curr.get("sheet_name", ""),
                    issue_type,
                    severity,
                    "This current scan entry does not exist in the saved baseline.",
                    "",
                    join_headers(as_header_list(curr.get("normalized_headers", []))) if current_class == "Data Sheet" else "",
                    suggested_action,
                )
            ]
        monitor = monitor_row(
            curr.get("file_name", ""),
            curr.get("sheet_name", ""),
            curr.get("detected_report_type", "unknown"),
            current_class,
            int(curr.get("effective_data_rows", 0) or 0),
            normalize_text(curr.get("header_detection_status", "Not Detected")) or "Not Detected",
            "Current Only",
            str(curr.get("row_count", "")),
            "",
            str(curr.get("column_count", "")),
            "",
            as_header_list(curr.get("normalized_headers", [])) if current_class == "Data Sheet" else [],
            len(as_header_list(curr.get("normalized_headers", []))) if current_class == "Data Sheet" else 0,
            severity,
            drift_status,
            suggested_action,
        )
        looker = {
            "Check_Date": monitor["Check_Date"],
            "File_Name": monitor["File_Name"],
            "Sheet_Name": monitor["Sheet_Name"],
            "Detected_Report_Type": monitor["Detected_Report_Type"],
            "Severity": monitor["Severity"],
            "Drift_Status": monitor["Drift_Status"],
            "Issue_Count": "1",
            "Suggested_Action": monitor["Suggested_Action"],
            "Last_Updated": monitor["Last_Updated"],
        }
        return monitor, issue_rows_local, looker

    for key in sorted(set(baseline_map) | set(current_map)):
        base = baseline_map.get(key)
        curr = current_map.get(key)
        if base and curr:
            monitor, issues_local, looker = compare_pair(curr, base)
        elif base and not curr:
            monitor, issues_local, looker = compare_missing_base(base)
        else:
            monitor, issues_local, looker = compare_new_current(curr or {})
        monitor_rows.append(monitor)
        issue_rows.extend(issues_local)
        looker_rows.append(looker)

    summary = Counter()
    for row in monitor_rows:
        severity = normalize_text(row.get("Severity", ""))
        drift_status = normalize_text(row.get("Drift_Status", ""))
        sheet_class = normalize_text(row.get("Sheet_Class", ""))
        if drift_status == "OK":
            summary["ok_count"] += 1
            if sheet_class in {"Empty Sheet", "Helper Sheet"}:
                summary["empty_helper_ok_count"] += 1
                summary["false_positive_prevented_count"] += 1
            if sheet_class == "Data Sheet":
                summary["data_sheet_ok_count"] += 1
        elif drift_status == "Minor Change":
            summary["minor_change_count"] += 1
        elif severity == "Critical":
            summary["critical_issue_count"] += 1
        else:
            summary["major_change_count"] += 1

    return monitor_rows, issue_rows, looker_rows, dict(summary)
