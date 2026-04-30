# Flipkart Streamlit Dashboard SOP

Use this dashboard as the primary daily operating view for Flipkart.
It is read-only and only reads Google Sheets through the existing auth helpers.

## Start

```powershell
.\run_flipkart_dashboard.ps1
```

## Open

```text
http://localhost:8501
```

## Stop

```text
Ctrl + C
```

## Daily Pages

Use these pages most often:

1. `Executive Overview`
1. `Alerts & Actions`
1. `Profit & COGS`
1. `Ads Planner`
1. `Returns Intelligence`
1. `Return Comments Explorer`
1. `FSN Deep Dive`
1. `Listing Issues`
1. `Run History & Comparison`

Use `Competitor Risk`, `Data Quality`, and `Raw Data Explorer / Downloads` when you need deeper diagnosis.

## Warnings

- `Keyword cache pending` means the demand profile still has pending keyword rows.
- `Competitor Not Enough Data` means the competitor row needs more context before it should be treated as high-confidence.
- `Google Ads Basic Access pending` means keyword planning is still cache-backed and live Ads access is not ready yet.
- `Google Sheets quota limit. Wait 5 minutes and refresh.` means the sheet read quota was hit. Wait and use the Refresh data cache button.
- Missing tabs show a warning and the dashboard keeps running.

## Safety Rules

- Do not edit data inside the dashboard.
- Do not write back to Google Sheets from Streamlit.
- Do not run the Flipkart pipeline from the dashboard.
- Do not call Google Ads API from the dashboard.
- Do not call SerpApi or Google Lens from the dashboard.
- Do not touch `MASTER_SKU`.
- Do not touch other marketplaces.
- Do not expose credentials.

## Data Editing

Use Google Sheets directly for controlled edits.

- Edit action tracker rows in `FLIPKART_ACTION_TRACKER`
- Edit cost master rows in `FLIPKART_COST_MASTER`
- Keep manual fields in those tabs intact

## Source Tabs

The dashboard reads these read-only source tabs:

- `LOOKER_FLIPKART_EXECUTIVE_SUMMARY`
- `LOOKER_FLIPKART_FSN_METRICS`
- `LOOKER_FLIPKART_ALERTS`
- `LOOKER_FLIPKART_ACTIONS`
- `LOOKER_FLIPKART_ADS`
- `LOOKER_FLIPKART_RETURNS`
- `LOOKER_FLIPKART_LISTINGS`
- `LOOKER_FLIPKART_RUN_COMPARISON`
- `LOOKER_FLIPKART_ADJUSTED_PROFIT`
- `LOOKER_FLIPKART_REPORT_FORMAT_MONITOR`
- `LOOKER_FLIPKART_RUN_QUALITY_SCORE`
- `LOOKER_FLIPKART_MODULE_CONFIDENCE`
- `LOOKER_FLIPKART_DEMAND_PROFILE`
- `LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE`
- `FLIPKART_RETURN_COMMENTS`
- `FLIPKART_RETURN_ISSUE_SUMMARY`
- `FLIPKART_RETURN_REASON_PIVOT`
- `FLIPKART_MISSING_ACTIVE_LISTINGS`
- `FLIPKART_FSN_RUN_COMPARISON`
- `FLIPKART_VISUAL_COMPETITOR_RESULTS`

## Notes

- Streamlit is the main dashboard now.
- Looker Studio is optional and uses the `LOOKER_*` tabs as a downstream source layer.
- Use the manual Refresh data cache button when the Google Sheet has changed.
