# Flipkart Streamlit Dashboard SOP

This guide covers the code-generated Flipkart Control Tower dashboard that replaces the manual Looker Studio setup for daily use.

## What this dashboard does

- Reads the Flipkart Google Sheet through the existing OAuth auth helper
- Uses the `LOOKER_*` source tabs as its data source
- Provides a fast, code-controlled alternative to Looker Studio
- Stays read-only and does not call the Flipkart pipeline, Google Ads API, or SerpApi

## Source tabs

The dashboard reads these tabs:

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

## Pages

1. Executive Overview
1. Alerts & Actions
1. Profit & COGS
1. Ads Planner
1. Competitor Risk
1. Data Quality
1. FSN Drilldown

## Filters

- Page selector in the sidebar
- FSN / SKU / Product search
- Alert severity
- Owner
- Status
- Ads decision
- Competition risk

## Warnings to watch

- Keyword cache pending means the demand profile still has pending keyword rows. This is normal when Google Ads access is still pending.
- `Not Enough Data` in competitor intelligence means the row needs more competitor context before it should be treated as high confidence.

## Run command

Use the PowerShell wrapper from the repo root:

```powershell
.\run_flipkart_dashboard.ps1
```

## Safety rules

- Do not edit `MASTER_SKU`
- Do not touch other marketplaces
- Do not expose or commit credentials
- Do not run the full Flipkart pipeline from this dashboard
- Do not auto-call external APIs from the dashboard

## Troubleshooting

- If the sheet cannot be loaded, check the local OAuth token and the `data/output/master_sku_sheet.json` pointer.
- If the dashboard is blank, refresh the cached data from the sidebar.
- If `streamlit` is missing, install the dependencies listed in `requirements.txt`.
