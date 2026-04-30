# Looker Studio Setup

Use this guide to connect the Flipkart Google Sheet to Looker Studio.

## 1. Open Looker Studio

Go to Looker Studio and sign in with the same Google account that can open the master Flipkart Google Sheet.

## 2. Create blank report

Start a new blank report.

## 3. Add data source -> Google Sheets

Choose a Google Sheets data source.

## 4. Select the master Google Sheet

Pick the master Flipkart Google Sheet that holds the `LOOKER_*` tabs.

## 5. Add these LOOKER tabs

- `LOOKER_FLIPKART_EXECUTIVE_SUMMARY`
- `LOOKER_FLIPKART_FSN_METRICS`
- `LOOKER_FLIPKART_ALERTS`
- `LOOKER_FLIPKART_ACTIONS`
- `LOOKER_FLIPKART_ADS`
- `LOOKER_FLIPKART_RETURNS`
- `LOOKER_FLIPKART_LISTINGS`
- `LOOKER_FLIPKART_RUN_COMPARISON`
- `LOOKER_FLIPKART_ADJUSTED_PROFIT`
- `LOOKER_FLIPKART_RUN_QUALITY_SCORE`
- `LOOKER_FLIPKART_MODULE_CONFIDENCE`
- `LOOKER_FLIPKART_DEMAND_PROFILE`
- `LOOKER_FLIPKART_COMPETITOR_INTELLIGENCE`

## 6. Suggested dashboard pages

- Executive Overview
- SKU/FSN Performance
- Alerts & Actions
- Profit & COGS
- Returns
- Ads Planner
- Competitor Risk
- Run Quality

## 7. Suggested filters

- FSN
- SKU
- Product type
- Alert severity
- Owner
- Status
- Ads decision
- Competition risk

## 8. Important rule

Do not edit data inside Looker Studio. Edit in Google Sheets only.

