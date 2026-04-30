# Flipkart Team SOP

This guide is for the non-technical Flipkart team. If you can open Google Sheets and double-click a PowerShell file, you can use this system.

## 1. What this system does

The Flipkart Control Tower reads Flipkart reports and turns them into simple working tabs for the team.

It can:
- Read Flipkart reports
- Build SKU and FSN analysis
- Calculate profit after COGS
- Find missing COGS
- Find high return products
- Find listing problems
- Find ads-ready products
- Find competitor risk
- Build dashboard and Looker tabs
- Keep the action tracker updated

## 2. What the team should open daily

Open the Google Sheet and check these tabs first:
- `FLIPKART_DASHBOARD`
- `FLIPKART_ACTIVE_TASKS`
- `FLIPKART_ACTION_TRACKER`
- `FLIPKART_FSN_DRILLDOWN`
- `FLIPKART_ADS_PLANNER`
- `FLIPKART_COMPETITOR_PRICE_INTELLIGENCE`
- `FLIPKART_COST_MASTER`

## 3. What the team should NOT touch

Do not edit these generated tabs:
- `FLIPKART_SKU_ANALYSIS`
- `FLIPKART_ALERTS_GENERATED`
- `FLIPKART_DASHBOARD_DATA`
- `FLIPKART_RUN_HISTORY`
- `FLIPKART_FSN_HISTORY`
- `LOOKER_*` tabs
- normalized output files

## 4. Daily refresh command

Use:

```powershell
.\run_flipkart_quick_refresh.ps1
```

## 5. Monthly full report process

Use this only after replacing the monthly raw reports.

Steps:
1. Download the latest Flipkart reports
2. Replace files in `data/input/marketplaces/flipkart/raw`
3. Run the report format drift check
4. Run the full safe refresh
5. Run the health check
6. Sync the run archive to Google Drive
7. Commit code only if code changed, not data

## 6. COGS update process

Use this when the cost team updates pricing.

1. Fill COGS in `FLIPKART_COST_MASTER`
2. Run:

```powershell
.\run_flipkart_cogs_refresh.ps1
```

## 7. Competitor visual search process

Use this only for a small batch of selected FSNs.

1. Fill `Product_Image_URL` for the selected FSNs
2. Run controlled visual search only in small batches
3. Do not use `--force` repeatedly
4. Check `FLIPKART_COMPETITOR_PRICE_INTELLIGENCE`

## 8. Action tracker process

Update these fields in `FLIPKART_ACTION_TRACKER`:
- `Owner`
- `Status`
- `Action_Taken`
- `Remarks`

Then run:

```powershell
.\run_flipkart_actions_refresh.ps1
```

## 9. Google Keyword Planner status

- Currently pending Basic Access
- The system works without it
- Once approved, run keyword refresh manually

## 10. Common warnings and meaning

- `CACHE_EMPTY` = Google Ads approval pending
- keyword cache pending = normal until Google approval
- `Not Enough Data` competitor = image or search data missing
- Google Sheets 429 = wait 5 minutes and rerun

## 11. Emergency recovery

- Code backup is on GitHub
- Run archives are in Google Drive
- Credentials are local only
- Do not delete the `credentials` folder

