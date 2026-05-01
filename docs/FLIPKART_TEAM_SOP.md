# Flipkart Team SOP

This guide is for the non-technical Flipkart team. If you can open Google Sheets and double-click a PowerShell file, you can use this system.

## 1. What this system does

The Flipkart Control Tower reads Flipkart reports and turns them into simple working tabs for the team.

It can:
- Read Flipkart reports
- Build SKU and FSN analysis
- Calculate profit after COGS
- Find missing COGS
- Find customer return products and courier return issues separately
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
- `FLIPKART_ORDER_ITEM_MASTER`
- `FLIPKART_RETURN_ALL_DETAILS`
- `FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY`
- `FLIPKART_COURIER_RETURN_SUMMARY`
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

## 4. Return intelligence

Customer returns and courier returns are different:

- `customer_return` = product or customer dissatisfaction
- `courier_return` = logistics, RTO, cancellation, or delivery issue

Use the customer return tabs for product-quality review:
- `FLIPKART_CUSTOMER_RETURN_COMMENTS`
- `FLIPKART_CUSTOMER_RETURN_ISSUE_SUMMARY`

Use the courier return tabs for logistics review:
- `FLIPKART_COURIER_RETURN_COMMENTS`
- `FLIPKART_COURIER_RETURN_SUMMARY`

Use `FLIPKART_RETURN_TYPE_PIVOT` to compare the mix.
Do not use courier returns to measure product quality or to block ads by themselves.

## 5. Order ID lookup

Use `FLIPKART_ORDER_ITEM_MASTER` for day-to-day copy and search work.

Use `FLIPKART_ORDER_ITEM_SOURCE_DETAIL` only when you need to trace why a value differs across orders, returns, settlement, PNL, or return-intelligence rows.

## 6. Daily refresh command

Use:

```powershell
.\run_flipkart_quick_refresh.ps1
```

## 7. Monthly full report process

Use the `Monthly Raw File Replacement SOP` in section 14 before any full refresh.

## 8. COGS update process

Use this when the cost team updates pricing.

1. Fill COGS in `FLIPKART_COST_MASTER`
2. Run:

```powershell
.\run_flipkart_cogs_refresh.ps1
```

## 9. Competitor visual search process

Use this only for a small batch of selected FSNs.

1. Fill `Product_Image_URL` for the selected FSNs
2. Run controlled visual search only in small batches
3. Do not use `--force` repeatedly
4. Check `FLIPKART_COMPETITOR_PRICE_INTELLIGENCE`

## 10. Action tracker process

Update these fields in `FLIPKART_ACTION_TRACKER`:
- `Owner`
- `Status`
- `Action_Taken`
- `Remarks`

Then run:

```powershell
.\run_flipkart_actions_refresh.ps1
```

## 11. Google Keyword Planner status

- Currently pending Basic Access
- The system works without it
- Once approved, run keyword refresh manually

## 12. Common warnings and meaning

- `CACHE_EMPTY` = Google Ads approval pending
- keyword cache pending = normal until Google approval
- `Not Enough Data` competitor = image or search data missing
- Google Sheets 429 = wait 5 minutes and rerun

## 13. Emergency recovery

- Code backup is on GitHub
- Run archives are in Google Drive
- Credentials are local only
- Do not delete the `credentials` folder

## 14. Monthly Raw File Replacement SOP

This is the safest way to replace raw Flipkart files each month.

1. Open `data/input/marketplaces/flipkart/raw`
2. Move old files to `data/input/marketplaces/flipkart/archive/YYYY-MM`
3. Paste only the current cycle files into `data/input/marketplaces/flipkart/raw`
4. Run:

```powershell
.\check_flipkart_raw_input_safety.ps1
```

5. If the result is `PASS` or `PASS_WITH_WARNINGS`, run:

```powershell
.\run_flipkart_full_safe_refresh.ps1
```

6. If the result is `BLOCKED`, do not run full refresh
7. Read `next_action`
8. Fix the raw folder first
9. Never keep old and new report files mixed in the raw folder
10. Do not delete raw files immediately; archive them
