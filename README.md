# Ecommerce Automation Analysis

## Flipkart Control Tower - Team Commands

Use these first for normal team work:

```powershell
.\run_flipkart_dashboard.ps1
.\run_flipkart_quick_refresh.ps1
.\run_flipkart_health_check.ps1
.\run_flipkart_cogs_refresh.ps1
.\run_flipkart_actions_refresh.ps1
.\run_flipkart_competitor_refresh.ps1
.\run_flipkart_full_safe_refresh.ps1
```

The Streamlit dashboard is now the primary daily dashboard.
Looker Studio remains optional and reads the `LOOKER_*` tabs as a downstream source layer.

Use quick refresh normally.
Use full safe refresh only after replacing monthly raw reports.
Use health check if unsure.
Do not run paid/API external calls unless intentionally doing keyword or visual search.

## Hosted Dashboard

- Local launch:
  - `.\run_flipkart_dashboard.ps1`
- Cloud launch:
  - Streamlit Community Cloud URL after deployment
- Data refresh:
  - Run the normal local pipeline or quick refresh locally.
  - Updated rows land in Google Sheets.
  - The cloud dashboard reads the updated tabs automatically.
- Read-only rule:
  - The hosted dashboard is read-only and does not write back to Google Sheets.
- Service account:
  - Share the Google Sheet with `streamlit-flipkart-dashboard@dn-data-487114.iam.gserviceaccount.com`.

Source:
- https://chatgpt.com/share/69f04e97-13b4-83a5-b2a6-cdf42730e47d

Notes:
- I could not reliably read the shared conversation from this environment, so this folder is a clean project scaffold based on the share title/slug.
- Add the conversation transcript, screenshots, exports, or working notes here as they become available.

Suggested structure:
- `inputs/` for source materials
- `notes/` for analysis and working notes
- `output/` for deliverables

Phase 2 run command:
```powershell
python src\create_sku_image_folders.py
```

Phase 3 run command:
```powershell
python src\create_product_content_sheet.py
```

Phase 4 run command:
```powershell
python src\create_attribute_map_sheet.py
```

Phase 5 run command:
```powershell
python src\create_marketplace_export_sheets.py
```

Phase 6 run command:
```powershell
python src\create_listing_status_tracker.py
```

Phase 7 run command:
```powershell
python src\import_orders.py
```

Phase 7 note:
- The importer scans every CSV/XLSX file in `data\input\orders`
- Reruns skip duplicate `Order_ID + Marketplace + SKU_ID` rows

Phase 8 run command:
```powershell
python src\import_settlements.py
```

Phase 9 run command:
```powershell
python src\import_ads.py
```

Phase 10 run command:
```powershell
python src\calculate_sku_pnl.py
```

Phase 11 run command:
```powershell
python src\create_review_rating_tracker.py
```

Phase 12 run command:
```powershell
python src\create_sku_scorecard.py
```

Phase 13 run command:
```powershell
python src\create_decision_tags.py
```

Phase 14 run command:
```powershell
python src\create_daily_alerts.py
```

Phase 15 backup command:
```powershell
python src\backup_google_sheet.py
```

Phase 15 import command:
```powershell
python src\import_master_skus.py
```

Phase 15 validation command:
```powershell
python src\validate_phase15_pipeline.py
```

Flipkart production runner:
```powershell
.\run_flipkart_pipeline.ps1
```

This one command runs the full Flipkart flow in order, stops on the first failure, refreshes the Flipkart alerts and action tracker layer, and only pushes to Google Sheets if the audit passes.

Flipkart alerts and tasks refresh:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_alerts_and_tasks
```

Flipkart alerts and tasks verification:
```powershell
python -m src.marketplaces.flipkart.verify_flipkart_alerts_tasks
```

Flipkart dashboard summary:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_dashboard
```

Flipkart FSN drilldown dashboard:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_fsn_drilldown
```

Flipkart order ID explorer:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_order_item_explorer
python -m src.marketplaces.flipkart.verify_flipkart_order_item_explorer
```

Flipkart COGS layer verification:
```powershell
python -m src.marketplaces.flipkart.verify_flipkart_cogs_layer
```

Stage 7 return comments analysis:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_return_comments_analysis
```

Stage 7 return comments verification:
```powershell
python -m src.marketplaces.flipkart.verify_flipkart_return_comments_analysis
```

Stage 7B return issue integration:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_alerts_and_tasks
python -m src.marketplaces.flipkart.create_flipkart_dashboard
python -m src.marketplaces.flipkart.create_flipkart_fsn_drilldown
python -m src.marketplaces.flipkart.verify_flipkart_return_issue_integration
```

Stage 8 ads planner foundation:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_ads_planner_foundation
python -m src.marketplaces.flipkart.verify_flipkart_ads_planner_foundation
```

Stage 8B ads report mapping diagnostic:
```powershell
python -m src.marketplaces.flipkart.diagnose_flipkart_ads_report
```

Stage 8B ads report mapping:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_ads_mapping
```

Stage 8B ads report mapping verification:
```powershell
python -m src.marketplaces.flipkart.verify_flipkart_ads_mapping
```

Stage 8C final ads recommendation logic:
```powershell
python -m src.marketplaces.flipkart.update_flipkart_ads_recommendations
python -m src.marketplaces.flipkart.verify_flipkart_ads_recommendations
```

Fast daily refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode quick
```

Only Looker refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode looker-only
```

Only competitor refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode competitor-only
```

Only COGS refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode cogs-only
```

Only health check:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode health-only
```

Full safe refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode full --sleep-seconds 5 --health-delay-seconds 30
```

Default quick refresh does NOT call:
- Google Ads API
- SerpApi / Google Lens

Stage 9 listing presence workflow:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_listing_presence_workflow
python -m src.marketplaces.flipkart.verify_flipkart_listing_presence_workflow
```

Looker Studio source tabs:
```powershell
python -m src.marketplaces.flipkart.create_looker_studio_sources
python -m src.marketplaces.flipkart.verify_looker_studio_sources
```

Flipkart run comparison:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_run_comparison
python -m src.marketplaces.flipkart.verify_flipkart_run_comparison
```

Flipkart adjustment ledger:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_adjustment_ledger
python -m src.marketplaces.flipkart.apply_flipkart_adjustments
python -m src.marketplaces.flipkart.verify_flipkart_adjustment_ledger
```

Flipkart report format drift monitor:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_report_format_baseline
python -m src.marketplaces.flipkart.check_flipkart_report_format_drift
python -m src.marketplaces.flipkart.verify_flipkart_report_format_monitor
```

Report format monitor SOP:
- Create or refresh the baseline only after a known-good raw report cycle.
- Use the drift check for the normal recurring validation cycle.
- Refresh the baseline again only when a report format change is intentional and the parser update has been confirmed.

Flipkart run quality score:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_run_quality_score
python -m src.marketplaces.flipkart.verify_flipkart_run_quality_score
```

Run quality score SOP:
- Use the score to judge whether the latest run is reliable enough for business decisions.
- Treat the score as a score/output layer only.
- Do not rerun the full pipeline to refresh this layer unless upstream report tabs changed.
- Upgrade 7 stays standalone; use the two `python -m` commands above instead of the full wrapper.

Flipkart module confidence:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_module_confidence
python -m src.marketplaces.flipkart.verify_flipkart_module_confidence
```

Module confidence SOP:
- Use this as a standalone confidence layer for FSN-level module health.
- Do not wire it into the full wrapper yet.
- Keep the source tabs untouched except for the non-destructive `FLIPKART_SKU_ANALYSIS` confidence columns.

Looker Studio connection guide:
1. Open Looker Studio
2. Create blank report
3. Add data source
4. Choose Google Sheets
5. Select `MASTER_SKU_DATABASE`
6. Connect the `LOOKER_*` tabs
7. Use `LOOKER_FLIPKART_EXECUTIVE_SUMMARY` for scorecards
8. Use `LOOKER_FLIPKART_FSN_METRICS` for the product table
9. Use `LOOKER_FLIPKART_ALERTS` and `LOOKER_FLIPKART_ACTIONS` for the operational dashboard
10. Use `LOOKER_FLIPKART_ADS`, `LOOKER_FLIPKART_RETURNS`, and `LOOKER_FLIPKART_LISTINGS` for the supporting views
11. Use `LOOKER_FLIPKART_ORDER_ITEM_EXPLORER` for copy-friendly Order ID / Order Item ID checks

Stage 6 note:
- `create_flipkart_alerts_and_tasks` now prefers `Final_Net_Profit` and `Final_Profit_Margin` when `COGS_Status` is `Entered` or `Verified`, and falls back to `Net_Profit_Before_COGS` plus COGS-missing alerts when cost is unavailable
- `create_flipkart_dashboard` now surfaces COGS completion, final profit totals, and final margin metrics
- `create_flipkart_fsn_drilldown` now shows the COGS and final-profit fields alongside the earlier before-COGS metrics
- The full wrapper is still intentionally separate from this stage-specific refresh

Flipkart cost master:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_cost_master
```

Flipkart profit after COGS:
```powershell
python -m src.marketplaces.flipkart.update_flipkart_profit_after_cogs
```

Code backup:
```powershell
python -m src.backup_project_code
```

Flipkart history tabs:
- `FLIPKART_RUN_HISTORY` tracks one row per Flipkart run
- `FLIPKART_FSN_HISTORY` tracks one row per FSN per run
- Both tabs are append-only and preserve prior runs

Google Drive archive sync:
```powershell
python -m src.marketplaces.flipkart.sync_flipkart_run_archive_to_drive
python -m src.marketplaces.flipkart.verify_flipkart_drive_archive_sync
python -m src.marketplaces.flipkart.sync_flipkart_run_archive_to_drive --run-id FLIPKART_YYYYMMDD_HHMMSS
```

Google Drive archive SOP:
- Local run archives live under `data/output/marketplaces/flipkart/runs/<run_id>`
- The archive sync mirrors each completed run to `ECOM_CONTROL_TOWER/03_RUN_ARCHIVES/FLIPKART/<run_id>`
- Unsafe files, credentials, token files, `.env`, `.venv`, `__pycache__`, and `.pyc` files are skipped

Final Flipkart SOP:

A. When raw Flipkart reports change:
```powershell
.\run_flipkart_pipeline.ps1
.\run_flipkart_post_analysis_refresh.ps1
```

Fast daily refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode quick
```

Only Looker refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode looker-only
```

Only competitor refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode competitor-only
```

Only COGS refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode cogs-only
```

Only health check:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode health-only
```

Full safe refresh:
```powershell
python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --mode full --sleep-seconds 5 --health-delay-seconds 30
```

B. When only COGS changed:
```powershell
python -m src.marketplaces.flipkart.update_flipkart_profit_after_cogs
.\run_flipkart_post_analysis_refresh.ps1
```

C. When only action tracker statuses changed:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_dashboard
python -m src.marketplaces.flipkart.create_flipkart_fsn_drilldown
```

D. When only ads CSV changed:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_ads_mapping
python -m src.marketplaces.flipkart.update_flipkart_ads_recommendations
python -m src.marketplaces.flipkart.create_flipkart_dashboard
```

E. When only Returns Report.csv changed:
```powershell
python -m src.marketplaces.flipkart.create_flipkart_return_comments_analysis
python -m src.marketplaces.flipkart.create_flipkart_alerts_and_tasks
python -m src.marketplaces.flipkart.create_flipkart_dashboard
python -m src.marketplaces.flipkart.create_flipkart_fsn_drilldown
```

F. Final health check:
```powershell
python -m src.marketplaces.flipkart.verify_flipkart_system_health
```

## GitHub Backup SOP

- Create a private GitHub repo for this project before the first push.
- Run `git init` before the first commit.
- Run the safety check before every commit.
- Only commit when `safe_to_commit=true`.
- Never commit credentials, exports, reports, logs, or other generated data.
- Keep the template files in `config/` as placeholders only.

Safety check:
```powershell
.\check_repo_safety.ps1
```

First commit workflow:
```powershell
git init
git branch -M main
.\check_repo_safety.ps1
git add .
git commit -m "Initial private backup setup"
git remote add origin https://github.com/YOUR_USERNAME/YOUR_PRIVATE_REPO.git
git push -u origin main
```

Normal commit workflow:
```powershell
.\check_repo_safety.ps1
git add -A
git commit -m "Describe your change"
git push
```

Requirements note:
- The repo safety check uses only the Python standard library, so no new package installation is required.
