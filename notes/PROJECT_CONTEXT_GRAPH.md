# Project Context Graph

## Completed Phases
1. MASTER_SKU sheet
2. Drive folder system
3. SKU image folder system
4. PRODUCT_CONTENT sheet
5. ATTRIBUTE_MAP sheet
6. Marketplace Export Sheets
7. LISTING_STATUS tracker
8. Order Import System
9. Settlement Import System
10. Ads Import System
11. SKU-Level P&L Sheet
12. Review & Rating Tracker
13. SKU Scorecard
14. Decision Tags
15. Daily Alert Sheet
16. Backup + Run History foundation
17. Flipkart Alerts + Action Tracker foundation
18. Flipkart Interactive Dashboard Summary
19. Flipkart FSN Drilldown Dashboard
20. Flipkart Ads Planner foundation
21. Flipkart Ads Report Mapping
22. Flipkart Run Comparison
23. Flipkart Adjustment Ledger

## Latest Completed Phase
Phase 23 - Flipkart Adjustment Ledger

### Added
- `src/backup_google_sheet.py` added
- `src/import_master_skus.py` added
- `src/validate_phase15_pipeline.py` added
- `data/input/sku_import_template.csv` created
- Phase 15 prepares the system for real SKU onboarding and validation
- Test/sample SKUs remain in place; no fake real SKUs were imported
- `src/backup_project_code.py` added
- `src/marketplaces/flipkart/update_flipkart_run_history.py` added
- `FLIPKART_RUN_HISTORY` and `FLIPKART_FSN_HISTORY` support added to the Flipkart runner
- Flipkart runs now keep local code backups and append-only run history
- `src/marketplaces/flipkart/create_flipkart_alerts_and_tasks.py` added
- `FLIPKART_ALERTS_GENERATED`, `FLIPKART_ACTION_TRACKER`, and `FLIPKART_ACTIVE_TASKS` support added
- `src/marketplaces/flipkart/verify_flipkart_alerts_tasks.py` added
- Stage 2 alerts/tasks layer is implemented and verified with read-only tab checks
- Latest Stage 2 verification: `alerts_generated_rows=235`, `action_tracker_rows=235`, `active_tasks_rows=235`
- Latest Stage 2 verification: `critical_alerts=22`, `high_alerts=70`, `medium_alerts=104`, `low_alerts=39`
- Latest Stage 2 verification: `duplicate_alert_id_count=0`, all current tracker statuses `Open`
- Latest successful wrapper run: `.\run_flipkart_pipeline.ps1`
- Latest successful run_id: `FLIPKART_20260429_124238`
- Latest successful run summary: `target FSNs=123`, `rows written=123`, `high confidence=80`, `medium confidence=1`, `low confidence=42`
- Latest successful audit: `audit passed=true`, `pushed to Google Sheet=true`
- Latest history update: `run_history_updated=true`, `fsn_history_rows_added=123`
- Latest alerts/tasks run summary: `generated_alert_count=235`, `critical_alert_count=22`, `high_alert_count=70`, `medium_alert_count=104`, `low_alert_count=39`
- Latest alerts/tasks tracker summary: `tracker_rows_created=235`, `tracker_rows_updated=0`, `active_tasks_count=235`
- Latest archive folder: `data/output/marketplaces/flipkart/runs/FLIPKART_20260429_124238`
- Verified foundation: Flipkart production runner, PowerShell wrapper, code backup, run archive, Google Sheet push, `FLIPKART_RUN_HISTORY`, `FLIPKART_FSN_HISTORY`, and Stage 2 alerts/tasks
- `src/marketplaces/flipkart/create_flipkart_dashboard.py` added
- `FLIPKART_DASHBOARD`, `FLIPKART_DASHBOARD_DATA`, `FLIPKART_TOP_ALERTS`, and `FLIPKART_ACTION_SUMMARY` support added
- Latest dashboard run: `status=SUCCESS`, `latest_run_id=FLIPKART_20260429_124238`, `total_alerts=327`, `critical_alerts=22`, `high_alerts=85`, `medium_alerts=181`, `low_alerts=39`, `active_tasks=327`
- Latest dashboard log: `data/logs/marketplaces/flipkart/flipkart_dashboard_log.csv`
- `src/marketplaces/flipkart/create_flipkart_fsn_drilldown.py` added
- `FLIPKART_FSN_DRILLDOWN` support added
- Latest drilldown run: `status=SUCCESS`, `fsn_count=123`, `default_selected_fsn=OTLGPN5GHRDFW8MJ`
- Latest drilldown sections: `Identity`, `Business Metrics`, `Alert Summary`, `Active Alerts for Selected FSN`, `Return Details for Selected FSN`, `Historical Trend for Selected FSN`
- Latest drilldown log: `data/logs/marketplaces/flipkart/flipkart_fsn_drilldown_log.csv`
- `src/marketplaces/flipkart/create_flipkart_cost_master.py` added
- `src/marketplaces/flipkart/update_flipkart_profit_after_cogs.py` added
- `FLIPKART_COST_MASTER` support added
- COGS-backed profit columns added to `FLIPKART_SKU_ANALYSIS`
- Latest Stage 6 COGS result: `rows_read=123`, `rows_written=123`, `missing_cost_rows=63`, `missing_cogs_rows=63`
- Latest Stage 6 COGS result: `FLIPKART_COST_MASTER rows=123`, `cogs_entered_fsns=60`, `cogs_missing_fsns=63`, `cogs_completion_percent=48.78`
- Latest Stage 5 profit columns: `Cost_Price`, `Packaging_Cost`, `Other_Cost`, `Total_Unit_COGS`, `Total_COGS`, `Final_Net_Profit`, `Final_Profit_Per_Order`, `Final_Profit_Margin`, `COGS_Status`
- `src/marketplaces/flipkart/create_flipkart_return_comments_analysis.py` added
- `src/marketplaces/flipkart/verify_flipkart_return_comments_analysis.py` added
- `FLIPKART_RETURN_COMMENTS`, `FLIPKART_RETURN_ISSUE_SUMMARY`, and `FLIPKART_RETURN_REASON_PIVOT` support added
- Latest Stage 7 return-comments result: `raw_return_rows=3879`, `target_fsn_return_rows=421`, `unmapped_rows=3458`
- Latest Stage 7 return-comments result: `return_comments_rows_written=421`, `return_issue_summary_rows=59`, `return_reason_pivot_rows=53`
- Latest Stage 7 return-comments result: `duplicate_return_id_count=0`, `blank_fsn_count=0`
- Latest Stage 7 verifier result: `status=PASS`
- Latest Stage 7 issue distribution: `Other=270`, `Logistics / Courier=46`, `Product Not Working=38`, `Damaged Product=26`, `Quality Issue=19`, `Wrong Product=9`, `Customer Refused / RTO=7`, `Size / Expectation Mismatch=5`, `Return Fraud / Suspicious=1`
- Latest Stage 7 tabs created: `FLIPKART_RETURN_COMMENTS`, `FLIPKART_RETURN_ISSUE_SUMMARY`, `FLIPKART_RETURN_REASON_PIVOT`
- Latest Stage 7 local outputs: `flipkart_return_comments.csv`, `flipkart_return_issue_summary.csv`, `flipkart_return_reason_pivot.csv`
- `src/marketplaces/flipkart/create_flipkart_ads_planner_foundation.py` added
- `src/marketplaces/flipkart/verify_flipkart_ads_planner_foundation.py` added
- `FLIPKART_PRODUCT_AD_PROFILE`, `GOOGLE_ADS_KEYWORD_SEEDS`, `GOOGLE_KEYWORD_METRICS_CACHE`, `PRODUCT_TYPE_DEMAND_PROFILE`, and `FLIPKART_ADS_PLANNER` support added
- Latest Stage 8 foundation result: `fsn_count=123`
- Latest Stage 8 foundation product types: `Flood Light=74`, `Gate/Wall/Post Light=31`, `Unknown=18`
- Latest Stage 8 foundation ads actions: `Fill COGS First=63`, `Do Not Run Ads / Improve Economics=15`, `Fix Product First=7`, `Fix Product/Listing First=19`, `Resolve Critical Alert First=2`, `Test Ads=6`, `Always-On Test=11`
- Latest Stage 8 foundation result: `ready_for_test_ads_count=17`
- Latest Stage 8 foundation verifier result: `status=PASS`
- Latest Stage 8 local outputs:
  - `data/output/marketplaces/flipkart/flipkart_product_ad_profile.csv`
  - `data/output/marketplaces/flipkart/google_ads_keyword_seeds.csv`
  - `data/output/marketplaces/flipkart/google_keyword_metrics_cache.csv`
  - `data/output/marketplaces/flipkart/product_type_demand_profile.csv`
  - `data/output/marketplaces/flipkart/flipkart_ads_planner.csv`
- Latest Stage 8 log: `data/logs/marketplaces/flipkart/flipkart_ads_planner_foundation_log.csv`
- `src/marketplaces/flipkart/diagnose_flipkart_ads_report.py` added
- `src/marketplaces/flipkart/create_flipkart_ads_mapping.py` added
- `src/marketplaces/flipkart/verify_flipkart_ads_mapping.py` added
- Stage 8B ads diagnostic result: `ADS.csv rows=24`, `detected_sku_column=Sku Id`, `detected_fsn_column=none`, `detected_views_column=Views`, `detected_clicks_column=Clicks`, `detected_revenue_column=Total Revenue (Rs.)`, `detected_roi_column=ROI`, `detected_spend_column=none`, `mapping_possible_by_sku=true`
- Stage 8B raw ads mapping result: `raw_ads_rows=24`, `mapped_ads_rows=7`, `mapping_issue_rows=17`, `fsns_with_ads=7`
- Stage 8B raw ads mapping result: `total_views=1051412`, `total_clicks=23903`, `total_revenue=409928`, `total_estimated_ad_spend=30035.81`, `average_roas=8.2239`, `average_acos=0.1942`
- Stage 8B mapping issues: `No Matching FSN=17`; only 7 ad rows safely mapped to current target FSNs
- Stage 8B verification result: `status=PASS`, `ads_master_rows=7`, `ads_mapping_issues_rows=17`, `ads_summary_fsn_rows=7`, `planner_rows=123`, `planner_fsns_with_ads_data=7`
- Stage 8B verification result: `duplicate_ad_master_key_count=0`, `blank_fsn_in_ads_master_count=0`
- Stage 8B tabs added: `FLIPKART_ADS_MASTER`, `FLIPKART_ADS_MAPPING_ISSUES`, `FLIPKART_ADS_SUMMARY_BY_FSN`
- Stage 8B local outputs: `flipkart_ads_master.csv`, `flipkart_ads_mapping_issues.csv`, `flipkart_ads_summary_by_fsn.csv`, `flipkart_ads_report_diagnostic.json`
- Stage 8B logs: `flipkart_ads_report_diagnostic_log.csv`, `flipkart_ads_mapping_log.csv`
- `src/marketplaces/flipkart/update_flipkart_ads_recommendations.py` added
- `src/marketplaces/flipkart/verify_flipkart_ads_recommendations.py` added
- Latest Stage 8C result: `planner_rows=123`
- Latest Stage 8C result: `final_decision_distribution=Fill COGS First:63, Improve Economics Before Ads:15, Fix Product First:7, Fix Product/Listing First:19, Resolve Critical Alert First:2, Scale Ads:2, Test Ads:15`
- Latest Stage 8C result: `budget_distribution=Do Not Run:106, Low Test:15, Medium Test:2`
- Latest Stage 8C result: `risk_distribution=Critical:7, High:99, Low:17`
- Latest Stage 8C result: `opportunity_distribution=High:2, Medium:15, Low:106`
- Latest Stage 8C verification: `status=PASS`, `blank_final_decision_count=0`, `manual_override_columns_preserved=true`, `no_unmapped_ads_were_forced_into_planner=true`
- Latest Stage 8C local outputs: `flipkart_ads_final_recommendations.csv`
- Latest Stage 8C log: `data/logs/marketplaces/flipkart/flipkart_ads_recommendations_log.csv`
- `src/marketplaces/flipkart/create_flipkart_listing_presence_workflow.py` added
- `src/marketplaces/flipkart/verify_flipkart_listing_presence_workflow.py` added
- `FLIPKART_LISTING_PRESENCE`, `FLIPKART_MISSING_ACTIVE_LISTINGS`, and `FLIPKART_LISTING_STATUS_ISSUES` support added
- Latest Stage 9 listing presence result: `target_fsn_count=123`, `active_listing_fsn_count=9565`, `found_in_active_listing_count=101`, `missing_from_active_listing_count=22`
- Latest Stage 9 listing presence result: `critical_missing_count=3`, `high_missing_count=0`, `medium_missing_count=19`
- Latest Stage 9 verification: `status=PASS`
- Latest Stage 9 tabs created/updated: `FLIPKART_LISTING_PRESENCE`, `FLIPKART_MISSING_ACTIVE_LISTINGS`, `FLIPKART_LISTING_STATUS_ISSUES`, `FLIPKART_SKU_ANALYSIS`
- `src/marketplaces/flipkart/create_flipkart_run_comparison.py` added
- `src/marketplaces/flipkart/verify_flipkart_run_comparison.py` added
- `FLIPKART_RUN_COMPARISON`, `FLIPKART_FSN_RUN_COMPARISON`, and `LOOKER_FLIPKART_RUN_COMPARISON` support added
- Latest Upgrade 4 run result: `status=SUCCESS`, `latest_run_id=FLIPKART_20260429_181349`, `previous_run_id=FLIPKART_20260429_124238`, `run_comparison_rows=22`, `fsn_comparison_rows=123`
- Latest Upgrade 4 run result: `run_status_distribution=No Change:14, New:8`, `fsn_status_distribution=No Major Change:123`
- Latest Upgrade 4 verification result: `status=PASS`, `blank_fsn_count=0`
- Latest Upgrade 4 verification result: `comparison_status_distribution=No Major Change:123`
- Latest Upgrade 4 run summary: `improved_count=0`, `worsened_count=0`, `no_change_count=14`, `not_enough_history=0`
- Latest Upgrade 4 local outputs: `flipkart_run_comparison.csv`, `flipkart_fsn_run_comparison.csv`, `looker_flipkart_run_comparison.csv`
- Latest Upgrade 4 log: `data/logs/marketplaces/flipkart/flipkart_run_comparison_log.csv`
- `src/marketplaces/flipkart/flipkart_sheet_helpers.py` added
- `src/marketplaces/flipkart/create_flipkart_adjustment_ledger.py` added
- `src/marketplaces/flipkart/apply_flipkart_adjustments.py` added
- `src/marketplaces/flipkart/verify_flipkart_adjustment_ledger.py` added
- Upgrade 5 adjustment ledger is complete and verified
- Latest Upgrade 5 result: `FLIPKART_ADJUSTMENTS_LEDGER created`
- Latest Upgrade 5 result: `ledger_rows=0`, `valid_adjustment_rows=0`
- Latest Upgrade 5 result: `FLIPKART_ADJUSTED_PROFIT rows=492`, `LOOKER_FLIPKART_ADJUSTED_PROFIT rows=492`
- Latest Upgrade 5 result: `fsns_with_adjustments=0`, `net_adjustment=0`
- Latest Upgrade 5 verification result: `status=PASS`

## Next Phase
Upgrade 10 - Flipkart-only Visual Competitor Intelligence

## Current Focus
Flipkart v1 is complete and production-safe. Upgrade 5 adjustment ledger is complete and verified, Upgrade 6 report-format monitoring is complete and verified, Upgrade 7 run quality score is complete and verified, and Upgrade 8 module-wise data confidence is complete and verified.
- Upgrade 10 visual competitor intelligence code is now in place: `config/visual_search_template.env`, `src/integrations/visual_search/visual_search_config.py`, `src/integrations/visual_search/search_google_lens_flipkart_only.py`, `src/marketplaces/flipkart/create_flipkart_competitor_search_queue.py`, `src/marketplaces/flipkart/run_flipkart_visual_competitor_search.py`, `src/marketplaces/flipkart/create_flipkart_competitor_price_intelligence.py`, and `src/marketplaces/flipkart/verify_flipkart_competitor_intelligence.py`
- Upgrade 10 stays optional, cached, Flipkart-only, and ad-ready FSN only; it does not touch `MASTER_SKU`, the full pipeline, core P&L calculations, prices, or ads decisions
- Upgrade 10 local outputs now include `flipkart_competitor_search_queue.csv`, `flipkart_visual_competitor_results.csv`, `flipkart_competitor_price_intelligence.csv`, and `looker_flipkart_competitor_intelligence.csv`
- Upgrade 6 implementation files are now in place: `src/marketplaces/flipkart/create_flipkart_report_format_baseline.py`, `src/marketplaces/flipkart/check_flipkart_report_format_drift.py`, and `src/marketplaces/flipkart/verify_flipkart_report_format_monitor.py`
- The remaining safe next step is a known-good baseline capture followed by recurring drift checks, not a full Flipkart pipeline rerun
- Upgrade 6 monitor classification is now stable: helper and empty sheets are treated separately from data sheets, and the immediate baseline-vs-current check returns `critical_issue_count=0`
- Upgrade 7 result: `run_id=FLIPKART_20260429_181349`, `overall_score=74.91`, `grade=Usable With Warnings`, `decision_recommendation=Use Carefully`
- Upgrade 7 result: `critical_warnings=COGS completion is below 70%`, `major_warnings=Ads mapping quality is weak | Critical alerts need immediate attention`
- Upgrade 7 verification result: `status=PASS`
- Upgrade 8 result: `run_id=FLIPKART_20260429_181349`, `fsn_count=123`, `average_overall_confidence=78.57`
- Upgrade 8 result: `HIGH confidence=42`, `MEDIUM confidence=68`, `LOW confidence=13`
- Upgrade 8 result: `primary data gaps=COGS Missing:63 | Format Issue:36 | Ads Mapping Weak:15 | Listing Missing:9`
- Upgrade 8 verification result: `status=PASS`
- Upgrade 9 result: Google Keyword Planner API interface built in fallback-safe mode
- Upgrade 9 result: Google Ads Basic Access approval is pending
- Upgrade 9 result: `keyword_seed_rows=26`, `keyword_cache_rows=26`, `cache_status_distribution=Pending 26`
- Upgrade 9 result: `PRODUCT_TYPE_DEMAND_PROFILE rows=7`
- Upgrade 9 verification result: `update_product_type_demand_profile status=SUCCESS_WITH_WARNINGS`
- Upgrade 9 verification result: `verify_google_keyword_metrics_cache status=PASS_WITH_WARNINGS`, warning=`CACHE_EMPTY`
- Upgrade 10 implementation is now in place; optional credentialed verification is next if needed
- Upgrade 10 scope: optional and cached competitor lookup, Flipkart.com URLs only, no aggressive scraping, no `MASTER_SKU`, no other marketplaces, no core P&L changes, no auto price changes, no auto ads changes without clear output
- Upgrade 10 quota guard now uses a local usage ledger at `data/logs/marketplaces/flipkart/visual_search_usage_log.csv`
- Upgrade 10 must start with Scale Ads + Test Ads FSNs only

### Latest Flipkart Status
- Flipkart Run Control System is complete and verified
- Latest dashboard command: `python -m src.marketplaces.flipkart.create_flipkart_dashboard`
- Latest successful dashboard run status: `SUCCESS`
- Latest dashboard summary: `latest_run_id=FLIPKART_20260429_124238`, `total_alerts=327`, `critical_alerts=22`, `high_alerts=85`, `medium_alerts=181`, `low_alerts=39`, `active_tasks=327`
- Latest Stage 7B dashboard summary: `fsns_with_return_issue_summary=59`, `critical_return_issue_fsns=20`, `product_issue_fsns=26`, `logistics_issue_fsns=19`, `customer_rto_issue_fsns=5`
- Latest Stage 7B dashboard summary: `return_fraud_risk_fsns=0`, `top_return_issue_category=Other`, `total_classified_return_comments=151`, `other_return_comments_count=270`
- Flipkart API is not usable right now; Developer Access is pending and API tests returned HTTP 401
- Do not depend on Flipkart API for Stage 8
- Google Ads API should be kept in mind for future automation, but Stage 8 must work without Google Ads API credentials
- Upgrade 9 is complete in fallback-safe mode; Google Ads Basic Access approval is still pending
- Upgrade 9 latest verified counts: `keyword_seed_rows=26`, `keyword_cache_rows=26`, `cache_status_distribution=Pending 26`
- Upgrade 9 latest verified profile rows: `PRODUCT_TYPE_DEMAND_PROFILE rows=7`
- Upgrade 9 latest verification statuses: `update_product_type_demand_profile=SUCCESS_WITH_WARNINGS`, `verify_google_keyword_metrics_cache=PASS_WITH_WARNINGS`
- Latest Stage 6 COGS result: `FLIPKART_COST_MASTER exists`, `FLIPKART_SKU_ANALYSIS now has COGS profit columns`
- Latest Stage 6 COGS result: `rows read=123`, `rows written=123`, `missing_cost_rows=63`, `missing_cogs_rows=63`
- Latest Stage 6 COGS result: `cogs_entered_fsns=60`, `cogs_missing_fsns=63`, `cogs_completion_percent=48.78`
- Latest Upgrade 4 result: `latest_run_id=FLIPKART_20260429_181349`, `previous_run_id=FLIPKART_20260429_124238`, `run_comparison_rows=22`, `fsn_comparison_rows=123`
- Latest Upgrade 4 result: `run_status_distribution=No Change:14, New:8`, `comparison_status_distribution=No Major Change:123`
- Latest Upgrade 4 verification: `status=PASS`, `blank_fsn_count=0`, `not_enough_history=0`
- Latest Upgrade 4 tabs created/updated: `FLIPKART_RUN_COMPARISON`, `FLIPKART_FSN_RUN_COMPARISON`, `LOOKER_FLIPKART_RUN_COMPARISON`
- Upgrade 5 result: `FLIPKART_ADJUSTMENTS_LEDGER created`
- Upgrade 5 result: `ledger_rows=0`, `valid_adjustment_rows=0`, `FLIPKART_ADJUSTED_PROFIT rows=492`, `LOOKER_FLIPKART_ADJUSTED_PROFIT rows=492`
- Upgrade 5 result: `fsns_with_adjustments=0`, `net_adjustment=0`, `verification status=PASS`
- Next stage starting: `Upgrade 6 - Report Format Drift Monitor`
- Upgrade 6 rules: detect Flipkart raw report structure drift only, warning/output tabs only, no full pipeline, no normalized parser changes, no core calculation changes
- Current production features:
  - one-command PowerShell wrapper works
  - Python runner remains the underlying execution path
  - `run_id` generated
  - `input_manifest.csv` created
  - `backup_before_push.csv` created
  - `pipeline_run_summary.json` created
  - final outputs archived into run folder
  - Google Sheet push works after audit pass
  - analysis logic untouched
  - local code backup zip support added
  - FLIPKART_RUN_HISTORY append support added
  - FLIPKART_FSN_HISTORY append support added
  - FLIPKART_ALERTS_GENERATED rebuild support added
  - FLIPKART_ACTION_TRACKER manual preservation support added
  - FLIPKART_ACTIVE_TASKS rebuild support added
  - FLIPKART_DASHBOARD rebuild support added
  - FLIPKART_DASHBOARD_DATA rebuild support added
  - FLIPKART_TOP_ALERTS rebuild support added
  - FLIPKART_ACTION_SUMMARY rebuild support added
- Stage 5 cost master layer complete
- Stage 6 COGS-aware alerts and dashboard complete
- Stage 2 verifier added for read-only tab checks
- Latest Stage 6 output showed a data-source mismatch: `generated_alert_count=399`, `critical_alerts=22`, `high_alerts=70`, `medium_alerts=268`, `low_alerts=39`
- Latest Stage 6 output showed COGS counts were wrong: `cogs_available_fsn_count=0`, `cogs_missing_fsn_count=123`, `fsns_with_cogs=0`, `fsns_missing_cogs=123`
- Latest COGS verification result: `FLIPKART_COST_MASTER rows=123`, `cost_master_cogs_entered_count=60`, `FLIPKART_SKU_ANALYSIS COGS entered count=60`
- Current blocker cleared: user-entered COGS is now being detected in `FLIPKART_COST_MASTER`, so Stage 6 metrics are trustable again
- Main `MASTER_SKU` tab still appears to contain only test SKUs and is not the active Flipkart COGS source
- Current Stage 6 conclusion: COGS-aware alerts and dashboard are verified; move on to return reason analysis for target FSNs
- Current Stage 7 conclusion: return comments analysis is complete and verified; move on to operational return issue alerts/dashboard/drilldown
- Current Stage 7B conclusion: return issue alerts, dashboard integration, and FSN drilldown are complete and verified
- Latest output CSV: `data/output/marketplaces/flipkart/flipkart_sku_analysis.csv`
- Latest Google Sheet tab: `FLIPKART_SKU_ANALYSIS`
- Latest return comments outputs:
  - `data/output/marketplaces/flipkart/flipkart_return_comments.csv`
  - `data/output/marketplaces/flipkart/flipkart_return_issue_summary.csv`
  - `data/output/marketplaces/flipkart/flipkart_return_reason_pivot.csv`
- Current Stage 8 conclusion: final ads recommendation logic is complete and verified
- Current Stage 9 result: listing presence workflow is complete and verified
- Current Stage 9 rule: missing from active listing is not assumed as blocked
- Current Stage 9 rule: blocked/inactive/rejected reason requires a separate future report
- Current Stage 9 rule: keep FSN as the primary key and preserve manual action tracker fields
- Current conclusion: Flipkart v1 is production-ready and run-safe
- Latest Stage 2 verification: `FLIPKART_ALERTS_GENERATED=327`, `FLIPKART_ACTION_TRACKER=327`, `FLIPKART_ACTIVE_TASKS=327`
- Latest Stage 2 verification: `duplicate Alert_ID count=0`, tracker statuses are `Open`

### Current Debug Findings
- Manual correction: the first sheet in `Orders.xlsx` and `Returns.xlsx` had been `Help`, and that parser path was corrected before the final build
- Orders verification: selected sheet is the real `Orders` tab, not `Help`
- Returns verification: selected sheet is the real `Returns` tab, not `Help`
- Current audit conclusion: the final Flipkart CSV passed audit with zero confidence/profit/return-rate/PNL-difference mismatches
- Flipkart runner now has explicit failure finalization, JSON error output, run summary writes, and `--debug` progress mode
- Stage 2 selector now skips partial run folders and uses the latest completed `pipeline_run_summary.json`
- Stage 7 return-comment analysis should use `data/input/marketplaces/flipkart/raw/Returns Report.csv` and remain FSN-first, target-FSN-only, and read-only for normalized parsers and core P&L logic
- Stage 7B should add return issue alerts, dashboard metrics, a top return issue table, and an FSN drilldown section
- Stage 7B must not run the full wrapper, must not change normalized parsers or P&L calculations, must not touch `MASTER_SKU`, and must preserve manual fields in `FLIPKART_ACTION_TRACKER`
- Stage 8 should build `FLIPKART_PRODUCT_AD_PROFILE`, `GOOGLE_ADS_KEYWORD_SEEDS`, `GOOGLE_KEYWORD_METRICS_CACHE`, `PRODUCT_TYPE_DEMAND_PROFILE`, and `FLIPKART_ADS_PLANNER`
- Stage 8 must stay API-ready but sheet-first: no Flipkart API dependency, no Google Ads API call yet, and no change to `MASTER_SKU`
- Stage 8 foundation is complete and verified
- Stage 8B raw ads mapping is complete and verified; only 7 ad rows mapped safely and 17 remained in mapping issues
- Current rule: do not depend on `MASTER_SKU` for Flipkart ads planning; use Flipkart-specific tabs and local Flipkart output files
- Next execution stage is only a later fix if one is requested
- Stage 8B must read `data/input/marketplaces/flipkart/raw/ADS.csv`
- Stage 8B must keep SKU mapping unique only; ambiguous SKU-to-FSN mappings go to mapping issues, not planner rows
- Stage 8B must not fabricate ad spend if the source data is insufficient
- Stage 8C should update `FLIPKART_ADS_PLANNER` using profit after COGS, return readiness, listing readiness, active alerts, product seasonality, mapped ads ROAS/ACOS, and ads mapping status
- Stage 8C must preserve manual override fields and avoid force-mapping unmatched ads
- Stage 8C final recommendation logic is complete and verified
- Stage 10 post-analysis refresh was attempted and `verify_flipkart_system_health` passed
- Live Google Sheet is usable and the key Flipkart tabs exist with rows
- Stage 10 runner failed with `MemoryError` in `create_flipkart_dashboard` when modules were executed inside one Python process
- Stage 10 post-analysis refresh must be refactored to run each stage as a separate subprocess
- Stage 10 order is now: COGS update, return comments analysis, ads planner foundation, ads mapping, ads recommendations, listing presence workflow, alerts/tasks, dashboard, FSN drilldown, then verifications
- Stage 10 subprocess runner was tested
- Latest Stage 10 result: `verification_passed=true`, `manual_tabs_preserved=false`, `status=FAIL`, `failed_step=manual_tabs_preserved`
- Latest Stage 10 system health attempt hit Google Sheets API 429 quota limits on read requests per minute per user
- Latest Stage 10 control issue is now execution control, not business logic
- Manual-tab preservation check needs to be loosened or corrected and system health needs fewer Google Sheets reads with 429 backoff
- Latest Stage 10 test: `python -m src.marketplaces.flipkart.run_flipkart_post_analysis_refresh --sleep-seconds 5`
- Latest Stage 10 result: refresh steps completed until verifications, then failed at `verify_flipkart_ads_recommendations` with Google Sheets API 429
- Latest Stage 10 health check: `status=PASS`, all required tabs present, and the system is healthy
- Latest Stage 10 decision: default `run_flipkart_post_analysis_refresh` should run refresh steps only, then lightweight `verify_flipkart_system_health`
- Latest Stage 10 decision: detailed verifiers should be optional with `--verify-all`
- Latest Stage 10 control fix: manual-tab preservation check now matches the live tracker tab shape and returns `true`

### Next Task
- Upgrade 10 implementation is locally added; the next optional step is a credentialed local verification run if `credentials/visual_search.env` is supplied
- Keep the competitor lookup optional and cached
- Keep URLs Flipkart-only and skip other marketplaces
- Start with Scale Ads + Test Ads FSNs only
- Enforce the 200-search safe monthly limit before live calls
- Do not run the full Flipkart pipeline
- Do not scrape Flipkart aggressively
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Do not change core P&L calculations
- Do not auto-change prices
- Do not auto-change ads decisions without clear output
- Keep the layer isolated from business logic and profit recalculation

### Rules
- `FSN` is the primary key, primary filter, and primary join key
- `SKU_ID` is reference-only on Flipkart and may be duplicated
- Use `order_item_id` as the bridge where `FSN` is missing in returns, settlements, or P&L
- Use SKU bridge only as fallback and record `Mapping_Confidence`
- For Stage 8B, only map SKU when it resolves to one unique FSN
- For Stage 8B, send ambiguous SKU-to-FSN rows to mapping issues
- For Stage 8B, do not force ad rows into the planner when mapping confidence is low
- Do not overwrite the global `MASTER_SKU` pipeline
- Build Flipkart modules separately under `src/marketplaces/flipkart`
- Keep output audit-friendly and token-efficient
- Do not rebuild the full Flipkart pipeline unless needed
- Do not push to Google Sheet again unless a later fix requires it
- Push to Google Sheet only after audit passes
- Keep backup and history changes separate from analysis calculations
- Keep Stage 2 as an operating layer only, not a recalculation layer
- Keep the dashboard layer separate from analysis calculations
- Current Stage 10 goal: create clean production commands and final verification for all Flipkart optional modules built after the core pipeline
- Current Stage 10 scope: COGS update, alerts/tasks, dashboard, FSN drilldown, return comments analysis, ads planner foundation, ads mapping, ads recommendations, and listing presence workflow
- Current Stage 10 rule: do not change core analysis calculations
- Current Stage 10 rule: do not change normalized parsers
- Current Stage 10 rule: do not touch `MASTER_SKU`
- Current Stage 10 rule: do not touch other marketplaces
- Current Stage 10 rule: do not wipe manual tabs `FLIPKART_ACTION_TRACKER`, `FLIPKART_COST_MASTER`, `FLIPKART_PRODUCT_AD_PROFILE` manual columns, or `FLIPKART_ADS_PLANNER` manual columns
- Do not run the full wrapper for dashboard-only work
- Dashboard tabs may be rebuilt
- Build the cost layer separately first
- Do not wipe `FLIPKART_ACTION_TRACKER`
- Preserve manual fields in `FLIPKART_ACTION_TRACKER`

### Inputs
1. `Master FSN File Fk (SPARKWORLD) (3).xlsx` - target FSN control file
2. `Listing.xls` - listing, catalog, price, stock, and status source
3. `Orders.xlsx` - order, order item, and FSN transaction source
4. `Returns.xlsx` - return source; FSN may be blank, so map via `order_item_id`
5. `Settled Transactions.xlsx` - settlement, fees, and taxes source; map via `order_item_id` where FSN is missing
6. `PNL.xlsx` - Flipkart P&L reconciliation source
7. `Sales Report.xlsx` - tax, sales, and invoice source
8. `GSTR return report.xlsx` - GST summary source, not main FSN analysis source
9. `ADS.csv` - ads source; may not have FSN, map via SKU bridge only with confidence flag

### Output
- `data/output/marketplaces/flipkart/flipkart_sku_analysis.csv`
- Optional Google Sheet tab: `FLIPKART_SKU_ANALYSIS`
- Current dashboard system: `FLIPKART_DASHBOARD`, `FLIPKART_DASHBOARD_DATA`, `FLIPKART_TOP_ALERTS`, `FLIPKART_ACTION_SUMMARY`, `FLIPKART_FSN_DRILLDOWN`, `FLIPKART_ALERTS_GENERATED`, `FLIPKART_ACTION_TRACKER`, `FLIPKART_ACTIVE_TASKS`

### V2 Upgrade Track
- Upgrade 1: Private GitHub repo + safe code backup
- Goal: make the code recoverable, version-controlled, and Codex-friendly
- Scope: repo-safe code and documentation only
- Do not run Flipkart full pipeline
- Do not touch Google Sheets data
- Do not upload credentials
- Do not upload token files
- Do not upload raw reports
- Do not upload business output CSVs
- Do not upload `data/input`
- Do not upload `data/output`
- Do not upload `.venv`

- Upgrade 1 status: complete
- Private GitHub repo: `https://github.com/priyankyi/ecommerce-automation-analysis.git`
- Initial safe backup commit: `ccceff8 - Initial private backup setup`

- Upgrade 2: Google Drive Auto-Archive Sync
- Goal: copy local Flipkart run folders to Google Drive archive storage
- Local source: `data/output/marketplaces/flipkart/runs/<run_id>`
- Drive target: `ECOM_CONTROL_TOWER/03_RUN_ARCHIVES/FLIPKART/<run_id>`
- Upload only run archive outputs and metadata
- Make the sync idempotent by `run_id` so duplicate folders/files are not created
- Do not run the full Flipkart pipeline
- Do not change core Flipkart calculations
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Do not upload credentials, token files, or `.venv`
- Upgrade 2 status: complete and committed
- Upgrade 2 commit: `d47e3f1 - Add Google Drive archive sync for Flipkart runs`
- Upgrade 2 latest synced run: `FLIPKART_20260429_181349`
- Upgrade 2 verification: `PASS`
- Upgrade 2 files_uploaded: `24`
- Upgrade 2 drive archive URL: exists and key files were verified

- Upgrade 3: Looker Studio Dashboard Foundation
- Goal: create clean Looker Studio source tabs in Google Sheets so Looker Studio can connect without relying on messy operational tabs
- Scope: `LOOKER_*` source tabs only
- Do not run the full Flipkart pipeline
- Do not change core calculations
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Do not wipe manual tabs
- Keep the source layer clean and Looker-friendly
- Upgrade 3 status: complete and committed
- Upgrade 3 commit: `c5699b4 - Add Looker Studio source tabs for Flipkart dashboard`

- Upgrade 4: Run Comparison
- Goal: compare latest Flipkart run versus previous run so the team can see what improved, worsened, resolved, or newly appeared
- Scope: comparison/output tabs only
- Use `FLIPKART_RUN_HISTORY`, `FLIPKART_FSN_HISTORY`, `FLIPKART_SKU_ANALYSIS`, `FLIPKART_ALERTS_GENERATED`, and `FLIPKART_ACTIVE_TASKS`
- Do not run the full Flipkart pipeline
- Do not change core calculations
- Do not change normalized parsers
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Do not wipe manual tabs
- Keep comparison outputs separate from analysis calculations
- Upgrade 4 status: complete and verified
- Upgrade 4 latest run: `FLIPKART_20260429_181349`
- Upgrade 4 previous run: `FLIPKART_20260429_124238`
- Upgrade 4 verification: `PASS`
- Upgrade 4 row counts: `run_comparison_rows=22`, `fsn_comparison_rows=123`, `LOOKER_FLIPKART_RUN_COMPARISON rows=22`
- Upgrade 4 history counts: `history_run_count=4`, `fsn_history_rows=492`

- Upgrade 5: Flipkart Adjustment Ledger
- Goal: create an adjustment ledger to handle delayed Flipkart deductions/additions without overwriting original run history
- Scope: adjustment ledger only
- The ledger is manually editable
- Original profit stays unchanged
- Adjusted profit is calculated separately
- Do not run the full Flipkart pipeline
- Do not change core historical run data
- Do not overwrite `FLIPKART_FSN_HISTORY`
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Do not wipe manual tabs
- Keep adjustment outputs separate from analysis calculations

- Upgrade 6: Report Format Drift Monitor
- Goal: detect raw Flipkart report structure drift before analysis runs
- Scope: warning/output tabs only
- Monitor sheet names, header names, row counts, required columns, and layout drift
- Do not run the full Flipkart pipeline
- Do not change core calculations
- Do not change normalized parsers
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Do not write business recalculation outputs
- Keep the monitor separate from business logic
- Latest Upgrade 6 verification: `status=PASS`, `monitor_rows=36`, `issue_rows=0`, `critical_issue_count=0`, `empty_helper_ok_count=17`, `data_sheet_ok_count=16`

- Upgrade 7: Run Quality Score
- Goal: create a trust score for every Flipkart run so the latest run can be judged for business readiness
- Scope: score/output tabs only
- Latest Upgrade 7 result: `run_id=FLIPKART_20260429_181349`, `overall_score=74.91`, `grade=Usable With Warnings`, `decision_recommendation=Use Carefully`
- Latest Upgrade 7 result: `critical_warnings=COGS completion is below 70%`, `major_warnings=Ads mapping quality is weak | Critical alerts need immediate attention`, `verification status=PASS`
- Consider report format drift, required tabs, target FSN count, order mapping, settlement/P&L coverage, COGS completion, ads mapping, returns mapping, listing coverage, data confidence, missing COGS, missing active listings, and critical alerts
- Do not run the full Flipkart pipeline
- Do not change core calculations
- Do not change normalized parsers
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Keep the score layer separate from business recalculation
- Upgrade 8 is complete and verified; the next user-directed slice is `Upgrade 9 - Google Keyword Planner API Interface`

### V2 Guardrails
- Keep Flipkart v1 untouched
- Keep core analysis calculations unchanged
- Keep normalized parsers unchanged
- Do not touch `MASTER_SKU`
- Do not touch other marketplaces
- Keep work recoverable and version-controlled
- Keep backup and history changes separate from analysis calculations
- Keep output audit-friendly and token-efficient
- Build only one upgrade at a time
- Build Upgrade 2 as a storage/archive sync only, not a recalculation layer
- Build Upgrade 3 as a source-tab foundation only, not a recalculation layer
- Build Upgrade 4 as a comparison layer only, not a recalculation layer
- Build Upgrade 5 as an adjustment layer only, not a recalculation layer
- Build Upgrade 6 as a drift-monitor layer only, not a recalculation layer

### Working Rules
- `FSN` remains the primary key, primary filter, and primary join key
- `SKU_ID` is reference-only on Flipkart and may be duplicated
- Use `order_item_id` as the bridge where `FSN` is missing in returns, settlements, or P&L
- Use SKU bridge only as fallback and record `Mapping_Confidence`
- Read this context file first in every new Codex chat
- Do not delete existing files
- Do not rebuild completed phases
- Do not build OMS
- Keep prompts and code token-efficient

## Strict Rules
- Read this context file first in every new Codex chat
- Do not delete existing files
- Do not rebuild completed phases
- Do not build OMS
- Build only one phase at a time
- Keep prompts and code token-efficient
