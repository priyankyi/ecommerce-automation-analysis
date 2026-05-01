# Flipkart Command Index

## Quick refresh

```powershell
.\run_flipkart_quick_refresh.ps1
```

## Looker light refresh

```powershell
.\run_flipkart_looker_refresh.ps1
```

```powershell
.\run_flipkart_looker_light_refresh.ps1
```

Use `run_flipkart_looker_full_refresh.ps1` only when you need the large audit tabs.

## Looker full refresh

```powershell
.\run_flipkart_looker_full_refresh.ps1
```

## Looker ads refresh

```powershell
.\run_flipkart_looker_ads_refresh.ps1
```

## Health-only check

```powershell
.\run_flipkart_health_check.ps1
```

## Raw input safety check

```powershell
.\check_flipkart_raw_input_safety.ps1
```

## COGS-only refresh

```powershell
.\run_flipkart_cogs_refresh.ps1
```

## Actions-only refresh

```powershell
.\run_flipkart_actions_refresh.ps1
```

## Competitor-only refresh

```powershell
.\run_flipkart_competitor_refresh.ps1
```

## Full safe refresh

```powershell
.\run_flipkart_full_safe_refresh.ps1
```

## Drive archive sync

```powershell
.\run_flipkart_drive_archive_sync.ps1
```

## Safety check

```powershell
.\check_repo_safety.ps1
```

## GitHub commit workflow

Use this only when you intentionally want to publish code changes:

```powershell
git status
git add <files>
git commit -m "Describe the Flipkart operating-layer update"
git push
```

Do not commit credentials, token files, or raw report data.
