$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path $repoRoot '.venv'
$activatePath = Join-Path $venvPath 'Scripts\Activate.ps1'

try {
    if (-not (Test-Path -LiteralPath $venvPath)) {
        throw "Missing .venv at: $venvPath"
    }
    if (-not (Test-Path -LiteralPath $activatePath)) {
        throw "Missing PowerShell activation script at: $activatePath"
    }

    Push-Location $repoRoot
    try {
        . $activatePath
        Write-Host 'Running Flipkart Looker light refresh...' -ForegroundColor Cyan
        & python -m src.marketplaces.flipkart.create_looker_studio_sources --group light
        if ($LASTEXITCODE -ne 0) {
            throw "Flipkart Looker light refresh failed with exit code $LASTEXITCODE."
        }
        Write-Host 'Flipkart Looker light refresh completed successfully.' -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-Host "Flipkart Looker light refresh failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
