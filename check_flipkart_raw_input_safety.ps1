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
        Write-Host 'Running Flipkart raw input safety check...' -ForegroundColor Cyan
        & python -m src.marketplaces.flipkart.check_flipkart_raw_input_safety
        if ($LASTEXITCODE -ne 0) {
            throw "Flipkart raw input safety check failed with exit code $LASTEXITCODE."
        }
        Write-Host 'Flipkart raw input safety check completed successfully.' -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-Host "Flipkart raw input safety check failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
