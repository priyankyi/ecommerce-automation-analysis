$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = $PSScriptRoot
Set-Location $repoRoot

$venvRoot = Join-Path $repoRoot ".venv"
if (-not (Test-Path -LiteralPath $venvRoot)) {
    throw "Missing .venv at $venvRoot"
}

$activatePath = Join-Path $venvRoot "Scripts\Activate.ps1"
if (-not (Test-Path -LiteralPath $activatePath)) {
    throw "Missing PowerShell activation script: $activatePath"
}

Write-Output "Starting Flipkart dashboard..."
Write-Output "Using virtual environment at: $venvRoot"

. $activatePath

$dashboardApp = Join-Path $repoRoot "src\dashboard\flipkart_streamlit_app.py"
if (-not (Test-Path -LiteralPath $dashboardApp)) {
    throw "Missing dashboard app: $dashboardApp"
}

try {
    Write-Output "Launching Streamlit with: python -m streamlit run $dashboardApp"
    python -m streamlit run $dashboardApp
    Write-Output "Flipkart dashboard exited successfully."
}
catch {
    Write-Output "Flipkart dashboard failed."
    throw
}
