$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path $repoRoot '.venv'
$activatePath = Join-Path $venvPath 'Scripts\Activate.ps1'

function Invoke-FlipkartPostAnalysisRefresh {
    $stdoutFile = New-TemporaryFile
    $stderrFile = New-TemporaryFile
    try {
        $process = Start-Process -FilePath 'python' -ArgumentList @('-m', 'src.marketplaces.flipkart.run_flipkart_post_analysis_refresh', '--sleep-seconds', '5', '--health-delay-seconds', '30') -WorkingDirectory $repoRoot -NoNewWindow -PassThru -Wait -RedirectStandardOutput $stdoutFile.FullName -RedirectStandardError $stderrFile.FullName
        $stdoutText = Get-Content -LiteralPath $stdoutFile.FullName -Raw -ErrorAction SilentlyContinue
        $stderrText = Get-Content -LiteralPath $stderrFile.FullName -Raw -ErrorAction SilentlyContinue

        if (-not [string]::IsNullOrWhiteSpace($stdoutText)) {
            Write-Host $stdoutText -NoNewline
        }

        if (-not [string]::IsNullOrWhiteSpace($stderrText)) {
            Write-Host $stderrText -ForegroundColor Red -NoNewline
        }

        return @{
            ExitCode = $process.ExitCode
            Stdout = $stdoutText
            Stderr = $stderrText
        }
    }
    finally {
        Remove-Item -LiteralPath $stdoutFile.FullName -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $stderrFile.FullName -Force -ErrorAction SilentlyContinue
    }
}

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

        Write-Host 'Running Flipkart post-analysis refresh...' -ForegroundColor Cyan
        $result = Invoke-FlipkartPostAnalysisRefresh

        if ($result.ExitCode -ne 0) {
            Write-Host 'Python stdout:' -ForegroundColor Yellow
            if (-not [string]::IsNullOrWhiteSpace($result.Stdout)) {
                Write-Host $result.Stdout -NoNewline
            }
            Write-Host 'Python stderr:' -ForegroundColor Yellow
            if (-not [string]::IsNullOrWhiteSpace($result.Stderr)) {
                Write-Host $result.Stderr -ForegroundColor Red -NoNewline
            }
            throw "Flipkart post-analysis refresh failed with exit code $($result.ExitCode)."
        }

        Write-Host 'Flipkart post-analysis refresh completed successfully.' -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-Host "Flipkart post-analysis refresh failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
