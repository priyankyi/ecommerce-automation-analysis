$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path $repoRoot '.venv'
$activatePath = Join-Path $venvPath 'Scripts\Activate.ps1'
$runsDir = Join-Path $repoRoot 'data\output\marketplaces\flipkart\runs'

function Write-LatestRunFolder {
    if (Test-Path -LiteralPath $runsDir) {
        $latestRun = Get-ChildItem -LiteralPath $runsDir -Directory |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1

        if ($null -ne $latestRun) {
            Write-Host "Latest run folder: $($latestRun.FullName)" -ForegroundColor Yellow
        }
    }
}

function Invoke-FlipkartPipeline {
    param(
        [switch]$Debug
    )

    $stdoutFile = New-TemporaryFile
    $stderrFile = New-TemporaryFile
    try {
        $pythonArgs = @('-m', 'src.marketplaces.flipkart.run_flipkart_pipeline')
        if ($Debug) {
            $pythonArgs += '--debug'
        }

        $process = Start-Process -FilePath 'python' -ArgumentList $pythonArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -Wait -RedirectStandardOutput $stdoutFile.FullName -RedirectStandardError $stderrFile.FullName
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

        Write-Host 'Running Flipkart pipeline...' -ForegroundColor Cyan
        $result = Invoke-FlipkartPipeline

        if ($result.ExitCode -ne 0) {
            Write-Host 'Python stdout:' -ForegroundColor Yellow
            if (-not [string]::IsNullOrWhiteSpace($result.Stdout)) {
                Write-Host $result.Stdout -NoNewline
            }
            Write-Host 'Python stderr:' -ForegroundColor Yellow
            if (-not [string]::IsNullOrWhiteSpace($result.Stderr)) {
                Write-Host $result.Stderr -ForegroundColor Red -NoNewline
            }
            throw "Flipkart pipeline failed with exit code $($result.ExitCode)."
        }

        Write-Host 'Flipkart pipeline completed successfully.' -ForegroundColor Green
        Write-LatestRunFolder
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-Host "Flipkart pipeline failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-LatestRunFolder
    exit 1
}
