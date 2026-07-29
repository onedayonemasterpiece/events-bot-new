$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

try {
    if (-not [Environment]::Is64BitOperatingSystem) {
        throw "Windows x64 is required."
    }
    $Required = @(
        "START-DEMONSTRATOR.cmd",
        "bootstrap.ps1",
        "test-config.json",
        "agent\agent.mjs",
        "agent\abort-utils.mjs",
        "agent\pacing.mjs",
        "agent\scenario-contract.mjs",
        "agent\outro-contract.mjs",
        "agent\package.json",
        "agent\package-lock.json"
    )
    foreach ($Relative in $Required) {
        $Path = Join-Path $Root $Relative
        if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
            throw "Missing file: $Relative"
        }
    }
    $Config = Get-Content -LiteralPath (Join-Path $Root "test-config.json") -Raw | ConvertFrom-Json
    if ([string]$Config.release_kind -ne "FIRST_TEST_NOT_M3") {
        throw "Unexpected release kind."
    }
    if (-not ([string]$Config.relay_url).StartsWith("https://")) {
        throw "Relay URL must use HTTPS."
    }
    if (-not ([string]$Config.stage_url).StartsWith("https://")) {
        throw "Stage URL must use HTTPS."
    }
    if ([string]::IsNullOrWhiteSpace([string]$Config.agent_token)) {
        throw "Agent token is missing."
    }
    Write-Host "Package files, Windows x64 and HTTPS configuration are valid." -ForegroundColor Green
    exit 0
}
catch {
    Write-Host ("ERROR: {0}" -f $_.Exception.Message) -ForegroundColor Red
    exit 1
}
