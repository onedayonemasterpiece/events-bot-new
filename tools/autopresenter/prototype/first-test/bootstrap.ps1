$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Runtime = Join-Path $Root "runtime"
$NodeHome = Join-Path $Runtime "node"
$NodeExe = Join-Path $NodeHome "node.exe"
$AgentDir = Join-Path $Root "agent"
$ConfigPath = Join-Path $Root "test-config.json"
$LogsDir = Join-Path $Root "logs"
$LogPath = Join-Path $LogsDir "latest.log"
$NodeVersion = "22.12.0"
$NodeArchive = "node-v$NodeVersion-win-x64.zip"
$NodeUrl = "https://nodejs.org/dist/v$NodeVersion/$NodeArchive"
$NodeSha256 = "2b8f2256382f97ad51e29ff71f702961af466c4616393f767455501e6aece9b8"

New-Item -ItemType Directory -Force -Path $Runtime, $LogsDir | Out-Null

function Write-Step([string]$Message) {
    $Line = "[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $Message
    Write-Host $Line -ForegroundColor Cyan
    Add-Content -LiteralPath $LogPath -Value $Line -Encoding UTF8
}

function Require-File([string]$Path, [string]$Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label is missing: $Path"
    }
}

try {
    Set-Content -LiteralPath $LogPath -Value "Autopresenter first-test bootstrap" -Encoding UTF8
    if (-not [Environment]::Is64BitOperatingSystem) {
        throw "Windows x64 is required."
    }
    Require-File $ConfigPath "Test configuration"
    Require-File (Join-Path $AgentDir "agent.mjs") "Presenter agent"
    Require-File (Join-Path $AgentDir "package-lock.json") "Pinned dependency lock"
    $Config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json

    if (-not (Test-Path -LiteralPath $NodeExe -PathType Leaf)) {
        Write-Step "Downloading portable Node.js $NodeVersion..."
        $Download = Join-Path $Runtime $NodeArchive
        Invoke-WebRequest -UseBasicParsing -Uri $NodeUrl -OutFile $Download
        $ActualSha = (Get-FileHash -LiteralPath $Download -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($ActualSha -ne $NodeSha256) {
            throw "Portable Node.js checksum mismatch."
        }
        $ExtractRoot = Join-Path $Runtime "node-extract"
        Remove-Item -LiteralPath $ExtractRoot -Recurse -Force -ErrorAction SilentlyContinue
        Expand-Archive -LiteralPath $Download -DestinationPath $ExtractRoot -Force
        Remove-Item -LiteralPath $NodeHome -Recurse -Force -ErrorAction SilentlyContinue
        Move-Item -LiteralPath (Join-Path $ExtractRoot "node-v$NodeVersion-win-x64") -Destination $NodeHome
        Remove-Item -LiteralPath $ExtractRoot -Recurse -Force
        Remove-Item -LiteralPath $Download -Force
    }

    $NpmCli = Join-Path $NodeHome "node_modules\npm\bin\npm-cli.js"
    Require-File $NpmCli "Portable npm"
    Write-Step "Checking pinned presenter dependencies..."
    & $NodeExe $NpmCli ci --prefix $AgentDir --no-audit --no-fund
    if ($LASTEXITCODE -ne 0) { throw "npm ci failed with exit code $LASTEXITCODE." }

    $Env:PLAYWRIGHT_BROWSERS_PATH = Join-Path $Runtime "playwright-browsers"
    $Env:PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT = "120000"
    $PlaywrightCli = Join-Path $AgentDir "node_modules\playwright\cli.js"
    Require-File $PlaywrightCli "Playwright CLI"
    Write-Step "Checking the pinned Playwright-managed browser..."
    & $NodeExe $PlaywrightCli install chromium
    if ($LASTEXITCODE -ne 0) { throw "Playwright browser install failed with exit code $LASTEXITCODE." }

    $Env:AUTOPRESENTER_RELAY_URL = [string]$Config.relay_url
    $Env:AUTOPRESENTER_STAGE_URL = [string]$Config.stage_url
    $Env:AUTOPRESENTER_AGENT_TOKEN = [string]$Config.agent_token
    $Env:AUTOPRESENTER_AGENT_ID = [string]$Config.agent_id
    $Env:AUTOPRESENTER_FULLSCREEN = "1"

    Write-Step "READY. Opening the 1920x1080 demonstrator..."
    Write-Host ""
    Write-Host "Use the PHONE link and press Run." -ForegroundColor Green
    Write-Host "Close this window to stop the demonstrator." -ForegroundColor DarkGray
    Write-Host ""
    & $NodeExe (Join-Path $AgentDir "agent.mjs") 2>&1 | Tee-Object -FilePath $LogPath -Append
    exit $LASTEXITCODE
}
catch {
    $Message = "ERROR: {0}" -f $_.Exception.Message
    Write-Host $Message -ForegroundColor Red
    Add-Content -LiteralPath $LogPath -Value $Message -Encoding UTF8
    exit 1
}
