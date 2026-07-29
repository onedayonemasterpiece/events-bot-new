$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8NoBom
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$AgentDir = Join-Path $Root "agent"
$ConfigPath = Join-Path $Root "test-config.json"
$LogsDir = Join-Path $Root "logs"
$LogPath = Join-Path $LogsDir "latest.log"
$NodeVersion = "22.12.0"
$LocalAppData = [Environment]::GetFolderPath("LocalApplicationData")
if ([string]::IsNullOrWhiteSpace($LocalAppData)) {
    throw "Windows LocalApplicationData is unavailable."
}
$CacheRoot = Join-Path $LocalAppData "KenigEvents\Autopresenter\cache-v1"
$NodeRoot = Join-Path $CacheRoot "node"
$NodeHome = Join-Path $NodeRoot $NodeVersion
$NodeExe = Join-Path $NodeHome "node.exe"
$NpmCache = Join-Path $CacheRoot "npm"
$PlaywrightBrowsers = Join-Path $CacheRoot "playwright-browsers"
$NodeArchive = "node-v$NodeVersion-win-x64.zip"
$NodeUrl = "https://nodejs.org/dist/v$NodeVersion/$NodeArchive"
$NodeSha256 = "2b8f2256382f97ad51e29ff71f702961af466c4616393f767455501e6aece9b8"
$ConsoleModeState = $null
$ExitCode = 1

New-Item -ItemType Directory -Force -Path $CacheRoot, $NodeRoot, $NpmCache, $PlaywrightBrowsers, $LogsDir | Out-Null

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

function Disable-ConsoleQuickEdit {
    if (-not ("FirstTestConsoleMode" -as [type])) {
        Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public static class FirstTestConsoleMode
{
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern IntPtr GetStdHandle(int nStdHandle);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool GetConsoleMode(IntPtr hConsoleHandle, out uint lpMode);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool SetConsoleMode(IntPtr hConsoleHandle, uint dwMode);
}
"@ | Out-Null
    }

    $StdInputHandle = -10
    $EnableQuickEditMode = [uint32]0x0040
    $EnableExtendedFlags = [uint32]0x0080
    $Handle = [FirstTestConsoleMode]::GetStdHandle($StdInputHandle)
    if ($Handle -eq [IntPtr]::Zero -or $Handle -eq [IntPtr](-1)) {
        throw "Cannot obtain the current console input handle."
    }

    $OriginalMode = [uint32]0
    if (-not [FirstTestConsoleMode]::GetConsoleMode($Handle, [ref]$OriginalMode)) {
        $Code = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        throw "Cannot read the current console input mode (Win32 error $Code)."
    }

    $AutomaticMode = $OriginalMode
    if (($AutomaticMode -band $EnableQuickEditMode) -ne 0) {
        $AutomaticMode = $AutomaticMode - $EnableQuickEditMode
    }
    $AutomaticMode = [uint32]($AutomaticMode -bor $EnableExtendedFlags)
    if (-not [FirstTestConsoleMode]::SetConsoleMode($Handle, $AutomaticMode)) {
        $Code = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        throw "Cannot disable QuickEdit for the current console (Win32 error $Code)."
    }

    return [pscustomobject]@{
        Handle = $Handle
        OriginalMode = $OriginalMode
    }
}

function Restore-ConsoleMode($State) {
    if ($null -eq $State) {
        return
    }
    if (-not [FirstTestConsoleMode]::SetConsoleMode($State.Handle, [uint32]$State.OriginalMode)) {
        $Code = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        Write-Step "WARNING: Could not restore the current console input mode (Win32 error $Code)."
    }
}

try {
    Set-Content -LiteralPath $LogPath -Value "Autopresenter first-test bootstrap" -Encoding UTF8
    if (-not [Environment]::Is64BitOperatingSystem) {
        throw "Windows x64 is required."
    }
    try {
        $ConsoleModeState = Disable-ConsoleQuickEdit
        Write-Step "Automatic console mode enabled for this launch."
    }
    catch {
        Write-Step ("WARNING: Could not change QuickEdit for this console; startup will continue. {0}" -f $_.Exception.Message)
        $ConsoleModeState = $null
    }
    Require-File $ConfigPath "Test configuration"
    Require-File (Join-Path $AgentDir "agent.mjs") "Presenter agent"
    Require-File (Join-Path $AgentDir "abort-utils.mjs") "Presenter abort utilities"
    Require-File (Join-Path $AgentDir "pacing.mjs") "Presenter pacing contract"
    Require-File (Join-Path $AgentDir "scenario-contract.mjs") "Presenter scenario contract"
    Require-File (Join-Path $AgentDir "package.json") "Pinned dependency manifest"
    Require-File (Join-Path $AgentDir "package-lock.json") "Pinned dependency lock"
    $Config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
    Write-Step "Persistent shared cache: $CacheRoot"

    if (-not (Test-Path -LiteralPath $NodeExe -PathType Leaf)) {
        Write-Step "Downloading portable Node.js $NodeVersion..."
        $Download = Join-Path $CacheRoot $NodeArchive
        Invoke-WebRequest -UseBasicParsing -Uri $NodeUrl -OutFile $Download
        $ActualSha = (Get-FileHash -LiteralPath $Download -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($ActualSha -ne $NodeSha256) {
            throw "Portable Node.js checksum mismatch."
        }
        $ExtractRoot = Join-Path $CacheRoot "node-extract"
        Remove-Item -LiteralPath $ExtractRoot -Recurse -Force -ErrorAction SilentlyContinue
        Expand-Archive -LiteralPath $Download -DestinationPath $ExtractRoot -Force
        Remove-Item -LiteralPath $NodeHome -Recurse -Force -ErrorAction SilentlyContinue
        Move-Item -LiteralPath (Join-Path $ExtractRoot "node-v$NodeVersion-win-x64") -Destination $NodeHome
        Remove-Item -LiteralPath $ExtractRoot -Recurse -Force
        Remove-Item -LiteralPath $Download -Force
    }
    else {
        Write-Step "Reusing portable Node.js $NodeVersion from the shared cache."
    }

    $NpmCli = Join-Path $NodeHome "node_modules\npm\bin\npm-cli.js"
    Require-File $NpmCli "Portable npm"
    $Env:CI = "true"
    $Env:NPM_CONFIG_AUDIT = "false"
    $Env:NPM_CONFIG_FUND = "false"
    $Env:NPM_CONFIG_PROGRESS = "false"
    $Env:NPM_CONFIG_UPDATE_NOTIFIER = "false"
    $Env:NPM_CONFIG_YES = "true"
    $LockHash = (Get-FileHash -LiteralPath (Join-Path $AgentDir "package-lock.json") -Algorithm SHA256).Hash.ToLowerInvariant()
    $DependencyKey = $LockHash.Substring(0, 20)
    $DependencyHome = Join-Path $CacheRoot "dependencies\$DependencyKey"
    $DependencyMarker = Join-Path $DependencyHome ".autopresenter-ready"
    $CachedPlaywright = Join-Path $DependencyHome "node_modules\playwright\package.json"
    $DependencyReady = (
        (Test-Path -LiteralPath $DependencyMarker -PathType Leaf) -and
        (Test-Path -LiteralPath $CachedPlaywright -PathType Leaf) -and
        ((Get-Content -LiteralPath $DependencyMarker -Raw).Trim() -eq $LockHash)
    )
    if (-not $DependencyReady) {
        Write-Step "Installing pinned presenter dependencies into the shared cache..."
        Remove-Item -LiteralPath $DependencyHome -Recurse -Force -ErrorAction SilentlyContinue
        New-Item -ItemType Directory -Force -Path $DependencyHome | Out-Null
        Copy-Item -LiteralPath (Join-Path $AgentDir "package.json") -Destination $DependencyHome
        Copy-Item -LiteralPath (Join-Path $AgentDir "package-lock.json") -Destination $DependencyHome
        & $NodeExe $NpmCli ci --prefix $DependencyHome --cache $NpmCache --no-audit --no-fund
        if ($LASTEXITCODE -ne 0) { throw "npm ci failed with exit code $LASTEXITCODE." }
        Set-Content -LiteralPath $DependencyMarker -Value $LockHash -Encoding ASCII
    }
    else {
        Write-Step "Reusing pinned presenter dependencies from the shared cache."
    }

    $Env:AUTOPRESENTER_DEPENDENCY_ROOT = $DependencyHome
    $Env:PLAYWRIGHT_BROWSERS_PATH = $PlaywrightBrowsers
    $Env:PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT = "120000"
    $PlaywrightCli = Join-Path $DependencyHome "node_modules\playwright\cli.js"
    Require-File $PlaywrightCli "Playwright CLI"
    $BrowsersJsonPath = Join-Path $DependencyHome "node_modules\playwright-core\browsers.json"
    Require-File $BrowsersJsonPath "Playwright browser registry"
    $BrowsersJson = Get-Content -LiteralPath $BrowsersJsonPath -Raw | ConvertFrom-Json
    $ChromiumRevision = [string](($BrowsersJson.browsers | Where-Object { $_.name -eq "chromium" } | Select-Object -First 1).revision)
    if ([string]::IsNullOrWhiteSpace($ChromiumRevision)) {
        throw "Pinned Chromium revision is missing from Playwright browser registry."
    }
    $BrowserMarker = Join-Path $PlaywrightBrowsers ("ready-" + $DependencyKey)
    $PinnedBrowserHome = Join-Path $PlaywrightBrowsers ("chromium-" + $ChromiumRevision)
    $CachedBrowserExe = Get-ChildItem -LiteralPath $PinnedBrowserHome -Filter "chrome.exe" -File -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    $BrowserReady = (
        (Test-Path -LiteralPath $BrowserMarker -PathType Leaf) -and
        ($null -ne $CachedBrowserExe) -and
        ((Get-Content -LiteralPath $BrowserMarker -Raw).Trim() -eq $LockHash)
    )
    if (-not $BrowserReady) {
        Write-Step "Installing the pinned Playwright-managed browser into the shared cache..."
        & $NodeExe $PlaywrightCli install --no-shell chromium
        if ($LASTEXITCODE -ne 0) { throw "Playwright browser install failed with exit code $LASTEXITCODE." }
        Set-Content -LiteralPath $BrowserMarker -Value $LockHash -Encoding ASCII
    }
    else {
        Write-Step "Reusing the Playwright-managed browser from the shared cache."
    }

    $Env:AUTOPRESENTER_RELAY_URL = [string]$Config.relay_url
    $Env:AUTOPRESENTER_STAGE_URL = [string]$Config.stage_url
    $Env:AUTOPRESENTER_AGENT_TOKEN = [string]$Config.agent_token
    $Env:AUTOPRESENTER_AGENT_ID = [string]$Config.agent_id
    $Env:AUTOPRESENTER_FULLSCREEN = "1"

    Write-Step "READY. Opening the 1920x1080 demonstrator..."
    Write-Host ""
    Write-Host "Use the PHONE link and choose a scenario." -ForegroundColor Green
    Write-Host "Use 'Close presentation' on the phone to finish everything." -ForegroundColor DarkGray
    Write-Host ""
    $PreviousErrorActionPreference = $ErrorActionPreference
    try {
        # Windows PowerShell converts native stderr lines (including harmless
        # Node warnings) into ErrorRecord objects. Do not turn those lines into
        # a terminating bootstrap failure; the native process exit code owns
        # success/failure here.
        $ErrorActionPreference = "Continue"
        & $NodeExe (Join-Path $AgentDir "agent.mjs") 2>&1 | Tee-Object -FilePath $LogPath -Append
        $AgentExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    if ($AgentExitCode -ne 0) {
        throw "Presenter agent failed with exit code $AgentExitCode."
    }
    $ExitCode = 0
}
catch {
    $Message = "ERROR: {0}" -f $_.Exception.Message
    Write-Host $Message -ForegroundColor Red
    Add-Content -LiteralPath $LogPath -Value $Message -Encoding UTF8
    $ExitCode = 1
}
finally {
    Restore-ConsoleMode $ConsoleModeState
}

exit $ExitCode
