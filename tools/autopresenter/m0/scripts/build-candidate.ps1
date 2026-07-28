[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[a-z0-9][a-z0-9-]*$')]
    [string]$CandidateId,

    [string]$OutputRoot
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Content
    )

    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $encoding)
}

function Get-Sha256 {
    param([Parameter(Mandatory = $true)][string]$Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-ReleaseRelativePath {
    param(
        [Parameter(Mandatory = $true)][string]$ReleaseRoot,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $rootPrefix = $ReleaseRoot.TrimEnd('\') + '\'
    if (-not $Path.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Path is outside release root: $Path"
    }
    return $Path.Substring($rootPrefix.Length).Replace('\', '/')
}

function Restore-EnvironmentValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [AllowNull()][string]$PreviousValue
    )

    if ($null -eq $PreviousValue) {
        Remove-Item -LiteralPath ("Env:" + $Name) -ErrorAction SilentlyContinue
    } else {
        Set-Item -LiteralPath ("Env:" + $Name) -Value $PreviousValue
    }
}

if ($env:OS -ne 'Windows_NT') {
    throw 'Candidate packages must be built on Windows so the managed Windows browser is downloaded.'
}
if (-not [Environment]::Is64BitOperatingSystem) {
    throw 'Candidate packages must be built on Windows x64.'
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..\..\..')).Path
$m0Root = Join-Path $repoRoot 'tools\autopresenter\m0'
$candidateRoot = Join-Path $m0Root ('candidates\' + $CandidateId)
$candidateManifestPath = Join-Path $candidateRoot 'candidate.json'
$candidatePackagePath = Join-Path $candidateRoot 'package.json'
$candidateLockPath = Join-Path $candidateRoot 'package-lock.json'
$runtimeSourcePath = Join-Path $m0Root 'src'
$fixtureSourcePath = Join-Path $m0Root 'fixture'
$reportingSourcePath = Join-Path $m0Root 'reporting'
$schemasSourcePath = Join-Path $m0Root 'schemas'
$templateRoot = Join-Path $m0Root 'release-m0\templates'

foreach ($requiredPath in @(
    $candidateManifestPath,
    $candidatePackagePath,
    $candidateLockPath,
    $runtimeSourcePath,
    $fixtureSourcePath,
    $reportingSourcePath,
    $schemasSourcePath,
    (Join-Path $runtimeSourcePath 'run-suite.js'),
    (Join-Path $runtimeSourcePath 'self-test.js'),
    (Join-Path $fixtureSourcePath 'index.html'),
    (Join-Path $fixtureSourcePath 'zavtra\index.html'),
    (Join-Path $templateRoot 'start.cmd.in'),
    (Join-Path $templateRoot 'self-test.cmd.in'),
    (Join-Path $templateRoot 'path-matrix.cmd.in'),
    (Join-Path $templateRoot 'system-info.cmd.in'),
    (Join-Path $templateRoot 'prepare-evidence.cmd.in'),
    (Join-Path $templateRoot 'finalize-evidence.cmd.in')
)) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Required source is absent: $requiredPath"
    }
}

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $m0Root 'release-m0\out'
} elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot $OutputRoot
}
$OutputRoot = [System.IO.Path]::GetFullPath($OutputRoot)

$dirtyLines = @(& git -C $repoRoot status --porcelain --untracked-files=all)
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to inspect the repository state with git.'
}
if ($dirtyLines.Count -ne 0) {
    throw 'Refusing to package a dirty tracked source tree. Commit or restore the source first.'
}
$sourceCommit = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $sourceCommit -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to resolve the exact source commit.'
}

$candidate = Get-Content -LiteralPath $candidateManifestPath -Raw | ConvertFrom-Json
if ($candidate.schemaVersion -ne 1 -or $candidate.id -ne $CandidateId) {
    throw "Candidate manifest identity mismatch: $candidateManifestPath"
}
if ($candidate.target.os -ne 'windows' -or $candidate.target.architecture -ne 'x64') {
    throw 'Only the exact Windows x64 M0 candidate target is accepted.'
}
if ($candidate.node.version -ne '22.12.0' -or
    $candidate.node.sha256 -ne '2b8f2256382f97ad51e29ff71f702961af466c4616393f767455501e6aece9b8') {
    throw 'Portable Node must remain exactly v22.12.0 win-x64 with its official archive SHA-256.'
}
if ($CandidateId -eq 'current-control') {
    if ($candidate.playwright.version -ne '1.61.1' -or
        $candidate.playwright.corePackage.version -ne '1.61.1' -or
        $candidate.managedBrowser.revision -ne '1228' -or
        $candidate.managedBrowser.version -ne '149.0.7827.55' -or
        $candidate.managedBrowser.executableRelativePath -ne 'browsers/chromium-1228/chrome-win64/chrome.exe') {
        throw 'current-control must remain the exact Node 22.12.0 / Playwright 1.61.1 / browser 149.0.7827.55 revision 1228 candidate.'
    }
} elseif ($CandidateId -eq 'pre-cft-compat') {
    if ($candidate.playwright.version -ne '1.54.2' -or
        $candidate.playwright.corePackage.version -ne '1.54.2' -or
        $candidate.managedBrowser.revision -ne '1181' -or
        $candidate.managedBrowser.version -ne '139.0.7258.5' -or
        $candidate.managedBrowser.executableRelativePath -ne 'browsers/chromium-1181/chrome-win/chrome.exe') {
        throw 'pre-cft-compat must remain the exact Node 22.12.0 / Playwright 1.54.2 / browser 139.0.7258.5 revision 1181 candidate.'
    }
} else {
    throw "Unknown M0 candidate: $CandidateId"
}
if ($candidate.runtimePolicy.playwrightBrowsersPath -ne 'browsers' -or
    $candidate.runtimePolicy.downloadAllowedAtRuntime -ne $false -or
    $candidate.runtimePolicy.allowGlobalBrowserCache -ne $false -or
    $candidate.runtimePolicy.allowSystemBrowser -ne $false -or
    $candidate.runtimePolicy.allowBrowserChannel -ne $false -or
    $candidate.runtimePolicy.allowNpmOrNpxAtRuntime -ne $false -or
    $candidate.runtimePolicy.requiresAdministrator -ne $false -or
    $candidate.runtimePolicy.changesExecutionPolicy -ne $false) {
    throw 'Candidate runtime policy is not hermetic and fail-closed.'
}
if ($candidate.playwright.packageLockSha256 -ne (Get-Sha256 -Path $candidateLockPath) -or
    $candidate.managedBrowser.executableSha256.resolution -ne 'computed-from-packaged-executable-at-build' -or
    $candidate.managedBrowser.executableSha256.recordedIn -ne 'VERSIONS.json' -or
    $candidate.launch.headless -ne $false -or
    $null -ne $candidate.launch.browserChannel -or
    @($candidate.launch.arguments).Count -ne 0 -or
    @($candidate.launch.profileModes).Count -ne 2 -or
    $candidate.launch.profileModes[0] -ne 'fresh' -or
    $candidate.launch.profileModes[1] -ne 'persistent' -or
    $candidate.launch.viewport.width -ne 430 -or
    $candidate.launch.viewport.height -ne 932) {
    throw 'Candidate manifest does not exactly lock the package, headed launch, profile, and viewport contract.'
}

$candidateOutputRoot = Join-Path $OutputRoot $CandidateId
if (Test-Path -LiteralPath $candidateOutputRoot) {
    throw "Candidate output already exists; choose an empty OutputRoot: $candidateOutputRoot"
}

$cacheRoot = Join-Path $OutputRoot '_cache'
$nodeArchivePath = Join-Path $cacheRoot $candidate.node.archiveFile
$nodeExtractRoot = Join-Path $candidateOutputRoot '_node'
$releaseRoot = Join-Path $candidateOutputRoot 'Autopresenter-Win10-x64'
$releaseRuntimeRoot = Join-Path $releaseRoot 'runtime'
$releaseAppRoot = Join-Path $releaseRoot 'app'
$releaseBrowsersRoot = Join-Path $releaseRoot 'browsers'
$releaseDataRoot = Join-Path $releaseRoot 'data\browser-profile'
$releaseLogsRoot = Join-Path $releaseRoot 'logs'

New-Item -ItemType Directory -Path $cacheRoot -Force | Out-Null
New-Item -ItemType Directory -Path $candidateOutputRoot | Out-Null
New-Item -ItemType Directory -Path $nodeExtractRoot | Out-Null
New-Item -ItemType Directory -Path $releaseRuntimeRoot | Out-Null
New-Item -ItemType Directory -Path $releaseAppRoot | Out-Null
New-Item -ItemType Directory -Path $releaseBrowsersRoot | Out-Null
New-Item -ItemType Directory -Path $releaseDataRoot | Out-Null
New-Item -ItemType Directory -Path $releaseLogsRoot | Out-Null

$candidateLock = Get-Content -LiteralPath $candidateLockPath -Raw | ConvertFrom-Json
$lockedRoot = $candidateLock.packages.PSObject.Properties[''].Value
$lockedPlaywright = $candidateLock.packages.PSObject.Properties['node_modules/playwright'].Value
$lockedCore = $candidateLock.packages.PSObject.Properties['node_modules/playwright-core'].Value
if ($candidateLock.lockfileVersion -ne 3 -or
    $lockedRoot.dependencies.playwright -ne $candidate.playwright.version -or
    $lockedPlaywright.version -ne $candidate.playwright.version -or
    $lockedPlaywright.integrity -ne $candidate.playwright.package.integrity -or
    $lockedCore.version -ne $candidate.playwright.corePackage.version -or
    $lockedCore.integrity -ne $candidate.playwright.corePackage.integrity) {
    throw 'Candidate package-lock.json does not exactly match the pinned Playwright package metadata.'
}

if (-not (Test-Path -LiteralPath $nodeArchivePath)) {
    Write-Host "Downloading exact portable Node archive for $CandidateId..."
    Invoke-WebRequest -UseBasicParsing -Uri $candidate.node.downloadUrl -OutFile $nodeArchivePath
}
$nodeArchiveHash = Get-Sha256 -Path $nodeArchivePath
if ($nodeArchiveHash -ne $candidate.node.sha256) {
    throw "Portable Node archive SHA-256 mismatch: expected $($candidate.node.sha256), got $nodeArchiveHash"
}

Expand-Archive -LiteralPath $nodeArchivePath -DestinationPath $nodeExtractRoot
$nodeDistributionRoot = Join-Path $nodeExtractRoot ('node-v' + $candidate.node.version + '-win-x64')
$buildNodePath = Join-Path $nodeDistributionRoot 'node.exe'
$buildNpmPath = Join-Path $nodeDistributionRoot 'npm.cmd'
if (-not (Test-Path -LiteralPath $buildNodePath) -or -not (Test-Path -LiteralPath $buildNpmPath)) {
    throw 'The verified portable Node archive did not contain node.exe and npm.cmd at the pinned paths.'
}
Copy-Item -LiteralPath $buildNodePath -Destination (Join-Path $releaseRuntimeRoot 'node.exe')

Copy-Item -LiteralPath $runtimeSourcePath -Destination (Join-Path $releaseAppRoot 'src') -Recurse
Copy-Item -LiteralPath $fixtureSourcePath -Destination (Join-Path $releaseAppRoot 'fixture') -Recurse
Copy-Item -LiteralPath $reportingSourcePath -Destination (Join-Path $releaseAppRoot 'reporting') -Recurse
Copy-Item -LiteralPath $schemasSourcePath -Destination (Join-Path $releaseAppRoot 'schemas') -Recurse
Copy-Item -LiteralPath $candidatePackagePath -Destination (Join-Path $releaseAppRoot 'package.json')
Copy-Item -LiteralPath $candidateLockPath -Destination (Join-Path $releaseAppRoot 'package-lock.json')
Copy-Item -LiteralPath $candidateManifestPath -Destination (Join-Path $releaseRoot 'CANDIDATE.json')

$previousSkipDownload = $env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD
try {
    $env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = '1'
    Push-Location $releaseAppRoot
    try {
        & $buildNpmPath ci --omit=dev --ignore-scripts --no-audit --no-fund
        if ($LASTEXITCODE -ne 0) {
            throw "Pinned npm ci failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} finally {
    Restore-EnvironmentValue -Name 'PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD' -PreviousValue $previousSkipDownload
}

$installedPlaywrightPackage = Get-Content -LiteralPath (Join-Path $releaseAppRoot 'node_modules\playwright\package.json') -Raw | ConvertFrom-Json
$installedCorePackagePath = Join-Path $releaseAppRoot 'node_modules\playwright-core\package.json'
$installedCorePackage = Get-Content -LiteralPath $installedCorePackagePath -Raw | ConvertFrom-Json
$installedBrowsersMetadataPath = Join-Path $releaseAppRoot 'node_modules\playwright-core\browsers.json'
$installedBrowsersMetadata = Get-Content -LiteralPath $installedBrowsersMetadataPath -Raw | ConvertFrom-Json
$installedChromium = @($installedBrowsersMetadata.browsers | Where-Object { $_.name -eq 'chromium' })

if ($installedPlaywrightPackage.version -ne $candidate.playwright.version -or
    $installedCorePackage.version -ne $candidate.playwright.corePackage.version) {
    throw 'Installed Playwright packages do not match the candidate manifest.'
}
if ($installedChromium.Count -ne 1 -or
    $installedChromium[0].revision -ne $candidate.managedBrowser.revision -or
    $installedChromium[0].browserVersion -ne $candidate.managedBrowser.version) {
    throw 'Installed playwright-core browser metadata does not match the exact candidate browser.'
}

$browserInstallCli = Join-Path $releaseAppRoot 'node_modules\playwright-core\cli.js'
if (-not (Test-Path -LiteralPath $browserInstallCli)) {
    throw "Pinned Playwright browser installer is absent: $browserInstallCli"
}

$previousBrowsersPath = $env:PLAYWRIGHT_BROWSERS_PATH
$previousSkipDownload = $env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD
try {
    $env:PLAYWRIGHT_BROWSERS_PATH = $releaseBrowsersRoot
    Remove-Item -LiteralPath 'Env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD' -ErrorAction SilentlyContinue
    & $buildNodePath $browserInstallCli install chromium --no-shell
    if ($LASTEXITCODE -ne 0) {
        throw "Pinned Playwright managed-browser install failed with exit code $LASTEXITCODE"
    }
} finally {
    Restore-EnvironmentValue -Name 'PLAYWRIGHT_BROWSERS_PATH' -PreviousValue $previousBrowsersPath
    Restore-EnvironmentValue -Name 'PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD' -PreviousValue $previousSkipDownload
}

$browserExecutableRelative = $candidate.managedBrowser.executableRelativePath.Replace('/', '\')
$browserExecutablePath = Join-Path $releaseRoot $browserExecutableRelative
if (-not (Test-Path -LiteralPath $browserExecutablePath -PathType Leaf)) {
    throw "Exact packaged browser executable is absent; no fallback is permitted: $browserExecutablePath"
}
$expectedRegistryRoot = Join-Path $releaseBrowsersRoot $candidate.managedBrowser.registryDirectory
$resolvedBrowserExecutable = (Resolve-Path -LiteralPath $browserExecutablePath).Path
$resolvedRegistryRoot = (Resolve-Path -LiteralPath $expectedRegistryRoot).Path.TrimEnd('\') + '\'
if (-not $resolvedBrowserExecutable.StartsWith($resolvedRegistryRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'Managed browser executable escaped its exact packaged revision directory.'
}

$browserExecutableForCmd = $browserExecutableRelative
$startTemplate = Get-Content -LiteralPath (Join-Path $templateRoot 'start.cmd.in') -Raw
$selfTestTemplate = Get-Content -LiteralPath (Join-Path $templateRoot 'self-test.cmd.in') -Raw
$pathMatrixTemplate = Get-Content -LiteralPath (Join-Path $templateRoot 'path-matrix.cmd.in') -Raw
$systemInfoTemplate = Get-Content -LiteralPath (Join-Path $templateRoot 'system-info.cmd.in') -Raw
$prepareEvidenceTemplate = Get-Content -LiteralPath (Join-Path $templateRoot 'prepare-evidence.cmd.in') -Raw
$finalizeEvidenceTemplate = Get-Content -LiteralPath (Join-Path $templateRoot 'finalize-evidence.cmd.in') -Raw
foreach ($template in @($startTemplate, $selfTestTemplate)) {
    if ($template -notmatch 'PLAYWRIGHT_BROWSERS_PATH' -or
        $template -notmatch 'PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1' -or
        $template -match '(?i)\b(?:npm|npx)\b') {
        throw 'Launcher template violates the no-download/no-npm runtime contract.'
    }
}
$startCmd = $startTemplate.Replace('@@CANDIDATE_ID@@', $CandidateId).Replace('@@BROWSER_EXECUTABLE_WINDOWS@@', $browserExecutableForCmd)
$selfTestCmd = $selfTestTemplate.Replace('@@BROWSER_EXECUTABLE_WINDOWS@@', $browserExecutableForCmd)
Write-Utf8NoBom -Path (Join-Path $releaseRoot 'start.cmd') -Content $startCmd
Write-Utf8NoBom -Path (Join-Path $releaseRoot 'self-test.cmd') -Content $selfTestCmd
Write-Utf8NoBom -Path (Join-Path $releaseRoot 'path-matrix.cmd') -Content $pathMatrixTemplate
Write-Utf8NoBom -Path (Join-Path $releaseRoot 'system-info.cmd') -Content $systemInfoTemplate
Write-Utf8NoBom -Path (Join-Path $releaseRoot 'prepare-evidence.cmd') -Content $prepareEvidenceTemplate
Write-Utf8NoBom -Path (Join-Path $releaseRoot 'finalize-evidence.cmd') -Content $finalizeEvidenceTemplate

$portableNodePath = Join-Path $releaseRuntimeRoot 'node.exe'
$packageLockPath = Join-Path $releaseAppRoot 'package-lock.json'
$versions = [ordered]@{
    schemaVersion = 1
    candidateId = $CandidateId
    target = [ordered]@{
        os = 'windows'
        architecture = 'x64'
        compatibilityGate = 'Windows 10 exact-machine evidence required'
    }
    node = [ordered]@{
        version = $candidate.node.version
        architecture = 'x64'
        executableRelativePath = 'runtime/node.exe'
        executableSha256 = Get-Sha256 -Path $portableNodePath
        sourceArchive = $candidate.node.archiveFile
        sourceArchiveSha256 = $nodeArchiveHash
    }
    playwright = [ordered]@{
        version = $installedPlaywrightPackage.version
        packageIntegrity = $candidate.playwright.package.integrity
        coreVersion = $installedCorePackage.version
        corePackageIntegrity = $candidate.playwright.corePackage.integrity
    }
    browser = [ordered]@{
        name = $candidate.managedBrowser.name
        version = $candidate.managedBrowser.version
        revision = $candidate.managedBrowser.revision
        executableRelativePath = $candidate.managedBrowser.executableRelativePath
        executableSha256 = Get-Sha256 -Path $browserExecutablePath
    }
    packageLock = [ordered]@{
        path = 'app/package-lock.json'
        sha256 = Get-Sha256 -Path $packageLockPath
    }
    runtimePolicy = $candidate.runtimePolicy
}
$versionsPath = Join-Path $releaseRoot 'VERSIONS.json'
Write-Utf8NoBom -Path $versionsPath -Content (($versions | ConvertTo-Json -Depth 10) + "`n")

$payloadFiles = @(
    Get-ChildItem -LiteralPath $releaseRoot -File -Recurse |
        Sort-Object -Property FullName |
        ForEach-Object {
            [ordered]@{
                path = Get-ReleaseRelativePath -ReleaseRoot $releaseRoot -Path $_.FullName
                sha256 = Get-Sha256 -Path $_.FullName
                sizeBytes = $_.Length
            }
        }
)
$releaseManifest = [ordered]@{
    schemaVersion = 1
    scope = 'm0-windows-compatibility-candidate'
    candidateId = $CandidateId
    sourceCommit = $sourceCommit
    builtAtUtc = [DateTime]::UtcNow.ToString('o')
    scenarioDataSha256 = $null
    m0ReportSha256 = $null
    candidateManifestSha256 = Get-Sha256 -Path (Join-Path $releaseRoot 'CANDIDATE.json')
    files = $payloadFiles
    generatedFiles = @('RELEASE-MANIFEST.json', 'SHA256SUMS.txt')
    checksumPolicy = 'SHA256SUMS covers every release file except SHA256SUMS.txt itself.'
}
$releaseManifestPath = Join-Path $releaseRoot 'RELEASE-MANIFEST.json'
Write-Utf8NoBom -Path $releaseManifestPath -Content (($releaseManifest | ConvertTo-Json -Depth 10) + "`n")

$checksumLines = @(
    Get-ChildItem -LiteralPath $releaseRoot -File -Recurse |
        Where-Object { $_.Name -ne 'SHA256SUMS.txt' } |
        Sort-Object -Property FullName |
        ForEach-Object {
            $relativePath = Get-ReleaseRelativePath -ReleaseRoot $releaseRoot -Path $_.FullName
            (Get-Sha256 -Path $_.FullName) + '  ' + $relativePath
        }
)
Write-Utf8NoBom -Path (Join-Path $releaseRoot 'SHA256SUMS.txt') -Content (($checksumLines -join "`n") + "`n")

$zipPath = Join-Path $candidateOutputRoot ($CandidateId + '-Autopresenter-Win10-x64.zip')
Compress-Archive -LiteralPath $releaseRoot -DestinationPath $zipPath -CompressionLevel Optimal
$zipHash = Get-Sha256 -Path $zipPath
Write-Utf8NoBom -Path ($zipPath + '.sha256') -Content ($zipHash + '  ' + [System.IO.Path]::GetFileName($zipPath) + "`n")

Write-Host ''
Write-Host "Built exact M0 candidate: $CandidateId"
Write-Host "Source commit: $sourceCommit"
Write-Host "ZIP: $zipPath"
Write-Host "ZIP SHA-256: $zipHash"
Write-Host 'The package still requires the target Windows 10 compatibility run; this build is not M0 PASS evidence.'
