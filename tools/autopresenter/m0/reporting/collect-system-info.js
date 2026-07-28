#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const crypto = require('node:crypto');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { validateSystemInfo } = require('./validate');
const { machineAccountFingerprint } = require('../src/machine-provenance');

const POWERSHELL = String.raw`
$ErrorActionPreference = 'Stop'
$os = Get-CimInstance Win32_OperatingSystem
$displayVersion = ''
try { $displayVersion = [string](Get-ItemPropertyValue 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion' DisplayVersion) } catch {}
$computer = Get-CimInstance Win32_ComputerSystem
$gpus = @(Get-CimInstance Win32_VideoController | Select-Object Name, DriverVersion)
$screen = $null
try {
  Add-Type -AssemblyName System.Windows.Forms
  $bounds = [System.Windows.Forms.Screen]::PrimaryScreen.Bounds
  $screen = @{ width = $bounds.Width; height = $bounds.Height }
} catch {}
$dpi = 96
try {
  $value = Get-ItemPropertyValue 'HKCU:\Control Panel\Desktop' LogPixels -ErrorAction Stop
  if ($value) { $dpi = [int]$value }
} catch {}
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$administrator = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$defender = 'unknown'
try {
  $mp = Get-MpComputerStatus
  $defender = if ($mp.AntivirusEnabled) { 'enabled' } else { 'disabled' }
} catch {}
$appLocker = 'unknown'
try {
  $service = Get-Service AppIDSvc -ErrorAction Stop
  $appLocker = if ($service.Status -eq 'Running') { 'enabled' } else { 'disabled' }
} catch {}
$wdac = 'unknown'
try {
  $ci = Get-CimInstance -Namespace root\Microsoft\Windows\CI -ClassName MSFT_CIPolicy -ErrorAction Stop
  if ($ci) { $wdac = 'enabled' } else { $wdac = 'disabled' }
} catch {}
$nodeOnPath = [bool](Get-Command node.exe -ErrorAction SilentlyContinue)
$chromePaths = @(
  "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
  "$([Environment]::GetEnvironmentVariable('ProgramFiles(x86)'))\Google\Chrome\Application\chrome.exe",
  "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
)
$installedChrome = [bool]($chromePaths | Where-Object { Test-Path $_ } | Select-Object -First 1)
$playwrightCache = Test-Path "$env:LOCALAPPDATA\ms-playwright"
$machineGuid = [string](Get-ItemPropertyValue 'HKLM:\SOFTWARE\Microsoft\Cryptography' MachineGuid)
$userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
[ordered]@{
  productName = if ($os.Caption -match 'Windows 10' -and [int]$os.BuildNumber -lt 22000) { 'Windows 10' } else { [string]$os.Caption }
  version = [string]$os.Caption
  build = [string]$os.BuildNumber
  winver = $displayVersion
  architecture = if ([Environment]::Is64BitOperatingSystem) { 'x64' } else { 'x86' }
  manufacturer = [string]$computer.Manufacturer
  model = [string]$computer.Model
  ramBytes = [int64]$computer.TotalPhysicalMemory
  gpus = $gpus
  display = $screen
  scalingPercent = [math]::Round(($dpi / 96.0) * 100)
  language = [Globalization.CultureInfo]::CurrentUICulture.Name
  locale = [Globalization.CultureInfo]::CurrentCulture.Name
  timezone = [System.TimeZoneInfo]::Local.Id
  administrator = [bool]$administrator
  installedNode = [bool]$nodeOnPath
  installedChrome = [bool]$installedChrome
  installedPlaywright = [bool]$playwrightCache
  defender = $defender
  appLocker = $appLocker
  wdac = $wdac
  machineGuid = $machineGuid
  userSid = $userSid
} | ConvertTo-Json -Depth 8 -Compress
`;

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function parseArgs(argv) {
  const result = {};
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i];
    const value = argv[i + 1];
    if (!name?.startsWith('--') || value === undefined) {
      throw new Error('Usage: collect-system-info.js --portable-root ROOT --path-matrix JSON --self-test JSON --output JSON');
    }
    result[name.slice(2)] = value;
  }
  for (const required of ['portable-root', 'path-matrix', 'self-test', 'output']) {
    if (!result[required]) throw new Error(`--${required} is required`);
  }
  return result;
}

function queryWindows() {
  if (process.platform !== 'win32' || process.arch !== 'x64') {
    throw new Error('SYSTEM_INFO_TARGET_REQUIRED: collector must run on target Windows x64');
  }
  const encoded = Buffer.from(POWERSHELL, 'utf16le').toString('base64');
  const result = spawnSync(
    'powershell.exe',
    ['-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', encoded],
    { encoding: 'utf8', timeout: 30000, windowsHide: true },
  );
  if (result.error || result.status !== 0) {
    throw new Error(`SYSTEM_INFO_PROBE_FAILED: ${String(result.error?.message || result.stderr).slice(0, 1000)}`);
  }
  return JSON.parse(result.stdout.trim());
}

function buildSystemInfo({
  raw,
  pathVariants,
  portableRoot,
  devicePixelRatio,
  sourceCandidateId,
}) {
  const display = raw.display || {};
  const value = {
    schemaVersion: 1,
    collectedAt: new Date().toISOString(),
    provenance: {
      machineAccountFingerprint: machineAccountFingerprint(
        raw.machineGuid,
        raw.userSid,
      ),
      sourceCandidateId,
    },
    os: {
      productName: raw.productName,
      version: raw.version,
      build: raw.build,
      winver: raw.winver,
      architecture: raw.architecture,
    },
    hardware: {
      manufacturer: raw.manufacturer,
      model: raw.model,
      ramBytes: raw.ramBytes,
      gpu: (Array.isArray(raw.gpus) ? raw.gpus : [raw.gpus]).filter(Boolean).map((gpu) => ({
        name: gpu.Name,
        driverVersion: gpu.DriverVersion,
      })),
    },
    display: {
      width: display.width,
      height: display.height,
      scalingPercent: raw.scalingPercent,
      devicePixelRatio,
    },
    locale: {
      language: raw.language,
      locale: raw.locale,
      timezone: raw.timezone,
    },
    user: {
      standardUser: !raw.administrator,
      administrator: raw.administrator,
      accountType: raw.administrator ? 'administrator' : 'standard',
    },
    launch: { portablePath: path.resolve(portableRoot) },
    baseline: {
      installedNode: raw.installedNode,
      installedChrome: raw.installedChrome,
      installedPlaywright: raw.installedPlaywright,
    },
    security: {
      defender: raw.defender,
      appLocker: raw.appLocker,
      wdac: raw.wdac,
      collectedWithoutElevation: !raw.administrator,
    },
    pathVariants,
  };
  validateSystemInfo(value);
  return value;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const pathVariants = JSON.parse(fs.readFileSync(args['path-matrix'], 'utf8'));
  const selfTest = fs.existsSync(args['self-test'])
    ? JSON.parse(fs.readFileSync(args['self-test'], 'utf8'))
    : null;
  const devicePixelRatio = selfTest?.platform?.devicePixelRatio;
  const measuredDevicePixelRatio = Number.isFinite(devicePixelRatio)
    ? devicePixelRatio
    : null;
  const output = path.resolve(args.output);
  const evidenceRoot = path.dirname(output);
  const candidateId = JSON.parse(
    fs.readFileSync(path.join(path.resolve(args['portable-root']), 'VERSIONS.json'), 'utf8'),
  ).candidateId;
  const retainedVariants = pathVariants.map((entry) => {
    const source = path.join(path.resolve(args['portable-root']), ...entry.selfTest.split('/'));
    if (!fs.existsSync(source)) throw new Error(`Path self-test is absent: ${entry.selfTest}`);
    if (sha256(source) !== entry.selfTestSha256) {
      throw new Error(`Path self-test checksum mismatch: ${entry.selfTest}`);
    }
    const destinationRelative = `path-matrix/${candidateId}/${path.basename(entry.selfTest)}`;
    const destination = path.join(evidenceRoot, ...destinationRelative.split('/'));
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    fs.copyFileSync(source, destination);
    return { ...entry, selfTest: destinationRelative };
  });
  const value = buildSystemInfo({
    raw: queryWindows(),
    pathVariants: retainedVariants,
    portableRoot: args['portable-root'],
    devicePixelRatio: measuredDevicePixelRatio,
    sourceCandidateId: candidateId,
  });
  fs.mkdirSync(evidenceRoot, { recursive: true });
  fs.writeFileSync(output, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${error.stack || error.message}\n`);
    process.exitCode = 2;
  }
}

module.exports = { buildSystemInfo };
