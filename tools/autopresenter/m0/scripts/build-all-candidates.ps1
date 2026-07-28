[CmdletBinding()]
param(
    [string]$OutputRoot
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$builder = Join-Path $PSScriptRoot 'build-candidate.ps1'
$candidateIds = @(
    'current-control',
    'pre-cft-compat'
)

foreach ($candidateId in $candidateIds) {
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
        & $builder -CandidateId $candidateId
    } else {
        & $builder -CandidateId $candidateId -OutputRoot $OutputRoot
    }
    if ($LASTEXITCODE -ne 0) {
        throw "Candidate build failed: $candidateId"
    }
}
