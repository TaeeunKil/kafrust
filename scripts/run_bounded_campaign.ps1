[CmdletBinding()]
param(
    [string]$RunId,
    [string]$OutputRoot,
    [string]$Config = "scripts/bounded_campaign_profiles.json",
    [string]$OnlyPhase,
    [switch]$Plan
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$wslRepo = (& wsl.exe wslpath -a $repoRoot).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($wslRepo)) {
    throw "could not translate the repository path into WSL"
}

$arguments = @(
    "--cd", $wslRepo,
    "--",
    "python3",
    "scripts/run_bounded_campaign.py",
    "--config",
    $Config
)
if ($RunId) {
    $arguments += @("--run-id", $RunId)
}
if ($OutputRoot) {
    $wslOutput = (& wsl.exe wslpath -a $OutputRoot).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($wslOutput)) {
        throw "could not translate the output path into WSL"
    }
    $arguments += @("--output-root", $wslOutput)
}
if ($OnlyPhase) {
    $arguments += @("--only-phase", $OnlyPhase)
}
if ($Plan) {
    $arguments += "--plan"
}

& wsl.exe @arguments
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
