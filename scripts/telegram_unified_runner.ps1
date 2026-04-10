param(
    [string]$PythonExe = "python",
    [ValidateSet("collect", "publish", "pipeline")]
    [string]$Mode = "pipeline",
    [string]$RepoRoot = "",
    [string]$Workspace = "",
    [string]$Profile = "cybertruck",
    [string]$Priority = "",
    [string]$Proxy = "",
    [switch]$UseSystemProxy,
    [switch]$ForceDirect,
    [string]$TelegramChatId = "",
    [int]$Limit = 0,
    [string]$UploadPlatforms = "",
    [string]$ExtraArgsJson = ""
)

$ErrorActionPreference = "Stop"
$root = if ($RepoRoot) { [System.IO.Path]::GetFullPath($RepoRoot) } else { Split-Path -Parent $PSScriptRoot }
$env:PYTHONPATH = "$root\src"

if ($Proxy) {
    $env:CYBERCAR_PROXY = $Proxy
    Remove-Item Env:CYBERCAR_USE_SYSTEM_PROXY -ErrorAction SilentlyContinue
}
elseif ($UseSystemProxy) {
    $env:CYBERCAR_USE_SYSTEM_PROXY = "1"
    Remove-Item Env:CYBERCAR_PROXY -ErrorAction SilentlyContinue
}
elseif ($ForceDirect) {
    Remove-Item Env:CYBERCAR_PROXY -ErrorAction SilentlyContinue
    Remove-Item Env:CYBERCAR_USE_SYSTEM_PROXY -ErrorAction SilentlyContinue
    foreach ($proxyEnvName in @("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")) {
        Remove-Item "Env:$proxyEnvName" -ErrorAction SilentlyContinue
    }
}

$cliArgs = @("-m", "cybercar", $Mode, "--profile", $Profile)
if ($UploadPlatforms) {
    $cliArgs += @("--platforms", $UploadPlatforms)
}
if ($Limit -gt 0) {
    $cliArgs += @("--limit", [string]$Limit)
}

$extraArgs = @()
if ($Workspace) {
    $extraArgs += @("--workspace", [System.IO.Path]::GetFullPath($Workspace))
}
if ($TelegramChatId) {
    $extraArgs += @("--telegram-chat-id", $TelegramChatId)
}
if ($ExtraArgsJson) {
    $decoded = ConvertFrom-Json -InputObject $ExtraArgsJson
    if ($decoded -is [System.Collections.IEnumerable] -and -not ($decoded -is [string])) {
        foreach ($item in $decoded) {
            if ($null -ne $item) {
                $extraArgs += [string]$item
            }
        }
    }
    elseif ($null -ne $decoded) {
        $extraArgs += [string]$decoded
    }
}

Push-Location $root
try {
    & $PythonExe @cliArgs @extraArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
