param(
    [string]$TaskName = "CyberCar_Cleanup",
    [string]$PythonExe = "python",
    [string]$RepoRoot = "",
    [string]$UserId = "",
    [int]$EveryHours = 6,
    [string]$StartTime = "03:30",
    [switch]$StartNow
)

$ErrorActionPreference = "Stop"

if ($EveryHours -lt 1 -or $EveryHours -gt 23) {
    throw "EveryHours must be between 1 and 23."
}

[void][datetime]::ParseExact($StartTime, "HH:mm", $null)

$root = if ($RepoRoot) { [System.IO.Path]::GetFullPath($RepoRoot) } else { Split-Path -Parent $PSScriptRoot }
$scriptPath = [System.IO.Path]::GetFullPath((Join-Path $root "scripts\cleanup.ps1"))
if (-not (Test-Path $scriptPath)) {
    throw "cleanup.ps1 not found: $scriptPath"
}

$resolvedPython = [string]$PythonExe
$resolvedUser = if ($UserId) { $UserId } else { "$env:USERDOMAIN\$env:USERNAME" }
$taskCommand = @(
    "powershell.exe",
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-WindowStyle", "Hidden",
    "-File", ('"{0}"' -f $scriptPath),
    "-PythonExe", ('"{0}"' -f $resolvedPython),
    "--apply"
) -join " "

function Invoke-Schtasks {
    param([string[]]$Arguments)

    $output = & schtasks.exe @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("schtasks failed: " + ($output -join [Environment]::NewLine))
    }
}

$createArgs = @(
    "/Create",
    "/TN", $TaskName,
    "/TR", $taskCommand,
    "/SC", "HOURLY",
    "/MO", [string]$EveryHours,
    "/ST", $StartTime,
    "/RL", "HIGHEST",
    "/F"
)
if ($UserId) {
    $createArgs += @("/RU", $resolvedUser)
}

Invoke-Schtasks -Arguments $createArgs

if ($StartNow) {
    Invoke-Schtasks -Arguments @("/Run", "/TN", $TaskName)
}

[pscustomobject]@{
    repo_root = $root
    python_exe = $resolvedPython
    user_id = $resolvedUser
    task_name = $TaskName
    every_hours = $EveryHours
    start_time = $StartTime
    start_now = [bool]$StartNow
} | ConvertTo-Json -Depth 3
