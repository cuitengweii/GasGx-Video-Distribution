param(
    [string]$TaskPrefix = "CyberCar_Telegram",
    [string]$PythonExe = "python",
    [string]$RepoRoot = "",
    [string]$UserId = "",
    [int]$CheckIntervalSeconds = 30,
    [int]$StaleHeartbeatSeconds = 120,
    [int]$StartupGraceSeconds = 90,
    [int]$RecoverRetries = 3,
    [int]$EnsureEveryMinutes = 5,
    [switch]$StartNow
)

$ErrorActionPreference = "Stop"

if ($EnsureEveryMinutes -lt 1) {
    throw "EnsureEveryMinutes must be >= 1."
}

$root = if ($RepoRoot) { [System.IO.Path]::GetFullPath($RepoRoot) } else { Split-Path -Parent $PSScriptRoot }
$supervisorLauncherPath = [System.IO.Path]::GetFullPath((Join-Path $root "scripts\telegram_supervisor_hidden.vbs"))
$ensureLauncherPath = [System.IO.Path]::GetFullPath((Join-Path $root "scripts\telegram_supervisor_once_hidden.vbs"))
if (-not (Test-Path $supervisorLauncherPath)) {
    throw "telegram_supervisor_hidden.vbs not found: $supervisorLauncherPath"
}
if (-not (Test-Path $ensureLauncherPath)) {
    throw "telegram_supervisor_once_hidden.vbs not found: $ensureLauncherPath"
}

$resolvedPython = [string]$PythonExe
$resolvedUser = if ($UserId) { $UserId } else { "$env:USERDOMAIN\$env:USERNAME" }
$supervisorTaskName = "${TaskPrefix}_Supervisor"
$ensureTaskName = "${TaskPrefix}_Ensure"

$supervisorArgument = @(
    ('"{0}"' -f $supervisorLauncherPath),
    "-PythonExe", ('"{0}"' -f $resolvedPython),
    "--check-interval-seconds", [string]$CheckIntervalSeconds,
    "--stale-heartbeat-seconds", [string]$StaleHeartbeatSeconds,
    "--startup-grace-seconds", [string]$StartupGraceSeconds,
    "--recover-retries", [string]$RecoverRetries
) -join " "

$ensureArgument = @(
    ('"{0}"' -f $ensureLauncherPath),
    "-PythonExe", ('"{0}"' -f $resolvedPython)
) -join " "

function Invoke-Schtasks {
    param(
        [string[]]$Arguments
    )

    $output = & schtasks.exe @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("schtasks failed: " + ($output -join [Environment]::NewLine))
    }
}

$supervisorCommand = @(
    "wscript.exe",
    "//B",
    "//Nologo",
    $supervisorArgument
) -join " "
$ensureCommand = @(
    "wscript.exe",
    "//B",
    "//Nologo",
    $ensureArgument
) -join " "
$ensureStartTime = (Get-Date).AddMinutes(1).ToString("HH:mm")

$supervisorCreateArgs = @(
    "/Create",
    "/TN", $supervisorTaskName,
    "/TR", $supervisorCommand,
    "/SC", "ONLOGON",
    "/RL", "HIGHEST",
    "/F"
)
if ($UserId) {
    $supervisorCreateArgs += @("/RU", $resolvedUser)
}

$ensureCreateArgs = @(
    "/Create",
    "/TN", $ensureTaskName,
    "/TR", $ensureCommand,
    "/SC", "MINUTE",
    "/MO", [string]$EnsureEveryMinutes,
    "/ST", $ensureStartTime,
    "/RL", "HIGHEST",
    "/F"
)
if ($UserId) {
    $ensureCreateArgs += @("/RU", $resolvedUser)
}

Invoke-Schtasks -Arguments $supervisorCreateArgs
Invoke-Schtasks -Arguments $ensureCreateArgs

if ($StartNow) {
    Invoke-Schtasks -Arguments @("/Run", "/TN", $supervisorTaskName)
    Invoke-Schtasks -Arguments @("/Run", "/TN", $ensureTaskName)
}

[pscustomobject]@{
    repo_root = $root
    python_exe = $resolvedPython
    user_id = $resolvedUser
    supervisor_task = $supervisorTaskName
    ensure_task = $ensureTaskName
    supervisor_launcher = $supervisorLauncherPath
    ensure_launcher = $ensureLauncherPath
    check_interval_seconds = $CheckIntervalSeconds
    stale_heartbeat_seconds = $StaleHeartbeatSeconds
    startup_grace_seconds = $StartupGraceSeconds
    ensure_every_minutes = $EnsureEveryMinutes
    start_now = [bool]$StartNow
} | ConvertTo-Json -Depth 3
