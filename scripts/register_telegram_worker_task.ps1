param(
    [string]$TaskName = "CyberCar_Telegram_Worker",
    [string]$RepoRoot = "",
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
$root = if ($RepoRoot) { [System.IO.Path]::GetFullPath($RepoRoot) } else { Split-Path -Parent $PSScriptRoot }
$runner = Join-Path $root "scripts\telegram_worker.ps1"
if (-not (Test-Path -LiteralPath $runner)) {
    throw "Runner script not found: $runner"
}

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$runner`" -PythonExe `"$PythonExe`"" `
    -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "Registered task: $TaskName"
