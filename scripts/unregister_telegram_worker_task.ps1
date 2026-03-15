param(
    [string]$TaskName = "CyberCar_Telegram_Worker"
)

$ErrorActionPreference = "Stop"
try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop
    Write-Host "Unregistered task: $TaskName"
}
catch {
    Write-Host "Task not found or already removed: $TaskName"
}
