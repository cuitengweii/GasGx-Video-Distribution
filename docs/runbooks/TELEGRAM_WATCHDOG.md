# Telegram Watchdog

## Goal

- Keep the CyberCar Telegram worker resident on Windows.
- Auto-recover when the worker process dies, the poll loop stalls, or the bot surface goes stale after network jitter.

## Runtime Pieces

- Resident watchdog: `python -m cybercar telegram supervise`
- One-shot self-heal probe: `python -m cybercar telegram supervise --once`
- PowerShell wrapper: `scripts/telegram_supervisor.ps1`
- Task installer: `scripts/install_telegram_supervisor_task.ps1`

## Install On Windows

```powershell
cd D:\code\CyberCar
powershell -ExecutionPolicy Bypass -File .\scripts\install_telegram_supervisor_task.ps1 -StartNow
```

This registers two tasks:

- `CyberCar_Telegram_Supervisor`: starts the resident watchdog at user logon.
- `CyberCar_Telegram_Ensure`: runs `--once` every 5 minutes so the bot can recover even if the resident watchdog itself was terminated.

## Manual Commands

```powershell
python -m cybercar telegram supervise
python -m cybercar telegram supervise --once
python -m cybercar telegram recover
```

## Health Rules

- Healthy: worker PID exists, process is alive, and heartbeat is fresh.
- Recover: worker PID is gone, status is `error` or `stopped`, or heartbeat exceeds the stale threshold.
- Throttle: watchdog stops repeated recover spam after too many restarts within the configured window.

## Logs And State

- Worker logs: `runtime/logs/telegram_worker_latest.out.log`, `runtime/logs/telegram_worker_latest.err.log`
- Watchdog logs: `runtime/logs/telegram_supervisor_YYYYMMDD.log`
- Watchdog state: `runtime/telegram_worker_supervisor_state.json`

## Verification

1. Confirm the task exists: `Get-ScheduledTask -TaskName CyberCar_Telegram_*`
2. Confirm the watchdog loop is active: `Get-Process python | Select-String cybercar`
3. Confirm one-shot health works: `python -m cybercar telegram supervise --once`
4. Kill the worker process once and verify the watchdog recreates it and refreshes the Telegram home surface.
