# Local Runtime Setup

## Paths

- Repo root: `D:\code\CyberCar`
- Runtime root: `D:\code\CyberCar\runtime`
- Default profile: `D:\code\CyberCar\profiles\default`
- WeChat profile: `D:\code\CyberCar\profiles\wechat`

## Bootstrap

```powershell
cd D:\code\CyberCar
python -m pip install -e ".[runtime,dev]"
python -m cybercar migrate-legacy
```

## Commands

```powershell
python -m cybercar login status --platform wechat
python -m cybercar telegram set-commands
python -m cybercar telegram home-refresh
python -m cybercar telegram worker
python -m cybercar telegram supervise
python -m cybercar immediate --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 1
python -m cybercar engage wechat --max-posts 3 --max-replies 1
```

## Operating mode

- The standalone repo no longer keeps scheduled collect/publish tasks.
- Use `python -m cybercar telegram worker` as the only long-running operator entry.
- Trigger collect/publish manually from the Telegram review flow or explicit CLI commands.
- If Windows auto-recovery is required, install the Telegram watchdog tasks with `powershell -ExecutionPolicy Bypass -File .\scripts\install_telegram_supervisor_task.ps1 -StartNow`.
- Video matrix job progress is cached in `runtime/video_matrix/jobs/{job_id}.json`, but if the service restarts before a job is completed, progress queries can still become stale; re-run the generation if the job no longer advances.
- Video matrix encoding is intentionally conservative by default: default render workers are capped to a small value, and each ffmpeg/x264 encode uses `VIDEO_MATRIX_FFMPEG_THREADS` when set or a derived per-job thread budget otherwise.
- Recommended tuning order: set `VIDEO_MATRIX_FFMPEG_THREADS` first, then raise `max_workers` only if the machine still has headroom.
