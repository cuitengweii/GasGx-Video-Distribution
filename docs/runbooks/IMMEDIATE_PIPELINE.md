# Immediate Pipeline

## Immediate Collect-Publish

```powershell
python -m cybercar immediate --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 1
```

## Collect Only

```powershell
python -m cybercar collect --profile cybertruck --limit 3
```

## Publish Only

```powershell
python -m cybercar publish --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 3
```

## Output Contract

- Logs: `D:\code\CyberCar\runtime\logs`
- Result states: `success`, `partial`, `skipped`, `login_required`, `failed`

## Operational Notes

- Telegram image review uses X `filter:images`; if the image branch shows a video-duration error, treat that as a regression.
- Immediate reviewed collect jobs are expected to keep X retry/fallback behavior even when `config/app.json` enables downloader `fail_fast` for other paths.
- If Telegram reports `未下载到新的可用素材`, inspect the newest `runtime\runtime\logs\immediate_collect_item_job_*.log` first, then check whether the failure is an X metadata timeout, archive dedupe, or a genuine empty candidate.
- If Telegram reports a persistent task lock with no live child process, inspect `runtime\runtime\telegram_command_worker_action_queue.json` for stale or corrupted queue state before restarting the worker again.
- Global `collect` now defaults to X downloader `fail_fast=true` for latest-first low-volume operation. In that mode, the downloader should only try the freshest requested URLs, skip retry/fallback layers, and continue cleanly even if X metadata resets produce zero downloads.
