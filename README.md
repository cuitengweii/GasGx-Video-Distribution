# CyberCar

Standalone CyberCar workspace extracted from the legacy monorepo.

## GasGx Video Distribution console

This workspace also contains a local GasGx matrix-account console for maintaining 30+ short-video accounts with separate browser profiles.

```powershell
python -m gasgx_distribution web
```

Default URL: `http://127.0.0.1:8765/`

The console stores its SQLite database at `runtime/gasgx_distribution.db` and creates per-account browser state under `profiles/matrix/<account_key>/<platform>/`. Phase 1 keeps existing CyberCar CLI behavior intact and exposes unsupported deep automations for LinkedIn, Facebook, YouTube, VK, and Instagram as explicit `unsupported` task records instead of pretending they are production-ready.

For one-pass WeChat test publishing across active matrix accounts, place videos in:

```powershell
G:\GasGx Video Distribution\runtime\materials\videos
```

Then run:

```powershell
python -m gasgx_distribution matrix-publish-wechat --dry-run
python -m gasgx_distribution matrix-publish-wechat
```

The matrix publisher assigns the newest unused video to each active account in account-id order and writes usage state to `runtime/matrix_publish_state.json` so the same material is not selected again.

## Core scope

- Manual collect/publish review through the Telegram bot
- Multi-platform login/session checks (including TikTok/X)
- WeChat like/comment automation
- No scheduled collect/publish tasks in the standalone repo

## Primary entry

```powershell
python -m cybercar telegram set-commands
python -m cybercar telegram home-refresh
python -m cybercar telegram recover
python -m cybercar telegram worker
python -m cybercar telegram supervise
```

Use the Telegram worker as the only operator entry. Keep collect/publish manual and trigger them from the bot review flow or explicit CLI runs.

If the bot surface goes stale after a Telegram/network hiccup, run `python -m cybercar telegram recover`.
For unattended resilience on Windows, install the watchdog tasks with `scripts/install_telegram_supervisor_task.ps1`.

## Supporting commands

```powershell
python -m cybercar immediate --profile cybertruck --platforms tiktok,x --limit 3
python -m cybercar collect --profile cybertruck --limit 3 --source-platforms douyin,xiaohongshu --source-keywords cybertruck,tesla
python -m cybercar publish --profile cybertruck --platforms tiktok,x --limit 3
python -m cybercar login open --platform x
python -m cybercar login open --platform tiktok
python -m cybercar login status --platform wechat
python -m cybercar login open --platform douyin
python -m cybercar login qr --platform wechat
python -m cybercar engage wechat --max-posts 3 --max-replies 1
python -m cybercar migrate-legacy
```

## Layout

- `src/cybercar/`: standalone package
- `config/`: app and profile config
- `runtime/`: local workspace root
- `profiles/`: local browser profiles
- `docs/`: state/decisions/architecture/runbooks

## X Session Isolation

- X collect/publish now uses its own Chrome profile at `profiles/x_collect` and debug port `9335` by default.
- Manual X cookies can be edited in `config/x_cookies.local.json`; this file is ignored by git.
- `config/x_cookies.example.json` shows the supported structure, including `active_account` switching.
- If `config/x_cookies.local.json` has values, they override the X collect profile cookies during GraphQL discovery and yt-dlp downloads.

```powershell
python -m cybercar login open --platform x
python -m cybercar collect --profile cybertruck --limit 3 --x-debug-port 9335 --x-chrome-user-data-dir D:\code\CyberCar\profiles\x_collect --x-cookie-file D:\code\CyberCar\config\x_cookies.local.json
python -m cybercar publish --profile cybertruck --platforms x --limit 3 --x-debug-port 9335 --x-chrome-user-data-dir D:\code\CyberCar\profiles\x_collect
```
