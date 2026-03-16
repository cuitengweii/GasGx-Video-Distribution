# CyberCar

Standalone CyberCar workspace extracted from the legacy monorepo.

## Core scope

- Manual collect/publish review through the Telegram bot
- Five-platform login/session checks
- WeChat like/comment automation
- No scheduled collect/publish tasks in the standalone repo

## Primary entry

```powershell
python -m cybercar telegram set-commands
python -m cybercar telegram home-refresh
python -m cybercar telegram recover
python -m cybercar telegram worker
```

Use the Telegram worker as the only operator entry. Keep collect/publish manual and trigger them from the bot review flow or explicit CLI runs.

If the bot surface goes stale after a Telegram/network hiccup, run `python -m cybercar telegram recover`.

## Supporting commands

```powershell
python -m cybercar immediate --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 3
python -m cybercar collect --profile cybertruck --limit 3
python -m cybercar publish --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 3
python -m cybercar login open --platform x
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

## X Collect Isolation

- X collect now uses its own Chrome profile at `profiles/x_collect` and debug port `9335` by default.
- Manual X cookies can be edited in `config/x_cookies.local.json`; this file is ignored by git.
- `config/x_cookies.example.json` shows the supported structure, including `active_account` switching.
- If `config/x_cookies.local.json` has values, they override the X collect profile cookies during GraphQL discovery and yt-dlp downloads.

```powershell
python -m cybercar login open --platform x
python -m cybercar collect --profile cybertruck --limit 3 --x-debug-port 9335 --x-chrome-user-data-dir D:\code\CyberCar\profiles\x_collect --x-cookie-file D:\code\CyberCar\config\x_cookies.local.json
```
