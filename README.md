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
python -m cybercar telegram worker
```

Use the Telegram worker as the only operator entry. Keep collect/publish manual and trigger them from the bot review flow or explicit CLI runs.

## Supporting commands

```powershell
python -m cybercar immediate --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 3
python -m cybercar collect --profile cybertruck --limit 3
python -m cybercar publish --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 3
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
