# CyberCar

Standalone CyberCar workspace extracted from the legacy monorepo.

## Core scope

- Immediate collect and publish
- Five-platform login/session checks
- WeChat like/comment automation

## Commands

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
