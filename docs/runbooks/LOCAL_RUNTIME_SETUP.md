# Local Runtime Setup

## Paths

- Repo root: `D:\code\CyberCar`
- Runtime root: `D:\code\CyberCar\runtime`
- Default profile: `D:\code\CyberCar\profiles\default`
- WeChat profile: `D:\code\CyberCar\profiles\wechat`

## Bootstrap

```powershell
cd D:\code\CyberCar
python -m pip install -e .[runtime,dev]
python -m cybercar migrate-legacy
```

## Commands

```powershell
python -m cybercar login status --platform wechat
python -m cybercar immediate --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 1
python -m cybercar engage wechat --max-posts 3 --max-replies 1
```
