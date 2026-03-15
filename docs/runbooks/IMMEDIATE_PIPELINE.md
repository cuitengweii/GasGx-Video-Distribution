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
