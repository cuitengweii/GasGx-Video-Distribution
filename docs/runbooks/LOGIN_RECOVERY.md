# Login Recovery

## Check Status

```powershell
python -m cybercar login status --platform x
python -m cybercar login status --platform wechat
python -m cybercar login status --platform douyin
python -m cybercar login status --platform xiaohongshu
python -m cybercar login status --platform kuaishou
python -m cybercar login status --platform bilibili
```

## Open Login Surface

```powershell
python -m cybercar login open --platform x
python -m cybercar login open --platform wechat
python -m cybercar login open --platform douyin
```

## WeChat QR Capture

```powershell
python -m cybercar login qr --platform wechat
```

- QR images are written into `D:\code\CyberCar\runtime\logs`.
- X collect uses `D:\code\CyberCar\profiles\x_collect` by default.
- Manual X cookie overrides can be edited in `D:\code\CyberCar\config\x_cookies.local.json`.

```powershell
python -m cybercar collect --profile cybertruck --limit 3 --x-debug-port 9335 --x-chrome-user-data-dir D:\code\CyberCar\profiles\x_collect --x-cookie-file D:\code\CyberCar\config\x_cookies.local.json
```
