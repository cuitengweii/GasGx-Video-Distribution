# Login Recovery

## Check Status

```powershell
python -m cybercar login status --platform wechat
python -m cybercar login status --platform douyin
python -m cybercar login status --platform xiaohongshu
python -m cybercar login status --platform kuaishou
python -m cybercar login status --platform bilibili
```

## Open Login Surface

```powershell
python -m cybercar login open --platform wechat
python -m cybercar login open --platform douyin
```

## WeChat QR Capture

```powershell
python -m cybercar login qr --platform wechat
```

- QR images are written into `D:\code\CyberCar\runtime\logs`.
