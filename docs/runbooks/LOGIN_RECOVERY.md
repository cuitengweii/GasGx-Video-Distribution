# Login Recovery

## GasGx WeChat Matrix Check

```powershell
python -m gasgx_distribution matrix-wechat-login-check --batch-size 5
```

- The matrix check rotates a small WeChat account batch and reuses each account's persisted `browser_profiles.profile_dir` and `browser_profiles.debug_port`.
- If an account reaches `https://channels.weixin.qq.com/login.html`, the result is marked `login_required` and the publish scheduler must not consume material for that account round.
- QR/login-required batches are stored under the brand database tables `login_qr_batches` and `login_qr_items` when the active schema has been initialized. If the remote Supabase schema is older, the command returns `storage_unavailable` in `login_batch` and still reports the affected accounts.
- The operations UI shows the pending scan queue and route switches for Telegram, DingTalk, and WeCom. Route switches only send when the matching AI robot platform is enabled and configured.
- If Chrome reports an existing debug process with the same profile on an old port, close that stale Chrome instance before expecting the persisted port to take effect.

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
