# Mac M2 Deployment Notes

This runbook records the minimum checks for running the GasGx Video Distribution local console on an Apple Silicon Mac.

## Scope

- Target machine: Mac with Apple Silicon, such as M1/M2/M3.
- Main entry: `python -m gasgx_distribution web`.
- Default local URL: `http://127.0.0.1:8765/`.
- Runtime state: `runtime/gasgx_distribution.db`.
- Browser profiles: `profiles/matrix/<account_key>/<platform>/`.

## Key Differences From Windows

- Use an arm64 Python 3.11+ environment. Avoid mixing Rosetta or Intel Python with arm64 native packages.
- Do not reuse Windows absolute paths such as `G:\...` or `D:\...`; convert them to Mac paths under the cloned repository.
- Treat PowerShell scripts in `scripts/*.ps1` as Windows operator helpers. On Mac, prefer direct `python -m ...` commands, shell scripts, `launchd`, or cron.
- Browser profile login state should be recreated on Mac. Windows Chrome profiles and cookies are not a reliable migration artifact because macOS uses a different browser storage and keychain model.
- macOS may require permissions for browser automation, including Accessibility, Screen Recording, and file access permissions for the terminal, Python process, or Chrome.

## Bootstrap Checklist

```bash
python3 --version
python3 -c "import platform; print(platform.machine())"
```

The architecture check should print `arm64`.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[runtime,dev]"
```

If audio/video processing is used, install FFmpeg:

```bash
brew install ffmpeg
```

## Path Conversion Rules

- Material input example:
  - Windows: `G:\GasGx Video Distribution\runtime\materials\videos`
  - Mac: `/Users/<user>/GasGx Video Distribution/runtime/materials/videos`
- X or platform profile examples must point inside the Mac checkout, for example:
  - `/Users/<user>/GasGx Video Distribution/profiles/x_collect`
  - `/Users/<user>/GasGx Video Distribution/config/x_cookies.local.json`
- Before a customer Mac install, search for hardcoded `G:\`, `D:\`, and backslash-only assumptions in config, docs, runtime rows, and operator commands.

## Runtime Validation

Start with the local web console:

```bash
python -m gasgx_distribution web
```

Then open:

```text
http://127.0.0.1:8765/
```

Validate the matrix publisher in dry-run mode before any real publish:

```bash
python -m gasgx_distribution matrix-publish-wechat --dry-run
```

If account login checks are needed:

```bash
python -m gasgx_distribution matrix-wechat-login-check --batch-size 5
```

## Browser Automation Risks

- `DrissionPage` and `browser-cookie3` are the highest-risk runtime dependencies on Mac because they depend on local browser behavior and cookie storage.
- Re-login each platform account on the Mac instead of copying Windows browser profiles.
- Confirm Chrome can launch, keep a stable debug port, persist cookies after restart, and survive a second run without losing the session.
- Expect platform risk checks when accounts move to a new device fingerprint.

## Background Operation

Windows Task Scheduler setup scripts do not apply on Mac.

For long-running service behavior, use `launchd` or another Mac-native process supervisor around one of these commands:

```bash
python -m gasgx_distribution web
python -m cybercar telegram supervise
```

The supervisor should define a working directory, virtualenv activation, log files, restart behavior, and environment variables explicitly.

## Environment And Network

- Recreate `.env` from `.env.example`; do not copy secrets blindly from another machine.
- Re-check Supabase keys, Telegram tokens, webhook URLs, and proxy variables.
- Verify proxy variables if network access depends on them:

```bash
echo "$HTTP_PROXY"
echo "$HTTPS_PROXY"
echo "$ALL_PROXY"
```

## Migration Guidance

- SQLite data can be copied, but review rows for Windows paths before using it on Mac.
- Browser profiles should be regenerated on Mac.
- Runtime cache, logs, lock files, and temporary media outputs should not be treated as required migration assets.
- A clean Mac acceptance pass should cover: web console opens, material directory is readable, dry-run publish assigns material correctly, browser profile opens, login state persists, and at least one platform-specific login check completes.

