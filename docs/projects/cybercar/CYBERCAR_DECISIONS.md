# CyberCar Decisions

Last updated: 2026-03-17

## 2026-03-15

- CLI is now the primary operator entrypoint for the standalone repo.
- Telegram dashboard remains out of scope, but the Telegram command worker is back in first-line scope and belongs inside the standalone repo.
- Runtime assets are colocated with the repository at `D:\code\CyberCar\runtime` and `D:\code\CyberCar\profiles` instead of the legacy external `D:\code\Runtime\...` layout.
- Legacy proven pipeline logic is preserved by local vendoring first; behavior-safe extraction takes priority over aggressive rewrites in the first standalone cut.
- New domain boundaries are introduced via `support/`, `services/`, `telegram/`, and `pipeline_core/`, while `engine.py`, `pipeline.py`, and the vendored worker remain compatibility-heavy until later extraction threads move logic out incrementally.

## 2026-03-16

- The standalone repo no longer treats Task Scheduler as a supported operator path; scheduled collect/publish and old Telegram startup tasks are removed instead of migrated.
- `python -m cybercar telegram worker` and `scripts/telegram_worker.ps1` are the only supported long-running operator entrypoints.
- Collect/publish remains available as explicit commands, but every video stays on the manual Telegram review path rather than unattended scheduling.
- Immediate image review must search X with `filter:images`; `filter:media` is not acceptable for the image branch because it admits videos and causes false failures on video-only guards.
- Global X download config may use `fail_fast=true`, but Telegram immediate review jobs must override that behavior and keep retry/fallback enabled so manually approved candidates are not discarded on the first transient X timeout.
- For low-volume latest-first manual collect, `fail_fast=true` is the preferred global default: skip downloader retry batches and all fallback work, target only the freshest `limit` discovered X URLs, and treat a no-download round as a non-fatal empty result instead of aborting the whole command.

## 2026-03-17

- Windows Task Scheduler is allowed again only as a bootstrap layer for Telegram bot availability, not as an unattended collect/publish runner.
- CyberCar now supports a resident Telegram watchdog plus a periodic `--once` self-heal probe so "worker dead but bot silent" incidents are recovered automatically.
- Watchdog recovery must reuse `recover_bot_surface()` so process restart, `/start` command refresh, and home-surface rebuild stay on one code path.
- On image platforms, DOM file-input presence is no longer the only source of truth. If the page already exposes stable editor-state evidence such as `编辑图片 / 已添加N张图片 / 预览图文 / 发布时间`, the flow should continue instead of failing purely because the upload input has been hidden or detached.
- Kuaishou image upload must use platform-specific shell handling. Generic file-input discovery is insufficient because the page can expose both a stale video input from unfinished-edit state and a shell image input that never binds; code should prioritize the image shell explicitly, log bind counts, and treat the shell/editor transition as the real readiness boundary.
- Douyin image publish readiness is no longer defined solely by DOM `input[type=file]` presence. If the page has already entered the image editor state (`编辑图片`, `已添加N张图片`, `继续添加`) and the publish form is visible, CyberCar should treat upload as complete and continue.
- Douyin image upload trigger selection must prefer the real image drop zone over the cover uploader. The uploader now uses a JS-scored candidate search that heavily favors `drop-*` targets and penalizes cover-related `content-upload-*` nodes.
- Douyin collection handling should locate the "添加合集" row by searching upward for the ancestor that owns multiple visible dropdown/combobox controls, rather than relying on a shallow `closest(...div)` match around the text label.
