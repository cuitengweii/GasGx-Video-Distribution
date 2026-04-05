# CyberCar Decisions

Last updated: 2026-04-05

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

- `CycleContext` construction for Telegram immediate publish must remain backward-compatible: the dataclass keeps a default empty `collection_names` map, and immediate worker code must pass resolved per-platform collection names explicitly instead of relying on positional constructor history.
- Douyin collection selection is a publish-step invariant, not an image-only branch. After caption fill, CyberCar should run `_select_douyin_collection()` for Douyin publishes regardless of whether the target is an image or a video.
- Telegram publish-result success cards should not spend a dedicated subtitle line on `平台发布成功` or `已返回平台结果` when the stacked platform header is already present and the `执行摘要` block follows immediately after it.
- Windows Task Scheduler is allowed again only as a bootstrap layer for Telegram bot availability, not as an unattended collect/publish runner.
- CyberCar now supports a resident Telegram watchdog plus a periodic `--once` self-heal probe so "worker dead but bot silent" incidents are recovered automatically.
- Watchdog recovery must reuse `recover_bot_surface()` so process restart, `/start` command refresh, and home-surface rebuild stay on one code path.
- On image platforms, DOM file-input presence is no longer the only source of truth. If the page already exposes stable editor-state evidence such as `编辑图片 / 已添加N张图片 / 预览图文 / 发布时间`, the flow should continue instead of failing purely because the upload input has been hidden or detached.
- Kuaishou image upload must use platform-specific shell handling. Generic file-input discovery is insufficient because the page can expose both a stale video input from unfinished-edit state and a shell image input that never binds; code should prioritize the image shell explicitly, log bind counts, and treat the shell/editor transition as the real readiness boundary.
- Douyin image publish readiness is no longer defined solely by DOM `input[type=file]` presence. If the page has already entered the image editor state (`编辑图片`, `已添加N张图片`, `继续添加`) and the publish form is visible, CyberCar should treat upload as complete and continue.
- Douyin image upload trigger selection must prefer the real image drop zone over the cover uploader. The uploader now uses a JS-scored candidate search that heavily favors `drop-*` targets and penalizes cover-related `content-upload-*` nodes.
- Douyin collection handling should locate the "添加合集" row by searching upward for the ancestor that owns multiple visible dropdown/combobox controls, rather than relying on a shallow `closest(...div)` match around the text label.
- Telegram success notifications should prioritize operator-facing sections over exhaustive detail: when `人工关注` is present on a success/done card, redundant `执行结果` blocks are suppressed and `机器信息` is capped so the first screen stays readable in Telegram.
- When success cards must compress `机器信息`, the renderer should prefer log and task/ID style clues over low-signal fields such as duration so machine-oriented debugging context survives the cut.
- Kuaishou publish confirmation must target modal dialog buttons only; the compose-page `发布` button is not a valid delayed-confirm target once a confirm dialog is present.
- For WeChat immediate publish and relogin checks, the worker must treat the publish create page as the primary session probe target. `login.html` is a helper or fallback URL only and may not drive the first probe path.
- A generic WeChat publish failure is no longer sufficient evidence for `login_required`. The worker must run `check_platform_login_status()` first and only request a QR code after the session probe confirms a true login loss.
- When success cards must compress `机器信息`, the renderer should prefer log and task/ID style clues over low-signal fields such as duration so machine-oriented debugging context survives the cut.
- Douyin video upload readiness may not fall back to a generic "not busy anymore" completion rule. The publish flow must wait for explicit video-editor evidence such as caption/title inputs, publish controls, or other stable editor hints before caption verification begins.

## 2026-04-03

- Telegram prefilter callback handling now treats `answerCallbackQuery` as first-step behavior for publish actions. Callback acknowledgement must happen before prefilter-queue I/O to avoid Telegram callback expiry during lock contention.
- Queue-read failures in callback hot paths now require deterministic operator feedback (`系统正忙，请稍后重试` / `处理失败，请稍后重试`) instead of silent failure. This is now part of the immediate-publish operator UX contract.
- For callback-path regressions, tests must explicitly cover "ack-before-read" order under lock-timeout simulation to prevent future latency-related UX regressions.

## 2026-03-21

- Existing Telegram immediate-review rows in `link_pending` remain rerunnable recovery state. A later collect-publish scan must be allowed to reissue the current review card instead of treating that row as a permanently satisfied reuse hit.
- Immediate latest-candidate discovery now expands in bounded rounds and only stops once enough fresh candidates remain after ledger filtering. Candidate-window growth is part of the default recovery strategy, not a test-only path.
- Explicit prefilter skips must be written into `candidate_ledger.json` as `review_skipped` so later scans suppress sources the operator already rejected.
- Same-story folding belongs between ledger filtering and prefilter card creation so multiple X posts about the same incident do not consume the requested review budget before the operator sees them.

## 2026-03-18

- Douyin image upload should be considered complete once the creator page has transitioned into the image editor form, even if `page.set.upload_files` leaves `input[type=file]` counts at zero. Editor-state evidence beats DOM file-input counts on the current creator page.
- Platform-specific collection defaults must override the shared `collection_name` fallback. For Douyin image publish, the canonical collection target is `赛博皮卡现车：aawbcc`, and the global fallback may not silently replace it with another platform's collection name.
- Telegram operator-facing cards now optimize for platform-first scanning instead of bot branding. `CyberCar` is removed from card headers, platform-result titles render the platform line first, and menu-path context is demoted to a weak helper line.
- Telegram shortcut vocabulary must remain identical across inline buttons, persistent reply keyboards, and help copy. The supported operator-facing labels are `登录 / 进度 / 即采即发 / 点赞评论`; longer variants remain accepted only as backward-compatible aliases.
- Failure-card login actions may not be inferred from generic summary prose. `🔐 登录` is reserved for explicit login-loss evidence from failure/platform-status/suggestion signals or `status=login_required`.

## 2026-04-05

- WeChat store parsing now treats desc and objectDesc as polymorphic fields: they may be strings or nested objects, and title extraction must read description / desc / content before falling back.
- WeChat store resolver logic may not stop at the first discovered store object across window/parent/top/iframe scopes; it must select the data-bearing candidate using feed/comment/export evidence.
- Telegram API calls now support transient proxy failover. On proxy-connection errors, the client opens a temporary direct-connect bypass window, rebuilds the session, and retries instead of failing immediately.
