# CyberCar Architecture

Last updated: 2026-03-21

## Top-Level Structure

- `src/cybercar/engine.py`: vendored legacy core with login, X download, processing, publishing, and WeChat engagement logic.
- `src/cybercar/pipeline.py`: vendored scheduling/pipeline coordinator used by `immediate`, `collect`, and `publish`.
- `src/cybercar/orchestrator.py`: new standalone runtime/config bridge that merges profile config and drives the vendored pipeline.
- `src/cybercar/session.py`: new login/session command surface for `status`, `open`, and `qr`.
- `src/cybercar/engagement.py`: new WeChat engagement command surface.
- `src/cybercar/migrate.py`: legacy asset migration logic.
- `src/cybercar/support/`: shared config, path, state, lock, browser, network, and text helpers for new code.
- `src/cybercar/services/`: domain facades split by `collect`, `process`, `login`, `publish`, and `engagement`.
- `src/cybercar/telegram/`: Telegram subsystem facades for transport, cards, state, locks, prefilter, home, commands, actions, bootstrap, worker startup, and watchdog supervision.
- `src/cybercar/pipeline_core/`: low-risk wrappers around legacy pipeline cycle functions.
- `src/Collection/cybercar/cybercar_video_capture_and_publishing_module/telegram_command_worker.py`: vendored legacy Telegram worker runtime.

## Runtime Boundaries

- Config lives in `config/app.json` and `config/profiles.json`.
- Workspace root is `runtime/`.
- Browser state root is `profiles/`.
- Logs are written under `runtime/logs/`.
- Windows Task Scheduler is supported only as a bootstrap layer for the Telegram watchdog, not for unattended collect/publish.

## Compatibility Strategy

- The standalone repo keeps local `Collection.*` compatibility wrappers under `src/Collection/...` so vendored legacy modules can run without depending on the old repository.
- New entrypoints are exposed only through the `cybercar` package and thin PowerShell wrappers.
- Telegram shared/common modules are now vendored locally under `src/cybercar/common/` and mirrored through `src/Collection/shared/common/` compatibility wrappers.

## Telegram Card Rendering

- `src/cybercar/common/telegram_ui.py` owns the final Telegram card assembly order: section prioritization, failure decoration, success compaction, low-priority success suppression, then subtitle/header decoration.
- For `success` and `done` cards, `人工关注` is treated as the operator-facing focus block. When present, the renderer can drop a redundant `执行结果` section and trim `机器信息` to the first two rows before rendering.
- Success-side machine-info compaction is content-aware inside `src/cybercar/common/telegram_ui.py`: rows mentioning logs, IDs, flags, status, or task identity outrank generic tails like duration.
- Subtitle generation still prefers platform-summary aggregation when present; otherwise it falls back to compacting the provided subtitle string.
- The header renderer in `src/cybercar/common/telegram_ui.py` now uses a platform-first contract for publish-result cards: `状态+平台主标题` on the first line, a weak `· 菜单路径` helper line when context exists, then any remaining subtitle line only if it is not redundant with the platform title.
- The Telegram UI layer now strips HTML-like markup from subtitle text, section titles, item labels, item values, and plain string items before rendering, so malformed upstream fragments cannot force a whole card into raw literal-tag output.
- `src/Collection/cybercar/cybercar_video_capture_and_publishing_module/telegram_command_worker.py` owns a separate but now aligned operator-entry surface: persistent reply-keyboard shortcuts and shortcut parsing both use the same short labels as inline card buttons.

## WeChat Login Recovery Flow

- The vendored Telegram worker resolves WeChat runtime context from `PLATFORM_CREATE_POST_URLS` before `PLATFORM_LOGIN_ENTRY_URLS`, so the first session probe stays on the actual publish surface instead of mutating a healthy tab into `login.html`.
- Post-publish login recovery is now a two-stage path inside the worker: `check_platform_login_status()` verifies whether the session is truly `needs_login`, then `_request_platform_login_qr()` is allowed to send the QR code without forcing another page refresh.

## Platform-Specific Publish Heuristics

- `src/cybercar/engine.py` now carries a Douyin-specific image upload path that scores visible upload candidates in the live page DOM and prefers the actual image drop zone over cover-upload widgets.
- The generic upload staging path now allows platform-specific "editor already ready" inference. For Douyin image posts, the transition into the image editor form is treated as the upload-complete boundary even if the creator page never surfaces a usable DOM file input.
- Douyin collection selection now depends on locating the full collection row container and then enumerating its visible dropdown controls, because the current creator page no longer guarantees that the text label and the real trigger share a shallow wrapper node.
- The Douyin upload-entry path now tries native selector clicks before JS scorer fallback, prioritizing the right-side phone-preview upload shell (`phone-screen` / `container-*`) and the dedicated upload button over the broader outer `content-right` wrapper.
- Collection resolution is now platform-aware inside `src/cybercar/engine.py`: CLI override stays highest, then `collection_names[platform]`, then legacy `<platform>_collection_name`, then `DEFAULT_PLATFORM_COLLECTION_NAMES[platform]`, and only then the shared global `collection_name`.

## Immediate Review Recovery Flow

- `src/Collection/cybercar/cybercar_video_capture_and_publishing_module/telegram_command_worker.py` now treats immediate latest-candidate discovery as a multi-stage reducer: discovery rounds expand the X window, unique URLs are accumulated, previously processed/rejected URLs are filtered through `candidate_ledger`, and same-story duplicates are collapsed before queue upsert or Telegram send.
- Existing prefilter rows are no longer split into "reusable but unrecoverable" pending state. The worker can reissue current cards for `link_pending`, `publish_requested`, `publish_running`, and related active states through `_reissue_immediate_candidate_prefilter_card(...)`.
- The prefilter downvote callback now feeds the collect ledger directly via `_record_prefilter_skip_source_in_collect_ledger(...)`, which links Telegram operator decisions back into the collect-side duplicate filter without waiting for a later process/download artifact.

## Command Flow

- `python -m cybercar immediate|collect|publish` -> `cybercar.cli` -> `cybercar.orchestrator` -> `cybercar.pipeline`
- `python -m cybercar login ...` -> `cybercar.cli` -> `cybercar.session` -> `cybercar.engine`
- `python -m cybercar engage wechat ...` -> `cybercar.cli` -> `cybercar.engagement` -> `cybercar.engine`
- `python -m cybercar telegram worker` -> `cybercar.cli` -> `cybercar.telegram.worker` -> `cybercar.telegram.bootstrap` -> vendored legacy Telegram worker
- `python -m cybercar telegram supervise` -> `cybercar.cli` -> `cybercar.telegram.supervisor` -> `cybercar.telegram.bootstrap.recover_bot_surface`
- `python -m cybercar telegram set-commands|home-refresh` -> `cybercar.cli` -> `cybercar.telegram.bootstrap` -> Telegram transport/home facades

## Operator Model

- The supported long-running process is the manual Telegram worker only.
- Scheduled tasks are limited to keeping the Telegram worker available; they do not launch collect/publish flows.
- Collect/publish jobs may still run as child actions from the Telegram review flow, but they are not launched by Task Scheduler.
