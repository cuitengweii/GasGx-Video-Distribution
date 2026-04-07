# CyberCar Lessons

Last updated: 2026-04-07

## 2026-03-15

- Symptom: the first standalone copy attempt failed even though the copy commands were correct.
- Root cause: directory creation and file copying were launched in parallel before the target `src/cybercar` path was guaranteed to exist.
- Earlier detection: verify the target tree exists before firing bulk copy operations in parallel on Windows.
- Prevention: do sequential directory bootstrap first, then bulk copy, then patch.

- Symptom: the old codebase was too large to safely rewrite in one extraction pass.
- Root cause: the proven CyberCar logic is concentrated in a few very large modules with deep shared/common coupling.
- Earlier detection: measure the vendored files before committing to a “full refactor first” path.
- Prevention: preserve stable legacy behavior locally first, then reduce complexity behind the new CLI and runtime boundaries in later threads.

- Symptom: copying the Telegram worker without patching defaults would have left it bound to the old repo and runtime layout.
- Root cause: the worker hardcoded repo root, workspace root, config paths, runner script paths, and child-process working-directory assumptions.
- Earlier detection: search path constants and subprocess spawn points before treating the worker as a simple file copy.
- Prevention: keep the legacy worker behavior, but patch its defaults to the new repo and insert a dedicated compatibility runner script for child jobs.

- Symptom: after the standalone extraction, Telegram automation still effectively depended on the old repo.
- Root cause: Windows scheduled tasks and a startup worker process were still registered against `D:\code\Python`, even though the new repo already had a local worker entry.
- Earlier detection: audit Task Scheduler and live process command lines, not just tracked files in the new repo.
- Prevention: when splitting a repo, remove or replace machine-level startup tasks in the same thread as the code migration.

## 2026-03-16

- Symptom: the image review branch surfaced a video-duration failure (`4.89s not in 5-120s`) even though the operator clicked the image menu.
- Root cause: X candidate discovery for the image branch still used `filter:media`, which admits both images and videos.
- Earlier detection: compare the actual search URL and the candidate media type whenever an image-only path reports a video-specific validation error.
- Prevention: force image review to use `filter:images` and keep a second guard that rejects non-image downloaded artifacts in the image branch.

- Symptom: reviewed image candidates failed with `未下载到新的可用素材` even though the source tweet was valid.
- Root cause: the repo-level X download config had `fail_fast=true`, so immediate reviewed collect jobs skipped retry batches and image fallback on the first X metadata timeout.
- Earlier detection: inspect the effective X download policy in the immediate collect log instead of assuming the manual-review path is immune to global downloader config.
- Prevention: treat immediate reviewed jobs as a separate operational class and explicitly strip fail-fast behavior from their child collect arguments.

- Symptom: Telegram home actions reported that a task was still occupying the lock even after the related process had already exited.
- Root cause: `runtime/runtime/telegram_command_worker_action_queue.json` became invalid JSON, so the queue could no longer be safely recovered and stale queued work blocked new actions.
- Earlier detection: validate queue-state JSON whenever the worker reports a persistent lock with no matching live child process.
- Prevention: add corruption-tolerant queue loading and automatic reset/backup for invalid queue files instead of requiring manual cleanup.

- Symptom: after enabling global X downloader `fail_fast`, a direct `collect` run still hit X GraphQL metadata resets and then aborted the whole command with `RuntimeError`, which defeated the intended "just skip failed fresh items" behavior.
- Root cause: the fail-fast change disabled retry layers, but the downloader still raised on a zero-output round and still over-selected discovered candidates far beyond the operator's requested `limit`.
- Earlier detection: real-host validation should check not only whether retry/fallback is skipped, but also whether a full no-download round exits cleanly and whether selected candidate count shrinks with the latest-first mode.
- Prevention: in fail-fast mode, cap selected X URLs to the freshest requested `limit` and treat a zero-download result as a non-fatal empty round.

## 2026-03-17

- Symptom: `即采即发 / 图片 / 全部平台` could fail before any platform-specific publish logic with `CycleContext.__init__() missing 1 required positional argument: 'collection_names'`.
- Root cause: one Telegram immediate worker constructor path had fallen behind the newer `CycleContext` shape and no longer passed the resolved per-platform `collection_names` payload.
- Earlier detection: compare every `runner.CycleContext(...)` call site whenever the dataclass signature changes, rather than assuming the main pipeline constructors cover the legacy worker path.
- Prevention: keep a backward-compatible default on `CycleContext.collection_names` and add regression coverage around immediate-worker context construction so signature drift cannot break publish flows silently.

- Symptom: Douyin video publishes could complete caption fill without ever assigning the configured collection even though image publishes still ran collection selection.
- Root cause: `_select_douyin_collection()` was gated behind `platform_name == "douyin" and _is_image_file(target)`, so the video branch skipped the collection step entirely.
- Earlier detection: compare post-caption logs across Douyin image and video publishes and look for missing collection-selection traces rather than only checking final publish success.
- Prevention: treat collection selection as a shared Douyin publish invariant and cover it with focused tests around `_fill_draft_once_generic()`.

- Symptom: a real Douyin video publish failed with `Failed to verify caption input on douyin (candidates=0, preview=-)` even though the same account and publish entry were still healthy a few minutes later.
- Root cause: `_wait_upload_ready_generic()` treated the Douyin video page as upload-complete as soon as the page stopped showing generic busy markers, but the real creator form had not rendered a visible caption editor yet.
- Earlier detection: compare the upload-complete log line with the first caption-fill attempt in the same `immediate_publish_douyin_*.log`; if "upload appears completed" is immediately followed by `candidates=0`, the ready heuristic is ahead of the live DOM.
- Prevention: keep a platform-specific Douyin video editor-state probe and require real editor evidence before entering caption verification; add regression coverage for the "upload shell first, editor ready later" sequence.

## 2026-03-20

- Symptom: Xiaohongshu immediate publish logs showed successful upload, successful title fill, successful caption fill, and a clickable publish button, but the run ended with `publish not confirmed` / `page returned to draft/compose state`.
- Root cause: this incident was traced to an account-side publish restriction / ban rather than a local automation breakage.
- Earlier detection: if Xiaohongshu reaches the compose form, accepts media plus text, and then repeatedly fails only after the publish click, check account status first before debugging selectors or upload bindings.
- Prevention: keep a standing operator reminder that this exact Xiaohongshu failure signature is a high-priority account-ban / account-limit indicator, and surface that diagnosis explicitly in future bot/log messaging.

## 2026-04-03

- Symptom: operators reported "点击发布没提示" in Telegram immediate publish flow.
- Root cause: callback acknowledgement was delayed until after prefilter-queue read; under queue-lock contention, `answerCallbackQuery` could arrive too late and be rejected as expired.
- Earlier detection: watch for lock-timeout signals (`Timed out waiting for lock: ... telegram_prefilter_queue.json`) and correlate them with `answerCallbackQuery ... query is too old` entries in the same worker window.
- Prevention: treat callback acknowledgement as an immediate action before any queue I/O, and keep explicit timeout/error fallback replies so operator interaction never appears silent.

## 2026-03-21

- Symptom: rerunning immediate collect-publish could hit an existing `link_pending` candidate and finish without sending any Telegram review card, even when the original pending card was no longer practically recoverable from chat.
- Root cause: `link_pending` was added to the reusable-state set, but `_should_reissue_immediate_candidate_card(...)` still blocked `link_pending` from the reissue path, creating a reuse/reissue state-machine mismatch.
- Earlier detection: every time a status is promoted into the reusable set, add a paired regression test that proves the default rerun path either reissues that state or intentionally recreates it.
- Prevention: keep reuse and reissue semantics aligned for all active prefilter states, and explicitly regression-test "pending card disappeared, rerun should recover it" before landing Telegram workflow changes.

## 2026-04-05

- Symptom: WeChat comment manager occasionally extracted blank or low-quality post titles, then post selection/matching became unstable.
- Root cause: store payload fields desc and objectDesc are not always plain strings; on some pages they are nested objects (description / desc / content). Existing normalization only handled flat strings.
- Earlier detection: whenever extracted post titles drop sharply or mismatch while feed count is non-zero, inspect raw store payload shape first before changing selectors.
- Prevention: keep polymorphic field parsing in _normalize_wechat_store_post and avoid first-hit store selection when multiple store objects exist across frame scopes.
- Symptom: Telegram API requests could fail hard when a configured local proxy had a transient connectivity outage.
- Root cause: session construction always pinned to proxy when present, and retry logic treated proxy-connection errors as generic request failures without a short direct-path failover window.
- Earlier detection: when logs show `ProxyError` or `Unable to connect to proxy`, validate direct connectivity before escalating as Telegram endpoint instability.
- Prevention: keep proxy bypass window + session reset in the request path, and test both proxy and bypass paths when modifying telegram transport code.


## 2026-04-07

- Symptom: operators clicked Telegram route buttons and saw no visible response.
- Root cause: worker poll loop/session could stay stale while process was still present, so callback handling and home-surface freshness drifted.
- Earlier detection: check `runtime/telegram_command_worker_state.json` heartbeat freshness and last `update_id` movement before assuming Telegram platform outage.
- Prevention: run `python -m cybercar telegram supervise --once` first, then `python -m cybercar telegram recover --retries 2` to rebuild commands/home surface and rotate stale worker process.

- Symptom: overseas route sometimes started from wrong source expectations (X-oriented collect behavior in a China-source scenario).
- Root cause: source-platform intent was not enforced per candidate; collect arguments could be composed without strict source-aware routing.
- Earlier detection: inspect collect command args for each candidate and verify `--source-platforms` plus `--source-url/--tweet-url` match the candidate host.
- Prevention: keep source-aware CLI build as invariant and keep route labels/source hints explicit in UI and logs.

- Symptom: `cn_to_global` operation can be fragile when collect and publish share one proxy policy.
- Root cause: domestic-source collect and overseas publish have opposite network requirements in China operation.
- Earlier detection: capture per-stage network mode in immediate collect/publish logs, not only final publish result.
- Prevention: enforce stage-specific network policy (collect direct for domestic sources, publish with VPN/proxy as needed) and validate this split on live runs after worker updates.
