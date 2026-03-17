# CyberCar State

Last updated: 2026-03-17

## Scope

- Project: `CyberCar`
- Current focus: manual-review standalone operation inside `D:\code\CyberCar`
- Core scope only: immediate collect-publish, five-platform login/session checks, WeChat like/comment

## Latest Milestone

- Established the standalone repository skeleton under `D:\code\CyberCar`.
- Localized runtime, config, profiles, docs, scripts, and migration tooling so the repo can run without importing code from the old monorepo.
- Preserved the proven legacy pipeline logic by vendoring the old CyberCar engine and pipeline into the new repo with local compatibility wrappers and a new CLI.
- Restored the Telegram command worker inside the standalone repo and added low-risk facade packages for `support`, `services`, `telegram`, and `pipeline_core`.
- Removed scheduled-task registration from the standalone repo and aligned the supported operator model around one manual Telegram worker entry.
- Tightened the immediate image-review flow so it searches X with `filter:images`, reports the last Telegram prefilter send error in failure cards, and lets immediate collect jobs keep retry/fallback behavior even when global X download config enables fail-fast mode.
- Unified X download policy resolution across `collect` and Telegram immediate paths, added downloader stage observability, and introduced global `fail_fast` support for low-volume latest-first collection.
- Adjusted fail-fast after real-host validation so low-volume collect only hits the newest `limit` candidates and does not abort the whole run when X GraphQL metadata requests all fail.
- Added a Telegram watchdog supervisor plus Windows task-registration scripts so the bot can auto-recover when the worker dies or its heartbeat stalls.
- Immediate image publish triage now has platform-specific recovery on Douyin and Kuaishou: Douyin no longer treats hidden file inputs as a hard failure when the editor already shows uploaded media, and Kuaishou now scores image upload inputs separately from the lingering "continue editing old video" shell.
- Kuaishou image publish diagnostics were expanded around upload shell detection, file-input bind state, staged `page.set.upload_files` retries, and upload-surface snapshots so real-host failures can be traced without guessing from the final timeout alone.
- Telegram failure cards now prefer the failing platform emoji from the failure-detail section so mixed-platform alerts surface the failing platform directly in the header.
- Telegram success cards now suppress redundant low-priority sections when a human-focused summary exists, and trim machine-info tails so successful bot feedback stays short enough to scan inside Telegram.
- Success-side machine info now keeps the most actionable rows first on compact cards, preferring log and task/ID clues over low-signal fields like duration.
- Repaired the Douyin image immediate-publish path through the upload stage: the uploader now targets the real image drop zone instead of the cover uploader and treats the image editor state as upload-ready even when the page exposes no DOM file input.
- Advanced the Douyin image publish flow past upload and caption fill into collection selection; the remaining blocker is confirming the updated "添加合集" row structure on the current creator page.

## Current Status

- Fixed the WeChat immediate-publish login false positive: the worker now probes the actual create page before the login helper URL and only requests a QR code after `check_platform_login_status()` confirms the session is truly `login_required`.
- Added regression coverage around the WeChat runtime context and post-publish login recheck so a generic publish failure no longer forces the active business tab back to `login.html`.

- `src/cybercar/engine.py`, `src/cybercar/pipeline.py`, and `src/cybercar/login_triage.py` are now local copies in the standalone repo.
- Public entrypoints now route through `python -m cybercar ...` and PowerShell wrappers under `scripts/`.
- Runtime defaults now target `D:\code\CyberCar\runtime` and `D:\code\CyberCar\profiles`.
- A migration command exists to copy key legacy runtime state and browser profiles into the new repo while skipping logs, Telegram worker residue, and lock artifacts.
- Telegram is now exposed through `python -m cybercar telegram worker|set-commands|home-refresh`.
- The vendored worker lives under `src/Collection/.../telegram_command_worker.py`, while new code should enter through `src/cybercar/telegram/`.
- Shared Telegram dependencies that used to live only in the old repo are now vendored under `src/cybercar/common/` and mirrored through `src/Collection/shared/common/`.
- Windows Task Scheduler is no longer a supported entry layer for CyberCar standalone operation.
- Local Telegram runtime now uses a single-bot registry under `runtime/secrets/telegram_bot_registry.json`; no manager bot or shared multi-bot routing remains in the supported path.
- Immediate image candidate discovery now uses X `filter:images` instead of `filter:media`, so image review no longer pulls short videos into the image-only branch.
- `config/app.json` currently enables X download `fail_fast=true` globally, but the Telegram immediate collect path strips that flag from child collect jobs so manual reviewed candidates can still retry and run image/direct fallback.
- Telegram prefilter fast-send now retries lightly and failure feedback cards include the last send error snippet to make network problems visible without opening logs.
- `collect` now logs a shared X retry policy block and X stage summary that include `fail_fast`, batch failure counts, transport fallback counts, direct fallback counts, and cookie export status.
- In `fail_fast=true` mode, downloader retries and image/direct fallback are skipped, selected X URLs are capped to the freshest `limit`, and a full metadata failure returns an empty result instead of raising `RuntimeError`.
- The worker was restarted on 2026-03-16 and a real image review run was exercised; main remaining production issue is intermittent X/Telegram network instability, not legacy path coupling.
- Real-host validation on 2026-03-16 still showed repeated X `Downloading GraphQL JSON` failures with `ConnectionResetError(10054)` even under direct-tun and low daily volume, so the dominant production risk remains X-side/network instability rather than downloader wiring.
- Telegram runtime now also exposes `python -m cybercar telegram supervise`, `scripts/telegram_supervisor.ps1`, and `scripts/install_telegram_supervisor_task.ps1` for self-healing worker supervision.
- `src/cybercar/common/telegram_ui.py` now hides the redundant success-side `执行结果` section whenever `人工关注` is present and caps `机器信息` output to the two highest-signal items on success cards.
- Regression coverage for the Telegram card compaction rules now lives in `tests/test_telegram.py` and `tests/test_telegram_success_card_compaction.py`.
- Douyin image publish now has a code-side escape hatch for pages that already show `编辑图片 / 已添加1张图片 / 预览图文` but no longer expose a usable DOM file input.
- Kuaishou image publish is still not production-stable. The latest live failure in `runtime/runtime/logs/immediate_publish_kuaishou_20260317_091800.log` shows the page staying on the upload shell for the full 420-second window with `File input bind state: count=0`, `max_count=0`, and repeated `拖拽图片到此或点击上传 / 上传图片` shell text.
- The latest Kuaishou shell snapshot confirms two hidden file inputs remain visible to automation: one stale video input from "continue editing old video" and one image input for the shell upload area. Neither currently binds the selected image on the live host.
- Douyin image publish no longer fails on `Could not find file input on douyin page.` for the inspected page state; current production failure has moved to collection confirmation on the image-post form.
- Latest Douyin logs now show successful transition into image editor state (`编辑图片 / 已添加1张图片 / 继续添加`) and successful caption fill before collection selection begins.

## Open Work

- A direct WeChat session probe on 2026-03-17 05:03 (Asia/Shanghai) reported `status=ready` on `https://channels.weixin.qq.com/platform/post/create`; the mixed "publish tab + login tab" operator screenshot was traced to the worker's own QR recovery path reopening `login.html`, not to a confirmed upstream logout.
- The current archive scan shows one unrelated unstaged tweak in `tests/test_telegram.py`; the WeChat misclassification fix itself is already present in repo code and covered by tests.
- Real-host validation is still required for five-platform login status, one fully successful manual bot-driven image collect-review-download path, one successful manual video collect-publish review run, and one WeChat engagement run.
- The vendored pipeline and worker still carry legacy internal complexity; future threads should continue moving logic out of `engine.py`, `pipeline.py`, and the legacy worker into the new domain packages.
- If legacy config or platform selectors drift, the new standalone repo must be updated locally rather than patched back through the old monorepo.
- The old repo's machine-level Telegram manager/guard scripts have not been ported into this repo as supported entrypoints; only the CyberCar worker path is supported.
- Common helper code still contains backward-compatible multi-bot parsing paths, but the active runtime configuration is now single-bot only.
- `runtime/runtime/telegram_command_worker_action_queue.json` had been corrupted by invalid JSON content during this thread and was reset manually; the queue format and corruption handling still need hardening in code.
- Real image candidate downloads can still fail on X metadata timeouts; if failures remain frequent after the immediate-path fail-fast override, network policy or timeout defaults need another pass.
- Direct `collect` still needs one more real-host confirmation that a round with `fail_fast=true` and zero successful downloads exits cleanly all the way back to the shell prompt after the latest patch.
- The watchdog path still needs one Windows validation pass that the scheduled tasks register correctly and recover a deliberately killed worker.
- Kuaishou image publish still needs a real-host fix for the upload shell itself; current code now isolates the failure precisely, but the live page still refuses to bind the file to the shell input and never transitions into the image editor form.
- Once Kuaishou is fixed, the immediate image path needs one more full live pass across Douyin, Xiaohongshu, and Kuaishou to confirm the new platform-specific upload readiness rules do not regress mixed-platform runs.
- Douyin image collection selection still needs one real-host confirmation against the current creator-center DOM. The latest failure is `douyin collection selection not confirmed: target=赛博皮卡天津港现车, current=-, episode=-`.
- Kuaishou image immediate publish is still timing out at `420s`; this thread did not address that branch after Xiaohongshu started succeeding again.
- Telegram success-card compaction still needs one live-bot confirmation to ensure the shortened machine-info tail and new log/ID prioritization do not hide the only useful operator clue for edge-case successes.

## Next Step

- Re-run Telegram `即采即发 > 图片 > 3条` after the worker restart and confirm at least one reviewed image candidate downloads into `runtime/1_Downloads_Images` or `runtime/2_Processed_Images`.
- If image collect still fails, capture the newest `immediate_collect_item_job_*.log` and decide whether to raise X timeout/retry policy or force proxy/system-proxy mode for X downloads.
- Verify `python -m cybercar login status --platform wechat`, `douyin`, `xiaohongshu`, `kuaishou`, and `bilibili`.
- Verify one complete manual publish from the Telegram review flow while the watchdog remains active in the background.
- Continue deleting dead multi-bot helper code and add code-side recovery for corrupted action-queue state once the single-bot worker path is stable through a few real sessions.
- Re-run `即采即发 > 图片 > 全部平台` after the latest Kuaishou upload-shell patch and inspect whether the new `page.set image stage` log lines ever move `max_count` above `0`.
- If Kuaishou still stays on the shell page, capture the next `immediate_publish_kuaishou_*.log` and patch the upload-shell click/bind sequence directly around the visible `上传图片` button and `_upload-container_ysbff_*` region instead of the hidden input alone.

- Trigger one known-success Telegram card locally and verify that the new success-card suppression still preserves enough operator context with only two prioritized `机器信息` rows and no duplicated `执行结果` block.

- Real-host validation is still required for the corrected WeChat branch in two opposite states: one normal "already logged in" immediate publish run and one true logout run that must still send the QR code on first detection.
- If Telegram `sendPhoto` transport errors remain intermittent during WeChat relogin, the transport layer still needs a separate retry and observability pass; this thread only removed the false-positive trigger path.

- Trigger one known-success Telegram card locally and verify that the new success-card suppression still preserves enough operator context with only two prioritized `机器信息` rows and no duplicated `执行结果` block.
## Next Step Update

- Re-run `python -m cybercar collect --profile cybertruck --limit 2` and verify the patched fail-fast path reaches the shell prompt with `Using discovered X URLs: 2` and no terminal `RuntimeError`.
- Validate one Telegram immediate reviewed collect still keeps retry/fallback behavior after `_build_immediate_fast_x_download_args()` strips `--x-download-fail-fast`.
- If X metadata resets still dominate, decide whether to shorten `socket_timeout_seconds` again for latest-first collection or force a different network path or cookie source for X only.
- Re-run one Telegram `即采即发 > 图片 > 全部平台` task and inspect the newest `immediate_publish_douyin_*.log` to confirm whether the patched collection-row locator can now see and click the actual Douyin collection dropdown.
- If Douyin still fails, capture the next log's `Collection select retry ... visible=...` line and compare it with the live creator page so the collection trigger heuristic can be tightened one step further.
- After Douyin is green, open a separate thread for `kuaishou upload timeout (420s)` using the newest `immediate_publish_kuaishou_*.log`.
