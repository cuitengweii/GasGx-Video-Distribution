# CyberCar State

Last updated: 2026-04-07

## Scope

- Project: `CyberCar`
- Current focus: manual-review standalone operation inside `D:\code\CyberCar`
- Core scope only: immediate collect-publish, five-platform login/session checks, WeChat like/comment

## Latest Milestone

- Immediate collect-publish candidate recovery was hardened on 2026-03-21: the Telegram worker now expands X discovery in rounds, collapses same-story duplicates before card issuance, syncs explicit prefilter skips into `candidate_ledger.json`, and reissues existing `link_pending` review cards on rerun so stale or missing pending cards can be recovered without minting duplicate review rows.
- Captured a new Xiaohongshu failure-triage rule from the 2026-03-20 live incident: when upload, title fill, and caption fill all succeed but publish stays on the compose page with repeated `publish not confirmed` / `page returned to draft/compose state`, treat account suspension / account-level publish restriction as the first diagnosis before chasing DOM-selector regressions.
- Finished a full Telegram operator-card cleanup pass focused on scan speed: platform result cards now put the platform title first, drop the redundant `CyberCar` prefix, weaken menu-path context lines, and hide duplicate success subtitle lines on single-platform success cards.
- Unified Telegram surface wording across inline actions, persistent reply-keyboard shortcuts, and help copy: operators now see the same short labels (`🔐 登录`, `📍 进度`, `⚡ 即采即发`, `💬 点赞评论`) instead of mixed old/new phrases like `平台登录` and `进程查看`.
- Hardened Telegram card rendering against malformed HTML fragments from upstream payloads: title/subtitle/section/item text is now stripped of stray `<b>/<i>` markup before rendering so a single broken tag no longer downgrades an entire card into raw literal markup output.
- Tightened failure-card action inference so `🔐 登录` only appears on explicit login-failure signals (`login_required`, failure/platform-status/suggestion sections) instead of generic summary phrases like “部分平台失败或需要登录”.
- Closed the long-running Douyin image-upload misdiagnosis loop: live logs now show the uploader clicking the real phone-preview upload trigger, entering `编辑图片 / 已添加1张图片 / 继续添加`, and treating that editor state as upload-ready even when the creator page still exposes `file_inputs=0`.
- Corrected platform collection resolution so Douyin no longer inherits the global fallback collection name. The intended Douyin image collection is now the exact live value `赛博皮卡现车：aawbcc`, and platform-specific defaults resolve ahead of the shared `collection_name`.
- Confirmed that the reported Kuaishou immediate-publish crash path caused by `CycleContext.__init__() missing 1 required positional argument: 'collection_names'` is already repaired in the current standalone repo: `CycleContext` now keeps a backward-compatible default and the Telegram immediate worker passes resolved `collection_names` into the publish context.
- Expanded Douyin collection selection so `_select_douyin_collection()` now runs for Douyin publishes regardless of media type, preventing video publishes from silently skipping collection assignment after caption fill.
- Locked the Telegram publish-result success-card layout around the operator summary block: the normalization tests now require `执行摘要` to appear directly after the stacked platform header instead of preserving a redundant `平台发布成功 / 已返回平台结果` subtitle line.
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
- Hardened the Douyin video immediate-publish path against a live upload-to-editor race: the uploader now waits for the real video editor form before running caption verification, instead of treating a missing busy marker as upload completion.

- Douyin collection selection is now factored into dedicated JS state/select probes that read the live `semi-select` selection text and option-title nodes directly, while Kuaishou publish confirmation now uses a dialog-only confirm helper so delayed modal confirmation does not re-click the compose-page publish button.

## Current Status

- `src/Collection/cybercar/cybercar_video_capture_and_publishing_module/telegram_command_worker.py` now runs immediate latest-candidate discovery as a staged pipeline: expand discovery window (`2x/4x/6x` plus baseline cap), accumulate unique X URLs, filter already-seen sources through `candidate_ledger`, then collapse same-story duplicates before prefilter card creation.
- The same worker now treats an existing `link_pending` row as recoverable state rather than terminal reuse-only state. When a rerun hits a previously sent-but-still-pending candidate, the worker reissues the current review card with `action=resent_existing_card` instead of silently counting it as reused and exiting.
- Prefilter downvote / skip actions now write the original X status URL into the collect candidate ledger as `review_skipped`, so later immediate scans suppress explicitly rejected sources before creating a new Telegram review card.
- `src/cybercar/common/telegram_ui.py` now owns the current Telegram header layout contract: platform-result cards render as `平台主标题 -> 弱化路径行 -> 正文`, and single-platform success cards no longer keep an extra `平台发布成功` line when the title already says `某平台已确认`.
- The Telegram UI layer now sanitizes subtitle text, section titles, item labels, item values, and plain string items with `_strip_html_like_markup(...)`, which prevents malformed upstream markup from surfacing as literal `<b>` / `<i>` text in menu cards.
- `src/Collection/cybercar/cybercar_video_capture_and_publishing_module/telegram_command_worker.py` now keeps the home reply keyboard and shortcut parser aligned on the short operator vocabulary: `登录 / 进度 / 即采即发 / 点赞评论`.
- Telegram regression coverage for this card-normalization round now spans `tests/test_telegram.py`, `tests/test_telegram_immediate.py`, `tests/test_telegram_card_normalization.py`, and `tests/test_telegram_success_card_compaction.py`, with the latest local run at `113 passed`.
- The latest Douyin live run in `runtime/runtime/logs/immediate_publish_douyin_20260317_175430.log` confirms the image upload stage is no longer the active blocker: the page enters the editor form, caption fill succeeds, and the failure has moved entirely to collection target resolution/selection.
- The same log proves the page already exposes the correct visible collection option `赛博皮卡现车：aawbcc`; the reported `target=赛博皮卡天津港现车` came from config-resolution precedence, not from a missing option in the creator UI.
- `src/cybercar/engine.py` now resolves platform collection names in this order: CLI override, `collection_names[platform]`, legacy `<platform>_collection_name`, platform default, then shared global `collection_name`.
- The Douyin upload trigger path now prefers native selector clicks on the right-side phone preview upload shell before falling back to JS-scored generic upload heuristics.
- The current repo code already contains the immediate-worker `collection_names` propagation fix for `runner.CycleContext(...)`, so the previously reported Kuaishou platform-processing failure is not represented by a new unstaged diff in this archive.
- `src/cybercar/engine.py` currently broadens Douyin collection selection from the image-only branch to all Douyin publishes, aligning video and image flows on the same post-caption collection assignment step.
- `tests/test_telegram_card_normalization.py` now asserts the compact success-card layout more strictly: stacked platform headers must flow straight into the `执行摘要` section instead of keeping a duplicated platform-result subtitle line.
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
- The failed Douyin video run in `runtime/runtime/logs/immediate_publish_douyin_20260317_192308.log` is now explained by premature upload-ready detection: the page was marked complete before any visible caption field existed, producing `Failed to verify caption input on douyin (candidates=0, preview=-)`.
- `src/cybercar/engine.py` now uses a dedicated Douyin video editor-state probe before caption fill, and `tests/test_engine_douyin_upload.py` covers both the ready-state detector and the "keep waiting until editor is ready" branch.

## Open Work

- Same-story collapse is only regression-covered on English paraphrases right now. Chinese paraphrase variants can still evade token-overlap collapse because the current tokenizer does not split CJK text into multiple overlap anchors.
- The Telegram card system still needs one live-bot validation pass against real messages after the latest “platform-first title / hidden single-platform success subtitle / weak path line” changes, to confirm Telegram clients do not cache or visually merge older card layouts.
- Menu-card and result-card shortening is now largely stable, but one more real-message review is still needed to decide whether single-platform success cards should also drop the redundant `平台已确认发布成功。` sentence from the `执行摘要` body.
- Real-host validation is still required for the reported Kuaishou image publish failure path: re-run `即采即发 > 图片 > 全部平台` and verify that the current repo no longer surfaces the old `CycleContext.__init__()` crash before platform handling starts.
- Douyin collection selection now runs on both image and video publishes in code, but one live creator-center pass is still needed to confirm the shared path does not regress the current video form.
- Telegram success-card compaction still needs one live bot confirmation to ensure removing the redundant platform-result subtitle does not hide the only operator-facing clue on edge-case success cards.
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
- The fixed Douyin video branch still needs one real-host rerun from the Telegram `即采即发 > 视频 > 全部平台` path to confirm the uploader now waits through the transient upload shell and reaches caption fill reliably.

## Next Step

- Add one focused regression pass for same-story collapse on Chinese captions and tighten tokenization if reruns still emit duplicate Chinese story cards for the same X incident.
- Run one real Telegram `即采即发 > 视频 > 1条` recovery check with an existing `link_pending` item whose old card is no longer visible, and verify the worker now sends a refreshed review card instead of ending on pure "沿用在审".
- Trigger 2-3 real Telegram messages that cover: one single-platform success, one partial-success mixed-platform result, and one home/menu card. Verify that the live client now shows platform-first titles, no stray HTML tags, and consistent bottom shortcut wording.
- If the single-platform success body still feels repetitive, remove the remaining `平台已确认发布成功。` sentence from `执行摘要` for that exact case while leaving partial/failure summaries intact.
- Re-run Telegram `即采即发 > 图片 > 全部平台` for the same Kuaishou image scenario and confirm the worker no longer fails at `CycleContext` construction before platform handling begins.
- Re-run one Douyin video publish path and verify the assigned collection is still selected after caption fill now that `_select_douyin_collection()` is no longer gated by `_is_image_file(target)`.
- Trigger one known-success Telegram publish-result card and confirm the operator still sees enough context when the header is followed directly by `执行摘要` and no extra `平台发布成功` subtitle line.

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
- Re-run the failed Douyin video candidate from `collect_publish_latest|ctim-b1e08111a76a71f4` and confirm the next `immediate_publish_douyin_*.log` contains `Waiting for video editor form readiness...` before the first successful caption verification.
- If the rerun still fails, capture the next Douyin log around upload completion and compare the page text snapshot with `_read_douyin_video_upload_state()` so the ready heuristic can be tightened around the current creator-center DOM.

## 2026-04-05 Archive Update

- `src/cybercar/engine.py` WeChat comment-store extraction was hardened in both runtime and Playwright paths:
  - feed title resolution now supports nested payloads where `desc` / `objectDesc` are objects (`description` / `desc` / `content`) instead of plain strings.
  - store discovery no longer picks the first discovered store blindly; it now collects candidate stores from `window`, `parent/top`, and iframes, then selects the best store by feed/comment availability score.
- This change removes a known mismatch where WeChat pages with nested description objects or multiple injected store contexts could produce empty/incorrect post titles and unstable post matching.
- `src/cybercar/common/telegram_api.py` now supports proxy-failure self-heal:
  - proxy connectivity errors trigger a temporary direct-connection bypass window and session reset, so Telegram API calls can recover from transient local proxy outages.
  - bypass duration is configurable via `CYBERCAR_TELEGRAM_PROXY_BYPASS_SECONDS` (minimum 30 seconds; default 180 seconds).
- Local regression results for this archive:
  - `pytest tests/test_telegram_api.py tests/test_engine_wechat.py tests/test_engine_publish_regressions.py -q` -> `78 passed`

## Next Step Update (2026-04-05)

- Run one real WeChat comment-manager session and verify extracted post cards now keep stable non-empty titles on pages where `desc` or `objectDesc` is object-shaped.
- During the same window, simulate or observe one transient proxy failure and confirm Telegram API retries continue through the direct-bypass window without manual restart.
- If title/matching still drifts in production, capture one live store snapshot and extend score weights or key fallbacks without changing the callback flow.

## 2026-04-03 Archive Update

- Immediate publish callback feedback path was hardened in `src/Collection/cybercar/cybercar_video_capture_and_publishing_module/telegram_command_worker.py`: callback queries are now acknowledged before prefilter-queue reads, so operators receive prompt feedback even when queue-lock contention exists.
- Added callback error fallback for queue-read failures (`TimeoutError` and generic exceptions). The worker now returns deterministic callback text (`系统正忙，请稍后重试` / `处理失败，请稍后重试`) instead of presenting a silent click with no visible prompt.
- Added focused regression coverage in `tests/test_telegram_immediate.py` to enforce "ack first, then queue read" behavior under simulated lock-timeout conditions.
- Archive scan baseline at 2026-04-03: repository started in clean state (`git status`: nothing to commit); this archive sync records operational behavior and next-step verification requirements.

## Next Step Update (2026-04-03)

- Run one real Telegram immediate publish click (`普通发布` or `原创发布`) while load is non-trivial and verify callback prompt appears immediately.
- If operators still report "点击无提示", capture the corresponding `telegram_command_worker_*.log` range and check for repeated lock contention on `telegram_prefilter_queue.json`.
- If contention remains frequent, schedule a focused lock-duration reduction pass on prefilter callback hot paths.

## 2026-03-18 Archive Update

- Douyin image upload triage is now narrowed to a post-upload collection issue rather than an upload-entry issue. The newest confirmed live log `runtime/runtime/logs/immediate_publish_douyin_20260317_175430.log` shows native clicking on the right-side phone-preview upload shell, transition into `编辑图片 / 已添加1张图片 / 继续添加`, and successful caption fill before collection handling starts.
- The remaining Douyin mismatch was traced to collection-resolution precedence, not missing DOM options. The page already exposes `赛博皮卡现车：aawbcc`, while older runs still targeted `赛博皮卡天津港现车`.
- `src/cybercar/engine.py` now resolves collection names with platform-specific defaults ahead of the shared global fallback, so Douyin keeps `赛博皮卡现车：aawbcc` even when other platforms still use the global `collection_name`.
- Latest verified local regression command: `pytest tests/test_engine_douyin.py tests/test_engine_douyin_upload.py tests/test_telegram_immediate.py -q` -> `89 passed`.
- Immediate next real-host check: rerun one Douyin image publish and confirm the new log prints `target=赛博皮卡现车：aawbcc`. If the log still shows the old target, the active worker/process has not picked up the latest repo code yet.

## 2026-04-07 Archive Update

- Telegram immediate automation is now split into two explicit routes:
  - `国内即采即发` -> profile `x_to_cn`
  - `海外即采即发` -> profile `cn_to_global`
  The two routes are wired in callback actions and the persistent bottom keyboard.
- Global route source targeting is now explicit:
  - default source platforms for `cn_to_global` are Douyin and Xiaohongshu
  - source platform is inferred per candidate from row payload or URL host
  - collect CLI args are built per source (`--tweet-url` for X, `--source-url` for Douyin/Xiaohongshu) with `--source-platforms`.
- For `cn_to_global` domestic-source candidates, immediate collect now forces direct network mode on collect stage (`proxy_override=""`, `use_system_proxy_override=False`) to avoid collecting China sources through VPN by default.
- Pipeline now supports `--no-domestic-source-discovery`; with this flag, domestic collect uses explicit source URLs only and skips keyword discovery expansion.
- This thread also validated operational recovery: `python -m cybercar telegram recover --retries 2` successfully rebuilt bot surface and restarted a stale worker process after button-click no-response reports.

## Next Step Update (2026-04-07)

- Run one full `海外即采即发` live pass and verify network split behavior matches operations:
  - collect from Douyin/Xiaohongshu without VPN dependency
  - publish to TikTok/X with VPN-enabled session.
- Validate real publish text quality:
  - TikTok description should not duplicate segments
  - X publish should keep non-empty description under non-premium length constraints.
- Keep source-keyword freshness checks for domestic sources and confirm active strategy remains `cybertruck` + `赛博皮卡`.
