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

## Current Status

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

## Open Work

- Real-host validation is still required for five-platform login status, one fully successful manual bot-driven image collect-review-download path, one successful manual video collect-publish review run, and one WeChat engagement run.
- The vendored pipeline and worker still carry legacy internal complexity; future threads should continue moving logic out of `engine.py`, `pipeline.py`, and the legacy worker into the new domain packages.
- If legacy config or platform selectors drift, the new standalone repo must be updated locally rather than patched back through the old monorepo.
- The old repo's machine-level Telegram manager/guard scripts have not been ported into this repo as supported entrypoints; only the CyberCar worker path is supported.
- Common helper code still contains backward-compatible multi-bot parsing paths, but the active runtime configuration is now single-bot only.
- `runtime/runtime/telegram_command_worker_action_queue.json` had been corrupted by invalid JSON content during this thread and was reset manually; the queue format and corruption handling still need hardening in code.
- Real image candidate downloads can still fail on X metadata timeouts; if failures remain frequent after the immediate-path fail-fast override, network policy or timeout defaults need another pass.
- Direct `collect` still needs one more real-host confirmation that a round with `fail_fast=true` and zero successful downloads exits cleanly all the way back to the shell prompt after the latest patch.
- The watchdog path still needs one Windows validation pass that the scheduled tasks register correctly and recover a deliberately killed worker.

## Next Step

- Re-run Telegram `即采即发 > 图片 > 3条` after the worker restart and confirm at least one reviewed image candidate downloads into `runtime/1_Downloads_Images` or `runtime/2_Processed_Images`.
- If image collect still fails, capture the newest `immediate_collect_item_job_*.log` and decide whether to raise X timeout/retry policy or force proxy/system-proxy mode for X downloads.
- Verify `python -m cybercar login status --platform wechat`, `douyin`, `xiaohongshu`, `kuaishou`, and `bilibili`.
- Verify one complete manual publish from the Telegram review flow while the watchdog remains active in the background.
- Continue deleting dead multi-bot helper code and add code-side recovery for corrupted action-queue state once the single-bot worker path is stable through a few real sessions.

## Next Step Update

- Re-run `python -m cybercar collect --profile cybertruck --limit 2` and verify the patched fail-fast path reaches the shell prompt with `Using discovered X URLs: 2` and no terminal `RuntimeError`.
- Validate one Telegram immediate reviewed collect still keeps retry/fallback behavior after `_build_immediate_fast_x_download_args()` strips `--x-download-fail-fast`.
- If X metadata resets still dominate, decide whether to shorten `socket_timeout_seconds` again for latest-first collection or force a different network path or cookie source for X only.
