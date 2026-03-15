# CyberCar State

Last updated: 2026-03-16

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

## Open Work

- Real-host validation is still required for five-platform login status, one manual bot-driven collect-publish review run, and one WeChat engagement run.
- The vendored pipeline and worker still carry legacy internal complexity; future threads should continue moving logic out of `engine.py`, `pipeline.py`, and the legacy worker into the new domain packages.
- If legacy config or platform selectors drift, the new standalone repo must be updated locally rather than patched back through the old monorepo.
- The old repo's machine-level Telegram manager/guard scripts have not been ported into this repo as supported entrypoints; only the CyberCar worker path is supported.
- Common helper code still contains backward-compatible multi-bot parsing paths, but the active runtime configuration is now single-bot only.

## Next Step

- Run `python -m cybercar migrate-legacy`.
- Verify `python -m cybercar login status --platform wechat`, `douyin`, `xiaohongshu`, `kuaishou`, and `bilibili`.
- Verify `python -m cybercar telegram set-commands` and `python -m cybercar telegram home-refresh` with the real bot config.
- Start one real `python -m cybercar telegram worker` session and confirm `/start`, home refresh, prefilter feedback, manual review, and WeChat comment-reply callbacks work end to end.
- Validate one manual collect/publish run from the Telegram review flow without any scheduled task involvement.
- Continue deleting dead multi-bot helper code once the single-bot worker path has been validated through a few real sessions.
