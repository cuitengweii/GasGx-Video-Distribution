# CyberCar State

Last updated: 2026-03-15

## Scope

- Project: `CyberCar`
- Current focus: standalone extraction from `D:\code\Python` into `D:\code\CyberCar`
- Core scope only: immediate collect-publish, five-platform login/session checks, WeChat like/comment

## Latest Milestone

- Established the standalone repository skeleton under `D:\code\CyberCar`.
- Localized runtime, config, profiles, docs, scripts, and migration tooling so the new repo can run without importing code from `D:\code\Python`.
- Preserved the proven legacy pipeline logic by vendoring the old CyberCar engine and pipeline into the new repo with local compatibility wrappers and a new CLI.

## Current Status

- `src/cybercar/engine.py`, `src/cybercar/pipeline.py`, and `src/cybercar/login_triage.py` are now local copies in the standalone repo.
- Public entrypoints now route through `python -m cybercar ...` and PowerShell wrappers under `scripts/`.
- Runtime defaults now target `D:\code\CyberCar\runtime` and `D:\code\CyberCar\profiles`.
- A migration command exists to copy key legacy runtime state and browser profiles into the new repo while skipping logs, Telegram worker residue, and lock artifacts.

## Open Work

- Real-host validation is still required for five-platform login status, one immediate collect-publish run, and one WeChat engagement run.
- The vendored pipeline still carries legacy internal complexity; future threads should continue reducing surface area behind the new CLI modules.
- If legacy config or platform selectors drift, the new standalone repo must be updated locally rather than patched back through the old monorepo.

## Next Step

- Run `python -m cybercar migrate-legacy`.
- Verify `python -m cybercar login status --platform wechat`, `douyin`, `xiaohongshu`, `kuaishou`, and `bilibili`.
- Execute one real `python -m cybercar immediate --profile cybertruck --platforms wechat,douyin,xiaohongshu,kuaishou,bilibili --limit 1` validation.
