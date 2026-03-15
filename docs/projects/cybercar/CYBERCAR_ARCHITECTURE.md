# CyberCar Architecture

Last updated: 2026-03-16

## Top-Level Structure

- `src/cybercar/engine.py`: vendored legacy core with login, X download, processing, publishing, and WeChat engagement logic.
- `src/cybercar/pipeline.py`: vendored scheduling/pipeline coordinator used by `immediate`, `collect`, and `publish`.
- `src/cybercar/orchestrator.py`: new standalone runtime/config bridge that merges profile config and drives the vendored pipeline.
- `src/cybercar/session.py`: new login/session command surface for `status`, `open`, and `qr`.
- `src/cybercar/engagement.py`: new WeChat engagement command surface.
- `src/cybercar/migrate.py`: legacy asset migration logic.
- `src/cybercar/support/`: shared config, path, state, lock, browser, network, and text helpers for new code.
- `src/cybercar/services/`: domain facades split by `collect`, `process`, `login`, `publish`, and `engagement`.
- `src/cybercar/telegram/`: Telegram subsystem facades for transport, cards, state, locks, prefilter, home, commands, actions, bootstrap, and worker startup.
- `src/cybercar/pipeline_core/`: low-risk wrappers around legacy pipeline cycle functions.
- `src/Collection/cybercar/cybercar_video_capture_and_publishing_module/telegram_command_worker.py`: vendored legacy Telegram worker runtime.

## Runtime Boundaries

- Config lives in `config/app.json` and `config/profiles.json`.
- Workspace root is `runtime/`.
- Browser state root is `profiles/`.
- Logs are written under `runtime/logs/`.
- No Windows scheduled-task layer is part of the supported standalone runtime path.

## Compatibility Strategy

- The standalone repo keeps local `Collection.*` compatibility wrappers under `src/Collection/...` so vendored legacy modules can run without depending on the old repository.
- New entrypoints are exposed only through the `cybercar` package and thin PowerShell wrappers.
- Telegram shared/common modules are now vendored locally under `src/cybercar/common/` and mirrored through `src/Collection/shared/common/` compatibility wrappers.

## Command Flow

- `python -m cybercar immediate|collect|publish` -> `cybercar.cli` -> `cybercar.orchestrator` -> `cybercar.pipeline`
- `python -m cybercar login ...` -> `cybercar.cli` -> `cybercar.session` -> `cybercar.engine`
- `python -m cybercar engage wechat ...` -> `cybercar.cli` -> `cybercar.engagement` -> `cybercar.engine`
- `python -m cybercar telegram worker` -> `cybercar.cli` -> `cybercar.telegram.worker` -> `cybercar.telegram.bootstrap` -> vendored legacy Telegram worker
- `python -m cybercar telegram set-commands|home-refresh` -> `cybercar.cli` -> `cybercar.telegram.bootstrap` -> Telegram transport/home facades

## Operator Model

- The supported long-running process is the manual Telegram worker only.
- Scheduled-task registration scripts were removed from the standalone repo to avoid reintroducing the old unattended execution model.
- Collect/publish jobs may still run as child actions from the Telegram review flow, but they are not launched by Task Scheduler.
