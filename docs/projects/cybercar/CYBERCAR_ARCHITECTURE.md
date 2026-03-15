# CyberCar Architecture

Last updated: 2026-03-15

## Top-Level Structure

- `src/cybercar/engine.py`: vendored legacy core with login, X download, processing, publishing, and WeChat engagement logic.
- `src/cybercar/pipeline.py`: vendored scheduling/pipeline coordinator used by `immediate`, `collect`, and `publish`.
- `src/cybercar/orchestrator.py`: new standalone runtime/config bridge that merges profile config and drives the vendored pipeline.
- `src/cybercar/session.py`: new login/session command surface for `status`, `open`, and `qr`.
- `src/cybercar/engagement.py`: new WeChat engagement command surface.
- `src/cybercar/migrate.py`: legacy asset migration logic.

## Runtime Boundaries

- Config lives in `config/app.json` and `config/profiles.json`.
- Workspace root is `runtime/`.
- Browser state root is `profiles/`.
- Logs are written under `runtime/logs/`.

## Compatibility Strategy

- The standalone repo keeps local `Collection.*` compatibility wrappers under `src/Collection/...` so vendored legacy modules can run without depending on the old repository.
- New entrypoints are exposed only through the `cybercar` package and thin PowerShell wrappers.

## Command Flow

- `python -m cybercar immediate|collect|publish` -> `cybercar.cli` -> `cybercar.orchestrator` -> `cybercar.pipeline`
- `python -m cybercar login ...` -> `cybercar.cli` -> `cybercar.session` -> `cybercar.engine`
- `python -m cybercar engage wechat ...` -> `cybercar.cli` -> `cybercar.engagement` -> `cybercar.engine`
