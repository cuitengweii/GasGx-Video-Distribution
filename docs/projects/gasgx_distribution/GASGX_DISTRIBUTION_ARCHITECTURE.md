# GasGx Video Distribution Architecture

Last updated: 2026-04-28

## Runtime Boundaries

- `gasgx_distribution.web` owns the local FastAPI console and static operator UI.
- `gasgx_distribution.public_settings` owns persisted distribution configuration under `runtime/publish_settings.json`.
- `gasgx_distribution.scheduler` owns the background matrix publish scheduler and writes status to `runtime/matrix_scheduler_state.json`.
- `gasgx_distribution.matrix_publish` owns account/video planning, per-account workspace preparation, per-account profile/port assignment, execution locking, and success evidence tracking.
- `cybercar.engine` and `cybercar.pipeline` remain the compatibility execution layer for the actual 视频号 publish form automation.

## Matrix Publish Flow

1. UI or scheduler calls the matrix 视频号 run endpoint.
2. `matrix_publish` acquires `runtime/matrix_publish.lock`.
3. Active 视频号 accounts are ordered by batch settings, previous success rotation, and optional in-batch shuffle.
4. Candidate videos are read from the configured material directory, excluding videos already marked used in `runtime/matrix_publish_state.json`.
5. Each selected account receives exactly one source video in its own workspace under `runtime/matrix_publish_runs/<timestamp>_<account_key>/`.
6. The execution command calls `python -m cybercar.pipeline --publish-only --upload-platforms wechat --limit 1` with the account profile path and account debug port.
7. Success is recorded only when the process exits cleanly and `uploaded_records_wechat.jsonl` evidence exists.
8. Used-video state and run history are appended after each account result.

## Configuration Model

- Global defaults live under `common`.
- Scheduled matrix job controls live under `jobs.matrix_wechat_publish`.
- Per-platform overrides live under `platforms.<platform>`.
- Effective 视频号 publish config resolves platform override first, then inherited global values, then code defaults.

## Video Matrix Module

- `gasgx_distribution.video_matrix` is a separate素材生产 namespace for ingestion, templates, cover generation, BGM/beat handling, preview and render helpers.
- `gasgx_distribution.video_matrix_api` exposes the Web/API boundary for the video-matrix UI.
- `config/video_matrix/` stores defaults, templates, cover templates, BGM library and UI state.
- This module is intentionally separate from `matrix_publish`; it prepares or manages assets, while `matrix_publish` distributes prepared videos to accounts.
