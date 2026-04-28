# GasGx Video Distribution Architecture

Last updated: 2026-04-29

## Supabase Multi-Brand Runtime

- `gasgx_distribution.control_plane` owns brand-instance metadata, templates, upgrade-run records, and the switch between SQLite control DB and Supabase control-plane backend.
- `gasgx_distribution.tenant` resolves the active brand from `X-Brand-Instance`, host/domain, or local fallback `LOCAL_BRAND_INSTANCE=gasgx`, then binds API requests to that brand runtime.
- `gasgx_distribution.supabase_backend` is the minimal PostgREST client for Supabase table CRUD and service-key resolution through `env:` references.
- `gasgx_distribution.service` has a runtime backend split: SQLite remains local/dev mode; Supabase branches are used for brand settings, matrix accounts/platforms/profiles, automation tasks, AI robot configs/messages, stats snapshots, and summary reads.
- Static web shell routes `/` and `/static/*` bypass tenant DB binding so remote DB latency cannot prevent the console shell from loading.
- `config/supabase/control_plane.sql` and `config/supabase/brand_baseline.sql` are the current manual initialization baselines for customer Supabase projects, including RLS helper functions and policies.

## AI Robot Runtime

- AI robot config lives in `ai_robot_configs`, with secret fields preserved server side and redacted from public API responses.
- AI robot inbound/outbound queue entries live in `ai_robot_messages`.
- Webhook verification uses per-platform signing secret and HMAC SHA256; successful inbound webhook payloads enqueue messages.
- The queue currently records messages and test messages; real platform sender workers are still a separate runtime component to implement.

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
