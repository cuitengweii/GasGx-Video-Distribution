# CyberCar Lessons

Last updated: 2026-03-16

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
