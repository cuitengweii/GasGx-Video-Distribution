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
