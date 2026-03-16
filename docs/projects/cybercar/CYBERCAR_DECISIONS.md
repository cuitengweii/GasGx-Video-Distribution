# CyberCar Decisions

Last updated: 2026-03-16

## 2026-03-15

- CLI is now the primary operator entrypoint for the standalone repo.
- Telegram dashboard remains out of scope, but the Telegram command worker is back in first-line scope and belongs inside the standalone repo.
- Runtime assets are colocated with the repository at `D:\code\CyberCar\runtime` and `D:\code\CyberCar\profiles` instead of the legacy external `D:\code\Runtime\...` layout.
- Legacy proven pipeline logic is preserved by local vendoring first; behavior-safe extraction takes priority over aggressive rewrites in the first standalone cut.
- New domain boundaries are introduced via `support/`, `services/`, `telegram/`, and `pipeline_core/`, while `engine.py`, `pipeline.py`, and the vendored worker remain compatibility-heavy until later extraction threads move logic out incrementally.

## 2026-03-16

- The standalone repo no longer treats Task Scheduler as a supported operator path; scheduled collect/publish and old Telegram startup tasks are removed instead of migrated.
- `python -m cybercar telegram worker` and `scripts/telegram_worker.ps1` are the only supported long-running operator entrypoints.
- Collect/publish remains available as explicit commands, but every video stays on the manual Telegram review path rather than unattended scheduling.
- Immediate image review must search X with `filter:images`; `filter:media` is not acceptable for the image branch because it admits videos and causes false failures on video-only guards.
- Global X download config may use `fail_fast=true`, but Telegram immediate review jobs must override that behavior and keep retry/fallback enabled so manually approved candidates are not discarded on the first transient X timeout.
- For low-volume latest-first manual collect, `fail_fast=true` is the preferred global default: skip downloader retry batches and all fallback work, target only the freshest `limit` discovered X URLs, and treat a no-download round as a non-fatal empty result instead of aborting the whole command.
