# CyberCar Decisions

Last updated: 2026-03-15

## 2026-03-15

- CLI is now the primary operator entrypoint for the standalone repo.
- Telegram dashboard remains out of scope, but the Telegram command worker is back in first-line scope and belongs inside the standalone repo.
- Runtime assets are colocated with the repository at `D:\code\CyberCar\runtime` and `D:\code\CyberCar\profiles` instead of the legacy external `D:\code\Runtime\...` layout.
- Legacy proven pipeline logic is preserved by local vendoring first; behavior-safe extraction takes priority over aggressive rewrites in the first standalone cut.
- New domain boundaries are introduced via `support/`, `services/`, `telegram/`, and `pipeline_core/`, while `engine.py`, `pipeline.py`, and the vendored worker remain compatibility-heavy until later extraction threads move logic out incrementally.
