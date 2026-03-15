# CyberCar Decisions

Last updated: 2026-03-15

## 2026-03-15

- CLI is now the primary operator entrypoint for the standalone repo.
- Telegram worker, dashboard, and old scheduling/watchdog shells are out of first-line scope for this repository.
- Runtime assets are colocated with the repository at `D:\code\CyberCar\runtime` and `D:\code\CyberCar\profiles` instead of the legacy external `D:\code\Runtime\...` layout.
- Legacy proven pipeline logic is preserved by local vendoring first; behavior-safe extraction takes priority over aggressive rewrites in the first standalone cut.
