# GasGx Repo Local Rules

## Web Port Policy

- GasGx Web fixed port: `8790`.
- Standard start command: `python -m gasgx_distribution web --host 127.0.0.1 --port 8790`.
- Do not start by `uvicorn gasgx_distribution.web:app ...` in routine operations.
- If `8790` is occupied, stop old GasGx web process first, then restart on `8790`; do not switch to a new port.

## Codex Execution Notes

- For local UI/debug checks, keep a single active GasGx web instance.
- Before starting a new web process, close prior `gasgx_distribution web` / `uvicorn ... gasgx_distribution.web:app` instances.
