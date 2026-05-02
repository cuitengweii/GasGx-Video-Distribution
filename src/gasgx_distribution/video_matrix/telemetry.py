from __future__ import annotations

import json
import shutil
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator


SCHEMA_VERSION = "1.0"
MAX_EVENT_LOG_BYTES = 50 * 1024 * 1024
MAX_RUN_DIRS = 200


class GenerationTrace:
    def __init__(self, job_id: str, log_root: Path, request_summary: dict[str, Any] | None = None) -> None:
        self.job_id = job_id
        self.log_root = log_root
        self.events_path = log_root / "generation_events.jsonl"
        self.started_at = time.time()
        self.started_iso = _iso(self.started_at)
        stamp = datetime.fromtimestamp(self.started_at).strftime("%Y%m%d_%H%M%S")
        self.run_dir = log_root / "runs" / f"{stamp}_{_safe_name(job_id)}"
        self.request_summary = _sanitize(request_summary or {})
        self._events: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._failed_writes: list[str] = []
        self._finished = False
        self._ensure_storage()
        self.event("job", "created", self.request_summary)

    def event(self, stage: str, name: str, payload: dict[str, Any] | None = None) -> None:
        record = self._record(stage, name, "event", payload)
        self._append(record)

    @contextmanager
    def span(self, stage: str, name: str, payload: dict[str, Any] | None = None) -> Iterator[None]:
        start = time.perf_counter()
        self._append(self._record(stage, name, "start", payload))
        try:
            yield
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self._append(
                self._record(
                    stage,
                    name,
                    "error",
                    {
                        **(payload or {}),
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "traceback_tail": traceback.format_exc(limit=4)[-2000:],
                    },
                    elapsed_ms=elapsed_ms,
                )
            )
            raise
        else:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self._append(self._record(stage, name, "end", payload, elapsed_ms=elapsed_ms))

    def variant(self, sequence_number: int) -> "VariantTrace":
        return VariantTrace(self, sequence_number)

    def finish(self, status: str, assets: list[Any] | None = None, error: BaseException | str | None = None) -> dict[str, Any]:
        if self._finished:
            return self.metrics_summary()
        self._finished = True
        payload: dict[str, Any] = {
            "status": status,
            "asset_count": len(assets or []),
            "total_output_bytes": _asset_bytes(assets or []),
        }
        if error is not None:
            payload["error"] = str(error)
            payload["error_type"] = type(error).__name__ if isinstance(error, BaseException) else "Error"
        self.event("job", "finished", payload)
        summary = self._build_summary(status, assets or [], error)
        self._write_run_file("run_summary.json", json.dumps(summary, indent=2, ensure_ascii=False))
        self._write_run_file("run_report.md", self._render_report(summary))
        self._prune_old_runs()
        return summary["metrics_summary"]

    def metrics_summary(self) -> dict[str, Any]:
        total_elapsed_ms = int((time.time() - self.started_at) * 1000)
        spans = self._completed_spans()
        slow_stages = _top_stage_totals(spans)
        return {
            "job_id": self.job_id,
            "elapsed_ms": total_elapsed_ms,
            "report_path": str(self.run_dir / "run_report.md"),
            "summary_path": str(self.run_dir / "run_summary.json"),
            "slow_stages": slow_stages[:5],
            "event_count": len(self._events),
            "telemetry_warnings": list(self._failed_writes[-3:]),
        }

    def _ensure_storage(self) -> None:
        try:
            self.log_root.mkdir(parents=True, exist_ok=True)
            self.run_dir.mkdir(parents=True, exist_ok=True)
            if self.events_path.exists() and self.events_path.stat().st_size > MAX_EVENT_LOG_BYTES:
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.events_path.replace(self.log_root / f"generation_events_{stamp}.jsonl")
        except Exception as exc:
            self._failed_writes.append(f"telemetry storage init failed: {exc}")

    def _record(
        self,
        stage: str,
        name: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        elapsed_ms: float | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        return {
            "schema_version": SCHEMA_VERSION,
            "job_id": self.job_id,
            "ts": _iso(now),
            "stage": stage,
            "name": name,
            "event": event_type,
            "elapsed_ms": round(elapsed_ms if elapsed_ms is not None else (now - self.started_at) * 1000, 3),
            "payload": _sanitize(payload or {}),
        }

    def _append(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._events.append(record)
            try:
                self.events_path.parent.mkdir(parents=True, exist_ok=True)
                with self.events_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            except Exception as exc:
                self._failed_writes.append(f"telemetry event write failed: {exc}")

    def _write_run_file(self, filename: str, content: str) -> None:
        try:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            (self.run_dir / filename).write_text(content, encoding="utf-8")
        except Exception as exc:
            self._failed_writes.append(f"telemetry {filename} write failed: {exc}")

    def _completed_spans(self) -> list[dict[str, Any]]:
        return [event for event in self._events if event.get("event") in {"end", "error"}]

    def _build_summary(self, status: str, assets: list[Any], error: BaseException | str | None) -> dict[str, Any]:
        spans = self._completed_spans()
        output_bytes = _asset_bytes(assets)
        variant_totals: dict[str, float] = {}
        for event in spans:
            variant = event.get("payload", {}).get("variant")
            if variant is not None:
                variant_totals[str(variant)] = variant_totals.get(str(variant), 0.0) + float(event.get("elapsed_ms") or 0)
        slow_variants = [
            {"variant": key, "elapsed_ms": round(value, 3)}
            for key, value in sorted(variant_totals.items(), key=lambda item: item[1], reverse=True)[:5]
        ]
        errors = [event for event in self._events if event.get("event") == "error"]
        metrics = self.metrics_summary()
        metrics.update(
            {
                "status": status,
                "success_count": len(assets) if status == "complete" else 0,
                "failed_count": 0 if status == "complete" else 1,
                "total_output_bytes": output_bytes,
                "average_output_bytes": int(output_bytes / len(assets)) if assets else 0,
                "slow_variants": slow_variants,
            }
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "job_id": self.job_id,
            "status": status,
            "started_at": self.started_iso,
            "finished_at": _iso(time.time()),
            "request_summary": self.request_summary,
            "metrics_summary": metrics,
            "top_slow_events": _top_events(spans),
            "top_slow_stages": metrics["slow_stages"],
            "top_slow_variants": slow_variants,
            "assets": [_asset_summary(asset) for asset in assets],
            "errors": errors[-5:],
            "error": str(error) if error is not None else "",
            "telemetry_warnings": list(self._failed_writes),
        }

    def _render_report(self, summary: dict[str, Any]) -> str:
        metrics = summary["metrics_summary"]
        lines = [
            f"# Video Generation Report: {self.job_id}",
            "",
            f"- Status: {summary['status']}",
            f"- Started: {summary['started_at']}",
            f"- Finished: {summary['finished_at']}",
            f"- Total elapsed: {metrics.get('elapsed_ms', 0)} ms",
            f"- Outputs: {metrics.get('success_count', 0)} success / {metrics.get('failed_count', 0)} failed",
            f"- Total output bytes: {metrics.get('total_output_bytes', 0)}",
            "",
            "## Slow Stages",
        ]
        for item in summary["top_slow_stages"] or []:
            lines.append(f"- {item['stage']}: {item['elapsed_ms']} ms")
        lines.extend(["", "## Slow Events"])
        for item in summary["top_slow_events"] or []:
            lines.append(f"- {item['stage']} / {item['name']}: {item['elapsed_ms']} ms")
        lines.extend(["", "## Slow Variants"])
        for item in summary["top_slow_variants"] or []:
            lines.append(f"- variant {item['variant']}: {item['elapsed_ms']} ms")
        if summary["errors"]:
            lines.extend(["", "## Errors"])
            for event in summary["errors"]:
                payload = event.get("payload", {})
                lines.append(f"- {event.get('stage')} / {event.get('name')}: {payload.get('error') or payload.get('error_type')}")
        if summary["telemetry_warnings"]:
            lines.extend(["", "## Telemetry Warnings"])
            lines.extend(f"- {item}" for item in summary["telemetry_warnings"])
        lines.append("")
        return "\n".join(lines)

    def _prune_old_runs(self) -> None:
        runs_root = self.log_root / "runs"
        try:
            dirs = sorted([path for path in runs_root.iterdir() if path.is_dir()], key=lambda path: path.stat().st_mtime, reverse=True)
            for old in dirs[MAX_RUN_DIRS:]:
                shutil.rmtree(old, ignore_errors=True)
        except Exception as exc:
            self._failed_writes.append(f"telemetry run prune failed: {exc}")


class VariantTrace:
    def __init__(self, parent: GenerationTrace, sequence_number: int) -> None:
        self.parent = parent
        self.sequence_number = sequence_number

    def event(self, stage: str, name: str, payload: dict[str, Any] | None = None) -> None:
        self.parent.event(stage, name, self._payload(payload))

    @contextmanager
    def span(self, stage: str, name: str, payload: dict[str, Any] | None = None) -> Iterator[None]:
        with self.parent.span(stage, name, self._payload(payload)):
            yield

    def _payload(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        return {"variant": self.sequence_number, **(payload or {})}


def _iso(value: float) -> str:
    return datetime.fromtimestamp(value).isoformat(timespec="seconds")


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)[:80] or "job"


def _sanitize(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _sanitize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _asset_bytes(assets: list[Any]) -> int:
    total = 0
    for asset in assets:
        path = getattr(asset, "video_path", None)
        try:
            if path and Path(path).exists():
                total += Path(path).stat().st_size
        except OSError:
            continue
    return total


def _asset_summary(asset: Any) -> dict[str, Any]:
    video_path = getattr(asset, "video_path", None)
    size = 0
    try:
        if video_path and Path(video_path).exists():
            size = Path(video_path).stat().st_size
    except OSError:
        size = 0
    variant = getattr(asset, "variant", None)
    return {
        "sequence_number": getattr(variant, "sequence_number", None),
        "signature": getattr(variant, "signature", ""),
        "video_path": str(video_path) if video_path else "",
        "video_bytes": size,
        "cover_path": str(getattr(asset, "cover_path", "") or ""),
        "copy_path": str(getattr(asset, "copy_path", "") or ""),
        "manifest_path": str(getattr(asset, "manifest_path", "") or ""),
    }


def _top_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = sorted(events, key=lambda item: float(item.get("elapsed_ms") or 0), reverse=True)[:10]
    return [
        {
            "stage": row.get("stage", ""),
            "name": row.get("name", ""),
            "elapsed_ms": row.get("elapsed_ms", 0),
            "variant": row.get("payload", {}).get("variant"),
        }
        for row in rows
    ]


def _top_stage_totals(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[str, float] = {}
    for event in events:
        stage = str(event.get("stage") or "unknown")
        totals[stage] = totals.get(stage, 0.0) + float(event.get("elapsed_ms") or 0)
    return [
        {"stage": stage, "elapsed_ms": round(elapsed, 3)}
        for stage, elapsed in sorted(totals.items(), key=lambda item: item[1], reverse=True)
    ]
