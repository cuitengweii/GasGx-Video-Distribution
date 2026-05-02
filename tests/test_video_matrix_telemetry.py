from __future__ import annotations

import json
from pathlib import Path

import pytest

from gasgx_distribution.video_matrix.telemetry import GenerationTrace


def test_generation_trace_writes_jsonl_summary_and_report(tmp_path: Path) -> None:
    trace = GenerationTrace("job-1", tmp_path / "logs", {"output_count": 1})

    trace.event("setup", "request_loaded", {"path": tmp_path / "input.mp4"})
    with trace.span("render", "ffmpeg_concat", {"variant": 1}):
        pass
    metrics = trace.finish("complete", assets=[])

    events = (tmp_path / "logs" / "generation_events.jsonl").read_text(encoding="utf-8").splitlines()
    assert events
    for line in events:
        payload = json.loads(line)
        assert payload["schema_version"] == "1.0"
        assert payload["job_id"] == "job-1"
        assert "stage" in payload
        assert "event" in payload
        assert "elapsed_ms" in payload

    summary_path = Path(metrics["summary_path"])
    report_path = Path(metrics["report_path"])
    assert summary_path.exists()
    assert report_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "complete"
    assert summary["top_slow_stages"]
    assert "Video Generation Report" in report_path.read_text(encoding="utf-8")


def test_generation_trace_records_span_errors(tmp_path: Path) -> None:
    trace = GenerationTrace("job-error", tmp_path / "logs")

    with pytest.raises(RuntimeError, match="boom"):
        with trace.span("planning", "plan_variants"):
            raise RuntimeError("boom")

    trace.finish("error", error="boom")
    records = [
        json.loads(line)
        for line in (tmp_path / "logs" / "generation_events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    error = next(record for record in records if record["event"] == "error")
    assert error["stage"] == "planning"
    assert error["payload"]["error"] == "boom"


def test_generation_trace_write_failures_do_not_raise(monkeypatch, tmp_path: Path) -> None:
    trace = GenerationTrace("job-warn", tmp_path / "logs")

    def fail_open(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(Path, "open", fail_open)
    trace.event("render", "still_running")
    metrics = trace.finish("complete")

    assert metrics["telemetry_warnings"]
