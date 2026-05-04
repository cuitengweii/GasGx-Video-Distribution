from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from Collection.cybercar.cybercar_video_capture_and_publishing_module import main

DEFAULT_SYNC_DIR = Path(__file__).resolve().parents[2] / "runtime" / "telegram_login_sync"


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _find_latest_session_artifact(sync_dir: Path, platform: str) -> Path | None:
    candidates = sorted(
        sync_dir.glob(f"{platform}_*.session.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_session_artifact(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def judge_session_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    platform = _normalize_text(payload.get("platform")).lower() or "wechat"
    status = _normalize_text(payload.get("status")).lower()
    diagnostics = payload.get("diagnostics")
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    reason = _normalize_text(diagnostics.get("reason") or payload.get("reason")).lower()
    current_url = _normalize_text(diagnostics.get("current_url") or payload.get("url"))
    expected_open_url = _normalize_text(diagnostics.get("expected_open_url"))
    recent_session_ready = bool(diagnostics.get("recent_session_ready"))
    root_cause_hint = _normalize_text(diagnostics.get("root_cause_hint"))
    source = _normalize_text(diagnostics.get("source")) or "page"
    matched_marker = _normalize_text(diagnostics.get("matched_marker"))
    page_text_excerpt = _normalize_text(diagnostics.get("page_text_excerpt"))
    needs_login = bool(diagnostics.get("needs_login")) or status == "login_required"

    if not needs_login:
        return {
            "actionable": False,
            "operator_bucket": "not_actionable_ready_snapshot",
            "final_judgment": "当前样本不是 relogin 事件，不能用于根因判定。",
            "platform": platform,
            "status": status or "unknown",
            "reason": reason,
            "root_cause_hint": root_cause_hint,
            "source": source,
            "matched_marker": matched_marker,
            "current_url": current_url,
            "expected_open_url": expected_open_url,
            "page_text_excerpt": page_text_excerpt,
            "recent_session_ready": recent_session_ready,
        }

    recomputed_hint = main._classify_platform_login_root_cause(
        platform,
        reason=reason,
        current_url=current_url,
        open_url=expected_open_url,
        recent_session_ready=recent_session_ready,
    )

    if recomputed_hint == "upstream_session_expired":
        operator_bucket = "upstream_session_expired"
        final_judgment = "更像是上游会话失效，优先按真实掉登录处理。"
    elif recomputed_hint in {
        "local_detection_or_route_anomaly",
        "local_detection_or_page_state_anomaly",
        "likely_local_detection_or_profile_issue",
    }:
        operator_bucket = "local_profile_or_detection_issue"
        final_judgment = "更像是本地 profile、登录检测或页面状态异常，不像单纯上游会话失效。"
    elif recomputed_hint == "upstream_session_or_route_error":
        operator_bucket = "ambiguous_upstream_or_route_error"
        final_judgment = "证据仍然混合，可能是上游会话失效，也可能是本地路由异常。"
    else:
        operator_bucket = "unknown"
        final_judgment = "证据不足，暂时无法稳定归因为上游会话失效或本地异常。"

    return {
        "actionable": True,
        "operator_bucket": operator_bucket,
        "final_judgment": final_judgment,
        "platform": platform,
        "status": status or "login_required",
        "reason": reason,
        "root_cause_hint": recomputed_hint,
        "persisted_root_cause_hint": root_cause_hint,
        "source": source,
        "matched_marker": matched_marker,
        "current_url": current_url,
        "expected_open_url": expected_open_url,
        "page_text_excerpt": page_text_excerpt,
        "recent_session_ready": recent_session_ready,
    }


def build_cli_summary(path: Path, payload: dict[str, Any], judgment: dict[str, Any]) -> str:
    lines = [
        f"artifact: {path}",
        f"platform: {judgment.get('platform') or '-'}",
        f"status: {judgment.get('status') or '-'}",
        f"actionable: {'yes' if judgment.get('actionable') else 'no'}",
        f"operator_bucket: {judgment.get('operator_bucket') or '-'}",
        f"final_judgment: {judgment.get('final_judgment') or '-'}",
        f"reason: {judgment.get('reason') or '-'}",
        f"root_cause_hint: {judgment.get('root_cause_hint') or '-'}",
        f"persisted_root_cause_hint: {judgment.get('persisted_root_cause_hint') or '-'}",
        f"matched_marker: {judgment.get('matched_marker') or '-'}",
        f"source: {judgment.get('source') or '-'}",
        f"current_url: {judgment.get('current_url') or '-'}",
        f"expected_open_url: {judgment.get('expected_open_url') or '-'}",
        f"recent_session_ready: {judgment.get('recent_session_ready')}",
        f"page_text_excerpt: {judgment.get('page_text_excerpt') or '-'}",
        f"updated_at: {_normalize_text(payload.get('updated_at')) or '-'}",
    ]
    return "\n".join(lines)


def main_cli() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect the latest CyberCar platform session artifact and print an operator-facing root-cause judgment."
    )
    parser.add_argument("--platform", default="wechat")
    parser.add_argument("--sync-dir", default=str(DEFAULT_SYNC_DIR))
    parser.add_argument("--artifact", default="")
    parser.add_argument("--json", action="store_true", dest="emit_json")
    args = parser.parse_args()

    artifact_path = Path(args.artifact).expanduser() if args.artifact else _find_latest_session_artifact(Path(args.sync_dir), args.platform)
    if artifact_path is None or not artifact_path.exists():
        parser.error(f"No session artifact found for platform={args.platform} under {args.sync_dir}")

    payload = _load_session_artifact(artifact_path)
    judgment = judge_session_artifact(payload)
    if args.emit_json:
        print(json.dumps({"artifact": str(artifact_path), "judgment": judgment}, ensure_ascii=False, indent=2))
    else:
        print(build_cli_summary(artifact_path, payload, judgment))
    return 0


if __name__ == "__main__":
    raise SystemExit(main_cli())
