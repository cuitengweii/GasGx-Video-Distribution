from __future__ import annotations

import json
import os
import re
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cybercar import engine
from cybercar.settings import apply_runtime_environment as apply_cybercar_environment

from . import service
from .matrix_publish import list_wechat_accounts, publish_lock_path
from .paths import get_paths
from .public_settings import load_distribution_settings

DEFAULT_STATS_URLS = [
    "https://channels.weixin.qq.com/platform/datacenter",
    "https://channels.weixin.qq.com/platform/analytics",
    "https://channels.weixin.qq.com/platform/post/list",
]


class StatsParseError(RuntimeError):
    pass


class StatsLoginRequired(RuntimeError):
    pass


def runtime_root() -> Path:
    path = get_paths().runtime_root / "wechat_stats_capture"
    path.mkdir(parents=True, exist_ok=True)
    return path


def stats_lock_path() -> Path:
    return runtime_root() / "matrix_stats_capture.lock"


def status_path() -> Path:
    return runtime_root() / "status.json"


def logs_root() -> Path:
    path = runtime_root() / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def screenshots_root() -> Path:
    path = runtime_root() / "screenshots"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _timezone() -> ZoneInfo:
    raw = str(load_distribution_settings().get("common", {}).get("timezone") or "Asia/Shanghai").strip()
    try:
        return ZoneInfo(raw)
    except ZoneInfoNotFoundError:
        return ZoneInfo("Asia/Shanghai")


def default_target_date() -> str:
    return (datetime.now(_timezone()).date() - timedelta(days=1)).isoformat()


def normalize_target_date(value: str | None = None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return default_target_date()
    return date.fromisoformat(raw[:10]).isoformat()


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_event(run_id: str, payload: dict[str, Any]) -> None:
    path = logs_root() / f"{run_id}.jsonl"
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps({"ts": int(time.time()), **payload}, ensure_ascii=False) + "\n")


def _acquire_lock() -> tuple[bool, dict[str, Any]]:
    path = stats_lock_path()
    existing = _read_json(path)
    if existing and _pid_is_running(int(existing.get("pid") or 0)):
        return False, existing
    if existing:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    payload = {"pid": os.getpid(), "started_at": int(time.time())}
    try:
        handle = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False, _read_json(path)
    with os.fdopen(handle, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    return True, payload


def _release_lock() -> None:
    try:
        stats_lock_path().unlink()
    except FileNotFoundError:
        pass


def _publish_lock_active() -> dict[str, Any]:
    payload = _read_json(publish_lock_path())
    if payload and _pid_is_running(int(payload.get("pid") or 0)):
        return payload
    return {}


def capture_status() -> dict[str, Any]:
    return {
        "status": _read_json(status_path()),
        "lock": _read_json(stats_lock_path()),
        "latest_run": service.latest_wechat_stats_capture_run(),
    }


def _stats_urls() -> list[str]:
    raw = str(os.getenv("GASGX_WECHAT_STATS_URLS") or "").strip()
    if not raw:
        return list(DEFAULT_STATS_URLS)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _metric_number(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip().replace(",", "").replace("，", "")
    if not text:
        return 0
    multiplier = 1
    if text.endswith("万"):
        multiplier = 10000
        text = text[:-1]
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return 0
    return int(float(match.group(0)) * multiplier)


def _metric_rate(value: Any) -> float:
    text = str(value or "").strip().replace("%", "")
    try:
        return float(text or 0)
    except ValueError:
        return 0.0


def _find_metric(text: str, labels: list[str]) -> int:
    lines = [line.strip() for line in re.split(r"[\r\n]+", text or "") if line.strip()]
    joined = "\n".join(lines)
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*([-+]?\d[\d,，]*(?:\.\d+)?万?)")
        match = pattern.search(joined)
        if match:
            return _metric_number(match.group(1))
    for index, line in enumerate(lines):
        if any(label in line for label in labels):
            for candidate in [line, *(lines[index + 1 : index + 3])]:
                value = _metric_number(candidate)
                if value:
                    return value
    return 0


def _find_rate(text: str, labels: list[str]) -> float:
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*([-+]?\d+(?:\.\d+)?%)")
        match = pattern.search(text or "")
        if match:
            return _metric_rate(match.group(1))
    return 0.0


def _normalize_work(raw: dict[str, Any], *, account_id: int, target_date: str) -> dict[str, Any]:
    title = str(raw.get("title") or raw.get("name") or "").strip()
    feed_id = str(raw.get("feed_id") or raw.get("feedId") or raw.get("video_ref") or "").strip()
    video_ref = str(raw.get("video_ref") or feed_id or title).strip()
    return {
        "account_id": account_id,
        "platform": "wechat",
        "stat_date": target_date,
        "video_ref": video_ref,
        "feed_id": feed_id or video_ref,
        "title": title or video_ref or "未命名作品",
        "views": _metric_number(raw.get("views")),
        "likes": _metric_number(raw.get("likes")),
        "comments": _metric_number(raw.get("comments")),
        "shares": _metric_number(raw.get("shares")),
        "messages": _metric_number(raw.get("messages")),
        "published_at": str(raw.get("published_at") or raw.get("publish_time") or ""),
        "source": "wechat_stats_capture",
        "raw_json": raw,
    }


def parse_wechat_stats_payload(raw: dict[str, Any], *, account_id: int, target_date: str) -> dict[str, Any]:
    text = str(raw.get("text") or "")
    metrics = raw.get("account_metrics") if isinstance(raw.get("account_metrics"), dict) else {}
    works_raw = raw.get("works") if isinstance(raw.get("works"), list) else []
    works = [_normalize_work(item, account_id=account_id, target_date=target_date) for item in works_raw if isinstance(item, dict)]
    account_snapshot = {
        "account_id": account_id,
        "platform": "wechat",
        "stat_date": target_date,
        "views": _metric_number(metrics.get("views")) or _find_metric(text, ["播放量", "播放", "浏览量", "观看量"]),
        "likes": _metric_number(metrics.get("likes")) or _find_metric(text, ["点赞量", "点赞"]),
        "comments": _metric_number(metrics.get("comments")) or _find_metric(text, ["评论量", "评论"]),
        "shares": _metric_number(metrics.get("shares")) or _find_metric(text, ["分享量", "分享", "转发"]),
        "messages": _metric_number(metrics.get("messages")) or _find_metric(text, ["私信", "咨询"]),
        "followers": _metric_number(metrics.get("followers")) or _find_metric(text, ["粉丝数", "粉丝"]),
        "follower_delta": _metric_number(metrics.get("follower_delta")) or _find_metric(text, ["新增粉丝", "净增粉丝"]),
        "profile_visits": _metric_number(metrics.get("profile_visits")) or _find_metric(text, ["主页访问", "主页访问量"]),
        "leads": _metric_number(metrics.get("leads")) or _find_metric(text, ["有效线索", "线索"]),
        "completed_rate": _metric_rate(metrics.get("completed_rate")) or _find_rate(text, ["完播率"]),
        "interaction_rate": _metric_rate(metrics.get("interaction_rate")) or _find_rate(text, ["互动率"]),
        "works_count": _metric_number(metrics.get("works_count")) or len(works),
        "source": "wechat_stats_capture",
        "raw_json": raw,
    }
    if not account_snapshot["views"] and works:
        account_snapshot["views"] = sum(_metric_number(item.get("views")) for item in works)
        account_snapshot["likes"] = account_snapshot["likes"] or sum(_metric_number(item.get("likes")) for item in works)
        account_snapshot["comments"] = account_snapshot["comments"] or sum(_metric_number(item.get("comments")) for item in works)
        account_snapshot["shares"] = account_snapshot["shares"] or sum(_metric_number(item.get("shares")) for item in works)
    if not any(account_snapshot[key] for key in ["views", "likes", "comments", "shares", "messages", "followers"]) and not works:
        raise StatsParseError("wechat stats page did not expose recognizable metrics")
    return {"account_snapshot": account_snapshot, "video_snapshots": works}


def _extract_page_payload(page: Any) -> dict[str, Any]:
    js = r"""
    const text = document.body ? document.body.innerText || '' : '';
    const title = document.title || '';
    const url = location.href;
    const parseNumber = (value) => {
      const text = String(value || '').replace(/[,，]/g, '').trim();
      const match = text.match(/[-+]?\d+(?:\.\d+)?\s*万?/);
      if (!match) return 0;
      const token = match[0].replace(/\s/g, '');
      const multi = token.endsWith('万') ? 10000 : 1;
      return Math.round(parseFloat(token.replace('万', '')) * multi);
    };
    const pickMetric = (source, names) => {
      const lines = String(source || '').split(/\n+/).map((line) => line.trim()).filter(Boolean);
      for (let i = 0; i < lines.length; i += 1) {
        if (!names.some((name) => lines[i].includes(name))) continue;
        const same = parseNumber(lines[i]);
        if (same) return same;
        for (const next of lines.slice(i + 1, i + 3)) {
          const value = parseNumber(next);
          if (value) return value;
        }
      }
      return 0;
    };
    const metricMap = {
      views: ['播放量', '播放', '浏览量', '观看量'],
      likes: ['点赞量', '点赞'],
      comments: ['评论量', '评论'],
      shares: ['分享量', '分享', '转发'],
      messages: ['私信', '咨询'],
      followers: ['粉丝数', '粉丝'],
      follower_delta: ['新增粉丝', '净增粉丝'],
      profile_visits: ['主页访问', '主页访问量'],
      leads: ['有效线索', '线索'],
      works_count: ['作品数', '发布作品']
    };
    const account_metrics = {};
    for (const [key, names] of Object.entries(metricMap)) account_metrics[key] = pickMetric(text, names);
    const nodes = Array.from(document.querySelectorAll('tr, [class*="video"], [class*="feed"], [class*="card"], [class*="item"]'));
    const works = [];
    const seen = new Set();
    for (const node of nodes.slice(0, 300)) {
      const rowText = (node.innerText || node.textContent || '').trim();
      if (!rowText || rowText.length < 8 || !/(播放|点赞|评论|分享|\d)/.test(rowText)) continue;
      const titleNode = node.querySelector('[title], a, strong, h3, h4, [class*="title"]');
      const title = ((titleNode && (titleNode.getAttribute('title') || titleNode.innerText || titleNode.textContent)) || rowText.split(/\n/)[0] || '').trim();
      if (!title || seen.has(title)) continue;
      const work = {
        title,
        video_ref: node.getAttribute('data-id') || node.getAttribute('data-feed-id') || title,
        views: pickMetric(rowText, ['播放量', '播放', '浏览量']),
        likes: pickMetric(rowText, ['点赞量', '点赞']),
        comments: pickMetric(rowText, ['评论量', '评论']),
        shares: pickMetric(rowText, ['分享量', '分享', '转发'])
      };
      if (work.views || work.likes || work.comments || work.shares) {
        seen.add(title);
        works.push(work);
      }
      if (works.length >= 50) break;
    }
    return {url, title, text, account_metrics, works};
    """
    try:
        payload = page.run_js(js)
    except Exception as exc:
        raise StatsParseError(f"failed to extract stats page payload: {exc}") from exc
    return payload if isinstance(payload, dict) else {}


def _apply_date_filter(page: Any, target_date: str) -> dict[str, Any]:
    js = r"""
    const target = arguments[0];
    const changed = [];
    const dispatch = (node) => {
      node.dispatchEvent(new Event('input', {bubbles: true}));
      node.dispatchEvent(new Event('change', {bubbles: true}));
    };
    for (const input of Array.from(document.querySelectorAll('input'))) {
      const hint = [input.placeholder, input.ariaLabel, input.name, input.className].join(' ');
      if (!/日期|时间|date|time/i.test(hint)) continue;
      input.focus();
      input.value = target;
      dispatch(input);
      changed.push(hint);
    }
    return {changed};
    """
    try:
        return page.run_js(js, target_date) or {}
    except Exception:
        return {"changed": [], "error": "date_filter_failed"}


def _capture_screenshot(page: Any, run_id: str, account_id: int) -> str:
    path = screenshots_root() / f"{run_id}_account_{account_id}.png"
    try:
        page.get_screenshot(path=str(path))
        return str(path)
    except Exception:
        return ""


def _capture_account(account: dict[str, Any], *, target_date: str, run_id: str) -> dict[str, Any]:
    account_id = int(account.get("id") or 0)
    profile_dir = str(account.get("wechat_profile_dir") or "")
    debug_port = int(account.get("wechat_debug_port") or 0)
    fingerprint = account.get("wechat_fingerprint") or {}
    apply_runtime_environment = apply_cybercar_environment
    apply_runtime_environment()
    page = None
    try:
        with service._chrome_fingerprint_env(fingerprint):  # type: ignore[attr-defined]
            page = engine._connect_chrome(  # type: ignore[attr-defined]
                debug_port=debug_port,
                auto_open_chrome=True,
                chrome_user_data_dir=profile_dir,
                startup_url=_stats_urls()[0],
            )
        for url in _stats_urls():
            try:
                page.get(url)
                time.sleep(1.2)
                login_state = engine.inspect_platform_login_gate(page, "wechat")
                if bool(login_state.get("needs_login")):
                    raise StatsLoginRequired(str(login_state.get("reason") or "login_required"))
                _apply_date_filter(page, target_date)
                raw = _extract_page_payload(page)
                parsed = parse_wechat_stats_payload(raw, account_id=account_id, target_date=target_date)
                parsed["raw"] = raw
                return parsed
            except StatsLoginRequired:
                raise
            except Exception:
                continue
        screenshot = _capture_screenshot(page, run_id, account_id) if page else ""
        raise StatsParseError(f"no supported wechat stats page parsed; screenshot={screenshot}")
    finally:
        if page is not None:
            try:
                engine._disconnect_chrome_page_quietly(page)  # type: ignore[attr-defined]
            except Exception:
                pass


def _ordered_accounts(limit: int = 0) -> list[dict[str, Any]]:
    latest = service.latest_wechat_account_stats_index()
    accounts = list_wechat_accounts()
    accounts.sort(key=lambda item: (int(latest.get(int(item.get("id") or 0), {}).get("captured_at") or 0), int(item.get("id") or 0)))
    if limit > 0:
        return accounts[:limit]
    return accounts


def run_wechat_stats_capture(
    *,
    target_date: str | None = None,
    batch_size: int = 5,
    limit: int = 0,
    dry_run: bool = False,
    notify: bool = True,
) -> dict[str, Any]:
    capture_date = normalize_target_date(target_date)
    run_id = f"wechat-stats-{capture_date}-{time.strftime('%H%M%S')}"
    accounts = _ordered_accounts(limit=max(0, int(limit or 0)))
    started_at = int(time.time())
    result: dict[str, Any] = {
        "ok": True,
        "run_id": run_id,
        "target_date": capture_date,
        "dry_run": bool(dry_run),
        "batch_size": max(1, int(batch_size or 5)),
        "account_total": len(accounts),
        "captured_accounts": 0,
        "skipped_accounts": 0,
        "failed_accounts": 0,
        "results": [],
    }
    service.upsert_wechat_stats_capture_run(
        {
            **result,
            "status": "planned" if dry_run else "running",
            "limit_accounts": int(limit or 0),
            "started_at": started_at,
            "payload_json": result,
        }
    )
    _write_json(status_path(), {**result, "status": "planned" if dry_run else "running", "started_at": started_at})
    if dry_run:
        result["planned_accounts"] = [
            {
                "account_id": int(item.get("id") or 0),
                "account_key": item.get("account_key"),
                "display_name": item.get("display_name"),
                "profile_dir": item.get("wechat_profile_dir"),
                "debug_port": item.get("wechat_debug_port"),
            }
            for item in accounts
        ]
        service.upsert_wechat_stats_capture_run(
            {
                **result,
                "status": "completed",
                "limit_accounts": int(limit or 0),
                "started_at": started_at,
                "finished_at": int(time.time()),
                "payload_json": result,
            }
        )
        _write_json(status_path(), {**result, "status": "completed", "finished_at": int(time.time())})
        return result

    active_publish_lock = _publish_lock_active()
    if active_publish_lock:
        result.update({"ok": False, "skipped": True, "reason": "matrix_publish_running", "publish_lock": active_publish_lock})
        service.upsert_wechat_stats_capture_run(
            {**result, "status": "skipped", "limit_accounts": int(limit or 0), "started_at": started_at, "finished_at": int(time.time()), "payload_json": result}
        )
        _write_json(status_path(), {**result, "status": "skipped", "finished_at": int(time.time())})
        return result

    locked, lock_payload = _acquire_lock()
    if not locked:
        result.update({"ok": False, "skipped": True, "reason": "stats_capture_running", "lock": lock_payload})
        service.upsert_wechat_stats_capture_run(
            {**result, "status": "skipped", "limit_accounts": int(limit or 0), "started_at": started_at, "finished_at": int(time.time()), "payload_json": result}
        )
        _write_json(status_path(), {**result, "status": "skipped", "finished_at": int(time.time())})
        return result

    try:
        for account in accounts:
            account_id = int(account.get("id") or 0)
            event = {"account_id": account_id, "account_key": account.get("account_key"), "status": "running"}
            _append_event(run_id, event)
            try:
                parsed = _capture_account(account, target_date=capture_date, run_id=run_id)
                import_result = service.import_stats(
                    {
                        "account_snapshots": [parsed["account_snapshot"]],
                        "video_snapshots": parsed["video_snapshots"],
                    }
                )
                account_result = {
                    **event,
                    "status": "captured",
                    "videos": len(parsed["video_snapshots"]),
                    "imported": import_result,
                }
                result["captured_accounts"] += 1
            except StatsLoginRequired as exc:
                login_item = {
                    "account_id": account_id,
                    "account_key": account.get("account_key"),
                    "display_name": account.get("display_name"),
                    "platform": "wechat",
                    "profile_dir": account.get("wechat_profile_dir"),
                    "debug_port": account.get("wechat_debug_port"),
                    "reason": str(exc) or "login_required",
                }
                login_batch = service.record_wechat_login_qr_batch([login_item], notify=notify)
                account_result = {**event, "status": "skipped", "reason": "login_required", "login_batch": login_batch}
                result["skipped_accounts"] += 1
            except Exception as exc:
                account_result = {
                    **event,
                    "status": "failed",
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                }
                result["failed_accounts"] += 1
            result["results"].append(account_result)
            _append_event(run_id, account_result)
            _write_json(status_path(), {**result, "status": "running", "updated_at": int(time.time())})
        result["ok"] = result["failed_accounts"] == 0
        final_status = "completed" if result["ok"] else "partial_failed"
        service.upsert_wechat_stats_capture_run(
            {
                **result,
                "status": final_status,
                "limit_accounts": int(limit or 0),
                "started_at": started_at,
                "finished_at": int(time.time()),
                "payload_json": result,
            }
        )
        _write_json(status_path(), {**result, "status": final_status, "finished_at": int(time.time())})
        return result
    finally:
        _release_lock()
