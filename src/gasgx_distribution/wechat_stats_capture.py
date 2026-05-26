from __future__ import annotations

import json
import csv
import hashlib
import os
import re
import shutil
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

STATS_DOWNLOAD_SOURCES = {
    "follower": {
        "url": "https://channels.weixin.qq.com/platform/statistic/follower",
        "frame_path": "/micro/statistic/follower",
        "file_prefix": "follower",
    },
    "post": {
        "url": "https://channels.weixin.qq.com/platform/statistic/post",
        "frame_path": "/micro/statistic/post",
        "file_prefix": "post",
    },
}


class StatsParseError(RuntimeError):
    pass


class StatsLoginRequired(RuntimeError):
    pass


class StatsAccountMismatch(RuntimeError):
    pass


def runtime_root() -> Path:
    path = get_paths().runtime_root / "wechat_stats_capture"
    path.mkdir(parents=True, exist_ok=True)
    return path


def stats_lock_path(account_id: int | None = None) -> Path:
    resolved = int(account_id or 0)
    if resolved > 0:
        return runtime_root() / f"matrix_stats_capture_a{resolved}.lock"
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


def downloads_root() -> Path:
    path = runtime_root() / "downloads"
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


def _acquire_lock(account_id: int | None = None) -> tuple[bool, dict[str, Any]]:
    path = stats_lock_path(account_id)
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


def _release_lock(account_id: int | None = None) -> None:
    try:
        stats_lock_path(account_id).unlink()
    except FileNotFoundError:
        pass


def _publish_lock_active() -> dict[str, Any]:
    payload = _read_json(publish_lock_path())
    if payload and _pid_is_running(int(payload.get("pid") or 0)):
        return payload
    return {}


def _active_lock_payloads() -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    paths = [stats_lock_path()]
    paths.extend(sorted(runtime_root().glob("matrix_stats_capture_a*.lock")))
    for path in paths:
        payload = _read_json(path)
        if payload and _pid_is_running(int(payload.get("pid") or 0)):
            payloads.append(payload)
    return payloads


def capture_status() -> dict[str, Any]:
    active_locks = _active_lock_payloads()
    return {
        "status": _read_json(status_path()),
        "lock": active_locks[0] if active_locks else _read_json(stats_lock_path()),
        "locks": active_locks,
        "latest_run": service.latest_wechat_stats_capture_run(),
    }


def _stats_urls() -> list[str]:
    raw = str(os.getenv("GASGX_WECHAT_STATS_URLS") or "").strip()
    if not raw:
        return list(DEFAULT_STATS_URLS)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _download_roots(profile_dir: str) -> list[Path]:
    roots: list[Path] = []
    user_profile = Path(os.getenv("USERPROFILE") or "")
    if str(user_profile):
        roots.append(user_profile / "Downloads")
    if profile_dir:
        roots.append(Path(profile_dir) / "Default" / "Downloads")
    return roots


def _recent_files(roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        try:
            for item in root.rglob("*"):
                if item.is_file():
                    files.append(item)
        except Exception:
            continue
    return files


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_downloaded_csv(path: Path, *, run_id: str, account_id: int, source_key: str) -> Path:
    target_dir = downloads_root() / run_id / str(account_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix or ".csv"
    target = target_dir / f"{source_key}_{int(time.time())}{suffix}"
    shutil.copy2(path, target)
    return target


def _download_candidates(roots: list[Path]) -> list[tuple[float, Path]]:
    candidates: list[tuple[float, Path]] = []
    for item in _recent_files(roots):
        suffix = item.suffix.lower()
        if suffix not in {".csv", ".xls", ".xlsx", ".crdownload"}:
            continue
        try:
            stat = item.stat()
        except Exception:
            continue
        candidates.append((float(stat.st_mtime), item))
    return candidates


def _download_snapshot(roots: list[Path]) -> dict[str, float]:
    return {str(path): mtime for mtime, path in _download_candidates(roots)}


def _wait_file_stable(path: Path, *, timeout: float = 10.0) -> Path:
    deadline = time.time() + timeout
    last_size = -1
    same_rounds = 0
    while time.time() < deadline:
        if not path.exists():
            time.sleep(0.25)
            continue
        if path.suffix.lower() == ".crdownload":
            time.sleep(0.25)
            continue
        try:
            size = int(path.stat().st_size)
        except Exception:
            time.sleep(0.25)
            continue
        if size > 0 and size == last_size:
            same_rounds += 1
            if same_rounds >= 2:
                return path
        else:
            same_rounds = 0
        last_size = size
        time.sleep(0.25)
    return path


def _wait_for_download(before: dict[str, float], roots: list[Path], *, timeout: float = 20.0) -> Path:
    deadline = time.time() + timeout
    newest: Path | None = None
    while time.time() < deadline:
        candidates: list[tuple[float, Path]] = []
        for mtime, item in _download_candidates(roots):
            item_text = str(item)
            # Detect both brand-new file paths and in-place updates to existing paths.
            if item_text in before and mtime <= float(before.get(item_text, 0.0)) + 1e-6:
                continue
            candidates.append((mtime, item))
        if candidates:
            candidates.sort(reverse=True)
            newest = candidates[0][1]
            if newest.suffix.lower() != ".crdownload":
                return _wait_file_stable(newest)
        time.sleep(0.5)
    if newest is not None and newest.exists() and newest.suffix.lower() != ".crdownload":
        return _wait_file_stable(newest)
    raise StatsParseError("downloaded stats csv not found")


def _find_stats_frame(page: Any, frame_path: str) -> Any:
    for _ in range(12):
        try:
            for frame in page.get_frames() or []:
                if frame_path in str(getattr(frame, "url", "") or ""):
                    return frame
        except Exception:
            pass
        time.sleep(0.5)
    raise StatsParseError(f"stats frame not found: {frame_path}")


def _click_download_link(frame: Any) -> dict[str, Any]:
    js = r"""
      const link = document.querySelector('.filter-extra a')
        || Array.from(document.querySelectorAll('a')).find((item) => ((item.innerText || item.textContent || '').trim().includes('下载表格')));
      if (!link) return {clicked: false, reason: 'download_link_missing', href: location.href};
      const text = (link.innerText || link.textContent || '').trim();
      link.click();
      return {clicked: true, text, href: location.href, outer: link.outerHTML || ''};
    """
    result = frame.run_js(js)
    if not isinstance(result, dict) or not result.get("clicked"):
        raise StatsParseError(f"download link not clicked: {result}")
    return result


def _extract_table_data_rows(frame: Any) -> list[list[str]]:
    js = r"""
      const dateRe = /^\d{4}\/\d{2}\/\d{2}$/;
      const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
      const out = [];
      const seen = new Set();
      const pushRow = (cells) => {
        const normalized = cells.map(clean).filter(Boolean);
        if (!normalized.length || !dateRe.test(normalized[0])) return;
        const key = normalized.join('\t');
        if (seen.has(key)) return;
        seen.add(key);
        out.push(normalized);
      };
      const rowNodes = Array.from(document.querySelectorAll('table tr, .table-wrap tr, [class*="table"] [class*="row"], [class*="table"] [class*="tr"]'));
      for (const row of rowNodes) {
        const cellNodes = Array.from(row.querySelectorAll('td, th'));
        if (cellNodes.length) {
          pushRow(cellNodes.map((cell) => cell.innerText || cell.textContent || ''));
          continue;
        }
        const childCells = Array.from(row.children || []).map((cell) => cell.innerText || cell.textContent || '');
        if (childCells.length > 1) {
          pushRow(childCells);
          continue;
        }
        const parts = clean(row.innerText || row.textContent || '').split(' ').filter(Boolean);
        pushRow(parts);
      }
      const bodyLines = clean(document.body ? document.body.innerText : '').split(' ').filter(Boolean);
      for (let index = 0; index < bodyLines.length; index += 1) {
        if (!dateRe.test(bodyLines[index])) continue;
        pushRow(bodyLines.slice(index, index + 8));
      }
      return out;
    """
    try:
        value = frame.run_js(js)
    except Exception:
        return []
    if not isinstance(value, list):
        return []
    rows: list[list[str]] = []
    for row in value:
        if not isinstance(row, list):
            continue
        cells = [str(cell or "").strip() for cell in row]
        if cells and re.match(r"^\d{4}/\d{2}/\d{2}$", cells[0]):
            rows.append(cells)
    return rows


def _table_data_row_count(frame: Any) -> int:
    return len(_extract_table_data_rows(frame))


def _wait_table_data_ready(frame: Any, *, timeout: float = 12.0) -> int:
    deadline = time.time() + timeout
    best = 0
    while time.time() < deadline:
        count = _table_data_row_count(frame)
        best = max(best, count)
        if count > 0:
            return count
        time.sleep(0.4)
    return best


def _capture_stats_source(page: Any, *, source_key: str, account_id: int, profile_dir: str, run_id: str) -> dict[str, Any]:
    source = STATS_DOWNLOAD_SOURCES[source_key]
    page.get(str(source["url"]))
    time.sleep(2.5)
    login_state = engine.inspect_platform_login_gate(page, "wechat")
    if bool(login_state.get("needs_login")):
        raise StatsLoginRequired(str(login_state.get("reason") or "login_required"))
    frame = _find_stats_frame(page, str(source["frame_path"]))
    _wait_table_data_ready(frame)
    roots = _download_roots(profile_dir)
    table_rows = _extract_table_data_rows(frame)
    result: dict[str, Any] = {
        "source": source_key,
        "path": "",
        "original_path": "",
        "source_hash": "",
        "click": {},
        "table_rows": table_rows,
        "data_source": "dom_table",
    }
    # Keep the official export as an audit artifact, but do not use it as the fact source.
    try:
        before = _download_snapshot(roots)
        click_result = _click_download_link(frame)
        downloaded = _wait_for_download(before, roots, timeout=30.0)
        stored = _copy_downloaded_csv(downloaded, run_id=run_id, account_id=account_id, source_key=source_key)
        result.update(
            {
                "path": str(stored),
                "original_path": str(downloaded),
                "source_hash": _file_hash(stored),
                "click": click_result,
            }
        )
    except Exception as exc:
        result["download_error"] = str(exc)
    return result


def _read_csv_rows(path: Path) -> list[list[str]]:
    for encoding in ("utf-8-sig", "gb18030", "gbk", "utf-8"):
        try:
            with path.open("r", encoding=encoding, newline="") as file:
                return [[cell.strip() for cell in row] for row in csv.reader(file)]
        except UnicodeDecodeError:
            continue
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as file:
        return [[cell.strip() for cell in row] for row in csv.reader(file)]


def _csv_data_rows(path: Path) -> list[list[str]]:
    rows = _read_csv_rows(path)
    return [
        row
        for row in rows
        if row and re.match(r"^\d{4}/\d{2}/\d{2}$", str(row[0] or "").strip())
    ]


def _csv_date(value: Any) -> str:
    text = str(value or "").strip().replace("/", "-")
    return date.fromisoformat(text[:10]).isoformat()


def _parse_follower_stats_rows(data_rows: list[list[str]], *, account_id: int, captured_at: int | None = None, meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    ts = int(captured_at or time.time())
    source_meta = dict(meta or {})
    rows: list[dict[str, Any]] = []
    for row in data_rows:
        rows.append(
            {
                "account_id": account_id,
                "platform": "wechat",
                "stat_date": _csv_date(row[0]),
                "followers": _metric_number(row[4] if len(row) > 4 else 0),
                "follower_delta": _metric_number(row[1] if len(row) > 1 else 0),
                "source": "wechat_official_dom",
                "raw_json": {
                    "capture_window": "rolling_7d",
                    "sources": {
                        "follower": {
                            **source_meta,
                            "row": row,
                            "new_followers": _metric_number(row[2] if len(row) > 2 else 0),
                            "unfollows": _metric_number(row[3] if len(row) > 3 else 0),
                            "followers": _metric_number(row[4] if len(row) > 4 else 0),
                            "captured_at": ts,
                        }
                    },
                },
                "captured_at": ts,
            }
        )
    return rows


def parse_follower_stats_csv(path: str | Path, *, account_id: int, captured_at: int | None = None, meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return _parse_follower_stats_rows(_csv_data_rows(Path(path)), account_id=account_id, captured_at=captured_at, meta=meta)


def _parse_post_stats_rows(data_rows: list[list[str]], *, account_id: int, captured_at: int | None = None, meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    ts = int(captured_at or time.time())
    source_meta = dict(meta or {})
    rows: list[dict[str, Any]] = []
    for row in data_rows:
        views = _metric_number(row[1] if len(row) > 1 else 0)
        like_a = _metric_number(row[2] if len(row) > 2 else 0)
        like_b = _metric_number(row[3] if len(row) > 3 else 0)
        comments = _metric_number(row[4] if len(row) > 4 else 0)
        shares = _metric_number(row[5] if len(row) > 5 else 0)
        interactions = like_a + like_b
        rows.append(
            {
                "account_id": account_id,
                "platform": "wechat",
                "stat_date": _csv_date(row[0]),
                "views": views,
                "likes": interactions,
                "comments": comments,
                "shares": shares,
                "interaction_rate": round(((interactions + comments + shares) / views * 100), 2) if views else 0,
                "source": "wechat_official_dom",
                "raw_json": {
                    "capture_window": "rolling_7d",
                    "sources": {
                        "post": {
                            **source_meta,
                            "row": row,
                            "post_columns": ["date", "views", "like_metric_a", "like_metric_b", "comments", "shares", "follows"],
                            "post_follows": _metric_number(row[6] if len(row) > 6 else 0),
                            "captured_at": ts,
                        }
                    },
                },
                "captured_at": ts,
            }
        )
    return rows


def parse_post_stats_csv(path: str | Path, *, account_id: int, captured_at: int | None = None, meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return _parse_post_stats_rows(_csv_data_rows(Path(path)), account_id=account_id, captured_at=captured_at, meta=meta)


def _merge_daily_snapshots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("stat_date") or "")
        if not key:
            continue
        current = merged.setdefault(
            key,
            {
                "account_id": row.get("account_id"),
                "platform": "wechat",
                "stat_date": key,
                "source": str(row.get("source") or "wechat_official_dom"),
                "raw_json": {"capture_window": "rolling_7d", "sources": {}},
                "captured_at": row.get("captured_at"),
            },
        )
        current["source"] = str(row.get("source") or current.get("source") or "wechat_official_dom")
        for field in ("views", "likes", "comments", "shares", "messages", "followers", "follower_delta", "profile_visits", "leads", "completed_rate", "interaction_rate", "works_count"):
            if field in row:
                current[field] = row[field]
        raw = row.get("raw_json")
        raw_payload = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, dict) else {})
        current_raw = current.setdefault("raw_json", {"capture_window": "rolling_7d", "sources": {}})
        current_raw.setdefault("capture_window", "rolling_7d")
        current_raw.setdefault("sources", {}).update(raw_payload.get("sources") or {})
        if row.get("captured_at"):
            current["captured_at"] = max(int(current.get("captured_at") or 0), int(row["captured_at"]))
    return [merged[key] for key in sorted(merged)]


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


def _normalize_account_match_token(value: Any) -> str:
    text = str(value or "").strip().replace("｜", "|")
    if not text:
        return ""
    prefix_match = re.search(r"gasgx\s*\|", text, flags=re.IGNORECASE)
    if prefix_match:
        text = text[prefix_match.end():]
    elif "|" in text and re.search(r"gasgx", text.split("|", 1)[0], flags=re.IGNORECASE):
        text = text.split("|", 1)[1]
    text = text.strip()
    lowered = text.lower()
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", lowered)


def _expected_account_match_tokens(account: dict[str, Any]) -> list[str]:
    expected: list[str] = []
    display_name = str(account.get("display_name") or "").strip()
    if display_name:
        token = _normalize_account_match_token(display_name)
        if token:
            expected.append(token)
    for platform in account.get("platforms") or []:
        if str(platform.get("platform") or "") != "wechat":
            continue
        handle = str(platform.get("handle") or "").strip()
        if handle:
            token = _normalize_account_match_token(handle)
            if token and token not in expected:
                expected.append(token)
    return expected


def _extract_wechat_account_identity(page: Any) -> dict[str, Any]:
    js = r"""
    const candidates = [];
    const push = (value) => {
      const text = String(value || '').replace(/\s+/g, ' ').trim();
      if (!text) return;
      if (text.length < 2 || text.length > 80) return;
      candidates.push(text);
    };
    push(document.title || '');
    const selectors = [
      '.finder-user-nick',
      '.account-name',
      '.nickname',
      '[class*="account-name"]',
      '[class*="nick-name"]',
      '[class*="nickname"]',
      '[class*="profile-name"]',
      '[class*="user-name"]',
      '[class*="finder"][class*="name"]',
      '[class*="channel"][class*="name"]',
      '[class*="account"][class*="name"]'
    ];
    for (const selector of selectors) {
      for (const node of Array.from(document.querySelectorAll(selector)).slice(0, 10)) {
        push(node.innerText || node.textContent || node.getAttribute('title') || '');
      }
    }
    const body = document.body ? String(document.body.innerText || '') : '';
    for (const line of body.split(/\n+/).slice(0, 220)) push(line);
    return { title: String(document.title || ''), candidates };
    """
    try:
        payload = page.run_js(js) or {}
    except Exception:
        return {"title": "", "name": "", "token": "", "candidates": []}
    title = str(payload.get("title") or "")
    raw_candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    generic_blacklist = {
        "视频号", "助手", "视频号助手", "数据中心", "关注者数据", "视频数据", "图文数据", "直播数据", "带货数据",
        "内容管理", "互动管理", "通知中心", "下载表格", "全部", "近7天", "近30天", "自定义", "统计时间",
    }
    seen: set[str] = set()
    cleaned: list[tuple[str, str]] = []
    for candidate in raw_candidates:
        raw_text = str(candidate or "").strip()
        if not raw_text:
            continue
        token = _normalize_account_match_token(raw_text)
        if not token or token in seen:
            continue
        seen.add(token)
        if any(keyword in raw_text for keyword in generic_blacklist):
            continue
        cleaned.append((raw_text, token))
    cleaned.sort(key=lambda item: len(item[1]), reverse=True)
    best_name, best_token = cleaned[0] if cleaned else ("", "")
    return {"title": title, "name": best_name, "token": best_token, "candidates": cleaned[:10]}


def _assert_wechat_account_identity(page: Any, account: dict[str, Any]) -> dict[str, Any]:
    expected_tokens = _expected_account_match_tokens(account)
    if not expected_tokens:
        return {"checked": False, "reason": "no_expected_token"}
    identity = _extract_wechat_account_identity(page)
    observed_token = str(identity.get("token") or "")
    if not observed_token:
        return {"checked": False, "reason": "no_observed_token", **identity}
    matched = any(token and (token in observed_token or observed_token in token) for token in expected_tokens)
    if not matched:
        display_name = str(account.get("display_name") or account.get("account_key") or account.get("id") or "").strip()
        raise StatsAccountMismatch(
            f"account_mismatch expected={display_name} observed={identity.get('name') or identity.get('title') or '-'}"
        )
    return {"checked": True, "expected_tokens": expected_tokens, **identity}


def _open_capture_work_page(
    page: Any,
    *,
    open_capture_in_new_tab: bool = False,
    capture_tab_foreground: bool = False,
) -> tuple[Any, bool]:
    if not bool(open_capture_in_new_tab):
        return page, False
    try:
        tab = page.new_tab(background=not bool(capture_tab_foreground))
    except Exception:
        tab = None
    if tab is None:
        return page, False
    return tab, True


def _close_capture_tab_quietly(page: Any) -> None:
    close = getattr(page, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _capture_account_legacy(
    account: dict[str, Any],
    *,
    target_date: str,
    run_id: str,
    keep_browser_open_on_login_required: bool = False,
    auto_open_browser: bool = True,
    open_capture_in_new_tab: bool = False,
    capture_tab_foreground: bool = False,
    keep_capture_tab_open: bool = False,
) -> dict[str, Any]:
    account_id = int(account.get("id") or 0)
    profile_dir = str(account.get("wechat_profile_dir") or "")
    debug_port = int(account.get("wechat_debug_port") or 0)
    fingerprint = account.get("wechat_fingerprint") or {}
    apply_runtime_environment = apply_cybercar_environment
    apply_runtime_environment()
    try:
        service._configure_account_clash_pure_vpn(account)  # type: ignore[attr-defined]
    except Exception:
        pass
    page = None
    capture_page = None
    capture_page_is_new_tab = False
    hold_browser_for_login = False
    try:
        with service._account_network_env(account), service._chrome_fingerprint_env(fingerprint):  # type: ignore[attr-defined]
            page = engine._connect_chrome(  # type: ignore[attr-defined]
                debug_port=debug_port,
                auto_open_chrome=bool(auto_open_browser),
                chrome_user_data_dir=profile_dir,
                startup_url=_stats_urls()[0],
            )
        capture_page, capture_page_is_new_tab = _open_capture_work_page(
            page,
            open_capture_in_new_tab=open_capture_in_new_tab,
            capture_tab_foreground=capture_tab_foreground,
        )
        for url in _stats_urls():
            try:
                capture_page.get(url)
                time.sleep(1.2)
                login_state = engine.inspect_platform_login_gate(capture_page, "wechat")
                if bool(login_state.get("needs_login")):
                    hold_browser_for_login = keep_browser_open_on_login_required
                    raise StatsLoginRequired(str(login_state.get("reason") or "login_required"))
                _assert_wechat_account_identity(capture_page, account)
                _apply_date_filter(capture_page, target_date)
                raw = _extract_page_payload(capture_page)
                parsed = parse_wechat_stats_payload(raw, account_id=account_id, target_date=target_date)
                parsed["raw"] = raw
                return parsed
            except StatsLoginRequired:
                hold_browser_for_login = hold_browser_for_login or keep_browser_open_on_login_required
                raise
            except Exception:
                continue
        screenshot = _capture_screenshot(capture_page, run_id, account_id) if capture_page else ""
        raise StatsParseError(f"no supported wechat stats page parsed; screenshot={screenshot}")
    finally:
        if hold_browser_for_login:
            pass
        elif capture_page_is_new_tab and capture_page is not None:
            if not keep_capture_tab_open:
                _close_capture_tab_quietly(capture_page)
        elif page is not None:
            try:
                engine._disconnect_chrome_page_quietly(page)  # type: ignore[attr-defined]
            except Exception:
                pass


def _capture_account(
    account: dict[str, Any],
    *,
    target_date: str,
    run_id: str,
    keep_browser_open_on_login_required: bool = False,
    auto_open_browser: bool = True,
    open_capture_in_new_tab: bool = False,
    capture_tab_foreground: bool = False,
    keep_capture_tab_open: bool = False,
) -> dict[str, Any]:
    account_id = int(account.get("id") or 0)
    profile_dir = str(account.get("wechat_profile_dir") or "")
    debug_port = int(account.get("wechat_debug_port") or 0)
    fingerprint = account.get("wechat_fingerprint") or {}
    apply_cybercar_environment()
    try:
        service._configure_account_clash_pure_vpn(account)  # type: ignore[attr-defined]
    except Exception:
        pass
    page = None
    capture_page = None
    capture_page_is_new_tab = False
    hold_browser_for_login = False
    source_rows: list[dict[str, Any]] = []
    source_results: dict[str, Any] = {}
    missing_sources: list[str] = []
    try:
        with service._account_network_env(account), service._chrome_fingerprint_env(fingerprint):  # type: ignore[attr-defined]
            page = engine._connect_chrome(  # type: ignore[attr-defined]
                debug_port=debug_port,
                auto_open_chrome=bool(auto_open_browser),
                chrome_user_data_dir=profile_dir,
                startup_url=str(STATS_DOWNLOAD_SOURCES["follower"]["url"]),
            )
        capture_page, capture_page_is_new_tab = _open_capture_work_page(
            page,
            open_capture_in_new_tab=open_capture_in_new_tab,
            capture_tab_foreground=capture_tab_foreground,
        )
        capture_page.get(str(STATS_DOWNLOAD_SOURCES["follower"]["url"]))
        time.sleep(1.1)
        login_state = engine.inspect_platform_login_gate(capture_page, "wechat")
        if bool(login_state.get("needs_login")):
            hold_browser_for_login = keep_browser_open_on_login_required
            raise StatsLoginRequired(str(login_state.get("reason") or "login_required"))
        _assert_wechat_account_identity(capture_page, account)
        for source_key in ("follower", "post"):
            try:
                download = _capture_stats_source(capture_page, source_key=source_key, account_id=account_id, profile_dir=profile_dir, run_id=run_id)
                meta = {
                    "data_source": "dom_table",
                    "download_file": download.get("path", ""),
                    "download_hash": download.get("source_hash", ""),
                    "original_path": download.get("original_path", ""),
                }
                if source_key == "follower":
                    rows = _parse_follower_stats_rows(
                        download.get("table_rows") or [],
                        account_id=account_id,
                        captured_at=int(time.time()),
                        meta=meta,
                    )
                else:
                    rows = _parse_post_stats_rows(
                        download.get("table_rows") or [],
                        account_id=account_id,
                        captured_at=int(time.time()),
                        meta=meta,
                    )
                source_rows.extend(rows)
                source_results[source_key] = {"rows": len(rows), "table_rows": len(download.get("table_rows") or []), **meta}
            except StatsLoginRequired:
                hold_browser_for_login = hold_browser_for_login or keep_browser_open_on_login_required
                raise
            except Exception as exc:
                missing_sources.append(source_key)
                source_results[source_key] = {"rows": 0, "error": str(exc)}
        account_snapshots = _merge_daily_snapshots(source_rows)
        if account_snapshots:
            covered_dates = sorted({str(row.get("stat_date") or "") for row in account_snapshots if row.get("stat_date")})
            return {
                "account_snapshots": account_snapshots,
                "video_snapshots": [],
                "raw": {"sources": source_results, "missing_sources": missing_sources},
                "source_results": source_results,
                "missing_sources": missing_sources,
                "covered_dates": covered_dates,
            }
        if source_results:
            # CSV download succeeded but contains no dated data rows (header-only export).
            # Treat as an explicit no-data capture result instead of falling back to legacy text parsing.
            return {
                "account_snapshots": [],
                "video_snapshots": [],
                "raw": {"sources": source_results, "missing_sources": missing_sources},
                "source_results": source_results,
                "missing_sources": missing_sources,
                "covered_dates": [],
                "no_data": True,
            }
    finally:
        if hold_browser_for_login:
            pass
        elif capture_page_is_new_tab and capture_page is not None:
            if not keep_capture_tab_open:
                _close_capture_tab_quietly(capture_page)
        elif page is not None:
            try:
                engine._disconnect_chrome_page_quietly(page)  # type: ignore[attr-defined]
            except Exception:
                pass
    legacy = _capture_account_legacy(
        account,
        target_date=target_date,
        run_id=run_id,
        keep_browser_open_on_login_required=keep_browser_open_on_login_required,
        auto_open_browser=auto_open_browser,
        open_capture_in_new_tab=open_capture_in_new_tab,
        capture_tab_foreground=capture_tab_foreground,
        keep_capture_tab_open=keep_capture_tab_open,
    )
    legacy["source_results"] = source_results
    legacy["missing_sources"] = missing_sources or ["follower", "post"]
    return legacy


def _ordered_accounts(limit: int = 0, *, account_id: int | None = None) -> list[dict[str, Any]]:
    latest = service.latest_wechat_account_stats_index()
    accounts = list_wechat_accounts()
    if account_id and int(account_id) > 0:
        accounts = [item for item in accounts if int(item.get("id") or 0) == int(account_id)]
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
    account_id: int | None = None,
    keep_browser_open_on_login_required: bool = False,
    auto_open_browser: bool = True,
    open_capture_in_new_tab: bool = False,
    capture_tab_foreground: bool = False,
    keep_capture_tab_open: bool = False,
) -> dict[str, Any]:
    capture_date = normalize_target_date(target_date)
    run_id = f"wechat-stats-{capture_date}-{time.strftime('%H%M%S')}"
    selected_account_id = int(account_id or 0)
    accounts = _ordered_accounts(limit=max(0, int(limit or 0)), account_id=selected_account_id if selected_account_id > 0 else None)
    started_at = int(time.time())
    result: dict[str, Any] = {
        "ok": True,
        "run_id": run_id,
        "target_date": capture_date,
        "dry_run": bool(dry_run),
        "account_id": selected_account_id if selected_account_id > 0 else None,
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

    locked, lock_payload = _acquire_lock(selected_account_id if selected_account_id > 0 else None)
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
                parsed = _capture_account(
                    account,
                    target_date=capture_date,
                    run_id=run_id,
                    keep_browser_open_on_login_required=keep_browser_open_on_login_required,
                    auto_open_browser=auto_open_browser,
                    open_capture_in_new_tab=open_capture_in_new_tab,
                    capture_tab_foreground=capture_tab_foreground,
                    keep_capture_tab_open=keep_capture_tab_open,
                )
                account_snapshots = parsed.get("account_snapshots")
                if not isinstance(account_snapshots, list):
                    account_snapshots = [parsed["account_snapshot"]]
                video_snapshots = parsed.get("video_snapshots")
                if not isinstance(video_snapshots, list):
                    video_snapshots = []
                if not account_snapshots and not video_snapshots:
                    account_result = {
                        **event,
                        "status": "skipped",
                        "reason": "no_data",
                        "follower_rows": int((parsed.get("source_results") or {}).get("follower", {}).get("rows") or 0),
                        "post_rows": int((parsed.get("source_results") or {}).get("post", {}).get("rows") or 0),
                        "covered_dates": parsed.get("covered_dates") or [],
                        "missing_sources": parsed.get("missing_sources") or [],
                    }
                    result["skipped_accounts"] += 1
                    result["results"].append(account_result)
                    _append_event(run_id, account_result)
                    _write_json(status_path(), {**result, "status": "running", "updated_at": int(time.time())})
                    continue
                import_result = service.import_stats(
                    {
                        "account_snapshots": account_snapshots,
                        "video_snapshots": video_snapshots,
                    }
                )
                account_result = {
                    **event,
                    "status": "captured",
                    "videos": len(video_snapshots),
                    "follower_rows": int((parsed.get("source_results") or {}).get("follower", {}).get("rows") or 0),
                    "post_rows": int((parsed.get("source_results") or {}).get("post", {}).get("rows") or 0),
                    "covered_dates": parsed.get("covered_dates") or [],
                    "missing_sources": parsed.get("missing_sources") or [],
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
            except StatsAccountMismatch as exc:
                account_result = {
                    **event,
                    "status": "skipped",
                    "reason": "account_mismatch",
                    "error": str(exc),
                }
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
        _release_lock(selected_account_id if selected_account_id > 0 else None)
