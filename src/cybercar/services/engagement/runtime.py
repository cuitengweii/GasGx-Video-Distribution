from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

from ... import engine


DOUYIN_COMMENT_MANAGER_URL = "https://creator.douyin.com/creator-micro/interactive/comment"
KUAISHOU_COMMENT_MANAGER_URL = "https://cp.kuaishou.com/article/comment"

COMMENT_REPLY_STATE_FILES = {
    "douyin": "douyin_comment_reply_state.json",
    "kuaishou": "kuaishou_comment_reply_state.json",
}
COMMENT_REPLY_MARKDOWN_FILES = {
    "douyin": "douyin_comment_reply_records.md",
    "kuaishou": "kuaishou_comment_reply_records.md",
}

COMMENT_REPLY_RETENTION_DAYS = 30
COMMENT_REPLY_SELF_AUTHOR_MARKERS = ("cybercar",)
COMMENT_REPLY_SELF_AUTHOR_TOKENS = ("作者", "author", "浣滆")


@dataclass(frozen=True)
class CommentPlatformAdapter:
    platform: str
    open_url: str
    collect_posts: Callable[[Any, int, bool], list[dict[str, Any]]]
    open_post: Callable[[Any, dict[str, Any], bool], bool]
    extract_comments: Callable[[Any], list[dict[str, Any]]]
    like_comment_if_needed: Callable[[Any, int], bool]
    submit_reply: Callable[[Any, int, str], bool]
    wait_reply_confirm: Callable[[Any, int, str, float], bool]
    scroll_comments: Callable[[Any], None]


def _normalize_platform(platform_name: str) -> str:
    return str(platform_name or "").strip().lower()


def _state_path(workspace: engine.Workspace, platform_name: str) -> Path:
    platform = _normalize_platform(platform_name)
    filename = COMMENT_REPLY_STATE_FILES.get(platform) or f"{platform}_comment_reply_state.json"
    return workspace.root / filename


def _markdown_path(workspace: engine.Workspace, platform_name: str) -> Path:
    platform = _normalize_platform(platform_name)
    filename = COMMENT_REPLY_MARKDOWN_FILES.get(platform) or f"{platform}_comment_reply_records.md"
    return workspace.root / filename


def _load_state(workspace: engine.Workspace, platform_name: str) -> dict[str, Any]:
    path = _state_path(workspace, platform_name)
    if not path.exists():
        return {"updated_at": "", "items": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"updated_at": "", "items": {}}
    if not isinstance(payload, dict):
        return {"updated_at": "", "items": {}}
    items = payload.get("items")
    if not isinstance(items, dict):
        items = {}
    return {
        "updated_at": str(payload.get("updated_at") or "").strip(),
        "items": items,
    }


def _save_state(workspace: engine.Workspace, platform_name: str, state: dict[str, Any]) -> None:
    path = _state_path(workspace, platform_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _prune_state_items(items: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(items, dict):
        return {}
    cutoff = datetime.now() - timedelta(days=COMMENT_REPLY_RETENTION_DAYS)
    kept: dict[str, Any] = {}
    for fingerprint, raw in items.items():
        if not isinstance(raw, dict):
            continue
        replied_at = str(raw.get("replied_at") or "").strip()
        if not replied_at:
            continue
        try:
            replied_dt = datetime.strptime(replied_at, "%Y-%m-%d %H:%M:%S")
        except Exception:
            kept[str(fingerprint)] = raw
            continue
        if replied_dt >= cutoff:
            kept[str(fingerprint)] = raw
    return kept


def _normalize_post_title(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip().lower()


def _normalize_comment_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _should_skip_comment_reply(
    comment: dict[str, Any],
    *,
    self_author_markers: tuple[str, ...] = COMMENT_REPLY_SELF_AUTHOR_MARKERS,
) -> tuple[bool, str]:
    author_text = _normalize_comment_text(comment.get("author") if isinstance(comment, dict) else "")
    content_text = _normalize_comment_text(comment.get("content") if isinstance(comment, dict) else "")
    if not content_text:
        return True, "empty_comment_content"
    if not author_text:
        return False, ""
    author_norm = re.sub(r"\s+", "", author_text).lower()
    if "作者" in author_text and any(marker and marker in author_norm for marker in self_author_markers):
        return True, "self_author_comment"
    return False, ""


def _normalize_post_digits(text: str) -> str:
    return re.sub(r"\D+", "", str(text or ""))


def _should_skip_comment_reply_guarded(
    comment: dict[str, Any],
    *,
    self_author_markers: tuple[str, ...] = COMMENT_REPLY_SELF_AUTHOR_MARKERS,
) -> tuple[bool, str]:
    author_text = _normalize_comment_text(comment.get("author") if isinstance(comment, dict) else "")
    content_text = _normalize_comment_text(comment.get("content") if isinstance(comment, dict) else "")
    if not content_text:
        return True, "empty_comment_content"
    if not author_text:
        return False, ""
    author_norm = re.sub(r"\s+", "", author_text).lower()
    has_self_author_token = any(token and token in author_norm for token in COMMENT_REPLY_SELF_AUTHOR_TOKENS)
    has_self_author_marker = any(marker and marker in author_norm for marker in self_author_markers)
    if has_self_author_token and has_self_author_marker:
        return True, "self_author_comment"
    return False, ""


def _same_published_text(left: str, right: str) -> bool:
    a = str(left or "").strip()
    b = str(right or "").strip()
    if not a or not b:
        return not a and not b
    if a == b or a in b or b in a:
        return True
    da = _normalize_post_digits(a)
    db = _normalize_post_digits(b)
    return bool(da and db and (da == db or da.endswith(db) or db.endswith(da)))


def _post_title_matches(left: str, right: str) -> bool:
    a = _normalize_post_title(left)
    b = _normalize_post_title(right)
    if not a or not b:
        return False
    if a == b or a.startswith(b) or b.startswith(a) or a in b or b in a:
        return True
    min_prefix = min(len(a), len(b), 12)
    return min_prefix >= 6 and a[:min_prefix] == b[:min_prefix]


def _build_post_key(title: str, published_text: str) -> str:
    seed = f"{title}|{published_text}"
    return hashlib.sha1(seed.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _run_js_list(page: Any, script: str, *args: Any) -> list[dict[str, Any]]:
    try:
        payload = page.run_js(script, *args)
    except Exception:
        payload = []
    return payload if isinstance(payload, list) else []


def _run_js_dict(page: Any, script: str, *args: Any) -> dict[str, Any]:
    try:
        payload = page.run_js(script, *args)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    return payload if isinstance(payload, dict) else {"ok": bool(payload)}


def _wait_until(predicate: Callable[[], bool], timeout_seconds: float = 8.0, poll_seconds: float = 0.35) -> bool:
    deadline = time.time() + max(1.0, float(timeout_seconds))
    while time.time() < deadline:
        try:
            if bool(predicate()):
                return True
        except Exception:
            pass
        time.sleep(max(0.1, float(poll_seconds)))
    return False


def _scroll_container(page: Any, selector_candidates: list[str]) -> None:
    js = """
    return ((selectors) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const list = Array.isArray(selectors) ? selectors : [];
      for (const selector of list) {
        const node = document.querySelector(selector);
        if (visible(node)) {
          node.scrollTop += Math.max(node.clientHeight || 700, 700);
          return true;
        }
      }
      const root = document.scrollingElement || document.documentElement || document.body;
      if (!root) return false;
      root.scrollTop += Math.max(window.innerHeight || 900, 900);
      return true;
    })(arguments[0]);
    """
    try:
        page.run_js(js, selector_candidates)
    except Exception:
        pass


def _force_click_by_text(page: Any, texts: tuple[str, ...]) -> bool:
    keywords = [str(text or "").strip() for text in texts if str(text or "").strip()]
    if not keywords:
        return False
    js = """
    return ((keywords) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function actionable(node) {
        return node && (node.closest('button,[role="button"],a,[tabindex]') || node);
      }
      function fire(target, type) {
        const rect = target.getBoundingClientRect();
        const x = rect.left + Math.min(rect.width / 2, Math.max(4, rect.width - 4));
        const y = rect.top + Math.min(rect.height / 2, Math.max(4, rect.height - 4));
        const base = { bubbles: true, cancelable: true, composed: true, clientX: x, clientY: y, button: 0 };
        try {
          if (type.startsWith('pointer')) {
            target.dispatchEvent(new PointerEvent(type, Object.assign({ pointerId: 1, pointerType: 'mouse', isPrimary: true }, base)));
          } else {
            target.dispatchEvent(new MouseEvent(type, base));
          }
          return true;
        } catch (e) {
          return false;
        }
      }
      function robustClick(node) {
        const target = actionable(node);
        if (!target || !visible(target)) return false;
        try { target.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { target.focus(); } catch (e) {}
        try { target.click(); return true; } catch (e) {}
        fire(target, 'pointerover');
        fire(target, 'mouseover');
        fire(target, 'pointerdown');
        fire(target, 'mousedown');
        fire(target, 'pointerup');
        fire(target, 'mouseup');
        fire(target, 'click');
        return true;
      }
      const words = Array.isArray(keywords) ? keywords.map(norm).filter(Boolean) : [];
      if (!words.length) return { ok: false, reason: 'no_keywords' };
      const nodes = Array.from(document.querySelectorAll('button, [role="button"], a, div, span'))
        .filter(visible)
        .map((node) => {
          const text = norm(node.innerText || node.textContent || '');
          const rect = node.getBoundingClientRect();
          return { node, text, area: Math.round(rect.width * rect.height), width: rect.width, height: rect.height };
        })
        .filter((entry) => entry.text && words.some((word) => entry.text.includes(word)));
      if (!nodes.length) return { ok: false, reason: 'node_not_found' };
      nodes.sort((left, right) => {
        const leftExact = words.some((word) => left.text === word) ? 1 : 0;
        const rightExact = words.some((word) => right.text === word) ? 1 : 0;
        if (leftExact !== rightExact) return rightExact - leftExact;
        const leftCompact = left.text.length <= 12 ? 1 : 0;
        const rightCompact = right.text.length <= 12 ? 1 : 0;
        if (leftCompact !== rightCompact) return rightCompact - leftCompact;
        if (left.area !== right.area) return left.area - right.area;
        return left.text.length - right.text.length;
      });
      const chosen = nodes[0];
      return {
        ok: robustClick(chosen.node),
        reason: 'clicked',
        text: chosen.text,
      };
    })(arguments[0]);
    """
    return bool(_run_js_dict(page, js, keywords).get("ok"))


def _cdp_click_by_text(page: Any, texts: tuple[str, ...]) -> bool:
    keywords = [str(text or "").strip() for text in texts if str(text or "").strip()]
    if not keywords or not hasattr(page, "run_cdp"):
        return False
    js = """
    return ((keywords) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function actionable(node) {
        return node && (node.closest('button,[role="button"],a,[tabindex]') || node);
      }
      const words = Array.isArray(keywords) ? keywords.map(norm).filter(Boolean) : [];
      const candidates = Array.from(document.querySelectorAll('button, [role="button"], a, div, span'))
        .filter(visible)
        .map((node) => {
          const target = actionable(node);
          const text = norm(node.innerText || node.textContent || '');
          const targetText = norm(target ? (target.innerText || target.textContent || '') : '');
          const rect = (target || node).getBoundingClientRect();
          return {
            text,
            targetText,
            left: rect.left,
            top: rect.top,
            width: rect.width,
            height: rect.height,
            area: Math.round(rect.width * rect.height),
          };
        })
        .filter((entry) => entry.targetText && words.some((word) => entry.text === word || entry.targetText === word || entry.text.includes(word) || entry.targetText.includes(word)));
      if (!candidates.length) return null;
      candidates.sort((left, right) => {
        const leftExact = words.some((word) => left.text === word || left.targetText === word) ? 1 : 0;
        const rightExact = words.some((word) => right.text === word || right.targetText === word) ? 1 : 0;
        if (leftExact !== rightExact) return rightExact - leftExact;
        if (left.area !== right.area) return left.area - right.area;
        return left.targetText.length - right.targetText.length;
      });
      const best = candidates[0];
      return {
        x: Math.max(1, Math.round(best.left + best.width / 2)),
        y: Math.max(1, Math.round(best.top + best.height / 2)),
        text: best.targetText || best.text,
      };
    })(arguments[0]);
    """
    point = _run_js_dict(page, js, keywords)
    if "x" not in point or "y" not in point:
        return False
    try:
        x = float(point["x"])
        y = float(point["y"])
        page.run_cdp("Input.dispatchMouseEvent", type="mouseMoved", x=x, y=y, button="left", buttons=1, clickCount=0)
        page.run_cdp("Input.dispatchMouseEvent", type="mousePressed", x=x, y=y, button="left", buttons=1, clickCount=1)
        page.run_cdp("Input.dispatchMouseEvent", type="mouseReleased", x=x, y=y, button="left", buttons=0, clickCount=1)
        return True
    except Exception:
        return False


def _diagnose_douyin_page(page: Any) -> dict[str, Any]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const pickerButton = Array.from(document.querySelectorAll('button, div, span'))
        .find((node) => visible(node) && /选择作品/.test(norm(node.innerText || node.textContent || '')));
      const pickerList = document.querySelector('ul.douyin-creator-interactive-list-items');
      return {
        url: String(location.href || ''),
        title: String(document.title || ''),
        has_picker_button: !!pickerButton,
        has_picker_list: !!pickerList,
        picker_items: pickerList ? pickerList.children.length : 0,
        body_preview: norm((document.body && document.body.innerText) || '').slice(0, 500),
      };
    })();
    """
    return _run_js_dict(page, js)


def _diagnose_kuaishou_page(page: Any) -> dict[str, Any]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const pickerButton = Array.from(document.querySelectorAll('button, div, span'))
        .find((node) => visible(node) && /选择视频/.test(norm(node.innerText || node.textContent || '')));
      const items = Array.from(document.querySelectorAll('.video-item')).filter(visible);
      return {
        url: String(location.href || ''),
        title: String(document.title || ''),
        has_picker_button: !!pickerButton,
        picker_items: items.length,
        body_preview: norm((document.body && document.body.innerText) || '').slice(0, 500),
      };
    })();
    """
    return _run_js_dict(page, js)


def _ensure_douyin_picker_open(page: Any) -> bool:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function actionable(node) {
        if (!node) return null;
        return node.closest('button,[role="button"]') || node;
      }
      function click(node) {
        const target = actionable(node);
        if (!target) return false;
        try { target.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { target.click(); return true; } catch (e) {}
        try { target.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const list = document.querySelector('ul.douyin-creator-interactive-list-items');
      if (visible(list)) return { ok: true, already_open: true };
      const nodes = Array.from(document.querySelectorAll('button, div, span'))
        .filter(visible)
        .map((node) => {
          const text = norm(node.innerText || node.textContent || '');
          const rect = node.getBoundingClientRect();
          return { node, text, area: Math.round(rect.width * rect.height) };
        })
        .filter((entry) => entry.text.includes('选择作品'));
      nodes.sort((left, right) => {
        const leftExact = left.text === '选择作品' ? 1 : 0;
        const rightExact = right.text === '选择作品' ? 1 : 0;
        if (leftExact !== rightExact) return rightExact - leftExact;
        return left.area - right.area;
      });
      const trigger = nodes.length ? nodes[0].node : null;
      if (!trigger) return { ok: false, reason: 'trigger_not_found' };
      return { ok: click(trigger), reason: 'trigger_clicked' };
    })();
    """
    result = _run_js_dict(page, js)
    if (not bool(result.get("ok"))) and not _force_click_by_text(page, ("选择作品",)):
        result = _run_js_dict(page, js)
    if (not bool(result.get("ok"))) and not _cdp_click_by_text(page, ("选择作品",)):
        result = _run_js_dict(page, js)
    if (not bool(result.get("ok"))) and not engine._click_first_matching_button(page, page, ("选择作品",), platform_name="douyin"):
        return False
    return _wait_until(
        lambda: bool(
            _run_js_dict(
                page,
                """
                return {
                  ok: !!document.querySelector('ul.douyin-creator-interactive-list-items')
                    || /作品列表/.test(String((document.body && document.body.innerText) || ''))
                };
                """,
            ).get("ok")
        ),
        timeout_seconds=6.0,
        poll_seconds=0.25,
    )


def _extract_current_douyin_post(page: Any) -> Optional[dict[str, Any]]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      const cards = Array.from(document.querySelectorAll('div'))
        .map((node) => ({
          node,
          text: norm(node.innerText || node.textContent || ''),
        }))
        .filter((entry) => entry.text && /发布于\\d{4}年\\d{1,2}月\\d{1,2}日/.test(entry.text));
      const card = cards[0];
      if (!card) return null;
      const lines = card.text.split(/\\s+/).filter(Boolean);
      const published = lines.find((text) => /发布于\\d{4}年\\d{1,2}月\\d{1,2}日/.test(text)) || '';
      const title = lines.find((text) => text && text !== published && !/^选择作品$/.test(text) && text.length >= 4) || '';
      return { title, published_text: published };
    })();
    """
    payload = _run_js_dict(page, js)
    if not payload or not any(payload.get(key) for key in ("title", "published_text")):
        return None
    comments = _extract_douyin_comments(page)
    if not comments:
        return None
    title = str(payload.get("title") or "").strip()
    published_text = str(payload.get("published_text") or "").strip()
    return {
        "post_key": _build_post_key(title, published_text),
        "title": title,
        "published_text": published_text,
        "comment_count": len(comments),
        "has_comments": True,
    }


def _extract_douyin_picker_posts(page: Any) -> list[dict[str, Any]]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function toInt(value) {
        const raw = String(value || '').replace(/[^\\d]/g, '');
        return raw ? parseInt(raw, 10) : 0;
      }
      const list = document.querySelector('ul.douyin-creator-interactive-list-items');
      if (!visible(list)) return [];
      const items = Array.from(list.children).filter(visible);
      return items.map((item, index) => {
        const texts = Array.from(item.querySelectorAll('div, span, p'))
          .map((node) => norm(node.innerText || node.textContent || ''))
          .filter(Boolean);
        const published = texts.find((text) => /发布于|\\d{4}年\\d{1,2}月\\d{1,2}日|\\d{4}[\\/-]\\d{1,2}[\\/-]\\d{1,2}/.test(text)) || '';
        const title = texts.find((text) => {
          if (!text || text === published) return false;
          if (/^\\d+$/.test(text)) return false;
          return text.length >= 4;
        }) || '';
        const metricNodes = Array.from(item.querySelectorAll('[class*="right-"] div, [class*="right-"] span'))
          .filter(visible)
          .map((node) => norm(node.innerText || node.textContent || ''))
          .filter((text) => /^\\d+$/.test(text));
        const commentCount = metricNodes.length ? toInt(metricNodes[metricNodes.length - 1]) : 0;
        return {
          index,
          title,
          published_text: published,
          comment_count: commentCount,
          has_comments: commentCount > 0,
        };
      }).filter((item) => item.title || item.published_text);
    })();
    """
    payload = _run_js_list(page, js)
    result: list[dict[str, Any]] = []
    for item in payload:
        title = str(item.get("title") or "").strip()
        published_text = str(item.get("published_text") or "").strip()
        comment_count = max(0, int(item.get("comment_count") or 0))
        result.append(
            {
                "post_key": _build_post_key(title, published_text),
                "title": title,
                "published_text": published_text,
                "comment_count": comment_count,
                "has_comments": bool(item.get("has_comments")) or comment_count > 0,
            }
        )
    return result


def _scroll_douyin_picker(page: Any) -> None:
    _scroll_container(page, ["ul.douyin-creator-interactive-list-items"])


def _collect_douyin_commented_posts(page: Any, limit: int, debug: bool = False) -> list[dict[str, Any]]:
    if not _ensure_douyin_picker_open(page):
        current_post = _extract_current_douyin_post(page)
        if current_post:
            engine._comment_reply_log(debug, "[douyin] Picker not opened; fallback to current selected post")
            return [current_post]
        return []
    target = max(1, int(limit))
    collected: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    stagnant_rounds = 0
    for round_index in range(8):
        cards = _extract_douyin_picker_posts(page)
        before = len(collected)
        for card in cards:
            if int(card.get("comment_count") or 0) <= 0 and not bool(card.get("has_comments")):
                continue
            post_key = str(card.get("post_key") or "").strip()
            if not post_key or post_key in seen_keys:
                continue
            seen_keys.add(post_key)
            collected.append(card)
        engine._comment_reply_log(debug, f"[douyin] Visible commented posts: {len(collected)} after picker round {round_index}")
        if len(collected) >= target:
            break
        if len(collected) == before:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
        if stagnant_rounds >= 2:
            break
        _scroll_douyin_picker(page)
        time.sleep(0.8)
    if not collected:
        current_post = _extract_current_douyin_post(page)
        if current_post:
            engine._comment_reply_log(debug, "[douyin] Picker returned no commented posts; fallback to current selected post")
            return [current_post]
    return collected[:target]


def _open_douyin_post(page: Any, post: dict[str, Any], debug: bool = False) -> bool:
    if not _ensure_douyin_picker_open(page):
        return False
    js = """
    return ((targetTitle, targetPublished) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function digits(value) {
        return norm(value).replace(/[^\\d]/g, '');
      }
      function titleMatch(left, right) {
        const a = norm(left).replace(/\\s+/g, '').toLowerCase();
        const b = norm(right).replace(/\\s+/g, '').toLowerCase();
        if (!a || !b) return false;
        if (a === b || a.startsWith(b) || b.startsWith(a) || a.includes(b) || b.includes(a)) return true;
        const minPrefix = Math.min(a.length, b.length, 12);
        return minPrefix >= 6 && a.slice(0, minPrefix) === b.slice(0, minPrefix);
      }
      function samePublished(left, right) {
        const a = norm(left);
        const b = norm(right);
        if (!a || !b) return !a && !b;
        if (a === b || a.includes(b) || b.includes(a)) return true;
        const da = digits(a);
        const db = digits(b);
        return !!da && !!db && (da === db || da.endsWith(db) || db.endsWith(da));
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const list = document.querySelector('ul.douyin-creator-interactive-list-items');
      if (!visible(list)) return { ok: false, reason: 'picker_not_ready' };
      const items = Array.from(list.children).filter(visible);
      const parsed = items.map((item) => {
        const texts = Array.from(item.querySelectorAll('div, span, p'))
          .map((node) => norm(node.innerText || node.textContent || ''))
          .filter(Boolean);
        const published = texts.find((text) => /发布于|\\d{4}年\\d{1,2}月\\d{1,2}日|\\d{4}[\\/-]\\d{1,2}[\\/-]\\d{1,2}/.test(text)) || '';
        const title = texts.find((text) => {
          if (!text || text === published) return false;
          if (/^\\d+$/.test(text)) return false;
          return text.length >= 4;
        }) || '';
        return { item, title, published_text: published };
      });
      const target = parsed.find((entry) => titleMatch(entry.title, targetTitle) && samePublished(entry.published_text, targetPublished));
      if (!target) {
        return {
          ok: false,
          reason: 'post_not_found',
          visible_posts: parsed.slice(0, 6).map((entry) => ({ title: entry.title, published_text: entry.published_text })),
        };
      }
      return { ok: click(target.item), reason: 'clicked', matched_title: target.title, matched_published_text: target.published_text };
    })(arguments[0], arguments[1]);
    """
    result: dict[str, Any] = {}
    for _ in range(8):
        result = _run_js_dict(page, js, str(post.get("title") or ""), str(post.get("published_text") or ""))
        if bool(result.get("ok")):
            matched = _wait_until(lambda: bool(_extract_douyin_comments(page)), timeout_seconds=8.0, poll_seconds=0.35)
            return matched or True
        if str(result.get("reason") or "") != "post_not_found":
            return False
        _scroll_douyin_picker(page)
        time.sleep(0.8)
    engine._comment_reply_log(
        debug,
        "[douyin] Open post failed: " + json.dumps({"post": post, "visible": result.get("visible_posts") if isinstance(result, dict) else []}, ensure_ascii=False),
    )
    return False


def _extract_douyin_comments(page: Any) -> list[dict[str, Any]]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        if (!visible(node)) return false;
        if (!node.querySelector('[class*="username-"]')) return false;
        if (!node.querySelector('[class*="time-"]')) return false;
        if (!node.querySelector('[class*="comment-content-text-"]')) return false;
        if (!node.querySelector('[class*="operations-"]')) return false;
        return /回复|删除|举报/.test(norm(node.innerText || node.textContent || ''));
      }
      const roots = [];
      const seen = new Set();
      const operationNodes = Array.from(document.querySelectorAll('[class*="operations-"]'));
      for (const op of operationNodes) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      return roots.map((root, index) => {
        const author = norm(root.querySelector('[class*="username-"]') ? root.querySelector('[class*="username-"]').innerText : '');
        const timeText = norm(root.querySelector('[class*="time-"]') ? root.querySelector('[class*="time-"]').innerText : '');
        const content = norm(root.querySelector('[class*="comment-content-text-"]') ? root.querySelector('[class*="comment-content-text-"]').innerText : '');
        const replyNodes = Array.from(root.querySelectorAll('[class*="reply-content-"], [class*="reply-list-"]')).filter(visible);
        const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
        const likeAction = actions[0] || null;
        const likeColor = likeAction ? window.getComputedStyle(likeAction).color : '';
        const liked = !!(likeAction && !/rgba?\\(28,\\s*31,\\s*35,\\s*0\\.6\\)|rgba?\\(140,\\s*140,\\s*140/i.test(String(likeColor || '')) && !/回复/.test(norm(likeAction.innerText || likeAction.textContent || '')));
        return {
          index,
          author,
          time_text: timeText,
          content,
          has_reply: replyNodes.length > 0,
          liked,
        };
      }).filter((item) => item.content || item.author || item.time_text);
    })();
    """
    return _run_js_list(page, js)


def _like_douyin_comment_if_needed(page: Any, comment_index: int) -> bool:
    js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="time-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
      const likeAction = actions.find((node) => !/回复|删除|举报/.test(norm(node.innerText || node.textContent || ''))) || actions[0];
      if (!likeAction) return { ok: false };
      const color = window.getComputedStyle(likeAction).color || '';
      if (!/rgba?\\(28,\\s*31,\\s*35,\\s*0\\.6\\)|rgba?\\(140,\\s*140,\\s*140/i.test(String(color || ''))) return { ok: true };
      return { ok: click(likeAction) };
    })(arguments[0]);
    """
    return bool(_run_js_dict(page, js, int(comment_index)).get("ok"))


def _submit_douyin_reply(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
      const replyAction = actions.find((node) => /回复/.test(norm(node.innerText || node.textContent || '')));
      if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyAction) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False
    locate_js = """
    return ((commentIndex) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const sendButton = Array.from(scope.querySelectorAll('button, div, span')).find((node) => /发送/.test(String(node.innerText || node.textContent || '').trim()) && visible(node));
        if (input && visible(input) && sendButton) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False
    fill_js = """
    return ((commentIndex, replyText) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function setValue(input, value) {
        const text = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, text);
          else input.value = text;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        input.textContent = text;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        return true;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const sendButton = Array.from(scope.querySelectorAll('button, div, span')).find((node) => /发送/.test(String(node.innerText || node.textContent || '').trim()) && visible(node));
        if (!input || !visible(input) || !sendButton) continue;
        setValue(input, replyText);
        return { ok: click(sendButton) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_douyin_reply_v2(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
      const replyAction = actions.find((node) => norm(node.innerText || node.textContent || '') === '回复');
      if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyAction) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary') && text === '发送';
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (input && visible(input) && sendButton) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function setValue(input, value) {
        const text = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, text);
          else input.value = text;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        input.textContent = text;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        return true;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')
          && text === '发送'
          && String(node.getAttribute('aria-disabled') || '').toLowerCase() != 'true'
          && !node.disabled;
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (!input || !visible(input) || !sendButton) continue;
        setValue(input, replyText);
        return { ok: click(sendButton) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_douyin_reply_v3(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
      const replyAction = actions.find((node) => norm(node.innerText || node.textContent || '') === '回复');
      if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyAction) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary') && text === '发送';
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (input && visible(input) && sendButton) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function triggerEditable(input, prefix, reply) {
        const text = String(reply || '');
        const fullText = `${String(prefix || '')}${text}`;
        input.focus();
        try {
          const selection = window.getSelection();
          if (selection) {
            const range = document.createRange();
            range.selectNodeContents(input);
            range.collapse(false);
            selection.removeAllRanges();
            selection.addRange(range);
          }
        } catch (e) {}
        try {
          document.execCommand('selectAll', false, null);
          document.execCommand('delete', false, null);
        } catch (e) {}
        input.textContent = fullText;
        try {
          document.execCommand('insertText', false, fullText);
        } catch (e) {}
        input.dispatchEvent(new InputEvent('beforeinput', { bubbles: true, cancelable: true, data: text, inputType: 'insertText' }));
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        input.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'a' }));
        input.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'a' }));
        return norm(input.innerText || input.textContent || '').includes(norm(text));
      }
      function setValue(input, value) {
        const reply = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, reply);
          else input.value = reply;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        const current = norm(input.innerText || input.textContent || '');
        const nextText = current && current.startsWith('回复') && !current.includes(reply) ? `${current}${reply}` : reply;
        return triggerEditable(input, nextText);
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')
          && text === '发送'
          && String(node.getAttribute('aria-disabled') || '').toLowerCase() !== 'true'
          && !node.disabled;
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
        if (!input || !visible(input)) continue;
        if (!setValue(input, replyText)) continue;
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (!sendButton) continue;
        return { ok: click(sendButton) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_douyin_reply_v4(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
      const replyAction = actions.find((node) => norm(node.innerText || node.textContent || '') === '回复');
      if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyAction) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary') && text === '发送';
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (input && visible(input) && sendButton) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function placeCaretAtEnd(node) {
        try {
          const selection = window.getSelection();
          if (!selection) return;
          const range = document.createRange();
          range.selectNodeContents(node);
          range.collapse(false);
          selection.removeAllRanges();
          selection.addRange(range);
        } catch (e) {}
      }
      function setValue(input, value) {
        const reply = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, reply);
          else input.value = reply;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        const currentRaw = String(input.innerText || input.textContent || '');
        const current = norm(currentRaw);
        const hasReplyPrefix = current && current.startsWith('回复') && !current.includes(norm(reply));
        const fullText = hasReplyPrefix ? `${currentRaw}${reply}` : reply;
        input.focus();
        placeCaretAtEnd(input);
        try {
          document.execCommand('insertText', false, reply);
        } catch (e) {}
        const afterExec = norm(input.innerText || input.textContent || '');
        if (!afterExec.includes(norm(reply))) {
          input.textContent = fullText;
        }
        input.dispatchEvent(new InputEvent('beforeinput', { bubbles: true, cancelable: true, data: reply, inputType: 'insertText' }));
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: reply, inputType: 'insertText' }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        input.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'a' }));
        input.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'a' }));
        return norm(input.innerText || input.textContent || '').includes(norm(reply));
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')
          && text === '发送'
          && String(node.getAttribute('aria-disabled') || '').toLowerCase() !== 'true'
          && !node.disabled;
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = scope.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
        if (!input || !visible(input)) continue;
        if (!setValue(input, replyText)) continue;
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (!sendButton) continue;
        return { ok: click(sendButton) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_douyin_reply_v5(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
      const replyAction = actions.find((node) => norm(node.innerText || node.textContent || '') === '回复');
      if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyAction) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary') && text === '发送';
      }
      function findEditor(scope) {
        const exact = Array.from(scope.querySelectorAll('div[contenteditable="true"]')).find((node) => {
          if (!visible(node)) return false;
          const placeholder = norm(node.getAttribute('placeholder') || node.getAttribute('aria-placeholder') || '');
          const cls = String(node.className || '');
          return placeholder.startsWith('回复') || cls.includes('input-');
        });
        if (exact) return exact;
        return scope.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = findEditor(scope);
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (input && visible(input) && sendButton) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function placeCaretAtEnd(node) {
        try {
          const selection = window.getSelection();
          if (!selection) return;
          const range = document.createRange();
          range.selectNodeContents(node);
          range.collapse(false);
          selection.removeAllRanges();
          selection.addRange(range);
        } catch (e) {}
      }
      function findEditor(scope) {
        const exact = Array.from(scope.querySelectorAll('div[contenteditable="true"]')).find((node) => {
          if (!visible(node)) return false;
          const placeholder = norm(node.getAttribute('placeholder') || node.getAttribute('aria-placeholder') || '');
          const cls = String(node.className || '');
          return placeholder.startsWith('回复') || cls.includes('input-');
        });
        if (exact) return exact;
        return scope.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
      }
      function setValue(input, value) {
        const reply = String(value || '');
        if (!input) return false;
        click(input);
        input.focus();
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, reply);
          else input.value = reply;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        const currentRaw = String(input.innerText || input.textContent || '');
        const current = norm(currentRaw);
        const hasReplyPrefix = current && current.startsWith('回复') && !current.includes(norm(reply));
        const fullText = hasReplyPrefix ? `${currentRaw}${reply}` : reply;
        placeCaretAtEnd(input);
        try {
          document.execCommand('insertText', false, reply);
        } catch (e) {}
        const afterExec = norm(input.innerText || input.textContent || '');
        if (!afterExec.includes(norm(reply))) {
          input.textContent = fullText;
        }
        input.dispatchEvent(new InputEvent('beforeinput', { bubbles: true, cancelable: true, data: reply, inputType: 'insertText' }));
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: reply, inputType: 'insertText' }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        input.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'a' }));
        input.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'a' }));
        return norm(input.innerText || input.textContent || '').includes(norm(reply));
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')
          && text === '发送'
          && String(node.getAttribute('aria-disabled') || '').toLowerCase() !== 'true'
          && !node.disabled;
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = findEditor(scope);
        if (!input || !visible(input)) continue;
        if (!setValue(input, replyText)) continue;
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (!sendButton) continue;
        return { ok: click(sendButton) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_douyin_reply_v6(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
      const replyAction = actions.find((node) => norm(node.innerText || node.textContent || '') === '回复');
      if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyAction) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary') && text === '发送';
      }
      function findEditor(scope) {
        const candidates = Array.from(scope.querySelectorAll(
          'div[class*="reply-content-"] div[class*="input-"][contenteditable="true"], div[class*="wrap-"] div[class*="input-"][contenteditable="true"], div[class*="input-"][contenteditable="true"], div[contenteditable="true"][placeholder], div[contenteditable="true"][aria-placeholder], div[contenteditable="true"][aria-label]'
        )).filter(visible);
        return candidates[0] || null;
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = findEditor(scope);
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (input && sendButton) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    activate_editor_js = """
    return ((commentIndex) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.focus(); } catch (e) {}
        try { node.click(); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        try { node.dispatchEvent(new FocusEvent('focus', { bubbles: true })); } catch (e) {}
        return true;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function findEditor(scope) {
        const candidates = Array.from(scope.querySelectorAll(
          'div[class*="reply-content-"] div[class*="input-"][contenteditable="true"], div[class*="wrap-"] div[class*="input-"][contenteditable="true"], div[class*="input-"][contenteditable="true"], div[contenteditable="true"][placeholder], div[contenteditable="true"][aria-placeholder], div[contenteditable="true"][aria-label]'
        )).filter(visible);
        return candidates[0] || null;
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = findEditor(scope);
        if (!input) continue;
        click(input);
        const active = document.activeElement === input;
        const selection = window.getSelection();
        const selectedInside = !!(selection && selection.anchorNode && input.contains(selection.anchorNode));
        return { ok: active || selectedInside || true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, activate_editor_js, int(comment_index)).get("ok")):
        return False

    fill_js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.focus(); } catch (e) {}
        try { node.click(); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
        return true;
      }
      function placeCaretAtEnd(node) {
        try {
          const selection = window.getSelection();
          if (!selection) return;
          const range = document.createRange();
          range.selectNodeContents(node);
          range.collapse(false);
          selection.removeAllRanges();
          selection.addRange(range);
        } catch (e) {}
      }
      function findEditor(scope) {
        const candidates = Array.from(scope.querySelectorAll(
          'div[class*="reply-content-"] div[class*="input-"][contenteditable="true"], div[class*="wrap-"] div[class*="input-"][contenteditable="true"], div[class*="input-"][contenteditable="true"], div[contenteditable="true"][placeholder], div[contenteditable="true"][aria-placeholder], div[contenteditable="true"][aria-label]'
        )).filter(visible);
        return candidates[0] || null;
      }
      function setValue(input, value) {
        const reply = String(value || '');
        if (!input) return false;
        click(input);
        input.focus();
        const currentRaw = String(input.innerText || input.textContent || '');
        const current = norm(currentRaw);
        const hasReplyPrefix = current && current.startsWith('回复') && !current.includes(norm(reply));
        const fullText = hasReplyPrefix ? `${currentRaw}${reply}` : reply;
        placeCaretAtEnd(input);
        try {
          document.execCommand('insertText', false, reply);
        } catch (e) {}
        const afterExec = norm(input.innerText || input.textContent || '');
        if (!afterExec.includes(norm(reply))) {
          input.textContent = fullText;
        }
        input.dispatchEvent(new InputEvent('beforeinput', { bubbles: true, cancelable: true, data: reply, inputType: 'insertText' }));
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: reply, inputType: 'insertText' }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        input.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'a' }));
        input.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'a' }));
        return norm(input.innerText || input.textContent || '').includes(norm(reply));
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function isSendButton(node) {
        if (!visible(node)) return false;
        const text = norm(node.innerText || node.textContent || '');
        return node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')
          && text === '发送'
          && String(node.getAttribute('aria-disabled') || '').toLowerCase() !== 'true'
          && !node.disabled;
      }
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const input = findEditor(scope);
        if (!input) continue;
        if (!setValue(input, replyText)) continue;
        const sendButton = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSendButton);
        if (!sendButton) continue;
        return { ok: click(sendButton) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_douyin_reply_v7(page: Any, comment_index: int, reply_text: str) -> bool:
    if not bool(
        _run_js_dict(
            page,
            """
            return ((commentIndex) => {
              function norm(value) {
                return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
              }
              function visible(node) {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
              }
              function click(node) {
                if (!node) return false;
                try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
                try { node.click(); return true; } catch (e) {}
                try { node.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
                try { node.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
                try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
                return false;
              }
              function isRoot(node) {
                return visible(node)
                  && !!node.querySelector('[class*="username-"]')
                  && !!node.querySelector('[class*="comment-content-text-"]')
                  && !!node.querySelector('[class*="operations-"]');
              }
              const roots = [];
              const seen = new Set();
              for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
                let node = op;
                while (node && node !== document.body) {
                  if (isRoot(node)) {
                    if (!seen.has(node)) {
                      seen.add(node);
                      roots.push(node);
                    }
                    break;
                  }
                  node = node.parentElement;
                }
              }
              const root = roots[Number(commentIndex)];
              if (!root) return { ok: false, reason: 'comment_not_found' };
              const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
              const replyAction = actions.find((node) => norm(node.innerText || node.textContent || '') === '回复');
              if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
              return { ok: click(replyAction) };
            })(arguments[0]);
            """,
            int(comment_index),
        ).get("ok")
    ):
        return False

    selector_candidates = (
        "css:div[contenteditable='true'][placeholder*='回复']",
        "css:div[contenteditable='true'][aria-placeholder*='回复']",
        "css:div[contenteditable='true'][aria-label*='回复']",
        "xpath://div[@contenteditable='true' and (contains(@placeholder,'回复') or contains(@aria-placeholder,'回复') or contains(@aria-label,'回复'))][1]",
        "css:div[class*='input-'][contenteditable='true']",
        "xpath://div[contains(@class,'input-') and @contenteditable='true'][1]",
    )

    editor = None
    if hasattr(page, "ele"):
        for selector in selector_candidates:
            try:
                candidate = page.ele(selector, timeout=1.5)
            except Exception:
                candidate = None
            if not candidate:
                continue
            if not engine._is_visible_element(candidate):
                continue
            editor = candidate
            break

    if editor is not None:
        try:
            editor.click(by_js=False)
        except Exception:
            try:
                editor.click(by_js=True)
            except Exception:
                pass
        if not engine._input_text_field_with_keyboard(editor, str(reply_text or "")):
            return False
        return _click_douyin_send_near_active_editor(page)

        send_selectors = (
            "xpath://button[contains(@class,'douyin-creator-interactive-button-primary') and normalize-space()='发送']",
            "css:button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary",
        )
        for selector in send_selectors:
            try:
                btn = page.ele(selector, timeout=1.0)
            except Exception:
                btn = None
            if not btn:
                continue
            if not engine._is_visible_element(btn):
                continue
            try:
                btn.click()
                return True
            except Exception:
                try:
                    btn.click(by_js=True)
                    return True
                except Exception:
                    continue
        return False

    return _submit_douyin_reply_v6(page, comment_index, reply_text)


def _submit_douyin_reply_v8(page: Any, comment_index: int, reply_text: str) -> bool:
    if not bool(
        _run_js_dict(
            page,
            """
            return ((commentIndex) => {
              function norm(value) {
                return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
              }
              function visible(node) {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
              }
              function click(node) {
                if (!node) return false;
                try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
                try { node.click(); return true; } catch (e) {}
                try { node.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
                try { node.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
                try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
                return false;
              }
              function isRoot(node) {
                return visible(node)
                  && !!node.querySelector('[class*="username-"]')
                  && !!node.querySelector('[class*="comment-content-text-"]')
                  && !!node.querySelector('[class*="operations-"]');
              }
              const roots = [];
              const seen = new Set();
              for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
                let node = op;
                while (node && node !== document.body) {
                  if (isRoot(node)) {
                    if (!seen.has(node)) {
                      seen.add(node);
                      roots.push(node);
                    }
                    break;
                  }
                  node = node.parentElement;
                }
              }
              const root = roots[Number(commentIndex)];
              if (!root) return { ok: false, reason: 'comment_not_found' };
              const actions = Array.from(root.querySelectorAll('[class*="operations-"] [class*="item-"]')).filter(visible);
              const replyAction = actions.find((node) => norm(node.innerText || node.textContent || '') === '回复');
              if (!replyAction) return { ok: false, reason: 'reply_button_not_found' };
              return { ok: click(replyAction) };
            })(arguments[0]);
            """,
            int(comment_index),
        ).get("ok")
    ):
        return False

    selector_candidates = (
        "css:div[contenteditable='true'][placeholder*='回复']",
        "css:div[contenteditable='true'][aria-placeholder*='回复']",
        "css:div[contenteditable='true'][aria-label*='回复']",
        "css:div[class*='reply-content-'] div[class*='input-'][contenteditable='true']",
        "css:div[class*='wrap-'] div[class*='input-'][contenteditable='true']",
        "css:div[class*='input-'][contenteditable='true']",
        "xpath://div[@contenteditable='true' and (contains(@placeholder,'回复') or contains(@aria-placeholder,'回复') or contains(@aria-label,'回复'))][1]",
        "xpath://div[contains(@class,'input-') and @contenteditable='true'][1]",
    )

    editor = None
    if hasattr(page, "ele"):
        for selector in selector_candidates:
            try:
                candidate = page.ele(selector, timeout=1.2)
            except Exception:
                candidate = None
            if not candidate:
                continue
            if not engine._is_visible_element(candidate):
                continue
            editor = candidate
            break

    clean_reply_text = str(reply_text or "").strip()
    active_editor_ready = bool(
        _run_js_dict(
            page,
            """
            return (() => {
              function norm(value) {
                return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
              }
              const active = document.activeElement;
              if (!active) return { ok: false };
              const editable = active.isContentEditable || String(active.getAttribute('contenteditable') || '').toLowerCase() === 'true';
              if (!editable) return { ok: false };
              const attrs = [
                active.getAttribute('placeholder') || '',
                active.getAttribute('aria-placeholder') || '',
                active.getAttribute('aria-label') || '',
                String(active.className || ''),
              ].join(' ');
              return { ok: /回复/.test(norm(attrs)) || String(active.className || '').includes('input-') };
            })();
            """,
        ).get("ok")
    )

    if editor is None and not active_editor_ready:
        return _submit_douyin_reply_v7(page, comment_index, reply_text)

    if editor is not None:
        try:
            editor.click(by_js=False)
        except Exception:
            try:
                editor.click(by_js=True)
            except Exception:
                pass

        try:
            editor.run_js(
                """
                this.focus();
                try {
                  const selection = window.getSelection();
                  if (selection) {
                    const range = document.createRange();
                    range.selectNodeContents(this);
                    range.collapse(false);
                    selection.removeAllRanges();
                    selection.addRange(range);
                  }
                } catch (e) {}
                """
            )
        except Exception:
            pass

    typed = False
    if hasattr(page, "run_cdp") and clean_reply_text:
        try:
            page.run_cdp("Input.insertText", text=clean_reply_text)
            typed = True
        except Exception:
            typed = False

    if not typed and not engine._input_text_field_with_keyboard(editor, clean_reply_text):
        return False

    verify_js = """
    return ((replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      const active = document.activeElement;
      if (!active) return { ok: false };
      const text = norm(active.innerText || active.textContent || active.value || '');
      return { ok: !!(norm(replyText) && text.includes(norm(replyText))) };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, verify_js, clean_reply_text).get("ok")), timeout_seconds=3.0, poll_seconds=0.2):
        return False
    return _click_douyin_send_near_active_editor(page)

    send_selectors = (
        "xpath://button[contains(@class,'douyin-creator-interactive-button-primary') and normalize-space()='发送']",
        "css:button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary",
    )
    for selector in send_selectors:
        try:
            btn = page.ele(selector, timeout=0.8)
        except Exception:
            btn = None
        if not btn:
            continue
        if not engine._is_visible_element(btn):
            continue
        try:
            btn.click()
            return True
        except Exception:
            try:
                btn.click(by_js=True)
                return True
            except Exception:
                continue
    return False


def _submit_douyin_reply_v9(page: Any, comment_index: int, reply_text: str) -> bool:
    clean_reply_text = str(reply_text or "").strip()
    active_editor_ready = bool(
        _run_js_dict(
            page,
            """
            return (() => {
              function norm(value) {
                return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
              }
              const active = document.activeElement;
              if (!active) return { ok: false };
              const editable = active.isContentEditable || String(active.getAttribute('contenteditable') || '').toLowerCase() === 'true';
              if (!editable) return { ok: false };
              const attrs = [
                active.getAttribute('placeholder') || '',
                active.getAttribute('aria-placeholder') || '',
                active.getAttribute('aria-label') || '',
                String(active.className || ''),
              ].join(' ');
              return { ok: /鍥炲/.test(norm(attrs)) || String(active.className || '').includes('input-') };
            })();
            """,
        ).get("ok")
    )

    if active_editor_ready and clean_reply_text:
        typed = False
        if hasattr(page, "run_cdp"):
            try:
                page.run_cdp("Input.insertText", text=clean_reply_text)
                typed = True
            except Exception:
                typed = False

        verify_js = """
        return ((replyText) => {
          function norm(value) {
            return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
          }
          const active = document.activeElement;
          if (!active) return { ok: false };
          const text = norm(active.innerText || active.textContent || active.value || '');
          return { ok: !!(norm(replyText) && text.includes(norm(replyText))) };
        })(arguments[0]);
        """
        if typed and _wait_until(lambda: bool(_run_js_dict(page, verify_js, clean_reply_text).get("ok")), timeout_seconds=3.0, poll_seconds=0.2):
            return _click_douyin_send_near_active_editor(page)
            send_selectors = (
                "xpath://button[contains(@class,'douyin-creator-interactive-button-primary') and normalize-space()='鍙戦€?]",
                "css:button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary",
            )
            if hasattr(page, "ele"):
                for selector in send_selectors:
                    try:
                        btn = page.ele(selector, timeout=0.8)
                    except Exception:
                        btn = None
                    if not btn:
                        continue
                    if not engine._is_visible_element(btn):
                        continue
                    try:
                        btn.click()
                        return True
                    except Exception:
                        try:
                            btn.click(by_js=True)
                            return True
                        except Exception:
                            continue

    return _submit_douyin_reply_v8(page, comment_index, reply_text)


def _wait_douyin_reply_confirm(page: Any, comment_index: int, reply_text: str, timeout_seconds: float = 12.0) -> bool:
    target = re.sub(r"\s+", "", str(reply_text or "").strip())
    js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, '').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isRoot(node) {
        return visible(node)
          && !!node.querySelector('[class*="username-"]')
          && !!node.querySelector('[class*="comment-content-text-"]')
          && !!node.querySelector('[class*="operations-"]');
      }
      function cleanText(node) {
        if (!node) return '';
        const clone = node.cloneNode(true);
        Array.from(clone.querySelectorAll('[class*="reply-content-"], [class*="wrap-"], textarea, input, [contenteditable="true"], [class*="operations-"], button')).forEach((child) => child.remove());
        return norm(clone.innerText || clone.textContent || '');
      }
      const target = norm(replyText);
      const roots = [];
      const seen = new Set();
      for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
        let node = op;
        while (node && node !== document.body) {
          if (isRoot(node)) {
            if (!seen.has(node)) {
              seen.add(node);
              roots.push(node);
            }
            break;
          }
          node = node.parentElement;
        }
      }
      const root = roots[Number(commentIndex)];
      if (!root || !target) return false;
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      return scopes.some((scope) => cleanText(scope).includes(target));
    })(arguments[0], arguments[1]);
    """
    return _wait_until(
        lambda: bool(_run_js_dict(page, js, int(comment_index), target).get("ok")),
        timeout_seconds=timeout_seconds,
        poll_seconds=0.35,
    )


def _click_douyin_send_near_active_editor(page: Any) -> bool:
    payload = _run_js_dict(
        page,
        """
        return (() => {
          function norm(value) {
            return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
          }
          function visible(node) {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            const rect = node.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
          }
          function isSend(node) {
            if (!visible(node)) return false;
            if (!node.matches('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')) return false;
            if (String(node.getAttribute('aria-disabled') || '').toLowerCase() === 'true') return false;
            if (node.disabled) return false;
            return norm(node.innerText || node.textContent || '') === '\\u53d1\\u9001';
          }
          const active = document.activeElement;
          if (!active) return { ok: false, reason: 'active_editor_missing' };
          const activeRect = active.getBoundingClientRect();
          const scopes = [];
          const seen = new Set();
          for (const node of [active, active.parentElement, active.parentElement && active.parentElement.parentElement, active.closest('[class*="reply-content-"]'), active.closest('[class*="wrap-"]')]) {
            if (!node || seen.has(node)) continue;
            seen.add(node);
            scopes.push(node);
          }
          let sendButton = null;
          for (const scope of scopes) {
            const direct = Array.from(scope.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).find(isSend);
            if (direct) {
              sendButton = direct;
              break;
            }
          }
          if (!sendButton) {
            const candidates = Array.from(document.querySelectorAll('button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary')).filter(isSend);
            candidates.sort((left, right) => {
              const a = left.getBoundingClientRect();
              const b = right.getBoundingClientRect();
              const da = Math.abs(a.top - activeRect.top) + Math.abs(a.left - activeRect.left);
              const db = Math.abs(b.top - activeRect.top) + Math.abs(b.left - activeRect.left);
              return da - db;
            });
            sendButton = candidates[0] || null;
          }
          if (!sendButton) return { ok: false, reason: 'send_button_not_found' };
          try { sendButton.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
          try { sendButton.click(); return { ok: true }; } catch (e) {}
          try { sendButton.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
          try { sendButton.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window })); } catch (e) {}
          try { sendButton.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return { ok: true }; } catch (e) {}
          return { ok: false, reason: 'send_click_failed' };
        })();
        """,
    )
    return bool(payload.get("ok"))


def _scroll_douyin_comments(page: Any) -> None:
    _scroll_container(page, ["[class*='comment-list']", "[class*='scroll-']", "main"])


def _ensure_kuaishou_picker_open(page: Any) -> bool:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function actionable(node) {
        if (!node) return null;
        return node.closest('button,[role="button"]') || node;
      }
      function click(node) {
        const target = actionable(node);
        if (!target) return false;
        try { target.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { target.click(); return true; } catch (e) {}
        try { target.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const item = document.querySelector('.video-item');
      if (visible(item)) return { ok: true, already_open: true };
      const nodes = Array.from(document.querySelectorAll('button, div, span'))
        .filter(visible)
        .map((node) => {
          const text = norm(node.innerText || node.textContent || '');
          const rect = node.getBoundingClientRect();
          return { node, text, area: Math.round(rect.width * rect.height) };
        })
        .filter((entry) => entry.text.includes('选择视频'));
      nodes.sort((left, right) => {
        const leftExact = left.text === '选择视频' ? 1 : 0;
        const rightExact = right.text === '选择视频' ? 1 : 0;
        if (leftExact !== rightExact) return rightExact - leftExact;
        return left.area - right.area;
      });
      const trigger = nodes.length ? nodes[0].node : null;
      if (!trigger) return { ok: false, reason: 'trigger_not_found' };
      return { ok: click(trigger) };
    })();
    """
    result = _run_js_dict(page, js)
    if (not bool(result.get("ok"))) and not _force_click_by_text(page, ("选择视频",)):
        result = _run_js_dict(page, js)
    if (not bool(result.get("ok"))) and not _cdp_click_by_text(page, ("选择视频",)):
        result = _run_js_dict(page, js)
    if (not bool(result.get("ok"))) and not engine._click_first_matching_button(page, page, ("选择视频",), platform_name="kuaishou"):
        return False
    return _wait_until(
        lambda: bool(
            _run_js_dict(
                page,
                """
                return {
                  ok: !!document.querySelector('.video-item')
                    || /作品列表/.test(String((document.body && document.body.innerText) || ''))
                };
                """,
            ).get("ok")
        ),
        timeout_seconds=6.0,
        poll_seconds=0.25,
    )


def _extract_current_kuaishou_post(page: Any) -> Optional[dict[str, Any]]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      const blocks = Array.from(document.querySelectorAll('div'))
        .map((node) => ({
          node,
          text: norm(node.innerText || node.textContent || ''),
        }))
        .filter((entry) => entry.text && /\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}/.test(entry.text));
      const block = blocks[0];
      if (!block) return null;
      const texts = block.text.split(/\\s+/).filter(Boolean);
      const published = texts.find((text) => /\\d{4}-\\d{2}-\\d{2}/.test(text)) || '';
      const title = texts.find((text) => text && text !== published && !/^选择视频$/.test(text) && text.length >= 4) || '';
      return { title, published_text: published };
    })();
    """
    payload = _run_js_dict(page, js)
    if not payload or not any(payload.get(key) for key in ("title", "published_text")):
        return None
    comments = _extract_kuaishou_comments(page)
    if not comments:
        return None
    title = str(payload.get("title") or "").strip()
    published_text = str(payload.get("published_text") or "").strip()
    return {
        "post_key": _build_post_key(title, published_text),
        "title": title,
        "published_text": published_text,
        "comment_count": len(comments),
        "has_comments": True,
    }


def _extract_kuaishou_picker_posts(page: Any) -> list[dict[str, Any]]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function toInt(value) {
        const raw = String(value || '').replace(/[^\\d]/g, '');
        return raw ? parseInt(raw, 10) : 0;
      }
      const items = Array.from(document.querySelectorAll('.video-item')).filter(visible);
      return items.map((item) => {
        const title = norm(item.querySelector('.video-info__content__title') ? item.querySelector('.video-info__content__title').innerText : item.innerText);
        const published = norm(item.querySelector('.video-info__content__date') ? item.querySelector('.video-info__content__date').innerText : '');
        const metricBlocks = Array.from(item.querySelectorAll('.video-info__content__detail > div')).filter(visible);
        const commentCount = metricBlocks.length >= 2
          ? toInt(metricBlocks[1].innerText || metricBlocks[1].textContent || '')
          : 0;
        return {
          title,
          published_text: published,
          comment_count: commentCount,
          has_comments: commentCount > 0,
        };
      }).filter((item) => item.title || item.published_text);
    })();
    """
    payload = _run_js_list(page, js)
    result: list[dict[str, Any]] = []
    for item in payload:
        title = str(item.get("title") or "").strip()
        published_text = str(item.get("published_text") or "").strip()
        comment_count = max(0, int(item.get("comment_count") or 0))
        result.append(
            {
                "post_key": _build_post_key(title, published_text),
                "title": title,
                "published_text": published_text,
                "comment_count": comment_count,
                "has_comments": bool(item.get("has_comments")) or comment_count > 0,
            }
        )
    return result


def _scroll_kuaishou_picker(page: Any) -> None:
    _scroll_container(page, [".semi-drawer-body", ".video-item"])


def _collect_kuaishou_commented_posts(page: Any, limit: int, debug: bool = False) -> list[dict[str, Any]]:
    if not _ensure_kuaishou_picker_open(page):
        current_post = _extract_current_kuaishou_post(page)
        if current_post:
            engine._comment_reply_log(debug, "[kuaishou] Picker not opened; fallback to current selected post")
            return [current_post]
        return []
    target = max(1, int(limit))
    collected: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    stagnant_rounds = 0
    for round_index in range(8):
        cards = _extract_kuaishou_picker_posts(page)
        before = len(collected)
        for card in cards:
            if int(card.get("comment_count") or 0) <= 0 and not bool(card.get("has_comments")):
                continue
            post_key = str(card.get("post_key") or "").strip()
            if not post_key or post_key in seen_keys:
                continue
            seen_keys.add(post_key)
            collected.append(card)
        engine._comment_reply_log(debug, f"[kuaishou] Visible commented posts: {len(collected)} after picker round {round_index}")
        if len(collected) >= target:
            break
        if len(collected) == before:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
        if stagnant_rounds >= 2:
            break
        _scroll_kuaishou_picker(page)
        time.sleep(0.8)
    if not collected:
        current_post = _extract_current_kuaishou_post(page)
        if current_post:
            engine._comment_reply_log(debug, "[kuaishou] Picker returned no commented posts; fallback to current selected post")
            return [current_post]
    return collected[:target]


def _open_kuaishou_post(page: Any, post: dict[str, Any], debug: bool = False) -> bool:
    if not _ensure_kuaishou_picker_open(page):
        return False
    js = """
    return ((targetTitle, targetPublished) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function digits(value) {
        return norm(value).replace(/[^\\d]/g, '');
      }
      function titleMatch(left, right) {
        const a = norm(left).replace(/\\s+/g, '').toLowerCase();
        const b = norm(right).replace(/\\s+/g, '').toLowerCase();
        if (!a || !b) return false;
        if (a === b || a.startsWith(b) || b.startsWith(a) || a.includes(b) || b.includes(a)) return true;
        const minPrefix = Math.min(a.length, b.length, 12);
        return minPrefix >= 6 && a.slice(0, minPrefix) === b.slice(0, minPrefix);
      }
      function samePublished(left, right) {
        const a = norm(left);
        const b = norm(right);
        if (!a || !b) return !a && !b;
        if (a === b || a.includes(b) || b.includes(a)) return true;
        const da = digits(a);
        const db = digits(b);
        return !!da && !!db && (da === db || da.endsWith(db) || db.endsWith(da));
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const items = Array.from(document.querySelectorAll('.video-item')).filter(visible);
      const parsed = items.map((item) => ({
        item,
        title: norm(item.querySelector('.video-info__content__title') ? item.querySelector('.video-info__content__title').innerText : item.innerText),
        published_text: norm(item.querySelector('.video-info__content__date') ? item.querySelector('.video-info__content__date').innerText : ''),
      }));
      const target = parsed.find((entry) => titleMatch(entry.title, targetTitle) && samePublished(entry.published_text, targetPublished));
      if (!target) {
        return {
          ok: false,
          reason: 'post_not_found',
          visible_posts: parsed.slice(0, 6).map((entry) => ({ title: entry.title, published_text: entry.published_text })),
        };
      }
      return { ok: click(target.item) };
    })(arguments[0], arguments[1]);
    """
    result: dict[str, Any] = {}
    for _ in range(8):
        result = _run_js_dict(page, js, str(post.get("title") or ""), str(post.get("published_text") or ""))
        if bool(result.get("ok")):
            matched = _wait_until(lambda: bool(_extract_kuaishou_comments(page)), timeout_seconds=8.0, poll_seconds=0.35)
            return matched or True
        if str(result.get("reason") or "") != "post_not_found":
            return False
        _scroll_kuaishou_picker(page)
        time.sleep(0.8)
    engine._comment_reply_log(
        debug,
        "[kuaishou] Open post failed: " + json.dumps({"post": post, "visible": result.get("visible_posts") if isinstance(result, dict) else []}, ensure_ascii=False),
    )
    return False


def _extract_kuaishou_comments(page: Any) -> list[dict[str, Any]]:
    js = """
    return (() => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      return roots.map((root, index) => {
        const author = norm(root.querySelector('.comment-content__username') ? root.querySelector('.comment-content__username').innerText : '');
        const timeText = norm(root.querySelector('.comment-content__date') ? root.querySelector('.comment-content__date').innerText : '');
        const content = norm(root.querySelector('.comment-content__detail') ? root.querySelector('.comment-content__detail').innerText : '');
        const detailNodes = Array.from(root.querySelectorAll('.comment-content__detail')).filter(visible);
        const replyMarkers = Array.from(root.querySelectorAll('[class*="reply-"], .reply-content, .reply-list, .comment-reply')).filter(visible);
        const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
        const likeButton = buttons[0] || null;
        const likeColor = likeButton ? window.getComputedStyle(likeButton).color : '';
        return {
          index,
          author,
          time_text: timeText,
          content,
          has_reply: detailNodes.length > 1 || replyMarkers.length > 0,
          liked: !!(likeButton && !/rgb\\(140,\\s*140,\\s*140\\)|#8c8c8c/i.test(String(likeColor || ''))),
        };
      }).filter((item) => item.content || item.author || item.time_text);
    })();
    """
    return _run_js_list(page, js)


def _like_kuaishou_comment_if_needed(page: Any, comment_index: int) -> bool:
    js = """
    return ((commentIndex) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
      const likeButton = buttons.find((node) => !/回复|删除|举报|置顶/.test(String(node.innerText || node.textContent || '').trim())) || buttons[0];
      if (!likeButton) return { ok: false };
      const color = window.getComputedStyle(likeButton).color || '';
      if (!/rgb\\(140,\\s*140,\\s*140\\)|#8c8c8c/i.test(String(color || ''))) return { ok: true };
      return { ok: click(likeButton) };
    })(arguments[0]);
    """
    return bool(_run_js_dict(page, js, int(comment_index)).get("ok"))


def _submit_kuaishou_reply(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
      const replyButton = buttons.find((node) => /回复/.test(String(node.innerText || node.textContent || '').trim()));
      if (!replyButton) return { ok: false };
      return { ok: click(replyButton) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False
    locate_js = """
    return ((commentIndex) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const wrapper = root.querySelector('.comment-input-wrapper');
      if (!wrapper || !visible(wrapper)) return { ok: false };
      const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
      const submit = Array.from(wrapper.querySelectorAll('button, div, span')).find((node) => /确认/.test(String(node.innerText || node.textContent || '').trim()) && visible(node));
      return { ok: !!(input && submit) };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False
    fill_js = """
    return ((commentIndex, replyText) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function setValue(input, value) {
        const text = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, text);
          else input.value = text;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        input.textContent = text;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        return true;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const wrapper = root.querySelector('.comment-input-wrapper');
      if (!wrapper || !visible(wrapper)) return { ok: false };
      const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
      const submit = Array.from(wrapper.querySelectorAll('button, div, span')).find((node) => /确认/.test(String(node.innerText || node.textContent || '').trim()) && visible(node));
      if (!input || !submit) return { ok: false };
      setValue(input, replyText);
      return { ok: click(submit) };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _wait_kuaishou_reply_confirm(page: Any, comment_index: int, reply_text: str, timeout_seconds: float = 12.0) -> bool:
    target = re.sub(r"\s+", "", str(reply_text or "").strip())
    js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, '').trim();
      }
      const target = norm(replyText);
      if (!target) return { ok: false };
      const roots = Array.from(document.querySelectorAll('.comment-content'));
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const scopes = [
        root,
        root.nextElementSibling,
        root.parentElement,
        root.parentElement && root.parentElement.nextElementSibling,
      ].filter(Boolean);
      for (const scope of scopes) {
        const text = cleanText(scope);
        if (!text) continue;
        if (text.includes(target)) {
          return { ok: true };
        }
      }
      return { ok: false, reason: 'reply_not_visible_near_comment' };
    })(arguments[0], arguments[1]);
    """
    return _wait_until(
        lambda: bool(_run_js_dict(page, js, int(comment_index), target).get("ok")),
        timeout_seconds=timeout_seconds,
        poll_seconds=0.35,
    )


def _submit_kuaishou_reply_v2(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
      const replyButton = buttons.find((node) => /回复/.test(norm(node.innerText || node.textContent || '')))
        || buttons[1]
        || buttons[0];
      if (!replyButton) return { ok: false };
      return { ok: click(replyButton) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const submit = wrapper.querySelector('.comment-content__btns__btn.reply-active, .comment-content__btns__btn[class*="reply-active"]')
          || Array.from(wrapper.querySelectorAll('button, div, span')).find((node) => /确认|发送/.test(norm(node.innerText || node.textContent || '')) && visible(node));
        if (input && submit && visible(input) && visible(submit)) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function setValue(input, value) {
        const text = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, text);
          else input.value = text;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        input.textContent = text;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        return true;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const submit = wrapper.querySelector('.comment-content__btns__btn.reply-active, .comment-content__btns__btn[class*="reply-active"]')
          || Array.from(wrapper.querySelectorAll('button, div, span')).find((node) => /确认|发送/.test(norm(node.innerText || node.textContent || '')) && visible(node));
        if (!input || !submit || !visible(input) || !visible(submit)) continue;
        setValue(input, replyText);
        return { ok: click(submit) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_kuaishou_reply_v3(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
      const replyButton = buttons.find((node) => /回复/.test(norm(node.innerText || node.textContent || '')))
        || buttons[1]
        || buttons[0];
      if (!replyButton) return { ok: false };
      return { ok: click(replyButton) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const submit = scope.querySelector('.comment-content__btns__btn.reply-active, .comment-content__btns__btn[class*="reply-active"]')
          || Array.from(scope.querySelectorAll('button, div, span')).find((node) => /确认/.test(norm(node.innerText || node.textContent || '')) && visible(node));
        if (input && submit && visible(input) && visible(submit)) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_js = """
    return ((commentIndex, replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function setValue(input, value) {
        const text = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, text);
          else input.value = text;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        input.textContent = text;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        return true;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        const submit = scope.querySelector('.comment-content__btns__btn.reply-active, .comment-content__btns__btn[class*="reply-active"]')
          || Array.from(scope.querySelectorAll('button, div, span')).find((node) => /确认/.test(norm(node.innerText || node.textContent || '')) && visible(node));
        if (!input || !submit || !visible(input) || !visible(submit)) continue;
        setValue(input, replyText);
        return { ok: click(submit) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    return bool(_run_js_dict(page, fill_js, int(comment_index), str(reply_text or "")).get("ok"))


def _submit_kuaishou_reply_v4(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
      const replyButton = buttons.find((node) => /回复/.test(norm(node.innerText || node.textContent || '')))
        || buttons[1]
        || buttons[0];
      if (!replyButton) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyButton) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_editor_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        if (input && visible(input)) {
          return { ok: true };
        }
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_editor_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_only_js = """
    return ((commentIndex, replyText) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function setValue(input, value) {
        const text = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, text);
          else input.value = text;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        input.textContent = text;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        return true;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        if (!input || !visible(input)) continue;
        return { ok: setValue(input, replyText) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    if not bool(_run_js_dict(page, fill_only_js, int(comment_index), str(reply_text or "")).get("ok")):
        return False

    wait_confirm_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const button = Array.from(scope.querySelectorAll('button, div, span'))
          .find((node) => /确认/.test(norm(node.innerText || node.textContent || '')) && visible(node));
        if (button) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, wait_confirm_js, int(comment_index)).get("ok")), timeout_seconds=3.0, poll_seconds=0.2):
        return False

    click_confirm_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const button = Array.from(scope.querySelectorAll('button, div, span'))
          .find((node) => /确认/.test(norm(node.innerText || node.textContent || '')) && visible(node));
        if (!button) continue;
        return { ok: click(button) };
      }
      return { ok: false };
    })(arguments[0]);
    """
    return bool(_run_js_dict(page, click_confirm_js, int(comment_index)).get("ok"))


def _submit_kuaishou_reply_v5(page: Any, comment_index: int, reply_text: str) -> bool:
    open_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false, reason: 'comment_not_found' };
      const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
      const replyButton = buttons.find((node) => norm(node.innerText || node.textContent || '') === '回复') || buttons[1] || buttons[0];
      if (!replyButton) return { ok: false, reason: 'reply_button_not_found' };
      return { ok: click(replyButton) };
    })(arguments[0]);
    """
    if not bool(_run_js_dict(page, open_js, int(comment_index)).get("ok")):
        return False

    locate_editor_js = """
    return ((commentIndex) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        if (input && visible(input)) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, locate_editor_js, int(comment_index)).get("ok")), timeout_seconds=5.0, poll_seconds=0.25):
        return False

    fill_only_js = """
    return ((commentIndex, replyText) => {
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function setValue(input, value) {
        const text = String(value || '');
        if (!input) return false;
        if (input.tagName === 'TEXTAREA' || input.tagName === 'INPUT') {
          const proto = input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, 'value');
          if (setter && setter.set) setter.set.call(input, text);
          else input.value = text;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        input.textContent = text;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
        return true;
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const wrapper = scope.querySelector('.comment-input__wrapper, .comment-input-wrapper');
        if (!wrapper || !visible(wrapper)) continue;
        const input = wrapper.querySelector('textarea, input[type="text"], input:not([type]), [contenteditable="true"]');
        if (!input || !visible(input)) continue;
        return { ok: setValue(input, replyText) };
      }
      return { ok: false };
    })(arguments[0], arguments[1]);
    """
    if not bool(_run_js_dict(page, fill_only_js, int(comment_index), str(reply_text or "")).get("ok")):
        return False

    wait_confirm_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function isConfirmButton(node) {
        return !!node
          && visible(node)
          && node.matches('.comment-input__wrapper__control__btns .comment-btn.sure-btn.sure-btn--is-active, .comment-btn.sure-btn.sure-btn--is-active')
          && norm(node.innerText || node.textContent || '') === '确认';
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const button = Array.from(scope.querySelectorAll('.comment-input__wrapper__control__btns .comment-btn.sure-btn.sure-btn--is-active, .comment-btn.sure-btn.sure-btn--is-active')).find(isConfirmButton);
        if (button) return { ok: true };
      }
      return { ok: false };
    })(arguments[0]);
    """
    if not _wait_until(lambda: bool(_run_js_dict(page, wait_confirm_js, int(comment_index)).get("ok")), timeout_seconds=3.0, poll_seconds=0.2):
        return False

    click_confirm_js = """
    return ((commentIndex) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
      }
      function visible(node) {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        const rect = node.getBoundingClientRect();
        return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
      }
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      function isConfirmButton(node) {
        return !!node
          && visible(node)
          && node.matches('.comment-input__wrapper__control__btns .comment-btn.sure-btn.sure-btn--is-active, .comment-btn.sure-btn.sure-btn--is-active')
          && norm(node.innerText || node.textContent || '') === '确认';
      }
      const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
      const root = roots[Number(commentIndex)];
      if (!root) return { ok: false };
      const scopes = [root, root.nextElementSibling, root.parentElement, root.parentElement && root.parentElement.nextElementSibling].filter(Boolean);
      for (const scope of scopes) {
        const button = Array.from(scope.querySelectorAll('.comment-input__wrapper__control__btns .comment-btn.sure-btn.sure-btn--is-active, .comment-btn.sure-btn.sure-btn--is-active')).find(isConfirmButton);
        if (!button) continue;
        return { ok: click(button) };
      }
      return { ok: false };
    })(arguments[0]);
    """
    return bool(_run_js_dict(page, click_confirm_js, int(comment_index)).get("ok"))


def _scroll_kuaishou_comments(page: Any) -> None:
    _scroll_container(page, [".comment-list", "main", "body"])


PLATFORM_ADAPTERS: dict[str, CommentPlatformAdapter] = {
    "douyin": CommentPlatformAdapter(
        platform="douyin",
        open_url=DOUYIN_COMMENT_MANAGER_URL,
        collect_posts=_collect_douyin_commented_posts,
        open_post=_open_douyin_post,
        extract_comments=_extract_douyin_comments,
        like_comment_if_needed=_like_douyin_comment_if_needed,
        submit_reply=_submit_douyin_reply_v9,
        wait_reply_confirm=_wait_douyin_reply_confirm,
        scroll_comments=_scroll_douyin_comments,
    ),
    "kuaishou": CommentPlatformAdapter(
        platform="kuaishou",
        open_url=KUAISHOU_COMMENT_MANAGER_URL,
        collect_posts=_collect_kuaishou_commented_posts,
        open_post=_open_kuaishou_post,
        extract_comments=_extract_kuaishou_comments,
        like_comment_if_needed=_like_kuaishou_comment_if_needed,
        submit_reply=_submit_kuaishou_reply_v5,
        wait_reply_confirm=_wait_kuaishou_reply_confirm,
        scroll_comments=_scroll_kuaishou_comments,
    ),
}

PLATFORM_DIAGNOSTICS: dict[str, Callable[[Any], dict[str, Any]]] = {
    "douyin": _diagnose_douyin_page,
    "kuaishou": _diagnose_kuaishou_page,
}

PLATFORM_PICKER_OPENERS: dict[str, Callable[[Any], bool]] = {
    "douyin": _ensure_douyin_picker_open,
    "kuaishou": _ensure_kuaishou_picker_open,
}


def _diagnostics_dir(workspace: engine.Workspace) -> Path:
    return workspace.root / "diagnostics"


def _snapshot_page_html(page: Any) -> str:
    payload = _run_js_dict(
        page,
        """
        return {
          ok: true,
          html: String((document.documentElement && document.documentElement.outerHTML) || '')
        };
        """,
    )
    return str(payload.get("html") or "")


def _snapshot_page_text(page: Any) -> str:
    payload = _run_js_dict(
        page,
        """
        return {
          ok: true,
          text: String(((document.body || document.documentElement) && (document.body || document.documentElement).innerText) || '')
        };
        """,
    )
    return str(payload.get("text") or "")


def reply_douyin_focused_editor(
    *,
    workspace: engine.Workspace,
    runtime_config: dict[str, Any],
    debug_port: int,
    reply_text: str,
    ignore_state: bool = False,
    chrome_path: Optional[str] = None,
    chrome_user_data_dir: str = "",
    auto_open_chrome: bool = True,
    debug: bool = False,
    notify_env_prefix: str = engine.DEFAULT_NOTIFY_ENV_PREFIX,
) -> dict[str, Any]:
    del runtime_config
    del notify_env_prefix

    platform = "douyin"
    clean_reply_text = str(reply_text or "").strip()
    state_path = str(_state_path(workspace, platform))
    markdown_path = str(_markdown_path(workspace, platform))
    empty_result = {
        "ok": False,
        "platform": platform,
        "reason": "",
        "state_path": state_path,
        "markdown_path": markdown_path,
        "records": [],
        "posts_scanned": 0,
        "posts_selected": 0,
        "replies_sent": 0,
    }
    if not clean_reply_text:
        empty_result["reason"] = "reply_text_empty"
        return empty_result

    state = _load_state(workspace, platform)
    items = _prune_state_items(state.get("items") if isinstance(state, dict) else {})
    state["items"] = items
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    page = engine._connect_chrome(
        debug_port=int(debug_port),
        auto_open_chrome=auto_open_chrome,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=DOUYIN_COMMENT_MANAGER_URL,
    )
    time.sleep(0.3)

    page_meta = _run_js_dict(
        page,
        """
        return {
          ok: true,
          url: String(location.href || ''),
          title: String(document.title || '')
        };
        """,
    )
    current_url = str(page_meta.get("url") or "").strip()
    current_title = str(page_meta.get("title") or "").strip()
    if "creator.douyin.com/creator-micro/interactive/comment" not in current_url:
        empty_result["reason"] = "douyin_comment_page_not_open"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    focus_info = _run_js_dict(
        page,
        """
        return (() => {
          function norm(value) {
            return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
          }
          function visible(node) {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            const rect = node.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
          }
          function isRoot(node) {
            if (!visible(node)) return false;
            return !!node.querySelector('[class*="username-"]')
              && !!node.querySelector('[class*="time-"]')
              && !!node.querySelector('[class*="comment-content-text-"]')
              && !!node.querySelector('[class*="operations-"]');
          }
          const active = document.activeElement;
          const editable = !!active && (active.isContentEditable || String(active.getAttribute('contenteditable') || '').toLowerCase() === 'true');
          const roots = [];
          const seen = new Set();
          for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
            let node = op;
            while (node && node !== document.body) {
              if (isRoot(node)) {
                if (!seen.has(node)) {
                  seen.add(node);
                  roots.push(node);
                }
                break;
              }
              node = node.parentElement;
            }
          }
          let root = null;
          if (active) {
            for (const candidate of roots) {
              if (candidate.contains(active)) {
                root = candidate;
                break;
              }
            }
          }
          const index = root ? roots.indexOf(root) : -1;
          return {
            ok: editable && index >= 0,
            editable,
            comment_index: index,
            author: norm(root && root.querySelector('[class*="username-"]') ? root.querySelector('[class*="username-"]').innerText : ''),
            time_text: norm(root && root.querySelector('[class*="time-"]') ? root.querySelector('[class*="time-"]').innerText : ''),
            content: norm(root && root.querySelector('[class*="comment-content-text-"]') ? root.querySelector('[class*="comment-content-text-"]').innerText : ''),
            active_text: norm(active ? (active.innerText || active.textContent || active.value || '') : '')
          };
        })();
        """,
    )
    if not bool(focus_info.get("ok")):
        empty_result["reason"] = "focused_reply_editor_not_ready"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        empty_result["focus_info"] = focus_info
        return empty_result

    comment_index = max(0, int(focus_info.get("comment_index") or 0))
    comments = _extract_douyin_comments(page)
    comment: dict[str, Any]
    if 0 <= comment_index < len(comments):
        comment = dict(comments[comment_index])
    else:
        comment = {
            "index": comment_index,
            "author": str(focus_info.get("author") or "").strip(),
            "time_text": str(focus_info.get("time_text") or "").strip(),
            "content": str(focus_info.get("content") or "").strip(),
            "has_reply": False,
            "liked": False,
        }
    comment["index"] = comment_index

    post = _extract_current_douyin_post(page) or {
        "post_key": _build_post_key("", ""),
        "title": "",
        "published_text": "",
        "comment_count": len(comments) if comments else 1,
        "has_comments": True,
    }

    engine._comment_reply_log(
        debug,
        f"[douyin-focused] Active comment index={comment_index} author={engine._single_line_preview(str(comment.get('author') or ''), 24)}",
    )
    should_skip, skip_reason = _should_skip_comment_reply_guarded(comment)
    if should_skip:
        empty_result["reason"] = skip_reason
        empty_result["posts_scanned"] = 1
        empty_result["posts_selected"] = 1
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    typed = False
    if hasattr(page, "run_cdp"):
        try:
            page.run_cdp("Input.insertText", text=clean_reply_text)
            typed = True
        except Exception:
            typed = False

    if not typed:
        typed = bool(
            _run_js_dict(
                page,
                """
                return ((replyText) => {
                  const active = document.activeElement;
                  if (!active) return { ok: false };
                  const editable = active.isContentEditable || String(active.getAttribute('contenteditable') || '').toLowerCase() === 'true';
                  if (!editable) return { ok: false };
                  try { active.focus(); } catch (e) {}
                  try {
                    document.execCommand('insertText', false, String(replyText || ''));
                  } catch (e) {}
                  const text = String(active.innerText || active.textContent || active.value || '');
                  return { ok: text.includes(String(replyText || '')) };
                })(arguments[0]);
                """,
                clean_reply_text,
            ).get("ok")
        )

    if not typed and hasattr(page, "ele"):
        try:
            active_ele = page.ele("css:[contenteditable='true']:focus", timeout=0.5)
        except Exception:
            active_ele = None
        if active_ele:
            typed = engine._input_text_field_with_keyboard(active_ele, clean_reply_text)

    typed_ready = _wait_until(
        lambda: bool(
            _run_js_dict(
                page,
                """
                return ((replyText) => {
                  function norm(value) {
                    return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
                  }
                  const active = document.activeElement;
                  if (!active) return { ok: false };
                  const text = norm(active.innerText || active.textContent || active.value || '');
                  return { ok: !!(norm(replyText) && text.includes(norm(replyText))) };
                })(arguments[0]);
                """,
                clean_reply_text,
            ).get("ok")
        ),
        timeout_seconds=3.0,
        poll_seconds=0.2,
    )
    if not typed_ready:
        empty_result["reason"] = "reply_text_not_inserted"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        empty_result["focus_info"] = focus_info
        return empty_result
    send_clicked = _click_douyin_send_near_active_editor(page)
    if not send_clicked:
        empty_result["reason"] = "send_button_not_clicked"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    send_clicked = True
    if False and hasattr(page, "ele"):
        for selector in (
            "xpath://button[contains(@class,'douyin-creator-interactive-button-primary') and not(@disabled)]",
            "css:button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary",
        ):
            try:
                btn = page.ele(selector, timeout=0.8)
            except Exception:
                btn = None
            if not btn or not engine._is_visible_element(btn):
                continue
            try:
                disabled_attr = str(btn.attr("disabled") or "").strip().lower()
            except Exception:
                disabled_attr = ""
            if disabled_attr in {"true", "disabled"}:
                continue
            try:
                btn.click()
                send_clicked = True
                break
            except Exception:
                try:
                    btn.click(by_js=True)
                    send_clicked = True
                    break
                except Exception:
                    continue
    if not send_clicked:
        empty_result["reason"] = "send_button_not_clicked"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    if not _wait_douyin_reply_confirm(page, comment_index, clean_reply_text, 12.0):
        empty_result["reason"] = "reply_confirm_timeout"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    fingerprint = engine._comment_reply_fingerprint(post, comment)
    record = engine._remember_comment_reply(
        items,
        fingerprint=fingerprint,
        post=post,
        comment=comment,
        reply_text=clean_reply_text,
    )
    state["items"] = items
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save_state(workspace, platform, state)
    engine._append_comment_reply_markdown(_markdown_path(workspace, platform), platform, record)
    return {
        "ok": True,
        "platform": platform,
        "reason": "",
        "state_path": state_path,
        "markdown_path": markdown_path,
        "records": [record],
        "posts_scanned": 1,
        "posts_selected": 1,
        "replies_sent": 1,
        "current_url": current_url,
        "current_title": current_title,
    }


def reply_douyin_focused_generated(
    *,
    workspace: engine.Workspace,
    runtime_config: dict[str, Any],
    debug_port: int,
    ignore_state: bool = False,
    chrome_path: Optional[str] = None,
    chrome_user_data_dir: str = "",
    auto_open_chrome: bool = True,
    debug: bool = False,
    notify_env_prefix: str = engine.DEFAULT_NOTIFY_ENV_PREFIX,
) -> dict[str, Any]:
    del notify_env_prefix

    platform = "douyin"
    state_path = str(_state_path(workspace, platform))
    markdown_path = str(_markdown_path(workspace, platform))
    base_result = {
        "ok": False,
        "platform": platform,
        "reason": "",
        "state_path": state_path,
        "markdown_path": markdown_path,
        "records": [],
        "posts_scanned": 0,
        "posts_selected": 0,
        "replies_sent": 0,
    }

    comment_cfg = engine._merge_comment_reply_config(runtime_config.get("comment_reply"))
    reply_max_chars = max(6, int(comment_cfg.get("reply_max_chars") or 20))
    configured_markers = tuple(
        str(item or "").strip().lower()
        for item in (comment_cfg.get("self_author_markers") if isinstance(comment_cfg.get("self_author_markers"), list) else [])
        if str(item or "").strip()
    )
    self_author_markers = configured_markers or COMMENT_REPLY_SELF_AUTHOR_MARKERS
    state = _load_state(workspace, platform)
    items = _prune_state_items(state.get("items") if isinstance(state, dict) else {})
    state["items"] = items
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    page = engine._connect_chrome(
        debug_port=int(debug_port),
        auto_open_chrome=auto_open_chrome,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=DOUYIN_COMMENT_MANAGER_URL,
    )
    time.sleep(0.3)

    page_meta = _run_js_dict(
        page,
        """
        return {
          ok: true,
          url: String(location.href || ''),
          title: String(document.title || '')
        };
        """,
    )
    current_url = str(page_meta.get("url") or "").strip()
    current_title = str(page_meta.get("title") or "").strip()
    if "creator.douyin.com/creator-micro/interactive/comment" not in current_url:
        base_result["reason"] = "douyin_comment_page_not_open"
        base_result["current_url"] = current_url
        base_result["current_title"] = current_title
        return base_result

    focus_info = _run_js_dict(
        page,
        """
        return (() => {
          function norm(value) {
            return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
          }
          function visible(node) {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            const rect = node.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
          }
          function isRoot(node) {
            if (!visible(node)) return false;
            return !!node.querySelector('[class*="username-"]')
              && !!node.querySelector('[class*="time-"]')
              && !!node.querySelector('[class*="comment-content-text-"]')
              && !!node.querySelector('[class*="operations-"]');
          }
          const active = document.activeElement;
          const editable = !!active && (active.isContentEditable || String(active.getAttribute('contenteditable') || '').toLowerCase() === 'true');
          const roots = [];
          const seen = new Set();
          for (const op of Array.from(document.querySelectorAll('[class*="operations-"]'))) {
            let node = op;
            while (node && node !== document.body) {
              if (isRoot(node)) {
                if (!seen.has(node)) {
                  seen.add(node);
                  roots.push(node);
                }
                break;
              }
              node = node.parentElement;
            }
          }
          let root = null;
          if (active) {
            for (const candidate of roots) {
              if (candidate.contains(active)) {
                root = candidate;
                break;
              }
            }
          }
          const index = root ? roots.indexOf(root) : -1;
          return {
            ok: editable && index >= 0,
            comment_index: index,
            author: norm(root && root.querySelector('[class*="username-"]') ? root.querySelector('[class*="username-"]').innerText : ''),
            time_text: norm(root && root.querySelector('[class*="time-"]') ? root.querySelector('[class*="time-"]').innerText : ''),
            content: norm(root && root.querySelector('[class*="comment-content-text-"]') ? root.querySelector('[class*="comment-content-text-"]').innerText : '')
          };
        })();
        """,
    )
    if not bool(focus_info.get("ok")):
        base_result["reason"] = "focused_reply_editor_not_ready"
        base_result["current_url"] = current_url
        base_result["current_title"] = current_title
        return base_result

    comments = _extract_douyin_comments(page)
    comment_index = max(0, int(focus_info.get("comment_index") or 0))
    comment = dict(comments[comment_index]) if 0 <= comment_index < len(comments) else {
        "index": comment_index,
        "author": str(focus_info.get("author") or "").strip(),
        "time_text": str(focus_info.get("time_text") or "").strip(),
        "content": str(focus_info.get("content") or "").strip(),
        "has_reply": False,
        "liked": False,
    }
    comment["index"] = comment_index
    post = _extract_current_douyin_post(page) or {
        "post_key": _build_post_key("", ""),
        "title": "",
        "published_text": "",
        "comment_count": len(comments) if comments else 1,
        "has_comments": True,
    }
    should_skip, skip_reason = _should_skip_comment_reply_guarded(comment)
    if should_skip:
        base_result["reason"] = skip_reason
        base_result["posts_scanned"] = 1
        base_result["posts_selected"] = 1
        return base_result
    fingerprint = engine._comment_reply_fingerprint(post, comment)
    if (not bool(ignore_state)) and (fingerprint in items or bool(comment.get("has_reply"))):
        base_result["reason"] = "duplicate_or_has_reply"
        base_result["posts_scanned"] = 1
        base_result["posts_selected"] = 1
        return base_result

    reply_text = engine.generate_comment_reply(
        post=post,
        comment=comment,
        spark_ai=runtime_config.get("spark_ai") if isinstance(runtime_config.get("spark_ai"), dict) else {},
        prompt_template=str(comment_cfg.get("prompt_template") or engine.DEFAULT_COMMENT_REPLY_PROMPT_TEMPLATE),
        fallback_replies=list(comment_cfg.get("fallback_replies") or engine.DEFAULT_COMMENT_REPLY_FALLBACKS),
        max_chars=reply_max_chars,
    )
    if not reply_text:
        base_result["reason"] = "reply_text_empty"
        base_result["posts_scanned"] = 1
        base_result["posts_selected"] = 1
        return base_result

    return reply_douyin_focused_editor(
        workspace=workspace,
        runtime_config=runtime_config,
        debug_port=debug_port,
        reply_text=reply_text,
        ignore_state=ignore_state,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        auto_open_chrome=auto_open_chrome,
        debug=debug,
        notify_env_prefix=engine.DEFAULT_NOTIFY_ENV_PREFIX,
    )


def reply_kuaishou_focused_generated(
    *,
    workspace: engine.Workspace,
    runtime_config: dict[str, Any],
    debug_port: int,
    ignore_state: bool = False,
    chrome_path: Optional[str] = None,
    chrome_user_data_dir: str = "",
    auto_open_chrome: bool = True,
    debug: bool = False,
    notify_env_prefix: str = engine.DEFAULT_NOTIFY_ENV_PREFIX,
) -> dict[str, Any]:
    del notify_env_prefix

    platform = "kuaishou"
    state_path = str(_state_path(workspace, platform))
    markdown_path = str(_markdown_path(workspace, platform))
    base_result = {
        "ok": False,
        "platform": platform,
        "reason": "",
        "state_path": state_path,
        "markdown_path": markdown_path,
        "records": [],
        "posts_scanned": 0,
        "posts_selected": 0,
        "replies_sent": 0,
    }

    comment_cfg = engine._merge_comment_reply_config(runtime_config.get("comment_reply"))
    reply_max_chars = max(6, int(comment_cfg.get("reply_max_chars") or 20))
    state = _load_state(workspace, platform)
    items = _prune_state_items(state.get("items") if isinstance(state, dict) else {})
    state["items"] = items
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    page = engine._connect_chrome(
        debug_port=int(debug_port),
        auto_open_chrome=auto_open_chrome,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=KUAISHOU_COMMENT_MANAGER_URL,
    )
    time.sleep(0.3)

    page_meta = _run_js_dict(
        page,
        """
        return {
          ok: true,
          url: String(location.href || ''),
          title: String(document.title || '')
        };
        """,
    )
    current_url = str(page_meta.get("url") or "").strip()
    current_title = str(page_meta.get("title") or "").strip()
    if "cp.kuaishou.com/article/comment" not in current_url:
        base_result["reason"] = "kuaishou_comment_page_not_open"
        base_result["current_url"] = current_url
        base_result["current_title"] = current_title
        return base_result

    focus_info = _run_js_dict(
        page,
        """
        return (() => {
          function norm(value) {
            return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
          }
          function visible(node) {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            const rect = node.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
          }
          function toElement(node) {
            if (!node) return null;
            return node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
          }
          function isEditable(node) {
            return !!node && (
              node.isContentEditable
              || node.tagName === 'TEXTAREA'
              || node.tagName === 'INPUT'
              || String(node.getAttribute('contenteditable') || '').toLowerCase() === 'true'
            );
          }
          function resolveInput() {
            const active = document.activeElement;
            if (isEditable(active)) return active;
            const selection = window.getSelection ? window.getSelection() : null;
            const anchor = selection && selection.anchorNode ? toElement(selection.anchorNode) : null;
            if (isEditable(anchor)) return anchor;
            const anchorEditable = anchor ? anchor.closest('[contenteditable="true"], textarea, input') : null;
            if (isEditable(anchorEditable)) return anchorEditable;
            const activeEditable = active ? active.closest('[contenteditable="true"], textarea, input') : null;
            if (isEditable(activeEditable)) return activeEditable;
            const wrapper = anchor ? (anchor.closest('.comment-input__wrapper, .comment-input-wrapper') || null) : (active ? active.closest('.comment-input__wrapper, .comment-input-wrapper') : null);
            if (!wrapper) return null;
            const nested = wrapper.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
            return isEditable(nested) ? nested : null;
          }
          const input = resolveInput();
          const wrapper = input ? (input.closest('.comment-input__wrapper, .comment-input-wrapper') || input.parentElement) : null;
          const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
          let root = null;
          for (const candidate of roots) {
            const scopes = [candidate, candidate.nextElementSibling, candidate.parentElement, candidate.parentElement && candidate.parentElement.nextElementSibling].filter(Boolean);
            if (wrapper && scopes.some((scope) => scope === wrapper || scope.contains(wrapper))) {
              root = candidate;
              break;
            }
            if (candidate.querySelector('.comment-content__btns__btn.reply-active')) {
              root = candidate;
            }
          }
          const index = root ? roots.indexOf(root) : -1;
          return {
            ok: !!input && index >= 0,
            comment_index: index,
            author: norm(root && root.querySelector('.comment-content__username') ? root.querySelector('.comment-content__username').innerText : ''),
            time_text: norm(root && root.querySelector('.comment-content__date') ? root.querySelector('.comment-content__date').innerText : ''),
            content: norm(root && root.querySelector('.comment-content__detail') ? root.querySelector('.comment-content__detail').innerText : ''),
            input_text: norm(input ? (input.innerText || input.textContent || input.value || '') : '')
          };
        })();
        """,
    )
    if not bool(focus_info.get("ok")):
        base_result["reason"] = "focused_reply_editor_not_ready"
        base_result["current_url"] = current_url
        base_result["current_title"] = current_title
        base_result["focus_info"] = focus_info
        return base_result

    comment_index = max(0, int(focus_info.get("comment_index") or 0))
    comments = _extract_kuaishou_comments(page)
    comment = dict(comments[comment_index]) if 0 <= comment_index < len(comments) else {
        "index": comment_index,
        "author": str(focus_info.get("author") or "").strip(),
        "time_text": str(focus_info.get("time_text") or "").strip(),
        "content": str(focus_info.get("content") or "").strip(),
        "has_reply": False,
        "liked": False,
    }
    comment["index"] = comment_index
    post = _extract_current_kuaishou_post(page) or {
        "post_key": _build_post_key("", ""),
        "title": "",
        "published_text": "",
        "comment_count": len(comments) if comments else 1,
        "has_comments": True,
    }
    should_skip, skip_reason = _should_skip_comment_reply_guarded(comment)
    if should_skip:
        base_result["reason"] = skip_reason
        base_result["posts_scanned"] = 1
        base_result["posts_selected"] = 1
        return base_result
    fingerprint = engine._comment_reply_fingerprint(post, comment)
    if (not bool(ignore_state)) and (fingerprint in items or bool(comment.get("has_reply"))):
        base_result["reason"] = "duplicate_or_has_reply"
        base_result["posts_scanned"] = 1
        base_result["posts_selected"] = 1
        return base_result

    reply_text = engine.generate_comment_reply(
        post=post,
        comment=comment,
        spark_ai=runtime_config.get("spark_ai") if isinstance(runtime_config.get("spark_ai"), dict) else {},
        prompt_template=str(comment_cfg.get("prompt_template") or engine.DEFAULT_COMMENT_REPLY_PROMPT_TEMPLATE),
        fallback_replies=list(comment_cfg.get("fallback_replies") or engine.DEFAULT_COMMENT_REPLY_FALLBACKS),
        max_chars=reply_max_chars,
    )
    if not reply_text:
        base_result["reason"] = "reply_text_empty"
        base_result["posts_scanned"] = 1
        base_result["posts_selected"] = 1
        return base_result

    return reply_kuaishou_focused_editor(
        workspace=workspace,
        runtime_config=runtime_config,
        debug_port=debug_port,
        reply_text=reply_text,
        ignore_state=ignore_state,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        auto_open_chrome=auto_open_chrome,
        debug=debug,
        notify_env_prefix=engine.DEFAULT_NOTIFY_ENV_PREFIX,
    )


def reply_kuaishou_focused_editor(
    *,
    workspace: engine.Workspace,
    runtime_config: dict[str, Any],
    debug_port: int,
    reply_text: str,
    ignore_state: bool = False,
    chrome_path: Optional[str] = None,
    chrome_user_data_dir: str = "",
    auto_open_chrome: bool = True,
    debug: bool = False,
    notify_env_prefix: str = engine.DEFAULT_NOTIFY_ENV_PREFIX,
) -> dict[str, Any]:
    del runtime_config
    del notify_env_prefix

    platform = "kuaishou"
    clean_reply_text = str(reply_text or "").strip()
    state_path = str(_state_path(workspace, platform))
    markdown_path = str(_markdown_path(workspace, platform))
    empty_result = {
        "ok": False,
        "platform": platform,
        "reason": "",
        "state_path": state_path,
        "markdown_path": markdown_path,
        "records": [],
        "posts_scanned": 0,
        "posts_selected": 0,
        "replies_sent": 0,
    }
    if not clean_reply_text:
        empty_result["reason"] = "reply_text_empty"
        return empty_result

    state = _load_state(workspace, platform)
    items = _prune_state_items(state.get("items") if isinstance(state, dict) else {})
    state["items"] = items
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    page = engine._connect_chrome(
        debug_port=int(debug_port),
        auto_open_chrome=auto_open_chrome,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=KUAISHOU_COMMENT_MANAGER_URL,
    )
    time.sleep(0.3)

    page_meta = _run_js_dict(
        page,
        """
        return {
          ok: true,
          url: String(location.href || ''),
          title: String(document.title || '')
        };
        """,
    )
    current_url = str(page_meta.get("url") or "").strip()
    current_title = str(page_meta.get("title") or "").strip()
    if "cp.kuaishou.com/article/comment" not in current_url:
        empty_result["reason"] = "kuaishou_comment_page_not_open"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    focus_info = _run_js_dict(
        page,
        """
        return (() => {
          function norm(value) {
            return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
          }
          function visible(node) {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            const rect = node.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
          }
          function toElement(node) {
            if (!node) return null;
            return node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
          }
          function isEditable(node) {
            return !!node && (
              node.isContentEditable
              || node.tagName === 'TEXTAREA'
              || node.tagName === 'INPUT'
              || String(node.getAttribute('contenteditable') || '').toLowerCase() === 'true'
            );
          }
          function resolveInput() {
            const active = document.activeElement;
            if (isEditable(active)) return active;
            const selection = window.getSelection ? window.getSelection() : null;
            const anchor = selection && selection.anchorNode ? toElement(selection.anchorNode) : null;
            if (isEditable(anchor)) return anchor;
            const anchorEditable = anchor ? anchor.closest('[contenteditable="true"], textarea, input') : null;
            if (isEditable(anchorEditable)) return anchorEditable;
            const activeEditable = active ? active.closest('[contenteditable="true"], textarea, input') : null;
            if (isEditable(activeEditable)) return activeEditable;
            const wrapper = anchor ? (anchor.closest('.comment-input__wrapper, .comment-input-wrapper') || null) : (active ? active.closest('.comment-input__wrapper, .comment-input-wrapper') : null);
            if (!wrapper) return null;
            const nested = wrapper.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
            return isEditable(nested) ? nested : null;
          }
          const input = resolveInput();
          const wrapper = input ? (input.closest('.comment-input__wrapper, .comment-input-wrapper') || input.parentElement) : null;
          const roots = Array.from(document.querySelectorAll('.comment-content')).filter(visible);
          let root = null;
          for (const candidate of roots) {
            const scopes = [candidate, candidate.nextElementSibling, candidate.parentElement, candidate.parentElement && candidate.parentElement.nextElementSibling].filter(Boolean);
            if (wrapper && scopes.some((scope) => scope === wrapper || scope.contains(wrapper))) {
              root = candidate;
              break;
            }
            if (candidate.querySelector('.comment-content__btns__btn.reply-active')) {
              root = candidate;
            }
          }
          const index = root ? roots.indexOf(root) : -1;
          return {
            ok: !!input && index >= 0,
            comment_index: index,
            author: norm(root && root.querySelector('.comment-content__username') ? root.querySelector('.comment-content__username').innerText : ''),
            time_text: norm(root && root.querySelector('.comment-content__date') ? root.querySelector('.comment-content__date').innerText : ''),
            content: norm(root && root.querySelector('.comment-content__detail') ? root.querySelector('.comment-content__detail').innerText : ''),
            input_text: norm(input ? (input.innerText || input.textContent || input.value || '') : '')
          };
        })();
        """,
    )
    if not bool(focus_info.get("ok")):
        empty_result["reason"] = "focused_reply_editor_not_ready"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        empty_result["focus_info"] = focus_info
        return empty_result

    comment_index = max(0, int(focus_info.get("comment_index") or 0))
    comments = _extract_kuaishou_comments(page)
    comment = dict(comments[comment_index]) if 0 <= comment_index < len(comments) else {
        "index": comment_index,
        "author": str(focus_info.get("author") or "").strip(),
        "time_text": str(focus_info.get("time_text") or "").strip(),
        "content": str(focus_info.get("content") or "").strip(),
        "has_reply": False,
        "liked": False,
    }
    comment["index"] = comment_index

    post = _extract_current_kuaishou_post(page) or {
        "post_key": _build_post_key("", ""),
        "title": "",
        "published_text": "",
        "comment_count": len(comments) if comments else 1,
        "has_comments": True,
    }
    should_skip, skip_reason = _should_skip_comment_reply_guarded(comment)
    if should_skip:
        empty_result["reason"] = skip_reason
        empty_result["posts_scanned"] = 1
        empty_result["posts_selected"] = 1
        return empty_result
    fingerprint = engine._comment_reply_fingerprint(post, comment)
    if (not bool(ignore_state)) and (fingerprint in items or bool(comment.get("has_reply"))):
        empty_result["reason"] = "duplicate_or_has_reply"
        empty_result["posts_scanned"] = 1
        empty_result["posts_selected"] = 1
        return empty_result

    typed = False
    if hasattr(page, "run_cdp"):
        try:
            page.run_cdp("Input.insertText", text=clean_reply_text)
            typed = True
        except Exception:
            typed = False
    if not typed:
        typed = bool(
            _run_js_dict(
                page,
                """
                return ((replyText) => {
                  function toElement(node) {
                    if (!node) return null;
                    return node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
                  }
                  function isEditable(node) {
                    return !!node && (
                      node.isContentEditable
                      || node.tagName === 'TEXTAREA'
                      || node.tagName === 'INPUT'
                      || String(node.getAttribute('contenteditable') || '').toLowerCase() === 'true'
                    );
                  }
                  function resolveInput() {
                    const active = document.activeElement;
                    if (isEditable(active)) return active;
                    const selection = window.getSelection ? window.getSelection() : null;
                    const anchor = selection && selection.anchorNode ? toElement(selection.anchorNode) : null;
                    if (isEditable(anchor)) return anchor;
                    const anchorEditable = anchor ? anchor.closest('[contenteditable="true"], textarea, input') : null;
                    if (isEditable(anchorEditable)) return anchorEditable;
                    const activeEditable = active ? active.closest('[contenteditable="true"], textarea, input') : null;
                    if (isEditable(activeEditable)) return activeEditable;
                    const wrapper = anchor ? (anchor.closest('.comment-input__wrapper, .comment-input-wrapper') || null) : (active ? active.closest('.comment-input__wrapper, .comment-input-wrapper') : null);
                    if (!wrapper) return null;
                    return wrapper.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
                  }
                  const active = resolveInput();
                  if (!active) return { ok: false };
                  const text = String(replyText || '');
                  if (active.tagName === 'TEXTAREA' || active.tagName === 'INPUT') {
                    active.value = text;
                    active.dispatchEvent(new Event('input', { bubbles: true }));
                    active.dispatchEvent(new Event('change', { bubbles: true }));
                    return { ok: true };
                  }
                  active.textContent = text;
                  active.dispatchEvent(new InputEvent('input', { bubbles: true, data: text, inputType: 'insertText' }));
                  return { ok: true };
                })(arguments[0]);
                """,
                clean_reply_text,
            ).get("ok")
        )
    typed_ready = _wait_until(
        lambda: bool(
            _run_js_dict(
                page,
                """
                return ((replyText) => {
                  function norm(value) {
                    return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
                  }
                  function toElement(node) {
                    if (!node) return null;
                    return node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
                  }
                  function isEditable(node) {
                    return !!node && (
                      node.isContentEditable
                      || node.tagName === 'TEXTAREA'
                      || node.tagName === 'INPUT'
                      || String(node.getAttribute('contenteditable') || '').toLowerCase() === 'true'
                    );
                  }
                  function resolveInput() {
                    const active = document.activeElement;
                    if (isEditable(active)) return active;
                    const selection = window.getSelection ? window.getSelection() : null;
                    const anchor = selection && selection.anchorNode ? toElement(selection.anchorNode) : null;
                    if (isEditable(anchor)) return anchor;
                    const anchorEditable = anchor ? anchor.closest('[contenteditable="true"], textarea, input') : null;
                    if (isEditable(anchorEditable)) return anchorEditable;
                    const activeEditable = active ? active.closest('[contenteditable="true"], textarea, input') : null;
                    if (isEditable(activeEditable)) return activeEditable;
                    const wrapper = anchor ? (anchor.closest('.comment-input__wrapper, .comment-input-wrapper') || null) : (active ? active.closest('.comment-input__wrapper, .comment-input-wrapper') : null);
                    if (!wrapper) return null;
                    return wrapper.querySelector('[contenteditable="true"], textarea, input[type="text"], input:not([type])');
                  }
                  const active = resolveInput();
                  if (!active) return { ok: false };
                  const text = norm(active.innerText || active.textContent || active.value || '');
                  return { ok: !!(norm(replyText) && text.includes(norm(replyText))) };
                })(arguments[0]);
                """,
                clean_reply_text,
            ).get("ok")
        ),
        timeout_seconds=3.0,
        poll_seconds=0.2,
    )
    if not typed_ready:
        empty_result["reason"] = "reply_text_not_inserted"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    confirm_clicked = bool(
        _run_js_dict(
            page,
            """
            return (() => {
              function norm(value) {
                return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, ' ').trim();
              }
              function visible(node) {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width > 0 && rect.height > 0;
              }
              function click(node) {
                if (!node) return false;
                try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
                try { node.click(); return true; } catch (e) {}
                try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
                return false;
              }
              function toElement(node) {
                if (!node) return null;
                return node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
              }
              const active = document.activeElement;
              const selection = window.getSelection ? window.getSelection() : null;
              const anchor = selection && selection.anchorNode ? toElement(selection.anchorNode) : null;
              const wrapper = (anchor && anchor.closest('.comment-input__wrapper, .comment-input-wrapper'))
                || (active && active.closest('.comment-input__wrapper, .comment-input-wrapper'))
                || (anchor ? anchor.parentElement : null)
                || (active ? active.parentElement : null);
              if (!wrapper) return { ok: false };
              const button = wrapper.querySelector('.comment-input__wrapper__control__btns .comment-btn.sure-btn.sure-btn--is-active')
                || wrapper.querySelector('.comment-btn.sure-btn.sure-btn--is-active')
                || Array.from(wrapper.querySelectorAll('button, div, span')).find((node) => visible(node) && norm(node.innerText || node.textContent || '') === '确认');
              return { ok: click(button) };
            })();
            """,
        ).get("ok")
    )
    if not confirm_clicked:
        empty_result["reason"] = "confirm_button_not_clicked"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    if not _wait_kuaishou_reply_confirm(page, comment_index, clean_reply_text, 12.0):
        empty_result["reason"] = "reply_confirm_timeout"
        empty_result["current_url"] = current_url
        empty_result["current_title"] = current_title
        return empty_result

    record = engine._remember_comment_reply(
        items,
        fingerprint=fingerprint,
        post=post,
        comment=comment,
        reply_text=clean_reply_text,
    )
    state["items"] = items
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save_state(workspace, platform, state)
    engine._append_comment_reply_markdown(_markdown_path(workspace, platform), platform, record)
    return {
        "ok": True,
        "platform": platform,
        "reason": "",
        "state_path": state_path,
        "markdown_path": markdown_path,
        "records": [record],
        "posts_scanned": 1,
        "posts_selected": 1,
        "replies_sent": 1,
        "current_url": current_url,
        "current_title": current_title,
    }


def diagnose_platform_comment_page(
    *,
    platform_name: str,
    workspace: engine.Workspace,
    debug_port: int,
    chrome_path: Optional[str] = None,
    chrome_user_data_dir: str = "",
    auto_open_chrome: bool = True,
) -> dict[str, Any]:
    platform = _normalize_platform(platform_name)
    adapter = PLATFORM_ADAPTERS.get(platform)
    diagnose = PLATFORM_DIAGNOSTICS.get(platform)
    if adapter is None or diagnose is None:
        return {
            "ok": False,
            "platform": platform,
            "reason": f"unsupported engagement platform: {platform}",
        }

    page = engine._connect_chrome(
        debug_port=int(debug_port),
        auto_open_chrome=auto_open_chrome,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=adapter.open_url,
    )
    page.get(adapter.open_url)
    time.sleep(1.0)

    before = diagnose(page)
    picker_opener = PLATFORM_PICKER_OPENERS.get(platform)
    picker_opened = bool(picker_opener(page)) if picker_opener is not None else False
    time.sleep(0.8)
    after = diagnose(page)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = _diagnostics_dir(workspace)
    out_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{platform}_comment_page_{timestamp}"
    json_path = out_dir / f"{prefix}.json"
    html_path = out_dir / f"{prefix}.html"
    text_path = out_dir / f"{prefix}.txt"

    html_body = _snapshot_page_html(page)
    text_body = _snapshot_page_text(page)
    payload = {
        "ok": True,
        "platform": platform,
        "open_url": adapter.open_url,
        "picker_opened": picker_opened,
        "before": before,
        "after": after,
        "json_path": str(json_path),
        "html_path": str(html_path),
        "text_path": str(text_path),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    html_path.write_text(html_body, encoding="utf-8")
    text_path.write_text(text_body, encoding="utf-8")
    return payload


def run_platform_comment_reply(
    *,
    platform_name: str,
    workspace: engine.Workspace,
    runtime_config: dict[str, Any],
    debug_port: int,
    chrome_path: Optional[str] = None,
    chrome_user_data_dir: str = "",
    auto_open_chrome: bool = True,
    max_posts_override: int = 0,
    max_replies_override: int = 0,
    latest_only: bool = False,
    debug: bool = False,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str = "",
    telegram_chat_id: str = "",
    telegram_registry_file: str = "",
    telegram_timeout_seconds: int = 20,
    telegram_api_base: str = "",
    notify_env_prefix: str = engine.DEFAULT_NOTIFY_ENV_PREFIX,
) -> dict[str, Any]:
    platform = _normalize_platform(platform_name)
    adapter = PLATFORM_ADAPTERS.get(platform)
    if adapter is None:
        return {
            "ok": False,
            "platform": platform,
            "implemented": False,
            "reason": f"unsupported engagement platform: {platform}",
        }

    comment_cfg = engine._merge_comment_reply_config(runtime_config.get("comment_reply"))
    if not bool(comment_cfg.get("enabled")):
        return {
            "ok": False,
            "platform": platform,
            "reason": "comment_reply_disabled",
            "state_path": str(_state_path(workspace, platform)),
            "records": [],
            "posts_scanned": 0,
            "posts_selected": 0,
            "replies_sent": 0,
        }

    max_posts = max(1, int(max_posts_override or comment_cfg.get("max_posts_per_run") or 1))
    max_replies = 1 if latest_only else max(1, int(max_replies_override or comment_cfg.get("max_replies_per_run") or 1))
    reply_max_chars = max(6, int(comment_cfg.get("reply_max_chars") or 20))
    debug_enabled = bool(debug or comment_cfg.get("debug"))

    state = _load_state(workspace, platform)
    items = _prune_state_items(state.get("items") if isinstance(state, dict) else {})
    state["items"] = items
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_reply_at = engine._latest_comment_reply_time(items)

    page = engine._connect_chrome(
        debug_port=int(debug_port),
        auto_open_chrome=auto_open_chrome,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=adapter.open_url,
    )
    page.get(adapter.open_url)
    engine._check_platform_login_ready(
        page,
        platform_name=platform,
        open_url=adapter.open_url,
        chrome_user_data_dir=chrome_user_data_dir,
        debug_port=int(debug_port),
        chrome_path=chrome_path,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        telegram_bot_identifier=telegram_bot_identifier,
        telegram_registry_file=telegram_registry_file,
        telegram_timeout_seconds=telegram_timeout_seconds,
        telegram_api_base=telegram_api_base,
        notify_env_prefix=notify_env_prefix,
    )

    posts = adapter.collect_posts(page, max_posts, debug_enabled)
    if debug_enabled and not posts:
        diagnose = PLATFORM_DIAGNOSTICS.get(platform)
        if diagnose is not None:
            engine._comment_reply_log(
                True,
                f"[{platform}] No commented posts selected. Diagnostics: "
                + json.dumps(diagnose(page), ensure_ascii=False),
            )
    reply_records: list[dict[str, Any]] = []
    posts_scanned = 0

    for post in posts:
        if len(reply_records) >= max_replies:
            break
        posts_scanned += 1
        engine._comment_reply_log(
            debug_enabled,
            f"[{platform}] Open post '{engine._single_line_preview(str(post.get('title') or ''), 72)}' comments={post.get('comment_count')}",
        )
        page.get(adapter.open_url)
        time.sleep(1.0)
        if not adapter.open_post(page, post, debug_enabled):
            engine._comment_reply_log(debug_enabled, f"[{platform}] Skip post: comment manager not opened")
            continue
        time.sleep(1.0)

        seen_comment_fingerprints: set[str] = set()
        stagnant_rounds = 0
        for _ in range(8):
            comments = adapter.extract_comments(page)
            engine._comment_reply_log(debug_enabled, f"[{platform}] Visible comments in manager: {len(comments)}")
            sent_in_round = False
            for comment in comments:
                if len(reply_records) >= max_replies:
                    break
                if not isinstance(comment, dict):
                    continue
                should_skip, skip_reason = _should_skip_comment_reply_guarded(comment, self_author_markers=self_author_markers)
                if should_skip:
                    engine._comment_reply_log(
                        debug_enabled,
                        f"[{platform}] Skip comment: {skip_reason} author={engine._single_line_preview(str(comment.get('author') or ''), 24)}",
                    )
                    continue
                fingerprint = engine._comment_reply_fingerprint(post, comment)
                if fingerprint in seen_comment_fingerprints:
                    continue
                seen_comment_fingerprints.add(fingerprint)
                if fingerprint in items or bool(comment.get("has_reply")):
                    continue
                reply_text = engine.generate_comment_reply(
                    post=post,
                    comment=comment,
                    spark_ai=runtime_config.get("spark_ai") if isinstance(runtime_config.get("spark_ai"), dict) else {},
                    prompt_template=str(comment_cfg.get("prompt_template") or engine.DEFAULT_COMMENT_REPLY_PROMPT_TEMPLATE),
                    fallback_replies=list(comment_cfg.get("fallback_replies") or engine.DEFAULT_COMMENT_REPLY_FALLBACKS),
                    max_chars=reply_max_chars,
                )
                if not reply_text:
                    continue
                last_reply_at = engine._apply_comment_reply_wait(
                    comment_cfg,
                    last_reply_at=last_reply_at,
                    debug=debug_enabled,
                )
                if bool(comment_cfg.get("auto_like")):
                    adapter.like_comment_if_needed(page, int(comment.get("index") or 0))
                    engine._apply_comment_reply_like_to_reply_wait(comment_cfg, debug=debug_enabled)
                if not adapter.submit_reply(page, int(comment.get("index") or 0), reply_text):
                    engine._comment_reply_log(debug_enabled, f"[{platform}] Submit reply failed")
                    continue
                if not adapter.wait_reply_confirm(page, int(comment.get("index") or 0), reply_text, 12.0):
                    engine._comment_reply_log(debug_enabled, f"[{platform}] Reply confirm timeout")
                    continue
                record = engine._remember_comment_reply(
                    items,
                    fingerprint=fingerprint,
                    post=post,
                    comment=comment,
                    reply_text=reply_text,
                )
                reply_records.append(record)
                engine._append_comment_reply_markdown(_markdown_path(workspace, platform), platform, record)
                state["items"] = items
                state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _save_state(workspace, platform, state)
                last_reply_at = engine._parse_comment_reply_timestamp(record.get("replied_at")) or datetime.now()
                sent_in_round = True
                time.sleep(1.0)
                if latest_only or len(reply_records) >= max_replies:
                    break
            if latest_only or len(reply_records) >= max_replies:
                break
            if sent_in_round:
                stagnant_rounds = 0
            else:
                stagnant_rounds += 1
            if stagnant_rounds >= 2:
                break
            adapter.scroll_comments(page)
            time.sleep(1.0)

    state["items"] = _prune_state_items(items)
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save_state(workspace, platform, state)
    return {
        "ok": True,
        "platform": platform,
        "reason": "",
        "state_path": str(_state_path(workspace, platform)),
        "markdown_path": str(_markdown_path(workspace, platform)),
        "records": reply_records,
        "posts_scanned": posts_scanned,
        "posts_selected": len(posts),
        "replies_sent": len(reply_records),
    }
