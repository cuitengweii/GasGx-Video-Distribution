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

COMMENT_REPLY_RETENTION_DAYS = 30


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


def _normalize_post_digits(text: str) -> str:
    return re.sub(r"\D+", "", str(text or ""))


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
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const list = document.querySelector('ul.douyin-creator-interactive-list-items');
      if (visible(list)) return { ok: true, already_open: true };
      const nodes = Array.from(document.querySelectorAll('button, div, span')).filter(visible);
      const trigger = nodes.find((node) => /选择作品/.test(norm(node.innerText || node.textContent || '')));
      if (!trigger) return { ok: false, reason: 'trigger_not_found' };
      return { ok: click(trigger), reason: 'trigger_clicked' };
    })();
    """
    result = _run_js_dict(page, js)
    if not bool(result.get("ok")):
        return False
    return _wait_until(
        lambda: bool(_run_js_dict(page, "return { ok: !!document.querySelector('ul.douyin-creator-interactive-list-items') };").get("ok")),
        timeout_seconds=6.0,
        poll_seconds=0.25,
    )


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
        const countNodes = Array.from(item.querySelectorAll('div, span'))
          .filter(visible)
          .map((node) => {
            const text = norm(node.innerText || node.textContent || '');
            const rect = node.getBoundingClientRect();
            return { text, width: rect.width, height: rect.height };
          })
          .filter((entry) => /^\\d+$/.test(entry.text) && entry.width <= 64 && entry.height <= 36);
        const commentCount = countNodes.length ? toInt(countNodes[countNodes.length - 1].text) : 0;
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


def _wait_douyin_reply_confirm(page: Any, comment_index: int, reply_text: str, timeout_seconds: float = 12.0) -> bool:
    target = re.sub(r"\s+", "", str(reply_text or "").strip())
    js = """
    return ((replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, '').trim();
      }
      const target = norm(replyText);
      const bodyText = norm((document.body || document.documentElement).innerText || '');
      return !!(target && bodyText.includes(target));
    })(arguments[0]);
    """
    return _wait_until(lambda: bool(_run_js_dict(page, js, target).get("ok")), timeout_seconds=timeout_seconds, poll_seconds=0.35)


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
      function click(node) {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
        try { node.click(); return true; } catch (e) {}
        try { node.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window })); return true; } catch (e) {}
        return false;
      }
      const item = document.querySelector('.video-item');
      if (visible(item)) return { ok: true, already_open: true };
      const trigger = Array.from(document.querySelectorAll('button, div, span')).find((node) => visible(node) && /选择视频/.test(norm(node.innerText || node.textContent || '')));
      if (!trigger) return { ok: false, reason: 'trigger_not_found' };
      return { ok: click(trigger) };
    })();
    """
    result = _run_js_dict(page, js)
    if not bool(result.get("ok")):
        return False
    return _wait_until(
        lambda: bool(_run_js_dict(page, "return { ok: !!document.querySelector('.video-item') };").get("ok")),
        timeout_seconds=6.0,
        poll_seconds=0.25,
    )


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
        const metrics = Array.from(item.querySelectorAll('.video-info__content__detail div, .video-info__content__detail span, div, span'))
          .map((node) => norm(node.innerText || node.textContent || ''))
          .filter((text) => /^\\d+$/.test(text));
        const commentCount = metrics.length ? toInt(metrics[metrics.length - 1]) : 0;
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
        const buttons = Array.from(root.querySelectorAll('.comment-content__btns__btn')).filter(visible);
        const likeButton = buttons[0] || null;
        const likeColor = likeButton ? window.getComputedStyle(likeButton).color : '';
        return {
          index,
          author,
          time_text: timeText,
          content,
          has_reply: false,
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
    return ((replyText) => {
      function norm(value) {
        return String(value || '').replace(/[\\u200B-\\u200D\\uFEFF]/g, '').replace(/\\s+/g, '').trim();
      }
      const target = norm(replyText);
      const bodyText = norm((document.body || document.documentElement).innerText || '');
      return !!(target && bodyText.includes(target));
    })(arguments[0]);
    """
    return _wait_until(lambda: bool(_run_js_dict(page, js, target).get("ok")), timeout_seconds=timeout_seconds, poll_seconds=0.35)


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
        submit_reply=_submit_douyin_reply,
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
        submit_reply=_submit_kuaishou_reply,
        wait_reply_confirm=_wait_kuaishou_reply_confirm,
        scroll_comments=_scroll_kuaishou_comments,
    ),
}

PLATFORM_DIAGNOSTICS: dict[str, Callable[[Any], dict[str, Any]]] = {
    "douyin": _diagnose_douyin_page,
    "kuaishou": _diagnose_kuaishou_page,
}


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
        "records": reply_records,
        "posts_scanned": posts_scanned,
        "posts_selected": len(posts),
        "replies_sent": len(reply_records),
    }
