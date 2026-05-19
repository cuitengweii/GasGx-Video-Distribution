from __future__ import annotations

import json
import time
from typing import Any


def _engine():
    from ... import engine as _engine_module

    return _engine_module


def prefers_store_open(post: dict[str, Any]) -> bool:
    if not isinstance(post, dict):
        return False
    source = str(post.get("source") or "").strip().lower()
    if source == "store":
        return True
    return bool(str(post.get("object_id") or "").strip() or str(post.get("export_id") or "").strip())


def open_comment_manager_via_store(page: Any, post: dict[str, Any], timeout_seconds: float = 10.0) -> bool:
    engine = _engine()
    args = {
        "title": str(post.get("title") or "").strip(),
        "published_text": str(post.get("published_text") or "").strip(),
        "object_id": str(post.get("object_id") or "").strip(),
        "export_id": str(post.get("export_id") or "").strip(),
    }
    title = args["title"]
    published_text = args["published_text"]
    js = engine.WECHAT_COMMENT_DOC_HELPER_JS + """
    return ((targetTitle, targetPublishedText) => {
      function norm(value) {
        return String(value || '').replace(/[\u200B-\u200D\uFEFF]/g, '').replace(/\s+/g, ' ').trim();
      }
      function digits(value) {
        return norm(value).replace(/[^\d]/g, '');
      }
      function normalizePublished(value) {
        const raw = norm(value);
        if (!raw) return '';
        const pureDigits = raw.replace(/[^\d]/g, '');
        if (/^\d{10,13}$/.test(pureDigits)) {
          const ms = pureDigits.length === 10 ? Number(pureDigits) * 1000 : Number(pureDigits);
          const date = new Date(ms);
          if (!Number.isNaN(date.getTime())) {
            const year = String(date.getFullYear());
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            const hour = String(date.getHours()).padStart(2, '0');
            const minute = String(date.getMinutes()).padStart(2, '0');
            return `${year}/${month}/${day} ${hour}:${minute}`;
          }
        }
        return raw;
      }
      function samePublished(left, right) {
        const a = normalizePublished(left);
        const b = normalizePublished(right);
        if (!a || !b) return !a && !b;
        if (a === b || a.includes(b) || b.includes(a)) return true;
        const da = digits(a);
        const db = digits(b);
        return !!da && !!db && (da === db || da.endsWith(db) || db.endsWith(da));
      }
      function fuzzyTitleMatch(left, right) {
        const a = norm(left);
        const b = norm(right);
        if (!a || !b) return false;
        if (a === b || a.includes(b) || b.includes(a)) return true;
        const minPrefix = Math.min(a.length, b.length, 18);
        return minPrefix >= 8 && a.slice(0, minPrefix) === b.slice(0, minPrefix);
      }
      function readTitle(node) {
        const titleNode = node && node.querySelector(".feed-title, [class*='feed-title'], .post-title, [class*='post-title'], [data-testid*='title']");
        if (titleNode) return norm(titleNode.innerText || titleNode.textContent || '');
        const fallbackText = norm(node && (node.innerText || node.textContent || ''));
        return fallbackText.split(' ').slice(0, 24).join(' ');
      }
      function readPublished(node) {
        const timeNode = node && node.querySelector(".feed-time, [class*='feed-time'], .post-time, [class*='post-time'], [class*='publish-time'], time");
        return norm(timeNode ? (timeNode.innerText || timeNode.textContent || '') : '');
      }
      function click(el) {
        if (!el) return false;
        try { el.scrollIntoView({block: 'center', inline: 'nearest'}); } catch (e) {}
        const eventInit = { bubbles: true, cancelable: true, view: window };
        try { el.click(); return true; } catch (e) {}
        try { el.dispatchEvent(new MouseEvent('click', eventInit)); return true; } catch (e) {}
        return false;
      }
      function clickEntry(card) {
        const candidates = [
          ['comment-total', card && card.querySelector('.feed-comment-total')],
          ['comment-icon', card && card.querySelector('.weui-icon-outlined-comment')],
          ['comment-action', card && card.querySelector('.feed-tool-item.comment, .action-item.comment, [class*="comment"][class*="action"], [data-testid*="comment"], [data-role*="comment"]')],
          ['feed-content', card && card.querySelector('.feed-content')],
          ['feed-title', card && card.querySelector('.feed-title')],
          ['feed-image', card && card.querySelector('.feed-img')],
          ['feed-card', card],
        ];
        for (const pair of candidates) {
          const name = pair[0];
          const node = pair[1];
          if (node && click(node)) return name;
        }
        return '';
      }
      const cards = Array.from(document.querySelectorAll(".comment-feed-wrap, [class*='comment-feed'], .feed-item, [class*='feed-item'], .post-item, [class*='post-item'], .finder-card, .finder-card-flex, .comment-view, [class*='finder-card'], [data-testid*='feed'], [data-testid*='post']"));
      const targetTitleNorm = norm(targetTitle);
      const targetPublishedNorm = norm(targetPublishedText);
      const exactCard = cards.find((node) => {
        const title = readTitle(node);
        const published = readPublished(node);
        return title === targetTitleNorm && samePublished(published, targetPublishedNorm);
      });
      const fuzzyCard = exactCard || cards.find((node) => {
        const title = readTitle(node);
        const published = readPublished(node);
        if (targetPublishedNorm && !samePublished(published, targetPublishedNorm)) return false;
        if (!targetTitleNorm) return !!(title || published);
        return fuzzyTitleMatch(title, targetTitleNorm);
      });
      const card = fuzzyCard;
      const visibleCards = cards.slice(0, 5).map((node) => ({
        title: readTitle(node),
        published_text: readPublished(node),
      }));
      if (!card) return { ok: false, reason: 'feed_not_found', visible_cards: visibleCards };
      const clickedTarget = clickEntry(card);
      return {
        ok: !!clickedTarget,
        reason: clickedTarget ? 'feed_clicked' : 'feed_click_failed',
        clicked_target: clickedTarget,
        visible_cards: visibleCards,
      };
    })(arguments[0], arguments[1]);
    """
    result = None
    last_error = None
    contexts = []
    try:
        contexts.extend(list(page.get_frames(timeout=max(0.2, min(4.0, float(timeout_seconds))))))
    except Exception:
        pass
    if not contexts:
        contexts = [page]
    for ctx in contexts:
        try:
            candidate = ctx.run_js(js, title, published_text)
        except Exception as exc:
            last_error = exc
            continue
        result = candidate
        if isinstance(candidate, dict) and bool(candidate.get("ok")):
            break
    if result is None and last_error is not None:
        engine._log(f"[CommentReply] Open comment manager failed: {last_error}")
        return False
    if isinstance(result, dict) and not bool(result.get("ok")):
        engine._comment_reply_log(
            True,
            "[CommentReply] Native comment manager target not opened: "
            f"reason={result.get('reason') or 'unknown'} title={title!r} published={published_text!r} "
            f"clicked_target={result.get('clicked_target') or '-'} visible_cards={result.get('visible_cards')!r}",
        )
        return False
    confirm_js = engine.WECHAT_COMMENT_DOC_HELPER_JS + """
    return (() => {
      const doc = resolveWechatCommentDoc();
      return !!doc.querySelector('.feed-detail');
    })();
    """
    try:
        return bool(
            engine._wait_until(lambda: page.run_js(confirm_js), timeout_seconds=timeout_seconds, poll_seconds=0.35)
        )
    except Exception:
        return False


def open_comment_manager(page: Any, post: dict[str, Any], timeout_seconds: float = 10.0) -> bool:
    if prefers_store_open(post):
        if open_comment_manager_via_store(page, post, timeout_seconds=timeout_seconds):
            return True
    return open_comment_manager_via_store(page, post, timeout_seconds=timeout_seconds)
