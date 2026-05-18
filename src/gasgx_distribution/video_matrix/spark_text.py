from __future__ import annotations

import json
import random
import re
import unicodedata
from typing import Any, Iterable

from cybercar.common.xfyun_spark import SparkAIClient, extract_json_object

from .models import VideoVariant
from .settings import ProjectSettings


LANGUAGE_LABELS = {
    "zh": "Chinese",
    "en": "English",
    "ru": "Russian",
}

TEXT_VARIANT_TOPICS = [
    ("燃气现场发电", "稳定供电", "把现场废气转成稳定电力"),
    ("弃气变算力", "算力变现", "把废气转成可用收益"),
    ("分布式能源", "就地供能", "适配矿场和工业负载"),
    ("低成本供电", "可靠输出", "让现场设备持续运行"),
    ("移动式电站", "快速部署", "几天完成上线"),
    ("油气伴生气", "持续利用", "把闲置资源变成电"),
]

TEXT_VARIANT_EXPANSION_TOPICS = [
    ("现场供电方案", "降本增效", "适合边远站点和临时项目"),
    ("天然气发电系统", "稳定运行", "支持全天候连续发电"),
    ("分布式电力模块", "按需扩容", "从试点快速扩展"),
    ("工业负载供能", "适配场景", "服务数据中心和工厂"),
    ("野外电力站", "快速落地", "适合井场和工地"),
    ("绿色能源利用", "减少浪费", "提升每一份气的价值"),
    ("撬装式发电", "灵活部署", "可移动可复制"),
    ("油气资源再利用", "持续变现", "让废气变成电力资产"),
    ("远程站点供电", "稳定可控", "保障关键设备不停机"),
    ("现场能源改造", "一步到位", "减少施工和维护成本"),
    ("零碳改造路径", "高效落地", "兼顾排放和收益"),
    ("热电联供优化", "综合利用", "提升整体能效"),
]

TEXT_VARIANT_ANGLES = [
    ("Field Ready", "现场就绪"),
    ("Stable Output", "稳定输出"),
    ("Fast Deploy", "快速部署"),
    ("Low Cost", "低成本"),
    ("High Uptime", "高可用"),
    ("Remote Site", "远程站点"),
    ("Clean Power", "清洁供能"),
    ("Value Recovery", "价值回收"),
]

DEFAULT_HEADLINE_SEED = "Gas Engines That Turn Field Gas Into Power"
CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
LATIN_RE = re.compile(r"[A-Za-z]")
EXTRA_PROMPT_MAX_CHARS = 240
EXTRA_PROMPT_MAX_LINES = 4
EXTRA_PROMPT_URL_RE = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
HUD_MAX_LINES = 2
HUD_MAX_CHARS_PER_LINE = 10
LIST_MARKER_PREFIX_RE = re.compile(r"^\s*(?:[\-*•·]+\s*)?(?:\(?\d{1,3}\)?[.)、:：\-]\s+)+")
LIST_MARKER_SUFFIX_RE = re.compile(r"\s+(?:[\-*•·]+\s*)?(?:\(?\d{1,3}\)?[.)、:：\-]?)\s*$")


def _normalize_extra_prompt(raw: str) -> str:
    text = str(raw or "").replace("\r\n", "\n").strip()
    if not text:
        return ""
    text = EXTRA_PROMPT_URL_RE.sub("", text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    normalized = "\n".join(lines[:EXTRA_PROMPT_MAX_LINES]).strip()
    if len(normalized) > EXTRA_PROMPT_MAX_CHARS:
        normalized = normalized[:EXTRA_PROMPT_MAX_CHARS].strip()
    return normalized


def clean_generated_text(text: str) -> str:
    lines: list[str] = []
    for raw_line in str(text or "").replace("\r\n", "\n").split("\n"):
        line = _clean_generated_text_line(raw_line)
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def sanitize_headline_text(text: str) -> str:
    lines: list[str] = []
    for raw_line in clean_generated_text(text).splitlines():
        line = _sanitize_headline_line(raw_line)
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def _clean_generated_text_line(text: str) -> str:
    line = unicodedata.normalize("NFKC", str(text or "")).replace("\r\n", "\n").strip()
    if not line:
        return ""
    line = re.sub(r"\s+", " ", line)
    tokens = line.split(" ")
    if len(tokens) > 2 and re.fullmatch(r"(?:#\d{1,3}|\(?\d{1,3}\)?[.)、:：\-]?|\d{1,3})", tokens[0]):
        tokens = tokens[1:]
    if len(tokens) > 2 and re.fullmatch(r"(?:#\d{1,3}|\(?\d{1,3}\)?[.)、:：\-]?|\d{1,3})", tokens[-1]):
        tokens = tokens[:-1]
    line = " ".join(tokens).strip()
    line = LIST_MARKER_PREFIX_RE.sub("", line).strip()
    line = LIST_MARKER_SUFFIX_RE.sub("", line).strip()
    return re.sub(r"\s+", " ", line).strip()


def _sanitize_headline_line(text: str) -> str:
    line = unicodedata.normalize("NFKC", str(text or "")).strip()
    if not line:
        return ""
    line = re.sub(r"\s+", " ", line)
    line = re.sub(r"\s+(?:#\d{1,3}|\(?\d{1,3}\)?[.)、:：\-]?)(?=\s*$)", "", line).strip()
    line = re.sub(r"^(?:#\d{1,3}|\(?\d{1,3}\)?[.)、:：\-]?|\d{1,3})\s+", "", line).strip()
    return re.sub(r"\s+", " ", line).strip()


def build_marketing_copy(
    variant: VideoVariant,
    settings: ProjectSettings,
    language: str,
    template_copy: str,
    ending_follow_text: str = "",
    extra_prompt: str = "",
) -> str:
    local_copy = _local_copy(variant, settings, template_copy, ending_follow_text)
    spark_copy = _try_spark_copy(variant, settings, language, ending_follow_text, extra_prompt=extra_prompt)
    return clean_generated_text(spark_copy or local_copy)


def build_text_variants(
    settings: ProjectSettings,
    hud_lines: list[str],
    count: int,
    *,
    language: str = "zh",
    narrative_templates: list[dict] | None = None,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[dict[str, object]]:
    target_count = max(1, int(count or 1))
    avoid_keys = _text_key_set(avoid_texts)
    request_count = target_count + len(avoid_keys)
    local_variants = _fallback_text_variants(settings, hud_lines, max(target_count, request_count), source="template")
    spark_variants = _try_spark_text_variants(settings, hud_lines, target_count, language, narrative_templates, extra_prompt=extra_prompt, avoid_texts=avoid_texts)
    variants = _normalize_text_variants([*(spark_variants or []), *local_variants], target_count, avoid_texts=avoid_texts)
    if len(variants) < target_count:
        refill = _fallback_text_variants(settings, hud_lines, max(target_count * 4, request_count * 2), source="template")
        variants = _normalize_text_variants([*variants, *refill], target_count, avoid_texts=avoid_texts)
    return variants[:target_count]


def build_headline_variants(
    seed_headline: str,
    count: int,
    *,
    language: str = "zh",
    settings: ProjectSettings | None = None,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[str]:
    target_count = max(1, int(count or 1))
    seed = seed_headline.strip() or DEFAULT_HEADLINE_SEED
    target_length = _content_length(seed)
    if target_length <= 0:
        target_length = _content_length(DEFAULT_HEADLINE_SEED)
    min_len = max(10, int(target_length * 0.8))
    max_len = max(min_len + 4, int(target_length * 1.2))

    spark_variants = _try_spark_headline_variants(
        settings=settings,
        seed_headline=seed,
        count=target_count,
        language=language,
        min_len=min_len,
        max_len=max_len,
        extra_prompt=extra_prompt,
        avoid_texts=avoid_texts,
    )
    avoid_keys = _text_key_set(avoid_texts)
    fallback_variants = _fallback_headline_variants(seed, target_count + len(avoid_keys), min_len=min_len, max_len=max_len)
    merged = _normalize_headline_variants([*spark_variants, *fallback_variants], target_count, min_len=min_len, max_len=max_len, avoid_texts=avoid_texts)
    if len(merged) < target_count:
        merged = _fill_headline_variants_with_relaxed_fallback(
            merged,
            [*spark_variants, *fallback_variants, *_fallback_headline_variants(seed, target_count * 4, min_len=1, max_len=10_000)],
            target_count,
            avoid_texts=avoid_texts,
        )
    return merged[:target_count]


def build_description_variants(
    seed_description: str,
    count: int,
    *,
    language: str = "zh",
    settings: ProjectSettings | None = None,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[str]:
    target_count = max(1, int(count or 1))
    seed = str(seed_description or "").strip()
    spark_variants = _try_spark_description_variants(
        settings=settings,
        seed_description=seed,
        count=target_count,
        language=language,
        extra_prompt=extra_prompt,
        avoid_texts=avoid_texts,
    )
    fallback_variants = _fallback_description_variants(seed, target_count)
    return _normalize_bilingual_description_variants([*spark_variants, *fallback_variants], target_count, avoid_texts=avoid_texts)


def build_follow_text_variants(
    seed_text: str,
    count: int,
    *,
    language: str = "zh",
    settings: ProjectSettings | None = None,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[str]:
    target_count = max(1, int(count or 1))
    seed = str(seed_text or "").strip()
    spark_variants = _try_spark_follow_text_variants(
        settings=settings,
        seed_text=seed,
        count=target_count,
        language=language,
        extra_prompt=extra_prompt,
        avoid_texts=avoid_texts,
    )
    fallback_variants = _fallback_follow_text_variants(seed, target_count)
    normalized = _normalize_simple_text_variants([*spark_variants, *fallback_variants], target_count, avoid_texts=avoid_texts)
    if not normalized:
        return _fallback_follow_text_variants(seed, target_count)
    return normalized


def normalize_hud_lines(lines: Iterable[str], *, max_lines: int = HUD_MAX_LINES, max_chars_per_line: int = HUD_MAX_CHARS_PER_LINE) -> list[str]:
    normalized: list[str] = []
    for raw in lines:
        text = _normalize_hud_line(raw, max_chars=max_chars_per_line)
        if not text:
            continue
        normalized.append(text)
        if len(normalized) >= max_lines:
            break
    return normalized


def build_hud_variants(
    seed_lines: Iterable[str],
    count: int,
    *,
    language: str = "zh",
    settings: ProjectSettings | None = None,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[list[str]]:
    target_count = max(1, int(count or 1))
    seed = normalize_hud_lines(seed_lines)
    spark_variants = _try_spark_hud_variants(
        settings=settings,
        seed_lines=seed,
        count=target_count,
        language=language,
        extra_prompt=extra_prompt,
        avoid_texts=avoid_texts,
    )
    fallback_variants = _fallback_hud_variants(seed, target_count)
    merged = _normalize_hud_variants([*spark_variants, *fallback_variants], target_count, avoid_texts=avoid_texts)
    if len(merged) < target_count:
        refill = _fallback_hud_variants(seed, target_count * 3)
        merged = _normalize_hud_variants([*merged, *refill], target_count, avoid_texts=avoid_texts)
    return merged[:target_count]


def _spark_client(settings: ProjectSettings | None = None) -> SparkAIClient | None:
    if settings is not None and not str(settings.copy_mode or "").startswith("spark"):
        return None
    client = SparkAIClient(timeout_seconds=12)
    if not client.is_ready():
        return None
    return client


def _try_spark_text_variants(
    settings: ProjectSettings,
    hud_lines: list[str],
    count: int,
    language: str,
    narrative_templates: list[dict] | None,
    *,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[dict[str, object]]:
    client = _spark_client(settings)
    if client is None:
        return []

    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    narrative_names = ", ".join(
        str(item.get("name") or item.get("id") or "")
        for item in narrative_templates or []
        if isinstance(item, dict)
    )
    prompt = (
        f"Generate {count} distinct short-video text variants in {target_language} for GasGx.\n"
        "Return JSON only with this schema: "
        '{"variants":[{"title":"...","slogan":"...","hud_lines":["..."],"opening_text":"..."}]}.\n'
        "Keep each title and slogan concise, and avoid duplicate openings.\n"
        f"Project: {settings.project_name}.\n"
        f"Narrative skeletons: {narrative_names or 'quick showcase, FAQ, contrast, step list, voiceover b-roll'}.\n"
        f"Existing HUD hints: {' | '.join(hud_lines)}."
    )
    guidance = _normalize_extra_prompt(extra_prompt)
    if guidance:
        prompt = f"{prompt}\nAdditional guidance (must follow):\n{guidance}"
    avoid_prompt = _avoidance_prompt(avoid_texts)
    if avoid_prompt:
        prompt = f"{prompt}\nAvoid reusing these same-day texts:\n{avoid_prompt}"
    output = client.chat(prompt)
    payload = _extract_json_payload(output)
    raw_variants = payload.get("variants") if isinstance(payload, dict) else []
    if not isinstance(raw_variants, list):
        return []

    variants: list[dict[str, object]] = []
    for index, item in enumerate(raw_variants, start=1):
        if not isinstance(item, dict):
            continue
        variants.append(
            {
                "id": f"spark_{index:02d}",
                "source": "spark",
                "title": str(item.get("title") or "").strip(),
                "slogan": str(item.get("slogan") or "").strip(),
                "hud_lines": [str(line).strip() for line in item.get("hud_lines") or [] if str(line).strip()],
                "opening_text": str(item.get("opening_text") or "").strip(),
            }
        )
    return variants


def _try_spark_headline_variants(
    *,
    settings: ProjectSettings | None,
    seed_headline: str,
    count: int,
    language: str,
    min_len: int,
    max_len: int,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[str]:
    client = _spark_client(settings)
    if client is None:
        return []

    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    request_count = max(count * 2, count + 2)
    prompt = (
        f"Generate {request_count} unique short-video headlines for GasGx.\n"
        "Each headline must be exactly two lines: line 1 English, line 2 Chinese.\n"
        "Output JSON only: "
        '{"variants":[{"en":"...","zh":"..."}]} or {"variants":["EN line\\n中文行"]}.\n'
        "Keep meaning related to gas engines, generator sets, field gas monetization, and onsite power.\n"
        f"Use mixed bilingual style for {target_language} audience.\n"
        f"Length target per headline (non-space chars): {min_len} to {max_len}.\n"
        f"Reference seed headline: {seed_headline}"
    )
    guidance = _normalize_extra_prompt(extra_prompt)
    if guidance:
        prompt = f"{prompt}\nAdditional guidance (must follow):\n{guidance}"
    avoid_prompt = _avoidance_prompt(avoid_texts)
    if avoid_prompt:
        prompt = f"{prompt}\nAvoid reusing these same-day headlines:\n{avoid_prompt}"
    output = client.chat(prompt)
    payload = _extract_json_payload(output)
    raw_variants = payload.get("variants") if isinstance(payload, dict) else []
    if not isinstance(raw_variants, list):
        return []

    lines: list[str] = []
    for item in raw_variants:
        if isinstance(item, str):
            lines.append(item)
            continue
        if not isinstance(item, dict):
            continue
        headline = str(item.get("headline") or "").strip()
        if headline:
            lines.append(headline)
            continue
        en = str(item.get("en") or item.get("english") or "").strip()
        zh = str(item.get("zh") or item.get("chinese") or "").strip()
        if en and zh:
            lines.append(f"{en}\n{zh}")
    return lines


def _try_spark_description_variants(
    *,
    settings: ProjectSettings | None,
    seed_description: str,
    count: int,
    language: str,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[str]:
    client = _spark_client(settings)
    if client is None:
        return []
    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    request_count = max(count * 2, count + 2)
    prompt = (
        f"Generate {request_count} unique short social-video descriptions for GasGx in {target_language}.\n"
        "Each item must be exactly two lines: line 1 English, line 2 Chinese.\n"
        "Do not mix languages in the same line.\n"
        "Output JSON only: {\"variants\":[{\"en\":\"...\",\"zh\":\"...\"}]} or {\"variants\":[\"EN line\\n中文行\"]}.\n"
        "Do not include links, hashtags, markdown, labels, or section titles.\n"
        "Each item should be concise and publication-ready."
    )
    if seed_description:
        prompt = f"{prompt}\nReference description:\n{seed_description}"
    guidance = _normalize_extra_prompt(extra_prompt)
    if guidance:
        prompt = f"{prompt}\nAdditional guidance (must follow):\n{guidance}"
    avoid_prompt = _avoidance_prompt(avoid_texts)
    if avoid_prompt:
        prompt = f"{prompt}\nAvoid reusing these same-day descriptions:\n{avoid_prompt}"
    payload = _extract_json_payload(client.chat(prompt))
    raw_variants = payload.get("variants") if isinstance(payload, dict) else []
    if not isinstance(raw_variants, list):
        return []
    variants: list[str] = []
    for item in raw_variants:
        text = ""
        if isinstance(item, dict):
            en = clean_generated_text(str(item.get("en") or item.get("english") or "")).strip()
            zh = clean_generated_text(str(item.get("zh") or item.get("chinese") or "")).strip()
            if en and zh:
                text = f"{en}\n{zh}"
            else:
                text = str(item.get("text") or item.get("description") or "").strip()
        elif isinstance(item, list) and len(item) >= 2:
            en = clean_generated_text(str(item[0] or "")).strip()
            zh = clean_generated_text(str(item[1] or "")).strip()
            if en and zh:
                text = f"{en}\n{zh}"
        else:
            text = str(item or "").strip()
        normalized = _normalize_bilingual_description(text)
        if normalized:
            variants.append(normalized)
    return variants


def _try_spark_follow_text_variants(
    *,
    settings: ProjectSettings | None,
    seed_text: str,
    count: int,
    language: str,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[str]:
    client = _spark_client(settings)
    if client is None:
        return []
    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    request_count = max(count * 2, count + 2)
    prompt = (
        f"Generate {request_count} unique ending follow-copy lines for GasGx in {target_language}.\n"
        "Output JSON only: {\"variants\":[\"...\"]}.\n"
        "Each line should be concise, natural, and contain no links."
    )
    if seed_text:
        prompt = f"{prompt}\nReference follow copy:\n{seed_text}"
    guidance = _normalize_extra_prompt(extra_prompt)
    if guidance:
        prompt = f"{prompt}\nAdditional guidance (must follow):\n{guidance}"
    avoid_prompt = _avoidance_prompt(avoid_texts)
    if avoid_prompt:
        prompt = f"{prompt}\nAvoid reusing these same-day follow copies:\n{avoid_prompt}"
    payload = _extract_json_payload(client.chat(prompt))
    raw_variants = payload.get("variants") if isinstance(payload, dict) else []
    if not isinstance(raw_variants, list):
        return []
    return [clean_generated_text(str(item or "")).strip() for item in raw_variants if clean_generated_text(str(item or "")).strip()]


def _try_spark_hud_variants(
    *,
    settings: ProjectSettings | None,
    seed_lines: list[str],
    count: int,
    language: str,
    extra_prompt: str = "",
    avoid_texts: Iterable[str] | None = None,
) -> list[list[str]]:
    client = _spark_client(settings)
    if client is None:
        return []
    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    request_count = max(count * 2, count + 2)
    prompt = (
        f"Generate {request_count} unique HUD text pairs for GasGx in {target_language}.\n"
        "Each item must contain exactly 2 short lines.\n"
        f"Each line must be <= {HUD_MAX_CHARS_PER_LINE} non-space characters.\n"
        "Output JSON only: {\"variants\":[{\"lines\":[\"...\",\"...\"]}]}\n"
        "Do not include links."
    )
    if seed_lines:
        prompt = f"{prompt}\nReference HUD lines: {' | '.join(seed_lines)}"
    guidance = _normalize_extra_prompt(extra_prompt)
    if guidance:
        prompt = f"{prompt}\nAdditional guidance (must follow):\n{guidance}"
    avoid_prompt = _avoidance_prompt(avoid_texts)
    if avoid_prompt:
        prompt = f"{prompt}\nAvoid reusing these same-day HUD texts:\n{avoid_prompt}"
    payload = _extract_json_payload(client.chat(prompt))
    raw_variants = payload.get("variants") if isinstance(payload, dict) else []
    if not isinstance(raw_variants, list):
        return []
    variants: list[list[str]] = []
    for item in raw_variants:
        if isinstance(item, dict):
            lines = item.get("lines") or item.get("hud_lines") or []
        elif isinstance(item, list):
            lines = item
        elif isinstance(item, str):
            lines = str(item).replace("|", "\n").splitlines()
        else:
            lines = []
        normalized = normalize_hud_lines([str(line) for line in lines])
        if normalized:
            variants.append(normalized)
    return variants


def _extract_json_payload(output: str | None) -> dict[str, Any]:
    text = str(output or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    payload = extract_json_object(text)
    return payload if isinstance(payload, dict) else {}


def _normalize_simple_text_variants(raw_items: list[str], count: int, *, avoid_texts: Iterable[str] | None = None) -> list[str]:
    variants: list[str] = []
    seen: set[str] = set()
    avoid_keys = _text_key_set(avoid_texts)
    for item in raw_items:
        text = clean_generated_text(str(item or "")).strip()
        if not text:
            continue
        key = _text_key(text)
        if not key or key in seen or key in avoid_keys:
            continue
        seen.add(key)
        variants.append(text)
        if len(variants) >= count:
            break
    return variants


def _normalize_bilingual_description(text: str) -> str | None:
    lines = [clean_generated_text(line).strip() for line in str(text or "").replace("\r\n", "\n").split("\n") if line.strip()]
    if len(lines) != 2:
        return None
    english, chinese = lines
    if not LATIN_RE.search(english):
        return None
    if not CJK_RE.search(chinese):
        return None
    return f"{english}\n{chinese}"


def _normalize_bilingual_description_variants(raw_items: list[str], count: int, *, avoid_texts: Iterable[str] | None = None) -> list[str]:
    variants: list[str] = []
    seen: set[str] = set()
    avoid_keys = _text_key_set(avoid_texts)
    for item in raw_items:
        normalized = _normalize_bilingual_description(item)
        if not normalized:
            continue
        key = _text_key(normalized)
        if not key or key in seen or key in avoid_keys:
            continue
        seen.add(key)
        variants.append(normalized)
        if len(variants) >= count:
            break
    return variants


def _normalize_hud_line(text: str, *, max_chars: int = HUD_MAX_CHARS_PER_LINE) -> str:
    clean = clean_generated_text(str(text or ""))
    if not clean:
        return ""
    if _content_length(clean) <= max_chars:
        return clean
    out: list[str] = []
    count = 0
    for char in clean:
        if char.isspace():
            if out and out[-1] != " ":
                out.append(" ")
            continue
        if count >= max_chars:
            break
        out.append(char)
        count += 1
    return "".join(out).strip()


def _normalize_hud_variants(raw_items: list[list[str]], count: int, *, avoid_texts: Iterable[str] | None = None) -> list[list[str]]:
    variants: list[list[str]] = []
    seen: set[str] = set()
    avoid_keys = _text_key_set(avoid_texts)
    for item in raw_items:
        lines = normalize_hud_lines(item)
        if not lines:
            continue
        key = _text_key(" | ".join(lines))
        if not key or key in seen or key in avoid_keys:
            continue
        seen.add(key)
        variants.append(lines)
        if len(variants) >= count:
            break
    return variants


def _fallback_description_variants(seed_description: str, count: int) -> list[str]:
    seed = _normalize_bilingual_description(seed_description)
    seeds = [
        seed or (
            "GasGx turns stranded gas into stable onsite power.\n"
            "GasGx 把搁浅天然气转成稳定的现场电力。"
        ),
        (
            "Deploy generator sets near the gas source to reduce waste.\n"
            "在气源附近部署发电机组，减少浪费。"
        ),
        (
            "Turn stranded gas into continuous power and improve efficiency.\n"
            "把搁浅天然气转成持续电力，提升能效。"
        ),
        (
            "GasGx supports remote industrial and compute loads.\n"
            "GasGx 支持远程工业和算力负载。"
        ),
        (
            "Build onsite power from field gas and keep loads running steadily.\n"
            "把现场气源转成就地供电，让负载稳定运行。"
        ),
        (
            "Turn excess gas into usable electricity with practical deployment.\n"
            "把富余气源转成可用电力，适合落地部署。"
        ),
        (
            "Reduce flaring while creating reliable onsite power.\n"
            "减少放空的同时，生成可靠的现场电力。"
        ),
    ]
    variants: list[str] = []
    for index in range(max(1, count)):
        base = seeds[index % len(seeds)]
        if not base:
            continue
        if index >= len(seeds):
            extras = [
                ("for onsite deployment", "适合现场部署"),
                ("for practical field use", "适合现场使用"),
                ("for remote power cases", "适合远程供电场景"),
                ("with stable output focus", "强调稳定输出"),
                ("with low-cost energy delivery", "强调低成本供能"),
            ]
            extra_en, extra_zh = extras[(index - len(seeds)) % len(extras)]
            english, chinese = base.split("\n", 1)
            base = f"{english} {extra_en}\n{chinese}，{extra_zh}"
        variants.append(base)
    return variants


def _fallback_follow_text_variants(seed_text: str, count: int) -> list[str]:
    seed = seed_text.strip()
    seeds = [
        seed,
        "Follow GasGx for field power updates",
        "See more onsite gas power cases",
        "Track new gas engine deployments",
        "Watch more real site operations",
        "Get more practical energy deployment ideas",
        "See more low-cost onsite power builds",
    ]
    variants: list[str] = []
    for index in range(max(1, count)):
        base = seeds[index % len(seeds)].strip()
        if not base:
            continue
        if index >= len(seeds):
            extras = [
                "for field updates",
                "for site deployment notes",
                "for practical power cases",
                "for more build stories",
            ]
            base = f"{base} {extras[(index - len(seeds)) % len(extras)]}"
        variants.append(base)
    return variants


def _fallback_hud_variants(seed_lines: list[str], count: int) -> list[list[str]]:
    seed = normalize_hud_lines(seed_lines)
    seeds = [
        seed,
        ["Gas to Power", "Stable Output"],
        ["Onsite Energy", "Field Ready"],
        ["Low Cost kWh", "Remote Load"],
        ["Engine + Genset", "24/7 Supply"],
        ["Gas Field", "Power Live"],
        ["Site Build", "Clean Output"],
    ]
    variants: list[list[str]] = []
    for index in range(max(1, count)):
        base = seeds[index % len(seeds)] or ["GasGx Power", "Field Deploy"]
        normalized = normalize_hud_lines(base)
        if not normalized:
            continue
        if index >= len(seeds):
            modifiers = ["Field", "Site", "Power", "Ready"]
            normalized = normalize_hud_lines([f"{normalized[0]} {modifiers[(index - len(seeds)) % len(modifiers)]}", normalized[-1]])
        variants.append(normalized)
    return variants


def _try_spark_copy(
    variant: VideoVariant,
    settings: ProjectSettings,
    language: str,
    ending_follow_text: str = "",
    *,
    extra_prompt: str = "",
) -> str | None:
    client = _spark_client(settings)
    if client is None:
        return None
    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    prompt = (
        f"Generate a concise social video publishing copy in {target_language}.\n"
        "Brand: GasGx.\n"
        f"Title: {variant.title}\n"
        f"Slogan: {variant.slogan}\n"
        f"Ending follow copy: {ending_follow_text.strip()}\n"
        f"HUD text: {' | '.join(variant.hud_lines)}\n"
        "Return only the final copy. Do not include CTA labels or links."
    )
    guidance = _normalize_extra_prompt(extra_prompt)
    if guidance:
        prompt = f"{prompt}\nAdditional guidance (must follow):\n{guidance}"
    output = client.chat(prompt)
    clean = str(output or "").strip()
    return clean or None


def _local_copy(
    variant: VideoVariant,
    settings: ProjectSettings,
    template_copy: str,
    ending_follow_text: str = "",
) -> str:
    return template_copy.format(
        title=variant.title,
        slogan=variant.slogan,
        sequence_number=f"{variant.sequence_number:02d}",
        ending_follow_text=ending_follow_text.strip(),
        hud_summary="\n".join(variant.hud_lines),
    )


def _fallback_text_variants(settings: ProjectSettings, hud_lines: list[str], count: int, *, source: str) -> list[dict[str, object]]:
    titles = [str(item).strip() for item in settings.titles if str(item).strip()] or [settings.default_title_prefix or "GasGx"]
    slogans = [str(item).strip() for item in settings.slogans if str(item).strip()] or [settings.project_name or "GasGx"]
    base_hud = [str(line).strip() for line in hud_lines if str(line).strip()]
    topics = [*TEXT_VARIANT_TOPICS, *TEXT_VARIANT_EXPANSION_TOPICS]
    variants: list[dict[str, object]] = []
    total_topics = len(topics)
    total_angles = len(TEXT_VARIANT_ANGLES)
    for index in range(max(1, count)):
        topic, slogan_tail, hud_line = topics[index % total_topics]
        cycle = index // total_topics
        title_base = titles[index % len(titles)]
        title = title_base if index == 0 else f"{topic}: {title_base}"
        slogan = slogans[index % len(slogans)] if index == 0 else slogan_tail
        opening_text = hud_line
        if index == 0:
            slogan = slogans[index % len(slogans)]
        elif cycle > 0:
            angle, angle_tail = TEXT_VARIANT_ANGLES[cycle % total_angles]
            title = f"{topic}: {title_base} - {angle}"
            slogan = f"{slogan_tail} | {angle_tail}"
            opening_text = f"{hud_line} · {angle_tail}"
        variant_hud = [opening_text]
        if index > 0 and base_hud:
            variant_hud = [opening_text, *base_hud[:1]]
        elif index == 0 and base_hud:
            variant_hud = base_hud
        variants.append(
            {
                "id": f"{source}_{index + 1:02d}",
                "source": source,
                "title": title,
                "slogan": slogan,
                "hud_lines": variant_hud,
                "opening_text": opening_text,
            }
        )
    return variants


def _normalize_text_variants(raw_items: list[dict[str, object]], count: int, *, avoid_texts: Iterable[str] | None = None) -> list[dict[str, object]]:
    variants: list[dict[str, object]] = []
    seen: set[str] = set()
    avoid_keys = _text_key_set(avoid_texts)
    for item in raw_items:
        title = clean_generated_text(str(item.get("title") or "")).strip()
        slogan = clean_generated_text(str(item.get("slogan") or "")).strip()
        hud_lines = [clean_generated_text(str(line)).strip() for line in item.get("hud_lines") or [] if str(line).strip()]
        opening_text = clean_generated_text(str(item.get("opening_text") or "")).strip()
        key = " ".join([title, slogan, opening_text, *hud_lines]).strip().lower()
        normalized_key = _text_key(key)
        display_key = _text_key(" ".join([title, slogan, *hud_lines]))
        if not key or normalized_key in seen or display_key in seen or normalized_key in avoid_keys or display_key in avoid_keys:
            continue
        seen.add(normalized_key)
        seen.add(display_key)
        variants.append(
            {
                "id": str(item.get("id") or f"text_{len(variants) + 1:02d}").strip(),
                "source": str(item.get("source") or "template").strip(),
                "title": title,
                "slogan": slogan,
                "hud_lines": hud_lines,
                "opening_text": opening_text,
            }
        )
        if len(variants) >= count:
            break
    return variants


def _normalize_headline_variants(raw_items: list[str], count: int, *, min_len: int, max_len: int, avoid_texts: Iterable[str] | None = None) -> list[str]:
    variants: list[str] = []
    seen: set[str] = set()
    avoid_keys = _text_key_set(avoid_texts)
    for item in raw_items:
        normalized = _normalize_bilingual_headline(item)
        if not normalized:
            continue
        length = _content_length(normalized)
        if length < min_len or length > max_len:
            continue
        key = _text_key(normalized)
        if key in seen or key in avoid_keys:
            continue
        seen.add(key)
        variants.append(normalized)
        if len(variants) >= count:
            break
    return variants


def _fill_headline_variants_with_relaxed_fallback(existing: list[str], raw_items: list[str], count: int, *, avoid_texts: Iterable[str] | None = None) -> list[str]:
    variants = list(existing)
    seen = {_text_key(item) for item in variants}
    avoid_keys = _text_key_set(avoid_texts)
    for item in raw_items:
        normalized = _normalize_bilingual_headline(item)
        if not normalized:
            continue
        key = _text_key(normalized)
        if key in seen or key in avoid_keys:
            continue
        seen.add(key)
        variants.append(normalized)
        if len(variants) >= count:
            break
    return variants


def _text_key(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").replace("\r\n", "\n")).strip().lower()


def _text_key_set(items: Iterable[str] | None) -> set[str]:
    keys: set[str] = set()
    for item in items or []:
        key = _text_key(item)
        if key:
            keys.add(key)
    return keys


def _avoidance_prompt(items: Iterable[str] | None, *, max_items: int = 30) -> str:
    lines: list[str] = []
    seen: set[str] = set()
    for item in items or []:
        text = str(item or "").replace("\r\n", "\n").strip()
        key = _text_key(text)
        if not text or key in seen:
            continue
        seen.add(key)
        lines.append(f"- {text}")
        if len(lines) >= max_items:
            break
    return "\n".join(lines)


def _normalize_bilingual_headline(text: str) -> str | None:
    lines = [sanitize_headline_text(line).strip() for line in str(text or "").replace("\r\n", "\n").split("\n") if line.strip()]
    if len(lines) != 2:
        return None
    en, zh = lines
    if not LATIN_RE.search(en):
        return None
    if not CJK_RE.search(zh):
        return None
    return f"{en}\n{zh}"


def _content_length(text: str) -> int:
    return sum(1 for ch in str(text or "") if not ch.isspace())


def _fallback_headline_variants(seed: str, count: int, *, min_len: int, max_len: int) -> list[str]:
    seed = seed.strip() or DEFAULT_HEADLINE_SEED
    topics = [
        ("Field Gas to Stable Power", "现场燃气转稳定电力"),
        ("Turn Waste Gas Into Hashrate", "把废气变成算力"),
        ("Deploy Power Where Gas Lives", "有气的地方就能发电"),
        ("Industrial Load, Onsite Energy", "工业负载，本地供能"),
        ("Generator Sets for Remote Sites", "适合远程站点的发电机组"),
        ("Lower Flaring, Higher Output", "减少放空，提升产出"),
        ("Gas Engines Built for Uptime", "为高可用而生的燃气发动机"),
        ("From Gas Source to Digital Value", "从气源到数字价值"),
    ]
    variants: list[str] = []
    seen: set[str] = set()

    modifiers = [
        ("Onsite Power", "现场供电"),
        ("Field Ready", "现场可用"),
        ("Industrial Load", "工业负载"),
        ("Remote Sites", "远程站点"),
        ("Clean Output", "清洁输出"),
        ("Stable Supply", "稳定供给"),
    ]
    for index in range(max(count * 2, len(topics))):
        en_base, zh_base = topics[index % len(topics)]
        suffix = index // len(topics)
        if suffix > 0:
            mod_en, mod_zh = modifiers[(suffix - 1) % len(modifiers)]
            en = f"{en_base} {mod_en}"
            zh = f"{zh_base} · {mod_zh}"
        else:
            en = en_base
            zh = zh_base
        candidate = _fit_headline_length(f"{en}\n{zh}", seed, min_len=min_len, max_len=max_len)
        normalized = _normalize_bilingual_headline(candidate)
        if not normalized:
            continue
        key = normalized.replace("\n", " ").lower()
        if key in seen:
            continue
        seen.add(key)
        variants.append(normalized)
        if len(variants) >= count:
            break
    return variants


def _fit_headline_length(candidate: str, seed: str, *, min_len: int, max_len: int) -> str:
    normalized = _normalize_bilingual_headline(candidate)
    if not normalized:
        return candidate
    en, zh = normalized.split("\n", 1)
    seed_en, _seed_zh = _headline_seed_lines(seed)
    length = _content_length(normalized)
    if length > max_len:
        en_words = en.split()
        while en_words and _content_length(" ".join(en_words) + "\n" + zh) > max_len:
            en_words.pop()
        en = " ".join(en_words) or "GasGx Onsite Power"
    elif length < min_len and seed_en:
        en = f"{en} {seed_en}".strip()
    return f"{en}\n{zh}"


def _headline_seed_lines(seed: str) -> tuple[str, str]:
    parts = [line.strip() for line in seed.replace("\r\n", "\n").split("\n") if line.strip()]
    if not parts:
        return "", ""
    seed_en = next((line for line in parts if LATIN_RE.search(line)), "")
    seed_zh = next((line for line in parts if CJK_RE.search(line)), "")
    return seed_en, seed_zh
