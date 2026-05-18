from __future__ import annotations

import json
import random
import re
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
    ("现场负载", "燃气机组稳定带载", "现场气源转成稳定电力"),
    ("余气利用", "把废气变成可用能源", "减少放散同时提升用能效率"),
    ("机组巡检", "关键参数一眼看懂", "从气源到发电保持连续输出"),
    ("矿场供能", "低成本电力支撑算力", "让边远场站也能稳定运行"),
    ("项目交付", "模块化部署更快落地", "机组、控制和负载协同启动"),
    ("运维对比", "少浪费，多输出", "把不稳定燃气变成可管理电力"),
]

TEXT_VARIANT_EXPANSION_TOPICS = [
    ("现场气源", "现场气源直接转电力", "把现场天然气转成可用电力"),
    ("移动电站", "模块化机组快速到场", "让偏远现场也能快速起电"),
    ("矿场电力", "低成本电力支撑算力", "用稳定电力支撑持续算力"),
    ("余气回收", "少放散 多产出", "把原本浪费的气变成收益"),
    ("并机供能", "多机组协同带载", "让负载波动也保持稳定输出"),
    ("项目落地", "设备 控制 负载一体交付", "从气源到用电现场快速闭环"),
    ("运维巡检", "关键参数清晰可见", "让现场运行状态一眼看懂"),
    ("海外能源", "靠近气源部署电力", "把电力建在能源最接近的地方"),
    ("工业负载", "燃气机组稳定带载", "为连续生产提供现场电力"),
    ("收益路径", "气 电 算力形成闭环", "让能源资产产生持续现金流"),
    ("快速部署", "集装箱方案缩短周期", "把复杂电站压缩成可交付模块"),
    ("低价电力", "一美分电力打开新场景", "用更低电力成本提升项目空间"),
]

TEXT_VARIANT_ANGLES = [
    ("真实现场", "现场实拍感更强"),
    ("交付视角", "突出项目落地"),
    ("运维视角", "强调稳定运行"),
    ("成本视角", "突出用电成本"),
    ("海外视角", "面向海外气源场景"),
    ("算力视角", "强调能源变现"),
    ("设备视角", "突出机组能力"),
    ("效率视角", "减少浪费提升输出"),
]

DEFAULT_HEADLINE_SEED = "Gas Engines That Turn Field Gas Into Power"
CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
LATIN_RE = re.compile(r"[A-Za-z]")
EXTRA_PROMPT_MAX_CHARS = 240
EXTRA_PROMPT_MAX_LINES = 4
EXTRA_PROMPT_URL_RE = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)


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
    return spark_copy or local_copy


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
    for index in range(max(1, count)):
        topic, slogan_tail, hud_line = topics[index % len(topics)]
        cycle = index // len(topics)
        title_base = titles[index % len(titles)]
        title = titles[index % len(titles)] if index == 0 else f"{topic}：{titles[index % len(titles)]}"
        slogan = slogans[index % len(slogans)] if index == 0 else slogan_tail
        variant_hud = base_hud if index == 0 and base_hud else [hud_line]
        if index == 0:
            title = title_base
            slogan = slogans[index % len(slogans)]
            opening_text = hud_line
        else:
            angle, angle_tail = TEXT_VARIANT_ANGLES[cycle % len(TEXT_VARIANT_ANGLES)]
            title = f"{topic}: {title_base}" if cycle == 0 else f"{topic}: {title_base} - {angle}"
            slogan = slogan_tail if cycle == 0 else f"{slogan_tail} | {angle_tail}"
            opening_text = hud_line if cycle == 0 else f"{hud_line}，{angle_tail}"
            variant_hud = [opening_text]
        if index > 0 and base_hud:
            variant_hud = [opening_text, *base_hud[:1]]
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
        title = str(item.get("title") or "").strip()
        slogan = str(item.get("slogan") or "").strip()
        hud_lines = [str(line).strip() for line in item.get("hud_lines") or [] if str(line).strip()]
        opening_text = str(item.get("opening_text") or "").strip()
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
    lines = [line.strip() for line in str(text or "").replace("\r\n", "\n").split("\n") if line.strip()]
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
        ("Turn Waste Gas Into Hashrate", "把废气变成算力收益"),
        ("Deploy Power Where Gas Lives", "气源在哪里 电力就部署到哪里"),
        ("Industrial Load, Onsite Energy", "工业负载配套就地能源"),
        ("Generator Sets for Remote Sites", "偏远站点也能稳定供电"),
        ("Lower Flaring, Higher Output", "更少放散 更高产出"),
        ("Gas Engines Built for Uptime", "燃气机组为持续运行而生"),
        ("From Gas Source to Digital Value", "从气源到数字价值闭环"),
    ]
    variants: list[str] = []
    seen: set[str] = set()

    for index in range(max(count * 2, len(topics))):
        en_base, zh_base = topics[index % len(topics)]
        suffix = index // len(topics)
        if suffix > 0:
            en = f"{en_base} {suffix + 1}"
            zh = f"{zh_base}{suffix + 1}"
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
