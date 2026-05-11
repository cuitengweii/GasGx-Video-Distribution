from __future__ import annotations

import json
import subprocess
from pathlib import Path

from .models import VideoVariant
from .settings import ProjectSettings


LANGUAGE_LABELS = {
    "zh": "Chinese",
    "en": "English",
    "ru": "Russian",
}

PYTHON_WORKSPACE = Path(r"D:\code\Python")
SPARK_BASE = ["python", "-m", "Collection.other.xfyun_spark_cli_module.cli"]
TEXT_VARIANT_TOPICS = [
    ("现场负载", "燃气机组稳定带载", "现场气源转成稳定电力"),
    ("余气利用", "把放空气变成可用能源", "减少放散同时提升用能效率"),
    ("机组巡检", "关键参数一眼看清", "从气源到发电保持连续输出"),
    ("矿场供能", "低成本电力支撑算力", "让边远场站也能稳定运行"),
    ("项目交付", "模块化部署更快落地", "机组、控制和负载协同启动"),
    ("运维对比", "少浪费，多输出", "把不稳定燃气变成可管理电力"),
]


def build_marketing_copy(
    variant: VideoVariant,
    settings: ProjectSettings,
    language: str,
    template_copy: str,
    ending_follow_text: str = "",
) -> str:
    local_copy = _local_copy(variant, settings, template_copy, ending_follow_text)
    spark_copy = _try_spark_copy(variant, settings, language, ending_follow_text)
    return spark_copy or local_copy


def build_text_variants(
    settings: ProjectSettings,
    hud_lines: list[str],
    count: int,
    *,
    language: str = "zh",
    narrative_templates: list[dict] | None = None,
) -> list[dict[str, object]]:
    target_count = max(1, int(count or 1))
    local_variants = _fallback_text_variants(settings, hud_lines, target_count, source="template")
    spark_variants = _try_spark_text_variants(settings, hud_lines, target_count, language, narrative_templates)
    variants = _normalize_text_variants([*(spark_variants or []), *local_variants], target_count)
    return variants[:target_count]


def _try_spark_text_variants(
    settings: ProjectSettings,
    hud_lines: list[str],
    count: int,
    language: str,
    narrative_templates: list[dict] | None,
) -> list[dict[str, object]]:
    if not PYTHON_WORKSPACE.exists() or not str(settings.copy_mode or "").startswith("spark"):
        return []
    if not _spark_health_ready():
        return []
    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    narrative_names = ", ".join(str(item.get("name") or item.get("id") or "") for item in narrative_templates or [] if isinstance(item, dict))
    prompt = (
        f"Generate {count} distinct short-video text variants in {target_language} for GasGx.\n"
        "Return JSON only: {\"variants\":[{\"title\":\"...\",\"slogan\":\"...\",\"hud_lines\":[\"...\"],\"opening_text\":\"...\"}]}.\n"
        "Avoid repeating the same opening sentence. Keep each title and slogan concise.\n"
        f"Project: {settings.project_name}.\n"
        f"Narrative skeletons: {narrative_names or 'quick showcase, FAQ, contrast, step list, voiceover b-roll'}.\n"
        f"Existing HUD hints: {' | '.join(hud_lines)}."
    )
    code, payload = _call_spark(["chat", "--prompt", prompt, "--json", "--retry", "1"])
    if code != 0 or not payload.get("ok"):
        return []
    output = str(payload.get("data", {}).get("output", "") or "").strip()
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        return []
    raw_variants = parsed.get("variants") if isinstance(parsed, dict) else []
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


def _try_spark_copy(
    variant: VideoVariant,
    settings: ProjectSettings,
    language: str,
    ending_follow_text: str = "",
) -> str | None:
    if not PYTHON_WORKSPACE.exists():
        return None
    if not _spark_health_ready():
        return None
    target_language = LANGUAGE_LABELS.get(language, "Chinese")
    prompt = (
        f"Generate a concise social video publishing copy in {target_language}.\n"
        "Brand: GasGx.\n"
        f"Title: {variant.title}\n"
        f"Slogan: {variant.slogan}\n"
        f"Ending follow copy: {ending_follow_text.strip()}\n"
        f"HUD text: {' | '.join(variant.hud_lines)}\n"
        "Return only the final copy. Do not include a CTA label or ROI link. Keep it suitable for overseas short-video publishing."
    )
    code, payload = _call_spark(["chat", "--prompt", prompt, "--json", "--retry", "1"])
    if code != 0 or not payload.get("ok"):
        return None
    output = payload.get("data", {}).get("output", "")
    return output.strip() or None


def _spark_health_ready() -> bool:
    code, payload = _call_spark(["health", "--json"])
    data = payload.get("data", {})
    return code == 0 and bool(payload.get("ok")) and bool(data.get("ready", True))


def _call_spark(args: list[str]) -> tuple[int, dict]:
    try:
        process = subprocess.run(
            SPARK_BASE + args,
            cwd=str(PYTHON_WORKSPACE),
            capture_output=True,
            text=True,
            check=False,
            timeout=8,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, {"ok": False, "error": {"message": str(exc)}}
    payload: dict = {}
    stdout = process.stdout.strip()
    if stdout:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            payload = {"ok": False, "error": {"message": stdout}}
    return process.returncode, payload


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
    variants: list[dict[str, object]] = []
    for index in range(max(1, count)):
        topic, slogan_tail, hud_line = TEXT_VARIANT_TOPICS[index % len(TEXT_VARIANT_TOPICS)]
        title = titles[index % len(titles)] if index == 0 else f"{topic}：{titles[index % len(titles)]}"
        slogan = slogans[index % len(slogans)] if index == 0 else slogan_tail
        variant_hud = base_hud if index == 0 and base_hud else [hud_line]
        if index > 0 and base_hud:
            variant_hud = [hud_line, *base_hud[:1]]
        variants.append(
            {
                "id": f"{source}_{index + 1:02d}",
                "source": source,
                "title": title,
                "slogan": slogan,
                "hud_lines": variant_hud,
                "opening_text": hud_line,
            }
        )
    return variants


def _normalize_text_variants(raw_items: list[dict[str, object]], count: int) -> list[dict[str, object]]:
    variants: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in raw_items:
        title = str(item.get("title") or "").strip()
        slogan = str(item.get("slogan") or "").strip()
        hud_lines = [str(line).strip() for line in item.get("hud_lines") or [] if str(line).strip()]
        opening_text = str(item.get("opening_text") or "").strip()
        key = " ".join([title, slogan, opening_text, *hud_lines]).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
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
