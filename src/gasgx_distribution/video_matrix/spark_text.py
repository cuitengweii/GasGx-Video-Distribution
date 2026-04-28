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


def build_marketing_copy(
    variant: VideoVariant,
    settings: ProjectSettings,
    transcript_text: str,
    language: str,
    template_copy: str,
) -> str:
    local_copy = _local_copy(variant, settings, transcript_text, template_copy)
    spark_copy = _try_spark_copy(variant, settings, transcript_text, language)
    return spark_copy or local_copy


def _try_spark_copy(
    variant: VideoVariant,
    settings: ProjectSettings,
    transcript_text: str,
    language: str,
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
        f"ROI link: {settings.website_url}\n"
        f"HUD data: {' | '.join(variant.hud_lines)}\n"
        f"Transcript:\n{transcript_text.strip() or 'No uploaded transcript.'}\n\n"
        "Return only the final copy. Keep it suitable for overseas short-video publishing."
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
    process = subprocess.run(
        SPARK_BASE + args,
        cwd=str(PYTHON_WORKSPACE),
        capture_output=True,
        text=True,
        check=False,
    )
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
    transcript_text: str,
    template_copy: str,
) -> str:
    return template_copy.format(
        title=variant.title,
        slogan=variant.slogan,
        sequence_number=f"{variant.sequence_number:02d}",
        website_url=settings.website_url,
        hud_summary="\n".join(variant.hud_lines),
        transcript=(transcript_text.strip() or "No uploaded transcript."),
    )
