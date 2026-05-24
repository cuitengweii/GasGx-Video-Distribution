from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import Any, Iterable

from .models import VideoVariant
from .settings import ProjectSettings


@dataclass(slots=True)
class RandomVariationPlan:
    enabled: bool = False
    family: str = ""
    signature: str = ""
    seed: int = 0
    strength: float = 0.0
    filter_suffix: str = ""
    filters: list[str] = field(default_factory=list)
    history_hit: bool = False
    attempts: int = 0
    fallback_reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "family": self.family,
            "signature": self.signature,
            "seed": self.seed,
            "strength": self.strength,
            "filter_suffix": self.filter_suffix,
            "filters": list(self.filters),
            "history_hit": self.history_hit,
            "attempts": self.attempts,
            "fallback_reason": self.fallback_reason,
        }


def apply_random_variation_profiles(
    variants: list[VideoVariant],
    settings: ProjectSettings,
    history_features: Iterable[dict[str, Any]] | None = None,
    *,
    enabled: bool = False,
) -> list[RandomVariationPlan]:
    blocked_signatures = _history_variation_signatures(history_features)
    seen_signatures = set(blocked_signatures)
    plans: list[RandomVariationPlan] = []
    for variant in variants:
        plan = build_random_variation_plan(
            variant,
            settings,
            blocked_signatures=seen_signatures,
            enabled=enabled,
        )
        _apply_plan_to_variant(variant, plan)
        plans.append(plan)
        if plan.signature:
            seen_signatures.add(plan.signature)
    return plans


def build_random_variation_plan(
    variant: VideoVariant,
    settings: ProjectSettings,
    *,
    blocked_signatures: set[str] | None = None,
    enabled: bool = False,
) -> RandomVariationPlan:
    if not enabled:
        return RandomVariationPlan(enabled=False)

    seed = _stable_seed(variant, settings)
    rng = random.Random(seed)
    candidates = _build_candidates(variant, settings, rng)
    if not candidates:
        return RandomVariationPlan(enabled=False, seed=seed, fallback_reason="no_candidates")

    order = list(range(len(candidates)))
    rng.shuffle(order)
    blocked = set(str(item or "").strip() for item in (blocked_signatures or set()) if str(item or "").strip())

    history_hit = False
    attempts = 0
    for candidate_index in order:
        attempts += 1
        candidate = candidates[candidate_index]
        if candidate.signature and candidate.signature in blocked:
            history_hit = True
            continue
        candidate.attempts = attempts
        candidate.history_hit = history_hit
        return candidate

    fallback = candidates[order[0]]
    return RandomVariationPlan(
        enabled=False,
        seed=fallback.seed,
        history_hit=history_hit,
        attempts=attempts or 1,
        fallback_reason="history_saturated",
    )


def random_variation_payload(plan: RandomVariationPlan | None) -> dict[str, Any]:
    return plan.as_dict() if plan is not None else RandomVariationPlan().as_dict()


def _apply_plan_to_variant(variant: VideoVariant, plan: RandomVariationPlan) -> None:
    variant.random_variation_enabled = plan.enabled
    variant.random_variation_family = plan.family
    variant.random_variation_signature = plan.signature
    variant.random_variation_profile = plan.as_dict()


def _stable_seed(variant: VideoVariant, settings: ProjectSettings) -> int:
    payload = "|".join(
        [
            str(settings.project_name or ""),
            str(settings.target_width or 0),
            str(settings.target_height or 0),
            str(settings.target_fps or 0),
            str(variant.sequence_number or 0),
            str(variant.signature or ""),
            str(variant.title or ""),
            str(variant.slogan or ""),
            str(variant.bgm_offset_bucket or ""),
        ]
    )
    digest = hashlib.sha1(payload.encode("utf-8", errors="replace")).hexdigest()
    return int(digest[:16], 16)


def _history_variation_signatures(history_features: Iterable[dict[str, Any]] | None) -> set[str]:
    signatures: set[str] = set()
    for feature in history_features or []:
        if not isinstance(feature, dict):
            continue
        for key in ("random_variation_signature", "variation_signature"):
            signature = str(feature.get(key) or "").strip()
            if signature:
                signatures.add(signature)
        profile = feature.get("random_variation_profile")
        if isinstance(profile, dict):
            signature = str(profile.get("signature") or "").strip()
            if signature:
                signatures.add(signature)
    return signatures


def _build_candidates(variant: VideoVariant, settings: ProjectSettings, rng: random.Random) -> list[RandomVariationPlan]:
    base_strengths = (0.78, 0.92, 1.06)
    family_builders = (
        ("tone_soft", _build_tone_soft_filters),
        ("tone_warm", _build_tone_warm_filters),
        ("tone_cool", _build_tone_cool_filters),
        ("framing_shift", _build_framing_shift_filters),
        ("grain_light", _build_grain_light_filters),
        ("grain_vignette", _build_grain_vignette_filters),
        ("edge_clean", _build_edge_clean_filters),
        ("edge_soft", _build_edge_soft_filters),
        ("lift_fade", _build_lift_fade_filters),
    )
    candidates: list[RandomVariationPlan] = []
    for family_index, (family_name, builder) in enumerate(family_builders):
        for strength_index, base_strength in enumerate(base_strengths):
            strength = round(base_strength + rng.uniform(-0.04, 0.04), 3)
            filters = builder(strength, rng, variant, settings, family_index, strength_index)
            if not filters:
                continue
            filter_suffix = "," + ",".join(filters)
            signature = hashlib.sha1(
                "|".join([family_name, f"{strength:.3f}", filter_suffix]).encode("utf-8", errors="replace")
            ).hexdigest()[:24]
            candidates.append(
                RandomVariationPlan(
                    enabled=True,
                    family=family_name,
                    signature=signature,
                    seed=_candidate_seed(variant, settings, family_name, strength, family_index, strength_index),
                    strength=strength,
                    filter_suffix=filter_suffix,
                    filters=list(filters),
                )
            )
    return candidates


def _candidate_seed(
    variant: VideoVariant,
    settings: ProjectSettings,
    family: str,
    strength: float,
    family_index: int,
    strength_index: int,
) -> int:
    payload = "|".join(
        [
            str(settings.project_name or ""),
            str(variant.signature or ""),
            str(variant.sequence_number or 0),
            family,
            f"{strength:.3f}",
            str(family_index),
            str(strength_index),
        ]
    )
    return int(hashlib.sha1(payload.encode("utf-8", errors="replace")).hexdigest()[:16], 16)


def _build_tone_soft_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    contrast = 1.0 + 0.025 * strength + rng.uniform(-0.005, 0.006)
    brightness = -0.02 + 0.014 * strength + rng.uniform(-0.005, 0.005)
    saturation = 1.02 + 0.018 * strength + rng.uniform(-0.004, 0.005)
    return [
        _eq_filter(contrast, brightness, saturation),
    ]


def _build_tone_warm_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    contrast = 1.0 + 0.02 * strength + rng.uniform(-0.004, 0.005)
    brightness = -0.015 + 0.012 * strength + rng.uniform(-0.004, 0.004)
    saturation = 1.03 + 0.02 * strength + rng.uniform(-0.004, 0.005)
    hue = 2.0 + rng.uniform(-0.8, 1.2)
    hue_sat = 1.02 + 0.015 * strength + rng.uniform(-0.003, 0.004)
    return [
        _eq_filter(contrast, brightness, saturation),
        f"hue=h={hue:.2f}:s={hue_sat:.3f}",
    ]


def _build_tone_cool_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    contrast = 1.0 + 0.018 * strength + rng.uniform(-0.004, 0.005)
    brightness = -0.02 + 0.01 * strength + rng.uniform(-0.004, 0.004)
    saturation = 1.0 + 0.014 * strength + rng.uniform(-0.004, 0.005)
    hue = -2.4 + rng.uniform(-1.0, 0.9)
    hue_sat = 1.0 + 0.012 * strength + rng.uniform(-0.003, 0.004)
    return [
        _eq_filter(contrast, brightness, saturation),
        f"hue=h={hue:.2f}:s={hue_sat:.3f}",
    ]


def _build_grain_light_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    amount = int(round(2 + 5 * strength + rng.uniform(-0.5, 0.5)))
    amount = max(1, min(12, amount))
    return [
        _eq_filter(1.0 + rng.uniform(-0.01, 0.015), -0.015 + rng.uniform(-0.006, 0.004), 1.01 + rng.uniform(-0.003, 0.01)),
        f"noise=alls={amount}:allf=t+u",
    ]


def _build_framing_shift_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    zoom = 1.0 + 0.018 * strength + rng.uniform(0.004, 0.012)
    scaled_width = max(int(settings.target_width) + 2, int(round(settings.target_width * zoom)))
    scaled_height = max(int(settings.target_height) + 2, int(round(settings.target_height * zoom)))
    max_crop_x = max(0, scaled_width - int(settings.target_width))
    max_crop_y = max(0, scaled_height - int(settings.target_height))
    crop_x = 0 if max_crop_x == 0 else int(round(max_crop_x * rng.uniform(0.15, 0.85)))
    crop_y = 0 if max_crop_y == 0 else int(round(max_crop_y * rng.uniform(0.15, 0.85)))
    if max_crop_x > 0:
        crop_x = max(0, min(max_crop_x, crop_x))
    if max_crop_y > 0:
        crop_y = max(0, min(max_crop_y, crop_y))
    return [
        f"scale={scaled_width}:{scaled_height}:flags=lanczos",
        f"crop={int(settings.target_width)}:{int(settings.target_height)}:{crop_x}:{crop_y}",
        _eq_filter(1.0 + rng.uniform(-0.008, 0.012), -0.01 + rng.uniform(-0.004, 0.004), 1.0 + rng.uniform(-0.004, 0.01)),
    ]


def _build_grain_vignette_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    amount = int(round(3 + 5 * strength + rng.uniform(-0.5, 0.5)))
    amount = max(2, min(14, amount))
    angle = 4.0 + rng.uniform(-0.4, 0.6)
    return [
        _eq_filter(1.0 + rng.uniform(-0.008, 0.012), -0.018 + rng.uniform(-0.005, 0.004), 1.0 + rng.uniform(-0.003, 0.008)),
        f"noise=alls={amount}:allf=t+u",
        f"vignette=PI/{angle:.2f}",
    ]


def _build_edge_clean_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    sharp = 0.5 + 0.4 * strength + rng.uniform(-0.08, 0.08)
    return [
        f"unsharp=5:5:{sharp:.3f}:5:5:0.000",
        _eq_filter(1.01 + rng.uniform(-0.01, 0.01), -0.01 + rng.uniform(-0.004, 0.004), 1.02 + rng.uniform(-0.004, 0.006)),
    ]


def _build_edge_soft_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    sharp = 0.32 + 0.24 * strength + rng.uniform(-0.05, 0.05)
    amount = int(round(2 + 3 * strength + rng.uniform(-0.4, 0.5)))
    amount = max(1, min(10, amount))
    return [
        f"unsharp=5:5:{sharp:.3f}:5:5:0.000",
        f"noise=alls={amount}:allf=t+u",
    ]


def _build_lift_fade_filters(
    strength: float,
    rng: random.Random,
    _variant: VideoVariant,
    _settings: ProjectSettings,
    family_index: int,
    strength_index: int,
) -> list[str]:
    contrast = 0.98 + 0.02 * strength + rng.uniform(-0.005, 0.004)
    brightness = -0.03 + 0.018 * strength + rng.uniform(-0.005, 0.004)
    saturation = 1.0 + 0.012 * strength + rng.uniform(-0.003, 0.004)
    return [
        _eq_filter(contrast, brightness, saturation),
        f"vignette=PI/{(5.0 + strength * 1.1 + rng.uniform(-0.2, 0.3)):.2f}",
    ]


def _eq_filter(contrast: float, brightness: float, saturation: float) -> str:
    return f"eq=contrast={contrast:.3f}:brightness={brightness:.3f}:saturation={saturation:.3f}"
