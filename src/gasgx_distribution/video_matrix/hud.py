from __future__ import annotations

from dataclasses import dataclass

import requests

from .settings import ProjectSettings


@dataclass(slots=True)
class HudPayload:
    lines: list[str]
    used_live_data: bool


def build_hud_payload(settings: ProjectSettings, timeout: float = 4.0) -> HudPayload:
    if settings.hud_enable_live_data:
        try:
            btc_response = requests.get(settings.hud_sources["btc_usd"], timeout=timeout)
            btc_response.raise_for_status()
            btc_amount = btc_response.json()["data"]["amount"]

            hashrate_response = requests.get(settings.hud_sources["hashrate"], timeout=timeout)
            hashrate_response.raise_for_status()
            hashrate = float(hashrate_response.text.strip())

            return HudPayload(
                lines=[
                    f"BTC/USD {btc_amount}",
                    f"NET_HASHRATE {hashrate:.0f} GH/s",
                    settings.hud_fixed_formulas[0],
                ],
                used_live_data=True,
            )
        except Exception:
            pass

    return HudPayload(lines=list(settings.hud_fixed_formulas[:3]), used_live_data=False)
