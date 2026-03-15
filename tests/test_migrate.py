from pathlib import Path

from cybercar import migrate


def test_migrate_skips_logs_and_telegram_runtime(tmp_path: Path) -> None:
    legacy_root = tmp_path / "legacy"
    target_root = tmp_path / "target"
    target_profiles = tmp_path / "profiles"
    (legacy_root / "2_Processed").mkdir(parents=True)
    (legacy_root / "2_Processed" / "video.mp4").write_text("ok", encoding="utf-8")
    (legacy_root / "runtime" / "logs").mkdir(parents=True)
    (legacy_root / "runtime" / "logs" / "a.log").write_text("x", encoding="utf-8")
    (legacy_root / "runtime" / "telegram_prefilter_queue.json").write_text("{}", encoding="utf-8")
    (legacy_root / "review_state.json").write_text("{}", encoding="utf-8")
    default_profile = tmp_path / "legacy_default_profile"
    wechat_profile = tmp_path / "legacy_wechat_profile"
    (default_profile / "Default").mkdir(parents=True)
    (wechat_profile / "Default").mkdir(parents=True)

    summary = migrate.migrate_legacy_assets(
        legacy_runtime_root=legacy_root,
        legacy_default_profile=default_profile,
        legacy_wechat_profile=wechat_profile,
        target_runtime_root=target_root,
        target_default_profile=target_profiles / "default",
        target_wechat_profile=target_profiles / "wechat",
    )

    assert (target_root / "2_Processed" / "video.mp4").exists()
    assert (target_root / "review_state.json").exists()
    assert not (target_root / "logs").exists()
    assert not any("telegram_prefilter_queue.json" in item for item in summary.copied)
