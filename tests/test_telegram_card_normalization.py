from cybercar.common import telegram_ui


def test_build_telegram_card_normalizes_section_titles_and_emojis() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {"title": "执行状态", "emoji": "x", "items": [{"label": "结果", "value": "失败"}]},
                {"title": "下一步", "emoji": "x", "items": ["请检查日志。"]},
                {"title": "任务日志", "emoji": "x", "items": [{"label": "日志", "value": "job.log"}]},
            ],
        },
    )

    text = str(card["text"])
    assert "<b>📌 执行摘要</b>" in text
    assert "<b>🛠️ 下一步</b>" in text
    assert "<b>🧾 任务日志</b>" in text
    assert "执行状态" not in text


def test_build_telegram_card_hides_redundant_success_platform_status() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "人工关注",
                    "items": [
                        {"label": "执行结果", "value": "成功"},
                        {"label": "平台摘要", "value": "✅ 小红书成功"},
                    ],
                },
                {
                    "title": "平台状态",
                    "items": [
                        {"label": "小红书", "value": "平台已确认发布成功"},
                    ],
                },
            ],
        },
    )

    text = str(card["text"])
    assert "<b>🧩 平台状态</b>" not in text
    assert "平台已确认发布成功" not in text


def test_build_telegram_card_hides_source_link_on_success_candidate_section() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "候选信息",
                    "items": [
                        {"label": "标题", "value": "Good morning"},
                        {"label": "原帖链接", "value": "https://x.com/example/status/1"},
                    ],
                }
            ],
        },
    )

    text = str(card["text"])
    assert "原帖链接" not in text
    assert "Good morning" in text


def test_build_telegram_card_deprioritizes_current_task_and_link_in_success_machine_info() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "人工关注",
                    "items": [
                        {"label": "执行结果", "value": "成功"},
                    ],
                },
                {
                    "title": "机器信息",
                    "items": [
                        {"label": "当前任务", "value": "collect_publish_latest"},
                        {"label": "当前链路", "value": "即采即发 > 图片 > 3条"},
                        {"label": "job_id", "value": "publish-42"},
                        {"label": "log", "value": "publish.log"},
                    ],
                },
            ],
        },
    )

    text = str(card["text"])
    assert "• <b>log</b>：publish.log" in text
    assert "• <b>job_id</b>：publish-42" in text
    assert "当前任务" not in text
    assert "当前链路" not in text
