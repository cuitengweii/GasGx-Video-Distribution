from cybercar.common import telegram_ui


def test_build_telegram_card_prefers_log_and_ids_in_success_machine_info() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "人工关注",
                    "emoji": "🎯",
                    "items": [
                        {"label": "执行结果", "value": "成功"},
                        {"label": "目标平台", "value": "X"},
                    ],
                },
                {
                    "title": "机器信息",
                    "emoji": "🤖",
                    "items": [
                        {"label": "duration", "value": "12.3s"},
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
    assert "• <b>duration</b>：12.3s" not in text
