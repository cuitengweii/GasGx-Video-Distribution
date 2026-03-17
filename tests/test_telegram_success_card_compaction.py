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


def test_build_publish_result_card_keeps_comment_reply_sections_on_success() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "点赞评论已完成",
            "subtitle": "已返回本次命中的短视频和评论内容，便于直接复查。",
            "sections": [
                {
                    "title": "结果",
                    "items": [
                        {"label": "命中视频", "value": "1"},
                        {"label": "实际回复", "value": "1"},
                    ],
                },
                {
                    "title": "回复 1",
                    "items": [
                        {"label": "短视频", "value": "test title"},
                        {"label": "原评论", "value": "comment body"},
                        {"label": "自动回复", "value": "reply body"},
                    ],
                },
            ],
        },
    )

    text = str(card["text"])
    assert "<b>结果</b>" in text
    assert "<b>回复 1</b>" in text
    assert "comment body" in text
    assert "reply body" in text
