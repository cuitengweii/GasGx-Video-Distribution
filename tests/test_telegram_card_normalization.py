import re

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
    assert "<b>📌 执行摘要</b>" not in text
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
    assert "内容已省略" in text


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
    assert "日志" in text
    assert "publish.log" not in text
    assert "publish-42" not in text
    assert "机器信息" not in text
    assert "当前任务" not in text
    assert "当前链路" not in text


def test_build_collect_start_card_keeps_only_minimum_sections() -> None:
    card = telegram_ui.build_telegram_card(
        "collect_start",
        {
            "status": "queued",
            "title": "候选已整理",
            "sections": [
                {"title": "任务概览", "items": [{"label": "目标平台", "value": "抖音 / 小红书"}, {"label": "候选目标", "value": "3条"}]},
                {"title": "候选信息", "items": [{"label": "标题", "value": "Good morning"}, {"label": "原帖链接", "value": "https://x.test/1"}]},
                {"title": "发布选项", "items": ["普通发布", "原创发布", "跳过本条"]},
                {"title": "原帖摘要", "items": ["这段应该被折掉"]},
                {"title": "机器信息", "items": [{"label": "当前任务", "value": "collect_publish_latest"}]},
            ],
        },
    )

    text = str(card["text"])
    assert "<b>📦 任务概览</b>" in text
    assert "<b>🧾 候选信息</b>" in text
    assert "<b>🛠️ 发布选项</b>" not in text
    assert "原帖链接" not in text
    assert "原帖摘要" not in text
    assert "机器信息" not in text


def test_build_failed_card_keeps_platform_status() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {"title": "失败原因", "items": [{"label": "原因", "value": "平台未登录"}]},
                {"title": "平台状态", "items": [{"label": "小红书", "value": "需要登录"}]},
                {"title": "任务日志", "items": [{"label": "日志", "value": "publish.log"}]},
            ],
        },
    )

    text = str(card["text"])
    assert "<b>⚠️ 失败原因</b>" in text
    assert "<b>🧩 平台状态</b>" in text
    assert "<b>🧾 任务日志</b>" in text


def test_build_telegram_card_compacts_candidate_source_subtitle() -> None:
    card = telegram_ui.build_telegram_card(
        "collect_start",
        {
            "status": "queued",
            "title": "候选已整理",
            "subtitle": "候选来源：X 搜索结果最近 3 条",
            "sections": [{"title": "任务概览", "items": [{"label": "候选目标", "value": "3条"}]}],
        },
    )

    assert "· 来源最近 3 条" in str(card["text"])


def test_build_telegram_card_compacts_processing_subtitle() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "running",
            "title": "发布处理中",
            "subtitle": "当前卡片已锁定，等待后台下载素材并分平台执行",
            "sections": [{"title": "执行摘要", "items": [{"label": "结果", "value": "处理中"}]}],
        },
    )

    assert "· 后台处理中" in str(card["text"])


def test_build_telegram_card_compacts_platform_result_subtitle() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "小红书发布已确认",
            "subtitle": "平台已返回最新处理结果",
            "sections": [{"title": "执行摘要", "items": [{"label": "结果", "value": "成功"}]}],
        },
    )

    assert "<i>✅ 已返回平台结果</i>" not in str(card["text"])


def test_build_telegram_card_compacts_platform_success_title() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "小红书发布已确认",
            "sections": [{"title": "执行摘要", "items": [{"label": "结果", "value": "成功"}]}],
        },
    )

    text = str(card["text"])
    assert text.startswith("<b>✅ 📝 小红书已确认</b>")


def test_build_telegram_card_compacts_collect_start_title() -> None:
    card = telegram_ui.build_telegram_card(
        "collect_start",
        {
            "status": "queued",
            "title": "图片即采即发候选已整理",
            "sections": [{"title": "任务概览", "items": [{"label": "候选目标", "value": "3条"}]}],
        },
    )

    assert str(card["text"]).startswith("<b>🕓 图片候选已整理</b>")


def test_build_telegram_card_compacts_immediate_summary_title() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "即采即发已全部完成",
            "sections": [{"title": "执行摘要", "items": [{"label": "结果", "value": "全部完成"}]}],
        },
    )

    assert str(card["text"]).startswith("<b>✅ 全部完成</b>")


def test_build_telegram_card_templates_login_failure_text() -> None:
    card = telegram_ui.build_telegram_card(
        "alert",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {"title": "失败原因", "items": [{"label": "原因", "value": "平台未登录"}]},
                {"title": "处理建议", "items": ["请先登录后重试。"]},
            ],
        },
    )

    text = str(card["text"])
    assert "• <b>🔐 原因</b>：登录失效" in text
    assert "• 🛠️ 去登录" in text


def test_build_telegram_card_templates_network_failure_text() -> None:
    card = telegram_ui.build_telegram_card(
        "alert",
        {
            "status": "failed",
            "title": "发送失败",
            "sections": [
                {"title": "失败原因", "items": [{"label": "原因", "value": "Telegram timeout while sending card"}]},
                {"title": "处理建议", "items": ["请稍后刷新重试。"]},
            ],
        },
    )

    text = str(card["text"])
    assert "• <b>🌐 原因</b>：网络超时" in text
    assert "• 🛠️ 刷新后看进度" in text


def test_build_telegram_card_templates_duplicate_skip_text() -> None:
    card = telegram_ui.build_telegram_card(
        "alert",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {"title": "结果说明", "items": [{"label": "说明", "value": "平台已有历史发布记录，本轮已自动跳过。"}]},
                {"title": "处理建议", "items": ["无需重复提交。"]},
            ],
        },
    )

    text = str(card["text"])
    assert "• <b>⏭️ 说明</b>：重复发布｜已跳过" in text
    assert "• 🛠️ 无需处理" in text


def test_build_telegram_home_strips_html_from_title() -> None:
    card = telegram_ui.build_telegram_home(
        "cybercar",
        {
            "title": "<b>🏠 CyberCar｜即采即发</b>",
            "subtitle": "当前配置：cybertruck",
            "sections": [],
        },
    )

    text = str(card["text"])
    assert "<b><b>" not in text
    assert "&lt;b&gt;" not in text
    assert "<b>🏠 CyberCar｜🏠 CyberCar｜即采即发</b>" not in text
    assert "<b>🏠 即采即发</b>" in text
    assert "当前配置" not in text


def test_build_telegram_home_strips_html_from_subtitle_and_section_content() -> None:
    card = telegram_ui.build_telegram_home(
        "cybercar",
        {
            "title": "<b>🏠 即采即发</b>",
            "subtitle": "<i>当前配置：cybertruck｜视频/图片双流程</i>",
            "sections": [
                {
                    "title": "<b>执行说明</b>",
                    "items": ["<b>视频即采即发：</b>只扫 X 视频帖。", {"label": "<b>说明</b>", "value": "<i>两条流程互相独立。</i>"}],
                }
            ],
        },
    )

    text = str(card["text"])
    assert "<b><b>" not in text
    assert "<i><i>" not in text
    assert "&lt;b&gt;" not in text
    assert "&lt;i&gt;" not in text
    assert "当前配置" not in text
    assert "执行说明" not in text
    assert "视频即采即发：只扫 X 视频帖。" not in text
    assert "• <b>说明</b>：两条流程互相独立。" not in text


def test_build_telegram_card_splits_prefixed_title_into_title_and_subtitle() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "【即采即发/图片/全部平台】快手已确认",
            "subtitle": "平台发布成功",
            "sections": [{"title": "执行摘要", "items": [{"label": "结果", "value": "成功"}]}],
        },
    )

    text = str(card["text"])
    lines = text.splitlines()
    assert lines[0] == "<b>✅ ⚡ 快手已确认</b>"
    assert all("即采即发 / 图片 / 全部平台" not in line for line in lines)
    assert "<b>📌 执行摘要</b>" not in text


def test_build_telegram_card_hides_candidate_info_when_platform_status_exists() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {"title": "执行摘要", "items": [{"label": "成功平台", "value": "2"}]},
                {"title": "候选信息", "items": [{"label": "标题", "value": "candidate"}]},
                {"title": "平台状态", "items": [{"label": "小红书", "value": "平台已确认发布成功"}]},
                {"title": "机器信息", "items": [{"label": "当前任务", "value": "collect_publish_latest"}]},
            ],
        },
    )

    text = str(card["text"])
    assert "<b>📌 执行摘要</b>" not in text
    assert "<b>🤖 机器信息</b>" not in text
    assert "<b>🧩 平台状态</b>" in text
    assert "<b>🧾 候选信息</b>" not in text


def test_build_telegram_card_orders_summary_counts_for_platform_results() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {
                    "title": "执行摘要",
                    "items": [
                        {"label": "目标平台", "value": "3"},
                        {"label": "失败平台", "value": "1"},
                        {"label": "成功平台", "value": "2"},
                    ],
                }
            ],
        },
    )

    text = str(card["text"])
    assert "<b>📌 执行摘要</b>" not in text
    assert "成功平台" not in text
    assert "失败平台" not in text
    assert "目标平台" not in text


def test_build_telegram_card_places_platform_title_on_dedicated_header_line() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "【即采即发/图片/全部平台】小红书已确认",
            "subtitle": "平台发布成功",
            "sections": [{"title": "执行摘要", "items": [{"label": "结果", "value": "成功"}]}],
        },
    )

    text = str(card["text"])
    lines = text.splitlines()
    assert lines[0] == "<b>✅ 📝 小红书已确认</b>"
    assert all("即采即发 / 图片 / 全部平台" not in line for line in lines)
    assert "<b>📌 执行摘要</b>" not in text


def test_build_telegram_card_splits_prefixed_badge_title_into_stacked_platform_header() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "📝 【即采即发/图片/全部平台】抖音已确认",
            "subtitle": "平台发布成功",
            "sections": [{"title": "执行摘要", "items": [{"label": "结果", "value": "成功"}]}],
        },
    )

    lines = str(card["text"]).splitlines()
    assert lines[0] == "<b>✅ 🎵 抖音已确认</b>"
    assert all("即采即发 / 图片 / 全部平台" not in line for line in lines)
    assert "<b>📌 执行摘要</b>" not in str(card["text"])


def test_build_telegram_card_normalizes_platform_status_items_without_platform_emojis() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {
                    "title": "平台状态",
                    "items": [
                        {"label": "📝小红书", "value": "平台已确认发布成功"},
                        {"label": "🎵抖音", "value": "平台处理失败；原因：Could not find file input on douyin page；建议：查看该平台日志后重试"},
                        {"label": "⚡快手", "value": "需要登录"},
                    ],
                }
            ],
        },
    )

    text = str(card["text"])
    assert "• <b>快手</b>：❌ 需要登录" in text
    assert "• <b>抖音</b>：❌ 发布失败｜错误码" in text
    assert "• <b>小红书</b>：✅ 发布成功" in text
    assert text.index("• <b>快手</b>") < text.index("• <b>抖音</b>")
    assert text.index("• <b>抖音</b>") < text.index("• <b>小红书</b>")


def test_build_telegram_card_uses_neutral_overview_header_for_immediate_platform_summary() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "即采即发部分平台已完成",
            "subtitle": "部分平台待处理",
            "sections": [
                {
                    "title": "平台状态",
                    "items": [
                        {"label": "视频号", "value": "需要登录"},
                        {"label": "抖音", "value": "平台处理失败；原因：collection not confirmed"},
                        {"label": "小红书", "value": "平台已确认发布成功"},
                    ],
                },
                {
                    "title": "执行摘要",
                    "items": [
                        {"label": "成功平台", "value": "1"},
                        {"label": "失败平台", "value": "2"},
                        {"label": "目标平台", "value": "3"},
                    ],
                },
            ],
        },
    )

    text = str(card["text"])
    assert text.startswith("<b>📌 平台概览</b>")
    assert "• <b>视频号</b>：❌ 需要登录" in text
    assert "• <b>抖音</b>：❌ 发布失败｜错误码" in text


def test_build_telegram_card_removes_ascii_letters_from_visible_text() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "CyberCar publish failed",
            "subtitle": "worker running on profile x_to_cn",
            "sections": [
                {"title": "平台状态", "items": [{"label": "douyin", "value": "Could not find file input on douyin page"}]},
                {"title": "候选信息", "items": [{"label": "title", "value": "Good morning"}]},
            ],
        },
    )
    text = str(card["text"])
    visible = re.sub(r"<[^>]+>", "", text)
    assert re.search(r"[A-Za-z]", visible) is None


def test_build_telegram_card_limits_and_dedupes_platform_status_items() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {
                    "title": "平台状态",
                    "items": [
                        {"label": "抖音", "value": "发布失败，错误码:ERR_UPLOAD_TIMEOUT"},
                        {"label": "抖音", "value": "发布失败，错误码:ERR_UPLOAD_TIMEOUT"},
                        {"label": "小红书", "value": "平台已确认发布成功"},
                        {"label": "快手", "value": "需要登录"},
                        {"label": "视频号", "value": "发布中"},
                        {"label": "B站", "value": "已排队"},
                        {"label": "微博", "value": "待确认"},
                    ],
                }
            ],
        },
    )

    text = str(card["text"])
    assert text.count("• <b>") == 5
    assert "错误码：ERR_UPLOAD_TIMEOUT" in text or "错误码:ERR_UPLOAD_TIMEOUT" in text
    assert "• <b>B站</b>：已排队" in text
    assert "• <b>站</b>：🕓 已排队" not in text


def test_build_telegram_card_platform_failure_uses_error_code_not_log_name() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {
                    "title": "平台状态",
                    "items": [
                        {
                            "label": "抖音",
                            "value": "发布失败，日志:immediate_publish_douyin_20260405_160000.log，错误码:ERR_UPLOAD_TIMEOUT",
                        }
                    ],
                }
            ],
        },
    )

    text = str(card["text"])
    assert "错误码：ERR_UPLOAD_TIMEOUT" in text or "错误码:ERR_UPLOAD_TIMEOUT" in text
    assert "日志:immediate_publish_douyin_20260405_160000.log" not in text
    assert "日志：immediate_publish_douyin_20260405_160000.log" not in text


def test_localize_card_text_compacts_task_identifier_timestamp_to_hour_minute() -> None:
    text = telegram_ui._localize_card_text("collect_publish_latest|video|3|20260405_160321")

    assert "20260405" not in text
    assert "3｜16" not in text
    assert "3|16" not in text
    assert re.search(r"16[:：]03", text)


def test_build_telegram_card_sorts_candidate_section_and_uses_readable_link_label() -> None:
    long_title = "这是一个用于验证移动端卡片长文本截断效果的候选标题" * 3
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {
                    "title": "候选信息",
                    "items": [
                        {"label": "原帖链接", "value": "https://x.com/example/status/123"},
                        {"label": "标题", "value": long_title},
                        {"label": "平台", "value": "抖音 / 小红书 / 快手"},
                    ],
                }
            ],
        },
    )

    text = str(card["text"])
    assert text.index("• <b>标题</b>") < text.index("• <b>平台</b>")
    assert text.index("• <b>平台</b>") < text.index("• <b>原帖链接</b>")
    assert "查看原帖" in text
    assert "…" in text


def test_build_telegram_card_filters_garbled_text_and_unifies_punctuation() -> None:
    card = telegram_ui.build_telegram_card(
        "alert",
        {
            "status": "failed",
            "title": "发送失败",
            "subtitle": "网络 timeout, please retry!",
            "sections": [
                {"title": "失败原因", "items": [{"label": "原因", "value": "???????"}]},
                {"title": "处理建议", "items": ["retry now!"]},
                {"title": "结果说明", "items": [{"label": "说明", "value": "原因: network timeout; 建议: refresh"}]},
            ],
        },
    )

    text = str(card["text"])
    assert "????" not in text
    assert "???" not in text
    assert "：" in text
    assert "；" in text or "｜" in text


def test_build_telegram_card_strips_candidate_platform_emojis() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {
                    "title": "候选信息",
                    "items": [
                        {"label": "平台", "value": "📱视频号 / 🎵抖音 / 📝小红书 / ⚡快手 / 📺B站"},
                    ],
                }
            ],
        },
    )
    text = str(card["text"])
    assert "视频号 / 抖音 / 小红书 / 快手 / 哔哩哔哩" in text
    assert "📱" not in text
    assert "🎵" not in text
    assert "📝小红书" not in text
    assert "⚡快手" not in text
    assert "📺" not in text


def test_build_telegram_card_removes_operation_record_section() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {
                    "title": "候选信息",
                    "items": [{"label": "标题", "value": "示例标题"}],
                },
                {
                    "title": "操作记录",
                    "items": [{"label": "操作人", "value": "@"}, {"label": "时间", "value": "2026-04-05 10:00:00"}],
                },
            ],
        },
    )

    text = str(card["text"])
    assert "操作记录" not in text
    assert "操作人" not in text
    assert "2026-04-05" not in text
