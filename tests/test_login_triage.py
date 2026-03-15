from cybercar import login_triage as triage


def test_ready_snapshot_is_not_actionable() -> None:
    judgment = triage.judge_session_artifact(
        {
            "platform": "wechat",
            "status": "ready",
            "url": "https://channels.weixin.qq.com/platform/interaction/comment?isImageMode=0",
            "diagnostics": {
                "needs_login": False,
                "reason": "",
                "root_cause_hint": "local_detection_or_page_state_anomaly",
                "source": "page",
                "matched_marker": "",
                "current_url": "https://channels.weixin.qq.com/platform/interaction/comment?isImageMode=0",
                "expected_open_url": "https://channels.weixin.qq.com/platform/interaction/comment?isImageMode=0",
                "recent_session_ready": True,
                "page_text_excerpt": ""
            }
        }
    )

    assert judgment["actionable"] is False
    assert judgment["operator_bucket"] == "not_actionable_ready_snapshot"


def test_login_url_maps_to_upstream_session_expired() -> None:
    judgment = triage.judge_session_artifact(
        {
            "platform": "wechat",
            "status": "login_required",
            "diagnostics": {
                "needs_login": True,
                "reason": "login_url",
                "source": "page",
                "matched_marker": "微信扫码登录",
                "current_url": "https://channels.weixin.qq.com/login.html",
                "expected_open_url": "https://channels.weixin.qq.com/platform/post/create",
                "recent_session_ready": False,
                "page_text_excerpt": "微信扫码登录 视频号助手"
            }
        }
    )

    assert judgment["actionable"] is True
    assert judgment["operator_bucket"] == "upstream_session_expired"
