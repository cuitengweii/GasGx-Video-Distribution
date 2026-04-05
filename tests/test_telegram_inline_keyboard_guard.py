import json

from Collection.cybercar.cybercar_video_capture_and_publishing_module import telegram_command_worker as worker_impl
from cybercar.common import telegram_ui


def test_worker_inline_keyboard_switch_stays_enabled() -> None:
    assert worker_impl._DISABLE_CARD_INLINE_BUTTONS is False


def test_telegram_ui_inline_keyboard_switch_stays_enabled() -> None:
    assert telegram_ui._DISABLE_CARD_INLINE_BUTTONS is False


def test_telegram_ui_message_params_preserve_inline_keyboard() -> None:
    card = {
        "text": "inline keyboard guard",
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "国内即采即发", "callback_data": "immediate:domestic"},
                ]
            ]
        },
    }

    params = telegram_ui._message_params("6067625538", card)

    assert "reply_markup" in params
    payload = json.loads(str(params["reply_markup"]))
    assert payload["inline_keyboard"][0][0]["callback_data"] == "immediate:domestic"
