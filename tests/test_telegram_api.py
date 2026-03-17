from cybercar.common import telegram_api


class _FakeResponse:
    status_code = 200
    text = '{"ok": true, "result": {"message_id": 1}}'

    def json(self) -> dict[str, object]:
        return {"ok": True, "result": {"message_id": 1}}


class _FakeSession:
    def __init__(self) -> None:
        self.proxies: dict[str, str] = {}
        self.mounted: list[tuple[str, object]] = []
        self.calls: list[tuple[str, str, dict[str, object], int]] = []
        self.closed = False

    def mount(self, prefix: str, adapter: object) -> None:
        self.mounted.append((prefix, adapter))

    def post(self, url: str, data: dict[str, object], timeout: int) -> _FakeResponse:
        self.calls.append(("post", url, dict(data), timeout))
        return _FakeResponse()

    def get(self, url: str, params: dict[str, object], timeout: int) -> _FakeResponse:
        self.calls.append(("get", url, dict(params), timeout))
        return _FakeResponse()

    def close(self) -> None:
        self.closed = True


def test_call_telegram_api_uses_cybercar_proxy(monkeypatch) -> None:
    created: list[_FakeSession] = []

    def build_session() -> _FakeSession:
        session = _FakeSession()
        created.append(session)
        return session

    monkeypatch.setenv("CYBERCAR_PROXY", "http://127.0.0.1:33210")
    monkeypatch.delenv("CYBERCAR_USE_SYSTEM_PROXY", raising=False)
    telegram_api._SESSIONS.clear()
    monkeypatch.setattr(telegram_api.requests, "Session", build_session)

    response = telegram_api.call_telegram_api(
        bot_token="123456:abcdefghijklmnopqrstuvwxyzABCDE",
        method="setMyCommands",
        params={"commands": "[]"},
        timeout_seconds=20,
        use_post=True,
    )

    assert response["ok"] is True
    assert len(created) == 1
    assert created[0].proxies == {
        "http": "http://127.0.0.1:33210",
        "https": "http://127.0.0.1:33210",
    }
    assert created[0].calls[0][0] == "post"


def test_telegram_session_rebuilds_when_proxy_changes(monkeypatch) -> None:
    created: list[_FakeSession] = []

    def build_session() -> _FakeSession:
        session = _FakeSession()
        created.append(session)
        return session

    telegram_api._SESSIONS.clear()
    monkeypatch.setattr(telegram_api.requests, "Session", build_session)
    monkeypatch.setenv("CYBERCAR_PROXY", "http://127.0.0.1:33210")
    first = telegram_api._telegram_session(use_post=True)
    monkeypatch.setenv("CYBERCAR_PROXY", "http://127.0.0.1:33211")
    second = telegram_api._telegram_session(use_post=True)

    assert first is not second
    assert len(created) == 2
    assert created[0].proxies["https"] == "http://127.0.0.1:33210"
    assert created[1].proxies["https"] == "http://127.0.0.1:33211"
