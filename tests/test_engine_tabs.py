from cybercar import engine


class _FakeTab:
    def __init__(self, tab_id: str, url: str) -> None:
        self.tab_id = tab_id
        self.url = url
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakePage(_FakeTab):
    def __init__(self, tab_id: str, url: str, tabs: list[_FakeTab]) -> None:
        super().__init__(tab_id, url)
        self._tabs = tabs
        self.activated: list[str] = []
        self.closed_tab_ids: list[str] = []

    def get_tabs(self, tab_type: str = "page") -> list[_FakeTab]:
        return list(self._tabs)

    def activate_tab(self, tab: object) -> None:
        tab_id = str(getattr(tab, "tab_id", tab))
        self.activated.append(tab_id)

    def close_tabs(self, tabs_or_ids: object, others: bool = False) -> None:
        raw_items = tabs_or_ids if isinstance(tabs_or_ids, list) else [tabs_or_ids]
        target_ids = {str(getattr(item, "tab_id", item)) for item in raw_items}
        for tab in self._tabs:
            if tab.tab_id in target_ids:
                tab.closed = True
                self.closed_tab_ids.append(tab.tab_id)


def test_stabilize_platform_session_page_prefers_business_tab_and_closes_stale_login_tabs() -> None:
    login_tab = _FakeTab("tab-login", "https://channels.weixin.qq.com/login.html")
    business_tab = _FakeTab("tab-list", "https://channels.weixin.qq.com/platform/post/list")
    page = _FakePage(login_tab.tab_id, login_tab.url, [login_tab, business_tab])

    chosen = engine._stabilize_platform_session_page(
        page,
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        close_stale_login_tabs=True,
    )

    assert getattr(chosen, "tab_id", "") == "tab-list"
    assert page.activated == ["tab-list"]
    assert login_tab.closed is True
    assert business_tab.closed is False


def test_stabilize_platform_session_page_keeps_current_tab_when_no_business_tab_exists() -> None:
    login_tab = _FakeTab("tab-login", "https://channels.weixin.qq.com/login.html")
    page = _FakePage(login_tab.tab_id, login_tab.url, [login_tab])

    chosen = engine._stabilize_platform_session_page(
        page,
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        close_stale_login_tabs=True,
    )

    assert chosen is page
    assert page.activated == []
    assert login_tab.closed is False
