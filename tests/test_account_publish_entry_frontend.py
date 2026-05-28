from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_account_matrix_domestic_publish_entry_morphs_from_open_to_publish() -> None:
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")

    assert "const ACCOUNT_PLATFORM_PUBLISH_DELAY_MS = 15000;" in app
    assert "const accountPlatformPublishReadyAtByKey = new Map();" in app
    assert "const accountPlatformPublishInFlightByKey = new Map();" in app
    assert "const accountDomesticOpenInFlightByAccountId = new Map();" in app
    assert "const accountDomesticOpenResultByAccountId = new Map();" in app
    assert "function accountPlatformPublishKey(accountId, platform)" in app
    assert 'function accountPlatformPublishState(accountId, platform, loginStatus = "")' in app
    assert 'function accountPlatformPublishButtonMeta(accountId, platform, loginStatus = "")' in app
    assert 'function accountPlatformPublishButtonMarkup(accountId, platform, loginStatus = "")' in app
    assert "function accountDomesticOpenKey(accountId)" in app
    assert "function accountDomesticOpenPlatforms(account)" in app
    assert "function accountDomesticOpenState(account)" in app
    assert "function accountDomesticOpenButtonMeta(account)" in app
    assert "function accountDomesticOpenButtonMarkup(account)" in app
    assert "async function handleAccountDomesticOpen(domesticOpenButton)" in app
    assert "function markAccountDomesticOpen(accountId)" in app
    assert "function clearAccountDomesticOpenState(accountId)" in app
    assert "function updateAccountDomesticOpenButtons()" in app
    assert "function startAccountPlatformPublishCountdown(accountId, platform)" in app
    assert "function markAccountPlatformPublishing(accountId, platform)" in app
    assert "function clearAccountPlatformPublishState(accountId, platform)" in app
    assert "function updateAccountPlatformPublishButtons()" in app
    assert "accountPlatformPublishButtonMarkup(p.account_id, p.platform, p.login_status)" in app
    assert 'data-account-platform-action=' in app
    assert 'data-account-platform-action-state=' in app
    assert 'data-account-platform-login-status=' in app
    assert 'data-account-domestic-open=' in app
    assert 'data-account-api-id=' in app
    assert 'data-account-domestic-open-state=' in app
    assert 'data-open="${escapeHtml(`${key}:domestic`)}"' in app
    assert 'data-account-domestic-publish=' in app
    assert 'data-account-domestic-publish-state=' in app
    assert 'data-no-global-loading="1"' in app
    assert 'data-account-domestic-open-bound="1"' not in app
    assert "\u767b\u5f55 / \u6253\u5f00\u6d4f\u89c8\u5668" in app
    assert "\u76f4\u63a5\u53d1\u5e03" in app
    assert "\u4e00\u952e\u6253\u5f00\u56fd\u5185\u5e73\u53f0" in app
    assert "\u6253\u5f00\u4e2d" in app
    assert "\u5df2\u6253\u5f00" in app
    assert "\u90e8\u5206\u5931\u8d25" in app
    assert "\u4e00\u952e\u53d1\u5e03\u56fd\u5185\u5e73\u53f0" in app
    assert "\u76f4\u63a5\u53d1\u5e03" in app
    assert "\u8bf7\u5148\u786e\u8ba4\u767b\u5f55" not in app
    assert 'const probe = await api(`/api/accounts/${accountApiId(account)}/platforms/${platform}/login-status`, { method: "POST" });' not in app
    assert 'setButtonLoading(platformAction, meta.mode === "open" ? "打开中" : "检查中")' not in app
    assert "\u53d1\u5e03\u4e2d" in app
    assert "\u53d1\u5e03\u4e2d" in app
    assert "\u5df2\u53d1\u8d77" in app
    assert '/api/accounts/${accountApiId(account)}/platforms/${platform}/open-browser' in app
    assert '/api/accounts/${accountApiId(account)}/platforms/${platform}/login-status' in app
    assert '/api/accounts/${accountApiId(account)}/platforms/${platform}/emergency-publish' in app
    assert '/api/accounts/${accountApiIdValue}/platforms/domestic/open-browser' in app
    assert '/api/accounts/${accountApiId(account)}/platforms/domestic/emergency-publish' in app
    assert "[data-account-platform-action]" in app
    assert "[data-account-domestic-open]" in app
    assert "[data-account-domestic-publish]" in app
    assert ".platform-wechat-action" in css
    assert ".account-platform-summary-actions" in css
    assert ".platform-action-copy" in css
    assert ".platform-action-main" in css
    assert ".platform-action-hint" in css
    assert ".platform-wechat-action.is-open" in css
    assert ".platform-wechat-action.is-countdown" in css
    assert ".platform-wechat-action.is-ready" in css
    assert ".platform-wechat-action.is-opened" in css
    assert ".platform-wechat-action.is-partial" in css
    assert ".platform-wechat-action.is-publishing" in css
