from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_video_matrix_bgm_uses_local_library_with_visible_directory_hint() -> None:
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_styles.css").read_text(encoding="utf-8")

    assert "本地背景音乐" in app
    assert "暂无本地 MP3 文件" in app
    assert "local_bgm_dir" in app
    html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix.html").read_text(encoding="utf-8")
    assert "视频碎片素材目录" in html
    assert "手动上传素材" not in html
    assert "上传文字稿" not in html
    assert "文案参考资料" in html
    assert "transcriptFile" not in html
    assert "transcriptText" in html
    assert "generationConfirmModal" in html
    assert "点击查看" in html
    assert "bgmLibraryPopover" in html
    assert "materialCategories(data)" in app
    assert "settings.material_categories" in app
    assert "addMaterialCategory" in app
    assert "shortPath(data.source_dirs[category.id]" in app
    assert "const outputRoot = state.output_root || settings.output_root" in app
    assert '$("outputRoot").dataset.fullPath = outputRoot' in app
    assert "output_root: outputRootPath()" in app
    assert "toggleBgmLibraryPopover" in app
    assert "本地曲库列表" in app
    assert "downloadBgmToLibrary" in app
    assert "/api/video-matrix/bgm/download" in app
    assert "loadPixabayTracks" in app
    assert "/api/video-matrix/pixabay/industry" in app
    assert "Pixabay industry" in app
    assert "pixabay-track-list" in app
    assert "toggleBgmLibrarySize" in app
    assert 'panel.classList.toggle("modal", !isHidden)' in app
    assert "bgm-modal-open" in app
    assert 'panel.classList.add("hidden")' in app
    assert 'document.querySelector(".sidebar details summary")' in app
    assert "音频地址" in app
    assert "bgm-local-section" in app
    assert "bgm-pixabay-section" in app
    assert "生成时每次随机取 1 首" in app
    shell_css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")
    assert ".add-category-row" in css
    assert ".bgm-library-popover" in css
    assert ".dir-row code" in css
    assert ".embed-mode .sidebar" in css
    assert "overflow: hidden" in css
    assert "body.video-matrix-active" in shell_css
    assert "body.video-matrix-active .sidebar" in shell_css
    assert "position: fixed" in shell_css
    assert "margin-left: 0" in shell_css
    assert "height: 100vh" in shell_css
    app_shell = ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js"
    shell_app = app_shell.read_text(encoding="utf-8")
    shell_html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "index.html").read_text(encoding="utf-8")
    assert "function isWebhookOnlyAiRobot" in shell_app
    assert '["wecom", "dingtalk", "lark"].includes(platform)' in shell_app
    assert "function aiRobotWebhookHint" in shell_app
    assert "function aiRobotCallbackUrl" in shell_app
    assert "钉钉群机器人 Webhook 地址" in shell_app
    assert "飞书事件订阅 URL 验证" in shell_app
    assert "ai-lark-callback-url" in shell_app
    assert "/api/ai-robots/" in shell_app
    assert "webhook-simple-mode" in shell_app
    assert "lark-callback-mode" in shell_app
    assert 'const formHidden = !editingPlatform' in shell_app
    assert 'configPanel.hidden = formHidden' in shell_app
    assert "data-ai-toggle" in shell_app
    assert "通知开启" in shell_app
    assert "通知关闭" in shell_app
    assert "已配置" in shell_app
    assert "企业微信、钉钉、飞书填 Webhook 地址；Telegram 填 Bot Token" in shell_app
    assert 'class="ai-webhook-url-field"' in shell_html
    assert 'class="ai-test-message-field"' in shell_html
    assert 'id="ai-config-mode-card"' in shell_html
    assert 'id="ai-config-panel" hidden' in shell_html
    assert 'id="ai-lark-callback-field"' in shell_html
    assert 'id="ai-copy-lark-callback"' in shell_html
    assert 'id="ai-save-config-panel"' in shell_html
    assert 'id="ai-send-test-panel"' in shell_html
    assert ".notify-switch" in shell_css
    assert ".ai-config-mode-card" in shell_css
    assert ".ai-config-panel[hidden]" in shell_css
    assert ".ai-callback-field" in shell_css
    assert ".ai-config-actions" in shell_css
    assert ".ai-config-form.webhook-simple-mode .ai-advanced-field" in shell_css
    assert ".ai-config-form.webhook-simple-mode .ai-test-message-field" in shell_css
    assert "body.video-matrix-active" in shell_css
    assert "overflow: hidden" in shell_css
    assert "flex-direction: column" in shell_css
    assert "height: 100%" in shell_css
    assert "grid-template-columns: minmax(78px, 116px) minmax(0, 1fr) 54px" in css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" in css
    assert "智能分类轮换算法" in app
    assert "按视频碎片分类目录读取素材" in app
    assert '"Upload files", "手动上传"' not in app
    assert 'bgm_source: "Local library"' in app
    assert "bgmUpload" not in app
    assert "上传文件" not in app
    assert "bgmLibraryHint" in app
    assert 'bgm_library_id: ""' in app
    assert "renderComposition" in app
    assert "composition_sequence" in app
    assert "composition_customized" in app
    assert "defaultCompositionSequence" in app
    assert "scheduleStateSave" in app
    assert "const recentLimits = state.recent_limits || {}" in app
    assert '$("transcriptText").value = state.transcript_text || ""' in app
    assert 'transcript_text: $("transcriptText").value' in app
    assert "confirmGeneration(statePayload)" in app
    assert "generationConfirmHtml" in app
    assert "启用素材分类" in app
    assert "生成结构" in app
    assert "本次算法框架" in app
    assert "分析本地背景音乐节拍" in app
    assert "data-composition-category" in app
    assert "data-composition-duration" in app
    assert "data-composition-remove" in app
    assert "/api/video-matrix/bgm/" in app
    assert ".help-dot" in css
    assert ".bgm-local-item" in css
    assert ".bgm-download-box" in css
    assert ".pixabay-track-list" in css
    assert ".pixabay-track" in css
    assert ".bgm-library-popover.modal" in css
    assert "100vmax rgba(0, 0, 0, .64)" in css
    assert ".bgm-popover-head" in css
    assert ".bgm-local-section" in css
    assert ".bgm-pixabay-section" in css
    assert "scrollbar-width: thin" in css
    assert "::-webkit-scrollbar-thumb" in css
    assert "#saveState" in css
    assert 'classList.toggle("error-message", isError)' in app
    assert ".job-status-card.error" in css
    assert "#jobMessage.error-message" in css
    assert ".confirm-modal" in css
    assert ".confirm-panel" in css
    assert ".confirm-algorithm" in css
    assert "composition-panel" in html
    assert "videoDurationMax" in html
    assert "video_duration_max" in app
    assert "composition-rows" in css
    assert "composition-row" in css
