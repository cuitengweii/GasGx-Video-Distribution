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
    assert "TEXT视频文字设置" not in html
    assert "主标题 / 视频口号" not in html
    assert "副标题 / 视频标题" not in html
    assert "首屏 CTA" not in html
    assert "结尾关注提醒" not in html
    assert "视频 HUD 文本" not in html
    assert "文案参考资料" not in html
    assert '<input id="headline" type="hidden" />' in html
    assert html.index('class="cover-workbench"') < html.index('class="video-template-workbench"') < html.index('class="side-editor"')
    assert "transcriptFile" not in html
    assert "transcriptText" in html
    assert "generationConfirmModal" in html
    assert "点击查看" in html
    assert "openBgmDir" in html
    assert "bgmLibraryPopover" in html
    assert "materialCategories(data)" in app
    assert "settings.material_categories" in app
    assert "addMaterialCategory" in app
    assert "shortPath(data.source_dirs[category.id]" in app
    assert "const outputRoot = state.output_root || settings.output_root" in app
    assert '$("outputRoot").dataset.fullPath = outputRoot' in app
    assert "output_root: outputRootPath()" in app
    assert "toggleBgmLibraryPopover" in app
    assert '$("openBgmDir").onclick = () => openFolder(bgmLibraryState.directory)' in app
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
    assert "6 条未读" in shell_html
    assert "视频生成完成" in shell_html
    assert "视频号发布失败" in shell_html
    assert "素材分类不足" in shell_html
    assert "企业微信群机器人已发送日报" in shell_html
    assert "发布前审核等待确认" in shell_html
    assert "当前没有新的系统提醒" not in shell_html
    assert ".notification-list" in shell_css
    assert ".notification-card.danger" in shell_css
    assert ".notification-card.warning" in shell_css
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
    assert "使用帮助知识库" in shell_html
    assert "help-layout" in shell_html
    assert "help-category-section" in shell_html
    assert "docs/help/VIDEO_GENERATION_WORKBENCH.md" in shell_html
    assert "docs/help/ACCOUNT_MATRIX.md" in shell_html
    assert "docs/help/SYSTEM_SETTINGS.md" in shell_html
    assert "docs/help/DEVELOPER_VIDEO_GENERATION_ALGORITHM.md" in shell_html
    assert ".notify-switch" in shell_css
    assert ".ai-config-mode-card" in shell_css
    assert ".ai-config-panel[hidden]" in shell_css
    assert ".ai-callback-field" in shell_css
    assert ".ai-config-actions" in shell_css
    assert ".ai-config-form.webhook-simple-mode .ai-advanced-field" in shell_css
    assert ".ai-config-form.webhook-simple-mode .ai-test-message-field" in shell_css
    assert ".help-layout" in shell_css
    assert ".help-hero-panel" in shell_css
    assert ".help-doc-grid" in shell_css
    assert ".help-doc-card" in shell_css
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
    assert "scheduleVideoTemplateSave" in app
    assert "scheduleCoverTemplateSave" in app
    assert "saveTemplateSelection" in app
    assert "cloneVideoTemplate" in app
    assert "nextTemplateCloneId" in app
    assert "saveCoverAsNewTemplate" in app
    assert "nextCoverTemplateMeta" in app
    assert '<button type="button" id="saveCover">保存为新模板</button>' in app
    assert "cover_template_${serial}" in app
    assert "第一屏封面模板 ${serial}" in app
    assert "selectVideoTemplate" in app
    assert "title_bg_height" in app
    assert "title_bg_opacity" in app
    assert 'id="cloneVideoTemplate"' in app
    assert '<button type="button" id="saveVideoTemplate">保存当前</button>' in app
    assert 'button.textContent = "保存中..."' in app
    assert "切换正文模板..." in app
    assert "selectCoverTemplate" in app
    assert "切换第一屏模板..." in app
    assert "coverTemplateBackgrounds" not in html
    assert "renderCoverTemplateBackgrounds" not in app
    assert "coverVisualToolbarHtml" in app
    assert "gasgx-cover-template-command" in app
    assert "gasgx-cover-template-update" in app
    assert "gasgx-cover-template-text-update" in app
    assert "applyCoverTemplateUpdates" in app
    assert "applyCoverTextUpdates" in app
    assert "可视化调整" in app
    assert "当前模板调整" not in app
    assert "color-swatch-button" in app
    assert "color-picker-icon" in app
    assert "color-current-dot" in app
    assert 'data-value="left" title="左对齐">左齐' in app
    assert 'data-value="center" title="居中对齐">居中' in app
    assert 'data-value="right" title="右对齐">右齐' in app
    assert "show_template_mask: false" in app
    assert "show_template_mask: true" in app
    assert "coverMaskModeOptions" in app
    assert "上渐变蒙版" in app
    assert "下渐变蒙版" in app
    assert "全蒙版" in app
    assert 'data-key="mask_color"' in app
    assert 'data-key="tile_titles_text"' in app
    assert "selectedCoverModelImageUrl" not in app
    assert "切换封面背景..." not in app
    assert "background_image_urls: modelImages.map" in app
    assert "background_image_url: modelImages[0]?.url || \"\"" in app
    assert "await selectVideoTemplate(card.dataset.id)" in app
    assert "正文模板自动保存失败" in app
    assert "第一屏模板自动保存失败" in app
    assert "const recentLimits = state.recent_limits || settings.recent_limits || {}" in app
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
    assert "#openBgmDir" in css
    assert ".template-actions" in css
    assert "grid-template-columns: minmax(0, 1fr) 124px" in css
    assert "cursor:pointer" in css
    assert "videoTemplateCaption" not in html
    assert "videoTemplateCaption" not in app
    assert html.index('id="videoTemplateBackgrounds"') < html.index('class="template-preview-editor"') < html.index('class="video-template-picker"') < html.index('id="videoTemplateGallery"') < html.index('id="videoTemplateSelector"')
    assert ".video-template-picker" in css
    assert "height: calc(var(--preview-phone-height) + 36px)" in css
    assert "minmax(340px, 380px)" in css
    assert "width: min(100%, 380px)" in css
    assert "grid-template-columns: repeat(auto-fill, 154px)" in css
    assert "justify-self: center" in css
    assert "font-size: 13px" in css
    assert ".color-swatch-button" in css
    assert ".color-picker-icon" in css
    assert ".color-current-dot" in css
    assert "margin: 8px 0 14px" in css
    assert "inset: 0" in css
    assert "pointer-events: auto" in css
    assert "coverSelector" not in html
    assert "#coverForm" in css
    assert ".visual-editor-hint" in css
    assert ".cover-template-actions" in css
    assert "max-height: none" in css
    assert "overflow: visible" in css
    assert "background: rgba(7, 13, 9, .42)" in css
    assert "#videoTemplateForm .range-control" in css
    assert "padding: 6px 10px" in css
    assert "min-height: 28px" in css
    assert 'classList.toggle("error-message", isError)' in app
    assert ".job-status-card.error" in css
    assert "#jobMessage.error-message" in css
    assert ".confirm-modal" in css
    assert ".confirm-panel" in css
    assert ".confirm-algorithm" in css
    assert "composition-panel" in css
    assert "videoDurationMax" in html
    assert "video_duration_max" in app
    assert "composition-rows" in css
    assert "composition-row" in css


def test_video_matrix_preview_keeps_video_audio_available() -> None:
    preview_html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_preview.html").read_text(encoding="utf-8")
    video_tag = preview_html[preview_html.index("<video"):preview_html.index("</video>")]
    assert "muted" not in video_tag
    assert "video.muted = false" in preview_html
    assert "video.volume = 1" in preview_html
    assert "soundToggle.addEventListener" in preview_html
    assert ".sound-toggle" in preview_html
    assert "templateSloganBar" in preview_html
    assert "templateTitleBar" in preview_html
    assert "vm-template-text-bar" in preview_html
    assert "slogan_bg_height" in preview_html
    assert "title_bg_height" in preview_html
    assert "isBackgroundTarget" in preview_html
    assert "coverProfileShell" in preview_html
    assert "cover-profile-mode" in preview_html
    assert "cover-profile-grid" in preview_html
    assert "GasGx | 燃气发电解决方案" in preview_html
    assert "gasgx-cover-template-command" in preview_html
    assert "postCoverTemplateUpdates" in preview_html
    assert "coverTargetOffsetKey" in preview_html
    assert "profile_cta_text" in preview_html
    assert "cover-tile-like" in preview_html
    assert "1097" in preview_html
    assert "background_image_urls" in preview_html
    assert "payloadImages" in preview_html
    assert "place-items: center" in preview_html
    assert "formatCoverHeadline" in preview_html
    assert "min-height: 4.8em" in preview_html
    assert "padding: 8px 20px 0" in preview_html
    assert "applyTemplateMask" in preview_html
    assert "hexToRgba" in preview_html
    assert "coverTileTitles" in preview_html
    assert "tile_brand_text" in preview_html
    assert "tile_tagline_text" in preview_html
    assert 'class="sound-toggle hidden"' in preview_html


def test_video_matrix_preview_matches_wechat_phone_reference_shell() -> None:
    preview_html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_preview.html").read_text(encoding="utf-8")
    preview_css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_preview.css").read_text(encoding="utf-8")

    assert "phone-mockup" in preview_html
    assert "dynamic-island" in preview_html
    assert "bg-overlay" in preview_html
    assert "https://cdn.tailwindcss.com" in preview_html
    assert "w-[393px] h-[852px]" in preview_html
    assert "text-stroke-yellow" in preview_html
    assert "模块化算力单元即装即产" not in preview_html
    assert "设备扩展零延迟" not in preview_html
    assert "冗余电力系统智能切换" not in preview_html
    assert "在线率维持95%以上" not in preview_html
    assert "收益连续性行业标杆</h2>" not in preview_html
    assert "GasGx小白" in preview_html
    assert 'text-[#5dd62c]">GasGx' in preview_html
    assert "headline-overlay" not in preview_html
    assert "benefit-overlay" not in preview_html
    assert "content-meta" not in preview_html
    assert "iphone-air" not in preview_html
    assert "phone-status" not in preview_html
    assert "creator-bar" not in preview_html
    assert "channels-nav" not in preview_html
    assert "width: 393px" in preview_html
    assert "height: 852px" in preview_html
    assert "border: 14px solid #1a1a1a" in preview_html
    assert "border-radius: 55px" in preview_html
    assert "width: 90px" in preview_html
    assert "height: 26px" in preview_html
    assert "top: 10px" in preview_html
    assert "bottom-action-scale" in preview_html
    assert "transform: scale(0.75)" in preview_html
    assert "status-scale" in preview_html
    assert "gap-24" in preview_html
    assert "pt-[18px]" in preview_html
    assert "transform: translateX(-50%) scale(0.75)" in preview_html
    assert "width: 500px" in preview_html
    assert "justify-content: space-between" in preview_html
    assert "bottom-meta" in preview_html
    assert "left: 21px" in preview_html
    assert "bottom: 64px" in preview_html
    assert "creator-group" in preview_html
    assert "transform: translateX(-52px)" in preview_html
    assert "back-control" in preview_html
    assert "transform: translateY(-24px)" in preview_html
    assert 'aria-label="小窗"' not in preview_html
    assert "phone.addEventListener" in preview_html
    assert "toggleVideoPlayback" in preview_html
    assert "rgba(0,0,0,0.4) 0%" in preview_html
    assert "self-contained" in preview_css
