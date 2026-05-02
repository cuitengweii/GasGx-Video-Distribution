from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_video_matrix_bgm_uses_local_library_with_visible_directory_hint() -> None:
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_styles.css").read_text(encoding="utf-8")
    preview = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_preview.html").read_text(encoding="utf-8")

    assert "本地背景音乐" in app
    assert "暂无本地 MP3 文件" in app
    assert "local_bgm_dir" in app
    html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix.html").read_text(encoding="utf-8")
    assert "视频碎片素材目录" in html
    assert "languageGroup" not in html
    assert "languageGroup" not in app
    assert "#languageGroup" not in css
    assert 'radioValue("copy_language")' not in app
    assert "检查字幕背板、片尾文案和语言设置。" not in app
    assert "文案语言配置无效" not in app
    assert "手动上传素材" not in html
    assert "上传文字稿" not in html
    assert "TEXT视频文字设置" not in html
    assert "主标题 / 视频口号" not in html
    assert "副标题 / 视频标题" not in html
    assert "首屏 CTA" not in html
    assert "结尾关注提醒" not in html
    assert "视频 HUD 文本" not in html
    assert "字幕背板调整区" in app
    assert "HUD调整区" not in app
    assert "文案参考资料" not in html
    assert '<input id="headline" type="hidden" />' in html
    assert html.index('class="cover-workbench"') < html.index('class="video-template-workbench"') < html.index('class="side-editor"')
    assert html.index('class="video-template-workbench"') < html.index('class="ending-workbench cover-workbench"') < html.index('class="side-editor"')
    assert "transcriptFile" not in html
    assert "transcriptText" not in html
    assert "generationConfirmModal" in html
    assert "generationPreflightModal" in html
    assert "generationPreflightBody" in html
    assert "generationWaitOverlay" in html
    assert "generationWaitPercent" in html
    assert "preflightClose" in html
    assert '"Queued": "任务已提交，正在排队准备。请保持当前页面打开，系统会自动开始处理。"' in app
    assert 'queued: "任务已提交，正在等待开始"' in app
    assert "function localizedJobTitle" in app
    assert "function showGenerationWaitOverlay" in app
    assert "function startJobProgressTicker" in app
    assert "displayedJobPercent" in app
    assert "视频生成中，系统会以 1% 为单位持续刷新等待进度。" in app
    assert "revealNextPreflightStep(index + 1)" in app
    assert "片尾封面模板" in html
    assert "endingTemplatePreview" in html
    assert "endingAssetPreview" in html
    assert "endingTemplateForm" in html
    assert "endingTemplateSwitch" in html
    assert "endingTemplateMenu" in html
    assert "openEndingTemplateDirInline" in app
    assert "点击查看" not in html
    assert '<button id="saveState" type="button">背景音乐库</button>' in html
    assert '<button id="openBgmDir" type="button">打开音乐目录</button>' in html
    assert "<details><summary>背景音乐库</summary>" not in html
    assert "coverTemplateSwitch" in html
    assert "coverTemplateMenu" in html
    assert "模板切换" in html
    assert "openBgmDir" in html
    assert "bgmLibraryPopover" in html
    assert '<label>输出文件<select id="outputOptions"><option value="mp4">mp4</option></select></label>' in html
    assert '<option value="png">png</option>' not in html
    assert '<option value="txt">txt</option>' not in html
    assert '<option value="json">json</option>' not in html
    assert 'id="videoDurationMin" class="number-field"' in html
    assert 'id="videoDurationMax" class="number-field"' in html
    assert 'class="input-with-unit"><input id="videoDurationMin"' in html
    assert 'class="input-with-unit"><input id="videoDurationMax"' in html
    assert "materialCategories(data)" in app
    assert "settings.material_categories" in app
    assert "addMaterialCategory" in app
    assert "renameMaterialCategory" in app
    assert "data-category-rename" not in app
    assert "class=\"category-edit-icon\"" not in app
    assert "按原文修改" not in app
    assert "已保存素材目录名称" in app
    assert "<span>秒</span>" not in app
    assert "<span>秒</span>" in html
    assert "<span>采用最新前</span>" in app
    assert "<span>条</span>" in app
    assert "source-total-count" in app
    assert "素材总数：" in app
    assert "/api/video-matrix/material-categories/${encodeURIComponent(categoryId)}" in app
    assert "const outputRoot = state.output_root || settings.output_root" in app
    assert '$("outputRoot").dataset.fullPath = outputRoot' in app
    assert "output_root: outputRootPath()" in app
    assert "toggleBgmLibraryPopover" in app
    assert '$("openBgmDir").onclick = () => openFolder(bgmLibraryState.directory)' in app
    assert 'document.querySelector(".sidebar details summary")' not in app
    assert "const bgmPanel = $(\"bgmPanel\")" in app
    assert "if (!bgmPanel) return" in app
    assert "本地曲库列表" in app
    assert "本地曲库</strong>" in app
    assert "网络曲库" not in app
    assert "bgm-select-check" in app
    assert "downloadPixabayTrack" not in app
    assert "data-pixabay-download" not in app
    assert "pixabaySearchInput" not in app
    assert "pixabayPlayer" not in app
    assert "downloadCurrentPixabay" not in app
    assert "selectPixabayTrack" not in app
    assert "syncPixabayPlayer" not in app
    assert "track.audio_error" not in app
    assert "/api/video-matrix/bgm/mock-download" not in app
    assert "模拟下载到本地" not in app
    assert "track.is_cdn_audio" not in app
    assert "点击曲目载入播放器试听" not in app
    assert "loadPixabayTracks" not in app
    assert "/api/video-matrix/pixabay/industry" not in app
    assert "Pixabay industry" not in app
    assert "pixabay-track-list" not in app
    assert "toggleBgmLibrarySize" in app
    assert 'panel.classList.toggle("modal", !isHidden)' in app
    assert "bgm-modal-open" in app
    assert 'panel.classList.add("hidden")' in app
    assert "downloadBgmToLibrary" not in app
    assert "downloadBgm" not in app
    assert "bgmDownloadUrl" not in app
    assert "bgmUrlPreview" not in app
    assert "<span>音频地址</span>" not in app
    assert "音频直链试听 / 下载" not in app
    assert "bgm-local-section" in app
    assert "bgm-pixabay-section" not in app
    assert "未选中时生成会随机取 1 首" in app
    assert "selectedBgmLibraryId()" in app
    assert "const selectedBgm = selectedBgmLibraryId()" in app
    assert "[selectedBgm, ...bgmLibraryState.local.filter" in app
    assert "data-bgm-select" in app
    assert "bgm_library_id: selectedBgmLibraryId()" in app
    assert "bgm-popover-links" not in app
    shell_css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")
    assert ".add-category-row" in css
    assert "[data-category-label]" in css
    assert ".category-edit-icon" not in css
    assert ".source-total-count" in css
    assert ".bgm-library-popover" in css
    assert ".generation-wait-overlay" in css
    assert ".generation-wait-panel" in css
    assert ".dir-row code" in css
    assert ".embed-mode .sidebar" in css
    assert "overflow: hidden" in css
    assert "font-size: 12px" in css
    assert ".embed-mode .sidebar-radio-field .radio-row" in css
    assert ".embed-mode .library-action" in css
    assert "min-height: clamp(28px, 3.5vh, 34px)" in css
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
    assert "bgm_library_id: selectedBgmLibraryId()" in app
    assert "renderComposition" in app
    assert "composition_sequence" in app
    assert "composition_customized" in app
    assert "defaultCompositionSequence" in app
    assert "scheduleStateSave" in app
    assert "scheduleVideoTemplateSave" in app
    assert "scheduleCoverTemplateSave" in app
    assert "saveTemplateSelection" in app
    assert "renderEndingTemplatePanel" in app
    assert "endingTemplateModeOptions" in app
    assert "endingModeLoading" in app
    assert 'buttonLoadingInline("切换中...")' in app
    assert 'item.disabled = true' in app
    assert "ending_template_dir" in app
    assert "ending_templates" in app
    assert "ending_template_mode" in app
    assert "ending_template_id" in app
    assert "ending_template_ids" in app
    assert "ending_template_dir" in app
    assert "ending_cover_template_id" in app
    assert "ending_cover_templates" in app
    assert "endingRandomMaterialHtml" in app
    assert "data-ending-template-choice" in app
    assert "data-ending-play" not in app
    assert "data-ending-stop" not in app
    assert "data-ending-preview-toggle" in app
    assert "endingPreviewToggleButtonHtml" in app
    assert 'M8 5.8v12.4L18 12 8 5.8Z' in app
    assert "endingPreviewOverrideName" in app
    assert "data-ending-preview-video" in app
    assert "video?.play?.().catch(() => {})" in app
    assert "renderEndingTemplatePanel({ ending_templates: endingTemplateState.local" in app
    assert "renderEndingTemplateMenu" in app
    assert "selectEndingCoverTemplate" in app
    assert "片尾封面模板 01" in app
    assert "saveCurrentEndingCoverTemplate" in app
    assert "saveEndingCoverAsNewTemplate" in app
    assert '<button type="button" id="saveEndingCover">保存</button>' in app
    assert '<button type="button" id="saveEndingCoverAsNew">新建保存</button>' in app
    assert 'buttonLoadingInline("保存中...")' in app
    assert 'buttonLoadingInline("新建中...")' in app
    assert "endingFollowText" not in app
    ending_panel_source = app[app.index("function renderEndingTemplatePanel"):app.index("function isIndependentCover")]
    assert "一句话视频描述" not in ending_panel_source
    assert '片尾文案<textarea data-ending-cover-key="single_cover_title_text"' in app
    assert 'if (key === "single_cover_title_text")' in app
    assert "function endingCopyTextValue()" in app
    assert 'follow_text: endingCopyText' in app
    assert 'cta: ""' not in app
    assert 'event.source === $("endingTemplatePreview")?.contentWindow ? "ending" : "cover"' in app
    assert "从 video_matrix\\\\ending_template 勾选备用视频片尾" in app
    assert ".ending-material-list" in css
    assert ".ending-material-row" in css
    assert ".ending-material-row button" in css
    assert ".ending-preview-toggle svg" in css
    assert ".ending-preview-toggle:hover" in css
    assert ".ending-preview-toggle.active" in css
    assert ".template-tabs button.is-loading" in css
    assert "border-radius: 999px" in css
    assert "background: var(--green)" in css
    assert "grid-template-columns: minmax(0, 1fr) auto auto" in css
    assert "openEndingTemplateDirInline" in app
    assert 'ending-template-dir-row ${mode === "random" ? "" : "hidden"}' in app
    assert '"specific") state.ending_template_mode = "random"' in app
    assert '["specific", "指定素材"]' not in app
    assert "endingAssetPreview" in app
    assert "cloneVideoTemplate" in app
    assert "nextTemplateCloneId" in app
    assert "saveCoverAsNewTemplate" in app
    assert "nextCoverTemplateMeta" in app
    assert '<button type="button" id="saveCover">保存</button>' in app
    assert '<button type="button" id="saveCoverAsNew">新建保存</button>' in app
    assert "saveCurrentCoverTemplate" in app
    assert '$("saveCoverAsNew").onclick = saveCoverAsNewTemplate' in app
    assert 'newTemplate.cover_layout = "single_video"' in app
    assert "已新建独立封面模板" in app
    assert "已自动生成 9 组九宫格图片模板" not in app
    assert 'showTemplateActionStatus("保存成功", "coverForm")' in app
    assert 'await api("/api/video-matrix/cover-templates"' in app
    assert "state.cover_templates = coverTemplates" in app
    assert "buildCoverTemplateVariants(sourceTemplate)" in app
    assert "九宫格图片模板 ${serial}" in app
    assert "cover_template_${serial}" in app
    assert "第一屏封面模板 ${serial}" in app
    assert "selectVideoTemplate" in app
    assert "title_bg_height" in preview
    assert "title_bg_opacity" in preview
    assert 'id="cloneVideoTemplate"' in app
    assert '<button type="button" id="saveVideoTemplate">保存当前</button>' in app
    assert 'button.textContent = "保存中..."' in app
    assert 'showTemplateActionStatus("保存成功")' in app
    assert 'button.innerHTML = buttonLoadingInline("新建中...")' in app
    assert 'showTemplateActionStatus("新建模板成功")' in app
    assert ".template-action-status" in css
    assert ".template-actions button.is-loading" in css
    assert ".ending-editor" in css
    assert ".ending-asset-preview" in css
    assert ".ending-template-dir-row" in css
    assert "切换正文模板..." in app
    assert "selectCoverTemplate" in app
    assert "renderCoverTemplateMenu" in app
    assert "coverTemplateDisplayName" in app
    assert "videoTemplateDisplayName" in app
    assert "data-cover-template" in app
    assert 'trigger.innerHTML = buttonLoadingInline("切换中...")' in app
    assert "切换第一屏模板..." in app
    assert "第一屏封面模板" in app
    assert "视频叠层模板" in app
    assert 'const nextName = videoTemplateDisplayName(nextId, {}, Object.keys(templates).length)' in app
    assert "coverTemplateBackgrounds" not in html
    assert "renderCoverTemplateBackgrounds" not in app
    assert "coverVisualToolbarHtml" in app
    assert "gasgx-cover-template-command" in app
    assert "gasgx-cover-template-update" in app
    assert "gasgx-cover-template-text-update" in app
    assert "applyCoverTemplateUpdates" in app
    assert "applyCoverTextUpdates" in app
    assert "body.cover-profile-mode .vm-template-mask-bg" in preview
    assert 'id="templateMaskBg" class="vm-template-mask-bg" hidden' in preview
    assert "singleCoverMaskStyle(template)" in preview
    assert 'node.style.left = align === "center" ? "50%"' in preview
    assert 'translateX(-50%)' in preview
    assert 'node.style.right = ""' in preview
    assert 'mode === "dual_gradient"' in preview
    assert "tileCoverFontSize" in preview
    assert 'style="${maskStyle}"' in preview
    assert "box-sizing: border-box" in preview
    assert ".device-side-button" in preview
    assert "可视化调整" in app
    assert "当前模板调整" not in app
    assert "color-swatch-button" in app
    assert "color-picker-icon" in app
    assert "color-current-dot" in app
    assert app.index('data-value="right" title="右对齐">右齐') < app.index('title="文字颜色"') < app.index('data-visual-command="font-family"')
    assert "z-index: 2;" in preview
    assert ".vm-template-text.text-effect-typewriter.active" in preview
    assert "border-right: 0;" in preview
    assert "function pointerSelectionTarget(event, node)" in preview
    assert "function pairedTextNode(target)" in preview
    assert '.vm-template-editable.active[data-template-target="sloganBar"]' in preview
    assert '.vm-template-editable.active[data-template-target="titleBar"]' in preview
    assert '.vm-template-editable.active[data-template-target="hudBar"]' in preview
    assert 'data-template-target="hud"></div>' in preview
    assert 'outline-color: #ff4053' in preview
    assert "pairedTextTarget(activeTarget)" in preview
    assert "pairedHudTarget(activeTarget)" in preview
    assert "const DRAG_START_THRESHOLD = 5" in preview
    assert "Math.hypot(event.clientX - dragging.startX, event.clientY - dragging.startY)" in preview
    assert "if (moved < DRAG_START_THRESHOLD) return" in preview
    assert 'data-value="left" title="左对齐">左齐' in app
    assert 'data-value="center" title="居中对齐">居中' in app
    assert 'data-value="right" title="右对齐">右齐' in app
    assert "show_template_mask: false" in app
    assert "show_template_mask: true" in app
    assert "coverMaskModeOptions" in app
    assert "上渐变蒙版" in app
    assert "下渐变蒙版" in app
    assert "上下渐变蒙版" in app
    assert "dual_gradient" in app
    assert "全蒙版" in app
    assert 'data-key="mask_color"' in app
    assert 'data-key="tile_titles_text"' not in app
    assert 'data-key="single_cover_logo_text"' in app
    assert 'data-key="single_cover_slogan_text"' in app
    assert 'data-key="single_cover_title_text"' in app
    assert "selectedCoverModelImageUrl" not in app
    assert "切换封面背景..." not in app
    assert "background_image_urls: modelImages.map" in app
    assert "background_image_url: selectedModelImageUrl || modelImages[0]?.url || \"\"" in app
    assert "await refreshVideoTemplateGallery();" in app
    api = (ROOT / "src" / "gasgx_distribution" / "video_matrix_api.py").read_text(encoding="utf-8")
    template_preview = (ROOT / "src" / "gasgx_distribution" / "video_matrix" / "template_preview.py").read_text(encoding="utf-8")
    assert "ENDING_TEMPLATE_DIR" in api
    assert '"/ending-templates/{filename}"' in api
    assert 'payload.get("background_image_url")' in api
    assert "background=background" in api
    assert "background: Image.Image | None = None" in template_preview
    assert "_fit_background(background, width, height)" in template_preview
    assert 'str(template.get("hud_color") or template["primary_color"])' in template_preview
    assert "sourcePreviewVideos = Array.isArray(data.source_videos) ? data.source_videos : []" in app
    assert "videoTemplatePreviewVideos" in app
    assert "videoTemplateCardPreviewHtml(template)" in app
    assert "videoTemplateCardBarHtml(template, \"hud\")" in app
    assert '${escapeHtml(id)} / ${escapeHtml(videoTemplateDisplayName' not in app
    assert '${escapeHtml(videoTemplateDisplayName(id, template, index))}' in app
    assert '<button type="button" id="saveCoverAsNew">新建保存</button>' in app
    assert "片尾封面模板 01" in app
    assert "isInheritedEndingCoverName" in app
    assert "function videoTemplateCardBarStyle" in app
    assert "selectedModelImageUrl || modelImages[0]?.url || \"\"" in app
    assert "video-template-thumb-mask" in app
    assert "/api/video-matrix/preview-file?path=" not in app
    assert "toggleTemplateCardVideo" in app
    assert "coverGallery" not in html
    assert "setPanelLoading(\"coverGallery\"" not in app
    assert "refreshGallery" not in app
    assert "await selectVideoTemplate(card.dataset.id)" in app
    assert "正文模板自动保存失败" in app
    assert "第一屏模板自动保存失败" in app
    assert "pulseImageLoading(\"coverPreview\"" in app
    assert "pulseImageLoading(\"videoTemplatePreview\"" in app
    assert "应用封面参数..." in app
    assert "应用模板参数..." in app
    assert "background: transparent" in css
    assert "box-shadow: none" in css
    assert "const recentLimits = state.recent_limits || settings.recent_limits || {}" in app
    assert "transcript_text" not in app
    assert "runPreflightChecks(statePayload)" in app
    assert "function buildPreflightChecks" in app
    assert "function setPreflightStepStatus" in app
    assert "function animatePreflightProgress" in app
    assert "data-preflight-progress" in app
    assert "data-preflight-percent" in app
    assert "preflight-config" in app
    assert "function shortText" in app
    assert "点击继续进入最终确认" in app
    assert "优化建议：把最大节拍分析时长调到不低于" in app
    assert "提交完整性" in app
    assert "生成文案" in app
    assert "预检通过" in app
    assert "片尾配置" in app
    assert "本地 BGM" in app
    assert "active_category_ids" in app
    assert "confirmGeneration(statePayload)" in app
    assert "generationConfirmHtml" in app
    assert "启用素材分类" in app
    assert "生成结构" in app
    assert "本次算法框架" in app
    assert "分析本地背景音乐节拍" in app
    assert "data-composition-category" in app
    assert "data-composition-duration" not in app
    assert "data-composition-remove" in app
    assert "/api/video-matrix/bgm/" in app
    assert ".help-dot" in css
    assert ".bgm-local-item" in css
    assert ".bgm-select-check" in css
    assert 'content: "\\2713"' in css
    assert "color: #071108" in css
    assert "box-shadow: 0 0 0 2px rgba(93, 214, 44, .28)" in css
    assert "background: linear-gradient(90deg, var(--green), #7cff4f)" in css
    assert ".pixabay-track audio" not in css
    assert ".pixabay-search-row" not in css
    assert ".pixabay-player-card" not in css
    assert ".pixabay-track.is-selected" not in css
    assert ".pixabay-track.is-cdn-audio button" not in css
    assert "border: 1px dashed rgba(93, 214, 44, .48)" not in css
    assert ".bgm-download-box" not in css
    assert ".pixabay-refresh-button" not in css
    assert ".pixabay-track-list" not in css
    assert ".pixabay-track" not in css
    assert ".bgm-library-popover.modal" in css
    assert "z-index: 80" in css
    assert "body.bgm-modal-open .sidebar {\n  z-index: 120;\n}" in css
    assert "100vmax rgba(0, 0, 0, .64)" in css
    assert ".bgm-popover-head" in css
    assert ".bgm-local-section" in css
    assert ".bgm-pixabay-section" not in css
    assert "scrollbar-width: thin" in css
    assert "::-webkit-scrollbar-thumb" in css
    assert "#saveState" in css
    assert "#openBgmDir" in css
    assert ".input-with-unit" in css
    assert ".template-actions" in css
    assert "grid-template-columns: minmax(0, 1fr) 124px" in css
    assert "cursor:pointer" in css
    assert "model-image-workbench" in html
    assert "模拟素材选择" in html
    assert "preview-caption-actions" in html
    assert "button-icon" in html
    assert "模板切换" in html
    assert "独立视频封面" in html
    assert ".model-image-workbench" in css
    assert ".preview-caption-actions" in css
    assert ".button-icon" in css
    assert "grid-template-columns: minmax(88px, .9fr) minmax(100px, 1fr) minmax(116px, 148px) minmax(56px, 70px) minmax(54px, 58px)" in css
    assert ".source-panel .source-composition-row button {\n  width: 100%;" in css
    assert "left: 0;\n  right: auto;" in css
    assert "@media (max-width: 1180px)" in css
    assert "videoTemplateCaption" not in html
    assert "videoTemplateCaption" not in app
    assert html.index('id="videoTemplateBackgrounds"') < html.index('class="cover-workbench"') < html.index('class="video-template-workbench"')
    assert html.index('class="template-preview-editor"') < html.index('class="video-template-picker"') < html.index('id="videoTemplateGallery"')
    assert 'id="videoTemplateSelector"' not in html
    assert "video-template-name-button" in app
    assert ".video-template-name-button" in css
    assert ".video-template-picker" in css
    assert "height: calc(var(--preview-phone-height) + 36px)" in css
    assert "--preview-phone-width: 479.25px" in css
    assert "width: 479.25px" in css
    assert "minmax(360px, 1fr)" in css
    assert "justify-content:stretch" in css
    assert "max-width: none" in css
    assert "hud_bar_width" in preview
    assert "hud_bar_x" in preview
    assert "slogan_bg_x" in preview
    assert "slogan_bg_y" in preview
    assert "slogan_bg_width" in preview
    assert "title_bg_x" in preview
    assert "title_bg_y" in preview
    assert "title_bg_width" in preview
    assert "hudBar.style.left = designX(template.hud_bar_x || 0)" in preview
    assert "template.slogan_bg_x || 0" in preview
    assert "template.title_bg_x || 0" in preview
    assert "template.slogan_bg_y ?? template.slogan_y" in preview
    assert "template.title_bg_y ?? template.title_y" in preview
    assert "hud_bar_x: toDesignX(x)" in preview
    assert "slogan_bg_x: toDesignX(x)" in preview
    assert "title_bg_x: toDesignX(x)" in preview
    assert "slogan_bg_y: toDesignY(y)" in preview
    assert "title_bg_y: toDesignY(y)" in preview
    assert "slogan_bg_width || 1080" in preview
    assert "title_bg_width || 1080" in preview
    assert "ensureTextTarget()" in preview
    assert "ensureHudTarget(command)" in preview
    assert 'if (target === "sloganBar") return "slogan"' in preview
    assert 'if (target === "titleBar") return "title"' in preview
    assert 'if (target === "hudBar") return "hud"' in preview
    assert 'if (target === "slogan") return "sloganBar"' in preview
    assert 'if (target === "title") return "titleBar"' in preview
    assert 'if (target === "hud") return "hudBar"' in preview
    assert "textTargetForAlignment(activeTarget)" in preview
    assert 'const useHudAlignBox = target === "hud" && template[alignKey]' in preview
    assert "node.style.left = useHudAlignBox ? designX(template.hud_bar_x ?? x) : designX(x)" in preview
    assert "node.style.width = useHudAlignBox ? designX(template.hud_bar_width || 1080) : \"\"" in preview
    assert "文字调整区" in app
    assert 'data-visual-command="bar-align" data-value="left" title="字幕背板左对齐">左齐' in app
    assert 'data-visual-command="bar-align" data-value="center" title="字幕背板居中对齐">居中' in app
    assert 'data-visual-command="bar-align" data-value="right" title="字幕背板右对齐">右齐' in app
    assert 'data-value="hudBar" title="选择 字幕背板 背景">字幕背板背景' not in app
    assert 'data-visual-command="opacity"' in app
    assert "const oldWidth = clamp(Number(template[widthKey] || 1080), 120, 1080)" in preview
    assert "const centerX = oldX + oldWidth / 2" in preview
    assert "const newX = clamp(Math.round(centerX - newWidth / 2), 0, 1080 - newWidth)" in preview
    assert "postTemplateUpdates({ [widthKey]: newWidth, [xKey]: newX })" in preview
    assert "videoTextFontOptions" in app
    assert "fontPreviewEnglish" in app
    assert "fontPreviewChinese" in app
    assert "function fontSamplePreviewHtml(label)" in app
    assert "const chineseOnly = /中文|阿里|思源|鸿蒙|优设/.test(normalized)" in app
    assert "const englishOnly = /英文|English|DIN|硬核|复古|招牌/.test(normalized)" in app
    assert "if (chineseOnly && !/中英/.test(normalized))" in app
    assert "GasGx" in app
    assert "盖斯基克斯" in app
    assert "中文主标题" in app
    assert "英文主标题 DIN" in app
    assert "阿里普惠重黑" in app
    assert "思源重黑" in app
    assert "鸿蒙粗黑" in app
    assert "优设标题黑" in app
    assert "DIN 压缩广告" in app
    assert "font-sample-picker" in app
    assert "font-sample-option" in app
    assert "font-sample-en" in app
    assert "font-sample-cn" in app
    assert "grid-template-columns: repeat(3, minmax(0, 1fr))" in css
    assert "overflow: visible" in css
    assert "max-height: 390px" not in css
    font_sample_css = css[css.index(".font-sample-picker {"):css.index(".font-sample-option {")]
    assert "overflow-y: auto" not in font_sample_css
    assert "style=\"font-family:" in app
    assert "中英混排粗黑" in app
    assert "数据机甲" in app
    assert "中文黑体冲击" in app
    assert "English Data Mono" in app
    assert "English Pop Comic" in app
    assert "textEffectOptions" in app
    assert app.index('["none", "无动效"]') < app.index('["fade-in", "淡入"]')
    for effect in [
        "fade-in", "fade-out", "fade-in-out",
        "slide-down", "slide-left", "slide-right",
        "blink", "wave", "jitter", "zoom-in", "shadow-pop",
    ]:
        assert effect in app
    assert 'data-visual-command="text-effect"' in app
    assert "文字动效" in app
    assert 'data-visual-command="hud-bg-color"' in app
    assert 'aria-label="字幕背板背景色"' in app
    assert 'data-visual-command="hud-radius"' in app
    assert 'data-visual-command="hud-radius" type="range" min="0" max="100"' in app
    assert "setBackgroundOpacity(value)" in preview
    assert "setBackgroundAlign(value || \"left\")" in preview
    assert "setHudBackgroundColor(value)" in preview
    assert "setHudRadius(value)" in preview
    assert "clamp(Number(value), 0, 100)" in preview
    assert "hudBar.style.borderRadius = designRadius(template.hud_bar_radius ?? 10)" in preview
    assert "function designRadius(value)" in preview
    assert "setTextEffect(value)" in preview
    assert "applyTextEffect(node" in preview
    assert "textColorValue(\"slogan\", template)" in preview
    assert "textColorValue(\"title\", template)" in preview
    assert "textColorValue(\"hud\", template)" in preview
    assert 'if (target === "hud") return template.hud_color || template.primary_color || "#ffffff"' in preview
    assert 'const color = template.hud_color || template.primary_color || "#ffffff"' in app
    assert 'postTemplateUpdates({ [`${target}_color`]: value })' in preview
    assert "text-effect-glow" in preview
    assert "text-effect-fade-in" in preview
    assert "text-effect-slide-left" in preview
    assert "text-effect-shadow-pop" in preview
    assert "white-space: pre;" in preview
    assert "word-break: keep-all" in preview
    assert 'if (target === "singleSlogan" || target === "singleTitle")' in preview
    assert 'node.style.maxWidth = "none"' in preview
    assert "@keyframes vmTextGlow" in preview
    assert "@keyframes vmTextFadeIn" in preview
    assert "@keyframes vmTextShadowPop" in preview
    assert "@keyframes vmTextType" in preview
    assert ".visual-effect-control" in css
    assert 'backgroundTargetForControl(activeTarget || "title")' in preview
    assert "backgroundTargetXKey(backgroundTarget)" in preview
    assert "function backgroundScreenAlignX(value, width)" in preview
    assert 'const key = `${target}_font_size`' in preview
    assert 'if (!activeTarget && command === "align") selectTarget("title")' in preview
    assert 'if (command === "align") {' in preview
    assert 'if (activeTarget !== "hud") selectTarget("hud")' in preview
    assert "updates.hud_x = hudScreenAlignX(align)" in preview
    assert 'updates.hud_bar_x = hudBarScreenAlignX(align, Number(template.hud_bar_width || 1080))' in preview
    assert "function hudScreenAlignX(value) {\n        return 0;\n      }" in preview
    assert 'return Math.round((1080 - safeWidth) / 2)' in preview
    assert "显示上标题" in app
    assert "显示中标题" in app
    assert "显示下标题" in app
    assert "上标题背景 X" not in app
    assert "中标题背景 X" not in app
    assert "下标题背景 X" not in app
    assert "上标题背景高度" not in app
    assert "中标题背景高度" not in app
    assert "下标题背景高度" not in app
    assert 'data-visual-command="select-target" data-value="slogan"' not in app
    assert 'data-visual-command="select-target" data-value="title"' not in app
    assert 'data-visual-command="select-target" data-value="hud"' not in app
    assert 'command === "select-target"' in preview
    assert ".visual-target-tabs" in css
    assert 'data-visual-command="width-down"' in app
    assert 'data-visual-command="width-up"' in app
    assert 'data-visual-command="height-down"' in app
    assert 'data-visual-command="height-up"' in app
    assert "adjustHeight" in preview
    assert "backgroundTargetHeightKey" in preview
    assert "defaultBackgroundHeight" in preview
    assert "grid-template-columns: repeat(auto-fill, 154px)" in css
    assert "justify-self: center" in css
    assert "font-size: 13px" in css
    assert ".color-swatch-button" in css
    assert ".color-picker-icon" in css
    assert ".color-current-dot" in css
    assert ".cover-template-switcher" in css
    assert ".cover-template-menu" in css
    assert ".cover-card img," in css
    assert ".cover-card video" in css
    assert "aspect-ratio: 9 / 16" in css
    assert "video-template-thumb" in css
    assert "video-template-thumb-bar" in css
    assert ".video-template-thumb img" in css
    assert '<video src="${escapeHtml(videoSrc)}" poster="${data.data_url}"' not in app
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
    assert app.index("模板名称<input data-key=\"name\"") < app.index("visualTemplateToolbarHtml(template)")
    assert 'classList.toggle("error-message", isError)' in app
    assert '["render_wait", "等待导出", 82, ["render"]]' in app
    assert "backendJobMessageMap" in app
    assert "displayTemplateName" in app
    assert "已保存正文模板：${displayTemplateName" in app
    assert "警示：视频生成期间不可以关闭页面，否则会影响视频生成" in html
    assert ".job-warning" in css
    assert "repeat(auto-fit, minmax(96px, 1fr))" in css
    assert ".job-status-card.error" in css
    assert "#jobMessage.error-message" in css
    assert ".confirm-modal" in css
    assert ".confirm-panel" in css
    assert ".confirm-algorithm" in css
    assert ".preflight-step" in css
    assert ".preflight-status" in css
    assert ".preflight-progress" in css
    assert ".preflight-progress-wrap" in css
    assert "@keyframes preflightScan" in css
    assert ".preflight-config" in css
    assert ".preflight-step.warn small" in css
    assert ".preflight-actions" in css
    assert "composition-panel" in css
    assert "videoDurationMin" in html
    assert "videoDurationMax" in html
    assert "sidebarCoverTemplate" in html
    assert "sidebarVideoTemplate" in html
    assert "sidebarEndingTemplateMode" in html
    assert '<option value="dynamic">文字片尾</option>' in html
    assert '<option value="random">视频片尾</option>' in html
    assert "renderSidebarTemplateSelectors" in app
    assert "selectCoverTemplate(coverSelect.value)" in app
    assert "selectVideoTemplate(videoSelect.value)" in app
    assert "switchEndingTemplateMode(endingSelect.value)" in app
    assert "video_duration_min" in app
    assert "targetFpsGroup" in html
    assert "video_duration_max" in app
    assert 'renderRadio("targetFpsGroup", "target_fps"' in app
    assert 'target_fps: Number(radioValue("target_fps") || settings.target_fps || 60)' in app
    assert "template_config: activeVideoTemplateSnapshot()" in app
    assert "cover_template_config: activeCoverTemplateSnapshot()" in app
    assert "ending_cover_template: endingCoverTemplate" in app
    assert "function activeVideoTemplateSnapshot()" in app
    assert "function activeCoverTemplateSnapshot()" in app
    assert "function activeEndingCoverTemplateSnapshot" in app
    assert "snapshot.cover_layout = \"single_video\"" in app
    assert "目标帧率" in app
    assert "${statePayload.target_fps}fps" in app
    assert ".sidebar-radio-field" in css
    assert ".sidebar-radio-field .radio-row label:has(input:checked)" in css
    assert "background: var(--green)" in css
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
    assert "selectionFrameTarget(target)" in preview_html
    assert "return target;" in preview_html
    assert "node.dataset.templateTarget === frameTarget" in preview_html
    assert ".vm-template-text.active" not in preview_html
    assert "slogan_bg_height" in preview_html
    assert "title_bg_height" in preview_html
    assert "template.title_bg_height || template.slogan_bg_height" not in preview_html
    assert "template.title_bg_height || 92" in preview_html
    assert "isBackgroundTarget" in preview_html
    assert "coverProfileShell" in preview_html
    assert "cover-profile-mode" in preview_html
    assert "body.cover-profile-mode .phone-mockup" in preview_html
    assert "body.cover-single-mode .phone-mockup" in preview_html
    assert "border: 0" in preview_html
    assert "box-shadow: 0 0 0 14px #1a1a1a, 0 0 0 16px #333" in preview_html
    assert "border-radius: 55px" in preview_html
    assert "body.cover-profile-mode .cover-profile-shell" in preview_html
    assert "border-radius: 41px" in preview_html
    assert "body.cover-profile-mode .device-side-button" in preview_html
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
    assert "singleCoverMaskStyle" in preview_html
    assert "template.single_cover_logo_text" in preview_html
    assert "template.single_cover_slogan_text" in preview_html
    assert "template.single_cover_title_text" in preview_html
    assert "cover-tile-logo" in preview_html
    assert "cover-tile-slogan" in preview_html
    assert "aspect-ratio: 9 / 16" in preview_html
    assert 'class="sound-toggle hidden"' in preview_html


def test_video_matrix_preview_matches_wechat_phone_reference_shell() -> None:
    preview_html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_preview.html").read_text(encoding="utf-8")
    preview_css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_preview.css").read_text(encoding="utf-8")

    assert "phone-mockup" in preview_html
    assert "dynamic-island" in preview_html
    assert "bg-overlay" in preview_html
    assert "https://cdn.tailwindcss.com" in preview_html
    assert "w-[479.25px] h-[852px]" in preview_html
    assert "const PREVIEW_WIDTH = 479.25" in preview_html
    assert "const PREVIEW_HEIGHT = 852" in preview_html
    assert "PREVIEW_WIDTH / 1080" in preview_html
    assert "PREVIEW_HEIGHT / 1920" in preview_html
    assert "text-stroke-yellow" in preview_html
    assert "模块化算力单元即装即产" not in preview_html
    assert "设备扩展零延迟" not in preview_html
    assert "冗余电力系统智能切换" not in preview_html
    assert "在线率维持95%以上" not in preview_html
    assert "收益连续性行业标杆</h2>" not in preview_html
    assert "GasGx小白" in preview_html
    assert 'bg-black border border-[#5dd62c]' in preview_html
    assert 'text-[#5dd62c]">GasGx' in preview_html
    assert '<span class="text-[10px]">Gx</span>' not in preview_html
    assert "headline-overlay" not in preview_html
    assert "benefit-overlay" not in preview_html
    assert "content-meta" not in preview_html
    assert "iphone-air" not in preview_html
    assert "phone-status" not in preview_html
    assert "creator-bar" not in preview_html
    assert "channels-nav" not in preview_html
    assert "width: 479.25px" in preview_html
    assert "height: 852px" in preview_html
    assert "border: 0" in preview_html
    assert "box-shadow: 0 0 0 14px #1a1a1a, 0 0 0 16px #333" in preview_html
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
