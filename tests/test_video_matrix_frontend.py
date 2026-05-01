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
    assert html.index('class="video-template-workbench"') < html.index('class="ending-workbench cover-workbench"') < html.index('class="side-editor"')
    assert "transcriptFile" not in html
    assert "transcriptText" in html
    assert "generationConfirmModal" in html
    assert "片尾封面模板" in html
    assert "endingTemplatePreview" in html
    assert "endingAssetPreview" in html
    assert "endingTemplateForm" in html
    assert "openEndingTemplateDir" in html
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
    assert "renderEndingTemplatePanel" in app
    assert "endingTemplateModeOptions" in app
    assert "ending_template_dir" in app
    assert "ending_templates" in app
    assert "ending_template_mode" in app
    assert "ending_template_id" in app
    assert "ending_template_ids" in app
    assert "ending_template_dir" in app
    assert "endingRandomMaterialHtml" in app
    assert "data-ending-template-choice" in app
    assert "data-ending-play" not in app
    assert "data-ending-stop" not in app
    assert "data-ending-preview-toggle" in app
    assert "endingPreviewToggleButtonHtml" in app
    assert "endingPreviewOverrideName" in app
    assert "endingFollowText" not in app
    ending_panel_source = app[app.index("function renderEndingTemplatePanel"):app.index("function isIndependentCover")]
    assert "一句话视频描述" not in ending_panel_source
    assert '片尾文案<textarea data-ending-cover-key="single_cover_title_text"' in app
    assert 'if (key === "single_cover_title_text")' in app
    assert 'event.source === $("endingTemplatePreview")?.contentWindow ? "ending" : "cover"' in app
    assert "从 video_matrix\\\\ending_template 勾选备用片尾素材" in app
    assert ".ending-material-list" in css
    assert ".ending-material-row" in css
    assert ".ending-material-row button" in css
    assert ".ending-preview-toggle svg" in css
    assert ".ending-preview-toggle.active" in css
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
    assert '<button type="button" id="saveCover">保存当前模板</button>' in app
    assert "saveCurrentCoverTemplate" in app
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
    assert "切换第一屏模板..." in app
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
    assert '.vm-template-editable.active[data-template-target="sloganBar"]' in preview
    assert '.vm-template-editable.active[data-template-target="titleBar"]' in preview
    assert '.vm-template-editable.active[data-template-target="hud"]' in preview
    assert '.vm-template-editable.active[data-template-target="hudBar"]' in preview
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
    assert "z-index: 80" in css
    assert "body.bgm-modal-open .sidebar {\n  z-index: 120;\n}" in css
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
    assert 'target === "hud" && template[alignKey] ? "0px" : designX(x)' in preview
    assert 'target === "hud" && template[alignKey] ? "100%" : ""' in preview
    assert "文字调整区" in app
    assert "HUD调整区" in app
    assert 'data-value="hudBar" title="选择 HUD 背景">HUD背景' not in app
    assert 'data-visual-command="opacity"' in app
    assert "videoTextFontOptions" in app
    assert "爆款粗黑" in app
    assert "招牌漫画" in app
    assert "数据机甲" in app
    assert "中文黑体冲击" in app
    assert "English Neon Bold" in app
    assert "English Pop Comic" in app
    assert "textEffectOptions" in app
    assert 'data-visual-command="text-effect"' in app
    assert "文字动效" in app
    assert 'data-visual-command="hud-bg-color"' in app
    assert 'aria-label="HUD 背景色"' in app
    assert 'data-visual-command="hud-radius"' in app
    assert 'data-visual-command="hud-radius" type="range" min="0" max="100"' in app
    assert "setBackgroundOpacity(value)" in preview
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
    assert "@keyframes vmTextGlow" in preview
    assert "@keyframes vmTextType" in preview
    assert ".visual-effect-control" in css
    assert "backgroundTargetForControl(activeTarget)" not in preview
    assert 'const key = `${target}_font_size`' in preview
    assert 'if (!activeTarget && command === "align") selectTarget("title")' in preview
    assert "updates.hud_x = hudScreenAlignX(align)" in preview
    assert 'updates.hud_bar_x = hudBarScreenAlignX(align, Number(template.hud_bar_width || 1080))' in preview
    assert 'if (value === "center") return 540' in preview
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
    assert "selectionFrameTarget(target)" in preview_html
    assert "return target;" in preview_html
    assert "node.dataset.templateTarget === frameTarget" in preview_html
    assert ".vm-template-text.active" not in preview_html
    assert "slogan_bg_height" in preview_html
    assert "title_bg_height" in preview_html
    assert "isBackgroundTarget" in preview_html
    assert "coverProfileShell" in preview_html
    assert "cover-profile-mode" in preview_html
    assert "body.cover-profile-mode .phone-mockup" in preview_html
    assert "body.cover-single-mode .phone-mockup" in preview_html
    assert "border: 14px solid #1a1a1a" in preview_html
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
    assert "w-[393px] h-[852px]" in preview_html
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
