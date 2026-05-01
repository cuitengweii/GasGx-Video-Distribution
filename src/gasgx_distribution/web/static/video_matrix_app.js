const $ = (id) => document.getElementById(id);
let state = {};
let templates = {};
let coverTemplates = {};
let selectedCover = "";
let selectedVideoTemplate = "";
let settings = {};
let lastPreviewPath = "";
let bgmLibraryState = { local: [], directory: "", links: [], pixabay: [] };
let pendingTemplateSave = "";
let modelImages = [];
let selectedModelImageUrl = "";

const jobStepLabels = [
  ["queued", "任务排队"],
  ["ingestion", "整理素材"],
  ["hud", "准备数据字幕"],
  ["beat", "分析音乐节奏"],
  ["planning", "规划混剪方案"],
  ["render", "生成视频"],
  ["finalizing", "整理导出文件"],
  ["complete", "完成"],
];

const jobMessages = {
  queued: "任务已提交，正在等待开始。",
  ingestion: "正在读取并整理素材视频，请确认素材目录里有视频文件。",
  hud: "正在准备视频里的 HUD 数据和文字信息。",
  beat: "正在分析背景音乐节奏，用于卡点混剪。",
  planning: "正在规划每条视频的素材组合，避免重复。",
  render: "正在调用 FFmpeg 生成视频，这一步耗时最长。",
  finalizing: "正在整理导出的 MP4、封面和文案文件。",
  complete: "生成完成，可以到输出目录查看文件。",
  error: "生成失败，请按下方提示处理后重试。",
};

const coverFields = [
  ["name", "模板名称", "text"], ["brand", "品牌文字", "text"], ["eyebrow", "眉标文字", "text"], ["cta", "CTA 按钮文字", "text"],
  ["align", "文字对齐", "select"], ["brand_y", "品牌 Y", "range", 0, 420], ["headline_y", "主标题 Y", "range", 0, 1320],
  ["subhead_y", "副标题 Y", "range", 0, 1500], ["hud_y", "HUD Y", "range", 0, 1780], ["cta_y", "CTA Y", "range", 0, 1840],
  ["primary_color", "主文字色", "color"], ["secondary_color", "副文字色", "color"], ["accent_color", "强调色", "color"],
  ["tint_color", "背景罩色", "color"], ["gradient_color", "渐变色", "color"], ["panel_color", "HUD 面板色", "color"],
  ["tint_opacity", "背景罩透明度", "rangeFloat", 0, 1], ["gradient_opacity", "渐变透明度", "rangeFloat", 0, 1],
  ["panel_opacity", "HUD 面板透明度", "rangeFloat", 0, 1],
];
const videoTemplateFields = [
  ["name", "模板名称", "text"],
  ["show_slogan", "显示上标题", "checkbox"],
  ["show_title", "显示中标题", "checkbox"],
  ["show_hud", "显示下标题", "checkbox"],
];
const visualFontOptions = [
  ["'Microsoft YaHei', 'Noto Sans SC', sans-serif", "雅黑黑体"],
  ["'Microsoft JhengHei', 'Microsoft YaHei', sans-serif", "广告黑体"],
  ["'Arial Black', Impact, sans-serif", "重磅标题"],
  ["Impact, 'Arial Black', sans-serif", "冲击海报"],
  ["'Bahnschrift Condensed', 'Arial Narrow', sans-serif", "窄体工业"],
  ["'Trebuchet MS', 'Microsoft YaHei', sans-serif", "现代圆体"],
  ["'Segoe UI Black', 'Arial Black', sans-serif", "科技粗体"],
  ["'Franklin Gothic Heavy', 'Arial Black', sans-serif", "商业粗体"],
  ["Georgia, 'Times New Roman', serif", "高级衬线"],
  ["'Courier New', Consolas, monospace", "数据等宽"],
];
const videoTextFontOptions = [
  ["'Arial Black', Impact, 'Microsoft YaHei', sans-serif", "爆款粗黑"],
  ["Impact, 'Arial Black', 'Microsoft YaHei', sans-serif", "冲击海报"],
  ["'Segoe UI Black', 'Arial Black', 'Microsoft YaHei', sans-serif", "霓虹重磅"],
  ["'Bahnschrift Condensed', 'Arial Narrow', 'Microsoft YaHei', sans-serif", "压缩工业"],
  ["'Franklin Gothic Heavy', 'Arial Black', 'Microsoft YaHei', sans-serif", "硬核广告"],
  ["'Cooper Black', Georgia, 'Microsoft YaHei', serif", "复古胖字"],
  ["'Showcard Gothic', 'Arial Black', 'Microsoft YaHei', sans-serif", "招牌漫画"],
  ["'Courier New', Consolas, 'Microsoft YaHei', monospace", "数据机甲"],
  ["'Microsoft YaHei UI', 'Noto Sans SC', sans-serif", "中文雅黑海报"],
  ["SimHei, 'Microsoft YaHei', sans-serif", "中文黑体冲击"],
  ["SimSun, 'Microsoft YaHei', serif", "中文宋体刊头"],
  ["'Noto Sans SC', 'Microsoft YaHei', sans-serif", "中文大屏粗体"],
  ["'Microsoft JhengHei', 'Microsoft YaHei', sans-serif", "中文广告黑体"],
  ["'Trebuchet MS', 'Microsoft YaHei', sans-serif", "英文圆体科技"],
  ["'Arial Narrow', 'Bahnschrift Condensed', 'Microsoft YaHei', sans-serif", "English Condensed"],
  ["'Segoe UI Black', Impact, 'Microsoft YaHei', sans-serif", "English Neon Bold"],
  ["'Franklin Gothic Heavy', Impact, 'Microsoft YaHei', sans-serif", "English Ad Heavy"],
  ["Georgia, 'Times New Roman', 'Microsoft YaHei', serif", "English Serif Luxe"],
  ["'Lucida Console', 'Courier New', 'Microsoft YaHei', monospace", "English Data Mono"],
  ["'Comic Sans MS', 'Arial Black', 'Microsoft YaHei', sans-serif", "English Pop Comic"],
];
const textEffectOptions = [
  ["none", "无动效"],
  ["pulse", "呼吸放大"],
  ["glow", "霓虹闪光"],
  ["slide-up", "上浮入场"],
  ["shake", "轻微震动"],
  ["typewriter", "打字机"],
  ["pop", "弹跳强调"],
];
const coverMaskModeOptions = [
  ["none", "无蒙版"],
  ["top_gradient", "上渐变蒙版"],
  ["bottom_gradient", "下渐变蒙版"],
  ["full", "全蒙版"],
];

async function api(path, options = {}) {
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function loadingInline(label = "加载中...") {
  return `<div class="loading-inline"><span class="loading-spinner" aria-hidden="true"></span><span>${label}</span></div>`;
}

function buttonLoadingInline(label) {
  return `<span class="loading-spinner" aria-hidden="true"></span><span>${escapeHtml(label)}</span>`;
}

function setPanelLoading(id, label = "加载中...") {
  const node = $(id);
  if (node) node.innerHTML = loadingInline(label);
}

function setImageLoading(id, label = "生成预览中...") {
  const image = $(id);
  if (!image) return;
  const holder = image.closest(".preview-stage");
  holder?.classList.add("is-loading");
  holder?.setAttribute("data-loading-label", label);
}

function clearImageLoading(id) {
  const holder = $(id)?.closest(".preview-stage");
  holder?.classList.remove("is-loading");
  holder?.removeAttribute("data-loading-label");
}

function encodePreviewPayload(payload) {
  return btoa(unescape(encodeURIComponent(JSON.stringify(payload))));
}

function refreshPhonePreviewFrame(id, payload = null) {
  const frame = $(id);
  if (!frame) return;
  const url = payload
    ? `/static/video_matrix_preview.html?template=${encodeURIComponent(encodePreviewPayload(payload))}`
    : "/static/video_matrix_preview.html";
  if (frame.getAttribute("src") !== url) frame.src = url;
}

function setInitialLoading() {
  ["sourceDirs", "recentLimits", "compositionRows", "videoTemplateSelector", "videoTemplateForm", "coverForm", "bgmPanel"].forEach((id) => setPanelLoading(id));
  setPanelLoading("videoTemplateGallery", "加载正文模板...");
  setPanelLoading("coverGallery", "加载封面模板...");
  setImageLoading("videoTemplatePreview", "加载正文预览...");
  setImageLoading("coverPreview", "加载封面预览...");
}

async function init() {
  setInitialLoading();
  const data = await api("/api/video-matrix/state");
  state = data.ui_state; templates = data.templates; coverTemplates = data.cover_templates; settings = data.settings;
  selectedCover = state.cover_template_id || Object.keys(coverTemplates)[0];
  selectedVideoTemplate = state.template_id || Object.keys(templates)[0];
  renderSidebar(data);
  renderSource(data);
  renderComposition(data);
  renderTextSettings();
  renderVideoTemplateSelector();
  renderVideoTemplateEditor();
  renderCoverSelector();
  renderCoverEditor();
  await loadModelImages();
  await refreshAllPreviews();
}

function renderSidebar(data) {
  $("outputCount").value = state.output_count || 3;
  $("maxWorkers").value = state.max_workers || 3;
  $("videoDurationMax").value = state.video_duration_max || settings.video_duration_max || 12;
  syncNumber("outputCount");
  syncNumber("videoDurationMax");
  syncRange("maxWorkers");
  const outputRoot = state.output_root || settings.output_root;
  $("outputRoot").dataset.fullPath = outputRoot;
  $("outputRoot").title = outputRoot;
  $("outputRoot").value = shortPath(outputRoot);
  $("outputOptions").value = (state.output_options || ["mp4"])[0] || "mp4";
  $("outputOptions").onchange = scheduleStateSave;
  $("openOutput").onclick = () => openFolder(outputRootPath());
  renderRadio("languageGroup", "copy_language", [["zh", "中文"], ["en", "英文"], ["ru", "俄文"]], state.copy_language || "zh", scheduleStateSave);
  renderBgm(data);
  $("saveState").onclick = toggleBgmLibraryPopover;
  $("openBgmDir").onclick = () => openFolder(bgmLibraryState.directory);
  document.querySelector(".sidebar details summary")?.addEventListener("click", (event) => {
    event.preventDefault();
    toggleBgmLibraryPopover();
  });
}

function renderSource(data) {
  $("metricSources").textContent = Object.values(data.category_counts).reduce((a, b) => a + b, 0);
  $("metricCount").textContent = $("outputCount").value;
  $("metricWorkers").textContent = $("maxWorkers").value;
  const categories = materialCategories(data);
  const activeCategoryIds = activeCategories(categories);
  $("sourceDirs").innerHTML = categories.map((category) =>
    `<div class="dir-row"><label class="category-toggle"><input type="checkbox" data-category-id="${escapeHtml(category.id)}" ${activeCategoryIds.includes(category.id) ? "checked" : ""}><span class="badge">${escapeHtml(category.label)}</span></label><code title="${escapeHtml(data.source_dirs[category.id] || "")}">${escapeHtml(shortPath(data.source_dirs[category.id] || ""))}</code><button data-path="${escapeHtml(data.source_dirs[category.id] || "")}">打开</button></div>`).join("");
  $("sourceDirs").querySelectorAll("button").forEach((btn) => btn.onclick = () => openFolder(btn.dataset.path));
  $("sourceDirs").querySelectorAll("[data-category-id]").forEach((input) => input.onchange = () => {
    state.active_category_ids = selectedActiveCategoryIds(categories);
    if (!state.composition_customized) {
      state.composition_sequence = defaultCompositionSequence(categories);
      renderComposition(data);
    }
    updateRecentLimitVisibility(categories);
    saveState();
  });
  $("addCategory").onclick = addMaterialCategory;
  $("sourceCounts").textContent = "算法：按视频碎片分类目录读取素材，优先取每类最新文件，再按节奏窗口自动组合混剪。";
  renderRadio("sourceModeGroup", "source_mode", [["Category folders", "智能分类轮换算法"]], state.source_mode || "Category folders", () => {
    updateSourceMode();
    scheduleStateSave();
  });
  updateRecentLimitVisibility(categories);
  updateSourceMode();
}

async function addMaterialCategory() {
  const input = $("newCategoryLabel");
  const label = input.value.trim();
  if (!label) {
    log("请先输入目录名称。");
    return;
  }
  await api("/api/video-matrix/material-categories", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({label})});
  input.value = "";
  const data = await api("/api/video-matrix/state");
  state = data.ui_state; settings = data.settings;
  renderSource(data);
  renderComposition(data);
  log(`已添加素材目录：${label}`);
}

function renderComposition(data = { settings }) {
  const categories = materialCategories(data);
  const savedRows = Array.isArray(state.composition_sequence) && state.composition_sequence.length
    ? normalizeCompositionSequence(state.composition_sequence)
    : [];
  const rows = savedRows.length ? savedRows : defaultCompositionSequence(categories);
  state.composition_sequence = rows;
  const recentLimits = state.recent_limits || settings.recent_limits || {};
  $("compositionRows").innerHTML = rows.map((row, index) => {
    const options = categories.map((category) =>
      `<option value="${escapeHtml(category.id)}" ${category.id === row.category_id ? "selected" : ""}>${escapeHtml(category.label)} / ${escapeHtml(category.id)}</option>`
    ).join("");
    const limit = clamp(Number(recentLimits[row.category_id] || settings.recent_limits?.[row.category_id] || 8), 1, 10);
    return `
      <div class="composition-row" data-index="${index}">
        <span>${index + 1}</span>
        <select data-composition-category>${options}</select>
        <input data-composition-duration type="number" min="0.2" max="12" step="0.1" value="${Number(row.duration || 1).toFixed(1)}" />
        <input data-composition-limit list="recentLimitOptions" type="text" inputmode="numeric" pattern="[1-9]|10" value="${limit}" placeholder="1-10" title="最新素材数量" aria-label="最新素材数量" />
        <button type="button" data-composition-remove>删除</button>
      </div>`;
  }).join("");
  $("compositionRows").querySelectorAll(".composition-row").forEach((row) => {
    row.querySelector("[data-composition-category]").onchange = () => {
      updateCompositionState(true);
      renderComposition(data);
      scheduleStateSave();
    };
    row.querySelector("[data-composition-duration]").oninput = () => {
      updateCompositionState(true);
      scheduleStateSave();
    };
    row.querySelector("[data-composition-limit]").oninput = () => {
      updateRecentLimitFromRow(row);
      scheduleStateSave();
    };
    row.querySelector("[data-composition-limit]").onchange = () => {
      updateRecentLimitFromRow(row);
      renderComposition(data);
      scheduleStateSave();
    };
    row.querySelector("[data-composition-remove]").onclick = () => removeCompositionRow(Number(row.dataset.index));
  });
  $("addCompositionRow").onclick = addCompositionRow;
}

function updateRecentLimitFromRow(row) {
  const categoryId = row.querySelector("[data-composition-category]")?.value;
  const input = row.querySelector("[data-composition-limit]");
  if (!categoryId || !input) return;
  const value = clamp(Number(input.value || 1), 1, 10);
  input.value = String(value);
  state.recent_limits = { ...(state.recent_limits || {}) };
  state.recent_limits[categoryId] = value;
}

function compositionSequence() {
  const source = Array.isArray(state.composition_sequence) && state.composition_sequence.length
    ? state.composition_sequence
    : settings.composition_sequence;
  return normalizeCompositionSequence(source);
}

function defaultCompositionSequence(categories) {
  const selected = activeCategories(categories);
  if (selected.length) {
    return selected.map((categoryId) => ({category_id: categoryId, duration: defaultDurationForCategory(categoryId)}));
  }
  const fallback = settings.composition_sequence || [
    {category_id: "category_A", duration: 1.5},
    {category_id: "category_B", duration: 3.4},
    {category_id: "category_A", duration: 1.5},
    {category_id: "category_C", duration: 3.0},
  ];
  const available = new Set(categories.map((category) => category.id));
  return normalizeCompositionSequence(fallback).filter((row) => available.has(row.category_id));
}

function defaultDurationForCategory(categoryId) {
  const existing = normalizeCompositionSequence(settings.composition_sequence)
    .find((row) => row.category_id === categoryId);
  return existing?.duration || 2.0;
}

function normalizeCompositionSequence(source) {
  return (Array.isArray(source) ? source : [])
    .map((row) => ({category_id: String(row.category_id || "").trim(), duration: Number(row.duration || 0)}))
    .filter((row) => row.category_id && row.duration > 0);
}

function updateCompositionState(markCustomized = false) {
  if (markCustomized) state.composition_customized = true;
  state.composition_sequence = [...document.querySelectorAll(".composition-row")].map((row) => ({
    category_id: row.querySelector("[data-composition-category]").value,
    duration: Number(row.querySelector("[data-composition-duration]").value || 1),
  })).filter((row) => row.category_id && row.duration > 0);
}

function addCompositionRow() {
  updateCompositionState();
  state.composition_customized = true;
  const categories = materialCategories({ settings });
  const category = categories[0]?.id || "category_A";
  state.composition_sequence.push({category_id: category, duration: 2.0});
  renderComposition({ settings });
  saveState();
}

function removeCompositionRow(index) {
  updateCompositionState();
  if (state.composition_sequence.length <= 1) {
    log("生成结构至少保留 1 个片段。");
    return;
  }
  state.composition_sequence.splice(index, 1);
  state.composition_customized = true;
  renderComposition({ settings });
  saveState();
}

function renderTextSettings() {
  $("headline").value = state.headline || "";
  $("subhead").value = state.subhead || "";
  $("cta").value = state.cta || "";
  $("followText").value = state.follow_text || "";
  $("hudText").value = state.hud_text || "";
  $("transcriptText").value = state.transcript_text || "";
  ["headline", "subhead", "cta", "followText", "hudText", "transcriptText"].forEach((id) => {
    $(id).addEventListener("input", scheduleStateSave);
  });
  ["headline", "subhead", "cta", "hudText"].forEach((id) => $(id).addEventListener("input", debounce(refreshAllPreviews, 250)));
  $("generateBtn").onclick = generate;
}

function renderCoverSelector() {
  if (!$("coverSelector")) return;
  $("coverSelector").innerHTML = Object.entries(coverTemplates).map(([id, item]) =>
    `<button class="${id === selectedCover ? "active" : ""}" data-id="${id}">${item.name || id}</button>`).join("");
  $("coverSelector").querySelectorAll("button").forEach((btn) => btn.onclick = async () => {
    selectedCover = btn.dataset.id;
    renderCoverSelector(); renderCoverEditor(); await saveTemplateSelection(); await refreshAllPreviews();
  });
}

function renderVideoTemplateSelector() {
  $("videoTemplateSelector").innerHTML = Object.entries(templates).map(([id, item]) =>
    `<button class="${id === selectedVideoTemplate ? "active" : ""}" data-id="${id}">${item.name || id}</button>`).join("");
  $("videoTemplateSelector").querySelectorAll("button").forEach((btn) => btn.onclick = async () => {
    await selectVideoTemplate(btn.dataset.id, { refreshGallery: false });
  });
}

async function selectVideoTemplate(templateId, options = {}) {
  if (!templateId || !templates[templateId]) return;
  const refreshGallery = options.refreshGallery !== false;
  selectedVideoTemplate = templateId;
  setImageLoading("videoTemplatePreview", "切换正文模板...");
  if (refreshGallery) setPanelLoading("videoTemplateGallery", "切换正文模板...");
  renderVideoTemplateSelector();
  renderVideoTemplateEditor();
  await saveTemplateSelection();
  await refreshVideoTemplatePreview();
  if (refreshGallery) await refreshVideoTemplateGallery();
}

function renderCoverEditor() {
  const t = coverTemplates[selectedCover];
  $("previewCaption").textContent = `${selectedCover} / ${t.name || selectedCover}`;
  const maskModeOptions = coverMaskModeOptions.map(([value, label]) =>
    `<option value="${value}" ${value === coverTemplateValue(t, "mask_mode", "bottom_gradient") ? "selected" : ""}>${label}</option>`
  ).join("");
  const html = [`<h3>可视化调整</h3>`, coverVisualToolbarHtml(t), `
    <p class="visual-editor-hint">点击预览里的文字或按钮后拖动定位；工具栏可调整字号、颜色、对齐和文字内容。</p>
    <label>模板名称<input data-key="name" type="text" value="${escapeHtml(t.name || "")}"></label>
    <label>蒙版类型<select data-key="mask_mode">${maskModeOptions}</select></label>
    <label>蒙版颜色<input data-key="mask_color" type="color" value="${escapeHtml(coverTemplateValue(t, "mask_color", t.gradient_color || t.tint_color || "#071015"))}"></label>
    ${rangeControlHtml({key: "mask_opacity", label: "蒙版透明度", min: 0, max: 1, step: 0.01, value: coverTemplateValue(t, "mask_opacity", t.gradient_opacity ?? t.tint_opacity ?? 0.35), className: "cover-template-control"})}
    <label>九宫格 Logo文字<input data-key="tile_brand_text" type="text" value="${escapeHtml(coverTemplateValue(t, "tile_brand_text", "GasGx"))}"></label>
    <label>九宫格 Slogan文字<input data-key="tile_tagline_text" type="text" value="${escapeHtml(coverTemplateValue(t, "tile_tagline_text", "终结废气 | 重塑能源 | 就地变现"))}"></label>
    <label>九宫格 一句话视频描述<textarea data-key="tile_titles_text" rows="5">${escapeHtml(coverTemplateValue(t, "tile_titles_text", defaultCoverTileTitles().join("\n")))}</textarea></label>
    <div class="template-actions cover-template-actions">
      <button type="button" id="saveCover">保存并重建模板库</button>
    </div>`];
  $("coverForm").innerHTML = html.join("");
  $("coverForm").querySelectorAll("input[data-key], select[data-key], textarea[data-key]").forEach((input) => {
    input.value = t[input.dataset.key] ?? input.value;
    if (input.classList.contains("control-number")) return;
    input.oninput = () => updateCoverTemplateField(input);
    input.onchange = () => updateCoverTemplateField(input);
  });
  $("coverForm").querySelectorAll(".cover-template-control[data-key]").forEach((control) => {
    bindRangeControl(control.dataset.key, () => updateCoverTemplateField(control.querySelector('input[type="range"]')));
  });
  bindCoverVisualToolbar();
  $("saveCover").onclick = saveCoverAsNewTemplate;
}

function coverTemplateValue(template, key, fallback = "") {
  return template[key] ?? fallback;
}

function defaultCoverTileTitles() {
  return ["燃气发电机组并网测试", "油田伴生气资源再利用", "移动式算力中心部署", "野外发电设备日常维护", "零燃除：变废为宝", "集装箱数据中心内景", "高效燃气轮机运行状态", "夜间井场持续发电作业", "极寒环境设备启动测试"];
}

function updateCoverTemplateField(input) {
  const template = coverTemplates[selectedCover];
  if (!template || !input) return;
  const key = input.dataset.key;
  template[key] = input.type === "range" || input.type === "number" ? Number(input.value) : input.value;
  refreshAllPreviews();
  scheduleCoverTemplateSave();
}

function coverVisualToolbarHtml(template) {
  const fontValue = template.title_font_family || visualFontOptions[0][0];
  const fontOptions = visualFontOptions.map(([value, label]) =>
    `<option value="${escapeHtml(value)}" ${value === fontValue ? "selected" : ""}>${label}</option>`
  ).join("");
  return `
    <div class="visual-toolbar-panel cover-visual-toolbar" aria-label="第一屏封面可视化工具">
      <button type="button" data-cover-command="size-down" title="缩小字号">A-</button>
      <button type="button" data-cover-command="size-up" title="放大字号">A+</button>
      <button type="button" data-cover-command="edit" title="编辑文字">编辑</button>
      <button type="button" data-cover-command="align" data-value="left" title="左对齐">左齐</button>
      <button type="button" data-cover-command="align" data-value="center" title="居中对齐">居中</button>
      <button type="button" data-cover-command="align" data-value="right" title="右对齐">右齐</button>
      <select data-cover-command="font-family">${fontOptions}</select>
      <label class="color-swatch-button" title="文字颜色">
        <svg class="color-picker-icon" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 3a9 9 0 0 0 0 18h1.4a2 2 0 0 0 1.7-3l-.2-.4a1.7 1.7 0 0 1 1.5-2.6H18a6 6 0 0 0 0-12h-6Z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>
          <circle cx="7.5" cy="10" r="1.3" fill="currentColor"/>
          <circle cx="10.5" cy="6.8" r="1.3" fill="currentColor"/>
          <circle cx="15" cy="7.8" r="1.3" fill="currentColor"/>
          <circle cx="16.8" cy="11.5" r="1.3" fill="currentColor"/>
        </svg>
        <span class="color-current-dot" style="background:${escapeHtml(template.primary_color || "#ffffff")}"></span>
        <input data-cover-command="color" type="color" value="${escapeHtml(template.primary_color || "#ffffff")}" aria-label="文字颜色">
      </label>
    </div>`;
}

function bindCoverVisualToolbar() {
  const toolbar = $("coverForm").querySelector(".cover-visual-toolbar");
  if (!toolbar) return;
  toolbar.querySelectorAll("button[data-cover-command]").forEach((button) => {
    button.onclick = () => postCoverTemplateCommand(button.dataset.coverCommand, button.dataset.value || "");
  });
  toolbar.querySelectorAll("select[data-cover-command], input[data-cover-command]").forEach((input) => {
    input.oninput = () => {
      updateColorSwatch(input);
      postCoverTemplateCommand(input.dataset.coverCommand, input.value);
    };
    input.onchange = () => {
      updateColorSwatch(input);
      postCoverTemplateCommand(input.dataset.coverCommand, input.value);
    };
  });
}

function postCoverTemplateCommand(command, value = "") {
  $("coverPreview")?.contentWindow?.postMessage({
    type: "gasgx-cover-template-command",
    command,
    value,
  }, window.location.origin);
}

async function refreshAllPreviews() {
  await refreshVideoTemplatePreview();
  await refreshVideoTemplateGallery();
  await refreshMainPreview();
  await refreshGallery();
}

function renderVideoTemplateEditor() {
  const template = templates[selectedVideoTemplate];
  if (!template) return;
  const html = [`<h3>模板调整区</h3>`, visualTemplateToolbarHtml(template)];
  for (const [key, label, type, min, max] of videoTemplateFields) {
    const value = template[key] ?? "";
    if (type === "checkbox") {
      html.push(`<label class="check-row"><input data-key="${key}" type="checkbox" ${value ? "checked" : ""}><span>${label}</span></label>`);
    } else if (type === "select") {
      html.push(`<label>${label}<select data-key="${key}"><option value="left">左对齐</option><option value="center">居中</option><option value="right">右对齐</option></select></label>`);
    } else if (type === "range") {
      html.push(rangeControlHtml({key, label, min, max, value, className: "template-control"}));
    } else if (type === "rangeFloat") {
      html.push(rangeControlHtml({key, label, min, max, step: 0.01, value, className: "template-control"}));
    } else {
      html.push(`<label>${label}<input data-key="${key}" type="${type}" value="${escapeHtml(value)}"></label>`);
    }
  }
  html.push(`
    <div class="template-actions">
      <button type="button" id="saveVideoTemplate">保存当前</button>
      <button type="button" id="cloneVideoTemplate" class="secondary" title="基于当前正文模板克隆一个新模板">新建模板</button>
    </div>`);
  $("videoTemplateForm").innerHTML = html.join("");
  $("videoTemplateForm").querySelectorAll("input[data-key], select[data-key], textarea[data-key]").forEach((input) => {
    const key = input.dataset.key;
    if (input.type === "checkbox") input.checked = Boolean(template[key]);
    else input.value = template[key] ?? input.value;
    if (input.classList.contains("control-number")) return;
    input.oninput = () => updateVideoTemplateField(input);
    input.onchange = () => updateVideoTemplateField(input);
  });
  $("videoTemplateForm").querySelectorAll(".template-control[data-key]").forEach((control) => {
    bindRangeControl(control.dataset.key, () => updateVideoTemplateField(control.querySelector('input[type="range"]')));
  });
  bindVisualTemplateToolbar();
  $("saveVideoTemplate").onclick = saveVideoTemplate;
  $("cloneVideoTemplate").onclick = cloneVideoTemplate;
}

function visualTemplateToolbarHtml(template) {
  const fontValue = template.title_font_family || videoTextFontOptions[0][0];
  const effectValue = template.title_text_effect || "none";
  const hudOpacity = Number(template.hud_bar_opacity ?? 0.68);
  const hudRadius = Number(template.hud_bar_radius ?? 10);
  const hudColor = template.hud_bar_color || "#0E1A10";
  const fontOptions = videoTextFontOptions.map(([value, label]) =>
    `<option value="${escapeHtml(value)}" ${value === fontValue ? "selected" : ""}>${label}</option>`
  ).join("");
  const effectOptions = textEffectOptions.map(([value, label]) =>
    `<option value="${escapeHtml(value)}" ${value === effectValue ? "selected" : ""}>${label}</option>`
  ).join("");
  return `
    <div class="visual-toolbar-panel" aria-label="文字可视化工具">
      <div class="visual-control-section visual-text-controls" aria-label="文字调整区">
        <div class="visual-section-title">文字调整区</div>
        <div class="visual-target-tabs" aria-label="标题选择">
          <button type="button" data-visual-command="select-target" data-value="slogan" title="选择上标题文字">上标题</button>
          <button type="button" data-visual-command="select-target" data-value="title" title="选择中标题文字">中标题</button>
          <button type="button" data-visual-command="select-target" data-value="hud" title="选择下标题文字">下标题</button>
        </div>
        <button type="button" data-visual-command="size-down" title="缩小字号">A-</button>
        <button type="button" data-visual-command="size-up" title="放大字号">A+</button>
        <button type="button" data-visual-command="edit" title="编辑文字">编辑</button>
        <button type="button" data-visual-command="align" data-value="left" title="左对齐">左齐</button>
        <button type="button" data-visual-command="align" data-value="center" title="居中对齐">居中</button>
        <button type="button" data-visual-command="align" data-value="right" title="右对齐">右齐</button>
        <select data-visual-command="font-family">${fontOptions}</select>
        <label class="visual-effect-control">文字动效<select data-visual-command="text-effect">${effectOptions}</select></label>
        <label class="color-swatch-button" title="文字颜色">
          <svg class="color-picker-icon" viewBox="0 0 24 24" aria-hidden="true">
            <path d="M12 3a9 9 0 0 0 0 18h1.4a2 2 0 0 0 1.7-3l-.2-.4a1.7 1.7 0 0 1 1.5-2.6H18a6 6 0 0 0 0-12h-6Z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>
            <circle cx="7.5" cy="10" r="1.3" fill="currentColor"/>
            <circle cx="10.5" cy="6.8" r="1.3" fill="currentColor"/>
            <circle cx="15" cy="7.8" r="1.3" fill="currentColor"/>
            <circle cx="16.8" cy="11.5" r="1.3" fill="currentColor"/>
          </svg>
          <span class="color-current-dot" style="background:${escapeHtml(template.primary_color || "#ffffff")}"></span>
          <input data-visual-command="color" type="color" value="${escapeHtml(template.primary_color || "#ffffff")}" aria-label="文字颜色">
        </label>
      </div>
      <div class="visual-control-section visual-background-controls" aria-label="背景调整区">
        <div class="visual-section-title">背景调整区</div>
        <div class="visual-target-tabs" aria-label="背景选择">
          <button type="button" data-visual-command="select-target" data-value="sloganBar" title="选择上标题背景">上背景</button>
          <button type="button" data-visual-command="select-target" data-value="titleBar" title="选择中标题背景">中背景</button>
          <button type="button" data-visual-command="select-target" data-value="hudBar" title="选择 HUD 背景">HUD背景</button>
        </div>
        <button type="button" data-visual-command="width-down" title="缩小背景宽度">W-</button>
        <button type="button" data-visual-command="width-up" title="放大背景宽度">W+</button>
        <label class="color-swatch-button" title="HUD 背景色">
          <svg class="color-picker-icon" viewBox="0 0 24 24" aria-hidden="true">
            <rect x="4" y="7" width="16" height="10" rx="3" fill="none" stroke="currentColor" stroke-width="1.8"/>
            <path d="M8 12h8" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
          </svg>
          <span class="color-current-dot" style="background:${escapeHtml(hudColor)}"></span>
          <input data-visual-command="hud-bg-color" type="color" value="${escapeHtml(hudColor)}" aria-label="HUD 背景色">
        </label>
        <label class="visual-opacity-control">HUD 透明度<input data-visual-command="opacity" type="range" min="0" max="1" step="0.01" value="${escapeHtml(hudOpacity.toFixed(2))}"><output>${escapeHtml(hudOpacity.toFixed(2))}</output></label>
        <label class="visual-opacity-control">HUD 圆角<input data-visual-command="hud-radius" type="range" min="0" max="80" step="1" value="${escapeHtml(String(Math.round(hudRadius)))}"><output>${escapeHtml(String(Math.round(hudRadius)))}</output></label>
      </div>
    </div>`;
}

function bindVisualTemplateToolbar() {
  const toolbar = $("videoTemplateForm").querySelector(".visual-toolbar-panel");
  if (!toolbar) return;
  toolbar.querySelectorAll("button[data-visual-command]").forEach((button) => {
    button.onclick = () => postVisualTemplateCommand(button.dataset.visualCommand, button.dataset.value || "");
  });
  toolbar.querySelectorAll("select[data-visual-command], input[data-visual-command]").forEach((input) => {
    input.oninput = () => {
      updateColorSwatch(input);
      updateVisualOutput(input);
      postVisualTemplateCommand(input.dataset.visualCommand, input.value);
    };
    input.onchange = () => {
      updateColorSwatch(input);
      updateVisualOutput(input);
      postVisualTemplateCommand(input.dataset.visualCommand, input.value);
    };
  });
}

function updateVisualOutput(input) {
  const out = input.closest("label")?.querySelector("output");
  if (out) out.textContent = input.value;
}

function updateColorSwatch(input) {
  if (input?.dataset.visualCommand !== "color" && input?.dataset.visualCommand !== "hud-bg-color" && input?.dataset.coverCommand !== "color") return;
  const swatch = input.closest(".color-swatch-button")?.querySelector(".color-current-dot");
  if (swatch) swatch.style.background = input.value;
}

function postVisualTemplateCommand(command, value = "") {
  $("videoTemplatePreview")?.contentWindow?.postMessage({
    type: "gasgx-video-template-command",
    command,
    value,
  }, window.location.origin);
}

function updateVideoTemplateField(input) {
  const template = templates[selectedVideoTemplate];
  const key = input.dataset.key;
  if (input.type === "checkbox") template[key] = input.checked;
  else if (input.type === "range") template[key] = Number(input.value);
  else template[key] = input.value;
  const out = input.parentElement.querySelector("output");
  if (out) out.textContent = input.value;
  refreshVideoTemplatePreview();
  scheduleVideoTemplateSave();
}

function applyVisualTemplateUpdates(updates) {
  const template = templates[selectedVideoTemplate];
  if (!template || !updates) return;
  Object.assign(template, updates);
  Object.entries(updates).forEach(([key, value]) => {
    const input = $(`videoTemplateForm`)?.querySelector(`[data-key="${key}"]`);
    if (!input) return;
    input.value = value;
    const out = input.parentElement.querySelector("output");
    if (out) out.textContent = value;
  });
  scheduleVideoTemplateSave();
}

function applyVisualTextUpdates(text) {
  if (!text) return;
  const fieldMap = { slogan: "headline", title: "subhead", hud_text: "hudText" };
  Object.entries(text).forEach(([key, value]) => {
    const field = $(fieldMap[key]);
    if (field) field.value = value;
  });
  scheduleStateSave();
}

function applyCoverTemplateUpdates(updates) {
  const template = coverTemplates[selectedCover];
  if (!template || !updates) return;
  Object.assign(template, updates);
  Object.entries(updates).forEach(([key, value]) => {
    const input = $("coverForm")?.querySelector(`[data-key="${key}"]`);
    if (input) input.value = value;
  });
  scheduleCoverTemplateSave();
}

function applyCoverTextUpdates(text) {
  if (!text) return;
  const template = coverTemplates[selectedCover];
  const fieldMap = { headline: "headline" };
  Object.entries(text).forEach(([key, value]) => {
    if (key === "brand" || key === "eyebrow" || key === "subhead" || key === "cta") {
      if (template) template[`profile_${key}_text`] = value;
      return;
    }
    const field = $(fieldMap[key]);
    if (field) field.value = value;
  });
  scheduleCoverTemplateSave();
  scheduleStateSave();
}

async function loadModelImages() {
  try {
    const data = await api("/api/video-matrix/model-images");
    modelImages = data.images || [];
    selectedModelImageUrl = selectedModelImageUrl || modelImages[0]?.url || "";
  } catch {
    modelImages = [];
  }
  renderVideoTemplateBackgrounds();
}

function renderVideoTemplateBackgrounds() {
  const node = $("videoTemplateBackgrounds");
  if (!node) return;
  if (!modelImages.length) {
    node.innerHTML = `<span class="muted">modelimg 目录暂无可预览图片</span>`;
    return;
  }
  node.innerHTML = modelImages.map((image) => `
    <button class="model-image-chip ${image.url === selectedModelImageUrl ? "active" : ""}" type="button" data-model-image="${escapeHtml(image.url)}" title="${escapeHtml(image.name)}">
      <img src="${escapeHtml(image.url)}" alt="">
      <span>${escapeHtml(image.name)}</span>
    </button>
  `).join("");
  node.querySelectorAll("[data-model-image]").forEach((button) => {
    button.onclick = () => {
      selectedModelImageUrl = button.dataset.modelImage || "";
      renderVideoTemplateBackgrounds();
      refreshVideoTemplatePreview();
      refreshGallery();
    };
  });
}

async function refreshVideoTemplatePreview() {
  const template = templates[selectedVideoTemplate];
  if (!template) return;
  refreshPhonePreviewFrame("videoTemplatePreview", videoTemplatePreviewPayload(template));
  clearImageLoading("videoTemplatePreview");
}

async function refreshVideoTemplateGallery() {
  setPanelLoading("videoTemplateGallery", "生成正文模板列表...");
  const cards = [];
  for (const [id, template] of Object.entries(templates)) {
    const data = await api("/api/video-matrix/template-preview", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(videoTemplatePreviewPayload(template))});
    cards.push(`<div class="cover-card ${id === selectedVideoTemplate ? "active" : ""}" data-id="${id}"><img src="${data.data_url}"><span>${id} / ${template.name || id}</span></div>`);
  }
  $("videoTemplateGallery").innerHTML = cards.join("");
  $("videoTemplateGallery").querySelectorAll(".cover-card").forEach((card) => card.onclick = async () => {
    await selectVideoTemplate(card.dataset.id);
  });
}

function videoTemplatePreviewPayload(template) {
  return {
    template,
    slogan: $("headline").value,
    title: $("subhead").value,
    hud_text: $("hudText").value,
    background_image_url: selectedModelImageUrl,
    show_template_mask: false,
  };
}

async function saveVideoTemplate() {
  const button = $("saveVideoTemplate");
  const label = button?.textContent || "保存当前";
  if (button) {
    button.disabled = true;
    button.textContent = "保存中...";
  }
  try {
    await api(`/api/video-matrix/templates/${selectedVideoTemplate}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(templates[selectedVideoTemplate])});
    await saveState();
    pendingTemplateSave = "";
    log(`已保存正文模板：${templates[selectedVideoTemplate].name || selectedVideoTemplate}`);
    renderVideoTemplateSelector();
    renderVideoTemplateEditor();
    showTemplateActionStatus("保存成功");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.textContent = label;
    }
    log(`正文模板保存失败：${error.message}`);
  }
}

async function cloneVideoTemplate() {
  const button = $("cloneVideoTemplate");
  const label = button?.textContent || "新建模板";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("新建中...");
  }
  const sourceTemplate = templates[selectedVideoTemplate];
  if (!sourceTemplate) {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    return;
  }
  const nextId = nextTemplateCloneId(selectedVideoTemplate, templates);
  const nextName = `${sourceTemplate.name || selectedVideoTemplate} Copy`;
  try {
    templates[nextId] = {...JSON.parse(JSON.stringify(sourceTemplate)), name: nextName};
    selectedVideoTemplate = nextId;
    state.template_id = nextId;
    await api(`/api/video-matrix/templates/${nextId}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(templates[nextId])});
    await saveTemplateSelection();
    renderVideoTemplateSelector();
    renderVideoTemplateEditor();
    await refreshVideoTemplatePreview();
    await refreshVideoTemplateGallery();
    log(`已基于当前正文模板新建：${nextName}`);
    showTemplateActionStatus("新建模板成功");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    log(`正文模板新建失败：${error.message}`);
  }
}

function showTemplateActionStatus(message) {
  const actions = $("videoTemplateForm")?.querySelector(".template-actions");
  if (!actions) return;
  let status = actions.querySelector(".template-action-status");
  if (!status) {
    status = document.createElement("div");
    status.className = "template-action-status";
    actions.appendChild(status);
  }
  status.textContent = message;
  status.hidden = false;
  window.clearTimeout(showTemplateActionStatus.timer);
  showTemplateActionStatus.timer = window.setTimeout(() => {
    status.hidden = true;
  }, 2200);
}

function nextTemplateCloneId(sourceId, templateMap) {
  const base = `${String(sourceId || "template").replace(/_copy(?:_\d+)?$/i, "")}_copy`;
  if (!templateMap[base]) return base;
  let index = 2;
  while (templateMap[`${base}_${index}`]) index += 1;
  return `${base}_${index}`;
}

function nextCoverTemplateMeta(templateMap) {
  let next = 1;
  Object.entries(templateMap || {}).forEach(([id, template]) => {
    const idMatch = String(id).match(/^cover_template_(\d+)$/);
    const nameMatch = String(template?.name || "").match(/^第一屏封面模板\s*(\d+)$/);
    const value = Math.max(Number(idMatch?.[1] || 0), Number(nameMatch?.[1] || 0));
    if (value >= next) next = value + 1;
  });
  const serial = String(next).padStart(2, "0");
  return { id: `cover_template_${serial}`, name: `第一屏封面模板 ${serial}` };
}

async function refreshMainPreview() {
  const template = coverTemplates[selectedCover];
  if (!template) return;
  refreshPhonePreviewFrame("coverPreview", {
    template,
    cover_mode: true,
    slogan: $("headline").value,
    title: $("subhead").value,
    headline: $("headline").value,
    subhead: $("subhead").value,
    cta: $("cta").value || template.cta,
    hud_text: $("hudText").value,
    background_image_url: modelImages[0]?.url || "",
    background_image_urls: modelImages.map((image) => image.url).filter(Boolean),
    show_template_mask: true,
  });
  clearImageLoading("coverPreview");
}

async function refreshGallery() {
  setPanelLoading("coverGallery", "生成封面模板列表...");
  const cards = [];
  for (const [id, t] of Object.entries(coverTemplates)) {
    const data = await api("/api/video-matrix/cover-preview", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(previewPayload(t))});
    cards.push(`<div class="cover-card ${id === selectedCover ? "active" : ""}" data-id="${id}"><img src="${data.data_url}"><span>${id} / ${t.name || id}</span></div>`);
  }
  $("coverGallery").innerHTML = cards.join("");
  $("coverGallery").querySelectorAll(".cover-card").forEach((card) => card.onclick = async () => {
    await selectCoverTemplate(card.dataset.id);
  });
}

async function selectCoverTemplate(templateId) {
  if (!templateId || !coverTemplates[templateId]) return;
  selectedCover = templateId;
  setImageLoading("coverPreview", "切换第一屏模板...");
  renderCoverSelector();
  renderCoverEditor();
  await saveTemplateSelection();
  await refreshMainPreview();
  await refreshGallery();
}

function previewPayload(template) {
  const payload = {...template};
  if ($("cta").value) payload.cta = $("cta").value;
  return {template: payload, cover_mode: true, slogan: $("headline").value, title: $("subhead").value, headline: $("headline").value, subhead: $("subhead").value, cta: $("cta").value || payload.cta, hud_text: $("hudText").value, background_image_url: selectedModelImageUrl || modelImages[0]?.url || "", background_image_urls: modelImages.map((image) => image.url).filter(Boolean)};
}

async function saveCoverAsNewTemplate() {
  const sourceTemplate = coverTemplates[selectedCover];
  if (!sourceTemplate) return;
  const previousCover = selectedCover;
  const previousTemplates = {...coverTemplates};
  const button = $("saveCover");
  const label = button?.textContent || "保存并重建模板库";
  if (button) {
    button.disabled = true;
    button.textContent = "保存中...";
  }
  coverTemplates = buildCoverTemplateVariants(sourceTemplate);
  selectedCover = "cover_template_01";
  state.cover_template_id = selectedCover;
  state.cover_templates = coverTemplates;
  try {
    await api("/api/video-matrix/cover-templates", {method:"PUT", headers:{"Content-Type":"application/json"}, body: JSON.stringify({templates: coverTemplates, selected_cover: selectedCover})});
    await saveTemplateSelection();
    pendingTemplateSave = "";
    renderCoverSelector();
    renderCoverEditor();
    await refreshMainPreview();
    await refreshGallery();
    log("已自动生成 9 组九宫格图片模板");
  } catch (error) {
    coverTemplates = previousTemplates;
    selectedCover = previousCover;
    state.cover_template_id = previousCover;
    state.cover_templates = previousTemplates;
    if (button) {
      button.disabled = false;
      button.textContent = label;
    }
    log(`第一屏新模板保存失败：${error.message}`);
  }
}

function buildCoverTemplateVariants(sourceTemplate) {
  const source = JSON.parse(JSON.stringify(sourceTemplate || {}));
  const variants = [
    ["none", "#000000", 0, "left", 8, 12],
    ["top_gradient", "#071015", 0.42, "center", 8, 12],
    ["bottom_gradient", "#0E1A10", 0.52, "left", 64, 14],
    ["full", "#143E72", 0.32, "center", 72, 14],
    ["top_gradient", "#5DD62C", 0.28, "right", 10, 12],
    ["bottom_gradient", "#071015", 0.62, "center", 104, 13],
    ["full", "#10130D", 0.46, "left", 34, 15],
    ["top_gradient", "#00A3FF", 0.24, "left", 96, 13],
    ["bottom_gradient", "#FF9900", 0.28, "right", 72, 14],
  ];
  return Object.fromEntries(variants.map(([maskMode, maskColor, maskOpacity, textAlign, copyY, titleSize], index) => {
    const serial = String(index + 1).padStart(2, "0");
    return [`cover_template_${serial}`, {
      ...source,
      name: `九宫格图片模板 ${serial}`,
      mask_mode: maskMode,
      mask_color: maskColor,
      mask_opacity: maskOpacity,
      tile_text_align: textAlign,
      tile_copy_y: copyY,
      tile_title_font_size: titleSize,
    }];
  }));
}

const scheduleVideoTemplateSave = debounce(async () => {
  if (!selectedVideoTemplate || !templates[selectedVideoTemplate]) return;
  const templateId = selectedVideoTemplate;
  pendingTemplateSave = `video:${templateId}`;
  try {
    await api(`/api/video-matrix/templates/${templateId}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(templates[templateId])});
    if (pendingTemplateSave === `video:${templateId}`) pendingTemplateSave = "";
    renderVideoTemplateSelector();
  } catch (error) {
    log(`正文模板自动保存失败：${error.message}`);
  }
}, 700);

const scheduleCoverTemplateSave = debounce(async () => {
  if (!selectedCover || !coverTemplates[selectedCover]) return;
  const templateId = selectedCover;
  pendingTemplateSave = `cover:${templateId}`;
  try {
    await api(`/api/video-matrix/cover-templates/${templateId}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(coverTemplates[templateId])});
    if (pendingTemplateSave === `cover:${templateId}`) pendingTemplateSave = "";
    renderCoverSelector();
  } catch (error) {
    log(`第一屏模板自动保存失败：${error.message}`);
  }
}, 700);

async function saveTemplateSelection() {
  state = collectState();
  await api("/api/video-matrix/state", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(state)});
}

async function saveState() {
  state = collectState();
  await api("/api/video-matrix/state", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(state)});
  log("当前设置已保存");
}

const scheduleStateSave = debounce(async () => {
  state = collectState();
  await api("/api/video-matrix/state", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(state)});
}, 500);

async function generate() {
  const button = $("generateBtn");
  if (lastPreviewPath && button.dataset.mode === "preview") {
    window.open(`/static/video_matrix_preview.html?path=${encodeURIComponent(lastPreviewPath)}`, "_blank", "noopener");
    return;
  }
  lastPreviewPath = "";
  button.dataset.mode = "generate";
  try {
    const statePayload = collectState();
    if (!(await confirmGeneration(statePayload))) return;
    button.disabled = true;
    button.textContent = "提交中...";
    updateJobStatus({ status: "queued", stage: "queued", progress: 0, message: "正在提交生成任务..." });
    if (!bgmLibraryState.local.length) {
      throw new Error("本地背景音乐库还没有可用 MP3。请把 MP3 文件放入左侧问号提示里的目录，然后刷新页面。");
    }
    const form = new FormData();
    form.append("payload", JSON.stringify(statePayload));
    [...($("sourceFiles")?.files || [])].forEach((file) => form.append("source_files", file));
    const {job_id} = await api("/api/video-matrix/generate", {method:"POST", body: form});
    updateJobStatus({ status: "queued", stage: "queued", progress: 0.02, message: `任务已提交：${job_id}` });
    pollJob(job_id);
  } catch (error) {
    updateJobStatus({ status: "error", stage: "error", progress: 0, message: error.message, error: error.message });
  } finally {
    button.disabled = false;
    if (!lastPreviewPath) button.textContent = "立即生成";
  }
}

async function pollJob(jobId) {
  const job = await api(`/api/video-matrix/jobs/${jobId}`);
  updateJobStatus(job);
  if (job.status === "complete") {
    lastPreviewPath = job.assets?.[0]?.video_path || "";
    updateJobStatus({...job, message: `生成完成，已导出 ${job.assets.length} 条视频。点击下方按钮可预览第一条视频在视频号里的展示效果。`});
    const button = $("generateBtn");
    if (lastPreviewPath) {
      button.dataset.mode = "preview";
      button.textContent = "预览视频";
    }
  } else if (job.status !== "error") setTimeout(() => pollJob(jobId), 1200);
}

function confirmGeneration(statePayload) {
  const modal = $("generationConfirmModal");
  $("generationConfirmBody").innerHTML = generationConfirmHtml(statePayload);
  modal.classList.remove("hidden");
  document.body.classList.add("confirm-modal-open");
  return new Promise((resolve) => {
    const close = (confirmed) => {
      modal.classList.add("hidden");
      document.body.classList.remove("confirm-modal-open");
      $("confirmSubmit").onclick = null;
      $("confirmCancel").onclick = null;
      $("confirmClose").onclick = null;
      resolve(confirmed);
    };
    $("confirmSubmit").onclick = () => close(true);
    $("confirmCancel").onclick = () => close(false);
    $("confirmClose").onclick = () => close(false);
  });
}

function generationConfirmHtml(statePayload) {
  const categories = materialCategories({ settings });
  const categoryNames = Object.fromEntries(categories.map((category) => [category.id, category.label]));
  const active = statePayload.active_category_ids || [];
  const activeRows = active.length
    ? active.map((id) => `<tr><td>${escapeHtml(categoryNames[id] || id)}</td><td>${escapeHtml(id)}</td><td>${statePayload.recent_limits?.[id] || 0}</td></tr>`).join("")
    : `<tr><td colspan="3">未选择素材分类</td></tr>`;
  const compositionRows = (statePayload.composition_sequence || []).map((row, index) =>
    `<tr><td>${index + 1}</td><td>${escapeHtml(categoryNames[row.category_id] || row.category_id)}</td><td>${escapeHtml(row.category_id)}</td><td>${Number(row.duration || 0).toFixed(1)} 秒</td></tr>`
  ).join("") || `<tr><td colspan="4">未配置生成结构</td></tr>`;
  return `
    <div class="confirm-summary">
      <div><span>生成数量</span><strong>${statePayload.output_count}</strong></div>
      <div><span>并行线程</span><strong>${statePayload.max_workers}</strong></div>
      <div><span>最大节拍分析</span><strong>${statePayload.video_duration_max} 秒</strong></div>
      <div><span>输出格式</span><strong>${escapeHtml((statePayload.output_options || []).join(", "))}</strong></div>
    </div>
    <section>
      <h4>输出目录</h4>
      <code>${escapeHtml(statePayload.output_root)}</code>
    </section>
    <section>
      <h4>启用素材分类</h4>
      <table><thead><tr><th>分类</th><th>ID</th><th>最近素材</th></tr></thead><tbody>${activeRows}</tbody></table>
    </section>
    <section>
      <h4>生成结构</h4>
      <table><thead><tr><th>#</th><th>分类</th><th>ID</th><th>片段秒数</th></tr></thead><tbody>${compositionRows}</tbody></table>
    </section>
    <section class="confirm-algorithm">
      <h4>本次算法框架</h4>
      <ol>
        <li>按启用分类读取素材目录，每类最多取“最近素材”数量对应的新文件。</li>
        <li>将候选素材归一化为 1080:1920、60fps 的短视频片段库。</li>
        <li>按“生成结构”的分类顺序和片段秒数，为每条视频抽取不同素材片段。</li>
        <li>分析本地背景音乐节拍，把片段切换点尽量对齐节奏窗口。</li>
        <li>按当前模板、HUD 文本和文案参考资料并行渲染，导出到最终视频目录。</li>
      </ol>
    </section>
  `;
}

function collectState() {
  const categories = Array.isArray(settings.material_categories) ? settings.material_categories : [];
  updateCompositionState();
  return {
    output_count: Number($("outputCount").value), max_workers: Number($("maxWorkers").value),
    video_duration_max: Number($("videoDurationMax").value || settings.video_duration_max || 12),
    output_options: [$("outputOptions").value], output_root: outputRootPath(),
    template_id: selectedVideoTemplate, cover_template_id: selectedCover, copy_language: radioValue("copy_language"),
    source_mode: radioValue("source_mode") || "Category folders", use_live_data: true,
    headline: $("headline").value, subhead: $("subhead").value, cta: $("cta").value,
    follow_text: $("followText").value, hud_text: $("hudText").value,
    transcript_text: $("transcriptText").value,
    bgm_source: "Local library", bgm_library_id: "",
    composition_sequence: state.composition_sequence,
    composition_customized: Boolean(state.composition_customized),
    active_category_ids: selectedActiveCategoryIds(categories),
    recent_limits: Object.fromEntries(categories.map((category) => [
      category.id,
      clamp(Number(state.recent_limits?.[category.id] || settings.recent_limits?.[category.id] || 8), 1, 10),
    ]))
  };
}

function activeCategories(categories) {
  const saved = Array.isArray(state.active_category_ids) ? state.active_category_ids : [];
  return saved.length ? saved : categories.map((category) => category.id);
}

function selectedActiveCategoryIds(categories) {
  const selected = categories
    .map((category) => document.querySelector(`[data-category-id="${CSS.escape(category.id)}"]`))
    .filter((input) => input?.checked)
    .map((input) => input.dataset.categoryId);
  return selected;
}

function updateRecentLimitVisibility(categories) {
  const selected = new Set(selectedActiveCategoryIds(categories));
  categories.forEach((category) => {
    const input = $(category.id);
    if (input) input.closest("label").classList.toggle("disabled-category", !selected.has(category.id));
  });
}

function materialCategories(data = { settings }) {
  const source = data.settings || settings;
  const categories = Array.isArray(source.material_categories) ? source.material_categories : [];
  return categories.length ? categories : [
    { id: "category_A", label: "A 类" },
    { id: "category_B", label: "B 类" },
    { id: "category_C", label: "C 类" },
  ];
}

function shortPath(value) {
  const parts = String(value).split(/[\\/]+/).filter(Boolean);
  return parts.slice(-2).join("\\") || value;
}

function outputRootPath() {
  return $("outputRoot").dataset.fullPath || $("outputRoot").value;
}

function renderBgm(data) {
  const localBgm = Array.isArray(data.local_bgm) ? data.local_bgm : [];
  const localBgmDir = data.local_bgm_dir || "runtime/video_matrix/bgm";
  bgmLibraryState = {
    local: localBgm,
    directory: localBgmDir,
    links: Object.values(data.bgm_library || {}),
    pixabay: [],
  };
  $("bgmPanel").innerHTML = `
    <div class="bgm-label-row">
      <strong>本地背景音乐</strong>
      <button class="help-dot" type="button" aria-label="背景音乐目录" title="把 MP3 文件放到：${escapeHtml(localBgmDir)}">?</button>
    </div>
    <p id="bgmLibraryHint" class="bgm-library-hint"></p>
    <div class="links bgm-links"></div>`;
  $("bgmLibraryHint").textContent = localBgm.length
    ? `已找到 ${localBgm.length} 首本地音频，生成时每次随机取 1 首：${localBgmDir}`
    : `请把 MP3 文件放入：${localBgmDir}，然后刷新页面。`;
  document.querySelector("#bgmPanel .links").innerHTML = Object.values(data.bgm_library || {}).map(item => `<a href="${item.download_page}" target="_blank" rel="noopener">${item.name}</a>`).join("");
}
function toggleBgmLibraryPopover() {
  const panel = $("bgmLibraryPopover");
  if (!panel) return;
  const isHidden = panel.classList.toggle("hidden");
  panel.classList.toggle("modal", !isHidden);
  document.body.classList.toggle("bgm-modal-open", !isHidden);
  if (isHidden) return;
  const localList = bgmLibraryState.local.length
    ? bgmLibraryState.local.map((name) => `
      <li class="bgm-local-item">
        <span>${escapeHtml(name)}</span>
        <audio controls preload="none" src="/api/video-matrix/bgm/${encodeURIComponent(name)}"></audio>
      </li>`).join("")
    : "<li>暂无本地 MP3 文件</li>";
  const linkList = bgmLibraryState.links.length
    ? bgmLibraryState.links.map((item) => `<a href="${escapeHtml(item.download_page || "#")}" target="_blank" rel="noopener">${escapeHtml(item.name || "曲库来源")} / 打开试听来源</a>`).join("")
    : "<span>暂无外部曲库链接</span>";
  const pixabayList = bgmLibraryState.pixabay.length
    ? renderPixabayTracks()
    : `<button id="loadPixabayTracks" type="button">抓取 Pixabay industry 前 10 首</button>`;
  panel.innerHTML = `
    <div class="bgm-popover-head">
      <div>
        <strong>本地曲库列表</strong>
        <small title="${escapeHtml(bgmLibraryState.directory)}">下载目录：${escapeHtml(shortPath(bgmLibraryState.directory))}</small>
      </div>
      <button id="toggleBgmLibrarySize" type="button" class="secondary">收起</button>
    </div>
    <section class="bgm-local-section">
      <ul>${localList}</ul>
    </section>
    <section class="bgm-pixabay-section">
      <strong>Pixabay industry 曲库</strong>
      <div id="pixabayTrackList" class="pixabay-track-list">${pixabayList}</div>
    </section>
    <div class="bgm-download-box">
      <strong>音频直链试听 / 下载</strong>
      <label><span>音频地址</span><input id="bgmDownloadUrl" placeholder="https://.../music.mp3"></label>
      <audio id="bgmUrlPreview" controls preload="none"></audio>
      <button id="downloadBgm" type="button">下载到本地曲库</button>
      <small id="bgmDownloadStatus"></small>
    </div>
    <div class="bgm-popover-links">${linkList}</div>
  `;
  $("bgmDownloadUrl").oninput = () => {
    $("bgmUrlPreview").src = $("bgmDownloadUrl").value.trim();
  };
  $("downloadBgm").onclick = downloadBgmToLibrary;
  $("toggleBgmLibrarySize").onclick = toggleBgmLibrarySize;
  $("loadPixabayTracks")?.addEventListener("click", loadPixabayTracks);
  panel.querySelectorAll("[data-pixabay-open]").forEach((button) => {
    button.onclick = () => window.open(button.dataset.pixabayOpen, "_blank", "noopener");
  });
  bindExclusiveBgmAudioPlayback(panel);
}
function bindExclusiveBgmAudioPlayback(panel) {
  panel.querySelectorAll("audio").forEach((audio) => {
    audio.addEventListener("play", () => {
      panel.querySelectorAll("audio").forEach((otherAudio) => {
        if (otherAudio !== audio) otherAudio.pause();
      });
    });
  });
}
function toggleBgmLibrarySize() {
  const panel = $("bgmLibraryPopover");
  panel.classList.add("hidden");
  panel.classList.remove("modal");
  document.body.classList.remove("bgm-modal-open");
}
function renderPixabayTracks() {
  return bgmLibraryState.pixabay.slice(0, 10).map((track, index) => `
    <article class="pixabay-track">
      <div><strong>${index + 1}. ${escapeHtml(track.title)}</strong><span>${escapeHtml(track.artist)} / ${escapeHtml(track.duration)}</span></div>
      <button type="button" data-pixabay-open="${escapeHtml(track.source_url)}">试听</button>
      <button type="button" data-pixabay-open="${escapeHtml(track.source_url)}">下载页</button>
    </article>
  `).join("");
}
async function loadPixabayTracks() {
  const list = $("pixabayTrackList");
  list.innerHTML = loadingInline("正在抓取 Pixabay industry...");
  try {
    const data = await api("/api/video-matrix/pixabay/industry");
    bgmLibraryState.pixabay = data.tracks || [];
    list.innerHTML = renderPixabayTracks();
    list.querySelectorAll("[data-pixabay-open]").forEach((button) => {
      button.onclick = () => window.open(button.dataset.pixabayOpen, "_blank", "noopener");
    });
  } catch (error) {
    list.textContent = error.message;
  }
}
async function downloadBgmToLibrary() {
  const url = $("bgmDownloadUrl").value.trim();
  const status = $("bgmDownloadStatus");
  if (!url) {
    status.textContent = "请先粘贴 MP3/WAV/M4A 音频直链。";
    return;
  }
  status.innerHTML = loadingInline("正在下载到本地曲库...");
  try {
    const result = await api("/api/video-matrix/bgm/download", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({url})});
    status.textContent = `已下载：${result.filename}`;
    const data = await api("/api/video-matrix/state");
    state = data.ui_state; settings = data.settings;
    renderBgm(data);
    const panel = $("bgmLibraryPopover");
    panel.classList.remove("hidden");
    toggleBgmLibraryPopover();
    toggleBgmLibraryPopover();
  } catch (error) {
    status.textContent = error.message;
  }
}
function updateSourceMode() { $("uploadSourcesWrap")?.classList.toggle("hidden", true); }
function renderRadio(containerId, name, options, selected, onchange) {
  $(containerId).innerHTML = options.map(([value, label]) => `<label><input type="radio" name="${name}" value="${value}" ${value === selected ? "checked" : ""}>${label}</label>`).join("");
  document.querySelectorAll(`input[name="${name}"]`).forEach(r => r.onchange = onchange || (() => {}));
}
function radioValue(name) { return document.querySelector(`input[name="${name}"]:checked`)?.value || ""; }
function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }
function syncNumber(id) { const el = $(id); if (!el) return; el.oninput = () => { let value = Number(el.value || 3); value = Math.max(Number(el.min || 1), Math.min(Number(el.max || 100), value)); if (String(value) !== el.value) el.value = value; if (id === "outputCount") $("metricCount").textContent = el.value; scheduleStateSave(); }; }
function syncRange(id) { bindRangeControl(id, () => { if (id === "outputCount") $("metricCount").textContent = $(id).value; if (id === "maxWorkers") $("metricWorkers").textContent = $(id).value; scheduleStateSave(); }); }
function rangeControlHtml({ id = "", key = "", label, min, max, step = 1, value, className = "" }) {
  const attr = key ? `data-key="${escapeHtml(key)}"` : "";
  const rangeId = id || `control-${key}`;
  return `<label class="range-control ${className}" ${attr}><span>${escapeHtml(label)}</span><div><input id="${escapeHtml(rangeId)}" ${attr} type="range" min="${min}" max="${max}" step="${step}" value="${escapeHtml(value)}"><input class="control-number" ${attr} type="number" min="${min}" max="${max}" step="${step}" value="${escapeHtml(value)}"></div></label>`;
}
function bindRangeControl(idOrKey, onchange) {
  const range = $(idOrKey) || document.querySelector(`.range-control[data-key="${CSS.escape(idOrKey)}"] input[type="range"]`);
  if (!range) return;
  const control = range.closest(".range-control");
  const number = control?.querySelector(".control-number");
  const sync = (source) => {
    let value = Number(source.value || range.min || 0);
    value = Math.max(Number(range.min || value), Math.min(Number(range.max || value), value));
    const step = String(range.step || "1");
    const next = step.includes(".") ? String(value) : String(Math.round(value));
    range.value = next;
    if (number) number.value = next;
    onchange?.();
  };
  if (number) number.value = range.value;
  range.oninput = () => sync(range);
  range.onchange = () => sync(range);
  if (number) {
    number.oninput = () => sync(number);
    number.onchange = () => sync(number);
  }
}
function setMulti(select, values) { [...select.options].forEach(o => o.selected = values.includes(o.value)); }
function openFolder(path) { return api("/api/video-matrix/open-folder", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({path})}); }
function updateJobStatus(job) {
  const stage = job.stage || job.status || "queued";
  const percent = Math.max(0, Math.min(100, Math.round((job.progress || 0) * 100)));
  const isError = job.status === "error";
  $("jobStatusTitle").textContent = job.status === "error" ? "生成失败" : job.status === "complete" ? "生成完成" : `正在${jobMessages[stage] ? jobMessages[stage].replace(/^正在/, "").replace(/。$/, "") : "处理"}`;
  $("jobPercent").textContent = `${percent}%`;
  $("jobProgressFill").style.width = `${percent}%`;
  $("jobMessage").textContent = job.error || job.message || jobMessages[stage] || "正在处理，请稍等。";
  $("jobLog").classList.toggle("error", isError);
  $("jobMessage").classList.toggle("error-message", isError);
  $("jobSteps").innerHTML = jobStepLabels.map(([key, label]) => {
    const stepPercent = key === "queued" ? 0 : key === "ingestion" ? 5 : key === "hud" ? 20 : key === "beat" ? 30 : key === "planning" ? 42 : key === "render" ? 45 : key === "finalizing" ? 97 : 100;
    const done = percent >= stepPercent || job.status === "complete";
    const active = key === stage || (key === "render" && stage === "render");
    return `<li class="${done ? "done" : ""} ${active ? "active" : ""}"><span></span>${label}</li>`;
  }).join("");
}
function log(text) { updateJobStatus({ status: "running", stage: "queued", progress: 0, message: text }); }
function escapeHtml(value) { return String(value).replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch])); }
function debounce(fn, ms) { let timer; return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), ms); }; }

window.addEventListener("message", (event) => {
  if (event.origin !== window.location.origin || event.data?.type !== "gasgx-video-template-update") return;
  applyVisualTemplateUpdates(event.data.updates);
});
window.addEventListener("message", (event) => {
  if (event.origin !== window.location.origin || event.data?.type !== "gasgx-video-template-text-update") return;
  applyVisualTextUpdates(event.data.text);
});
window.addEventListener("message", (event) => {
  if (event.origin !== window.location.origin || event.data?.type !== "gasgx-cover-template-update") return;
  applyCoverTemplateUpdates(event.data.updates);
});
window.addEventListener("message", (event) => {
  if (event.origin !== window.location.origin || event.data?.type !== "gasgx-cover-template-text-update") return;
  applyCoverTextUpdates(event.data.text);
});

init().catch((err) => log(err.message));
