const $ = (id) => document.getElementById(id);
let state = {};
let templates = {};
let coverTemplates = {};
let selectedCover = "";
let selectedVideoTemplate = "";
let settings = {};
let lastPreviewPath = "";
let bgmLibraryState = { local: [], directory: "", links: [], pixabay: [] };
let endingTemplateState = { local: [], directory: "" };
let endingPreviewOverrideName = "";
let pendingTemplateSave = "";
let coverEditingContext = "cover";
let modelImages = [];
let selectedModelImageUrl = "";
let sourcePreviewVideos = [];
let endingModeLoading = "";

const jobStepLabels = [
  ["queued", "任务提交", 0, ["queued"]],
  ["ingest_scan", "扫描素材", 5, ["ingestion"]],
  ["ingestion", "整理素材", 12, ["ingestion"]],
  ["hud", "准备数据字幕", 20, ["hud"]],
  ["beat", "分析音乐节奏", 30, ["beat"]],
  ["planning", "规划混剪方案", 42, ["planning"]],
  ["render_start", "启动渲染", 45, ["render"]],
  ["render", "生成视频", 56, ["render"]],
  ["render_wait", "等待导出", 82, ["render"]],
  ["finalizing", "整理导出文件", 97, ["finalizing"]],
  ["complete", "完成", 100, ["complete"]],
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

const backendJobMessageMap = {
  "Queued": "任务已提交，正在排队准备。请保持当前页面打开，系统会自动开始处理。",
  "Collecting and normalizing source clips": "正在扫描并整理素材视频，按分类目录读取可用片段。",
  "Preparing GasGx data HUD": "正在准备视频里的 HUD 数据、标题和字幕字段。",
  "Analyzing BGM beat grid": "正在分析背景音乐节拍网格，用于后续卡点混剪。",
  "Planning de-duplicated video variants": "正在规划每条视频的素材组合，避免重复使用同一片段。",
  "Finalizing preview assets and manifests": "正在整理导出的 MP4、预览文件和清单。",
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
  ["dual_gradient", "上下渐变蒙版"],
  ["full", "全蒙版"],
];
const endingTemplateModeOptions = [
  ["dynamic", "动态封面"],
  ["random", "随机素材"],
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
function buttonIconLabel(icon, label) {
  return `<span class="button-icon" aria-hidden="true">${escapeHtml(icon)}</span><span>${escapeHtml(label)}</span>`;
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

function pulseImageLoading(id, label = "应用中...") {
  setImageLoading(id, label);
  window.clearTimeout(pulseImageLoading.timers?.[id]);
  pulseImageLoading.timers = pulseImageLoading.timers || {};
  pulseImageLoading.timers[id] = window.setTimeout(() => clearImageLoading(id), 650);
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
  ["sourceDirs", "recentLimits", "compositionRows", "videoTemplateSelector", "videoTemplateForm", "coverForm", "endingTemplateForm", "bgmPanel"].forEach((id) => setPanelLoading(id));
  setPanelLoading("videoTemplateGallery", "加载正文模板...");
  setImageLoading("videoTemplatePreview", "加载正文预览...");
  setImageLoading("coverPreview", "加载封面预览...");
  setImageLoading("endingTemplatePreview", "加载片尾预览...");
}

async function init() {
  setInitialLoading();
  const data = await api("/api/video-matrix/state");
  state = data.ui_state; templates = data.templates; coverTemplates = data.cover_templates; settings = data.settings;
  sourcePreviewVideos = Array.isArray(data.source_videos) ? data.source_videos : [];
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
  renderEndingTemplatePanel(data);
  await loadModelImages();
  await refreshAllPreviews();
}

function renderSidebar(data) {
  $("outputCount").value = state.output_count || 3;
  $("maxWorkers").value = state.max_workers || 3;
  $("videoDurationMin").value = state.video_duration_min || settings.video_duration_min || 8;
  $("videoDurationMax").value = state.video_duration_max || settings.video_duration_max || 12;
  syncNumber("outputCount");
  syncNumber("videoDurationMin");
  syncNumber("videoDurationMax");
  syncRange("maxWorkers");
  renderSidebarTemplateSelectors();
  const outputRoot = state.output_root || settings.output_root;
  $("outputRoot").dataset.fullPath = outputRoot;
  $("outputRoot").title = outputRoot;
  $("outputRoot").value = shortPath(outputRoot);
  $("outputOptions").value = (state.output_options || ["mp4"])[0] || "mp4";
  $("outputOptions").onchange = scheduleStateSave;
  $("openOutput").onclick = () => openFolder(outputRootPath());
  renderRadio("targetFpsGroup", "target_fps", [["30", "30 fps"], ["60", "60 fps"]], String(state.target_fps || settings.target_fps || 60), scheduleStateSave);
  renderRadio("languageGroup", "copy_language", [["zh", "中文"], ["en", "英文"], ["ru", "俄文"]], state.copy_language || "zh", scheduleStateSave);
  renderBgm(data);
  $("saveState").onclick = toggleBgmLibraryPopover;
  $("openBgmDir").onclick = () => openFolder(bgmLibraryState.directory);
  document.querySelector(".sidebar details summary")?.addEventListener("click", (event) => {
    event.preventDefault();
    toggleBgmLibraryPopover();
  });
}

function renderSidebarTemplateSelectors() {
  const coverSelect = $("sidebarCoverTemplate");
  if (coverSelect) {
    coverSelect.innerHTML = Object.entries(coverTemplates).map(([id, item], index) =>
      `<option value="${escapeHtml(id)}" ${id === selectedCover ? "selected" : ""}>${escapeHtml(coverTemplateDisplayName(id, item, index))}</option>`
    ).join("");
    coverSelect.onchange = () => selectCoverTemplate(coverSelect.value);
  }
  const videoSelect = $("sidebarVideoTemplate");
  if (videoSelect) {
    videoSelect.innerHTML = Object.entries(templates).map(([id, item], index) =>
      `<option value="${escapeHtml(id)}" ${id === selectedVideoTemplate ? "selected" : ""}>${escapeHtml(videoTemplateDisplayName(id, item, index))}</option>`
    ).join("");
    videoSelect.onchange = () => selectVideoTemplate(videoSelect.value);
  }
  const endingSelect = $("sidebarEndingTemplateMode");
  if (endingSelect) {
    endingSelect.value = state.ending_template_mode === "random" ? "random" : "dynamic";
    endingSelect.onchange = () => switchEndingTemplateMode(endingSelect.value);
  }
}

function renderSource(data) {
  $("metricSources").textContent = Object.values(data.category_counts).reduce((a, b) => a + b, 0);
  $("metricCount").textContent = $("outputCount").value;
  $("metricWorkers").textContent = $("maxWorkers").value;
  const categories = materialCategories(data);
  const activeCategoryIds = activeCategories(categories);
  const rows = compositionRowsByCategory(data, categories);
  const recentLimits = state.recent_limits || settings.recent_limits || {};
  $("sourceDirs").innerHTML = categories.map((category, index) => {
    const row = rows.get(category.id) || { category_id: category.id, duration: defaultDurationForCategory(category.id) };
    const limit = clamp(Number(recentLimits[category.id] || settings.recent_limits?.[category.id] || 8), 1, 10);
    const checked = activeCategoryIds.includes(category.id);
    const totalCount = Number(data.category_counts?.[category.id] || 0);
    return `<div class="dir-row source-composition-row composition-row" data-index="${index}" data-source-category="${escapeHtml(category.id)}">
      <label class="category-toggle">
        <input type="checkbox" data-category-id="${escapeHtml(category.id)}" ${checked ? "checked" : ""}>
        <span class="badge" title="${escapeHtml(data.source_dirs[category.id] || "")}">${escapeHtml(category.id)}</span>
      </label>
      <input type="hidden" data-composition-category value="${escapeHtml(category.id)}">
      <input data-category-label value="${escapeHtml(category.label || category.id)}" aria-label="${escapeHtml(category.id)}目录名称" ${checked ? "" : "disabled"}>
      <label class="composition-unit-field">
        <input data-composition-duration type="number" min="0.2" max="12" step="0.1" value="${Number(row.duration || 1).toFixed(1)}" aria-label="${escapeHtml(category.label)}片段秒数" ${checked ? "" : "disabled"}>
        <span>秒</span>
      </label>
      <label class="composition-unit-field composition-material-count">
        <span>采用最新前</span>
        <input data-composition-limit list="recentLimitOptions" type="text" inputmode="numeric" pattern="[1-9]|10" value="${limit}" placeholder="1-10" title="最新素材数量" aria-label="${escapeHtml(category.label)}最新素材数量" ${checked ? "" : "disabled"}>
        <span>条</span>
      </label>
      <span class="source-total-count">素材总数：<b>${totalCount}</b></span>
      <button type="button" data-source-open data-path="${escapeHtml(data.source_dirs[category.id] || "")}">打开目录</button>
    </div>`;
  }).join("");
  $("sourceDirs").querySelectorAll("[data-source-open]").forEach((btn) => btn.onclick = () => openFolder(btn.dataset.path));
  $("sourceDirs").querySelectorAll("[data-category-label]").forEach((input) => {
    input.dataset.savedValue = input.value;
    input.onchange = () => renameMaterialCategory(input, data);
    input.onkeydown = (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        input.blur();
      }
    };
  });
  $("sourceDirs").querySelectorAll("[data-category-id]").forEach((input) => input.onchange = () => {
    state.active_category_ids = selectedActiveCategoryIds(categories);
    updateCompositionState(true);
    renderSource(data);
    renderComposition(data);
    updateRecentLimitVisibility(categories);
    saveState();
  });
  $("sourceDirs").querySelectorAll("[data-composition-duration]").forEach((input) => {
    input.oninput = () => {
      updateCompositionState(true);
      scheduleStateSave();
    };
  });
  $("sourceDirs").querySelectorAll("[data-composition-limit]").forEach((input) => {
    input.oninput = () => {
      updateRecentLimitFromRow(input.closest(".composition-row"));
      scheduleStateSave();
    };
    input.onchange = () => {
      updateRecentLimitFromRow(input.closest(".composition-row"));
      renderSource(data);
      renderComposition(data);
      scheduleStateSave();
    };
  });
  $("addCategory").onclick = addMaterialCategory;
  $("sourceCounts").textContent = "算法：按视频碎片分类目录读取素材；每次按照目录把最新拍摄的短视频上传进对应的目录；勾选素材目录后，直接在对应行设置片段秒数和最新素材数量，系统按行顺序自动组合混剪。";
  renderRadio("sourceModeGroup", "source_mode", [["Category folders", "智能分类轮换算法"]], state.source_mode || "Category folders", () => {
    updateSourceMode();
    scheduleStateSave();
  });
  updateRecentLimitVisibility(categories);
  updateSourceMode();
}

function compositionRowsByCategory(data, categories) {
  const rows = compositionSequence();
  const defaults = defaultCompositionSequence(categories);
  return new Map(categories.map((category) => {
    const row = rows.find((item) => item.category_id === category.id)
      || defaults.find((item) => item.category_id === category.id)
      || { category_id: category.id, duration: defaultDurationForCategory(category.id) };
    return [category.id, row];
  }));
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

async function renameMaterialCategory(button, data) {
  const row = button.closest(".composition-row");
  const categoryId = button.dataset.categoryId || row?.dataset.sourceCategory || "";
  const input = row?.querySelector("[data-category-label]");
  const label = input?.value.trim() || "";
  if (!categoryId || !label) {
    log("请先输入目录名称。");
    return;
  }
  if (input?.dataset.savedValue === label) return;
  button.disabled = true;
  try {
    await api(`/api/video-matrix/material-categories/${encodeURIComponent(categoryId)}`, {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify({label}),
    });
    const nextData = await api("/api/video-matrix/state");
    state = nextData.ui_state; settings = nextData.settings;
    renderSource(nextData);
    renderComposition(nextData);
    log(`已保存素材目录名称：${label}`);
  } finally {
    button.disabled = false;
  }
}

function renderComposition(data = { settings }) {
  if ($("compositionRows")) $("compositionRows").innerHTML = "";
  if ($("addCompositionRow")) $("addCompositionRow").onclick = addCompositionRow;
  return;
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
        <label class="composition-unit-field">
          <input data-composition-duration type="number" min="0.2" max="12" step="0.1" value="${Number(row.duration || 1).toFixed(1)}" aria-label="片段秒数" />
          <span>s</span>
        </label>
        <label class="composition-unit-field composition-material-count">
          <input data-composition-limit list="recentLimitOptions" type="text" inputmode="numeric" pattern="[1-9]|10" value="${limit}" placeholder="1-10" title="最新素材数量" aria-label="最新素材数量" />
          <span>条素材</span>
        </label>
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
  state.composition_sequence = [...document.querySelectorAll(".composition-row")]
    .filter((row) => row.querySelector("[data-category-id]")?.checked !== false)
    .map((row) => ({
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
  $("followText").value = state.follow_text || "";
  $("hudText").value = state.hud_text || "";
  ["headline", "subhead", "followText", "hudText"].forEach((id) => {
    $(id).addEventListener("input", scheduleStateSave);
  });
  ["headline", "subhead", "hudText"].forEach((id) => $(id).addEventListener("input", debounce(refreshAllPreviews, 250)));
  $("generateBtn").onclick = generate;
}

function renderCoverSelector() {
  if (!$("coverSelector")) return;
  $("coverSelector").innerHTML = Object.entries(coverTemplates).map(([id, item], index) =>
    `<button class="${id === selectedCover ? "active" : ""}" data-id="${id}">${escapeHtml(coverTemplateDisplayName(id, item, index))}</button>`).join("");
  $("coverSelector").querySelectorAll("button").forEach((btn) => btn.onclick = async () => {
    selectedCover = btn.dataset.id;
    renderCoverSelector(); renderCoverEditor(); await saveTemplateSelection(); await refreshAllPreviews();
  });
}

function renderCoverTemplateMenu() {
  const menu = $("coverTemplateMenu");
  const trigger = $("coverTemplateSwitch");
  if (!menu || !trigger) return;
  menu.innerHTML = Object.entries(coverTemplates).map(([id, item], index) =>
    `<button type="button" class="${id === selectedCover ? "active" : ""}" data-cover-template="${escapeHtml(id)}">${escapeHtml(coverTemplateDisplayName(id, item, index))}</button>`
  ).join("");
  trigger.onclick = () => {
    const expanded = menu.classList.toggle("hidden") === false;
    trigger.setAttribute("aria-expanded", expanded ? "true" : "false");
  };
  menu.querySelectorAll("[data-cover-template]").forEach((button) => {
    button.onclick = async () => {
      menu.classList.add("hidden");
      trigger.setAttribute("aria-expanded", "false");
      trigger.disabled = true;
      trigger.classList.add("is-loading");
      trigger.innerHTML = buttonLoadingInline("切换中...");
      try {
        await selectCoverTemplate(button.dataset.coverTemplate);
      } finally {
        trigger.disabled = false;
        trigger.classList.remove("is-loading");
        trigger.innerHTML = buttonIconLabel("⇄", "模板切换");
      }
    };
  });
}

function renderVideoTemplateSelector() {
  $("videoTemplateSelector").innerHTML = Object.entries(templates).map(([id, item], index) =>
    `<button class="${id === selectedVideoTemplate ? "active" : ""}" data-id="${id}">${escapeHtml(videoTemplateDisplayName(id, item, index))}</button>`).join("");
  $("videoTemplateSelector").querySelectorAll("button").forEach((btn) => btn.onclick = async () => {
    await selectVideoTemplate(btn.dataset.id, { refreshTemplateGallery: false });
  });
}

function numberedTemplateName(prefix, id, item, index, pattern) {
  const rawName = String(item?.name || "");
  const rawId = String(id || "");
  const matched = rawName.match(pattern) || rawId.match(/_(\d+)$/);
  const serial = String(Number(matched?.[1] || index + 1)).padStart(2, "0");
  return `${prefix} ${serial}`;
}

function coverTemplateDisplayName(id, item, index = Object.keys(coverTemplates).indexOf(id)) {
  return numberedTemplateName("第一屏封面模板", id, item, index, /(?:第一屏封面模板|九宫格图片模板)\s*(\d+)/);
}

function videoTemplateDisplayName(id, item, index = Object.keys(templates).indexOf(id)) {
  return numberedTemplateName("视频叠层模板", id, item, index, /视频叠层模板\s*(\d+)/);
}

async function selectVideoTemplate(templateId, options = {}) {
  if (!templateId || !templates[templateId]) return;
  const refreshTemplateGallery = options.refreshTemplateGallery !== false;
  selectedVideoTemplate = templateId;
  setImageLoading("videoTemplatePreview", "切换正文模板...");
  if (refreshTemplateGallery) setPanelLoading("videoTemplateGallery", "切换正文模板...");
  if ($("sidebarVideoTemplate")) $("sidebarVideoTemplate").value = templateId;
  renderVideoTemplateSelector();
  renderVideoTemplateEditor();
  await saveTemplateSelection();
  await refreshVideoTemplatePreview();
  if (refreshTemplateGallery) await refreshVideoTemplateGallery();
}

function renderCoverEditor() {
  const t = coverTemplates[selectedCover];
  applyIndependentCoverDefaults(t);
  const independentCover = isIndependentCover(t);
  $("previewCaption").textContent = `${selectedCover} / ${coverTemplateDisplayName(selectedCover, t)}${independentCover ? " / 独立视频封面" : ""}`;
  renderCoverTemplateMenu();
  const toggle = $("coverLayoutToggle");
  if (toggle) {
    toggle.innerHTML = independentCover ? buttonIconLabel("☷", "列表效果预览") : buttonIconLabel("▣", "独立封面预览");
    toggle.classList.toggle("active", independentCover);
    toggle.onclick = toggleCoverLayout;
  }
  const maskModeOptions = coverMaskModeOptions.map(([value, label]) =>
    `<option value="${value}" ${value === coverTemplateValue(t, "mask_mode", "bottom_gradient") ? "selected" : ""}>${label}</option>`
  ).join("");
  const html = [`<h3>可视化调整</h3>`, `
    <label>模板名称<input data-key="name" type="text" value="${escapeHtml(t.name || "")}"></label>
    <div class="cover-section-title">蒙版编辑区</div>
    <label>蒙版类型<select data-key="mask_mode">${maskModeOptions}</select></label>
    <label>蒙版颜色<input data-key="mask_color" type="color" value="${escapeHtml(coverTemplateValue(t, "mask_color", t.gradient_color || t.tint_color || "#071015"))}"></label>
    ${rangeControlHtml({key: "mask_opacity", label: "蒙版透明度", min: 0, max: 1, step: 0.01, value: coverTemplateValue(t, "mask_opacity", t.gradient_opacity ?? t.tint_opacity ?? 0.35), className: "cover-template-control"})}
    <div class="cover-section-title">独立封面文字</div>
    <label>Logo文字<input data-key="single_cover_logo_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_logo_text", "GasGx"))}"></label>
    <label>Slogan文字<input data-key="single_cover_slogan_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_slogan_text", defaultSingleCoverSlogan()))}"></label>
    <label>一句话视频描述<textarea data-key="single_cover_title_text" rows="3">${escapeHtml(coverTemplateValue(t, "single_cover_title_text", defaultSingleCoverTitle()))}</textarea></label>
    ${coverVisualToolbarHtml(t)}
    <p class="visual-editor-hint">点击预览里的文字或按钮后拖动定位；工具栏可调整字号、颜色、对齐和文字内容。</p>
    <div class="template-actions cover-template-actions">
      <button type="button" id="saveCover">保存</button>
      <button type="button" id="saveCoverAsNew" class="secondary">新建保存</button>
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
  $("saveCover").onclick = saveCurrentCoverTemplate;
  $("saveCoverAsNew").onclick = saveCoverAsNewTemplate;
}

function renderEndingTemplatePanel(data) {
  const localTemplates = Array.isArray(data.ending_templates) ? data.ending_templates : [];
  endingTemplateState = {
    local: localTemplates,
    directory: data.ending_template_dir || "runtime/video_matrix/ending_template",
  };
  if (!state.ending_template_mode) state.ending_template_mode = "dynamic";
  if (state.ending_template_mode === "specific") state.ending_template_mode = "random";
  if (!state.ending_template_id && localTemplates.length) state.ending_template_id = localTemplates[0].name;
  if (!Array.isArray(state.ending_template_ids) && localTemplates.length) {
    state.ending_template_ids = localTemplates.map((item) => item.name);
  }
  const mode = state.ending_template_mode || "dynamic";
  const selected = endingTemplateSelectedName();
  const modeButtons = endingTemplateModeOptions.map(([value, label]) =>
    `<button type="button" class="${mode === value ? "active" : ""} ${endingModeLoading === value ? "is-loading" : ""}" data-ending-mode="${value}" ${endingModeLoading ? "disabled" : ""}>${endingModeLoading === value ? buttonLoadingInline("切换中...") : label}</button>`
  ).join("");
  const options = localTemplates.length
    ? localTemplates.map((item) => `<option value="${escapeHtml(item.name)}" ${selected === item.name ? "selected" : ""}>${escapeHtml(item.name)}</option>`).join("")
    : `<option value="">目录内暂无片尾素材</option>`;
  $("endingTemplateForm").innerHTML = `
    <h3>片尾模板调整区</h3>
    <div class="template-tabs ending-mode-tabs">${modeButtons}</div>
    ${mode === "dynamic" ? endingCoverEditorHtml() : ""}
    ${mode === "random" ? endingRandomMaterialHtml(localTemplates) : ""}
    <div class="ending-template-dir-row ${mode === "random" ? "" : "hidden"}">
      <code title="${escapeHtml(endingTemplateState.directory)}">${escapeHtml(shortPath(endingTemplateState.directory))}</code>
      <span class="badge">${localTemplates.length} 个素材</span>
      <button id="openEndingTemplateDirInline" class="secondary" type="button">打开</button>
    </div>
  `;
  $("endingTemplateForm").querySelectorAll("[data-ending-mode]").forEach((button) => {
    button.onclick = () => switchEndingTemplateMode(button.dataset.endingMode, button);
  });
  if (mode === "dynamic") bindEndingCoverEditor();
  if (mode === "random") bindEndingRandomMaterials();
  const selector = $("endingTemplateSelect");
  if (selector) {
    selector.onchange = () => {
      state.ending_template_id = selector.value;
      scheduleStateSave();
      refreshEndingTemplatePreview();
    };
  }
  $("openEndingTemplateDir").onclick = () => openFolder(endingTemplateState.directory);
  $("openEndingTemplateDirInline").onclick = () => openFolder(endingTemplateState.directory);
}

async function switchEndingTemplateMode(mode, sourceButton = null) {
  if (!mode) return;
  endingModeLoading = mode;
  if (sourceButton) {
    sourceButton.classList.add("is-loading");
    sourceButton.innerHTML = buttonLoadingInline("切换中...");
  }
  $("endingTemplateForm")?.querySelectorAll("[data-ending-mode]").forEach((item) => item.disabled = true);
  state.ending_template_mode = mode;
  if (state.ending_template_mode === "specific" && !state.ending_template_id && endingTemplateState.local.length) {
    state.ending_template_id = endingTemplateState.local[0].name;
  }
  try {
    renderSidebarTemplateSelectors();
    renderEndingTemplatePanel({ ending_templates: endingTemplateState.local, ending_template_dir: endingTemplateState.directory });
    await saveTemplateSelection();
    await refreshEndingTemplatePreview();
  } finally {
    endingModeLoading = "";
    renderSidebarTemplateSelectors();
    renderEndingTemplatePanel({ ending_templates: endingTemplateState.local, ending_template_dir: endingTemplateState.directory });
    refreshEndingTemplatePreview();
  }
}

function selectedEndingTemplateNames(localTemplates = endingTemplateState.local || []) {
  const availableNames = localTemplates.map((item) => item.name);
  const selected = Array.isArray(state.ending_template_ids)
    ? state.ending_template_ids.filter((name) => availableNames.includes(name))
    : availableNames;
  return selected.length ? selected : availableNames;
}

function endingRandomMaterialHtml(localTemplates) {
  const selected = new Set(selectedEndingTemplateNames(localTemplates));
  const rows = localTemplates.length
    ? localTemplates.map((item) => `
      <label class="ending-material-row">
        <input data-ending-template-choice type="checkbox" value="${escapeHtml(item.name)}" ${selected.has(item.name) ? "checked" : ""}>
        <span>${escapeHtml(item.name)}</span>
        <small>${item.type === "video" ? "视频" : "图片"}</small>
        ${item.type === "video" ? endingPreviewToggleButtonHtml(item.name) : ""}
      </label>`).join("")
    : `<div class="ending-template-empty compact">暂无片尾素材，请先上传到目录。</div>`;
  return `
    <div class="cover-section-title">随机素材选择</div>
    <div class="ending-material-list">${rows}</div>
    <p class="visual-editor-hint">从 video_matrix\\ending_template 勾选备用片尾素材；生成时会从已勾选素材里随机取一个。</p>`;
}

function bindEndingRandomMaterials() {
  $("endingTemplateForm").querySelectorAll("[data-ending-template-choice]").forEach((input) => {
    input.onchange = () => {
      const selected = [...$("endingTemplateForm").querySelectorAll("[data-ending-template-choice]:checked")].map((node) => node.value);
      state.ending_template_ids = selected.length ? selected : selectedEndingTemplateNames();
      endingPreviewOverrideName = "";
      scheduleStateSave();
      refreshEndingTemplatePreview();
    };
  });
  $("endingTemplateForm").querySelectorAll("[data-ending-preview-toggle]").forEach((button) => {
    button.onclick = () => {
      const name = button.dataset.endingPreviewToggle || "";
      endingPreviewOverrideName = endingPreviewOverrideName === name ? "" : name;
      renderEndingTemplatePanel({ ending_templates: endingTemplateState.local, ending_template_dir: endingTemplateState.directory });
      refreshEndingTemplatePreview();
    };
  });
}

function endingPreviewToggleButtonHtml(name) {
  const active = endingPreviewOverrideName === name;
  const icon = active
    ? `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 6.5h4v11H7zM13 6.5h4v11h-4z"></path></svg>`
    : `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5.8v12.4L18 12 8 5.8Z"></path></svg>`;
  return `<button class="ending-preview-toggle ${active ? "active" : ""}" type="button" data-ending-preview-toggle="${escapeHtml(name)}" title="${active ? "停止预览" : "播放预览"}" aria-label="${active ? "停止预览" : "播放预览"}">${icon}</button>`;
}

function endingCoverTemplate() {
  const base = state.ending_cover_template || JSON.parse(JSON.stringify(coverTemplates[selectedCover] || {}));
  state.ending_cover_template = base;
  applyIndependentCoverDefaults(base);
  base.cover_layout = "single_video";
  return base;
}

function endingCoverEditorHtml() {
  const t = endingCoverTemplate();
  const maskModeOptions = coverMaskModeOptions.map(([value, label]) =>
    `<option value="${value}" ${value === coverTemplateValue(t, "mask_mode", "bottom_gradient") ? "selected" : ""}>${label}</option>`
  ).join("");
  return `
    <label>模板名称<input data-ending-cover-key="name" type="text" value="${escapeHtml(t.name || "Ending Cover")}"></label>
    <div class="cover-section-title">蒙版编辑区</div>
    <label>蒙版类型<select data-ending-cover-key="mask_mode">${maskModeOptions}</select></label>
    <label>蒙版颜色<input data-ending-cover-key="mask_color" type="color" value="${escapeHtml(coverTemplateValue(t, "mask_color", t.gradient_color || t.tint_color || "#071015"))}"></label>
    ${rangeControlHtml({key: "ending-mask-opacity", label: "蒙版透明度", min: 0, max: 1, step: 0.01, value: coverTemplateValue(t, "mask_opacity", t.gradient_opacity ?? t.tint_opacity ?? 0.35), className: "ending-cover-control"})}
    <div class="cover-section-title">独立封面文字</div>
    <label>Logo文字<input data-ending-cover-key="single_cover_logo_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_logo_text", "GasGx"))}"></label>
    <label>Slogan文字<input data-ending-cover-key="single_cover_slogan_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_slogan_text", defaultSingleCoverSlogan()))}"></label>
    <label>片尾文案<textarea data-ending-cover-key="single_cover_title_text" rows="3">${escapeHtml(coverTemplateValue(t, "single_cover_title_text", $("followText").value || state.follow_text || defaultSingleCoverTitle()))}</textarea></label>
    ${coverVisualToolbarHtml(t, "ending-cover-visual-toolbar")}
    <p class="visual-editor-hint">点击片尾预览里的文字后拖动定位；这组设置只影响片尾动态封面。</p>`;
}

function bindEndingCoverEditor() {
  $("endingTemplateForm").querySelectorAll("input[data-ending-cover-key], select[data-ending-cover-key], textarea[data-ending-cover-key]").forEach((input) => {
    input.oninput = () => updateEndingCoverTemplateField(input);
    input.onchange = () => updateEndingCoverTemplateField(input);
  });
  $("endingTemplateForm").querySelectorAll(".ending-cover-control[data-key]").forEach((control) => {
    bindRangeControl(control.dataset.key, () => updateEndingCoverTemplateField(control.querySelector('input[type="range"]')));
  });
  bindCoverVisualToolbar("endingTemplateForm", "endingTemplatePreview");
}

function updateEndingCoverTemplateField(input) {
  const template = endingCoverTemplate();
  const key = input.dataset.endingCoverKey || (input.dataset.key === "ending-mask-opacity" ? "mask_opacity" : input.dataset.key);
  if (!template || !key) return;
  template[key] = input.type === "range" || input.type === "number" ? Number(input.value) : input.value;
  if (key === "single_cover_title_text") {
    $("followText").value = input.value;
    state.follow_text = input.value;
  }
  setImageLoading("endingTemplatePreview", "应用片尾封面参数...");
  refreshEndingTemplatePreview();
  scheduleStateSave();
}

function endingTemplateMode() {
  return state.ending_template_mode || $("endingTemplateForm")?.querySelector("[data-ending-mode].active")?.dataset.endingMode || "dynamic";
}

function endingTemplateSelectedName() {
  return $("endingTemplateSelect")?.value || state.ending_template_id || "";
}

function selectedEndingTemplateAsset() {
  const localTemplates = endingTemplateState.local || [];
  if (!localTemplates.length) return null;
  const mode = endingTemplateMode();
  if (mode === "specific") {
    const selected = endingTemplateSelectedName();
    return localTemplates.find((item) => item.name === selected) || localTemplates[0];
  }
  if (mode === "random") {
    const selected = new Set(selectedEndingTemplateNames(localTemplates));
    if (endingPreviewOverrideName) {
      const override = localTemplates.find((item) => item.name === endingPreviewOverrideName);
      if (override) return override;
    }
    return localTemplates.find((item) => selected.has(item.name)) || localTemplates[0];
  }
  return null;
}

async function refreshEndingTemplatePreview() {
  const mode = endingTemplateMode();
  const asset = selectedEndingTemplateAsset();
  const frame = $("endingTemplatePreview");
  const assetBox = $("endingAssetPreview");
  const caption = $("endingTemplateCaption");
  if (!frame || !assetBox || !caption) return;
  if (mode === "random" || mode === "specific") {
    frame.classList.toggle("hidden", Boolean(asset));
    assetBox.classList.toggle("hidden", !asset);
    if (asset) {
      assetBox.innerHTML = asset.type === "video"
        ? `<video data-ending-preview-video src="${escapeHtml(asset.url)}" muted loop controls playsinline ${endingPreviewOverrideName ? "autoplay" : ""}></video>`
        : `<img src="${escapeHtml(asset.url)}" alt="">`;
      caption.textContent = `${mode === "random" ? "随机片尾素材预览" : "指定片尾素材"} / ${asset.name}`;
      if (asset.type === "video" && endingPreviewOverrideName) {
        const video = assetBox.querySelector("[data-ending-preview-video]");
        video?.play?.().catch(() => {});
      }
    } else {
      assetBox.innerHTML = `<div class="ending-template-empty">暂无片尾素材</div>`;
      assetBox.classList.remove("hidden");
      caption.textContent = `${shortPath(endingTemplateState.directory)} / 0 个素材`;
    }
    clearImageLoading("endingTemplatePreview");
    return;
  }
  frame.classList.remove("hidden");
  assetBox.classList.add("hidden");
  assetBox.innerHTML = "";
  const template = endingCoverTemplate();
  if (template) {
    refreshPhonePreviewFrame("endingTemplatePreview", {
      template,
      cover_mode: true,
      ending_cover_mode: true,
      slogan: $("followText").value,
      title: "Follow GasGx for more gas engine and generator set cases",
      headline: $("followText").value,
      subhead: "Follow GasGx for more gas engine and generator set cases",
      cta: template.cta,
      hud_text: $("hudText").value,
      background_image_url: selectedModelImageUrl || modelImages[0]?.url || "",
      background_image_urls: modelImages.map((image) => image.url).filter(Boolean),
    });
  }
  caption.textContent = `动态片尾封面 / ${template.name || "Ending Cover"}`;
  clearImageLoading("endingTemplatePreview");
}

function isIndependentCover(template) {
  return (template?.cover_layout || "profile") === "single_video";
}

function defaultSingleCoverTitle() {
  return "全球领先的搁浅天然气算力变现引擎";
}

function defaultSingleCoverSlogan() {
  return "终结废气 | 重塑能源 | 就地变现";
}

function applyIndependentCoverDefaults(template) {
  if (!template) return;
  if (!template.cover_layout) template.cover_layout = "profile";
  if (!template.single_cover_logo_text) template.single_cover_logo_text = "GasGx";
  if (!template.single_cover_slogan_text) template.single_cover_slogan_text = defaultSingleCoverSlogan();
  if (!template.single_cover_title_text) template.single_cover_title_text = defaultSingleCoverTitle();
  if (!template.single_cover_logo_font_size) template.single_cover_logo_font_size = 84;
  if (!template.single_cover_slogan_font_size) template.single_cover_slogan_font_size = 60;
  if (!template.single_cover_title_font_size) template.single_cover_title_font_size = 54;
  template.tile_brand_text = template.single_cover_logo_text;
  template.tile_tagline_text = template.single_cover_slogan_text;
  template.tile_titles_text = template.single_cover_title_text;
}

async function toggleCoverLayout() {
  const template = coverTemplates[selectedCover];
  if (!template) return;
  applyIndependentCoverDefaults(template);
  template.cover_layout = isIndependentCover(template) ? "profile" : "single_video";
  renderCoverEditor();
  setImageLoading("coverPreview", "切换封面布局...");
  await refreshMainPreview();
  scheduleCoverTemplateSave();
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
  setImageLoading("coverPreview", "应用封面参数...");
  refreshAllPreviews();
  scheduleCoverTemplateSave();
}

function coverVisualToolbarHtml(template, extraClass = "") {
  const fontValue = template.title_font_family || visualFontOptions[0][0];
  const fontOptions = visualFontOptions.map(([value, label]) =>
    `<option value="${escapeHtml(value)}" ${value === fontValue ? "selected" : ""}>${label}</option>`
  ).join("");
  return `
    <div class="visual-toolbar-panel cover-visual-toolbar ${extraClass}" aria-label="封面可视化工具">
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

function bindCoverVisualToolbar(formId = "coverForm", previewId = "coverPreview") {
  const toolbar = $(formId).querySelector(".cover-visual-toolbar");
  if (!toolbar) return;
  toolbar.querySelectorAll("button[data-cover-command]").forEach((button) => {
    button.onclick = () => postCoverTemplateCommand(button.dataset.coverCommand, button.dataset.value || "", previewId);
  });
  toolbar.querySelectorAll("select[data-cover-command], input[data-cover-command]").forEach((input) => {
    input.oninput = () => {
      updateColorSwatch(input);
      postCoverTemplateCommand(input.dataset.coverCommand, input.value, previewId);
    };
    input.onchange = () => {
      updateColorSwatch(input);
      postCoverTemplateCommand(input.dataset.coverCommand, input.value, previewId);
    };
  });
}

function postCoverTemplateCommand(command, value = "", previewId = "coverPreview") {
  coverEditingContext = previewId === "endingTemplatePreview" ? "ending" : "cover";
  if (previewId === "endingTemplatePreview") {
    pulseImageLoading("endingTemplatePreview", "应用片尾封面参数...");
  } else {
    pulseImageLoading("coverPreview", "应用封面参数...");
  }
  $(previewId)?.contentWindow?.postMessage({
    type: "gasgx-cover-template-command",
    command,
    value,
  }, window.location.origin);
}

async function refreshAllPreviews() {
  await refreshVideoTemplatePreview();
  await refreshVideoTemplateGallery();
  await refreshMainPreview();
  await refreshEndingTemplatePreview();
}

function renderVideoTemplateEditor() {
  const template = templates[selectedVideoTemplate];
  if (!template) return;
  const html = [
    `<h3>模板调整区</h3>`,
    `<label>模板名称<input data-key="name" type="text" value="${escapeHtml(template.name || "")}"></label>`,
    visualTemplateToolbarHtml(template),
  ];
  for (const [key, label, type, min, max] of videoTemplateFields) {
    if (key === "name") continue;
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
        <button type="button" data-visual-command="size-down" title="缩小字号">A-</button>
        <button type="button" data-visual-command="size-up" title="放大字号">A+</button>
        <button type="button" data-visual-command="edit" title="编辑文字">编辑</button>
        <button type="button" data-visual-command="align" data-value="left" title="左对齐">左齐</button>
        <button type="button" data-visual-command="align" data-value="center" title="居中对齐">居中</button>
        <button type="button" data-visual-command="align" data-value="right" title="右对齐">右齐</button>
        <label class="color-swatch-button" title="文字颜色">
          ${colorPickerIconSvg()}
          <span class="color-current-dot" style="background:${escapeHtml(template.primary_color || "#ffffff")}"></span>
          <input data-visual-command="color" type="color" value="${escapeHtml(template.primary_color || "#ffffff")}" aria-label="文字颜色">
        </label>
        <select data-visual-command="font-family">${fontOptions}</select>
        <label class="visual-effect-control">文字动效<select data-visual-command="text-effect">${effectOptions}</select></label>
      </div>
      <div class="visual-control-section visual-hud-controls" aria-label="HUD调整区">
        <div class="visual-section-title">HUD调整区</div>
        <button type="button" data-visual-command="width-down" title="缩小背景宽度">W-</button>
        <button type="button" data-visual-command="width-up" title="放大背景宽度">W+</button>
        <button type="button" data-visual-command="height-down" title="缩小背景高度">H-</button>
        <button type="button" data-visual-command="height-up" title="放大背景高度">H+</button>
        <label class="color-swatch-button" title="HUD 背景色">
          ${colorPickerIconSvg()}
          <span class="color-current-dot" style="background:${escapeHtml(hudColor)}"></span>
          <input data-visual-command="hud-bg-color" type="color" value="${escapeHtml(hudColor)}" aria-label="HUD 背景色">
        </label>
        <label class="visual-opacity-control">HUD 透明度<input data-visual-command="opacity" type="range" min="0" max="1" step="0.01" value="${escapeHtml(hudOpacity.toFixed(2))}"><output>${escapeHtml(hudOpacity.toFixed(2))}</output></label>
        <label class="visual-opacity-control">HUD 圆角<input data-visual-command="hud-radius" type="range" min="0" max="100" step="1" value="${escapeHtml(String(Math.round(hudRadius)))}"><output>${escapeHtml(String(Math.round(hudRadius)))}</output></label>
      </div>
    </div>`;
}

function colorPickerIconSvg() {
  return `<svg class="color-picker-icon" viewBox="0 0 24 24" aria-hidden="true">
    <path d="M12 3a9 9 0 0 0 0 18h1.4a2 2 0 0 0 1.7-3l-.2-.4a1.7 1.7 0 0 1 1.5-2.6H18a6 6 0 0 0 0-12h-6Z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>
    <circle cx="7.5" cy="10" r="1.3" fill="currentColor"/>
    <circle cx="10.5" cy="6.8" r="1.3" fill="currentColor"/>
    <circle cx="15" cy="7.8" r="1.3" fill="currentColor"/>
    <circle cx="16.8" cy="11.5" r="1.3" fill="currentColor"/>
  </svg>`;
}

function bindVisualTemplateToolbar() {
  const toolbar = $("videoTemplateForm").querySelector(".visual-toolbar-panel");
  if (!toolbar) return;
  toolbar.querySelectorAll("button[data-visual-command]").forEach((button) => {
    button.onclick = () => postVisualTemplateCommand(button.dataset.visualCommand, button.dataset.value || "", visualCommandScope(button));
  });
  toolbar.querySelectorAll("select[data-visual-command], input[data-visual-command]").forEach((input) => {
    input.oninput = () => {
      updateColorSwatch(input);
      updateVisualOutput(input);
      postVisualTemplateCommand(input.dataset.visualCommand, input.value, visualCommandScope(input));
    };
    input.onchange = () => {
      updateColorSwatch(input);
      updateVisualOutput(input);
      postVisualTemplateCommand(input.dataset.visualCommand, input.value, visualCommandScope(input));
    };
  });
}

function visualCommandScope(node) {
  return node.closest(".visual-hud-controls") ? "hud" : "text";
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

function postVisualTemplateCommand(command, value = "", scope = "") {
  pulseImageLoading("videoTemplatePreview", "应用模板参数...");
  $("videoTemplatePreview")?.contentWindow?.postMessage({
    type: "gasgx-video-template-command",
    command,
    value,
    scope,
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
  setImageLoading("videoTemplatePreview", "应用模板参数...");
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
  if (coverEditingContext === "ending") {
    applyEndingCoverTemplateUpdates(updates);
    return;
  }
  const template = coverTemplates[selectedCover];
  if (!template || !updates) return;
  Object.assign(template, updates);
  Object.entries(updates).forEach(([key, value]) => {
    const input = $("coverForm")?.querySelector(`[data-key="${key}"]`);
    if (input) input.value = value;
  });
  scheduleCoverTemplateSave();
}

function applyEndingCoverTemplateUpdates(updates) {
  const template = endingCoverTemplate();
  if (!template || !updates) return;
  Object.assign(template, updates);
  Object.entries(updates).forEach(([key, value]) => {
    const input = $("endingTemplateForm")?.querySelector(`[data-ending-cover-key="${key}"]`);
    if (input) input.value = value;
  });
  scheduleStateSave();
}

function applyCoverTextUpdates(text) {
  if (coverEditingContext === "ending") {
    applyEndingCoverTextUpdates(text);
    return;
  }
  if (!text) return;
  const template = coverTemplates[selectedCover];
  const fieldMap = { headline: "headline" };
  Object.entries(text).forEach(([key, value]) => {
    if (key === "singleLogo" || key === "singleSlogan" || key === "singleTitle") {
      const templateKey = {
        singleLogo: "single_cover_logo_text",
        singleSlogan: "single_cover_slogan_text",
        singleTitle: "single_cover_title_text",
      }[key];
      if (template) {
        template[templateKey] = value;
        const input = $("coverForm")?.querySelector(`[data-key="${templateKey}"]`);
        if (input) input.value = value;
      }
      return;
    }
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

function applyEndingCoverTextUpdates(text) {
  if (!text) return;
  const template = endingCoverTemplate();
  Object.entries(text).forEach(([key, value]) => {
    if (key === "singleLogo" || key === "singleSlogan" || key === "singleTitle") {
      const templateKey = {
        singleLogo: "single_cover_logo_text",
        singleSlogan: "single_cover_slogan_text",
        singleTitle: "single_cover_title_text",
      }[key];
      template[templateKey] = value;
      const input = $("endingTemplateForm")?.querySelector(`[data-ending-cover-key="${templateKey}"]`);
      if (input) input.value = value;
      if (templateKey === "single_cover_title_text") {
        $("followText").value = value;
        state.follow_text = value;
      }
    }
  });
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
    button.onclick = async () => {
      selectedModelImageUrl = button.dataset.modelImage || "";
      renderVideoTemplateBackgrounds();
      refreshVideoTemplatePreview();
      await refreshVideoTemplateGallery();
      refreshMainPreview();
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
  Object.entries(templates).forEach(([id, template], index) => {
    cards.push(`<div class="cover-card video-template-card ${id === selectedVideoTemplate ? "active" : ""}" data-id="${id}">${videoTemplateCardPreviewHtml(template)}<span>${id} / ${videoTemplateDisplayName(id, template, index)}</span></div>`);
  });
  $("videoTemplateGallery").innerHTML = cards.join("");
  $("videoTemplateGallery").querySelectorAll(".cover-card").forEach((card) => {
    card.onclick = async (event) => {
      if (event.target?.tagName === "VIDEO") {
        toggleTemplateCardVideo(event.target);
        return;
      }
      await selectVideoTemplate(card.dataset.id);
    };
  });
}

function videoTemplateCardPreviewHtml(template) {
  const imageUrl = selectedModelImageUrl || modelImages[0]?.url || "";
  if (!imageUrl) return `<div class="video-template-thumb empty"><span>暂无背景图</span></div>`;
  return `
    <div class="video-template-thumb">
      <img src="${escapeHtml(imageUrl)}" alt="">
      <div class="video-template-thumb-mask"></div>
      ${videoTemplateCardBarHtml(template, "slogan")}
      ${videoTemplateCardBarHtml(template, "title")}
      ${videoTemplateCardBarHtml(template, "hud")}
      ${videoTemplateCardTextHtml(template, "slogan", $("headline").value)}
      ${videoTemplateCardTextHtml(template, "title", $("subhead").value)}
      ${videoTemplateCardHudHtml(template)}
    </div>`;
}

function videoTemplateCardBarHtml(template, target) {
  if (target === "slogan" && !template?.show_slogan) return "";
  if (target === "title" && !template?.show_title) return "";
  if (target === "hud" && !template?.show_hud) return "";
  const x = target === "slogan" ? Number(template.slogan_bg_x ?? 0)
    : target === "title" ? Number(template.title_bg_x ?? 0)
    : Number(template.hud_bar_x ?? 0);
  const y = target === "slogan" ? Number(template.slogan_bg_y ?? template.slogan_y ?? 0)
    : target === "title" ? Number(template.title_bg_y ?? template.title_y ?? 0)
    : Number(template.hud_bar_y ?? 0);
  const width = target === "slogan" ? Number(template.slogan_bg_width ?? 1080)
    : target === "title" ? Number(template.title_bg_width ?? 1080)
    : Number(template.hud_bar_width ?? 1080);
  const height = target === "slogan" ? Number(template.slogan_bg_height ?? 80)
    : target === "title" ? Number(template.title_bg_height ?? template.slogan_bg_height ?? 92)
    : Number(template.hud_bar_height ?? 120);
  const color = target === "slogan" ? (template.slogan_bg_color || template.hud_bar_color || "#0E1A10")
    : target === "title" ? (template.title_bg_color || template.hud_bar_color || "#0E1A10")
    : (template.hud_bar_color || "#0E1A10");
  const opacity = target === "slogan" ? Number(template.slogan_bg_opacity ?? 0.62)
    : target === "title" ? Number(template.title_bg_opacity ?? template.slogan_bg_opacity ?? 0.62)
    : Number(template.hud_bar_opacity ?? 0.68);
  const radius = target === "slogan" ? Number(template.slogan_bg_radius ?? template.hud_bar_radius ?? 10)
    : target === "title" ? Number(template.title_bg_radius ?? template.hud_bar_radius ?? 10)
    : Number(template.hud_bar_radius ?? 10);
  return `<div class="video-template-thumb-bar" style="${videoTemplateCardBarStyle(x, y, width, height, color, opacity, radius)}"></div>`;
}

function videoTemplateCardTextHtml(template, target, value) {
  const visibleKey = target === "slogan" ? "show_slogan" : "show_title";
  if (!template?.[visibleKey]) return "";
  const x = Number(template[`${target}_x`] ?? 0);
  const y = Number(template[`${target}_y`] ?? 0);
  const size = Number(template[`${target}_font_size`] ?? 36);
  const color = template[`${target}_color`] || (target === "slogan" ? template.primary_color : template.secondary_color) || "#ffffff";
  return `<div class="video-template-thumb-text" style="${videoTemplateCardTextStyle(x, y, size, color)}">${escapeHtml(value || "")}</div>`;
}

function videoTemplateCardHudHtml(template) {
  if (!template?.show_hud) return "";
  const x = Number(template.hud_x ?? 0);
  const y = Number(template.hud_y ?? 0);
  const size = Number(template.hud_font_size ?? 30);
  const color = template.hud_color || template.primary_color || "#ffffff";
  return `<div class="video-template-thumb-hud" style="${videoTemplateCardTextStyle(x, y, size, color)}">${escapeHtml($("hudText").value || "")}</div>`;
}

function videoTemplateCardTextStyle(x, y, size, color) {
  const left = Math.max(0, Math.min(100, x / 1080 * 100));
  const top = Math.max(0, Math.min(100, y / 1920 * 100));
  const fontSize = Math.max(6, Math.min(14, size / 1920 * 154));
  return `left:${left.toFixed(2)}%;top:${top.toFixed(2)}%;font-size:${fontSize.toFixed(1)}px;color:${escapeHtml(color)};`;
}

function videoTemplateCardBarStyle(x, y, width, height, color, opacity, radius) {
  const left = Math.max(0, Math.min(100, x / 1080 * 100));
  const top = Math.max(0, Math.min(100, y / 1920 * 100));
  const barWidth = Math.max(8, Math.min(100, width / 1080 * 100));
  const barHeight = Math.max(3, Math.min(100, height / 1920 * 100));
  const alpha = Math.max(0, Math.min(1, opacity));
  const corner = Math.max(0, Math.min(10, radius / 1920 * 154));
  return `left:${left.toFixed(2)}%;top:${top.toFixed(2)}%;width:${barWidth.toFixed(2)}%;height:${barHeight.toFixed(2)}%;background:${hexToRgba(color, alpha)};border-radius:${corner.toFixed(1)}px;`;
}

function hexToRgba(value, opacity) {
  const hex = String(value || "#0E1A10").trim().replace("#", "");
  const normalized = hex.length === 3 ? hex.split("").map((char) => char + char).join("") : hex.padEnd(6, "0").slice(0, 6);
  const red = parseInt(normalized.slice(0, 2), 16) || 0;
  const green = parseInt(normalized.slice(2, 4), 16) || 0;
  const blue = parseInt(normalized.slice(4, 6), 16) || 0;
  return `rgba(${red}, ${green}, ${blue}, ${Math.max(0, Math.min(1, Number(opacity)))})`;
}

function videoTemplatePreviewVideos() {
  return sourcePreviewVideos.filter((item) => item?.path);
}

function toggleTemplateCardVideo(video) {
  if (!video) return;
  document.querySelectorAll("#videoTemplateGallery video").forEach((item) => {
    if (item !== video) {
      item.pause();
      item.currentTime = 0;
      item.closest(".cover-card")?.classList.remove("is-playing");
    }
  });
  if (video.paused) {
    video.play();
    video.closest(".cover-card")?.classList.add("is-playing");
  } else {
    video.pause();
    video.currentTime = 0;
    video.closest(".cover-card")?.classList.remove("is-playing");
  }
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
    log(`已保存正文模板：${displayTemplateName(templates[selectedVideoTemplate].name || selectedVideoTemplate)}`);
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
  const nextName = videoTemplateDisplayName(nextId, {}, Object.keys(templates).length);
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

function showTemplateActionStatus(message, formId = "videoTemplateForm") {
  const actions = $(formId)?.querySelector(".template-actions");
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
  applyIndependentCoverDefaults(template);
  refreshPhonePreviewFrame("coverPreview", {
    template,
    cover_mode: true,
    slogan: $("headline").value,
    title: $("subhead").value,
    headline: $("headline").value,
    subhead: $("subhead").value,
    hud_text: $("hudText").value,
    background_image_url: selectedModelImageUrl || modelImages[0]?.url || "",
    background_image_urls: modelImages.map((image) => image.url).filter(Boolean),
    show_template_mask: true,
  });
  clearImageLoading("coverPreview");
}

async function selectCoverTemplate(templateId) {
  if (!templateId || !coverTemplates[templateId]) return;
  selectedCover = templateId;
  setImageLoading("coverPreview", "切换第一屏模板...");
  if ($("sidebarCoverTemplate")) $("sidebarCoverTemplate").value = templateId;
  renderCoverSelector();
  renderCoverEditor();
  await saveTemplateSelection();
  await refreshMainPreview();
}

function previewPayload(template) {
  const payload = {...template};
  applyIndependentCoverDefaults(payload);
  payload.cta = "";
  return {template: payload, cover_mode: true, slogan: $("headline").value, title: $("subhead").value, headline: $("headline").value, subhead: $("subhead").value, hud_text: $("hudText").value, background_image_url: selectedModelImageUrl || modelImages[0]?.url || "", background_image_urls: modelImages.map((image) => image.url).filter(Boolean)};
}

async function saveCoverAsNewTemplate() {
  const sourceTemplate = coverTemplates[selectedCover];
  if (!sourceTemplate) return;
  const previousCover = selectedCover;
  const previousTemplates = {...coverTemplates};
  const nextMeta = nextCoverTemplateMeta(coverTemplates);
  const button = $("saveCoverAsNew");
  const label = button?.textContent || "新建保存";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("新建中...");
  }
  const newTemplate = JSON.parse(JSON.stringify(sourceTemplate));
  applyIndependentCoverDefaults(newTemplate);
  newTemplate.name = nextMeta.name;
  newTemplate.cover_layout = "single_video";
  coverTemplates = {...coverTemplates, [nextMeta.id]: newTemplate};
  selectedCover = nextMeta.id;
  state.cover_template_id = selectedCover;
  state.cover_templates = coverTemplates;
  try {
    await api("/api/video-matrix/cover-templates", {method:"PUT", headers:{"Content-Type":"application/json"}, body: JSON.stringify({templates: coverTemplates, selected_cover: selectedCover})});
    await saveTemplateSelection();
    pendingTemplateSave = "";
    renderCoverSelector();
    renderCoverEditor();
    await refreshMainPreview();
    log(`已新建独立封面模板：${nextMeta.name}`);
    showTemplateActionStatus("新建保存成功", "coverForm");
  } catch (error) {
    coverTemplates = previousTemplates;
    selectedCover = previousCover;
    state.cover_template_id = previousCover;
    state.cover_templates = previousTemplates;
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    log(`第一屏新模板保存失败：${error.message}`);
  }
}

async function saveCurrentCoverTemplate() {
  const template = coverTemplates[selectedCover];
  if (!template) return;
  const button = $("saveCover");
  const label = button?.textContent || "保存";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("保存中...");
  }
  try {
    applyIndependentCoverDefaults(template);
    await api(`/api/video-matrix/cover-templates/${selectedCover}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(template)});
    await saveTemplateSelection();
    pendingTemplateSave = "";
    log(`已保存第一屏模板：${template.name || selectedCover}`);
    renderCoverSelector();
    renderCoverEditor();
    await refreshMainPreview();
    showTemplateActionStatus("保存成功", "coverForm");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    log(`第一屏模板保存失败：${error.message}`);
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
      cover_layout: "profile",
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
    if (!(await runPreflightChecks(statePayload))) return;
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

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

async function runPreflightChecks(statePayload) {
  const modal = $("generationPreflightModal");
  const body = $("generationPreflightBody");
  const actions = modal?.querySelector(".preflight-actions");
  if (!modal || !body || !actions) return true;
  let liveData = null;
  const checks = buildPreflightChecks(
    statePayload,
    () => liveData,
    (data) => { liveData = data; }
  );
  body.innerHTML = preflightChecksHtml(checks);
  $("preflightCancel").hidden = true;
  $("preflightContinue").hidden = true;
  actions.classList.add("is-running");
  actions.classList.remove("is-complete");
  modal.classList.remove("hidden");
  document.body.classList.add("confirm-modal-open");

  let hasFail = false;
  for (let index = 0; index < checks.length; index += 1) {
    setPreflightStepStatus(index, "checking", "检查中...");
    await wait(80);
    let result;
    try {
      result = await checks[index].run();
    } catch (error) {
      result = { status: "fail", detail: error.message || "预检执行失败" };
    }
    const status = result?.status || "pass";
    if (status === "fail") hasFail = true;
    setPreflightStepStatus(index, status, result?.detail || checks[index].readyText);
    await wait(50);
  }

  actions.classList.remove("is-running");
  if (!hasFail) {
    actions.classList.add("is-complete");
    setPreflightSummary("预检通过", "当前保存条件未发现会阻断提交的问题，正在进入最终确认。");
    await wait(420);
    closePreflightModal();
    return true;
  }

  setPreflightSummary("预检未通过", "请按红色节点提示修正后再点击立即生成。");
  $("preflightCancel").hidden = false;
  return new Promise((resolve) => {
    const close = () => {
      closePreflightModal();
      $("preflightCancel").onclick = null;
      $("preflightClose").onclick = null;
      resolve(false);
    };
    $("preflightCancel").onclick = close;
    $("preflightClose").onclick = close;
  });
}

function closePreflightModal() {
  const modal = $("generationPreflightModal");
  modal?.classList.add("hidden");
  document.body.classList.remove("confirm-modal-open");
}

function buildPreflightChecks(statePayload, getLiveData, setLiveData) {
  const categories = materialCategories({ settings });
  const categoryNames = Object.fromEntries(categories.map((category) => [category.id, category.label]));
  const activeIds = Array.isArray(statePayload.active_category_ids) ? statePayload.active_category_ids : [];
  const composition = Array.isArray(statePayload.composition_sequence) ? statePayload.composition_sequence : [];
  const selectedEndingNames = Array.isArray(statePayload.ending_template_ids) ? statePayload.ending_template_ids.filter(Boolean) : [];
  return [
    {
      title: "连接生成接口",
      pendingText: "读取最新素材、模板、BGM 和片尾目录状态。",
      readyText: "接口可用，已读取最新状态。",
      run: async () => {
        const data = await api("/api/video-matrix/state");
        setLiveData(data);
        return { status: "pass", detail: "接口可用，已读取最新状态。" };
      },
    },
    {
      title: "输出参数",
      pendingText: "检查数量、并行、帧率、节拍分析时长和输出目录。",
      readyText: "输出参数完整。",
      run: () => {
        const formats = Array.isArray(statePayload.output_options) ? statePayload.output_options.filter(Boolean) : [];
        if (!Number.isFinite(statePayload.output_count) || statePayload.output_count < 1) return { status: "fail", detail: "生成数量必须大于 0。" };
        if (!Number.isFinite(statePayload.max_workers) || statePayload.max_workers < 1) return { status: "fail", detail: "并行线程必须大于 0。" };
        if (![30, 60].includes(Number(statePayload.target_fps))) return { status: "fail", detail: "目标帧率只能是 30 或 60。" };
        if (!formats.length) return { status: "fail", detail: "至少需要选择一种输出格式。" };
        if (!String(statePayload.output_root || "").trim()) return { status: "fail", detail: "最终视频生成目录不能为空。" };
        if (Number(statePayload.video_duration_min) > Number(statePayload.video_duration_max)) return { status: "fail", detail: "最小节拍分析时长不能大于最大节拍分析时长。" };
        return { status: "pass", detail: `${statePayload.output_count} 条 / ${statePayload.max_workers} 线程 / ${statePayload.target_fps}fps / ${formats.join(", ")}` };
      },
    },
    {
      title: "模板可用性",
      pendingText: "检查第一屏封面模板和正文叠层模板是否存在。",
      readyText: "模板配置可用。",
      run: () => {
        const live = getLiveData() || {};
        const liveVideoTemplates = live.templates || templates || {};
        const liveCoverTemplates = live.cover_templates || coverTemplates || {};
        if (!liveVideoTemplates[statePayload.template_id]) return { status: "fail", detail: `正文叠层模板不存在：${statePayload.template_id || "未选择"}` };
        if (!liveCoverTemplates[statePayload.cover_template_id]) return { status: "fail", detail: `第一屏封面模板不存在：${statePayload.cover_template_id || "未选择"}` };
        return { status: "pass", detail: `正文 ${statePayload.template_id} / 封面 ${statePayload.cover_template_id}` };
      },
    },
    {
      title: "本地 BGM",
      pendingText: "检查本地背景音乐库是否有可用 MP3。",
      readyText: "BGM 可用。",
      run: () => {
        const live = getLiveData() || {};
        const localBgm = Array.isArray(live.local_bgm) ? live.local_bgm : bgmLibraryState.local;
        if (!localBgm.length) return { status: "fail", detail: "本地背景音乐库没有可用 MP3。" };
        if (statePayload.bgm_library_id && !localBgm.includes(statePayload.bgm_library_id)) return { status: "fail", detail: `已选 BGM 不在本地曲库：${statePayload.bgm_library_id}` };
        return { status: "pass", detail: statePayload.bgm_library_id ? `已选 ${statePayload.bgm_library_id}` : `本地曲库 ${localBgm.length} 首，生成时随机取 1 首。` };
      },
    },
    {
      title: "分类素材",
      pendingText: "检查启用分类是否有可用素材。",
      readyText: "启用分类都有素材。",
      run: () => {
        const live = getLiveData() || {};
        const counts = live.category_counts || {};
        if (!activeIds.length) return { status: "fail", detail: "至少需要启用一个素材分类。" };
        const empty = activeIds.filter((id) => Number(counts[id] || 0) < 1);
        if (empty.length) return { status: "fail", detail: `这些分类没有素材：${empty.map((id) => categoryNames[id] || id).join("、")}` };
        const total = activeIds.reduce((sum, id) => sum + Number(counts[id] || 0), 0);
        return { status: "pass", detail: `${activeIds.length} 个分类可用，共 ${total} 个素材。` };
      },
    },
    {
      title: "生成结构",
      pendingText: "检查每个片段分类和片段秒数。",
      readyText: "生成结构可用。",
      run: () => {
        const live = getLiveData() || {};
        const counts = live.category_counts || {};
        if (!composition.length) return { status: "fail", detail: "生成结构不能为空。" };
        const invalidDuration = composition.find((row) => !Number.isFinite(Number(row.duration)) || Number(row.duration) <= 0);
        if (invalidDuration) return { status: "fail", detail: "生成结构里存在无效片段秒数。" };
        const missing = composition
          .map((row) => row.category_id)
          .filter((id) => !activeIds.includes(id) || Number(counts[id] || 0) < 1);
        if (missing.length) return { status: "fail", detail: `结构引用了未启用或无素材分类：${[...new Set(missing)].map((id) => categoryNames[id] || id).join("、")}` };
        const seconds = composition.reduce((sum, row) => sum + Number(row.duration || 0), 0);
        const status = seconds > Number(statePayload.video_duration_max || 0) ? "warn" : "pass";
        const detail = `结构 ${composition.length} 段，合计约 ${seconds.toFixed(1)} 秒。${status === "warn" ? "节拍分析会按结构总时长兜底。" : ""}`;
        return { status, detail };
      },
    },
    {
      title: "片尾素材",
      pendingText: "检查片尾封面模板或随机片尾素材是否可读取。",
      readyText: "片尾配置可用。",
      run: () => {
        const mode = statePayload.ending_template_mode || "dynamic";
        if (mode === "dynamic") {
          if (!statePayload.ending_cover_template) return { status: "fail", detail: "动态片尾缺少片尾封面模板配置。" };
          return { status: "pass", detail: "使用动态片尾封面模板。" };
        }
        const live = getLiveData() || {};
        const endingItems = Array.isArray(live.ending_templates) ? live.ending_templates : endingTemplateState.local;
        const endingNames = new Set(endingItems.map((item) => item.name));
        if (!endingItems.length) return { status: "fail", detail: "片尾素材目录没有可用素材。" };
        if (selectedEndingNames.length) {
          const missing = selectedEndingNames.filter((name) => !endingNames.has(name));
          if (missing.length) return { status: "fail", detail: `已选片尾素材不存在：${missing.join("、")}` };
          return { status: "pass", detail: `随机范围 ${selectedEndingNames.length} 个已选片尾素材。` };
        }
        return { status: "pass", detail: `未指定片尾素材，随机范围为目录内 ${endingItems.length} 个素材。` };
      },
    },
  ];
}

function preflightChecksHtml(checks) {
  return `
    <div class="preflight-summary">
      <strong data-preflight-summary-title>正在预检</strong>
      <span data-preflight-summary-detail>逐项确认当前生成条件，失败项会阻止提交。</span>
    </div>
    <ol class="preflight-list">
      ${checks.map((check, index) => `
        <li class="preflight-step" data-preflight-step="${index}">
          <span class="preflight-status" data-preflight-status>·</span>
          <div>
            <strong>${escapeHtml(check.title)}</strong>
            <small data-preflight-detail>${escapeHtml(check.pendingText || "")}</small>
          </div>
          <span class="preflight-badge" data-preflight-badge>等待</span>
        </li>
      `).join("")}
    </ol>
  `;
}

function setPreflightSummary(title, detail) {
  const titleNode = document.querySelector("[data-preflight-summary-title]");
  const detailNode = document.querySelector("[data-preflight-summary-detail]");
  if (titleNode) titleNode.textContent = title;
  if (detailNode) detailNode.textContent = detail;
}

function setPreflightStepStatus(index, status, detail) {
  const node = document.querySelector(`[data-preflight-step="${index}"]`);
  if (!node) return;
  node.classList.remove("checking", "pass", "warn", "fail");
  node.classList.add(status);
  const icon = node.querySelector("[data-preflight-status]");
  const badge = node.querySelector("[data-preflight-badge]");
  const detailNode = node.querySelector("[data-preflight-detail]");
  const icons = { checking: "...", pass: "✓", warn: "!", fail: "×" };
  const labels = { checking: "检查中", pass: "通过", warn: "提醒", fail: "失败" };
  if (icon) icon.textContent = icons[status] || "·";
  if (badge) badge.textContent = labels[status] || "等待";
  if (detailNode) detailNode.textContent = detail || "";
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
      <div><span>最小节拍分析</span><strong>${statePayload.video_duration_min} 秒</strong></div>
      <div><span>最大节拍分析</span><strong>${statePayload.video_duration_max} 秒</strong></div>
      <div><span>目标帧率</span><strong>${statePayload.target_fps} fps</strong></div>
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
        <li>将候选素材归一化为 1080:1920、${statePayload.target_fps}fps 的短视频片段库。</li>
        <li>按“生成结构”的分类顺序和片段秒数，为每条视频抽取不同素材片段。</li>
        <li>分析本地背景音乐节拍，把片段切换点尽量对齐节奏窗口。</li>
        <li>按当前模板、HUD 文本和片尾文案并行渲染，导出到最终视频目录。</li>
      </ol>
    </section>
  `;
}

function collectState() {
  const categories = Array.isArray(settings.material_categories) ? settings.material_categories : [];
  updateCompositionState();
  const endingCopyText = endingCopyTextValue();
  return {
    output_count: Number($("outputCount").value), max_workers: Number($("maxWorkers").value),
    video_duration_min: Number($("videoDurationMin").value || settings.video_duration_min || 8),
    video_duration_max: Number($("videoDurationMax").value || settings.video_duration_max || 12),
    target_fps: Number(radioValue("target_fps") || settings.target_fps || 60),
    output_options: [$("outputOptions").value], output_root: outputRootPath(),
    template_id: selectedVideoTemplate, cover_template_id: selectedCover, copy_language: radioValue("copy_language"),
    source_mode: radioValue("source_mode") || "Category folders",
    headline: $("headline").value, subhead: $("subhead").value,
    follow_text: endingCopyText, hud_text: $("hudText").value,
    ending_template_mode: endingTemplateMode(),
    ending_template_id: endingTemplateSelectedName(),
    ending_template_ids: selectedEndingTemplateNames(),
    ending_template_dir: endingTemplateState.directory,
    ending_cover_template: state.ending_cover_template,
    bgm_source: "Local library", bgm_library_id: selectedBgmLibraryId(),
    composition_sequence: state.composition_sequence,
    composition_customized: Boolean(state.composition_customized),
    active_category_ids: selectedActiveCategoryIds(categories),
    recent_limits: Object.fromEntries(categories.map((category) => [
      category.id,
      clamp(Number(state.recent_limits?.[category.id] || settings.recent_limits?.[category.id] || 8), 1, 10),
    ]))
  };
}

function endingCopyTextValue() {
  const input = document.querySelector('[data-ending-cover-key="single_cover_title_text"]');
  const value = input?.value ?? state.ending_cover_template?.single_cover_title_text ?? $("followText").value ?? state.follow_text ?? "";
  return String(value).trim();
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
  state.bgm_library_id = localBgm.includes(state.bgm_library_id) ? state.bgm_library_id : "";
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
    ? (state.bgm_library_id
      ? `已选中唯一背景音乐：${state.bgm_library_id}`
      : `已找到 ${localBgm.length} 首本地音频，未选中时生成会随机取 1 首：${localBgmDir}`)
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
      <li class="bgm-local-item ${name === selectedBgmLibraryId() ? "is-selected" : ""}" data-bgm-name="${escapeHtml(name)}">
        <button type="button" class="bgm-local-select" data-bgm-select="${escapeHtml(name)}" aria-pressed="${name === selectedBgmLibraryId() ? "true" : "false"}" title="设为本次唯一背景音乐">
          <span class="bgm-select-check" aria-hidden="true"></span>
          <span>${escapeHtml(name)}</span>
        </button>
        <audio controls preload="none" src="/api/video-matrix/bgm/${encodeURIComponent(name)}"></audio>
      </li>`).join("")
    : "<li>暂无本地 MP3 文件</li>";
  const pixabayList = bgmLibraryState.pixabay.length
    ? renderPixabayTracks()
    : `<span class="pixabay-empty">正在准备 Pixabay industry 最新曲库。</span>`;
  panel.innerHTML = `
    <div class="bgm-popover-head">
      <div>
        <strong>本地曲库列表</strong>
        <small title="${escapeHtml(bgmLibraryState.directory)}">下载目录：${escapeHtml(shortPath(bgmLibraryState.directory))}</small>
      </div>
      <button id="toggleBgmLibrarySize" type="button" class="secondary">收起</button>
    </div>
    <section class="bgm-local-section">
      <strong>本地曲库</strong>
      <ul>${localList}</ul>
    </section>
    <section class="bgm-pixabay-section">
      <strong>网络曲库</strong>
      <button id="loadPixabayTracksInline" class="pixabay-refresh-button" type="button">抓取 Pixabay industry 前 10 首</button>
      <div id="pixabayTrackList" class="pixabay-track-list">${pixabayList}</div>
    </section>
  `;
  $("toggleBgmLibrarySize").onclick = toggleBgmLibrarySize;
  $("loadPixabayTracksInline")?.addEventListener("click", loadPixabayTracks);
  panel.querySelectorAll("[data-bgm-select]").forEach((button) => {
    button.onclick = () => selectBgmLibraryId(button.dataset.bgmSelect || "");
  });
  panel.querySelectorAll("[data-pixabay-open]").forEach((button) => {
    button.onclick = () => window.open(button.dataset.pixabayOpen, "_blank", "noopener");
  });
  bindExclusiveBgmAudioPlayback(panel);
  loadPixabayTracks();
}
function selectedBgmLibraryId() {
  return bgmLibraryState.local.includes(state.bgm_library_id) ? state.bgm_library_id : "";
}
function selectBgmLibraryId(name) {
  state.bgm_library_id = selectedBgmLibraryId() === name ? "" : name;
  scheduleStateSave();
  renderBgm({ local_bgm: bgmLibraryState.local, local_bgm_dir: bgmLibraryState.directory, bgm_library: {} });
  const panel = $("bgmLibraryPopover");
  panel.classList.remove("hidden");
  panel.classList.add("modal");
  document.body.classList.add("bgm-modal-open");
  toggleBgmLibraryPopover();
  toggleBgmLibraryPopover();
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
    <article class="pixabay-track ${track.audio_url ? "" : "audio-unavailable"}">
      <div class="pixabay-track-main">
        <strong>${index + 1}. ${escapeHtml(track.title)}</strong>
        <span>${escapeHtml(track.artist)} / ${escapeHtml(track.duration)}</span>
        ${track.audio_url ? `<audio controls preload="none" src="${escapeHtml(track.audio_url)}"></audio>` : `<small>音频地址暂不可用</small>`}
      </div>
      <button type="button" data-pixabay-download="${escapeHtml(track.audio_url || "")}" data-pixabay-title="${escapeHtml(track.title || "pixabay-industry")}" ${track.audio_url ? "" : "disabled"}>下载到本地</button>
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
    list.querySelectorAll("[data-pixabay-download]").forEach((button) => {
      button.onclick = () => downloadPixabayTrack(button);
    });
    bindExclusiveBgmAudioPlayback($("bgmLibraryPopover"));
  } catch (error) {
    list.textContent = error.message;
  }
}
async function downloadPixabayTrack(button) {
  const url = button.dataset.pixabayDownload || "";
  if (!url) return;
  const label = button.textContent;
  button.disabled = true;
  button.classList.add("is-loading");
  button.innerHTML = buttonLoadingInline("下载中...");
  try {
    const result = await api("/api/video-matrix/bgm/download", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({url, filename: `${button.dataset.pixabayTitle || "pixabay-industry"}.mp3`}),
    });
    button.textContent = `已下载：${result.filename}`;
    const data = await api("/api/video-matrix/state");
    state = data.ui_state; settings = data.settings;
    renderBgm(data);
    const panel = $("bgmLibraryPopover");
    panel.classList.remove("hidden");
    panel.classList.add("modal");
    document.body.classList.add("bgm-modal-open");
    toggleBgmLibraryPopover();
    toggleBgmLibraryPopover();
  } catch (error) {
    button.textContent = error.message;
  } finally {
    window.setTimeout(() => {
      if (!button.isConnected) return;
      button.disabled = false;
      button.classList.remove("is-loading");
      if (button.textContent !== label) button.textContent = label;
    }, 1600);
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
  $("jobStatusTitle").textContent = localizedJobTitle(job, stage);
  $("jobPercent").textContent = `${percent}%`;
  $("jobProgressFill").style.width = `${percent}%`;
  $("jobMessage").textContent = localizedJobMessage(job, stage);
  $("jobLog").classList.toggle("error", isError);
  $("jobMessage").classList.toggle("error-message", isError);
  $("jobSteps").innerHTML = jobStepLabels.map(([key, label, stepPercent, stageKeys]) => {
    const done = percent >= stepPercent || job.status === "complete";
    const active = stageKeys.includes(stage) && percent >= stepPercent && !done || key === stage || (stage === "render" && stageKeys.includes("render") && percent >= stepPercent && percent < 97);
    return `<li class="${done ? "done" : ""} ${active ? "active" : ""}"><span></span>${label}</li>`;
  }).join("");
}
function log(text) { updateJobStatus({ status: "running", stage: "queued", progress: 0, message: text }); }
function localizedJobTitle(job, stage) {
  if (job.status === "error") return "生成失败，请查看下方提示";
  if (job.status === "complete") return "生成完成，视频已导出";
  const titles = {
    queued: "任务已提交，正在等待开始",
    ingestion: "正在扫描并整理素材",
    hud: "正在准备视频数据和字幕",
    beat: "正在分析背景音乐节奏",
    planning: "正在规划混剪方案",
    render: "正在生成视频，请耐心等待",
    finalizing: "正在整理导出文件",
  };
  return titles[stage] || "正在处理，请稍等";
}
function localizedJobMessage(job, stage) {
  if (job.error) return job.error;
  const message = String(job.message || "").trim();
  if (!message) return jobMessages[stage] || "正在处理，请稍等。";
  if (backendJobMessageMap[message]) return backendJobMessageMap[message];
  const rendered = message.match(/^Rendered video (\d+)\/(\d+)$/);
  if (rendered) return `正在生成视频：已完成 ${rendered[1]} / ${rendered[2]} 条。`;
  const rendering = message.match(/^Rendering (\d+) videos with (\d+) workers$/);
  if (rendering) return `正在启动视频生成：共 ${rendering[1]} 条，并行线程 ${rendering[2]} 个。`;
  const completed = message.match(/^Completed (\d+) exports$/);
  if (completed) return `生成完成，已导出 ${completed[1]} 条视频。`;
  return displayTemplateName(message);
}
function displayTemplateName(value) {
  return String(value || "")
    .replace(/\bCopy\b/g, "副本")
    .replace(/\bMode\b/g, "模式")
    .replace(/\bCenter\b/g, "居中")
    .replace(/\bBrand\b/g, "品牌")
    .replace(/\bClean Data\b/g, "清爽数据")
    .replace(/\bImpact Hud\b/g, "冲击 HUD")
    .replace(/_/g, " ");
}
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
  coverEditingContext = event.source === $("endingTemplatePreview")?.contentWindow ? "ending" : "cover";
  applyCoverTemplateUpdates(event.data.updates);
});
window.addEventListener("message", (event) => {
  if (event.origin !== window.location.origin || event.data?.type !== "gasgx-cover-template-text-update") return;
  coverEditingContext = event.source === $("endingTemplatePreview")?.contentWindow ? "ending" : "cover";
  applyCoverTextUpdates(event.data.text);
});

init().catch((err) => log(err.message));
