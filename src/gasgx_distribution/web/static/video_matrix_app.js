const $ = (id) => document.getElementById(id);
let state = {};
let templates = {};
let coverTemplates = {};
let selectedCover = "";
let selectedVideoTemplate = "";
let settings = {};
let lastPreviewPath = "";
let bgmLibraryState = { local: [], directory: "", links: [], pixabay: [] };

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
  ["show_hud", "显示 HUD", "checkbox"],
  ["show_slogan", "显示口号", "checkbox"],
  ["show_title", "显示标题", "checkbox"],
  ["hud_bar_y", "HUD 背景 Y", "range", 0, 1920],
  ["hud_bar_height", "HUD 背景高度", "range", 40, 320],
  ["hud_x", "HUD 文本 X", "range", 0, 1080],
  ["hud_y", "HUD 文本 Y", "range", 0, 1920],
  ["hud_font_size", "HUD 字号", "range", 12, 72],
  ["slogan_x", "口号 X", "range", 0, 1080],
  ["slogan_y", "口号 Y", "range", 0, 1920],
  ["slogan_font_size", "口号字号", "range", 18, 96],
  ["title_x", "标题 X", "range", 0, 1080],
  ["title_y", "标题 Y", "range", 0, 1920],
  ["title_font_size", "标题字号", "range", 12, 72],
  ["hud_bar_color", "HUD 背景色", "color"],
  ["hud_bar_opacity", "HUD 透明度", "rangeFloat", 0, 1],
  ["primary_color", "主文字色", "color"],
  ["secondary_color", "副文字色", "color"],
];

async function api(path, options = {}) {
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function init() {
  const data = await api("/api/video-matrix/state");
  state = data.ui_state; templates = data.templates; coverTemplates = data.cover_templates; settings = data.settings;
  selectedCover = state.cover_template_id || Object.keys(coverTemplates)[0];
  selectedVideoTemplate = state.template_id || Object.keys(templates)[0];
  renderSidebar(data);
  renderSource(data);
  renderTextSettings();
  renderVideoTemplateEditor();
  renderCoverSelector();
  renderCoverEditor();
  await refreshAllPreviews();
}

function renderSidebar(data) {
  $("outputCount").value = state.output_count || 3;
  $("maxWorkers").value = state.max_workers || 3;
  syncNumber("outputCount");
  syncRange("maxWorkers");
  $("outputRoot").dataset.fullPath = settings.output_root;
  $("outputRoot").title = settings.output_root;
  $("outputRoot").value = shortPath(settings.output_root);
  $("outputOptions").value = (state.output_options || ["mp4"])[0] || "mp4";
  $("videoTemplate").innerHTML = Object.entries(templates).map(([id, t]) => `<option value="${id}">${t.name || id}</option>`).join("");
  $("videoTemplate").value = selectedVideoTemplate;
  $("videoTemplate").onchange = async () => {
    selectedVideoTemplate = $("videoTemplate").value;
    renderVideoTemplateEditor();
    await refreshVideoTemplatePreview();
  };
  $("openOutput").onclick = () => openFolder(outputRootPath());
  renderRadio("languageGroup", "copy_language", [["zh", "中文"], ["en", "英文"], ["ru", "俄文"]], state.copy_language || "zh");
  renderBgm(data);
  $("saveState").onclick = toggleBgmLibraryPopover;
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
  $("sourceDirs").innerHTML = categories.map((category) =>
    `<div class="dir-row"><span class="badge">${escapeHtml(category.label)}</span><code title="${escapeHtml(data.source_dirs[category.id] || "")}">${escapeHtml(shortPath(data.source_dirs[category.id] || ""))}</code><button data-path="${escapeHtml(data.source_dirs[category.id] || "")}">打开</button></div>`).join("");
  $("sourceDirs").querySelectorAll("button").forEach((btn) => btn.onclick = () => openFolder(btn.dataset.path));
  $("addCategory").onclick = addMaterialCategory;
  $("sourceCounts").textContent = "算法：按视频碎片分类目录读取素材，优先取每类最新文件，再按节奏窗口自动组合混剪。";
  renderRadio("sourceModeGroup", "source_mode", [["Category folders", "智能分类轮换算法"]], "Category folders", updateSourceMode);
  $("recentLimits").innerHTML = categories.map((category) =>
    `<label>${escapeHtml(category.label)}最新素材<input id="${category.id}" type="range" min="1" max="50" value="${settings.recent_limits[category.id] || 8}"><output id="${category.id}Value"></output></label>`).join("");
  categories.forEach((category) => syncRange(category.id));
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
  log(`已添加素材目录：${label}`);
}

function renderTextSettings() {
  $("headline").value = state.headline || "";
  $("subhead").value = state.subhead || "";
  $("cta").value = state.cta || "";
  $("followText").value = state.follow_text || "";
  $("hudText").value = state.hud_text || "";
  ["headline", "subhead", "cta", "hudText"].forEach((id) => $(id).addEventListener("input", debounce(refreshAllPreviews, 250)));
  $("generateBtn").onclick = generate;
}

function renderCoverSelector() {
  $("coverSelector").innerHTML = Object.entries(coverTemplates).map(([id, item]) =>
    `<button class="${id === selectedCover ? "active" : ""}" data-id="${id}">${item.name || id}</button>`).join("");
  $("coverSelector").querySelectorAll("button").forEach((btn) => btn.onclick = async () => {
    selectedCover = btn.dataset.id;
    renderCoverSelector(); renderCoverEditor(); await refreshAllPreviews();
  });
  $("coverStatus").textContent = `第一屏封面：${coverTemplates[selectedCover]?.name || selectedCover}`;
}

function renderCoverEditor() {
  const t = coverTemplates[selectedCover];
  $("previewCaption").textContent = `${selectedCover} / ${t.name || selectedCover}`;
  const html = [`<h3>当前模板调整</h3>`];
  for (const [key, label, type, min, max] of coverFields) {
    const value = t[key] ?? "";
    if (type === "select") html.push(`<label>${label}<select data-key="${key}"><option value="left">left</option><option value="center">center</option></select></label>`);
    else if (type === "range") html.push(`<label>${label}<input data-key="${key}" type="range" min="${min}" max="${max}" value="${value}"><output>${value}</output></label>`);
    else if (type === "rangeFloat") html.push(`<label>${label}<input data-key="${key}" type="range" min="${min}" max="${max}" step="0.01" value="${value}"><output>${value}</output></label>`);
    else html.push(`<label>${label}<input data-key="${key}" type="${type}" value="${escapeHtml(value)}"></label>`);
  }
  html.push(`<button type="button" id="saveCover">保存当前第一屏模板</button>`);
  $("coverForm").innerHTML = html.join("");
  $("coverForm").querySelectorAll("[data-key]").forEach((input) => {
    input.value = t[input.dataset.key] ?? input.value;
    input.oninput = () => {
      const key = input.dataset.key;
      t[key] = input.type === "range" ? Number(input.value) : input.value;
      const out = input.parentElement.querySelector("output"); if (out) out.textContent = input.value;
      refreshAllPreviews();
    };
  });
  $("saveCover").onclick = saveCoverTemplate;
}

async function refreshAllPreviews() {
  await refreshVideoTemplatePreview();
  await refreshMainPreview();
  await refreshGallery();
}

function renderVideoTemplateEditor() {
  const template = templates[selectedVideoTemplate];
  if (!template) return;
  $("videoTemplateCaption").textContent = `${selectedVideoTemplate} / ${template.name || selectedVideoTemplate}`;
  const html = [`<h3>当前正文模板调整</h3>`];
  for (const [key, label, type, min, max] of videoTemplateFields) {
    const value = template[key] ?? "";
    if (type === "checkbox") {
      html.push(`<label class="check-row"><input data-key="${key}" type="checkbox" ${value ? "checked" : ""}><span>${label}</span></label>`);
    } else if (type === "range") {
      html.push(`<label>${label}<input data-key="${key}" type="range" min="${min}" max="${max}" value="${value}"><output>${value}</output></label>`);
    } else if (type === "rangeFloat") {
      html.push(`<label>${label}<input data-key="${key}" type="range" min="${min}" max="${max}" step="0.01" value="${value}"><output>${value}</output></label>`);
    } else {
      html.push(`<label>${label}<input data-key="${key}" type="${type}" value="${escapeHtml(value)}"></label>`);
    }
  }
  html.push(`<button type="button" id="saveVideoTemplate">保存当前正文模板</button>`);
  $("videoTemplateForm").innerHTML = html.join("");
  $("videoTemplateForm").querySelectorAll("[data-key]").forEach((input) => {
    const key = input.dataset.key;
    if (input.type === "checkbox") input.checked = Boolean(template[key]);
    else input.value = template[key] ?? input.value;
    input.oninput = () => updateVideoTemplateField(input);
    input.onchange = () => updateVideoTemplateField(input);
  });
  $("saveVideoTemplate").onclick = saveVideoTemplate;
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
}

async function refreshVideoTemplatePreview() {
  const template = templates[selectedVideoTemplate];
  if (!template) return;
  const data = await api("/api/video-matrix/template-preview", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(videoTemplatePreviewPayload(template))});
  $("videoTemplatePreview").src = data.data_url;
  $("videoTemplateCaption").textContent = `${selectedVideoTemplate} / ${template.name || selectedVideoTemplate}`;
}

function videoTemplatePreviewPayload(template) {
  return {
    template,
    slogan: $("headline").value,
    title: $("subhead").value,
    hud_text: $("hudText").value,
  };
}

async function saveVideoTemplate() {
  await api(`/api/video-matrix/templates/${selectedVideoTemplate}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(templates[selectedVideoTemplate])});
  await saveState();
  log(`已保存正文模板：${templates[selectedVideoTemplate].name || selectedVideoTemplate}`);
  const option = Array.from($("videoTemplate").options).find((item) => item.value === selectedVideoTemplate);
  if (option) option.textContent = templates[selectedVideoTemplate].name || selectedVideoTemplate;
  renderVideoTemplateEditor();
}

async function refreshMainPreview() {
  const data = await api("/api/video-matrix/cover-preview", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(previewPayload(coverTemplates[selectedCover]))});
  $("coverPreview").src = data.data_url;
}

async function refreshGallery() {
  const cards = [];
  for (const [id, t] of Object.entries(coverTemplates)) {
    const data = await api("/api/video-matrix/cover-preview", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(previewPayload(t))});
    cards.push(`<div class="cover-card ${id === selectedCover ? "active" : ""}" data-id="${id}"><img src="${data.data_url}"><span>${id} / ${t.name || id}</span></div>`);
  }
  $("coverGallery").innerHTML = cards.join("");
  $("coverGallery").querySelectorAll(".cover-card").forEach((card) => card.onclick = async () => {
    selectedCover = card.dataset.id; renderCoverSelector(); renderCoverEditor(); await refreshAllPreviews();
  });
}

function previewPayload(template) {
  const payload = {...template};
  if ($("cta").value) payload.cta = $("cta").value;
  return {template: payload, headline: $("headline").value, subhead: $("subhead").value, hud_text: $("hudText").value};
}

async function saveCoverTemplate() {
  await api(`/api/video-matrix/cover-templates/${selectedCover}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(coverTemplates[selectedCover])});
  await saveState();
  log(`已保存第一屏模板：${coverTemplates[selectedCover].name || selectedCover}`);
}

async function saveState() {
  state = collectState();
  await api("/api/video-matrix/state", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(state)});
  log("当前设置已保存");
}

async function generate() {
  const button = $("generateBtn");
  if (lastPreviewPath && button.dataset.mode === "preview") {
    window.open(`/static/video_matrix_preview.html?path=${encodeURIComponent(lastPreviewPath)}`, "_blank", "noopener");
    return;
  }
  lastPreviewPath = "";
  button.dataset.mode = "generate";
  button.disabled = true;
  button.textContent = "提交中...";
  updateJobStatus({ status: "queued", stage: "queued", progress: 0, message: "正在提交生成任务..." });
  try {
    const statePayload = collectState();
    if (!bgmLibraryState.local.length) {
      throw new Error("本地背景音乐库还没有可用 MP3。请把 MP3 文件放入左侧问号提示里的目录，然后刷新页面。");
    }
    const form = new FormData();
    form.append("payload", JSON.stringify({...statePayload, transcript_text: $("transcriptText").value}));
    [...($("sourceFiles")?.files || [])].forEach((file) => form.append("source_files", file));
    const {job_id} = await api("/api/video-matrix/generate", {method:"POST", body: form});
    updateJobStatus({ status: "queued", stage: "queued", progress: 0.02, message: `任务已提交：${job_id}` });
    pollJob(job_id);
  } catch (error) {
    updateJobStatus({ status: "error", stage: "error", progress: 0, message: error.message, error: error.message });
  } finally {
    button.disabled = false;
    if (!lastPreviewPath) button.textContent = "生成 Vibe Matrix";
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

function collectState() {
  const categories = Array.isArray(settings.material_categories) ? settings.material_categories : [];
  return {
    output_count: Number($("outputCount").value), max_workers: Number($("maxWorkers").value),
    output_options: [$("outputOptions").value], output_root: outputRootPath(),
    template_id: selectedVideoTemplate, cover_template_id: selectedCover, copy_language: radioValue("copy_language"),
    source_mode: radioValue("source_mode"), use_live_data: true,
    headline: $("headline").value, subhead: $("subhead").value, cta: $("cta").value,
    follow_text: $("followText").value, hud_text: $("hudText").value,
    bgm_source: "Local library", bgm_library_id: "",
    recent_limits: Object.fromEntries(categories.map((category) => [category.id, Number($(category.id)?.value || settings.recent_limits[category.id] || 8)]))
  };
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
  list.textContent = "正在抓取 Pixabay industry...";
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
  status.textContent = "正在下载到本地曲库...";
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
function syncNumber(id) { const el = $(id); if (!el) return; el.oninput = () => { let value = Number(el.value || 3); value = Math.max(Number(el.min || 1), Math.min(Number(el.max || 100), value)); if (String(value) !== el.value) el.value = value; if (id === "outputCount") $("metricCount").textContent = el.value; }; }
function syncRange(id) { const el = $(id), out = $(`${id}Value`); if (!el || !out) return; out.textContent = el.value; el.oninput = () => { out.textContent = el.value; if (id === "outputCount") $("metricCount").textContent = el.value; if (id === "maxWorkers") $("metricWorkers").textContent = el.value; }; }
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

init().catch((err) => log(err.message));
