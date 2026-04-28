const $ = (id) => document.getElementById(id);
let state = {};
let templates = {};
let coverTemplates = {};
let selectedCover = "";
let selectedVideoTemplate = "";
let settings = {};
let lastPreviewPath = "";

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
  renderCoverSelector();
  renderCoverEditor();
  await refreshAllPreviews();
}

function renderSidebar(data) {
  $("outputCount").value = state.output_count || 3;
  $("maxWorkers").value = state.max_workers || 3;
  syncNumber("outputCount");
  syncRange("maxWorkers");
  $("outputRoot").value = settings.output_root;
  $("outputOptions").value = (state.output_options || ["mp4"])[0] || "mp4";
  $("videoTemplate").innerHTML = Object.entries(templates).map(([id, t]) => `<option value="${id}">${t.name || id}</option>`).join("");
  $("videoTemplate").value = selectedVideoTemplate;
  $("videoTemplate").onchange = () => selectedVideoTemplate = $("videoTemplate").value;
  $("openOutput").onclick = () => openFolder($("outputRoot").value);
  renderRadio("languageGroup", "copy_language", [["zh", "中文"], ["en", "英文"], ["ru", "俄文"]], state.copy_language || "zh");
  renderBgm(data);
  $("saveState").onclick = saveState;
}

function renderSource(data) {
  $("metricSources").textContent = Object.values(data.category_counts).reduce((a, b) => a + b, 0);
  $("metricCount").textContent = $("outputCount").value;
  $("metricWorkers").textContent = $("maxWorkers").value;
  $("sourceDirs").innerHTML = Object.entries(data.source_dirs).map(([key, path]) =>
    `<div class="dir-row"><span class="badge">${key}</span><code>${path}</code><button data-path="${path}">打开</button></div>`).join("");
  $("sourceDirs").querySelectorAll("button").forEach((btn) => btn.onclick = () => openFolder(btn.dataset.path));
  $("sourceCounts").textContent = `当前素材数量：A=${data.category_counts.category_A} / B=${data.category_counts.category_B} / C=${data.category_counts.category_C}`;
  renderRadio("sourceModeGroup", "source_mode", [["Category folders", "分类目录"], ["Upload files", "手动上传"]], state.source_mode || "Category folders", updateSourceMode);
  $("recentLimits").innerHTML = ["category_A", "category_B", "category_C"].map((id, index) =>
    `<label>${"ABC"[index]} 类最新素材<input id="${id}" type="range" min="1" max="50" value="${settings.recent_limits[id] || 8}"><output id="${id}Value"></output></label>`).join("");
  ["category_A", "category_B", "category_C"].forEach(syncRange);
  updateSourceMode();
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
  await refreshMainPreview();
  await refreshGallery();
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
    const bgm = $("bgmUpload")?.files?.[0];
    if (statePayload.bgm_source !== "Local library" && !bgm) {
      throw new Error("请先上传背景音乐，或在背景音乐库中选择本地音乐。");
    }
    if (statePayload.bgm_source === "Local library" && !statePayload.bgm_library_id) {
      throw new Error("本地音乐库没有可用音乐，请先上传背景音乐。");
    }
    if (statePayload.source_mode === "Upload files" && !$("sourceFiles").files.length) {
      throw new Error("手动上传模式下请先选择素材视频。");
    }
    const form = new FormData();
    form.append("payload", JSON.stringify({...statePayload, transcript_text: $("transcriptText").value}));
    if (bgm) form.append("bgm_file", bgm);
    [...($("sourceFiles").files || [])].forEach((file) => form.append("source_files", file));
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
  return {
    output_count: Number($("outputCount").value), max_workers: Number($("maxWorkers").value),
    output_options: [$("outputOptions").value], output_root: $("outputRoot").value,
    template_id: selectedVideoTemplate, cover_template_id: selectedCover, copy_language: radioValue("copy_language"),
    source_mode: radioValue("source_mode"), use_live_data: true,
    headline: $("headline").value, subhead: $("subhead").value, cta: $("cta").value,
    follow_text: $("followText").value, hud_text: $("hudText").value,
    bgm_source: radioValue("bgm_source"), bgm_library_id: $("bgmLibrary")?.value || "",
    recent_limits: {category_A:Number($("category_A").value), category_B:Number($("category_B").value), category_C:Number($("category_C").value)}
  };
}

function renderBgm(data) {
  $("bgmPanel").innerHTML = `
    <div class="radio-row" id="bgmSourceGroup"></div>
    <select id="bgmLibrary"></select>
    <label class="file-picker">
      <input id="bgmUpload" type="file" accept=".mp3,.wav,.m4a">
      <span>选择背景音乐</span>
      <small id="bgmFileName">未选择文件</small>
    </label>
    <div class="links bgm-links"></div>`;
  renderRadio("bgmSourceGroup", "bgm_source", [["Upload file", "上传文件"], ["Local library", "本地库"]], state.bgm_source || "Upload file", updateBgmMode);
  $("bgmLibrary").innerHTML = data.local_bgm.map(name => `<option>${name}</option>`).join("");
  $("bgmLibrary").value = state.bgm_library_id || "";
  document.querySelector("#bgmPanel .links").innerHTML = Object.values(data.bgm_library || {}).map(item => `<a href="${item.download_page}" target="_blank" rel="noopener">${item.name}</a>`).join("");
  $("bgmUpload").onchange = () => {
    $("bgmFileName").textContent = $("bgmUpload").files?.[0]?.name || "未选择文件";
  };
  updateBgmMode();
}

function updateBgmMode() {
  const local = radioValue("bgm_source") === "Local library";
  $("bgmLibrary").classList.toggle("hidden", !local);
  $("bgmUpload").classList.toggle("hidden", local);
}
function updateSourceMode() { $("uploadSourcesWrap").classList.toggle("hidden", radioValue("source_mode") !== "Upload files"); }
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
  $("jobStatusTitle").textContent = job.status === "error" ? "生成失败" : job.status === "complete" ? "生成完成" : `正在${jobMessages[stage] ? jobMessages[stage].replace(/^正在/, "").replace(/。$/, "") : "处理"}`;
  $("jobPercent").textContent = `${percent}%`;
  $("jobProgressFill").style.width = `${percent}%`;
  $("jobMessage").textContent = job.error || job.message || jobMessages[stage] || "正在处理，请稍等。";
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
