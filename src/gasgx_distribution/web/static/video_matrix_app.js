const $ = (id) => document.getElementById(id);
let state = {};
let templates = {};
let coverTemplates = {};
let selectedCover = "";
let selectedVideoTemplate = "";
let settings = {};
let lastPreviewPath = "";
let bgmLibraryState = { local: [], directory: "", links: [] };
let endingTemplateState = { local: [], directory: "" };
let endingPreviewOverrideName = "";
let pendingTemplateSave = "";
let coverEditingContext = "cover";
let modelImages = [];
let selectedModelImageUrl = "";
let sourcePreviewVideos = [];
let endingModeLoading = "";
let displayedJobPercent = 0;
let jobProgressTimer = null;
let lastJobSnapshot = null;
let visualDropdownCloseBound = false;
let videoTemplateThumbScaleBound = false;
const AI_PROMPT_HINT_MAX_CHARS = 240;
const AI_PROMPT_HINT_MAX_LINES = 4;
const AI_PROMPT_HINT_URL_RE = /(https?:\/\/\S+|www\.\S+)/gi;
const HUD_TEXT_MAX_LINES = 2;
const HUD_TEXT_MAX_CHARS_PER_LINE = 10;

const narrativeTemplateNameMap = {
  quick_showcase: "蹇垏灞曠ず",
  faq_explainer: "FAQ 鏁欑▼",
  contrast_compare: "鍙嶅樊瀵规瘮",
  step_list: "娓呭崟姝ラ",
  voiceover_broll: "鍙ｆ挱鎸傜敾闈?,
};

const narrativeAccountPoolNameMap = {
  result_showcase: "鎴愭灉灞曠ず",
  tutorial_faq: "鏁欑▼绛旂枒",
  contrast: "鍙嶅樊瀵规瘮",
  howto_steps: "姝ラ娓呭崟",
  onsite_voice: "鐜板満鍙ｆ挱",
};

const jobStepLabels = [
  ["queued", "浠诲姟鎻愪氦", 0, ["queued"]],
  ["ingest_scan", "鎵弿绱犳潗", 5, ["ingestion"]],
  ["ingestion", "鏁寸悊绱犳潗", 12, ["ingestion"]],
  ["hud", "鍑嗗鏁版嵁瀛楀箷", 20, ["hud"]],
  ["beat", "鍒嗘瀽闊充箰鑺傚", 30, ["beat"]],
  ["planning", "瑙勫垝鍘婚噸鏂规", 42, ["planning"]],
  ["render_start", "鍚姩娓叉煋", 45, ["render"]],
  ["render", "鐢熸垚瑙嗛", 56, ["render"]],
  ["render_wait", "绛夊緟瀵煎嚭", 82, ["render"]],
  ["finalizing", "鏁寸悊瀵煎嚭鏂囦欢", 97, ["finalizing"]],
  ["complete", "瀹屾垚", 100, ["complete"]],
];

const jobMessages = {
  queued: "浠诲姟宸叉彁浜わ紝姝ｅ湪绛夊緟寮€濮嬨€?,
  ingestion: "姝ｅ湪璇诲彇骞舵暣鐞嗙礌鏉愯棰戯紝璇风‘璁ょ礌鏉愮洰褰曢噷鏈夎棰戞枃浠躲€?,
  hud: "姝ｅ湪鍑嗗瑙嗛閲岀殑瀛楀箷鑳屾澘鏁版嵁鍜屾枃瀛椾俊鎭€?,
  beat: "姝ｅ湪鍒嗘瀽鑳屾櫙闊充箰鑺傚锛岀敤浜庡崱鐐规贩鍓€?,
  planning: "姝ｅ湪瑙勫垝姣忔潯瑙嗛鐨勫彊浜嬮鏋躲€佺礌鏉愮粍鍚堝拰浣庢垚鏈幓閲嶃€?,
  render: "姝ｅ湪璋冪敤 FFmpeg 鐢熸垚瑙嗛锛岃繖涓€姝ヨ€楁椂鏈€闀裤€?,
  finalizing: "姝ｅ湪鏁寸悊瀵煎嚭鐨?MP4銆佸皝闈㈠拰鏂囨鏂囦欢銆?,
  complete: "鐢熸垚瀹屾垚锛屽彲浠ュ埌杈撳嚭鐩綍鏌ョ湅鏂囦欢銆?,
  error: "鐢熸垚澶辫触锛岃鎸変笅鏂规彁绀哄鐞嗗悗閲嶈瘯銆?,
};

const backendJobMessageMap = {
  "Queued": "浠诲姟宸叉彁浜わ紝姝ｅ湪鎺掗槦鍑嗗銆傝淇濇寔褰撳墠椤甸潰鎵撳紑锛岀郴缁熶細鑷姩寮€濮嬪鐞嗐€?,
  "Collecting and normalizing source clips": "姝ｅ湪鎵弿骞舵暣鐞嗙礌鏉愯棰戯紝鎸夊垎绫荤洰褰曡鍙栧彲鐢ㄧ墖娈点€?,
  "Preparing GasGx data HUD": "姝ｅ湪鍑嗗瑙嗛閲岀殑瀛楀箷鑳屾澘鏁版嵁銆佹爣棰樺拰瀛楀箷瀛楁銆?,
  "Analyzing BGM beat grid": "姝ｅ湪鍒嗘瀽鑳屾櫙闊充箰鑺傛媿缃戞牸锛岀敤浜庡悗缁崱鐐规贩鍓€?,
  "Planning de-duplicated video variants": "姝ｅ湪瑙勫垝姣忔潯瑙嗛鐨勫彊浜嬮鏋跺拰鍘婚噸绛栫暐锛屽懡涓噸澶嶄細鑷姩閲嶅壀銆?,
  "Finalizing preview assets and manifests": "姝ｅ湪鏁寸悊瀵煎嚭鐨?MP4銆侀瑙堟枃浠跺拰娓呭崟銆?,
};

const coverFields = [
  ["name", "妯℃澘鍚嶇О", "text"], ["brand", "鍝佺墝鏂囧瓧", "text"], ["eyebrow", "鐪夋爣鏂囧瓧", "text"], ["cta", "CTA 鎸夐挳鏂囧瓧", "text"],
  ["align", "鏂囧瓧瀵归綈", "select"], ["brand_y", "鍝佺墝 Y", "range", 0, 420], ["headline_y", "涓绘爣棰?Y", "range", 0, 1320],
  ["subhead_y", "鍓爣棰?Y", "range", 0, 1500], ["hud_y", "瀛楀箷鑳屾澘 Y", "range", 0, 1780], ["cta_y", "CTA Y", "range", 0, 1840],
  ["primary_color", "涓绘枃瀛楄壊", "color"], ["secondary_color", "鍓枃瀛楄壊", "color"], ["accent_color", "寮鸿皟鑹?, "color"],
  ["tint_color", "鑳屾櫙缃╄壊", "color"], ["gradient_color", "娓愬彉鑹?, "color"], ["panel_color", "瀛楀箷鑳屾澘闈㈡澘鑹?, "color"],
  ["tint_opacity", "鑳屾櫙缃╅€忔槑搴?, "rangeFloat", 0, 1], ["gradient_opacity", "娓愬彉閫忔槑搴?, "rangeFloat", 0, 1],
  ["panel_opacity", "瀛楀箷鑳屾澘闈㈡澘閫忔槑搴?, "rangeFloat", 0, 1],
];
const videoTemplateFields = [
  ["name", "妯℃澘鍚嶇О", "text"],
  ["show_slogan", "鏄剧ず涓婃爣棰?, "checkbox"],
  ["show_title", "鏄剧ず涓爣棰?, "checkbox"],
  ["show_hud", "鏄剧ず涓嬫爣棰?, "checkbox"],
];
const visualFontOptions = [
  ["'Microsoft YaHei', 'Noto Sans SC', sans-serif", "闆呴粦榛戜綋"],
  ["'Microsoft JhengHei', 'Microsoft YaHei', sans-serif", "骞垮憡榛戜綋"],
  ["'Arial Black', Impact, sans-serif", "閲嶇鏍囬"],
  ["Impact, 'Arial Black', sans-serif", "鍐插嚮娴锋姤"],
  ["'Bahnschrift Condensed', 'Arial Narrow', sans-serif", "绐勪綋宸ヤ笟"],
  ["'Trebuchet MS', 'Microsoft YaHei', sans-serif", "鐜颁唬鍦嗕綋"],
  ["'Segoe UI Black', 'Arial Black', sans-serif", "绉戞妧绮椾綋"],
  ["'Franklin Gothic Heavy', 'Arial Black', sans-serif", "鍟嗕笟绮椾綋"],
  ["Georgia, 'Times New Roman', serif", "楂樼骇琛嚎"],
  ["'Courier New', Consolas, monospace", "鏁版嵁绛夊"],
];
const videoTextFontOptions = [
  ["'Microsoft YaHei Bold', 'Microsoft YaHei', 'Noto Sans SC', sans-serif", "涓枃涓绘爣棰?],
  ["'Noto Sans SC Bold', 'Noto Sans SC', 'Microsoft YaHei', sans-serif", "涓枃澶у睆绮椾綋"],
  ["SimHei, 'Microsoft YaHei', sans-serif", "涓枃榛戜綋鍐插嚮"],
  ["DINNextLTPro-Bold, 'Segoe UI Black', Impact, 'Microsoft YaHei', sans-serif", "鑻辨枃涓绘爣棰?DIN"],
  ["'Segoe UI Black', Impact, 'Microsoft YaHei', sans-serif", "鑻辨枃涓绘爣棰?Black"],
  ["Impact, 'Arial Black', 'Microsoft YaHei', sans-serif", "鑻辨枃涓绘爣棰?Impact"],
  ["'Arial Black', Impact, 'Microsoft YaHei', sans-serif", "涓嫳娣锋帓绮楅粦"],
  ["Bahnschrift, 'Bahnschrift Condensed', 'Microsoft YaHei', sans-serif", "宸ヤ笟绉戞妧椋?],
  ["Consolas, 'Courier New', 'Microsoft YaHei', monospace", "鏁版嵁鏈虹敳椋?],
  ["'Alibaba PuHuiTi Heavy', 'Microsoft YaHei', 'Noto Sans SC', sans-serif", "闃块噷鏅儬閲嶉粦"],
  ["'Source Han Sans Heavy', 'Noto Sans SC', 'Microsoft YaHei', sans-serif", "鎬濇簮閲嶉粦"],
  ["'HarmonyOS Sans SC Bold', 'Noto Sans SC', 'Microsoft YaHei', sans-serif", "楦胯挋绮楅粦"],
  ["YouSheBiaoTiHei, SimHei, 'Microsoft YaHei', sans-serif", "浼樿鏍囬榛?],
  ["'DIN Condensed', DINNextLTPro-Bold, Bahnschrift, 'Microsoft YaHei', sans-serif", "DIN 鍘嬬缉骞垮憡"],
  ["'Franklin Gothic Heavy', 'Arial Black', 'Microsoft YaHei', sans-serif", "纭牳骞垮憡"],
  ["'Cooper Black', Georgia, 'Microsoft YaHei', serif", "澶嶅彜鑳栧瓧"],
  ["'Showcard Gothic', 'Arial Black', 'Microsoft YaHei', sans-serif", "鎷涚墝婕敾"],
  ["SimSun, 'Microsoft YaHei', serif", "涓枃瀹嬩綋鍒婂ご"],
  ["'Microsoft JhengHei', 'Microsoft YaHei', sans-serif", "涓枃骞垮憡榛戜綋"],
  ["'Trebuchet MS', 'Microsoft YaHei', sans-serif", "鑻辨枃鍦嗕綋绉戞妧"],
  ["Georgia, 'Times New Roman', 'Microsoft YaHei', serif", "English Serif Luxe"],
  ["'Lucida Console', 'Courier New', 'Microsoft YaHei', monospace", "English Data Mono"],
  ["'Comic Sans MS', 'Arial Black', 'Microsoft YaHei', sans-serif", "English Pop Comic"],
];
const fontPreviewEnglish = "GasGx";
const fontPreviewChinese = "鐩栨柉鍩哄厠鏂?;
function fontSamplePreviewHtml(label) {
  const normalized = String(label || "");
  const chineseOnly = /涓枃|闃块噷|鎬濇簮|楦胯挋|浼樿/.test(normalized);
  const englishOnly = /鑻辨枃|English|DIN|纭牳|澶嶅彜|鎷涚墝/.test(normalized);
  if (chineseOnly && !/涓嫳/.test(normalized)) {
    return `<span class="font-sample-cn">${escapeHtml(fontPreviewChinese)}</span>`;
  }
  if (englishOnly) {
    return `<span class="font-sample-en">${escapeHtml(fontPreviewEnglish)}</span>`;
  }
  return `<span class="font-sample-en">${escapeHtml(fontPreviewEnglish)}</span><span class="font-sample-cn">${escapeHtml(fontPreviewChinese)}</span>`;
}
function visualOptionLabel(options, value) {
  const found = options.find(([optionValue]) => optionValue === value);
  return found ? found[1] : (options[0]?.[1] || "");
}
function visualDropdownOptionsHtml(options, selectedValue, command) {
  return options.map(([value, label]) => `
          <button type="button" class="visual-dropdown-option ${value === selectedValue ? "active" : ""}" data-visual-command="${escapeHtml(command)}" data-value="${escapeHtml(value)}">${escapeHtml(label)}</button>
  `).join("");
}
const textEffectOptions = [
  ["none", "鏃犲姩鏁?],
  ["fade-in", "娣″叆"],
  ["fade-out", "娣″嚭"],
  ["fade-in-out", "娣″叆鍚庢贰鍑?],
  ["pulse", "鍛煎惛鏀惧ぇ"],
  ["glow", "闇撹櫣闂厜"],
  ["slide-up", "涓婃诞鍏ュ満"],
  ["slide-down", "涓嬫粦鍏ュ満"],
  ["slide-left", "宸︽粦鍏ュ満"],
  ["slide-right", "鍙虫粦鍏ュ満"],
  ["shake", "杞诲井闇囧姩"],
  ["typewriter", "鎵撳瓧鏈?],
  ["pop", "寮硅烦寮鸿皟"],
  ["blink", "闂儊"],
  ["wave", "娉㈡氮鎽嗗姩"],
  ["jitter", "楂橀杞诲井鎶栧姩"],
  ["zoom-in", "鏀惧ぇ鍏ュ満"],
  ["shadow-pop", "闃村奖鍐插嚮"],
];
const textStyleOptions = [
  ["none", "鍩虹鏂囧瓧"],
  ["soft-shadow", "鏌斿拰闃村奖"],
  ["hard-shadow", "纭槾褰?],
  ["outline", "榛戣壊鎻忚竟"],
  ["white-outline", "鐧借壊鎻忚竟"],
  ["glow", "澶栧彂鍏?],
  ["neon", "闇撹櫣瀛?],
  ["gradient", "鍙岃壊娓愬彉"],
  ["reflection", "鏂囧瓧鍊掑奖"],
];
const coverMaskModeOptions = [
  ["none", "鏃犺挋鐗?],
  ["top_gradient", "涓婃笎鍙樿挋鐗?],
  ["bottom_gradient", "涓嬫笎鍙樿挋鐗?],
  ["dual_gradient", "涓婁笅娓愬彉钂欑増"],
  ["full", "鍏ㄨ挋鐗?],
];
const endingTemplateModeOptions = [
  ["dynamic", "鏂囧瓧鐗囧熬"],
  ["random", "瑙嗛鐗囧熬"],
];
const PREVIEW_FRAME_PLACEHOLDER = "data:text/html;charset=utf-8,%3C!doctype%20html%3E%3Chtml%3E%3Chead%3E%3Cstyle%3Ehtml%2Cbody%7Bmargin%3A0%3Bwidth%3A100%25%3Bheight%3A100%25%3Boverflow%3Ahidden%3Bbackground%3A%23050505%3Bcolor-scheme%3Adark%3B%7D%3C%2Fstyle%3E%3C%2Fhead%3E%3Cbody%3E%3C%2Fbody%3E%3C%2Fhtml%3E";
const VIDEO_TEMPLATE_DEFAULT_TEXT_WIDTH = 760;

async function api(path, options = {}) {
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function sanitizeLimitedPrompt(value, maxChars = AI_PROMPT_HINT_MAX_CHARS, maxLines = AI_PROMPT_HINT_MAX_LINES) {
  const text = String(value || "").replace(/\r\n/g, "\n").replace(AI_PROMPT_HINT_URL_RE, "").trim();
  if (!text) return "";
  const lines = text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, maxLines);
  const merged = lines.join("\n").slice(0, maxChars);
  return merged.trim();
}

function sanitizeAiPromptHint(value) {
  return sanitizeLimitedPrompt(value, AI_PROMPT_HINT_MAX_CHARS, AI_PROMPT_HINT_MAX_LINES);
}

function aiPromptHintLineCount(value) {
  const text = String(value || "").trim();
  if (!text) return 0;
  return text.split("\n").length;
}

function truncateNonSpaceChars(value, maxChars) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text || maxChars <= 0) return "";
  let count = 0;
  let out = "";
  for (const char of text) {
    if (/\s/.test(char)) {
      if (out && !out.endsWith(" ")) out += " ";
      continue;
    }
    if (count >= maxChars) break;
    out += char;
    count += 1;
  }
  return out.trim();
}

function sanitizeHudText(value) {
  const lines = String(value || "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => truncateNonSpaceChars(line, HUD_TEXT_MAX_CHARS_PER_LINE))
    .filter(Boolean)
    .slice(0, HUD_TEXT_MAX_LINES);
  return lines.join("\n");
}

function syncFollowTextFixedField(value) {
  const field = $("followTextFixed");
  if (!field) return;
  const nextValue = String(value || "");
  if (field.value !== nextValue) field.value = nextValue;
}

function syncHudTextFixedField(value) {
  const field = $("hudTextFixed");
  if (!field) return;
  const nextValue = sanitizeHudText(value || "");
  if (field.value !== nextValue) field.value = nextValue;
}

function replaceSelection(value, start, end, insertText) {
  const safeValue = String(value || "");
  const from = Math.max(0, Number(start || 0));
  const to = Math.max(from, Number(end || from));
  return `${safeValue.slice(0, from)}${insertText}${safeValue.slice(to)}`;
}

function syncAiPromptHintMeta(value) {
  const meta = $("aiPromptHintMeta");
  if (!meta) return;
  const lineCount = aiPromptHintLineCount(value);
  meta.textContent = `限制：最多 ${AI_PROMPT_HINT_MAX_LINES} 行、最多 ${AI_PROMPT_HINT_MAX_CHARS} 字符，链接会被自动移除。已用 ${value.length}/${AI_PROMPT_HINT_MAX_CHARS} 字符，${lineCount}/${AI_PROMPT_HINT_MAX_LINES} 行。`;
}

function syncPromptHintMeta(metaId, value) {
  const meta = $(metaId);
  if (!meta) return;
  const lineCount = aiPromptHintLineCount(value);
  meta.textContent = `限制：最多 ${AI_PROMPT_HINT_MAX_LINES} 行、最多 ${AI_PROMPT_HINT_MAX_CHARS} 字符，链接会被自动移除。已用 ${value.length}/${AI_PROMPT_HINT_MAX_CHARS} 字符，${lineCount}/${AI_PROMPT_HINT_MAX_LINES} 行。`;
}

function bindPromptHintField(inputId, stateKey, metaId) {
  const input = $(inputId);
  if (!input) return;
  const normalized = sanitizeAiPromptHint(state[stateKey] || "");
  state[stateKey] = normalized;
  input.value = normalized;
  if (metaId) syncPromptHintMeta(metaId, normalized);
  input.onkeydown = (event) => {
    if (event.key !== "Enter" || event.isComposing) return;
    const start = input.selectionStart ?? input.value.length;
    const end = input.selectionEnd ?? start;
    const nextRaw = replaceSelection(input.value, start, end, "\n");
    const nextValue = sanitizeAiPromptHint(nextRaw);
    if (aiPromptHintLineCount(nextValue) > AI_PROMPT_HINT_MAX_LINES || nextValue === input.value) {
      event.preventDefault();
    }
  };
  input.onpaste = (event) => {
    const pasted = String(event.clipboardData?.getData("text") || "");
    if (!pasted) return;
    const start = input.selectionStart ?? input.value.length;
    const end = input.selectionEnd ?? start;
    const nextRaw = replaceSelection(input.value, start, end, pasted.replace(/\r\n/g, "\n"));
    const nextValue = sanitizeAiPromptHint(nextRaw);
    event.preventDefault();
    input.value = nextValue;
    state[stateKey] = nextValue;
    if (metaId) syncPromptHintMeta(metaId, nextValue);
    scheduleStateSave();
  };
  input.oninput = () => {
    const nextValue = sanitizeAiPromptHint(input.value);
    if (nextValue !== input.value) input.value = nextValue;
    state[stateKey] = nextValue;
    if (metaId) syncPromptHintMeta(metaId, nextValue);
    scheduleStateSave();
  };
}
function bindMobileSidebarToggle() {
  const toggle = $("mobileSidebarToggle");
  if (!toggle) return;
  const media = window.matchMedia("(max-width: 920px)");
  const sync = () => {
    if (!media.matches) {
      document.body.classList.remove("mobile-sidebar-open");
      toggle.setAttribute("aria-expanded", "false");
      toggle.textContent = "灞曞紑鍙傛暟";
      return;
    }
    const open = document.body.classList.contains("mobile-sidebar-open");
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    toggle.textContent = open ? "鏀惰捣鍙傛暟" : "灞曞紑鍙傛暟";
  };
  toggle.onclick = (event) => {
    event.stopPropagation();
    const next = !document.body.classList.contains("mobile-sidebar-open");
    document.body.classList.toggle("mobile-sidebar-open", next);
    sync();
  };
  document.addEventListener("click", (event) => {
    if (!media.matches || !document.body.classList.contains("mobile-sidebar-open")) return;
    const sidebar = document.querySelector(".sidebar");
    if (sidebar && !sidebar.contains(event.target)) {
      document.body.classList.remove("mobile-sidebar-open");
      sync();
    }
  });
  if (typeof media.addEventListener === "function") {
    media.addEventListener("change", sync);
  } else if (typeof media.addListener === "function") {
    media.addListener(sync);
  }
  sync();
}

function loadingInline(label = "鍔犺浇涓?..") {
  return `<div class="loading-inline"><span class="loading-spinner" aria-hidden="true"></span><span>${label}</span></div>`;
}

function buttonLoadingInline(label) {
  return `<span class="loading-spinner" aria-hidden="true"></span><span>${escapeHtml(label)}</span>`;
}
function buttonIconLabel(icon, label) {
  return `<span class="button-icon" aria-hidden="true">${escapeHtml(icon)}</span><span>${escapeHtml(label)}</span>`;
}

function setPanelLoading(id, label = "鍔犺浇涓?..") {
  const node = $(id);
  if (node) node.innerHTML = loadingInline(label);
}

function setImageLoading(id, label = "鐢熸垚棰勮涓?..") {
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

function pulseImageLoading(id, label = "搴旂敤涓?..") {
  setImageLoading(id, label);
  window.clearTimeout(pulseImageLoading.timers?.[id]);
  pulseImageLoading.timers = pulseImageLoading.timers || {};
  pulseImageLoading.timers[id] = window.setTimeout(() => clearImageLoading(id), 650);
}

function encodePreviewPayload(payload) {
  return btoa(unescape(encodeURIComponent(JSON.stringify(payload))));
}

function previewFrameUrlForPayload(payload = null) {
  return payload
    ? `/static/video_matrix_preview.html?template=${encodeURIComponent(encodePreviewPayload(payload))}`
    : "/static/video_matrix_preview.html";
}

function refreshPhonePreviewFrame(id, payload = null) {
  const frame = $(id);
  if (!frame) return;
  const url = previewFrameUrlForPayload(payload);
  if (frame.getAttribute("src") === url) return;
  frame.src = PREVIEW_FRAME_PLACEHOLDER;
  window.requestAnimationFrame(() => {
    frame.src = url;
  });
}

function setInitialLoading() {
  ["sourceDirs", "recentLimits", "compositionRows", "videoTemplateForm", "coverForm", "endingTemplateForm", "bgmPanel"].forEach((id) => setPanelLoading(id));
  setPanelLoading("videoTemplateGallery", "鍔犺浇姝ｆ枃妯℃澘...");
  setImageLoading("videoTemplatePreview", "鍔犺浇姝ｆ枃棰勮...");
  setImageLoading("coverPreview", "鍔犺浇灏侀潰棰勮...");
  setImageLoading("endingTemplatePreview", "鍔犺浇鐗囧熬棰勮...");
}

async function init() {
  bindMobileSidebarToggle();
  setInitialLoading();
  const data = await api("/api/video-matrix/state");
  state = data.ui_state; templates = data.templates; coverTemplates = data.cover_templates; settings = data.settings;
  sourcePreviewVideos = Array.isArray(data.source_videos) ? data.source_videos : [];
  selectedCover = state.cover_template_id || Object.keys(coverTemplates)[0];
  selectedVideoTemplate = state.template_id || Object.keys(templates)[0];
  const coverFallback = Object.keys(coverTemplates)[0] || "";
  const templateFallback = Object.keys(templates)[0] || "";
  let normalizedSelection = false;
  if (!coverTemplates[selectedCover] && coverFallback) {
    selectedCover = coverFallback;
    state.cover_template_id = coverFallback;
    normalizedSelection = true;
  }
  if (!templates[selectedVideoTemplate] && templateFallback) {
    selectedVideoTemplate = templateFallback;
    state.template_id = templateFallback;
    normalizedSelection = true;
  }
  renderSidebar(data);
  renderSource(data);
  renderComposition(data);
  renderNarrativeTemplates(data);
  renderTextSettings();
  renderVideoTemplateEditor();
  renderCoverSelector();
  renderCoverEditor();
  renderEndingTemplatePanel(data);
  await loadModelImages();
  await refreshAllPreviews();
  if (normalizedSelection) {
    scheduleStateSave();
    log("宸茶嚜鍔ㄥ洖閫€鍒板彲鐢ㄦā鏉匡紝鏃фā鏉?ID 涓嶅瓨鍦ㄣ€?);
  }
}

function renderSidebar(data) {
  $("outputCount").value = state.output_count || 3;
  $("maxWorkers").value = state.max_workers || 3;
  $("maxWorkersValue").textContent = `${$("maxWorkers").value}`;
  $("videoDurationMin").value = state.video_duration_min || settings.video_duration_min || 9;
  $("videoDurationMax").value = state.video_duration_max || settings.video_duration_max || 15;
  $("miningBgmVolume").value = clamp(Number(state.mining_bgm_volume ?? 1), 0, 2).toFixed(2);
  $("libraryBgmVolume").value = clamp(Number(state.library_bgm_volume ?? 0.35), 0, 2).toFixed(2);
  $("miningBgmVolumeValue").textContent = Number($("miningBgmVolume").value).toFixed(2);
  $("libraryBgmVolumeValue").textContent = Number($("libraryBgmVolume").value).toFixed(2);
  syncNumber("outputCount");
  syncNumber("videoDurationMin");
  syncNumber("videoDurationMax");
  syncRange("maxWorkers");
  syncRange("miningBgmVolume");
  syncRange("libraryBgmVolume");
  renderSidebarTemplateSelectors();
  const outputRoot = state.output_root || settings.output_root;
  $("outputRoot").dataset.fullPath = outputRoot;
  $("outputRoot").title = outputRoot;
  $("outputRoot").value = shortPath(outputRoot);
  $("outputOptions").value = (state.output_options || ["mp4"])[0] || "mp4";
  $("outputOptions").onchange = scheduleStateSave;
  $("openOutput").onclick = () => openFolder(outputRootPath());
  renderRadio("targetFpsGroup", "target_fps", [["30", "30 fps"], ["60", "60 fps"]], String(state.target_fps || settings.target_fps || 30), scheduleStateSave);
  renderRadio("renderSpeedModeGroup", "render_speed_mode", [["fast_first", "蹇€熼鍑?], ["quality", "鏍囧噯璐ㄩ噺"]], String(state.render_speed_mode || "quality"), scheduleStateSave);
  const headlineAiToggle = $("headlineAiEnabled");
  if (headlineAiToggle) {
    headlineAiToggle.checked = Boolean(state.headline_ai_enabled);
    headlineAiToggle.onchange = () => {
      state.headline_ai_enabled = headlineAiToggle.checked;
      scheduleStateSave();
    };
  }
  bindPromptHintField("aiPromptHint", "ai_prompt_hint", "aiPromptHintMeta");
  const descriptionAiToggle = $("descriptionAiEnabled");
  if (descriptionAiToggle) {
    descriptionAiToggle.checked = Boolean(state.description_ai_enabled);
    descriptionAiToggle.onchange = () => {
      state.description_ai_enabled = descriptionAiToggle.checked;
      scheduleStateSave();
    };
  }
  const descriptionText = $("descriptionText");
  if (descriptionText) {
    descriptionText.value = state.description_text || "";
    descriptionText.oninput = () => {
      state.description_text = descriptionText.value;
      scheduleStateSave();
    };
  }
  bindPromptHintField("descriptionAiPromptHint", "description_ai_prompt_hint", "descriptionAiPromptHintMeta");
  const followTextAiToggle = $("followTextAiEnabled");
  if (followTextAiToggle) {
    followTextAiToggle.checked = Boolean(state.follow_text_ai_enabled);
    followTextAiToggle.onchange = () => {
      state.follow_text_ai_enabled = followTextAiToggle.checked;
      scheduleStateSave();
    };
  }
  const followTextFixed = $("followTextFixed");
  if (followTextFixed) {
    const fixedFollow = state.follow_text || "";
    followTextFixed.value = fixedFollow;
    const hiddenFollow = $("followText");
    if (hiddenFollow) hiddenFollow.value = fixedFollow;
    followTextFixed.oninput = () => {
      const value = followTextFixed.value;
      state.follow_text = value;
      if (hiddenFollow) hiddenFollow.value = value;
      scheduleStateSave();
      refreshEndingTemplatePreview();
    };
  }
  bindPromptHintField("followTextAiPromptHint", "follow_text_ai_prompt_hint", "followTextAiPromptHintMeta");
  const hudAiToggle = $("hudAiEnabled");
  if (hudAiToggle) {
    hudAiToggle.checked = Boolean(state.hud_ai_enabled);
    hudAiToggle.onchange = () => {
      state.hud_ai_enabled = hudAiToggle.checked;
      scheduleStateSave();
    };
  }
  const hudTextFixed = $("hudTextFixed");
  if (hudTextFixed) {
    const normalizedHud = sanitizeHudText(state.hud_text || "");
    state.hud_text = normalizedHud;
    hudTextFixed.value = normalizedHud;
    const hiddenHud = $("hudText");
    if (hiddenHud) hiddenHud.value = normalizedHud;
    hudTextFixed.oninput = () => {
      const value = sanitizeHudText(hudTextFixed.value);
      if (value !== hudTextFixed.value) hudTextFixed.value = value;
      state.hud_text = value;
      if (hiddenHud) hiddenHud.value = value;
      scheduleStateSave();
      debounce(refreshAllPreviews, 250)();
    };
  }
  bindPromptHintField("hudAiPromptHint", "hud_ai_prompt_hint", "hudAiPromptHintMeta");
  renderBgm(data);
  $("saveState").onclick = toggleBgmLibraryPopover;
  $("openBgmDir").onclick = () => openFolder(bgmLibraryState.directory);
  $("openPreviewVideo").onclick = openPreviewVideoPage;
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
    const limit = clamp(Number(recentLimits[category.id] || settings.recent_limits?.[category.id] || 8), 1, 10);
    const checked = activeCategoryIds.includes(category.id);
    const totalCount = Number(data.category_counts?.[category.id] || 0);
    return `<div class="dir-row source-composition-row composition-row" data-index="${index}" data-source-category="${escapeHtml(category.id)}">
      <label class="category-toggle">
        <input type="checkbox" data-category-id="${escapeHtml(category.id)}" ${checked ? "checked" : ""}>
        <span class="badge" title="${escapeHtml(data.source_dirs[category.id] || "")}">${escapeHtml(category.id)}</span>
      </label>
      <input type="hidden" data-composition-category value="${escapeHtml(category.id)}">
      <input data-category-label value="${escapeHtml(category.label || category.id)}" aria-label="${escapeHtml(category.id)}鐩綍鍚嶇О" ${checked ? "" : "disabled"}>
      <label class="composition-unit-field composition-material-count">
        <span>閲囩敤鏈€鏂板墠</span>
        <input data-composition-limit list="recentLimitOptions" type="text" inputmode="numeric" pattern="[1-9]|10" value="${limit}" placeholder="1-10" title="鏈€鏂扮礌鏉愭暟閲? aria-label="${escapeHtml(category.label)}鏈€鏂扮礌鏉愭暟閲? ${checked ? "" : "disabled"}>
        <span>鏉?/span>
      </label>
      <span class="source-total-count">绱犳潗鎬绘暟锛?b>${totalCount}</b></span>
      <button type="button" data-source-open data-path="${escapeHtml(data.source_dirs[category.id] || "")}">鎵撳紑鐩綍</button>
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
  $("sourceCounts").textContent = "绠楁硶锛氭寜瑙嗛纰庣墖鍒嗙被鐩綍璇诲彇绱犳潗锛涙瘡娆℃寜鐓х洰褰曟妸鏈€鏂版媿鎽勭殑鐭棰戜笂浼犺繘瀵瑰簲鐨勭洰褰曪紱鍕鹃€夌礌鏉愮洰褰曞苟璁剧疆鏈€鏂扮礌鏉愭暟閲忓悗锛岀郴缁熶細鑷姩璁＄畻鐗囨鏃堕暱骞舵寜琛岄『搴忕粍鍚堟贩鍓€?;
  renderRadio("sourceModeGroup", "source_mode", [["Category folders", "鏅鸿兘鍒嗙被杞崲绠楁硶"]], "Category folders", () => {
    updateSourceMode();
    scheduleStateSave();
  });
  updateRecentLimitVisibility(categories);
  updateSourceMode();
  renderNarrativeTemplates(data);
}

function renderNarrativeTemplates(data = { settings }) {
  const containers = [...document.querySelectorAll("[data-narrative-templates]")];
  if (!containers.length) return;
  const templates = narrativeTemplates();
  const categories = materialCategories(data);
  const categoryNames = Object.fromEntries(categories.map((category) => [category.id, category.label || category.id]));
  let html = "";
  if (!templates.length) {
    html = `<div class="narrative-empty">褰撳墠鏈厤缃彊浜嬮鏋讹紝灏嗘部鐢ㄧ敓鎴愮粨鏋勯『搴忋€?/div>`;
    containers.forEach((container) => { container.innerHTML = html; });
    return;
  }
  html = templates.map((template, index) => {
    const sequence = narrativeTemplateSequence(template);
    const steps = sequence.length
      ? sequence.map((row) => {
          const categoryId = String(row.category_id || "").trim();
          const name = categoryNames[categoryId] || categoryId;
          return `<span title="${escapeHtml(categoryId)}">${escapeHtml(name)}<b>${Number(row.duration || 0).toFixed(1)}s</b></span>`;
        }).join("")
      : `<span>娌跨敤鐢熸垚缁撴瀯</span>`;
    return `<article class="narrative-template-card" data-narrative-template="${escapeHtml(template.id)}">
      <div class="narrative-template-head">
        <strong>${index + 1}. ${escapeHtml(narrativeTemplateDisplayName(template))}</strong>
        <code title="${escapeHtml(template.id)}">妯℃澘 ${index + 1}</code>
      </div>
      <div class="narrative-template-path">${steps}</div>
      <small>璐﹀彿姹狅細${escapeHtml(narrativeAccountPoolDisplayName(template.account_pool_id || template.id))}</small>
    </article>`;
  }).join("");
  containers.forEach((container) => { container.innerHTML = html; });
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
    log("璇峰厛杈撳叆鐩綍鍚嶇О銆?);
    return;
  }
  await api("/api/video-matrix/material-categories", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({label})});
  input.value = "";
  const data = await api("/api/video-matrix/state");
  state = data.ui_state; settings = data.settings;
  renderSource(data);
  renderComposition(data);
  log(`宸叉坊鍔犵礌鏉愮洰褰曪細${label}`);
}

async function renameMaterialCategory(button, data) {
  const row = button.closest(".composition-row");
  const categoryId = button.dataset.categoryId || row?.dataset.sourceCategory || "";
  const input = row?.querySelector("[data-category-label]");
  const label = input?.value.trim() || "";
  if (!categoryId || !label) {
    log("璇峰厛杈撳叆鐩綍鍚嶇О銆?);
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
    log(`宸蹭繚瀛樼礌鏉愮洰褰曞悕绉帮細${label}`);
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
        <label class="composition-unit-field composition-material-count">
          <input data-composition-limit list="recentLimitOptions" type="text" inputmode="numeric" pattern="[1-9]|10" value="${limit}" placeholder="1-10" title="鏈€鏂扮礌鏉愭暟閲? aria-label="鏈€鏂扮礌鏉愭暟閲? />
          <span>鏉＄礌鏉?/span>
        </label>
        <button type="button" data-composition-remove>鍒犻櫎</button>
      </div>`;
  }).join("");
  $("compositionRows").querySelectorAll(".composition-row").forEach((row) => {
    row.querySelector("[data-composition-category]").onchange = () => {
      updateCompositionState(true);
      renderComposition(data);
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
      duration: defaultDurationForCategory(row.querySelector("[data-composition-category]").value),
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
    log("鐢熸垚缁撴瀯鑷冲皯淇濈暀 1 涓墖娈点€?);
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
  $("hudText").value = sanitizeHudText(state.hud_text || "");
  state.hud_text = $("hudText").value;
  syncFollowTextFixedField($("followText").value);
  syncHudTextFixedField($("hudText").value);
  ["headline", "subhead", "followText", "hudText"].forEach((id) => {
    $(id).addEventListener("input", scheduleStateSave);
  });
  $("followText").addEventListener("input", () => syncFollowTextFixedField($("followText").value));
  $("hudText").addEventListener("input", () => {
    const value = sanitizeHudText($("hudText").value);
    if (value !== $("hudText").value) $("hudText").value = value;
    state.hud_text = value;
    syncHudTextFixedField(value);
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
      trigger.innerHTML = buttonLoadingInline("鍒囨崲涓?..");
      try {
        await selectCoverTemplate(button.dataset.coverTemplate);
      } finally {
        trigger.disabled = false;
        trigger.classList.remove("is-loading");
        trigger.innerHTML = buttonIconLabel("鈬?, "妯℃澘鍒囨崲");
      }
    };
  });
}

function renderVideoTemplateSelector() {
  if (!$("videoTemplateSelector")) return;
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
  return numberedTemplateName("绗竴灞忓皝闈㈡ā鏉?, id, item, index, /(?:绗竴灞忓皝闈㈡ā鏉縷涔濆鏍煎浘鐗囨ā鏉?\s*(\d+)/);
}

function videoTemplateDisplayName(id, item, index = Object.keys(templates).indexOf(id)) {
  return numberedTemplateName("瑙嗛鍙犲眰妯℃澘", id, item, index, /瑙嗛鍙犲眰妯℃澘\s*(\d+)/);
}

function endingTemplateDisplayName(id, item, index = Object.keys(endingCoverTemplates()).indexOf(id)) {
  return numberedTemplateName("鐗囧熬灏侀潰妯℃澘", id, item, index, /(?:鐗囧熬灏侀潰妯℃澘|鐗囧熬妯℃澘)\s*(\d+)/);
}

async function selectVideoTemplate(templateId, options = {}) {
  if (!templateId || !templates[templateId]) return;
  const refreshTemplateGallery = options.refreshTemplateGallery !== false;
  selectedVideoTemplate = templateId;
  setImageLoading("videoTemplatePreview", "鍒囨崲姝ｆ枃妯℃澘...");
  if (refreshTemplateGallery) setPanelLoading("videoTemplateGallery", "鍒囨崲姝ｆ枃妯℃澘...");
  if ($("sidebarVideoTemplate")) $("sidebarVideoTemplate").value = templateId;
  renderVideoTemplateEditor();
  await saveTemplateSelection();
  await refreshVideoTemplatePreview();
  if (refreshTemplateGallery) await refreshVideoTemplateGallery();
}

function renderCoverEditor() {
  const t = coverTemplates[selectedCover];
  if (!t) {
    clearImageLoading("coverPreview");
    $("previewCaption").textContent = "绗竴灞忔ā鏉跨己澶憋紝璇峰垏鎹㈠埌鍙敤妯℃澘";
    $("coverForm").innerHTML = `<div class="muted">褰撳墠妯℃澘涓嶅瓨鍦紝璇蜂粠宸︿晶鈥滅涓€灞忓皝闈㈡ā鏉库€濋噸鏂伴€夋嫨銆?/div>`;
    return;
  }
  applyIndependentCoverDefaults(t);
  const independentCover = isIndependentCover(t);
  $("previewCaption").textContent = `${selectedCover} / ${coverTemplateDisplayName(selectedCover, t)}${independentCover ? " / 鐙珛瑙嗛灏侀潰" : ""}`;
  renderCoverTemplateMenu();
  const toggle = $("coverLayoutToggle");
  if (toggle) {
    toggle.innerHTML = independentCover ? buttonIconLabel("鈽?, "鍒楄〃鏁堟灉棰勮") : buttonIconLabel("鈻?, "鐙珛灏侀潰棰勮");
    toggle.classList.toggle("active", independentCover);
    toggle.onclick = toggleCoverLayout;
  }
  const maskModeOptions = coverMaskModeOptions.map(([value, label]) =>
    `<option value="${value}" ${value === coverTemplateValue(t, "mask_mode", "bottom_gradient") ? "selected" : ""}>${label}</option>`
  ).join("");
  const html = [`<h3>鍙鍖栬皟鏁?/h3>`, `
    <label>妯℃澘鍚嶇О<input data-key="name" type="text" value="${escapeHtml(t.name || "")}"></label>
    <div class="cover-section-title">钂欑増缂栬緫鍖?/div>
    <label>钂欑増绫诲瀷<select data-key="mask_mode">${maskModeOptions}</select></label>
    <label>钂欑増棰滆壊<input data-key="mask_color" type="color" value="${escapeHtml(coverTemplateValue(t, "mask_color", t.gradient_color || t.tint_color || "#071015"))}"></label>
    ${rangeControlHtml({key: "mask_opacity", label: "钂欑増閫忔槑搴?, min: 0, max: 1, step: 0.01, value: coverTemplateValue(t, "mask_opacity", t.gradient_opacity ?? t.tint_opacity ?? 0.35), className: "cover-template-control"})}
    <div class="cover-section-title">鐙珛灏侀潰鏂囧瓧</div>
    <label>Logo鏂囧瓧<input data-key="single_cover_logo_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_logo_text", "GasGx"))}"></label>
    <label>Slogan鏂囧瓧<input data-key="single_cover_slogan_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_slogan_text", defaultSingleCoverSlogan()))}"></label>
    <label>涓€鍙ヨ瘽瑙嗛鎻忚堪<textarea data-key="single_cover_title_text" rows="3">${escapeHtml(coverTemplateValue(t, "single_cover_title_text", defaultSingleCoverTitle()))}</textarea></label>
    ${coverVisualToolbarHtml(t)}
    <p class="visual-editor-hint">鐐瑰嚮棰勮閲岀殑鏂囧瓧鎴栨寜閽悗鎷栧姩瀹氫綅锛涘伐鍏锋爮鍙皟鏁村瓧鍙枫€侀鑹层€佸榻愬拰鏂囧瓧鍐呭銆?/p>
    <div class="template-actions cover-template-actions">
      <button type="button" id="saveCover">淇濆瓨</button>
      <button type="button" id="saveCoverAsNew">鏂板缓淇濆瓨</button>
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
    `<button type="button" class="${mode === value ? "active" : ""} ${endingModeLoading === value ? "is-loading" : ""}" data-ending-mode="${value}" ${endingModeLoading ? "disabled" : ""}>${endingModeLoading === value ? buttonLoadingInline("鍒囨崲涓?..") : label}</button>`
  ).join("");
  const options = localTemplates.length
    ? localTemplates.map((item) => `<option value="${escapeHtml(item.name)}" ${selected === item.name ? "selected" : ""}>${escapeHtml(item.name)}</option>`).join("")
    : `<option value="">鐩綍鍐呮殏鏃犺棰戠墖灏剧礌鏉?/option>`;
  $("endingTemplateForm").innerHTML = `
    <h3>鐗囧熬妯℃澘璋冩暣鍖?/h3>
    <div id="endingTemplateUploadStatus" class="ending-template-upload-status" hidden></div>
    <div class="template-tabs ending-mode-tabs">${modeButtons}</div>
    ${mode === "dynamic" ? endingCoverEditorHtml() : ""}
    ${mode === "random" ? endingRandomMaterialHtml(localTemplates) : ""}
    <div class="ending-template-upload-row">
      <label class="ending-template-upload">
        <span>涓婁紶鐗囧熬 MP4</span>
        <div class="ending-template-upload-trigger">
          <span class="ending-template-upload-btn">閫夋嫨鏂囦欢</span>
          <span id="endingTemplateUploadName" class="ending-template-upload-name">鏈€夋嫨浠讳綍鏂囦欢</span>
          <input id="endingTemplateUpload" type="file" accept=".mp4,video/mp4">
        </div>
        <small>浠呮敮鎸?MP4锛屽缓璁枃浠跺悕淇濇寔鍘熷绱犳潗鍚嶏紝涓婁紶鍚庝細杩涘叆鐗囧熬鐩綍銆?/small>
      </label>
    </div>
    <div class="ending-template-dir-row ${mode === "random" ? "" : "hidden"}">
      <code title="${escapeHtml(endingTemplateState.directory)}">${escapeHtml(shortPath(endingTemplateState.directory))}</code>
      <span class="badge">${localTemplates.length} 涓礌鏉?/span>
      <button id="openEndingTemplateDirInline" class="secondary" type="button">鎵撳紑</button>
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
  renderEndingTemplateMenu();
  $("openEndingTemplateDirInline").onclick = () => openFolder(endingTemplateState.directory);
  $("endingTemplateUpload").onchange = async () => {
    const file = $("endingTemplateUpload").files?.[0];
    const fileName = $("endingTemplateUploadName");
    if (!file) {
      if (fileName) fileName.textContent = "鏈€夋嫨浠讳綍鏂囦欢";
      return;
    }
    if (fileName) fileName.textContent = file.name;
    if (!/\.mp4$/i.test(file.name)) {
      showEndingTemplateUploadStatus("浠呮敮鎸?MP4 鏂囦欢锛岃閲嶆柊閫夋嫨銆?, "warn");
      $("endingTemplateUpload").value = "";
      if (fileName) fileName.textContent = "鏈€夋嫨浠讳綍鏂囦欢";
      return;
    }
    const form = new FormData();
    form.append("file", file);
    try {
      showEndingTemplateUploadStatus("姝ｅ湪涓婁紶鐗囧熬 MP4...", "loading");
      await api("/api/video-matrix/ending-templates/upload", { method: "POST", body: form });
      $("endingTemplateUpload").value = "";
      if (fileName) fileName.textContent = "鏈€夋嫨浠讳綍鏂囦欢";
      const data = await api("/api/video-matrix/state");
      renderEndingTemplatePanel(data);
      await refreshEndingTemplatePreview();
      showEndingTemplateUploadStatus(`涓婁紶鎴愬姛锛?{file.name}`, "success");
      log(`宸蹭笂浼犵墖灏剧礌鏉愶細${file.name}`);
    } catch (error) {
      showEndingTemplateUploadStatus(`涓婁紶澶辫触锛?{error.message}`, "error");
    }
  };
}

function showEndingTemplateUploadStatus(message, tone = "success") {
  const node = $("endingTemplateUploadStatus");
  if (!node) return;
  node.hidden = false;
  node.dataset.tone = tone;
  node.textContent = message;
  window.clearTimeout(showEndingTemplateUploadStatus.timer);
  showEndingTemplateUploadStatus.timer = window.setTimeout(() => {
    node.hidden = true;
  }, 2600);
}

async function switchEndingTemplateMode(mode, sourceButton = null) {
  if (!mode) return;
  endingModeLoading = mode;
  if (sourceButton) {
    sourceButton.classList.add("is-loading");
    sourceButton.innerHTML = buttonLoadingInline("鍒囨崲涓?..");
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
        <small>${item.type === "video" ? "瑙嗛" : "鍥剧墖"}</small>
        ${item.type === "video" ? endingPreviewToggleButtonHtml(item.name) : ""}
      </label>`).join("")
    : `<div class="ending-template-empty compact">鏆傛棤瑙嗛鐗囧熬绱犳潗锛岃鍏堜笂浼犲埌鐩綍銆?/div>`;
  return `
    <div class="cover-section-title">瑙嗛鐗囧熬閫夋嫨</div>
    <div class="ending-material-list">${rows}</div>
    <p class="visual-editor-hint">浠?video_matrix\\ending_template 鍕鹃€夊鐢ㄨ棰戠墖灏撅紱鐢熸垚鏃朵細浠庡凡鍕鹃€夌礌鏉愰噷闅忔満鍙栦竴涓€?/p>`;
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
  return `<button class="ending-preview-toggle ${active ? "active" : ""}" type="button" data-ending-preview-toggle="${escapeHtml(name)}" title="${active ? "鍋滄棰勮" : "鎾斁棰勮"}" aria-label="${active ? "鍋滄棰勮" : "鎾斁棰勮"}">${icon}</button>`;
}

function endingCoverTemplate() {
  ensureEndingCoverTemplates();
  const selectedId = state.ending_cover_template_id;
  const base = state.ending_cover_templates[selectedId];
  state.ending_cover_template = JSON.parse(JSON.stringify(base));
  applyIndependentCoverDefaults(state.ending_cover_template);
  state.ending_cover_template.cover_layout = "single_video";
  return state.ending_cover_template;
}

function isInheritedEndingCoverName(name) {
  const value = String(name || "").trim();
  if (!value || value === "Ending Cover") return true;
  if (/^鐗囧熬妯℃澘\s*\d+$/i.test(value)) return true;
  return Object.values(coverTemplates || {}).some((template) => String(template?.name || "").trim() === value);
}

function ensureEndingCoverTemplates() {
  const fallback = state.ending_cover_template && typeof state.ending_cover_template === "object"
    ? JSON.parse(JSON.stringify(state.ending_cover_template))
    : JSON.parse(JSON.stringify(coverTemplates[selectedCover] || {}));
  applyIndependentCoverDefaults(fallback);
  fallback.cover_layout = "single_video";
  if (isInheritedEndingCoverName(fallback.name)) fallback.name = "鐗囧熬灏侀潰妯℃澘 01";
  if (!state.ending_cover_templates || typeof state.ending_cover_templates !== "object") {
    state.ending_cover_templates = { ending_cover_template_01: fallback };
  }
  if (!state.ending_cover_template_id || !state.ending_cover_templates[state.ending_cover_template_id]) {
    state.ending_cover_template_id = Object.keys(state.ending_cover_templates)[0] || "ending_cover_template_01";
  }
  if (!state.ending_cover_templates[state.ending_cover_template_id]) {
    state.ending_cover_templates[state.ending_cover_template_id] = fallback;
  }
}

function endingCoverTemplates() {
  ensureEndingCoverTemplates();
  return state.ending_cover_templates;
}

function renderEndingTemplateMenu() {
  const menu = $("endingTemplateMenu");
  const trigger = $("endingTemplateSwitch");
  if (!menu || !trigger) return;
  menu.innerHTML = Object.entries(endingCoverTemplates()).map(([id, item], index) =>
    `<button type="button" class="${id === state.ending_cover_template_id ? "active" : ""}" data-ending-cover-template="${escapeHtml(id)}">${escapeHtml(endingTemplateDisplayName(id, item, index))}</button>`
  ).join("");
  trigger.onclick = () => {
    const expanded = menu.classList.toggle("hidden") === false;
    trigger.setAttribute("aria-expanded", expanded ? "true" : "false");
  };
  menu.querySelectorAll("[data-ending-cover-template]").forEach((button) => {
    button.onclick = async () => {
      menu.classList.add("hidden");
      trigger.setAttribute("aria-expanded", "false");
      trigger.disabled = true;
      trigger.classList.add("is-loading");
      trigger.innerHTML = buttonLoadingInline("鍒囨崲涓?..");
      try {
        await selectEndingCoverTemplate(button.dataset.endingCoverTemplate);
      } finally {
        trigger.disabled = false;
        trigger.classList.remove("is-loading");
        trigger.innerHTML = buttonIconLabel("鈬?, "妯℃澘鍒囨崲");
      }
    };
  });
}

async function selectEndingCoverTemplate(templateId) {
  const templateMap = endingCoverTemplates();
  if (!templateId || !templateMap[templateId]) return;
  state.ending_cover_template_id = templateId;
  state.ending_cover_template = JSON.parse(JSON.stringify(templateMap[templateId]));
  setImageLoading("endingTemplatePreview", "鍒囨崲鐗囧熬妯℃澘...");
  renderEndingTemplatePanel({ ending_templates: endingTemplateState.local, ending_template_dir: endingTemplateState.directory });
  await saveState();
  await refreshEndingTemplatePreview();
}

function endingCoverEditorHtml() {
  const t = endingCoverTemplate();
  const maskModeOptions = coverMaskModeOptions.map(([value, label]) =>
    `<option value="${value}" ${value === coverTemplateValue(t, "mask_mode", "bottom_gradient") ? "selected" : ""}>${label}</option>`
  ).join("");
  return `
    <label>妯℃澘鍚嶇О<input data-ending-cover-key="name" type="text" value="${escapeHtml(t.name || "鐗囧熬灏侀潰妯℃澘 01")}"></label>
    <div class="cover-section-title">钂欑増缂栬緫鍖?/div>
    <label>钂欑増绫诲瀷<select data-ending-cover-key="mask_mode">${maskModeOptions}</select></label>
    <label>钂欑増棰滆壊<input data-ending-cover-key="mask_color" type="color" value="${escapeHtml(coverTemplateValue(t, "mask_color", t.gradient_color || t.tint_color || "#071015"))}"></label>
    ${rangeControlHtml({key: "ending-mask-opacity", label: "钂欑増閫忔槑搴?, min: 0, max: 1, step: 0.01, value: coverTemplateValue(t, "mask_opacity", t.gradient_opacity ?? t.tint_opacity ?? 0.35), className: "ending-cover-control"})}
    <div class="cover-section-title">鐙珛灏侀潰鏂囧瓧</div>
    <label>Logo鏂囧瓧<input data-ending-cover-key="single_cover_logo_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_logo_text", "GasGx"))}"></label>
    <label>Slogan鏂囧瓧<input data-ending-cover-key="single_cover_slogan_text" type="text" value="${escapeHtml(coverTemplateValue(t, "single_cover_slogan_text", defaultSingleCoverSlogan()))}"></label>
    <label>鐗囧熬鏂囨<textarea data-ending-cover-key="single_cover_title_text" rows="3">${escapeHtml(coverTemplateValue(t, "single_cover_title_text", $("followText").value || state.follow_text || defaultSingleCoverTitle()))}</textarea></label>
    ${coverVisualToolbarHtml(t, "ending-cover-visual-toolbar")}
    <div class="template-actions ending-template-actions">
      <button type="button" id="saveEndingCover">淇濆瓨</button>
      <button type="button" id="saveEndingCoverAsNew">鏂板缓淇濆瓨</button>
    </div>
    <p class="visual-editor-hint">鐐瑰嚮鐗囧熬棰勮閲岀殑鏂囧瓧鍚庢嫋鍔ㄥ畾浣嶏紱杩欑粍璁剧疆鍙奖鍝嶆枃瀛楃墖灏俱€?/p>`;
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
  $("saveEndingCover").onclick = saveCurrentEndingCoverTemplate;
  $("saveEndingCoverAsNew").onclick = saveEndingCoverAsNewTemplate;
}

function updateEndingCoverTemplateField(input) {
  const template = endingCoverTemplate();
  const key = input.dataset.endingCoverKey || (input.dataset.key === "ending-mask-opacity" ? "mask_opacity" : input.dataset.key);
  if (!template || !key) return;
  template[key] = input.type === "range" || input.type === "number" ? Number(input.value) : input.value;
  if (state.ending_cover_template_id && state.ending_cover_templates?.[state.ending_cover_template_id]) {
    state.ending_cover_templates[state.ending_cover_template_id] = JSON.parse(JSON.stringify(template));
  }
  if (key === "single_cover_title_text") {
    $("followText").value = input.value;
    state.follow_text = input.value;
    syncFollowTextFixedField(input.value);
  }
  setImageLoading("endingTemplatePreview", "搴旂敤鐗囧熬灏侀潰鍙傛暟...");
  refreshEndingTemplatePreview();
  scheduleStateSave();
}

function nextEndingCoverTemplateMeta(templateMap) {
  let next = 1;
  Object.entries(templateMap || {}).forEach(([id, template]) => {
    const idMatch = String(id).match(/^ending_cover_template_(\d+)$/);
    const nameMatch = String(template?.name || "").match(/^鐗囧熬灏侀潰妯℃澘\s*(\d+)$/);
    const value = Math.max(Number(idMatch?.[1] || 0), Number(nameMatch?.[1] || 0));
    if (value >= next) next = value + 1;
  });
  const serial = String(next).padStart(2, "0");
  return { id: `ending_cover_template_${serial}`, name: `鐗囧熬灏侀潰妯℃澘 ${serial}` };
}

async function saveCurrentEndingCoverTemplate() {
  const template = endingCoverTemplate();
  const selectedId = state.ending_cover_template_id;
  const button = $("saveEndingCover");
  const label = button?.textContent || "淇濆瓨";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("淇濆瓨涓?..");
  }
  try {
    state.ending_cover_templates[selectedId] = JSON.parse(JSON.stringify(template));
    state.ending_cover_template = JSON.parse(JSON.stringify(template));
    await saveState();
    pendingTemplateSave = "";
    renderEndingTemplatePanel({ ending_templates: endingTemplateState.local, ending_template_dir: endingTemplateState.directory });
    await refreshEndingTemplatePreview();
    log(`宸蹭繚瀛樼墖灏炬ā鏉匡細${template.name || selectedId}`);
    showTemplateActionStatus("淇濆瓨鎴愬姛", "endingTemplateForm");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    log(`鐗囧熬妯℃澘淇濆瓨澶辫触锛?{error.message}`);
  }
}

async function saveEndingCoverAsNewTemplate() {
  const sourceTemplate = endingCoverTemplate();
  const nextMeta = nextEndingCoverTemplateMeta(endingCoverTemplates());
  const button = $("saveEndingCoverAsNew");
  const label = button?.textContent || "鏂板缓淇濆瓨";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("鏂板缓涓?..");
  }
  try {
    const newTemplate = JSON.parse(JSON.stringify(sourceTemplate));
    applyIndependentCoverDefaults(newTemplate);
    newTemplate.cover_layout = "single_video";
    newTemplate.name = nextMeta.name;
    state.ending_cover_templates = {...state.ending_cover_templates, [nextMeta.id]: newTemplate};
    state.ending_cover_template_id = nextMeta.id;
    state.ending_cover_template = JSON.parse(JSON.stringify(newTemplate));
    await saveState();
    pendingTemplateSave = "";
    renderEndingTemplatePanel({ ending_templates: endingTemplateState.local, ending_template_dir: endingTemplateState.directory });
    await refreshEndingTemplatePreview();
    log(`宸叉柊寤虹墖灏炬ā鏉匡細${nextMeta.name}`);
    showTemplateActionStatus("鏂板缓淇濆瓨鎴愬姛", "endingTemplateForm");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    log(`鐗囧熬妯℃澘鏂板缓澶辫触锛?{error.message}`);
  }
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
      caption.textContent = `${mode === "random" ? "瑙嗛鐗囧熬绱犳潗棰勮" : "鎸囧畾瑙嗛鐗囧熬"} / ${asset.name}`;
      if (asset.type === "video" && endingPreviewOverrideName) {
        const video = assetBox.querySelector("[data-ending-preview-video]");
        video?.play?.().catch(() => {});
      }
    } else {
      assetBox.innerHTML = `<div class="ending-template-empty">鏆傛棤瑙嗛鐗囧熬绱犳潗</div>`;
      assetBox.classList.remove("hidden");
      caption.textContent = `${shortPath(endingTemplateState.directory)} / 0 涓礌鏉恅;
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
  caption.textContent = `鏂囧瓧鐗囧熬 / ${template.name || "Ending Cover"}`;
  clearImageLoading("endingTemplatePreview");
}

function isIndependentCover(template) {
  return (template?.cover_layout || "profile") === "single_video";
}

function defaultSingleCoverTitle() {
  return "鍏ㄧ悆棰嗗厛鐨勬悂娴呭ぉ鐒舵皵绠楀姏鍙樼幇寮曟搸";
}

function defaultSingleCoverSlogan() {
  return "缁堢粨搴熸皵 | 閲嶅鑳芥簮 | 灏卞湴鍙樼幇";
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
  setImageLoading("coverPreview", "鍒囨崲灏侀潰甯冨眬...");
  await refreshMainPreview();
  scheduleCoverTemplateSave();
}

function coverTemplateValue(template, key, fallback = "") {
  return template[key] ?? fallback;
}

function defaultCoverTileTitles() {
  return ["鐕冩皵鍙戠數鏈虹粍骞剁綉娴嬭瘯", "娌圭敯浼寸敓姘旇祫婧愬啀鍒╃敤", "绉诲姩寮忕畻鍔涗腑蹇冮儴缃?, "閲庡鍙戠數璁惧鏃ュ父缁存姢", "闆剁噧闄わ細鍙樺簾涓哄疂", "闆嗚绠辨暟鎹腑蹇冨唴鏅?, "楂樻晥鐕冩皵杞満杩愯鐘舵€?, "澶滈棿浜曞満鎸佺画鍙戠數浣滀笟", "鏋佸瘨鐜璁惧鍚姩娴嬭瘯"];
}

function updateCoverTemplateField(input) {
  const template = coverTemplates[selectedCover];
  if (!template || !input) return;
  const key = input.dataset.key;
  template[key] = input.type === "range" || input.type === "number" ? Number(input.value) : input.value;
  setImageLoading("coverPreview", "搴旂敤灏侀潰鍙傛暟...");
  refreshAllPreviews();
  scheduleCoverTemplateSave();
}

function coverVisualToolbarHtml(template, extraClass = "") {
  const fontValue = template.title_font_family || visualFontOptions[0][0];
  const fontOptions = visualFontOptions.map(([value, label]) =>
    `<option value="${escapeHtml(value)}" ${value === fontValue ? "selected" : ""}>${label}</option>`
  ).join("");
  return `
    <div class="visual-toolbar-panel cover-visual-toolbar ${extraClass}" aria-label="灏侀潰鍙鍖栧伐鍏?>
      <button type="button" data-cover-command="size-down" title="缂╁皬瀛楀彿">A-</button>
      <button type="button" data-cover-command="size-up" title="鏀惧ぇ瀛楀彿">A+</button>
      <button type="button" data-cover-command="edit" title="缂栬緫鏂囧瓧">缂栬緫</button>
      <button type="button" data-cover-command="align" data-value="left" title="宸﹀榻?>宸﹂綈</button>
      <button type="button" data-cover-command="align" data-value="center" title="灞呬腑瀵归綈">灞呬腑</button>
      <button type="button" data-cover-command="align" data-value="right" title="鍙冲榻?>鍙抽綈</button>
      <select data-cover-command="font-family">${fontOptions}</select>
      <label class="color-swatch-button" title="鏂囧瓧棰滆壊">
        <svg class="color-picker-icon" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 3a9 9 0 0 0 0 18h1.4a2 2 0 0 0 1.7-3l-.2-.4a1.7 1.7 0 0 1 1.5-2.6H18a6 6 0 0 0 0-12h-6Z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>
          <circle cx="7.5" cy="10" r="1.3" fill="currentColor"/>
          <circle cx="10.5" cy="6.8" r="1.3" fill="currentColor"/>
          <circle cx="15" cy="7.8" r="1.3" fill="currentColor"/>
          <circle cx="16.8" cy="11.5" r="1.3" fill="currentColor"/>
        </svg>
        <span class="color-current-dot" style="background:${escapeHtml(template.primary_color || "#ffffff")}"></span>
        <input data-cover-command="color" type="color" value="${escapeHtml(template.primary_color || "#ffffff")}" aria-label="鏂囧瓧棰滆壊">
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
    pulseImageLoading("endingTemplatePreview", "搴旂敤鐗囧熬灏侀潰鍙傛暟...");
  } else {
    pulseImageLoading("coverPreview", "搴旂敤灏侀潰鍙傛暟...");
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
  const visibilityChecks = [];
  const html = [
    `<h3>妯℃澘璋冩暣鍖?/h3>`,
    `<label>妯℃澘鍚嶇О<input data-key="name" type="text" value="${escapeHtml(template.name || "")}"></label>`,
    visualTemplateToolbarHtml(template),
  ];
  for (const [key, label, type, min, max] of videoTemplateFields) {
    if (key === "name") continue;
    const value = template[key] ?? "";
    if (type === "checkbox") {
      visibilityChecks.push(`<label class="check-row"><input data-key="${key}" type="checkbox" ${value ? "checked" : ""}><span>${label}</span></label>`);
    } else if (type === "select") {
      html.push(`<label>${label}<select data-key="${key}"><option value="left">宸﹀榻?/option><option value="center">灞呬腑</option><option value="right">鍙冲榻?/option></select></label>`);
    } else if (type === "range") {
      html.push(rangeControlHtml({key, label, min, max, value, className: "template-control"}));
    } else if (type === "rangeFloat") {
      html.push(rangeControlHtml({key, label, min, max, step: 0.01, value, className: "template-control"}));
    } else {
      html.push(`<label>${label}<input data-key="${key}" type="${type}" value="${escapeHtml(value)}"></label>`);
    }
  }
  if (visibilityChecks.length) {
    html.push(`<div class="template-visibility-row">${visibilityChecks.join("")}</div>`);
  }
  html.push(`
    <div class="template-actions">
      <button type="button" id="saveVideoTemplate">淇濆瓨褰撳墠</button>
      <button type="button" id="cloneVideoTemplate" title="鍩轰簬褰撳墠姝ｆ枃妯℃澘鏂板缓淇濆瓨">鏂板缓淇濆瓨</button>
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
  const styleValue = template.title_text_style || "none";
  const hudOpacity = Number(template.hud_bar_opacity ?? 0.68);
  const hudRadius = Number(template.hud_bar_radius ?? 10);
  const hudColor = template.hud_bar_color || "#0E1A10";
  const fontSamples = videoTextFontOptions.map(([value, label]) => `
          <button type="button" class="font-sample-option ${value === fontValue ? "active" : ""}" data-visual-command="font-family" data-value="${escapeHtml(value)}" title="${escapeHtml(label)}">
            <span class="font-sample-name">${escapeHtml(label)}</span>
            <span class="font-sample-lines" style="font-family:${escapeHtml(value)}">
              ${fontSamplePreviewHtml(label)}
            </span>
          </button>`).join("");
  const effectLabel = visualOptionLabel(textEffectOptions, effectValue);
  const styleLabel = visualOptionLabel(textStyleOptions, styleValue);
  const effectOptions = visualDropdownOptionsHtml(textEffectOptions, effectValue, "text-effect");
  const styleOptions = visualDropdownOptionsHtml(textStyleOptions, styleValue, "text-style");
  return `
    <div class="visual-toolbar-panel" aria-label="鏂囧瓧鍙鍖栧伐鍏?>
      <div class="visual-target-tabs" aria-label="鍙犲眰閫夋嫨">
        <button type="button" data-visual-command="select-target" data-value="slogan" title="閫夋嫨涓婃爣棰?>涓婃爣棰?/button>
        <button type="button" data-visual-command="select-target" data-value="title" title="閫夋嫨涓爣棰?>涓爣棰?/button>
        <button type="button" data-visual-command="select-target" data-value="hud" title="閫夋嫨涓嬫爣棰?>涓嬫爣棰?/button>
      </div>
      <div class="visual-control-section visual-text-controls" aria-label="鏂囧瓧璋冩暣鍖?>
        <div class="visual-section-title">鏂囧瓧璋冩暣鍖?/div>
        <button type="button" data-visual-command="text-align" data-value="left" title="鏂囧瓧闈犲乏">鏂囧乏</button>
        <button type="button" data-visual-command="text-align" data-value="center" title="鏂囧瓧灞呬腑">鏂囦腑</button>
        <button type="button" data-visual-command="text-align" data-value="right" title="鏂囧瓧闈犲彸">鏂囧彸</button>
        <button type="button" data-visual-command="text-width-down" title="缂╁皬鏂囧瓧妗?>妗哤-</button>
        <button type="button" data-visual-command="text-width-up" title="鏀惧ぇ鏂囧瓧妗?>妗哤+</button>
        <button type="button" data-visual-command="size-down" title="缂╁皬瀛楀彿">A-</button>
        <button type="button" data-visual-command="size-up" title="鏀惧ぇ瀛楀彿">A+</button>
        <button type="button" data-visual-command="edit" title="缂栬緫鏂囧瓧">缂栬緫</button>
        <label class="color-swatch-button" title="鏂囧瓧棰滆壊">
          ${colorPickerIconSvg()}
          <span class="color-current-dot" style="background:${escapeHtml(template.primary_color || "#ffffff")}"></span>
          <input data-visual-command="color" type="color" value="${escapeHtml(template.primary_color || "#ffffff")}" aria-label="鏂囧瓧棰滆壊">
        </label>
        <div class="font-sample-picker" role="listbox" aria-label="瀛椾綋鏍峰紶閫夋嫨">
          ${fontSamples}
        </div>
        <label class="visual-effect-control">鏂囧瓧鏍峰紡
          <div class="visual-dropdown">
            <button type="button" class="visual-dropdown-trigger" data-visual-dropdown-trigger aria-expanded="false">${escapeHtml(styleLabel)}</button>
            <div class="visual-dropdown-menu" role="listbox">${styleOptions}</div>
          </div>
        </label>
        <label class="visual-effect-control">鏂囧瓧鍔ㄦ晥
          <div class="visual-dropdown">
            <button type="button" class="visual-dropdown-trigger" data-visual-dropdown-trigger aria-expanded="false">${escapeHtml(effectLabel)}</button>
            <div class="visual-dropdown-menu" role="listbox">${effectOptions}</div>
          </div>
        </label>
      </div>
      <div class="visual-control-section visual-hud-controls" aria-label="瀛楀箷鑳屾澘璋冩暣鍖?>
        <div class="visual-section-title">瀛楀箷鑳屾澘璋冩暣鍖?/div>
        <button type="button" data-visual-command="background-full" title="鑳屾櫙鑷姩椤舵弧瀹藉害">婊″</button>
        <button type="button" data-visual-command="background-partial-center" title="鑳屾櫙灞呬腑鍧?>灞呬腑鍧?/button>
        <button type="button" data-visual-command="width-down" title="缂╁皬鑳屾櫙瀹藉害">W-</button>
        <button type="button" data-visual-command="width-up" title="鏀惧ぇ鑳屾櫙瀹藉害">W+</button>
        <button type="button" data-visual-command="height-down" title="缂╁皬鑳屾櫙楂樺害">H-</button>
        <button type="button" data-visual-command="height-up" title="鏀惧ぇ鑳屾櫙楂樺害">H+</button>
        <button type="button" data-visual-command="bar-align" data-value="left" title="瀛楀箷鑳屾澘宸﹀榻?>宸﹂綈</button>
        <button type="button" data-visual-command="bar-align" data-value="center" title="瀛楀箷鑳屾澘灞呬腑瀵归綈">灞呬腑</button>
        <button type="button" data-visual-command="bar-align" data-value="right" title="瀛楀箷鑳屾澘鍙冲榻?>鍙抽綈</button>
        <label class="color-swatch-button" title="瀛楀箷鑳屾澘鑳屾櫙鑹?>
          ${colorPickerIconSvg()}
          <span class="color-current-dot" style="background:${escapeHtml(hudColor)}"></span>
          <input data-visual-command="hud-bg-color" type="color" value="${escapeHtml(hudColor)}" aria-label="瀛楀箷鑳屾澘鑳屾櫙鑹?>
        </label>
        <label class="visual-opacity-control">瀛楀箷鑳屾澘閫忔槑搴?input data-visual-command="opacity" type="range" min="0" max="1" step="0.01" value="${escapeHtml(hudOpacity.toFixed(2))}"><output>${escapeHtml(hudOpacity.toFixed(2))}</output></label>
        <label class="visual-opacity-control">瀛楀箷鑳屾澘鍦嗚<input data-visual-command="hud-radius" type="range" min="0" max="100" step="1" value="${escapeHtml(String(Math.round(hudRadius)))}"><output>${escapeHtml(String(Math.round(hudRadius)))}</output></label>
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
  if (!visualDropdownCloseBound) {
    document.addEventListener("click", (event) => {
      if (event.target.closest(".visual-dropdown")) return;
      document.querySelectorAll(".visual-dropdown.open").forEach((node) => {
        node.classList.remove("open");
        const trigger = node.querySelector("[data-visual-dropdown-trigger]");
        if (trigger) trigger.setAttribute("aria-expanded", "false");
      });
    });
    visualDropdownCloseBound = true;
  }
  toolbar.querySelectorAll("button[data-visual-command]").forEach((button) => {
    button.onclick = () => {
      if (button.classList.contains("visual-dropdown-option")) {
        const dropdown = button.closest(".visual-dropdown");
        const trigger = dropdown?.querySelector("[data-visual-dropdown-trigger]");
        dropdown?.querySelectorAll(".visual-dropdown-option.active").forEach((node) => node.classList.remove("active"));
        button.classList.add("active");
        if (trigger) {
          trigger.textContent = button.textContent.trim();
          trigger.setAttribute("aria-expanded", "false");
        }
        dropdown?.classList.remove("open");
      }
      if (button.dataset.visualCommand === "font-family") {
        toolbar.querySelectorAll(".font-sample-option.active").forEach((node) => node.classList.remove("active"));
        button.classList.add("active");
      }
      postVisualTemplateCommand(button.dataset.visualCommand, button.dataset.value || "", visualCommandScope(button));
    };
  });
  toolbar.querySelectorAll("[data-visual-dropdown-trigger]").forEach((button) => {
    button.onclick = () => {
      const dropdown = button.closest(".visual-dropdown");
      const willOpen = !dropdown?.classList.contains("open");
      toolbar.querySelectorAll(".visual-dropdown.open").forEach((node) => {
        node.classList.remove("open");
        const trigger = node.querySelector("[data-visual-dropdown-trigger]");
        if (trigger) trigger.setAttribute("aria-expanded", "false");
      });
      if (dropdown && willOpen) {
        dropdown.classList.add("open");
        button.setAttribute("aria-expanded", "true");
      }
    };
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
  pulseImageLoading("videoTemplatePreview", "搴旂敤妯℃澘鍙傛暟...");
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
  setImageLoading("videoTemplatePreview", "搴旂敤妯℃澘鍙傛暟...");
  refreshVideoTemplatePreview();
  refreshVideoTemplateGallery({ showLoading: false });
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
  refreshVideoTemplateGallery({ showLoading: false });
  scheduleVideoTemplateSave();
}

function applyVisualTextUpdates(text) {
  if (!text) return;
  const fieldMap = { slogan: "headline", title: "subhead", hud_text: "hudText" };
  Object.entries(text).forEach(([key, value]) => {
    const field = $(fieldMap[key]);
    if (field) field.value = value;
  });
  refreshVideoTemplateGallery({ showLoading: false });
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
  if (state.ending_cover_template_id && state.ending_cover_templates?.[state.ending_cover_template_id]) {
    state.ending_cover_templates[state.ending_cover_template_id] = JSON.parse(JSON.stringify(template));
  }
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
        syncFollowTextFixedField(value);
      }
    }
  });
  if (state.ending_cover_template_id && state.ending_cover_templates?.[state.ending_cover_template_id]) {
    state.ending_cover_templates[state.ending_cover_template_id] = JSON.parse(JSON.stringify(template));
  }
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
    node.innerHTML = `<span class="muted">modelimg 鐩綍鏆傛棤鍙瑙堝浘鐗?/span>`;
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
  if (!template) {
    clearImageLoading("videoTemplatePreview");
    return;
  }
  refreshPhonePreviewFrame("videoTemplatePreview", videoTemplatePreviewPayload(template));
  clearImageLoading("videoTemplatePreview");
}

async function refreshVideoTemplateGallery(options = {}) {
  if (options.showLoading !== false) setPanelLoading("videoTemplateGallery", "鐢熸垚姝ｆ枃妯℃澘鍒楄〃...");
  const cards = [];
  Object.entries(templates).forEach(([id, template], index) => {
    cards.push(`<div class="cover-card video-template-card ${id === selectedVideoTemplate ? "active" : ""}" data-id="${id}">${videoTemplateCardPreviewHtml(id, template)}<button type="button" class="video-template-name-button" data-template-name="${escapeHtml(id)}">${escapeHtml(videoTemplateDisplayName(id, template, index))}</button></div>`);
  });
  const gallery = $("videoTemplateGallery");
  gallery.innerHTML = cards.join("");
  gallery.querySelectorAll(".cover-card").forEach((card) => {
    card.onclick = async (event) => {
      if (event.target?.tagName === "VIDEO") {
        toggleTemplateCardVideo(event.target);
        return;
      }
      await selectVideoTemplate(card.dataset.id);
    };
  });
  window.requestAnimationFrame(() => fitVideoTemplateThumbFrames(gallery));
  bindVideoTemplateThumbFrameScale();
}

function videoTemplateCardPreviewHtml(templateId, template) {
  const hasBg = Boolean(selectedModelImageUrl || modelImages[0]?.url);
  if (!hasBg) return `<div class="video-template-thumb empty"><span>鏆傛棤鑳屾櫙鍥?/span></div>`;
  const payload = videoTemplatePreviewPayload(template);
  return `
    <div class="video-template-thumb">
      <iframe
        class="video-template-thumb-frame"
        src="${escapeHtml(previewFrameUrlForPayload(payload))}"
        title="${escapeHtml(template?.name || templateId)}"
        loading="lazy"
        scrolling="no"
        tabindex="-1"
      ></iframe>
    </div>
  `;
}

function fitVideoTemplateThumbFrames(root = document) {
  const designWidth = 479.25;
  const designHeight = 852;
  root.querySelectorAll(".video-template-thumb-frame").forEach((frame) => {
    const thumb = frame.closest(".video-template-thumb");
    if (!thumb) return;
    const innerWidth = Math.max(1, thumb.clientWidth - 12);
    const innerHeight = Math.max(1, thumb.clientHeight - 12);
    const scale = Math.max(0.12, Math.min(innerWidth / designWidth, innerHeight / designHeight));
    frame.style.transform = `translate(-50%, -50%) scale(${scale.toFixed(4)})`;
  });
}

function bindVideoTemplateThumbFrameScale() {
  if (videoTemplateThumbScaleBound) return;
  const rerender = () => {
    const gallery = $("videoTemplateGallery");
    if (!gallery) return;
    fitVideoTemplateThumbFrames(gallery);
  };
  window.addEventListener("resize", rerender);
  videoTemplateThumbScaleBound = true;
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
  const label = button?.textContent || "淇濆瓨褰撳墠";
  if (button) {
    button.disabled = true;
    button.textContent = "淇濆瓨涓?..";
  }
  try {
    await api(`/api/video-matrix/templates/${selectedVideoTemplate}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(templates[selectedVideoTemplate])});
    await saveState();
    pendingTemplateSave = "";
    log(`宸蹭繚瀛樻鏂囨ā鏉匡細${displayTemplateName(templates[selectedVideoTemplate].name || selectedVideoTemplate)}`);
    renderVideoTemplateSelector();
    renderVideoTemplateEditor();
    showTemplateActionStatus("淇濆瓨鎴愬姛");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.textContent = label;
    }
    log(`姝ｆ枃妯℃澘淇濆瓨澶辫触锛?{error.message}`);
  }
}

async function cloneVideoTemplate() {
  const button = $("cloneVideoTemplate");
  const label = button?.textContent || "鏂板缓淇濆瓨";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("鏂板缓涓?..");
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
    log(`宸插熀浜庡綋鍓嶆鏂囨ā鏉挎柊寤猴細${nextName}`);
    showTemplateActionStatus("鏂板缓淇濆瓨鎴愬姛");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    log(`姝ｆ枃妯℃澘鏂板缓澶辫触锛?{error.message}`);
  }
}

function showTemplateActionStatus(message, formId = "videoTemplateForm") {
  let status = $("templateActionToast");
  if (!status) {
    status = document.createElement("div");
    status.id = "templateActionToast";
    status.className = "template-action-toast";
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");
    document.body.appendChild(status);
  }
  status.textContent = message;
  status.classList.add("show");
  window.clearTimeout(showTemplateActionStatus.timer);
  showTemplateActionStatus.timer = window.setTimeout(() => {
    status.classList.remove("show");
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
    const nameMatch = String(template?.name || "").match(/^绗竴灞忓皝闈㈡ā鏉縗s*(\d+)$/);
    const value = Math.max(Number(idMatch?.[1] || 0), Number(nameMatch?.[1] || 0));
    if (value >= next) next = value + 1;
  });
  const serial = String(next).padStart(2, "0");
  return { id: `cover_template_${serial}`, name: `绗竴灞忓皝闈㈡ā鏉?${serial}` };
}

async function refreshMainPreview() {
  const template = coverTemplates[selectedCover];
  if (!template) {
    clearImageLoading("coverPreview");
    return;
  }
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
  setImageLoading("coverPreview", "鍒囨崲绗竴灞忔ā鏉?..");
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
  const label = button?.textContent || "鏂板缓淇濆瓨";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("鏂板缓涓?..");
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
    log(`宸叉柊寤虹嫭绔嬪皝闈㈡ā鏉匡細${nextMeta.name}`);
    showTemplateActionStatus("鏂板缓淇濆瓨鎴愬姛", "coverForm");
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
    log(`绗竴灞忔柊妯℃澘淇濆瓨澶辫触锛?{error.message}`);
  }
}

async function saveCurrentCoverTemplate() {
  const template = coverTemplates[selectedCover];
  if (!template) return;
  const button = $("saveCover");
  const label = button?.textContent || "淇濆瓨";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.innerHTML = buttonLoadingInline("淇濆瓨涓?..");
  }
  try {
    applyIndependentCoverDefaults(template);
    await api(`/api/video-matrix/cover-templates/${selectedCover}`, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(template)});
    await saveTemplateSelection();
    pendingTemplateSave = "";
    log(`宸蹭繚瀛樼涓€灞忔ā鏉匡細${template.name || selectedCover}`);
    renderCoverSelector();
    renderCoverEditor();
    await refreshMainPreview();
    showTemplateActionStatus("淇濆瓨鎴愬姛", "coverForm");
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = label;
    }
    log(`绗竴灞忔ā鏉夸繚瀛樺け璐ワ細${error.message}`);
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
      name: `涔濆鏍煎浘鐗囨ā鏉?${serial}`,
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
    log(`姝ｆ枃妯℃澘鑷姩淇濆瓨澶辫触锛?{error.message}`);
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
    log(`绗竴灞忔ā鏉胯嚜鍔ㄤ繚瀛樺け璐ワ細${error.message}`);
  }
}, 700);

async function saveTemplateSelection() {
  state = collectState();
  await api("/api/video-matrix/state", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(state)});
}

async function saveState() {
  state = collectState();
  await api("/api/video-matrix/state", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(state)});
  log("褰撳墠璁剧疆宸蹭繚瀛?);
}

const scheduleStateSave = debounce(async () => {
  state = collectState();
  await api("/api/video-matrix/state", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(state)});
}, 500);

async function resolvePreviewVideoPath() {
  if (lastPreviewPath) return lastPreviewPath;
  const root = outputRootPath();
  if (!root) return "";
  try {
    const payload = await api(`/api/video-matrix/preview-files?path=${encodeURIComponent(root)}`);
    const firstVideo = Array.isArray(payload.videos) ? payload.videos[0] : null;
    return payload.current || firstVideo?.path || "";
  } catch {
    return "";
  }
}

async function openPreviewVideoPage() {
  const path = await resolvePreviewVideoPath();
  if (!path) {
    log("娌℃湁鎵惧埌鍙瑙堢殑瑙嗛銆傝鍏堢敓鎴愯棰戯紝鎴栫‘璁ゆ渶缁堣棰戠敓鎴愮洰褰曢噷鏈?MP4銆?);
  }
  const url = path
    ? `/static/video_matrix_preview.html?path=${encodeURIComponent(path)}`
    : "/static/video_matrix_preview.html";
  window.open(url, "_blank", "noopener");
}

async function generate() {
  const button = $("generateBtn");
  if (lastPreviewPath && button.dataset.mode === "preview") {
    await openPreviewVideoPage();
    return;
  }
  lastPreviewPath = "";
  button.dataset.mode = "generate";
  try {
    const statePayload = collectState();
    displayedJobPercent = 0;
    showGenerationWaitOverlay(true, { progress: 0, message: "姝ｅ湪鎻愪氦鐢熸垚浠诲姟..." });
    button.disabled = true;
    button.textContent = "鎻愪氦涓?..";
    updateJobStatus({ status: "queued", stage: "queued", progress: 0, message: "姝ｅ湪鎻愪氦鐢熸垚浠诲姟..." });
    if (!bgmLibraryState.local.length) {
      throw new Error("鏈湴鑳屾櫙闊充箰搴撹繕娌℃湁鍙敤 MP3銆傝鎶?MP3 鏂囦欢鏀惧叆宸︿晶闂彿鎻愮ず閲岀殑鐩綍锛岀劧鍚庡埛鏂伴〉闈€?);
    }
    const form = new FormData();
    form.append("payload", JSON.stringify(statePayload));
    [...($("sourceFiles")?.files || [])].forEach((file) => form.append("source_files", file));
    const {job_id} = await api("/api/video-matrix/generate", {method:"POST", body: form});
    updateJobStatus({ status: "queued", stage: "queued", progress: 0.02, message: `浠诲姟宸叉彁浜わ細${job_id}` });
    startJobProgressTicker();
    pollJob(job_id);
  } catch (error) {
    updateJobStatus({ status: "error", stage: "error", progress: 0, message: error.message, error: error.message });
    stopJobProgressTicker();
    showGenerationWaitOverlay(false);
  } finally {
    button.disabled = false;
    if (!lastPreviewPath) button.textContent = "绔嬪嵆鐢熸垚";
  }
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function pollJob(jobId) {
  const job = await api(`/api/video-matrix/jobs/${jobId}`);
  updateJobStatus(job);
  if (job.status === "complete") {
    stopJobProgressTicker();
    lastPreviewPath = job.assets?.[0]?.video_path || "";
    const dedupeSummary = summarizeDedupeStatuses(job.assets || []);
    const dedupeText = dedupeSummary ? ` 鍘婚噸锛?{dedupeSummary}銆俙 : " ";
    updateJobStatus({...job, message: `鐢熸垚瀹屾垚锛屽凡瀵煎嚭 ${job.assets.length} 鏉¤棰戙€?{dedupeText}鐐瑰嚮鎸夐挳鍙瑙堢涓€鏉¤棰戝湪瑙嗛鍙烽噷鐨勫睍绀烘晥鏋溿€俙});
    showGenerationWaitOverlay(false);
    const button = $("generateBtn");
    if (lastPreviewPath) {
      button.dataset.mode = "preview";
      button.textContent = "棰勮瑙嗛";
    }
  } else if ((job.status === "running" || job.status === "queued") && ((job.assets && job.assets.length > 0) || job.first_asset_ready)) {
    const first = job.assets?.[0]?.video_path || "";
    if (first) {
      lastPreviewPath = first;
      const button = $("generateBtn");
      button.dataset.mode = "preview";
      button.textContent = "棰勮瑙嗛";
    }
    setTimeout(() => pollJob(jobId), 1200);
  } else if (job.status === "error") {
    stopJobProgressTicker();
    showGenerationWaitOverlay(false);
  } else setTimeout(() => pollJob(jobId), 1200);
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
    setPreflightStepStatus(index, "checking", "妫€鏌ヤ腑...");
    await wait(80);
    let result;
    try {
      result = await checks[index].run(index);
    } catch (error) {
      result = { status: "fail", detail: error.message || "棰勬鎵ц澶辫触" };
    }
    const status = result?.status || "pass";
    if (status === "fail") hasFail = true;
    setPreflightStepStatus(index, status, result?.detail || checks[index].readyText);
    revealNextPreflightStep(index + 1);
    await wait(50);
  }

  actions.classList.remove("is-running");
  if (!hasFail) {
    setPreflightSummary("棰勬閫氳繃", "褰撳墠淇濆瓨鏉′欢鏈彂鐜颁細闃绘柇鎻愪氦鐨勯棶棰橈紝鐐瑰嚮缁х画杩涘叆鏈€缁堢‘璁ゃ€?);
    $("preflightContinue").hidden = false;
    $("preflightCancel").hidden = false;
    $("preflightCancel").textContent = "杩斿洖淇敼";
    return new Promise((resolve) => {
      const cleanup = () => {
        $("preflightContinue").onclick = null;
        $("preflightCancel").onclick = null;
        $("preflightClose").onclick = null;
      };
      $("preflightContinue").onclick = () => {
        cleanup();
        closePreflightModal();
        resolve(true);
      };
      $("preflightCancel").onclick = () => {
        cleanup();
        closePreflightModal();
        resolve(false);
      };
      $("preflightClose").onclick = () => {
        cleanup();
        closePreflightModal();
        resolve(false);
      };
    });
  }

  setPreflightSummary("棰勬鏈€氳繃", "璇锋寜绾㈣壊鑺傜偣鎻愮ず淇鍚庡啀鐐瑰嚮绔嬪嵆鐢熸垚銆?);
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
  const formats = Array.isArray(statePayload.output_options) ? statePayload.output_options.filter(Boolean) : [];
  const categorySummary = activeIds.length
    ? activeIds.map((id) => `${categoryNames[id] || id}(${statePayload.recent_limits?.[id] || 0})`).join(" / ")
    : "鏈惎鐢ㄥ垎绫?;
  const compositionSeconds = composition.reduce((sum, row) => sum + Number(row.duration || 0), 0);
  const compositionSummary = composition.length
    ? composition.map((row, index) => `${index + 1}.${categoryNames[row.category_id] || row.category_id} ${Number(row.duration || 0).toFixed(1)}s`).join(" / ")
    : "鏈厤缃粨鏋?;
  const endingSummary = statePayload.ending_template_mode === "random"
    ? `瑙嗛鐗囧熬 / 宸查€?${selectedEndingNames.length || "鐩綍鍏ㄩ儴"} / 鐩綍 ${shortPath(statePayload.ending_template_dir || "")}`
    : `鏂囧瓧鐗囧熬 / ${statePayload.ending_cover_template?.name || "鏈懡鍚嶆ā鏉?}`;
  return [
    {
      title: "杩炴帴鐢熸垚鎺ュ彛",
      pendingText: "璇诲彇鏈€鏂扮礌鏉愩€佹ā鏉裤€丅GM 鍜岀墖灏剧洰褰曠姸鎬併€?,
      readyText: "鎺ュ彛鍙敤锛屽凡璇诲彇鏈€鏂扮姸鎬併€?,
      configText: "鎺ュ彛 /api/video-matrix/state锛涚敤浜庡埛鏂扮礌鏉愭暟閲忋€佹ā鏉垮垪琛ㄣ€丅GM 鏇插簱鍜岀墖灏剧洰褰曘€?,
      run: async (index) => {
        await animatePreflightProgress(index, 0, "寮€濮嬭繛鎺ユ帴鍙?..");
        const data = await api("/api/video-matrix/state");
        setLiveData(data);
        await animatePreflightProgress(index, 100, "鎺ュ彛鐘舵€佽鍙栧畬鎴愩€?);
        return { status: "pass", detail: "鎺ュ彛鍙敤锛屽凡璇诲彇鏈€鏂扮姸鎬併€? };
      },
    },
    {
      title: "杈撳嚭鍙傛暟",
      pendingText: "妫€鏌ユ暟閲忋€佸苟琛屻€佸抚鐜囥€佽妭鎷嶅垎鏋愭椂闀垮拰杈撳嚭鐩綍銆?,
      readyText: "杈撳嚭鍙傛暟瀹屾暣銆?,
      configText: `鏁伴噺 ${statePayload.output_count} / 骞惰 ${statePayload.max_workers} / 甯х巼 ${statePayload.target_fps}fps / 閫熷害 ${statePayload.render_speed_mode || "quality"} / 鑺傛媿 ${statePayload.video_duration_min}-${statePayload.video_duration_max}s / 杈撳嚭 ${formats.join(", ") || "鏈€夋嫨"} / 鐩綍 ${shortPath(statePayload.output_root || "")}`,
      run: async (index) => {
        await animatePreflightProgress(index, 15, "妫€鏌ョ敓鎴愭暟閲忓拰骞惰绾跨▼...");
        const formats = Array.isArray(statePayload.output_options) ? statePayload.output_options.filter(Boolean) : [];
        if (!Number.isFinite(statePayload.output_count) || statePayload.output_count < 1) return { status: "fail", detail: "鐢熸垚鏁伴噺蹇呴』澶т簬 0銆? };
        if (!Number.isFinite(statePayload.max_workers) || statePayload.max_workers < 1) return { status: "fail", detail: "骞惰绾跨▼蹇呴』澶т簬 0銆? };
        await animatePreflightProgress(index, 45, "妫€鏌ョ洰鏍囧抚鐜囧拰杈撳嚭鏍煎紡...");
        if (![30, 60].includes(Number(statePayload.target_fps))) return { status: "fail", detail: "鐩爣甯х巼鍙兘鏄?30 鎴?60銆? };
        if (!formats.length) return { status: "fail", detail: "鑷冲皯闇€瑕侀€夋嫨涓€绉嶈緭鍑烘牸寮忋€? };
        await animatePreflightProgress(index, 75, "妫€鏌ヨ緭鍑虹洰褰曞拰鑺傛媿鏃堕暱...");
        if (!String(statePayload.output_root || "").trim()) return { status: "fail", detail: "鏈€缁堣棰戠敓鎴愮洰褰曚笉鑳戒负绌恒€? };
        if (Number(statePayload.video_duration_min) > Number(statePayload.video_duration_max)) return { status: "fail", detail: "鏈€灏忚妭鎷嶅垎鏋愭椂闀夸笉鑳藉ぇ浜庢渶澶ц妭鎷嶅垎鏋愭椂闀裤€? };
        await animatePreflightProgress(index, 100, "杈撳嚭鍙傛暟妫€鏌ュ畬鎴愩€?);
        return { status: "pass", detail: `${statePayload.output_count} 鏉?/ ${statePayload.max_workers} 绾跨▼ / ${statePayload.target_fps}fps / ${formats.join(", ")}` };
      },
    },
    {
      title: "妯℃澘鍙敤鎬?,
      pendingText: "妫€鏌ョ涓€灞忓皝闈㈡ā鏉垮拰姝ｆ枃鍙犲眰妯℃澘鏄惁瀛樺湪銆?,
      readyText: "妯℃澘閰嶇疆鍙敤銆?,
      configText: `姝ｆ枃鍙犲眰 ${statePayload.template_id || "鏈€夋嫨"} / 绗竴灞忓皝闈?${statePayload.cover_template_id || "鏈€夋嫨"}`,
      run: async (index) => {
        await animatePreflightProgress(index, 20, "妫€鏌ユ鏂囧彔灞傛ā鏉?..");
        const live = getLiveData() || {};
        const liveVideoTemplates = live.templates || templates || {};
        const liveCoverTemplates = live.cover_templates || coverTemplates || {};
        if (!liveVideoTemplates[statePayload.template_id]) return { status: "fail", detail: `姝ｆ枃鍙犲眰妯℃澘涓嶅瓨鍦細${statePayload.template_id || "鏈€夋嫨"}` };
        await animatePreflightProgress(index, 55, "妫€鏌ョ涓€灞忓皝闈㈡ā鏉?..");
        if (!liveCoverTemplates[statePayload.cover_template_id]) return { status: "fail", detail: `绗竴灞忓皝闈㈡ā鏉夸笉瀛樺湪锛?{statePayload.cover_template_id || "鏈€夋嫨"}` };
        await animatePreflightProgress(index, 100, "妯℃澘妫€鏌ュ畬鎴愩€?);
        return { status: "pass", detail: `姝ｆ枃 ${statePayload.template_id} / 灏侀潰 ${statePayload.cover_template_id}` };
      },
    },
    {
      title: "鏈湴 BGM",
      pendingText: "妫€鏌ユ湰鍦拌儗鏅煶涔愬簱鏄惁鏈夊彲鐢?MP3銆?,
      readyText: "BGM 鍙敤銆?,
      configText: `鏉ユ簮 Local library / 宸查€?${statePayload.bgm_library_id || "鏈寚瀹氾紝鐢熸垚鏃堕殢鏈?} / 鏇插簱鐩綍 ${shortPath(bgmLibraryState.directory || "")}`,
      run: async (index) => {
        await animatePreflightProgress(index, 25, "璇诲彇鏈湴鏇插簱...");
        const live = getLiveData() || {};
        const localBgm = Array.isArray(live.local_bgm) ? live.local_bgm : bgmLibraryState.local;
        if (!localBgm.length) return { status: "fail", detail: "鏈湴鑳屾櫙闊充箰搴撴病鏈夊彲鐢?MP3銆? };
        await animatePreflightProgress(index, 70, "鏍稿宸查€?BGM...");
        if (statePayload.bgm_library_id && !localBgm.includes(statePayload.bgm_library_id)) return { status: "fail", detail: `宸查€?BGM 涓嶅湪鏈湴鏇插簱锛?{statePayload.bgm_library_id}` };
        await animatePreflightProgress(index, 100, "BGM 妫€鏌ュ畬鎴愩€?);
        return { status: "pass", detail: statePayload.bgm_library_id ? `宸查€?${statePayload.bgm_library_id}` : `鏈湴鏇插簱 ${localBgm.length} 棣栵紝鐢熸垚鏃堕殢鏈哄彇 1 棣栥€俙 };
      },
    },
    {
      title: "鍒嗙被绱犳潗",
      pendingText: "閫愪釜绫荤洰妫€鏌ュ彲鐢ㄧ礌鏉愭暟閲忋€?,
      readyText: "鍚敤鍒嗙被閮芥湁绱犳潗銆?,
      configText: `鍚敤 ${activeIds.length} 绫?/ 鏈€杩戠礌鏉愪笂闄愶細${categorySummary}`,
      run: async (index) => {
        const live = getLiveData() || {};
        const counts = live.category_counts || {};
        if (!activeIds.length) return { status: "fail", detail: "鑷冲皯闇€瑕佸惎鐢ㄤ竴涓礌鏉愬垎绫汇€? };
        const empty = [];
        for (let categoryIndex = 0; categoryIndex < activeIds.length; categoryIndex += 1) {
          const id = activeIds[categoryIndex];
          const percent = Math.round((categoryIndex / activeIds.length) * 100);
          const label = categoryNames[id] || id;
          await animatePreflightProgress(index, percent, `妫€绱?${label}锛?{Number(counts[id] || 0)} 涓礌鏉恅);
          await wait(80);
          if (Number(counts[id] || 0) < 1) empty.push(id);
        }
        await animatePreflightProgress(index, 100, "鍒嗙被绱犳潗閫愰」妫€绱㈠畬鎴愩€?);
        if (empty.length) return { status: "fail", detail: `杩欎簺鍒嗙被娌℃湁绱犳潗锛?{empty.map((id) => categoryNames[id] || id).join("銆?)}` };
        const total = activeIds.reduce((sum, id) => sum + Number(counts[id] || 0), 0);
        return { status: "pass", detail: `${activeIds.length} 涓垎绫诲彲鐢紝鍏?${total} 涓礌鏉愩€俙 };
      },
    },
    {
      title: "鐢熸垚缁撴瀯",
      pendingText: "妫€鏌ョ墖娈靛垎绫诲拰绠楁硶鏃堕暱閰嶇疆銆?,
      readyText: "鐢熸垚缁撴瀯鍙敤銆?,
      configText: `缁撴瀯 ${composition.length} 娈?/ 鍚堣绾?${compositionSeconds.toFixed(1)}s / ${compositionSummary}`,
      run: async (index) => {
        const live = getLiveData() || {};
        const counts = live.category_counts || {};
        if (!composition.length) return { status: "fail", detail: "鐢熸垚缁撴瀯涓嶈兘涓虹┖銆? };
        await animatePreflightProgress(index, 12, "妫€鏌ョ畻娉曠墖娈垫椂闀?..");
        const invalidDuration = composition.find((row) => !Number.isFinite(Number(row.duration)) || Number(row.duration) <= 0);
        if (invalidDuration) return { status: "fail", detail: "鐢熸垚缁撴瀯閲屽瓨鍦ㄦ棤鏁堢畻娉曠墖娈垫椂闀裤€? };
        for (let rowIndex = 0; rowIndex < composition.length; rowIndex += 1) {
          const row = composition[rowIndex];
          const percent = 18 + Math.round(((rowIndex + 1) / composition.length) * 58);
          await animatePreflightProgress(index, percent, `鏍稿缁撴瀯绗?${rowIndex + 1} 娈碉細${categoryNames[row.category_id] || row.category_id}`);
          await wait(45);
        }
        const missing = composition
          .map((row) => row.category_id)
          .filter((id) => !activeIds.includes(id) || Number(counts[id] || 0) < 1);
        if (missing.length) return { status: "fail", detail: `缁撴瀯寮曠敤浜嗘湭鍚敤鎴栨棤绱犳潗鍒嗙被锛?{[...new Set(missing)].map((id) => categoryNames[id] || id).join("銆?)}` };
        await animatePreflightProgress(index, 100, "鐢熸垚缁撴瀯妫€鏌ュ畬鎴愩€?);
        const seconds = composition.reduce((sum, row) => sum + Number(row.duration || 0), 0);
        const maxDuration = Number(statePayload.video_duration_max || 0);
        const status = seconds > maxDuration ? "warn" : "pass";
        const detail = status === "warn"
          ? `缁撴瀯 ${composition.length} 娈碉紝鍚堣绾?${seconds.toFixed(1)} 绉掞紝瓒呰繃鏈€澶ц妭鎷嶅垎鏋?${maxDuration.toFixed(1)} 绉掋€備紭鍖栧缓璁細鎶婃渶澶ц妭鎷嶅垎鏋愭椂闀胯皟鍒颁笉浣庝簬 ${Math.ceil(seconds)} 绉掞紝鎴栧噺灏戠敓鎴愮粨鏋勭墖娈电鏁帮紱涓嶈皟鏁翠篃鑳界户缁紝绯荤粺浼氭寜缁撴瀯鎬绘椂闀垮厹搴曘€俙
          : `缁撴瀯 ${composition.length} 娈碉紝鍚堣绾?${seconds.toFixed(1)} 绉掋€俙;
        return { status, detail };
      },
    },
    {
      title: "鐗囧熬閰嶇疆",
      pendingText: "妫€鏌ユ枃瀛楃墖灏炬ā鏉挎垨瑙嗛鐗囧熬绱犳潗鏄惁鍙鍙栥€?,
      readyText: "鐗囧熬閰嶇疆鍙敤銆?,
      configText: endingSummary,
      run: async (index) => {
        const mode = statePayload.ending_template_mode || "dynamic";
        await animatePreflightProgress(index, 20, "璇嗗埆鐗囧熬妯″紡...");
        if (mode === "dynamic") {
          if (!statePayload.ending_cover_template) return { status: "fail", detail: "鏂囧瓧鐗囧熬缂哄皯鐗囧熬灏侀潰妯℃澘閰嶇疆銆? };
          await animatePreflightProgress(index, 100, "鏂囧瓧鐗囧熬妯℃澘妫€鏌ュ畬鎴愩€?);
          return { status: "pass", detail: "浣跨敤鏂囧瓧鐗囧熬妯℃澘銆? };
        }
        const live = getLiveData() || {};
        const endingItems = Array.isArray(live.ending_templates) ? live.ending_templates : endingTemplateState.local;
        const endingNames = new Set(endingItems.map((item) => item.name));
        if (!endingItems.length) return { status: "fail", detail: "瑙嗛鐗囧熬鐩綍娌℃湁鍙敤绱犳潗銆? };
        await animatePreflightProgress(index, 55, "鏍稿宸查€夎棰戠墖灏?..");
        if (selectedEndingNames.length) {
          const missing = selectedEndingNames.filter((name) => !endingNames.has(name));
          if (missing.length) return { status: "fail", detail: `宸查€夎棰戠墖灏句笉瀛樺湪锛?{missing.join("銆?)}` };
          await animatePreflightProgress(index, 100, "宸查€夎棰戠墖灏炬鏌ュ畬鎴愩€?);
          return { status: "pass", detail: `闅忔満鑼冨洿 ${selectedEndingNames.length} 涓凡閫夎棰戠墖灏俱€俙 };
        }
        await animatePreflightProgress(index, 100, "瑙嗛鐗囧熬鐩綍妫€鏌ュ畬鎴愩€?);
        return { status: "pass", detail: `鏈寚瀹氳棰戠墖灏撅紝闅忔満鑼冨洿涓虹洰褰曞唴 ${endingItems.length} 涓礌鏉愩€俙 };
      },
    },
    {
      title: "鐢熸垚鏂囨",
      pendingText: "妫€鏌ュ瓧骞曡儗鏉垮拰鐗囧熬鏂囨銆?,
      readyText: "鏂囨瀛楁鍙敤銆?,
      configText: `瀛楀箷鑳屾澘 ${shortText(statePayload.hud_text, 32) || "绌?} / 鐗囧熬 ${shortText(statePayload.follow_text, 32) || "绌?} / 涓婃爣棰?${statePayload.headline_ai_enabled ? "AI 鎵归噺鐢熸垚" : "鍥哄畾鏂囨"}`,
      run: async (index) => {
        await animatePreflightProgress(index, 55, "妫€鏌ュ瓧骞曡儗鏉垮拰鐗囧熬鏂囨...");
        const emptyFields = [
          ["瀛楀箷鑳屾澘鏂囨湰", statePayload.hud_text],
          ["鐗囧熬鏂囨", statePayload.follow_text],
        ].filter(([, value]) => !String(value || "").trim()).map(([label]) => label);
        await animatePreflightProgress(index, 100, "鏂囨瀛楁妫€鏌ュ畬鎴愩€?);
        if (emptyFields.length) return { status: "warn", detail: `${emptyFields.join("銆?)}涓虹┖锛屼粛鍙敓鎴愪絾鐢婚潰鏂囨浼氬彉灏戙€俙 };
        return { status: "pass", detail: statePayload.headline_ai_enabled ? "瀛楀箷鑳屾澘銆佺墖灏炬枃妗堝彲鐢紱涓婃爣棰樺皢鎸夌敓鎴愭暟閲忕敱 AI 鎵归噺鐢熸垚銆? : "瀛楀箷鑳屾澘銆佺墖灏炬枃妗堝彲鐢紱涓婃爣棰樹娇鐢ㄥ浐瀹氭枃妗堛€? };
      },
    },
    {
      title: "鎻愪氦瀹屾暣鎬?,
      pendingText: "妫€鏌ュ嵆灏嗘彁浜ょ粰鐢熸垚鎺ュ彛鐨勬牳蹇冨瓧娈点€?,
      readyText: "鎻愪氦杞借嵎瀹屾暣銆?,
      configText: `鏍稿績瀛楁锛氭ā鏉裤€佸皝闈€丅GM銆佺墖灏俱€佸垎绫汇€佺敓鎴愮粨鏋勩€佽緭鍑虹洰褰曪紱鎻愪氦鏍煎紡 FormData + payload JSON`,
      run: async (index) => {
        await animatePreflightProgress(index, 25, "鏍稿鏍稿績瀛楁...");
        const required = ["template_id", "cover_template_id", "output_root", "composition_sequence", "active_category_ids"];
        const missing = required.filter((key) => {
          const value = statePayload[key];
          return Array.isArray(value) ? !value.length : !value;
        });
        await animatePreflightProgress(index, 70, "鏍稿鐗囧熬鍜?BGM 瀛楁...");
        if (statePayload.ending_template_mode === "random" && !statePayload.ending_template_dir) missing.push("ending_template_dir");
        if (statePayload.bgm_source !== "Local library") missing.push("bgm_source");
        await animatePreflightProgress(index, 100, "鎻愪氦杞借嵎妫€鏌ュ畬鎴愩€?);
        if (missing.length) return { status: "fail", detail: `鎻愪氦瀛楁涓嶅畬鏁达細${missing.join("銆?)}` };
        return { status: "pass", detail: "鏍稿績瀛楁瀹屾暣锛屽彲浠ヨ繘鍏ユ渶缁堢‘璁ゃ€? };
      },
    },
  ];
}

function preflightChecksHtml(checks) {
  return `
    <div class="preflight-summary">
      <strong data-preflight-summary-title>姝ｅ湪棰勬</strong>
      <span data-preflight-summary-detail>閫愰」纭褰撳墠鐢熸垚鏉′欢锛屽け璐ラ」浼氶樆姝㈡彁浜ゃ€?/span>
    </div>
    <ol class="preflight-list">
      ${checks.map((check, index) => `
        <li class="preflight-step" data-preflight-step="${index}">
          <span class="preflight-status" data-preflight-status>路</span>
          <div>
            <strong>${escapeHtml(check.title)}</strong>
            <div class="preflight-config">${escapeHtml(check.configText || "浣跨敤褰撳墠椤甸潰宸蹭繚瀛橀厤缃€?)}</div>
            <small data-preflight-detail>${escapeHtml(check.pendingText || "")}</small>
            <div class="preflight-progress-wrap">
              <div class="preflight-progress" aria-hidden="true"><div data-preflight-progress></div></div>
              <span data-preflight-percent>0%</span>
            </div>
          </div>
          <span class="preflight-badge" data-preflight-badge>绛夊緟</span>
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
  const icons = { checking: "...", pass: "鉁?, warn: "!", fail: "脳" };
  const labels = { checking: "妫€鏌ヤ腑", pass: "閫氳繃", warn: "鎻愰啋", fail: "澶辫触" };
  if (icon) icon.textContent = icons[status] || "路";
  if (badge) badge.textContent = labels[status] || "绛夊緟";
  if (detailNode) detailNode.textContent = detail || "";
  if (status === "checking") setPreflightProgress(index, 0);
  if (status === "pass" || status === "warn" || status === "fail") setPreflightProgress(index, 100);
  if (status === "checking") revealNextPreflightStep(index);
}

function revealNextPreflightStep(index) {
  const node = document.querySelector(`[data-preflight-step="${index}"]`);
  if (!node) return;
  node.scrollIntoView({ block: "start", behavior: "smooth" });
}

function setPreflightProgress(index, percent) {
  const node = document.querySelector(`[data-preflight-step="${index}"]`);
  const bar = node?.querySelector("[data-preflight-progress]");
  const percentNode = node?.querySelector("[data-preflight-percent]");
  const value = Math.round(clamp(Number(percent) || 0, 0, 100));
  if (bar) bar.style.width = `${value}%`;
  if (percentNode) percentNode.textContent = `${value}%`;
}

async function animatePreflightProgress(index, percent, detail = "") {
  setPreflightProgress(index, percent);
  if (detail) {
    const node = document.querySelector(`[data-preflight-step="${index}"]`);
    const detailNode = node?.querySelector("[data-preflight-detail]");
    if (detailNode) detailNode.textContent = `${Math.round(clamp(Number(percent) || 0, 0, 100))}% - ${detail}`;
  }
  await wait(35);
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
  const compositionRows = (statePayload.composition_sequence || []).map((row, index) =>
    `<tr><td>${index + 1}</td><td>${escapeHtml(categoryNames[row.category_id] || row.category_id)}</td><td>${escapeHtml(row.category_id)}</td><td>${Number(row.duration || 0).toFixed(1)} 绉掞紙鍙傝€冿級</td></tr>`
  ).join("") || `<tr><td colspan="4">鏈厤缃敓鎴愮粨鏋?/td></tr>`;
  return `
    <div class="confirm-summary">
      <div><span>鐢熸垚鏁伴噺</span><strong>${statePayload.output_count}</strong></div>
      <div><span>骞惰绾跨▼</span><strong>${statePayload.max_workers}</strong></div>
      <div><span>鏈€灏忚妭鎷嶅垎鏋?/span><strong>${statePayload.video_duration_min} 绉?/strong></div>
      <div><span>鏈€澶ц妭鎷嶅垎鏋?/span><strong>${statePayload.video_duration_max} 绉?/strong></div>
      <div><span>鐩爣甯х巼</span><strong>${statePayload.target_fps} fps</strong></div>
      <div><span>鐢熸垚閫熷害</span><strong>${escapeHtml(statePayload.render_speed_mode || "quality")}</strong></div>
      <div><span>杈撳嚭鏍煎紡</span><strong>${escapeHtml((statePayload.output_options || []).join(", "))}</strong></div>
    </div>
    <section>
      <h4>杈撳嚭鐩綍</h4>
      <code>${escapeHtml(statePayload.output_root)}</code>
    </section>
    <section>
      <h4>鐢熸垚缁撴瀯</h4>
      <table><thead><tr><th>#</th><th>鍒嗙被</th><th>ID</th><th>鍩虹璁″垝鏃堕暱</th></tr></thead><tbody>${compositionRows}</tbody></table>
      <small>鎻愮ず锛氭渶缁堢墖娈垫椂闀跨敱鍔ㄦ€佺畻娉曟寜鑺傛媿鍜岀礌鏉愭湁鏁堝尯闂村疄鏃惰皟鏁达紝杩欓噷浠呭睍绀哄熀纭€璁″垝鍊笺€?/small>
    </section>
    <section class="confirm-algorithm">
      <h4>鏈绠楁硶妗嗘灦</h4>
      <ol>
        <li>鎸夊惎鐢ㄥ垎绫昏鍙栫礌鏉愮洰褰曪紝姣忕被鏈€澶氬彇鈥滄渶杩戠礌鏉愨€濇暟閲忓搴旂殑鏂版枃浠躲€?/li>
        <li>灏嗗€欓€夌礌鏉愬綊涓€鍖栦负 1080:1920銆?{statePayload.target_fps}fps 鐨勭煭瑙嗛鐗囨搴撱€?/li>
        <li>鎸夊彊浜嬮鏋躲€佸垎绫婚『搴忓拰鐗囨绉掓暟锛屼负姣忔潯瑙嗛鎶藉彇涓嶅悓绱犳潗鐗囨銆?/li>
        <li>鍒嗘瀽鏈湴鑳屾櫙闊充箰鑺傛媿锛屾妸鐗囨鍒囨崲鐐瑰敖閲忓榻愯妭濂忕獥鍙ｃ€?/li>
        <li>鎸夊綋鍓嶆ā鏉裤€佸瓧骞曡儗鏉挎枃鏈拰鐗囧熬鏂囨骞惰娓叉煋锛屽鍑哄埌鏈€缁堣棰戠洰褰曘€?/li>
      </ol>
    </section>
  `;
}

function collectState() {
  const categories = Array.isArray(settings.material_categories) ? settings.material_categories : [];
  updateCompositionState();
  const endingCopyText = endingCopyTextValue();
  const endingCoverTemplate = activeEndingCoverTemplateSnapshot(endingCopyText);
  const aiPromptHint = sanitizeAiPromptHint($("aiPromptHint")?.value || state.ai_prompt_hint || "");
  state.ai_prompt_hint = aiPromptHint;
  return {
    output_count: Number($("outputCount").value), max_workers: Number($("maxWorkers").value),
    video_duration_min: Number($("videoDurationMin").value || settings.video_duration_min || 9),
    video_duration_max: Number($("videoDurationMax").value || settings.video_duration_max || 15),
    target_fps: Number(radioValue("target_fps") || settings.target_fps || 30),
    render_speed_mode: String(radioValue("render_speed_mode") || state.render_speed_mode || "quality"),
    output_options: [$("outputOptions").value], output_root: outputRootPath(),
    template_id: selectedVideoTemplate, cover_template_id: selectedCover, copy_language: state.copy_language || settings.copy_language || "zh",
    template_config: activeVideoTemplateSnapshot(),
    cover_template_config: activeCoverTemplateSnapshot(),
    source_mode: "Category folders",
    headline_ai_enabled: Boolean($("headlineAiEnabled")?.checked),
    ai_prompt_hint: aiPromptHint,
    headline: $("headline").value, subhead: $("subhead").value,
    follow_text: endingCopyText, hud_text: $("hudText").value,
    ending_template_mode: endingTemplateMode(),
    ending_template_id: endingTemplateSelectedName(),
    ending_template_ids: selectedEndingTemplateNames(),
    ending_template_dir: endingTemplateState.directory,
    ending_cover_template_id: state.ending_cover_template_id,
    ending_cover_templates: state.ending_cover_templates,
    ending_cover_template: endingCoverTemplate,
    bgm_source: "Local library", bgm_library_id: selectedBgmLibraryId(),
    mining_bgm_volume: Number($("miningBgmVolume")?.value || state.mining_bgm_volume || 1),
    library_bgm_volume: Number($("libraryBgmVolume")?.value || state.library_bgm_volume || 0.35),
    composition_sequence: state.composition_sequence,
    composition_customized: Boolean(state.composition_customized),
    active_category_ids: selectedActiveCategoryIds(categories),
    recent_limits: Object.fromEntries(categories.map((category) => [
      category.id,
      clamp(Number(state.recent_limits?.[category.id] || settings.recent_limits?.[category.id] || 8), 1, 10),
    ]))
  };
}

function activeVideoTemplateSnapshot() {
  const template = templates[selectedVideoTemplate];
  return template ? JSON.parse(JSON.stringify(template)) : {};
}

function activeCoverTemplateSnapshot() {
  const template = coverTemplates[selectedCover];
  return template ? JSON.parse(JSON.stringify(template)) : {};
}

function activeEndingCoverTemplateSnapshot(endingCopyText = "") {
  if (endingTemplateMode() !== "dynamic") {
    return state.ending_cover_template ? JSON.parse(JSON.stringify(state.ending_cover_template)) : {};
  }
  const template = endingCoverTemplate();
  const snapshot = template ? JSON.parse(JSON.stringify(template)) : {};
  applyIndependentCoverDefaults(snapshot);
  snapshot.cover_layout = "single_video";
  if (endingCopyText) snapshot.single_cover_title_text = endingCopyText;
  state.ending_cover_template = JSON.parse(JSON.stringify(snapshot));
  if (state.ending_cover_template_id && state.ending_cover_templates?.[state.ending_cover_template_id]) {
    state.ending_cover_templates[state.ending_cover_template_id] = JSON.parse(JSON.stringify(snapshot));
  }
  return snapshot;
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
    { id: "category_A", label: "A 绫? },
    { id: "category_B", label: "B 绫? },
    { id: "category_C", label: "C 绫? },
  ];
}

function narrativeTemplates() {
  return (Array.isArray(settings.narrative_templates) ? settings.narrative_templates : [])
    .filter((template) => template && typeof template === "object" && String(template.id || "").trim())
    .map((template) => ({
      id: String(template.id || "").trim(),
      name: String(template.name || template.id || "").trim(),
      account_pool_id: String(template.account_pool_id || template.id || "").trim(),
      composition_sequence: narrativeTemplateSequence(template),
    }));
}

function narrativeTemplateSequence(template) {
  return (Array.isArray(template?.composition_sequence) ? template.composition_sequence : [])
    .map((row) => ({
      category_id: String(row.category_id || "").trim(),
      duration: Number(row.duration || 0),
    }))
    .filter((row) => row.category_id && row.duration > 0);
}

function narrativeTemplateDisplayName(template) {
  const id = typeof template === "object" ? String(template.id || "").trim() : String(template || "").trim();
  const rawName = typeof template === "object" ? String(template.name || "").trim() : "";
  return narrativeTemplateNameMap[id] || rawName || id || "榛樿缁撴瀯";
}

function narrativeAccountPoolDisplayName(poolId) {
  const id = String(poolId || "").trim();
  return narrativeAccountPoolNameMap[id] || narrativeTemplateNameMap[id] || id || "榛樿璐﹀彿姹?;
}

function narrativeTemplateLabel(templateId) {
  const id = String(templateId || "").trim();
  if (!id) return "榛樿缁撴瀯";
  const template = narrativeTemplates().find((item) => item.id === id);
  return template ? narrativeTemplateDisplayName(template) : narrativeTemplateDisplayName(id);
}

function shortPath(value) {
  const parts = String(value).split(/[\\/]+/).filter(Boolean);
  return parts.slice(-2).join("\\") || value;
}

function shortText(value, maxLength = 36) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text.length > maxLength ? `${text.slice(0, maxLength - 1)}鈥 : text;
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
  };
  const bgmPanel = $("bgmPanel");
  if (!bgmPanel) return;
  bgmPanel.innerHTML = `
    <div class="bgm-label-row">
      <strong>鏈湴鑳屾櫙闊充箰</strong>
      <button class="help-dot" type="button" aria-label="鑳屾櫙闊充箰鐩綍" title="鎶?MP3 鏂囦欢鏀惧埌锛?{escapeHtml(localBgmDir)}">?</button>
    </div>
    <p id="bgmLibraryHint" class="bgm-library-hint"></p>
    <div class="links bgm-links"></div>`;
  $("bgmLibraryHint").textContent = localBgm.length
    ? (state.bgm_library_id
      ? `宸查€変腑鍞竴鑳屾櫙闊充箰锛?{state.bgm_library_id}`
      : `宸叉壘鍒?${localBgm.length} 棣栨湰鍦伴煶棰戯紝鏈€変腑鏃剁敓鎴愪細闅忔満鍙?1 棣栵細${localBgmDir}`)
    : `璇锋妸 MP3 鏂囦欢鏀惧叆锛?{localBgmDir}锛岀劧鍚庡埛鏂伴〉闈€俙;
  document.querySelector("#bgmPanel .links").innerHTML = Object.values(data.bgm_library || {}).map(item => `<a href="${item.download_page}" target="_blank" rel="noopener">${item.name}</a>`).join("");
}
function toggleBgmLibraryPopover() {
  const panel = $("bgmLibraryPopover");
  if (!panel) return;
  const isHidden = panel.classList.toggle("hidden");
  panel.classList.toggle("modal", !isHidden);
  document.body.classList.toggle("bgm-modal-open", !isHidden);
  if (isHidden) return;
  const selectedBgm = selectedBgmLibraryId();
  const sortedLocalBgm = selectedBgm
    ? [selectedBgm, ...bgmLibraryState.local.filter((name) => name !== selectedBgm)]
    : bgmLibraryState.local;
  const localList = sortedLocalBgm.length
    ? sortedLocalBgm.map((name) => `
      <li class="bgm-local-item ${name === selectedBgm ? "is-selected" : ""}" data-bgm-name="${escapeHtml(name)}">
        <button type="button" class="bgm-local-select" data-bgm-select="${escapeHtml(name)}" aria-pressed="${name === selectedBgm ? "true" : "false"}" title="璁句负鏈鍞竴鑳屾櫙闊充箰">
          <span class="bgm-select-check" aria-hidden="true"></span>
          <span>${escapeHtml(name)}</span>
        </button>
        <audio controls preload="none" src="/api/video-matrix/bgm/${encodeURIComponent(name)}"></audio>
      </li>`).join("")
    : "<li>鏆傛棤鏈湴 MP3 鏂囦欢</li>";
  panel.innerHTML = `
    <div class="bgm-popover-head">
      <div>
        <strong>鏈湴鏇插簱鍒楄〃</strong>
        <small title="${escapeHtml(bgmLibraryState.directory)}">涓嬭浇鐩綍锛?{escapeHtml(shortPath(bgmLibraryState.directory))}</small>
      </div>
      <button id="toggleBgmLibrarySize" type="button" class="secondary">鏀惰捣</button>
    </div>
    <section class="bgm-local-section">
      <strong>鏈湴鏇插簱</strong>
      <ul>${localList}</ul>
    </section>
  `;
  $("toggleBgmLibrarySize").onclick = toggleBgmLibrarySize;
  panel.querySelectorAll("[data-bgm-select]").forEach((button) => {
    button.onclick = () => selectBgmLibraryId(button.dataset.bgmSelect || "");
  });
  bindExclusiveBgmAudioPlayback(panel);
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
function updateSourceMode() {
  const wrap = $("uploadSourcesWrap");
  if (!wrap) return;
  wrap.classList.add("hidden");
}
function renderRadio(containerId, name, options, selected, onchange) {
  $(containerId).innerHTML = options.map(([value, label]) => `<label><input type="radio" name="${name}" value="${value}" ${value === selected ? "checked" : ""}>${label}</label>`).join("");
  document.querySelectorAll(`input[name="${name}"]`).forEach(r => r.onchange = onchange || (() => {}));
}
function radioValue(name) { return document.querySelector(`input[name="${name}"]:checked`)?.value || ""; }
function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }
function syncNumber(id) { const el = $(id); if (!el) return; el.oninput = () => { let value = Number(el.value || 3); value = Math.max(Number(el.min || 1), Math.min(Number(el.max || 100), value)); if (String(value) !== el.value) el.value = value; if (id === "outputCount") $("metricCount").textContent = el.value; scheduleStateSave(); }; }
function syncRange(id) { bindRangeControl(id, () => { if (id === "outputCount") $("metricCount").textContent = $(id).value; if (id === "maxWorkers") { $("metricWorkers").textContent = $(id).value; $("maxWorkersValue").textContent = $(id).value; } if (id === "miningBgmVolume") $("miningBgmVolumeValue").textContent = Number($(id).value).toFixed(2); if (id === "libraryBgmVolume") $("libraryBgmVolumeValue").textContent = Number($(id).value).toFixed(2); scheduleStateSave(); }); }
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
  lastJobSnapshot = {...(lastJobSnapshot || {}), ...job};
  const stage = job.stage || job.status || "queued";
  const rawPercent = Math.max(0, Math.min(100, Math.round((job.progress || 0) * 100)));
  const percent = job.status === "error" ? rawPercent : Math.max(displayedJobPercent, rawPercent);
  displayedJobPercent = job.status === "complete" ? 100 : percent;
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
  renderDedupeReport(job);
  updateGenerationWaitOverlay(job, percent);
}

function renderDedupeReport(job) {
  const box = $("dedupeReport");
  if (!box) return;
  const assets = Array.isArray(job.assets) ? job.assets : [];
  if (!assets.length) {
    box.classList.add("hidden");
    box.innerHTML = "";
    return;
  }
  const rows = assets
    .map((asset, index) => {
      const dedupe = assetDedupe(asset);
      const report = dedupe.report && typeof dedupe.report === "object" ? dedupe.report : {};
      const reasons = (Array.isArray(report.reasons) && report.reasons.length) ? report.reasons : ["low_cost_dedupe_passed"];
      const status = String(dedupe.status || "pass");
      const retryCount = Number(dedupe.retry_count || 0);
      const displayStatus = dedupeDisplayStatus(status, reasons, report);
      const maxRiskScore = Math.max(
        Number(report.visual_score || 0),
        Number(report.audio_score || 0),
        Number(report.text_score || 0),
        Number(report.structure_score || 0)
      );
      return { asset, index, dedupe, report, reasons, retryCount, displayStatus, maxRiskScore };
    })
    .sort((a, b) => {
      const priorityDiff = dedupePriorityWeight(b.displayStatus) - dedupePriorityWeight(a.displayStatus);
      if (priorityDiff) return priorityDiff;
      const retryDiff = b.retryCount - a.retryCount;
      if (retryDiff) return retryDiff;
      const riskDiff = b.maxRiskScore - a.maxRiskScore;
      if (riskDiff) return riskDiff;
      return a.index - b.index;
    })
    .map(({ asset, index, dedupe, report, reasons, retryCount, displayStatus }) => {
      const narrativeId = String(dedupe.narrative_template_id || asset.narrative_template_id || "").trim();
      const narrativeName = narrativeTemplateLabel(narrativeId);
      const avoidanceSummary = dedupeAvoidanceSummary(reasons, dedupe);
      const scores = [
        ["瑙嗚", report.visual_score],
        ["闊抽", report.audio_score],
        ["鏂囨湰", report.text_score],
        ["缁撴瀯", report.structure_score],
      ].map(([label, value]) => `<span class="dedupe-score ${scoreRiskClass(value)}">${label} ${scorePercent(value)}</span>`).join("");
      const reasonChips = reasons.map((reason) =>
        `<span class="dedupe-reason ${reasonRiskClass(reason)}">${escapeHtml(dedupeReasonLabel(reason))}</span>`
      ).join("");
    return `<article class="dedupe-report-row">
      <div class="dedupe-report-main">
        <span class="dedupe-status ${escapeHtml(displayStatus)}">${escapeHtml(dedupeStatusLabel(displayStatus, retryCount))}</span>
        <strong>${escapeHtml(String(asset.name || asset.video_path || `瑙嗛 ${index + 1}`).split(/[\\/]/).pop())}</strong>
      </div>
      <p class="dedupe-conclusion">${escapeHtml(dedupeConclusion(displayStatus, reasons, retryCount))}</p>
      <div class="dedupe-narrative"><span>楠ㄦ灦</span><b title="${escapeHtml(narrativeId)}">${escapeHtml(narrativeName)}</b></div>
      <div class="dedupe-avoidance"><span>閬块噸鏉ユ簮</span><b>${escapeHtml(avoidanceSummary)}</b></div>
      <div class="dedupe-scores">${scores}</div>
      <div class="dedupe-reasons">${reasonChips}</div>
      <div class="dedupe-suggestion">${escapeHtml(dedupeSuggestion(reasons, report))}</div>
    </article>`;
  }).join("");
  box.innerHTML = `<div class="dedupe-report-head">
    <strong>鍘婚噸鎶ュ憡</strong>
    <span>${escapeHtml(summarizeDedupeStatuses(assets) || "绛夊緟鏇村缁撴灉")}</span>
  </div><div class="dedupe-report-list">${rows}</div>`;
  box.classList.remove("hidden");
}

function assetDedupe(asset) {
  return asset?.dedupe && typeof asset.dedupe === "object" ? asset.dedupe : {};
}

function summarizeDedupeStatuses(assets) {
  const counts = {};
  (Array.isArray(assets) ? assets : []).forEach((asset) => {
    const dedupe = assetDedupe(asset);
    const report = dedupe.report && typeof dedupe.report === "object" ? dedupe.report : {};
    const reasons = (Array.isArray(report.reasons) && report.reasons.length) ? report.reasons : ["low_cost_dedupe_passed"];
    const status = dedupeDisplayStatus(String(dedupe.status || "pass"), reasons, report);
    counts[status] = (counts[status] || 0) + 1;
  });
  return Object.entries(counts)
    .map(([status, count]) => `${dedupeStatusLabel(status)} ${count}`)
    .join("锛?);
}

function dedupeStatusLabel(status, retryCount = 0) {
  if (status === "recut_passed" && retryCount > 0) return `閲嶅壀 ${retryCount} 娆￠€氳繃`;
  return ({
    pass: "閫氳繃",
    suggest_recut: "寤鸿閲嶅壀",
    recut_passed: "閲嶅壀鍚庨€氳繃",
    manual_review: "浜哄伐澶嶆牳",
    retry: "寰呴噸鍓?,
    borderline: "杈圭晫閲嶅",
  })[status] || status || "閫氳繃";
}

function dedupePriorityWeight(status) {
  return ({
    manual_review: 5,
    retry: 4,
    suggest_recut: 3,
    borderline: 2,
    recut_passed: 1,
    pass: 0,
  })[status] ?? 0;
}

function dedupeConclusion(status, reasons, retryCount = 0) {
  if (status === "manual_review") return "闇€澶嶆牳锛氶噸澶嶉闄╄緝楂橈紝璇蜂汉宸ョ‘璁ゃ€?;
  if (reasons.includes("preflight_limited_pool")) return "鍓嶇疆閬块噸宸查檷绾э細鍙敤绱犳潗鎴栨枃鏈€欓€変笉瓒筹紝绯荤粺閫夋嫨浜嗗綋鍓嶆渶浣庨闄╃増鏈€?;
  if (status === "suggest_recut") return "寤鸿閲嶅壀锛氬崟椤圭浉浼煎害鍋忛珮锛岀洿鎺ュ彂甯冨鏄撴挒棣栧睆鎴栨枃妗堛€?;
  if (status === "recut_passed") return retryCount > 0 ? `閫氳繃锛氱郴缁熷凡鑷姩閲嶅壀 ${retryCount} 娆°€俙 : "閫氳繃锛氱郴缁熷凡鑷姩閲嶅壀銆?;
  if (reasons.includes("same_hook_clip")) return "娉ㄦ剰锛欻ook 闀滃ご鐩稿悓锛屽缓璁叧娉ㄩ灞忓樊寮傘€?;
  if (reasons.includes("text_near") || reasons.includes("structure_near")) return "娉ㄦ剰锛氭枃鏈垨缁撴瀯鎺ヨ繎锛屽綋鍓嶄粛鍦ㄩ槇鍊煎唴銆?;
  return "閫氳繃锛氭湭鍙戠幇鏄庢樉閲嶅銆?;
}

function dedupeDisplayStatus(status, reasons, report = {}) {
  if (status !== "pass") return status;
  const visual = Number(report.visual_score || 0);
  const text = Number(report.text_score || 0);
  const structure = Number(report.structure_score || 0);
  const riskyReason = reasons.some((reason) => ["same_hook_clip", "visual_near", "text_near", "structure_near"].includes(reason));
  return riskyReason || visual >= 0.9 || text >= 0.96 || structure >= 0.86 ? "suggest_recut" : "pass";
}

function dedupeReasonLabel(reason) {
  return ({
    low_cost_dedupe_passed: "浣庢垚鏈娴嬮€氳繃",
    signature_exact: "缁勫悎绛惧悕閲嶅",
    segment_exact: "鐗囨閲嶅",
    same_hook_clip: "Hook 闀滃ご鐩稿悓",
    hook_offset_shifted: "Hook 宸查敊寮€璧风偣",
    visual_plan_key_reuse: "棣栧睆缁勫悎閲嶅",
    visual_near: "鐢婚潰楂樺害鐩镐技",
    same_bgm: "BGM 鐩稿悓",
    same_bgm_offset_shifted: "BGM 宸查敊寮€璧风偣",
    text_near: "鏍囬/瀛楀箷杩戜技",
    structure_near: "鍙欎簨缁撴瀯杩戜技",
    same_structure_variant: "缁撴瀯鍙樹綋鐩稿悓",
    visual_preflight_ok: "瑙嗚鍓嶇疆閬块噸",
    visual_preflight_avoided: "瑙嗚鍊欓€夊凡閬胯",
    ai_text_variant: "AI 鏂囨湰鍙樹綋",
    template_text_variant: "妯℃澘鏂囨湰鍙樹綋",
    text_preflight_avoided: "鏂囨湰鍊欓€夊凡閬胯",
    structure_preflight_ok: "缁撴瀯鍓嶇疆閬块噸",
    structure_preflight_avoided: "缁撴瀯鍊欓€夊凡閬胯",
    bgm_offset_used: "BGM 闅忔満鍒囩墖",
    preflight_limited_pool: "鍊欓€夋睜涓嶈冻",
  })[reason] || String(reason || "");
}

function dedupeAvoidanceSummary(reasons, dedupe = {}) {
  const items = [];
  if (reasons.includes("visual_preflight_ok") || reasons.includes("visual_preflight_avoided") || dedupe.visual_plan_key) items.push("棣栧睆璋冨害");
  if (reasons.includes("ai_text_variant")) items.push("AI 鏂囨湰");
  if (reasons.includes("template_text_variant")) items.push("妯℃澘鏂囨湰");
  if (reasons.includes("structure_preflight_ok") || reasons.includes("structure_preflight_avoided") || dedupe.structure_variant_id) items.push("缁撴瀯杞崲");
  if (reasons.includes("bgm_offset_used") || dedupe.bgm_start_offset > 0) items.push("BGM 鍒囩墖");
  return items.length ? Array.from(new Set(items)).join(" / ") : "甯歌鍘婚噸";
}

function dedupeSuggestion(reasons, report = {}) {
  const visual = Number(report.visual_score || 0);
  const text = Number(report.text_score || 0);
  const structure = Number(report.structure_score || 0);
  if (reasons.includes("signature_exact") || reasons.includes("segment_exact")) return "瑙ｅ喅鏂规锛氫涪寮冭鍊欓€夛紝閲嶆柊鎶藉彇闀滃ご缁勫悎銆?;
  if (reasons.includes("preflight_limited_pool")) return "瑙ｅ喅鏂规锛氳ˉ鍏呮洿澶?Hook 绱犳潗銆丄I 鏂囨湰鍊欓€夋垨 BGM 闀块煶棰戯紝闄嶄綆鍊欓€夋睜涓嶈冻甯︽潵鐨勯噸澶嶉闄┿€?;
  if (reasons.includes("visual_preflight_avoided")) return "瑙ｅ喅鏂规锛欻ook 鍊欓€変笉瓒筹紝寤鸿琛ュ厖鍚岀被棣栧睆绱犳潗鎴栧鍔犲彲鐢ㄨ捣鐐规爣娉ㄣ€?;
  if (reasons.includes("text_preflight_avoided")) return "瑙ｅ喅鏂规锛欰I 鏂囨湰鍊欓€変笉瓒筹紝寤鸿澧炲姞绱犳潗鏍囩鎴栨墿澶?Spark 鏂囨鐢熸垚鏁伴噺銆?;
  if (reasons.includes("structure_preflight_avoided")) return "瑙ｅ喅鏂规锛氬彊浜嬬粨鏋勫€欓€変笉瓒筹紝寤鸿澧炲姞鍒嗙被绱犳潗鎴栨墿灞曢鏋跺彉浣撱€?;
  if (reasons.includes("same_hook_clip") || reasons.includes("visual_near") || visual >= 0.9) return "瑙ｅ喅鏂规锛氭崲 Hook 闀滃ご锛屾垨鎶婇 1 绉掑垏鍒颁笉鍚屽垎绫?涓嶅悓璧风偣锛屽啀鎹㈠皝闈㈠抚銆?;
  if (reasons.includes("text_near") || text >= 0.96) return "瑙ｅ喅鏂规锛氶噸鍐欏瓧骞曠涓€鍙ュ拰鏍囬锛岄伩鍏嶅悓涓€鍙ュ紑澶磋繛缁嚭鐜般€?;
  if (reasons.includes("structure_near") || structure >= 0.86) return "瑙ｅ喅鏂规锛氭崲鍙欎簨楠ㄦ灦鎴栨墦涔遍暅澶撮『搴忋€?;
  if (reasons.includes("same_bgm")) return "瑙ｅ喅鏂规锛氭崲 BGM锛屾垨鑷冲皯閿欏紑闊充箰璧风偣銆?;
  return "瑙ｅ喅鏂规锛氭棤闇€澶勭悊锛屽彲杩涘叆鍙戝竷姹犮€?;
}

function scoreRiskClass(value) {
  const number = Number(value || 0);
  if (number >= 0.8) return "risk-high";
  if (number >= 0.5) return "risk-mid";
  return "risk-low";
}

function reasonRiskClass(reason) {
  if (["signature_exact", "segment_exact"].includes(reason)) return "risk-high";
  if (["same_hook_clip", "visual_near", "text_near", "structure_near", "same_bgm", "visual_preflight_avoided", "text_preflight_avoided", "structure_preflight_avoided", "preflight_limited_pool"].includes(reason)) return "risk-mid";
  return "risk-low";
}

function scorePercent(value) {
  const number = Number(value || 0);
  return `${Math.round(Math.max(0, Math.min(1, number)) * 100)}%`;
}

function showGenerationWaitOverlay(visible, job = {}) {
  const overlay = $("generationWaitOverlay");
  if (!overlay) return;
  overlay.classList.toggle("hidden", !visible);
  if (visible) updateGenerationWaitOverlay(job, Math.max(displayedJobPercent, Math.round((job.progress || 0) * 100)));
}
function updateGenerationWaitOverlay(job, percent) {
  const overlay = $("generationWaitOverlay");
  if (!overlay || overlay.classList.contains("hidden")) return;
  const stage = job.stage || job.status || "queued";
  const value = Math.max(0, Math.min(100, Math.round(percent || 0)));
  const title = $("generationWaitTitle");
  const detail = $("generationWaitDetail");
  const fill = $("generationWaitFill");
  const label = $("generationWaitPercent");
  if (title) title.textContent = localizedJobTitle(job, stage);
  if (detail) detail.textContent = `${localizedJobMessage(job, stage)} 椤甸潰宸查攣瀹氾紝闃叉璇Е銆俙;
  if (fill) fill.style.width = `${value}%`;
  if (label) label.textContent = `${value}%`;
}
function startJobProgressTicker() {
  window.clearInterval(jobProgressTimer);
  jobProgressTimer = window.setInterval(() => {
    const current = displayedJobPercent;
    const next = current < 95 ? current + 1 : current;
    if (next !== current) {
      updateJobStatus({ ...(lastJobSnapshot || { status: "running", stage: "render", message: "瑙嗛鐢熸垚涓紝绯荤粺浼氫互 1% 涓哄崟浣嶆寔缁埛鏂扮瓑寰呰繘搴︺€? }), progress: next / 100 });
    }
  }, 1400);
}
function stopJobProgressTicker() {
  window.clearInterval(jobProgressTimer);
  jobProgressTimer = null;
}
function log(text) { updateJobStatus({ status: "running", stage: "queued", progress: 0, message: text }); }
function localizedJobTitle(job, stage) {
  if (job.status === "error") return "鐢熸垚澶辫触锛岃鏌ョ湅涓嬫柟鎻愮ず";
  if (job.status === "complete") return "鐢熸垚瀹屾垚锛岃棰戝凡瀵煎嚭";
  const titles = {
    queued: "浠诲姟宸叉彁浜わ紝姝ｅ湪绛夊緟寮€濮?,
    ingestion: "姝ｅ湪鎵弿骞舵暣鐞嗙礌鏉?,
    hud: "姝ｅ湪鍑嗗瑙嗛鏁版嵁鍜屽瓧骞?,
    beat: "姝ｅ湪鍒嗘瀽鑳屾櫙闊充箰鑺傚",
    planning: "姝ｅ湪瑙勫垝娣峰壀鏂规",
    render: "姝ｅ湪鐢熸垚瑙嗛锛岃鑰愬績绛夊緟",
    finalizing: "姝ｅ湪鏁寸悊瀵煎嚭鏂囦欢",
  };
  return titles[stage] || "姝ｅ湪澶勭悊锛岃绋嶇瓑";
}
function localizedJobMessage(job, stage) {
  if (job.error) return job.error;
  const message = String(job.message || "").trim();
  if (!message) return jobMessages[stage] || "姝ｅ湪澶勭悊锛岃绋嶇瓑銆?;
  if (backendJobMessageMap[message]) return backendJobMessageMap[message];
  const rendered = message.match(/^Rendered video (\d+)\/(\d+)$/);
  if (rendered) return `姝ｅ湪鐢熸垚瑙嗛锛氬凡瀹屾垚 ${rendered[1]} / ${rendered[2]} 鏉°€俙;
  const rendering = message.match(/^Rendering (\d+) videos with (\d+) workers$/);
  if (rendering) return `姝ｅ湪鍚姩瑙嗛鐢熸垚锛氬叡 ${rendering[1]} 鏉★紝骞惰绾跨▼ ${rendering[2]} 涓€俙;
  const completed = message.match(/^Completed (\d+) exports$/);
  if (completed) return `鐢熸垚瀹屾垚锛屽凡瀵煎嚭 ${completed[1]} 鏉¤棰戙€俙;
  return displayTemplateName(message);
}
function displayTemplateName(value) {
  return String(value || "")
    .replace(/\bCopy\b/g, "鍓湰")
    .replace(/\bMode\b/g, "妯″紡")
    .replace(/\bCenter\b/g, "灞呬腑")
    .replace(/\bBrand\b/g, "鍝佺墝")
    .replace(/\bClean Data\b/g, "娓呯埥鏁版嵁")
    .replace(/\bImpact Hud\b/g, "鍐插嚮瀛楀箷鑳屾澘")
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


