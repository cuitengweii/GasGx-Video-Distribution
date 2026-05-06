const PLATFORM_LABELS = {
  wechat: "视频号",
  douyin: "抖音",
  kuaishou: "快手",
  xiaohongshu: "小红书",
  bilibili: "B站",
  tiktok: "TikTok",
  x: "X",
  linkedin: "LinkedIn",
  facebook: "Facebook",
  youtube: "YouTube",
  vk: "VK",
  instagram: "Instagram",
};

const REGION_LABELS = {
  cn: "国内平台",
  global: "国外平台",
};

const PLATFORM_LOGOS = {
  wechat: { icon: "simple-icons:wechat", bg: "#07c160", fg: "ffffff" },
  douyin: { icon: "simple-icons:tiktok", bg: "#000000", fg: "ffffff" },
  kuaishou: { icon: "simple-icons:kuaishou", bg: "#ff4906", fg: "ffffff" },
  xiaohongshu: { icon: "simple-icons:xiaohongshu", bg: "#ff2442", fg: "ffffff" },
  bilibili: { icon: "simple-icons:bilibili", bg: "#00aeec", fg: "ffffff" },
  tiktok: { icon: "simple-icons:tiktok", bg: "#000000", fg: "ffffff" },
  x: { icon: "simple-icons:x", bg: "#000000", fg: "ffffff" },
  linkedin: { icon: "simple-icons:linkedin", bg: "#0a66c2", fg: "ffffff" },
  facebook: { icon: "simple-icons:facebook", bg: "#1877f2", fg: "ffffff" },
  youtube: { icon: "simple-icons:youtube", bg: "#ff0000", fg: "ffffff" },
  vk: { icon: "simple-icons:vk", bg: "#0077ff", fg: "ffffff" },
  instagram: { icon: "simple-icons:instagram", bg: "linear-gradient(135deg, #feda75 0%, #fa7e1e 28%, #d62976 58%, #962fbf 78%, #4f5bd5 100%)", fg: "ffffff" },
};

const AI_ROBOT_LOGOS = {
  wecom: { icon: "tdesign:logo-wecom-filled", bg: "#20c160", fg: "ffffff" },
  dingtalk: { icon: "mingcute:dingtalk-fill", bg: "#1677ff", fg: "ffffff" },
  lark: { icon: "icon-park:lark", bg: "#00c795", fg: "06130f" },
  telegram: { icon: "simple-icons:telegram", bg: "#2aabee", fg: "ffffff" },
  whatsapp: { icon: "simple-icons:whatsapp", bg: "#25d366", fg: "07150b" },
};

const PLATFORM_ORDER = [
  "wechat",
  "douyin",
  "kuaishou",
  "xiaohongshu",
  "bilibili",
  "tiktok",
  "x",
  "linkedin",
  "facebook",
  "youtube",
  "vk",
  "instagram",
];

const state = {
  accounts: [],
  platforms: [],
  tasks: [],
  stats: [],
  statsCaptureStatus: {},
  summary: {},
  distributionSettings: { common: {}, platforms: {} },
  matrixJobStatus: {},
  aiRobotConfigs: [],
  aiRobotMessages: [],
  notificationRoutes: [],
  notificationEvents: [],
  loginQrBatches: [],
  terminalExecution: { colors: [], operators: [], windows: [], summary: {}, platform_capabilities: {}, profile_by_platform: {}, active_platform: "wechat" },
  terminalRoute: "hub",
  terminalQrVisible: false,
  terminalConfigOpen: false,
  aiRobotEditingPlatform: "",
  aiRobotMessagesCollapsed: true,
  brand: { settings: {} },
  databaseDictionary: null,
  databaseDictionaryExpanded: {},
  databaseDictionaryLocalized: false,
  analytics: {},
  operatorWechats: ["aamecc", "aalbcc"],
};

const TERMINAL_ERROR_FLOW_NODES = [
  {
    title: "获取二维码",
    items: [
      ["二维码缓存失败", "读取浏览器页源或写入二维码缓存时失败。"],
      ["打开浏览器失败", "二维码缓存为空后会尝试自动打开浏览器。"],
      ["二维码过期", "二维码已过期，需要刷新后重新扫码。"],
    ],
  },
  {
    title: "发布启动",
    items: [
      ["无可用素材", "当天素材为空，或已被 consumed 去重。"],
      ["发布配置缺失", "未读取到视频号发布配置或调试端口配置。"],
      ["启动发布子进程失败", "发布进程在创建阶段就报错退出。"],
    ],
  },
  {
    title: "发布执行",
    items: [
      ["进程无效", "发布子进程 PID 无效或已提前退出。"],
      ["未检测到发布证据", "进程结束，但没有找到视频号上传证据。"],
    ],
  },
  {
    title: "人工确认",
    items: [
      ["下一账号二维码刷新失败", "发布成功后切下一账号时，二维码未能重新拉起。"],
    ],
  },
];

const taskSelection = new Set();
const taskFilters = { account: "", platform: "", status: "", taskType: "" };
const TASK_TYPE_OPTIONS = [
  ["draft", "保存草稿"],
  ["publish", "自动发布"],
  ["comment", "自动评论"],
  ["message", "自动私信"],
  ["stats", "数据统计"],
];

const loadedViews = new Set();
let currentView = document.querySelector(".nav-btn.active")?.dataset.view || "overview";
let terminalPollTimer = null;
let terminalCountdownTimer = null;
let terminalPollRequestInFlight = false;
let terminalErrorModalSignature = "";

const SHELL_THEME_KEY = "gasgx-shell-theme";
const SHELL_BRAND_KEY = "gasgx-shell-brand";
const SHELL_AUTH_KEY = "gasgx-shell-auth";
const DATABASE_DICTIONARY_LOCALE_KEY = "gasgx-db-dictionary-locale";
const SETTINGS_CARD_KEY = "gasgx-settings-card";
const PERMISSION_DENIED_MESSAGE = "您权限不足";
const PERMISSION_INTERACTIVE_SELECTOR = "button, input, select, textarea, a, [role=\"button\"], [tabindex]";

const FEATURE_ENTRIES = [
  { id: "overview", label: "总览", group: "业务工作台" },
  { id: "accounts", label: "账号矩阵", group: "业务工作台" },
  { id: "settings", label: "公共设置", group: "业务工作台" },
  { id: "tasks", label: "任务中心", group: "业务工作台" },
  { id: "terminal-execution", label: "终端执行", group: "业务工作台" },
  { id: "stats", label: "数据统计", group: "业务工作台" },
  { id: "ai-robot", label: "AI机器人", group: "业务工作台" },
  { id: "video-matrix", label: "视频生成", group: "业务工作台" },
  { id: "user-center", label: "用户中心", group: "系统管理" },
  { id: "notifications", label: "通知中心", group: "系统管理" },
  { id: "system-settings", label: "系统设置", group: "系统管理" },
  { id: "help-center", label: "帮助文档", group: "系统管理" },
];

const DEFAULT_AUTH_STATE = {
  currentUserId: "allen",
  roles: {
    super_admin: {
      name: "超级管理员",
      permissions: FEATURE_ENTRIES.map((item) => item.id),
    },
    publisher: {
      name: "发布员",
      permissions: ["overview", "accounts", "settings", "tasks", "terminal-execution", "video-matrix", "user-center", "notifications", "help-center"],
    },
    material_manager: {
      name: "素材维护员",
      permissions: ["overview", "accounts", "video-matrix", "user-center", "notifications", "help-center"],
    },
    data_monitor: {
      name: "数据监控员",
      permissions: ["overview", "stats", "user-center", "notifications", "help-center"],
    },
  },
  users: [
    { id: "allen", name: "Allen", roleId: "super_admin" },
    { id: "publisher", name: "发布员", roleId: "publisher" },
    { id: "material", name: "素材维护员", roleId: "material_manager" },
    { id: "analyst", name: "数据监控员", roleId: "data_monitor" },
  ],
  editingRoleId: "super_admin",
};

const SHELL_THEMES = [
  { id: "gasgx-green", name: "GasGx Green", accent: "#5dd62c", soft: "rgba(93, 214, 44, 0.14)" },
  { id: "engine-lime", name: "Engine Lime", accent: "#8ee63f", soft: "rgba(142, 230, 63, 0.14)" },
  { id: "generator-cyan", name: "Generator Cyan", accent: "#28d7c4", soft: "rgba(40, 215, 196, 0.14)" },
  { id: "field-blue", name: "Field Blue", accent: "#4ca3ff", soft: "rgba(76, 163, 255, 0.14)" },
  { id: "grid-violet", name: "Grid Violet", accent: "#8f73ff", soft: "rgba(143, 115, 255, 0.14)" },
  { id: "alert-red", name: "Alert Red", accent: "#ff4d5f", soft: "rgba(255, 77, 95, 0.14)" },
  { id: "power-amber", name: "Power Amber", accent: "#ffb02e", soft: "rgba(255, 176, 46, 0.14)" },
  { id: "steel-silver", name: "Steel Silver", accent: "#b8c0cc", soft: "rgba(184, 192, 204, 0.14)" },
  { id: "methane-teal", name: "Methane Teal", accent: "#00c795", soft: "rgba(0, 199, 149, 0.14)" },
  { id: "night-gold", name: "Night Gold", accent: "#d6b85d", soft: "rgba(214, 184, 93, 0.14)" },
  { id: "neon-magenta", name: "Neon Magenta", accent: "#ff2bd6", soft: "rgba(255, 43, 214, 0.16)" },
  { id: "laser-orange", name: "Laser Orange", accent: "#ff5a1f", soft: "rgba(255, 90, 31, 0.16)" },
  { id: "electric-indigo", name: "Electric Indigo", accent: "#536dff", soft: "rgba(83, 109, 255, 0.16)" },
  { id: "acid-yellow", name: "Acid Yellow", accent: "#dfff24", soft: "rgba(223, 255, 36, 0.16)" },
  { id: "plasma-pink", name: "Plasma Pink", accent: "#ff2f7d", soft: "rgba(255, 47, 125, 0.16)" },
  { id: "volt-green", name: "Volt Green", accent: "#39ff14", soft: "rgba(57, 255, 20, 0.16)" },
  { id: "aqua-burst", name: "Aqua Burst", accent: "#00e5ff", soft: "rgba(0, 229, 255, 0.16)" },
  { id: "solar-orange", name: "Solar Orange", accent: "#ff7a00", soft: "rgba(255, 122, 0, 0.16)" },
  { id: "royal-purple", name: "Royal Purple", accent: "#b026ff", soft: "rgba(176, 38, 255, 0.16)" },
  { id: "hot-coral", name: "Hot Coral", accent: "#ff4f3a", soft: "rgba(255, 79, 58, 0.16)" },
];

const VIEW_HEADERS = {
  overview: ["账号矩阵维护系统", "独立账号、独立浏览器、发布/评论/私信/统计任务入口"],
  accounts: ["账号矩阵", "维护 GasGx 国内外平台账号、独立浏览器配置和登录状态。"],
  "user-center": ["用户中心", "预留操作者资料、角色权限、工作偏好和本地部署身份入口。"],
  settings: ["公共设置", "配置发布素材目录、上传策略、平台参数和矩阵发布作业。"],
  tasks: ["任务中心", "查看发布、评论、私信、登录检测等任务队列和执行状态。"],
  "terminal-execution": ["终端执行", "预留本地终端命令执行入口。"],
  stats: ["数据统计", "短视频账号矩阵数字化营销客户端数据看板。"],
  "ai-robot": ["AI机器人", "AI客服、企业微信、钉钉、飞书、Telegram 与 WhatsApp 统一接入。"],
  "video-matrix": ["视频生成", "分类素材、第一屏封面、视频文字、背景音乐和批量导出工作台。"],
  notifications: ["通知中心", "集中展示生成完成、发布失败、登录失效和素材不足提醒。"],
  "system-settings": ["系统设置", "预留本地部署、存储缓存、安全策略和系统维护入口。"],
  "help-center": ["帮助文档", "预留操作手册、部署说明、视频生成流程和常见问题。"],
};

function displayDatabaseKeyword(value) {
  return String(value ?? "").replaceAll("Supabase", "数据库");
}

state.databaseDictionaryLocalized = localStorage.getItem(DATABASE_DICTIONARY_LOCALE_KEY) === "zh";
let currentSettingsCard = localStorage.getItem(SETTINGS_CARD_KEY) === "platform-publish" ? "platform-publish" : "publish-window";
let currentTerminalInitCard = "window";

const DATABASE_DICTIONARY_TABLE_LABELS = {
  matrix_accounts: "矩阵账号",
  account_platforms: "账号平台",
  browser_profiles: "浏览器配置",
  notification_routes: "通知路由",
  login_qr_batches: "登录二维码批次",
  login_qr_items: "登录二维码明细",
  automation_tasks: "自动化任务",
  video_stats_snapshots: "视频统计快照",
  ai_robot_configs: "AI 机器人配置",
  ai_robot_messages: "AI 机器人消息",
  brand_settings: "品牌设置",
  schema_migrations: "数据库迁移",
  app_settings: "应用设置",
  analytics_items: "分析条目",
  video_matrix_assets: "视频矩阵素材",
  video_matrix_jobs: "视频矩阵任务",
  video_matrix_generation_runs: "视频矩阵生成记录",
  video_matrix_generation_assets: "视频矩阵生成素材",
  video_matrix_generation_segments: "视频矩阵生成片段",
  app_seed_runs: "初始化种子记录",
  brand_members: "品牌成员",
};

const DATABASE_DICTIONARY_COLUMN_LABELS = {
  id: "编号",
  account_key: "账号标识",
  display_name: "显示名称",
  niche: "领域",
  status: "状态",
  notes: "备注",
  created_at: "创建时间",
  updated_at: "更新时间",
  account_id: "账号编号",
  platform: "平台",
  handle: "账号句柄",
  enabled: "启用",
  capability_status: "能力状态",
  login_status: "登录状态",
  last_checked_at: "最后检查时间",
  profile_dir: "配置目录",
  debug_port: "调试端口",
  fingerprint_json: "指纹配置",
  event_type: "事件类型",
  batch_id: "批次编号",
  payload_json: "载荷数据",
  notified_at: "通知时间",
  reason: "原因",
  url: "链接",
  qr_path: "二维码路径",
  qr_fingerprint: "二维码指纹",
  task_type: "任务类型",
  summary: "摘要",
  error: "错误",
  retry_count: "重试次数",
  last_attempt_at: "最后尝试时间",
  sent_at: "发送时间",
  video_ref: "视频引用",
  views: "播放量",
  likes: "点赞数",
  comments: "评论数",
  shares: "分享数",
  messages: "私信数",
  published_at: "发布时间",
  captured_at: "抓取时间",
  bot_name: "机器人名称",
  webhook_url: "回调地址",
  webhook_secret: "回调密钥",
  signing_secret: "签名密钥",
  target_id: "目标编号",
  message_type: "消息类型",
  name: "名称",
  slogan: "标语",
  logo_asset_path: "Logo 资源路径",
  primary_color: "主色",
  theme_id: "主题编号",
  default_account_prefix: "默认账号前缀",
  version: "版本",
  app_version: "应用版本",
  applied_at: "应用时间",
  setting_key: "设置键",
  asset_key: "素材键",
  asset_type: "素材类型",
  title: "标题",
  path: "路径",
  metadata_json: "元数据",
  source: "来源",
  job_key: "任务编号",
  stage: "阶段",
  progress: "进度",
  message: "消息",
  request_json: "请求数据",
  assets_json: "素材数据",
  run_id: "运行编号",
  bgm_filename: "背景音乐文件名",
  bgm_path: "背景音乐路径",
  composition_json: "组合数据",
  sequence_number: "序号",
  signature: "签名",
  copy_path: "文案路径",
  manifest_path: "清单路径",
  template_id: "模板编号",
  cover_template_id: "封面模板编号",
  copy_language: "文案语言",
  segment_index: "片段序号",
  clip_id: "片段编号",
  category: "分类",
  source_path: "源文件路径",
  normalized_path: "标准化路径",
  start_time: "开始时间",
  duration: "时长",
  user_id: "用户编号",
  role: "角色",
  item_key: "条目键",
  section: "分区",
  sort_order: "排序",
};

const DATABASE_DICTIONARY_TYPE_LABELS = {
  bigint: "大整数",
  integer: "整数",
  numeric: "数值",
  text: "文本",
  jsonb: "JSON 数据",
  uuid: "UUID",
};

function translateDatabaseName(name, map, localized) {
  const raw = String(name ?? "").trim();
  if (!localized) return raw;
  return map[raw] || raw;
}

function translateDatabaseType(type, localized) {
  const raw = String(type ?? "").trim();
  if (!localized) return displayDatabaseKeyword(raw);
  return DATABASE_DICTIONARY_TYPE_LABELS[raw.toLowerCase()] || displayDatabaseKeyword(raw);
}

function translateDatabaseDefaultValue(meta, localized) {
  if (!localized) return meta.defaultValue ? displayDatabaseKeyword(meta.defaultValue) : "NULL";
  const raw = String(meta.defaultValue || "").trim();
  if (!raw) return "空值";
  if (/^null$/i.test(raw)) return "空值";
  if (/^as identity$/i.test(raw)) return "自增标识";
  if (/^''$/i.test(raw)) return "空字符串";
  if (/^\{\}::jsonb$/i.test(raw)) return "空 JSON 对象";
  if (/^\[\]::jsonb$/i.test(raw)) return "空 JSON 数组";
  const rawValue = raw.replace(/^'(.+)'$/u, "$1");
  const defaultValueLabels = {
    active: "启用",
    pending: "待处理",
    registered: "已登记",
    unknown: "未知",
    draft: "草稿",
    public: "公开",
    inherit: "继承",
    queued: "排队中",
    available: "可用",
    seed: "种子",
    sent: "已发送",
    retry: "重试",
    failed: "失败",
    running: "运行中",
    complete: "完成",
    info: "提示",
    warning: "警告",
    error: "错误",
    blocking: "阻塞",
    critical: "严重",
    enabled: "已启用",
    disabled: "已禁用",
    short_video: "短视频",
    video: "视频",
    text: "文本",
    image: "图片",
  };
  if (defaultValueLabels[rawValue]) return defaultValueLabels[rawValue];
  return displayDatabaseKeyword(raw);
}

function translateDatabaseConstraintSummary(meta, localized) {
  if (!localized) return meta.raw || "无约束";
  const raw = meta.raw || "";
  const parts = [];
  if (meta.primary) parts.push("主键");
  if (meta.notNull) parts.push("非空");
  if (/unique/i.test(raw)) parts.push("唯一");
  if (/references/i.test(raw)) parts.push("外键");
  if (/generated by default as identity/i.test(raw)) parts.push("自增");
  if (/on delete cascade/i.test(raw)) parts.push("删除级联");
  if (/on delete set null/i.test(raw)) parts.push("删除置空");
  if (/check/i.test(raw)) parts.push("校验");
  if (/default/i.test(raw) && meta.defaultValue) parts.push(`默认 ${translateDatabaseDefaultValue(meta, true)}`);
  return parts.length ? parts.join(" / ") : "无约束";
}

function parseDatabaseColumnMeta(constraints) {
  const raw = String(constraints || "").trim();
  const primary = /\bPRIMARY\s+KEY\b/i.test(raw);
  const notNull = /\bNOT\s+NULL\b/i.test(raw);
  const defaultMatch = raw.match(
    /\bDEFAULT\b\s+(.+?)(?=\s+\b(?:PRIMARY\s+KEY|NOT\s+NULL|UNIQUE|REFERENCES|CHECK|COLLATE|CONSTRAINT)\b|$)/i
  );
  const defaultValue = defaultMatch ? defaultMatch[1].trim().replace(/,+$/, "") : "";
  return {
    raw,
    primary,
    notNull,
    defaultValue,
  };
}

function isDatabaseTableExpanded(tableName) {
  return state.databaseDictionaryExpanded?.[tableName] !== false;
}

function toggleDatabaseTable(tableName) {
  if (!tableName) return;
  const next = { ...(state.databaseDictionaryExpanded || {}) };
  next[tableName] = !isDatabaseTableExpanded(tableName);
  state.databaseDictionaryExpanded = next;
  renderDatabaseDictionary();
}

function toggleDatabaseDictionaryLocale() {
  state.databaseDictionaryLocalized = !state.databaseDictionaryLocalized;
  localStorage.setItem(DATABASE_DICTIONARY_LOCALE_KEY, state.databaseDictionaryLocalized ? "zh" : "en");
  renderDatabaseDictionary();
}

function setViewHeader(view) {
  const [title, description] = VIEW_HEADERS[view] || VIEW_HEADERS.overview;
  document.querySelector("#page-title").textContent = title;
  document.querySelector("#page-description").textContent = description;
  document.querySelector("#refresh")?.classList.toggle("hidden", view === "video-matrix");
}

function broadcastShellTheme(theme) {
  document.querySelectorAll(".video-matrix-frame").forEach((frame) => {
    frame.contentWindow?.postMessage({ type: "gasgx-shell-theme", theme }, window.location.origin);
  });
}

function applyShellTheme(themeId) {
  const theme = SHELL_THEMES.find((item) => item.id === themeId) || SHELL_THEMES[0];
  document.documentElement.style.setProperty("--accent-aurora", theme.accent);
  document.documentElement.style.setProperty("--accent-soft", theme.soft);
  localStorage.setItem(SHELL_THEME_KEY, theme.id);
  document.querySelectorAll(".theme-card").forEach((card) => {
    card.classList.toggle("active", card.dataset.themeId === theme.id);
  });
  broadcastShellTheme(theme);
}

function renderThemePalette() {
  const grid = document.querySelector("#theme-palette-grid");
  if (!grid) return;
  grid.innerHTML = SHELL_THEMES.map((theme) => `
    <button class="theme-card" type="button" data-theme-id="${theme.id}">
      <span class="theme-dot" style="color:${theme.accent};background:${theme.accent}"></span>
      <span><strong>${theme.name}</strong><small>${theme.accent}</small></span>
    </button>
  `).join("");
  grid.querySelectorAll("[data-theme-id]").forEach((button) => {
    button.addEventListener("click", () => applyShellTheme(button.dataset.themeId));
  });
  applyShellTheme(localStorage.getItem(SHELL_THEME_KEY) || SHELL_THEMES[0].id);
}

function applyShellBrand(brand) {
  const next = {
    name: brand?.name || "GasGx",
    slogan: brand?.slogan || "Video Distribution",
    logoDataUrl: brand?.logoDataUrl || "",
  };
  document.querySelector("#brand-name").textContent = next.name;
  document.querySelector("#brand-slogan").textContent = displayDatabaseKeyword(next.slogan);
  document.querySelector("#brand-preview-name").textContent = next.name;
  document.querySelector("#brand-preview-slogan").textContent = displayDatabaseKeyword(next.slogan);
  document.querySelector("#brand-name-input").value = next.name;
  document.querySelector("#brand-slogan-input").value = next.slogan;

  const logoNodes = [document.querySelector("#brand-logo-image"), document.querySelector("#brand-preview-logo")];
  const markNodes = [document.querySelector("#brand-mark"), document.querySelector("#brand-preview-mark")];
  logoNodes.forEach((node) => {
    node.src = next.logoDataUrl;
    node.classList.toggle("hidden", !next.logoDataUrl);
  });
  markNodes.forEach((node) => node.classList.toggle("hidden", Boolean(next.logoDataUrl)));
}

function readStoredBrand() {
  try {
    return JSON.parse(localStorage.getItem(SHELL_BRAND_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveShellBrand(brand) {
  localStorage.setItem(SHELL_BRAND_KEY, JSON.stringify(brand));
  applyShellBrand(brand);
}

function initBrandSettings() {
  const stored = readStoredBrand();
  applyShellBrand(stored);
  const nameInput = document.querySelector("#brand-name-input");
  const sloganInput = document.querySelector("#brand-slogan-input");
  const upload = document.querySelector("#brand-logo-upload");
  const syncPreview = () => applyShellBrand({ ...readStoredBrand(), name: nameInput.value, slogan: sloganInput.value });
  nameInput.addEventListener("input", syncPreview);
  sloganInput.addEventListener("input", syncPreview);
  upload.addEventListener("change", () => {
    const file = upload.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => applyShellBrand({ name: nameInput.value, slogan: sloganInput.value, logoDataUrl: String(reader.result || "") });
    reader.readAsDataURL(file);
  });
  document.querySelector("#save-brand-settings").addEventListener("click", async (event) => {
    const restoreButton = setButtonLoading(event.currentTarget, "保存中...");
    const currentLogo = document.querySelector("#brand-logo-image").src || "";
    const payload = {
      name: nameInput.value,
      slogan: sloganInput.value,
      logo_asset_path: currentLogo.startsWith("data:") ? currentLogo : "",
      primary_color: getComputedStyle(document.documentElement).getPropertyValue("--accent-aurora").trim() || "#5dd62c",
      theme_id: localStorage.getItem(SHELL_THEME_KEY) || "gasgx-green",
      default_account_prefix: nameInput.value || "GasGx",
    };
    try {
      const settings = await api("/api/brand", { method: "PATCH", body: JSON.stringify(payload) });
      saveShellBrand({ name: settings.name, slogan: settings.slogan, logoDataUrl: settings.logo_asset_path || "" });
    } finally {
      restoreButton();
    }
  });
  document.querySelector("#reset-brand-settings").addEventListener("click", (event) => {
    const restoreButton = setButtonLoading(event.currentTarget, "恢复中...");
    localStorage.removeItem(SHELL_BRAND_KEY);
    upload.value = "";
    applyShellBrand({});
    window.setTimeout(restoreButton, 160);
  });
}

function applyServerBrand(brand) {
  const settings = brand?.settings || {};
  applyShellBrand({
    name: settings.name || "GasGx",
    slogan: settings.slogan || "Video Distribution",
    logoDataUrl: settings.logo_asset_path || "",
  });
  if (settings.theme_id) applyShellTheme(settings.theme_id);
  const prefix = document.querySelector('input[name="brand_prefix"]');
  if (prefix) prefix.value = settings.default_account_prefix || settings.name || "GasGx";
}

let authState = { ...DEFAULT_AUTH_STATE, features: FEATURE_ENTRIES };

function readStoredAuthSession() {
  try {
    return JSON.parse(localStorage.getItem(SHELL_AUTH_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveAuthSession(nextSession) {
  localStorage.setItem(SHELL_AUTH_KEY, JSON.stringify(nextSession));
}

function currentAuthUser(statePayload = authState) {
  if (!statePayload.currentUserId) return null;
  return statePayload.users.find((user) => user.id === statePayload.currentUserId) || statePayload.users[0];
}

function currentPermissions(statePayload = authState) {
  const user = currentAuthUser(statePayload);
  const role = statePayload.roles[user?.roleId] || statePayload.roles.publisher;
  return new Set(role?.permissions || []);
}

function canUseView(view, statePayload = authState) {
  const user = currentAuthUser(statePayload);
  if (!user) return view === "user-center";
  if (user.roleId === "super_admin") return true;
  return currentPermissions(statePayload).has(view);
}

function isPermissionLimitedView(view = currentView, statePayload = authState) {
  const section = document.querySelector(`#${view}`);
  return Boolean(section && !canUseView(view, statePayload));
}

function showPermissionDenied() {
  let toast = document.querySelector("#permission-denied-toast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "permission-denied-toast";
    toast.className = "permission-denied-toast";
    toast.setAttribute("role", "status");
    toast.setAttribute("aria-live", "polite");
    document.body.appendChild(toast);
  }
  toast.textContent = PERMISSION_DENIED_MESSAGE;
  toast.classList.add("show");
  clearTimeout(showPermissionDenied.timer);
  showPermissionDenied.timer = setTimeout(() => {
    toast.classList.remove("show");
  }, 1800);
}

function showAccountCreatedToast(account) {
  let toast = document.querySelector("#account-created-toast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "account-created-toast";
    toast.className = "permission-denied-toast account-created-toast";
    toast.setAttribute("role", "status");
    toast.setAttribute("aria-live", "polite");
    document.body.appendChild(toast);
  }
  toast.innerHTML = `<strong>账号创建成功</strong><span>#${account.id} ${account.display_name}</span>`;
  toast.classList.add("show");
  clearTimeout(showAccountCreatedToast.timer);
  showAccountCreatedToast.timer = setTimeout(() => {
    toast.classList.remove("show");
  }, 2600);
}

function terminalErrorStageTitle(stage) {
  const map = {
    load: "终端执行加载失败",
    qr: "获取二维码失败",
    publish_start: "发布启动失败",
    publish_run: "发布执行失败",
    confirm: "人工确认失败",
  };
  return map[stage] || "终端流程错误";
}

function terminalErrorStageFromMessage(message) {
  const text = String(message || "");
  if (text.startsWith("发布启动失败")) return "publish_start";
  if (text.startsWith("发布失败")) return "publish_run";
  if (/二维码/.test(text)) return "qr";
  return "";
}

function terminalErrorFlowMarkup() {
  return TERMINAL_ERROR_FLOW_NODES.map((section) => `
    <section class="terminal-error-flow-section">
      <strong>${escapeHtml(section.title)}</strong>
      <div class="terminal-error-flow-list">
        ${section.items.map(([label, desc]) => `
          <article class="terminal-error-flow-item">
            <strong>${escapeHtml(label)}</strong>
            <p>${escapeHtml(desc)}</p>
          </article>
        `).join("")}
      </div>
    </section>
  `).join("");
}

function terminalErrorSnapshot() {
  const terminal = state.terminalExecution || {};
  if (terminal.error) {
    const message = String(terminal.error || "");
    const signature = `load|${message}`;
    return {
      stage: "load",
      title: terminalErrorStageTitle("load"),
      message,
      signature,
      nodes: TERMINAL_ERROR_FLOW_NODES,
      context: "终端执行数据加载失败",
    };
  }
  for (const windowItem of terminal.windows || []) {
    const accounts = windowItem?.accounts || [];
    const currentIndex = Number(windowItem?.current_index || 0);
    const current = accounts[currentIndex];
    if (!current) continue;
    const status = String(current.status || "").toLowerCase();
    const run = windowItem.publish_run || {};
    const runStatus = String(run.status || "").toLowerCase();
    if (status !== "error" && runStatus !== "failed") continue;
    const stage = String(current.error_stage || run.error_stage || terminalErrorStageFromMessage(current.status_text || run.error || "") || "").trim() || "qr";
    const title = String(current.error_title || run.error_title || terminalErrorStageTitle(stage));
    const message = String(current.error_detail || current.status_text || run.error || "终端流程发生错误");
    const signature = [
      "terminal",
      windowItem.id,
      current.id,
      stage,
      title,
      message,
      runStatus,
    ].join("|");
    return {
      stage,
      title,
      message,
      signature,
      nodes: TERMINAL_ERROR_FLOW_NODES,
      context: `窗口 #${windowItem.id} · 账号 #${current.id}`,
    };
  }
  return null;
}

function showTerminalErrorModal(payload) {
  const modal = document.querySelector("#terminalErrorModal");
  if (!modal) return;
  const titleNode = modal.querySelector("#terminalErrorTitle");
  const stageNode = modal.querySelector("#terminalErrorStage");
  const contextNode = modal.querySelector("#terminalErrorContext");
  const messageNode = modal.querySelector("#terminalErrorMessage");
  const flowNode = modal.querySelector("#terminalErrorFlow");
  if (titleNode) titleNode.textContent = payload.title || "终端流程错误";
  if (stageNode) stageNode.textContent = payload.stage ? `错误节点：${terminalErrorStageTitle(payload.stage)}` : "错误节点";
  if (contextNode) contextNode.textContent = payload.context || "";
  if (messageNode) messageNode.textContent = payload.message || "终端流程发生错误";
  if (flowNode) flowNode.innerHTML = terminalErrorFlowMarkup();
  modal.classList.remove("hidden");
}

function hideTerminalErrorModal() {
  document.querySelector("#terminalErrorModal")?.classList.add("hidden");
}

function syncTerminalErrorModal() {
  const payload = terminalErrorSnapshot();
  if (!payload) {
    terminalErrorModalSignature = "";
    hideTerminalErrorModal();
    return;
  }
  if (payload.signature === terminalErrorModalSignature) return;
  terminalErrorModalSignature = payload.signature;
  showTerminalErrorModal(payload);
}

function showTerminalFlowGuideModal(platform) {
  const token = String(platform || "wechat");
  const label = terminalPlatformName(token);
  showTerminalErrorModal({
    stage: "",
    title: `${label}流程错误节点`,
    message: "以下为该平台流程中的常见错误节点与排查方向。",
    context: `${label} · 每次进入平台时展示`,
    signature: `guide|${token}|${Date.now()}`,
  });
}

function applyPermissionLimitedState() {
  document.querySelectorAll(".view").forEach((section) => {
    const locked = isPermissionLimitedView(section.id);
    section.classList.toggle("permission-limited-view", locked);
    section.querySelectorAll(PERMISSION_INTERACTIVE_SELECTOR).forEach((node) => {
      if (!node.closest(".permission-notice")) node.classList.toggle("permission-blocked-control", locked);
    });
  });
}

function setOperatorWechatValue(value) {
  const picker = document.querySelector("#operator-wechat-select");
  const hidden = document.querySelector("#operator-wechat-value");
  const trigger = picker?.querySelector(".inline-select-trigger");
  if (!picker || !hidden || !trigger || !value) return;
  picker.dataset.value = value;
  hidden.value = value;
  trigger.textContent = value;
  trigger.setAttribute("aria-expanded", "false");
  picker.querySelector(".inline-select-menu")?.classList.add("hidden");
}

function renderOperatorWechatPicker() {
  const picker = document.querySelector("#operator-wechat-select");
  const menu = picker?.querySelector(".inline-select-menu");
  const addRow = menu?.querySelector(".inline-select-add");
  if (!picker || !menu || !addRow) return;
  menu.querySelectorAll("[data-operator-wechat-option]").forEach((item) => item.remove());
  state.operatorWechats.forEach((value) => {
    const option = document.createElement("button");
    option.className = "inline-select-option";
    option.type = "button";
    option.dataset.operatorWechatOption = value;
    option.textContent = value;
    menu.insertBefore(option, addRow);
  });
  const current = state.operatorWechats.includes(picker.dataset.value) ? picker.dataset.value : state.operatorWechats[0];
  setOperatorWechatValue(current || "aamecc");
}

function addOperatorWechatOptionFromMenu() {
  const picker = document.querySelector("#operator-wechat-select");
  const input = document.querySelector("#operator-wechat-add-input");
  const menu = picker?.querySelector(".inline-select-menu");
  const value = input?.value.trim();
  if (!picker || !input || !menu || !value) {
    input?.focus();
    return;
  }
  const addButton = document.querySelector("#operator-wechat-add");
  const restoreButton = setButtonLoading(addButton, "保存中");
  api("/api/operator-wechats", {
    method: "POST",
    body: JSON.stringify({ operator_wechat: value }),
  }).then((result) => {
    state.operatorWechats = result.items || state.operatorWechats;
    input.value = "";
    renderOperatorWechatPicker();
    setOperatorWechatValue(result.operator_wechat || value);
  }).finally(restoreButton);
}

function applyPermissions() {
  const statePayload = authState;
  const user = currentAuthUser(statePayload);
  const role = user ? statePayload.roles[user?.roleId] || statePayload.roles.publisher : null;
  const permissions = user ? currentPermissions(statePayload) : new Set(["user-center"]);
  document.querySelector("#signed-user-name").textContent = user?.name || "未登录";
  document.querySelector(".signed-user-badge")?.setAttribute("aria-label", `当前登录用户 ${user?.name || "未登录"}`);
  const sessionUserName = document.querySelector("#session-user-name");
  const sessionUserDesc = document.querySelector("#session-user-desc");
  const sessionRoleBadge = document.querySelector("#session-role-badge");
  const sessionAvatar = document.querySelector("#session-avatar");
  if (sessionUserName) sessionUserName.textContent = user?.name || "未登录";
  if (sessionUserDesc) sessionUserDesc.textContent = user ? `${role?.name || "未分配角色"} / ${user?.roleId === "super_admin" ? "可分配账号与角色权限" : "按角色显示功能入口"}` : "请用已分配账号登录";
  if (sessionRoleBadge) sessionRoleBadge.textContent = role?.name || "未登录";
  if (sessionAvatar) sessionAvatar.textContent = (user?.name || "G").slice(0, 1).toUpperCase();
  document.body.classList.toggle("auth-logged-out", !user);
  document.querySelectorAll("[data-permission]").forEach((node) => {
    const allowed = permissions.has(node.dataset.permission) || user?.roleId === "super_admin";
    node.classList.toggle("permission-denied-entry", !allowed);
    if (!allowed) node.setAttribute("title", PERMISSION_DENIED_MESSAGE);
    else node.removeAttribute("title");
  });
  document.querySelectorAll("[data-admin-only]").forEach((node) => {
    node.classList.toggle("permission-admin-only", user?.roleId !== "super_admin");
  });
  applyPermissionLimitedState();
}

function renderLoginOptions(statePayload = authState) {
  const loginSelect = document.querySelector("#login-user-select");
  const roleSelect = document.querySelector("#operator-role-select");
  if (loginSelect) {
    const loginUsers = statePayload.users;
    loginSelect.innerHTML = loginUsers.map((user) => {
      const role = statePayload.roles[user.roleId];
      if (user.id === "allen") return `<option value="${user.id}">${role?.name || "超级管理员"}</option>`;
      return `<option value="${user.id}">${user.name} · ${role?.name || "未分配"}</option>`;
    }).join("");
    loginSelect.value = loginUsers.some((user) => user.id === statePayload.currentUserId)
      ? statePayload.currentUserId
      : loginUsers[0]?.id || "";
  }
  if (roleSelect) {
    roleSelect.innerHTML = Object.entries(statePayload.roles).map(([roleId, role]) => `<option value="${roleId}">${role.name}</option>`).join("");
  }
}

function renderOperatorAccounts(statePayload = authState) {
  const list = document.querySelector("#operator-account-list");
  if (!list) return;
  list.innerHTML = statePayload.users.map((user) => {
    const role = statePayload.roles[user.roleId];
    return `
      <article class="operator-account-row">
        <strong>${user.name}</strong>
        <select data-user-role="${user.id}" ${user.roleId === "super_admin" ? "disabled" : ""}>
          ${Object.entries(statePayload.roles).map(([roleId, item]) => `<option value="${roleId}" ${roleId === user.roleId ? "selected" : ""}>${item.name}</option>`).join("")}
        </select>
        <input data-user-password="${user.id}" type="text" placeholder="${user.roleId === "super_admin" ? "系统固定" : "设置/重置口令"}" ${user.roleId === "super_admin" ? "disabled" : ""}>
        <button class="btn secondary" type="button" data-save-user-password="${user.id}" ${user.roleId === "super_admin" ? "disabled" : ""}>保存口令</button>
        <span>${role?.name || "未分配"}</span>
      </article>
    `;
  }).join("");
  list.querySelectorAll("[data-user-role]").forEach((select) => {
    select.addEventListener("change", async () => {
      authState = await api(`/api/auth/users/${encodeURIComponent(select.dataset.userRole)}/role`, {
        method: "PATCH",
        body: JSON.stringify({ role_id: select.value }),
      });
      renderAuthCenter();
    });
  });
  list.querySelectorAll("[data-save-user-password]").forEach((button) => {
    button.addEventListener("click", async () => {
      const userId = button.dataset.saveUserPassword;
      const input = list.querySelector(`[data-user-password="${userId}"]`);
      const password = input?.value.trim() || "";
      if (!password) return;
      const restoreButton = setButtonLoading(button, "保存中...");
      try {
        authState = await api(`/api/auth/users/${encodeURIComponent(userId)}/password`, {
          method: "PATCH",
          body: JSON.stringify({ password }),
        });
        input.value = "";
        renderAuthCenter();
      } finally {
        restoreButton();
      }
    });
  });
}

function renderRoleTabs(statePayload = authState) {
  const tabs = document.querySelector("#role-tabs");
  if (!tabs) return;
  tabs.innerHTML = Object.entries(statePayload.roles).map(([roleId, role]) => `
    <button class="role-tab ${roleId === statePayload.editingRoleId ? "active" : ""}" type="button" data-role-tab="${roleId}">
      ${role.name}
    </button>
  `).join("");
  tabs.querySelectorAll("[data-role-tab]").forEach((button) => {
    button.addEventListener("click", async () => {
      const restoreButton = setButtonLoading(button, "切换中...");
      const session = readStoredAuthSession();
      session.editingRoleId = button.dataset.roleTab;
      saveAuthSession(session);
      try {
        authState = await api(`/api/auth/state?current_user_id=${encodeURIComponent(authState.currentUserId)}&editing_role_id=${encodeURIComponent(session.editingRoleId)}`);
        renderAuthCenter();
      } finally {
        restoreButton();
      }
    });
  });
}

function renderPermissionGrid(statePayload = authState) {
  const grid = document.querySelector("#permission-grid");
  const badge = document.querySelector("#permission-role-badge");
  if (!grid) return;
  const roleId = statePayload.editingRoleId || "super_admin";
  const role = statePayload.roles[roleId] || statePayload.roles.super_admin;
  const permissionSet = new Set(role.permissions || []);
  if (badge) badge.textContent = role.name;
  grid.innerHTML = statePayload.features.map((entry) => `
    <label class="permission-item">
      <input type="checkbox" data-role-permission="${entry.id}" ${permissionSet.has(entry.id) ? "checked" : ""} ${roleId === "super_admin" ? "disabled" : ""}>
      <span><strong>${entry.label}</strong><small>${entry.group}</small></span>
    </label>
  `).join("");
  grid.querySelectorAll("[data-role-permission]").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const nextRole = authState.roles[authState.editingRoleId];
      if (!nextRole || authState.editingRoleId === "super_admin") return;
      const nextPermissions = new Set(nextRole.permissions || []);
      if (checkbox.checked) nextPermissions.add(checkbox.dataset.rolePermission);
      else nextPermissions.delete(checkbox.dataset.rolePermission);
      authState = await api(`/api/auth/roles/${encodeURIComponent(authState.editingRoleId)}/permissions`, {
        method: "PUT",
        body: JSON.stringify({ permissions: Array.from(nextPermissions) }),
      });
      renderAuthCenter();
    });
  });
}

function renderAuthCenter() {
  renderLoginOptions(authState);
  renderOperatorAccounts(authState);
  renderRoleTabs(authState);
  renderPermissionGrid(authState);
  applyPermissions();
}

async function loadAuthCenter() {
  const session = readStoredAuthSession();
  authState = await api(`/api/auth/state?current_user_id=${encodeURIComponent(session.currentUserId || "allen")}&editing_role_id=${encodeURIComponent(session.editingRoleId || "super_admin")}`);
  if (session.loggedOut) authState.currentUserId = "";
  renderAuthCenter();
}

function initAuthCenter() {
  loadAuthCenter().catch(() => renderAuthCenter());
  document.querySelector("#local-login-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const password = document.querySelector("#login-password").value.trim();
    const errorNode = document.querySelector("#login-error");
    if (errorNode) errorNode.textContent = "";
    if (!password) return;
    const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "登录中...");
    try {
      authState = await api("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({ user_id: document.querySelector("#login-user-select").value, password }),
      });
      saveAuthSession({ currentUserId: authState.currentUserId, editingRoleId: authState.editingRoleId, loggedOut: false });
      document.querySelector("#login-password").value = "";
      renderAuthCenter();
      activateView("overview");
    } catch (error) {
      if (errorNode) errorNode.textContent = error.message || "登录失败";
    } finally {
      restoreButton();
    }
  });
  document.querySelector("#operator-account-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const nameInput = document.querySelector("#operator-name-input");
    const name = nameInput.value.trim();
    if (!name) return;
    const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "添加中...");
    try {
      authState = await api("/api/auth/users", {
        method: "POST",
        body: JSON.stringify({
          name,
          role_id: document.querySelector("#operator-role-select").value || "publisher",
          password: document.querySelector("#operator-password-input").value.trim(),
        }),
      });
      nameInput.value = "";
      document.querySelector("#operator-password-input").value = "";
      renderAuthCenter();
    } finally {
      restoreButton();
    }
  });
  document.querySelector("#role-create-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = document.querySelector("#role-name-input");
    const name = input.value.trim();
    if (!name) return;
    const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "添加中...");
    try {
      authState = await api("/api/auth/roles", { method: "POST", body: JSON.stringify({ name }) });
      saveAuthSession({ currentUserId: authState.currentUserId, editingRoleId: authState.editingRoleId });
      input.value = "";
      renderAuthCenter();
    } finally {
      restoreButton();
    }
  });
  document.querySelectorAll("[data-logout]").forEach((button) => {
    button.addEventListener("click", async () => {
      const restoreButton = setButtonLoading(button, "退出中...");
      try {
        saveAuthSession({ currentUserId: "", editingRoleId: "super_admin", loggedOut: true });
        authState = await api("/api/auth/state?current_user_id=allen&editing_role_id=super_admin");
        authState.currentUserId = "";
        renderAuthCenter();
        window.history.replaceState(null, "", "#login");
      } finally {
        restoreButton();
      }
    });
  });
}

function initUserMenu() {
  const toggle = document.querySelector("#user-menu-toggle");
  const menu = document.querySelector("#sidebar-user-actions");
  if (toggle && menu) toggle.addEventListener("click", () => {
    const open = menu.classList.toggle("hidden") === false;
    toggle.setAttribute("aria-expanded", String(open));
  });
  const topToggle = document.querySelector("#top-user-toggle");
  const topMenu = document.querySelector("#top-user-menu");
  topToggle?.addEventListener("click", (event) => {
    event.stopPropagation();
    if (!topMenu) return;
    const open = !topMenu.classList.contains("open");
    topMenu.classList.toggle("open", open);
    topToggle.setAttribute("aria-expanded", String(open));
  });
  document.addEventListener("click", (event) => {
    if (toggle && menu && !(toggle.contains(event.target) || menu.contains(event.target))) {
      menu.classList.add("hidden");
      toggle.setAttribute("aria-expanded", "false");
    }
    if (topToggle && topMenu && !(topToggle.contains(event.target) || topMenu.contains(event.target))) {
      topMenu.classList.remove("open");
      topToggle.setAttribute("aria-expanded", "false");
    }
  });
  document.querySelectorAll("[data-quick-view]").forEach((button) => {
    button.addEventListener("click", () => {
      activateView(button.dataset.quickView);
      menu?.classList.add("hidden");
      toggle?.setAttribute("aria-expanded", "false");
      topMenu?.classList.remove("open");
      topToggle?.setAttribute("aria-expanded", "false");
    });
  });
  const sidebarToggle = document.querySelector("#sidebar-toggle");
  sidebarToggle?.addEventListener("click", () => {
    const collapsed = document.body.classList.toggle("sidebar-collapsed");
    sidebarToggle.textContent = collapsed ? "›" : "‹";
    sidebarToggle.setAttribute("aria-expanded", String(!collapsed));
    sidebarToggle.setAttribute("aria-label", collapsed ? "显示左侧栏" : "隐藏左侧栏");
  });
}

function shouldBlockPermissionAction(target) {
  if (target.closest("#refresh") && isPermissionLimitedView()) return true;
  const interactive = target.closest(PERMISSION_INTERACTIVE_SELECTOR);
  const activeView = target.closest(".view.active");
  if (activeView && isPermissionLimitedView(activeView.id)) return true;
  if (!interactive) return false;
  const adminRegion = target.closest("[data-admin-only]");
  const user = currentAuthUser(authState);
  return Boolean(adminRegion && user?.roleId !== "super_admin");
}

function initPermissionGuards() {
  document.addEventListener("click", (event) => {
    if (!shouldBlockPermissionAction(event.target)) return;
    event.preventDefault();
    event.stopPropagation();
    showPermissionDenied();
  }, true);
  document.addEventListener("submit", (event) => {
    if (!shouldBlockPermissionAction(event.target)) return;
    event.preventDefault();
    event.stopPropagation();
    showPermissionDenied();
  }, true);
  document.addEventListener("change", (event) => {
    if (!shouldBlockPermissionAction(event.target)) return;
    event.preventDefault();
    event.stopPropagation();
    showPermissionDenied();
    renderAuthCenter();
  }, true);
}

function showTaskState(message, kind = "muted") {
  let node = document.querySelector("#task-create-state");
  if (!node) {
    node = document.createElement("div");
    node.id = "task-create-state";
    node.className = "muted";
    document.querySelector("#task-form").appendChild(node);
  }
  node.className = kind;
  node.textContent = message;
}

function formatFriendlyMessage(message) {
  const text = String(message || "");
  const duplicateTaskMatch = text.match(/^duplicate active task already queued: #(\d+)$/i);
  if (duplicateTaskMatch) return `已有相同任务在队列中：#${duplicateTaskMatch[1]}`;
  if (text === "queued for manual worker execution") return "已加入队列，等待人工执行";
  if (text === "pending") return "待处理";
  if (text === "paused") return "已暂停";
  if (text === "unsupported") return "暂不支持";
  return text || "操作失败，请稍后重试";
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const text = await response.arrayBuffer().then((buffer) => {
    const decoded = new TextDecoder("utf-8").decode(buffer);
    if (/[ÃÂåçæèäöü]/.test(decoded) && !/[\u4e00-\u9fff]/.test(decoded)) {
      try {
        const repaired = new TextDecoder("utf-8").decode(Uint8Array.from(decoded, (char) => char.charCodeAt(0)));
        if (/[\u4e00-\u9fff]/.test(repaired)) return repaired;
      } catch (_error) {
        return decoded;
      }
    }
    return decoded;
  });
  if (!response.ok) {
    let body = {};
    try {
      body = JSON.parse(text);
    } catch (_error) {
      body = {};
    }
    throw new Error(body.detail || response.statusText);
  }
  return JSON.parse(text);
}

function setButtonLoading(button, loadingText = "处理中") {
  if (!button) return () => {};
  const previousHtml = button.innerHTML;
  const previousDisabled = button.disabled;
  const previousBusy = button.getAttribute("aria-busy");
  button.disabled = true;
  button.classList.add("loading");
  button.setAttribute("aria-busy", "true");
  button.innerHTML = `<span class="btn-spinner" aria-hidden="true"></span><span>${loadingText}</span>`;
  return () => {
    button.innerHTML = previousHtml;
    button.disabled = previousDisabled;
    button.classList.remove("loading");
    if (previousBusy === null) button.removeAttribute("aria-busy");
    else button.setAttribute("aria-busy", previousBusy);
  };
}

function pulseButtonLoading(button, loadingText = "处理中", holdMs = 280) {
  if (!button || button.disabled || button.classList.contains("loading")) return () => {};
  const restoreButton = setButtonLoading(button, loadingText);
  const timer = window.setTimeout(() => {
    restoreButton();
  }, holdMs);
  return () => {
    window.clearTimeout(timer);
    restoreButton();
  };
}

function loadingInline(label = "加载中...") {
  return `<div class="loading-inline"><span class="btn-spinner" aria-hidden="true"></span><span>${label}</span></div>`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

function renderHelpMarkdown(markdown) {
  const lines = String(markdown || "").split(/\r?\n/);
  const html = [];
  let listOpen = false;
  let codeOpen = false;
  const closeList = () => {
    if (listOpen) {
      html.push("</ul>");
      listOpen = false;
    }
  };
  lines.forEach((line) => {
    if (line.trim().startsWith("```")) {
      closeList();
      html.push(codeOpen ? "</code></pre>" : "<pre><code>");
      codeOpen = !codeOpen;
      return;
    }
    if (codeOpen) {
      html.push(escapeHtml(line));
      return;
    }
    const trimmed = line.trim();
    if (!trimmed) {
      closeList();
      return;
    }
    if (trimmed.startsWith("# ")) {
      closeList();
      html.push(`<h1>${escapeHtml(trimmed.slice(2))}</h1>`);
      return;
    }
    if (trimmed.startsWith("## ")) {
      closeList();
      html.push(`<h2>${escapeHtml(trimmed.slice(3))}</h2>`);
      return;
    }
    if (trimmed.startsWith("### ")) {
      closeList();
      html.push(`<h3>${escapeHtml(trimmed.slice(4))}</h3>`);
      return;
    }
    if (trimmed.startsWith("- ")) {
      if (!listOpen) {
        html.push("<ul>");
        listOpen = true;
      }
      html.push(`<li>${escapeHtml(trimmed.slice(2))}</li>`);
      return;
    }
    closeList();
    html.push(`<p>${escapeHtml(trimmed)}</p>`);
  });
  closeList();
  if (codeOpen) html.push("</code></pre>");
  return html.join("");
}

async function openHelpDocument(path) {
  const docName = String(path || "").split("/").pop();
  if (!docName) return;
  const reader = document.querySelector("#help-doc-reader");
  const body = document.querySelector("#help-reader-body");
  if (!reader || !body) return;
  reader.classList.remove("hidden");
  body.innerHTML = loadingInline("加载帮助文档...");
  const doc = await api(`/api/help-docs/${encodeURIComponent(docName)}`);
  const firstTitle = String(doc.content || "").split(/\r?\n/).find((line) => line.startsWith("# "));
  document.querySelector("#help-reader-title").textContent = firstTitle ? firstTitle.replace(/^#\s*/, "") : doc.name;
  body.innerHTML = renderHelpMarkdown(doc.content);
  reader.scrollIntoView({ behavior: "smooth", block: "start" });
}

function initHelpCenter() {
  document.querySelectorAll(".help-doc-card").forEach((card) => {
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "button");
    card.addEventListener("click", () => openHelpDocument(card.querySelector("code")?.textContent || ""));
    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openHelpDocument(card.querySelector("code")?.textContent || "");
      }
    });
  });
  document.querySelector("#help-reader-close")?.addEventListener("click", () => {
    document.querySelector("#help-doc-reader")?.classList.add("hidden");
  });
}

function setPageLoading(label = "加载中...") {
  const targets = [
    ["#summary", "加载概览..."],
    ["#platforms", "加载平台..."],
    ["#accounts-list", "加载账号..."],
    ["#tasks-list", "加载任务..."],
    ["#stats-overview", "加载统计..."],
    ["#operation-progress", "加载进度..."],
    ["#platform-settings-list", "加载设置..."],
    ["#matrix-job-status", "加载作业..."],
    ["#operation-notice-routes", "加载通知..."],
    ["#login-qr-batches", "加载登录批次..."],
    ["#supabase-health-list", "加载数据库字典..."],
  ];
  targets.forEach(([selector, text]) => {
    const node = document.querySelector(selector);
    if (node) node.innerHTML = loadingInline(text || label);
  });
  renderAiRobotLoading();
}

function setViewLoading(view) {
  const targets = {
    overview: [
      ["#summary", "加载概览..."],
      ["#platforms", "加载平台..."],
    ],
    accounts: [["#accounts-list", "加载账号..."]],
    settings: [
      ["#platform-settings-list", "加载设置..."],
      ["#matrix-job-status", "加载作业..."],
    ],
    tasks: [["#tasks-list", "加载任务..."]],
    stats: [
      ["#stats-overview", "加载统计..."],
      ["#operation-progress", "加载进度..."],
    ],
    "ai-robot": [],
    notifications: [
      ["#operation-notice-routes", "加载通知..."],
      ["#login-qr-batches", "加载登录批次..."],
    ],
    "terminal-execution": [
      ["#terminal-config-list", "加载运营微信配置..."],
    ],
    "system-settings": [["#supabase-health-list", "加载数据库字典..."]],
  };
  if (view === "ai-robot") renderAiRobotLoading();
  (targets[view] || []).forEach(([selector, text]) => {
    const node = document.querySelector(selector);
    if (node) node.innerHTML = loadingInline(text || label);
  });
  if (view === "terminal-execution") {
    document.querySelector("#terminal-init-modal")?.classList.add("hidden");
  }
}

function platformLabel(key) {
  return PLATFORM_LABELS[key] || key;
}

function platformIcon(key) {
  const logo = PLATFORM_LOGOS[key] || { icon: "simple-icons:simpleicons", bg: "#5dd62c", fg: "ffffff" };
  const src = `https://api.iconify.design/${logo.icon}.svg?color=%23${logo.fg}`;
  return `<span class="platform-logo platform-app-logo" title="${platformLabel(key)}" aria-hidden="true" style="background:${logo.bg}">
    <img src="${src}" alt="" loading="lazy" decoding="async">
  </span>`;
}

function platformLogo(key) {
  return platformIcon(key);
}

function platformName(key) {
  return `<span class="platform-name">${platformIcon(key)}<span>${platformLabel(key)}</span></span>`;
}

function platformStatusIcon(status) {
  const normalized = String(status || "unknown").toLowerCase();
  const ok = ["ready", "active", "logged_in", "success", "ok"].includes(normalized);
  const warn = ["unknown", "pending", "checking", "not_checked"].includes(normalized);
  const color = ok ? "currentColor" : warn ? "currentColor" : "currentColor";
  const path = ok
    ? '<path d="M20 6 9 17l-5-5"/>'
    : warn
      ? '<path d="M12 8v4"/><path d="M12 16h.01"/>'
      : '<path d="m15 9-6 6"/><path d="m9 9 6 6"/>';
  return `<svg class="platform-status-icon" aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="${color}" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">${path}</svg>`;
}

function platformStatusLabel(status) {
  const normalized = String(status || "unknown").toLowerCase();
  const labels = {
    active: "已启用",
    ready: "已部署",
    logged_in: "已部署",
    success: "正常",
    ok: "正常",
    login_required: "需登录",
    logged_out: "未登录",
    failed: "异常",
    error: "异常",
    pending: "待检查",
    checking: "检查中",
    not_checked: "待检查",
    unknown: "未知",
  };
  return labels[normalized] || status || "未知";
}

function platformStatusClass(status) {
  const normalized = String(status || "unknown").toLowerCase();
  if (["ready", "active", "logged_in", "success", "ok"].includes(normalized)) return "ready";
  if (["unknown", "pending", "checking", "not_checked"].includes(normalized)) return "pending";
  return "error";
}

function aiRobotLogo(platform) {
  const logo = AI_ROBOT_LOGOS[platform] || { icon: "simple-icons:simpleicons", bg: "#5dd62c", fg: "101010" };
  const src = `https://api.iconify.design/${logo.icon}.svg?color=%23${logo.fg}`;
  return `<span class="bot-logo ${platform}" title="${aiPlatformLabel(platform)}" aria-hidden="true" style="background:${logo.bg}">
    <img src="${src}" alt="" loading="lazy" decoding="async">
  </span>`;
}

function metric(label, value) {
  return `<div class="metric"><span>${label}</span><strong>${value}</strong></div>`;
}

function renderSummary() {
  const s = state.summary;
  document.querySelector("#summary").innerHTML = [
    metric("账号", s.accounts || 0),
    metric("平台槽位", s.platforms || 0),
    metric("剩余素材", s.remaining_material_videos || 0),
    metric("运行中任务", s.running_tasks || 0),
    metric("失败任务", s.failed_tasks || 0),
    metric("播放", s.views || 0),
    metric("评论", s.comments || 0),
  ].join("");
}

function abilityText(item) {
  return [
    item.can_publish ? "发布" : "",
    item.can_comment ? "评论" : "",
    item.can_message ? "私信" : "",
    item.can_login_status ? "登录检测" : "浏览器维护",
  ].filter(Boolean);
}

function renderPlatforms() {
  const grouped = { cn: [], global: [] };
  state.platforms.forEach((item) => {
    grouped[item.region === "cn" ? "cn" : "global"].push(item);
  });
  document.querySelector("#platforms").innerHTML = ["cn", "global"].map((region) => {
    const cards = grouped[region].sort((a, b) => PLATFORM_ORDER.indexOf(a.key) - PLATFORM_ORDER.indexOf(b.key));
    return `<section class="platform-region">
      <div class="region-title">${REGION_LABELS[region]}</div>
      <div class="platform-grid">
        ${cards.map((item) => `<div class="platform-card">
          <div class="row-head"><strong>${platformName(item.key)}</strong><span>${item.region === "cn" ? "国内" : "国外"}</span></div>
          <div class="chips">${abilityText(item).map((a) => `<span class="chip">${a}</span>`).join("")}</div>
        </div>`).join("")}
      </div>
    </section>`;
  }).join("");
}

function renderTaskSelects() {
  const accountSelect = document.querySelector("#task-account-select");
  accountSelect.innerHTML = state.accounts.length
    ? state.accounts.map((account) => `<option value="${account.id}">#${account.id} ${account.display_name}</option>`).join("")
    : `<option value="">请先创建账号</option>`;

  const platformSelect = document.querySelector("#task-platform-select");
  const groupedOptions = ["cn", "global"].map((region) => {
    const options = state.platforms
      .filter((item) => (item.region === "cn") === (region === "cn"))
      .sort((a, b) => PLATFORM_ORDER.indexOf(a.key) - PLATFORM_ORDER.indexOf(b.key))
      .map((item) => `<option value="${item.key}">${platformLabel(item.key)}</option>`)
      .join("");
    return `<optgroup label="${REGION_LABELS[region]}">${options}</optgroup>`;
  }).join("");
  platformSelect.innerHTML = groupedOptions;
}

function renderPlatformStatusGroup(platforms, region) {
  const items = platforms
    .filter((p) => {
      const platform = state.platforms.find((item) => item.key === p.platform);
      return platform && (platform.region === "cn") === (region === "cn");
    })
    .sort((a, b) => PLATFORM_ORDER.indexOf(a.platform) - PLATFORM_ORDER.indexOf(b.platform));
  return `<div class="account-platform-group">
    <div class="region-title compact">${REGION_LABELS[region]}</div>
    <div class="browser-actions">
      ${items.map((p) => `<button class="btn secondary platform-open-btn" data-open="${p.account_id}:${p.platform}">${platformIcon(p.platform)}<span>${platformLabel(p.platform)}</span><span class="platform-inline-status ${platformStatusClass(p.login_status)}">${platformStatusIcon(p.login_status)}${platformStatusLabel(p.login_status)}</span></button>`).join("")}
    </div>
  </div>`;
}

function accountOperatorWechat(account) {
  const match = String(account?.notes || "").match(/绑定运营微信：([^；;]+)/);
  return match ? match[1].trim() : "";
}

function renderAccounts() {
  document.querySelector("#accounts-list").innerHTML = state.accounts.map((account) => {
    const platforms = account.platforms || [];
    const operatorWechat = accountOperatorWechat(account);
    return `<article class="account-row">
      <div class="row-head">
        <div class="account-title-wrap">
          <strong class="account-title">#${account.id} ${account.display_name}</strong>
          <div class="account-subtitle">${account.account_key} · ${account.niche || ""}</div>
          <div class="account-operator-wechat">绑定运营微信：<strong>${operatorWechat || "-"}</strong></div>
        </div>
        <div class="account-badges">
          <span class="chip">${account.status}</span>
          <span class="chip success-chip">发布成功 ${account.publish_success_count || 0}</span>
          <button class="btn ghost btn-sm danger-action" type="button" data-delete-account="${account.id}" data-account-name="${account.display_name}">删除账号</button>
        </div>
      </div>
      ${renderPlatformStatusGroup(platforms, "cn")}
      ${renderPlatformStatusGroup(platforms, "global")}
    </article>`;
  }).join("") || `<div class="muted">暂无账号</div>`;
}

function taskTypeLabel(type) {
  return TASK_TYPE_OPTIONS.find(([value]) => value === type)?.[1] || type || "未指定";
}

function taskAccountLabel(task) {
  const accountId = Number(task.account_id || 0);
  const account = state.accounts.find((item) => Number(item.id) === accountId);
  return account
    ? `#${account.id} ${account.display_name || account.account_key || "未命名账号"}`
    : (accountId ? `#${accountId} 未知账号` : "未指定账号");
}

function filteredTasks() {
  return state.tasks.filter((task) => {
    const accountId = String(task.account_id || "");
    return (!taskFilters.account || accountId === taskFilters.account)
      && (!taskFilters.platform || task.platform === taskFilters.platform)
      && (!taskFilters.status || task.status === taskFilters.status)
      && (!taskFilters.taskType || task.task_type === taskFilters.taskType);
  });
}

function taskFilterOptions(items, valueGetter, labelGetter) {
  const seen = new Set();
  return items.map((item) => {
    const value = String(valueGetter(item) || "");
    if (!value || seen.has(value)) return "";
    seen.add(value);
    return `<option value="${value}">${labelGetter(item)}</option>`;
  }).join("");
}

function taskAccountFilterOptions() {
  const seen = new Set();
  const accountOptions = state.accounts.map((account) => {
    const value = String(account.id || "");
    if (!value || seen.has(value)) return "";
    seen.add(value);
    return `<option value="${value}">#${account.id} ${account.display_name || account.account_key || "Unnamed Account"}</option>`;
  }).join("");
  const taskOnlyOptions = state.tasks.map((task) => {
    const value = String(task.account_id || "");
    if (!value || seen.has(value)) return "";
    seen.add(value);
    return `<option value="${value}">${taskAccountLabel(task)}</option>`;
  }).join("");
  return accountOptions + taskOnlyOptions;
}

function renderTasks() {
  const list = filteredTasks();
  const visibleIds = list.map((task) => Number(task.id));
  Array.from(taskSelection).forEach((id) => {
    if (!state.tasks.some((task) => Number(task.id) === Number(id))) taskSelection.delete(id);
  });
  const selectedVisible = visibleIds.filter((id) => taskSelection.has(id));
  document.querySelector("#tasks-list").innerHTML = `
    <div class="task-toolbar">
      <div class="task-filter-grid">
        <label>账号<select data-task-filter="account"><option value="">全部账号</option>${taskAccountFilterOptions()}</select></label>
        <label>平台<select data-task-filter="platform"><option value="">全部平台</option>${taskFilterOptions(state.tasks, (task) => task.platform, (task) => platformLabel(task.platform))}</select></label>
        <label>状态<select data-task-filter="status"><option value="">全部状态</option>${taskFilterOptions(state.tasks, (task) => task.status, (task) => formatFriendlyMessage(task.status))}</select></label>
        <label>任务类型<select data-task-filter="taskType"><option value="">全部类型</option>${TASK_TYPE_OPTIONS.map(([value, label]) => `<option value="${value}">${label}</option>`).join("")}</select></label>
      </div>
      <div class="task-bulk-actions">
        <label class="task-check-all"><input type="checkbox" data-task-select-all ${visibleIds.length && selectedVisible.length === visibleIds.length ? "checked" : ""}>全选</label>
        <span class="muted">已选 ${taskSelection.size} 条</span>
        <button class="btn secondary btn-sm" type="button" data-task-bulk-status="paused" ${taskSelection.size ? "" : "disabled"}>暂停队列</button>
        <button class="btn secondary btn-sm danger-action" type="button" data-task-bulk-delete ${taskSelection.size ? "" : "disabled"}>删除队列</button>
      </div>
    </div>
    ${list.map((task) => {
      const accountLabel = taskAccountLabel(task);
      return `<article class="task-row">
      <div class="row-head">
        <label class="task-row-check"><input type="checkbox" data-task-select="${task.id}" ${taskSelection.has(Number(task.id)) ? "checked" : ""}></label>
        <div class="task-title-wrap">
          <strong>#${task.id} ${platformName(task.platform)}</strong>
          <span class="task-account-name">${accountLabel}</span>
        </div>
        <span class="task-actions">
          <span class="status-${task.status}">${formatFriendlyMessage(task.status)}</span>
          <button class="btn secondary task-delete" data-delete-task="${task.id}" type="button">删除队列</button>
        </span>
      </div>
      <div class="task-meta">
        <span>任务类型：${taskTypeLabel(task.task_type)}</span>
        <span>账号：${accountLabel}</span>
        <span>平台：${platformLabel(task.platform)}</span>
      </div>
      <div class="muted">${formatFriendlyMessage(task.summary || task.error || "")}</div>
    </article>`;
    }).join("") || `<div class="muted">暂无匹配任务</div>`}
  `;
  document.querySelectorAll("[data-task-filter]").forEach((select) => {
    select.value = taskFilters[select.dataset.taskFilter] || "";
  });
}

function terminalColorByIndex(index, terminalState = state.terminalExecution) {
  const colors = terminalState?.colors || [];
  return colors[index % Math.max(1, colors.length)] || { hex: "#3B82F6", name: "科技蓝" };
}

function readTerminalConfigRows(rootTarget = "#terminal-config-list") {
  const root = typeof rootTarget === "string" ? document.querySelector(rootTarget) : rootTarget;
  if (!root) return [];
  return Array.from(root.querySelectorAll("[data-terminal-config-row]")).map((row) => {
    const activeSwatch = row.querySelector(".terminal-swatch.active");
    return {
      id: Number(row.dataset.terminalConfigRow || 0),
      enabled: Boolean(row.querySelector("[data-terminal-enabled]")?.checked),
      operator_wechat: row.querySelector("[data-terminal-operator]")?.value || "",
      color: activeSwatch?.dataset.terminalColor || terminalColorByIndex(0).hex,
    };
  });
}

function installTerminalConfigInteractions(rootTarget = "#terminal-config-list") {
  const root = typeof rootTarget === "string" ? document.querySelector(rootTarget) : rootTarget;
  if (!root || root.dataset.terminalConfigBound === "true") return;
  root.dataset.terminalConfigBound = "true";
  root.addEventListener("change", (event) => {
    const row = event.target.closest("[data-terminal-config-row]");
    if (!row) return;
    const enabled = event.target.matches("[data-terminal-enabled]") ? event.target.checked : row.querySelector("[data-terminal-enabled]")?.checked;
    row.classList.toggle("disabled", !enabled);
    row.querySelectorAll("[data-terminal-color]").forEach((button) => button.toggleAttribute("disabled", !enabled));
    row.querySelector("[data-terminal-operator]")?.toggleAttribute("disabled", !enabled);
  });
  root.addEventListener("click", (event) => {
    const swatch = event.target.closest("[data-terminal-color]");
    if (!swatch) return;
    const row = swatch.closest("[data-terminal-config-row]");
    row?.querySelectorAll("[data-terminal-color]").forEach((button) => button.classList.remove("active"));
    swatch.classList.add("active");
  });
}

function renderTerminalConfig(rootTarget = "#terminal-config-list", terminalState = state.terminalExecution) {
  const list = typeof rootTarget === "string" ? document.querySelector(rootTarget) : rootTarget;
  if (!list) return;
  const platform = String(terminalState?.active_platform || terminalActivePlatform());
  if (platform !== "wechat") {
    list.innerHTML = `
      <div class="terminal-config-row">
        <div class="terminal-config-left">
          <span>当前平台仅展示按需检测配置</span>
        </div>
        <div class="terminal-session-card-body">
          <div>该平台不使用视频号矩阵窗位、运营微信绑定和二维码队列。</div>
          <div>请在页面顶部切换平台后，再通过“当前平台配置”查看对应字段组。</div>
        </div>
      </div>
    `;
    installTerminalConfigInteractions(list);
    return;
  }
  const operators = terminalState?.operators || [];
  const colors = terminalState?.colors || [];
  const savedRows = terminalState?.config || [];
  const defaultRows = Array.from({ length: 5 }, (_, index) => ({
    id: index + 1,
    enabled: false,
    operator_wechat: operators[index]?.operator_wechat || operators[index % Math.max(1, operators.length)]?.operator_wechat || "",
    color: terminalColorByIndex(index, terminalState).hex,
  })).map((row, index) => {
    const saved = savedRows[index] || {};
    return {
      ...row,
      id: Number(saved.id || row.id || index + 1),
      enabled: typeof saved.enabled === "boolean" ? saved.enabled : row.enabled,
      operator_wechat: String(saved.operator_wechat || row.operator_wechat || ""),
      color: String(saved.color || row.color || terminalColorByIndex(index, terminalState).hex),
    };
  });
  if (!defaultRows.length) {
    list.innerHTML = `
      <div class="terminal-empty-state">
        <strong>暂无可编辑配置</strong>
        <p class="muted">请先确认已加载终端数据，或切换到视频号路由后重新打开配置。</p>
      </div>
    `;
    installTerminalConfigInteractions(list);
    return;
  }
  list.innerHTML = defaultRows.map((row) => `
    <div class="terminal-config-row ${row.enabled ? "" : "disabled"}" data-terminal-config-row="${row.id}">
      <label class="terminal-config-left">
        <input type="checkbox" class="terminal-checkbox" data-terminal-enabled ${row.enabled ? "checked" : ""}>
        <span>终端 ${String(row.id).padStart(2, "0")}</span>
      </label>
      <select class="terminal-wx-select" data-terminal-operator ${row.enabled ? "" : "disabled"}>
        ${operators.map((operator) => `<option value="${operator.operator_wechat}" ${operator.operator_wechat === row.operator_wechat ? "selected" : ""}>${operator.operator_wechat}</option>`).join("") || `<option value="">暂无绑定运营微信</option>`}
      </select>
      <div class="terminal-swatch-group">
        ${colors.map((color) => `
          <button class="terminal-swatch ${color.hex === row.color ? "active" : ""}" type="button" data-terminal-color="${color.hex}" title="${color.name}" style="background:${color.hex};color:${color.hex}" ${row.enabled ? "" : "disabled"}></button>
        `).join("")}
      </div>
    </div>
  `).join("");
  installTerminalConfigInteractions(list);
}

function renderSettingsPlatformPublishConfig() {
  const list = document.querySelector("#settings-platform-config-list");
  if (!list) return;
  const terminalState = state.terminalExecution || {};
  if (!terminalState.colors?.length && !terminalState.operators?.length && !terminalState.config?.length) {
    list.innerHTML = `<div class="loading-inline"><span class="btn-spinner" aria-hidden="true"></span><span>加载平台发布配置...</span></div>`;
    return;
  }
  renderTerminalConfig(list, terminalState);
}

function renderSettingsCardMode() {
  const publishWindowContent = document.querySelector("#settings-publish-window-content");
  publishWindowContent?.classList.remove("hidden");
}

function renderTerminalInitCardMode() {
  const windowContent = document.querySelector("#terminal-init-window-content");
  const platformContent = document.querySelector("#terminal-init-platform-content");
  document.querySelectorAll("[data-terminal-init-card]").forEach((button) => {
    const active = button.dataset.terminalInitCard === currentTerminalInitCard;
    button.classList.toggle("active", active);
  });
  windowContent?.classList.toggle("hidden", currentTerminalInitCard !== "window");
  platformContent?.classList.toggle("hidden", currentTerminalInitCard !== "platform");
}

function terminalCurrentPlatformKey() {
  const route = terminalCurrentRoute();
  if (PLATFORM_ORDER.includes(route)) return route;
  const active = String(state.terminalExecution?.active_platform || "");
  if (PLATFORM_ORDER.includes(active)) return active;
  return "wechat";
}

function collectTerminalPlatformSetting(root, platform) {
  const get = (name, fallback = "") => root.querySelector(`[name="${name}"]`)?.value ?? fallback;
  const payload = {
    enabled: get(`platforms.${platform}.enabled`, "true") === "true",
    content_type: get(`platforms.${platform}.content_type`, "short_video"),
    publish_mode: get(`platforms.${platform}.publish_mode`, "inherit"),
    visibility: get(`platforms.${platform}.visibility`, "public"),
    comment_permission: get(`platforms.${platform}.comment_permission`, "public"),
    caption: get(`platforms.${platform}.caption`, ""),
    upload_timeout: Number(state.distributionSettings?.common?.upload_timeout || 60),
  };
  if (platform === "wechat") {
    const shortTitleMode = get("platforms.wechat.short_title_mode", "custom");
    const locationMode = get("platforms.wechat.location_mode", "custom");
    const captionMode = get("platforms.wechat.caption_mode", "custom");
    payload.collection_name = get("platforms.wechat.collection_name", "");
    payload.declare_original = get("platforms.wechat.declare_original", "inherit");
    payload.short_title = shortTitleMode === "inherit" ? "inherit" : get("platforms.wechat.short_title", "GasGx燃气发电挖矿");
    payload.location = locationMode === "inherit" ? "inherit" : get("platforms.wechat.location", "");
    payload.caption = captionMode === "inherit" ? "inherit" : get("platforms.wechat.caption", "");
  }
  return payload;
}

async function renderTerminalPlatformPublishPanel() {
  const host = document.querySelector("#terminal-platform-publish-list");
  if (!host) return;
  if (!state.distributionSettings?.platforms) {
    state.distributionSettings = await api("/api/settings/distribution");
  }
  const key = terminalCurrentPlatformKey();
  const platform = (state.platforms || []).find((item) => item.key === key) || {
    key,
    label: platformName(key),
    region: "cn",
  };
  host.innerHTML = `
    <section class="platform-settings-region">
      <div class="region-title">当前平台：${platformName(platform.key)}</div>
      <div class="platform-settings-grid">${renderPlatformSettingsCard(platform)}</div>
    </section>
  `;
  syncWechatInheritModeInputs(host);
}

function setSettingsCardMode(card) {
  currentSettingsCard = "publish-window";
  localStorage.setItem(SETTINGS_CARD_KEY, "publish-window");
  renderSettingsCardMode();
}

function terminalPlaceholderIcon() {
  return `<svg width="112" height="112" viewBox="0 0 112 112" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="等待开始登录">
    <rect x="22" y="18" width="68" height="76" rx="12" fill="var(--terminal-glass-bg)" stroke="var(--term-color)" stroke-opacity=".7" stroke-width="2"/>
    <rect x="34" y="30" width="16" height="16" rx="3" stroke="var(--term-color)" stroke-width="2"/>
    <rect x="62" y="30" width="16" height="16" rx="3" stroke="var(--term-color)" stroke-width="2"/>
    <rect x="34" y="58" width="16" height="16" rx="3" stroke="var(--term-color)" stroke-width="2"/>
    <path d="M63 58h15v15M63 74h6M78 82h-8M54 84h-8" stroke="var(--term-color)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
    <path d="M38 90h36" stroke="var(--terminal-text-sub)" stroke-width="2" stroke-linecap="round"/>
    <circle cx="56" cy="56" r="44" stroke="var(--term-color)" stroke-opacity=".18" stroke-width="2"/>
  </svg>`;
}

function terminalExpiredPlaceholderIcon() {
  return `<svg width="112" height="112" viewBox="0 0 112 112" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="二维码已过期">
    <rect x="22" y="18" width="68" height="76" rx="12" fill="#f5f7fa" stroke="var(--term-color)" stroke-opacity=".45" stroke-width="2" stroke-dasharray="5 4"/>
    <path d="M34 34h44M34 52h44M34 70h44" stroke="#94a3b8" stroke-opacity=".7" stroke-width="2" stroke-linecap="round"/>
    <path d="M38 30l36 36M74 30L38 66" stroke="#ef4444" stroke-opacity=".55" stroke-width="3" stroke-linecap="round"/>
    <text x="56" y="88" text-anchor="middle" fill="#ef4444" font-size="12" font-weight="700">已过期</text>
  </svg>`;
}

function terminalQrLifecycle(window) {
  const refreshing = Boolean(window?.qr_refreshing);
  if (refreshing) {
    return {
      hasQr: false,
      expiresAt: 0,
      remaining: 0,
      expired: false,
      active: false,
      countdownText: "待获取",
      placeholderState: "idle",
    };
  }
  const hasQr = Boolean(window?.qr_url);
  let expiresAt = Number(window?.qr_expires_at || 0);
  if (hasQr && expiresAt <= 0) {
    expiresAt = Date.now() / 1000 + 60;
    window.qr_expires_at = expiresAt;
  }
  const now = Date.now() / 1000;
  const remaining = hasQr && expiresAt > 0 ? Math.max(0, expiresAt - now) : 0;
  const expired = hasQr && expiresAt > 0 && remaining <= 0;
  return {
    hasQr,
    expiresAt,
    remaining,
    expired,
    active: hasQr && !expired,
    countdownText: !hasQr ? "待获取" : expired ? "已过期" : `${remaining.toFixed(2)}s`,
    placeholderState: !hasQr ? "idle" : expired ? "expired" : "active",
  };
}

function terminalWindowActionButtons(window, current, loginStarted) {
  const accounts = window.accounts || [];
  const currentIndex = Number(window.current_index || 0);
  const hasCurrent = Boolean(current && current.id && currentIndex < accounts.length);
  const run = window?.publish_run || {};
  const runStatus = String(run?.status || "").toLowerCase();
  const runActiveForCurrent = Number(run?.account_id || 0) === Number(current?.id || 0);
  const publishRunning = runActiveForCurrent && runStatus === "running";
  const publishSucceeded = runActiveForCurrent && runStatus === "success";
  const qrState = terminalQrLifecycle(window);
  const isSuccess = String(current?.status || "") === "success";
  const canPublish = loginStarted && hasCurrent && qrState.active && !publishRunning && !publishSucceeded && !isSuccess;
  const canConfirm = loginStarted && hasCurrent && publishSucceeded && !isSuccess;
  const hasNext = currentIndex + 1 < accounts.length;
  const publishLabel = !hasCurrent
    ? "全部完成"
    : publishRunning
      ? "发布中"
      : publishSucceeded
        ? "已完成待确认"
        : qrState.active
          ? "发布"
          : "先获取二维码";
  const confirmLabel = hasNext ? "发布成功，下一账号" : "发布成功，完成";
  return `
    <div class="terminal-window-actions">
      <button class="terminal-col-btn" type="button" data-terminal-manual="${window.id}" ${canPublish ? "" : "disabled"}>${publishLabel}</button>
      <button class="terminal-col-btn secondary" type="button" data-terminal-confirm-success="${window.id}" ${canConfirm ? "" : "disabled"}>${confirmLabel}</button>
    </div>
  `;
}

function terminalQrImageMarkup(window, currentAccountId) {
  const qrState = terminalQrLifecycle(window);
  const qrUrl = window.qr_url || "";
  const fallbackCurrentId = Number(window?.accounts?.[Number(window?.current_index || 0)]?.id || 0);
  const refreshAccountId = Number(currentAccountId || 0) || fallbackCurrentId;
  if (!qrState.hasQr) {
    return `
      <button class="terminal-qr-image-button" type="button" data-terminal-qr-refresh="${window.id}:${refreshAccountId}" aria-label="点击获取二维码">
        ${terminalPlaceholderIcon()}
      </button>
    `;
  }
  if (qrState.expired) {
    return `
      <button class="terminal-qr-image-button expired" type="button" data-terminal-qr-refresh="${window.id}:${refreshAccountId}" aria-label="二维码已过期，点击刷新">
        ${terminalExpiredPlaceholderIcon()}
      </button>
    `;
  }
  return `
    <button class="terminal-qr-image-button" type="button" data-terminal-qr-refresh="${window.id}:${refreshAccountId}">
      <img src="${qrUrl}" alt="视频号登录二维码">
    </button>
  `;
}

function terminalWechatAccountStatusText(window, account, index, currentIndex, loginStarted) {
  const qrState = terminalQrLifecycle(window);
  if (index === currentIndex && loginStarted && String(account.status || "") === "waiting_qr") {
    if (qrState.hasQr) {
      return qrState.expired
        ? `二维码已过期，请刷新`
        : `正在等待扫码确认`;
    }
    return `正在等待扫码确认`;
  }
  return account.status_text || "未登录";
}

function terminalAccountStatusToken(account) {
  const status = String(account?.status || "").toLowerCase();
  if (status === "success") return "success";
  if (status === "error" || status === "failed") return "error";
  if (status === "waiting_qr" || status === "opening" || status === "running") return "waiting";
  if (status === "pending") return "pending";
  return "idle";
}

function terminalAccountStatusAvatar(account) {
  const token = terminalAccountStatusToken(account);
  const icon = token === "success"
    ? `<svg viewBox="0 0 24 24" role="img" aria-label="发布成功"><path d="M9.6 16.8 5.9 13.1l-1.4 1.4 5.1 5.1L19.5 9.7l-1.4-1.4z"/></svg>`
    : token === "error"
      ? `<svg viewBox="0 0 24 24" role="img" aria-label="状态异常"><path d="M12 3 2.5 20h19zM11 9h2v6h-2zm0 7h2v2h-2z"/></svg>`
      : token === "waiting"
        ? `<svg viewBox="0 0 24 24" role="img" aria-label="等待扫码"><path d="M12 2a10 10 0 1 0 10 10h-2a8 8 0 1 1-8-8z"/><path d="M11 6h2v7h5v2h-7z"/></svg>`
        : token === "pending"
          ? `<svg viewBox="0 0 24 24" role="img" aria-label="待处理"><path d="M12 4a8 8 0 1 0 8 8h-2a6 6 0 1 1-6-6z"/></svg>`
          : `<svg viewBox="0 0 24 24" role="img" aria-label="未登录"><path d="M12 5a7 7 0 1 0 7 7h-2a5 5 0 1 1-5-5z"/></svg>`;
  return `<div class="terminal-avatar terminal-avatar-${token}">${icon}</div>`;
}

function terminalAccountTaskBadge(account) {
  const taskId = String(account?.task_id || "").trim();
  if (String(account?.status || "") === "success") {
    return `<div class="terminal-status-badge success">发布成功</div>`;
  }
  if (taskId) {
    return `<div class="terminal-status-badge">任务:${taskId}</div>`;
  }
  return "";
}

function terminalWechatWindowMarkup(window, loginStarted) {
  const accounts = window.accounts || [];
  const currentIndex = Number(window.current_index || 0);
  const color = window.color || "#3B82F6";
  const colorDim = `${color}33`;
  const successCount = accounts.filter((account) => account.status === "success").length;
  const current = accounts[currentIndex] || {};
  const qrState = terminalQrLifecycle(window);
  return `
    <div class="terminal-task-column terminal-glass" data-terminal-window-id="${window.id}" style="--term-color:${color};--term-color-dim:${colorDim}">
      <div class="terminal-color-anchor"></div>
      <div class="terminal-col-header">
        <div class="terminal-col-header-top">
          <span style="font-weight:700;font-size:16px;">终端执行窗 ${String(window.id).padStart(2, "0")}</span>
          <span class="terminal-status-badge theme">色标: ${window.color_name || ""}</span>
        </div>
        <div class="terminal-wx-operator">运营微信: ${window.operator_wechat || "-"}</div>
      </div>
      <div class="terminal-qr-section" data-terminal-qr-section="${window.id}" data-terminal-qr-state="${qrState.placeholderState}">
        <div class="terminal-qr-placeholder" data-terminal-qr-placeholder="${window.id}" data-terminal-qr-placeholder-state="${qrState.placeholderState}">${terminalQrImageMarkup(window, current.id)}</div>
        <div class="terminal-qr-status-row"><span class="terminal-qr-sequence terminal-qr-countdown ${qrState.expired ? "expired" : qrState.active ? "active" : "idle"}" data-terminal-qr-countdown="${window.id}">${qrState.countdownText}</span></div>
      </div>
      <div class="terminal-account-list">
        ${accounts.map((account, index) => `
          <div class="terminal-account-item ${index === currentIndex ? "active" : ""}">
            <div class="terminal-account-info">
              ${terminalAccountStatusAvatar(account)}
              <div>
                <div class="terminal-acc-name">${account.display_name || account.account_key || `账号 ${account.id}`}</div>
                <div class="terminal-acc-status" ${index === currentIndex ? `data-terminal-current-status="${window.id}"` : ""}>${terminalWechatAccountStatusText(window, account, index, currentIndex, loginStarted)}</div>
              </div>
            </div>
            ${terminalAccountTaskBadge(account)}
          </div>
        `).join("") || `<div class="muted">暂无账号</div>`}
      </div>
      <div class="terminal-col-footer">
        <div class="terminal-progress-bar"><div class="terminal-progress-fill" style="width:${accounts.length ? Math.round((successCount / accounts.length) * 100) : 0}%;"></div></div>
        ${terminalWindowActionButtons(window, current, loginStarted)}
      </div>
    </div>
  `;
}

function syncTerminalWechatSummary(summary, windows) {
  const summaryNode = document.querySelector(".terminal-wechat-summary");
  if (summaryNode) {
    summaryNode.innerHTML = `
      <div class="metric"><span>已完成账号数</span><strong>${summary.success || 0}</strong></div>
      <div class="metric"><span>总账号数</span><strong>${summary.total || 0}</strong></div>
      <div class="metric"><span>活跃窗数量</span><strong>${summary.active_windows || 0}</strong></div>
    `;
  }
  const progress = document.querySelector("#terminal-global-progress");
  if (progress) progress.textContent = `${summary.success || 0}/${summary.total || 0}`;
  const active = document.querySelector("#terminal-active-windows");
  if (active) active.textContent = String(summary.active_windows || windows.length || 0);
}

function waitForTerminalQrVisible(windowId, accountId, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve) => {
    const tick = () => {
      const button = document.querySelector(`[data-terminal-account-qr="${windowId}:${accountId}"]`);
      const windowNode = button?.closest(".terminal-task-column");
      const currentText = windowNode?.querySelector(".terminal-qr-section img[alt='视频号登录二维码']");
      if (currentText?.getAttribute("src")) {
        const img = currentText;
        if (img.complete && img.naturalWidth > 0) {
          resolve(true);
          return;
        }
        img.addEventListener("load", () => resolve(true), { once: true });
        img.addEventListener("error", () => resolve(false), { once: true });
        return;
      }
      if (Date.now() >= deadline) {
        resolve(false);
        return;
      }
      window.requestAnimationFrame(tick);
    };
    tick();
  });
}

async function refreshTerminalAccountQr(windowId, accountId, button) {
  const restoreButton = setButtonLoading(button, "刷新中");
  terminalErrorModalSignature = "";
  hideTerminalErrorModal();
  try {
    const refreshRoot = document.querySelector(".terminal-workspace-wechat") || document.querySelector("#terminal-matrix-workspace");
    const targetNode = refreshRoot?.querySelector(`[data-terminal-window-id="${windowId}"]`);
    const pageScrollX = window.scrollX;
    const pageScrollY = window.scrollY;
    const rootScrollLeft = refreshRoot?.scrollLeft ?? 0;
    const rootScrollTop = refreshRoot?.scrollTop ?? 0;
    targetNode?.querySelectorAll(".terminal-qr-placeholder img[alt='视频号登录二维码']").forEach((img) => img.remove());
    const currentStateWindows = state.terminalExecution.windows || [];
    const pendingWindow = currentStateWindows.find((item) => String(item.id) === String(windowId));
    if (pendingWindow) {
      pendingWindow.qr_refreshing = true;
      pendingWindow.qr_url = "";
      pendingWindow.qr_expires_at = 0;
      if (refreshRoot && targetNode) {
        const loginStartedPending = Boolean(state.terminalExecution.login_started);
        targetNode.outerHTML = terminalWechatWindowMarkup(pendingWindow, loginStartedPending);
        syncTerminalWechatSummary(state.terminalExecution.summary || {}, currentStateWindows);
        refreshRoot.scrollLeft = rootScrollLeft;
        refreshRoot.scrollTop = rootScrollTop;
        window.scrollTo(pageScrollX, pageScrollY);
      } else {
        renderTerminalExecution();
      }
    }

    const nextState = await api(`/api/terminal-execution/windows/${windowId}/accounts/${accountId}/qr`, { method: "POST" });
    state.terminalExecution = nextState;
    const now = Date.now() / 1000;
    (state.terminalExecution.windows || []).forEach((item) => {
      if (String(item.id) === String(windowId)) {
        item.qr_refreshing = false;
        if (item.qr_url) item.qr_expires_at = now + 60;
      }
    });
    const loginStarted = Boolean(nextState.login_started);
    const targetWindow = (nextState.windows || []).find((item) => String(item.id) === String(windowId));
    if (targetWindow) {
      targetWindow.qr_refreshing = false;
      if (targetWindow.qr_url) targetWindow.qr_expires_at = now + 60;
    }
    if (refreshRoot && targetNode && targetWindow) {
      targetNode.outerHTML = terminalWechatWindowMarkup(targetWindow, loginStarted);
      syncTerminalWechatSummary(nextState.summary || {}, nextState.windows || []);
      refreshRoot.scrollLeft = rootScrollLeft;
      refreshRoot.scrollTop = rootScrollTop;
      window.scrollTo(pageScrollX, pageScrollY);
      startTerminalPolling();
    } else {
      renderTerminalExecution();
      window.scrollTo(pageScrollX, pageScrollY);
    }
  } catch (error) {
    const currentStateWindows = state.terminalExecution.windows || [];
    const pendingWindow = currentStateWindows.find((item) => String(item.id) === String(windowId));
    if (pendingWindow) {
      pendingWindow.qr_refreshing = false;
    }
    renderTerminalExecution();
    showTerminalErrorModal({
      stage: "qr",
      title: "二维码刷新失败",
      message: error.message || "二维码刷新失败",
      context: `窗口 #${windowId} · 账号 #${accountId}`,
      signature: `qr-network|${windowId}|${accountId}|${error.message || "unknown"}`,
    });
  } finally {
    restoreButton();
  }
}

function installGlobalButtonLoading() {
  document.addEventListener("click", (event) => {
    const button = event.target.closest("button");
    if (!button || button.disabled || button.classList.contains("loading")) return;
    if (button.dataset.noGlobalLoading === "1") return;
    if (button.matches("[data-terminal-manual], [data-terminal-confirm-success], [data-terminal-qr-refresh], [data-open], [data-delete-account], [data-task-bulk-status], [data-task-bulk-delete], [data-task-select-all], [data-task-select], [data-notice-route]")) return;
    pulseButtonLoading(button, "处理中");
  }, true);
}

function renderTerminalExecution() {
  renderTerminalConfig();
  const windows = state.terminalExecution.windows || [];
  const summary = state.terminalExecution.summary || {};
  const loadError = state.terminalExecution.error || "";
  const loginStarted = Boolean(state.terminalExecution.login_started);
  document.querySelector("#terminal-init-modal")?.classList.toggle("hidden", !state.terminalConfigOpen);
  const startLoginButton = document.querySelector("#terminal-start-login");
  if (startLoginButton) {
    startLoginButton.disabled = false;
    startLoginButton.textContent = "获取登录二维码";
  }
  const progress = document.querySelector("#terminal-global-progress");
  if (progress) progress.textContent = `${summary.success || 0}/${summary.total || 0}`;
  const active = document.querySelector("#terminal-active-windows");
  if (active) active.textContent = String(summary.active_windows || windows.length || 0);
  const workspace = document.querySelector("#terminal-matrix-workspace");
  if (!workspace) return;
  workspace.innerHTML = windows.map((window) => {
    const accounts = window.accounts || [];
    const currentIndex = Number(window.current_index || 0);
    const color = window.color || "#3B82F6";
    const colorDim = `${color}33`;
    const successCount = accounts.filter((account) => account.status === "success").length;
    const current = accounts[currentIndex] || {};
    const manualWait = loginStarted ? Math.max(0, Number(window.manual_available_at || 0) - Math.floor(Date.now() / 1000)) : 0;
    const qrVisible = loginStarted && window.qr_url;
    return `
      <div class="terminal-task-column terminal-glass" style="--term-color:${color};--term-color-dim:${colorDim}">
        <div class="terminal-color-anchor"></div>
        <div class="terminal-col-header">
          <div class="terminal-col-header-top">
            <span style="font-weight:700;font-size:16px;">终端执行窗 ${String(window.id).padStart(2, "0")}</span>
            <span class="terminal-status-badge theme">色标: ${window.color_name || ""}</span>
          </div>
          <div class="terminal-wx-operator">运营微信: ${window.operator_wechat || "-"}</div>
        </div>
        <div class="terminal-qr-section">
          <div class="terminal-qr-placeholder">${terminalQrImageMarkup(window, qrVisible, current.id)}</div>
          <div class="terminal-qr-status-row"><span class="terminal-qr-sequence">#${Number(window.qr_sequence || 0)}</span></div>
        </div>
        <div class="terminal-account-list">
          ${accounts.map((account, index) => `
            <div class="terminal-account-item ${index === currentIndex ? "active" : ""}">
              <div class="terminal-account-info">
                ${terminalAccountStatusAvatar(account)}
                <div>
                  <div class="terminal-acc-name">${account.display_name || account.account_key || `账号 ${account.id}`}</div>
                  <div class="terminal-acc-status">${terminalWechatAccountStatusText(window, account, index, currentIndex, loginStarted)}</div>
                </div>
              </div>
            ${terminalAccountTaskBadge(account)}
          </div>
        `).join("") || `<div class="muted">暂无账号</div>`}
        </div>
        <div class="terminal-col-footer">
          <div class="terminal-progress-bar"><div class="terminal-progress-fill" style="width:${accounts.length ? Math.round((successCount / accounts.length) * 100) : 0}%;"></div></div>
          ${terminalWindowActionButtons(window, current, loginStarted)}
        </div>
      </div>
    `;
  }).join("");
}

function terminalPlatformPolicyLabel(policy) {
  return policy === "daily_qr" ? "每日登录" : "会话长期有效";
}

function terminalPlatformList() {
  const capabilities = state.terminalExecution.platform_capabilities || {};
  return Object.entries(capabilities).map(([key, item]) => ({
    key,
    label: item?.label || platformName(key),
    sessionPolicy: item?.sessionPolicy || "persistent",
    openUrl: item?.openUrl || "",
  }));
}

function terminalActivePlatform() {
  return String(state.terminalExecution.active_platform || "wechat");
}

function terminalPlatformContext() {
  const platform = terminalActivePlatform();
  const capability = state.terminalExecution.platform_capabilities?.[platform] || {};
  const profile = state.terminalExecution.profile_by_platform?.[platform] || {};
  const sessionPolicy = capability.sessionPolicy || profile.sessionPolicy || (platform === "wechat" ? "daily_qr" : "persistent");
  return {
    platform,
    label: capability.label || platformName(platform),
    sessionPolicy,
    openUrl: capability.openUrl || profile.openUrl || "",
    profile,
    capability,
  };
}

function renderTerminalConfigPanel() {
  const panel = document.querySelector("#terminal-platform-config-panel");
  if (!panel) return;
  const context = terminalPlatformContext();
  const isWechat = context.platform === "wechat";
  panel.classList.toggle("hidden", false);
  panel.innerHTML = isWechat
    ? `
      <div class="terminal-config-panel-head">
        <div class="terminal-config-panel-head-copy">
          <strong>当前平台配置：视频号</strong>
          <span class="terminal-status-badge theme">每日登录</span>
        </div>
        <button class="terminal-close-btn" type="button" data-terminal-close-config aria-label="关闭前置配置区">×</button>
      </div>
      <div class="terminal-config-panel-body">
        <div>只渲染视频号所需字段组：运营微信、色标、窗口启用。</div>
        <div>切换到其它平台后，会改为长会话配置/检测视图，不复用二维码占位。</div>
      </div>
      <div class="terminal-config-panel-actions">
        <button class="btn primary" type="button" id="terminal-save-config">更新配置</button>
      </div>
    `
    : `
      <div class="terminal-config-panel-head">
        <div class="terminal-config-panel-head-copy">
          <strong>当前平台配置：${context.label}</strong>
          <span class="terminal-status-badge theme">按需检测</span>
        </div>
        <button class="terminal-close-btn" type="button" data-terminal-close-config aria-label="关闭前置配置区">×</button>
      </div>
      <div class="terminal-config-panel-body">
        <div>只展示该平台需要的配置字段，缺失字段不渲染。</div>
        <div>入口：${context.openUrl || "-"}</div>
        <div>浏览器运行态：${context.profile.browserRuntime || "-"}</div>
      </div>
      <div class="terminal-config-panel-actions">
        <button class="btn primary" type="button" id="terminal-save-config">更新配置</button>
      </div>
    `;
}

function openTerminalConfigPanel() {
  state.terminalConfigOpen = true;
  currentTerminalInitCard = "window";
  renderTerminalInitCardMode();
  renderTerminalExecution();
  const panel = document.querySelector("#terminal-platform-config-panel");
  if (panel) {
    panel.scrollIntoView({ block: "start", behavior: "smooth" });
  }
}

function renderTerminalDailyQrView(root) {
  const windows = state.terminalExecution.windows || [];
  const summary = state.terminalExecution.summary || {};
  const loginStarted = Boolean(state.terminalExecution.login_started);
  const workspace = root instanceof Element ? root : document.querySelector("#terminal-matrix-workspace");
  if (!workspace) return;
  workspace.innerHTML = windows.map((window) => terminalWechatWindowMarkup(window, loginStarted)).join("");
  syncTerminalWechatSummary(summary, windows);
}

function renderTerminalSessionBoardView() {
  const workspace = document.querySelector("#terminal-matrix-workspace");
  if (!workspace) return;
  const profileByPlatform = state.terminalExecution.profile_by_platform || {};
  const platforms = terminalPlatformList().filter((platform) => platform.key !== "wechat");
  workspace.innerHTML = `
    <div class="terminal-session-board">
      ${platforms.map((platform) => {
        const profile = profileByPlatform[platform.key] || {};
        return `
          <article class="terminal-session-card terminal-glass">
            <div class="terminal-session-card-head">
              <strong>${platform.label}</strong>
              <span class="terminal-status-badge theme">${terminalPlatformPolicyLabel(platform.sessionPolicy)}</span>
            </div>
            <div class="muted">入口: ${profile.openUrl || platform.openUrl || "-"}</div>
            <div class="terminal-session-card-body">
              <div>会话状态: 长会话，按需检测或失效时再登录</div>
              <div>浏览器运行态: ${profile.browserRuntime || "-"}</div>
            </div>
          </article>
        `;
      }).join("")}
    </div>
  `;
  const progress = document.querySelector("#terminal-global-progress");
  if (progress) progress.textContent = "0/0";
  const active = document.querySelector("#terminal-active-windows");
  if (active) active.textContent = "0";
}

function renderTerminalExecution() {
  renderTerminalConfig();
  const context = terminalPlatformContext();
  const configPanel = document.querySelector("#terminal-platform-config-panel");
  if (configPanel) {
    if (context.platform === "wechat") {
      renderTerminalConfigPanel();
    } else {
      configPanel.classList.add("hidden");
      configPanel.innerHTML = "";
    }
  }
  document.querySelector("#terminal-init-modal")?.classList.toggle("hidden", !state.terminalConfigOpen);
  const startLoginButton = document.querySelector("#terminal-start-login");
  if (startLoginButton) {
    const loginStarted = Boolean(state.terminalExecution.login_started);
    startLoginButton.disabled = context.platform !== "wechat";
    startLoginButton.textContent = context.platform === "wechat" ? "获取登录二维码" : "检测全部";
  }
  const subtitle = document.querySelector("#terminal-header-subtitle");
  if (subtitle) {
    subtitle.textContent = context.platform === "wechat"
      ? "视频号采用每日登录扫码队列；其它平台采用长会话按需检测。"
      : `${context.label}采用长会话模型，按需检测或失效时再登录。`;
  }
  const progressLabel = document.querySelector("#terminal-progress-label");
  const activeLabel = document.querySelector("#terminal-active-label");
  if (progressLabel) progressLabel.textContent = context.platform === "wechat" ? "总进度:" : "待关注:";
  if (activeLabel) activeLabel.textContent = context.platform === "wechat" ? "运行窗口:" : "会话卡片:";
  if (context.platform === "wechat") {
    renderTerminalDailyQrView();
  } else {
    renderTerminalSessionBoardView();
  }
  startTerminalPolling();
}

function updateTerminalManualCountdowns() {
  const windows = state.terminalExecution.windows || [];
  const loginStarted = Boolean(state.terminalExecution.login_started);
  const windowById = new Map(windows.map((window) => [String(window.id), window]));
  const updatedWindows = new Set();
  document.querySelectorAll("[data-terminal-manual]").forEach((button) => {
    const window = windowById.get(String(button.dataset.terminalManual || ""));
    if (!window || updatedWindows.has(String(window.id))) return;
    const footerNode = button.closest(".terminal-task-column")?.querySelector(".terminal-col-footer");
    if (!footerNode) return;
    if (footerNode.querySelector("button.loading")) return;
    const currentIndex = Number(window.current_index || 0);
    const current = window.accounts?.[currentIndex] || {};
    const nextActionsMarkup = terminalWindowActionButtons(window, current, loginStarted).trim();
    const actionsNode = footerNode.querySelector(".terminal-window-actions");
    if (actionsNode) {
      if (actionsNode.outerHTML.trim() !== nextActionsMarkup) {
        const wrapper = document.createElement("div");
        wrapper.innerHTML = nextActionsMarkup;
        const nextActionsNode = wrapper.firstElementChild;
        if (nextActionsNode) {
          actionsNode.replaceWith(nextActionsNode);
        }
      }
    } else {
      footerNode.insertAdjacentHTML("beforeend", nextActionsMarkup);
    }
    updatedWindows.add(String(window.id));
  });
}

function updateTerminalQrCountdowns() {
  if (terminalCurrentRoute() !== "wechat") return;
  const windows = state.terminalExecution.windows || [];
  const workspace = document.querySelector("#terminal-matrix-workspace");
  if (!workspace) return;
  const loginStarted = true;
  for (const window of windows) {
    const windowNode = workspace.querySelector(`[data-terminal-window-id="${window.id}"]`);
    if (!windowNode) continue;
    const qrState = terminalQrLifecycle(window);
    const currentIndex = Number(window.current_index || 0);
    const current = window.accounts?.[currentIndex] || {};
    const countdownNode = windowNode.querySelector(`[data-terminal-qr-countdown="${window.id}"]`);
    if (countdownNode) {
      countdownNode.textContent = qrState.countdownText;
      countdownNode.classList.toggle("expired", qrState.expired);
      countdownNode.classList.toggle("active", qrState.active);
      countdownNode.classList.toggle("idle", !qrState.hasQr);
    }
    const placeholderNode = windowNode.querySelector(`[data-terminal-qr-placeholder="${window.id}"]`);
    if (placeholderNode && placeholderNode.dataset.terminalQrPlaceholderState !== qrState.placeholderState) {
      placeholderNode.dataset.terminalQrPlaceholderState = qrState.placeholderState;
      placeholderNode.innerHTML = terminalQrImageMarkup(window, current.id);
    }
    const currentStatusNode = windowNode.querySelector(`[data-terminal-current-status="${window.id}"]`);
    if (currentStatusNode) {
      currentStatusNode.textContent = terminalWechatAccountStatusText(window, current, currentIndex, currentIndex, loginStarted);
    }
  }
  updateTerminalManualCountdowns();
}

function startTerminalPolling() {
  if (terminalPollTimer) clearInterval(terminalPollTimer);
  terminalPollTimer = null;
  terminalPollRequestInFlight = false;
  if (terminalCountdownTimer) clearInterval(terminalCountdownTimer);
  terminalCountdownTimer = null;
  if (terminalCurrentRoute() !== "wechat") return;
  const shouldPoll = Boolean(state.terminalExecution?.login_started);
  if (shouldPoll) {
    const pollOnce = async () => {
      if (terminalPollRequestInFlight) return;
      if (currentView !== "terminal-execution" || terminalCurrentRoute() !== "wechat") return;
      if (!state.terminalExecution?.login_started) return;
      terminalPollRequestInFlight = true;
      try {
        const nextState = await api("/api/terminal-execution/poll", { method: "POST" });
        if (currentView !== "terminal-execution" || terminalCurrentRoute() !== "wechat") return;
        state.terminalExecution = nextState;
        renderTerminalExecution();
      } catch (_) {
        // Keep UI responsive even if one poll fails.
      } finally {
        terminalPollRequestInFlight = false;
      }
    };
    terminalPollTimer = window.setInterval(pollOnce, 1200);
  }
  const hasAnyQr = (state.terminalExecution.windows || []).some((window) => Boolean(window?.qr_url));
  if (!hasAnyQr) return;
  updateTerminalQrCountdowns();
  terminalCountdownTimer = window.setInterval(updateTerminalQrCountdowns, 100);
}

function terminalCurrentRoute() {
  return String(state.terminalRoute || "hub");
}

function ensureTerminalQrExpiryWindows() {
  const now = Date.now() / 1000;
  (state.terminalExecution.windows || []).forEach((window) => {
    if (window?.qr_url && Number(window.qr_expires_at || 0) <= 0) {
      window.qr_expires_at = now + 60;
    }
  });
}

function terminalSetRoute(route, { updateHash = true } = {}) {
  state.terminalRoute = route || "hub";
  if (updateHash) {
    const hash = state.terminalRoute === "hub" ? "#terminal-execution" : `#terminal/${state.terminalRoute}`;
    if (window.location.hash !== hash) window.history.replaceState(null, "", hash);
  }
  renderTerminalExecution();
}

function renderTerminalExecution() {
  ensureTerminalQrExpiryWindows();
  const route = terminalCurrentRoute();
  const section = document.querySelector("#terminal-execution");
  if (!section) return;
  const workspace = document.querySelector("#terminal-matrix-workspace");
  if (!workspace) {
    return;
  }
  const initModal = document.querySelector("#terminal-init-modal");
  const loadError = state.terminalExecution.error || "";
  const subtitle = document.querySelector("#terminal-header-subtitle");
  const routeHint = document.querySelector("#terminal-route-hint");
  const terminalShellDesc = document.querySelector("#terminal-shell-desc");
  const configPanel = document.querySelector("#terminal-platform-config-panel");
  const loginStarted = Boolean(state.terminalExecution.login_started);
  const summary = state.terminalExecution.summary || {};
  const platforms = (state.platforms || []).filter((item) => ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili", "tiktok", "x", "linkedin", "facebook", "youtube", "vk", "instagram"].includes(item.key));
  const platformMap = new Map(platforms.map((item) => [item.key, item]));
  const isHubRoute = route === "hub";
  section.dataset.terminalRoute = route;

  if (configPanel && (isHubRoute || route === "wechat")) {
    configPanel.classList.add("hidden");
    configPanel.innerHTML = "";
  }
  if (!isHubRoute && route !== "wechat") {
    renderTerminalConfigPanel();
  }

  if (initModal) initModal.classList.toggle("hidden", !state.terminalConfigOpen);
  if (routeHint) routeHint.textContent = route === "wechat"
    ? "视频号独立流程，配置、扫码队列、窗态和矩阵衔接都在本页闭环。"
    : route === "hub"
      ? "仅列平台入口，不混排扫码窗。"
      : "长会话平台统一模板，检测登录后再打开创作者后台。";
  if (terminalShellDesc) terminalShellDesc.textContent = route === "hub"
    ? "先选平台，再进入平台专属子流程。"
    : "一次登录长期有效；失效后重新检测或重新登录。";
  if (subtitle) subtitle.textContent = route === "hub"
    ? "平台枢纽页只列入口卡片，不混排视频号多窗。"
    : route === "wechat"
      ? "视频号独立流程：配置、获取二维码、扫码、发布、进入下一账号。"
      : "长会话平台：选账号、检测登录、打开创作者后台。";

  if (route === "hub") {
    if (state.terminalConfigOpen) {
      renderTerminalConfig();
    }
    const groups = [
      { title: "短会话平台", items: ["wechat"] },
      { title: "长会话平台", items: ["douyin", "kuaishou", "xiaohongshu", "bilibili", "tiktok", "x", "linkedin", "facebook", "youtube", "vk", "instagram"] },
    ];
    const hubHasCards = groups.some((group) => group.items.some((platform) => platformMap.has(platform)));
    workspace.innerHTML = `
      <div class="terminal-hub-layout">
        ${loadError ? `<div class="terminal-load-error">${loadError}</div>` : ""}
        ${hubHasCards ? groups.map((group) => {
          const cards = group.items.filter((platform) => platformMap.has(platform)).map((platform) => {
            const item = platformMap.get(platform);
            const capability = state.terminalExecution.platform_capabilities?.[platform] || {};
            const health = terminalHealthSummary(platform);
            return `
              <article class="terminal-entry-card ${platform === "wechat" ? "wechat" : "long-session"}">
                <div class="terminal-entry-head">
                  <div class="platform-name">${platformLogo(platform)}<strong>${item?.label || terminalPlatformName(platform)}</strong></div>
                  <span class="chip">${terminalSessionPolicyLabel(capability.sessionPolicy || (platform === "wechat" ? "daily_qr" : "persistent"))}</span>
                </div>
                ${platform === "wechat" ? "" : ""}
                <div class="terminal-entry-meta">
                  ${terminalStatusChip(platform, health)}
                </div>
                <div class="terminal-entry-actions">
                  <button class="btn primary" type="button" data-terminal-enter="${platform === "wechat" ? "wechat" : platform}">${terminalCardButtonLabel(platform, health)}</button>
                  <button class="btn secondary" type="button" data-terminal-config-jump="${platform}">账号与浏览器配置</button>
                </div>
              </article>
            `;
          }).join("");
          return `
            <section class="terminal-group-panel">
              <div class="panel-head">
                <div>
                  <h2>${group.title}</h2>
                  <p class="muted">${group.title === "短会话平台" ? "强调独立流程，不与其它平台混排。" : "统一长会话模板，平台间样式一致。"}</p>
                </div>
              </div>
              <div class="terminal-entry-grid">${cards}</div>
            </section>
          `;
        }).join("") : `<div class="terminal-empty-state terminal-empty-state-large"><strong>终端执行暂无平台数据</strong><p class="muted">当前只渲染平台入口卡片。请先点击右上角“刷新健康”或检查平台配置后再进入具体平台。</p></div>`}
      </div>
    `;
    startTerminalPolling();
    return;
  }

  if (route === "wechat") {
    renderTerminalConfig();
    workspace.innerHTML = `
      <div class="terminal-wechat-page">
        ${loadError ? `<div class="terminal-load-error">${loadError}</div>` : ""}
        <section class="terminal-group-panel">
          <div class="panel-head">
            <div>
              <h2>视频号终端</h2>
              <p class="muted">每日登录 / 多窗扫码 / 与素材矩阵的关系在这里闭环。</p>
            </div>
          </div>
          <div class="terminal-wechat-summary">
            <div class="metric"><span>已完成账号数</span><strong>${summary.success || 0}</strong></div>
            <div class="metric"><span>总账号数</span><strong>${summary.total || 0}</strong></div>
            <div class="metric"><span>活跃窗数量</span><strong>${summary.active_windows || 0}</strong></div>
          </div>
          <div class="terminal-entry-actions terminal-route-actions">
            <button class="btn primary" type="button" data-terminal-start-action="1">更新所有二维码</button>
          </div>
          <div class="terminal-workspace terminal-workspace-wechat"></div>
        </section>
      </div>
    `;
    renderTerminalDailyQrView(workspace.querySelector(".terminal-workspace-wechat"));
    startTerminalPolling();
    return;
  }

  const activePlatform = platformMap.has(route) ? route : "douyin";
  const context = terminalPlatformContextFor(activePlatform);
  const accounts = terminalLongSessionAccounts(activePlatform);
  workspace.innerHTML = `
    <div class="terminal-long-session-page">
      ${loadError ? `<div class="terminal-load-error">${loadError}</div>` : ""}
      <section class="terminal-group-panel">
        <div class="panel-head">
          <div>
            <h2>${context.label} 终端</h2>
            <p class="muted">一次登录长期有效；失效后请重新登录。</p>
          </div>
        </div>
        <div class="terminal-long-session-toolbar">
          <label>账号选择
            <select id="terminal-account-select" data-terminal-platform="${activePlatform}">
              ${accounts.map((account) => `<option value="${account.id}">${account.display_name} · ${account.login_status}</option>`).join("") || `<option value="">暂无启用账号</option>`}
            </select>
          </label>
          <div class="terminal-entry-meta">
            ${terminalStatusChip(activePlatform, terminalHealthSummary(activePlatform))}
            <span class="system-status">profile / 调试端口摘要：${accounts[0] ? `${accounts[0].profile_dir || "-"} · ${accounts[0].debug_port || "-"}` : "-"}</span>
          </div>
          <div class="terminal-entry-actions terminal-route-actions">
            <button class="btn primary" type="button" data-terminal-long-detect="${activePlatform}">检测登录</button>
            <button class="btn secondary" type="button" data-terminal-long-open="${activePlatform}">打开浏览器（创作者后台）</button>
          </div>
        </div>
        <div class="terminal-long-session-list">
          ${accounts.map((account) => `
            <article class="terminal-long-session-card">
              <strong>${account.display_name}</strong>
              <p class="muted">状态：${account.login_status} · profile：${account.profile_dir || "-"}</p>
              <p class="muted">debug port：${account.debug_port || "-"}</p>
            </article>
          `).join("") || `<div class="muted">暂无启用该平台的 active 账号。</div>`}
        </div>
      </section>
    </div>
  `;
  startTerminalPolling();
}

function updateTerminalManualCountdowns() {
  const windows = state.terminalExecution.windows || [];
  const loginStarted = Boolean(state.terminalExecution.login_started);
  const windowById = new Map(windows.map((window) => [String(window.id), window]));
  document.querySelectorAll("[data-terminal-manual]").forEach((button) => {
    const window = windowById.get(String(button.dataset.terminalManual || ""));
    const manualWait = loginStarted ? Math.max(0, Number(window?.manual_available_at || 0) - Math.floor(Date.now() / 1000)) : 0;
    button.disabled = !loginStarted || manualWait > 0;
    button.textContent = !loginStarted ? "先获取二维码" : (manualWait > 0 ? `发布 (${manualWait}s)` : "发布");
  });
}

function renderStats() {
  const summary = state.summary || {};
  const overview = [
    ["矩阵账号总数", summary.accounts || 4, "+8.4%", "up"],
    ["累计作品总量", 186, "+18.6%", "up"],
    ["累计总曝光", "68.4万", "+24.8%", "up"],
    ["累计总播放", "28.6万", "+19.2%", "up"],
    ["矩阵总粉丝", "4.8万", "+9.7%", "up"],
    ["周期净增粉丝", 3280, "+14.5%", "up"],
    ["累计互动量", "2.6万", "+11.3%", "up"],
    ["累计线索量", 426, "-3.2%", "down"],
  ];
  document.querySelector("#stats-overview").innerHTML = overview.map(([label, value, change, trend]) => `
    <div class="metric client-metric"><span>${label}</span><strong>${value}</strong><em class="${trend}">${change}</em></div>
  `).join("");

  const statsCaptureStatus = state.statsCaptureStatus || {};
  const statsCaptureNode = document.querySelector("#matrix-stats-capture-status");
  if (statsCaptureNode) {
    const latest = statsCaptureStatus.latest_run || {};
    const lock = statsCaptureStatus.lock || {};
    const parts = [];
    if (latest.status) parts.push(latest.status);
    if (latest.target_date) parts.push(latest.target_date);
    if (!parts.length && lock.pid) parts.push(`PID ${lock.pid}`);
    statsCaptureNode.textContent = parts.length ? parts.join(" · ") : "未加载";
  }

  const statsAccountFilter = document.querySelector("#stats-account-filter");
  if (statsAccountFilter) {
    const activeAccounts = (state.accounts || [])
      .filter((account) => String(account.status || "").toLowerCase() === "active")
      .sort((a, b) => Number(a.id || 0) - Number(b.id || 0));
    const currentValue = statsAccountFilter.value;
    const activeOptions = activeAccounts.map((account) => {
      const label = account.display_name || account.account_key || `账号 ${account.id}`;
      return `<option value="${account.id}">#${account.id} ${label}</option>`;
    }).join("");
    statsAccountFilter.innerHTML = `<option value="">全部账号</option>${activeOptions}`;
    if (currentValue && [...statsAccountFilter.options].some((option) => option.value === currentValue)) {
      statsAccountFilter.value = currentValue;
    } else {
      statsAccountFilter.value = "";
    }
  }

  const accounts = [
    ["GasGx小绿", "视频号", "正常", "86,200", "18,600", "12,480", "+860", "42.1%", "8.6%", 12, "爆款账号", ""],
    ["GasGx小黄", "抖音", "正常", "72,100", "16,900", "10,220", "+640", "37.8%", "7.9%", 10, "稳定账号", ""],
    ["发电机组案例", "小红书", "低流量", "18,400", "3,420", "3,180", "+92", "28.4%", "4.1%", 5, "潜力账号", "低流量"],
    ["燃气发动机现场", "快手", "休眠", "9,860", "1,160", "1,204", "-36", "22.6%", "2.8%", 1, "低效账号", "长期断更"],
  ];
  const accountHeaders = ["账号名称", "平台", "状态", "总播放", "周期播放", "粉丝", "增粉", "完播率", "互动率", "更新", "分层", "异常"];
  let sortIndex = 0;
  let sortDir = 1;
  const renderAccountTable = () => {
    const keyword = document.querySelector("#account-stats-search")?.value.trim().toLowerCase() || "";
    const filtered = accounts
      .filter((row) => row.join(" ").toLowerCase().includes(keyword))
      .sort((a, b) => String(a[sortIndex]).localeCompare(String(b[sortIndex]), "zh-Hans-CN", { numeric: true }) * sortDir);
    document.querySelector("#account-stats-table").innerHTML = `
      <table><thead><tr>${accountHeaders.map((header, index) => `<th><button type="button" data-account-sort="${index}">${header}</button></th>`).join("")}</tr></thead>
      <tbody>${filtered.map((row) => `<tr>${row.map((cell, index) => `<td>${index >= 10 && cell ? `<span class="chip">${cell}</span>` : cell || "-"}</td>`).join("")}</tr>`).join("")}</tbody></table>
      <div class="table-pager">第 1 / 1 页 · ${filtered.length} 条账号</div>
    `;
    document.querySelectorAll("[data-account-sort]").forEach((button) => {
      button.addEventListener("click", () => {
        const nextIndex = Number(button.dataset.accountSort);
        sortDir = sortIndex === nextIndex ? sortDir * -1 : 1;
        sortIndex = nextIndex;
        renderAccountTable();
      });
    });
  };
  document.querySelector("#account-stats-search")?.addEventListener("input", renderAccountTable);
  renderAccountTable();

  const works = [
    ["燃气发动机组现场并机", "8.6万", "爆款"],
    ["油气田自发电改造案例", "6.9万", "爆款"],
    ["发电机组负载测试", "4.2万", "普通"],
    ["矿场用电成本对比", "3.8万", "普通"],
  ];
  document.querySelector("#content-top-list").innerHTML = works.map((item, index) => `
    <article class="rank-row"><span>${index + 1}</span><strong>${item[0]}</strong><em>${item[1]}</em><b>${item[2]}</b></article>
  `).join("");

  const traffic = [["推荐流量", "54%"], ["搜索流量", "18%"], ["主页流量", "12%"], ["同城流量", "6%"], ["分享流量", "7%"], ["付费流量", "3%"]];
  document.querySelector("#traffic-list").innerHTML = traffic.map(([label, value]) => `<div><span>${label}</span><strong>${value}</strong></div>`).join("");

  const conversions = [["主页访问量", "17,860"], ["私信咨询量", "1,286"], ["评论咨询量", "824"], ["有效线索数", "426"], ["表单留资量", "196"], ["私域引流数", "158"], ["意向客户数", "138"], ["整体线索转化率", "0.15%"]];
  document.querySelector("#conversion-cards").innerHTML = conversions.map(([label, value]) => `<div><span>${label}</span><strong>${value}</strong></div>`).join("");

  const ops = [["计划发布量 VS 实际发布量", 92], ["周期文案产出数", 84], ["剪辑产出数", 78], ["私信回复处理量", 88], ["评论互动处理量", 76], ["账号优化次数", 64], ["内容迭代优化次数", 72]];
  document.querySelector("#operation-progress").innerHTML = ops.map(([label, value]) => `<div><div><strong>${label}</strong><span>${value}%</span></div><i style="--p:${value}%"></i></div>`).join("");

  const risks = ["违规作品 1 条，待整改", "1 个账号播放断崖下跌", "1 个账号长期断更休眠", "高掉粉账号预警 1 个"];
  document.querySelector("#risk-list").innerHTML = risks.map((risk) => `<article>${risk}</article>`).join("");
  renderAnalyticsFromDatabase();
}

function renderAnalyticsFromDatabase() {
  const analytics = state.analytics || {};
  if (!Object.keys(analytics).length) return;
  const overview = analytics.overview || [];
  if (overview.length) {
    document.querySelector("#stats-overview").innerHTML = [
      { label: "矩阵账号总数", value: state.summary?.accounts || 0, change: "+8.4%", trend: "up" },
      ...overview,
    ].map((item) => `<div class="metric client-metric"><span>${item.label}</span><strong>${item.value}</strong><em class="${item.trend || "up"}">${item.change || ""}</em></div>`).join("");
  }
  const accounts = (analytics.account_rank || []).map((item) => item.row).filter(Boolean);
  if (accounts.length) {
    const headers = ["账号名称", "平台", "状态", "总播放", "周期播放", "粉丝", "增粉", "完播率", "互动率", "更新", "分层", "异常"];
    document.querySelector("#account-stats-table").innerHTML = `<table><thead><tr>${headers.map((header) => `<th>${header}</th>`).join("")}</tr></thead><tbody>${accounts.map((row) => `<tr>${row.map((cell, index) => `<td>${index >= 10 && cell ? `<span class="chip">${cell}</span>` : cell || "-"}</td>`).join("")}</tr>`).join("")}</tbody></table><div class="table-pager">1 / 1 · ${accounts.length} 条账号</div>`;
  }
  const works = analytics.content_top || [];
  if (works.length) document.querySelector("#content-top-list").innerHTML = works.map((item, index) => `<article class="rank-row"><span>${index + 1}</span><strong>${item.title}</strong><em>${item.value}</em><b>${item.tag}</b></article>`).join("");
  const traffic = analytics.traffic || [];
  if (traffic.length) document.querySelector("#traffic-list").innerHTML = traffic.map((item) => `<div><span>${item.label}</span><strong>${item.value}</strong></div>`).join("");
  const conversions = analytics.conversion || [];
  if (conversions.length) document.querySelector("#conversion-cards").innerHTML = conversions.map((item) => `<div><span>${item.label}</span><strong>${item.value}</strong></div>`).join("");
  const ops = analytics.operation || [];
  if (ops.length) document.querySelector("#operation-progress").innerHTML = ops.map((item) => `<div><div><strong>${item.label}</strong><span>${item.value}%</span></div><i style="--p:${item.value}%"></i></div>`).join("");
  const risksFromDb = analytics.risk || [];
  if (risksFromDb.length) document.querySelector("#risk-list").innerHTML = risksFromDb.map((item) => `<article>${item.text}</article>`).join("");
}

function initSystemInitialize() {
  const button = document.querySelector("#system-initialize");
  const stateNode = document.querySelector("#system-initialize-state");
  if (!button || !stateNode) return;
  button.addEventListener("click", async () => {
    const password = await confirmSuperAdminPassword();
    if (!password) return;
    const restoreButton = setButtonLoading(button, "初始化中");
    stateNode.innerHTML = `<div class="muted">正在补齐数据库初始化数据...</div>`;
    try {
      const result = await api("/api/system/initialize", { method: "POST", body: JSON.stringify({ password }) });
      const inserted = Object.entries(result.inserted || {}).map(([key, value]) => `${key}: ${value}`).join(" / ") || "无";
      const skipped = Object.entries(result.skipped || {}).map(([key, value]) => `${key}: ${value}`).join(" / ") || "无";
      stateNode.innerHTML = `<div><strong>${result.ok ? "初始化完成" : "初始化未完成"}</strong><span>${result.seed_version || result.error || ""}</span></div><div><span>新增</span><strong>${inserted}</strong></div><div><span>跳过</span><strong>${skipped}</strong></div>`;
      await refresh();
    } catch (error) {
      stateNode.innerHTML = `<div><strong>初始化失败</strong><span>${error.message}</span></div>`;
    } finally {
      restoreButton();
    }
  });
}

function confirmSuperAdminPassword() {
  const modal = document.querySelector("#systemInitializePasswordModal");
  if (!modal) return Promise.resolve("");
  const input = document.querySelector("#systemInitializePasswordInput");
  const error = document.querySelector("#systemInitializePasswordError");
  const submit = document.querySelector("#systemInitializePasswordSubmit");
  const cancel = document.querySelector("#systemInitializePasswordCancel");
  const closeButton = document.querySelector("#systemInitializePasswordClose");
  modal.classList.remove("hidden");
  if (input) input.value = "";
  if (error) error.textContent = "";
  window.setTimeout(() => input?.focus(), 0);
  return new Promise((resolve) => {
    const close = (password = "") => {
      modal.classList.add("hidden");
      if (submit) submit.onclick = null;
      if (cancel) cancel.onclick = null;
      if (closeButton) closeButton.onclick = null;
      if (input) input.onkeydown = null;
      resolve(password);
    };
    const verify = () => {
      const password = input?.value.trim() || "";
      if (!password) {
        if (error) error.textContent = "请输入超级管理员密码";
        return;
      }
      close(password);
    };
    if (submit) submit.onclick = verify;
    if (cancel) cancel.onclick = () => close("");
    if (closeButton) closeButton.onclick = () => close("");
    if (input) input.onkeydown = (event) => {
      if (event.key === "Enter") verify();
      if (event.key === "Escape") close("");
    };
  });
}

const confirmSystemInitializePassword = confirmSuperAdminPassword;

function initSupabaseReadCacheClear() {
  const button = document.querySelector("#clear-supabase-read-cache");
  const stateNode = document.querySelector("#supabase-read-cache-state");
  if (!button) return;
  button.addEventListener("click", async () => {
    const restoreButton = setButtonLoading(button, "清理中...");
    if (stateNode) {
      stateNode.hidden = false;
      stateNode.textContent = "";
      stateNode.classList.remove("danger");
    }
    try {
      const result = await api("/api/system/supabase-read-cache/clear", { method: "POST" });
      if (stateNode) {
        if (result.cleared) {
          stateNode.textContent = "已清空进程内 Supabase 读缓存，后续请求将重新拉取远端数据。";
        } else {
          stateNode.textContent =
            result.backend === "sqlite"
              ? "当前品牌库为 SQLite，未启用 Supabase 读缓存。"
              : "未清理缓存。";
        }
      }
    } catch (error) {
      if (stateNode) {
        stateNode.textContent = `清理失败：${error.message}`;
        stateNode.classList.add("danger");
      }
      throw error;
    } finally {
      restoreButton();
    }
  });
}

function initSystemDirectoryActions() {
  const stateNode = document.querySelector("#system-directory-state");
  document.querySelectorAll("[data-system-dir]").forEach((button) => {
    button.addEventListener("click", async () => {
      const label = button.textContent.trim();
      const restoreButton = setButtonLoading(button, "打开中...");
      if (stateNode) {
        stateNode.textContent = `正在打开：${label}`;
        stateNode.classList.remove("danger");
        stateNode.removeAttribute("title");
      }
      try {
        const result = await api(`/api/system/open-directory/${encodeURIComponent(button.dataset.systemDir)}`, { method: "POST" });
        if (stateNode) {
          stateNode.textContent = `已打开：${label}`;
          stateNode.title = result.path || "";
        }
      } catch (error) {
        if (stateNode) {
          stateNode.textContent = `打开失败：${error.message}`;
          stateNode.classList.add("danger");
          stateNode.removeAttribute("title");
        }
        throw error;
      } finally {
        restoreButton();
      }
    });
  });
}

function formatTime(seconds) {
  if (!seconds) return "-";
  return new Date(Number(seconds) * 1000).toLocaleString();
}

function renderMatrixJobStatus() {
  const node = document.querySelector("#matrix-job-status");
  if (!node) return;
  const status = state.matrixJobStatus || {};
  const lastResult = status.last_result || {};
  node.innerHTML = [
    ["开关", status.enabled ? "开启" : "关闭"],
    ["运行中", status.running ? "是" : "否"],
    ["后台线程", status.thread_alive ? "正常" : "未运行"],
    ["定时模式", status.schedule_mode === "daily" ? "每天固定时间" : "按间隔"],
    ["定时参数", status.schedule_mode === "daily" ? (status.daily_time || "09:00") : `${status.run_interval_minutes || 1440} 分钟`],
    ["下次启动", formatTime(status.next_run_at)],
    ["上次启动", formatTime(status.last_started_at)],
    ["上次完成", formatTime(status.last_finished_at)],
    ["上次结果", status.last_ok === true ? "成功" : status.last_ok === false ? "失败" : "-"],
    ["发布数量", lastResult.count ?? "-"],
    ["上次巡检", formatTime(status.last_login_check_at)],
    ["巡检结果", status.last_login_check_ok === true ? "正常" : status.last_login_check_ok === false ? "需扫码" : "-"],
  ].map(([label, value]) => `<div class="job-status-item"><span>${label}</span><strong>${value}</strong></div>`).join("");
}

function renderOperationNotifications() {
  const routeNode = document.querySelector("#operation-notice-routes");
  const batchNode = document.querySelector("#login-qr-batches");
  if (routeNode) {
    const routes = state.notificationRoutes || [];
    const eventTypes = Array.from(new Set(routes.map((item) => item.event_type)));
    routeNode.innerHTML = eventTypes.map((eventType) => {
      const eventRoutes = routes.filter((item) => item.event_type === eventType);
      const first = eventRoutes[0] || {};
      const byPlatform = Object.fromEntries(eventRoutes.map((item) => [item.platform, item]));
      const routeButtons = ["telegram", "dingtalk", "wecom"].map((platform) => {
        const enabled = Boolean(byPlatform[platform]?.enabled);
        return `<button class="btn btn-sm ${enabled ? "primary" : "ghost"}" data-notice-route="${eventType}" data-notice-platform="${platform}" data-notice-enabled="${enabled ? "0" : "1"}">${aiPlatformLabel(platform)} ${enabled ? "开启" : "关闭"}</button>`;
      }).join("");
      const severity = first.default_severity || "info";
      const cardClass = severity === "critical" || severity === "blocking" ? "danger" : severity === "warning" || severity === "error" ? "warning" : "info";
      const subtypes = (first.subtypes || []).join(" / ");
      return `
        <article class="notification-card ${cardClass}">
          <span class="notification-dot"></span>
          <div>
            <strong>${first.label || eventType}</strong>
            <p>${first.source || ""}</p>
            <p>${eventType}${subtypes ? ` · ${subtypes}` : ""}</p>
            <div class="inline-actions">${routeButtons}</div>
          </div>
          <time>${severity}</time>
        </article>
      `;
    }).join("");
  }
  if (batchNode) {
    const batches = state.loginQrBatches || [];
    batchNode.innerHTML = batches.length ? batches.slice(0, 3).map((batch) => {
      let payload = batch.payload_json || {};
      if (typeof payload === "string") {
        try { payload = JSON.parse(payload || "{}"); } catch (_error) { payload = {}; }
      }
      const items = payload.items || [];
      return `
        <article class="notification-card danger">
          <span class="notification-dot"></span>
          <div>
            <strong>待扫码批次 ${batch.batch_id}</strong>
            <p>${items.map((item) => `${item.display_name || item.account_key} / port ${item.debug_port}`).join("；") || "等待巡检结果"}</p>
          </div>
          <time>${formatTime(batch.created_at)}</time>
        </article>
      `;
    }).join("") : `
      <article class="notification-card success">
        <span class="notification-dot"></span>
        <div><strong>暂无待扫码视频号</strong><p>登录巡检没有发现需要运营扫码的账号。</p></div>
        <time>实时</time>
      </article>
    `;
  }
}

function aiPlatformLabel(platform) {
  return {
    wecom: "企业微信",
    dingtalk: "钉钉",
    lark: "飞书 / Lark",
    telegram: "Telegram",
    whatsapp: "WhatsApp",
  }[platform] || platform;
}

function selectedAiRobotConfig() {
  const platform = document.querySelector("#ai-platform-select")?.value || "wecom";
  return state.aiRobotConfigs.find((item) => item.platform === platform) || { platform };
}

function aiRobotConfigFor(platform) {
  return state.aiRobotConfigs.find((item) => item.platform === platform) || { platform };
}

function isAiRobotBound(config) {
  return Boolean(config && config.webhook_url);
}

function isWebhookOnlyAiRobot(platform) {
  return ["wecom", "dingtalk", "lark"].includes(platform);
}

function aiRobotWebhookHint(platform) {
  if (platform === "dingtalk") return "填写钉钉群机器人 Webhook 地址，保存后可独立开启或关闭通知。";
  if (platform === "lark") return "填写飞书群机器人 Webhook 地址；下方回调地址用于飞书事件订阅 URL 验证。";
  return "填写企业微信群机器人 Webhook 地址，保存后可独立开启或关闭通知。";
}

function aiRobotCallbackUrl(platform) {
  return `${window.location.origin}/api/ai-robots/${encodeURIComponent(platform)}/webhook`;
}

function visibleAiRobotConfigs() {
  return state.aiRobotConfigs.filter((item) => item.platform !== "whatsapp");
}

function syncTelegramSetupVisibility() {
  const form = document.querySelector("#ai-robot-form");
  const card = document.querySelector("#telegram-setup-card");
  if (!form || !card) return;
  const platform = form.elements.platform.value;
  const isTelegram = platform === "telegram";
  const isWebhookOnly = isWebhookOnlyAiRobot(platform);
  const modeTitle = document.querySelector("#ai-config-mode-title");
  const modeDesc = document.querySelector("#ai-config-mode-desc");
  const larkCallbackField = document.querySelector("#ai-lark-callback-field");
  const larkCallbackInput = document.querySelector("#ai-lark-callback-url");
  card.hidden = !isTelegram;
  form.classList.toggle("telegram-simple-mode", isTelegram);
  form.classList.toggle("webhook-simple-mode", isWebhookOnly);
  form.classList.toggle("lark-callback-mode", platform === "lark");
  if (larkCallbackField) larkCallbackField.hidden = platform !== "lark";
  if (larkCallbackInput) larkCallbackInput.value = aiRobotCallbackUrl("lark");
  if (modeTitle) modeTitle.textContent = isTelegram ? "Telegram 快速配置" : `${aiPlatformLabel(platform)} Webhook 配置`;
  if (modeDesc) modeDesc.textContent = isTelegram ? "填写 Bot Token 并获取 Chat ID，保存后可独立开启或关闭通知。" : aiRobotWebhookHint(platform);
  if (isTelegram && !form.elements.bot_name.value) {
    form.elements.bot_name.value = "GasGx Telegram Bot";
  }
  if (isWebhookOnly && !form.elements.bot_name.value) {
    form.elements.bot_name.value = `${aiPlatformLabel(form.elements.platform.value)}机器人`;
  }
}

function telegramWebhookUrl(token) {
  return `https://api.telegram.org/bot${token}/sendMessage`;
}

async function openTelegramBotChat() {
  const form = document.querySelector("#ai-robot-form");
  if (!form) return;
  const token = String(form.elements.telegram_bot_token?.value || "").trim();
  if (!token) {
    setTelegramChatIdState("Fill Bot token first, then open bot chat.", "danger");
    return;
  }
  setTelegramChatIdState("Finding bot username...");
  try {
    const payload = await api("/api/ai-robots/telegram/resolve", {
      method: "POST",
      body: JSON.stringify({ token }),
    });
    if (payload.chat_id) {
      form.elements.telegram_chat_id.value = String(payload.chat_id);
      form.elements.target_id.value = String(payload.chat_id);
    }
    if (!payload.username) {
      throw new Error("Telegram did not return a bot username.");
    }
    window.open(`https://t.me/${encodeURIComponent(payload.username)}`, "_blank", "noopener,noreferrer");
    setTelegramChatIdState("Telegram opened. Press Start or send hi, then return here and click Save config.");
  } catch (error) {
    setTelegramChatIdState(`Telegram setup failed: ${error.message || "failed to open bot chat."}`, "danger");
  }
}

function fillTelegramFields() {
  const form = document.querySelector("#ai-robot-form");
  if (!form) return false;
  const token = String(form.elements.telegram_bot_token?.value || "").trim();
  const chatId = String(form.elements.telegram_chat_id?.value || "").trim();
  if (!token || !chatId) {
    window.alert("Fill Bot token and Chat ID first.");
    return false;
  }
  form.elements.platform.value = "telegram";
  form.elements.enabled.value = "true";
  form.elements.bot_name.value = form.elements.bot_name.value || "GasGx Telegram Bot";
  form.elements.webhook_url.value = telegramWebhookUrl(token);
  form.elements.target_id.value = chatId;
  form.elements.webhook_secret.value = "";
  if (!form.elements.signing_secret.value) {
    form.elements.signing_secret.value = `gasgx-${Date.now().toString(36)}`;
  }
  syncTelegramSetupVisibility();
  return true;
}

function setTelegramChatIdState(message, tone = "") {
  const node = document.querySelector("#telegram-chat-id-state");
  if (!node) return;
  node.textContent = message;
  node.classList.toggle("danger", tone === "danger");
}

async function fetchTelegramChatId() {
  const form = document.querySelector("#ai-robot-form");
  if (!form) return false;
  const token = String(form.elements.telegram_bot_token?.value || "").trim();
  if (!token) {
    setTelegramChatIdState("Fill Bot token first.", "danger");
    return false;
  }
  setTelegramChatIdState("Fetching chat id...");
  try {
    const payload = await api("/api/ai-robots/telegram/resolve", {
      method: "POST",
      body: JSON.stringify({ token }),
    });
    if (!payload.chat_id) {
      setTelegramChatIdState("No chat found. Send one message to the bot or group, then save again.", "danger");
      return false;
    }
    form.elements.telegram_chat_id.value = String(payload.chat_id);
    form.elements.target_id.value = String(payload.chat_id);
    setTelegramChatIdState(`Chat ID found: ${payload.chat_id}`);
    return true;
  } catch (error) {
    setTelegramChatIdState(`Telegram setup failed: ${error.message || "failed to fetch chat id."}`, "danger");
    return false;
  }
}

function renderAiRobot() {
  const form = document.querySelector("#ai-robot-form");
  if (!form) return;
  const configPanel = document.querySelector("#ai-config-panel");
  const telegramConfig = state.aiRobotConfigs.find((item) => item.platform === "telegram") || { platform: "telegram" };
  const telegramBound = isAiRobotBound(telegramConfig);
  const editingPlatform = state.aiRobotEditingPlatform;
  const editingTelegram = editingPlatform === "telegram" || !telegramBound;
  const configured = visibleAiRobotConfigs().filter(isAiRobotBound);
  const config = editingPlatform ? aiRobotConfigFor(editingPlatform) : (configured.length ? configured[0] : selectedAiRobotConfig());
  const saveButton = document.querySelector("#ai-save-config");
  const sendTestButton = document.querySelector("#ai-send-test");
  const panelSaveButton = document.querySelector("#ai-save-config-panel");
  const panelSendTestButton = document.querySelector("#ai-send-test-panel");
  const formHidden = !editingPlatform;
  if (configPanel) configPanel.hidden = formHidden;
  form.hidden = formHidden;
  saveButton?.classList.toggle("hidden", formHidden);
  sendTestButton?.classList.toggle("hidden", formHidden);
  panelSaveButton?.classList.toggle("hidden", formHidden);
  panelSendTestButton?.classList.toggle("hidden", formHidden);
  form.elements.platform.value = config.platform || "wecom";
  form.elements.bot_name.value = config.bot_name || "";
  form.elements.enabled.value = String(config.enabled === true);
  form.elements.webhook_url.value = config.webhook_url || "";
  form.elements.webhook_secret.value = "";
  form.elements.signing_secret.value = "";
  form.elements.target_id.value = config.target_id || "";
  if (form.elements.telegram_bot_token) form.elements.telegram_bot_token.value = "";
  if (form.elements.telegram_chat_id) form.elements.telegram_chat_id.value = config.platform === "telegram" ? (config.target_id || "") : "";
  syncTelegramSetupVisibility();
  document.querySelector("#ai-config-state").textContent = configured.length && !editingPlatform ? "已配置" : (config.enabled ? "已启用" : "未启用");
  renderBoundAiRobotPlatforms();
  document.querySelector("#ai-channel-grid").innerHTML = visibleAiRobotConfigs().map((item) => `
    <article class="bot-channel-card">
      ${aiRobotLogo(item.platform)}
      <div>
        <strong>${aiPlatformLabel(item.platform)}</strong>
        <p>${item.webhook_url ? "已配置" : "未配置"} · ${item.enabled ? "通知开启" : "通知关闭"} · ${item.has_signing_secret ? "验签密钥已保存" : "无需验签密钥"}</p>
      </div>
      <button class="btn secondary" type="button" data-ai-platform="${item.platform}">配置</button>
    </article>
  `).join("");
  document.querySelectorAll("[data-ai-platform]").forEach((button) => {
    button.onclick = () => {
      state.aiRobotEditingPlatform = button.dataset.aiPlatform;
      renderAiRobot();
      document.querySelector("#ai-config-panel")?.scrollIntoView({ behavior: "smooth", block: "start" });
    };
  });
  const messageList = document.querySelector("#ai-message-list");
  const messageToggle = document.querySelector("#ai-message-toggle");
  if (!messageList) return;
  messageList.hidden = state.aiRobotMessagesCollapsed;
  if (messageToggle) {
    messageToggle.textContent = state.aiRobotMessagesCollapsed ? "展开" : "最近 100 条";
  }
  messageList.innerHTML = state.aiRobotMessages.length
    ? state.aiRobotMessages.map((item) => `<article class="task-row">
        <div><strong>#${item.id} ${aiPlatformLabel(item.platform)}</strong><span>${item.summary || item.message_type}</span></div>
        <span class="task-status">${item.status}</span>
      </article>`).join("")
    : `<div class="muted">暂无机器人消息队列。</div>`;
}

function renderBoundAiRobotPlatforms() {
  const node = document.querySelector("#ai-bound-platforms");
  if (!node) return;
  const bound = state.aiRobotConfigs.filter(isAiRobotBound);
  if (!bound.length) {
    node.innerHTML = `<div class="bound-empty">还没有配置消息机器人。企业微信、钉钉、飞书填 Webhook 地址；Telegram 填 Bot Token。</div>`;
    return;
  }
  node.innerHTML = bound.map((item) => `
    <article class="bound-platform-card">
      ${aiRobotLogo(item.platform)}
      <div>
        <strong>${aiPlatformLabel(item.platform)} 已配置</strong>
        <p>${item.enabled ? "通知开启" : "通知关闭"} · ${item.target_id ? `目标会话 ${item.target_id}` : "Webhook 已保存"} · 可发送测试消息</p>
      </div>
      <div class="bound-platform-actions">
        <button class="notify-switch ${item.enabled ? "enabled" : ""}" type="button" data-ai-toggle="${item.platform}" aria-pressed="${item.enabled ? "true" : "false"}">
          <span></span><b>${item.enabled ? "通知开" : "通知关"}</b>
        </button>
        <button class="btn secondary" type="button" data-ai-test="${item.platform}">发送测试</button>
        <button class="btn secondary" type="button" data-ai-edit="${item.platform}">修改</button>
        <button class="btn secondary danger" type="button" data-ai-delete="${item.platform}">删除</button>
      </div>
    </article>
  `).join("");
  node.querySelectorAll("[data-ai-test]").forEach((button) => {
    button.onclick = async () => {
      await sendAiRobotTest(button.dataset.aiTest, button);
    };
  });
  node.querySelectorAll("[data-ai-toggle]").forEach((button) => {
    button.onclick = async () => {
      const platform = button.dataset.aiToggle;
      const config = aiRobotConfigFor(platform);
      const restoreButton = setButtonLoading(button, config.enabled ? "关闭中" : "开启中");
      try {
        await api(`/api/ai-robots/${platform}/config`, {
          method: "PUT",
          body: JSON.stringify({
            enabled: !config.enabled,
            bot_name: config.bot_name || `${aiPlatformLabel(platform)}机器人`,
            webhook_url: config.webhook_url || "",
            webhook_secret: "",
            signing_secret: "",
            target_id: config.target_id || "",
          }),
        });
        state.aiRobotConfigs = await api("/api/ai-robots/configs");
        state.aiRobotMessages = await api("/api/ai-robots/messages");
        renderAiRobot();
      } finally {
        restoreButton();
      }
    };
  });
  node.querySelectorAll("[data-ai-edit]").forEach((button) => {
    button.onclick = () => {
      state.aiRobotEditingPlatform = button.dataset.aiEdit;
      renderAiRobot();
      document.querySelector("#ai-config-panel")?.scrollIntoView({ behavior: "smooth", block: "start" });
    };
  });
  node.querySelectorAll("[data-ai-delete]").forEach((button) => {
    button.onclick = async () => {
      const platform = button.dataset.aiDelete;
      if (!window.confirm(`确认删除 ${aiPlatformLabel(platform)} 机器人配置？删除后需要重新填写 Bot Token。`)) return;
      const restoreButton = setButtonLoading(button, "删除中");
      try {
        await api(`/api/ai-robots/${platform}/config`, { method: "DELETE" });
        state.aiRobotConfigs = await api("/api/ai-robots/configs");
        state.aiRobotMessages = await api("/api/ai-robots/messages");
        state.aiRobotEditingPlatform = "";
        renderAiRobot();
        document.querySelector("#ai-config-state").textContent = "已删除";
      } finally {
        restoreButton();
      }
    };
  });
}

function renderDatabaseDictionary() {
  const status = document.querySelector("#supabase-health-state");
  const list = document.querySelector("#supabase-health-list");
  const toggle = document.querySelector("#database-dictionary-locale-toggle");
  if (!status || !list) return;
  const localized = Boolean(state.databaseDictionaryLocalized);
  if (toggle) {
    toggle.textContent = localized ? "英文版" : "中文版";
    toggle.setAttribute("aria-pressed", String(localized));
  }
  const dictionary = state.databaseDictionary;
  if (!dictionary) {
    status.textContent = "未加载";
    list.innerHTML = `<div class="muted">暂无数据库字典。</div>`;
    return;
  }
  const tables = dictionary.tables || [];
  status.textContent = `${tables.length} 张表`;
  status.classList.remove("danger");
  list.innerHTML = tables.map((table) => {
    const columns = table.columns || [];
    const expanded = isDatabaseTableExpanded(table.name);
    const tableName = translateDatabaseName(table.name, DATABASE_DICTIONARY_TABLE_LABELS, localized);
    return `
      <article class="db-dictionary-table ${expanded ? "is-expanded" : "is-collapsed"}">
        <button class="db-dictionary-head" type="button" data-db-table="${table.name}" aria-expanded="${expanded}">
          <span class="db-dictionary-head-copy">
            <strong>${tableName}</strong>
            <small>${localized ? `${columns.length} 个字段` : `${columns.length} 字段`}</small>
          </span>
          <span class="db-dictionary-table-badge">${expanded ? "折叠" : "展开"}</span>
        </button>
        <div class="db-dictionary-shell" ${expanded ? "" : 'hidden aria-hidden="true"'}>
          <div class="db-dictionary-toolbar">
            <span class="db-dictionary-toolbar-title">${localized ? "字段列表" : "Columns"}</span>
            <button class="db-dictionary-about" type="button" disabled>${localized ? "字段类型说明" : "About data types"}</button>
          </div>
          <div class="db-dictionary-grid db-dictionary-grid-head" aria-hidden="true">
            <span>${localized ? "字段名" : "Name"}</span>
            <span>${localized ? "类型" : "Type"}</span>
            <span>${localized ? "默认值" : "Default Value"}</span>
          </div>
          <div class="db-dictionary-rows">
            ${columns.map((column) => {
              const meta = parseDatabaseColumnMeta(column.constraints);
              const columnName = translateDatabaseName(column.name, DATABASE_DICTIONARY_COLUMN_LABELS, localized);
              const defaultValue = translateDatabaseDefaultValue(meta, localized);
              return `
                <div class="db-dictionary-grid db-dictionary-row">
                  <div class="db-dictionary-name">
                    <span class="db-dictionary-icon">${meta.primary ? "#" : "T"}</span>
                    <div>
                      <strong>${columnName}</strong>
                      <small>${translateDatabaseConstraintSummary(meta, localized)}</small>
                    </div>
                  </div>
                  <div class="db-dictionary-type">${translateDatabaseType(column.type || "-", localized)}</div>
                  <div class="db-dictionary-default ${meta.defaultValue ? "" : "is-null"}">${defaultValue}</div>
                </div>
              `;
            }).join("")}
          </div>
        </div>
      </article>
    `;
  }).join("");

  list.querySelectorAll("[data-db-table]").forEach((button) => {
    button.onclick = () => toggleDatabaseTable(button.dataset.dbTable || "");
  });
}

function healthDetailText(details) {
  return Object.entries(details)
    .map(([key, value]) => `${key}: ${displayDatabaseKeyword(value)}`)
    .join(" · ");
}

function terminalSessionPolicyLabel(policy) {
  return policy === "daily_qr" ? "每日登录（扫码）" : "一次登录 · 长期有效";
}

function terminalRouteFromHash(hash = window.location.hash) {
  const raw = String(hash || "").replace(/^#/, "");
  if (!raw) return { view: "overview", route: "hub" };
  if (raw === "terminal-execution") return { view: "terminal-execution", route: "hub" };
  if (raw.startsWith("terminal/")) return { view: "terminal-execution", route: raw.slice("terminal/".length) || "hub" };
  return { view: raw, route: "hub" };
}

function terminalHealthSummary(platform) {
  const normalized = String(platform || "").toLowerCase();
  if (normalized === "wechat") {
    const summary = state.terminalExecution.summary || {};
    if (state.terminalExecution.login_started) {
      return summary.success === summary.total && summary.total ? "已登录" : "等待扫码";
    }
    return state.terminalExecution.initialized ? "已配置" : "未检测";
  }
  const accounts = (state.accounts || []).filter((item) => {
    const platforms = item.platforms || [];
    return String(item.status || "") === "active" && platforms.some((entry) => String(entry.platform || "") === normalized);
  });
  if (!accounts.length) return "未配置浏览器";
  if (accounts.some((item) => (item.platforms || []).some((entry) => String(entry.platform || "") === normalized && String(entry.login_status || "").toLowerCase() === "ready"))) {
    return "已就绪";
  }
  if (accounts.some((item) => (item.platforms || []).some((entry) => String(entry.platform || "") === normalized && String(entry.login_status || "").toLowerCase() === "login_required"))) {
    return "需重新登录";
  }
  return "未检测";
}

function terminalStatusChip(platform, text) {
  const danger = String(text || "").includes("需") || String(text || "").includes("未配置");
  return `<span class="system-status ${danger ? "danger" : ""}">${terminalPlatformName(platform)} · ${String(text || "-")}</span>`;
}

function terminalCardButtonLabel(platform, health) {
  if (platform === "wechat") return "进入";
  if (health === "已登录" || health === "已就绪") return "进入";
  if (health === "需重新登录") return "检测登录";
  if (health === "未配置浏览器") return "打开创作者后台";
  return "检测登录";
}

function terminalPlatformName(platform) {
  return platformLabel(platform);
}

function terminalPlatformContextFor(platform) {
  const capability = state.terminalExecution.platform_capabilities?.[platform] || {};
  const profile = state.terminalExecution.profile_by_platform?.[platform] || {};
  return {
    platform,
    label: capability.label || terminalPlatformName(platform),
    sessionPolicy: capability.sessionPolicy || profile.sessionPolicy || (platform === "wechat" ? "daily_qr" : "persistent"),
    openUrl: capability.openUrl || profile.openUrl || "",
    profile,
    capability,
  };
}

function terminalPlatformCards() {
  const order = ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili", "tiktok", "x", "linkedin", "facebook", "youtube", "vk", "instagram"];
  const capabilities = state.terminalExecution.platform_capabilities || {};
  return order
    .filter((platform) => capabilities[platform])
    .map((platform) => {
      const capability = capabilities[platform];
      const health = terminalHealthSummary(platform);
      const actionLabel = terminalCardButtonLabel(platform, health);
      const route = platform === "wechat" ? "wechat" : platform;
      return `
        <article class="terminal-entry-card long-session">
          <div class="terminal-entry-head">
            <div class="platform-name">
              ${platformLogo(platform)}
              <strong>${capability.label || terminalPlatformName(platform)}</strong>
            </div>
            <span class="chip">${terminalSessionPolicyLabel(capability.sessionPolicy)}</span>
          </div>
          <p class="muted">${platform === "wechat" ? "视频号独立流程，进入后只处理扫码、窗态和矩阵衔接。" : ""}</p>
          <div class="terminal-entry-meta">
            ${terminalStatusChip(platform, health)}
            <span class="system-status">${capability.canOpenBrowser ? "可打开浏览器" : "未配置浏览器"}</span>
          </div>
          <div class="terminal-entry-actions">
            <button class="btn primary" type="button" data-terminal-enter="${route}">${actionLabel}</button>
            <button class="btn secondary" type="button" data-terminal-config-jump="${platform}">账号与浏览器配置</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function terminalLongSessionAccounts(platform) {
  return (state.accounts || [])
    .filter((account) => String(account.status || "") === "active" && (account.platforms || []).some((item) => String(item.platform || "") === platform))
    .map((account) => {
      const platformInfo = (account.platforms || []).find((item) => String(item.platform || "") === platform) || {};
      return {
        id: account.id,
        display_name: account.display_name || account.account_key,
        account_key: account.account_key,
        login_status: String(platformInfo.login_status || "unknown"),
        profile_dir: platformInfo.profile_dir || "",
        debug_port: platformInfo.debug_port || "",
        open_url: platformInfo.open_url || terminalPlatformContextFor(platform).openUrl,
      };
    });
}

async function refresh() {
  return loadViewData(currentView, { force: true });
}

async function loadShellData() {
  const brand = await api("/api/brand");
  state.brand = brand;
  applyServerBrand(brand);
}

async function loadPlatforms() {
  state.platforms = await api("/api/platforms");
}

async function loadAccounts() {
  state.accounts = await api("/api/accounts");
}

async function loadOperatorWechats() {
  state.operatorWechats = await api("/api/operator-wechats");
}

async function loadTasks() {
  state.tasks = await api("/api/tasks");
}

async function loadViewData(view, { force = false } = {}) {
  if (!force && loadedViews.has(view)) return;
  setViewLoading(view);
  if (view !== "video-matrix") {
    unmountVideoMatrixWorkbench();
  }
  if (view === "terminal-execution") {
    state.terminalQrVisible = false;
    state.terminalExecution = {
      colors: [],
      operators: [],
      windows: [],
      summary: {},
      platform_capabilities: {},
      profile_by_platform: {},
      active_platform: "wechat",
      login_started: false,
      initialized: false,
    };
    renderTerminalExecution();
  }
  await loadShellData();

  if (view === "overview") {
    await loadPlatforms();
    state.summary = await api("/api/summary");
    renderSummary();
    renderPlatforms();
  } else if (view === "accounts") {
    await loadPlatforms();
    await loadOperatorWechats();
    await loadAccounts();
    renderOperatorWechatPicker();
    updateAccountPhoneHint();
    renderAccounts();
  } else if (view === "settings") {
    await loadPlatforms();
    state.distributionSettings = await api("/api/settings/distribution");
    state.matrixJobStatus = await api("/api/jobs/matrix-wechat/status");
    try {
      state.terminalExecution = await api("/api/terminal-execution/state");
    } catch {
      state.terminalExecution = state.terminalExecution || { colors: [], operators: [], config: [], active_platform: "wechat" };
    }
    renderDistributionSettings();
    renderMatrixJobStatus();
    renderSettingsCardMode();
  } else if (view === "tasks") {
    await loadPlatforms();
    await loadAccounts();
    await loadTasks();
    renderTaskSelects();
    renderTasks();
  } else if (view === "terminal-execution") {
    try {
      await Promise.all([
        loadPlatforms(),
        loadAccounts(),
      ]);
      renderTerminalExecution();
      api("/api/terminal-execution/state")
        .then((terminalState) => {
          if (currentView !== "terminal-execution") return;
          state.terminalExecution = terminalState;
          renderTerminalExecution();
        })
        .catch((error) => {
          if (currentView !== "terminal-execution") return;
          state.terminalExecution = {
            ...(state.terminalExecution || {}),
            error: error.message || "加载终端执行数据失败",
          };
          renderTerminalExecution();
        });
    } catch (error) {
      state.terminalExecution = {
        ...(state.terminalExecution || {}),
        error: error.message || "加载终端执行数据失败",
      };
      renderTerminalExecution();
    }
  } else if (view === "stats") {
    await loadAccounts();
    state.summary = await api("/api/summary");
    state.stats = await api("/api/stats");
    state.analytics = await api("/api/stats/analytics");
    state.statsCaptureStatus = await api("/api/jobs/matrix-wechat/stats-capture/status");
    renderStats();
  } else if (view === "ai-robot") {
    state.aiRobotConfigs = await api("/api/ai-robots/configs");
    state.aiRobotMessages = await api("/api/ai-robots/messages");
    renderAiRobot();
  } else if (view === "notifications") {
    state.notificationEvents = await api("/api/notification-events");
    state.notificationRoutes = await api("/api/notification-routes");
    state.loginQrBatches = await api("/api/login-qr-batches");
    renderOperationNotifications();
  } else if (view === "system-settings") {
    state.databaseDictionary = await api("/api/system/database-dictionary");
    renderDatabaseDictionary();
  } else if (view === "video-matrix") {
    mountVideoMatrixWorkbench();
  }
  loadedViews.add(view);
}

function renderDistributionSettings() {
  const form = document.querySelector("#distribution-settings-form");
  if (!form) return;
  const settings = state.distributionSettings || { common: {}, platforms: {} };
  const common = settings.common || {};
  const jobs = settings.jobs || {};
  const matrixJob = jobs.matrix_wechat_publish || {};
  form.elements["common.material_dir"].value = common.material_dir || "runtime/materials/videos";
  const followsVm = common.material_dir_follows_video_matrix !== false;
  form.querySelectorAll('input[name="common.material_dir_follows_video_matrix"]').forEach((input) => {
    input.checked = input.value === (followsVm ? "true" : "false");
  });
  const matDirHint = document.querySelector("#resolved-material-dir-hint");
  if (matDirHint) {
    const resolved = settings.resolved_material_dir || "";
    matDirHint.textContent = resolved ? `当前实际扫描目录：${resolved}` : "";
  }
  const matDirInput = form.elements["common.material_dir"];
  const openMatBtn = document.querySelector("#open-material-dir");
  const syncMatDirFieldState = () => {
    const follow = form.querySelector('input[name="common.material_dir_follows_video_matrix"]:checked')?.value === "true";
    matDirInput.readOnly = Boolean(follow);
    if (openMatBtn) openMatBtn.disabled = false;
  };
  syncMatDirFieldState();
  form.querySelectorAll('input[name="common.material_dir_follows_video_matrix"]').forEach((input) => {
    input.onchange = syncMatDirFieldState;
  });
  form.elements["common.publish_mode"].value = common.publish_mode || "publish";
  form.elements["common.topics"].value = common.topics || "#天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿";
  form.elements["common.upload_timeout"].value = String(common.upload_timeout || 60);
  form.elements["common.wechat_content_type"].value = common.wechat_content_type || "short_video";
  form.elements["common.wechat_visibility"].value = common.wechat_visibility || "public";
  form.elements["common.wechat_comment_permission"].value = common.wechat_comment_permission || "public";
  form.elements["common.wechat_collection_name"].value = common.wechat_collection_name ?? "GasGx";
  form.elements["common.wechat_declare_original"].value = String(common.wechat_declare_original === true);
  form.elements["common.wechat_short_title"].value = common.wechat_short_title || "GasGx燃气发电挖矿";
  form.elements["common.wechat_location"].value = common.wechat_location || "";
  form.elements["common.wechat_caption"].value = common.wechat_caption || "";
  form.elements["jobs.matrix_wechat_publish.batch_size"].value = String(matrixJob.batch_size || 5);
  form.elements["jobs.matrix_wechat_publish.enabled"].value = String(matrixJob.enabled === true);
  form.elements["jobs.matrix_wechat_publish.schedule_mode"].value = matrixJob.schedule_mode || "interval";
  form.elements["jobs.matrix_wechat_publish.daily_time"].value = matrixJob.daily_time || "09:00";
  form.elements["jobs.matrix_wechat_publish.run_interval_minutes"].value = String(matrixJob.run_interval_minutes || 1440);
  form.elements["jobs.matrix_wechat_publish.batch_interval_min_minutes"].value = String(matrixJob.batch_interval_min_minutes ?? 5);
  form.elements["jobs.matrix_wechat_publish.batch_interval_max_minutes"].value = String(matrixJob.batch_interval_max_minutes ?? 15);
  form.elements["jobs.matrix_wechat_publish.rotate_start_group"].value = String(matrixJob.rotate_start_group !== false);
  form.elements["jobs.matrix_wechat_publish.shuffle_within_batch"].value = String(matrixJob.shuffle_within_batch !== false);
  form.elements["jobs.matrix_wechat_publish.retry_failed_last"].value = String(matrixJob.retry_failed_last !== false);
  document.querySelector("#platform-settings-list").innerHTML = ["cn", "global"].map((region) => {
    const items = state.platforms
      .filter((item) => (item.region === "cn") === (region === "cn"))
      .sort((a, b) => PLATFORM_ORDER.indexOf(a.key) - PLATFORM_ORDER.indexOf(b.key));
    return `<section class="platform-settings-region">
      <div class="region-title">${REGION_LABELS[region]}</div>
      <div class="platform-settings-grid">${items.map(renderPlatformSettingsCard).join("")}</div>
    </section>`;
  }).join("");
  syncWechatInheritModeInputs(document.querySelector("#distribution-settings-form") || document);
}

function renderPlatformSettingsCard(platform) {
  const common = state.distributionSettings.common || {};
  const value = (state.distributionSettings.platforms || {})[platform.key] || {};
  const hasOwn = (key) => Object.prototype.hasOwnProperty.call(value, key);
  const declareOriginalRaw = value.declare_original;
  const declareOriginalInherit = !hasOwn("declare_original") || String(declareOriginalRaw || "").toLowerCase() === "inherit";
  const declareOriginalTrue = declareOriginalRaw === true || String(declareOriginalRaw || "").toLowerCase() === "true";
  const declareOriginalFalse = !declareOriginalInherit && !declareOriginalTrue;
  const shortTitleInherit = !hasOwn("short_title") || value.short_title === "inherit";
  const locationInherit = !hasOwn("location") || value.location === "inherit";
  const captionInherit = !hasOwn("caption") || value.caption === "inherit";
  const collectionInherit = !hasOwn("collection_name") || value.collection_name === "inherit";
  const contentTypeInherit = !hasOwn("content_type") || value.content_type === "inherit";
  const visibilityInherit = !hasOwn("visibility") || value.visibility === "inherit";
  const commentPermissionInherit = !hasOwn("comment_permission") || value.comment_permission === "inherit";
  const shortTitle = escapeHtml(shortTitleInherit ? "" : (value.short_title || common.wechat_short_title || "GasGx燃气发电挖矿"));
  const location = escapeHtml(locationInherit ? "" : (value.location || ""));
  const caption = escapeHtml(captionInherit ? "" : (value.caption || common.wechat_caption || ""));
  const isWechat = platform.key === "wechat";
  const extra = platform.key === "wechat" ? `
    <label>短标题
      <select name="platforms.${platform.key}.short_title_mode">
        <option value="inherit" ${shortTitleInherit ? "selected" : ""}>继承全局</option>
        <option value="custom" ${!shortTitleInherit ? "selected" : ""}>自定义</option>
      </select>
      <input name="platforms.${platform.key}.short_title" value="${shortTitle}" placeholder="GasGx燃气发电挖矿" ${shortTitleInherit ? "disabled" : ""}>
    </label>
    <label>位置
      <select name="platforms.${platform.key}.location_mode">
        <option value="inherit" ${locationInherit ? "selected" : ""}>继承全局</option>
        <option value="custom" ${!locationInherit ? "selected" : ""}>自定义</option>
      </select>
      <input name="platforms.${platform.key}.location" value="${location}" placeholder="留空则不显示位置" ${locationInherit ? "disabled" : ""}>
    </label>
    <label>视频号合集
      <select name="platforms.${platform.key}.collection_name">
        <option value="inherit" ${collectionInherit ? "selected" : ""}>继承全局</option>
        <option value="GasGx" ${value.collection_name === "GasGx" ? "selected" : ""}>GasGx</option>
        <option value="" ${value.collection_name === "" ? "selected" : ""}>不选择合集</option>
      </select>
    </label>
    <label>原创声明
      <select name="platforms.${platform.key}.declare_original">
        <option value="inherit" ${declareOriginalInherit ? "selected" : ""}>继承全局</option>
        <option value="false" ${declareOriginalFalse ? "selected" : ""}>不声明原创</option>
        <option value="true" ${declareOriginalTrue ? "selected" : ""}>声明原创</option>
      </select>
    </label>` : "";
  return `<article class="platform-settings-card" data-platform-card="${platform.key}">
    <div class="row-head">
      <strong>${platformName(platform.key)}</strong>
      <span class="chip">${platform.region === "cn" ? "国内" : "国外"}</span>
    </div>
    <label>启用发布配置
      <select name="platforms.${platform.key}.enabled">
        <option value="true" ${value.enabled !== false ? "selected" : ""}>启用</option>
        <option value="false" ${value.enabled === false ? "selected" : ""}>停用</option>
      </select>
    </label>
    <label>内容类型
      <select name="platforms.${platform.key}.content_type">
        ${isWechat ? `<option value="inherit" ${contentTypeInherit ? "selected" : ""}>继承全局</option>` : ""}
        <option value="short_video" ${(!isWechat && (value.content_type || "short_video") === "short_video") || (isWechat && !contentTypeInherit && value.content_type === "short_video") ? "selected" : ""}>短视频</option>
        <option value="image_text" ${value.content_type === "image_text" ? "selected" : ""}>图文</option>
        <option value="article" ${value.content_type === "article" ? "selected" : ""}>文章</option>
      </select>
    </label>
    <label>发布方式
      <select name="platforms.${platform.key}.publish_mode">
        <option value="inherit" ${(value.publish_mode || "inherit") === "inherit" ? "selected" : ""}>继承全局</option>
        <option value="publish" ${value.publish_mode === "publish" ? "selected" : ""}>立即发布</option>
        <option value="draft" ${value.publish_mode === "draft" ? "selected" : ""}>保存草稿</option>
      </select>
    </label>
    <label>可见范围
      <select name="platforms.${platform.key}.visibility">
        ${isWechat ? `<option value="inherit" ${visibilityInherit ? "selected" : ""}>继承全局</option>` : ""}
        <option value="public" ${(!isWechat && (value.visibility || "public") === "public") || (isWechat && !visibilityInherit && value.visibility === "public") ? "selected" : ""}>公开</option>
        <option value="private" ${value.visibility === "private" ? "selected" : ""}>仅自己可见</option>
        <option value="friends" ${value.visibility === "friends" ? "selected" : ""}>好友/粉丝可见</option>
      </select>
    </label>
    <label>评论权限
      <select name="platforms.${platform.key}.comment_permission">
        ${isWechat ? `<option value="inherit" ${commentPermissionInherit ? "selected" : ""}>继承全局</option>` : ""}
        <option value="public" ${(!isWechat && (value.comment_permission || "public") === "public") || (isWechat && !commentPermissionInherit && value.comment_permission === "public") ? "selected" : ""}>允许评论</option>
        <option value="closed" ${value.comment_permission === "closed" ? "selected" : ""}>关闭评论</option>
        <option value="followers" ${value.comment_permission === "followers" ? "selected" : ""}>仅粉丝评论</option>
      </select>
    </label>
    ${extra}
    <label class="wide-field">视频描述
      ${isWechat ? `<select name="platforms.${platform.key}.caption_mode">
        <option value="inherit" ${captionInherit ? "selected" : ""}>继承全局</option>
        <option value="custom" ${!captionInherit ? "selected" : ""}>自定义</option>
      </select>` : ""}
      <textarea name="platforms.${platform.key}.caption" rows="3" placeholder="留空则使用视频默认文案" ${isWechat && captionInherit ? "disabled" : ""}>${caption}</textarea>
    </label>
  </article>`;
}

function syncWechatInheritModeInputs(root = document) {
  const configs = [
    { mode: "platforms.wechat.short_title_mode", field: 'input[name="platforms.wechat.short_title"]' },
    { mode: "platforms.wechat.location_mode", field: 'input[name="platforms.wechat.location"]' },
    { mode: "platforms.wechat.caption_mode", field: 'textarea[name="platforms.wechat.caption"]' },
  ];
  configs.forEach(({ mode, field }) => {
    const selector = `select[name="${mode}"]`;
    root.querySelectorAll(selector).forEach((modeSelect) => {
      const label = modeSelect.closest("label");
      const target = label?.querySelector(field) || root.querySelector(field);
      if (!target) return;
      const inherit = String(modeSelect.value || "").toLowerCase() === "inherit";
      target.disabled = inherit;
    });
  });
}

function collectDistributionSettings(form) {
  const data = new FormData(form);
  const common = {
    material_dir: data.get("common.material_dir") || "runtime/materials/videos",
    material_dir_follows_video_matrix: data.get("common.material_dir_follows_video_matrix") === "true",
    publish_mode: data.get("common.publish_mode") || "publish",
    topics: data.get("common.topics") || "#天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿",
    upload_timeout: Number(data.get("common.upload_timeout") || 60),
    wechat_content_type: data.get("common.wechat_content_type") || "short_video",
    wechat_visibility: data.get("common.wechat_visibility") || "public",
    wechat_comment_permission: data.get("common.wechat_comment_permission") || "public",
    wechat_collection_name: data.get("common.wechat_collection_name") || "",
    wechat_declare_original: data.get("common.wechat_declare_original") === "true",
    wechat_short_title: data.get("common.wechat_short_title") || "GasGx燃气发电挖矿",
    wechat_location: data.get("common.wechat_location") || "",
    wechat_caption: data.get("common.wechat_caption") || "",
  };
  const jobs = {
    matrix_wechat_publish: {
      batch_size: Number(data.get("jobs.matrix_wechat_publish.batch_size") || 5),
      enabled: data.get("jobs.matrix_wechat_publish.enabled") === "true",
      schedule_mode: data.get("jobs.matrix_wechat_publish.schedule_mode") || "interval",
      daily_time: data.get("jobs.matrix_wechat_publish.daily_time") || "09:00",
      run_interval_minutes: Number(data.get("jobs.matrix_wechat_publish.run_interval_minutes") || 1440),
      batch_interval_min_minutes: Number(data.get("jobs.matrix_wechat_publish.batch_interval_min_minutes") || 5),
      batch_interval_max_minutes: Number(data.get("jobs.matrix_wechat_publish.batch_interval_max_minutes") || 15),
      rotate_start_group: data.get("jobs.matrix_wechat_publish.rotate_start_group") === "true",
      shuffle_within_batch: data.get("jobs.matrix_wechat_publish.shuffle_within_batch") === "true",
      retry_failed_last: data.get("jobs.matrix_wechat_publish.retry_failed_last") === "true",
    },
  };
  const platforms = {};
  PLATFORM_ORDER.forEach((platform) => {
    platforms[platform] = {
      enabled: data.get(`platforms.${platform}.enabled`) === "true",
      content_type: data.get(`platforms.${platform}.content_type`) || "short_video",
      publish_mode: data.get(`platforms.${platform}.publish_mode`) || "inherit",
      visibility: data.get(`platforms.${platform}.visibility`) || "public",
      comment_permission: data.get(`platforms.${platform}.comment_permission`) || "public",
      caption: data.get(`platforms.${platform}.caption`) || "",
      upload_timeout: common.upload_timeout,
    };
    if (platform === "wechat") {
      const shortTitleMode = data.get("platforms.wechat.short_title_mode") || "custom";
      const locationMode = data.get("platforms.wechat.location_mode") || "custom";
      const captionMode = data.get("platforms.wechat.caption_mode") || "custom";
      platforms[platform].collection_name = data.get("platforms.wechat.collection_name") || "";
      platforms[platform].declare_original = data.get("platforms.wechat.declare_original") || "inherit";
      platforms[platform].short_title = shortTitleMode === "inherit" ? "inherit" : (data.get("platforms.wechat.short_title") || "GasGx燃气发电挖矿");
      platforms[platform].location = locationMode === "inherit" ? "inherit" : (data.get("platforms.wechat.location") || "");
      platforms[platform].caption = captionMode === "inherit" ? "inherit" : (data.get("platforms.wechat.caption") || "");
    }
  });
  return { common, jobs, platforms };
}

function makeAccountKey(displayName, suffix) {
  const slug = displayName
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, "-")
    .replace(/^-+|-+$/g, "");
  const seq = suffix === "auto" ? String(Date.now()).slice(-4) : suffix;
  return `gasgx-${slug || "account"}-${seq}`;
}

function accountPhone(account) {
  const match = String(account?.notes || "").match(/账号手机号：(\d{11})/);
  return match ? match[1] : "";
}

function updateAccountPhoneHint() {
  const input = document.querySelector('#account-form input[name="phone"]');
  const hint = document.querySelector("#account-phone-hint");
  if (!input || !hint) return;
  const phone = input.value.trim();
  if (!/^\d{11}$/.test(phone)) {
    hint.innerHTML = "";
    hint.classList.remove("warning");
    return;
  }
  const matches = state.accounts.filter((account) => accountPhone(account) === phone);
  if (!matches.length) {
    hint.innerHTML = "";
    hint.classList.remove("warning");
    return;
  }
  hint.innerHTML = `<svg class="account-phone-hint-icon" aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M10.3 3.7 1.9 18a2 2 0 0 0 1.7 3h16.8a2 2 0 0 0 1.7-3L13.7 3.7a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg><span>该手机号已用于 ${matches.map((account) => `#${account.id} ${account.display_name}`).join("、")}，仍可继续创建。</span>`;
  hint.classList.add("warning");
}

function activateView(view, updateHash = true) {
  if (view === "terminal-execution") {
    state.terminalRoute = "hub";
  }
  const button = document.querySelector(`.nav-btn[data-view="${view}"]`);
  const section = document.querySelector(`#${view}`);
  if (!button || !section) return;
  document.querySelectorAll(".nav-btn").forEach((item) => item.classList.remove("active"));
  document.querySelectorAll(".view").forEach((item) => item.classList.remove("active"));
  button.classList.add("active");
  section.classList.add("active");
  currentView = view;
  document.body.classList.toggle("video-matrix-active", view === "video-matrix");
  setViewHeader(view);
  applyPermissionLimitedState();
  if (view === "video-matrix") {
    mountVideoMatrixWorkbench();
  } else {
    loadViewData(view).catch((error) => {
      const target = section.querySelector(".loading-inline") || section;
      target.innerHTML = `<div class="muted">加载失败：${error.message}</div>`;
    });
  }
  if (updateHash && window.location.hash !== `#${view}`) {
    const hash = view === "terminal-execution" ? "#terminal-execution" : `#${view}`;
    window.history.replaceState(null, "", hash);
  }
  window.scrollTo({ top: 0, left: 0 });
}

document.querySelectorAll(".nav-btn").forEach((button) => {
  button.addEventListener("click", () => {
    if (button.dataset.view === "terminal-execution" && currentView === "terminal-execution") {
      terminalSetRoute("hub");
      return;
    }
    activateView(button.dataset.view);
  });
});

document.querySelector("#refresh")?.addEventListener("click", async (event) => {
  const restoreButton = setButtonLoading(event.currentTarget, "刷新中");
  try {
    state.aiRobotConfigs = await api("/api/ai-robots/configs");
    state.aiRobotMessages = await api("/api/ai-robots/messages");
    renderAiRobot();
    document.querySelector("#ai-config-state").textContent = "已保存";
  } finally {
    restoreButton();
  }
});

document.querySelector("#account-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "创建中");
  const data = Object.fromEntries(new FormData(event.target).entries());
  const brandPrefix = String(data.brand_prefix || "").trim();
  const accountName = String(data.account_name || "").trim();
  const operatorWechat = String(data.operator_wechat || "").trim();
  const phone = String(data.phone || "").trim();
  updateAccountPhoneHint();
  if (!operatorWechat || operatorWechat === "__new__") {
    showTaskState("请先在下拉中新增运营微信号", "status-unsupported");
    restoreButton();
    return;
  }
  if (!/^\d{11}$/.test(phone)) {
    const phoneInput = event.target.elements.phone;
    phoneInput?.setCustomValidity("账号手机号需为 11 位数字");
    phoneInput?.reportValidity();
    phoneInput?.setCustomValidity("");
    restoreButton();
    return;
  }
  data.display_name = [brandPrefix, accountName].filter(Boolean).join(" ");
  data.account_key = makeAccountKey(data.display_name, "auto");
  data.niche = "短视频矩阵";
  data.notes = `绑定运营微信：${operatorWechat}；账号手机号：${phone}`;
  delete data.brand_prefix;
  delete data.account_name;
  delete data.operator_wechat;
  delete data.phone;
  data.platforms = PLATFORM_ORDER;
  try {
    const created = await api("/api/accounts", { method: "POST", body: JSON.stringify(data) });
    event.target.reset();
    setOperatorWechatValue("aamecc");
    await refresh();
    showAccountCreatedToast(created);
  } finally {
    restoreButton();
  }
});

document.querySelector("#operator-wechat-select")?.addEventListener("click", (event) => {
  const picker = event.currentTarget;
  const option = event.target.closest("[data-operator-wechat-option]");
  if (option) {
    setOperatorWechatValue(option.dataset.operatorWechatOption);
    return;
  }
  if (event.target.closest("#operator-wechat-add")) {
    addOperatorWechatOptionFromMenu();
    return;
  }
  if (event.target.closest(".inline-select-trigger")) {
    const menu = picker.querySelector(".inline-select-menu");
    const expanded = menu?.classList.toggle("hidden") === false;
    picker.querySelector(".inline-select-trigger")?.setAttribute("aria-expanded", String(expanded));
    if (expanded) picker.querySelector("#operator-wechat-add-input")?.focus();
  }
});
document.querySelector("#operator-wechat-add-input")?.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    addOperatorWechatOptionFromMenu();
  }
});
document.querySelector('#account-form input[name="phone"]')?.addEventListener("input", updateAccountPhoneHint);
document.addEventListener("click", (event) => {
  const picker = document.querySelector("#operator-wechat-select");
  if (!picker || picker.contains(event.target)) return;
  picker.querySelector(".inline-select-menu")?.classList.add("hidden");
  picker.querySelector(".inline-select-trigger")?.setAttribute("aria-expanded", "false");
});

document.querySelector("#task-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "加入中");
  const data = Object.fromEntries(new FormData(event.target).entries());
  data.account_id = data.account_id ? Number(data.account_id) : null;
  showTaskState("加入队列中...");
  try {
    await api("/api/tasks", { method: "POST", body: JSON.stringify(data) });
    showTaskState("已加入队列。");
    event.target.reset();
    await refresh();
  } catch (error) {
    showTaskState(formatFriendlyMessage(error.message), "status-unsupported");
  } finally {
    restoreButton();
  }
});

document.querySelector("#tasks-list").addEventListener("change", (event) => {
  const filter = event.target.closest("[data-task-filter]");
  if (filter) {
    taskFilters[filter.dataset.taskFilter] = filter.value;
    renderTasks();
    return;
  }
  const selectAll = event.target.closest("[data-task-select-all]");
  if (selectAll) {
    const ids = filteredTasks().map((task) => Number(task.id));
    ids.forEach((id) => selectAll.checked ? taskSelection.add(id) : taskSelection.delete(id));
    renderTasks();
    return;
  }
  const taskCheckbox = event.target.closest("[data-task-select]");
  if (taskCheckbox) {
    const id = Number(taskCheckbox.dataset.taskSelect);
    if (taskCheckbox.checked) taskSelection.add(id);
    else taskSelection.delete(id);
    renderTasks();
  }
});

installTerminalConfigInteractions("#terminal-config-list");
installTerminalConfigInteractions("#settings-platform-config-list");

document.addEventListener("change", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLSelectElement)) return;
  const name = String(target.name || "");
  if (!name.endsWith("_mode")) return;
  if (!name.startsWith("platforms.wechat.")) return;
  const root = target.closest("form, .platform-settings-grid, .platform-settings-card") || document;
  syncWechatInheritModeInputs(root);
});

document.querySelectorAll("[data-terminal-init-card]").forEach((button) => {
  button.addEventListener("click", async () => {
    currentTerminalInitCard = button.dataset.terminalInitCard === "platform" ? "platform" : "window";
    if (currentTerminalInitCard === "platform") {
      await renderTerminalPlatformPublishPanel();
    }
    renderTerminalInitCardMode();
  });
});


document.querySelector("#terminal-save-platform-config")?.addEventListener("click", async (event) => {
  const restoreButton = setButtonLoading(event.currentTarget, "更新中");
  try {
    if (!state.distributionSettings) state.distributionSettings = await api("/api/settings/distribution");
    const root = document.querySelector("#terminal-platform-publish-list");
    if (!root) throw new Error("平台配置容器不存在");
    const platformKey = terminalCurrentPlatformKey();
    const nextPlatformSetting = collectTerminalPlatformSetting(root, platformKey);
    const next = {
      common: state.distributionSettings.common || {},
      jobs: state.distributionSettings.jobs || {},
      platforms: {
        ...(state.distributionSettings.platforms || {}),
        [platformKey]: nextPlatformSetting,
      },
    };
    // Optimistic local update to avoid immediate UI fallback to stale values.
    state.distributionSettings = next;
    await api("/api/settings/distribution", {
      method: "PATCH",
      body: JSON.stringify(next),
    });
    state.distributionSettings = await api("/api/settings/distribution");
    await renderTerminalPlatformPublishPanel();
  } catch (error) {
    window.alert(`更新平台配置失败：${formatFriendlyMessage(error?.message || "unknown error")}`);
  } finally {
    restoreButton();
  }
});

document.querySelector("#terminal-start-login-legacy")?.addEventListener("click", async (event) => {
  const restoreButton = setButtonLoading(event.currentTarget, "启动中");
  try {
    if (!state.terminalExecution.initialized || state.terminalConfigOpen) {
      state.terminalExecution = await api("/api/terminal-execution/start", {
        method: "POST",
        body: JSON.stringify({ windows: readTerminalConfigRows() }),
      });
    }
    state.terminalExecution = await api("/api/terminal-execution/start-login", { method: "POST" });
    state.terminalQrVisible = true;
    state.terminalConfigOpen = false;
    renderTerminalExecution();
  } finally {
    restoreButton();
  }
});

document.querySelector("#terminal-edit-config-legacy")?.addEventListener("click", () => {
  state.terminalConfigOpen = true;
  renderTerminalExecution();
});

document.querySelector("#distribution-settings-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "保存中");
  const stateNode = document.querySelector("#settings-save-state");
  stateNode.textContent = "保存中...";
  try {
    await api("/api/settings/distribution", {
      method: "PATCH",
      body: JSON.stringify(collectDistributionSettings(event.target)),
    });
    stateNode.textContent = "已保存，下一次矩阵分发会按全局配置和平台独立配置执行。";
    await refresh();
  } finally {
    restoreButton();
  }
});

document.querySelector("#ai-platform-select").addEventListener("change", () => {
  renderAiRobot();
  syncTelegramSetupVisibility();
});

document.querySelector("#telegram-auto-fill")?.addEventListener("click", fillTelegramFields);

document.querySelector("#telegram-fetch-chat-id")?.addEventListener("click", async (event) => {
  const restoreButton = setButtonLoading(event.currentTarget, "Fetching");
  try {
    await fetchTelegramChatId();
  } finally {
    restoreButton();
  }
});

document.querySelector("#ai-message-toggle")?.addEventListener("click", () => {
  state.aiRobotMessagesCollapsed = !state.aiRobotMessagesCollapsed;
  renderAiRobot();
});

document.querySelector("#telegram-open-bot-chat")?.addEventListener("click", openTelegramBotChat);
document.querySelector("#telegram-open-bot-chat-primary")?.addEventListener("click", openTelegramBotChat);

document.querySelector("#telegram-open-updates")?.addEventListener("click", () => {
  const form = document.querySelector("#ai-robot-form");
  const token = String(form?.elements.telegram_bot_token?.value || "").trim();
  if (!token) {
    window.alert("Fill Bot token first.");
    return;
  }
  window.open(`https://api.telegram.org/bot${encodeURIComponent(token)}/getUpdates`, "_blank", "noopener,noreferrer");
});

document.querySelector("#ai-save-config")?.addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const form = document.querySelector("#ai-robot-form");
  const stateNode = document.querySelector("#ai-config-state");
  stateNode.textContent = "保存中...";
  stateNode.classList.remove("danger");
  let saved = false;
  const restoreButton = setButtonLoading(button, "保存中");
  try {
    if (form.elements.platform.value === "telegram") {
      const token = String(form.elements.telegram_bot_token?.value || "").trim();
      if (token && !String(form.elements.telegram_chat_id?.value || "").trim()) {
        await fetchTelegramChatId();
      }
      const chatId = String(form.elements.telegram_chat_id?.value || "").trim();
      if (token) {
        form.elements.platform.value = "telegram";
        form.elements.enabled.value = "true";
        form.elements.bot_name.value = form.elements.bot_name.value || "GasGx Telegram Bot";
        form.elements.webhook_url.value = telegramWebhookUrl(token);
        form.elements.target_id.value = chatId || form.elements.target_id.value;
        form.elements.webhook_secret.value = "";
        if (!form.elements.signing_secret.value) {
          form.elements.signing_secret.value = `gasgx-${Date.now().toString(36)}`;
        }
      }
    }
    const data = Object.fromEntries(new FormData(form).entries());
    const platform = data.platform;
    if (isWebhookOnlyAiRobot(platform)) {
      const existing = aiRobotConfigFor(platform);
      data.enabled = existing.webhook_url ? String(existing.enabled === true) : "true";
      data.bot_name = data.bot_name || `${aiPlatformLabel(platform)}机器人`;
      data.webhook_secret = "";
      data.signing_secret = "";
      data.target_id = "";
    }
    delete data.platform;
    delete data.test_text;
    delete data.telegram_bot_token;
    delete data.telegram_chat_id;
    data.enabled = data.enabled === "true";
    await api(`/api/ai-robots/${platform}/config`, { method: "PUT", body: JSON.stringify(data) });
    state.aiRobotConfigs = await api("/api/ai-robots/configs");
    state.aiRobotMessages = await api("/api/ai-robots/messages");
    state.aiRobotEditingPlatform = "";
    renderAiRobot();
    stateNode.textContent = "已保存";
    saved = true;
  } catch (error) {
    stateNode.textContent = error.message || "保存失败";
    stateNode.classList.add("danger");
  } finally {
    restoreButton();
    if (saved) {
      button.textContent = "已保存";
    }
  }
});

document.querySelector("#ai-save-config-panel")?.addEventListener("click", () => {
  const form = document.querySelector("#ai-robot-form");
  const button = document.querySelector("#ai-save-config-panel");
  if (form && button) saveAiRobotConfig(form, button);
});

document.querySelector("#ai-send-test-panel")?.addEventListener("click", () => {
  const form = document.querySelector("#ai-robot-form");
  const button = document.querySelector("#ai-send-test-panel");
  if (form && button) sendAiRobotTest(form.elements.platform.value, button);
});

document.querySelector("#ai-copy-lark-callback")?.addEventListener("click", async (event) => {
  const input = document.querySelector("#ai-lark-callback-url");
  if (!input) return;
  try {
    await navigator.clipboard.writeText(input.value);
    event.currentTarget.textContent = "已复制";
  } catch {
    input.select();
    document.execCommand("copy");
    event.currentTarget.textContent = "已复制";
  }
});

async function sendAiRobotTest(platform, button) {
  const form = document.querySelector("#ai-robot-form");
  const stateNode = document.querySelector("#ai-config-state");
  const text = form && !form.hidden && form.elements.platform.value === platform
    ? (form.elements.test_text.value || "GasGx AI robot test message")
    : "GasGx AI robot test message";
  stateNode.textContent = "发送中...";
  stateNode.classList.remove("danger");
  let finalButtonText = "";
  const restoreButton = setButtonLoading(button, "发送中");
  try {
    const result = await api(`/api/ai-robots/${platform}/test-message`, {
      method: "POST",
      body: JSON.stringify({ message_type: "text", text }),
    });
    state.aiRobotConfigs = await api("/api/ai-robots/configs");
    state.aiRobotMessages = await api("/api/ai-robots/messages");
    renderAiRobot();
    if (result.status === "sent") {
      stateNode.textContent = "测试消息已发送";
      finalButtonText = "已发送";
      return;
    }
    stateNode.textContent = `发送失败：${result.error || result.summary || result.status || "未知错误"}`;
    stateNode.classList.add("danger");
    finalButtonText = "发送失败";
  } catch (error) {
    stateNode.textContent = `发送失败：${error.message || "未知错误"}`;
    stateNode.classList.add("danger");
    finalButtonText = "发送失败";
  } finally {
    restoreButton();
    if (finalButtonText) {
      button.textContent = finalButtonText;
    }
  }
}

function renderAiRobotLoading() {
  const loading = `<div class="loading-inline"><span class="btn-spinner" aria-hidden="true"></span><span>加载中...</span></div>`;
  const channelGrid = document.querySelector("#ai-channel-grid");
  const messageList = document.querySelector("#ai-message-list");
  if (channelGrid) channelGrid.innerHTML = loading;
  if (messageList && !state.aiRobotMessagesCollapsed) messageList.innerHTML = loading;
}

document.querySelector("#ai-send-test")?.addEventListener("click", async (event) => {
  const form = document.querySelector("#ai-robot-form");
  await sendAiRobotTest(form.elements.platform.value, event.currentTarget);
});

document.querySelector("#open-material-dir").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const form = document.querySelector("#distribution-settings-form");
  const materialDir = form.elements["common.material_dir"].value || "runtime/materials/videos";
  const password = await confirmSuperAdminPassword();
  if (!password) return;
  const restoreButton = setButtonLoading(button, "打开中...");
  try {
    await api("/api/settings/material-dir/open", {
      method: "POST",
      body: JSON.stringify({ material_dir: materialDir, password }),
    });
  } finally {
    restoreButton();
  }
});

function confirmMatrixRunNow() {
  const modal = document.querySelector("#matrixRunConfirmModal");
  if (!modal) return Promise.resolve(true);
  modal.classList.remove("hidden");
  const submit = document.querySelector("#matrixRunConfirmSubmit");
  const cancel = document.querySelector("#matrixRunConfirmCancel");
  const closeButton = document.querySelector("#matrixRunConfirmClose");
  return new Promise((resolve) => {
    const close = (confirmed) => {
      modal.classList.add("hidden");
      if (submit) submit.onclick = null;
      if (cancel) cancel.onclick = null;
      if (closeButton) closeButton.onclick = null;
      resolve(confirmed);
    };
    if (submit) submit.onclick = () => close(true);
    if (cancel) cancel.onclick = () => close(false);
    if (closeButton) closeButton.onclick = () => close(false);
  });
}

document.querySelector("#terminalErrorClose")?.addEventListener("click", hideTerminalErrorModal);
document.querySelector("#terminalErrorDismiss")?.addEventListener("click", hideTerminalErrorModal);
document.querySelector("#terminalErrorModal")?.addEventListener("click", (event) => {
  if (event.target === event.currentTarget) hideTerminalErrorModal();
});

document.querySelector("#matrix-run-now").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const confirmed = await confirmMatrixRunNow();
  if (!confirmed) return;
  const restoreButton = setButtonLoading(button, "启动中");
  try {
    await api("/api/jobs/matrix-wechat/run-now", { method: "POST" });
    await refresh();
  } finally {
    restoreButton();
  }
});

setInterval(() => {
  if (!loadedViews.has("settings")) return;
  api("/api/jobs/matrix-wechat/status")
    .then((matrixJobStatus) => {
      state.matrixJobStatus = matrixJobStatus;
      if (currentView === "settings") renderMatrixJobStatus();
    })
    .catch(() => {});
}, 15000);

setInterval(() => {
  if (!loadedViews.has("stats")) return;
  api("/api/jobs/matrix-wechat/stats-capture/status")
    .then((statsCaptureStatus) => {
      state.statsCaptureStatus = statsCaptureStatus;
      if (currentView === "stats") renderStats();
    })
    .catch(() => {});
}, 15000);

document.querySelector("#matrix-stats-capture-run-now")?.addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const restoreButton = setButtonLoading(button, "采集中");
  try {
    await api("/api/jobs/matrix-wechat/stats-capture/run-now", {
      method: "POST",
      body: JSON.stringify({ target_date: "", limit: 0, dry_run: false }),
    });
    state.statsCaptureStatus = await api("/api/jobs/matrix-wechat/stats-capture/status");
    renderStats();
  } finally {
    restoreButton();
  }
});

document.addEventListener("click", async (event) => {
  const routeButton = event.target.closest("[data-notice-route]");
  if (routeButton) {
    const eventType = routeButton.dataset.noticeRoute;
    const platform = routeButton.dataset.noticePlatform;
    const enabled = routeButton.dataset.noticeEnabled === "1";
    const restoreButton = setButtonLoading(routeButton, "保存中");
    try {
      await api(`/api/notification-routes/${eventType}/${platform}`, {
        method: "POST",
        body: JSON.stringify({ enabled }),
      });
      state.notificationRoutes = await api("/api/notification-routes");
      renderOperationNotifications();
    } finally {
      restoreButton();
    }
    return;
  }

  const deleteButton = event.target.closest("[data-delete-task]");
  if (deleteButton) {
    const taskId = deleteButton.dataset.deleteTask;
    const restoreButton = setButtonLoading(deleteButton, "删除中");
    try {
      await api(`/api/tasks/${taskId}`, { method: "DELETE" });
      taskSelection.delete(Number(taskId));
      await refresh();
    } finally {
      restoreButton();
    }
    return;
  }

  const bulkStatusButton = event.target.closest("[data-task-bulk-status]");
  if (bulkStatusButton) {
    const ids = Array.from(taskSelection);
    if (!ids.length) return;
    const status = bulkStatusButton.dataset.taskBulkStatus;
    const restoreButton = setButtonLoading(bulkStatusButton, "调整中");
    try {
      await api("/api/tasks/bulk-status", {
        method: "POST",
        body: JSON.stringify({ ids, status }),
      });
      taskSelection.clear();
      await refresh();
    } finally {
      restoreButton();
    }
    return;
  }

  const bulkDeleteButton = event.target.closest("[data-task-bulk-delete]");
  if (bulkDeleteButton) {
    const ids = Array.from(taskSelection);
    if (!ids.length) return;
    if (!window.confirm(`确认删除已选 ${ids.length} 条队列任务？`)) return;
    const restoreButton = setButtonLoading(bulkDeleteButton, "删除中");
    try {
      await api("/api/tasks/bulk-delete", {
        method: "POST",
        body: JSON.stringify({ ids }),
      });
      taskSelection.clear();
      await refresh();
    } finally {
      restoreButton();
    }
    return;
  }

  const terminalManualButton = event.target.closest("[data-terminal-manual]");
  if (terminalManualButton) {
    const restoreButton = setButtonLoading(terminalManualButton, "发布中");
    terminalErrorModalSignature = "";
    hideTerminalErrorModal();
    try {
      state.terminalExecution = await api(`/api/terminal-execution/windows/${terminalManualButton.dataset.terminalManual}/manual-publish`, { method: "POST" });
      renderTerminalExecution();
    } catch (error) {
      showTerminalErrorModal({
        stage: "publish_start",
        title: "发布请求失败",
        message: error.message || "发布请求失败",
        context: `窗口 #${terminalManualButton.dataset.terminalManual}`,
        signature: `publish-request|manual|${terminalManualButton.dataset.terminalManual}|${error.message || "unknown"}`,
      });
    } finally {
      restoreButton();
    }
    return;
  }

  const terminalQrRefreshButton = event.target.closest("[data-terminal-qr-refresh]");
  if (terminalQrRefreshButton) {
    const [windowId, accountId] = String(terminalQrRefreshButton.dataset.terminalQrRefresh || "").split(":");
    const windowIdNumber = Number(windowId);
    if (!windowIdNumber) return;
    let accountIdNumber = Number(accountId);
    if (!accountIdNumber) {
      const targetWindow = (state.terminalExecution.windows || []).find((item) => Number(item.id) === windowIdNumber);
      const currentIndex = Number(targetWindow?.current_index || 0);
      accountIdNumber = Number(targetWindow?.accounts?.[currentIndex]?.id || 0);
    }
    if (!accountIdNumber) return;
    await refreshTerminalAccountQr(windowIdNumber, accountIdNumber, terminalQrRefreshButton);
    return;
  }

  const terminalConfirmButton = event.target.closest("[data-terminal-confirm-success]");
  if (terminalConfirmButton) {
    const restoreButton = setButtonLoading(terminalConfirmButton, "进入下一个");
    terminalErrorModalSignature = "";
    hideTerminalErrorModal();
    try {
      state.terminalExecution = await api(`/api/terminal-execution/windows/${terminalConfirmButton.dataset.terminalConfirmSuccess}/confirm-publish-success`, { method: "POST" });
      renderTerminalExecution();
    } catch (error) {
      showTerminalErrorModal({
        stage: "confirm",
        title: "确认请求失败",
        message: error.message || "确认请求失败",
        context: `窗口 #${terminalConfirmButton.dataset.terminalConfirmSuccess}`,
        signature: `publish-request|confirm|${terminalConfirmButton.dataset.terminalConfirmSuccess}|${error.message || "unknown"}`,
      });
    } finally {
      restoreButton();
    }
    return;
  }

  const deleteAccountButton = event.target.closest("[data-delete-account]");
  if (deleteAccountButton) {
    const accountId = deleteAccountButton.dataset.deleteAccount;
    const accountName = deleteAccountButton.dataset.accountName || `#${accountId}`;
    if (!window.confirm(`确认删除矩阵账号「${accountName}」？相关平台、浏览器配置和任务记录会一并删除。`)) return;
    const restoreButton = setButtonLoading(deleteAccountButton, "删除中...");
    try {
      await api(`/api/accounts/${accountId}`, { method: "DELETE" });
      await refresh();
    } finally {
      restoreButton();
    }
    return;
  }

  const target = event.target.closest("[data-open]");
  if (!target) return;
  const [accountId, platform] = target.dataset.open.split(":");
  const originalText = target.textContent;
  const successText = `${platformLabel(platform)}已打开`;
  target.disabled = true;
  target.classList.add("loading");
  target.innerHTML = `<span class="btn-spinner" aria-hidden="true"></span><span>打开中</span>`;
  try {
    await api(`/api/accounts/${accountId}/platforms/${platform}/open-browser`, { method: "POST" });
    target.classList.add("opened");
    target.textContent = successText;
    setTimeout(() => {
      if (target.classList.contains("opened")) {
        target.classList.remove("opened");
        target.textContent = originalText;
      }
    }, 3500);
  } catch (error) {
    target.textContent = originalText;
    throw error;
  } finally {
    target.classList.remove("loading");
    target.disabled = false;
  }
});

refresh().catch((error) => {
  document.querySelector("#summary").innerHTML = `<div class="metric"><span>加载失败</span><strong>${error.message}</strong></div>`;
});
setViewHeader(document.querySelector(".nav-btn.active")?.dataset.view || "overview");
renderThemePalette();
initBrandSettings();
initSystemInitialize();
initSystemDirectoryActions();
initSupabaseReadCacheClear();
document.querySelector("#database-dictionary-locale-toggle")?.addEventListener("click", toggleDatabaseDictionaryLocale);
initUserMenu();
initPermissionGuards();
initAuthCenter();
initHelpCenter();
installGlobalButtonLoading();


/*
const vm = {
  loaded: false,
  state: {},
  settings: {},
  templates: {},
  coverTemplates: {},
  selectedCover: "",
  selectedVideoTemplate: "",
};

const vmCoverFields = [
  ["name", "模板名称", "text"], ["brand", "品牌文字", "text"], ["eyebrow", "眉标文字", "text"], ["cta", "CTA 按钮文字", "text"],
  ["align", "对齐方式", "select"], ["brand_y", "品牌 Y", "range", 0, 420], ["headline_y", "主标题 Y", "range", 0, 1320],
  ["subhead_y", "副标题 Y", "range", 0, 1500], ["hud_y", "HUD Y", "range", 0, 1780], ["cta_y", "CTA Y", "range", 0, 1840],
  ["primary_color", "主文字颜色", "color"], ["secondary_color", "辅助文字颜色", "color"], ["accent_color", "强调色", "color"],
  ["tint_color", "底色", "color"], ["gradient_color", "渐变色", "color"], ["panel_color", "HUD 背景色", "color"],
  ["tint_opacity", "底色透明度", "rangeFloat", 0, 1], ["gradient_opacity", "渐变透明度", "rangeFloat", 0, 1],
  ["panel_opacity", "HUD 背景透明度", "rangeFloat", 0, 1],
];

function vmNode(id) { return document.querySelector(`#${id}`); }

async function vmApi(path, options = {}) {
  const response = await fetch(`/api/video-matrix${path}`, { headers: { "Content-Type": "application/json" }, ...options });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || response.statusText);
  }
  return response.json();
}

async function initVideoMatrix() {
  if (vm.loaded) return;
  const data = await vmApi("/state");
  vm.loaded = true;
  vm.state = data.ui_state;
  vm.settings = data.settings;
  vm.templates = data.templates;
  vm.coverTemplates = data.cover_templates;
  vm.selectedCover = vm.state.cover_template_id || Object.keys(vm.coverTemplates)[0];
  vm.selectedVideoTemplate = vm.state.template_id || Object.keys(vm.templates)[0];
  renderVideoMatrixSidebar(data);
  renderVideoMatrixSource(data);
  renderVideoMatrixTextSettings();
  renderVideoMatrixSelector();
  renderVideoMatrixEditor();
  await refreshVideoMatrixPreviews();
}

function renderVideoMatrixSidebar(data) {
  vmNode("vm-output-count").value = vm.state.output_count || vm.settings.output_count;
  vmNode("vm-max-workers").value = vm.state.max_workers || 3;
  vmSyncRange("vm-output-count");
  vmSyncRange("vm-max-workers");
  vmNode("vm-output-root").value = vm.settings.output_root;
  vmSetMulti(vmNode("vm-output-options"), vm.state.output_options || ["mp4"]);
  vmNode("vm-video-template").innerHTML = Object.entries(vm.templates).map(([id, item]) => `<option value="${id}">${item.name || id}</option>`).join("");
  vmNode("vm-video-template").value = vm.selectedVideoTemplate;
  vmNode("vm-video-template").onchange = () => { vm.selectedVideoTemplate = vmNode("vm-video-template").value; };
  vmNode("vm-open-output").onclick = () => vmOpenFolder(vmNode("vm-output-root").value);
  renderVmRadio("vm-language-group", "vm_copy_language", [["zh", "中文"], ["en", "英文"], ["ru", "俄文"]], vm.state.copy_language || "zh");
  renderVideoMatrixBgm(data);
  vmNode("video-matrix-save-state").onclick = saveVideoMatrixState;
}

function renderVideoMatrixSource(data) {
  const total = Object.values(data.category_counts).reduce((sum, value) => sum + value, 0);
  const categories = vmMaterialCategories(data);
  vmNode("video-matrix-metrics").innerHTML = [
    `<div class="metric"><span>本地素材</span><strong>${total}</strong></div>`,
    `<div class="metric"><span>生成数量</span><strong id="vm-metric-count">${vmNode("vm-output-count").value}</strong></div>`,
    `<div class="metric"><span>并行线程</span><strong id="vm-metric-workers">${vmNode("vm-max-workers").value}</strong></div>`,
    `<div class="metric"><span>默认比例</span><strong>1080:1920</strong></div>`,
  ].join("");
  vmNode("vm-source-dirs").innerHTML = categories.map((category) => `
    <div class="vm-dir-row"><span class="vm-badge">${vmEscape(category.label)}</span><code>${vmEscape(data.source_dirs[category.id] || "")}</code><button class="btn primary" data-vm-open="${vmEscape(data.source_dirs[category.id] || "")}">鎵撳紑</button></div>
  `).join("");
  vmNode("vm-source-dirs").querySelectorAll("[data-vm-open]").forEach((button) => { button.onclick = () => vmOpenFolder(button.dataset.vmOpen); });
  vmNode("vm-source-counts").textContent = `褰撳墠绱犳潗鏁伴噺锛${categories.map((category) => `${category.label}=${data.category_counts[category.id] || 0}`).join(" / ")}`;
  renderVmRadio("vm-source-mode-group", "vm_source_mode", [["Category folders", "鍒嗙被鐩綍"], ["Upload files", "鎵嬪姩涓婁紶"]], vm.state.source_mode || "Category folders", updateVideoMatrixSourceMode);
  vmNode("vm-recent-limits").innerHTML = categories.map((category) => `
    <label>${vmEscape(category.label)} 绫昏鍙栨渶鏂扮礌鏉?input id="vm-${category.id}" type="range" min="1" max="50" value="${vm.settings.recent_limits[category.id] || 8}"><strong id="vm-${category.id}-value"></strong></label>
  `).join("");
  categories.forEach((category) => vmSyncRange(`vm-${category.id}`));
  updateVideoMatrixSourceMode();
}

function renderVideoMatrixTextSettings() {
  vmNode("vm-headline").value = vm.state.headline || "";
  vmNode("vm-subhead").value = vm.state.subhead || "";
  vmNode("vm-cta").value = vm.state.cta || "";
  vmNode("vm-follow-text").value = vm.state.follow_text || "";
  vmNode("vm-hud-text").value = vm.state.hud_text || "";
  ["vm-headline", "vm-subhead", "vm-cta", "vm-hud-text"].forEach((id) => vmNode(id).addEventListener("input", vmDebounce(refreshVideoMatrixPreviews, 250)));
  vmNode("vm-generate").onclick = generateVideoMatrix;
}

function renderVideoMatrixSelector() {
  vmNode("vm-cover-selector").innerHTML = Object.entries(vm.coverTemplates).map(([id, item]) => `
    <button class="btn secondary ${id === vm.selectedCover ? "active" : ""}" data-vm-cover="${id}" type="button">${item.name || id}</button>
  `).join("");
  vmNode("vm-cover-selector").querySelectorAll("[data-vm-cover]").forEach((button) => {
    button.onclick = async () => { vm.selectedCover = button.dataset.vmCover; renderVideoMatrixSelector(); renderVideoMatrixEditor(); await refreshVideoMatrixPreviews(); };
  });
}

function renderVideoMatrixEditor() {
  const template = vm.coverTemplates[vm.selectedCover];
  vmNode("vm-preview-caption").textContent = `${vm.selectedCover} / ${template.name || vm.selectedCover}`;
  const fields = [`<h3>当前模板独立编辑区</h3>`];
  vmCoverFields.forEach(([key, label, type, min, max]) => {
    const value = template[key] ?? "";
    if (type === "select") fields.push(`<label>${label}<select data-vm-key="${key}"><option value="left">left</option><option value="center">center</option></select></label>`);
    else if (type === "range" || type === "rangeFloat") fields.push(`<label>${label}<input data-vm-key="${key}" type="range" min="${min}" max="${max}" step="${type === "rangeFloat" ? "0.01" : "1"}" value="${value}"><strong>${value}</strong></label>`);
    else fields.push(`<label>${label}<input data-vm-key="${key}" type="${type}" value="${vmEscape(value)}"></label>`);
  });
  fields.push(`<button class="btn primary" type="button" id="vm-save-cover">保存这个封面模板</button>`);
  vmNode("vm-cover-form").innerHTML = fields.join("");
  vmNode("vm-cover-form").querySelectorAll("[data-vm-key]").forEach((input) => {
    input.value = template[input.dataset.vmKey] ?? input.value;
    input.oninput = () => {
      const key = input.dataset.vmKey;
      template[key] = input.type === "range" ? Number(input.value) : input.value;
      const valueNode = input.parentElement.querySelector("strong");
      if (valueNode) valueNode.textContent = input.value;
      refreshVideoMatrixPreviews();
    };
  });
  vmNode("vm-save-cover").onclick = saveVideoMatrixCoverTemplate;
}

async function refreshVideoMatrixPreviews() { await refreshVideoMatrixMainPreview(); await refreshVideoMatrixGallery(); }

async function refreshVideoMatrixMainPreview() {
  const data = await vmApi("/cover-preview", { method: "POST", body: JSON.stringify(vmPreviewPayload(vm.coverTemplates[vm.selectedCover])) });
  vmNode("vm-cover-preview").src = data.data_url;
}

async function refreshVideoMatrixGallery() {
  const cards = [];
  for (const [id, template] of Object.entries(vm.coverTemplates)) {
    const data = await vmApi("/cover-preview", { method: "POST", body: JSON.stringify(vmPreviewPayload(template)) });
    cards.push(`<div class="vm-cover-card ${id === vm.selectedCover ? "active" : ""}" data-vm-gallery="${id}"><img src="${data.data_url}" alt=""><span>${id} / ${template.name || id}</span></div>`);
  }
  vmNode("vm-cover-gallery").innerHTML = cards.join("");
  vmNode("vm-cover-gallery").querySelectorAll("[data-vm-gallery]").forEach((card) => {
    card.onclick = async () => { vm.selectedCover = card.dataset.vmGallery; renderVideoMatrixSelector(); renderVideoMatrixEditor(); await refreshVideoMatrixPreviews(); };
  });
}

function vmPreviewPayload(template) {
  const payload = { ...template };
  if (vmNode("vm-cta").value) payload.cta = vmNode("vm-cta").value;
  return { template: payload, headline: vmNode("vm-headline").value, subhead: vmNode("vm-subhead").value, hud_text: vmNode("vm-hud-text").value };
}

async function saveVideoMatrixCoverTemplate() {
  await vmApi(`/cover-templates/${vm.selectedCover}`, { method: "POST", body: JSON.stringify(vm.coverTemplates[vm.selectedCover]) });
  await saveVideoMatrixState();
  vmLog(`已保存封面模板：${vm.coverTemplates[vm.selectedCover].name || vm.selectedCover}`);
}

async function saveVideoMatrixState() {
  vm.state = collectVideoMatrixState();
  await vmApi("/state", { method: "POST", body: JSON.stringify(vm.state) });
  vmLog("已保存当前设置");
}

async function generateVideoMatrix() {
  const form = new FormData();
  form.append("payload", JSON.stringify(collectVideoMatrixState()));
  const bgm = vmNode("vm-bgm-upload")?.files?.[0];
  if (bgm) form.append("bgm_file", bgm);
  [...(vmNode("vm-source-files").files || [])].forEach((file) => form.append("source_files", file));
  const response = await fetch("/api/video-matrix/generate", { method: "POST", body: form });
  if (!response.ok) throw new Error(await response.text());
  const { job_id } = await response.json();
  pollVideoMatrixJob(job_id);
}

async function pollVideoMatrixJob(jobId) {
  const job = await vmApi(`/jobs/${jobId}`);
  vmNode("vm-progress-bar").style.width = `${Math.round((job.progress || 0) * 100)}%`;
  vmLog(`${job.status}: ${job.message || ""}${job.error ? `\n${job.error}` : ""}`);
  if (job.status === "complete") vmLog(`完成\n${job.assets.map((asset) => asset.video_path).join("\n")}`);
  else if (job.status !== "error") setTimeout(() => pollVideoMatrixJob(jobId), 1200);
}

function collectVideoMatrixState() {
  const categories = Array.isArray(vm.settings.material_categories) ? vm.settings.material_categories : [];
  return {
    output_count: Number(vmNode("vm-output-count").value), max_workers: Number(vmNode("vm-max-workers").value),
    output_options: [...vmNode("vm-output-options").selectedOptions].map((item) => item.value), output_root: vmNode("vm-output-root").value,
    template_id: vm.selectedVideoTemplate, cover_template_id: vm.selectedCover, copy_language: vmRadioValue("vm_copy_language"),
    source_mode: vmRadioValue("vm_source_mode"), headline: vmNode("vm-headline").value, subhead: vmNode("vm-subhead").value,
    follow_text: vmNode("vm-follow-text").value, hud_text: vmNode("vm-hud-text").value,
    bgm_source: vmRadioValue("vm_bgm_source"), bgm_library_id: vmNode("vm-bgm-library")?.value || "",
    recent_limits: Object.fromEntries(categories.map((category) => [category.id, Number(vmNode(`vm-${category.id}`)?.value || vm.settings.recent_limits[category.id] || 8)])),
  };
}

function vmMaterialCategories(data = { settings: vm.settings }) {
  const source = data.settings || vm.settings;
  const categories = Array.isArray(source.material_categories) ? source.material_categories : [];
  return categories.length ? categories : [
    { id: "category_A", label: "A 类" },
    { id: "category_B", label: "B 类" },
    { id: "category_C", label: "C 类" },
  ];
}

function renderVideoMatrixBgm(data) {
  vmNode("vm-bgm-panel").innerHTML = `<div class="radio-line" id="vm-bgm-source-group"></div><select id="vm-bgm-library"></select><input id="vm-bgm-upload" type="file" accept=".mp3,.wav,.m4a"><div class="muted">${Object.values(data.bgm_library || {}).map((item) => `<a href="${item.download_page}" target="_blank">${item.name}</a>`).join("<br>")}</div>`;
  renderVmRadio("vm-bgm-source-group", "vm_bgm_source", [["Upload file", "上传文件"], ["Local library", "本地音乐库"]], vm.state.bgm_source || "Upload file", updateVideoMatrixBgmMode);
  vmNode("vm-bgm-library").innerHTML = data.local_bgm.map((name) => `<option>${name}</option>`).join("");
  vmNode("vm-bgm-library").value = vm.state.bgm_library_id || "";
  updateVideoMatrixBgmMode();
}
function updateVideoMatrixBgmMode() { const local = vmRadioValue("vm_bgm_source") === "Local library"; vmNode("vm-bgm-library").classList.toggle("hidden", !local); vmNode("vm-bgm-upload").classList.toggle("hidden", local); }
function updateVideoMatrixSourceMode() {
  const uploadMode = vmRadioValue("vm_source_mode") === "Upload files";
  vmNode("vm-source-mode-group")?.classList.remove("hidden");
  vmNode("vm-upload-sources-wrap")?.classList.toggle("hidden", !uploadMode);
}
function renderVmRadio(containerId, name, options, selected, onchange) { vmNode(containerId).innerHTML = options.map(([value, label]) => `<label><input type="radio" name="${name}" value="${value}" ${value === selected ? "checked" : ""}>${label}</label>`).join(""); document.querySelectorAll(`input[name="${name}"]`).forEach((radio) => { radio.onchange = onchange || (() => {}); }); }
function vmRadioValue(name) { return document.querySelector(`input[name="${name}"]:checked`)?.value || ""; }
function vmSyncRange(id) { const input = vmNode(id); const output = vmNode(`${id}-value`); if (!input || !output) return; output.textContent = input.value; input.oninput = () => { output.textContent = input.value; const count = document.querySelector("#vm-metric-count"); const workers = document.querySelector("#vm-metric-workers"); if (id === "vm-output-count" && count) count.textContent = input.value; if (id === "vm-max-workers" && workers) workers.textContent = input.value; }; }
function vmSetMulti(select, values) { [...select.options].forEach((option) => { option.selected = values.includes(option.value); }); }
function vmOpenFolder(path) { return vmApi("/open-folder", { method: "POST", body: JSON.stringify({ path }) }); }
function vmLog(text) { vmNode("vm-job-log").textContent = text; }
function vmEscape(value) { return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]); }
function vmDebounce(fn, delay) { let timer; return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), delay); }; }

*/
function mountVideoMatrixWorkbench() {
  const section = document.querySelector("#video-matrix");
  if (!section || section.dataset.mounted === "true") return;
  section.dataset.mounted = "true";
  section.innerHTML = `<iframe class="video-matrix-frame" src="/static/video_matrix.html?embed=1" title="GasGx 视频生成工作台"></iframe>`;
  section.querySelector(".video-matrix-frame")?.addEventListener("load", () => {
    const theme = SHELL_THEMES.find((item) => item.id === localStorage.getItem(SHELL_THEME_KEY)) || SHELL_THEMES[0];
    broadcastShellTheme(theme);
  });
}

function unmountVideoMatrixWorkbench() {
  const section = document.querySelector("#video-matrix");
  if (!section || section.dataset.mounted !== "true") return;
  section.dataset.mounted = "false";
  section.innerHTML = "";
}

document.querySelector('[data-view="video-matrix"]').addEventListener("click", mountVideoMatrixWorkbench);

document.addEventListener("click", async (event) => {
  const enter = event.target.closest("[data-terminal-enter]");
  const configJump = event.target.closest("[data-terminal-config-jump]");
  const longDetect = event.target.closest("[data-terminal-long-detect]");
  const longOpen = event.target.closest("[data-terminal-long-open]");
  const terminalSave = event.target.closest("#terminal-save-config, [data-terminal-save-config]");
  const terminalCloseInit = event.target.closest("[data-terminal-close-init]");
  const terminalCloseConfig = event.target.closest("[data-terminal-close-config]");
  const embeddedStart = event.target.closest("[data-terminal-start-action]");
  const embeddedEdit = event.target.closest("[data-terminal-edit-action]");
  if (!enter && !configJump && !longDetect && !longOpen && !terminalSave && !terminalCloseInit && !terminalCloseConfig && !embeddedStart && !embeddedEdit) return;
  if (terminalSave || terminalCloseInit || terminalCloseConfig || enter || configJump || longDetect || longOpen || embeddedStart || embeddedEdit) {
    event.stopImmediatePropagation();
  }
  if (terminalCloseInit) {
    state.terminalConfigOpen = false;
    document.querySelector("#terminal-init-modal")?.classList.add("hidden");
    return;
  }
  if (terminalCloseConfig) {
    state.terminalConfigOpen = false;
    renderTerminalExecution();
    return;
  }
  if (terminalSave) {
    const route = terminalCurrentRoute();
    const effectiveRoute = route === "hub" ? "wechat" : route;
    const restoreButton = setButtonLoading(terminalSave, "更新中");
    try {
      if (effectiveRoute === "wechat") {
        if (route === "hub") {
          state.terminalRoute = "wechat";
          if (window.location.hash !== "#terminal/wechat") {
            window.history.replaceState(null, "", "#terminal/wechat");
          }
        }
        state.terminalExecution = await api("/api/terminal-execution/start", {
          method: "POST",
          body: JSON.stringify({ windows: readTerminalConfigRows() }),
        });
        state.terminalConfigOpen = false;
        renderTerminalExecution();
      }
    } finally {
      restoreButton();
    }
    return;
  }
  if (embeddedStart) {
    const route = terminalCurrentRoute();
    if (route === "hub") {
      await refresh();
      return;
    }
    if (route === "wechat") {
      if (!state.terminalExecution.initialized || state.terminalConfigOpen) {
        state.terminalExecution = await api("/api/terminal-execution/start", {
          method: "POST",
          body: JSON.stringify({ windows: readTerminalConfigRows() }),
        });
      }
      state.terminalExecution = await api("/api/terminal-execution/start-login", { method: "POST" });
      state.terminalQrVisible = true;
      state.terminalConfigOpen = false;
      renderTerminalExecution();
      return;
    }
    const account = terminalLongSessionAccounts(route)[0];
    if (account?.id) {
      await api(`/api/accounts/${account.id}/platforms/${route}/login-status`, { method: "POST" });
      state.accounts = await api("/api/accounts");
      renderTerminalExecution();
    }
    return;
  }
  if (embeddedEdit) {
    const route = terminalCurrentRoute();
    if (route === "hub") {
      terminalSetRoute("wechat");
      return;
    }
    if (route === "wechat") {
      openTerminalConfigPanel();
      return;
    }
    const account = terminalLongSessionAccounts(route)[0];
    if (account?.open_url) window.open(account.open_url, "_blank", "noopener,noreferrer");
    return;
  }
  if (enter) {
    const nextRoute = enter.dataset.terminalEnter || "hub";
    const fromHubRoute = window.location.hash === "#terminal-execution";
    terminalSetRoute(nextRoute);
    const shouldShowGuide = fromHubRoute
      && nextRoute !== "hub"
      && Boolean(enter.closest(".terminal-entry-card .terminal-entry-actions"));
    if (shouldShowGuide) showTerminalFlowGuideModal(nextRoute);
    return;
  }
  if (configJump) {
    const platform = configJump.dataset.terminalConfigJump || "";
    if (platform === "wechat") {
      terminalSetRoute("wechat");
      showTerminalFlowGuideModal("wechat");
      openTerminalConfigPanel();
    } else {
      terminalSetRoute(platform);
      if (platform) showTerminalFlowGuideModal(platform);
    }
    return;
  }
  if (longDetect) {
    const platform = longDetect.dataset.terminalLongDetect || "";
    const account = terminalLongSessionAccounts(platform)[0];
    if (account?.id) {
      await api(`/api/accounts/${account.id}/platforms/${platform}/login-status`, { method: "POST" });
      state.accounts = await api("/api/accounts");
      renderTerminalExecution();
    }
    return;
  }
  if (longOpen) {
    const platform = longOpen.dataset.terminalLongOpen || "";
    const account = terminalLongSessionAccounts(platform)[0];
    if (account?.open_url) window.open(account.open_url, "_blank", "noopener,noreferrer");
    return;
  }
  if (terminalStart) {
    const route = terminalCurrentRoute();
    if (route === "hub") {
      await refresh();
      return;
    }
    if (route === "wechat") {
      if (!state.terminalExecution.initialized || state.terminalConfigOpen) {
        state.terminalExecution = await api("/api/terminal-execution/start", {
          method: "POST",
          body: JSON.stringify({ windows: readTerminalConfigRows() }),
        });
      }
      state.terminalExecution = await api("/api/terminal-execution/start-login", { method: "POST" });
      state.terminalQrVisible = true;
      state.terminalConfigOpen = false;
      renderTerminalExecution();
      return;
    }
    const account = terminalLongSessionAccounts(route)[0];
    if (account?.id) {
      await api(`/api/accounts/${account.id}/platforms/${route}/login-status`, { method: "POST" });
      state.accounts = await api("/api/accounts");
      renderTerminalExecution();
    }
    return;
  }
  if (terminalEdit) {
    const route = terminalCurrentRoute();
    if (route === "hub") {
      terminalSetRoute("wechat");
      return;
    }
    if (route === "wechat") {
      state.terminalConfigOpen = true;
      renderTerminalExecution();
      return;
    }
    const account = terminalLongSessionAccounts(route)[0];
    if (account?.open_url) window.open(account.open_url, "_blank", "noopener,noreferrer");
  }
}, true);

window.addEventListener("load", () => {
  const requested = terminalRouteFromHash();
  if (requested.view) {
    if (requested.view === "terminal-execution") state.terminalRoute = requested.route;
    activateView(requested.view, false);
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 50);
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 300);
  }
});
