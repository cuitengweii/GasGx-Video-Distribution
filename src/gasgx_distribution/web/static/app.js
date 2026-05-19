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

const TERMINAL_LOGIN_CONFIRM_TEXT = "扫码后点登录";
const TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT = "已发布后点下一个";
const TERMINAL_LEGACY_MANUAL_CONFIRM_TEXT = "发布已执行，等待人工确认";

const state = {
  accounts: [],
  platforms: [],
  tasks: [],
  stats: [],
  statsCaptureStatus: {},
  weeklySummary: {},
  statsMode: "capture",
  summary: {},
  distributionSettings: { common: {}, platforms: {} },
  matrixJobStatus: {},
  aiRobotConfigs: [],
  aiRobotMessages: [],
  interactionManagement: { config: {}, status: {}, history: [] },
  notificationRoutes: [],
  notificationEvents: [],
  notificationPolicies: [],
  notificationIncidents: [],
  notificationSla: {},
  loginQrBatches: [],
  terminalExecution: { colors: [], operators: [], windows: [], summary: {}, platform_capabilities: {}, profile_by_platform: {}, active_platform: "wechat", loading: false },
  terminalRoute: "hub",
  terminalQrVisible: false,
  terminalConfigOpen: false,
  aiRobotEditingPlatform: "",
  aiRobotMessagesCollapsed: true,
  brand: { settings: {} },
  databaseDictionary: null,
  syncStatus: null,
  databaseDictionaryExpanded: {},
  databaseDictionaryLocalized: false,
  analytics: {},
  operatorWechats: ["aamecc", "aalbcc"],
};

const TERMINAL_ERROR_GUIDE_ORDER = ["login_browser", "login_probe", "publish_start", "publish_run", "confirm", "unknown"];
const TERMINAL_ERROR_STAGE_GUIDES = {
  login_browser: {
    title: "打开登录浏览器",
    items: [
      ["浏览器启动失败", "Chrome profile 被占用、调试端口冲突，或 Chrome 未能在超时内启动。"],
      ["账号配置缺失", "账号没有视频号平台配置、profile_dir 或 debug_port。"],
      ["扫码未完成", "浏览器已打开，但后端登录检测仍未返回 ready。"],
    ],
  },
  login_probe: {
    title: "登录检测",
    items: [
      ["登录态异常", "账号会话失效或未完成扫码，登录检测返回未就绪。"],
      ["网络或接口异常", "登录检测请求超时/失败，导致状态无法确认。"],
    ],
  },
  publish_start: {
    title: "发布启动",
    items: [
      ["无可用素材", "当天素材为空，或已被 consumed 去重。"],
      ["发布配置缺失", "未读取到视频号发布配置或调试端口配置。"],
      ["启动发布子进程失败", "发布进程在创建阶段就报错退出。"],
    ],
  },
  publish_run: {
    title: "发布执行",
    items: [
      ["进程无效", "发布子进程 PID 无效或已提前退出。"],
      ["发布未确认", "平台未确认发布成功，需先人工核实后台结果。"],
      ["未检测到发布证据", "进程结束后未找到上传记录 evidence。"],
    ],
  },
  confirm: {
    title: "人工确认",
    items: [
      ["下一账号浏览器打开失败", "发布成功后切下一账号时，登录浏览器未能打开。"],
    ],
  },
  unknown: {
    title: "未知异常",
    items: [
      ["未分类错误", "错误未命中已知节点，请先查看日志并反馈原始报错。"],
    ],
  },
};

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
let currentInteractionTab = localStorage.getItem("gasgx-interaction-tab") || "comment";
let terminalCountdownTimer = null;
let terminalWechatStatePollTimer = null;
let terminalWechatStatePollInFlight = false;
let terminalWechatSelectedAccountId = "";
const terminalAutoPublishWindowIds = new Set();
const terminalAutoPublishStageByWindowId = new Map();
const terminalResetWindowIds = new Set();
const terminalWindowActionEpochById = new Map();
const terminalManualActionStateByWindow = new Map();
const terminalManualCycleAccountByWindow = new Map();
const terminalButtonCooldownByKey = new Map();
let terminalButtonCooldownTimer = null;
let terminalErrorModalSignature = "";
let terminalFullLoadingCount = 0;
let tongjiFollowerChart = null;
let tongjiVideoChart = null;
let tongjiResizeBound = false;

const TERMINAL_BROWSER_WARMUP_TIMEOUT_MS = 12000;
const SHELL_THEME_KEY = "gasgx-shell-theme";
const SHELL_BRAND_KEY = "gasgx-shell-brand";
const SHELL_AUTH_KEY = "gasgx-shell-auth";
const DATABASE_DICTIONARY_LOCALE_KEY = "gasgx-db-dictionary-locale";
const SETTINGS_CARD_KEY = "gasgx-settings-card";
const PERMISSION_DENIED_MESSAGE = "您权限不足";
const PERMISSION_INTERACTIVE_SELECTOR = "button, input, select, textarea, a, [role=\"button\"], [tabindex]";

const TONGJI_SNAPSHOT = {
  overview: [
    { en: "TOTAL FOLLOWERS", cn: "总关注者资产", value: "1,573", unit: "", accent: "accent-green" },
    { en: "7-DAY PLAYS", cn: "7日累计播放量", value: "4,646", unit: "次", accent: "accent-info" },
    { en: "7-DAY NEW FOLLOWS", cn: "7日新增关注转化", value: "+20", unit: "人", accent: "accent-warning" },
    { en: "TOTAL INTERACTIONS", cn: "7日综合互动(赞/评/转)", value: "28", unit: "次", accent: "accent-violet" },
  ],
  dates: ["05/11", "05/12", "05/13", "05/14", "05/15", "05/16", "05/17"],
  totalFollowers: [1555, 1556, 1557, 1557, 1559, 1563, 1573],
  newFollowers: [0, 1, 2, 0, 2, 4, 11],
  unfollowers: [0, 0, -1, 0, 0, 0, -1],
  videoPlays: [1, 2539, 286, 84, 223, 454, 1059],
  videoInteractions: [0, 13, 2, 0, 6, 8, 16],
  matrixRows: [
    ["2026/05/17", "1573", "+11", "-1", "1059", "2", "1", "3", "1", "10"],
    ["2026/05/16", "1563", "+4", "0", "454", "3", "0", "1", "0", "4"],
    ["2026/05/15", "1559", "+2", "0", "223", "4", "0", "0", "0", "2"],
    ["2026/05/14", "1557", "0", "0", "84", "0", "0", "0", "0", "0"],
    ["2026/05/13", "1557", "+2", "-1", "286", "0", "0", "0", "1", "1"],
    ["2026/05/12", "1556", "+1", "0", "2539", "5", "2", "6", "0", "1"],
    ["2026/05/11", "1555", "0", "0", "1", "0", "0", "0", "0", "0"],
  ],
};

const FEATURE_ENTRIES = [
  { id: "overview", label: "总览", group: "业务工作台" },
  { id: "tasks", label: "执行队列", group: "业务工作台" },
  { id: "ai-robot", label: "运营客服", group: "业务工作台" },
  { id: "accounts", label: "账号管理", group: "矩阵管理" },
  { id: "video-matrix", label: "生成视频", group: "矩阵管理" },
  { id: "settings", label: "发布配置", group: "矩阵管理" },
  { id: "terminal-execution", label: "批量发布", group: "矩阵管理" },
  { id: "interaction-management", label: "粉丝互动", group: "矩阵管理" },
  { id: "stats", label: "数据统计", group: "矩阵管理" },
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
      permissions: ["overview", "accounts", "settings", "tasks", "terminal-execution", "interaction-management", "video-matrix", "user-center", "notifications", "help-center"],
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
  { id: "arctic-mint", name: "Arctic Mint", accent: "#66ffd1", soft: "rgba(102, 255, 209, 0.16)" },
];

const VIEW_HEADERS = {
  overview: ["业务总览", "独立账号、独立浏览器、发布/评论/私信/统计任务入口"],
  accounts: ["账号管理", "维护 GasGx 国内外平台账号、独立浏览器配置和登录状态。"],
  "user-center": ["用户中心", "预留操作者资料、角色权限、工作偏好和本地部署身份入口。"],
  settings: ["发布配置", "配置发布素材目录、上传策略、平台参数和矩阵发布作业。"],
  tasks: ["执行队列", "查看发布、评论、私信、登录检测等任务队列和执行状态。"],
  "terminal-execution": ["批量发布", "预留本地终端命令执行入口。"],
  stats: ["数据统计", "短视频账号矩阵数字化营销客户端数据看板。"],
  "ai-robot": ["运营客服", "AI客服、企业微信、钉钉、飞书、Telegram 与 WhatsApp 统一接入。"],
  "interaction-management": ["粉丝互动", "视频号评论与私信自动回复、弹幕占位入口。"],
  "video-matrix": ["生成视频", "分类素材、第一屏封面、视频文字、背景音乐和批量导出工作台。"],
  notifications: ["通知中心", "集中展示生成完成、发布失败、登录失效和素材不足提醒。"],
  "system-settings": ["系统设置", "预留本地部署、存储缓存、安全策略和系统维护入口。"],
  "help-center": ["帮助文档", "预留操作手册、部署说明、视频生成流程和常见问题。"],
};

function displayDatabaseKeyword(value) {
  return String(value ?? "").replace(/supabase/gi, "☁️云端数据库");
}

state.databaseDictionaryLocalized = localStorage.getItem(DATABASE_DICTIONARY_LOCALE_KEY) === "zh";
let currentSettingsCard = localStorage.getItem(SETTINGS_CARD_KEY) === "platform-publish" ? "platform-publish" : "publish-window";
let currentTerminalInitCard = "window";

const DATABASE_DICTIONARY_TABLE_LABELS = {
  matrix_accounts: "矩阵账号",
  account_platforms: "账号平台",
  browser_profiles: "浏览器配置",
  notification_routes: "通知路由",
  notification_policies: "通知策略",
  notification_incidents: "通知事件",
  notification_actions: "通知处理动作",
  login_qr_batches: "登录二维码批次",
  login_qr_items: "登录二维码明细",
  automation_tasks: "自动化任务",
  video_stats_snapshots: "视频统计快照",
  ai_robot_configs: "AI 机器人配置",
  ai_robot_messages: "AI 机器人消息",
  brand_settings: "品牌设置",
  sync_outbox: "同步队列",
  sync_conflicts: "同步冲突日志",
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
  table_name: "表名",
  entity_type: "实体类型",
  entity_id: "实体编号",
  operation: "操作",
  last_attempt_at: "最后尝试时间",
  synced_at: "同步时间",
  local_payload_json: "本地数据",
  remote_payload_json: "云端数据",
  resolution: "处理策略",
  warning: "警告",
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
  const uploadTrigger = document.querySelector("#brand-logo-upload-trigger");
  const uploadName = document.querySelector("#brand-logo-upload-name");
  const syncUploadName = () => {
    if (!uploadName) return;
    const file = upload.files?.[0];
    uploadName.textContent = file?.name || "未选择文件";
  };
  const syncPreview = () => applyShellBrand({ ...readStoredBrand(), name: nameInput.value, slogan: sloganInput.value });
  nameInput.addEventListener("input", syncPreview);
  sloganInput.addEventListener("input", syncPreview);
  uploadTrigger?.addEventListener("click", () => upload.click());
  upload.addEventListener("change", () => {
    const file = upload.files?.[0];
    syncUploadName();
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => applyShellBrand({ name: nameInput.value, slogan: sloganInput.value, logoDataUrl: String(reader.result || "") });
    reader.readAsDataURL(file);
  });
  syncUploadName();
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
    syncUploadName();
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

function terminalFriendlyMessage(message, stage = "") {
  const raw = String(message || "").trim();
  if (!raw) return "操作失败，请稍后重试。";
  const lowered = raw.toLowerCase();
  const stageToken = String(stage || "").trim().toLowerCase();
  if (stageToken === "login_browser" || stageToken === "qr") {
    return "请先点击“打开浏览器扫码”完成登录，再重试当前操作。";
  }
  if (stageToken === "login_probe" || lowered.includes("login_probe")) {
    return "登录检测未通过，请先在浏览器完成登录后再重试。";
  }
  if (lowered.includes("account not found") || raw.includes("未找到账号") || raw.includes("账号不存在")) {
    return "账号不存在。请先到“账号矩阵”检查账号是否被删除或禁用，然后回到当前窗口重新打开浏览器。";
  }
  if (
    lowered.includes("httpsconnectionpool(") ||
    lowered.includes("max retries exceeded") ||
    lowered.includes("traceback") ||
    lowered.includes("connection aborted") ||
    lowered.includes("name or service not known")
  ) {
    return "网络连接异常。请稍后重试；若连续失败，请检查网络后再试。";
  }
  if (/[A-Za-z]{3,}/.test(raw)) {
    return "执行失败，请稍后重试。";
  }
  return raw;
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

function showAccountCreateErrorToast(message) {
  let toast = document.querySelector("#account-create-error-toast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "account-create-error-toast";
    toast.className = "permission-denied-toast";
    toast.setAttribute("role", "alert");
    toast.setAttribute("aria-live", "assertive");
    document.body.appendChild(toast);
  }
  toast.textContent = message || "账号创建失败";
  toast.classList.add("show");
  clearTimeout(showAccountCreateErrorToast.timer);
  showAccountCreateErrorToast.timer = setTimeout(() => {
    toast.classList.remove("show");
  }, 3000);
}

function showAccountRepairToast(result) {
  let toast = document.querySelector("#account-repair-toast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "account-repair-toast";
    toast.className = "permission-denied-toast account-created-toast";
    toast.setAttribute("role", "status");
    toast.setAttribute("aria-live", "polite");
    document.body.appendChild(toast);
  }
  const repaired = Number(result?.repaired_accounts || 0);
  const platforms = Number(result?.created_platforms || 0);
  const profiles = Number(result?.created_profiles || 0) + Number(result?.updated_profiles || 0);
  const detail = repaired
    ? `已修复 ${repaired} 个账号，补齐 ${platforms} 个平台配置、${profiles} 个浏览器配置`
    : "所有账号配置已完整";
  toast.innerHTML = `<strong>账号配置修复完成</strong><span>${escapeHtml(detail)}</span>`;
  toast.classList.add("show");
  clearTimeout(showAccountRepairToast.timer);
  showAccountRepairToast.timer = setTimeout(() => {
    toast.classList.remove("show");
  }, 3200);
}

function terminalErrorStageTitle(stage) {
  const map = {
    load: "终端执行加载失败",
    account_missing: "账号配置异常",
    login_browser: "打开登录浏览器失败",
    qr: "打开登录浏览器失败",
    login_probe: "登录检测失败",
    publish_start: "发布启动失败",
    publish_run: "发布执行失败",
    confirm: "人工确认失败",
    unknown: "未分类异常",
  };
  return map[stage] || "终端流程错误";
}

function terminalErrorStageFromMessage(message) {
  const text = String(message || "").trim();
  const lowered = text.toLowerCase();
  if (!text) return "";
  if (text.startsWith("发布启动失败")) return "publish_start";
  if (
    text.startsWith("发布失败") ||
    lowered.includes("publish_unconfirmed") ||
    lowered.includes("e_publish_unconfirmed") ||
    lowered.includes("未检测到发布证据") ||
    lowered.includes("invalid publish pid")
  ) return "publish_run";
  if (
    lowered.includes("no available material for today") ||
    lowered.includes("wechat platform config missing")
  ) return "publish_start";
  if (lowered.includes("account not found") || /账号不存在|未找到账号/.test(text)) return "account_missing";
  if (lowered.includes("login_probe")) return "login_probe";
  if (/二维码|浏览器/.test(text)) return "login_browser";
  if (/登录|会话|network|connection|timeout|超时|接口/.test(text)) return "login_probe";
  return "";
}

function terminalErrorGuideSections(stage, showAll = false) {
  if (showAll || !stage) {
    return TERMINAL_ERROR_GUIDE_ORDER
      .map((key) => TERMINAL_ERROR_STAGE_GUIDES[key])
      .filter(Boolean);
  }
  const primary = TERMINAL_ERROR_STAGE_GUIDES[stage];
  if (!primary) return [TERMINAL_ERROR_STAGE_GUIDES.unknown];
  return [primary];
}

function terminalErrorFlowMarkup(stage, { showAll = false } = {}) {
  return terminalErrorGuideSections(stage, showAll).map((section) => `
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
    const message = String(current.error_detail || current.status_text || run.error || "终端流程发生错误");
    if (terminalRunIsManualConfirmableFailure(run) || terminalTextIsManualConfirmableFailure(message)) continue;
    const stage = String(
      current.error_stage
      || run.error_stage
      || (runStatus === "failed" ? "publish_run" : "")
      || terminalErrorStageFromMessage(message)
      || (status === "error" ? "unknown" : "")
      || "unknown"
    ).trim();
    const title = String(current.error_title || run.error_title || terminalErrorStageTitle(stage));
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
  if (titleNode) titleNode.textContent = payload.title || "操作未完成";
  if (stageNode) {
    const stageText = payload.stage ? terminalErrorStageTitle(payload.stage) : "";
    stageNode.textContent = stageText;
    stageNode.classList.toggle("hidden", !stageText);
  }
  if (contextNode) contextNode.textContent = payload.context || "";
  if (messageNode) messageNode.textContent = terminalFriendlyMessage(payload.message || "操作失败", payload.stage);
  if (flowNode) flowNode.innerHTML = terminalErrorFlowMarkup(payload.stage, { showAll: Boolean(payload.showAllGuides) });
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
  const isMobileNavViewport = () => window.matchMedia("(max-width: 920px)").matches;
  const setMobileNavOpen = (open) => {
    const visible = Boolean(open && isMobileNavViewport());
    document.body.classList.toggle("mobile-nav-open", visible);
    const mobileToggle = document.querySelector("#mobile-nav-toggle");
    mobileToggle?.setAttribute("aria-expanded", visible ? "true" : "false");
  };
  const closeMobileNavigation = () => setMobileNavOpen(false);
  const toggle = document.querySelector("#user-menu-toggle");
  const menu = document.querySelector("#sidebar-user-actions");
  const mobileToggle = document.querySelector("#mobile-nav-toggle");
  mobileToggle?.addEventListener("click", (event) => {
    event.stopPropagation();
    setMobileNavOpen(!document.body.classList.contains("mobile-nav-open"));
  });
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
    const sidebar = document.querySelector(".sidebar");
    if (isMobileNavViewport() && document.body.classList.contains("mobile-nav-open") && sidebar && !sidebar.contains(event.target)) {
      closeMobileNavigation();
    }
  });
  document.querySelectorAll("[data-quick-view]").forEach((button) => {
    button.addEventListener("click", () => {
      activateView(button.dataset.quickView);
      menu?.classList.add("hidden");
      toggle?.setAttribute("aria-expanded", "false");
      topMenu?.classList.remove("open");
      topToggle?.setAttribute("aria-expanded", "false");
      closeMobileNavigation();
    });
  });
  const sidebarToggle = document.querySelector("#sidebar-toggle");
  sidebarToggle?.addEventListener("click", () => {
    const collapsed = document.body.classList.toggle("sidebar-collapsed");
    sidebarToggle.textContent = collapsed ? "›" : "‹";
    sidebarToggle.setAttribute("aria-expanded", String(!collapsed));
    sidebarToggle.setAttribute("aria-label", collapsed ? "显示左侧栏" : "隐藏左侧栏");
  });
  window.addEventListener("resize", () => {
    if (!isMobileNavViewport()) closeMobileNavigation();
  });
  closeMobileNavigation();
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
  const text = String(message || "").trim();
  const duplicateTaskMatch = text.match(/^duplicate active task already queued: #(\d+)$/i);
  if (duplicateTaskMatch) return `已有相同任务在队列中：#${duplicateTaskMatch[1]}`;
  if (text === "queued for manual worker execution") return "已加入队列，等待人工执行";
  if (text === "pending") return "待处理";
  if (text === "paused") return "已暂停";
  if (text === "unsupported") return "暂不支持";
  if (/account not found/i.test(text)) return "账号不存在，请到账号矩阵检查后重试";
  if (/unknown error/i.test(text)) return "未知异常，请稍后重试";
  if (/window not found/i.test(text)) return "窗口不存在，请刷新后重试";
  if (/[A-Za-z]{3,}/.test(text)) return "执行异常，请稍后重试";
  return text || "操作失败，请稍后重试";
}

async function api(path, options = {}) {
  const { timeoutMs = 0, ...fetchOptions } = options || {};
  const timeout = Number(timeoutMs || 0);
  const controller = timeout > 0 ? new AbortController() : null;
  const timer = controller
    ? window.setTimeout(() => controller.abort(), timeout)
    : null;
  let response;
  try {
    response = await fetch(path, {
      ...fetchOptions,
      headers: { "Content-Type": "application/json", ...(fetchOptions.headers || {}) },
      signal: controller ? controller.signal : fetchOptions.signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("请求超时，请稍后刷新状态");
    }
    throw error;
  } finally {
    if (timer) window.clearTimeout(timer);
  }
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

function workspaceLoadingTitle(view) {
  const entry = FEATURE_ENTRIES.find((item) => item.id === view);
  return entry ? `${entry.label}加载中，请稍候...` : "页面加载中，请稍候...";
}

function setWorkspaceLoading(active, message = "页面加载中，请稍候...", detail = "正在同步右侧面板数据。") {
  const mask = document.querySelector("#workspace-loading");
  if (!mask) return;
  if (active) {
    const textNode = mask.querySelector("[data-workspace-loading-text]");
    const detailNode = mask.querySelector("[data-workspace-loading-detail]");
    if (textNode) textNode.textContent = message;
    if (detailNode) detailNode.textContent = detail;
    mask.classList.remove("hidden");
    mask.setAttribute("aria-hidden", "false");
    return;
  }
  mask.classList.add("hidden");
  mask.setAttribute("aria-hidden", "true");
}

function terminalPublishLoadingInline(label = "发布中") {
  return `<span class="terminal-publish-loading-inline"><span class="btn-spinner" aria-hidden="true"></span><span>${label}</span><span class="terminal-publish-dots" aria-hidden="true">...</span></span>`;
}

function terminalActiveWindowCount(terminalState = state.terminalExecution) {
  return (terminalState?.windows || []).filter((window) => {
    const accounts = Array.isArray(window?.accounts) ? window.accounts : [];
    return Number(window?.current_index || 0) < accounts.length;
  }).length;
}

function terminalBrowserLoadingTitle(terminalState = state.terminalExecution) {
  const count = terminalActiveWindowCount(terminalState);
  return count > 0 ? `正在打开 ${count} 个登录浏览器，请稍候...` : "正在打开登录浏览器，请稍候...";
}

function terminalBrowserLoadingDetail() {
  return "Chrome 窗口会依次弹出，全部拉起后再显示扫码队列，请不要重复点击。";
}

function terminalNeedsBrowserWarmup(terminalState = state.terminalExecution) {
  if (terminalCurrentRoute() !== "wechat") return false;
  if (!terminalState?.login_started) return false;
  return (terminalState.windows || []).some((window) => {
    const accounts = Array.isArray(window?.accounts) ? window.accounts : [];
    const current = accounts[Number(window?.current_index || 0)] || {};
    const status = String(current?.status || "").toLowerCase();
    return status === "pending" || status === "opening";
  });
}

async function warmTerminalBrowsersBeforeRender(message = "") {
  return false;
}

async function startTerminalWechatLoginWithLoading(button = null) {
  const restoreButton = setButtonLoading(button, "打开中");
  setTerminalFullLoading(true, terminalBrowserLoadingTitle(), terminalBrowserLoadingDetail());
  try {
    if (!state.terminalExecution.initialized || state.terminalConfigOpen) {
      state.terminalExecution = await api("/api/terminal-execution/start", {
        method: "POST",
        body: JSON.stringify({ windows: readTerminalConfigRows() }),
      });
      updateTerminalFullLoading(terminalBrowserLoadingTitle(), terminalBrowserLoadingDetail());
    }
    state.terminalExecution = await api("/api/terminal-execution/start-login", {
      method: "POST",
      timeoutMs: TERMINAL_BROWSER_WARMUP_TIMEOUT_MS,
    });
    state.terminalQrVisible = true;
    state.terminalConfigOpen = false;
    renderTerminalExecution();
    return state.terminalExecution;
  } finally {
    restoreButton();
    setTerminalFullLoading(false);
  }
}

function updateTerminalFullLoading(message, detail = "") {
  const mask = document.querySelector("#terminal-full-loading");
  if (!mask) return;
  const textNode = mask.querySelector("[data-terminal-loading-text]");
  const detailNode = mask.querySelector("[data-terminal-loading-detail]");
  if (textNode && message) textNode.textContent = message;
  if (detailNode && detail) detailNode.textContent = detail;
}

function setTerminalFullLoading(active, message = "终端执行页面加载中，请稍候...", detail = "正在同步窗口状态、账号进度和二维码缓存。") {
  const mask = document.querySelector("#terminal-full-loading");
  if (!mask) return;
  if (active) {
    terminalFullLoadingCount += 1;
    updateTerminalFullLoading(message, detail);
    mask.classList.remove("hidden");
    mask.setAttribute("aria-hidden", "false");
    return;
  }
  terminalFullLoadingCount = Math.max(0, terminalFullLoadingCount - 1);
  if (terminalFullLoadingCount > 0) return;
  mask.classList.add("hidden");
  mask.setAttribute("aria-hidden", "true");
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

function helpDocNameFromPath(path) {
  return String(path || "").split("/").pop() || "";
}

function helpCenterDocPathFromLocation(pathname = window.location.pathname) {
  const match = String(pathname || "").match(/^\/help-center\/([^/]+\.md)$/i);
  if (!match) return "";
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return "";
  }
}

function helpCenterDocUrl(docName) {
  return `/help-center/${encodeURIComponent(String(docName || "").trim())}`;
}

function setHelpCenterStandaloneMode(active) {
  document.body.classList.toggle("help-doc-standalone", Boolean(active));
}

async function openHelpDocument(path, { syncUrl = true } = {}) {
  const docName = String(path || "").split("/").pop();
  if (!docName) return;
  const reader = document.querySelector("#help-doc-reader");
  const body = document.querySelector("#help-reader-body");
  if (!reader || !body) return;
  if (syncUrl) window.history.replaceState(null, "", helpCenterDocUrl(docName));
  reader.classList.remove("hidden");
  setWorkspaceLoading(true, "加载帮助文档...", "正在读取知识库内容。");
  body.innerHTML = loadingInline("加载帮助文档...");
  try {
    const doc = await api(`/api/help-docs/${encodeURIComponent(docName)}`);
    const firstTitle = String(doc.content || "").split(/\r?\n/).find((line) => line.startsWith("# "));
    document.querySelector("#help-reader-title").textContent = firstTitle ? firstTitle.replace(/^#\s*/, "") : doc.name;
    body.innerHTML = renderHelpMarkdown(doc.content);
    if (!window.location.pathname.startsWith("/help-center/")) {
      reader.scrollIntoView({ behavior: "smooth", block: "start" });
    } else {
      window.scrollTo({ top: 0, left: 0 });
    }
  } catch (error) {
    body.innerHTML = `<div class="muted">加载失败：${escapeHtml(formatFriendlyMessage(error?.message || "未知异常"))}</div>`;
  } finally {
    setWorkspaceLoading(false);
  }
}

function initHelpCenter() {
  document.querySelectorAll(".help-doc-card").forEach((card) => {
    const docPath = (card.querySelector("code")?.textContent || "").trim();
    const docName = helpDocNameFromPath(docPath);
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "button");
    card.addEventListener("click", () => {
      if (!docName) return;
      setHelpCenterStandaloneMode(true);
      openHelpDocument(docPath, { syncUrl: true });
    });
    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        if (!docName) return;
        setHelpCenterStandaloneMode(true);
        openHelpDocument(docPath, { syncUrl: true });
      }
    });
  });
  document.querySelector("#help-reader-close")?.addEventListener("click", () => {
    if (window.location.pathname.startsWith("/help-center/")) {
      setHelpCenterStandaloneMode(false);
      window.history.replaceState(null, "", "/#help-center");
      document.querySelector("#help-doc-reader")?.classList.add("hidden");
      window.scrollTo({ top: 0, left: 0 });
      return;
    }
    document.querySelector("#help-doc-reader")?.classList.add("hidden");
  });
}

function setPageLoading(label = "加载中...") {
  setWorkspaceLoading(true, label, "正在同步右侧面板数据。");
}

function setViewLoading(view) {
  if (view === "terminal-execution" && terminalCurrentRoute() === "hub") return;
  setWorkspaceLoading(true, workspaceLoadingTitle(view), "正在同步右侧面板数据。");
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

function terminalWechatAccountLoginStatusLabel(status) {
  const normalized = String(status || "unknown").toLowerCase();
  const labels = {
    active: "已启用",
    ready: "已登录",
    logged_in: "已登录",
    success: "已登录",
    ok: "正常",
    login_required: "需登录",
    logged_out: "未登录",
    unavailable: "不可用",
    failed: "异常",
    error: "异常",
    pending: "待检查",
    checking: "检查中",
    not_checked: "待检查",
    unknown: "未知",
  };
  return labels[normalized] || platformStatusLabel(status);
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
    ? state.accounts.map((account) => `<option value="${account.id}">#${account.id} ${cleanAccountDisplayName(account)}</option>`).join("")
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
  const match = String(account?.notes || "").match(/绑定运营微信[:：]\s*([^；;]+)/);
  return match ? match[1].trim() : "";
}

function accountPhone(account) {
  const match = String(account?.notes || "").match(/账号手机号[:：]\s*(\d{11})/);
  return match ? match[1] : "";
}

function accountSubtitle(account) {
  const key = String(account?.account_key || "").trim();
  const niche = String(account?.niche || "").trim();
  const cleanNiche = /^\?+$/.test(niche) ? "" : niche;
  return [key, cleanNiche].filter(Boolean).join(" · ");
}

function accountStatusLabel(status) {
  const normalized = String(status || "").toLowerCase();
  const labels = {
    active: "启用",
    warmup: "养号",
    paused: "暂停",
  };
  return labels[normalized] || status || "未知";
}

function accountStatusEnabled(account) {
  return String(account?.status || "").toLowerCase() === "active";
}

function cleanAccountDisplayName(account) {
  const raw = String(account?.display_name || "").trim();
  const cleaned = raw
    .split(/\s+/)
    .filter((part) => !/^\?+$/.test(part))
    .join(" ")
    .trim();
  return cleaned || String(account?.account_key || "").trim() || "未命名账号";
}

function accountSearchText(account) {
  return [
    account?.id,
    account?.display_name,
    cleanAccountDisplayName(account),
    account?.account_key,
    account?.niche,
    account?.status,
    accountStatusLabel(account?.status),
    accountOperatorWechat(account),
    accountPhone(account),
    account?.notes,
  ].map((value) => String(value || "").toLowerCase()).join(" ");
}

function filteredAccounts() {
  const keyword = String(document.querySelector("#account-search-input")?.value || "").trim().toLowerCase();
  if (!keyword) return state.accounts || [];
  return (state.accounts || []).filter((account) => accountSearchText(account).includes(keyword));
}

function accountEditIcon() {
  return `<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="m16.5 3.5 4 4L7 21l-4 1 1-4Z"/></svg>`;
}

function accountDeleteIcon() {
  return `<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v5"/><path d="M14 11v5"/></svg>`;
}

function accountOperatorWechatOptions(current = "") {
  const values = [
    ...(state.operatorWechats || []),
    ...(state.accounts || []).map(accountOperatorWechat),
    current,
  ].map((value) => String(value || "").trim()).filter(Boolean);
  return Array.from(new Set(values));
}

function accountNotesWith(account, updates = {}) {
  const notes = String(account?.notes || "");
  const operatorWechat = updates.operatorWechat ?? accountOperatorWechat(account);
  const phone = updates.phone ?? accountPhone(account);
  const extras = notes
    .split(/[；;]/)
    .map((part) => part.trim())
    .filter((part) => part && !/^绑定运营微信[:：]/.test(part) && !/^账号手机号[:：]/.test(part));
  const fields = [];
  if (operatorWechat) fields.push(`绑定运营微信：${operatorWechat}`);
  if (phone) fields.push(`账号手机号：${phone}`);
  return [...fields, ...extras].join("；");
}

function renderAccounts() {
  const accounts = filteredAccounts();
  const keyword = String(document.querySelector("#account-search-input")?.value || "").trim();
  document.querySelector("#accounts-list").innerHTML = accounts.map((account) => {
    const platforms = account.platforms || [];
    const operatorWechat = accountOperatorWechat(account);
    const phone = accountPhone(account);
    const displayName = cleanAccountDisplayName(account);
    const title = `#${account.id} ${displayName}`;
    return `<article class="account-row" data-account-id="${account.id}">
      <div class="row-head">
        <div class="account-title-wrap">
          <div class="account-title-line">
            <strong class="account-title">${escapeHtml(title)}</strong>
            <button class="account-edit-btn" type="button" title="修改账号名称" aria-label="修改账号名称" data-no-global-loading="1" data-account-edit="name">${accountEditIcon()}</button>
          </div>
          <div class="account-subtitle">${escapeHtml(accountSubtitle(account))}</div>
          <div class="account-meta-row">
            <div class="account-operator-wechat"><span>运营微信</span><strong>${escapeHtml(operatorWechat || "-")}</strong><button class="account-edit-btn compact" type="button" title="修改绑定运营微信" aria-label="修改绑定运营微信" data-no-global-loading="1" data-account-edit="operator">${accountEditIcon()}</button></div>
            <div class="account-phone-line"><span>手机号</span><strong>${escapeHtml(phone || "-")}</strong><button class="account-edit-btn compact" type="button" title="修改账号手机号" aria-label="修改账号手机号" data-no-global-loading="1" data-account-edit="phone">${accountEditIcon()}</button></div>
          </div>
        </div>
        <div class="account-badges">
          <button class="account-status-toggle ${accountStatusEnabled(account) ? "enabled" : "paused"}" type="button" data-no-global-loading="1" data-account-status-toggle="${account.id}" aria-pressed="${accountStatusEnabled(account)}" title="${accountStatusEnabled(account) ? "点击暂停账号" : "点击启用账号"}">
            <span class="account-status-toggle-knob" aria-hidden="true"></span>
            <span>${escapeHtml(accountStatusLabel(account.status))}</span>
          </button>
          <span class="chip success-chip" title="基于真实发布成功记录去重统计">已发布成功 ${account.publish_success_count || 0}</span>
          <button class="btn ghost btn-sm danger-action" type="button" data-delete-account="${account.id}" data-account-name="${escapeHtml(displayName)}">${accountDeleteIcon()}<span>删除账号</span></button>
        </div>
      </div>
      ${renderPlatformStatusGroup(platforms, "cn")}
      ${renderPlatformStatusGroup(platforms, "global")}
    </article>`;
  }).join("") || `<div class="muted">${keyword ? "没有匹配的账号" : "暂无账号"}</div>`;
}

function accountById(accountId) {
  return (state.accounts || []).find((account) => Number(account.id) === Number(accountId));
}

function replaceAccountInState(updated) {
  const index = (state.accounts || []).findIndex((account) => Number(account.id) === Number(updated?.id));
  if (index >= 0) state.accounts[index] = updated;
}

async function repairAccountConfigs(button) {
  if (!window.confirm("确认检索并修复所有账号的矩阵配置？缺失的平台和浏览器配置会自动补齐。")) return;
  const restoreButton = setButtonLoading(button, "修复中");
  try {
    const result = await api("/api/accounts/repair-config", { method: "POST" });
    await loadAccounts();
    renderAccounts();
    loadedViews.delete("overview");
    loadedViews.delete("tasks");
    loadedViews.delete("terminal-execution");
    showAccountRepairToast(result);
  } catch (error) {
    showAccountCreateErrorToast(formatFriendlyMessage(error.message));
  } finally {
    restoreButton();
  }
}

function accountInlineActions(field) {
  return `<span class="account-inline-actions">
    <button class="btn secondary btn-sm" type="button" data-no-global-loading="1" data-account-save="${field}">保存</button>
    <button class="btn ghost btn-sm" type="button" data-no-global-loading="1" data-account-cancel>取消</button>
  </span>`;
}

function startAccountNameEdit(row, account) {
  const target = row.querySelector(".account-title-line");
  if (!target) return;
  target.innerHTML = `<div class="account-inline-edit account-name-edit">
    <input class="account-inline-input" type="text" value="${escapeHtml(account.display_name || "")}" aria-label="账号名称">
    ${accountInlineActions("name")}
  </div>`;
  const input = target.querySelector("input");
  input?.focus();
  input?.select();
}

function startAccountOperatorEdit(row, account) {
  const target = row.querySelector(".account-operator-wechat");
  if (!target) return;
  const current = accountOperatorWechat(account);
  const options = accountOperatorWechatOptions(current);
  target.innerHTML = `绑定运营微信：<span class="account-inline-edit">
    <select class="account-inline-select" aria-label="绑定运营微信">
      ${options.map((value) => `<option value="${escapeHtml(value)}" ${value === current ? "selected" : ""}>${escapeHtml(value)}</option>`).join("") || `<option value="">暂无绑定运营微信</option>`}
    </select>
    ${accountInlineActions("operator")}
  </span>`;
  target.querySelector("select")?.focus();
}

function startAccountPhoneEdit(row, account) {
  const target = row.querySelector(".account-phone-line");
  if (!target) return;
  target.innerHTML = `账号手机号：<span class="account-inline-edit">
    <input class="account-inline-input phone" type="text" inputmode="numeric" maxlength="11" value="${escapeHtml(accountPhone(account))}" aria-label="账号手机号">
    ${accountInlineActions("phone")}
  </span>`;
  const input = target.querySelector("input");
  input?.focus();
  input?.select();
}

async function saveAccountInlineEdit(button) {
  const row = button.closest("[data-account-id]");
  const field = button.dataset.accountSave;
  const account = accountById(row?.dataset.accountId);
  if (!row || !account || !field) return;
  const payload = {};
  if (field === "name") {
    const value = row.querySelector(".account-title-line input")?.value.trim();
    if (!value) {
      showAccountCreateErrorToast("账号名称不能为空");
      return;
    }
    payload.display_name = value;
  } else if (field === "operator") {
    const value = row.querySelector(".account-operator-wechat select")?.value.trim();
    if (!value) {
      showAccountCreateErrorToast("请选择绑定运营微信");
      return;
    }
    payload.notes = accountNotesWith(account, { operatorWechat: value });
  } else if (field === "phone") {
    const value = row.querySelector(".account-phone-line input")?.value.trim();
    if (!/^\d{11}$/.test(value || "")) {
      showAccountCreateErrorToast("账号手机号需为 11 位数字");
      return;
    }
    payload.notes = accountNotesWith(account, { phone: value });
  }
  const restoreButton = setButtonLoading(button, "保存中");
  try {
    const updated = await api(`/api/accounts/${account.id}`, { method: "PATCH", body: JSON.stringify(payload) });
    replaceAccountInState(updated);
    renderAccounts();
    updateAccountPhoneHint();
  } catch (error) {
    showAccountCreateErrorToast(formatFriendlyMessage(error.message));
  } finally {
    restoreButton();
  }
}

async function toggleAccountStatus(button) {
  const account = accountById(button?.dataset.accountStatusToggle);
  if (!account) return;
  const nextStatus = accountStatusEnabled(account) ? "paused" : "active";
  const restoreButton = setButtonLoading(button, nextStatus === "active" ? "启用中" : "暂停中");
  try {
    const updated = await api(`/api/accounts/${account.id}`, { method: "PATCH", body: JSON.stringify({ status: nextStatus }) });
    replaceAccountInState(updated);
    renderAccounts();
  } catch (error) {
    showAccountCreateErrorToast(formatFriendlyMessage(error.message));
  } finally {
    restoreButton();
  }
}

function taskTypeLabel(type) {
  return TASK_TYPE_OPTIONS.find(([value]) => value === type)?.[1] || type || "未指定";
}

function taskAccountLabel(task) {
  const accountId = Number(task.account_id || 0);
  const account = state.accounts.find((item) => Number(item.id) === accountId);
  return account
    ? `#${account.id} ${cleanAccountDisplayName(account)}`
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
    return `<option value="${value}">#${account.id} ${cleanAccountDisplayName(account)}</option>`;
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
  return colors[index % Math.max(1, colors.length)] || { hex: "#EF4444", name: "标准红" };
}

function terminalSlotLabel(slotIndex) {
  const normalized = Number(slotIndex) || 0;
  return String(Math.max(0, normalized) + 1).padStart(2, "0");
}

function terminalWindowLabel(windowId) {
  return `终端执行窗口 ${String(Number(windowId) || 0).padStart(2, "0")}`;
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
          <div>该平台不使用视频号矩阵窗位、运营微信绑定和扫码浏览器队列。</div>
          <div>请在页面顶部切换平台后，再通过“当前平台配置”查看对应字段组。</div>
        </div>
      </div>
    `;
    installTerminalConfigInteractions(list);
    return;
  }
  const operators = terminalState?.operators || [];
  const colors = terminalState?.colors || [];
  const savedRows = Array.isArray(terminalState?.config) ? terminalState.config : [];
  const slotRows = Array.from({ length: 5 }, (_, index) => ({
    slot_id: index + 1,
    slot_label: terminalSlotLabel(index),
    enabled: false,
    operator_wechat: operators[index]?.operator_wechat || operators[index % Math.max(1, operators.length)]?.operator_wechat || "",
    color: terminalColorByIndex(index, terminalState).hex,
  }));
  const applySavedRow = (row, saved, index) => ({
    ...row,
    enabled: typeof saved?.enabled === "boolean" ? saved.enabled : row.enabled,
    operator_wechat: String(saved?.operator_wechat || row.operator_wechat || ""),
    color: String(saved?.color || row.color || terminalColorByIndex(index, terminalState).hex),
  });
  const usedSlots = new Set();
  const consumedSavedIndexes = new Set();
  savedRows.forEach((saved, savedIndex) => {
    const slotId = Number(saved?.id || 0);
    if (slotId < 1 || slotId > slotRows.length || usedSlots.has(slotId)) return;
    const slotIndex = slotId - 1;
    slotRows[slotIndex] = applySavedRow(slotRows[slotIndex], saved, slotIndex);
    usedSlots.add(slotId);
    consumedSavedIndexes.add(savedIndex);
  });
  const unslottedSavedRows = savedRows.filter((_saved, savedIndex) => !consumedSavedIndexes.has(savedIndex));
  unslottedSavedRows.forEach((saved) => {
    const slotIndex = slotRows.findIndex((row) => !usedSlots.has(Number(row.slot_id || 0)));
    if (slotIndex < 0) return;
    slotRows[slotIndex] = applySavedRow(slotRows[slotIndex], saved, slotIndex);
    usedSlots.add(Number(slotRows[slotIndex].slot_id || 0));
  });
  if (!slotRows.length) {
    list.innerHTML = `
      <div class="terminal-empty-state">
        <strong>暂无可编辑配置</strong>
        <p class="muted">请先确认已加载终端数据，或切换到视频号路由后重新打开配置。</p>
      </div>
    `;
    installTerminalConfigInteractions(list);
    return;
  }
  list.innerHTML = slotRows.map((row) => `
    <div class="terminal-config-row ${row.enabled ? "" : "disabled"}" data-terminal-config-row="${row.slot_id}">
      <label class="terminal-config-left">
        <input type="checkbox" class="terminal-checkbox" data-terminal-enabled ${row.enabled ? "checked" : ""}>
        <span>终端执行窗口 ${row.slot_label}</span>
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
  if (terminalWindowIsCompleted(window)) {
    return {
      hasQr: false,
      expiresAt: 0,
      remaining: 0,
      expired: false,
      active: false,
      countdownText: "完成",
      placeholderState: "completed",
    };
  }
  if (Boolean(window?.confirming_next)) {
    return {
      hasQr: false,
      expiresAt: 0,
      remaining: 0,
      expired: false,
      active: false,
      countdownText: "进入下一步",
      placeholderState: "confirming_next",
    };
  }
  const refreshing = Boolean(window?.qr_refreshing);
  const accounts = Array.isArray(window?.accounts) ? window.accounts : [];
  const currentIndex = Number(window?.current_index || 0);
  const current = accounts[currentIndex] || {};
  const currentStatus = String(current?.status || "").toLowerCase();
  const browserClosed = current?.browser_open === false || window?.browser_open === false;
  if (refreshing) {
    return {
      hasQr: false,
      expiresAt: 0,
      remaining: 0,
      expired: false,
      active: false,
      countdownText: "打开中...",
      placeholderState: "opening",
    };
  }
  if (currentStatus === "ready" && browserClosed) {
    return {
      hasQr: false,
      expiresAt: 0,
      remaining: 0,
      expired: false,
      active: false,
      countdownText: "浏览器已关闭",
      placeholderState: "browser_closed",
    };
  }
  if (currentStatus === "ready") {
    return {
      hasQr: false,
      expiresAt: 0,
      remaining: 0,
      expired: false,
      active: false,
      countdownText: "已登录",
      placeholderState: "ready",
    };
  }
  if (currentStatus === "waiting_qr" || currentStatus === "opening") {
    return {
      hasQr: false,
      expiresAt: 0,
      remaining: 0,
      expired: false,
      active: false,
      countdownText: currentStatus === "opening" ? "打开中..." : "扫码中",
      placeholderState: "browser",
    };
  }
  return {
    hasQr: false,
    expiresAt: 0,
    remaining: 0,
    expired: false,
    active: false,
    countdownText: "待打开",
    placeholderState: "idle",
  };
}

function terminalRunIsManualConfirmableFailure(run) {
  const runStatus = String(run?.status || "").toLowerCase();
  if (runStatus !== "failed" && runStatus !== "error") return false;
  const errorText = `${String(run?.error || "")} ${String(run?.error_title || "")}`.toLowerCase();
  return terminalTextIsManualConfirmableFailure(errorText);
}

function terminalTextIsManualConfirmableFailure(text) {
  const errorText = String(text || "").toLowerCase();
  return (
    errorText.includes("未检测到视频号发布成功记录")
    || errorText.includes("未检测到发布证据")
    || errorText.includes("publish process finished but no wechat evidence")
    || errorText.includes("publish_unconfirmed")
    || errorText.includes("publish was not confirmed")
    || errorText.includes("发布未确认")
  );
}

function terminalAccountHasLegacyManualConfirmFailure(account) {
  const status = String(account?.status || "").toLowerCase();
  if (status !== "error" && status !== "failed") return false;
  return terminalTextIsManualConfirmableFailure(`${String(account?.status_text || "")} ${String(account?.error_detail || "")} ${String(account?.error_title || "")}`);
}

function terminalAccountIsManualConfirmable(account) {
  return terminalAccountHasLegacyManualConfirmFailure(account);
}

function terminalCurrentIsManualConfirmable(window, account, index, currentIndex) {
  if (!window || !account) return false;
  if (Number(index) !== Number(currentIndex)) return false;
  const run = window?.publish_run || {};
  const runActiveForCurrent = Number(run?.account_id || 0) === Number(account?.id || 0);
  return runActiveForCurrent && terminalRunIsManualConfirmableFailure(run);
}

function terminalDisplayAccount(window, account, index, currentIndex) {
  if (terminalCurrentIsManualConfirmable(window, account, index, currentIndex)) {
    return { ...account, status: "running", status_text: TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT };
  }
  if (terminalAccountHasLegacyManualConfirmFailure(account)) {
    if (Number(index) < Number(currentIndex)) {
      return { ...account, status: "success", status_text: "发布成功" };
    }
    if (Number(index) === Number(currentIndex)) {
      return { ...account, status: "running", status_text: TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT };
    }
  }
  return account;
}

function terminalWindowIsCompleted(window) {
  const accounts = Array.isArray(window?.accounts) ? window.accounts : [];
  const currentIndex = Number(window?.current_index || 0);
  const successCount = accounts.filter((account) => String(account?.status || "").toLowerCase() === "success").length;
  const allSucceeded = accounts.length > 0 && successCount >= accounts.length;
  return Boolean(window?.completed) || (accounts.length > 0 && currentIndex >= accounts.length) || allSucceeded;
}

function terminalWechatAccountChoices() {
  return statsWechatActiveAccounts().map((account) => {
    const platforms = Array.isArray(account.platforms) ? account.platforms : [];
    const wechat = platforms.find((item) => String(item.platform || "") === "wechat") || {};
    const accountId = terminalResolveAccountId(account);
    const accountName = account.display_name || account.account_key || `账号 ${account.id}`;
    const profileDir = String(wechat.profile_dir || "").trim();
    const debugPort = Number(wechat.debug_port || 0);
    return {
      accountId,
      accountName,
      profileDir,
      debugPort,
      label: accountName,
    };
  }).filter((item) => Boolean(item.accountId));
}

function terminalWechatSelectedAccountChoice() {
  const choices = terminalWechatAccountChoices();
  if (!choices.length) {
    terminalWechatSelectedAccountId = "";
    return null;
  }
  const selected = choices.find((item) => String(item.accountId) === String(terminalWechatSelectedAccountId || "").trim())
    || choices[0];
  terminalWechatSelectedAccountId = selected.accountId;
  return selected;
}

function terminalAutoPublishStageFromStatusText(statusText) {
  const text = String(statusText || "").trim();
  if (!text) return "";
  if (text.includes("登录")) return "confirm_login";
  if (text.includes("准备")) return "prepare_publish";
  if (text.includes("发布")) return "publishing";
  return "";
}

function terminalAutoPublishStageLabel(stage) {
  switch (String(stage || "")) {
    case "confirm_login":
      return "登录中";
    case "prepare_publish":
      return "准备中";
    case "publishing":
      return "发布中";
    case "stopping":
      return "停止中";
    default:
      return "";
  }
}

function terminalPublishLoadingLabelFromText(text) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  if (raw.includes("停止中")) return "停止中";
  if (raw.includes("登录中")) return "登录中";
  if (raw.includes("准备中")) return "准备中";
  if (raw.includes("发布中")) return "发布中";
  return "";
}

function terminalPrecheckPrimaryIssue(window) {
  const precheck = window?.publish_precheck || {};
  const issues = precheck?.issues || {};
  const p0 = Array.isArray(issues.p0) ? issues.p0 : [];
  const p1 = Array.isArray(issues.p1) ? issues.p1 : [];
  const p2 = Array.isArray(issues.p2) ? issues.p2 : [];
  if (p0.length) return { level: "p0", item: p0[0] };
  if (p1.length) return { level: "p1", item: p1[0] };
  if (p2.length) return { level: "p2", item: p2[0] };
  return null;
}

function terminalAccountCanStartPublish(account) {
  const status = String(account?.status || "").toLowerCase();
  return status === "ready" || status === "running" || status === "success";
}

function terminalManualActionState(windowId, accountId) {
  const windowKey = String(windowId || "").trim();
  const accountKey = String(accountId || "").trim();
  const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === windowKey);
  const targetAccount = (targetWindow?.accounts || []).find((item) => String(item?.id ?? item?.account_id ?? "") === accountKey);
  if (targetAccount && typeof targetAccount === "object") {
    const hasLoginFlag = Object.prototype.hasOwnProperty.call(targetAccount, "login_clicked");
    const inferredLoginClicked = hasLoginFlag
      ? Boolean(targetAccount.login_clicked)
      : Number(targetWindow?.current_index || 0) > 0;
    return {
      loginClicked: inferredLoginClicked,
      publishClicked: Boolean(targetAccount.publish_clicked),
      nextClicked: Boolean(targetAccount.next_clicked),
    };
  }
  if (!windowKey || !accountKey) return { loginClicked: false, publishClicked: false, nextClicked: false };
  if (!terminalManualActionStateByWindow.has(windowKey)) terminalManualActionStateByWindow.set(windowKey, new Map());
  const accountMap = terminalManualActionStateByWindow.get(windowKey);
  if (!accountMap.has(accountKey)) accountMap.set(accountKey, { loginClicked: false, publishClicked: false, nextClicked: false });
  return accountMap.get(accountKey);
}

function markTerminalManualAction(windowId, accountId, action) {
  const manualState = terminalManualActionState(windowId, accountId);
  if (action === "login") manualState.loginClicked = true;
  if (action === "publish") manualState.publishClicked = true;
  if (action === "next") manualState.nextClicked = true;
  const windowKey = String(windowId || "").trim();
  const accountKey = String(accountId || "").trim();
  const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === windowKey);
  const targetAccount = (targetWindow?.accounts || []).find((item) => String(item?.id ?? item?.account_id ?? "") === accountKey);
  if (targetAccount && typeof targetAccount === "object") {
    if (action === "login") targetAccount.login_clicked = true;
    if (action === "publish") targetAccount.publish_clicked = true;
    if (action === "next") targetAccount.next_clicked = true;
  }
}

function terminalForcePostLoginStage(windowId, accountId) {
  const windowKey = String(windowId || "").trim();
  const accountKey = String(accountId || "").trim();
  if (!windowKey || !accountKey) return;
  const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === windowKey);
  const targetAccount = (targetWindow?.accounts || []).find((item) => String(item?.id ?? item?.account_id ?? "") === accountKey);
  if (!targetAccount || typeof targetAccount !== "object") return;
  targetAccount.login_clicked = true;
  if (targetAccount.publish_clicked == null) targetAccount.publish_clicked = false;
  if (targetAccount.next_clicked == null) targetAccount.next_clicked = false;
  const status = String(targetAccount.status || "").toLowerCase();
  const statusText = String(targetAccount.status_text || "").trim();
  if (
    status === "pending" || status === "opening" || status === "waiting_qr"
    || !statusText
    || /待点登录|待登录|扫码后点登录/.test(statusText)
  ) {
    targetAccount.status = "waiting_qr";
    targetAccount.status_text = "待发布";
  }
}

function parseWindowAccountToken(rawToken) {
  const raw = String(rawToken || "").trim();
  if (!raw) return { windowId: "", accountId: "" };
  const idx = raw.indexOf(":");
  if (idx < 0) return { windowId: raw, accountId: "" };
  return {
    windowId: raw.slice(0, idx).trim(),
    accountId: raw.slice(idx + 1).trim(),
  };
}

function terminalResolveAccountId(account) {
  if (!account || typeof account !== "object") return "";
  const candidate = account.id ?? account.account_id ?? account.accountId ?? "";
  return String(candidate || "").trim();
}

function terminalResolveWindowAccountId(window, index) {
  const accounts = Array.isArray(window?.accounts) ? window.accounts : [];
  if (!accounts.length) return "";
  const targetIndex = Number.isFinite(Number(index)) ? Number(index) : Number(window?.current_index || 0);
  const primary = terminalResolveAccountId(accounts[targetIndex]);
  if (primary) return primary;
  const fallback = terminalResolveAccountId(accounts[0]);
  return fallback || "";
}

function terminalCooldownKey(windowId, accountId, action) {
  return `${String(windowId || "").trim()}:${String(accountId || "").trim()}:${String(action || "").trim()}`;
}

function terminalWindowActionEpoch(windowId) {
  return Number(terminalWindowActionEpochById.get(String(windowId || "").trim()) || 0);
}

function terminalBumpWindowActionEpoch(windowId) {
  const key = String(windowId || "").trim();
  if (!key) return 0;
  const nextValue = terminalWindowActionEpoch(key) + 1;
  terminalWindowActionEpochById.set(key, nextValue);
  return nextValue;
}

function mergeTerminalWindowState(nextState, windowId) {
  if (!nextState || typeof nextState !== "object") return false;
  const targetWindowId = String(windowId || "").trim();
  if (!targetWindowId) {
    state.terminalExecution = nextState;
    return true;
  }
  const nextWindows = Array.isArray(nextState.windows) ? nextState.windows : [];
  const nextWindow = nextWindows.find((item) => String(item?.id || "") === targetWindowId);
  if (!nextWindow) {
    state.terminalExecution = nextState;
    return true;
  }
  const currentState = (state.terminalExecution && typeof state.terminalExecution === "object")
    ? state.terminalExecution
    : {};
  const currentWindows = Array.isArray(currentState.windows) ? currentState.windows : [];
  let replaced = false;
  const mergedWindows = currentWindows.map((item) => {
    if (String(item?.id || "") === targetWindowId) {
      replaced = true;
      return nextWindow;
    }
    return item;
  });
  if (!replaced) mergedWindows.push(nextWindow);
  state.terminalExecution = {
    ...currentState,
    ...nextState,
    windows: mergedWindows,
  };
  return true;
}

function terminalButtonCooldownRemaining(windowId, accountId, action) {
  const key = terminalCooldownKey(windowId, accountId, action);
  const expiresAt = Number(terminalButtonCooldownByKey.get(key) || 0);
  if (!expiresAt) return 0;
  const remainMs = expiresAt - Date.now();
  if (remainMs <= 0) {
    terminalButtonCooldownByKey.delete(key);
    return 0;
  }
  return Math.ceil(remainMs / 1000);
}

function startTerminalCooldownTicker() {
  if (terminalButtonCooldownTimer) return;
  terminalButtonCooldownTimer = window.setInterval(() => {
    if (!terminalButtonCooldownByKey.size) {
      window.clearInterval(terminalButtonCooldownTimer);
      terminalButtonCooldownTimer = null;
      return;
    }
    const now = Date.now();
    for (const [key, expiresAt] of terminalButtonCooldownByKey.entries()) {
      if (Number(expiresAt || 0) <= now) terminalButtonCooldownByKey.delete(key);
    }
    if (currentView === "terminal-execution" && terminalCurrentRoute() === "wechat") {
      updateTerminalManualCountdowns();
    }
  }, 1000);
}

function setTerminalButtonCooldown(windowId, accountId, action, seconds = 15) {
  const w = String(windowId || "").trim();
  const a = String(accountId || "").trim();
  const act = String(action || "").trim();
  if (!w || !a || !act) return;
  terminalButtonCooldownByKey.set(terminalCooldownKey(w, a, act), Date.now() + Math.max(1, Number(seconds) || 15) * 1000);
  startTerminalCooldownTicker();
}

function clearTerminalButtonCooldown(windowId, accountId, action) {
  const w = String(windowId || "").trim();
  const a = String(accountId || "").trim();
  const act = String(action || "").trim();
  if (!w || !a || !act) return;
  terminalButtonCooldownByKey.delete(terminalCooldownKey(w, a, act));
}

function terminalCurrentWindowAccountId(windowId) {
  const w = (state.terminalExecution.windows || []).find((item) => String(item.id) === String(windowId));
  if (!w) return "";
  return terminalResolveWindowAccountId(w, Number(w.current_index || 0));
}

function terminalReapplyInFlightWindowActions() {
  if (!terminalAutoPublishWindowIds.size) return;
  for (const windowId of terminalAutoPublishWindowIds) {
    const accountId = terminalCurrentWindowAccountId(windowId);
    if (!accountId) continue;
    // Keep optimistic publish stage stable for windows that are still waiting for API response.
    terminalForcePostLoginStage(windowId, accountId);
    markTerminalManualAction(windowId, accountId, "publish");
  }
}

function terminalMarkPublishPending(windowId, accountId) {
  const windowKey = String(windowId || "").trim();
  const accountKey = String(accountId || "").trim();
  if (!windowKey || !accountKey) return;
  const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === windowKey);
  const targetAccount = (targetWindow?.accounts || []).find((item) => String(item?.id ?? item?.account_id ?? "") === accountKey);
  if (!targetAccount || typeof targetAccount !== "object") return;
  delete targetAccount.error_stage;
  delete targetAccount.error_title;
  delete targetAccount.error_detail;
  targetAccount.status = "running";
  targetAccount.status_text = "发布页准备中";
  targetAccount.task_id = null;
}

function resetTerminalManualAction(windowId, accountId) {
  const manualState = terminalManualActionState(windowId, accountId);
  manualState.loginClicked = false;
  manualState.publishClicked = false;
  manualState.nextClicked = false;
  const windowKey = String(windowId || "").trim();
  const accountKey = String(accountId || "").trim();
  const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === windowKey);
  const targetAccount = (targetWindow?.accounts || []).find((item) => String(item?.id ?? item?.account_id ?? "") === accountKey);
  if (targetAccount && typeof targetAccount === "object") {
    targetAccount.login_clicked = false;
    targetAccount.publish_clicked = false;
    targetAccount.next_clicked = false;
  }
}

function terminalAdvanceWindowLocally(windowId) {
  const target = (state.terminalExecution.windows || []).find((item) => String(item.id) === String(windowId));
  if (!target) return false;
  const accounts = Array.isArray(target.accounts) ? target.accounts : [];
  if (!accounts.length) return false;
  const currentIndex = Number(target.current_index || 0);
  if (currentIndex >= accounts.length) return false;
  const current = accounts[currentIndex] || {};
  if (String(current.status || "").toLowerCase() !== "success") {
    current.status = "success";
    current.status_text = "已完成";
  }
  current.next_clicked = true;
  const nextIndex = currentIndex + 1;
  target.current_index = nextIndex;
  target.confirming_next = false;
  target.qr_refreshing = false;
  if (nextIndex < accounts.length) {
    const nextAccount = accounts[nextIndex] || {};
    nextAccount.login_clicked = true;
    nextAccount.publish_clicked = false;
    nextAccount.next_clicked = false;
    if (String(nextAccount.status || "").toLowerCase() === "pending" || !String(nextAccount.status_text || "").trim()) {
      nextAccount.status_text = "待发布";
    }
  }
  if (nextIndex >= accounts.length) {
    target.completed = true;
  }
  return true;
}

function terminalWindowActionButtons(window, current, loginStarted) {
  const accounts = window.accounts || [];
  const windowIdText = String(window?.id || "").trim();
  const currentIndex = Number(window.current_index || 0);
  const confirmingNext = Boolean(window?.confirming_next);
  const completed = terminalWindowIsCompleted(window);
  if (completed) {
    return `
      <div class="terminal-window-actions">
        <button class="terminal-col-btn completed" type="button" disabled>全部完成</button>
      </div>
    `;
  }
  const hasCurrent = currentIndex >= 0 && currentIndex < accounts.length;
  const inFlightAutoPublish = terminalAutoPublishWindowIds.has(windowIdText);
  const resettingWindow = terminalResetWindowIds.has(windowIdText);
  const currentAccountId = terminalResolveAccountId(current) || terminalResolveWindowAccountId(window, currentIndex);
  const cycleAccountMarker = terminalManualCycleAccountByWindow.get(windowIdText) || "";
  const currentAccountMarker = currentAccountId;
  if (currentAccountMarker && cycleAccountMarker !== currentAccountMarker) {
    const localWindowMap = terminalManualActionStateByWindow.get(windowIdText);
    if (localWindowMap instanceof Map) {
      localWindowMap.delete(currentAccountMarker);
    }
    terminalManualCycleAccountByWindow.set(windowIdText, currentAccountMarker);
  }
  const manualState = hasCurrent && currentAccountId
    ? terminalManualActionState(window.id, currentAccountId)
    : { loginClicked: false, publishClicked: false, nextClicked: false };
  const cycleStage = !manualState.loginClicked
    ? "login"
    : (!manualState.publishClicked ? "publish" : "next");
  const hasNext = currentIndex + 1 < accounts.length;
  const canLoginBase = hasCurrent && cycleStage === "login" && !confirmingNext && !resettingWindow;
  const canAutoPublish = hasCurrent && cycleStage === "publish" && !confirmingNext && !inFlightAutoPublish && !resettingWindow;
  const canConfirm = hasCurrent && cycleStage === "next" && !confirmingNext && !resettingWindow;
  const publishCooldown = hasCurrent && currentAccountId ? terminalButtonCooldownRemaining(windowIdText, currentAccountId, "publish") : 0;
  const publishLoginCooldown = hasCurrent && currentAccountId ? terminalButtonCooldownRemaining(windowIdText, currentAccountId, "publish_login_wait") : 0;
  const nextCooldown = hasCurrent && currentAccountId ? terminalButtonCooldownRemaining(windowIdText, currentAccountId, "next") : 0;
  const publishBlockedByLoginWait = publishLoginCooldown > 0;
  const effectivePublishCooldown = Math.max(publishCooldown, publishLoginCooldown);
  const publishButtonLoading = false;
  const publishLabel = publishBlockedByLoginWait
    ? `请扫码登录(${publishLoginCooldown}s)`
    : (effectivePublishCooldown > 0 ? `发布(${effectivePublishCooldown}s)` : "发布");
  const confirmReadyLabel = hasNext ? "下一个" : "完成";
  const confirmIdleLabel = hasNext ? "下一个" : "完成";
  const confirmLabel = nextCooldown > 0
    ? `${hasNext ? "下一个" : "完成"}(${nextCooldown}s)`
    : (canConfirm ? confirmReadyLabel : confirmIdleLabel);
  const showLoginButton = cycleStage === "login";
  const publishButtonClass = `terminal-col-btn ${cycleStage === "publish" ? "cycle-active" : ""}`;
  const publishButtonBusy = "false";
  const publishButtonContent = publishLabel;
  const qrState = terminalQrLifecycle(window);
  const loginAccountId = currentAccountId;
  const loginOpening = qrState.placeholderState === "opening";
  const canLogin = canLoginBase && !loginOpening;
  const loginButtonClass = `terminal-col-btn secondary ${cycleStage === "login" ? "cycle-active" : ""} ${loginOpening ? "loading strong-loading" : ""}`;
  const loginButtonContent = loginOpening ? `${loadingInline("登录中...")}` : "登录";
  const nextButtonClass = `terminal-col-btn secondary ${cycleStage === "next" ? "cycle-active" : ""} ${confirmingNext ? "loading strong-loading" : ""}`;
  const canAutoPublishWithCooldown = canAutoPublish && effectivePublishCooldown <= 0;
  const canConfirmWithCooldown = canConfirm && nextCooldown <= 0;
  return `
    <div class="terminal-window-actions">
      <div class="terminal-window-actions-top ${showLoginButton ? "" : "single-action"}">
        ${showLoginButton ? `<button class="${loginButtonClass}" type="button" data-terminal-login-open="${window.id}:${loginAccountId}" ${canLogin ? "" : "disabled"} aria-busy="${loginOpening ? "true" : "false"}">${loginButtonContent}</button>` : ""}
        <button class="${publishButtonClass}" type="button" data-terminal-auto-publish="${window.id}" ${canAutoPublishWithCooldown ? "" : "disabled"} aria-busy="${publishButtonBusy}">${publishButtonContent}</button>
      </div>
      <button class="${nextButtonClass}" type="button" data-terminal-confirm-success="${window.id}" ${canConfirmWithCooldown ? "" : "disabled"} aria-busy="${confirmingNext ? "true" : "false"}">${confirmingNext ? `${loadingInline("正在进入下一个...")}` : confirmLabel}</button>
    </div>
  `;
}

function terminalQrImageMarkup(window, currentAccountId) {
  const qrState = terminalQrLifecycle(window);
  const accounts = Array.isArray(window?.accounts) ? window.accounts : [];
  const currentIndex = Number(window?.current_index || 0);
  const fallbackCurrentId = terminalResolveWindowAccountId(window, currentIndex);
  const refreshAccountId = String(currentAccountId || "").trim() || fallbackCurrentId;
  if (qrState.placeholderState === "completed") {
    return `
      <button class="terminal-qr-image-button" type="button" disabled aria-label="全部完成">
        ${terminalPlaceholderIcon()}
      </button>
    `;
  }
  if (qrState.placeholderState === "ready") {
    return `
      <button class="terminal-qr-image-button browser-state" type="button" disabled aria-label="已登录，无需扫码">
        ${terminalPlaceholderIcon()}
        <span class="terminal-qr-loading-text">已登录，无需扫码</span>
      </button>
    `;
  }
  if (qrState.placeholderState === "browser_closed") {
    return `
      <button class="terminal-qr-image-button browser-state" type="button" data-terminal-qr-refresh="${window.id}:${refreshAccountId}" aria-label="浏览器已关闭，点击重新打开">
        ${terminalPlaceholderIcon()}
        <span class="terminal-qr-loading-text">浏览器已关闭<br>点击重新打开</span>
      </button>
    `;
  }
  if (qrState.placeholderState === "opening") {
    return `
      <button class="terminal-qr-image-button loading-state" type="button" disabled aria-busy="true" aria-label="正在打开登录浏览器">
        <span class="btn-spinner" aria-hidden="true"></span>
        <span class="terminal-qr-loading-text">正在打开浏览器</span>
      </button>
    `;
  }
  if (qrState.placeholderState === "confirming_next") {
    return `
      <button class="terminal-qr-image-button loading-state confirming-state" type="button" disabled aria-busy="true" aria-label="正在进入下一个账号">
        <span class="btn-spinner" aria-hidden="true"></span>
        <span class="terminal-qr-loading-text">正在进入下一个账号</span>
      </button>
    `;
  }
  if (qrState.placeholderState === "browser") {
    return `
      <button class="terminal-qr-image-button browser-state" type="button" data-terminal-qr-refresh="${window.id}:${refreshAccountId}" aria-label="已打开浏览器，点击显示窗口">
        ${terminalPlaceholderIcon()}
        <span class="terminal-qr-loading-text">已打开浏览器<br>点击显示窗口</span>
      </button>
    `;
  }
  return `
    <button class="terminal-qr-image-button browser-state" type="button" data-terminal-qr-refresh="${window.id}:${refreshAccountId}" aria-label="打开登录浏览器">
      ${terminalPlaceholderIcon()}
      <span class="terminal-qr-loading-text">打开浏览器扫码</span>
    </button>
  `;
}

function terminalWechatAccountStatusText(window, account, index, currentIndex, loginStarted) {
  if (terminalCurrentIsManualConfirmable(window, account, index, currentIndex)) {
    return TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT;
  }
  const qrState = terminalQrLifecycle(window);
  if (index === currentIndex && loginStarted && String(account.status || "") === "waiting_qr") {
    if (qrState.placeholderState === "opening") {
      return "正在打开登录浏览器...";
    }
    const rawStatus = sanitizeTerminalStatusText(account.status_text, account.status);
    if (rawStatus && rawStatus !== "未登录") {
      return rawStatus;
    }
    return TERMINAL_LOGIN_CONFIRM_TEXT;
  }
  if (index === currentIndex && loginStarted && String(account.status || "").toLowerCase() === "ready" && terminalQrLifecycle(window).placeholderState === "browser_closed") {
    return "已登录，浏览器已关闭";
  }
  return sanitizeTerminalStatusText(account.status_text, account.status);
}

function sanitizeTerminalStatusText(statusText, status) {
  const raw = String(statusText || "").trim();
  if (!raw) return "未登录";
  if (/请扫码.*我已登录/.test(raw) || /扫码.*点登录/.test(raw)) return "扫码后点登录";
  if (/等待开始登录/.test(raw) || /等待打开登录浏览器/.test(raw) || /待点登录/.test(raw)) return "待点登录";
  if (/正在打开登录浏览器/.test(raw)) return "正在打开浏览器";
  if (/正在准备发布页面/.test(raw)) return "发布页准备中";
  if (/登录.*点击发布.*(点成功|成功)/.test(raw)) return "待发布";
  if (/手动发布.*点成功/.test(raw) || /已发布后点下一个/.test(raw)) return "已发布后点下一个";
  if (/发布成功/.test(raw) || /已完成/.test(raw)) return "已完成";
  if (terminalTextIsManualConfirmableFailure(raw)) {
    return TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT;
  }
  const lowered = raw.toLowerCase();
  if (lowered.includes("account not found") || /未找到账号|账号不存在/.test(raw)) {
    return "账号不存在。请到账号矩阵检查后重试。";
  }
  if (
    lowered.includes("httpsconnectionpool(") ||
    lowered.includes("max retries exceeded") ||
    lowered.includes("traceback") ||
    lowered.includes("connection aborted") ||
    lowered.includes("name or service not known")
  ) {
    return "网络连接异常，请稍后重试";
  }
  if (/[A-Za-z]{3,}/.test(raw)) {
    return "执行异常，请重试；若仍失败，请检查账号配置";
  }
  if ((String(status || "").toLowerCase() === "error" || String(status || "").toLowerCase() === "failed") && raw.length > 64) {
    return "执行异常，请查看日志";
  }
  return raw;
}

function terminalStatusNeedsManualConfirm(account, statusText) {
  const text = String(statusText || account?.status_text || "").trim();
  return terminalAccountIsManualConfirmable(account)
    || text.includes("等待人工确认")
    || text.includes("手动发布")
    || text.includes("点成功");
}

function terminalManualConfirmTone(account, statusText) {
  const text = String(statusText || account?.status_text || "").trim();
  if (!terminalStatusNeedsManualConfirm(account, text)) return "";
  if (terminalAccountIsManualConfirmable(account) || text.includes(TERMINAL_LEGACY_MANUAL_CONFIRM_TEXT)) return "warning";
  return "success";
}

function terminalManualConfirmClass(account, statusText) {
  const tone = terminalManualConfirmTone(account, statusText);
  return tone ? ` manual-confirm-${tone}` : "";
}

function terminalStatusSizeClass(statusText) {
  const text = String(statusText || "").trim();
  if (!text) return "";
  if (text.length >= 14) return " compact";
  if (text.length >= 10) return " compact-sm";
  return "";
}

function terminalAccountStatusMarkup(account, statusText) {
  const text = escapeHtml(statusText || "");
  if (!terminalStatusNeedsManualConfirm(account, statusText)) return text;
  const tone = terminalManualConfirmTone(account, statusText);
  const isWarning = tone === "warning";
  return `
    <span class="terminal-acc-confirm-icon ${isWarning ? "warning" : "success"}" aria-hidden="true">
      ${isWarning
    ? `<svg viewBox="0 0 24 24"><path d="M12 3 2.5 20h19L12 3zm-1 6h2v6h-2zm0 7h2v2h-2z"/></svg>`
    : `<svg viewBox="0 0 24 24"><path d="M12 2a10 10 0 1 0 10 10A10 10 0 0 0 12 2Zm-1 14-4-4 1.4-1.4L11 13.2l4.6-4.6L17 10l-6 6Z"/></svg>`}
    </span>
    <span class="terminal-acc-status-text">${text}</span>
  `;
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
        ? `<svg viewBox="0 0 24 24" role="img" aria-label="视频号"><path d="M12 2a10 10 0 1 0 10 10h-2a8 8 0 1 1-8-8z"/><path d="M11 6h2v7h5v2h-7z"/></svg>`
        : token === "pending"
          ? `<svg viewBox="0 0 24 24" role="img" aria-label="待处理"><path d="M12 4a8 8 0 1 0 8 8h-2a6 6 0 1 1-6-6z"/></svg>`
          : `<svg viewBox="0 0 24 24" role="img" aria-label="未登录"><path d="M12 5a7 7 0 1 0 7 7h-2a5 5 0 1 1-5-5z"/></svg>`;
  return `<div class="terminal-avatar terminal-avatar-${token}">${icon}</div>`;
}

function terminalAccountTaskBadge(account) {
  const taskId = String(account?.task_id || "").trim();
  if (taskId) {
    return `<div class="terminal-status-badge">任务:${taskId}</div>`;
  }
  return "";
}

function terminalProgressMarkup(accounts, successCount) {
  const total = Array.isArray(accounts) ? accounts.length : 0;
  const done = Math.max(0, Number(successCount) || 0);
  const percent = total ? Math.round((done / total) * 100) : 0;
  return `
    <div class="terminal-progress-bar" aria-label="窗口进度 ${done}/${total}">
      <div class="terminal-progress-fill" style="width:${percent}%;"></div>
      <span class="terminal-progress-label">${done}/${total}</span>
    </div>
  `;
}

function terminalWechatAccountItemMarkup(window, account, index, currentIndex, loginStarted, options = {}) {
  const displayAccount = terminalDisplayAccount(window, account, index, currentIndex);
  const statusText = terminalWechatAccountStatusText(window, displayAccount, index, currentIndex, loginStarted);
  const needsManualConfirm = terminalStatusNeedsManualConfirm(displayAccount, statusText);
  const manualConfirmClass = terminalManualConfirmClass(displayAccount, statusText);
  const statusSizeClass = terminalStatusSizeClass(statusText);
  const isCurrent = Boolean(options?.isCurrent);
  return `
    <div class="terminal-account-item ${isCurrent ? "active" : ""} ${needsManualConfirm ? `manual-confirm${manualConfirmClass}` : ""}">
      <div class="terminal-account-info">
        ${terminalAccountStatusAvatar(displayAccount)}
        <div>
          <div class="terminal-acc-name">${displayAccount.display_name || displayAccount.account_key || `账号 ${displayAccount.id}`}</div>
          <div class="terminal-acc-status ${needsManualConfirm ? `manual-confirm${manualConfirmClass}` : ""}${statusSizeClass}" ${isCurrent ? `data-terminal-current-status="${window.id}"` : ""}>${terminalAccountStatusMarkup(displayAccount, statusText)}</div>
        </div>
      </div>
      ${terminalAccountTaskBadge(displayAccount)}
    </div>
  `;
}

function terminalWechatWindowMarkup(window, loginStarted) {
  const accounts = window.accounts || [];
  const currentIndex = Number(window.current_index || 0);
  const color = window.color || "#EF4444";
  const colorDim = `${color}33`;
  const successCount = accounts.filter((account) => String(account?.status || "").toLowerCase() === "success").length;
  const current = accounts[currentIndex] || {};
  const hasCurrent = currentIndex >= 0 && currentIndex < accounts.length;
  const resetAccountId = String(current?.id ?? accounts?.[currentIndex]?.id ?? accounts?.[0]?.id ?? "").trim();
  const actionsMarkup = terminalWindowActionButtons(window, current, loginStarted);
  const progressMarkup = terminalProgressMarkup(accounts, successCount);
  const currentAccountMarkup = hasCurrent
    ? terminalWechatAccountItemMarkup(window, current, currentIndex, currentIndex, loginStarted, { isCurrent: true })
    : `<div class="terminal-account-current-empty muted">当前窗口已无进行中账号</div>`;
  const queueAccountsMarkup = accounts
    .map((account, index) => ({ account, index }))
    .filter((item) => item.index !== currentIndex)
    .map((item) => terminalWechatAccountItemMarkup(window, item.account, item.index, currentIndex, loginStarted))
    .join("");
  const currentPositionText = hasCurrent ? `#${currentIndex + 1}/${accounts.length}` : "已完成";
  return `
    <div class="terminal-task-column terminal-glass ${window?.confirming_next ? "confirming-next" : ""}" data-terminal-window-id="${window.id}" style="--term-color:${color};--term-color-dim:${colorDim}">
      <div class="terminal-color-anchor"></div>
      <div class="terminal-col-header">
          <div class="terminal-col-header-top">
            <span class="terminal-col-title">${terminalWindowLabel(window.id)}</span>
            <div class="terminal-col-header-controls">
              <span class="terminal-status-badge theme">色标: ${window.color_name || ""}</span>
              <button class="terminal-header-reset-btn" type="button" data-terminal-cycle-reset="${window.id}:${resetAccountId}" ${resetAccountId ? "" : "disabled"} title="重置当前账号按钮流程">重置</button>
            </div>
          </div>
        <div class="terminal-wx-operator">运营微信: ${window.operator_wechat || "-"}</div>
        <div class="terminal-col-header-progress">${progressMarkup}</div>
      </div>
      <div class="terminal-col-actions">
        ${actionsMarkup}
      </div>
      <div class="terminal-account-current">
        <div class="terminal-account-current-head">
          <span>当前操作账号</span>
          <strong>${currentPositionText}</strong>
        </div>
        ${currentAccountMarkup}
      </div>
      <div class="terminal-account-list">
        ${queueAccountsMarkup || `<div class="terminal-account-queue-empty muted">暂无待处理或历史账号</div>`}
      </div>
    </div>
  `;
}

function syncTerminalWechatSummary(summary, windows) {
  const summaryNode = document.querySelector(".terminal-wechat-summary");
  if (summaryNode) {
    summaryNode.innerHTML = `
      <div class="metric"><span>总账号数</span><strong>${summary.total || 0}</strong></div>
      <div class="metric"><span>已完成账号数</span><strong>${summary.success || 0}</strong></div>
      <div class="metric"><span>当日素材剩余数量</span><strong>${summary.remaining_material_videos || 0}</strong></div>
      <div class="metric"><span>当日总素材数量</span><strong>${summary.today_materials || 0}</strong></div>
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

function replaceTerminalWindowNode(refreshRoot, windowId, windowData, loginStarted) {
  const liveNode = refreshRoot?.querySelector(`[data-terminal-window-id="${windowId}"]`);
  if (!liveNode || !liveNode.parentNode) return false;
  liveNode.outerHTML = terminalWechatWindowMarkup(windowData, loginStarted);
  return true;
}

async function refreshTerminalAccountQr(windowId, accountId, button) {
  const requestWindowId = String(windowId || "").trim();
  const requestEpoch = terminalWindowActionEpoch(requestWindowId);
  const restoreButton = setButtonLoading(button, "打开中");
  terminalErrorModalSignature = "";
  hideTerminalErrorModal();
  try {
    const refreshRoot = document.querySelector(".terminal-workspace-wechat") || document.querySelector("#terminal-matrix-workspace");
    const pageScrollX = window.scrollX;
    const pageScrollY = window.scrollY;
    const rootScrollLeft = refreshRoot?.scrollLeft ?? 0;
    const rootScrollTop = refreshRoot?.scrollTop ?? 0;
    const targetNode = refreshRoot?.querySelector(`[data-terminal-window-id="${windowId}"]`);
    targetNode?.querySelectorAll(".terminal-qr-placeholder img[alt='视频号登录二维码']").forEach((img) => img.remove());
    const currentStateWindows = state.terminalExecution.windows || [];
    const pendingWindow = currentStateWindows.find((item) => String(item.id) === String(windowId));
    if (pendingWindow) {
      pendingWindow.qr_refreshing = true;
      pendingWindow.qr_url = "";
      pendingWindow.qr_expires_at = 0;
      if (refreshRoot) {
        const loginStartedPending = Boolean(state.terminalExecution.login_started);
        if (replaceTerminalWindowNode(refreshRoot, windowId, pendingWindow, loginStartedPending)) {
          syncTerminalWechatSummary(state.terminalExecution.summary || {}, currentStateWindows);
          refreshRoot.scrollLeft = rootScrollLeft;
          refreshRoot.scrollTop = rootScrollTop;
          window.scrollTo(pageScrollX, pageScrollY);
        } else {
          renderTerminalExecution();
        }
      } else {
        renderTerminalExecution();
      }
    }

    const nextState = await api(`/api/terminal-execution/windows/${windowId}/accounts/${accountId}/qr`, { method: "POST" });
    if (terminalWindowActionEpoch(requestWindowId) !== requestEpoch) return;
    state.terminalExecution = nextState;
    terminalReapplyInFlightWindowActions();
    terminalForcePostLoginStage(windowId, accountId);
    terminalManualCycleAccountByWindow.set(String(windowId || "").trim(), String(accountId || "").trim());
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
    if (refreshRoot && targetWindow && replaceTerminalWindowNode(refreshRoot, windowId, targetWindow, loginStarted)) {
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
    if (terminalWindowActionEpoch(requestWindowId) !== requestEpoch) return;
    const currentStateWindows = state.terminalExecution.windows || [];
    const pendingWindow = currentStateWindows.find((item) => String(item.id) === String(windowId));
    if (pendingWindow) {
      pendingWindow.qr_refreshing = false;
    }
    renderTerminalExecution();
    showTerminalErrorModal({
      stage: "login_browser",
      title: "登录浏览器打开失败",
      message: error.message || "登录浏览器打开失败",
      context: `窗口 #${windowId} · 账号 #${accountId}`,
      signature: `login-browser|${windowId}|${accountId}|${error.message || "unknown"}`,
    });
  } finally {
    restoreButton();
  }
}

function installGlobalButtonLoading() {
  document.addEventListener("click", (event) => {
    const button = event.target.closest("button");
    if (!button || button.disabled || button.classList.contains("loading")) return;
    if (String(button.getAttribute("type") || "").toLowerCase() === "submit") return;
    if (button.dataset.noGlobalLoading === "1") return;
    if (button.matches("[data-terminal-auto-publish], [data-terminal-confirm-success], [data-terminal-qr-refresh], [data-terminal-login-open], [data-terminal-cycle-reset], [data-terminal-save-config], #terminal-save-config, #terminal-save-platform-config, [data-open], [data-delete-account], [data-task-bulk-status], [data-task-bulk-delete], [data-task-select-all], [data-task-select], [data-notice-route], [data-save-notification-policy], [data-incident-action], [data-interaction-tab], [data-interaction-comment-run], [data-interaction-private-run], [data-interaction-refresh], #interaction-management-save, #user-menu-toggle, #top-user-toggle")) return;
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
    startLoginButton.textContent = "打开登录浏览器";
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
    const color = window.color || "#EF4444";
    const colorDim = `${color}33`;
    const successCount = accounts.filter((account) => String(account?.status || "").toLowerCase() === "success").length;
    const current = accounts[currentIndex] || {};
    const manualWait = loginStarted ? Math.max(0, Number(window.manual_available_at || 0) - Math.floor(Date.now() / 1000)) : 0;
    const qrVisible = loginStarted && window.qr_url;
    return `
      <div class="terminal-task-column terminal-glass ${window?.confirming_next ? "confirming-next" : ""}" data-terminal-window-id="${window.id}" style="--term-color:${color};--term-color-dim:${colorDim}">
        <div class="terminal-color-anchor"></div>
        <div class="terminal-col-header">
          <div class="terminal-col-header-top">
            <span class="terminal-col-title">终端执行窗 ${String(window.id).padStart(2, "0")}</span>
            <span class="terminal-status-badge theme">色标: ${window.color_name || ""}</span>
          </div>
          <div class="terminal-wx-operator">运营微信: ${window.operator_wechat || "-"}</div>
        </div>
        <div class="terminal-qr-section">
          <div class="terminal-qr-placeholder">${terminalQrImageMarkup(window, qrVisible, current.id)}</div>
          <div class="terminal-qr-status-row"><span class="terminal-qr-sequence">#${Number(window.qr_sequence || 0)}</span></div>
        </div>
        <div class="terminal-account-list">
          ${accounts.map((account, index) => {
            const displayAccount = terminalDisplayAccount(window, account, index, currentIndex);
            const statusText = terminalWechatAccountStatusText(window, displayAccount, index, currentIndex, loginStarted);
            const needsManualConfirm = terminalStatusNeedsManualConfirm(displayAccount, statusText);
            const manualConfirmClass = terminalManualConfirmClass(displayAccount, statusText);
            const statusSizeClass = terminalStatusSizeClass(statusText);
            return `
            <div class="terminal-account-item ${index === currentIndex ? "active" : ""} ${needsManualConfirm ? `manual-confirm${manualConfirmClass}` : ""}">
              <div class="terminal-account-info">
                ${terminalAccountStatusAvatar(displayAccount)}
                <div>
                  <div class="terminal-acc-name">${displayAccount.display_name || displayAccount.account_key || `账号 ${displayAccount.id}`}</div>
                  <div class="terminal-acc-status ${needsManualConfirm ? `manual-confirm${manualConfirmClass}` : ""}${statusSizeClass}">${terminalAccountStatusMarkup(displayAccount, statusText)}</div>
                </div>
              </div>
            ${terminalAccountTaskBadge(displayAccount)}
          </div>
        `;
          }).join("") || `<div class="muted">暂无账号</div>`}
        </div>
        <div class="terminal-col-footer">
          ${terminalProgressMarkup(accounts, successCount)}
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
        <div>切换到其它平台后，会改为长会话配置/检测视图，不复用扫码浏览器占位。</div>
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

function renderTerminalWechatQuickActionBar() {
  const choices = terminalWechatAccountChoices();
  const selected = terminalWechatSelectedAccountChoice();
  const optionsMarkup = choices.length
    ? choices.map((choice) => `
        <option value="${escapeHtml(choice.accountId)}"${String(choice.accountId) === String(selected?.accountId || "") ? " selected" : ""}>
          ${escapeHtml(choice.label)}
        </option>
      `).join("")
    : `<option value="">暂无可选账号</option>`;
  const loginDisabled = !selected;
  const publishDisabled = !selected;
  const buttonLoadingAttr = " data-no-global-loading=\"1\"";
  return `
    <div class="terminal-wechat-quick-action terminal-glass">
      <div class="terminal-wechat-quick-action-buttons">
        <button class="btn secondary" type="button" data-terminal-wechat-login-selected${buttonLoadingAttr} ${loginDisabled ? "disabled" : ""}>登录</button>
        <button class="btn primary" type="button" data-terminal-wechat-publish-selected${buttonLoadingAttr} ${publishDisabled ? "disabled" : ""}>发布</button>
      </div>
      <label class="terminal-wechat-account-select-field">
        <span>指定账号</span>
        <select id="terminal-wechat-account-select">
          ${optionsMarkup}
        </select>
      </label>
    </div>
  `;
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
    startLoginButton.textContent = context.platform === "wechat" ? "打开登录浏览器" : "检测全部";
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
  document.querySelectorAll("[data-terminal-auto-publish]").forEach((button) => {
    const window = windowById.get(String(button.dataset.terminalAutoPublish || ""));
    if (!window || updatedWindows.has(String(window.id))) return;
    const windowNode = button.closest(".terminal-task-column");
    const actionsContainer = windowNode?.querySelector(".terminal-col-actions") || windowNode?.querySelector(".terminal-col-footer");
    if (!actionsContainer) return;
    if (actionsContainer.querySelector("button.loading")) return;
    const currentIndex = Number(window.current_index || 0);
    const current = window.accounts?.[currentIndex] || {};
    const nextActionsMarkup = terminalWindowActionButtons(window, current, loginStarted).trim();
    const actionsNode = actionsContainer.querySelector(".terminal-window-actions");
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
      actionsContainer.insertAdjacentHTML("beforeend", nextActionsMarkup);
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
      const statusText = terminalWechatAccountStatusText(window, current, currentIndex, currentIndex, loginStarted);
      const needsManualConfirm = terminalStatusNeedsManualConfirm(current, statusText);
      const manualConfirmTone = terminalManualConfirmTone(current, statusText);
      currentStatusNode.innerHTML = terminalAccountStatusMarkup(current, statusText);
      currentStatusNode.classList.toggle("manual-confirm", needsManualConfirm);
      currentStatusNode.classList.toggle("manual-confirm-success", needsManualConfirm && manualConfirmTone === "success");
      currentStatusNode.classList.toggle("manual-confirm-warning", needsManualConfirm && manualConfirmTone === "warning");
      currentStatusNode.closest(".terminal-account-item")?.classList.toggle("manual-confirm", needsManualConfirm);
      currentStatusNode.closest(".terminal-account-item")?.classList.toggle("manual-confirm-success", needsManualConfirm && manualConfirmTone === "success");
      currentStatusNode.closest(".terminal-account-item")?.classList.toggle("manual-confirm-warning", needsManualConfirm && manualConfirmTone === "warning");
    }
  }
  updateTerminalManualCountdowns();
}

function startTerminalPolling() {
  if (terminalCountdownTimer) clearInterval(terminalCountdownTimer);
  terminalCountdownTimer = null;
  if (terminalWechatStatePollTimer) clearInterval(terminalWechatStatePollTimer);
  terminalWechatStatePollTimer = null;
  if (terminalCurrentRoute() !== "wechat") return;
  if (state.terminalConfigOpen) return;
  const hasAnyQr = (state.terminalExecution.windows || []).some((window) => Boolean(window?.qr_url));
  const hasSupplementalRuns = terminalWechatHasActiveSupplementalRuns();
  if (hasAnyQr) {
    updateTerminalQrCountdowns();
    terminalCountdownTimer = window.setInterval(updateTerminalQrCountdowns, 1000);
  }
  if (hasAnyQr || hasSupplementalRuns) {
    terminalWechatStatePollTimer = window.setInterval(refreshTerminalWechatState, 2000);
  }
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

function terminalWechatHasActiveSupplementalRuns(terminalState = state.terminalExecution) {
  return Array.isArray(terminalState?.emergency_publish_runs) && terminalState.emergency_publish_runs.some((run) => {
    const status = String(run?.status || "").toLowerCase();
    return status === "running";
  });
}

async function refreshTerminalWechatState() {
  if (terminalWechatStatePollInFlight) return;
  if (terminalCurrentRoute() !== "wechat" || state.terminalConfigOpen) return;
  terminalWechatStatePollInFlight = true;
  try {
    const nextState = await api("/api/terminal-execution/poll", {
      method: "POST",
      body: JSON.stringify({ allow_browser_open: true, allow_login_probe: true }),
    });
    state.terminalExecution = { ...nextState, loading: false };
    if (currentView === "terminal-execution" && terminalCurrentRoute() === "wechat") {
      renderTerminalExecution();
    }
  } catch {
    // Keep the page responsive even if polling briefly fails.
  } finally {
    terminalWechatStatePollInFlight = false;
  }
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
  const isLoading = Boolean(state.terminalExecution.loading);
  section.dataset.terminalRoute = route;

  if (isLoading && !isHubRoute) {
    workspace.innerHTML = `
      <div class="terminal-loading-state terminal-glass">
        <div class="loading-inline terminal-loading-inline">
          <span class="btn-spinner" aria-hidden="true"></span>
          <span>终端执行加载中，请稍候...</span>
        </div>
        <p class="muted">正在同步窗口状态、账号进度和二维码缓存。</p>
      </div>
    `;
    return;
  }

  if (configPanel && (isHubRoute || route === "wechat")) {
    configPanel.classList.add("hidden");
    configPanel.innerHTML = "";
  }
  if (!isHubRoute && route !== "wechat") {
    renderTerminalConfigPanel();
  }

  if (initModal) initModal.classList.toggle("hidden", !state.terminalConfigOpen);
  if (routeHint) routeHint.textContent = route === "wechat"
    ? "视频号独立流程，配置、登录队列、窗态和矩阵衔接都在本页闭环。"
    : route === "hub"
      ? "仅列平台入口，不混排扫码窗。"
      : "长会话平台统一模板，检测登录后再打开创作者后台。";
  if (terminalShellDesc) terminalShellDesc.textContent = route === "hub"
    ? "先选平台，再进入平台专属子流程。"
    : "一次登录长期有效；失效后重新检测或重新登录。";
  if (subtitle) subtitle.textContent = route === "hub"
    ? "平台枢纽页只列入口卡片，不混排视频号多窗。"
    : route === "wechat"
      ? "视频号独立流程：配置、打开登录浏览器、登录、发布、进入下一账号。"
      : "长会话平台：选账号、检测登录、打开创作者后台。";

  if (route === "hub") {
    if (state.terminalConfigOpen) {
      renderTerminalConfig();
    }
    const groups = [
      {
        title: "短会话平台",
        tone: "short",
        eyebrow: "每日登录",
        summary: "视频号独立扫码，流程与其它平台分开。",
        items: ["wechat"],
      },
      {
        title: "长会话平台",
        tone: "long",
        eyebrow: "长期会话",
        summary: "复用统一模板，检测登录后进入创作者后台。",
        items: ["douyin", "kuaishou", "xiaohongshu", "bilibili", "tiktok", "x", "linkedin", "facebook", "youtube", "vk", "instagram"],
      },
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
            const policy = terminalSessionPolicyLabel(capability.sessionPolicy || (platform === "wechat" ? "daily_qr" : "persistent"));
            const cardDesc = platform === "wechat"
              ? ""
              : "账号与浏览器配置复用同一套长会话入口。";
            const statusMarkup = platform === "wechat" ? "" : `<div class="terminal-entry-meta" aria-label="${terminalPlatformName(platform)} 状态">${terminalStatusChip(platform, health)}</div>`;
            return `
              <article class="terminal-entry-card ${platform === "wechat" ? "wechat" : "long-session"}">
                <div class="terminal-entry-head">
                  <div class="platform-name">${platformLogo(platform)}<strong>${terminalPlatformName(platform)}</strong></div>
                  <span class="terminal-policy-chip">${policy}</span>
                </div>
                ${cardDesc ? `<p class="terminal-entry-desc">${cardDesc}</p>` : ""}
                ${statusMarkup}
                <div class="terminal-entry-actions">
                  <button class="btn primary" type="button" data-terminal-enter="${platform === "wechat" ? "wechat" : platform}">${terminalCardButtonLabel(platform, health)}</button>
                  <button class="btn secondary" type="button" data-terminal-config-jump="${platform}">账号与浏览器配置</button>
                </div>
              </article>
            `;
          }).join("");
          const availableCount = group.items.filter((platform) => platformMap.has(platform)).length;
          return `
            <section class="terminal-group-panel terminal-group-${group.tone}">
              <div class="panel-head">
                <div>
                  <div class="terminal-group-title-row">
                    <h2>${group.title}</h2>
                    <span class="terminal-group-eyebrow">${group.eyebrow}</span>
                  </div>
                  <p class="muted">${group.summary}</p>
                </div>
                <span class="terminal-group-count">${availableCount} 个入口</span>
              </div>
              <div class="terminal-entry-grid">${cards}</div>
            </section>
          `;
        }).join("") : ""}
      </div>
    `;
    startTerminalPolling();
    return;
  }

  if (route === "wechat") {
    renderTerminalConfig();
    const hasWechatWindows = Array.isArray(state.terminalExecution.windows) && state.terminalExecution.windows.length > 0;
    const showWechatLoadingState = !loadError && !hasWechatWindows && !Boolean(state.terminalExecution.initialized);
    workspace.innerHTML = `
      <div class="terminal-wechat-page">
        ${loadError ? `<div class="terminal-load-error">${loadError}</div>` : ""}
        <div class="terminal-wechat-header">
          <h2>视频号终端</h2>
          <p class="muted">每日登录 / 多窗扫码 / 与素材矩阵的关系在这里闭环。</p>
        </div>
        ${renderTerminalWechatQuickActionBar()}
        <div class="terminal-wechat-summary">
          <div class="metric"><span>总账号数</span><strong>${showWechatLoadingState ? "加载中" : (summary.total || 0)}</strong></div>
          <div class="metric"><span>已完成账号数</span><strong>${showWechatLoadingState ? "加载中" : (summary.success || 0)}</strong></div>
          <div class="metric"><span>当日素材剩余数量</span><strong>${showWechatLoadingState ? "加载中" : (summary.remaining_material_videos || 0)}</strong></div>
          <div class="metric"><span>当日总素材数量</span><strong>${showWechatLoadingState ? "加载中" : (summary.today_materials || 0)}</strong></div>
        </div>
        ${showWechatLoadingState ? `<p class="system-action-state muted">正在拉取终端执行状态，请稍候…</p>` : ""}
        <div class="terminal-workspace terminal-workspace-wechat"></div>
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

function statsWechatActiveAccounts() {
  return (state.accounts || [])
    .filter((account) => String(account.status || "").toLowerCase() === "active")
    .filter((account) => {
      const platforms = Array.isArray(account.platforms) ? account.platforms : [];
      const wechat = platforms.find((item) => String(item.platform || "") === "wechat");
      if (!wechat) return false;
      if (wechat.enabled === undefined || wechat.enabled === null) return true;
      return Boolean(wechat.enabled);
    })
    .sort((a, b) => Number(a.id || 0) - Number(b.id || 0));
}

function setStatsMode(mode) {
  state.statsMode = mode === "capture" ? "capture" : "dashboard";
  const dashboardNode = document.querySelector("#stats-tab-dashboard");
  const captureNode = document.querySelector("#stats-tab-capture");
  const dashboardButton = document.querySelector("#stats-mode-dashboard");
  const captureButton = document.querySelector("#stats-mode-capture");
  if (dashboardNode) dashboardNode.classList.toggle("hidden", state.statsMode !== "dashboard");
  if (captureNode) captureNode.classList.toggle("hidden", state.statsMode !== "capture");
  if (dashboardButton) dashboardButton.classList.toggle("active", state.statsMode === "dashboard");
  if (captureButton) captureButton.classList.toggle("active", state.statsMode === "capture");
}

function renderStatsCapturePanel() {
  const statsCaptureStatus = state.statsCaptureStatus || {};
  const latest = statsCaptureStatus.latest_run || {};
  const lock = statsCaptureStatus.lock || {};
  const captureStatusNode = document.querySelector("#matrix-stats-capture-status");
  if (captureStatusNode) {
    const parts = [];
    if (latest.status) parts.push(latest.status);
    if (latest.target_date) parts.push(latest.target_date);
    if (!parts.length && lock.pid) parts.push(`PID ${lock.pid}`);
    captureStatusNode.textContent = parts.length ? parts.join(" · ") : "未加载";
  }
  const lockStatusNode = document.querySelector("#stats-capture-lock-status");
  if (lockStatusNode) {
    lockStatusNode.textContent = lock.pid ? `锁状态：运行中（PID ${lock.pid}）` : "锁状态：空闲";
    lockStatusNode.classList.toggle("danger", Boolean(lock.pid));
  }
  const lastRunNode = document.querySelector("#stats-capture-last-run");
  if (lastRunNode) {
    const runId = latest.run_id || "-";
    const statusLabel = {
      completed: "已完成",
      captured: "已采集",
      running: "进行中",
      partial_failed: "部分失败",
      failed: "失败",
      skipped: "已跳过",
      pending: "待处理",
    }[String(latest.status || "").toLowerCase()] || (latest.status || "-");
    const counts = `已采集 ${latest.captured_accounts || 0} / 跳过 ${latest.skipped_accounts || 0} / 失败 ${latest.failed_accounts || 0}`;
    lastRunNode.textContent = `最近运行：${runId} · ${statusLabel} · ${counts}`;
  }
  const captureAccountFilter = document.querySelector("#stats-capture-account-filter");
  if (captureAccountFilter) {
    const accounts = statsWechatActiveAccounts();
    const currentValue = captureAccountFilter.value;
    const options = accounts.map((account) => {
      const label = account.display_name || account.account_key || `账号 ${account.id}`;
      return `<option value="${account.id}">#${account.id} ${escapeHtml(label)}</option>`;
    }).join("");
    captureAccountFilter.innerHTML = `<option value="">请选择账号</option>${options}`;
    if (currentValue && [...captureAccountFilter.options].some((option) => option.value === currentValue)) {
      captureAccountFilter.value = currentValue;
    }
  }
}

function renderTongjiSection() {
  const overviewNode = document.querySelector("#tongji-overview");
  if (overviewNode) {
    overviewNode.innerHTML = TONGJI_SNAPSHOT.overview.map((card) => `
      <article class="tongji-card ${card.accent}">
        <small class="tongji-card-en">${card.en}</small>
        <span class="tongji-card-cn">${card.cn}</span>
        <div class="tongji-card-value">
          <strong>${card.value}</strong>
          ${card.unit ? `<span class="unit">${card.unit}</span>` : ""}
        </div>
      </article>
    `).join("");
  }

  const matrixNode = document.querySelector("#tongji-data-matrix");
  if (matrixNode) {
    matrixNode.innerHTML = TONGJI_SNAPSHOT.matrixRows.map((row) => {
      const [date, followers, deltaUp, deltaDown, plays, likes, recommend, shares, comments, traffic] = row;
      const signedPositive = deltaUp === "0" ? `<span class="muted-cell">0</span>` : `<span class="${deltaUp.startsWith("-") ? "negative" : "positive"}">${deltaUp}</span>`;
      const signedNegative = deltaDown === "0" ? `<span class="muted-cell">0</span>` : `<span class="${deltaDown.startsWith("-") ? "negative" : "positive"}">${deltaDown}</span>`;
      const playCellClass = Number(plays || 0) > 0 ? "blue-cell" : "muted-cell";
      const trafficCellClass = Number(traffic || 0) > 0 ? "orange-cell" : "muted-cell";
      return `
        <tr>
          <td class="time-cell">${date}</td>
          <td class="green-cell">${followers}</td>
          <td>${signedPositive} / ${signedNegative}</td>
          <td class="split-cell ${playCellClass}">${plays}</td>
          <td>${likes} / ${recommend}</td>
          <td>${shares} / ${comments}</td>
          <td class="${trafficCellClass}">${traffic}</td>
        </tr>
      `;
    }).join("");
  }

  if (!window.echarts) return;

  const tooltipStyle = {
    backgroundColor: "rgba(20, 20, 20, 0.85)",
    borderColor: "rgba(93, 214, 44, 0.3)",
    borderWidth: 1,
    textStyle: { color: "#E0E0E0", fontFamily: "JetBrains Mono" },
    backdropFilter: "blur(12px)",
    padding: 12,
  };

  const followerDom = document.getElementById("followerChart");
  if (followerDom) {
    tongjiFollowerChart = window.echarts.getInstanceByDom(followerDom) || window.echarts.init(followerDom, "dark");
    tongjiFollowerChart.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis", ...tooltipStyle },
      grid: { left: "3%", right: "3%", bottom: "5%", top: "15%", containLabel: true },
      xAxis: { type: "category", data: TONGJI_SNAPSHOT.dates, axisLine: { lineStyle: { color: "#333" } }, axisTick: { show: false } },
      yAxis: [
        { type: "value", splitLine: { lineStyle: { color: "#333", type: "dashed" } }, axisLabel: { color: "#c7c7c7" } },
        { type: "value", min: 1540, splitLine: { show: false }, axisLabel: { color: "#c7c7c7" } },
      ],
      series: [
        {
          name: "关注总数",
          type: "line",
          yAxisIndex: 1,
          smooth: true,
          symbolSize: 7,
          itemStyle: { color: "#5DD62C" },
          lineStyle: { width: 3, shadowColor: "rgba(93, 214, 44, 0.5)", shadowBlur: 10 },
          areaStyle: {
            color: new window.echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: "rgba(93, 214, 44, 0.3)" },
              { offset: 1, color: "rgba(93, 214, 44, 0)" },
            ]),
          },
          data: TONGJI_SNAPSHOT.totalFollowers,
        },
        {
          name: "新增关注",
          type: "bar",
          barWidth: "30%",
          itemStyle: { color: "#FF9900", borderRadius: [2, 2, 0, 0] },
          data: TONGJI_SNAPSHOT.newFollowers,
        },
        {
          name: "取关关注",
          type: "bar",
          barWidth: "30%",
          itemStyle: { color: "#FF3366", borderRadius: [0, 0, 2, 2] },
          data: TONGJI_SNAPSHOT.unfollowers,
        },
      ],
    }, true);
  }

  const videoDom = document.getElementById("videoChart");
  if (videoDom) {
    tongjiVideoChart = window.echarts.getInstanceByDom(videoDom) || window.echarts.init(videoDom, "dark");
    tongjiVideoChart.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis", ...tooltipStyle },
      grid: { left: "3%", right: "3%", bottom: "5%", top: "15%", containLabel: true },
      xAxis: { type: "category", data: TONGJI_SNAPSHOT.dates, axisLine: { lineStyle: { color: "#333" } }, axisTick: { show: false } },
      yAxis: [
        { type: "value", name: "播放量", splitLine: { lineStyle: { color: "#333", type: "dashed" } }, axisLabel: { color: "#c7c7c7" } },
        { type: "value", name: "总互动", splitLine: { show: false }, axisLabel: { color: "#c7c7c7" } },
      ],
      series: [
        {
          name: "播放量",
          type: "bar",
          barWidth: "40%",
          itemStyle: { color: "#00A3FF", borderRadius: [2, 2, 0, 0] },
          data: TONGJI_SNAPSHOT.videoPlays,
        },
        {
          name: "综合互动量",
          type: "line",
          yAxisIndex: 1,
          smooth: true,
          symbolSize: 7,
          itemStyle: { color: "#9C27B0" },
          lineStyle: { width: 3, shadowColor: "rgba(156, 39, 176, 0.5)", shadowBlur: 10 },
          data: TONGJI_SNAPSHOT.videoInteractions,
        },
      ],
    }, true);
  }

  if (!tongjiResizeBound) {
    window.addEventListener("resize", () => {
      tongjiFollowerChart?.resize();
      tongjiVideoChart?.resize();
    });
    tongjiResizeBound = true;
  }
}

function renderStats() {
  setStatsMode(state.statsMode || "capture");
  renderTongjiSection();
  renderStatsCapturePanel();

  const statsWindowNode = document.querySelector("#stats-weekly-window");
  if (statsWindowNode) {
    const windowInfo = state.weeklySummary?.window || {};
    const start = windowInfo.start_date || "";
    const end = windowInfo.end_date || "";
    statsWindowNode.textContent = start && end ? `近7天窗口 ${start} ~ ${end}` : "近7天汇总未加载";
  }

  const statsAccountFilter = document.querySelector("#stats-account-filter");
  if (statsAccountFilter) {
    const activeAccounts = statsWechatActiveAccounts();
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
  const accountHeaders = ["账号名称", "平台", "总播放", "周期播放", "粉丝", "增粉", "完播率", "互动率", "更新", "采集状态"];
  let sortIndex = 0;
  let sortDir = 1;
  const renderAccountTable = () => {
    const keyword = document.querySelector("#account-stats-search")?.value.trim().toLowerCase() || "";
    const filtered = accounts
      .map((row) => [row[0], row[1], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]])
      .filter((row) => row.join(" ").toLowerCase().includes(keyword))
      .sort((a, b) => String(a[sortIndex]).localeCompare(String(b[sortIndex]), "zh-Hans-CN", { numeric: true }) * sortDir);
    document.querySelector("#account-stats-table").innerHTML = `
      <table><thead><tr>${accountHeaders.map((header, index) => `<th><button type="button" data-account-sort="${index}">${header}</button></th>`).join("")}</tr></thead>
      <tbody>${filtered.map((row) => `<tr>${row.map((cell, index) => `<td>${index >= 9 && cell ? `<span class="chip">${cell}</span>` : cell || "-"}</td>`).join("")}</tr>`).join("")}</tbody></table>
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
  const statsSearchInput = document.querySelector("#account-stats-search");
  if (statsSearchInput) {
    statsSearchInput.oninput = () => {
      if (Array.isArray(state.weeklySummary?.rows) && state.weeklySummary.rows.length) {
        renderStatsWeeklySummary();
      } else {
        renderAccountTable();
      }
    };
  }
  if (statsAccountFilter) {
    statsAccountFilter.onchange = () => {
      if (Array.isArray(state.weeklySummary?.rows) && state.weeklySummary.rows.length) {
        renderStatsWeeklySummary();
      } else {
        renderAccountTable();
      }
    };
  }
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
  renderStatsWeeklySummary();
  renderNotificationSlaStats();
}

function renderStatsWeeklySummary() {
  const summary = state.weeklySummary || {};
  const rows = Array.isArray(summary.rows) ? summary.rows : [];
  const formatNumber = (value) => Number(value || 0).toLocaleString("zh-Hans-CN");
  const formatSigned = (value) => {
    const number = Number(value || 0);
    const prefix = number >= 0 ? "+" : "";
    return `${prefix}${formatNumber(number)}`;
  };
  const formatPercent = (value, digits = 1) => `${Number(value || 0).toFixed(digits)}%`;
  const parsePercent = (value) => {
    if (typeof value === "number") return value;
    const parsed = Number(String(value || "").replace("%", ""));
    return Number.isFinite(parsed) ? parsed : 0;
  };
  const average = (values) => {
    const nums = values.map((value) => Number(value || 0)).filter((value) => Number.isFinite(value));
    if (!nums.length) return 0;
    return nums.reduce((sum, value) => sum + value, 0) / nums.length;
  };
  const pct = (a, b) => {
    const total = Number(b || 0);
    if (!total) return "0.0%";
    return `${((Number(a || 0) / total) * 100).toFixed(1)}%`;
  };
  const statusCounts = summary.status_counts || {};
  const matrixDonutNode = document.querySelector("#matrix-status-donut");
  const matrixDonutLabel = document.querySelector("#matrix-status-donut-label");
  const matrixCollectionGridNode = document.querySelector("#matrix-collection-grid");
  const matrixStatusBandNode = document.querySelector("#matrix-status-band");
  const averageCompletedNode = document.querySelector("#matrix-average-completed-rate");
  const averageInteractionNode = document.querySelector("#matrix-average-interaction-rate");
  const abnormalRateNode = document.querySelector("#matrix-abnormal-rate");
  const analyticsRows = Array.isArray(state.analytics?.account_rank)
    ? state.analytics.account_rank.map((item) => item.row).filter(Boolean)
    : [];
  const totalAccounts = rows.length || Object.values(statusCounts).reduce((sum, value) => sum + Number(value || 0), 0) || analyticsRows.length;
  const completeCount = Number(statusCounts.完整 || 0);
  const abnormalCount = Math.max(0, totalAccounts - completeCount);
  const abnormalPercent = totalAccounts ? (abnormalCount / totalAccounts) * 100 : 0;
  const completedRate = average(analyticsRows.map((row) => parsePercent(row[7])));
  const interactionRate = average(analyticsRows.map((row) => parsePercent(row[8])));
  const totals = rows.reduce((acc, row) => {
    acc.followers += Number(row.followers_current || 0);
    acc.newFollowers += Number(row.new_followers_7d || 0);
    acc.unfollows += Number(row.unfollows_7d || 0);
    acc.views += Number(row.views_7d || 0);
    acc.likes += Number(row.likes_7d || 0);
    acc.comments += Number(row.comments_7d || 0);
    acc.shares += Number(row.shares_7d || 0);
    acc.traffic += Number(row.follower_delta_7d || 0);
    return acc;
  }, {
    followers: 0,
    newFollowers: 0,
    unfollows: 0,
    views: 0,
    likes: 0,
    comments: 0,
    shares: 0,
    traffic: 0,
  });
  const statusPalette = {
    完整: "#5dd62c",
    "近7天缺口": "#ffb02e",
    "缺关注者": "#28d7c4",
    "缺视频": "#4ca3ff",
    "缺关注者+缺视频": "#8f73ff",
    未登录: "#ff4d5f",
  };
  if (matrixDonutNode && totalAccounts > 0) {
    const order = ["完整", "近7天缺口", "缺关注者", "缺视频", "缺关注者+缺视频", "未登录"];
    const segments = order
      .map((key) => [key, Number(statusCounts[key] || 0)])
      .filter(([, count]) => count > 0);
    const fallbackSegments = segments.length ? segments : [["完整", totalAccounts]];
    const gradient = [];
    let offset = 0;
    for (const [key, count] of fallbackSegments) {
      const start = (offset / totalAccounts) * 100;
      offset += count;
      const end = (offset / totalAccounts) * 100;
      gradient.push(`${statusPalette[key] || "#8e8e8e"} ${start.toFixed(1)}% ${end.toFixed(1)}%`);
    }
    matrixDonutNode.style.background = `conic-gradient(${gradient.join(", ")})`;
    matrixDonutNode.style.boxShadow = "inset 0 0 0 32px #101010";
  }
  if (matrixDonutLabel) {
    matrixDonutLabel.innerHTML = `异常<br>${formatPercent(abnormalPercent)}`;
  }
  if (averageCompletedNode) averageCompletedNode.textContent = formatPercent(completedRate);
  if (averageInteractionNode) averageInteractionNode.textContent = formatPercent(interactionRate);
  if (abnormalRateNode) abnormalRateNode.textContent = formatPercent(abnormalPercent);
  if (matrixCollectionGridNode) {
    const cards = [
      {
        label: "关注者总数",
        value: formatNumber(totals.followers),
        note: `覆盖 ${formatNumber(totalAccounts)} 个账号`,
        accent: "green",
      },
      {
        label: "新增/取关",
        value: `${formatSigned(totals.newFollowers)} / ${totals.unfollows ? `-${formatNumber(totals.unfollows)}` : "0"}`,
        note: `净增 ${formatSigned(totals.traffic)}`,
        accent: "orange",
      },
      {
        label: "播放总量",
        value: formatNumber(totals.views),
        note: `账号均值 ${formatNumber(totalAccounts ? Math.round(totals.views / totalAccounts) : 0)}`,
        accent: "blue",
      },
      {
        label: "喜欢/推荐",
        value: `${formatNumber(totals.likes)} / ${formatNumber(totals.comments)}`,
        note: `互动基数 ${formatNumber(totals.likes + totals.comments)}`,
        accent: "violet",
      },
      {
        label: "分享/评论",
        value: `${formatNumber(totals.shares)} / ${formatNumber(totals.comments)}`,
        note: `传播强度 ${formatNumber(totals.shares + totals.comments)}`,
        accent: "cyan",
      },
      {
        label: "视频引流关注",
        value: formatNumber(totals.newFollowers),
        note: `近7天净增 ${formatSigned(totals.traffic)}`,
        accent: "amber",
      },
    ];
    matrixCollectionGridNode.innerHTML = cards.map((card) => `
      <article class="matrix-collection-card accent-${card.accent}">
        <span>${card.label}</span>
        <strong>${card.value}</strong>
        <em>${card.note}</em>
      </article>
    `).join("");
  }
  if (matrixStatusBandNode) {
    const bandOrder = ["完整", "近7天缺口", "缺关注者", "缺视频", "缺关注者+缺视频", "未登录"];
    matrixStatusBandNode.innerHTML = bandOrder.map((key) => {
      const count = Number(statusCounts[key] || 0);
      const share = totalAccounts ? ((count / totalAccounts) * 100).toFixed(1) : "0.0";
      const color = statusPalette[key] || "#8e8e8e";
      return `
        <article class="matrix-status-pill">
          <i style="--status-color:${color}"></i>
          <div>
            <strong>${key}</strong>
            <span>${count} 个 · ${share}%</span>
          </div>
        </article>
      `;
    }).join("");
  }
  if (!rows.length) return;
  const statsWindowNode = document.querySelector("#stats-weekly-window");
  if (statsWindowNode) {
    const windowInfo = summary.window || {};
    const start = windowInfo.start_date || "";
    const end = windowInfo.end_date || "";
    const statusText = Object.entries(statusCounts).map(([key, value]) => `${key}:${value}`).join(" · ");
    statsWindowNode.textContent = start && end
      ? `近7天窗口 ${start} ~ ${end}${statusText ? ` · ${statusText}` : ""}`
      : "近7天汇总未加载";
  }

  const accountFilter = document.querySelector("#stats-account-filter")?.value || "";
  const keyword = document.querySelector("#account-stats-search")?.value.trim().toLowerCase() || "";
  const tableRows = rows
    .filter((row) => !accountFilter || String(row.account_id || "") === String(accountFilter))
    .filter((row) => {
      const token = [row.display_name, row.account_key, row.capture_status, ...(row.missing_sources || [])].join(" ").toLowerCase();
      return token.includes(keyword);
    })
    .sort((a, b) => Number(b.views_7d || 0) - Number(a.views_7d || 0));
  const headers = ["账号名称", "平台", "关注者总数", "新增/取关", "播放总量", "喜欢/推荐", "分享/评论", "视频引流关注", "采集状态"];
  document.querySelector("#account-stats-table").innerHTML = `<table><thead><tr>${headers.map((header) => `<th>${header}</th>`).join("")}</tr></thead><tbody>${tableRows.map((row) => {
    const views = Number(row.views_7d || 0);
    const followers = Number(row.followers_current || 0);
    const newFollowers = Number(row.new_followers_7d || 0);
    const unfollows = Number(row.unfollows_7d || 0);
    const likes = Number(row.likes_7d || 0);
    const shares = Number(row.shares_7d || 0);
    const comments = Number(row.comments_7d || 0);
    const traffic = Math.max(0, newFollowers);
    const captureText = row.capture_status || "未知";
    const cells = [
      `${row.display_name || row.account_key || `账号 ${row.account_id}`}`,
      "视频号",
      formatNumber(followers),
      `${formatSigned(newFollowers)} / ${unfollows ? `-${formatNumber(unfollows)}` : "0"}`,
      formatNumber(views),
      `${formatNumber(likes)} / ${formatNumber(comments)}`,
      `${formatNumber(shares)} / ${formatNumber(comments)}`,
      formatNumber(traffic),
      captureText,
    ];
    return `<tr>${cells.map((cell, index) => `<td>${index >= 8 && cell ? `<span class="chip">${cell}</span>` : cell || "-"}</td>`).join("")}</tr>`;
  }).join("")}</tbody></table><div class="table-pager">第 1 / 1 页 · ${tableRows.length} 条账号</div>`;

  const alerts = Array.isArray(summary.alerts) ? summary.alerts : [];
  if (alerts.length) {
    document.querySelector("#risk-list").innerHTML = alerts.slice(0, 8).map((item) => {
      const name = item.display_name || item.account_key || `账号 ${item.account_id}`;
      const missing = Array.isArray(item.missing_sources) && item.missing_sources.length ? `，缺源 ${item.missing_sources.join("/")}` : "";
      return `<article>${name}：${item.capture_status}（覆盖 ${item.coverage_days || 0}/7${missing}）</article>`;
    }).join("");
  }
}

function renderAnalyticsFromDatabase() {
  const analytics = state.analytics || {};
  if (!Object.keys(analytics).length) return;
  const accounts = (analytics.account_rank || []).map((item) => item.row).filter(Boolean);
  const hasWeeklyRows = Array.isArray(state.weeklySummary?.rows) && state.weeklySummary.rows.length > 0;
  if (accounts.length && !hasWeeklyRows) {
    const headers = ["账号名称", "平台", "关注者总数", "新增/取关", "播放总量", "喜欢/推荐", "分享/评论", "视频引流关注", "采集状态"];
    document.querySelector("#account-stats-table").innerHTML = `<table><thead><tr>${headers.map((header) => `<th>${header}</th>`).join("")}</tr></thead><tbody>${accounts.map((row) => {
      const cells = [row[0], row[1], row[5], row[6], row[3], row[7], row[8], row[9], row[10]];
      return `<tr>${cells.map((cell, index) => `<td>${index >= 8 && cell ? `<span class="chip">${cell}</span>` : cell || "-"}</td>`).join("")}</tr>`;
    }).join("")}</tbody></table><div class="table-pager">1 / 1 · ${accounts.length} 条账号</div>`;
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

function renderNotificationSlaStats() {
  const sla = state.notificationSla || {};
  const cardNode = document.querySelector("#notification-sla-cards");
  const topNode = document.querySelector("#notification-sla-top");
  if (cardNode) {
    const cards = [
      ["未闭环", sla.open_count || 0],
      ["未确认", sla.unacknowledged_count || 0],
      ["平均确认", notificationDuration(sla.avg_ack_seconds || 0)],
      ["平均关闭", notificationDuration(sla.avg_resolve_seconds || 0)],
      ["升级次数", sla.escalation_count || 0],
      ["送达失败率", `${sla.delivery_failed_rate || 0}%`],
    ];
    cardNode.innerHTML = cards.map(([label, value]) => `<div><span>${label}</span><strong>${value}</strong></div>`).join("");
  }
  if (topNode) {
    const top = sla.top_events || [];
    topNode.innerHTML = top.length
      ? top.map((item) => `<article>${escapeHtml(item.label || item.event_type)} · ${Number(item.count || 0)} 次</article>`).join("")
      : `<article>暂无通知事件</article>`;
  }
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
      const result = await api("/api/system/cache/clear", { method: "POST" });
      if (stateNode) {
        const supabase = result.supabase_read_cache || {};
        if (supabase.cleared) {
          stateNode.textContent = "已清空进程内应用缓存，后续请求将重新读取。";
        } else {
          stateNode.textContent =
            result.backend === "sqlite"
              ? "当前本地 SQLite 模式无远端读缓存，已完成本地缓存刷新。"
              : "已执行缓存刷新。";
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
          stateNode.textContent = `已请求打开：${label}`;
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

function notificationEventLabel(eventType) {
  const token = String(eventType || "").trim();
  const matched = (state.notificationEvents || []).find((item) => item.event_type === token);
  return String(matched?.label || token || "通知");
}

function setNotificationRouteState(message, tone = "") {
  const node = document.querySelector("#notification-route-state");
  if (!node) return;
  node.textContent = String(message || "");
  node.classList.toggle("danger", tone === "danger");
}

function notificationMessagePayload(message) {
  const raw = message?.payload_json;
  if (raw && typeof raw === "object") return raw;
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw || "{}");
      if (parsed && typeof parsed === "object") return parsed;
    } catch (_error) {}
  }
  return {};
}

function notificationCardTone(status) {
  const token = String(status || "").trim().toLowerCase();
  if (token === "sent") return "success";
  if (token === "failed" || token === "retry") return "danger";
  if (token === "pending" || token === "sending" || token === "unsupported") return "warning";
  return "info";
}

function notificationSeverityTone(severity) {
  const token = String(severity || "").trim().toLowerCase();
  if (token === "critical" || token === "blocking" || token === "error") return "danger";
  if (token === "warning") return "warning";
  if (token === "resolved" || token === "sent") return "success";
  return "info";
}

function notificationIncidentStatusLabel(status) {
  return {
    open: "未确认",
    acknowledged: "已确认",
    assigned: "处理中",
    resolved: "已关闭",
    ignored: "已忽略",
  }[String(status || "").toLowerCase()] || status || "未知";
}

function notificationActorName() {
  const user = (authState.users || []).find((item) => item.id === authState.currentUserId);
  return user?.name || "Allen";
}

function notificationDuration(seconds) {
  const value = Math.max(0, Number(seconds || 0));
  if (value >= 86400) return `${Math.round(value / 86400)} 天`;
  if (value >= 3600) return `${Math.round(value / 3600)} 小时`;
  if (value >= 60) return `${Math.round(value / 60)} 分钟`;
  return `${Math.round(value)} 秒`;
}

function renderOperationNotifications() {
  const routeNode = document.querySelector("#operation-notice-routes");
  const policyNode = document.querySelector("#notification-policy-list");
  const incidentNode = document.querySelector("#notification-incident-list");
  const batchNode = document.querySelector("#login-qr-batches");
  const historyNode = document.querySelector("#notification-history");
  const unreadNode = document.querySelector("#notification-unread-count");
  const messageQueue = state.aiRobotMessages || [];
  const incidents = state.notificationIncidents || [];
  const pendingStatuses = new Set(["pending", "retry", "failed", "sending"]);
  const pendingMessages = messageQueue.filter((item) => pendingStatuses.has(String(item.status || "").toLowerCase())).length;
  const openIncidents = incidents.filter((item) => ["open", "acknowledged", "assigned"].includes(String(item.status || "").toLowerCase()));
  const unreadCount = pendingMessages + openIncidents.length + (state.loginQrBatches || []).length;
  if (unreadNode) unreadNode.textContent = `${unreadCount} 条待处理`;
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
            <strong>${escapeHtml(first.label || eventType)}</strong>
            <p>${escapeHtml(first.source || "")}</p>
            <p>${escapeHtml(eventType)}${subtypes ? ` · ${escapeHtml(subtypes)}` : ""}</p>
            <div class="inline-actions">${routeButtons}</div>
          </div>
          <time>${escapeHtml(severity)}</time>
        </article>
      `;
    }).join("");
  }
  if (policyNode) {
    const policies = (state.notificationPolicies || []).filter((item) => !item.platform && !item.account_scope);
    policyNode.innerHTML = policies.length ? policies.map((policy) => {
      const targets = new Set(policy.target_platforms || []);
      const escalationTargets = new Set(policy.escalation_platforms || []);
      const platformToggles = ["telegram", "dingtalk", "wecom"].map((platform) => `
        <label class="notification-check"><input type="checkbox" data-policy-target="${platform}" ${targets.has(platform) ? "checked" : ""}>${aiPlatformLabel(platform)}</label>
      `).join("");
      const escalationToggles = ["telegram", "dingtalk", "wecom"].map((platform) => `
        <label class="notification-check"><input type="checkbox" data-policy-escalation-target="${platform}" ${escalationTargets.has(platform) ? "checked" : ""}>${aiPlatformLabel(platform)}</label>
      `).join("");
      return `
        <article class="notification-card ${notificationSeverityTone(policy.severity)} notification-policy-card" data-policy-card="${escapeHtml(policy.event_type)}" data-policy-severity="${escapeHtml(policy.severity)}">
          <span class="notification-dot"></span>
          <div>
            <strong>${escapeHtml(policy.label || notificationEventLabel(policy.event_type))} · 策略</strong>
            <p>默认 ${escapeHtml(policy.severity)} · 空目标表示跟随上方路由开关。</p>
            <div class="notification-policy-grid">
              <label>启用<select data-policy-enabled><option value="true" ${policy.enabled ? "selected" : ""}>启用</option><option value="false" ${!policy.enabled ? "selected" : ""}>停用外发</option></select></label>
              <label>冷却秒数<input type="number" min="0" data-policy-cooldown value="${Number(policy.cooldown_seconds || 0)}"></label>
              <label>静默开始<input type="time" data-policy-quiet-start value="${escapeHtml(policy.quiet_start || "")}"></label>
              <label>静默结束<input type="time" data-policy-quiet-end value="${escapeHtml(policy.quiet_end || "")}"></label>
              <label>升级分钟<input type="number" min="0" data-policy-escalation-minutes value="${Number(policy.escalation_minutes || 0)}"></label>
              <label>负责人<input data-policy-owner value="${escapeHtml(policy.owner_hint || "")}" placeholder="可选"></label>
            </div>
            <div class="notification-check-row"><span>目标</span>${platformToggles}</div>
            <div class="notification-check-row"><span>升级</span>${escalationToggles}</div>
            <div class="inline-actions"><button class="btn btn-sm primary" type="button" data-save-notification-policy>保存策略</button></div>
          </div>
          <time>${escapeHtml(policy.event_type)}</time>
        </article>
      `;
    }).join("") : "";
  }
  if (incidentNode) {
    incidentNode.innerHTML = incidents.length ? incidents.slice(0, 12).map((incident) => {
      const status = String(incident.status || "open").toLowerCase();
      const tone = status === "resolved" || status === "ignored" ? "success" : notificationSeverityTone(incident.severity);
      const summary = String(incident.summary || incident.payload?.summary || "").trim() || "暂无摘要";
      const owner = incident.assigned_to || incident.owner_hint || "未指派";
      return `
        <article class="notification-card ${tone}">
          <span class="notification-dot"></span>
          <div>
            <strong>${escapeHtml(incident.title || notificationEventLabel(incident.event_type))}</strong>
            <p>${escapeHtml(summary)}</p>
            <p>${escapeHtml(notificationIncidentStatusLabel(status))} · ${escapeHtml(incident.event_type)} · ${escapeHtml(incident.platform || "通用")} · ${Number(incident.occurrence_count || 1)} 次 · ${escapeHtml(owner)}</p>
            <div class="inline-actions">
              <button class="btn btn-sm ghost" type="button" data-incident-action="ack" data-incident-id="${incident.id}" ${status !== "open" ? "disabled" : ""}>确认</button>
              <button class="btn btn-sm ghost" type="button" data-incident-action="assign" data-incident-id="${incident.id}" ${status === "resolved" || status === "ignored" ? "disabled" : ""}>指派给我</button>
              <button class="btn btn-sm primary" type="button" data-incident-action="resolve" data-incident-id="${incident.id}" ${status === "resolved" || status === "ignored" ? "disabled" : ""}>关闭</button>
              <button class="btn btn-sm ghost" type="button" data-incident-action="ignore" data-incident-id="${incident.id}" ${status === "resolved" || status === "ignored" ? "disabled" : ""}>忽略</button>
              <button class="btn btn-sm ghost" type="button" data-incident-action="resend" data-incident-id="${incident.id}">重发</button>
            </div>
          </div>
          <time>${formatTime(incident.updated_at || incident.last_seen_at || incident.created_at)}</time>
        </article>
      `;
    }).join("") : `
      <article class="notification-card success">
        <span class="notification-dot"></span>
        <div><strong>暂无待处理事件</strong><p>通知事件会在这里聚合、确认、指派和关闭。</p></div>
        <time>实时</time>
      </article>
    `;
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
            <strong>待扫码批次 ${escapeHtml(batch.batch_id)}</strong>
            <p>${escapeHtml(items.map((item) => `${item.display_name || item.account_key} / port ${item.debug_port}`).join("；") || "等待巡检结果")}</p>
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
  if (historyNode) {
    const items = (state.aiRobotMessages || []).slice(0, 8);
    historyNode.innerHTML = items.length ? items.map((item) => {
      const payload = notificationMessagePayload(item);
      const eventType = String(item.message_type || payload.message_type || "text").trim();
      const platform = String(item.platform || payload.platform || "").trim();
      const status = String(item.status || "").trim().toLowerCase();
      const cardTone = notificationCardTone(status);
      const statusLabel = {
        sent: "已发送",
        failed: "发送失败",
        retry: "重试中",
        pending: "待发送",
        sending: "发送中",
        unsupported: "未启用",
      }[status] || status || "未知";
      const rawText = String(payload.text || payload.summary || item.summary || "").trim() || "暂无正文";
      const content = rawText.length > 280 ? `${rawText.slice(0, 280)}...` : rawText;
      const title = `${notificationEventLabel(eventType)} · ${platform ? aiPlatformLabel(platform) : "通用"}`;
      return `
        <article class="notification-card ${cardTone}">
          <span class="notification-dot"></span>
          <div>
            <strong>${escapeHtml(title)}</strong>
            <p>${escapeHtml(content)}</p>
            <p>${escapeHtml(eventType)} · ${escapeHtml(statusLabel)}</p>
          </div>
          <time>${formatTime(item.updated_at || item.created_at)}</time>
        </article>
      `;
    }).join("") : `
      <article class="notification-card info">
        <span class="notification-dot"></span>
        <div><strong>暂无外发记录</strong><p>先配置机器人并触发一次测试或业务事件。</p></div>
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

const INTERACTION_TAB_META = {
  comment: { label: "评论", description: "公开视频评论自动回复" },
  private_message: { label: "私信", description: "私信自动回复" },
  barrage: { label: "弹幕", description: "暂不支持" },
};

function normalizeInteractionTab(tab) {
  return Object.prototype.hasOwnProperty.call(INTERACTION_TAB_META, tab) ? tab : "comment";
}

function syncInteractionNavState() {
  document.querySelectorAll("[data-interaction-tab]").forEach((button) => {
    const active = currentView === "interaction-management" && normalizeInteractionTab(button.dataset.interactionTab) === currentInteractionTab;
    button.classList.toggle("active", active);
  });
}

function setInteractionTab(tab) {
  currentInteractionTab = normalizeInteractionTab(tab);
  localStorage.setItem("gasgx-interaction-tab", currentInteractionTab);
  syncInteractionNavState();
  renderInteractionManagement();
}

function splitInteractionLines(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function joinInteractionLines(value) {
  if (Array.isArray(value)) return value.map((item) => String(item || "").trim()).filter(Boolean).join("\n");
  return String(value || "");
}

function interactionStatusText(status = {}) {
  const running = Boolean(status.running);
  if (running) return "运行中";
  const value = String(status.status || "").trim();
  if (value === "completed") return "已完成";
  if (value === "failed") return "失败";
  if (value === "idle") return "空闲";
  return value || "未就绪";
}

function interactionTaskSummary(kind, result = {}) {
  const label = kind === "private_message" ? "私信" : "评论";
  const ok = Boolean(result.ok);
  const selected = Number(result.conversations_selected || result.posts_selected || 0);
  const sent = Number(result.replies_sent || 0);
  const statePath = String(result.state_path || "").trim();
  return `${label}任务${ok ? "已完成" : "已结束"} · 选中 ${selected} 条 · 发送 ${sent} 条${statePath ? ` · ${statePath}` : ""}`;
}

function renderInteractionManagement() {
  const root = document.querySelector("#interaction-management-root");
  if (!root) return;
  const config = state.interactionManagement.config || {};
  const status = state.interactionManagement.status || {};
  const comment = config.comment_reply || {};
  const privateMessage = config.private_message_reply || {};
  const lastResult = status.last_result || {};
  const history = Array.isArray(lastResult.records) ? lastResult.records : [];
  const activeTab = normalizeInteractionTab(currentInteractionTab);
  const statusTone = status.running ? "warning" : (status.status === "failed" ? "danger" : "success");
  root.innerHTML = `
    <div class="interaction-management-layout">
      <section class="panel interaction-management-config-panel">
        <div class="panel-head">
          <div>
            <h2>互动自动回复</h2>
            <p class="muted">评论与私信走 GasGx 官方客服身份，私信提示语独立配置；弹幕先保留入口，不自动化。</p>
          </div>
          <span class="system-status ${statusTone}">${escapeHtml(interactionStatusText(status))}</span>
        </div>
        <div class="interaction-tab-switcher">
          ${Object.entries(INTERACTION_TAB_META).map(([tab, meta]) => `
            <button class="interaction-tab-btn ${activeTab === tab ? "active" : ""}" type="button" data-interaction-tab="${tab}">
              <strong>${escapeHtml(meta.label)}</strong>
              <span>${escapeHtml(meta.description)}</span>
            </button>
          `).join("")}
        </div>
        <div class="interaction-panel-stack">
          <section class="interaction-tab-panel ${activeTab === "comment" ? "active" : "hidden"}" data-interaction-panel="comment">
            <div class="interaction-panel-head">
              <h3>评论自动回复</h3>
              <button class="btn primary" type="button" data-interaction-comment-run>立即运行</button>
            </div>
            <div class="interaction-form-grid">
              <label class="interaction-toggle"><input type="checkbox" data-interaction-comment-enabled ${comment.enabled !== false ? "checked" : ""}>启用评论自动回复</label>
              <label>每次最多扫描视频数<input type="number" min="1" max="200" data-interaction-comment-max-posts value="${Number(comment.max_posts_per_run || 50)}"></label>
              <label>每次最多回复数<input type="number" min="1" max="100" data-interaction-comment-max-replies value="${Number(comment.max_replies_per_run || 20)}"></label>
              <label>回复最小字数<input type="number" min="5" max="80" data-interaction-comment-min-chars value="${Number(comment.reply_min_chars || 8)}"></label>
              <label>回复最大字数<input type="number" min="8" max="120" data-interaction-comment-max-chars value="${Number(comment.reply_max_chars || 40)}"></label>
              <label>最小回复间隔(秒)<input type="number" min="1" max="60" data-interaction-comment-min-interval value="${Number(comment.min_reply_interval_seconds || 1)}"></label>
              <label>最大回复间隔(秒)<input type="number" min="1" max="60" data-interaction-comment-max-interval value="${Number(comment.max_reply_interval_seconds || 5)}"></label>
              <label>动作随机延迟最小(秒)<input type="number" min="0" max="30" data-interaction-comment-action-min value="${Number(comment.min_action_delay_seconds ?? 1)}"></label>
              <label>动作随机延迟最大(秒)<input type="number" min="0" max="30" data-interaction-comment-action-max value="${Number(comment.max_action_delay_seconds ?? 5)}"></label>
              <label class="interaction-toggle"><input type="checkbox" data-interaction-comment-auto-like ${comment.auto_like !== false ? "checked" : ""}>自动点赞后再回复</label>
              <label class="interaction-wide-field">评论提示语<textarea rows="8" data-interaction-comment-prompt>${escapeHtml(joinInteractionLines(comment.prompt_template || ""))}</textarea></label>
              <label class="interaction-wide-field">评论兜底回复（每行一条）<textarea rows="4" data-interaction-comment-fallbacks>${escapeHtml(joinInteractionLines(comment.fallback_replies || []))}</textarea></label>
            </div>
          </section>
          <section class="interaction-tab-panel ${activeTab === "private_message" ? "active" : "hidden"}" data-interaction-panel="private_message">
            <div class="interaction-panel-head">
              <h3>私信自动回复</h3>
              <button class="btn primary" type="button" data-interaction-private-run>立即运行</button>
            </div>
            <p class="muted">运行时会依次处理“私信”和“打招呼消息”两个标签，并按配置的随机延迟执行点击、打开和发送动作。</p>
            <div class="interaction-form-grid">
              <label class="interaction-toggle"><input type="checkbox" data-interaction-private-enabled ${privateMessage.enabled !== false ? "checked" : ""}>启用私信自动回复</label>
              <label>每次最多扫描会话数<input type="number" min="1" max="200" data-interaction-private-max-conversations value="${Number(privateMessage.max_conversations_per_run || 20)}"></label>
              <label>每次最多回复数<input type="number" min="1" max="100" data-interaction-private-max-replies value="${Number(privateMessage.max_replies_per_run || 20)}"></label>
              <label>回复最小字数<input type="number" min="10" max="120" data-interaction-private-min-chars value="${Number(privateMessage.reply_min_chars || 30)}"></label>
              <label>回复最大字数<input type="number" min="20" max="200" data-interaction-private-max-chars value="${Number(privateMessage.reply_max_chars || 120)}"></label>
              <label>最小回复间隔(秒)<input type="number" min="1" max="60" data-interaction-private-min-interval value="${Number(privateMessage.min_reply_interval_seconds || 1)}"></label>
              <label>最大回复间隔(秒)<input type="number" min="1" max="60" data-interaction-private-max-interval value="${Number(privateMessage.max_reply_interval_seconds || 5)}"></label>
              <label>动作随机延迟最小(秒)<input type="number" min="0" max="30" data-interaction-private-action-min value="${Number(privateMessage.min_action_delay_seconds ?? 1)}"></label>
              <label>动作随机延迟最大(秒)<input type="number" min="0" max="30" data-interaction-private-action-max value="${Number(privateMessage.max_action_delay_seconds ?? 5)}"></label>
              <label class="interaction-wide-field">私信提示语<textarea rows="8" data-interaction-private-prompt>${escapeHtml(joinInteractionLines(privateMessage.prompt_template || ""))}</textarea></label>
              <label class="interaction-wide-field">私信兜底回复（每行一条）<textarea rows="4" data-interaction-private-fallbacks>${escapeHtml(joinInteractionLines(privateMessage.fallback_replies || []))}</textarea></label>
            </div>
          </section>
          <section class="interaction-tab-panel ${activeTab === "barrage" ? "active" : "hidden"}" data-interaction-panel="barrage">
            <div class="interaction-panel-head">
              <h3>弹幕</h3>
            </div>
            <div class="interaction-placeholder">
              <strong>暂不支持弹幕自动回复</strong>
              <p class="muted">先保留入口，后续如果视频号弹幕页稳定可用，再补接自动化。</p>
            </div>
          </section>
        </div>
        <div class="interaction-config-actions">
          <button class="btn secondary" type="button" data-interaction-refresh>刷新状态</button>
          <button class="btn primary" type="button" id="interaction-management-save">保存配置</button>
        </div>
      </section>
      <section class="panel interaction-management-status-panel">
        <div class="panel-head">
          <div>
            <h2>运行状态</h2>
            <p class="muted">后台线程会自动执行当前任务，页面可先离开，稍后再回来查看结果。</p>
          </div>
          <span class="system-status ${statusTone}">${escapeHtml(status.message || status.error || "待命")}</span>
        </div>
        <div class="interaction-status-grid">
          <div class="metric"><span>当前任务</span><strong>${escapeHtml(status.kind || "无")}</strong></div>
          <div class="metric"><span>开始时间</span><strong>${escapeHtml(status.started_at || "-")}</strong></div>
          <div class="metric"><span>结束时间</span><strong>${escapeHtml(status.finished_at || "-")}</strong></div>
          <div class="metric"><span>失败信息</span><strong>${escapeHtml(status.error || "-")}</strong></div>
        </div>
        <div class="interaction-history">
          <div class="panel-head compact">
            <h3>最近结果</h3>
            <span class="system-status">${escapeHtml(interactionTaskSummary(status.kind, lastResult))}</span>
          </div>
          ${history.length ? history.map((item) => `
            <article class="interaction-history-row">
              <div class="row-head">
                <strong>${escapeHtml(item.conversation_name || item.comment_author || item.post_title || "记录")}</strong>
                <span class="chip">${escapeHtml(item.replied_at || "-")}</span>
              </div>
              <div class="muted">${escapeHtml(item.message_preview || item.comment_preview || "")}</div>
              <div class="interaction-history-reply">${escapeHtml(item.reply_text || "")}</div>
            </article>
          `).join("") : `<div class="muted">暂无回复记录</div>`}
        </div>
      </section>
    </div>
  `;

  root.querySelectorAll("[data-interaction-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      setInteractionTab(button.dataset.interactionTab);
      if (currentView !== "interaction-management") {
        activateView("interaction-management");
      } else {
        renderInteractionManagement();
      }
    });
  });

  root.querySelector("#interaction-management-save")?.addEventListener("click", async (event) => {
    await saveInteractionManagementConfig(event.currentTarget);
  });

  root.querySelector("[data-interaction-refresh]")?.addEventListener("click", async (event) => {
    const restoreButton = setButtonLoading(event.currentTarget, "刷新中");
    try {
      await refreshInteractionManagement();
    } finally {
      restoreButton();
    }
  });

  root.querySelector("[data-interaction-comment-run]")?.addEventListener("click", async (event) => {
    await runInteractionCommentReply(event.currentTarget);
  });

  root.querySelector("[data-interaction-private-run]")?.addEventListener("click", async (event) => {
    await runInteractionPrivateMessageReply(event.currentTarget);
  });
}

function collectInteractionManagementConfig() {
  const root = document.querySelector("#interaction-management-root");
  if (!root) return null;
  const existing = state.interactionManagement.config || {};
  const readNumber = (selector, fallback) => {
    const value = Number(root.querySelector(selector)?.value || fallback || 0);
    return Number.isFinite(value) ? value : Number(fallback || 0);
  };
  const readBool = (selector, fallback) => {
    const node = root.querySelector(selector);
    return node ? Boolean(node.checked) : Boolean(fallback);
  };
  const commentPrompt = root.querySelector("[data-interaction-comment-prompt]")?.value || "";
  const commentFallbacks = root.querySelector("[data-interaction-comment-fallbacks]")?.value || "";
  const privatePrompt = root.querySelector("[data-interaction-private-prompt]")?.value || "";
  const privateFallbacks = root.querySelector("[data-interaction-private-fallbacks]")?.value || "";
  return {
    comment_reply: {
      ...existing.comment_reply,
      enabled: readBool("[data-interaction-comment-enabled]", true),
      max_posts_per_run: readNumber("[data-interaction-comment-max-posts]", existing.comment_reply?.max_posts_per_run || 50),
      max_replies_per_run: readNumber("[data-interaction-comment-max-replies]", existing.comment_reply?.max_replies_per_run || 20),
      reply_min_chars: readNumber("[data-interaction-comment-min-chars]", existing.comment_reply?.reply_min_chars || 8),
      reply_max_chars: readNumber("[data-interaction-comment-max-chars]", existing.comment_reply?.reply_max_chars || 40),
      min_reply_interval_seconds: readNumber("[data-interaction-comment-min-interval]", existing.comment_reply?.min_reply_interval_seconds || 1),
      max_reply_interval_seconds: readNumber("[data-interaction-comment-max-interval]", existing.comment_reply?.max_reply_interval_seconds || 5),
      min_action_delay_seconds: readNumber("[data-interaction-comment-action-min]", existing.comment_reply?.min_action_delay_seconds || 1),
      max_action_delay_seconds: readNumber("[data-interaction-comment-action-max]", existing.comment_reply?.max_action_delay_seconds || 5),
      auto_like: readBool("[data-interaction-comment-auto-like]", true),
      prompt_template: commentPrompt,
      fallback_replies: splitInteractionLines(commentFallbacks),
    },
    private_message_reply: {
      ...existing.private_message_reply,
      enabled: readBool("[data-interaction-private-enabled]", true),
      max_conversations_per_run: readNumber("[data-interaction-private-max-conversations]", existing.private_message_reply?.max_conversations_per_run || 20),
      max_replies_per_run: readNumber("[data-interaction-private-max-replies]", existing.private_message_reply?.max_replies_per_run || 20),
      reply_min_chars: readNumber("[data-interaction-private-min-chars]", existing.private_message_reply?.reply_min_chars || 30),
      reply_max_chars: readNumber("[data-interaction-private-max-chars]", existing.private_message_reply?.reply_max_chars || 120),
      min_reply_interval_seconds: readNumber("[data-interaction-private-min-interval]", existing.private_message_reply?.min_reply_interval_seconds || 1),
      max_reply_interval_seconds: readNumber("[data-interaction-private-max-interval]", existing.private_message_reply?.max_reply_interval_seconds || 5),
      min_action_delay_seconds: readNumber("[data-interaction-private-action-min]", existing.private_message_reply?.min_action_delay_seconds || 1),
      max_action_delay_seconds: readNumber("[data-interaction-private-action-max]", existing.private_message_reply?.max_action_delay_seconds || 5),
      prompt_template: privatePrompt,
      fallback_replies: splitInteractionLines(privateFallbacks),
    },
    spark_ai: existing.spark_ai || {},
    chrome: existing.chrome || {},
  };
}

async function saveInteractionManagementConfig(button = null) {
  const restoreButton = setButtonLoading(button || document.querySelector("#interaction-management-save"), "保存中");
  try {
    const payload = collectInteractionManagementConfig();
    if (!payload) throw new Error("互动管理配置面板不存在");
    state.interactionManagement.config = await api("/api/interaction-management/config", {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    state.interactionManagement.status = await api("/api/interaction-management/status");
    renderInteractionManagement();
  } catch (error) {
    window.alert(`保存互动管理配置失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
  } finally {
    restoreButton();
  }
}

async function refreshInteractionManagement() {
  try {
    state.interactionManagement.config = await api("/api/interaction-management/config");
    state.interactionManagement.status = await api("/api/interaction-management/status");
    if (currentView === "interaction-management") renderInteractionManagement();
  } catch (error) {
    window.alert(`刷新互动管理状态失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
  }
}

async function runInteractionCommentReply(button = null) {
  const restoreButton = setButtonLoading(button || document.querySelector("[data-interaction-comment-run]"), "运行中");
  try {
    state.interactionManagement.status = await api("/api/interaction-management/comment/run", { method: "POST" });
    renderInteractionManagement();
  } catch (error) {
    window.alert(`启动评论自动回复失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
  } finally {
    restoreButton();
  }
}

async function runInteractionPrivateMessageReply(button = null) {
  const restoreButton = setButtonLoading(button || document.querySelector("[data-interaction-private-run]"), "运行中");
  try {
    state.interactionManagement.status = await api("/api/interaction-management/private-msg/run", { method: "POST" });
    renderInteractionManagement();
  } catch (error) {
    window.alert(`启动私信自动回复失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
  } finally {
    restoreButton();
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

function formatSyncTime(value) {
  const raw = Number(value || 0);
  if (!raw) return "暂无成功同步";
  return new Date(raw * 1000).toLocaleString();
}

function renderSyncStatus() {
  const status = state.syncStatus || {};
  const badge = document.querySelector("#sync-status-badge");
  const localBackend = document.querySelector("#sync-local-backend");
  const pending = document.querySelector("#sync-pending-count");
  const failed = document.querySelector("#sync-failed-count");
  const actionState = document.querySelector("#sync-action-state");
  if (badge) {
    const failedCount = Number(status.failed || 0);
    const pendingCount = Number(status.pending || 0);
    badge.textContent = failedCount ? "待重试" : pendingCount ? "待同步" : "已就绪";
    badge.classList.toggle("danger", failedCount > 0);
  }
  if (localBackend) localBackend.textContent = String(status.backend || "sqlite").toUpperCase();
  if (pending) pending.textContent = `${Number(status.pending || 0)} 条`;
  if (failed) failed.textContent = `${Number(status.failed || 0)} / ${Number(status.conflicts || 0)}`;
  if (actionState) {
    const cloud = status.supabase_configured ? "☁️云端数据库已配置" : "☁️云端数据库未配置";
    const last = formatSyncTime(status.last_synced_at);
    const error = status.last_error ? `；最近错误：${displayDatabaseKeyword(status.last_error)}` : "";
    actionState.textContent = `${cloud}；最近成功：${last}${error}`;
    actionState.classList.toggle("danger", Number(status.failed || 0) > 0);
  }
}

async function loadSyncStatus() {
  state.syncStatus = await api("/api/sync/status");
  renderSyncStatus();
  return state.syncStatus;
}

function initSyncActions() {
  const stateNode = document.querySelector("#sync-action-state");
  const pullButton = document.querySelector("#sync-pull-supabase");
  const pushButton = document.querySelector("#sync-push-supabase");
  const retryButton = document.querySelector("#sync-retry-failed");
  pullButton?.addEventListener("click", async () => {
    const restoreButton = setButtonLoading(pullButton, "导入中...");
    try {
      const result = await api("/api/sync/supabase/pull", { method: "POST", timeoutMs: 45000 });
      state.syncStatus = result.status || await api("/api/sync/status");
      renderSyncStatus();
      loadedViews.delete("accounts");
      if (currentView === "accounts") {
        await loadViewData("accounts", { force: true });
      }
      if (stateNode) {
        stateNode.textContent = `已导入账号 ${result.accounts || 0} 个，平台 ${result.platforms || 0} 条，浏览器配置 ${result.profiles || 0} 条。`;
        stateNode.classList.remove("danger");
      }
    } catch (error) {
      if (stateNode) {
        stateNode.textContent = `导入失败：${displayDatabaseKeyword(error.message)}`;
        stateNode.classList.add("danger");
      }
      throw error;
    } finally {
      restoreButton();
    }
  });
  pushButton?.addEventListener("click", async () => {
    const restoreButton = setButtonLoading(pushButton, "推送中...");
    try {
      const result = await api("/api/sync/supabase/push", { method: "POST", timeoutMs: 60000 });
      state.syncStatus = result.status || await api("/api/sync/status");
      renderSyncStatus();
      if (stateNode) stateNode.textContent = `推送完成：成功 ${result.pushed || 0}，失败 ${result.failed || 0}。`;
    } catch (error) {
      if (stateNode) {
        stateNode.textContent = `推送失败：${displayDatabaseKeyword(error.message)}`;
        stateNode.classList.add("danger");
      }
      throw error;
    } finally {
      restoreButton();
    }
  });
  retryButton?.addEventListener("click", async () => {
    const restoreButton = setButtonLoading(retryButton, "重试中...");
    try {
      const result = await api("/api/sync/retry", { method: "POST", timeoutMs: 30000 });
      state.syncStatus = result.status || await api("/api/sync/status");
      renderSyncStatus();
      if (stateNode) stateNode.textContent = `已重新排队 ${result.retried || 0} 条失败同步。`;
    } finally {
      restoreButton();
    }
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
    if (state.terminalExecution.login_started) {
      const summary = state.terminalExecution.summary || {};
      return summary.success === summary.total && summary.total ? "已登录" : "处理中";
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
  const showTerminalLoading = view === "terminal-execution" && terminalCurrentRoute() !== "hub";
  if (showTerminalLoading) {
    setTerminalFullLoading(true, "终端执行页面加载中，请稍候...");
  }
  if (view !== "video-matrix") {
    unmountVideoMatrixWorkbench();
  }
  try {
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
        loading: terminalCurrentRoute() !== "hub",
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
        await loadPlatforms();
        const terminalState = await api("/api/terminal-execution/state");
        if (currentView === "terminal-execution") {
          state.terminalExecution = { ...terminalState, loading: false };
          // Page entry is passive; browser opening only happens after an explicit operator action.
          renderTerminalExecution();
        }
        loadAccounts()
          .then(() => {
            if (currentView === "terminal-execution") {
              renderTerminalExecution();
            }
          })
          .catch(() => {
            // Keep terminal first-screen fast even if account list refresh fails.
          });
      } catch (error) {
        state.terminalExecution = {
          ...(state.terminalExecution || {}),
          loading: false,
          error: error.message || "加载终端执行数据失败",
        };
        renderTerminalExecution();
      }
    } else if (view === "stats") {
      await loadAccounts();
      state.summary = await api("/api/summary");
      state.stats = await api("/api/stats");
      state.analytics = await api("/api/stats/analytics");
      state.weeklySummary = await api("/api/stats/weekly-summary?platform=wechat");
      state.notificationSla = await api("/api/stats/notification-sla");
      state.statsCaptureStatus = await api("/api/jobs/matrix-wechat/stats-capture/status");
      renderStats();
    } else if (view === "ai-robot") {
      state.aiRobotConfigs = await api("/api/ai-robots/configs");
      state.aiRobotMessages = await api("/api/ai-robots/messages");
      renderAiRobot();
    } else if (view === "interaction-management") {
      state.interactionManagement.config = await api("/api/interaction-management/config");
      state.interactionManagement.status = await api("/api/interaction-management/status");
      renderInteractionManagement();
    } else if (view === "notifications") {
      state.notificationEvents = await api("/api/notification-events");
      state.notificationRoutes = await api("/api/notification-routes");
      state.notificationPolicies = await api("/api/notification-policies");
      state.notificationIncidents = await api("/api/notification-incidents");
      state.notificationSla = await api("/api/stats/notification-sla");
      state.loginQrBatches = await api("/api/login-qr-batches");
      state.aiRobotMessages = await api("/api/ai-robots/messages");
      renderOperationNotifications();
    } else if (view === "system-settings") {
      await loadSyncStatus();
      state.databaseDictionary = await api("/api/system/database-dictionary");
      renderDatabaseDictionary();
    } else if (view === "video-matrix") {
      mountVideoMatrixWorkbench();
    }
    loadedViews.add(view);
  } finally {
    if (showTerminalLoading) {
      setTerminalFullLoading(false);
    }
    setWorkspaceLoading(false);
  }
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
  syncWechatInheritModeInputs(form);
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
  const caption = escapeHtml(captionInherit ? (common.wechat_caption || "") : (value.caption || common.wechat_caption || ""));
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
  const existingPlatforms = state.distributionSettings?.platforms || {};
  const platforms = Object.fromEntries(
    Object.entries(existingPlatforms).map(([platform, value]) => [platform, { ...(value || {}) }])
  );
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
  if (view === "terminal-execution" && updateHash) {
    state.terminalRoute = "hub";
  }
  if (view !== "terminal-execution") {
    state.terminalConfigOpen = false;
    document.querySelector("#terminal-init-modal")?.classList.add("hidden");
    document.querySelector("#terminal-platform-config-panel")?.classList.add("hidden");
    document.querySelector("#terminal-full-loading")?.classList.add("hidden");
  }
  const button = document.querySelector(`.nav-btn[data-view="${view}"]`);
  const section = document.querySelector(`#${view}`);
  if (!button || !section) return;
  document.querySelectorAll(".nav-btn").forEach((item) => item.classList.remove("active"));
  document.querySelectorAll(".view").forEach((item) => item.classList.remove("active"));
  button.classList.add("active");
  section.classList.add("active");
  currentView = view;
  syncInteractionNavState();
  document.body.classList.toggle("terminal-execution-active", view === "terminal-execution");
  document.body.classList.toggle("video-matrix-active", view === "video-matrix");
  document.body.classList.remove("mobile-nav-open");
  document.querySelector("#mobile-nav-toggle")?.setAttribute("aria-expanded", "false");
  setViewHeader(view);
  if (view !== "help-center" || !window.location.pathname.startsWith("/help-center/")) {
    setHelpCenterStandaloneMode(false);
  }
  applyPermissionLimitedState();
  if (view === "video-matrix") {
    setWorkspaceLoading(true, workspaceLoadingTitle(view), "正在加载视频生成工作台。");
    mountVideoMatrixWorkbench();
  } else {
    if (view === "terminal-execution" && terminalCurrentRoute() === "hub" && !loadedViews.has(view)) {
      setWorkspaceLoading(true, workspaceLoadingTitle(view), "正在同步右侧面板数据。");
    }
    const forceReload = view === "settings";
    loadViewData(view, { force: forceReload }).catch((error) => {
      const target = section.querySelector(".loading-inline") || section;
      target.innerHTML = `<div class="muted">加载失败：${escapeHtml(formatFriendlyMessage(error.message))}</div>`;
    });
  }
  if (updateHash && window.location.hash !== `#${view}`) {
    const hash = view === "terminal-execution" ? "#terminal-execution" : `#${view}`;
    const pathPrefix = window.location.pathname.startsWith("/help-center/") && view !== "help-center" ? "/" : window.location.pathname;
    window.history.replaceState(null, "", `${pathPrefix}${hash}`);
  }
  window.scrollTo({ top: 0, left: 0 });
}

document.querySelectorAll(".nav-btn").forEach((button) => {
  button.addEventListener("click", () => {
    if (button.dataset.view === "terminal-execution" && currentView === "terminal-execution") {
      terminalSetRoute("hub");
      document.body.classList.remove("mobile-nav-open");
      document.querySelector("#mobile-nav-toggle")?.setAttribute("aria-expanded", "false");
      return;
    }
    activateView(button.dataset.view);
  });
});

document.querySelectorAll("[data-interaction-tab]").forEach((button) => {
  button.addEventListener("click", () => {
    setInteractionTab(button.dataset.interactionTab);
    if (currentView !== "interaction-management") {
      activateView("interaction-management");
    } else {
      renderInteractionManagement();
    }
  });
});

document.querySelector("#interaction-management-save")?.addEventListener("click", async (event) => {
  await saveInteractionManagementConfig(event.currentTarget);
});

document.querySelector("[data-interaction-refresh]")?.addEventListener("click", async (event) => {
  const restoreButton = setButtonLoading(event.currentTarget, "刷新中");
  try {
    await refreshInteractionManagement();
  } finally {
    restoreButton();
  }
});

document.querySelector("[data-interaction-comment-run]")?.addEventListener("click", async (event) => {
  await runInteractionCommentReply(event.currentTarget);
});

document.querySelector("[data-interaction-private-run]")?.addEventListener("click", async (event) => {
  await runInteractionPrivateMessageReply(event.currentTarget);
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
  try {
    const data = Object.fromEntries(new FormData(event.target).entries());
    const brandPrefix = String(data.brand_prefix || "").trim();
    const accountName = String(data.account_name || "").trim();
    const operatorWechat = String(data.operator_wechat || "").trim();
    const phone = String(data.phone || "").trim();
    updateAccountPhoneHint();
    if (!operatorWechat || operatorWechat === "__new__") {
      showAccountCreateErrorToast("请先在下拉中新增运营微信号");
      return;
    }
    if (!/^\d{11}$/.test(phone)) {
      const phoneInput = event.target.elements.phone;
      phoneInput?.setCustomValidity("账号手机号需为 11 位数字");
      phoneInput?.reportValidity();
      phoneInput?.setCustomValidity("");
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
    const created = await api("/api/accounts", { method: "POST", body: JSON.stringify(data) });
    event.target.reset();
    setOperatorWechatValue("aamecc");
    showAccountCreatedToast(created);
    refresh().catch((error) => {
      showAccountCreateErrorToast(formatFriendlyMessage(error.message));
    });
  } catch (error) {
    showAccountCreateErrorToast(formatFriendlyMessage(error.message));
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
document.querySelector("#account-search-input")?.addEventListener("input", renderAccounts);
document.querySelector("#accounts-repair-config")?.addEventListener("click", (event) => {
  repairAccountConfigs(event.currentTarget);
});
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
    window.alert(`更新平台配置失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
  } finally {
    restoreButton();
  }
});

document.querySelector("#terminal-start-login-legacy")?.addEventListener("click", async (event) => {
  try {
    await startTerminalWechatLoginWithLoading(event.currentTarget);
  } catch (error) {
    window.alert(`启动登录浏览器失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
  }
});

document.querySelector("#terminal-edit-config-legacy")?.addEventListener("click", () => {
  state.terminalConfigOpen = true;
  renderTerminalExecution();
});

const distributionSettingsForm = document.querySelector("#distribution-settings-form");
let distributionSettingsSaving = false;

async function saveDistributionSettingsForm(form, submitter = null) {
  if (!form || distributionSettingsSaving) return;
  distributionSettingsSaving = true;
  const restoreButton = setButtonLoading(submitter || form.querySelector('button[type="submit"]'), "保存中");
  const stateNode = document.querySelector("#settings-save-state");
  if (stateNode) stateNode.textContent = "保存中...";
  try {
    await api("/api/settings/distribution", {
      method: "PATCH",
      body: JSON.stringify(collectDistributionSettings(form)),
    });
    if (stateNode) stateNode.textContent = "已保存，下一次矩阵分发会按公共配置执行。";
    await refresh();
  } finally {
    distributionSettingsSaving = false;
    restoreButton();
  }
}

distributionSettingsForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  await saveDistributionSettingsForm(event.target, event.submitter || null);
});

distributionSettingsForm?.querySelectorAll('button[type="submit"]').forEach((button) => {
  button.addEventListener("click", async (event) => {
    event.preventDefault();
    await saveDistributionSettingsForm(distributionSettingsForm, button);
  });
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
  if (!loadedViews.has("interaction-management")) return;
  api("/api/interaction-management/status")
    .then((interactionStatus) => {
      state.interactionManagement.status = interactionStatus;
      if (currentView === "interaction-management") renderInteractionManagement();
    })
    .catch(() => {});
}, 10000);

setInterval(() => {
  if (!loadedViews.has("stats")) return;
  api("/api/stats/weekly-summary?platform=wechat")
    .then((weeklySummary) => {
      state.weeklySummary = weeklySummary;
      if (currentView === "stats" && state.statsMode === "dashboard") renderStats();
    })
    .catch(() => {});
}, 15000);

document.querySelector("#stats-mode-dashboard")?.addEventListener("click", () => {
  setStatsMode("dashboard");
  renderStats();
});

document.querySelector("#stats-mode-capture")?.addEventListener("click", () => {
  setStatsMode("capture");
  renderStats();
});

async function refreshStatsCaptureStatus({ refreshWeekly = true } = {}) {
  state.statsCaptureStatus = await api("/api/jobs/matrix-wechat/stats-capture/status");
  if (refreshWeekly) {
    try {
      state.weeklySummary = await api("/api/stats/weekly-summary?platform=wechat");
    } catch (error) {
      void error;
    }
  }
  renderStatsCapturePanel();
  return state.statsCaptureStatus;
}

function latestStatsCaptureResultForAccount(accountId) {
  const currentId = Number(accountId || 0);
  if (!currentId) return null;
  const statusPayload = state.statsCaptureStatus?.status || {};
  const results = Array.isArray(statusPayload.results) ? statusPayload.results : [];
  return results.find((item) => Number(item?.account_id || 0) === currentId) || null;
}

function resolveStatsCaptureTip(accountId) {
  const currentId = Number(accountId || 0);
  const statusPayload = state.statsCaptureStatus?.status || {};
  const lock = state.statsCaptureStatus?.lock || {};
  const runningStatus = String(statusPayload.status || "").toLowerCase();
  if (lock.pid || runningStatus === "running" || runningStatus === "planned") {
    return currentId
      ? `账号 #${currentId} 采集中，请稍后点击“刷新状态”查看结果。`
      : "采集中，请稍后点击“刷新状态”查看结果。";
  }
  const latest = latestStatsCaptureResultForAccount(currentId);
  if (!latest) return "状态已刷新。";
  const reason = String(latest.reason || "").toLowerCase();
  if (latest.status === "captured") return `账号 #${currentId} 采集完成并已入库。`;
  if (reason === "no_data") return `账号 #${currentId} 下载成功，但当前近7天导出无数据行。`;
  if (reason === "login_required") return `账号 #${currentId} 未登录，请先点击“登录该账号”完成登录。`;
  if (reason === "account_mismatch") return `账号 #${currentId} 账号不匹配，请确认当前登录账号后重试。`;
  if (latest.status === "failed") return `账号 #${currentId} 采集失败：${formatFriendlyMessage(latest.error || "未知异常")}`;
  return "状态已刷新。";
}

document.querySelector("#stats-temp-capture-open-login")?.addEventListener("click", async (event) => {
  const accountId = Number(document.querySelector("#stats-capture-account-filter")?.value || 0);
  if (!accountId) {
    window.alert("请先选择要登录的账号");
    return;
  }
  const button = event.currentTarget;
  const restoreButton = setButtonLoading(button, "打开中");
  const tipNode = document.querySelector("#stats-capture-tip");
  try {
    await api(`/api/accounts/${accountId}/platforms/wechat/open-browser`, { method: "POST" });
    if (tipNode) tipNode.textContent = `账号 #${accountId} 浏览器已打开，请在弹出窗口完成微信视频号助手登录。`;
  } catch (error) {
    if (tipNode) tipNode.textContent = `打开登录窗口失败：${formatFriendlyMessage(error?.message || "未知异常")}`;
  } finally {
    restoreButton();
  }
});

document.querySelector("#stats-temp-capture-run-now")?.addEventListener("click", async (event) => {
  const accountId = Number(document.querySelector("#stats-capture-account-filter")?.value || 0);
  if (!accountId) {
    window.alert("请先选择要采集的账号");
    return;
  }
  const button = event.currentTarget;
  const restoreButton = setButtonLoading(button, "采集中");
  const tipNode = document.querySelector("#stats-capture-tip");
  try {
    if (tipNode) tipNode.textContent = `账号 #${accountId} 采集任务已提交，正在后台执行。`;
    const started = await api("/api/jobs/matrix-wechat/stats-capture/run-now", {
      method: "POST",
      body: JSON.stringify({
        target_date: "",
        dry_run: false,
        account_id: accountId,
        keep_browser_open_on_login_required: true,
        auto_open_browser: false,
      }),
    });
    if (String(started?.status || "") === "already_running" || started?.ok === false) {
      if (tipNode) tipNode.textContent = "已有采集在运行，请稍后再试。";
      await refreshStatsCaptureStatus();
      return;
    }
    if (tipNode) tipNode.textContent = `账号 #${accountId} 采集已触发。请等待片刻后点击“刷新状态”查看结果。`;
    await refreshStatsCaptureStatus();
    if (tipNode) tipNode.textContent = resolveStatsCaptureTip(accountId);
  } catch (error) {
    if (tipNode) tipNode.textContent = `采集失败：${formatFriendlyMessage(error?.message || "未知异常")}`;
  } finally {
    restoreButton();
  }
});

document.querySelector("#stats-temp-capture-refresh-status")?.addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const restoreButton = setButtonLoading(button, "刷新中");
  const tipNode = document.querySelector("#stats-capture-tip");
  try {
    await refreshStatsCaptureStatus();
    const accountId = Number(document.querySelector("#stats-capture-account-filter")?.value || 0);
    if (tipNode) tipNode.textContent = resolveStatsCaptureTip(accountId);
    renderStats();
  } catch (error) {
    if (tipNode) tipNode.textContent = `刷新状态失败：${formatFriendlyMessage(error?.message || "未知异常")}`;
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
      const result = await api(`/api/notification-routes/${eventType}/${platform}`, {
        method: "POST",
        body: JSON.stringify({ enabled, send_probe: enabled }),
      });
      state.notificationRoutes = await api("/api/notification-routes");
      state.aiRobotMessages = await api("/api/ai-robots/messages");
      if (!enabled) {
        setNotificationRouteState(`${notificationEventLabel(eventType)} · ${aiPlatformLabel(platform)} 已关闭`);
      } else {
        const probe = result?.probe || {};
        const probeStatus = String(probe.status || "").toLowerCase();
        if (probeStatus === "sent") {
          setNotificationRouteState(`${notificationEventLabel(eventType)} · ${aiPlatformLabel(platform)} 联调通知已发送`);
        } else {
          const detail = String(probe.error || probe.summary || probe.status || "未发送").trim();
          setNotificationRouteState(`${notificationEventLabel(eventType)} · ${aiPlatformLabel(platform)} 联调失败：${detail}`, "danger");
        }
      }
      renderOperationNotifications();
    } finally {
      restoreButton();
    }
    return;
  }

  const policyButton = event.target.closest("[data-save-notification-policy]");
  if (policyButton) {
    const card = policyButton.closest("[data-policy-card]");
    if (!card) return;
    const policy = {
      event_type: card.dataset.policyCard || "",
      severity: card.dataset.policySeverity || "",
      enabled: card.querySelector("[data-policy-enabled]")?.value === "true",
      cooldown_seconds: Number(card.querySelector("[data-policy-cooldown]")?.value || 0),
      quiet_start: card.querySelector("[data-policy-quiet-start]")?.value || "",
      quiet_end: card.querySelector("[data-policy-quiet-end]")?.value || "",
      escalation_enabled: Number(card.querySelector("[data-policy-escalation-minutes]")?.value || 0) > 0,
      escalation_minutes: Number(card.querySelector("[data-policy-escalation-minutes]")?.value || 0),
      owner_hint: card.querySelector("[data-policy-owner]")?.value || "",
      target_platforms: [...card.querySelectorAll("[data-policy-target]:checked")].map((input) => input.dataset.policyTarget),
      escalation_platforms: [...card.querySelectorAll("[data-policy-escalation-target]:checked")].map((input) => input.dataset.policyEscalationTarget),
    };
    const restoreButton = setButtonLoading(policyButton, "保存中");
    try {
      state.notificationPolicies = await api("/api/notification-policies", {
        method: "PUT",
        body: JSON.stringify({ policies: [policy] }),
      });
      setNotificationRouteState(`${notificationEventLabel(policy.event_type)} 策略已保存`);
      renderOperationNotifications();
    } finally {
      restoreButton();
    }
    return;
  }

  const incidentButton = event.target.closest("[data-incident-action]");
  if (incidentButton) {
    const incidentId = incidentButton.dataset.incidentId;
    const action = incidentButton.dataset.incidentAction;
    const restoreButton = setButtonLoading(incidentButton, "处理中");
    try {
      await api(`/api/notification-incidents/${incidentId}/${action}`, {
        method: "POST",
        body: JSON.stringify({
          actor: notificationActorName(),
          assigned_to: notificationActorName(),
        }),
      });
      state.notificationIncidents = await api("/api/notification-incidents");
      state.notificationSla = await api("/api/stats/notification-sla");
      state.aiRobotMessages = await api("/api/ai-robots/messages");
      renderOperationNotifications();
      if (currentView === "stats") renderNotificationSlaStats();
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

  const terminalAutoPublishButton = event.target.closest("[data-terminal-auto-publish]");
  if (terminalAutoPublishButton) {
    const windowId = String(terminalAutoPublishButton.dataset.terminalAutoPublish || "").trim();
    if (!windowId) return;
    const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === windowId);
    const targetIndex = Number(targetWindow?.current_index || 0);
    const targetAccountId = terminalResolveWindowAccountId(targetWindow, targetIndex);
    if (targetAccountId) markTerminalManualAction(windowId, targetAccountId, "publish");
    if (targetAccountId) terminalMarkPublishPending(windowId, targetAccountId);
    if (targetAccountId) clearTerminalButtonCooldown(windowId, targetAccountId, "publish_login_wait");
    if (targetAccountId) setTerminalButtonCooldown(windowId, targetAccountId, "publish", 15);
    if (targetAccountId) setTerminalButtonCooldown(windowId, targetAccountId, "next", 60);
    // Button flow is local-first: click publish immediately unlocks "下一步" for this account.
    renderTerminalExecution();
    if (terminalAutoPublishWindowIds.has(windowId)) return;
    terminalAutoPublishWindowIds.add(windowId);
    terminalAutoPublishStageByWindowId.set(windowId, "publishing");
    terminalErrorModalSignature = "";
    hideTerminalErrorModal();
    let stage = "manual_publish";
    try {
      const nextState = await api(`/api/terminal-execution/windows/${windowId}/manual-publish`, { method: "POST" });
      mergeTerminalWindowState(nextState, windowId);
      terminalReapplyInFlightWindowActions();
      terminalAutoPublishStageByWindowId.delete(windowId);
      renderTerminalExecution();
    } catch (error) {
      terminalAutoPublishStageByWindowId.delete(windowId);
      console.warn("[terminal:auto-publish] failed", { windowId, stage, error: error?.message || error });
      showTerminalErrorModal({
        stage: "publish_start",
        title: "发布执行失败",
        message: error?.message || "发布执行失败",
        context: `窗口 #${windowId}`,
        signature: `publish-request|${stage}|${windowId}|${error?.message || "unknown"}`,
      });
      renderTerminalExecution();
    } finally {
      terminalAutoPublishWindowIds.delete(windowId);
      terminalAutoPublishStageByWindowId.delete(windowId);
      renderTerminalExecution();
    }
    return;
  }

  const terminalLoginOpenButton = event.target.closest("[data-terminal-login-open]");
  if (terminalLoginOpenButton) {
    const token = parseWindowAccountToken(terminalLoginOpenButton.dataset.terminalLoginOpen || "");
    const windowIdValue = token.windowId;
    const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === String(windowIdValue));
    const accountIdValue = token.accountId || terminalResolveWindowAccountId(targetWindow, Number(targetWindow?.current_index || 0));
    if (!windowIdValue || !accountIdValue) {
      showTerminalErrorModal({
        stage: "login_browser",
        title: "登录浏览器打开失败",
        message: "当前窗口没有可用账号，无法打开登录浏览器。请先检查账号配置后重试。",
        context: `窗口 #${windowIdValue || "-"}`,
        signature: `login-browser|empty-account|${windowIdValue || "-"}`,
      });
      return;
    }
    // Button flow is local-first: click login immediately unlocks "发布" for this account.
    markTerminalManualAction(windowIdValue, accountIdValue, "login");
    terminalForcePostLoginStage(windowIdValue, accountIdValue);
    setTerminalButtonCooldown(windowIdValue, accountIdValue, "publish", 15);
    terminalManualCycleAccountByWindow.set(windowIdValue, accountIdValue);
    renderTerminalExecution();
    void refreshTerminalAccountQr(windowIdValue, accountIdValue, terminalLoginOpenButton);
    return;
  }

  const terminalCycleResetButton = event.target.closest("[data-terminal-cycle-reset]");
  if (terminalCycleResetButton) {
    const token = parseWindowAccountToken(terminalCycleResetButton.dataset.terminalCycleReset || "");
    const windowIdValue = token.windowId;
    const stateWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === String(windowIdValue));
    const accountIdValue = token.accountId || terminalResolveWindowAccountId(stateWindow, Number(stateWindow?.current_index || 0));
    if (!windowIdValue || !accountIdValue) return;
    if (terminalResetWindowIds.has(windowIdValue)) return;
    terminalBumpWindowActionEpoch(windowIdValue);
    terminalResetWindowIds.add(windowIdValue);
    const restoreButton = setButtonLoading(terminalCycleResetButton, "重置中");
    try {
      state.terminalExecution = await api(`/api/terminal-execution/windows/${windowIdValue}/accounts/${accountIdValue}/reset-manual-flow`, { method: "POST" });
      terminalReapplyInFlightWindowActions();
      terminalErrorModalSignature = "";
      hideTerminalErrorModal();
      const syncedWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === String(windowIdValue));
      const syncedCurrentId = terminalResolveWindowAccountId(syncedWindow, Number(syncedWindow?.current_index || 0));
      const resetAccountIds = Array.from(new Set([String(accountIdValue || "").trim(), String(syncedCurrentId || "").trim()])).filter(Boolean);
      resetAccountIds.forEach((resetAccountId) => {
        resetTerminalManualAction(windowIdValue, resetAccountId);
        clearTerminalButtonCooldown(windowIdValue, resetAccountId, "publish");
        clearTerminalButtonCooldown(windowIdValue, resetAccountId, "publish_login_wait");
        clearTerminalButtonCooldown(windowIdValue, resetAccountId, "next");
        const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === String(windowIdValue));
        const targetAccount = (targetWindow?.accounts || []).find((item) => String(item?.id ?? item?.account_id ?? "") === String(resetAccountId));
        if (targetAccount && typeof targetAccount === "object") {
          targetAccount.status = "pending";
          targetAccount.status_text = "待点登录";
          targetAccount.task_id = null;
          delete targetAccount.error_stage;
          delete targetAccount.error_title;
          delete targetAccount.error_detail;
        }
      });
      terminalManualCycleAccountByWindow.set(windowIdValue, accountIdValue);
      terminalAutoPublishWindowIds.delete(windowIdValue);
      terminalAutoPublishStageByWindowId.delete(windowIdValue);
      renderTerminalExecution();
    } finally {
      terminalResetWindowIds.delete(windowIdValue);
      restoreButton();
      renderTerminalExecution();
    }
    return;
  }

  const terminalQrRefreshButton = event.target.closest("[data-terminal-qr-refresh]");
  if (terminalQrRefreshButton) {
    const token = parseWindowAccountToken(terminalQrRefreshButton.dataset.terminalQrRefresh || "");
    const windowIdValue = token.windowId;
    if (!windowIdValue) return;
    const targetWindow = (state.terminalExecution.windows || []).find((item) => String(item.id) === String(windowIdValue));
    const accountIdValue = token.accountId || terminalResolveWindowAccountId(targetWindow, Number(targetWindow?.current_index || 0));
    if (!accountIdValue) {
      showTerminalErrorModal({
        stage: "login_browser",
        title: "登录浏览器打开失败",
        message: "当前窗口没有可用账号，无法打开登录浏览器。请先在配置中添加账号后重试。",
        context: `窗口 #${windowIdValue}`,
        signature: `login-browser|empty-account|${windowIdValue}`,
      });
      return;
    }
    await refreshTerminalAccountQr(windowIdValue, accountIdValue, terminalQrRefreshButton);
    return;
  }

  const terminalConfirmButton = event.target.closest("[data-terminal-confirm-success]");
  if (terminalConfirmButton) {
    const windowId = terminalConfirmButton.dataset.terminalConfirmSuccess;
    if (!windowId) return;
    const currentAccountId = terminalCurrentWindowAccountId(windowId);
    if (currentAccountId) setTerminalButtonCooldown(windowId, currentAccountId, "next", 15);
    if (currentAccountId) markTerminalManualAction(windowId, currentAccountId, "next");
    const restoreButton = setButtonLoading(terminalConfirmButton, "打开浏览器中");
    try {
      state.terminalExecution = await api(`/api/terminal-execution/windows/${windowId}/confirm-publish-success`, { method: "POST" });
      terminalReapplyInFlightWindowActions();
      terminalErrorModalSignature = "";
      hideTerminalErrorModal();
      const nextAccountId = terminalCurrentWindowAccountId(windowId);
      if (nextAccountId) {
        // Force the next account into publish stage locally: login is treated as already confirmed by operator.
        markTerminalManualAction(windowId, nextAccountId, "login");
        setTerminalButtonCooldown(windowId, nextAccountId, "publish_login_wait", 10);
        clearTerminalButtonCooldown(windowId, nextAccountId, "publish");
      }
      renderTerminalExecution();
    } catch (error) {
      const ok = terminalAdvanceWindowLocally(windowId);
      if (ok) {
        const nextAccountId = terminalCurrentWindowAccountId(windowId);
        if (nextAccountId) {
          // Keep local behavior consistent with success path even when backend confirm fails.
          markTerminalManualAction(windowId, nextAccountId, "login");
          setTerminalButtonCooldown(windowId, nextAccountId, "publish_login_wait", 10);
          clearTerminalButtonCooldown(windowId, nextAccountId, "publish");
        }
        renderTerminalExecution();
      }
      showTerminalErrorModal({
        stage: "confirm",
        title: "下一个已本地推进",
        message: `后端记录失败：${error?.message || "unknown"}`,
        context: `窗口 #${windowId}`,
        signature: `confirm-local-fallback|${windowId}|${error?.message || "unknown"}`,
      });
    } finally {
      restoreButton();
    }
    return;
  }

  const accountStatusButton = event.target.closest("[data-account-status-toggle]");
  if (accountStatusButton) {
    await toggleAccountStatus(accountStatusButton);
    return;
  }

  const saveAccountButton = event.target.closest("[data-account-save]");
  if (saveAccountButton) {
    await saveAccountInlineEdit(saveAccountButton);
    return;
  }

  if (event.target.closest("[data-account-cancel]")) {
    renderAccounts();
    return;
  }

  const editAccountButton = event.target.closest("[data-account-edit]");
  if (editAccountButton) {
    const row = editAccountButton.closest("[data-account-id]");
    const account = accountById(row?.dataset.accountId);
    if (!row || !account) return;
    if (editAccountButton.dataset.accountEdit === "name") startAccountNameEdit(row, account);
    if (editAccountButton.dataset.accountEdit === "operator") startAccountOperatorEdit(row, account);
    if (editAccountButton.dataset.accountEdit === "phone") startAccountPhoneEdit(row, account);
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

document.addEventListener("keydown", (event) => {
  const editor = event.target.closest?.(".account-inline-edit");
  if (!editor) return;
  if (event.key === "Escape") {
    event.preventDefault();
    renderAccounts();
  }
  if (event.key === "Enter") {
    const saveButton = editor.querySelector("[data-account-save]");
    if (saveButton) {
      event.preventDefault();
      saveButton.click();
    }
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
initSyncActions();
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
  if (!section) return;
  if (section.dataset.mounted === "true") {
    window.setTimeout(() => setWorkspaceLoading(false), 180);
    return;
  }
  section.dataset.mounted = "true";
  section.innerHTML = `<iframe class="video-matrix-frame" src="/static/video_matrix.html?embed=1" title="GasGx 视频生成工作台"></iframe>`;
  section.querySelector(".video-matrix-frame")?.addEventListener("load", () => {
    const theme = SHELL_THEMES.find((item) => item.id === localStorage.getItem(SHELL_THEME_KEY)) || SHELL_THEMES[0];
    broadcastShellTheme(theme);
    setWorkspaceLoading(false);
  });
  window.setTimeout(() => setWorkspaceLoading(false), 3000);
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
  const terminalWechatLoginSelected = event.target.closest("[data-terminal-wechat-login-selected]");
  const terminalWechatPublishSelected = event.target.closest("[data-terminal-wechat-publish-selected]");
  const terminalSave = event.target.closest("#terminal-save-config, [data-terminal-save-config]");
  const terminalCloseInit = event.target.closest("[data-terminal-close-init]");
  const terminalCloseConfig = event.target.closest("[data-terminal-close-config]");
  const embeddedStart = event.target.closest("[data-terminal-start-action]");
  const embeddedEdit = event.target.closest("[data-terminal-edit-action]");
  const terminalStart = event.target.closest("#terminal-start-login");
  const terminalEdit = event.target.closest("#terminal-edit-config");
  if (!enter && !configJump && !longDetect && !longOpen && !terminalWechatLoginSelected && !terminalWechatPublishSelected && !terminalSave && !terminalCloseInit && !terminalCloseConfig && !embeddedStart && !embeddedEdit && !terminalStart && !terminalEdit) return;
  if (terminalSave || terminalCloseInit || terminalCloseConfig || enter || configJump || longDetect || longOpen || terminalWechatLoginSelected || terminalWechatPublishSelected || embeddedStart || embeddedEdit || terminalStart || terminalEdit) {
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
    const fromWindowConfigModal = terminalSave.matches("[data-terminal-save-config]");
    const restoreButton = setButtonLoading(terminalSave, "更新中");
    try {
      if (effectiveRoute === "wechat" || fromWindowConfigModal) {
        state.terminalRoute = "wechat";
        if (window.location.hash !== "#terminal/wechat") {
          window.history.replaceState(null, "", "#terminal/wechat");
        }
        state.terminalExecution = await api("/api/terminal-execution/start", {
          method: "POST",
          body: JSON.stringify({ windows: readTerminalConfigRows() }),
        });
        state.terminalConfigOpen = false;
        renderTerminalExecution();
      } else {
        state.terminalConfigOpen = false;
        renderTerminalExecution();
      }
    } catch (error) {
      window.alert(`更新配置失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
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
      try {
        await startTerminalWechatLoginWithLoading(embeddedStart);
      } catch (error) {
        window.alert(`启动登录浏览器失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
      }
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
    terminalSetRoute(nextRoute);
    return;
  }
  if (configJump) {
    const platform = configJump.dataset.terminalConfigJump || "";
    if (platform === "wechat") {
      terminalSetRoute("wechat");
      openTerminalConfigPanel();
    } else {
      terminalSetRoute(platform);
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
  if (terminalWechatLoginSelected) {
    const selected = terminalWechatSelectedAccountChoice();
    if (!selected) {
      showTerminalErrorModal({
        stage: "login_browser",
        title: "登录浏览器打开失败",
        message: "当前没有可选账号，请先完成终端配置后重试。",
        context: "指定账号登录",
        signature: "wechat-selected-login|empty",
      });
      return;
    }
    const restoreButton = setButtonLoading(terminalWechatLoginSelected, "登录中");
    terminalErrorModalSignature = "";
    hideTerminalErrorModal();
    try {
      await api(`/api/accounts/${selected.accountId}/platforms/wechat/open-browser`, { method: "POST" });
      renderTerminalExecution();
    } catch (error) {
      showTerminalErrorModal({
        stage: "login_browser",
        title: "登录浏览器打开失败",
        message: error?.message || "登录浏览器打开失败",
        context: `账号 #${selected.accountId}`,
        signature: `wechat-selected-login|${selected.accountId}|${error?.message || "unknown"}`,
      });
    } finally {
      restoreButton();
    }
    return;
  }
  if (terminalWechatPublishSelected) {
    const selected = terminalWechatSelectedAccountChoice();
    if (!selected) {
      showTerminalErrorModal({
        stage: "publish_start",
        title: "发布启动失败",
        message: "当前没有可选账号，请先完成终端配置后重试。",
        context: "指定账号发布",
        signature: "wechat-selected-publish|empty",
      });
      return;
    }
    const restoreButton = setButtonLoading(terminalWechatPublishSelected, "发布中");
    terminalErrorModalSignature = "";
    hideTerminalErrorModal();
    try {
      state.terminalExecution = await api(`/api/accounts/${selected.accountId}/platforms/wechat/emergency-publish`, { method: "POST" });
      renderTerminalExecution();
    } catch (error) {
      showTerminalErrorModal({
        stage: "publish_start",
        title: "发布启动失败",
        message: error?.message || "发布启动失败",
        context: `账号 #${selected.accountId}`,
        signature: `wechat-selected-publish|${selected.accountId}|${error?.message || "unknown"}`,
      });
    } finally {
      restoreButton();
    }
    return;
  }
  if (terminalStart) {
    const route = terminalCurrentRoute();
    if (route === "hub") {
      await refresh();
      return;
    }
    if (route === "wechat") {
      try {
        await startTerminalWechatLoginWithLoading(terminalStart);
      } catch (error) {
        window.alert(`启动登录浏览器失败：${formatFriendlyMessage(error?.message || "未知异常")}`);
      }
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

document.addEventListener("change", (event) => {
  const select = event.target.closest("#terminal-wechat-account-select");
  if (!select) return;
  terminalWechatSelectedAccountId = String(select.value || "").trim();
  renderTerminalExecution();
}, true);

window.addEventListener("load", () => {
  const helpDocFromPath = helpCenterDocPathFromLocation();
  if (helpDocFromPath) {
    setHelpCenterStandaloneMode(true);
    activateView("help-center", false);
    openHelpDocument(helpDocFromPath, { syncUrl: false });
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 50);
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 300);
    return;
  }
  setHelpCenterStandaloneMode(false);
  const requested = terminalRouteFromHash();
  if (requested.view) {
    if (requested.view === "terminal-execution") state.terminalRoute = requested.route;
    activateView(requested.view, false);
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 50);
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 300);
  }
});

window.addEventListener("hashchange", () => {
  const requested = terminalRouteFromHash();
  if (!requested.view) return;
  if (requested.view === "terminal-execution") state.terminalRoute = requested.route;
  activateView(requested.view, false);
});
