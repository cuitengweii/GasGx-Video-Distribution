const PLATFORM_LABELS = {
  wechat: "瑙嗛鍙?,
  douyin: "鎶栭煶",
  kuaishou: "蹇墜",
  xiaohongshu: "灏忕孩涔?,
  bilibili: "B绔?,
  tiktok: "TikTok",
  x: "X",
  linkedin: "LinkedIn",
  facebook: "Facebook",
  youtube: "YouTube",
  vk: "VK",
  instagram: "Instagram",
};

const REGION_LABELS = {
  cn: "鍥藉唴骞冲彴",
  global: "鍥藉骞冲彴",
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
  summary: {},
  distributionSettings: { common: {}, platforms: {} },
  matrixJobStatus: {},
  aiRobotConfigs: [],
  aiRobotMessages: [],
  notificationRoutes: [],
  notificationEvents: [],
  loginQrBatches: [],
  terminalExecution: { colors: [], operators: [], windows: [], summary: {} },
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

const taskSelection = new Set();
const taskFilters = { account: "", platform: "", status: "", taskType: "" };
const TASK_TYPE_OPTIONS = [
  ["draft", "淇濆瓨鑽夌"],
  ["publish", "鑷姩鍙戝竷"],
  ["comment", "鑷姩璇勮"],
  ["message", "鑷姩绉佷俊"],
  ["stats", "鏁版嵁缁熻"],
];

const loadedViews = new Set();
let currentView = document.querySelector(".nav-btn.active")?.dataset.view || "overview";
let terminalPollTimer = null;
let terminalCountdownTimer = null;

const SHELL_THEME_KEY = "gasgx-shell-theme";
const SHELL_BRAND_KEY = "gasgx-shell-brand";
const SHELL_AUTH_KEY = "gasgx-shell-auth";
const DATABASE_DICTIONARY_LOCALE_KEY = "gasgx-db-dictionary-locale";
const PERMISSION_DENIED_MESSAGE = "鎮ㄦ潈闄愪笉瓒?;
const PERMISSION_INTERACTIVE_SELECTOR = "button, input, select, textarea, a, [role=\"button\"], [tabindex]";

const FEATURE_ENTRIES = [
  { id: "overview", label: "鎬昏", group: "涓氬姟宸ヤ綔鍙? },
  { id: "accounts", label: "璐﹀彿鐭╅樀", group: "涓氬姟宸ヤ綔鍙? },
  { id: "settings", label: "鍏叡璁剧疆", group: "涓氬姟宸ヤ綔鍙? },
  { id: "tasks", label: "浠诲姟涓績", group: "涓氬姟宸ヤ綔鍙? },
  { id: "terminal-execution", label: "缁堢鎵ц", group: "涓氬姟宸ヤ綔鍙? },
  { id: "stats", label: "鏁版嵁缁熻", group: "涓氬姟宸ヤ綔鍙? },
  { id: "ai-robot", label: "AI鏈哄櫒浜?, group: "涓氬姟宸ヤ綔鍙? },
  { id: "video-matrix", label: "瑙嗛鐢熸垚", group: "涓氬姟宸ヤ綔鍙? },
  { id: "user-center", label: "鐢ㄦ埛涓績", group: "绯荤粺绠＄悊" },
  { id: "notifications", label: "閫氱煡涓績", group: "绯荤粺绠＄悊" },
  { id: "system-settings", label: "绯荤粺璁剧疆", group: "绯荤粺绠＄悊" },
  { id: "help-center", label: "甯姪鏂囨。", group: "绯荤粺绠＄悊" },
];

const DEFAULT_AUTH_STATE = {
  currentUserId: "allen",
  roles: {
    super_admin: {
      name: "瓒呯骇绠＄悊鍛?,
      permissions: FEATURE_ENTRIES.map((item) => item.id),
    },
    publisher: {
      name: "鍙戝竷鍛?,
      permissions: ["overview", "accounts", "settings", "tasks", "terminal-execution", "video-matrix", "user-center", "notifications", "help-center"],
    },
    material_manager: {
      name: "绱犳潗缁存姢鍛?,
      permissions: ["overview", "accounts", "video-matrix", "user-center", "notifications", "help-center"],
    },
    data_monitor: {
      name: "鏁版嵁鐩戞帶鍛?,
      permissions: ["overview", "stats", "user-center", "notifications", "help-center"],
    },
  },
  users: [
    { id: "allen", name: "Allen", roleId: "super_admin" },
    { id: "publisher", name: "鍙戝竷鍛?, roleId: "publisher" },
    { id: "material", name: "绱犳潗缁存姢鍛?, roleId: "material_manager" },
    { id: "analyst", name: "鏁版嵁鐩戞帶鍛?, roleId: "data_monitor" },
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
  overview: ["璐﹀彿鐭╅樀缁存姢绯荤粺", "鐙珛璐﹀彿銆佺嫭绔嬫祻瑙堝櫒銆佸彂甯?璇勮/绉佷俊/缁熻浠诲姟鍏ュ彛"],
  accounts: ["璐﹀彿鐭╅樀", "缁存姢 GasGx 鍥藉唴澶栧钩鍙拌处鍙枫€佺嫭绔嬫祻瑙堝櫒閰嶇疆鍜岀櫥褰曠姸鎬併€?],
  "user-center": ["鐢ㄦ埛涓績", "棰勭暀鎿嶄綔鑰呰祫鏂欍€佽鑹叉潈闄愩€佸伐浣滃亸濂藉拰鏈湴閮ㄧ讲韬唤鍏ュ彛銆?],
  settings: ["鍏叡璁剧疆", "閰嶇疆鍙戝竷绱犳潗鐩綍銆佷笂浼犵瓥鐣ャ€佸钩鍙板弬鏁板拰鐭╅樀鍙戝竷浣滀笟銆?],
  tasks: ["浠诲姟涓績", "鏌ョ湅鍙戝竷銆佽瘎璁恒€佺淇°€佺櫥褰曟娴嬬瓑浠诲姟闃熷垪鍜屾墽琛岀姸鎬併€?],
  "terminal-execution": ["缁堢鎵ц", "棰勭暀鏈湴缁堢鍛戒护鎵ц鍏ュ彛銆?],
  stats: ["鏁版嵁缁熻", "鐭棰戣处鍙风煩闃垫暟瀛楀寲钀ラ攢瀹㈡埛绔暟鎹湅鏉裤€?],
  "ai-robot": ["AI鏈哄櫒浜?, "AI瀹㈡湇銆佷紒涓氬井淇°€侀拤閽夈€侀涔︺€乀elegram 涓?WhatsApp 缁熶竴鎺ュ叆銆?],
  "video-matrix": ["瑙嗛鐢熸垚", "鍒嗙被绱犳潗銆佺涓€灞忓皝闈€佽棰戞枃瀛椼€佽儗鏅煶涔愬拰鎵归噺瀵煎嚭宸ヤ綔鍙般€?],
  notifications: ["閫氱煡涓績", "闆嗕腑灞曠ず鐢熸垚瀹屾垚銆佸彂甯冨け璐ャ€佺櫥褰曞け鏁堝拰绱犳潗涓嶈冻鎻愰啋銆?],
  "system-settings": ["绯荤粺璁剧疆", "棰勭暀鏈湴閮ㄧ讲銆佸瓨鍌ㄧ紦瀛樸€佸畨鍏ㄧ瓥鐣ュ拰绯荤粺缁存姢鍏ュ彛銆?],
  "help-center": ["甯姪鏂囨。", "棰勭暀鎿嶄綔鎵嬪唽銆侀儴缃茶鏄庛€佽棰戠敓鎴愭祦绋嬪拰甯歌闂銆?],
};

function displayDatabaseKeyword(value) {
  return String(value ?? "").replaceAll("Supabase", "鏁版嵁搴?);
}

state.databaseDictionaryLocalized = localStorage.getItem(DATABASE_DICTIONARY_LOCALE_KEY) === "zh";

const DATABASE_DICTIONARY_TABLE_LABELS = {
  matrix_accounts: "鐭╅樀璐﹀彿",
  account_platforms: "璐﹀彿骞冲彴",
  browser_profiles: "娴忚鍣ㄩ厤缃?,
  notification_routes: "閫氱煡璺敱",
  login_qr_batches: "鐧诲綍浜岀淮鐮佹壒娆?,
  login_qr_items: "鐧诲綍浜岀淮鐮佹槑缁?,
  automation_tasks: "鑷姩鍖栦换鍔?,
  video_stats_snapshots: "瑙嗛缁熻蹇収",
  ai_robot_configs: "AI 鏈哄櫒浜洪厤缃?,
  ai_robot_messages: "AI 鏈哄櫒浜烘秷鎭?,
  brand_settings: "鍝佺墝璁剧疆",
  schema_migrations: "鏁版嵁搴撹縼绉?,
  app_settings: "搴旂敤璁剧疆",
  analytics_items: "鍒嗘瀽鏉＄洰",
  video_matrix_assets: "瑙嗛鐭╅樀绱犳潗",
  video_matrix_jobs: "瑙嗛鐭╅樀浠诲姟",
  video_matrix_generation_runs: "瑙嗛鐭╅樀鐢熸垚璁板綍",
  video_matrix_generation_assets: "瑙嗛鐭╅樀鐢熸垚绱犳潗",
  video_matrix_generation_segments: "瑙嗛鐭╅樀鐢熸垚鐗囨",
  app_seed_runs: "鍒濆鍖栫瀛愯褰?,
  brand_members: "鍝佺墝鎴愬憳",
};

const DATABASE_DICTIONARY_COLUMN_LABELS = {
  id: "缂栧彿",
  account_key: "璐﹀彿鏍囪瘑",
  display_name: "鏄剧ず鍚嶇О",
  niche: "棰嗗煙",
  status: "鐘舵€?,
  notes: "澶囨敞",
  created_at: "鍒涘缓鏃堕棿",
  updated_at: "鏇存柊鏃堕棿",
  account_id: "璐﹀彿缂栧彿",
  platform: "骞冲彴",
  handle: "璐﹀彿鍙ユ焺",
  enabled: "鍚敤",
  capability_status: "鑳藉姏鐘舵€?,
  login_status: "鐧诲綍鐘舵€?,
  last_checked_at: "鏈€鍚庢鏌ユ椂闂?,
  profile_dir: "閰嶇疆鐩綍",
  debug_port: "璋冭瘯绔彛",
  fingerprint_json: "鎸囩汗閰嶇疆",
  event_type: "浜嬩欢绫诲瀷",
  batch_id: "鎵规缂栧彿",
  payload_json: "杞借嵎鏁版嵁",
  notified_at: "閫氱煡鏃堕棿",
  reason: "鍘熷洜",
  url: "閾炬帴",
  qr_path: "浜岀淮鐮佽矾寰?,
  qr_fingerprint: "浜岀淮鐮佹寚绾?,
  task_type: "浠诲姟绫诲瀷",
  summary: "鎽樿",
  error: "閿欒",
  retry_count: "閲嶈瘯娆℃暟",
  last_attempt_at: "鏈€鍚庡皾璇曟椂闂?,
  sent_at: "鍙戦€佹椂闂?,
  video_ref: "瑙嗛寮曠敤",
  views: "鎾斁閲?,
  likes: "鐐硅禐鏁?,
  comments: "璇勮鏁?,
  shares: "鍒嗕韩鏁?,
  messages: "绉佷俊鏁?,
  published_at: "鍙戝竷鏃堕棿",
  captured_at: "鎶撳彇鏃堕棿",
  bot_name: "鏈哄櫒浜哄悕绉?,
  webhook_url: "鍥炶皟鍦板潃",
  webhook_secret: "鍥炶皟瀵嗛挜",
  signing_secret: "绛惧悕瀵嗛挜",
  target_id: "鐩爣缂栧彿",
  message_type: "娑堟伅绫诲瀷",
  name: "鍚嶇О",
  slogan: "鏍囪",
  logo_asset_path: "Logo 璧勬簮璺緞",
  primary_color: "涓昏壊",
  theme_id: "涓婚缂栧彿",
  default_account_prefix: "榛樿璐﹀彿鍓嶇紑",
  version: "鐗堟湰",
  app_version: "搴旂敤鐗堟湰",
  applied_at: "搴旂敤鏃堕棿",
  setting_key: "璁剧疆閿?,
  asset_key: "绱犳潗閿?,
  asset_type: "绱犳潗绫诲瀷",
  title: "鏍囬",
  path: "璺緞",
  metadata_json: "鍏冩暟鎹?,
  source: "鏉ユ簮",
  job_key: "浠诲姟缂栧彿",
  stage: "闃舵",
  progress: "杩涘害",
  message: "娑堟伅",
  request_json: "璇锋眰鏁版嵁",
  assets_json: "绱犳潗鏁版嵁",
  run_id: "杩愯缂栧彿",
  bgm_filename: "鑳屾櫙闊充箰鏂囦欢鍚?,
  bgm_path: "鑳屾櫙闊充箰璺緞",
  composition_json: "缁勫悎鏁版嵁",
  sequence_number: "搴忓彿",
  signature: "绛惧悕",
  copy_path: "鏂囨璺緞",
  manifest_path: "娓呭崟璺緞",
  template_id: "妯℃澘缂栧彿",
  cover_template_id: "灏侀潰妯℃澘缂栧彿",
  copy_language: "鏂囨璇█",
  segment_index: "鐗囨搴忓彿",
  clip_id: "鐗囨缂栧彿",
  category: "鍒嗙被",
  source_path: "婧愭枃浠惰矾寰?,
  normalized_path: "鏍囧噯鍖栬矾寰?,
  start_time: "寮€濮嬫椂闂?,
  duration: "鏃堕暱",
  user_id: "鐢ㄦ埛缂栧彿",
  role: "瑙掕壊",
  item_key: "鏉＄洰閿?,
  section: "鍒嗗尯",
  sort_order: "鎺掑簭",
};

const DATABASE_DICTIONARY_TYPE_LABELS = {
  bigint: "澶ф暣鏁?,
  integer: "鏁存暟",
  numeric: "鏁板€?,
  text: "鏂囨湰",
  jsonb: "JSON 鏁版嵁",
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
  if (!raw) return "绌哄€?;
  if (/^null$/i.test(raw)) return "绌哄€?;
  if (/^as identity$/i.test(raw)) return "鑷鏍囪瘑";
  if (/^''$/i.test(raw)) return "绌哄瓧绗︿覆";
  if (/^\{\}::jsonb$/i.test(raw)) return "绌?JSON 瀵硅薄";
  if (/^\[\]::jsonb$/i.test(raw)) return "绌?JSON 鏁扮粍";
  const rawValue = raw.replace(/^'(.+)'$/u, "$1");
  const defaultValueLabels = {
    active: "鍚敤",
    pending: "寰呭鐞?,
    registered: "宸茬櫥璁?,
    unknown: "鏈煡",
    draft: "鑽夌",
    public: "鍏紑",
    inherit: "缁ф壙",
    queued: "鎺掗槦涓?,
    available: "鍙敤",
    seed: "绉嶅瓙",
    sent: "宸插彂閫?,
    retry: "閲嶈瘯",
    failed: "澶辫触",
    running: "杩愯涓?,
    complete: "瀹屾垚",
    info: "鎻愮ず",
    warning: "璀﹀憡",
    error: "閿欒",
    blocking: "闃诲",
    critical: "涓ラ噸",
    enabled: "宸插惎鐢?,
    disabled: "宸茬鐢?,
    short_video: "鐭棰?,
    video: "瑙嗛",
    text: "鏂囨湰",
    image: "鍥剧墖",
  };
  if (defaultValueLabels[rawValue]) return defaultValueLabels[rawValue];
  return displayDatabaseKeyword(raw);
}

function translateDatabaseConstraintSummary(meta, localized) {
  if (!localized) return meta.raw || "鏃犵害鏉?;
  const raw = meta.raw || "";
  const parts = [];
  if (meta.primary) parts.push("涓婚敭");
  if (meta.notNull) parts.push("闈炵┖");
  if (/unique/i.test(raw)) parts.push("鍞竴");
  if (/references/i.test(raw)) parts.push("澶栭敭");
  if (/generated by default as identity/i.test(raw)) parts.push("鑷");
  if (/on delete cascade/i.test(raw)) parts.push("鍒犻櫎绾ц仈");
  if (/on delete set null/i.test(raw)) parts.push("鍒犻櫎缃┖");
  if (/check/i.test(raw)) parts.push("鏍￠獙");
  if (/default/i.test(raw) && meta.defaultValue) parts.push(`榛樿 ${translateDatabaseDefaultValue(meta, true)}`);
  return parts.length ? parts.join(" / ") : "鏃犵害鏉?;
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
    const restoreButton = setButtonLoading(event.currentTarget, "淇濆瓨涓?..");
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
    const restoreButton = setButtonLoading(event.currentTarget, "鎭㈠涓?..");
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
  toast.innerHTML = `<strong>璐﹀彿鍒涘缓鎴愬姛</strong><span>#${account.id} ${account.display_name}</span>`;
  toast.classList.add("show");
  clearTimeout(showAccountCreatedToast.timer);
  showAccountCreatedToast.timer = setTimeout(() => {
    toast.classList.remove("show");
  }, 2600);
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
  const restoreButton = setButtonLoading(addButton, "淇濆瓨涓?);
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
  document.querySelector("#signed-user-name").textContent = user?.name || "鏈櫥褰?;
  document.querySelector(".signed-user-badge")?.setAttribute("aria-label", `褰撳墠鐧诲綍鐢ㄦ埛 ${user?.name || "鏈櫥褰?}`);
  const sessionUserName = document.querySelector("#session-user-name");
  const sessionUserDesc = document.querySelector("#session-user-desc");
  const sessionRoleBadge = document.querySelector("#session-role-badge");
  const sessionAvatar = document.querySelector("#session-avatar");
  if (sessionUserName) sessionUserName.textContent = user?.name || "鏈櫥褰?;
  if (sessionUserDesc) sessionUserDesc.textContent = user ? `${role?.name || "鏈垎閰嶈鑹?} / ${user?.roleId === "super_admin" ? "鍙垎閰嶈处鍙蜂笌瑙掕壊鏉冮檺" : "鎸夎鑹叉樉绀哄姛鑳藉叆鍙?}` : "璇风敤宸插垎閰嶈处鍙风櫥褰?;
  if (sessionRoleBadge) sessionRoleBadge.textContent = role?.name || "鏈櫥褰?;
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
      if (user.id === "allen") return `<option value="${user.id}">${role?.name || "瓒呯骇绠＄悊鍛?}</option>`;
      return `<option value="${user.id}">${user.name} 路 ${role?.name || "鏈垎閰?}</option>`;
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
        <input data-user-password="${user.id}" type="text" placeholder="${user.roleId === "super_admin" ? "绯荤粺鍥哄畾" : "璁剧疆/閲嶇疆鍙ｄ护"}" ${user.roleId === "super_admin" ? "disabled" : ""}>
        <button class="btn secondary" type="button" data-save-user-password="${user.id}" ${user.roleId === "super_admin" ? "disabled" : ""}>淇濆瓨鍙ｄ护</button>
        <span>${role?.name || "鏈垎閰?}</span>
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
      const restoreButton = setButtonLoading(button, "淇濆瓨涓?..");
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
      const restoreButton = setButtonLoading(button, "鍒囨崲涓?..");
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
    const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "鐧诲綍涓?..");
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
      if (errorNode) errorNode.textContent = error.message || "鐧诲綍澶辫触";
    } finally {
      restoreButton();
    }
  });
  document.querySelector("#operator-account-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const nameInput = document.querySelector("#operator-name-input");
    const name = nameInput.value.trim();
    if (!name) return;
    const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "娣诲姞涓?..");
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
    const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "娣诲姞涓?..");
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
      const restoreButton = setButtonLoading(button, "閫€鍑轰腑...");
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
    sidebarToggle.textContent = collapsed ? "鈥? : "鈥?;
    sidebarToggle.setAttribute("aria-expanded", String(!collapsed));
    sidebarToggle.setAttribute("aria-label", collapsed ? "鏄剧ず宸︿晶鏍? : "闅愯棌宸︿晶鏍?);
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
  if (duplicateTaskMatch) return `宸叉湁鐩稿悓浠诲姟鍦ㄩ槦鍒椾腑锛?${duplicateTaskMatch[1]}`;
  if (text === "queued for manual worker execution") return "宸插姞鍏ラ槦鍒楋紝绛夊緟浜哄伐鎵ц";
  if (text === "pending") return "寰呭鐞?;
  if (text === "paused") return "宸叉殏鍋?;
  if (text === "unsupported") return "鏆備笉鏀寔";
  return text || "鎿嶄綔澶辫触锛岃绋嶅悗閲嶈瘯";
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const text = await response.arrayBuffer().then((buffer) => {
    const decoded = new TextDecoder("utf-8").decode(buffer);
    if (/[脙脗氓莽忙猫盲枚眉]/.test(decoded) && !/[\u4e00-\u9fff]/.test(decoded)) {
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

function setButtonLoading(button, loadingText = "澶勭悊涓?) {
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

function loadingInline(label = "鍔犺浇涓?..") {
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
  body.innerHTML = loadingInline("鍔犺浇甯姪鏂囨。...");
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

function setPageLoading(label = "鍔犺浇涓?..") {
  const targets = [
    ["#summary", "鍔犺浇姒傝..."],
    ["#platforms", "鍔犺浇骞冲彴..."],
    ["#accounts-list", "鍔犺浇璐﹀彿..."],
    ["#tasks-list", "鍔犺浇浠诲姟..."],
    ["#stats-overview", "鍔犺浇缁熻..."],
    ["#operation-progress", "鍔犺浇杩涘害..."],
    ["#platform-settings-list", "鍔犺浇璁剧疆..."],
    ["#matrix-job-status", "鍔犺浇浣滀笟..."],
    ["#operation-notice-routes", "鍔犺浇閫氱煡..."],
    ["#login-qr-batches", "鍔犺浇鐧诲綍鎵规..."],
    ["#supabase-health-list", "鍔犺浇鏁版嵁搴撳瓧鍏?.."],
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
      ["#summary", "鍔犺浇姒傝..."],
      ["#platforms", "鍔犺浇骞冲彴..."],
    ],
    accounts: [["#accounts-list", "鍔犺浇璐﹀彿..."]],
    settings: [
      ["#platform-settings-list", "鍔犺浇璁剧疆..."],
      ["#matrix-job-status", "鍔犺浇浣滀笟..."],
    ],
    tasks: [["#tasks-list", "鍔犺浇浠诲姟..."]],
    stats: [
      ["#stats-overview", "鍔犺浇缁熻..."],
      ["#operation-progress", "鍔犺浇杩涘害..."],
    ],
    "ai-robot": [],
    notifications: [
      ["#operation-notice-routes", "鍔犺浇閫氱煡..."],
      ["#login-qr-batches", "鍔犺浇鐧诲綍鎵规..."],
    ],
    "terminal-execution": [
      ["#terminal-config-list", "鍔犺浇杩愯惀寰俊閰嶇疆..."],
      ["#terminal-matrix-workspace", "鍔犺浇缁堢鎵ц鏁版嵁..."],
    ],
    "system-settings": [["#supabase-health-list", "鍔犺浇鏁版嵁搴撳瓧鍏?.."]],
  };
  if (view === "ai-robot") renderAiRobotLoading();
  (targets[view] || []).forEach(([selector, text]) => {
    const node = document.querySelector(selector);
    if (node) node.innerHTML = loadingInline(text || label);
  });
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
    active: "宸插惎鐢?,
    ready: "宸查儴缃?,
    logged_in: "宸查儴缃?,
    success: "姝ｅ父",
    ok: "姝ｅ父",
    login_required: "闇€鐧诲綍",
    logged_out: "鏈櫥褰?,
    failed: "寮傚父",
    error: "寮傚父",
    pending: "寰呮鏌?,
    checking: "妫€鏌ヤ腑",
    not_checked: "寰呮鏌?,
    unknown: "鏈煡",
  };
  return labels[normalized] || status || "鏈煡";
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
    metric("璐﹀彿", s.accounts || 0),
    metric("骞冲彴妲戒綅", s.platforms || 0),
    metric("鍓╀綑绱犳潗", s.remaining_material_videos || 0),
    metric("杩愯涓换鍔?, s.running_tasks || 0),
    metric("澶辫触浠诲姟", s.failed_tasks || 0),
    metric("鎾斁", s.views || 0),
    metric("璇勮", s.comments || 0),
  ].join("");
}

function abilityText(item) {
  return [
    item.can_publish ? "鍙戝竷" : "",
    item.can_comment ? "璇勮" : "",
    item.can_message ? "绉佷俊" : "",
    item.can_login_status ? "鐧诲綍妫€娴? : "娴忚鍣ㄧ淮鎶?,
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
          <div class="row-head"><strong>${platformName(item.key)}</strong><span>${item.region === "cn" ? "鍥藉唴" : "鍥藉"}</span></div>
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
    : `<option value="">璇峰厛鍒涘缓璐﹀彿</option>`;

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
  const match = String(account?.notes || "").match(/缁戝畾杩愯惀寰俊锛?[^锛?]+)/);
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
          <div class="account-subtitle">${account.account_key} 路 ${account.niche || ""}</div>
          <div class="account-operator-wechat">缁戝畾杩愯惀寰俊锛?strong>${operatorWechat || "-"}</strong></div>
        </div>
        <div class="account-badges">
          <span class="chip">${account.status}</span>
          <span class="chip success-chip">鍙戝竷鎴愬姛 ${account.publish_success_count || 0}</span>
          <button class="btn ghost btn-sm danger-action" type="button" data-delete-account="${account.id}" data-account-name="${account.display_name}">鍒犻櫎璐﹀彿</button>
        </div>
      </div>
      ${renderPlatformStatusGroup(platforms, "cn")}
      ${renderPlatformStatusGroup(platforms, "global")}
    </article>`;
  }).join("") || `<div class="muted">鏆傛棤璐﹀彿</div>`;
}

function taskTypeLabel(type) {
  return TASK_TYPE_OPTIONS.find(([value]) => value === type)?.[1] || type || "鏈寚瀹?;
}

function taskAccountLabel(task) {
  const accountId = Number(task.account_id || 0);
  const account = state.accounts.find((item) => Number(item.id) === accountId);
  return account
    ? `#${account.id} ${account.display_name || account.account_key || "鏈懡鍚嶈处鍙?}`
    : (accountId ? `#${accountId} 鏈煡璐﹀彿` : "鏈寚瀹氳处鍙?);
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
        <label>璐﹀彿<select data-task-filter="account"><option value="">鍏ㄩ儴璐﹀彿</option>${taskAccountFilterOptions()}</select></label>
        <label>骞冲彴<select data-task-filter="platform"><option value="">鍏ㄩ儴骞冲彴</option>${taskFilterOptions(state.tasks, (task) => task.platform, (task) => platformLabel(task.platform))}</select></label>
        <label>鐘舵€?select data-task-filter="status"><option value="">鍏ㄩ儴鐘舵€?/option>${taskFilterOptions(state.tasks, (task) => task.status, (task) => formatFriendlyMessage(task.status))}</select></label>
        <label>浠诲姟绫诲瀷<select data-task-filter="taskType"><option value="">鍏ㄩ儴绫诲瀷</option>${TASK_TYPE_OPTIONS.map(([value, label]) => `<option value="${value}">${label}</option>`).join("")}</select></label>
      </div>
      <div class="task-bulk-actions">
        <label class="task-check-all"><input type="checkbox" data-task-select-all ${visibleIds.length && selectedVisible.length === visibleIds.length ? "checked" : ""}>鍏ㄩ€?/label>
        <span class="muted">宸查€?${taskSelection.size} 鏉?/span>
        <button class="btn secondary btn-sm" type="button" data-task-bulk-status="paused" ${taskSelection.size ? "" : "disabled"}>鏆傚仠闃熷垪</button>
        <button class="btn secondary btn-sm danger-action" type="button" data-task-bulk-delete ${taskSelection.size ? "" : "disabled"}>鍒犻櫎闃熷垪</button>
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
          <button class="btn secondary task-delete" data-delete-task="${task.id}" type="button">鍒犻櫎闃熷垪</button>
        </span>
      </div>
      <div class="task-meta">
        <span>浠诲姟绫诲瀷锛?{taskTypeLabel(task.task_type)}</span>
        <span>璐﹀彿锛?{accountLabel}</span>
        <span>骞冲彴锛?{platformLabel(task.platform)}</span>
      </div>
      <div class="muted">${formatFriendlyMessage(task.summary || task.error || "")}</div>
    </article>`;
    }).join("") || `<div class="muted">鏆傛棤鍖归厤浠诲姟</div>`}
  `;
  document.querySelectorAll("[data-task-filter]").forEach((select) => {
    select.value = taskFilters[select.dataset.taskFilter] || "";
  });
}

function terminalColorByIndex(index) {
  return (state.terminalExecution.colors || [])[index % Math.max(1, (state.terminalExecution.colors || []).length)] || { hex: "#3B82F6", name: "绉戞妧钃? };
}

function renderTerminalConfig() {
  const list = document.querySelector("#terminal-config-list");
  if (!list) return;
  const operators = state.terminalExecution.operators || [];
  const colors = state.terminalExecution.colors || [];
  const savedRows = state.terminalExecution.config || [];
  const defaultRows = savedRows.length ? savedRows : Array.from({ length: 5 }, (_, index) => ({
    id: index + 1,
    enabled: Boolean(operators[index] || operators[0]),
    operator_wechat: operators[index]?.operator_wechat || operators[index % Math.max(1, operators.length)]?.operator_wechat || "",
    color: terminalColorByIndex(index).hex,
  }));
  list.innerHTML = defaultRows.map((row, index) => `
    <div class="terminal-config-row ${row.enabled ? "" : "disabled"}" data-terminal-config-row="${row.id}">
      <label class="terminal-config-left">
        <input type="checkbox" class="terminal-checkbox" data-terminal-enabled ${row.enabled ? "checked" : ""}>
        <span>缁堢 ${String(row.id).padStart(2, "0")}</span>
      </label>
      <select class="terminal-wx-select" data-terminal-operator ${row.enabled ? "" : "disabled"}>
        ${operators.map((operator) => `<option value="${operator.operator_wechat}" ${operator.operator_wechat === row.operator_wechat ? "selected" : ""}>${operator.operator_wechat}</option>`).join("") || `<option value="">鏆傛棤缁戝畾杩愯惀寰俊</option>`}
      </select>
      <div class="terminal-swatch-group">
        ${colors.map((color, colorIndex) => `
          <button class="terminal-swatch ${color.hex === row.color ? "active" : ""}" type="button" data-terminal-color="${color.hex}" title="${color.name}" style="background:${color.hex};color:${color.hex}" ${row.enabled ? "" : "disabled"}></button>
        `).join("")}
      </div>
    </div>
  `).join("");
}

function terminalPlaceholderIcon() {
  return `<svg width="112" height="112" viewBox="0 0 112 112" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="绛夊緟寮€濮嬬櫥褰?>
    <rect x="22" y="18" width="68" height="76" rx="12" fill="var(--terminal-glass-bg)" stroke="var(--term-color)" stroke-opacity=".7" stroke-width="2"/>
    <rect x="34" y="30" width="16" height="16" rx="3" stroke="var(--term-color)" stroke-width="2"/>
    <rect x="62" y="30" width="16" height="16" rx="3" stroke="var(--term-color)" stroke-width="2"/>
    <rect x="34" y="58" width="16" height="16" rx="3" stroke="var(--term-color)" stroke-width="2"/>
    <path d="M63 58h15v15M63 74h6M78 82h-8M54 84h-8" stroke="var(--term-color)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
    <path d="M38 90h36" stroke="var(--terminal-text-sub)" stroke-width="2" stroke-linecap="round"/>
    <circle cx="56" cy="56" r="44" stroke="var(--term-color)" stroke-opacity=".18" stroke-width="2"/>
  </svg>`;
}

function renderTerminalExecution() {
  renderTerminalConfig();
  const windows = state.terminalExecution.windows || [];
  const summary = state.terminalExecution.summary || {};
  const loginStarted = Boolean(state.terminalExecution.login_started);
  document.querySelector("#terminal-init-modal")?.classList.toggle("hidden", !state.terminalConfigOpen);
  const startLoginButton = document.querySelector("#terminal-start-login");
  if (startLoginButton) {
    startLoginButton.disabled = loginStarted;
    startLoginButton.textContent = loginStarted ? "鐧诲綍涓? : "寮€濮嬬櫥褰?;
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
    const qrStatusText = loginStarted ? `姝ｅ湪绛夊緟 [${current.display_name || "-"}] 鎵爜纭` : `绛夊緟鐐瑰嚮寮€濮嬬櫥褰?[${current.display_name || "-"}]`;
    return `
      <div class="terminal-task-column terminal-glass" style="--term-color:${color};--term-color-dim:${colorDim}">
        <div class="terminal-color-anchor"></div>
        <div class="terminal-col-header">
          <div class="terminal-col-header-top">
            <span style="font-weight:700;font-size:16px;">缁堢鎵ц绐?${String(window.id).padStart(2, "0")}</span>
            <span class="terminal-status-badge theme">鑹叉爣: ${window.color_name || ""}</span>
          </div>
          <div class="terminal-wx-operator">杩愯惀寰俊: ${window.operator_wechat || "-"}</div>
        </div>
        <div class="terminal-qr-section">
          <div class="terminal-qr-placeholder">${state.terminalQrVisible && loginStarted && window.qr_url ? `<img src="${window.qr_url}" alt="瑙嗛鍙风櫥褰曚簩缁寸爜">` : terminalPlaceholderIcon()}</div>
          <div style="font-size:12px;color:var(--terminal-text-sub);">${qrStatusText}</div>
        </div>
        <div class="terminal-account-list">
          ${accounts.map((account, index) => `
            <div class="terminal-account-item ${index === currentIndex ? "active" : ""}">
              <div class="terminal-account-info">
                <div class="terminal-avatar"></div>
                <div>
                  <div class="terminal-acc-name">${account.display_name || account.account_key || `璐﹀彿 ${account.id}`}</div>
                  <div class="terminal-acc-status">${account.status_text || "鏈櫥褰?}</div>
                </div>
              </div>
              <div class="terminal-status-badge ${account.status === "success" ? "success" : ""}">${account.status === "success" ? "鍙戝竷鎴愬姛" : (account.task_id ? `浠诲姟:${account.task_id}` : "-")}</div>
            </div>
          `).join("") || `<div class="muted">鏆傛棤璐﹀彿</div>`}
        </div>
        <div class="terminal-col-footer">
          <div class="terminal-progress-bar"><div class="terminal-progress-fill" style="width:${accounts.length ? Math.round((successCount / accounts.length) * 100) : 0}%;"></div></div>
          <button class="terminal-col-btn" type="button" data-terminal-manual="${window.id}" ${!loginStarted || manualWait > 0 ? "disabled" : ""}>${!loginStarted ? "绛夊緟鐧诲綍" : (manualWait > 0 ? `涓诲姩鍙戝竷 (${manualWait}s)` : "涓诲姩鍙戝竷")}</button>
        </div>
      </div>
    `;
  }).join("");
}

function updateTerminalManualCountdowns() {
  const windows = state.terminalExecution.windows || [];
  const loginStarted = Boolean(state.terminalExecution.login_started);
  const windowById = new Map(windows.map((window) => [String(window.id), window]));
  document.querySelectorAll("[data-terminal-manual]").forEach((button) => {
    const window = windowById.get(String(button.dataset.terminalManual || ""));
    const manualWait = loginStarted ? Math.max(0, Number(window?.manual_available_at || 0) - Math.floor(Date.now() / 1000)) : 0;
    button.disabled = !loginStarted || manualWait > 0;
    button.textContent = !loginStarted ? "绛夊緟鐧诲綍" : (manualWait > 0 ? `涓诲姩鍙戝竷 (${manualWait}s)` : "涓诲姩鍙戝竷");
  });
}

function readTerminalConfigRows() {
  return Array.from(document.querySelectorAll("[data-terminal-config-row]")).map((row) => {
    const activeSwatch = row.querySelector(".terminal-swatch.active");
    return {
      id: Number(row.dataset.terminalConfigRow || 0),
      enabled: Boolean(row.querySelector("[data-terminal-enabled]")?.checked),
      operator_wechat: row.querySelector("[data-terminal-operator]")?.value || "",
      color: activeSwatch?.dataset.terminalColor || terminalColorByIndex(0).hex,
    };
  });
}

function startTerminalPolling() {
  if (terminalPollTimer) clearInterval(terminalPollTimer);
  terminalPollTimer = setInterval(async () => {
    if (currentView !== "terminal-execution") return;
    state.terminalExecution = await api("/api/terminal-execution/poll", { method: "POST" });
    renderTerminalExecution();
  }, 10000);
  if (terminalCountdownTimer) clearInterval(terminalCountdownTimer);
  terminalCountdownTimer = setInterval(() => {
    if (currentView !== "terminal-execution") return;
    updateTerminalManualCountdowns();
  }, 1000);
}

function renderStats() {
  const summary = state.summary || {};
  const overview = [
    ["鐭╅樀璐﹀彿鎬绘暟", summary.accounts || 4, "+8.4%", "up"],
    ["绱浣滃搧鎬婚噺", 186, "+18.6%", "up"],
    ["绱鎬绘洕鍏?, "68.4涓?, "+24.8%", "up"],
    ["绱鎬绘挱鏀?, "28.6涓?, "+19.2%", "up"],
    ["鐭╅樀鎬荤矇涓?, "4.8涓?, "+9.7%", "up"],
    ["鍛ㄦ湡鍑€澧炵矇涓?, 3280, "+14.5%", "up"],
    ["绱浜掑姩閲?, "2.6涓?, "+11.3%", "up"],
    ["绱绾跨储閲?, 426, "-3.2%", "down"],
  ];
  document.querySelector("#stats-overview").innerHTML = overview.map(([label, value, change, trend]) => `
    <div class="metric client-metric"><span>${label}</span><strong>${value}</strong><em class="${trend}">${change}</em></div>
  `).join("");

  const statsAccountFilter = document.querySelector("#stats-account-filter");
  if (statsAccountFilter) {
    const activeAccounts = (state.accounts || [])
      .filter((account) => String(account.status || "").toLowerCase() === "active")
      .sort((a, b) => Number(a.id || 0) - Number(b.id || 0));
    const currentValue = statsAccountFilter.value;
    const activeOptions = activeAccounts.map((account) => {
      const label = account.display_name || account.account_key || `璐﹀彿 ${account.id}`;
      return `<option value="${account.id}">#${account.id} ${label}</option>`;
    }).join("");
    statsAccountFilter.innerHTML = `<option value="">鍏ㄩ儴璐﹀彿</option>${activeOptions}`;
    if (currentValue && [...statsAccountFilter.options].some((option) => option.value === currentValue)) {
      statsAccountFilter.value = currentValue;
    } else {
      statsAccountFilter.value = "";
    }
  }

  const accounts = [
    ["GasGx灏忕豢", "瑙嗛鍙?, "姝ｅ父", "86,200", "18,600", "12,480", "+860", "42.1%", "8.6%", 12, "鐖嗘璐﹀彿", ""],
    ["GasGx灏忛粍", "鎶栭煶", "姝ｅ父", "72,100", "16,900", "10,220", "+640", "37.8%", "7.9%", 10, "绋冲畾璐﹀彿", ""],
    ["鍙戠數鏈虹粍妗堜緥", "灏忕孩涔?, "浣庢祦閲?, "18,400", "3,420", "3,180", "+92", "28.4%", "4.1%", 5, "娼滃姏璐﹀彿", "浣庢祦閲?],
    ["鐕冩皵鍙戝姩鏈虹幇鍦?, "蹇墜", "浼戠湢", "9,860", "1,160", "1,204", "-36", "22.6%", "2.8%", 1, "浣庢晥璐﹀彿", "闀挎湡鏂洿"],
  ];
  const accountHeaders = ["璐﹀彿鍚嶇О", "骞冲彴", "鐘舵€?, "鎬绘挱鏀?, "鍛ㄦ湡鎾斁", "绮変笣", "澧炵矇", "瀹屾挱鐜?, "浜掑姩鐜?, "鏇存柊", "鍒嗗眰", "寮傚父"];
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
      <div class="table-pager">绗?1 / 1 椤?路 ${filtered.length} 鏉¤处鍙?/div>
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
    ["鐕冩皵鍙戝姩鏈虹粍鐜板満骞舵満", "8.6涓?, "鐖嗘"],
    ["娌规皵鐢拌嚜鍙戠數鏀归€犳渚?, "6.9涓?, "鐖嗘"],
    ["鍙戠數鏈虹粍璐熻浇娴嬭瘯", "4.2涓?, "鏅€?],
    ["鐭垮満鐢ㄧ數鎴愭湰瀵规瘮", "3.8涓?, "鏅€?],
  ];
  document.querySelector("#content-top-list").innerHTML = works.map((item, index) => `
    <article class="rank-row"><span>${index + 1}</span><strong>${item[0]}</strong><em>${item[1]}</em><b>${item[2]}</b></article>
  `).join("");

  const traffic = [["鎺ㄨ崘娴侀噺", "54%"], ["鎼滅储娴侀噺", "18%"], ["涓婚〉娴侀噺", "12%"], ["鍚屽煄娴侀噺", "6%"], ["鍒嗕韩娴侀噺", "7%"], ["浠樿垂娴侀噺", "3%"]];
  document.querySelector("#traffic-list").innerHTML = traffic.map(([label, value]) => `<div><span>${label}</span><strong>${value}</strong></div>`).join("");

  const conversions = [["涓婚〉璁块棶閲?, "17,860"], ["绉佷俊鍜ㄨ閲?, "1,286"], ["璇勮鍜ㄨ閲?, "824"], ["鏈夋晥绾跨储鏁?, "426"], ["琛ㄥ崟鐣欒祫閲?, "196"], ["绉佸煙寮曟祦鏁?, "158"], ["鎰忓悜瀹㈡埛鏁?, "138"], ["鏁翠綋绾跨储杞寲鐜?, "0.15%"]];
  document.querySelector("#conversion-cards").innerHTML = conversions.map(([label, value]) => `<div><span>${label}</span><strong>${value}</strong></div>`).join("");

  const ops = [["璁″垝鍙戝竷閲?VS 瀹為檯鍙戝竷閲?, 92], ["鍛ㄦ湡鏂囨浜у嚭鏁?, 84], ["鍓緫浜у嚭鏁?, 78], ["绉佷俊鍥炲澶勭悊閲?, 88], ["璇勮浜掑姩澶勭悊閲?, 76], ["璐﹀彿浼樺寲娆℃暟", 64], ["鍐呭杩唬浼樺寲娆℃暟", 72]];
  document.querySelector("#operation-progress").innerHTML = ops.map(([label, value]) => `<div><div><strong>${label}</strong><span>${value}%</span></div><i style="--p:${value}%"></i></div>`).join("");

  const risks = ["杩濊浣滃搧 1 鏉★紝寰呮暣鏀?, "1 涓处鍙锋挱鏀炬柇宕栦笅璺?, "1 涓处鍙烽暱鏈熸柇鏇翠紤鐪?, "楂樻帀绮夎处鍙烽璀?1 涓?];
  document.querySelector("#risk-list").innerHTML = risks.map((risk) => `<article>${risk}</article>`).join("");
  renderAnalyticsFromDatabase();
}

function renderAnalyticsFromDatabase() {
  const analytics = state.analytics || {};
  if (!Object.keys(analytics).length) return;
  const overview = analytics.overview || [];
  if (overview.length) {
    document.querySelector("#stats-overview").innerHTML = [
      { label: "鐭╅樀璐﹀彿鎬绘暟", value: state.summary?.accounts || 0, change: "+8.4%", trend: "up" },
      ...overview,
    ].map((item) => `<div class="metric client-metric"><span>${item.label}</span><strong>${item.value}</strong><em class="${item.trend || "up"}">${item.change || ""}</em></div>`).join("");
  }
  const accounts = (analytics.account_rank || []).map((item) => item.row).filter(Boolean);
  if (accounts.length) {
    const headers = ["璐﹀彿鍚嶇О", "骞冲彴", "鐘舵€?, "鎬绘挱鏀?, "鍛ㄦ湡鎾斁", "绮変笣", "澧炵矇", "瀹屾挱鐜?, "浜掑姩鐜?, "鏇存柊", "鍒嗗眰", "寮傚父"];
    document.querySelector("#account-stats-table").innerHTML = `<table><thead><tr>${headers.map((header) => `<th>${header}</th>`).join("")}</tr></thead><tbody>${accounts.map((row) => `<tr>${row.map((cell, index) => `<td>${index >= 10 && cell ? `<span class="chip">${cell}</span>` : cell || "-"}</td>`).join("")}</tr>`).join("")}</tbody></table><div class="table-pager">1 / 1 路 ${accounts.length} 鏉¤处鍙?/div>`;
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
    const restoreButton = setButtonLoading(button, "鍒濆鍖栦腑");
    stateNode.innerHTML = `<div class="muted">姝ｅ湪琛ラ綈鏁版嵁搴撳垵濮嬪寲鏁版嵁...</div>`;
    try {
      const result = await api("/api/system/initialize", { method: "POST", body: JSON.stringify({ password }) });
      const inserted = Object.entries(result.inserted || {}).map(([key, value]) => `${key}: ${value}`).join(" / ") || "鏃?;
      const skipped = Object.entries(result.skipped || {}).map(([key, value]) => `${key}: ${value}`).join(" / ") || "鏃?;
      stateNode.innerHTML = `<div><strong>${result.ok ? "鍒濆鍖栧畬鎴? : "鍒濆鍖栨湭瀹屾垚"}</strong><span>${result.seed_version || result.error || ""}</span></div><div><span>鏂板</span><strong>${inserted}</strong></div><div><span>璺宠繃</span><strong>${skipped}</strong></div>`;
      await refresh();
    } catch (error) {
      stateNode.innerHTML = `<div><strong>鍒濆鍖栧け璐?/strong><span>${error.message}</span></div>`;
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
        if (error) error.textContent = "璇疯緭鍏ヨ秴绾х鐞嗗憳瀵嗙爜";
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
    const restoreButton = setButtonLoading(button, "娓呯悊涓?..");
    if (stateNode) {
      stateNode.hidden = false;
      stateNode.textContent = "";
      stateNode.classList.remove("danger");
    }
    try {
      const result = await api("/api/system/supabase-read-cache/clear", { method: "POST" });
      if (stateNode) {
        if (result.cleared) {
          stateNode.textContent = "宸叉竻绌鸿繘绋嬪唴搴旂敤缂撳瓨锛屽悗缁х画璇锋眰灏嗛噸鏂版媺鍙栬繙绔暟鎹€?;
        } else {
          stateNode.textContent =
            result.backend === "sqlite"
              ? "褰撳墠鍝佺墝搴撲负 SQLite锛屾湭鍚敤搴旂敤缂撳瓨銆?
              : "鏈竻鐞嗙紦瀛樸€?";
        }
      }
    } catch (error) {
      if (stateNode) {
        stateNode.textContent = `娓呯悊澶辫触锛?{error.message}`;
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
      const restoreButton = setButtonLoading(button, "鎵撳紑涓?..");
      if (stateNode) {
        stateNode.textContent = `姝ｅ湪鎵撳紑锛?{label}`;
        stateNode.classList.remove("danger");
        stateNode.removeAttribute("title");
      }
      try {
        const result = await api(`/api/system/open-directory/${encodeURIComponent(button.dataset.systemDir)}`, { method: "POST" });
        if (stateNode) {
          stateNode.textContent = `宸叉墦寮€锛?{label}`;
          stateNode.title = result.path || "";
        }
      } catch (error) {
        if (stateNode) {
          stateNode.textContent = `鎵撳紑澶辫触锛?{error.message}`;
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
    ["寮€鍏?, status.enabled ? "寮€鍚? : "鍏抽棴"],
    ["杩愯涓?, status.running ? "鏄? : "鍚?],
    ["鍚庡彴绾跨▼", status.thread_alive ? "姝ｅ父" : "鏈繍琛?],
    ["瀹氭椂妯″紡", status.schedule_mode === "daily" ? "姣忓ぉ鍥哄畾鏃堕棿" : "鎸夐棿闅?],
    ["瀹氭椂鍙傛暟", status.schedule_mode === "daily" ? (status.daily_time || "09:00") : `${status.run_interval_minutes || 1440} 鍒嗛挓`],
    ["涓嬫鍚姩", formatTime(status.next_run_at)],
    ["涓婃鍚姩", formatTime(status.last_started_at)],
    ["涓婃瀹屾垚", formatTime(status.last_finished_at)],
    ["涓婃缁撴灉", status.last_ok === true ? "鎴愬姛" : status.last_ok === false ? "澶辫触" : "-"],
    ["鍙戝竷鏁伴噺", lastResult.count ?? "-"],
    ["涓婃宸℃", formatTime(status.last_login_check_at)],
    ["宸℃缁撴灉", status.last_login_check_ok === true ? "姝ｅ父" : status.last_login_check_ok === false ? "闇€鎵爜" : "-"],
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
        return `<button class="btn btn-sm ${enabled ? "primary" : "ghost"}" data-notice-route="${eventType}" data-notice-platform="${platform}" data-notice-enabled="${enabled ? "0" : "1"}">${aiPlatformLabel(platform)} ${enabled ? "寮€鍚? : "鍏抽棴"}</button>`;
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
            <p>${eventType}${subtypes ? ` 路 ${subtypes}` : ""}</p>
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
            <strong>寰呮壂鐮佹壒娆?${batch.batch_id}</strong>
            <p>${items.map((item) => `${item.display_name || item.account_key} / port ${item.debug_port}`).join("锛?) || "绛夊緟宸℃缁撴灉"}</p>
          </div>
          <time>${formatTime(batch.created_at)}</time>
        </article>
      `;
    }).join("") : `
      <article class="notification-card success">
        <span class="notification-dot"></span>
        <div><strong>鏆傛棤寰呮壂鐮佽棰戝彿</strong><p>鐧诲綍宸℃娌℃湁鍙戠幇闇€瑕佽繍钀ユ壂鐮佺殑璐﹀彿銆?/p></div>
        <time>瀹炴椂</time>
      </article>
    `;
  }
}

function aiPlatformLabel(platform) {
  return {
    wecom: "浼佷笟寰俊",
    dingtalk: "閽夐拤",
    lark: "椋炰功 / Lark",
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
  if (platform === "dingtalk") return "濉啓閽夐拤缇ゆ満鍣ㄤ汉 Webhook 鍦板潃锛屼繚瀛樺悗鍙嫭绔嬪紑鍚垨鍏抽棴閫氱煡銆?;
  if (platform === "lark") return "濉啓椋炰功缇ゆ満鍣ㄤ汉 Webhook 鍦板潃锛涗笅鏂瑰洖璋冨湴鍧€鐢ㄤ簬椋炰功浜嬩欢璁㈤槄 URL 楠岃瘉銆?;
  return "濉啓浼佷笟寰俊缇ゆ満鍣ㄤ汉 Webhook 鍦板潃锛屼繚瀛樺悗鍙嫭绔嬪紑鍚垨鍏抽棴閫氱煡銆?;
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
  if (modeTitle) modeTitle.textContent = isTelegram ? "Telegram 蹇€熼厤缃? : `${aiPlatformLabel(platform)} Webhook 閰嶇疆`;
  if (modeDesc) modeDesc.textContent = isTelegram ? "濉啓 Bot Token 骞惰幏鍙?Chat ID锛屼繚瀛樺悗鍙嫭绔嬪紑鍚垨鍏抽棴閫氱煡銆? : aiRobotWebhookHint(platform);
  if (isTelegram && !form.elements.bot_name.value) {
    form.elements.bot_name.value = "GasGx Telegram Bot";
  }
  if (isWebhookOnly && !form.elements.bot_name.value) {
    form.elements.bot_name.value = `${aiPlatformLabel(form.elements.platform.value)}鏈哄櫒浜篳;
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
  saveButton.classList.toggle("hidden", formHidden);
  sendTestButton.classList.toggle("hidden", formHidden);
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
  document.querySelector("#ai-config-state").textContent = configured.length && !editingPlatform ? "宸查厤缃? : (config.enabled ? "宸插惎鐢? : "鏈惎鐢?);
  renderBoundAiRobotPlatforms();
  document.querySelector("#ai-channel-grid").innerHTML = visibleAiRobotConfigs().map((item) => `
    <article class="bot-channel-card">
      ${aiRobotLogo(item.platform)}
      <div>
        <strong>${aiPlatformLabel(item.platform)}</strong>
        <p>${item.webhook_url ? "宸查厤缃? : "鏈厤缃?} 路 ${item.enabled ? "閫氱煡寮€鍚? : "閫氱煡鍏抽棴"} 路 ${item.has_signing_secret ? "楠岀瀵嗛挜宸蹭繚瀛? : "鏃犻渶楠岀瀵嗛挜"}</p>
      </div>
      <button class="btn secondary" type="button" data-ai-platform="${item.platform}">閰嶇疆</button>
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
  messageList.hidden = state.aiRobotMessagesCollapsed;
  if (messageToggle) {
    messageToggle.textContent = state.aiRobotMessagesCollapsed ? "灞曞紑" : "鏈€杩?100 鏉?;
  }
  messageList.innerHTML = state.aiRobotMessages.length
    ? state.aiRobotMessages.map((item) => `<article class="task-row">
        <div><strong>#${item.id} ${aiPlatformLabel(item.platform)}</strong><span>${item.summary || item.message_type}</span></div>
        <span class="task-status">${item.status}</span>
      </article>`).join("")
    : `<div class="muted">鏆傛棤鏈哄櫒浜烘秷鎭槦鍒椼€?/div>`;
}

function renderBoundAiRobotPlatforms() {
  const node = document.querySelector("#ai-bound-platforms");
  if (!node) return;
  const bound = state.aiRobotConfigs.filter(isAiRobotBound);
  if (!bound.length) {
    node.innerHTML = `<div class="bound-empty">杩樻病鏈夐厤缃秷鎭満鍣ㄤ汉銆備紒涓氬井淇°€侀拤閽夈€侀涔﹀～ Webhook 鍦板潃锛汿elegram 濉?Bot Token銆?/div>`;
    return;
  }
  node.innerHTML = bound.map((item) => `
    <article class="bound-platform-card">
      ${aiRobotLogo(item.platform)}
      <div>
        <strong>${aiPlatformLabel(item.platform)} 宸查厤缃?/strong>
        <p>${item.enabled ? "閫氱煡寮€鍚? : "閫氱煡鍏抽棴"} 路 ${item.target_id ? `鐩爣浼氳瘽 ${item.target_id}` : "Webhook 宸蹭繚瀛?} 路 鍙彂閫佹祴璇曟秷鎭?/p>
      </div>
      <div class="bound-platform-actions">
        <button class="notify-switch ${item.enabled ? "enabled" : ""}" type="button" data-ai-toggle="${item.platform}" aria-pressed="${item.enabled ? "true" : "false"}">
          <span></span><b>${item.enabled ? "閫氱煡寮€" : "閫氱煡鍏?}</b>
        </button>
        <button class="btn secondary" type="button" data-ai-test="${item.platform}">鍙戦€佹祴璇?/button>
        <button class="btn secondary" type="button" data-ai-edit="${item.platform}">淇敼</button>
        <button class="btn secondary danger" type="button" data-ai-delete="${item.platform}">鍒犻櫎</button>
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
      const restoreButton = setButtonLoading(button, config.enabled ? "鍏抽棴涓? : "寮€鍚腑");
      try {
        await api(`/api/ai-robots/${platform}/config`, {
          method: "PUT",
          body: JSON.stringify({
            enabled: !config.enabled,
            bot_name: config.bot_name || `${aiPlatformLabel(platform)}鏈哄櫒浜篳,
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
      if (!window.confirm(`纭鍒犻櫎 ${aiPlatformLabel(platform)} 鏈哄櫒浜洪厤缃紵鍒犻櫎鍚庨渶瑕侀噸鏂板～鍐?Bot Token銆俙)) return;
      const restoreButton = setButtonLoading(button, "鍒犻櫎涓?);
      try {
        await api(`/api/ai-robots/${platform}/config`, { method: "DELETE" });
        state.aiRobotConfigs = await api("/api/ai-robots/configs");
        state.aiRobotMessages = await api("/api/ai-robots/messages");
        state.aiRobotEditingPlatform = "";
        renderAiRobot();
        document.querySelector("#ai-config-state").textContent = "宸插垹闄?;
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
    toggle.textContent = localized ? "鑻辨枃鐗? : "涓枃鐗?;
    toggle.setAttribute("aria-pressed", String(localized));
  }
  const dictionary = state.databaseDictionary;
  if (!dictionary) {
    status.textContent = "鏈姞杞?;
    list.innerHTML = `<div class="muted">鏆傛棤鏁版嵁搴撳瓧鍏搞€?/div>`;
    return;
  }
  const tables = dictionary.tables || [];
  status.textContent = `${tables.length} 寮犺〃`;
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
            <small>${localized ? `${columns.length} 涓瓧娈礰 : `${columns.length} 瀛楁`}</small>
          </span>
          <span class="db-dictionary-table-badge">${expanded ? "鎶樺彔" : "灞曞紑"}</span>
        </button>
        <div class="db-dictionary-shell" ${expanded ? "" : 'hidden aria-hidden="true"'}>
          <div class="db-dictionary-toolbar">
            <span class="db-dictionary-toolbar-title">${localized ? "瀛楁鍒楄〃" : "Columns"}</span>
            <button class="db-dictionary-about" type="button" disabled>${localized ? "瀛楁绫诲瀷璇存槑" : "About data types"}</button>
          </div>
          <div class="db-dictionary-grid db-dictionary-grid-head" aria-hidden="true">
            <span>${localized ? "瀛楁鍚? : "Name"}</span>
            <span>${localized ? "绫诲瀷" : "Type"}</span>
            <span>${localized ? "榛樿鍊? : "Default Value"}</span>
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
    .join(" 路 ");
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
    renderDistributionSettings();
    renderMatrixJobStatus();
  } else if (view === "tasks") {
    await loadPlatforms();
    await loadAccounts();
    await loadTasks();
    renderTaskSelects();
    renderTasks();
  } else if (view === "terminal-execution") {
    state.terminalQrVisible = false;
    state.terminalExecution = await api("/api/terminal-execution/state");
    renderTerminalExecution();
    startTerminalPolling();
  } else if (view === "stats") {
    await loadAccounts();
    state.summary = await api("/api/summary");
    state.stats = await api("/api/stats");
    state.analytics = await api("/api/stats/analytics");
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
  form.elements["common.publish_mode"].value = common.publish_mode || "publish";
  form.elements["common.topics"].value = common.topics || "#澶╃劧姘?#澶╃劧姘斿彂鐢垫満缁?#鐕冩皵鍙戠數鏈虹粍 #娴峰鍙戠數 #娴峰鎸栫熆";
  form.elements["common.upload_timeout"].value = String(common.upload_timeout || 60);
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
}

function renderPlatformSettingsCard(platform) {
  const value = (state.distributionSettings.platforms || {})[platform.key] || {};
  const extra = platform.key === "wechat" ? `
    <label>鐭爣棰?
      <input name="platforms.${platform.key}.short_title" value="${value.short_title || "GasGx"}" placeholder="GasGx">
    </label>
    <label>浣嶇疆
      <input name="platforms.${platform.key}.location" value="${value.location || ""}" placeholder="鐣欑┖鍒欎笉鏄剧ず浣嶇疆">
    </label>
    <label>瑙嗛鍙峰悎闆?
      <select name="platforms.${platform.key}.collection_name">
        <option value="GasGx" ${value.collection_name === "GasGx" ? "selected" : ""}>GasGx</option>
        <option value="" ${!value.collection_name ? "selected" : ""}>涓嶉€夋嫨鍚堥泦</option>
      </select>
    </label>
    <label>鍘熷垱澹版槑
      <select name="platforms.${platform.key}.declare_original">
        <option value="false" ${!value.declare_original ? "selected" : ""}>涓嶅０鏄庡師鍒?/option>
        <option value="true" ${value.declare_original ? "selected" : ""}>澹版槑鍘熷垱</option>
      </select>
    </label>` : "";
  return `<article class="platform-settings-card" data-platform-card="${platform.key}">
    <div class="row-head">
      <strong>${platformName(platform.key)}</strong>
      <span class="chip">${platform.region === "cn" ? "鍥藉唴" : "鍥藉"}</span>
    </div>
    <label>鍚敤鍙戝竷閰嶇疆
      <select name="platforms.${platform.key}.enabled">
        <option value="true" ${value.enabled !== false ? "selected" : ""}>鍚敤</option>
        <option value="false" ${value.enabled === false ? "selected" : ""}>鍋滅敤</option>
      </select>
    </label>
    <label>鍐呭绫诲瀷
      <select name="platforms.${platform.key}.content_type">
        <option value="short_video" ${(value.content_type || "short_video") === "short_video" ? "selected" : ""}>鐭棰?/option>
        <option value="image_text" ${value.content_type === "image_text" ? "selected" : ""}>鍥炬枃</option>
        <option value="article" ${value.content_type === "article" ? "selected" : ""}>鏂囩珷</option>
      </select>
    </label>
    <label>鍙戝竷鏂瑰紡
      <select name="platforms.${platform.key}.publish_mode">
        <option value="inherit" ${(value.publish_mode || "inherit") === "inherit" ? "selected" : ""}>缁ф壙鍏ㄥ眬</option>
        <option value="publish" ${value.publish_mode === "publish" ? "selected" : ""}>绔嬪嵆鍙戝竷</option>
        <option value="draft" ${value.publish_mode === "draft" ? "selected" : ""}>淇濆瓨鑽夌</option>
      </select>
    </label>
    <label>鍙鑼冨洿
      <select name="platforms.${platform.key}.visibility">
        <option value="public" ${(value.visibility || "public") === "public" ? "selected" : ""}>鍏紑</option>
        <option value="private" ${value.visibility === "private" ? "selected" : ""}>浠呰嚜宸卞彲瑙?/option>
        <option value="friends" ${value.visibility === "friends" ? "selected" : ""}>濂藉弸/绮変笣鍙</option>
      </select>
    </label>
    <label>璇勮鏉冮檺
      <select name="platforms.${platform.key}.comment_permission">
        <option value="public" ${(value.comment_permission || "public") === "public" ? "selected" : ""}>鍏佽璇勮</option>
        <option value="closed" ${value.comment_permission === "closed" ? "selected" : ""}>鍏抽棴璇勮</option>
        <option value="followers" ${value.comment_permission === "followers" ? "selected" : ""}>浠呯矇涓濊瘎璁?/option>
      </select>
    </label>
    ${extra}
    <label class="wide-field">骞冲彴鏂囨
      <textarea name="platforms.${platform.key}.caption" rows="3" placeholder="鐣欑┖鍒欎娇鐢ㄨ棰戦粯璁ゆ枃妗?>${value.caption || ""}</textarea>
    </label>
  </article>`;
}

function collectDistributionSettings(form) {
  const data = new FormData(form);
  const common = {
    material_dir: data.get("common.material_dir") || "runtime/materials/videos",
    publish_mode: data.get("common.publish_mode") || "publish",
    topics: data.get("common.topics") || "#澶╃劧姘?#澶╃劧姘斿彂鐢垫満缁?#鐕冩皵鍙戠數鏈虹粍 #娴峰鍙戠數 #娴峰鎸栫熆",
    upload_timeout: Number(data.get("common.upload_timeout") || 60),
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
      platforms[platform].collection_name = data.get("platforms.wechat.collection_name") || "";
      platforms[platform].declare_original = data.get("platforms.wechat.declare_original") === "true";
      platforms[platform].short_title = data.get("platforms.wechat.short_title") || "GasGx";
      platforms[platform].location = data.get("platforms.wechat.location") || "";
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
  const match = String(account?.notes || "").match(/璐﹀彿鎵嬫満鍙凤細(\d{11})/);
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
  hint.innerHTML = `<svg class="account-phone-hint-icon" aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M10.3 3.7 1.9 18a2 2 0 0 0 1.7 3h16.8a2 2 0 0 0 1.7-3L13.7 3.7a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg><span>璇ユ墜鏈哄彿宸茬敤浜?${matches.map((account) => `#${account.id} ${account.display_name}`).join("銆?)}锛屼粛鍙户缁垱寤恒€?/span>`;
  hint.classList.add("warning");
}

function activateView(view, updateHash = true) {
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
      target.innerHTML = `<div class="muted">鍔犺浇澶辫触锛?{error.message}</div>`;
    });
  }
  if (updateHash && window.location.hash !== `#${view}`) {
    window.history.replaceState(null, "", `#${view}`);
  }
  window.scrollTo({ top: 0, left: 0 });
}

document.querySelectorAll(".nav-btn").forEach((button) => {
  button.addEventListener("click", () => activateView(button.dataset.view));
});

document.querySelector("#refresh")?.addEventListener("click", async (event) => {
  const restoreButton = setButtonLoading(event.currentTarget, "鍒锋柊涓?);
  try {
    state.aiRobotConfigs = await api("/api/ai-robots/configs");
    state.aiRobotMessages = await api("/api/ai-robots/messages");
    renderAiRobot();
    document.querySelector("#ai-config-state").textContent = "宸蹭繚瀛?;
  } finally {
    restoreButton();
  }
});

document.querySelector("#account-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "鍒涘缓涓?);
  const data = Object.fromEntries(new FormData(event.target).entries());
  const brandPrefix = String(data.brand_prefix || "").trim();
  const accountName = String(data.account_name || "").trim();
  const operatorWechat = String(data.operator_wechat || "").trim();
  const phone = String(data.phone || "").trim();
  updateAccountPhoneHint();
  if (!operatorWechat || operatorWechat === "__new__") {
    showTaskState("璇峰厛鍦ㄤ笅鎷変腑鏂板杩愯惀寰俊鍙?, "status-unsupported");
    restoreButton();
    return;
  }
  if (!/^\d{11}$/.test(phone)) {
    const phoneInput = event.target.elements.phone;
    phoneInput?.setCustomValidity("璐﹀彿鎵嬫満鍙烽渶涓?11 浣嶆暟瀛?);
    phoneInput?.reportValidity();
    phoneInput?.setCustomValidity("");
    restoreButton();
    return;
  }
  data.display_name = [brandPrefix, accountName].filter(Boolean).join(" ");
  data.account_key = makeAccountKey(data.display_name, "auto");
  data.niche = "鐭棰戠煩闃?;
  data.notes = `缁戝畾杩愯惀寰俊锛?{operatorWechat}锛涜处鍙锋墜鏈哄彿锛?{phone}`;
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
  const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "鍔犲叆涓?);
  const data = Object.fromEntries(new FormData(event.target).entries());
  data.account_id = data.account_id ? Number(data.account_id) : null;
  showTaskState("鍔犲叆闃熷垪涓?..");
  try {
    await api("/api/tasks", { method: "POST", body: JSON.stringify(data) });
    showTaskState("宸插姞鍏ラ槦鍒椼€?);
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

document.querySelector("#terminal-config-list")?.addEventListener("change", (event) => {
  const row = event.target.closest("[data-terminal-config-row]");
  if (!row) return;
  const enabled = Boolean(row.querySelector("[data-terminal-enabled]")?.checked);
  row.classList.toggle("disabled", !enabled);
  row.querySelector("[data-terminal-operator]")?.toggleAttribute("disabled", !enabled);
  row.querySelectorAll("[data-terminal-color]").forEach((button) => button.toggleAttribute("disabled", !enabled));
});

document.querySelector("#terminal-config-list")?.addEventListener("click", (event) => {
  const swatch = event.target.closest("[data-terminal-color]");
  if (!swatch || swatch.disabled) return;
  const row = swatch.closest("[data-terminal-config-row]");
  row?.querySelectorAll("[data-terminal-color]").forEach((button) => button.classList.remove("active"));
  swatch.classList.add("active");
});

document.querySelector("#terminal-start-login")?.addEventListener("click", async (event) => {
  const restoreButton = setButtonLoading(event.currentTarget, "鍚姩涓?);
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
    startTerminalPolling();
  } finally {
    restoreButton();
  }
});

document.querySelector("#terminal-edit-config")?.addEventListener("click", () => {
  state.terminalConfigOpen = true;
  renderTerminalExecution();
});

document.querySelector("#distribution-settings-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const restoreButton = setButtonLoading(event.submitter || event.target.querySelector('button[type="submit"]'), "淇濆瓨涓?);
  const stateNode = document.querySelector("#settings-save-state");
  stateNode.textContent = "淇濆瓨涓?..";
  try {
    await api("/api/settings/distribution", {
      method: "PATCH",
      body: JSON.stringify(collectDistributionSettings(event.target)),
    });
    stateNode.textContent = "宸蹭繚瀛橈紝涓嬩竴娆＄煩闃靛垎鍙戜細鎸夊叏灞€閰嶇疆鍜屽钩鍙扮嫭绔嬮厤缃墽琛屻€?;
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
  stateNode.textContent = "淇濆瓨涓?..";
  stateNode.classList.remove("danger");
  let saved = false;
  const restoreButton = setButtonLoading(button, "淇濆瓨涓?);
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
      data.bot_name = data.bot_name || `${aiPlatformLabel(platform)}鏈哄櫒浜篳;
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
    stateNode.textContent = "宸蹭繚瀛?;
    saved = true;
  } catch (error) {
    stateNode.textContent = error.message || "淇濆瓨澶辫触";
    stateNode.classList.add("danger");
  } finally {
    restoreButton();
    if (saved) {
      button.textContent = "宸蹭繚瀛?;
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
    event.currentTarget.textContent = "宸插鍒?;
  } catch {
    input.select();
    document.execCommand("copy");
    event.currentTarget.textContent = "宸插鍒?;
  }
});

async function sendAiRobotTest(platform, button) {
  const form = document.querySelector("#ai-robot-form");
  const stateNode = document.querySelector("#ai-config-state");
  const text = form && !form.hidden && form.elements.platform.value === platform
    ? (form.elements.test_text.value || "GasGx AI robot test message")
    : "GasGx AI robot test message";
  stateNode.textContent = "鍙戦€佷腑...";
  stateNode.classList.remove("danger");
  let finalButtonText = "";
  const restoreButton = setButtonLoading(button, "鍙戦€佷腑");
  try {
    const result = await api(`/api/ai-robots/${platform}/test-message`, {
      method: "POST",
      body: JSON.stringify({ message_type: "text", text }),
    });
    state.aiRobotConfigs = await api("/api/ai-robots/configs");
    state.aiRobotMessages = await api("/api/ai-robots/messages");
    renderAiRobot();
    if (result.status === "sent") {
      stateNode.textContent = "娴嬭瘯娑堟伅宸插彂閫?;
      finalButtonText = "宸插彂閫?;
      return;
    }
    stateNode.textContent = `鍙戦€佸け璐ワ細${result.error || result.summary || result.status || "鏈煡閿欒"}`;
    stateNode.classList.add("danger");
    finalButtonText = "鍙戦€佸け璐?;
  } catch (error) {
    stateNode.textContent = `鍙戦€佸け璐ワ細${error.message || "鏈煡閿欒"}`;
    stateNode.classList.add("danger");
    finalButtonText = "鍙戦€佸け璐?;
  } finally {
    restoreButton();
    if (finalButtonText) {
      button.textContent = finalButtonText;
    }
  }
}

function renderAiRobotLoading() {
  const loading = `<div class="loading-inline"><span class="btn-spinner" aria-hidden="true"></span><span>鍔犺浇涓?..</span></div>`;
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
  const restoreButton = setButtonLoading(button, "鎵撳紑涓?..");
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

document.querySelector("#matrix-run-now").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const confirmed = await confirmMatrixRunNow();
  if (!confirmed) return;
  const restoreButton = setButtonLoading(button, "鍚姩涓?);
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

document.addEventListener("click", async (event) => {
  const routeButton = event.target.closest("[data-notice-route]");
  if (routeButton) {
    const eventType = routeButton.dataset.noticeRoute;
    const platform = routeButton.dataset.noticePlatform;
    const enabled = routeButton.dataset.noticeEnabled === "1";
    const restoreButton = setButtonLoading(routeButton, "淇濆瓨涓?);
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
    const restoreButton = setButtonLoading(deleteButton, "鍒犻櫎涓?);
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
    const restoreButton = setButtonLoading(bulkStatusButton, "璋冩暣涓?);
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
    if (!window.confirm(`纭鍒犻櫎宸查€?${ids.length} 鏉￠槦鍒椾换鍔★紵`)) return;
    const restoreButton = setButtonLoading(bulkDeleteButton, "鍒犻櫎涓?);
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
    const restoreButton = setButtonLoading(terminalManualButton, "瑙﹀彂涓?);
    try {
      state.terminalExecution = await api(`/api/terminal-execution/windows/${terminalManualButton.dataset.terminalManual}/manual-publish`, { method: "POST" });
      renderTerminalExecution();
    } finally {
      restoreButton();
    }
    return;
  }

  const deleteAccountButton = event.target.closest("[data-delete-account]");
  if (deleteAccountButton) {
    const accountId = deleteAccountButton.dataset.deleteAccount;
    const accountName = deleteAccountButton.dataset.accountName || `#${accountId}`;
    if (!window.confirm(`纭鍒犻櫎鐭╅樀璐﹀彿銆?{accountName}銆嶏紵鐩稿叧骞冲彴銆佹祻瑙堝櫒閰嶇疆鍜屼换鍔¤褰曚細涓€骞跺垹闄ゃ€俙)) return;
    const restoreButton = setButtonLoading(deleteAccountButton, "鍒犻櫎涓?..");
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
  const successText = `${platformLabel(platform)}宸叉墦寮€`;
  target.disabled = true;
  target.classList.add("loading");
  target.innerHTML = `<span class="btn-spinner" aria-hidden="true"></span><span>鎵撳紑涓?/span>`;
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
  document.querySelector("#summary").innerHTML = `<div class="metric"><span>鍔犺浇澶辫触</span><strong>${error.message}</strong></div>`;
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
  ["name", "妯℃澘鍚嶇О", "text"], ["brand", "鍝佺墝鏂囧瓧", "text"], ["eyebrow", "鐪夋爣鏂囧瓧", "text"], ["cta", "CTA 鎸夐挳鏂囧瓧", "text"],
  ["align", "瀵归綈鏂瑰紡", "select"], ["brand_y", "鍝佺墝 Y", "range", 0, 420], ["headline_y", "涓绘爣棰?Y", "range", 0, 1320],
  ["subhead_y", "鍓爣棰?Y", "range", 0, 1500], ["hud_y", "HUD Y", "range", 0, 1780], ["cta_y", "CTA Y", "range", 0, 1840],
  ["primary_color", "涓绘枃瀛楅鑹?, "color"], ["secondary_color", "杈呭姪鏂囧瓧棰滆壊", "color"], ["accent_color", "寮鸿皟鑹?, "color"],
  ["tint_color", "搴曡壊", "color"], ["gradient_color", "娓愬彉鑹?, "color"], ["panel_color", "HUD 鑳屾櫙鑹?, "color"],
  ["tint_opacity", "搴曡壊閫忔槑搴?, "rangeFloat", 0, 1], ["gradient_opacity", "娓愬彉閫忔槑搴?, "rangeFloat", 0, 1],
  ["panel_opacity", "HUD 鑳屾櫙閫忔槑搴?, "rangeFloat", 0, 1],
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
  renderVmRadio("vm-language-group", "vm_copy_language", [["zh", "涓枃"], ["en", "鑻辨枃"], ["ru", "淇勬枃"]], vm.state.copy_language || "zh");
  renderVideoMatrixBgm(data);
  vmNode("video-matrix-save-state").onclick = saveVideoMatrixState;
}

function renderVideoMatrixSource(data) {
  const total = Object.values(data.category_counts).reduce((sum, value) => sum + value, 0);
  const categories = vmMaterialCategories(data);
  vmNode("video-matrix-metrics").innerHTML = [
    `<div class="metric"><span>鏈湴绱犳潗</span><strong>${total}</strong></div>`,
    `<div class="metric"><span>鐢熸垚鏁伴噺</span><strong id="vm-metric-count">${vmNode("vm-output-count").value}</strong></div>`,
    `<div class="metric"><span>骞惰绾跨▼</span><strong id="vm-metric-workers">${vmNode("vm-max-workers").value}</strong></div>`,
    `<div class="metric"><span>榛樿姣斾緥</span><strong>1080:1920</strong></div>`,
  ].join("");
  vmNode("vm-source-dirs").innerHTML = categories.map((category) => `
    <div class="vm-dir-row"><span class="vm-badge">${vmEscape(category.label)}</span><code>${vmEscape(data.source_dirs[category.id] || "")}</code><button class="btn primary" data-vm-open="${vmEscape(data.source_dirs[category.id] || "")}">閹垫挸绱?/button></div>
  `).join("");
  vmNode("vm-source-dirs").querySelectorAll("[data-vm-open]").forEach((button) => { button.onclick = () => vmOpenFolder(button.dataset.vmOpen); });
  vmNode("vm-source-counts").textContent = `瑜版挸澧犵槐鐘虫綏閺佷即鍣洪敍${categories.map((category) => `${category.label}=${data.category_counts[category.id] || 0}`).join(" / ")}`;
  renderVmRadio("vm-source-mode-group", "vm_source_mode", [["Category folders", "閸掑棛琚惄顔肩秿"], ["Upload files", "閹靛濮╂稉濠佺炊"]], vm.state.source_mode || "Category folders", updateVideoMatrixSourceMode);
  vmNode("vm-recent-limits").innerHTML = categories.map((category) => `
    <label>${vmEscape(category.label)} 缁槒顕伴崣鏍ㄦ付閺傛壆绀岄弶?input id="vm-${category.id}" type="range" min="1" max="50" value="${vm.settings.recent_limits[category.id] || 8}"><strong id="vm-${category.id}-value"></strong></label>
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
  const fields = [`<h3>褰撳墠妯℃澘鐙珛缂栬緫鍖?/h3>`];
  vmCoverFields.forEach(([key, label, type, min, max]) => {
    const value = template[key] ?? "";
    if (type === "select") fields.push(`<label>${label}<select data-vm-key="${key}"><option value="left">left</option><option value="center">center</option></select></label>`);
    else if (type === "range" || type === "rangeFloat") fields.push(`<label>${label}<input data-vm-key="${key}" type="range" min="${min}" max="${max}" step="${type === "rangeFloat" ? "0.01" : "1"}" value="${value}"><strong>${value}</strong></label>`);
    else fields.push(`<label>${label}<input data-vm-key="${key}" type="${type}" value="${vmEscape(value)}"></label>`);
  });
  fields.push(`<button class="btn primary" type="button" id="vm-save-cover">淇濆瓨杩欎釜灏侀潰妯℃澘</button>`);
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
  vmLog(`宸蹭繚瀛樺皝闈㈡ā鏉匡細${vm.coverTemplates[vm.selectedCover].name || vm.selectedCover}`);
}

async function saveVideoMatrixState() {
  vm.state = collectVideoMatrixState();
  await vmApi("/state", { method: "POST", body: JSON.stringify(vm.state) });
  vmLog("宸蹭繚瀛樺綋鍓嶈缃?);
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
  if (job.status === "complete") vmLog(`瀹屾垚\n${job.assets.map((asset) => asset.video_path).join("\n")}`);
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
    { id: "category_A", label: "A 绫? },
    { id: "category_B", label: "B 绫? },
    { id: "category_C", label: "C 绫? },
  ];
}

function renderVideoMatrixBgm(data) {
  vmNode("vm-bgm-panel").innerHTML = `<div class="radio-line" id="vm-bgm-source-group"></div><select id="vm-bgm-library"></select><input id="vm-bgm-upload" type="file" accept=".mp3,.wav,.m4a"><div class="muted">${Object.values(data.bgm_library || {}).map((item) => `<a href="${item.download_page}" target="_blank">${item.name}</a>`).join("<br>")}</div>`;
  renderVmRadio("vm-bgm-source-group", "vm_bgm_source", [["Upload file", "涓婁紶鏂囦欢"], ["Local library", "鏈湴闊充箰搴?]], vm.state.bgm_source || "Upload file", updateVideoMatrixBgmMode);
  vmNode("vm-bgm-library").innerHTML = data.local_bgm.map((name) => `<option>${name}</option>`).join("");
  vmNode("vm-bgm-library").value = vm.state.bgm_library_id || "";
  updateVideoMatrixBgmMode();
}
function updateVideoMatrixBgmMode() { const local = vmRadioValue("vm_bgm_source") === "Local library"; vmNode("vm-bgm-library").classList.toggle("hidden", !local); vmNode("vm-bgm-upload").classList.toggle("hidden", local); }
function updateVideoMatrixSourceMode() { vmNode("vm-upload-sources-wrap").classList.toggle("hidden", vmRadioValue("vm_source_mode") !== "Upload files"); }
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
  section.innerHTML = `<iframe class="video-matrix-frame" src="/static/video_matrix.html?embed=1" title="GasGx 瑙嗛鐢熸垚宸ヤ綔鍙?></iframe>`;
  section.querySelector(".video-matrix-frame")?.addEventListener("load", () => {
    const theme = SHELL_THEMES.find((item) => item.id === localStorage.getItem(SHELL_THEME_KEY)) || SHELL_THEMES[0];
    broadcastShellTheme(theme);
  });
}

document.querySelector('[data-view="video-matrix"]').addEventListener("click", mountVideoMatrixWorkbench);
window.addEventListener("load", () => {
  const requestedView = window.location.hash.replace("#", "");
  if (requestedView) {
    activateView(requestedView, false);
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 50);
    setTimeout(() => window.scrollTo({ top: 0, left: 0 }), 300);
  }
});

