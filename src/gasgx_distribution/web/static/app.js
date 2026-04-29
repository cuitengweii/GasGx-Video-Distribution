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
  aiRobotEditingPlatform: "",
  aiRobotMessagesCollapsed: true,
  brand: { settings: {} },
  systemHealth: null,
};

const SHELL_THEME_KEY = "gasgx-shell-theme";
const SHELL_BRAND_KEY = "gasgx-shell-brand";

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
];

const VIEW_HEADERS = {
  overview: ["账号矩阵维护系统", "独立账号、独立浏览器、发布/评论/私信/统计任务入口"],
  accounts: ["账号矩阵", "维护 GasGx 国内外平台账号、独立浏览器配置和登录状态。"],
  "user-center": ["用户中心", "预留操作者资料、角色权限、工作偏好和本地部署身份入口。"],
  settings: ["公共设置", "配置发布素材目录、上传策略、平台参数和矩阵发布作业。"],
  tasks: ["任务中心", "查看发布、评论、私信、登录检测等任务队列和执行状态。"],
  stats: ["数据统计", "短视频账号矩阵数字化营销客户端数据看板。"],
  "ai-robot": ["AI机器人", "AI客服、企业微信、钉钉、飞书、Telegram 与 WhatsApp 统一接入。"],
  "video-matrix": ["视频生成", "分类素材、第一屏封面、视频文字、背景音乐和批量导出工作台。"],
  notifications: ["通知中心", "集中展示生成完成、发布失败、登录失效和素材不足提醒。"],
  "system-settings": ["系统设置", "预留本地部署、存储缓存、安全策略和系统维护入口。"],
  "help-center": ["帮助文档", "预留操作手册、部署说明、视频生成流程和常见问题。"],
};

function setViewHeader(view) {
  const [title, description] = VIEW_HEADERS[view] || VIEW_HEADERS.overview;
  document.querySelector("#page-title").textContent = title;
  document.querySelector("#page-description").textContent = description;
  document.querySelector("#refresh").classList.toggle("hidden", view === "video-matrix");
}

function applyShellTheme(themeId) {
  const theme = SHELL_THEMES.find((item) => item.id === themeId) || SHELL_THEMES[0];
  document.documentElement.style.setProperty("--accent-aurora", theme.accent);
  document.documentElement.style.setProperty("--accent-soft", theme.soft);
  localStorage.setItem(SHELL_THEME_KEY, theme.id);
  document.querySelectorAll(".theme-card").forEach((card) => {
    card.classList.toggle("active", card.dataset.themeId === theme.id);
  });
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
  document.querySelector("#brand-slogan").textContent = next.slogan;
  document.querySelector("#brand-preview-name").textContent = next.name;
  document.querySelector("#brand-preview-slogan").textContent = next.slogan;
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
  document.querySelector("#save-brand-settings").addEventListener("click", async () => {
    const currentLogo = document.querySelector("#brand-logo-image").src || "";
    const payload = {
      name: nameInput.value,
      slogan: sloganInput.value,
      logo_asset_path: currentLogo.startsWith("data:") ? currentLogo : "",
      primary_color: getComputedStyle(document.documentElement).getPropertyValue("--accent-aurora").trim() || "#5dd62c",
      theme_id: localStorage.getItem(SHELL_THEME_KEY) || "gasgx-green",
      default_account_prefix: nameInput.value || "GasGx",
    };
    const settings = await api("/api/brand", { method: "PATCH", body: JSON.stringify(payload) });
    saveShellBrand({ name: settings.name, slogan: settings.slogan, logoDataUrl: settings.logo_asset_path || "" });
  });
  document.querySelector("#reset-brand-settings").addEventListener("click", () => {
    localStorage.removeItem(SHELL_BRAND_KEY);
    upload.value = "";
    applyShellBrand({});
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

function initUserMenu() {
  const toggle = document.querySelector("#user-menu-toggle");
  const menu = document.querySelector("#sidebar-user-actions");
  if (!toggle || !menu) return;
  toggle.addEventListener("click", () => {
    const open = menu.classList.toggle("hidden") === false;
    toggle.setAttribute("aria-expanded", String(open));
  });
  document.addEventListener("click", (event) => {
    if (toggle.contains(event.target) || menu.contains(event.target)) return;
    menu.classList.add("hidden");
    toggle.setAttribute("aria-expanded", "false");
  });
  document.querySelectorAll("[data-quick-view]").forEach((button) => {
    button.addEventListener("click", () => {
      activateView(button.dataset.quickView);
      menu.classList.add("hidden");
      toggle.setAttribute("aria-expanded", "false");
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

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || response.statusText);
  }
  return response.json();
}

function setButtonLoading(button, loadingText = "处理中") {
  if (!button) return () => {};
  const previousHtml = button.innerHTML;
  const previousDisabled = button.disabled;
  button.disabled = true;
  button.classList.add("loading");
  button.innerHTML = `<span class="btn-spinner" aria-hidden="true"></span><span>${loadingText}</span>`;
  return () => {
    button.innerHTML = previousHtml;
    button.disabled = previousDisabled;
    button.classList.remove("loading");
  };
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
    <div class="chips">
      ${items.map((p) => `<span class="chip platform-chip">${platformIcon(p.platform)}<span>${platformLabel(p.platform)} · ${p.login_status}</span></span>`).join("")}
    </div>
    <div class="browser-actions">
      ${items.map((p) => `<button class="btn secondary" data-open="${p.account_id}:${p.platform}">${platformIcon(p.platform)}<span>${platformLabel(p.platform)}浏览器</span></button>`).join("")}
    </div>
  </div>`;
}

function renderAccounts() {
  document.querySelector("#accounts-list").innerHTML = state.accounts.map((account) => {
    const platforms = account.platforms || [];
    return `<article class="account-row">
      <div class="row-head">
        <div class="account-title-wrap">
          <strong class="account-title">#${account.id} ${account.display_name}</strong>
          <div class="account-subtitle">${account.account_key} · ${account.niche || ""}</div>
        </div>
        <div class="account-badges">
          <span class="chip">${account.status}</span>
          <span class="chip success-chip">发布成功 ${account.publish_success_count || 0}</span>
        </div>
      </div>
      ${renderPlatformStatusGroup(platforms, "cn")}
      ${renderPlatformStatusGroup(platforms, "global")}
    </article>`;
  }).join("") || `<div class="muted">暂无账号</div>`;
}

function renderTasks() {
  document.querySelector("#tasks-list").innerHTML = state.tasks.map((task) => {
    return `<article class="task-row">
      <div class="row-head">
        <strong>#${task.id} ${task.task_type} ${platformName(task.platform)}</strong>
        <span class="task-actions">
          <span class="status-${task.status}">${task.status}</span>
          <button class="btn secondary task-delete" data-delete-task="${task.id}" type="button">删除队列</button>
        </span>
      </div>
      <div class="muted">${task.summary || task.error || ""}</div>
    </article>`;
  }).join("") || `<div class="muted">暂无任务</div>`;
}

function renderStats() {
  const summary = state.summary || {};
  const overview = [
    ["矩阵账号总数", summary.accounts || 4, "+8.4%", "up"],
    ["新增账号数", 2, "+100%", "up"],
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
  ].map(([label, value]) => `<div class="job-status-item"><span>${label}</span><strong>${value}</strong></div>`).join("");
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
  document.querySelector("#ai-config-state").textContent = configured.length && !editingPlatform ? "已配置" : (config.enabled ? "已启用" : "未启用");
  renderBoundAiRobotPlatforms();
  document.querySelector("#ai-channel-grid").innerHTML = visibleAiRobotConfigs().map((item) => `
    <article class="bot-channel-card">
      <span class="bot-logo ${item.platform}">${aiPlatformLabel(item.platform).slice(0, 1)}</span>
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
      <span class="bot-logo ${item.platform}">${aiPlatformLabel(item.platform).slice(0, 1)}</span>
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

function renderSystemHealth() {
  const status = document.querySelector("#supabase-health-state");
  const list = document.querySelector("#supabase-health-list");
  const meta = document.querySelector("#supabase-health-meta");
  if (!status || !list || !meta) return;
  const health = state.systemHealth;
  if (!health) {
    status.textContent = "未加载";
    list.innerHTML = `<div class="muted">暂无 Supabase 健康检查结果。</div>`;
    meta.textContent = "";
    return;
  }
  status.textContent = health.ok ? "正常" : "异常";
  status.classList.toggle("danger", !health.ok);
  meta.textContent = `品牌 ${health.brand_id || "-"} · App ${health.app_version || "-"} · Schema ${health.schema_version || "-"}`;
  list.innerHTML = (health.checks || []).map((item) => `
    <div class="health-row ${item.ok ? "ok" : "fail"}">
      <span>${item.name}</span>
      <strong>${item.ok ? "通过" : "失败"}</strong>
      <small>${item.ok ? healthDetailText(item.details || {}) : item.error || "未知错误"}</small>
    </div>
  `).join("");
}

function healthDetailText(details) {
  return Object.entries(details)
    .map(([key, value]) => `${key}: ${value}`)
    .join(" · ");
}

async function refresh() {
  renderAiRobotLoading();
  const [brand, platforms, accounts, tasks, distributionSettings, matrixJobStatus, aiRobotConfigs, aiRobotMessages] = await Promise.all([
    api("/api/brand"),
    api("/api/platforms"),
    api("/api/accounts"),
    api("/api/tasks"),
    api("/api/settings/distribution"),
    api("/api/jobs/matrix-wechat/status"),
    api("/api/ai-robots/configs"),
    api("/api/ai-robots/messages"),
  ]);
  Object.assign(state, { brand, platforms, accounts, tasks, distributionSettings, matrixJobStatus, aiRobotConfigs, aiRobotMessages });

  const optional = await Promise.allSettled([
    api("/api/summary"),
    api("/api/stats"),
    api("/api/system/supabase-health"),
  ]);
  if (optional[0].status === "fulfilled") state.summary = optional[0].value;
  if (optional[1].status === "fulfilled") state.stats = optional[1].value;
  if (optional[2].status === "fulfilled") state.systemHealth = optional[2].value;
  applyServerBrand(brand);
  renderSummary();
  renderPlatforms();
  renderTaskSelects();
  renderDistributionSettings();
  renderMatrixJobStatus();
  renderAccounts();
  renderTasks();
  renderStats();
  renderAiRobot();
  renderSystemHealth();
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
  form.elements["common.topics"].value = common.topics || "#天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿";
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
    <label>短标题
      <input name="platforms.${platform.key}.short_title" value="${value.short_title || "GasGx"}" placeholder="GasGx">
    </label>
    <label>位置
      <input name="platforms.${platform.key}.location" value="${value.location || ""}" placeholder="留空则不显示位置">
    </label>
    <label>视频号合集
      <select name="platforms.${platform.key}.collection_name">
        <option value="赛博皮卡天津港现车" ${value.collection_name === "赛博皮卡天津港现车" ? "selected" : ""}>赛博皮卡天津港现车</option>
        <option value="赛博皮卡现车：aawbcc" ${value.collection_name === "赛博皮卡现车：aawbcc" ? "selected" : ""}>赛博皮卡现车：aawbcc</option>
        <option value="" ${!value.collection_name ? "selected" : ""}>不选择合集</option>
      </select>
    </label>
    <label>原创声明
      <select name="platforms.${platform.key}.declare_original">
        <option value="false" ${!value.declare_original ? "selected" : ""}>不声明原创</option>
        <option value="true" ${value.declare_original ? "selected" : ""}>声明原创</option>
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
        <option value="short_video" ${(value.content_type || "short_video") === "short_video" ? "selected" : ""}>短视频</option>
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
        <option value="public" ${(value.visibility || "public") === "public" ? "selected" : ""}>公开</option>
        <option value="private" ${value.visibility === "private" ? "selected" : ""}>仅自己可见</option>
        <option value="friends" ${value.visibility === "friends" ? "selected" : ""}>好友/粉丝可见</option>
      </select>
    </label>
    <label>评论权限
      <select name="platforms.${platform.key}.comment_permission">
        <option value="public" ${(value.comment_permission || "public") === "public" ? "selected" : ""}>允许评论</option>
        <option value="closed" ${value.comment_permission === "closed" ? "selected" : ""}>关闭评论</option>
        <option value="followers" ${value.comment_permission === "followers" ? "selected" : ""}>仅粉丝评论</option>
      </select>
    </label>
    ${extra}
    <label class="wide-field">平台文案
      <textarea name="platforms.${platform.key}.caption" rows="3" placeholder="留空则使用视频默认文案">${value.caption || ""}</textarea>
    </label>
  </article>`;
}

function collectDistributionSettings(form) {
  const data = new FormData(form);
  const common = {
    material_dir: data.get("common.material_dir") || "runtime/materials/videos",
    publish_mode: data.get("common.publish_mode") || "publish",
    topics: data.get("common.topics") || "#天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿",
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

function activateView(view, updateHash = true) {
  const button = document.querySelector(`.nav-btn[data-view="${view}"]`);
  const section = document.querySelector(`#${view}`);
  if (!button || !section) return;
  document.querySelectorAll(".nav-btn").forEach((item) => item.classList.remove("active"));
  document.querySelectorAll(".view").forEach((item) => item.classList.remove("active"));
  button.classList.add("active");
  section.classList.add("active");
  document.body.classList.toggle("video-matrix-active", view === "video-matrix");
  setViewHeader(view);
  if (view === "video-matrix") {
    mountVideoMatrixWorkbench();
  }
  if (updateHash && window.location.hash !== `#${view}`) {
    window.history.replaceState(null, "", `#${view}`);
  }
  window.scrollTo({ top: 0, left: 0 });
}

document.querySelectorAll(".nav-btn").forEach((button) => {
  button.addEventListener("click", () => activateView(button.dataset.view));
});

document.querySelector("#refresh").addEventListener("click", async (event) => {
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
  data.display_name = [brandPrefix, accountName].filter(Boolean).join(" ");
  data.account_key = makeAccountKey(data.display_name, data.account_suffix);
  delete data.brand_prefix;
  delete data.account_name;
  delete data.account_suffix;
  data.notes = "";
  data.platforms = PLATFORM_ORDER;
  try {
    await api("/api/accounts", { method: "POST", body: JSON.stringify(data) });
    event.target.reset();
    await refresh();
  } finally {
    restoreButton();
  }
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
    showTaskState(error.message, "status-unsupported");
  } finally {
    restoreButton();
  }
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

document.querySelector("#ai-save-config").addEventListener("click", async (event) => {
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
  document.querySelector("#ai-save-config")?.click();
});

document.querySelector("#ai-send-test-panel")?.addEventListener("click", () => {
  document.querySelector("#ai-send-test")?.click();
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

document.querySelector("#ai-send-test").addEventListener("click", async (event) => {
  const form = document.querySelector("#ai-robot-form");
  await sendAiRobotTest(form.elements.platform.value, event.currentTarget);
});

document.querySelector("#open-material-dir").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const form = document.querySelector("#distribution-settings-form");
  const materialDir = form.elements["common.material_dir"].value || "runtime/materials/videos";
  const restoreButton = setButtonLoading(button, "修改中");
  try {
    await api("/api/settings/material-dir/open", {
      method: "POST",
      body: JSON.stringify({ material_dir: materialDir }),
    });
  } finally {
    restoreButton();
  }
});

document.querySelector("#matrix-run-now").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const confirmed = window.confirm("确认立即启动一次矩阵发布作业？系统会按当前设置为可用账号分配素材并执行发布。");
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
  api("/api/jobs/matrix-wechat/status")
    .then((matrixJobStatus) => {
      state.matrixJobStatus = matrixJobStatus;
      renderMatrixJobStatus();
    })
    .catch(() => {});
}, 15000);

document.addEventListener("click", async (event) => {
  const deleteButton = event.target.closest("[data-delete-task]");
  if (deleteButton) {
    const taskId = deleteButton.dataset.deleteTask;
    const restoreButton = setButtonLoading(deleteButton, "删除中");
    try {
      await api(`/api/tasks/${taskId}`, { method: "DELETE" });
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
initUserMenu();


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
  form.append("payload", JSON.stringify({ ...collectVideoMatrixState(), transcript_text: vmNode("vm-transcript-text").value }));
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
    source_mode: vmRadioValue("vm_source_mode"), use_live_data: true, headline: vmNode("vm-headline").value, subhead: vmNode("vm-subhead").value,
    cta: vmNode("vm-cta").value, follow_text: vmNode("vm-follow-text").value, hud_text: vmNode("vm-hud-text").value,
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
  section.innerHTML = `<iframe class="video-matrix-frame" src="/static/video_matrix.html?embed=1" title="GasGx 视频生成工作台"></iframe>`;
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
