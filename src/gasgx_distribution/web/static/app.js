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
};

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

function platformLabel(key) {
  return PLATFORM_LABELS[key] || key;
}

function metric(label, value) {
  return `<div class="metric"><span>${label}</span><strong>${value}</strong></div>`;
}

function renderSummary() {
  const s = state.summary;
  document.querySelector("#summary").innerHTML = [
    metric("账号", s.accounts || 0),
    metric("平台槽位", s.platforms || 0),
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
          <div class="row-head"><strong>${platformLabel(item.key)}</strong><span>${item.region === "cn" ? "国内" : "国外"}</span></div>
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
      ${items.map((p) => `<span class="chip">${platformLabel(p.platform)} · ${p.login_status}</span>`).join("")}
    </div>
    <div class="browser-actions">
      ${items.map((p) => `<button class="btn secondary" data-open="${p.account_id}:${p.platform}">${platformLabel(p.platform)}浏览器</button>`).join("")}
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
        <span class="chip">${account.status}</span>
      </div>
      ${renderPlatformStatusGroup(platforms, "cn")}
      ${renderPlatformStatusGroup(platforms, "global")}
    </article>`;
  }).join("") || `<div class="muted">暂无账号</div>`;
}

function renderTasks() {
  document.querySelector("#tasks-list").innerHTML = state.tasks.map((task) => {
    return `<article class="task-row">
      <div class="row-head"><strong>#${task.id} ${task.task_type} ${platformLabel(task.platform) || ""}</strong><span class="status-${task.status}">${task.status}</span></div>
      <div class="muted">${task.summary || task.error || ""}</div>
    </article>`;
  }).join("") || `<div class="muted">暂无任务</div>`;
}

function renderStats() {
  document.querySelector("#stats-list").innerHTML = state.stats.map((item) => {
    return `<article class="stat-row">
      <div class="row-head"><strong>${platformLabel(item.platform)} ${item.video_ref || ""}</strong><span>${new Date(item.captured_at * 1000).toLocaleString()}</span></div>
      <div class="chips">
        <span class="chip">播放 ${item.views}</span>
        <span class="chip">点赞 ${item.likes}</span>
        <span class="chip">评论 ${item.comments}</span>
        <span class="chip">私信 ${item.messages}</span>
      </div>
    </article>`;
  }).join("") || `<div class="muted">暂无统计快照</div>`;
}

async function refresh() {
  const [summary, platforms, accounts, tasks, stats, distributionSettings] = await Promise.all([
    api("/api/summary"),
    api("/api/platforms"),
    api("/api/accounts"),
    api("/api/tasks"),
    api("/api/stats"),
    api("/api/settings/distribution"),
  ]);
  Object.assign(state, { summary, platforms, accounts, tasks, stats, distributionSettings });
  renderSummary();
  renderPlatforms();
  renderTaskSelects();
  renderDistributionSettings();
  renderAccounts();
  renderTasks();
  renderStats();
}

function renderDistributionSettings() {
  const form = document.querySelector("#distribution-settings-form");
  if (!form) return;
  const settings = state.distributionSettings || { common: {}, platforms: {} };
  const common = settings.common || {};
  form.elements["common.material_dir"].value = common.material_dir || "runtime/materials/videos";
  form.elements["common.publish_mode"].value = common.publish_mode || "publish";
  form.elements["common.upload_timeout"].value = String(common.upload_timeout || 60);
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
      <strong>${platformLabel(platform.key)}</strong>
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
    upload_timeout: Number(data.get("common.upload_timeout") || 60),
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
    }
  });
  return { common, platforms };
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

document.querySelectorAll(".nav-btn").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll(".nav-btn").forEach((item) => item.classList.remove("active"));
    document.querySelectorAll(".view").forEach((item) => item.classList.remove("active"));
    button.classList.add("active");
    document.querySelector(`#${button.dataset.view}`).classList.add("active");
  });
});

document.querySelector("#refresh").addEventListener("click", refresh);

document.querySelector("#account-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(event.target).entries());
  data.account_key = makeAccountKey(data.display_name, data.account_suffix);
  delete data.account_suffix;
  data.notes = "";
  data.platforms = PLATFORM_ORDER;
  await api("/api/accounts", { method: "POST", body: JSON.stringify(data) });
  event.target.reset();
  await refresh();
});

document.querySelector("#task-form").addEventListener("submit", async (event) => {
  event.preventDefault();
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
  }
});

document.querySelector("#distribution-settings-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const stateNode = document.querySelector("#settings-save-state");
  stateNode.textContent = "保存中...";
  await api("/api/settings/distribution", {
    method: "PATCH",
    body: JSON.stringify(collectDistributionSettings(event.target)),
  });
  stateNode.textContent = "已保存，下一次矩阵分发会按全局配置和平台独立配置执行。";
  await refresh();
});

document.addEventListener("click", async (event) => {
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
