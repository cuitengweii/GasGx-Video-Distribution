# GasGx Video Distribution State

## 2026-04-29 Thread Archive: Supabase Multi-Brand And Video Matrix

- Current repository path: `G:\GasGx Video Distribution`.
- Current Git remote: `https://github.com/cuitengweii/GasGx-Video-Distribution.git`.
- Latest pushed commit: `2ddbd2f feat(gasgx): refine video matrix and supabase policies` on `main`.
- Supabase multi-brand runtime is committed and pushed: control-plane schema, brand baseline schema, tenant resolver, Supabase REST backend, brand settings API, AI robot config/message queue API, and `/api/system/supabase-health`.
- Local browser loading failure on `http://127.0.0.1:8765/#overview` was fixed by restarting the current-code service, bypassing tenant DB binding for `/` and `/static/*`, defaulting local `127.0.0.1/localhost` to the `gasgx` brand, and bounding Supabase REST timeout.
- AI robot config is now real enough for platform config save, secret preservation/redaction, webhook HMAC verification, webhook/test message enqueue, and queue display. Real outbound sender workers remain pending.
- Supabase SQL now includes control-plane and brand-database RLS policy scaffolding, with targeted tests for expected SQL policy structures.
- Video Matrix UI/API refinements and targeted tests were committed and pushed in `2ddbd2f`.

## Current Open Work From 2026-04-29 Archive

- Rotate the Supabase `service_role` key before production/customer use because it was exposed in chat during setup.
- Add real AI robot outbound worker senders for WeCom, DingTalk, Lark, Telegram, and WhatsApp.
- Decide whether remote Supabase smoke/demo rows should be deleted or converted into formal demo seed data.
- Replace Supabase dashboard summary's small-data client aggregation with a SQL view/RPC before larger customer datasets.
- Finish Supabase Storage-backed brand logo upload/read/delete.
- Build customer setup/diagnostic flow for manual Supabase SQL initialization, project connection validation, health checks, and RLS readiness.
- Run full browser end-to-end acceptance for account creation, task creation, AI robot test message, brand settings persistence, video matrix UI, and system health.

## 2026-04-28 Supabase RLS Update

- Supabase control-plane SQL now defines `control_members` with `owner/admin/operator/viewer` roles, helper functions `control_current_role()` / `control_has_role()`, RLS enablement, and policies for brand instances, templates, upgrade runs, upgrade run items, and member management.
- Brand baseline SQL now defines `brand_members` with the same role set, helper functions `brand_current_role()` / `brand_has_role()`, RLS enablement, and policies for matrix accounts, account platforms, browser profiles, automation tasks, stats snapshots, AI robot config/message tables, brand settings, schema migrations, and member management.
- The customer deployment model is direct installation on the customer's computer plus manual Supabase SQL initialization. The temporary local `Control Plane` frontend page was removed, so the customer-facing console does not expose brand registry or upgrade-run management.
- Targeted regression passed: `pytest tests\test_supabase_rls_sql.py tests\test_gasgx_distribution.py -q` and `node --check src\gasgx_distribution\web\static\app.js`.

## Current Open Work From This Update

- Migration runner is no longer the immediate deployment path for customer setup. For now, initialize each customer's free Supabase database by manually executing the SQL files, then configure the project connection for that customer's local install.
- AI robot worker still needs real senders for WeCom, DingTalk, Lark, Telegram, and WhatsApp.
- Supabase dashboard aggregation still needs a SQL view/RPC path to avoid pulling full tables at larger scale.
- Brand logo storage still needs Supabase Storage upload/read/delete instead of config-field data URL handling.

Last updated: 2026-04-28

## Scope

- Project path: `G:\GasGx Video Distribution`
- Source baseline: restored CyberCar tracked files in the G-side repository, then applied the current `D:\code\CyberCar\src\cybercar\engine.py` working diff into G only.
- Source protection: `D:\code\CyberCar` remains read-only for this project; do not write, clean, commit, or push from the source repo.

## Current Capability

- `python -m cybercar ...` remains the compatibility CLI for the original collect, publish, Telegram, login, and engagement flows.
- `python -m gasgx_distribution web` starts the local matrix-account Web console on `127.0.0.1:8765`.
- Matrix data is stored in `runtime/gasgx_distribution.db`.
- Account browser profiles are isolated under `profiles/matrix/<account_key>/<platform>/`.
- Phase 1 platform registration covers 视频号, 抖音, 快手, 小红书, B站, X, TikTok, LinkedIn, Facebook, YouTube, VK, and Instagram.

## Open Work

- Add real worker execution behind queued `publish`, `comment`, `message`, and `stats` tasks.
- Extend deep platform adapters for LinkedIn, Facebook, YouTube, VK, and Instagram after the base console is stable.
- Add authenticated multi-user access only if the console moves beyond local single-machine operation.

## 2026-04-27 Public Settings Update

- 公共设置已升级为“全局公共配置 + 平台独立配置”，保存位置为 `runtime/publish_settings.json`。
- 全局公共配置覆盖素材目录、默认发布方式、默认上传超时；每个平台可以单独配置启用状态、内容类型、平台文案、可见范围、评论权限、发布方式。
- 视频号独立配置额外保留合集名称和原创声明；矩阵批量视频号发布命令继续读取兼容函数 `load_wechat_publish_settings()`。
- 旧的 `runtime/wechat_publish_settings.json` 会作为兼容迁移来源读取，但新设置以 `runtime/publish_settings.json` 为准。
- 公共设置里的发布素材目录已允许手动编辑，并提供常用素材目录候选；任务创建会阻止同一账号、同一平台、同一任务类型重复加入 pending/running 队列。
- 后续还需继续把各平台真实发布页的更多选项接入执行层，例如封面、定位、同步设置、定时发布、平台专属标签等。

## 2026-04-28 Matrix Publish And Operator Console Update

- 本地控制台当前运行入口为 `python -m uvicorn gasgx_distribution.web:app --host 127.0.0.1 --port 8766`，浏览器入口为 `http://127.0.0.1:8766/`。
- 视频号矩阵发布作业已进入可配置执行状态：后台配置可控制每批账号数 1-10、后台定时开关、按间隔/每天固定时间启动、批次最小/最大间隔、起始批次轮换、批次内随机、失败最后补跑。
- 当前矩阵发布策略为“一个账号一次只分配一个素材视频”：按候选视频顺序为账号分配未使用视频，成功写入 evidence 后才标记该视频已用，避免同一视频在失败场景中被误消费。
- 发布执行层增加进程级发布锁 `runtime/matrix_publish.lock`，防止手动启动和后台调度同时占用浏览器标签或同一 profile。
- 每个视频号账号使用独立 profile 和独立 debug port：`profiles/matrix/<account_key>/wechat`，端口按账号 ID 映射到 `9400 + account_id`。
- 视频号配置优先级已明确写入设置页：平台独立配置优先，其次继承全局公共配置，最后使用代码默认值；视频描述不会覆盖短标题。
- 视频号独立配置当前包括：短标题默认 `GasGx`、位置留空表示“不显示位置”、合集、原创声明、发布方式、文案、可见范围、评论权限、上传超时。
- 全局公共配置当前包括：发布素材目录、默认发布方式、全局话题、全局上传超时；默认话题为 `#天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿`。
- 总览页已统计剩余素材数量，账号列表展示每个账号发布成功数量；任务中心的队列项可删除。
- 设置页“立即启动一次”按钮已移到明显位置并弹窗确认；刷新、保存、打开目录、立即启动、删除队列等异步按钮均有加载圈。
- 全站平台名称默认显示标准 App 外框 logo，当前使用 Iconify/Simple Icons SVG 源；抖音由于 Simple Icons/Iconify 暂无独立 `douyin` 条目，暂用同源 TikTok 音符 SVG 并以抖音平台名展示。
- 新增视频矩阵素材生产模块雏形：`src/gasgx_distribution/video_matrix/`、`src/gasgx_distribution/video_matrix_api.py`、`config/video_matrix/` 和对应静态页面，用于后续素材入库、模板、封面、BGM、预览与渲染管理。

## Current Open Work

- 浏览器页面需要人工刷新后确认新版 logo 和 loading 状态的真实视觉效果；当前自动截图验证受本机缺少 Playwright 包限制。
- 如需完全离线稳定显示平台 logo，应把 Iconify/Simple Icons SVG 下载为本地静态资产；当前实现依赖 `https://api.iconify.design`。
- 抖音官方 SVG 仍需后续补源；当前没有第三方源可直接提供独立 Douyin 图标。
- 视频矩阵模块已进入仓库但仍需一次端到端运行验收：素材上传、模板选择、封面预览、渲染输出和 Web API 流程。

## 2026-04-28 Local Shell And AI Robot Update

- 本地 Web 控制台继续统一在 `http://127.0.0.1:8765/` 的内嵌系统壳层内运行，左侧业务工作台已扩展为：总览、账号矩阵、公共设置、任务中心、数据统计、AI机器人、视频生成。
- 页面头部改为固定顶栏，左侧栏固定在左边；侧栏折叠按钮已移动到顶部标题区左侧，避免折叠后遮挡右侧业务内容。
- 顶部右侧默认展示已登录用户 `Allen` 和圆形头像；左下角保留系统管理入口，用于弹出用户中心、通知中心、系统设置、帮助文档和退出入口。
- 账号矩阵新增表单已拆分为“品牌前缀 + 名称”，提交时合并成 `display_name`，继续按合并后的名称生成 `account_key`。
- 账号列表卡片已增强视觉层级，使用左侧强调条、强化边框和背景层级，便于区分每个矩阵账号块。
- 公共设置页发布素材目录输入框扩展为更长路径显示，旁边按钮文案从“打开”调整为“修改”；矩阵发布作业配置项增加统一单色 SVG 图标。
- 新增 AI机器人页面，当前为前端预留功能面：AI客服、客户数据看板推送、告警通知、指令交互；平台接入卡片覆盖企业微信、钉钉、飞书/Lark、Telegram、WhatsApp。
- 数据统计页已作为客户视角短视频矩阵数字化营销看板：包含周期/平台/账号筛选、总览指标、账号矩阵、作品内容、流量来源、营销转化、运营执行、趋势和风险模块。

## Current Open Work

- AI机器人页面目前是 UI 和入口预留，尚未接入真实机器人配置 API、Webhook 密钥保存、回调验签、消息发送队列或权限控制。
- 数据统计页当前使用前端样例数据展示结构，后续需要接真实统计聚合接口和时间/平台/账号筛选联动。
- 侧栏折叠后展开按钮在顶部标题区保留，仍需人工在真实浏览器中确认不同页面滚动位置下不会影响标题可读性。
