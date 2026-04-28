# GasGx Video Distribution State

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
