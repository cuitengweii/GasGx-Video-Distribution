# GasGx Video Distribution State

Last updated: 2026-04-27

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
