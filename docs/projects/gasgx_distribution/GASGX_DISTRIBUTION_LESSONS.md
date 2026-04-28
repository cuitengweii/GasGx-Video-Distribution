# GasGx Video Distribution Lessons

Last updated: 2026-04-28

## 2026-04-28

- Symptom: 视频号发布页填完描述后看起来被下一个标签覆盖，发布没有真正完成，甚至同一账号重复尝试同一素材。
  - Root cause: 矩阵发布缺少全局执行锁和 per-account debug port/profile 强约束，同时描述 read-back 在动态编辑器里可能拿不到刚输入的配置文本。
  - Earlier detection: 每次发布调试都要同时检查 profile path、debug port、workspace、evidence 文件和浏览器标签行为，不能只看子进程 returncode。
  - Prevention: 矩阵发布必须先拿锁；每个账号绑定独立 profile/debug port；成功必须以 `uploaded_records_wechat.jsonl` evidence 为准。

- Symptom: 后台配置了新话题和短标题，但视频号页面仍出现旧话题、短标题被视频描述覆盖或位置保留旧城市。
  - Root cause: CyberCar 默认必填话题和短标题自动生成逻辑覆盖了矩阵配置；位置清理逻辑只识别旧的乱码文本。
  - Earlier detection: 生成的 `matrix_wechat_publish_config.json` 与实际页面字段需要一起核对，尤其是短标题、位置、话题和发布方式。
  - Prevention: 矩阵发布设置 `CYBERCAR_DISABLE_REQUIRED_HASHTAGS=1`；短标题显式传入 `fill_draft_wechat()`；位置为空时优先选择“不显示位置”。

- Symptom: 操作员点击刷新、保存、启动、删除等按钮后无法判断是否正在处理。
  - Root cause: 部分异步按钮只改文字或完全没有 loading 状态。
  - Earlier detection: 所有触发 HTTP 请求或后台任务的按钮都应检查 loading/disabled 状态。
  - Prevention: 前端异步按钮统一使用 loading helper，显示 spinner 并禁用按钮直到请求完成。

- Symptom: 平台名称只显示文字或假 logo，跨页面识别效率低。
  - Root cause: 缺少统一的平台 logo helper 和真实 SVG 资产策略。
  - Earlier detection: UI 评审时应检查总览、账号、任务、统计、设置页是否都使用同一平台标识组件。
  - Prevention: 平台展示统一走 `platformIcon()` / `platformName()`；logo 使用第三方真实 SVG 源并套标准 App 外框，后续可迁移成本地静态资产。

- Symptom: 左侧栏折叠按钮放在侧栏中部时，折叠后容易遮挡右侧业务内容或被隐藏区域一起带走。
  - Root cause: 折叠控制和侧栏内容混在同一区域，按钮定位没有跟随顶栏信息架构。
  - Earlier detection: 侧栏折叠类功能必须同时检查“隐藏后如何恢复”和“恢复按钮是否遮挡主内容”。
  - Prevention: 折叠按钮放入固定顶部标题区，作为全局壳层控制，而不是放在侧栏正文区域。

- Symptom: 新增功能入口如果只做独立页面或外跳，会让用户误以为左侧系统能力丢失。
  - Root cause: 本地控制台已经从单页工具升级为系统壳层，但新增模块容易沿用工具页思路。
  - Earlier detection: 每个新模块必须先确认是否应该成为左侧一级菜单，并继承统一顶栏、用户入口和系统管理入口。
  - Prevention: 新增业务能力默认在统一壳层内新增 `view`，通过 `data-view` 路由，不做页面外跳。
