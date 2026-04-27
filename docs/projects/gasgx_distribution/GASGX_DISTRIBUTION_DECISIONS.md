# GasGx Video Distribution Decisions

Last updated: 2026-04-27

## 2026-04-27

- Keep the `cybercar` package and CLI names intact to preserve existing automation behavior.
- Add `gasgx_distribution` as a separate local console package instead of renaming or refactoring CyberCar internals.
- Use FastAPI for the local operator API and static UI, with SQLite in `runtime/gasgx_distribution.db` for single-machine state.
- Do not copy `D:\code\CyberCar\runtime`, `profiles`, or `config/x_cookies.local.json`; matrix profiles must be created independently in the G-side project.
- Unsupported phase-one platform automations must return explicit `unsupported` task state rather than presenting incomplete adapters as working.
- 发布页选项统一沉淀为“全局公共配置 + 平台独立配置”，矩阵账号发布默认继承全局配置，同时允许每个平台覆盖自己的文案、权限和发布策略。
- 视频号只是第一个执行层接入的平台；UI 和设置存储必须为 抖音、快手、小红书、B站、TikTok、X、LinkedIn、Facebook、YouTube、VK、Instagram 保留独立配置槽位。
- 当前配置覆盖第一批字段：全局素材目录、默认发布方式、默认上传超时、平台启用状态、内容类型、平台文案、可见范围、评论权限、平台发布方式；视频号额外包含合集名称和原创声明。
- 自动化任务队列中，同一账号、同一平台、同一任务类型在 pending/running 状态下只能存在一条，避免重复点击造成重复执行。

## UI Rules

- Follow GasGx Cyber-Industrial styling: dark main surface, card panels, aurora green `#5DD62C` primary accent, recessed dark inputs, and high-contrast primary buttons.
- Avoid default blue select/hover/active states.
- Use dense operational views instead of a landing page: overview, account matrix, task center, and stats are the first-screen product surface.
- 发布页选项类配置必须放在“公共设置”视图中维护，顶部先展示全局公共配置，再按平台展示独立配置，避免散落在账号卡片和任务表单里。
- 发布素材目录是操作员可编辑配置项，不能做成只读；但 UI 应提供常用目录候选，降低手输错误。
