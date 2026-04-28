# GasGx Video Distribution Decisions

Last updated: 2026-04-28

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

## 2026-04-28

- 视频号矩阵发布的稳定规则是“每个账号每轮只发布一个视频，账号与视频一一分配，成功后才标记素材已使用”，避免单号连续占用多个素材或失败时误消费素材。
- 自动发布作业必须通过配置驱动，不能硬编码批次数、间隔、轮换顺序和定时方式；设置页是操作员随时启停和调整作业的主入口。
- 发布执行必须有进程级互斥锁，避免手动启动和后台调度同时打开多个标签页、抢占同一账号浏览器上下文。
- 每个矩阵账号必须使用独立 cookie/profile 和独立 debug port；账号浏览器上下文不能复用默认 profile 或共享调试端口。
- 视频号配置优先级固定为：平台独立配置 > 继承全局公共配置 > 代码默认值；视频描述不得覆盖短标题，位置留空必须表示“不显示位置”。
- 视频号草稿/发布成功判定必须依赖 `uploaded_records_wechat.jsonl` evidence，不能只看子进程 returncode 0。
- 平台 logo 默认使用真实 SVG 源并套标准 App 外框；不得用字母占位块伪装为 logo。当前第三方源为 Iconify/Simple Icons，后续可迁移成本地静态资产。
- UI 异步按钮默认必须有加载圈和 disabled 状态，避免操作员误判为“死按钮”或重复点击。
- 新增视频矩阵素材生产模块时保持独立命名空间 `gasgx_distribution.video_matrix`，避免把素材生产逻辑混入账号矩阵发布执行层。

## UI Rules

- 平台名称展示默认应调用统一 logo helper，而不是每个页面单独拼 logo。
- 原生 `select option` 不作为 SVG 展示目标；如确实需要下拉项显示平台 logo，必须另开自定义 select 组件任务。

## 2026-04-28 Local Console Shell And AI Robot Decisions

- 本地控制台新增业务入口时必须内嵌在统一系统壳层内，不再通过“新窗口打开”方式绕开左侧导航、用户中心和统一顶栏。
- 业务工作台新增“AI机器人”作为一级菜单，定位为 AI客服与企业通讯机器人入口；国内平台优先企业微信、钉钉、飞书/Lark，海外平台优先 Telegram 和 WhatsApp。
- AI机器人页面当前只做配置入口和能力说明，不伪装成已完成真实消息发送；后续真实接入必须补齐 token/webhook 配置、回调验签、发送队列、失败重试和权限控制。
- 账号创建 UI 采用“品牌前缀 + 名称”拆分输入，但后端接口仍保持 `display_name` 字段；前端提交前合并，避免引入后端 schema 变更。
- 发布素材目录按钮文案使用“修改”，强调它是目录配置动作；真正打开目录或选择目录如果要做，应单独设计文件选择/系统打开能力。
