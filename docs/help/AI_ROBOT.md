# AI 机器人与通知

## 这篇文档解决什么问题

AI 机器人用于把任务结果、登录提醒、系统异常和运营摘要发送到企业通知渠道。它减少人工巡检成本，让关键异常及时出现在团队群或机器人会话中。

## 适用对象

- 超级管理员：配置通知渠道和密钥。
- 发布员：接收发布失败、登录失效和任务完成提醒。
- 素材维护员：接收素材不足或生成异常提醒。

## 支持的通知渠道

平台当前预留或支持以下渠道：

- 企业微信。
- 钉钉。
- 飞书 / Lark。
- Telegram。
- WhatsApp。

不同渠道配置方式不同，通常包括 Webhook 地址、签名密钥、Bot Token 或目标会话 ID。

## 配置流程

1. 进入 AI 机器人页面。
2. 选择要配置的平台。
3. 根据平台填写 Webhook、Token 或目标 ID。
4. 保存配置。
5. 发送测试消息。
6. 到目标群或会话确认是否收到消息。

## 通知类型

- 视频生成完成。
- 发布任务失败。
- 登录状态失效。
- 素材分类不足。
- 系统错误。
- 日报或运营摘要。

## 事件路由对照

AI 机器人外发走通知路由表，当前路由平台为 Telegram、钉钉、企业微信。飞书/Lark、WhatsApp 已属于 AI 机器人发送平台，但暂不纳入运营通知路由开关；如需同表路由，应先扩展 `NOTIFICATION_PLATFORMS`。

| 方面 | 触发来源 | 默认级别 | 通知中心 | AI机器人路由 | event_type / 子类型 |
| --- | --- | --- | --- | --- | --- |
| 登录二维码 | 登录巡检或发布前置检查生成/更新扫码二维码 | blocking | 是 | 是 | `wechat_login_qr` / `qr_generated`, `qr_updated`, `login_required` |
| 登录状态 | 登录失效、即将失效、登录失败、终端执行登录巡检结论 | blocking | 是 | 是 | `login_status` / `expired`, `expiring_soon`, `failed`, `inspection_result` |
| 发布与任务 | 发布成功/失败、上传失败、草稿保存、仅上传、排队/执行/取消等关键节点 | error | 是 | 是 | `publish_result` / `published`, `failed`, `upload_failed`, `draft_saved`, `uploaded_only`, `queued`, `running`, `cancelled` |
| 视频生成 | 视频生成完成、失败或异常中断 | warning | 是 | 是 | `video_generation` / `completed`, `failed`, `interrupted` |
| 素材问题 | 素材不足、分类不完整、不可用素材被跳过 | warning | 是 | 是 | `material_issue` / `insufficient`, `category_incomplete`, `skipped_unusable` |
| 系统稳定性 | 关键依赖不可用、调度/作业失败、存储或配置异常 | critical | 是 | 是 | `system_error` / `dependency_unavailable`, `scheduler_failed`, `job_failed`, `storage_error`, `config_error` |
| 运营汇总 | 日报、运营摘要推送完成或失败 | info | 是 | 是 | `ops_summary` / `daily_sent`, `daily_failed`, `summary_sent`, `summary_failed` |
| 人工处理 | 需人工确认、补充配置、处理队列积压 | warning | 是 | 是 | `action_required` / `confirmation_required`, `configuration_required`, `queue_backlog` |

代码入口：事件清单由 `NOTIFICATION_EVENT_DEFINITIONS` 维护；新业务触发点应调用 `route_operation_notification(event_type, payload)`，不要直接拼平台发送。

## Telegram 快速配置

1. 在 Telegram 中通过 BotFather 创建机器人。
2. 获取 Bot Token。
3. 打开机器人会话并发送一条消息。
4. 在平台中解析 Chat ID。
5. 保存配置并发送测试消息。

## 常见问题

### 测试消息发送失败怎么办？

检查 Webhook 地址、Token、签名密钥和目标会话 ID。确认机器人已加入目标群，并具有发送消息权限。

### 为什么保存了配置但没有通知？

检查通知路由是否开启，任务是否触发了对应事件，以及消息发送队列是否有失败记录。

### 为什么同一事件收到多条消息？

可能是重复触发任务、失败重试或多个通知渠道同时启用。需要检查通知路由和任务状态。

## 操作检查清单

- Webhook 或 Token 正确。
- 目标会话可接收消息。
- 测试消息已成功。
- 通知路由已开启。
- 失败消息可在队列中追踪。
