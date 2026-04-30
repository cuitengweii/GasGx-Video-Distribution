# AI 机器人与通知

## 2026-04-30 Operations Notice Routes

- AI robot settings store the platform credentials and sender configuration.
- Operations notice routes are separate switches stored in `notification_routes`.
- WeChat login QR notices currently support route switches for Telegram, DingTalk, and WeCom.
- A route is sendable only when both conditions are true: the operation route is enabled, and the corresponding AI robot platform is enabled with a usable webhook/Bot target.
- QR login notices are grouped by `login_batch_id` so operators receive one summary for a batch instead of one noisy message per account.
- Telegram, DingTalk, and WeCom currently receive the QR summary text and local QR path/link. Telegram direct photo upload can be added later when QR image upload is required.

## 用途

AI 机器人用于配置企业微信、钉钉、飞书/Lark、Telegram、WhatsApp 等通知通道，并把测试消息或系统提醒写入发送队列。

## 主要功能

- 保存平台机器人 Webhook 或 Bot Token。
- 发送测试消息并写入消息队列。
- 查看消息状态、重试次数和发送结果。
- 通过 worker 执行 pending 或 retry 消息发送。

## 运行入口

- CLI: `python -m gasgx_distribution ai-robot-send-worker --limit 10`
- Web API: `POST /api/ai-robots/messages/send-worker?limit=10`

## 注意事项

- 真实可用性依赖每个平台的 webhook_url、secret、target_id 或 Bot Token 配置。
- 钉钉、飞书等签名参数如果需要精确适配，应按真实平台协议实测校准。
