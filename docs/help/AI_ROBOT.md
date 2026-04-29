# AI 机器人与通知

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
