# 视频生成算法说明

## 这篇文档解决什么问题

本文面向维护人员和开发人员，解释视频生成链路的主要阶段、关键输入、输出结果和排查方向。普通运营人员通常只需要阅读“视频生成工作台”文档。

## 工作架构图

文档更新时间：2026-05-07

```text
[素材输入]
    -> [ingestion 筛选]
    -> [composition 组合]
    -> [render 渲染]
    -> [输出文件 + 进度状态]
```

## 多素材碎片拼接与去重架构图

文档更新时间：2026-05-07

```text
[ingestion 素材池]
    -> [按 category 分桶 buckets]
    -> [recent_limits / active_category_ids 过滤]
    -> [ClipMetadata(clip_id, category, duration, normalized_path)]

[composition_sequence + beat_grid + video_duration_max]
    -> [_pick_segments 逐段拼片]
    -> [按类别挑 clip: _pick_clip(优先 fresh clip_id, 回退全量)]
    -> [随机 start_time]
    -> [按节拍对齐 duration: _align_duration]
    -> [segment_key = clip_id:start_time:duration]

[候选 variant 组装]
    -> [注入标题/口号/HUD/lut/zoom/mirror/offset]
    -> [signature = sha1(segments + 文案 + 视觉参数 + HUD)]

[去重判定]
    -> [批内去重: signature ∉ seen_signatures ?]
    -> [历史去重: signature ∉ historical_signatures ?]
    -> [片段去重: segment_key 不触碰 recent_segment_keys ?]
    -> [不满足则重抽, attempts <= max_variant_attempts]
    -> [超限且有 history_collision: 降级接受]
    -> [超限且无可用候选: 抛错终止]

[通过去重的 variants]
    -> [render_variant 并发渲染]
    -> [视频/封面/文案/manifest 输出]
```

## 关键去重键

- `clip_id`：素材级唯一标识（来源路径哈希）。
- `segment_key`：片段级唯一键，格式为 `clip_id:start_time:duration`。
- `signature`：变体级唯一签名，包含片段组合 + 文案 + 视觉扰动参数 + HUD 文本。
- `max_variant_attempts`：单条变体最大重试次数；超过后按实现走降级接受或抛错。

## 核心模块

- `src/gasgx_distribution/video_matrix/pipeline.py`：组织生成任务流程。
- `src/gasgx_distribution/video_matrix/ingestion.py`：读取和筛选素材。
- `src/gasgx_distribution/video_matrix/composition.py`：组合素材片段和模板配置。
- `src/gasgx_distribution/video_matrix/render.py`：调用渲染与导出逻辑。
- `src/gasgx_distribution/video_matrix/beat.py`：处理节拍、切片和时间点。

## 输入数据

视频生成通常需要以下输入：

- 素材分类目录。
- 输出数量。
- 输出目录。
- 封面模板配置。
- 正文模板配置。
- 片尾模板配置。
- 本地背景音乐。
- 组合顺序和去重策略。
- 目标帧率、时长范围和并发参数。

## 素材选择逻辑

素材选择会尽量根据分类目录读取可用视频片段。维护时重点关注：

- 分类目录是否存在。
- 每个分类是否有足够素材。
- 文件扩展名是否受支持。
- 文件是否损坏或不可读。
- 最近使用限制是否导致可选素材过少。

如果素材不足，生成结果可能重复或失败。

## 组合逻辑

组合逻辑负责把素材片段、模板、封面、片尾和音频组合为一个生成计划。它需要保证：

- 输出数量符合用户设置。
- 每条视频的片段顺序合理。
- 模板快照完整。
- 片尾模式明确。
- 背景音乐路径可用。
- 输出文件名不会冲突。

## 渲染逻辑

渲染阶段会把组合计划转换为实际视频文件。排查时重点看：

- FFmpeg 是否可用。
- 输出目录是否可写。
- 输入素材是否能被解码。
- 音频和视频时长是否匹配。
- 并发数是否过高导致资源不足。

## 进度状态

生成任务通常会经历：

- 排队。
- 读取素材。
- 生成组合。
- 渲染视频。
- 写入输出文件。
- 完成或失败。

前端会按任务状态刷新进度，用户应保持页面和本地服务运行。

## 常见问题

### 为什么生成速度慢？

可能是素材体积大、并发过低、机器资源不足、模板复杂或输出数量过多。可以先减少输出数量进行验证。

### 为什么某些素材没有被选中？

可能是分类不匹配、最近使用限制、文件格式不支持或素材读取失败。

### 为什么输出文件缺少音频？

检查本地背景音乐是否可读，音频编码是否支持，以及渲染日志中是否有音频处理错误。

## 开发排查清单

- 使用小批量素材复现问题。
- 保存失败任务的输入参数。
- 检查素材分类、模板快照和输出目录。
- 先验证单条生成，再扩大到批量。
- 修改算法后补充前端和后端测试。
