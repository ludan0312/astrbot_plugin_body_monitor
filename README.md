# Body Monitor - AstrBot 身体数据监测插件

**Version:** v1.2.1

基于 Health Connect Webhook 方案，接收小米手环 + 小米体脂秤 S400 数据，进行基线计算和异常检测，触发 LLM 主动关心。

## 架构

```
小米手环/体脂秤 -> 小米运动健康 -> Health Connect -> Health Connect Webhook App -> 插件 HTTP 端点
```

## 支持数据

| 设备 | 数据类型 |
|------|---------|
| 小米手环 | 心率、步数、睡眠时长/评分、血氧、压力、HRV |
| 小米体脂秤 S400 | 体重、体脂率、BMI、肌肉量、水分率、骨量、基础代谢、内脏脂肪 |

## 安装

1. AstrBot WebUI → **插件管理** → 搜索 `Body Monitor` 安装，或上传本插件 zip
2. 在 WebUI 中配置插件参数（见下方配置项）
3. 使用 `/body_target_add here` 添加推送目标
4. 重启 AstrBot

## 配置项

| 配置项 | 说明 | 默认值 |
|-------|------|-------|
| `llm_provider_id` | LLM 提供商（留空则自动选择默认） | `""` |
| `persona_id` | 人格设定（留空则使用 AstrBot 默认人格） | `""` |
| `data_port` | HTTP 数据接收端口 | `7788` |
| `baseline_days` | 基线收集天数 | `7` |
| `baseline_mode` | 基线模式 (`sliding`/`fixed`) | `sliding` |
| `check_interval` | 检测间隔（秒） | `300` |
| `quiet_hours_enabled` | 是否启用静默时段 | `true` |
| `quiet_hours_start` | 静默开始时间 | `23:00` |
| `quiet_hours_end` | 静默结束时间 | `08:00` |
| `heart_rate_enabled` | 心率异常检测开关 | `true` |
| `heart_rate_threshold` | 心率异常 z-score 阈值 | `2.0` |
| `heart_rate_cooldown` | 心率告警冷却（小时） | `4` |
| `sleep_score_enabled` | 睡眠评分异常检测开关 | `true` |
| `sleep_score_threshold` | 睡眠评分异常 z-score 阈值 | `1.5` |
| `sleep_score_cooldown` | 睡眠评分告警冷却（小时） | `8` |
| `spo2_enabled` | 血氧异常检测开关 | `true` |
| `spo2_threshold` | 血氧异常 z-score 阈值 | `2.0` |
| `spo2_cooldown` | 血氧告警冷却（小时） | `4` |

### LLM 与人格配置

- **LLM 提供商**：在插件配置界面点击"选择提供商..."，从 AstrBot 已配置的 LLM 中选择。留空则自动使用默认 Provider。
- **人格设定**：在插件配置界面点击"选择人格..."，从 AstrBot 已配置的人格中选择。留空则使用默认人格。

数据查询命令（`/body_status` 等）会走 AstrBot 的 LLM 管道，因此：
- 回复会**继承所选人格的口吻和角色设定**
- 如果安装了 RVC/TTS 插件，回复会**自动触发语音转换**
- 人格的 `system_prompt` 和数据注入共同作用，生成自然、个性化的关心回复

## 手机端配置

1. 安装 Health Connect Webhook App
2. 授予 Health Connect 权限
3. 配置 Webhook URL: `https://你的DDNS:7788/upload`
4. 同步间隔: 15 分钟
5. 在小米运动健康中开启 Health Connect 分享

## 命令

### 数据查询（走 LLM 管道，支持语音）
- `/body_status` - 查看监测状态（含体脂秤数据）
- `/body_baseline` - 查看基线统计
- `/body_body` - 查看体脂秤身体成分数据
- `/body_alerts` - 查看最近告警

> 以上命令会触发 LLM 生成自然语言回复，数据自动注入到 LLM 上下文中。如果配置了 RVC/TTS，会自动输出语音。

### 目标会话管理（全平台）

插件通过 AstrBot 统一会话标识（UMO）收发消息，**不限平台**：QQ / OneBot、Telegram、Discord、飞书、钉钉、企微、微信生态、KOOK、Slack 等均可用。在对应平台的对话里执行：

- `/body_target_add here` - 将当前会话添加为推送目标
- `/body_target_add <UMO>` - 添加指定 UMO 为推送目标
- `/body_target_remove <UMO>` - 移除目标
- `/body_target_list` - 列出所有目标

### 测试
- `/body_test` - 测试发送主动关心消息

## 主动关心机制

插件会定时检查数据异常，触发条件：
1. **基线建立期**：前 7 天收集数据，不触发异常检测
2. **异常检测**：心率、睡眠评分、血氧等指标偏离基线 z-score 阈值时触发
3. **静默时段**：默认 23:00-08:00 不发送关心消息
4. **冷却时间**：同一指标有冷却时间，避免刷屏

关心消息通过 LLM 生成，使用配置的人格口吻，自动推送到 `/body_target_add` 添加的目标会话。

## 数据解析

插件支持 Health Connect Webhook 的多种数据格式：
- 单条记录: `{"type": "heart_rate", "value": 72}`
- 批量记录: `{"records": [...]}`
- 按类型分组: `{"heart_rate": [...], "steps": [...], "weight": [...]}`

## 体脂秤数据说明

- 体脂秤数据一天一次，不参与实时异常检测
- 用于 LLM 关心时的上下文（如"今天体重比昨天轻了 0.5kg"）
- 称重时同步测量的心率会参与实时异常检测

## 路由器配置

映射外部端口 `7788` 到 Unraid 的 `7788` 端口。

## 部署指南

- [Body Monitor 身体数据监测插件 – 通用部署指南](http://ludanhome.online:19192/?p=319)

## 注意事项

- 前 7 天为基线收集期，不会触发异常检测
- 静默时段（默认 23:00-08:00）不会发送关心消息
- 同一指标有冷却时间，避免刷屏
- 体脂秤数据需要先在小米运动健康中同步到 Health Connect
- 插件数据存储在 AstrBot 数据目录下，卸载/重装不会丢失历史数据
- 数据查询命令走 LLM 管道，需要确保 AstrBot 已配置可用的 LLM Provider
