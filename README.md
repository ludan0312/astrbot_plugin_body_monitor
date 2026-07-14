# Body Monitor - AstrBot 身体数据监测插件

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

1. 将本插件复制到 AstrBot 插件目录
2. 安装依赖: `pip install -r requirements.txt`
3. 在 AstrBot WebUI 中配置插件
4. 重启 AstrBot

## 配置项

| 配置项 | 说明 | 默认值 |
|-------|------|-------|
| `data_port` | HTTP 接收端口 | 7788 |
| `targets` | 目标消息平台 | `[]` |
| `baseline_days` | 基线收集天数 | 7 |
| `baseline_mode` | 基线模式 | `sliding` |
| `check_interval` | 检测间隔(秒) | 300 |
| `quiet_hours` | 静默时段 | 23:00-08:00 |
| `metrics` | 指标检测配置 | 见代码 |
| `api_key` | AstrBot HTTP API Key | `""` |

## 手机端配置

1. 安装 Health Connect Webhook App
2. 授予 Health Connect 权限
3. 配置 Webhook URL: `https://你的DDNS:7788/upload`
4. 同步间隔: 15 分钟
5. 在小米运动健康中开启 Health Connect 分享

## 命令

- `/body_status` - 查看监测状态（含体脂秤数据）
- `/body_baseline` - 查看基线统计
- `/body_body` - 查看体脂秤身体成分数据
- `/body_alerts` - 查看最近告警
- `/body_test` - 测试发送关心消息

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

## 部署指南链接
如果你还没有部署配套服务，可参考以下通用 Linux 部署指南：

- [Body Monitor 身体数据监测插件 – 通用部署指南](http://ludanhome.online:19192/?p=319)

## 注意事项

- 前 7 天为基线收集期，不会触发异常检测
- 静默时段（默认 23:00-08:00）不会发送关心消息
- 同一指标有冷却时间，避免刷屏
- 体脂秤数据需要先在小米运动健康中同步到 Health Connect
