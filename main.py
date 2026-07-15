import asyncio
import sqlite3
import json
import math
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from aiohttp import web
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api import logger
from astrbot.api.star import Context, Star, register
from astrbot.api.provider import ProviderRequest


@register("body_monitor", "ludan", "小米手环+体脂秤身体数据监测与主动关心", "v1.1.0")
class BodyMonitorPlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.context = context

        # 配置项（从 _conf_schema.json 读取）
        self.config = config or {}
        self.data_port = self.config.get("data_port", 7788)
        self.baseline_days = self.config.get("baseline_days", 7)
        self.baseline_mode = self.config.get("baseline_mode", "sliding")
        self.check_interval = self.config.get("check_interval", 300)

        # 静默时段
        self.quiet_hours = {
            "enabled": self.config.get("quiet_hours_enabled", True),
            "start": self.config.get("quiet_hours_start", "23:00"),
            "end": self.config.get("quiet_hours_end", "08:00")
        }

        # 指标配置
        self.metrics_config = {
            "heart_rate": {
                "enabled": self.config.get("heart_rate_enabled", True),
                "threshold": self.config.get("heart_rate_threshold", 2.0),
                "cooldown": self.config.get("heart_rate_cooldown", 4)
            },
            "sleep_score": {
                "enabled": self.config.get("sleep_score_enabled", True),
                "threshold": self.config.get("sleep_score_threshold", 1.5),
                "cooldown": self.config.get("sleep_score_cooldown", 8)
            },
            "spo2": {
                "enabled": self.config.get("spo2_enabled", True),
                "threshold": self.config.get("spo2_threshold", 2.0),
                "cooldown": self.config.get("spo2_cooldown", 4)
            }
        }

        # 数据库：使用 AstrBot 推荐的插件数据目录，避免更新/重装插件时丢失数据
        self.db_path = self._get_db_path()
        self._init_db()

        # 告警冷却记录
        self.alert_cooldown = {}

        # 启动 HTTP 服务器和定时任务
        self.app = web.Application()
        self.app.router.add_post("/upload", self._handle_upload)
        self.app.router.add_get("/health", self._handle_health)
        self.runner = None
        self.site = None

        self._server_task = asyncio.create_task(self._start_server())
        self._periodic_task = asyncio.create_task(self._start_periodic_check())

    def _get_db_path(self) -> str:
        """获取数据库文件路径，优先使用 AstrBot 的 plugin_data 目录。

        会依次尝试：
        1. AstrBot 官方插件数据目录（get_astrbot_plugin_data_path）
        2. AstrBot 数据目录下的 plugin_data（get_astrbot_data_path）
        3. 插件自身目录
        4. 系统临时目录（最后兜底）
        """
        plugin_name = getattr(self, "name", "body_monitor")
        candidates = []

        # 1. 官方插件数据目录（v4.9.2+）
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path
            candidates.append(Path(get_astrbot_plugin_data_path()) / plugin_name)
        except Exception:
            pass

        # 2. 数据目录 / plugin_data / <name>
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path
            candidates.append(Path(get_astrbot_data_path()) / "plugin_data" / plugin_name)
        except Exception:
            pass

        # 3. 插件目录
        try:
            candidates.append(Path(os.path.dirname(__file__)))
        except Exception:
            pass

        # 4. 临时目录兜底
        candidates.append(Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / plugin_name)

        last_error = None
        old_db = Path(os.path.dirname(__file__)) / "body_monitor.db"

        for data_dir in candidates:
            try:
                data_dir.mkdir(parents=True, exist_ok=True)
                db_file = data_dir / "body_monitor.db"

                # 尝试迁移旧数据库（插件目录 -> 新位置）
                if old_db.exists() and not db_file.exists():
                    try:
                        shutil.move(str(old_db), str(db_file))
                        logger.info(f"[BodyMonitor] Migrated old database to {db_file}")
                    except Exception as e:
                        logger.warning(f"[BodyMonitor] Failed to migrate old database: {e}")

                # 测试是否真的能打开
                conn = sqlite3.connect(str(db_file))
                conn.close()
                logger.info(f"[BodyMonitor] Database path resolved: {db_file}")
                return str(db_file)
            except Exception as e:
                last_error = e
                logger.debug(f"[BodyMonitor] DB path candidate failed {data_dir}: {e}")
                continue

        # 走到这里说明所有候选都不行，理论上不应该发生
        raise RuntimeError(f"[BodyMonitor] Unable to resolve writable database path: {last_error}")

    def _db_connect(self):
        """确保数据库目录存在后建立连接。"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        conn = self._db_connect()
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                source TEXT,
                heart_rate REAL,
                steps INTEGER,
                sleep_duration REAL,
                sleep_score REAL,
                spo2 REAL,
                stress REAL,
                hrv REAL,
                calories REAL,
                distance REAL,
                weight REAL,
                body_fat REAL,
                bmi REAL,
                muscle_mass REAL,
                water_rate REAL,
                bone_mass REAL,
                basal_metabolism REAL,
                visceral_fat REAL,
                raw_json TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS baseline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                metric TEXT,
                mean REAL,
                std REAL,
                sample_count INTEGER,
                UNIQUE(date, metric)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                metric TEXT,
                value REAL,
                baseline_mean REAL,
                baseline_std REAL,
                z_score REAL,
                llm_response TEXT,
                resolved INTEGER DEFAULT 0
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                umo TEXT UNIQUE,
                created_at TEXT
            )
        """)

        conn.commit()
        conn.close()

    async def _start_server(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "0.0.0.0", self.data_port)
        await self.site.start()
        logger.info(f"[BodyMonitor] HTTP server started on port {self.data_port}")

    async def _start_periodic_check(self):
        await asyncio.sleep(10)
        while True:
            try:
                await self._periodic_check()
            except asyncio.CancelledError:
                logger.debug("[BodyMonitor] Periodic check cancelled")
                break
            except Exception as e:
                logger.error(f"[BodyMonitor] Periodic check error: {e}", exc_info=True)
            await asyncio.sleep(self.check_interval)

    async def _handle_health(self, request):
        return web.json_response({"status": "ok", "plugin": "body-monitor"})

    async def _handle_upload(self, request):
        try:
            data = await request.json()
            logger.debug(f"[BodyMonitor] Received webhook data")

            parsed = self._parse_health_connect_data(data)
            self._insert_raw_data(parsed)

            if parsed.get("heart_rate"):
                await self._check_metric("heart_rate", parsed["heart_rate"])

            return web.json_response({"status": "success", "received": True})
        except Exception as e:
            logger.error(f"[BodyMonitor] Upload error: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    def _parse_health_connect_data(self, data: dict) -> dict:
        result = {
            "timestamp": datetime.now().isoformat(),
            "source": "health_connect_webhook",
            "raw_json": json.dumps(data, ensure_ascii=False)
        }

        # Health Connect Webhook 格式: {"heart_rate": [{"bpm": 72, "time": "..."}], ...}

        # 心率 (取最新一条)
        if "heart_rate" in data and isinstance(data["heart_rate"], list) and data["heart_rate"]:
            latest = data["heart_rate"][-1]
            if isinstance(latest, dict) and "bpm" in latest:
                result["heart_rate"] = float(latest["bpm"])

        # 静息心率 (备用)
        if "resting_heart_rate" in data and isinstance(data["resting_heart_rate"], list) and data["resting_heart_rate"]:
            latest = data["resting_heart_rate"][-1]
            if isinstance(latest, dict) and "bpm" in latest and "heart_rate" not in result:
                result["heart_rate"] = float(latest["bpm"])

        # 步数 (取最新一条的 count)
        if "steps" in data and isinstance(data["steps"], list) and data["steps"]:
            latest = data["steps"][-1]
            if isinstance(latest, dict) and "count" in latest:
                result["steps"] = int(latest["count"])

        # 睡眠 (计算总时长，取最新一条)
        if "sleep" in data and isinstance(data["sleep"], list) and data["sleep"]:
            latest = data["sleep"][-1]
            if isinstance(latest, dict):
                if "duration_seconds" in latest:
                    result["sleep_duration"] = float(latest["duration_seconds"]) / 60  # 转分钟
                # 睡眠评分估算: 深睡比例越高评分越高
                if "stages" in latest and isinstance(latest["stages"], list):
                    total = float(latest.get("duration_seconds", 1))
                    deep = sum(s.get("duration_seconds", 0) for s in latest["stages"] if s.get("stage") == "deep")
                    result["sleep_score"] = min(100, int((deep / total) * 200)) if total > 0 else 0

        # 血氧
        if "oxygen_saturation" in data and isinstance(data["oxygen_saturation"], list) and data["oxygen_saturation"]:
            latest = data["oxygen_saturation"][-1]
            if isinstance(latest, dict) and "percentage" in latest:
                result["spo2"] = float(latest["percentage"])

        # 压力 (HRV 近似映射)
        if "heart_rate_variability" in data and isinstance(data["heart_rate_variability"], list) and data["heart_rate_variability"]:
            latest = data["heart_rate_variability"][-1]
            if isinstance(latest, dict) and "rmssd_millis" in latest:
                # HRV 越低压力越高，简单映射
                hrv = float(latest["rmssd_millis"])
                result["stress"] = max(0, min(100, int(100 - hrv)))

        # 体重
        if "weight" in data and isinstance(data["weight"], list) and data["weight"]:
            latest = data["weight"][-1]
            if isinstance(latest, dict) and "kilograms" in latest:
                result["weight"] = float(latest["kilograms"])

        # 体脂率
        if "body_fat" in data and isinstance(data["body_fat"], list) and data["body_fat"]:
            latest = data["body_fat"][-1]
            if isinstance(latest, dict) and "percentage" in latest:
                result["body_fat"] = float(latest["percentage"])

        # BMI
        if "bmi" in data and isinstance(data["bmi"], list) and data["bmi"]:
            latest = data["bmi"][-1]
            if isinstance(latest, dict) and "value" in latest:
                result["bmi"] = float(latest["value"])

        # 肌肉量
        if "lean_body_mass" in data and isinstance(data["lean_body_mass"], list) and data["lean_body_mass"]:
            latest = data["lean_body_mass"][-1]
            if isinstance(latest, dict) and "kilograms" in latest:
                result["muscle_mass"] = float(latest["kilograms"])

        # 基础代谢
        if "basal_metabolic_rate" in data and isinstance(data["basal_metabolic_rate"], list) and data["basal_metabolic_rate"]:
            latest = data["basal_metabolic_rate"][-1]
            if isinstance(latest, dict) and "watts" in latest:
                result["basal_metabolism"] = float(latest["watts"])

        # 兼容旧格式 (单条/分组/records)
        if not result.get("heart_rate") and "type" in data:
            metric_type = data.get("type", "").lower()
            value = data.get("value")
            if metric_type in ["heart_rate", "heartrate"] and value:
                result["heart_rate"] = float(value)
            elif metric_type in ["steps", "step_count"] and value:
                result["steps"] = int(value)
            elif metric_type in ["sleep", "sleep_session"] and value:
                result["sleep_duration"] = float(value)
            elif metric_type in ["oxygen_saturation", "spo2"] and value:
                result["spo2"] = float(value)
            elif metric_type in ["stress"] and value:
                result["stress"] = float(value)
            elif metric_type in ["weight"] and value:
                result["weight"] = float(value)
            elif metric_type in ["body_fat", "bodyfat", "fat_percentage"] and value:
                result["body_fat"] = float(value)
            elif metric_type in ["bmi"] and value:
                result["bmi"] = float(value)

        return result

    def _insert_raw_data(self, data: dict):
        conn = self._db_connect()
        c = conn.cursor()
        c.execute("""
            INSERT INTO raw_data 
            (timestamp, source, heart_rate, steps, sleep_duration, sleep_score, 
             spo2, stress, hrv, calories, distance, weight, body_fat, bmi,
             muscle_mass, water_rate, bone_mass, basal_metabolism, visceral_fat, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("timestamp"),
            data.get("source"),
            data.get("heart_rate"),
            data.get("steps"),
            data.get("sleep_duration"),
            data.get("sleep_score"),
            data.get("spo2"),
            data.get("stress"),
            data.get("hrv"),
            data.get("calories"),
            data.get("distance"),
            data.get("weight"),
            data.get("body_fat"),
            data.get("bmi"),
            data.get("muscle_mass"),
            data.get("water_rate"),
            data.get("bone_mass"),
            data.get("basal_metabolism"),
            data.get("visceral_fat"),
            data.get("raw_json")
        ))
        conn.commit()
        conn.close()

    async def _periodic_check(self):
        if self._in_quiet_hours():
            return

        if not self._is_baseline_ready():
            return

        for metric, config in self.metrics_config.items():
            if not config.get("enabled", False):
                continue

            latest_value = self._get_latest_metric(metric)
            if latest_value is None:
                continue

            await self._check_metric(metric, latest_value)

    async def _check_metric(self, metric: str, value: float):
        baseline = self._calculate_baseline(metric)
        if baseline is None:
            return

        mean, std = baseline
        if std == 0:
            return

        z_score = (value - mean) / std
        threshold = self.metrics_config.get(metric, {}).get("threshold", 2.0)

        if abs(z_score) > threshold:
            cooldown_hours = self.metrics_config.get(metric, {}).get("cooldown", 4)
            if self._is_in_cooldown(metric, cooldown_hours):
                return

            message = await self._generate_care_message(metric, value, mean, std, z_score)
            await self._send_care_message(message)

            self._record_alert(metric, value, mean, std, z_score, message)
            self.alert_cooldown[metric] = datetime.now()

    def _calculate_baseline(self, metric: str) -> Optional[tuple]:
        conn = self._db_connect()
        c = conn.cursor()

        if self.baseline_mode == "sliding":
            start_date = (datetime.now() - timedelta(days=self.baseline_days)).isoformat()
            c.execute(f"""
                SELECT {metric} FROM raw_data 
                WHERE timestamp >= ? AND {metric} IS NOT NULL
            """, (start_date,))
        else:
            c.execute("SELECT MIN(timestamp) as start FROM raw_data")
            row = c.fetchone()
            if not row or not row[0]:
                conn.close()
                return None
            start = datetime.fromisoformat(row[0])
            end = start + timedelta(days=self.baseline_days)
            c.execute(f"""
                SELECT {metric} FROM raw_data 
                WHERE timestamp >= ? AND timestamp <= ? AND {metric} IS NOT NULL
            """, (start.isoformat(), end.isoformat()))

        rows = c.fetchall()
        conn.close()

        if len(rows) < 10:
            return None

        values = [r[0] for r in rows]
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(variance)

        return (mean, std)

    def _is_baseline_ready(self) -> bool:
        conn = self._db_connect()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM raw_data")
        count = c.fetchone()[0]
        conn.close()

        min_records = self.baseline_days * 24 * 2
        return count >= min_records

    def _get_latest_metric(self, metric: str) -> Optional[float]:
        conn = self._db_connect()
        c = conn.cursor()
        c.execute(f"""
            SELECT {metric} FROM raw_data 
            WHERE {metric} IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """)
        row = c.fetchone()
        conn.close()
        return row[0] if row else None

    def _is_in_cooldown(self, metric: str, hours: int) -> bool:
        if metric not in self.alert_cooldown:
            return False
        last_alert = self.alert_cooldown[metric]
        return (datetime.now() - last_alert) < timedelta(hours=hours)

    def _in_quiet_hours(self) -> bool:
        if not self.quiet_hours.get("enabled", False):
            return False

        now = datetime.now()
        start_str = self.quiet_hours.get("start", "23:00")
        end_str = self.quiet_hours.get("end", "08:00")

        start_time = datetime.strptime(start_str, "%H:%M").time()
        end_time = datetime.strptime(end_str, "%H:%M").time()
        current_time = now.time()

        if start_time < end_time:
            return start_time <= current_time <= end_time
        else:
            return current_time >= start_time or current_time <= end_time

    async def _generate_care_message(self, metric: str, value: float, mean: float, std: float, z_score: float) -> str:
        context = self._get_today_context()
        body_context = self._get_body_composition_context()

        metric_names = {
            "heart_rate": "心率",
            "steps": "步数",
            "sleep_score": "睡眠评分",
            "spo2": "血氧",
            "stress": "压力值"
        }
        metric_name = metric_names.get(metric, metric)

        weight_change = body_context.get("weight_change", "")
        body_fat = body_context.get("body_fat", "")
        bmi = body_context.get("bmi", "")

        body_hint = ""
        if weight_change:
            body_hint += f"体重较昨日{weight_change}。"
        if body_fat:
            body_hint += f"体脂率{body_fat}%。"
        if bmi:
            body_hint += f"BMI {bmi}。"

        prompt = f"""当前时间：{datetime.now().strftime("%H:%M")}
异常指标：{metric_name} {value:.0f}
基线：{mean:.1f} ± {std:.1f}
偏离程度：{z_score:.2f} 个标准差

用户今日状态：
- 步数：{context.get('steps', '未知')}
- 睡眠评分：{context.get('sleep_score', '未知')}
- 血氧：{context.get('spo2', '未知')}%

用户身体成分：
- {body_hint if body_hint else '今日未称重'}

请用自然、温柔的口吻发一条关心消息，不要吓人，50字以内。如果今日有称重数据，可以结合体重变化给出关心。"""

        try:
            provider = await self.context.get_provider_by_id("default")
            if provider:
                req = ProviderRequest()
                req.prompt = prompt
                # 不传 system_prompt，复用 AstrBot 全局人格
                response = await provider.text_chat(req)
                return response.completion_text.strip()
        except Exception as e:
            logger.error(f"[BodyMonitor] LLM error: {e}")

        fallbacks = {
            "heart_rate": f"刚才心率有点{'快' if z_score > 0 else '慢'}哦，记得{'深呼吸休息一下' if z_score > 0 else '活动活动'}~",
            "sleep_score": "昨晚睡眠评分有点低，今天早点休息吧，别熬夜了",
            "spo2": "血氧有点低，注意通风，不舒服的话及时休息",
            "stress": "压力值有点高，深呼吸，放轻松~"
        }
        return fallbacks.get(metric, f"{metric_name}有点异常，注意身体哦~")

    def _get_today_context(self) -> dict:
        conn = self._db_connect()
        c = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")

        context = {}

        c.execute("SELECT MAX(steps) FROM raw_data WHERE timestamp LIKE ? AND steps IS NOT NULL", (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["steps"] = row[0]

        c.execute("SELECT sleep_score FROM raw_data WHERE timestamp LIKE ? AND sleep_score IS NOT NULL ORDER BY timestamp DESC LIMIT 1", (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["sleep_score"] = row[0]

        c.execute("SELECT spo2 FROM raw_data WHERE timestamp LIKE ? AND spo2 IS NOT NULL ORDER BY timestamp DESC LIMIT 1", (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["spo2"] = row[0]

        conn.close()
        return context

    def _get_body_composition_context(self) -> dict:
        conn = self._db_connect()
        c = conn.cursor()

        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        context = {}

        c.execute("""
            SELECT weight FROM raw_data 
            WHERE timestamp LIKE ? AND weight IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        today_weight = row[0] if row and row[0] else None

        c.execute("""
            SELECT weight FROM raw_data 
            WHERE timestamp LIKE ? AND weight IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{yesterday}%",))
        row = c.fetchone()
        yesterday_weight = row[0] if row and row[0] else None

        if today_weight and yesterday_weight:
            diff = today_weight - yesterday_weight
            if abs(diff) >= 0.1:
                context["weight_change"] = f"{'上升' if diff > 0 else '下降'}{abs(diff):.1f}kg"
            else:
                context["weight_change"] = "持平"
        elif today_weight:
            context["weight_change"] = f"{today_weight:.1f}kg"

        c.execute("""
            SELECT body_fat FROM raw_data 
            WHERE timestamp LIKE ? AND body_fat IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["body_fat"] = f"{row[0]:.1f}"

        c.execute("""
            SELECT bmi FROM raw_data 
            WHERE timestamp LIKE ? AND bmi IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["bmi"] = f"{row[0]:.1f}"

        c.execute("""
            SELECT muscle_mass FROM raw_data 
            WHERE timestamp LIKE ? AND muscle_mass IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["muscle_mass"] = f"{row[0]:.1f}"

        c.execute("""
            SELECT water_rate FROM raw_data 
            WHERE timestamp LIKE ? AND water_rate IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["water_rate"] = f"{row[0]:.1f}"

        c.execute("""
            SELECT bone_mass FROM raw_data 
            WHERE timestamp LIKE ? AND bone_mass IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["bone_mass"] = f"{row[0]:.1f}"

        c.execute("""
            SELECT basal_metabolism FROM raw_data 
            WHERE timestamp LIKE ? AND basal_metabolism IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["basal_metabolism"] = f"{row[0]:.0f}"

        c.execute("""
            SELECT visceral_fat FROM raw_data 
            WHERE timestamp LIKE ? AND visceral_fat IS NOT NULL 
            ORDER BY timestamp DESC LIMIT 1
        """, (f"{today}%",))
        row = c.fetchone()
        if row and row[0]:
            context["visceral_fat"] = f"{row[0]:.1f}"

        conn.close()
        return context

    async def _send_care_message(self, message: str):
        umos = self._get_targets()
        if not umos:
            logger.warning("[BodyMonitor] No targets configured, use /body_target_add to add")
            return

        for umo in umos:
            try:
                chain = MessageChain().message(message)
                await self.context.send_message(umo, chain)
                logger.info(f"[BodyMonitor] Care message sent to {umo}")
            except Exception as e:
                logger.error(f"[BodyMonitor] Send error to {umo}: {e}")

    def _get_targets(self) -> List[str]:
        conn = self._db_connect()
        c = conn.cursor()
        c.execute("SELECT umo FROM targets")
        rows = c.fetchall()
        conn.close()
        return [r[0] for r in rows]

    def _record_alert(self, metric: str, value: float, mean: float, std: float, z_score: float, message: str):
        conn = self._db_connect()
        c = conn.cursor()
        c.execute("""
            INSERT INTO alerts (timestamp, metric, value, baseline_mean, baseline_std, z_score, llm_response)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), metric, value, mean, std, z_score, message))
        conn.commit()
        conn.close()

    # ========== 目标平台管理命令 ==========

    @filter.command("body_target_add")
    async def cmd_target_add(self, event: AstrMessageEvent):
        """添加目标会话，格式: /body_target_add <UMO>
        UMO 获取方式：在任何对话中发消息，机器人会记录该会话的 unified_msg_origin
        或者直接用当前会话: /body_target_add here"""
        args = event.message_str.split()
        if len(args) < 2:
            yield event.plain_result("用法: /body_target_add <UMO 或 here>\n"
                                      "例如: /body_target_add here\n"
                                      "或: /body_target_add qq_private:123456789")
            return

        if args[1] == "here":
            umo = event.unified_msg_origin
        else:
            umo = args[1]

        conn = self._db_connect()
        c = conn.cursor()
        try:
            c.execute("INSERT INTO targets (umo, created_at) VALUES (?, ?)", 
                     (umo, datetime.now().isoformat()))
            conn.commit()
            yield event.plain_result(f"✅ 已添加目标会话: {umo}")
        except sqlite3.IntegrityError:
            yield event.plain_result(f"⚠️ 目标已存在: {umo}")
        finally:
            conn.close()

    @filter.command("body_target_remove")
    async def cmd_target_remove(self, event: AstrMessageEvent):
        """移除目标会话"""
        args = event.message_str.split()
        if len(args) < 2:
            yield event.plain_result("用法: /body_target_remove <UMO>")
            return

        umo = args[1]
        conn = self._db_connect()
        c = conn.cursor()
        c.execute("DELETE FROM targets WHERE umo = ?", (umo,))
        conn.commit()
        conn.close()
        yield event.plain_result(f"✅ 已移除目标: {umo}")

    @filter.command("body_target_list")
    async def cmd_target_list(self, event: AstrMessageEvent):
        """列出所有目标会话"""
        umos = self._get_targets()
        if not umos:
            yield event.plain_result("📭 暂无目标会话，使用 /body_target_add here 添加当前会话")
            return

        msg = "📬 当前目标会话:\n"
        for umo in umos:
            msg += f"\n  - {umo}"
        yield event.plain_result(msg)

    # ========== 数据查询命令 ==========

    @filter.command("body_status")
    async def cmd_status(self, event: AstrMessageEvent):
        conn = self._db_connect()
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM raw_data")
        total = c.fetchone()[0]

        baseline_ready = self._is_baseline_ready()

        c.execute("""
            SELECT timestamp, heart_rate, steps, sleep_score, spo2, stress, 
                   weight, body_fat, bmi
            FROM raw_data ORDER BY timestamp DESC LIMIT 1
        """)
        row = c.fetchone()
        conn.close()

        msg = f"📊 身体监测状态\n"
        msg += f"数据总量: {total} 条\n"
        msg += f"基线状态: {'✅ 已就绪' if baseline_ready else '⏳ 收集中...'}\n"

        if baseline_ready:
            msg += f"基线天数: {self.baseline_days} 天\n"
        else:
            msg += f"基线进度: {total}/{self.baseline_days * 24 * 2} 条数据\n"

        if row:
            msg += f"\n📋 最新数据 ({row[0][:16]}):\n"
            if row[1]: msg += f"  心率: {row[1]:.0f} bpm\n"
            if row[2]: msg += f"  步数: {row[2]}\n"
            if row[3]: msg += f"  睡眠评分: {row[3]:.0f}\n"
            if row[4]: msg += f"  血氧: {row[4]:.0f}%\n"
            if row[5]: msg += f"  压力: {row[5]:.0f}\n"
            if row[6]: msg += f"  体重: {row[6]:.1f}kg\n"
            if row[7]: msg += f"  体脂率: {row[7]:.1f}%\n"
            if row[8]: msg += f"  BMI: {row[8]:.1f}\n"

        yield event.plain_result(msg)

    @filter.command("body_baseline")
    async def cmd_baseline(self, event: AstrMessageEvent):
        msg = "📈 基线统计（最近7天）\n"

        for metric in ["heart_rate", "steps", "sleep_score", "spo2", "stress"]:
            baseline = self._calculate_baseline(metric)
            if baseline:
                mean, std = baseline
                msg += f"\n{metric}: {mean:.1f} ± {std:.1f}"
            else:
                msg += f"\n{metric}: 数据不足"

        yield event.plain_result(msg)

    @filter.command("body_body")
    async def cmd_body(self, event: AstrMessageEvent):
        body = self._get_body_composition_context()

        if not body:
            yield event.plain_result("📋 暂无体脂秤数据，请先称重")
            return

        msg = "📋 身体成分数据\n"
        if "weight_change" in body:
            msg += f"\n体重: {body['weight_change']}"
        if "body_fat" in body:
            msg += f"\n体脂率: {body['body_fat']}%"
        if "bmi" in body:
            msg += f"\nBMI: {body['bmi']}"
        if "muscle_mass" in body:
            msg += f"\n肌肉量: {body['muscle_mass']}kg"
        if "water_rate" in body:
            msg += f"\n水分率: {body['water_rate']}%"
        if "bone_mass" in body:
            msg += f"\n骨量: {body['bone_mass']}kg"
        if "basal_metabolism" in body:
            msg += f"\n基础代谢: {body['basal_metabolism']}kcal"
        if "visceral_fat" in body:
            msg += f"\n内脏脂肪: {body['visceral_fat']}"

        yield event.plain_result(msg)

    @filter.command("body_alerts")
    async def cmd_alerts(self, event: AstrMessageEvent):
        conn = self._db_connect()
        c = conn.cursor()
        c.execute("""
            SELECT timestamp, metric, value, z_score, llm_response 
            FROM alerts ORDER BY timestamp DESC LIMIT 5
        """)
        rows = c.fetchall()
        conn.close()

        if not rows:
            yield event.plain_result("✅ 最近没有异常告警")
            return

        msg = "🚨 最近5条异常告警\n"
        for row in rows:
            ts, metric, value, z_score, response = row
            msg += f"\n[{ts[11:16]}] {metric}: {value:.0f} (z={z_score:.2f})\n"
            msg += f"  → {response}\n"

        yield event.plain_result(msg)

    @filter.command("body_test")
    async def cmd_test(self, event: AstrMessageEvent):
        test_msg = "测试关心消息：记得多喝水，注意休息~"
        await self._send_care_message(test_msg)
        yield event.plain_result("✅ 测试消息已发送")

    async def terminate(self):
        # 取消后台任务，避免重载/升级后旧实例继续运行
        for task in (getattr(self, "_periodic_task", None), getattr(self, "_server_task", None)):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        logger.info("[BodyMonitor] Plugin terminated")
