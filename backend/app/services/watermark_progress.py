"""水位推进数据记录业务规则。

在原有水位线配置（只读）之上，补齐空闲事件流的水位推进判定与窗口收口能力：

- 阅读 WATE 配置的时间字段、空闲超时与延迟容忍，校验事件时间后写入推进记录；
- 空闲周期内没有新事件且达到空闲超时才推进水位，推进时保留可确认水位；
- 迟到事件进入缓冲，不重复生成窗口触发；
- 同一份配置并发更新只接受一个版本（乐观版本号 + 按配置加锁）；
- 遇到无数据或处理中断保留原结果，失败项可重试并保留失败原因；
- 跨数据链路同步同一份记录，消费侧维护版本标识。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any

from app.store import store

CONFIG_MODULE = "watermark"
PROGRESS_MODULE = "watermark_progress"

# 支持的时长写法：毫秒/秒/分，兼容纯数字（按秒处理）
_DURATION_UNITS = {"ms": 0.001, "s": 1.0, "秒": 1.0, "m": 60.0, "min": 60.0, "分": 60.0}


def parse_duration(value: Any) -> float | None:
    """把 "30s"、"10秒"、500、"500ms" 这类配置解析成秒；无法解析返回 None。"""
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text.isdigit():
        return float(text)
    unit = next((suffix for suffix in _DURATION_UNITS if text.endswith(suffix)), "")
    number_part = text[: -len(unit)] if unit else text
    try:
        number = float(number_part)
    except ValueError:
        return None
    if number < 0:
        return None
    return number * _DURATION_UNITS.get(unit, 1.0)


def parse_event_time(value: Any) -> datetime | None:
    """解析事件时间，支持 ISO 8601（含毫秒、Z 结尾）和 "YYYY-MM-DD HH:MM:SS"。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="milliseconds") if value else None


class WatermarkProgressService:
    """按水位线配置维护一份可核对的水位推进数据记录。"""

    def __init__(self) -> None:
        # 同一配置的写入串行化，配合版本号保证并发只接受一个版本
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ---------- 基础读取 ----------

    def _lock_for(self, config_code: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(config_code)
            if lock is None:
                lock = threading.Lock()
                self._locks[config_code] = lock
            return lock

    def _find_config(self, config_code: str) -> dict[str, Any] | None:
        for row in store.rows(CONFIG_MODULE):
            if str(row.get("配置编号", "")) == config_code:
                return row
        return None

    def _table(self) -> list[dict[str, Any]]:
        return store.rows(PROGRESS_MODULE)

    def _find_record(self, config_code: str) -> dict[str, Any] | None:
        for record in self._table():
            if record["配置编号"] == config_code:
                return record
        return None

    def list_records(
        self,
        *,
        keyword: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._table()
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("配置编号", ""))]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_record(self, config_code: str) -> dict[str, Any] | None:
        return self._find_record(config_code)

    def _new_record(self, config: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": len(self._table()) + 1,
            "配置编号": config["配置编号"],
            "流名称": config.get("流名称"),
            "时间字段": config.get("时间字段"),
            "空闲超时": config.get("空闲超时"),
            "延迟容忍": config.get("延迟容忍"),
            "版本标识": 0,
            "可确认水位": None,
            "观察最大事件时间": None,
            "最近事件时间": None,
            "最近心跳时间": None,
            "空闲状态": "活跃",
            "迟到事件缓冲": [],
            "触发窗口": [],
            "失败项": [],
            "最近推进原因": "尚未推进",
            "同步链路": [],
            "更新时间": None,
        }

    def _ensure_record(self, config: dict[str, Any]) -> dict[str, Any]:
        record = self._find_record(str(config["配置编号"]))
        if record is None:
            record = self._new_record(config)
            self._table().append(record)
        return record

    def _config_settings(self, config: dict[str, Any]) -> tuple[float | None, float | None, str]:
        """读取并校验配置里的空闲超时、延迟容忍与时间字段，返回 (idle, lag, 错误说明)。"""
        time_field = str(config.get("时间字段") or "").strip()
        if not time_field:
            return None, None, "配置缺少时间字段，无法校验事件时间"
        idle = parse_duration(config.get("空闲超时"))
        if idle is None:
            return None, None, f"空闲超时「{config.get('空闲超时')}」无法解析"
        lag = parse_duration(config.get("延迟容忍"))
        if lag is None:
            return None, None, f"延迟容忍「{config.get('延迟容忍')}」无法解析"
        return idle, lag, ""

    # ---------- 核心判定 ----------

    def _advance_watermark(
        self,
        record: dict[str, Any],
        *,
        confirmed: datetime,
        reason: str,
        now: datetime,
    ) -> None:
        """推进可确认水位，并在越过窗口结束边界时收口窗口（只触发一次）。"""
        previous = parse_event_time(record["可确认水位"])
        if previous is not None and confirmed <= previous:
            # 水位单调不减，不产生重复推进
            return
        record["可确认水位"] = _iso(confirmed)
        record["最近推进原因"] = reason
        record["版本标识"] = int(record["版本标识"]) + 1
        record["更新时间"] = _iso(now)

        closed: list[dict[str, Any]] = []
        for window in record["触发窗口"]:
            if window["窗口状态"] != "等待触发":
                continue
            end_at = parse_event_time(window["窗口结束"])
            if end_at is not None and end_at <= confirmed:
                window["窗口状态"] = "已触发收口"
                window["收口水位"] = record["可确认水位"]
                window["收口版本"] = record["版本标识"]
                window["收口时间"] = _iso(now)
                closed.append({
                    "窗口编号": window["窗口编号"],
                    "窗口结束": window["窗口结束"],
                    "收口版本": window["收口版本"],
                })
        if closed:
            record["最近推进原因"] = f"{reason}；收口窗口 {len(closed)} 个"

    def _classify_event(
        self,
        record: dict[str, Any],
        event: dict[str, Any],
        event_time: datetime,
        now: datetime,
    ) -> str:
        """校验通过后的事件归类：迟到进缓冲，正常事件参与水位计算。返回归类结果。"""
        record["最近事件时间"] = _iso(now)
        record["空闲状态"] = "活跃"
        confirmed = parse_event_time(record["可确认水位"])
        if confirmed is not None and event_time < confirmed:
            # 迟到事件：必须保留在缓冲，且不再触发窗口
            already = {item["事件标识"] for item in record["迟到事件缓冲"]}
            event_id = str(event.get("event_id") or event.get("id") or event_time.isoformat())
            if event_id in already:
                return "重复迟到事件已忽略"
            record["迟到事件缓冲"].append({
                "事件标识": event_id,
                "事件时间": _iso(event_time),
                "进入缓冲时间": _iso(now),
                "当前水位": record["可确认水位"],
                "原因": "事件时间早于可确认水位",
            })
            return "迟到事件已进入缓冲"

        observed = parse_event_time(record["观察最大事件时间"])
        if observed is None or event_time > observed:
            record["观察最大事件时间"] = _iso(event_time)
        return "事件已接收"

    # ---------- 对外动作 ----------

    def ingest_events(
        self,
        config_code: str,
        events: list[dict[str, Any]],
        *,
        expected_version: int | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """写入一批事件：逐项校验事件时间，失败项保留原因并可重试，不影响其他事件。

        now 为处理时间（默认取当前时钟），测试或回放时可显式注入。
        """
        config = self._find_config(config_code)
        if config is None:
            return {"ok": False, "message": f"水位线配置 {config_code} 不存在或已归档", "conflict": False}
        if not isinstance(events, list) or not events:
            return {"ok": False, "message": "事件列表为空，未写入任何数据；原推进记录保持不变", "conflict": False}

        with self._lock_for(config_code):
            record = self._ensure_record(config)
            if expected_version is not None and int(expected_version) != int(record["版本标识"]):
                return self._version_conflict(record)

            idle, lag, settings_error = self._config_settings(config)
            if settings_error:
                # 配置本身不可用：整批记为失败项，原结果保留
                self._append_failures(record, events, settings_error)
                return {
                    "ok": False,
                    "message": f"{settings_error}；原推进记录保持不变，失败项可重试",
                    "record": record,
                    "conflict": False,
                }

            assert idle is not None and lag is not None
            time_field = str(config["时间字段"])
            accepted: list[tuple[dict[str, Any], datetime]] = []
            failures: list[dict[str, Any]] = []
            for event in events:
                if not isinstance(event, dict):
                    failures.append(self._failure_item(event, "事件必须是对象结构"))
                    continue
                raw_time = event.get(time_field)
                event_time = parse_event_time(raw_time)
                if event_time is None:
                    failures.append(
                        self._failure_item(event, f"时间字段「{time_field}」缺失或无法解析：{raw_time!r}")
                    )
                    continue
                accepted.append((event, event_time))

            current = now or datetime.now()
            classified: list[str] = []
            for event, event_time in accepted:
                classified.append(self._classify_event(record, event, event_time, current))

            # 正常事件按 观察最大事件时间 - 延迟容忍 推进水位
            observed = parse_event_time(record["观察最大事件时间"])
            if observed is not None:
                on_time = [item for item in classified if item == "事件已接收"]
                if on_time:
                    candidate = observed - timedelta(seconds=lag)
                    self._advance_watermark(
                        record,
                        confirmed=candidate,
                        reason=f"按事件时间推进（观察最大值 {_iso(observed)} - 延迟容忍 {lag:g}s）",
                        now=current,
                    )
            record["更新时间"] = _iso(current)
            if failures:
                record["失败项"].extend(failures)
                record["版本标识"] = int(record["版本标识"]) + 1
            return {
                "ok": not failures,
                "message": self._ingest_message(len(accepted), len(failures), classified),
                "record": record,
                "conflict": False,
            }

    def idle_tick(
        self,
        config_code: str,
        *,
        now: datetime | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """空闲心跳：空闲周期内没有新事件且达到空闲超时，才推进水位。"""
        config = self._find_config(config_code)
        if config is None:
            return {"ok": False, "message": f"水位线配置 {config_code} 不存在或已归档", "conflict": False}

        with self._lock_for(config_code):
            record = self._ensure_record(config)
            if expected_version is not None and int(expected_version) != int(record["版本标识"]):
                return self._version_conflict(record)

            idle, lag, settings_error = self._config_settings(config)
            if settings_error:
                return {"ok": False, "message": f"{settings_error}；原推进记录保持不变", "record": record, "conflict": False}
            assert idle is not None and lag is not None

            current = now or datetime.now()
            last_event = parse_event_time(record["最近事件时间"])
            last_tick = parse_event_time(record["最近心跳时间"])
            baseline = last_event or last_tick
            record["最近心跳时间"] = _iso(current)

            # 无数据：还没有任何事件/心跳时，无法判断空闲起点，保留原结果
            if baseline is None:
                record["空闲状态"] = "等待数据"
                record["更新时间"] = _iso(current)
                return {
                    "ok": True,
                    "advanced": False,
                    "message": "尚无事件到达，无法计算空闲周期，可确认水位保持原值",
                    "record": record,
                    "conflict": False,
                }

            idle_seconds = (current - baseline).total_seconds()
            if idle_seconds < idle:
                record["空闲状态"] = "活跃"
                return {
                    "ok": True,
                    "advanced": False,
                    "message": (
                        f"空闲 {idle_seconds:.0f}s 未达到空闲超时 {idle:g}s，"
                        "不推进水位，可确认水位保持原值"
                    ),
                    "record": record,
                    "conflict": False,
                }

            observed = parse_event_time(record["观察最大事件时间"])
            if observed is None:
                # 空闲超时但没有可参考的事件时间，保留原结果
                record["空闲状态"] = "空闲"
                return {
                    "ok": True,
                    "advanced": False,
                    "message": "空闲已超时但缺少事件时间基准，可确认水位保持原值",
                    "record": record,
                    "conflict": False,
                }

            # 空闲周期内没有新事件且达到超时：按处理时间对齐推进
            # 可确认水位 = 心跳时间 - 空闲超时 - 延迟容忍，并与事件时间口径取较大者
            idle_watermark = current - timedelta(seconds=idle + lag)
            event_watermark = observed - timedelta(seconds=lag)
            confirmed = max(idle_watermark, event_watermark)
            previous = parse_event_time(record["可确认水位"])
            if previous is not None and confirmed <= previous:
                record["空闲状态"] = "空闲"
                return {
                    "ok": True,
                    "advanced": False,
                    "message": "空闲已超时但计算水位未超过当前可确认水位，保持原值",
                    "record": record,
                    "conflict": False,
                }

            record["空闲状态"] = "空闲"
            self._advance_watermark(
                record,
                confirmed=confirmed,
                reason=(
                    f"空闲超时推进（空闲 {idle_seconds:.0f}s ≥ {idle:g}s，"
                    f"心跳 {_iso(current)} - 空闲超时 - 延迟容忍 {lag:g}s）"
                ),
                now=current,
            )
            return {
                "ok": True,
                "advanced": True,
                "message": f"空闲周期无新事件且达到 {idle:g}s 超时，可确认水位推进至 {record['可确认水位']}",
                "record": record,
                "conflict": False,
            }

    def register_window(
        self,
        config_code: str,
        window: dict[str, Any],
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """登记一个待收口窗口；窗口结束时间越过可确认水位时才允许收口。"""
        config = self._find_config(config_code)
        if config is None:
            return {"ok": False, "message": f"水位线配置 {config_code} 不存在或已归档", "conflict": False}
        window_code = str(window.get("窗口编号") or "").strip()
        end_at = parse_event_time(window.get("窗口结束"))
        if not window_code:
            return {"ok": False, "message": "窗口编号缺失，未登记", "conflict": False}
        if end_at is None:
            return {"ok": False, "message": f"窗口结束时间无法解析：{window.get('窗口结束')!r}", "conflict": False}

        with self._lock_for(config_code):
            record = self._ensure_record(config)
            if expected_version is not None and int(expected_version) != int(record["版本标识"]):
                return self._version_conflict(record)
            for existing in record["触发窗口"]:
                if existing["窗口编号"] == window_code:
                    return {"ok": False, "message": f"窗口 {window_code} 已登记，不能重复登记", "record": record, "conflict": False}
            confirmed = parse_event_time(record["可确认水位"])
            status = "已触发收口" if confirmed is not None and end_at <= confirmed else "等待触发"
            record["触发窗口"].append({
                "窗口编号": window_code,
                "窗口开始": window.get("窗口开始"),
                "窗口结束": _iso(end_at),
                "窗口状态": status,
                "收口水位": record["可确认水位"] if status == "已触发收口" else None,
                "收口版本": record["版本标识"] if status == "已触发收口" else None,
                "收口时间": None,
            })
            record["版本标识"] = int(record["版本标识"]) + 1
            record["更新时间"] = _iso(datetime.now())
            return {
                "ok": True,
                "message": f"窗口 {window_code} 已登记，状态：{status}",
                "record": record,
                "conflict": False,
            }

    def close_window(
        self,
        config_code: str,
        window_code: str,
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """窗口收口写入：只能依据当前这份可核对记录，且必须带对版本标识。

        - 版本不一致：判定为结果版本冲突，拒绝写入，不关闭窗口；
        - 窗口仍可能接收迟到事件（结束时间未越过可确认水位）：拒绝提前关闭。
        """
        config = self._find_config(config_code)
        if config is None:
            return {"ok": False, "message": f"水位线配置 {config_code} 不存在或已归档", "conflict": False}

        with self._lock_for(config_code):
            record = self._ensure_record(config)
            if expected_version is not None and int(expected_version) != int(record["版本标识"]):
                return self._version_conflict(record)
            target = next((w for w in record["触发窗口"] if w["窗口编号"] == window_code), None)
            if target is None:
                return {"ok": False, "message": f"窗口 {window_code} 未在推进记录中登记", "record": record, "conflict": False}
            if target["窗口状态"] == "已触发收口":
                return {"ok": False, "message": f"窗口 {window_code} 已收口，不能重复触发", "record": record, "conflict": False}

            confirmed = parse_event_time(record["可确认水位"])
            end_at = parse_event_time(target["窗口结束"])
            if confirmed is None or end_at is None or end_at > confirmed:
                return {
                    "ok": False,
                    "conflict": False,
                    "message": (
                        f"窗口 {window_code} 结束时间 {target['窗口结束']} 尚未越过可确认水位 "
                        f"{record['可确认水位']}，仍可能接收迟到事件，拒绝提前关闭"
                    ),
                    "record": record,
                }

            target["窗口状态"] = "已触发收口"
            target["收口水位"] = record["可确认水位"]
            target["收口版本"] = int(record["版本标识"]) + 1
            target["收口时间"] = _iso(datetime.now())
            record["版本标识"] = int(record["版本标识"]) + 1
            record["更新时间"] = target["收口时间"]
            return {
                "ok": True,
                "message": f"窗口 {window_code} 依据版本 {record['版本标识']} 的推进记录完成收口",
                "record": record,
                "conflict": False,
            }

    def retry_failures(self, config_code: str, *, now: datetime | None = None) -> dict[str, Any]:
        """重试失败项：重新校验事件时间，成功则走正常归类，仍失败则刷新失败原因。"""
        config = self._find_config(config_code)
        if config is None:
            return {"ok": False, "message": f"水位线配置 {config_code} 不存在或已归档", "conflict": False}

        with self._lock_for(config_code):
            record = self._find_record(config_code)
            if record is None or not record["失败项"]:
                return {"ok": True, "message": "没有待重试的失败项", "record": record, "conflict": False}

            idle, lag, settings_error = self._config_settings(config)
            pending = record["失败项"]
            record["失败项"] = []
            if settings_error:
                for item in pending:
                    item["重试次数"] = int(item.get("重试次数", 0)) + 1
                    item["最近失败原因"] = settings_error
                record["失败项"] = pending
                return {"ok": False, "message": f"{settings_error}；失败项保留，可稍后再试", "record": record, "conflict": False}
            assert idle is not None and lag is not None
            time_field = str(config["时间字段"])
            current = now or datetime.now()
            still_failed: list[dict[str, Any]] = []
            replayed: list[str] = []
            for item in pending:
                event = item.get("原始事件") if isinstance(item.get("原始事件"), dict) else {}
                event_time = parse_event_time(event.get(time_field))
                if event_time is None:
                    item["重试次数"] = int(item.get("重试次数", 0)) + 1
                    item["最近失败原因"] = f"时间字段「{time_field}」仍无法解析：{event.get(time_field)!r}"
                    still_failed.append(item)
                    continue
                replayed.append(self._classify_event(record, event, event_time, current))

            observed = parse_event_time(record["观察最大事件时间"])
            if observed is not None and any(text == "事件已接收" for text in replayed):
                candidate = observed - timedelta(seconds=lag)
                self._advance_watermark(
                    record,
                    confirmed=candidate,
                    reason="失败项重试成功后按事件时间推进",
                    now=current,
                )
            record["失败项"] = still_failed
            record["版本标识"] = int(record["版本标识"]) + 1
            record["更新时间"] = _iso(current)
            return {
                "ok": not still_failed,
                "message": (
                    f"重试 {len(pending)} 项，成功 {len(pending) - len(still_failed)} 项"
                    + (f"，仍失败 {len(still_failed)} 项并保留原因" if still_failed else "")
                ),
                "record": record,
                "conflict": False,
            }

    def sync_link(
        self,
        config_code: str,
        link_name: str,
        *,
        consumer_version: int | None = None,
    ) -> dict[str, Any]:
        """跨数据链路同步：只下发同一份推进记录，消费侧维护版本标识。

        消费侧带来的版本落后于当前版本才同步；相同版本视为已同步；版本超前判定冲突。
        """
        config = self._find_config(config_code)
        if config is None:
            return {"ok": False, "message": f"水位线配置 {config_code} 不存在或已归档", "conflict": False}

        with self._lock_for(config_code):
            record = self._ensure_record(config)
            current_version = int(record["版本标识"])
            link = next((item for item in record["同步链路"] if item["链路名称"] == link_name), None)
            if link is None:
                link = {"链路名称": link_name, "消费侧版本": 0, "最近同步版本": None, "最近同步时间": None}
                record["同步链路"].append(link)

            if consumer_version is not None and int(consumer_version) > current_version:
                return {
                    "ok": False,
                    "conflict": True,
                    "message": (
                        f"链路 {link_name} 消费侧版本 {consumer_version} 超前于当前版本 {current_version}，"
                        "结果版本存在冲突，拒绝同步"
                    ),
                    "record": record,
                }
            if consumer_version is not None and int(consumer_version) == current_version:
                link["消费侧版本"] = current_version
                return {
                    "ok": True,
                    "synced": False,
                    "message": f"链路 {link_name} 已是版本 {current_version}，无需重复同步",
                    "record": record,
                    "conflict": False,
                }

            link["消费侧版本"] = current_version
            link["最近同步版本"] = current_version
            link["最近同步时间"] = _iso(datetime.now())
            record["更新时间"] = link["最近同步时间"]
            return {
                "ok": True,
                "synced": True,
                "message": f"链路 {link_name} 已同步版本 {current_version} 的同一份推进记录",
                "record": record,
                "conflict": False,
            }

    # ---------- 内部工具 ----------

    def _version_conflict(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": False,
            "conflict": True,
            "message": (
                f"版本冲突：提交版本与当前版本标识 {record['版本标识']} 不一致，"
                "同一配置并发更新只接受一个版本，请读取最新记录后重试"
            ),
            "record": record,
        }

    def _failure_item(self, event: Any, reason: str) -> dict[str, Any]:
        return {
            "事件标识": str(event.get("event_id") or event.get("id")) if isinstance(event, dict) else str(event),
            "原始事件": event if isinstance(event, dict) else {"raw": event},
            "失败原因": reason,
            "最近失败原因": reason,
            "重试次数": 0,
        }

    def _append_failures(self, record: dict[str, Any], events: list[dict[str, Any]], reason: str) -> None:
        for event in events:
            record["失败项"].append(self._failure_item(event, reason))
        record["版本标识"] = int(record["版本标识"]) + 1
        record["更新时间"] = _iso(datetime.now())

    def _ingest_message(
        self,
        accepted_count: int,
        failure_count: int,
        classified: list[str],
    ) -> str:
        buffered = sum(1 for text in classified if "缓冲" in text)
        duplicate = sum(1 for text in classified if "重复" in text)
        parts = [f"接收 {accepted_count} 个事件"]
        if buffered:
            parts.append(f"{buffered} 个迟到事件进入缓冲")
        if duplicate:
            parts.append(f"{duplicate} 个重复迟到事件已忽略")
        if failure_count:
            parts.append(f"{failure_count} 个事件校验失败并保留原因，可重试")
        return "；".join(parts)


service = WatermarkProgressService()
