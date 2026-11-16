"""水位推进数据记录端到端验收脚本（不依赖网络，直接驱动服务层）。

覆盖：
1. 验收样例 WATE-0002：ts / idle=30s / lag=10s，10:00-10:00:35 无事件推进水位，late-event-24 进缓冲；
2. 空闲未达超时不推进、无数据保留原结果；
3. 并发更新同一配置只接受一个版本（乐观版本号）；
4. 迟到事件保留缓冲且不重复生成窗口触发；
5. 窗口收口必须依据可核对记录、拒绝提前关闭、版本冲突拒绝；
6. 失败项重试并说明原因；
7. 跨链路同步同一份记录，消费侧维护版本标识。
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.watermark_progress import service  # noqa: E402

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name}" + (f" -> {detail}" if detail else ""))
    if not condition:
        failures.append(name)


def ts(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 11, 16, hour, minute, second)


def iso(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds")


# ---------- 1. 验收样例 WATE-0002 ----------
print("== 验收样例 WATE-0002 ==")
config = service._find_config("WATE-0002")
check("读取 WATE-0002 配置", config is not None)
check("时间字段为 ts", config and config.get("时间字段") == "ts", str(config and config.get("时间字段")))
idle, lag, err = service._config_settings(config)
check("空闲超时解析为 30s", idle == 30, f"idle={idle}")
check("延迟容忍解析为 10s", lag == 10, f"lag={lag}")

# 10:00 到达一个事件
r = service.ingest_events(
    "WATE-0002",
    [{"event_id": "evt-1", "ts": iso(ts(10, 0, 0))}],
    now=ts(10, 0, 0),
)
check("10:00 事件写入成功", r["ok"], r["message"])
record = service.get_record("WATE-0002")
# 事件时间口径水位 = 10:00:00 - 10s = 09:59:50
check("事件后可确认水位=09:59:50", record["可确认水位"] == iso(ts(9, 59, 50)), record["可确认水位"])
version_after_event = record["版本标识"]

# 10:00:20 心跳：空闲 20s < 30s，不推进
r = service.idle_tick("WATE-0002", now=ts(10, 0, 20))
check("空闲 20s 未达超时不推进", r["ok"] and r["advanced"] is False, r["message"])
record = service.get_record("WATE-0002")
check("未推进时水位保持原值", record["可确认水位"] == iso(ts(9, 59, 50)), record["可确认水位"])
check("未推进时版本不变", record["版本标识"] == version_after_event, str(record["版本标识"]))

# 10:00:35 心跳：空闲 35s >= 30s，推进
r = service.idle_tick("WATE-0002", now=ts(10, 0, 35))
check("空闲 35s 达到超时推进水位", r["ok"] and r["advanced"] is True, r["message"])
record = service.get_record("WATE-0002")
# 空闲口径 = 10:00:35 - 30s - 10s = 10:00:-05 -> 09:59:55；事件口径 09:59:50，取大 => 09:59:55
check("推进后可确认水位=09:59:55", record["可确认水位"] == iso(ts(9, 59, 55)), record["可确认水位"])
check("推进后版本递增", record["版本标识"] == version_after_event + 1, str(record["版本标识"]))
check("推进原因记录空闲超时", "空闲超时推进" in record["最近推进原因"], record["最近推进原因"])

# late-event-24：事件时间 09:59:00 早于水位 09:59:55，进入缓冲
r = service.ingest_events(
    "WATE-0002",
    [{"event_id": "late-event-24", "ts": iso(ts(9, 59, 0))}],
    now=ts(10, 0, 35),
)
check("late-event-24 写入被接收", r["ok"], r["message"])
record = service.get_record("WATE-0002")
buffered_ids = [item["事件标识"] for item in record["迟到事件缓冲"]]
check("late-event-24 进入迟到事件缓冲", "late-event-24" in buffered_ids, str(buffered_ids))
check("迟到事件不回退水位", record["可确认水位"] == iso(ts(9, 59, 55)), record["可确认水位"])

# 重复迟到事件不能重复进入缓冲、不能重复触发窗口
r = service.ingest_events(
    "WATE-0002",
    [{"event_id": "late-event-24", "ts": iso(ts(9, 59, 0))}],
    now=ts(10, 0, 36),
)
record = service.get_record("WATE-0002")
check("重复迟到事件只保留一份", len(record["迟到事件缓冲"]) == 1, str(len(record["迟到事件缓冲"])))

# 可核对结果三块都在记录里
for field in ("可确认水位", "迟到事件缓冲", "触发窗口"):
    check(f"记录保留字段：{field}", field in record)

# ---------- 2. 无数据与处理中断保留原结果 ----------
print("== 无数据保留原结果 ==")
r = service.ingest_events("WATE-0001", [])
check("空事件批次拒绝且说明原因", r["ok"] is False, r["message"])
check("空批次未生成推进记录", service.get_record("WATE-0001") is None)
r = service.idle_tick("WATE-0001", now=ts(10, 0, 0))
record = service.get_record("WATE-0001")
check("无数据心跳保留空结果不推进", r["advanced"] is False and record["可确认水位"] is None, r["message"])

# ---------- 3. 事件时间校验失败 + 失败项重试 ----------
print("== 失败项重试 ==")
r = service.ingest_events(
    "WATE-0002",
    [
        {"event_id": "bad-1", "ts": "not-a-time"},
        {"event_id": "bad-2"},
        {"event_id": "ok-2", "ts": iso(ts(10, 1, 0))},
    ],
    now=ts(10, 1, 0),
)
check("批次部分失败仍返回可读结果", r["ok"] is False, r["message"])
record = service.get_record("WATE-0002")
check("失败项保留原因", len(record["失败项"]) == 2, str(len(record["失败项"])))
check("同批正常事件仍被接收", record["观察最大事件时间"] == iso(ts(10, 1, 0)), record["观察最大事件时间"])
# 修正原始事件后重试
for item in record["失败项"]:
    item["原始事件"]["ts"] = iso(ts(10, 1, 5))
r = service.retry_failures("WATE-0002", now=ts(10, 1, 5))
check("重试成功清空失败项", r["ok"] and service.get_record("WATE-0002")["失败项"] == [], r["message"])
# 再次重试无失败项
r = service.retry_failures("WATE-0002", now=ts(10, 1, 6))
check("无失败项时重试安全返回", r["ok"], r["message"])
# 仍然非法的事件重试后继续保留原因
service.ingest_events(
    "WATE-0002",
    [{"event_id": "bad-3", "ts": "xx"}],
    now=ts(10, 1, 10),
)
r = service.retry_failures("WATE-0002", now=ts(10, 1, 11))
record = service.get_record("WATE-0002")
check("重试仍失败保留原因与次数", not r["ok"] and record["失败项"][0]["重试次数"] == 1, r["message"])

# ---------- 4. 并发更新只接受一个版本 ----------
print("== 乐观版本控制 ==")
record = service.get_record("WATE-0002")
stale_version = record["版本标识"] - 1
r = service.ingest_events(
    "WATE-0002",
    [{"event_id": "concurrent-1", "ts": iso(ts(10, 2, 0))}],
    expected_version=stale_version,
    now=ts(10, 2, 0),
)
check("旧版本并发写入被拒绝", r["ok"] is False and r["conflict"] is True, r["message"])
r = service.idle_tick("WATE-0002", now=ts(10, 5, 0), expected_version=stale_version)
check("旧版本心跳被拒绝", r["conflict"] is True, r["message"])
current = service.get_record("WATE-0002")["版本标识"]
r = service.ingest_events(
    "WATE-0002",
    [{"event_id": "concurrent-2", "ts": iso(ts(10, 2, 0))}],
    expected_version=current,
    now=ts(10, 2, 1),
)
check("当前版本写入被接受", r["ok"] and not r["conflict"], r["message"])

# ---------- 5. 窗口登记、收口与迟到事件保护 ----------
print("== 窗口收口 ==")
record = service.get_record("WATE-0002")
current = record["版本标识"]
# 登记一个结束于 10:05:00 的窗口，当前水位远早于它，应等待触发
r = service.register_window(
    "WATE-0002",
    {"窗口编号": "WIN-24", "窗口开始": iso(ts(10, 0, 0)), "窗口结束": iso(ts(10, 5, 0))},
    expected_version=current,
)
check("窗口登记成功", r["ok"], r["message"])
record = service.get_record("WATE-0002")
win = next(w for w in record["触发窗口"] if w["窗口编号"] == "WIN-24")
check("新窗口处于等待触发", win["窗口状态"] == "等待触发", win["窗口状态"])

# 提前收口必须被拒绝（窗口仍可能接收迟到事件）
r = service.close_window("WATE-0002", "WIN-24", expected_version=record["版本标识"])
check("拒绝提前关闭未越过水位的窗口", not r["ok"], r["message"])
# 错误版本收口同样被拒绝
r = service.close_window("WATE-0002", "WIN-24", expected_version=record["版本标识"] + 99)
check("版本冲突时拒绝收口", r["conflict"] is True, r["message"])

# 推进水位越过 10:05:00：心跳到 10:05:41 => 10:05:41 - 40s = 10:05:01
r = service.idle_tick("WATE-0002", now=ts(10, 5, 41))
check("水位越过窗口结束边界", r["ok"], r["message"])
record = service.get_record("WATE-0002")
win = next(w for w in record["触发窗口"] if w["窗口编号"] == "WIN-24")
check("水位推进自动收口窗口（只触发一次）", win["窗口状态"] == "已触发收口", win["窗口状态"])
check("收口记录关联水位与版本", win["收口水位"] == record["可确认水位"] and win["收口版本"] == record["版本标识"])
# 重复收口
r = service.close_window("WATE-0002", "WIN-24", expected_version=record["版本标识"])
check("已收口窗口不能重复触发", not r["ok"], r["message"])
# 迟到事件依旧只在缓冲，没有产生第二条窗口触发
record = service.get_record("WATE-0002")
check("迟到事件仍在缓冲且窗口只触发一次",
      len(record["迟到事件缓冲"]) >= 1 and
      sum(1 for w in record["触发窗口"] if w["窗口编号"] == "WIN-24") == 1)

# ---------- 6. 跨数据链路同步 ----------
print("== 跨链路同步 ==")
record = service.get_record("WATE-0002")
current = record["版本标识"]
r = service.sync_link("WATE-0002", "downstream-a", consumer_version=0)
check("链路按当前版本完成同步", r["ok"] and r["synced"] is True, r["message"])
record = service.get_record("WATE-0002")
link = next(item for item in record["同步链路"] if item["链路名称"] == "downstream-a")
check("消费侧维护版本标识", link["消费侧版本"] == current, str(link))
# 同版本重复同步不重复下发
r = service.sync_link("WATE-0002", "downstream-a", consumer_version=current)
check("相同版本无需重复同步", r["ok"] and r["synced"] is False, r["message"])
# 消费侧版本超前 => 冲突
r = service.sync_link("WATE-0002", "downstream-b", consumer_version=current + 5)
check("消费侧版本超前判定冲突", r["conflict"] is True, r["message"])
# 同步下发的是同一份记录（可核对三要素）
synced_record = r["record"]
check("同步载荷为同一份可核对记录",
      synced_record["可确认水位"] == record["可确认水位"]
      and "迟到事件缓冲" in synced_record and "触发窗口" in synced_record)

print()
if failures:
    print(f"共 {len(failures)} 项失败：{failures}")
    sys.exit(1)
print("全部验收项通过")
