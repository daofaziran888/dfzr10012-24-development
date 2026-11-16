"""水位推进数据记录接口。

围绕空闲事件流的水位推进判定与窗口收口提供可核对接口：
事件写入、空闲心跳、窗口登记/收口、失败项重试、跨链路同步。
所有写接口都支持 expected_version 做乐观并发控制。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult, PageResult
from app.services.watermark_progress import parse_event_time, service

router = APIRouter(prefix="/api/watermark-progress", tags=["水位推进数据记录"])


class IngestPayload(BaseModel):
    events: list[dict[str, Any]] = Field(default_factory=list)
    expected_version: int | None = None
    processed_at: str | None = None


class TickPayload(BaseModel):
    now: str | None = None
    expected_version: int | None = None


class WindowPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = None


class ClosePayload(BaseModel):
    expected_version: int | None = None


class RetryPayload(BaseModel):
    processed_at: str | None = None


class SyncPayload(BaseModel):
    link_name: str
    consumer_version: int | None = None


def _to_action(result: dict[str, Any]) -> ActionResult:
    return ActionResult(
        ok=bool(result.get("ok")),
        message=str(result.get("message", "")),
        entry=result.get("record"),
    )


@router.get("", response_model=PageResult[dict])
def list_records(
    keyword: str | None = Query(default=None, description="按配置编号检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """列出全部水位推进数据记录；没有数据时返回空页。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_records(keyword=keyword, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{config_code}", response_model=dict)
def get_record(config_code: str) -> dict[str, Any]:
    """读取单条水位推进数据记录，核对可确认水位、迟到事件缓冲与触发窗口。"""
    record = service.get_record(config_code)
    if record is None:
        raise HTTPException(status_code=404, detail=f"配置 {config_code} 暂无水位推进数据记录")
    return record


@router.post("/{config_code}/events", response_model=ActionResult)
def ingest_events(config_code: str, payload: IngestPayload) -> ActionResult:
    """写入事件：校验配置的时间字段后归类，失败项保留原因且不影响其他事件。"""
    processed_at = parse_event_time(payload.processed_at) if payload.processed_at else None
    if payload.processed_at and processed_at is None:
        raise HTTPException(status_code=400, detail=f"处理时间无法解析：{payload.processed_at!r}")
    result = service.ingest_events(
        config_code,
        payload.events,
        expected_version=payload.expected_version,
        now=processed_at,
    )
    return _to_action(result)


@router.post("/{config_code}/tick", response_model=ActionResult)
def idle_tick(config_code: str, payload: TickPayload) -> ActionResult:
    """空闲心跳：仅当空闲周期内无新事件且达到空闲超时才推进可确认水位。"""
    now = parse_event_time(payload.now) if payload.now else datetime.now()
    if payload.now and now is None:
        raise HTTPException(status_code=400, detail=f"心跳时间无法解析：{payload.now!r}")
    result = service.idle_tick(
        config_code,
        now=now,
        expected_version=payload.expected_version,
    )
    return _to_action(result)


@router.post("/{config_code}/windows", response_model=ActionResult)
def register_window(config_code: str, payload: WindowPayload) -> ActionResult:
    """登记待收口窗口，窗口结束时间越过可确认水位时才会被收口。"""
    result = service.register_window(
        config_code,
        payload.values,
        expected_version=payload.expected_version,
    )
    return _to_action(result)


@router.post("/{config_code}/windows/{window_code}/close", response_model=ActionResult)
def close_window(config_code: str, window_code: str, payload: ClosePayload) -> ActionResult:
    """窗口收口写入：校验版本标识，拒绝提前关闭仍可能接收迟到事件的窗口。"""
    result = service.close_window(
        config_code,
        window_code,
        expected_version=payload.expected_version,
    )
    return _to_action(result)


@router.post("/{config_code}/retry", response_model=ActionResult)
def retry_failures(config_code: str, payload: RetryPayload | None = None) -> ActionResult:
    """重试失败项：重新校验事件时间，仍失败的项刷新并保留失败原因。"""
    processed_at = None
    if payload is not None and payload.processed_at:
        processed_at = parse_event_time(payload.processed_at)
        if processed_at is None:
            raise HTTPException(status_code=400, detail=f"处理时间无法解析：{payload.processed_at!r}")
    result = service.retry_failures(config_code, now=processed_at)
    return _to_action(result)


@router.post("/{config_code}/sync", response_model=ActionResult)
def sync_link(config_code: str, payload: SyncPayload) -> ActionResult:
    """跨数据链路同步同一份推进记录，消费侧提交并维护版本标识。"""
    name = payload.link_name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="链路名称不能为空")
    result = service.sync_link(config_code, name, consumer_version=payload.consumer_version)
    return _to_action(result)
