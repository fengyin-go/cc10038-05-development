"""分片接入路由：批量交付、整批缺口视图、授权解析、行号映射与冲突裁决。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.shard_delivery import shard_ingest

router = APIRouter(prefix="/api/shard-ingest", tags=["分片接入"])


class ShardPayload(BaseModel):
    shard_no: str = Field(..., min_length=1, description="片号，允许非连续乱序到达")
    lines: list[str] = Field(default_factory=list, description="片内原始正文行，按行顺序")
    start_line: int | None = Field(default=None, ge=1, description="起始原始行号；缺省接整批末尾")


class DeliverPayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    source_id: str | None = None
    time_window: str = ""
    shards: list[ShardPayload] = Field(..., min_length=1)


class ParseAuthorizedPayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    parser_rule_name: str | None = None


class ResolveConflictPayload(BaseModel):
    manifest_id: int
    adopt_version: str = Field(..., min_length=1)


@router.post("/deliver")
def deliver_shards(payload: DeliverPayload) -> dict[str, Any]:
    """一次提交多个分片（可多选），服务端完成区间校验、幂等复用与冲突留痕。"""
    return shard_ingest.deliver(
        payload.batch_no,
        [shard.model_dump() for shard in payload.shards],
        source_id=payload.source_id,
        time_window=payload.time_window,
    )


@router.get("/batch/{batch_no}/view")
def batch_view(batch_no: str) -> dict[str, Any]:
    """整批分片视图：缺失片号、待补行区间、行状态网格、冲突清单。"""
    return shard_ingest.batch_view(batch_no)


@router.get("/batch/{batch_no}/authorized-lines")
def authorized_lines(batch_no: str) -> dict[str, Any]:
    """实际获准解析的原始行（缺口行与未裁决冲突版本自动排除）。"""
    return shard_ingest.authorized_lines(batch_no)


@router.post("/parse-authorized")
def parse_authorized(payload: ParseAuthorizedPayload) -> dict[str, Any]:
    """按分片交付清单的授权范围解析，并把结果回写采集待处理清单。"""
    return shard_ingest.parse_authorized(payload.batch_no, payload.parser_rule_name)


@router.get("/batch/{batch_no}/line-map")
def line_map(batch_no: str) -> dict[str, Any]:
    """原始行号 ↔ 成品行号全量映射，收齐前后均可对照。"""
    return shard_ingest.line_number_map(batch_no)


@router.get("/batch/{batch_no}/conflicts")
def list_conflicts(batch_no: str) -> dict[str, Any]:
    """批次内全部片号冲突及逐行差异。"""
    view = shard_ingest.batch_view(batch_no)
    if not view.get("ok"):
        return view
    detailed = []
    for conflict in view["conflicts"]:
        diff = shard_ingest.conflict_diff(batch_no, conflict["片号"])
        detailed.append(diff["conflict"] if diff.get("ok") else conflict)
    return {"ok": True, "batch_no": batch_no, "conflicts": detailed}


@router.get("/batch/{batch_no}/shards/{shard_no}/conflict-diff")
def conflict_diff(batch_no: str, shard_no: str) -> dict[str, Any]:
    """同片号两次接入的版本与逐行正文差异。"""
    return shard_ingest.conflict_diff(batch_no, shard_no)


@router.post("/conflicts/resolve")
def resolve_conflict(payload: ResolveConflictPayload) -> dict[str, Any]:
    """人工裁决：指定该片授权采用的正文版本。"""
    return shard_ingest.resolve_conflict(payload.manifest_id, payload.adopt_version)
