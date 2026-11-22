"""落盘数据检查专用接口：清单版本管理、检查结果签发、湖仓受理与源区间核对。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services.data_diff import DataDiffService
from app.services.sink_check import SinkCheckService
from app.services.sink_manifest import SinkManifestService

router = APIRouter(prefix="/api/sink-check", tags=["落盘数据检查"])

manifest_service = SinkManifestService()
check_service = SinkCheckService()
diff_service = DataDiffService()


class ManifestPayload(BaseModel):
    stream: str = Field(description="数据流")
    version: str = Field(description="清单版本")
    target: str = Field(default="", description="目标载体")
    files: list[str] = Field(default_factory=list, description="清单内文件身份明细")
    registered_at: str = Field(default="2026-11-22 00:00:00")
    activate: bool = True


class SwitchManifestPayload(BaseModel):
    stream: str
    version: str


class RegisterCheckPayload(BaseModel):
    stream: str
    batch: str
    start_position: str
    end_position: str
    manifest_version: str = ""
    zero_row_credential: str = ""
    landed_files: list[str] = Field(default_factory=list)
    checked_at: str = "2026-11-22 00:00:00"


class LandFilePayload(BaseModel):
    stream: str
    batch: str
    files: list[str]


class AcceptPayload(BaseModel):
    stream: str
    batch: str
    active_manifest_version: str | None = None


def _result(ok: bool, message: str, entry: dict[str, Any] | None = None) -> ActionResult:
    return ActionResult(ok=ok, message=message, entry=entry)


@router.post("/manifests", response_model=ActionResult)
def register_manifest(payload: ManifestPayload) -> ActionResult:
    entry, error = manifest_service.register_manifest(
        stream=payload.stream,
        version=payload.version,
        target=payload.target,
        files=payload.files,
        registered_at=payload.registered_at,
        activate=payload.activate,
    )
    return _result(not error, error or "落盘清单已登记", entry)


@router.post("/manifests/switch-active", response_model=ActionResult)
def switch_active_manifest(payload: SwitchManifestPayload) -> ActionResult:
    entry, error = manifest_service.switch_active_manifest(payload.stream, payload.version)
    return _result(not error, error or f"活动清单已切换到 {payload.version}", entry)


@router.post("/register", response_model=ActionResult)
def register_check(payload: RegisterCheckPayload) -> ActionResult:
    entry, error = check_service.register_check(
        stream=payload.stream,
        batch=payload.batch,
        start_position=payload.start_position,
        end_position=payload.end_position,
        manifest_version=payload.manifest_version,
        zero_row_credential=payload.zero_row_credential,
        landed_files=payload.landed_files,
        checked_at=payload.checked_at,
    )
    return _result(not error, error or f"落盘检查已签发：{entry['status'] if entry else ''}", entry)


@router.post("/land-file", response_model=ActionResult)
def land_file(payload: LandFilePayload) -> ActionResult:
    entry, error = check_service.land_file(payload.stream, payload.batch, payload.files)
    return _result(not error, error or f"补文件后复核完成：{entry['status'] if entry else ''}", entry)


@router.post("/accept", response_model=ActionResult)
def accept_for_commit(payload: AcceptPayload) -> ActionResult:
    entry, message = check_service.accept_for_commit(
        payload.stream,
        payload.batch,
        active_manifest_version=payload.active_manifest_version,
    )
    return _result(entry is not None, message, entry)


@router.get("/verify")
def verify_by_source_range(
    start_position: str,
    end_position: str,
    stream: str | None = None,
) -> dict[str, Any]:
    return diff_service.verify_by_source_range(
        start_position, end_position, stream=stream
    )
