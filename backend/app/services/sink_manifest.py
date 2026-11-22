"""落盘清单服务：登记每个批次的落盘文件明细与清单版本，管理活动/冻结版本。

写入端同一批次可能存在多个清单版本（失败重试、相邻批次文件误入等场景），
活动清单可被切换，但已用于落盘检查的版本会冻结保留，仍可按身份验证。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SinkManifestService(BaseModuleService):
    MODULE = "sink_manifest"
    ENTITY = "落盘清单"
    OPERATION_TERM = "登记"
    LIST_FIELDS = [
        "清单编号", "数据流", "清单版本", "版本状态", "目标载体",
        "文件总数", "文件明细", "登记时间",
    ]
    REQUIRED_FIELDS = ["清单编号", "数据流", "清单版本"]
    STATUS_ORDER = ["生效中", "已冻结"]
    ACTION_RULES = {"冻结版本": "已冻结"}
    NEGATIVE_ACTIONS = []

    def active_manifest(self, stream: str) -> dict[str, Any] | None:
        """取数据流当前活动清单；没有活动版本时返回 None（调用方负责报错）。"""
        for row in reversed(store.rows(self.MODULE)):
            if row.get("数据流") == stream and row.get("版本状态") == "活动":
                return row
        return None

    def get_manifest_version(self, stream: str, version: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("数据流") == stream and row.get("清单版本") == version:
                return row
        return None

    def register_manifest(
        self,
        *,
        stream: str,
        version: str,
        target: str,
        files: list[str],
        registered_at: str,
        activate: bool = True,
    ) -> tuple[dict[str, Any] | None, str]:
        """登记一份清单；activate=True 时将同数据流旧活动版本冻结后切换。"""
        stream = str(stream or "").strip()
        version = str(version or "").strip()
        if not stream or not version:
            return None, "数据流与清单版本必填"
        files = [str(f).strip() for f in files if str(f).strip()]
        if not files:
            return None, "清单必须包含至少一份落盘文件明细"
        if self.get_manifest_version(stream, version) is not None:
            return None, f"清单版本 {version} 在数据流 {stream} 已存在，不能重复登记"

        if activate:
            for row in store.rows(self.MODULE):
                if row.get("数据流") == stream and row.get("版本状态") == "活动":
                    row["版本状态"] = "冻结"
                    row["status"] = "已冻结"
                    row["pending"] = False

        rows = store.rows(self.MODULE)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "清单编号": f"MAN-{max((int(r.get('id', 0)) for r in rows), default=0) + 1:04d}",
            "数据流": stream,
            "清单版本": version,
            "版本状态": "活动" if activate else "冻结",
            "目标载体": str(target or ""),
            "文件总数": len(files),
            "文件明细": ";".join(files),
            "登记时间": registered_at,
            "status": "生效中" if activate else "已冻结",
            "pending": activate,
            "abnormal": False,
        }
        rows.append(entry)
        return entry, ""

    def switch_active_manifest(self, stream: str, version: str) -> tuple[dict[str, Any] | None, str]:
        """把写入端活动清单切换到另一版本；原活动版本冻结，目标版本解冻为活动。

        用于失败重试、相邻批次文件误入等场景。已被落盘检查冻结引用的版本
        只是不再活动，记录本身保留，仍可按文件身份验证。
        """
        target_row = self.get_manifest_version(stream, version)
        if target_row is None:
            return None, f"数据流 {stream} 不存在清单版本 {version}，无法切换活动清单"
        for row in store.rows(self.MODULE):
            if row.get("数据流") != stream:
                continue
            if row is target_row:
                row["版本状态"] = "活动"
                row["status"] = "生效中"
                row["pending"] = True
            elif row.get("版本状态") == "活动":
                row["版本状态"] = "冻结"
                row["status"] = "已冻结"
                row["pending"] = False
        return target_row, ""
