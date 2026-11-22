"""数据比对领域服务：字段、状态和动作来自业务配置，并承载源区间核对逻辑。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.sink_check import (
    RESULT_COMMITTED,
    RESULT_COMMITTABLE,
    RESULT_NO_CHANGE,
    RESULT_PENDING,
    SinkCheckService,
)
from app.store import store

sink_check_service = SinkCheckService()


class DataDiffService(BaseModuleService):
    MODULE = "data_diff"
    ENTITY = "比对任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "源端", "目标端", "比对字段", "差异类型", "差异行数", "修复建议", "比对时间"]
    REQUIRED_FIELDS = ["任务编号", "源端", "目标端"]
    STATUS_ORDER = ["待执行", "比对中", "一致", "存在差异"]
    ACTION_RULES = {"启动比对": "比对中", "确认一致": "一致", "标记差异": "存在差异"}
    NEGATIVE_ACTIONS = []

    def verify_by_source_range(
        self,
        start_position: str,
        end_position: str,
        *,
        stream: str | None = None,
    ) -> dict[str, Any]:
        """读取落盘检查确认响应，对照同一源日志区间核对三种结果。

        - 无变更：能查到零行结束确认，且没有为该区间生成数据快照。
        - 已提交：可提交检查已受理，快照绑定的源区间与文件身份可对上。
        - 缺失输入（待齐）：检查仍待齐，区间内没有受理快照，提示缺文件。
        """
        checks = sink_check_service.find_by_source_range(
            start_position, end_position, stream=stream
        )
        if not checks:
            return {
                "ok": False,
                "源端起始位点": start_position,
                "源端结束位点": end_position,
                "结论": "区间无检查记录",
                "明细": [],
            }

        details: list[dict[str, Any]] = []
        all_consistent = True
        for check in checks:
            status = check.get("status")
            snapshots = [
                row for row in store.rows("iceberg_commit")
                if row.get("来源批次") == check.get("批次")
                and row.get("源端起始位点") == start_position
                and row.get("源端结束位点") == end_position
            ]
            item: dict[str, Any] = {
                "检查编号": check.get("检查编号"),
                "批次": check.get("批次"),
                "检查状态": status,
                "清单版本": check.get("清单版本"),
                "零行凭据": check.get("零行凭据"),
                "落盘文件身份": check.get("落盘文件身份"),
                "快照": [snap["快照编号"] for snap in snapshots],
            }

            if status == RESULT_NO_CHANGE:
                # 零变更确认必须可查询，且区间内不得产生快照。
                item["核对结果"] = "零变更确认可查-区间内无快照" if not snapshots else "异常-零变更区间出现快照"
                item["一致"] = not snapshots
            elif status == RESULT_COMMITTED:
                # 已提交：最多一份快照，且源区间、清单版本、文件身份绑定一致。
                one_snapshot = len(snapshots) == 1
                bound_ok = bool(
                    snapshots
                    and snapshots[0].get("清单版本") == check.get("清单版本")
                    and snapshots[0].get("落盘检查编号") == check.get("检查编号")
                )
                item["核对结果"] = "已提交-快照与源区间绑定一致" if one_snapshot and bound_ok else "异常-快照绑定不一致或重复"
                item["一致"] = one_snapshot and bound_ok
            elif status == RESULT_PENDING:
                item["核对结果"] = f"缺失输入-{check.get('检查结论')}"
                item["一致"] = False
            elif status == RESULT_COMMITTABLE:
                item["核对结果"] = "检查已可提交但湖仓尚未受理"
                item["一致"] = False
            else:
                item["核对结果"] = f"未知检查状态 {status}"
                item["一致"] = False

            all_consistent = all_consistent and bool(item["一致"])
            details.append(item)

        return {
            "ok": all_consistent,
            "源端起始位点": start_position,
            "源端结束位点": end_position,
            "结论": "同一源区间核对通过" if all_consistent else "同一源区间核对存在异常",
            "明细": details,
        }
