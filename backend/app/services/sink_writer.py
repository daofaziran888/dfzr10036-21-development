"""结果落盘服务：写入完成后依据落盘数据检查触发 iceberg_commit 原子提交和 data_diff 比对。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.sink_check import SinkCheckService
from app.store import store

sink_check_service = SinkCheckService()


class SinkWriterService(BaseModuleService):
    MODULE = "sink_writer"
    ENTITY = "落盘管道"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["管道编号", "数据流", "目标载体", "写入模式", "分区列", "批次大小", "压缩格式", "写入速率"]
    REQUIRED_FIELDS = ["管道编号", "数据流", "目标载体"]
    STATUS_ORDER = ["草稿", "写入中", "写入阻塞", "已停止"]
    ACTION_RULES = {"启动写入": "写入中", "解除阻塞": "写入阻塞", "停止写入": "已停止"}
    NEGATIVE_ACTIONS = []

    def trigger_iceberg_commit(self, entry_id: int) -> dict[str, Any] | None:
        """落盘管道写入中时，按落盘数据检查结果触发 iceberg_commit 原子提交快照。

        数据流链路: sink_writer -> sink_check(落盘数据检查) -> iceberg_commit
        只有「可提交」且通过受理校验的批次才会生成快照；待齐批次被闸门拦下，
        无变更批次只保留零变更确认、不产生快照。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入中":
            return None

        stream = str(entry.get("数据流", ""))
        # 该数据流只要登记过落盘检查，就必须走受理闸门：
        # 待齐批次被拦下、无变更只留确认、可提交且清单校验通过才生成快照。
        stream_checks = [row for row in store.rows("sink_check") if row.get("数据流") == stream]
        if stream_checks:
            candidate = stream_checks[-1]
            accepted, message = sink_check_service.accept_for_commit(
                stream, str(candidate.get("批次"))
            )
            if accepted is None:
                return {"闸门拦截": True, "原因": message, "批次": candidate.get("批次")}
            snapshot_id = accepted.get("快照编号")
            if not snapshot_id:
                # 零行结束确认：不生成快照，返回可查询的确认结果而非提交行。
                return {"零变更确认": True, "批次": candidate.get("批次"), "原因": message}
            return next(
                (row for row in store.rows("iceberg_commit") if row.get("快照编号") == snapshot_id),
                None,
            )

        # 兼容未登记落盘检查的历史链路，保留原有直提快照行为。
        commit_rows = store.rows("iceberg_commit")
        new_id = max((int(r.get("id", 0)) for r in commit_rows), default=0) + 1
        commit = {
            "id": new_id,
            "快照编号": f"ICEB-{new_id:04d}",
            "目标表": str(entry.get("目标载体", "unknown")),
            "数据文件": f"s3://warehouse/data/{entry.get('管道编号', 'default')}/part-00001.parquet",
            "清单文件": f"metadata/manifest-{new_id}.avro",
            "快照ID": f"{new_id * 1000 + 500}",
            "提交状态": "待提交",
            "影响行数": 128340,
            "提交时间": "2026-11-18 10:32:45",
            "status": "提交中",
            "pending": True,
            "abnormal": False,
        }
        commit_rows.append(commit)
        return commit

    def trigger_block_cleanup(self, entry_id: int) -> dict[str, Any] | None:
        """写入阻塞时触发 compaction 合并小文件。

        数据流链路: sink_writer(写入阻塞) -> compaction
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入阻塞":
            return None

        comp_rows = store.rows("compaction")
        new_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
        comp = {
            "id": new_id,
            "任务编号": f"COMP-{new_id:04d}",
            "目标表": str(entry.get("目标载体", "unknown")),
            "文件数量": 85,
            "合并策略": "BinPack-按大小合并",
            "触发阈值": "文件数 >= 50",
            "执行内存": "2GB",
            "合并耗时": "未执行",
            "释放空间": "未执行",
            "status": "合并中",
            "pending": True,
            "abnormal": False,
        }
        comp_rows.append(comp)
        return comp