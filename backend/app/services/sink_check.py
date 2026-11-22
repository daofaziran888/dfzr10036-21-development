"""落盘数据检查服务：从本批落盘文件明细签发湖仓提交依据。

三类检查结果（交接）：
- 无变更：清单为空但携带上游零行结束凭据（zero-row EOT），确认源区间内零行，
  不产生快照，但确认结果可按源日志区间查询。
- 待齐：清单为空且没有零行凭据（两段文件还缺一段），或已落文件未覆盖清单全部条目。
  缺文件的批次在写入依据收齐前不能标记为已提交。
- 可提交：清单期望的文件全部落盘且身份与冻结清单一致，记录绑定源区间、
  清单版本和每份文件身份，供湖仓受理。

重试兼容：受理/重试必须与已登记的源日志区间、冻结清单完全对得上，
相邻批次的文件即使出现在新活动清单里也不能拼入本次数据。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.sink_manifest import SinkManifestService
from app.store import store

# 三类检查结果
RESULT_NO_CHANGE = "无变更"
RESULT_PENDING = "待齐"
RESULT_COMMITTABLE = "可提交"
RESULT_COMMITTED = "已提交"

manifest_service = SinkManifestService()


class SinkCheckService(BaseModuleService):
    MODULE = "sink_check"
    ENTITY = "落盘数据检查"
    OPERATION_TERM = "校验"
    LIST_FIELDS = [
        "检查编号", "数据流", "批次", "源端起始位点", "源端结束位点",
        "清单版本", "期望文件数", "已到文件数", "零行凭据", "落盘文件身份",
        "检查结论", "检查时间", "快照编号",
    ]
    REQUIRED_FIELDS = ["检查编号", "数据流", "批次", "源端起始位点", "源端结束位点"]
    # 无变更/待齐为中间或确认态，可提交后由湖仓受理流转到已提交。
    STATUS_ORDER = [RESULT_PENDING, RESULT_COMMITTABLE, RESULT_COMMITTED, RESULT_NO_CHANGE]
    ACTION_RULES = {
        "补齐复核": RESULT_COMMITTABLE,
        "受理提交": RESULT_COMMITTED,
    }
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def find_by_batch(self, stream: str, batch: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("数据流") == stream and row.get("批次") == batch:
                return row
        return None

    def find_by_source_range(
        self, start_position: str, end_position: str, stream: str | None = None
    ) -> list[dict[str, Any]]:
        """按源日志区间查检查确认结果（含零行确认），供数据比对追溯。"""
        rows = store.rows(self.MODULE)
        return [
            row for row in rows
            if row.get("源端起始位点") == start_position
            and row.get("源端结束位点") == end_position
            and (stream is None or row.get("数据流") == stream)
        ]

    @staticmethod
    def _files_of(manifest_row: dict[str, Any] | None) -> list[str]:
        if not manifest_row:
            return []
        detail = str(manifest_row.get("文件明细", "") or "")
        return [item.strip() for item in detail.split(";") if item.strip()]

    def _re_evaluate(self, entry: dict[str, Any], landed: list[str]) -> None:
        """根据当前已落文件与冻结清单重算检查结论。"""
        expected = int(entry.get("期望文件数", 0))
        landed_unique: list[str] = []
        for item in landed:
            item = str(item).strip()
            if item and item not in landed_unique:
                landed_unique.append(item)

        if expected == 0:
            # 空清单：有零行结束凭据才是确认过的无变更，否则一直待齐。
            if entry.get("零行凭据"):
                entry["status"] = RESULT_NO_CHANGE
                entry["pending"] = False
                entry["检查结论"] = "上游零行结束-无变更已确认"
            else:
                entry["status"] = RESULT_PENDING
                entry["pending"] = True
                entry["检查结论"] = "清单为空且缺少零行结束凭据-等待同批剩余文件或零行确认"
            entry["已到文件数"] = 0
            entry["落盘文件身份"] = ""
            return

        frozen = manifest_service.get_manifest_version(
            str(entry.get("数据流", "")), str(entry.get("清单版本", ""))
        )
        expected_files = self._files_of(frozen) or list(entry.get("_期望文件", []))
        entry["_期望文件"] = expected_files

        # 只接受属于本批冻结清单的文件；相邻批次文件一律不并入本次检查。
        accepted = [f for f in landed_unique if f in expected_files]
        entry["已到文件数"] = len(accepted)
        entry["落盘文件身份"] = ";".join(accepted)

        if len(accepted) >= expected and all(f in accepted for f in expected_files):
            entry["status"] = RESULT_COMMITTABLE
            entry["pending"] = True
            entry["检查结论"] = "清单齐备-可提交湖仓"
        else:
            entry["status"] = RESULT_PENDING
            entry["pending"] = True
            missing = [f for f in expected_files if f not in accepted]
            entry["检查结论"] = f"清单未收齐-缺{len(missing)}份文件:{'、'.join(missing)}"

    # ------------------------------------------------------------------
    # 登记与补文件
    # ------------------------------------------------------------------
    def register_check(
        self,
        *,
        stream: str,
        batch: str,
        start_position: str,
        end_position: str,
        manifest_version: str = "",
        zero_row_credential: str = "",
        landed_files: list[str] | None = None,
        checked_at: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """从本批落盘文件明细签发检查结果；空清单靠零行凭据区分无变更与待齐。"""
        stream = str(stream or "").strip()
        batch = str(batch or "").strip()
        if not stream or not batch or not start_position or not end_position:
            return None, "数据流、批次、源端起止位点必填"
        if self.find_by_batch(stream, batch) is not None:
            return None, f"批次 {batch} 的落盘检查已登记，请走补文件/重试流程"

        landed = [str(f).strip() for f in (landed_files or []) if str(f).strip()]
        credential = str(zero_row_credential or "").strip()
        rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1

        entry: dict[str, Any] = {
            "id": new_id,
            "检查编号": f"SCK-{new_id:04d}",
            "数据流": stream,
            "批次": batch,
            "源端起始位点": str(start_position).strip(),
            "源端结束位点": str(end_position).strip(),
            "清单版本": str(manifest_version or "").strip(),
            "期望文件数": 0,
            "已到文件数": 0,
            "零行凭据": credential,
            "落盘文件身份": "",
            "检查结论": "",
            "检查时间": checked_at or "2026-11-22 00:00:00",
            "快照编号": "",
            "abnormal": False,
            "_期望文件": [],
        }

        if not manifest_version:
            # 空清单：零行结束凭据 => 无变更（兼容历史零行结束数据包）；否则待齐。
            entry["status"] = RESULT_NO_CHANGE if credential else RESULT_PENDING
            entry["pending"] = not bool(credential)
            entry["检查结论"] = (
                "上游零行结束-无变更已确认"
                if credential
                else "清单为空且缺少零行结束凭据-等待同批剩余文件或零行确认"
            )
            rows.append(entry)
            return entry, ""

        frozen = manifest_service.get_manifest_version(stream, manifest_version)
        if frozen is None:
            return None, f"清单版本 {manifest_version} 未登记，不能作为落盘检查依据"
        expected_files = self._files_of(frozen)
        entry["期望文件数"] = len(expected_files)
        entry["_期望文件"] = expected_files
        rows.append(entry)
        self._re_evaluate(entry, landed)
        return entry, ""

    def land_file(
        self,
        stream: str,
        batch: str,
        files: list[str],
    ) -> tuple[dict[str, Any] | None, str]:
        """同批后续落盘文件到达后重新校验；只接受冻结清单内的本批文件。"""
        entry = self.find_by_batch(stream, batch)
        if entry is None:
            return None, f"批次 {batch} 的落盘检查不存在"
        if entry.get("status") == RESULT_COMMITTED:
            return None, f"批次 {batch} 已提交，不能再补文件"
        current = [f for f in str(entry.get("落盘文件身份", "")).split(";") if f]
        self._re_evaluate(entry, current + list(files or []))
        return entry, ""

    # ------------------------------------------------------------------
    # 湖仓受理
    # ------------------------------------------------------------------
    def accept_for_commit(
        self,
        stream: str,
        batch: str,
        *,
        active_manifest_version: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """湖仓按落盘数据检查结果受理：只接受清单齐备的可提交批次。

        - 无变更：登记可查询的零变更确认，不生成快照。
        - 待齐：拒绝受理，回执说明缺文件原因，值班员据此继续处理。
        - 可提交：校验活动清单/文件身份与冻结清单一致后创建 Iceberg 快照。
          活动清单被切换且混入相邻批文件时拒绝，防止相邻批文件拼入本次数据。
        - 已提交：幂等返回原快照，保证 B 最多生成一份已确认快照。
        """
        entry = self.find_by_batch(stream, batch)
        if entry is None:
            return None, f"批次 {batch} 的落盘检查不存在，湖仓无受理依据"

        status = entry.get("status")
        if status == RESULT_NO_CHANGE:
            return entry, f"批次 {batch} 为零行结束确认（{entry.get('零行凭据')}），零变更不生成快照"
        if status == RESULT_PENDING:
            return None, f"批次 {batch} 仍为待齐：{entry.get('检查结论')}，写入依据不足，不予受理"
        if status == RESULT_COMMITTED:
            return entry, f"批次 {batch} 已受理提交，快照 {entry.get('快照编号')} 已存在（幂等不重复生成）"

        # 可提交：冻结清单必须仍可验证，且活动清单若已切换不得污染本批文件集。
        frozen = manifest_service.get_manifest_version(
            stream, str(entry.get("清单版本", ""))
        )
        if frozen is None:
            return None, f"批次 {batch} 绑定的清单版本已丢失，无法验证文件身份"
        frozen_files = self._files_of(frozen)
        accepted = [f for f in str(entry.get("落盘文件身份", "")).split(";") if f]
        if sorted(accepted) != sorted(frozen_files):
            return None, f"批次 {batch} 已落文件与冻结清单不一致，拒绝受理"

        if active_manifest_version is not None:
            active = manifest_service.get_manifest_version(stream, active_manifest_version)
            if active is not None:
                active_files = set(self._files_of(active))
                foreign = active_files - set(frozen_files)
                if foreign:
                    return None, (
                        f"活动清单 {active_manifest_version} 含相邻批次文件 "
                        f"{'、'.join(sorted(foreign))}，与批次 {batch} 冻结清单不符，拒绝受理"
                    )

        snapshot = self._create_iceberg_snapshot(entry, frozen)
        entry["status"] = RESULT_COMMITTED
        entry["pending"] = False
        entry["快照编号"] = snapshot["快照编号"]
        entry["检查结论"] = f"湖仓已受理-快照{snapshot['快照编号']}"
        return entry, f"批次 {batch} 已提交，采用清单版本 {entry.get('清单版本')}"

    @staticmethod
    def _create_iceberg_snapshot(
        entry: dict[str, Any], frozen_manifest: dict[str, Any]
    ) -> dict[str, Any]:
        commit_rows = store.rows("iceberg_commit")
        new_id = max((int(r.get("id", 0)) for r in commit_rows), default=0) + 1
        files = [f for f in str(entry.get("落盘文件身份", "")).split(";") if f]
        snapshot = {
            "id": new_id,
            "快照编号": f"IC-B{entry.get('批次', 'X')}-{new_id:04d}",
            "目标表": str(frozen_manifest.get("目标载体", "unknown")),
            "数据文件": ";".join(files),
            "清单文件": f"metadata/{entry.get('清单版本', 'manifest')}.avro",
            "快照ID": f"snap-{entry.get('源端起始位点', '')}-{entry.get('源端结束位点', '')}",
            "提交状态": "COMMITTED",
            "影响行数": len(files),
            "提交时间": str(entry.get("检查时间", "")),
            "status": "已提交",
            "pending": False,
            "abnormal": False,
            # 绑定检查交接，保证快照可回溯到源区间、清单版本与文件身份。
            "来源批次": entry.get("批次"),
            "源端起始位点": entry.get("源端起始位点"),
            "源端结束位点": entry.get("源端结束位点"),
            "清单版本": entry.get("清单版本"),
            "落盘检查编号": entry.get("检查编号"),
        }
        commit_rows.append(snapshot)
        return snapshot
