"""落盘数据检查端到端剧本：空批零行确认、缺文件待齐、清单切换防污染与源区间核对。

可直接运行：
    cd backend && .venv/bin/python -m unittest app.tests.test_sink_check -v
或：
    cd backend && .venv/bin/python -m app.tests.scenario_sink_check
"""
from __future__ import annotations

import unittest

from app.services.data_diff import DataDiffService
from app.services.sink_check import (
    RESULT_COMMITTED,
    RESULT_COMMITTABLE,
    RESULT_NO_CHANGE,
    RESULT_PENDING,
    SinkCheckService,
)
from app.services.sink_manifest import SinkManifestService
from app.store import store

STREAM = "order_events_cdc"
# 区间 A：历史数据包以零行结束；区间 B：两段文件的正常批次。
RANGE_A = ("binlog.001234:1000", "binlog.001234:2000")
RANGE_B = ("binlog.001234:2000", "binlog.001234:3000")
B_FILE_1 = "s3://warehouse/order_events/batch-b/part-00001.parquet"
B_FILE_2 = "s3://warehouse/order_events/batch-b/part-00002.parquet"
# 相邻批次 C 的文件，活动清单被恶意/误切换后混入，绝不能提交给 B。
C_FILE = "s3://warehouse/order_events/batch-c/part-00009.parquet"


class SinkCheckScenarioTest(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        # 清掉种子示例，保证剧本里的批次/清单编号不与演示数据冲突。
        store.rows("sink_check").clear()
        store.rows("sink_manifest").clear()
        self.manifests = SinkManifestService()
        self.checks = SinkCheckService()
        self.diffs = DataDiffService()

    # ------------------------------------------------------------------
    def test_empty_batch_without_credential_stays_pending(self) -> None:
        """空清单无零行凭据 => 待齐，且受理被拒；补零行凭据前不能标记已提交。"""
        entry, error = self.checks.register_check(
            stream=STREAM, batch="batch-empty",
            start_position="binlog:0", end_position="binlog:100",
        )
        self.assertEqual(error, "")
        self.assertEqual(entry["status"], RESULT_PENDING)
        accepted, message = self.checks.accept_for_commit(STREAM, "batch-empty")
        self.assertIsNone(accepted)
        self.assertIn("待齐", message)

    def test_full_scenario_a_zero_row_b_manifest_switch_and_verify(self) -> None:
        # 0. 登记 B 的冻结清单 v1（两段文件）。
        manifest_v1, error = self.manifests.register_manifest(
            stream=STREAM, version="B-manifest-v1", target="ads.order_events_iceberg",
            files=[B_FILE_1, B_FILE_2], registered_at="2026-11-22 09:00:00",
        )
        self.assertEqual(error, "")
        self.assertEqual(manifest_v1["版本状态"], "活动")

        # 1. 区间 A：以历史零行结束凭据确认无变更。
        check_a, error = self.checks.register_check(
            stream=STREAM, batch="batch-A",
            start_position=RANGE_A[0], end_position=RANGE_A[1],
            zero_row_credential="zero-row-eot:binlog.001234:1000-2000",
            checked_at="2026-11-22 09:05:00",
        )
        self.assertEqual(error, "")
        self.assertEqual(check_a["status"], RESULT_NO_CHANGE)
        # 无变更受理只返回确认，不生成快照。
        accepted_a, msg_a = self.checks.accept_for_commit(STREAM, "batch-A")
        self.assertIsNotNone(accepted_a)
        self.assertEqual(accepted_a["快照编号"], "")
        self.assertIn("零变更", msg_a)
        a_snapshots = [r for r in store.rows("iceberg_commit") if r.get("来源批次") == "batch-A"]
        self.assertEqual(a_snapshots, [])
        # 即使已证实零行，后续比对也要查得到这段确认日志。
        verify_a = self.diffs.verify_by_source_range(RANGE_A[0], RANGE_A[1], stream=STREAM)
        self.assertTrue(verify_a["ok"], verify_a)
        self.assertEqual(verify_a["明细"][0]["检查状态"], RESULT_NO_CHANGE)
        self.assertIn("无快照", verify_a["明细"][0]["核对结果"])

        # 2. 区间 B：两段文件暂缺一段 => 待齐，快照历史里没有 B。
        check_b, error = self.checks.register_check(
            stream=STREAM, batch="batch-B",
            start_position=RANGE_B[0], end_position=RANGE_B[1],
            manifest_version="B-manifest-v1",
            landed_files=[B_FILE_1],
            checked_at="2026-11-22 09:10:00",
        )
        self.assertEqual(error, "")
        self.assertEqual(check_b["status"], RESULT_PENDING)
        self.assertEqual(check_b["已到文件数"], 1)
        self.assertIn("part-00002", check_b["检查结论"])
        accepted_b_pending, msg_pending = self.checks.accept_for_commit(STREAM, "batch-B")
        self.assertIsNone(accepted_b_pending)
        self.assertIn("待齐", msg_pending)
        verify_b_pending = self.diffs.verify_by_source_range(RANGE_B[0], RANGE_B[1], stream=STREAM)
        self.assertFalse(verify_b_pending["ok"])
        self.assertIn("缺失输入", verify_b_pending["明细"][0]["核对结果"])

        # 3. 补到最后一份 B 文件 => 签发可提交，绑定源区间/清单版本/两份文件身份。
        check_b, error = self.checks.land_file(STREAM, "batch-B", [B_FILE_2])
        self.assertEqual(error, "")
        self.assertEqual(check_b["status"], RESULT_COMMITTABLE)
        self.assertEqual(check_b["清单版本"], "B-manifest-v1")
        self.assertEqual(
            set(check_b["落盘文件身份"].split(";")), {B_FILE_1, B_FILE_2}
        )

        # 4. 湖仓实际受理前暂停，写入端把活动清单切到 v2，其中第二份属于相邻批 C。
        manifest_v2, error = self.manifests.register_manifest(
            stream=STREAM, version="B-manifest-v2", target="ads.order_events_iceberg",
            files=[B_FILE_1, C_FILE], registered_at="2026-11-22 09:20:00",
        )
        self.assertEqual(error, "")
        # 登记即切换：v1 冻结保留，B 的两份文件与冻结清单仍可验证。
        self.assertEqual(manifest_v2["版本状态"], "活动")
        self.assertEqual(manifest_v1["版本状态"], "冻结")
        self.assertEqual(self.manifests.active_manifest(STREAM)["清单版本"], "B-manifest-v2")
        still_ok, _ = self.checks.land_file(STREAM, "batch-B", [])
        self.assertEqual(still_ok["status"], RESULT_COMMITTABLE)

        # 5. 用原签发结果恢复受理：当场核对状态与实际采用的清单版本。
        #    活动清单含相邻批 C 文件 => 拒绝，回执说明原因，B 未被提交。
        rejected, reject_msg = self.checks.accept_for_commit(
            STREAM, "batch-B", active_manifest_version="B-manifest-v2"
        )
        self.assertIsNone(rejected)
        self.assertIn(C_FILE, reject_msg)
        self.assertIn("拒绝受理", reject_msg)
        b_snapshots_before = [r for r in store.rows("iceberg_commit") if r.get("来源批次") == "batch-B"]
        self.assertEqual(b_snapshots_before, [])
        self.assertEqual(self.checks.find_by_batch(STREAM, "batch-B")["status"], RESULT_COMMITTABLE)

        # 6. 值班员修正活动清单回冻结版本 v1，再重试 => B 提交成功。
        switched, error = self.manifests.switch_active_manifest(STREAM, "B-manifest-v1")
        self.assertEqual(error, "")
        self.assertEqual(switched["版本状态"], "活动")
        accepted_b, msg_b = self.checks.accept_for_commit(
            STREAM, "batch-B", active_manifest_version="B-manifest-v1"
        )
        self.assertIsNotNone(accepted_b, msg_b)
        self.assertEqual(accepted_b["status"], RESULT_COMMITTED)
        self.assertIn("B-manifest-v1", msg_b)

        # 7. B 最多一份已确认快照，快照绑定源区间、清单版本与两份 B 文件身份。
        b_snapshots = [r for r in store.rows("iceberg_commit") if r.get("来源批次") == "batch-B"]
        self.assertEqual(len(b_snapshots), 1)
        snapshot = b_snapshots[0]
        self.assertEqual(snapshot["清单版本"], "B-manifest-v1")
        self.assertEqual(snapshot["源端起始位点"], RANGE_B[0])
        self.assertEqual(snapshot["源端结束位点"], RANGE_B[1])
        self.assertEqual(set(snapshot["数据文件"].split(";")), {B_FILE_1, B_FILE_2})
        self.assertNotIn(C_FILE, snapshot["数据文件"])

        # 8. 受理幂等：重复受理不产生第二份快照。
        again, msg_again = self.checks.accept_for_commit(STREAM, "batch-B")
        self.assertIsNotNone(again)
        self.assertIn("幂等", msg_again)
        self.assertEqual(
            len([r for r in store.rows("iceberg_commit") if r.get("来源批次") == "batch-B"]),
            1,
        )

        # 9. 数据比对按源区间同时查到 A 的零变更与 B 的实际提交结果。
        verify_a2 = self.diffs.verify_by_source_range(RANGE_A[0], RANGE_A[1], stream=STREAM)
        verify_b2 = self.diffs.verify_by_source_range(RANGE_B[0], RANGE_B[1], stream=STREAM)
        self.assertTrue(verify_a2["ok"])
        self.assertEqual(verify_a2["明细"][0]["检查状态"], RESULT_NO_CHANGE)
        self.assertTrue(verify_b2["ok"])
        self.assertEqual(verify_b2["明细"][0]["检查状态"], RESULT_COMMITTED)
        self.assertEqual(verify_b2["明细"][0]["快照"], [snapshot["快照编号"]])
        self.assertIn("绑定一致", verify_b2["明细"][0]["核对结果"])

        # 10. 相邻批 C 文件从未提交给 B。
        all_committed_files = ";".join(
            str(r.get("数据文件", "")) for r in store.rows("iceberg_commit")
        )
        self.assertNotIn(C_FILE, all_committed_files)

    def test_retry_must_match_registered_source_range(self) -> None:
        """失败重试与已登记源日志区间对不上时，不能借相邻批次文件拼入本次数据。"""
        self.manifests.register_manifest(
            stream=STREAM, version="R-v1", target="ads.order_events_iceberg",
            files=[B_FILE_1, B_FILE_2], registered_at="2026-11-22 10:00:00",
        )
        check, error = self.checks.register_check(
            stream=STREAM, batch="batch-R",
            start_position="binlog:100", end_position="binlog:200",
            manifest_version="R-v1", landed_files=[B_FILE_1, B_FILE_2],
        )
        self.assertEqual(error, "")
        self.assertEqual(check["status"], RESULT_COMMITTABLE)
        # 相邻批文件落盘到同一管道：re-evaluate 时被冻结清单过滤，不影响可提交集合。
        rechecked, error = self.checks.land_file(STREAM, "batch-R", [C_FILE])
        self.assertEqual(error, "")
        self.assertEqual(rechecked["status"], RESULT_COMMITTABLE)
        self.assertNotIn(C_FILE, rechecked["落盘文件身份"])
        accepted, _ = self.checks.accept_for_commit(STREAM, "batch-R")
        self.assertIsNotNone(accepted)
        snapshot = next(r for r in store.rows("iceberg_commit") if r.get("来源批次") == "batch-R")
        self.assertEqual(snapshot["源端起始位点"], "binlog:100")
        self.assertEqual(snapshot["源端结束位点"], "binlog:200")
        self.assertNotIn(C_FILE, snapshot["数据文件"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
