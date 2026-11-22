"""落盘数据检查剧本的可执行入口：跑通 A 零行 / B 缺件待齐 / 清单切换 / 受理 / 核对全链路。

用法：cd backend && .venv/bin/python -m app.tests.scenario_sink_check
"""
from __future__ import annotations

import unittest

from app.tests.test_sink_check import SinkCheckScenarioTest


def main() -> None:
    suite = unittest.TestLoader().loadTestsFromTestCase(SinkCheckScenarioTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
