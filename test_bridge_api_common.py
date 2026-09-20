from __future__ import annotations

import os
import unittest
from pathlib import Path

import bridge_api_common as api_common


class BridgeApiCommonTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous = os.environ.pop(api_common.DATA_ROOT_ENV, None)

    def tearDown(self) -> None:
        if self._previous is None:
            os.environ.pop(api_common.DATA_ROOT_ENV, None)
        else:
            os.environ[api_common.DATA_ROOT_ENV] = self._previous

    def test_prepare_sql_rejects_forbidden_and_qualifies_from(self) -> None:
        sql, limit = api_common.prepare_sql("SELECT 1", "club_board_results", 12)
        self.assertEqual(sql, "FROM self SELECT 1")
        self.assertEqual(limit, 12)
        with self.assertRaises(ValueError):
            api_common.prepare_sql("PRAGMA show_tables", "club_board_results")

    def test_data_root_search_paths(self) -> None:
        os.environ[api_common.DATA_ROOT_ENV] = "/data"
        self.assertEqual(
            api_common.data_root_search_paths("ffbridge")[0],
            Path("/data/stats/ffbridge"),
        )


if __name__ == "__main__":
    unittest.main()
