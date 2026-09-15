from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

import bridgestatslib as lib
import build_ffbridge_club_parquets as builder


class DealerColumnTests(unittest.TestCase):
    def test_schema_advertises_virtual_dealer_for_old_parquet(self) -> None:
        items = lib.inject_fabricated_columns(
            "club_board_results",
            [
                {"name": "session_id", "dtype": "String"},
                {"name": "Board", "dtype": "Int64"},
            ],
        )
        self.assertEqual(
            [item["name"] for item in items],
            ["session_id", "Board", "Dealer"],
        )

    def test_run_sql_fabricates_dealer_for_old_parquet(self) -> None:
        boards = pl.DataFrame(
            {
                "session_id": ["s1"] * 5,
                "Board": [1, 2, 3, 4, 5],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "boards.parquet"
            boards.write_parquet(path)
            with patch.object(lib, "resolve_source_path", return_value=path):
                payload = lib.run_sql(
                    "SELECT Board, Dealer FROM self ORDER BY Board",
                    "club_board_results",
                )
        self.assertEqual(
            [row["Dealer"] for row in payload["rows"]],
            ["N", "E", "S", "W", "N"],
        )

    def test_builder_persists_and_normalizes_dealer(self) -> None:
        raw = pl.DataFrame(
            {
                "session_id": ["s1"] * 5,
                "Board": [1, 2, 3, 4, 0],
                "Dealer": [None, "E", "O", "invalid", None],
            }
        )
        out = builder.map_board_results(raw)
        self.assertEqual(
            out["Dealer"].to_list(),
            ["N", "E", "W", "W", None],
        )


if __name__ == "__main__":
    unittest.main()
