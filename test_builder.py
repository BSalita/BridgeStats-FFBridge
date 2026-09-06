from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import polars as pl

import build_ffbridge_club_parquets as builder


class BuilderTests(unittest.TestCase):
    def test_demo_writes_required_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            written = builder.build(output_dir=Path(tmp), demo=True)
            self.assertEqual(len(written), 4)
            boards = pl.read_parquet(written[builder.BOARD_FILENAME])
            self.assertIn("Club", boards.columns)
            self.assertIn("Declarer", boards.columns)
            self.assertGreater(boards.height, 0)
            players = pl.read_parquet(written[builder.PLAYER_FILENAME])
            self.assertIn("player_id", players.columns)
            clubs = pl.read_parquet(written[builder.CLUB_FILENAME])
            self.assertEqual(clubs["id"][0], "750001")

    def test_map_board_results_derives_dummy_from_seats(self) -> None:
        raw = pl.DataFrame(
            {
                "club_code": ["750001"],
                "session_id": ["99"],
                "Date": ["2024-03-01"],
                "Declarer_Direction": ["N"],
                "Player_ID_N": ["1"],
                "Player_ID_E": ["2"],
                "Player_ID_S": ["3"],
                "Player_ID_W": ["4"],
                "PBN": ["N:AK..."],
                "Board": [7],
            }
        )
        out = builder.map_board_results(raw)
        self.assertEqual(out["Club"][0], "750001")
        self.assertEqual(out["Declarer"][0], "1")
        self.assertEqual(out["Dummy"][0], "3")
        self.assertEqual(out["OnLead"][0], "2")
        self.assertEqual(out["NotOnLead"][0], "4")


if __name__ == "__main__":
    unittest.main()
