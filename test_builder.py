from __future__ import annotations

import datetime
import json
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

    def test_from_quality_cache_skips_training_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            source.mkdir()
            (source / "ffbridge_training_data_df.parquet").write_bytes(b"not-a-parquet")
            with self.assertRaises(FileNotFoundError):
                builder.build(
                    output_dir=root / "out",
                    source_dir=source,
                    from_quality_cache=True,
                )

    def test_map_board_results_parses_datetime_dates(self) -> None:
        raw = pl.DataFrame(
            {
                "session_id": ["99"],
                "Date": [datetime.datetime(2024, 3, 1, 0, 0)],
                "Declarer_Direction": ["N"],
                "Player_ID_N": ["1"],
                "Player_ID_E": ["2"],
                "Player_ID_S": ["3"],
                "Player_ID_W": ["4"],
                "PBN": ["N:AK..."],
                "Board": [7],
                "Vul_Declarer": [True],
            }
        )
        out = builder.map_board_results(raw)
        self.assertEqual(out["Date"][0], datetime.date(2024, 3, 1))
        self.assertEqual(out["Vul_Declarer"][0], "Y")

    def test_apply_session_lookup_fills_date_and_club(self) -> None:
        boards, hands, players, _clubs = builder.demo_frames()
        boards = boards.with_columns(
            pl.lit(None).cast(pl.Date).alias("Date"),
            pl.lit(None).cast(pl.Utf8).alias("Club"),
        )
        hands = hands.with_columns(pl.lit(None).cast(pl.Date).alias("game_date"))
        lookup = pl.DataFrame(
            {
                "session_id": ["s1"],
                "Date": [datetime.date(2024, 1, 15)],
                "Club": ["2300024"],
                "club_name": ["Sable Bridge Club"],
            }
        )
        boards, hands, players, clubs = builder.apply_session_lookup(
            boards, hands, players, lookup
        )
        self.assertTrue((boards["Date"] == datetime.date(2024, 1, 15)).all())
        self.assertTrue((boards["Club"] == "2300024").all())
        self.assertTrue((hands["game_date"] == datetime.date(2024, 1, 15)).all())
        self.assertEqual(clubs["id"][0], "2300024")
        self.assertTrue((players["club"] == "2300024").all())

    def test_load_session_lookup_reads_ffb_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "data"
            sessions = source / "competitions" / "sessions"
            sessions.mkdir(parents=True)
            (sessions / "145560.json").write_text(
                json.dumps(
                    {
                        "id": 145560,
                        "groupSessions": [
                            {
                                "date": "2024-12-11T00:00:00+01:00",
                                "group": {
                                    "phase": {
                                        "stade": {
                                            "organization": {
                                                "ffbCode": "5803064",
                                                "label": "Bridge Club De Clichy",
                                            }
                                        }
                                    }
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            lookup = builder.load_session_lookup(source)
            self.assertEqual(lookup["session_id"][0], "145560")
            self.assertEqual(lookup["Date"][0], datetime.date(2024, 12, 11))
            self.assertEqual(lookup["Club"][0], "5803064")

    def test_session_fragments_roundtrip_without_concatenating_fat_frames(self) -> None:
        boards, hands, players, clubs = builder.demo_frames()
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            builder.write_session_fragments(out, "s1", boards, hands)
            builder.write_session_fragments(out, "s2", boards, hands)
            got_boards, got_hands = builder.read_club_fragments(out, ["s1", "s2"])
            assert got_boards is not None
            assert got_hands is not None
            self.assertEqual(got_boards.height, boards.height * 2)
            self.assertEqual(got_hands.height, hands.height)
            self.assertEqual(players.height, 6)
            self.assertEqual(clubs.height, 1)


if __name__ == "__main__":
    unittest.main()
