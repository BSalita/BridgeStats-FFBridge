from __future__ import annotations

import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

import bridgestatslib as lib


def _selected_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Date": [datetime.date(2020, 1, 15), datetime.date(2020, 1, 15)],
            "session_id": ["s1", "s1"],
            "HandRecordBoard": ["b1", "b1"],
            "Declarer": ["2663279", "9524304"],
            "Declarer_Name": ["Robert", "Kerry"],
            "Dummy": ["9524304", "2663279"],
            "Declarer_Pair": ["2663279_9524304", "9524304_2663279"],
            "Declarer_Pct": [0.6, 0.4],
            "Score_Declarer": [420, -50],
            "Tricks": [10, 8],
            "DD_Tricks": [9, 9],
            "ParScore": [400, 400],
            "Result": [1, -1],
            "Count": [0, 0],
            "PBN": ["N:AK...", "N:AK..."],
        }
    )


class ReportLibTests(unittest.TestCase):
    def test_add_board_scoring_columns(self) -> None:
        out = lib.add_board_scoring_columns(_selected_df())
        self.assertEqual(out["DD_GE"][0], 1)
        self.assertEqual(out["OverTricks"][0], 1)
        self.assertEqual(out["UnderTricks"][1], 1)

    def test_dedupe_keeps_last_pbn_declarer(self) -> None:
        df = pl.concat([_selected_df(), _selected_df()])
        out = lib.dedupe_boards_by_pbn_declarer(df)
        self.assertEqual(out.height, 2)

    def test_aggregate_by_declarer_and_session(self) -> None:
        df = lib.add_board_scoring_columns(_selected_df())
        df = lib.attach_pair_player_names(df, {"2663279": "Robert", "9524304": "Kerry"})
        by_decl = lib.aggregate_by_declarer(df, "Declarer_Pct", min_declares=1, top_n=10)
        self.assertEqual(by_decl.height, 2)
        by_sess = lib.aggregate_by_session(df, "Declarer_Pct")
        self.assertEqual(by_sess.height, 1)

    def test_identical_boards_and_pair_h2h(self) -> None:
        df = lib.add_board_scoring_columns(_selected_df())
        df = lib.attach_pair_player_names(df)
        comparison = lib.identical_boards_comparison(df, "Declarer_Pct")
        self.assertEqual(comparison["n_boards"], 1)
        self.assertGreater(comparison["identical_boards"]["row_count"], 0)
        h2h = lib.pair_head_to_head(df, "Declarer_Pct")
        self.assertGreater(h2h.height, 0)

    def test_chart_series_bar_payload(self) -> None:
        df = lib.add_board_scoring_columns(_selected_df())
        charts = lib.chart_series(df, ["Declarer_Pct"])
        kinds = {chart["kind"] for chart in charts}
        self.assertTrue({"info", "bar"} & kinds)

    def test_run_sql_rejects_unknown_source_and_forbidden(self) -> None:
        with self.assertRaises(ValueError):
            lib.run_sql("SELECT 1", "not_a_source")
        with self.assertRaises(ValueError):
            lib.run_sql("COPY self TO 'x.csv'", "club_board_results")

    def test_run_sql_joins_extra_self_table(self) -> None:
        boards = pl.DataFrame(
            {
                "session_id": ["s1", "s1"],
                "Declarer_Pct": [0.6, 0.4],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "boards.parquet"
            boards.write_parquet(path)
            with patch.object(lib, "resolve_source_path", return_value=path):
                payload = lib.run_sql(
                    "SELECT h.tournament_id, AVG(s.Declarer_Pct) AS mean_pct "
                    "FROM self h LEFT JOIN club_board_results s "
                    "ON s.session_id = h.tournament_id GROUP BY h.tournament_id",
                    "club_board_results",
                    extra_tables={"self": [{"tournament_id": "s1"}]},
                )
        self.assertEqual(payload["row_count"], 1)
        self.assertAlmostEqual(payload["rows"][0]["mean_pct"], 0.5)

    def test_frame_to_table_round_trip(self) -> None:
        table = lib.frame_to_table(_selected_df(), limit=1)
        self.assertEqual(table["row_count"], 1)
        self.assertTrue(table["truncated"])
        back = lib.table_to_frame(table)
        self.assertEqual(back.height, 1)

    def test_table_to_frame_accepts_late_date_strings(self) -> None:
        rows = [{"club_enrolled": None} for _ in range(120)]
        rows.append({"club_enrolled": "2018-12-29"})
        table = {"columns": ["club_enrolled"], "rows": rows, "row_count": len(rows)}
        back = lib.table_to_frame(table)
        self.assertEqual(back.height, 121)
        self.assertEqual(back["club_enrolled"][-1], "2018-12-29")

    def test_fuzzy_player_name_and_exact_number_filters(self) -> None:
        df = pl.DataFrame(
            {
                "player_id": ["246273", "282839", "111111"],
                "first_name": ["Robert", "Kerry", "Jean"],
                "last_name": ["Salita", "Flom", "Balleroy"],
            }
        )
        by_last = lib.filter_players_by_name(df, "salita")
        self.assertEqual(by_last["player_id"].to_list(), ["246273"])
        by_first = lib.filter_players_by_name(df, "robert")
        self.assertEqual(by_first["player_id"].to_list(), ["246273"])
        by_typo = lib.filter_players_by_name(df, "salitta")
        self.assertEqual(by_typo["player_id"].to_list(), ["246273"])
        by_or = lib.filter_players_by_name(df, "salita, flom")
        self.assertEqual(sorted(by_or["player_id"].to_list()), ["246273", "282839"])
        short = lib.filter_players_by_name(df, "al")
        self.assertEqual(short.height, 0)
        exact = lib.filter_players_by_number(df, "246273", "player_id")
        self.assertEqual(exact["player_id"].to_list(), ["246273"])
        partial = lib.filter_players_by_number(df, "246", "player_id")
        self.assertEqual(partial.height, 0)

    def test_player_number_accepts_license_and_lancelot_aliases(self) -> None:
        import os
        import tempfile
        from pathlib import Path

        catalog = pl.DataFrame(
            {
                "player_id": ["597539", "111111"],
                "first_name": ["Robert", "Jean"],
                "last_name": ["Salita", "Balleroy"],
            }
        )
        persons = pl.DataFrame(
            {
                "lancelot_person_id": ["246273"],
                "classic_person_id": ["597539"],
                "license_number": ["9500754"],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            persons.write_parquet(Path(tmp) / "lancelot_persons.parquet")
            previous = os.environ.get(lib.PERSON_INDEX_DIR_ENV)
            os.environ[lib.PERSON_INDEX_DIR_ENV] = tmp
            lib.reset_person_alias_cache()
            try:
                by_license = lib.filter_players_by_number(
                    catalog, "9500754", "player_id"
                )
                by_lancelot = lib.filter_players_by_number(
                    catalog, "246273", "player_id"
                )
                by_classic = lib.filter_players_by_number(
                    catalog, "597539", "player_id"
                )
            finally:
                lib.reset_person_alias_cache()
                if previous is None:
                    os.environ.pop(lib.PERSON_INDEX_DIR_ENV, None)
                else:
                    os.environ[lib.PERSON_INDEX_DIR_ENV] = previous
        self.assertEqual(by_license["player_id"].to_list(), ["597539"])
        self.assertEqual(by_lancelot["player_id"].to_list(), ["597539"])
        self.assertEqual(by_classic["player_id"].to_list(), ["597539"])
        ranked = lib.filter_players_by_name(
            pl.DataFrame(
                {
                    "player_id": ["1", "2", "3"],
                    "first_name": ["Marie", "Robert", "Jean"],
                    "last_name": ["Salitas", "Salita", "Balleroy"],
                }
            ),
            "salita",
        )
        self.assertEqual(ranked["player_id"].to_list(), ["2", "1"])

    def test_schema_advertises_fabricated_contract(self) -> None:
        items = lib.inject_fabricated_contract_columns(
            "club_board_results",
            [
                {"name": "BidLvl", "dtype": "UInt8"},
                {"name": "BidSuit", "dtype": "String"},
                {"name": "Dbl", "dtype": "String"},
                {"name": "Declarer_Direction", "dtype": "String"},
            ],
        )
        self.assertEqual(
            [item["name"] for item in items],
            ["BidLvl", "BidSuit", "Dbl", "Contract", "Declarer_Direction"],
        )
        self.assertFalse(lib.sql_requests_contract("SELECT ContractType FROM self"))
        self.assertTrue(lib.sql_requests_contract("SELECT s.Contract FROM self"))

    def test_run_sql_fabricates_contract_on_request(self) -> None:
        boards = pl.DataFrame(
            {
                "session_id": ["s1", "s1", "s1"],
                "BidLvl": [4, None, 1],
                "BidSuit": ["H", None, "C"],
                "Dbl": ["", None, "X"],
                "Declarer_Direction": ["N", None, "W"],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "boards.parquet"
            boards.write_parquet(path)
            with patch.object(lib, "resolve_source_path", return_value=path):
                payload = lib.run_sql(
                    "SELECT Contract FROM self ORDER BY Contract",
                    "club_board_results",
                )
                untouched = lib.run_sql(
                    "SELECT BidLvl FROM self",
                    "club_board_results",
                )
        self.assertEqual(
            [row["Contract"] for row in payload["rows"]],
            ["1CXW", "4HN", "PASS"],
        )
        self.assertNotIn("Contract", untouched["columns"])

    def test_run_sql_cannot_fabricate_contract_without_pieces(self) -> None:
        boards = pl.DataFrame({"session_id": ["s1"], "Declarer_Pct": [0.5]})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "boards.parquet"
            boards.write_parquet(path)
            with patch.object(lib, "resolve_source_path", return_value=path):
                with self.assertRaises(ValueError) as exc:
                    lib.run_sql("SELECT Contract FROM self", "club_board_results")
        self.assertIn("cannot be fabricated", str(exc.exception))

    def test_club_probe_skips_optional_club_column(self) -> None:
        _filename, required, optional = lib.SOURCE_FILES["club_board_results"]
        probe = lib.source_probe_columns(required, optional)
        self.assertNotIn("Club", probe)
        self.assertIn("session_id", probe)


if __name__ == "__main__":
    unittest.main()
