from __future__ import annotations

import datetime
import os
import tempfile
import unittest

import polars as pl
from fastapi.testclient import TestClient


def _write_player_and_club(root) -> None:
    pl.DataFrame(
        {
            "player_id": ["246273", "282839"],
            "first_name": ["Robert", "Kerry"],
            "last_name": ["Salita", "Flom"],
            "club": ["750001", "750001"],
        }
    ).write_parquet(root / "ffbridge_player_info.parquet")
    pl.DataFrame({"id": ["750001"], "name": ["Paris Bridge"]}).write_parquet(
        root / "ffbridge_clubs.parquet"
    )


def _write_board_results(root) -> None:
    pl.DataFrame(
        {
            "Club": ["750001", "750001"],
            "session_id": ["s1", "s1"],
            "Date": [datetime.date(2024, 1, 15), datetime.date(2024, 1, 15)],
            "Declarer_Direction": ["N", "S"],
            "Declarer": ["246273", "282839"],
            "Dummy": ["282839", "246273"],
            "OnLead": ["111111", "222222"],
            "NotOnLead": ["333333", "444444"],
            "Declarer_Name": ["Robert", "Kerry"],
            "Player_ID_N": ["246273", "282839"],
            "Player_ID_E": ["111111", "222222"],
            "Player_ID_S": ["282839", "246273"],
            "Player_ID_W": ["333333", "444444"],
            "Player_Name_N": ["Robert", "Kerry"],
            "Player_Name_E": ["A", "C"],
            "Player_Name_S": ["Kerry", "Robert"],
            "Player_Name_W": ["B", "D"],
            "Vul_Declarer": ["None", "None"],
            "ParScore": [400, 400],
            "MP_Par_Pct_Declarer": [0.5, 0.5],
            "Score_Declarer": [420, -50],
            "DD_Tricks": [9, 9],
            "Tricks": [10, 8],
            "DD_Score_Declarer": [400, 400],
            "MP_DD_Pct_Declarer": [0.6, 0.4],
            "EV_Score_Declarer": [410, 390],
            "EV_Max_Declarer": [450, 450],
            "MP_EV_Pct_Declarer": [0.55, 0.45],
            "MP_EV_Max_Pct_Declarer": [0.7, 0.7],
            "Declarer_Pct": [0.6, 0.4],
            "HandRecordBoard": ["b1", "b1"],
            "Board": [1, 1],
            "Result": [1, -1],
            "BidLvl": [4, 4],
            "BidSuit": ["H", "H"],
            "Dbl": ["", ""],
            "Vul": ["None", "None"],
            "ContractType": ["game", "game"],
            "PBN": ["N:AK...", "N:AK..."],
        }
    ).write_parquet(root / "ffbridge_club_board_results_augmented.parquet")


def _write_hand_records(root) -> None:
    pl.DataFrame(
        {
            "PBN": ["N:AK..."],
            "HandRecordBoard": ["b1"],
            "game_date": [datetime.date(2024, 1, 15)],
            "session_id": ["s1"],
            "ParScore": [400],
            "CT_N_S": [0],
            "CT_N_H": [1],
            "CT_N_D": [0],
            "CT_N_C": [0],
            "CT_N_N": [0],
            "DD_N_C": [7],
            "DD_N_D": [7],
            "DD_N_H": [9],
            "DD_N_S": [8],
            "DD_N_N": [8],
            "SL_N_C": [3],
            "SL_N_D": [3],
            "SL_N_H": [5],
            "SL_N_S": [2],
            "SL_N_ML_SJ": [0],
            "HCP_NS": [20],
            "HCP_EW": [20],
            "HCP_N": [10],
            "HCP_E": [10],
            "HCP_S": [10],
            "HCP_W": [10],
            "QT_N": [2.0],
            "QT_E": [2.0],
            "QT_S": [2.0],
            "QT_W": [2.0],
            "QT_NS": [4.0],
            "QT_EW": [4.0],
            "DP_N": [1],
            "DP_N_C": [0],
            "DP_N_D": [0],
            "DP_N_H": [1],
            "DP_N_S": [0],
            "DP_NS": [2],
            "DP_EW": [2],
        }
    ).write_parquet(root / "ffbridge_club_hand_records_augmented_narrow.parquet")


class BridgeStatsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = __import__("pathlib").Path(self._tmpdir.name)
        _write_player_and_club(self.root)
        _write_board_results(self.root)
        _write_hand_records(self.root)
        self._prev_data = os.environ.get("BRIDGESTATS_FFBRIDGE_DATA_DIR")
        os.environ["BRIDGESTATS_FFBRIDGE_DATA_DIR"] = str(self.root)
        import importlib

        import bridgestatslib
        import bridgestats_api_server

        importlib.reload(bridgestatslib)
        importlib.reload(bridgestats_api_server)
        self.client = TestClient(bridgestats_api_server.app)

    def tearDown(self) -> None:
        if self._prev_data is None:
            os.environ.pop("BRIDGESTATS_FFBRIDGE_DATA_DIR", None)
        else:
            os.environ["BRIDGESTATS_FFBRIDGE_DATA_DIR"] = self._prev_data
        self._tmpdir.cleanup()

    def test_health_and_dataset_info(self) -> None:
        health = self.client.get("/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")
        self.assertEqual(health.json()["service"], "ffbridge-stats-api")
        info = self.client.get("/ffbridge-stats/dataset-info")
        self.assertEqual(info.status_code, 200)
        self.assertEqual(info.json()["service"], "ffbridge-stats")
        self.assertIn("club_board_results", info.json()["sources"])
        self.assertNotIn("tournament_board_results", info.json()["sources"])

    def test_sql_and_board_results(self) -> None:
        sql = self.client.post(
            "/ffbridge-stats/sql",
            json={"sql": "SELECT Club, Declarer FROM self LIMIT 2", "source": "club_board_results"},
        )
        self.assertEqual(sql.status_code, 200)
        self.assertGreaterEqual(sql.json()["row_count"], 1)
        report = self.client.post(
            "/ffbridge-stats/board-results",
            json={
                "club_or_tournament": "club",
                "clubs": ["750001"],
                "players": ["246273"],
                "start_date": "2020-01-01",
                "end_date": "2025-01-01",
                "include_charts": False,
            },
        )
        self.assertEqual(report.status_code, 200, report.text)
        body = report.json()
        self.assertGreaterEqual(body["selected_count"], 1)
        self.assertTrue(body["player_boards"])

    def test_hand_records(self) -> None:
        report = self.client.post(
            "/ffbridge-stats/hand-records",
            json={
                "club_or_tournament": "club",
                "start_date": "2020-01-01",
                "end_date": "2025-12-31",
                "sample_size": 100,
                "table_limit": 10,
            },
        )
        self.assertEqual(report.status_code, 200, report.text)
        self.assertGreaterEqual(report.json()["selected_count"], 1)


if __name__ == "__main__":
    unittest.main()
