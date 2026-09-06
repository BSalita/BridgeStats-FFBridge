from __future__ import annotations

import unittest

import polars as pl

import bridgestatslib

REQUIRED_FILES = (
    "ffbridge_club_board_results_augmented.parquet",
    "ffbridge_club_hand_records_augmented_narrow.parquet",
    "ffbridge_clubs.parquet",
    "ffbridge_player_info.parquet",
)

BOARD_RESULT_COLUMNS = (
    "Club",
    "session_id",
    "Date",
    "Declarer_Direction",
    "Dummy",
    "OnLead",
    "NotOnLead",
    "Declarer_Name",
    "Player_ID_N",
    "Player_ID_E",
    "Player_ID_S",
    "Player_ID_W",
    "Vul_Declarer",
    "ParScore",
    "MP_Par_Pct_Declarer",
    "Score_Declarer",
    "DD_Tricks",
    "Tricks",
    "DD_Score_Declarer",
    "MP_DD_Pct_Declarer",
    "EV_Score_Declarer",
    "EV_Max_Declarer",
    "MP_EV_Pct_Declarer",
    "MP_EV_Max_Pct_Declarer",
    "Declarer_Pct",
    "HandRecordBoard",
    "Board",
    "Result",
    "BidLvl",
    "BidSuit",
    "Dbl",
    "Vul",
    "ContractType",
    "PBN",
)

REQUIRED_COLUMNS = {
    "ffbridge_club_board_results_augmented.parquet": BOARD_RESULT_COLUMNS,
    "ffbridge_club_hand_records_augmented_narrow.parquet": (
        "PBN",
        "game_date",
        "session_id",
        "ParScore",
        "CT_N_S",
        "DD_N_N",
        "HCP_NS",
    ),
    "ffbridge_clubs.parquet": ("id", "name"),
    "ffbridge_player_info.parquet": (
        "player_id",
        "last_name",
        "club",
    ),
}


class DataManifestTests(unittest.TestCase):
    def test_required_files_exist(self) -> None:
        missing = []
        for name in REQUIRED_FILES:
            try:
                bridgestatslib.resolve_data_file(name)
            except FileNotFoundError:
                missing.append(name)
        self.assertEqual(missing, [], f"Missing required data files: {missing}")

    def test_parquet_schemas_have_required_columns(self) -> None:
        for name, columns in REQUIRED_COLUMNS.items():
            with self.subTest(name=name):
                path = bridgestatslib.resolve_data_file(name, required_columns=columns)
                schema = pl.read_parquet_schema(str(path))
                missing = [col for col in columns if col not in schema]
                self.assertEqual(missing, [], f"{name} missing columns: {missing}")

    def test_loaders_use_current_column_names(self) -> None:
        forbidden = {
            "board_record_string",
            "Player_Number_N",
            "Player_Number_E",
            "Player_Number_S",
            "Player_Number_W",
            "Declarer_Score",
            "Declarer_DD_Tricks",
            "Declarer_DD_Score",
            "Declarer_ParScore",
            "Session",
        }
        self.assertFalse(forbidden & set(bridgestatslib.BOARD_RESULT_COLUMNS))
        self.assertFalse(forbidden & set(bridgestatslib.HAND_RECORD_COLUMNS))
        self.assertNotIn("tournament_board_results", bridgestatslib.SOURCE_FILES)


if __name__ == "__main__":
    unittest.main()
