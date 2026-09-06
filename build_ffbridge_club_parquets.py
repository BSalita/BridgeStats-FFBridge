"""Build FFBridge Club BridgeStats parquets from the simultaneous cache.

Runtime never calls Lancelot. This builder reads the existing
E:\\bridge\\data\\ffbridge cache (and optionally refreshes missing sessions
through the quality pipeline / mlBridge) then writes ACBL-shaped Club files.

Usage:
    python build_ffbridge_club_parquets.py --demo
    python build_ffbridge_club_parquets.py --source-dir E:\\bridge\\data\\ffbridge\\data
    python build_ffbridge_club_parquets.py --training-parquet path\\to\\ffbridge_training_data_df.parquet
"""

from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys
from typing import Any, Dict, List, Optional, Sequence

import polars as pl

from bridgestatslib import (
    BOARD_RESULT_COLUMNS,
    HAND_RECORD_COLUMNS,
    resolve_data_path,
)

SEATS = ("N", "E", "S", "W")
PARTNER = {"N": "S", "S": "N", "E": "W", "W": "E"}
ON_LEAD = {"N": "E", "E": "S", "S": "W", "W": "N"}
NOT_ON_LEAD = {"N": "W", "E": "N", "S": "E", "W": "S"}

BOARD_FILENAME = "ffbridge_club_board_results_augmented.parquet"
HAND_FILENAME = "ffbridge_club_hand_records_augmented_narrow.parquet"
PLAYER_FILENAME = "ffbridge_player_info.parquet"
CLUB_FILENAME = "ffbridge_clubs.parquet"


def _first_present(frame: pl.DataFrame, names: Sequence[str]) -> Optional[str]:
    for name in names:
        if name in frame.columns:
            return name
    return None


def _as_string(frame: pl.DataFrame, name: str) -> pl.Expr:
    return pl.col(name).cast(pl.Utf8)


def _seat_player_expr(direction_col: str, prefix: str) -> pl.Expr:
    expr = pl.lit(None, dtype=pl.Utf8)
    for seat in SEATS:
        col = f"{prefix}{seat}"
        expr = (
            pl.when(pl.col(direction_col) == seat)
            .then(pl.col(col).cast(pl.Utf8))
            .otherwise(expr)
        )
    return expr


def map_board_results(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize an augmented or raw FFBridge frame to Club board-result columns."""
    out = frame
    club_col = _first_present(out, ("Club", "club_code", "simultaneousId", "club"))
    if club_col and club_col != "Club":
        out = out.with_columns(_as_string(out, club_col).alias("Club"))
    elif "Club" in out.columns:
        out = out.with_columns(_as_string(out, "Club").alias("Club"))
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Club"))

    date_col = _first_present(out, ("Date", "game_date", "session_date"))
    if date_col:
        if out.schema[date_col] == pl.Date:
            if date_col != "Date":
                out = out.with_columns(pl.col(date_col).alias("Date"))
        else:
            out = out.with_columns(
                pl.col(date_col).cast(pl.Utf8).str.to_date(strict=False).alias("Date")
            )
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Date).alias("Date"))

    if "session_id" not in out.columns:
        out = out.with_columns(pl.lit("unknown").alias("session_id"))
    out = out.with_columns(pl.col("session_id").cast(pl.Utf8))

    if "Declarer_Direction" in out.columns:
        out = out.with_columns(
            pl.col("Declarer_Direction").cast(pl.Utf8).str.replace("O", "W")
        )
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Declarer_Direction"))

    for seat in SEATS:
        col = f"Player_ID_{seat}"
        if col not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
        else:
            out = out.with_columns(pl.col(col).cast(pl.Utf8))

    if "Declarer" not in out.columns:
        out = out.with_columns(
            _seat_player_expr("Declarer_Direction", "Player_ID_").alias("Declarer")
        )
    else:
        out = out.with_columns(pl.col("Declarer").cast(pl.Utf8))

    if "Dummy" not in out.columns:
        dummy = pl.lit(None, dtype=pl.Utf8)
        for seat, partner in PARTNER.items():
            dummy = (
                pl.when(pl.col("Declarer_Direction") == seat)
                .then(pl.col(f"Player_ID_{partner}"))
                .otherwise(dummy)
            )
        out = out.with_columns(dummy.alias("Dummy"))
    if "OnLead" not in out.columns:
        onlead = pl.lit(None, dtype=pl.Utf8)
        for seat, left in ON_LEAD.items():
            onlead = (
                pl.when(pl.col("Declarer_Direction") == seat)
                .then(pl.col(f"Player_ID_{left}"))
                .otherwise(onlead)
            )
        out = out.with_columns(onlead.alias("OnLead"))
    if "NotOnLead" not in out.columns:
        right = pl.lit(None, dtype=pl.Utf8)
        for seat, opp in NOT_ON_LEAD.items():
            right = (
                pl.when(pl.col("Declarer_Direction") == seat)
                .then(pl.col(f"Player_ID_{opp}"))
                .otherwise(right)
            )
        out = out.with_columns(right.alias("NotOnLead"))

    if "Declarer_Name" not in out.columns:
        name_col = _first_present(out, ("Player_Name_N", "firstName", "Declarer_Name"))
        if name_col:
            out = out.with_columns(pl.col(name_col).cast(pl.Utf8).alias("Declarer_Name"))
        else:
            out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Declarer_Name"))

    for seat in SEATS:
        col = f"Player_Name_{seat}"
        if col not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    score_col = _first_present(out, ("Score_Declarer", "nsScore", "NS_Score"))
    if score_col == "Score_Declarer":
        pass
    elif score_col:
        ns = pl.col(score_col).cast(pl.Int64, strict=False)
        if "ewScore" in out.columns:
            ew = pl.col("ewScore").cast(pl.Int64, strict=False)
            out = out.with_columns(
                pl.when(pl.col("Declarer_Direction").is_in(["N", "S"]))
                .then(ns)
                .otherwise(ew)
                .alias("Score_Declarer")
            )
        else:
            out = out.with_columns(ns.alias("Score_Declarer"))
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Int64).alias("Score_Declarer"))

    numeric_defaults: Dict[str, Any] = {
        "ParScore": 0,
        "MP_Par_Pct_Declarer": 0.5,
        "DD_Tricks": 0,
        "Tricks": 0,
        "DD_Score_Declarer": 0,
        "MP_DD_Pct_Declarer": 0.5,
        "EV_Score_Declarer": 0,
        "EV_Max_Declarer": 0,
        "MP_EV_Pct_Declarer": 0.5,
        "MP_EV_Max_Pct_Declarer": 0.5,
        "Declarer_Pct": 0.5,
        "Board": 0,
        "Result": 0,
        "BidLvl": 0,
    }
    aliases = {
        "DD_Tricks": ("DD_Tricks", "DDTricks", "dd_tricks"),
        "Tricks": ("Tricks", "tricks", "ResultMade"),
        "ParScore": ("ParScore", "ParScore_NS", "par"),
        "DD_Score_Declarer": ("DD_Score_Declarer", "DDScore_Declarer", "DD_Score"),
        "Declarer_Pct": ("Declarer_Pct", "Percentage", "pct"),
        "Board": ("Board", "boardNumber", "board_id"),
        "Result": ("Result", "result"),
        "BidLvl": ("BidLvl", "bid_level"),
    }
    for dest, default in numeric_defaults.items():
        if dest in out.columns:
            continue
        src = _first_present(out, aliases.get(dest, (dest,)))
        if src:
            out = out.with_columns(pl.col(src).alias(dest))
        else:
            out = out.with_columns(pl.lit(default).alias(dest))

    string_defaults = {
        "Vul_Declarer": "None",
        "BidSuit": "N",
        "Dbl": "",
        "Vul": "None",
        "ContractType": "",
        "PBN": "",
        "HandRecordBoard": "",
    }
    string_aliases = {
        "PBN": ("PBN", "board_deal", "deal"),
        "HandRecordBoard": ("HandRecordBoard", "board_id", "Board"),
        "BidSuit": ("BidSuit",),
        "ContractType": ("ContractType",),
        "Vul": ("Vul", "Vulnerability"),
        "Vul_Declarer": ("Vul_Declarer",),
    }
    for dest, default in string_defaults.items():
        if dest in out.columns:
            continue
        src = _first_present(out, string_aliases.get(dest, (dest,)))
        if src:
            out = out.with_columns(pl.col(src).cast(pl.Utf8).alias(dest))
        else:
            out = out.with_columns(pl.lit(default).alias(dest))

    if "HandRecordBoard" in out.columns:
        out = out.with_columns(
            pl.when(pl.col("HandRecordBoard").cast(pl.Utf8).str.len_chars() == 0)
            .then(pl.col("session_id") + "_" + pl.col("Board").cast(pl.Utf8))
            .otherwise(pl.col("HandRecordBoard").cast(pl.Utf8))
            .alias("HandRecordBoard")
        )

    selected = []
    for col in BOARD_RESULT_COLUMNS:
        if col in out.columns:
            selected.append(pl.col(col))
        else:
            selected.append(pl.lit(None).alias(col))
    return out.select(selected)


def map_hand_records(frame: pl.DataFrame) -> pl.DataFrame:
    out = frame
    if "PBN" not in out.columns:
        pbn = _first_present(out, ("board_deal", "deal"))
        if pbn:
            out = out.with_columns(pl.col(pbn).cast(pl.Utf8).alias("PBN"))
        else:
            return pl.DataFrame({col: [] for col in HAND_RECORD_COLUMNS})
    out = out.filter(
        pl.col("PBN").is_not_null() & (pl.col("PBN").cast(pl.Utf8).str.len_chars() > 0)
    )
    if "game_date" not in out.columns:
        date_col = _first_present(out, ("Date", "session_date"))
        if date_col:
            out = out.with_columns(pl.col(date_col).cast(pl.Date).alias("game_date"))
        else:
            out = out.with_columns(pl.lit(None, dtype=pl.Date).alias("game_date"))
    if "session_id" not in out.columns:
        out = out.with_columns(pl.lit("unknown").alias("session_id"))
    if "HandRecordBoard" not in out.columns:
        board = _first_present(out, ("Board", "board_id"))
        if board:
            out = out.with_columns(pl.col(board).cast(pl.Utf8).alias("HandRecordBoard"))
        else:
            out = out.with_columns(pl.lit("").alias("HandRecordBoard"))
    defaults: Dict[str, Any] = {
        "ParScore": 0,
        "CT_N_S": 0,
        "CT_N_H": 0,
        "CT_N_D": 0,
        "CT_N_C": 0,
        "CT_N_N": 0,
        "DD_N_C": 0,
        "DD_N_D": 0,
        "DD_N_H": 0,
        "DD_N_S": 0,
        "DD_N_N": 0,
        "SL_N_C": 0,
        "SL_N_D": 0,
        "SL_N_H": 0,
        "SL_N_S": 0,
        "SL_N_ML_SJ": 0,
        "HCP_NS": 0,
        "HCP_EW": 0,
        "HCP_N": 0,
        "HCP_E": 0,
        "HCP_S": 0,
        "HCP_W": 0,
        "QT_N": 0.0,
        "QT_E": 0.0,
        "QT_S": 0.0,
        "QT_W": 0.0,
        "QT_NS": 0.0,
        "QT_EW": 0.0,
        "DP_N": 0,
        "DP_N_C": 0,
        "DP_N_D": 0,
        "DP_N_H": 0,
        "DP_N_S": 0,
        "DP_NS": 0,
        "DP_EW": 0,
    }
    for col, default in defaults.items():
        if col not in out.columns:
            alias = _first_present(out, (col, col.replace("DD_N_", "DD_N_"), f"DD_{col[-1]}_{col.split('_')[-1]}" if False else col))
            if alias and alias != col:
                out = out.with_columns(pl.col(alias).alias(col))
            else:
                out = out.with_columns(pl.lit(default).alias(col))
    out = out.unique(subset=["PBN"], maintain_order=True)
    return out.select(list(HAND_RECORD_COLUMNS))


def map_players(frame: pl.DataFrame) -> pl.DataFrame:
    rows: List[Dict[str, str]] = []
    club_col = _first_present(frame, ("Club", "club_code", "club"))
    for seat in SEATS:
        id_col = f"Player_ID_{seat}"
        if id_col not in frame.columns:
            continue
        name_col = f"Player_Name_{seat}"
        exprs = [pl.col(id_col).cast(pl.Utf8).alias("player_id")]
        if name_col in frame.columns:
            exprs.append(pl.col(name_col).cast(pl.Utf8).alias("display_name"))
        else:
            exprs.append(pl.lit("").alias("display_name"))
        exprs.append(
            pl.col(club_col).cast(pl.Utf8).alias("club")
            if club_col
            else pl.lit("").alias("club")
        )
        work = frame.select(exprs).filter(
            pl.col("player_id").is_not_null() & (pl.col("player_id") != "")
        )
        for rec in work.unique(subset=["player_id"]).iter_rows(named=True):
            name = str(rec.get("display_name") or "").strip()
            parts = name.split(" ", 1) if name else ["", ""]
            rows.append(
                {
                    "player_id": rec["player_id"],
                    "first_name": parts[0],
                    "last_name": parts[1] if len(parts) > 1 else "",
                    "club": rec.get("club") or "",
                }
            )
    if not rows:
        return pl.DataFrame(
            {"player_id": [], "first_name": [], "last_name": [], "club": []}
        )
    return pl.DataFrame(rows).unique(subset=["player_id"], maintain_order=True)


def map_clubs(frame: pl.DataFrame) -> pl.DataFrame:
    club_col = _first_present(frame, ("Club", "club_code", "club", "id"))
    name_col = _first_present(frame, ("club_name", "Club_Name", "name"))
    if not club_col:
        return pl.DataFrame({"id": [], "name": []})
    work = frame.select(
        [
            pl.col(club_col).cast(pl.Utf8).alias("id"),
            pl.col(name_col).cast(pl.Utf8).alias("name")
            if name_col
            else pl.lit("").alias("name"),
        ]
    ).filter(pl.col("id").is_not_null() & (pl.col("id") != ""))
    return work.unique(subset=["id"], maintain_order=True)


def write_outputs(
    output_dir: pathlib.Path,
    boards: pl.DataFrame,
    hands: pl.DataFrame,
    players: pl.DataFrame,
    clubs: pl.DataFrame,
) -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        BOARD_FILENAME: boards,
        HAND_FILENAME: hands,
        PLAYER_FILENAME: players,
        CLUB_FILENAME: clubs,
    }
    written = {}
    for name, frame in paths.items():
        path = output_dir / name
        frame.write_parquet(path)
        written[name] = str(path)
        print(f"[ffbridge-stats-builder] wrote {path} ({frame.height} rows)", flush=True)
    return written


def demo_frames() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    boards = pl.DataFrame(
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
            "Player_Name_N": ["Robert Salita", "Kerry Flom"],
            "Player_Name_E": ["A East", "C East"],
            "Player_Name_S": ["Kerry Flom", "Robert Salita"],
            "Player_Name_W": ["B West", "D West"],
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
            "HandRecordBoard": ["s1_1", "s1_1"],
            "Board": [1, 1],
            "Result": [1, -1],
            "BidLvl": [4, 4],
            "BidSuit": ["H", "H"],
            "Dbl": ["", ""],
            "Vul": ["None", "None"],
            "ContractType": ["game", "game"],
            "PBN": ["N:AKQxxx.xxx.xxx.xx E:...", "N:AKQxxx.xxx.xxx.xx E:..."],
        }
    )
    hands = map_hand_records(boards.with_columns(pl.col("Date").alias("game_date")))
    players = pl.DataFrame(
        {
            "player_id": ["246273", "282839", "111111", "222222", "333333", "444444"],
            "first_name": ["Robert", "Kerry", "A", "C", "B", "D"],
            "last_name": ["Salita", "Flom", "East", "East", "West", "West"],
            "club": ["750001"] * 6,
        }
    )
    clubs = pl.DataFrame({"id": ["750001"], "name": ["Paris Bridge"]})
    return boards, hands, players, clubs


def build_from_frame(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    boards = map_board_results(frame)
    hands = map_hand_records(frame if "PBN" in frame.columns or "board_deal" in frame.columns else boards)
    players = map_players(frame if any(f"Player_ID_{s}" in frame.columns for s in SEATS) else boards)
    clubs = map_clubs(frame if _first_present(frame, ("Club", "club_code", "club")) else boards)
    if players.height == 0:
        players = map_players(boards)
    if clubs.height == 0:
        clubs = map_clubs(boards)
    return boards, hands, players, clubs


def _load_training_or_augmented(path: pathlib.Path) -> pl.DataFrame:
    print(f"[ffbridge-stats-builder] reading {path}", flush=True)
    return pl.read_parquet(path)


def _try_build_from_quality_cache(source_dir: pathlib.Path, limit: Optional[int]) -> Optional[pl.DataFrame]:
    """Reuse the Elo quality pipeline's raw+augment path when available."""
    elo_dir = pathlib.Path(__file__).resolve().parent.parent / "elo"
    if str(elo_dir) not in sys.path:
        sys.path.insert(0, str(elo_dir))
    try:
        from ffbridge_quality_pipeline import (  # type: ignore
            audit_historical_cache,
            augment_raw_session,
            load_raw_session,
        )
    except Exception as exc:
        print(f"[ffbridge-stats-builder] quality pipeline unavailable: {exc}", flush=True)
        return None
    try:
        audit = audit_historical_cache(source_dir)
    except Exception as exc:
        print(f"[ffbridge-stats-builder] cache audit failed: {exc}", flush=True)
        return None
    frames: List[pl.DataFrame] = []
    complete = [session for session in audit.sessions if session.complete]
    if limit:
        complete = complete[:limit]
    print(f"[ffbridge-stats-builder] augmenting {len(complete)} cached sessions", flush=True)
    for session in complete:
        try:
            raw, _unmapped = load_raw_session(source_dir, session)
            if "Date" not in raw.columns and session.session_date:
                raw = raw.with_columns(pl.lit(session.session_date).alias("Date"))
            augmented = augment_raw_session(raw)
            frames.append(augmented)
        except Exception as exc:
            print(
                f"[ffbridge-stats-builder] skip session {session.session_id}: {exc}",
                flush=True,
            )
    if not frames:
        return None
    return pl.concat(frames, how="diagonal_relaxed")


def build(
    *,
    output_dir: pathlib.Path,
    source_dir: Optional[pathlib.Path] = None,
    training_parquet: Optional[pathlib.Path] = None,
    demo: bool = False,
    session_limit: Optional[int] = None,
) -> Dict[str, str]:
    if demo:
        boards, hands, players, clubs = demo_frames()
        return write_outputs(output_dir, boards, hands, players, clubs)

    frame: Optional[pl.DataFrame] = None
    if training_parquet and training_parquet.is_file():
        frame = _load_training_or_augmented(training_parquet)
    elif source_dir:
        training = source_dir / "ffbridge_training_data_df.parquet"
        if training.is_file():
            frame = _load_training_or_augmented(training)
        else:
            frame = _try_build_from_quality_cache(source_dir, session_limit)
    if frame is None or frame.height == 0:
        raise FileNotFoundError(
            "No FFBridge Club source found. Pass --training-parquet, "
            "--source-dir with cache/training data, or --demo."
        )
    boards, hands, players, clubs = build_from_frame(frame)
    return write_outputs(output_dir, boards, hands, players, clubs)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=None,
        help="Destination directory (default: BridgeStats data path).",
    )
    parser.add_argument(
        "--source-dir",
        type=pathlib.Path,
        default=pathlib.Path(r"E:\bridge\data\ffbridge\data"),
        help="Raw FFBridge simultaneous cache root.",
    )
    parser.add_argument(
        "--training-parquet",
        type=pathlib.Path,
        default=None,
        help="Optional already-augmented training parquet.",
    )
    parser.add_argument("--demo", action="store_true", help="Write a tiny fixture dataset.")
    parser.add_argument(
        "--session-limit",
        type=int,
        default=None,
        help="When augmenting from cache, process at most this many sessions.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    output_dir = args.output_dir or resolve_data_path()
    written = build(
        output_dir=output_dir,
        source_dir=args.source_dir,
        training_parquet=args.training_parquet,
        demo=args.demo,
        session_limit=args.session_limit,
    )
    print(json.dumps({"output_dir": str(output_dir), "files": written}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
