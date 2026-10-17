# FFBridge BridgeStats

Sibling of `bridgestats-acbl` for FFBridge pair tournaments, including national
simultaneous series (Rondes de France, Roy René, Octopus). Competitions (the
ACBL-tournament analog) are out of scope.

## Run locally

```bash
# Terminal 1: API on :8525
python bridgestats_api_server.py

# Terminal 2: Streamlit on :8524
streamlit run Home.py --server.port 8524
```

Cloudflare `ffbridge-stats` should point at `http://localhost:8524`.

## Data

Runtime reads local Club parquets only. Lancelot is used only by
`build_ffbridge_club_parquets.py` to refresh those files.

```bash
python build_ffbridge_club_parquets.py --demo
python build_ffbridge_club_parquets.py --source-dir E:\bridge\data\ffbridge\data
python build_ffbridge_club_parquets.py --from-quality-cache --source-dir E:\bridge\data\ffbridge\data
```

Full refresh + publish is `..\ffbridge-pipeline\ffbridge_all.bat` (the `acbl_all.bat` counterpart).

Expected files:

- `ffbridge_club_board_results_augmented.parquet`
- `ffbridge_club_hand_records_augmented_narrow.parquet`
- `ffbridge_player_info.parquet`
- `ffbridge_clubs.parquet`

`club_board_results.Dealer` is stored on the current parquet. Older files
still receive that column from the standard duplicate-board rotation.

`Declarer` in SQL and reports is the declarer's player id. A rebuilt file may
store N/E/S/W in that column; the API replaces it from `Player_ID_*`.
`Declarer_Direction` stays the compass. `Result` is overtricks as an integer
(`+2`, `=`, and `-1` in the file). Compare `Score_Declarer` with `ParScore`.

Player `numbers` accept a license, a Lancelot person id, or a Classic id.
`data/lancelot_persons.parquet` supplies the aliases. Seat columns and the
lookup `player_id` are the Classic id.

Search roots: `BRIDGESTATS_FFBRIDGE_DATA_DIR`, `data/`, `E:\bridge\data\ffbridge`.
