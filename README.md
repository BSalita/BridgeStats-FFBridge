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

## Design notes

### Contract rarity and n-off contracts (designed, not implemented)

Decision (October 2026): documented only, in FFBridge and ACBL BridgeStats alike. The
postmortem "unique bids" favorites are unchanged.

- Key: per board (`session_id`, `HandRecordBoard`), a contract is
  `BidLvl || BidSuit || Dbl || NS/EW`. Rows without a contract (`BidLvl IS NULL`) are skipped,
  and boards need at least 4 tables.
- `Tables` is `COUNT(*) OVER (PARTITION BY session_id, HandRecordBoard)` and `Same_Contract`
  adds the contract key to the partition.
- n-off: `Same_Contract` itself. 1-off means only that table played the contract, 2-off means
  two tables did, and so on. A frequency favorite counts a player's boards by n-off.
- Rarity: `-log2(Same_Contract / Tables)` bits, and excess bits relative to the board average.
  A leaderboard averages them over a player's declared or dummy boards.
- Attribution: declarer and dummy names come from the `Player_Name_*` seat columns through
  `Declarer_Direction`.
- The candidate SQL starts with a `sessions` CTE filtered by the date, club, player, and pair
  macros. It then scans every row of those sessions, because the field is needed to count
  contracts. So `/favorites/{id}` would have to stop pre-filtering to the player's own rows.

Measured cost per query at 8 DuckDB threads: 0.9–2.3 s and at most 0.7 GB over the last 12
months, and 3.2–8.5 s and about 2 GB over the full history. ACBL figures are in the ACBL
BridgeStats README. The cost is paid per request; nothing is precomputed.

### DuckDB thread cap

Every query-serving DuckDB connection uses `threads = 8` (`DUCKDB_THREADS` in
`bridge_api_common.py`, and likewise in Elo, the four postmortem services, and the chatbot).
DuckDB's default is every core (128 on the server). Window-heavy favorites ran 2–14× slower at
128 threads than at 8, and simple aggregates were no faster. 8 and 16 threads measured about
the same.

### Heavy favorites

`Pair_Head_To_Head` and `Identical_Boards` self-join the field. With no player or pair filter
over 12 months, `Pair_Head_To_Head` took 339 s and 50 GB and returned 28M rows, and
`Identical_Boards` took 39 s and 10.8 GB. The API materializes the whole result before
truncating it to the row limit, so run these with a player or pair filter.

### Data issues fixed in the builder (October 2026)

These take effect after the Club parquets are rebuilt without `--if-stale`. Existing session
fragments are corrected when they are combined, so no session has to be re-augmented.

- `Declarer_Name` was always North's name. It now comes from the declaring seat's
  `Player_Name_*`.
- `Declarer` values `-` and empty (rows with no contract) become null instead of placeholder
  ids.
