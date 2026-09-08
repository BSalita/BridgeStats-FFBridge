import pathlib
import sys
import time

import streamlit as st

_APP_DIR = pathlib.Path(__file__).resolve().parent.parent
_SRC_DIR = _APP_DIR.parent
_streamlit = next(
    (p for p in (_APP_DIR / "streamlitlib", _SRC_DIR / "streamlitlib") if p.is_dir()),
    None,
)
if _streamlit is None:
    raise FileNotFoundError(f"streamlitlib not found under {_APP_DIR} or {_SRC_DIR}")
for _p in (_APP_DIR, _SRC_DIR, _streamlit):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.append(_s)
import streamlitlib  # must be placed after sys.path.append. vscode re-format likes to move this to the top

import bridgestats_api_client as api
import player_sidebar

st.header("Lookup Player Information")
st.sidebar.header("Settings for Player Lookup")
st.sidebar.header("Settings")

key_prefix = "Player_Lookup"
clubs = st.sidebar.text_input(
    "Narrow search to these FFBridge club codes. Enter one or more codes (empty means all):",
    placeholder="Enter list of FFBridge club codes",
    key=key_prefix + "-Club",
    help="Example: 750001",
)
player_names, player_numbers = player_sidebar.sidebar_player_filters(key_prefix)

with st.spinner(text="Reading data ..."):
    start_time = time.time()
    try:
        payload = api.player_lookup(
            clubs=clubs,
            numbers=player_numbers,
            names=player_names,
            limit=2000,
        )
    except api.BridgeStatsApiClientError as exc:
        st.error(str(exc))
        st.stop()
    selected_df = api.table_to_frame(payload)
    st.caption(
        f"Data read completed in {round(time.time() - start_time, 2)} seconds. "
        f"{payload.get('total', selected_df.height)} rows read."
    )

table, charts = st.tabs(["Data Table", "Charts"])
st.caption(
    f"Database has {payload.get('total', selected_df.height)} matching players. "
    f"Showing {selected_df.height} (cap 2000). Use the player name or number filters to search."
)
if selected_df.height == 0:
    st.warning("No rows selected")
    st.stop()

with table:
    with st.spinner(text="Creating data table ..."):
        start_time = time.time()
        streamlitlib.ShowDataFrameTable(selected_df)
        st.caption(f"Data table created in {round(time.time() - start_time, 2)} seconds.")

with charts:
    pass
