import streamlit as st
import pandas as pd

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

st.header("Home Page of FFBridge BridgeStats")
st.subheader("To begin, click on one of the links on the left sidebar.")
st.subheader("Access to this project is by invitation only. The project is for research purposes.")
st.subheader("This project is in the proof of concept stage. Statistics should be considered provisional, perhaps unreliable.")
st.subheader("The mission is to display important statistical information by using data mining techniques.")
st.subheader("Data is from publicly available FFBridge tournament results. Anyone can access the same data.")
st.subheader("Statistics use FFBridge pair tournaments that have hand records, including national simultaneous series such as Rondes de France. No Competitions (the ACBL-tournament analog) are included.")
st.subheader("Club filters use FFBridge club codes so a simultaneous session can be restricted to one club.")
st.subheader("A Connection Error is usually due to a lack of memory on the server. Try either reloading the webpage immediately or come back later.")
st.subheader("An Error 410 requires that you clear your browser's cache before proceeding.")
st.caption("Project lead is Robert Salita research@AiPolice.org. Code written in Python and is currently not publically available. UI is written in Streamlit. Database is parquet. Query engine is DuckDB. Website is self-hosted using Ubuntu and Cloudflare Tunnel.")
st.caption(f"Pandas:{pd.__version__}")

st.sidebar.markdown("---")
st.sidebar.markdown("**Other Morty websites**")
st.sidebar.markdown("🔗 [ACBL Elo Ratings](https://acbl-elo.7nt.info)")
st.sidebar.markdown("🔗 [ACBL Postmortem](https://acbl.postmortem.chat)")
st.sidebar.markdown("🔗 [ACBL Statistics](https://acbl-stats.7nt.info)")
st.sidebar.markdown("🔗 [FFBridge Elo Ratings](https://ffbridge-elo.7nt.info)")
st.sidebar.markdown("🔗 [FFBridge Postmortem](https://ffbridge.postmortem.chat)")
st.sidebar.markdown("🔗 [FFBridge Statistics](https://ffbridge-stats.7nt.info)")
st.sidebar.markdown("🔗 [PBN/LIN Postmortem](https://pbn.postmortem.chat)")
