"""Simple Streamlit dashboard for CSV logs.

Run locally:
    streamlit run dashboard.py
"""
from __future__ import annotations

import os
import pandas as pd
import streamlit as st

DATA_DIR = os.getenv("DATA_DIR", "data")

st.set_page_config(page_title="Iron Condor Paper Bot", layout="wide")
st.title("NIFTY Weekly Iron Condor Paper Bot")


def load_csv(name: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        st.warning(f"Could not read {name}: {e}")
        return pd.DataFrame()

trades = load_csv("trades.csv")
capital = load_csv("capital.csv")
vix = load_csv("vix_log.csv")
snapshots = load_csv("snapshots.csv")
logs = load_csv("system_logs.csv")

c1, c2, c3, c4 = st.columns(4)
latest_cap = capital["capital"].iloc[-1] if not capital.empty and "capital" in capital else 0
active = trades[trades["status"] == "OPEN"] if not trades.empty and "status" in trades else pd.DataFrame()
last_pnl = snapshots["pnl_total"].iloc[-1] if not snapshots.empty and "pnl_total" in snapshots else 0
last_vix = vix["vix"].iloc[-1] if not vix.empty and "vix" in vix else 0

c1.metric("Paper Capital", f"₹{latest_cap:,.0f}")
c2.metric("Active Trades", len(active))
c3.metric("Latest PnL", f"₹{last_pnl:,.0f}")
c4.metric("Latest VIX", f"{last_vix:.2f}")

st.subheader("Trades")
st.dataframe(trades, use_container_width=True)

st.subheader("PnL Snapshots")
if not snapshots.empty:
    st.line_chart(snapshots.set_index("timestamp")[["pnl_total"]])
st.dataframe(snapshots, use_container_width=True)

st.subheader("VIX Log")
if not vix.empty:
    st.line_chart(vix.set_index("timestamp")[["vix"]])
st.dataframe(vix, use_container_width=True)

st.subheader("System Logs")
st.dataframe(logs.tail(100), use_container_width=True)
