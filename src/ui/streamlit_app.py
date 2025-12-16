import time
import requests
import pandas as pd
import streamlit as st
import plotly.express as px

API = "http://127.0.0.1:8000"

st.set_page_config(page_title="Trader Analytics MVP", layout="wide")

@st.cache_data(ttl=2)
def get_symbols():
    return requests.get(f"{API}/symbols").json().get("symbols", [])

def get_latest_tick(symbol):
    return requests.get(f"{API}/latest_tick", params={"symbol": symbol}).json()

def get_bars(symbol, tf, lookback_sec):
    r = requests.get(f"{API}/bars", params={"symbol": symbol, "tf": tf, "lookback_sec": lookback_sec}).json()
    return pd.DataFrame(r["rows"])

def get_pairs(a, b, tf, window, lookback_sec):
    r = requests.get(f"{API}/pairs/analytics", params={"a": a, "b": b, "tf": tf, "window": window, "lookback_sec": lookback_sec}).json()
    return r.get("stats", {}), pd.DataFrame(r.get("table", []))

st.title("Real-time Trader Helper Analytics (MVP)")

symbols = get_symbols()
if len(symbols) < 2:
    st.warning("Waiting for ticks... keep backend running for a few seconds.")
    st.stop()

with st.sidebar:
    st.header("Controls")
    a = st.selectbox("Symbol A", symbols, index=0)
    b = st.selectbox("Symbol B", symbols, index=min(1, len(symbols)-1))
    tf = st.selectbox("Timeframe", ["1s", "1m", "5m"], index=1)
    window = st.slider("Rolling window (bars)", 20, 300, 60, 5)
    lookback_sec = st.slider("Lookback (seconds)", 600, 6*3600, 3600, 300)
    live_refresh = st.select_slider("Live refresh (sec)", options=[0.5, 1.0, 2.0, 5.0], value=0.5)

    st.divider()
    st.subheader("Alert")
    alert_name = st.text_input("Name", value="Zscore High")
    threshold = st.number_input("z-score threshold", value=2.0, step=0.1)
    if st.button("Create alert rule"):
        requests.post(f"{API}/alerts", params={"name": alert_name, "a": a, "b": b, "timeframe": tf, "window": window, "threshold": threshold, "enabled": True})
        st.success("Alert rule created")

col1, col2, col3, col4 = st.columns(4)
tick_a = get_latest_tick(a)
tick_b = get_latest_tick(b)

col1.metric(f"{a} last", f"{tick_a.get('price','-')}")
col2.metric(f"{b} last", f"{tick_b.get('price','-')}")
col3.metric("Tick ts", str(tick_a.get("ts_iso", "-"))[:19])
col4.metric("TF / window", f"{tf} / {window}")

stats, table = get_pairs(a, b, tf, window, lookback_sec)

m1, m2, m3, m4 = st.columns(4)
m1.metric("beta (OLS)", f"{stats.get('beta','-'):.4f}" if isinstance(stats.get("beta"), (int,float)) else "-")
m2.metric("spread", f"{stats.get('latest_spread','-'):.6f}" if isinstance(stats.get("latest_spread"), (int,float)) else "-")
m3.metric("z-score", f"{stats.get('latest_zscore','-'):.3f}" if isinstance(stats.get("latest_zscore"), (int,float)) else "-")
m4.metric("rolling corr", f"{stats.get('latest_corr','-'):.3f}" if isinstance(stats.get("latest_corr"), (int,float)) else "-")

left, right = st.columns(2)

with left:
    st.subheader("Prices (close)")
    if not table.empty:
        dfp = table[["ts","a","b"]].copy()
        fig = px.line(dfp, x="ts", y=["a","b"])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough resampled data yet.")

with right:
    st.subheader("Spread & z-score")
    if not table.empty:
        dfs = table[["ts","spread","zscore"]].copy()
        fig = px.line(dfs, x="ts", y=["spread","zscore"])
        st.plotly_chart(fig, use_container_width=True)

c1, c2 = st.columns(2)
with c1:
    st.subheader("Volume")
    if not table.empty:
        dfv = table[["ts","vol_a","vol_b"]].copy()
        fig = px.bar(dfv, x="ts", y=["vol_a","vol_b"])
        st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Rolling correlation")
    if not table.empty:
        dfc = table[["ts","rolling_corr"]].copy()
        fig = px.line(dfc, x="ts", y=["rolling_corr"])
        st.plotly_chart(fig, use_container_width=True)

st.divider()
d1, d2, d3 = st.columns([1,1,2])

with d1:
    if st.button("Run ADF test"):
        adf = requests.get(f"{API}/pairs/adf", params={"a": a, "b": b, "tf": tf, "window": window, "lookback_sec": lookback_sec}).json()
        st.session_state["adf"] = adf

with d2:
    st.download_button("Download analytics.csv",
        data=requests.get(f"{API}/export/analytics.csv", params={"a": a, "b": b, "tf": tf, "window": window, "lookback_sec": lookback_sec}).content,
        file_name="analytics.csv",
        mime="text/csv"
    )

with d3:
    st.download_button("Download bars(A).csv",
        data=requests.get(f"{API}/export/bars.csv", params={"symbol": a, "tf": tf, "lookback_sec": lookback_sec}).content,
        file_name="bars.csv",
        mime="text/csv"
    )

if "adf" in st.session_state:
    st.subheader("ADF Result")
    st.json(st.session_state["adf"])

st.subheader("Alert events")
events = requests.get(f"{API}/alerts/events").json().get("events", [])
st.dataframe(pd.DataFrame(events), use_container_width=True)

# Live refresh
time.sleep(float(live_refresh))
st.rerun()
