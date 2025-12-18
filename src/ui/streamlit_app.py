import time
import requests
import pandas as pd
import streamlit as st
import plotly.express as px
import numpy as np
import pandas as pd

def backtest_mean_reversion_zscore(
    df: pd.DataFrame,
    z_col: str = "zscore",
    ts_col: str = "ts",
    entry_z: float = 2.0,
    exit_z: float = 0.0,
):
    """
    Simple mean-reversion backtest on z-score.
    """
    if df is None or df.empty or z_col not in df.columns:
        return {"trades": 0}, pd.DataFrame()

    d = df[[ts_col, z_col]].dropna().copy()
    if d.empty:
        return {"trades": 0}, pd.DataFrame()

    d = d.sort_values(ts_col)

    pos = 0
    entry_idx = None
    entry_ts = None
    entry_zv = None

    trades = []

    z = d[z_col].to_numpy()
    ts = d[ts_col].to_numpy()

    for i in range(len(d)):
        zi = float(z[i])

        if pos == 0:
            if zi >= entry_z:
                pos = -1
                entry_idx = i
                entry_ts = ts[i]
                entry_zv = zi
            elif zi <= -entry_z:
                pos = +1
                entry_idx = i
                entry_ts = ts[i]
                entry_zv = zi

        elif pos == -1 and zi <= exit_z:
            trades.append({
                "side": "SHORT",
                "entry_ts": entry_ts,
                "exit_ts": ts[i],
                "entry_z": entry_zv,
                "exit_z": zi,
                "z_move": entry_zv - zi,
                "bars_held": i - entry_idx,
            })
            pos = 0

        elif pos == +1 and zi >= -exit_z:
            trades.append({
                "side": "LONG",
                "entry_ts": entry_ts,
                "exit_ts": ts[i],
                "entry_z": entry_zv,
                "exit_z": zi,
                "z_move": zi - entry_zv,
                "bars_held": i - entry_idx,
            })
            pos = 0

    trades_df = pd.DataFrame(trades)

    if trades_df.empty:
        return {"trades": 0}, trades_df

    trades_df["win"] = trades_df["z_move"] > 0

    summary = {
        "trades": len(trades_df),
        "win_rate": trades_df["win"].mean(),
        "avg_bars_held": trades_df["bars_held"].mean(),
        "avg_z_move": trades_df["z_move"].mean(),
    }

    return summary, trades_df


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

if table.empty:
    st.info("Not enough resampled data yet.")
    st.stop()



st.sidebar.markdown("### 🧪 Backtest (Z-score rules)")

entry_z = st.sidebar.number_input(
    "Backtest entry |z|", value=2.0, step=0.1
)
exit_z = st.sidebar.number_input(
    "Backtest exit to 0", value=0.0, step=0.1
)

bt_summary, bt_trades = backtest_mean_reversion_zscore(
    table,                 # 👈 use table (your variable)
    z_col="zscore",
    ts_col="ts",
    entry_z=float(entry_z),
    exit_z=float(exit_z),
)

st.subheader("🧪 Mini mean-reversion backtest (z-score rules)")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Trades", bt_summary.get("trades", 0))
c2.metric(
    "Win rate",
    f"{bt_summary.get('win_rate', 0.0)*100:.1f}%"
    if bt_summary.get("trades", 0) > 0 else "—"
)
c3.metric(
    "Avg bars held",
    f"{bt_summary.get('avg_bars_held', 0.0):.1f}"
    if bt_summary.get("trades", 0) > 0 else "—"
)
c4.metric(
    "Avg z-move",
    f"{bt_summary.get('avg_z_move', 0.0):.3f}"
    if bt_summary.get("trades", 0) > 0 else "—"
)

with st.expander("View trades"):
    st.dataframe(bt_trades, use_container_width=True)

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
