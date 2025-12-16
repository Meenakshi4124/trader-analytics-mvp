import asyncio
import time
from fastapi import FastAPI, Query
from fastapi.responses import Response
import pandas as pd

from src.storage.sqlite_repo import init_db, insert_tick, read_ticks, list_symbols, upsert_alert_rule, get_alert_rules, log_alert_event, get_alert_events
from src.data_sources.binance_ws import stream_trades
from src.analytics.pairs import resample_ohlcv, compute_pairs_analytics, adf_test_on_spread

app = FastAPI(title="Trader Analytics MVP")

# In-memory last tick cache for UI speed
LAST = {}  # symbol -> dict

TF_MAP = {"1s": "1S", "1m": "1min", "5m": "5min"}

@app.on_event("startup")
async def startup():
    init_db()

    # default symbols for demo (you can change in UI later)
    symbols = ["btcusdt", "ethusdt", "bnbusdt", "solusdt"]

    async def on_tick(t):
        LAST[t["symbol"]] = t
        insert_tick(t["ts_ms"], t["ts_iso"], t["symbol"], t["price"], t["size"])

    # background WS task
    asyncio.create_task(stream_trades(symbols, on_tick))

    # background alert checker
    asyncio.create_task(alert_loop())

@app.get("/symbols")
def symbols():
    # union: seen in db + default cache
    s = sorted(set(list_symbols(200)) | set(LAST.keys()))
    return {"symbols": s}

@app.get("/latest_tick")
def latest_tick(symbol: str):
    return LAST.get(symbol.lower(), {})

def _ticks_to_df(rows):
    # rows: (ts_ms, ts_iso, price, size)
    if not rows:
        return pd.DataFrame(columns=["price", "size"])
    df = pd.DataFrame(rows, columns=["ts_ms", "ts_iso", "price", "size"])
    df["dt"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df = df.set_index("dt").sort_index()
    return df[["price", "size"]]

@app.get("/bars")
def bars(symbol: str, tf: str = Query("1m", pattern="^(1s|1m|5m)$"), lookback_sec: int = 3600):
    tfp = TF_MAP[tf]
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - int(lookback_sec * 1000)
    rows = read_ticks(symbol.lower(), since_ms)
    df = _ticks_to_df(rows)
    out = resample_ohlcv(df, tfp)
    out = out.reset_index()
    out["ts"] = out["dt"].astype(str)
    return {"rows": out.drop(columns=["dt"]).to_dict(orient="records")}

@app.get("/pairs/analytics")
def pairs_analytics(
    a: str, b: str,
    tf: str = Query("1m", pattern="^(1s|1m|5m)$"),
    window: int = 60,
    lookback_sec: int = 7200
):
    tfp = TF_MAP[tf]
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - int(lookback_sec * 1000)
    dfa = _ticks_to_df(read_ticks(a.lower(), since_ms))
    dfb = _ticks_to_df(read_ticks(b.lower(), since_ms))
    ba = resample_ohlcv(dfa, tfp)
    bb = resample_ohlcv(dfb, tfp)

    stats, table = compute_pairs_analytics(ba, bb, window)
    if table is None:
        return {"stats": stats, "table": []}

    t = table.reset_index()
    t["ts"] = t["dt"].astype(str)
    return {"stats": stats, "table": t.drop(columns=["dt"]).to_dict(orient="records")}

@app.get("/pairs/adf")
def pairs_adf(a: str, b: str, tf: str = Query("1m", pattern="^(1s|1m|5m)$"), window: int = 120, lookback_sec: int = 7200):
    res = pairs_analytics(a, b, tf=tf, window=window, lookback_sec=lookback_sec)
    table = pd.DataFrame(res["table"])
    if table.empty or "spread" not in table:
        return {"error": "not enough data"}
    return adf_test_on_spread(table["spread"])

@app.post("/alerts")
def create_alert(name: str, a: str, b: str, timeframe: str = "1m", window: int = 60, threshold: float = 2.0, enabled: bool = True):
    rule_id = upsert_alert_rule(name, a.lower(), b.lower(), timeframe, window, threshold, enabled)
    return {"id": rule_id}

@app.get("/alerts")
def list_alerts():
    rows = get_alert_rules()
    return {"rules": [
        {"id": r[0], "name": r[1], "a": r[2], "b": r[3], "tf": r[4], "window": r[5], "threshold": r[6], "enabled": bool(r[7])}
        for r in rows
    ]}

@app.get("/alerts/events")
def alert_events(limit: int = 200):
    rows = get_alert_events(limit)
    return {"events": [
        {"id": r[0], "ts_ms": r[1], "rule_id": r[2], "rule_name": r[3], "message": r[4]}
        for r in rows
    ]}

@app.get("/export/analytics.csv")
def export_analytics(a: str, b: str, tf: str = "1m", window: int = 60, lookback_sec: int = 7200):
    res = pairs_analytics(a, b, tf=tf, window=window, lookback_sec=lookback_sec)
    df = pd.DataFrame(res["table"])
    csv = df.to_csv(index=False).encode("utf-8")
    return Response(content=csv, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=analytics.csv"})

@app.get("/export/bars.csv")
def export_bars(symbol: str, tf: str = "1m", lookback_sec: int = 3600):
    res = bars(symbol, tf=tf, lookback_sec=lookback_sec)
    df = pd.DataFrame(res["rows"])
    csv = df.to_csv(index=False).encode("utf-8")
    return Response(content=csv, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=bars.csv"})

async def alert_loop():
    # simple rule: if latest zscore > threshold => log event
    while True:
        try:
            rules = get_alert_rules()
            for r in rules:
                rule_id, name, a, b, tf, window, threshold, enabled = r
                if not enabled:
                    continue
                res = pairs_analytics(a, b, tf=tf, window=int(window), lookback_sec=7200)
                stats = res.get("stats", {})
                z = stats.get("latest_zscore")
                if z is None or isinstance(z, dict):
                    continue
                if float(z) > float(threshold):
                    ts_ms = int(time.time() * 1000)
                    log_alert_event(rule_id, ts_ms, f"ALERT: {name} | zscore={z:.3f} > {threshold}")
        except Exception as e:
            print("[ALERT] error:", e)
        await asyncio.sleep(1.0)
