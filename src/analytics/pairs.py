import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import OLS
from statsmodels.tools.tools import add_constant
from statsmodels.tsa.stattools import adfuller

def resample_ohlcv(df_ticks: pd.DataFrame, tf: str) -> pd.DataFrame:
    # df_ticks index = datetime, cols: price, size
    ohlc = df_ticks["price"].resample(tf).ohlc()
    vol = df_ticks["size"].resample(tf).sum().rename("volume")
    out = pd.concat([ohlc, vol], axis=1).dropna()
    return out

def compute_beta_ols(series_a: pd.Series, series_b: pd.Series) -> float:
    # log-price regression: log(A) = c + beta*log(B)
    a = np.log(series_a.values)
    b = np.log(series_b.values)
    X = add_constant(b)
    model = OLS(a, X, missing="drop").fit()
    return float(model.params[1])

def compute_pairs_analytics(bars_a: pd.DataFrame, bars_b: pd.DataFrame, window: int):
    # align by timestamp
    df = pd.DataFrame({
        "a": bars_a["close"],
        "b": bars_b["close"],
        "vol_a": bars_a.get("volume", 0.0),
        "vol_b": bars_b.get("volume", 0.0),
    }).dropna()

    if len(df) < max(30, window + 5):
        return {"error": "not enough data"}, None

    beta = compute_beta_ols(df["a"], df["b"])
    spread = np.log(df["a"]) - beta * np.log(df["b"])
    spread = spread.rename("spread")

    m = spread.rolling(window).mean()
    s = spread.rolling(window).std(ddof=0)
    z = ((spread - m) / s).rename("zscore")

    ret_a = np.log(df["a"]).diff()
    ret_b = np.log(df["b"]).diff()
    corr = ret_a.rolling(window).corr(ret_b).rename("rolling_corr")

    out = pd.concat([df, spread, z, corr], axis=1).dropna()

    latest = out.iloc[-1]
    stats = {
        "beta": beta,
        "latest_spread": float(latest["spread"]),
        "latest_zscore": float(latest["zscore"]),
        "latest_corr": float(latest["rolling_corr"]),
        "n": int(len(out)),
    }
    return stats, out

def adf_test_on_spread(spread: pd.Series):
    res = adfuller(spread.dropna().values, autolag="AIC")
    return {
        "adf_stat": float(res[0]),
        "p_value": float(res[1]),
        "used_lag": int(res[2]),
        "nobs": int(res[3]),
        "crit_values": {k: float(v) for k, v in res[4].items()},
    }
