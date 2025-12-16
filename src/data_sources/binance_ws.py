import asyncio
import json
import time
from datetime import datetime, timezone
import websockets

# Binance combined stream: wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
BINANCE_WS = "wss://stream.binance.com:9443/stream"

def iso_now_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()

async def stream_trades(symbols: list[str], on_tick):
    streams = "/".join([f"{s.lower()}@trade" for s in symbols])
    url = f"{BINANCE_WS}?streams={streams}"
    backoff = 1

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                backoff = 1
                async for msg in ws:
                    payload = json.loads(msg)
                    data = payload.get("data", payload)

                    # trade event schema (Binance)
                    # data: { E:eventTime, s:symbol, p:price, q:qty }
                    ts_ms = int(data.get("E") or int(time.time()*1000))
                    symbol = str(data.get("s") or "").lower()
                    price = float(data.get("p"))
                    size = float(data.get("q"))

                    await on_tick({
                        "ts_ms": ts_ms,
                        "ts_iso": iso_now_ms(ts_ms),
                        "symbol": symbol,
                        "price": price,
                        "size": size,
                    })
        except Exception as e:
            print(f"[WS] error: {e}. reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
