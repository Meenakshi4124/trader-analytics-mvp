import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path("data.db")

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("""
        CREATE TABLE IF NOT EXISTS ticks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms INTEGER NOT NULL,
            ts_iso TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            size REAL NOT NULL
        );
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts_ms);")

        con.execute("""
        CREATE TABLE IF NOT EXISTS alert_rules(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            symbol_a TEXT NOT NULL,
            symbol_b TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            window INTEGER NOT NULL,
            threshold REAL NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS alert_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            ts_ms INTEGER NOT NULL,
            message TEXT NOT NULL
        );
        """)
        con.commit()

@contextmanager
def connect():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        yield con
    finally:
        con.close()

def insert_tick(ts_ms: int, ts_iso: str, symbol: str, price: float, size: float):
    with connect() as con:
        con.execute(
            "INSERT INTO ticks(ts_ms, ts_iso, symbol, price, size) VALUES (?,?,?,?,?)",
            (ts_ms, ts_iso, symbol, price, size),
        )
        con.commit()

def read_ticks(symbol: str, since_ms: int):
    with connect() as con:
        cur = con.execute(
            "SELECT ts_ms, ts_iso, price, size FROM ticks WHERE symbol=? AND ts_ms>=? ORDER BY ts_ms ASC",
            (symbol, since_ms),
        )
        return cur.fetchall()

def list_symbols(limit: int = 50):
    with connect() as con:
        cur = con.execute("SELECT DISTINCT symbol FROM ticks ORDER BY symbol LIMIT ?", (limit,))
        return [r[0] for r in cur.fetchall()]

def upsert_alert_rule(name, a, b, tf, window, threshold, enabled=True):
    with connect() as con:
        cur = con.execute("""
            INSERT INTO alert_rules(name, symbol_a, symbol_b, timeframe, window, threshold, enabled)
            VALUES (?,?,?,?,?,?,?)
        """, (name, a, b, tf, int(window), float(threshold), 1 if enabled else 0))
        con.commit()
        return cur.lastrowid

def get_alert_rules():
    with connect() as con:
        cur = con.execute("SELECT id, name, symbol_a, symbol_b, timeframe, window, threshold, enabled FROM alert_rules")
        return cur.fetchall()

def log_alert_event(rule_id: int, ts_ms: int, message: str):
    with connect() as con:
        con.execute("INSERT INTO alert_events(rule_id, ts_ms, message) VALUES (?,?,?)", (rule_id, ts_ms, message))
        con.commit()

def get_alert_events(limit: int = 200):
    with connect() as con:
        cur = con.execute("""
            SELECT e.id, e.ts_ms, e.rule_id, r.name, e.message
            FROM alert_events e
            JOIN alert_rules r ON r.id = e.rule_id
            ORDER BY e.id DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()
