"""Единая база истории («озеро»): архив Binance + данные движка.

Слои:
  • lake/<набор>/<ГГГГ-ММ>.parquet       — архив Binance, по месяцу на набор (все монеты в одном файле);
  • lake/live/<набор>/<ГГГГ-ММ-ДД>.parquet — законченные дни из базы движка (раз в сутки, engine/lake_export.py);
  • база движка (SQLite)                  — самые свежие данные, подключается только на чтение.
open_lake() отдаёт DuckDB-соединение с едиными представлениями klines_5m, klines_1m, metrics_5m, funding,
book_depth_5m: старое берётся из архива, дальше — из дневных файлов движка, самое свежее — прямо из его базы.
Соединение в памяти: файлы только читаются, поэтому одновременно работают движок, веб и любые инструменты.

Договорённости — как в движке и эталонах: время в мс UTC; t у свечей — открытие бара;
у метрик и стакана значение «на баре t» — на конец бара t (create_time архива = t).
"""
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

BAR = 300_000

SCHEMAS = {
    "klines_5m": pa.schema([("symbol", pa.string()), ("t", pa.int64()), ("o", pa.float64()), ("h", pa.float64()),
                            ("l", pa.float64()), ("c", pa.float64()), ("v", pa.float64()), ("qv", pa.float64()),
                            ("n", pa.int64()), ("tbv", pa.float64()), ("tbq", pa.float64())]),
    "funding": pa.schema([("symbol", pa.string()), ("t", pa.int64()), ("interval_h", pa.float64()),
                          ("rate", pa.float64())]),
    "metrics_5m": pa.schema([("symbol", pa.string()), ("t", pa.int64()), ("oi", pa.float64()), ("oi_usd", pa.float64()),
                             ("top_acc", pa.float64()), ("top_pos", pa.float64()), ("ls_global", pa.float64()),
                             ("taker_ratio", pa.float64())]),
    "book_depth_5m": pa.schema([("symbol", pa.string()), ("t", pa.int64()), ("ts", pa.int64())]
                               + [(f"{s}{p}", pa.float64()) for s in ("a", "b") for p in (1, 2, 3, 4, 5)]),
}
SCHEMAS["klines_1m"] = SCHEMAS["klines_5m"]

# имя папки набора в архиве → набор озера
FOLDER = {"klines_5m": "klines_5m", "klines_1m": "klines_1m", "fundingrate": "funding", "funding": "funding",
          "funding_rate": "funding", "metrics": "metrics_5m", "bookdepth": "book_depth_5m", "book_depth": "book_depth_5m"}


def dataset_of(path: Path) -> str | None:
    """Набор по папке (klines_5m, metrics, bookDepth, fundingRate…), иначе — по имени файла Binance."""
    d = FOLDER.get(path.parent.name.lower())
    if d:
        return d
    name = path.name
    for pat, ds in (("-bookDepth-", "book_depth_5m"), ("-metrics-", "metrics_5m"), ("-fundingRate-", "funding"),
                    ("-5m-", "klines_5m"), ("-1m-", "klines_1m")):
        if pat in name:
            return ds
    return None


def _first_line(zf: zipfile.ZipFile, member: str) -> str:
    with zf.open(member) as f:
        return f.readline().decode("utf-8", "replace").strip()


def _read_csv(zf: zipfile.ZipFile, member: str, names: list[str]) -> pd.DataFrame:
    head = _first_line(zf, member)
    has_header = bool(head) and not head.split(",")[0].strip().replace(".", "").isdigit()
    with zf.open(member) as f:
        return pd.read_csv(f, header=0 if has_header else None, names=names, usecols=range(len(names)),
                           skiprows=0, low_memory=False)


def _ms(x: pd.Series) -> pd.Series:
    """Время в мс: числа (мс или мкс) или строки «ГГГГ-ММ-ДД ЧЧ:ММ:СС» в UTC."""
    if pd.api.types.is_numeric_dtype(x):
        v = x.astype("int64")
        return pd.Series(np.where(v > 10 ** 14, v // 1000, v), index=x.index)
    dt = pd.to_datetime(x, utc=True, format="mixed")
    # разрешение datetime у pandas бывает разным (нс, мкс, с) — считаем через разницу с эпохой
    return ((dt - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(milliseconds=1)).astype("int64")


def parse_zip(path: Path, symbol: str, dataset: str) -> pd.DataFrame:
    """Один zip архива Binance → таблица в схеме набора озера."""
    with zipfile.ZipFile(path) as zf:
        member = next((m for m in zf.namelist() if m.lower().endswith(".csv")), None)
        if member is None:
            return pd.DataFrame(columns=SCHEMAS[dataset].names)
        if dataset in ("klines_5m", "klines_1m"):
            df = _read_csv(zf, member, ["open_time", "open", "high", "low", "close", "volume", "close_time",
                                        "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume"])
            out = pd.DataFrame({"symbol": symbol, "t": _ms(df["open_time"]), "o": df["open"], "h": df["high"],
                                "l": df["low"], "c": df["close"], "v": df["volume"], "qv": df["quote_volume"],
                                "n": df["count"].fillna(0).astype("int64"), "tbv": df["taker_buy_volume"],
                                "tbq": df["taker_buy_quote_volume"]})
        elif dataset == "funding":
            df = _read_csv(zf, member, ["calc_time", "funding_interval_hours", "last_funding_rate"])
            out = pd.DataFrame({"symbol": symbol, "t": _ms(df["calc_time"]), "interval_h": df["funding_interval_hours"],
                                "rate": df["last_funding_rate"]})
        elif dataset == "metrics_5m":
            df = _read_csv(zf, member, ["create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
                                        "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
                                        "count_long_short_ratio", "sum_taker_long_short_vol_ratio"])
            out = pd.DataFrame({"symbol": symbol, "t": _ms(df["create_time"]), "oi": df["sum_open_interest"],
                                "oi_usd": df["sum_open_interest_value"],
                                "top_acc": df["count_toptrader_long_short_ratio"],
                                "top_pos": df["sum_toptrader_long_short_ratio"],
                                "ls_global": df["count_long_short_ratio"],
                                "taker_ratio": df["sum_taker_long_short_vol_ratio"]})
        elif dataset == "book_depth_5m":
            df = _read_csv(zf, member, ["timestamp", "percentage", "depth", "notional"])
            out = depth_to_5m(symbol, df)
        else:
            raise ValueError(dataset)
    for col, typ in zip(SCHEMAS[dataset].names, SCHEMAS[dataset].types):
        if col not in out.columns:
            out[col] = np.nan
        elif pa.types.is_floating(typ):
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    return out[SCHEMAS[dataset].names].dropna(subset=["t"])


def depth_to_5m(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    """Стакан bookDepth (накопленный объём в $ на уровнях ±1…±5% от цены) → последний снимок в каждом 5m-баре:
    a1…a5 — офферы до +1…+5%, b1…b5 — биды до −1…−5%, в $. Уровни дальше ±5% в архиве нет."""
    if df.empty:
        return pd.DataFrame(columns=SCHEMAS["book_depth_5m"].names)
    ts = _ms(df["timestamp"])
    pct = pd.to_numeric(df["percentage"], errors="coerce").round(2)
    d = pd.DataFrame({"ts": ts, "pct": pct, "notional": pd.to_numeric(df["notional"], errors="coerce")})
    d = d[d["pct"].isin([-5, -4, -3, -2, -1, 1, 2, 3, 4, 5])]
    if d.empty:
        return pd.DataFrame(columns=SCHEMAS["book_depth_5m"].names)
    d["t"] = d["ts"] // BAR * BAR
    last = d.groupby("t")["ts"].transform("max")
    d = d[d["ts"] == last]
    w = d.pivot_table(index=["t", "ts"], columns="pct", values="notional", aggfunc="last").reset_index()
    out = pd.DataFrame({"symbol": symbol, "t": w["t"].astype("int64"), "ts": w["ts"].astype("int64")})
    for p in (1, 2, 3, 4, 5):
        out[f"a{p}"] = w[p] if p in w.columns else np.nan
        out[f"b{p}"] = w[-p] if -p in w.columns else np.nan
    return out


# ================================================================ единые представления
def _paths(cfg: dict) -> tuple[Path, Path]:
    lk = cfg.get("lake", {})
    root = cfg["_root"]
    lake = Path(lk.get("lake_dir", "Bulk/lake"))
    arch = Path(lk.get("archive_dir", "Bulk/history"))
    return (lake if lake.is_absolute() else root / lake), (arch if arch.is_absolute() else root / arch)


def lake_dir(cfg: dict) -> Path:
    return _paths(cfg)[0]


def archive_dir(cfg: dict) -> Path:
    return _paths(cfg)[1]


ENGINE_SQL = {
    "klines_5m": "SELECT symbol, t, o, h, l, c, v, qv, n, tbv, tbq FROM eng.raw_klines_5m",
    "klines_1m": "SELECT symbol, t, o, h, l, c, NULL::DOUBLE AS v, qv, NULL::BIGINT AS n, NULL::DOUBLE AS tbv, "
                 "NULL::DOUBLE AS tbq FROM eng.raw_klines_1m",
    "metrics_5m": "SELECT o.symbol, o.t, COALESCE(o.oi_hist, o.oi_live) AS oi, o.oi_hist_usd AS oi_usd, "
                  "ta.ratio AS top_acc, tp.ratio AS top_pos, g.ratio AS ls_global, NULL::DOUBLE AS taker_ratio "
                  "FROM eng.raw_oi o "
                  "LEFT JOIN eng.raw_ls g ON g.symbol=o.symbol AND g.t=o.t AND g.kind='global' "
                  "LEFT JOIN eng.raw_ls ta ON ta.symbol=o.symbol AND ta.t=o.t AND ta.kind='top_acc' "
                  "LEFT JOIN eng.raw_ls tp ON tp.symbol=o.symbol AND tp.t=o.t AND tp.kind='top_pos'",
}


# ================================================================ Bybit / OKX
# lake/<биржа>/<набор>/ГГГГ-ММ.parquet (история, csv_to_lake.py)
# + lake/live/<биржа>/<набор>/ГГГГ-ММ-ДД.parquet (дни из движка).
# Схемы как у Binance: funding — symbol, t, interval_h, rate; oi_* — symbol, t, oi (монеты), oi_usd.
XSCHEMA_OI = pa.schema([("symbol", pa.string()), ("t", pa.int64()),
                        ("oi", pa.float64()), ("oi_usd", pa.float64())])
XSETS = {
    "bybit": {"funding": SCHEMAS["funding"], "oi_5m": XSCHEMA_OI, "oi_15m": XSCHEMA_OI},
    "okx":   {"funding": SCHEMAS["funding"], "oi_5m": XSCHEMA_OI, "oi_1h": XSCHEMA_OI, "oi_1d": XSCHEMA_OI},
}
XENGINE_SQL: dict = {}   # (биржа, набор) -> SQL к базе движка; заполним, когда движок начнёт собирать


def _layered(con, names, dirs, engine_sql=None) -> list[str]:
    """Части UNION: каждый следующий слой берётся только после последнего t предыдущего."""
    parts, cut = [], None
    cols = ", ".join(names)
    for sub in dirs:
        files = sorted(sub.glob("*.parquet")) if sub.exists() else []
        if not files:
            continue
        src = f"read_parquet('{(sub / '*.parquet').as_posix()}', union_by_name=true)"
        parts.append(f"SELECT {cols} FROM {src}" + (f" WHERE t > {cut}" if cut else ""))
        mx = con.execute(f"SELECT MAX(t) FROM read_parquet('{files[-1].as_posix()}')").fetchone()[0]
        cut = max(cut or 0, mx or 0) or cut
    if engine_sql:
        try:
            con.execute(f"SELECT 1 FROM ({engine_sql}) LIMIT 1")
            parts.append(f"SELECT * FROM ({engine_sql})" + (f" WHERE t > {cut}" if cut else ""))
        except Exception:                                     # noqa: BLE001
            pass
    return parts


def _cross_views(con, xviews: dict) -> None:
    """symbol_map, funding_all, oi_5m_all — сводные таблицы по всем биржам."""
    fund = [("binance", "funding")] + [(ex, xviews[(ex, "funding")]) for ex in XSETS if (ex, "funding") in xviews]
    sel = " UNION ALL ".join(f"SELECT DISTINCT '{ex}' AS exchange, symbol FROM {v}" for ex, v in fund)
    # core — тикер без множителя (1000PEPEUSDT, SHIB1000USDT -> PEPEUSDT, SHIBUSDT);
    # mult — сколько монет в одной единице тикера; bsymbol — такой же тикер на Binance
    con.execute(f"""
        CREATE OR REPLACE TABLE symbol_map AS
        WITH s AS ({sel}),
        p AS (
            SELECT exchange, symbol,
                   regexp_extract(symbol, '^(1000000|10000|1000|1M)[A-Z]', 1) AS pre,
                   regexp_extract(symbol, '[A-Z](1000)USDT$', 1) AS suf,
                   regexp_replace(regexp_replace(symbol, '^(1000000|10000|1000|1M)([A-Z])', '\\2'),
                                  '([A-Z])1000USDT$', '\\1USDT') AS core
            FROM s WHERE symbol IS NOT NULL
        ),
        b AS (SELECT symbol, core FROM p WHERE exchange = 'binance')
        SELECT p.exchange, p.symbol, p.core,
               CASE p.pre WHEN '1000000' THEN 1e6 WHEN '1M' THEN 1e6 WHEN '10000' THEN 1e4
                          WHEN '1000' THEN 1e3
                          ELSE CASE WHEN p.suf = '1000' THEN 1e3 ELSE 1 END END AS mult,
               COALESCE(MAX(CASE WHEN b.symbol = p.symbol THEN b.symbol END), MIN(b.symbol)) AS bsymbol
        FROM p LEFT JOIN b ON b.core = p.core
        GROUP BY p.exchange, p.symbol, p.core, p.pre, p.suf
    """)
    parts = [f"SELECT '{ex}' AS exchange, f.symbol, m.bsymbol, (f.t // 60000) * 60000 AS t, "
             f"f.interval_h, f.rate FROM {v} f "
             f"LEFT JOIN symbol_map m ON m.exchange = '{ex}' AND m.symbol = f.symbol" for ex, v in fund]
    con.execute("CREATE OR REPLACE VIEW funding_all AS " + " UNION ALL ".join(parts))

    oi = [("binance", "metrics_5m")] + [(ex, xviews[(ex, "oi_5m")]) for ex in XSETS if (ex, "oi_5m") in xviews]
    parts = [f"SELECT '{ex}' AS exchange, o.symbol, m.bsymbol, o.t, o.oi * COALESCE(m.mult, 1) AS oi_coins, "
             f"o.oi_usd FROM {v} o "
             f"LEFT JOIN symbol_map m ON m.exchange = '{ex}' AND m.symbol = o.symbol" for ex, v in oi]
    con.execute("CREATE OR REPLACE VIEW oi_5m_all AS " + " UNION ALL ".join(parts))


def open_lake(cfg: dict, engine_db: Path | None = None):
    """DuckDB в памяти с представлениями klines_5m, klines_1m, metrics_5m, funding, book_depth_5m,
    наборами Bybit/OKX (bybit_funding, okx_oi_1h, …) и сводными symbol_map, funding_all, oi_5m_all.
    Граница слоёв — по времени: архив до своего последнего бара, дальше дневные файлы движка, дальше его база."""
    import duckdb
    import logging
    lake = lake_dir(cfg)
    con = duckdb.connect()
    if engine_db is not None and Path(engine_db).exists():
        try:
            con.execute("INSTALL sqlite; LOAD sqlite;")
            con.execute(f"ATTACH '{Path(engine_db).as_posix()}' AS eng (TYPE sqlite, READ_ONLY)")
        except Exception:                                     # noqa: BLE001
            engine_db = None

    # Binance — как раньше
    for ds in ("klines_5m", "klines_1m", "metrics_5m", "funding", "book_depth_5m"):
        parts = _layered(con, SCHEMAS[ds].names, (lake / ds, lake / "live" / ds),
                         ENGINE_SQL.get(ds) if engine_db is not None else None)
        if parts:
            con.execute(f"CREATE OR REPLACE VIEW {ds} AS " + " UNION ALL ".join(parts))
        else:
            cols = ", ".join(f"NULL::{'VARCHAR' if n == 'symbol' else 'BIGINT' if n in ('t', 'n', 'ts') else 'DOUBLE'} "
                             f"AS {n}" for n in SCHEMAS[ds].names)
            con.execute(f"CREATE OR REPLACE VIEW {ds} AS SELECT {cols} WHERE false")

    # Bybit / OKX — представление создаётся, только если есть данные
    xviews = {}
    for ex, sets in XSETS.items():
        for ds, schema in sets.items():
            parts = _layered(con, schema.names, (lake / ex / ds, lake / "live" / ex / ds),
                             XENGINE_SQL.get((ex, ds)) if engine_db is not None else None)
            if parts:
                name = f"{ex}_{ds}"
                con.execute(f"CREATE OR REPLACE VIEW {name} AS " + " UNION ALL ".join(parts))
                xviews[(ex, ds)] = name
    try:
        _cross_views(con, xviews)
    except Exception:                                         # noqa: BLE001
        logging.getLogger("lake").warning("сводные представления по биржам не созданы", exc_info=True)
    return con


def month_of(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m")
