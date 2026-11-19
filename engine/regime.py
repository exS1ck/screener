"""Режим рынка v0.4 — функциями эталона (engine/ref04.py).

На каждом закрытом 5m-баре по всем монетам с данными: вселенная (оборот 24 ч $0.5–30M), медиана 5m-лог-доходностей
по вселенной (med), доля монет выше, чем 24 ч назад (breadth24), BTC за 24 ч (btc24). Это ровно то, что считает
ref04.market_regime; чтобы не держать в памяти всю историю, расчёт идёт кусками с перекрытием 300 баров
(вселенной нужно 288 баров истории, доходности — 1), а mkt24 / mkt72 = exp(сумма med за 288 / 864 бара) − 1 —
то же, что exp(idx[k] − idx[k − 288]) − 1 в эталоне. Режим — по порогам эталона.

Результат — таблица market_regime; затем теги режима проставляются искрам (бар, закрывшийся в T) и сделкам
(бар, закрывшийся перед входом) — в том числе задним числом, по всей истории.
"""
import logging
import warnings

import numpy as np

from . import db as D
from . import ref04
from .clock import BAR_MS, now_ms

log = logging.getLogger("regime")
OVERLAP = 300
CHUNK = 3000


def load_matrix(conn, t_from: int, t_to: int):
    """5m-бары [t_from, t_to) всех монет: close, qv — матрицы (бары × монеты) и список монет."""
    n = (t_to - t_from) // BAR_MS
    syms = [r[0] for r in conn.execute("SELECT DISTINCT symbol FROM raw_klines_5m WHERE t>=? AND t<?", (t_from, t_to))]
    if "BTCUSDT" not in syms:
        syms.append("BTCUSDT")
    col = {s: i for i, s in enumerate(syms)}
    C = np.full((n, len(syms)), np.nan)
    Q = np.full((n, len(syms)), np.nan)
    for s, t, c, q in conn.execute("SELECT symbol, t, c, qv FROM raw_klines_5m WHERE t>=? AND t<?", (t_from, t_to)):
        k = (t - t_from) // BAR_MS
        C[k, col[s]] = c
        Q[k, col[s]] = q
    return C, Q, syms, col["BTCUSDT"]


def label(m24, m72) -> str:
    if m24 is None or m72 is None or not (np.isfinite(m24) and np.isfinite(m72)):
        return ""
    if m24 >= 0 and m72 >= -1:
        return "рост"
    if m24 < -1 or m72 < -2.5:
        return "падение"
    return "нейтрально"


def compute_base(conn, t_from: int, t_to: int) -> list:
    """med, breadth24, btc24, размер вселенной для баров [t_from, t_to) — кусками с перекрытием."""
    out = []
    cs = t_from
    while cs < t_to:
        ce = min(t_to, cs + CHUNK * BAR_MS)
        lo = cs - OVERLAP * BAR_MS
        C, Q, syms, btc = load_matrix(conn, lo, ce)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ref04.market_regime(C, Q, btc)
            uni, _ = ref04.universe_mask(Q)
        idx = df["idx"].values
        med = np.diff(idx, prepend=0.0)
        for k in range(OVERLAP, len(idx)):
            b24, bt = df["breadth24"].values[k], df["btc24"].values[k]
            out.append((lo + k * BAR_MS, float(med[k]), None if not np.isfinite(b24) else float(b24),
                        None if not np.isfinite(bt) else float(bt), int(uni[k].sum())))
        cs = ce
    return out


class MarketRegime:
    def __init__(self, cfg: dict, db: D.Db, col):
        self.db = db
        self.col = col
        self.ready = False

    async def update(self, b: int) -> None:
        """Досчитать режим для всех закрытых баров до b (t < b) и проставить теги."""
        await self.db.flush()

        def job(conn):
            last = conn.execute("SELECT MAX(t) FROM market_regime").fetchone()[0]
            first = conn.execute("SELECT MIN(t) FROM raw_klines_5m").fetchone()[0]
            if first is None:
                return 0
            start = (last + BAR_MS) if last is not None else first
            start = max(start, first)
            if start >= b:
                return 0
            base = compute_base(conn, start, b)
            # mkt24 / mkt72 по накопленным med (предыдущие 863 бара — из таблицы)
            prev = conn.execute("SELECT t, med FROM market_regime WHERE t>=? ORDER BY t",
                                (start - 864 * BAR_MS,)).fetchall()
            series = [(t, m) for t, m in prev] + [(r[0], r[1]) for r in base]
            ts = np.array([t for t, _ in series], dtype=np.int64)
            ms = np.array([m for _, m in series], dtype=float)
            first_bar = first
            cum = np.concatenate([[0.0], np.cumsum(ms)])
            rows = []
            off = len(prev)
            for i, (t, med, br, bt, nu) in enumerate(base):
                j = off + i
                bars_since = (t - first_bar) // BAR_MS          # индекс бара от начала данных, как k в эталоне
                m24 = m72 = None
                if bars_since >= 288 and j >= 288 and ts[j] - ts[j - 288] == 288 * BAR_MS:
                    m24 = float((np.exp(cum[j + 1] - cum[j + 1 - 288]) - 1) * 100)
                if bars_since >= 864 and j >= 864 and ts[j] - ts[j - 864] == 864 * BAR_MS:
                    m72 = float((np.exp(cum[j + 1] - cum[j + 1 - 864]) - 1) * 100)
                rows.append((t, med, m24, m72, br, bt, label(m24, m72), nu, now_ms()))
            conn.executemany("INSERT OR REPLACE INTO market_regime(t, med, mkt24, mkt72, breadth24, btc24, regime, "
                             "n_univ, ms) VALUES(?,?,?,?,?,?,?,?,?)", rows)
            conn.commit()
            return len(rows)

        n = await self.db.run_in_thread(job)
        if n > 1:
            log.info("режим рынка: досчитано баров %d", n)
        await self.tag()
        self.ready = True

    async def tag(self) -> None:
        """Теги режима: искрам — бар, закрывшийся в T; сделкам — бар, закрывшийся перед входом."""
        sets = ("reg_t=r.t, regime=r.regime, reg_mkt24=r.mkt24, reg_mkt72=r.mkt72, reg_breadth=r.breadth24, "
                "reg_btc24=r.btc24")

        def job(conn):
            n = 0
            for tbl, expr in (("sparks_15m", "sparks_15m.t - 300000"), ("sparks_5m", "sparks_5m.t - 300000"),
                              ("paper_trades", "(paper_trades.entry_ms / 300000) * 300000 - 300000"),
                              ("live_trades", "(live_trades.entry_ms / 300000) * 300000 - 300000")):
                n += conn.execute(f"UPDATE {tbl} SET {sets} FROM market_regime r WHERE {tbl}.reg_t IS NULL "
                                  f"AND r.t = {expr}").rowcount
            conn.commit()
            return n

        n = await self.db.run_in_thread(job)
        if n > 20:
            log.info("режим рынка: теги проставлены у %d записей", n)

    async def current(self) -> dict:
        r = await self.db.read("SELECT t, mkt24, mkt72, breadth24, btc24, regime FROM market_regime "
                               "ORDER BY t DESC LIMIT 1")
        if not r:
            return {}
        t, m24, m72, br, bt, rg = r[0]
        return dict(t=t, mkt24=m24, mkt72=m72, breadth24=br, btc24=bt, regime=rg)
