"""Индикаторы для искр (RSI, MFI, NATR, форма свечи, объём к медиане за 40 свечей).

Классика Уайлдера, как в TradingView: RSI(14) и ATR(14) сглаживаются RMA (первое значение — SMA
первых 14), MFI(14) — суммы денежного потока за 14 баров, денежный поток = типичная цена × объём в USDT.
Пропущенные бары из ряда выкидываются (индикатор идёт по имеющимся барам, как TradingView по своим свечам).
Все значения — на барах, закрытых до T (кроме RSI 15m на T+30).

Считается по журналу, как контекст: каждый проход берёт искры без ind_ms и досчитывает. Поэтому
после обновления движка все имеющиеся искры получают индикаторы сами, отдельный скрипт не нужен.
"""
import logging
import math

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, DAY_BARS, floor_bar, now_ms
from .sparks import M15, floor15, load_bars

log = logging.getLogger("ind")

HOUR = 12 * BAR_MS
H4 = 48 * BAR_MS
WEEK = 7 * DAY_BARS
WARM4 = 100                                # 4h-баров разгона RSI до начала 5m-окна (оно и так на неделю раньше искры)


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if math.isfinite(x) else None


# ================================================================ индикаторы
def _rma(x: np.ndarray, n: int) -> np.ndarray:
    out = np.full(len(x), np.nan)
    if len(x) < n:
        return out
    v = float(np.mean(x[:n]))
    out[n - 1] = v
    for k in range(n, len(x)):
        v += (x[k] - v) / n
        out[k] = v
    return out


def _compress(*arrs):
    ok = np.ones(len(arrs[0]), bool)
    for a in arrs:
        ok &= np.isfinite(a)
    idx = np.where(ok)[0]
    return idx, [a[idx] for a in arrs]


def rsi(c: np.ndarray, n: int = 14) -> np.ndarray:
    out = np.full(len(c), np.nan)
    idx, (cc,) = _compress(c)
    if len(cc) <= n:
        return out
    d = np.diff(cc)
    up = _rma(np.maximum(d, 0), n)
    dn = _rma(np.maximum(-d, 0), n)
    with np.errstate(all="ignore"):
        r = np.where(dn == 0, 100.0, np.where(up == 0, 0.0, 100 - 100 / (1 + up / dn)))
    r = np.where(np.isfinite(up) & np.isfinite(dn), r, np.nan)
    out[idx[1:]] = r
    return out


def atr(h: np.ndarray, l: np.ndarray, c: np.ndarray, n: int = 14) -> tuple[np.ndarray, np.ndarray]:
    """ATR (RMA) и true range, выровненные по исходным индексам."""
    a_out, tr_out = np.full(len(c), np.nan), np.full(len(c), np.nan)
    idx, (hh, ll, cc) = _compress(h, l, c)
    if len(cc) < 2:
        return a_out, tr_out
    tr = np.empty(len(cc))
    tr[0] = hh[0] - ll[0]
    tr[1:] = np.maximum(hh[1:] - ll[1:], np.maximum(np.abs(hh[1:] - cc[:-1]), np.abs(ll[1:] - cc[:-1])))
    tr_out[idx] = tr
    a_out[idx] = _rma(tr, n)
    return a_out, tr_out


def mfi(h, l, c, v, n: int = 14) -> np.ndarray:
    out = np.full(len(c), np.nan)
    idx, (hh, ll, cc, vv) = _compress(h, l, c, v)
    if len(cc) <= n:
        return out
    tp = (hh + ll + cc) / 3
    flow = tp * vv
    pos = np.where(np.diff(tp) > 0, flow[1:], 0.0)
    neg = np.where(np.diff(tp) < 0, flow[1:], 0.0)
    cp, cn = np.concatenate([[0], np.cumsum(pos)]), np.concatenate([[0], np.cumsum(neg)])
    r = np.full(len(cc), np.nan)
    for k in range(n, len(cc)):
        sp, sn = cp[k] - cp[k - n], cn[k] - cn[k - n]
        r[k] = 100.0 if sn == 0 else 100 - 100 / (1 + sp / sn)
    out[idx] = r
    return out


def agg(o, h, l, c, v, start: int, m: int) -> dict:
    """Бары по m штук 5m, начиная с индекса start. Бар считается, только если все m баров есть."""
    k = (len(c) - start) // m
    if k <= 0:
        e = np.array([])
        return dict(o=e, h=e, l=e, c=e, v=e)
    r = lambda a: a[start:start + k * m].reshape(k, m)
    full = np.isfinite(r(c)).all(1)
    with np.errstate(all="ignore"):
        res = dict(o=r(o)[:, 0], h=np.nanmax(r(h), 1), l=np.nanmin(r(l), 1), c=r(c)[:, -1], v=np.nansum(r(v), 1))
    return {key: np.where(full, val, np.nan) for key, val in res.items()}


def candle_shape(o, h, l, c) -> dict:
    """Максимум от открытия и закрытие от минимума (%), доли верхней и нижней тени (% от диапазона)."""
    if not all(np.isfinite(x) for x in (o, h, l, c)) or o <= 0 or l <= 0:
        return dict(hi_from_open=None, close_from_low=None, wick_up=None, wick_dn=None)
    rng = h - l
    return dict(hi_from_open=_n((h / o - 1) * 100), close_from_low=_n((c / l - 1) * 100),
                wick_up=_n((h - max(o, c)) / rng * 100) if rng > 0 else None,
                wick_dn=_n((min(o, c) - l) / rng * 100) if rng > 0 else None)


def _win(a: np.ndarray, lo: int, hi: int, min_n: int, fn):
    w = a[max(0, lo):hi]
    w = w[np.isfinite(w)]
    return fn(w) if len(w) >= min_n else np.nan


# ================================================================ расчёт для искры
IND15 = list(D.IND15_COLS)
CANDLE = list(D.CANDLE_COLS)
SQL_IND15 = D.update_sql("sparks_15m", IND15 + CANDLE + ["ind_ms"])
SQL_IND5 = D.update_sql("sparks_5m", CANDLE + ["ind_ms"])
SQL_IND15_CONF = D.update_sql("sparks_15m", ["rsi15_conf", "ind_conf_ms"])


class SymbolSeries:
    """5m-ряд монеты (t_from — граница 15m), 15m- и 1h-свечи, 4h из raw_klines_4h, индикаторы."""

    def __init__(self, conn, sym: str, t_from: int, t_to: int, p: dict):
        self.t_from = t_from
        self.o, self.h, self.l, self.c, self.qv, self.tbq = load_bars(conn, sym, t_from, t_to)
        n = int(p["rsi_period"])
        na = int(p["atr_period"])
        nm = int(p["mfi_period"])
        b = ref.agg15(self.o, self.h, self.l, self.c, self.qv, self.tbq)
        self.b15 = b
        self.rsi15 = rsi(b["c"], n)
        self.mfi15 = mfi(b["h"], b["l"], b["c"], b["q"], nm)
        self.atr15, self.tr15 = atr(b["h"], b["l"], b["c"], na)
        with np.errstate(all="ignore"):
            self.natr15 = self.atr15 / b["c"] * 100
        # 1h: с первой полной часовой границы
        k0 = ((HOUR - t_from % HOUR) % HOUR) // BAR_MS
        self.h1_t0 = t_from + k0 * BAR_MS
        b1 = agg(self.o, self.h, self.l, self.c, self.qv, k0, 12)
        self.rsi1h = rsi(b1["c"], n)
        self.mfi1h = mfi(b1["h"], b1["l"], b1["c"], b1["v"], nm)
        # 4h: из базы 4h-свечей, пропуски — из 5m, если они есть целиком
        self.h4_t0 = t_from - t_from % H4 - WARM4 * H4
        n4 = (t_to - self.h4_t0) // H4 + 1
        self.n4 = n4
        c4 = np.full(n4, np.nan)
        for t, cl in conn.execute("SELECT t, c FROM raw_klines_4h WHERE symbol=? AND t>=? AND t<?",
                                  (sym, self.h4_t0, self.h4_t0 + n4 * H4)):
            c4[(t - self.h4_t0) // H4] = cl
        for k in range(n4):
            if np.isfinite(c4[k]):
                continue
            a = (self.h4_t0 + k * H4 - t_from) // BAR_MS
            if a >= 0 and a + 48 <= len(self.c) and np.isfinite(self.c[a:a + 48]).all():
                c4[k] = self.c[a + 47]
        self.c4 = c4
        self.rsi4h = rsi(c4, n)

    def at_T(self, T: int, p: dict) -> dict:
        i = (T - self.t_from) // M15
        q = self.b15["q"]
        hh = (T - self.h1_t0) // HOUR - 1                  # последний час, закрытый не позже T
        k4 = (T - self.h4_t0) // H4 - 1                    # последний 4h-бар, закрытый не позже T
        r15 = self.rsi15[i - 1] if i >= 1 else np.nan
        r1min = _win(self.rsi1h, hh - 23, hh + 1, 20, np.min) if hh >= 0 else np.nan
        natr3 = _win(self.natr15, i - 288, i, 250, np.mean)
        natr_pre = self.natr15[i - 1] if i >= 1 else np.nan
        med40 = _win(q, i - 40, i, int(p["med40_min"]), np.median)
        hot, cold = float(p["rsi_hot"]), float(p["rsi_cold"])
        if np.isfinite(r15) or np.isfinite(r1min):
            extreme = int(bool((np.isfinite(r15) and r15 > hot) or (np.isfinite(r1min) and r1min < cold)))
        else:
            extreme = None
        a_pre = self.atr15[i - 1] if i >= 1 else np.nan
        return dict(
            rsi15_pre=_n(r15),
            rsi1h=_n(self.rsi1h[hh]) if 0 <= hh < len(self.rsi1h) else None,
            rsi1h_min24=_n(r1min),
            rsi4h=_n(self.rsi4h[k4]) if 0 <= k4 < self.n4 else None,
            rsi4h_min7=_n(_win(self.rsi4h, k4 - 41, k4 + 1, 36, np.min)),
            rsi4h_max7=_n(_win(self.rsi4h, k4 - 41, k4 + 1, 36, np.max)),
            rsi4h_min14=_n(_win(self.rsi4h, k4 - 83, k4 + 1, 72, np.min)),
            rsi4h_max14=_n(_win(self.rsi4h, k4 - 83, k4 + 1, 72, np.max)),
            mfi15_pre=_n(self.mfi15[i - 1]) if i >= 1 else None,
            mfi1h=_n(self.mfi1h[hh]) if 0 <= hh < len(self.mfi1h) else None,
            extreme=extreme,
            natr_pre=_n(natr_pre),
            natr_1d=_n(_win(self.natr15, i - 96, i, 80, np.mean)),
            natr_3d=_n(natr3),
            natr_ratio=_n(natr_pre / natr3) if np.isfinite(natr_pre) and np.isfinite(natr3) and natr3 > 0 else None,
            spark_atr=_n(self.tr15[i] / a_pre) if np.isfinite(a_pre) and a_pre > 0 else None,
            relvol_med40=_n(q[i] / med40) if np.isfinite(med40) and med40 > 0 else None,
        ) | candle_shape(self.b15["o"][i], self.b15["h"][i], self.b15["l"][i], self.b15["c"][i])


class IndicatorBuilder:
    def __init__(self, cfg: dict, db: D.Db):
        self.p = cfg.get("indicators", {})
        for k, v in dict(rsi_period=14, atr_period=14, mfi_period=14, rsi_hot=70, rsi_cold=30, med40_min=30,
                         conf_wait_min=10).items():
            self.p.setdefault(k, v)
        self.db = db
        t = cfg.get("trade", {})
        self.u = (float(t.get("u_ask2_max", 150_000)), float(t.get("u_oi_min", 1_000_000)), float(t.get("u_oi_q24", 0.4)))

    async def process(self) -> None:
        now = now_ms()
        b = floor_bar(now)
        wait = int(float(self.p["conf_wait_min"]) * 60_000)
        p = self.p

        def job(conn):
            p15 = conn.execute(
                "SELECT symbol, t, ind_ms FROM sparks_15m WHERE (ind_ms IS NULL AND t+?<=?) "
                "OR (ind_ms IS NOT NULL AND ind_conf_ms IS NULL AND t+?<=?)", (M15, b, 2 * M15, b)).fetchall()
            p5 = conn.execute("SELECT symbol, t FROM sparks_5m WHERE ind_ms IS NULL AND t+?<=?", (BAR_MS, b)).fetchall()
            by: dict[str, list] = {}
            for s, t, ind in p15:
                by.setdefault(s, []).append(("15m", t, ind))
            for s, t in p5:
                by.setdefault(s, []).append(("5m", t, None))
            o15, o5, oc = [], [], []
            for sym, items in by.items():
                tmin = min(t for _, t, _ in items)
                tmax = max(t for _, t, _ in items)
                t_from = floor15(tmin - (WEEK + 3) * BAR_MS)
                ss = SymbolSeries(conn, sym, t_from, min(b, tmax + 2 * M15), p)
                for kind, T, ind in items:
                    if kind == "5m":
                        j = (T - t_from) // BAR_MS
                        sh = candle_shape(ss.o[j], ss.h[j], ss.l[j], ss.c[j])
                        o5.append([sh[k] for k in CANDLE] + [now, sym, T])
                        continue
                    if ind is None:
                        v = ss.at_T(T, p)
                        o15.append([v[k] for k in IND15] + [v[k] for k in CANDLE] + [now, sym, T])
                    # RSI 15m на T+30 — на свече T+15…T+30
                    i = (T - t_from) // M15 + 1
                    if T + 2 * M15 <= b and i < len(ss.rsi15):
                        val = ss.rsi15[i]
                        if np.isfinite(val) or now > T + 2 * M15 + wait:
                            oc.append([_n(val), now, sym, T])
            return o15, o5, oc

        o15, o5, oc = await self.db.run_in_thread(job)
        self.db.put(SQL_IND15, o15)
        self.db.put(SQL_IND5, o5)
        self.db.put(SQL_IND15_CONF, oc)
        # Правило D = A + «крайности» (RSI 15m > 70 перед искрой или RSI 1h за сутки < 30): пересчёт при любом
        # изменении A или индикаторов (починка подхвата, пересчёт «первой за 12 ч»).
        d, dm = "(COALESCE(rA,0)=1 AND COALESCE(extreme,0)=1)", "(COALESCE(rA_mean,0)=1 AND COALESCE(extreme,0)=1)"
        self.db.put(f"UPDATE sparks_15m SET rD={d}, rD_mean={dm} WHERE conf_ms IS NOT NULL AND ind_ms IS NOT NULL "
                    f"AND (rD IS NULL OR rD_mean IS NULL OR rD!={d} OR rD_mean!={dm})", [()])
        # Счёт U: OI в $ на T (у старых искр — досчитать из oi_t × open) и флаг rU.
        self.db.put("UPDATE sparks_15m SET oi_usd_t = oi_t * o WHERE oi_usd_t IS NULL AND oi_t IS NOT NULL", [()])
        u = self.u
        ru = (f"(COALESCE(rA_mean,0)=1 AND ask2_med IS NOT NULL AND ask2_med < {u[0]} AND oi_usd_t IS NOT NULL "
              f"AND oi_usd_t >= {u[1]} AND q24 IS NOT NULL AND oi_usd_t >= {u[2]} * q24 * 1000000)")
        self.db.put(f"UPDATE sparks_15m SET rU={ru} WHERE conf_ms IS NOT NULL AND (rU IS NULL OR rU!={ru})", [()])
        if o15 or o5:
            log.info("индикаторы: 15m — %d, 5m — %d, RSI на T+30 — %d", len(o15), len(o5), len(oc))
