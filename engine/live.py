"""Этап 6: живые теги — стадия движения, пять красных флагов, маркеры выхода.

bar  — на закрытии каждого 5m-бара для искр под наблюдением (от T+30 до T+watch_hours),
       ровно как в исследовании: ref.stage_flags на закрытых барах.
fast — раз в fast_sec для монет «в фокусе»: к закрытым барам добавляется текущий незакрытый
       бар из WS и последний живой OI. Пороги для этого режима проверим по журналу.
L/S публикуется с задержкой, поэтому в живом режиме берётся последняя опубликованная точка
(ls_age_min показывает, насколько она старая). В исследовании было ls[t] на конец бара.
"""
import asyncio
import logging
import math

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, floor_bar, fmt, now_ms
from .context import Series
from .sparks import M15

log = logging.getLogger("live")

FLAG_BITS = (("ext", 1), ("ls", 2), ("oi", 4), ("vol", 8), ("chase", 16))


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if math.isfinite(x) else None


def ffill_tail(a: np.ndarray, start: int) -> tuple[np.ndarray, int | None]:
    """Протянуть последнее известное значение на хвост массива (с позиции start).
    Возвращает массив и индекс последней настоящей точки."""
    a = a.copy()
    last = None
    for k in range(max(0, start), len(a)):
        if np.isfinite(a[k]):
            last = k
        elif last is not None:
            a[k] = a[last]
    return a, last


class LiveTags:
    def __init__(self, cfg: dict, db: D.Db, col, met):
        self.x = cfg["live"]
        self.watch_ms = int(float(cfg["metrics"]["watch_hours"]) * 3600_000)
        self.db = db
        self.col = col
        self.met = met
        self.prev_exit: dict[tuple[str, int], tuple[int, int]] = {}

    # ---------------------------------------------------------------- расчёт одной точки
    def close_trail(self, T: int, h, l, c, last: int) -> list:
        """Выход v0.2 для ручной сделки от входа T+30 (E = close бара T+25…T+30, S = минимум искры − 0.2%·E):
        трейлинг по закрытию 5m (8%, после +15% — 5%), стоп по минимуму бара, проверка 6 ч (+1.5%).
        Бары j+6 … last — закрытые после входа. Возвращает [уровень, p, причина, момент, цена, 6 ч]."""
        j = 3
        if last < j + 6 or not np.isfinite(c[j + 5]):
            return [None] * 6
        E = c[j + 5]
        L = np.nanmin(l[j:j + 3])
        S = L - 0.002 * E
        p1, p2, sw = float(self.x.get("close_p1", 0.08)), float(self.x.get("close_p2", 0.05)), \
            1 + float(self.x.get("close_switch", 0.15))
        mclose, mhigh, ex = None, E, None
        for k in range(j + 6, last + 1):
            if np.isfinite(h[k]):
                mhigh = max(mhigh, h[k])
            if np.isfinite(l[k]) and l[k] <= S:
                ex = ("стоп", k, S)
                break
            if not np.isfinite(c[k]):
                continue
            pk = p2 if mhigh >= E * sw else p1
            if mclose is None:
                mclose = c[k]
            elif c[k] <= mclose * (1 - pk):
                ex = ("трейлинг", k, c[k])
                break
            else:
                mclose = max(mclose, c[k])
        p_now = p2 if mhigh >= E * sw else p1
        level = mclose * (1 - p_now) if mclose else S
        k6 = j + 5 + 72                                   # бар, закрывающийся через 360 мин после входа
        six = None
        if last >= k6 and np.isfinite(c[k6]) and (ex is None or ex[1] > k6):
            six = int(c[k6] >= E * (1 + float(self.x.get("six_gain", 0.015))))
        t0 = T - M15
        return [_n(level), p_now, ex[0] if ex else None, t0 + (ex[1] + 1) * BAR_MS if ex else None,
                _n(ex[2]) if ex else None, six]

    def compute(self, sym: str, T: int, o15: float, o, h, c, qv, oi, ls, mode: str, t_ms: int,
                l=None) -> list | None:
        j = 3                                            # массивы начинаются за 15 мин до T
        t = len(c) - 1
        if t < j + 5 or not (o15 and np.isfinite(o15)) or not np.isfinite(c[t]):
            return None
        oi_f, _ = ffill_tail(oi, j - 1)
        ls_f, last_ls = ffill_tail(ls, j - 1)
        if not np.isfinite(ls_f[j - 1]) or not np.isfinite(oi_f[j - 1]):
            return None                                  # нет L/S или OI на момент T — флаги не посчитать
        f = ref.stage_flags(t, j, o15, o, c, qv, oi_f, ls_f)
        max_gain = (np.nanmax(h[j:t + 1]) / o15 - 1) * 100
        reached = max_gain >= float(self.x["exit_after_gain"])
        exit_oi = int(bool(reached and np.isfinite(f["oi_from_max"])
                           and f["oi_from_max"] <= float(self.x["exit_oi_drop"])))
        exit_ls = int(bool(reached and np.isfinite(f["ls_from_min"])
                           and f["ls_from_min"] >= float(self.x["exit_ls_rise"])))
        bits = sum(b for k, b in FLAG_BITS if f[k])
        ls_age = (t - last_ls) * 5 if last_ls is not None else None
        tr = self.close_trail(T, h, l, c, t if mode == "bar" else t - 1) if l is not None else [None] * 6
        return [sym, T, t_ms, mode, _n(c[t]), _n(f["gain"]), _n(max_gain), _n(f["ls_from_min"]),
                _n(f["oi_from_max"]), _n(f["vol_vs_peak"]), _n(f["r15"]), bits, int(f["n_red"]), f["stage"],
                exit_oi, exit_ls, ls_age] + tr

    def _log_exits(self, rows: list) -> None:
        for r in rows:
            key = (r[0], r[1])
            cur = (r[14], r[15])
            prev = self.prev_exit.get(key, (0, 0))
            if (cur[0] and not prev[0]) or (cur[1] and not prev[1]):
                what = [w for w, on in (("OI −10% от максимума", cur[0]), ("L/S +10% от минимума", cur[1])) if on]
                log.info("маркер выхода %s (искра %s): %s, сейчас %+.1f%% от искры, стадия «%s»",
                         r[0], fmt(r[1]), ", ".join(what), r[5] or 0, r[13])
            self.prev_exit[key] = cur

    # ---------------------------------------------------------------- на закрытии бара
    async def bar_pass(self, b: int) -> None:
        focus = set(self.met.focus)
        lo = b - self.watch_ms - BAR_MS
        hi = b - 30 * 60_000
        fs_from = b - int(float(self.x["focus_spark_hours"]) * 3600_000)

        def job(conn):
            items = {(s, T): o for s, T, o in conn.execute(
                "SELECT symbol, t, o FROM sparks_15m WHERE in_range=1 AND (cand_med=1 OR cand_mean=1) "
                "AND t>=? AND t<=?", (lo, hi))}
            for sym in focus:                            # монета в фокусе — по последней искре, дольше 4.5 ч
                r = conn.execute("SELECT symbol, t, o FROM sparks_15m WHERE symbol=? AND in_range=1 "
                                 "AND t>=? AND t<=? ORDER BY t DESC LIMIT 1", (sym, fs_from, hi)).fetchone()
                if r:
                    items[(r[0], r[1])] = r[2]
            # искры с открытой позицией бота — до закрытия позиции (колонка «Выход»)
            try:
                for s_, T_, o_ in conn.execute("SELECT DISTINCT p.symbol, p.t_spark, s.o FROM paper_trades p "
                                               "JOIN sparks_15m s ON s.symbol=p.symbol AND s.t=p.t_spark "
                                               "WHERE p.exit_ms IS NULL"):
                    items[(s_, T_)] = o_
            except Exception:
                pass
            rows = []
            for (sym, T), o15 in items.items():
                s = Series(conn, sym, T - M15, b)
                r = self.compute(sym, T, o15, s.o, s.h, s.c, s.qv, s.oi, s.ls, "bar", b, l=s.l)
                if r:
                    rows.append(r)
            return rows

        rows = await self.db.run_in_thread(job)
        self.db.put(D.SQL_LIVE, rows)
        self.db.put(D.SQL_LIVE_LATEST, rows)
        self._log_exits(rows)

    # ---------------------------------------------------------------- быстро, для фокуса
    async def fast_loop(self) -> None:
        every = float(self.x["fast_sec"])
        while True:
            await asyncio.sleep(every)
            if self.met.focus:
                try:
                    await self.fast_pass()
                except Exception:
                    log.exception("ошибка быстрых живых тегов")

    async def fast_pass(self) -> None:
        now = now_ms()
        b = floor_bar(now)
        focus = set(self.met.focus)
        fs_from = b - int(float(self.x["focus_spark_hours"]) * 3600_000)
        partial = {s: self.col.partial.get(s) for s in focus}
        last_oi = {s: self.met.last_oi.get(s) for s in focus}

        def job(conn):
            rows = []
            for sym in focus:
                r = conn.execute("SELECT t, o FROM sparks_15m WHERE symbol=? AND in_range=1 AND t>=? AND t<=? "
                                 "ORDER BY t DESC LIMIT 1", (sym, fs_from, b - 30 * 60_000)).fetchone()
                p = partial.get(sym)
                if not r or not p or p[0] != b:
                    continue                             # нет искры или нет текущего бара из WS
                T, o15 = r
                s = Series(conn, sym, T - M15, b)
                o = np.append(s.o, p[1])
                h = np.append(s.h, p[2])
                c = np.append(s.c, p[4])
                qv = np.append(s.qv, p[5])
                lo = last_oi.get(sym)
                oi = np.append(s.oi, lo[1] if lo and lo[0] >= b else np.nan)
                ls = np.append(s.ls, np.nan)
                lo_ = np.append(s.l, p[3])
                row = self.compute(sym, T, o15, o, h, c, qv, oi, ls, "fast", now, l=lo_)
                if row:
                    rows.append(row)
            return rows

        rows = await self.db.run_in_thread(job)
        self.db.put(D.SQL_LIVE, rows)
        self.db.put(D.SQL_LIVE_LATEST, rows)
        self._log_exits(rows)
