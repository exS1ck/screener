"""Этап 5: досчёт исходов искр через 12, 24 и 48 часов.

Раз в every_min минут находит искры, у которых созрел новый горизонт, и пересчитывает их строку
в outcomes целиком (пересчёт детерминированный: если дыры в свечах догрузились, итог уточнится).
Памп и модель сделки — функции engine/ref.py. «+10% раньше −5%» в эталоне нет — определение здесь.
"""
import asyncio
import logging
import math

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, floor_bar, now_ms
from .sparks import M15, floor15, load_bars

log = logging.getLogger("outcomes")

H = 3600_000
HORIZONS = (12, 24, 48)
TRADE_READY = 30 * 60_000 + 48 * H + BAR_MS      # c[e+576] закрылся: e = j+5, ещё 576 баров
SPAN = TRADE_READY + BAR_MS


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if math.isfinite(x) else None


def level_possible(kind: str, T: int, b: int, has_entry: bool) -> int:
    lv = sum(1 for hh in HORIZONS if T + hh * H <= b)
    if lv == 3 and kind == "15m" and has_entry and T + TRADE_READY <= b:
        lv = 4
    return lv


def hit_10_5(h, l, e: int, E: float, last: int) -> tuple[int | None, int | None]:
    """С бара после входа (e+1) до last включительно: что раньше, +10% или −5% от E."""
    up, dn = E * 1.10, E * 0.95
    for k in range(e + 1, last + 1):
        hu = np.isfinite(h[k]) and h[k] >= up
        hd = np.isfinite(l[k]) and l[k] <= dn
        if hd:                                    # оба в одном баре — консервативно считаем −5%
            return -1, (k - e) * 5
        if hu:
            return 1, (k - e) * 5
    return None, None


def outcome_row(kind: str, sym: str, T: int, a: np.ndarray, t_from: int, b: int, has_entry: bool) -> list | None:
    o, h, l, c, qv, tbq = a
    j = (T - t_from) // BAR_MS
    lv = level_possible(kind, T, b, has_entry)
    if lv == 0:
        return None
    if kind == "15m":
        b15 = ref.agg15(o, h, l, c, qv, tbq)
        i = j // 3
        o_ref = b15["o"][i]
    else:
        b15, i, o_ref = None, None, o[j]
    if not np.isfinite(o_ref) or o_ref <= 0:
        return None
    row = dict(kind=kind, symbol=sym, t=T, o_ref=_n(o_ref), entry=None, level=lv, computed_ms=now_ms())
    for k, hh in enumerate(HORIZONS, 1):
        if k > lv:
            break
        n = hh * 12
        hw, lw = h[j:j + n], l[j:j + n]
        if np.isfinite(hw).any():
            pk = int(np.nanargmax(hw))
            row[f"max{hh}"] = _n((np.nanmax(hw) / o_ref - 1) * 100)
            row[f"min{hh}"] = _n((np.nanmin(lw) / o_ref - 1) * 100)
            row[f"peak{hh}_min"] = pk * 5
        row[f"inc{hh}"] = int(np.isnan(c[j:j + n]).sum())
    row["pump12"] = int(ref.pump_label(h, o_ref, j)) if np.isfinite(h[j:j + 144]).any() else None
    if kind == "15m":
        e = j + 5
        E = c[e]
        if np.isfinite(E) and E > 0:
            row["entry"] = _n(E)
            last = min(e + 576, len(c) - 1, (b - t_from) // BAR_MS - 1)
            d, m = hit_10_5(h, l, e, E, last)
            if d is None and lv >= 3:
                d = 0
            row["hit_dir"], row["hit_min"] = d, m
            if lv >= 4:
                pct, k, reason = ref.trade_trailing(i, b15, h, l, c)
                row["trade_pct"], row["trade_bars"], row["trade_reason"] = _n(pct), int(k) + 1, reason
    return [row.get(cname) for cname in D.OUTCOME_COLS]


class OutcomeBuilder:
    def __init__(self, cfg: dict, db: D.Db, spk):
        self.o = cfg["outcomes"]
        self.db = db
        self.spk = spk

    async def loop(self) -> None:
        await self.spk.ready.wait()
        while True:
            try:
                await self.process()
            except Exception:
                log.exception("ошибка досчёта исходов")
            await asyncio.sleep(float(self.o["every_min"]) * 60)

    async def process(self) -> None:
        b = floor_bar(now_ms())
        since = b - int(self.o["max_age_days"]) * 24 * H

        def job(conn):
            have = {(k, s, t): lv for k, s, t, lv in conn.execute(
                "SELECT kind, symbol, t, level FROM outcomes WHERE t >= ?", (since,))}
            todo: dict[str, list] = {}
            for kind, sql in (("15m", "SELECT symbol, t, entry IS NOT NULL OR conf_ms IS NULL FROM sparks_15m WHERE t >= ?"),
                              ("5m", "SELECT symbol, t, 0 FROM sparks_5m WHERE t >= ?")):
                for sym, T, ent in conn.execute(sql, (since,)):
                    # Пока подхват не посчитан, считаем, что вход будет: модель сделки подождёт.
                    lv = level_possible(kind, T, b, bool(ent))
                    if lv > have.get((kind, sym, T), 0):
                        todo.setdefault(sym, []).append((kind, T, bool(ent)))
            rows = []
            for sym, items in todo.items():
                tmin = min(T for _, T, _ in items)
                tmax = max(T for _, T, _ in items)
                t_from = floor15(tmin)
                a = load_bars(conn, sym, t_from, min(b, tmax + SPAN))
                for kind, T, ent in items:
                    r = outcome_row(kind, sym, T, a, t_from, b, ent)
                    if r:
                        rows.append(r)
            return rows

        rows = await self.db.run_in_thread(job)
        self.db.put(D.SQL_OUTCOME, rows)
        if rows:
            n15 = sum(1 for r in rows if r[0] == "15m")
            log.info("исходы: 15m — %d, 5m — %d", n15, len(rows) - n15)
        await self.forks(b)

    async def forks(self, b: int) -> None:
        """Исход «вилки» для моментов живых тегов (режим bar): от цены в момент тега — что раньше,
        +10% или −5%; если ни то ни другое за 48 ч — результат по close. Для таблицы «исходы по стадиям»."""
        up, dn = 1 + float(self.o["fork_up_pct"]) / 100, 1 - float(self.o["fork_down_pct"]) / 100
        span = int(float(self.o["fork_hours"]) * H)

        def job(conn):
            pend = conn.execute("SELECT symbol, t_spark, t_ms, price FROM live_tags WHERE mode='bar' "
                                "AND fork_ms IS NULL AND price IS NOT NULL AND t_ms<=?", (b - BAR_MS,)).fetchall()
            by: dict[tuple, list] = {}
            for sym, ts, t, px in pend:
                by.setdefault((sym, ts), []).append((t, px))
            out = []
            for (sym, ts), items in by.items():
                t0 = min(t for t, _ in items)
                t1 = min(b, max(t for t, _ in items) + span)
                bars = conn.execute("SELECT t, h, l, c FROM raw_klines_5m WHERE symbol=? AND t>=? AND t<? ORDER BY t",
                                    (sym, t0, t1)).fetchall()
                for t, px in items:
                    res = None
                    last_c = None
                    for bt, h, l, c in bars:
                        if bt < t:
                            continue
                        if bt >= t + span:
                            break
                        if l is not None and l <= px * dn:
                            res = (-float(self.o["fork_down_pct"]), -1)
                            break
                        if h is not None and h >= px * up:
                            res = (float(self.o["fork_up_pct"]), 1)
                            break
                        last_c = c
                    if res is None and t + span <= b and last_c:
                        res = ((last_c / px - 1) * 100, 0)
                    if res:
                        out.append((res[0], res[1], now_ms(), sym, ts, t))
            return out

        rows = await self.db.run_in_thread(job)
        self.db.put("UPDATE live_tags SET fork_pct=?, fork_dir=?, fork_ms=? WHERE symbol=? AND t_spark=? AND t_ms=?",
                    rows)
