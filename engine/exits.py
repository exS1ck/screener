"""Модель выходов v0.2 на 1m-свечах — функции engine/ref02.py (копия эталона), здесь только данные.

Для каждой 15m-искры с подхватом, когда путь 48 ч после входа закрылся, движок берёт 1m-свечи через REST
(хранит в raw_klines_1m) и считает три выхода: A-c (трейлинг по закрытию 5m), A-c-t (+ проверка на 6-м часу)
и «спокойный» (⅓ на +6%, безубыток, остаток трейлингом). Результат — в outcomes (колонки x_*).
Вход E — close бара T+25…T+30 (как в эталоне), стоп S = L − 0.002·E. Минута k пути — [вход + k, вход + k + 1),
выход в минуту k происходит в момент вход + (k + 1) мин.
"""
import asyncio
import logging
import math

import numpy as np

from . import db as D
from . import ref02
from .clock import floor_bar, fmt, now_ms
from .rest import RestError

log = logging.getLogger("exits")

MIN = 60_000
HOLD = 2880                                    # минут в 48 ч
MODES = (("c", ref02.exit_A_c), ("ct", ref02.exit_A_c_t), ("calm", ref02.exit_calm))


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if math.isfinite(x) else None


def minutes_after(bars: dict, entry_ms: int, n: int = HOLD) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """h, l, c 1m-баров с минуты сразу после входа: индекс k = бар с открытием entry_ms + k минут."""
    h, l, c = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    for t, (hh, ll, cc) in bars.items():
        k = (t - entry_ms) // MIN
        if 0 <= k < n:
            h[k], l[k], c[k] = hh, ll, cc
    return h, l, c


def run_models(h, l, c, E: float, S: float) -> dict:
    out = {}
    for name, fn in MODES:
        r = fn(h, l, c, E, S)
        out[name] = dict(pct=_n(r["pnl_pct"]), reason=r["reason"], min=int(r["exit_k"]) + 1,
                         price=_n(r["exit_price"]), tp1=None if r.get("tp1_k") is None else int(r["tp1_k"]) + 1)
    return out


SQL_X = ("UPDATE outcomes SET x_c_pct=?, x_c_reason=?, x_c_min=?, x_ct_pct=?, x_ct_reason=?, x_ct_min=?, "
         "x_calm_pct=?, x_calm_reason=?, x_calm_min=?, x_calm_tp1_min=?, x_n1m=?, x_ms=? "
         "WHERE kind='15m' AND symbol=? AND t=?")


class ExitModels:
    def __init__(self, cfg: dict, db: D.Db, rest):
        self.db = db
        self.rest = rest
        self.every = float(cfg.get("outcomes", {}).get("every_min", 15)) * 60
        self.max_age = int(float(cfg.get("outcomes", {}).get("max_age_days", 60))) * 86_400_000

    async def fetch_1m(self, sym: str, start: int, n: int = HOLD) -> dict:
        """1m-свечи [start, start + n мин): сначала из базы, недостающее — REST (по 1500 за запрос)."""
        have = {t: (h, l, c) for t, h, l, c in await self.db.read(
            "SELECT t, h, l, c FROM raw_klines_1m WHERE symbol=? AND t>=? AND t<?", (sym, start, start + n * MIN))}
        if len(have) >= n - 5:
            return have
        rows = []
        cur, end = start, min(start + n * MIN, floor_bar(now_ms()))
        while cur < end:
            lim = min(1500, (end - cur) // MIN)
            if lim <= 0:
                break
            try:
                d = await self.rest.get("/fapi/v1/klines", {"symbol": sym, "interval": "1m", "startTime": cur,
                                                            "limit": lim}, weight=10 if lim > 1000 else 5)
            except RestError as e:
                log.warning("%s: 1m-свечи: %s", sym, e)
                break
            if not d:
                break
            for r in d:
                t = int(r[0])
                if t + MIN <= now_ms():
                    rows.append((sym, t, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7])))
                    have[t] = (float(r[2]), float(r[3]), float(r[4]))
            cur = int(d[-1][0]) + MIN
        self.db.put(D.SQL_K1, rows)
        return have

    async def loop(self) -> None:
        while True:
            try:
                await self.process()
            except Exception:
                log.exception("ошибка модели выходов")
            await asyncio.sleep(self.every)

    async def process(self) -> None:
        now = now_ms()
        ready = now - (30 + 1) * MIN - HOLD * MIN               # путь 48 ч после входа уже закрылся
        rows = await self.db.read(
            "SELECT s.symbol, s.t, s.l, s.entry FROM sparks_15m s JOIN outcomes o "
            "ON o.kind='15m' AND o.symbol=s.symbol AND o.t=s.t "
            "WHERE s.conf=1 AND s.entry IS NOT NULL AND o.x_ms IS NULL AND s.t<=? AND s.t>=? ORDER BY s.t",
            (ready, now - self.max_age))
        out = []
        for sym, T, L, E in rows:
            entry_ms = T + 30 * MIN
            bars = await self.fetch_1m(sym, entry_ms)
            h, l, c = minutes_after(bars, entry_ms)
            m = run_models(h, l, c, E, ref02.stop_price(E, L))
            out.append([m["c"]["pct"], m["c"]["reason"], m["c"]["min"], m["ct"]["pct"], m["ct"]["reason"],
                        m["ct"]["min"], m["calm"]["pct"], m["calm"]["reason"], m["calm"]["min"], m["calm"]["tp1"],
                        int(np.isfinite(c).sum()), now_ms(), sym, T])
        self.db.put(SQL_X, out)
        if out:
            log.info("модель выходов v0.2 на 1m: %d искр", len(out))
