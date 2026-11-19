"""Watchlist «Перепроданность RSI» (заменяет на экране «капитуляцию 4h»; сама капитуляция считается дальше —
для тега в ленте и журнала).

Монета в списке, если RSI(14) хотя бы на одном таймфрейме — 1h, 2h, 3h, 4h — ниже порога (30) на последнем
закрытом баре. 1h-свечи берутся REST-запросом и копятся в raw_klines_1h; 2h, 3h и 4h собираются из них
с границами от 00:00 UTC (бар считается, только если все его часы есть). RSI — Уайлдер, как в TradingView.
Рядом — L/S всех счетов (последняя точка) и его процентиль за 7 дней, как в «капитуляции».
Пересчёт RSI — на закрытии каждого часа; L/S и цена у монет из списка — каждые 5 минут.
"""
import asyncio
import logging

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, DAY_BARS, floor_bar, now_ms
from .context import grid
from .indicators import rsi
from .rest import RestError

log = logging.getLogger("rsi")

H1 = 3_600_000
TFS = (1, 2, 3, 4)
N1 = 300                                      # часов истории: разгон RSI на 3h/4h
WEEK = 7 * DAY_BARS


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if np.isfinite(x) else None


class OversoldRSI:
    def __init__(self, cfg: dict, db: D.Db, col, rest):
        self.w = cfg.get("watchlist", {})
        self.db = db
        self.col = col
        self.rest = rest
        self.th = float(self.w.get("rsi_oversold", 30))
        self.n = int(self.w.get("rsi_period", 14))
        self.ready = asyncio.Event()

    # ---------------------------------------------------------------- 1h-свечи
    async def fetch(self) -> None:
        b1 = now_ms() // H1 * H1
        last = dict(await self.db.read("SELECT symbol, MAX(t) FROM raw_klines_1h GROUP BY symbol"))
        sem = asyncio.Semaphore(8)

        async def one(sym):
            lt = last.get(sym)
            need = N1 + 1 if lt is None else min(N1 + 1, (b1 - lt) // H1 + 2)
            if lt is not None and lt >= b1 - H1:
                return 0
            async with sem:
                try:
                    rows = await self.rest.get("/fapi/v1/klines", {"symbol": sym, "interval": "1h", "limit": need},
                                               weight=1 if need < 100 else 2)
                except RestError as e:
                    log.warning("%s: 1h-свечи: %s", sym, e)
                    return 0
            out = [(sym, int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7]))
                   for r in rows if int(r[0]) + H1 <= now_ms()]
            self.db.put(D.SQL_K1H, out)
            return len(out)

        n = await asyncio.gather(*(one(s) for s in list(self.col.symbols)))
        await self.db.flush()
        log.info("1h-свечи: %d новых по %d монетам", sum(n), sum(1 for x in n if x))

    # ---------------------------------------------------------------- RSI по таймфреймам
    def _ls(self, conn, sym: str, t_last: int) -> tuple:
        """Последняя точка L/S (не старше часа) и её процентиль за 7 дней (доля строго меньших)."""
        t_from = t_last - (WEEK + 12) * BAR_MS
        n = WEEK + 13
        ls = grid(conn, "SELECT t, ratio FROM raw_ls WHERE symbol=? AND kind='global' AND t>=? AND t<=?",
                  (sym, t_from, t_last), t_from, n)
        idx = np.where(np.isfinite(ls[-12:]))[0]
        if not len(idx):
            return None, None, 0
        k = n - 12 + int(idx[-1])
        exact = int(np.isfinite(ls[max(0, k - WEEK):k]).sum() >= int(self.w.get("ls_min_history", 1900)))
        return _n(ls[k]), _n(ref.ls_percentile_at(ls, k)), exact

    async def process(self) -> None:
        b1 = now_ms() // H1 * H1
        t0 = b1 - N1 * H1
        t0 -= t0 % (12 * H1)                           # общая граница для 2h/3h/4h от 00:00 UTC
        n = (b1 - t0) // H1
        syms = sorted(self.col.symbols)
        th, per = self.th, self.n
        b5 = floor_bar(now_ms()) - BAR_MS

        def job(conn):
            out, events, allv = [], [], {}
            det = now_ms()
            for s in syms:
                c1 = np.full(n, np.nan)
                qv = np.full(n, np.nan)
                for t, c, q in conn.execute("SELECT t, c, qv FROM raw_klines_1h WHERE symbol=? AND t>=? AND t<?",
                                            (s, t0, b1)):
                    c1[(t - t0) // H1] = c
                    qv[(t - t0) // H1] = q
                if np.isfinite(c1).sum() < 30:
                    continue
                vals, since = {}, {}
                for m in TFS:
                    k = n // m
                    grp = c1[:k * m].reshape(k, m)
                    cm = np.where(np.isfinite(grp).all(1), grp[:, -1], np.nan)
                    r = rsi(cm, per)
                    last = k - 1
                    while last >= 0 and not np.isfinite(r[last]):
                        last -= 1
                    if last < 0 or last < k - 2:               # последний бар без RSI — данных нет
                        vals[m], since[m] = None, None
                        continue
                    vals[m] = float(r[last])
                    if r[last] < th:
                        j = last
                        while j - 1 >= 0 and np.isfinite(r[j - 1]) and r[j - 1] < th:
                            j -= 1
                        since[m] = t0 + (j + 1) * m * H1        # закрытие первого бара серии ниже порога
                        events.append([f"rsi{m}h", s, since[m], None, _n(cm[j]), None, None, None, None, None,
                                       None, None, None, 1, None, None, None, None, "live", det])
                    else:
                        since[m] = None
                allv[s] = dict(vals)
                if not any(v is not None and v < th for v in vals.values()):
                    continue
                ls, lsp, exact = self._ls(conn, s, b5)
                pr = conn.execute("SELECT c FROM raw_klines_5m WHERE symbol=? ORDER BY t DESC LIMIT 1", (s,)).fetchone()
                q24 = np.nansum(qv[-24:]) / 1e6 if np.isfinite(qv[-24:]).sum() >= 20 else None
                out.append([s, b1, *(vals[m] for m in TFS), *(since[m] for m in TFS), ls, lsp, exact,
                            _n(q24), pr[0] if pr else None, det])
            conn.execute("DELETE FROM rsi_now")
            return out, events, allv

        out, events, allv = await self.db.run_in_thread(job)
        self.db.put(D.SQL_RSI_NOW, out)
        self.db.put(D.SQL_WATCH_EVENT, events)
        await self.db.flush()
        cnt = {m: sum(1 for r in out if r[1 + m] is not None and r[1 + m] < th) for m in TFS}
        log.info("перепроданность RSI < %g: %d монет (1h %d, 2h %d, 3h %d, 4h %d)", th, len(out), *cnt.values())

    # ---------------------------------------------------------------- каждые 5 минут: L/S и цена
    async def bar_pass(self, b: int) -> None:
        if not self.ready.is_set():
            return

        def job(conn):
            rows = []
            for (s,) in conn.execute("SELECT symbol FROM rsi_now").fetchall():
                ls, lsp, exact = self._ls(conn, s, b - BAR_MS)
                pr = conn.execute("SELECT c FROM raw_klines_5m WHERE symbol=? ORDER BY t DESC LIMIT 1", (s,)).fetchone()
                rows.append((ls, lsp, exact, pr[0] if pr else None, now_ms(), s))
            return rows

        rows = await self.db.run_in_thread(job)
        self.db.put("UPDATE rsi_now SET ls=?, ls_pct=?, ls_exact=?, price=?, upd_ms=? WHERE symbol=?", rows)

    async def loop(self) -> None:
        await self.col.backfill_done.wait()
        while True:
            try:
                await self.fetch()
                await self.process()
                self.ready.set()
            except Exception:
                log.exception("ошибка «перепроданности RSI»")
            now = now_ms()
            nxt = now // H1 * H1 + H1 + int(float(self.w.get("rsi_delay_sec", 75)) * 1000)
            await asyncio.sleep((nxt - now) / 1000)
