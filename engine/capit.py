"""Этап 7: список «капитуляция 4h».

4h-свечи берутся REST-запросом (границы 00/04/08… UTC — те же, что у agg_tf в эталоне) и копятся в базе.
WVF, зелёные бары, начало зоны и rel — функции engine/ref.py (wvf, capitulation_event, market_index).
L/S берётся на открытии бара события: точка на конец 5m-бара перед ним, история — 7 дней.
Событие пишется, только если вся зона попала в окно, где WVF посчитан полностью (окна 22 / 20 / 50).
Исход: +25% за 14 дней от close бара события. «Израсходован»: цена ушла > +30% от минимума зоны.
Чего нет в эталоне: исключение баров с замершей ценой (в ТЗ упомянуто, в ref.wvf его нет — не делаем).
"""
import asyncio
import logging
import warnings

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, DAY_BARS, fmt, now_ms
from .context import grid
from .rest import RestError

log = logging.getLogger("capit")

H4 = 48 * BAR_MS
WARM = 22 + 50 - 2          # с этого индекса WVF и обе полосы посчитаны полностью
OUT_BARS = 84               # 14 дней
WEEK = 7 * DAY_BARS


def floor4(ms: int) -> int:
    return ms - ms % H4


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if np.isfinite(x) else None


class Capit:
    def __init__(self, cfg: dict, db: D.Db, col, rest):
        self.w = cfg["watchlist"]
        self.db = db
        self.col = col
        self.rest = rest
        self.n_bars = int(self.w["capit_bars"])
        self.ready = asyncio.Event()

    # ---------------------------------------------------------------- 4h-свечи
    async def fetch(self) -> None:
        b4 = floor4(now_ms())
        last = dict(await self.db.read("SELECT symbol, MAX(t) FROM raw_klines_4h GROUP BY symbol"))
        sem = asyncio.Semaphore(8)

        async def one(sym):
            lt = last.get(sym)
            # +1: последняя свеча в ответе — текущая, ещё не закрытая, её отбрасываем
            need = self.n_bars + 1 if lt is None else min(self.n_bars + 1, (b4 - lt) // H4 + 2)
            if need <= 1 and lt is not None and lt >= b4 - H4:
                return 0
            async with sem:
                try:
                    rows = await self.rest.get("/fapi/v1/klines", {"symbol": sym, "interval": "4h", "limit": need},
                                               weight=1 if need < 100 else 2)
                except RestError as e:
                    log.warning("%s: 4h-свечи: %s", sym, e)
                    return 0
            out = [(sym, int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7]))
                   for r in rows if int(r[0]) + H4 <= now_ms()]
            self.db.put(D.SQL_K4, out)
            return len(out)

        n = await asyncio.gather(*(one(s) for s in list(self.col.symbols)))
        await self.db.flush()
        log.info("4h-свечи: %d новых по %d монетам", sum(n), sum(1 for x in n if x))

    # ---------------------------------------------------------------- события
    async def process(self) -> None:
        b4 = floor4(now_ms())
        N = self.n_bars
        t0 = b4 - N * H4
        syms = sorted(self.col.symbols)
        ls_low = ref.TH["ls_low"]
        spent_k = 1 + float(self.w["capit_spent_pct"]) / 100

        def job(conn):
            idx = {s: k for k, s in enumerate(syms)}
            A = {f: np.full((N, len(syms)), np.nan) for f in ("o", "h", "l", "c")}
            for s, t, o, h, l, c in conn.execute("SELECT symbol, t, o, h, l, c FROM raw_klines_4h WHERE t>=? AND t<?",
                                                 (t0, b4)):
                if s in idx:
                    i = (t - t0) // H4
                    A["o"][i, idx[s]], A["h"][i, idx[s]], A["l"][i, idx[s]], A["c"][i, idx[s]] = o, h, l, c
            # Бар, где ни у одной монеты нет данных (например, начало окна), даёт предупреждение numpy
            # «All-NaN slice»; ref.market_index сам заменяет такой шаг на 0 — на rel это не влияет.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                mk = ref.market_index(A["c"])
            known = {(s, t): (inl, outc, spent) for s, t, inl, outc, spent in conn.execute(
                "SELECT symbol, t_in, in_list, outcome, spent_ms FROM watchlist_events WHERE list='capit' AND t_in>=?",
                (t0,))}
            new, upd_out, upd_spent = [], [], []
            dummy_ls = np.full(N * 48, np.nan)
            det = now_ms()
            for s in syms:
                k_ = idx[s]
                o4, h4, l4, c4 = A["o"][:, k_], A["h"][:, k_], A["l"][:, k_], A["c"][:, k_]
                if np.isfinite(c4).sum() < WARM:
                    continue
                w, g, up, rh = ref.wvf(c4, l4)
                for i in range(WARM + 2, N):
                    ev = ref.capitulation_event(i, g, c4, dummy_ls, mk)
                    if ev is None or ev["zone_start"] < WARM:
                        continue
                    t_in = t0 + i * H4
                    k = ev["zone_start"]
                    zmin = float(np.nanmin(l4[k:i]))
                    st = known.get((s, t_in))
                    if st is None:
                        ls_pct, exact = self._ls_at(conn, s, t_in)
                        inl = int(bool(np.isfinite(ls_pct) and ls_pct < ls_low))
                        new.append(["capit", s, t_in, None, _n(c4[i]), None, _n(ls_pct), exact, None, None,
                                    t0 + k * H4, zmin, _n(ev["rel"]), inl, None, None, None, None,
                                    "live" if t_in >= b4 - 2 * H4 else "history", det])
                        st = (inl, None, None)
                    # исход за 14 дней
                    if st[1] is None and i + OUT_BARS < N and np.isfinite(c4[i]):
                        mx = np.nanmax(h4[i + 1:i + 1 + OUT_BARS]) / c4[i] - 1
                        upd_out.append((int(mx >= 0.25), _n(mx * 100), det, s, t_in))
                    # израсходован по 4h-максимумам после события
                    if st[0] and st[2] is None:
                        after = h4[i + 1:]
                        hit = np.where(np.isfinite(after) & (after > zmin * spent_k))[0]
                        if len(hit):
                            upd_spent.append((t0 + (i + 1 + int(hit[0])) * H4 + H4, s, t_in))
            return new, upd_out, upd_spent

        new, upd_out, upd_spent = await self.db.run_in_thread(job)
        self.db.put(D.SQL_WATCH_EVENT, new)
        self.db.put("UPDATE watchlist_events SET outcome=?, outcome_max=?, outcome_ms=? "
                    "WHERE list='capit' AND symbol=? AND t_in=?", upd_out)
        self.db.put("UPDATE watchlist_events SET spent_ms=? WHERE list='capit' AND symbol=? AND t_in=? "
                    "AND spent_ms IS NULL", upd_spent)
        await self.db.flush()
        sig = [r for r in new if r[13]]
        log.info("капитуляция 4h: новых концов зон %d, из них сигналов (L/S < 20) %d", len(new), len(sig))
        for r in sig:
            if r[18] == "live":
                log.info("«капитуляция 4h» +%s: зона с %s, L/S %.0f%% недели, к рынку %s",
                         r[1], fmt(r[10]), r[6], f"{r[12]:+.1f}%" if r[12] is not None else "?")

    def _ls_at(self, conn, sym: str, t_in: int) -> tuple[float, int]:
        """Процентиль L/S на открытии 4h-бара события: ls на конец 5m-бара перед ним, история 7 дней."""
        t = t_in - BAR_MS
        t_from = t - WEEK * BAR_MS
        n = WEEK + 1
        ls = grid(conn, "SELECT t, ratio FROM raw_ls WHERE symbol=? AND kind='global' AND t>=? AND t<=?",
                  (sym, t_from, t), t_from, n)
        exact = int(bool(np.isfinite(ls[n - 1]) and np.isfinite(ls[:n - 1]).sum() >= int(self.w["ls_min_history"])))
        return ref.ls_percentile_at(ls, n - 1), exact

    # ---------------------------------------------------------------- 5m: «израсходован» и текущее состояние
    async def bar_pass(self, b: int) -> None:
        if not self.ready.is_set():
            return
        spent_k = 1 + float(self.w["capit_spent_pct"]) / 100
        since = b - (OUT_BARS * H4 + H4)

        def job(conn):
            rows, spent, gone = [], [], []
            act = conn.execute("SELECT symbol, t_in, zone_min, ls_pct FROM watchlist_events WHERE list='capit' "
                               "AND in_list=1 AND spent_ms IS NULL AND t_in>=?", (since,)).fetchall()
            for sym, t_in, zmin, lsp in act:
                close_t = t_in + H4
                r = conn.execute("SELECT MAX(h) FROM raw_klines_5m WHERE symbol=? AND t>=?", (sym, close_t)).fetchone()
                last = conn.execute("SELECT c FROM raw_klines_5m WHERE symbol=? ORDER BY t DESC LIMIT 1",
                                    (sym,)).fetchone()
                if r and r[0] and zmin and r[0] > zmin * spent_k:
                    spent.append((b, sym, t_in))
                    gone.append((sym, t_in))
                    continue
                price = last[0] if last else None
                rows.append(["capit", sym, t_in, b - BAR_MS, price, None, None, None, lsp, None,
                             (price / zmin - 1) * 100 if price and zmin else None])
            live = {(r[1], r[2]) for r in rows}
            gone += [(s, t) for s, t in conn.execute("SELECT symbol, t_in FROM watch_now WHERE list='capit'")
                     if (s, t) not in live]
            return rows, spent, gone

        rows, spent, gone = await self.db.run_in_thread(job)
        self.db.put("UPDATE watchlist_events SET spent_ms=? WHERE list='capit' AND symbol=? AND t_in=? "
                    "AND spent_ms IS NULL", spent)
        self.db.put("DELETE FROM watch_now WHERE list='capit' AND symbol=? AND t_in=?", gone)
        self.db.put(D.SQL_WATCH_NOW, rows)
        for _, sym, t in spent:
            log.info("«капитуляция 4h» %s (событие %s): израсходован — цена ушла больше +%s%% от минимума зоны",
                     sym, fmt(t), self.w["capit_spent_pct"])

    # ---------------------------------------------------------------- цикл раз в 4 ч
    async def loop(self) -> None:
        await self.col.backfill_done.wait()
        while True:
            try:
                await self.fetch()
                await self.process()
                self.ready.set()
            except Exception:
                log.exception("ошибка «капитуляции 4h»")
            now = now_ms()
            nxt = floor4(now) + H4 + int(float(self.w["capit_delay_sec"]) * 1000)
            await asyncio.sleep((nxt - now) / 1000)
