"""Этап 1, шаг 2: REST-опрос OI, L/S и стакана.

OI (живой, /fapi/v1/openInterest):
  все монеты — сразу после закрытия каждого 5m-бара → raw_oi.oi_live[k];
  под наблюдением — раз в oi_watch_sec, «в фокусе» — раз в oi_focus_sec → raw_oi_fast.
OI (история, /futures/data/openInterestHist): при старте ~41 ч, потом раз в час последние точки → raw_oi.oi_hist.
L/S всех счетов: все монеты раз в 5 мин, при старте 7 дней истории → raw_ls (kind=global).
L/S топ-трейдеров (счета и позиции): только монеты под наблюдением → raw_ls (top_acc / top_pos).
Стакан limit=500: все монеты раз в час, равномерно по часу → raw_depth_hourly.
"""
import asyncio
import logging
import sqlite3
import statistics

from . import db as D
from .clock import BAR_MS, DAY_BARS, floor_bar, fmt, now_ms
from .rest import Rest, RestError

log = logging.getLogger("metrics")

LS_PATHS = {
    "global": "/futures/data/globalLongShortAccountRatio",
    "top_acc": "/futures/data/topLongShortAccountRatio",
    "top_pos": "/futures/data/topLongShortPositionRatio",
}


def depth_stats(bids: list, asks: list) -> dict | None:
    b = [(float(p), float(q)) for p, q in bids]
    a = [(float(p), float(q)) for p, q in asks]
    if not b or not a:
        return None
    bb, ba = b[0][0], a[0][0]
    mid = (bb + ba) / 2

    def ask_sum(lim):
        return sum(p * q for p, q in a if p <= lim)

    def bid_sum(lim):
        return sum(p * q for p, q in b if p >= lim)

    return dict(
        mid=mid, bid=bb, ask=ba,
        ask1=ask_sum(mid * 1.01), ask2=ask_sum(mid * 1.02), ask5=ask_sum(mid * 1.05),
        ask2_best=ask_sum(ba * 1.02),
        bid1=bid_sum(mid * 0.99), bid2=bid_sum(mid * 0.98), bid5=bid_sum(mid * 0.95),
        ask_cov=(a[-1][0] / mid - 1) * 100, bid_cov=(1 - b[-1][0] / mid) * 100,
    )


class Metrics:
    def __init__(self, cfg: dict, db: D.Db, rest: Rest, col):
        self.cfg = cfg
        self.m = cfg["metrics"]
        self.db = db
        self.rest = rest
        self.col = col                                   # Collector: col.symbols — вселенная
        self.focus: set[str] = set()
        self.watch: set[str] = set()                     # под наблюдением (включая фокус)
        self.ls_last: dict[tuple[str, str], int] = {}    # (монета, вид) → t последней точки
        self.fast_due: dict[str, float] = {}
        self.last_oi: dict[str, tuple[int, float]] = {}  # последний живой OI по монете: (момент, значение)
        self.ls_ready = asyncio.Event()
        self.stat = dict(oi_live=0, oi_late=0, oi_fail=0, oi_fast=0, oi_hist=0,
                         ls_points=0, ls_fresh=0, ls_total=0, depth=0, depth_short=0)
        self.last_oi_bar = (0, 0, 0)                     # (бар, получено, опоздало)

    # ================================================================ наборы монет
    async def load_state(self) -> None:
        def job(conn):
            out = []
            syms = {r[0] for r in conn.execute("SELECT symbol FROM symbols")} | set(self.col.symbols)
            for s in syms:
                for k in ("global", "top_acc", "top_pos"):
                    t = conn.execute("SELECT MAX(t) FROM raw_ls WHERE symbol=? AND kind=?", (s, k)).fetchone()[0]
                    if t is not None:
                        out.append((s, k, t))
            return out
        for s, k, t in await self.db.run_in_thread(job):
            self.ls_last[(s, k)] = t
        log.info("L/S в базе: %d рядов", len(self.ls_last))

    async def sets_loop(self) -> None:
        every = float(self.m["sets_refresh_sec"])
        hours = float(self.m["watch_hours"])
        while True:
            since = now_ms() - int(hours * 3600_000)

            def job(conn):
                focus = {r[0] for r in conn.execute("SELECT DISTINCT symbol FROM focus WHERE off_ms IS NULL")}
                watch = set()
                # Таблицы появятся на этапах 2 и 8; до тех пор наблюдение = только фокус.
                for sql in ("SELECT DISTINCT symbol FROM sparks_15m WHERE t >= ? AND in_range = 1",
                            "SELECT DISTINCT symbol FROM paper_trades WHERE exit_ms IS NULL"):
                    try:
                        args = (since,) if "?" in sql else ()
                        watch |= {r[0] for r in conn.execute(sql, args)}
                    except sqlite3.OperationalError:
                        pass
                return focus, watch

            try:
                focus, watch = await self.db.run_in_thread(job)
                watch |= focus
                if focus != self.focus or watch != self.watch:
                    log.info("в фокусе: %s | под наблюдением: %d", ", ".join(sorted(focus)) or "—", len(watch))
                self.focus, self.watch = focus, watch
            except Exception:
                log.exception("ошибка чтения фокуса")
            await asyncio.sleep(every)

    # ================================================================ OI
    async def _oi_now(self, sym: str) -> tuple[float, int] | None:
        try:
            d = await self.rest.get("/fapi/v1/openInterest", {"symbol": sym}, weight=1, high=True, retries=1)
            return float(d["openInterest"]), int(d.get("time") or now_ms())
        except (RestError, KeyError, ValueError):
            return None

    async def oi_bar_loop(self) -> None:
        """Сразу после закрытия бара k — OI по всем монетам в raw_oi.oi_live[k]."""
        delay = float(self.m["oi_bar_delay_sec"]) * 1000
        late_ms = float(self.m["oi_late_sec"]) * 1000
        sem = asyncio.Semaphore(int(self.m["oi_concurrency"]))
        while True:
            now = now_ms()
            b = floor_bar(now) + BAR_MS
            await asyncio.sleep((b + delay - now) / 1000)
            k = b - BAR_MS

            async def one(s):
                async with sem:
                    r = await self._oi_now(s)
                return None if r is None else (s, k, r[0], r[1])

            res = await asyncio.gather(*(one(s) for s in list(self.col.symbols)))
            rows = [r for r in res if r]
            late = sum(1 for r in rows if r[3] - b > late_ms)
            self.db.put(D.SQL_OI_LIVE, rows)
            self.stat["oi_live"] += len(rows)
            self.stat["oi_late"] += late
            self.stat["oi_fail"] += len(res) - len(rows)
            self.last_oi_bar = (k, len(rows), late)

    async def oi_fast_loop(self) -> None:
        focus_sec = float(self.m["oi_focus_sec"])
        watch_sec = float(self.m["oi_watch_sec"])
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(1)
            t = loop.time()
            due = [s for s in self.watch if self.fast_due.get(s, 0) <= t]
            for s in due:
                self.fast_due[s] = t + (focus_sec if s in self.focus else watch_sec)
            for s in list(self.fast_due):
                if s not in self.watch:
                    del self.fast_due[s]
            if not due:
                continue
            res = await asyncio.gather(*(self._oi_now(s) for s in due))
            rows = [(s, r[1], r[0]) for s, r in zip(due, res) if r]
            for s, t, v in rows:
                self.last_oi[s] = (t, v)
            self.db.put(D.SQL_OI_FAST, rows)
            self.stat["oi_fast"] += len(rows)

    async def _oi_hist(self, sym: str, limit: int, end: int | None = None) -> int:
        end = floor_bar(now_ms()) if end is None else end
        start = end - (limit - 1) * BAR_MS
        try:
            d = await self.rest.get("/futures/data/openInterestHist",
                                    {"symbol": sym, "period": "5m", "limit": limit,
                                     "startTime": start, "endTime": end}, fd=True)
        except RestError as e:
            log.warning("%s: openInterestHist: %s", sym, e)
            return 0
        rows = [(sym, int(x["timestamp"]) - BAR_MS, float(x["sumOpenInterest"]),
                 float(x.get("sumOpenInterestValue") or 0)) for x in d]
        self.db.put(D.SQL_OI_HIST, rows)
        self.stat["oi_hist"] += len(rows)
        return len(rows)

    async def _oi_hist_week(self, sym: str, sem: asyncio.Semaphore) -> int:
        """История OI за oi_hist_days постранично: openInterestHist отдаёт до 500 точек (~41.7 ч) за запрос.
        Идём назад от текущего момента через endTime; страницы, которые в базе уже есть, пропускаем."""
        days = float(self.m.get("oi_hist_days", 7))
        page = 500
        now_b = floor_bar(now_ms())
        n_pages = int(days * DAY_BARS / page) + 1
        got = 0
        for k in range(n_pages):
            end = now_b - k * page * BAR_MS
            start = end - (page - 1) * BAR_MS
            have = await self.db.read("SELECT COUNT(*) FROM raw_oi WHERE symbol=? AND t>=? AND t<=? AND oi_hist IS NOT NULL",
                                      (sym, start - BAR_MS, end - BAR_MS))
            if have and have[0][0] >= page - 10:
                continue
            async with sem:
                got += await self._oi_hist(sym, page, end)
        return got

    async def oi_hist_loop(self) -> None:
        """При старте — история OI за 7 дней (постранично), потом раз в час последние точки
        (дыры и сверка живого OI с архивным)."""
        sem = asyncio.Semaphore(int(self.m["fd_concurrency"]))
        await self.ls_ready.wait()                       # сначала L/S за неделю — он нужнее для балла
        n = await asyncio.gather(*(self._oi_hist_week(s, sem) for s in list(self.col.symbols)))
        log.info("OI история за %s дн.: %d точек по %d монетам", self.m.get("oi_hist_days", 7), sum(n), len(n))
        limit = int(self.m["oi_hist_limit"])
        while True:
            await asyncio.sleep(float(self.m["oi_hist_every_min"]) * 60)

            async def one(s):
                async with sem:
                    return await self._oi_hist(s, limit)
            n = await asyncio.gather(*(one(s) for s in list(self.col.symbols)))
            log.info("OI история: %d точек по %d монетам (limit=%d)", sum(n), len(n), limit)

    # ================================================================ L/S
    async def _ls_sync(self, sym: str, kind: str) -> tuple[int, bool]:
        """Догнать ряд L/S от последней точки. Возвращает (новых точек, есть ли точка за последний бар)."""
        now = now_ms()
        last = self.ls_last.get((sym, kind))
        if last is None:
            if kind == "global":
                back = int(self.m["ls_history_days"]) * DAY_BARS * BAR_MS
            else:
                back = int(float(self.m["ls_top_history_hours"]) * 12) * BAR_MS
            x = floor_bar(now) - back
        else:
            x = last + 2 * BAR_MS                        # метка X следующей точки = t + 5 мин
        # Binance отдаёт /futures/data только за последние 30 дней: более ранний startTime — ошибка -1130.
        # Если монета давно не была под наблюдением, её последняя точка старше — догоняем не от неё,
        # а от того, что ещё можно получить (у топ-трейдеров — только последние ls_top_history_hours).
        floor_ms = floor_bar(now) - 29 * DAY_BARS * BAR_MS
        if kind != "global":
            floor_ms = max(floor_ms, floor_bar(now) - int(float(self.m["ls_top_history_hours"]) * 12) * BAR_MS)
        x = max(x, floor_ms)
        got = 0
        while x <= now:
            x_end = min(x + 499 * BAR_MS, now)
            try:
                d = await self.rest.get(LS_PATHS[kind], {"symbol": sym, "period": "5m", "limit": 500,
                                                         "startTime": x, "endTime": x_end}, fd=True)
            except RestError as e:
                log.warning("%s: L/S %s: %s", sym, kind, e)
                break
            fetched = now_ms()
            rows = []
            for p in d:
                t = int(p["timestamp"]) - BAR_MS
                rows.append((sym, kind, t, float(p["longShortRatio"]), float(p.get("longAccount") or 0),
                             float(p.get("shortAccount") or 0), fetched))
            if rows:
                self.db.put(D.SQL_LS, rows)
                got += len(rows)
                tmax = max(r[2] for r in rows)
                if tmax > self.ls_last.get((sym, kind), -1):
                    self.ls_last[(sym, kind)] = tmax
            x = x_end + BAR_MS
        fresh = self.ls_last.get((sym, kind), 0) >= floor_bar(now) - BAR_MS
        return got, fresh

    async def ls_loop(self) -> None:
        """Раз в 5 мин через ls_delay_sec после закрытия бара. Первый проход — неделя истории."""
        sem = asyncio.Semaphore(int(self.m["fd_concurrency"]))
        delay = float(self.m["ls_delay_sec"]) * 1000
        first = True
        while True:
            jobs = [(s, "global") for s in self.col.symbols]
            jobs += [(s, k) for s in self.watch for k in ("top_acc", "top_pos")]

            async def one(s, k):
                async with sem:
                    return await self._ls_sync(s, k)

            res = await asyncio.gather(*(one(s, k) for s, k in jobs))
            g = [r for (s, k), r in zip(jobs, res) if k == "global"]
            pts = sum(r[0] for r in res)
            fresh = sum(1 for r in g if r[1])
            self.stat["ls_points"] += pts
            self.stat["ls_fresh"], self.stat["ls_total"] = fresh, len(g)
            if first:
                log.info("L/S история загружена: %d точек", pts)
                self.ls_ready.set()
                first = False
            now = now_ms()
            b = floor_bar(now)
            target = b + delay if now < b + delay else b + BAR_MS + delay
            await asyncio.sleep((target - now) / 1000)

    # ================================================================ стакан
    async def depth_loop(self) -> None:
        loop = asyncio.get_running_loop()
        limit = int(self.m["depth_limit"])
        weight = {5: 2, 10: 2, 20: 2, 50: 2, 100: 5, 500: 10, 1000: 20}.get(limit, 20)
        period = float(self.m["depth_every_min"]) * 60
        while True:
            syms = sorted(self.col.symbols)
            step = period / max(1, len(syms))
            for s in syms:
                t0 = loop.time()
                try:
                    d = await self.rest.get("/fapi/v1/depth", {"symbol": s, "limit": limit}, weight=weight)
                    st = depth_stats(d.get("bids", []), d.get("asks", []))
                    if st:
                        self.db.put(D.SQL_DEPTH, [(s, int(d.get("E") or now_ms()), st["mid"], st["bid"],
                                                   st["ask"], st["ask1"], st["ask2"], st["ask5"],
                                                   st["ask2_best"], st["bid1"], st["bid2"], st["bid5"],
                                                   st["ask_cov"], st["bid_cov"])])
                        self.stat["depth"] += 1
                        self.stat["depth_short"] += int(st["ask_cov"] < 2)
                except RestError as e:
                    log.warning("%s: стакан: %s", s, e)
                await asyncio.sleep(max(0.0, step - (loop.time() - t0)))

    # ================================================================ статус
    async def status_loop(self) -> None:
        every = float(self.cfg["collect"]["status_every_sec"])
        prev = dict(self.stat)
        while True:
            await asyncio.sleep(every)
            d = {k: self.stat[k] - prev.get(k, 0) for k in self.stat}
            prev = dict(self.stat)
            k, got, late = self.last_oi_bar
            log.info("OI за бар %s: %d/%d (позже %s с: %d) | быстрых OI %d, OI-истории %d | "
                     "L/S: точка за последний бар есть у %d/%d, новых точек %d | стакан %d (не дотянул до +2%%: %d) | "
                     "futures/data за 5 мин %d/%d",
                     fmt(k + BAR_MS) if k else "-", got, len(self.col.symbols), self.m["oi_late_sec"], late,
                     d["oi_fast"], d["oi_hist"], self.stat["ls_fresh"], self.stat["ls_total"], d["ls_points"],
                     d["depth"], d["depth_short"], self.rest.fd_used(), self.rest.fd_budget)
            t = now_ms()
            self.db.put(D.SQL_STATUS, [
                ("oi_last_bar", f"{k} {got} {late}", t),
                ("ls_fresh", f"{self.stat['ls_fresh']}/{self.stat['ls_total']}", t),
                ("ls_ready", str(int(self.ls_ready.is_set())), t),
                ("focus", ",".join(sorted(self.focus)), t),
                ("watch_count", str(len(self.watch)), t),
            ])
