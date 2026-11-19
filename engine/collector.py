"""Этап 1: сбор сырых данных — свечи и WS-потоки.
WS: 5m-свечи всех USDT-перпетуалов, !ticker@arr, !markPrice@arr, !forceOrder@arr.
REST: exchangeInfo, догрузка истории и пропусков свечей.
OI, L/S и стакан — в metrics.py."""
import asyncio
import logging
from urllib.parse import quote

from . import db as D
from .clock import BAR_MS, DAY_BARS, floor_bar, fmt, now_ms
from .rest import Rest, RestError
from .ws import StreamConn

log = logging.getLogger("collect")


def _f(x) -> float:
    return float(x) if x not in (None, "") else 0.0


def _stream_name(s: str) -> str:
    return quote(s, safe="@!_")


class Collector:
    def __init__(self, cfg: dict, db: D.Db, rest: Rest):
        self.cfg = cfg
        self.b = cfg["binance"]
        self.c = cfg["collect"]
        self.db = db
        self.rest = rest
        self.symbols: dict[str, dict] = {}       # торгуемые USDT-перпетуалы
        self.last_t: dict[str, int] = {}         # время открытия последнего записанного бара
        self.mark: dict[str, tuple] = {}         # последний markPrice по монете
        self.partial: dict[str, tuple] = {}      # текущий незакрытый 5m-бар (t, o, h, l, c, qv, tbq)
        self.closed_at: dict[str, tuple] = {}    # последний закрытый бар из WS: (t, когда пришёл)
        self.last_px: dict[str, tuple] = {}      # последняя цена из свечей: (момент, цена)
        self._px_written: dict[str, int] = {}
        self.bar_listeners: list = []            # f(symbol, t, h, l, c) на закрытии 5m-бара (бот v0.2)
        self.kline_conns: list[StreamConn] = []
        self.kline_tasks: list[asyncio.Task] = []
        self.misc_conn: StreamConn | None = None
        self.backfill_done = asyncio.Event()
        self.filling: set[str] = set()
        self.stat = dict(ws_bars=0, rest_bars=0, tickers=0, marks=0, liqs=0, gaps=0)

    # ================================================================ вселенная
    async def refresh_universe(self) -> bool:
        info = await self.rest.exchange_info()
        t = now_ms()
        rows, new = [], {}
        for s in info.get("symbols", []):
            if s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != self.b["quote_asset"]:
                continue
            f = {x["filterType"]: x for x in s.get("filters", [])}
            d = dict(
                base=s.get("baseAsset"),
                status=s.get("status"),
                onboard=int(s.get("onboardDate") or 0),
                tick=_f(f.get("PRICE_FILTER", {}).get("tickSize")),
                step=_f(f.get("LOT_SIZE", {}).get("stepSize")),
                min_qty=_f(f.get("LOT_SIZE", {}).get("minQty")),
                min_notional=_f(f.get("MIN_NOTIONAL", {}).get("notional")),
            )
            rows.append((s["symbol"], d["base"], d["status"], d["onboard"], d["tick"], d["step"],
                         d["min_qty"], d["min_notional"], t))
            if d["status"] == "TRADING":
                new[s["symbol"]] = d
        self.db.put(D.SQL_SYMBOL, rows)
        # Интервал funding: у большинства 8 ч, у части монет Binance его сократил (fundingInfo).
        hours = {}
        try:
            for x in await self.rest.get("/fapi/v1/fundingInfo", weight=1):
                if x.get("fundingIntervalHours"):
                    hours[x["symbol"]] = float(x["fundingIntervalHours"])
        except RestError as e:
            log.warning("fundingInfo не получен, считаю интервал 8 ч: %s", e)
        for sym, d in new.items():
            d["funding_h"] = hours.get(sym, 8.0)
        self.db.put(D.SQL_FUNDING_H, [(d["funding_h"], sym) for sym, d in new.items()])
        changed = set(new) != set(self.symbols)
        added = set(new) - set(self.symbols)
        removed = set(self.symbols) - set(new)
        self.symbols = new
        if changed:
            log.info("вселенная: %d монет (+%d / -%d)", len(new), len(added), len(removed))
        return changed

    async def load_last_t(self) -> None:
        """Последний бар по каждой монете. Запрос по монете (поиск по индексу) — мгновенно даже на большой базе;
        GROUP BY по всей таблице читал бы все строки (на медленном диске — минуты)."""
        log.info("читаю из базы последние бары по монетам…")

        def job(conn):
            syms = [r[0] for r in conn.execute("SELECT symbol FROM symbols")] or list(self.symbols)
            out = []
            for s in set(syms) | set(self.symbols):
                t = conn.execute("SELECT MAX(t) FROM raw_klines_5m WHERE symbol=?", (s,)).fetchone()[0]
                if t is not None:
                    out.append((s, t))
            return out
        for sym, t in await self.db.run_in_thread(job):
            self.last_t[sym] = t
        log.info("в базе уже есть свечи по %d монетам", len(self.last_t))

    # ================================================================ WS
    def _chunks(self) -> list[list[str]]:
        n = int(self.b["streams_per_connection"])
        ascii_syms = sorted(s for s in self.symbols if s.isascii())
        other = sorted(s for s in self.symbols if not s.isascii())
        chunks = [ascii_syms[i:i + n] for i in range(0, len(ascii_syms), n)]
        # Монеты с не-ASCII именами — отдельным соединением, чтобы их сбой не ронял остальные.
        chunks += [other[i:i + n] for i in range(0, len(other), n)]
        return chunks

    def start_kline_streams(self) -> None:
        for t in self.kline_tasks:
            t.cancel()
        self.kline_conns, self.kline_tasks = [], []
        base = self.b["ws_market_base"].rstrip("/")
        for k, chunk in enumerate(self._chunks()):
            url = base + "/stream?streams=" + "/".join(_stream_name(f"{s.lower()}@kline_5m") for s in chunk)
            conn = StreamConn(f"klines#{k + 1}", url, self.on_kline,
                              on_connect=lambda ch=chunk: self.catch_up(ch, "переподключение"),
                              idle_timeout=self.b["ws_idle_timeout_sec"])
            self.kline_conns.append(conn)
            self.kline_tasks.append(asyncio.create_task(conn.run(), name=conn.name))
        log.info("свечи: %d соединений по %d монетам", len(self.kline_conns), len(self.symbols))

    def start_misc_stream(self) -> asyncio.Task:
        base = self.b["ws_market_base"].rstrip("/")
        # !ticker@arr больше не используется (Binance помечает его устаревшим): последняя цена берётся из
        # незакрытых 5m-свечей, которые и так приходят по всем монетам несколько раз в секунду.
        url = base + "/stream?streams=!markPrice@arr/!forceOrder@arr"
        self.misc_conn = StreamConn("mark+liq", url, self.on_misc,
                                    idle_timeout=self.b["ws_idle_timeout_sec"])
        return asyncio.create_task(self.misc_conn.run(), name="misc")

    def on_kline(self, stream: str, d: dict) -> None:
        k = d.get("k") or {}
        sym, t = d["s"], int(k["t"])
        self.last_px[sym] = (int(d.get("E") or now_ms()), float(k["c"]))   # последняя цена — для ticker_latest
        if not k.get("x"):
            # Незакрытый бар — только в память, для быстрых живых тегов монет «в фокусе». В базу не пишется.
            self.partial[sym] = (t, float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]),
                                 float(k["q"]), float(k["Q"]))
            return
        self.db.put(D.SQL_KLINE, [(sym, t, float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]),
                                   float(k["v"]), float(k["q"]), int(k["n"]),
                                   float(k["V"]), float(k["Q"]), 0)])
        if t > self.last_t.get(sym, 0):
            self.last_t[sym] = t
        self.closed_at[sym] = (t, now_ms())
        self.stat["ws_bars"] += 1
        for f in self.bar_listeners:
            try:
                f(sym, t, float(k["h"]), float(k["l"]), float(k["c"]))
            except Exception:
                log.exception("ошибка обработчика закрытия бара")

    async def prices_loop(self) -> None:
        """Раз в 2 с — последние цены всех монет в ticker_latest (для цен на вебе и цены ручного закрытия)."""
        while True:
            await asyncio.sleep(2)
            rows = [(s, t, px, None, None) for s, (t, px) in self.last_px.items() if self._px_written.get(s) != t]
            for r in rows:
                self._px_written[r[0]] = r[1]
            self.db.put(D.SQL_TICKER, rows)
            self.stat["tickers"] += len(rows)

    def on_misc(self, stream: str, d) -> None:
        if stream.startswith("!ticker"):
            rows = [(x["s"], int(x["E"]), _f(x["c"]), _f(x["q"]), _f(x["P"])) for x in d]
            self.db.put(D.SQL_TICKER, rows)
            self.stat["tickers"] += len(rows)
        elif stream.startswith("!markPrice"):
            for x in d:
                self.mark[x["s"]] = (_f(x.get("r")), _f(x.get("p")), _f(x.get("i")), int(x.get("T") or 0))
            self.stat["marks"] += len(d)
        elif stream.startswith("!forceOrder"):
            o = d["o"]
            self.db.put(D.SQL_LIQ, [(o["s"], int(o["T"]), o["S"], _f(o["p"]), _f(o["ap"]),
                                     _f(o["q"]), _f(o["z"]), o["X"])])
            self.stat["liqs"] += 1

    # ================================================================ догрузка
    def _gap(self, sym: str, a: int, b: int, reason: str) -> None:
        self.db.put(D.SQL_GAP, [(sym, "kline", a, b, reason, now_ms())])
        self.stat["gaps"] += 1
        log.warning("%s: дыра %s … %s (%s)", sym, fmt(a), fmt(b), reason)

    async def fill(self, sym: str, start: int, end: int) -> int:
        """Догрузить через REST бары с открытием в [start, end). Возвращает число баров."""
        info = self.symbols.get(sym)
        if info and info["onboard"]:
            start = max(start, floor_bar(info["onboard"]))
        cur, got = start, 0
        while cur < end:
            limit = min(1500, (end - cur) // BAR_MS)
            if limit <= 0:
                break
            try:
                rows = await self.rest.klines(sym, cur, limit)
            except RestError as e:
                log.warning("%s: REST не отдал свечи: %s", sym, e)
                return got
            now = now_ms()
            good = [r for r in rows if cur <= int(r[0]) < end and int(r[0]) + BAR_MS <= now]
            if not good:
                if now - end > 60_000:              # свежий бар мог ещё не появиться в REST
                    self._gap(sym, cur, end, "rest_empty")
                break
            expected = cur
            out = []
            for r in good:
                t = int(r[0])
                if t > expected:
                    self._gap(sym, expected, t, "rest_hole")
                expected = t + BAR_MS
                out.append((sym, t, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]),
                            float(r[7]), int(r[8]), float(r[9]), float(r[10]), 1))
            self.db.put(D.SQL_KLINE, out)
            got += len(out)
            self.stat["rest_bars"] += len(out)
            last = int(good[-1][0])
            if last > self.last_t.get(sym, 0):
                self.last_t[sym] = last
            cur = last + BAR_MS
            if len(rows) < limit and cur < end:
                if now - end > 60_000:
                    self._gap(sym, cur, end, "rest_short")
                break
        return got

    async def catch_up(self, syms: list[str], why: str) -> None:
        """Догнать монеты от последнего записанного бара до текущей границы."""
        if not self.backfill_done.is_set():
            return                                  # стартовая догрузка сделает это сама
        end = floor_bar(now_ms())
        tasks = []
        for s in syms:
            if s in self.filling:
                continue
            start = self.last_t[s] + BAR_MS if s in self.last_t else end - DAY_BARS * BAR_MS
            if start < end:
                tasks.append(self._fill_guarded(s, start, end))
        if tasks:
            res = await asyncio.gather(*tasks)
            log.info("догрузка (%s): %d монет, %d баров", why, len(tasks), sum(res))

    async def _fill_guarded(self, s: str, start: int, end: int, sem: asyncio.Semaphore | None = None) -> int:
        self.filling.add(s)
        try:
            if sem:
                async with sem:
                    return await self.fill(s, start, end)
            return await self.fill(s, start, end)
        finally:
            self.filling.discard(s)

    async def initial_backfill(self) -> None:
        end = floor_bar(now_ms())
        hist_start = end - int(self.c["history_days_on_start"]) * DAY_BARS * BAR_MS
        plan = {s: max(hist_start, self.last_t[s] + BAR_MS if s in self.last_t else hist_start)
                for s in self.symbols}
        plan = {s: a for s, a in plan.items() if a < end}
        total_bars = sum((end - a) // BAR_MS for a in plan.values())
        log.info("стартовая догрузка: %d монет, до %d баров", len(plan), total_bars)
        sem = asyncio.Semaphore(int(self.c["backfill_concurrency"]))
        done = 0

        async def one(s, a):
            nonlocal done
            n = await self._fill_guarded(s, a, end, sem)
            done += 1
            if done % 50 == 0 or done == len(plan):
                log.info("догрузка: %d / %d монет", done, len(plan))
            return n

        res = await asyncio.gather(*(one(s, a) for s, a in plan.items()))
        log.info("стартовая догрузка завершена: %d баров", sum(res))
        self.backfill_done.set()
        await self.catch_up(list(self.symbols), "после стартовой")

    # ================================================================ фоновые циклы
    async def reconcile_loop(self) -> None:
        """После каждого закрытия бара: у кого нет последнего бара — догрузить через REST."""
        await self.backfill_done.wait()
        delay = float(self.c["reconcile_delay_sec"]) * 1000
        while True:
            now = now_ms()
            b = floor_bar(now)
            target = b + delay if now < b + delay else b + BAR_MS + delay
            await asyncio.sleep((target - now) / 1000)
            closed = floor_bar(now_ms()) - BAR_MS    # открытие последнего закрытого бара
            lag = [s for s in self.symbols if self.last_t.get(s, 0) < closed]
            if lag:
                log.info("не пришёл бар %s у %d монет", fmt(closed), len(lag))
                await self.catch_up(lag, "сверка")

    async def hole_scan_loop(self) -> None:
        await self.backfill_done.wait()
        while True:
            try:
                await self.hole_scan()
            except Exception:
                log.exception("ошибка проверки дыр")
            await asyncio.sleep(float(self.c["hole_scan_every_min"]) * 60)

    async def hole_scan(self) -> None:
        end = floor_bar(now_ms()) - BAR_MS                 # последний бар оставляем сверке
        since = end - int(self.c["hole_scan_hours"]) * 12 * BAR_MS
        syms = list(self.symbols)

        def job(conn):
            have = {}
            for s in syms:
                have[s] = {r[0] for r in conn.execute(
                    "SELECT t FROM raw_klines_5m WHERE symbol=? AND t>=? AND t<?", (s, since, end))}
            gaps = conn.execute("SELECT symbol,start_ms,end_ms FROM gaps WHERE kind='kline' AND end_ms>?",
                                (since,)).fetchall()
            return have, gaps

        have, gaps = await self.db.run_in_thread(job)
        known: dict[str, list] = {}
        for s, a, b in gaps:
            known.setdefault(s, []).append((a, b))
        ranges = []
        for s in syms:
            info = self.symbols.get(s)
            start = max(since, floor_bar(info["onboard"]) if info and info["onboard"] else since)
            run_a = None
            for t in range(start, end, BAR_MS):
                missing = t not in have[s] and not any(a <= t < b for a, b in known.get(s, ()))
                if missing and run_a is None:
                    run_a = t
                elif not missing and run_a is not None:
                    ranges.append((s, run_a, t))
                    run_a = None
            if run_a is not None:
                ranges.append((s, run_a, end))
        if not ranges:
            log.info("дыр в свечах за %s ч нет", self.c["hole_scan_hours"])
            return
        log.info("найдено дыр: %d, догружаю", len(ranges))
        sem = asyncio.Semaphore(int(self.c["backfill_concurrency"]))
        got = await asyncio.gather(*(self._fill_guarded(s, a, b, sem) for s, a, b in ranges))
        log.info("дыры: догружено %d баров", sum(got))

    async def funding_loop(self) -> None:
        """Снимок funding/mark на каждой границе 5m."""
        while True:
            now = now_ms()
            nxt = floor_bar(now) + BAR_MS
            await asyncio.sleep((nxt - now) / 1000 + 2)
            rows = [(s, nxt, *v) for s, v in self.mark.items() if s in self.symbols]
            self.db.put(D.SQL_FUNDING, rows)

    async def universe_loop(self) -> None:
        while True:
            await asyncio.sleep(float(self.c["universe_refresh_hours"]) * 3600)
            try:
                old = set(self.symbols)
                if await self.refresh_universe():
                    self.start_kline_streams()
                    added = list(set(self.symbols) - old)
                    if added:
                        await self.catch_up(added, "новые монеты")
            except Exception:
                log.exception("ошибка обновления вселенной")

    async def retention_loop(self) -> None:
        days = int(self.c["retention_days"])
        while True:
            await asyncio.sleep(3600)
            cutoff = now_ms() - days * 86_400_000

            def job(conn):
                syms = [r[0] for r in conn.execute("SELECT symbol FROM symbols")]
                n = 0
                for s in syms:
                    for tbl in D.RAW_TABLES:
                        n += conn.execute(f"DELETE FROM {tbl} WHERE symbol=? AND t<?", (s, cutoff)).rowcount
                    n += conn.execute("DELETE FROM raw_oi_fast WHERE symbol=? AND t_ms<?", (s, cutoff)).rowcount
                n += conn.execute("DELETE FROM live_tags WHERE t_ms<?", (cutoff,)).rowcount
                n += conn.execute("DELETE FROM ex_log WHERE ms<?", (cutoff,)).rowcount
                n += conn.execute("DELETE FROM ex_events WHERE ms<?", (cutoff,)).rowcount
                n += conn.execute("DELETE FROM raw_liquidations WHERE t<?", (cutoff,)).rowcount
                return n

            try:
                n = await self.db.run_in_thread(job)
                if n:
                    log.info("чистка: удалено %d строк старше %d дней", n, days)
            except Exception:
                log.exception("ошибка чистки")
            await asyncio.sleep(23 * 3600)

    async def status_loop(self) -> None:
        every = float(self.c["status_every_sec"])
        prev = dict(self.stat)
        while True:
            await asyncio.sleep(every)
            up = sum(c.connected for c in self.kline_conns)
            misc_up = bool(self.misc_conn and self.misc_conn.connected)
            last = max(self.last_t.values(), default=0)
            closed = floor_bar(now_ms()) - BAR_MS
            fresh = sum(1 for s in self.symbols if self.last_t.get(s, 0) >= closed)
            d = {k: self.stat[k] - prev.get(k, 0) for k in self.stat}
            prev = dict(self.stat)
            log.info("WS свечи %d/%d, тикеры %s | монет %d, с последним баром %d | за %.0f с: "
                     "WS-баров %d, REST-баров %d, тикеров %d, mark %d, ликвидаций %d, дыр %d | "
                     "вес REST %d/%d, в очереди записи %d",
                     up, len(self.kline_conns), "да" if misc_up else "НЕТ",
                     len(self.symbols), fresh, every, d["ws_bars"], d["rest_bars"], d["tickers"],
                     d["marks"], d["liqs"], d["gaps"], self.rest.used, self.rest.budget,
                     self.db.pending_rows())
            t = now_ms()
            self.db.put(D.SQL_STATUS, [
                ("heartbeat", fmt(t), t),
                ("ws_klines_up", f"{up}/{len(self.kline_conns)}", t),
                ("ws_misc_up", str(int(misc_up)), t),
                ("symbols", str(len(self.symbols)), t),
                ("symbols_fresh", str(fresh), t),
                ("last_bar_t", str(last), t),
                ("backfill_done", str(int(self.backfill_done.is_set())), t),
                ("config", f"v{self.cfg['meta']['config_version']} {self.cfg['_hash']}", t),
            ])
