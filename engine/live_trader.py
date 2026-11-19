"""Реальная торговля на Binance по шаблону счёта B-c (вкладка «Торговля»).

Сигнал — тот же, что у бумажного B-c: правило B (15m-искра, первая за 12 ч, подхват, L/S в нижних 20% недели,
тонкий стакан: офферы до +2% < $50k) в потоке [trade] live_stream (по умолчанию mean — среднее 20).
Размер — фиксированный ($6 по умолчанию, [trade] live_notional). Стоп — по правилам счёта: минимум искры − 0.2%
от фактической цены входа; если до него дальше 15% — сделку не берём. Выход — как у B-c.
Включается переключателем «Автоторговля B-c» (controls.trade_live = on); аварийная кнопка его выключает.

Жизнь сделки:
  подготовка — при искре (T+15) выставить по монете плечо 5× и изолированную маржу, чтобы на входе не тратить время;
  вход на T+30 — проверка (по монете нет позиции и ордеров, хватает маржи) → рыночный BUY → цена исполнения →
      стоп STOP_MARKET через algoOrder (до 3 попыток; не встал — закрыть позицию);
  трейлинг по закрытию 5m (8%, после +15% от входа — 5%): рыночный SELL reduceOnly, потом снять стоп
      (если биржа уже сняла его сама — это норма); таймаут 48 ч; «закрыть руками» с вкладки;
  стоп сработал на бирже — видно по сверке: позиции нет, а мы не выходили → выход «стоп», цена из сделок;
  перезапуск движка — открытые сделки подхватываются из базы и сверяются с биржей (стоп на месте? позиция есть?).
Результат — по сделкам биржи: сумма realizedPnl минус комиссии. Каждый шаг — в live_steps (хронология сделки).
"""
import asyncio
import logging
import time

from . import db as D
from .clock import BAR_MS, floor_bar, fmt, now_ms
from .exchange import ExchangeError

log = logging.getLogger("live")

H = 3_600_000


class LiveTrader:
    def __init__(self, cfg: dict, db: D.Db, col, ex):
        self.t = cfg.get("trade", {})
        self.p = cfg.get("paper", {})
        self.thin = float(cfg.get("thresholds", {}).get("thin_ask2", 50_000))
        self.suf = "" if self.t.get("live_stream", "mean") == "med" else "_mean"
        self.db = db
        self.col = col
        self.ex = ex
        self.enabled = ex.enabled
        self.trades: dict[int, dict] = {}          # открытые сделки по id
        self.prepped: set[str] = set()            # монеты с выставленными плечом и маржой
        self.done: set[tuple] = set()             # (монета, T) — сигналы, по которым решение уже принято
        self.loaded = False
        self.busy = asyncio.Lock()

    # ================================================================ журнал
    def _step(self, tid: int, step: str, ok: bool, detail: str = "") -> None:
        self.db.put(D.SQL_LIVE_STEP, [(tid, now_ms(), step, int(ok), detail[:1000])])
        (log.info if ok else log.warning)("реал B-c #%d — %s: %s %s", tid, step, "ок" if ok else "ОШИБКА", detail)

    def _save(self, tr: dict) -> None:
        self.db.put(D.SQL_LIVE_TRADE_UPD, [(tr["status"], tr.get("max_price"), tr.get("mclose"), tr.get("close_t"),
                                            tr.get("algo_id"), tr.get("exit_ms"), tr.get("exit_price"),
                                            tr.get("exit_reason"), tr.get("pnl_usd"), tr.get("fees_usd"), tr["id"])])

    async def _new_id(self, row: tuple) -> int:
        await self.db.flush()

        def job(conn):
            cur = conn.execute(D.SQL_LIVE_TRADE_NEW, row)
            conn.commit()
            return cur.lastrowid
        return await self.db.run_in_thread(job)

    async def load(self) -> None:
        rows = await self.db.read("SELECT id, symbol, t_spark, entry_ms, qty_s, entry, stop, stop_cid, algo_id, "
                                  "max_price, mclose, close_t FROM live_trades WHERE status='open'")
        for r in rows:
            tid, sym, T, ems, q, e, st, scid, aid, mx, mc, ct = r
            self.trades[tid] = dict(id=tid, symbol=sym, t_spark=T, entry_ms=ems, qty_s=q, entry=e, stop=st,
                                    stop_cid=scid, algo_id=aid, max_price=mx or e, mclose=mc, close_t=ct or 0,
                                    status="open", pending=None)
            self.done.add((sym, T))
        for sym, T in await self.db.read("SELECT symbol, t_spark FROM live_trades"):
            self.done.add((sym, T))
        self.loaded = True
        if self.trades:
            log.info("реал B-c: подхвачено открытых сделок %d — сверяю с биржей", len(self.trades))
            for tr in list(self.trades.values()):
                await self._reconcile(tr, after_restart=True)

    # ================================================================ проход на закрытии бара
    async def bar_pass(self, b: int) -> None:
        if not self.enabled:
            return
        if not self.loaded:
            await self.load()
        async with self.busy:
            await self._prep()
            await self._intake(b)
            await self._bars_fallback(b)

    async def _prep(self) -> None:
        """Подготовка при искре (T+15): плечо и изолированная маржа по монетам, у которых может быть вход B-c
        (первая за 12 ч, L/S в нижних 20%, тонкий стакан — до подхвата на T+30 остаётся 15 минут)."""
        since = now_ms() - 40 * 60_000
        f, cnd = ("first12_mean", "cand_mean") if self.suf else ("first12_med", "cand_med")
        rows = await self.db.read(f"SELECT DISTINCT symbol FROM sparks_15m WHERE t>=? AND {f}=1 AND in_range=1 "
                                  f"AND {cnd}=1 AND ls_pct<20 AND ask2_med<?", (since, self.thin))
        for (sym,) in rows:
            if sym not in self.prepped:
                await self._set_lev(sym)

    async def _set_lev(self, sym: str) -> bool:
        try:
            await self.ex.request("POST", "/fapi/v1/leverage", {"symbol": sym, "leverage": int(self.p.get("leverage", 5))})
            try:
                await self.ex.request("POST", "/fapi/v1/marginType", {"symbol": sym, "marginType": "ISOLATED"})
            except ExchangeError as e:
                if e.code != -4046:
                    raise
            self.prepped.add(sym)
            return True
        except ExchangeError as e:
            log.warning("реал B-c: %s — не удалось выставить плечо/маржу: %s", sym, e)
            return False

    async def _intake(self, b: int) -> None:
        on = await self.db.read("SELECT value FROM controls WHERE key='trade_live'")
        if not (on and on[0][0] == "on"):
            return
        late = float(self.p.get("late_sec", 900)) * 1000
        since = b - late - 35 * 60_000
        rows = await self.db.read(f"SELECT symbol, t, l FROM sparks_15m WHERE conf_ms IS NOT NULL AND t>=? "
                                  f"AND rB{self.suf}=1 ORDER BY t", (since,))
        for sym, T, L in rows:
            if (sym, T) in self.done:
                continue
            self.done.add((sym, T))
            await self._enter(sym, T, L)

    # ================================================================ вход
    async def _enter(self, sym: str, T: int, L: float) -> None:
        t_dec = now_ms()
        bar_close = T + 30 * 60_000
        info = self.col.symbols.get(sym) or {}
        px = (self.col.last_px.get(sym) or (0, 0))[1]
        tid = await self._new_id((sym, T, "opening", bar_close, t_dec))
        self._step(tid, "сигнал", True, f"{sym}: искра {fmt(T)}, решение через {(t_dec - bar_close) / 1000:.0f} с "
                                        f"после закрытия бара, цена {px}")

        def skip(why: str):
            self._step(tid, "пропуск", False, why)
            self.db.put(D.SQL_LIVE_TRADE_UPD, [("skipped", None, None, None, None, now_ms(), None, why[:60], None,
                                                None, tid)])

        if t_dec - bar_close > float(self.p.get("late_sec", 900)) * 1000:
            return skip("опоздание")
        if len(self.trades) >= int(self.t.get("live_max_positions", 5)):
            return skip(f"уже открыто {len(self.trades)} позиций")
        if any(tr["symbol"] == sym for tr in self.trades.values()):
            return skip("по монете уже есть сделка бота")
        if not px or not info:
            return skip("нет цены или данных монеты")
        stop0 = L - float(self.p.get("stop_pad", 0.002)) * px
        if stop0 >= px:
            return skip(f"цена {px} уже ниже стопа {stop0:.6g}")
        if (px - stop0) / px * 100 > float(self.p.get("max_stop_pct", 15)):
            return skip(f"стоп дальше 15%: {(px - stop0) / px * 100:.1f}%")
        notional = float(self.t.get("live_notional", 6.0))
        try:
            amt, oo = await asyncio.gather(self.ex.position_amt(sym),
                                           self.ex.request("GET", "/fapi/v1/openOrders", {"symbol": sym}, weight=1))
        except ExchangeError as e:
            return skip(f"проверка на бирже: {e}")
        if amt != 0 or oo:
            return skip(f"по монете на бирже уже есть позиция ({amt}) или ордера ({len(oo)}) — не трогаю")
        avail = float((self.ex.state.get("account") or {}).get("availableBalance") or 0)
        need = notional / float(self.p.get("leverage", 5)) * 1.2
        if avail < need:
            return skip(f"мало свободной маржи: ${avail:.2f} < ${need:.2f}")
        if sym not in self.prepped:
            ok = await self._set_lev(sym)
            self._step(tid, "плечо и маржа", ok, "не были выставлены заранее — выставлены перед входом" if ok else "не удалось")
            if not ok:
                return skip("плечо/маржа")
        step = info.get("step") or 0
        qty = self.ex._fmt(max(notional, float(info.get("min_notional") or 5) * 1.05) / px, step, up=True)
        if float(qty) < (info.get("min_qty") or 0):
            qty = self.ex._fmt(info["min_qty"], step, up=True)
        cid = f"tbB{tid}-in"
        t0 = time.time()
        try:
            o = await self.ex.market(sym, "BUY", qty, cid)
            o = await self.ex.wait_filled(sym, cid, o)
        except ExchangeError as e:
            return skip(f"вход не прошёл: {e}")
        if o.get("status") != "FILLED":
            self._step(tid, "вход", False, f"ордер {o.get('status')} — проверь на бирже вручную")
            return skip("вход не исполнился")
        filled = o.get("executedQty") or qty
        E = await self.ex.fill_price(sym, cid, o) or px
        t_fill = now_ms()
        self._step(tid, "вход", True, f"BUY {filled} по {E:.8g} за {(time.time() - t0) * 1000:.0f} мс "
                                      f"(цена до входа {px}, {(E / px - 1) * 100:+.3f}%)")
        self.db.put(D.SQL_EX_POS, [(sym, "B-c", tid, float(filled), E, t_fill)])
        stop = L - float(self.p.get("stop_pad", 0.002)) * E
        tr = dict(id=tid, symbol=sym, t_spark=T, entry_ms=t_fill, qty_s=filled, entry=E, stop=stop,
                  stop_cid=f"tbB{tid}-sl", algo_id=None, max_price=E, mclose=None, close_t=floor_bar(t_fill) - BAR_MS,
                  status="open", pending=None)
        self.db.put(D.SQL_LIVE_TRADE_OPEN, [(t_fill, filled, E, stop, tr["stop_cid"], tr["close_t"], tid)])
        self.trades[tid] = tr
        if not await self._place_stop(tr):
            self._step(tid, "стоп", False, "стоп не встал за все попытки — закрываю позицию")
            await self._exit(tr, "стоп не встал")

    async def _place_stop(self, tr: dict) -> bool:
        info = self.col.symbols.get(tr["symbol"]) or {}
        trig = self.ex._fmt(tr["stop"], info.get("tick") or 0)
        for att in range(1, int(self.t.get("stop_attempts", 3)) + 1):
            try:
                r = await self.ex.stop_market(tr["symbol"], "SELL", tr["qty_s"], trig, tr["stop_cid"])
                tr["algo_id"] = str(r.get("algoId"))
                self._save(tr)
                self._step(tr["id"], "стоп", True, f"попытка {att}: STOP_MARKET {tr['qty_s']} по {trig} "
                                                   f"({(float(trig) / tr['entry'] - 1) * 100:+.2f}%), algoId {tr['algo_id']}")
                return True
            except ExchangeError as e:
                self._step(tr["id"], "стоп", False, f"попытка {att}: {e}")
                if e.code == -2021:                      # «сработал бы сразу» — цена уже у стопа
                    return False
                await asyncio.sleep(1)
        return False

    # ================================================================ выход
    async def _exit(self, tr: dict, reason: str) -> None:
        """Рыночный SELL reduceOnly на весь объём, потом снять стоп (биржа может снять его сама)."""
        sym, tid = tr["symbol"], tr["id"]
        cid = f"tbB{tid}-out"
        try:
            o = await self.ex.market(sym, "SELL", tr["qty_s"], cid, reduce=True)
            o = await self.ex.wait_filled(sym, cid, o)
            px = await self.ex.fill_price(sym, cid, o) if o.get("status") == "FILLED" else 0
            self._step(tid, "выход", o.get("status") == "FILLED", f"{reason}: SELL {o.get('executedQty')} по "
                                                                   f"{f'{px:.8g}' if px else '?'}")
        except ExchangeError as e:
            self._step(tid, "выход", False, f"{reason}: {e}")
            if e.code == -2022:                          # reduceOnly отклонён — позиции уже нет (сработал стоп?)
                await self._reconcile(tr)
            return
        if tr.get("algo_id"):
            try:
                await self.ex.cancel_algo(tr["algo_id"])
                self._step(tid, "снятие стопа", True, f"algoId {tr['algo_id']}")
            except ExchangeError:
                still = True
                for _ in range(6):
                    await asyncio.sleep(0.7)
                    try:
                        still = await self.ex.open_algo(sym, tr["stop_cid"], tr["algo_id"]) is not None
                    except ExchangeError:
                        still = True
                    if not still:
                        break
                self._step(tid, "снятие стопа", not still,
                           "биржа сняла стоп сама при закрытии позиции" if not still else "стоп всё ещё открыт — сниму позже")
        await self._finish(tr, reason, px or None)

    async def _finish(self, tr: dict, reason: str, px: float | None) -> None:
        """Итог сделки по сделкам биржи: realizedPnl и комиссии с момента входа."""
        sym = tr["symbol"]
        pnl = fees = None
        try:
            trades = await self.ex.request("GET", "/fapi/v1/userTrades",
                                           {"symbol": sym, "startTime": int(tr["entry_ms"]) - 5000}, weight=5)
            mine = [x for x in trades or []]
            pnl = sum(float(x.get("realizedPnl") or 0) for x in mine)
            fees = sum(float(x.get("commission") or 0) for x in mine if x.get("commissionAsset") == "USDT")
            if not px:
                sells = [x for x in mine if x.get("side") == "SELL"]
                q = sum(float(x["qty"]) for x in sells)
                px = sum(float(x["price"]) * float(x["qty"]) for x in sells) / q if q else None
        except ExchangeError as e:
            self._step(tr["id"], "итог", False, f"сделки биржи не получены: {e}")
        tr.update(status="closed", exit_ms=now_ms(), exit_price=px, exit_reason=reason,
                  pnl_usd=(pnl - fees) if pnl is not None and fees is not None else None, fees_usd=fees)
        self._save(tr)
        self.trades.pop(tr["id"], None)
        self.db.put("DELETE FROM ex_positions WHERE symbol=? AND cmd_id=? AND kind='B-c'", [(sym, tr["id"])])
        res = f"{(px / tr['entry'] - 1) * 100:+.2f}%" if px else "?"
        self._step(tr["id"], "итог", True, f"{reason}: выход {px if px else '?'} ({res}), результат "
                                          f"{'$%.4f' % tr['pnl_usd'] if tr['pnl_usd'] is not None else '?'} с комиссией")
        self.ex.sync_now.set()

    # ================================================================ сверка и управление
    async def _reconcile(self, tr: dict, after_restart: bool = False) -> None:
        """Позиции нет на бирже, а мы не выходили → сработал стоп. Позиция есть, а стопа нет → поставить."""
        sym = tr["symbol"]
        try:
            amt = await self.ex.position_amt(sym)
        except ExchangeError as e:
            self._step(tr["id"], "сверка", False, str(e))
            return
        if amt == 0:
            self._step(tr["id"], "сверка", True, "позиции на бирже нет — сработал стоп" +
                       (" (пока движок был остановлен)" if after_restart else ""))
            await self._finish(tr, "стоп", None)
            return
        if after_restart:
            try:
                st = await self.ex.open_algo(sym, tr["stop_cid"], tr.get("algo_id"))
            except ExchangeError:
                st = None
            if st is None:
                self._step(tr["id"], "сверка", False, "после перезапуска стопа на бирже нет — ставлю заново")
                if not await self._place_stop(tr):
                    await self._exit(tr, "стоп не встал")
            else:
                self._step(tr["id"], "сверка", True, f"после перезапуска: позиция {amt}, стоп на месте")

    async def _check_stops(self) -> None:
        """Раз в несколько минут: у каждой открытой сделки стоп на бирже на месте? Если пропал (сняли в
        приложении, биржа отменила) — поставить заново (3 попытки), не встал — закрыть позицию."""
        for tr in list(self.trades.values()):
            if tr.get("exiting"):
                continue
            try:
                st = await self.ex.open_algo(tr["symbol"], tr["stop_cid"], tr.get("algo_id"))
                if st is not None:
                    continue
                amt = await self.ex.position_amt(tr["symbol"])
            except ExchangeError as e:
                log.warning("реал B-c #%d: проверка стопа не прошла: %s", tr["id"], e)
                continue
            if amt == 0:
                await self._reconcile(tr)
                continue
            self._step(tr["id"], "проверка стопа", False, "стопа на бирже нет, позиция открыта — ставлю заново")
            tr["stop_cid"] = f"tbB{tr['id']}-sl{int(time.time()) % 100000}"     # новый ID: старый мог быть «занят»
            if not await self._place_stop(tr):
                await self._exit_locked(tr, "стоп не встал")

    def on_bar_close(self, sym: str, t: int, h: float, l: float, c: float) -> None:
        """Из WS: закрылся 5m-бар — трейлинг по закрытию для реальных сделок по этой монете."""
        for tr in list(self.trades.values()):
            if tr["symbol"] == sym:
                reason = self._close_logic(tr, t, h, c)
                if reason:
                    asyncio.create_task(self._exit_locked(tr, reason))

    async def _exit_locked(self, tr: dict, reason: str) -> None:
        if tr.get("exiting"):
            return
        tr["exiting"] = True
        await self._exit(tr, reason)

    def _close_logic(self, tr: dict, t: int, h, c) -> str | None:
        if t <= (tr.get("close_t") or 0) or t + BAR_MS <= tr["entry_ms"] or c is None:
            return None
        if t >= tr["entry_ms"] and h is not None and h > tr["max_price"]:
            tr["max_price"] = h
        tr["close_t"] = t
        p1, p2 = float(self.p.get("close_p1", 0.08)), float(self.p.get("close_p2", 0.05))
        sw = 1 + float(self.p.get("close_switch", 0.15))
        p = p2 if tr["max_price"] >= tr["entry"] * sw else p1
        reason = None
        if tr["mclose"] is None:
            tr["mclose"] = c
        elif c <= tr["mclose"] * (1 - p):
            reason = "трейлинг"
        else:
            tr["mclose"] = max(tr["mclose"], c)
        self._save(tr)
        return reason

    async def _bars_fallback(self, b: int) -> None:
        """Запасной путь: закрытые 5m-бары из базы, которые не пришли по WS (или пока движок стоял)."""
        for tr in list(self.trades.values()):
            if tr.get("exiting"):
                continue
            rows = await self.db.read("SELECT t, h, c FROM raw_klines_5m WHERE symbol=? AND t>? AND t<? ORDER BY t",
                                      (tr["symbol"], tr.get("close_t") or 0, b))
            for t, h, c in rows:
                reason = self._close_logic(tr, t, h, c)
                if reason:
                    await self._exit_locked(tr, reason + " (по бару из базы)")
                    break

    async def loop(self) -> None:
        """Раз в 3 с: таймаут 48 ч, кнопка «закрыть руками», сработавший стоп (позиции нет на бирже)."""
        if not self.enabled:
            return
        while not self.loaded:
            await asyncio.sleep(1)
        hold = float(self.p.get("hold_hours", 48)) * H
        stop_every = float(self.t.get("stop_check_sec", 300))
        last_stop_check = time.time()
        while True:
            await asyncio.sleep(3)
            if self.trades and time.time() - last_stop_check > stop_every:
                last_stop_check = time.time()
                await self._check_stops()
            # сделки, закрытые извне (аварийная кнопка), — убрать из памяти
            if self.trades:
                ids = ",".join(str(i) for i in self.trades)
                open_ids = {r[0] for r in await self.db.read(f"SELECT id FROM live_trades WHERE status='open' AND id IN ({ids})")}
                for i in list(self.trades):
                    if i not in open_ids:
                        self.trades.pop(i, None)
            man = {r[0]: r[1] for r in await self.db.read(
                "SELECT symbol, id FROM ex_commands WHERE cmd='close' AND status='new'")}
            open_syms = {tr["symbol"] for tr in self.trades.values()}
            for sym, cid in man.items():
                if sym not in open_syms:
                    self.db.put("UPDATE ex_commands SET status='fail', finished_ms=? WHERE id=?", [(now_ms(), cid)])
            snap = self.ex.state
            pos_syms = {p["symbol"] for p in snap.get("positions", [])} if snap.get("ok") else None
            for tr in list(self.trades.values()):
                if tr.get("exiting"):
                    continue
                if tr["symbol"] in man:
                    self.db.put("UPDATE ex_commands SET status='ok', finished_ms=? WHERE id=?", [(now_ms(), man[tr["symbol"]])])
                    await self._exit_locked(tr, "ручной")
                elif now_ms() >= tr["entry_ms"] + hold:
                    await self._exit_locked(tr, "таймаут")
                elif pos_syms is not None and snap.get("t", 0) > tr["entry_ms"] + 3000 and tr["symbol"] not in pos_syms:
                    await self._reconcile(tr)
