"""Подключение к счёту Binance USDⓈ-M: чтение (этап 1) и тестовый цикл ордеров + аварийная кнопка (этап 2).

Что делает:
  • подписанные запросы (HMAC SHA256, метка времени с поправкой часов к Binance, recvWindow);
  • поток событий счёта: listenKey (POST /fapi/v1/listenKey, продление раз в 30 мин),
    wss://fstream.binance.com/private/ws/<listenKey> — ORDER_TRADE_UPDATE, ACCOUNT_UPDATE, ALGO_UPDATE и др.;
  • сверка с биржей: при старте, после переподключения потока и раз в минуту — баланс, позиции, открытые ордера
    и открытые условные (algo) ордера; раз в 30 мин — режим позиций (должен быть односторонний) и мультиактивы;
  • журнал: каждый подписанный запрос (ex_log), каждое событие потока (ex_events), снимок состояния (ex_state).
Источник правды — биржа: снимок в ex_state всегда тот, что вернула биржа при последней сверке.
Ключи — в secrets.toml ([binance] api_key, api_secret). Без ключей модуль выключен.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import math
import time
import zlib
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from urllib.parse import urlencode

import httpx

from . import db as D
from .clock import fmt, now_ms
from .ws import StreamConn

log = logging.getLogger("exchange")


class ExchangeError(Exception):
    def __init__(self, status: int, code, msg: str):
        super().__init__(f"HTTP {status} code {code}: {msg}")
        self.status, self.code, self.msg = status, code, msg


class Exchange:
    def __init__(self, cfg: dict, db: D.Db, rest):
        self.x = cfg.get("exchange", {})
        self.db = db
        self.rest = rest                       # общий бюджет веса запросов с публичным REST
        sec = cfg.get("_secrets", {}).get("binance", {})
        self.key, self.secret = sec.get("api_key", ""), sec.get("api_secret", "")
        self.enabled = bool(self.key and self.secret and "<" not in self.key)
        self.base = cfg["binance"]["rest_base"].rstrip("/")
        self.ws_base = self.x.get("private_ws_base", "wss://fstream.binance.com/private").rstrip("/")
        self.client = httpx.AsyncClient(base_url=self.base, timeout=httpx.Timeout(15.0, connect=10.0))
        self.listen_key: str | None = None
        self.stream: StreamConn | None = None
        self.stream_task: asyncio.Task | None = None
        self.sync_now = asyncio.Event()
        self.state: dict = {}
        self.errors = 0
        if not self.enabled:
            log.info("биржа: ключей нет (secrets.toml [binance]) — модуль торговли выключен")

    # ================================================================ запросы
    def _sign(self, params: dict) -> str:
        p = dict(params)
        p["timestamp"] = now_ms()
        p["recvWindow"] = int(self.x.get("recv_window_ms", 5000))
        qs = urlencode(p)
        sig = hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        return f"{qs}&signature={sig}"

    async def request(self, method: str, path: str, params: dict | None = None, weight: int = 1,
                      signed: bool = True):
        """Подписанный запрос. Ошибка биржи — ExchangeError с её кодом. Каждый запрос — в журнал ex_log."""
        params = params or {}
        await self.rest._reserve(weight, True)
        url = f"{path}?{self._sign(params)}" if signed else (f"{path}?{urlencode(params)}" if params else path)
        t0 = time.time()
        status, code, msg, body = 0, None, "", None
        try:
            r = await self.client.request(method, url, headers={"X-MBX-APIKEY": self.key})
            status = r.status_code
            used = r.headers.get("x-mbx-used-weight-1m")
            if used and used.isdigit():
                self.rest.used = max(self.rest.used, int(used))
            body = r.json() if r.content else {}
            if status >= 400 or (isinstance(body, dict) and isinstance(body.get("code"), int) and body["code"] < 0):
                code = body.get("code") if isinstance(body, dict) else None
                msg = body.get("msg", "") if isinstance(body, dict) else str(body)[:300]
                raise ExchangeError(status, code, msg)
            return body
        except ExchangeError:
            self.errors += 1
            raise
        except (httpx.TransportError, httpx.TimeoutException, ValueError) as e:
            self.errors += 1
            msg = repr(e)
            raise ExchangeError(status, None, msg) from e
        finally:
            self.db.put(D.SQL_EX_LOG, [(now_ms(), method, path, json.dumps(params, ensure_ascii=False)[:500], status,
                                        code, msg[:500] if msg else None, int((time.time() - t0) * 1000))])

    # ================================================================ поток событий счёта
    async def _new_listen_key(self) -> str:
        d = await self.request("POST", "/fapi/v1/listenKey", weight=1, signed=False)
        return d["listenKey"]

    def _on_event(self, stream: str, d) -> None:
        et = d.get("e") if isinstance(d, dict) else None
        self.db.put(D.SQL_EX_EVENT, [(now_ms(), et, json.dumps(d, ensure_ascii=False)[:4000])])
        if et == "listenKeyExpired":
            log.warning("биржа: listenKey истёк — переподключаю поток событий")
            asyncio.create_task(self._restart_stream())
        elif et in ("ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE", "ALGO_UPDATE", "TRADE_LITE"):
            self.sync_now.set()                  # что-то изменилось на счёте — сверка вне очереди

    async def _on_stream_connect(self) -> None:
        log.info("биржа: поток событий счёта подключён")
        self.sync_now.set()                      # после (пере)подключения — сверка

    async def _restart_stream(self) -> None:
        if self.stream_task:
            self.stream_task.cancel()
        try:
            self.listen_key = await self._new_listen_key()
        except ExchangeError as e:
            log.error("биржа: не удалось получить listenKey: %s", e)
            self.listen_key = None
            return
        self.stream = StreamConn("биржа: события", f"{self.ws_base}/ws/{self.listen_key}", self._on_event,
                                 on_connect=self._on_stream_connect,
                                 idle_timeout=float(self.x.get("user_stream_idle_sec", 3900)))
        self.stream_task = asyncio.create_task(self.stream.run(), name="user_stream")

    async def stream_loop(self) -> None:
        """Держит поток событий: listenKey продлевается раз в 30 мин; если продление не прошло — новый ключ."""
        if not self.enabled:
            return
        await self._restart_stream()
        while True:
            await asyncio.sleep(float(self.x.get("keepalive_min", 30)) * 60)
            try:
                await self.request("PUT", "/fapi/v1/listenKey", weight=1, signed=False)
            except ExchangeError as e:
                log.warning("биржа: продление listenKey не прошло (%s) — новый ключ", e)
                await self._restart_stream()

    # ================================================================ сверка с биржей
    async def sync(self, full: bool = False) -> None:
        t = now_ms()
        st: dict = {"t": t}
        try:
            acc = await self.request("GET", "/fapi/v3/account", weight=5)
            st["account"] = {k: acc.get(k) for k in ("totalWalletBalance", "totalUnrealizedProfit", "totalMarginBalance",
                                                      "availableBalance", "maxWithdrawAmount", "totalInitialMargin")}
            st["assets"] = [a for a in acc.get("assets", []) if float(a.get("walletBalance") or 0) != 0]
            st["positions"] = await self.request("GET", "/fapi/v3/positionRisk", weight=5)
            st["positions"] = [p for p in st["positions"] if float(p.get("positionAmt") or 0) != 0]
            for p in st["positions"]:                 # плечо и тип маржи по монетам с позицией
                try:
                    sc = await self.request("GET", "/fapi/v1/symbolConfig", {"symbol": p["symbol"]}, weight=5)
                    if sc:
                        p["leverage"], p["marginType"] = sc[0].get("leverage"), sc[0].get("marginType")
                except ExchangeError:
                    pass
            st["orders"] = await self.request("GET", "/fapi/v1/openOrders", weight=40)
            try:
                st["algo_orders"] = await self.request("GET", "/fapi/v1/openAlgoOrders", weight=40)
            except ExchangeError as e:
                st["algo_orders_error"] = str(e)
            if full or "mode" not in self.state:
                dual = await self.request("GET", "/fapi/v1/positionSide/dual", weight=30)
                multi = await self.request("GET", "/fapi/v1/multiAssetsMargin", weight=30)
                st["mode"] = {"dualSidePosition": dual.get("dualSidePosition"),
                              "multiAssetsMargin": multi.get("multiAssetsMargin"), "t": t}
            else:
                st["mode"] = self.state["mode"]
            st["ok"] = True
        except ExchangeError as e:
            st = dict(self.state, ok=False, error=str(e), error_t=t)
            log.warning("биржа: сверка не прошла: %s", e)
        st["stream"] = {"connected": bool(self.stream and self.stream.connected),
                        "last_msg_ms": self.stream.last_msg_ms if self.stream else 0,
                        "reconnects": self.stream.reconnects if self.stream else 0,
                        "listen_key": bool(self.listen_key)}
        st["errors"] = self.errors
        self.state = st
        self.db.put(D.SQL_EX_STATE, [("snapshot", json.dumps(st, ensure_ascii=False), now_ms())])
        mode = st.get("mode") or {}
        if mode.get("dualSidePosition") is True:
            log.error("биржа: включён хедж-режим позиций — для бота нужен односторонний (One-way)")
        if mode.get("multiAssetsMargin") is True:
            log.warning("биржа: включён мультиактивный режим маржи — для бота нужен выключенный")

    async def sync_loop(self) -> None:
        if not self.enabled:
            self.db.put(D.SQL_EX_STATE, [("snapshot", json.dumps({"enabled": False}), now_ms())])
            return
        every = float(self.x.get("sync_sec", 60))
        n = 0
        while True:
            await self.sync(full=(n % 30 == 0))
            n += 1
            try:
                await asyncio.wait_for(self.sync_now.wait(), timeout=every)
                await asyncio.sleep(1)              # события часто идут пачкой — дать им прийти
            except asyncio.TimeoutError:
                pass
            self.sync_now.clear()

    # ================================================================ ордера
    @staticmethod
    def _fmt(x: float, step: float, up: bool = False) -> str:
        """Округлить к шагу биржи (вниз, или вверх для объёма до минимума) и записать без хвостов float."""
        if not step:
            return repr(float(x))
        st = Decimal(repr(step)).normalize()
        q = (Decimal(repr(x)) / st).to_integral_value(rounding=ROUND_UP if up else ROUND_DOWN) * st
        return format(q.quantize(st) if st.as_tuple().exponent < 0 else q.quantize(Decimal(1)), "f")

    async def market(self, sym: str, side: str, qty: str, cid: str, reduce: bool = False) -> dict:
        p = {"symbol": sym, "side": side, "type": "MARKET", "quantity": qty, "newClientOrderId": cid,
             "newOrderRespType": "RESULT"}
        if reduce:
            p["reduceOnly"] = "true"
        return await self.request("POST", "/fapi/v1/order", p, weight=1)

    async def order_status(self, sym: str, cid: str) -> dict:
        return await self.request("GET", "/fapi/v1/order", {"symbol": sym, "origClientOrderId": cid}, weight=1)

    async def wait_filled(self, sym: str, cid: str, first: dict, timeout: float = 8) -> dict:
        """Ордер исполнен? Если ответ не FILLED — опрашиваем по своему ID до timeout секунд."""
        o, t_end = first, time.time() + timeout
        while o.get("status") != "FILLED" and time.time() < t_end:
            await asyncio.sleep(0.5)
            try:
                o = await self.order_status(sym, cid)
            except ExchangeError:
                pass
        return o

    async def fill_price(self, sym: str, cid: str, o: dict) -> float:
        """Средняя цена исполнения. Ответ на рыночный ордер может прийти без avgPrice (или с нулём) —
        тогда: cumQuote / executedQty из ответа → запрос ордера по своему ID → сделки по ордеру (userTrades)."""
        def from_order(d: dict) -> float:
            avg = float(d.get("avgPrice") or 0)
            if avg > 0:
                return avg
            cq, ex = float(d.get("cumQuote") or 0), float(d.get("executedQty") or 0)
            return cq / ex if cq > 0 and ex > 0 else 0.0

        avg = from_order(o)
        if avg <= 0:
            try:
                q = await self.order_status(sym, cid)
                avg = from_order(q)
                o = {**o, **q}
            except ExchangeError:
                pass
        if avg <= 0 and o.get("orderId"):
            try:
                tr = await self.request("GET", "/fapi/v1/userTrades", {"symbol": sym, "orderId": o["orderId"]}, weight=5)
                qty = sum(float(t["qty"]) for t in tr)
                avg = sum(float(t["price"]) * float(t["qty"]) for t in tr) / qty if qty else 0.0
            except ExchangeError:
                pass
        return avg

    async def stop_market(self, sym: str, side: str, qty: str, trigger: str, cid: str) -> dict:
        """Жёсткий стоп: условный STOP_MARKET через Algo Service, reduceOnly, триггер по последней цене."""
        return await self.request("POST", "/fapi/v1/algoOrder", {
            "algoType": "CONDITIONAL", "symbol": sym, "side": side, "type": "STOP_MARKET", "quantity": qty,
            "triggerPrice": trigger, "workingType": "CONTRACT_PRICE", "reduceOnly": "true", "clientAlgoId": cid,
        }, weight=1)

    async def algo_status(self, algo_id) -> dict:
        return await self.request("GET", "/fapi/v1/algoOrder", {"algoId": algo_id}, weight=1)

    async def cancel_algo(self, algo_id) -> dict:
        return await self.request("DELETE", "/fapi/v1/algoOrder", {"algoId": algo_id}, weight=1)

    async def open_algo(self, sym: str, cid: str, algo_id=None) -> dict | None:
        """Наш стоп среди открытых условных ордеров монеты (по clientAlgoId или algoId), иначе None."""
        rows = await self.request("GET", "/fapi/v1/openAlgoOrders", {"symbol": sym}, weight=1)
        for a in rows or []:
            if a.get("clientAlgoId") == cid or (algo_id is not None and str(a.get("algoId")) == str(algo_id)):
                return a
        return None

    async def position_amt(self, sym: str) -> float:
        r = await self.request("GET", "/fapi/v3/positionRisk", {"symbol": sym}, weight=5)
        return sum(float(x.get("positionAmt") or 0) for x in r)

    # ================================================================ команды из веба
    def _step(self, cmd_id: int, step: str, ok: bool, detail: str = "") -> None:
        self.db.put(D.SQL_EX_STEP, [(cmd_id, now_ms(), step, int(ok), detail[:1000])])
        (log.info if ok else log.warning)("биржа: команда %d — %s: %s %s", cmd_id, step, "ок" if ok else "ОШИБКА", detail)

    async def command_loop(self, col) -> None:
        """Команды с вкладки «Торговля» (таблица ex_commands): test — тестовый цикл, panic — аварийная кнопка."""
        if not self.enabled:
            return
        self.col = col
        while True:
            await asyncio.sleep(1)
            rows = await self.db.read("SELECT id, cmd, symbol FROM ex_commands WHERE status='new' AND cmd IN ('test','panic') "
                                      "ORDER BY id LIMIT 1")
            if not rows:
                continue
            cid, cmd, sym = rows[0]
            self.db.put("UPDATE ex_commands SET status='running', started_ms=? WHERE id=?", [(now_ms(), cid)])
            await self.db.flush()
            try:
                ok = await (self.test_cycle(cid, sym) if cmd == "test" else self.panic(cid))
            except Exception as e:                      # noqa: BLE001
                log.exception("биржа: команда %d упала", cid)
                self._step(cid, "сбой команды", False, repr(e))
                ok = False
            self.db.put("UPDATE ex_commands SET status=?, finished_ms=? WHERE id=?",
                        [("ok" if ok else "fail", now_ms(), cid)])
            self.sync_now.set()

    async def test_cycle(self, cmd_id: int, sym: str) -> bool:
        """Полный цикл на минимальный размер: вход по рынку → стоп (algo, до 3 попыток) → проверка стопа →
        пауза → выход по рынку reduceOnly → снятие стопа → проверка, что позиции и ордеров нет."""
        x = self.x
        sym = (sym or "").upper().strip()
        info = self.col.symbols.get(sym)
        if not info:
            self._step(cmd_id, "проверка", False, f"{sym}: нет такой монеты среди торгуемых USDT-перпетуалов")
            return False
        px = (self.col.last_px.get(sym) or (0, 0))[1]
        if not px:
            self._step(cmd_id, "проверка", False, "нет текущей цены из потока свечей")
            return False
        # 1. по монете не должно быть ничего чужого: позиция или ордера — не трогаем
        amt = await self.position_amt(sym)
        oo = await self.request("GET", "/fapi/v1/openOrders", {"symbol": sym}, weight=1)
        if amt != 0 or oo:
            self._step(cmd_id, "проверка", False, f"по {sym} уже есть позиция ({amt}) или ордера ({len(oo)}) — тест не запускаю")
            return False
        step, tick = info.get("step") or 0, info.get("tick") or 0
        min_n = max(float(info.get("min_notional") or 5), 5.0)
        target = max(float(x.get("test_notional", 6.0)), min_n * 1.1)
        if target > float(x.get("test_max_notional", 25.0)):
            self._step(cmd_id, "проверка", False, f"минимальная позиция ${target:.2f} больше предела теста")
            return False
        qty = self._fmt(target / px, step, up=True)
        if float(qty) < (info.get("min_qty") or 0):
            qty = self._fmt(info["min_qty"], step, up=True)
        self._step(cmd_id, "проверка", True, f"{sym}: цена {px}, объём {qty} (≈${float(qty) * px:.2f}), шаг {step}, тик {tick}")
        # 2. плечо и изолированная маржа
        try:
            await self.request("POST", "/fapi/v1/leverage", {"symbol": sym, "leverage": int(x.get("leverage", 5))})
            self._step(cmd_id, "плечо", True, f"{int(x.get('leverage', 5))}×")
        except ExchangeError as e:
            self._step(cmd_id, "плечо", False, str(e))
            return False
        try:
            await self.request("POST", "/fapi/v1/marginType", {"symbol": sym, "marginType": "ISOLATED"})
            self._step(cmd_id, "маржа", True, "ISOLATED")
        except ExchangeError as e:
            if e.code == -4046:                              # «No need to change margin type»
                self._step(cmd_id, "маржа", True, "уже ISOLATED")
            else:
                self._step(cmd_id, "маржа", False, str(e))
                return False
        base = f"tbT{cmd_id}"
        # 3. вход по рынку
        try:
            t0 = time.time()
            o = await self.market(sym, "BUY", qty, f"{base}-in")
            o = await self.wait_filled(sym, f"{base}-in", o)
        except ExchangeError as e:
            self._step(cmd_id, "вход", False, str(e))
            return False
        if o.get("status") != "FILLED":
            self._step(cmd_id, "вход", False, f"ордер не исполнился: {o.get('status')} — проверь вручную")
            return False
        ms_fill = (time.time() - t0) * 1000
        filled = o.get("executedQty") or qty
        avg = await self.fill_price(sym, f"{base}-in", o)
        self.db.put(D.SQL_EX_POS, [(sym, "TEST", cmd_id, float(filled), avg, now_ms())])
        if avg > 0:
            self._step(cmd_id, "вход", True, f"исполнено {filled} по {avg:.8g} за {ms_fill:.0f} мс "
                                             f"(цена до входа {px}, проскальзывание {(avg / px - 1) * 100:+.3f}%)")
        else:
            self._step(cmd_id, "вход", False, f"исполнено {filled}, но цену исполнения узнать не удалось — "
                                              f"стоп считаю от цены до входа {px}")
        ok = avg > 0
        base_px = avg if avg > 0 else px
        # 4. стоп: до 3 попыток, иначе закрыть позицию
        trig = self._fmt(base_px * (1 - float(x.get("test_stop_pct", 3.0)) / 100), tick)
        stop_cid, placed, algo_id = f"{base}-sl", False, None
        for att in range(1, int(x.get("stop_attempts", 3)) + 1):
            try:
                r = await self.stop_market(sym, "SELL", filled, trig, stop_cid)
                algo_id = r.get("algoId")
                placed = True
                self._step(cmd_id, "стоп", True, f"попытка {att}: STOP_MARKET {filled} по {trig}, algoId {r.get('algoId')}")
                break
            except ExchangeError as e:
                self._step(cmd_id, "стоп", False, f"попытка {att}: {e}")
                await asyncio.sleep(1)
        if placed:
            # Сервис условных ордеров отвечает с задержкой: стоп ищем в открытых условных ордерах монеты,
            # несколько раз с паузой (запрос одного ордера сразу после постановки иногда даёт «не существует»).
            a = None
            for _ in range(5):
                await asyncio.sleep(0.7)
                try:
                    a = await self.open_algo(sym, stop_cid, algo_id)
                except ExchangeError:
                    a = None
                if a:
                    break
            if a:
                self._step(cmd_id, "стоп на бирже", True, f"статус {a.get('algoStatus')}, триггер {a.get('triggerPrice')}, "
                                                          f"по {a.get('workingType')}, reduceOnly {a.get('reduceOnly')}")
            else:
                self._step(cmd_id, "стоп на бирже", False, "за 3.5 с стоп не появился в открытых условных ордерах")
                ok = False
            await asyncio.sleep(float(x.get("test_hold_sec", 5)))
        else:
            ok = False
            self._step(cmd_id, "стоп", False, "стоп не встал за все попытки — закрываю позицию")
        # 5. выход по рынку (reduceOnly), потом снять стоп
        try:
            o2 = await self.market(sym, "SELL", filled, f"{base}-out", reduce=True)
            o2 = await self.wait_filled(sym, f"{base}-out", o2)
            ex_px = await self.fill_price(sym, f"{base}-out", o2) if o2.get("status") == "FILLED" else 0
            res = f", результат {(ex_px / base_px - 1) * 100:+.3f}% без комиссии" if ex_px and base_px else ""
            self._step(cmd_id, "выход", o2.get("status") == "FILLED",
                       f"{o2.get('status')}: {o2.get('executedQty')} по {f'{ex_px:.8g}' if ex_px else '?'}{res}")
            ok &= o2.get("status") == "FILLED"
        except ExchangeError as e:
            self._step(cmd_id, "выход", False, str(e))
            ok = False
        if placed:
            # Binance сам снимает reduceOnly-стоп, когда позиция закрыта, — иногда раньше, чем мы успеваем.
            # Поэтому: снимаем по algoId; если биржа отвечает «неизвестный ордер» — проверяем, что стопа
            # действительно нет среди открытых (несколько раз с паузой).
            try:
                await self.cancel_algo(algo_id)
                self._step(cmd_id, "снятие стопа", True, f"algoId {algo_id}")
            except ExchangeError as e:
                still = True
                for _ in range(6):
                    await asyncio.sleep(0.7)
                    try:
                        still = await self.open_algo(sym, stop_cid, algo_id) is not None
                    except ExchangeError:
                        still = True
                    if not still:
                        break
                self._step(cmd_id, "снятие стопа", not still,
                           "стоп уже снят биржей вместе с закрытием позиции" if not still else f"{e}; стоп всё ещё открыт!")
                ok &= not still
        # 6. итоговая проверка
        await asyncio.sleep(1)
        amt = await self.position_amt(sym)
        algo_open = await self.request("GET", "/fapi/v1/openAlgoOrders", {"symbol": sym}, weight=1)
        mine = [a for a in (algo_open or []) if str(a.get("clientAlgoId", "")).startswith(base)]
        clean = amt == 0 and not mine
        if amt == 0:
            self.db.put("DELETE FROM ex_positions WHERE symbol=? AND cmd_id=?", [(sym, cmd_id)])
        self._step(cmd_id, "итог", clean and ok, f"позиция {amt}, наших стопов открыто {len(mine)}")
        return clean and ok

    async def panic(self, cmd_id: int) -> bool:
        """Аварийная кнопка (только то, что открыл бот): снять все ордера и стопы с нашими ID («tb…»),
        закрыть по рынку позиции из ex_positions, выключить автоторговлю."""
        self.db.put("INSERT OR REPLACE INTO controls(key, value, ms) VALUES('trade_live', 'off', ?)", [(now_ms(),)])
        ok = True
        try:
            algo = await self.request("GET", "/fapi/v1/openAlgoOrders", weight=40)
            for a in algo or []:
                if str(a.get("clientAlgoId", "")).startswith("tb"):
                    try:
                        await self.cancel_algo(a["algoId"])
                        self._step(cmd_id, "снят стоп", True, f"{a.get('symbol')} {a['clientAlgoId']}")
                    except ExchangeError as e:
                        self._step(cmd_id, "снят стоп", False, f"{a.get('symbol')}: {e}")
                        ok = False
            oo = await self.request("GET", "/fapi/v1/openOrders", weight=40)
            for o in oo or []:
                if str(o.get("clientOrderId", "")).startswith("tb"):
                    try:
                        await self.request("DELETE", "/fapi/v1/order",
                                           {"symbol": o["symbol"], "origClientOrderId": o["clientOrderId"]})
                        self._step(cmd_id, "снят ордер", True, f"{o['symbol']} {o['clientOrderId']}")
                    except ExchangeError as e:
                        self._step(cmd_id, "снят ордер", False, f"{o['symbol']}: {e}")
                        ok = False
        except ExchangeError as e:
            self._step(cmd_id, "ордера", False, str(e))
            ok = False
        for sym, kind, cmd, qty, entry, ms in await self.db.read("SELECT * FROM ex_positions"):
            try:
                amt = await self.position_amt(sym)
                if amt > 0:
                    q = self._fmt(min(amt, qty), (self.col.symbols.get(sym) or {}).get("step") or 0)
                    xid = f"tbX{cmd_id}-{zlib.crc32(sym.encode()) % 10 ** 8}"
                    o = await self.market(sym, "SELL", q, xid, reduce=True)
                    o = await self.wait_filled(sym, xid, o)
                    fp = await self.fill_price(sym, xid, o) if o.get("status") == "FILLED" else 0
                    self._step(cmd_id, "закрыта позиция", o.get("status") == "FILLED",
                               f"{sym} {o.get('executedQty')} по {f'{fp:.8g}' if fp else '?'}")
                self.db.put("DELETE FROM ex_positions WHERE symbol=? AND cmd_id=?", [(sym, cmd)])
            except ExchangeError as e:
                self._step(cmd_id, "закрыта позиция", False, f"{sym}: {e}")
                ok = False
        self.db.put("UPDATE live_trades SET status='closed', exit_reason='аварийная', exit_ms=? WHERE status='open'",
                    [(now_ms(),)])
        self._step(cmd_id, "итог", ok, "автоторговля выключена")
        return ok

    async def close(self) -> None:
        if self.stream_task:
            self.stream_task.cancel()
        if self.enabled and self.listen_key:
            try:
                await self.request("DELETE", "/fapi/v1/listenKey", weight=1, signed=False)
            except Exception:                         # noqa: BLE001
                pass
        await self.client.aclose()
        log.info("биржа: отключено (%s)", fmt(now_ms()))
