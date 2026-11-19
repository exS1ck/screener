"""Связь движка с пультом в Telegram (сервер remote_bot на Render).

  • раз в минуту — «я жив» с состоянием: автоторговля, открытые реальные позиции, сверка с биржей, итог дня;
  • события реальной торговли — по хронологии сделок (live_steps): вход, выход с результатом, сбои шагов;
    плюс аварийная кнопка и жёсткий перезапуск движка;
  • каждые 3 с забирает команды с сервера (close / panic / on / off), выполняет и отвечает результатом.
Все запросы подписаны HMAC-SHA256 общим секретом. Адрес и секрет — secrets.toml, раздел [remote].
Сервер ничего не знает о бирже: команды исполняет движок теми же путями, что и вкладка «Торговля».
"""
import asyncio
import hashlib
import hmac
import html
import json
import logging
import secrets
import time

import httpx

from . import db as D
from .clock import BAR_MS, fmt, now_ms

log = logging.getLogger("remote")
DAY = 86_400_000


def esc(x) -> str:
    return html.escape(str(x), quote=False)


class Remote:
    def __init__(self, cfg: dict, db: D.Db, col, ex, live):
        sec = cfg.get("_secrets", {}).get("remote", {})
        self.url = str(sec.get("url", "")).rstrip("/")
        self.secret = str(sec.get("secret", "")).encode()
        self.enabled = bool(self.url and self.secret and "<" not in self.url)
        self.db, self.col, self.ex, self.live = db, col, ex, live
        self.notional = cfg.get("trade", {}).get("live_notional", 6)
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(40.0, connect=15.0))   # холодный старт Render
        if not self.enabled:
            log.info("пульт Telegram: нет [remote] url/secret в secrets.toml — выключен")

    # ---------------------------------------------------------------- запросы
    async def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body, ensure_ascii=False) if body is not None else ""
        ts, nonce = str(int(time.time())), secrets.token_hex(16)
        sg = hmac.new(self.secret, f"{ts}.{nonce}.{method}.{path}.{data}".encode(), hashlib.sha256).hexdigest()
        r = await self.client.request(method, self.url + path, content=data.encode() if data else None,
                                      headers={"X-TS": ts, "X-Nonce": nonce, "X-Sig": sg,
                                               "Content-Type": "application/json"})
        r.raise_for_status()
        return r.json()

    async def event(self, text: str, kind: str = "info", trade_id: int | None = None) -> None:
        try:
            await self.call("POST", "/engine/push", {"type": "event", "kind": kind, "text": text, "trade_id": trade_id})
        except Exception as e:                                   # noqa: BLE001
            log.warning("пульт: событие не отправлено: %r", e)

    # ---------------------------------------------------------------- «я жив»
    async def state(self) -> dict:
        ctl = await self.db.read("SELECT value FROM controls WHERE key='trade_live'")
        day0 = now_ms() - now_ms() % DAY
        today = await self.db.read("SELECT COUNT(*), COALESCE(SUM(pnl_usd), 0) FROM live_trades "
                                   "WHERE status='closed' AND exit_ms>=?", (day0,))
        pos = []
        for tr in list(self.live.trades.values()):
            px = (self.col.last_px.get(tr["symbol"]) or (0, 0))[1]
            pos.append(dict(id=tr["id"], symbol=tr["symbol"], qty=tr["qty_s"], entry=round(tr["entry"], 8),
                            pnl_pct=round((px / tr["entry"] - 1) * 100, 2) if px and tr["entry"] else None))
        return dict(auto=bool(ctl and ctl[0][0] == "on"), positions=pos, ex_ok=bool(self.ex.state.get("ok")),
                    today_n=today[0][0] if today else 0, today_pnl=round(today[0][1], 4) if today else 0,
                    notional=self.notional)

    async def hb_loop(self) -> None:
        if not self.enabled:
            return
        while True:
            try:
                await self.call("POST", "/engine/push", {"type": "heartbeat", "state": await self.state()})
            except Exception as e:                               # noqa: BLE001
                log.warning("пульт: «я жив» не отправлен: %r", e)
            await asyncio.sleep(60)

    # ---------------------------------------------------------------- события
    async def _last(self, key: str) -> int:
        r = await self.db.read("SELECT value FROM engine_status WHERE key=?", (key,))
        return int(r[0][0]) if r else -1

    def _set_last(self, key: str, v: int) -> None:
        self.db.put(D.SQL_STATUS, [(key, str(v), now_ms())])

    async def events_loop(self) -> None:
        if not self.enabled:
            return
        last = await self._last("tg_last_step")
        if last < 0:                       # первый запуск — не пересылать старую историю
            r = await self.db.read("SELECT COALESCE(MAX(id), 0) FROM live_steps")
            last = r[0][0]
        last_cmd = await self._last("tg_last_cmd")
        if last_cmd < 0:
            r = await self.db.read("SELECT COALESCE(MAX(id), 0) FROM ex_commands")
            last_cmd = r[0][0]
        last_reset = await self._last("tg_last_reset")
        while True:
            await asyncio.sleep(3)
            try:
                rows = await self.db.read("SELECT s.id, s.trade_id, s.step, s.ok, s.detail, t.symbol, t.entry, t.stop, "
                                          "t.qty_s, t.exit_price, t.exit_reason, t.pnl_usd, t.entry_ms, t.exit_ms, "
                                          "t.bar_close_ms FROM live_steps s JOIN live_trades t ON t.id=s.trade_id "
                                          "WHERE s.id>? ORDER BY s.id", (last,))
                for (sid, tid, step, ok, detail, sym, entry, stop, qty, exit_px, reason, pnl, ems, xms, bcm) in rows:
                    await self._step_event(tid, step, ok, detail, sym, entry, stop, qty, exit_px, reason, pnl, ems, xms, bcm)
                    last = sid
                if rows:
                    self._set_last("tg_last_step", last)
                cmds = await self.db.read("SELECT id, cmd, status FROM ex_commands WHERE id>? AND cmd='panic' "
                                          "AND status IN ('ok','fail') ORDER BY id", (last_cmd,))
                for cid, cmd, st in cmds:
                    await self.event(f"⛔ Аварийная остановка торговли выполнена: {'успешно' if st == 'ok' else 'С ОШИБКАМИ — проверь вкладку «Торговля»'}.")
                    last_cmd = cid
                if cmds:
                    self._set_last("tg_last_cmd", last_cmd)
                hr = await self.db.read("SELECT value, t FROM engine_status WHERE key='hard_reset'")
                if hr and hr[0][1] > last_reset:
                    if last_reset >= 0:
                        await self.event(f"♻️ Жёсткий перезапуск движка: {esc(hr[0][0])}")
                    last_reset = hr[0][1]
                    self._set_last("tg_last_reset", last_reset)
            except Exception:                                    # noqa: BLE001
                log.exception("пульт: ошибка событий")

    async def _step_event(self, tid, step, ok, detail, sym, entry, stop, qty, exit_px, reason, pnl, ems, xms, bcm):
        s = f"<b>{esc(sym)}</b>"
        if step == "вход" and ok:
            sp = f" ({(stop / entry - 1) * 100:+.1f}%)" if stop and entry else ""
            delay = f", через {(ems - bcm) / 1000:.0f} с после сигнала" if ems and bcm else ""
            await self.event(f"🟢 Вход {s}: {esc(qty)} по {entry:.8g}, стоп {stop:.8g}{sp}{delay}", "open", tid)
        elif step == "итог" and ok and reason:
            res = f"{(exit_px / entry - 1) * 100:+.2f}%" if exit_px and entry else "?"
            dur = f", {((xms or 0) - (ems or 0)) / 3_600_000:.1f} ч" if xms and ems else ""
            usd = f"${pnl:+.4f}" if pnl is not None else "$?"
            icon = "✅" if (pnl or 0) > 0 else "🔻"
            await self.event(f"{icon} Выход {s}: {esc(reason)}, {entry:.8g} → {exit_px:.8g} ({res}), "
                             f"результат {usd} с комиссией{dur}" if exit_px else
                             f"{icon} Выход {s}: {esc(reason)}, результат {usd}", "close", tid)
        elif not ok and step not in ("пропуск",):
            await self.event(f"❗ {s}: {esc(step)} — {esc(detail)}", "error", tid)

    # ---------------------------------------------------------------- команды
    async def cmd_loop(self) -> None:
        if not self.enabled:
            return
        while True:
            await asyncio.sleep(3)
            try:
                d = await self.call("GET", "/engine/commands")
            except Exception as e:                               # noqa: BLE001
                log.debug("пульт: команды не получены: %r", e)
                continue
            for c in d.get("commands", []):
                ok, text = await self.execute(str(c.get("cmd")), str(c.get("arg") or ""))
                try:
                    await self.call("POST", "/engine/ack", {"id": c.get("id"), "ok": ok, "text": text})
                except Exception as e:                           # noqa: BLE001
                    log.warning("пульт: ответ на команду не отправлен: %r", e)

    async def execute(self, cmd: str, arg: str) -> tuple[bool, str]:
        log.info("пульт: команда %s %s", cmd, arg)
        self.db.put(D.SQL_STATUS, [("tg_last_command", f"{cmd} {arg}", now_ms())])
        if cmd == "close":
            if not arg.isdigit():
                return False, "неверный номер сделки"
            tr = self.live.trades.get(int(arg))
            if not tr:
                return False, "эта позиция уже закрыта"
            self.db.put("INSERT INTO ex_commands(ms, cmd, symbol, status) VALUES(?, 'close', ?, 'new')",
                        [(now_ms(), tr["symbol"])])
            return True, f"закрываю {esc(tr['symbol'])} по рынку — итог придёт отдельным сообщением"
        if cmd == "panic":
            self.db.put("INSERT INTO ex_commands(ms, cmd, symbol, status) VALUES(?, 'panic', NULL, 'new')", [(now_ms(),)])
            return True, "аварийная остановка запущена — итог придёт отдельным сообщением"
        if cmd in ("on", "off"):
            self.db.put("INSERT OR REPLACE INTO controls(key, value, ms) VALUES('trade_live', ?, ?)", [(cmd, now_ms())])
            return True, f"автоторговля {'включена' if cmd == 'on' else 'выключена'}"
        return False, "неизвестная команда"

    async def close(self) -> None:
        await self.client.aclose()
