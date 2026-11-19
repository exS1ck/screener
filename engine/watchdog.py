"""Сторож движка: синхронизация часов с Binance и «жёсткий перезапуск».

Часы: раз в clock_sync_min минут движок заново меряет расхождение локальных часов с сервером Binance и
обновляет поправку, которой пользуется везде (now_ms). Системные часы Windows не трогаются — для этого нужны
права администратора; если расхождение больше 2 с, в лог пишется предупреждение.

Жёсткий перезапуск: сбои копятся в окне window_min минут —
  • внеплановое переподключение WS-потока (свечи, тикеры/mark/ликвидации, aggTrade); плановый разрыв Binance
    раз в 24 ч не считается, несколько потоков за одну проверку — один сбой;
  • у большинства монет нет последнего 5m-бара через 90 с после закрытия;
  • живой OI за бар получен меньше чем у половины монет;
  • поток mark price (бывший поток тикеров) молчит дольше 2 мин.
Если сбоев набралось max_events и с прошлого перезапуска прошло не меньше min_interval_min, движок аккуратно
останавливается (очередь записи сбрасывается, соединения закрываются) и выходит с кодом 3. Заново его запускает
run_forever.py — как если бы ты остановил и запустил его руками.
"""
import asyncio
import logging
from collections import deque

from . import db as D
from .clock import BAR_MS, floor_bar, fmt, get_offset, now_ms, set_offset

log = logging.getLogger("watchdog")
RESET_CODE = 3


class Watchdog:
    def __init__(self, cfg: dict, db: D.Db, col, met, rest, bot=None):
        self.w = cfg.get("watchdog", {})
        self.db = db
        self.col = col
        self.met = met
        self.rest = rest
        self.bot = bot
        self.reset = asyncio.Event()
        self.reason = ""
        self.events: deque = deque()

    # ---------------------------------------------------------------- часы
    async def clock_loop(self) -> None:
        every = float(self.w.get("clock_sync_min", 30)) * 60
        while True:
            await asyncio.sleep(every)
            try:
                off = await self.rest.server_offset_ms()
            except Exception as e:                                   # noqa: BLE001
                log.warning("часы: не удалось сверить с Binance: %r", e)
                continue
            delta = off - get_offset()
            set_offset(off)
            self.db.put(D.SQL_STATUS, [("clock_offset", str(off), now_ms())])
            if abs(off) > 2000:
                log.warning("часы: локальное время отстаёт/спешит на %+d мс — синхронизируй время Windows", off)
            elif abs(delta) > 250:
                log.info("часы: поправка к Binance %+d мс (изменилась на %+d мс)", off, delta)

    # ---------------------------------------------------------------- сбои
    def _event(self, what: str) -> None:
        self.events.append((now_ms(), what))
        log.warning("сторож: сбой — %s", what)

    def _conns(self) -> list:
        cs = list(self.col.kline_conns)
        if self.col.misc_conn:
            cs.append(self.col.misc_conn)
        if self.bot is not None and self.bot.conn is not None:
            cs.append(self.bot.conn)
        return cs

    async def loop(self) -> None:
        await self.col.backfill_done.wait()
        every = float(self.w.get("check_sec", 60))
        window = float(self.w.get("window_min", 15)) * 60_000
        max_ev = int(self.w.get("max_events", 3))
        min_gap = float(self.w.get("min_interval_min", 30)) * 60_000
        fresh_min = float(self.w.get("fresh_min_share", 0.5))
        seen_rec = {id(c): c.reconnects - c.planned for c in self._conns()}
        last_oi_bar = None
        misc_down_since = None
        last_fresh_bar = None
        while True:
            await asyncio.sleep(every)
            now = now_ms()
            # 1) внеплановые переподключения WS (плановый разрыв раз в 24 ч не считаем); несколько потоков
            #    за одну проверку — один сбой
            names = []
            for c in self._conns():
                cur = c.reconnects - c.planned
                was = seen_rec.get(id(c), cur)
                if cur > was:
                    names.append(f"{c.name} ×{cur - was}")
                seen_rec[id(c)] = cur
            if names:
                self._event("переподключение " + ", ".join(names))
            # 2) свежесть 5m-баров: через 90 с после закрытия бара
            b = floor_bar(now)
            if now - b > 90_000 and b != last_fresh_bar and self.col.symbols:
                last_fresh_bar = b
                closed = b - BAR_MS
                fresh = sum(1 for s in self.col.symbols if self.col.last_t.get(s, 0) >= closed)
                if fresh < fresh_min * len(self.col.symbols):
                    self._event(f"последний бар есть только у {fresh} из {len(self.col.symbols)} монет")
            # 3) живой OI за бар
            k, got, _late = self.met.last_oi_bar
            if k and k != last_oi_bar:
                last_oi_bar = k
                if got < fresh_min * len(self.col.symbols):
                    self._event(f"OI за бар {fmt(k + BAR_MS)} получен только у {got} монет")
            # 4) поток тикеров
            mc = self.col.misc_conn
            if mc is not None and not mc.connected:
                misc_down_since = misc_down_since or now
                if now - misc_down_since > 120_000:
                    self._event("поток mark price молчит дольше 2 мин")
                    misc_down_since = now
            else:
                misc_down_since = None

            while self.events and self.events[0][0] < now - window:
                self.events.popleft()
            if len(self.events) < max_ev:
                continue
            last = await self.db.read("SELECT t FROM engine_status WHERE key='hard_reset'")
            if last and now - last[0][0] < min_gap:
                continue
            self.reason = "; ".join(w for _, w in list(self.events)[-max_ev:])
            n = await self.db.read("SELECT value FROM engine_status WHERE key='hard_resets'")
            cnt = int(n[0][0]) + 1 if n else 1
            self.db.put(D.SQL_STATUS, [("hard_reset", self.reason, now), ("hard_resets", str(cnt), now)])
            log.error("сторож: %d сбоев за %d мин — ЖЁСТКИЙ ПЕРЕЗАПУСК движка (%s)",
                      len(self.events), window // 60_000, self.reason)
            self.reset.set()
            return
