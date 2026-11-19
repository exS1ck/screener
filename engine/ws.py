"""Одно WS-соединение с комбинированными потоками и вечным переподключением."""
import asyncio
import json
import logging
from typing import Awaitable, Callable

from websockets.asyncio.client import connect

from .clock import now_ms

log = logging.getLogger("ws")


class StreamConn:
    def __init__(self, name: str, url: str, on_msg: Callable[[str, object], None],
                 on_connect: Callable[[], Awaitable[None]] | None = None, idle_timeout: float = 90):
        self.name = name
        self.url = url
        self.on_msg = on_msg
        self.on_connect = on_connect
        self.idle_timeout = idle_timeout
        self.connected = False
        self.msgs = 0
        self.reconnects = 0
        self.planned = 0              # плановые разрывы: Binance закрывает соединение раз в 24 ч
        self.up_since = 0
        self.last_msg_ms = 0

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with connect(self.url, ping_interval=None, max_size=None,
                                   open_timeout=20, close_timeout=5) as ws:
                    self.connected = True
                    self.up_since = now_ms()
                    backoff = 1.0
                    log.info("%s: подключено", self.name)
                    if self.on_connect:
                        asyncio.create_task(self.on_connect())
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout)
                        self.msgs += 1
                        self.last_msg_ms = now_ms()
                        m = json.loads(raw)
                        try:
                            if isinstance(m, dict) and "stream" in m:
                                self.on_msg(m["stream"], m["data"])
                            else:
                                self.on_msg("", m)
                        except Exception:
                            log.exception("%s: ошибка обработки сообщения", self.name)
            except asyncio.CancelledError:
                self.connected = False
                raise
            except TimeoutError:
                log.warning("%s: тишина дольше %s с, переподключаюсь", self.name, self.idle_timeout)
            except Exception as e:
                log.warning("%s: разрыв: %r", self.name, e)
            self.connected = False
            self.reconnects += 1
            if self.up_since and now_ms() - self.up_since >= 23 * 3_600_000:
                self.planned += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
