"""REST Binance USDⓈ-M с собственным бюджетом веса в минуту.
Учитывает заголовок X-MBX-USED-WEIGHT-1M, ждёт на 429/418.
Срочные запросы (живой OI) могут залезть в резерв сверх бюджета.
У /futures/data/* свой лимит по числу запросов за 5 минут — отдельный счётчик."""
import asyncio
import logging
import time
from collections import deque

import httpx

log = logging.getLogger("rest")


class RestError(Exception):
    pass


def klines_weight(limit: int) -> int:
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


class Rest:
    def __init__(self, base: str, budget_per_min: int, reserve_high: int = 0, fd_per_5min: int = 900):
        self.client = httpx.AsyncClient(base_url=base, timeout=httpx.Timeout(15.0, connect=10.0),
                                        limits=httpx.Limits(max_connections=40, max_keepalive_connections=40))
        self.budget = budget_per_min
        self.reserve_high = reserve_high
        self.used = 0
        self.fd_budget = fd_per_5min
        self.fd_times: deque[float] = deque()
        self.fd_lock = asyncio.Lock()
        self.pause_until = 0.0
        self.minute = int(time.time() // 60)
        self.lock = asyncio.Lock()
        self.requests = 0
        self.errors = 0

    async def _wait_pause(self) -> None:
        while time.time() < self.pause_until:
            await asyncio.sleep(self.pause_until - time.time() + 0.1)

    async def _reserve(self, weight: int, high: bool) -> None:
        cap = self.budget + (self.reserve_high if high else 0)
        await self._wait_pause()
        while True:
            async with self.lock:
                m = int(time.time() // 60)
                if m != self.minute:
                    self.minute, self.used = m, 0
                if self.used + weight <= cap:
                    self.used += weight
                    return
                wait = 60 - time.time() % 60 + 0.3
            await asyncio.sleep(wait)

    def fd_used(self) -> int:
        now = time.time()
        while self.fd_times and self.fd_times[0] <= now - 300:
            self.fd_times.popleft()
        return len(self.fd_times)

    async def _reserve_fd(self) -> None:
        await self._wait_pause()
        while True:
            async with self.fd_lock:
                if self.fd_used() < self.fd_budget:
                    self.fd_times.append(time.time())
                    return
                wait = self.fd_times[0] + 300 - time.time() + 0.2
            await asyncio.sleep(max(wait, 0.2))

    async def get(self, path: str, params: dict | None = None, weight: int = 1, retries: int = 4,
                  high: bool = False, fd: bool = False):
        """fd=True — эндпоинт /futures/data/*: считается по своему лимиту запросов за 5 минут."""
        delay = 1.0
        for attempt in range(retries + 1):
            if fd:
                await self._reserve_fd()
            else:
                await self._reserve(weight, high)
            try:
                r = await self.client.get(path, params=params)
            except (httpx.TransportError, httpx.TimeoutException) as e:
                self.errors += 1
                if attempt == retries:
                    raise RestError(f"{path}: {e!r}") from e
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            self.requests += 1
            used = r.headers.get("x-mbx-used-weight-1m")
            if used and used.isdigit():
                self.used = max(self.used, int(used))
            if r.status_code in (418, 429):
                self.errors += 1
                pause = int(r.headers.get("retry-after", "60") or 60)
                log.error("Binance %s на %s, пауза %s с", r.status_code, path, pause)
                self.used = self.budget
                self.pause_until = max(self.pause_until, time.time() + pause)
                await self._wait_pause()
                continue
            if r.status_code >= 500:
                self.errors += 1
                if attempt == retries:
                    raise RestError(f"{path}: HTTP {r.status_code}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            if r.status_code >= 400:
                self.errors += 1
                raise RestError(f"{path} {params}: HTTP {r.status_code} {r.text[:200]}")
            return r.json()
        raise RestError(f"{path}: попытки кончились")

    async def server_offset_ms(self) -> int:
        t0 = time.time() * 1000
        d = await self.get("/fapi/v1/time")
        t1 = time.time() * 1000
        return int(d["serverTime"] - (t0 + t1) / 2)

    async def exchange_info(self) -> dict:
        return await self.get("/fapi/v1/exchangeInfo", weight=1)

    async def klines(self, symbol: str, start_ms: int, limit: int) -> list:
        limit = max(1, min(1500, limit))
        return await self.get("/fapi/v1/klines",
                              {"symbol": symbol, "interval": "5m", "startTime": start_ms, "limit": limit},
                              weight=klines_weight(limit))

    async def close(self) -> None:
        await self.client.aclose()
