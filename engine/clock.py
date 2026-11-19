"""Время. Всё хранится в мс UTC. Сетка 5m: бар открывается в t, закрывается в t + BAR_MS."""
import time
from datetime import datetime, timezone, timedelta

BAR_MS = 300_000
DAY_BARS = 288
MSK = timezone(timedelta(hours=3))

_offset_ms = 0


def set_offset(ms: int) -> None:
    global _offset_ms
    _offset_ms = int(ms)


def get_offset() -> int:
    return _offset_ms


def now_ms() -> int:
    """Время по часам Binance (локальные часы + поправка)."""
    return int(time.time() * 1000) + _offset_ms


def floor_bar(ms: int) -> int:
    return ms - ms % BAR_MS


def fmt(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, MSK).strftime("%d.%m %H:%M:%S МСК")
