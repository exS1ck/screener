"""Движок TraderBot v0.1 — этап 1: сбор данных в SQLite (свечи, WS-потоки, OI, L/S, стакан)
этап 2: детекторы 5m- и 15m-искр; этап 3: контекст, подхват, балл, правила;
этап 5: исходы через 12, 24, 48 ч; этап 6: живые теги (стадия, флаги, выход);
этап 7: списки «ползут сейчас» и «капитуляция 4h»;
этап 8: бумажный бот A / B / C.
Запуск:  python run_engine.py   (остановка — Ctrl+C)"""
import asyncio
from pathlib import Path
import logging
import sys
import warnings
from logging.handlers import RotatingFileHandler

from engine import config
from engine.clock import fmt, now_ms, set_offset
from engine.collector import Collector
from engine.db import Db
from engine.db import SQL_STATUS as D_STATUS
from engine.metrics import Metrics
from engine.sparks import SparkDetector
from engine.context import ContextBuilder
from engine.outcomes import OutcomeBuilder
from engine.live import LiveTags
from engine.creep import Creep
from engine.capit import Capit
from engine.paper import PaperBot
from engine.indicators import IndicatorBuilder
from engine.exits import ExitModels
from engine.oversold import OversoldRSI
from engine.watchdog import RESET_CODE, Watchdog
from engine.exchange import Exchange
from engine.live_trader import LiveTrader
from engine.remote import Remote
from engine.regime import MarketRegime
from engine.shorts import Shorts
from engine.lake_export import LakeExporter
from engine.rest import Rest

log = logging.getLogger("engine")


def disable_quickedit() -> None:
    """Windows: выделение мышью в консоли ставит вывод на паузу, и вместе с ним замирает весь движок.
    Отключаем режим быстрого выделения для этого окна."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        k = ctypes.windll.kernel32
        h = k.GetStdHandle(-10)                      # STD_INPUT_HANDLE
        mode = ctypes.c_uint32()
        if k.GetConsoleMode(h, ctypes.byref(mode)):
            ENABLE_QUICK_EDIT, ENABLE_EXTENDED_FLAGS = 0x40, 0x80
            k.SetConsoleMode(h, (mode.value & ~ENABLE_QUICK_EDIT) | ENABLE_EXTENDED_FLAGS)
    except Exception:
        pass


def setup_logging(cfg: dict) -> None:
    path = config.path_of(cfg, "log")
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt_ = logging.Formatter("%(asctime)s %(levelname)-7s %(name)-8s %(message)s", "%d.%m %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = RotatingFileHandler(path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt_)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt_)
    root.addHandler(fh)
    root.addHandler(sh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def supervise(name: str, fn) -> None:
    """Фоновая задача, которая не роняет движок: при ошибке пишет лог и перезапускается."""
    while True:
        try:
            await fn()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("задача %s упала, перезапуск через 10 с", name)
            await asyncio.sleep(10)


async def main() -> bool:
    """Возвращает True, если сторож запросил жёсткий перезапуск."""
    cfg = config.load()
    setup_logging(cfg)
    log.info("старт, конфиг v%s (%s)", cfg["meta"]["config_version"], cfg["_hash"])

    dbp = config.path_of(cfg, "db")
    for f in (dbp, Path(str(dbp) + "-wal")):
        if f.exists():
            log.info("база: %s — %.1f МБ", f.name, f.stat().st_size / 1e6)
    db = Db(dbp)
    b = cfg["binance"]
    rest = Rest(b["rest_base"], int(b["rest_weight_budget_per_min"]),
                reserve_high=int(b["rest_weight_reserve_high"]), fd_per_5min=int(b["futures_data_per_5min"]))
    writer = asyncio.create_task(db.run(float(cfg["collect"]["db_flush_sec"])), name="db")
    col = bot = ex = remote = None
    hard = False
    try:
        offset = await rest.server_offset_ms()
        set_offset(offset)
        log.info("часы: поправка к Binance %+d мс, сейчас %s", offset, fmt(now_ms()))
        if abs(offset) > 2000:
            log.warning("локальные часы расходятся с Binance больше чем на 2 с — синхронизируй время Windows")

        col = Collector(cfg, db, rest)
        await col.refresh_universe()
        await col.load_last_t()

        # WS — сразу, чтобы не было разрыва между историей и живыми данными.
        col.start_kline_streams()
        misc = col.start_misc_stream()

        met = Metrics(cfg, db, rest, col)
        await met.load_state()
        spk = SparkDetector(cfg, db, col)
        spk.ctx = ContextBuilder(cfg, db, col)
        spk.ind = IndicatorBuilder(cfg, db)
        out = OutcomeBuilder(cfg, db, spk)
        live = LiveTags(cfg, db, col, met)
        spk.live = live
        capit = Capit(cfg, db, col, rest)
        bot = PaperBot(cfg, db, col, met, rest)
        rsi_list = OversoldRSI(cfg, db, col, rest)
        spk.watchers = [Creep(cfg, db, col), capit, rsi_list, bot]
        col.bar_listeners.append(bot.on_bar_close)
        xm = ExitModels(cfg, db, rest)
        wd = Watchdog(cfg, db, col, met, rest, bot)
        ex = Exchange(cfg, db, rest)
        live_u = LiveTrader(cfg, db, col, ex)
        spk.watchers.append(live_u)
        col.bar_listeners.append(live_u.on_bar_close)
        remote = Remote(cfg, db, col, ex, live_u)
        # v0.4: режим рынка и шорт-счета — последними в проходе бара, чтобы не задерживать лонговые счета
        spk.watchers.append(Shorts(cfg, db, col, MarketRegime(cfg, db, col), rest))
        db.put(D_STATUS, [("clock_offset", str(offset), now_ms())])

        jobs = [
            ("backfill", col.initial_backfill),
            ("reconcile", col.reconcile_loop),
            ("holes", col.hole_scan_loop),
            ("funding", col.funding_loop),
            ("universe", col.universe_loop),
            ("retention", col.retention_loop),
            ("status", col.status_loop),
            ("sets", met.sets_loop),
            ("oi_bar", met.oi_bar_loop),
            ("oi_fast", met.oi_fast_loop),
            ("oi_hist", met.oi_hist_loop),
            ("ls", met.ls_loop),
            ("depth", met.depth_loop),
            ("m_status", met.status_loop),
            ("sparks_catchup", spk.catch_up),
            ("sparks", spk.live_loop),
            ("big5", spk.backfill_big),
            ("outcomes", out.loop),
            ("live_fast", live.fast_loop),
            ("capit", capit.loop),
            ("paper", bot.loop),
            ("exits", xm.loop),
            ("rsi", rsi_list.loop),
            ("clock", wd.clock_loop),
            ("prices", col.prices_loop),
            ("ex_stream", ex.stream_loop),
            ("ex_sync", ex.sync_loop),
            ("ex_cmd", lambda: ex.command_loop(col)),
            ("live", live_u.loop),
            ("lake", LakeExporter(cfg, db).loop),
            ("tg_hb", remote.hb_loop),
            ("tg_events", remote.events_loop),
            ("tg_cmd", remote.cmd_loop),
            ("watchdog", wd.loop),
        ]
        tasks = [asyncio.create_task(supervise(n, f), name=n) for n, f in jobs]
        main_task = asyncio.gather(misc, *tasks)
        reset_task = asyncio.create_task(wd.reset.wait())
        done, _ = await asyncio.wait({main_task, reset_task}, return_when=asyncio.FIRST_COMPLETED)
        if reset_task in done:
            hard = True
            main_task.cancel()
            try:
                await main_task
            except asyncio.CancelledError:
                pass
        else:
            reset_task.cancel()
            await main_task
    finally:
        # WS-соединения запущены отдельными задачами — закрываем их сами, иначе при остановке asyncio
        # пишет «Task was destroyed but it is pending!» (безвредно, но пугает).
        ws_tasks = []
        if col is not None:
            ws_tasks += list(col.kline_tasks)
        if bot is not None and bot.task is not None:
            ws_tasks.append(bot.task)
        if ex is not None and ex.stream_task is not None:
            ws_tasks.append(ex.stream_task)
        for t in ws_tasks:
            t.cancel()
        if ws_tasks:
            await asyncio.wait(ws_tasks, timeout=6)
        if remote is not None:
            try:
                await asyncio.wait_for(remote.close(), timeout=3)
            except Exception:                            # noqa: BLE001
                pass
        if ex is not None:
            try:
                await asyncio.wait_for(ex.close(), timeout=5)
            except Exception:                            # noqa: BLE001
                pass
        writer.cancel()
        try:
            await writer
        except asyncio.CancelledError:
            pass
        await rest.close()
        log.info("остановлен%s", " для жёсткого перезапуска" if hard else "")
    return hard


if __name__ == "__main__":
    # numpy предупреждает о пустых окнах (монета с пропусками, начало истории) — это норма, не засоряем консоль
    warnings.filterwarnings("ignore", message="All-NaN slice encountered")
    warnings.filterwarnings("ignore", message="Mean of empty slice")
    disable_quickedit()
    code = 0
    try:
        if asyncio.run(main()):
            code = RESET_CODE                      # run_forever.py запустит движок заново
    except KeyboardInterrupt:
        pass
    sys.exit(code)
