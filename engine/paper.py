"""Этап 8: бумажный бот — три виртуальных счёта A / B / C по правилам исследования.

Вход: на проходе после T+30, когда посчитан подхват и флаг правила (rA / rB / rC для потока медианы,
или rA_mean… — config [paper] stream). Цена входа = close бара T+30 × (1 + проскальзывание по стакану).
Стоп = минимум 15m-искры − 0.2% от цены T+30. Трейлинг: уровень = max(стоп, максимум × (1 − 8%)).
Таймаут 48 ч от T+30. Комиссия 0.05% тейкер в каждую сторону.
Исполнение — по живым сделкам aggTrade: первая сделка по цене на уровне или ниже закрывает позицию по
своей цене. Запасной путь — проверка закрытых 5m-баров (если поток сделок пропустил): выход ровно по уровню.
Размер: риск 1% текущего баланса / расстояние до стопа. Лимиты: 5 позиций, суммарный риск 5%,
одна позиция на монету, стоп дальше 15% — пропуск.
"""
import asyncio
import logging
import math
from dataclasses import dataclass
from urllib.parse import quote

from . import db as D
from .clock import BAR_MS, floor_bar, fmt, now_ms
from .rest import RestError
from .ws import StreamConn

log = logging.getLogger("paper")

BASE_ACCS = ("A", "B", "C", "A-c", "B-c", "D-c", "A-c-t", "U")
# «Спокойный» двойник каждого счёта: те же сигналы и вход, ⅓ фиксируется на +6% от входа, стоп остатка —
# в безубыток (вход × 1.002), остаток выходит по правилам своего счёта. Свой баланс, размер и лимиты.
CALM = "·сп"
# Система U: правило A в потоке среднего 20 + стакан не толстый (ask2_med < $150k, без данных — не брать)
# + OI в $ на T ≥ $1M + OI/оборот 24 ч ≥ 0.4. Выход как у A-c, без проверки 6 ч.
# U и U·сп — бумажные счета вкладки «Бот», как остальные (кнопка «закрыть руками» — отдельный ручной подсчёт).
# Бумажного счёта на вкладке «Торговля» больше нет: она — пульт реальной торговли (engine/exchange.py).
TRADE_ACCS: tuple = ()
U_FAMILY = {"U", "U" + "·сп"}
ACCS = BASE_ACCS + tuple(a + CALM for a in BASE_ACCS) + TRADE_ACCS


def base_of(acc: str) -> str:
    return acc[:-len(CALM)] if acc.endswith(CALM) else acc
# Счета v0.2: выход трейлингом по закрытию 5m-свечи (8%, после +15% — 5%), жёсткий стоп по тикам, 48 ч от входа;
# A-c-t — плюс проверка на 6-м часу. Правило: A-c и A-c-t — флаг A, B-c — флаг B (A + тонкий стакан),
# D-c — флаг D (A + «крайности» RSI). B-c в исследовании v0.2 отдельно не проверялся.
CLOSE_ACCS = {"A-c", "B-c", "D-c", "A-c-t", "U", "U-торг"}
RULE_OF = {"A": "A", "B": "B", "C": "C", "A-c": "A", "B-c": "B", "D-c": "D", "A-c-t": "A", "U": "U"}
RULE_OF |= {a + CALM: r for a, r in RULE_OF.items()}
RULE_OF["U-торг"] = "U"
H = 3600_000


@dataclass
class Pos:
    acc: str
    sym: str
    t_spark: int
    entry_price: float
    stop: float
    qty: float
    notional: float
    risk_usd: float
    max_price: float
    last_bar_t: int
    deadline: int
    dirty: bool = True
    risk_pct: float = 1.0                  # риск в % собственного баланса на входе — для лимита 5%
    entry_ms: int = 0                      # момент входа: сделки раньше него позицию не трогают
    mode: str = "tick"                     # tick — трейлинг 8% по сделкам; close — по закрытию 5m (v0.2)
    mclose: float | None = None            # close: максимум закрытий 5m-баров с входа
    close_t: int = 0                       # close: открытие последнего 5m-бара, по которому проверен трейлинг
    six_done: int = 0                      # A-c-t: проверка на 6-м часу уже была
    pending: tuple | None = None           # close: (причина, уровень, когда) — ждём первую сделку после закрытия
    calm: bool = False                     # «спокойный» двойник
    tp1_ms: int = 0                        # когда зафиксирована ⅓
    tp1_px: float | None = None            # по какой цене (вход × 1.06)
    frac: float = 1.0                      # доля позиции, которая ещё открыта

    def level(self, trail: float) -> float:
        return max(self.stop, self.max_price * (1 - trail))


def floor_step(x: float, step: float) -> float:
    if not step or step <= 0:
        return x
    return math.floor(x / step + 1e-9) * step


class PaperBot:
    def __init__(self, cfg: dict, db: D.Db, col, met, rest):
        self.p = cfg["paper"]
        self.cfg_u = cfg.get("trade", {})
        self.cfg_ver = int(cfg["meta"]["config_version"])
        self.db = db
        self.col = col
        self.met = met
        self.rest = rest
        self.base_ws = cfg["binance"]["ws_market_base"].rstrip("/")
        self.trail = float(self.p["trail"])
        self.fee = float(self.p["fee_pct"]) / 100
        self.pos: dict[tuple[str, str, int], Pos] = {}
        self.balance: dict[str, float] = {}
        self.last_px: dict[str, tuple[int, float]] = {}
        self.sub: tuple[str, ...] = ()
        self.conn: StreamConn | None = None
        self.task: asyncio.Task | None = None
        self.loaded = False
        self.losses_row: dict[str, int] = {a: 0 for a in ACCS}

    # ================================================================ период счёта («начать заново»)
    async def _load_balances(self) -> None:
        """Баланс каждого счёта — депозит + результат сделок, открытых с начала текущего периода счёта
        (кнопка «начать счёт заново» пишет controls epoch:<счёт>). Старые сделки остаются в журнале."""
        try:
            self.epoch = {k.split(":", 1)[1]: int(v) for k, v in await self.db.read(
                "SELECT key, value FROM controls WHERE key LIKE 'epoch:%'")}
        except Exception:                                      # noqa: BLE001
            self.epoch = {}
        dep = float(self.p["deposit"])
        self.balance = {a: dep for a in ACCS}
        for a in ACCS:
            r = await self.db.read("SELECT COALESCE(SUM(pnl_usd), 0) FROM paper_trades WHERE acc=? AND exit_ms IS NOT NULL "
                                   "AND entry_ms>=?", (a, self.epoch.get(a, 0)))
            self.balance[a] = dep + (r[0][0] if r else 0)

    def _in_period(self, p) -> bool:
        return (p.entry_ms or 0) >= getattr(self, "epoch", {}).get(p.acc, 0)

    # ================================================================ уровни
    def _p_close(self, p: Pos) -> float:
        switch = 1 + float(self.p.get("close_switch", 0.15))
        return float(self.p.get("close_p2", 0.05)) if p.max_price >= p.entry_price * switch \
            else float(self.p.get("close_p1", 0.08))

    def _lvl(self, p: Pos) -> float:
        """Текущий уровень выхода: tick — max(стоп, максимум × 0.92); close — max закрытий × (1 − p)."""
        if p.mode == "close":
            return p.mclose * (1 - self._p_close(p)) if p.mclose else p.stop
        return p.level(self.trail)

    def _tp1(self, p: Pos, t: int, price_hit: float) -> None:
        """«Спокойный»: цена дошла до вход × (1 + calm_tp) — фиксируем долю calm_frac лимитом по этому уровню,
        стоп остатка переносим в безубыток (вход × (1 + calm_be))."""
        if not p.calm or p.tp1_ms:
            return
        tp = p.entry_price * (1 + float(self.p.get("calm_tp", 0.06)))
        if price_hit < tp:
            return
        p.tp1_ms, p.tp1_px = t, tp
        p.frac = 1 - float(self.p.get("calm_frac", 1 / 3))
        p.stop = max(p.stop, p.entry_price * (1 + float(self.p.get("calm_be", 0.002))))
        p.dirty = True
        log.info("бот %s: %s — зафиксирована ⅓ по %.6g, стоп остатка в безубыток %.6g", p.acc, p.sym, tp, p.stop)

    # ================================================================ состояние
    async def load(self) -> None:
        # Прошлая версия: «U» был счётом вкладки «Торговля». Теперь это U-торг, а «U» — счёт вкладки «Бот».
        if not await self.db.read("SELECT 1 FROM engine_status WHERE key='u_renamed'"):
            def job(conn):
                for tbl in ("paper_trades", "paper_skipped", "manual_exits"):
                    conn.execute(f"UPDATE {tbl} SET acc='U-торг' WHERE acc='U'")
                conn.execute("INSERT OR REPLACE INTO engine_status(key, value, t) VALUES('u_renamed', '1', ?)", (now_ms(),))
                conn.commit()
            await self.db.run_in_thread(job)
        await self._load_balances()
        for r in await self.db.read(
                "SELECT acc, symbol, t_spark, entry_price, stop_price, qty, notional, risk_usd, max_price, "
                "last_bar_t, balance_in, entry_ms, mode, mclose, close_t, six_done, tp1_ms, tp1_px, frac, stop_now "
                "FROM paper_trades WHERE exit_ms IS NULL"):
            a, s, t, ep, st, q, n, rk, mx, lb, bal_in, ems, mode, mcl, ct, six, tp_ms, tp_px, fr, st_now = r
            if a not in self.balance:
                continue
            rp = rk / bal_in * 100 if rk and bal_in else float(self.p["risk_pct"])
            self.pos[(a, s, t)] = Pos(a, s, t, ep, st, q, n, rk, mx or ep, lb or (t + 25 * 60_000),
                                      t + 30 * 60_000 + int(float(self.p["hold_hours"]) * H), risk_pct=rp,
                                      entry_ms=ems or 0, mode=mode or ("close" if base_of(a) in CLOSE_ACCS else "tick"),
                                      mclose=mcl, close_t=ct or 0, six_done=six or 0, calm=a.endswith(CALM),
                                      tp1_ms=tp_ms or 0, tp1_px=tp_px, frac=fr or 1.0)
            if st_now:
                self.pos[(a, s, t)].stop = st_now
            if self.pos[(a, s, t)].mode == "close" and ems:
                self.pos[(a, s, t)].deadline = ems + int(float(self.p["hold_hours"]) * H)
        for a in ACCS:
            seq = [r[0] for r in await self.db.read(
                "SELECT pnl_usd FROM paper_trades WHERE acc=? AND exit_ms IS NOT NULL ORDER BY exit_ms DESC LIMIT 20",
                (a,))]
            n = 0
            for v in seq:
                if v is not None and v < 0:
                    n += 1
                else:
                    break
            self.losses_row[a] = n
        self.loaded = True
        log.info("бумажный бот: баланс %s, открытых позиций %d",
                 ", ".join(f"{a} ${self.balance[a]:.2f}" for a in ACCS), len(self.pos))
        self._ensure_stream()

    # ================================================================ поток сделок
    def _ensure_stream(self) -> None:
        want = tuple(sorted({p.sym for p in self.pos.values()} | set(self.met.focus)))
        if want == self.sub:
            return
        self.sub = want
        if self.task:
            self.task.cancel()
            self.task = None
        if not want:
            self.conn = None
            return
        url = self.base_ws + "/stream?streams=" + "/".join(quote(f"{s.lower()}@aggTrade", safe="@_") for s in want)
        self.conn = StreamConn("aggTrade", url, self.on_trade, idle_timeout=float(self.p["trade_idle_sec"]))
        self.task = asyncio.create_task(self.conn.run(), name="aggTrade")
        log.info("сделки aggTrade: %s", ", ".join(want))

    def on_trade(self, stream: str, d: dict) -> None:
        sym = d.get("s")
        if not sym:
            return
        px, t = float(d["p"]), int(d["T"])
        self.last_px[sym] = (t, px)
        for key, p in list(self.pos.items()):
            if p.sym != sym or t < p.entry_ms:
                continue                                   # сделки до входа позицию не трогают
            if px > p.max_price:
                p.max_price, p.dirty = px, True
            self._tp1(p, t, px)                            # «спокойный»: фиксация ⅓ раньше проверки стопа
            if p.mode == "close":
                if p.pending:                              # рыночный выход после закрытия 5m — первая сделка
                    self._close(p, t, p.pending[1], px, "trade", reason=p.pending[0])
                elif px <= p.stop:                         # жёсткий стоп (на бирже — STOP_MARKET) — по тикам
                    self._close(p, t, p.stop, px, "trade")
                continue
            lvl = self._lvl(p)
            if px <= lvl:
                self._close(p, t, lvl, px, "trade")

    # ================================================================ закрытие 5m-бара (счета v0.2)
    def on_bar_close(self, sym: str, t: int, h: float, l: float, c: float) -> None:
        """Из WS: закрылся 5m-бар монеты. Трейлинг по закрытию для счетов v0.2."""
        for p in list(self.pos.values()):
            if p.sym == sym and p.mode == "close":
                self._close_bar(p, t, h, l, c, "trade")

    def _close_bar(self, p: Pos, t: int, h, l, c, src: str) -> None:
        """Один закрытый 5m-бар для позиции close: максимум → стоп → трейлинг по закрытию → проверка 6 ч.
        src = trade: выход рыночным — по первой сделке после закрытия; bar (запасной путь) — сразу по close."""
        if (p.acc, p.sym, p.t_spark) not in self.pos or t <= p.close_t or t + BAR_MS <= p.entry_ms or c is None:
            return
        if t >= p.entry_ms and h is not None and h > p.max_price:   # бар целиком после входа
            p.max_price = h
        if t >= p.entry_ms and h is not None:
            self._tp1(p, t + BAR_MS, h)
        if l is not None and l <= p.stop and t >= p.entry_ms:
            self._close(p, t + BAR_MS, p.stop, p.stop, "bar")
            return
        p.close_t, p.dirty = t, True
        if p.pending:
            return
        reason = None
        if p.mclose is None:
            p.mclose = c                                            # первое закрытие после входа — без проверки
        elif c <= p.mclose * (1 - self._p_close(p)):
            reason = "трейлинг"
        else:
            p.mclose = max(p.mclose, c)
        if reason is None and base_of(p.acc) == "A-c-t" and not p.six_done and \
                t + BAR_MS >= p.entry_ms + int(float(self.p.get("six_min", 360))) * 60_000:
            p.six_done = 1
            if c >= p.entry_price * (1 + float(self.p.get("six_gain", 0.015))):
                reason = "6 часов"
        if reason:
            if src == "bar":
                self._close(p, t + BAR_MS, c, c, "bar", reason=reason)
            else:
                p.pending = (reason, c, now_ms())

    # ================================================================ выход
    def _close(self, p: Pos, t: int, lvl: float, px: float, src: str, reason: str | None = None) -> None:
        if (p.acc, p.sym, p.t_spark) not in self.pos:
            return
        del self.pos[(p.acc, p.sym, p.t_spark)]
        if reason is None:
            if lvl <= p.stop * (1 + 1e-12):
                reason = "безубыток" if p.tp1_ms else "стоп"
            else:
                reason = "трейл" if p.mode == "tick" else "трейлинг"
        q_rem = p.qty * p.frac
        q_tp = p.qty - q_rem
        fees = self.fee * (p.qty * p.entry_price + q_tp * (p.tp1_px or 0) + q_rem * px)
        pnl = q_tp * ((p.tp1_px or p.entry_price) - p.entry_price) + q_rem * (px - p.entry_price) - fees
        if self._in_period(p):                       # позиция прошлого периода в новый баланс не входит
            self.balance[p.acc] += pnl
            self.losses_row[p.acc] = self.losses_row[p.acc] + 1 if pnl < 0 else 0
        self.db.put(D.SQL_PAPER_CLOSE, [(p.max_price, lvl, t, lvl, px, reason, src, fees, pnl,
                                         pnl / p.notional * 100, pnl / p.risk_usd if p.risk_usd else None,
                                         now_ms(), p.acc, p.sym, p.t_spark)])
        log.info("бот %s: выход %s (искра %s) — %s, %s, %+.2f%% / %+.2fR, баланс $%.2f",
                 p.acc, p.sym, fmt(p.t_spark), reason, "по сделке" if src == "trade" else "по бару",
                 pnl / p.notional * 100, pnl / p.risk_usd if p.risk_usd else 0, self.balance[p.acc])

    def _price_now(self, sym: str) -> float | None:
        lp = self.last_px.get(sym)
        if lp and now_ms() - lp[0] < 60_000:
            return lp[1]
        part = self.col.partial.get(sym)
        return part[4] if part else (lp[1] if lp else None)

    # ================================================================ каждые несколько секунд
    async def loop(self) -> None:
        while not self.loaded:
            await asyncio.sleep(1)
        every = float(self.p["state_every_sec"])
        while True:
            await asyncio.sleep(every)
            now = now_ms()
            try:                                           # нажали «начать счёт заново» — пересчитать балансы
                ep = {k.split(":", 1)[1]: int(v) for k, v in await self.db.read(
                    "SELECT key, value FROM controls WHERE key LIKE 'epoch:%'")}
                old = dict(getattr(self, "epoch", {}))
                if ep != old:
                    await self._load_balances()
                    log.info("бот: новый период счёта %s", ", ".join(a for a in ep if ep[a] != old.get(a)))
            except Exception:                              # noqa: BLE001
                pass
            await self._manual_u()
            for p in list(self.pos.values()):
                if p.pending and now - p.pending[2] > float(self.p.get("pending_sec", 15)) * 1000:
                    px = self._price_now(p.sym) or p.pending[1]       # сделки не было — по последней цене
                    self._close(p, now, p.pending[1], px, "bar", reason=p.pending[0])
                    continue
                if now >= p.deadline:
                    px = self._price_now(p.sym)
                    if px:
                        self._close(p, now, self._lvl(p), px, "trade", reason="время" if p.mode == "tick" else "таймаут")
            rows = []
            for p in self.pos.values():
                if p.dirty:
                    rows.append((p.max_price, self._lvl(p), p.last_bar_t, now, p.mclose, p.close_t, p.six_done,
                                 p.tp1_ms or None, p.tp1_px, p.frac, p.stop, p.acc, p.sym, p.t_spark))
                    p.dirty = False
            self.db.put(D.SQL_PAPER_STATE, rows)
            self._ensure_stream()

    async def _manual_u(self) -> None:
        """Счёт U: кнопка «закрыть руками» на вкладке «Торговля» закрывает позицию по-настоящему —
        по цене в момент нажатия, причина «ручной»."""
        opened = [p for p in self.pos.values() if p.acc in TRADE_ACCS]
        if not opened or not TRADE_ACCS:
            return
        rows = await self.db.read("SELECT acc, symbol, t_spark, ms, price FROM manual_exits WHERE acc IN (%s)"
                                  % ",".join("?" * len(TRADE_ACCS)), TRADE_ACCS)
        man = {(a, s, t): (ms, px) for a, s, t, ms, px in rows}
        for p in opened:
            m = man.get((p.acc, p.sym, p.t_spark))
            if m and m[1]:
                self._close(p, m[0], self._lvl(p), m[1], "manual", reason="ручной")

    # ================================================================ на закрытии бара
    async def bar_pass(self, b: int) -> None:
        if not self.loaded:
            await self.load()
        await self._bars_fallback(b)
        await self._intake(b)
        self._ensure_stream()

    async def _bars_fallback(self, b: int) -> None:
        """Закрытые 5m-бары после последнего проверенного: low ≤ уровня — выход по уровню; иначе максимум по high."""
        if not self.pos:
            return
        items = list(self.pos.values())

        def job(conn):
            out = {}
            for p in items:
                out[(p.acc, p.sym, p.t_spark)] = conn.execute(
                    "SELECT t, h, l, c FROM raw_klines_5m WHERE symbol=? AND t>? AND t<? ORDER BY t",
                    (p.sym, p.last_bar_t, b)).fetchall()
            return out

        bars = await self.db.run_in_thread(job)
        for key, rows in bars.items():
            p = self.pos.get(key)
            if p is not None and p.mode == "close":
                for t, h, l, c in rows:
                    if key not in self.pos:
                        break
                    if t + BAR_MS >= p.deadline and c is not None:
                        self._close(p, t + BAR_MS, self._lvl(p), c, "bar", reason="таймаут")
                        break
                    self._close_bar(p, t, h, l, c, "bar")
                    p.last_bar_t = max(p.last_bar_t, t)
                continue
            for t, h, l, c in rows:
                if p is None or (p.acc, p.sym, p.t_spark) not in self.pos:
                    break
                if h is not None:
                    self._tp1(p, t + BAR_MS, h)             # пессимистично: фиксация, потом стоп в том же баре
                lvl = self._lvl(p)
                if l is not None and l <= lvl:
                    self._close(p, t + BAR_MS, lvl, lvl, "bar")
                    break
                if t + BAR_MS >= p.deadline and c is not None:
                    self._close(p, t + BAR_MS, lvl, c, "bar", reason="время")
                    break
                if h is not None and h > p.max_price:
                    p.max_price = h
                p.last_bar_t, p.dirty = t, True

    # ================================================================ новые сигналы
    async def _intake(self, b: int) -> None:
        stream = self.p["stream"]
        suf = "" if stream == "med" else "_mean"
        late_ms = int(float(self.p.get("late_sec", 900)) * 1000)
        since = b - max(late_ms, int(float(self.p.get("max_signal_age_min", 15)) * 60_000)) - 35 * 60_000
        rows = await self.db.read(
            f"SELECT symbol, t, l, entry, rA{suf}, rB{suf}, rC{suf}, conf_ms, rD{suf}, rA_mean, ask2_med, oi_usd_t, q24 "
            f"FROM sparks_15m WHERE conf_ms IS NOT NULL AND t>=? AND (rA{suf}=1 OR rB{suf}=1 OR rC{suf}=1 "
            f"OR rA_mean=1) ORDER BY t", (since,))
        if not rows:
            return
        ctl = await self.db.read("SELECT value FROM controls WHERE key='trade_U'")
        u_on = bool(ctl and ctl[0][0] == "on")
        done = {(a, s, t) for a, s, t in await self.db.read(
            "SELECT acc, symbol, t_spark FROM paper_trades WHERE t_spark>=? "
            "UNION SELECT acc, symbol, t_spark FROM paper_skipped WHERE t_spark>=?", (since, since))}
        depth_cache: dict[str, tuple] = {}
        max_age = late_ms
        for sym, T, l15, E, ra, rb, rc, conf_ms, rd, ra_mean, ask2, oi_usd, q24 in rows:
            rule = {"A": ra, "B": rb, "C": rc, "D": rd, "U": ra_mean}
            both = int(bool(rb and rc))
            for acc in ACCS:
                if not rule[RULE_OF[acc]] or (acc, sym, T) in done:
                    continue
                done.add((acc, sym, T))
                if acc in U_FAMILY:
                    why = self._u_filter(ask2, oi_usd, q24)
                    if why:
                        self._skip(acc, sym, T, *why, self._timing(sym, T, conf_ms))
                        continue
                    if acc in TRADE_ACCS and not u_on:
                        self._skip(acc, sym, T, "торговля выключена", "", self._timing(sym, T, conf_ms))
                        continue
                await self._try_open(acc, sym, T, l15, E, both, b, max_age, "mean" if acc in U_FAMILY else stream,
                                     depth_cache, conf_ms)

    def _u_filter(self, ask2, oi_usd, q24) -> tuple | None:
        """Фильтры счёта U поверх правила A (поток среднего 20). Возвращает (причина, подробности) или None."""
        t = self.cfg_u
        if ask2 is None:
            return "нет данных о стакане", ""
        if ask2 >= float(t.get("u_ask2_max", 150_000)):
            return "стакан толстый", f"офферы до +2% ${ask2 / 1000:.0f}k ≥ ${float(t.get('u_ask2_max', 150_000)) / 1000:.0f}k"
        if oi_usd is None:
            return "нет данных об OI", ""
        if oi_usd < float(t.get("u_oi_min", 1_000_000)):
            return "OI мал", f"OI ${oi_usd / 1e6:.2f}M < ${float(t.get('u_oi_min', 1_000_000)) / 1e6:.1f}M"
        k = float(t.get("u_oi_q24", 0.4))
        if q24 is None or oi_usd < k * q24 * 1e6:
            return "OI мал к обороту", (f"OI/оборот 24 ч {oi_usd / (q24 * 1e6):.2f} < {k:g}" if q24 else "нет оборота")
        return None

    def _timing(self, sym: str, T: int, conf_ms) -> dict:
        """Хронология решения: закрытие бара T+30, приход этого бара по WS, расчёт подхвата, решение."""
        bar_close = T + 30 * 60_000
        ca = self.col.closed_at.get(sym)
        ws = ca[1] if ca and ca[0] == bar_close - BAR_MS else None
        return dict(bar_close_ms=bar_close, ws_ms=ws, conf_calc_ms=conf_ms, decision_ms=now_ms())

    def _skip(self, acc: str, sym: str, T: int, reason: str, detail: str = "", tm: dict | None = None) -> None:
        tm = tm or {}
        self.db.put(D.SQL_PAPER_SKIP, [(acc, sym, T, reason, detail, now_ms(), tm.get("bar_close_ms"),
                                        tm.get("ws_ms"), tm.get("conf_calc_ms"), tm.get("decision_ms"))])
        log.info("бот %s: пропуск %s (искра %s) — %s %s", acc, sym, fmt(T), reason, detail)

    async def _try_open(self, acc, sym, T, l15, E, both, b, max_age, stream, depth_cache, conf_ms=None) -> None:
        tm = self._timing(sym, T, conf_ms)
        now = tm["decision_ms"]
        skip = lambda reason, detail="": self._skip(acc, sym, T, reason, detail, tm)
        if not E or not l15:
            return skip("нет данных", "нет цены входа или минимума искры")
        late_ms = float(self.p.get("late_sec", 900)) * 1000
        delay = now - tm["bar_close_ms"]
        if delay > late_ms:
            ws_txt = f", бар пришёл по WS через {(tm['ws_ms'] - tm['bar_close_ms']) / 1000:.0f} с" if tm["ws_ms"] else ""
            return skip("опоздание", f"решение через {delay / 1000:.0f} с после закрытия бара "
                                     f"(порог {late_ms / 1000:.0f} с){ws_txt}")
        if self._paused(acc):
            return skip("пауза", "серия убытков")
        opened = [p for p in self.pos.values() if p.acc == acc and self._in_period(p)]
        if any(p.sym == sym for p in opened):
            return skip("уже в позиции")
        bal = self.balance[acc]
        risk = bal * float(self.p["risk_pct"]) / 100
        # Лимит своего счёта: открытых меньше max_positions и сумма риска (каждая позиция — в % своего
        # баланса на входе) плюс риск новой ≤ max_total_risk_pct. Сравнение нестрогое.
        open_risk = sum(p.risk_pct for p in opened)
        if len(opened) >= int(self.p["max_positions"]) or \
                open_risk + float(self.p["risk_pct"]) > float(self.p["max_total_risk_pct"]) + 1e-9:
            return skip("лимит позиций", f"открыто {len(opened)}, риск открытых {open_risk:.2f}%")

        # Вход — по фактической цене в момент решения: средняя цена покупки на размер позиции по стакану,
        # снятому сейчас (если стакана нет — последняя цена из WS, если и её нет — close T+30).
        # Стоп и его расстояние считаются от фактического входа. Фильтра по уходу цены нет.
        pad = float(self.p["stop_pad"])
        part = self.col.partial.get(sym)
        mkt = part[4] if part and part[0] >= tm["bar_close_ms"] else None
        base = mkt or E
        stop0 = l15 - pad * base
        if base <= stop0:
            return skip("цена ниже стопа", f"цена {base:.6g} ≤ стоп {stop0:.6g}")
        notional0 = risk / ((base - stop0) / base)
        slip, vwap = await self._slippage(sym, notional0, depth_cache)
        entry = vwap or base
        stop = l15 - pad * entry
        stop_pct = (entry - stop) / entry
        if stop_pct <= 0:
            return skip("цена ниже стопа", f"вход {entry:.6g} ≤ стоп {stop:.6g}")
        if stop_pct * 100 > float(self.p["max_stop_pct"]):
            return skip("стоп дальше 15%", f"{stop_pct * 100:.1f}% от фактического входа {entry:.6g}")
        info = self.col.symbols.get(sym) or {}
        qty = floor_step(risk / stop_pct / entry, info.get("step") or 0)
        notional = qty * entry
        if qty <= 0 or notional < (info.get("min_notional") or 0) or qty < (info.get("min_qty") or 0):
            return skip("мин. ордер", f"${notional:.2f} < ${info.get('min_notional') or 0}")
        risk_real = qty * (entry - stop)
        lev = float(self.p["leverage"])
        # Бары до входа позицию не трогают: запасная проверка по барам начнётся со следующего бара.
        close_mode = base_of(acc) in CLOSE_ACCS
        hold = int(float(self.p["hold_hours"]) * H)
        # tick: проверка по барам — со следующего бара; close: бар, в котором был вход, тоже нужен —
        # его закрытие первое после входа (с него начинается максимум закрытий).
        p = Pos(acc, sym, T, entry, stop, qty, notional, risk_real, entry,
                floor_bar(now) - (BAR_MS if close_mode else 0),
                (now if close_mode else T + 30 * 60_000) + hold, risk_pct=risk_real / bal * 100,
                entry_ms=now, mode="close" if close_mode else "tick", calm=acc.endswith(CALM))
        self.pos[(acc, sym, T)] = p
        self.db.put(D.SQL_PAPER_OPEN, [(acc, sym, T, stream, now, E, slip, entry, mkt, stop, stop_pct * 100, qty,
                                        notional, notional / lev, risk_real, bal, both, entry, self._lvl(p),
                                        p.last_bar_t, now, self.cfg_ver, tm["bar_close_ms"], tm["ws_ms"],
                                        tm["conf_calc_ms"], now, now, now, p.risk_pct, p.mode)])
        log.info("бот %s: вход %s (искра %s) по %.6g (T+30 %.6g, к нему %+.2f%%; проскальзывание %s), стоп %.6g "
                 "(−%.1f%%), позиция $%.2f, риск $%.2f, решение через %.0f с после закрытия бара", acc, sym, fmt(T),
                 entry, E, (entry / E - 1) * 100, f"{slip * 100:+.2f}%" if slip is not None else "?", stop,
                 stop_pct * 100, notional, risk_real, (now - tm["bar_close_ms"]) / 1000)

    def _paused(self, acc: str) -> bool:
        if not self.p.get("pause_enabled"):
            return False
        return self.losses_row[acc] >= int(self.p["pause_losses"])   # −3R за сутки — добавим по журналу

    async def _slippage(self, sym: str, notional: float, cache: dict) -> tuple[float | None, float | None]:
        """Средняя цена покупки на notional по офферам стакана (vwap) и её отклонение от mid (slip)."""
        if sym not in cache:
            try:
                d = await self.rest.get("/fapi/v1/depth", {"symbol": sym, "limit": 50}, weight=2, high=True)
                cache[sym] = (d.get("bids") or [], d.get("asks") or [])
            except RestError as e:
                log.warning("%s: стакан для проскальзывания: %s", sym, e)
                cache[sym] = None
        book = cache[sym]
        if not book or not book[0] or not book[1]:
            return None, None
        bids, asks = book
        mid = (float(bids[0][0]) + float(asks[0][0])) / 2
        left, cost, qty = notional, 0.0, 0.0
        for p, q in asks:
            p, q = float(p), float(q)
            take = min(left, p * q)
            cost += take
            qty += take / p
            left -= take
            if left <= 1e-9:
                break
        if qty <= 0 or left > 1e-9:
            return None, None
        vwap = cost / qty
        return vwap / mid - 1, vwap
