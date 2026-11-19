"""Шорт-счета: v0.4 (Ш1, Ш2) и v0.5 (Ш1-b, Ш2-b, Ш2-c, Ш3, поздние сигналы) — по эталонам engine/ref04.py и
engine/ref05.py. Бумага: депозит $100, риск 1%; у каждого счёта свои баланс и лимиты.

На закрытии каждого 5m-бара k (после режима рынка):
  1. Вселенная: оборот за 288 баров (не меньше 250 баров с данными) в $0.5–30M.
  2. Эпизоды 12 ч (ref04.pump_episodes по одному бару) и сигналы Ш1/Ш2 (ref04.scan_episode по одному бару).
     В момент первого сигнала своего типа — фильтр силы пампа M/base − 1 (ref05.signals_v05):
       Ш1-b — 35–55%, сделка как Ш1;  Ш2-b — от 35%, сетка;  Ш2-c — от 35%, один шорт (+30% / −10%, 48 ч).
     Первый сигнал не прошёл фильтр → эпизод под наблюдением: первый более поздний бар, где выполнены все условия
     счёта, — поздний сигнал (ref05.late_signals_v05) → виртуальная сделка (virtual_trades, вне баланса).
  3. Эпизоды 7 дней для Ш3 (ref05.pump_episodes_7d по одному бару): base — минимум low за 2016 баров, памп ≥ 100%;
     сигнал (ref05.scan_episode_s3) ищется 48 ч; t_peak — по окончании окна. Новый эпизод монеты ищется только
     после окончания окна прошлого (в эталоне t_peak заглядывает вперёд — вживую так нельзя).
  4. Сделки по барам, как в эталоне: доборы и стоп по high, потом цель по low; издержки эталона.
Лимиты: Ш1, Ш1-b, Ш2-c — до 5 позиций; Ш2, Ш2-b, Ш3 — до 3 сеток; везде одна позиция на монету.
Кнопка «начать счёт заново» (controls epoch:<счёт>) — баланс, лимиты и итоги считаются от этого момента;
позиции, открытые раньше, доводятся до выхода и остаются в прошлом периоде.

Один раз — досчёт по истории эталоном (src = backfill): эпизоды, сигналы, модельные сделки (status = model).
"""
import json
import logging
import warnings

import numpy as np

from . import db as D
from . import ref04
from . import ref05
from . import ref06
from .clock import BAR_MS, fmt, now_ms

log = logging.getLogger("shorts")
H1 = 3_600_000
DAY = 86_400_000
LOOK, LIFE, GAP = 144, 432, 288
RSI_WIN = 260 * H1
RSI4_WIN = 40 * DAY
ACCS = ("Ш1", "Ш1-b", "Ш1-c", "Ш1-d", "Ш2", "Ш2-b", "Ш2-c", "Ш3")
KIND = {"Ш1": "s1", "Ш1-b": "s1", "Ш1-c": "s1", "Ш1-d": "s1", "Ш2": "grid", "Ш2-b": "grid", "Ш3": "grid",
        "Ш2-c": "single"}
HZ = {"s1": ref04.BARS_12H, "grid": ref04.BARS_24H, "single": ref05.S2C_HZ}
GRID_LOSS = abs(ref05.GRID_MAX_LOSS)
S1B_LO, S1B_HI = ref05.S1B_PUMP
S2B_MIN = ref05.S2B_PUMP_MIN


def _n(x):
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return x if np.isfinite(x) else None


def load_sym(conn, sym: str, t_from: int, t_to: int):
    """Бары монеты [t_from, t_to] включительно: h, l, c, qv по сетке 5m."""
    n = (t_to - t_from) // BAR_MS + 1
    h, l, c, q = (np.full(n, np.nan) for _ in range(4))
    for t, hh, ll, cc, qq in conn.execute("SELECT t, h, l, c, qv FROM raw_klines_5m WHERE symbol=? AND t>=? AND t<=?",
                                          (sym, t_from, t_to)):
        k = (t - t_from) // BAR_MS
        h[k], l[k], c[k], q[k] = hh, ll, cc, qq
    return h, l, c, q


def snapshot_at(h, l, c, r15, r1, k: int, tb: int, regime: str, mkt24) -> dict | None:
    """Один бар из ref04.scan_episode: M и base от t_base до k, RSI, просадка от M."""
    M, base = np.nanmax(h[tb:k + 1]), np.nanmin(l[tb:k + 1])
    if not (M / base - 1 >= 0.25) or not np.isfinite(c[k]):
        return None
    i15, i1 = (k + 1) // 3 - 1, (k + 1) // 12 - 1
    i15_first = (tb + 1) // 3
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        rnow = r15[i15] if i15 >= 0 else np.nan
        rmax = np.nanmax(r15[i15_first:i15 + 1]) if i15 >= i15_first else np.nan
    r1now = r1[i1] if i1 >= 0 else np.nan
    return dict(M=float(M), base=float(base), close=float(c[k]), rsi15=rnow, rsi15_max=rmax, rsi1h=r1now,
                mkt24=mkt24, regime=regime, dd=1 - c[k] / M, pump=float(M / base - 1))


def is_s1(s) -> bool:
    return s["dd"] >= 0.08 and s["rsi1h"] >= 70 and s["regime"] not in ("рост", "")


def is_s2(s) -> bool:
    m = s["mkt24"]
    return (s["dd"] <= 0.02 and s["rsi15_max"] >= 75 and s["rsi15"] <= s["rsi15_max"] - 10
            and m is not None and np.isfinite(m) and m < 0)


def load_flow(conn, sym: str, t_from: int, t_to: int):
    """5m-оборот и оборот тейкер-покупок монеты [t_from, t_to] включительно."""
    n = (t_to - t_from) // BAR_MS + 1
    q, tb = np.full(n, np.nan), np.full(n, np.nan)
    for t, qq, bb in conn.execute("SELECT t, qv, tbq FROM raw_klines_5m WHERE symbol=? AND t>=? AND t<=?",
                                  (sym, t_from, t_to)):
        k = (t - t_from) // BAR_MS
        q[k], tb[k] = qq, bb
    return q, tb


def v06_filters(conn, sym: str, t_k: int, t_base: int) -> dict:
    """Фильтры Ш1-c / Ш1-d на сигнале (ref06): доля тейкер-покупок за последний час (бары k−11…k) и длительность
    пампа от t_base до сигнала. Только 5m-свечи — доступно сразу на закрытии бара."""
    q, tb = load_flow(conn, sym, t_k - 11 * BAR_MS, t_k)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sq = np.nansum(q)
        share = np.nansum(tb) / sq if sq > 0 else np.nan
    hours = max((t_k - t_base) // BAR_MS, 1) * 5 / 60
    acc = ref06.accounts_v06(dict(taker_share_1h=share, pump_hours=hours))
    return dict(taker=float(share) if np.isfinite(share) else None, hours=hours, S1c=acc["S1c"], S1d=acc["S1d"])


def s1b_ok(p: float) -> bool:
    return S1B_LO <= p <= S1B_HI


def levels_for(kind: str, E: float, M: float) -> tuple:
    """Стоп, цель, риск в % (для R) по типу сделки."""
    if kind == "s1":
        stop = M * 1.01
        return stop, E * 0.95, (stop / E - 1) * 100
    if kind == "single":
        return E * (1 + ref05.S2C_STOP), E * (1 - ref05.S2C_TP), ref05.S2C_STOP * 100
    return E * (1 + ref04.S2_STEP * ref04.S2_ADDS + ref04.S2_STOP_AFTER), E * (1 - ref04.S2_TP), GRID_LOSS


def grid_pnl(fills, px, reason) -> float:
    exit_fee = ref04.FEE_LIMIT if reason == "цель" else ref04.FEE_STOP
    return (sum((1 - px / f) * 100 for f, _ in fills) - ref04.FEE_MARKET - ref04.FEE_LIMIT * (len(fills) - 1)
            - exit_fee * len(fills))


def step_trade(kind: str, E, stop, tgt, fills, h, l, c, j: int):
    """Один бар сделки после сигнала (j — номер бара после сигнального). Возвращает (цена, причина, результат %)
    или None. Для сетки дописывает исполнения в fills и пересчитывает цель от средней."""
    if kind == "s1":
        if h >= stop:
            return stop, "стоп", (1 - stop / E) * 100 - ref04.FEE_MARKET - ref04.FEE_STOP, tgt
        if l <= tgt:
            return tgt, "цель", 5.0 - ref04.FEE_MARKET - ref04.FEE_LIMIT, tgt
    elif kind == "single":
        if h >= stop:
            return stop, "стоп", -ref05.S2C_STOP * 100 - ref04.FEE_MARKET - ref04.FEE_STOP, tgt
        if l <= tgt:
            return tgt, "цель", ref05.S2C_TP * 100 - ref04.FEE_MARKET - ref04.FEE_LIMIT, tgt
    else:
        E0 = fills[0][0]
        for lv in [E0 * (1 + ref04.S2_STEP * (i + 1)) for i in range(ref04.S2_ADDS)][len(fills) - 1:]:
            if h >= lv:
                fills.append([lv, None])
        if h >= stop:
            return stop, "стоп", grid_pnl(fills, stop, "стоп"), tgt
        avg = len(fills) / sum(1 / f for f, _ in fills)
        tgt = avg * (1 - ref04.S2_TP)
        if l <= tgt:
            return tgt, "цель", grid_pnl(fills, tgt, "цель"), tgt
    if j >= HZ[kind] and np.isfinite(c):
        px = float(c)
        if kind == "grid":
            return px, "время", grid_pnl(fills, px, "время"), tgt
        return px, "время", (1 - px / E) * 100 - ref04.FEE_MARKET - ref04.FEE_STOP, tgt
    return None, None, None, tgt


class Shorts:
    def __init__(self, cfg: dict, db: D.Db, col, regime, rest=None):
        self.rest = rest
        self.x = cfg.get("shorts", {})
        self.db = db
        self.col = col
        self.regime = regime
        self.dep = float(self.x.get("deposit", 100))
        self.loaded = False
        self.last_t: int | None = None

    # ================================================================ проход на закрытии бара
    async def bar_pass(self, b: int) -> None:
        await self.regime.update(b)
        if not self.loaded:
            await self.backfill()
            await self.backfill_v05()
            await self.backfill_v06()
            self.loaded = True
        t_k = b - BAR_MS
        if self.last_t is None:
            r = await self.db.read("SELECT value FROM engine_status WHERE key='shorts_last_t'")
            self.last_t = int(r[0][0]) if r else t_k - BAR_MS
        await self.db.flush()
        done = await self.db.run_in_thread(lambda conn: self._process(conn, self.last_t + BAR_MS, t_k))
        self.last_t = max(self.last_t, done)
        self.db.put(D.SQL_STATUS, [("shorts_last_t", str(self.last_t), now_ms())])
        try:
            await self.enrich()
        except Exception:                                        # noqa: BLE001
            log.exception("признаки сигналов v0.6: ошибка (продолжаю)")

    def _process(self, conn, t_from: int, t_to: int) -> int:
        t = t_from
        last = t_from - BAR_MS
        while t <= t_to:
            if conn.execute("SELECT 1 FROM market_regime WHERE t=?", (t,)).fetchone() is None:
                break
            self._bar(conn, t)
            last = t
            t += BAR_MS
        conn.commit()
        return last

    def _epochs(self, conn) -> dict:
        try:
            return {k.split(":", 1)[1]: int(v) for k, v in conn.execute(
                "SELECT key, value FROM controls WHERE key LIKE 'epoch:%'")}
        except Exception:                                        # noqa: BLE001
            return {}

    def _bar(self, conn, t_k: int) -> None:
        reg = conn.execute("SELECT regime, mkt24, mkt72, breadth24, btc24 FROM market_regime WHERE t=?", (t_k,)).fetchone()
        regime, mkt24 = reg[0] or "", reg[1]
        self.ep = self._epochs(conn)
        uni = {s: (sm / 1e6) for s, sm, n in conn.execute(
            "SELECT symbol, SUM(qv), COUNT(qv) FROM raw_klines_5m WHERE t>? AND t<=? GROUP BY symbol",
            (t_k - 288 * BAR_MS, t_k)) if n >= 250 and sm is not None and 0.5 <= sm / 1e6 <= 30}
        self._episodes_12h(conn, t_k, uni)
        self._signals_12h(conn, t_k, reg, regime, mkt24)
        self._episodes_7d(conn, t_k, uni)
        for tbl in ("short_trades", "virtual_trades"):
            for (tid,) in conn.execute(f"SELECT id FROM {tbl} WHERE status='open' AND last_t<?", (t_k,)).fetchall():
                self._manage(conn, tbl, tid, t_k)

    # ---------------------------------------------------------------- эпизоды 12 ч
    def _episodes_12h(self, conn, t_k, uni) -> None:
        cand = [s for s, lo, hi, nl, nh in conn.execute(
            "SELECT symbol, MIN(l), MAX(h), COUNT(l), COUNT(h) FROM raw_klines_5m WHERE t>? AND t<=? GROUP BY symbol",
            (t_k - LOOK * BAR_MS, t_k)) if nl >= LOOK and nh >= LOOK and lo and hi / lo - 1 >= 0.25 and s in uni]
        last_ep = {s: tb for s, tb in conn.execute("SELECT symbol, MAX(t_base) FROM pump_episodes GROUP BY symbol")}
        for s in cand:
            prev = last_ep.get(s)
            if prev is not None and t_k <= prev + LIFE * BAR_MS:
                continue
            h, l, _, _ = load_sym(conn, s, t_k - (LOOK - 1) * BAR_MS, t_k)
            if not np.isfinite(l).any():
                continue
            i = int(np.nanargmin(l))
            tb, base = t_k - (LOOK - 1 - i) * BAR_MS, float(l[i])
            M = float(np.nanmax(h[i:]))
            if M / base - 1 >= 0.25 and (prev is None or tb >= prev + GAP * BAR_MS):
                conn.execute("INSERT OR IGNORE INTO pump_episodes(symbol, t_base, start_t, base, q24, src, ms) "
                             "VALUES(?,?,?,?,?,'live',?)", (s, tb, t_k, base, uni.get(s), now_ms()))
                log.info("памп: %s, начало эпизода %s (+%.0f%%)", s, fmt(t_k + BAR_MS), (M / base - 1) * 100)

    # ---------------------------------------------------------------- сигналы 12 ч, фильтры v0.5, поздние
    def _signals_12h(self, conn, t_k, reg, regime, mkt24) -> None:
        rows = conn.execute("SELECT symbol, t_base, s1_t, s2_t, late1, late2 FROM pump_episodes WHERE t_base>? "
                            "AND start_t<=? AND (s1_t IS NULL OR s2_t IS NULL OR late1='watch' OR late2='watch')",
                            (t_k - LIFE * BAR_MS, t_k)).fetchall()
        for s, tb, s1, s2, late1, late2 in rows:
            if t_k >= tb + LIFE * BAR_MS:
                continue
            start = (t_k - RSI_WIN) // H1 * H1
            h, l, c, _ = load_sym(conn, s, start, t_k)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                r15, r1 = ref04.rsi_15m_1h(c)
            k, tbi = (t_k - start) // BAR_MS, (tb - start) // BAR_MS
            snap = snapshot_at(h, l, c, r15, r1, k, tbi, regime, mkt24)
            if snap is None:
                continue
            p = snap["pump"]
            # --- тип 1: Ш1 и Ш1-b
            if s1 is None and is_s1(snap):
                self._signal(conn, "Ш1", s, t_k, tb, snap, reg)
                f6 = v06_filters(conn, s, t_k, tb)
                snap["taker_1h"], snap["pump_hours"] = f6["taker"], f6["hours"]
                self._open(conn, "Ш1", s, t_k, snap, tb)
                ok = s1b_ok(p)
                if ok:
                    self._open(conn, "Ш1-b", s, t_k, snap, tb)
                if f6["S1c"]:
                    self._open(conn, "Ш1-c", s, t_k, snap, tb)
                if f6["S1d"]:
                    self._open(conn, "Ш1-d", s, t_k, snap, tb)
                conn.execute("UPDATE pump_episodes SET s1_t=?, s1_pump=?, late1=? WHERE symbol=? AND t_base=?",
                             (t_k, p * 100, "none" if ok else "watch", s, tb))
            elif late1 == "watch" and s1 is not None and t_k != s1 and is_s1(snap) and s1b_ok(p):
                self._virtual(conn, "Ш1-b", s, t_k, tb, snap, s1)
                conn.execute("UPDATE pump_episodes SET late1='done' WHERE symbol=? AND t_base=?", (s, tb))
            # --- тип 2: Ш2, Ш2-b, Ш2-c
            if s2 is None and is_s2(snap):
                self._signal(conn, "Ш2", s, t_k, tb, snap, reg)
                self._open(conn, "Ш2", s, t_k, snap, tb)
                ok = p >= S2B_MIN
                if ok:
                    self._open(conn, "Ш2-b", s, t_k, snap, tb)
                    self._open(conn, "Ш2-c", s, t_k, snap, tb)
                conn.execute("UPDATE pump_episodes SET s2_t=?, s2_pump=?, late2=? WHERE symbol=? AND t_base=?",
                             (t_k, p * 100, "none" if ok else "watch", s, tb))
            elif late2 == "watch" and s2 is not None and t_k != s2 and is_s2(snap) and p >= S2B_MIN:
                self._virtual(conn, "Ш2-b", s, t_k, tb, snap, s2)
                self._virtual(conn, "Ш2-c", s, t_k, tb, snap, s2)
                conn.execute("UPDATE pump_episodes SET late2='done' WHERE symbol=? AND t_base=?", (s, tb))

    def _signal(self, conn, acc, s, t_k, tb, snap, reg) -> None:
        conn.execute("INSERT OR IGNORE INTO short_signals(acc, symbol, t, t_base, M, base, close, rsi15, rsi15_max, rsi1h, "
                     "mkt24, mkt72, breadth24, btc24, regime, src, ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'live',?)",
                     (acc, s, t_k, tb, snap["M"], snap["base"], snap["close"], _n(snap.get("rsi15")),
                      _n(snap.get("rsi15_max")), _n(snap.get("rsi1h")), _n(reg[1]), _n(reg[2]), _n(reg[3]), _n(reg[4]),
                      reg[0] or "", now_ms()))

    # ---------------------------------------------------------------- эпизоды 7 дней (Ш3)
    def _episodes_7d(self, conn, t_k, uni) -> None:
        look, scan = ref05.S3_LOOK, ref05.S3_SCAN
        # окно сигнала закончилось — записать пик
        for s, tb, st in conn.execute("SELECT symbol, t_base, start_t FROM pump_episodes_7d WHERE t_peak IS NULL "
                                      "AND start_t + ? <= ?", (scan * BAR_MS, t_k)).fetchall():
            end = st + scan * BAR_MS
            h, _, _, _ = load_sym(conn, s, tb, end)
            if np.isfinite(h).any():
                i = int(np.nanargmax(h))
                conn.execute("UPDATE pump_episodes_7d SET t_peak=?, peak=? WHERE symbol=? AND t_base=?",
                             (tb + i * BAR_MS, float(h[i]), s, tb))
        # новые эпизоды
        cand = [s for s, lo, hi, nl, nh in conn.execute(
            "SELECT symbol, MIN(l), MAX(h), COUNT(l), COUNT(h) FROM raw_klines_5m WHERE t>? AND t<=? GROUP BY symbol",
            (t_k - look * BAR_MS, t_k)) if nl >= look and nh >= look and lo and hi / lo - 1 >= ref05.S3_RISE and s in uni]
        last = {s: (st, tp) for s, st, tp, _ in conn.execute(
            "SELECT symbol, start_t, t_peak, MAX(t_base) FROM pump_episodes_7d GROUP BY symbol")}
        for s in cand:
            pv = last.get(s)
            if pv is not None:
                st, tp = pv
                if tp is None or t_k <= max(st, tp) + ref05.S3_GAP * BAR_MS:
                    continue
            h, l, _, _ = load_sym(conn, s, t_k - (look - 1) * BAR_MS, t_k)
            if not np.isfinite(l).any():
                continue
            i = int(np.nanargmin(l))
            tb = t_k - (look - 1 - i) * BAR_MS
            if pv is not None and tb <= pv[1]:
                continue
            if np.nanmax(h[i:]) / l[i] - 1 < ref05.S3_RISE:
                continue
            conn.execute("INSERT OR IGNORE INTO pump_episodes_7d(symbol, t_base, start_t, base, src, ms) "
                         "VALUES(?,?,?,?,'live',?)", (s, tb, t_k, float(l[i]), now_ms()))
            log.info("большой памп (7 дней): %s, начало эпизода %s (+%.0f%%)", s, fmt(t_k + BAR_MS),
                     (np.nanmax(h[i:]) / l[i] - 1) * 100)
        # сигнал Ш3 в окне 48 ч
        for s, tb, st in conn.execute("SELECT symbol, t_base, start_t FROM pump_episodes_7d WHERE s3_t IS NULL "
                                      "AND start_t<=? AND start_t + ? > ?", (t_k, scan * BAR_MS, t_k)).fetchall():
            start = (t_k - RSI4_WIN) // DAY * DAY                 # от 00:00 UTC — 4h-свечи с 00/04/08… UTC
            h, l, c, _ = load_sym(conn, s, start, t_k)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                r4 = ref05.rsi_4h(c)
                k, tbi = (t_k - start) // BAR_MS, (tb - start) // BAR_MS
                M, base = np.nanmax(h[tbi:k + 1]), np.nanmin(l[tbi:k + 1])
                if M / base - 1 < ref05.S3_RISE or not np.isfinite(c[k]):
                    continue
                i4, i4_first = (k + 1) // 48 - 1, (tbi + 1) // 48
                if i4 < i4_first:
                    continue
                rnow, rmax = r4[i4], np.nanmax(r4[i4_first:i4 + 1])
            if 1 - c[k] / M <= 0.05 and rmax >= 70 and rnow <= rmax - 10:
                snap = dict(M=float(M), base=float(base), close=float(c[k]), pump=float(M / base - 1),
                            rsi4h=float(rnow), rsi4h_max=float(rmax))
                conn.execute("UPDATE pump_episodes_7d SET s3_t=? WHERE symbol=? AND t_base=?", (t_k, s, tb))
                conn.execute("INSERT OR IGNORE INTO short_signals(acc, symbol, t, t_base, M, base, close, src, ms) "
                             "VALUES('Ш3',?,?,?,?,?,?,'live',?)", (s, t_k, tb, snap["M"], snap["base"], snap["close"], now_ms()))
                self._open(conn, "Ш3", s, t_k, snap, tb)

    # ---------------------------------------------------------------- сделки
    def _balance(self, conn, acc: str) -> float:
        ep = self.ep.get(acc, 0)
        r = conn.execute("SELECT COALESCE(SUM(pnl_usd), 0) FROM short_trades WHERE acc=? AND status='closed' "
                         "AND t_signal>=?", (acc, ep)).fetchone()
        return self.dep + (r[0] or 0)

    def _open(self, conn, acc: str, sym: str, t_k: int, snap: dict, tb: int) -> None:
        kind = KIND[acc]
        E, M = snap["close"], snap["M"]
        stop, tgt, risk_pct = levels_for(kind, E, M)
        bal = self._balance(conn, acc)
        loss = risk_pct / 100 if kind != "grid" else GRID_LOSS / 100
        notional = bal * 0.01 / loss if loss > 0 else 0
        ep = self.ep.get(acc, 0)
        opened = conn.execute("SELECT symbol FROM short_trades WHERE acc=? AND status='open' AND t_signal>=?",
                              (acc, ep)).fetchall()
        lim = int(self.x.get("grid_max", 3)) if kind == "grid" else int(self.x.get("pos_max", 5))
        why = None
        if kind == "s1" and risk_pct > float(self.x.get("s1_max_stop", 0.15)) * 100:
            why = f"стоп дальше 15%: {risk_pct:.1f}%"
        elif any(o[0] == sym for o in opened):
            why = "уже в позиции по монете"
        elif len(opened) >= lim:
            why = f"лимит: открыто {len(opened)}"
        status = "skipped" if why else "open"
        conn.execute("INSERT OR IGNORE INTO short_trades(acc, symbol, t_signal, entry, stop, target, risk_pct, fills, avg, "
                     "notional, balance_in, status, src, last_t, max_high, min_low, M, reason, ms, pump_pct, ep_t_base, "
                     "rsi4h, rsi4h_max, taker_1h, pump_hours) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'live',?,?,?,?,?,?,?,?,?,?,?,?)",
                     (acc, sym, t_k, E, stop, tgt, risk_pct, json.dumps([[E, t_k]]), E, notional, bal, status, t_k, E, E,
                      M, why, now_ms(), snap["pump"] * 100, tb, _n(snap.get("rsi4h")), _n(snap.get("rsi4h_max")),
                      _n(snap.get("taker_1h")), _n(snap.get("pump_hours"))))
        log.info("шорт %s: %s %s по %.6g (памп +%.0f%%)%s", acc, "вход" if not why else "пропуск", sym, E,
                 snap["pump"] * 100, f" — {why}" if why else "")

    def _virtual(self, conn, acc, sym, t_k, tb, snap, t_first) -> None:
        kind = KIND[acc]
        E, M = snap["close"], snap["M"]
        stop, tgt, risk_pct = levels_for(kind, E, M)
        fp = conn.execute("SELECT s1_pump, s2_pump FROM pump_episodes WHERE symbol=? AND t_base=?", (sym, tb)).fetchone()
        first_pump = (fp[0] if kind == "s1" else fp[1]) if fp else None
        skip = kind == "s1" and risk_pct > 15
        conn.execute("INSERT OR IGNORE INTO virtual_trades(acc, symbol, t_signal, t_first, first_pump, pump_pct, ep_t_base, "
                     "status, src, entry, stop, target, risk_pct, fills, avg, last_t, max_high, min_low, M, reason, ms) "
                     "VALUES(?,?,?,?,?,?,?,?,'live',?,?,?,?,?,?,?,?,?,?,?,?)",
                     (acc, sym, t_k, t_first, first_pump, snap["pump"] * 100, tb, "skipped" if skip else "open", E, stop,
                      tgt, risk_pct, json.dumps([[E, t_k]]), E, t_k, E, E, M,
                      "стоп дальше 15%" if skip else None, now_ms()))
        log.info("поздний сигнал %s: %s (памп +%.0f%%, первый был +%s%%) — виртуальная сделка", acc, sym,
                 snap["pump"] * 100, f"{first_pump:.0f}" if first_pump is not None else "?")

    def _manage(self, conn, tbl: str, tid: int, t_k: int) -> None:
        r = conn.execute(f"SELECT acc, symbol, t_signal, entry, stop, target, risk_pct, fills, last_t, max_high, min_low, M"
                         f"{', notional' if tbl == 'short_trades' else ''} FROM {tbl} WHERE id=?", (tid,)).fetchone()
        acc, sym, ts, E, stop, tgt, risk_pct, fills_s, last_t, mx, mn, M = r[:12]
        notional = r[12] if tbl == "short_trades" else None
        kind = KIND[acc]
        fills = json.loads(fills_s)
        h, l, c, _ = load_sym(conn, sym, last_t + BAR_MS, t_k)
        E0 = fills[0][0]
        for i in range(len(c)):
            tj = last_t + (i + 1) * BAR_MS
            j = (tj - ts) // BAR_MS
            if np.isfinite(h[i]):
                mx = max(mx, h[i])
            if np.isfinite(l[i]):
                mn = min(mn, l[i])
            n0 = len(fills)
            px, reason, pnl, tgt = step_trade(kind, E, stop, tgt, fills, h[i], l[i], c[i], j)
            for f in fills[n0:]:
                f[1] = tj
            if reason:
                R = pnl / risk_pct if kind != "grid" else pnl / GRID_LOSS
                avg = len(fills) / sum(1 / f for f, _ in fills)
                usd = notional * pnl / 100 if notional is not None else None
                extra = ", pnl_usd=?" if tbl == "short_trades" else ""
                args = [tj, px, reason, pnl, R] + ([usd] if tbl == "short_trades" else []) + [
                    json.dumps(fills), avg, tgt, tj, mx, mn, (mx / E0 - 1) * 100, int(mx > M), tid]
                conn.execute(f"UPDATE {tbl} SET status='closed', exit_t=?, exit_price=?, reason=?, pnl_pct=?, R=?{extra}, "
                             f"fills=?, avg=?, target=?, last_t=?, max_high=?, min_low=?, mae_pct=?, new_high=? WHERE id=?",
                             args)
                log.info("%s %s: выход %s — %s, %.2f%% (%.3fR)", "шорт" if tbl == "short_trades" else "виртуально",
                         acc, sym, reason, pnl, R)
                return
        avg = len(fills) / sum(1 / f for f, _ in fills)
        conn.execute(f"UPDATE {tbl} SET fills=?, avg=?, target=?, last_t=?, max_high=?, min_low=?, mae_pct=?, new_high=? "
                     f"WHERE id=?", (json.dumps(fills), avg, tgt, t_k, mx, mn, (mx / E0 - 1) * 100, int(mx > M), tid))

    # ================================================================ v0.6: признаки сигналов
    async def enrich(self, limit: int = 6) -> None:
        """Признаки для сигналов Ш1/Ш2, у которых их ещё нет (новые — сразу, старые — понемногу за каждый проход)."""
        rows = await self.db.read("SELECT acc, symbol, t, t_base FROM short_signals WHERE feat_ms IS NULL "
                                  "AND acc IN ('Ш1','Ш2') AND t_base IS NOT NULL ORDER BY t DESC LIMIT ?", (limit,))
        for acc, sym, t, tb in rows:
            ext = await _fetch_ext(self.rest, sym, t, tb)
            f = await self.db.run_in_thread(lambda conn: features_job(conn, sym, t, tb, ext))
            self.db.put("UPDATE short_signals SET " + ", ".join(f"{k}=?" for k in FEATS) +
                        ", feat_ms=?, feat_note=? WHERE acc=? AND symbol=? AND t=?",
                        [tuple(f[k] for k in FEATS) + (now_ms(), ext.get("note") or None, acc, sym, t)])

    # ================================================================ история v0.6 (один раз)
    async def backfill_v06(self) -> None:
        if await self.db.read("SELECT 1 FROM engine_status WHERE key='shorts_v06_backfill'"):
            return
        await self.db.flush()

        def job(conn):
            reg = conn.execute("SELECT t, regime, mkt24 FROM market_regime ORDER BY t").fetchall()
            r = conn.execute("SELECT value FROM engine_status WHERE key='shorts_last_t'").fetchone()
            if not reg or not r:
                return {}
            t_end = int(r[0])
            S0, n, lab, m24 = _regime_arrays(conn, reg, t_end, H1)
            cnt = {"Ш1-c": 0, "Ш1-d": 0}
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for s in [x[0] for x in conn.execute("SELECT DISTINCT symbol FROM raw_klines_5m")]:
                    h, l, c, q = load_sym(conn, s, S0, t_end)
                    _, tbq = load_flow(conn, s, S0, t_end)
                    uni = ref04.universe_mask(q[:, None])[0][:, 0]
                    r15, r1 = ref04.rsi_15m_1h(c)
                    T = lambda i: S0 + i * BAR_MS
                    for k, tb in ref04.pump_episodes(h, l, uni):
                        sig = ref04.scan_episode(h, l, c, r15, r1, k, tb, lab, m24)
                        if "S1" not in sig:
                            continue
                        sn = sig["S1"]
                        f = ref06.short_signal_features(sn["k"], tb, q, tbq)
                        acc = ref06.accounts_v06(f)
                        for name, key in (("Ш1-c", "S1c"), ("Ш1-d", "S1d")):
                            if acc[key]:
                                cnt[name] += _model(conn, name, s, h, l, c, sn["k"], sn, n, T, tb)
            conn.execute("INSERT OR REPLACE INTO engine_status(key, value, t) VALUES('shorts_v06_backfill', ?, ?)",
                         (json.dumps(cnt, ensure_ascii=False), now_ms()))
            conn.commit()
            return cnt

        cnt = await self.db.run_in_thread(job)
        log.info("шорты v0.6: по истории модельных сделок %s", cnt)

    # ================================================================ история v0.4 (один раз)
    async def backfill(self) -> None:
        if await self.db.read("SELECT 1 FROM engine_status WHERE key='shorts_backfill'"):
            return
        await self.db.flush()

        def job(conn):
            reg = conn.execute("SELECT t, regime, mkt24 FROM market_regime ORDER BY t").fetchall()
            if not reg:
                return 0, 0, 0
            t_end = reg[-1][0]
            S0, n, lab, m24 = _regime_arrays(conn, reg, t_end, H1)
            ne = ns = nt = 0
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for s in [r[0] for r in conn.execute("SELECT DISTINCT symbol FROM raw_klines_5m")]:
                    h, l, c, q = load_sym(conn, s, S0, t_end)
                    uni, q24 = ref04.universe_mask(q[:, None])
                    uni = uni[:, 0]
                    r15, r1 = ref04.rsi_15m_1h(c)
                    T = lambda i: S0 + i * BAR_MS
                    for k, tb in ref04.pump_episodes(h, l, uni):
                        out = ref04.scan_episode(h, l, c, r15, r1, k, tb, lab, m24)
                        conn.execute("INSERT OR IGNORE INTO pump_episodes(symbol, t_base, start_t, base, q24, s1_t, s2_t, "
                                     "src, ms) VALUES(?,?,?,?,?,?,?,'backfill',?)",
                                     (s, T(tb), T(k), float(l[tb]), _n(q24[k, 0]),
                                      T(out["S1"]["k"]) if "S1" in out else None,
                                      T(out["S2"]["k"]) if "S2" in out else None, now_ms()))
                        ne += 1
                        for acc, key in (("Ш1", "S1"), ("Ш2", "S2")):
                            if key not in out:
                                continue
                            sn = out[key]
                            ks = sn["k"]
                            conn.execute("INSERT OR IGNORE INTO short_signals(acc, symbol, t, t_base, M, base, close, rsi15, "
                                         "rsi15_max, rsi1h, mkt24, regime, src, ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,"
                                         "'backfill',?)", (acc, s, T(ks), T(tb), float(sn["M"]), float(sn["base"]),
                                                          float(sn["close"]), _n(sn["rsi15"]), _n(sn["rsi15_max"]),
                                                          _n(sn["rsi1h"]), _n(sn["mkt24"]), sn["regime"], now_ms()))
                            ns += 1
                            nt += _model(conn, acc, s, h, l, c, ks, sn, n, T, tb)
            conn.execute("INSERT OR REPLACE INTO engine_status(key, value, t) VALUES('shorts_backfill', ?, ?)",
                         (f"{ne}/{ns}/{nt}", now_ms()))
            conn.execute("INSERT OR REPLACE INTO engine_status(key, value, t) VALUES('shorts_last_t', ?, ?)",
                         (str(t_end - BAR_MS), now_ms()))
            conn.commit()
            return ne, ns, nt

        ne, ns, nt = await self.db.run_in_thread(job)
        self.last_t = None
        log.info("шорты v0.4: по истории эпизодов %d, сигналов %d, модельных сделок %d", ne, ns, nt)

    # ================================================================ история v0.5 (один раз)
    async def backfill_v05(self) -> None:
        if await self.db.read("SELECT 1 FROM engine_status WHERE key='shorts_v05_backfill'"):
            return
        await self.db.flush()

        def job(conn):
            reg = conn.execute("SELECT t, regime, mkt24 FROM market_regime ORDER BY t").fetchall()
            r = conn.execute("SELECT value FROM engine_status WHERE key='shorts_last_t'").fetchone()
            if not reg or not r:
                return {}
            t_end = int(r[0])                                     # последний бар, обработанный вживую
            S0, n, lab, m24 = _regime_arrays(conn, reg, t_end, DAY)   # от 00:00 UTC — нужно для RSI 4h
            cnt = dict(m1b=0, m2b=0, m2c=0, v=0, e7=0, m3=0, watch=0)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for s in [x[0] for x in conn.execute("SELECT DISTINCT symbol FROM raw_klines_5m")]:
                    h, l, c, q = load_sym(conn, s, S0, t_end)
                    uni = ref04.universe_mask(q[:, None])[0][:, 0]
                    r15, r1 = ref04.rsi_15m_1h(c)
                    r4 = ref05.rsi_4h(c)
                    T = lambda i: S0 + i * BAR_MS
                    for k, tb in ref04.pump_episodes(h, l, uni):
                        sig = ref04.scan_episode(h, l, c, r15, r1, k, tb, lab, m24)
                        late = ref05.late_signals_v05(h, l, c, r15, r1, k, tb, lab, m24)
                        upd = {}
                        alive = tb + LIFE > n - 1
                        for key, kfirst, lk, accs, ok_f in (
                                ("S1", "s1", "S1b", ("Ш1-b",), s1b_ok),
                                ("S2", "s2", "S2bc", ("Ш2-b", "Ш2-c"), lambda p: p >= S2B_MIN)):
                            if key not in sig:
                                continue
                            sn = sig[key]
                            p = sn["M"] / sn["base"] - 1
                            upd[f"{kfirst}_pump"] = p * 100
                            if ok_f(p):
                                upd[f"late{kfirst[1]}"] = "none"
                                for acc in accs:
                                    cnt[{"Ш1-b": "m1b", "Ш2-b": "m2b", "Ш2-c": "m2c"}[acc]] += \
                                        _model(conn, acc, s, h, l, c, sn["k"], sn, n, T, tb, pump=p)
                            elif lk in late:
                                upd[f"late{kfirst[1]}"] = "done"
                                for acc in accs:
                                    cnt["v"] += _virtual_model(conn, acc, s, h, l, c, late[lk], n, T, tb, T(sn["k"]),
                                                               p * 100)
                            elif alive:
                                upd[f"late{kfirst[1]}"] = "watch"
                                cnt["watch"] += 1
                            else:
                                upd[f"late{kfirst[1]}"] = "none"
                        if upd:
                            conn.execute("UPDATE pump_episodes SET " + ", ".join(f"{a}=?" for a in upd) +
                                         " WHERE symbol=? AND t_base=?", list(upd.values()) + [s, T(tb)])
                    for k, tb, tp in ref05.pump_episodes_7d(h, l, uni):
                        done = k + ref05.S3_SCAN <= n - 1
                        s3 = ref05.scan_episode_s3(h, l, c, r4, k, tb)
                        conn.execute("INSERT OR IGNORE INTO pump_episodes_7d(symbol, t_base, start_t, t_peak, base, peak, "
                                     "s3_t, src, ms) VALUES(?,?,?,?,?,?,?,'backfill',?)",
                                     (s, T(tb), T(k), T(tp) if done else None, float(l[tb]),
                                      float(h[tp]) if done else None, T(s3["k"]) if s3 else None, now_ms()))
                        cnt["e7"] += 1
                        if s3:
                            conn.execute("INSERT OR IGNORE INTO short_signals(acc, symbol, t, t_base, M, base, close, src, "
                                         "ms) VALUES('Ш3',?,?,?,?,?,?,'backfill',?)",
                                         (s, T(s3["k"]), T(tb), float(s3["M"]), float(s3["base"]), float(s3["close"]),
                                          now_ms()))
                            cnt["m3"] += _model(conn, "Ш3", s, h, l, c, s3["k"], s3, n, T, tb, pump=s3["pump_pct"] / 100,
                                                rsi4=(s3["rsi4h"], s3["rsi4h_max"]))
            conn.execute("INSERT OR REPLACE INTO engine_status(key, value, t) VALUES('shorts_v05_backfill', ?, ?)",
                         (json.dumps(cnt), now_ms()))
            conn.commit()
            return cnt

        cnt = await self.db.run_in_thread(job)
        log.info("шорты v0.5: по истории %s", cnt)


# ================================================================ v0.6: признаки сигналов (журнал)
FEATS = ("pump_hours", "taker_share_1h", "taker_share_pump", "vol_1h_vs_pump", "vol_pump_vs_day_before", "oi_pump_pct",
         "oi_1h_pct", "oi_from_max_pct", "oi_usd", "oi_to_turnover", "ls_pct_week", "ls_pump_pct", "ls_1h_pct",
         "top_ls_pos", "top_ls_pos_pump_pct", "taker_ratio_1h", "funding_h", "funding_max_pump_h")


async def _fetch_ext(rest, sym: str, t_k: int, tb: int) -> dict:
    """Метрики, которых нет в базе для любой монеты: taker buy/sell ratio, L/S топ-трейдеров по позициям (REST
    /futures/data — хранятся у Binance 30 дней) и история funding. Метка API X = конец бара X − 5 мин."""
    out, notes = {}, []
    if rest is None:
        return dict(note="нет REST")
    try:
        d = await rest.get("/futures/data/takerlongshortRatio", {"symbol": sym, "period": "5m",
                                                                 "startTime": t_k - 11 * BAR_MS + BAR_MS,
                                                                 "endTime": t_k + BAR_MS, "limit": 30}, fd=True)
        out["tk"] = {int(x["timestamp"]) - BAR_MS: float(x["buySellRatio"]) for x in d or []}
    except Exception as e:                                       # noqa: BLE001
        notes.append(f"taker ratio: {type(e).__name__}")
    try:
        d = await rest.get("/futures/data/topLongShortPositionRatio", {"symbol": sym, "period": "5m",
                                                                       "startTime": tb + BAR_MS, "endTime": t_k + BAR_MS,
                                                                       "limit": 500}, fd=True)
        out["tp"] = {int(x["timestamp"]) - BAR_MS: float(x["longShortRatio"]) for x in d or []}
    except Exception as e:                                       # noqa: BLE001
        notes.append(f"top L/S: {type(e).__name__}")
    try:
        d = await rest.get("/fapi/v1/fundingRate", {"symbol": sym, "startTime": tb - 3 * DAY, "endTime": t_k + BAR_MS,
                                                     "limit": 200}, weight=1)
        out["fr"] = [(int(x["fundingTime"]), float(x["fundingRate"])) for x in d or []]
    except Exception as e:                                       # noqa: BLE001
        notes.append(f"funding: {type(e).__name__}")
    out["note"] = "; ".join(notes)
    return out


def features_job(conn, sym: str, t_k: int, tb: int, ext: dict) -> dict:
    """Признаки сигнала функцией эталона ref06.short_signal_features по окну с недельной историей L/S."""
    S = min(tb - 288 * BAR_MS, t_k - 2016 * BAR_MS)
    n = (t_k - S) // BAR_MS + 1
    k, tbi = (t_k - S) // BAR_MS, (tb - S) // BAR_MS
    q, tbq = load_flow(conn, sym, S, t_k)
    _, _, c, _ = load_sym(conn, sym, S, t_k)
    oi, oiv = np.full(n, np.nan), np.full(n, np.nan)
    for t, live, hist, husd in conn.execute("SELECT t, oi_live, oi_hist, oi_hist_usd FROM raw_oi WHERE symbol=? AND t>=? "
                                            "AND t<=?", (sym, S, t_k)):
        j = (t - S) // BAR_MS
        v = live if live is not None else hist
        if v is not None:
            oi[j] = v
            oiv[j] = husd if husd is not None else (v * c[j] if np.isfinite(c[j]) else np.nan)
    ga, tp, tk = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    for t, kind, r in conn.execute("SELECT t, kind, ratio FROM raw_ls WHERE symbol=? AND t>=? AND t<=? "
                                   "AND kind IN ('global','top_pos')", (sym, S, t_k)):
        (ga if kind == "global" else tp)[(t - S) // BAR_MS] = r
    for src, arr in ((ext.get("tp") or {}, tp), (ext.get("tk") or {}, tk)):
        for t, v in src.items():
            if S <= t <= t_k:
                arr[(t - S) // BAR_MS] = v
    fi = fr = None
    if ext.get("fr"):
        fh = conn.execute("SELECT funding_h FROM symbols WHERE symbol=?", (sym,)).fetchone()
        hrs = float(fh[0]) if fh and fh[0] else 8.0
        pts = [((ft - S) // BAR_MS, rate * 100 / hrs) for ft, rate in ext["fr"]]
        fi = np.array([p[0] for p in pts], dtype=np.int64)
        fr = np.array([p[1] for p in pts], dtype=float)
    q24 = np.nansum(q[max(0, k - 287):k + 1]) / 1e6
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        f = ref06.short_signal_features(k, tbi, q, tbq, oi, oiv, ga, tp, tk, q24, fi, fr)
    return {key: _n(f.get(key)) for key in FEATS}


def _regime_arrays(conn, reg, t_end: int, align: int):
    first = conn.execute("SELECT MIN(t) FROM raw_klines_5m").fetchone()[0]
    S0 = first // align * align
    n = (t_end - S0) // BAR_MS + 1
    lab = np.array([""] * n, dtype=object)
    m24 = np.full(n, np.nan)
    for t, rg, m in reg:
        k = (t - S0) // BAR_MS
        if 0 <= k < n:
            lab[k], m24[k] = rg or "", np.nan if m is None else m
    return S0, n, lab, m24


def _model(conn, acc, s, h, l, c, ks, sn, n, T, tb, pump=None, rsi4=None) -> int:
    """Модельная сделка эталона по истории (status = model), если её горизонт целиком в данных."""
    kind = KIND[acc]
    if ks + HZ[kind] > n - 1:
        return 0
    if kind == "s1":
        tr = ref04.trade_S1(h, l, c, ks, sn["M"])
        if tr is None:
            return 0
        fills, pnl = [[float(c[ks]), T(ks)]], tr["pnl_pct"]
    elif kind == "single":
        tr = ref05.trade_single_short(h, l, c, ks)
        fills, pnl = [[float(c[ks]), T(ks)]], tr["pnl_pct"]
    else:
        tr = ref05.trade_grid(h, l, c, ks)
        fills, pnl = [[float(f), T(j)] for f, j in tr["fills"]], tr["pnl_unit_pct"]
    avg = len(fills) / sum(1 / f for f, _ in fills)
    p = pump if pump is not None else sn["M"] / sn["base"] - 1
    conn.execute("INSERT OR IGNORE INTO short_trades(acc, symbol, t_signal, status, src, entry, fills, avg, M, exit_t, "
                 "exit_price, reason, pnl_pct, R, ms, pump_pct, ep_t_base, rsi4h, rsi4h_max) "
                 "VALUES(?,?,?,'model','backfill',?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (acc, s, T(ks), float(c[ks]), json.dumps(fills), avg, float(sn["M"]), T(tr["exit_k"]),
                  float(tr["exit_price"]), tr["reason"], float(pnl), float(tr["R"]), now_ms(), p * 100, T(tb),
                  rsi4[0] if rsi4 else None, rsi4[1] if rsi4 else None))
    return 1


def _virtual_model(conn, acc, s, h, l, c, sn, n, T, tb, t_first, first_pump) -> int:
    """Поздний сигнал по истории: виртуальная сделка эталона. Не успела закончиться — открытая (дальше ведёт движок)."""
    kind = KIND[acc]
    ks = sn["k"]
    E, M = float(c[ks]), float(sn["M"])
    stop, tgt, risk_pct = levels_for(kind, E, M)
    base = (acc, s, T(ks), t_first, first_pump, sn["pump_pct"], T(tb))
    if kind == "s1" and risk_pct > 15:
        conn.execute("INSERT OR IGNORE INTO virtual_trades(acc, symbol, t_signal, t_first, first_pump, pump_pct, ep_t_base, "
                     "status, src, entry, stop, target, risk_pct, reason, ms) VALUES(?,?,?,?,?,?,?,'skipped','backfill',"
                     "?,?,?,?,'стоп дальше 15%',?)", base + (E, stop, tgt, risk_pct, now_ms()))
        return 1
    if ks + HZ[kind] > n - 1:                     # не закончилась — открытая, движок доведёт по барам
        conn.execute("INSERT OR IGNORE INTO virtual_trades(acc, symbol, t_signal, t_first, first_pump, pump_pct, ep_t_base, "
                     "status, src, entry, stop, target, risk_pct, fills, avg, last_t, max_high, min_low, M, ms) "
                     "VALUES(?,?,?,?,?,?,?,'open','backfill',?,?,?,?,?,?,?,?,?,?,?)",
                     base + (E, stop, tgt, risk_pct, json.dumps([[E, T(ks)]]), E, T(ks), E, E, M, now_ms()))
        return 1
    if kind == "s1":
        tr = ref04.trade_S1(h, l, c, ks, M)
        fills, pnl = [[E, T(ks)]], tr["pnl_pct"]
    elif kind == "single":
        tr = ref05.trade_single_short(h, l, c, ks)
        fills, pnl = [[E, T(ks)]], tr["pnl_pct"]
    else:
        tr = ref05.trade_grid(h, l, c, ks)
        fills, pnl = [[float(f), T(j)] for f, j in tr["fills"]], tr["pnl_unit_pct"]
    avg = len(fills) / sum(1 / f for f, _ in fills)
    conn.execute("INSERT OR IGNORE INTO virtual_trades(acc, symbol, t_signal, t_first, first_pump, pump_pct, ep_t_base, "
                 "status, src, entry, stop, target, risk_pct, fills, avg, M, exit_t, exit_price, reason, pnl_pct, R, ms) "
                 "VALUES(?,?,?,?,?,?,?,'model','backfill',?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 base + (E, stop, tgt, risk_pct, json.dumps(fills), avg, M, T(tr["exit_k"]), float(tr["exit_price"]),
                         tr["reason"], float(pnl), float(tr["R"]), now_ms()))
    return 1
