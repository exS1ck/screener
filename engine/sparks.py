"""Этап 2: детекторы 5m- и 15m-искр.

Все окна и формулы — функции engine/ref.py (копия эталона исследования), здесь только
сборка массивов на сетке 5m и порядок отбора «первая за 12 ч».
Сетка: массив начинается с границы 15m, поэтому 15m-свеча i = 5m-бары 3i, 3i+1, 3i+2 (как в эталоне).
Пропущенный бар = NaN. Используются только закрытые бары.
"""
import asyncio
import logging
import math
from dataclasses import dataclass

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, DAY_BARS, floor_bar, fmt, now_ms

log = logging.getLogger("sparks")

M15 = 3 * BAR_MS
HOUR = 12 * BAR_MS


def floor15(ms: int) -> int:
    return ms - ms % M15


def _num(x):
    """numpy → Python; NaN/inf → NULL."""
    if x is None:
        return None
    x = float(x)
    return x if math.isfinite(x) else None


@dataclass
class SymState:
    # «Первая за 12 ч» по исследованию (first12r_*): от последней ПРИНЯТОЙ искры, отброшенные паузу не продлевают.
    last5: int = -10 ** 15        # T последней принятой 5m-искры
    last_med: int = -10 ** 15     # то же для 15m, база медиана
    last_mean: int = -10 ** 15    # то же для 15m, база среднее 20
    # «Первая за 12 ч» v0.2 (first12_*): буквально — за 12 ч до неё не было НИКАКОЙ искры-кандидата этого потока
    # (в проверенном обороте, 15m — с ростом от +2% без потолка). Повторная = не первая.
    last5c: int = -10 ** 15
    last_medc: int = -10 ** 15
    last_meanc: int = -10 ** 15


def load_bars(conn, sym: str, t_from: int, t_to: int) -> np.ndarray:
    """Массив [6, n] (o, h, l, c, qv, tbq) на сетке 5m для баров с открытием в [t_from, t_to)."""
    n = (t_to - t_from) // BAR_MS
    a = np.full((6, n), np.nan)
    for t, o, h, l, c, qv, tbq in conn.execute(
            "SELECT t,o,h,l,c,qv,tbq FROM raw_klines_5m WHERE symbol=? AND t>=? AND t<?", (sym, t_from, t_to)):
        a[:, (t - t_from) // BAR_MS] = (o, h, l, c, qv, tbq)
    return a


def scan(sym: str, a: np.ndarray, t_from: int, t5: list[int], t15: list[int], st: SymState,
         write_from: int, src: str, ver: int, chash: str, big_ok: bool = True,
         only_big: bool = False) -> tuple[list, list, list]:
    """Проверить цели t5 (открытия 5m-баров) и t15 (открытия 15m-свечей) по порядку времени.
    Состояние «первая за 12 ч» обновляется для всех целей, пишутся только t ≥ write_from.
    Возвращает (строки sparks_5m, строки sparks_15m, цели, где бара ещё нет)."""
    TH = ref.TH
    o, h, l, c, qv, tbq = a
    det = now_ms()
    rows5, rows15, missing = [], [], []

    # ---------------------------------------------------------------- 5m
    for t in sorted(t5):
        j = (t - t_from) // BAR_MS
        if j < DAY_BARS or j >= len(c):
            continue
        if not (np.isfinite(o[j]) and np.isfinite(c[j]) and np.isfinite(qv[j])):
            missing.append(("5m", t))
            continue
        ret = (c[j] / o[j] - 1) * 100
        if ret < TH["spark5_ret_min"]:
            continue
        _ok_research, d = ref.spark5_at(j, o, c, qv)
        base, q24 = d["base"], d["q24"]
        if not (np.isfinite(base) and base > 0
                and qv[j] >= TH["spark5_vx"] * base and np.isfinite(q24) and q24 >= TH["liq_min_musd"]):
            continue
        # «Большая свеча»: 5m-алерт без потолка +8%. Пишется с флагом big и в отборе «первая за 12 ч»
        # не участвует — поток исследования (потолок до отбора) остаётся как был.
        big = ret > TH["spark5_ret_max"]
        if big and not big_ok:
            continue
        tested = q24 <= TH["liq_max_musd"]
        gap5 = TH["spark5_gap_bars"] * BAR_MS
        first_r = first = None
        if tested and not big:                        # поток исследования: потолок +8% до отбора
            first_r = (t - st.last5) >= gap5
            if first_r:
                st.last5 = t
        if tested:                                    # v0.2: любая предыдущая искра (и «большая свеча») блокирует
            if not big:
                first = (t - st.last5c) >= gap5
            st.last5c = t
        if t >= write_from and (not only_big or big):
            inc = int(np.isnan(qv[j - DAY_BARS:j + 1]).sum())
            rows5.append((sym, t, _num(o[j]), _num(c[j]), _num(qv[j]), _num(ret), _num(base), _num(d["vx"]),
                          _num(q24), None if first is None else int(first), int(tested), inc,
                          src, det, ver, chash, int(big), None if first_r is None else int(first_r)))

    # ---------------------------------------------------------------- 15m
    if t15:
        b15 = ref.agg15(o, h, l, c, qv, tbq)
        for T in sorted(t15):
            jb = (T - t_from) // BAR_MS
            i = jb // 3
            if jb % 3 or jb < DAY_BARS or i >= len(b15["c"]):
                continue
            if not (np.isfinite(b15["o"][i]) and np.isfinite(b15["c"][i])):
                missing.append(("15m", T))
                continue
            ret = (b15["c"][i] / b15["o"][i] - 1) * 100
            if not ret >= TH["spark15_ret_min"]:
                continue
            q15 = b15["q"]
            rv_med = ref.relvol_median24(q15, i)
            rv_mean = ref.relvol_mean20(q15, i)
            q24 = ref.q24_15m(q15, i)
            liq_ok = bool(np.isfinite(q24) and q24 >= TH["liq_min_musd"])
            tested = bool(liq_ok and q24 <= TH["liq_max_musd"])
            cand_med = bool(liq_ok and np.isfinite(rv_med) and rv_med >= TH["spark15_relvol"])
            # Кандидат исследования: оборот внутри $0.5–30M входит в условие (как spark15_candidates).
            cand_mean = bool(tested and np.isfinite(rv_mean) and rv_mean >= TH["spark15_relvol"])
            if not (cand_med or cand_mean):
                continue
            gap = TH["spark15_gap_bars"] * M15
            fr_med = fr_mean = f_med = f_mean = None
            if cand_med and tested:
                fr_med = (T - st.last_med) >= gap              # исследование
                if fr_med:
                    st.last_med = T
                f_med = (T - st.last_medc) >= gap              # v0.2
                st.last_medc = T
            if cand_mean:
                fr_mean = (T - st.last_mean) >= gap
                if fr_mean:
                    st.last_mean = T
                f_mean = (T - st.last_meanc) >= gap
                st.last_meanc = T
            if T >= write_from:
                inc = int(np.isnan(qv[jb - DAY_BARS:jb + 3]).sum())
                b_ = lambda x: None if x is None else int(x)
                rows15.append((sym, T, _num(b15["o"][i]), _num(b15["h"][i]), _num(b15["l"][i]),
                               _num(b15["c"][i]), _num(q15[i]), _num(b15["tb"][i]), _num(ret), _num(rv_med),
                               _num(rv_mean), _num(q24), int(cand_med), int(cand_mean), b_(f_med), b_(f_mean),
                               int(ret <= TH["spark15_ret_max"]), int(tested), inc, src, det, ver, chash,
                               b_(fr_med), b_(fr_mean)))
    return rows5, rows15, missing


class SparkDetector:
    def __init__(self, cfg: dict, db: D.Db, col):
        self.cfg = cfg
        self.s = cfg["sparks"]
        self.db = db
        self.col = col
        self.ver = int(cfg["meta"]["config_version"])
        self.chash = cfg["_hash"]
        for k, v in cfg["thresholds"].items():
            if k not in ref.TH:
                raise KeyError(f"config [thresholds]: неизвестный ключ {k}")
            ref.TH[k] = type(ref.TH[k])(v) if not isinstance(ref.TH[k], tuple) else tuple(v)
        self.state: dict[str, SymState] = {}
        self.retry: dict[str, list] = {}
        self.ready = asyncio.Event()
        self.done = 0                                   # граница: всё, что закрылось до неё, проверено
        self.ctx = None                                 # ContextBuilder (этап 3), подключается в run_engine
        self.live = None                                # LiveTags (этап 6)
        self.ind = None                                 # IndicatorBuilder: RSI, MFI, NATR, форма свечи
        self.watchers: list = []                        # списки наблюдения (этап 7): bar_pass(b) на каждом баре
        self.n5 = self.n15 = 0

    def _st(self, sym: str) -> SymState:
        return self.state.setdefault(sym, SymState())

    async def _done_t(self) -> int | None:
        r = await self.db.read("SELECT value FROM engine_status WHERE key='spark_done_t'")
        return int(r[0][0]) if r else None

    def _set_done(self, b: int) -> None:
        self.done = b
        self.db.put(D.SQL_STATUS, [("spark_done_t", str(b), now_ms())])

    async def _seed_state(self, before: int) -> None:
        """Продолжение после перезапуска: состояние «первой за 12 ч» до момента before — из журнала."""
        self.state = {}
        for sql, attr in (
                ("SELECT symbol, MAX(t) FROM sparks_5m WHERE first12r=1 AND t<? GROUP BY symbol", "last5"),
                ("SELECT symbol, MAX(t) FROM sparks_15m WHERE first12r_med=1 AND t<? GROUP BY symbol", "last_med"),
                ("SELECT symbol, MAX(t) FROM sparks_15m WHERE first12r_mean=1 AND t<? GROUP BY symbol", "last_mean"),
                ("SELECT symbol, MAX(t) FROM sparks_5m WHERE liq_tested=1 AND t<? GROUP BY symbol", "last5c"),
                ("SELECT symbol, MAX(t) FROM sparks_15m WHERE cand_med=1 AND liq_tested=1 AND t<? GROUP BY symbol",
                 "last_medc"),
                ("SELECT symbol, MAX(t) FROM sparks_15m WHERE cand_mean=1 AND t<? GROUP BY symbol", "last_meanc")):
            for sym, t in await self.db.read(sql, (before,)):
                setattr(self._st(sym), attr, t)

    async def migrate_first12(self) -> None:
        """Один раз: старые флаги «первая за 12 ч» (по исследованию) переносятся в first12r_*, а first12_*
        пересчитываются по определению v0.2 по всему журналу; вслед за ними — флаги правил A/B/C."""
        if await self.db.read("SELECT 1 FROM engine_status WHERE key='first12_v2'"):
            return
        await self.db.flush()
        gap15 = ref.TH["spark15_gap_bars"] * M15
        gap5 = ref.TH["spark5_gap_bars"] * BAR_MS

        def job(conn):
            conn.execute("BEGIN")
            conn.execute("UPDATE sparks_15m SET first12r_med=first12_med, first12r_mean=first12_mean "
                         "WHERE first12r_med IS NULL AND first12r_mean IS NULL")
            conn.execute("UPDATE sparks_5m SET first12r=first12 WHERE first12r IS NULL")
            n15 = n5 = 0
            last: dict = {}
            upd = []
            for sym, t, cm, cn, tested in conn.execute(
                    "SELECT symbol, t, cand_med, cand_mean, liq_tested FROM sparks_15m ORDER BY symbol, t").fetchall():
                fm = fn = None
                if cm and tested:
                    fm = int(t - last.get((sym, "m"), -10 ** 15) >= gap15)
                    last[(sym, "m")] = t
                if cn:
                    fn = int(t - last.get((sym, "n"), -10 ** 15) >= gap15)
                    last[(sym, "n")] = t
                upd.append((fm, fn, sym, t))
            conn.executemany("UPDATE sparks_15m SET first12_med=?, first12_mean=? WHERE symbol=? AND t=?", upd)
            n15 = len(upd)
            upd = []
            for sym, t, tested, big in conn.execute(
                    "SELECT symbol, t, liq_tested, COALESCE(big, 0) FROM sparks_5m ORDER BY symbol, t").fetchall():
                f = None
                if tested:
                    if not big:
                        f = int(t - last.get((sym, "5"), -10 ** 15) >= gap5)
                    last[(sym, "5")] = t
                upd.append((f, sym, t))
            conn.executemany("UPDATE sparks_5m SET first12=? WHERE symbol=? AND t=?", upd)
            n5 = len(upd)
            # флаги правил: «первая за 12 ч» поменялась — пересчитать по сохранённым признакам
            rupd = []
            for (sym, t, conf, g, a2, p6, fm, fn, inr, tested) in conn.execute(
                    "SELECT symbol, t, conf, ls_pct, ask2_med, pre6h_log, first12_med, first12_mean, in_range, "
                    "liq_tested FROM sparks_15m WHERE conf_ms IS NOT NULL").fetchall():
                rr = ref.rules_abc(bool(conf), np.nan if g is None else g, np.nan if a2 is None else a2,
                                   np.nan if p6 is None else p6)
                med_ok = fm == 1 and inr == 1 and tested == 1
                mean_ok = fn == 1 and inr == 1
                rupd.append((int(med_ok and rr["A"]), int(med_ok and rr["B"]), int(med_ok and rr["C"]),
                             int(mean_ok and rr["A"]), int(mean_ok and rr["B"]), int(mean_ok and rr["C"]), sym, t))
            conn.executemany("UPDATE sparks_15m SET rA=?, rB=?, rC=?, rA_mean=?, rB_mean=?, rC_mean=? "
                             "WHERE symbol=? AND t=?", rupd)
            conn.execute("INSERT OR REPLACE INTO engine_status(key, value, t) VALUES('first12_v2', ?, ?)",
                         (f"{n15}/{n5}", now_ms()))
            conn.execute("COMMIT")
            return n15, n5, len(rupd)

        n15, n5, nr = await self.db.run_in_thread(job)
        log.info("«первая за 12 ч» пересчитана по v0.2: 15m — %d строк, 5m — %d, флаги правил — %d", n15, n5, nr)

    @staticmethod
    def targets(done: int, b: int) -> tuple[list[int], list[int]]:
        """Цели, закрывшиеся после done и до b: 5m-бары с открытием в [done, b),
        15m-свечи, у которых закрытие (T + 15 мин) попало в (done, b]."""
        t5 = list(range(done, b, BAR_MS))
        t15 = [T for T in range(floor15(done), b, M15) if done < T + M15 <= b]
        return t5, t15

    def _put(self, rows5: list, rows15: list, src: str) -> None:
        self.db.put(D.SQL_SPARK5, rows5)
        self.db.put(D.SQL_SPARK15, rows15)
        self.n5 += len(rows5)
        self.n15 += len(rows15)
        if src != "live":
            return
        for r in rows15:
            if r[16] and (r[12] or r[13]):           # in_range и кандидат (r[14] — первая за 12 ч v0.2)
                log.info("15m-искра %s %s: %+.1f%%, объём ×%s к медиане (×%s к среднему 20), оборот $%.1fM%s%s",
                         r[0], fmt(r[1]), r[8], f"{r[9]:.1f}" if r[9] else "-",
                         f"{r[10]:.1f}" if r[10] else "-", r[11] or 0,
                         ", первая за 12 ч" if r[14] else "", "" if r[17] else ", оборот не проверен")
        if rows5:
            log.info("5m-искр: %d (%s)", len(rows5), ", ".join(r[0] + (" (большая)" if r[16] else "")
                                                                for r in rows5[:10]))

    # ---------------------------------------------------------------- догон по истории
    async def catch_up(self) -> None:
        """Первый старт: искры по истории за catchup_hours, перед ней разгон «первой за 12 ч»
        на warmup_hours (не пишется). Перезапуск: с места остановки минус rescan_hours (досчитать то,
        что могло не записаться), состояние «первой за 12 ч» — из журнала до начала пересмотра."""
        await self.col.backfill_done.wait()
        await self.db.flush()
        await self.migrate_first12()
        b = floor_bar(now_ms())                         # бары с открытием < b закрыты
        done = await self._done_t()
        if done is None:
            start = floor15(b - int(self.s["catchup_hours"]) * HOUR)
            warm = floor15(start - int(self.s["warmup_hours"]) * HOUR)
        else:
            start = warm = floor15(done - int(float(self.s["rescan_hours"]) * HOUR))
            await self._seed_state(start)
        t5, t15 = self.targets(warm, b)
        t_from = floor15(warm) - DAY_BARS * BAR_MS
        syms = sorted(self.col.symbols)
        states = {s: self._st(s) for s in syms}
        ver, chash = self.ver, self.chash
        log.info("искры по истории: %s … %s (разгон с %s), %d монет", fmt(start), fmt(b), fmt(warm), len(syms))

        def job(conn):
            out5, out15 = [], []
            for s in syms:
                a = load_bars(conn, s, t_from, b)
                r5, r15, _ = scan(s, a, t_from, t5, t15, states[s], start, "backfill", ver, chash)
                out5 += r5
                out15 += r15
            return out5, out15

        r5, r15 = await self.db.run_in_thread(job)
        self._put(r5, r15, "backfill")
        self._set_done(b)
        await self.db.flush()
        n15 = sum(1 for r in r15 if r[16] and r[12])
        log.info("искры по истории: 5m — %d, 15m — %d строк (+2…+8%% по медиане — %d)", len(r5), len(r15), n15)
        await self.after_pass()
        self.ready.set()

    # ---------------------------------------------------------------- «большие свечи» по истории
    async def backfill_big(self) -> None:
        """Один раз: 5m-алерты «большая свеча» (> +8%) по уже накопленной истории. Они не влияют на
        «первую за 12 ч», поэтому их можно досчитать отдельно, не трогая остальной журнал."""
        await self.ready.wait()
        if await self.db.read("SELECT 1 FROM engine_status WHERE key='big5_backfill'"):
            return
        b = self.done
        start = b - int(float(self.s.get("big_backfill_days", 30)) * 24 * HOUR)
        t_from = start - (DAY_BARS + 1) * BAR_MS
        syms = sorted(self.col.symbols)
        ver, chash = self.ver, self.chash
        t5 = list(range(start, b, BAR_MS))

        def job(conn):
            out = []
            for s in syms:
                a = load_bars(conn, s, t_from, b)
                r5, _, _ = scan(s, a, t_from, t5, [], SymState(), start, "backfill", ver, chash, only_big=True)
                out += r5
            return out

        rows = await self.db.run_in_thread(job)
        self.db.put(D.SQL_SPARK5, rows)
        self.db.put(D.SQL_STATUS, [("big5_backfill", str(len(rows)), now_ms())])
        await self.db.flush()
        log.info("«большие свечи» 5m по истории: %d", len(rows))

    # ---------------------------------------------------------------- живой поиск
    async def live_loop(self) -> None:
        await self.ready.wait()
        delay = float(self.s["delay_sec"]) * 1000
        while True:
            now = now_ms()
            b = floor_bar(now)
            target = b + delay if now < b + delay else b + BAR_MS + delay
            await asyncio.sleep((target - now) / 1000)
            b = floor_bar(now_ms())
            await self.db.flush()                        # всё, что пришло по WS и REST, — в базу
            await self.live_pass(b)
            await self.after_pass()
            if self.live is not None:
                await self.db.flush()
                try:
                    await self.live.bar_pass(b)
                except Exception:
                    log.exception("ошибка живых тегов")
            for w in self.watchers:
                await self.db.flush()
                try:
                    await w.bar_pass(b)
                except Exception:
                    log.exception("ошибка списка наблюдения")

    async def after_pass(self) -> None:
        if self.ctx is None:
            return
        await self.db.flush()
        try:
            await self.ctx.process()
        except Exception:
            log.exception("ошибка расчёта контекста")
        if self.ind is not None:
            await self.db.flush()                        # подхват этого прохода — в базе до правила D
            try:
                await self.ind.process()
            except Exception:
                log.exception("ошибка расчёта индикаторов")

    async def live_pass(self, b: int) -> None:
        if b <= self.done:
            return
        t5_all, t15_all = self.targets(self.done, b)
        retry, self.retry = self.retry, {}
        syms = sorted(self.col.symbols)
        ver, chash = self.ver, self.chash
        states = {s: self._st(s) for s in syms}

        def job(conn):
            out5, out15, miss = [], [], {}
            for s in syms:
                rt = retry.get(s, [])
                t5 = t5_all + [t for k, t in rt if k == "5m"]
                t15 = t15_all + [t for k, t in rt if k == "15m"]
                t_from = floor15(min(t5 + t15)) - DAY_BARS * BAR_MS
                a = load_bars(conn, s, t_from, b)
                r5, r15, m = scan(s, a, t_from, t5, t15, states[s], 0, "live", ver, chash)
                out5 += r5
                out15 += r15
                # Повтор — один раз: бар, которого нет и при второй попытке, пропускаем.
                m = [x for x in m if x not in rt]
                if m:
                    miss[s] = m
            return out5, out15, miss

        r5, r15, miss = await self.db.run_in_thread(job)
        self.retry = miss
        self._put(r5, r15, "live")
        self._set_done(b)
        if miss:
            log.info("искры: у %d монет нет последнего бара, повторю на следующем проходе", len(miss))
