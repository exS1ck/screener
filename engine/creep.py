"""Этап 7: список «ползут сейчас».

Эпизоды — как ref.creep_episodes: вход при росте за 6 ч (close к close) ≥ 5% и обороте в коридоре
$0.5–30M, если с прошлого выхода прошло ≥ 24 бара; выход при росте < 1% или пропуске данных.
Признаки на баре входа t: процентиль L/S по ls[t], OI за 6 ч = oi[t]/oi[t-72]. В список (in_list)
монета попадает, если L/S в нижних 20% недели и OI за 6 ч больше +3%.
Живое отличие: L/S на конец бара входа публикуется с задержкой — пока его нет, берём последнюю
точку (ls_exact = 0) и пересчитываем, когда точная появится.
"""
import logging
from dataclasses import dataclass

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, DAY_BARS, floor_bar, fmt, now_ms
from .context import grid
from .live import ffill_tail
from .sparks import load_bars

log = logging.getLogger("creep")
WEEK = 7 * DAY_BARS


@dataclass
class CState:
    in_ep: bool = False
    t_in: int | None = None
    last_exit: int = -10 ** 15


class Creep:
    def __init__(self, cfg: dict, db: D.Db, col):
        self.w = cfg["watchlist"]
        self.db = db
        self.col = col
        self.st: dict[str, CState] = {}
        self.done: int | None = None

    # ---------------------------------------------------------------- признаки на баре
    def features(self, conn, sym: str, t: int) -> dict:
        """L/S-процентиль по ls[t] и OI за 6 ч на баре с открытием t."""
        t_from = t - (WEEK + 2) * BAR_MS
        n = (t + BAR_MS - t_from) // BAR_MS
        k = n - 1
        ls = grid(conn, "SELECT t, ratio FROM raw_ls WHERE symbol=? AND kind='global' AND t>=? AND t<=?",
                  (sym, t_from, t), t_from, n)
        oi = grid(conn, "SELECT t, COALESCE(oi_live, oi_hist) FROM raw_oi WHERE symbol=? AND t>=? AND t<=?",
                  (sym, t_from, t), t_from, n)
        exact = bool(np.isfinite(ls[k])) and np.isfinite(ls[k - WEEK:k]).sum() >= int(self.w["ls_min_history"])
        ls_f, _ = ffill_tail(ls, k - 12)                 # не больше часа назад
        g = ref.ls_percentile_at(ls_f, k)
        oi6 = (oi[k] / oi[k - 72] - 1) * 100 if np.isfinite(oi[k]) and np.isfinite(oi[k - 72]) else np.nan
        return dict(ls_pct=g, ls_exact=int(exact), oi6=oi6)

    def in_list(self, f: dict) -> int:
        return int(bool(np.isfinite(f["ls_pct"]) and f["ls_pct"] < ref.TH["ls_low"]
                        and np.isfinite(f["oi6"]) and f["oi6"] > float(self.w["creep_oi6_min"])))

    # ---------------------------------------------------------------- проход по барам
    def _scan(self, conn, syms: list[str], start: int, b: int, write_from: int, src: str):
        th_in, th_out = float(self.w["creep_in"]), float(self.w["creep_out"])
        cool = int(self.w["creep_cooldown_bars"]) * BAR_MS
        lo, hi = ref.TH["liq_min_musd"], ref.TH["liq_max_musd"]
        t_from = start - (DAY_BARS + 1) * BAR_MS
        ins, outs, now_rows, active = [], [], [], []
        det = now_ms()
        for sym in syms:
            st = self.st.setdefault(sym, CState())
            o, h, l, c, qv, tbq = load_bars(conn, sym, t_from, b)
            for t in range(start, b, BAR_MS):
                k = (t - t_from) // BAR_MS
                v = (c[k] / c[k - 72] - 1) * 100 if np.isfinite(c[k]) and np.isfinite(c[k - 72]) else np.nan
                if not st.in_ep:
                    q24 = ref.liq5(qv, k)
                    if (np.isfinite(v) and v >= th_in and np.isfinite(q24) and lo <= q24 <= hi
                            and t - st.last_exit >= cool):
                        st.in_ep, st.t_in = True, t
                        if t >= write_from:
                            f = self.features(conn, sym, t)
                            ins.append(["creep", sym, t, None, float(c[k]), float(v), _n(f["ls_pct"]), f["ls_exact"],
                                        _n(f["oi6"]), float(q24), None, None, None, self.in_list(f), None, None,
                                        None, None, src, det])
                else:
                    if not np.isfinite(v) or v < th_out:
                        outs.append((t, sym, st.t_in))
                        st.in_ep, st.last_exit, st.t_in = False, t, None
            if st.in_ep:
                active.append(sym)
                k = len(c) - 1
                f = self.features(conn, sym, b - BAR_MS)
                pre6 = (c[k] / c[k - 72] - 1) * 100
                r24 = (c[k] / c[k - DAY_BARS] - 1) * 100
                now_rows.append(["creep", sym, st.t_in, b - BAR_MS, _n(c[k]), _n(pre6), _n(r24), _n(f["oi6"]),
                                 _n(f["ls_pct"]), _n(ref.liq5(qv, k)), None])
        return ins, outs, now_rows, active

    async def bar_pass(self, b: int) -> None:
        syms = sorted(self.col.symbols)
        if self.done is None:
            # Старт: восстановить эпизоды прогоном по истории. Вход на самом первом баре прогона
            # может быть ложным (состояние до него неизвестно) — его не пишем.
            start = b - int(float(self.w["creep_replay_hours"]) * 3600_000)
            write_from = start + BAR_MS
            self.st = {}
            src = "replay"
        else:
            start, write_from, src = self.done, self.done, "live"
        if start >= b:
            return

        def job(conn):
            if src == "replay":
                conn.execute("DELETE FROM watch_now WHERE list='creep'")
            ins, outs, now_rows, active = self._scan(conn, syms, start, b, write_from, src)
            fixes = self._fix_ls(conn, b)
            res = self._outcomes(conn, b)
            gone = [(r[0], r[1]) for r in conn.execute("SELECT symbol, t_in FROM watch_now WHERE list='creep'")
                    if r[0] not in active]
            return ins, outs, now_rows, fixes, res, gone

        ins, outs, now_rows, fixes, res, gone = await self.db.run_in_thread(job)
        self.db.put(D.SQL_WATCH_EVENT, ins)
        self.db.put("UPDATE watchlist_events SET t_out=? WHERE list='creep' AND symbol=? AND t_in=?", outs)
        self.db.put("DELETE FROM watch_now WHERE list='creep' AND symbol=? AND t_in=?", gone)
        self.db.put(D.SQL_WATCH_NOW, now_rows)
        self.db.put("UPDATE watchlist_events SET ls_pct=?, ls_exact=?, oi6=?, in_list=? "
                    "WHERE list='creep' AND symbol=? AND t_in=?", fixes)
        self.db.put("UPDATE watchlist_events SET outcome=?, outcome_max=?, outcome_ms=? "
                    "WHERE list='creep' AND symbol=? AND t_in=?", res)
        self.done = b
        if src == "replay":
            log.info("«ползут»: история восстановлена, входов %d, сейчас ползут %d (в списке %d)",
                     len(ins), len(now_rows), sum(1 for r in ins if r[13] and r[3] is None))
        for r in ins:
            if src == "live" and r[13]:
                log.info("«ползут сейчас» +%s: рост 6 ч %+.1f%%, L/S %s%% недели, OI 6 ч %s",
                         r[1], r[5], f"{r[6]:.0f}" if r[6] is not None else "?",
                         f"{r[8]:+.1f}%" if r[8] is not None else "?")

    def _fix_ls(self, conn, b: int) -> list:
        """Входы, где L/S был неточный: пересчитать, если точка уже опубликована."""
        out = []
        for sym, t in conn.execute("SELECT symbol, t_in FROM watchlist_events WHERE list='creep' AND ls_exact=0 "
                                   "AND t_in>=?", (b - int(self.w["ls_fix_minutes"]) * 60_000,)).fetchall():
            f = self.features(conn, sym, t)
            if f["ls_exact"]:
                out.append((_n(f["ls_pct"]), 1, _n(f["oi6"]), self.in_list(f), sym, t))
        return out

    def _outcomes(self, conn, b: int) -> list:
        """+25% за 24 ч от close бара входа: max(h[t+1 : t+289]) / c[t] − 1 ≥ 0.25."""
        out = []
        rows = conn.execute("SELECT symbol, t_in, close_in FROM watchlist_events WHERE list='creep' "
                            "AND outcome IS NULL AND t_in+?<=?", (BAR_MS + DAY_BARS * BAR_MS, b)).fetchall()
        for sym, t, c0 in rows:
            hs = [r[0] for r in conn.execute("SELECT h FROM raw_klines_5m WHERE symbol=? AND t>? AND t<=?",
                                            (sym, t, t + DAY_BARS * BAR_MS)) if r[0] is not None]
            if not hs or not c0:
                out.append((None, None, now_ms(), sym, t))
                continue
            mx = max(hs) / c0 - 1
            out.append((int(mx >= 0.25), mx * 100, now_ms(), sym, t))
        return out


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if np.isfinite(x) else None
