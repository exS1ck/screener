"""Этап 3: контекст на момент искры, подхват на T+30, балл, правила A/B/C.

Работает по журналу: каждый проход ищет искры, у которых ещё не посчитан контекст (ctx_ms)
или подхват (conf_ms), и досчитывает, когда данные готовы. Одинаково для живых искр и истории.
Формулы — функции engine/ref.py. Своё здесь только то, чего нет в эталоне:
неделя (max/min за 7 дней), funding на 8 ч, доля роста на закрытии шортов, сценарий провала.
"""
import logging
import math
from datetime import datetime, timezone

import numpy as np

from . import db as D
from . import ref
from .clock import BAR_MS, DAY_BARS, fmt, floor_bar, now_ms
from .sparks import M15, floor15, load_bars

log = logging.getLogger("context")

WEEK = 7 * DAY_BARS                        # 2016 баров
CTX = list(D.CTX_COLS)
CONF = list(D.CONF_COLS)
SQL_CTX15 = D.update_sql("sparks_15m", CTX)
SQL_CTX5 = D.update_sql("sparks_5m", CTX)
SQL_CONF = D.update_sql("sparks_15m", CONF)


def _n(x):
    if x is None:
        return None
    x = float(x)
    return x if math.isfinite(x) else None


def _b(x):
    return None if x is None else int(bool(x))


def grid(conn, sql: str, params: tuple, t_from: int, n: int) -> np.ndarray:
    a = np.full(n, np.nan)
    for t, v in conn.execute(sql, params):
        k = (t - t_from) // BAR_MS
        if 0 <= k < n and v is not None:
            a[k] = v
    return a


class Series:
    """Ряды одной монеты на общей сетке 5m от t_from (граница 15m)."""

    def __init__(self, conn, sym: str, t_from: int, t_to: int):
        self.t_from = t_from
        self.sym = sym
        self.o, self.h, self.l, self.c, self.qv, self.tbq = load_bars(conn, sym, t_from, t_to)
        n = len(self.c)
        self.ls = grid(conn, "SELECT t, ratio FROM raw_ls WHERE symbol=? AND kind='global' AND t>=? AND t<?",
                       (sym, t_from, t_to), t_from, n)
        self.oi = grid(conn, "SELECT t, COALESCE(oi_live, oi_hist) FROM raw_oi WHERE symbol=? AND t>=? AND t<?",
                       (sym, t_from, t_to), t_from, n)
        self.conn = conn
        self._b15 = None

    @property
    def b15(self):
        if self._b15 is None:
            self._b15 = ref.agg15(self.o, self.h, self.l, self.c, self.qv, self.tbq)
        return self._b15

    def j(self, t: int) -> int:
        return (t - self.t_from) // BAR_MS

    def at(self, a: np.ndarray, k: int) -> float:
        return a[k] if 0 <= k < len(a) else np.nan


class ContextBuilder:
    def __init__(self, cfg: dict, db: D.Db, col):
        self.x = cfg["context"]
        self.db = db
        self.col = col

    # ================================================================ контекст на T
    def context(self, s: Series, T: int, now: int, wait_ms: int) -> dict | None:
        j = s.j(T)
        ls_at = s.at(s.ls, j - 1)
        if not np.isfinite(ls_at) and now < T + wait_ms:
            return None                                       # точка L/S ещё не опубликована
        hw = s.h[max(0, j - WEEK):j]
        lw = s.l[max(0, j - WEEK):j]
        week = np.nan
        if np.isfinite(hw).sum() >= int(self.x["week_min_bars"]):
            week = (np.nanmax(hw) / np.nanmin(lw) - 1) * 100
        snaps = s.conn.execute("SELECT ask2, ask2_best FROM raw_depth_hourly WHERE symbol=? AND t>=? AND t<=?",
                               (s.sym, T - 24 * 3600_000, T - 6 * 3600_000)).fetchall()
        fr = s.conn.execute("SELECT rate FROM raw_funding WHERE symbol=? AND t<=? AND t>? ORDER BY t DESC LIMIT 1",
                            (s.sym, T, T - 3600_000)).fetchone()
        fh = (self.col.symbols.get(s.sym) or {}).get("funding_h") or 8.0
        return dict(
            ls_at=_n(ls_at),
            ls_pct=_n(ref.g_lvl_spark(s.ls, j)),
            pre6h_log=_n(ref.pre6h_log(s.o, s.c, j)) if j >= 72 else None,
            hour_msk=ref.hour_msk(datetime.fromtimestamp(T / 1000, timezone.utc).hour),
            ask2_med=_n(ref.ask2_median([r[0] for r in snaps])) if snaps else None,
            ask2_best_med=_n(ref.ask2_median([r[1] for r in snaps])) if snaps else None,
            ask2_n=len(snaps),
            week_range=_n(week),
            funding8=_n(fr[0] * 100 * 8 / fh) if fr and fr[0] is not None else None,
            ctx_ms=now,
        )

    # ================================================================ подхват на T+30
    def confirmation(self, s: Series, T: int, row: dict, ctx: dict, now: int, wait_ms: int) -> dict | None:
        j = s.j(T)
        i = j // 3
        if j + 5 >= len(s.c):
            return None                                       # бары подхвата ещё не закрылись
        bars_ok = bool(np.isfinite(s.c[j + 3:j + 6]).all() and np.isfinite(s.qv[j + 3:j + 6]).all())
        if not bars_ok and now < T + 30 * 60_000 + wait_ms:
            return None                                       # бар подхвата ещё не записан — ждём
        if not np.isfinite(s.oi[j + 5]) and now < T + 30 * 60_000 + wait_ms:
            return None                                       # ждём OI на T+30
        r = ref.confirmation(i, s.b15, s.l, s.c, s.qv, s.tbq)
        oi30 = ref.oi_30(s.oi, j)
        # Доля роста на закрытии шортов: рост цены в барах j..j+5 с падающим OI / весь рост за 30 мин.
        up_all = up_short = 0.0
        for k in range(j, j + 6):
            rk = s.c[k] / s.c[k - 1] - 1
            if np.isfinite(rk) and rk > 0:
                up_all += rk
                if np.isfinite(s.oi[k]) and np.isfinite(s.oi[k - 1]) and s.oi[k] < s.oi[k - 1]:
                    up_short += rk
        short = up_short / up_all * 100 if up_all > 0 else np.nan

        if r is None:
            conf, scen = None, "нет данных"
        else:
            conf = r["conf"]
            if conf:
                scen = "подхват"
            elif np.isfinite(r["vx3"]) and r["vx3"] < float(self.x["fade_vx"]):
                scen = "затухание"
            elif (np.isfinite(r["vx3"]) and r["vx3"] >= ref.TH["conf_vx"]
                  and np.isfinite(r["tbr3"]) and r["tbr3"] < ref.TH["conf_tbr"]
                  and np.isfinite(r["r3"]) and r["r3"] < 0):
                scen = "продавцы"
            else:
                scen = "прочее"

        g = ctx.get("ls_pct")
        g = np.nan if g is None else g
        p6 = ctx.get("pre6h_log")
        p6 = np.nan if p6 is None else p6
        a2 = ctx.get("ask2_med")
        a2 = np.nan if a2 is None else a2
        h_utc = datetime.fromtimestamp(T / 1000, timezone.utc).hour
        score = ref.score5(bool(conf), g, p6, oi30, h_utc)
        score_nan = int(conf is None) + int(not np.isfinite(g)) + int(not np.isfinite(p6)) + int(not np.isfinite(oi30))
        rules = ref.rules_abc(bool(conf), g, a2, p6)
        med_ok = row["first12_med"] == 1 and row["in_range"] == 1 and row["liq_tested"] == 1
        mean_ok = row["first12_mean"] == 1 and row["in_range"] == 1
        return dict(
            base5=_n(r["base5"]) if r else None, vx3=_n(r["vx3"]) if r else None,
            tbr3=_n(r["tbr3"]) if r else None, dd3=_n(r["dd3"]) if r else None,
            r3=_n(r["r3"]) if r else None, entry=_n(r["entry"]) if r else None,
            conf=_b(conf), scenario=scen,
            oi_t=_n(s.at(s.oi, j - 1)), oi_t30=_n(s.at(s.oi, j + 5)), oi30=_n(oi30), short_share=_n(short),
            oi_usd_t=_n(s.at(s.oi, j - 1) * s.o[j]) if j < len(s.o) else None,
            score=int(score), score_nan=score_nan,
            rA=int(med_ok and rules["A"]), rB=int(med_ok and rules["B"]), rC=int(med_ok and rules["C"]),
            rA_mean=int(mean_ok and rules["A"]), rB_mean=int(mean_ok and rules["B"]),
            rC_mean=int(mean_ok and rules["C"]),
            conf_ms=now,
        )

    # ================================================================ повторная искра (v0.2)
    def repeat_rows(self, conn) -> list:
        """Для искр с посчитанным подхватом: предыдущий кандидат той же монеты в каждом потоке
        (медиана: кандидат по медиане в обороте $0.5–30M; среднее 20: кандидат по среднему).
        На T+30 подхват предыдущей кандидатуры уже известен: она не позже T−15 мин."""
        now = now_ms()
        out = []
        rows = conn.execute("SELECT symbol, t, o, cand_med, cand_mean, liq_tested FROM sparks_15m "
                            "WHERE rep_ms IS NULL AND conf_ms IS NOT NULL").fetchall()
        for sym, T, o, cm, cn, tested in rows:
            vals = []
            for ok, cond in ((cm and tested, "cand_med=1 AND liq_tested=1"), (cn, "cand_mean=1")):
                if not ok:
                    vals += [None, None, None]
                    continue
                pr = conn.execute(f"SELECT t, c, conf FROM sparks_15m WHERE symbol=? AND t<? AND {cond} "
                                  f"ORDER BY t DESC LIMIT 1", (sym, T)).fetchone()
                if not pr:
                    vals += [None, None, None]
                    continue
                pt, pc, pconf = pr
                vals += [(T - pt) / 3_600_000, _n((o / pc - 1) * 100) if o and pc else None,
                         None if pconf is None else int(pconf)]
            out.append(vals + [now, sym, T])
        return out

    # ================================================================ проход
    async def process(self) -> None:
        now = now_ms()
        b = floor_bar(now)
        ctx_wait = int(float(self.x["ctx_wait_min"]) * 60_000)
        conf_wait = int(float(self.x["conf_wait_min"]) * 60_000)
        cols15 = ["symbol", "t", "first12_med", "first12_mean", "in_range", "liq_tested", "src", "conf_ms",
                  "entry AS old_entry", "oi30 AS old_oi30"] + CTX
        keys15 = [c.split(" AS ")[-1] for c in cols15]
        repair_from = b - int(float(self.x.get("repair_hours", 48)) * 3600_000)
        repair_oi_from = b - int(float(self.x.get("repair_oi_days", 8)) * 24 * 3600_000)

        def job(conn):
            # Плюс починка: подхват посчитан, а цены входа нет — значит, бара T+30 тогда не было в базе.
            # Пересчитываем, когда бары появились (свечи догружаются сверкой и проверкой дыр).
            # И починка OI за 30 минут: история OI догружается за 7 дней — у старых искр он появится позже.
            p15 = [dict(zip(keys15, r)) for r in conn.execute(
                f"SELECT {', '.join(cols15)} FROM sparks_15m WHERE ((ctx_ms IS NULL OR conf_ms IS NULL) "
                f"OR (conf_ms IS NOT NULL AND entry IS NULL AND t>=?) "
                f"OR (conf_ms IS NOT NULL AND oi30 IS NULL AND t>=?)) AND t+?<=?",
                (repair_from, repair_oi_from, M15, b))]
            p5 = [dict(symbol=r[0], t=r[1]) for r in conn.execute(
                "SELECT symbol, t FROM sparks_5m WHERE ctx_ms IS NULL AND t+?<=?", (BAR_MS, b))]
            by: dict[str, list] = {}
            for r in p15:
                by.setdefault(r["symbol"], []).append(("15m", r))
            for r in p5:
                by.setdefault(r["symbol"], []).append(("5m", r))
            out_c15, out_c5, out_conf = [], [], []
            for sym, items in by.items():
                tmin = min(r["t"] for _, r in items)
                tmax = max(r["t"] for _, r in items)
                t_from = floor15(tmin - (WEEK + 3) * BAR_MS)
                s = Series(conn, sym, t_from, min(b, tmax + 30 * 60_000))
                for kind, r in items:
                    T = r["t"]
                    if kind == "5m":
                        c = self.context(s, T, now, BAR_MS + ctx_wait)
                        if c:
                            out_c5.append([c[k] for k in CTX] + [sym, T])
                        continue
                    ctx = {k: r[k] for k in CTX} if r["ctx_ms"] is not None else None
                    if ctx is None:
                        ctx = self.context(s, T, now, M15 + ctx_wait)
                        if ctx is None:
                            continue
                        out_c15.append([ctx[k] for k in CTX] + [sym, T])
                    if r["conf_ms"] is not None:          # починка: только если стало чем дополнить
                        j = s.j(T)
                        if j + 5 >= len(s.c) or not np.isfinite(s.c[j + 3:j + 6]).all():
                            continue
                        oi_now = bool(np.isfinite(s.at(s.oi, j - 1)) and np.isfinite(s.at(s.oi, j + 5)))
                        if not ((r["old_entry"] is None) or (r["old_oi30"] is None and oi_now)):
                            continue
                    cf = self.confirmation(s, T, r, ctx, now, conf_wait)
                    if cf:
                        out_conf.append(([cf[k] for k in CONF] + [sym, T], r["src"], ctx, r["conf_ms"] is not None))
            return out_c15, out_c5, out_conf, self.repeat_rows(conn)

        c15, c5, conf, rep = await self.db.run_in_thread(job)
        self.db.put("UPDATE sparks_15m SET rep_gap_med=?, rep_vs_med=?, rep_conf_med=?, rep_gap_mean=?, "
                    "rep_vs_mean=?, rep_conf_mean=?, rep_ms=? WHERE symbol=? AND t=?", rep)
        self.db.put(SQL_CTX15, c15)
        self.db.put(SQL_CTX5, c5)
        self.db.put(SQL_CONF, [x[0] for x in conf])
        if c15 or c5 or conf:
            log.info("контекст: 15m — %d, 5m — %d; подхват посчитан — %d", len(c15), len(c5), len(conf))
        fixed = [x[0] for x in conf if x[3]]
        if fixed:
            log.info("подхват пересчитан у %d искр (не было бара T+30 или OI): %s", len(fixed),
                     ", ".join(f"{r[-2]} {fmt(r[-1])}" for r in fixed[:10]))
        for row, src, ctx, rep in conf:
            v = dict(zip(CONF + ["symbol", "t"], row))
            if src == "live" and (v["conf"] or v["score"] >= 3):
                rules = [n for n, k in (("A", "rA"), ("B", "rB"), ("C", "rC")) if v[k]]
                log.info("%s %s: %s, балл %d/5, L/S %s%% недели, OI %s, правила: %s",
                         v["symbol"], fmt(v["t"]), v["scenario"], v["score"],
                         f"{ctx['ls_pct']:.0f}" if ctx.get("ls_pct") is not None else "?",
                         f"{v['oi30']:+.1f}%" if v["oi30"] is not None else "?",
                         ", ".join(rules) or "—")
