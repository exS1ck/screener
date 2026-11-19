"""Этап 9: сводки из журнала для вкладки «Статистика».

Всё считается по SQLite на лету. Ожидания — из ТЗ и README эталона (август–сентябрь 2026).
База искр: mean — поток исследования (среднее 20), med — поток v0.1 (медиана). Оба — только
первые за 12 ч, рост +2…+8%, оборот $0.5–30M.
"""
import sqlite3
import statistics
import time

DAY = 86_400_000
H4 = 4 * 3_600_000

# Ожидания исследования (README эталона и ТЗ). per_day — в сутки.
EXPECT_SCORE = {0: (20, 1.3), 1: (27, 3.4), 2: (17, 5.8), 3: (6.8, 10.6), 4: (1.5, 31.5), 5: (0.2, None)}
EXPECT_FLAGS = {0: (1.03, 21), 1: (0.22, None), 2: (-0.24, None), 3: (-0.94, 64), 4: (-1.74, 73)}


def _avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _share(xs):
    xs = [x for x in xs if x is not None]
    return sum(1 for x in xs if x) / len(xs) * 100 if xs else None


def _streak(xs):
    best = cur = 0
    for x in xs:
        cur = cur + 1 if x < 0 else 0
        best = max(best, cur)
    return best


def v04_rows(conn, since: int, now: int, row) -> list:
    """v0.4: доля времени в режимах, правило A (модель A-c на 1m) по режимам, эпизоды пампа, шорт-счета."""
    out = []
    try:
        reg = conn.execute("SELECT regime, COUNT(*) FROM market_regime WHERE t>=? AND regime!='' GROUP BY regime",
                           (since,)).fetchall()
    except sqlite3.OperationalError:
        return out
    tot = sum(n for _, n in reg)
    for name, exp in (("рост", 16), ("нейтрально", 25), ("падение", 59)):
        n = dict(reg).get(name, 0)
        out.append(row(f"Режим рынка: доля времени «{name}»", exp, n / tot * 100 if tot else None, tot, "%",
                       "эталон v0.4 (авг–сен)"))
    for name, exp, w in (("рост", 5.41, 66), ("нейтрально", 1.27, 45), ("падение", 2.02, 33)):
        v = [r[0] for r in conn.execute(
            "SELECT o.x_c_pct FROM sparks_15m s JOIN outcomes o ON o.kind='15m' AND o.symbol=s.symbol AND o.t=s.t "
            "WHERE s.t>=? AND s.rA_mean=1 AND s.regime=? AND o.x_c_pct IS NOT NULL", (since, name))]
        out.append(row(f"Правило A, выход A-c (1m), режим «{name}»: средняя", exp, sum(v) / len(v) if v else None,
                       len(v), "%", f"ждём винрейт {w}%" + (f"; сейчас {sum(1 for x in v if x > 0) / len(v) * 100:.0f}%" if v else "")))
    eps = conn.execute("SELECT MIN(start_t), MAX(start_t), COUNT(*) FROM pump_episodes WHERE start_t>=?", (since,)).fetchone()
    span = (eps[1] - eps[0]) / 86_400_000 if eps and eps[2] > 1 else None
    out.append(row("Эпизодов пампа в сутки (шорт-счета)", 10.6, eps[2] / span if span else None, eps[2] or 0, "",
                   "по истории и вживую"))
    for acc, exp_n, exp_avg, exp_win, exp_r in (("Ш1", 4.5, 0.72, 71, 0.068), ("Ш1-b", 1.5, 1.86, 78, 0.164),
                                                 ("Ш1-c", 2.8, 1.65, 78, 0.154), ("Ш1-d", 1.7, 2.53, 84, 0.233),
                                                 ("Ш2", 3.5, 3.07, 92, 0.048), ("Ш2-b", 1.1, 5.56, 97, 0.087),
                                                 ("Ш2-c", 1.1, 3.03, 81, 0.101), ("Ш3", 0.25, 8.90, 100, 0.139)):
        unit = " от заявки" if acc in ("Ш2", "Ш2-b", "Ш3") else ""
        for src, label in (("live", "бумага"), ("backfill", "модель по истории")):
            st = "closed" if src == "live" else "model"
            v = conn.execute("SELECT t_signal, pnl_pct, R FROM short_trades WHERE acc=? AND src=? AND status=? "
                             "AND t_signal>=?", (acc, src, st, since)).fetchall()
            sp = (max(x[0] for x in v) - min(x[0] for x in v)) / 86_400_000 if len(v) > 1 else None
            avg = sum(x[1] for x in v) / len(v) if v else None
            win = sum(1 for x in v if x[1] > 0) / len(v) * 100 if v else None
            r = sum(x[2] for x in v) / len(v) if v else None
            out.append(row(f"{acc} ({label}): R на сделку", exp_r, r, len(v), "",
                           (f"в сутки {len(v) / sp:.1f} (ждём {exp_n}); " if sp else "") +
                           (f"средняя {avg:+.2f}%{unit} (ждём {exp_avg:+.2f}%); винрейт {win:.0f}% (ждём {exp_win}%)"
                            if v else f"ждём {exp_avg:+.2f}%{unit}, винрейт {exp_win}%")))
    for acc, exp_r in (("Ш1-b", 0.069), ("Ш2-b", -0.014), ("Ш2-c", 0.072)):
        try:
            v = conn.execute("SELECT R FROM virtual_trades WHERE acc=? AND status IN ('closed','model') AND t_signal>=?",
                             (acc, since)).fetchall()
        except sqlite3.OperationalError:
            v = []
        r = sum(x[0] for x in v) / len(v) if v else None
        out.append(row(f"{acc}: поздние сигналы (виртуально), R на сделку", exp_r, r, len(v), "",
                       f"сумма {sum(x[0] for x in v):+.2f}R" if v else "по истории и вживую"))
    try:
        e7 = conn.execute("SELECT MIN(start_t), MAX(start_t), COUNT(*) FROM pump_episodes_7d WHERE start_t>=?",
                          (since,)).fetchone()
        sp7 = (e7[1] - e7[0]) / 86_400_000 if e7 and e7[2] > 1 else None
        out.append(row("Эпизодов больших пампов (7 дней, +100%) в сутки", 69 / 61, e7[2] / sp7 if sp7 else None,
                       e7[2] or 0, "", "Ш3; по истории и вживую"))
    except sqlite3.OperationalError:
        pass
    return out


def compute(conn: sqlite3.Connection, days: float, base: str) -> dict:
    conn.row_factory = sqlite3.Row
    now = int(time.time() * 1000)
    since = now - int(days * DAY) if days > 0 else 0
    suf = "_mean" if base == "mean" else ""
    # Сверка с исследованием — по его определению «первой за 12 ч» (first12r_*); у старых записей — first12_*
    first = "COALESCE(s.first12r_mean, s.first12_mean)" if base == "mean" else "COALESCE(s.first12r_med, s.first12_med)"
    first_v2 = "first12_mean" if base == "mean" else "first12_med"
    cand = "cand_mean" if base == "mean" else "cand_med"

    sp = [dict(r) for r in conn.execute(
        f"SELECT s.*, o.pump12, o.max12, o.min12, o.trade_pct, o.trade_bars, o.trade_reason, o.level AS out_level, "
        f"o.x_c_pct, o.x_c_reason, o.x_ct_pct, o.x_ct_reason, o.x_calm_pct, o.x_calm_reason "
        f"FROM sparks_15m s LEFT JOIN outcomes o ON o.kind='15m' AND o.symbol=s.symbol AND o.t=s.t "
        f"WHERE s.t>=? AND s.{cand}=1 AND {first}=1 AND s.in_range=1 AND s.liq_tested=1", (since,))]
    t_min = min((r["t"] for r in sp), default=now)
    span = max((now - t_min) / DAY, 1e-9)

    capit: dict[str, list] = {}
    for r in conn.execute("SELECT symbol, t_in, spent_ms FROM watchlist_events WHERE list='capit' AND in_list=1"):
        capit.setdefault(r[0], []).append((r[1], r[2]))

    def capit_at(r):
        return any(t_in + H4 <= r["t"] < t_in + H4 + 14 * DAY and (not spent or spent > r["t"])
                   for t_in, spent in capit.get(r["symbol"], ()))

    conf = [r for r in sp if r["conf"] == 1]
    rule = {k: [r for r in sp if r.get(f"r{k}{suf}") == 1] for k in "ABC"}

    def model(rows):
        tr = sorted([r for r in rows if r["trade_pct"] is not None], key=lambda r: r["t"])
        if not tr:
            return dict(n=0)
        pct = [r["trade_pct"] for r in tr]
        rr = []
        for r in tr:
            if r["entry"] and r["l"]:
                risk = ((1 - r["l"] / r["entry"]) + 0.002) * 100
                rr.append(r["trade_pct"] / risk if risk > 0 else None)
        wins = [x for x in pct if x > 0]
        losses = [x for x in pct if x <= 0]
        return dict(n=len(tr), winrate=len(wins) / len(tr) * 100, avg=_avg(pct), r=_avg(rr),
                    avg_win=_avg(wins), avg_loss=_avg(losses), streak=_streak(pct),
                    hold_h=statistics.median([r["trade_bars"] * 5 / 60 for r in tr if r["trade_bars"]]) if tr else None)

    mA, mB, mC = model(rule["A"]), model(rule["B"]), model(rule["C"])
    rule["D"] = [r for r in sp if r.get(f"rD{suf}") == 1]

    def xmodel(rows, key):
        """Модель выходов v0.2 на 1m (outcomes.x_*): средняя, винрейт, доли причин."""
        v = [(r[f"x_{key}_pct"], r[f"x_{key}_reason"]) for r in rows if r.get(f"x_{key}_pct") is not None]
        if not v:
            return dict(n=0)
        pc = [x for x, _ in v]
        reasons: dict[str, int] = {}
        for _, rs in v:
            reasons[rs] = reasons.get(rs, 0) + 1
        return dict(n=len(v), avg=_avg(pc), win=sum(1 for x in pc if x > 0) / len(pc) * 100,
                    reasons=", ".join(f"{k} {round(c / len(v) * 100)}%" for k, c in
                                      sorted(reasons.items(), key=lambda kv: -kv[1])))

    s5 = [r[0] for r in conn.execute(
        "SELECT o.pump12 FROM sparks_5m s JOIN outcomes o ON o.kind='5m' AND o.symbol=s.symbol AND o.t=s.t "
        "WHERE s.t>=? AND s.first12=1", (since,))]
    big = [dict(r) for r in conn.execute(
        "SELECT s.t, o.pump12 FROM sparks_5m s LEFT JOIN outcomes o ON o.kind='5m' AND o.symbol=s.symbol AND o.t=s.t "
        "WHERE s.t>=? AND s.big=1 AND s.liq_tested=1", (since,))]
    big_span = max((now - min((r["t"] for r in big), default=now)) / DAY, 1e-9)
    creep = [dict(r) for r in conn.execute(
        "SELECT t_in, in_list, outcome FROM watchlist_events WHERE list='creep' AND t_in>=?", (since,))]
    creep_span = max((now - min((r["t_in"] for r in creep), default=now)) / DAY, 1e-9)
    cap_all = [dict(r) for r in conn.execute(
        "SELECT t_in, in_list, outcome FROM watchlist_events WHERE list='capit' AND t_in>=? AND ls_exact=1", (since,))]
    cap_span = max((now - min((r["t_in"] for r in cap_all), default=now)) / DAY, 1e-9)
    cap_sig = [r for r in cap_all if r["in_list"]]

    def row(name, expect, value, n=None, unit="", note=""):
        return dict(name=name, expect=expect, value=value, n=n, unit=unit, note=note)

    pumps = lambda rows: _share([r["pump12"] for r in rows if r["out_level"]])
    n_out = lambda rows: sum(1 for r in rows if r["out_level"])
    expect = [
        row("15m-искр в сутки (первые за 12 ч)", 75.7 if base == "mean" else None, len(sp) / span, len(sp),
            note="медиана даёт примерно на четверть больше" if base == "med" else ""),
        row("…из них первые и по v0.2 (без искры за 12 ч до них)", None,
            sum(1 for r in sp if r[first_v2] == 1) / span, sum(1 for r in sp if r[first_v2] == 1),
            note="бот и правила теперь берут только такие"),
        row("…из них с подхватом, в сутки", 10.4 if base == "mean" else None, len(conf) / span, len(conf)),
        row("Искры с подхватом: стали пампом", 13.4, pumps(conf), n_out(conf), "%"),
        row("Правило A: сигналов в сутки", 2.6, len(rule["A"]) / span, len(rule["A"]),
            note="правила — с «первой за 12 ч» по v0.2: чуть строже исследования"),
        row("Правило A: стали пампом", 23.2, pumps(rule["A"]), n_out(rule["A"]), "%"),
        row("Правило A, модель: винрейт", 48, mA.get("winrate"), mA["n"], "%"),
        row("Правило A, модель: средняя", 2.73, mA.get("avg"), mA["n"], "%"),
        row("Правило A, модель: в R", 0.48, mA.get("r"), mA["n"], "R"),
        row("Правило A, модель: средняя прибыль", 10.6, mA.get("avg_win"), mA["n"], "%"),
        row("Правило A, модель: средний убыток", -4.5, mA.get("avg_loss"), mA["n"], "%"),
        row("Правило A, модель: худшая серия убытков", 8, mA.get("streak"), mA["n"], "", "в исследовании за 2 месяца"),
        row("Правило A, модель: время в сделке (медиана)", 4, mA.get("hold_h"), mA["n"], "ч"),
        row("Правило B: сигналов в сутки", 1.1, len(rule["B"]) / span, len(rule["B"]),
            note="первые дни занижено: история стакана копится с запуска"),
        row("Правило B, модель: средняя", 4.83, mB.get("avg"), mB["n"], "%"),
        row("Правило C: сигналов в сутки", 0.9, len(rule["C"]) / span, len(rule["C"])),
        row("Правило C, модель: средняя", 3.6, mC.get("avg"), mC["n"], "%"),
        *[row(f"Выход {name}, правило {rl}: средняя", exp_avg, xm.get("avg"), xm["n"], "%",
              (f"винрейт {xm['win']:.0f}% (ждём {exp_win}%); " + xm["reasons"]) if xm["n"] else f"ждём винрейт {exp_win}%")
          for name, key, rl, exp_avg, exp_win in (
              ("A-c (по закрытию 5m)", "c", "A", 2.66, 44), ("A-c (по закрытию 5m)", "c", "D", 4.91, 58),
              ("A-c-t (+ проверка 6 ч)", "ct", "A", 2.95, 53), ("A-c-t (+ проверка 6 ч)", "ct", "D", 4.80, 64),
              ("«спокойный»", "calm", "A", 1.85, 62), ("«спокойный»", "calm", "D", 3.8, 75))
          for xm in [xmodel(rule[rl], key)]],
        row("5m-искры, первые за 12 ч: стали пампом", 6.2, _share(s5), len(s5), "%"),
        row("5m «большая свеча» (> +8%): в сутки", 1.5, len(big) / big_span, len(big), "",
            "алерт без потолка, оборот $0.5–30M"),
        row("5m «большая свеча»: стали пампом", None, _share([r["pump12"] for r in big]),
            sum(1 for r in big if r["pump12"] is not None), "%", "в исследовании — ещё около 5% всех пампов"),
        *v04_rows(conn, since, now, row),
        row("«Ползут»: входов в сутки (рост ≥ 5%)", 142.9, len(creep) / creep_span, len(creep)),
        row("«Ползут»: в список в сутки", 13, sum(1 for r in creep if r["in_list"]) / creep_span,
            sum(1 for r in creep if r["in_list"])),
        row("«Ползут» в списке: +25% за 24 ч", 10, _share([r["outcome"] for r in creep if r["in_list"]]),
            sum(1 for r in creep if r["in_list"] and r["outcome"] is not None), "%"),
        row("«Капитуляция 4h»: сигналов в сутки", 12.3, len(cap_sig) / cap_span, len(cap_sig),
            note="только события с полной неделей L/S"),
        row("«Капитуляция 4h»: +25% за 14 дней", 43.9, _share([r["outcome"] for r in cap_sig]),
            sum(1 for r in cap_sig if r["outcome"] is not None), "%"),
    ]

    # ---------------------------------------------------------------- балл
    by_score = []
    for sc in range(6):
        rows = [r for r in sp if r["score"] == sc]
        e = EXPECT_SCORE[sc]
        by_score.append(dict(score=sc, per_day=len(rows) / span, n=len(rows), pump=pumps(rows), n_out=n_out(rows),
                             exp_per_day=e[0], exp_pump=e[1]))

    # ---------------------------------------------------------------- теги
    def f(v, op, x):
        return v is not None and (v < x if op == "<" else v >= x if op == ">=" else v > x)

    TAGS = [
        ("L/S < 20% недели", lambda r: f(r["ls_pct"], "<", 20)),
        ("L/S 20–80%", lambda r: r["ls_pct"] is not None and 20 <= r["ls_pct"] < 80),
        ("L/S ≥ 80%", lambda r: f(r["ls_pct"], ">=", 80)),
        ("рост 6 ч > 3", lambda r: f(r["pre6h_log"], ">", 3)),
        ("рост 6 ч > 8", lambda r: f(r["pre6h_log"], ">", 8)),
        ("стакан < $50k", lambda r: f(r["ask2_med"], "<", 50_000)),
        ("стакан > $150k", lambda r: f(r["ask2_med"], ">", 150_000)),
        ("утро 06–12 МСК", lambda r: r["hour_msk"] is not None and 6 <= r["hour_msk"] < 12),
        ("подхват", lambda r: r["scenario"] == "подхват"),
        ("затухание", lambda r: r["scenario"] == "затухание"),
        ("встретили продавцы", lambda r: r["scenario"] == "продавцы"),
        ("OI 30 мин ≥ +2%", lambda r: f(r["oi30"], ">=", 2)),
        ("OI 30 мин < 0", lambda r: f(r["oi30"], "<", 0)),
        ("OI 30 мин > +5%", lambda r: f(r["oi30"], ">", 5)),
        ("рост на закрытии шортов > 50%", lambda r: f(r["short_share"], ">", 50)),
        ("funding < −0.01%", lambda r: f(r["funding8"], "<", -0.01)),
        ("неделя > +40% (серийный)", lambda r: f(r["week_range"], ">", 40)),
        ("капитуляция 4h за 14 дней", capit_at),
        ("повторная по v0.2 (искра за 12 ч до неё)", lambda r: r.get(first_v2) == 0),
        ("повторная, окно 4–8 ч", lambda r: r.get(first_v2) == 0 and r.get("rep_gap_" + ("mean" if base == "mean" else "med")) is not None
         and 4 <= r["rep_gap_" + ("mean" if base == "mean" else "med")] < 8),
        ("RSI 15m перед искрой > 70", lambda r: f(r.get("rsi15_pre"), ">", 70)),
        ("RSI 1h за сутки < 30", lambda r: f(r.get("rsi1h_min24"), "<", 30)),
        ("крайности (RSI15 > 70 или RSI1h < 30)", lambda r: r.get("extreme") == 1),
        ("RSI 4h < 60", lambda r: f(r.get("rsi4h"), "<", 60)),
        ("RSI 4h был < 30 за 14 дней", lambda r: f(r.get("rsi4h_min14"), "<", 30)),
        ("MFI 15m перед искрой > 80", lambda r: f(r.get("mfi15_pre"), ">", 80)),
        ("тихая: NATR за 3 дня < 0.58%", lambda r: f(r.get("natr_3d"), "<", 0.58)),
        ("волатильность взорвалась (NATR × 1.56)", lambda r: f(r.get("natr_ratio"), ">", 1.56)),
        ("искра 3–7 ATR", lambda r: r.get("spark_atr") is not None and 3 <= r["spark_atr"] <= 7),
        ("верхняя тень 30–50%", lambda r: r.get("wick_up") is not None and 30 <= r["wick_up"] <= 50),
    ]
    base_pump = pumps(sp)
    by_tag = []
    for name, fn in TAGS:
        rows = [r for r in sp if fn(r)]
        rows_c = [r for r in rows if r["conf"] == 1]
        by_tag.append(dict(tag=name, n=len(rows), n_out=n_out(rows), pump=pumps(rows),
                           n_conf=len(rows_c), pump_conf=pumps(rows_c),
                           max12=_avg([r["max12"] for r in rows if r["out_level"]])))

    # ---------------------------------------------------------------- стадии и флаги
    lt = [dict(r) for r in conn.execute(
        "SELECT n_red, stage, fork_pct, fork_dir FROM live_tags WHERE mode='bar' AND fork_ms IS NOT NULL "
        "AND t_ms>=?", (since,))]
    by_flags = []
    for k in range(6):
        rows = [r for r in lt if (r["n_red"] == k if k < 5 else r["n_red"] >= 5)]
        e = EXPECT_FLAGS.get(k, (None, None))
        by_flags.append(dict(flags=k, n=len(rows), avg=_avg([r["fork_pct"] for r in rows]),
                             down_first=_share([r["fork_dir"] == -1 for r in rows]),
                             exp_avg=e[0], exp_down=e[1]))
    by_stage = []
    for st in ("ранняя", "середина", "поздняя", "выдыхание", "не классифицировано"):
        rows = [r for r in lt if r["stage"] == st]
        by_stage.append(dict(stage=st, n=len(rows), avg=_avg([r["fork_pct"] for r in rows]),
                             up_first=_share([r["fork_dir"] == 1 for r in rows]),
                             down_first=_share([r["fork_dir"] == -1 for r in rows])))

    # ---------------------------------------------------------------- счета: бумага против модели
    accounts = []
    for acc in [a + sfx for a in ("A", "B", "C", "A-c", "B-c", "D-c", "A-c-t", "U") for sfx in ("", "·сп")] + ["U-торг"]:
        tr = [dict(r) for r in conn.execute(
            "SELECT p.pnl_pct, p.r_mult, p.exit_reason, p.exit_src, p.slip_pct, p.exit_price, p.exit_level, "
            "o.trade_pct AS model FROM paper_trades p LEFT JOIN outcomes o "
            "ON o.kind='15m' AND o.symbol=p.symbol AND o.t=p.t_spark "
            "WHERE p.acc=? AND p.exit_ms IS NOT NULL AND p.t_spark>=?", (acc, since))]
        both = [r for r in tr if r["model"] is not None]
        breach = [(r["exit_price"] / r["exit_level"] - 1) * 100 for r in tr
                  if r["exit_reason"] != "время" and r["exit_level"]]
        accounts.append(dict(
            acc=acc, n=len(tr), avg=_avg([r["pnl_pct"] for r in tr]), r=_avg([r["r_mult"] for r in tr]),
            winrate=_share([r["pnl_pct"] > 0 for r in tr]), slip=_avg([r["slip_pct"] * 100 for r in tr if r["slip_pct"] is not None]),
            breach=_avg(breach), by_bar=sum(1 for r in tr if r["exit_src"] == "bar"),
            n_both=len(both), paper_both=_avg([r["pnl_pct"] for r in both]), model_both=_avg([r["model"] for r in both]),
            skipped=conn.execute("SELECT COUNT(*) FROM paper_skipped WHERE acc=? AND t_spark>=?", (acc, since)).fetchone()[0],
        ))

    return dict(expect=expect, by_score=by_score, by_tag=by_tag, base_pump=base_pump, n_sparks=len(sp),
                by_flags=by_flags, by_stage=by_stage, accounts=accounts, span_days=span, base=base, server_ms=now)
