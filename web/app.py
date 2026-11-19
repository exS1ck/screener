"""Веб (этап 4): только читает SQLite, кроме таблицы focus (кнопка «в фокус»).
Изменения уходят в браузер через Server-Sent Events: /api/stream."""
import json
import sqlite3
import time
from contextlib import contextmanager

from flask import Flask, Response, jsonify, render_template, request, send_file

from engine import config
from web import stats as stats_mod
from web.export import Exporter

H = 3600_000


def now_ms() -> int:
    return int(time.time() * 1000)


def create_app() -> Flask:
    cfg = config.load()
    db_path = config.path_of(cfg, "db")
    ui = cfg.get("ui", {})
    exporter = Exporter(db_path, cfg["_root"] / "exports", cfg)
    _c = sqlite3.connect(db_path, timeout=10)
    _c.execute("CREATE TABLE IF NOT EXISTS manual_exits(acc TEXT NOT NULL, symbol TEXT NOT NULL, t_spark INTEGER NOT NULL, "
               "ms INTEGER, price REAL, PRIMARY KEY(acc, symbol, t_spark))")
    _c.execute("CREATE TABLE IF NOT EXISTS controls(key TEXT PRIMARY KEY, value TEXT, ms INTEGER)")
    _c.execute("CREATE TABLE IF NOT EXISTS ex_commands(id INTEGER PRIMARY KEY, ms INTEGER, cmd TEXT, symbol TEXT, "
               "status TEXT, started_ms INTEGER, finished_ms INTEGER)")
    _c.commit()
    _c.close()
    app = Flask(__name__)
    # Шаблон перечитывается при изменении файла — после замены index.html хватает обновить страницу.
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.jinja_env.auto_reload = True

    def conn() -> sqlite3.Connection:
        c = sqlite3.connect(db_path, timeout=10)
        c.execute("PRAGMA busy_timeout=10000")
        c.row_factory = sqlite3.Row
        return c

    @contextmanager
    def db():
        c = conn()
        try:
            yield c
            c.commit()
        finally:
            c.close()

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/config")
    def api_config():
        return jsonify(ui=ui, thresholds=cfg.get("thresholds", {}), version=cfg["meta"]["config_version"])

    @app.get("/api/signals")
    def api_signals():
        hours = min(float(request.args.get("hours", 24)), 24 * 14)
        since = now_ms() - int(hours * H)
        with db() as c:
            rows = c.execute(
                "SELECT s.*, tl.price AS price_now, tl.t AS price_t, "
                "oc.max12 AS out_max12, oc.min12 AS out_min12, oc.pump12 AS out_pump12, oc.max48 AS out_max48, "
                "oc.hit_dir AS out_hit, oc.trade_pct AS out_trade, oc.trade_reason AS out_trade_reason, "
                "oc.level AS out_level, "
                "ll.t_ms AS lv_t, ll.mode AS lv_mode, ll.gain AS lv_gain, ll.max_gain AS lv_max_gain, "
                "ll.ls_from_min AS lv_ls, ll.oi_from_max AS lv_oi, ll.vol_vs_peak AS lv_vol, ll.r15 AS lv_r15, "
                "ll.flags AS lv_flags, ll.n_red AS lv_n_red, ll.stage AS lv_stage, ll.exit_oi AS lv_exit_oi, "
                "ll.exit_ls AS lv_exit_ls, ll.ls_age_min AS lv_ls_age, ll.tr_level AS lv_tr_level, ll.tr_p AS lv_tr_p, "
                "ll.tr_reason AS lv_tr_reason, ll.tr_exit_ms AS lv_tr_exit_ms, ll.tr_exit_px AS lv_tr_exit_px, "
                "ll.tr_six AS lv_tr_six "
                "FROM sparks_15m s LEFT JOIN ticker_latest tl ON tl.symbol = s.symbol "
                "LEFT JOIN outcomes oc ON oc.kind = '15m' AND oc.symbol = s.symbol AND oc.t = s.t "
                "LEFT JOIN live_latest ll ON ll.symbol = s.symbol AND ll.t_spark = s.t "
                "WHERE s.t >= ? ORDER BY s.t DESC", (since,)).fetchall()
            focus = [r[0] for r in c.execute("SELECT DISTINCT symbol FROM focus WHERE off_ms IS NULL")]
            capit = [dict(r) for r in c.execute(
                "SELECT symbol, t_in, spent_ms FROM watchlist_events WHERE list='capit' AND in_list=1 AND t_in>=?",
                (since - 15 * 24 * H,))]
            bot = [dict(r) for r in c.execute(
                "SELECT acc, symbol, t_spark, entry_price, stop_pct, exit_ms, exit_reason, pnl_pct, r_mult "
                "FROM paper_trades WHERE t_spark>=? OR exit_ms IS NULL", (since,))]
            skipped = [dict(r) for r in c.execute(
                "SELECT acc, symbol, t_spark, reason FROM paper_skipped WHERE t_spark>=?", (since,))]
        return jsonify(rows=[dict(r) for r in rows], focus=focus, capit=capit, bot=bot, skipped=skipped,
                       server_ms=now_ms())

    ACC_ALL = ([a + sfx for a in ("A", "B", "C", "A-c", "B-c", "D-c", "A-c-t", "U") for sfx in ("", "·сп")]
               + ["Ш1", "Ш1-b", "Ш1-c", "Ш1-d", "Ш2", "Ш2-b", "Ш2-c", "Ш3"])

    def epochs(c) -> dict:
        try:
            return {r[0].split(":", 1)[1]: int(r[1]) for r in c.execute(
                "SELECT key, value FROM controls WHERE key LIKE 'epoch:%'")}
        except sqlite3.OperationalError:
            return {}

    @app.post("/api/account/reset")
    def api_account_reset():
        """«Начать счёт заново»: с этого момента баланс счёта снова $100, лимиты и итоги — по новым сделкам.
        Старые сделки не удаляются: они остаются в журнале и показываются как «прошлый период»."""
        acc = str(request.get_json(force=True).get("acc", ""))
        if acc not in ACC_ALL:
            return jsonify(ok=False, error="неизвестный счёт"), 400
        with db() as c:
            c.execute("INSERT OR REPLACE INTO controls(key, value, ms) VALUES(?, ?, ?)",
                      (f"epoch:{acc}", str(now_ms()), now_ms()))
        return jsonify(ok=True)

    @app.get("/api/bot")
    def api_bot():
        days = min(float(request.args.get("days", 30)), 120)
        since = now_ms() - int(days * 24 * H)
        paper = cfg.get("paper", {})
        with db() as c:
            trades = [dict(r) for r in c.execute(
                "SELECT p.*, oc.trade_pct AS model_pct, oc.trade_reason AS model_reason, oc.x_c_pct AS model_c_pct, "
                "oc.x_c_reason AS model_c_reason, "
                "me.ms AS man_ms, me.price AS man_price FROM paper_trades p "
                "LEFT JOIN outcomes oc ON oc.kind='15m' AND oc.symbol=p.symbol AND oc.t=p.t_spark "
                "LEFT JOIN manual_exits me ON me.acc=p.acc AND me.symbol=p.symbol AND me.t_spark=p.t_spark "
                "WHERE p.t_spark>=? OR p.exit_ms IS NULL ORDER BY COALESCE(p.exit_ms, p.entry_ms) DESC", (since,))]
            # «Ручной» подсчёт: сделка с нажатой кнопкой — по ручной цене, без кнопки — как у бота.
            fee = float(paper.get("fee_pct", 0.05)) / 100
            for t in trades:
                t["man_pnl_usd"] = t["man_pnl_pct"] = None
                if t.get("man_price"):
                    q, e = t["qty"], t["entry_price"]
                    frac = t.get("frac") or 1.0
                    tp_done = t.get("tp1_ms") and t["tp1_ms"] <= t["man_ms"]
                    q_rem = q * (frac if tp_done else 1.0)
                    q_tp = q - q_rem
                    tp_px = t.get("tp1_px") or e
                    pnl = q_tp * (tp_px - e) + q_rem * (t["man_price"] - e) \
                        - fee * (q * e + q_tp * tp_px + q_rem * t["man_price"])
                    t["man_pnl_usd"], t["man_pnl_pct"] = pnl, pnl / t["notional"] * 100
            manual: dict[str, dict] = {}
            ep0 = epochs(c)
            for t in trades:
                if (t["entry_ms"] or 0) < ep0.get(t["acc"], 0):
                    continue                                   # прошлый период счёта
                if t.get("man_price"):
                    v = t["man_pnl_usd"]
                elif t["exit_ms"]:
                    v = t["pnl_usd"]
                else:
                    continue
                m = manual.setdefault(t["acc"], dict(n=0, pnl=0.0, wins=0, pct=0.0, n_man=0))
                m["n"] += 1
                m["pnl"] += v
                m["wins"] += int(v > 0)
                m["pct"] += v / t["notional"] * 100
                m["n_man"] += int(bool(t.get("man_price")))
            for m in manual.values():
                m["winrate"], m["avg_pct"] = m["wins"] / m["n"], m["pct"] / m["n"]
            skipped = [dict(r) for r in c.execute(
                "SELECT * FROM paper_skipped WHERE t_spark>=? ORDER BY t_spark DESC", (since,))]
            ep = epochs(c)
            sums, prev = {}, {}
            for r in c.execute("SELECT acc, entry_ms, pnl_usd, pnl_pct, r_mult, exit_src, exit_reason, exit_price, "
                               "exit_level FROM paper_trades WHERE exit_ms IS NOT NULL"):
                tgt = sums if (r["entry_ms"] or 0) >= ep.get(r["acc"], 0) else prev
                a = tgt.setdefault(r["acc"], dict(acc=r["acc"], n=0, pnl=0.0, _pct=0.0, _r=0.0, _w=0, by_bar=0, _b=0.0, _bn=0))
                a["n"] += 1
                a["pnl"] += r["pnl_usd"] or 0
                a["_pct"] += r["pnl_pct"] or 0
                a["_r"] += r["r_mult"] or 0
                a["_w"] += int((r["pnl_usd"] or 0) > 0)
                a["by_bar"] += int(r["exit_src"] == "bar")
                if r["exit_reason"] != "время" and r["exit_level"]:
                    a["_b"] += (r["exit_price"] / r["exit_level"] - 1) * 100
                    a["_bn"] += 1
            for d_ in (sums, prev):
                for a in d_.values():
                    a.update(avg_pct=a["_pct"] / a["n"], avg_r=a["_r"] / a["n"], winrate=a["_w"] / a["n"],
                             avg_breach=a["_b"] / a["_bn"] if a["_bn"] else None)
            for t in trades:
                t["old_period"] = int((t["entry_ms"] or 0) < ep.get(t["acc"], 0))
            manual = {k: v for k, v in manual.items()}
            n_skip = {r[0]: r[1] for r in c.execute("SELECT acc, COUNT(*) FROM paper_skipped GROUP BY acc")}
            first = c.execute("SELECT MIN(entry_ms) FROM paper_trades").fetchone()[0]
            try:
                controls = {r["key"]: dict(value=r["value"], ms=r["ms"]) for r in c.execute("SELECT * FROM controls")}
            except sqlite3.OperationalError:
                controls = {}
            skip_why = {}
            for r in c.execute("SELECT reason, COUNT(*) FROM paper_skipped WHERE acc='U-торг' GROUP BY reason"):
                skip_why[r[0]] = r[1]
        return jsonify(trades=trades, skipped=skipped, sums=sums, prev=prev, epochs=ep, n_skip=n_skip, first_ms=first,
                       manual=manual,
                       controls=controls, u_skip=skip_why, trade_cfg=cfg.get("trade", {}),
                       deposit=paper.get("deposit", 100), stream=paper.get("stream", "med"), server_ms=now_ms())

    @app.get("/api/exchange")
    def api_exchange():
        """Снимок счёта Binance (последняя сверка движка), журнал запросов и событий."""
        with db() as c:
            try:
                r = c.execute("SELECT value, ms FROM ex_state WHERE key='snapshot'").fetchone()
                snap = json.loads(r["value"]) if r else {"enabled": None}
                logs = [dict(x) for x in c.execute("SELECT * FROM ex_log ORDER BY id DESC LIMIT 40")]
                events = [dict(x) for x in c.execute("SELECT * FROM ex_events ORDER BY id DESC LIMIT 40")]
                cmds = [dict(x) for x in c.execute("SELECT * FROM ex_commands ORDER BY id DESC LIMIT 5")]
                for cm in cmds:
                    cm["steps"] = [dict(x) for x in c.execute("SELECT * FROM ex_steps WHERE cmd_id=? ORDER BY id",
                                                              (cm["id"],))]
                bot_pos = [dict(x) for x in c.execute("SELECT * FROM ex_positions")]
            except sqlite3.OperationalError:
                snap, logs, events, cmds, bot_pos = {"enabled": None}, [], [], [], []
        return jsonify(snapshot=snap, log=logs, events=events, commands=cmds, bot_positions=bot_pos,
                       server_ms=now_ms())

    @app.get("/api/live")
    def api_live():
        """Реальные сделки (шаблон B-c): открытые, закрытые, пропуски — с хронологией и бумажным B-c по той же искре."""
        with db() as c:
            try:
                rows = [dict(r) for r in c.execute(
                    "SELECT lt.*, p.pnl_pct AS paper_pct, p.exit_reason AS paper_reason FROM live_trades lt "
                    "LEFT JOIN paper_trades p ON p.acc='B-c' AND p.symbol=lt.symbol AND p.t_spark=lt.t_spark "
                    "ORDER BY lt.id DESC LIMIT 100")]
                for r in rows:
                    r["steps"] = [dict(x) for x in c.execute("SELECT ms, step, ok, detail FROM live_steps WHERE trade_id=? "
                                                             "ORDER BY id", (r["id"],))]
                ctl = c.execute("SELECT value, ms FROM controls WHERE key='trade_live'").fetchone()
            except sqlite3.OperationalError:
                rows, ctl = [], None
        cl = [r for r in rows if r["status"] == "closed"]
        summ = dict(n=len(cl), pnl=sum(r["pnl_usd"] or 0 for r in cl), fees=sum(r["fees_usd"] or 0 for r in cl),
                    wins=sum(1 for r in cl if (r["pnl_usd"] or 0) > 0))
        return jsonify(trades=rows, summary=summ, control=dict(ctl) if ctl else None,
                       notional=cfg.get("trade", {}).get("live_notional", 6), server_ms=now_ms())

    @app.post("/api/exchange/command")
    def api_exchange_command():
        """Команда движку: test (тестовый цикл по монете) или panic (аварийная кнопка). Одна за раз."""
        d = request.get_json(force=True)
        cmd = d.get("cmd")
        if cmd not in ("test", "panic", "close"):
            return jsonify(ok=False, error="неизвестная команда"), 400
        sym = str(d.get("symbol", "")).upper().strip()
        if cmd == "test" and not sym:
            return jsonify(ok=False, error="укажи монету"), 400
        with db() as c:
            busy = c.execute("SELECT id, cmd FROM ex_commands WHERE status IN ('new','running') AND cmd!='close'").fetchone()
            if busy and cmd == "test":
                return jsonify(ok=False, error=f"уже выполняется команда {busy['id']} ({busy['cmd']})"), 409
            c.execute("INSERT INTO ex_commands(ms, cmd, symbol, status) VALUES(?,?,?,'new')", (now_ms(), cmd, sym or None))
        return jsonify(ok=True)

    # ---------------------------------------------------------------- шорт-счета v0.4
    @app.get("/api/shorts")
    def api_shorts():
        """Шорт-счета v0.4/v0.5: сделки (живые), периоды счетов, ручной подсчёт, модель по истории, поздние сигналы,
        активные эпизоды пампа (12 ч и 7 дней)."""
        fee_m, fee_s, fee_l = 0.05, 0.10, 0.02
        accs = ("Ш1", "Ш1-b", "Ш1-c", "Ш1-d", "Ш2", "Ш2-b", "Ш2-c", "Ш3")
        with db() as c:
            ep = epochs(c)
            try:
                tr = [dict(r) for r in c.execute(
                    "SELECT st.*, me.ms AS man_ms, me.price AS man_price FROM short_trades st "
                    "LEFT JOIN manual_exits me ON me.acc=st.acc AND me.symbol=st.symbol AND me.t_spark=st.t_signal "
                    "WHERE st.src='live' AND (st.status='open' OR st.t_signal>=?) "
                    "ORDER BY COALESCE(st.exit_t, st.t_signal) DESC LIMIT 600", (now_ms() - 60 * 24 * H,))]
                model = [dict(r) for r in c.execute("SELECT acc, t_signal, reason, pnl_pct, R FROM short_trades "
                                                    "WHERE status='model'")]
                virt = [dict(r) for r in c.execute("SELECT * FROM virtual_trades ORDER BY t_signal DESC")]
                eps = [dict(r) for r in c.execute(
                    "SELECT e.*, (SELECT MAX(h) FROM raw_klines_5m k WHERE k.symbol=e.symbol AND k.t>=e.t_base) AS M, "
                    "(SELECT c FROM raw_klines_5m k WHERE k.symbol=e.symbol ORDER BY t DESC LIMIT 1) AS last "
                    "FROM pump_episodes e WHERE e.t_base>? ORDER BY e.start_t DESC", (now_ms() - 36 * H,))]
                eps7 = [dict(r) for r in c.execute(
                    "SELECT e.*, (SELECT MAX(h) FROM raw_klines_5m k WHERE k.symbol=e.symbol AND k.t>=e.t_base) AS M, "
                    "(SELECT c FROM raw_klines_5m k WHERE k.symbol=e.symbol ORDER BY t DESC LIMIT 1) AS last "
                    "FROM pump_episodes_7d e WHERE e.start_t>? ORDER BY e.start_t DESC", (now_ms() - 72 * H,))]
            except sqlite3.OperationalError:
                tr, model, virt, eps, eps7 = [], [], [], [], []
        for t in tr:
            fills = json.loads(t["fills"] or "[]")
            t["fills_list"] = fills
            t["old_period"] = int(t["t_signal"] < ep.get(t["acc"], 0))
            t["man_pnl_pct"] = t["man_pnl_usd"] = None
            if t.get("man_price"):
                px = t["man_price"]
                if t["acc"] in ("Ш1", "Ш1-b", "Ш1-c", "Ш1-d", "Ш2-c"):
                    p_ = (1 - px / t["entry"]) * 100 - fee_m - fee_s
                else:
                    fl = [f for f, ft in fills if (ft or 0) <= (t["man_ms"] or 0)] or [fills[0][0]]
                    p_ = sum((1 - px / f) * 100 for f in fl) - fee_m - fee_l * (len(fl) - 1) - fee_s * len(fl)
                t["man_pnl_pct"], t["man_pnl_usd"] = p_, (t["notional"] or 0) * p_ / 100
        dep = 100.0

        def agg(rows, key="pnl_pct"):
            n = len(rows)
            return dict(n=n, pnl=sum(r.get("pnl_usd") or 0 for r in rows), wins=sum(1 for r in rows if (r[key] or 0) > 0),
                        avg=(sum(r[key] or 0 for r in rows) / n) if n else None,
                        R=(sum(r["R"] or 0 for r in rows) / n) if n else None,
                        Rsum=sum(r["R"] or 0 for r in rows))
        sums, prev, manual, mstat, vstat = {}, {}, {}, {}, {}
        for acc in accs:
            cur = [t for t in tr if t["acc"] == acc and not t["old_period"]]
            cl = [t for t in cur if t["status"] == "closed"]
            sums[acc] = dict(agg(cl), open=sum(1 for t in cur if t["status"] == "open"),
                             skipped=sum(1 for t in cur if t["status"] == "skipped"))
            old = [t for t in tr if t["acc"] == acc and t["old_period"] and t["status"] == "closed"]
            prev[acc] = agg(old) if old else None
            mv = [(t["man_pnl_usd"] if t.get("man_price") else t["pnl_usd"]) for t in cur
                  if t["status"] == "closed" or t.get("man_price")]
            manual[acc] = dict(n=len(mv), pnl=sum(v or 0 for v in mv), n_man=sum(1 for t in cur if t.get("man_price")))
            md = [m for m in model if m["acc"] == acc]
            span = (max(m["t_signal"] for m in md) - min(m["t_signal"] for m in md)) / 86_400_000 if len(md) > 1 else None
            reasons: dict = {}
            for m in md:
                reasons[m["reason"]] = reasons.get(m["reason"], 0) + 1
            mstat[acc] = dict(agg(md), per_day=(len(md) / span) if span else None, reasons=reasons)
            vv = [v for v in virt if v["acc"] == acc and v["status"] in ("closed", "model")]
            vstat[acc] = dict(agg(vv), open=sum(1 for v in virt if v["acc"] == acc and v["status"] == "open"),
                              live=sum(1 for v in vv if v["src"] == "live"))
        return jsonify(trades=tr, sums=sums, prev=prev, epochs={a: ep.get(a) for a in accs}, manual=manual, model=mstat,
                       virtual=vstat, virtual_rows=virt[:200], episodes=eps, episodes7=eps7, deposit=dep,
                       server_ms=now_ms())

    @app.post("/api/short_manual")
    def api_short_manual():
        """«Закрыть руками» для шорт-счёта: отдельный ручной подсчёт, сделка счёта идёт дальше по правилам."""
        d = request.get_json(force=True)
        key = (str(d.get("acc", "")), str(d.get("symbol", "")), int(d.get("t_signal", 0)))
        if key[0] not in ("Ш1", "Ш1-b", "Ш1-c", "Ш1-d", "Ш2", "Ш2-b", "Ш2-c", "Ш3"):
            return jsonify(ok=False, error="неизвестный счёт"), 400
        with db() as c:
            tr = c.execute("SELECT status FROM short_trades WHERE acc=? AND symbol=? AND t_signal=? AND src='live'",
                           key).fetchone()
            if not tr or tr["status"] != "open":
                return jsonify(ok=False, error="сделка не открыта"), 400
            if d.get("cancel"):
                c.execute("DELETE FROM manual_exits WHERE acc=? AND symbol=? AND t_spark=?", key)
                return jsonify(ok=True)
            px = c.execute("SELECT price, t FROM ticker_latest WHERE symbol=?", (key[1],)).fetchone()
            if not px or not px["price"] or now_ms() - (px["t"] or 0) > 120_000:
                return jsonify(ok=False, error="нет свежей цены — движок работает?"), 400
            c.execute("INSERT OR REPLACE INTO manual_exits(acc, symbol, t_spark, ms, price) VALUES(?,?,?,?,?)",
                      key + (now_ms(), px["price"]))
        return jsonify(ok=True, price=px["price"])

    @app.post("/api/trade/toggle")
    def api_trade_toggle():
        """Кнопка на вкладке «Торговля»: включить или выключить автоторговлю на реальном счёте (шаблон B-c).
        Открытые позиции движок ведёт до выхода в любом случае."""
        on = bool(request.get_json(force=True).get("on"))
        with db() as c:
            c.execute("INSERT OR REPLACE INTO controls(key, value, ms) VALUES('trade_live', ?, ?)",
                      ("on" if on else "off", now_ms()))
        return jsonify(ok=True, on=on)

    @app.post("/api/manual")
    def api_manual():
        """Кнопка «закрыть руками»: записать текущую цену как ручной выход (или отменить). Сделка бота не трогается."""
        d = request.get_json(force=True)
        key = (str(d.get("acc", "")), str(d.get("symbol", "")), int(d.get("t_spark", 0)))
        with db() as c:
            tr = c.execute("SELECT exit_ms FROM paper_trades WHERE acc=? AND symbol=? AND t_spark=?", key).fetchone()
            if not tr:
                return jsonify(ok=False, error="нет такой сделки"), 404
            if tr["exit_ms"]:
                return jsonify(ok=False, error="сделка уже закрыта ботом"), 400
            if d.get("cancel"):
                c.execute("DELETE FROM manual_exits WHERE acc=? AND symbol=? AND t_spark=?", key)
                return jsonify(ok=True)
            px = c.execute("SELECT price, t FROM ticker_latest WHERE symbol=?", (key[1],)).fetchone()
            if not px or not px["price"] or now_ms() - (px["t"] or 0) > 120_000:
                return jsonify(ok=False, error="нет свежей цены — движок работает?"), 400
            c.execute("INSERT OR REPLACE INTO manual_exits(acc, symbol, t_spark, ms, price) VALUES(?,?,?,?,?)",
                      key + (now_ms(), px["price"]))
        return jsonify(ok=True, price=px["price"])

    @app.get("/api/watchlist")
    def api_watchlist():
        t = now_ms()
        with db() as c:
            creep = c.execute(
                "SELECT wn.*, we.pre6 AS pre6_in, we.ls_pct AS ls_in, we.oi6 AS oi6_in, we.q24 AS q24_in, "
                "we.in_list, we.ls_exact, we.close_in FROM watch_now wn JOIN watchlist_events we "
                "ON we.list = wn.list AND we.symbol = wn.symbol AND we.t_in = wn.t_in WHERE wn.list = 'creep'").fetchall()
            capit = c.execute(
                "SELECT we.*, wn.price AS price_now, wn.from_min FROM watchlist_events we "
                "LEFT JOIN watch_now wn ON wn.list = we.list AND wn.symbol = we.symbol AND wn.t_in = we.t_in "
                "WHERE we.list = 'capit' AND we.t_in >= ? ORDER BY we.t_in DESC", (t - 15 * 24 * H,)).fetchall()
            try:
                rsi_rows = [dict(r) for r in c.execute("SELECT * FROM rsi_now")]
            except sqlite3.OperationalError:
                rsi_rows = []
        return jsonify(creep=[dict(r) for r in creep], capit=[dict(r) for r in capit], rsi=rsi_rows, server_ms=t)

    @app.get("/api/sparks5")
    def api_sparks5():
        hours = min(float(request.args.get("hours", 24)), 24 * 14)
        with db() as c:
            rows = c.execute(
                "SELECT s.*, tl.price AS price_now, oc.max12 AS out_max12, oc.min12 AS out_min12, "
                "oc.pump12 AS out_pump12, oc.level AS out_level "
                "FROM sparks_5m s LEFT JOIN ticker_latest tl ON tl.symbol = s.symbol "
                "LEFT JOIN outcomes oc ON oc.kind = '5m' AND oc.symbol = s.symbol AND oc.t = s.t "
                "WHERE s.t >= ? ORDER BY s.t DESC", (now_ms() - int(hours * H),)).fetchall()
        return jsonify(rows=[dict(r) for r in rows])

    @app.get("/api/stats")
    def api_stats():
        days = float(request.args.get("days", 0))
        base = request.args.get("base", "mean")
        with db() as c:
            return jsonify(stats_mod.compute(c, days, "med" if base == "med" else "mean"))

    # ---------------------------------------------------------------- выгрузка
    @app.get("/api/export")
    def api_export_state():
        with db() as c:
            lo, hi = c.execute("SELECT MIN(t), MAX(t) FROM sparks_15m").fetchone()
        return jsonify(job=exporter.status(), files=exporter.files(), first_ms=lo, last_ms=hi, server_ms=now_ms())

    @app.post("/api/export")
    def api_export_start():
        from datetime import datetime, timedelta, timezone
        msk = timezone(timedelta(hours=3))
        d = request.get_json(force=True)
        now = now_ms()
        mode = d.get("mode", "all")
        if mode == "days":
            since, until = now - int(float(d.get("days", 7)) * 24 * H), now
        elif mode == "range":
            f = datetime.strptime(d["from"], "%Y-%m-%d").replace(tzinfo=msk)
            t = datetime.strptime(d["to"], "%Y-%m-%d").replace(tzinfo=msk) + timedelta(days=1)
            since, until = int(f.timestamp() * 1000), int(t.timestamp() * 1000)
        else:
            since, until = 0, now
            with db() as c:
                lo = c.execute("SELECT MIN(t) FROM sparks_15m").fetchone()[0]
            since = lo or now - 24 * H
        if until <= since:
            return jsonify(ok=False, error="пустой период"), 400
        raw = "all" if d.get("raw") == "all" else "around"
        return jsonify(exporter.start(since, until, raw, float(d.get("before_h", 24)), float(d.get("after_h", 48))))

    @app.get("/api/export/file/<name>")
    def api_export_file(name):
        p = exporter.path_of(name)
        if not p:
            return "нет такого файла", 404
        return send_file(p, as_attachment=True, download_name=p.name)

    @app.post("/api/export/delete")
    def api_export_delete():
        p = exporter.path_of(str(request.get_json(force=True).get("name", "")))
        if p:
            p.unlink()
        return jsonify(ok=bool(p))

    @app.get("/api/status")
    def api_status():
        return jsonify(status_dict())

    def status_dict() -> dict:
        with db() as c:
            st = {r["key"]: dict(value=r["value"], t=r["t"]) for r in c.execute("SELECT * FROM engine_status")}
            try:
                r = c.execute("SELECT t, mkt24, mkt72, breadth24, btc24, regime FROM market_regime "
                              "ORDER BY t DESC LIMIT 1").fetchone()
                st["_regime"] = dict(r) if r else None
            except sqlite3.OperationalError:
                st["_regime"] = None
        st["_server_ms"] = now_ms()
        return st

    @app.post("/api/focus")
    def api_focus():
        d = request.get_json(force=True)
        sym, on = str(d.get("symbol", "")).upper(), bool(d.get("on"))
        if not sym:
            return jsonify(ok=False), 400
        t = now_ms()
        with db() as c:
            active = c.execute("SELECT 1 FROM focus WHERE symbol=? AND off_ms IS NULL", (sym,)).fetchone()
            if on and not active:
                c.execute("INSERT INTO focus(symbol, on_ms, off_ms) VALUES(?,?,NULL)", (sym, t))
            elif not on and active:
                c.execute("UPDATE focus SET off_ms=? WHERE symbol=? AND off_ms IS NULL", (t, sym))
        return jsonify(ok=True)

    @app.get("/api/stream")
    def api_stream():
        poll = float(ui.get("sse_poll_sec", 2))
        price_every = float(ui.get("sse_prices_sec", 3))
        status_every = float(ui.get("sse_status_sec", 10))

        def gen():
            last_sig, last_price, last_status = None, 0.0, 0.0
            c = conn()
            try:
                yield "retry: 3000\n\n"
                while True:
                    tnow = time.time()
                    since = now_ms() - 48 * H
                    sig = tuple(c.execute(
                        "SELECT COUNT(*), MAX(detected_ms), MAX(ctx_ms), MAX(conf_ms) FROM sparks_15m WHERE t >= ?",
                        (since,)).fetchone())
                    sig += tuple(c.execute("SELECT COUNT(*), MAX(on_ms), MAX(off_ms) FROM focus").fetchone())
                    sig += tuple(c.execute("SELECT MAX(computed_ms) FROM outcomes WHERE t >= ?", (since,)).fetchone())
                    sig += tuple(c.execute("SELECT MAX(t_ms) FROM live_latest WHERE t_spark >= ?", (since,)).fetchone())
                    sig += tuple(c.execute("SELECT COUNT(*), MAX(t) FROM watch_now").fetchone())
                    sig += tuple(c.execute("SELECT COUNT(*), MAX(upd_ms) FROM paper_trades").fetchone())
                    sig += tuple(c.execute("SELECT COUNT(*) FROM paper_skipped").fetchone())
                    sig += tuple(c.execute("SELECT COUNT(*), MAX(ms) FROM manual_exits").fetchone())
                    sig += tuple(c.execute("SELECT COUNT(*), MAX(ms) FROM controls").fetchone())
                    try:
                        sig += tuple(c.execute("SELECT COUNT(*), MAX(last_t), MAX(exit_t) FROM short_trades").fetchone())
                        sig += tuple(c.execute("SELECT COUNT(*), MAX(last_t) FROM virtual_trades").fetchone())
                        sig += tuple(c.execute("SELECT MAX(t) FROM market_regime").fetchone())
                    except sqlite3.OperationalError:
                        pass
                    try:
                        sig += tuple(c.execute("SELECT MAX(ms) FROM ex_state").fetchone())
                        sig += tuple(c.execute("SELECT MAX(id), MAX(ms) FROM ex_steps").fetchone())
                        sig += tuple(c.execute("SELECT MAX(id), MAX(finished_ms) FROM ex_commands").fetchone())
                        sig += tuple(c.execute("SELECT MAX(id) FROM live_steps").fetchone())
                    except sqlite3.OperationalError:
                        pass
                    try:
                        sig += tuple(c.execute("SELECT COUNT(*), MAX(t), MAX(upd_ms) FROM rsi_now").fetchone())
                    except sqlite3.OperationalError:
                        pass
                    sig += tuple(c.execute("SELECT MAX(detected_ms), MAX(outcome_ms), MAX(spent_ms) "
                                           "FROM watchlist_events").fetchone())
                    if sig != last_sig:
                        last_sig = sig
                        yield "event: signals\ndata: {}\n\n"
                    if tnow - last_price >= price_every:
                        last_price = tnow
                        # цены: монеты с искрами за период, открытые бумажные лонги, а также открытые шорты,
                        # реальные сделки и живые эпизоды пампа (у шорт-сигнала может не быть 15m-искры)
                        extra = ""
                        for tbl, cond in (("short_trades", "status='open'"), ("virtual_trades", "status='open'"),
                                          ("live_trades", "status IN ('open','opening')"),
                                          ("pump_episodes", f"t_base > {now_ms() - 36 * H}"),
                                          ("pump_episodes_7d", f"start_t > {now_ms() - 72 * H}")):
                            try:
                                c.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                                extra += f" UNION SELECT symbol FROM {tbl} WHERE {cond}"
                            except sqlite3.OperationalError:
                                pass
                        px = {r[0]: r[1] for r in c.execute(
                            "SELECT tl.symbol, tl.price FROM ticker_latest tl "
                            "WHERE tl.symbol IN (SELECT DISTINCT symbol FROM sparks_15m WHERE t >= ? "
                            "UNION SELECT symbol FROM paper_trades WHERE exit_ms IS NULL" + extra + ")", (since,))}
                        yield f"event: prices\ndata: {json.dumps(px)}\n\n"
                    if tnow - last_status >= status_every:
                        last_status = tnow
                        st = status_dict()
                        yield f"event: status\ndata: {json.dumps(st)}\n\n"
                    time.sleep(poll)
            finally:
                c.close()

        return Response(gen(), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app
