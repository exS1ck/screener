"""Выгрузка для анализа: журнал (искры с контекстом, индикаторами, исходами, сделками бота), списки,
живые теги и сырые данные — в один zip с CSV и словарём колонок README.md.

Сырьё: around — окна вокруг 15m-искр (по умолчанию −24 ч … +48 ч) только по монетам с искрами;
all — всё сырьё за период по всем монетам (большой файл).
База открывается только на чтение; движок можно не останавливать.
"""
import csv
import io
import shutil
import sqlite3
import threading
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

MSK = timezone(timedelta(hours=3))
H = 3_600_000
DAY = 24 * H
H4 = 4 * H

RAW_TABLES = [
    # (таблица, колонка времени, описание)
    ("raw_klines_5m", "t", "5m-свечи"),
    ("raw_klines_1m", "t", "1m-свечи на пути сделок"),
    ("raw_klines_1h", "t", "1h-свечи (RSI-список)"),
    ("raw_oi", "t", "OI на сетке 5m"),
    ("raw_ls", "t", "L/S"),
    ("raw_funding", "t", "funding и mark"),
    ("raw_depth_hourly", "t", "снимки стакана (итоги)"),
    ("raw_oi_fast", "t_ms", "частый OI (фокус и наблюдение)"),
    ("raw_liquidations", "t", "ликвидации"),
]


def _utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if ms else ""


def _msk(ms):
    return datetime.fromtimestamp(ms / 1000, MSK).strftime("%Y-%m-%d %H:%M") if ms else ""


class Exporter:
    def __init__(self, db_path: Path, out_dir: Path, cfg: dict):
        self.db_path = db_path
        self.out = out_dir
        self.cfg = cfg
        self.lock = threading.Lock()
        self.job: dict = dict(state="idle")

    # ---------------------------------------------------------------- управление
    def status(self) -> dict:
        with self.lock:
            return dict(self.job)

    def start(self, since: int, until: int, raw: str, before_h: float, after_h: float) -> dict:
        with self.lock:
            if self.job.get("state") == "running":
                return dict(ok=False, error="выгрузка уже собирается")
            self.job = dict(state="running", step="подготовка", started=int(time.time() * 1000), rows=0,
                            since=since, until=until, raw=raw)
        threading.Thread(target=self._run, args=(since, until, raw, before_h, after_h), daemon=True).start()
        return dict(ok=True)

    def _set(self, **kw):
        with self.lock:
            self.job.update(kw)

    def files(self) -> list[dict]:
        self.out.mkdir(parents=True, exist_ok=True)
        files = sorted(self.out.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        return [dict(name=p.name, size=p.stat().st_size, mtime=int(p.stat().st_mtime * 1000)) for p in files]

    def path_of(self, name: str) -> Path | None:
        p = (self.out / name).resolve()
        return p if p.parent == self.out.resolve() and p.suffix == ".zip" and p.exists() else None

    # ---------------------------------------------------------------- сборка
    def _run(self, since, until, raw, before_h, after_h):
        tmp = None
        try:
            self.out.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(MSK).strftime("%Y%m%d_%H%M")
            name = f"traderbot_{stamp}_{_msk(since)[:10]}_{_msk(until - 1)[:10]}_{'raw-all' if raw == 'all' else 'raw-around'}.zip"
            tmp = self.out / (name + ".part")
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
            counts: dict[str, int] = {}
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
                self._journal(conn, z, since, until, counts)
                self._raw(conn, z, since, until, raw, before_h, after_h, counts)
                self._meta(conn, z, counts)
                self._set(step="словарь колонок")
                z.writestr("README.md", self._readme(since, until, raw, before_h, after_h, counts))
            conn.close()
            final = self.out / name
            shutil.move(tmp, final)
            self._set(state="done", step="готово", file=name, size=final.stat().st_size,
                      finished=int(time.time() * 1000), counts=counts)
        except Exception as e:                                   # noqa: BLE001
            if tmp and tmp.exists():
                tmp.unlink()
            self._set(state="error", step="ошибка", error=f"{type(e).__name__}: {e}")

    def _write(self, z, name: str, header: list[str], rows, counts: dict) -> None:
        self._set(step=name)
        n = 0
        with z.open(name, "w", force_zip64=True) as f:
            tw = io.TextIOWrapper(f, encoding="utf-8", newline="")
            w = csv.writer(tw, lineterminator="\n")
            w.writerow(header)
            for r in rows:
                w.writerow(["" if x is None else (f"{x:.10g}" if isinstance(x, float) else x) for x in r])
                n += 1
                if n % 50_000 == 0:
                    with self.lock:
                        self.job["rows"] = self.job.get("rows", 0) + 50_000
            tw.flush()
            tw.detach()                                  # файл в архиве закрывает сам zipfile
        with self.lock:
            self.job["rows"] = self.job.get("rows", 0) + n % 50_000
        counts[name] = n

    # ---------------------------------------------------------------- журнал
    def _journal(self, conn, z, since, until, counts):
        conn.row_factory = sqlite3.Row
        self._set(step="журнал 15m-искр")
        sp = [dict(r) for r in conn.execute("SELECT * FROM sparks_15m WHERE t>=? AND t<? ORDER BY t, symbol",
                                            (since, until))]
        out15 = {(r["symbol"], r["t"]): dict(r) for r in conn.execute(
            "SELECT * FROM outcomes WHERE kind='15m' AND t>=? AND t<?", (since, until))}
        trades = [dict(r) for r in conn.execute("SELECT * FROM paper_trades WHERE t_spark>=? AND t_spark<?",
                                                (since, until))]
        skipped = [dict(r) for r in conn.execute("SELECT * FROM paper_skipped WHERE t_spark>=? AND t_spark<?",
                                                 (since, until))]
        accs = sorted({r["acc"] for r in trades} | {r["acc"] for r in skipped})
        tr_by = {(r["acc"], r["symbol"], r["t_spark"]): r for r in trades}
        sk_by = {(r["acc"], r["symbol"], r["t_spark"]): r for r in skipped}
        capit: dict[str, list] = {}
        for r in conn.execute("SELECT symbol, t_in, spent_ms FROM watchlist_events WHERE list='capit' AND in_list=1"):
            capit.setdefault(r[0], []).append((r[1], r[2]))
        lt: dict[tuple, list] = {}
        for r in conn.execute("SELECT symbol, t_spark, t_ms, n_red, stage FROM live_tags WHERE mode='bar' "
                              "AND t_spark>=? AND t_spark<? ORDER BY t_ms", (since, until)):
            lt.setdefault((r[0], r[1]), []).append((r[2], r[3], r[4]))

        out_cols = ["o_ref", "max12", "min12", "peak12_min", "inc12", "max24", "min24", "peak24_min", "inc24",
                    "max48", "min48", "peak48_min", "inc48", "pump12", "hit_dir", "hit_min", "trade_pct",
                    "trade_bars", "trade_reason", "level", "x_c_pct", "x_c_reason", "x_c_min", "x_ct_pct",
                    "x_ct_reason", "x_ct_min", "x_calm_pct", "x_calm_reason", "x_calm_min", "x_calm_tp1_min", "x_n1m"]
        bot_cols = ["entry_price", "slip_pct", "stop_pct", "exit_ms", "exit_reason", "exit_src", "pnl_pct", "r_mult",
                    "decision_ms"]
        base_cols = list(sp[0].keys()) if sp else [r[1] for r in conn.execute("PRAGMA table_info(sparks_15m)")]
        header = (["t_utc", "t_msk"] + base_cols + ["out_" + c for c in out_cols]
                  + ["capit_active", "lt_moments", "lt_max_red", "lt_last_stage", "lt_min_to_late"]
                  + [f"bot_{a}_{c}" for a in accs for c in bot_cols] + [f"bot_{a}_skip" for a in accs])

        def rows15():
            for r in sp:
                key = (r["symbol"], r["t"])
                o = out15.get(key, {})
                ca = int(any(t_in + H4 <= r["t"] < t_in + H4 + 14 * DAY and (not s or s > r["t"])
                             for t_in, s in capit.get(r["symbol"], ())))
                m = lt.get(key, [])
                late = next(((tm - r["t"]) // 60_000 for tm, _, st in m if st in ("поздняя", "выдыхание")), None)
                row = [_utc(r["t"]), _msk(r["t"])] + [r[c] for c in base_cols] + [o.get(c) for c in out_cols]
                row += [ca, len(m), max((x[1] for x in m), default=None), m[-1][2] if m else None, late]
                for a in accs:
                    t = tr_by.get((a, r["symbol"], r["t"]))
                    row += [t.get(c) if t else None for c in bot_cols]
                row += [sk_by[(a, r["symbol"], r["t"])]["reason"] if (a, r["symbol"], r["t"]) in sk_by else None
                        for a in accs]
                yield row

        self._write(z, "journal/sparks_15m.csv", header, rows15(), counts)

        self._set(step="журнал 5m-искр")
        s5 = [dict(r) for r in conn.execute("SELECT * FROM sparks_5m WHERE t>=? AND t<? ORDER BY t, symbol",
                                            (since, until))]
        out5 = {(r["symbol"], r["t"]): dict(r) for r in conn.execute(
            "SELECT * FROM outcomes WHERE kind='5m' AND t>=? AND t<?", (since, until))}
        cols5 = list(s5[0].keys()) if s5 else [r[1] for r in conn.execute("PRAGMA table_info(sparks_5m)")]
        oc5 = ["o_ref", "max12", "min12", "peak12_min", "max24", "min24", "max48", "min48", "pump12", "level"]
        self._write(z, "journal/sparks_5m.csv", ["t_utc", "t_msk"] + cols5 + ["out_" + c for c in oc5],
                    ([_utc(r["t"]), _msk(r["t"])] + [r[c] for c in cols5]
                     + [out5.get((r["symbol"], r["t"]), {}).get(c) for c in oc5] for r in s5), counts)
        conn.row_factory = None

        def dump(name, sql, args):
            cur = conn.execute(sql, args)
            self._write(z, name, [d[0] for d in cur.description], cur, counts)

        dump("journal/live_tags.csv", "SELECT * FROM live_tags WHERE t_spark>=? AND t_spark<? ORDER BY t_spark, symbol, t_ms",
             (since, until))
        dump("journal/watchlist_events.csv", "SELECT * FROM watchlist_events WHERE t_in>=? AND t_in<? ORDER BY t_in",
             (since, until))
        dump("journal/paper_trades.csv", "SELECT * FROM paper_trades WHERE t_spark>=? AND t_spark<? ORDER BY entry_ms",
             (since, until))
        dump("journal/paper_skipped.csv", "SELECT * FROM paper_skipped WHERE t_spark>=? AND t_spark<? ORDER BY t_spark",
             (since, until))
        # v0.4: режим рынка, эпизоды пампа, сигналы и сделки шорт-счетов
        for name, sql in (("journal/market_regime.csv", "SELECT * FROM market_regime WHERE t>=? AND t<? ORDER BY t"),
                          ("journal/pump_episodes.csv", "SELECT * FROM pump_episodes WHERE start_t>=? AND start_t<? "
                                                        "ORDER BY start_t"),
                          ("journal/short_signals.csv", "SELECT * FROM short_signals WHERE t>=? AND t<? ORDER BY t"),
                          ("journal/short_trades.csv", "SELECT * FROM short_trades WHERE t_signal>=? AND t_signal<? "
                                                       "ORDER BY t_signal"),
                          ("journal/virtual_trades.csv", "SELECT * FROM virtual_trades WHERE t_signal>=? AND t_signal<? "
                                                         "ORDER BY t_signal"),
                          ("journal/pump_episodes_7d.csv", "SELECT * FROM pump_episodes_7d WHERE start_t>=? AND start_t<? "
                                                           "ORDER BY start_t")):
            try:
                dump(name, sql, (since, until))
            except sqlite3.OperationalError:
                pass

    # ---------------------------------------------------------------- сырьё
    def _raw(self, conn, z, since, until, raw, before_h, after_h, counts):
        conn.row_factory = None
        hist = 7 * DAY                       # свечи, L/S и OI — с недельной историей (L/S-процентиль, RSI, NATR)

        def windows(before_ms: int) -> dict:
            if raw == "all":
                syms_ = [r[0] for r in conn.execute("SELECT symbol FROM symbols UNION SELECT DISTINCT symbol "
                                                    "FROM sparks_15m ORDER BY 1")]
                return {s: [(since - before_ms, until)] for s in syms_}
            w: dict[str, list] = {}
            a = int(after_h * H)
            for s, t in conn.execute("SELECT symbol, t FROM sparks_15m WHERE t>=? AND t<? ORDER BY symbol, t",
                                     (since, until)):
                iv = w.setdefault(s, [])
                lo, hi = t - before_ms, t + a
                if iv and lo <= iv[-1][1]:
                    iv[-1] = (iv[-1][0], max(iv[-1][1], hi))
                else:
                    iv.append((lo, hi))
            return w

        wins_short = windows(0 if raw == "all" else int(before_h * H))
        wins_week = windows(max(hist, int(before_h * H)) if raw != "all" else hist)
        syms = sorted(wins_short)
        wins = wins_week
        for tbl, tcol, desc in RAW_TABLES:
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")]
            ww = wins_week if tbl in ("raw_klines_5m", "raw_ls", "raw_oi") else wins_short
            if tbl == "raw_liquidations":
                def gen(ww=ww):
                    for s in syms:
                        for lo, hi in ww[s]:
                            yield from conn.execute("SELECT * FROM raw_liquidations WHERE t>=? AND t<? AND symbol=? "
                                                    "ORDER BY t", (lo, hi, s))
            else:
                def gen(tbl=tbl, tcol=tcol, ww=ww):
                    for s in syms:
                        for lo, hi in ww[s]:
                            yield from conn.execute(f"SELECT * FROM {tbl} WHERE symbol=? AND {tcol}>=? AND {tcol}<? "
                                                    f"ORDER BY {tcol}", (s, lo, hi))
            self._write(z, f"raw/{tbl}.csv", cols, gen(), counts)
        # 4h — с запасом 35 дней до периода (WVF, RSI 4h)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(raw_klines_4h)")]

        def gen4():
            for s in syms:
                lo = min(x[0] for x in wins[s]) - 35 * DAY
                hi = max(x[1] for x in wins[s])
                yield from conn.execute("SELECT * FROM raw_klines_4h WHERE symbol=? AND t>=? AND t<? ORDER BY t",
                                        (s, lo, hi))
        self._write(z, "raw/raw_klines_4h.csv", cols, gen4(), counts)

    def _meta(self, conn, z, counts):
        conn.row_factory = None
        for name, sql in (("meta/symbols.csv", "SELECT * FROM symbols ORDER BY symbol"),
                          ("meta/gaps.csv", "SELECT * FROM gaps ORDER BY start_ms"),
                          ("meta/engine_status.csv", "SELECT * FROM engine_status")):
            cur = conn.execute(sql)
            self._write(z, name, [d[0] for d in cur.description], cur, counts)
        p = self.cfg["_root"] / "config.toml"
        if p.exists():
            z.write(p, "meta/config.toml")

    # ---------------------------------------------------------------- словарь
    def _readme(self, since, until, raw, before_h, after_h, counts) -> str:
        files = "\n".join(f"| `{k}` | {v} |" for k, v in counts.items())
        raw_txt = ("всё сырьё за период по всем монетам" if raw == "all" else
                   f"окна вокруг каждой 15m-искры: −{before_h:g} ч … +{after_h:g} ч, только монеты с искрами")
        raw_txt += ("; 5m-свечи, L/S и OI — с историей 7 дней до начала окна (для L/S-процентиля, RSI, NATR); "
                    "4h-свечи — с запасом 35 дней")
        return README.format(since=_msk(since), until=_msk(until - 1), made=datetime.now(MSK).strftime("%Y-%m-%d %H:%M"),
                             ver=self.cfg["meta"]["config_version"], hash=self.cfg["_hash"], raw=raw_txt, files=files)


README = """# Выгрузка TraderBot v0.1

Период (по времени искры, МСК): {since} … {until}. Собрано: {made} МСК. Конфиг: v{ver} ({hash}), копия — `meta/config.toml`.
Сырьё: {raw}.

| Файл | Строк |
| --- | --- |
{files}

## Время и соглашения

- Все `t`, `*_ms` — миллисекунды UTC. В журналах рядом `t_utc` и `t_msk`.
- 5m-бар `t` — время **открытия**, закрывается в `t + 5 мин`. 15m-свеча искры `t = T` = 5m-бары `T, T+5, T+10`.
- `raw_oi`, `raw_ls`: значение на **конец** бара с открытием `t` (как `oi[k]`, `ls[k]` в эталоне исследования).
  «На момент T» = строка с `t = T − 5 мин`. Метка API X → `t = X − 5 мин`; `create_time` архива metrics = `t`.
- Проценты — в процентах (2.5 = +2.5%), доли тейкер-покупок `tbr3` — в долях (0.47).
- Пустая ячейка — данных не было (NULL).

## journal/sparks_15m.csv — главный файл, строка = 15m-искра

Пишется, если свеча — кандидат хотя бы по одной базе объёма.

**Свеча и отбор:** `o,h,l,c` 15m-свечи; `q` — quote volume (USDT), `tbq` — taker buy quote; `ret` — рост свечи, %;
`relvol_med` — объём / медиана 15m-объёмов за 24 ч без последнего часа (v0.1); `relvol_mean20` — / среднее 20 свечей (исследование);
`relvol_med40` — / медиана 40 свечей; `q24` — оборот за 24 ч, $M; `cand_med`, `cand_mean` — кандидат по базе
(медиана: оборот от $0.5M; среднее: $0.5–30M); `first12_med`, `first12_mean` — первая за 12 ч по v0.2: за 12 ч до неё
не было никакой искры-кандидата этого потока (1/0; пусто — вне $0.5–30M); `first12r_med`, `first12r_mean` — по
исследованию: от последней принятой искры (отброшенные паузу не продлевают); `rep_gap_*` — часов с предыдущего
кандидата того же потока, `rep_vs_*` — open искры к close предыдущей, %, `rep_conf_*` — был ли у предыдущей подхват
(повторная = gap ≤ 12 ч, окно 4–8 ч — гипотеза «затухло и продолжилось»); `in_range` — рост ≤ +8%; `liq_tested` — оборот $0.5–30M; `incomplete` — 5m-баров нет в окне 24 ч;
`src` — live (найдена вживую) / backfill (досчёт по истории); `detected_ms`; `cfg_ver`, `cfg_hash`.

**Контекст на T:** `ls_at` — L/S всех счетов на T; `ls_pct` — его процентиль за 7 дней (доля строго меньших), %;
`pre6h_log` — ln(close T−5 мин / open T−6 ч) × 100; `hour_msk`; `ask2_med` / `ask2_best_med` — медиана офферов до +2% от mid /
от лучшего аска по снимкам [T−24 ч, T−6 ч], $; `ask2_n` — снимков; `week_range` — max/min за 7 дней − 1, % (размах, без направления);
`funding8` — последний funding до T на 8 ч, %; `ctx_ms`.

**Индикаторы (Уайлдер, как TradingView, на барах, закрытых до T):** `rsi15_pre` — RSI(14) 15m на свече перед искрой;
`rsi15_conf` — на свече T+15…T+30; `rsi1h`, `rsi1h_min24` — RSI 1h последнего закрытого часа и минимум за 24 ч;
`rsi4h`, `rsi4h_min7/max7/min14/max14` — RSI 4h и экстремумы за 7/14 дней; `mfi15_pre`, `mfi1h` — MFI(14), поток = типичная цена ×
объём в USDT; `extreme` — RSI15 > 70 или минимум RSI 1h < 30; `natr_pre` — ATR(14)/close × 100 на 15m перед искрой;
`natr_1d`, `natr_3d` — средний 15m NATR за 96 / 288 свечей; `natr_ratio` = natr_pre / natr_3d; `spark_atr` — true range искры / ATR перед ней;
`hi_from_open`, `close_from_low` — %, `wick_up`, `wick_dn` — тени, % от диапазона свечи.

**Подхват на T+30 (бары T+15, T+20, T+25):** `base5` — средний 5m-объём за [T−24 ч, T−6 ч]; `vx3` — средний объём трёх баров / base5;
`tbr3` — доля тейкер-покупок; `dd3` — минимум трёх баров к закрытию искры, %; `r3` — close T+30 к закрытию искры, %;
`entry` — close T+30 (вход модели); `conf` — подхват (vx3 ≥ 5, tbr3 ≥ 0.45, dd3 > −1.5, r3 > 1); `scenario` — подхват / затухание
(vx3 < 2) / продавцы (vx3 ≥ 5, tbr3 < 0.45, r3 < 0) / прочее / нет данных; `oi_t`, `oi_t30`, `oi30` — OI на T, T+30 и изменение, %;
`short_share` — доля роста цены в барах с падающим OI, %; `score` — балл 0–5 (подхват, L/S < 20, pre6h_log > 3, oi30 ≥ 2, 06–12 МСК);
`score_nan` — сколько входов балла без данных; `rA, rB, rC, rD` — правила по потоку медианы, `rA_mean…` — по потоку
среднего 20 (D = A + «крайности»: RSI 15m > 70 перед искрой или RSI 1h за сутки < 30)
(с «первой за 12 ч» по v0.2; бот берёт флаги потока из config [paper] stream).

**Исходы (`out_*`, от открытия искры `o_ref`):** `max12/24/48`, `min…` — экстремумы за 12/24/48 ч, %; `peakN_min` — минут до пика;
`incN` — пропущенных баров; `pump12` — +25% за 12 ч, включая бары искры; `hit_dir` — от входа T+30: 1 = +10% раньше −5%, −1 = наоборот
(оба в одном баре → −1), 0 = ни то ни другое за 48 ч; `hit_min`; `trade_pct` — модель сделки исследования (стоп = минимум искры − 0.2% от
входа, трейлинг 8%, 48 ч, издержки 0.2%), `trade_bars`, `trade_reason`; `level` — до какого горизонта досчитано (4 = всё).
`out_x_*` — модель выходов v0.2 на 1m-свечах (эталон research_reference_v02): `c` — трейлинг по закрытию 5m (8%, после +15% — 5%),
`ct` — то же + проверка на 6-м часу (+1.5% → закрыть), `calm` — «спокойный» (⅓ на +6%, безубыток +0.2%, остаток трейлингом);
`_pct` — результат net, %, `_reason` — причина, `_min` — минута выхода от входа T+30, `x_calm_tp1_min` — минута фиксации ⅓,
`x_n1m` — сколько 1m-баров пути было (из 2880).

**Прочее:** `capit_active` — был сигнал «капитуляции 4h» за 14 дней до T и не израсходован; `lt_moments` — сколько моментов живых тегов
(закрытия баров T+30 … T+4.5 ч); `lt_max_red` — максимум красных флагов; `lt_last_stage`; `lt_min_to_late` — минут от T до первой
стадии «поздняя»/«выдыхание». `bot_<счёт>_*` — бумажная сделка по этой искре (вход с проскальзыванием, стоп %, выход, результат % и R);
`bot_<счёт>_skip` — причина пропуска.

## Остальные файлы журнала

- `sparks_5m.csv` — 5m-искры: `ret`, `vx` (объём / медиана за 24 ч без последнего часа), `base`, `q24`, `first12` (v0.2: не было
  5m-искры за 12 ч, большие свечи тоже блокируют; пусто вне $0.5–30M и у больших свечей), `first12r` (по исследованию), `big` — «большая свеча» (> +8%, без потолка), контекст как у 15m, форма свечи; `out_*` от открытия бара.
- `live_tags.csv` — живые теги: `mode` bar (закрытие бара, как в исследовании) / fast (фокус, 10–15 с); `gain`, `max_gain` от открытия искры, %;
  `tr_*` — выход v0.2 для ручной сделки от входа T+30 (уровень трейлинга по закрытию, p, причина/момент/цена выхода, если он уже был
  бы, `tr_six` — проверка 6 ч); `ls_from_min` — L/S от минимума с T, %; `oi_from_max` — OI от максимума с T, %; `vol_vs_peak`; `r15` — рост за 15 мин, %;
  `flags` — маска (1 > +10%, 2 L/S > +5% от мин., 4 OI −4% от макс., 8 объём > 50% пика, 16 +2% за 15 мин); `n_red`; `stage`;
  `exit_oi`, `exit_ls` — маркеры выхода; `ls_age_min`; `fork_pct`, `fork_dir` — от цены в момент тега: +10 / −5 первым или close через 48 ч.
- `watchlist_events.csv` — `list` creep («ползут»: вход при росте 6 ч ≥ 5%, `in_list` — L/S < 20 и OI 6 ч > +3%, `outcome` — +25% за 24 ч)
  и capit («капитуляция 4h»: `t_in` — открытие 4h-бара события, `zone_start`, `zone_min`, `rel` — монета к рынку за зону, %,
  `in_list` — L/S < 20, `outcome` — +25% за 14 дней, `spent_ms` — цена ушла > +30% от минимума зоны). `ls_exact` = 0 — L/S неточный.
- `paper_trades.csv` — сделки бота. Счета: A, B, C — трейлинг 8% по сделкам; A-c, D-c, A-c-t — v0.2, трейлинг по закрытию 5m
  (`mode` = close, `mclose` — максимум закрытий, `level_now` — уровень). Вход — по фактической цене в момент решения (vwap стакана на
  размер позиции): `entry_model` (close T+30), `mkt_price` (последняя цена), `entry_price` (фактический), `slip_pct`; хронология
  `bar_close_ms` (закрытие T+30), `ws_ms` (бар пришёл по WS), `conf_calc_ms`, `decision_ms`; `risk_pct` — риск в % своего баланса; `stop_price`,
  `qty`, `notional`, `risk_usd`, `max_price`, `exit_level` (уровень), `exit_price` (исполнение), `exit_src` (trade — по aggTrade, bar — по 5m-бару),
  `fees`, `pnl_usd`, `pnl_pct`, `r_mult`, `stream` (флаги какого потока).
- `paper_skipped.csv` — сигналы правил, которые бот не взял, с причиной.

## v0.4: режим рынка и шорт-счета

- Колонки режима у искр (`sparks_15m`, `sparks_5m` — бар, закрывшийся в T) и у сделок (`paper_trades`, `live_trades` — бар,
  закрывшийся перед входом): `regime` (рост / нейтрально / падение, пусто — нет 72 ч истории), `reg_mkt24`, `reg_mkt72` — индекс
  альтов за 24/72 ч, %, `reg_breadth` — доля монет вселенной выше, чем сутки назад, %, `reg_btc24` — BTC за 24 ч, %, `reg_t` — бар.
- `market_regime.csv` — режим на каждом 5m-баре: `t` — открытие бара, `med` — медиана 5m-лог-доходностей по вселенной
  (индекс — их накопленная сумма), `n_univ` — монет во вселенной.
- `pump_episodes.csv` — эпизоды пампа (только по прошлым данным): `t_base` — бар минимума, `start_t` — бар начала эпизода,
  `base`, `q24` — оборот, $M; `s1_t`, `s2_t` — первый сигнал Ш1 / Ш2; `src` — live / backfill (досчёт эталоном по истории).
- `short_signals.csv` — сигналы: M, base, close, RSI 15m (текущий и максимум за памп), RSI 1h, поля режима.
- `short_trades.csv` — сделки Ш1/Ш2: `status` open / closed / skipped / model (модель по истории, вне баланса); `fills` — JSON
  [[цена, t бара], …], `avg` — средняя; Ш1: `pnl_pct` — % от номинала, `R` — к расстоянию до стопа; Ш2: `pnl_pct` — % от одной
  заявки, `R` — к худшему исходу 64.2%; `notional` — позиция (Ш1) или одна заявка (Ш2); `mae_pct` — максимум против позиции, %;
  `new_high` — цена уходила выше M. Все времена `t*` — открытие 5m-бара (сигнал/выход — на его закрытии, +5 мин).

## v0.5: Ш1-b, Ш2-b, Ш2-c, Ш3 и поздние сигналы

- `short_trades.csv` — те же поля, счета Ш1-b, Ш2-b, Ш2-c (один шорт: стоп +30%, цель −10%, `R` = результат / 30) и Ш3;
  `pump_pct` — сила пампа M/base − 1 в момент сигнала, %; `ep_t_base` — эпизод; у Ш3 — `rsi4h`, `rsi4h_max`.
- `pump_episodes.csv` — добавлены `s1_pump`, `s2_pump` (сила пампа у первых сигналов) и `late1`, `late2` — поздний сигнал
  Ш1-b / Ш2-b,c: none (первый прошёл фильтр), watch (ждём), done (был).
- `virtual_trades.csv` — виртуальные сделки по поздним сигналам: `t_first`, `first_pump` — первый сигнал, `t_signal`, `pump_pct` —
  поздний; исполнения, выход, причина, результат, R; `status` closed / open / skipped / model, `src` live / backfill.
- `pump_episodes_7d.csv` — эпизоды больших пампов (Ш3): `t_base`, `start_t`, `t_peak`, `peak`, `s3_t` — сигнал Ш3.
- Период счёта: кнопка «начать заново» записывает момент в таблицу `controls` (`epoch:<счёт>`); сделки, открытые раньше, —
  прошлый период счёта (в журнале остаются все).

## v0.6: признаки шорт-сигналов, Ш1-c и Ш1-d

- `short_signals.csv` — у каждого сигнала Ш1 и Ш2 признаки эталона v0.6 (`short_signal_features`): `pump_hours`,
  `taker_share_1h`, `taker_share_pump`, `vol_1h_vs_pump`, `vol_pump_vs_day_before`, `oi_pump_pct`, `oi_1h_pct`,
  `oi_from_max_pct`, `oi_usd`, `oi_to_turnover`, `ls_pct_week`, `ls_pump_pct`, `ls_1h_pct`, `top_ls_pos`,
  `top_ls_pos_pump_pct`, `taker_ratio_1h`, `funding_h`, `funding_max_pump_h`; `feat_ms` — когда посчитаны, `feat_note` —
  каких метрик не было (L/S топ-трейдеров и taker ratio Binance хранит 30 дней, у старых сигналов они могут быть пустыми).
- `short_trades.csv` — счета Ш1-c (тейкер-покупки за час ≥ 48%) и Ш1-d (плюс памп не дольше 12 ч); `taker_1h`, `pump_hours` —
  значения фильтров на сигнале.

## Сырьё (raw/)

- `raw_klines_5m` — закрытые 5m-свечи: `v` base volume, `qv` quote, `n` сделок, `tbv`/`tbq` taker buy base/quote, `src` 0 WS / 1 REST.
- `raw_klines_1m` — 1m-свечи на пути сделок (48 ч после входа T+30 у искр с подхватом) — по ним считается модель выходов v0.2.
- `raw_klines_1h` — 1h-свечи (REST) для списка «Перепроданность RSI»; 2h/3h/4h собираются из них от 00:00 UTC.
- `raw_klines_4h` — 4h-свечи (REST, границы 00/04/08… UTC).
- `raw_oi` — `oi_live` (/openInterest сразу после закрытия бара, `live_ms` — момент замера), `oi_hist` / `oi_hist_usd` (openInterestHist).
- `raw_oi_fast` — частый OI монет в фокусе и под наблюдением, `t_ms` — момент замера.
- `raw_ls` — `kind` global (все счета) / top_acc / top_pos (топ-трейдеры); `ratio`, `long_acc`, `short_acc`; `fetched_ms` — когда получена.
- `raw_funding` — снимок на границе 5m (`t` — момент): `rate` за текущий интервал, `mark`, `idx`, `next_ms`; интервал — `meta/symbols.csv` `funding_h`.
- `raw_depth_hourly` — итоги снимка стакана limit=500 раз в час: `mid`, `bid`, `ask`, суммы офферов до +1/+2/+5% (`ask1/2/5`) и бидов до
  −1/−2/−5% от mid, $; `ask2_best` — до +2% от лучшего аска; `ask_cov`/`bid_cov` — докуда дотянулись 500 уровней, %. Полного стакана нет.
- `raw_liquidations` — поток !forceOrder (Binance отдаёт не больше одной ликвидации на монету в секунду).

## meta/

`symbols.csv` (шаг цены и лота, мин. ордер, интервал funding), `gaps.csv` (интервалы, которые не удалось догрузить), `engine_status.csv`,
`config.toml` — пороги, с которыми считались флаги (см. `cfg_ver`/`cfg_hash` в строках).
"""
