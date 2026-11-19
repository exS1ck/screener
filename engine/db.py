"""SQLite в режиме WAL. Пишет только движок, одним писателем, пачками раз в db_flush_sec.
Веб и проверки читают отдельными соединениями."""
import asyncio
import logging
import sqlite3
import threading
from pathlib import Path

log = logging.getLogger("db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS symbols(
    symbol       TEXT PRIMARY KEY,
    base         TEXT,
    status       TEXT,
    onboard_ms   INTEGER,
    tick_size    REAL,
    step_size    REAL,
    min_qty      REAL,
    min_notional REAL,
    updated_ms   INTEGER
);

-- Закрытые 5m-свечи. t — время ОТКРЫТИЯ бара, мс UTC (бар j в эталоне).
-- qv — quote volume (USDT), tbq — taker buy quote volume. src: 0 = WS, 1 = REST.
CREATE TABLE IF NOT EXISTS raw_klines_5m(
    symbol TEXT    NOT NULL,
    t      INTEGER NOT NULL,
    o REAL, h REAL, l REAL, c REAL,
    v   REAL,
    qv  REAL,
    n   INTEGER,
    tbv REAL,
    tbq REAL,
    src INTEGER NOT NULL,
    PRIMARY KEY(symbol, t)
) WITHOUT ROWID;

-- Снимок markPrice@arr на каждой границе 5m. t — момент снимка (граница бара).
CREATE TABLE IF NOT EXISTS raw_funding(
    symbol  TEXT    NOT NULL,
    t       INTEGER NOT NULL,
    rate    REAL,
    mark    REAL,
    idx     REAL,
    next_ms INTEGER,
    PRIMARY KEY(symbol, t)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS raw_liquidations(
    id         INTEGER PRIMARY KEY,
    symbol     TEXT,
    t          INTEGER,
    side       TEXT,
    price      REAL,
    avg_price  REAL,
    qty        REAL,
    filled_qty REAL,
    status     TEXT
);
CREATE INDEX IF NOT EXISTS ix_liq_t ON raw_liquidations(t);

CREATE TABLE IF NOT EXISTS ticker_latest(
    symbol  TEXT PRIMARY KEY,
    t       INTEGER,
    price   REAL,
    qv24    REAL,
    chg_pct REAL
);

-- Интервалы, которые не удалось догрузить. Искры в них помечаются как неполные.
CREATE TABLE IF NOT EXISTS gaps(
    symbol     TEXT,
    kind       TEXT,
    start_ms   INTEGER,
    end_ms     INTEGER,
    reason     TEXT,
    created_ms INTEGER,
    PRIMARY KEY(symbol, kind, start_ms)
);

-- OI на сетке 5m: значение на КОНЕЦ бара, t — время ОТКРЫТИЯ этого бара (oi[k] в эталоне).
-- oi_live — /fapi/v1/openInterest сразу после закрытия бара (live_ms — момент замера на бирже).
-- oi_hist — openInterestHist, точка с меткой X кладётся в t = X - 5 мин (так же мерило исследование).
CREATE TABLE IF NOT EXISTS raw_oi(
    symbol      TEXT    NOT NULL,
    t           INTEGER NOT NULL,
    oi_live     REAL,
    live_ms     INTEGER,
    oi_hist     REAL,
    oi_hist_usd REAL,
    PRIMARY KEY(symbol, t)
) WITHOUT ROWID;

-- Частый OI для монет под наблюдением и «в фокусе». t_ms — момент замера на бирже.
CREATE TABLE IF NOT EXISTS raw_oi_fast(
    symbol TEXT    NOT NULL,
    t_ms   INTEGER NOT NULL,
    oi     REAL,
    PRIMARY KEY(symbol, t_ms)
) WITHOUT ROWID;

-- L/S: kind = global / top_acc / top_pos. Точка API с меткой X → t = X - 5 мин (ls[k] в эталоне).
-- fetched_ms — когда точку получили впервые (для замера задержки публикации).
CREATE TABLE IF NOT EXISTS raw_ls(
    symbol     TEXT    NOT NULL,
    kind       TEXT    NOT NULL,
    t          INTEGER NOT NULL,
    ratio      REAL,
    long_acc   REAL,
    short_acc  REAL,
    fetched_ms INTEGER,
    PRIMARY KEY(symbol, kind, t)
) WITHOUT ROWID;

-- Снимок стакана (limit=500), суммы в USDT. askN — офферы до +N% от mid, bidN — биды до −N%.
-- ask2_best — офферы до +2% от лучшего аска (от какой цены меряет архив bookDepth — не проверено).
-- ask_cov / bid_cov — докуда в % от mid дотянулись 500 уровней: если < 2, ask2 занижен.
CREATE TABLE IF NOT EXISTS raw_depth_hourly(
    symbol    TEXT    NOT NULL,
    t         INTEGER NOT NULL,
    mid       REAL,
    bid       REAL,
    ask       REAL,
    ask1 REAL, ask2 REAL, ask5 REAL,
    ask2_best REAL,
    bid1 REAL, bid2 REAL, bid5 REAL,
    ask_cov   REAL,
    bid_cov   REAL,
    PRIMARY KEY(symbol, t)
) WITHOUT ROWID;

-- Монеты «в фокусе». Пишет веб по кнопке (единственная таблица, куда пишет веб).
CREATE TABLE IF NOT EXISTS focus(
    symbol TEXT    NOT NULL,
    on_ms  INTEGER NOT NULL,
    off_ms INTEGER,
    PRIMARY KEY(symbol, on_ms)
);

-- 15m-искры (этап 2). t = T — открытие 15m-свечи. Строка пишется, если свеча — кандидат хотя бы по
-- одной базе объёма. cand_med — v0.1 (медиана, оборот от $0.5M), cand_mean — правило исследования
-- (среднее 20 свечей, оборот $0.5–30M). first12_* — «первая за 12 ч» в своём потоке кандидатов;
-- считается только в проверенном диапазоне оборота, вне его NULL. Свеча > +8% (in_range=0) тоже
-- блокирует следующие 12 ч, как в исследовании. incomplete — сколько 5m-баров нет в окне 24 ч.
CREATE TABLE IF NOT EXISTS sparks_15m(
    symbol        TEXT    NOT NULL,
    t             INTEGER NOT NULL,
    o REAL, h REAL, l REAL, c REAL,
    q             REAL,
    tbq           REAL,
    ret           REAL,
    relvol_med    REAL,
    relvol_mean20 REAL,
    q24           REAL,
    cand_med      INTEGER,
    cand_mean     INTEGER,
    first12_med   INTEGER,
    first12_mean  INTEGER,
    in_range      INTEGER,
    liq_tested    INTEGER,
    incomplete    INTEGER,
    src           TEXT,
    detected_ms   INTEGER,
    cfg_ver       INTEGER,
    cfg_hash      TEXT,
    PRIMARY KEY(symbol, t)
);
CREATE INDEX IF NOT EXISTS ix_sparks15_t ON sparks_15m(t);

-- 5m-искры (ранний сигнал). Пишутся все с ростом +2…+8% и оборотом от $0.5M.
-- first12 — как в исследовании (потолок +8% до отбора), только в проверенном диапазоне оборота.
CREATE TABLE IF NOT EXISTS sparks_5m(
    symbol      TEXT    NOT NULL,
    t           INTEGER NOT NULL,
    o REAL, c REAL,
    qv          REAL,
    ret         REAL,
    base        REAL,
    vx          REAL,
    q24         REAL,
    first12     INTEGER,
    liq_tested  INTEGER,
    incomplete  INTEGER,
    src         TEXT,
    detected_ms INTEGER,
    cfg_ver     INTEGER,
    cfg_hash    TEXT,
    PRIMARY KEY(symbol, t)
);
CREATE INDEX IF NOT EXISTS ix_sparks5_t ON sparks_5m(t);

-- Исходы искр (этап 5). kind = 15m / 5m. Проценты — от открытия искры (o_ref), кроме hit и trade.
-- maxN/minN — максимум high и минимум low за N часов от T (бары j … j+12N−1), peakN_min — минуты от T до пика.
-- pump12 — +25% от открытия искры за 12 ч, включая бары самой искры (как в исследовании).
-- hit_dir — что раньше от входа T+30 (только 15m): 1 = +10%, −1 = −5% (оба в одном баре — считаем −5%),
--   0 = ни то ни другое за 48 ч; hit_min — минуты от входа.
-- trade_* — модель сделки исследования (trade_trailing: стоп, трейлинг 8%, 48 ч, издержки 0.2%).
-- level — до какого горизонта досчитано: 1 = 12 ч, 2 = 24 ч, 3 = 48 ч, 4 = 48 ч и модель сделки.
CREATE TABLE IF NOT EXISTS outcomes(
    kind         TEXT    NOT NULL,
    symbol       TEXT    NOT NULL,
    t            INTEGER NOT NULL,
    o_ref        REAL,
    entry        REAL,
    max12 REAL, min12 REAL, peak12_min INTEGER, inc12 INTEGER,
    max24 REAL, min24 REAL, peak24_min INTEGER, inc24 INTEGER,
    max48 REAL, min48 REAL, peak48_min INTEGER, inc48 INTEGER,
    pump12       INTEGER,
    hit_dir      INTEGER,
    hit_min      INTEGER,
    trade_pct    REAL,
    trade_bars   INTEGER,
    trade_reason TEXT,
    level        INTEGER,
    computed_ms  INTEGER,
    PRIMARY KEY(kind, symbol, t)
);

-- Живые теги (этап 6): стадия движения, пять красных флагов, маркеры выхода.
-- mode: bar — на закрытии 5m-бара (как в исследовании), fast — раз в 10–15 с для монет «в фокусе»
-- (текущий незакрытый бар и последний живой OI; пороги для этого режима ещё проверим по журналу).
-- Для каждой искры от T+30 до T+4.5 ч; в фокусе — дольше. t_spark — T искры, t_ms — момент расчёта.
-- flags — битовая маска: 1 ext (> +10% от искры), 2 ls (L/S > +5% от минимума), 4 oi (OI −4% от максимума),
-- 8 vol (объём 15 мин > 50% пика), 16 chase (+2% за 15 мин). ls_age_min — возраст последней точки L/S.
CREATE TABLE IF NOT EXISTS live_tags(
    symbol      TEXT    NOT NULL,
    t_spark     INTEGER NOT NULL,
    t_ms        INTEGER NOT NULL,
    mode        TEXT,
    price       REAL,
    gain        REAL,
    max_gain    REAL,
    ls_from_min REAL,
    oi_from_max REAL,
    vol_vs_peak REAL,
    r15         REAL,
    flags       INTEGER,
    n_red       INTEGER,
    stage       TEXT,
    exit_oi     INTEGER,
    exit_ls     INTEGER,
    ls_age_min  INTEGER,
    PRIMARY KEY(symbol, t_spark, t_ms)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_live_tags_t ON live_tags(t_ms);

-- Последнее состояние живых тегов по каждой искре — для ленты.
CREATE TABLE IF NOT EXISTS live_latest(
    symbol      TEXT    NOT NULL,
    t_spark     INTEGER NOT NULL,
    t_ms        INTEGER,
    mode        TEXT,
    price       REAL,
    gain        REAL,
    max_gain    REAL,
    ls_from_min REAL,
    oi_from_max REAL,
    vol_vs_peak REAL,
    r15         REAL,
    flags       INTEGER,
    n_red       INTEGER,
    stage       TEXT,
    exit_oi     INTEGER,
    exit_ls     INTEGER,
    ls_age_min  INTEGER,
    PRIMARY KEY(symbol, t_spark)
) WITHOUT ROWID;

-- 1m-свечи — только на пути сделок (48 ч после входа T+30 у искр с подхватом), для модели выходов v0.2.
CREATE TABLE IF NOT EXISTS raw_klines_1m(
    symbol TEXT    NOT NULL,
    t      INTEGER NOT NULL,
    o REAL, h REAL, l REAL, c REAL,
    qv     REAL,
    PRIMARY KEY(symbol, t)
) WITHOUT ROWID;

-- 1h-свечи (REST) для «перепроданности RSI»: из них же собираются 2h, 3h и 4h.
CREATE TABLE IF NOT EXISTS raw_klines_1h(
    symbol TEXT    NOT NULL,
    t      INTEGER NOT NULL,
    o REAL, h REAL, l REAL, c REAL,
    qv     REAL,
    PRIMARY KEY(symbol, t)
) WITHOUT ROWID;

-- Watchlist «Перепроданность RSI»: монеты, у которых RSI(14) < 30 хотя бы на одном таймфрейме.
-- rsiNh — RSI на последнем закрытом баре; sinceNh — закрытие первого бара текущей серии ниже порога
-- (пусто — на этом таймфрейме не перепродан); ls, ls_pct — L/S всех счетов и его процентиль за 7 дней.
CREATE TABLE IF NOT EXISTS rsi_now(
    symbol   TEXT PRIMARY KEY,
    t        INTEGER,
    rsi1h REAL, rsi2h REAL, rsi3h REAL, rsi4h REAL,
    since1h INTEGER, since2h INTEGER, since3h INTEGER, since4h INTEGER,
    ls       REAL,
    ls_pct   REAL,
    ls_exact INTEGER,
    q24      REAL,
    price    REAL,
    upd_ms   INTEGER
);

-- 4h-свечи для WVF и «капитуляции 4h» (REST, границы 00/04/08… UTC). t — открытие.
CREATE TABLE IF NOT EXISTS raw_klines_4h(
    symbol TEXT    NOT NULL,
    t      INTEGER NOT NULL,
    o REAL, h REAL, l REAL, c REAL,
    qv     REAL,
    PRIMARY KEY(symbol, t)
) WITHOUT ROWID;

-- События списков наблюдения (этап 7). list = creep («ползут сейчас») / capit («капитуляция 4h»).
-- creep: t_in — открытие 5m-бара входа t (признаки на его закрытии), t_out — бар выхода;
--   pre6 — рост 6 ч close к close, ls_pct — процентиль L/S по ls[t], oi6 — OI за 6 ч, q24 — оборот;
--   in_list — условие списка (L/S < 20 и OI 6 ч > +3%); outcome — +25% за 24 ч от close бара входа.
-- capit: t_in — открытие 4h-бара события (первый не зелёный после зоны), zone_start — открытие первого
--   зелёного бара, zone_min — минимум зоны, ls_pct — L/S на открытии бара события, rel — монета против
--   рынка за зону, in_list — сигнал (L/S < 20); outcome — +25% за 14 дней от close бара события;
--   spent_ms — когда цена ушла больше чем на +30% от минимума зоны («израсходован»).
-- ls_exact = 0 — L/S взят не точный (ещё не опубликован или неполная история за неделю).
CREATE TABLE IF NOT EXISTS watchlist_events(
    list        TEXT    NOT NULL,
    symbol      TEXT    NOT NULL,
    t_in        INTEGER NOT NULL,
    t_out       INTEGER,
    close_in    REAL,
    pre6        REAL,
    ls_pct      REAL,
    ls_exact    INTEGER,
    oi6         REAL,
    q24         REAL,
    zone_start  INTEGER,
    zone_min    REAL,
    rel         REAL,
    in_list     INTEGER,
    outcome     INTEGER,
    outcome_max REAL,
    outcome_ms  INTEGER,
    spent_ms    INTEGER,
    src         TEXT,
    detected_ms INTEGER,
    PRIMARY KEY(list, symbol, t_in)
);
CREATE INDEX IF NOT EXISTS ix_watch_t ON watchlist_events(t_in);

-- Текущее состояние активных записей списков — для вкладки Watchlist.
CREATE TABLE IF NOT EXISTS watch_now(
    list     TEXT    NOT NULL,
    symbol   TEXT    NOT NULL,
    t_in     INTEGER NOT NULL,
    t        INTEGER,
    price    REAL,
    pre6     REAL,
    r24      REAL,
    oi6      REAL,
    ls_pct   REAL,
    q24      REAL,
    from_min REAL,
    PRIMARY KEY(list, symbol, t_in)
);

-- Бумажный бот (этап 8). Ключ сделки — (счёт, монета, T искры).
-- entry_model — close бара T+30 (вход в модели исследования), slip_pct — оценка проскальзывания по стакану,
-- entry_price = entry_model × (1 + slip). mkt_price — цена в момент решения (для сравнения).
-- stop_price = минимум 15m-искры − 0.2% от entry_model. Трейлинг: уровень = max(стоп, максимум × (1 − trail)).
-- exit_level — уровень, на котором сработал выход; exit_price — цена исполнения (первая сделка на уровне
-- или ниже из aggTrade; src = bar — запасная проверка по 5m-бару, исполнение ровно по уровню).
CREATE TABLE IF NOT EXISTS paper_trades(
    acc          TEXT    NOT NULL,
    symbol       TEXT    NOT NULL,
    t_spark      INTEGER NOT NULL,
    stream       TEXT,
    entry_ms     INTEGER,
    entry_model  REAL,
    slip_pct     REAL,
    entry_price  REAL,
    mkt_price    REAL,
    stop_price   REAL,
    stop_pct     REAL,
    qty          REAL,
    notional     REAL,
    margin       REAL,
    risk_usd     REAL,
    balance_in   REAL,
    both_bc      INTEGER,
    max_price    REAL,
    level_now    REAL,
    last_bar_t   INTEGER,
    upd_ms       INTEGER,
    exit_ms      INTEGER,
    exit_level   REAL,
    exit_price   REAL,
    exit_reason  TEXT,
    exit_src     TEXT,
    fees         REAL,
    pnl_usd      REAL,
    pnl_pct      REAL,
    r_mult       REAL,
    cfg_ver      INTEGER,
    PRIMARY KEY(acc, symbol, t_spark)
);
CREATE INDEX IF NOT EXISTS ix_paper_open ON paper_trades(exit_ms);

-- Ручное закрытие сделки бота (кнопка на вкладке «Бот»; пишет веб). Сделка бота идёт дальше по правилам,
-- а в «ручном» подсчёте берётся эта цена. price — последняя цена в момент нажатия.
CREATE TABLE IF NOT EXISTS manual_exits(
    acc     TEXT    NOT NULL,
    symbol  TEXT    NOT NULL,
    t_spark INTEGER NOT NULL,
    ms      INTEGER,
    price   REAL,
    PRIMARY KEY(acc, symbol, t_spark)
);

-- Биржа (реальный счёт). ex_log — каждый подписанный запрос (без ключей и подписи), ex_events — каждое событие
-- потока счёта (как пришло), ex_state — последний снимок сверки с биржей (JSON).
CREATE TABLE IF NOT EXISTS ex_log(
    id      INTEGER PRIMARY KEY,
    ms      INTEGER,
    method  TEXT,
    path    TEXT,
    params  TEXT,
    status  INTEGER,
    code    INTEGER,
    msg     TEXT,
    dur_ms  INTEGER
);
CREATE INDEX IF NOT EXISTS ix_ex_log_ms ON ex_log(ms);
CREATE TABLE IF NOT EXISTS ex_events(
    id   INTEGER PRIMARY KEY,
    ms   INTEGER,
    type TEXT,
    data TEXT
);
CREATE INDEX IF NOT EXISTS ix_ex_events_ms ON ex_events(ms);
-- Команды с вкладки «Торговля» (пишет веб, выполняет движок): test — тестовый цикл по монете, panic — аварийная кнопка.
CREATE TABLE IF NOT EXISTS ex_commands(
    id          INTEGER PRIMARY KEY,
    ms          INTEGER,
    cmd         TEXT,
    symbol      TEXT,
    status      TEXT,
    started_ms  INTEGER,
    finished_ms INTEGER
);
-- Хронология выполнения команды: каждый шаг (проверка, вход, стоп, выход…), удачно или нет, подробности.
CREATE TABLE IF NOT EXISTS ex_steps(
    id     INTEGER PRIMARY KEY,
    cmd_id INTEGER,
    ms     INTEGER,
    step   TEXT,
    ok     INTEGER,
    detail TEXT
);
-- Реальные позиции, которые открыл бот (аварийная кнопка закрывает только их).
CREATE TABLE IF NOT EXISTS ex_positions(
    symbol   TEXT,
    kind     TEXT,
    cmd_id   INTEGER,
    qty      REAL,
    entry    REAL,
    ms       INTEGER,
    PRIMARY KEY(symbol, cmd_id)
);
-- Реальные сделки системы U. status: opening → open → closed (или skipped — решение не входить, причина в exit_reason).
CREATE TABLE IF NOT EXISTS live_trades(
    id           INTEGER PRIMARY KEY,
    symbol       TEXT,
    t_spark      INTEGER,
    status       TEXT,
    bar_close_ms INTEGER,
    decision_ms  INTEGER,
    entry_ms     INTEGER,
    qty_s        TEXT,
    entry        REAL,
    stop         REAL,
    stop_cid     TEXT,
    algo_id      TEXT,
    max_price    REAL,
    mclose       REAL,
    close_t      INTEGER,
    exit_ms      INTEGER,
    exit_price   REAL,
    exit_reason  TEXT,
    pnl_usd      REAL,
    fees_usd     REAL
);
CREATE TABLE IF NOT EXISTS live_steps(
    id       INTEGER PRIMARY KEY,
    trade_id INTEGER,
    ms       INTEGER,
    step     TEXT,
    ok       INTEGER,
    detail   TEXT
);
CREATE TABLE IF NOT EXISTS ex_state(
    key   TEXT PRIMARY KEY,
    value TEXT,
    ms    INTEGER
);

-- v0.4: режим рынка на каждом закрытом 5m-баре (t — открытие бара). med — медиана 5m-лог-доходностей монет
-- вселенной на этом баре (индекс рынка — их накопленная сумма), mkt24/mkt72 — exp(сумма med за 24/72 ч) − 1, %.
CREATE TABLE IF NOT EXISTS market_regime(
    t         INTEGER PRIMARY KEY,
    med       REAL,
    mkt24     REAL,
    mkt72     REAL,
    breadth24 REAL,
    btc24     REAL,
    regime    TEXT,
    n_univ    INTEGER,
    ms        INTEGER
);
-- v0.4: эпизоды пампа (только по прошлым данным). start_t / t_base — открытие баров k_start и t_base.
CREATE TABLE IF NOT EXISTS pump_episodes(
    symbol  TEXT    NOT NULL,
    t_base  INTEGER NOT NULL,
    start_t INTEGER,
    base    REAL,
    q24     REAL,
    s1_t    INTEGER,
    s2_t    INTEGER,
    src     TEXT,
    ms      INTEGER,
    PRIMARY KEY(symbol, t_base)
);
-- v0.4: сигналы шорт-счетов — первый сигнал Ш1 и Ш2 в эпизоде (t — открытие сигнального бара).
CREATE TABLE IF NOT EXISTS short_signals(
    acc      TEXT    NOT NULL,
    symbol   TEXT    NOT NULL,
    t        INTEGER NOT NULL,
    t_base   INTEGER,
    M        REAL,
    base     REAL,
    close    REAL,
    rsi15    REAL,
    rsi15_max REAL,
    rsi1h    REAL,
    mkt24    REAL,
    mkt72    REAL,
    breadth24 REAL,
    btc24    REAL,
    regime   TEXT,
    src      TEXT,
    ms       INTEGER,
    PRIMARY KEY(acc, symbol, t)
);
-- v0.4: сделки шорт-счетов Ш1/Ш2. status: open / closed / skipped (не взяли, причина в reason) /
-- model (досчитано по истории эталоном, в баланс счёта не входит). fills — JSON [[цена, t бара], …].
-- Ш1: pnl_pct — результат в % от номинала; Ш2: pnl_pct — в % от размера одной заявки (notional — одна заявка).
CREATE TABLE IF NOT EXISTS short_trades(
    id        INTEGER PRIMARY KEY,
    acc       TEXT,
    symbol    TEXT,
    t_signal  INTEGER,
    status    TEXT,
    src       TEXT,
    entry     REAL,
    stop      REAL,
    target    REAL,
    risk_pct  REAL,
    fills     TEXT,
    avg       REAL,
    notional  REAL,
    balance_in REAL,
    last_t    INTEGER,
    max_high  REAL,
    min_low   REAL,
    M         REAL,
    exit_t    INTEGER,
    exit_price REAL,
    reason    TEXT,
    pnl_pct   REAL,
    R         REAL,
    pnl_usd   REAL,
    mae_pct   REAL,
    new_high  INTEGER,
    ms        INTEGER,
    UNIQUE(acc, symbol, t_signal)
);

-- v0.5: эпизоды больших пампов за 7 дней (Ш3). t_peak — бар максимума за 48 ч эпизода (пишется по окончании окна).
CREATE TABLE IF NOT EXISTS pump_episodes_7d(
    symbol  TEXT    NOT NULL,
    t_base  INTEGER NOT NULL,
    start_t INTEGER,
    t_peak  INTEGER,
    base    REAL,
    peak    REAL,
    s3_t    INTEGER,
    src     TEXT,
    ms      INTEGER,
    PRIMARY KEY(symbol, t_base)
);
-- v0.5: виртуальные сделки по поздним сигналам Ш1-b / Ш2-b / Ш2-c (не исполняются, вне баланса и лимитов).
-- t_first / first_pump — первый сигнал эпизода (не прошёл фильтр силы пампа), t_signal / pump_pct — поздний.
CREATE TABLE IF NOT EXISTS virtual_trades(
    id        INTEGER PRIMARY KEY,
    acc       TEXT,
    symbol    TEXT,
    t_signal  INTEGER,
    t_first   INTEGER,
    first_pump REAL,
    pump_pct  REAL,
    ep_t_base INTEGER,
    status    TEXT,
    src       TEXT,
    entry     REAL,
    stop      REAL,
    target    REAL,
    risk_pct  REAL,
    fills     TEXT,
    avg       REAL,
    last_t    INTEGER,
    max_high  REAL,
    min_low   REAL,
    M         REAL,
    exit_t    INTEGER,
    exit_price REAL,
    reason    TEXT,
    pnl_pct   REAL,
    R         REAL,
    mae_pct   REAL,
    new_high  INTEGER,
    ms        INTEGER,
    UNIQUE(acc, symbol, t_signal)
);

-- Переключатели из веба: trade_live = on/off — автоторговля на реальном счёте (вкладка «Торговля», шаблон B-c).
CREATE TABLE IF NOT EXISTS controls(
    key   TEXT PRIMARY KEY,
    value TEXT,
    ms    INTEGER
);

-- Сигналы правил, которые бот не взял, и почему: лимит позиций, стоп дальше 15%, пауза, опоздание и т. п.
CREATE TABLE IF NOT EXISTS paper_skipped(
    acc     TEXT    NOT NULL,
    symbol  TEXT    NOT NULL,
    t_spark INTEGER NOT NULL,
    reason  TEXT,
    detail  TEXT,
    ms      INTEGER,
    PRIMARY KEY(acc, symbol, t_spark)
);

CREATE TABLE IF NOT EXISTS engine_status(
    key   TEXT PRIMARY KEY,
    value TEXT,
    t     INTEGER
);
"""

SQL_KLINE = ("INSERT INTO raw_klines_5m(symbol,t,o,h,l,c,v,qv,n,tbv,tbq,src) "
             "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(symbol,t) DO NOTHING")
SQL_FUNDING = ("INSERT OR REPLACE INTO raw_funding(symbol,t,rate,mark,idx,next_ms) "
               "VALUES(?,?,?,?,?,?)")
SQL_LIQ = ("INSERT INTO raw_liquidations(symbol,t,side,price,avg_price,qty,filled_qty,status) "
           "VALUES(?,?,?,?,?,?,?,?)")
SQL_TICKER = "INSERT OR REPLACE INTO ticker_latest(symbol,t,price,qv24,chg_pct) VALUES(?,?,?,?,?)"
SQL_GAP = ("INSERT OR REPLACE INTO gaps(symbol,kind,start_ms,end_ms,reason,created_ms) "
           "VALUES(?,?,?,?,?,?)")
SQL_OI_LIVE = ("INSERT INTO raw_oi(symbol,t,oi_live,live_ms) VALUES(?,?,?,?) "
               "ON CONFLICT(symbol,t) DO UPDATE SET oi_live=excluded.oi_live, live_ms=excluded.live_ms "
               "WHERE raw_oi.oi_live IS NULL")
SQL_OI_HIST = ("INSERT INTO raw_oi(symbol,t,oi_hist,oi_hist_usd) VALUES(?,?,?,?) "
               "ON CONFLICT(symbol,t) DO UPDATE SET oi_hist=excluded.oi_hist, oi_hist_usd=excluded.oi_hist_usd")
SQL_OI_FAST = "INSERT OR IGNORE INTO raw_oi_fast(symbol,t_ms,oi) VALUES(?,?,?)"
SQL_LS = ("INSERT INTO raw_ls(symbol,kind,t,ratio,long_acc,short_acc,fetched_ms) VALUES(?,?,?,?,?,?,?) "
          "ON CONFLICT(symbol,kind,t) DO NOTHING")
SQL_DEPTH = ("INSERT OR REPLACE INTO raw_depth_hourly(symbol,t,mid,bid,ask,ask1,ask2,ask5,ask2_best,"
             "bid1,bid2,bid5,ask_cov,bid_cov) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
SQL_SPARK15 = ("INSERT OR IGNORE INTO sparks_15m(symbol,t,o,h,l,c,q,tbq,ret,relvol_med,relvol_mean20,q24,"
               "cand_med,cand_mean,first12_med,first12_mean,in_range,liq_tested,incomplete,src,detected_ms,"
               "cfg_ver,cfg_hash,first12r_med,first12r_mean) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
SQL_SPARK5 = ("INSERT OR IGNORE INTO sparks_5m(symbol,t,o,c,qv,ret,base,vx,q24,first12,liq_tested,incomplete,"
              "src,detected_ms,cfg_ver,cfg_hash,big,first12r) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
OUTCOME_COLS = ["kind", "symbol", "t", "o_ref", "entry",
                "max12", "min12", "peak12_min", "inc12", "max24", "min24", "peak24_min", "inc24",
                "max48", "min48", "peak48_min", "inc48", "pump12", "hit_dir", "hit_min",
                "trade_pct", "trade_bars", "trade_reason", "level", "computed_ms"]
SQL_OUTCOME = (f"INSERT OR REPLACE INTO outcomes({','.join(OUTCOME_COLS)}) "
               f"VALUES({','.join('?' * len(OUTCOME_COLS))})")
LIVE_COLS = ["symbol", "t_spark", "t_ms", "mode", "price", "gain", "max_gain", "ls_from_min", "oi_from_max",
             "vol_vs_peak", "r15", "flags", "n_red", "stage", "exit_oi", "exit_ls", "ls_age_min",
             "tr_level", "tr_p", "tr_reason", "tr_exit_ms", "tr_exit_px", "tr_six"]
SQL_LIVE = f"INSERT OR REPLACE INTO live_tags({','.join(LIVE_COLS)}) VALUES({','.join('?' * len(LIVE_COLS))})"
SQL_LIVE_LATEST = (f"INSERT OR REPLACE INTO live_latest({','.join(LIVE_COLS)}) "
                   f"VALUES({','.join('?' * len(LIVE_COLS))})")
SQL_K1 = "INSERT OR REPLACE INTO raw_klines_1m(symbol,t,o,h,l,c,qv) VALUES(?,?,?,?,?,?,?)"
SQL_K1H = "INSERT OR REPLACE INTO raw_klines_1h(symbol,t,o,h,l,c,qv) VALUES(?,?,?,?,?,?,?)"
SQL_RSI_NOW = ("INSERT OR REPLACE INTO rsi_now(symbol,t,rsi1h,rsi2h,rsi3h,rsi4h,since1h,since2h,since3h,since4h,"
               "ls,ls_pct,ls_exact,q24,price,upd_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
SQL_K4 = "INSERT OR REPLACE INTO raw_klines_4h(symbol,t,o,h,l,c,qv) VALUES(?,?,?,?,?,?,?)"
WATCH_COLS = ["list", "symbol", "t_in", "t_out", "close_in", "pre6", "ls_pct", "ls_exact", "oi6", "q24",
              "zone_start", "zone_min", "rel", "in_list", "outcome", "outcome_max", "outcome_ms", "spent_ms",
              "src", "detected_ms"]
SQL_WATCH_EVENT = (f"INSERT OR IGNORE INTO watchlist_events({','.join(WATCH_COLS)}) "
                   f"VALUES({','.join('?' * len(WATCH_COLS))})")
NOW_COLS = ["list", "symbol", "t_in", "t", "price", "pre6", "r24", "oi6", "ls_pct", "q24", "from_min"]
SQL_WATCH_NOW = f"INSERT OR REPLACE INTO watch_now({','.join(NOW_COLS)}) VALUES({','.join('?' * len(NOW_COLS))})"
PAPER_OPEN_COLS = ["acc", "symbol", "t_spark", "stream", "entry_ms", "entry_model", "slip_pct", "entry_price",
                   "mkt_price", "stop_price", "stop_pct", "qty", "notional", "margin", "risk_usd", "balance_in",
                   "both_bc", "max_price", "level_now", "last_bar_t", "upd_ms", "cfg_ver",
                   "bar_close_ms", "ws_ms", "conf_calc_ms", "decision_ms", "order_ms", "fill_ms", "risk_pct", "mode"]
SQL_PAPER_OPEN = (f"INSERT OR IGNORE INTO paper_trades({','.join(PAPER_OPEN_COLS)}) "
                  f"VALUES({','.join('?' * len(PAPER_OPEN_COLS))})")
SQL_PAPER_STATE = ("UPDATE paper_trades SET max_price=?, level_now=?, last_bar_t=?, upd_ms=?, mclose=?, close_t=?, "
                   "six_done=?, tp1_ms=?, tp1_px=?, frac=?, stop_now=? "
                   "WHERE acc=? AND symbol=? AND t_spark=? AND exit_ms IS NULL")
SQL_PAPER_CLOSE = ("UPDATE paper_trades SET max_price=?, level_now=?, exit_ms=?, exit_level=?, exit_price=?, "
                   "exit_reason=?, exit_src=?, fees=?, pnl_usd=?, pnl_pct=?, r_mult=?, upd_ms=? "
                   "WHERE acc=? AND symbol=? AND t_spark=? AND exit_ms IS NULL")
SQL_PAPER_SKIP = ("INSERT OR IGNORE INTO paper_skipped(acc,symbol,t_spark,reason,detail,ms,bar_close_ms,ws_ms,"
                  "conf_calc_ms,decision_ms) VALUES(?,?,?,?,?,?,?,?,?,?)")
SQL_EX_LOG = "INSERT INTO ex_log(ms,method,path,params,status,code,msg,dur_ms) VALUES(?,?,?,?,?,?,?,?)"
SQL_EX_EVENT = "INSERT INTO ex_events(ms,type,data) VALUES(?,?,?)"
SQL_EX_STEP = "INSERT INTO ex_steps(cmd_id,ms,step,ok,detail) VALUES(?,?,?,?,?)"
SQL_EX_POS = "INSERT OR REPLACE INTO ex_positions(symbol,kind,cmd_id,qty,entry,ms) VALUES(?,?,?,?,?,?)"
SQL_LIVE_STEP = "INSERT INTO live_steps(trade_id,ms,step,ok,detail) VALUES(?,?,?,?,?)"
SQL_LIVE_TRADE_NEW = "INSERT INTO live_trades(symbol,t_spark,status,bar_close_ms,decision_ms) VALUES(?,?,?,?,?)"
SQL_LIVE_TRADE_OPEN = ("UPDATE live_trades SET status='open', entry_ms=?, qty_s=?, entry=?, stop=?, stop_cid=?, close_t=? "
                       "WHERE id=?")
SQL_LIVE_TRADE_UPD = ("UPDATE live_trades SET status=?, max_price=?, mclose=?, close_t=?, algo_id=?, exit_ms=?, "
                      "exit_price=?, exit_reason=?, pnl_usd=?, fees_usd=? WHERE id=?")
SQL_EX_STATE = "INSERT OR REPLACE INTO ex_state(key,value,ms) VALUES(?,?,?)"
SQL_STATUS = "INSERT OR REPLACE INTO engine_status(key,value,t) VALUES(?,?,?)"
SQL_SYMBOL = ("INSERT OR REPLACE INTO symbols(symbol,base,status,onboard_ms,tick_size,step_size,"
              "min_qty,min_notional,updated_ms) VALUES(?,?,?,?,?,?,?,?,?)")


# Сырые таблицы со столбцами symbol и t (мс) — чистятся скользящим окном.
RAW_TABLES = ("raw_klines_5m", "raw_funding", "raw_oi", "raw_ls", "raw_depth_hourly", "raw_klines_4h",
              "raw_klines_1m", "raw_klines_1h")


# Колонки, добавленные после создания таблиц (этап 3): добавляются при старте, данные не теряются.
# Контекст на момент искры T — и у 15m-, и у 5m-искр.
CTX_COLS = {
    "ls_at": "REAL",          # L/S всех счетов на момент T (ls[j-1])
    "ls_pct": "REAL",         # его процентиль среди 7 дней (строго меньших), %
    "pre6h_log": "REAL",      # рост за 6 ч, лог-процент (правило C, балл)
    "hour_msk": "INTEGER",
    "ask2_med": "REAL",       # медиана офферов до +2% от mid по снимкам [T-24ч, T-6ч]
    "ask2_best_med": "REAL",  # то же от лучшего аска
    "ask2_n": "INTEGER",      # сколько снимков стакана в окне
    "week_range": "REAL",     # максимум / минимум за 7 дней − 1, %
    "funding8": "REAL",       # последний funding до T в пересчёте на 8 ч, %
    "ctx_ms": "INTEGER",      # когда посчитан контекст
}
# Подхват на T+30, балл и правила — только у 15m-искр.
CONF_COLS = {
    "base5": "REAL", "vx3": "REAL", "tbr3": "REAL", "dd3": "REAL", "r3": "REAL",
    "entry": "REAL",          # close бара j+5 — цена входа бота
    "conf": "INTEGER",        # подхват выполнен
    "scenario": "TEXT",       # подхват / затухание / продавцы / прочее / нет данных
    "oi_t": "REAL",           # OI на момент T (oi[j-1])
    "oi_usd_t": "REAL",       # OI на момент T в долларах = oi_t × open искры
    "oi_t30": "REAL",         # OI на T+30 (oi[j+5])
    "oi30": "REAL",           # рост OI за 30 мин, %
    "short_share": "REAL",    # доля роста цены в барах с падающим OI, %
    "score": "INTEGER",       # балл 0–5
    "score_nan": "INTEGER",   # сколько входов балла не посчитано (данных нет)
    "rA": "INTEGER", "rB": "INTEGER", "rC": "INTEGER",                  # правила, поток медианы
    "rA_mean": "INTEGER", "rB_mean": "INTEGER", "rC_mean": "INTEGER",   # правила, поток среднего 20
    "conf_ms": "INTEGER",
}
# Индикаторы (RSI, MFI, NATR, объём к медиане за 40 свечей) — на момент T, только у 15m-искр.
IND15_COLS = {
    "rsi15_pre": "REAL",      # RSI(14) 15m на последней свече до T
    "rsi1h": "REAL",          # RSI(14) 1h на последнем часе, закрытом до T
    "rsi1h_min24": "REAL",    # минимум RSI 1h за 24 закрытых часа
    "rsi4h": "REAL",          # RSI(14) 4h на последнем закрытом 4h-баре
    "rsi4h_min7": "REAL", "rsi4h_max7": "REAL",     # за 7 дней (42 бара 4h)
    "rsi4h_min14": "REAL", "rsi4h_max14": "REAL",   # за 14 дней (84 бара 4h)
    "mfi15_pre": "REAL",      # MFI(14) 15m на последней свече до T
    "mfi1h": "REAL",          # MFI(14) 1h на последнем закрытом часе
    "extreme": "INTEGER",     # «крайности»: RSI 15m перед искрой > 70 или минимум RSI 1h за 24 ч < 30
    "natr_pre": "REAL",       # ATR(14)/close × 100 на 15m, последняя свеча до T
    "natr_1d": "REAL",        # средний 15m NATR за 96 свечей до T
    "natr_3d": "REAL",        # … за 288 свечей до T
    "natr_ratio": "REAL",     # NATR перед искрой / средний за 3 дня
    "spark_atr": "REAL",      # true range свечи-искры / ATR перед ней
    "relvol_med40": "REAL",   # объём искры / медиана 40 свечей до неё
}
# Форма свечи-искры — у 15m- и 5m-искр.
CANDLE_COLS = {
    "hi_from_open": "REAL",   # максимум от открытия, %
    "close_from_low": "REAL", # закрытие от минимума, %
    "wick_up": "REAL",        # верхняя тень, % от диапазона свечи
    "wick_dn": "REAL",        # нижняя тень, % от диапазона свечи
}

ADDED_COLUMNS = {
    # Модель выходов v0.2 на 1m (engine/exits.py): результат (% net), причина и минута выхода для A-c,
    # A-c-t и «спокойного» режима; x_n1m — сколько 1m-баров пути было; x_ms — когда посчитано.
    "outcomes": {"x_c_pct": "REAL", "x_c_reason": "TEXT", "x_c_min": "INTEGER",
                 "x_ct_pct": "REAL", "x_ct_reason": "TEXT", "x_ct_min": "INTEGER",
                 "x_calm_pct": "REAL", "x_calm_reason": "TEXT", "x_calm_min": "INTEGER", "x_calm_tp1_min": "INTEGER",
                 "x_n1m": "INTEGER", "x_ms": "INTEGER"},
    "symbols": {"funding_h": "REAL"},
    # big = 1 — «большая свеча»: 5m-алерт без потолка +8% (рост больше +8%), в «первой за 12 ч» не участвует
    "sparks_5m": {**CTX_COLS, **CANDLE_COLS, "ind_ms": "INTEGER", "big": "INTEGER",
                  "first12r": "INTEGER"},     # «первая за 12 ч» по исследованию; first12 — по v0.2
    "sparks_15m": {**CTX_COLS, **CONF_COLS, **IND15_COLS, **CANDLE_COLS,
                   "rsi15_conf": "REAL",      # RSI(14) 15m на свече T+15…T+30
                   "ind_ms": "INTEGER", "ind_conf_ms": "INTEGER",
                   # «Первая за 12 ч» по исследованию (от последней принятой искры). first12_* — по v0.2:
                   # за 12 ч до искры не было никакой искры-кандидата этого потока.
                   "first12r_med": "INTEGER", "first12r_mean": "INTEGER",
                   # правило D = A + «крайности» RSI (по потоку медианы и среднего 20)
                   "rD": "INTEGER", "rD_mean": "INTEGER",
                   # счёт U = A (поток среднего 20) + стакан < $150k + OI на T ≥ $1M + OI ≥ 0.4 × оборот 24 ч
                   "rU": "INTEGER",
                   # Повторная искра (v0.2), по каждому потоку: часы с предыдущего кандидата той же монеты,
                   # open искры к close предыдущей (%), был ли у предыдущей подхват. repeat = gap ≤ 12 ч.
                   "rep_gap_med": "REAL", "rep_vs_med": "REAL", "rep_conf_med": "INTEGER",
                   "rep_gap_mean": "REAL", "rep_vs_mean": "REAL", "rep_conf_mean": "INTEGER",
                   "rep_ms": "INTEGER"},
    # «Вилка» от момента живого тега (этап 9): что раньше от цены в этот момент — +10% или −5%.
    # fork_dir: 1 = +10%, −1 = −5% (оба в одном баре — −5%), 0 = ни то ни другое за 48 ч (тогда fork_pct —
    # результат по close через 48 ч). fork_ms — когда посчитано.
    # v0.4: режим рынка — у искр в момент T (бар, закрывшийся в T), у сделок — в момент входа
    **{tbl: {"reg_t": "INTEGER", "regime": "TEXT", "reg_mkt24": "REAL", "reg_mkt72": "REAL",
             "reg_breadth": "REAL", "reg_btc24": "REAL"} for tbl in ("sparks_5m_reg", "paper_trades_reg",
                                                                    "live_trades_reg", "sparks_15m_reg")},
    # v0.5: сила пампа у первых сигналов эпизода и состояние поздних сигналов (watch / done / none)
    "pump_episodes": {"s1_pump": "REAL", "s2_pump": "REAL", "late1": "TEXT", "late2": "TEXT"},
    # v0.6: признаки шорт-сигнала (ref06.short_signal_features) у каждого сигнала Ш1/Ш2; feat_ms — когда посчитаны
    "short_signals": {"pump_hours": "REAL", "taker_share_1h": "REAL", "taker_share_pump": "REAL", "vol_1h_vs_pump": "REAL", "vol_pump_vs_day_before": "REAL", "oi_pump_pct": "REAL", "oi_1h_pct": "REAL", "oi_from_max_pct": "REAL", "oi_usd": "REAL", "oi_to_turnover": "REAL", "ls_pct_week": "REAL", "ls_pump_pct": "REAL", "ls_1h_pct": "REAL", "top_ls_pos": "REAL", "top_ls_pos_pump_pct": "REAL", "taker_ratio_1h": "REAL", "funding_h": "REAL", "funding_max_pump_h": "REAL", "feat_ms": "INTEGER", "feat_note": "TEXT"},
    # v0.5: сила пампа на сигнале, эпизод, для Ш3 — RSI 4h и его максимум за памп
    "short_trades": {"pump_pct": "REAL", "ep_t_base": "INTEGER", "rsi4h": "REAL", "rsi4h_max": "REAL",
                     # v0.6: доля тейкер-покупок за последний час и длительность пампа на сигнале (фильтры Ш1-c, Ш1-d)
                     "taker_1h": "REAL", "pump_hours": "REAL"},
    "live_tags": {"fork_pct": "REAL", "fork_dir": "INTEGER", "fork_ms": "INTEGER", **{
        # выход v0.2 для ручной сделки от входа T+30: уровень трейлинга по закрытию 5m, p, причина/момент/цена
        # выхода, если он уже был бы (стоп или трейлинг), проверка 6 ч (1 — закрыть, 0 — нет, пусто — ещё не было)
        "tr_level": "REAL", "tr_p": "REAL", "tr_reason": "TEXT", "tr_exit_ms": "INTEGER", "tr_exit_px": "REAL",
        "tr_six": "INTEGER"}},
    "live_latest": {"tr_level": "REAL", "tr_p": "REAL", "tr_reason": "TEXT", "tr_exit_ms": "INTEGER",
                    "tr_exit_px": "REAL", "tr_six": "INTEGER"},
    # Задержка решения бота: закрытие бара T+30, приход этого бара по WS, подхват посчитан, решение,
    # отправка ордера и исполнение (в бумаге — момент решения). Риск позиции в % её собственного баланса.
    "paper_trades": {"bar_close_ms": "INTEGER", "ws_ms": "INTEGER", "conf_calc_ms": "INTEGER",
                     "decision_ms": "INTEGER", "order_ms": "INTEGER", "fill_ms": "INTEGER", "risk_pct": "REAL",
                     # счета v0.2 (трейлинг по закрытию 5m): режим, максимум закрытий, последний бар, проверка 6 ч
                     "mode": "TEXT", "mclose": "REAL", "close_t": "INTEGER", "six_done": "INTEGER",
                     # «спокойный» двойник: когда и по какой цене зафиксирована ⅓, открытая доля, текущий стоп
                     "tp1_ms": "INTEGER", "tp1_px": "REAL", "frac": "REAL", "stop_now": "REAL"},
    "paper_skipped": {"bar_close_ms": "INTEGER", "ws_ms": "INTEGER", "conf_calc_ms": "INTEGER",
                      "decision_ms": "INTEGER"},
}


def ensure_columns(conn: sqlite3.Connection) -> None:
    for key, cols in ADDED_COLUMNS.items():
        table = key.removesuffix("_reg")              # «…_reg» — второй набор колонок той же таблицы (режим v0.4)
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        for c, typ in cols.items():
            if c not in have:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {c} {typ}")


def update_sql(table: str, cols: list[str]) -> str:
    return f"UPDATE {table} SET " + ", ".join(f"{c}=?" for c in cols) + " WHERE symbol=? AND t=?"


SQL_FUNDING_H = "UPDATE symbols SET funding_h=? WHERE symbol=?"


def open_conn(path: Path, timeout: float = 30.0) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=timeout, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    return conn


class Db:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = open_conn(path)
        self.conn.executescript(SCHEMA)
        ensure_columns(self.conn)
        # Очередь записи — список (sql, строки) в порядке поступления: UPDATE не обгонит свой INSERT.
        self.pending: list[tuple[str, list]] = []
        self.rows_written = 0
        self.fails = 0
        self.alock = asyncio.Lock()          # пачки уходят в базу строго по очереди
        self.tlock = threading.Lock()        # одно соединение — один поток (в том числе при остановке)

    # ---------------------------------------------------------------- запись
    def put(self, sql: str, rows: list) -> None:
        if not rows:
            return
        if self.pending and self.pending[-1][0] == sql:
            self.pending[-1][1].extend(rows)
        else:
            self.pending.append((sql, list(rows)))

    def pending_rows(self) -> int:
        return sum(len(r) for _, r in self.pending)

    def _flush_sync(self, batch: list[tuple[str, list]]) -> int:
        with self.tlock:
            cur = self.conn.cursor()
            cur.execute("BEGIN")
            try:
                n = 0
                for sql, rows in batch:
                    cur.executemany(sql, rows)
                    n += len(rows)
                cur.execute("COMMIT")
                return n
            except BaseException:
                if self.conn.in_transaction:
                    cur.execute("ROLLBACK")
                raise

    async def flush(self) -> None:
        async with self.alock:
            if not self.pending:
                return
            batch, self.pending = self.pending, []
            try:
                self.rows_written += await asyncio.to_thread(self._flush_sync, batch)
                self.fails = 0
            except Exception:
                self.fails += 1
                if self.fails <= 5:
                    self.pending = batch + self.pending      # вернуть пачку в начало очереди, повторить
                    log.warning("запись не прошла (попытка %d), пачка возвращена в очередь", self.fails)
                else:
                    log.error("запись не проходит 5 раз подряд, пачка %d строк отброшена",
                              sum(len(r) for _, r in batch))
                    self.fails = 0
                raise

    async def run(self, interval: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.flush()
                except Exception:
                    log.exception("ошибка записи в SQLite")
        finally:
            if self.pending:
                batch, self.pending = self.pending, []
                self._flush_sync(batch)
                log.info("остаток записан при остановке")

    # ---------------------------------------------------------------- чтение
    def _read_sync(self, sql: str, params: tuple) -> list:
        conn = open_conn(self.path)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    async def read(self, sql: str, params: tuple = ()) -> list:
        return await asyncio.to_thread(self._read_sync, sql, params)

    async def run_in_thread(self, fn, *args):
        """fn(conn, *args) на отдельном соединении — для тяжёлых чтений и чистки."""
        def job():
            conn = open_conn(self.path, timeout=120)
            try:
                return fn(conn, *args)
            finally:
                conn.close()
        return await asyncio.to_thread(job)
