"""
КОПИЯ research_reference.py ИЗ ИССЛЕДОВАНИЯ. НЕ ПРАВИТЬ: движок вызывает эти функции напрямую,
чтобы расчёты совпадали с исследованием один в один. Пороги TH движок берёт из config.toml [thresholds].

Эталонные определения из исследования (август–сентябрь 2026) для скринера v0.1.

Это не код скринера, а образец: функции воспроизводят расчёты исследования
один в один (проверено скриптом verify_reference.py на тех же данных).
Порт в движок должен давать те же числа на тех же входных данных.

=====================================================================
СОГЛАШЕНИЯ ОБ ИНДЕКСАХ (главное, что легко сломать)
=====================================================================
1. Сетка 5m. Бар j открывается в t0 + 5*j минут (UTC), t0 — полночь.
   Все массивы свечей одной монеты — 1D numpy длины n по этой сетке:
       o, h, l, c        — цены бара
       qv                — quote volume бара (USDT)
       tbq               — taker buy quote volume бара (USDT)
   Пропущенный бар = NaN. Используются только ЗАКРЫТЫЕ бары.

2. Метрики (OI, L/S) на той же сетке, но значение oi[k] / ls[k] — это
   состояние НА КОНЕЦ бара k (= на момент открытия бара k+1).
   Значит, «значение в момент T = открытие бара j» — это oi[j-1].
   API openInterestHist / globalLongShortAccountRatio: точка с меткой X
   относится к индексу k = (X - t0)/5мин - 1.
   Архив data.binance.vision/metrics: create_time на 5 минут раньше
   метки API -> индекс k = (create_time - t0)/5мин.
   Живой OI из /fapi/v1/openInterest, снятый сразу после закрытия бара k,
   кладётся в oi[k].

3. 15m-свеча i состоит из 5m-баров 3i, 3i+1, 3i+2 (границы :00/:15/:30/:45).
   T (время искры) = открытие 15m-свечи = открытие 5m-бара j = 3*i.

4. Срезы — как в Python: a[x:y] включает x, НЕ включает y.
   «Окно от T-24ч до T-6ч» для 5m = qv[j-288 : j-72] (216 баров).

5. Проценты: ret = (x/y - 1)*100, кроме pre6h_log (натуральный лог * 100).
"""
import numpy as np

# ---------------------------------------------------------------- пороги
TH = dict(
    spark15_ret_min=2.0, spark15_ret_max=8.0, spark15_relvol=4.0,
    spark15_gap_bars=48,              # 12 ч в 15m-свечах
    liq_min_musd=0.5, liq_max_musd=30.0,
    conf_vx=5.0, conf_tbr=0.45, conf_dd=-1.5, conf_r=1.0,
    ls_low=20.0, thin_ask2=50_000.0, creep_log=3.0, oi30_up=2.0,
    morning=(6, 12),                  # часы МСК, [6, 12)
    spark5_ret_min=2.0, spark5_ret_max=8.0, spark5_vx=5.0, spark5_gap_bars=144,
    pump_mult=1.25, pump_bars=144,    # +25% за 12 ч от открытия искры
    trail=0.08, stop_pad=0.002, hold_bars=576, cost_pct=0.2,
)


# ================================================================ helpers
def _nanmean_min(x, min_n):
    v = x[np.isfinite(x)]
    return v.mean() if len(v) >= min_n else np.nan


def _nanmedian_min(x, min_n):
    v = x[np.isfinite(x)]
    return np.median(v) if len(v) >= min_n else np.nan


def _nansum_min(x, min_n):
    v = x[np.isfinite(x)]
    return v.sum() if len(v) >= min_n else np.nan


# ================================================================ 15m из 5m
def agg15(o, h, l, c, qv, tbq):
    """15m-свечи из 5m. Возвращает dict массивов длины n//3.
    q15 = NaN, если close 15m-свечи неизвестен (как в исследовании)."""
    n15 = len(c) // 3
    r = lambda a: a[:n15 * 3].reshape(n15, 3)
    with np.errstate(all="ignore"):
        o15 = r(o)[:, 0]
        c15 = r(c)[:, 2]
        h15 = np.nanmax(r(h), 1)
        l15 = np.nanmin(r(l), 1)
        q15 = np.nansum(r(qv), 1)
        t15 = np.nansum(r(tbq), 1)
    q15 = np.where(np.isfinite(c15), q15, np.nan)
    return dict(o=o15, h=h15, l=l15, c=c15, q=q15, tb=t15)


# ================================================================ 15m-искра
def q24_15m(q15, i):
    """Оборот за 24 ч до свечи i (без неё): сумма q15[i-96:i], нужно ≥ 90 значений. В $M."""
    s = _nansum_min(q15[max(0, i - 96):i], 90)
    return s / 1e6 if np.isfinite(s) else np.nan


def relvol_mean20(q15, i):
    """Объём свечи i к среднему 20 ПРЕДЫДУЩИХ свечей (все 20 должны быть)."""
    w = q15[i - 20:i] if i >= 20 else np.array([])
    if len(w) < 20 or not np.all(np.isfinite(w)):
        return np.nan
    m = w.mean()
    return q15[i] / m if m > 0 else np.nan


def relvol_median24(q15, i):
    """Вариант v0.1: объём свечи i к медиане q15[i-96 : i-4] (24 ч без последнего часа), ≥ 70 значений."""
    m = _nanmedian_min(q15[max(0, i - 96):max(0, i - 4)], 70)
    return q15[i] / m if np.isfinite(m) and m > 0 else np.nan


def spark15_candidates(b15, base="mean20"):
    """Индексы i, где выполнено условие искры БЕЗ потолка роста (потолок 8% —
    после отбора «первая за 12 ч»: так было в исследовании, крупная свеча
    >8% тоже блокирует следующие 12 ч)."""
    with np.errstate(all="ignore"):
        ret = (b15["c"] / b15["o"] - 1) * 100
    out = []
    for i in np.where(ret >= TH["spark15_ret_min"])[0]:
        rv = relvol_mean20(b15["q"], i) if base == "mean20" else relvol_median24(b15["q"], i)
        if not (np.isfinite(rv) and rv >= TH["spark15_relvol"]):
            continue
        q24 = q24_15m(b15["q"], i)
        if not (np.isfinite(q24) and TH["liq_min_musd"] <= q24 <= TH["liq_max_musd"]):
            continue
        out.append(int(i))
    return out


def first_in_window(idx_list, gap):
    """Оставить искру, если с последней ПРИНЯТОЙ прошло ≥ gap баров.
    Отброшенные искры паузу не продлевают."""
    acc, last = [], -10 ** 9
    for i in idx_list:
        if i - last >= gap:
            acc.append(i)
            last = i
    return acc


def spark15_list(b15, base="mean20"):
    """Итоговые 15m-искры правила бота: кандидаты → первая за 12 ч → потолок +8%."""
    acc = first_in_window(spark15_candidates(b15, base), TH["spark15_gap_bars"])
    return [i for i in acc if (b15["c"][i] / b15["o"][i] - 1) * 100 <= TH["spark15_ret_max"]]


# ================================================================ подхват
def confirmation(i, b15, l, c, qv, tbq):
    """Подхват по трём 5m-барам j+3..j+5 (T+15 … T+30). j = 3*i."""
    j = 3 * i
    sc = b15["c"][i]
    base5 = np.nanmean(qv[max(0, j - 288):j - 72])          # T-24ч … T-6ч, без минимума значений
    if not np.isfinite(base5) or base5 <= 0 or not np.isfinite(sc):
        return None
    w = slice(j + 3, j + 6)
    q3 = np.nansum(qv[w])
    r = dict(
        base5=base5,
        vx3=q3 / 3 / base5,
        tbr3=np.nansum(tbq[w]) / q3 if q3 > 0 else np.nan,
        dd3=(np.nanmin(l[w]) / sc - 1) * 100,
        r3=(c[j + 5] / sc - 1) * 100,
        entry=c[j + 5],                                        # цена входа = close бара j+5 (T+30)
    )
    r["conf"] = bool(r["vx3"] >= TH["conf_vx"] and r["tbr3"] >= TH["conf_tbr"]
                     and r["dd3"] > TH["conf_dd"] and r["r3"] > TH["conf_r"])
    return r


# ================================================================ контекст
def ls_percentile_at(ls, k, lookback=2016, min_n=100):
    """Процентиль ls[k] среди ls[k-lookback : k] (строго меньше). 2016 = 7 дней по 5 мин."""
    v = ls[k]
    hist = ls[max(0, k - lookback):k]
    hist = hist[np.isfinite(hist)]
    if len(hist) <= min_n or not np.isfinite(v):
        return np.nan
    return float((hist < v).mean() * 100)


def g_lvl_spark(ls, j):
    """L/S к неделе для искры: значение в момент T = ls[j-1]."""
    return ls_percentile_at(ls, j - 1)


def oi_30(oi, j):
    """OI с момента T до T+30: oi[j+5] / oi[j-1] - 1, в %."""
    return (oi[j + 5] / oi[j - 1] - 1) * 100


def pre6h_log(o, c, j):
    """Рост за 6 ч до искры, ЛОГ-процент: ln(close бара j-1 / open бара j-72) * 100. Порог > 3."""
    return np.log(c[j - 1] / o[j - 72]) * 100


def ask2_median(snapshots_ask2):
    """Тонкость: медиана суммы офферов в пределах +2% от mid по снимкам
    в окне [T-24ч, T-6ч] ВКЛЮЧИТЕЛЬНО. В исследовании — снимки архива
    bookDepth каждые 30 с (колонка notional при percentage=2)."""
    v = np.asarray(snapshots_ask2, float)
    v = v[np.isfinite(v)]
    return float(np.median(v)) if len(v) else np.nan


def hour_msk(T_utc_hour):
    return (T_utc_hour + 3) % 24


def score5(conf, g_lvl, pre6_log, oi30, T_utc_hour):
    """Балл для ручной торговли: 5 признаков по 1."""
    h = hour_msk(T_utc_hour)
    return int(bool(conf)) + int(g_lvl < TH["ls_low"]) + int(pre6_log > TH["creep_log"]) \
        + int(oi30 >= TH["oi30_up"]) + int(TH["morning"][0] <= h < TH["morning"][1])


def rules_abc(conf, g_lvl, ask2, pre6_log):
    A = bool(conf) and g_lvl < TH["ls_low"]
    return dict(A=A, B=A and ask2 < TH["thin_ask2"], C=A and pre6_log > TH["creep_log"])


# ================================================================ исходы
def pump_label(h, o15_open, j):
    """Памп: максимум high за 144 5m-бара от ОТКРЫТИЯ искры (j … j+143, включая бары искры) ≥ +25%."""
    return bool(np.nanmax(h[j:j + TH["pump_bars"]]) / o15_open >= TH["pump_mult"])


def trade_trailing(i, b15, h, l, c, trail=None, hold=None, cost=None):
    """Бумажная сделка, как в исследовании.
    Вход E = c[j+5]. Стоп (доля) = -( (1 - L_искры/E) + 0.002 ),
    т.е. цена стопа = L_искры - 0.002*E. Трейлинг: уровень = max(стоп, (1+mx)*(1-trail)-1),
    где mx — максимум high с входа по ПРЕДЫДУЩИМ барам (сначала проверка, потом обновление mx).
    Пересечение low ≤ уровня → выход ровно по уровню. Иначе выход по close последнего бара
    окна hold (576 баров = 48 ч). Результат в % минус cost (0.2 в исследовании)."""
    trail = TH["trail"] if trail is None else trail
    hold = TH["hold_bars"] if hold is None else hold
    cost = TH["cost_pct"] if cost is None else cost
    j = 3 * i
    e = j + 5
    E = c[e]
    risk = (1 - b15["l"][i] / E) + TH["stop_pad"]
    hh = h[e + 1:e + 1 + hold] / E - 1
    ll = l[e + 1:e + 1 + hold] / E - 1
    cc = c[e + 1:e + 1 + hold] / E - 1
    stop, mx = -risk, 0.0
    for k in range(min(hold, len(hh))):
        st = max(stop, (1 + mx) * (1 - trail) - 1)
        if ll[k] <= st:
            return st * 100 - cost, k, ("стоп" if st <= stop + 1e-12 else "трейл")
        mx = max(mx, hh[k])
    k = min(hold, len(hh)) - 1
    return cc[k] * 100 - cost, k, "время"


# ================================================================ стадии (живые флаги)
def stage_flags(t, j, o15_open, o, c, qv, oi, ls):
    """Флаги на закрытии 5m-бара t (j+5 ≤ t). j — открытие искры."""
    gain = (c[t] / o15_open - 1) * 100
    ls_from_min = (ls[t] / np.nanmin(ls[j - 1:t + 1]) - 1) * 100
    oi_from_max = (oi[t] / np.nanmax(oi[j - 1:t + 1]) - 1) * 100
    vol_vs_peak = np.nanmean(qv[t - 2:t + 1]) / np.nanmax(qv[j:t + 1])
    r15 = (c[t] / c[t - 3] - 1) * 100
    f = dict(ext=gain > 10, ls=ls_from_min > 5, oi=oi_from_max < -4, vol=vol_vs_peak > 0.5, chase=r15 > 2)
    f["n_red"] = sum(f.values())
    # Метки стадий — как их мерили в исследовании: «середина/поздняя/выдыхание» только при уходе > +10%.
    if f["n_red"] == 0:
        stage = "ранняя"
    elif gain > 10 and ls_from_min > 5 and oi_from_max < -4:
        stage = "выдыхание"
    elif gain > 10 and ls_from_min > 2:
        stage = "поздняя"
    elif gain > 10 and oi_from_max > -1:
        stage = "середина"
    else:
        stage = "не классифицировано"
    f.update(gain=gain, ls_from_min=ls_from_min, oi_from_max=oi_from_max, vol_vs_peak=vol_vs_peak, r15=r15, stage=stage)
    return f


# ================================================================ 5m-искра (ранний сигнал)
def liq5(qv, j):
    """Оборот за 24 ч до бара j (без него): сумма qv[j-288:j], ≥ 250 значений. В $M."""
    s = _nansum_min(qv[max(0, j - 288):j], 250)
    return s / 1e6 if np.isfinite(s) else np.nan


def spark5_at(j, o, c, qv):
    ret = (c[j] / o[j] - 1) * 100
    base = _nanmedian_min(qv[max(0, j - 288):max(0, j - 12)], 200)   # T-24ч … T-1ч
    q24 = liq5(qv, j)
    ok = (TH["spark5_ret_min"] <= ret <= TH["spark5_ret_max"] and np.isfinite(base) and base > 0
          and qv[j] >= TH["spark5_vx"] * base and np.isfinite(q24)
          and TH["liq_min_musd"] <= q24 <= TH["liq_max_musd"])
    return ok, dict(ret=ret, base=base, vx=qv[j] / base if base else np.nan, q24=q24)
# Для 5m-искр потолок 8% применяется ДО отбора «первая за 12 ч» (gap=144 бара),
# в отличие от 15m. В v0.1 пишутся все, флаг «первая за 12 ч» — через first_in_window.


# ================================================================ «ползут сейчас»
def creep_episodes(c, qv, oi, ls, th_in=5.0, th_out=1.0, cooldown=24, t_start=0, t_end=None):
    """Вход в список: (c[t]/c[t-72]-1)*100 ≥ th_in и ликвидность в коридоре и с последнего
    выхода прошло ≥ 24 бара (2 ч). Выход: рост за 6 ч < th_out или нет данных.
    Признаки на баре входа t: L/S-процентиль по ls[t] среди ls[t-2016:t], OI за 6 ч = oi[t]/oi[t-72]-1.
    Исход в исследовании: max(h[t+1:t+289])/c[t]-1 ≥ 0.25."""
    n = len(c)
    pre6 = np.full(n, np.nan)
    pre6[72:] = (c[72:] / c[:-72] - 1) * 100
    liq = liq5_series(qv)
    ev, st, last_exit = [], False, -10 ** 9
    for t in range(t_start, n if t_end is None else t_end):
        v = pre6[t]
        if not st:
            q24 = liq[t]
            if (np.isfinite(v) and v >= th_in and np.isfinite(q24)
                    and TH["liq_min_musd"] <= q24 <= TH["liq_max_musd"] and t - last_exit >= cooldown):
                st = True
                ev.append(dict(t=t, pre6=v, g_lvl=ls_percentile_at(ls, t),
                               oi6=(oi[t] / oi[t - 72] - 1) * 100 if t >= 72 else np.nan))
        else:
            if not np.isfinite(v) or v < th_out:
                st, last_exit = False, t
    return ev


def liq5_series(qv):
    """То же, что liq5(qv, j), для всех j сразу."""
    n = len(qv)
    x = np.where(np.isfinite(qv), qv, 0.0)
    cnt = np.isfinite(qv).astype(float)
    cs = np.r_[0, np.cumsum(x)]; cc = np.r_[0, np.cumsum(cnt)]
    out = np.full(n, np.nan)
    for j in range(n):
        a = max(0, j - 288)
        if cc[j] - cc[a] >= 250:
            out[j] = (cs[j] - cs[a]) / 1e6
    return out


# ================================================================ WVF и «капитуляция 4h»
def agg_tf(o, h, l, c, m):
    """Бары таймфрейма m×5мин на сетке от t0 (4h: m=48, границы 00/04/08… UTC)."""
    k = len(c) // m
    r = lambda a: a[:k * m].reshape(k, m)
    with np.errstate(all="ignore"):
        return dict(o=r(o)[:, 0], h=np.nanmax(r(h), 1), l=np.nanmin(r(l), 1), c=r(c)[:, -1])


def wvf(c, l, pd_=22, bbl=20, mult=2.0, lb=50, ph=0.85):
    """Williams Vix Fix (настройки CM). Все окна ВКЛЮЧАЮТ текущий бар.
    green = ((w ≥ SMA20+2σ20 и σ20>0) или (w ≥ 0.85·max50(w) и max50>0)) и w>0; σ — ddof=0."""
    n = len(c)
    w = np.full(n, np.nan); up = np.full(n, np.nan); rh = np.full(n, np.nan); sd = np.full(n, np.nan)
    for i in range(n):
        win = c[i - pd_ + 1:i + 1] if i >= pd_ - 1 else np.array([])
        if len(win) == pd_ and np.all(np.isfinite(win)) and np.isfinite(l[i]):
            hh = win.max()
            w[i] = (hh - l[i]) / hh * 100
    for i in range(n):
        if i >= bbl - 1:
            x = w[i - bbl + 1:i + 1]
            if np.all(np.isfinite(x)):
                sd[i] = x.std(ddof=0); up[i] = x.mean() + mult * sd[i]
        if i >= lb - 1:
            x = w[i - lb + 1:i + 1]
            if np.all(np.isfinite(x)):
                rh[i] = x.max() * ph
    with np.errstate(invalid="ignore"):
        green = (((w >= up) & (sd > 1e-9)) | ((w >= rh) & (rh > 1e-9))) & (w > 0)
    green[~np.isfinite(w)] = False
    return w, green, up, rh


def capitulation_event(i, g4, c4, ls, mk, m=48):
    """Событие на закрытом 4h-баре i: бары i-1 и i-2 зелёные, бар i — нет.
    Начало зоны k — первый бар этой зелёной серии.
    L/S берётся на ОТКРЫТИИ бара i: значение ls[m*i-1], история ls[m*i-1-2016 : m*i-1].
    rel = (ln(c4[i]/c4[k-1]) - (mk[i]-mk[k-1]))*100, где mk — накопленная медиана лог-доходностей
    всех монет по 4h-барам (индекс рынка). Сигнал: L/S < 20; доп. тег rel < -5."""
    if not (g4[i - 1] and g4[i - 2] and not g4[i]):
        return None
    k = i - 1
    while k > 0 and g4[k - 1]:
        k -= 1
    b = m * i - 1
    return dict(zone_start=k, ls_lvl=ls_percentile_at(ls, b),
                rel=(np.log(c4[i] / c4[k - 1]) - (mk[i] - mk[k - 1])) * 100)


def market_index(c4_matrix):
    """mk[t] = Σ медиана по монетам ln(c4[t]/c4[t-1]); mk[0]=0. c4_matrix: [бары, монеты]."""
    lr = np.log(c4_matrix[1:] / c4_matrix[:-1])
    with np.errstate(all="ignore"):
        med = np.nanmedian(lr, axis=1)
    return np.r_[0, np.cumsum(np.nan_to_num(med))]
