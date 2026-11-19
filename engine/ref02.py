"""
КОПИЯ research_reference_v02.py ИЗ ИССЛЕДОВАНИЯ. НЕ ПРАВИТЬ: движок и tools/verify_exits.py вызывают
эти функции напрямую, чтобы выходы считались один в один с эталоном.

Эталон v0.2: выходы и тег «повторная искра».
Дополняет research_reference.py (v0.1). Все цены — последняя цена сделки (не mark price).

Входные данные выхода — минутные бары ПОСЛЕ входа:
    h, l, c : массивы high/low/close 1m-баров, начиная с минуты сразу после входа
              (вход = close 3-го 5m-бара подхвата, T+30; первая минута пути — T+30..T+31)
    E       : цена входа
    S       : цена жёсткого стопа = L_искры - 0.002 * E
Минута k закрывается в момент entry + (k+1) минут. 5m-бар закрывается на минутах k, где (k+1) % 5 == 0
(вход стоит ровно на границе 5m-бара, поэтому сетки совпадают).

Порядок внутри минуты (режим «пессимистичный», как в проверке):
    1) обновить максимум цены M_high по high этой минуты;
    2) частичная фиксация (если есть) по high;
    3) стоп/безубыток по low;
    4) на закрытии 5m-бара — трейлинг по закрытию;
    5) проверка на 6-м часу — на закрытии 360-й минуты.
В живом режиме по тикам порядок естественный; этот порядок нужен для воспроизведения эталона на 1m-барах.
"""
import numpy as np

COST_PCT = 0.2      # комиссии+проскальзывание на круг, в %, как в исследовании


def stop_price(E, L_spark):
    return L_spark - 0.002 * E


def exit_close_trailing(h, l, c, E, S, p1=0.08, p2=0.05, switch=0.15, hold_min=2880,
                        time_check_min=None, time_check_gain=0.015,
                        tp1=None, tp1_frac=1/3, be_gain=0.002):
    """Трейлинг по закрытию 5m-свечи (+ опции: проверка по времени, «спокойный» режим).
    Возвращает dict: pnl_pct (net), exit_k (минута выхода), exit_price, reason, tp1_k.

    Трейлинг: M_close — максимум закрытий 5m-баров с момента входа. На ПЕРВОМ закрытии 5m-бара
    после входа только записываем M_close (проверки нет). Далее на каждом закрытии 5m-бара:
        p = p2, если M_high >= E*(1+switch), иначе p1   (M_high — максимум high с входа, включая текущую минуту)
        если close <= M_close*(1-p) -> выход по close этого бара
        затем M_close = max(M_close, close)
    Жёсткий стоп: low <= S -> выход по S. После частичной фиксации стоп = max(S, E*(1+be_gain)).
    """
    n = min(len(h), hold_min)
    pos, real, M_high, M_close, tp1_k = 1.0, 0.0, E, None, None
    stop = S
    for k in range(n):
        if not np.isfinite(h[k]):
            continue
        M_high = max(M_high, h[k])
        if tp1 is not None and tp1_k is None and h[k] >= E * (1 + tp1):           # частичная фиксация
            real += tp1_frac * tp1; pos -= tp1_frac; tp1_k = k
            stop = max(stop, E * (1 + be_gain))                                    # безубыток
        if l[k] <= stop:                                                           # жёсткий стоп / безубыток
            real += pos * (stop / E - 1)
            return dict(pnl_pct=real * 100 - COST_PCT, exit_k=k, exit_price=stop,
                        reason='безубыток' if stop > S else 'стоп', tp1_k=tp1_k)
        if (k + 1) % 5 == 0:                                                       # закрытие 5m-бара
            p = p2 if M_high >= E * (1 + switch) else p1
            if M_close is not None and c[k] <= M_close * (1 - p):
                real += pos * (c[k] / E - 1)
                return dict(pnl_pct=real * 100 - COST_PCT, exit_k=k, exit_price=c[k], reason='трейлинг', tp1_k=tp1_k)
            M_close = c[k] if M_close is None else max(M_close, c[k])
        if time_check_min is not None and k == time_check_min - 1 and c[k] >= E * (1 + time_check_gain):
            real += pos * (c[k] / E - 1)                                           # проверка на 6-м часу
            return dict(pnl_pct=real * 100 - COST_PCT, exit_k=k, exit_price=c[k], reason='6 часов', tp1_k=tp1_k)
    k = n - 1
    while k > 0 and not np.isfinite(c[k]):
        k -= 1
    real += pos * (c[k] / E - 1)
    return dict(pnl_pct=real * 100 - COST_PCT, exit_k=k, exit_price=c[k], reason='таймаут', tp1_k=tp1_k)


def exit_A_c(h, l, c, E, S):
    return exit_close_trailing(h, l, c, E, S)


def exit_A_c_t(h, l, c, E, S):
    return exit_close_trailing(h, l, c, E, S, time_check_min=360)


def exit_calm(h, l, c, E, S):
    return exit_close_trailing(h, l, c, E, S, tp1=0.06)


def repeat_tag(spark_opens_15m_idx, i, o15, c15):
    """Тег «повторная искра» для 15m-искры с индексом i.
    spark_opens_15m_idx — отсортированные индексы 15m-свечей ВСЕХ кандидатов-искр монеты того же потока
    (рост ≥ 2% без потолка, объём, оборот). Предыдущая = ближайший кандидат раньше i.
    """
    prev = [x for x in spark_opens_15m_idx if x < i]
    if not prev:
        return dict(repeat=False)
    pi = prev[-1]
    gap_h = (i - pi) / 4
    return dict(repeat=gap_h <= 12, gap_h=gap_h, window_4_8=4 <= gap_h < 8,
                vs_prev_close=(o15[i] / c15[pi] - 1) * 100)
