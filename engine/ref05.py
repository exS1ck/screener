"""
КОПИЯ research_reference_v05.py ИЗ ИССЛЕДОВАНИЯ. НЕ ПРАВИТЬ (кроме строки импорта v0.4 ниже):
движок и tools/verify_v05.py вызывают эти функции напрямую.

Эталон v0.5: шорт-счета Ш1-b, Ш2-b, Ш2-c, Ш3.
Использует research_reference_v04.py (режим рынка, RSI, эпизоды пампа 12 ч, сделки Ш1 и сетку Ш2).
Договорённости — как в v0.4 (README_v04.md). Дополнительно:
  * RSI 4h — по закрытиям 4h-свечей (5m-бары 47, 95, …); на закрытии 5m-бара k последняя закрытая
    4h-свеча — (k+1)//48 − 1. Сетка 5m должна начинаться в 00:00 UTC, чтобы 4h-свечи шли с 00/04/08/… UTC.
  * «Сила пампа в момент сигнала» = M / base − 1, где M и base — текущие значения эпизода на этом баре.
  * Ш1-b, Ш2-b, Ш2-c берут только ПЕРВЫЙ сигнал своего типа в эпизоде (как Ш1 и Ш2) и входят, если сила пампа
    в этот момент в диапазоне. Ш3 — первый бар эпизода 7 дней, где выполнены все его условия.
"""
import numpy as np
import pandas as pd
from . import ref04 as V4      # ← единственная правка копии: имя модуля v0.4 в движке

FEE_MARKET, FEE_LIMIT, FEE_STOP = V4.FEE_MARKET, V4.FEE_LIMIT, V4.FEE_STOP


def rsi_4h(c5):
    return V4.rsi_wilder(c5[47::48])


# ================================================================ Ш1-b, Ш2-b, Ш2-c: сигналы Ш1 и Ш2 из v0.4 + фильтр силы пампа
S1B_PUMP = (0.35, 0.55)     # Ш1-b: сила пампа в момент сигнала, включительно
S2B_PUMP_MIN = 0.35         # Ш2-b и Ш2-c


def signals_v05(h, l, c, r15, r1, k_start, t_base, regime_lab, mkt24):
    """Счета Ш1-b, Ш2-b, Ш2-c реагируют только на ПЕРВЫЙ сигнал своего типа в эпизоде — тот же, что у Ш1 и Ш2
    (V4.scan_episode), — и входят, только если сила пампа M/base − 1 в этот момент в нужном диапазоне.
    Если первый сигнал эпизода не прошёл фильтр, более поздние сигналы этого эпизода не берутся.
    Возвращает dict с ключами 'S1b' и/или 'S2bc' (снимок сигнала + pump_pct)."""
    sig = V4.scan_episode(h, l, c, r15, r1, k_start, t_base, regime_lab, mkt24)
    out = {}
    if 'S1' in sig:
        g = dict(sig['S1']); p = g['M'] / g['base'] - 1; g['pump_pct'] = p * 100
        if S1B_PUMP[0] <= p <= S1B_PUMP[1]:
            out['S1b'] = g
    if 'S2' in sig:
        g = dict(sig['S2']); p = g['M'] / g['base'] - 1; g['pump_pct'] = p * 100
        if p >= S2B_PUMP_MIN:
            out['S2bc'] = g
    return out


S2C_STOP, S2C_TP, S2C_HZ = 0.30, 0.10, 576


def trade_single_short(h, l, c, k, stop_pct=S2C_STOP, tp_pct=S2C_TP, hz=S2C_HZ):
    """Ш2-c: один шорт по close[k]; стоп вход×1.30, цель вход×0.90, таймаут 48 ч. R = результат / 30."""
    E = c[k]; stop = E * (1 + stop_pct); tgt = E * (1 - tp_pct)
    for j in range(k + 1, min(k + 1 + hz, len(c))):
        if h[j] >= stop:
            p = -stop_pct * 100 - FEE_MARKET - FEE_STOP
            return dict(exit_k=j, exit_price=stop, reason='стоп', pnl_pct=p, R=p / (stop_pct * 100))
        if l[j] <= tgt:
            p = tp_pct * 100 - FEE_MARKET - FEE_LIMIT
            return dict(exit_k=j, exit_price=tgt, reason='цель', pnl_pct=p, R=p / (stop_pct * 100))
    j = min(k + hz, len(c) - 1)
    p = (1 - c[j] / E) * 100 - FEE_MARKET - FEE_STOP
    return dict(exit_k=j, exit_price=c[j], reason='время', pnl_pct=p, R=p / (stop_pct * 100))


# ================================================================ Ш3: большие пампы за 7 дней
S3_LOOK, S3_RISE, S3_SCAN, S3_GAP = 2016, 1.0, 576, 288


def pump_episodes_7d(h, l, in_universe, look=S3_LOOK, min_rise=S3_RISE, scan=S3_SCAN, gap=S3_GAP):
    """Эпизоды больших пампов. На закрытии бара k: base — минимум low за бары k−2015…k (7 дней), t_base — его бар,
    M — максимум high от t_base до k. Эпизод начинается на первом k, где M/base − 1 ≥ 100% и монета во вселенной.
    Сигналы ищутся на барах k_start…k_start + 575 (48 ч). После эпизода: t_peak — бар максимума high на
    [t_base, k_start + 576]; следующий эпизод — только при k > max(k_start, t_peak) + 288 и новом t_base > t_peak.
    Возвращает список (k_start, t_base, t_peak)."""
    n = len(h)
    lo = pd.Series(l).rolling(look, min_periods=look).min().values
    hi = pd.Series(h).rolling(look, min_periods=look).max().values
    cand = np.where((hi / lo - 1 >= min_rise) & in_universe)[0]
    eps = []; last_peak = -1; last_end = -1
    for k in cand:
        if k <= last_end:
            continue
        w = l[k - look + 1:k + 1]
        if not np.isfinite(w).any():
            continue
        tb = k - look + 1 + int(np.nanargmin(w))
        if tb <= last_peak or np.nanmax(h[tb:k + 1]) / l[tb] - 1 < min_rise:
            continue
        end = min(k + scan, n - 1)
        tp = tb + int(np.nanargmax(h[tb:end + 1]))
        eps.append((int(k), int(tb), int(tp)))
        last_peak = tp; last_end = max(k, tp) + gap
    return eps


def scan_episode_s3(h, l, c, r4, k_start, t_base, scan=S3_SCAN):
    """Ш3: первый бар эпизода, где памп ≥ 100%, цена ≥ M × 0.95, max RSI 4h за памп ≥ 70, RSI 4h ≤ max − 10."""
    base = np.nanmin(l[t_base:k_start + 1]); M = np.nanmax(h[t_base:k_start + 1]); i4_first = (t_base + 1) // 48
    for k in range(k_start, min(k_start + scan, len(c) - 1)):
        if k > k_start:
            M = max(M, h[k]); base = min(base, l[k])
        pump = M / base - 1
        if pump < S3_RISE or not np.isfinite(c[k]):
            continue
        i4 = (k + 1) // 48 - 1
        if i4 < i4_first:
            continue
        rnow = r4[i4]; rmax = np.nanmax(r4[i4_first:i4 + 1])
        if 1 - c[k] / M <= 0.05 and rmax >= 70 and rnow <= rmax - 10:
            return dict(k=k, M=M, base=base, pump_pct=pump * 100, close=c[k], rsi4h=rnow, rsi4h_max=rmax)
    return None


# сетка Ш2-b и Ш3 — та же, что Ш2 в v0.4
trade_grid = V4.trade_S2
GRID_MAX_LOSS = V4.S2_MAX_LOSS


# ================================================================ поздние сигналы (виртуальные сделки, без исполнения)
def late_signals_v05(h, l, c, r15, r1, k_start, t_base, regime_lab, mkt24, life=432):
    """Для Ш1-b и Ш2-b/Ш2-c: если ПЕРВЫЙ сигнал своего типа в эпизоде не прошёл фильтр силы пампа, находит первый
    более поздний бар эпизода, где выполнены все условия счёта (включая фильтр). Такой бар — «поздний сигнал»:
    он пишется в журнал как виртуальная сделка и не влияет на баланс и лимиты счёта.
    Возвращает dict: 'S1b' и/или 'S2bc' → снимок позднего сигнала + время и сила пампа первого сигнала."""
    out = {}; first = {}
    base = np.nanmin(l[t_base:k_start + 1]); M = np.nanmax(h[t_base:k_start + 1]); i15_first = (t_base + 1) // 3
    for k in range(k_start, min(t_base + life, len(c) - 1)):
        if k > k_start:
            M = max(M, h[k]); base = min(base, l[k])
        p = M / base - 1
        if not (p >= 0.25) or not np.isfinite(c[k]):
            continue
        i15 = (k + 1) // 3 - 1; i1 = (k + 1) // 12 - 1
        rnow = r15[i15] if i15 >= 0 else np.nan
        rmax = np.nanmax(r15[i15_first:i15 + 1]) if i15 >= i15_first else np.nan
        dd = 1 - c[k] / M
        s1 = dd >= 0.08 and r1[i1] >= 70 and regime_lab[k] not in ('рост', '')
        s2 = dd <= 0.02 and rmax >= 75 and rnow <= rmax - 10 and np.isfinite(mkt24[k]) and mkt24[k] < 0
        snap = dict(k=k, M=M, base=base, pump_pct=p * 100, close=c[k], rsi15=rnow, rsi15_max=rmax, rsi1h=r1[i1],
                    mkt24=mkt24[k], regime=regime_lab[k])
        if s1 and 'S1' not in first:
            first['S1'] = (k, p)
        if s2 and 'S2' not in first:
            first['S2'] = (k, p)
        if 'S1b' not in out and s1 and S1B_PUMP[0] <= p <= S1B_PUMP[1] and first['S1'][0] != k:
            out['S1b'] = dict(snap, first_k=first['S1'][0], first_pump_pct=first['S1'][1] * 100)
        if 'S2bc' not in out and s2 and p >= S2B_PUMP_MIN and first['S2'][0] != k:
            out['S2bc'] = dict(snap, first_k=first['S2'][0], first_pump_pct=first['S2'][1] * 100)
        # если первый сигнал прошёл фильтр — позднего сигнала у этого счёта нет
        if 'S1' in first and first['S1'][0] == k and S1B_PUMP[0] <= p <= S1B_PUMP[1]:
            out.setdefault('S1b', None)
        if 'S2' in first and first['S2'][0] == k and p >= S2B_PUMP_MIN:
            out.setdefault('S2bc', None)
        if len(out) == 2:
            break
    return {key: v for key, v in out.items() if v is not None}
