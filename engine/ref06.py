"""
КОПИЯ research_reference_v06.py ИЗ ИССЛЕДОВАНИЯ. НЕ ПРАВИТЬ (кроме строки импорта v0.4 ниже).

Эталон v0.6: признаки шорт-сигналов (пишутся в журнал) и счета Ш1-c, Ш1-d.
Использует research_reference_v04.py (эпизоды 12 ч, сигнал и сделка Ш1).

Договорённости — как в v0.4 (README_v04.md). Дополнительно:
  * k — 5m-бар сигнала (решение на его закрытии), tb — t_base эпизода (бар минимума).
  * «Последний час» — 12 баров k−11…k включительно.
  * Метрики фьючерсов (OI, L/S, taker ratio) — 5-минутные; значение «на баре j» — значение на КОНЕЦ бара j
    (архив Binance: create_time на 5 мин раньше API, индекс j = (create_time − t0) / 5 мин).
    В живом режиме берётся последнее доступное значение на момент решения.
  * funding — последняя известная ставка на баре k, делённая на интервал в часах, в % в час.
  * Ш1-c и Ш1-d реагируют только на ПЕРВЫЙ сигнал Ш1 в эпизоде (как Ш1-b): если фильтр не пройден,
    более поздние сигналы эпизода не берутся.
"""
import numpy as np
from . import ref04 as V4      # ← единственная правка копии: имя модуля v0.4 в движке

TAKER_MIN = 0.48          # Ш1-c и Ш1-d: доля тейкер-покупок за последний час
PUMP_MAX_HOURS = 12.0     # Ш1-d: длительность пампа от t_base до сигнала


def _pct(a, b):
    return (a / b - 1) * 100 if np.isfinite(a) and np.isfinite(b) and b > 0 else np.nan


def short_signal_features(k, tb, q, tbq, oi=None, oiv=None, ga=None, tp=None, tk=None, q24_musd=np.nan,
                          funding_idx=None, funding_rate_h=None):
    """Признаки шорт-сигнала. q, tbq — 5m оборот и оборот тейкер-покупок монеты; oi, oiv, ga, tp, tk — 5m-метрики
    (открытый интерес в контрактах и в $, L/S всех счетов, L/S топ-трейдеров по позициям, taker buy/sell ratio);
    funding_idx / funding_rate_h — бары начислений и ставки в %/ч."""
    h1 = slice(k - 11, k + 1); pump = slice(tb, k + 1)
    f = dict(
        pump_hours=max(k - tb, 1) * 5 / 60,
        taker_share_1h=np.nansum(tbq[h1]) / np.nansum(q[h1]) if np.nansum(q[h1]) > 0 else np.nan,
        taker_share_pump=np.nansum(tbq[pump]) / np.nansum(q[pump]) if np.nansum(q[pump]) > 0 else np.nan,
        vol_1h_vs_pump=(np.nansum(q[h1]) / 12) / np.nanmean(q[pump]) if np.nanmean(q[pump]) > 0 else np.nan,
        vol_pump_vs_day_before=np.nanmean(q[pump]) / np.nanmean(q[max(0, tb - 288):tb]) if tb > 0 and np.nanmean(q[max(0, tb - 288):tb]) > 0 else np.nan,
    )
    if oi is not None:
        f.update(oi_pump_pct=_pct(oi[k], oi[tb]), oi_1h_pct=_pct(oi[k], oi[k - 12]),
                 oi_from_max_pct=_pct(oi[k], np.nanmax(oi[pump])),
                 oi_usd=oiv[k], oi_to_turnover=oiv[k] / (q24_musd * 1e6) if q24_musd and q24_musd > 0 else np.nan)
    if ga is not None:
        hist = ga[max(0, k - 2016):k]; hist = hist[np.isfinite(hist)]
        f.update(ls_pct_week=(hist < ga[k]).mean() * 100 if len(hist) > 100 and np.isfinite(ga[k]) else np.nan,
                 ls_pump_pct=_pct(ga[k], ga[tb]), ls_1h_pct=_pct(ga[k], ga[k - 12]))
    if tp is not None:
        f.update(top_ls_pos=tp[k], top_ls_pos_pump_pct=_pct(tp[k], tp[tb]))
    if tk is not None:
        f.update(taker_ratio_1h=np.nanmean(tk[h1]))
    if funding_idx is not None and len(funding_idx):
        j = np.searchsorted(funding_idx, k, side='right') - 1
        f['funding_h'] = funding_rate_h[j] if j >= 0 else np.nan
        sel = (funding_idx >= tb) & (funding_idx <= k)
        f['funding_max_pump_h'] = np.nanmax(funding_rate_h[sel]) if sel.any() else f['funding_h']
    return f


def accounts_v06(feat):
    """По признакам первого сигнала Ш1 эпизода решает, входят ли Ш1-c и Ш1-d."""
    t_ok = np.isfinite(feat['taker_share_1h']) and feat['taker_share_1h'] >= TAKER_MIN
    return dict(S1c=t_ok, S1d=t_ok and feat['pump_hours'] <= PUMP_MAX_HOURS)
