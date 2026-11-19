"""
КОПИЯ research_reference_v04.py ИЗ ИССЛЕДОВАНИЯ. НЕ ПРАВИТЬ: движок и tools/verify_v04.py вызывают эти функции напрямую.

Эталон v0.4: режим рынка, эпизоды пампа (только по прошлым данным), шорт-счета Ш1 и Ш2.
Дополняет research_reference.py (v0.1) и research_reference_v02.py.

Договорённости (как в v0.1):
  * 5m-бар k: время открытия t0 + 5 мин × k, закрывается в t0 + 5 мин × (k+1). Решение — на закрытии бара k.
  * Все цены — последняя цена сделки (не mark price).
  * 15m-свеча i = 5m-бары 3i, 3i+1, 3i+2 (границы :00/:15/:30/:45). На закрытии 5m-бара k последняя
    ЗАКРЫТАЯ 15m-свеча — (k+1)//3 − 1. Аналогично 1h: (k+1)//12 − 1. Сетка должна начинаться ровно в :00.
  * RSI(14) — Уайлдер (ewm alpha=1/14, adjust=False, min_periods=14) по закрытиям 15m / 1h.
  * Внутри 5m-бара для шорта порядок пессимистичный: сначала всё, что против позиции (доборы, стоп — по high),
    потом цель (по low).
"""
import numpy as np
import pandas as pd

# ---------------------------------------------------------------- издержки (в % от номинала заявки)
FEE_MARKET = 0.05      # рыночный вход
FEE_LIMIT = 0.02       # лимитный добор / лимитная цель
FEE_STOP = 0.10        # стоп или выход по рынку (комиссия + проскальзывание)

BARS_24H, BARS_72H, BARS_12H = 288, 864, 144


# ================================================================ режим рынка
def universe_mask(quote_volume):
    """Вселенная на каждом баре: оборот за 24 ч (сумма 288 баров ДО и включая k) в $0.5–30M."""
    q24 = pd.DataFrame(quote_volume).rolling(BARS_24H, min_periods=250).sum().values / 1e6
    return (q24 >= 0.5) & (q24 <= 30), q24


def market_regime(close, quote_volume, btc_col):
    """Возвращает DataFrame по барам: idx, mkt24, mkt72, breadth24, btc24, regime.
    idx — накопленная сумма медиан 5m-лог-доходностей по монетам вселенной (вселенная — на баре k)."""
    uni, _ = universe_mask(quote_volume)
    with np.errstate(all='ignore'):
        lr = np.log(close[1:] / close[:-1])
    lr = np.vstack([np.full((1, close.shape[1]), np.nan), lr])
    lr_u = np.where(uni, lr, np.nan)
    with np.errstate(all='ignore'):
        med = np.nanmedian(lr_u, axis=1)
    med = np.nan_to_num(med)
    idx = np.cumsum(med)
    n = len(idx)
    mkt24 = np.full(n, np.nan); mkt72 = np.full(n, np.nan); breadth = np.full(n, np.nan); btc24 = np.full(n, np.nan)
    mkt24[BARS_24H:] = (np.exp(idx[BARS_24H:] - idx[:-BARS_24H]) - 1) * 100
    mkt72[BARS_72H:] = (np.exp(idx[BARS_72H:] - idx[:-BARS_72H]) - 1) * 100
    with np.errstate(all='ignore'):
        up = close[BARS_24H:] > close[:-BARS_24H]
        valid = uni[BARS_24H:] & np.isfinite(close[BARS_24H:]) & np.isfinite(close[:-BARS_24H])
        breadth[BARS_24H:] = np.where(valid.sum(1) > 0, (up & valid).sum(1) / np.maximum(valid.sum(1), 1) * 100, np.nan)
        btc24[BARS_24H:] = (close[BARS_24H:, btc_col] / close[:-BARS_24H, btc_col] - 1) * 100
    regime = np.where((mkt24 >= 0) & (mkt72 >= -1), 'рост',
             np.where((mkt24 < -1) | (mkt72 < -2.5), 'падение', 'нейтрально'))
    regime = np.where(np.isfinite(mkt24) & np.isfinite(mkt72), regime, '')
    return pd.DataFrame(dict(idx=idx, mkt24=mkt24, mkt72=mkt72, breadth24=breadth, btc24=btc24, regime=regime))


# ================================================================ RSI
def rsi_wilder(c, p=14):
    d = np.diff(c, prepend=np.nan)
    up = pd.Series(np.clip(d, 0, None)).ewm(alpha=1 / p, adjust=False, min_periods=p).mean().values
    dn = pd.Series(np.clip(-d, 0, None)).ewm(alpha=1 / p, adjust=False, min_periods=p).mean().values
    with np.errstate(all='ignore'):
        return 100 - 100 / (1 + up / dn)


def rsi_15m_1h(c5):
    """RSI по закрытиям 15m (каждый 3-й 5m-бар, индексы 2,5,8…) и 1h (11,23,…)."""
    return rsi_wilder(c5[2::3]), rsi_wilder(c5[11::12])


# ================================================================ эпизоды пампа (только по прошлому)
def pump_episodes(h, l, in_universe, min_rise=0.25, look=BARS_12H, life=432, gap=BARS_24H):
    """Эпизоды пампа одной монеты.
    На закрытии бара k: base = минимум low за последние 12 ч (бары k−143…k), t_base — его бар (первый при равенстве),
    M = максимум high от t_base до k. Памп активен, если M/base − 1 ≥ 25% и монета во вселенной на баре k.
    Эпизод: от t_base до t_base + 432 бара (36 ч). Следующий эпизод — если его t_base ≥ t_base прошлого + 288 (24 ч).
    Возвращает список (k_start, t_base)."""
    n = len(h)
    lo = pd.Series(l).rolling(look, min_periods=look).min().values
    hi = pd.Series(h).rolling(look, min_periods=look).max().values
    cand = np.where((hi / lo - 1 >= min_rise) & in_universe)[0]       # необходимое условие — для скорости
    eps = []; last_base = -10 ** 9
    for k in cand:
        if eps and k <= eps[-1][1] + life:
            continue
        w = l[k - look + 1:k + 1]
        if not np.isfinite(w).any():
            continue
        tb = k - look + 1 + int(np.nanargmin(w)); base = l[tb]
        M = np.nanmax(h[tb:k + 1])
        if M / base - 1 >= min_rise and tb >= last_base + gap:
            eps.append((int(k), int(tb))); last_base = tb
    return eps


# ================================================================ сигналы
def scan_episode(h, l, c, r15, r1, k_start, t_base, regime_lab, mkt24, life=432):
    """Проходит эпизод бар за баром (от k_start до t_base+life) и возвращает первые сигналы Ш1 и Ш2.
    base и M пересчитываются от t_base на каждом баре."""
    out = {}
    base = np.nanmin(l[t_base:k_start + 1]); M = np.nanmax(h[t_base:k_start + 1])
    i15_first = (t_base + 1) // 3
    for k in range(k_start, min(t_base + life, len(c) - 1)):
        if k > k_start:
            M = max(M, h[k]); base = min(base, l[k])
        if not (M / base - 1 >= 0.25) or not np.isfinite(c[k]):
            continue
        i15 = (k + 1) // 3 - 1; i1 = (k + 1) // 12 - 1
        rnow = r15[i15] if i15 >= 0 else np.nan
        rmax = np.nanmax(r15[i15_first:i15 + 1]) if i15 >= i15_first else np.nan
        r1now = r1[i1] if i1 >= 0 else np.nan
        dd = 1 - c[k] / M
        snap = dict(k=k, M=M, base=base, close=c[k], rsi15=rnow, rsi15_max=rmax, rsi1h=r1now,
                    mkt24=mkt24[k], regime=regime_lab[k])
        if 'S1' not in out and dd >= 0.08 and r1now >= 70 and regime_lab[k] not in ('рост', ''):
            out['S1'] = snap
        if 'S2' not in out and dd <= 0.02 and rmax >= 75 and rnow <= rmax - 10 and np.isfinite(mkt24[k]) and mkt24[k] < 0:
            out['S2'] = snap
        if len(out) == 2:
            break
    return out


# ================================================================ сделки
def trade_S1(h, l, c, k, M, max_stop=0.15, hz=BARS_12H):
    """Ш1: шорт по close[k], стоп M×1.01, цель вход×0.95, таймаут 12 ч. Стоп дальше 15% — пропуск (None)."""
    E = c[k]; stop = M * 1.01; tgt = E * 0.95; risk = stop / E - 1
    if risk > max_stop:
        return None
    for j in range(k + 1, min(k + 1 + hz, len(c))):
        if h[j] >= stop:
            pnl = (1 - stop / E) * 100 - FEE_MARKET - FEE_STOP
            return dict(exit_k=j, exit_price=stop, reason='стоп', pnl_pct=pnl, R=pnl / (risk * 100), risk_pct=risk * 100)
        if l[j] <= tgt:
            pnl = 5.0 - FEE_MARKET - FEE_LIMIT
            return dict(exit_k=j, exit_price=tgt, reason='цель', pnl_pct=pnl, R=pnl / (risk * 100), risk_pct=risk * 100)
    j = min(k + hz, len(c) - 1)
    pnl = (1 - c[j] / E) * 100 - FEE_MARKET - FEE_STOP
    return dict(exit_k=j, exit_price=c[j], reason='время', pnl_pct=pnl, R=pnl / (risk * 100), risk_pct=risk * 100)


S2_STEP, S2_ADDS, S2_TP, S2_STOP_AFTER = 0.08, 3, 0.05, 0.05
S2_MAX_LOSS = sum((1 - (1 + S2_STEP * S2_ADDS + S2_STOP_AFTER) / (1 + S2_STEP * i)) * 100 for i in range(S2_ADDS + 1)) \
    - FEE_MARKET - FEE_LIMIT * S2_ADDS - FEE_STOP * (S2_ADDS + 1)          # ≈ −64.2% от размера одной заявки


def trade_S2(h, l, c, k, hz=BARS_24H):
    """Ш2: сетка. Вход 1 заявкой по close[k] (E0); доборы E0×1.08/1.16/1.24; цель — средняя×0.95;
    финальный стоп E0×1.29; таймаут 24 ч. Результат — в % от размера ОДНОЙ заявки; R = результат / |S2_MAX_LOSS|."""
    E0 = c[k]; fills = [(E0, k)]; levels = [E0 * (1 + S2_STEP * (i + 1)) for i in range(S2_ADDS)]
    stop = E0 * (1 + S2_STEP * S2_ADDS + S2_STOP_AFTER)

    def pnl(px, reason):
        exit_fee = FEE_LIMIT if reason == 'цель' else FEE_STOP
        return sum((1 - px / f) * 100 for f, _ in fills) - FEE_MARKET - FEE_LIMIT * (len(fills) - 1) - exit_fee * len(fills)

    for j in range(k + 1, min(k + 1 + hz, len(c))):
        for lv in levels[len(fills) - 1:]:
            if h[j] >= lv:
                fills.append((lv, j))
        if h[j] >= stop:
            p = pnl(stop, 'стоп'); return dict(exit_k=j, exit_price=stop, reason='стоп', fills=fills, pnl_unit_pct=p, R=p / abs(S2_MAX_LOSS))
        avg = len(fills) / sum(1 / f for f, _ in fills)
        if l[j] <= avg * (1 - S2_TP):
            px = avg * (1 - S2_TP); p = pnl(px, 'цель')
            return dict(exit_k=j, exit_price=px, reason='цель', fills=fills, pnl_unit_pct=p, R=p / abs(S2_MAX_LOSS))
    j = min(k + hz, len(c) - 1); p = pnl(c[j], 'время')
    return dict(exit_k=j, exit_price=c[j], reason='время', fills=fills, pnl_unit_pct=p, R=p / abs(S2_MAX_LOSS))
