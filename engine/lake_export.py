"""Раз в сутки — законченные дни из базы движка в единую базу истории (lake/live/<набор>/<ГГГГ-ММ-ДД>.parquet).

Так история не обрывается, когда движок через 60 дней удаляет старые строки у себя: архив Binance (месячные файлы),
дальше дневные файлы движка, дальше сама база движка — для запросов это одна таблица (engine/lake.open_lake).
Наборы: klines_5m, klines_1m (только окна сделок), metrics_5m (OI, L/S всех счетов и топ-трейдеров; taker ratio
движок не собирает), funding (ставка на момент начисления — последний снимок перед ним).
"""
import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from .clock import now_ms

log = logging.getLogger("lake")
DAY = 86_400_000


class LakeExporter:
    def __init__(self, cfg: dict, db):
        self.cfg = cfg
        self.db = db
        try:
            import pyarrow  # noqa: F401
            from .lake import lake_dir
            self.root = lake_dir(cfg) / "live"
            self.enabled = bool(cfg.get("lake", {}).get("export", True))
        except ImportError:
            self.enabled = False
            log.warning("единая база: нет pyarrow (pip install -r requirements.txt) — выгрузка дней выключена")

    def _day_job(self, conn, d0: int) -> dict:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from .lake import SCHEMAS
        d1 = d0 + DAY
        name = datetime.fromtimestamp(d0 / 1000, timezone.utc).strftime("%Y-%m-%d")
        out = {}
        q = {
            "klines_5m": ("SELECT symbol, t, o, h, l, c, v, qv, n, tbv, tbq FROM raw_klines_5m WHERE t>=? AND t<?", ()),
            "klines_1m": ("SELECT symbol, t, o, h, l, c, NULL AS v, qv, NULL AS n, NULL AS tbv, NULL AS tbq "
                          "FROM raw_klines_1m WHERE t>=? AND t<?", ()),
            "metrics_5m": ("SELECT o.symbol, o.t, COALESCE(o.oi_hist, o.oi_live) AS oi, o.oi_hist_usd AS oi_usd, "
                           "ta.ratio AS top_acc, tp.ratio AS top_pos, g.ratio AS ls_global, NULL AS taker_ratio "
                           "FROM raw_oi o "
                           "LEFT JOIN raw_ls g ON g.symbol=o.symbol AND g.t=o.t AND g.kind='global' "
                           "LEFT JOIN raw_ls ta ON ta.symbol=o.symbol AND ta.t=o.t AND ta.kind='top_acc' "
                           "LEFT JOIN raw_ls tp ON tp.symbol=o.symbol AND tp.t=o.t AND tp.kind='top_pos' "
                           "WHERE o.t>=? AND o.t<?", ()),
            "funding": ("SELECT f.symbol, f.next_ms AS t, COALESCE(s.funding_h, 8) AS interval_h, f.rate FROM raw_funding f "
                        "JOIN (SELECT symbol, next_ms, MAX(t) AS mt FROM raw_funding WHERE next_ms>=? AND next_ms<? "
                        "AND t<next_ms GROUP BY symbol, next_ms) m ON m.symbol=f.symbol AND m.mt=f.t "
                        "LEFT JOIN symbols s ON s.symbol=f.symbol", ()),
        }
        for ds, (sql, _) in q.items():
            path = self.root / ds / f"{name}.parquet"
            if path.exists():
                continue
            try:
                df = pd.read_sql_query(sql, conn, params=(d0, d1))
            except Exception as e:                                # noqa: BLE001
                out[ds] = f"нет данных ({type(e).__name__})"
                continue
            if df.empty:
                continue
            df = df.drop_duplicates(subset=["symbol", "t"], keep="last").sort_values(["symbol", "t"])
            for col, typ in zip(SCHEMAS[ds].names, SCHEMAS[ds].types):
                if col != "symbol":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    if pa.types.is_integer(typ):
                        df[col] = df[col].astype("Int64")
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            pq.write_table(pa.Table.from_pandas(df[SCHEMAS[ds].names], schema=SCHEMAS[ds], preserve_index=False),
                           tmp, compression="zstd")
            os.replace(tmp, path)
            out[ds] = len(df)
        return out

    async def export_pending(self) -> None:
        first = await self.db.read("SELECT MIN(t) FROM raw_klines_5m")
        if not first or first[0][0] is None:
            return
        d0 = first[0][0] // DAY * DAY
        last_full = now_ms() // DAY * DAY - DAY            # вчера — последний законченный день (UTC)
        while d0 <= last_full:
            name = datetime.fromtimestamp(d0 / 1000, timezone.utc).strftime("%Y-%m-%d")
            if not (self.root / "klines_5m" / f"{name}.parquet").exists() or \
                    not (self.root / "metrics_5m" / f"{name}.parquet").exists():
                res = await self.db.run_in_thread(lambda conn, d=d0: self._day_job(conn, d))
                if res:
                    log.info("единая база: день %s → %s", name, res)
            d0 += DAY

    async def loop(self) -> None:
        if not self.enabled:
            return
        while True:
            try:
                await self.export_pending()
            except Exception:                                     # noqa: BLE001
                log.exception("единая база: ошибка выгрузки дня")
            now = now_ms()
            nxt = now // DAY * DAY + DAY + 20 * 60_000        # 00:20 UTC следующих суток
            await asyncio.sleep((nxt - now) / 1000)
