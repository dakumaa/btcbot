#!/usr/bin/env python3
# ============================================================
# TRADING SIGNAL BOT — Supply & Demand + Fractal Scalping
# Aset    : BTC/USDT + XAUUSDT (Gold proxy)
# TF      : 30M (zona) → 5M (konfirmasi) → 1M (entry trigger)
# Strategi 1: Supply & Demand Classic (Ruang Trader style)
# Strategi 2: Fractal Scalping Set & Forget (Forex Sarjana style)
# Tracking: SQLite database + Daily Report 07:00 WIB
# ============================================================

import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from threading import Thread

import pandas as pd
import pandas_ta as ta
import requests
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# ⚙️  CONFIGURATION
# ============================================================
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "your_token_here")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID",   "your_chat_id_here")
# Binance Vision = mirror publik Binance, tidak ada geo-block, tanpa API key
# Sama persis dengan yang dipakai bot Polymarket sebelumnya
BINANCE_KLINES_URL  = "https://data-api.binance.vision/api/v3/klines"
BINANCE_TICKER_URL  = "https://data-api.binance.vision/api/v3/ticker/price"

# ── Aset ──────────────────────────────────────────────────────────────────────
ASSETS = [
    {"symbol": "BTC/USDT",  "name": "BTC",  "risk_usd": 100.0},
    {"symbol": "XAUT/USDT", "name": "XAU",  "risk_usd": 100.0},
]

# ── Timeframes ────────────────────────────────────────────────────────────────
TF_HIGH  = "30m"   # deteksi zona Supply & Demand
TF_MID   = "5m"    # konfirmasi zona + rejection
TF_LOW   = "1m"    # entry trigger

# ── Strategi parameter ────────────────────────────────────────────────────────
ATR_PERIOD          = 14
STRONG_LEG_MULT     = 1.5   # candle leg out harus ≥ 1.5× ATR
ZONE_FRESH_BUFFER   = 0.001 # 0.1% toleransi zona fresh
RR_RATIO            = 3.0   # Risk:Reward 1:3
SCAN_INTERVAL_SEC   = 300   # scan setiap 5 menit

# ── Report ────────────────────────────────────────────────────────────────────
DAILY_REPORT_HOUR   = 0     # 00:00 UTC = 07:00 WIB
DB_PATH             = "signals.db"
LOG_FILE            = "bot.log"
MAX_RETRIES         = 3

# ============================================================
# 📋  LOGGING
# ============================================================
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("TradeBot")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "[%(asctime)s UTC] %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = setup_logger()

# ============================================================
# 🗄️  DATABASE
# ============================================================
def init_db():
    """Buat tabel SQLite jika belum ada."""
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL,
            asset       TEXT    NOT NULL,
            strategy    TEXT    NOT NULL,
            signal_type TEXT    NOT NULL,
            entry       REAL    NOT NULL,
            sl          REAL    NOT NULL,
            tp          REAL    NOT NULL,
            rr          REAL    NOT NULL,
            tf_confirm  TEXT    NOT NULL,
            status      TEXT    DEFAULT 'OPEN',
            result      TEXT,
            pnl_pct     REAL,
            pnl_usd     REAL,
            close_time  TEXT,
            notes       TEXT
        )
    """)
    conn.commit()
    conn.close()
    log.info("✅ Database siap.")

def save_signal(asset, strategy, signal_type, entry, sl, tp, tf_confirm, notes="") -> int:
    """Simpan sinyal baru ke database, return ID."""
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO signals
        (timestamp, asset, strategy, signal_type, entry, sl, tp, rr, tf_confirm, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        asset, strategy, signal_type, entry, sl, tp, RR_RATIO, tf_confirm, notes
    ))
    sig_id = cur.lastrowid
    conn.commit()
    conn.close()
    return sig_id

def update_signal_result(sig_id: int, result: str, close_price: float, risk_usd: float):
    """Update hasil trade (TP/SL) di database."""
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("SELECT entry, sl, tp, signal_type FROM signals WHERE id=?", (sig_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return

    entry, sl, tp, stype = row
    if result == "TP":
        pnl_pct = RR_RATIO * 1.0  # +3%
        pnl_usd = risk_usd * RR_RATIO
    else:
        pnl_pct = -1.0             # -1%
        pnl_usd = -risk_usd

    cur.execute("""
        UPDATE signals
        SET status='CLOSED', result=?, pnl_pct=?, pnl_usd=?, close_time=?
        WHERE id=?
    """, (
        result, pnl_pct, pnl_usd,
        datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        sig_id
    ))
    conn.commit()
    conn.close()

def get_daily_stats(date_str: str) -> dict:
    """Ambil statistik harian dari database, termasuk streak & drawdown."""
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        SELECT strategy, asset, result, pnl_pct, pnl_usd, close_time
        FROM signals
        WHERE DATE(timestamp) = ? AND status = 'CLOSED'
        ORDER BY close_time ASC
    """, (date_str,))
    rows = cur.fetchall()
    conn.close()

    stats = {}
    for strategy, asset, result, pnl_pct, pnl_usd, close_time in rows:
        key = (strategy, asset)
        if key not in stats:
            stats[key] = {
                "win": 0, "loss": 0,
                "pnl_pct": 0.0, "pnl_usd": 0.0,
                "results": [],          # urutan hasil untuk hitung streak
                "max_losestreak": 0,    # losestreak terpanjang
                "max_winstreak": 0,     # winstreak terpanjang
                "max_drawdown_pct": 0.0, # drawdown maksimum (%)
                "max_drawdown_usd": 0.0,
            }
        s = stats[key]
        if result == "TP":
            s["win"] += 1
        else:
            s["loss"] += 1
        s["pnl_pct"] += pnl_pct or 0.0
        s["pnl_usd"] += pnl_usd or 0.0
        s["results"].append(result)

    # Hitung streak dan drawdown dari urutan hasil
    for key, s in stats.items():
        results = s["results"]
        cur_lose = cur_win = 0
        max_lose = max_win = 0
        cur_dd_pct = cur_dd_usd = 0.0
        max_dd_pct = max_dd_usd = 0.0

        for r in results:
            if r == "SL":
                cur_lose += 1
                cur_win   = 0
                cur_dd_pct += 1.0   # -1% per SL
                cur_dd_usd += 100.0 # asumsi risk $100
                max_dd_pct = max(max_dd_pct, cur_dd_pct)
                max_dd_usd = max(max_dd_usd, cur_dd_usd)
            else:
                cur_win  += 1
                cur_lose  = 0
                cur_dd_pct = 0.0  # reset drawdown saat TP
                cur_dd_usd = 0.0

            max_lose = max(max_lose, cur_lose)
            max_win  = max(max_win, cur_win)

        s["max_losestreak"]   = max_lose
        s["max_winstreak"]    = max_win
        s["max_drawdown_pct"] = max_dd_pct
        s["max_drawdown_usd"] = max_dd_usd

    return stats

def get_alltime_streak() -> dict:
    """
    Hitung losestreak dan drawdown sepanjang waktu dari semua closed trades.
    Dipakai untuk alert real-time saat losestreak terjadi.
    """
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        SELECT strategy, asset, result, pnl_usd
        FROM signals
        WHERE status = 'CLOSED'
        ORDER BY close_time ASC
    """)
    rows = cur.fetchall()
    conn.close()

    streaks = {}
    for strategy, asset, result, pnl_usd in rows:
        key = (strategy, asset)
        if key not in streaks:
            streaks[key] = {
                "cur_lose": 0, "cur_win": 0,
                "max_lose": 0, "max_win": 0,
                "cur_dd_usd": 0.0, "max_dd_usd": 0.0,
            }
        s = streaks[key]
        if result == "SL":
            s["cur_lose"]   += 1
            s["cur_win"]     = 0
            s["cur_dd_usd"] += abs(pnl_usd or 100.0)
            s["max_dd_usd"]  = max(s["max_dd_usd"], s["cur_dd_usd"])
        else:
            s["cur_win"]    += 1
            s["cur_lose"]    = 0
            s["cur_dd_usd"]  = 0.0
        s["max_lose"] = max(s["max_lose"], s["cur_lose"])
        s["max_win"]  = max(s["max_win"], s["cur_win"])

    return streaks

# ============================================================
# 📡  BINANCE DATA FETCHER via CCXT
# ============================================================
# ── Mapping symbol ccxt → Binance REST format ─────────────────────────────────
# ccxt pakai "BTC/USDT", Binance REST pakai "BTCUSDT"
def to_binance_symbol(symbol: str) -> str:
    return symbol.replace("/", "")

# ── Mapping timeframe label → Binance interval string ─────────────────────────
TF_MAP = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h"}

def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 100) -> pd.DataFrame | None:
    """
    Fetch OHLCV dari Binance Vision REST API.
    Tidak ada geo-block, tidak butuh API key.
    Candle terakhir (live) dibuang otomatis.
    """
    bn_symbol = to_binance_symbol(symbol)
    interval  = TF_MAP.get(timeframe, timeframe)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                BINANCE_KLINES_URL,
                params={"symbol": bn_symbol, "interval": interval, "limit": limit + 1},
                timeout=15,
            )
            resp.raise_for_status()
            raw = resp.json()

            if not isinstance(raw, list) or len(raw) < 3:
                return None

            data = [
                [int(r[0]), float(r[1]), float(r[2]),
                 float(r[3]), float(r[4]), float(r[5])]
                for r in raw
            ]
            df = pd.DataFrame(data, columns=["timestamp","open","high","low","close","volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            return df.iloc[:-1].reset_index(drop=True)  # buang candle live

        except requests.exceptions.RequestException as e:
            log.warning(f"  ⚠️ Fetch {symbol} {timeframe} gagal (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5)

    log.error(f"❌ Fetch {symbol} {timeframe} gagal setelah {MAX_RETRIES} attempts.")
    return None

def get_current_price(symbol: str) -> float | None:
    """Ambil harga terkini dari Binance Vision REST API."""
    bn_symbol = to_binance_symbol(symbol)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                BINANCE_TICKER_URL,
                params={"symbol": bn_symbol},
                timeout=10,
            )
            resp.raise_for_status()
            return float(resp.json()["price"])
        except Exception as e:
            log.warning(f"  ⚠️ Harga {symbol} gagal (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(3)
    return None

# ============================================================
# 📐  INDIKATOR TEKNIKAL
# ============================================================
def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR menggunakan pandas_ta."""
    atr = ta.atr(df["high"], df["low"], df["close"], length=period)
    return atr

def detect_fractals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deteksi Fractal High dan Fractal Low (Williams Fractal).
    Fractal High: candle[i].high > candle[i-2, i-1, i+1, i+2].high
    Fractal Low:  candle[i].low  < candle[i-2, i-1, i+1, i+2].low

    Returns df dengan kolom fractal_high dan fractal_low.
    """
    df = df.copy()
    df["fractal_high"] = False
    df["fractal_low"]  = False

    for i in range(2, len(df) - 2):
        h = df["high"].iloc
        l = df["low"].iloc

        if (h[i] > h[i-1] and h[i] > h[i-2] and
                h[i] > h[i+1] and h[i] > h[i+2]):
            df.at[df.index[i], "fractal_high"] = True

        if (l[i] < l[i-1] and l[i] < l[i-2] and
                l[i] < l[i+1] and l[i] < l[i+2]):
            df.at[df.index[i], "fractal_low"] = True

    return df

# ============================================================
# 🏗️  STRATEGI 1: SUPPLY & DEMAND CLASSIC
# ============================================================
def detect_sd_zones(df_30m: pd.DataFrame) -> list[dict]:
    """
    Deteksi zona Supply & Demand di 30M.

    Kriteria zona valid:
    1. Fresh / Unmitigated — harga belum masuk kembali ke zona
    2. Strong Leg Out — candle keluar zona ≥ 1.5× ATR
    3. Struktur: Leg In → Base → Leg Out yang jelas

    Returns: list of zone dict
    {
      type: "DEMAND" | "SUPPLY",
      zone_high: float,
      zone_low: float,
      leg_out_size: float,
      atr: float,
      timestamp: str,
    }
    """
    if df_30m is None or len(df_30m) < ATR_PERIOD + 5:
        return []

    atr_series = calc_atr(df_30m, ATR_PERIOD)
    zones      = []

    for i in range(3, len(df_30m) - 1):
        atr_val = atr_series.iloc[i]
        if pd.isna(atr_val) or atr_val <= 0:
            continue

        candle = df_30m.iloc[i]
        prev1  = df_30m.iloc[i - 1]
        prev2  = df_30m.iloc[i - 2]

        # ── Cek struktur Leg In → Base → Leg Out ──────────────────────────────
        # Leg In:  candle[i-2] bergerak kuat ke satu arah
        # Base:    candle[i-1] kecil (konsolidasi / doji)
        # Leg Out: candle[i] bergerak kuat ke arah berlawanan

        body_prev2 = abs(prev2["close"] - prev2["open"])
        body_prev1 = abs(prev1["close"] - prev1["open"])
        body_curr  = abs(candle["close"] - candle["open"])

        # Base harus kecil (body < 0.5× ATR)
        if body_prev1 > 0.5 * atr_val:
            continue

        # Leg Out harus besar (≥ STRONG_LEG_MULT × ATR)
        leg_out_size = candle["high"] - candle["low"]
        if leg_out_size < STRONG_LEG_MULT * atr_val:
            continue

        ts_str = str(df_30m["timestamp"].iloc[i])

        # ── DEMAND ZONE: Leg In turun → Base → Leg Out naik ──────────────────
        if (prev2["close"] < prev2["open"] and   # leg in = bearish
                candle["close"] > candle["open"] and  # leg out = bullish
                candle["close"] > prev2["open"]):    # leg out lebih tinggi

            zone_low  = min(prev1["low"],  candle["low"])
            zone_high = max(prev1["high"], candle["open"])

            # Cek fresh: harga setelah zona tidak masuk kembali ke zona
            is_fresh = True
            for j in range(i + 1, len(df_30m)):
                if df_30m["low"].iloc[j] <= zone_high:
                    is_fresh = False
                    break

            if is_fresh:
                zones.append({
                    "type":        "DEMAND",
                    "zone_high":   zone_high,
                    "zone_low":    zone_low,
                    "leg_out_size": leg_out_size,
                    "atr":         atr_val,
                    "timestamp":   ts_str,
                    "index":       i,
                })

        # ── SUPPLY ZONE: Leg In naik → Base → Leg Out turun ──────────────────
        elif (prev2["close"] > prev2["open"] and  # leg in = bullish
                candle["close"] < candle["open"] and  # leg out = bearish
                candle["close"] < prev2["open"]):    # leg out lebih rendah

            zone_high = max(prev1["high"], candle["high"])
            zone_low  = min(prev1["low"],  candle["open"])

            is_fresh = True
            for j in range(i + 1, len(df_30m)):
                if df_30m["high"].iloc[j] >= zone_low:
                    is_fresh = False
                    break

            if is_fresh:
                zones.append({
                    "type":        "SUPPLY",
                    "zone_high":   zone_high,
                    "zone_low":    zone_low,
                    "leg_out_size": leg_out_size,
                    "atr":         atr_val,
                    "timestamp":   ts_str,
                    "index":       i,
                })

    return zones

def check_strategy1_entry(
    symbol: str, asset_name: str, zones: list[dict],
    df_5m: pd.DataFrame, df_1m: pd.DataFrame
) -> dict | None:
    """
    Konfirmasi entry Strategi 1 di 5M dan 1M.

    1. Harga mendekati zona 30M (dalam zona ± 0.1% dari zone_high/low)
    2. Di 5M: ada rejection candle (wick panjang) atau engulfing
    3. Di 1M: candle penutup melewati high/low candle sebelumnya (trigger)
    """
    if df_5m is None or df_1m is None or not zones:
        return None

    price_5m = df_5m["close"].iloc[-1]

    for zone in zones:
        zh = zone["zone_high"]
        zl = zone["zone_low"]

        # Cek apakah harga mendekati zona
        if zone["type"] == "DEMAND":
            in_zone = zl * (1 - ZONE_FRESH_BUFFER) <= price_5m <= zh * (1 + ZONE_FRESH_BUFFER)
        else:
            in_zone = zl * (1 - ZONE_FRESH_BUFFER) <= price_5m <= zh * (1 + ZONE_FRESH_BUFFER)

        if not in_zone:
            continue

        # Konfirmasi 5M: cari rejection candle
        last_5m  = df_5m.iloc[-1]
        body_5m  = abs(last_5m["close"] - last_5m["open"])
        range_5m = last_5m["high"] - last_5m["low"]
        wick_ratio = (range_5m - body_5m) / range_5m if range_5m > 0 else 0

        confirmed_5m = False
        if zone["type"] == "DEMAND" and wick_ratio >= 0.6 and last_5m["close"] > last_5m["open"]:
            confirmed_5m = True
        elif zone["type"] == "SUPPLY" and wick_ratio >= 0.6 and last_5m["close"] < last_5m["open"]:
            confirmed_5m = True

        if not confirmed_5m:
            continue

        # Trigger 1M: candle bullish melewati high sebelumnya (demand)
        # atau candle bearish melewati low sebelumnya (supply)
        last_1m = df_1m.iloc[-1]
        prev_1m = df_1m.iloc[-2]
        triggered = False

        if zone["type"] == "DEMAND" and last_1m["close"] > prev_1m["high"]:
            triggered = True
        elif zone["type"] == "SUPPLY" and last_1m["close"] < prev_1m["low"]:
            triggered = True

        if not triggered:
            continue

        # Hitung entry, SL, TP
        if zone["type"] == "DEMAND":
            entry  = last_1m["close"]
            sl     = zl * (1 - 0.0005)   # SL di bawah zona dengan buffer kecil
            risk   = entry - sl
            tp     = entry + risk * RR_RATIO
            stype  = "LONG"
        else:
            entry  = last_1m["close"]
            sl     = zh * (1 + 0.0005)
            risk   = sl - entry
            tp     = entry - risk * RR_RATIO
            stype  = "SHORT"

        return {
            "strategy":   "STRATEGI 1 - S/D Classic",
            "asset":      asset_name,
            "symbol":     symbol,
            "type":       stype,
            "entry":      entry,
            "sl":         sl,
            "tp":         tp,
            "zone":       zone,
            "tf_confirm": f"{TF_HIGH} zone / {TF_MID} rejection / {TF_LOW} trigger",
        }

    return None

# ============================================================
# 🏗️  STRATEGI 2: FRACTAL SCALPING SET & FORGET
# ============================================================
def detect_order_blocks(df_30m: pd.DataFrame) -> list[dict]:
    """
    Deteksi Order Block di 30M.
    Order Block = candle terakhir yang berlawanan arah sebelum
    pergerakan impulsif (strong move).

    Dikombinasikan dengan Fractal untuk filter kualitas tinggi.
    """
    if df_30m is None or len(df_30m) < ATR_PERIOD + 5:
        return []

    atr_series = calc_atr(df_30m, ATR_PERIOD)
    df_frac    = detect_fractals(df_30m)
    obs        = []

    for i in range(2, len(df_30m) - 1):
        atr_val = atr_series.iloc[i]
        if pd.isna(atr_val) or atr_val <= 0:
            continue

        curr  = df_30m.iloc[i]
        prev  = df_30m.iloc[i - 1]
        next_ = df_30m.iloc[i + 1] if i + 1 < len(df_30m) else None

        if next_ is None:
            continue

        # Move impulsif setelah candle ini
        next_move = next_["high"] - next_["low"]

        # ── Bullish Order Block ────────────────────────────────────────────────
        # Candle[i] bearish → candle[i+1] bullish impulsif (≥ 1.5× ATR)
        if (curr["close"] < curr["open"] and
                next_["close"] > next_["open"] and
                next_move >= STRONG_LEG_MULT * atr_val):

            # Diperkuat jika ada Fractal Low di dekat area ini
            near_fractal = df_frac["fractal_low"].iloc[max(0, i-3):i+1].any()

            ob_high = curr["high"]
            ob_low  = curr["low"]

            # Fresh check
            is_fresh = True
            for j in range(i + 2, len(df_30m)):
                if df_30m["low"].iloc[j] <= ob_low:
                    is_fresh = False
                    break

            obs.append({
                "type":      "BULLISH_OB",
                "ob_high":   ob_high,
                "ob_low":    ob_low,
                "atr":       atr_val,
                "fractal":   near_fractal,
                "quality":   "HIGH" if near_fractal else "MEDIUM",
                "fresh":     is_fresh,
                "timestamp": str(df_30m["timestamp"].iloc[i]),
                "index":     i,
            })

        # ── Bearish Order Block ────────────────────────────────────────────────
        elif (curr["close"] > curr["open"] and
                next_["close"] < next_["open"] and
                next_move >= STRONG_LEG_MULT * atr_val):

            near_fractal = df_frac["fractal_high"].iloc[max(0, i-3):i+1].any()

            ob_high = curr["high"]
            ob_low  = curr["low"]

            is_fresh = True
            for j in range(i + 2, len(df_30m)):
                if df_30m["high"].iloc[j] >= ob_high:
                    is_fresh = False
                    break

            obs.append({
                "type":      "BEARISH_OB",
                "ob_high":   ob_high,
                "ob_low":    ob_low,
                "atr":       atr_val,
                "fractal":   near_fractal,
                "quality":   "HIGH" if near_fractal else "MEDIUM",
                "fresh":     is_fresh,
                "timestamp": str(df_30m["timestamp"].iloc[i]),
                "index":     i,
            })

    # Kembalikan hanya yang fresh
    return [ob for ob in obs if ob["fresh"]]

def check_strategy2_entry(
    symbol: str, asset_name: str, obs: list[dict],
    df_5m: pd.DataFrame, df_1m: pd.DataFrame
) -> dict | None:
    """
    Konfirmasi entry Strategi 2 di 5M (Fractal) dan 1M (trigger).

    Filter tambahan vs Strategi 1:
    - Hanya ambil Order Block berkualitas HIGH (ada Fractal nearby)
    - 5M: Fractal Low/High di dalam OB area
    - 1M: Candle trigger dengan volume konfirmasi
    """
    if df_5m is None or df_1m is None or not obs:
        return None

    df_5m_frac = detect_fractals(df_5m)
    price_5m   = df_5m["close"].iloc[-1]

    for ob in obs:
        # Prioritaskan OB berkualitas HIGH
        if ob["quality"] != "HIGH":
            continue

        # Cek harga mendekati OB
        buffer = ob["atr"] * 0.3
        near_ob = ob["ob_low"] - buffer <= price_5m <= ob["ob_high"] + buffer

        if not near_ob:
            continue

        # ── Konfirmasi 5M: ada Fractal Low di area OB Bullish ─────────────────
        if ob["type"] == "BULLISH_OB":
            # Cari fractal low di 5M yang berada dalam OB range
            frac_in_ob = False
            for i, row in df_5m.iterrows():
                if df_5m_frac["fractal_low"].iloc[i] and ob["ob_low"] <= row["low"] <= ob["ob_high"]:
                    frac_in_ob = True
                    break

            if not frac_in_ob:
                continue

            # Trigger 1M: candle bullish break previous high
            last_1m = df_1m.iloc[-1]
            prev_1m = df_1m.iloc[-2]
            if not (last_1m["close"] > prev_1m["high"] and last_1m["close"] > last_1m["open"]):
                continue

            entry = last_1m["close"]
            sl    = ob["ob_low"] * (1 - 0.0003)
            risk  = entry - sl
            tp    = entry + risk * RR_RATIO

            return {
                "strategy":   "STRATEGI 2 - Fractal Scalping",
                "asset":      asset_name,
                "symbol":     symbol,
                "type":       "LONG",
                "entry":      entry,
                "sl":         sl,
                "tp":         tp,
                "ob":         ob,
                "tf_confirm": f"{TF_HIGH} OB / {TF_MID} Fractal / {TF_LOW} trigger",
                "quality":    ob["quality"],
            }

        # ── Konfirmasi 5M: ada Fractal High di area OB Bearish ────────────────
        elif ob["type"] == "BEARISH_OB":
            frac_in_ob = False
            for i, row in df_5m.iterrows():
                if df_5m_frac["fractal_high"].iloc[i] and ob["ob_low"] <= row["high"] <= ob["ob_high"]:
                    frac_in_ob = True
                    break

            if not frac_in_ob:
                continue

            last_1m = df_1m.iloc[-1]
            prev_1m = df_1m.iloc[-2]
            if not (last_1m["close"] < prev_1m["low"] and last_1m["close"] < last_1m["open"]):
                continue

            entry = last_1m["close"]
            sl    = ob["ob_high"] * (1 + 0.0003)
            risk  = sl - entry
            tp    = entry - risk * RR_RATIO

            return {
                "strategy":   "STRATEGI 2 - Fractal Scalping",
                "asset":      asset_name,
                "symbol":     symbol,
                "type":       "SHORT",
                "entry":      entry,
                "sl":         sl,
                "tp":         tp,
                "ob":         ob,
                "tf_confirm": f"{TF_HIGH} OB / {TF_MID} Fractal / {TF_LOW} trigger",
                "quality":    ob["quality"],
            }

    return None

# ============================================================
# 📨  TELEGRAM
# ============================================================
def send_telegram(message: str) -> bool:
    if TELEGRAM_BOT_TOKEN == "your_token_here":
        log.warning("Telegram token belum dikonfigurasi.")
        return False
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "text":       message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            log.warning(f"  ⚠️ Telegram gagal (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(3)
    return False

# ============================================================
# 🏗️  BUILD MESSAGES
# ============================================================
def build_signal_message(sig: dict) -> str:
    now_str   = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    # Konversi ke WIB (UTC+7)
    wib_str   = (datetime.now(tz=timezone.utc) + timedelta(hours=7)).strftime("%H:%M WIB")
    stype     = sig["type"]
    arrow     = "🔼 LONG" if stype == "LONG" else "🔽 SHORT"
    strategy  = sig["strategy"]
    asset     = sig["asset"]

    entry = sig["entry"]
    sl    = sig["sl"]
    tp    = sig["tp"]
    risk  = abs(entry - sl)
    rr    = abs(tp - entry) / risk if risk > 0 else 0

    # Zona info
    zone_info = ""
    if "zone" in sig:
        z = sig["zone"]
        zone_info = (
            f"📦 Zona {z['type']}: ${z['zone_low']:.4f} - ${z['zone_high']:.4f}\n"
            f"   Leg Out: {z['leg_out_size']:.4f} ({z['leg_out_size']/z['atr']:.1f}× ATR)\n"
        )
    elif "ob" in sig:
        ob = sig["ob"]
        quality_icon = "⭐⭐" if ob["quality"] == "HIGH" else "⭐"
        zone_info = (
            f"📦 Order Block: ${ob['ob_low']:.4f} - ${ob['ob_high']:.4f}\n"
            f"   Kualitas: {quality_icon} {ob['quality']} "
            f"{'(+Fractal)' if ob['fractal'] else ''}\n"
        )

    return (
        f"🚨 <b>[{strategy}]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Aset      : <b>{asset}</b>\n"
        f"📌 Sinyal    : <b>{arrow}</b>\n"
        f"⏰ Waktu     : {now_str} / {wib_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entry     : <b>${entry:.4f}</b>\n"
        f"🛑 Stop Loss : ${sl:.4f} ({abs(entry-sl)/entry*100:.2f}%)\n"
        f"🎯 Take Profit: ${tp:.4f} ({abs(tp-entry)/entry*100:.2f}%)\n"
        f"📐 Risk:Reward: 1:{rr:.1f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{zone_info}"
        f"🔍 TF Confirm: {sig['tf_confirm']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Ini notifikasi sinyal, bukan rekomendasi finansial.\n"
        f"Selalu gunakan risk management yang tepat.</i>"
    )

def build_daily_report(date_str: str, stats: dict) -> str:
    wib_str = (datetime.now(tz=timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")
    msg     = f"📊 <b>DAILY REPORT — {wib_str} (07:00 WIB)</b>\n"
    msg    += "━━━━━━━━━━━━━━━━━━━━━━\n"

    total_win = total_loss = 0
    total_pnl_pct = total_pnl_usd = 0.0
    total_max_lose = total_max_dd = 0

    for (strategy, asset), s in sorted(stats.items()):
        total    = s["win"] + s["loss"]
        wr       = (s["win"] / total * 100) if total > 0 else 0
        total_win      += s["win"]
        total_loss     += s["loss"]
        total_pnl_pct  += s["pnl_pct"]
        total_pnl_usd  += s["pnl_usd"]
        total_max_lose  = max(total_max_lose, s["max_losestreak"])
        total_max_dd    = max(total_max_dd,   s["max_drawdown_usd"])

        strat_label = strategy.split(" - ")[1] if " - " in strategy else strategy
        pnl_sign = "+" if s["pnl_pct"] >= 0 else ""

        # Streak icons
        lose_icon = "🔴" * min(s["max_losestreak"], 5)
        win_icon  = "🟢" * min(s["max_winstreak"], 5)

        msg += (
            f"\n📌 <b>{strat_label} | {asset}</b>\n"
            f"   Trade    : {total} (✅{s['win']} ❌{s['loss']})\n"
            f"   WR       : {wr:.1f}%\n"
            f"   PnL      : {pnl_sign}{s['pnl_pct']:.1f}% ({pnl_sign}${s['pnl_usd']:.2f})\n"
            f"   Win streak: {win_icon} max {s['max_winstreak']}x\n"
            f"   Lose streak: {lose_icon} max {s['max_losestreak']}x\n"
            f"   Max DD   : -${s['max_drawdown_usd']:.0f} (-{s['max_drawdown_pct']:.0f}%)\n"
        )

    total_all = total_win + total_loss
    wr_all    = (total_win / total_all * 100) if total_all > 0 else 0
    pnl_sign  = "+" if total_pnl_pct >= 0 else ""

    msg += (
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>TOTAL SEMUA STRATEGI</b>\n"
        f"   Trade    : {total_all} (✅{total_win} ❌{total_loss})\n"
        f"   WR       : {wr_all:.1f}%\n"
        f"   PnL      : {pnl_sign}{total_pnl_pct:.1f}% ({pnl_sign}${total_pnl_usd:.2f})\n"
        f"   Max Losestreak: {'🔴'*min(total_max_lose,5)} {total_max_lose}x berturut\n"
        f"   Max Drawdown  : -${total_max_dd:.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Asumsi risk $100/trade (1%). Hasil aktual bisa berbeda.</i>"
    )
    return msg

# ============================================================
# 🔄  OPEN TRADE MONITOR
# Cek apakah trade aktif sudah kena TP atau SL
# ============================================================
_open_trades: list[dict] = []  # {id, symbol, type, entry, sl, tp, risk_usd}

def monitor_open_trades():
    """Cek setiap trade yang masih OPEN apakah sudah kena TP atau SL."""
    global _open_trades
    still_open = []

    for trade in _open_trades:
        price = get_current_price(trade["symbol"])
        if price is None:
            still_open.append(trade)
            continue

        hit_tp = hit_sl = False
        if trade["type"] == "LONG":
            if price >= trade["tp"]:
                hit_tp = True
            elif price <= trade["sl"]:
                hit_sl = True
        else:
            if price <= trade["tp"]:
                hit_tp = True
            elif price >= trade["sl"]:
                hit_sl = True

        if hit_tp or hit_sl:
            result = "TP" if hit_tp else "SL"
            update_signal_result(trade["id"], result, price, trade["risk_usd"])

            icon    = "🎯" if hit_tp else "🛑"
            pnl_pct = RR_RATIO if hit_tp else -1.0
            pnl_usd = trade["risk_usd"] * RR_RATIO if hit_tp else -trade["risk_usd"]
            pnl_sign = "+" if pnl_pct > 0 else ""

            send_telegram(
                f"{icon} <b>TRADE CLOSED — {result}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Aset    : {trade['asset']}\n"
                f"📌 Tipe    : {'🔼 LONG' if trade['type']=='LONG' else '🔽 SHORT'}\n"
                f"💰 Entry   : ${trade['entry']:.4f}\n"
                f"💰 Close   : ${price:.4f}\n"
                f"📐 Hasil   : <b>{result}</b>\n"
                f"💵 PnL     : {pnl_sign}{pnl_pct:.1f}% ({pnl_sign}${pnl_usd:.2f})\n"
                f"📌 Strategi: {trade['strategy']}"
            )
            log.info(f"  {'✅' if hit_tp else '❌'} {trade['asset']} {result} | PnL: {pnl_sign}{pnl_pct:.1f}%")

            # ── Cek losestreak real-time ───────────────────────────────────────
            streaks = get_alltime_streak()
            key = (trade["strategy"], trade["asset"])
            if key in streaks:
                sk = streaks[key]
                cur_lose = sk["cur_lose"]
                cur_dd   = sk["cur_dd_usd"]

                # Alert losestreak ≥ 3
                if cur_lose >= 3:
                    lose_icons = "🔴" * min(cur_lose, 7)
                    send_telegram(
                        f"⚠️ <b>LOSESTREAK ALERT!</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 Aset    : {trade['asset']}\n"
                        f"🎯 Strategi: {trade['strategy'].split(' - ')[1]}\n"
                        f"🔴 Losestreak: {lose_icons} <b>{cur_lose}x berturut!</b>\n"
                        f"💸 Drawdown : -${cur_dd:.0f} (-{cur_lose:.0f}%)\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🛑 <i>Pertimbangkan untuk pause trading sementara\n"
                        f"dan evaluasi kondisi market.</i>"
                    )

                # Alert winstreak ≥ 3
                if sk["cur_win"] >= 3:
                    win_icons = "🟢" * min(sk["cur_win"], 7)
                    send_telegram(
                        f"🔥 <b>WINSTREAK!</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 Aset    : {trade['asset']}\n"
                        f"🎯 Strategi: {trade['strategy'].split(' - ')[1]}\n"
                        f"🟢 Winstreak: {win_icons} <b>{sk['cur_win']}x berturut!</b>\n"
                        f"🚀 <i>Strategi sedang on fire!</i>"
                    )

        else:
            still_open.append(trade)

    _open_trades = still_open

# ============================================================
# 🚀  MAIN SCAN LOOP
# ============================================================
_last_signals: dict = {}   # {(symbol, strategy): last_signal_ts} untuk deduplicate
_last_report_date: str = ""

def should_send_signal(symbol: str, strategy: str) -> bool:
    """Hindari mengirim sinyal yang sama dalam 1 jam."""
    key    = (symbol, strategy)
    now_ts = time.time()
    last   = _last_signals.get(key, 0)
    if now_ts - last < 3600:  # cooldown 1 jam
        return False
    _last_signals[key] = now_ts
    return True

def run_scan():
    """Scan semua aset dengan kedua strategi."""
    log.info("🔍 Mulai scan...")

    for asset in ASSETS:
        symbol    = asset["symbol"]
        name      = asset["name"]
        risk_usd  = asset["risk_usd"]

        log.info(f"  📊 Scan {name} ({symbol})")

        # Fetch data semua TF
        df_30m = fetch_ohlcv(symbol, TF_HIGH,  limit=60)
        df_5m  = fetch_ohlcv(symbol, TF_MID,   limit=60)
        df_1m  = fetch_ohlcv(symbol, TF_LOW,   limit=20)

        if df_30m is None:
            log.warning(f"  ⚠️ Data {name} {TF_HIGH} tidak tersedia.")
            continue

        # ── Strategi 1: S/D Classic ───────────────────────────────────────────
        zones = detect_sd_zones(df_30m)
        log.debug(f"  Strategi 1: {len(zones)} zona valid ditemukan")

        sig1 = check_strategy1_entry(symbol, name, zones, df_5m, df_1m)
        if sig1 and should_send_signal(symbol, "S1"):
            sig_id = save_signal(
                name, sig1["strategy"], sig1["type"],
                sig1["entry"], sig1["sl"], sig1["tp"],
                sig1["tf_confirm"],
                notes=f"Zone: {sig1['zone']['type']} {sig1['zone']['zone_low']:.4f}-{sig1['zone']['zone_high']:.4f}"
            )
            send_telegram(build_signal_message(sig1))
            _open_trades.append({
                "id":       sig_id,
                "symbol":   symbol,
                "asset":    name,
                "strategy": sig1["strategy"],
                "type":     sig1["type"],
                "entry":    sig1["entry"],
                "sl":       sig1["sl"],
                "tp":       sig1["tp"],
                "risk_usd": risk_usd,
            })
            log.info(f"  📨 Sinyal S1 {name} {sig1['type']} dikirim (ID: {sig_id})")

        # ── Strategi 2: Fractal Scalping ──────────────────────────────────────
        obs  = detect_order_blocks(df_30m)
        log.debug(f"  Strategi 2: {len(obs)} OB valid ditemukan")

        sig2 = check_strategy2_entry(symbol, name, obs, df_5m, df_1m)
        if sig2 and should_send_signal(symbol, "S2"):
            sig_id = save_signal(
                name, sig2["strategy"], sig2["type"],
                sig2["entry"], sig2["sl"], sig2["tp"],
                sig2["tf_confirm"],
                notes=f"OB: {sig2['ob']['type']} Q:{sig2['ob']['quality']}"
            )
            send_telegram(build_signal_message(sig2))
            _open_trades.append({
                "id":       sig_id,
                "symbol":   symbol,
                "asset":    name,
                "strategy": sig2["strategy"],
                "type":     sig2["type"],
                "entry":    sig2["entry"],
                "sl":       sig2["sl"],
                "tp":       sig2["tp"],
                "risk_usd": risk_usd,
            })
            log.info(f"  📨 Sinyal S2 {name} {sig2['type']} dikirim (ID: {sig_id})")

    # Monitor open trades
    monitor_open_trades()

def check_daily_report():
    """Kirim daily report jam 07:00 WIB (00:00 UTC)."""
    global _last_report_date
    now_utc   = datetime.now(tz=timezone.utc)
    yesterday = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")

    if now_utc.hour == DAILY_REPORT_HOUR and now_utc.minute < 6:
        if _last_report_date != yesterday:
            stats = get_daily_stats(yesterday)
            if stats:
                send_telegram(build_daily_report(yesterday, stats))
                log.info(f"📊 Daily report {yesterday} dikirim.")
            else:
                send_telegram(
                    f"📊 <b>DAILY REPORT — {yesterday}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Tidak ada trade yang closed kemarin."
                )
            _last_report_date = yesterday

# ============================================================
# ▶️  ENTRY POINT
# ============================================================
def run_bot():
    log.info("🚀 Trading Signal Bot AKTIF")
    log.info(f"   Aset      : {', '.join(a['name'] for a in ASSETS)}")
    log.info(f"   Strategi  : S/D Classic + Fractal Scalping")
    log.info(f"   TF        : {TF_HIGH} / {TF_MID} / {TF_LOW}")
    log.info(f"   Scan      : setiap {SCAN_INTERVAL_SEC//60} menit")
    log.info(f"   RR        : 1:{RR_RATIO}")

    # Init database
    init_db()

    # Kirim pesan startup
    send_telegram(
        "🤖 <b>Trading Signal Bot AKTIF</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Aset: BTC/USDT + XAUT/USDT (Gold)\n"
        f"🎯 Strategi 1: Supply & Demand Classic\n"
        f"🎯 Strategi 2: Fractal Scalping\n"
        f"⏱️ TF: {TF_HIGH} → {TF_MID} → {TF_LOW}\n"
        f"📐 RR: 1:{RR_RATIO}\n"
        f"🔄 Scan setiap {SCAN_INTERVAL_SEC//60} menit\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <i>Bukan rekomendasi investasi.</i>"
    )

    # Scan pertama langsung
    try:
        run_scan()
    except Exception as e:
        log.error(f"💥 Scan pertama error: {e}", exc_info=True)

    # Loop utama
    while True:
        try:
            time.sleep(SCAN_INTERVAL_SEC)
            check_daily_report()
            run_scan()
        except KeyboardInterrupt:
            log.info("🛑 Bot dihentikan. Sampai jumpa! 👋")
            break
        except Exception as e:
            log.error(f"💥 Error di loop utama: {e}", exc_info=True)
            time.sleep(30)

if __name__ == "__main__":
    run_bot()
