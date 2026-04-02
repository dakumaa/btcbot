# ============================================================
# POLYMARKET 15M SIGNAL BOT v8.2
# Fix v8: Harga referensi = Close candle 15M (bukan entry harga real-time)
#         Ini sesuai cara Polymarket menentukan UP/DOWN:
#         "Apakah harga AKHIR window > harga AWAL window?"
#         Harga awal window = Close candle 15M sebelumnya
#         = Open candle 15M window yang sedang berjalan
# ============================================================

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from scipy.signal import find_peaks

load_dotenv()

# ============================================================
# ⚙️  CONFIGURATION
# ============================================================
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "your_token_here")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID",   "your_chat_id_here")

BACKTEST_MODE       = os.getenv("BACKTEST_MODE", "False").lower() == "true"

WICK_RATIO_MIN      = float(os.getenv("WICK_RATIO_MIN",   "0.70"))
FALSE_BREAK_MIN     = float(os.getenv("FALSE_BREAK_MIN",  "0.0015"))
BODY_RATIO_MIN      = float(os.getenv("BODY_RATIO_MIN",   "0.45"))  # longgar 55%→45%
CLOSE_UPPER_MIN     = float(os.getenv("CLOSE_UPPER_MIN",  "0.65"))  # longgar 70%→65%
CLOSE_LOWER_MAX     = float(os.getenv("CLOSE_LOWER_MAX",  "0.35"))  # longgar 30%→35%
VOLUME_MULT         = float(os.getenv("VOLUME_MULT",      "1.2"))   # longgar 1.3×→1.2×
RSI_UP_MAX          = float(os.getenv("RSI_UP_MAX",       "52"))
RSI_DOWN_MIN        = float(os.getenv("RSI_DOWN_MIN",     "48"))
SNR_TOLERANCE       = float(os.getenv("SNR_TOLERANCE",    "0.003"))
CANDLE_RANGE_MIN    = float(os.getenv("CANDLE_RANGE_MIN", "0.0015"))

HTF_STRICT          = os.getenv("HTF_STRICT", "False").lower() == "true"
EMA_FAST            = 9
EMA_SLOW            = 21
HTF_INTERVAL        = "1h"

# ── Candle Exhaustion (strategi baru) ────────────────────────────────────────
# Setelah N candle berturut-turut satu warna → kemungkinan besar berbalik
EXHAUSTION_MIN      = int(os.getenv("EXHAUSTION_MIN", "3"))    # min 3 candle searah
EXHAUSTION_MAX      = int(os.getenv("EXHAUSTION_MAX", "5"))    # max 5 candle searah
# True = wajib dekat S/R, False = boleh tanpa S/R (lebih banyak sinyal)
EXHAUSTION_SNR_REQ  = os.getenv("EXHAUSTION_SNR_REQ", "False").lower() == "true"

CONFIRM_WAIT_SEC    = int(os.getenv("CONFIRM_WAIT_SEC", "180"))
CONFIRM_TF          = "1m"
CONFIRM_BODY_MIN    = 0.50
CONFIRM_CLOSE_POS   = 0.70
HAMMER_WICK_MIN     = 0.65

RSI_PERIOD          = 14
SNR_LOOKBACK        = 40
SNR_PEAK_DIST       = 5
MAX_RETRIES         = 3
RETRY_DELAY         = 5
STREAK_THRESHOLD    = 3
DAILY_REPORT_HOUR   = 0

BINANCE_KLINES_URL  = "https://data-api.binance.vision/api/v3/klines"
BINANCE_TICKER_URL  = "https://data-api.binance.vision/api/v3/ticker/price"
LOG_FILE            = "bot.log"

ET_OFFSET_HOURS     = -4
ET_OFFSET           = timedelta(hours=ET_OFFSET_HOURS)
TF_INTERVAL_SEC     = 900

COINS = [
    {"symbol": "BTCUSDT",  "name": "BTC"},
    {"symbol": "ETHUSDT",  "name": "ETH"},
    {"symbol": "SOLUSDT",  "name": "SOL"},
    {"symbol": "XRPUSDT",  "name": "XRP"},
    {"symbol": "DOGEUSDT", "name": "DOGE"},
    {"symbol": "BNBUSDT",  "name": "BNB"},
]

# ============================================================
# 📋  LOGGING
# ============================================================
def setup_logger():
    logger = logging.getLogger("PolyBot")
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
# 📡  FETCH DATA
# ============================================================
def fetch_candles(symbol, interval, limit=100):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                BINANCE_KLINES_URL,
                params={"symbol": symbol, "interval": interval, "limit": limit + 1},
                timeout=15,
            )
            resp.raise_for_status()
            raw = resp.json()
            if not isinstance(raw, list):
                return []
            candles = [
                [int(r[0]), float(r[1]), float(r[2]),
                 float(r[3]), float(r[4]), float(r[5])]
                for r in raw
            ]
            return candles[:-1]
        except requests.exceptions.RequestException as e:
            log.warning(f"  ⚠️ Fetch {symbol} {interval} gagal (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return []

def fetch_current_price(symbol):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BINANCE_TICKER_URL, params={"symbol": symbol}, timeout=10)
            resp.raise_for_status()
            return float(resp.json()["price"])
        except Exception as e:
            log.warning(f"  ⚠️ Fetch price {symbol} gagal (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return None

# ============================================================
# ⏱️  POLYMARKET HELPERS
# ============================================================
def get_polymarket_link(name, open_ts_ms):
    """Link ke window BERIKUTNYA (window yang aktif setelah candle tutup)."""
    open_ts_sec = open_ts_ms // 1000
    next_window = ((open_ts_sec // TF_INTERVAL_SEC) + 1) * TF_INTERVAL_SEC
    return f"https://polymarket.com/event/{name.lower()}-updown-15m-{next_window}"

def get_poly_window_end_utc(open_ts_ms):
    """Waktu tutup window Polymarket berikutnya (ET-aligned) dalam UTC detik."""
    open_ts_sec   = open_ts_ms // 1000
    open_dt_utc   = datetime.fromtimestamp(open_ts_sec, tz=timezone.utc)
    open_dt_et    = open_dt_utc + ET_OFFSET
    et_epoch      = int(open_dt_et.timestamp())
    et_floored    = (et_epoch // TF_INTERVAL_SEC) * TF_INTERVAL_SEC
    et_window_end = et_floored + TF_INTERVAL_SEC
    return et_window_end - int(ET_OFFSET.total_seconds())

def get_result_ready_ts(open_ts_ms):
    return get_poly_window_end_utc(open_ts_ms) + 10

def fmt_utc(ts_sec):
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%H:%M")

def fmt_et(ts_sec):
    return (datetime.fromtimestamp(ts_sec, tz=timezone.utc) + ET_OFFSET).strftime("%H:%M")

# ============================================================
# 📐  INDIKATOR
# ============================================================
def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    df    = pd.Series(closes)
    delta = df.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_g = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_l = loss.ewm(com=period - 1, min_periods=period).mean()
    rs    = avg_g / avg_l.replace(0, 1e-10)
    return float((100 - (100 / (1 + rs))).iloc[-1])

def calc_ema(closes, period):
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])

def get_htf_bias(symbol):
    candles = fetch_candles(symbol, HTF_INTERVAL, limit=50)
    if len(candles) < EMA_SLOW + 5:
        return "NEUTRAL"
    closes = [c[4] for c in candles]
    ema9   = calc_ema(closes, EMA_FAST)
    ema21  = calc_ema(closes, EMA_SLOW)
    lc     = closes[-1]
    if lc > ema9 and lc > ema21:
        return "BULLISH"
    if lc < ema9 and lc < ema21:
        return "BEARISH"
    return "NEUTRAL"

# ============================================================
# 📐  S/R DETECTION
# ============================================================
def detect_snr(candles):
    if len(candles) < SNR_LOOKBACK:
        return {"supports": [], "resistances": []}
    df = pd.DataFrame(
        candles[-SNR_LOOKBACK:],
        columns=["ts", "open", "high", "low", "close", "volume"]
    )
    highs = df["high"].values
    lows  = df["low"].values
    peak_idx, _   = find_peaks(highs, distance=SNR_PEAK_DIST)
    trough_idx, _ = find_peaks(-lows, distance=SNR_PEAK_DIST)
    return {
        "supports":    sorted(set(round(v, 6) for v in lows[trough_idx])),
        "resistances": sorted(set(round(v, 6) for v in highs[peak_idx])),
    }

def find_nearest_snr(close, direction, snr):
    tolerance  = close * SNR_TOLERANCE
    candidates = snr["supports"] if direction == "UP" else snr["resistances"]
    level_type = "Support" if direction == "UP" else "Resistance"
    nearest, nearest_diff = None, float("inf")
    for level in candidates:
        diff = abs(close - level)
        if diff <= tolerance and diff < nearest_diff:
            nearest, nearest_diff = level, diff
    return nearest, level_type

# ============================================================
# 🔍  DETEKSI POLA
# ============================================================
def detect_pattern(symbol, candles):
    if len(candles) < SNR_LOOKBACK + 5:
        return None

    c = candles[-1]
    ts, o, h, l, close, vol = c
    candle_range = h - l

    if candle_range < 1e-8:
        return None
    if candle_range / close < CANDLE_RANGE_MIN:
        return None

    body       = abs(close - o)
    up_wick    = h - max(o, close)
    lo_wick    = min(o, close) - l
    up_ratio   = up_wick / candle_range
    lo_ratio   = lo_wick / candle_range
    body_ratio = body / candle_range
    close_pos  = (close - l) / candle_range

    prev_vols = [cd[5] for cd in candles[-6:-1]]
    avg_vol   = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ok    = vol >= VOLUME_MULT * avg_vol
    vol_ratio = vol / avg_vol if avg_vol > 0 else 0

    closes = [cd[4] for cd in candles]
    rsi    = calc_rsi(closes, RSI_PERIOD)
    snr    = detect_snr(candles)
    result = None

    # ══════════════════════════════════════════════════════════════════════════
    # URUTAN PRIORITAS DETEKSI POLA:
    # 1. MOMENTUM  — body kuat + volume tinggi (paling cepat dikonfirmasi)
    # 2. EXHAUSTION — 3-5 candle warna sama → kemungkinan berbalik
    # 3. WICK       — ekor panjang + dekat S/R
    # 4. FALSE BREAK — spike tembus S/R lalu balik (paling jarang, paling kuat)
    # ══════════════════════════════════════════════════════════════════════════

    # POLA 1: MOMENTUM
    if vol_ok:
        if close > o and body_ratio >= BODY_RATIO_MIN and close_pos >= CLOSE_UPPER_MIN:
            lvl, ltype = find_nearest_snr(close, "UP", snr)
            result = {"signal":"UP","pattern":"MOMENTUM","wick_pct":None,"body_pct":body_ratio*100,
                      "lvl":lvl,"lvl_type":ltype or "Support",
                      "extra":f"Bullish Body {body_ratio*100:.1f}% Vol×{vol_ratio:.2f}"}
        elif close < o and body_ratio >= BODY_RATIO_MIN and close_pos <= CLOSE_LOWER_MAX:
            lvl, ltype = find_nearest_snr(close, "DOWN", snr)
            result = {"signal":"DOWN","pattern":"MOMENTUM","wick_pct":None,"body_pct":body_ratio*100,
                      "lvl":lvl,"lvl_type":ltype or "Resistance",
                      "extra":f"Bearish Body {body_ratio*100:.1f}% Vol×{vol_ratio:.2f}"}

    # POLA 2: EXHAUSTION (3-5 candle warna sama → prediksi balik arah)
    if result is None:
        lookback = candles[-(EXHAUSTION_MAX + 2):-1]

        bearish_streak = 0
        for prev_c in reversed(lookback):
            if prev_c[4] < prev_c[1]:
                bearish_streak += 1
            else:
                break

        bullish_streak = 0
        for prev_c in reversed(lookback):
            if prev_c[4] >= prev_c[1]:
                bullish_streak += 1
            else:
                break

        if EXHAUSTION_MIN <= bearish_streak <= EXHAUSTION_MAX:
            lvl, ltype = find_nearest_snr(close, "UP", snr)
            if not EXHAUSTION_SNR_REQ or lvl is not None:
                snr_info = f" + {ltype} ${lvl:.4f}" if lvl else " (no S/R)"
                result = {
                    "signal":"UP","pattern":"EXHAUSTION",
                    "wick_pct":None,"body_pct":None,
                    "lvl":lvl,"lvl_type":ltype or "Support",
                    "extra":f"{bearish_streak}🔴 berturut → potensi UP{snr_info}",
                }
        elif EXHAUSTION_MIN <= bullish_streak <= EXHAUSTION_MAX:
            lvl, ltype = find_nearest_snr(close, "DOWN", snr)
            if not EXHAUSTION_SNR_REQ or lvl is not None:
                snr_info = f" + {ltype} ${lvl:.4f}" if lvl else " (no S/R)"
                result = {
                    "signal":"DOWN","pattern":"EXHAUSTION",
                    "wick_pct":None,"body_pct":None,
                    "lvl":lvl,"lvl_type":ltype or "Resistance",
                    "extra":f"{bullish_streak}🟢 berturut → potensi DOWN{snr_info}",
                }

    # POLA 3: WICK
    if result is None and lo_ratio >= WICK_RATIO_MIN:
        lvl, ltype = find_nearest_snr(close, "UP", snr)
        if lvl is not None:
            result = {"signal":"UP","pattern":"WICK","wick_pct":lo_ratio*100,"body_pct":None,
                      "lvl":lvl,"lvl_type":ltype,
                      "extra":f"Lower Wick {lo_ratio*100:.1f}% + {ltype} ${lvl:.4f}"}
    if result is None and up_ratio >= WICK_RATIO_MIN:
        lvl, ltype = find_nearest_snr(close, "DOWN", snr)
        if lvl is not None:
            result = {"signal":"DOWN","pattern":"WICK","wick_pct":up_ratio*100,"body_pct":None,
                      "lvl":lvl,"lvl_type":ltype,
                      "extra":f"Upper Wick {up_ratio*100:.1f}% + {ltype} ${lvl:.4f}"}

    # POLA 4: FALSE BREAK
    if result is None:
        for sup in snr["supports"]:
            depth = (sup - l) / close
            if l < sup and close > sup and depth >= FALSE_BREAK_MIN:
                result = {"signal":"UP","pattern":"FALSE_BREAK","wick_pct":None,"body_pct":None,
                          "lvl":sup,"lvl_type":"Support",
                          "extra":f"False Break ↓{depth*100:.2f}% bawah ${sup:.4f}"}
                break
    if result is None:
        for res in reversed(snr["resistances"]):
            depth = (h - res) / close
            if h > res and close < res and depth >= FALSE_BREAK_MIN:
                result = {"signal":"DOWN","pattern":"FALSE_BREAK","wick_pct":None,"body_pct":None,
                          "lvl":res,"lvl_type":"Resistance",
                          "extra":f"False Break ↑{depth*100:.2f}% atas ${res:.4f}"}
                break

    if result is None:
        return None

    # Filter RSI
    rsi_ok = (result["signal"]=="UP" and rsi < RSI_UP_MAX) or (result["signal"]=="DOWN" and rsi > RSI_DOWN_MIN)
    if not rsi_ok:
        log.debug(f"  ↳ DITOLAK RSI {rsi:.1f}")
        return None

    # Filter Volume
    if result["pattern"] == "MOMENTUM" and not vol_ok:
        log.debug(f"  ↳ DITOLAK Volume ×{vol_ratio:.2f}")
        return None

    # Filter HTF Bias
    htf_bias = get_htf_bias(symbol)
    if HTF_STRICT:
        htf_ok = (result["signal"]=="UP" and htf_bias=="BULLISH") or (result["signal"]=="DOWN" and htf_bias=="BEARISH")
    else:
        htf_ok = (result["signal"]=="UP" and htf_bias in ("BULLISH","NEUTRAL")) or \
                 (result["signal"]=="DOWN" and htf_bias in ("BEARISH","NEUTRAL"))
    if not htf_ok:
        log.debug(f"  ↳ DITOLAK HTF 1H={htf_bias}")
        return None

    result.update({"candle":c,"rsi":rsi,"vol_ratio":vol_ratio,"vol_ok":vol_ok,"htf_bias":htf_bias})
    return result

# ============================================================
# 🕯️  KONFIRMASI 1M
# ============================================================
def wait_for_confirmation(symbol, signal, ref_price):
    """
    Cek konfirmasi candle 1M dengan urutan prioritas:

    PRIORITAS 1 — POLA V (terkuat, WR tertinggi)
    ─────────────────────────────────────────────
    UP  : harga dip di bawah ref_price dulu (0.05% min),
          lalu bounce naik kembali melampaui ref_price
          → bentuk huruf V = reversal terkonfirmasi

    DOWN: harga spike di atas ref_price dulu (0.05% min),
          lalu turun kembali melampaui ref_price
          → bentuk huruf Λ (V terbalik) = reversal terkonfirmasi

    PRIORITAS 2 — HAMMER / SHOOTING STAR (kuat)
    ─────────────────────────────────────────────
    UP  : muncul candle Hammer di 1M
          (ekor bawah ≥ 65%, close di upper 50%)
          → rejection support = bullish

    DOWN: muncul candle Shooting Star di 1M
          (ekor atas ≥ 65%, close di lower 50%)
          → rejection resistance = bearish

    PRIORITAS 3 — POLA A (paling sering, paling lemah)
    ─────────────────────────────────────────────
    UP  : candle 1M langsung bullish kuat
          (body ≥ 50%, close di upper 70%)
          → momentum langsung searah sinyal

    DOWN: candle 1M langsung bearish kuat
          (body ≥ 50%, close di lower 30%)
          → momentum langsung searah sinyal
    """
    log.info(f"  ⏳ Menunggu konfirmasi 1M (max {CONFIRM_WAIT_SEC}s) | Prioritas: V → Hammer/Star → A")
    start_time = time.time()
    prev_low   = ref_price   # track titik terendah untuk Pola V UP
    prev_high  = ref_price   # track titik tertinggi untuk Pola V DOWN

    while time.time() - start_time < CONFIRM_WAIT_SEC:
        time.sleep(30)
        candles_1m = fetch_candles(symbol, CONFIRM_TF, limit=10)
        if not candles_1m or len(candles_1m) < 2:
            continue

        _, o1, h1, l1, c1, _ = candles_1m[-1]
        range_1m = h1 - l1
        if range_1m < 1e-8:
            continue

        body_r  = abs(c1 - o1) / range_1m
        lo_w    = (min(o1, c1) - l1) / range_1m
        up_w    = (h1 - max(o1, c1)) / range_1m
        close_p = (c1 - l1) / range_1m

        elapsed = int(time.time() - start_time)
        log.debug(
            f"  🕯️  1M [{elapsed}s] O:{o1:.4f} H:{h1:.4f} L:{l1:.4f} C:{c1:.4f} | "
            f"body:{body_r*100:.0f}% lo_w:{lo_w*100:.0f}% up_w:{up_w*100:.0f}%"
        )

        # ══════════════════════════════════════════════════════════════
        # PRIORITAS 1 — POLA V (cek setiap iterasi, bukan hanya pertama)
        # Terus update prev_low/prev_high setiap candle
        # ══════════════════════════════════════════════════════════════
        if signal == "UP":
            # Update titik terendah
            if l1 < prev_low:
                prev_low = l1
                log.debug(f"  ↳ Pola V: new low ${prev_low:.4f}")

            # V terkonfirmasi: ada dip minimal 0.05% lalu close naik kembali
            dip_pct = (ref_price - prev_low) / ref_price
            if dip_pct >= 0.0005 and c1 > ref_price:
                return True, "POLA V", (
                    f"V-reversal ↓dip {dip_pct*100:.3f}% "
                    f"(low ${prev_low:.4f}) → bounce ${c1:.4f}"
                )
        else:
            # Update titik tertinggi
            if h1 > prev_high:
                prev_high = h1
                log.debug(f"  ↳ Pola V(Λ): new high ${prev_high:.4f}")

            # Λ terkonfirmasi: ada spike minimal 0.05% lalu close turun kembali
            spike_pct = (prev_high - ref_price) / ref_price
            if spike_pct >= 0.0005 and c1 < ref_price:
                return True, "POLA V", (
                    f"Λ-reversal ↑spike {spike_pct*100:.3f}% "
                    f"(high ${prev_high:.4f}) → drop ${c1:.4f}"
                )

        # ══════════════════════════════════════════════════════════════
        # PRIORITAS 2 — HAMMER / SHOOTING STAR
        # ══════════════════════════════════════════════════════════════
        if signal == "UP":
            # Hammer: ekor bawah panjang ≥ 65%, close di upper 50%
            if lo_w >= HAMMER_WICK_MIN and close_p >= 0.50:
                return True, "HAMMER", (
                    f"Hammer 1M — ekor bawah {lo_w*100:.0f}% "
                    f"close pos {close_p*100:.0f}%"
                )
        else:
            # Shooting Star: ekor atas panjang ≥ 65%, close di lower 50%
            if up_w >= HAMMER_WICK_MIN and close_p <= 0.50:
                return True, "SHOOTING STAR", (
                    f"Shooting Star 1M — ekor atas {up_w*100:.0f}% "
                    f"close pos {close_p*100:.0f}%"
                )

        # ══════════════════════════════════════════════════════════════
        # PRIORITAS 3 — POLA A (immediate breakout)
        # Hanya dicek jika Pola V dan Hammer/Star tidak terpenuhi
        # ══════════════════════════════════════════════════════════════
        if signal == "UP":
            # Candle 1M langsung bullish kuat
            if c1 > o1 and close_p >= CONFIRM_CLOSE_POS and body_r >= CONFIRM_BODY_MIN:
                return True, "POLA A", (
                    f"Breakout bullish — body {body_r*100:.0f}% "
                    f"close pos {close_p*100:.0f}%"
                )
        else:
            # Candle 1M langsung bearish kuat
            if c1 < o1 and close_p <= (1 - CONFIRM_CLOSE_POS) and body_r >= CONFIRM_BODY_MIN:
                return True, "POLA A", (
                    f"Breakout bearish — body {body_r*100:.0f}% "
                    f"close pos {close_p*100:.0f}%"
                )

    return False, "TIMEOUT", "Tidak ada konfirmasi dalam waktu yang ditentukan"

# ============================================================
# 📨  TELEGRAM
# ============================================================
def send_telegram(message):
    if BACKTEST_MODE:
        log.info(f"[BACKTEST] MSG:\n{message}\n")
        return True
    if TELEGRAM_BOT_TOKEN == "your_token_here":
        log.warning("Telegram token belum dikonfigurasi.")
        return False
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT_ID,"text":message,
                      "parse_mode":"HTML","disable_web_page_preview":True},
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            log.warning(f"  ⚠️ Telegram gagal (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

# ============================================================
# 🏗️  BUILD MESSAGES
# ============================================================
def build_presignal_message(sig, name, open_ts_ms):
    ts, o, h, l, c, _ = sig["candle"]
    dt_str    = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    arrow     = "🐂 UP" if sig["signal"]=="UP" else "🐻 DOWN"
    poly_link = get_polymarket_link(name, open_ts_ms)
    we        = get_poly_window_end_utc(open_ts_ms)
    lvl_str   = f"${sig['lvl']:,.6f}" if sig["lvl"] else "N/A"
    htf_icon  = "📈" if sig["htf_bias"]=="BULLISH" else ("📉" if sig["htf_bias"]=="BEARISH" else "➡️")

    return (
        f"👁️ <b>PRE-SIGNAL [{sig['pattern']}] — {arrow} — [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Candle    : {dt_str} (UTC)\n"
        f"📊 Coin      : {name}\n"
        f"📊 OHLC      : O:<code>{o:.4f}</code> H:<code>{h:.4f}</code> "
        f"L:<code>{l:.4f}</code> C:<code>{c:.4f}</code>\n"
        f"📋 Pola      : {sig['extra']}\n"
        f"📌 Level     : {sig['lvl_type']} {lvl_str}\n"
        f"💰 Ref Price : <b>${c:.6f}</b> (close candle = acuan Polymarket)\n"
        f"📊 RSI:{sig['rsi']:.1f} | Vol×{sig['vol_ratio']:.2f} | {htf_icon} 1H {sig['htf_bias']}\n"
        f"🔗 Polymarket: {poly_link}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ <i>Menunggu konfirmasi candle 1M (max {CONFIRM_WAIT_SEC}s)...</i>\n"
        f"🕐 Window tutup: {fmt_et(we)} ET / {fmt_utc(we)} UTC"
    )

def build_signal_message(sig, name, open_ts_ms, confirm_type, confirm_detail):
    ts, o, h, l, c, _ = sig["candle"]
    dt_str    = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    now_str   = datetime.now(tz=timezone.utc).strftime("%H:%M")
    arrow     = "🐂 UP" if sig["signal"]=="UP" else "🐻 DOWN"
    poly_link = get_polymarket_link(name, open_ts_ms)
    we        = get_poly_window_end_utc(open_ts_ms)
    lvl_str   = f"${sig['lvl']:,.6f}" if sig["lvl"] else "N/A"
    htf_icon  = "📈" if sig["htf_bias"]=="BULLISH" else ("📉" if sig["htf_bias"]=="BEARISH" else "➡️")
    cemoji    = {"POLA V":"🔄","POLA A":"⚡","HAMMER":"🔨","SHOOTING STAR":"⭐"}.get(confirm_type,"✅")

    return (
        f"🚨 <b>SIGNAL AKTIF [{sig['pattern']}] — {arrow} — [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Candle    : {dt_str} (UTC)\n"
        f"⏰ Konfirmasi: {now_str} UTC\n"
        f"📊 Coin      : {name}\n"
        f"📊 OHLC      : O:<code>{o:.4f}</code> H:<code>{h:.4f}</code> "
        f"L:<code>{l:.4f}</code> C:<code>{c:.4f}</code>\n"
        f"📋 Pola      : {sig['extra']}\n"
        f"📌 Level     : {sig['lvl_type']} {lvl_str}\n"
        f"📊 RSI:{sig['rsi']:.1f} | Vol×{sig['vol_ratio']:.2f} | {htf_icon} 1H {sig['htf_bias']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Ref Price : <b>${c:.6f}</b>\n"
        f"   <i>(Harga close candle 15M = acuan Polymarket UP/DOWN)</i>\n"
        f"{cemoji} Konfirmasi: <b>{confirm_type}</b> — {confirm_detail}\n"
        f"🔗 Polymarket: {poly_link}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Window tutup: {fmt_et(we)} ET / {fmt_utc(we)} UTC\n"
        f"⏳ <i>Result dikirim tepat saat window ET tutup</i>"
    )

def build_cancelled_message(name, signal, pattern):
    return (
        f"❎ <b>DIBATALKAN [{pattern}] {'UP' if signal=='UP' else 'DOWN'} — {name} [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Tidak ada konfirmasi 1M dalam {CONFIRM_WAIT_SEC} detik.\n"
        f"💤 <i>Sinyal tidak dieksekusi.</i>"
    )

def build_result_message(pending, result_price, now_str):
    """
    FIX v8: Penilaian BENAR/SALAH menggunakan ref_price (close candle 15M)
    bukan entry_price (harga real-time saat konfirmasi).

    Ini sesuai cara Polymarket:
    UP  menang jika harga akhir window > close candle 15M sebelumnya
    DOWN menang jika harga akhir window < close candle 15M sebelumnya
    """
    ref_price  = pending["ref_price"]    # close candle 15M = acuan Polymarket
    signal     = pending["signal"]
    pattern    = pending["pattern"]

    price_diff = result_price - ref_price
    pct_change = (price_diff / ref_price * 100) if ref_price > 0 else 0
    diff_sign  = "+" if price_diff > 0 else ""

    # Penilaian sesuai Polymarket
    is_correct = (result_price > ref_price) if signal == "UP" else (result_price < ref_price)
    direction  = "⬆️ Naik" if price_diff > 0 else "⬇️ Turun"
    verdict    = "✅ <b>BENAR</b>" if is_correct else "❌ <b>SALAH</b>"
    emoji      = "🎯" if is_correct else "💔"
    desc       = "Prediksi tepat! Harga bergerak sesuai sinyal." if is_correct else "Prediksi meleset. Harga bergerak berlawanan."

    now_dt  = datetime.strptime(now_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    now_et  = (now_dt + ET_OFFSET).strftime("%Y-%m-%d %H:%M")

    return (
        f"{emoji} <b>HASIL [{pattern}] — {verdict} — [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Coin        : {pending['name']}\n"
        f"📌 Sinyal      : <b>{'🐂 UP' if signal=='UP' else '🐻 DOWN'}</b>\n"
        f"🔄 Konfirmasi  : {pending['confirm_type']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Ref Price   : <b>${ref_price:.6f}</b>\n"
        f"   <i>(Close candle 15M = acuan Polymarket)</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Window End  : {now_et} ET / {now_str} UTC\n"
        f"💰 Close Price : <b>${result_price:.6f}</b>\n"
        f"📈 Pergerakan  : {direction} "
        f"<code>{diff_sign}{price_diff:.6f}</code> "
        f"(<code>{diff_sign}{pct_change:.3f}%</code>)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 Verdict     : {verdict}\n"
        f"💬 <i>{desc}</i>\n"
        f"📌 <i>Dinilai vs ref price (close candle 15M)</i>"
    )

# ============================================================
# 📈  DAILY & WEEKLY REPORT
# ============================================================
def build_daily_report(stats_pat, stats_coin):
    now_str  = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    mode_str = " [BACKTEST]" if BACKTEST_MODE else ""
    msg      = f"📊 <b>DAILY REPORT 15M{mode_str} — {now_str}</b>\n"
    msg     += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg     += "\n📋 <b>Per Pola:</b>\n"
    tw, tl   = 0, 0
    for pattern in ["WICK","FALSE_BREAK","MOMENTUM","EXHAUSTION"]:
        s     = stats_pat.get(pattern, {"win":0,"loss":0,"cancelled":0})
        total = s["win"] + s["loss"]
        wr    = (s["win"]/total*100) if total > 0 else 0
        tw   += s["win"]
        tl   += s["loss"]
        msg  += f"  [{pattern}] ✅{s['win']} ❌{s['loss']} ❎{s['cancelled']} | WR:{wr:.1f}%\n"
    msg += "\n🪙 <b>Per Coin:</b>\n"
    for cn, cs in sorted(stats_coin.items()):
        ct  = cs["win"] + cs["loss"]
        cwr = (cs["win"]/ct*100) if ct > 0 else 0
        msg += f"  {cn}: ✅{cs['win']} ❌{cs['loss']} | WR:{cwr:.1f}%\n"
    tt  = tw + tl
    twr = (tw/tt*100) if tt > 0 else 0
    msg += (
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 TOTAL: {tt} sinyal | ✅{tw} ❌{tl}\n"
        f"🎯 Overall WR: {twr:.1f}%\n"
        f"🔧 HTF_STRICT:{HTF_STRICT} | Konfirmasi:{CONFIRM_WAIT_SEC}s\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Data direset tiap hari.</i>"
    )
    return msg

def build_weekly_report(stats_pat, stats_coin, days):
    msg  = f"📊 <b>WEEKLY REPORT 15M ({days} hari)</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "\n📋 <b>Per Pola:</b>\n"
    tw, tl = 0, 0
    for pattern in ["WICK","FALSE_BREAK","MOMENTUM","EXHAUSTION"]:
        s     = stats_pat.get(pattern, {"win":0,"loss":0,"cancelled":0})
        total = s["win"] + s["loss"]
        wr    = (s["win"]/total*100) if total > 0 else 0
        tw   += s["win"]
        tl   += s["loss"]
        msg  += f"  [{pattern}] ✅{s['win']} ❌{s['loss']} | WR:{wr:.1f}%\n"
    msg += "\n🪙 <b>Per Coin:</b>\n"
    for cn, cs in sorted(stats_coin.items()):
        ct  = cs["win"] + cs["loss"]
        cwr = (cs["win"]/ct*100) if ct > 0 else 0
        msg += f"  {cn}: ✅{cs['win']} ❌{cs['loss']} | WR:{cwr:.1f}%\n"
    tt  = tw + tl
    twr = (tw/tt*100) if tt > 0 else 0
    msg += (
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 TOTAL: {tt} sinyal | ✅{tw} ❌{tl}\n"
        f"🎯 Overall WR: {twr:.1f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    return msg

def check_streak(name, pattern, results):
    if len(results) < STREAK_THRESHOLD:
        return None
    recent = results[-STREAK_THRESHOLD:]
    if all(r is True for r in recent):
        return f"🔥 <b>WIN STREAK [{pattern}]</b>\n   {name} — {STREAK_THRESHOLD}x benar berturut!\n   🎯 On fire!"
    if all(r is False for r in recent):
        return f"⚠️ <b>LOSE STREAK [{pattern}]</b>\n   {name} — {STREAK_THRESHOLD}x salah berturut!\n   🛑 Pertimbangkan pause."
    return None

# ============================================================
# ⏱️  TIMING
# ============================================================
def seconds_until_next_15m():
    now     = time.time()
    elapsed = now % 900
    return (900 - elapsed) + 3

# ============================================================
# 🤖  MAIN BOT LOOP
# ============================================================
def run_bot():
    mode_label = "BACKTEST" if BACKTEST_MODE else "LIVE"
    log.info(f"🚀 Polymarket 15M Signal Bot v8 [{mode_label}] AKTIF")
    log.info(f"   Coins      : {', '.join(c['name'] for c in COINS)}")
    log.info(f"   HTF_STRICT : {HTF_STRICT}")
    log.info(f"   Konfirmasi : max {CONFIRM_WAIT_SEC}s")
    log.info(f"   FIX v8     : Ref price = close candle 15M (acuan Polymarket)")

    pending_signals      = []
    daily_stats_pat      = {p:{"win":0,"loss":0,"cancelled":0} for p in ["WICK","FALSE_BREAK","MOMENTUM","EXHAUSTION"]}
    daily_stats_coin     = {c["name"]:{"win":0,"loss":0} for c in COINS}
    weekly_stats_pat     = {p:{"win":0,"loss":0,"cancelled":0} for p in ["WICK","FALSE_BREAK","MOMENTUM","EXHAUSTION"]}
    weekly_stats_coin    = {c["name"]:{"win":0,"loss":0} for c in COINS}
    result_history       = defaultdict(list)
    last_processed       = {}
    daily_report_sent    = None
    weekly_day_count     = 0

    wait = seconds_until_next_15m()
    log.info(f"⏳ Scan pertama dalam {wait:.0f} detik...")
    time.sleep(wait)

    while True:
        try:
            now_utc   = datetime.now(tz=timezone.utc)
            now_ts    = int(time.time())
            today_str = now_utc.strftime("%Y-%m-%d")

            # ── Daily Report ──────────────────────────────────────────────────
            if now_utc.hour == DAILY_REPORT_HOUR and now_utc.minute < 6:
                if daily_report_sent != today_str:
                    send_telegram(build_daily_report(daily_stats_pat, daily_stats_coin))
                    log.info("📊 Daily report dikirim.")

                    weekly_day_count += 1
                    for p in ["WICK","FALSE_BREAK","MOMENTUM","EXHAUSTION"]:
                        for k in ["win","loss","cancelled"]:
                            weekly_stats_pat[p][k] += daily_stats_pat[p].get(k, 0)
                    for cn in daily_stats_coin:
                        for k in ["win","loss"]:
                            weekly_stats_coin[cn][k] += daily_stats_coin[cn].get(k, 0)

                    daily_stats_pat  = {p:{"win":0,"loss":0,"cancelled":0} for p in ["WICK","FALSE_BREAK","MOMENTUM","EXHAUSTION"]}
                    daily_stats_coin = {c["name"]:{"win":0,"loss":0} for c in COINS}
                    daily_report_sent = today_str

                    if weekly_day_count >= 7:
                        send_telegram(build_weekly_report(weekly_stats_pat, weekly_stats_coin, weekly_day_count))
                        log.info("📊 Weekly report dikirim.")
                        weekly_stats_pat  = {p:{"win":0,"loss":0,"cancelled":0} for p in ["WICK","FALSE_BREAK","MOMENTUM","EXHAUSTION"]}
                        weekly_stats_coin = {c["name"]:{"win":0,"loss":0} for c in COINS}
                        weekly_day_count  = 0

            # ── Cek Hasil Pending ─────────────────────────────────────────────
            still_pending = []
            for ps in pending_signals:
                if now_ts < ps["result_ready_ts"]:
                    still_pending.append(ps)
                    continue

                log.info(f"  🏁 Ambil result {ps['name']} [{ps['pattern']}]")
                result_price = fetch_current_price(ps["symbol"])
                if result_price is None:
                    still_pending.append(ps)
                    continue

                now_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                send_telegram(build_result_message(ps, result_price, now_str))

                # FIX v8: bandingkan result_price vs ref_price (close candle 15M)
                is_win  = (result_price > ps["ref_price"]) if ps["signal"]=="UP" else (result_price < ps["ref_price"])
                pattern   = ps["pattern"]
                coin_name = ps["name"]

                if is_win:
                    daily_stats_pat[pattern]["win"]    += 1
                    daily_stats_coin[coin_name]["win"] += 1
                    result_history[pattern].append(True)
                    log.info(f"  ✅ {coin_name} [{pattern}] BENAR (ref:{ps['ref_price']:.4f} → result:{result_price:.4f})")
                else:
                    daily_stats_pat[pattern]["loss"]    += 1
                    daily_stats_coin[coin_name]["loss"] += 1
                    result_history[pattern].append(False)
                    log.info(f"  ❌ {coin_name} [{pattern}] SALAH (ref:{ps['ref_price']:.4f} → result:{result_price:.4f})")

                streak = check_streak(coin_name, pattern, result_history[pattern])
                if streak:
                    send_telegram(streak)

            pending_signals = still_pending

            # ── Scan Semua Coins ──────────────────────────────────────────────
            for coin in COINS:
                key     = coin["symbol"]
                candles = fetch_candles(key, "15m", limit=SNR_LOOKBACK + 15)
                if not candles:
                    continue

                last_open_ts = candles[-1][0]
                if last_processed.get(key) == last_open_ts:
                    continue
                last_processed[key] = last_open_ts

                dt_c = datetime.fromtimestamp(last_open_ts/1000, tz=timezone.utc).strftime("%H:%M")
                log.info(f"🔍 {coin['name']} 15M [{dt_c} UTC]")

                sig = detect_pattern(key, candles)
                if sig is None:
                    log.debug(f"  ↳ {coin['name']} — tidak ada pola valid.")
                    continue

                # ref_price = close candle 15M = acuan Polymarket UP/DOWN
                ref_price = sig["candle"][4]

                log.info(
                    f"  🔔 PRE-SIGNAL {sig['signal']} [{sig['pattern']}] "
                    f"{coin['name']} | ref_price: ${ref_price:.4f}"
                )
                send_telegram(build_presignal_message(sig, coin["name"], last_open_ts))

                confirmed, confirm_type, confirm_detail = wait_for_confirmation(
                    symbol=key, signal=sig["signal"], ref_price=ref_price
                )

                if not confirmed:
                    send_telegram(build_cancelled_message(coin["name"], sig["signal"], sig["pattern"]))
                    daily_stats_pat[sig["pattern"]]["cancelled"] += 1
                    log.info(f"  ❎ {coin['name']} [{sig['pattern']}] DIBATALKAN")
                    continue

                log.info(f"  ✅ Konfirmasi {confirm_type}: {confirm_detail}")
                send_telegram(build_signal_message(
                    sig=sig, name=coin["name"], open_ts_ms=last_open_ts,
                    confirm_type=confirm_type, confirm_detail=confirm_detail,
                ))

                result_ready = get_result_ready_ts(last_open_ts)
                window_end   = get_poly_window_end_utc(last_open_ts)

                log.info(
                    f"  📨 {coin['name']} [{sig['pattern']}] → "
                    f"result {fmt_et(window_end)} ET / {fmt_utc(window_end)} UTC"
                )

                pending_signals.append({
                    "signal":          sig["signal"],
                    "pattern":         sig["pattern"],
                    "symbol":          key,
                    "name":            coin["name"],
                    "ref_price":       ref_price,        # FIX v8: close candle 15M
                    "entry_time":      datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "confirm_type":    confirm_type,
                    "result_ready_ts": result_ready,
                    "window_end_utc":  window_end,
                })

            wait = seconds_until_next_15m()
            log.info(f"⏳ Scan berikutnya dalam {wait:.0f} detik...\n")
            time.sleep(wait)

        except KeyboardInterrupt:
            log.info("🛑 Bot dihentikan. Sampai jumpa! 👋")
            break
        except Exception as e:
            log.error(f"💥 Error: {e}", exc_info=True)
            time.sleep(15)

if __name__ == "__main__":
    run_bot()
