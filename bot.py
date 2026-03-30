# ============================================================
# POLYMARKET 15M SIGNAL BOT v5
# Timeframe  : 15M ONLY
# Coins      : BTC, ETH, SOL, XRP, DOGE, BNB, HYPE
# Pola Sinyal: 1) Wick Rejection
#              2) False Break (Liquidity Grab)
#              3) Momentum Candle
# Konfirmasi : Pola V / Pola A / Hammer-Shooting Star (1M)
#              Tunggu max 5 menit setelah sinyal
# Filter     : RSI, Volume, S/R (tanpa session filter)
# Penilaian  : Harga real-time tepat saat window ET tutup
# Target     : 8-12 sinyal/hari | WR 65-70%
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
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "your_token_here")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "your_chat_id_here")

# ── Wick pattern ──────────────────────────────────────────────────────────────
WICK_RATIO_MIN     = float(os.getenv("WICK_RATIO_MIN",  "0.68"))  # ≥ 68%

# ── False Break pattern ───────────────────────────────────────────────────────
# Spike harus menembus S/R minimal sekian persen lalu close balik
FALSE_BREAK_MIN    = float(os.getenv("FALSE_BREAK_MIN", "0.0005")) # 0.05% spike

# ── Momentum candle ───────────────────────────────────────────────────────────
BODY_RATIO_MIN     = float(os.getenv("BODY_RATIO_MIN",  "0.50"))  # body ≥ 50%
CLOSE_UPPER_MIN    = float(os.getenv("CLOSE_UPPER_MIN", "0.70"))  # close upper 70%
CLOSE_LOWER_MAX    = float(os.getenv("CLOSE_LOWER_MAX", "0.30"))  # close lower 30%

# ── Filter ────────────────────────────────────────────────────────────────────
VOLUME_MULT        = float(os.getenv("VOLUME_MULT",     "1.2"))   # volume ≥ 1.2×
RSI_UP_MAX         = float(os.getenv("RSI_UP_MAX",      "55"))    # RSI < 55 untuk UP
RSI_DOWN_MIN       = float(os.getenv("RSI_DOWN_MIN",    "45"))    # RSI > 45 untuk DOWN
SNR_TOLERANCE      = float(os.getenv("SNR_TOLERANCE",   "0.003")) # ±0.3% S/R
CANDLE_RANGE_MIN   = float(os.getenv("CANDLE_RANGE_MIN","0.0015"))# range ≥ 0.15%

# ── Konfirmasi candle 1M ──────────────────────────────────────────────────────
CONFIRM_WAIT_SEC   = 300   # max tunggu konfirmasi = 5 menit
CONFIRM_TF         = "1m"  # TF konfirmasi

# ── Indikator ─────────────────────────────────────────────────────────────────
RSI_PERIOD         = 14
SNR_LOOKBACK       = 40
SNR_PEAK_DIST      = 5

# ── Report & streak ───────────────────────────────────────────────────────────
STREAK_THRESHOLD   = 3
DAILY_REPORT_HOUR  = 0   # 00:00 UTC = 07:00 WIB

# ── API ───────────────────────────────────────────────────────────────────────
BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
BINANCE_TICKER_URL = "https://data-api.binance.vision/api/v3/ticker/price"
LOG_FILE           = "bot.log"

# ── Polymarket ET timezone ────────────────────────────────────────────────────
# EDT = UTC-4 (Maret-November), EST = UTC-5 (November-Maret)
ET_OFFSET_HOURS    = -4
ET_OFFSET          = timedelta(hours=ET_OFFSET_HOURS)
TF_INTERVAL        = 900  # 15 menit dalam detik

# ============================================================
# 🪙  COINS
# ============================================================
COINS = [
    {"symbol": "BTCUSDT",  "name": "BTC"},
    {"symbol": "ETHUSDT",  "name": "ETH"},
    {"symbol": "SOLUSDT",  "name": "SOL"},
    {"symbol": "XRPUSDT",  "name": "XRP"},
    {"symbol": "DOGEUSDT", "name": "DOGE"},
    {"symbol": "BNBUSDT",  "name": "BNB"},
    {"symbol": "HYPEUSDT", "name": "HYPE"},
]

# ============================================================
# 📋  LOGGING
# ============================================================
def setup_logger() -> logging.Logger:
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
def fetch_candles(symbol: str, interval: str, limit: int = 100) -> list:
    """Fetch closed OHLCV dari Binance. Candle live terakhir dibuang."""
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
        log.error(f"❌ Fetch gagal {symbol} {interval}: {e}")
        return []

def fetch_current_price(symbol: str) -> float | None:
    """Ambil harga terkini dari Binance ticker."""
    try:
        resp = requests.get(
            BINANCE_TICKER_URL,
            params={"symbol": symbol},
            timeout=10,
        )
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception as e:
        log.error(f"❌ Gagal fetch harga {symbol}: {e}")
        return None

# ============================================================
# ⏱️  POLYMARKET ET WINDOW HELPERS
# ============================================================
def get_poly_window_end_utc(open_ts_ms: int) -> int:
    """
    Hitung timestamp UTC (detik) saat window Polymarket 15M TUTUP.
    Polymarket pakai ET timezone — window aligned ke menit di ET.

    Contoh: candle buka 05:15 UTC → ET = 01:15 AM → window ET 01:15-01:30
    → window tutup 01:30 AM ET = 05:30 UTC
    """
    open_ts_sec  = open_ts_ms // 1000
    open_dt_utc  = datetime.fromtimestamp(open_ts_sec, tz=timezone.utc)
    open_dt_et   = open_dt_utc + ET_OFFSET
    et_epoch     = int(open_dt_et.timestamp())
    et_floored   = (et_epoch // TF_INTERVAL) * TF_INTERVAL
    et_window_end = et_floored + TF_INTERVAL
    return et_window_end - int(ET_OFFSET.total_seconds())

def get_result_ready_ts(open_ts_ms: int) -> int:
    """Waktu bot boleh ambil result = window tutup + 10 detik buffer."""
    return get_poly_window_end_utc(open_ts_ms) + 10

def get_polymarket_link(name: str, open_ts_ms: int) -> str:
    window_end = get_poly_window_end_utc(open_ts_ms)
    return f"https://polymarket.com/event/{name.lower()}-updown-15m-{window_end}"

def fmt_utc(ts_sec: int) -> str:
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%H:%M")

def fmt_et(ts_sec: int) -> str:
    dt_et = datetime.fromtimestamp(ts_sec, tz=timezone.utc) + ET_OFFSET
    return dt_et.strftime("%H:%M")

# ============================================================
# 📐  INDIKATOR
# ============================================================
def calc_rsi(closes: list, period: int = 14) -> float:
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

# ============================================================
# 📐  S/R DETECTION
# ============================================================
def detect_snr(candles: list) -> dict:
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

def find_nearest_snr(close: float, direction: str, snr: dict):
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
# 🔍  DETEKSI POLA SINYAL 15M
# ============================================================
def detect_pattern(candles: list) -> dict | None:
    """
    Deteksi 3 pola sinyal dari candle 15M yang baru closed.

    Pola 1 — WICK REJECTION:
      Ekor panjang ≥ 68% dari range + close dekat S/R

    Pola 2 — FALSE BREAK (Liquidity Grab):
      Harga spike menembus S/R lalu close kembali ke sisi berlawanan
      Ini sinyal sangat kuat — market maker grab liquidity lalu reversal

    Pola 3 — MOMENTUM CANDLE:
      Body ≥ 50% + close di upper/lower 30% + volume tinggi
    """
    if len(candles) < SNR_LOOKBACK + 5:
        return None

    c          = candles[-1]
    ts, o, h, l, close, vol = c
    candle_range = h - l

    # Filter candle terlalu kecil (noise)
    if candle_range < 1e-8:
        return None
    if candle_range / close < CANDLE_RANGE_MIN:
        log.debug(f"  ↳ Candle range terlalu kecil ({candle_range/close*100:.3f}%), skip.")
        return None

    body        = abs(close - o)
    upper_wick  = h - max(o, close)
    lower_wick  = min(o, close) - l
    upper_ratio = upper_wick / candle_range
    lower_ratio = lower_wick / candle_range
    body_ratio  = body / candle_range
    close_pos   = (close - l) / candle_range

    # Volume
    prev_vols = [cd[5] for cd in candles[-6:-1]]
    avg_vol   = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ok    = vol >= VOLUME_MULT * avg_vol
    vol_ratio = vol / avg_vol if avg_vol > 0 else 0

    # RSI
    closes = [cd[4] for cd in candles]
    rsi    = calc_rsi(closes, RSI_PERIOD)

    # S/R
    snr = detect_snr(candles)

    # ── Coba semua pola ───────────────────────────────────────────────────────
    result = None

    # ── POLA 1: WICK REJECTION ────────────────────────────────────────────────
    if lower_ratio >= WICK_RATIO_MIN:
        lvl, ltype = find_nearest_snr(close, "UP", snr)
        if lvl is not None:
            result = {
                "signal":     "UP",
                "pattern":    "WICK",
                "wick_pct":   lower_ratio * 100,
                "body_pct":   None,
                "lvl":        lvl,
                "lvl_type":   ltype,
                "extra":      f"Lower Wick {lower_ratio*100:.1f}%",
            }

    if result is None and upper_ratio >= WICK_RATIO_MIN:
        lvl, ltype = find_nearest_snr(close, "DOWN", snr)
        if lvl is not None:
            result = {
                "signal":     "DOWN",
                "pattern":    "WICK",
                "wick_pct":   upper_ratio * 100,
                "body_pct":   None,
                "lvl":        lvl,
                "lvl_type":   ltype,
                "extra":      f"Upper Wick {upper_ratio*100:.1f}%",
            }

    # ── POLA 2: FALSE BREAK (Liquidity Grab) ──────────────────────────────────
    # UP False Break: Low spike MENEMBUS support lalu close KEMBALI di atas support
    if result is None:
        for sup in snr["supports"]:
            spike_below = l < sup  # low tembus bawah support
            close_above = close > sup  # tapi close kembali di atas
            spike_depth = (sup - l) / close  # seberapa dalam spike

            if spike_below and close_above and spike_depth >= FALSE_BREAK_MIN:
                result = {
                    "signal":   "UP",
                    "pattern":  "FALSE_BREAK",
                    "wick_pct": None,
                    "body_pct": None,
                    "lvl":      sup,
                    "lvl_type": "Support",
                    "extra":    f"Spike {spike_depth*100:.2f}% bawah Support ${sup:.4f}",
                }
                break

    # DOWN False Break: High spike MENEMBUS resistance lalu close KEMBALI di bawah
    if result is None:
        for res in reversed(snr["resistances"]):
            spike_above = h > res
            close_below = close < res
            spike_depth = (h - res) / close

            if spike_above and close_below and spike_depth >= FALSE_BREAK_MIN:
                result = {
                    "signal":   "DOWN",
                    "pattern":  "FALSE_BREAK",
                    "wick_pct": None,
                    "body_pct": None,
                    "lvl":      res,
                    "lvl_type": "Resistance",
                    "extra":    f"Spike {spike_depth*100:.2f}% atas Resistance ${res:.4f}",
                }
                break

    # ── POLA 3: MOMENTUM CANDLE ───────────────────────────────────────────────
    if result is None and vol_ok:
        if close > o and body_ratio >= BODY_RATIO_MIN and close_pos >= CLOSE_UPPER_MIN:
            lvl, ltype = find_nearest_snr(close, "UP", snr)
            result = {
                "signal":   "UP",
                "pattern":  "MOMENTUM",
                "wick_pct": None,
                "body_pct": body_ratio * 100,
                "lvl":      lvl,
                "lvl_type": ltype if ltype else "Support",
                "extra":    f"Body {body_ratio*100:.1f}% Vol ×{vol_ratio:.2f}",
            }

        elif close < o and body_ratio >= BODY_RATIO_MIN and close_pos <= CLOSE_LOWER_MAX:
            lvl, ltype = find_nearest_snr(close, "DOWN", snr)
            result = {
                "signal":   "DOWN",
                "pattern":  "MOMENTUM",
                "wick_pct": None,
                "body_pct": body_ratio * 100,
                "lvl":      lvl,
                "lvl_type": ltype if ltype else "Resistance",
                "extra":    f"Body {body_ratio*100:.1f}% Vol ×{vol_ratio:.2f}",
            }

    if result is None:
        return None

    # ── Filter RSI dan Volume (wajib) ─────────────────────────────────────────
    rsi_ok = (
        (result["signal"] == "UP"   and rsi < RSI_UP_MAX) or
        (result["signal"] == "DOWN" and rsi > RSI_DOWN_MIN)
    )
    if not rsi_ok:
        log.debug(f"  ↳ DITOLAK RSI {rsi:.1f} — tidak mendukung {result['signal']}")
        return None

    if not vol_ok and result["pattern"] == "MOMENTUM":
        log.debug(f"  ↳ DITOLAK Volume ×{vol_ratio:.2f} untuk MOMENTUM")
        return None

    result.update({
        "candle":    c,
        "rsi":       rsi,
        "vol_ratio": vol_ratio,
        "vol_ok":    vol_ok,
    })
    return result

# ============================================================
# 🕯️  KONFIRMASI CANDLE 1M (max 5 menit)
# Tiga tipe konfirmasi — cukup salah satu
# ============================================================
def wait_for_confirmation(symbol: str, signal: str, entry_price: float) -> tuple:
    """
    Monitor candle 1M selama max CONFIRM_WAIT_SEC detik.
    Cek 3 tipe konfirmasi setiap 30 detik.

    Returns: (confirmed: bool, confirm_type: str, confirm_detail: str)

    Konfirmasi yang dicek:
    A) Pola V  — harga turun lalu naik (UP) / naik lalu turun (DOWN)
    B) Pola A  — candle 1M pertama langsung close searah sinyal
    C) Hammer  — hammer (UP) atau shooting star (DOWN) di 1M
    """
    log.info(f"  ⏳ Menunggu konfirmasi 1M untuk {signal} (max {CONFIRM_WAIT_SEC}s)...")

    start_time    = time.time()
    prev_low      = entry_price   # untuk deteksi pola V UP
    prev_high     = entry_price   # untuk deteksi pola V DOWN
    first_check   = True

    while time.time() - start_time < CONFIRM_WAIT_SEC:
        time.sleep(30)  # cek setiap 30 detik

        candles_1m = fetch_candles(symbol, CONFIRM_TF, limit=10)
        if not candles_1m:
            continue

        # Ambil beberapa candle 1M terbaru setelah entry
        recent = [c for c in candles_1m if c[0] > entry_price]
        # Lebih tepat: ambil candle 1M yang open setelah sinyal dikirim
        if len(candles_1m) < 2:
            continue

        last_1m    = candles_1m[-1]   # candle 1M terbaru (closed)
        _, o1, h1, l1, c1, v1 = last_1m
        body_1m    = abs(c1 - o1)
        range_1m   = h1 - l1
        body_r_1m  = body_1m / range_1m if range_1m > 1e-8 else 0
        lower_w_1m = (min(o1, c1) - l1) / range_1m if range_1m > 1e-8 else 0
        upper_w_1m = (h1 - max(o1, c1)) / range_1m if range_1m > 1e-8 else 0
        close_pos  = (c1 - l1) / range_1m if range_1m > 1e-8 else 0.5

        # ── Konfirmasi B: Pola A (immediate break) ────────────────────────────
        # Candle 1M pertama langsung close searah sinyal dengan momentum
        if first_check:
            if signal == "UP" and c1 > o1 and close_pos >= 0.65 and body_r_1m >= 0.40:
                return True, "POLA A", f"Candle 1M bullish kuat (body {body_r_1m*100:.0f}%)"
            if signal == "DOWN" and c1 < o1 and close_pos <= 0.35 and body_r_1m >= 0.40:
                return True, "POLA A", f"Candle 1M bearish kuat (body {body_r_1m*100:.0f}%)"
            first_check = False

        # ── Konfirmasi C: Hammer / Shooting Star ──────────────────────────────
        if signal == "UP":
            # Hammer: ekor bawah panjang ≥ 60%, close di upper half
            if lower_w_1m >= 0.60 and close_pos >= 0.50:
                return True, "HAMMER", f"Hammer 1M (lower wick {lower_w_1m*100:.0f}%)"
        else:
            # Shooting Star: ekor atas panjang ≥ 60%, close di lower half
            if upper_w_1m >= 0.60 and close_pos <= 0.50:
                return True, "SHOOTING STAR", f"Shooting Star 1M (upper wick {upper_w_1m*100:.0f}%)"

        # ── Konfirmasi A: Pola V ──────────────────────────────────────────────
        # UP Pola V: harga turun dulu (l1 < prev_low) lalu close naik lagi
        if signal == "UP":
            if l1 < prev_low:
                prev_low = l1   # catat titik terendah
            if prev_low < entry_price and c1 > entry_price:
                return True, "POLA V", f"V-shape reversal (low ${prev_low:.4f} → close ${c1:.4f})"

        # DOWN Pola V: harga naik dulu lalu close turun lagi
        else:
            if h1 > prev_high:
                prev_high = h1
            if prev_high > entry_price and c1 < entry_price:
                return True, "POLA V", f"V-shape reversal (high ${prev_high:.4f} → close ${c1:.4f})"

    # Timeout — tidak ada konfirmasi dalam 5 menit
    log.info(f"  ⏰ Timeout konfirmasi — sinyal dibatalkan.")
    return False, "TIMEOUT", "Tidak ada konfirmasi dalam 5 menit"

# ============================================================
# 📨  TELEGRAM
# ============================================================
def send_telegram(message: str) -> bool:
    if TELEGRAM_BOT_TOKEN == "your_token_here":
        log.warning("Telegram token belum dikonfigurasi.")
        return False
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
        log.error(f"❌ Telegram gagal: {e}")
        return False

# ============================================================
# 🏗️  BUILD MESSAGES
# ============================================================
def build_presignal_message(
    sig:    dict,
    name:   str,
    ts_ms:  int,
) -> str:
    """
    Pesan PRE-SIGNAL: dikirim saat pola terdeteksi,
    SEBELUM konfirmasi candle 1M.
    """
    ts, o, h, l, c, _ = sig["candle"]
    dt_str    = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    arrow     = "🐂 UP" if sig["signal"] == "UP" else "🐻 DOWN"
    pattern   = sig["pattern"]
    poly_link = get_polymarket_link(name, ts_ms)

    window_end = get_poly_window_end_utc(ts_ms)
    win_et     = fmt_et(window_end)
    win_utc    = fmt_utc(window_end)

    lvl_str = f"${sig['lvl']:,.6f}" if sig["lvl"] else "N/A"

    return (
        f"👁️ <b>PRE-SIGNAL [{pattern}] — {arrow} — [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Time    : {dt_str} (UTC)\n"
        f"📊 Coin    : {name}\n"
        f"📊 Candle  : O: <code>{o:.6f}</code>  H: <code>{h:.6f}</code>  "
        f"L: <code>{l:.6f}</code>  C: <code>{c:.6f}</code>\n"
        f"📌 {sig['lvl_type']} : {lvl_str}\n"
        f"📋 Detail  : {sig['extra']}\n"
        f"📊 RSI     : {sig['rsi']:.1f} | Vol ×{sig['vol_ratio']:.2f}\n"
        f"🔗 Polymarket: {poly_link}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ <i>Menunggu konfirmasi candle 1M (max 5 menit)...</i>\n"
        f"🕐 Window tutup: {win_et} ET / {win_utc} UTC"
    )

def build_signal_message(
    sig:          dict,
    name:         str,
    ts_ms:        int,
    confirm_type: str,
    confirm_detail: str,
    entry_price:  float,
) -> str:
    """Pesan SIGNAL AKTIF setelah konfirmasi terpenuhi."""
    ts, o, h, l, c, _ = sig["candle"]
    dt_str    = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    now_str   = datetime.now(tz=timezone.utc).strftime("%H:%M")
    arrow     = "🐂 UP" if sig["signal"] == "UP" else "🐻 DOWN"
    pattern   = sig["pattern"]
    poly_link = get_polymarket_link(name, ts_ms)

    window_end = get_poly_window_end_utc(ts_ms)
    win_et     = fmt_et(window_end)
    win_utc    = fmt_utc(window_end)

    lvl_str = f"${sig['lvl']:,.6f}" if sig["lvl"] else "N/A"

    confirm_emoji = {
        "POLA V":        "🔄",
        "POLA A":        "⚡",
        "HAMMER":        "🔨",
        "SHOOTING STAR": "⭐",
    }.get(confirm_type, "✅")

    return (
        f"🚨 <b>SIGNAL AKTIF [{pattern}] — {arrow} — [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Candle  : {dt_str} (UTC)\n"
        f"⏰ Entry   : {now_str} UTC\n"
        f"📊 Coin    : {name}\n"
        f"📊 Candle  : O: <code>{o:.6f}</code>  H: <code>{h:.6f}</code>  "
        f"L: <code>{l:.6f}</code>  C: <code>{c:.6f}</code>\n"
        f"📌 {sig['lvl_type']} : {lvl_str}\n"
        f"📋 Pola    : {sig['extra']}\n"
        f"📊 RSI     : {sig['rsi']:.1f} | Vol ×{sig['vol_ratio']:.2f}\n"
        f"💰 Entry   : <b>${entry_price:.6f}</b>\n"
        f"{confirm_emoji} Konfirmasi: <b>{confirm_type}</b> — {confirm_detail}\n"
        f"🔗 Polymarket: {poly_link}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Window tutup: {win_et} ET / {win_utc} UTC\n"
        f"⏳ <i>Result dikirim tepat saat window ET tutup</i>"
    )

def build_cancelled_message(name: str, signal: str, pattern: str) -> str:
    """Pesan jika konfirmasi timeout — sinyal dibatalkan."""
    arrow = "UP" if signal == "UP" else "DOWN"
    return (
        f"❎ <b>SINYAL DIBATALKAN [{pattern}] {arrow} — {name} [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Tidak ada konfirmasi candle 1M dalam 5 menit.\n"
        f"💤 <i>Sinyal tidak dieksekusi.</i>"
    )

def build_result_message(pending: dict, result_price: float, now_str: str) -> str:
    """Result berdasarkan harga real-time tepat saat window ET tutup."""
    entry_price = pending["entry_price"]
    signal      = pending["signal"]
    pattern     = pending["pattern"]
    price_diff  = result_price - entry_price
    pct_change  = (price_diff / entry_price) * 100 if entry_price > 0 else 0
    diff_sign   = "+" if price_diff > 0 else ""
    is_correct  = (result_price > entry_price) if signal == "UP" else (result_price < entry_price)
    direction   = "⬆️ Naik" if price_diff > 0 else "⬇️ Turun"
    verdict     = "✅ <b>BENAR</b>" if is_correct else "❌ <b>SALAH</b>"
    emoji       = "🎯" if is_correct else "💔"
    desc        = "Prediksi tepat! Harga bergerak sesuai sinyal." if is_correct else "Prediksi meleset. Harga bergerak berlawanan."

    # Tampilkan waktu dalam ET juga
    now_dt_utc = datetime.strptime(now_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    now_et     = (now_dt_utc + ET_OFFSET).strftime("%Y-%m-%d %H:%M")

    return (
        f"{emoji} <b>HASIL [{pattern}] — {verdict} — [15M]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Coin       : {pending['name']}\n"
        f"📌 Sinyal     : <b>{'🐂 UP' if signal == 'UP' else '🐻 DOWN'}</b>\n"
        f"🔄 Konfirmasi : {pending['confirm_type']}\n"
        f"⏰ Entry      : {pending['entry_time']} UTC\n"
        f"💰 Entry Price: <b>${entry_price:.6f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Window End : {now_et} ET / {now_str} UTC\n"
        f"💰 Close Price: <b>${result_price:.6f}</b>\n"
        f"📈 Pergerakan : {direction} "
        f"<code>{diff_sign}{price_diff:.6f}</code> "
        f"(<code>{diff_sign}{pct_change:.3f}%</code>)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 Verdict    : {verdict}\n"
        f"💬 <i>{desc}</i>\n"
        f"📌 <i>Harga diambil tepat saat window ET tutup</i>"
    )

# ============================================================
# 📈  DAILY REPORT — dipisah per pola
# ============================================================
def build_daily_report(stats: dict) -> str:
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    msg     = f"📊 <b>DAILY REPORT 15M — {now_str} (07:00 WIB)</b>\n"
    msg    += "━━━━━━━━━━━━━━━━━━━━━━\n"

    total_w, total_l = 0, 0
    for pattern in ["WICK", "FALSE_BREAK", "MOMENTUM"]:
        s     = stats.get(pattern, {"win": 0, "loss": 0, "cancelled": 0})
        total = s["win"] + s["loss"]
        wr    = (s["win"] / total * 100) if total > 0 else 0
        total_w += s["win"]
        total_l += s["loss"]
        msg += (
            f"\n📌 <b>[{pattern}]</b>\n"
            f"   ✅ Benar     : {s['win']}\n"
            f"   ❌ Salah     : {s['loss']}\n"
            f"   ❎ Dibatalkan: {s['cancelled']}\n"
            f"   📊 Total     : {total} sinyal\n"
            f"   🎯 WR        : {wr:.1f}%\n"
        )

    tt  = total_w + total_l
    twr = (total_w / tt * 100) if tt > 0 else 0
    msg += (
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 TOTAL SEMUA: {tt} sinyal\n"
        f"✅ Benar: {total_w} | ❌ Salah: {total_l}\n"
        f"🎯 Overall WR : {twr:.1f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Data direset tiap hari. Test 1 minggu!</i>"
    )
    return msg

def check_streak(name: str, pattern: str, results: list) -> str | None:
    if len(results) < STREAK_THRESHOLD:
        return None
    recent = results[-STREAK_THRESHOLD:]
    if all(r is True for r in recent):
        return (
            f"🔥 <b>WIN STREAK [{pattern}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD}x benar berturut!\n"
            f"   🎯 On fire!"
        )
    if all(r is False for r in recent):
        return (
            f"⚠️ <b>LOSE STREAK [{pattern}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD}x salah berturut!\n"
            f"   🛑 Pertimbangkan pause."
        )
    return None

# ============================================================
# ⏱️  TIMING
# ============================================================
def seconds_until_next_15m() -> float:
    now     = time.time()
    elapsed = now % 900
    return (900 - elapsed) + 3

# ============================================================
# 🤖  MAIN BOT LOOP
# ============================================================
def run_bot() -> None:
    log.info("🚀 Polymarket 15M Signal Bot v5 AKTIF")
    log.info(f"   Coins      : {', '.join(c['name'] for c in COINS)}")
    log.info(f"   Timeframe  : 15M only")
    log.info(f"   Pola       : WICK | FALSE BREAK | MOMENTUM")
    log.info(f"   Konfirmasi : POLA V / POLA A / HAMMER (max 5 menit)")
    log.info(f"   RSI UP<{RSI_UP_MAX} / DOWN>{RSI_DOWN_MIN} | Vol ≥{VOLUME_MULT}×")
    log.info(f"   SNR Tol    : ±{SNR_TOLERANCE*100:.2f}%")
    log.info(f"   ET Offset  : UTC{ET_OFFSET_HOURS} (EDT)")
    log.info(f"   Target WR  : 65-70% | 8-12 sinyal/hari")

    # ── State ─────────────────────────────────────────────────────────────────
    pending_signals: list[dict] = []

    # Stats per pola
    daily_stats: dict = {
        p: {"win": 0, "loss": 0, "cancelled": 0}
        for p in ["WICK", "FALSE_BREAK", "MOMENTUM"]
    }
    result_history: dict   = defaultdict(list)
    last_processed: dict   = {}
    daily_report_sent_date = None

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
                if daily_report_sent_date != today_str:
                    send_telegram(build_daily_report(daily_stats))
                    log.info("📊 Daily report dikirim.")
                    daily_stats = {
                        p: {"win": 0, "loss": 0, "cancelled": 0}
                        for p in ["WICK", "FALSE_BREAK", "MOMENTUM"]
                    }
                    daily_report_sent_date = today_str

            # ── Cek Hasil Pending Signals ─────────────────────────────────────
            still_pending = []
            for ps in pending_signals:
                if now_ts < ps["result_ready_ts"]:
                    still_pending.append(ps)
                    continue

                log.info(
                    f"  🏁 Ambil result {ps['name']} [{ps['pattern']}] "
                    f"— window ET tutup"
                )

                result_price = fetch_current_price(ps["symbol"])
                if result_price is None:
                    still_pending.append(ps)
                    continue

                now_str = datetime.fromtimestamp(
                    now_ts, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M")

                send_telegram(build_result_message(ps, result_price, now_str))

                is_win  = (
                    result_price > ps["entry_price"]
                    if ps["signal"] == "UP"
                    else result_price < ps["entry_price"]
                )
                pattern = ps["pattern"]

                if is_win:
                    daily_stats[pattern]["win"] += 1
                    result_history[pattern].append(True)
                    log.info(f"  ✅ {ps['name']} [{pattern}] BENAR")
                else:
                    daily_stats[pattern]["loss"] += 1
                    result_history[pattern].append(False)
                    log.info(f"  ❌ {ps['name']} [{pattern}] SALAH")

                streak = check_streak(ps["name"], pattern, result_history[pattern])
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

                dt_c = datetime.fromtimestamp(
                    last_open_ts / 1000, tz=timezone.utc
                ).strftime("%H:%M")
                log.info(f"🔍 {coin['name']} 15M [{dt_c} UTC]")

                sig = detect_pattern(candles)
                if sig is None:
                    log.debug(f"  ↳ {coin['name']} — tidak ada pola.")
                    continue

                log.info(
                    f"  🔔 PRE-SIGNAL {sig['signal']} [{sig['pattern']}] "
                    f"{coin['name']} — menunggu konfirmasi 1M..."
                )

                # Kirim PRE-SIGNAL ke Telegram
                send_telegram(build_presignal_message(sig, coin["name"], last_open_ts))

                # ── Tunggu konfirmasi candle 1M (max 5 menit) ─────────────────
                confirmed, confirm_type, confirm_detail = wait_for_confirmation(
                    symbol       = key,
                    signal       = sig["signal"],
                    entry_price  = sig["candle"][4],
                )

                if not confirmed:
                    # Konfirmasi gagal — kirim pesan pembatalan
                    send_telegram(build_cancelled_message(
                        coin["name"], sig["signal"], sig["pattern"]
                    ))
                    daily_stats[sig["pattern"]]["cancelled"] += 1
                    log.info(f"  ❎ {coin['name']} [{sig['pattern']}] — DIBATALKAN")
                    continue

                # ── Konfirmasi berhasil — ambil harga entry real-time ──────────
                entry_price = fetch_current_price(key)
                if entry_price is None:
                    entry_price = sig["candle"][4]  # fallback ke close candle

                log.info(
                    f"  ✅ Konfirmasi {confirm_type}: {confirm_detail}\n"
                    f"     Entry: ${entry_price:.6f}"
                )

                # Kirim SIGNAL AKTIF
                send_telegram(build_signal_message(
                    sig           = sig,
                    name          = coin["name"],
                    ts_ms         = last_open_ts,
                    confirm_type  = confirm_type,
                    confirm_detail = confirm_detail,
                    entry_price   = entry_price,
                ))

                # Simpan ke pending untuk dicek hasilnya
                result_ready = get_result_ready_ts(last_open_ts)
                window_end   = get_poly_window_end_utc(last_open_ts)

                log.info(
                    f"  📨 {coin['name']} [{sig['pattern']}] → "
                    f"result jam {fmt_et(window_end)} ET / {fmt_utc(window_end)} UTC"
                )

                pending_signals.append({
                    "signal":         sig["signal"],
                    "pattern":        sig["pattern"],
                    "symbol":         key,
                    "name":           coin["name"],
                    "entry_price":    entry_price,
                    "entry_time":     datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "confirm_type":   confirm_type,
                    "result_ready_ts": result_ready,
                    "window_end_utc": window_end,
                })

            # ── Tunggu candle 15M berikutnya ──────────────────────────────────
            wait = seconds_until_next_15m()
            log.info(f"⏳ Scan berikutnya dalam {wait:.0f} detik...\n")
            time.sleep(wait)

        except KeyboardInterrupt:
            log.info("🛑 Bot dihentikan. Sampai jumpa! 👋")
            break

        except Exception as e:
            log.error(f"💥 Error: {e}", exc_info=True)
            time.sleep(15)

# ============================================================
# ▶️  ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_bot()
