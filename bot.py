# ============================================================
# POLYMARKET BTC 5-MINUTE NOTIFIER BOT
# Strategi: Teori Ekor (Pinbar Rejection) + Dynamic SNR Filter
# Mode: VERY STRICT | Timeframe: FIXED 5m BTCUSDT
# Data Source: Binance REST API (polling — tanpa WebSocket/CCXT)
# ============================================================

import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from scipy.signal import find_peaks

# ─── Load .env file ────────────────────────────────────────────────────────────
load_dotenv()

# ============================================================
# ⚙️  CONFIGURATION
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "your_token_here")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "your_chat_id_here")

WICK_MULTIPLIER = float(os.getenv("WICK_MULTIPLIER", "2.5"))
WICK_RATIO      = float(os.getenv("WICK_RATIO",      "0.70"))
SNR_TOLERANCE   = float(os.getenv("SNR_TOLERANCE",   "0.0020"))

SYMBOL             = "BTCUSDT"
TIMEFRAME          = "5m"
SNR_LOOKBACK       = 30
SNR_PEAK_DIST      = 4
LOG_FILE           = "btc_notifier.log"
BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"

# ============================================================
# 📋  LOGGING SETUP
# ============================================================
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("BTCNotifier")
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
# 📡  FETCH CANDLES — Binance REST API
# ============================================================
def fetch_candles(limit: int = 100) -> list:
    """
    Fetch closed OHLCV candles dari Binance REST API.
    Candle terakhir (yang masih berjalan) dibuang otomatis.
    """
    try:
        resp = requests.get(
            BINANCE_KLINES_URL,
            params={
                "symbol":   SYMBOL,
                "interval": TIMEFRAME,
                "limit":    limit + 1,
            },
            timeout=15,
        )
        resp.raise_for_status()
        raw = resp.json()

        candles = [
            [
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            ]
            for row in raw
        ]

        # Buang candle terakhir (belum closed)
        return candles[:-1]

    except requests.exceptions.RequestException as e:
        log.error(f"❌ Gagal fetch candles: {e}")
        return []

# ============================================================
# 📊  WICK / PINBAR DETECTION
# ============================================================
def analyze_candle(o: float, h: float, l: float, c: float) -> dict:
    body         = abs(c - o)
    candle_range = h - l
    upper_wick   = h - max(o, c)
    lower_wick   = min(o, c) - l

    if candle_range < 1e-8:
        return {"signal": None}

    body_safe   = body if body > 1e-8 else 1e-8
    upper_ratio = upper_wick / candle_range
    lower_ratio = lower_wick / candle_range
    signal      = None

    # 🐂 Bullish — Ekor bawah panjang
    if lower_wick >= WICK_MULTIPLIER * body_safe and lower_ratio >= WICK_RATIO:
        signal = "BULLISH"
    # 🐻 Bearish — Ekor atas panjang
    elif upper_wick >= WICK_MULTIPLIER * body_safe and upper_ratio >= WICK_RATIO:
        signal = "BEARISH"

    return {
        "signal":       signal,
        "upper_wick":   upper_wick,
        "lower_wick":   lower_wick,
        "upper_ratio":  upper_ratio,
        "lower_ratio":  lower_ratio,
        "body":         body,
        "candle_range": candle_range,
    }

# ============================================================
# 📐  DYNAMIC S/R DETECTION
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
    resistances   = highs[peak_idx].tolist()

    trough_idx, _ = find_peaks(-lows, distance=SNR_PEAK_DIST)
    supports      = lows[trough_idx].tolist()

    return {
        "supports":    sorted(set(round(v, 2) for v in supports)),
        "resistances": sorted(set(round(v, 2) for v in resistances)),
    }

# ============================================================
# 🎯  SNR CONFLUENCE CHECK
# ============================================================
def find_nearest_snr(close: float, signal: str, snr: dict) -> tuple:
    tolerance  = close * SNR_TOLERANCE
    candidates = snr["supports"] if signal == "BULLISH" else snr["resistances"]
    level_type = "Support" if signal == "BULLISH" else "Resistance"

    for level in candidates:
        if abs(close - level) <= tolerance:
            return level, level_type

    return None, None

# ============================================================
# 📨  TELEGRAM NOTIFICATION
# ============================================================
def send_telegram(message: str) -> bool:
    if TELEGRAM_BOT_TOKEN == "your_token_here":
        log.warning("Telegram token belum dikonfigurasi — notifikasi dilewati.")
        return False

    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("✅ Notifikasi Telegram berhasil dikirim.")
        return True
    except requests.exceptions.RequestException as e:
        log.error(f"❌ Gagal kirim Telegram: {e}")
        return False

# ============================================================
# 🔗  POLYMARKET LINK GENERATOR
# ============================================================
def get_polymarket_link(unix_ts: float) -> str:
    window_ts = int(unix_ts // 300) * 300
    return f"https://polymarket.com/event/btc-updown-5m-{window_ts}"

# ============================================================
# 🏗️  BUILD NOTIFICATION MESSAGE
# ============================================================
def build_message(
    signal:        str,
    candle:        list,
    wick_data:     dict,
    nearest_level: float,
    level_type:    str,
) -> str:
    ts, o, h, l, c, _ = candle
    dt_str    = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    poly_link = get_polymarket_link(ts / 1000)

    if signal == "BULLISH":
        header     = "🚨 <b>EKOR BAWAH PANJANG + SNR → POTENSI UP!</b> (HIGH CONFIDENCE)"
        wick_label = "Lower Wick Ratio"
        wick_pct   = f"{wick_data['lower_ratio'] * 100:.1f}%"
        snr_line   = f"Nearest Support   : <b>${nearest_level:,.2f}</b> (±0.2%)"
    else:
        header     = "🚨 <b>EKOR ATAS PANJANG + SNR → POTENSI DOWN!</b> (HIGH CONFIDENCE)"
        wick_label = "Upper Wick Ratio"
        wick_pct   = f"{wick_data['upper_ratio'] * 100:.1f}%"
        snr_line   = f"Nearest Resistance: <b>${nearest_level:,.2f}</b> (±0.2%)"

    return (
        f"{header}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Time      : {dt_str} (UTC)\n"
        f"📊 Candle    : O: <code>{o:,.2f}</code>  H: <code>{h:,.2f}</code>  "
        f"L: <code>{l:,.2f}</code>  C: <code>{c:,.2f}</code>\n"
        f"📏 {wick_label}: <b>{wick_pct}</b>\n"
        f"📌 {snr_line}\n"
        f"💰 Current BTC : <b>${c:,.2f}</b>\n"
        f"🔗 Polymarket 5m: <a href='{poly_link}'>Open Market</a>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Notifier only. DYOR. Not financial advice.</i>"
    )

# ============================================================
# ⏱️  HITUNG WAKTU TUNGGU KE CANDLE BERIKUTNYA
# ============================================================
def seconds_until_next_candle(interval_seconds: int = 300) -> float:
    now       = time.time()
    elapsed   = now % interval_seconds
    remaining = interval_seconds - elapsed
    return remaining + 2  # +2 detik buffer

# ============================================================
# 🤖  MAIN BOT LOOP
# ============================================================
def run_bot() -> None:
    log.info("🚀 BTC 5m Notifier Bot AKTIF (Binance REST | Strict SNR Mode)")
    log.info(f"   Symbol    : {SYMBOL}")
    log.info(f"   Timeframe : {TIMEFRAME} (FIXED)")
    log.info(f"   Wick Mult : {WICK_MULTIPLIER}x | Wick Ratio: {WICK_RATIO*100:.0f}%")
    log.info(f"   SNR Tol   : ±{SNR_TOLERANCE*100:.2f}% | Lookback: {SNR_LOOKBACK} candles")
    log.info("   Mode      : REST API Polling (tanpa WebSocket/CCXT)")

    last_processed_ts = None

    # Tunggu sampai candle pertama tertutup
    wait = seconds_until_next_candle()
    log.info(f"⏳ Menunggu candle 5m berikutnya dalam {wait:.0f} detik...")
    time.sleep(wait)

    while True:
        try:
            # ─── Fetch candles ─────────────────────────────────────────────────
            candles = fetch_candles(limit=SNR_LOOKBACK + 10)

            if not candles or len(candles) < SNR_LOOKBACK:
                log.warning(
                    f"⚠️  Data candle tidak cukup "
                    f"({len(candles) if candles else 0}/{SNR_LOOKBACK}), "
                    f"retry dalam 30s..."
                )
                time.sleep(30)
                continue

            # ─── Ambil candle yang baru closed ─────────────────────────────────
            closed_candle = candles[-1]
            candle_ts     = closed_candle[0]

            if candle_ts == last_processed_ts:
                log.debug("  ↳ Candle sudah diproses. Menunggu candle baru...")
                time.sleep(10)
                continue

            last_processed_ts = candle_ts
            ts, o, h, l, c, v = closed_candle
            dt_str = datetime.fromtimestamp(
                ts / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M")
            log.info(
                f"📈 Candle closed [{dt_str} UTC] | "
                f"O:{o:.2f} H:{h:.2f} L:{l:.2f} C:{c:.2f}"
            )

            # ─── Step 1: Deteksi wick ──────────────────────────────────────────
            wick = analyze_candle(o, h, l, c)

            if wick["signal"] is None:
                log.info("  ↳ Tidak ada sinyal wick. Menunggu candle berikutnya.")
            else:
                log.info(f"  ↳ 🕯️  Wick terdeteksi: {wick['signal']}!")

                # ─── Step 2: Deteksi S/R ───────────────────────────────────────
                snr = detect_snr(candles)
                log.debug(
                    f"  ↳ Supports: {snr['supports'][-3:]} | "
                    f"Resistances: {snr['resistances'][-3:]}"
                )

                # ─── Step 3: Cek SNR confluence ────────────────────────────────
                nearest_level, level_type = find_nearest_snr(
                    c, wick["signal"], snr
                )

                if nearest_level is None:
                    log.info("  ↳ ❌ SNR filter TIDAK lolos — tidak dekat S/R. Skip.")
                else:
                    log.info(
                        f"  ↳ ✅ SNR confluence! "
                        f"Dekat {level_type}: ${nearest_level:,.2f}"
                    )

                    # ─── Step 4: Kirim Telegram ────────────────────────────────
                    message = build_message(
                        signal        = wick["signal"],
                        candle        = closed_candle,
                        wick_data     = wick,
                        nearest_level = nearest_level,
                        level_type    = level_type,
                    )
                    log.info("  ↳ 📨 Mengirim notifikasi Telegram...")
                    send_telegram(message)

            # ─── Tunggu candle berikutnya ──────────────────────────────────────
            wait = seconds_until_next_candle()
            log.info(f"⏳ Candle berikutnya dalam {wait:.0f} detik...")
            time.sleep(wait)

        except KeyboardInterrupt:
            log.info("🛑 Bot dihentikan. Sampai jumpa! 👋")
            break

        except Exception as e:
            log.error(f"💥 Unexpected error: {e}", exc_info=True)
            log.info("   Retry dalam 15 detik...")
            time.sleep(15)

# ============================================================
# ▶️  ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_bot()
