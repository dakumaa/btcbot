# ============================================================
# POLYMARKET MULTI-TIMEFRAME SIGNAL BOT
# Timeframe : 5M & 15M
# Coins     : BTC, ETH, SOL, XRP, DOGE, BNB, HYPE + more
# Strategi  : Wick Rejection + Momentum Candle + RSI + EMA Filter
# Fitur     : Result Tracker, Daily Report, Streak Detection
# Data      : Binance REST API (tanpa WebSocket/CCXT)
# ============================================================

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

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

# Wick strategy params
WICK_RATIO_MIN     = float(os.getenv("WICK_RATIO_MIN",    "0.65"))   # 65% wick ratio
SNR_TOLERANCE      = float(os.getenv("SNR_TOLERANCE",     "0.0040")) # ±0.4%

# Momentum candle params
BODY_RATIO_MIN     = float(os.getenv("BODY_RATIO_MIN",    "0.40"))   # body ≥ 40% range
CLOSE_UPPER_MIN    = float(os.getenv("CLOSE_UPPER_MIN",   "0.70"))   # close upper 70% (bullish)
CLOSE_LOWER_MAX    = float(os.getenv("CLOSE_LOWER_MAX",   "0.30"))   # close lower 30% (bearish)
VOLUME_MULT        = float(os.getenv("VOLUME_MULT",        "1.3"))   # volume ≥ 1.3× avg

# Filter params
RSI_PERIOD         = 14
EMA_FAST           = 9
EMA_SLOW           = 21
SNR_LOOKBACK       = 30
SNR_PEAK_DIST      = 4

# Streak thresholds untuk alert
STREAK_THRESHOLD   = 3  # alert kalau win/lose streak ≥ 3

# Daily report jam (UTC)
DAILY_REPORT_HOUR  = 0   # jam 00:00 UTC = jam 07:00 WIB

BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
LOG_FILE           = "bot.log"

# ============================================================
# 🪙  COINS & TIMEFRAMES
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

TIMEFRAMES = ["5m", "15m"]

# Higher timeframe untuk EMA bias
HIGHER_TF = {
    "5m":  "1h",
    "15m": "4h",
}

# ============================================================
# 📋  LOGGING SETUP
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
# 📡  FETCH CANDLES
# ============================================================
def fetch_candles(symbol: str, interval: str, limit: int = 100) -> list:
    """
    Fetch closed OHLCV candles dari Binance REST API.
    Returns list of [ts, open, high, low, close, volume].
    """
    try:
        resp = requests.get(
            BINANCE_KLINES_URL,
            params={
                "symbol":   symbol,
                "interval": interval,
                "limit":    limit + 1,
            },
            timeout=15,
        )
        resp.raise_for_status()
        raw = resp.json()

        # Pastikan response adalah list (bukan error dict)
        if not isinstance(raw, list):
            log.error(f"❌ Response tidak valid untuk {symbol} {interval}: {raw}")
            return []

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
        log.error(f"❌ Gagal fetch {symbol} {interval}: {e}")
        return []

# ============================================================
# 📐  INDIKATOR TEKNIKAL
# ============================================================
def calc_rsi(closes: list, period: int = 14) -> float:
    """Hitung RSI dari list harga close."""
    if len(closes) < period + 1:
        return 50.0  # default neutral

    df     = pd.Series(closes)
    delta  = df.diff()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)
    avg_g  = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_l  = loss.ewm(com=period - 1, min_periods=period).mean()

    rs  = avg_g / avg_l.replace(0, 1e-10)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

def calc_ema(closes: list, period: int) -> float:
    """Hitung EMA dari list harga close."""
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    s = pd.Series(closes)
    return float(s.ewm(span=period, adjust=False).mean().iloc[-1])

def get_higher_tf_bias(symbol: str, tf: str) -> str:
    """
    Cek bias Higher Timeframe (1H untuk 5M, 4H untuk 15M).
    Bullish jika close > EMA9 dan close > EMA21.
    Bearish jika close < EMA9 dan close < EMA21.
    Returns: 'BULLISH', 'BEARISH', atau 'NEUTRAL'
    """
    higher = HIGHER_TF.get(tf, "1h")
    candles = fetch_candles(symbol, higher, limit=50)
    if len(candles) < EMA_SLOW + 5:
        return "NEUTRAL"

    closes  = [c[4] for c in candles]
    ema9    = calc_ema(closes, EMA_FAST)
    ema21   = calc_ema(closes, EMA_SLOW)
    last_c  = closes[-1]

    if last_c > ema9 and last_c > ema21:
        return "BULLISH"
    elif last_c < ema9 and last_c < ema21:
        return "BEARISH"
    return "NEUTRAL"

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
        "supports":    sorted(set(round(v, 4) for v in lows[trough_idx])),
        "resistances": sorted(set(round(v, 4) for v in highs[peak_idx])),
    }

def find_nearest_snr(close: float, signal: str, snr: dict):
    tolerance  = close * SNR_TOLERANCE
    candidates = snr["supports"] if signal == "UP" else snr["resistances"]
    level_type = "Support" if signal == "UP" else "Resistance"

    nearest     = None
    nearest_diff = float("inf")

    for level in candidates:
        diff = abs(close - level)
        if diff <= tolerance and diff < nearest_diff:
            nearest      = level
            nearest_diff = diff

    return nearest, level_type

# ============================================================
# 🔍  SIGNAL ANALYSIS — satu coin, satu timeframe
# ============================================================
def analyze(symbol: str, name: str, tf: str, candles: list) -> dict | None:
    """
    Analisa sinyal UP/DOWN untuk satu coin & timeframe.
    Returns dict sinyal atau None jika tidak ada sinyal.
    """
    if len(candles) < SNR_LOOKBACK + 5:
        return None

    closed  = candles[-1]
    ts, o, h, l, c, vol = closed

    candle_range = h - l
    if candle_range < 1e-8:
        return None

    body        = abs(c - o)
    upper_wick  = h - max(o, c)
    lower_wick  = min(o, c) - l
    upper_ratio = upper_wick / candle_range
    lower_ratio = lower_wick / candle_range
    body_ratio  = body / candle_range

    # Posisi close dalam range (0 = bottom, 1 = top)
    close_pos = (c - l) / candle_range

    # Volume average 5 candle sebelumnya
    prev_vols  = [cd[5] for cd in candles[-6:-1]]
    avg_vol    = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ok     = vol >= VOLUME_MULT * avg_vol

    # RSI
    closes = [cd[4] for cd in candles]
    rsi    = calc_rsi(closes, RSI_PERIOD)

    # Higher TF bias
    htf_bias = get_higher_tf_bias(symbol, tf)

    # S/R levels
    snr = detect_snr(candles)

    signal     = None
    reason     = []
    filter_log = []
    nearest_level = None
    level_type    = None
    wick_pct      = None
    body_pct      = None
    signal_type   = None  # "WICK" atau "MOMENTUM"

    # ── UP SIGNAL ─────────────────────────────────────────────────────────────
    # Kondisi 1: Wick bawah panjang + dekat Support
    wick_up_ok = lower_ratio >= WICK_RATIO_MIN
    snr_up_lvl, _ = find_nearest_snr(c, "UP", snr)
    snr_up_ok  = snr_up_lvl is not None

    # Kondisi 2: Momentum candle bullish
    momentum_up = (
        c > o and                          # candle hijau
        body_ratio >= BODY_RATIO_MIN and   # body ≥ 40%
        close_pos >= CLOSE_UPPER_MIN and   # close di upper 70%
        vol_ok                             # volume OK
    )

    if wick_up_ok and snr_up_ok:
        signal      = "UP"
        signal_type = "WICK"
        wick_pct    = lower_ratio * 100
        nearest_level, level_type = snr_up_lvl, "Support"
        reason.append(f"Lower Wick {wick_pct:.1f}% ≥ {WICK_RATIO_MIN*100:.0f}%")
        reason.append(f"Dekat Support ${nearest_level:,.4f}")

    elif momentum_up:
        signal      = "UP"
        signal_type = "MOMENTUM"
        body_pct    = body_ratio * 100
        snr_up_lvl2, _ = find_nearest_snr(c, "UP", snr)
        nearest_level = snr_up_lvl2
        level_type    = "Support"
        reason.append(f"Momentum Bullish Body {body_pct:.1f}%")

    # ── DOWN SIGNAL ───────────────────────────────────────────────────────────
    wick_dn_ok = upper_ratio >= WICK_RATIO_MIN
    snr_dn_lvl, _ = find_nearest_snr(c, "DOWN", snr)
    snr_dn_ok  = snr_dn_lvl is not None

    momentum_dn = (
        c < o and
        body_ratio >= BODY_RATIO_MIN and
        close_pos <= CLOSE_LOWER_MAX and
        vol_ok
    )

    if signal is None:
        if wick_dn_ok and snr_dn_ok:
            signal      = "DOWN"
            signal_type = "WICK"
            wick_pct    = upper_ratio * 100
            nearest_level, level_type = snr_dn_lvl, "Resistance"
            reason.append(f"Upper Wick {wick_pct:.1f}% ≥ {WICK_RATIO_MIN*100:.0f}%")
            reason.append(f"Dekat Resistance ${nearest_level:,.4f}")

        elif momentum_dn:
            signal      = "DOWN"
            signal_type = "MOMENTUM"
            body_pct    = body_ratio * 100
            snr_dn_lvl2, _ = find_nearest_snr(c, "DOWN", snr)
            nearest_level = snr_dn_lvl2
            level_type    = "Resistance"
            reason.append(f"Momentum Bearish Body {body_pct:.1f}%")

    if signal is None:
        return None

    # ── Filter wajib (minimal 2 dari 3 harus pass) ────────────────────────────
    filters_passed = 0

    if vol_ok:
        filters_passed += 1
        filter_log.append(f"Volume OK (×{vol / avg_vol:.2f})")

    if signal == "UP" and rsi < 50:
        filters_passed += 1
        filter_log.append(f"RSI {rsi:.0f} < 50")
    elif signal == "DOWN" and rsi > 50:
        filters_passed += 1
        filter_log.append(f"RSI {rsi:.0f} > 50")
    else:
        filter_log.append(f"RSI {rsi:.0f} ({'terlalu tinggi' if signal == 'UP' else 'terlalu rendah'})")

    if signal == "UP" and htf_bias == "BULLISH":
        filters_passed += 1
        filter_log.append(f"{HIGHER_TF[tf].upper()} Bullish")
    elif signal == "DOWN" and htf_bias == "BEARISH":
        filters_passed += 1
        filter_log.append(f"{HIGHER_TF[tf].upper()} Bearish")
    else:
        filter_log.append(f"{HIGHER_TF[tf].upper()} {htf_bias}")

    if signal_type == "MOMENTUM":
        filter_log.append("Momentum Candle ✓")
        filters_passed += 1  # momentum candle sendiri dihitung sebagai filter

    # Minimal 2 filter harus pass
    if filters_passed < 2:
        log.debug(
            f"  ↳ {name} {tf} {signal} signal DITOLAK — "
            f"hanya {filters_passed}/3 filter pass: {filter_log}"
        )
        return None

    return {
        "signal":        signal,
        "signal_type":   signal_type,
        "symbol":        symbol,
        "name":          name,
        "tf":            tf,
        "candle":        closed,
        "wick_pct":      wick_pct,
        "body_pct":      body_pct,
        "nearest_level": nearest_level,
        "level_type":    level_type,
        "filter_log":    filter_log,
        "rsi":           rsi,
        "htf_bias":      htf_bias,
        "vol_ratio":     vol / avg_vol if avg_vol > 0 else 0,
    }

# ============================================================
# 🔗  POLYMARKET LINK
# ============================================================
def get_polymarket_link(name: str, tf: str, unix_ms: int) -> str:
    """
    Generate Polymarket link berdasarkan coin, timeframe, dan timestamp.
    Format window sesuai timeframe.
    """
    interval = 300 if tf == "5m" else 900
    window   = int((unix_ms / 1000) // interval) * interval
    coin_slug = name.lower()
    return f"https://polymarket.com/event/{coin_slug}-updown-{tf}-{window}"

# ============================================================
# 📨  TELEGRAM
# ============================================================
def send_telegram(message: str) -> bool:
    if TELEGRAM_BOT_TOKEN == "your_token_here":
        log.warning("Telegram token belum dikonfigurasi.")
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
        return True
    except requests.exceptions.RequestException as e:
        log.error(f"❌ Gagal kirim Telegram: {e}")
        return False

# ============================================================
# 🏗️  BUILD SIGNAL MESSAGE
# ============================================================
def build_signal_message(sig: dict) -> str:
    ts, o, h, l, c, _ = sig["candle"]
    dt_str    = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    tf_label  = f"[{sig['tf'].upper()}]"
    poly_link = get_polymarket_link(sig["name"], sig["tf"], ts)

    if sig["signal"] == "UP":
        header     = f"🚨 UP EKOR BAWAH PANJANG + MOMENTUM → POTENSI UP! (HIGH CONFIDENCE) <b>{tf_label}</b>"
        wick_line  = (
            f"📏 Lower Wick Ratio: {sig['wick_pct']:.1f}%"
            if sig["signal_type"] == "WICK"
            else f"📏 Body Ratio: {sig['body_pct']:.1f}%"
        )
    else:
        header     = f"🚨 DOWN EKOR ATAS PANJANG + MOMENTUM → POTENSI DOWN! (HIGH CONFIDENCE) <b>{tf_label}</b>"
        wick_line  = (
            f"📏 Upper Wick Ratio: {sig['wick_pct']:.1f}%"
            if sig["signal_type"] == "WICK"
            else f"📏 Body Ratio: {sig['body_pct']:.1f}%"
        )

    lvl_str = f"${sig['nearest_level']:,.4f}" if sig["nearest_level"] else "N/A"
    filters = " + ".join(sig["filter_log"])

    return (
        f"{header}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Time      : {dt_str} (UTC)\n"
        f"📊 Coin      : {sig['name']}\n"
        f"📊 Candle    : O: <code>{o:,.4f}</code>  H: <code>{h:,.4f}</code>  "
        f"L: <code>{l:,.4f}</code>  C: <code>{c:,.4f}</code>\n"
        f"{wick_line}\n"
        f"📌 Nearest {sig['level_type']} : {lvl_str} (±{SNR_TOLERANCE*100:.1f}%)\n"
        f"💰 Entry     : <b>${c:,.4f}</b>\n"
        f"🔥 Filter Pass: {filters}\n"
        f"🔗 Polymarket: {poly_link}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ <i>Menunggu hasil candle berikutnya...</i>"
    )

# ============================================================
# 📊  BUILD RESULT MESSAGE
# ============================================================
def build_result_message(pending: dict, result_candle: list) -> str:
    ts, o, h, l, c, _ = result_candle
    dt_str      = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    entry_price = pending["entry_price"]
    signal      = pending["signal"]
    price_diff  = c - entry_price
    pct_change  = (price_diff / entry_price) * 100
    diff_sign   = "+" if price_diff > 0 else ""

    if signal == "UP":
        is_correct = c > entry_price
        direction  = "⬆️ Naik" if price_diff > 0 else "⬇️ Turun"
    else:
        is_correct = c < entry_price
        direction  = "⬇️ Turun" if price_diff < 0 else "⬆️ Naik"

    tf_label = f"[{pending['tf'].upper()}]"

    if is_correct:
        verdict = "✅ <b>BENAR</b>"
        emoji   = "🎯"
        desc    = "Prediksi tepat! Harga bergerak sesuai sinyal."
    else:
        verdict = "❌ <b>SALAH</b>"
        emoji   = "💔"
        desc    = "Prediksi meleset. Harga bergerak berlawanan."

    return (
        f"{emoji} <b>HASIL SINYAL {tf_label} — {verdict}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Coin      : {pending['name']}\n"
        f"📌 Sinyal    : <b>{'🐂 UP' if signal == 'UP' else '🐻 DOWN'}</b>\n"
        f"⏰ Entry     : {pending['entry_time']} (UTC)\n"
        f"💰 Entry Price: <b>${entry_price:,.4f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Result    : {dt_str} (UTC)\n"
        f"📊 Result Candle:\n"
        f"   O: <code>{o:,.4f}</code>  H: <code>{h:,.4f}</code>\n"
        f"   L: <code>{l:,.4f}</code>  C: <code>{c:,.4f}</code>\n"
        f"📈 Pergerakan: {direction} "
        f"<code>{diff_sign}{price_diff:,.4f}</code> "
        f"(<code>{diff_sign}{pct_change:.3f}%</code>)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 Verdict   : {verdict}\n"
        f"💬 <i>{desc}</i>"
    )

# ============================================================
# 📈  DAILY REPORT
# ============================================================
def build_daily_report(stats: dict) -> str:
    """
    Build laporan harian untuk semua timeframe.
    stats format: {tf: {"win": int, "loss": int, "streak": int, "streak_type": str}}
    """
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    msg     = f"📊 <b>DAILY REPORT — {now_str} (07:00 WIB)</b>\n"
    msg    += f"━━━━━━━━━━━━━━━━━━━━━━\n"

    for tf in TIMEFRAMES:
        s     = stats.get(tf, {"win": 0, "loss": 0})
        total = s["win"] + s["loss"]
        wr    = (s["win"] / total * 100) if total > 0 else 0

        msg += (
            f"\n⏱️ <b>Timeframe {tf.upper()}</b>\n"
            f"   ✅ Benar : {s['win']}\n"
            f"   ❌ Salah : {s['loss']}\n"
            f"   📊 Total : {total} sinyal\n"
            f"   🎯 Winrate: {wr:.1f}%\n"
        )

    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚠️ <i>Data direset setiap hari. Test selama seminggu!</i>"
    return msg

def check_streak_alert(name: str, tf: str, results: list) -> str | None:
    """
    Cek apakah ada win streak atau lose streak ≥ STREAK_THRESHOLD.
    results: list boolean terbaru [True=win, False=loss]
    """
    if len(results) < STREAK_THRESHOLD:
        return None

    recent = results[-STREAK_THRESHOLD:]

    if all(r is True for r in recent):
        return (
            f"🔥 <b>WIN STREAK ALERT [{tf.upper()}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD} sinyal benar berturut-turut!\n"
            f"   🎯 Strategi sedang on fire!"
        )
    elif all(r is False for r in recent):
        return (
            f"⚠️ <b>LOSE STREAK ALERT [{tf.upper()}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD} sinyal salah berturut-turut!\n"
            f"   🛑 Pertimbangkan untuk pause dulu."
        )
    return None

# ============================================================
# ⏱️  TIMING HELPERS
# ============================================================
def seconds_until_next_candle(tf: str) -> float:
    interval = 300 if tf == "5m" else 900
    now      = time.time()
    elapsed  = now % interval
    return (interval - elapsed) + 2

def get_next_candle_ts(tf: str) -> int:
    """Timestamp UTC dari candle berikutnya (dalam detik)."""
    interval = 300 if tf == "5m" else 900
    now      = int(time.time())
    return ((now // interval) + 1) * interval

def is_daily_report_time() -> bool:
    """Cek apakah sudah waktunya kirim daily report (jam 00:00 UTC / 07:00 WIB)."""
    now = datetime.now(tz=timezone.utc)
    return now.hour == DAILY_REPORT_HOUR and now.minute < 5

# ============================================================
# 🤖  MAIN BOT
# ============================================================
def run_bot() -> None:
    log.info("🚀 Polymarket Multi-TF Signal Bot AKTIF")
    log.info(f"   Coins     : {', '.join(c['name'] for c in COINS)}")
    log.info(f"   Timeframes: {', '.join(TIMEFRAMES)}")
    log.info(f"   Wick Min  : {WICK_RATIO_MIN*100:.0f}% | SNR Tol: ±{SNR_TOLERANCE*100:.1f}%")
    log.info(f"   Volume    : ≥{VOLUME_MULT}× avg | RSI: {RSI_PERIOD} | EMA: {EMA_FAST}/{EMA_SLOW}")
    log.info(f"   Daily Report: {DAILY_REPORT_HOUR:02d}:00 UTC (07:00 WIB)")

    # ── State management ───────────────────────────────────────────────────────
    # pending_signals: list of dict yg menunggu hasil di candle berikutnya
    # Format: {signal, name, tf, entry_price, entry_time, next_candle_ts}
    pending_signals: list[dict] = []

    # Stats harian per timeframe
    # {tf: {"win": 0, "loss": 0}}
    daily_stats: dict = {tf: {"win": 0, "loss": 0} for tf in TIMEFRAMES}

    # History hasil per tf untuk streak detection
    # {tf: [True/False, ...]}
    result_history: dict = defaultdict(list)

    # Tracking candle terakhir yang sudah diproses per coin+tf
    # {(symbol, tf): last_ts}
    last_processed: dict = {}

    # Flag daily report sudah dikirim hari ini
    daily_report_sent_date = None

    # Tunggu candle 5m pertama tertutup
    wait = seconds_until_next_candle("5m")
    log.info(f"⏳ Menunggu scan pertama dalam {wait:.0f} detik...")
    time.sleep(wait)

    while True:
        try:
            now_utc = datetime.now(tz=timezone.utc)

            # ══════════════════════════════════════════════════════════════════
            # 📊 DAILY REPORT — jam 00:00 UTC (07:00 WIB)
            # ══════════════════════════════════════════════════════════════════
            today_str = now_utc.strftime("%Y-%m-%d")
            if is_daily_report_time() and daily_report_sent_date != today_str:
                report_msg = build_daily_report(daily_stats)
                send_telegram(report_msg)
                log.info("📊 Daily report dikirim.")

                # Reset stats harian
                daily_stats = {tf: {"win": 0, "loss": 0} for tf in TIMEFRAMES}
                daily_report_sent_date = today_str

            # ══════════════════════════════════════════════════════════════════
            # 🏁 CEK HASIL PENDING SIGNALS
            # ══════════════════════════════════════════════════════════════════
            now_ts        = int(time.time())
            still_pending = []

            for ps in pending_signals:
                # Hasil siap dicek jika candle berikutnya sudah tertutup
                if now_ts < ps["next_candle_ts"]:
                    still_pending.append(ps)
                    continue

                # Fetch candle result
                interval   = 300 if ps["tf"] == "5m" else 900
                result_can = fetch_candles(ps["symbol"], ps["tf"], limit=5)

                if not result_can:
                    still_pending.append(ps)
                    continue

                # Cari candle yang sesuai dengan next_candle_ts
                target_ts  = ps["next_candle_ts"] * 1000  # ke milliseconds
                matched    = None
                for can in reversed(result_can):
                    if can[0] <= target_ts:
                        matched = can
                        break

                if matched is None:
                    matched = result_can[-1]

                # Kirim hasil
                result_msg = build_result_message(ps, matched)
                send_telegram(result_msg)

                # Update stats
                entry_price = ps["entry_price"]
                close_price = matched[4]
                is_win = (
                    close_price > entry_price if ps["signal"] == "UP"
                    else close_price < entry_price
                )

                tf = ps["tf"]
                if is_win:
                    daily_stats[tf]["win"] += 1
                    result_history[tf].append(True)
                    log.info(f"  ↳ ✅ {ps['name']} {tf} → BENAR")
                else:
                    daily_stats[tf]["loss"] += 1
                    result_history[tf].append(False)
                    log.info(f"  ↳ ❌ {ps['name']} {tf} → SALAH")

                # Cek streak alert
                streak_msg = check_streak_alert(
                    ps["name"], tf, result_history[tf]
                )
                if streak_msg:
                    send_telegram(streak_msg)
                    log.info(f"  ↳ 🔔 Streak alert dikirim untuk {ps['name']} {tf}")

            pending_signals = still_pending

            # ══════════════════════════════════════════════════════════════════
            # 🔍 SCAN SEMUA COINS & TIMEFRAMES
            # ══════════════════════════════════════════════════════════════════
            signals_found = []

            for coin in COINS:
                for tf in TIMEFRAMES:
                    key = (coin["symbol"], tf)

                    # Fetch candles
                    candles = fetch_candles(coin["symbol"], tf, limit=SNR_LOOKBACK + 15)
                    if not candles:
                        continue

                    last_ts = candles[-1][0]

                    # Skip jika candle ini sudah diproses
                    if last_processed.get(key) == last_ts:
                        continue

                    last_processed[key] = last_ts

                    dt_c = datetime.fromtimestamp(
                        last_ts / 1000, tz=timezone.utc
                    ).strftime("%H:%M")
                    log.info(f"🔍 Scan {coin['name']} {tf} [{dt_c} UTC]")

                    # Analisa sinyal
                    sig = analyze(coin["symbol"], coin["name"], tf, candles)

                    if sig is None:
                        log.debug(f"  ↳ {coin['name']} {tf} — Tidak ada sinyal.")
                        continue

                    log.info(
                        f"  ↳ ✨ SINYAL {sig['signal']} ditemukan! "
                        f"{coin['name']} {tf} | "
                        f"Filter: {sig['filter_log']}"
                    )
                    signals_found.append(sig)

            # ── Kirim semua sinyal yang ditemukan ─────────────────────────────
            if signals_found:
                for sig in signals_found:
                    msg = build_signal_message(sig)
                    send_telegram(msg)
                    log.info(f"  ↳ 📨 Sinyal {sig['name']} {sig['tf']} dikirim ke Telegram.")

                    # Simpan ke pending untuk dicek hasilnya
                    ts       = sig["candle"][0]
                    interval = 300 if sig["tf"] == "5m" else 900
                    next_ts  = (int(ts / 1000) // interval + 1) * interval

                    pending_signals.append({
                        "signal":        sig["signal"],
                        "symbol":        sig["symbol"],
                        "name":          sig["name"],
                        "tf":            sig["tf"],
                        "entry_price":   sig["candle"][4],
                        "entry_time":    datetime.fromtimestamp(
                            ts / 1000, tz=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M"),
                        "next_candle_ts": next_ts,
                    })
            else:
                log.info("💤 Tidak ada sinyal valid di semua market saat ini.")

            # ── Tunggu sampai candle 5m berikutnya tertutup ───────────────────
            wait = seconds_until_next_candle("5m")
            log.info(f"⏳ Scan berikutnya dalam {wait:.0f} detik...\n")
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
