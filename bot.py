# ============================================================
# POLYMARKET MULTI-TIMEFRAME SIGNAL BOT
# Timeframe : 5M & 15M
# Coins     : BTC, ETH, SOL, XRP, DOGE, BNB, HYPE
# Strategi  : Wick Rejection + Momentum Candle + RSI + EMA Filter
# Fitur     : Result Tracker, Daily Report, Streak Detection
# Data      : Binance REST API (tanpa WebSocket/CCXT)
# Fix       : next_candle_ts calculation & result candle matching
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

WICK_RATIO_MIN   = float(os.getenv("WICK_RATIO_MIN",  "0.65"))
SNR_TOLERANCE    = float(os.getenv("SNR_TOLERANCE",   "0.0040"))
BODY_RATIO_MIN   = float(os.getenv("BODY_RATIO_MIN",  "0.40"))
CLOSE_UPPER_MIN  = float(os.getenv("CLOSE_UPPER_MIN", "0.70"))
CLOSE_LOWER_MAX  = float(os.getenv("CLOSE_LOWER_MAX", "0.30"))
VOLUME_MULT      = float(os.getenv("VOLUME_MULT",     "1.3"))

RSI_PERIOD       = 14
EMA_FAST         = 9
EMA_SLOW         = 21
SNR_LOOKBACK     = 30
SNR_PEAK_DIST    = 4
STREAK_THRESHOLD = 3
DAILY_REPORT_HOUR = 0   # 00:00 UTC = 07:00 WIB

BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
LOG_FILE           = "bot.log"

# Interval dalam detik per timeframe
TF_INTERVAL = {
    "5m":  300,
    "15m": 900,
}

# Higher timeframe untuk EMA bias
HIGHER_TF = {
    "5m":  "1h",
    "15m": "4h",
}

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
    Candle terakhir (sedang berjalan) selalu dibuang.
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

        if not isinstance(raw, list):
            log.error(f"❌ Response tidak valid untuk {symbol} {interval}: {raw}")
            return []

        candles = [
            [
                int(row[0]),    # open_time (ms)
                float(row[1]),  # open
                float(row[2]),  # high
                float(row[3]),  # low
                float(row[4]),  # close
                float(row[5]),  # volume
            ]
            for row in raw
        ]

        # Buang candle terakhir (belum closed)
        return candles[:-1]

    except requests.exceptions.RequestException as e:
        log.error(f"❌ Gagal fetch {symbol} {interval}: {e}")
        return []

# ============================================================
# ⏱️  CANDLE TIMESTAMP HELPER
# FIX UTAMA: Hitung next_candle_ts dengan benar
# ============================================================
def get_candle_close_ts(open_ts_ms: int, tf: str) -> int:
    """
    Hitung timestamp PENUTUPAN candle (Unix detik) dari open_ts (ms).
    open_ts_ms = timestamp pembukaan candle dalam milliseconds.
    Contoh: candle 15m buka jam 00:30 → tutup jam 00:45.
    """
    interval_sec = TF_INTERVAL[tf]
    open_ts_sec  = open_ts_ms // 1000
    return open_ts_sec + interval_sec  # waktu tutup = buka + interval

def get_next_open_ts(open_ts_ms: int, tf: str) -> int:
    """
    Hitung timestamp PEMBUKAAN candle BERIKUTNYA (Unix detik).
    Ini adalah waktu di mana kita bisa mengambil result candle.
    +5 detik buffer agar Binance sudah punya data closed.
    """
    return get_candle_close_ts(open_ts_ms, tf) + 5  # +5 detik buffer

# ============================================================
# 📐  INDIKATOR TEKNIKAL
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
    rsi   = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

def calc_ema(closes: list, period: int) -> float:
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    s = pd.Series(closes)
    return float(s.ewm(span=period, adjust=False).mean().iloc[-1])

def get_higher_tf_bias(symbol: str, tf: str) -> str:
    higher  = HIGHER_TF.get(tf, "1h")
    candles = fetch_candles(symbol, higher, limit=50)
    if len(candles) < EMA_SLOW + 5:
        return "NEUTRAL"

    closes = [c[4] for c in candles]
    ema9   = calc_ema(closes, EMA_FAST)
    ema21  = calc_ema(closes, EMA_SLOW)
    last_c = closes[-1]

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
        "supports":    sorted(set(round(v, 6) for v in lows[trough_idx])),
        "resistances": sorted(set(round(v, 6) for v in highs[peak_idx])),
    }

def find_nearest_snr(close: float, direction: str, snr: dict):
    tolerance  = close * SNR_TOLERANCE
    candidates = snr["supports"] if direction == "UP" else snr["resistances"]
    level_type = "Support" if direction == "UP" else "Resistance"

    nearest      = None
    nearest_diff = float("inf")

    for level in candidates:
        diff = abs(close - level)
        if diff <= tolerance and diff < nearest_diff:
            nearest      = level
            nearest_diff = diff

    return nearest, level_type

# ============================================================
# 🔍  SIGNAL ANALYSIS
# ============================================================
def analyze(symbol: str, name: str, tf: str, candles: list) -> dict | None:
    if len(candles) < SNR_LOOKBACK + 5:
        return None

    closed       = candles[-1]
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
    close_pos   = (c - l) / candle_range

    prev_vols = [cd[5] for cd in candles[-6:-1]]
    avg_vol   = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ok    = vol >= VOLUME_MULT * avg_vol

    closes    = [cd[4] for cd in candles]
    rsi       = calc_rsi(closes, RSI_PERIOD)
    htf_bias  = get_higher_tf_bias(symbol, tf)
    snr       = detect_snr(candles)

    signal      = None
    signal_type = None
    nearest_level = None
    level_type    = None
    wick_pct      = None
    body_pct      = None
    filter_log    = []

    # ── Deteksi sinyal UP ──────────────────────────────────────────────────────
    wick_up_ok = lower_ratio >= WICK_RATIO_MIN
    snr_up_lvl, _ = find_nearest_snr(c, "UP", snr)
    snr_up_ok  = snr_up_lvl is not None

    momentum_up = (
        c > o and
        body_ratio >= BODY_RATIO_MIN and
        close_pos >= CLOSE_UPPER_MIN and
        vol_ok
    )

    if wick_up_ok and snr_up_ok:
        signal        = "UP"
        signal_type   = "WICK"
        wick_pct      = lower_ratio * 100
        nearest_level = snr_up_lvl
        level_type    = "Support"
    elif momentum_up:
        signal        = "UP"
        signal_type   = "MOMENTUM"
        body_pct      = body_ratio * 100
        nearest_level = snr_up_lvl  # bisa None
        level_type    = "Support"

    # ── Deteksi sinyal DOWN ────────────────────────────────────────────────────
    if signal is None:
        wick_dn_ok = upper_ratio >= WICK_RATIO_MIN
        snr_dn_lvl, _ = find_nearest_snr(c, "DOWN", snr)
        snr_dn_ok  = snr_dn_lvl is not None

        momentum_dn = (
            c < o and
            body_ratio >= BODY_RATIO_MIN and
            close_pos <= CLOSE_LOWER_MAX and
            vol_ok
        )

        if wick_dn_ok and snr_dn_ok:
            signal        = "DOWN"
            signal_type   = "WICK"
            wick_pct      = upper_ratio * 100
            nearest_level = snr_dn_lvl
            level_type    = "Resistance"
        elif momentum_dn:
            signal        = "DOWN"
            signal_type   = "MOMENTUM"
            body_pct      = body_ratio * 100
            snr_dn_lvl2, _ = find_nearest_snr(c, "DOWN", snr)
            nearest_level = snr_dn_lvl2
            level_type    = "Resistance"

    if signal is None:
        return None

    # ── Filter wajib (min 2 dari 3 harus pass) ────────────────────────────────
    filters_passed = 0

    if vol_ok:
        filters_passed += 1
        filter_log.append(f"Volume OK (×{vol / avg_vol:.2f})")
    else:
        filter_log.append(f"Volume LOW (×{vol / avg_vol:.2f})")

    if signal == "UP" and rsi < 50:
        filters_passed += 1
        filter_log.append(f"RSI {rsi:.0f} ✓")
    elif signal == "DOWN" and rsi > 50:
        filters_passed += 1
        filter_log.append(f"RSI {rsi:.0f} ✓")
    else:
        filter_log.append(f"RSI {rsi:.0f} ✗")

    if signal == "UP" and htf_bias == "BULLISH":
        filters_passed += 1
        filter_log.append(f"{HIGHER_TF[tf].upper()} Bullish ✓")
    elif signal == "DOWN" and htf_bias == "BEARISH":
        filters_passed += 1
        filter_log.append(f"{HIGHER_TF[tf].upper()} Bearish ✓")
    else:
        filter_log.append(f"{HIGHER_TF[tf].upper()} {htf_bias} ✗")

    if signal_type == "MOMENTUM":
        filters_passed += 1
        filter_log.append("Momentum Candle ✓")

    if filters_passed < 2:
        log.debug(
            f"  ↳ {name} {tf} {signal} DITOLAK — "
            f"{filters_passed}/3 filter: {filter_log}"
        )
        return None

    return {
        "signal":        signal,
        "signal_type":   signal_type,
        "symbol":        symbol,
        "name":          name,
        "tf":            tf,
        "candle":        closed,       # [ts_ms, o, h, l, c, vol]
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
def get_polymarket_link(name: str, tf: str, open_ts_ms: int) -> str:
    """
    Generate link Polymarket berdasarkan coin, TF, dan candle open timestamp.
    window = timestamp penutupan candle (bukan pembukaan).
    """
    close_ts  = get_candle_close_ts(open_ts_ms, tf)
    coin_slug = name.lower()
    return f"https://polymarket.com/event/{coin_slug}-updown-{tf}-{close_ts}"

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
    tf_label  = sig["tf"].upper()
    poly_link = get_polymarket_link(sig["name"], sig["tf"], ts)

    # Hitung kapan result akan dikirim
    result_time_ts = get_next_open_ts(ts, sig["tf"])
    result_time_str = datetime.fromtimestamp(
        result_time_ts, tz=timezone.utc
    ).strftime("%H:%M")

    if sig["signal"] == "UP":
        header    = f"🚨 <b>UP — EKOR BAWAH PANJANG + MOMENTUM → POTENSI UP! (HIGH CONFIDENCE) [{tf_label}]</b>"
        wick_line = (
            f"📏 Lower Wick Ratio: {sig['wick_pct']:.1f}%"
            if sig["signal_type"] == "WICK"
            else f"📏 Body Ratio: {sig['body_pct']:.1f}%"
        )
    else:
        header    = f"🚨 <b>DOWN — EKOR ATAS PANJANG + MOMENTUM → POTENSI DOWN! (HIGH CONFIDENCE) [{tf_label}]</b>"
        wick_line = (
            f"📏 Upper Wick Ratio: {sig['wick_pct']:.1f}%"
            if sig["signal_type"] == "WICK"
            else f"📏 Body Ratio: {sig['body_pct']:.1f}%"
        )

    lvl_str = f"${sig['nearest_level']:,.6f}" if sig["nearest_level"] else "N/A"
    filters = " + ".join(sig["filter_log"])

    return (
        f"{header}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Time      : {dt_str} (UTC)\n"
        f"📊 Coin      : {sig['name']}\n"
        f"📊 Candle    : O: <code>{o:.6f}</code>  H: <code>{h:.6f}</code>  "
        f"L: <code>{l:.6f}</code>  C: <code>{c:.6f}</code>\n"
        f"{wick_line}\n"
        f"📌 Nearest {sig['level_type']} : {lvl_str} (±{SNR_TOLERANCE*100:.1f}%)\n"
        f"💰 Entry     : <b>${c:.6f}</b>\n"
        f"🔥 Filter Pass: {filters}\n"
        f"🔗 Polymarket: {poly_link}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ <i>Result dikirim jam {result_time_str} UTC "
        f"(setelah candle {tf_label} berikutnya tutup)</i>"
    )

# ============================================================
# 📊  BUILD RESULT MESSAGE
# FIX: Pastikan result candle BERBEDA dari entry candle
# ============================================================
def build_result_message(pending: dict, result_candle: list) -> str:
    ts, o, h, l, c, _ = result_candle
    dt_str      = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    entry_price = pending["entry_price"]
    signal      = pending["signal"]
    tf_label    = pending["tf"].upper()

    price_diff = c - entry_price
    pct_change = (price_diff / entry_price) * 100 if entry_price > 0 else 0
    diff_sign  = "+" if price_diff > 0 else ""

    if signal == "UP":
        is_correct = c > entry_price
        direction  = "⬆️ Naik" if price_diff > 0 else "⬇️ Turun"
    else:
        is_correct = c < entry_price
        direction  = "⬇️ Turun" if price_diff < 0 else "⬆️ Naik"

    if is_correct:
        verdict = "✅ <b>BENAR</b>"
        emoji   = "🎯"
        desc    = "Prediksi tepat! Harga bergerak sesuai sinyal."
    else:
        verdict = "❌ <b>SALAH</b>"
        emoji   = "💔"
        desc    = "Prediksi meleset. Harga bergerak berlawanan."

    return (
        f"{emoji} <b>HASIL SINYAL [{tf_label}] — {verdict}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Coin      : {pending['name']}\n"
        f"📌 Sinyal    : <b>{'🐂 UP' if signal == 'UP' else '🐻 DOWN'}</b>\n"
        f"⏰ Entry     : {pending['entry_time']} (UTC)\n"
        f"💰 Entry Price: <b>${entry_price:.6f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Result    : {dt_str} (UTC)\n"
        f"📊 Result Candle:\n"
        f"   O: <code>{o:.6f}</code>  H: <code>{h:.6f}</code>\n"
        f"   L: <code>{l:.6f}</code>  C: <code>{c:.6f}</code>\n"
        f"📈 Pergerakan: {direction} "
        f"<code>{diff_sign}{price_diff:.6f}</code> "
        f"(<code>{diff_sign}{pct_change:.3f}%</code>)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 Verdict   : {verdict}\n"
        f"💬 <i>{desc}</i>"
    )

# ============================================================
# 📈  DAILY REPORT & STREAK
# ============================================================
def build_daily_report(stats: dict) -> str:
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    msg     = f"📊 <b>DAILY REPORT — {now_str} (07:00 WIB)</b>\n"
    msg    += "━━━━━━━━━━━━━━━━━━━━━━\n"

    for tf in TIMEFRAMES:
        s     = stats.get(tf, {"win": 0, "loss": 0})
        total = s["win"] + s["loss"]
        wr    = (s["win"] / total * 100) if total > 0 else 0
        msg  += (
            f"\n⏱️ <b>Timeframe {tf.upper()}</b>\n"
            f"   ✅ Benar  : {s['win']}\n"
            f"   ❌ Salah  : {s['loss']}\n"
            f"   📊 Total  : {total} sinyal\n"
            f"   🎯 Winrate: {wr:.1f}%\n"
        )

    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⚠️ <i>Data direset tiap hari. Test selama seminggu!</i>"
    return msg

def check_streak_alert(name: str, tf: str, results: list) -> str | None:
    if len(results) < STREAK_THRESHOLD:
        return None

    recent = results[-STREAK_THRESHOLD:]
    if all(r is True for r in recent):
        return (
            f"🔥 <b>WIN STREAK [{tf.upper()}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD} sinyal benar berturut-turut!\n"
            f"   🎯 Strategi sedang on fire!"
        )
    elif all(r is False for r in recent):
        return (
            f"⚠️ <b>LOSE STREAK [{tf.upper()}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD} sinyal salah berturut-turut!\n"
            f"   🛑 Pertimbangkan pause dulu."
        )
    return None

# ============================================================
# ⏱️  TIMING
# ============================================================
def seconds_until_next_5m() -> float:
    """Hitung detik sampai candle 5m berikutnya tutup."""
    now     = time.time()
    elapsed = now % 300
    return (300 - elapsed) + 3  # +3 detik buffer

# ============================================================
# 🤖  MAIN BOT LOOP
# ============================================================
def run_bot() -> None:
    log.info("🚀 Polymarket Multi-TF Signal Bot AKTIF")
    log.info(f"   Coins     : {', '.join(c['name'] for c in COINS)}")
    log.info(f"   Timeframes: {', '.join(TIMEFRAMES)}")
    log.info(f"   Wick Min  : {WICK_RATIO_MIN*100:.0f}% | SNR Tol: ±{SNR_TOLERANCE*100:.1f}%")
    log.info(f"   Daily Report: {DAILY_REPORT_HOUR:02d}:00 UTC (07:00 WIB)")

    # ── State ─────────────────────────────────────────────────────────────────
    # pending_signals: list of dict menunggu result
    # {signal, symbol, name, tf, entry_price, entry_time,
    #  entry_candle_ts_ms, result_ready_ts (unix detik)}
    pending_signals: list[dict] = []

    daily_stats: dict          = {tf: {"win": 0, "loss": 0} for tf in TIMEFRAMES}
    result_history: dict       = defaultdict(list)
    last_processed: dict       = {}  # {(symbol, tf): last_open_ts_ms}
    daily_report_sent_date     = None

    # Tunggu candle 5m pertama
    wait = seconds_until_next_5m()
    log.info(f"⏳ Bot siap — scan pertama dalam {wait:.0f} detik...")
    time.sleep(wait)

    while True:
        try:
            now_utc = datetime.now(tz=timezone.utc)
            now_ts  = int(time.time())

            # ══════════════════════════════════════════════════════════════════
            # 📊 DAILY REPORT
            # ══════════════════════════════════════════════════════════════════
            today_str = now_utc.strftime("%Y-%m-%d")
            if now_utc.hour == DAILY_REPORT_HOUR and now_utc.minute < 6:
                if daily_report_sent_date != today_str:
                    send_telegram(build_daily_report(daily_stats))
                    log.info("📊 Daily report dikirim.")
                    daily_stats            = {tf: {"win": 0, "loss": 0} for tf in TIMEFRAMES}
                    daily_report_sent_date = today_str

            # ══════════════════════════════════════════════════════════════════
            # 🏁 CEK HASIL PENDING SIGNALS
            # FIX: Cek berdasarkan result_ready_ts, bukan candle terakhir
            # ══════════════════════════════════════════════════════════════════
            still_pending = []

            for ps in pending_signals:
                # Belum saatnya cek result
                if now_ts < ps["result_ready_ts"]:
                    remaining = ps["result_ready_ts"] - now_ts
                    log.debug(
                        f"  ⏳ Pending {ps['name']} {ps['tf']} — "
                        f"result dalam {remaining}s"
                    )
                    still_pending.append(ps)
                    continue

                log.info(
                    f"  🏁 Ambil result {ps['name']} {ps['tf']} "
                    f"(entry candle ts: {ps['entry_candle_ts_ms']})"
                )

                # Fetch candle result — ambil beberapa candle terbaru
                result_candles = fetch_candles(ps["symbol"], ps["tf"], limit=5)

                if not result_candles:
                    log.warning(f"  ⚠️ Gagal fetch result candle, retry next cycle.")
                    still_pending.append(ps)
                    continue

                # FIX UTAMA: Cari candle yang open_ts > entry_candle_ts
                # Result candle adalah candle SETELAH entry candle
                result_candle = None
                for can in result_candles:
                    if can[0] > ps["entry_candle_ts_ms"]:
                        result_candle = can
                        break

                if result_candle is None:
                    log.warning(
                        f"  ⚠️ Result candle belum tersedia untuk "
                        f"{ps['name']} {ps['tf']}, retry..."
                    )
                    still_pending.append(ps)
                    continue

                # Kirim result message
                result_msg = build_result_message(ps, result_candle)
                send_telegram(result_msg)

                # Update stats
                entry_price = ps["entry_price"]
                close_price = result_candle[4]
                is_win      = (
                    close_price > entry_price if ps["signal"] == "UP"
                    else close_price < entry_price
                )

                tf = ps["tf"]
                if is_win:
                    daily_stats[tf]["win"] += 1
                    result_history[tf].append(True)
                    log.info(f"  ✅ {ps['name']} {tf} → BENAR (entry:{entry_price:.6f} result:{close_price:.6f})")
                else:
                    daily_stats[tf]["loss"] += 1
                    result_history[tf].append(False)
                    log.info(f"  ❌ {ps['name']} {tf} → SALAH (entry:{entry_price:.6f} result:{close_price:.6f})")

                # Streak check
                streak_msg = check_streak_alert(ps["name"], tf, result_history[tf])
                if streak_msg:
                    send_telegram(streak_msg)

            pending_signals = still_pending

            # ══════════════════════════════════════════════════════════════════
            # 🔍 SCAN SEMUA COINS & TIMEFRAMES
            # ══════════════════════════════════════════════════════════════════
            signals_found = []

            for coin in COINS:
                for tf in TIMEFRAMES:
                    key     = (coin["symbol"], tf)
                    candles = fetch_candles(coin["symbol"], tf, limit=SNR_LOOKBACK + 15)

                    if not candles:
                        continue

                    last_open_ts = candles[-1][0]  # open_ts dalam ms

                    # Skip jika candle ini sudah diproses
                    if last_processed.get(key) == last_open_ts:
                        continue

                    last_processed[key] = last_open_ts

                    dt_c = datetime.fromtimestamp(
                        last_open_ts / 1000, tz=timezone.utc
                    ).strftime("%H:%M")
                    log.info(f"🔍 Scan {coin['name']} {tf} [candle {dt_c} UTC]")

                    sig = analyze(coin["symbol"], coin["name"], tf, candles)

                    if sig is None:
                        log.debug(f"  ↳ {coin['name']} {tf} — Tidak ada sinyal.")
                        continue

                    log.info(
                        f"  ✨ SINYAL {sig['signal']} | "
                        f"{coin['name']} {tf} | {sig['filter_log']}"
                    )
                    signals_found.append(sig)

            # ── Kirim semua sinyal & simpan pending ───────────────────────────
            if signals_found:
                for sig in signals_found:
                    send_telegram(build_signal_message(sig))
                    log.info(f"  📨 Sinyal {sig['name']} {sig['tf']} dikirim.")

                    open_ts_ms    = sig["candle"][0]
                    result_ready  = get_next_open_ts(open_ts_ms, sig["tf"])

                    result_time_str = datetime.fromtimestamp(
                        result_ready, tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M")
                    log.info(
                        f"  ⏳ Result {sig['name']} {sig['tf']} akan dicek "
                        f"jam {result_time_str} UTC"
                    )

                    pending_signals.append({
                        "signal":            sig["signal"],
                        "symbol":            sig["symbol"],
                        "name":              sig["name"],
                        "tf":                sig["tf"],
                        "entry_price":       sig["candle"][4],   # close price
                        "entry_time":        datetime.fromtimestamp(
                            open_ts_ms / 1000, tz=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M"),
                        "entry_candle_ts_ms": open_ts_ms,        # FIX: simpan open_ts asli
                        "result_ready_ts":   result_ready,       # FIX: kapan result siap
                    })
            else:
                log.info("💤 Tidak ada sinyal valid saat ini.")

            # ── Tunggu candle 5m berikutnya ───────────────────────────────────
            wait = seconds_until_next_5m()
            log.info(f"⏳ Scan berikutnya dalam {wait:.0f} detik...\n")
            time.sleep(wait)

        except KeyboardInterrupt:
            log.info("🛑 Bot dihentikan. Sampai jumpa! 👋")
            break

        except Exception as e:
            log.error(f"💥 Error: {e}", exc_info=True)
            log.info("   Retry dalam 15 detik...")
            time.sleep(15)

# ============================================================
# ▶️  ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_bot()
