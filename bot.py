# ============================================================
# POLYMARKET MULTI-TIMEFRAME SIGNAL BOT v3
# Timeframe  : 5M & 15M
# Coins      : BTC, ETH, SOL, XRP, DOGE, BNB, HYPE
# Strategi   : Wick Rejection + Momentum — DIPISAH tracking-nya
# Konfirmasi : Candle 1M (untuk 5M) dan 3M (untuk 15M)
# Penilaian  : Harga akhir window Polymarket
# Target WR  : 70%+
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

WICK_RATIO_MIN     = float(os.getenv("WICK_RATIO_MIN",   "0.72"))   # naik 65%→72%
SNR_TOLERANCE      = float(os.getenv("SNR_TOLERANCE",    "0.0025"))  # turun 0.4%→0.25%
BODY_RATIO_MIN     = float(os.getenv("BODY_RATIO_MIN",   "0.55"))   # naik 40%→55%
CLOSE_UPPER_MIN    = float(os.getenv("CLOSE_UPPER_MIN",  "0.75"))   # naik 70%→75%
CLOSE_LOWER_MAX    = float(os.getenv("CLOSE_LOWER_MAX",  "0.25"))   # turun 30%→25%
VOLUME_MULT        = float(os.getenv("VOLUME_MULT",      "1.5"))    # naik 1.3×→1.5×

CONFIRM_CANDLES    = 2     # candle konfirmasi 1M/3M harus searah
RSI_PERIOD         = 14
RSI_UP_MAX         = 45    # strict: RSI < 45 untuk UP
RSI_DOWN_MIN       = 55    # strict: RSI > 55 untuk DOWN
EMA_FAST           = 9
EMA_SLOW           = 21
SNR_LOOKBACK       = 40    # naik 30→40
SNR_PEAK_DIST      = 5     # naik 4→5
STREAK_THRESHOLD   = 3
DAILY_REPORT_HOUR  = 0     # 00:00 UTC = 07:00 WIB

BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
LOG_FILE           = "bot.log"

COINS = [
    {"symbol": "BTCUSDT",  "name": "BTC"},
    {"symbol": "ETHUSDT",  "name": "ETH"},
    {"symbol": "SOLUSDT",  "name": "SOL"},
    {"symbol": "XRPUSDT",  "name": "XRP"},
    {"symbol": "DOGEUSDT", "name": "DOGE"},
    {"symbol": "BNBUSDT",  "name": "BNB"},
    {"symbol": "HYPEUSDT", "name": "HYPE"},
]

TIMEFRAMES  = ["5m", "15m"]
TF_INTERVAL = {"5m": 300, "15m": 900}
CONFIRM_TF  = {"5m": "1m", "15m": "3m"}
HIGHER_TF   = {"5m": "1h", "15m": "4h"}

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
# 📡  FETCH CANDLES
# ============================================================
def fetch_candles(symbol: str, interval: str, limit: int = 100) -> list:
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

# ============================================================
# ⏱️  TIMESTAMP HELPERS
# ============================================================
def get_candle_close_ts(open_ts_ms: int, tf: str) -> int:
    return (open_ts_ms // 1000) + TF_INTERVAL[tf]

def get_result_ready_ts(open_ts_ms: int, tf: str) -> int:
    return get_candle_close_ts(open_ts_ms, tf) + 8

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

def calc_ema(closes: list, period: int) -> float:
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    return float(pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1])

def get_higher_tf_bias(symbol: str, tf: str) -> str:
    candles = fetch_candles(symbol, HIGHER_TF[tf], limit=60)
    if len(candles) < EMA_SLOW + 5:
        return "NEUTRAL"
    closes = [c[4] for c in candles]
    ema9   = calc_ema(closes, EMA_FAST)
    ema21  = calc_ema(closes, EMA_SLOW)
    last_c = closes[-1]
    if last_c > ema9 and last_c > ema21:
        return "BULLISH"
    if last_c < ema9 and last_c < ema21:
        return "BEARISH"
    return "NEUTRAL"

# ============================================================
# 🕯️  KONFIRMASI CANDLE TF LEBIH RENDAH
# Kunci utama untuk WR 70%+
# 5M  → cek 2 candle 1M terakhir harus searah
# 15M → cek 2 candle 3M terakhir harus searah
# ============================================================
def get_confirmation(symbol: str, tf: str, direction: str) -> tuple:
    confirm_interval = CONFIRM_TF[tf]
    candles = fetch_candles(symbol, confirm_interval, limit=CONFIRM_CANDLES + 3)
    if len(candles) < CONFIRM_CANDLES:
        return False, f"Data {confirm_interval} tidak cukup"
    recent = candles[-CONFIRM_CANDLES:]
    if direction == "UP":
        confirmed_count = sum(1 for c in recent if c[4] >= c[1])
    else:
        confirmed_count = sum(1 for c in recent if c[4] <= c[1])
    is_confirmed = confirmed_count >= CONFIRM_CANDLES
    reason = (
        f"{confirm_interval.upper()} Konfirmasi ✓ ({confirmed_count}/{CONFIRM_CANDLES})"
        if is_confirmed
        else f"{confirm_interval.upper()} Tidak Konfirmasi ✗ ({confirmed_count}/{CONFIRM_CANDLES})"
    )
    return is_confirmed, reason

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
# 🔍  SIGNAL ANALYSIS — SEMUA filter wajib pass untuk WR 70%+
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
    vol_ratio = vol / avg_vol if avg_vol > 0 else 0

    closes = [cd[4] for cd in candles]
    rsi    = calc_rsi(closes, RSI_PERIOD)
    snr    = detect_snr(candles)

    signal        = None
    signal_type   = None
    nearest_level = None
    level_type    = None
    wick_pct      = None
    body_pct      = None

    # ── Deteksi pola ──────────────────────────────────────────────────────────
    if lower_ratio >= WICK_RATIO_MIN:
        snr_lvl, _ = find_nearest_snr(c, "UP", snr)
        if snr_lvl is not None:
            signal, signal_type = "UP", "WICK"
            wick_pct, nearest_level, level_type = lower_ratio * 100, snr_lvl, "Support"

    if signal is None and upper_ratio >= WICK_RATIO_MIN:
        snr_lvl, _ = find_nearest_snr(c, "DOWN", snr)
        if snr_lvl is not None:
            signal, signal_type = "DOWN", "WICK"
            wick_pct, nearest_level, level_type = upper_ratio * 100, snr_lvl, "Resistance"

    if signal is None and c > o and body_ratio >= BODY_RATIO_MIN and close_pos >= CLOSE_UPPER_MIN and vol_ok:
        snr_lvl, _ = find_nearest_snr(c, "UP", snr)
        signal, signal_type = "UP", "MOMENTUM"
        body_pct, nearest_level, level_type = body_ratio * 100, snr_lvl, "Support"

    if signal is None and c < o and body_ratio >= BODY_RATIO_MIN and close_pos <= CLOSE_LOWER_MAX and vol_ok:
        snr_lvl, _ = find_nearest_snr(c, "DOWN", snr)
        signal, signal_type = "DOWN", "MOMENTUM"
        body_pct, nearest_level, level_type = body_ratio * 100, snr_lvl, "Resistance"

    if signal is None:
        return None

    filter_log = []

    # Filter 1: Volume (wajib untuk MOMENTUM)
    if signal_type == "MOMENTUM" and not vol_ok:
        log.debug(f"  ↳ {name} {tf} DITOLAK — Volume ×{vol_ratio:.2f}")
        return None
    filter_log.append(f"Vol ×{vol_ratio:.2f} {'✓' if vol_ok else ''}")

    # Filter 2: RSI strict (WAJIB)
    rsi_ok = (signal == "UP" and rsi < RSI_UP_MAX) or (signal == "DOWN" and rsi > RSI_DOWN_MIN)
    if not rsi_ok:
        log.debug(f"  ↳ {name} {tf} DITOLAK — RSI {rsi:.1f}")
        return None
    filter_log.append(f"RSI {rsi:.0f} ✓")

    # Filter 3: Higher TF bias (WAJIB)
    htf_bias = get_higher_tf_bias(symbol, tf)
    htf_ok   = (signal == "UP" and htf_bias == "BULLISH") or (signal == "DOWN" and htf_bias == "BEARISH")
    if not htf_ok:
        log.debug(f"  ↳ {name} {tf} DITOLAK — {HIGHER_TF[tf]} {htf_bias}")
        return None
    filter_log.append(f"{HIGHER_TF[tf].upper()} {htf_bias} ✓")

    # Filter 4: Konfirmasi candle TF lebih rendah (WAJIB — kunci WR 70%+)
    confirmed, confirm_reason = get_confirmation(symbol, tf, signal)
    if not confirmed:
        log.debug(f"  ↳ {name} {tf} DITOLAK — {confirm_reason}")
        return None
    filter_log.append(confirm_reason)

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
        "vol_ratio":     vol_ratio,
    }

# ============================================================
# 🔗  POLYMARKET LINK
# ============================================================
def get_polymarket_link(name: str, tf: str, open_ts_ms: int) -> str:
    close_ts = get_candle_close_ts(open_ts_ms, tf)
    return f"https://polymarket.com/event/{name.lower()}-updown-{tf}-{close_ts}"

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
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
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
def build_signal_message(sig: dict) -> str:
    ts, o, h, l, c, _ = sig["candle"]
    dt_str      = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    tf_label    = sig["tf"].upper()
    stype       = sig["signal_type"]
    poly_link   = get_polymarket_link(sig["name"], sig["tf"], ts)
    result_time = datetime.fromtimestamp(
        get_result_ready_ts(ts, sig["tf"]), tz=timezone.utc
    ).strftime("%H:%M")

    arrow = "UP" if sig["signal"] == "UP" else "DOWN"
    wick_word = "EKOR BAWAH" if sig["signal"] == "UP" else "EKOR ATAS"
    header = (
        f"🚨 <b>{arrow} — {'WICK '+wick_word if stype=='WICK' else 'MOMENTUM CANDLE'} "
        f"→ POTENSI {arrow}! (HIGH CONFIDENCE) [{tf_label}][{stype}]</b>"
    )
    wick_line = (
        f"📏 {'Lower' if sig['signal']=='UP' else 'Upper'} Wick : {sig['wick_pct']:.1f}%"
        if stype == "WICK"
        else f"📏 Body Ratio : {sig['body_pct']:.1f}%"
    )
    lvl_str  = f"${sig['nearest_level']:,.6f}" if sig["nearest_level"] else "N/A"
    filters  = " + ".join(sig["filter_log"])

    return (
        f"{header}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Time      : {dt_str} (UTC)\n"
        f"📊 Coin      : {sig['name']}\n"
        f"📊 Candle    : O: <code>{o:.6f}</code>  H: <code>{h:.6f}</code>  "
        f"L: <code>{l:.6f}</code>  C: <code>{c:.6f}</code>\n"
        f"{wick_line}\n"
        f"📌 Nearest {sig['level_type']} : {lvl_str} (±{SNR_TOLERANCE*100:.2f}%)\n"
        f"💰 Entry     : <b>${c:.6f}</b>\n"
        f"🔥 Filter    : {filters}\n"
        f"🔗 Polymarket: {poly_link}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ <i>Result jam {result_time} UTC (akhir window {tf_label})</i>"
    )

def build_result_message(pending: dict, result_candle: list) -> str:
    ts, o, h, l, c, _ = result_candle
    dt_str      = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    entry_price = pending["entry_price"]
    signal      = pending["signal"]
    tf_label    = pending["tf"].upper()
    stype       = pending["signal_type"]
    price_diff  = c - entry_price
    pct_change  = (price_diff / entry_price) * 100 if entry_price > 0 else 0
    diff_sign   = "+" if price_diff > 0 else ""
    is_correct  = (c > entry_price) if signal == "UP" else (c < entry_price)
    direction   = "⬆️ Naik" if price_diff > 0 else "⬇️ Turun"
    verdict     = "✅ <b>BENAR</b>" if is_correct else "❌ <b>SALAH</b>"
    emoji       = "🎯" if is_correct else "💔"
    desc        = "Prediksi tepat! Harga bergerak sesuai sinyal." if is_correct else "Prediksi meleset. Harga bergerak berlawanan."

    return (
        f"{emoji} <b>HASIL [{tf_label}][{stype}] — {verdict}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Coin      : {pending['name']}\n"
        f"📌 Sinyal    : <b>{'🐂 UP' if signal == 'UP' else '🐻 DOWN'}</b>\n"
        f"⏰ Entry     : {pending['entry_time']} (UTC)\n"
        f"💰 Entry     : <b>${entry_price:.6f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Window End: {dt_str} (UTC)\n"
        f"💰 Close     : <b>${c:.6f}</b>\n"
        f"📈 Pergerakan: {direction} "
        f"<code>{diff_sign}{price_diff:.6f}</code> "
        f"(<code>{diff_sign}{pct_change:.3f}%</code>)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 Verdict   : {verdict}\n"
        f"💬 <i>{desc}</i>"
    )

# ============================================================
# 📈  DAILY REPORT — dipisah WICK vs MOMENTUM per TF
# ============================================================
def build_daily_report(stats: dict) -> str:
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    msg     = f"📊 <b>DAILY REPORT — {now_str} (07:00 WIB)</b>\n"
    msg    += "━━━━━━━━━━━━━━━━━━━━━━\n"
    for tf in TIMEFRAMES:
        tw, tl = 0, 0
        msg   += f"\n⏱️ <b>Timeframe {tf.upper()}</b>\n"
        for stype in ["WICK", "MOMENTUM"]:
            s     = stats.get(tf, {}).get(stype, {"win": 0, "loss": 0})
            total = s["win"] + s["loss"]
            wr    = (s["win"] / total * 100) if total > 0 else 0
            tw   += s["win"]
            tl   += s["loss"]
            msg  += (
                f"  📌 [{stype}] ✅{s['win']} ❌{s['loss']} "
                f"({total} sinyal) WR: {wr:.1f}%\n"
            )
        tt   = tw + tl
        twr  = (tw / tt * 100) if tt > 0 else 0
        msg += f"  ─── Total: {tt} sinyal | WR: {twr:.1f}%\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⚠️ <i>Data direset tiap hari. Test 1 minggu!</i>"
    return msg

def check_streak_alert(name: str, tf: str, stype: str, results: list) -> str | None:
    if len(results) < STREAK_THRESHOLD:
        return None
    recent = results[-STREAK_THRESHOLD:]
    if all(r is True for r in recent):
        return (
            f"🔥 <b>WIN STREAK [{tf.upper()}][{stype}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD}x benar berturut!\n"
            f"   🎯 On fire!"
        )
    if all(r is False for r in recent):
        return (
            f"⚠️ <b>LOSE STREAK [{tf.upper()}][{stype}]</b>\n"
            f"   {name} — {STREAK_THRESHOLD}x salah berturut!\n"
            f"   🛑 Pertimbangkan pause."
        )
    return None

# ============================================================
# ⏱️  TIMING
# ============================================================
def seconds_until_next_5m() -> float:
    now     = time.time()
    elapsed = now % 300
    return (300 - elapsed) + 3

# ============================================================
# 🤖  MAIN BOT LOOP
# ============================================================
def run_bot() -> None:
    log.info("🚀 Polymarket Multi-TF Signal Bot v3 AKTIF")
    log.info(f"   Coins     : {', '.join(c['name'] for c in COINS)}")
    log.info(f"   Timeframes: {', '.join(TIMEFRAMES)}")
    log.info(f"   Wick Min  : {WICK_RATIO_MIN*100:.0f}% | SNR: ±{SNR_TOLERANCE*100:.2f}%")
    log.info(f"   RSI UP<{RSI_UP_MAX} / DOWN>{RSI_DOWN_MIN} | Vol ≥{VOLUME_MULT}×")
    log.info(f"   Konfirmasi: 1M untuk 5M | 3M untuk 15M | {CONFIRM_CANDLES} candle")
    log.info(f"   Target WR : 70%+")

    pending_signals: list[dict] = []
    daily_stats: dict = {
        tf: {"WICK": {"win": 0, "loss": 0}, "MOMENTUM": {"win": 0, "loss": 0}}
        for tf in TIMEFRAMES
    }
    result_history: dict       = defaultdict(list)
    last_processed: dict       = {}
    daily_report_sent_date     = None

    wait = seconds_until_next_5m()
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
                        tf: {"WICK": {"win": 0, "loss": 0}, "MOMENTUM": {"win": 0, "loss": 0}}
                        for tf in TIMEFRAMES
                    }
                    daily_report_sent_date = today_str

            # ── Cek Hasil Pending ─────────────────────────────────────────────
            still_pending = []
            for ps in pending_signals:
                if now_ts < ps["result_ready_ts"]:
                    still_pending.append(ps)
                    continue

                log.info(f"  🏁 Result {ps['name']} {ps['tf']} [{ps['signal_type']}]")
                result_candles = fetch_candles(ps["symbol"], ps["tf"], limit=5)

                if not result_candles:
                    still_pending.append(ps)
                    continue

                result_candle = None
                for can in result_candles:
                    if can[0] > ps["entry_candle_ts_ms"]:
                        result_candle = can
                        break

                if result_candle is None:
                    log.warning("  ⚠️ Result candle belum tersedia, retry...")
                    still_pending.append(ps)
                    continue

                send_telegram(build_result_message(ps, result_candle))

                is_win = (
                    result_candle[4] > ps["entry_price"]
                    if ps["signal"] == "UP"
                    else result_candle[4] < ps["entry_price"]
                )
                tf    = ps["tf"]
                stype = ps["signal_type"]

                if is_win:
                    daily_stats[tf][stype]["win"] += 1
                    result_history[(tf, stype)].append(True)
                    log.info(f"  ✅ {ps['name']} {tf} [{stype}] BENAR")
                else:
                    daily_stats[tf][stype]["loss"] += 1
                    result_history[(tf, stype)].append(False)
                    log.info(f"  ❌ {ps['name']} {tf} [{stype}] SALAH")

                streak = check_streak_alert(ps["name"], tf, stype, result_history[(tf, stype)])
                if streak:
                    send_telegram(streak)

            pending_signals = still_pending

            # ── Scan Semua Coins ──────────────────────────────────────────────
            signals_found = []
            for coin in COINS:
                for tf in TIMEFRAMES:
                    key     = (coin["symbol"], tf)
                    candles = fetch_candles(coin["symbol"], tf, limit=SNR_LOOKBACK + 15)
                    if not candles:
                        continue
                    last_open_ts = candles[-1][0]
                    if last_processed.get(key) == last_open_ts:
                        continue
                    last_processed[key] = last_open_ts

                    dt_c = datetime.fromtimestamp(
                        last_open_ts / 1000, tz=timezone.utc
                    ).strftime("%H:%M")
                    log.info(f"🔍 {coin['name']} {tf} [{dt_c}]")

                    sig = analyze(coin["symbol"], coin["name"], tf, candles)
                    if sig is None:
                        continue

                    log.info(f"  ✨ {sig['signal']} [{sig['signal_type']}] {coin['name']} {tf}")
                    signals_found.append(sig)

            if signals_found:
                for sig in signals_found:
                    send_telegram(build_signal_message(sig))
                    open_ts_ms = sig["candle"][0]
                    result_ts  = get_result_ready_ts(open_ts_ms, sig["tf"])
                    pending_signals.append({
                        "signal":             sig["signal"],
                        "signal_type":        sig["signal_type"],
                        "symbol":             sig["symbol"],
                        "name":               sig["name"],
                        "tf":                 sig["tf"],
                        "entry_price":        sig["candle"][4],
                        "entry_time":         datetime.fromtimestamp(
                            open_ts_ms / 1000, tz=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M"),
                        "entry_candle_ts_ms": open_ts_ms,
                        "result_ready_ts":    result_ts,
                    })
                    log.info(
                        f"  📨 {sig['name']} {sig['tf']} [{sig['signal_type']}] → "
                        f"result jam "
                        f"{datetime.fromtimestamp(result_ts, tz=timezone.utc).strftime('%H:%M')} UTC"
                    )
            else:
                log.info("💤 Tidak ada sinyal valid.")

            wait = seconds_until_next_5m()
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
