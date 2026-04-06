# ============================================================
# POLYMARKET SIGNAL BOT v10
# Coins     : BTC, ETH, BNB
# Timeframe : 15M dan 1H (bersamaan, notif terpisah)
# Strategi  : EXHAUSTION dengan adaptive streak:
#             - ADX < 23 (sideways) → streak 3 candle (+ lanjut 4 jika salah)
#             - ADX ≥ 23 (momentum) → streak 5 candle + konfirmasi candle ke-6
# Odds      : Semua sinyal masuk, tandai jika odds ≤ 45¢
# Report    : Akumulasi Day1-Day7 → Weekly → Reset
#             Kolom: semua sinyal vs sinyal ber-odds
# ============================================================

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# ⚙️  CONFIGURATION
# ============================================================
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "your_token_here")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID",   "your_chat_id_here")

# ── Market condition thresholds ───────────────────────────────────────────────
ADX_SIDEWAYS        = float(os.getenv("ADX_SIDEWAYS",    "23"))  # ADX < 23 = sideways
ADX_PERIOD          = 14
ATR_PERIOD          = 14

# ── Streak levels ─────────────────────────────────────────────────────────────
# Sideways: sinyal di streak 3, lanjut ke 4 jika salah
SIDEWAYS_STREAK_1   = 3
SIDEWAYS_STREAK_2   = 4

# Momentum: sinyal saat 5 candle sama + candle ke-6 sama warna (konfirmasi)
MOMENTUM_STREAK     = 6   # 5 sama + 1 konfirmasi

# ── Odds notification threshold ───────────────────────────────────────────────
ODDS_THRESHOLD      = float(os.getenv("ODDS_THRESHOLD", "0.45"))  # ≤ 45¢ = value

# ── Timeframes ────────────────────────────────────────────────────────────────
TIMEFRAMES = {
    "15m": {"interval_sec": 900,  "label": "15M", "candles_needed": 30},
    "1h":  {"interval_sec": 3600, "label": "1H",  "candles_needed": 30},
}

# ── ET timezone ───────────────────────────────────────────────────────────────
ET_OFFSET_HOURS     = -4
ET_OFFSET           = timedelta(hours=ET_OFFSET_HOURS)

# ── API ───────────────────────────────────────────────────────────────────────
BINANCE_KLINES_URL  = "https://data-api.binance.vision/api/v3/klines"
BINANCE_TICKER_URL  = "https://data-api.binance.vision/api/v3/ticker/price"
POLYMARKET_GAMMA    = "https://gamma-api.polymarket.com/markets"
LOG_FILE            = "bot.log"

MAX_RETRIES         = 3
RETRY_DELAY         = 5
STREAK_ALERT_N      = 3   # alert jika win/lose streak ≥ 3
WEEKLY_DAYS         = 7
DAILY_REPORT_HOUR   = 0   # 00:00 UTC = 07:00 WIB

# ============================================================
# 🪙  COINS
# ============================================================
COINS = [
    {"symbol": "BTCUSDT", "name": "BTC"},
    {"symbol": "ETHUSDT", "name": "ETH"},
    {"symbol": "BNBUSDT", "name": "BNB"},
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
def fetch_candles(symbol: str, interval: str, limit: int = 50) -> list:
    """Fetch closed OHLCV candles dari Binance."""
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
            log.warning(f"  ⚠️ Fetch {symbol} {interval} gagal (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return []

def fetch_current_price(symbol: str) -> float | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BINANCE_TICKER_URL, params={"symbol": symbol}, timeout=10)
            resp.raise_for_status()
            return float(resp.json()["price"])
        except Exception as e:
            log.warning(f"  ⚠️ Fetch price {symbol} gagal (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return None

# ============================================================
# 📐  INDIKATOR — ADX dan ATR
# ============================================================
def calc_atr(candles: list, period: int = 14) -> tuple[float, float]:
    """
    Hitung ATR dan rata-rata ATR 5 periode.
    Returns: (atr_current, atr_avg_5)
    """
    if len(candles) < period + 1:
        return 0.0, 0.0

    df = pd.DataFrame(candles, columns=["ts","open","high","low","close","vol"])
    df["prev_close"] = df["close"].shift(1)
    df["tr"] = df[["high","low","prev_close"]].apply(
        lambda r: max(
            r["high"] - r["low"],
            abs(r["high"] - r["prev_close"]),
            abs(r["low"]  - r["prev_close"])
        ), axis=1
    )
    df["atr"] = df["tr"].ewm(span=period, adjust=False).mean()

    atr_current = float(df["atr"].iloc[-1])
    atr_avg5    = float(df["atr"].iloc[-6:-1].mean()) if len(df) >= 6 else atr_current

    return atr_current, atr_avg5

def calc_adx(candles: list, period: int = 14) -> float:
    """
    Hitung ADX(14).
    ADX < 23  → sideways (gunakan streak 3)
    ADX ≥ 23  → momentum (gunakan streak 5+1)
    """
    if len(candles) < period * 2 + 1:
        return 20.0  # default: sideways jika data kurang

    df = pd.DataFrame(candles, columns=["ts","open","high","low","close","vol"])

    df["prev_high"]  = df["high"].shift(1)
    df["prev_low"]   = df["low"].shift(1)
    df["prev_close"] = df["close"].shift(1)

    df["tr"] = df.apply(
        lambda r: max(
            r["high"] - r["low"],
            abs(r["high"] - r["prev_close"]),
            abs(r["low"]  - r["prev_close"])
        ), axis=1
    )

    df["+dm"] = df.apply(
        lambda r: max(r["high"] - r["prev_high"], 0)
        if r["high"] - r["prev_high"] > r["prev_low"] - r["low"] else 0,
        axis=1
    )
    df["-dm"] = df.apply(
        lambda r: max(r["prev_low"] - r["low"], 0)
        if r["prev_low"] - r["low"] > r["high"] - r["prev_high"] else 0,
        axis=1
    )

    atr  = df["tr"].ewm(span=period, adjust=False).mean()
    pdm  = df["+dm"].ewm(span=period, adjust=False).mean()
    mdm  = df["-dm"].ewm(span=period, adjust=False).mean()

    pdi  = 100 * pdm / atr.replace(0, 1e-10)
    mdi  = 100 * mdm / atr.replace(0, 1e-10)
    dx   = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, 1e-10)
    adx  = dx.ewm(span=period, adjust=False).mean()

    return float(adx.iloc[-1])

def get_market_condition(candles: list) -> dict:
    """
    Tentukan kondisi market: SIDEWAYS atau MOMENTUM.
    Returns: {condition, adx, atr, atr_avg5, streak_mode}
    """
    adx              = calc_adx(candles, ADX_PERIOD)
    atr, atr_avg5    = calc_atr(candles, ATR_PERIOD)

    # Sideways: ADX rendah DAN ATR tidak signifikan naik
    atr_ratio        = atr / atr_avg5 if atr_avg5 > 0 else 1.0
    is_sideways      = adx < ADX_SIDEWAYS

    condition        = "SIDEWAYS" if is_sideways else "MOMENTUM"
    streak_mode      = (
        f"EXH-{SIDEWAYS_STREAK_1}/{SIDEWAYS_STREAK_2}"
        if is_sideways
        else f"EXH-{MOMENTUM_STREAK}"
    )

    return {
        "condition":   condition,
        "adx":         adx,
        "atr":         atr,
        "atr_avg5":    atr_avg5,
        "atr_ratio":   atr_ratio,
        "streak_mode": streak_mode,
    }

# ============================================================
# 🔍  EXHAUSTION DETECTION
# ============================================================
def detect_exhaustion(candles: list, exh_state: dict, market: dict) -> dict | None:
    """
    Deteksi pola exhaustion berdasarkan kondisi market.

    SIDEWAYS (ADX < 23):
      Streak 3 candle sama warna → sinyal
      Jika salah → lanjut ke streak 4 (state tersimpan)

    MOMENTUM (ADX ≥ 23):
      5 candle sama warna + candle ke-6 sama warna (konfirmasi) → sinyal
      Tidak ada lanjutan setelah sinyal momentum
    """
    if len(candles) < 8:
        return None

    condition  = market["condition"]
    recent     = candles[-8:]  # ambil 8 candle untuk hitung streak

    # Hitung streak termasuk candle sinyal (candles[-1])
    bullish_streak = 0
    for c in reversed(recent):
        if c[4] >= c[1]:
            bullish_streak += 1
        else:
            break

    bearish_streak = 0
    for c in reversed(recent):
        if c[4] < c[1]:
            bearish_streak += 1
        else:
            break

    prev_color = exh_state.get("color", "")
    prev_count = exh_state.get("count", 0)
    sinyal_c   = candles[-1]

    log.debug(
        f"  Cond:{condition} 🟢={bullish_streak} 🔴={bearish_streak} "
        f"ADX:{market['adx']:.1f} state={prev_color}/{prev_count}"
    )

    # ══════════════════════════════════════════════════════════════════════════
    # MODE SIDEWAYS — streak 3, lanjut 4 jika salah
    # ══════════════════════════════════════════════════════════════════════════
    if condition == "SIDEWAYS":

        # 3 candle hijau → sinyal DOWN
        if bullish_streak == SIDEWAYS_STREAK_1:
            return _make_sig("DOWN", "GREEN", SIDEWAYS_STREAK_1, sinyal_c, market,
                             f"3🟢 (sideways) → potensi DOWN")

        # 4 candle hijau → sinyal DOWN lanjutan (hanya jika EXH-3 sebelumnya salah)
        if bullish_streak == SIDEWAYS_STREAK_2 and prev_color == "GREEN" and prev_count == SIDEWAYS_STREAK_1:
            return _make_sig("DOWN", "GREEN", SIDEWAYS_STREAK_2, sinyal_c, market,
                             f"4🟢 (sideways lanjutan) → potensi DOWN")

        # 3 candle merah → sinyal UP
        if bearish_streak == SIDEWAYS_STREAK_1:
            return _make_sig("UP", "RED", SIDEWAYS_STREAK_1, sinyal_c, market,
                             f"3🔴 (sideways) → potensi UP")

        # 4 candle merah → sinyal UP lanjutan
        if bearish_streak == SIDEWAYS_STREAK_2 and prev_color == "RED" and prev_count == SIDEWAYS_STREAK_1:
            return _make_sig("UP", "RED", SIDEWAYS_STREAK_2, sinyal_c, market,
                             f"4🔴 (sideways lanjutan) → potensi UP")

    # ══════════════════════════════════════════════════════════════════════════
    # MODE MOMENTUM — 5 candle sama + candle ke-6 konfirmasi (sama warna)
    # Logika: streak ke-6 = 5 candle sebelumnya sudah sama + candle ini sama
    # Artinya bullish_streak == 6 atau bearish_streak == 6
    # ══════════════════════════════════════════════════════════════════════════
    else:
        if bullish_streak == MOMENTUM_STREAK:
            return _make_sig("DOWN", "GREEN", MOMENTUM_STREAK, sinyal_c, market,
                             f"5🟢+1🟢 konfirmasi (momentum) → potensi DOWN")

        if bearish_streak == MOMENTUM_STREAK:
            return _make_sig("UP", "RED", MOMENTUM_STREAK, sinyal_c, market,
                             f"5🔴+1🔴 konfirmasi (momentum) → potensi UP")

    return None

def _make_sig(signal, streak_color, streak_count, candle, market, extra) -> dict:
    return {
        "signal":       signal,
        "streak_color": streak_color,
        "streak_count": streak_count,
        "candle":       candle,
        "extra":        extra,
        "condition":    market["condition"],
        "adx":          market["adx"],
        "atr_ratio":    market["atr_ratio"],
    }

# ============================================================
# 🎰  POLYMARKET ODDS FETCHER
# ============================================================
def fetch_polymarket_odds(name: str, open_ts_ms: int, tf: str) -> dict | None:
    """
    Ambil odds dari Polymarket Gamma API.
    Slug format: {coin}-updown-{tf}-{next_window_ts}
    """
    interval_sec = TIMEFRAMES[tf]["interval_sec"]
    open_ts_sec  = open_ts_ms // 1000
    next_window  = ((open_ts_sec // interval_sec) + 1) * interval_sec
    slug         = f"{name.lower()}-updown-{tf}-{next_window}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                POLYMARKET_GAMMA,
                params={"slug": slug},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data or not isinstance(data, list):
                log.debug(f"  ↳ Market tidak ditemukan: {slug}")
                return None

            market   = data[0]
            outcomes = market.get("outcomes", "[]")
            prices   = market.get("outcomePrices", "[]")

            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
                prices   = json.loads(prices)

            if len(outcomes) < 2 or len(prices) < 2:
                return None

            up_idx, dn_idx = 0, 1
            for i, o in enumerate(outcomes):
                ol = str(o).lower()
                if ol in ("up", "higher", "yes"):
                    up_idx = i
                elif ol in ("down", "lower", "no"):
                    dn_idx = i

            up_price = float(prices[up_idx])
            dn_price = float(prices[dn_idx])

            return {
                "up":   up_price,
                "down": dn_price,
                "slug": slug,
                "link": f"https://polymarket.com/event/{slug}",
            }

        except Exception as e:
            log.warning(f"  ⚠️ Polymarket odds gagal (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return None

# ============================================================
# ⏱️  TIMING
# ============================================================
def get_poly_window_end_utc(open_ts_ms: int, tf: str) -> int:
    interval_sec  = TIMEFRAMES[tf]["interval_sec"]
    open_ts_sec   = open_ts_ms // 1000
    open_dt_utc   = datetime.fromtimestamp(open_ts_sec, tz=timezone.utc)
    open_dt_et    = open_dt_utc + ET_OFFSET
    et_epoch      = int(open_dt_et.timestamp())
    et_floored    = (et_epoch // interval_sec) * interval_sec
    et_window_end = et_floored + interval_sec
    return et_window_end - int(ET_OFFSET.total_seconds())

def get_result_ready_ts(open_ts_ms: int, tf: str) -> int:
    return get_poly_window_end_utc(open_ts_ms, tf) + 10

def fmt_utc(ts_sec: int) -> str:
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%H:%M")

def fmt_et(ts_sec: int) -> str:
    return (datetime.fromtimestamp(ts_sec, tz=timezone.utc) + ET_OFFSET).strftime("%H:%M")

def seconds_until_next_15m() -> float:
    now = time.time()
    return (900 - now % 900) + 3

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
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                      "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            log.warning(f"  ⚠️ Telegram gagal (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

# ============================================================
# 🏗️  BUILD MESSAGES
# ============================================================
def build_signal_message(
    sig: dict, name: str, tf: str, open_ts_ms: int,
    odds: dict | None
) -> str:
    ts, o, h, l, c, _ = sig["candle"]
    dt_str   = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    tf_label = TIMEFRAMES[tf]["label"]
    arrow    = "🐂 UP" if sig["signal"] == "UP" else "🐻 DOWN"
    n        = sig["streak_count"]
    we       = get_poly_window_end_utc(open_ts_ms, tf)

    # Odds section
    if odds:
        our_odds = odds["up"] if sig["signal"] == "UP" else odds["down"]
        is_value = our_odds <= ODDS_THRESHOLD
        value_tag = "✅ VALUE BET!" if is_value else "⚠️ Odds tinggi"
        odds_line = (
            f"\n🎰 <b>Polymarket Odds:</b>\n"
            f"   Beli {arrow}: <b>{our_odds*100:.0f}¢</b> {value_tag}\n"
            f"   BEP jika beli: WR ≥ {our_odds*100:.0f}%\n"
            f"   Profit jika menang: +{(1-our_odds)*100:.0f}¢\n"
            f"🔗 Market: {odds['link']}"
        )
        odds_flag = "🎰" if is_value else "📊"
    else:
        odds_line = "\n📊 <i>Odds Polymarket tidak tersedia saat ini</i>"
        odds_flag = "📊"

    return (
        f"🚨 <b>[{tf_label}] {odds_flag} SIGNAL — {arrow}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Candle  : {dt_str} (UTC)\n"
        f"📊 Coin    : {name}\n"
        f"📊 OHLC    : O:<code>{o:.4f}</code> H:<code>{h:.4f}</code> "
        f"L:<code>{l:.4f}</code> C:<code>{c:.4f}</code>\n"
        f"📋 Pola    : {sig['extra']}\n"
        f"📈 Kondisi : {sig['condition']} | ADX:{sig['adx']:.1f} | ATR ratio:{sig['atr_ratio']:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Ref Price: <b>${c:.6f}</b>\n"
        f"   <i>(Close candle {tf_label} = acuan Polymarket)</i>"
        f"{odds_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Window tutup: {fmt_et(we)} ET / {fmt_utc(we)} UTC\n"
        f"⏳ <i>Result dikirim tepat saat window {tf_label} tutup</i>"
    )

def build_result_message(pending: dict, result_price: float, now_str: str) -> str:
    ref_price  = pending["ref_price"]
    signal     = pending["signal"]
    tf_label   = TIMEFRAMES[pending["tf"]]["label"]
    n          = pending["streak_count"]
    price_diff = result_price - ref_price
    pct_change = (price_diff / ref_price * 100) if ref_price > 0 else 0
    diff_sign  = "+" if price_diff > 0 else ""
    is_correct = (result_price > ref_price) if signal == "UP" else (result_price < ref_price)
    direction  = "⬆️ Naik" if price_diff > 0 else "⬇️ Turun"
    verdict    = "✅ <b>BENAR</b>" if is_correct else "❌ <b>SALAH</b>"
    emoji      = "🎯" if is_correct else "💔"

    now_et = (datetime.strptime(now_str, "%Y-%m-%d %H:%M")
              .replace(tzinfo=timezone.utc) + ET_OFFSET).strftime("%Y-%m-%d %H:%M")

    # Odds P/L jika tersedia
    odds_pl = ""
    if pending.get("our_odds") is not None:
        our_odds = pending["our_odds"]
        pl = (1 - our_odds) if is_correct else -our_odds
        odds_pl = f"\n💰 P/L (jika bet $1): <b>{'+' if pl>=0 else ''}{pl*100:.0f}¢</b>"

    return (
        f"{emoji} <b>[{tf_label}] HASIL — {verdict}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Coin      : {pending['name']}\n"
        f"📌 Sinyal    : <b>{'🐂 UP' if signal=='UP' else '🐻 DOWN'}</b> "
        f"[EXH-{n} {pending['condition']}]\n"
        f"⏰ Entry     : {pending['entry_time']} UTC\n"
        f"💰 Ref Price : <b>${ref_price:.6f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Window End: {now_et} ET / {now_str} UTC\n"
        f"💰 Close     : <b>${result_price:.6f}</b>\n"
        f"📈 Pergerakan: {direction} "
        f"<code>{diff_sign}{price_diff:.6f}</code> "
        f"(<code>{diff_sign}{pct_change:.3f}%</code>)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 Verdict   : {verdict}{odds_pl}\n"
        f"📌 <i>Dinilai vs ref price (close candle {tf_label})</i>"
    )

# ============================================================
# 📈  STATS STRUCTURE
# ============================================================
def new_stat() -> dict:
    """
    Struktur statistik per kategori:
    all_  = semua sinyal
    odds_ = hanya sinyal yang odds ≤ 45¢
    """
    return {
        "all_win": 0, "all_loss": 0,
        "odds_win": 0, "odds_loss": 0,
    }

def stat_line(s: dict, label: str) -> str:
    """Buat baris statistik dengan WR."""
    at = s["all_win"] + s["all_loss"]
    awr = (s["all_win"] / at * 100) if at > 0 else 0
    ot = s["odds_win"] + s["odds_loss"]
    owr = (s["odds_win"] / ot * 100) if ot > 0 else 0
    return (
        f"  {label}\n"
        f"    Semua  : ✅{s['all_win']} ❌{s['all_loss']} "
        f"({at} sinyal) WR:{awr:.1f}%\n"
        f"    Ber-odds≤45¢: ✅{s['odds_win']} ❌{s['odds_loss']} "
        f"({ot} sinyal) WR:{owr:.1f}%\n"
    )

# ============================================================
# 📊  DAILY & WEEKLY REPORT
# ============================================================
def build_day_report(day_num: int, day_stats: dict) -> str:
    """
    day_stats format:
    {
      "15m": {"EXH3": new_stat(), "EXH4": new_stat(), "EXH6": new_stat()},
      "1h":  {"EXH3": new_stat(), "EXH4": new_stat(), "EXH6": new_stat()},
      "coin": {"BTC": new_stat(), "ETH": new_stat(), "BNB": new_stat()},
    }
    """
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    msg     = f"📊 <b>DAY {day_num} REPORT — {now_str} (07:00 WIB)</b>\n"
    msg    += "━━━━━━━━━━━━━━━━━━━━━━\n"

    for tf_key, tf_label in [("15m", "15M"), ("1h", "1H")]:
        msg += f"\n⏱️ <b>Timeframe {tf_label}:</b>\n"
        for exh_key, exh_label in [("EXH3","Sideways EXH-3"),
                                    ("EXH4","Sideways EXH-4"),
                                    ("EXH6","Momentum EXH-6")]:
            s = day_stats.get(tf_key, {}).get(exh_key, new_stat())
            if s["all_win"] + s["all_loss"] > 0:
                msg += stat_line(s, exh_label)

    msg += f"\n🪙 <b>Per Coin:</b>\n"
    for cn in ["BTC", "ETH", "BNB"]:
        s = day_stats.get("coin", {}).get(cn, new_stat())
        if s["all_win"] + s["all_loss"] > 0:
            msg += stat_line(s, cn)

    # Total
    total = new_stat()
    for tf_key in ["15m", "1h"]:
        for exh_key in ["EXH3", "EXH4", "EXH6"]:
            s = day_stats.get(tf_key, {}).get(exh_key, new_stat())
            for k in ["all_win","all_loss","odds_win","odds_loss"]:
                total[k] += s[k]
    msg += f"\n{stat_line(total, '📊 TOTAL')}"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚠️ <i>Data akumulasi — tidak direset tiap hari.</i>"
    return msg

def build_weekly_report(all_day_stats: list) -> str:
    """Akumulasi semua day stats."""
    msg  = f"📊 <b>WEEKLY REPORT ({len(all_day_stats)} hari)</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"

    # Akumulasi
    weekly = {
        "15m":  {"EXH3": new_stat(), "EXH4": new_stat(), "EXH6": new_stat()},
        "1h":   {"EXH3": new_stat(), "EXH4": new_stat(), "EXH6": new_stat()},
        "coin": {"BTC": new_stat(), "ETH": new_stat(), "BNB": new_stat()},
    }
    for ds in all_day_stats:
        for tf_key in ["15m", "1h"]:
            for exh_key in ["EXH3", "EXH4", "EXH6"]:
                s  = ds.get(tf_key, {}).get(exh_key, new_stat())
                ws = weekly[tf_key][exh_key]
                for k in ["all_win","all_loss","odds_win","odds_loss"]:
                    ws[k] += s[k]
        for cn in ["BTC", "ETH", "BNB"]:
            s  = ds.get("coin", {}).get(cn, new_stat())
            ws = weekly["coin"][cn]
            for k in ["all_win","all_loss","odds_win","odds_loss"]:
                ws[k] += s[k]

    for tf_key, tf_label in [("15m","15M"), ("1h","1H")]:
        msg += f"\n⏱️ <b>Timeframe {tf_label}:</b>\n"
        for exh_key, exh_label in [("EXH3","Sideways EXH-3"),
                                    ("EXH4","Sideways EXH-4"),
                                    ("EXH6","Momentum EXH-6")]:
            s = weekly[tf_key][exh_key]
            if s["all_win"] + s["all_loss"] > 0:
                msg += stat_line(s, exh_label)

    msg += f"\n🪙 <b>Per Coin:</b>\n"
    for cn in ["BTC", "ETH", "BNB"]:
        s = weekly["coin"][cn]
        if s["all_win"] + s["all_loss"] > 0:
            msg += stat_line(s, cn)

    total = new_stat()
    for tf_key in ["15m", "1h"]:
        for exh_key in ["EXH3", "EXH4", "EXH6"]:
            s = weekly[tf_key][exh_key]
            for k in ["all_win","all_loss","odds_win","odds_loss"]:
                total[k] += s[k]
    msg += f"\n{stat_line(total, '📊 TOTAL WEEKLY')}"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "♻️ <i>Data direset untuk minggu berikutnya.</i>"
    return msg

def check_streak_alert(name: str, tf: str, exh_key: str, results: list) -> str | None:
    if len(results) < STREAK_ALERT_N:
        return None
    recent = results[-STREAK_ALERT_N:]
    tf_label = TIMEFRAMES[tf]["label"]
    if all(r is True for r in recent):
        return (
            f"🔥 <b>WIN STREAK [{tf_label}/{exh_key}]</b>\n"
            f"   {name} — {STREAK_ALERT_N}x benar berturut!\n"
            f"   🎯 On fire!"
        )
    if all(r is False for r in recent):
        return (
            f"⚠️ <b>LOSE STREAK [{tf_label}/{exh_key}]</b>\n"
            f"   {name} — {STREAK_ALERT_N}x salah berturut!\n"
            f"   🛑 Pertimbangkan pause."
        )
    return None

# ============================================================
# 🤖  MAIN BOT LOOP
# ============================================================
def make_day_stats() -> dict:
    return {
        "15m":  {"EXH3": new_stat(), "EXH4": new_stat(), "EXH6": new_stat()},
        "1h":   {"EXH3": new_stat(), "EXH4": new_stat(), "EXH6": new_stat()},
        "coin": {"BTC": new_stat(), "ETH": new_stat(), "BNB": new_stat()},
    }

def streak_key(streak_count: int) -> str:
    if streak_count == 3:
        return "EXH3"
    if streak_count == 4:
        return "EXH4"
    return "EXH6"

def run_bot() -> None:
    log.info("🚀 Polymarket Signal Bot v10 AKTIF")
    log.info(f"   Coins    : {', '.join(c['name'] for c in COINS)}")
    log.info(f"   TF       : 15M + 1H (bersamaan)")
    log.info(f"   Sideways : ADX < {ADX_SIDEWAYS} → streak {SIDEWAYS_STREAK_1}/{SIDEWAYS_STREAK_2}")
    log.info(f"   Momentum : ADX ≥ {ADX_SIDEWAYS} → streak {MOMENTUM_STREAK} (5+1 konfirmasi)")
    log.info(f"   Odds tag : ≤{ODDS_THRESHOLD*100:.0f}¢ ditandai sebagai value bet")
    log.info(f"   Report   : Day1–Day7 akumulasi → Weekly → Reset")

    # ── State ─────────────────────────────────────────────────────────────────
    pending_signals: list[dict] = []

    # Exhaustion state per coin per TF
    exh_state: dict = {
        c["name"]: {tf: {"color": "", "count": 0} for tf in TIMEFRAMES}
        for c in COINS
    }

    last_processed: dict = {}   # {(symbol, tf): last_open_ts_ms}

    # Stats
    current_day_stats   = make_day_stats()
    all_day_stats: list = []   # list of day_stats, max 7
    day_number          = 1
    daily_report_sent   = None
    result_history: dict = defaultdict(list)  # {(tf, exh_key): [True/False]}

    wait = seconds_until_next_15m()
    log.info(f"⏳ Scan pertama dalam {wait:.0f} detik...")
    time.sleep(wait)

    while True:
        try:
            now_utc   = datetime.now(tz=timezone.utc)
            now_ts    = int(time.time())
            today_str = now_utc.strftime("%Y-%m-%d")

            # ── Daily Report (07:00 WIB = 00:00 UTC) ──────────────────────────
            if now_utc.hour == DAILY_REPORT_HOUR and now_utc.minute < 6:
                if daily_report_sent != today_str:
                    # Kirim day report (akumulasi, tidak reset)
                    send_telegram(build_day_report(day_number, current_day_stats))
                    log.info(f"📊 Day {day_number} report dikirim.")

                    all_day_stats.append(current_day_stats)
                    day_number       += 1
                    daily_report_sent = today_str

                    # Weekly report setelah 7 hari → reset
                    if len(all_day_stats) >= WEEKLY_DAYS:
                        send_telegram(build_weekly_report(all_day_stats))
                        log.info("📊 Weekly report dikirim → reset.")
                        all_day_stats       = []
                        current_day_stats   = make_day_stats()
                        day_number          = 1
                        result_history      = defaultdict(list)

            # ── Cek Hasil Pending ─────────────────────────────────────────────
            still_pending = []
            for ps in pending_signals:
                if now_ts < ps["result_ready_ts"]:
                    still_pending.append(ps)
                    continue

                log.info(f"  🏁 Result {ps['name']} [{TIMEFRAMES[ps['tf']]['label']}]")
                result_price = fetch_current_price(ps["symbol"])
                if result_price is None:
                    still_pending.append(ps)
                    continue

                now_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                send_telegram(build_result_message(ps, result_price, now_str))

                is_win  = (result_price > ps["ref_price"]) if ps["signal"] == "UP" \
                          else (result_price < ps["ref_price"])
                tf      = ps["tf"]
                cn      = ps["name"]
                sk      = streak_key(ps["streak_count"])
                has_odds = ps.get("our_odds") is not None
                is_value = has_odds and ps["our_odds"] <= ODDS_THRESHOLD

                def _update(stat: dict, win: bool, value: bool):
                    if win:
                        stat["all_win"] += 1
                        if value:
                            stat["odds_win"] += 1
                    else:
                        stat["all_loss"] += 1
                        if value:
                            stat["odds_loss"] += 1

                _update(current_day_stats[tf][sk], is_win, is_value)
                _update(current_day_stats["coin"][cn], is_win, is_value)

                rh_key = (tf, sk)
                result_history[rh_key].append(is_win)

                if is_win:
                    log.info(f"  ✅ {cn} [{TIMEFRAMES[tf]['label']}/{sk}] BENAR")
                    # Reset exhaustion state jika menang
                    exh_state[cn][tf] = {"color": "", "count": 0}
                else:
                    log.info(f"  ❌ {cn} [{TIMEFRAMES[tf]['label']}/{sk}] SALAH")
                    # Update exhaustion state jika salah di sideways EXH-3
                    if sk == "EXH3":
                        exh_state[cn][tf] = {
                            "color": ps["streak_color"],
                            "count": SIDEWAYS_STREAK_1,
                        }
                        log.info(f"  ➕ {cn} {tf} siap EXH-4")
                    else:
                        exh_state[cn][tf] = {"color": "", "count": 0}

                alert = check_streak_alert(cn, tf, sk, result_history[rh_key])
                if alert:
                    send_telegram(alert)

            pending_signals = still_pending

            # ── Scan Semua Coins × Timeframes ─────────────────────────────────
            for coin in COINS:
                for tf in TIMEFRAMES:
                    key = (coin["symbol"], tf)
                    tf_cfg = TIMEFRAMES[tf]

                    candles = fetch_candles(
                        coin["symbol"], tf,
                        limit=tf_cfg["candles_needed"]
                    )
                    if not candles:
                        continue

                    last_open_ts = candles[-1][0]
                    if last_processed.get(key) == last_open_ts:
                        continue
                    last_processed[key] = last_open_ts

                    dt_c = datetime.fromtimestamp(
                        last_open_ts / 1000, tz=timezone.utc
                    ).strftime("%H:%M")
                    log.info(f"🔍 {coin['name']} {tf_cfg['label']} [{dt_c} UTC]")

                    # Deteksi kondisi market
                    market = get_market_condition(candles)
                    log.debug(
                        f"  Kondisi: {market['condition']} "
                        f"ADX:{market['adx']:.1f} "
                        f"ATR_ratio:{market['atr_ratio']:.2f}"
                    )

                    # Deteksi exhaustion
                    exh_st = exh_state[coin["name"]][tf]
                    sig    = detect_exhaustion(candles, exh_st, market)

                    if sig is None:
                        # Reset state jika streak putus
                        last_c    = candles[-1]
                        exh_color = exh_st.get("color", "")
                        if (exh_color == "GREEN" and last_c[4] < last_c[1]) or \
                           (exh_color == "RED"   and last_c[4] >= last_c[1]):
                            exh_state[coin["name"]][tf] = {"color": "", "count": 0}
                        log.debug(f"  ↳ {coin['name']} {tf} — tidak ada sinyal.")
                        continue

                    # Update exhaustion state
                    exh_state[coin["name"]][tf] = {
                        "color": sig["streak_color"],
                        "count": sig["streak_count"],
                    }

                    log.info(
                        f"  🔔 {sig['signal']} [{streak_key(sig['streak_count'])}] "
                        f"{coin['name']} {tf_cfg['label']} | {sig['condition']}"
                    )

                    # Fetch Polymarket odds (opsional, tidak jadi filter)
                    odds = fetch_polymarket_odds(coin["name"], last_open_ts, tf)
                    if odds:
                        our_odds = odds["up"] if sig["signal"] == "UP" else odds["down"]
                        log.info(
                            f"  🎰 Odds: {our_odds*100:.0f}¢ "
                            f"{'✅ VALUE' if our_odds <= ODDS_THRESHOLD else '⚠️ Mahal'}"
                        )
                    else:
                        our_odds = None

                    # Kirim sinyal (semua sinyal, odds hanya notif)
                    ref_price    = sig["candle"][4]
                    result_ready = get_result_ready_ts(last_open_ts, tf)
                    window_end   = get_poly_window_end_utc(last_open_ts, tf)

                    send_telegram(build_signal_message(
                        sig=sig, name=coin["name"], tf=tf,
                        open_ts_ms=last_open_ts, odds=odds,
                    ))

                    log.info(
                        f"  📨 {coin['name']} {tf_cfg['label']} → "
                        f"result {fmt_et(window_end)} ET"
                    )

                    pending_signals.append({
                        "signal":          sig["signal"],
                        "streak_color":    sig["streak_color"],
                        "streak_count":    sig["streak_count"],
                        "condition":       sig["condition"],
                        "symbol":          coin["symbol"],
                        "name":            coin["name"],
                        "tf":              tf,
                        "ref_price":       ref_price,
                        "our_odds":        our_odds,
                        "entry_time":      datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
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

# ============================================================
# ▶️  ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_bot()
