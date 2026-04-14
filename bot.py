#!/usr/bin/env python3
# ============================================================
# FOREX SIGNAL BOT — 3 Strategi Scalping Akun Cent $300
# Pair     : EURUSD, GBPUSD, USDJPY, XAUUSD, BTCUSD
# Strategi : S1 Imbalance+Fractal+EMA | S2 S&D RBR/DBR | S3 Pullback+OB+PA
# Data     : Twelve Data API (Forex/Gold) + Binance Vision (BTC)
# Risk     : $20/trade (6.5%), RR 1:2.5, spread 3 pips fixed
# Signal   : Setup entry (limit order), bukan market order langsung
# ============================================================

import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import pandas_ta as ta
import requests
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "your_token_here")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "your_chat_id_here")

# Twelve Data API — gratis 800 req/day, daftar di twelvedata.com
TWELVE_DATA_KEY    = os.getenv("TWELVE_DATA_KEY", "your_key_here")
TWELVE_DATA_URL    = "https://api.twelvedata.com/time_series"
TWELVE_PRICE_URL   = "https://api.twelvedata.com/price"

# Binance Vision — BTC only, no geo-block, no API key
BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
BINANCE_TICKER_URL = "https://data-api.binance.vision/api/v3/ticker/price"

# Account
ACCOUNT_BALANCE    = float(os.getenv("ACCOUNT_BALANCE", "300"))
RISK_PER_TRADE_USD = float(os.getenv("RISK_USD", "20"))
RR_RATIO           = 2.5
SPREAD_PIPS        = 3

# Timeframes
TF_HIGH = "30min"; TF_MID = "5min"; TF_LOW = "1min"   # Twelve Data
TF_H_BN = "30m";   TF_M_BN = "5m";  TF_L_BN = "1m"   # Binance

# Indicators
ATR_PERIOD = 14; EMA_FAST = 21; EMA_SLOW = 50
MA_FAST = 9;     MA_SLOW  = 21; STRONG_LEG_MULT = 1.5
VOLUME_LOOKBACK = 20

# Bot config
SCAN_INTERVAL_SEC  = 900   # scan tiap 15 menit saat aktif
DAILY_REPORT_HOUR  = 0     # 00:00 UTC = 07:00 WIB

# ── Sesi trading aktif (WIB = UTC+7) ──────────────────────────────────────────
# London: 13:00–22:00 WIB = 06:00–15:00 UTC
# New York: 20:00–03:00 WIB = 13:00–20:00 UTC
# Gabungan aktif: 13:00–03:00 WIB = 06:00–20:00 UTC
# Tidur (Sesi Asia): 03:00–13:00 WIB = 20:00–06:00 UTC
SESSION_START_UTC  = 6     # 06:00 UTC = 13:00 WIB (London buka)
SESSION_END_UTC    = 20    # 20:00 UTC = 03:00 WIB (NY tutup)
SIGNAL_COOLDOWN_H = 2; DB_PATH = "signals.db"; LOG_FILE = "bot.log"
MAX_RETRIES = 3

# ── Twelve Data rate limit strategy ──────────────────────────────────────────
# Free plan: 800 credits/day, 8 req/menit
# Setiap request ke /time_series = 1 credit
# Solusi: batch request (1 call untuk semua pair) + delay antar batch
#
# Hitung kebutuhan per scan:
#   4 pair forex × 3 TF = 12 req → pakai batch → 3 req (1 per TF)
#   1 pair BTC via Binance = 0 req Twelve Data
#   Monitor harga: pakai close candle TF1 terakhir → 0 req tambahan
#   Total per scan: 3 req Twelve Data
#   Scan tiap 5 menit = 3 req per 5 menit = 0.6 req/menit ✅ jauh di bawah 8
#
TWELVE_REQ_DELAY  = 10.0  # detik antar request, safety margin
_last_twelve_req  = 0.0

# Pip sizes
PIP_SIZE = {"EURUSD":0.0001,"GBPUSD":0.0001,"USDJPY":0.01,
            "XAUUSD":0.01,"BTCUSD":1.00}

# ── Lot value per pip untuk akun CENT ────────────────────────────────────────
# Akun cent: 1 lot cent = 0.01 lot standard = 1,000 unit
#
# Cara hitung nilai pip per 1 lot cent (dalam USD):
#   EURUSD : 1 pip = 0.0001 × 1,000 unit = $0.10 per lot cent
#   GBPUSD : sama dengan EURUSD = $0.10 per lot cent
#   USDJPY : 1 pip = 0.01 JPY × 1,000 / USDJPY_rate
#            ≈ 10 JPY / 150 ≈ $0.067 per lot cent → pakai $0.07
#   XAUUSD : 1 pip = 0.01 USD × 1,000 = $10 per lot standard
#            per lot cent = $10 × 0.01 = $0.10 per lot cent
#   BTCUSD : 1 pip = $1 × 1,000 unit... perlu verifikasi broker
#
# Contoh: USDJPY SL=10pips, Risk=$20
#   Lot = $20 / (10pips × $0.07) = 28.6 lot cent
#
# SAFETY CAP: MAX_LOT memastikan lot tidak absurd
LOT_VALUE_PER_PIP = {"EURUSD":0.10,"GBPUSD":0.10,"USDJPY":0.07,
                     "XAUUSD":0.10,"BTCUSD":0.10}

# Batas maksimum lot per trade — WAJIB ada untuk safety
# Sesuaikan dengan margin yang tersedia di akun $300 cent
MAX_LOT = {"EURUSD":10.0,"GBPUSD":10.0,"USDJPY":10.0,
           "XAUUSD": 5.0,"BTCUSD": 2.0}

ASSETS = [
    {"name":"EURUSD","source":"twelve","sym_td":"EUR/USD","sym_bn":None},
    {"name":"GBPUSD","source":"twelve","sym_td":"GBP/USD","sym_bn":None},
    {"name":"USDJPY","source":"twelve","sym_td":"USD/JPY","sym_bn":None},
    {"name":"XAUUSD","source":"twelve","sym_td":"XAU/USD","sym_bn":None},
    {"name":"BTCUSD","source":"binance","sym_td":None,"sym_bn":"BTCUSDT"},
]

# ============================================================
# LOGGING
# ============================================================
def setup_logger():
    logger = logging.getLogger("ForexBot")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("[%(asctime)s UTC] %(levelname)s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(); ch.setLevel(logging.INFO); ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG); fh.setFormatter(fmt); logger.addHandler(fh)
    return logger

log = setup_logger()

# ============================================================
# DATABASE
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL, pair TEXT NOT NULL,
            strategy TEXT NOT NULL, signal_type TEXT NOT NULL,
            entry REAL, sl REAL, tp REAL,
            zone_high REAL, zone_low REAL,
            lot_size REAL, pips_risk REAL, rr REAL,
            spread_pips REAL, tf_confirm TEXT,
            status TEXT DEFAULT 'OPEN', result TEXT,
            pnl_pct REAL, pnl_usd REAL,
            close_time TEXT, notes TEXT
        )
    """)
    conn.commit(); conn.close(); log.info("Database siap.")

def save_signal(pair, strategy, sig_type, entry, sl, tp, tf_confirm,
                lot=0, pips_r=0, zh=None, zl=None, notes="") -> int:
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("""INSERT INTO signals
        (timestamp,pair,strategy,signal_type,entry,sl,tp,zone_high,zone_low,
         lot_size,pips_risk,rr,spread_pips,tf_confirm,notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
         pair,strategy,sig_type,entry,sl,tp,zh,zl,lot,pips_r,
         RR_RATIO,SPREAD_PIPS,tf_confirm,notes))
    sid = cur.lastrowid; conn.commit(); conn.close(); return sid

def update_result(sid: int, result: str):
    if result == "EXPIRED":
        pnl_usd = 0.0   # tidak ada P&L jika order tidak kena
    elif result == "TP":
        pnl_usd = RISK_PER_TRADE_USD * RR_RATIO
    else:
        pnl_usd = -RISK_PER_TRADE_USD
    pnl_pct = pnl_usd / ACCOUNT_BALANCE * 100
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("UPDATE signals SET status='CLOSED',result=?,pnl_pct=?,pnl_usd=?,close_time=? WHERE id=?",
        (result,pnl_pct,pnl_usd,datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),sid))
    conn.commit(); conn.close()

def get_daily_stats(date_str: str) -> dict:
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("""SELECT strategy,pair,result,pnl_pct,pnl_usd FROM signals
        WHERE DATE(timestamp)=? AND status='CLOSED' ORDER BY close_time""", (date_str,))
    rows = cur.fetchall(); conn.close()
    stats = {}
    for strategy, pair, result, pnl_pct, pnl_usd in rows:
        k = (strategy, pair)
        if k not in stats:
            stats[k] = {"win":0,"loss":0,"pnl_pct":0.0,"pnl_usd":0.0,
                        "results":[],"max_ls":0,"max_ws":0,"max_dd":0.0}
        s = stats[k]
        if result=="TP": s["win"]+=1
        else: s["loss"]+=1
        s["pnl_pct"]+=pnl_pct or 0; s["pnl_usd"]+=pnl_usd or 0
        s["results"].append(result)
    for s in stats.values():
        cl=cw=0; ml=mw=0; cd=md=0.0
        for r in s["results"]:
            if r=="SL": cl+=1;cw=0;cd+=RISK_PER_TRADE_USD;md=max(md,cd)
            else: cw+=1;cl=0;cd=0.0
            ml=max(ml,cl);mw=max(mw,cw)
        s["max_ls"]=ml;s["max_ws"]=mw;s["max_dd"]=md
    return stats

def get_alltime_streak() -> dict:
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor()
    cur.execute("SELECT strategy,pair,result FROM signals WHERE status='CLOSED' ORDER BY close_time")
    rows = cur.fetchall(); conn.close()
    sk = {}
    for strategy, pair, result in rows:
        k = (strategy, pair)
        if k not in sk: sk[k] = {"cl":0,"cw":0,"cd":0.0,"md":0.0}
        s = sk[k]
        if result=="SL": s["cl"]+=1;s["cw"]=0;s["cd"]+=RISK_PER_TRADE_USD;s["md"]=max(s["md"],s["cd"])
        else: s["cw"]+=1;s["cl"]=0;s["cd"]=0.0
    return sk

# ============================================================
# DATA FETCHER
# ============================================================
def pip(pair): return PIP_SIZE.get(pair, 0.0001)
def p2price(pair, n): return n * pip(pair)
def price2p(pair, diff): return abs(diff) / pip(pair)

def calc_lot(pair, pips_r) -> float:
    """
    Hitung lot size akun cent.
    Lot = Risk_USD / (pips_SL × lot_value_per_pip)

    Di-cap ketat oleh MAX_LOT untuk mencegah lot absurd.
    Jika SL terlalu kecil (pips_r < 3), gunakan minimum 3 pips
    untuk menghindari lot yang sangat besar.
    """
    lpv     = LOT_VALUE_PER_PIP.get(pair, 0.10)
    max_lot = MAX_LOT.get(pair, 10.0)

    # Pastikan pips_r tidak terlalu kecil (min 3 pips untuk keamanan)
    effective_pips = max(pips_r, 3.0)

    if effective_pips <= 0 or lpv <= 0:
        return 0.01

    lot = RISK_PER_TRADE_USD / (effective_pips * lpv)
    lot = round(lot / 0.01) * 0.01   # bulatkan ke 0.01
    lot_raw = lot

    # Cap wajib — jangan sampai lot melebihi MAX_LOT
    lot = max(0.01, min(lot, max_lot))

    if lot < lot_raw:
        log.warning(
            f"  Lot {pair} di-cap: {lot_raw:.2f} → {lot:.2f} lot cent "
            f"(SL={pips_r:.1f}pips, MAX={max_lot})"
        )
    return lot

def _twelve_rate_limit():
    """Pastikan jeda minimal TWELVE_REQ_DELAY detik antar request Twelve Data."""
    global _last_twelve_req
    elapsed = time.time() - _last_twelve_req
    if elapsed < TWELVE_REQ_DELAY:
        wait = TWELVE_REQ_DELAY - elapsed
        log.debug(f"  Rate limit: tunggu {wait:.1f}s")
        time.sleep(wait)
    _last_twelve_req = time.time()

def fetch_twelve_batch(symbols: list, interval: str, limit: int = 80) -> dict:
    """
    Batch request Twelve Data — 1 request untuk semua pair sekaligus.
    Hemat credit: 4 pair = 1 request (bukan 4 request).
    Returns: {symbol: DataFrame} atau {} jika gagal.
    """
    _twelve_rate_limit()
    sym_str = ",".join(symbols)  # "EUR/USD,GBP/USD,USD/JPY,XAU/USD"
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(TWELVE_DATA_URL, params={
                "symbol": sym_str, "interval": interval,
                "outputsize": limit+1, "apikey": TWELVE_DATA_KEY, "format": "JSON"
            }, timeout=20); r.raise_for_status()
            d = r.json()

            # Validasi: response harus dict
            if not isinstance(d, dict):
                log.warning(f"  Twelve Data response bukan dict: {type(d).__name__}")
                time.sleep(8); continue

            # Cek rate limit atau error global
            msg_global = str(d.get("message", ""))
            if "You have run" in msg_global or d.get("status") == "error":
                log.warning(f"  Rate limit/error Twelve Data: {msg_global[:80]} — tunggu 65 detik...")
                time.sleep(65); continue

            result = {}

            # Twelve Data response format:
            # 1 symbol  → {"values": [...], "meta": {...}}
            # N symbols → {"EUR/USD": {"values": [...], "meta": {...}}, "GBP/USD": {...}, ...}
            if len(symbols) == 1:
                sym = symbols[0]
                if "values" in d:
                    parsed = _parse_twelve_df(d)
                    if parsed is not None:
                        result[sym] = parsed
                else:
                    log.warning(f"  Twelve {sym} {interval}: {d.get('message','no values')}")
            else:
                for sym in symbols:
                    sym_data = d.get(sym)
                    # sym_data bisa: dict dengan values, dict dengan error, None, atau int
                    if not isinstance(sym_data, dict):
                        log.warning(f"  Twelve {sym}: response tidak valid (type={type(sym_data).__name__})")
                        continue
                    if "values" in sym_data:
                        parsed = _parse_twelve_df(sym_data)
                        if parsed is not None:
                            result[sym] = parsed
                    else:
                        log.warning(f"  Twelve {sym} {interval}: {sym_data.get('message','no values')}")

            return result

        except Exception as e:
            log.warning(f"Twelve batch {interval} attempt {attempt}: {e}")
            if attempt < MAX_RETRIES: time.sleep(8)
    return {}

def _parse_twelve_df(data: dict) -> pd.DataFrame | None:
    """
    Parse response Twelve Data menjadi DataFrame.
    Handle berbagai format response yang mungkin dikembalikan API.
    """
    try:
        # Validasi: data harus dict dan punya key "values"
        if not isinstance(data, dict):
            log.warning(f"  Parse error: data bukan dict (type={type(data).__name__})")
            return None

        values = data.get("values")

        # Validasi: values harus list of dict, bukan int/string/None
        if not isinstance(values, list):
            msg = data.get("message", data.get("status", str(values)[:80]))
            log.warning(f"  Twelve Data: values bukan list — {msg}")
            return None

        if len(values) < 2:
            log.warning(f"  Twelve Data: data terlalu sedikit ({len(values)} candle)")
            return None

        # Validasi: setiap row harus dict
        if not isinstance(values[0], dict):
            log.warning(f"  Parse error: row bukan dict (type={type(values[0]).__name__})")
            return None

        df = pd.DataFrame(values).rename(columns={"datetime": "timestamp"})

        if "timestamp" not in df.columns:
            log.warning("  Parse error: kolom timestamp tidak ada")
            return None

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            else:
                df[c] = 0.0

        df = df.dropna(subset=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

        if len(df) < 2:
            return None

        return df.iloc[:-1]  # buang candle live
    except Exception as e:
        log.warning(f"  Parse Twelve Data error: {e}")
        return None

def fetch_twelve(sym_td, interval, limit=80):
    """Single fetch — dipakai jika batch tidak tersedia."""
    result = fetch_twelve_batch([sym_td], interval, limit)
    return result.get(sym_td)

def fetch_binance(sym_bn, interval, limit=80):
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(BINANCE_KLINES_URL,
                params={"symbol":sym_bn,"interval":interval,"limit":limit+1},timeout=15)
            r.raise_for_status(); raw = r.json()
            if not isinstance(raw,list) or len(raw)<3: return None
            df = pd.DataFrame([[int(x[0]),float(x[1]),float(x[2]),
                                float(x[3]),float(x[4]),float(x[5])] for x in raw],
                              columns=["timestamp","open","high","low","close","volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"],unit="ms",utc=True)
            return df.iloc[:-1].reset_index(drop=True)
        except Exception as e:
            log.warning(f"Binance {sym_bn} {interval} attempt {attempt}: {e}")
            if attempt < MAX_RETRIES: time.sleep(5)
    return None

def fetch_ohlcv(asset, tf_td, tf_bn, limit=80):
    if asset["source"]=="binance": return fetch_binance(asset["sym_bn"], tf_bn, limit)
    return fetch_twelve(asset["sym_td"], tf_td, limit)

def get_price(asset):
    """
    Ambil harga terkini.
    BTC: Binance Vision (gratis, no limit)
    Forex/Gold: Ambil dari cache TF1 terakhir yang sudah di-fetch saat scan.
                Hemat credit — tidak perlu request /price terpisah.
    """
    try:
        if asset["source"]=="binance":
            r = requests.get(BINANCE_TICKER_URL,params={"symbol":asset["sym_bn"]},timeout=10)
            return float(r.json()["price"])
        # Untuk forex/gold: gunakan close candle TF1 dari cache
        name = asset["name"]
        if name in _price_cache:
            return _price_cache[name]
        # Fallback: fetch TF1 terbaru (1 request, hemat vs /price)
        df1 = fetch_twelve(asset["sym_td"], TF_LOW, limit=5)
        if df1 is not None and len(df1) > 0:
            price = float(df1["close"].iloc[-1])
            _price_cache[name] = price
            return price
        return None
    except: return None

# Cache harga dari TF1 saat scan — di-update tiap scan, dipakai monitor
_price_cache: dict = {}

# ============================================================
# INDICATORS
# ============================================================
def atr(df, p=14): return ta.atr(df["high"],df["low"],df["close"],length=p)
def ema(df, p): return df["close"].ewm(span=p,adjust=False).mean()
def ma(df, p): return df["close"].rolling(p).mean()

def fractals(df):
    df = df.copy(); df["fh"]=False; df["fl"]=False
    for i in range(2,len(df)-2):
        h=df["high"].iloc; l=df["low"].iloc
        if h[i]>h[i-1] and h[i]>h[i-2] and h[i]>h[i+1] and h[i]>h[i+2]:
            df.at[df.index[i],"fh"]=True
        if l[i]<l[i-1] and l[i]<l[i-2] and l[i]<l[i+1] and l[i]<l[i+2]:
            df.at[df.index[i],"fl"]=True
    return df

def vol_ok(df, lb=VOLUME_LOOKBACK):
    if len(df)<lb+1: return False
    avg=df["volume"].iloc[-(lb+1):-1].mean()
    return df["volume"].iloc[-1]>avg if avg>0 else False

# ============================================================
# SPREAD HELPERS
# ============================================================
def e_buy(pair,price): return price + p2price(pair,SPREAD_PIPS)
def e_sell(pair,price): return price - p2price(pair,SPREAD_PIPS)
def sl_buy_f(pair,raw): return raw - p2price(pair,SPREAD_PIPS)
def sl_sell_f(pair,raw): return raw + p2price(pair,SPREAD_PIPS)
def calc_tp(entry,sl):
    risk=abs(entry-sl)
    return entry+risk*RR_RATIO if entry>sl else entry-risk*RR_RATIO

# ============================================================
# STRATEGI 1 — IMBALANCE + FRACTAL + EMA21/50
# ============================================================
def s1_zones(df30, pair):
    if df30 is None or len(df30)<EMA_SLOW+5: return []
    atr_s=atr(df30); e21=ema(df30,EMA_FAST); e50=ema(df30,EMA_SLOW)
    fr=fractals(df30); zones=[]
    for i in range(3,len(df30)-1):
        av=atr_s.iloc[i]
        if pd.isna(av) or av<=0: continue
        c1,c2,c3=df30.iloc[i-2],df30.iloc[i-1],df30.iloc[i]
        up=e21.iloc[i]>e50.iloc[i]; dn=e21.iloc[i]<e50.iloc[i]
        # Bullish imbalance
        if (up and c1["close"]<c1["open"] and c3["close"]>c3["open"]
                and c2["low"]>c1["high"]
                and (c3["high"]-c1["low"])>=STRONG_LEG_MULT*av):
            fp=next((df30["low"].iloc[j] for j in range(i-1,max(0,i-20),-1)
                     if fr["fl"].iloc[j]),None)
            il,ih=c1["high"],c2["high"]
            if all(df30["low"].iloc[j]>il for j in range(i+1,len(df30))):
                zones.append({"type":"BULL","sig":"BUY","zh":ih,"zl":il,"fp":fp,"av":av,"i":i,"trend":"UPTREND"})
        # Bearish imbalance
        elif (dn and c1["close"]>c1["open"] and c3["close"]<c3["open"]
                and c2["high"]<c1["low"]
                and (c1["high"]-c3["low"])>=STRONG_LEG_MULT*av):
            fp=next((df30["high"].iloc[j] for j in range(i-1,max(0,i-20),-1)
                     if fr["fh"].iloc[j]),None)
            ih,il=c1["low"],c2["low"]
            if all(df30["high"].iloc[j]<ih for j in range(i+1,len(df30))):
                zones.append({"type":"BEAR","sig":"SELL","zh":ih,"zl":il,"fp":fp,"av":av,"i":i,"trend":"DOWNTREND"})
    return zones[-5:]

def s1_entry(pair, zones, df5, df1):
    if not zones or df5 is None or df1 is None: return None
    av5=atr(df5); av5v=float(av5.iloc[-1]) if not pd.isna(av5.iloc[-1]) else 0
    fr5=fractals(df5); p5=df5["close"].iloc[-1]; vok=vol_ok(df5)
    for z in reversed(zones):
        buf=z["av"]*0.2
        if not((z["zl"]-buf)<=p5<=(z["zh"]+buf)): continue
        # Refine to TF5 fractal
        ref=None
        for j in range(len(df5)-3,max(0,len(df5)-25),-1):
            if z["sig"]=="BUY" and fr5["fl"].iloc[j] and z["zl"]<=df5["low"].iloc[j]<=z["zh"]:
                ref=df5["low"].iloc[j]; break
            elif z["sig"]=="SELL" and fr5["fh"].iloc[j] and z["zl"]<=df5["high"].iloc[j]<=z["zh"]:
                ref=df5["high"].iloc[j]; break
        if ref is None: ref=z["zl"] if z["sig"]=="BUY" else z["zh"]
        # TF5 rejection
        l5=df5.iloc[-1]; r5=l5["high"]-l5["low"]; b5=abs(l5["close"]-l5["open"])
        wr=(r5-b5)/r5 if r5>0 else 0
        rej=((z["sig"]=="BUY" and wr>=0.5 and l5["close"]>l5["open"]) or
             (z["sig"]=="SELL" and wr>=0.5 and l5["close"]<l5["open"]))
        if not rej and not vok: continue
        if z["sig"]=="BUY":
            e=e_buy(pair,ref); rsl=df1["low"].iloc[-5:].min()-av5v
            s=sl_buy_f(pair,rsl); risk=e-s
        else:
            e=e_sell(pair,ref); rsl=df1["high"].iloc[-5:].max()+av5v
            s=sl_sell_f(pair,rsl); risk=s-e
        if risk<=0: continue
        t=calc_tp(e,s); pr=price2p(pair,risk); lot=calc_lot(pair,pr)
        return {"strategy":"S1-Imbalance+Fractal+EMA","sig":z["sig"],"pair":pair,
                "entry":e,"sl":s,"tp":t,"lot":lot,"pr":pr,
                "zh":z["zh"],"zl":z["zl"],"trend":z["trend"],
                "tf_confirm":"TF30 EMA+IMB / TF5 Fractal / TF1 limit",
                "notes":f"IMB {z['type']} frac@{z['fp']:.5f}" if z["fp"] else f"IMB {z['type']}"}
    return None

# ============================================================
# STRATEGI 2 — S&D RBR/DBR/DBD/RBD
# ============================================================
def s2_zones(df30, pair):
    if df30 is None or len(df30)<ATR_PERIOD+8: return []
    atr_s=atr(df30); zones=[]
    for i in range(3,len(df30)-2):
        av=atr_s.iloc[i]
        if pd.isna(av) or av<=0: continue
        cl,cb,co=df30.iloc[i-2],df30.iloc[i-1],df30.iloc[i]
        bb=abs(cb["close"]-cb["open"]); om=co["high"]-co["low"]
        if bb>0.5*av or om<STRONG_LEG_MULT*av: continue
        if co["close"]>co["open"]:  # demand
            zh=max(cb["high"],co["open"]); zl=min(cb["low"],co["open"]*0.999)
            fvg=co["low"]>cl["high"]*0.998
            ph=df30["high"].iloc[max(0,i-10):i].max()
            bos=any(df30["high"].iloc[j]>ph for j in range(i+1,min(i+5,len(df30))))
            if not(fvg or bos): continue
            fresh=all(df30["low"].iloc[j]>zl for j in range(i+1,len(df30)))
            patt="RBR" if cl["close"]>cl["open"] else "DBR"
            if fresh: zones.append({"type":"DEMAND","patt":patt,"sig":"BUY","zh":zh,"zl":zl,"av":av,"i":i})
        elif co["close"]<co["open"]:  # supply
            zl=min(cb["low"],co["open"]); zh=max(cb["high"],co["open"]*1.001)
            fvg=co["high"]<cl["low"]*1.002
            pl=df30["low"].iloc[max(0,i-10):i].min()
            bos=any(df30["low"].iloc[j]<pl for j in range(i+1,min(i+5,len(df30))))
            if not(fvg or bos): continue
            fresh=all(df30["high"].iloc[j]<zh for j in range(i+1,len(df30)))
            patt="DBD" if cl["close"]<cl["open"] else "RBD"
            if fresh: zones.append({"type":"SUPPLY","patt":patt,"sig":"SELL","zh":zh,"zl":zl,"av":av,"i":i})
    return zones[-5:]

def s2_entry(pair, zones, df5, df1):
    if not zones or df5 is None or df1 is None: return None
    av5=atr(df5); av5v=float(av5.iloc[-1]) if not pd.isna(av5.iloc[-1]) else 0
    p5=df5["close"].iloc[-1]; vok=vol_ok(df5)
    for z in reversed(zones):
        zh,zl=z["zh"],z["zl"]; buf=z["av"]*0.15
        if not((zl-buf)<=p5<=(zh+buf)): continue
        mid=(zh+zl)/2
        if z["sig"]=="BUY" and p5<mid: continue
        if z["sig"]=="SELL" and p5>mid: continue
        l5=df5.iloc[-1]; r5=l5["high"]-l5["low"]; b5=abs(l5["close"]-l5["open"])
        wr=(r5-b5)/r5 if r5>0 else 0
        rej=((z["sig"]=="BUY" and wr>=0.55 and l5["close"]>l5["open"]) or
             (z["sig"]=="SELL" and wr>=0.55 and l5["close"]<l5["open"]))
        if not rej and not vok: continue
        if z["sig"]=="BUY":
            e=e_buy(pair,zl); rsl=zl-av5v; s=sl_buy_f(pair,rsl); risk=e-s
        else:
            e=e_sell(pair,zh); rsl=zh+av5v; s=sl_sell_f(pair,rsl); risk=s-e
        if risk<=0: continue
        t=calc_tp(e,s); pr=price2p(pair,risk); lot=calc_lot(pair,pr)
        return {"strategy":"S2-SD-RBR/DBD","sig":z["sig"],"pair":pair,
                "entry":e,"sl":s,"tp":t,"lot":lot,"pr":pr,
                "zh":zh,"zl":zl,"patt":z["patt"],
                "tf_confirm":f"TF30 {z['patt']} / TF5 rej+FVG / TF1 trigger",
                "notes":f"{z['patt']} demand/supply"}
    return None

# ============================================================
# STRATEGI 3 — MOMENTUM PULLBACK + OB + PA
# ============================================================
_entry_count = {}

def s3_zones(df30, pair):
    if df30 is None or len(df30)<MA_SLOW+5: return []
    m9=ma(df30,MA_FAST); m21=ma(df30,MA_SLOW); zones=[]
    for i in range(2,len(df30)-1):
        if pd.isna(m9.iloc[i]) or pd.isna(m21.iloc[i]): continue
        up=m9.iloc[i]>m21.iloc[i]; dn=m9.iloc[i]<m21.iloc[i]
        cp,cc=df30.iloc[i-1],df30.iloc[i]
        if (up and cp["close"]<cp["open"] and cc["close"]>cc["open"] and cc["close"]>cp["high"]):
            zones.append({"type":"PB_BUY","sig":"BUY","zh":cc["high"],"zl":cp["low"],"trend":"UPTREND","i":i})
        elif (dn and cp["close"]>cp["open"] and cc["close"]<cc["open"] and cc["close"]<cp["low"]):
            zones.append({"type":"PB_SELL","sig":"SELL","zh":cp["high"],"zl":cc["low"],"trend":"DOWNTREND","i":i})
    return zones[-5:]

def s3_entry(pair, zones, df5, df1):
    if not zones or df5 is None or df1 is None: return None
    av5=atr(df5); av5v=float(av5.iloc[-1]) if not pd.isna(av5.iloc[-1]) else 0
    p5=df5["close"].iloc[-1]; vok=vol_ok(df5)
    for z in reversed(zones):
        zh,zl=z["zh"],z["zl"]; zk=(pair,z["type"],z["i"])
        if _entry_count.get(zk,0)>=2: continue
        buf=av5v*0.5
        if not((zl-buf)<=p5<=(zh+buf)): continue
        ob_l=ob_h=None
        for j in range(len(df5)-2,max(0,len(df5)-25),-1):
            c5=df5.iloc[j]
            if z["sig"]=="BUY" and c5["close"]<c5["open"] and zl<=c5["low"]<=zh:
                ob_l=c5["low"]; ob_h=c5["high"]; break
            elif z["sig"]=="SELL" and c5["close"]>c5["open"] and zl<=c5["high"]<=zh:
                ob_l=c5["low"]; ob_h=c5["high"]; break
        if ob_l is None or not vok: continue
        l1,p1=df1.iloc[-1],df1.iloc[-2]
        if z["sig"]=="BUY":
            cfm=(l1["close"]>l1["open"] and l1["close"]>p1["high"] and p1["close"]<p1["open"]) or vok
        else:
            cfm=(l1["close"]<l1["open"] and l1["close"]<p1["low"] and p1["close"]>p1["open"]) or vok
        if not cfm: continue
        if z["sig"]=="BUY":
            e=e_buy(pair,ob_l); rsl=df1["low"].iloc[-5:].min()-av5v
            s=sl_buy_f(pair,rsl); risk=e-s
        else:
            e=e_sell(pair,ob_h); rsl=df1["high"].iloc[-5:].max()+av5v
            s=sl_sell_f(pair,rsl); risk=s-e
        if risk<=0: continue
        t=calc_tp(e,s); pr=price2p(pair,risk); lot=calc_lot(pair,pr)
        _entry_count[zk]=_entry_count.get(zk,0)+1; en=_entry_count[zk]
        return {"strategy":"S3-Pullback+OB+PA","sig":z["sig"],"pair":pair,
                "entry":e,"sl":s,"tp":t,"lot":lot,"pr":pr,
                "zh":zh,"zl":zl,"ob_h":ob_h,"ob_l":ob_l,
                "trend":z["trend"],"en":en,
                "tf_confirm":"TF30 pullback / TF5 OB+Vol / TF1 PA",
                "notes":f"PB {z['trend']} OB:{ob_l:.5f}-{ob_h:.5f} E{en}/2"}
    return None

# ============================================================
# TELEGRAM
# ============================================================
def send_tg(msg):
    if TELEGRAM_BOT_TOKEN=="your_token_here": return False
    for a in range(1,MAX_RETRIES+1):
        try:
            r=requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML",
                      "disable_web_page_preview":True},timeout=10)
            r.raise_for_status(); return True
        except Exception as e:
            log.warning(f"Telegram attempt {a}: {e}")
            if a<MAX_RETRIES: time.sleep(3)
    return False

# ============================================================
# MESSAGES
# ============================================================
def sig_msg(sig):
    now=datetime.now(tz=timezone.utc)
    wib=(now+timedelta(hours=7)).strftime("%H:%M WIB")
    utc=now.strftime("%Y-%m-%d %H:%M UTC")
    pair=sig["pair"]; s=sig["sig"]
    arrow="🔼 BUY" if s=="BUY" else "🔽 SELL"
    pr=sig.get("pr",0); pt=pr*RR_RATIO
    lot=sig.get("lot",0.01); en=sig.get("en",1)
    etag=f" [Entry {en}/2]" if en>1 else ""
    zinfo=f"📦 Zone: ${sig.get('zl',0):.5f} – ${sig.get('zh',0):.5f}\n"
    if sig.get("ob_h"): zinfo+=f"📦 OB  : ${sig.get('ob_l',0):.5f} – ${sig.get('ob_h',0):.5f}\n"
    if sig.get("patt"): zinfo+=f"📋 Pola: {sig['patt']}\n"
    if sig.get("trend"): zinfo+=f"📈 Tren: {sig['trend']}\n"
    return (f"🚨 <b>[{sig['strategy']}]{etag}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💱 Pair     : <b>{pair}</b>\n"
            f"📌 Signal   : <b>{arrow}</b>\n"
            f"⏰ Waktu    : {utc} / {wib}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 <b>SETUP ENTRY (Limit Order)</b>\n"
            f"💰 Entry    : <b>${sig['entry']:.5f}</b>\n"
            f"🛑 Stop Loss: <b>${sig['sl']:.5f}</b> ({pr:.1f} pips)\n"
            f"🎯 Take Profit: <b>${sig['tp']:.5f}</b> ({pt:.1f} pips)\n"
            f"📐 RR       : 1:{RR_RATIO}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{zinfo}"
            f"📐 Spread   : +{SPREAD_PIPS} pips (sudah dalam entry & SL)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏦 Lot      : <b>{lot:.2f} lot cent</b>\n"
            f"💸 Risk     : ~${RISK_PER_TRADE_USD:.0f} ({RISK_PER_TRADE_USD/ACCOUNT_BALANCE*100:.1f}%)\n"
            f"💵 Profit TP: ~${RISK_PER_TRADE_USD*RR_RATIO:.0f}\n"
            f"🔍 Confirm  : {sig['tf_confirm']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>Pasang LIMIT ORDER di harga entry.\n"
            f"Bukan market order — hindari telat entry.\n"
            f"Bukan rekomendasi finansial.</i>")

def result_msg(trade, result, price):
    pnl=RISK_PER_TRADE_USD*RR_RATIO if result=="TP" else -RISK_PER_TRADE_USD
    icon="🎯" if result=="TP" else "🛑"; sign="+" if pnl>0 else ""
    return (f"{icon} <b>TRADE CLOSED — {result}</b>\n"
            f"💱 {trade['pair']} | {'🔼 BUY' if trade['sig']=='BUY' else '🔽 SELL'}\n"
            f"🎯 {trade['strategy']}\n"
            f"💰 Entry: ${trade['entry']:.5f} → Close: ${price:.5f}\n"
            f"💵 P&L : <b>{sign}${pnl:.2f}</b> ({sign}{pnl/ACCOUNT_BALANCE*100:.1f}%)\n"
            f"🏦 Saldo est: ~${ACCOUNT_BALANCE+pnl:.0f}")

def daily_msg(date_str, stats):
    wib=(datetime.now(tz=timezone.utc)+timedelta(hours=7)).strftime("%Y-%m-%d")
    msg=(f"📊 <b>DAILY REPORT — {wib} (07:00 WIB)</b>\n"
         f"🏦 Saldo: ${ACCOUNT_BALANCE} | Risk: ${RISK_PER_TRADE_USD}/trade\n"
         f"━━━━━━━━━━━━━━━━━━━━━━\n")
    tw=tl=0; tp=td=0.0
    for (strat,pair),s in sorted(stats.items()):
        tot=s["win"]+s["loss"]; wr=(s["win"]/tot*100) if tot>0 else 0
        tw+=s["win"]; tl+=s["loss"]; tp+=s["pnl_usd"]; td=max(td,s["max_dd"])
        sign="+" if s["pnl_usd"]>=0 else ""
        li="🔴"*min(s["max_ls"],5); wi="🟢"*min(s["max_ws"],5)
        msg+=(f"\n📌 <b>{strat.split('-')[0]} | {pair}</b>\n"
              f"   {tot} trade ✅{s['win']} ❌{s['loss']} WR:{wr:.1f}%\n"
              f"   P&L:{sign}${s['pnl_usd']:.2f} | DD:-${s['max_dd']:.0f}\n"
              f"   {wi}max{s['max_ws']}w {li}max{s['max_ls']}l\n")
    tt=tw+tl; twr=(tw/tt*100) if tt>0 else 0; sign="+" if tp>=0 else ""
    msg+=(f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
          f"📊 TOTAL {tt} trade ✅{tw} ❌{tl} WR:{twr:.1f}%\n"
          f"💵 P&L: <b>{sign}${tp:.2f}</b> | MaxDD:-${td:.0f}\n"
          f"🏦 Saldo est: ~${ACCOUNT_BALANCE+tp:.0f}")
    return msg

# ============================================================
# OPEN TRADE MONITOR
# ============================================================
_open_trades = []

# Cache harga per pair agar tidak double-request saat ada banyak open trade
_price_cache: dict = {}
_price_cache_ts: dict = {}
PRICE_CACHE_SEC = 30  # cache harga selama 30 detik

def get_cached_price(asset) -> float | None:
    """Ambil harga dengan cache — hindari request berulang per pair."""
    name = asset["name"]
    now  = time.time()
    if name in _price_cache and now - _price_cache_ts.get(name, 0) < PRICE_CACHE_SEC:
        return _price_cache[name]
    price = get_price(asset)
    if price:
        _price_cache[name]    = price
        _price_cache_ts[name] = now
    return price

# Waktu toleransi entry limit order (15 menit = 900 detik)
# Jika dalam 15 menit harga tidak menyentuh entry, anggap order tidak kena
ENTRY_TIMEOUT_SEC = int(os.getenv("ENTRY_TIMEOUT_SEC", "900"))

def monitor():
    """
    Monitor open trade dengan 2 tahap:

    TAHAP 1 — Cek apakah entry sudah kena:
      Trade dipasang sebagai limit order.
      Entry BUY kena jika harga <= entry (harga turun ke limit).
      Entry SELL kena jika harga >= entry (harga naik ke limit).
      Jika belum kena dan sudah timeout → batalkan trade.

    TAHAP 2 — Cek TP/SL (hanya jika entry sudah kena):
      Entry BUY: TP jika price >= tp, SL jika price <= sl.
      Entry SELL: TP jika price <= tp, SL jika price >= sl.
    """
    global _open_trades; still=[]
    now_ts = time.time()

    for t in _open_trades:
        price = get_cached_price(t["asset"])
        if price is None:
            still.append(t); continue

        entry     = t["entry"]
        sig       = t["sig"]
        entry_ts  = t.get("entry_ts", now_ts)
        filled    = t.get("filled", False)

        # ── TAHAP 1: Cek apakah limit order sudah kena ────────────────────────
        if not filled:
            # BUY limit: kena jika harga turun ke atau di bawah entry
            # SELL limit: kena jika harga naik ke atau di atas entry
            entry_hit = (
                (sig == "BUY"  and price <= entry * 1.0002) or  # toleransi 0.02%
                (sig == "SELL" and price >= entry * 0.9998)
            )
            if entry_hit:
                t["filled"]    = True
                t["fill_price"] = price
                t["fill_ts"]   = now_ts
                log.info(f"  ✅ {t['pair']} entry KENA @ ${price:.5f} (limit {sig} @ ${entry:.5f})")
                send_tg(
                    "✅ <b>ENTRY KENA!</b>\n"
                    f"💱 {t['pair']} | {'🔼 BUY' if sig=='BUY' else '🔽 SELL'}\n"
                    f"💰 Fill Price: ${price:.5f}\n"
                    f"🛑 SL: ${t['sl']:.5f} | 🎯 TP: ${t['tp']:.5f}\n"
                    "⏳ Menunggu TP/SL..."
                )
                still.append(t)
            elif now_ts - entry_ts > ENTRY_TIMEOUT_SEC:
                # Timeout — order tidak kena dalam batas waktu
                log.info(f"  ⏰ {t['pair']} order TIMEOUT ({ENTRY_TIMEOUT_SEC//60} menit) — dibatalkan")
                send_tg(
                    "⏰ <b>ORDER EXPIRED</b>\n"
                    f"💱 {t['pair']} | {'🔼 BUY' if sig=='BUY' else '🔽 SELL'}\n"
                    f"💰 Limit order @ ${entry:.5f} tidak kena\n"
                    f"   Harga sekarang: ${price:.5f}\n"
                    "❌ Order dibatalkan otomatis."
                )
                update_result(t["id"], "EXPIRED")
                # Tidak masuk still → trade dihapus dari open
            else:
                elapsed = int(now_ts - entry_ts)
                log.debug(f"  ⏳ {t['pair']} menunggu entry ({elapsed}s/{ENTRY_TIMEOUT_SEC}s) price={price:.5f} entry={entry:.5f}")
                still.append(t)
            continue

        # ── TAHAP 2: Cek TP/SL (entry sudah kena) ─────────────────────────────
        hit_tp = price >= t["tp"] if sig == "BUY" else price <= t["tp"]
        hit_sl = price <= t["sl"] if sig == "BUY" else price >= t["sl"]

        if hit_tp or hit_sl:
            res = "TP" if hit_tp else "SL"
            update_result(t["id"], res)
            send_tg(result_msg(t, res, price))
            log.info(f"  {'✅' if hit_tp else '❌'} {t['pair']} {res} @ ${price:.5f}")
            sk = get_alltime_streak(); k = (t["strategy"], t["pair"])
            if k in sk:
                s = sk[k]
                if s["cl"] >= 3:
                    icons = "🔴" * min(s["cl"], 7)
                    send_tg(f"⚠️ <b>LOSESTREAK!</b>\n"
                            f"💱 {t['pair']} | {t['strategy']}\n"
                            f"{icons} <b>{s['cl']}x berturut!</b>\n"
                            f"💸 Drawdown: -${s['cd']:.0f}\n"
                            f"🛑 Pertimbangkan pause & evaluasi.")
                if s["cw"] >= 3:
                    icons = "🟢" * min(s["cw"], 7)
                    send_tg(f"🔥 <b>WINSTREAK!</b> {t['pair']} {s['cw']}x! {icons}")
        else:
            still.append(t)

    _open_trades = still

# ============================================================
# SIGNAL COOLDOWN
# ============================================================
_last_sig={}
def can_send(pair,code):
    k=(pair,code); now=time.time()
    if now-_last_sig.get(k,0)<SIGNAL_COOLDOWN_H*3600: return False
    _last_sig[k]=now; return True

# ============================================================
# MAIN SCAN
# ============================================================
def has_active_trade(pair: str) -> bool:
    """Cek apakah pair ini masih punya open trade."""
    return any(t["pair"] == pair for t in _open_trades)

def scan():
    """
    SCAN 2-FASE untuk hemat API credit Twelve Data:

    FASE 1 — TF30 screening (1 request batch untuk semua pair):
      Fetch hanya TF30. Deteksi zona potensial (imbalance/S&D/pullback).
      Jika tidak ada zona → SKIP pair ini, tidak fetch TF5/TF1.
      Credit dipakai: 1 request untuk semua 4 pair forex (batch).

    FASE 2 — TF5 + TF1 (hanya jika ada zona di TF30):
      Fetch TF5 dan TF1 hanya untuk pair yang punya zona aktif.
      Credit dipakai: 2 request per pair yang lolos screening.

    Monitor open trade:
      Cek TP/SL hanya jika ada open trade.
      Harga dari TF1 yang sudah difetch (tidak request tambahan).

    Estimasi penggunaan harian:
      Worst case (semua pair punya zona): 1 + 4×2 = 9 req/scan
      Typical case (1-2 pair punya zona): 1 + 2×2 = 5 req/scan
      Scan per hari: 24×60/5 = 288 scan
      Typical daily: 288 × 5 = 1,440 req → masih melebihi limit 800

      Solusi tambahan: scan interval diperpanjang ke 15 menit
      → 96 scan/hari × worst 9 = 864 req (mendekati limit)
      → Typical 96 × 5 = 480 req/hari (aman di bawah 800)
    """
    global _price_cache
    log.info("=== SCAN DIMULAI ===")

    # ════════════════════════════════════════════════════════════
    # FASE 1: Fetch TF30 saja untuk semua pair forex (1 batch req)
    # ════════════════════════════════════════════════════════════
    forex_assets = [a for a in ASSETS if a["source"]=="twelve"]
    forex_syms   = [a["sym_td"] for a in forex_assets]

    log.info(f"  Fase 1: Fetch TF30 ({len(forex_syms)} pairs, 1 request)...")
    batch30 = fetch_twelve_batch(forex_syms, TF_HIGH, limit=60)

    # Screening: pair mana yang punya zona potensial?
    pairs_with_zones = []
    for asset in ASSETS:
        name = asset["name"]
        if asset["source"] == "twelve":
            df30 = batch30.get(asset["sym_td"])
        else:
            # BTC — Binance Vision tidak pakai kredit Twelve Data
            df30 = fetch_binance(asset["sym_bn"], TF_H_BN, 60)

        if df30 is None:
            log.debug(f"  {name}: no TF30 data, skip")
            continue

        # Cek apakah ada zona potensial di TF30
        z1 = s1_zones(df30, name)
        z2 = s2_zones(df30, name)
        z3 = s3_zones(df30, name)
        has_zone   = bool(z1 or z2 or z3)
        has_trade  = has_active_trade(name)

        if has_zone or has_trade:
            reason = []
            if z1: reason.append(f"S1:{len(z1)}zona")
            if z2: reason.append(f"S2:{len(z2)}zona")
            if z3: reason.append(f"S3:{len(z3)}zona")
            if has_trade: reason.append("open_trade")
            log.info(f"  {name}: AKTIF ({', '.join(reason)}) → fetch TF5+TF1")
            pairs_with_zones.append({
                "asset": asset, "df30": df30,
                "z1": z1, "z2": z2, "z3": z3,
                "has_trade": has_trade,
            })
        else:
            log.info(f"  {name}: tidak ada zona → SKIP (hemat 2 request)")

    if not pairs_with_zones:
        log.info("  Tidak ada pair aktif. Scan selesai. Credit dipakai: 1 request.")
        return

    # ════════════════════════════════════════════════════════════
    # FASE 2: Fetch TF5 + TF1 hanya untuk pair yang punya zona
    # ════════════════════════════════════════════════════════════
    # Kumpulkan pair forex yang butuh TF5/TF1
    active_forex = [p for p in pairs_with_zones if p["asset"]["source"]=="twelve"]
    active_forex_syms = [p["asset"]["sym_td"] for p in active_forex]

    batch5 = {}; batch1 = {}
    if active_forex_syms:
        log.info(f"  Fase 2: Fetch TF5 ({len(active_forex_syms)} pairs)...")
        batch5 = fetch_twelve_batch(active_forex_syms, TF_MID, limit=40)
        time.sleep(TWELVE_REQ_DELAY)
        log.info(f"  Fase 2: Fetch TF1 ({len(active_forex_syms)} pairs)...")
        batch1 = fetch_twelve_batch(active_forex_syms, TF_LOW, limit=15)

        # Update price cache dari TF1
        for p in active_forex:
            df1 = batch1.get(p["asset"]["sym_td"])
            if df1 is not None and len(df1) > 0:
                _price_cache[p["asset"]["name"]] = float(df1["close"].iloc[-1])

    total_req = 1 + (2 if active_forex_syms else 0)
    log.info(f"  Fase 2 selesai. Total credit dipakai: ~{total_req} request.")

    # ════════════════════════════════════════════════════════════
    # Proses entry untuk setiap pair aktif
    # ════════════════════════════════════════════════════════════
    for pdata in pairs_with_zones:
        asset = pdata["asset"]
        name  = asset["name"]
        df30  = pdata["df30"]

        if asset["source"] == "twelve":
            df5 = batch5.get(asset["sym_td"])
            df1 = batch1.get(asset["sym_td"])
        else:
            # BTC — fetch TF5/TF1 dari Binance (gratis)
            df5 = fetch_binance(asset["sym_bn"], TF_M_BN, 40)
            df1 = fetch_binance(asset["sym_bn"], TF_L_BN, 15)

        if df5 is None or df1 is None:
            log.warning(f"  {name}: TF5/TF1 tidak tersedia")
            continue

        # Update price cache dari TF1
        if asset["source"] == "binance" and len(df1) > 0:
            _price_cache[name] = float(df1["close"].iloc[-1])

        def send_sig(sig, code, _name=name, _asset=asset):
            if sig and can_send(_name, code):
                sid=save_signal(sig["pair"],sig["strategy"],sig["sig"],
                    sig["entry"],sig["sl"],sig["tp"],sig["tf_confirm"],
                    lot=sig.get("lot",0),pips_r=sig.get("pr",0),
                    zh=sig.get("zh"),zl=sig.get("zl"),notes=sig.get("notes",""))
                send_tg(sig_msg(sig))
                _open_trades.append({
                    "id":       sid,
                    "pair":     sig["pair"],
                    "strategy": sig["strategy"],
                    "sig":      sig["sig"],
                    "entry":    sig["entry"],
                    "sl":       sig["sl"],
                    "tp":       sig["tp"],
                    "asset":    _asset,
                    "entry_ts": time.time(),  # untuk timeout check
                    "filled":   False,         # belum kena entry
                })
                log.info(f"  [{code}] {_name} {sig['sig']} sent ID:{sid}")

        send_sig(s1_entry(name, pdata["z1"], df5, df1), "S1")
        send_sig(s2_entry(name, pdata["z2"], df5, df1), "S2")
        send_sig(s3_entry(name, pdata["z3"], df5, df1), "S3")

    # Monitor open trade (pakai price cache, tidak request baru)
    monitor()
    log.info("=== SCAN SELESAI ===")

# ============================================================
# DAILY REPORT
# ============================================================
_last_rpt=""
def is_trading_session() -> bool:
    """
    Cek apakah sekarang dalam sesi London atau US (WIB).
    Aktif  : 13:00–03:00 WIB = 06:00–20:00 UTC
    Istirahat: 03:00–13:00 WIB = 20:00–06:00 UTC (Sesi Asia)
    """
    now_utc = datetime.now(tz=timezone.utc)
    return SESSION_START_UTC <= now_utc.hour < SESSION_END_UTC

def check_report():
    """
    Kirim daily report tepat jam 07:00 WIB (00:00 UTC).
    Report berisi P&L, WR, streak, dan drawdown hari sebelumnya.
    """
    global _last_rpt
    now_utc = datetime.now(tz=timezone.utc)
    now_wib = now_utc + timedelta(hours=7)
    today   = now_wib.strftime("%Y-%m-%d")
    yd_utc  = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")

    # Kirim tepat jam 07:00–07:05 WIB (00:00–00:05 UTC)
    if now_utc.hour == DAILY_REPORT_HOUR and now_utc.minute < 6 and _last_rpt != today:
        stats = get_daily_stats(yd_utc)
        if stats:
            msg = daily_msg(yd_utc, stats)
        else:
            msg = (f"📊 <b>DAILY REPORT — {now_wib.strftime('%Y-%m-%d')} (07:00 WIB)</b>\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━\n"
                   f"Tidak ada trade closed kemarin.\n"
                   f"🏦 Saldo est: ${ACCOUNT_BALANCE}")
        send_tg(msg)
        log.info(f"Daily report dikirim ({yd_utc}).")
        _last_rpt = today

# ============================================================
# ENTRY POINT
# ============================================================
_session_notified = False  # flag notif pergantian sesi

def run():
    log.info("Forex Signal Bot START")
    log.info(f"Pairs: {', '.join(a['name'] for a in ASSETS)}")
    log.info(f"Risk: ${RISK_PER_TRADE_USD}/trade | RR 1:{RR_RATIO} | Spread {SPREAD_PIPS}pips")
    init_db()
    send_tg("🤖 <b>Forex Signal Bot AKTIF</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "💱 EURUSD | GBPUSD | USDJPY | XAUUSD | BTCUSD\n"
            "🎯 S1: Imbalance+Fractal+EMA21/50\n"
            "🎯 S2: S&D RBR/DBR/DBD/RBD\n"
            "🎯 S3: Momentum Pullback+OB+PA\n"
            f"📐 RR: 1:{RR_RATIO} | Spread: {SPREAD_PIPS}pips fixed\n"
            f"💸 Risk: ${RISK_PER_TRADE_USD}/trade | Saldo: ${ACCOUNT_BALANCE}\n"
            "📦 Signal = LIMIT ORDER, bukan market order.\n"
            "⚠️ Bukan rekomendasi finansial.")
    # Scan pertama hanya jika dalam sesi aktif
    if is_trading_session():
        try: scan()
        except Exception as e: log.error(f"First scan error: {e}", exc_info=True)
    else:
        now_wib = datetime.now(tz=timezone.utc) + timedelta(hours=7)
        log.info(f"Bot aktif tapi sekarang sesi Asia ({now_wib.strftime('%H:%M')} WIB) — menunggu London 13:00 WIB")
        send_tg(f"😴 <b>Bot Standby — Sesi Asia</b>\n"
                f"⏰ Sekarang: {now_wib.strftime('%H:%M')} WIB\n"
                f"🟢 Aktif kembali: 13:00 WIB (London)\n"
                f"💤 Tidak scan saat sesi Asia untuk hemat API credit.")

    while True:
        try:
            time.sleep(SCAN_INTERVAL_SEC)
            check_report()

            now_wib = datetime.now(tz=timezone.utc) + timedelta(hours=7)
            in_session = is_trading_session()

            if in_session:
                if not _session_notified:
                    log.info(f"🟢 Sesi aktif ({now_wib.strftime('%H:%M')} WIB) — mulai scan")
                    send_tg(f"🟢 <b>Sesi Aktif</b>\n"
                            f"⏰ {now_wib.strftime('%H:%M')} WIB\n"
                            f"📊 London/NY — bot mulai scan signal.")
                    _session_notified = True
                scan()
            else:
                if _session_notified:
                    log.info(f"😴 Sesi Asia ({now_wib.strftime('%H:%M')} WIB) — bot istirahat")
                    send_tg(f"😴 <b>Sesi Asia — Bot Istirahat</b>\n"
                            f"⏰ {now_wib.strftime('%H:%M')} WIB\n"
                            f"💤 Aktif kembali 13:00 WIB.\n"
                            f"💡 Hemat API credit Twelve Data.")
                    _session_notified = False
                else:
                    log.debug(f"Sesi Asia ({now_wib.strftime('%H:%M')} WIB) — skip scan")

                # Cek open trade meski di luar sesi (lindungi posisi aktif)
                if _open_trades:
                    log.info(f"  Ada {len(_open_trades)} open trade — monitor tetap jalan")
                    monitor()

        except KeyboardInterrupt: log.info("Bot stopped."); break
        except Exception as e: log.error(f"Error: {e}", exc_info=True); time.sleep(30)

if __name__=="__main__": run()
