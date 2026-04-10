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
SCAN_INTERVAL_SEC = 300; DAILY_REPORT_HOUR = 0
SIGNAL_COOLDOWN_H = 2; DB_PATH = "signals.db"; LOG_FILE = "bot.log"
MAX_RETRIES = 3

# Pip sizes
PIP_SIZE = {"EURUSD":0.0001,"GBPUSD":0.0001,"USDJPY":0.01,
            "XAUUSD":0.01,"BTCUSD":1.00}

# Lot value per pip per 0.01 lot (cent account)
LOT_VALUE_PER_PIP = {"EURUSD":0.001,"GBPUSD":0.001,"USDJPY":0.001,
                     "XAUUSD":0.01,"BTCUSD":0.1}

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
    pnl_usd = RISK_PER_TRADE_USD * RR_RATIO if result=="TP" else -RISK_PER_TRADE_USD
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
    lpv = LOT_VALUE_PER_PIP.get(pair, 0.001)
    if pips_r <= 0 or lpv <= 0: return 0.01
    lot = RISK_PER_TRADE_USD / (pips_r * lpv)
    return max(0.01, round(lot / 0.01) * 0.01)

def fetch_twelve(sym_td, interval, limit=80):
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(TWELVE_DATA_URL, params={
                "symbol":sym_td,"interval":interval,
                "outputsize":limit+1,"apikey":TWELVE_DATA_KEY,"format":"JSON"
            }, timeout=15); r.raise_for_status()
            d = r.json()
            if "values" not in d:
                log.warning(f"Twelve Data {sym_td}: {d.get('message','no data')}"); return None
            df = pd.DataFrame(d["values"]).rename(columns={"datetime":"timestamp"})
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            for c in ["open","high","low","close","volume"]:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)
            df = df.sort_values("timestamp").reset_index(drop=True)
            return df.iloc[:-1]
        except Exception as e:
            log.warning(f"Twelve {sym_td} {interval} attempt {attempt}: {e}")
            if attempt < MAX_RETRIES: time.sleep(5)
    return None

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
    try:
        if asset["source"]=="binance":
            r = requests.get(BINANCE_TICKER_URL,params={"symbol":asset["sym_bn"]},timeout=10)
            return float(r.json()["price"])
        r = requests.get(TWELVE_PRICE_URL,
            params={"symbol":asset["sym_td"],"apikey":TWELVE_DATA_KEY},timeout=10)
        return float(r.json()["price"])
    except: return None

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

def monitor():
    global _open_trades; still=[]
    for t in _open_trades:
        price=get_price(t["asset"])
        if price is None: still.append(t); continue
        hit_tp = price>=t["tp"] if t["sig"]=="BUY" else price<=t["tp"]
        hit_sl = price<=t["sl"] if t["sig"]=="BUY" else price>=t["sl"]
        if hit_tp or hit_sl:
            res="TP" if hit_tp else "SL"
            update_result(t["id"],res)
            send_tg(result_msg(t,res,price))
            log.info(f"  {'✅' if hit_tp else '❌'} {t['pair']} {res}")
            sk=get_alltime_streak(); k=(t["strategy"],t["pair"])
            if k in sk:
                s=sk[k]
                if s["cl"]>=3:
                    icons="🔴"*min(s["cl"],7)
                    send_tg(f"⚠️ <b>LOSESTREAK!</b>\n"
                            f"💱 {t['pair']} | {t['strategy']}\n"
                            f"{icons} <b>{s['cl']}x berturut!</b>\n"
                            f"💸 Drawdown: -${s['cd']:.0f}\n"
                            f"🛑 Pertimbangkan pause & evaluasi.")
                if s["cw"]>=3:
                    icons="🟢"*min(s["cw"],7)
                    send_tg(f"🔥 <b>WINSTREAK!</b> {t['pair']} {s['cw']}x! {icons}")
        else: still.append(t)
    _open_trades=still

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
def scan():
    log.info("Scan 5 pairs x 3 strategies...")
    for asset in ASSETS:
        name=asset["name"]; log.info(f"  {name}")
        df30=fetch_ohlcv(asset,TF_HIGH,TF_H_BN,80)
        df5 =fetch_ohlcv(asset,TF_MID, TF_M_BN,60)
        df1 =fetch_ohlcv(asset,TF_LOW, TF_L_BN,20)
        if df30 is None: log.warning(f"  No data {name}"); continue

        def send_sig(sig, code):
            if sig and can_send(name, code):
                sid=save_signal(sig["pair"],sig["strategy"],sig["sig"],
                    sig["entry"],sig["sl"],sig["tp"],sig["tf_confirm"],
                    lot=sig.get("lot",0),pips_r=sig.get("pr",0),
                    zh=sig.get("zh"),zl=sig.get("zl"),notes=sig.get("notes",""))
                send_tg(sig_msg(sig))
                _open_trades.append({"id":sid,"pair":sig["pair"],"strategy":sig["strategy"],
                    "sig":sig["sig"],"entry":sig["entry"],"sl":sig["sl"],"tp":sig["tp"],
                    "asset":asset})
                log.info(f"  [{code}] {name} {sig['sig']} sent ID:{sid}")

        send_sig(s1_entry(name, s1_zones(df30,name), df5, df1), "S1")
        send_sig(s2_entry(name, s2_zones(df30,name), df5, df1), "S2")
        send_sig(s3_entry(name, s3_zones(df30,name), df5, df1), "S3")

    monitor()

# ============================================================
# DAILY REPORT
# ============================================================
_last_rpt=""
def check_report():
    global _last_rpt
    now=datetime.now(tz=timezone.utc); yd=(now-timedelta(days=1)).strftime("%Y-%m-%d")
    if now.hour==DAILY_REPORT_HOUR and now.minute<6 and _last_rpt!=yd:
        stats=get_daily_stats(yd)
        msg=daily_msg(yd,stats) if stats else f"📊 Daily Report {yd}: tidak ada trade."
        send_tg(msg); log.info(f"Daily report {yd} sent."); _last_rpt=yd

# ============================================================
# ENTRY POINT
# ============================================================
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
    try: scan()
    except Exception as e: log.error(f"First scan error: {e}", exc_info=True)
    while True:
        try:
            time.sleep(SCAN_INTERVAL_SEC)
            check_report(); scan()
        except KeyboardInterrupt: log.info("Bot stopped."); break
        except Exception as e: log.error(f"Error: {e}", exc_info=True); time.sleep(30)

if __name__=="__main__": run()
