# POLYMARKET 15M SIGNAL BOT v6
import logging, os, time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import pandas as pd
import requests
from dotenv import load_dotenv
from scipy.signal import find_peaks
load_dotenv()

TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","your_token_here")
TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID","your_chat_id_here")
WICK_RATIO_MIN=float(os.getenv("WICK_RATIO_MIN","0.68"))
FALSE_BREAK_MIN=float(os.getenv("FALSE_BREAK_MIN","0.0015"))
BODY_RATIO_MIN=float(os.getenv("BODY_RATIO_MIN","0.50"))
CLOSE_UPPER_MIN=float(os.getenv("CLOSE_UPPER_MIN","0.70"))
CLOSE_LOWER_MAX=float(os.getenv("CLOSE_LOWER_MAX","0.30"))
VOLUME_MULT=float(os.getenv("VOLUME_MULT","1.2"))
RSI_UP_MAX=float(os.getenv("RSI_UP_MAX","55"))
RSI_DOWN_MIN=float(os.getenv("RSI_DOWN_MIN","45"))
SNR_TOLERANCE=float(os.getenv("SNR_TOLERANCE","0.003"))
CANDLE_RANGE_MIN=float(os.getenv("CANDLE_RANGE_MIN","0.0015"))
EMA_FAST=9; EMA_SLOW=21; HTF_BIAS_TF="1h"
CONFIRM_WAIT_SEC=300; CONFIRM_TF="1m"; CONFIRM_BODY_MIN=0.55; CONFIRM_WICK_MIN=0.70
RSI_PERIOD=14; SNR_LOOKBACK=40; SNR_PEAK_DIST=5
MAX_RETRIES=3; RETRY_DELAY_SEC=5; STREAK_THRESHOLD=3; DAILY_REPORT_HOUR=0
BINANCE_KLINES_URL="https://data-api.binance.vision/api/v3/klines"
BINANCE_TICKER_URL="https://data-api.binance.vision/api/v3/ticker/price"
LOG_FILE="bot.log"
ET_OFFSET_HOURS=-4; ET_OFFSET=timedelta(hours=ET_OFFSET_HOURS); TF_INTERVAL=900

COINS=[
    {"symbol":"BTCUSDT","name":"BTC","active":True},
    {"symbol":"ETHUSDT","name":"ETH","active":True},
    {"symbol":"SOLUSDT","name":"SOL","active":True},
    {"symbol":"XRPUSDT","name":"XRP","active":True},
    {"symbol":"DOGEUSDT","name":"DOGE","active":True},
    {"symbol":"BNBUSDT","name":"BNB","active":True},
    {"symbol":"HYPEUSDT","name":"HYPE","active":False},
]

def setup_logger():
    logger=logging.getLogger("PolyBot"); logger.setLevel(logging.DEBUG)
    fmt=logging.Formatter("[%(asctime)s UTC] %(levelname)s | %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
    ch=logging.StreamHandler(); ch.setLevel(logging.INFO); ch.setFormatter(fmt); logger.addHandler(ch)
    fh=logging.FileHandler(LOG_FILE,encoding="utf-8"); fh.setLevel(logging.DEBUG); fh.setFormatter(fmt); logger.addHandler(fh)
    return logger
log=setup_logger()

def fetch_candles(symbol,interval,limit=100):
    for attempt in range(1,MAX_RETRIES+1):
        try:
            resp=requests.get(BINANCE_KLINES_URL,params={"symbol":symbol,"interval":interval,"limit":limit+1},timeout=15)
            resp.raise_for_status()
            raw=resp.json()
            if not isinstance(raw,list): return []
            return [[int(r[0]),float(r[1]),float(r[2]),float(r[3]),float(r[4]),float(r[5])] for r in raw][:-1]
        except requests.exceptions.HTTPError:
            if resp.status_code==429:
                w=RETRY_DELAY_SEC*attempt*2; log.warning(f"Rate limit, tunggu {w}s"); time.sleep(w)
            else: return []
        except Exception as e:
            log.warning(f"Fetch gagal {symbol} {interval} ({attempt}): {e}")
            if attempt<MAX_RETRIES: time.sleep(RETRY_DELAY_SEC)
    return []

def fetch_current_price(symbol):
    for attempt in range(1,MAX_RETRIES+1):
        try:
            resp=requests.get(BINANCE_TICKER_URL,params={"symbol":symbol},timeout=10)
            resp.raise_for_status(); return float(resp.json()["price"])
        except Exception as e:
            log.warning(f"Fetch price gagal {symbol} ({attempt}): {e}")
            if attempt<MAX_RETRIES: time.sleep(RETRY_DELAY_SEC)
    return None

def check_symbol_exists(symbol):
    try: return requests.get(BINANCE_TICKER_URL,params={"symbol":symbol},timeout=10).status_code==200
    except: return False

def get_polymarket_link(name,open_ts_ms):
    floored=(open_ts_ms//1000//900)*900
    return f"https://polymarket.com/event/{name.lower()}-updown-15m-{floored}"

def get_poly_window_end_utc(open_ts_ms):
    open_dt_et=datetime.fromtimestamp(open_ts_ms//1000,tz=timezone.utc)+ET_OFFSET
    et_epoch=int(open_dt_et.timestamp())
    et_floored=(et_epoch//TF_INTERVAL)*TF_INTERVAL
    return et_floored+TF_INTERVAL-int(ET_OFFSET.total_seconds())

def get_result_ready_ts(open_ts_ms): return get_poly_window_end_utc(open_ts_ms)+10
def fmt_utc(ts): return datetime.fromtimestamp(ts,tz=timezone.utc).strftime("%H:%M")
def fmt_et(ts): return (datetime.fromtimestamp(ts,tz=timezone.utc)+ET_OFFSET).strftime("%H:%M")

def calc_rsi(closes,period=14):
    if len(closes)<period+1: return 50.0
    df=pd.Series(closes); delta=df.diff()
    gain=delta.clip(lower=0); loss=(-delta).clip(lower=0)
    avg_g=gain.ewm(com=period-1,min_periods=period).mean()
    avg_l=loss.ewm(com=period-1,min_periods=period).mean()
    rs=avg_g/avg_l.replace(0,1e-10)
    return float((100-(100/(1+rs))).iloc[-1])

def calc_ema(closes,period):
    if len(closes)<period: return closes[-1] if closes else 0.0
    return float(pd.Series(closes).ewm(span=period,adjust=False).mean().iloc[-1])

def get_htf_bias(symbol):
    candles=fetch_candles(symbol,HTF_BIAS_TF,limit=50)
    if len(candles)<EMA_SLOW+5: return "NEUTRAL"
    closes=[c[4] for c in candles]
    ema9=calc_ema(closes,EMA_FAST); ema21=calc_ema(closes,EMA_SLOW); last_c=closes[-1]
    if last_c>ema9 and ema9>ema21: return "BULLISH"
    if last_c<ema9 and ema9<ema21: return "BEARISH"
    return "NEUTRAL"

def detect_snr(candles):
    if len(candles)<SNR_LOOKBACK: return {"supports":[],"resistances":[]}
    df=pd.DataFrame(candles[-SNR_LOOKBACK:],columns=["ts","open","high","low","close","volume"])
    highs=df["high"].values; lows=df["low"].values
    pi,_=find_peaks(highs,distance=SNR_PEAK_DIST); ti,_=find_peaks(-lows,distance=SNR_PEAK_DIST)
    return {"supports":sorted(set(round(v,6) for v in lows[ti])),"resistances":sorted(set(round(v,6) for v in highs[pi]))}

def find_nearest_snr(close,direction,snr):
    tol=close*SNR_TOLERANCE
    cands=snr["supports"] if direction=="UP" else snr["resistances"]
    ltype="Support" if direction=="UP" else "Resistance"
    nearest,ndiff=None,float("inf")
    for level in cands:
        d=abs(close-level)
        if d<=tol and d<ndiff: nearest,ndiff=level,d
    return nearest,ltype

def detect_pattern(symbol,candles):
    if len(candles)<SNR_LOOKBACK+5: return None
    c=candles[-1]; ts,o,h,l,close,vol=c
    cr=h-l
    if cr<1e-8 or cr/close<CANDLE_RANGE_MIN: return None
    body=abs(close-o); uw=h-max(o,close); lw=min(o,close)-l
    ur=uw/cr; lr=lw/cr; br=body/cr; cp=(close-l)/cr
    pvols=[cd[5] for cd in candles[-6:-1]]
    avgv=sum(pvols)/len(pvols) if pvols else 1
    vol_ok=vol>=VOLUME_MULT*avgv; vr=vol/avgv if avgv>0 else 0
    closes=[cd[4] for cd in candles]; rsi=calc_rsi(closes,RSI_PERIOD)
    snr=detect_snr(candles); result=None

    if lr>=WICK_RATIO_MIN:
        lvl,ltype=find_nearest_snr(close,"UP",snr)
        if lvl: result={"signal":"UP","pattern":"WICK","wick_pct":lr*100,"body_pct":None,"lvl":lvl,"lvl_type":ltype,"extra":f"Lower Wick {lr*100:.1f}%"}
    if not result and ur>=WICK_RATIO_MIN:
        lvl,ltype=find_nearest_snr(close,"DOWN",snr)
        if lvl: result={"signal":"DOWN","pattern":"WICK","wick_pct":ur*100,"body_pct":None,"lvl":lvl,"lvl_type":ltype,"extra":f"Upper Wick {ur*100:.1f}%"}
    if not result:
        for sup in snr["supports"]:
            sd=(sup-l)/close
            if l<sup and close>sup and sd>=FALSE_BREAK_MIN and cp>=0.5:
                result={"signal":"UP","pattern":"FALSE_BREAK","wick_pct":None,"body_pct":None,"lvl":sup,"lvl_type":"Support","extra":f"Spike {sd*100:.2f}% bawah Support ${sup:.4f}"}; break
    if not result:
        for res in reversed(snr["resistances"]):
            sd=(h-res)/close
            if h>res and close<res and sd>=FALSE_BREAK_MIN and cp<=0.5:
                result={"signal":"DOWN","pattern":"FALSE_BREAK","wick_pct":None,"body_pct":None,"lvl":res,"lvl_type":"Resistance","extra":f"Spike {sd*100:.2f}% atas Resistance ${res:.4f}"}; break
    if not result and vol_ok:
        if close>o and br>=BODY_RATIO_MIN and cp>=CLOSE_UPPER_MIN:
            lvl,ltype=find_nearest_snr(close,"UP",snr)
            result={"signal":"UP","pattern":"MOMENTUM","wick_pct":None,"body_pct":br*100,"lvl":lvl,"lvl_type":ltype or "Support","extra":f"Body {br*100:.1f}% Vol x{vr:.2f}"}
        elif close<o and br>=BODY_RATIO_MIN and cp<=CLOSE_LOWER_MAX:
            lvl,ltype=find_nearest_snr(close,"DOWN",snr)
            result={"signal":"DOWN","pattern":"MOMENTUM","wick_pct":None,"body_pct":br*100,"lvl":lvl,"lvl_type":ltype or "Resistance","extra":f"Body {br*100:.1f}% Vol x{vr:.2f}"}
    if not result: return None

    rsi_ok=(result["signal"]=="UP" and rsi<RSI_UP_MAX) or (result["signal"]=="DOWN" and rsi>RSI_DOWN_MIN)
    if not rsi_ok: log.debug(f"  DITOLAK RSI {rsi:.1f}"); return None
    if result["pattern"]=="MOMENTUM" and not vol_ok: log.debug(f"  DITOLAK Vol x{vr:.2f}"); return None
    htf=get_htf_bias(symbol)
    if not ((result["signal"]=="UP" and htf=="BULLISH") or (result["signal"]=="DOWN" and htf=="BEARISH")):
        log.debug(f"  DITOLAK HTF {htf}"); return None
    result.update({"candle":c,"rsi":rsi,"vol_ratio":vr,"vol_ok":vol_ok,"htf_bias":htf})
    return result

def wait_for_confirmation(symbol,signal,entry_price):
    log.info(f"  Menunggu konfirmasi 1M (max {CONFIRM_WAIT_SEC}s)...")
    start=time.time(); prev_low=entry_price; prev_high=entry_price; first=True
    while time.time()-start<CONFIRM_WAIT_SEC:
        time.sleep(30)
        c1m=fetch_candles(symbol,CONFIRM_TF,limit=10)
        if not c1m or len(c1m)<2: continue
        _,o1,h1,l1,c1,v1=c1m[-1]; r1=h1-l1
        if r1<1e-8: continue
        br1=abs(c1-o1)/r1; lw1=(min(o1,c1)-l1)/r1; uw1=(h1-max(o1,c1))/r1; cp1=(c1-l1)/r1
        pv1=[cd[5] for cd in c1m[-6:-1]]; av1=sum(pv1)/len(pv1) if pv1 else 1; vok1=v1>=av1
        if first:
            if signal=="UP" and c1>o1 and br1>=CONFIRM_BODY_MIN and cp1>=0.70:
                return True,"POLA A",f"Bullish kuat (body {br1*100:.0f}%)"
            if signal=="DOWN" and c1<o1 and br1>=CONFIRM_BODY_MIN and cp1<=0.30:
                return True,"POLA A",f"Bearish kuat (body {br1*100:.0f}%)"
            first=False
        if signal=="UP" and lw1>=CONFIRM_WICK_MIN and cp1>=0.55 and vok1:
            return True,"HAMMER",f"Hammer 1M (lower wick {lw1*100:.0f}%)"
        if signal=="DOWN" and uw1>=CONFIRM_WICK_MIN and cp1<=0.45 and vok1:
            return True,"SHOOTING STAR",f"Shooting Star 1M (upper wick {uw1*100:.0f}%)"
        margin=entry_price*0.0005
        if signal=="UP":
            if l1<prev_low: prev_low=l1
            if prev_low<entry_price and c1>entry_price+margin:
                return True,"POLA V",f"V-reversal (low ${prev_low:.4f} -> ${c1:.4f})"
        else:
            if h1>prev_high: prev_high=h1
            if prev_high>entry_price and c1<entry_price-margin:
                return True,"POLA V",f"V-reversal (high ${prev_high:.4f} -> ${c1:.4f})"
    return False,"TIMEOUT","Tidak ada konfirmasi dalam 5 menit"

def send_telegram(message):
    if TELEGRAM_BOT_TOKEN=="your_token_here": log.warning("Token belum dikonfigurasi."); return False
    for attempt in range(1,MAX_RETRIES+1):
        try:
            resp=requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT_ID,"text":message,"parse_mode":"HTML","disable_web_page_preview":True},timeout=10)
            resp.raise_for_status(); return True
        except Exception as e:
            log.warning(f"Telegram gagal ({attempt}): {e}")
            if attempt<MAX_RETRIES: time.sleep(RETRY_DELAY_SEC)
    return False

def build_presignal_message(sig,name,open_ts_ms):
    ts,o,h,l,c,_=sig["candle"]
    dt=datetime.fromtimestamp(ts/1000,tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    arrow="🐂 UP" if sig["signal"]=="UP" else "🐻 DOWN"
    pl=get_polymarket_link(name,open_ts_ms); we=get_poly_window_end_utc(open_ts_ms)
    lvl=f"${sig['lvl']:,.6f}" if sig["lvl"] else "N/A"
    return (f"👁️ <b>PRE-SIGNAL [{sig['pattern']}] — {arrow} [15M]</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ Candle  : {dt} UTC\n📊 Coin    : {name}\n"
            f"📊 OHLC    : O:<code>{o:.4f}</code> H:<code>{h:.4f}</code> L:<code>{l:.4f}</code> C:<code>{c:.4f}</code>\n"
            f"📌 {sig['lvl_type']} : {lvl}\n📋 Detail  : {sig['extra']}\n"
            f"📊 RSI {sig['rsi']:.1f} | Vol x{sig['vol_ratio']:.2f} | 1H {sig['htf_bias']}\n"
            f"🔗 Polymarket: {pl}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ <i>Menunggu konfirmasi 1M (max 5 menit)...</i>\n"
            f"🕐 Window tutup: {fmt_et(we)} ET / {fmt_utc(we)} UTC")

def build_signal_message(sig,name,open_ts_ms,confirm_type,confirm_detail,entry_price):
    ts,o,h,l,c,_=sig["candle"]
    dt=datetime.fromtimestamp(ts/1000,tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    now=datetime.now(tz=timezone.utc).strftime("%H:%M")
    arrow="🐂 UP" if sig["signal"]=="UP" else "🐻 DOWN"
    pl=get_polymarket_link(name,open_ts_ms); we=get_poly_window_end_utc(open_ts_ms)
    lvl=f"${sig['lvl']:,.6f}" if sig["lvl"] else "N/A"
    ce={"POLA V":"🔄","POLA A":"⚡","HAMMER":"🔨","SHOOTING STAR":"⭐"}.get(confirm_type,"✅")
    return (f"🚨 <b>SIGNAL AKTIF [{sig['pattern']}] — {arrow} [15M]</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ Candle  : {dt} UTC\n⏰ Entry   : {now} UTC\n📊 Coin    : {name}\n"
            f"📊 OHLC    : O:<code>{o:.4f}</code> H:<code>{h:.4f}</code> L:<code>{l:.4f}</code> C:<code>{c:.4f}</code>\n"
            f"📌 {sig['lvl_type']} : {lvl}\n📋 Pola    : {sig['extra']}\n"
            f"📊 RSI {sig['rsi']:.1f} | Vol x{sig['vol_ratio']:.2f} | 1H {sig['htf_bias']}\n"
            f"💰 Entry   : <b>${entry_price:.6f}</b>\n"
            f"{ce} Konfirmasi: <b>{confirm_type}</b> — {confirm_detail}\n"
            f"🔗 Polymarket: {pl}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 Window tutup: {fmt_et(we)} ET / {fmt_utc(we)} UTC\n"
            f"⏳ <i>Result dikirim tepat saat window ET tutup</i>")

def build_cancelled_message(name,signal,pattern,reason):
    return (f"❎ <b>DIBATALKAN [{pattern}] {'UP' if signal=='UP' else 'DOWN'} — {name} [15M]</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n💬 Alasan: {reason}\n💤 <i>Sinyal tidak dieksekusi.</i>")

def build_result_message(pending,result_price,now_str):
    ep=pending["entry_price"]; sig=pending["signal"]; pat=pending["pattern"]
    diff=result_price-ep; pct=(diff/ep*100) if ep>0 else 0; ds="+" if diff>0 else ""
    ok=(result_price>ep) if sig=="UP" else (result_price<ep)
    dirn="⬆️ Naik" if diff>0 else "⬇️ Turun"
    verdict="✅ <b>BENAR</b>" if ok else "❌ <b>SALAH</b>"
    emoji="🎯" if ok else "💔"; desc="Prediksi tepat!" if ok else "Prediksi meleset."
    now_dt=datetime.strptime(now_str,"%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    now_et=(now_dt+ET_OFFSET).strftime("%Y-%m-%d %H:%M")
    return (f"{emoji} <b>HASIL [{pat}] — {verdict} [15M]</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Coin       : {pending['name']}\n"
            f"📌 Sinyal     : <b>{'🐂 UP' if sig=='UP' else '🐻 DOWN'}</b>\n"
            f"🔄 Konfirmasi : {pending['confirm_type']}\n"
            f"⏰ Entry      : {pending['entry_time']} UTC\n"
            f"💰 Entry Price: <b>${ep:.6f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ Window End : {now_et} ET / {now_str} UTC\n"
            f"💰 Close Price: <b>${result_price:.6f}</b>\n"
            f"📈 Pergerakan : {dirn} <code>{ds}{diff:.6f}</code> (<code>{ds}{pct:.3f}%</code>)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n🏁 Verdict    : {verdict}\n💬 <i>{desc}</i>")

def build_daily_report(stats):
    now_str=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    msg=f"📊 <b>DAILY REPORT 15M — {now_str} (07:00 WIB)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
    tw=tl=tc=0
    for pat in ["WICK","FALSE_BREAK","MOMENTUM"]:
        s=stats.get(pat,{"win":0,"loss":0,"cancelled":0}); tot=s["win"]+s["loss"]
        wr=(s["win"]/tot*100) if tot>0 else 0; tw+=s["win"]; tl+=s["loss"]; tc+=s["cancelled"]
        msg+=f"\n📌 <b>[{pat}]</b>\n   ✅ Benar: {s['win']} | ❌ Salah: {s['loss']} | ❎ Cancel: {s['cancelled']}\n   📊 {tot} sinyal | WR: {wr:.1f}%\n"
    tt=tw+tl; twr=(tw/tt*100) if tt>0 else 0
    msg+=f"\n━━━━━━━━━━━━━━━━━━━━━━\n📊 TOTAL: {tt} sinyal | ❎ Cancel: {tc}\n✅ {tw} Benar | ❌ {tl} Salah\n🎯 Overall WR: {twr:.1f}%\n━━━━━━━━━━━━━━━━━━━━━━\n⚠️ <i>Data direset tiap hari.</i>"
    return msg

def check_streak(name,pattern,results):
    if len(results)<STREAK_THRESHOLD: return None
    recent=results[-STREAK_THRESHOLD:]
    if all(r is True for r in recent): return f"🔥 <b>WIN STREAK [{pattern}]</b>\n   {name} — {STREAK_THRESHOLD}x benar berturut! 🎯"
    if all(r is False for r in recent): return f"⚠️ <b>LOSE STREAK [{pattern}]</b>\n   {name} — {STREAK_THRESHOLD}x salah berturut! 🛑"
    return None

def seconds_until_next_15m():
    return (900-(time.time()%900))+3

def validate_coins():
    log.info("🔍 Validasi symbol Binance...")
    for coin in COINS:
        if check_symbol_exists(coin["symbol"]): log.info(f"   ✅ {coin['name']} OK")
        else: coin["active"]=False; log.warning(f"   ❌ {coin['name']} tidak tersedia, dinonaktifkan")

def run_bot():
    log.info("🚀 Polymarket 15M Signal Bot v6 AKTIF")
    log.info(f"   Fix v6: Polymarket link UTC floored | HTF Bias 1H wajib | False Break >={FALSE_BREAK_MIN*100:.2f}% | Retry mechanism")
    validate_coins()
    active_coins=[c for c in COINS if c["active"]]
    log.info(f"   Coins aktif: {', '.join(c['name'] for c in active_coins)}")

    pending_signals=[]
    daily_stats={p:{"win":0,"loss":0,"cancelled":0} for p in ["WICK","FALSE_BREAK","MOMENTUM"]}
    result_history=defaultdict(list); last_processed={}; daily_report_sent_date=None

    wait=seconds_until_next_15m()
    log.info(f"⏳ Scan pertama dalam {wait:.0f} detik...")
    time.sleep(wait)

    while True:
        try:
            now_utc=datetime.now(tz=timezone.utc); now_ts=int(time.time()); today_str=now_utc.strftime("%Y-%m-%d")

            if now_utc.hour==DAILY_REPORT_HOUR and now_utc.minute<6:
                if daily_report_sent_date!=today_str:
                    send_telegram(build_daily_report(daily_stats)); log.info("📊 Daily report dikirim.")
                    daily_stats={p:{"win":0,"loss":0,"cancelled":0} for p in ["WICK","FALSE_BREAK","MOMENTUM"]}
                    daily_report_sent_date=today_str

            still_pending=[]
            for ps in pending_signals:
                if now_ts<ps["result_ready_ts"]: still_pending.append(ps); continue
                log.info(f"  🏁 Result {ps['name']} [{ps['pattern']}]")
                rp=fetch_current_price(ps["symbol"])
                if rp is None: still_pending.append(ps); continue
                ns=datetime.fromtimestamp(now_ts,tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                send_telegram(build_result_message(ps,rp,ns))
                is_win=(rp>ps["entry_price"]) if ps["signal"]=="UP" else (rp<ps["entry_price"])
                pat=ps["pattern"]
                if is_win: daily_stats[pat]["win"]+=1; result_history[pat].append(True); log.info(f"  ✅ {ps['name']} [{pat}] BENAR")
                else: daily_stats[pat]["loss"]+=1; result_history[pat].append(False); log.info(f"  ❌ {ps['name']} [{pat}] SALAH")
                st=check_streak(ps["name"],pat,result_history[pat])
                if st: send_telegram(st)
            pending_signals=still_pending

            for coin in active_coins:
                key=coin["symbol"]; candles=fetch_candles(key,"15m",limit=SNR_LOOKBACK+15)
                if not candles: continue
                lot=candles[-1][0]
                if last_processed.get(key)==lot: continue
                last_processed[key]=lot
                dt_c=datetime.fromtimestamp(lot/1000,tz=timezone.utc).strftime("%H:%M")
                log.info(f"🔍 {coin['name']} 15M [{dt_c} UTC]")
                sig=detect_pattern(key,candles)
                if sig is None: log.debug(f"  {coin['name']} — tidak ada pola valid."); continue
                log.info(f"  🔔 PRE-SIGNAL {sig['signal']} [{sig['pattern']}] {coin['name']}")
                send_telegram(build_presignal_message(sig,coin["name"],lot))
                confirmed,ct,cd=wait_for_confirmation(key,sig["signal"],sig["candle"][4])
                if not confirmed:
                    send_telegram(build_cancelled_message(coin["name"],sig["signal"],sig["pattern"],cd))
                    daily_stats[sig["pattern"]]["cancelled"]+=1; log.info(f"  ❎ {coin['name']} [{sig['pattern']}] DIBATALKAN"); continue
                ep=fetch_current_price(key) or sig["candle"][4]
                log.info(f"  ✅ {ct}: {cd} | Entry ${ep:.6f}")
                send_telegram(build_signal_message(sig,coin["name"],lot,ct,cd,ep))
                rr=get_result_ready_ts(lot); we=get_poly_window_end_utc(lot)
                log.info(f"  📨 Result jam {fmt_et(we)} ET / {fmt_utc(we)} UTC")
                pending_signals.append({"signal":sig["signal"],"pattern":sig["pattern"],"symbol":key,
                    "name":coin["name"],"entry_price":ep,
                    "entry_time":datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "confirm_type":ct,"result_ready_ts":rr,"window_end_utc":we})

            wait=seconds_until_next_15m()
            log.info(f"⏳ Scan berikutnya dalam {wait:.0f} detik...\n")
            time.sleep(wait)

        except KeyboardInterrupt:
            log.info("🛑 Bot dihentikan. Sampai jumpa! 👋"); break
        except Exception as e:
            log.error(f"💥 Error: {e}",exc_info=True); time.sleep(15)

if __name__=="__main__":
    run_bot()
