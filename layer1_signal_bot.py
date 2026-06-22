"""
╔══════════════════════════════════════════════════════════════╗
║  LAYER1 SİNYAL BOTU — v1.0                                  ║
║  Coinler: BTC, ETH, SOL, AVAX, ATOM, INJ                   ║
║  Gerçek 21 günlük veriden türetilen eşikler                 ║
║  TP sıralaması garantili: SL<giriş<TP1<TP2<TP3             ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import logging
from datetime import datetime
import os

import ccxt
import pandas as pd
import pandas_ta as ta
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8696312422:AAFlQe3YwHgLQEBAfHy9sEm1MZacWA0Y9w8")
CHAT_ID        = os.environ.get("CHAT_ID", "1131322901")

CHECK_INTERVAL = 60

# ═══════════════════════════════════════════════════════════
#  COIN CONFIG — gerçek veriden (21 gün, 1H)
# ═══════════════════════════════════════════════════════════
COIN_CONFIG = {
    "BTC/USDT": {
        "name": "BTC",
        "long_rsi": 39,      # ort: 38.9
        "short_rsi": 57,     # ort: 57.2
        "vol_min": 1.2,      # ort: 1.27x
        "atr_sl": 0.5,
        "atr_tp2": 1.5,
        "atr_tp3": 2.5,
        "min_rr": 1.5,
        "leverage_strong": "x12-15",
        "leverage_mid":    "x8-10",
        "leverage_weak":   "x5-7",
        "note": "ATR:%0.71 — en düşük volatilite, en güvenli kaldıraç",
    },
    "ETH/USDT": {
        "name": "ETH",
        "long_rsi": 41,      # ort: 41.0
        "short_rsi": 58,     # ort: 58.4
        "vol_min": 1.3,      # ort: 1.37x
        "atr_sl": 0.5,
        "atr_tp2": 1.5,
        "atr_tp3": 2.5,
        "min_rr": 1.5,
        "leverage_strong": "x8-10",
        "leverage_mid":    "x6-8",
        "leverage_weak":   "x4-6",
        "note": "ATR:%0.94 — iyi R/R:2.79",
    },
    "SOL/USDT": {
        "name": "SOL",
        "long_rsi": 40,      # ort: 40.1
        "short_rsi": 61,     # ort: 60.7 — RSI ekstrem gidiyor
        "vol_min": 1.25,     # ort: 1.29x
        "atr_sl": 0.5,
        "atr_tp2": 1.6,
        "atr_tp3": 2.8,
        "min_rr": 1.5,
        "leverage_strong": "x8-10",
        "leverage_mid":    "x6-8",
        "leverage_weak":   "x4-6",
        "note": "ATR:%1.20 — RSI daha ekstrem gidiyor, SHORT için 60+ bekle",
    },
    "AVAX/USDT": {
        "name": "AVAX",
        "long_rsi": 36,      # ort: 36.3 — düşük RSI'da dönüyor
        "short_rsi": 54,     # ort: 54.3 — erken SHORT
        "vol_min": 1.4,      # ort: 1.43x
        "atr_sl": 0.6,
        "atr_tp2": 1.5,
        "atr_tp3": 2.5,
        "min_rr": 1.3,
        "leverage_strong": "x6-8",
        "leverage_mid":    "x4-6",
        "leverage_weak":   "x3-5",
        "note": "ATR:%1.31 — SHORT RSI 54'te gelir, beklemeden gir",
    },
    "ATOM/USDT": {
        "name": "ATOM",
        "long_rsi": 40,      # ort: 39.9
        "short_rsi": 56,     # ort: 56.3
        "vol_min": 1.2,      # ort: 1.23x
        "atr_sl": 0.5,
        "atr_tp2": 1.5,
        "atr_tp3": 2.5,
        "min_rr": 1.5,
        "leverage_strong": "x7-10",
        "leverage_mid":    "x5-7",
        "leverage_weak":   "x3-5",
        "note": "ATR:%1.17 — dengeli coin, tutarlı sinyal",
    },
    "INJ/USDT": {
        "name": "INJ",
        "long_rsi": 38,      # ort: 37.8
        "short_rsi": 54,     # ort: 54.0
        "vol_min": 1.3,      # ort: 1.31x
        "atr_sl": 0.7,       # Daha geniş SL — ATR büyük
        "atr_tp2": 1.8,
        "atr_tp3": 3.0,
        "min_rr": 1.3,
        "leverage_strong": "x4-5",
        "leverage_mid":    "x3-4",
        "leverage_weak":   "x2-3",
        "note": "ATR:%2.23 — yüksek volatilite, düşük kaldıraç şart!",
    },
}

# İndikatör
RSI_LEN   = 14
EMA_FAST  = 9
EMA_SLOW  = 21
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIG  = 9
BB_LEN    = 20
BB_STD    = 2.0
ATR_LEN   = 14
STOCH_K   = 14
STOCH_D   = 3

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

exchange = ccxt.binance({
    "enableRateLimit": True,
    "urls": {"api": {
        "public":  "https://api1.binance.com/api/v3",
        "private": "https://api1.binance.com/api/v3",
    }}
})

last_signals: dict = {}


def fetch(symbol: str, tf: str, limit: int = 120) -> pd.DataFrame:
    bars = exchange.fetch_ohlcv(symbol, tf, limit=limit)
    df = pd.DataFrame(bars, columns=["ts","open","high","low","close","volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]       = ta.rsi(c, length=RSI_LEN)
    df["ema_fast"]  = ta.ema(c, length=EMA_FAST)
    df["ema_slow"]  = ta.ema(c, length=EMA_SLOW)
    macd            = ta.macd(c, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
    df["macd_hist"] = macd.iloc[:, 1]
    bb              = ta.bbands(c, length=BB_LEN, std=BB_STD)
    df["bb_up"]     = bb.iloc[:, 2]
    df["bb_low"]    = bb.iloc[:, 0]
    df["bb_mid"]    = bb.iloc[:, 1]
    df["atr"]       = ta.atr(h, l, c, length=ATR_LEN)
    df["vol_ma"]    = v.rolling(20).mean()
    df["vol_ratio"] = v / df["vol_ma"]
    stoch           = ta.stoch(h, l, c, k=STOCH_K, d=STOCH_D)
    df["stoch_k"]   = stoch.iloc[:, 0]
    df["stoch_d"]   = stoch.iloc[:, 1]
    df["support"]   = l.rolling(20).min()
    df["resist"]    = h.rolling(20).max()
    df["is_green"]  = c > df["open"]
    df["is_red"]    = c < df["open"]
    return df


def confirmed_entry(df: pd.DataFrame, sig_type: str) -> bool:
    if len(df) < 3:
        return False
    trigger = df.iloc[-2]
    confirm = df.iloc[-1]
    if sig_type == "LONG":
        return (trigger["low"] <= trigger["bb_low"] * 1.005
                and confirm["is_green"]
                and confirm["close"] >= trigger["low"] * 0.995)
    else:
        return (trigger["high"] >= trigger["bb_up"] * 0.995
                and confirm["is_red"]
                and confirm["close"] <= trigger["high"] * 1.005)


def calc_levels(sig_type, price, atr, bb_mid, bb_up, bb_low, cfg):
    """TP sıralaması garantili hesaplama."""
    if sig_type == "LONG":
        sl  = price - atr * cfg["atr_sl"]
        # TP1: BB orta VEYA 1 ATR, hangisi giriş üstündeyse
        tp1 = bb_mid if bb_mid > price else price + atr * 1.0
        tp2 = max(price + atr * cfg["atr_tp2"], tp1 * 1.001)
        tp3_cand = bb_up if bb_up > tp2 else price + atr * cfg["atr_tp3"]
        tp3 = max(tp3_cand, tp2 * 1.001)
    else:
        sl  = price + atr * cfg["atr_sl"]
        tp1 = bb_mid if bb_mid < price else price - atr * 1.0
        tp2 = min(price - atr * cfg["atr_tp2"], tp1 * 0.999)
        tp3_cand = bb_low if bb_low < tp2 else price - atr * cfg["atr_tp3"]
        tp3 = min(tp3_cand, tp2 * 0.999)

    risk   = abs(price - sl)
    reward = abs(tp1 - price)
    rr     = reward / risk if risk > 0 else 0
    return {"sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3, "rr": rr}


def evaluate(symbol: str, h1: pd.DataFrame, m15: pd.DataFrame) -> dict | None:
    cfg  = COIN_CONFIG[symbol]
    row  = h1.iloc[-1]
    m    = m15.iloc[-1]
    m2   = m15.iloc[-2]
    price= row["close"]

    for v in [row["rsi"], row["bb_mid"], row["atr"]]:
        if pd.isna(v):
            return None

    atr = row["atr"]

    # ── LONG (veriden: Destek+Teyit+BB en güçlü) ─────────
    lc = {
        "RSI aşırı satım":    row["rsi"] < cfg["long_rsi"],
        "BB alt / Destek":    (price <= row["bb_low"] * 1.006) or
                               (price <= row["support"] * 1.01),
        "MACD yukarı":        row["macd_hist"] > 0,
        "15M MACD/EMA":       (m["macd_hist"] > 0) or
                               (m["ema_fast"] > m["ema_slow"] and
                                m2["ema_fast"] <= m2["ema_slow"]),
        "Stoch dönüş":        row["stoch_k"] < 30 and row["stoch_k"] > row["stoch_d"],
        "Hacim güçlü":        row["vol_ratio"] >= cfg["vol_min"],
        "Teyit mumu":         confirmed_entry(m15, "LONG"),
    }

    # ── SHORT (veriden: Direnç+Teyit+MACD en güçlü) ──────
    sc = {
        "RSI aşırı alım":     row["rsi"] > cfg["short_rsi"],
        "BB üst / Direnç":    (price >= row["bb_up"] * 0.994) or
                               (price >= row["resist"] * 0.99),
        "MACD aşağı":         row["macd_hist"] < 0,
        "15M MACD/EMA":       (m["macd_hist"] < 0) or
                               (m["ema_fast"] < m["ema_slow"] and
                                m2["ema_fast"] >= m2["ema_slow"]),
        "Stoch dönüş":        row["stoch_k"] > 70 and row["stoch_k"] < row["stoch_d"],
        "Hacim güçlü":        row["vol_ratio"] >= cfg["vol_min"],
        "Teyit mumu":         confirmed_entry(m15, "SHORT"),
    }

    ls, ss = sum(lc.values()), sum(sc.values())
    if ls < 3 and ss < 3:
        return None
    if ls == ss:
        return None

    sig_type = "LONG" if ls > ss else "SHORT"
    conds    = lc if sig_type == "LONG" else sc
    score    = ls if sig_type == "LONG" else ss

    levels = calc_levels(sig_type, price, atr,
                         row["bb_mid"], row["bb_up"], row["bb_low"], cfg)

    if levels["rr"] < cfg["min_rr"]:
        return None

    # Güç & kaldıraç
    if score >= 6:
        strength = "💎 ÇOK GÜÇLÜ"
        leverage = cfg["leverage_strong"]
    elif score >= 5:
        strength = "⭐⭐⭐ GÜÇLÜ"
        leverage = cfg["leverage_mid"]
    elif score >= 4:
        strength = "⭐⭐ ORTA"
        leverage = cfg["leverage_mid"]
    else:
        strength = "⭐ ZAYIF"
        leverage = cfg["leverage_weak"]

    return {
        "symbol":    symbol,
        "name":      cfg["name"],
        "type":      sig_type,
        "price":     price,
        "sl":        levels["sl"],
        "tp1":       levels["tp1"],
        "tp2":       levels["tp2"],
        "tp3":       levels["tp3"],
        "rr":        levels["rr"],
        "score":     score,
        "strength":  strength,
        "leverage":  leverage,
        "rsi":       row["rsi"],
        "vol_ratio": row["vol_ratio"],
        "atr_pct":   atr / price * 100,
        "conds":     {k: v for k, v in conds.items() if v},
        "note":      cfg["note"],
    }


def format_signal(sig: dict) -> str:
    is_long = sig["type"] == "LONG"
    emoji   = "🟢" if is_long else "🔴"
    p       = sig["price"]

    def pct(t): return (t - p) / p * 100

    cond_lines = "\n".join(f"  ✓ {c}" for c in sig["conds"])
    now        = datetime.now().strftime("%d.%m %H:%M")

    return (
        f"{emoji} *{sig['name']}/USDT — {sig['type']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Giriş: `{p:,.4f}`\n"
        f"🛑 SL:    `{sig['sl']:,.4f}` ({pct(sig['sl']):+.2f}%)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 TP1:  `{sig['tp1']:,.4f}` ({pct(sig['tp1']):+.2f}%) ← BB Orta\n"
        f"   _TP1'de %50 kapat → SL'yi girişe çek_\n"
        f"🎯 TP2:  `{sig['tp2']:,.4f}` ({pct(sig['tp2']):+.2f}%)\n"
        f"🎯 TP3:  `{sig['tp3']:,.4f}` ({pct(sig['tp3']):+.2f}%) ← BB Üst/Alt\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 R/R:   `{sig['rr']:.1f}:1`\n"
        f"📡 RSI:   `{sig['rsi']:.0f}`\n"
        f"💹 Hacim: `{sig['vol_ratio']:.1f}x`\n"
        f"📐 ATR:   `{sig['atr_pct']:.2f}%`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Koşullar ({sig['score']}/7):\n{cond_lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ {sig['strength']}\n"
        f"💪 Kaldıraç: `{sig['leverage']}`\n"
        f"💡 _{sig['note']}_\n"
        f"⏰ {now}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ _Finansal tavsiye değildir._"
    )


def format_status(data: dict) -> str:
    now   = datetime.now().strftime("%H:%M")
    lines = [f"📊 *Layer1 Durum* — {now}\n"]
    for symbol, (h1, cfg) in data.items():
        row = h1.iloc[-1]
        rsi = row["rsi"]
        p   = row["close"]
        bbl = (p - row["bb_low"]) / p * 100
        bbu = (row["bb_up"] - p)  / p * 100

        if rsi < cfg["long_rsi"] - 5:    icon = "🔵🔵"
        elif rsi < cfg["long_rsi"]:       icon = "🔵"
        elif rsi > cfg["short_rsi"] + 5:  icon = "🟠🟠"
        elif rsi > cfg["short_rsi"]:      icon = "🟠"
        else:                              icon = "⚪"

        lines.append(
            f"{icon} *{cfg['name']}* `{p:,.2f}` RSI=`{rsi:.0f}` "
            f"↓BB={bbl:.1f}% ↑BB={bbu:.1f}%"
        )
    return "\n".join(lines)


def is_duplicate(symbol: str, sig: dict) -> bool:
    if symbol not in last_signals:
        return False
    prev = last_signals[symbol]
    if prev["type"] != sig["type"]:
        return False
    return abs(sig["price"] - prev["price"]) / prev["price"] < 0.003


status_counter = 0

async def scan(bot) -> None:
    global status_counter
    data = {}
    try:
        for symbol, cfg in COIN_CONFIG.items():
            try:
                h1  = add_indicators(fetch(symbol, "1h",  limit=80))
                m15 = add_indicators(fetch(symbol, "15m", limit=80))
                data[symbol] = (h1, cfg)

                sig = evaluate(symbol, h1, m15)
                if sig and not is_duplicate(symbol, sig):
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=format_signal(sig),
                        parse_mode="Markdown")
                    last_signals[symbol] = {"type": sig["type"], "price": sig["price"]}
                    log.info("Sinyal: %s %s @ %.4f RR=%.1f",
                             cfg["name"], sig["type"], sig["price"], sig["rr"])
            except Exception as e:
                log.error("Hata (%s): %s", symbol, e)
            await asyncio.sleep(0.5)

        status_counter += 1
        if status_counter % 60 == 0 and data:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=format_status(data),
                parse_mode="Markdown")
    except Exception as e:
        log.error("Genel hata: %s", e)


async def background_loop(bot) -> None:
    while True:
        await scan(bot)
        await asyncio.sleep(CHECK_INTERVAL)


async def cmd_start(update, context):
    coins = ", ".join(v["name"] for v in COIN_CONFIG.values())
    await update.message.reply_text(
        f"🤖 *Layer1 Sinyal Botu*\n\n"
        f"Takip: `{coins}`\n\n"
        f"/durum — Anlık RSI + BB mesafeleri\n"
        f"/kural — Coin bazında eşikler\n"
        f"/son   — Son sinyaller",
        parse_mode="Markdown")


async def cmd_durum(update, context):
    try:
        d = {}
        for symbol, cfg in COIN_CONFIG.items():
            h1 = add_indicators(fetch(symbol, "1h", limit=80))
            d[symbol] = (h1, cfg)
        await update.message.reply_text(format_status(d), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Hata: {e}")


async def cmd_kural(update, context):
    lines = ["📋 *Layer1 Sinyal Eşikleri*\n"]
    for symbol, cfg in COIN_CONFIG.items():
        lines.append(
            f"*{cfg['name']}*\n"
            f"  LONG RSI < {cfg['long_rsi']} | SHORT RSI > {cfg['short_rsi']}\n"
            f"  Hacim > {cfg['vol_min']}x | Min R/R: {cfg['min_rr']}\n"
            f"  Güçlü: `{cfg['leverage_strong']}` | Orta: `{cfg['leverage_mid']}`\n"
            f"  💡 _{cfg['note']}_\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_son(update, context):
    if not last_signals:
        await update.message.reply_text("Henüz sinyal yok.")
        return
    lines = ["*Son sinyaller:*\n"]
    for symbol, sig in last_signals.items():
        lines.append(
            f"{COIN_CONFIG[symbol]['name']}: *{sig['type']}* @ `{sig['price']:,.4f}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def post_init(application) -> None:
    asyncio.create_task(background_loop(application.bot))
    coins = ", ".join(v["name"] for v in COIN_CONFIG.values())
    try:
        await application.bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"✅ *Layer1 Botu başlatıldı!*\n"
                f"🪙 `{coins}`\n"
                f"⏱ Her {CHECK_INTERVAL}s tarama\n"
                f"✓ Gerçek veriden eşikler\n"
                f"✓ TP sıralaması garantili\n\n"
                f"/durum yazarak başla."
            ),
            parse_mode="Markdown")
    except Exception as e:
        log.warning("Başlangıç mesajı hatası: %s", e)


def main():
    app = (ApplicationBuilder()
           .token(TELEGRAM_TOKEN)
           .post_init(post_init)
           .build())
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("durum", cmd_durum))
    app.add_handler(CommandHandler("kural", cmd_kural))
    app.add_handler(CommandHandler("son",   cmd_son))
    log.info("Layer1 Botu başlatıldı.")
    app.run_polling()


if __name__ == "__main__":
    main()
