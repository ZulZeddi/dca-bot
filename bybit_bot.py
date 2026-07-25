import os
import sys
import uuid
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import argparse
import requests
import time
from loguru import logger
from supabase import create_client
import pandas as pd

logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True,
           format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

# === Settings ===
load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
DAILY_USD = float(os.getenv('DAILY_USD', 0.0))

TABLE_URL = os.getenv('TABLE_URL')
TABLE_PASSWORD = os.getenv('TABLE_PASSWORD')
supabase_client = None
if TABLE_URL and TABLE_PASSWORD:
    supabase_client = create_client(TABLE_URL, TABLE_PASSWORD)
else:
    logger.warning("Supabase credentials missing. Database logging will be disabled.")

USD_TYPE = os.getenv('STABLECOIN', 'USDT').split(',')[0].strip()

BUFFER_MULTIPLIER = 1.015
MIN_REDEMPTION_USD = 10.0

# ─── Safety / live-readiness controls ──────────────────────────────────────────
# DRY_RUN defaults ON: the bot simulates and places NO real orders until you
# explicitly set DRY_RUN=false (or pass --live). This is a money bot — fail safe.
DRY_RUN = os.getenv('DRY_RUN', 'true').lower() == 'true'
TESTNET = os.getenv('TESTNET', 'false').lower() == 'true'
# Hard ceiling on total stablecoin spent in a single run, regardless of balance,
# multipliers, or a config typo. Default = 3x the nominal daily budget. Set an
# explicit absolute value if your allocation weights do not sum to ~1.
MAX_SPEND_PER_RUN = float(os.getenv('MAX_SPEND_PER_RUN', max(DAILY_USD * 3.0, 1.0)))
# Cap on the product of all per-coin buy multipliers (VA × market boost × rebalance).
MAX_COMBINED_MULTIPLIER = float(os.getenv('MAX_COMBINED_MULTIPLIER', 3.0))
# Minimum residual (USD) worth routing through Convert after a partial spot fill.
MIN_CONVERT_USD = float(os.getenv('MIN_CONVERT_USD', 1.0))
# Run-lock to stop a duplicate/concurrent invocation from double-spending.
LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dca_bot.lock')
LOCK_STALE_SECONDS = int(os.getenv('LOCK_STALE_SECONDS', 1800))


# ─── Utilities ────────────────────────────────────────────────────────────────

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message,
                                        'parse_mode': 'Markdown'}, timeout=10)
        # Dynamic content (exception strings, symbols) can contain Markdown
        # metacharacters that make Telegram reject the message with HTTP 400 —
        # retry as plain text so a money-bot alert is never silently dropped.
        if resp.status_code != 200:
            requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message}, timeout=10)
    except Exception as e:
        logger.error(f"Telegram error: {e}")


def get_coin_balance(session, coin, account_type='UNIFIED'):
    """Returns wallet balance for coin, or None on API error (distinct from actual zero)."""
    try:
        resp = session.get_wallet_balance(accountType=account_type, coin=coin)
        if resp['result']['list'] and resp['result']['list'][0]['coin']:
            return round(float(resp['result']['list'][0]['coin'][0]['walletBalance']), 6)
        return 0.0
    except Exception as e:
        logger.error(f"Error getting {coin} balance: {e}")
        return None


def get_staked_balance(session, coin, category):
    """Returns staked/earn balance for coin in the given Earn category, 0.0 on error."""
    try:
        pos = session.get_staked_position(category=category, coin=coin)
        total = 0.0
        for p in (pos.get('result', {}).get('list') or []):
            amt = p.get('redeemableAmount') or p.get('amount') or 0
            total += float(amt or 0)
        return total
    except Exception as e:
        logger.error(f"Error getting staked {coin} ({category}) balance: {e}")
        return 0.0


def get_total_coin_holdings(session, coin):
    """
    Total holdings = liquid spot wallet balance + staked balance in the coin's
    configured Earn category. Returns None on spot-read API error (so callers can
    tell an API failure apart from a genuine zero), else the summed float.

    Balance-driven SIZING (portfolio weights, value averaging, rebalancing) must
    use this, NOT get_coin_balance(): stake_idle_coin() parks bought coins in
    Earn, so a spot-only read makes them look unbought and the bot over-buys
    them every run.
    """
    spot = get_coin_balance(session, coin)
    if spot is None:
        return None
    cfg = get_stake_config().get(coin.upper())
    staked = get_staked_balance(session, coin, cfg['category']) if cfg else 0.0
    return round(spot + staked, 8)


def get_current_price(session, coin):
    # Try the configured stablecoin pair first; fall back to the USDT pair as a
    # ~$1-parity price reference so a coin without a {coin}{USD_TYPE} spot pair
    # (e.g. TONUSDC) is still priced for guards/VA and can be bought via Convert.
    for quote in dict.fromkeys((USD_TYPE, 'USDT')):
        try:
            ticker = session.get_tickers(category='spot', symbol=f"{coin}{quote}")
            if ticker['result']['list']:
                price = float(ticker['result']['list'][0]['lastPrice'])
                if quote != USD_TYPE:
                    logger.warning(f"{coin}{USD_TYPE} unavailable — using {coin}{quote} as price reference")
                return price
        except Exception as e:
            logger.error(f"Error getting price for {coin}{quote}: {e}")
    return None


def log_trade(symbol, quantity, price, total_usd):
    if not supabase_client:
        logger.error("Supabase not initialized. Trade not logged.")
        return
    try:
        now = datetime.now(timezone.utc)
        data = {
            'trade_id': f"{symbol}-{now.strftime('%Y%m%d%H%M%S%f')}",
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'quantity': float(quantity),
            'price': float(price),
            'total_usd': float(total_usd),
        }
        supabase_client.table('trade_log').insert(data).execute()
    except Exception as e:
        logger.error(f"Supabase logging error: {e}")
        send_telegram(f"❌ Supabase logging error: {e}")


# ─── Idempotency / run safety ──────────────────────────────────────────────────

def acquire_run_lock():
    """
    Atomically create a lock file so a duplicate/concurrent invocation cannot
    double-spend. Returns True if acquired. A stale lock (older than
    LOCK_STALE_SECONDS, left by a crashed run) is stolen.
    """
    try:
        try:
            fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            age = time.time() - os.path.getmtime(LOCK_PATH)
            if age < LOCK_STALE_SECONDS:
                logger.error(f"Another run holds the lock (age {age:.0f}s). Aborting.")
                send_telegram("⛔ DCA skipped: another run already in progress (lock held).")
                return False
            logger.warning(f"Stale lock ({age:.0f}s old) — stealing it.")
            os.remove(LOCK_PATH)
            fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"{os.getpid()} {datetime.now(timezone.utc).isoformat()}".encode())
        os.close(fd)
        return True
    except Exception as e:
        logger.error(f"Lock acquire error: {e}")
        return False


def release_run_lock():
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except Exception as e:
        logger.error(f"Lock release error: {e}")


def already_traded_today(session):
    """True if a trade is already logged for the current UTC date (idempotency)."""
    if not supabase_client:
        return False
    try:
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        resp = (supabase_client.table('trade_log')
                .select('trade_id')
                .gte('timestamp', f"{today} 00:00:00")
                .limit(1)
                .execute())
        return bool(resp.data)
    except Exception as e:
        logger.error(f"already_traded_today check error: {e}")
        return False


# ─── Market Signals ────────────────────────────────────────────────────────────

def get_fear_greed_index():
    """Returns Crypto Fear & Greed Index (0=Extreme Fear, 100=Extreme Greed)."""
    try:
        resp = requests.get('https://api.alternative.me/fng/?limit=1', timeout=10)
        data = resp.json()
        value = int(data['data'][0]['value'])
        label = data['data'][0]['value_classification']
        logger.info(f"Fear & Greed: {value} ({label})")
        return value
    except Exception as e:
        logger.error(f"Fear & Greed API error: {e}")
        return 50  # neutral fallback


def get_bollinger_signal(session, coin, current_price, period=20):
    """
    Returns 1.0 (price below lower Bollinger Band — buy signal),
    -1.0 (above upper band — overbought), or 0.0 (inside bands).
    """
    try:
        klines = session.get_kline(
            category='spot',
            symbol=f'{coin}{USD_TYPE}',
            interval='D',
            limit=period + 2,
        )
        if not klines['result']['list']:
            return 0.0
        # API returns newest first; reverse for chronological order
        closes = [float(k[4]) for k in reversed(klines['result']['list'])]
        closes = closes[:-1]  # drop incomplete current candle
        if len(closes) < period:
            return 0.0
        closes = closes[-period:]
        sma = sum(closes) / period
        std = (sum((c - sma) ** 2 for c in closes) / period) ** 0.5
        lower = sma - 2 * std
        upper = sma + 2 * std
        if current_price < lower:
            logger.info(f"{coin}: below Bollinger lower band ${lower:.4f} — buy signal")
            return 1.0
        if current_price > upper:
            return -1.0
        return 0.0
    except Exception as e:
        logger.error(f"Bollinger error for {coin}: {e}")
        return 0.0


def get_market_boost(session, coin, current_price, fng_value):
    """
    Returns buy multiplier based on Fear & Greed Index + Bollinger Bands.
    Replaces the old avg-price-based DCA boost.
    """
    max_boost = float(os.getenv('DCA_BOOST_MULTIPLIER', 2.0))

    boost = 1.0
    if fng_value <= 20:
        boost += 0.75   # Extreme Fear — buy aggressively
    elif fng_value <= 40:
        boost += 0.35   # Fear
    elif fng_value >= 80:
        boost -= 0.20   # Extreme Greed — buy less
    elif fng_value >= 65:
        boost -= 0.10   # Greed

    bb = get_bollinger_signal(session, coin, current_price)
    if bb > 0:
        boost += 0.50   # below lower band
    elif bb < 0:
        boost -= 0.15   # above upper band

    result = min(max(boost, 0.3), max_boost)
    if abs(result - 1.0) > 0.05:
        direction = "boost" if result > 1.0 else "reduce"
        logger.info(f"Market signal {coin}: {direction} x{result:.2f} (F&G={fng_value}, BB={bb:+.0f})")
    return result


def is_flash_crash(session, coin, current_price):
    """Returns True if price dropped more than FLASH_CRASH_PCT% in last FLASH_CRASH_HOURS hours."""
    pct_threshold = float(os.getenv('FLASH_CRASH_PCT', 25))
    hours = int(os.getenv('FLASH_CRASH_HOURS', 1))
    try:
        klines = session.get_kline(
            category='spot',
            symbol=f'{coin}{USD_TYPE}',
            interval='60',
            limit=hours + 2,
        )
        if not klines['result']['list'] or len(klines['result']['list']) < 2:
            return False
        oldest_open = float(klines['result']['list'][-1][1])
        if oldest_open == 0:
            return False
        drop_pct = (oldest_open - current_price) / oldest_open * 100
        if drop_pct >= pct_threshold:
            msg = f"⚡ Flash crash: {coin} -{drop_pct:.1f}% in {hours}h. Skipping buy."
            logger.warning(msg)
            send_telegram(msg)
            return True
        return False
    except Exception as e:
        logger.error(f"Flash crash check error for {coin}: {e}")
        return False


# ─── Trade Execution ───────────────────────────────────────────────────────────

def convert_coins(session, fromCoin, toCoin, accountType, usd_amount):
    """
    Executes a buy via Bybit Convert API.
    Attempts to verify the actual executed amounts; falls back to quote amounts.
    Returns (fromCoin, toCoin, fromAmount, toAmount) as strings.
    """
    try:
        if usd_amount < 0.01:
            return (fromCoin, toCoin, "0.0", "0.0")

        quote_resp = session.request_a_quote(
            fromCoin=fromCoin, toCoin=toCoin, accountType=accountType,
            requestCoin=fromCoin, requestAmount=str(usd_amount),
        )
        if not quote_resp or quote_resp.get('retCode') != 0:
            raise Exception(f"Quote failed: {quote_resp}")

        quote_id = quote_resp['result']['quoteTxId']
        res = quote_resp['result']

        if DRY_RUN:
            logger.info(f"[DRY RUN] Would convert {res['fromAmount']} {fromCoin} → {res['toAmount']} {toCoin}")
            send_telegram(f"🧪 [DRY RUN] Convert {res['fromAmount']} {fromCoin} → {res['toAmount']} {toCoin}")
            return (fromCoin, toCoin, res['fromAmount'], res['toAmount'])

        confirm = session.confirm_a_quote(quoteTxId=quote_id)
        if not confirm or confirm.get('retCode') != 0:
            raise Exception(f"Confirmation failed: {confirm}")

        # Verify the ACTUAL executed amounts (not the quote estimate). Convert
        # settlement can lag, so retry with backoff before falling back.
        verified = None
        for delay in (2, 3, 5):
            time.sleep(delay)
            try:
                history = session.get_convert_history(limit=5)
                if history and history.get('result', {}).get('list'):
                    for record in history['result']['list']:
                        if record.get('quoteTxId') == quote_id:
                            verified = (record['fromAmount'], record['toAmount'])
                            break
            except Exception:
                pass
            if verified:
                break

        if verified:
            fa, ta = verified
            logger.info(f"Verified fill: {fa} {fromCoin} → {ta} {toCoin}")
            send_telegram(f"✅ Bought {ta} {toCoin} for {fa} {fromCoin}")
            return (fromCoin, toCoin, fa, ta)

        # Confirmed but unverified: surface this clearly rather than presenting
        # the estimate as a verified fill.
        logger.warning(f"Convert fill UNVERIFIED after retries — using quote estimate "
                       f"{res['fromAmount']} {fromCoin} → {res['toAmount']} {toCoin}")
        send_telegram(f"⚠️ Converted (unverified) {res['fromAmount']} {fromCoin} → {res['toAmount']} {toCoin}")
        return (fromCoin, toCoin, res['fromAmount'], res['toAmount'])

    except Exception as e:
        logger.error(f"Conversion error: {e}")
        send_telegram(f"❌ Conversion error: {e}")
        return (fromCoin, toCoin, "0.0", "0.0")


def try_spot_limit_order(session, coin, usd_amount):
    """
    Tries to buy via Post-Only spot limit order at the current bid price (maker, lowest fee).
    Returns (fromCoin, toCoin, fromAmount, toAmount) on fill, None on failure/timeout.
    """
    symbol = f'{coin}{USD_TYPE}'
    timeout = int(os.getenv('SPOT_ORDER_TIMEOUT', 180))
    if DRY_RUN:
        logger.info(f"[DRY RUN] Would place spot limit order for {coin}; using convert simulation instead.")
        return None
    try:
        ticker = session.get_tickers(category='spot', symbol=symbol)
        if not ticker['result']['list']:
            return None
        bid_price = float(ticker['result']['list'][0].get('bid1Price', 0))
        if bid_price == 0:
            return None

        inst = session.get_instruments_info(category='spot', symbol=symbol)
        if not inst['result']['list']:
            return None
        lot = inst['result']['list'][0]['lotSizeFilter']
        qty_step = float(lot['qtyStep'])
        min_qty = float(lot['minOrderQty'])
        min_amt = float(lot.get('minOrderAmt', 1.0))

        raw_qty = usd_amount / bid_price
        coin_qty = round((raw_qty // qty_step) * qty_step, 8)

        if coin_qty < min_qty or coin_qty * bid_price < min_amt:
            return None

        order_resp = session.place_order(
            category='spot', symbol=symbol, side='Buy',
            orderType='Limit', qty=str(coin_qty), price=str(bid_price),
            timeInForce='PostOnly',
            orderLinkId=f'dca-{coin}-{datetime.now(timezone.utc).strftime("%Y%m%d")}',
        )
        if not order_resp or order_resp.get('retCode') != 0:
            return None

        order_id = order_resp['result']['orderId']
        logger.info(f"Spot limit placed: {coin_qty} {coin} @ {bid_price}")

        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(10)
            s = session.get_order_realtime(category='spot', orderId=order_id, symbol=symbol)
            if not s or not s['result']['list']:
                continue
            info = s['result']['list'][0]
            status = info['orderStatus']
            if status == 'Filled':
                qty = float(info['cumExecQty'])
                val = float(info['cumExecValue'])
                logger.info(f"Spot filled: {qty} {coin} for ${val:.2f}")
                send_telegram(f"✅ Spot buy: {qty} {coin} for ${val:.2f} (maker order)")
                return (USD_TYPE, coin, str(val), str(qty))
            if status in ('Cancelled', 'Rejected', 'Expired'):
                break

        # Timed out (or terminal-but-unfilled): cancel any remainder, then return
        # whatever ACTUALLY executed so the caller buys only the unfilled residual.
        # Dropping a partial fill here would make execute_buy re-buy the full amount
        # via Convert — a silent over-spend.
        try:
            session.cancel_order(category='spot', orderId=order_id, symbol=symbol)
        except Exception:
            pass
        time.sleep(1)
        filled_qty, filled_val = 0.0, 0.0
        try:
            final = session.get_order_realtime(category='spot', orderId=order_id, symbol=symbol)
            if final and final['result']['list']:
                fi = final['result']['list'][0]
                filled_qty = float(fi.get('cumExecQty') or 0)
                filled_val = float(fi.get('cumExecValue') or 0)
        except Exception:
            pass
        if filled_qty > 0:
            logger.info(f"Spot order for {coin} partially filled: {filled_qty} for ${filled_val:.2f}. Cancelled remainder.")
            send_telegram(f"⚠️ Spot partial: {filled_qty} {coin} for ${filled_val:.2f}; buying residual via convert.")
            return (USD_TYPE, coin, str(filled_val), str(filled_qty))
        logger.info(f"Spot order for {coin} timed out after {timeout}s with no fill. Cancelled.")
        return None

    except Exception as e:
        logger.error(f"Spot limit order error for {coin}: {e}")
        return None


def execute_buy(session, coin, usd_amount):
    """
    Executes a buy. If USE_SPOT_ORDERS=true, tries a Post-Only limit order first.
    A partial spot fill is KEPT and only the unfilled residual is bought via
    Convert (no double-spend). Returns aggregated (fromCoin, toCoin, usd, qty).
    """
    spot_val, spot_qty = 0.0, 0.0
    if os.getenv('USE_SPOT_ORDERS', 'false').lower() == 'true':
        result = try_spot_limit_order(session, coin, usd_amount)
        if result:
            spot_val, spot_qty = float(result[2]), float(result[3])
        if spot_val >= usd_amount * 0.99:
            return result  # effectively fully filled on the maker order
        if spot_val > 0:
            logger.info(f"Spot partial ${spot_val:.2f}/{usd_amount:.2f} for {coin} — converting residual.")
        else:
            logger.info(f"Spot order for {coin} unfilled — falling back to convert.")

    residual = usd_amount - spot_val
    if residual < MIN_CONVERT_USD:
        if spot_qty > 0:
            return (USD_TYPE, coin, str(spot_val), str(spot_qty))
        logger.info(f"{coin}: residual ${residual:.2f} below convert minimum — skipping.")
        return (USD_TYPE, coin, "0.0", "0.0")

    conv = convert_coins(session, USD_TYPE, coin, 'eb_convert_uta', residual)
    total_val = spot_val + float(conv[2])
    total_qty = spot_qty + float(conv[3])
    return (USD_TYPE, coin, str(total_val), str(total_qty))


# ─── Staking ───────────────────────────────────────────────────────────────────

def stake_or_redeem(session, category, order_type, account_type, amount, coin):
    # Stablecoins use 2 dp; other coins need finer precision (ETH 0.0023 must not
    # round to 0.00). A 6-dp amount once triggered ErrCode 180001 ("Invalid
    # parameter") — see logs — so on that specific code we retry with coarser
    # amounts (never zeroing the value).
    base = round(amount, 2) if coin == USD_TYPE else round(amount, 6)
    candidates = []
    for c in (base, round(amount, 2), float(int(amount))):
        if c > 0 and c not in candidates:
            candidates.append(c)
    if not candidates:
        return False

    try:
        prod_info = session.get_earn_product_info(category=category, coin=coin)
        if not prod_info['result']['list']:
            logger.error(f"No earn product for {coin} ({category}).")
            return False
        product_id = prod_info['result']['list'][0]['productId']

        if DRY_RUN:
            logger.info(f"[DRY RUN] Would {order_type} {base} {coin} ({category}).")
            return True

        last = None
        for amt in candidates:
            res = session.stake_or_redeem(
                category=category, orderType=order_type, accountType=account_type,
                amount=str(amt), coin=coin, productId=product_id,
                orderLinkId=f"{order_type.lower()}-{uuid.uuid4().hex[:12]}",
            )
            last = res
            if res and res.get('retCode') == 0:
                logger.info(f"{order_type} {amt} {coin} OK.")
                send_telegram(f"✅ {order_type} {amt} {coin} OK.")
                return True
            if not res or res.get('retCode') != 180001:
                break  # only the precision error (180001) is worth retrying
            logger.warning(f"{order_type} {amt} {coin} rejected 180001 — retrying coarser amount.")
        logger.error(f"{order_type} {coin} failed: {last}")
        send_telegram(f"❌ {order_type} {coin} failed: {last.get('retMsg') if last else 'no response'}")
        return False
    except Exception as e:
        logger.error(f"Stake/Redeem error: {e}")
        send_telegram(f"❌ Stake/Redeem error: {e}")
        return False


def get_stake_config():
    """
    Parses STAKEABLE_COINS env var.
    Format: 'SOL:OnChain:0.2,ETH:FlexibleSaving:0.001'
            coin:category:liquid_buffer_qty
    Default keeps original SOL on-chain + adds ETH flexible staking.
    """
    raw = os.getenv('STAKEABLE_COINS', 'SOL:OnChain:0.2,ETH:FlexibleSaving:0.001')
    config = {}
    for entry in raw.split(','):
        parts = entry.strip().split(':')
        if len(parts) >= 2:
            config[parts[0].upper()] = {
                'category': parts[1],
                'buffer': float(parts[2]) if len(parts) > 2 else 0.0,
            }
    return config


def stake_idle_coin(session, coin):
    """
    Stakes idle coin balance via the configured earn category,
    keeping a liquid buffer so funds aren't fully locked.
    """
    cfg = get_stake_config().get(coin)
    if not cfg:
        return

    total_bal = get_coin_balance(session, coin)
    if total_bal is None or total_bal <= 0:
        return

    stakeable = round(max(0.0, total_bal - cfg['buffer']), 6)
    if stakeable < 0.001:
        return

    stake_or_redeem(session, cfg['category'], 'Stake', 'UNIFIED', stakeable, coin)


# ─── Stablecoin Management ─────────────────────────────────────────────────────

def ensure_stablecoin_balance(session, total_needed):
    """
    Ensures sufficient stablecoin in spot wallet, redeeming from Flexible Saving if needed.
    Polls for up to 30s after redemption to let funds settle.
    """
    current_bal = get_coin_balance(session, USD_TYPE) or 0.0
    if current_bal >= total_needed:
        return current_bal

    deficit = total_needed - current_bal
    logger.info(f"Deficit {deficit:.2f} {USD_TYPE}. Checking Flexible Saving...")
    send_telegram(f"⚠️ Low {USD_TYPE} balance. Redeeming {deficit:.2f} from Flexible Saving.")

    try:
        staked = session.get_staked_position(category='FlexibleSaving', coin=USD_TYPE)
        if staked['result']['list']:
            pos = staked['result']['list'][0]
            redeemable = float(pos.get('redeemableAmount') or pos.get('amount', 0))
            send_telegram(f"ℹ️ Redeemable: {redeemable:.2f} {USD_TYPE}")
            if redeemable > 0:
                to_redeem = min(redeemable, max(deficit * BUFFER_MULTIPLIER, MIN_REDEMPTION_USD))
                send_telegram(f"🔄 Redeeming {to_redeem:.2f} {USD_TYPE}")
                if stake_or_redeem(session, 'FlexibleSaving', 'Redeem', 'UNIFIED', to_redeem, USD_TYPE):
                    for _ in range(6):
                        time.sleep(5)
                        new_bal = get_coin_balance(session, USD_TYPE) or 0.0
                        if new_bal >= total_needed:
                            return new_bal
    except Exception as e:
        logger.error(f"Redemption error: {e}")

    return get_coin_balance(session, USD_TYPE) or 0.0


def sweep_stablecoin_surplus(session):
    """Sweeps excess stablecoin to Flexible Saving, keeping USDT_BUFFER_DAYS days of spend."""
    buffer_days = float(os.getenv('USDT_BUFFER_DAYS', 3))
    buffer = buffer_days * DAILY_USD

    spot_bal = get_coin_balance(session, USD_TYPE)
    if spot_bal is None or spot_bal <= buffer + 1:
        return

    surplus = spot_bal - buffer
    if surplus < MIN_REDEMPTION_USD:
        return

    logger.info(f"Sweeping {surplus:.2f} {USD_TYPE} surplus to Flexible Saving (buffer={buffer:.2f})")
    if stake_or_redeem(session, 'FlexibleSaving', 'Stake', 'UNIFIED', surplus, USD_TYPE):
        send_telegram(f"💰 Swept {surplus:.2f} {USD_TYPE} to Flexible Saving")


# ─── Value Averaging ───────────────────────────────────────────────────────────

def get_va_buy_amount(session, coin, base_daily_usd, current_price):
    """
    Value Averaging: target portfolio value grows by base_daily_usd per day.
    Buys more when below trajectory, less when ahead. Always buys at least 10%.
    """
    if not supabase_client or not current_price:
        return base_daily_usd
    try:
        symbol = f'{coin}{USD_TYPE}'
        resp = (supabase_client.table('trade_log')
                .select('timestamp')
                .eq('symbol', symbol)
                .order('timestamp', desc=False)
                .limit(1)
                .execute())
        if not resp.data:
            return base_daily_usd

        first_date = pd.to_datetime(resp.data[0]['timestamp'])
        if hasattr(first_date, 'tzinfo') and first_date.tzinfo:
            first_date = first_date.tz_convert('UTC').tz_localize(None)
        days_elapsed = max(1, (datetime.now(timezone.utc).replace(tzinfo=None) - first_date).days)

        target_value = days_elapsed * base_daily_usd
        current_qty = get_total_coin_holdings(session, coin) or 0.0  # incl. staked
        current_value = current_qty * current_price

        va_buy = target_value - current_value
        max_buy = base_daily_usd * float(os.getenv('VA_MAX_MULTIPLIER', 3.0))
        min_buy = base_daily_usd * 0.1
        result = max(min_buy, min(va_buy, max_buy))

        if abs(result - base_daily_usd) > base_daily_usd * 0.15:
            logger.info(f"VA {coin}: target=${target_value:.2f} current=${current_value:.2f} → buy=${result:.2f}")
        return result
    except Exception as e:
        logger.error(f"Value averaging error for {coin}: {e}")
        return base_daily_usd


# ─── Portfolio ─────────────────────────────────────────────────────────────────

def get_crypto_allocation():
    alloc = {}
    for pair in os.getenv('CRYPTO_ALLOCATION_STRING', '').split(','):
        if ':' not in pair:
            continue
        s, _, m = pair.strip().partition(':')
        try:
            weight = float(m)
        except ValueError:
            logger.error(f"Bad allocation entry '{pair.strip()}' — skipping.")
            continue
        if weight > 0:
            alloc[s.upper()] = weight
    return alloc


def get_max_prices():
    prices = {}
    for pair in os.getenv('MAX_PRICE_STRING', '').split(','):
        if ':' not in pair:
            continue
        coin, _, price = pair.strip().partition(':')
        try:
            prices[coin.upper()] = float(price)
        except ValueError:
            logger.error(f"Bad max-price entry '{pair.strip()}' — skipping.")
    return prices


def validate_trading_pairs(session, allocation):
    """Warn/alert about coins whose {coin}{USD_TYPE} spot pair is missing."""
    missing = []
    for coin in allocation:
        try:
            inst = session.get_instruments_info(category='spot', symbol=f"{coin}{USD_TYPE}")
            if not inst['result']['list']:
                missing.append(coin)
        except Exception:
            missing.append(coin)
    if missing:
        msg = (f"⚠️ No {USD_TYPE} spot pair for: {', '.join(missing)}. "
               f"Priced via USDT reference; buyable via Convert only (spot maker path unavailable).")
        logger.warning(msg)
        send_telegram(msg)
    return missing


def get_portfolio_weights(session, allocation):
    portfolio = {}
    for coin in allocation:
        price = get_current_price(session, coin)
        bal = get_total_coin_holdings(session, coin)  # spot + staked, else weights are wrong
        if price and bal is not None:
            portfolio[coin] = bal * price
    return portfolio


def get_buy_rebalance_multipliers(allocation, portfolio):
    """Returns per-coin buy-side multipliers when a coin is underweight."""
    threshold = float(os.getenv('REBALANCE_THRESHOLD', 5))
    boost = float(os.getenv('REBALANCE_MULTIPLIER', 1.5))
    total_value = sum(portfolio.values())
    if total_value == 0:
        return {coin: 1.0 for coin in allocation}

    total_alloc = sum(allocation.values())
    multipliers = {}
    for coin in allocation:
        target_w = allocation[coin] / total_alloc
        current_w = portfolio.get(coin, 0) / total_value
        drift = (target_w - current_w) * 100
        if drift > threshold:
            multipliers[coin] = boost
            logger.info(f"⚖️ Buy-rebalance: {coin} underweight {drift:.1f}% → x{boost}")
            send_telegram(f"⚖️ Rebalance: {coin} underweight {drift:.1f}%, applying x{boost} buy boost")
        else:
            multipliers[coin] = 1.0
    return multipliers


def do_sell_rebalance(session, allocation):
    """Sells overweight coins back to stablecoin. Enabled via REBALANCE_SELL=true."""
    threshold = float(os.getenv('REBALANCE_THRESHOLD', 5))
    portfolio = get_portfolio_weights(session, allocation)
    total_value = sum(portfolio.values())
    if total_value == 0:
        return

    total_alloc = sum(allocation.values())
    for coin, mult in allocation.items():
        target_w = mult / total_alloc
        current_w = portfolio.get(coin, 0) / total_value
        drift = (current_w - target_w) * 100
        if drift <= threshold:
            continue

        excess_value = (current_w - target_w) * total_value
        current_price = get_current_price(session, coin)
        if not current_price:
            continue

        symbol = f'{coin}{USD_TYPE}'
        try:
            inst = session.get_instruments_info(category='spot', symbol=symbol)
            if not inst['result']['list']:
                continue
            lot = inst['result']['list'][0]['lotSizeFilter']
            qty_step = float(lot['qtyStep'])
            min_qty = float(lot['minOrderQty'])

            excess_qty = excess_value / current_price
            sell_qty = round((excess_qty // qty_step) * qty_step, 8)
            # Can only sell what's liquid in spot — the rest is staked in Earn.
            liquid = get_coin_balance(session, coin) or 0.0
            sell_qty = min(sell_qty, round((liquid // qty_step) * qty_step, 8))
            if sell_qty < min_qty:
                continue

            msg = f"⚖️ Sell-rebalance: {coin} overweight {drift:.1f}%, selling {sell_qty} {coin} (~${excess_value:.2f})"
            logger.info(msg)
            send_telegram(msg)

            if DRY_RUN:
                logger.info(f"[DRY RUN] Would market-sell {sell_qty} {coin}.")
                continue

            resp = session.place_order(
                category='spot', symbol=symbol, side='Sell',
                orderType='Market', qty=str(sell_qty),
                orderLinkId=f'rebal-sell-{coin}-{int(time.time() * 1000)}',
            )
            if resp and resp.get('retCode') == 0:
                send_telegram(f"✅ Sold {sell_qty} {coin} for rebalancing")
            else:
                logger.error(f"Sell rebalance failed: {resp}")
        except Exception as e:
            logger.error(f"Sell rebalance error {coin}: {e}")


# ─── PnL ───────────────────────────────────────────────────────────────────────

def calculate_PnL(session, from_date=None, to_date=None):
    if not supabase_client:
        return
    try:
        query = supabase_client.table('trade_log').select('*')
        if from_date:
            query = query.gte('timestamp', from_date)
        if to_date:
            query = query.lte('timestamp', to_date)
        data_resp = query.execute()
        if not data_resp.data:
            return

        trades = pd.DataFrame(data_resp.data)
        trades['timestamp'] = pd.to_datetime(trades['timestamp'])
        trades['symbol'] = trades['symbol'].apply(lambda x: x.replace(USD_TYPE, ''))

        mask = trades['price'].isnull() & (trades['quantity'] > 0)
        trades.loc[mask, 'price'] = trades.loc[mask, 'total_usd'] / trades.loc[mask, 'quantity']

        fd = from_date or trades['timestamp'].min().strftime('%Y-%m-%d')
        td = to_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')

        for symbol in trades['symbol'].unique():
            st = trades[trades['symbol'] == symbol]
            total_invested = st['total_usd'].sum()
            total_qty = st['quantity'].sum()

            ticker = session.get_tickers(category='spot', symbol=f"{symbol}{USD_TYPE}")
            if not ticker['result']['list']:
                continue

            price_now = float(ticker['result']['list'][0]['lastPrice'])
            value_now = total_qty * price_now
            pnl = value_now - total_invested
            avg_buy = total_invested / total_qty if total_qty > 0 else 0
            pct = pnl / total_invested * 100 if total_invested > 0 else 0
            emoji = "🟢" if pnl >= 0 else "🔴"

            msg = (f"📊 PnL {symbol} ({fd} → {td}):\n"
                   f"   Invested: ${total_invested:.2f}\n"
                   f"   Avg Buy: ${avg_buy:.4f}\n"
                   f"   Price Now: ${price_now:.4f}\n"
                   f"   Value: ${value_now:.2f}\n"
                   f"{emoji} PnL: ${pnl:.2f} ({pct:.1f}%)")
            logger.info(f"PnL {symbol}: ${pnl:.2f} ({pct:.1f}%)")
            send_telegram(msg)
    except Exception as e:
        logger.error(f"PnL error: {e}")
        send_telegram(f"❌ PnL error: {e}")


def send_periodic_report(session, period):
    days = 7 if period == 'weekly' else 30
    from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')
    label = 'Weekly' if period == 'weekly' else 'Monthly'
    send_telegram(f"📅 {label} PnL Report ({from_date} → today)")
    calculate_PnL(session, from_date=from_date)


# ─── Main DCA ──────────────────────────────────────────────────────────────────

def run_dca_bot(session):
    logger.info(f"DCA bot starting. Stablecoin: {USD_TYPE}")
    send_telegram(f"🤖 DCA Bot starting. Stablecoin: {USD_TYPE}")

    if DRY_RUN:
        logger.warning("DRY_RUN is ON — no real orders will be placed. Set DRY_RUN=false (or pass --live) to trade.")
        send_telegram("🧪 DRY_RUN mode — simulating, no real orders.")

    # Idempotency: don't repeat today's DCA if a scheduler double-fires.
    if not DRY_RUN and already_traded_today(session):
        logger.info("Already traded today (UTC) — skipping to avoid a duplicate DCA.")
        send_telegram("✅ Already executed today's DCA — skipping duplicate run.")
        return

    allocation = get_crypto_allocation()
    if not allocation:
        logger.error("No allocation in CRYPTO_ALLOCATION_STRING.")
        send_telegram("❌ No allocation found. DCA aborted.")
        return

    validate_trading_pairs(session, allocation)

    # Optional: sell overweight coins before buying
    if os.getenv('REBALANCE_SELL', 'false').lower() == 'true':
        do_sell_rebalance(session, allocation)
        time.sleep(3)

    total_needed = sum(allocation.values()) * DAILY_USD
    logger.info(f"Planned base spend: ${total_needed:.2f} {USD_TYPE} across {len(allocation)} coin(s).")
    # Hard backstop against a DAILY_USD / allocation-weight typo draining Earn.
    if total_needed > MAX_SPEND_PER_RUN:
        msg = (f"⛔ Base spend ${total_needed:.2f} exceeds MAX_SPEND_PER_RUN ${MAX_SPEND_PER_RUN:.2f}. "
               f"Check DAILY_USD / allocation weights. DCA aborted.")
        logger.error(msg)
        send_telegram(msg)
        return

    current_bal = ensure_stablecoin_balance(session, total_needed)

    if current_bal < MIN_CONVERT_USD:
        msg = f"❌ Insufficient {USD_TYPE} ({current_bal:.2f}) — nothing to buy. DCA aborted."
        logger.error(msg)
        send_telegram(msg)
        return
    if current_bal < total_needed:
        logger.warning(f"Partial funding {current_bal:.2f}/{total_needed:.2f} {USD_TYPE} — buys scale down.")
        send_telegram(f"⚠️ Partial funding {current_bal:.2f}/{total_needed:.2f} {USD_TYPE} — scaling buys down.")

    max_prices = get_max_prices()
    use_market_boost = os.getenv('USE_MARKET_BOOST', 'true').lower() == 'true'
    use_value_avg = os.getenv('VALUE_AVERAGING', 'false').lower() == 'true'

    fng_value = get_fear_greed_index() if use_market_boost else 50
    portfolio = get_portfolio_weights(session, allocation)
    rebalance_multipliers = get_buy_rebalance_multipliers(allocation, portfolio)

    # ── Step 1: Compute desired buy amounts for every coin ──
    desired_buys = {}

    for coin, mult in allocation.items():
        base_buy_usd = mult * DAILY_USD
        current_price = get_current_price(session, coin)

        if current_price is None:
            send_telegram(f"❌ No price for {coin}. Skipping.")
            continue

        # Circuit breaker: skip on flash crash
        if is_flash_crash(session, coin, current_price):
            continue

        # Max price guard
        max_price = max_prices.get(coin)
        if max_price and current_price > max_price:
            msg = f"⏭️ {coin}: ${current_price:,.2f} > max ${max_price:,.2f}. Skipping."
            logger.info(msg)
            send_telegram(msg)
            continue

        # Value Averaging adjusts the base buy amount
        buy_usd = get_va_buy_amount(session, coin, base_buy_usd, current_price) if use_value_avg else base_buy_usd

        # Market boost (Fear & Greed + Bollinger)
        if use_market_boost:
            buy_usd *= get_market_boost(session, coin, current_price, fng_value)

        # Buy-side rebalance boost — applied after market boost
        buy_usd *= rebalance_multipliers.get(coin, 1.0)

        # Bound the product of all stacked multipliers (VA × boost × rebalance)
        # so they can't compound into a runaway buy.
        buy_usd = min(buy_usd, base_buy_usd * MAX_COMBINED_MULTIPLIER)

        desired_buys[coin] = buy_usd

    if not desired_buys:
        logger.info("No coins to buy this run.")
        send_telegram("ℹ️ No coins to buy this run (all skipped).")
        sweep_stablecoin_surplus(session)
        return

    # ── Step 2: Cap and scale ──
    total_desired = sum(desired_buys.values())
    # Absolute spend ceiling — last line of defence against multiplier runaway.
    if total_desired > MAX_SPEND_PER_RUN:
        scale = MAX_SPEND_PER_RUN / total_desired
        logger.warning(f"Spend cap: scaling all buys by {scale:.2%} to stay within ${MAX_SPEND_PER_RUN:.2f}")
        send_telegram(f"🛡️ Spend cap hit — scaling buys to ${MAX_SPEND_PER_RUN:.2f}")
        desired_buys = {coin: amt * scale for coin, amt in desired_buys.items()}
        total_desired = MAX_SPEND_PER_RUN
    # Scale to available balance (prevents dict-order coins starving later ones).
    if total_desired > current_bal:
        scale = current_bal / total_desired
        logger.info(f"Budget cap: scaling all buys by {scale:.2%}")
        desired_buys = {coin: amt * scale for coin, amt in desired_buys.items()}

    # ── Step 3: Execute buys ──
    remaining_bal = current_bal

    for coin, buy_usd in desired_buys.items():
        if remaining_bal < buy_usd * 0.95:
            logger.warning(f"Insufficient balance for {coin} (need {buy_usd:.2f}, have {remaining_bal:.2f}). Skipping.")
            continue

        logger.info(f"Buying {coin}: ${buy_usd:.2f} {USD_TYPE}")
        send_telegram(f"💰 Buying {coin}: ${buy_usd:.2f} {USD_TYPE}")

        order = execute_buy(session, coin, buy_usd)

        actual_usd = float(order[2])
        actual_qty = float(order[3])
        if actual_qty > 0 and actual_usd > 0:
            # Decrement only what was actually spent
            remaining_bal -= actual_usd

            log_trade(
                symbol=f"{coin}{USD_TYPE}",
                quantity=actual_qty,
                price=actual_usd / actual_qty,
                total_usd=actual_usd,
            )

            # Auto-stake newly accumulated balance
            stake_idle_coin(session, coin)

    # ── Step 4: Sweep surplus stablecoin to Flexible Saving ──
    sweep_stablecoin_surplus(session)

    # ── Step 5: PnL report ──
    calculate_PnL(session, from_date=os.getenv('PNL_FROM_DATE'))


def main():
    parser = argparse.ArgumentParser(description='DCA Bot')
    parser.add_argument('--report', choices=['weekly', 'monthly'],
                        help='Send PnL report without running DCA')
    parser.add_argument('--live', action='store_true',
                        help='Force live trading for this run (overrides DRY_RUN=true).')
    args = parser.parse_args()

    global DRY_RUN
    if args.live:
        DRY_RUN = False

    if not API_KEY or not API_SECRET:
        logger.error("API_KEY or API_SECRET missing.")
        return

    session = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=TESTNET)
    if TESTNET:
        logger.warning("TESTNET mode — connected to Bybit testnet.")

    if args.report:
        send_periodic_report(session, args.report)
        return

    if DAILY_USD <= 0:
        logger.error("DAILY_USD not set or zero. Aborting.")
        return

    # Run-lock guards against duplicate/concurrent invocations double-spending.
    if not acquire_run_lock():
        return
    try:
        run_dca_bot(session)
    finally:
        release_run_lock()


if __name__ == '__main__':
    main()
