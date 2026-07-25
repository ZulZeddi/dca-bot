import os
import sys
import json
import uuid
from pathlib import Path
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import argparse
import requests
import time
from loguru import logger
from supabase import create_client
import pandas as pd

import dca_core as core

# Absolute paths throughout: a scheduled run's working directory is not the
# script directory, so a relative .env or log path resolves somewhere else
# (under Task Scheduler, C:\Windows\System32) and fails silently.
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / 'log'
LOG_DIR.mkdir(exist_ok=True)

logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True,
           format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")
logger.add(LOG_DIR / "dca_bot_{time:YYYY-MM-DD}.log", level="DEBUG",
           rotation="1 day", retention="90 days", enqueue=True)

# === Settings ===
load_dotenv(BASE_DIR / '.env')
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
# Cap on the product of all per-coin buy multipliers (market boost × rebalance).
MAX_COMBINED_MULTIPLIER = float(os.getenv('MAX_COMBINED_MULTIPLIER', 3.0))
# Minimum residual (USD) worth routing through Convert after a partial spot fill.
MIN_CONVERT_USD = float(os.getenv('MIN_CONVERT_USD', 1.0))
# Run-lock to stop a duplicate/concurrent invocation from double-spending.
LOCK_PATH = str(BASE_DIR / 'dca_bot.lock')
# Dry-run trades go to a local file, never to the production ledger.
DRYRUN_TRADES_PATH = LOG_DIR / 'dryrun_trades.jsonl'
# Local idempotency record — checked before the remote ledger.
LAST_RUN_PATH = LOG_DIR / 'last_run.json'
# One JSON line per run: post-mortem debugging, heartbeat source, and the
# signal snapshot a future backtest needs (signals are otherwise thrown away).
RUNS_PATH = LOG_DIR / 'runs.jsonl'


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
    """
    Returns staked/earn balance for coin in the given Earn category, or None on
    API error. None must NOT be reported as 0.0: stake_idle_coin() parks almost
    the whole position in Earn, so a failed read that reads as zero makes an
    owned coin look unbought and the bot over-buys it.
    """
    try:
        pos = session.get_staked_position(category=category, coin=coin)
        total = 0.0
        for p in (pos.get('result', {}).get('list') or []):
            amt = p.get('redeemableAmount') or p.get('amount') or 0
            total += float(amt or 0)
        return total
    except Exception as e:
        logger.error(f"Error getting staked {coin} ({category}) balance: {e}")
        return None


def get_total_coin_holdings(session, coin):
    """
    Total holdings = liquid spot wallet balance + staked balance in the coin's
    configured Earn category. Returns None if EITHER read fails, so callers can
    tell an API failure apart from a genuine zero.

    Balance-driven SIZING (portfolio weights, rebalancing) must use this, NOT
    get_coin_balance(): stake_idle_coin() parks bought coins in Earn, so a
    spot-only read makes them look unbought and the bot over-buys them.
    """
    spot = get_coin_balance(session, coin)
    cfg = get_stake_config().get(coin.upper())
    staked = get_staked_balance(session, coin, cfg['category']) if cfg else 0.0
    total = core.total_holdings(spot, staked)
    return None if total is None else round(total, 8)


_SYMBOL_CACHE = {}


def resolve_symbol(session, coin):
    """
    Returns the market-data symbol for `coin`: the configured stablecoin pair if
    it exists, else the USDT pair as a ~$1-parity reference. Returns None if
    neither exists.

    Every price/kline consumer must resolve through here. Hardcoding
    f"{coin}{USD_TYPE}" is how the flash-crash breaker and Bollinger signal
    silently died for TON, which has no TONUSDC pair — the kline call errored
    and both fell back to "no signal" while looking healthy.

    Note this is for MARKET DATA only. A spot buy order must still be placed on
    the {coin}{USD_TYPE} pair, because that is the coin being spent.
    """
    if coin in _SYMBOL_CACHE:
        return _SYMBOL_CACHE[coin]

    resolved = None
    for quote in dict.fromkeys((USD_TYPE, 'USDT')):
        symbol = f"{coin}{quote}"
        try:
            ticker = session.get_tickers(category='spot', symbol=symbol)
            if ticker['result']['list']:
                resolved = symbol
                if quote != USD_TYPE:
                    logger.warning(f"{coin}{USD_TYPE} unavailable — using {symbol} for market data.")
                break
        except Exception as e:
            logger.error(f"Symbol lookup failed for {symbol}: {e}")

    _SYMBOL_CACHE[coin] = resolved
    return resolved


def get_current_price(session, coin):
    symbol = resolve_symbol(session, coin)
    if not symbol:
        logger.error(f"No tradable market-data pair for {coin}.")
        return None
    try:
        ticker = session.get_tickers(category='spot', symbol=symbol)
        if ticker['result']['list']:
            return float(ticker['result']['list'][0]['lastPrice'])
    except Exception as e:
        logger.error(f"Error getting price for {symbol}: {e}")
    return None


def log_trade(symbol, quantity, price, total_usd):
    now = datetime.now(timezone.utc)
    data = {
        'trade_id': f"{symbol}-{now.strftime('%Y%m%d%H%M%S%f')}",
        'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
        'symbol': symbol,
        'quantity': float(quantity),
        'price': float(price),
        'total_usd': float(total_usd),
    }

    # A simulated fill must NEVER reach the production ledger. Convert returns
    # real quote amounts in DRY_RUN, so without this guard a dry run writes a
    # fabricated trade that then corrupts PnL and makes the idempotency check
    # skip the real live run.
    if DRY_RUN:
        try:
            with open(DRYRUN_TRADES_PATH, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data) + '\n')
        except Exception as e:
            logger.error(f"Dry-run trade log error: {e}")
        logger.info(f"[DRY RUN] Trade not persisted to ledger: {symbol} qty={quantity} ${total_usd}")
        return

    if not supabase_client:
        logger.error("Supabase not initialized. Trade NOT logged — ledger is now incomplete.")
        send_telegram(f"❌ Trade executed but NOT logged (no Supabase): {symbol} ${total_usd}")
        return
    try:
        supabase_client.table('trade_log').insert(data).execute()
    except Exception as e:
        logger.error(f"Supabase logging error: {e}")
        send_telegram(f"❌ Supabase logging error: {e}")


# ─── Idempotency / run safety ──────────────────────────────────────────────────

_LOCK_HANDLE = None


def acquire_run_lock():
    """
    Take an OS-level advisory lock held for the lifetime of the process.
    The OS drops it on any exit — clean, crashed, or killed — so there is no
    stale lock to age out and no steal race to get wrong.
    """
    global _LOCK_HANDLE
    try:
        handle = open(LOCK_PATH, 'a+')
    except Exception as e:
        logger.error(f"Lock file error: {e}")
        send_telegram(f"❌ DCA aborted: cannot open run lock: {e}")
        return False

    try:
        if os.name == 'nt':
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        logger.error("Another run holds the lock. Aborting.")
        send_telegram("⛔ DCA skipped: another run already in progress.")
        return False
    except Exception as e:
        handle.close()
        logger.error(f"Lock acquire error: {e}")
        send_telegram(f"❌ DCA aborted: cannot acquire run lock: {e}")
        return False

    _LOCK_HANDLE = handle
    logger.debug(f"Run lock acquired (pid {os.getpid()}).")
    return True


def release_run_lock():
    global _LOCK_HANDLE
    if _LOCK_HANDLE is None:
        return
    try:
        if os.name == 'nt':
            import msvcrt
            _LOCK_HANDLE.seek(0)
            msvcrt.locking(_LOCK_HANDLE.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(_LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
    except Exception as e:
        logger.error(f"Lock release error: {e}")
    finally:
        try:
            _LOCK_HANDLE.close()
        except Exception:
            pass
        _LOCK_HANDLE = None


def write_run_summary(summary):
    """Appends one JSON line describing this run to log/runs.jsonl."""
    try:
        with open(RUNS_PATH, 'a', encoding='utf-8') as f:
            f.write(json.dumps(summary, default=str) + '\n')
    except Exception as e:
        logger.error(f"Run summary write error: {e}")


def _read_last_run():
    try:
        if LAST_RUN_PATH.exists():
            return json.loads(LAST_RUN_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        logger.error(f"last_run read error: {e}")
    return {}


def record_local_buy(coin):
    """Record locally, immediately, that `coin` was bought today (UTC)."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    data = _read_last_run()
    if data.get('date') != today:
        data = {'date': today, 'coins': []}
    if coin not in data.get('coins', []):
        data.setdefault('coins', []).append(coin)
    try:
        LAST_RUN_PATH.write_text(json.dumps(data), encoding='utf-8')
    except Exception as e:
        logger.error(f"last_run write error: {e}")


def coins_bought_today(allocation):
    """
    Returns the set of coins already bought today (UTC), or None if that cannot
    be established.

    Checks the local record first — the real duplicate-run threat is a repeated
    local invocation, and a local file catches it with no network round-trip —
    then the remote ledger. Returning None on a ledger error lets the caller
    FAIL CLOSED: an idempotency gate that errors toward "trade again" is worse
    than no gate at all.
    """
    now = datetime.now(timezone.utc)
    today = now.strftime('%Y-%m-%d')
    tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')

    bought = set()
    local = _read_last_run()
    if local.get('date') == today:
        bought.update(local.get('coins', []))

    if not supabase_client:
        return bought

    try:
        resp = (supabase_client.table('trade_log')
                .select('symbol')
                .gte('timestamp', f"{today} 00:00:00")
                .lt('timestamp', f"{tomorrow} 00:00:00")
                .execute())
        symbols = {(row.get('symbol') or '') for row in (resp.data or [])}
        bought.update(c for c in allocation if f"{c}{USD_TYPE}" in symbols)
        return bought
    except Exception as e:
        logger.error(f"Idempotency check failed: {e}")
        return None


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
    symbol = resolve_symbol(session, coin)
    if not symbol:
        return 0.0
    try:
        klines = session.get_kline(
            category='spot',
            symbol=symbol,
            interval='D',
            limit=period + 2,
        )
        if not klines['result']['list']:
            return 0.0
        # API returns newest first; reverse for chronological order, then drop
        # the in-progress candle so the bands use completed days only.
        closes = [float(k[4]) for k in reversed(klines['result']['list'])][:-1]
        signal = core.bollinger_from_closes(closes, current_price, period)
        if signal > 0:
            logger.info(f"{coin}: below Bollinger lower band — buy signal")
        return signal
    except Exception as e:
        logger.error(f"Bollinger error for {coin}: {e}")
        return 0.0


def get_market_boost(session, coin, current_price, fng_value):
    """Buy multiplier from the Fear & Greed Index + Bollinger Bands."""
    max_boost = float(os.getenv('DCA_BOOST_MULTIPLIER', 2.0))
    bb = get_bollinger_signal(session, coin, current_price)
    result = core.market_boost_from_signals(fng_value, bb, max_boost=max_boost)
    if abs(result - 1.0) > 0.05:
        direction = "boost" if result > 1.0 else "reduce"
        logger.info(f"Market signal {coin}: {direction} x{result:.2f} (F&G={fng_value}, BB={bb:+.0f})")
    return result


def is_flash_crash(session, coin, current_price):
    """
    Returns True if price is more than FLASH_CRASH_PCT% below its highest point
    in the last FLASH_CRASH_HOURS hours.

    Measured peak-to-current, not oldest-open-to-current: a V-shaped crash that
    has partly recovered is invisible to an open-vs-last comparison, which is
    exactly the move worth skipping. `limit=hours+1` covers `hours` completed
    candles plus the one in progress.
    """
    pct_threshold = float(os.getenv('FLASH_CRASH_PCT', 25))
    hours = max(1, int(os.getenv('FLASH_CRASH_HOURS', 1)))
    symbol = resolve_symbol(session, coin)
    if not symbol:
        return False
    try:
        klines = session.get_kline(
            category='spot',
            symbol=symbol,
            interval='60',
            limit=hours + 1,
        )
        rows = klines['result']['list']
        drop_pct = core.drawdown_from_high(rows, current_price)
        if drop_pct >= pct_threshold:
            msg = f"⚡ Flash crash: {coin} -{drop_pct:.1f}% from {hours}h high. Skipping buy."
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
    # Always return a 4-tuple of strings — the caller parses order[2]/order[3]
    # unconditionally, so any None here is a TypeError mid-run.
    if usd_amount <= 0:
        return (USD_TYPE, coin, "0.0", "0.0")

    spot_val, spot_qty = 0.0, 0.0
    if os.getenv('USE_SPOT_ORDERS', 'false').lower() == 'true':
        result = try_spot_limit_order(session, coin, usd_amount)
        if result:
            spot_val, spot_qty = float(result[2]), float(result[3])

    plan = core.plan_partial_fill(usd_amount, spot_val, spot_qty, MIN_CONVERT_USD)

    if plan.convert_usd <= 0:
        if plan.spot_qty > 0:
            return (USD_TYPE, coin, str(plan.spot_usd), str(plan.spot_qty))
        logger.info(f"{coin}: nothing left to buy above the convert minimum — skipping.")
        return (USD_TYPE, coin, "0.0", "0.0")

    if plan.spot_usd > 0:
        logger.info(f"Spot partial ${plan.spot_usd:.2f}/{usd_amount:.2f} for {coin} — converting ${plan.convert_usd:.2f} residual.")

    conv = convert_coins(session, USD_TYPE, coin, 'eb_convert_uta', plan.convert_usd)
    total_val, total_qty = core.aggregate_fill(
        plan.spot_usd, plan.spot_qty, float(conv[2]), float(conv[3]))
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
    """
    return core.parse_stake_config(
        os.getenv('STAKEABLE_COINS', 'SOL:OnChain:0.2,ETH:FlexibleSaving:0.001'))


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
    Ensures sufficient stablecoin in the spot wallet, redeeming from Flexible
    Saving if needed. Polls for up to 30s after redemption to let funds settle.

    Returns the available balance, or None if it cannot be read. A read failure
    must never be mistaken for "no money" — that would redeem funds out of yield
    on the strength of a network blip.
    """
    current_bal = get_coin_balance(session, USD_TYPE)
    if current_bal is None:
        logger.error(f"Cannot read {USD_TYPE} balance — not redeeming.")
        return None
    if current_bal >= total_needed:
        return current_bal

    deficit = total_needed - current_bal
    logger.info(f"Deficit {deficit:.2f} {USD_TYPE}. Checking Flexible Saving...")

    if DRY_RUN:
        logger.info(f"[DRY RUN] Would redeem ~{deficit:.2f} {USD_TYPE} from Flexible Saving.")
        return current_bal

    send_telegram(f"⚠️ Low {USD_TYPE} balance. Redeeming {deficit:.2f} from Flexible Saving.")
    try:
        staked = session.get_staked_position(category='FlexibleSaving', coin=USD_TYPE)
        if staked['result']['list']:
            pos = staked['result']['list'][0]
            redeemable = float(pos.get('redeemableAmount') or pos.get('amount', 0))
            logger.info(f"Redeemable: {redeemable:.2f} {USD_TYPE}")
            if redeemable > 0:
                to_redeem = min(redeemable, max(deficit * BUFFER_MULTIPLIER, MIN_REDEMPTION_USD))
                if stake_or_redeem(session, 'FlexibleSaving', 'Redeem', 'UNIFIED', to_redeem, USD_TYPE):
                    for _ in range(6):
                        time.sleep(5)
                        new_bal = get_coin_balance(session, USD_TYPE)
                        if new_bal is not None and new_bal >= total_needed:
                            return new_bal
            else:
                logger.warning(f"Nothing redeemable in {USD_TYPE} Flexible Saving.")
    except Exception as e:
        logger.error(f"Redemption error: {e}")
        send_telegram(f"❌ Redemption error: {e}")

    final = get_coin_balance(session, USD_TYPE)
    return current_bal if final is None else final


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
    if stake_or_redeem(session, 'FlexibleSaving', 'Stake', 'UNIFIED', surplus, USD_TYPE) and not DRY_RUN:
        send_telegram(f"💰 Swept {surplus:.2f} {USD_TYPE} to Flexible Saving")


# ─── Portfolio ─────────────────────────────────────────────────────────────────

def get_crypto_allocation():
    return core.parse_allocation(os.getenv('CRYPTO_ALLOCATION_STRING', ''))


def get_max_prices():
    return core.parse_max_prices(os.getenv('MAX_PRICE_STRING', ''))


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
    """
    Per-coin portfolio value (spot + staked). A coin whose price or balance
    could not be read maps to None — NOT omitted and not zero, so the caller can
    tell "I own none of this" apart from "I could not find out".
    """
    portfolio = {}
    for coin in allocation:
        price = get_current_price(session, coin)
        bal = get_total_coin_holdings(session, coin)  # spot + staked, else weights are wrong
        portfolio[coin] = bal * price if (price and bal is not None) else None
    return portfolio


def get_buy_rebalance_multipliers(allocation, portfolio):
    """Returns per-coin buy-side multipliers when a coin is underweight."""
    threshold = float(os.getenv('REBALANCE_THRESHOLD', 5))
    boost = float(os.getenv('REBALANCE_MULTIPLIER', 1.5))

    unreadable = [c for c, v in portfolio.items() if v is None]
    if unreadable:
        logger.warning(f"Portfolio incomplete ({', '.join(unreadable)}) — skipping buy-rebalance this run.")

    multipliers = core.rebalance_multipliers(allocation, portfolio, threshold, boost)
    for coin, m in multipliers.items():
        if m != 1.0:
            logger.info(f"⚖️ Buy-rebalance: {coin} underweight → x{m}")
            send_telegram(f"⚖️ Rebalance: {coin} underweight, applying x{m} buy boost")
    return multipliers


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
        # Group by BASE asset: history holds both ETHUSDT and ETHUSDC rows from
        # past stablecoin switches, and a substring replace would drop one half
        # of the invested capital while still pricing the full quantity.
        trades['symbol'] = trades['symbol'].apply(lambda s: core.normalize_symbol(s)[0])

        mask = trades['price'].isnull() & (trades['quantity'] > 0)
        trades.loc[mask, 'price'] = trades.loc[mask, 'total_usd'] / trades.loc[mask, 'quantity']

        fd = from_date or trades['timestamp'].min().strftime('%Y-%m-%d')
        td = to_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')

        for symbol in trades['symbol'].unique():
            st = trades[trades['symbol'] == symbol]
            total_invested = st['total_usd'].sum()
            total_qty = st['quantity'].sum()

            market_symbol = resolve_symbol(session, symbol)
            if not market_symbol:
                logger.warning(f"PnL: no market pair for {symbol} — omitted from report.")
                continue
            ticker = session.get_tickers(category='spot', symbol=market_symbol)
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
    """Runs one DCA cycle and always records a run summary, however it ends."""
    run = {
        'run_id': uuid.uuid4().hex[:12],
        'started_at': datetime.now(timezone.utc).isoformat(),
        'dry_run': DRY_RUN,
        'testnet': TESTNET,
        'stablecoin': USD_TYPE,
        'daily_usd': DAILY_USD,
        'coins': {},
        'errors': [],
        'total_spent': 0.0,
        'outcome': 'unknown',
    }
    try:
        _run_dca(session, run)
    except BaseException as e:
        run['outcome'] = 'crashed'
        run['errors'].append(f"{type(e).__name__}: {e}")
        raise
    finally:
        run['ended_at'] = datetime.now(timezone.utc).isoformat()
        write_run_summary(run)


def _run_dca(session, run):
    logger.info(f"DCA bot starting. Stablecoin: {USD_TYPE}")
    send_telegram(f"🤖 DCA Bot starting. Stablecoin: {USD_TYPE}")

    if DRY_RUN:
        logger.warning("DRY_RUN is ON — no real orders will be placed. Set DRY_RUN=false (or pass --live) to trade.")
        send_telegram("🧪 DRY_RUN mode — simulating, no real orders.")

    allocation = get_crypto_allocation()
    if not allocation:
        logger.error("No allocation in CRYPTO_ALLOCATION_STRING.")
        send_telegram("❌ No allocation found. DCA aborted.")
        run['outcome'] = 'no_allocation'
        return

    # Idempotency, per coin: a run that bought ETH and then died must still be
    # able to buy SOL and TON. Fail closed — if we cannot establish what was
    # already bought, skip the run rather than risk a duplicate spend.
    if not DRY_RUN:
        bought = coins_bought_today(allocation)
        if bought is None:
            logger.error("Cannot verify today's trades (ledger unreachable). Skipping run.")
            send_telegram("⛔ DCA skipped: cannot verify today's trades (ledger unreachable).")
            run['outcome'] = 'ledger_unreachable'
            return
        if bought:
            allocation = {c: w for c, w in allocation.items() if c not in bought}
            logger.info(f"Already bought today: {', '.join(sorted(bought))}.")
            if not allocation:
                send_telegram("✅ Today's DCA already executed for all coins — skipping.")
                run['outcome'] = 'already_done'
                return
            send_telegram(f"ℹ️ Already bought today: {', '.join(sorted(bought))}. Buying the remaining coins.")

    validate_trading_pairs(session, allocation)

    base_spend = sum(allocation.values()) * DAILY_USD
    logger.info(f"Planned base spend: ${base_spend:.2f} {USD_TYPE} across {len(allocation)} coin(s).")
    # Hard backstop against a DAILY_USD / allocation-weight typo draining Earn.
    if base_spend > MAX_SPEND_PER_RUN:
        msg = (f"⛔ Base spend ${base_spend:.2f} exceeds MAX_SPEND_PER_RUN ${MAX_SPEND_PER_RUN:.2f}. "
               f"Check DAILY_USD / allocation weights. DCA aborted.")
        logger.error(msg)
        send_telegram(msg)
        run['outcome'] = 'config_over_cap'
        return

    max_prices = get_max_prices()
    use_market_boost = os.getenv('USE_MARKET_BOOST', 'true').lower() == 'true'

    fng_value = get_fear_greed_index() if use_market_boost else 50
    run['fng'] = fng_value
    portfolio = get_portfolio_weights(session, allocation)
    rebalance_multipliers = get_buy_rebalance_multipliers(allocation, portfolio)

    # ── Step 1: Compute desired buy amounts for every coin ──
    desired_buys = {}

    for coin, mult in allocation.items():
        base_buy_usd = mult * DAILY_USD
        info = run['coins'].setdefault(coin, {})
        info['base_usd'] = base_buy_usd
        current_price = get_current_price(session, coin)
        info['price'] = current_price

        if current_price is None:
            send_telegram(f"❌ No price for {coin}. Skipping.")
            info['skipped'] = 'no_price'
            continue

        # Circuit breaker: skip on flash crash
        if is_flash_crash(session, coin, current_price):
            info['skipped'] = 'flash_crash'
            continue

        # Max price guard
        max_price = max_prices.get(coin)
        if max_price and current_price > max_price:
            msg = f"⏭️ {coin}: ${current_price:,.2f} > max ${max_price:,.2f}. Skipping."
            logger.info(msg)
            send_telegram(msg)
            info['skipped'] = 'above_max_price'
            continue

        boost = get_market_boost(session, coin, current_price, fng_value) if use_market_boost else 1.0
        rebal = rebalance_multipliers.get(coin, 1.0)
        info['boost'] = boost
        info['rebalance'] = rebal

        # The product of correlated multipliers is clamped, not just each one.
        desired_buys[coin] = core.combine_multipliers(
            base_buy_usd, boost, rebal, max_combined=MAX_COMBINED_MULTIPLIER)
        info['planned_usd'] = desired_buys[coin]

    if not desired_buys:
        logger.info("No coins to buy this run.")
        send_telegram("ℹ️ No coins to buy this run (all skipped).")
        sweep_stablecoin_surplus(session)
        run['outcome'] = 'all_skipped'
        return

    # ── Step 2: Cap, fund, then scale ──
    # Absolute spend ceiling first — last line of defence against a multiplier
    # runaway or a config typo.
    desired_buys, capped = core.apply_spend_caps(desired_buys, MAX_SPEND_PER_RUN, None)
    if capped:
        logger.warning(f"Spend cap hit — scaling buys to ${MAX_SPEND_PER_RUN:.2f}")
        send_telegram(f"🛡️ Spend cap hit — scaling buys to ${MAX_SPEND_PER_RUN:.2f}")
    total_desired = sum(desired_buys.values())

    # Fund AFTER the multipliers are known. Redeeming only the pre-boost base
    # would leave the boosts unfundable, so they would never actually apply.
    current_bal = ensure_stablecoin_balance(session, total_desired)
    if current_bal is None:
        logger.error("Stablecoin balance unavailable. DCA aborted.")
        send_telegram("❌ DCA aborted: stablecoin balance unavailable.")
        run['outcome'] = 'balance_unavailable'
        return
    if current_bal < MIN_CONVERT_USD:
        msg = f"❌ Insufficient {USD_TYPE} ({current_bal:.2f}) — nothing to buy. DCA aborted."
        logger.error(msg)
        send_telegram(msg)
        run['outcome'] = 'insufficient_funds'
        run['available'] = current_bal
        return

    # Scale to available balance (prevents dict-order coins starving later ones).
    desired_buys, limited = core.apply_spend_caps(desired_buys, None, current_bal)
    if limited:
        logger.warning(f"Partial funding {current_bal:.2f}/{total_desired:.2f} {USD_TYPE} — scaling buys down.")
        send_telegram(f"⚠️ Partial funding {current_bal:.2f}/{total_desired:.2f} {USD_TYPE} — scaling buys down.")

    # ── Step 3: Execute buys ──
    remaining_bal = current_bal
    spent_total = 0.0
    bought_coins = []

    for coin, buy_usd in desired_buys.items():
        info = run['coins'].setdefault(coin, {})
        info['final_usd'] = buy_usd

        if remaining_bal < buy_usd * 0.95:
            logger.warning(f"Insufficient balance for {coin} (need {buy_usd:.2f}, have {remaining_bal:.2f}). Skipping.")
            info['skipped'] = 'insufficient_remaining'
            continue

        logger.info(f"Buying {coin}: ${buy_usd:.2f} {USD_TYPE}")
        send_telegram(f"💰 Buying {coin}: ${buy_usd:.2f} {USD_TYPE}")

        # One coin's failure must not abandon the remaining coins mid-run.
        try:
            order = execute_buy(session, coin, buy_usd)

            actual_usd = float(order[2])
            actual_qty = float(order[3])
            fill_price = core.implied_price(actual_usd, actual_qty)
            if fill_price is not None:
                # Decrement only what was actually spent
                remaining_bal -= actual_usd
                spent_total += actual_usd
                bought_coins.append(coin)
                info.update(actual_usd=actual_usd, actual_qty=actual_qty, fill_price=fill_price)

                # Record locally FIRST: if logging or staking then fails, a
                # re-run must still know this coin was already bought today.
                record_local_buy(coin)

                log_trade(
                    symbol=f"{coin}{USD_TYPE}",
                    quantity=actual_qty,
                    price=fill_price,
                    total_usd=actual_usd,
                )

                # Auto-stake newly accumulated balance
                stake_idle_coin(session, coin)
            else:
                info['skipped'] = 'no_fill'
        except Exception as e:
            logger.exception(f"Buy failed for {coin}: {e}")
            send_telegram(f"❌ Buy failed for {coin}: {e}")
            info['error'] = f"{type(e).__name__}: {e}"
            run['errors'].append(f"{coin}: {type(e).__name__}: {e}")

    # ── Step 4: Sweep surplus stablecoin to Flexible Saving ──
    sweep_stablecoin_surplus(session)

    run['total_spent'] = spent_total
    run['bought'] = bought_coins
    run['outcome'] = 'ok' if bought_coins else 'nothing_bought'

    summary = (f"✅ Run complete{' [DRY RUN]' if DRY_RUN else ''}: "
               f"spent ${spent_total:.2f} {USD_TYPE} on {len(bought_coins)} coin(s)"
               f"{' — ' + ', '.join(bought_coins) if bought_coins else ''}.")
    logger.info(summary)
    send_telegram(summary)

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
        return 1

    session = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=TESTNET)
    if TESTNET:
        logger.warning("TESTNET mode — connected to Bybit testnet.")

    if args.report:
        send_periodic_report(session, args.report)
        return 0

    if DAILY_USD <= 0:
        logger.error("DAILY_USD not set or zero. Aborting.")
        return 1

    # Run-lock guards against duplicate/concurrent invocations double-spending.
    if not acquire_run_lock():
        return 1
    try:
        run_dca_bot(session)
        return 0
    finally:
        release_run_lock()


if __name__ == '__main__':
    # A money bot must never die quietly: without this, an unhandled exception
    # prints a traceback nobody reads and sends no alert at all.
    try:
        sys.exit(main() or 0)
    except SystemExit:
        raise
    except BaseException as exc:
        logger.exception("Unhandled error — run aborted.")
        try:
            send_telegram(f"🚨 DCA bot crashed: {type(exc).__name__}: {exc}")
        except Exception:
            pass
        sys.exit(1)
