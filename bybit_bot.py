import os
import sys
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from datetime import datetime, timedelta
import argparse
import requests
import time
from loguru import logger 
from supabase import create_client
import pandas as pd

logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

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

stablecoin_env = os.getenv('STABLECOIN', 'USDT')
USD_TYPE = [c.strip() for c in stablecoin_env.split(',')][0] 

# --- Constants ---
BUFFER_MULTIPLIER = 1.015 # buffer for fees/slippage
MIN_REDEMPTION_USD = 10.0 # Bybit's typical minimum for Flexible Saving

# --- Utility Functions ---

def send_telegram(message):
    """Sends a notification message to the configured Telegram chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    try:
        requests.post(url, data=payload)
    except Exception as e:
        logger.error(f"Telegram error: {e}")

def get_coin_balance(session, coin, account_type='UNIFIED'):
    """Retrieves the available wallet balance for a specific coin."""
    try:
        balances = session.get_wallet_balance(accountType=account_type, coin=coin)
        if balances['result']['list'] and balances['result']['list'][0]['coin']:
            available_balance = balances['result']['list'][0]['coin'][0]['walletBalance']
            return round(float(available_balance), 6)
        return 0.0 
    except Exception as e:
        logger.error(f"Error getting {coin} balance: {e}")
        send_telegram(f"❌ Error getting {coin} balance: {e}")
        return 0.0

def convert_coins(fromCoin, toCoin, accountType, usd_amount, session):
    """Executes a market buy equivalent using the Bybit Convert API."""
    try:
        if usd_amount < 0.01: 
             return (fromCoin, toCoin, "0.0", "0.0")

        request_a_quote = session.request_a_quote(
            fromCoin=fromCoin, toCoin=toCoin, accountType=accountType,
            requestCoin=fromCoin, requestAmount=str(usd_amount)
        )
        
        quote_id = request_a_quote['result']['quoteTxId']
        confirm = session.confirm_a_quote(quoteTxId=quote_id)
        if not confirm or confirm.get('retCode') != 0:
            raise Exception(f"Quote confirmation failed: {confirm}")

        res = request_a_quote['result']
        logger.info(f"Converted {res['fromAmount']} {res['fromCoin']} to {res['toAmount']} {res['toCoin']}")
        send_telegram(f"✅ Converted {res['fromAmount']} {res['fromCoin']} to {res['toAmount']} {res['toCoin']}")
        return (res['fromCoin'], res['toCoin'], res['fromAmount'], res['toAmount'])
    except Exception as e:
        logger.error(f"Conversion error: {e}")
        send_telegram(f"❌ Conversion error: {e}")
        return (fromCoin, toCoin, "0.0", "0.0") 

def log_trade(symbol, quantity, price, total_usd):
    """Logs the trade details into Supabase."""
    if not supabase_client:
        logger.error("Supabase client not initialized. Cannot log trade.")
        send_telegram("❌ Supabase client not initialized. Trade logging failed.")
        return
    try:
        data = {
            'trade_id': f"{symbol}-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'quantity': float(quantity),
            'price': float(price),
            'total_usd': float(total_usd)
        }
        supabase_client.table('trade_log').insert(data).execute()
    except Exception as e:
        logger.error(f"Supabase logging error: {e}")
        send_telegram(f"❌ Supabase logging error: {e}")

def calculate_PnL(session, from_date=None, to_date=None):
    """Calculates PnL comparing Supabase spend vs current market value with date filtering."""
    if not supabase_client:
        logger.error("Supabase client not initialized. Cannot calculate PnL.")
        send_telegram("❌ Supabase client not initialized. PnL calculation failed.")
        return
    try:
        query = supabase_client.table('trade_log').select('*')
        if from_date:
            query = query.gte('timestamp', from_date)
        if to_date:
            query = query.lte('timestamp', to_date)
        data_response = query.execute()

        if not data_response.data:
            return

        trades = pd.DataFrame(data_response.data)
        trades['timestamp'] = pd.to_datetime(trades['timestamp'])
        trades['symbol'] = trades['symbol'].apply(lambda x: x.replace(USD_TYPE, ''))

        # Data Cleaning: Handle missing prices
        if trades['price'].isnull().any():
            trades['price'] = trades['price'].fillna(trades['total_usd'] / trades['quantity'])

        if not from_date:
            from_date = trades['timestamp'].min().strftime('%Y-%m-%d')
        if not to_date:
            to_date = datetime.now().strftime('%Y-%m-%d')

        if trades.empty:
            logger.info(f"No trades found for period {from_date} to {to_date}")
            send_telegram(f"ℹ️ No trades found from {from_date} to {to_date} for PnL calculation.")
            return

        for symbol in trades['symbol'].unique():
            # Get base coin (e.g., BTC from BTCUSDT)
            symbol_trades = trades[trades['symbol'] == symbol]
            
            total_invested = symbol_trades['total_usd'].sum()
            total_quantity = symbol_trades['quantity'].sum()
            
            # Fetch current market price
            ticker = session.get_tickers(category='spot', symbol=f"{symbol}{USD_TYPE}")
            if not ticker['result']['list']:
                continue
            
            current_symbol_price = float(ticker['result']['list'][0]['lastPrice'])
            current_value = total_quantity * current_symbol_price
            pnl = current_value - total_invested
            avg_buy_price = total_invested / total_quantity if total_quantity > 0 else 0
            status_emoji = "🟢" if pnl >= 0 else "🔴"

            msg = (f"📊 PnL for {symbol} from {from_date} to {to_date}:\n"
                   f"   Invested: ${total_invested:.2f}\n"
                   f"   Avg Buy Price: ${avg_buy_price:.4f}\n"
                   f"   Current Price: ${current_symbol_price:.4f}\n"
                   f"   Current Value: ${current_value:.2f}\n"
                   f"{status_emoji} PnL: ${pnl:.2f}")
            
            logger.info(f"PnL for {symbol}: Invested ${total_invested:.2f}, PnL ${pnl:.2f}")
            send_telegram(msg)
            
    except Exception as e:
        logger.error(f"PnL calculation error: {e}")
        send_telegram(f"❌ PnL calculation error: {e}")

def stake_or_redeem(session, category, order_type, account_type, amount, coin):
    """Handles Staking/Redemption logic."""
    try:
        rounded_amount = round(amount, 2)
        prod_info = session.get_earn_product_info(category=category, coin=coin)
        if not prod_info['result']['list']: return False
        
        product_id = prod_info['result']['list'][0]['productId']
        res = session.stake_or_redeem(
            category=category, orderType=order_type, accountType=account_type,
            amount=str(rounded_amount), coin=coin, productId=product_id,
            orderLinkId=f"{order_type.lower()}-{int(time.time())}"
        )
        if res and res['retCode'] == 0:
            logger.info(f"{order_type} {rounded_amount} {coin} success.")
            send_telegram(f"✅ {order_type} {rounded_amount} {coin} success.")
            return True
        return False
    except Exception as e:
        logger.error(f"Stake/Redeem error: {e}")
        send_telegram(f"❌ Stake/Redeem error: {e}")
        return False

def get_crypto_allocation():
    """Parses allocation from .env."""
    alloc = {}
    m_string = os.getenv('CRYPTO_ALLOCATION_STRING', '')
    if m_string:
        for pair in m_string.split(','):
            if ':' in pair:
                s, m = pair.strip().split(':')
                alloc[s.upper()] = float(m)
    return alloc

def get_max_prices():
    """Parses max buy prices per coin from .env"""
    prices = {}
    m_string = os.getenv('MAX_PRICE_STRING', '')
    if m_string:
        for pair in m_string.split(','):
            if ':' in pair:
                coin, price = pair.strip().split(':')
                prices[coin.upper()] = float(price)
    return prices

def get_current_price(session, coin):
    """Returns current spot price for a coin."""
    try:
        ticker = session.get_tickers(category='spot', symbol=f"{coin}{USD_TYPE}")
        if ticker['result']['list']:
            return float(ticker['result']['list'][0]['lastPrice'])
    except Exception as e:
        logger.error(f"Error getting price for {coin}: {e}")
    return None

def get_avg_buy_price(symbol):
    """Returns average buy price for a symbol from trade log."""
    if not supabase_client:
        return None
    try:
        response = supabase_client.table('trade_log').select('quantity,total_usd').eq('symbol', symbol).execute()
        if not response.data:
            return None
        df = pd.DataFrame(response.data)
        total_qty = df['quantity'].sum()
        return df['total_usd'].sum() / total_qty if total_qty > 0 else None
    except Exception as e:
        logger.error(f"Error getting avg buy price for {symbol}: {e}")
        return None

def get_rebalance_multipliers(session, allocation):
    """Returns per-coin buy multipliers based on portfolio drift from target allocation."""
    threshold = float(os.getenv('REBALANCE_THRESHOLD', 5))
    boost = float(os.getenv('REBALANCE_MULTIPLIER', 1.5))

    portfolio = {}
    for coin in allocation:
        price = get_current_price(session, coin)
        if price:
            portfolio[coin] = get_coin_balance(session, coin) * price

    total_value = sum(portfolio.values())
    if total_value == 0:
        return {coin: 1.0 for coin in allocation}

    total_alloc = sum(allocation.values())
    target_weights = {coin: mult / total_alloc for coin, mult in allocation.items()}

    multipliers = {}
    for coin in allocation:
        drift = (target_weights[coin] - portfolio.get(coin, 0) / total_value) * 100
        if drift > threshold:
            multipliers[coin] = boost
            msg = f"⚖️ Rebalance: {coin} underweight by {drift:.1f}%, applying {boost}x boost"
            logger.info(msg)
            send_telegram(msg)
        else:
            multipliers[coin] = 1.0
    return multipliers

def send_periodic_report(session, period):
    """Sends weekly or monthly PnL report to Telegram."""
    days = 7 if period == 'weekly' else 30
    from_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    label = 'Weekly' if period == 'weekly' else 'Monthly'
    send_telegram(f"📅 {label} PnL Report ({from_date} → today)")
    calculate_PnL(session, from_date=from_date)

# === Main DCA logic ===
def run_dca_bot(session):
    logger.info(f"Starting DCA bot using {USD_TYPE}")
    send_telegram(f"🤖 DCA Bot starting. Target Stablecoin: {USD_TYPE}")
    
    allocation = get_crypto_allocation()
    if not allocation: 
        logger.error("No allocation found in CRYPTO_ALLOCATION_STRING.")
        send_telegram("❌ No allocation found. DCA aborted.")
        return 
    
    total_needed = sum(allocation.values()) * DAILY_USD
    current_bal = get_coin_balance(session, USD_TYPE)
    
    if current_bal < total_needed:
        deficit = total_needed - current_bal
        logger.info(f"Deficit of {deficit:.2f} {USD_TYPE}. Checking Flexible Saving...")
        send_telegram(f"⚠️ Insufficient {USD_TYPE} balance. Attempting to redeem {deficit:.2f} {USD_TYPE} from Flexible Saving.")
        
        staked = session.get_staked_position(category='FlexibleSaving', coin=USD_TYPE)
        if staked['result']['list']:
            pos = staked['result']['list'][0]
            redeemable = float(pos.get('redeemableAmount') or pos.get('amount', 0))
            send_telegram(f"ℹ️ Flexible Saving redeemable amount: {redeemable:.2f} {USD_TYPE}.")
            
            if redeemable > 0:
                to_redeem = min(redeemable, max(deficit * BUFFER_MULTIPLIER, MIN_REDEMPTION_USD))
                logger.info(f"Redeeming {to_redeem:.2f} {USD_TYPE}...")
                send_telegram(f"🔄 Redeeming {to_redeem:.2f} {USD_TYPE} from Flexible Saving.")
                if stake_or_redeem(session, 'FlexibleSaving', 'Redeem', 'UNIFIED', to_redeem, USD_TYPE):
                    time.sleep(5)
                    current_bal = get_coin_balance(session, USD_TYPE)

    if current_bal < total_needed:
        msg = f"❌ Insufficient {USD_TYPE} balance ({current_bal:.2f} < {total_needed:.2f}). DCA aborted."
        logger.error(msg)
        send_telegram(msg)
        return

    max_prices = get_max_prices()
    boost_threshold = float(os.getenv('DCA_BOOST_THRESHOLD', 0))
    boost_multiplier = float(os.getenv('DCA_BOOST_MULTIPLIER', 2.0))
    rebalance_multipliers = get_rebalance_multipliers(session, allocation)
    remaining_bal = current_bal

    for coin, multiplier in allocation.items():
        base_buy_usd = multiplier * DAILY_USD
        final_buy_usd = base_buy_usd

        current_price = get_current_price(session, coin)
        if current_price is None:
            send_telegram(f"❌ Could not fetch price for {coin}. Skipping.")
            continue

        # Price limit check
        max_price = max_prices.get(coin)
        if max_price and current_price > max_price:
            msg = f"⏭️ Skipping {coin}: price ${current_price:,.2f} > max ${max_price:,.2f}"
            logger.info(msg)
            send_telegram(msg)
            continue

        # DCA boost
        if boost_threshold > 0:
            avg_price = get_avg_buy_price(f"{coin}{USD_TYPE}")
            if avg_price and current_price <= avg_price * (1 - boost_threshold / 100):
                boosted_amount = base_buy_usd * boost_multiplier
                if remaining_bal >= boosted_amount:
                    drop_pct = (avg_price - current_price) / avg_price * 100
                    final_buy_usd = boosted_amount
                    msg = f"🚀 DCA boost for {coin}: price down {drop_pct:.1f}% from avg ${avg_price:.4f}, buying ${final_buy_usd:.2f}"
                    logger.info(msg)
                    send_telegram(msg)

        # Rebalance boost
        rebal = rebalance_multipliers.get(coin, 1.0)
        if rebal > 1.0:
            rebalanced_amount = final_buy_usd * rebal
            if remaining_bal >= rebalanced_amount:
                final_buy_usd = rebalanced_amount

        remaining_bal -= final_buy_usd

        logger.info(f"Buying {coin} with {final_buy_usd:.2f} {USD_TYPE}")
        send_telegram(f"💰 Buying {coin} with {final_buy_usd:.2f} {USD_TYPE}")

        order = convert_coins(USD_TYPE, coin, 'eb_convert_uta', final_buy_usd, session)

        if float(order[3]) > 0:
            log_trade(
                symbol=f"{coin}{USD_TYPE}",
                quantity=order[3],
                price=float(order[2])/float(order[3]),
                total_usd=order[2]
            )

            new_bal = get_coin_balance(session, coin)
            if coin == 'SOL' and new_bal >= 0.17:
                stake_or_redeem(session, 'OnChain', 'Stake', 'UNIFIED', new_bal, coin)
            elif coin == 'ETH' and new_bal >= 0.01:
                send_telegram(f"❗ ETH balance: {new_bal:.4f}. Consider manual staking.")

    calculate_PnL(session, from_date=os.getenv('PNL_FROM_DATE'))

def main():
    parser = argparse.ArgumentParser(description='DCA Bot')
    parser.add_argument('--report', choices=['weekly', 'monthly'], help='Send PnL report without running DCA')
    args = parser.parse_args()

    if not API_KEY or not API_SECRET:
        logger.error("API_KEY or API_SECRET missing.")
        return

    session = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=False)

    if args.report:
        send_periodic_report(session, args.report)
        return

    if DAILY_USD <= 0:
        logger.error("DAILY_USD is not set or zero. Aborting.")
        return

    run_dca_bot(session)

if __name__ == '__main__':
    main()