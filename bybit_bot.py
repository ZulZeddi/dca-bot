import os
import sys
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from datetime import datetime
import requests
import time
import numpy as np
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
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            requests.post(url, data=payload)
        else:
            logger.warning("Telegram not configured.")
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
        session.confirm_a_quote(quoteTxId=quote_id)
        
        res = request_a_quote['result']
        logger.info(f"Converted {res['fromAmount']} {res['fromCoin']} to {res['toAmount']} {res['toCoin']}")
        return (res['fromCoin'], res['toCoin'], res['fromAmount'], res['toAmount'])
    except Exception as e:
        logger.error(f"Conversion error: {e}")
        return (fromCoin, toCoin, "0.0", "0.0") 

def log_trade(symbol, quantity, price, total_usd):
    """Logs the trade details into Supabase."""
    if not supabase_client:
        logger.error("Supabase client not initialized. Cannot log trade.")
        return
    try:
        data = {
            'trade_id': f"{symbol}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'quantity': float(quantity),
            'price': float(price),
            'total_usd': float(total_usd)
        }
        supabase_client.table('trade_log').insert(data).execute()
    except Exception as e:
        logger.error(f"Supabase logging error: {e}")

def calculate_PnL(session, from_date=None, to_date=None):
    """Calculates PnL comparing Supabase spend vs current market value with date filtering."""
    if not supabase_client:
        logger.error("Supabase client not initialized. Cannot calculate PnL.")
        return
    try:
        data_response = supabase_client.table('trade_log').select('*').execute()
        if not data_response.data:
            return

        trades = pd.DataFrame(data_response.data)
        trades['timestamp'] = pd.to_datetime(trades['timestamp'])
        trades['symbol'] = trades['symbol'].apply(lambda x: x.replace('USDT', '').replace('USDC', ''))

        # Data Cleaning: Handle missing prices and extract base symbol
        if trades['price'].isnull().any():
            trades['price'] = trades['price'].fillna(trades['total_usd'] / trades['quantity'])

        # Date Filtering
        if from_date:
            trades = trades[trades['timestamp'] >= pd.to_datetime(from_date)]
        else:
            from_date = trades['timestamp'].min().strftime('%Y-%m-%d')

        if to_date:
            trades = trades[trades['timestamp'] <= pd.to_datetime(to_date)]
        else:
            to_date = datetime.now().strftime('%Y-%m-%d')
        
        if trades.empty:
            logger.info(f"No trades found for period {from_date} to {to_date}")
            return

        for symbol in trades['symbol'].unique():
            # Get base coin (e.g., BTC from BTCUSDT)
            symbol_trades = trades[trades['symbol'] == symbol]
            
            total_invested = symbol_trades['total_usd'].sum()
            total_quantity = symbol_trades['quantity'].sum()
            
            # Fetch current market price
            ticker = session.get_tickers(category='spot', symbol=symbol)
            if not ticker['result']['list']:
                continue
            
            current_symbol_price = float(ticker['result']['list'][0]['lastPrice'])
            current_value = total_quantity * current_symbol_price
            pnl = current_value - total_invested
            status_emoji = "🟢" if pnl >= 0 else "🔴"

            msg = (f"📊 PnL for {symbol} from {from_date} to {to_date}:\n"
                   f"   Invested: ${total_invested:.2f}\n"
                   f"   Current Value: ${current_value:.2f}\n"
                   f"{status_emoji} PnL: ${pnl:.2f}")
            
            logger.info(f"PnL for {symbol}: Invested ${total_invested:.2f}, PnL ${pnl:.2f}")
            send_telegram(msg)
            
    except Exception as e:
        logger.error(f"PnL calculation error: {e}")

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
            return True
        return False
    except Exception as e:
        logger.error(f"Stake/Redeem error: {e}")
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

# === Main DCA logic ===
def run_dca_bot(session):
    logger.info(f"Starting DCA bot using {USD_TYPE}")
    send_telegram(f"🤖 DCA Bot starting. Target Stablecoin: {USD_TYPE}")
    
    allocation = get_crypto_allocation()
    if not allocation: 
        logger.error("No allocation found in CRYPTO_ALLOCATION_STRING.")
        return 
    
    total_needed = sum(allocation.values()) * DAILY_USD
    current_bal = get_coin_balance(session, USD_TYPE)
    
    if current_bal < total_needed:
        deficit = total_needed - current_bal
        logger.info(f"Deficit of {deficit:.2f} {USD_TYPE}. Checking Flexible Saving...")
        
        staked = session.get_staked_position(category='FlexibleSaving', coin=USD_TYPE)
        if staked['result']['list']:
            pos = staked['result']['list'][0]
            redeemable = float(pos.get('redeemableAmount') or pos.get('amount', 0))
            
            if redeemable > 0:
                to_redeem = min(redeemable, max(deficit * BUFFER_MULTIPLIER, MIN_REDEMPTION_USD))
                logger.info(f"Redeeming {to_redeem:.2f} {USD_TYPE}...")
                if stake_or_redeem(session, 'FlexibleSaving', 'Redeem', 'UNIFIED', to_redeem, USD_TYPE):
                    time.sleep(5)
                    current_bal = get_coin_balance(session, USD_TYPE)

    if current_bal < total_needed:
        msg = f"❌ Insufficient {USD_TYPE} balance ({current_bal:.2f} < {total_needed:.2f}). DCA aborted."
        logger.error(msg)
        send_telegram(msg)
        return

    for coin, multiplier in allocation.items():
        buy_usd = multiplier * DAILY_USD
        logger.info(f"Buying {coin} with {buy_usd:.2f} {USD_TYPE}")
        
        order = convert_coins(USD_TYPE, coin, 'eb_convert_uta', buy_usd, session)
        
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
    if not API_KEY or not API_SECRET: 
        logger.error("API_KEY or API_SECRET missing.")
        return
    session = HTTP(api_key=API_KEY, api_secret=API_SECRET, testnet=False)
    run_dca_bot(session)

if __name__ == '__main__':
    main()