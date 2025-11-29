import os
import sys
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from datetime import datetime
import requests
import time
import pandas as pd
import numpy as np
from loguru import logger 

# --- Configure Loguru ---
logger.remove()
logger.add(sys.stderr, level="INFO")

# create 'log' directory if it doesn't exist
if not os.path.exists('log'):
    os.makedirs('log')

# add file logger with weekly rotation
logger.add(r"log/dca_bot_{time:YYYY-MM-DD}.log", rotation="1 week", level="DEBUG") 

# === Settings ===
load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
DAILY_USD = float(os.getenv('DAILY_USD', 0.0))

# Getting the list of stablecoins, default to 'USDT,USDC'
stablecoin_env = os.getenv('STABLECOIN_LIST')
if stablecoin_env is None or stablecoin_env.strip() == '':
    stablecoin_env = 'USDT,USDC'
    
STABLECOIN_LIST = [c.strip() for c in stablecoin_env.split(',')] 
USD_TYPE = STABLECOIN_LIST[0] # Initial primary stablecoin (e.g., USDT)

# --- Constants ---
BUFFER_MULTIPLIER = 1.015 # buffer to cover conversion fees/slippage
MIN_REDEMPTION_USD = 10.0 # minimum redemption amount from Flexible Saving

# === Log file ===
LOG_FILE = 'trade_log.csv'

# --- Utility Functions (Telegram, Balance, Convert, Log, Stake/Redeem, Allocation) ---

def send_telegram(message):
    """Sends a notification message to the configured Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    }
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            requests.post(url, data=payload)
        else:
            logger.warning("Telegram not configured (missing token/chat ID).")
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
    """
    Executes a market buy equivalent using the Bybit Convert API.
    Returns (quote_fromCoin, quote_toCoin, quote_fromAmount, quote_toAmount).
    """
    try:
        # Check minimum amount constraint for conversion
        if usd_amount < 0.01: 
             logger.warning(f"Conversion amount {usd_amount:.6f} is too small. Skipping conversion.")
             return (fromCoin, toCoin, "0.0", "0.0")

        request_a_quote = session.request_a_quote(
            fromCoin=fromCoin, toCoin=toCoin, accountType=accountType,
            requestCoin=fromCoin, requestAmount=str(usd_amount)
        )
        
        quote_toCoin = request_a_quote['result']['toCoin']
        quote_fromCoin = request_a_quote['result']['fromCoin']
        quote_fromAmount = request_a_quote['result']['fromAmount']
        quote_toAmount = request_a_quote['result']['toAmount']

        quote_id = request_a_quote['result']['quoteTxId']
        session.confirm_a_quote(quoteTxId=quote_id)
        
        msg = (f'Processed quote: {quote_fromAmount} {quote_fromCoin} ---> '
               f'{quote_toAmount} {quote_toCoin}')
        logger.info(msg) 
        send_telegram(f'✅ {msg}')

        return (quote_fromCoin, quote_toCoin, quote_fromAmount, quote_toAmount)
    except Exception as e:
        error_msg = f"❌ Error during coin conversion ({fromCoin} to {toCoin}): {e}"
        logger.error(error_msg) 
        send_telegram(error_msg)
        return (fromCoin, toCoin, "0.0", "0.0") 

def log_trade(symbol, quantity, price, total_usd):
    """Logs the executed trade details to the CSV file."""
    trade_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'symbol': symbol, 'quantity': quantity, 'price': price, 'total_usd': total_usd
    }

    df = pd.DataFrame([trade_data])
    if not os.path.exists(LOG_FILE):
        df.to_csv(LOG_FILE, index=False)
    else:
        df.to_csv(LOG_FILE, mode='a', header=False, index=False)

def stake_or_redeem(session, category, order_type, account_type, amount, coin):
    """Performs staking or redemption using the Bybit Earn API."""
    
    # round to 6 decimal places 
    rounded_amount = round(amount, 6) 
    
    productIdInfo = session.get_earn_product_info(category=category, coin=coin)
    if not productIdInfo['result']['list']:
        logger.warning(f"Product ID not found for {coin} in {category}. Skipping {order_type}.")
        send_telegram(f"❗ Product ID not found for {coin} in {category}. Skipping {order_type}.")
        return False 
        
    productId = productIdInfo['result']['list'][0]['productId']
    orderLinkId = f"{order_type.lower()}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    try:
        response = session.stake_or_redeem(
            category=category, orderType=order_type, accountType=account_type,
            amount=str(rounded_amount), 
            coin=coin, productId=productId, orderLinkId=orderLinkId
        )
        if response and response['retCode'] == 0:
            logger.info(f"✅ {order_type} {rounded_amount} {coin} successfully.")
            send_telegram(f"✅ {order_type} {rounded_amount:.6f} {coin} successfully.")
            return True 
        else:
            ret_msg = response.get('retMsg', 'Unknown error')
            logger.error(f"Error during stake/redeem: {ret_msg} (ErrCode: {response.get('retCode')})")
            send_telegram(f"❌ Error during stake/redeem: {ret_msg}")
            return False
    except Exception as e:
        logger.error(f"Error during stake/redeem: {e}")
        send_telegram(f"❌ Error during stake/redeem: {e}")
        return False

def get_crypto_allocation():
    """Reads and parses the crypto allocation strategy from the .env file."""
    crypto_allocation = {}
    multiplier_string = os.getenv('CRYPTO_ALLOCATION_STRING') or os.getenv('CRYPTO_MULTIPLIER_STRING', '') 
    
    if multiplier_string:
        try:
            pairs = multiplier_string.split(',')
            for pair in pairs:
                if ':' in pair:
                    symbol, multiplier_str = pair.strip().split(':')
                    crypto_allocation[symbol.upper()] = float(multiplier_str)
            
            if crypto_allocation and abs(sum(crypto_allocation.values()) - 1.0) > 0.001:
                logger.warning("Total crypto allocation does not sum to 1.0 (100%).")
                send_telegram("⚠️ Warning: Total crypto allocation does not sum to 1.0 (100%). Check settings.")
                
        except Exception as e:
            logger.error(f"Error parsing crypto allocation string: {e}")
            send_telegram(f"❌ Error parsing crypto allocation string: {e}. Check .env format.")
    
    if not crypto_allocation:
        logger.info("No valid crypto allocation string found. Exiting DCA bot.")
        send_telegram("❌ No valid crypto allocation string found. Exiting DCA bot.")

    return crypto_allocation


# === Main process ===
def run_dca_bot(session):
    """The core DCA logic: checks balance, redeems if needed, converts if needed, and executes buys."""
    global USD_TYPE
    logger.info(f"[{datetime.now()}] Starting DCA bot")
    send_telegram("🤖 Daily DCA bot is now running...")
    
    crypto_allocation = get_crypto_allocation()
    if not crypto_allocation:
        return 
    
    usd_amount_needed = np.sum(list(crypto_allocation.values())) * DAILY_USD
    
    # --- 1. Check initial balance of the primary stablecoin (USD_TYPE) ---
    current_coin_balance = get_coin_balance(session, USD_TYPE, account_type='UNIFIED')
    logger.info(f"Available {USD_TYPE} in Unified Account: {current_coin_balance:.2f}")

    final_coin_balance = current_coin_balance
    amount_to_cover = usd_amount_needed - final_coin_balance
    
    # --- 2. Attempt redemption of PRIMARY stablecoin (USDT) with BUFFER and MIN_LIMIT ---
    if amount_to_cover > 0: 
        logger.info(f"Deficit found in {USD_TYPE} Unified Account: {amount_to_cover:.2f}. Checking {USD_TYPE} in Flexible Saving.")
        
        redeemable_amount = 0.0
        try:
            staked_info = session.get_staked_position(category='FlexibleSaving', coin=USD_TYPE) 
            
            if staked_info['result']['list']:
                position = staked_info['result']['list'][0]
                # Use 'redeemableAmount' or fallback to 'amount'
                redeemable_amount = float(position.get('redeemableAmount') or position.get('amount', 0.0))
                logger.info(f"Flexible Saving Redeemable {USD_TYPE}: {redeemable_amount:.2f}")
                
        except Exception as e:
            logger.error(f"Error fetching Primary Coin Flexible Saving position: {e}")

        
        if redeemable_amount > 0:
            # 2a. Calculate required amount with buffer
            required_with_buffer = amount_to_cover * BUFFER_MULTIPLIER
            
            # 2b. Apply minimum redemption limit
            amount_to_redeem = max(required_with_buffer, MIN_REDEMPTION_USD)
            
            # 2c. Ensure we don't redeem more than available
            amount_to_redeem = min(amount_to_redeem, redeemable_amount)
            
            logger.info(f"Attempting to redeem {amount_to_redeem:.6f} {USD_TYPE} from Flexible Saving (Required: {amount_to_cover:.2f})...")
            send_telegram(f"⏳ Insufficient balance. Attempting to redeem {amount_to_redeem:.2f} {USD_TYPE} from Flexible Saving.")

            if stake_or_redeem(session, category='FlexibleSaving', order_type='Redeem', account_type='UNIFIED', amount=amount_to_redeem, coin=USD_TYPE):
                time.sleep(7.5) 

        # Re-check balance after primary coin redemption attempt
        final_coin_balance = get_coin_balance(session, USD_TYPE, account_type='UNIFIED')
        amount_to_cover = usd_amount_needed - final_coin_balance # Recalculate deficit

    # --- 3. Cover remaining deficit using SECONDARY stablecoins from Flexible Saving (with BUFFER and MIN_LIMIT) ---
    if amount_to_cover > 0:
        logger.info(f"Deficit of {amount_to_cover:.2f} {USD_TYPE} remains. Checking secondary stablecoin Flexible Savings.")
        
        # Iterate over secondary stablecoins (e.g., USDC)
        for stablecoin in STABLECOIN_LIST:
            if stablecoin == USD_TYPE:
                continue
            
            # --- 3a. Check redeemable amount of secondary stablecoin from Flexible Saving ---
            secondary_redeemable = 0.0
            try:
                staked_info = session.get_staked_position(category='FlexibleSaving', coin=stablecoin) 
                if staked_info['result']['list']:
                    position = staked_info['result']['list'][0]
                    secondary_redeemable = float(position.get('redeemableAmount') or position.get('amount', 0.0))
                    logger.info(f"Flexible Saving Redeemable {stablecoin}: {secondary_redeemable:.2f}")
            except Exception as e:
                logger.warning(f"Error checking {stablecoin} Flexible Saving position: {e}")
                continue # Skip this coin

            if secondary_redeemable > 0:
                # 3b. Calculate required amount with buffer (assuming 1:1 conversion rate)
                required_with_buffer = amount_to_cover * BUFFER_MULTIPLIER
                
                # 3c. Apply minimum redemption limit
                amount_to_redeem = max(required_with_buffer, MIN_REDEMPTION_USD)
                
                # 3d. Ensure we don't redeem more than available
                amount_to_redeem = min(amount_to_redeem, secondary_redeemable)
                
                logger.info(f"Attempting to redeem {amount_to_redeem:.6f} {stablecoin} for conversion (Required: {amount_to_cover:.2f}, Min Redeem: {MIN_REDEMPTION_USD:.2f}).")
                send_telegram(f"⏳ Redeeming {amount_to_redeem:.2f} {stablecoin} from Flexible Saving.")

                # --- 3e. Redeem the required amount of secondary coin to UTA ---
                if stake_or_redeem(session, category='FlexibleSaving', order_type='Redeem', account_type='UNIFIED', amount=amount_to_redeem, coin=stablecoin):
                    time.sleep(5) 
                    
                    # --- 3f. Convert the redeemed amount on UTA to the PRIMARY coin (USDT) ---
                    # We convert the ENTIRE amount that was just redeemed (to utilize the buffer).
                    redeemed_balance_on_uta = get_coin_balance(session, stablecoin, account_type='UNIFIED')
                    conversion_amount = redeemed_balance_on_uta 
                    
                    logger.info(f"Attempting conversion of ALL {conversion_amount:.2f} {stablecoin} to {USD_TYPE}.")

                    order = convert_coins(
                        fromCoin=stablecoin,
                        toCoin=USD_TYPE,
                        accountType='eb_convert_uta',
                        usd_amount=conversion_amount,
                        session=session
                    )
                    
                    if float(order[3]) > 0.0:
                        logger.info(f"Conversion successful. Gained {float(order[3]):.2f} {USD_TYPE}.")
                        
                        # Re-check balance and deficit after conversion
                        final_coin_balance = get_coin_balance(session, USD_TYPE, account_type='UNIFIED')
                        amount_to_cover = usd_amount_needed - final_coin_balance
                        
                        if amount_to_cover <= 0:
                            logger.info("Deficit successfully covered by secondary stablecoin conversion.")
                            break # Deficit covered, stop checking other secondary coins
                        else:
                            logger.warning(f"Deficit remains after {stablecoin} conversion: {amount_to_cover:.2f}")
                    else:
                        logger.error(f"Conversion of {stablecoin} failed.")

    # --- 4. Final balance check after ALL attempts ---
    final_coin_balance = get_coin_balance(session, USD_TYPE, account_type='UNIFIED')
    
    if final_coin_balance < usd_amount_needed:
        logger.error(f"❌ After all attempts, insufficient {USD_TYPE} balance for daily DCA: {final_coin_balance:.2f} < {usd_amount_needed:.2f}. Halting DCA.")
        send_telegram(
            f"❌ After all attempts (Flexible Saving + Conversion), insufficient {USD_TYPE} balance for daily DCA: {final_coin_balance:.2f} < {usd_amount_needed:.2f}. Halting DCA."
        )
        return
    
    # --- 5. Core DCA logic: buying ---
    for symbol, multiplier in crypto_allocation.items():
        # ... (buying logic remains the same)
        buy_amount_usd = multiplier * DAILY_USD
        
        if final_coin_balance < buy_amount_usd:
            logger.warning(f"Not enough {USD_TYPE} to buy {symbol}. Required {buy_amount_usd:.2f}, available {final_coin_balance:.2f}. Skipping {symbol}.")
            send_telegram(f"❗ Not enough {USD_TYPE} to buy {symbol}. Required {buy_amount_usd:.2f}, available {final_coin_balance:.2f}. Skipping {symbol}.")
            continue 

        logger.info(f"Buying {symbol} with {buy_amount_usd:.2f} {USD_TYPE}...")
        send_telegram(
            f"🤖 Buying {symbol} with {buy_amount_usd:.2f} {USD_TYPE}..."
        )
        order = convert_coins(
            fromCoin=USD_TYPE,
            toCoin=symbol,
            accountType='eb_convert_uta',
            usd_amount=buy_amount_usd,
            session=session
            )
        
        if float(order[3]) > 0:
            log_trade(
                symbol=f"{order[1]}{order[0]}",
                quantity=order[3],
                price=float(order[2])/float(order[3]) if float(order[3]) != 0 else 0,
                total_usd=order[2]
            )
            
            final_coin_balance -= float(order[2]) 
        
        # --- 6. Auto-staking/Liquidity adding logic ---
        # Note: This step is now separate from the deficit covering logic.
        coin_balance = get_coin_balance(session, symbol, account_type='UNIFIED')
        if symbol == 'SOL' and coin_balance >= 0.1:
            logger.info(f"Trying to stake {coin_balance} {symbol} to OnChain Earn...")
            stake_or_redeem(
                session,
                category='OnChain',
                order_type='Stake',
                account_type='UNIFIED',
                amount=coin_balance,
                coin=symbol
            )
        elif symbol == 'ETH' and coin_balance >= 0.01:
            send_telegram(
                f"❗ACTION REQUIRED: Manually add {coin_balance:.6f} {symbol} to Mining Liquidity."
            )

def daily_dca():
    """Initializes the session and runs the main DCA bot."""
    if not API_KEY or not API_SECRET:
        logger.error("Error: API_KEY or API_SECRET is missing. Cannot initialize session.")
        return
        
    session = HTTP(
        api_key=API_KEY,
        api_secret=API_SECRET,
        testnet=False    # True for testnet
    )
    run_dca_bot(session)


# === Start ===
if __name__ == '__main__':
    daily_dca()