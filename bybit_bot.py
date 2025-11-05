from pybit.unified_trading import HTTP # type: ignore
from dotenv import load_dotenv
from datetime import datetime
import requests
import time
import pandas as pd
import schedule # type: ignore
import os
import uuid
import numpy as np


# === Settings ===
load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
DAILY_USD = float(os.getenv('DAILY_USD'))
USD_TYPE = os.getenv('STABLECOIN')

CRYPTO_MULTIPLIER = {
    'ETH': 0.5,  # 50% of daily USD
    'SOL': 0.3,   # 30% of daily USD
    'TON': 0.2    # 20% of daily USD
}

# === Log file ===
LOG_FILE = 'trade_log.csv'


# ✅ Send Telegram notification
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    }
    try:
        requests.post(url, data=payload)
    except Exception as e:
        print(f"Telegram error: {e}")

# === Get USDT balance ===
def get_coin_balance(session, coin, account_type='UNIFIED'):
    try:
        balances = session.get_wallet_balance(accountType=account_type, coin=coin)
        available_balance = balances['result']['list'][0]['coin'][0]['walletBalance']
        return round(float(available_balance), 6)
    except Exception as e:
        print(f"Error getting {coin} balance: {e}")
        send_telegram(f"❌ Error getting {coin} balance: {e}")
        return 0.0

# === Market buy asset ===
def convert_coins(fromCoin,toCoin, accountType, usd_amount, session):

    request_a_quote = session.request_a_quote(
        fromCoin=fromCoin,
        toCoin=toCoin,
        accountType=accountType,
        requestCoin=fromCoin,
        requestAmount=str(usd_amount)
    )
    quote_toCoin = request_a_quote['result']['toCoin']
    quote_fromCoin = request_a_quote['result']['fromCoin']
    quote_fromAmount = request_a_quote['result']['fromAmount']
    quote_toAmount = request_a_quote['result']['toAmount']

    quote_id = request_a_quote['result']['quoteTxId']
    
    session.confirm_a_quote(
        quoteTxId=quote_id
    )
    print(f'Processed quote: {quote_fromAmount} {quote_fromCoin} ---> \
        {quote_toAmount} {quote_toCoin}')
    send_telegram(
        f'✅ Processed quote: {quote_fromAmount} {quote_fromCoin} ---> \
        {quote_toAmount} {quote_toCoin}'
    )

    return (quote_fromCoin, quote_toCoin, quote_fromAmount, quote_toAmount)

# === Log actions ===
def log_trade(symbol, quantity, price, total_usd):
    trade_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'symbol': symbol,
        'quantity': quantity,
        'price': price,
        'total_usd': total_usd
    }

    df = pd.DataFrame([trade_data])

    if not os.path.exists(LOG_FILE):
        df.to_csv(LOG_FILE, index=False)
    else:
        df.to_csv(LOG_FILE, mode='a', header=False, index=False)


def stake_or_redeem(session, category, order_type, account_type, amount, coin):
    productIdInfo = session.get_earn_product_info(
        category=category,
        coin=coin
    )
    productId = productIdInfo['result']['list'][0]['productId']
    # TODO: add check for minimum amount. Min USD amount=10
    orderLinkId = f"{order_type.lower()}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    try:
        response = session.stake_or_redeem(
            category=category,
            orderType=order_type,
            accountType=account_type,
            amount=str(amount),
            coin=coin,
            productId=productId,
            orderLinkId=orderLinkId
        )
        if response and response['retCode'] == 0:
            print(f"✅ {order_type} {amount} {coin} successfully.")
            send_telegram(f"✅ {order_type} {amount} {coin} successfully.")
    except Exception as e:
        print(f"Error during stake/redeem: {e}")
        send_telegram(f"❌ Error during stake/redeem: {e}")
            

def calculate_PnL(session):
    try:
        trades = pd.read_csv(LOG_FILE)
        for symbol in trades['symbol'].unique():
            symbol_trades = trades[trades['symbol'] == symbol]
            bought_quantity = symbol_trades['quantity'].sum()
            spended_usd = symbol_trades['total_usd'].sum()
            symbol_info = session.get_tickers(
                category='spot',
                symbol=symbol,
            )
            symbol_current_price = symbol_info['result']['list'][0]['lastPrice']
            symbol_usd_value = bought_quantity * float(symbol_current_price)
            pnl = symbol_usd_value - spended_usd
            pnl_pct = round((pnl / spended_usd) * 100 if spended_usd > 0 else 0, 2)
            pnl_pct = f"{pnl_pct:.2f}%"
            msg = f" - {symbol}: {bought_quantity:.4f} bought, spent {spended_usd:.2f} USD, current value {symbol_usd_value:.2f} USD\n{symbol} PnL: {pnl:.2f} USD ({pnl_pct})"
            print(msg)
            send_telegram(msg)
    except Exception as e:
        err = f"❌ Error calculating total PnL: {e}"
        print(err)
        send_telegram(err)


# === Main process ===
def run_dca_bot(session):
    print(f"[{datetime.now()}] Starting DCA bot")
    send_telegram("🤖 Daily DCA bot is now running...")
    
    coin_balance = get_coin_balance(session, USD_TYPE, account_type='UNIFIED')
    print(f"Available {USD_TYPE}: {coin_balance:.2f}")
    usd_amount_needed = np.sum(list(CRYPTO_MULTIPLIER.values())) * DAILY_USD
    if coin_balance < (usd_amount_needed):
        print(f"Insufficient {USD_TYPE} balance for daily DCA: {coin_balance:.2f} < {usd_amount_needed:.2f}")
        send_telegram(
            f"❌ Insufficient {USD_TYPE} balance for daily DCA: {coin_balance:.2f} < {usd_amount_needed:.2f}"
        )

        #TODO: ADD CHECK volatility GET TICKERS https://bybit-exchange.github.io/docs/v5/market/tickers
        #TODO: ADD CHECK minOrderQty https://bybit-exchange.github.io/docs/v5/market/instrument

        stake_or_redeem(
            session,
            category='FlexibleSaving',
            order_type='Redeem',
            account_type='UNIFIED',
            amount=usd_amount_needed,
            coin=USD_TYPE
        )
        time.sleep(7.5)

    for symbol, multiplier in CRYPTO_MULTIPLIER.items():
        print(f"Buying {symbol} with {multiplier * DAILY_USD:.2f} {USD_TYPE}...")
        send_telegram(
            f"🤖 Buying {symbol} with {multiplier * DAILY_USD:.2f} {USD_TYPE}..."
        )
        order = convert_coins(
            fromCoin=USD_TYPE,
            toCoin=symbol,
            accountType='eb_convert_uta',
            usd_amount=multiplier * DAILY_USD,
            session=session
            )
        log_trade(
            symbol=f"{order[1]}{order[0]}",
            quantity=order[3],
            price=float(order[2])/float(order[3]),
            total_usd=order[2]
        )
        # OnChain earn limits: min 0.1 ETH; 0.1 SOL
        # Mining liquidity limits: min 0.01 ETH; 2 SOL
        coin_balance = get_coin_balance(session, symbol, account_type='UNIFIED')
        if symbol == 'SOL' and coin_balance >= 0.1:
            print(f"Trying to stake {coin_balance} {symbol} to OnChain Earn...")
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
                f"❗NEED MANUALLY ADD {coin_balance} {symbol} TO MINING LIQUIDITY"
            )


def daily_dca():
    os.chdir(r'd:\Python\my-dca-bot')
    session = HTTP(
        api_key=API_KEY,
        api_secret=API_SECRET,
        testnet=False  # True for testnet
    )
    run_dca_bot(session)
    calculate_PnL(session)


# === Start ===
if __name__ == '__main__':

    # ⏰ Schedule to run every day at 10:00 AM
    schedule.every().day.at("10:00").do(daily_dca)

    while True:
        schedule.run_pending()
        time.sleep(60)




# --- For testing ---
# transfer = session.create_internal_transfer(
#     transferId=f'{uuid.uuid1()}',
#     coin='SOL',
#     amount='0.01',                                    <<<--- Transfer
#     fromAccountType='UNIFIED',
#     toAccountType='FUND'
# )
# print(transfer)
