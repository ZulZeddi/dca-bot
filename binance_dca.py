import requests
import time
import pandas as pd
import schedule # type: ignore
from binance.spot import Spot as Client
from binance.error import ClientError
import datetime
import os
from dotenv import load_dotenv


load_dotenv()  # Load environment variables from .env file
# 🔐 Binance API
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

# 🔐 Telegram API
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

client = Client(API_KEY, API_SECRET)

# ⚙️ Settings
DAILY_USDC = 13
ETH_RATIO = 0.6
SOL_RATIO = 0.4
LOG_FILE = 'trade_log.csv'
MIN_REQUIRED_BALANCE = DAILY_USDC  # in USDC

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

# ✅ Unstake USDC from Flexible Savings
def redeem_usdc_from_flexible(amount):

    # client.get_flexible_product_position()
    try:
        response = client.redeem_flexible_product(
            "USDC001", amount=DAILY_USDC, recvWindow=10000
        )
    except ClientError as error:
        "Found error. status: {}, error code: {}, error message: {}".format(
            error.status_code, error.error_code, error.error_message
        )
    if response['success'] == True:
        msg = f"[{datetime.datetime.now()}] ✅ Unstaked {amount} USDC from Flexible Savings."
        print(msg)
        send_telegram(msg)
    else:
        err = f"❌ Unstaking failed: {response.text}"
        print(err)
        send_telegram(err)

# ✅ Check USDC Spot Wallet Balance
def get_usdc_balance():
    try:
        balance = client.user_asset(asset="USDC", recvWindow=5000)
        return float(balance[0]['free']) if balance else 0.0
    except Exception as e:
        err = f"❌ Error checking USDC balance: {e}"
        print(err)
        send_telegram(err)
        return 0.0

# ✅ Buy ETH/SOL with USDC
def buy_crypto(symbol: str, usdc_amount: float):
    try:
        params = {
            'symbol': symbol,
            'side': 'BUY',
            'type': 'MARKET',
            'quoteOrderQty': usdc_amount,
            'recvWindow': 5000
        }
        order = client.new_order(**params)

        # Get the actual filled quantity from the order response
        filled_qty = order['fills'][0]['qty']
        avg_price = float(order['fills'][0]['price'])
        log_trade(symbol, filled_qty, avg_price, usdc_amount)
        msg = f"✅ Bought {filled_qty} {symbol} at ~{avg_price} for {usdc_amount} USDC."
        print(msg)
        send_telegram(msg)

        # Subscribe to Flexible Savings for the bought asset
        asset = symbol.replace('USDC', '')
        subscribe_to_flexible_savings(asset, filled_qty)

    except Exception as e:
        err = f"❌ Error buying {symbol}: {e}"
        print(err)
        send_telegram(err)

# ✅ Log trade
def log_trade(symbol, quantity, price, total):
    trade_data = {
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'symbol': symbol,
        'quantity': quantity,
        'price': price,
        'total_usdc': total
    }

    df = pd.DataFrame([trade_data])

    if not os.path.exists(LOG_FILE):
        df.to_csv(LOG_FILE, index=False)
    else:
        df.to_csv(LOG_FILE, mode='a', header=False, index=False)

# ✅ Subscribe to Flexible Savings
def subscribe_to_flexible_savings(asset: str, amount: float):
    """
    Subscribe the specified amount of asset to Binance Flexible Savings.
    asset: e.g., 'ETH', 'SOL'
    amount: float, amount to subscribe
    """
    try:
        # Get productId for the asset (e.g., 'ETH001', 'SOL001')
        product_id = f"{asset}001"
        response = client.subscribe_flexible_product(
            productId=product_id,
            amount=amount,
            recvWindow=5000
        )
        if response.get('success', False):
            msg = f"✅ Subscribed {amount} {asset} to Flexible Savings."
            print(msg)
            send_telegram(msg)
        else:
            err = f"❌ Subscription failed: {response}"
            print(err)
            send_telegram(err)
    except Exception as e:
        err = f"❌ Error subscribing {asset} to Flexible Savings: {e}"
        print(err)
        send_telegram(err)

# ✅ Main DCA task
def daily_dca():
    send_telegram("📈 Starting daily USDC DCA process...")

    balance = get_usdc_balance()
    if balance < MIN_REQUIRED_BALANCE:
        redeem_usdc_from_flexible(MIN_REQUIRED_BALANCE)
        for i in range(10,1,-1):
            print(f"⏳ Waiting {i} seconds for redemption to complete...")
            time.sleep(1)  # wait for redemption to complete
        msg = f"⚠️ Skipping trade. USDC balance ({balance}) is below required amount ({MIN_REQUIRED_BALANCE})."
        print(msg)
        send_telegram(msg)
        return

    buy_crypto('ETHUSDC', DAILY_USDC * ETH_RATIO)
    buy_crypto('SOLUSDC', DAILY_USDC * SOL_RATIO)
    
    msg = f"✅ Daily DCA completed."
    print(msg)
    send_telegram(msg)

    # calculate total PnL
    try:
        trades = pd.read_csv(LOG_FILE)
        for symbol in trades['symbol'].unique():
            symbol_trades = trades[trades['symbol'] == symbol]
            bought_quantity = symbol_trades['quantity'].sum()
            spended_usdc = symbol_trades['total_usdc'].sum()
            symbol_current_price = client.avg_price(symbol)
            symbol_usdc_value = bought_quantity * float(symbol_current_price['price'])
            pnl = symbol_usdc_value - spended_usdc
            pnl_pct = round((pnl / spended_usdc) * 100 if spended_usdc > 0 else 0, 2)
            pnl_pct = f"{pnl_pct:.2f}%"
            msg = f" - {symbol}: {bought_quantity:.4f} bought, spent {spended_usdc:.2f} USDC, current value {symbol_usdc_value:.2f} USDC\nTotal PnL: {pnl:.2f} USDC ({pnl_pct})"
            print(msg)
            send_telegram(msg)
    except Exception as e:
        err = f"❌ Error calculating total PnL: {e}"
        print(err)
        send_telegram(err)

os.chdir(r'd:\Python\my-dca-bot')

daily_dca()  # Run once at startup to ensure initial state

# # ⏰ Schedule to run every day at 10:00 AM
# schedule.every().day.at("10:00").do(daily_dca)

# print("USDC-based DCA bot started...")
# send_telegram("🤖 USDC DCA bot is now running.")

# while True:
#     schedule.run_pending()
#     time.sleep(60)
