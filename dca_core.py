"""
Pure decision logic for the DCA bot.

This module imports STDLIB ONLY — no pybit, no supabase, no requests, no
pandas, and nothing from bybit_bot. That restriction is the point: it makes it
physically impossible for a sizing function to reach for a live `session`, a
module-level config global, or the network, so these functions can be tested
directly with no mocks and no credentials.

Every function here is deterministic: same inputs, same output, no I/O, no
clock. I/O and orchestration live in bybit_bot.py.
"""

from collections import namedtuple

# Result of planning a buy that may be split between a spot maker order and a
# Convert fallback.
FillPlan = namedtuple('FillPlan', 'spot_usd spot_qty convert_usd')


# ─── Money paths ───────────────────────────────────────────────────────────────

def apply_spend_caps(desired, max_spend, available):
    """
    Scales a {coin: usd} plan down to fit both the absolute per-run ceiling and
    the funds actually available. Proportional, so no coin is starved by dict
    order. Returns (scaled_plan, reason) where reason is None, 'max_spend' or
    'available'.

    Every dollar the bot spends passes through here.
    """
    total = sum(desired.values())
    if total <= 0:
        return ({coin: 0.0 for coin in desired}, None)

    limit, reason = total, None
    if max_spend is not None and max_spend < limit:
        limit, reason = max_spend, 'max_spend'
    if available is not None and available < limit:
        limit, reason = available, 'available'

    if limit >= total:
        return (dict(desired), None)

    limit = max(0.0, limit)
    scale = limit / total
    return ({coin: amt * scale for coin, amt in desired.items()}, reason)


def combine_multipliers(base_usd, *multipliers, max_combined=3.0):
    """
    Applies stacked buy multipliers to a base amount and clamps the *product*.
    Each multiplier is individually bounded elsewhere, but their product is not
    — without this clamp, correlated signals compound into a runaway buy.
    """
    if base_usd <= 0:
        return 0.0
    result = base_usd
    for m in multipliers:
        result *= m
    return min(result, base_usd * max_combined)


def plan_partial_fill(usd_amount, spot_usd, spot_qty, min_convert_usd):
    """
    Given what a spot maker order actually filled, decides how much still needs
    to go through Convert.

    A partial fill must be KEPT and only the residual converted. Discarding it
    and re-buying the full amount is a silent over-spend of up to ~2x.
    """
    if usd_amount <= 0:
        return FillPlan(0.0, 0.0, 0.0)

    spot_usd = max(0.0, spot_usd)
    spot_qty = max(0.0, spot_qty)

    # Treat >=99% as complete: chasing a few cents through Convert costs more in
    # spread than it recovers.
    if spot_usd >= usd_amount * 0.99:
        return FillPlan(spot_usd, spot_qty, 0.0)

    residual = usd_amount - spot_usd
    if residual < min_convert_usd:
        return FillPlan(spot_usd, spot_qty, 0.0)
    return FillPlan(spot_usd, spot_qty, residual)


def aggregate_fill(spot_usd, spot_qty, convert_usd, convert_qty):
    """Totals the two legs of a split buy."""
    return (spot_usd + convert_usd, spot_qty + convert_qty)


def implied_price(usd, qty):
    """Average fill price, or None when it cannot be derived."""
    if qty is None or usd is None or qty <= 0 or usd <= 0:
        return None
    return usd / qty


# ─── Balances: None means "unknown", never zero ────────────────────────────────

def total_holdings(spot, staked):
    """
    Sums liquid and staked balances. Returns None if EITHER is unknown.

    Collapsing an unknown to zero is how a failed read turns into an over-buy:
    almost the whole position lives in Earn, so "staked unknown -> 0" makes an
    owned coin look unbought.
    """
    if spot is None or staked is None:
        return None
    return spot + staked


def rebalance_multipliers(allocation, portfolio, threshold_pct=5.0, boost=1.5):
    """
    Buy-side boost for underweight coins.

    `portfolio` maps coin -> current value, or None if it could not be read. An
    incomplete portfolio makes every weight wrong (the unreadable coin looks
    like zero holdings and inflates everyone else's weight), so the whole
    rebalance is skipped rather than applied incorrectly.
    """
    neutral = {coin: 1.0 for coin in allocation}

    if any(portfolio.get(coin) is None for coin in allocation):
        return neutral

    total_value = sum(portfolio.get(coin, 0.0) for coin in allocation)
    total_alloc = sum(allocation.values())
    if total_value <= 0 or total_alloc <= 0:
        return neutral

    multipliers = {}
    for coin, weight in allocation.items():
        target_w = weight / total_alloc
        current_w = portfolio.get(coin, 0.0) / total_value
        drift_pp = (target_w - current_w) * 100
        multipliers[coin] = boost if drift_pp > threshold_pct else 1.0
    return multipliers


# ─── Signals ───────────────────────────────────────────────────────────────────

def market_boost_from_signals(fng_value, bb_signal, max_boost=2.0, floor=0.3):
    """
    Combines the Fear & Greed index with a Bollinger position into a buy
    multiplier.

    Note the buy-side adders (+0.75/+0.35/+0.50) outweigh the sell-side ones
    (-0.20/-0.10/-0.15), so the expected multiplier over a symmetric cycle is
    above 1.0 — average spend structurally exceeds the nominal daily budget.
    """
    boost = 1.0
    if fng_value <= 20:
        boost += 0.75
    elif fng_value <= 40:
        boost += 0.35
    elif fng_value >= 80:
        boost -= 0.20
    elif fng_value >= 65:
        boost -= 0.10

    if bb_signal > 0:
        boost += 0.50
    elif bb_signal < 0:
        boost -= 0.15

    return min(max(boost, floor), max_boost)


def bollinger_from_closes(closes, current_price, period=20, num_std=2.0):
    """
    Returns +1.0 below the lower band, -1.0 above the upper, else 0.0.

    `closes` must be chronological (oldest first) and must NOT include the
    in-progress candle. Returns 0.0 — never a buy signal — when there is not
    enough history, so a new listing is not mistaken for a dip.
    """
    if not closes or len(closes) < period or not current_price:
        return 0.0

    window = closes[-period:]
    sma = sum(window) / period
    variance = sum((c - sma) ** 2 for c in window) / period
    std = variance ** 0.5
    if std == 0:
        return 0.0

    if current_price < sma - num_std * std:
        return 1.0
    if current_price > sma + num_std * std:
        return -1.0
    return 0.0


def drawdown_from_high(candles, current_price):
    """
    Percentage drop from the highest high in `candles` to `current_price`.

    Peak-to-current, not oldest-open-to-current: a V-shaped crash that has
    partly recovered is invisible to an open-vs-last comparison, and that is
    precisely the move a circuit breaker exists to catch. Each candle is a
    Bybit kline row: [start, open, high, low, close, ...].
    """
    if not candles or not current_price:
        return 0.0
    highs = [float(c[2]) for c in candles]
    peak = max(highs)
    if peak <= 0:
        return 0.0
    return (peak - current_price) / peak * 100


# ─── Config parsing ────────────────────────────────────────────────────────────

def parse_allocation(raw):
    """
    'ETH:0.5,SOL:0.4' -> {'ETH': 0.5, 'SOL': 0.4}

    Malformed entries are skipped rather than raising: a typo in one coin must
    not abort the whole run before any buy happens.
    """
    alloc = {}
    for pair in (raw or '').split(','):
        if ':' not in pair:
            continue
        coin, _, weight = pair.strip().partition(':')
        try:
            value = float(weight)
        except ValueError:
            continue
        if value > 0 and coin.strip():
            alloc[coin.strip().upper()] = value
    return alloc


def parse_max_prices(raw):
    """'BTC:100000,ETH:5000' -> {'BTC': 100000.0, 'ETH': 5000.0}"""
    prices = {}
    for pair in (raw or '').split(','):
        if ':' not in pair:
            continue
        coin, _, price = pair.strip().partition(':')
        try:
            prices[coin.strip().upper()] = float(price)
        except ValueError:
            continue
    return prices


def parse_stake_config(raw):
    """
    'SOL:OnChain:0.2,ETH:FlexibleSaving:0.001'
     -> {'SOL': {'category': 'OnChain', 'buffer': 0.2}, ...}

    A bad buffer falls back to 0.0 instead of raising — this parser runs inside
    balance lookups, so an exception here would crash the run.
    """
    config = {}
    for entry in (raw or '').split(','):
        parts = [p.strip() for p in entry.strip().split(':')]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            continue
        try:
            buffer = float(parts[2]) if len(parts) > 2 and parts[2] else 0.0
        except ValueError:
            buffer = 0.0
        config[parts[0].upper()] = {'category': parts[1], 'buffer': buffer}
    return config


def normalize_symbol(symbol, quote_coins=('USDT', 'USDC', 'USD')):
    """
    'ETHUSDC' -> ('ETH', 'USDC'). Returns (symbol, None) if no quote matches.

    Must strip a SUFFIX, not replace a substring: str.replace turns 'USDCUSDT'
    into 'USDT' and leaves 'ETHUSDT' untouched when the configured stablecoin is
    USDC — which silently drops that half of the history out of PnL.
    """
    if not symbol:
        return (symbol, None)
    for quote in sorted(quote_coins, key=len, reverse=True):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return (symbol[: -len(quote)], quote)
    return (symbol, None)
