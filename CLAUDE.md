# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Bot

```bash
# Run DCA — SIMULATION by default (DRY_RUN=true): places NO real orders
python bybit_bot.py

# Run DCA for real (either set DRY_RUN=false in .env, or force it for one run):
python bybit_bot.py --live

# Send weekly PnL report via Telegram (no trades executed)
python bybit_bot.py --report weekly

# Send monthly PnL report
python bybit_bot.py --report monthly
```

`DRY_RUN` defaults to **true** — the bot simulates and places no real orders until
you explicitly opt into live trading (`DRY_RUN=false` or `--live`). Set `TESTNET=true`
to point at Bybit testnet. A run-lock (`dca_bot.lock`) plus an "already traded today"
check (Supabase `trade_log`, UTC) prevent a double-firing scheduler from buying twice.

```bash
# Run the tests (pure logic, no network, no credentials)
pytest -q
```

The entry point is `bybit_bot.py`. There is no CI or linting config.

## Architecture

Two flat modules, no package:

- `bybit_bot.py` (~1100 lines) — orchestration, all I/O, entry point.
- `dca_core.py` — pure decision logic (sizing, caps, signals, parsers). Imports **stdlib only**, deliberately: the import graph makes it impossible for a sizing function to reach a live `session` or a config global, so it is testable with no mocks.
- `test_dca_core.py` — 52 tests, no network, sub-second.

Artifacts written to `log/`: dated run logs, `runs.jsonl` (one JSON line per run with the full decision trace — prices, signals, multipliers, planned vs actual), `last_run.json` (local idempotency record), `dryrun_trades.jsonl` (simulated fills, never sent to Supabase).

**Execution flow inside `run_dca_bot()`:**
1. `ensure_stablecoin_balance()` — redeems from Flexible Saving if spot wallet is low
2. **Step 1:** Pre-compute `desired_buys[coin]` for every coin:
   - `is_flash_crash()` — hourly kline circuit breaker
   - Max price guard via `MAX_PRICE_STRING`
   - `get_market_boost()` — Fear & Greed + Bollinger Bands multiplier
   - `get_buy_rebalance_multipliers()` — buy-side boost for underweight coins
3. **Step 2:** Cap at `MAX_SPEND_PER_RUN`, then scale to the available balance (prevents dict-order starvation)
4. **Step 3:** Execute each buy via `execute_buy()` → `try_spot_limit_order()` (optional) → `convert_coins()` fallback. Decrement `remaining_bal` only after confirmed fill. Then `stake_idle_coin()`.
5. **Step 4:** `sweep_stablecoin_surplus()` — parks idle stablecoin in Flexible Saving
6. **Step 5:** `calculate_PnL()` — reads trade_log table from Supabase, sends Telegram report

**Trade execution path:**
- Default: Bybit Convert API (`request_a_quote` → `confirm_a_quote` → `get_convert_history` for fill verification)
- Optional (USE_SPOT_ORDERS=true): Post-Only spot limit order at bid price; polls until filled or `SPOT_ORDER_TIMEOUT` seconds; falls back to Convert API on timeout

**Staking config pattern** (`STAKEABLE_COINS`):
- Format: `COIN:Category:liquid_buffer_qty`
- Example: `SOL:OnChain:0.2,ETH:FlexibleSaving:0.001`
- `stake_idle_coin()` stakes `(total_balance - buffer)` after each buy

**Market signals** (enabled via USE_MARKET_BOOST=true):
- Fear & Greed from `api.alternative.me/fng` — extreme fear adds +0.75x, extreme greed subtracts -0.20x
- Bollinger Bands (20-day daily closes, 2σ) — below lower band adds +0.50x, above upper subtracts -0.15x
- Total boost clamped to `[0.3, DCA_BOOST_MULTIPLIER]`

**Supabase:** Used for trade logging (`trade_log` table) and PnL history queries. If credentials are absent, logging is skipped but buys still execute.

## Environment Variables

Required:
- `API_KEY`, `API_SECRET` — Bybit Unified Trading account
- `DAILY_USD` — total USD to spend per run
- `CRYPTO_ALLOCATION_STRING` — e.g. `ETH:0.5,SOL:0.4,TON:0.1` (weights, not percentages)
- `STABLECOIN` — e.g. `USDT` or `USDC`; first value used as quote coin

Optional — Telegram:
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

Optional — Supabase:
- `TABLE_URL`, `TABLE_PASSWORD`

Optional — safety / live-readiness controls:
- `DRY_RUN=true` — simulate, place no real orders (DEFAULT). Set `false` (or pass `--live`) to trade.
- `TESTNET=false` — connect to Bybit testnet when `true`
- `MAX_SPEND_PER_RUN` — absolute USD ceiling per run (default `DAILY_USD × 3`). Set explicitly if allocation weights don't sum to ~1.
- `MAX_COMBINED_MULTIPLIER=3.0` — cap on the product of VA × market-boost × rebalance multipliers
- `MIN_CONVERT_USD=1.0` — minimum residual routed through Convert after a partial spot fill
(The run-lock is an OS-level advisory lock on `dca_bot.lock`; the OS releases it on any exit, so there is no staleness timeout to tune.)

Optional — feature flags (all have safe defaults):
- `USE_MARKET_BOOST=true` — Fear & Greed + Bollinger boost
- `DCA_BOOST_MULTIPLIER=2.0` — cap on market boost multiplier
- `REBALANCE_THRESHOLD=5` — drift % that triggers rebalance
- `REBALANCE_MULTIPLIER=1.5` — buy boost for underweight coins
- `USE_SPOT_ORDERS=false` — try Post-Only spot limit before Convert
- `SPOT_ORDER_TIMEOUT=180` — seconds to wait for spot fill
- `FLASH_CRASH_PCT=25` — % drop in window that skips a coin
- `FLASH_CRASH_HOURS=1` — window size for flash crash check
- `USDT_BUFFER_DAYS=3` — days of spend to keep liquid in spot wallet
- `STAKEABLE_COINS=SOL:OnChain:0.2,ETH:FlexibleSaving:0.001`
- `MAX_PRICE_STRING` — e.g. `BTC:100000,ETH:5000` skips buys above price
- `PNL_FROM_DATE` — ISO date to start PnL calculation from

## Key Invariants

- **`None` means "could not read", never "zero".** `get_coin_balance()`, `get_staked_balance()`, `get_total_coin_holdings()`, `ensure_stablecoin_balance()` and the values in `get_portfolio_weights()` all return `None` on API error. Never write `or 0.0` on these: a read failure that reads as zero makes an owned coin look unbought (→ over-buy) or an funded wallet look empty (→ pointless Earn redemption). `get_buy_rebalance_multipliers()` applies no rebalancing at all if any coin is unreadable.
- **Sizing must use `get_total_coin_holdings()` (spot + staked), not `get_coin_balance()`** — `stake_idle_coin()` parks bought coins in Earn, so portfolio weights would otherwise treat owned-but-staked coins as unbought and over-buy them every run.
- **DRY_RUN must gate every mutating call**, including `log_trade()` — a simulated fill written to the production ledger corrupts PnL and makes the idempotency gate skip the real run. Dry-run trades go to `log/dryrun_trades.jsonl`.
- **The idempotency gate fails closed.** `coins_bought_today()` returns `None` when it cannot establish the answer, and the caller skips the run. It is per-coin, so a run that bought ETH and then crashed still buys SOL and TON. `record_local_buy()` is called *before* logging/staking so a crash cannot cause a re-buy.
- **All paths are absolute** (`BASE_DIR`, `LOG_DIR`): a scheduled run's CWD is not the script directory.
- Market data resolves through `resolve_symbol()` (configured stablecoin pair, else USDT). Hardcoding `f"{coin}{USD_TYPE}"` for klines silently disables signals for coins without that pair. Spot *orders* still use `{coin}{USD_TYPE}` — that is the coin being spent.
- `remaining_bal` is decremented using `float(order[2])` (actual USD spent), never the planned amount. `execute_buy()` aggregates a partial spot fill + Convert residual so it never double-spends, and always returns a 4-tuple of strings.
- Stablecoin is redeemed **after** multipliers are known, or the boosts would be unfundable and therefore inert.
- All staking calls use `uuid.uuid4().hex[:12]` orderLinkIds; spot buy orders use a deterministic per-day `dca-{coin}-{YYYYMMDD}` orderLinkId (dedup safety net).
- Timestamps are `datetime.now(timezone.utc)` throughout.
- Supabase `trade_log` table columns: `trade_id`, `timestamp`, `symbol`, `quantity`, `price`, `total_usd`
