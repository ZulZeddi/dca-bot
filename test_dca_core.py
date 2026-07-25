"""
Tests for the money-critical decision logic.

Ordered by safety-per-effort: spend caps and fill planning first (every dollar
passes through them), then the None-vs-zero rules that caused real over-buys,
then parsers, then signal maths.

Each test names the failure it prevents, so a future edit that reintroduces the
bug fails with an explanation rather than a bare assertion.

Run: pytest -q
"""

import pytest

import dca_core as core


# ─── Tier 1: money paths ───────────────────────────────────────────────────────

class TestSpendCaps:
    def test_scales_proportionally_to_max_spend(self):
        plan = {'ETH': 22.5, 'SOL': 18.0, 'TON': 4.5}  # $45
        scaled, reason = core.apply_spend_caps(plan, max_spend=30.0, available=100.0)
        assert sum(scaled.values()) == pytest.approx(30.0)
        assert reason == 'max_spend'
        # Ratios preserved — no coin starved by dict order.
        assert scaled['ETH'] / scaled['SOL'] == pytest.approx(22.5 / 18.0)

    def test_available_balance_binds_below_max_spend(self):
        plan = {'ETH': 5.0, 'SOL': 5.0}
        scaled, reason = core.apply_spend_caps(plan, max_spend=30.0, available=7.0)
        assert sum(scaled.values()) == pytest.approx(7.0)
        assert reason == 'available'

    def test_no_scaling_when_plan_fits(self):
        plan = {'ETH': 5.0, 'SOL': 4.0}
        scaled, reason = core.apply_spend_caps(plan, max_spend=30.0, available=100.0)
        assert scaled == plan
        assert reason is None

    def test_zero_total_does_not_divide_by_zero(self):
        scaled, reason = core.apply_spend_caps({'ETH': 0.0}, max_spend=30.0, available=10.0)
        assert scaled == {'ETH': 0.0}
        assert reason is None

    def test_zero_available_yields_zero_plan(self):
        scaled, _ = core.apply_spend_caps({'ETH': 5.0}, max_spend=30.0, available=0.0)
        assert sum(scaled.values()) == pytest.approx(0.0)
        assert all(v >= 0 for v in scaled.values())


class TestCombineMultipliers:
    def test_clamps_runaway_stack(self):
        # 3.0 x 2.0 x 1.5 = 9x before the clamp.
        assert core.combine_multipliers(10.0, 3.0, 2.0, 1.5, max_combined=3.0) == pytest.approx(30.0)

    def test_current_config_maxes_out_exactly_at_the_cap(self):
        # Documents an executable fact: with boost<=2.0 and rebalance 1.5 the
        # product is exactly 3.0, so MAX_COMBINED_MULTIPLIER=3.0 never binds.
        # If this fails, someone changed a cap and the clamp now has an effect.
        assert core.combine_multipliers(10.0, 2.0, 1.5, max_combined=3.0) == pytest.approx(30.0)

    def test_below_cap_passes_through(self):
        assert core.combine_multipliers(10.0, 1.35, 1.0, max_combined=3.0) == pytest.approx(13.5)

    def test_zero_base_stays_zero(self):
        assert core.combine_multipliers(0.0, 2.0, max_combined=3.0) == 0.0


class TestPartialFill:
    def test_converts_only_the_residual(self):
        # The double-spend regression: filling $6 of $10 must convert $4, not $10.
        plan = core.plan_partial_fill(10.0, spot_usd=6.0, spot_qty=0.002, min_convert_usd=1.0)
        assert plan.convert_usd == pytest.approx(4.0)
        assert plan.spot_usd + plan.convert_usd == pytest.approx(10.0)

    def test_near_complete_fill_is_not_topped_up(self):
        plan = core.plan_partial_fill(10.0, spot_usd=9.95, spot_qty=0.003, min_convert_usd=1.0)
        assert plan.convert_usd == 0.0
        assert plan.spot_qty == pytest.approx(0.003)

    def test_zero_amount_never_returns_none(self):
        # The TypeError regression: a zero-sized buy must still be a valid plan.
        plan = core.plan_partial_fill(0.0, spot_usd=0.0, spot_qty=0.0, min_convert_usd=1.0)
        assert plan == core.FillPlan(0.0, 0.0, 0.0)

    def test_residual_below_convert_minimum_keeps_partial_without_rebuying(self):
        plan = core.plan_partial_fill(10.0, spot_usd=9.5, spot_qty=0.003, min_convert_usd=1.0)
        assert plan.convert_usd == 0.0
        assert plan.spot_usd == pytest.approx(9.5)

    def test_unfilled_order_converts_full_amount(self):
        plan = core.plan_partial_fill(10.0, spot_usd=0.0, spot_qty=0.0, min_convert_usd=1.0)
        assert plan.convert_usd == pytest.approx(10.0)


def test_aggregate_fill_sums_both_legs():
    usd, qty = core.aggregate_fill(6.0, 0.002, 4.0, 0.0013)
    assert usd == pytest.approx(10.0)
    assert qty == pytest.approx(0.0033)


@pytest.mark.parametrize('usd,qty', [(10.0, 0.0), (0.0, 1.0), (10.0, None), (None, 1.0)])
def test_implied_price_returns_none_instead_of_dividing_by_zero(usd, qty):
    assert core.implied_price(usd, qty) is None


def test_implied_price_normal_case():
    assert core.implied_price(10.0, 0.004) == pytest.approx(2500.0)


# ─── Tier 2: None means unknown, never zero ────────────────────────────────────

class TestTotalHoldings:
    def test_unknown_spot_propagates(self):
        assert core.total_holdings(None, 1.0) is None

    def test_unknown_staked_propagates(self):
        # The over-buy regression: a failed Earn read must not read as "owns
        # nothing", because nearly the whole position lives in Earn.
        assert core.total_holdings(1.0, None) is None

    def test_genuine_zero_is_distinct_from_unknown(self):
        assert core.total_holdings(0.0, 0.0) == 0.0

    def test_sums_both(self):
        assert core.total_holdings(0.0047, 8.9) == pytest.approx(8.9047)


class TestRebalanceMultipliers:
    ALLOC = {'ETH': 0.5, 'SOL': 0.4, 'TON': 0.1}

    def test_incomplete_portfolio_disables_rebalancing(self):
        # A read error must not look like "owns none" (spurious 1.5x boost) and
        # must not inflate the other coins' weights.
        portfolio = {'ETH': 100.0, 'SOL': None, 'TON': 10.0}
        assert core.rebalance_multipliers(self.ALLOC, portfolio) == {'ETH': 1.0, 'SOL': 1.0, 'TON': 1.0}

    def test_boosts_a_genuinely_underweight_coin(self):
        # SOL target 40%, actual ~4.8% -> drift far above the 5pp threshold.
        portfolio = {'ETH': 100.0, 'SOL': 5.0, 'TON': 0.0}
        result = core.rebalance_multipliers(self.ALLOC, portfolio, threshold_pct=5.0, boost=1.5)
        assert result['SOL'] == 1.5
        assert result['ETH'] == 1.0

    def test_zero_total_value_is_neutral(self):
        portfolio = {'ETH': 0.0, 'SOL': 0.0, 'TON': 0.0}
        assert core.rebalance_multipliers(self.ALLOC, portfolio) == {'ETH': 1.0, 'SOL': 1.0, 'TON': 1.0}


# ─── Tier 3: parsers ───────────────────────────────────────────────────────────

class TestParsers:
    def test_allocation_happy_path(self):
        assert core.parse_allocation('ETH:0.5,SOL:0.4,TON:0.1') == {'ETH': 0.5, 'SOL': 0.4, 'TON': 0.1}

    def test_allocation_skips_bad_entry_without_raising(self):
        # A typo in one coin must not abort the run before any buy happens.
        assert core.parse_allocation('ETH:abc,SOL:0.4') == {'SOL': 0.4}

    @pytest.mark.parametrize('raw', ['', 'ETH', None])
    def test_allocation_empty_inputs(self, raw):
        assert core.parse_allocation(raw) == {}

    def test_allocation_drops_non_positive_weights(self):
        assert core.parse_allocation('ETH:-1,SOL:0.4') == {'SOL': 0.4}

    def test_max_prices_skips_bad_entry(self):
        assert core.parse_max_prices('BTC:100000,ETH:bad') == {'BTC': 100000.0}

    def test_stake_config_defaults_missing_buffer(self):
        assert core.parse_stake_config('SOL:OnChain') == {'SOL': {'category': 'OnChain', 'buffer': 0.0}}

    def test_stake_config_bad_buffer_does_not_raise(self):
        # This parser runs inside balance lookups; raising here crashes the run.
        cfg = core.parse_stake_config('SOL:OnChain:o.2')
        assert cfg['SOL']['buffer'] == 0.0


class TestNormalizeSymbol:
    @pytest.mark.parametrize('symbol', ['ETHUSDT', 'ETHUSDC'])
    def test_both_quote_currencies_map_to_one_base(self, symbol):
        # Fixes the split history: ETHUSDT and ETHUSDC are the same asset, and
        # grouping them separately halved the invested capital in PnL.
        assert core.normalize_symbol(symbol)[0] == 'ETH'

    def test_strips_suffix_not_substring(self):
        # str.replace('USDC') would turn this into 'USDT'.
        assert core.normalize_symbol('USDCUSDT') == ('USDC', 'USDT')

    def test_unknown_quote_left_alone(self):
        assert core.normalize_symbol('ETHBTC') == ('ETHBTC', None)


# ─── Tier 4: signal maths ──────────────────────────────────────────────────────

class TestBollinger:
    def test_zero_variance_does_not_blow_up(self):
        assert core.bollinger_from_closes([100.0] * 20, 100.0, period=20) == 0.0

    def test_insufficient_history_is_never_a_buy_signal(self):
        # A new listing must not be mistaken for a dip.
        assert core.bollinger_from_closes([100.0] * 19, 50.0, period=20) == 0.0

    def test_below_lower_band(self):
        closes = [100.0 + (i % 2) for i in range(20)]  # small variance around ~100
        assert core.bollinger_from_closes(closes, 50.0, period=20) == 1.0

    def test_above_upper_band(self):
        closes = [100.0 + (i % 2) for i in range(20)]
        assert core.bollinger_from_closes(closes, 200.0, period=20) == -1.0

    def test_uses_only_the_last_period_closes(self):
        # 25 closes, period 20: the ancient outlier must not widen the bands.
        closes = [1000.0] * 5 + [100.0] * 20
        assert core.bollinger_from_closes(closes, 100.0, period=20) == 0.0


class TestDrawdown:
    def test_v_shaped_crash_is_detected(self):
        # open 100, low 70, recovered to 99. Comparing oldest-open to last price
        # sees -1% and misses it entirely; peak-to-current sees the real move.
        candles = [[0, 100.0, 100.0, 70.0, 99.0]]
        assert core.drawdown_from_high(candles, 99.0) == pytest.approx(1.0)
        # ...and the drawdown from the peak is what matters when still down:
        assert core.drawdown_from_high(candles, 70.0) == pytest.approx(30.0)

    def test_uses_highest_high_across_the_window(self):
        candles = [[0, 90.0, 95.0, 88.0, 92.0], [0, 100.0, 120.0, 99.0, 100.0]]
        assert core.drawdown_from_high(candles, 60.0) == pytest.approx(50.0)

    def test_empty_candles_is_no_crash(self):
        assert core.drawdown_from_high([], 100.0) == 0.0


class TestMarketBoost:
    def test_extreme_fear_plus_dip_is_clamped(self):
        # 1.0 + 0.75 + 0.50 = 2.25, clamped to max_boost.
        assert core.market_boost_from_signals(10, 1.0, max_boost=2.0) == pytest.approx(2.0)

    def test_neutral_is_one(self):
        assert core.market_boost_from_signals(50, 0.0) == pytest.approx(1.0)

    def test_reachable_minimum_is_065_so_the_03_floor_is_dead_code(self):
        # Documents that the configured floor of 0.3 can never be reached: the
        # most bearish combination is 1.0 - 0.20 - 0.15 = 0.65.
        worst = min(
            core.market_boost_from_signals(fng, bb)
            for fng in range(0, 101)
            for bb in (-1.0, 0.0, 1.0)
        )
        assert worst == pytest.approx(0.65)

    def test_buy_side_outweighs_sell_side(self):
        # The structural long bias: average spend exceeds the nominal budget.
        fear = core.market_boost_from_signals(10, 1.0, max_boost=99.0)
        greed = core.market_boost_from_signals(90, -1.0, max_boost=99.0)
        assert (fear - 1.0) > (1.0 - greed)
