from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

try:
    from research.wallet_intelligence import WalletIntelligenceEngine
except Exception:
    WalletIntelligenceEngine = None  # type: ignore[assignment]

try:
    from research.creator_entity_intelligence import CreatorEntityIntelligenceEngine
except Exception:
    CreatorEntityIntelligenceEngine = None  # type: ignore[assignment]


_WALLET_INTELLIGENCE_ENGINE: WalletIntelligenceEngine | None | bool = None
_CREATOR_ENTITY_INTELLIGENCE_ENGINE: CreatorEntityIntelligenceEngine | None | bool = None


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    if abs(d) < 1e-9:
        return default
    return n / d


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _seconds_since(value: str | None) -> float:
    dt = _parse_dt(value)
    if dt is None:
        return 0.0
    return max((datetime.now(timezone.utc) - dt).total_seconds(), 0.0)


def _get_wallet_intelligence_engine() -> WalletIntelligenceEngine | None:
    global _WALLET_INTELLIGENCE_ENGINE

    if _WALLET_INTELLIGENCE_ENGINE is False:
        return None

    if _WALLET_INTELLIGENCE_ENGINE is None:
        if WalletIntelligenceEngine is None:
            _WALLET_INTELLIGENCE_ENGINE = False
            return None
        try:
            _WALLET_INTELLIGENCE_ENGINE = WalletIntelligenceEngine()
        except Exception:
            _WALLET_INTELLIGENCE_ENGINE = False
            return None

    if _WALLET_INTELLIGENCE_ENGINE is False:
        return None
    return _WALLET_INTELLIGENCE_ENGINE


def _get_creator_entity_intelligence_engine() -> CreatorEntityIntelligenceEngine | None:
    global _CREATOR_ENTITY_INTELLIGENCE_ENGINE

    if _CREATOR_ENTITY_INTELLIGENCE_ENGINE is False:
        return None

    if _CREATOR_ENTITY_INTELLIGENCE_ENGINE is None:
        if CreatorEntityIntelligenceEngine is None:
            _CREATOR_ENTITY_INTELLIGENCE_ENGINE = False
            return None
        try:
            _CREATOR_ENTITY_INTELLIGENCE_ENGINE = CreatorEntityIntelligenceEngine()
        except Exception:
            _CREATOR_ENTITY_INTELLIGENCE_ENGINE = False
            return None

    if _CREATOR_ENTITY_INTELLIGENCE_ENGINE is False:
        return None
    return _CREATOR_ENTITY_INTELLIGENCE_ENGINE


def _select_wallets_for_memory(state: dict[str, Any], max_wallets: int = 8) -> list[str]:
    selected_wallets = list(state.get("helius_selected_wallets") or [])
    ordered: list[str] = []

    for wallet in selected_wallets:
        wallet = str(wallet or "").strip()
        if wallet and wallet not in ordered:
            ordered.append(wallet)
        if len(ordered) >= max_wallets:
            return ordered[:max_wallets]

    recent_trades = list(state.get("recent_trades") or [])
    buy_sizes: dict[str, float] = {}
    first_buy_order: list[str] = []
    first_seen: set[str] = set()

    for trade in recent_trades:
        wallet = str(trade.get("trader_wallet") or "").strip()
        if not wallet:
            continue
        if str(trade.get("tx_type") or "").lower() != "buy":
            continue

        amount = float(
            trade.get("effective_sol_amount", trade.get("sol_amount", 0.0)) or 0.0
        )
        buy_sizes[wallet] = buy_sizes.get(wallet, 0.0) + amount

        if wallet not in first_seen:
            first_buy_order.append(wallet)
            first_seen.add(wallet)

    ranked_by_size = [
        wallet for wallet, _ in sorted(
            buy_sizes.items(),
            key=lambda item: item[1],
            reverse=True,
        )
    ]

    for group in (
        first_buy_order[:4],
        ranked_by_size[:4],
        first_buy_order[4:8],
    ):
        for wallet in group:
            if wallet and wallet not in ordered:
                ordered.append(wallet)
            if len(ordered) >= max_wallets:
                return ordered[:max_wallets]

    return ordered[:max_wallets]


def _wallet_memory_features(state: dict[str, Any]) -> dict[str, float]:
    engine = _get_wallet_intelligence_engine()
    participant_wallets = _select_wallets_for_memory(state, max_wallets=8)

    if engine is None or not participant_wallets:
        return {
            "historical_cohort_quality_score": 0.5,
            "historical_cohort_confidence": 0.0,
            "historical_good_wallet_share": 0.0,
            "historical_bad_wallet_share": 0.0,
            "historical_known_wallet_share": 0.0,
            "historical_wallets_seen_count": 0.0,
            "historical_avg_wallet_quality": 0.5,
        }

    quality_scores: list[float] = []
    known_wallet_count = 0
    total_tokens_seen = 0
    good_wallet_count = 0
    bad_wallet_count = 0

    for wallet in participant_wallets:
        try:
            quality = float(engine.wallet_quality(wallet))
        except Exception:
            quality = 0.5

        quality_scores.append(quality)

        tokens_seen = 0
        try:
            record = engine.registry.get_wallet(wallet)
            tokens_seen = int(getattr(record, "tokens_seen", 0) or 0)
        except Exception:
            tokens_seen = 0

        if tokens_seen > 0:
            known_wallet_count += 1
            total_tokens_seen += tokens_seen

        if quality >= 0.65:
            good_wallet_count += 1
        elif quality <= 0.35:
            bad_wallet_count += 1

    avg_wallet_quality = (
        sum(quality_scores) / len(quality_scores) if quality_scores else 0.5
    )

    # Sample-size shrinkage: keep wallet-memory near neutral until there is enough evidence.
    known_wallet_share = _safe_div(known_wallet_count, len(participant_wallets), 0.0)
    observation_confidence = _clamp(
        _safe_div(total_tokens_seen, max(len(participant_wallets) * 3.0, 1.0), 0.0)
    )
    historical_cohort_confidence = _clamp(
        known_wallet_share * 0.55 + observation_confidence * 0.45
    )
    shrunk_cohort_quality = 0.5 + (
        (avg_wallet_quality - 0.5) * historical_cohort_confidence
    )

    return {
        "historical_cohort_quality_score": round(shrunk_cohort_quality, 4),
        "historical_cohort_confidence": round(historical_cohort_confidence, 4),
        "historical_good_wallet_share": round(
            _safe_div(good_wallet_count, len(participant_wallets), 0.0),
            4,
        ),
        "historical_bad_wallet_share": round(
            _safe_div(bad_wallet_count, len(participant_wallets), 0.0),
            4,
        ),
        "historical_known_wallet_share": round(known_wallet_share, 4),
        "historical_wallets_seen_count": float(total_tokens_seen),
        "historical_avg_wallet_quality": round(avg_wallet_quality, 4),
    }


def _creator_entity_features(state: dict[str, Any]) -> dict[str, float]:
    engine = _get_creator_entity_intelligence_engine()
    creator_profile = state.get("helius_creator_profile", {}) or {}

    creator_wallet = str(state.get("creator_wallet") or "").strip() or None
    first_hop_funder = str(
        state.get("wallet_memory_funder_wallet")
        or creator_profile.get("top_funder")
        or ""
    ).strip() or None

    if engine is None:
        return {
            "creator_entity_quality_score": 0.5,
            "creator_entity_confidence_score": 0.0,
            "creator_entity_launch_count": 0.0,
            "creator_entity_paper_trade_count": 0.0,
            "creator_entity_is_known": 0.0,
            "creator_entity_creator_wallet_count": 0.0,
            "creator_entity_funder_wallet_count": 0.0,
            "creator_first_hop_funder_seen_before": 0.0,
            "creator_wallet_seen_before": 0.0,
            "creator_new_wallet_but_known_entity_flag": 0.0,
            "creator_funder_cluster_win_rate": 0.0,
            "creator_funder_cluster_invalidation_rate": 0.0,
            "creator_exchange_touch_recurrence_score": 0.0,
        }

    try:
        features = engine.entity_features(
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
        )
    except Exception:
        features = {}

    return {
        "creator_entity_quality_score": round(
            float(features.get("creator_entity_quality_score", 0.5) or 0.5),
            4,
        ),
        "creator_entity_confidence_score": round(
            float(features.get("creator_entity_confidence_score", 0.0) or 0.0),
            4,
        ),
        "creator_entity_launch_count": float(
            features.get("creator_entity_launch_count", 0) or 0
        ),
        "creator_entity_paper_trade_count": float(
            features.get("creator_entity_paper_trade_count", 0) or 0
        ),
        "creator_entity_is_known": 1.0
        if bool(features.get("creator_entity_is_known"))
        else 0.0,
        "creator_entity_creator_wallet_count": float(
            features.get("creator_entity_creator_wallet_count", 0) or 0
        ),
        "creator_entity_funder_wallet_count": float(
            features.get("creator_entity_funder_wallet_count", 0) or 0
        ),
        "creator_first_hop_funder_seen_before": 1.0
        if bool(features.get("creator_first_hop_funder_seen_before"))
        else 0.0,
        "creator_wallet_seen_before": 1.0
        if bool(features.get("creator_wallet_seen_before"))
        else 0.0,
        "creator_new_wallet_but_known_entity_flag": 1.0
        if bool(features.get("creator_new_wallet_but_known_entity_flag"))
        else 0.0,
        "creator_funder_cluster_win_rate": round(
            float(features.get("creator_funder_cluster_win_rate", 0.0) or 0.0),
            4,
        ),
        "creator_funder_cluster_invalidation_rate": round(
            float(features.get("creator_funder_cluster_invalidation_rate", 0.0) or 0.0),
            4,
        ),
        "creator_exchange_touch_recurrence_score": round(
            float(features.get("creator_exchange_touch_recurrence_score", 0.0) or 0.0),
            4,
        ),
    }


DUST_TRADE_SOL_THRESHOLD = 0.001


def build_quant_features(
    state: dict[str, Any],
    *,
    include_helius: bool = True,
) -> dict[str, float]:
    current_mcap = float(state.get("current_market_cap_sol", 0.0) or 0.0)
    peak_mcap = float(state.get("peak_market_cap_sol", 0.0) or 0.0)
    trough_mcap = float(state.get("trough_market_cap_sol", 0.0) or 0.0)

    trades_1m = int(state.get("trades_last_1m", 0) or 0)
    trades_5m = int(state.get("trades_last_5m", 0) or 0)
    buys_1m = int(state.get("buys_last_1m", 0) or 0)
    buys_5m = int(state.get("buys_last_5m", 0) or 0)
    sells_1m = int(state.get("sells_last_1m", 0) or 0)
    sells_5m = int(state.get("sells_last_5m", 0) or 0)

    buy_sol_1m = float(state.get("buy_sol_last_1m", 0.0) or 0.0)
    buy_sol_5m = float(state.get("buy_sol_last_5m", 0.0) or 0.0)
    sell_sol_1m = float(state.get("sell_sol_last_1m", 0.0) or 0.0)
    sell_sol_5m = float(state.get("sell_sol_last_5m", 0.0) or 0.0)
    net_1m = float(state.get("net_sol_flow_last_1m", 0.0) or 0.0)
    net_5m = float(state.get("net_sol_flow_last_5m", 0.0) or 0.0)

    unique_buyers = int(state.get("unique_buyers", 0) or 0)
    unique_traders = int(state.get("unique_traders", 0) or 0)
    unique_sellers = int(state.get("unique_sellers", 0) or 0)

    unique_buyers_1m = int(state.get("unique_buyers_last_1m", unique_buyers) or 0)
    unique_buyers_5m = int(state.get("unique_buyers_last_5m", unique_buyers) or 0)
    unique_traders_1m = int(state.get("unique_traders_last_1m", unique_traders) or 0)
    unique_traders_5m = int(state.get("unique_traders_last_5m", unique_traders) or 0)
    unique_sellers_1m = int(state.get("unique_sellers_last_1m", unique_sellers) or 0)
    unique_sellers_5m = int(state.get("unique_sellers_last_5m", unique_sellers) or 0)

    total_sol_5m = buy_sol_5m + sell_sol_5m
    total_sol_1m = buy_sol_1m + sell_sol_1m

    wallet_novelty_score = float(state.get("wallet_novelty_score", 0.0) or 0.0)
    repeat_wallet_ratio = float(state.get("repeat_wallet_ratio", 0.0) or 0.0)
    repeat_buyer_ratio = float(state.get("repeat_buyer_ratio", 0.0) or 0.0)
    buyer_overlap_ratio = float(state.get("buyer_overlap_ratio", 0.0) or 0.0)
    participant_churn_ratio = float(state.get("participant_churn_ratio", 0.0) or 0.0)
    cluster_entropy = float(state.get("cluster_entropy", 0.0) or 0.0)
    participant_concentration_score = float(
        state.get("participant_concentration_score", 0.0) or 0.0
    )
    new_buyer_velocity = float(state.get("new_buyer_velocity", 0.0) or 0.0)
    participant_quality_score_v2 = float(
        state.get("participant_quality_score_v2", 0.0) or 0.0
    )
    revival_count = int(state.get("revival_count", 0) or 0)

    recent_trades = list(state.get("recent_trades") or [])
    dust_trades_1m = 0
    dust_trades_5m = 0
    dust_buys_1m = 0
    dust_buys_5m = 0
    non_dust_trades_1m = 0
    non_dust_trades_5m = 0
    non_dust_buys_1m = 0
    non_dust_buys_5m = 0
    for trade in recent_trades:
        age_seconds = _seconds_since(trade.get("captured_at_utc"))
        is_dust_trade = bool(trade.get("is_dust_trade")) or float(
            trade.get("sol_amount", 0.0) or 0.0
        ) < DUST_TRADE_SOL_THRESHOLD
        is_buy = str(trade.get("tx_type") or "").lower() == "buy"
        if age_seconds <= 300:
            if is_dust_trade:
                dust_trades_5m += 1
                if is_buy:
                    dust_buys_5m += 1
            else:
                non_dust_trades_5m += 1
                if is_buy:
                    non_dust_buys_5m += 1
        if age_seconds <= 60:
            if is_dust_trade:
                dust_trades_1m += 1
                if is_buy:
                    dust_buys_1m += 1
            else:
                non_dust_trades_1m += 1
                if is_buy:
                    non_dust_buys_1m += 1

    helius_summary = state.get("helius_wallet_cohort_summary", {}) or {}
    helius_creator = state.get("helius_creator_profile", {}) or {}
    if not include_helius:
        helius_summary = {}
        helius_creator = {}

    helius_profile_count = int(helius_summary.get("profile_count", 0) or 0)
    helius_avg_wallet_age_days = float(
        helius_summary.get("avg_wallet_age_days", 0.0) or 0.0
    )
    helius_median_wallet_age_days = float(
        helius_summary.get("median_wallet_age_days", 0.0) or 0.0
    )
    helius_fresh_wallet_share = float(
        helius_summary.get("fresh_wallet_share", 0.0) or 0.0
    )
    helius_sniper_wallet_share = float(
        helius_summary.get("sniper_wallet_share", 0.0) or 0.0
    )
    helius_recycled_wallet_share = float(
        helius_summary.get("recycled_wallet_share", 0.0) or 0.0
    )
    helius_high_velocity_wallet_share = float(
        helius_summary.get("high_velocity_wallet_share", 0.0) or 0.0
    )
    helius_funding_diversity_score = float(
        helius_summary.get("funding_diversity_score", 0.0) or 0.0
    )
    helius_top_funder_concentration_score = float(
        helius_summary.get("top_funder_concentration_score", 0.0) or 0.0
    )
    helius_creator_shared_funder_score = float(
        helius_summary.get("creator_shared_funder_score", 0.0) or 0.0
    )
    helius_creator_wallet_age_days = float(
        helius_summary.get(
            "creator_wallet_age_days",
            helius_creator.get("wallet_age_days", 0.0),
        )
        or 0.0
    )
    helius_creator_probable_fresh_wallet = (
        1.0
        if bool(
            helius_summary.get(
                "creator_probable_fresh_wallet",
                helius_creator.get("probable_fresh_wallet"),
            )
        )
        else 0.0
    )
    helius_creator_probable_sniper_wallet = (
        1.0
        if bool(
            helius_summary.get(
                "creator_probable_sniper_wallet",
                helius_creator.get("probable_sniper_wallet"),
            )
        )
        else 0.0
    )
    helius_creator_probable_recycled_wallet = (
        1.0
        if bool(
            helius_summary.get(
                "creator_probable_recycled_wallet",
                helius_creator.get("probable_recycled_wallet"),
            )
        )
        else 0.0
    )
    helius_cohort_quality_score = float(
        helius_summary.get("cohort_quality_score", 0.0) or 0.0
    )
    helius_profile_completion_confidence = float(
        helius_summary.get("profile_completion_confidence", 0.0) or 0.0
    )

    buyer_density_5m = _safe_div(unique_buyers_5m, buys_5m, 0.0)
    seller_density_5m = _safe_div(unique_sellers_5m, sells_5m, 0.0)
    trader_participation_ratio = _safe_div(unique_traders_5m, trades_5m, 0.0)
    buyer_participation_ratio = _safe_div(unique_buyers_5m, unique_traders_5m, 0.0)

    mcap_to_peak_ratio = (
        _safe_div(current_mcap, peak_mcap, 0.0) if peak_mcap > 0 else 0.0
    )
    peak_retrace_pct = (
        max(0.0, 1.0 - _safe_div(current_mcap, peak_mcap, 1.0)) * 100.0
        if peak_mcap > 0
        else 0.0
    )
    range_position_pct = (
        _safe_div(
            current_mcap - trough_mcap,
            max(peak_mcap - trough_mcap, 1e-9),
            0.0,
        )
        * 100.0
        if peak_mcap > trough_mcap
        else 0.0
    )
    recovery_ratio_from_trough = (
        _safe_div(
            current_mcap - trough_mcap,
            max(peak_mcap - trough_mcap, 1e-9),
            0.0,
        )
        if peak_mcap > trough_mcap
        else 0.0
    )
    peak_to_trough_damage_pct = (
        _safe_div(max(peak_mcap - trough_mcap, 0.0), peak_mcap, 0.0) * 100.0
        if peak_mcap > 0
        else 0.0
    )

    time_since_first_seen_seconds = _seconds_since(state.get("first_seen_at"))
    time_since_last_trade_seconds = _seconds_since(state.get("last_trade_at"))
    time_since_last_buy_seconds = _seconds_since(state.get("last_buy_at"))
    time_since_last_sell_seconds = _seconds_since(state.get("last_sell_at"))
    time_since_peak_seconds = (
        _seconds_since(state.get("peak_market_cap_at")) or time_since_last_trade_seconds
    )
    time_since_trough_seconds = (
        _seconds_since(state.get("trough_market_cap_at")) or time_since_last_trade_seconds
    )

    near_high_dwell_pct = 0.0
    if mcap_to_peak_ratio >= 0.90 and time_since_first_seen_seconds > 0:
        near_high_dwell_pct = (
            _clamp(
                time_since_peak_seconds / max(time_since_first_seen_seconds, 1.0),
                0.0,
                1.0,
            )
            * 100.0
        )

    seller_expansion_ratio = _safe_div(
        unique_sellers_1m * 5.0,
        max(unique_sellers_5m, 1),
        0.0,
    )
    buyer_refresh_ratio_1m_vs_5m = _safe_div(
        unique_buyers_1m,
        max(unique_buyers_5m, 1),
        0.0,
    )
    mcap_stability_score = _clamp(
        mcap_to_peak_ratio * 0.55
        + _clamp(1.0 - peak_retrace_pct / 35.0) * 0.25
        + _clamp(range_position_pct / 100.0) * 0.20
    )
    buy_pressure_5m = _safe_div(buy_sol_5m, total_sol_5m, 0.0)
    sell_absorption_score = _clamp(
        _clamp(
            _safe_div(buy_sol_1m, max(sell_sol_1m, 1e-9), 0.0),
            0.0,
            2.0,
        )
        / 2.0
        * 0.30
        + _clamp(buy_pressure_5m) * 0.30
        + _clamp(buyer_density_5m) * 0.20
        + _clamp(1.0 - max(seller_expansion_ratio - 0.8, 0.0), 0.0, 1.0) * 0.20
    )
    dust_trade_share_1m = _safe_div(
        dust_trades_1m,
        max(trades_1m + dust_trades_1m, 1),
        0.0,
    )
    dust_trade_share_5m = _safe_div(
        dust_trades_5m,
        max(trades_5m + dust_trades_5m, 1),
        0.0,
    )
    dust_buy_share_1m = _safe_div(
        dust_buys_1m,
        max(buys_1m + dust_buys_1m, 1),
        0.0,
    )
    dust_buy_share_5m = _safe_div(
        dust_buys_5m,
        max(buys_5m + dust_buys_5m, 1),
        0.0,
    )
    entry_confirmation_score = _clamp(
        _clamp(buy_pressure_5m) * 0.18
        + _clamp(buyer_density_5m) * 0.15
        + _clamp(participant_quality_score_v2) * 0.20
        + _clamp(mcap_stability_score) * 0.15
        + _clamp(sell_absorption_score) * 0.12
        + _clamp(recovery_ratio_from_trough) * 0.08
        + _clamp(1.0 - dust_trade_share_1m / 0.35) * 0.06
        + _clamp(1.0 - dust_trade_share_5m / 0.25) * 0.06
    )

    wallet_memory = _wallet_memory_features(state)
    creator_entity = _creator_entity_features(state)

    features = {
        "mcap_to_peak_ratio": round(mcap_to_peak_ratio, 4),
        "peak_retrace_pct": round(peak_retrace_pct, 4),
        "range_position_pct": round(range_position_pct, 4),
        "trade_acceleration_ratio": round(
            _safe_div(trades_1m * 5.0, trades_5m, 0.0),
            4,
        ),
        "buy_acceleration_ratio": round(
            _safe_div(buys_1m * 5.0, buys_5m, 0.0),
            4,
        ),
        "sell_acceleration_ratio": round(
            _safe_div(sells_1m * 5.0, sells_5m, 0.0),
            4,
        ),
        "buy_pressure_ratio_5m": round(buy_pressure_5m, 4),
        "buy_pressure_ratio_1m": round(
            _safe_div(buy_sol_1m, total_sol_1m, 0.0),
            4,
        ),
        "buy_sell_count_ratio_5m": round(
            _safe_div(buys_5m, max(sells_5m, 1), 0.0),
            4,
        ),
        "buy_sell_sol_ratio_5m": round(
            _safe_div(buy_sol_5m, max(sell_sol_5m, 1e-9), 0.0),
            4,
        ),
        "net_flow_per_trade_5m": round(_safe_div(net_5m, trades_5m, 0.0), 4),
        "net_flow_per_trade_1m": round(_safe_div(net_1m, trades_1m, 0.0), 4),
        "avg_buy_size_5m": round(_safe_div(buy_sol_5m, buys_5m, 0.0), 4),
        "avg_sell_size_5m": round(_safe_div(sell_sol_5m, sells_5m, 0.0), 4),
        "buyer_density_5m": round(buyer_density_5m, 4),
        "seller_density_5m": round(seller_density_5m, 4),
        "trader_participation_ratio": round(trader_participation_ratio, 4),
        "buyer_participation_ratio": round(buyer_participation_ratio, 4),
        "participation_quality_score": round(
            min(1.0, buyer_density_5m) * 0.6
            + min(1.0, trader_participation_ratio) * 0.4,
            4,
        ),
        "microburst_ratio": round(
            _safe_div(buys_1m, max(trades_1m, 1), 0.0),
            4,
        ),
        "flow_persistence_ratio": round(
            _safe_div(max(net_1m, 0.0) * 5.0, max(net_5m, 1e-9), 0.0)
            if net_5m > 0
            else 0.0,
            4,
        ),
        "wash_risk_ratio": round(
            _safe_div(trades_5m, max(unique_traders_5m, 1), 0.0),
            4,
        ),
        "breakout_strength": round(
            max(0.0, current_mcap - max(35.0, peak_mcap * 0.9)),
            4,
        ),
        "wallet_novelty_score": round(wallet_novelty_score, 4),
        "repeat_wallet_ratio": round(repeat_wallet_ratio, 4),
        "repeat_buyer_ratio": round(repeat_buyer_ratio, 4),
        "buyer_overlap_ratio": round(buyer_overlap_ratio, 4),
        "same_wallet_wave_overlap_score": round(buyer_overlap_ratio, 4),
        "participant_churn_ratio": round(participant_churn_ratio, 4),
        "cluster_entropy": round(cluster_entropy, 4),
        "participant_concentration_score": round(participant_concentration_score, 4),
        "new_buyer_velocity": round(new_buyer_velocity, 4),
        "fresh_wallet_expansion_velocity": round(new_buyer_velocity, 4),
        "participant_quality_score_v2": round(participant_quality_score_v2, 4),
        "time_since_first_seen_seconds": round(time_since_first_seen_seconds, 4),
        "time_since_last_trade_seconds": round(time_since_last_trade_seconds, 4),
        "time_since_last_buy_seconds": round(time_since_last_buy_seconds, 4),
        "time_since_last_sell_seconds": round(time_since_last_sell_seconds, 4),
        "time_since_peak_seconds": round(time_since_peak_seconds, 4),
        "time_since_trough_seconds": round(time_since_trough_seconds, 4),
        "mcap_recovery_from_trough_ratio": round(recovery_ratio_from_trough, 4),
        "recovery_ratio_from_trough": round(recovery_ratio_from_trough, 4),
        "peak_to_trough_damage_pct": round(peak_to_trough_damage_pct, 4),
        "near_high_dwell_pct": round(near_high_dwell_pct, 4),
        "revival_count": float(revival_count),
        "seller_expansion_ratio": round(seller_expansion_ratio, 4),
        "buyer_refresh_ratio_1m_vs_5m": round(buyer_refresh_ratio_1m_vs_5m, 4),
        "sell_absorption_score": round(sell_absorption_score, 4),
        "mcap_stability_score": round(mcap_stability_score, 4),
        "unique_buyers_last_1m": float(unique_buyers_1m),
        "unique_buyers_last_5m": float(unique_buyers_5m),
        "unique_traders_last_1m": float(unique_traders_1m),
        "unique_traders_last_5m": float(unique_traders_5m),
        "unique_sellers_last_1m": float(unique_sellers_1m),
        "unique_sellers_last_5m": float(unique_sellers_5m),
        "dust_trade_share_1m": round(dust_trade_share_1m, 4),
        "dust_trade_share_5m": round(dust_trade_share_5m, 4),
        "dust_buy_share_1m": round(dust_buy_share_1m, 4),
        "dust_buy_share_5m": round(dust_buy_share_5m, 4),
        "non_dust_trades_1m": float(non_dust_trades_1m),
        "non_dust_trades_5m": float(non_dust_trades_5m),
        "non_dust_buys_1m": float(non_dust_buys_1m),
        "non_dust_buys_5m": float(non_dust_buys_5m),
        "entry_confirmation_score": round(entry_confirmation_score, 4),
        "helius_profile_count": float(helius_profile_count),
        "helius_avg_wallet_age_days": round(helius_avg_wallet_age_days, 4),
        "helius_median_wallet_age_days": round(helius_median_wallet_age_days, 4),
        "helius_fresh_wallet_share": round(helius_fresh_wallet_share, 4),
        "helius_sniper_wallet_share": round(helius_sniper_wallet_share, 4),
        "helius_recycled_wallet_share": round(helius_recycled_wallet_share, 4),
        "helius_high_velocity_wallet_share": round(
            helius_high_velocity_wallet_share,
            4,
        ),
        "helius_funding_diversity_score": round(helius_funding_diversity_score, 4),
        "helius_top_funder_concentration_score": round(
            helius_top_funder_concentration_score,
            4,
        ),
        "helius_creator_shared_funder_score": round(
            helius_creator_shared_funder_score,
            4,
        ),
        "helius_creator_wallet_age_days": round(helius_creator_wallet_age_days, 4),
        "helius_creator_probable_fresh_wallet": round(
            helius_creator_probable_fresh_wallet,
            4,
        ),
        "helius_creator_probable_sniper_wallet": round(
            helius_creator_probable_sniper_wallet,
            4,
        ),
        "helius_creator_probable_recycled_wallet": round(
            helius_creator_probable_recycled_wallet,
            4,
        ),
        "helius_cohort_quality_score": round(helius_cohort_quality_score, 4),
        "helius_profile_completion_confidence": round(
            helius_profile_completion_confidence,
            4,
        ),
        "historical_cohort_quality_score": wallet_memory["historical_cohort_quality_score"],
        "historical_cohort_confidence": wallet_memory["historical_cohort_confidence"],
        "historical_good_wallet_share": wallet_memory["historical_good_wallet_share"],
        "historical_bad_wallet_share": wallet_memory["historical_bad_wallet_share"],
        "historical_known_wallet_share": wallet_memory["historical_known_wallet_share"],
        "historical_wallets_seen_count": wallet_memory["historical_wallets_seen_count"],
        "historical_avg_wallet_quality": wallet_memory["historical_avg_wallet_quality"],
        "creator_entity_quality_score": creator_entity["creator_entity_quality_score"],
        "creator_entity_confidence_score": creator_entity["creator_entity_confidence_score"],
        "creator_entity_launch_count": creator_entity["creator_entity_launch_count"],
        "creator_entity_paper_trade_count": creator_entity["creator_entity_paper_trade_count"],
        "creator_entity_is_known": creator_entity["creator_entity_is_known"],
        "creator_entity_creator_wallet_count": creator_entity["creator_entity_creator_wallet_count"],
        "creator_entity_funder_wallet_count": creator_entity["creator_entity_funder_wallet_count"],
        "creator_first_hop_funder_seen_before": creator_entity["creator_first_hop_funder_seen_before"],
        "creator_wallet_seen_before": creator_entity["creator_wallet_seen_before"],
        "creator_new_wallet_but_known_entity_flag": creator_entity["creator_new_wallet_but_known_entity_flag"],
        "creator_funder_cluster_win_rate": creator_entity["creator_funder_cluster_win_rate"],
        "creator_funder_cluster_invalidation_rate": creator_entity["creator_funder_cluster_invalidation_rate"],
        "creator_exchange_touch_recurrence_score": creator_entity["creator_exchange_touch_recurrence_score"],
    }
    return features
