from __future__ import annotations

from typing import Any


NEGATIVE_ARCHETYPES = {"BOT_WASH", "DEAD_LAUNCH", "LIQUIDITY_FADE", "INSIDER_ROTATION"}


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _round_scores(values: dict[str, float]) -> dict[str, float]:
    return {key: round(float(val), 4) for key, val in values.items()}


def _push(score_map: dict[str, float], reason_map: dict[str, list[str]], archetype: str, points: float, reason: str) -> None:
    score_map[archetype] = score_map.get(archetype, 0.0) + float(points)
    reason_map.setdefault(archetype, []).append(reason)


def classify_archetype(
    state: dict[str, Any],
    features: dict[str, float],
    events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    events = events or []
    event_types = {str(event.get("event_type") or "") for event in events}

    current_mcap = float(state.get("current_market_cap_sol", 0.0) or state.get("market_cap_sol", 0.0) or 0.0)
    peak_mcap = float(state.get("peak_market_cap_sol", 0.0) or 0.0)
    net_5m = float(state.get("net_sol_flow_last_5m", 0.0) or 0.0)
    trades_1m = int(state.get("trades_last_1m", 0) or 0)
    trades_5m = int(state.get("trades_last_5m", 0) or 0)
    unique_buyers_5m = int(state.get("unique_buyers_last_5m", state.get("unique_buyers", 0)) or 0)
    unique_traders_5m = int(state.get("unique_traders_last_5m", state.get("unique_traders", 0)) or 0)
    revival_count = int(state.get("revival_count", 0) or 0)

    mcap_to_peak = float(features.get("mcap_to_peak_ratio", 0.0) or 0.0)
    peak_retrace_pct = float(features.get("peak_retrace_pct", 0.0) or 0.0)
    range_position_pct = float(features.get("range_position_pct", 0.0) or 0.0)
    trade_acceleration = float(features.get("trade_acceleration_ratio", 0.0) or 0.0)
    buy_acceleration = float(features.get("buy_acceleration_ratio", 0.0) or 0.0)
    buy_pressure_1m = float(features.get("buy_pressure_ratio_1m", 0.0) or 0.0)
    buy_pressure_5m = float(features.get("buy_pressure_ratio_5m", 0.0) or 0.0)
    buyer_density_5m = float(features.get("buyer_density_5m", 0.0) or 0.0)
    seller_density_5m = float(features.get("seller_density_5m", 0.0) or 0.0)
    buyer_participation = float(features.get("buyer_participation_ratio", 0.0) or 0.0)
    net_efficiency = float(features.get("net_flow_per_trade_5m", 0.0) or 0.0)
    participant_quality = float(features.get("participant_quality_score_v2", 0.0) or 0.0)
    wallet_novelty = float(features.get("wallet_novelty_score", 0.0) or 0.0)
    repeat_wallet_ratio = float(features.get("repeat_wallet_ratio", 0.0) or 0.0)
    buyer_overlap_ratio = float(features.get("buyer_overlap_ratio", 0.0) or 0.0)
    participant_churn = float(features.get("participant_churn_ratio", 0.0) or 0.0)
    participant_concentration = float(features.get("participant_concentration_score", 0.0) or 0.0)
    new_buyer_velocity = float(features.get("new_buyer_velocity", 0.0) or 0.0)
    wash_risk = float(features.get("wash_risk_ratio", 0.0) or 0.0)
    recovery_ratio = float(features.get("recovery_ratio_from_trough", 0.0) or 0.0)
    peak_damage_pct = float(features.get("peak_to_trough_damage_pct", 0.0) or 0.0)
    near_high_dwell_pct = float(features.get("near_high_dwell_pct", 0.0) or 0.0)
    mcap_stability = float(features.get("mcap_stability_score", 0.0) or 0.0)
    sell_absorption = float(features.get("sell_absorption_score", 0.0) or 0.0)
    buyer_refresh = float(features.get("buyer_refresh_ratio_1m_vs_5m", 0.0) or 0.0)
    seller_expansion = float(features.get("seller_expansion_ratio", 0.0) or 0.0)
    event_sequence_score = float(features.get("event_sequence_score", 0.0) or 0.0)
    time_since_first_seen = float(features.get("time_since_first_seen_seconds", 0.0) or 0.0)
    time_since_last_trade = float(features.get("time_since_last_trade_seconds", 0.0) or 0.0)
    time_since_peak = float(features.get("time_since_peak_seconds", 0.0) or 0.0)
    time_since_last_buy = float(features.get("time_since_last_buy_seconds", 0.0) or 0.0)

    archetypes = [
        "BREAKOUT_RUNNER",
        "HIGH_TIGHT_RANGE",
        "MOMENTUM_EXPANSION",
        "EARLY_MICRO_PUMP",
        "REVIVAL_RECLAIM",
        "SLOW_BURN_ACCUMULATION",
        "INSIDER_ROTATION",
        "BOT_WASH",
        "DEAD_LAUNCH",
        "LIQUIDITY_FADE",
        "FAILED_BREAKOUT",
    ]
    scores = {name: 0.0 for name in archetypes}
    reasons = {name: [] for name in archetypes}
    regime_tags: list[str] = []
    regime_reasons: list[str] = []

    # --- Regime tags ---
    if participant_quality >= 0.56 and wallet_novelty >= 0.30 and repeat_wallet_ratio <= 0.42 and buyer_overlap_ratio <= 0.58:
        regime_tags.append("ORGANIC_PARTICIPATION")
        regime_reasons.append("participant_quality_and_wallet_mix_look_organic")
    if repeat_wallet_ratio >= 0.50 or buyer_overlap_ratio >= 0.72 or (wallet_novelty < 0.22 and participant_churn < 0.32):
        regime_tags.append("RECYCLED_PARTICIPATION")
        regime_reasons.append("repeat_wallet_or_overlap_pattern_is_elevated")
    if mcap_to_peak >= 0.97 and peak_mcap >= 35 and buy_pressure_1m < 0.58 and trade_acceleration < 1.0:
        regime_tags.append("OVEREXTENDED")
        regime_reasons.append("near_highs_but_short_term_flow_is_softening")
    if recovery_ratio >= 0.58 and participant_quality >= 0.52 and buy_pressure_5m >= 0.60 and repeat_wallet_ratio < 0.48:
        regime_tags.append("RECLAIM_CLEAN")
        regime_reasons.append("recovery_has_quality_and_is_not_heavily_recycled")
    if recovery_ratio >= 0.40 and (participant_quality < 0.50 or repeat_wallet_ratio >= 0.48 or seller_expansion >= 1.05):
        regime_tags.append("RECLAIM_FRAGILE")
        regime_reasons.append("recovery_exists_but_structure_is_still_fragile")
    if seller_expansion >= 1.15 or seller_density_5m >= 0.70 or (buy_pressure_5m < 0.52 and net_5m <= 0):
        regime_tags.append("SELLER_OVERHANG")
        regime_reasons.append("seller_expansion_or_sell_pressure_is_rising")
    if unique_traders_5m <= 3 or buyer_density_5m < 0.34:
        regime_tags.append("THIN_LIQUIDITY")
        regime_reasons.append("buyer_base_is_too_narrow_for_high_confidence")
    if trades_5m <= 5 or unique_buyers_5m <= 3 or unique_traders_5m <= 4:
        regime_tags.append("INSUFFICIENT_HISTORY")
        regime_reasons.append("observation_window_is_too_short_for_structural_classification")

    # --- Archetype scoring ---
    if current_mcap >= 35 and mcap_to_peak >= 0.94:
        _push(scores, reasons, "BREAKOUT_RUNNER", 3.2, "price_structure_is_holding_near_highs_above_breakout_zone")
    if buy_pressure_5m >= 0.63 and participant_quality >= 0.54:
        _push(scores, reasons, "BREAKOUT_RUNNER", 2.4, "buy_pressure_and_participant_quality_support_breakout_continuation")
    if net_efficiency > 0 and buyer_density_5m >= 0.50:
        _push(scores, reasons, "BREAKOUT_RUNNER", 1.6, "net_flow_is_positive_with_distributed_buyers")
    if "PEAK_BREAKOUT" in event_types:
        _push(scores, reasons, "BREAKOUT_RUNNER", 2.2, "peak_breakout_event_confirmed")
    if "ORGANIC_PARTICIPATION" in regime_tags:
        _push(scores, reasons, "BREAKOUT_RUNNER", 1.2, "organic_participation_tag_supports_continuation")

    if mcap_to_peak >= 0.92 and peak_retrace_pct <= 10.5:
        _push(scores, reasons, "HIGH_TIGHT_RANGE", 3.1, "token_is_compressing_close_to_highs")
    if near_high_dwell_pct >= 18 or mcap_stability >= 0.72:
        _push(scores, reasons, "HIGH_TIGHT_RANGE", 2.3, "dwell_or_stability_near_highs_is_constructive")
    if trade_acceleration <= 1.25 and buy_pressure_5m >= 0.58:
        _push(scores, reasons, "HIGH_TIGHT_RANGE", 1.5, "tight_hold_without_excessive_heat")
    if buy_pressure_1m >= 0.55 and sell_absorption >= 0.54:
        _push(scores, reasons, "HIGH_TIGHT_RANGE", 1.2, "sells_are_being_absorbed_while_holding_highs")

    if trade_acceleration >= 1.35:
        _push(scores, reasons, "MOMENTUM_EXPANSION", 3.0, "trade_acceleration_is_high")
    if buy_acceleration >= 1.25 and buy_pressure_1m >= 0.62:
        _push(scores, reasons, "MOMENTUM_EXPANSION", 2.0, "short_term_buy_flow_is_expanding")
    if wallet_novelty >= 0.30 and new_buyer_velocity >= 0.14:
        _push(scores, reasons, "MOMENTUM_EXPANSION", 1.6, "fresh_wallet_arrivals_support_momentum")
    if event_sequence_score >= 0.58:
        _push(scores, reasons, "MOMENTUM_EXPANSION", 1.4, "event_sequence_is_consistent_with_acceleration")

    if current_mcap < 35 and trade_acceleration >= 1.30:
        _push(scores, reasons, "EARLY_MICRO_PUMP", 2.8, "sub_breakout_name_is_showing_early_acceleration")
    if buy_pressure_5m >= 0.62 and unique_buyers_5m >= 2:
        _push(scores, reasons, "EARLY_MICRO_PUMP", 2.0, "buy_pressure_is_strong_for_early_stage_token")
    if wallet_novelty >= 0.28 or buyer_refresh >= 0.35:
        _push(scores, reasons, "EARLY_MICRO_PUMP", 1.2, "fresh_buyer_turnover_supports_early_push")

    if peak_retrace_pct >= 10 and recovery_ratio >= 0.48:
        _push(scores, reasons, "REVIVAL_RECLAIM", 3.2, "token_has_recovered_meaningfully_from_trough")
    if revival_count >= 1 or "STATUS_REAWAKENED_TO_ESCALATED" in event_types or "STATUS_WATCHING_TO_ESCALATED" in event_types:
        _push(scores, reasons, "REVIVAL_RECLAIM", 1.8, "lifecycle_revival_signal_is_present")
    if participant_quality >= 0.52 and buy_pressure_5m >= 0.58 and sell_absorption >= 0.52:
        _push(scores, reasons, "REVIVAL_RECLAIM", 2.2, "reclaim_is_backed_by_quality_flow")
    if "RECLAIM_CLEAN" in regime_tags:
        _push(scores, reasons, "REVIVAL_RECLAIM", 2.0, "clean_reclaim_tag_reinforces_setup")

    if time_since_first_seen >= 240 and mcap_to_peak >= 0.78:
        _push(scores, reasons, "SLOW_BURN_ACCUMULATION", 2.2, "token_has_survived_longer_than_a_pure_micro_pump")
    if 0.50 <= buy_pressure_5m <= 0.68 and participant_quality >= 0.48:
        _push(scores, reasons, "SLOW_BURN_ACCUMULATION", 2.0, "steady_buy_pressure_and_quality_fit_accumulation")
    if trade_acceleration <= 1.25 and seller_expansion < 1.05:
        _push(scores, reasons, "SLOW_BURN_ACCUMULATION", 1.4, "activity_is_constructive_without_chasing")
    if buyer_refresh >= 0.30 and wallet_novelty >= 0.20:
        _push(scores, reasons, "SLOW_BURN_ACCUMULATION", 1.0, "fresh_buyer_refresh_supports_slow_burn")

    if participant_concentration >= 0.66:
        _push(scores, reasons, "INSIDER_ROTATION", 2.8, "participant_concentration_is_high")
    if repeat_wallet_ratio >= 0.48 or buyer_overlap_ratio >= 0.72:
        _push(scores, reasons, "INSIDER_ROTATION", 2.2, "same_wallet_reuse_or_overlap_is_elevated")
    if wallet_novelty < 0.22 and participant_quality < 0.50:
        _push(scores, reasons, "INSIDER_ROTATION", 1.6, "wallet_base_looks_too_recycled_for_clean_expansion")
    if buyer_density_5m < 0.40 and current_mcap >= 28:
        _push(scores, reasons, "INSIDER_ROTATION", 1.0, "move_is_underdistributed_for_its_market_cap")

    if wash_risk >= 4.5:
        _push(scores, reasons, "BOT_WASH", 3.2, "wash_risk_ratio_is_extreme")
    if repeat_wallet_ratio >= 0.58:
        _push(scores, reasons, "BOT_WASH", 2.6, "repeat_wallet_ratio_is_too_high")
    if buyer_overlap_ratio >= 0.82:
        _push(scores, reasons, "BOT_WASH", 2.0, "buyer_overlap_is_consistent_with_recycled_flow")
    if participant_quality < 0.38:
        _push(scores, reasons, "BOT_WASH", 1.4, "participant_quality_breaks_down_under_flow")

    if current_mcap < 35:
        _push(scores, reasons, "DEAD_LAUNCH", 1.6, "token_never_established_itself_above_core_breakout_zone")
    if net_5m <= 0 and buy_pressure_5m < 0.52:
        _push(scores, reasons, "DEAD_LAUNCH", 2.6, "recent_flow_is_not_supportive")
    if participant_quality < 0.36 and wallet_novelty < 0.18:
        _push(scores, reasons, "DEAD_LAUNCH", 2.0, "quality_and_novelty_are_too_low")
    if trades_5m < 4 and time_since_last_trade > 60:
        _push(scores, reasons, "DEAD_LAUNCH", 1.2, "activity_has_stalled_early")

    if mcap_to_peak < 0.86:
        _push(scores, reasons, "LIQUIDITY_FADE", 1.8, "token_has_faded_off_peak")
    if seller_expansion >= 1.10:
        _push(scores, reasons, "LIQUIDITY_FADE", 2.2, "seller_presence_is_expanding")
    if buy_pressure_5m < 0.52 and trade_acceleration < 1.0:
        _push(scores, reasons, "LIQUIDITY_FADE", 2.0, "buy_pressure_and_activity_are_fading")
    if time_since_last_buy > 90 and trades_5m >= 6:
        _push(scores, reasons, "LIQUIDITY_FADE", 1.0, "buy_side_is_not_refreshing_fast_enough")

    breakout_like = current_mcap >= 35 or peak_mcap >= 35 or "PEAK_BREAKOUT" in event_types
    if breakout_like:
        _push(scores, reasons, "FAILED_BREAKOUT", 1.8, "token_reached_breakout_context")
    if peak_retrace_pct >= 14 and mcap_to_peak <= 0.88:
        _push(scores, reasons, "FAILED_BREAKOUT", 2.8, "breakout_has_taken_meaningful_damage")
    if net_5m <= 0 or "SELLER_OVERHANG" in regime_tags:
        _push(scores, reasons, "FAILED_BREAKOUT", 2.0, "post_breakout_flow_has_weakened")
    if recovery_ratio < 0.45:
        _push(scores, reasons, "FAILED_BREAKOUT", 1.4, "recovery_from_trough_is_not_good_enough")

    # Damp obviously conflicting archetypes.
    if "INSUFFICIENT_HISTORY" in regime_tags:
        scores["HIGH_TIGHT_RANGE"] *= 0.55
        scores["BREAKOUT_RUNNER"] *= 0.70
        scores["REVIVAL_RECLAIM"] *= 0.75
        scores["FAILED_BREAKOUT"] *= 0.85
        _push(scores, reasons, "EARLY_MICRO_PUMP", 1.2, "history_is_too_short_to_confirm_compression_or_reclaim")
        _push(scores, reasons, "MOMENTUM_EXPANSION", 0.6, "short_observation_window_favors_momentum_over_structure")
    if "BOT_WASH" in archetypes and scores["BOT_WASH"] >= 6.0:
        scores["BREAKOUT_RUNNER"] *= 0.65
        scores["HIGH_TIGHT_RANGE"] *= 0.70
        scores["REVIVAL_RECLAIM"] *= 0.75
    if scores["FAILED_BREAKOUT"] >= 5.0:
        scores["BREAKOUT_RUNNER"] *= 0.72
        scores["HIGH_TIGHT_RANGE"] *= 0.80
    if scores["REVIVAL_RECLAIM"] >= 5.5:
        scores["FAILED_BREAKOUT"] *= 0.80

    ranked = sorted(scores.items(), key=lambda item: (item[1], item[0]), reverse=True)
    primary_archetype, primary_score = ranked[0]
    secondary_archetype, secondary_score = ranked[1]
    margin = max(primary_score - secondary_score, 0.0)
    confidence = _clamp((primary_score / 10.0) * 0.65 + (margin / 4.0) * 0.35, 0.05, 0.99)
    if "INSUFFICIENT_HISTORY" in regime_tags:
        confidence = min(confidence, 0.58 if trades_5m >= 4 else 0.48)
    if "THIN_LIQUIDITY" in regime_tags:
        confidence = min(confidence, 0.52)

    all_reasons = list(reasons.get(primary_archetype, []))
    if regime_reasons:
        all_reasons.extend(regime_reasons[:2])

    return {
        "primary_archetype": primary_archetype,
        "secondary_archetype": secondary_archetype,
        "archetype_confidence": round(confidence, 4),
        "archetype_scores": _round_scores(scores),
        "top_archetypes": [{"name": name, "score": round(score, 4)} for name, score in ranked[:3]],
        "regime_tags": regime_tags,
        "archetype_reasons": all_reasons[:8],
        "archetype_reason_map": {key: value[:6] for key, value in reasons.items() if value},
        "is_negative_archetype": primary_archetype in NEGATIVE_ARCHETYPES,
    }
