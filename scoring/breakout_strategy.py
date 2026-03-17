from typing import Any


def score_breakout_strategy(state: dict[str, Any], events: list[dict[str, Any]] | None = None) -> tuple[float, list[str]]:
    events = events or []
    score = 0.0
    reasons: list[str] = []

    current_mcap = float(state.get("current_market_cap_sol", 0.0) or 0.0)
    peak_mcap = float(state.get("peak_market_cap_sol", 0.0) or 0.0)
    trades_1m = int(state.get("trades_last_1m", 0) or 0)
    trades_5m = int(state.get("trades_last_5m", 0) or 0)
    buy_sol_5m = float(state.get("buy_sol_last_5m", 0.0) or 0.0)
    net_5m = float(state.get("net_sol_flow_last_5m", 0.0) or 0.0)
    unique_buyers = int(state.get("unique_buyers_last_5m", state.get("unique_buyers", 0)) or 0)
    participant_quality = float(state.get("participant_quality_score_v2", 0.0) or 0.0)
    wallet_novelty = float(state.get("wallet_novelty_score", 0.0) or 0.0)
    repeat_wallet_ratio = float(state.get("repeat_wallet_ratio", 0.0) or 0.0)
    status = state.get("status")
    event_types = {e.get("event_type") for e in events}

    if current_mcap >= 30:
        score += 10
        reasons.append("mcap_above_30")
    if current_mcap >= 35:
        score += 12
        reasons.append("mcap_above_35")
    if current_mcap >= 45:
        score += 10
        reasons.append("mcap_above_45")

    if trades_1m >= 4:
        score += 10
        reasons.append("fast_1m_tape")
    if trades_5m >= 8:
        score += 10
        reasons.append("active_5m_tape")

    if buy_sol_5m >= 1.5:
        score += 12
        reasons.append("buy_flow_5m")
    if net_5m >= 1.0:
        score += 16
        reasons.append("net_inflow_5m")
    if net_5m >= 2.5:
        score += 12
        reasons.append("strong_net_inflow_5m")

    if unique_buyers >= 3:
        score += 10
        reasons.append("distributed_buyers")
    if unique_buyers >= 5:
        score += 8
        reasons.append("broad_buyer_set")
    if participant_quality >= 0.60:
        score += 10
        reasons.append("high_participant_quality")
    if wallet_novelty >= 0.40:
        score += 6
        reasons.append("buyer_novelty")

    if "PEAK_BREAKOUT" in event_types:
        score += 15
        reasons.append("peak_breakout_event")
    if "MCAP_CROSS_35" in event_types:
        score += 10
        reasons.append("fresh_mcap_cross")
    if "TX_ACCELERATION_1M" in event_types:
        score += 8
        reasons.append("tx_acceleration")
    if "PARTICIPANT_QUALITY_HIGH" in event_types:
        score += 8
        reasons.append("participant_quality_event")

    if status == "escalated":
        score += 10
        reasons.append("escalated_status")
    elif status == "reawakened":
        score += 8
        reasons.append("reawakened_status")

    if peak_mcap > 0 and current_mcap < peak_mcap * 0.72:
        score -= 15
        reasons.append("far_off_peak")
    if repeat_wallet_ratio >= 0.55 and trades_5m >= 6:
        score -= 10
        reasons.append("repeat_wallet_churn")

    return round(score, 2), reasons
