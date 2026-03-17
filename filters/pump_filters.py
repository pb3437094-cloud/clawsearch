from typing import Any


def score_token_state(state: dict) -> tuple[float, list[str]]:
    score = 0.0
    reasons = []

    net_5m = float(state.get("net_sol_flow_last_5m", 0.0) or 0.0)
    buys_5m = int(state.get("buys_last_5m", 0) or 0)
    sells_5m = int(state.get("sells_last_5m", 0) or 0)
    trades_5m = int(state.get("trades_last_5m", 0) or 0)
    trades_1m = int(state.get("trades_last_1m", 0) or 0)
    current_mcap = float(state.get("current_market_cap_sol", 0.0) or 0.0)
    peak_mcap = float(state.get("peak_market_cap_sol", 0.0) or 0.0)
    initial_buy = float(state.get("initial_buy", 0.0) or 0.0)
    unique_buyers_5m = int(state.get("unique_buyers_last_5m", state.get("unique_buyers", 0)) or 0)
    unique_traders_5m = int(state.get("unique_traders_last_5m", state.get("unique_traders", 0)) or 0)
    status = state.get("status")

    participant_quality = float(state.get("participant_quality_score_v2", 0.0) or 0.0)
    wallet_novelty = float(state.get("wallet_novelty_score", 0.0) or 0.0)
    repeat_wallet_ratio = float(state.get("repeat_wallet_ratio", 0.0) or 0.0)
    buyer_overlap_ratio = float(state.get("buyer_overlap_ratio", 0.0) or 0.0)
    participant_churn_ratio = float(state.get("participant_churn_ratio", 0.0) or 0.0)
    participant_concentration = float(state.get("participant_concentration_score", 0.0) or 0.0)
    new_buyer_velocity = float(state.get("new_buyer_velocity", 0.0) or 0.0)

    if initial_buy > 0:
        score += 10
        reasons.append("nonzero_initial_buy")
    if current_mcap >= 30:
        score += 10
        reasons.append("mcap_above_30")
    if current_mcap >= 35:
        score += 10
        reasons.append("mcap_above_35")
    if trades_5m >= 5:
        score += 10
        reasons.append("active_5m_trading")
    if trades_5m >= 10:
        score += 10
        reasons.append("heavy_5m_trading")
    if buys_5m > sells_5m:
        score += 10
        reasons.append("buy_pressure")
    if net_5m > 0.5:
        score += 15
        reasons.append("positive_5m_flow")
    if net_5m > 2.0:
        score += 15
        reasons.append("strong_positive_5m_flow")
    if unique_buyers_5m >= 3:
        score += 10
        reasons.append("multiple_recent_buyers")
    if unique_traders_5m >= 5:
        score += 10
        reasons.append("broad_recent_participation")
    if participant_quality >= 0.60:
        score += 15
        reasons.append("participant_quality_v2")
    if wallet_novelty >= 0.45 and new_buyer_velocity >= 0.20:
        score += 8
        reasons.append("fresh_wallet_expansion")
    if participant_churn_ratio >= 0.45 and trades_1m >= 3:
        score += 6
        reasons.append("fresh_participant_wave")
    if peak_mcap > current_mcap * 1.5 and net_5m < 0:
        score -= 15
        reasons.append("post_peak_weakness")
    if sells_5m > buys_5m + 3:
        score -= 15
        reasons.append("sell_pressure")
    if repeat_wallet_ratio >= 0.55 and trades_5m >= 6:
        score -= 15
        reasons.append("repeat_wallet_churn")
    if participant_concentration >= 0.65 and trades_5m >= 6:
        score -= 12
        reasons.append("participant_concentration")
    if buyer_overlap_ratio >= 0.75 and wallet_novelty < 0.25 and trades_1m >= 3:
        score -= 10
        reasons.append("recycled_buyer_wave")
    if status == "escalated":
        score += 15
        reasons.append("already_escalated_state")

    return round(score, 2), reasons


def should_escalate(state: dict) -> tuple[bool, float, list[str]]:
    score, reasons = score_token_state(state)
    escalate = state.get("status") == "escalated" or score >= 60
    return escalate, score, reasons
