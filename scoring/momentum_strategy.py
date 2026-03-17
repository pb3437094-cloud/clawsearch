from typing import Any


def score_momentum_strategy(state: dict[str, Any], events: list[dict[str, Any]] | None = None) -> tuple[float, list[str]]:
    events = events or []
    score = 0.0
    reasons: list[str] = []

    buys_1m = int(state.get("buys_last_1m", 0) or 0)
    buys_5m = int(state.get("buys_last_5m", 0) or 0)
    sells_5m = int(state.get("sells_last_5m", 0) or 0)
    buy_sol_1m = float(state.get("buy_sol_last_1m", 0.0) or 0.0)
    buy_sol_5m = float(state.get("buy_sol_last_5m", 0.0) or 0.0)
    sell_sol_5m = float(state.get("sell_sol_last_5m", 0.0) or 0.0)
    net_1m = float(state.get("net_sol_flow_last_1m", 0.0) or 0.0)
    net_5m = float(state.get("net_sol_flow_last_5m", 0.0) or 0.0)
    unique_traders = int(state.get("unique_traders_last_5m", state.get("unique_traders", 0)) or 0)
    participant_quality = float(state.get("participant_quality_score_v2", 0.0) or 0.0)
    wallet_novelty = float(state.get("wallet_novelty_score", 0.0) or 0.0)
    repeat_wallet_ratio = float(state.get("repeat_wallet_ratio", 0.0) or 0.0)
    buyer_overlap_ratio = float(state.get("buyer_overlap_ratio", 0.0) or 0.0)
    status = state.get("status")
    event_types = {e.get("event_type") for e in events}

    if buys_1m >= 2:
        score += 8
        reasons.append("recent_buy_cluster")
    if buys_5m >= 4:
        score += 12
        reasons.append("buy_count_5m")
    if buys_5m >= 7:
        score += 12
        reasons.append("strong_buy_count_5m")

    if buy_sol_1m >= 0.5:
        score += 10
        reasons.append("buy_sol_1m")
    if buy_sol_5m >= 2.0:
        score += 14
        reasons.append("buy_sol_5m")

    if net_1m > 0:
        score += 8
        reasons.append("positive_net_1m")
    if net_5m >= 1.0:
        score += 12
        reasons.append("positive_net_5m")
    if net_5m >= 2.0:
        score += 12
        reasons.append("strong_positive_net_5m")

    if unique_traders >= 5:
        score += 8
        reasons.append("trader_participation")
    if participant_quality >= 0.60:
        score += 8
        reasons.append("high_participant_quality")
    if wallet_novelty >= 0.40:
        score += 6
        reasons.append("wallet_novelty")

    if "BUY_FLOW_5M_CROSS_2" in event_types:
        score += 10
        reasons.append("buy_flow_cross_event")
    if "NET_FLOW_5M_CROSS_1" in event_types:
        score += 10
        reasons.append("net_flow_cross_event")
    if "UNIQUE_BUYERS_CROSS_3" in event_types:
        score += 8
        reasons.append("buyer_expansion_event")
    if "WALLET_NOVELTY_SURGE" in event_types:
        score += 6
        reasons.append("wallet_novelty_event")

    if sells_5m > buys_5m:
        score -= 10
        reasons.append("sell_dominant_5m")
    if sell_sol_5m > buy_sol_5m:
        score -= 12
        reasons.append("negative_sol_balance_5m")
    if repeat_wallet_ratio >= 0.55 and buys_5m >= 4:
        score -= 10
        reasons.append("repeat_wallet_churn")
    if buyer_overlap_ratio >= 0.75 and wallet_novelty < 0.25:
        score -= 8
        reasons.append("recycled_buyer_wave")

    if status == "cooling_off":
        score -= 15
        reasons.append("cooling_status")
    elif status == "inactive":
        score -= 20
        reasons.append("inactive_status")

    return round(score, 2), reasons
