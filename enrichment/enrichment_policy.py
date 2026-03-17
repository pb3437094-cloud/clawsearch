from __future__ import annotations

from typing import Any

LIGHT_TRIGGER_EVENTS = {
    "STATUS_WATCHING_TO_ESCALATED",
    "STATUS_REAWAKENED_TO_ESCALATED",
    "MCAP_CROSS_35",
    "BUY_FLOW_5M_CROSS_2",
    "NET_FLOW_5M_CROSS_1",
    "UNIQUE_BUYERS_CROSS_3",
    "TX_ACCELERATION_1M",
    "PEAK_BREAKOUT",
    "PARTICIPANT_QUALITY_HIGH",
    "WALLET_NOVELTY_SURGE",
}

DEEP_TRIGGER_EVENTS = {
    "STATUS_WATCHING_TO_ESCALATED",
    "STATUS_REAWAKENED_TO_ESCALATED",
    "PEAK_BREAKOUT",
    "PARTICIPANT_QUALITY_HIGH",
}


def _priority_rank(priority_tier: str | None) -> int:
    order = {"background": 0, "watch": 1, "priority": 2, "alpha": 3}
    return order.get(str(priority_tier or "background"), 0)


def build_enrichment_hint(
    state: dict[str, Any],
    events: list[dict[str, Any]] | None,
    local_snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    events = events or []
    event_types = [str(event.get("event_type") or "") for event in events]
    event_type_set = set(event_types)

    local_score = float(local_snapshot.get("composite_score", 0.0) or 0.0)
    priority_tier = str(local_snapshot.get("priority_tier") or "background")
    setup_state = str(local_snapshot.get("setup_state") or "research")
    features = local_snapshot.get("quant_features", {}) or {}
    previous_score = float(
        (previous_snapshot or {}).get("local_composite_score")
        or (previous_snapshot or {}).get("composite_score")
        or 0.0
    )

    current_mcap = float(
        state.get("current_market_cap_sol", state.get("market_cap_sol", 0.0)) or 0.0
    )
    status = str(state.get("status") or "new")
    trades_1m = int(state.get("trades_last_1m", 0) or 0)
    trades_5m = int(state.get("trades_last_5m", 0) or 0)
    buys_5m = int(state.get("buys_last_5m", 0) or 0)
    unique_buyers_5m = int(
        state.get("unique_buyers_last_5m", state.get("unique_buyers", 0)) or 0
    )

    reason = "local_background"
    tier = "none"
    max_wallets = 0

    deep_candidate = False
    if local_score >= 72:
        deep_candidate = True
        reason = "local_score_ge_72"
    elif _priority_rank(priority_tier) >= 3:
        deep_candidate = True
        reason = f"priority_tier_{priority_tier}"
    elif setup_state in {"candidate", "confirmed"}:
        deep_candidate = True
        reason = f"setup_state_{setup_state}"
    elif any(event_type in DEEP_TRIGGER_EVENTS for event_type in event_type_set):
        deep_candidate = True
        trigger = next(
            event_type for event_type in event_types if event_type in DEEP_TRIGGER_EVENTS
        )
        reason = f"event_{trigger.lower()}"
    elif local_score >= 66 and (local_score - previous_score) >= 8:
        deep_candidate = True
        reason = "local_score_jump"

    if deep_candidate:
        tier = "deep"
        max_wallets = 10
    else:
        light_candidate = False
        if local_score >= 52:
            light_candidate = True
            reason = "local_score_ge_52"
        elif _priority_rank(priority_tier) >= 2:
            light_candidate = True
            reason = f"priority_tier_{priority_tier}"
        elif setup_state in {"developing", "candidate", "confirmed"}:
            light_candidate = True
            reason = f"setup_state_{setup_state}"
        elif any(event_type in LIGHT_TRIGGER_EVENTS for event_type in event_type_set):
            light_candidate = True
            trigger = next(
                event_type
                for event_type in event_types
                if event_type in LIGHT_TRIGGER_EVENTS
            )
            reason = f"event_{trigger.lower()}"
        elif status in {"watching", "escalated", "reawakened"} and current_mcap >= 35:
            light_candidate = True
            reason = "status_and_mcap"
        elif trades_1m >= 6 or trades_5m >= 18:
            light_candidate = True
            reason = "trade_velocity"
        elif buys_5m >= 5 or unique_buyers_5m >= 4:
            light_candidate = True
            reason = "buyer_breadth"

        if light_candidate:
            tier = "light"
            max_wallets = 5

    if tier == "none":
        return {
            "tier": "none",
            "reason": reason,
            "max_wallets": 0,
            "score": round(local_score, 2),
        }

    return {
        "tier": tier,
        "reason": reason,
        "max_wallets": max_wallets,
        "score": round(local_score, 2),
        "priority_tier": priority_tier,
        "setup_state": setup_state,
        "events": event_types[-6:],
        "current_mcap": round(current_mcap, 4),
        "trades_1m": trades_1m,
        "trades_5m": trades_5m,
        "buys_5m": buys_5m,
        "unique_buyers_5m": unique_buyers_5m,
        "holder_concentration_ratio": float(
            features.get("holder_concentration_ratio", 0.0) or 0.0
        ),
    }
