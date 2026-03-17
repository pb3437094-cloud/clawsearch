from __future__ import annotations

from collections import Counter
from math import log
from typing import Any


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) < 1e-9:
        return default
    return numerator / denominator


def _wallet_counter(
    trades: list[dict[str, Any]],
    *,
    tx_type: str | None = None,
) -> Counter[str]:
    wallets: Counter[str] = Counter()
    for trade in trades:
        if tx_type and trade.get("tx_type") != tx_type:
            continue
        wallet = trade.get("trader_wallet")
        if wallet:
            wallets[wallet] += 1
    return wallets


def _normalized_entropy(counts: Counter[str]) -> float:
    total = sum(counts.values())
    unique = len(counts)
    if total <= 0 or unique <= 1:
        return 0.0

    entropy = 0.0
    for count in counts.values():
        share = count / total
        entropy -= share * log(share)

    return max(0.0, min(1.0, entropy / log(unique)))


def _normalized_concentration(counts: Counter[str]) -> float:
    total = sum(counts.values())
    unique = len(counts)
    if total <= 0 or unique == 0:
        return 0.0
    if unique == 1:
        return 1.0

    hhi = sum((count / total) ** 2 for count in counts.values())
    baseline = 1.0 / unique
    return max(0.0, min(1.0, (hhi - baseline) / (1.0 - baseline)))


def _new_wallet_count(window_counts: Counter[str], total_counts: dict[str, int]) -> int:
    fresh = 0
    for wallet, window_count in window_counts.items():
        historical_count = int(total_counts.get(wallet, 0) or 0)
        if historical_count <= window_count:
            fresh += 1
    return fresh


def build_participant_metrics(
    trades_1m: list[dict[str, Any]],
    trades_5m: list[dict[str, Any]],
    prior_trades_5m: list[dict[str, Any]],
    *,
    buyer_trade_counts: dict[str, int] | None = None,
    trader_trade_counts: dict[str, int] | None = None,
) -> dict[str, float | int]:
    buyer_trade_counts = buyer_trade_counts or {}
    trader_trade_counts = trader_trade_counts or {}

    trader_counts_1m = _wallet_counter(trades_1m)
    trader_counts_5m = _wallet_counter(trades_5m)
    buyer_counts_1m = _wallet_counter(trades_1m, tx_type="buy")
    buyer_counts_5m = _wallet_counter(trades_5m, tx_type="buy")
    seller_counts_1m = _wallet_counter(trades_1m, tx_type="sell")
    seller_counts_5m = _wallet_counter(trades_5m, tx_type="sell")
    prior_trader_counts = _wallet_counter(prior_trades_5m)
    prior_buyer_counts = _wallet_counter(prior_trades_5m, tx_type="buy")

    unique_traders_last_1m = len(trader_counts_1m)
    unique_traders_last_5m = len(trader_counts_5m)
    unique_buyers_last_1m = len(buyer_counts_1m)
    unique_buyers_last_5m = len(buyer_counts_5m)
    unique_sellers_last_1m = len(seller_counts_1m)
    unique_sellers_last_5m = len(seller_counts_5m)

    new_traders_last_1m = _new_wallet_count(trader_counts_1m, trader_trade_counts)
    new_traders_last_5m = _new_wallet_count(trader_counts_5m, trader_trade_counts)
    new_buyers_last_1m = _new_wallet_count(buyer_counts_1m, buyer_trade_counts)
    new_buyers_last_5m = _new_wallet_count(buyer_counts_5m, buyer_trade_counts)

    repeated_trader_trades_5m = sum(count for count in trader_counts_5m.values() if count > 1)
    repeated_buyer_trades_5m = sum(count for count in buyer_counts_5m.values() if count > 1)

    current_buyer_set = set(buyer_counts_1m)
    current_trader_set = set(trader_counts_1m)
    prior_buyer_set = set(prior_buyer_counts)
    prior_trader_set = set(prior_trader_counts)

    repeat_wallet_ratio = _safe_div(repeated_trader_trades_5m, len(trades_5m))
    repeat_buyer_ratio = _safe_div(
        repeated_buyer_trades_5m,
        sum(buyer_counts_5m.values()),
    )
    buyer_overlap_ratio = _safe_div(
        len(current_buyer_set & prior_buyer_set),
        len(current_buyer_set),
    )
    participant_churn_ratio = _safe_div(
        len(current_trader_set - prior_trader_set),
        len(current_trader_set),
    )
    wallet_novelty_score = _safe_div(new_buyers_last_5m, unique_buyers_last_5m)
    new_buyer_velocity = _safe_div(new_buyers_last_1m, unique_buyers_last_5m)
    buyer_density_5m = _safe_div(unique_buyers_last_5m, sum(buyer_counts_5m.values()))
    trader_density_5m = _safe_div(unique_traders_last_5m, len(trades_5m))
    cluster_entropy = _normalized_entropy(trader_counts_5m)
    participant_concentration_score = _normalized_concentration(trader_counts_5m)

    participant_quality_score_v2 = (
        wallet_novelty_score * 0.24
        + (1.0 - repeat_wallet_ratio) * 0.18
        + participant_churn_ratio * 0.16
        + cluster_entropy * 0.14
        + (1.0 - participant_concentration_score) * 0.14
        + buyer_density_5m * 0.08
        + trader_density_5m * 0.06
    )

    if buyer_overlap_ratio >= 0.75 and wallet_novelty_score < 0.30:
        participant_quality_score_v2 *= 0.88
    if repeat_wallet_ratio >= 0.60:
        participant_quality_score_v2 *= 0.90

    participant_quality_score_v2 = max(0.0, min(1.0, participant_quality_score_v2))

    return {
        "unique_traders_last_1m": unique_traders_last_1m,
        "unique_traders_last_5m": unique_traders_last_5m,
        "unique_buyers_last_1m": unique_buyers_last_1m,
        "unique_buyers_last_5m": unique_buyers_last_5m,
        "unique_sellers_last_1m": unique_sellers_last_1m,
        "unique_sellers_last_5m": unique_sellers_last_5m,
        "new_traders_last_1m": new_traders_last_1m,
        "new_traders_last_5m": new_traders_last_5m,
        "new_buyers_last_1m": new_buyers_last_1m,
        "new_buyers_last_5m": new_buyers_last_5m,
        "repeat_wallet_ratio": round(repeat_wallet_ratio, 4),
        "repeat_buyer_ratio": round(repeat_buyer_ratio, 4),
        "buyer_overlap_ratio": round(buyer_overlap_ratio, 4),
        "participant_churn_ratio": round(participant_churn_ratio, 4),
        "cluster_entropy": round(cluster_entropy, 4),
        "participant_concentration_score": round(participant_concentration_score, 4),
        "wallet_novelty_score": round(wallet_novelty_score, 4),
        "new_buyer_velocity": round(new_buyer_velocity, 4),
        "participant_quality_score_v2": round(participant_quality_score_v2, 4),
    }
