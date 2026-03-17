import hashlib
import json
from pathlib import Path
from typing import Any

ESC_DIR = Path("data/escalated_tokens")
ESC_DIR.mkdir(parents=True, exist_ok=True)

_MEANINGFUL_FIELDS = [
    "status",
    "status_reason",
    "current_market_cap_sol",
    "peak_market_cap_sol",
    "trough_market_cap_sol",
    "trades_last_1m",
    "trades_last_5m",
    "buys_last_1m",
    "buys_last_5m",
    "sells_last_1m",
    "sells_last_5m",
    "buy_sol_last_1m",
    "buy_sol_last_5m",
    "sell_sol_last_1m",
    "sell_sol_last_5m",
    "net_sol_flow_last_1m",
    "net_sol_flow_last_5m",
    "unique_buyers_last_1m",
    "unique_buyers_last_5m",
    "unique_traders_last_1m",
    "unique_traders_last_5m",
    "repeat_wallet_ratio",
    "buyer_overlap_ratio",
    "participant_churn_ratio",
    "cluster_entropy",
    "participant_concentration_score",
    "wallet_novelty_score",
    "participant_quality_score_v2",
    "initial_buy",
]


def _bucketize(value: float | int | None, bands: list[tuple[float, str]], default: str = "none") -> str:
    try:
        number = float(value if value is not None else 0.0)
    except (TypeError, ValueError):
        return default
    current = default
    for threshold, label in bands:
        if number >= threshold:
            current = label
    return current


def _normalize_meaningful_value(field: str, value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, int):
        return int(value)
    return value


def build_regime_snapshot(state: dict[str, Any], score: float, reasons: list[str]) -> dict[str, Any]:
    current_mcap = state.get("current_market_cap_sol") or state.get("market_cap_sol")
    peak_mcap = state.get("peak_market_cap_sol") or 0.0
    retrace_pct = 0.0
    try:
        if peak_mcap:
            retrace_pct = max(0.0, (float(peak_mcap) - float(current_mcap or 0.0)) / float(peak_mcap) * 100.0)
    except (TypeError, ValueError, ZeroDivisionError):
        retrace_pct = 0.0

    return {
        "status": state.get("status"),
        "score_band": _bucketize(score, [(50, "50"), (75, "75"), (100, "100"), (125, "125")]),
        "mcap_band": _bucketize(current_mcap, [(30, "30"), (35, "35"), (50, "50"), (80, "80")]),
        "net_flow_band": _bucketize(state.get("net_sol_flow_last_5m"), [(0.5, "0_5"), (1.0, "1"), (2.0, "2"), (4.0, "4")]),
        "buy_flow_band": _bucketize(state.get("buy_sol_last_5m"), [(1, "1"), (2, "2"), (5, "5"), (10, "10")]),
        "buyers_band": _bucketize(state.get("unique_buyers_last_5m"), [(3, "3"), (5, "5"), (8, "8"), (20, "20")]),
        "participant_quality_band": _bucketize(state.get("participant_quality_score_v2"), [(0.35, "0_35"), (0.5, "0_5"), (0.65, "0_65"), (0.8, "0_8")], default="0"),
        "novelty_band": _bucketize(state.get("wallet_novelty_score"), [(0.25, "0_25"), (0.4, "0_4"), (0.6, "0_6")], default="0"),
        "repeat_band": _bucketize(state.get("repeat_wallet_ratio"), [(0.35, "0_35"), (0.5, "0_5"), (0.65, "0_65")], default="0"),
        "retrace_band": _bucketize(retrace_pct, [(10, "10"), (20, "20"), (35, "35"), (50, "50")], default="0"),
        "reason_top": sorted((reasons or [])[:5]),
    }


def _fingerprint(regime: dict[str, Any]) -> str:
    payload = json.dumps(regime, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_escalation_regime_if_changed(
    state: dict[str, Any],
    score: float,
    reasons: list[str],
) -> tuple[str | None, str, dict[str, Any]]:
    mint = state["mint"]
    regime = build_regime_snapshot(state, score, reasons)
    fingerprint = _fingerprint(regime)
    path = ESC_DIR / f"{mint}.json"

    previous = None
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            previous = json.load(f)

    if previous is not None and previous.get("regime_fingerprint") == fingerprint:
        return None, "unchanged", regime

    meaningful_snapshot = {
        field: _normalize_meaningful_value(field, state.get(field))
        for field in _MEANINGFUL_FIELDS
    }

    payload = {
        **state,
        "escalation_score": score,
        "escalation_reasons": reasons,
        "meaningful_snapshot": meaningful_snapshot,
        "regime_snapshot": regime,
        "regime_fingerprint": fingerprint,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return str(path), "written" if previous is None else "transition", regime
