import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ALERT_LATEST_DIR = Path("data/alerts/latest")
ALERT_HISTORY_DIR = Path("data/alerts/history")
ALERT_LATEST_DIR.mkdir(parents=True, exist_ok=True)
ALERT_HISTORY_DIR.mkdir(parents=True, exist_ok=True)

NEGATIVE_ARCHETYPES = {"BOT_WASH", "DEAD_LAUNCH", "LIQUIDITY_FADE", "INSIDER_ROTATION"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_alert(snapshot: dict[str, Any]) -> str:
    score = float(snapshot.get("composite_score", 0.0) or 0.0)
    tier = snapshot.get("priority_tier", "background")
    setup_state = snapshot.get("setup_state", "")
    risk_flags = set(snapshot.get("risk_flags", []) or [])
    primary_archetype = str(snapshot.get("primary_archetype") or "")
    regime_tags = set(snapshot.get("regime_tags", []) or [])

    if "inactive" in risk_flags or "negative_net_flow" in risk_flags:
        return "invalidated"
    if primary_archetype in NEGATIVE_ARCHETYPES and score < 85:
        return "invalidated"
    if "THIN_LIQUIDITY" in regime_tags or "INSUFFICIENT_HISTORY" in regime_tags:
        return "watchlist"
    if {"RECYCLED_PARTICIPATION", "SELLER_OVERHANG"}.issubset(regime_tags):
        return "alert_candidate" if tier in ("alpha", "priority") and score >= 72 else "watchlist"
    if "SELLER_OVERHANG" in regime_tags and score < 75:
        return "watchlist"
    if tier == "alpha" and score >= 92 and setup_state == "paper_entry" and primary_archetype not in NEGATIVE_ARCHETYPES:
        return "paper_entry"
    if tier == "alpha" and score >= 84 and primary_archetype in {"BREAKOUT_RUNNER", "HIGH_TIGHT_RANGE", "REVIVAL_RECLAIM"}:
        return "paper_hold"
    if tier in ("alpha", "priority") and score >= 68:
        return "alert_candidate"
    if tier in ("watch", "priority") or score >= 40:
        return "watchlist"
    return "background"


def _build_payload(snapshot: dict[str, Any], band: str) -> dict[str, Any]:
    return {
        "mint": snapshot.get("rank_mint") or snapshot.get("mint"),
        "symbol": snapshot.get("symbol"),
        "name": snapshot.get("name"),
        "band": band,
        "score": snapshot.get("composite_score"),
        "priority_tier": snapshot.get("priority_tier"),
        "strategy_name": snapshot.get("strategy_name"),
        "primary_archetype": snapshot.get("primary_archetype"),
        "secondary_archetype": snapshot.get("secondary_archetype"),
        "archetype_confidence": snapshot.get("archetype_confidence"),
        "regime_tags": snapshot.get("regime_tags", []),
        "archetype_reasons": snapshot.get("archetype_reasons", []),
        "setup_state": snapshot.get("setup_state"),
        "risk_flags": snapshot.get("risk_flags", []),
        "why_now": snapshot.get("why_now", []),
        "current_market_cap_sol": snapshot.get("current_market_cap_sol"),
        "captured_at": _now(),
    }


def _load_latest(mint: str) -> dict[str, Any] | None:
    path = ALERT_LATEST_DIR / f"{mint}.json"
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fingerprint(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "band": payload.get("band"),
        "score_bucket": int(float(payload.get("score", 0.0) or 0.0) // 5),
        "priority_tier": payload.get("priority_tier"),
        "strategy_name": payload.get("strategy_name"),
        "primary_archetype": payload.get("primary_archetype"),
        "setup_state": payload.get("setup_state"),
        "regime_tags": sorted(payload.get("regime_tags", [])),
        "risk_flags": sorted(payload.get("risk_flags", [])),
    }


def write_alert(snapshot: dict[str, Any]) -> dict[str, Any]:
    mint = snapshot.get("rank_mint") or snapshot.get("mint")
    band = classify_alert(snapshot)
    payload = _build_payload(snapshot, band)
    previous = _load_latest(mint)

    action = "created"
    if previous is not None:
        old_fp = _fingerprint(previous)
        new_fp = _fingerprint(payload)
        if old_fp == new_fp:
            action = "unchanged"
        elif previous.get("band") != payload.get("band"):
            action = "transition"
        else:
            action = "updated"

    latest_path = ALERT_LATEST_DIR / f"{mint}.json"
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    if action in {"created", "transition", "updated"}:
        history_path = ALERT_HISTORY_DIR / f"{mint}.jsonl"
        with open(history_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"action": action, **payload}, ensure_ascii=False) + "\n")

    return {"action": action, **payload}
