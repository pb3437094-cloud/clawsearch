from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from research.wallet_intelligence import WalletIntelligenceEngine
except Exception:
    WalletIntelligenceEngine = None  # type: ignore[assignment]

try:
    from research.creator_entity_intelligence import CreatorEntityIntelligenceEngine
except Exception:
    CreatorEntityIntelligenceEngine = None  # type: ignore[assignment]

PAPER_DIR = Path("data/paper")
PAPER_DIR.mkdir(parents=True, exist_ok=True)
OPEN_TRADES_PATH = PAPER_DIR / "open_trades.json"
CLOSED_TRADES_PATH = PAPER_DIR / "closed_trades.json"
TOKEN_COHORT_CACHE_DIR = Path("data/enrichment/token_cohorts")

PAPER_MIN_HOLD_SECONDS = 8.0
PAPER_REENTRY_COOLDOWN_SECONDS = 45.0
PAPER_INVALIDATION_GREEN_HOLD_PNL_PCT = 5.0
PAPER_INVALIDATION_GREEN_HOLD_MAX_PNL_PCT = 10.0
PAPER_INVALIDATION_GREEN_GIVEBACK_FLOOR_PCT = -2.0
PAPER_STALE_NEGATIVE_CLOSE_SECONDS = 10.0 * 60.0
PAPER_STALE_ANY_CLOSE_SECONDS = 30.0 * 60.0
PAPER_STALE_LOSS_FLOOR_PNL_PCT = -10.0
PAPER_STALE_INVALIDATED_CLOSE_SECONDS = 3.0 * 60.0
PAPER_STALE_BACKGROUND_CLOSE_SECONDS = 6.0 * 60.0
PAPER_STALE_INVALIDATED_NONPOSITIVE_PNL_PCT = 0.5
PAPER_STALE_BACKGROUND_MAX_PNL_PCT = 1.0
PAPER_WINNER_LOCK_STAGE1_MAX_PNL_PCT = 12.0
PAPER_WINNER_LOCK_STAGE1_FLOOR_PNL_PCT = 1.0
PAPER_WINNER_LOCK_STAGE1_MIN_PULLBACK_PCT = 55.0
PAPER_WINNER_LOCK_STAGE2_MAX_PNL_PCT = 20.0
PAPER_WINNER_LOCK_STAGE2_FLOOR_PNL_PCT = 3.0
PAPER_WINNER_LOCK_STAGE2_MIN_PULLBACK_PCT = 45.0
PAPER_WINNER_LOCK_STAGE3_MAX_PNL_PCT = 35.0
PAPER_WINNER_LOCK_STAGE3_FLOOR_PNL_PCT = 8.0
PAPER_WINNER_LOCK_STAGE3_MIN_PULLBACK_PCT = 35.0

_WALLET_INTELLIGENCE_ENGINE: WalletIntelligenceEngine | None | bool = None
_CREATOR_ENTITY_INTELLIGENCE_ENGINE: CreatorEntityIntelligenceEngine | None | bool = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _seconds_since(value: str | None) -> float | None:
    dt = _parse_dt(value)
    if dt is None:
        return None
    return max((datetime.now(timezone.utc) - dt).total_seconds(), 0.0)


def _trade_age_seconds(trade: dict[str, Any]) -> float:
    age_seconds = _seconds_since(str(trade.get("opened_at_utc") or "").strip() or None)
    return float(age_seconds or 0.0)


def _current_trade_pnl_pct(trade: dict[str, Any]) -> float:
    pnl_pct = float(trade.get("pnl_pct_proxy", 0.0) or 0.0)
    current_mcap = float(trade.get("current_market_cap_sol", 0.0) or 0.0)
    entry_mcap = float(trade.get("entry_market_cap_sol", 0.0) or 0.0)
    if abs(pnl_pct) < 1e-9 and current_mcap > 0 and entry_mcap > 0:
        pnl_pct = ((current_mcap - entry_mcap) / entry_mcap) * 100.0
    return pnl_pct


def _winner_lock_floor_pnl_pct_for_trade(trade: dict[str, Any]) -> float | None:
    stored_floor = float(trade.get("winner_lock_floor_pnl_pct", 0.0) or 0.0)
    if stored_floor > 0:
        return stored_floor

    max_pnl_pct = float(trade.get("max_pnl_pct_proxy", 0.0) or 0.0)
    threshold = _winner_lock_threshold(max_pnl_pct)
    if threshold is None:
        return None
    floor_pnl_pct, _ = threshold
    return float(floor_pnl_pct or 0.0)


def _apply_winner_lock_exit_floor(
    trade: dict[str, Any],
    exit_reason: str,
) -> dict[str, Any]:
    if exit_reason not in {"winner_floor_stop", "winner_floor_timeout"}:
        return trade

    entry_mcap = float(trade.get("entry_market_cap_sol", 0.0) or 0.0)
    if entry_mcap <= 0:
        return trade

    floor_pnl_pct = _winner_lock_floor_pnl_pct_for_trade(trade)
    if floor_pnl_pct is None:
        return trade

    raw_current_mcap = float(trade.get("current_market_cap_sol", 0.0) or 0.0)
    raw_pnl_pct = _current_trade_pnl_pct(trade)
    floor_exit_mcap = entry_mcap * (1.0 + (float(floor_pnl_pct) / 100.0))
    protected_exit_mcap = max(raw_current_mcap, floor_exit_mcap)
    protected_pnl_pct = max(raw_pnl_pct, float(floor_pnl_pct))

    trade["winner_lock_floor_applied"] = True
    trade["winner_lock_raw_exit_market_cap_sol"] = round(raw_current_mcap, 6)
    trade["winner_lock_raw_pnl_pct_proxy"] = round(raw_pnl_pct, 4)
    trade["winner_lock_protected_exit_market_cap_sol"] = round(protected_exit_mcap, 6)
    trade["winner_lock_protected_pnl_pct_proxy"] = round(protected_pnl_pct, 4)
    trade["winner_lock_floor_pnl_pct"] = float(floor_pnl_pct)
    trade["current_market_cap_sol"] = round(protected_exit_mcap, 6)
    trade["pnl_pct_proxy"] = round(protected_pnl_pct, 4)
    return trade




def _set_close_decision_trace(
    trade: dict[str, Any],
    *,
    source: str,
    selected_reason: str,
    candidate_reasons: list[str] | tuple[str, ...],
    snapshot: dict[str, Any] | None = None,
    alert: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    extra = dict(extra or {})
    raw_current_pnl = extra.pop("raw_current_pnl_pct", _current_trade_pnl_pct(trade))
    raw_current_mcap = extra.pop(
        "raw_current_market_cap_sol",
        float(
            (
                snapshot.get("current_market_cap_sol", trade.get("current_market_cap_sol", 0.0))
                if snapshot is not None
                else trade.get("current_market_cap_sol", 0.0)
            )
            or 0.0
        ),
    )
    reason_before_close = str(trade.get("reason") or "").strip()
    qualify_reason = str(
        extra.pop("qualify_reason", trade.get("qualify_reason") or "")
    ).strip()
    invalidated_flag = bool(
        extra.pop(
            "invalidated_flag",
            qualify_reason == "invalidated"
            or bool(trade.get("invalidation_reasons"))
            or selected_reason == "invalidated",
        )
    )
    snapshot_risk_flags = list((snapshot or {}).get("risk_flags", []) or [])
    trade_risk_flags = list(trade.get("risk_flags", []) or [])
    risk_flags = []
    for flag in [*snapshot_risk_flags, *trade_risk_flags]:
        if flag not in risk_flags:
            risk_flags.append(flag)
    snapshot_invalidations = list((snapshot or {}).get("invalidation_reasons", []) or [])
    trade_invalidations = list(trade.get("invalidation_reasons", []) or [])
    invalidation_reasons = []
    for reason in [*snapshot_invalidations, *trade_invalidations]:
        if reason not in invalidation_reasons:
            invalidation_reasons.append(reason)

    normalized_candidates: list[str] = []
    for reason in candidate_reasons:
        reason = str(reason or "").strip()
        if reason and reason not in normalized_candidates:
            normalized_candidates.append(reason)

    trade["_pending_close_decision"] = {
        "path": str(extra.pop("path", source)).strip() or source,
        "source": source,
        "reason_selected": selected_reason,
        "reason_candidates": normalized_candidates,
        "current_pnl_pct": round(float(raw_current_pnl or 0.0), 4),
        "current_market_cap_sol": round(float(raw_current_mcap or 0.0), 6),
        "max_pnl_pct": round(float(extra.pop("max_pnl_pct", trade.get("max_pnl_pct_proxy", raw_current_pnl) or raw_current_pnl or 0.0)), 4),
        "winner_floor_pct": (
            round(float(extra.pop("winner_floor_pct")), 4)
            if extra.get("winner_floor_pct") is not None
            else None
        ),
        "pullback_pct": round(float(extra.pop("pullback_pct", 0.0) or 0.0), 4),
        "min_hold_active": bool(extra.pop("min_hold_active", False)),
        "trade_age_seconds": round(float(extra.pop("trade_age_seconds", _trade_age_seconds(trade)) or 0.0), 4),
        "idle_seconds": (
            round(float(extra.pop("idle_seconds")), 4)
            if extra.get("idle_seconds") is not None
            else None
        ),
        "qualify_reason": qualify_reason,
        "reason_before_close": reason_before_close,
        "reclaim_mode": str(trade.get("confirmation_mode") or "").strip(),
        "invalidated_flag": invalidated_flag,
        "priority_tier": str(((snapshot or {}).get("priority_tier") or trade.get("priority_tier") or "")).strip(),
        "setup_state": str(((snapshot or {}).get("setup_state") or trade.get("setup_state") or "")).strip(),
        "current_score": round(float(((snapshot or {}).get("composite_score", trade.get("current_score", 0.0)) or 0.0)), 4),
        "alert_action": str(((alert or {}).get("action") or "")).strip(),
        "risk_flags": risk_flags,
        "invalidation_reasons": invalidation_reasons,
        "profit_protected": bool(extra.pop("profit_protected", False)),
        "early_hold_active": bool(extra.pop("early_hold_active", False)),
        "metadata": extra,
    }

def _winner_lock_threshold(max_pnl_pct: float) -> tuple[float, float] | None:
    if max_pnl_pct >= PAPER_WINNER_LOCK_STAGE3_MAX_PNL_PCT:
        return (
            PAPER_WINNER_LOCK_STAGE3_FLOOR_PNL_PCT,
            PAPER_WINNER_LOCK_STAGE3_MIN_PULLBACK_PCT,
        )
    if max_pnl_pct >= PAPER_WINNER_LOCK_STAGE2_MAX_PNL_PCT:
        return (
            PAPER_WINNER_LOCK_STAGE2_FLOOR_PNL_PCT,
            PAPER_WINNER_LOCK_STAGE2_MIN_PULLBACK_PCT,
        )
    if max_pnl_pct >= PAPER_WINNER_LOCK_STAGE1_MAX_PNL_PCT:
        return (
            PAPER_WINNER_LOCK_STAGE1_FLOOR_PNL_PCT,
            PAPER_WINNER_LOCK_STAGE1_MIN_PULLBACK_PCT,
        )
    return None


def _winner_lock_breached(
    trade: dict[str, Any],
    pnl_pct: float | None = None,
    peak_mcap: float | None = None,
    current_mcap: float | None = None,
) -> tuple[bool, float | None, float, float]:
    if pnl_pct is None:
        pnl_pct = _current_trade_pnl_pct(trade)
    max_pnl_pct = float(trade.get("max_pnl_pct_proxy", pnl_pct) or pnl_pct)
    threshold = _winner_lock_threshold(max_pnl_pct)
    if threshold is None:
        return False, None, max_pnl_pct, 0.0

    floor_pnl_pct, min_pullback_pct = threshold
    pullback_from_peak = 0.0

    if peak_mcap is not None and current_mcap is not None and peak_mcap > 0:
        pullback_from_peak = max(((peak_mcap - current_mcap) / peak_mcap) * 100.0, 0.0)
    else:
        peak_market_cap_sol = float(trade.get("peak_market_cap_sol", 0.0) or 0.0)
        current_market_cap_sol = float(
            current_mcap
            if current_mcap is not None
            else trade.get("current_market_cap_sol", 0.0) or 0.0
        )
        if peak_market_cap_sol > 0 and current_market_cap_sol > 0:
            pullback_from_peak = max(
                ((peak_market_cap_sol - current_market_cap_sol) / peak_market_cap_sol) * 100.0,
                0.0,
            )
        elif max_pnl_pct > -100.0:
            peak_value = 100.0 + max_pnl_pct
            current_value = 100.0 + pnl_pct
            if peak_value > 0 and current_value > 0:
                pullback_from_peak = max(
                    ((peak_value - current_value) / peak_value) * 100.0,
                    0.0,
                )

    breached = pnl_pct <= floor_pnl_pct and pullback_from_peak >= min_pullback_pct
    return breached, floor_pnl_pct, max_pnl_pct, pullback_from_peak


def _profit_protected_trade(trade: dict[str, Any], pnl_pct: float | None = None) -> bool:
    if pnl_pct is None:
        pnl_pct = _current_trade_pnl_pct(trade)
    max_pnl_pct = float(trade.get("max_pnl_pct_proxy", pnl_pct) or pnl_pct)
    winner_lock_breached, _, _, _ = _winner_lock_breached(trade, pnl_pct)
    if winner_lock_breached:
        return False
    return bool(
        pnl_pct >= PAPER_INVALIDATION_GREEN_HOLD_PNL_PCT
        or (
            max_pnl_pct >= PAPER_INVALIDATION_GREEN_HOLD_MAX_PNL_PCT
            and pnl_pct >= PAPER_INVALIDATION_GREEN_GIVEBACK_FLOOR_PCT
        )
    )


def _recently_closed_trade(
    closed_trades: list[dict[str, Any]],
    mint: str,
) -> dict[str, Any] | None:
    for trade in reversed(closed_trades):
        if str(trade.get("mint") or "") == mint:
            return trade
    return None


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _confidence_rank(value: str | None) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(str(value or "").lower(), 0)


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(float(denominator or 0.0)) < 1e-9:
        return default
    return float(numerator or 0.0) / float(denominator or 0.0)


def _cohort_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    return snapshot.get("helius_wallet_cohort_summary", {}) or {}


def _quant_features(snapshot: dict[str, Any]) -> dict[str, Any]:
    return snapshot.get("quant_features", {}) or {}


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


def _token_cohort_cache_payload(mint: str) -> dict[str, Any]:
    mint = str(mint or "").strip()
    if not mint:
        return {}
    path = TOKEN_COHORT_CACHE_DIR / f"{mint}.json"
    payload = _load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _snapshot_wallet_memory_funder(snapshot: dict[str, Any]) -> str:
    creator_profile = snapshot.get("helius_creator_profile", {}) or {}
    return str(
        snapshot.get("wallet_memory_funder_wallet")
        or creator_profile.get("top_funder")
        or ""
    ).strip()


def _extract_trade_participant_wallets(trade: dict[str, Any]) -> list[str]:
    ordered: list[str] = []

    for wallet in list(trade.get("helius_selected_wallets") or []):
        wallet = str(wallet or "").strip()
        if wallet and wallet not in ordered:
            ordered.append(wallet)

    if ordered:
        return ordered[:12]

    cohort_payload = _token_cohort_cache_payload(str(trade.get("mint") or ""))
    for wallet in list(cohort_payload.get("helius_selected_wallets") or []):
        wallet = str(wallet or "").strip()
        if wallet and wallet not in ordered:
            ordered.append(wallet)

    return ordered[:12]


def _extract_trade_funder_wallet(trade: dict[str, Any]) -> str | None:
    stored = str(trade.get("wallet_memory_funder_wallet") or "").strip()
    if stored:
        return stored

    cohort_payload = _token_cohort_cache_payload(str(trade.get("mint") or ""))
    creator_profile = cohort_payload.get("helius_creator_profile", {}) or {}
    funder = str(creator_profile.get("top_funder") or "").strip()
    return funder or None


def _extract_trade_creator_wallet(trade: dict[str, Any]) -> str | None:
    stored = str(trade.get("creator_wallet") or "").strip()
    if stored:
        return stored

    cohort_payload = _token_cohort_cache_payload(str(trade.get("mint") or ""))
    creator_wallet = str(cohort_payload.get("creator_wallet") or "").strip()
    return creator_wallet or None


def _record_closed_trade_outcome(trade: dict[str, Any]) -> bool:
    engine = _get_wallet_intelligence_engine()
    mint = str(trade.get("mint") or "").strip()
    wallets = _extract_trade_participant_wallets(trade)

    if engine is None or not mint or not wallets:
        return False

    try:
        return bool(
            engine.record_closed_paper_trade(
                mint=mint,
                participant_wallets=wallets,
                pnl_pct=float(trade.get("pnl_pct_proxy", 0.0) or 0.0),
                max_pnl_pct=float(trade.get("max_pnl_pct_proxy", 0.0) or 0.0),
                min_pnl_pct=float(trade.get("min_pnl_pct_proxy", 0.0) or 0.0),
                exit_reason=str(trade.get("exit_reason") or "").strip() or None,
                resolved_at=str(trade.get("closed_at_utc") or "").strip() or None,
            )
        )
    except Exception:
        return False


def _record_closed_trade_creator_entity_outcome(trade: dict[str, Any]) -> bool:
    engine = _get_creator_entity_intelligence_engine()
    mint = str(trade.get("mint") or "").strip()
    creator_wallet = _extract_trade_creator_wallet(trade)
    first_hop_funder = _extract_trade_funder_wallet(trade)

    if engine is None or not mint or (not creator_wallet and not first_hop_funder):
        return False

    try:
        entity_key = engine.record_closed_paper_trade(
            mint=mint,
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
            pnl_pct=float(trade.get("pnl_pct_proxy", 0.0) or 0.0),
            max_pnl_pct=float(trade.get("max_pnl_pct_proxy", 0.0) or 0.0),
            min_pnl_pct=float(trade.get("min_pnl_pct_proxy", 0.0) or 0.0),
            exit_reason=str(trade.get("exit_reason") or "").strip() or None,
            resolved_at=str(trade.get("closed_at_utc") or "").strip() or None,
        )
        if entity_key:
            trade["creator_entity_key"] = entity_key
            return True
        return False
    except Exception:
        return False


def _research_confidence(snapshot: dict[str, Any]) -> tuple[str, float]:
    quant_features = _quant_features(snapshot)
    cohort_summary = _cohort_summary(snapshot)

    tier = str(snapshot.get("helius_enrichment_tier", "none") or "none")
    status = str(snapshot.get("helius_enrichment_status", "unknown") or "unknown")
    completed_wallet_count = int(snapshot.get("helius_completed_wallet_count", 0) or 0)
    profile_depth_bucket = str(
        snapshot.get("helius_profile_depth_bucket", "none") or "none"
    )
    partial_enrichment = bool(snapshot.get("helius_partial_enrichment", False))
    cohort_quality_score = float(
        quant_features.get(
            "helius_cohort_quality_score",
            cohort_summary.get("cohort_quality_score", 0.0),
        )
        or 0.0
    )
    creator_shared_funder_score = float(
        quant_features.get(
            "helius_creator_shared_funder_score",
            cohort_summary.get("creator_shared_funder_score", 0.0),
        )
        or 0.0
    )
    recycled_wallet_share = float(
        quant_features.get(
            "helius_recycled_wallet_share",
            cohort_summary.get("recycled_wallet_share", 0.0),
        )
        or 0.0
    )
    sniper_wallet_share = float(
        quant_features.get(
            "helius_sniper_wallet_share",
            cohort_summary.get("sniper_wallet_share", 0.0),
        )
        or 0.0
    )

    if tier in {"none", ""} or status in {"disabled", "unqualified", "unknown"}:
        return "local_only", 0.20

    if status == "collector_timeout":
        return "local_timeout", 0.18

    if tier == "light":
        if partial_enrichment or status == "light_partial":
            return "light_partial", 0.40
        return "light_enriched", 0.52

    if tier == "deep":
        if status == "deep_enriched" and completed_wallet_count >= 5:
            if (
                cohort_quality_score >= 0.62
                and creator_shared_funder_score <= 0.25
                and recycled_wallet_share <= 0.35
                and sniper_wallet_share <= 0.45
            ):
                return "deep_high_quality", 0.88
            return "deep_enriched", 0.78

        if status == "deep_enriched" and profile_depth_bucket == "2_4":
            return "deep_mid_depth", 0.70

        if partial_enrichment or status == "deep_partial":
            if completed_wallet_count >= 2:
                return "deep_partial_useful", 0.62
            if completed_wallet_count == 1:
                return "deep_partial_shallow", 0.52
            return "deep_partial_empty", 0.32

        if status == "creator_only":
            return "creator_only", 0.36

    return "uncertain", 0.30


def _entry_threshold(snapshot: dict[str, Any]) -> tuple[bool, str]:
    setup_state = str(snapshot.get("setup_state") or "")
    priority_tier = str(snapshot.get("priority_tier") or "")
    score = float(snapshot.get("composite_score", 0.0) or 0.0)
    entry_confirmation = float(snapshot.get("entry_confirmation_score", 0.0) or 0.0)
    participant_quality = float(
        (_quant_features(snapshot)).get("participant_quality_score_v2", 0.0) or 0.0
    )
    dust_trade_share_1m = float(
        (_quant_features(snapshot)).get("dust_trade_share_1m", 0.0) or 0.0
    )
    invalidations = set(snapshot.get("invalidation_reasons", []) or [])
    risk_flags = set(snapshot.get("risk_flags", []) or [])

    if invalidations:
        return False, "invalidated"
    if "negative_net_flow" in risk_flags:
        return False, "negative_net_flow"
    if "inactive" in risk_flags:
        return False, "inactive"
    if dust_trade_share_1m >= 0.35:
        return False, "dust_heavy"

    if setup_state == "paper_entry":
        return True, "paper_entry_ready"
    if priority_tier == "alpha" and score >= 88 and entry_confirmation >= 0.58:
        return True, "alpha_ready"
    if (
        priority_tier in {"alpha", "priority"}
        and score >= 76
        and entry_confirmation >= 0.54
        and participant_quality >= 0.45
    ):
        return True, "priority_confirmed"
    return False, "below_threshold"


def _reclaim_threshold(snapshot: dict[str, Any]) -> tuple[bool, str]:
    strategy_name = str(snapshot.get("strategy_name") or "")
    primary_archetype = str(snapshot.get("primary_archetype") or "")
    regime_tags = set(snapshot.get("regime_tags", []) or [])
    score = float(snapshot.get("composite_score", 0.0) or 0.0)
    mcap_to_peak = float(
        (_quant_features(snapshot)).get("mcap_to_peak_ratio", 0.0) or 0.0
    )
    participant_quality = float(
        (_quant_features(snapshot)).get("participant_quality_score_v2", 0.0) or 0.0
    )

    if strategy_name == "reclaim_continuation":
        return True, "strategy_reclaim"
    if primary_archetype == "REVIVAL_RECLAIM":
        return True, "archetype_reclaim"
    if "RECLAIM_CLEAN" in regime_tags and score >= 70 and mcap_to_peak >= 0.72:
        return True, "clean_reclaim"
    if score >= 78 and mcap_to_peak >= 0.80 and participant_quality >= 0.48:
        return True, "generic_reclaim"
    return False, "not_reclaim"


def _build_trade(snapshot: dict[str, Any], alert: dict[str, Any]) -> dict[str, Any]:
    quant_features = _quant_features(snapshot)
    cohort_summary = _cohort_summary(snapshot)
    research_confidence_label, research_confidence_score = _research_confidence(snapshot)

    entry_mcap = float(snapshot.get("current_market_cap_sol", 0.0) or 0.0)
    helius_requested_wallet_count = int(
        snapshot.get("helius_requested_wallet_count", 0) or 0
    )
    helius_completed_wallet_count = int(
        snapshot.get("helius_completed_wallet_count", 0) or 0
    )
    helius_profile_completion_ratio = float(
        snapshot.get(
            "helius_profile_completion_ratio",
            _safe_div(helius_completed_wallet_count, helius_requested_wallet_count, 0.0),
        )
        or 0.0
    )
    helius_profile_depth_bucket = str(
        snapshot.get("helius_profile_depth_bucket", "none") or "none"
    )

    return {
        "mint": snapshot.get("mint"),
        "symbol": snapshot.get("symbol"),
        "name": snapshot.get("name"),
        "opened_at_utc": _now(),
        "updated_at_utc": _now(),
        "closed_at_utc": None,
        "status": "open",
        "entry_market_cap_sol": entry_mcap,
        "current_market_cap_sol": entry_mcap,
        "peak_market_cap_sol": float(snapshot.get("peak_market_cap_sol", 0.0) or 0.0),
        "exit_market_cap_sol": None,
        "entry_score": float(snapshot.get("composite_score", 0.0) or 0.0),
        "current_score": float(snapshot.get("composite_score", 0.0) or 0.0),
        "local_entry_score": float(
            snapshot.get(
                "local_composite_score",
                snapshot.get("composite_score", 0.0),
            )
            or 0.0
        ),
        "enriched_score_delta_at_entry": float(
            snapshot.get("enriched_score_delta", 0.0) or 0.0
        ),
        "entry_confirmation_score": float(
            snapshot.get("entry_confirmation_score", 0.0) or 0.0
        ),
        "current_entry_confirmation_score": float(
            snapshot.get("entry_confirmation_score", 0.0) or 0.0
        ),
        "priority_tier": snapshot.get("priority_tier"),
        "confidence": snapshot.get("confidence"),
        "strategy_name": snapshot.get("strategy_name"),
        "primary_archetype": snapshot.get("primary_archetype"),
        "secondary_archetype": snapshot.get("secondary_archetype"),
        "setup_state": snapshot.get("setup_state"),
        "entry_bias": snapshot.get("entry_bias"),
        "analysis_mode": snapshot.get("analysis_mode", "full"),
        "research_confidence_label_at_entry": research_confidence_label,
        "research_confidence_score_at_entry": round(research_confidence_score, 4),
        "current_research_confidence_label": research_confidence_label,
        "current_research_confidence_score": round(research_confidence_score, 4),
        "helius_enrichment_tier": snapshot.get("helius_enrichment_tier", "none"),
        "helius_enrichment_status": snapshot.get("helius_enrichment_status", "unknown"),
        "helius_trigger_reason": snapshot.get("helius_trigger_reason"),
        "helius_last_error": snapshot.get("helius_last_error"),
        "helius_cached_error": snapshot.get("helius_cached_error"),
        "helius_wallet_target": int(snapshot.get("helius_wallet_target", 0) or 0),
        "helius_selected_wallet_count": int(
            snapshot.get("helius_selected_wallet_count", 0) or 0
        ),
        "helius_profiled_wallet_count": int(
            snapshot.get("helius_profiled_wallet_count", 0) or 0
        ),
        "helius_requested_wallet_count": helius_requested_wallet_count,
        "helius_completed_wallet_count": helius_completed_wallet_count,
        "helius_profile_depth_bucket": helius_profile_depth_bucket,
        "helius_profile_completion_ratio": round(
            helius_profile_completion_ratio,
            4,
        ),
        "helius_partial_enrichment": bool(
            snapshot.get("helius_partial_enrichment", False)
        ),
        "helius_creator_attempted": bool(
            snapshot.get("helius_creator_attempted", False)
        ),
        "helius_creator_profiled": bool(
            snapshot.get("helius_creator_profiled", False)
        ),
        "helius_time_budget_seconds": float(
            snapshot.get("helius_time_budget_seconds", 0.0) or 0.0
        ),
        "helius_time_budget_exhausted": bool(
            snapshot.get("helius_time_budget_exhausted", False)
        ),
        "helius_wallet_attempt_details": list(
            snapshot.get("helius_wallet_attempt_details", []) or []
        ),
        "helius_selected_wallets": list(
            snapshot.get("helius_selected_wallets", []) or []
        ),
        "creator_wallet": str(snapshot.get("creator_wallet", "") or ""),
        "wallet_memory_funder_wallet": _snapshot_wallet_memory_funder(snapshot),
        "helius_cohort_quality_score": float(
            quant_features.get(
                "helius_cohort_quality_score",
                cohort_summary.get("cohort_quality_score", 0.0),
            )
            or 0.0
        ),
        "helius_fresh_wallet_share": float(
            quant_features.get(
                "helius_fresh_wallet_share",
                cohort_summary.get("fresh_wallet_share", 0.0),
            )
            or 0.0
        ),
        "helius_sniper_wallet_share": float(
            quant_features.get(
                "helius_sniper_wallet_share",
                cohort_summary.get("sniper_wallet_share", 0.0),
            )
            or 0.0
        ),
        "helius_recycled_wallet_share": float(
            quant_features.get(
                "helius_recycled_wallet_share",
                cohort_summary.get("recycled_wallet_share", 0.0),
            )
            or 0.0
        ),
        "helius_funding_diversity_score": float(
            quant_features.get(
                "helius_funding_diversity_score",
                cohort_summary.get("funding_diversity_score", 0.0),
            )
            or 0.0
        ),
        "helius_creator_shared_funder_score": float(
            quant_features.get(
                "helius_creator_shared_funder_score",
                cohort_summary.get("creator_shared_funder_score", 0.0),
            )
            or 0.0
        ),
        "helius_profile_completion_confidence": float(
            cohort_summary.get("profile_completion_confidence", 0.0) or 0.0
        ),
        "participant_quality_score_v2": float(
            quant_features.get("participant_quality_score_v2", 0.0) or 0.0
        ),
        "wallet_novelty_score": float(
            quant_features.get("wallet_novelty_score", 0.0) or 0.0
        ),
        "repeat_wallet_ratio": float(
            quant_features.get("repeat_wallet_ratio", 0.0) or 0.0
        ),
        "buyer_overlap_ratio": float(
            quant_features.get("buyer_overlap_ratio", 0.0) or 0.0
        ),
        "seller_expansion_ratio": float(
            quant_features.get("seller_expansion_ratio", 0.0) or 0.0
        ),
        "mcap_stability_score": float(
            quant_features.get("mcap_stability_score", 0.0) or 0.0
        ),
        "recovery_ratio_from_trough": float(
            quant_features.get("recovery_ratio_from_trough", 0.0) or 0.0
        ),
        "dust_trade_share_1m": float(
            quant_features.get("dust_trade_share_1m", 0.0) or 0.0
        ),
        "dust_trade_share_5m": float(
            quant_features.get("dust_trade_share_5m", 0.0) or 0.0
        ),
        "why_now": list(snapshot.get("why_now", []) or []),
        "trigger_reasons": list(snapshot.get("trigger_reasons", []) or []),
        "risk_flags": list(snapshot.get("risk_flags", []) or []),
        "invalidation_reasons": list(snapshot.get("invalidation_reasons", []) or []),
        "entry_alert_action": alert.get("action"),
        "entry_alert_band": alert.get("band"),
        "entry_alert_score": float(alert.get("score", 0.0) or 0.0),
        "exit_reason": None,
        "pnl_pct_proxy": 0.0,
        "max_pnl_pct_proxy": 0.0,
        "min_pnl_pct_proxy": 0.0,
        "reclaim_confirmed": False,
        "confirmation_mode": "entry",
        "confirmation_progress": 1,
        "confirmation_required": 1,
        "qualify_reason": None,
        "reason": None,
        "wallet_memory_outcome_recorded": False,
        "creator_entity_outcome_recorded": False,
        "creator_entity_key": str(snapshot.get("creator_entity_key") or "").strip() or None,
        "creator_entity_quality_score": float(
            quant_features.get("creator_entity_quality_score", 0.5) or 0.5
        ),
        "creator_entity_confidence_score": float(
            quant_features.get("creator_entity_confidence_score", 0.0) or 0.0
        ),
        "creator_entity_launches_seen": float(
            quant_features.get("creator_entity_launch_count", 0.0) or 0.0
        ),
        "creator_entity_paper_trade_count": float(
            quant_features.get("creator_entity_paper_trade_count", 0.0) or 0.0
        ),
        "creator_entity_is_known": 1.0
        if bool(
            str(snapshot.get("creator_entity_key") or "").strip()
            or quant_features.get("creator_entity_launch_count", 0.0)
            or quant_features.get("creator_entity_creator_wallet_count", 0.0)
            or quant_features.get("creator_entity_funder_wallet_count", 0.0)
        )
        else 0.0,
    }


def _update_trade_metrics(trade: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    current_mcap = float(snapshot.get("current_market_cap_sol", 0.0) or 0.0)
    peak_mcap = float(snapshot.get("peak_market_cap_sol", 0.0) or 0.0)
    entry_mcap = float(trade.get("entry_market_cap_sol", 0.0) or 0.0)
    pnl_pct_proxy = (
        ((current_mcap - entry_mcap) / entry_mcap * 100.0)
        if entry_mcap > 0
        else 0.0
    )

    quant_features = _quant_features(snapshot)
    cohort_summary = _cohort_summary(snapshot)
    research_confidence_label, research_confidence_score = _research_confidence(snapshot)

    trade["updated_at_utc"] = _now()
    trade["current_market_cap_sol"] = current_mcap
    trade["peak_market_cap_sol"] = max(
        float(trade.get("peak_market_cap_sol", 0.0) or 0.0),
        peak_mcap,
        current_mcap,
    )
    trade["current_score"] = float(snapshot.get("composite_score", 0.0) or 0.0)
    trade["current_entry_confirmation_score"] = float(
        snapshot.get("entry_confirmation_score", 0.0) or 0.0
    )
    trade["priority_tier"] = snapshot.get("priority_tier")
    trade["confidence"] = snapshot.get("confidence")
    trade["setup_state"] = snapshot.get("setup_state")
    trade["analysis_mode"] = snapshot.get(
        "analysis_mode",
        trade.get("analysis_mode", "full"),
    )

    trade["current_research_confidence_label"] = research_confidence_label
    trade["current_research_confidence_score"] = round(research_confidence_score, 4)

    trade["helius_enrichment_tier"] = snapshot.get(
        "helius_enrichment_tier",
        trade.get("helius_enrichment_tier", "none"),
    )
    trade["helius_enrichment_status"] = snapshot.get(
        "helius_enrichment_status",
        trade.get("helius_enrichment_status", "unknown"),
    )
    trade["helius_trigger_reason"] = snapshot.get(
        "helius_trigger_reason",
        trade.get("helius_trigger_reason"),
    )
    trade["helius_last_error"] = snapshot.get(
        "helius_last_error",
        trade.get("helius_last_error"),
    )
    trade["helius_cached_error"] = snapshot.get(
        "helius_cached_error",
        trade.get("helius_cached_error"),
    )
    trade["helius_wallet_target"] = int(
        snapshot.get("helius_wallet_target", trade.get("helius_wallet_target", 0)) or 0
    )
    trade["helius_selected_wallet_count"] = int(
        snapshot.get(
            "helius_selected_wallet_count",
            trade.get("helius_selected_wallet_count", 0),
        )
        or 0
    )
    trade["helius_profiled_wallet_count"] = int(
        snapshot.get(
            "helius_profiled_wallet_count",
            trade.get("helius_profiled_wallet_count", 0),
        )
        or 0
    )
    trade["helius_requested_wallet_count"] = int(
        snapshot.get(
            "helius_requested_wallet_count",
            trade.get("helius_requested_wallet_count", 0),
        )
        or 0
    )
    trade["helius_completed_wallet_count"] = int(
        snapshot.get(
            "helius_completed_wallet_count",
            trade.get("helius_completed_wallet_count", 0),
        )
        or 0
    )
    trade["helius_profile_depth_bucket"] = str(
        snapshot.get(
            "helius_profile_depth_bucket",
            trade.get("helius_profile_depth_bucket", "none"),
        )
        or "none"
    )
    trade["helius_profile_completion_ratio"] = round(
        float(
            snapshot.get(
                "helius_profile_completion_ratio",
                trade.get("helius_profile_completion_ratio", 0.0),
            )
            or 0.0
        ),
        4,
    )
    trade["helius_partial_enrichment"] = bool(
        snapshot.get(
            "helius_partial_enrichment",
            trade.get("helius_partial_enrichment", False),
        )
    )
    trade["helius_creator_attempted"] = bool(
        snapshot.get(
            "helius_creator_attempted",
            trade.get("helius_creator_attempted", False),
        )
    )
    trade["helius_creator_profiled"] = bool(
        snapshot.get(
            "helius_creator_profiled",
            trade.get("helius_creator_profiled", False),
        )
    )
    trade["helius_time_budget_seconds"] = float(
        snapshot.get(
            "helius_time_budget_seconds",
            trade.get("helius_time_budget_seconds", 0.0),
        )
        or 0.0
    )
    trade["helius_time_budget_exhausted"] = bool(
        snapshot.get(
            "helius_time_budget_exhausted",
            trade.get("helius_time_budget_exhausted", False),
        )
    )
    trade["helius_wallet_attempt_details"] = list(
        snapshot.get(
            "helius_wallet_attempt_details",
            trade.get("helius_wallet_attempt_details", []),
        )
        or []
    )
    trade["helius_selected_wallets"] = list(
        snapshot.get(
            "helius_selected_wallets",
            trade.get("helius_selected_wallets", []),
        )
        or []
    )
    trade["creator_wallet"] = str(
        snapshot.get("creator_wallet", trade.get("creator_wallet", "")) or ""
    )
    trade["wallet_memory_funder_wallet"] = str(
        _snapshot_wallet_memory_funder(snapshot)
        or trade.get("wallet_memory_funder_wallet", "")
        or _extract_trade_funder_wallet(trade)
        or ""
    )
    trade["creator_entity_key"] = str(
        snapshot.get("creator_entity_key")
        or trade.get("creator_entity_key")
        or ""
    ).strip() or None
    trade["creator_entity_quality_score"] = float(
        quant_features.get(
            "creator_entity_quality_score",
            trade.get("creator_entity_quality_score", 0.5),
        )
        or 0.5
    )
    trade["creator_entity_confidence_score"] = float(
        quant_features.get(
            "creator_entity_confidence_score",
            trade.get("creator_entity_confidence_score", 0.0),
        )
        or 0.0
    )
    trade["creator_entity_launches_seen"] = float(
        quant_features.get(
            "creator_entity_launch_count",
            trade.get("creator_entity_launches_seen", 0.0),
        )
        or 0.0
    )
    trade["creator_entity_paper_trade_count"] = float(
        quant_features.get(
            "creator_entity_paper_trade_count",
            trade.get("creator_entity_paper_trade_count", 0.0),
        )
        or 0.0
    )
    trade["creator_entity_is_known"] = 1.0 if bool(
        trade.get("creator_entity_key")
        or trade.get("creator_entity_launches_seen", 0.0)
        or trade.get("creator_entity_paper_trade_count", 0.0)
    ) else 0.0

    trade["helius_cohort_quality_score"] = float(
        quant_features.get(
            "helius_cohort_quality_score",
            cohort_summary.get(
                "cohort_quality_score",
                trade.get("helius_cohort_quality_score", 0.0),
            ),
        )
        or 0.0
    )
    trade["helius_fresh_wallet_share"] = float(
        quant_features.get(
            "helius_fresh_wallet_share",
            cohort_summary.get(
                "fresh_wallet_share",
                trade.get("helius_fresh_wallet_share", 0.0),
            ),
        )
        or 0.0
    )
    trade["helius_sniper_wallet_share"] = float(
        quant_features.get(
            "helius_sniper_wallet_share",
            cohort_summary.get(
                "sniper_wallet_share",
                trade.get("helius_sniper_wallet_share", 0.0),
            ),
        )
        or 0.0
    )
    trade["helius_recycled_wallet_share"] = float(
        quant_features.get(
            "helius_recycled_wallet_share",
            cohort_summary.get(
                "recycled_wallet_share",
                trade.get("helius_recycled_wallet_share", 0.0),
            ),
        )
        or 0.0
    )
    trade["helius_funding_diversity_score"] = float(
        quant_features.get(
            "helius_funding_diversity_score",
            cohort_summary.get(
                "funding_diversity_score",
                trade.get("helius_funding_diversity_score", 0.0),
            ),
        )
        or 0.0
    )
    trade["helius_creator_shared_funder_score"] = float(
        quant_features.get(
            "helius_creator_shared_funder_score",
            cohort_summary.get(
                "creator_shared_funder_score",
                trade.get("helius_creator_shared_funder_score", 0.0),
            ),
        )
        or 0.0
    )
    trade["helius_profile_completion_confidence"] = float(
        cohort_summary.get(
            "profile_completion_confidence",
            trade.get("helius_profile_completion_confidence", 0.0),
        )
        or 0.0
    )

    trade["participant_quality_score_v2"] = float(
        quant_features.get(
            "participant_quality_score_v2",
            trade.get("participant_quality_score_v2", 0.0),
        )
        or 0.0
    )
    trade["wallet_novelty_score"] = float(
        quant_features.get(
            "wallet_novelty_score",
            trade.get("wallet_novelty_score", 0.0),
        )
        or 0.0
    )
    trade["repeat_wallet_ratio"] = float(
        quant_features.get(
            "repeat_wallet_ratio",
            trade.get("repeat_wallet_ratio", 0.0),
        )
        or 0.0
    )
    trade["buyer_overlap_ratio"] = float(
        quant_features.get(
            "buyer_overlap_ratio",
            trade.get("buyer_overlap_ratio", 0.0),
        )
        or 0.0
    )
    trade["seller_expansion_ratio"] = float(
        quant_features.get(
            "seller_expansion_ratio",
            trade.get("seller_expansion_ratio", 0.0),
        )
        or 0.0
    )
    trade["mcap_stability_score"] = float(
        quant_features.get(
            "mcap_stability_score",
            trade.get("mcap_stability_score", 0.0),
        )
        or 0.0
    )
    trade["recovery_ratio_from_trough"] = float(
        quant_features.get(
            "recovery_ratio_from_trough",
            trade.get("recovery_ratio_from_trough", 0.0),
        )
        or 0.0
    )
    trade["dust_trade_share_1m"] = float(
        quant_features.get(
            "dust_trade_share_1m",
            trade.get("dust_trade_share_1m", 0.0),
        )
        or 0.0
    )
    trade["dust_trade_share_5m"] = float(
        quant_features.get(
            "dust_trade_share_5m",
            trade.get("dust_trade_share_5m", 0.0),
        )
        or 0.0
    )

    trade["risk_flags"] = list(
        snapshot.get("risk_flags", trade.get("risk_flags", [])) or []
    )
    trade["invalidation_reasons"] = list(
        snapshot.get(
            "invalidation_reasons",
            trade.get("invalidation_reasons", []),
        )
        or []
    )
    trade["trigger_reasons"] = list(
        snapshot.get("trigger_reasons", trade.get("trigger_reasons", [])) or []
    )

    trade["pnl_pct_proxy"] = round(pnl_pct_proxy, 4)
    trade["max_pnl_pct_proxy"] = round(
        max(
            float(trade.get("max_pnl_pct_proxy", pnl_pct_proxy) or pnl_pct_proxy),
            pnl_pct_proxy,
        ),
        4,
    )
    trade["min_pnl_pct_proxy"] = round(
        min(
            float(trade.get("min_pnl_pct_proxy", pnl_pct_proxy) or pnl_pct_proxy),
            pnl_pct_proxy,
        ),
        4,
    )
    return trade



def _should_close(
    trade: dict[str, Any],
    snapshot: dict[str, Any],
    alert: dict[str, Any],
) -> tuple[bool, str | None]:
    score = float(snapshot.get("composite_score", 0.0) or 0.0)
    current_mcap = float(snapshot.get("current_market_cap_sol", 0.0) or 0.0)
    entry_mcap = float(trade.get("entry_market_cap_sol", 0.0) or 0.0)
    peak_mcap = max(
        float(trade.get("peak_market_cap_sol", 0.0) or 0.0),
        float(snapshot.get("peak_market_cap_sol", 0.0) or 0.0),
    )
    priority_tier = str(snapshot.get("priority_tier") or "")
    setup_state = str(snapshot.get("setup_state") or "")
    risk_flags = set(snapshot.get("risk_flags", []) or [])
    invalidations = set(snapshot.get("invalidation_reasons", []) or [])
    dust_trade_share_1m = float(
        (_quant_features(snapshot)).get("dust_trade_share_1m", 0.0) or 0.0
    )
    participant_quality = float(
        (_quant_features(snapshot)).get("participant_quality_score_v2", 0.0) or 0.0
    )
    trade_age_seconds = _trade_age_seconds(trade)
    early_hold_active = trade_age_seconds < PAPER_MIN_HOLD_SECONDS

    candidate_reasons: list[str] = []
    pnl_pct = 0.0
    pullback_from_peak = 0.0
    winner_lock_breached = False
    winner_lock_floor_pnl_pct: float | None = None
    winner_lock_max_pnl_pct = 0.0
    winner_lock_pullback_pct = 0.0
    if entry_mcap > 0:
        pnl_pct = ((current_mcap - entry_mcap) / entry_mcap) * 100.0
        pullback_from_peak = (
            ((peak_mcap - current_mcap) / peak_mcap) * 100.0
            if peak_mcap > 0
            else 0.0
        )
        (
            winner_lock_breached,
            winner_lock_floor_pnl_pct,
            winner_lock_max_pnl_pct,
            winner_lock_pullback_pct,
        ) = _winner_lock_breached(
            trade,
            pnl_pct=pnl_pct,
            peak_mcap=peak_mcap,
            current_mcap=current_mcap,
        )

        if pnl_pct <= -18.0:
            candidate_reasons.append("hard_stop")
        if pnl_pct >= 30.0 and pullback_from_peak >= 16.0:
            candidate_reasons.append("trail_stop")
        if pnl_pct >= 60.0 and pullback_from_peak >= 12.0:
            candidate_reasons.append("protect_winner")
        if winner_lock_breached and not early_hold_active:
            candidate_reasons.append("winner_floor_stop")
        if priority_tier == "background" and setup_state == "research" and pnl_pct >= 8.0:
            candidate_reasons.append("deprioritized_after_gain")

        for immediate_reason in (
            "hard_stop",
            "trail_stop",
            "protect_winner",
            "winner_floor_stop",
            "deprioritized_after_gain",
        ):
            if immediate_reason not in candidate_reasons:
                continue
            if immediate_reason == "winner_floor_stop":
                trade["winner_lock_floor_pnl_pct"] = float(winner_lock_floor_pnl_pct or 0.0)
                trade["winner_lock_max_pnl_pct"] = float(winner_lock_max_pnl_pct or 0.0)
                trade["winner_lock_pullback_pct"] = float(winner_lock_pullback_pct or 0.0)
            _set_close_decision_trace(
                trade,
                source="sync",
                selected_reason=immediate_reason,
                candidate_reasons=candidate_reasons,
                snapshot=snapshot,
                alert=alert,
                extra={
                    "path": "sync_immediate",
                    "raw_current_pnl_pct": pnl_pct,
                    "raw_current_market_cap_sol": current_mcap,
                    "pullback_pct": pullback_from_peak if immediate_reason != "winner_floor_stop" else winner_lock_pullback_pct,
                    "winner_floor_pct": winner_lock_floor_pnl_pct,
                    "max_pnl_pct": winner_lock_max_pnl_pct or float(trade.get("max_pnl_pct_proxy", pnl_pct) or pnl_pct),
                    "min_hold_active": early_hold_active,
                    "trade_age_seconds": trade_age_seconds,
                    "early_hold_active": early_hold_active,
                    "qualify_reason": str(trade.get("qualify_reason") or "").strip(),
                },
            )
            return True, immediate_reason

    soft_exit_reason: str | None = None
    if invalidations:
        soft_exit_reason = "invalidated"
    elif alert.get("action") == "reject":
        soft_exit_reason = "alert_reject"
    elif "negative_net_flow" in risk_flags and score < 72:
        soft_exit_reason = "negative_flow"
    elif "deep_retrace" in risk_flags:
        soft_exit_reason = "deep_retrace"
    elif "repeat_wallet_churn" in risk_flags and score < 74:
        soft_exit_reason = "repeat_wallet_churn"
    elif "creator_shared_funder" in risk_flags and score < 74:
        soft_exit_reason = "creator_shared_funder"
    elif dust_trade_share_1m >= 0.40 and score < 78:
        soft_exit_reason = "dust_microflow"
    elif participant_quality < 0.25 and score < 72:
        soft_exit_reason = "participant_quality_breakdown"
    elif score < 48 and priority_tier == "background":
        soft_exit_reason = "score_decay"

    if soft_exit_reason:
        candidate_reasons.append(soft_exit_reason)

        if early_hold_active:
            return False, None

        max_pnl_pct = float(trade.get("max_pnl_pct_proxy", pnl_pct) or pnl_pct)
        profit_protected = False
        selected_reason = soft_exit_reason
        if soft_exit_reason == "invalidated":
            invalidation_green_hold = (
                pnl_pct >= PAPER_INVALIDATION_GREEN_HOLD_PNL_PCT
                or (
                    max_pnl_pct >= PAPER_INVALIDATION_GREEN_HOLD_MAX_PNL_PCT
                    and pnl_pct >= PAPER_INVALIDATION_GREEN_GIVEBACK_FLOOR_PCT
                )
            )
            if invalidation_green_hold:
                return False, None
            if winner_lock_breached:
                selected_reason = "winner_floor_stop"
                if selected_reason not in candidate_reasons:
                    candidate_reasons.append(selected_reason)
                trade["winner_lock_floor_pnl_pct"] = float(winner_lock_floor_pnl_pct or 0.0)
                trade["winner_lock_max_pnl_pct"] = float(winner_lock_max_pnl_pct or 0.0)
                trade["winner_lock_pullback_pct"] = float(winner_lock_pullback_pct or 0.0)
            else:
                profit_protected = _profit_protected_trade(trade, pnl_pct)

        _set_close_decision_trace(
            trade,
            source="sync",
            selected_reason=selected_reason,
            candidate_reasons=candidate_reasons,
            snapshot=snapshot,
            alert=alert,
            extra={
                "path": "sync_soft",
                "raw_current_pnl_pct": pnl_pct,
                "raw_current_market_cap_sol": current_mcap,
                "winner_floor_pct": winner_lock_floor_pnl_pct,
                "max_pnl_pct": max_pnl_pct,
                "pullback_pct": pullback_from_peak if winner_lock_pullback_pct == 0.0 else winner_lock_pullback_pct,
                "min_hold_active": early_hold_active,
                "trade_age_seconds": trade_age_seconds,
                "early_hold_active": early_hold_active,
                "qualify_reason": str(trade.get("qualify_reason") or "").strip(),
                "profit_protected": profit_protected,
                "invalidated_flag": bool(invalidations),
            },
        )
        return True, selected_reason

    return False, None



def _finalize_close(
    trade: dict[str, Any],
    snapshot: dict[str, Any] | None,
    closed_trades: list[dict[str, Any]],
    open_trades: dict[str, dict[str, Any]],
    mint: str,
    exit_reason: str,
) -> dict[str, Any]:
    closed_trade = dict(trade)

    if snapshot is not None:
        closed_trade["current_market_cap_sol"] = float(
            snapshot.get(
                "current_market_cap_sol",
                closed_trade.get("current_market_cap_sol", 0.0),
            )
            or 0.0
        )
        closed_trade["current_score"] = float(
            snapshot.get("composite_score", closed_trade.get("current_score", 0.0))
            or 0.0
        )

    pending_decision = dict(closed_trade.pop("_pending_close_decision", {}) or {})
    pre_apply_current_market_cap_sol = float(
        closed_trade.get("current_market_cap_sol", 0.0) or 0.0
    )
    pre_apply_pnl_pct_proxy = float(closed_trade.get("pnl_pct_proxy", 0.0) or 0.0)

    closed_trade = _apply_winner_lock_exit_floor(closed_trade, exit_reason)

    if pending_decision:
        pending_decision["reason_selected"] = exit_reason
        pending_decision["raw_exit_market_cap_sol"] = round(pre_apply_current_market_cap_sol, 6)
        pending_decision["raw_exit_pnl_pct_proxy"] = round(pre_apply_pnl_pct_proxy, 4)
        pending_decision["final_exit_market_cap_sol"] = round(
            float(closed_trade.get("current_market_cap_sol", 0.0) or 0.0), 6
        )
        pending_decision["final_exit_pnl_pct_proxy"] = round(
            float(closed_trade.get("pnl_pct_proxy", 0.0) or 0.0), 4
        )
        pending_decision["winner_lock_floor_applied"] = bool(
            closed_trade.get("winner_lock_floor_applied", False)
        )
        closed_trade["close_decision_path"] = str(
            pending_decision.get("path") or pending_decision.get("source") or ""
        ).strip()
        closed_trade["close_decision_source"] = str(
            pending_decision.get("source") or ""
        ).strip()
        closed_trade["close_decision_reason_selected"] = str(
            pending_decision.get("reason_selected") or exit_reason
        ).strip()
        closed_trade["close_decision_reason_candidates"] = list(
            pending_decision.get("reason_candidates", []) or []
        )
        closed_trade["close_decision_current_pnl_pct"] = float(
            pending_decision.get("current_pnl_pct", pre_apply_pnl_pct_proxy) or 0.0
        )
        closed_trade["close_decision_current_market_cap_sol"] = float(
            pending_decision.get("current_market_cap_sol", pre_apply_current_market_cap_sol)
            or 0.0
        )
        closed_trade["close_decision_max_pnl_pct"] = float(
            pending_decision.get("max_pnl_pct", closed_trade.get("max_pnl_pct_proxy", 0.0))
            or 0.0
        )
        winner_floor_pct = pending_decision.get("winner_floor_pct")
        closed_trade["close_decision_winner_floor_pct"] = (
            float(winner_floor_pct)
            if winner_floor_pct is not None
            else None
        )
        closed_trade["close_decision_pullback_pct"] = float(
            pending_decision.get("pullback_pct", 0.0) or 0.0
        )
        closed_trade["close_decision_min_hold_active"] = bool(
            pending_decision.get("min_hold_active", False)
        )
        closed_trade["close_decision_trade_age_seconds"] = float(
            pending_decision.get("trade_age_seconds", 0.0) or 0.0
        )
        idle_seconds = pending_decision.get("idle_seconds")
        closed_trade["close_decision_idle_seconds"] = (
            float(idle_seconds) if idle_seconds is not None else None
        )
        closed_trade["close_decision_qualify_reason"] = str(
            pending_decision.get("qualify_reason") or ""
        ).strip()
        closed_trade["close_decision_reason_before_close"] = str(
            pending_decision.get("reason_before_close") or ""
        ).strip()
        closed_trade["close_decision_reclaim_mode"] = str(
            pending_decision.get("reclaim_mode") or ""
        ).strip()
        closed_trade["close_decision_invalidated_flag"] = bool(
            pending_decision.get("invalidated_flag", False)
        )
        closed_trade["close_decision_priority_tier"] = str(
            pending_decision.get("priority_tier") or ""
        ).strip()
        closed_trade["close_decision_setup_state"] = str(
            pending_decision.get("setup_state") or ""
        ).strip()
        closed_trade["close_decision_current_score"] = float(
            pending_decision.get("current_score", closed_trade.get("current_score", 0.0))
            or 0.0
        )
        closed_trade["close_decision_alert_action"] = str(
            pending_decision.get("alert_action") or ""
        ).strip()
        closed_trade["close_decision_risk_flags"] = list(
            pending_decision.get("risk_flags", []) or []
        )
        closed_trade["close_decision_invalidation_reasons"] = list(
            pending_decision.get("invalidation_reasons", []) or []
        )
        closed_trade["close_decision_profit_protected"] = bool(
            pending_decision.get("profit_protected", False)
        )
        closed_trade["close_decision_early_hold_active"] = bool(
            pending_decision.get("early_hold_active", False)
        )
        closed_trade["close_decision_raw_exit_market_cap_sol"] = float(
            pending_decision.get("raw_exit_market_cap_sol", pre_apply_current_market_cap_sol)
            or 0.0
        )
        closed_trade["close_decision_raw_exit_pnl_pct_proxy"] = float(
            pending_decision.get("raw_exit_pnl_pct_proxy", pre_apply_pnl_pct_proxy) or 0.0
        )
        closed_trade["close_decision_final_exit_market_cap_sol"] = float(
            pending_decision.get(
                "final_exit_market_cap_sol",
                closed_trade.get("current_market_cap_sol", 0.0),
            )
            or 0.0
        )
        closed_trade["close_decision_final_exit_pnl_pct_proxy"] = float(
            pending_decision.get(
                "final_exit_pnl_pct_proxy",
                closed_trade.get("pnl_pct_proxy", 0.0),
            )
            or 0.0
        )
        closed_trade["close_decision_winner_lock_floor_applied"] = bool(
            pending_decision.get("winner_lock_floor_applied", False)
        )
        closed_trade["close_decision_metadata"] = dict(
            pending_decision.get("metadata", {}) or {}
        )

    closed_trade["status"] = "closed"
    closed_trade["closed_at_utc"] = _now()
    closed_trade["exit_market_cap_sol"] = float(
        closed_trade.get("current_market_cap_sol", 0.0) or 0.0
    )
    closed_trade["exit_reason"] = exit_reason
    closed_trade["wallet_memory_outcome_recorded"] = _record_closed_trade_outcome(
        closed_trade
    )
    closed_trade["creator_entity_outcome_recorded"] = (
        _record_closed_trade_creator_entity_outcome(closed_trade)
    )
    closed_trades.append(closed_trade)
    open_trades.pop(mint, None)
    return closed_trade



def _sweep_stale_open_trades(
    open_trades: dict[str, dict[str, Any]],
    closed_trades: list[dict[str, Any]],
    preserve_mint: str | None = None,
) -> dict[str, Any]:
    stale_closed = 0
    closed_by_reason: dict[str, int] = {}
    preserve_mint = str(preserve_mint or "")

    for mint, trade in list(open_trades.items()):
        if not mint or mint == preserve_mint:
            continue

        trade_age_seconds = _trade_age_seconds(trade)
        idle_seconds = _seconds_since(
            str(trade.get("updated_at_utc") or "").strip() or None
        )
        if idle_seconds is None:
            idle_seconds = trade_age_seconds

        pnl_pct = _current_trade_pnl_pct(trade)
        invalidated_like = (
            str(trade.get("reason") or "") in {"hold_below_entry_threshold", "min_hold_active"}
            or str(trade.get("qualify_reason") or "") == "invalidated"
            or str(trade.get("entry_alert_band") or "") == "invalidated"
            or bool(trade.get("invalidation_reasons"))
        )
        background_like = (
            str(trade.get("priority_tier") or "") == "background"
            or str(trade.get("reason") or "") == "background"
            or float(trade.get("current_score", 0.0) or 0.0) < 48.0
        )
        winner_lock_breached, winner_lock_floor_pnl_pct, winner_lock_max_pnl_pct, winner_lock_pullback_pct = _winner_lock_breached(
            trade,
            pnl_pct=pnl_pct,
        )
        profit_protected = _profit_protected_trade(trade, pnl_pct)

        candidate_reasons: list[str] = []
        if winner_lock_breached and trade_age_seconds >= PAPER_MIN_HOLD_SECONDS and (
            invalidated_like or background_like or idle_seconds >= 30.0
        ):
            candidate_reasons.append("winner_floor_timeout")
        if trade_age_seconds >= PAPER_STALE_ANY_CLOSE_SECONDS:
            candidate_reasons.append("stale_timeout")
        if (
            invalidated_like
            and trade_age_seconds >= PAPER_STALE_INVALIDATED_CLOSE_SECONDS
            and pnl_pct <= PAPER_STALE_INVALIDATED_NONPOSITIVE_PNL_PCT
            and not profit_protected
        ):
            candidate_reasons.append("stale_invalidated_timeout")
        if (
            background_like
            and idle_seconds >= PAPER_STALE_BACKGROUND_CLOSE_SECONDS
            and pnl_pct <= PAPER_STALE_BACKGROUND_MAX_PNL_PCT
            and not profit_protected
        ):
            candidate_reasons.append("stale_background_timeout")
        if idle_seconds >= PAPER_STALE_NEGATIVE_CLOSE_SECONDS:
            if pnl_pct <= PAPER_STALE_LOSS_FLOOR_PNL_PCT:
                candidate_reasons.append("stale_loss_timeout")
            elif invalidated_like and pnl_pct <= 0.0 and not profit_protected:
                candidate_reasons.append("stale_invalidation_timeout")

        if not candidate_reasons:
            continue

        exit_reason = candidate_reasons[0]
        if exit_reason == "winner_floor_timeout":
            trade["winner_lock_floor_pnl_pct"] = float(winner_lock_floor_pnl_pct or 0.0)
            trade["winner_lock_max_pnl_pct"] = float(winner_lock_max_pnl_pct or 0.0)
            trade["winner_lock_pullback_pct"] = float(winner_lock_pullback_pct or 0.0)

        _set_close_decision_trace(
            trade,
            source="sweep",
            selected_reason=exit_reason,
            candidate_reasons=candidate_reasons,
            snapshot=None,
            alert=None,
            extra={
                "path": "sweep_stale",
                "raw_current_pnl_pct": pnl_pct,
                "raw_current_market_cap_sol": float(
                    trade.get("current_market_cap_sol", 0.0) or 0.0
                ),
                "winner_floor_pct": winner_lock_floor_pnl_pct,
                "max_pnl_pct": winner_lock_max_pnl_pct or float(trade.get("max_pnl_pct_proxy", pnl_pct) or pnl_pct),
                "pullback_pct": winner_lock_pullback_pct,
                "min_hold_active": trade_age_seconds < PAPER_MIN_HOLD_SECONDS,
                "trade_age_seconds": trade_age_seconds,
                "idle_seconds": idle_seconds,
                "qualify_reason": str(trade.get("qualify_reason") or "").strip(),
                "profit_protected": profit_protected,
                "invalidated_flag": invalidated_like,
                "background_like": background_like,
            },
        )

        _finalize_close(
            trade=trade,
            snapshot=None,
            closed_trades=closed_trades,
            open_trades=open_trades,
            mint=mint,
            exit_reason=exit_reason,
        )
        stale_closed += 1
        closed_by_reason[exit_reason] = closed_by_reason.get(exit_reason, 0) + 1

    return {
        "closed_count": stale_closed,
        "closed_by_reason": closed_by_reason,
    }


def sweep_open_trades(preserve_mint: str | None = None) -> dict[str, Any]:
    open_trades: dict[str, dict[str, Any]] = _load_json(OPEN_TRADES_PATH, {})
    closed_trades: list[dict[str, Any]] = _load_json(CLOSED_TRADES_PATH, [])
    result = _sweep_stale_open_trades(
        open_trades=open_trades,
        closed_trades=closed_trades,
        preserve_mint=preserve_mint,
    )
    if result.get("closed_count"):
        _save_json(OPEN_TRADES_PATH, open_trades)
        _save_json(CLOSED_TRADES_PATH, closed_trades)
    return {
        "closed_count": int(result.get("closed_count", 0) or 0),
        "closed_by_reason": dict(result.get("closed_by_reason", {}) or {}),
        "open_trade_count": len(open_trades),
        "closed_trade_count": len(closed_trades),
    }


def initialize_paper_book() -> dict[str, Any]:
    return sweep_open_trades()


def sync_trade(snapshot: dict[str, Any], alert: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    open_trades: dict[str, dict[str, Any]] = _load_json(OPEN_TRADES_PATH, {})
    closed_trades: list[dict[str, Any]] = _load_json(CLOSED_TRADES_PATH, [])

    mint = str(snapshot.get("mint") or "")
    if not mint:
        return "noop", None

    stale_sweep = _sweep_stale_open_trades(
        open_trades=open_trades,
        closed_trades=closed_trades,
        preserve_mint=mint,
    )
    if stale_sweep.get("closed_count"):
        _save_json(OPEN_TRADES_PATH, open_trades)
        _save_json(CLOSED_TRADES_PATH, closed_trades)

    existing = open_trades.get(mint)
    qualifies, qualify_reason = _entry_threshold(snapshot)
    reclaim_ok, reclaim_reason = _reclaim_threshold(snapshot)

    if existing is None:
        if not qualifies:
            wait_payload = {
                "mint": mint,
                "reason": "await_entry_confirmation",
                "qualify_reason": qualify_reason,
                "confirmation_mode": "entry",
                "confirmation_progress": 0,
                "confirmation_required": 1,
            }
            return "entry_wait", wait_payload

        recent_closed_trade = _recently_closed_trade(closed_trades, mint)
        recent_close_age_seconds = (
            _seconds_since(
                str(recent_closed_trade.get("closed_at_utc") or "").strip() or None
            )
            if recent_closed_trade
            else None
        )
        if recent_close_age_seconds is not None and recent_close_age_seconds < PAPER_REENTRY_COOLDOWN_SECONDS:
            wait_payload = {
                "mint": mint,
                "reason": "recent_reentry_cooldown",
                "qualify_reason": "cooldown_active",
                "confirmation_mode": "entry",
                "confirmation_progress": 0,
                "confirmation_required": 1,
                "cooldown_seconds_remaining": round(
                    max(PAPER_REENTRY_COOLDOWN_SECONDS - recent_close_age_seconds, 0.0),
                    2,
                ),
            }
            return "entry_wait", wait_payload

        trade = _build_trade(snapshot, alert)
        if reclaim_ok:
            trade["reclaim_confirmed"] = True
            trade["confirmation_mode"] = "reclaim"
            trade["qualify_reason"] = reclaim_reason
        else:
            trade["qualify_reason"] = qualify_reason

        open_trades[mint] = trade
        _save_json(OPEN_TRADES_PATH, open_trades)
        return "opened", trade

    trade = _update_trade_metrics(existing, snapshot)
    qualifies_now, qualify_reason_now = _entry_threshold(snapshot)
    reclaim_now, reclaim_reason_now = _reclaim_threshold(snapshot)
    early_hold_active = _trade_age_seconds(trade) < PAPER_MIN_HOLD_SECONDS

    if trade.get("confirmation_mode") == "reclaim" and not reclaim_now:
        wait_payload = {
            "mint": mint,
            "reason": "await_reclaim_confirmation",
            "qualify_reason": reclaim_reason_now,
            "confirmation_mode": "reclaim",
            "confirmation_progress": 0,
            "confirmation_required": 1,
        }
        open_trades[mint] = trade
        _save_json(OPEN_TRADES_PATH, open_trades)
        return "reclaim_wait", wait_payload

    if not qualifies_now:
        should_close, exit_reason = _should_close(trade, snapshot, alert)
        if should_close:
            trade = _finalize_close(
                trade=trade,
                snapshot=snapshot,
                closed_trades=closed_trades,
                open_trades=open_trades,
                mint=mint,
                exit_reason=exit_reason or qualify_reason_now,
            )
            _save_json(OPEN_TRADES_PATH, open_trades)
            _save_json(CLOSED_TRADES_PATH, closed_trades)
            return "closed", trade

        trade["reason"] = "min_hold_active" if early_hold_active else "hold_below_entry_threshold"
        trade["qualify_reason"] = qualify_reason_now
        open_trades[mint] = trade
        _save_json(OPEN_TRADES_PATH, open_trades)
        return "updated", trade

    should_close, exit_reason = _should_close(trade, snapshot, alert)
    if should_close:
        trade = _finalize_close(
            trade=trade,
            snapshot=snapshot,
            closed_trades=closed_trades,
            open_trades=open_trades,
            mint=mint,
            exit_reason=exit_reason or "rule_exit",
        )
        _save_json(OPEN_TRADES_PATH, open_trades)
        _save_json(CLOSED_TRADES_PATH, closed_trades)
        return "closed", trade

    trade["reason"] = "min_hold_active" if early_hold_active else "active"
    trade["qualify_reason"] = reclaim_reason_now if reclaim_now else qualify_reason_now
    if reclaim_now:
        trade["reclaim_confirmed"] = True
        trade["confirmation_mode"] = "reclaim"

    open_trades[mint] = trade
    _save_json(OPEN_TRADES_PATH, open_trades)
    return "updated", trade
