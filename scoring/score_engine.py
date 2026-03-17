from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from filters.pump_filters import score_token_state
from scoring.archetype_engine import classify_archetype
from scoring.breakout_strategy import score_breakout_strategy
from scoring.feature_engine import build_quant_features
from scoring.momentum_strategy import score_momentum_strategy

RESULTS_DIR = Path("data/strategy_results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
LATEST_DIR = RESULTS_DIR / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class StrategySnapshot:
    mint: str
    symbol: str | None
    name: str | None
    captured_at_utc: str
    status: str
    strategy_name: str
    primary_archetype: str
    secondary_archetype: str
    archetype_confidence: float
    archetype_scores: dict[str, float]
    top_archetypes: list[dict[str, Any]]
    regime_tags: list[str]
    archetype_reasons: list[str]
    entry_bias: str
    setup_state: str
    composite_score: float
    tie_break_score: float
    confidence: str
    priority_tier: str
    why_now: list[str]
    trigger_reasons: list[str]
    invalidation_reasons: list[str]
    risk_flags: list[str]
    penalties: dict[str, float]
    strategy_scores: dict[str, float]
    strategy_reasons: dict[str, list[str]]
    score_inputs: dict[str, Any]
    quant_features: dict[str, float]
    helius_enrichment_status: str
    helius_last_error: str | None
    research_confidence_label: str
    research_confidence_score: float
    entry_confirmation_score: float
    current_market_cap_sol: float
    peak_market_cap_sol: float
    market_cap_sol: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "mint": self.mint,
            "rank_mint": self.mint,
            "symbol": self.symbol,
            "name": self.name,
            "captured_at_utc": self.captured_at_utc,
            "status": self.status,
            "strategy_name": self.strategy_name,
            "primary_archetype": self.primary_archetype,
            "secondary_archetype": self.secondary_archetype,
            "archetype_confidence": self.archetype_confidence,
            "archetype_scores": self.archetype_scores,
            "top_archetypes": self.top_archetypes,
            "regime_tags": self.regime_tags,
            "archetype_reasons": self.archetype_reasons,
            "entry_bias": self.entry_bias,
            "setup_state": self.setup_state,
            "composite_score": self.composite_score,
            "tie_break_score": self.tie_break_score,
            "confidence": self.confidence,
            "priority_tier": self.priority_tier,
            "why_now": self.why_now,
            "trigger_reasons": self.trigger_reasons,
            "invalidation_reasons": self.invalidation_reasons,
            "risk_flags": self.risk_flags,
            "penalties": self.penalties,
            "strategy_scores": self.strategy_scores,
            "strategy_reasons": self.strategy_reasons,
            "score_inputs": self.score_inputs,
            "quant_features": self.quant_features,
            "helius_enrichment_status": self.helius_enrichment_status,
            "helius_last_error": self.helius_last_error,
            "research_confidence_label": self.research_confidence_label,
            "research_confidence_score": self.research_confidence_score,
            "entry_confirmation_score": self.entry_confirmation_score,
            "current_market_cap_sol": self.current_market_cap_sol,
            "peak_market_cap_sol": self.peak_market_cap_sol,
            "market_cap_sol": self.market_cap_sol,
        }


class StrategyScoreEngine:
    def __init__(self) -> None:
        self.latest_snapshots: dict[str, dict[str, Any]] = {}

    def evaluate(
        self,
        state: dict[str, Any],
        events: list[dict[str, Any]] | None = None,
        *,
        include_helius: bool = True,
        persist: bool = True,
        local_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        events = events or []

        filter_score, filter_reasons = score_token_state(state)
        breakout_score, breakout_reasons = score_breakout_strategy(state, events)
        momentum_score, momentum_reasons = score_momentum_strategy(state, events)
        features = build_quant_features(state, include_helius=include_helius)
        features["event_sequence_score"] = self._event_sequence_score(events)

        current_mcap = float(
            state.get("current_market_cap_sol", 0.0)
            or state.get("market_cap_sol", 0.0)
            or 0.0
        )
        peak_mcap = float(state.get("peak_market_cap_sol", 0.0) or 0.0)
        net_5m = float(state.get("net_sol_flow_last_5m", 0.0) or 0.0)
        buy_sol_5m = float(state.get("buy_sol_last_5m", 0.0) or 0.0)
        sell_sol_5m = float(state.get("sell_sol_last_5m", 0.0) or 0.0)
        trades_1m = int(state.get("trades_last_1m", 0) or 0)
        trades_5m = int(state.get("trades_last_5m", 0) or 0)
        unique_buyers = int(
            state.get("unique_buyers_last_5m", state.get("unique_buyers", 0)) or 0
        )
        unique_traders = int(
            state.get("unique_traders_last_5m", state.get("unique_traders", 0)) or 0
        )
        status = state.get("status") or "new"

        helius_enrichment_tier = str(state.get("helius_enrichment_tier", "none") or "none")
        helius_enrichment_status = str(
            state.get("helius_enrichment_status", "unknown") or "unknown"
        )
        helius_requested_wallet_count = int(
            state.get("helius_requested_wallet_count", 0) or 0
        )
        helius_completed_wallet_count = int(
            state.get("helius_completed_wallet_count", 0) or 0
        )
        helius_profile_depth_bucket = str(
            state.get("helius_profile_depth_bucket", "none") or "none"
        )
        helius_profile_completion_ratio = float(
            state.get("helius_profile_completion_ratio", 0.0) or 0.0
        )
        helius_partial_enrichment = bool(
            state.get("helius_partial_enrichment", False)
        )
        helius_time_budget_exhausted = bool(
            state.get("helius_time_budget_exhausted", False)
        )

        event_bonus = max(
            -8.0,
            min(
                12.0,
                sum(self._event_weight(event.get("event_type")) for event in events),
            ),
        )

        research_confidence_label, research_confidence_score = (
            self._research_confidence(
                state,
                features,
                include_helius=include_helius,
            )
        )

        feature_bonus = 0.0
        feature_triggers: list[str] = []

        if features["trade_acceleration_ratio"] >= 1.4:
            feature_bonus += 6.0
            feature_triggers.append("trade_acceleration")
        if features["buy_pressure_ratio_5m"] >= 0.7:
            feature_bonus += 6.0
            feature_triggers.append("strong_buy_pressure")
        if features["buyer_density_5m"] >= 0.55:
            feature_bonus += 4.0
            feature_triggers.append("distributed_buyers")
        if features["mcap_to_peak_ratio"] >= 0.93 and current_mcap >= 35:
            feature_bonus += 5.0
            feature_triggers.append("holding_near_highs")
        if features["net_flow_per_trade_5m"] >= 0.12 and net_5m > 0:
            feature_bonus += 4.0
            feature_triggers.append("efficient_net_flow")
        if features["participant_quality_score_v2"] >= 0.62:
            feature_bonus += 6.0
            feature_triggers.append("high_participant_quality")
        if (
            features["wallet_novelty_score"] >= 0.45
            and features["new_buyer_velocity"] >= 0.18
        ):
            feature_bonus += 5.0
            feature_triggers.append("novel_wallet_expansion")
        if features["participant_churn_ratio"] >= 0.45 and unique_traders >= 3:
            feature_bonus += 4.0
            feature_triggers.append("fresh_participant_wave")

        if features.get("helius_profile_count", 0.0) >= 3:
            if features.get("helius_cohort_quality_score", 0.0) >= 0.62:
                feature_bonus += 5.0
                feature_triggers.append("helius_cohort_quality")
            if (
                features.get("helius_funding_diversity_score", 0.0) >= 0.65
                and features.get("helius_recycled_wallet_share", 0.0) <= 0.25
                and features.get("helius_sniper_wallet_share", 0.0) <= 0.25
            ):
                feature_bonus += 4.0
                feature_triggers.append("diverse_wallet_funding")

        if include_helius:
            if research_confidence_label == "deep_high_quality":
                feature_bonus += 4.0
                feature_triggers.append("deep_high_quality_research")
            elif research_confidence_label == "deep_enriched":
                feature_bonus += 2.5
                feature_triggers.append("deep_research_conviction")
            elif research_confidence_label == "deep_mid_depth":
                feature_bonus += 1.5
                feature_triggers.append("mid_depth_research")
            elif research_confidence_label == "deep_partial_useful":
                feature_bonus += 1.0
                feature_triggers.append("useful_partial_research")
            elif research_confidence_label == "light_enriched":
                feature_bonus += 1.0
                feature_triggers.append("light_research_support")

            if (
                helius_completed_wallet_count >= 4
                and helius_profile_completion_ratio >= 0.60
            ):
                feature_bonus += 1.5
                feature_triggers.append("strong_profile_completion")
            elif (
                helius_completed_wallet_count >= 2
                and helius_profile_completion_ratio >= 0.35
            ):
                feature_bonus += 0.75
                feature_triggers.append("useful_profile_completion")

            if (
                helius_profile_depth_bucket == "5_plus"
                and features.get("helius_cohort_quality_score", 0.0) >= 0.58
            ):
                feature_bonus += 1.5
                feature_triggers.append("deep_profile_depth")

        # Wallet-memory signals: intentionally light and confidence-gated.
        historical_cohort_quality_score = float(
            features.get("historical_cohort_quality_score", 0.5) or 0.5
        )
        historical_cohort_confidence = float(
            features.get("historical_cohort_confidence", 0.0) or 0.0
        )
        historical_good_wallet_share = float(
            features.get("historical_good_wallet_share", 0.0) or 0.0
        )
        historical_bad_wallet_share = float(
            features.get("historical_bad_wallet_share", 0.0) or 0.0
        )
        historical_known_wallet_share = float(
            features.get("historical_known_wallet_share", 0.0) or 0.0
        )
        creator_entity_quality_score = float(
            features.get("creator_entity_quality_score", 0.5) or 0.5
        )
        creator_entity_confidence_score = float(
            features.get("creator_entity_confidence_score", 0.0) or 0.0
        )
        creator_entity_launch_count = float(
            features.get("creator_entity_launch_count", 0.0) or 0.0
        )
        creator_entity_paper_trade_count = float(
            features.get("creator_entity_paper_trade_count", 0.0) or 0.0
        )
        creator_entity_is_known = float(
            features.get("creator_entity_is_known", 0.0) or 0.0
        )

        creator_memory_bonus = 0.0
        if creator_entity_is_known > 0.0 and creator_entity_confidence_score > 0.0:
            creator_quality_centered = max(
                -1.0,
                min(1.0, (creator_entity_quality_score - 0.5) / 0.18),
            )
            creator_sample_support = min(
                0.35,
                (min(creator_entity_launch_count, 6.0) * 0.03)
                + (min(creator_entity_paper_trade_count, 4.0) * 0.05),
            )
            creator_memory_bonus = max(
                -1.5,
                min(
                    1.5,
                    creator_quality_centered
                    * creator_entity_confidence_score
                    * (3.0 + creator_sample_support),
                ),
            )

            if creator_memory_bonus >= 0.3:
                feature_triggers.append("creator_entity_support")
            elif creator_memory_bonus <= -0.3:
                feature_triggers.append("creator_entity_caution")

            feature_bonus += creator_memory_bonus

        if historical_cohort_confidence >= 0.35:
            if historical_cohort_quality_score >= 0.58:
                feature_bonus += 0.75
                feature_triggers.append("historical_cohort_support")
            if (
                historical_cohort_confidence >= 0.55
                and historical_cohort_quality_score >= 0.62
                and historical_good_wallet_share >= 0.35
            ):
                feature_bonus += 1.25
                feature_triggers.append("historical_good_wallets")
            if (
                historical_cohort_confidence >= 0.70
                and historical_good_wallet_share >= 0.50
            ):
                feature_bonus += 0.75
                feature_triggers.append("historical_wallet_repeat_winners")

        if features["sell_absorption_score"] >= 0.56:
            feature_bonus += 4.0
            feature_triggers.append("sell_absorption")
        if features["mcap_stability_score"] >= 0.72:
            feature_bonus += 3.0
            feature_triggers.append("mcap_stability")
        if (
            features["recovery_ratio_from_trough"] >= 0.55
            and features["peak_retrace_pct"] >= 10
        ):
            feature_bonus += 4.0
            feature_triggers.append("recovery_from_trough")
        if features["near_high_dwell_pct"] >= 18:
            feature_bonus += 3.0
            feature_triggers.append("near_high_dwell")
        if features["event_sequence_score"] >= 0.58:
            feature_bonus += 3.0
            feature_triggers.append("event_sequence")

        penalties: dict[str, float] = {}
        risk_flags: list[str] = []
        invalidation_reasons: list[str] = []

        if peak_mcap > 0 and current_mcap < peak_mcap * 0.65:
            penalties["deep_retrace"] = 12.0
            risk_flags.append("deep_retrace")
            invalidation_reasons.append("lost_too_much_from_peak")
        elif features["peak_retrace_pct"] >= 18:
            penalties["moderate_retrace"] = 6.0
            risk_flags.append("moderate_retrace")

        if net_5m < 0:
            penalties["negative_net_flow"] = 10.0
            risk_flags.append("negative_net_flow")
            invalidation_reasons.append("negative_5m_flow")

        if sell_sol_5m > buy_sol_5m and trades_5m >= 4:
            penalties["sell_dominance"] = 8.0
            risk_flags.append("sell_dominance")

        if features["buyer_density_5m"] < 0.35 and trades_5m >= 8:
            penalties["low_buyer_density"] = 7.0
            risk_flags.append("low_buyer_density")
            invalidation_reasons.append("narrow_buyer_base")

        if features["wash_risk_ratio"] >= 4.5 and trades_5m >= 10:
            penalties["churn_risk"] = 6.0
            risk_flags.append("churn_risk")

        if features["repeat_wallet_ratio"] >= 0.55 and trades_5m >= 6:
            penalties["repeat_wallet_churn"] = 9.0
            risk_flags.append("repeat_wallet_churn")
            invalidation_reasons.append("repeat_wallet_churn")

        if features["participant_concentration_score"] >= 0.68 and unique_traders >= 3:
            penalties["participant_concentration"] = 7.0
            risk_flags.append("participant_concentration")

        if (
            features["buyer_overlap_ratio"] >= 0.75
            and features["wallet_novelty_score"] < 0.25
            and trades_1m >= 3
        ):
            penalties["recycled_buyer_wave"] = 7.0
            risk_flags.append("recycled_buyer_wave")
            invalidation_reasons.append("recycled_buyer_wave")

        if features["participant_quality_score_v2"] < 0.35 and trades_5m >= 6:
            penalties["low_participant_quality"] = 8.0
            risk_flags.append("low_participant_quality")
            invalidation_reasons.append("low_participant_quality")

        if features.get("helius_profile_count", 0.0) >= 3:
            if features.get("helius_recycled_wallet_share", 0.0) >= 0.50:
                penalties["helius_recycled_cohort"] = 6.0
                risk_flags.append("helius_recycled_cohort")
            if features.get("helius_sniper_wallet_share", 0.0) >= 0.35:
                penalties["helius_sniper_cohort"] = 5.0
                risk_flags.append("helius_sniper_cohort")
            if features.get("helius_top_funder_concentration_score", 0.0) >= 0.55:
                penalties["wallet_funder_concentration"] = 5.0
                risk_flags.append("wallet_funder_concentration")
            if features.get("helius_creator_shared_funder_score", 0.0) >= 0.40:
                penalties["creator_shared_funder"] = 6.0
                risk_flags.append("creator_shared_funder")
            if features.get("helius_creator_probable_sniper_wallet", 0.0) >= 1.0:
                penalties["creator_sniper_profile"] = 5.0
                risk_flags.append("creator_sniper_profile")
            if features.get("helius_creator_probable_recycled_wallet", 0.0) >= 1.0:
                penalties["creator_recycled_profile"] = 4.0
                risk_flags.append("creator_recycled_profile")

        if (
            helius_enrichment_tier == "deep"
            and helius_enrichment_status in {"deep_partial", "deep_enriched"}
            and helius_completed_wallet_count >= 2
            and features.get("helius_creator_shared_funder_score", 0.0) >= 0.45
        ):
            penalties["deep_shared_funder_confirmation"] = (
                penalties.get("deep_shared_funder_confirmation", 0.0) + 2.0
            )
            risk_flags.append("deep_shared_funder_confirmation")

        if (
            helius_enrichment_tier == "deep"
            and helius_completed_wallet_count >= 2
            and features.get("helius_recycled_wallet_share", 0.0) >= 0.55
            and features.get("helius_sniper_wallet_share", 0.0) >= 0.35
        ):
            penalties["recycled_sniper_overlap"] = (
                penalties.get("recycled_sniper_overlap", 0.0) + 2.5
            )
            risk_flags.append("recycled_sniper_overlap")

        # Wallet-memory penalties: also light and only when evidence is meaningful.
        if historical_cohort_confidence >= 0.50:
            if (
                historical_bad_wallet_share >= 0.40
                and historical_cohort_quality_score <= 0.42
            ):
                penalties["historical_bad_wallets"] = 3.0
                risk_flags.append("historical_bad_wallets")
            if (
                historical_cohort_confidence >= 0.70
                and historical_cohort_quality_score <= 0.36
            ):
                penalties["historical_weak_cohort"] = (
                    penalties.get("historical_weak_cohort", 0.0) + 2.0
                )
                risk_flags.append("historical_weak_cohort")

        if (
            features["seller_expansion_ratio"] >= 1.20
            and features["buy_pressure_ratio_5m"] < 0.58
        ):
            penalties["seller_overhang"] = 6.0
            risk_flags.append("seller_overhang")

        if features.get("dust_trade_share_1m", 0.0) >= 0.35:
            penalties["dust_microflow_1m"] = 8.0
            risk_flags.append("dust_microflow")
        elif features.get("dust_trade_share_5m", 0.0) >= 0.25:
            penalties["dust_microflow_5m"] = 5.0
            risk_flags.append("dust_microflow")

        if features["time_since_last_buy_seconds"] >= 150 and trades_5m >= 6:
            penalties["buy_side_staleness"] = 4.0
            risk_flags.append("buy_side_staleness")

        if status == "inactive":
            penalties["inactive_status"] = 20.0
            risk_flags.append("inactive")
        elif status == "cooling_off":
            penalties["cooling_status"] = 10.0
            risk_flags.append("cooling_off")

        archetype = classify_archetype(state, features, events)
        (
            archetype_bonus,
            archetype_penalties,
            archetype_risk_flags,
            archetype_invalidations,
        ) = self._apply_archetype_policy(
            archetype,
            features,
        )
        penalties.update(archetype_penalties)
        risk_flags.extend(archetype_risk_flags)
        invalidation_reasons.extend(archetype_invalidations)

        base_composite = (
            filter_score * 0.25
            + breakout_score * 0.30
            + momentum_score * 0.25
            + event_bonus
            + feature_bonus
        )
        composite = round(
            max(base_composite + archetype_bonus - sum(penalties.values()), 0.0),
            2,
        )

        event_types = [event.get("event_type") for event in events]
        all_trigger_reasons = self._build_why_now(
            filter_reasons
            + breakout_reasons
            + momentum_reasons
            + feature_triggers
            + archetype.get("archetype_reasons", []),
            event_types,
            limit=16,
        )
        why_now = all_trigger_reasons[:8]
        confidence = self._confidence_from_score(composite, risk_flags, archetype)
        priority_tier = self._priority_from_score(composite, risk_flags, archetype)
        strategy_name = self._strategy_name(
            breakout_score,
            momentum_score,
            features,
            archetype,
        )
        entry_bias = self._entry_bias(features, net_5m, archetype)
        setup_state = self._setup_state(
            priority_tier,
            risk_flags,
            features,
            archetype,
            research_confidence_score=research_confidence_score,
            helius_enrichment_status=helius_enrichment_status,
            helius_profile_completion_ratio=helius_profile_completion_ratio,
        )
        tie_break_score = self._tie_break_score(
            state,
            features,
            penalties,
            archetype,
            research_confidence_score=research_confidence_score,
            helius_profile_completion_ratio=helius_profile_completion_ratio,
        )

        snapshot = StrategySnapshot(
            mint=state["mint"],
            symbol=state.get("symbol"),
            name=state.get("name"),
            captured_at_utc=datetime.now(timezone.utc).isoformat(),
            status=status,
            strategy_name=strategy_name,
            primary_archetype=archetype["primary_archetype"],
            secondary_archetype=archetype["secondary_archetype"],
            archetype_confidence=archetype["archetype_confidence"],
            archetype_scores=archetype["archetype_scores"],
            top_archetypes=archetype.get("top_archetypes", []),
            regime_tags=archetype.get("regime_tags", []),
            archetype_reasons=archetype.get("archetype_reasons", []),
            entry_bias=entry_bias,
            setup_state=setup_state,
            composite_score=composite,
            tie_break_score=tie_break_score,
            confidence=confidence,
            priority_tier=priority_tier,
            why_now=why_now,
            trigger_reasons=all_trigger_reasons,
            invalidation_reasons=sorted(set(invalidation_reasons)),
            risk_flags=sorted(set(risk_flags)),
            penalties={k: round(v, 2) for k, v in penalties.items()},
            strategy_scores={
                "filter_score": round(filter_score, 2),
                "breakout_score": round(breakout_score, 2),
                "momentum_score": round(momentum_score, 2),
                "event_bonus": round(event_bonus, 2),
                "feature_bonus": round(feature_bonus, 2),
                "creator_memory_bonus": round(creator_memory_bonus, 2),
                "archetype_bonus": round(archetype_bonus, 2),
                "penalty_total": round(sum(penalties.values()), 2),
                "base_composite": round(base_composite, 2),
            },
            strategy_reasons={
                "filter": filter_reasons,
                "breakout": breakout_reasons,
                "momentum": momentum_reasons,
                "features": feature_triggers,
                "events": event_types,
                "archetype": archetype.get("archetype_reasons", []),
            },
            score_inputs={
                "current_market_cap_sol": round(current_mcap, 4),
                "peak_market_cap_sol": round(peak_mcap, 4),
                "net_sol_flow_last_5m": round(net_5m, 4),
                "buy_sol_last_5m": round(buy_sol_5m, 4),
                "trades_last_1m": trades_1m,
                "trades_last_5m": trades_5m,
                "unique_buyers_last_5m": unique_buyers,
                "unique_traders_last_5m": unique_traders,
                "wallet_novelty_score": round(features["wallet_novelty_score"], 4),
                "repeat_wallet_ratio": round(features["repeat_wallet_ratio"], 4),
                "buyer_overlap_ratio": round(features["buyer_overlap_ratio"], 4),
                "participant_quality_score_v2": round(
                    features["participant_quality_score_v2"],
                    4,
                ),
                "helius_profile_count": round(
                    features.get("helius_profile_count", 0.0),
                    4,
                ),
                "helius_cohort_quality_score": round(
                    features.get("helius_cohort_quality_score", 0.0),
                    4,
                ),
                "helius_fresh_wallet_share": round(
                    features.get("helius_fresh_wallet_share", 0.0),
                    4,
                ),
                "helius_sniper_wallet_share": round(
                    features.get("helius_sniper_wallet_share", 0.0),
                    4,
                ),
                "helius_recycled_wallet_share": round(
                    features.get("helius_recycled_wallet_share", 0.0),
                    4,
                ),
                "helius_creator_shared_funder_score": round(
                    features.get("helius_creator_shared_funder_score", 0.0),
                    4,
                ),
                "helius_profile_depth_bucket": helius_profile_depth_bucket,
                "helius_profile_completion_ratio": round(
                    helius_profile_completion_ratio,
                    4,
                ),
                "helius_requested_wallet_count": helius_requested_wallet_count,
                "helius_completed_wallet_count": helius_completed_wallet_count,
                "helius_partial_enrichment": helius_partial_enrichment,
                "helius_time_budget_exhausted": helius_time_budget_exhausted,
                "historical_cohort_quality_score": round(
                    historical_cohort_quality_score,
                    4,
                ),
                "historical_cohort_confidence": round(
                    historical_cohort_confidence,
                    4,
                ),
                "historical_good_wallet_share": round(
                    historical_good_wallet_share,
                    4,
                ),
                "historical_bad_wallet_share": round(
                    historical_bad_wallet_share,
                    4,
                ),
                "historical_known_wallet_share": round(
                    historical_known_wallet_share,
                    4,
                ),
                "creator_entity_quality_score": round(
                    creator_entity_quality_score,
                    4,
                ),
                "creator_entity_confidence_score": round(
                    creator_entity_confidence_score,
                    4,
                ),
                "creator_entity_launch_count": round(
                    creator_entity_launch_count,
                    4,
                ),
                "creator_entity_paper_trade_count": round(
                    creator_entity_paper_trade_count,
                    4,
                ),
                "creator_entity_is_known": round(
                    creator_entity_is_known,
                    4,
                ),
                "creator_memory_bonus": round(
                    creator_memory_bonus,
                    4,
                ),
                "event_sequence_score": round(features["event_sequence_score"], 4),
                "dust_trade_share_1m": round(
                    features.get("dust_trade_share_1m", 0.0),
                    4,
                ),
                "dust_trade_share_5m": round(
                    features.get("dust_trade_share_5m", 0.0),
                    4,
                ),
                "entry_confirmation_score": round(
                    features.get("entry_confirmation_score", 0.0),
                    4,
                ),
                "helius_enrichment_status": helius_enrichment_status,
                "helius_enrichment_tier": helius_enrichment_tier,
                "research_confidence_label": research_confidence_label,
                "research_confidence_score": round(research_confidence_score, 4),
            },
            quant_features=features,
            helius_enrichment_status=helius_enrichment_status,
            helius_last_error=state.get("helius_last_error"),
            research_confidence_label=research_confidence_label,
            research_confidence_score=round(research_confidence_score, 4),
            entry_confirmation_score=round(
                features.get("entry_confirmation_score", 0.0),
                4,
            ),
            current_market_cap_sol=round(current_mcap, 4),
            peak_market_cap_sol=round(peak_mcap, 4),
            market_cap_sol=round(current_mcap, 4),
        ).to_dict()

        snapshot["analysis_mode"] = "full" if include_helius else "local_only"
        snapshot["helius_enrichment_tier"] = helius_enrichment_tier
        snapshot["helius_trigger_reason"] = state.get("helius_trigger_reason")
        snapshot["helius_wallet_target"] = int(state.get("helius_wallet_target", 0) or 0)
        snapshot["helius_selected_wallet_count"] = int(
            state.get("helius_selected_wallet_count", 0) or 0
        )
        snapshot["helius_profiled_wallet_count"] = int(
            state.get("helius_profiled_wallet_count", 0) or 0
        )
        snapshot["helius_requested_wallet_count"] = helius_requested_wallet_count
        snapshot["helius_completed_wallet_count"] = helius_completed_wallet_count
        snapshot["helius_profile_depth_bucket"] = helius_profile_depth_bucket
        snapshot["helius_profile_completion_ratio"] = round(
            helius_profile_completion_ratio,
            4,
        )
        snapshot["helius_partial_enrichment"] = helius_partial_enrichment
        snapshot["helius_cached_error"] = state.get("helius_cached_error")
        snapshot["helius_creator_attempted"] = bool(
            state.get("helius_creator_attempted", False)
        )
        snapshot["helius_creator_profiled"] = bool(
            state.get("helius_creator_profiled", False)
        )
        snapshot["helius_time_budget_seconds"] = float(
            state.get("helius_time_budget_seconds", 0.0) or 0.0
        )
        snapshot["helius_time_budget_exhausted"] = helius_time_budget_exhausted
        snapshot["helius_wallet_attempt_details"] = list(
            state.get("helius_wallet_attempt_details", []) or []
        )
        snapshot["local_composite_score"] = round(
            float((local_snapshot or {}).get("composite_score", composite) or composite),
            2,
        )
        snapshot["local_priority_tier"] = str(
            (local_snapshot or {}).get("priority_tier", priority_tier) or priority_tier
        )
        snapshot["local_confidence"] = str(
            (local_snapshot or {}).get("confidence", confidence) or confidence
        )
        snapshot["enriched_score_delta"] = round(
            snapshot["composite_score"] - snapshot["local_composite_score"],
            2,
        )
        snapshot["enrichment_applied"] = bool(
            include_helius and snapshot.get("helius_profiled_wallet_count", 0) > 0
        )

        if persist:
            self.latest_snapshots[state["mint"]] = snapshot
            self._persist_snapshot(snapshot)
        return snapshot

    def _persist_snapshot(self, snapshot: dict[str, Any]) -> None:
        path = LATEST_DIR / f"{snapshot['mint']}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)

    @staticmethod
    def _event_weight(event_type: str | None) -> float:
        weights = {
            "STATUS_WATCHING_TO_ESCALATED": 4.0,
            "STATUS_REAWAKENED_TO_ESCALATED": 4.0,
            "STATUS_ESCALATED_TO_WATCHING": 1.0,
            "MCAP_CROSS_35": 2.0,
            "BUY_FLOW_5M_CROSS_2": 3.0,
            "NET_FLOW_5M_CROSS_1": 3.0,
            "UNIQUE_BUYERS_CROSS_3": 2.0,
            "TX_ACCELERATION_1M": 2.0,
            "PEAK_BREAKOUT": 3.0,
            "PARTICIPANT_QUALITY_HIGH": 3.0,
            "WALLET_NOVELTY_SURGE": 2.0,
            "REPEAT_WALLET_CHURN": -3.0,
            "PARTICIPANT_QUALITY_BREAKDOWN": -3.0,
        }
        return weights.get(event_type or "", 1.0)

    @staticmethod
    def _event_sequence_score(events: list[dict[str, Any]]) -> float:
        if not events:
            return 0.0
        event_types = [str(event.get("event_type") or "") for event in events]
        score = 0.0
        positive = {
            "MCAP_CROSS_35",
            "BUY_FLOW_5M_CROSS_2",
            "NET_FLOW_5M_CROSS_1",
            "UNIQUE_BUYERS_CROSS_3",
            "TX_ACCELERATION_1M",
            "PEAK_BREAKOUT",
            "PARTICIPANT_QUALITY_HIGH",
            "WALLET_NOVELTY_SURGE",
            "STATUS_WATCHING_TO_ESCALATED",
            "STATUS_REAWAKENED_TO_ESCALATED",
        }
        negative = {"REPEAT_WALLET_CHURN", "PARTICIPANT_QUALITY_BREAKDOWN"}
        if any(event in positive for event in event_types):
            score += 0.35
        if "TX_ACCELERATION_1M" in event_types and "PEAK_BREAKOUT" in event_types:
            score += 0.20
        if (
            "PARTICIPANT_QUALITY_HIGH" in event_types
            or "WALLET_NOVELTY_SURGE" in event_types
        ):
            score += 0.20
        if (
            "STATUS_REAWAKENED_TO_ESCALATED" in event_types
            or "STATUS_WATCHING_TO_ESCALATED" in event_types
        ):
            score += 0.15
        if any(event in negative for event in event_types):
            score -= 0.20
        return round(max(0.0, min(score, 1.0)), 4)

    @staticmethod
    def _research_confidence(
        state: dict[str, Any],
        features: dict[str, float],
        *,
        include_helius: bool,
    ) -> tuple[str, float]:
        if not include_helius:
            return "local_only", 0.20

        tier = str(state.get("helius_enrichment_tier", "none") or "none")
        status = str(state.get("helius_enrichment_status", "unknown") or "unknown")
        completed_wallet_count = int(state.get("helius_completed_wallet_count", 0) or 0)
        profile_depth_bucket = str(
            state.get("helius_profile_depth_bucket", "none") or "none"
        )
        partial_enrichment = bool(state.get("helius_partial_enrichment", False))
        cohort_quality_score = float(
            features.get("helius_cohort_quality_score", 0.0) or 0.0
        )
        creator_shared_funder_score = float(
            features.get("helius_creator_shared_funder_score", 0.0) or 0.0
        )
        recycled_wallet_share = float(
            features.get("helius_recycled_wallet_share", 0.0) or 0.0
        )
        sniper_wallet_share = float(
            features.get("helius_sniper_wallet_share", 0.0) or 0.0
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

    @staticmethod
    def _apply_archetype_policy(
        archetype: dict[str, Any],
        features: dict[str, float],
    ) -> tuple[float, dict[str, float], list[str], list[str]]:
        primary = archetype.get("primary_archetype")
        regime_tags = set(archetype.get("regime_tags", []))
        penalties: dict[str, float] = {}
        risk_flags: list[str] = []
        invalidations: list[str] = []
        bonus = 0.0

        if primary == "BREAKOUT_RUNNER":
            bonus += 5.0
        elif primary == "HIGH_TIGHT_RANGE":
            bonus += 4.0
        elif primary == "MOMENTUM_EXPANSION":
            bonus += 4.0
        elif primary == "EARLY_MICRO_PUMP":
            bonus += 2.5
        elif primary == "REVIVAL_RECLAIM":
            bonus += 4.5
        elif primary == "SLOW_BURN_ACCUMULATION":
            bonus += 2.5
        elif primary == "FAILED_BREAKOUT":
            penalties["failed_breakout_structure"] = 8.0
            risk_flags.append("failed_breakout_structure")
        elif primary == "BOT_WASH":
            penalties["bot_wash_penalty"] = 14.0
            risk_flags.append("bot_wash")
            invalidations.append("bot_wash_archetype")
        elif primary == "INSIDER_ROTATION":
            penalties["insider_rotation_penalty"] = 10.0
            risk_flags.append("insider_rotation")
            invalidations.append("insider_rotation_risk")
        elif primary == "DEAD_LAUNCH":
            penalties["dead_launch_penalty"] = 12.0
            risk_flags.append("dead_launch")
            invalidations.append("dead_launch_structure")
        elif primary == "LIQUIDITY_FADE":
            penalties["liquidity_fade_penalty"] = 10.0
            risk_flags.append("liquidity_fade")
            invalidations.append("liquidity_fade_structure")

        if "ORGANIC_PARTICIPATION" in regime_tags:
            bonus += 3.0
        if "RECLAIM_CLEAN" in regime_tags:
            bonus += 2.0
        if "RECYCLED_PARTICIPATION" in regime_tags:
            penalties["recycled_participation"] = (
                penalties.get("recycled_participation", 0.0) + 4.0
            )
            risk_flags.append("recycled_participation")
        if "SELLER_OVERHANG" in regime_tags:
            penalties["seller_overhang_regime"] = (
                penalties.get("seller_overhang_regime", 0.0) + 5.0
            )
            risk_flags.append("seller_overhang")
        if "THIN_LIQUIDITY" in regime_tags:
            penalties["thin_liquidity"] = penalties.get("thin_liquidity", 0.0) + 3.0
            risk_flags.append("thin_liquidity")
        if "INSUFFICIENT_HISTORY" in regime_tags:
            penalties["insufficient_history"] = (
                penalties.get("insufficient_history", 0.0) + 5.0
            )
            risk_flags.append("insufficient_history")
        if "RECLAIM_FRAGILE" in regime_tags:
            penalties["fragile_reclaim"] = (
                penalties.get("fragile_reclaim", 0.0) + 3.0
            )
            risk_flags.append("fragile_reclaim")
        if "OVEREXTENDED" in regime_tags and features.get("buy_pressure_ratio_1m", 0.0) < 0.58:
            penalties["overextended"] = penalties.get("overextended", 0.0) + 3.0
            risk_flags.append("overextended")

        return round(bonus, 2), penalties, risk_flags, invalidations

    @staticmethod
    def _build_why_now(
        reason_pool: list[str],
        event_types: list[str],
        limit: int = 8,
    ) -> list[str]:
        ordered: list[str] = []
        for item in event_types + reason_pool:
            if item and item not in ordered:
                ordered.append(item)
        return ordered[:limit]

    @staticmethod
    def _confidence_from_score(
        score: float,
        risk_flags: list[str],
        archetype: dict[str, Any],
    ) -> str:
        regime_tags = set(archetype.get("regime_tags", []) or [])
        if "INSUFFICIENT_HISTORY" in regime_tags:
            return "medium" if score >= 68 else "low"
        if (
            score >= 82
            and len(risk_flags) <= 1
            and float(archetype.get("archetype_confidence", 0.0) or 0.0) >= 0.62
        ):
            return "high"
        if score >= 52:
            return "medium"
        return "low"

    @staticmethod
    def _priority_from_score(
        score: float,
        risk_flags: list[str],
        archetype: dict[str, Any],
    ) -> str:
        primary = archetype.get("primary_archetype")
        regime_tags = set(archetype.get("regime_tags", []) or [])
        if primary in {"BOT_WASH", "DEAD_LAUNCH", "LIQUIDITY_FADE", "INSIDER_ROTATION"} and score < 90:
            return "background"
        if "THIN_LIQUIDITY" in regime_tags:
            return "watch" if score >= 40 else "background"
        if "INSUFFICIENT_HISTORY" in regime_tags:
            return "watch" if score >= 40 else "background"
        if {"RECYCLED_PARTICIPATION", "SELLER_OVERHANG"}.issubset(regime_tags):
            return "priority" if score >= 68 else ("watch" if score >= 40 else "background")
        if (
            score >= 92
            and "deep_retrace" not in risk_flags
            and "negative_net_flow" not in risk_flags
        ):
            return "alpha"
        if score >= 68:
            return "priority"
        if score >= 40:
            return "watch"
        return "background"

    @staticmethod
    def _strategy_name(
        breakout_score: float,
        momentum_score: float,
        features: dict[str, float],
        archetype: dict[str, Any],
    ) -> str:
        primary = archetype.get("primary_archetype")
        if primary == "REVIVAL_RECLAIM":
            return "reclaim_continuation"
        if primary == "HIGH_TIGHT_RANGE":
            return "high_tight_range"
        if primary in {"MOMENTUM_EXPANSION", "EARLY_MICRO_PUMP"}:
            return "momentum_expansion"
        if primary == "BREAKOUT_RUNNER":
            return "breakout_continuation"
        if primary == "FAILED_BREAKOUT" and features["recovery_ratio_from_trough"] < 0.45:
            return "hybrid_flow"
        if (
            features["peak_retrace_pct"] >= 10
            and features["participant_quality_score_v2"] >= 0.55
            and features["wallet_novelty_score"] >= 0.30
        ):
            return "reclaim_continuation"
        if breakout_score >= momentum_score + 8:
            return "breakout_continuation"
        if momentum_score >= breakout_score + 8:
            return "momentum_expansion"
        if (
            features["mcap_to_peak_ratio"] >= 0.9
            and features["buy_pressure_ratio_5m"] >= 0.65
        ):
            return "high_tight_range"
        return "hybrid_flow"

    @staticmethod
    def _entry_bias(
        features: dict[str, float],
        net_5m: float,
        archetype: dict[str, Any],
    ) -> str:
        primary = archetype.get("primary_archetype")
        if primary == "REVIVAL_RECLAIM":
            return "reclaim"
        if (
            primary in {"BREAKOUT_RUNNER", "HIGH_TIGHT_RANGE"}
            and features["mcap_to_peak_ratio"] >= 0.95
            and net_5m > 0
        ):
            return "breakout"
        if primary in {"MOMENTUM_EXPANSION", "EARLY_MICRO_PUMP"}:
            return "momentum"
        if (
            features["trade_acceleration_ratio"] >= 1.4
            and features["buy_pressure_ratio_1m"] >= 0.65
        ):
            return "momentum"
        if (
            features["peak_retrace_pct"] >= 10
            and net_5m > 0
            and features["participant_quality_score_v2"] >= 0.45
        ):
            return "reclaim"
        return "watch_only"

    @staticmethod
    def _setup_state(
        priority_tier: str,
        risk_flags: list[str],
        features: dict[str, float],
        archetype: dict[str, Any],
        *,
        research_confidence_score: float,
        helius_enrichment_status: str,
        helius_profile_completion_ratio: float,
    ) -> str:
        primary = archetype.get("primary_archetype")
        regime_tags = set(archetype.get("regime_tags", []) or [])
        if primary in {"BOT_WASH", "DEAD_LAUNCH", "LIQUIDITY_FADE", "INSIDER_ROTATION"}:
            return "research"

        if "THIN_LIQUIDITY" in regime_tags or "INSUFFICIENT_HISTORY" in regime_tags:
            return (
                "watchlist"
                if features["trade_acceleration_ratio"] >= 1.2
                or features["wallet_novelty_score"] >= 0.35
                else "research"
            )

        strong_adverse_regime = (
            "SELLER_OVERHANG" in regime_tags or "RECYCLED_PARTICIPATION" in regime_tags
        )
        dust_heavy = (
            features.get("dust_trade_share_1m", 0.0) >= 0.35
            or features.get("dust_trade_share_5m", 0.0) >= 0.25
        )
        helius_adverse = (
            features.get("helius_profile_count", 0.0) >= 3
            and (
                features.get("helius_recycled_wallet_share", 0.0) >= 0.50
                or features.get("helius_sniper_wallet_share", 0.0) >= 0.45
                or features.get("helius_creator_shared_funder_score", 0.0) >= 0.45
            )
        )

        alpha_ready = (
            priority_tier == "alpha"
            and "negative_net_flow" not in risk_flags
            and "seller_overhang" not in risk_flags
            and "dust_microflow" not in risk_flags
            and not strong_adverse_regime
            and not helius_adverse
            and not dust_heavy
            and features.get("participant_quality_score_v2", 0.0) >= 0.58
            and features.get("entry_confirmation_score", 0.0) >= 0.60
            and features.get("buy_pressure_ratio_5m", 0.0) >= 0.60
            and features.get("buyer_density_5m", 0.0) >= 0.45
            and features.get("seller_expansion_ratio", 0.0) <= 1.10
            and features.get("mcap_stability_score", 0.0) >= 0.55
            and features.get("non_dust_trades_1m", 0.0) >= 4
            and features.get("non_dust_trades_5m", 0.0) >= 8
            and research_confidence_score >= 0.52
        )
        if alpha_ready:
            if helius_enrichment_status in {"collector_timeout", "collector_error"}:
                return "alert"
            if helius_profile_completion_ratio >= 0.35 or features.get("helius_profile_count", 0.0) <= 0:
                return "paper_entry"

        if (
            priority_tier in {"alpha", "priority"}
            and "negative_net_flow" not in risk_flags
            and not dust_heavy
            and features["participant_quality_score_v2"] >= 0.42
        ):
            return "alert"

        if (
            features["trade_acceleration_ratio"] >= 1.2
            or features["wallet_novelty_score"] >= 0.35
            or features.get("entry_confirmation_score", 0.0) >= 0.52
        ):
            return "watchlist"
        return "research"

    @staticmethod
    def _tie_break_score(
        state: dict[str, Any],
        features: dict[str, float],
        penalties: dict[str, float],
        archetype: dict[str, Any],
        *,
        research_confidence_score: float,
        helius_profile_completion_ratio: float,
    ) -> float:
        net_5m = float(state.get("net_sol_flow_last_5m", 0.0) or 0.0)
        buy_sol_5m = float(state.get("buy_sol_last_5m", 0.0) or 0.0)
        current_mcap = float(
            state.get("current_market_cap_sol", 0.0)
            or state.get("market_cap_sol", 0.0)
            or 0.0
        )
        archetype_confidence = float(archetype.get("archetype_confidence", 0.0) or 0.0)
        score = (
            net_5m * 3.0
            + buy_sol_5m * 1.2
            + min(current_mcap, 120.0) * 0.08
            + features["trade_acceleration_ratio"] * 8.0
            + features["buyer_density_5m"] * 12.0
            + features["buy_pressure_ratio_5m"] * 10.0
            + features["participant_quality_score_v2"] * 12.0
            + features["wallet_novelty_score"] * 6.0
            + features.get("sell_absorption_score", 0.0) * 6.0
            + features.get("mcap_stability_score", 0.0) * 5.0
            + archetype_confidence * 8.0
            + research_confidence_score * 6.0
            + helius_profile_completion_ratio * 4.0
            + features.get("historical_cohort_quality_score", 0.5) * 4.0
            + features.get("historical_cohort_confidence", 0.0) * 3.0
            + max(
                -0.9,
                min(
                    0.9,
                    max(
                        -1.0,
                        min(
                            1.0,
                            (features.get("creator_entity_quality_score", 0.5) - 0.5)
                            / 0.18,
                        ),
                    )
                    * features.get("creator_entity_confidence_score", 0.0)
                    * (1.6 + min(features.get("creator_entity_paper_trade_count", 0.0), 3.0) * 0.08),
                ),
            )
            - features["repeat_wallet_ratio"] * 10.0
            - features["participant_concentration_score"] * 8.0
            - features.get("historical_bad_wallet_share", 0.0) * 4.0
            - sum(penalties.values()) * 0.6
        )
        return round(score, 4)
