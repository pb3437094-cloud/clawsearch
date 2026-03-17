import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RESULTS_DIR = Path("data/strategy_results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
LEADERBOARD_PATH = RESULTS_DIR / "leaderboard.json"

HISTORY_DIR = RESULTS_DIR / "history"
HISTORY_DIR.mkdir(parents=True, exist_ok=True)

BY_ARCHETYPE_DIR = RESULTS_DIR / "by_archetype"
BY_ALERT_BAND_DIR = RESULTS_DIR / "by_alert_band"
BY_ENRICHMENT_DIR = RESULTS_DIR / "by_enrichment_tier"
BY_ENRICHMENT_STATUS_DIR = RESULTS_DIR / "by_enrichment_status"
BY_PROFILE_DEPTH_DIR = RESULTS_DIR / "by_profile_depth"

BY_ARCHETYPE_DIR.mkdir(parents=True, exist_ok=True)
BY_ALERT_BAND_DIR.mkdir(parents=True, exist_ok=True)
BY_ENRICHMENT_DIR.mkdir(parents=True, exist_ok=True)
BY_ENRICHMENT_STATUS_DIR.mkdir(parents=True, exist_ok=True)
BY_PROFILE_DEPTH_DIR.mkdir(parents=True, exist_ok=True)

RECLAIM_PATH = RESULTS_DIR / "reclaim_leaderboard.json"
HIGH_QUALITY_PATH = RESULTS_DIR / "high_quality_participation_leaderboard.json"


def _safe_slug(value: Any, default: str = "unknown") -> str:
    text = str(value or default).strip().lower()
    if not text:
        return default
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in {"_", "-"}:
            cleaned.append(ch)
        elif ch in {" ", "/", "."}:
            cleaned.append("_")
    slug = "".join(cleaned).strip("_")
    return slug or default


class StrategyLeaderboard:
    def __init__(self, max_entries: int = 25) -> None:
        self.max_entries = max_entries

    def _base_row(
        self,
        mint: str,
        snapshot: dict[str, Any],
        state_dict: dict[str, Any],
    ) -> dict[str, Any]:
        quant_features = snapshot.get("quant_features", {}) or {}
        cohort_summary = state_dict.get("helius_wallet_cohort_summary", {}) or {}

        helius_enrichment_tier = snapshot.get(
            "helius_enrichment_tier",
            state_dict.get("helius_enrichment_tier", "none"),
        )
        helius_enrichment_status = snapshot.get(
            "helius_enrichment_status",
            state_dict.get("helius_enrichment_status", "unknown"),
        )
        helius_requested_wallet_count = snapshot.get(
            "helius_requested_wallet_count",
            state_dict.get("helius_requested_wallet_count", 0),
        )
        helius_completed_wallet_count = snapshot.get(
            "helius_completed_wallet_count",
            state_dict.get("helius_completed_wallet_count", 0),
        )
        helius_profile_depth_bucket = snapshot.get(
            "helius_profile_depth_bucket",
            state_dict.get("helius_profile_depth_bucket", "none"),
        )
        helius_profile_completion_ratio = snapshot.get(
            "helius_profile_completion_ratio",
            state_dict.get("helius_profile_completion_ratio", 0.0),
        )
        helius_partial_enrichment = snapshot.get(
            "helius_partial_enrichment",
            state_dict.get("helius_partial_enrichment", False),
        )

        return {
            "rank_mint": mint,
            "symbol": snapshot.get("symbol"),
            "name": snapshot.get("name"),
            "status": snapshot.get("status"),
            "strategy_name": snapshot.get("strategy_name"),
            "primary_archetype": snapshot.get("primary_archetype"),
            "secondary_archetype": snapshot.get("secondary_archetype"),
            "archetype_confidence": snapshot.get("archetype_confidence", 0.0),
            "regime_tags": snapshot.get("regime_tags", []),
            "archetype_reasons": snapshot.get("archetype_reasons", []),
            "entry_bias": snapshot.get("entry_bias"),
            "setup_state": snapshot.get("setup_state"),
            "priority_tier": snapshot.get("priority_tier"),
            "confidence": snapshot.get("confidence"),
            "composite_score": snapshot.get("composite_score", 0.0),
            "tie_break_score": snapshot.get("tie_break_score", 0.0),
            "current_market_cap_sol": state_dict.get("current_market_cap_sol", 0.0),
            "peak_market_cap_sol": state_dict.get("peak_market_cap_sol", 0.0),
            "net_sol_flow_last_5m": state_dict.get("net_sol_flow_last_5m", 0.0),
            "buy_sol_last_5m": state_dict.get("buy_sol_last_5m", 0.0),
            "trades_last_1m": state_dict.get("trades_last_1m", 0),
            "trades_last_5m": state_dict.get("trades_last_5m", 0),
            "unique_buyers_last_5m": state_dict.get("unique_buyers_last_5m", 0),
            "unique_traders_last_5m": state_dict.get("unique_traders_last_5m", 0),
            "participant_quality_score_v2": quant_features.get(
                "participant_quality_score_v2",
                0.0,
            ),
            "wallet_novelty_score": quant_features.get("wallet_novelty_score", 0.0),
            "repeat_wallet_ratio": quant_features.get("repeat_wallet_ratio", 0.0),
            "buyer_overlap_ratio": quant_features.get("buyer_overlap_ratio", 0.0),
            "seller_expansion_ratio": quant_features.get(
                "seller_expansion_ratio",
                0.0,
            ),
            "recovery_ratio_from_trough": quant_features.get(
                "recovery_ratio_from_trough",
                0.0,
            ),
            "mcap_stability_score": quant_features.get("mcap_stability_score", 0.0),
            "dust_trade_share_1m": quant_features.get("dust_trade_share_1m", 0.0),
            "dust_trade_share_5m": quant_features.get("dust_trade_share_5m", 0.0),
            "entry_confirmation_score": snapshot.get(
                "entry_confirmation_score",
                quant_features.get("entry_confirmation_score", 0.0),
            ),
            "analysis_mode": snapshot.get("analysis_mode", "full"),
            "local_composite_score": snapshot.get(
                "local_composite_score",
                snapshot.get("composite_score", 0.0),
            ),
            "enriched_score_delta": snapshot.get("enriched_score_delta", 0.0),
            "helius_enrichment_tier": helius_enrichment_tier,
            "helius_enrichment_status": helius_enrichment_status,
            "helius_trigger_reason": snapshot.get(
                "helius_trigger_reason",
                state_dict.get("helius_trigger_reason"),
            ),
            "helius_last_error": snapshot.get(
                "helius_last_error",
                state_dict.get("helius_last_error"),
            ),
            "helius_cached_error": snapshot.get(
                "helius_cached_error",
                state_dict.get("helius_cached_error"),
            ),
            "helius_wallet_target": snapshot.get(
                "helius_wallet_target",
                state_dict.get("helius_wallet_target", 0),
            ),
            "helius_selected_wallet_count": snapshot.get(
                "helius_selected_wallet_count",
                state_dict.get("helius_selected_wallet_count", 0),
            ),
            "helius_profiled_wallet_count": snapshot.get(
                "helius_profiled_wallet_count",
                state_dict.get("helius_profiled_wallet_count", 0),
            ),
            "helius_requested_wallet_count": helius_requested_wallet_count,
            "helius_completed_wallet_count": helius_completed_wallet_count,
            "helius_profile_depth_bucket": helius_profile_depth_bucket,
            "helius_profile_completion_ratio": helius_profile_completion_ratio,
            "helius_partial_enrichment": helius_partial_enrichment,
            "helius_creator_attempted": snapshot.get(
                "helius_creator_attempted",
                state_dict.get("helius_creator_attempted", False),
            ),
            "helius_creator_profiled": snapshot.get(
                "helius_creator_profiled",
                state_dict.get("helius_creator_profiled", False),
            ),
            "helius_time_budget_seconds": snapshot.get(
                "helius_time_budget_seconds",
                state_dict.get("helius_time_budget_seconds", 0.0),
            ),
            "helius_time_budget_exhausted": snapshot.get(
                "helius_time_budget_exhausted",
                state_dict.get("helius_time_budget_exhausted", False),
            ),
            "helius_wallet_attempt_details": snapshot.get(
                "helius_wallet_attempt_details",
                state_dict.get("helius_wallet_attempt_details", []),
            ),
            "helius_profile_count": quant_features.get(
                "helius_profile_count",
                cohort_summary.get("profile_count", 0.0),
            ),
            "helius_cohort_quality_score": quant_features.get(
                "helius_cohort_quality_score",
                cohort_summary.get("cohort_quality_score", 0.0),
            ),
            "helius_fresh_wallet_share": quant_features.get(
                "helius_fresh_wallet_share",
                cohort_summary.get("fresh_wallet_share", 0.0),
            ),
            "helius_sniper_wallet_share": quant_features.get(
                "helius_sniper_wallet_share",
                cohort_summary.get("sniper_wallet_share", 0.0),
            ),
            "helius_recycled_wallet_share": quant_features.get(
                "helius_recycled_wallet_share",
                cohort_summary.get("recycled_wallet_share", 0.0),
            ),
            "helius_funding_diversity_score": quant_features.get(
                "helius_funding_diversity_score",
                cohort_summary.get("funding_diversity_score", 0.0),
            ),
            "helius_creator_shared_funder_score": quant_features.get(
                "helius_creator_shared_funder_score",
                cohort_summary.get("creator_shared_funder_score", 0.0),
            ),
            "helius_profile_completion_confidence": cohort_summary.get(
                "profile_completion_confidence",
                0.0,
            ),
            "risk_flags": snapshot.get("risk_flags", []),
            "trigger_reasons": snapshot.get("trigger_reasons", []),
            "invalidation_reasons": snapshot.get("invalidation_reasons", []),
            "penalties": snapshot.get("penalties", {}),
            "quant_features": quant_features,
            "why_now": snapshot.get("why_now", []),
        }

    def _sort_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows.sort(
            key=lambda row: (
                row.get("composite_score", 0.0),
                row.get("tie_break_score", 0.0),
                row.get("archetype_confidence", 0.0),
                row.get("helius_cohort_quality_score", 0.0),
                row.get("participant_quality_score_v2", 0.0),
                row.get("entry_confirmation_score", 0.0),
                row.get("helius_profile_completion_ratio", 0.0),
                -row.get("dust_trade_share_1m", 0.0),
                row.get("net_sol_flow_last_5m", 0.0),
                row.get("buy_sol_last_5m", 0.0),
                row.get("current_market_cap_sol", 0.0),
                row.get("trades_last_1m", 0),
            ),
            reverse=True,
        )
        return rows

    def _payload(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        top_rows = rows[: self.max_entries]
        for index, row in enumerate(top_rows, start=1):
            row["rank"] = index

        archetype_counter = Counter(
            row.get("primary_archetype")
            for row in top_rows
            if row.get("primary_archetype")
        )
        tag_counter = Counter(
            tag
            for row in top_rows
            for tag in row.get("regime_tags", [])
        )
        enrichment_tier_counter = Counter(
            row.get("helius_enrichment_tier")
            for row in top_rows
            if row.get("helius_enrichment_tier")
        )
        enrichment_status_counter = Counter(
            row.get("helius_enrichment_status")
            for row in top_rows
            if row.get("helius_enrichment_status")
        )
        profile_depth_counter = Counter(
            row.get("helius_profile_depth_bucket")
            for row in top_rows
            if row.get("helius_profile_depth_bucket")
        )

        return {
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "entry_count": len(top_rows),
            "archetype_breakdown": dict(archetype_counter),
            "regime_tag_breakdown": dict(tag_counter),
            "enrichment_tier_breakdown": dict(enrichment_tier_counter),
            "enrichment_status_breakdown": dict(enrichment_status_counter),
            "profile_depth_breakdown": dict(profile_depth_counter),
            "rows": top_rows,
        }

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    def build_and_persist(
        self,
        strategy_snapshots: dict[str, dict[str, Any]],
        states: dict[str, Any],
    ) -> dict[str, Any]:
        rows = []
        for mint, snapshot in strategy_snapshots.items():
            state = states.get(mint)
            if state is None:
                continue
            state_dict = state.to_dict() if hasattr(state, "to_dict") else dict(state)
            rows.append(self._base_row(mint, snapshot, state_dict))

        rows = self._sort_rows(rows)
        payload = self._payload(rows)
        self._write_json(LEADERBOARD_PATH, payload)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        history_path = HISTORY_DIR / f"leaderboard_{timestamp}.json"
        self._write_json(history_path, payload)

        archetypes = {
            row.get("primary_archetype")
            for row in rows
            if row.get("primary_archetype")
        }
        for archetype in sorted(archetypes):
            archetype_rows = [
                dict(row)
                for row in rows
                if row.get("primary_archetype") == archetype
            ]
            self._write_json(
                BY_ARCHETYPE_DIR / f"{_safe_slug(archetype)}.json",
                self._payload(archetype_rows),
            )

        enrichment_tiers = {
            row.get("helius_enrichment_tier")
            for row in rows
            if row.get("helius_enrichment_tier")
        }
        for enrichment_tier in sorted(enrichment_tiers):
            tier_rows = [
                dict(row)
                for row in rows
                if row.get("helius_enrichment_tier") == enrichment_tier
            ]
            self._write_json(
                BY_ENRICHMENT_DIR / f"{_safe_slug(enrichment_tier)}.json",
                self._payload(tier_rows),
            )

        enrichment_statuses = {
            row.get("helius_enrichment_status")
            for row in rows
            if row.get("helius_enrichment_status")
        }
        for enrichment_status in sorted(enrichment_statuses):
            status_rows = [
                dict(row)
                for row in rows
                if row.get("helius_enrichment_status") == enrichment_status
            ]
            self._write_json(
                BY_ENRICHMENT_STATUS_DIR / f"{_safe_slug(enrichment_status)}.json",
                self._payload(status_rows),
            )

        profile_depth_buckets = {
            row.get("helius_profile_depth_bucket")
            for row in rows
            if row.get("helius_profile_depth_bucket")
        }
        for profile_depth_bucket in sorted(profile_depth_buckets):
            depth_rows = [
                dict(row)
                for row in rows
                if row.get("helius_profile_depth_bucket") == profile_depth_bucket
            ]
            self._write_json(
                BY_PROFILE_DEPTH_DIR / f"{_safe_slug(profile_depth_bucket)}.json",
                self._payload(depth_rows),
            )

        bands = {row.get("setup_state") for row in rows if row.get("setup_state")}
        for band in sorted(bands):
            band_rows = [dict(row) for row in rows if row.get("setup_state") == band]
            self._write_json(
                BY_ALERT_BAND_DIR / f"{_safe_slug(band)}.json",
                self._payload(band_rows),
            )

        reclaim_rows = [
            dict(row)
            for row in rows
            if row.get("strategy_name") == "reclaim_continuation"
            or row.get("primary_archetype") == "REVIVAL_RECLAIM"
            or "RECLAIM_CLEAN" in (row.get("regime_tags") or [])
        ]
        self._write_json(RECLAIM_PATH, self._payload(reclaim_rows))

        high_quality_rows = [
            dict(row)
            for row in rows
            if row.get("participant_quality_score_v2", 0.0) >= 0.65
            or row.get("helius_cohort_quality_score", 0.0) >= 0.60
        ]
        self._write_json(HIGH_QUALITY_PATH, self._payload(high_quality_rows))

        return payload
