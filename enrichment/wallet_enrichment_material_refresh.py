from __future__ import annotations

import json
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from enrichment.helius_client import HeliusClient

try:
    from research.wallet_intelligence import WalletIntelligenceEngine
except Exception:
    WalletIntelligenceEngine = None  # type: ignore[assignment]

try:
    from research.creator_entity_intelligence import CreatorEntityIntelligenceEngine
except Exception:
    CreatorEntityIntelligenceEngine = None  # type: ignore[assignment]

_WALLET_INTELLIGENCE_ENGINE: WalletIntelligenceEngine | None | bool = None
_CREATOR_ENTITY_INTELLIGENCE_ENGINE: CreatorEntityIntelligenceEngine | None | bool = None

ENRICHMENT_DIR = Path("data/enrichment")
WALLET_CACHE_DIR = ENRICHMENT_DIR / "wallet_profiles"
TOKEN_CACHE_DIR = ENRICHMENT_DIR / "token_cohorts"
TOKEN_CONTROL_DIR = ENRICHMENT_DIR / "token_control"
WALLET_CACHE_DIR.mkdir(parents=True, exist_ok=True)
TOKEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
TOKEN_CONTROL_DIR.mkdir(parents=True, exist_ok=True)

WALLET_PROFILE_TTL_SECONDS = 6 * 60 * 60
TOKEN_COHORT_TTL_SECONDS = 180

LIGHT_PARTICIPANT_WALLETS_PER_TOKEN = 5
DEEP_PARTICIPANT_WALLETS_PER_TOKEN = 10

MIN_SECONDS_BETWEEN_LIGHT_ENRICHES = 45
MIN_SECONDS_BETWEEN_DEEP_ENRICHES = 90
TIMEOUT_BACKOFF_SECONDS = 150

MIN_MC_FOR_ENRICHMENT = 30.0
MIN_ENRICHMENT_BUDGET_SECONDS = 0.35
MIN_CREATOR_ENRICHMENT_BUDGET_SECONDS = 0.60
MEANINGFUL_BUY_SOL_THRESHOLD = 0.02

# Round 2.1: improve deep-tier depth by giving the first wallet a fuller read,
# then switching to cheaper follow-up wallet passes so deep enrichment can profile
# more than one participant before the collector budget is exhausted.
LIGHT_PRIMARY_OLDEST_CAP_SECONDS = 0.60
LIGHT_PRIMARY_RECENT_CAP_SECONDS = 0.95
LIGHT_FOLLOWUP_OLDEST_CAP_SECONDS = 0.45
LIGHT_FOLLOWUP_RECENT_CAP_SECONDS = 0.75

DEEP_PRIMARY_OLDEST_CAP_SECONDS = 0.75
DEEP_PRIMARY_RECENT_CAP_SECONDS = 1.35
DEEP_FOLLOWUP_OLDEST_CAP_SECONDS = 0.55
DEEP_FOLLOWUP_RECENT_CAP_SECONDS = 0.95
DEEP_LATE_OLDEST_CAP_SECONDS = 0.40
DEEP_LATE_RECENT_CAP_SECONDS = 0.70

CREATOR_OLDEST_CAP_SECONDS = 0.65
CREATOR_RECENT_CAP_SECONDS = 1.05

LIGHT_PRIMARY_TX_LIMIT = 12
LIGHT_FOLLOWUP_TX_LIMIT = 9
DEEP_PRIMARY_TX_LIMIT = 16
DEEP_FOLLOWUP_TX_LIMIT = 10
DEEP_LATE_TX_LIMIT = 8
CREATOR_TX_LIMIT = 12


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


def _seconds_until(value: str | None) -> float:
    dt = _parse_dt(value)
    if dt is None:
        return 0.0
    return max((dt - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) < 1e-9:
        return default
    return numerator / denominator


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


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


@dataclass
class WalletProfile:
    address: str
    last_enriched_at: str
    tx_sample_count: int
    wallet_age_days: float
    recent_active_days: int
    swap_tx_ratio: float
    transfer_tx_ratio: float
    unknown_tx_ratio: float
    distinct_sources: int
    distinct_mints: int
    inbound_funding_count: int
    funding_wallet_count: int
    top_funder: str | None
    top_funder_share: float
    probable_fresh_wallet: bool
    probable_sniper_wallet: bool
    probable_recycled_wallet: bool
    probable_high_velocity_wallet: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class WalletEnrichmentManager:
    def __init__(
        self,
        client: HeliusClient | None = None,
        *,
        token_ttl_seconds: int = TOKEN_COHORT_TTL_SECONDS,
        wallet_ttl_seconds: int = WALLET_PROFILE_TTL_SECONDS,
        light_wallets: int = LIGHT_PARTICIPANT_WALLETS_PER_TOKEN,
        deep_wallets: int = DEEP_PARTICIPANT_WALLETS_PER_TOKEN,
    ) -> None:
        self.client = client or HeliusClient()
        self.token_ttl_seconds = token_ttl_seconds
        self.wallet_ttl_seconds = wallet_ttl_seconds
        self.light_wallets = light_wallets
        self.deep_wallets = deep_wallets

    @property
    def enabled(self) -> bool:
        return self.client.enabled

    def maybe_attach_enrichment(
        self,
        state: dict[str, Any],
        hint: dict[str, Any] | None = None,
        *,
        time_budget_seconds: float | None = None,
    ) -> dict[str, Any]:
        hint = dict(hint or {})
        merged = dict(state)
        merged.setdefault("helius_enrichment_tier", hint.get("tier", "none"))
        merged.setdefault("helius_trigger_reason", hint.get("reason", "not_requested"))
        merged.setdefault("helius_wallet_target", int(hint.get("max_wallets", 0) or 0))
        merged.setdefault(
            "helius_local_score_hint",
            float(hint.get("local_score", 0.0) or 0.0),
        )
        merged.setdefault("helius_requested_wallet_count", 0)
        merged.setdefault("helius_completed_wallet_count", 0)
        merged.setdefault("helius_selected_wallet_count", 0)
        merged.setdefault("helius_profiled_wallet_count", 0)
        merged.setdefault("helius_profile_depth_bucket", "none")
        merged.setdefault("helius_profile_completion_ratio", 0.0)
        merged.setdefault("helius_partial_enrichment", False)
        merged.setdefault("helius_cached_error", None)
        merged.setdefault("helius_wallet_attempt_details", [])
        merged.setdefault("creator_entity_key", None)

        if not self.enabled:
            merged["helius_enrichment_status"] = "disabled"
            merged["helius_last_error"] = "missing_api_key"
            return merged

        mint = str(state.get("mint") or "")
        if not mint:
            merged["helius_enrichment_status"] = "missing_mint"
            merged["helius_last_error"] = "missing_mint"
            return merged

        plan = self._resolve_plan(state, hint)
        merged["helius_enrichment_tier"] = plan["tier"]
        merged["helius_trigger_reason"] = plan["reason"]
        merged["helius_wallet_target"] = int(plan["max_wallets"])
        merged["helius_local_score_hint"] = float(
            plan.get("local_score", hint.get("local_score", 0.0)) or 0.0
        )

        if plan["tier"] == "none":
            merged["helius_enrichment_status"] = "unqualified"
            merged["helius_last_error"] = None
            return merged

        control = self._load_control(mint) or {}
        cached = self._load_token_cache(mint)

        backoff_remaining = _seconds_until(control.get("timeout_backoff_until"))
        if backoff_remaining > 0 and not plan.get("force"):
            if cached:
                prepared = self._prepare_cached_payload(cached, "timeout_backoff")
                control_changed = False
                if self._maybe_record_wallet_memory_from_payload(
                    mint=mint,
                    state=state,
                    payload=prepared,
                    control=control,
                ):
                    control["wallet_memory_recorded"] = True
                    control["wallet_memory_recorded_at"] = _now()
                    control_changed = True
                if self._maybe_record_creator_entity_from_payload(
                    mint=mint,
                    state=state,
                    payload=prepared,
                    control=control,
                ):
                    control["creator_entity_recorded"] = True
                    control["creator_entity_recorded_at"] = _now()
                    control_changed = True
                if control_changed:
                    self._save_control(mint, control)
                merged.update(prepared)
            else:
                merged["helius_enrichment_status"] = "timeout_backoff"
                merged["helius_last_error"] = None
                merged["helius_cached_error"] = control.get("last_error")
            merged["helius_backoff_seconds_remaining"] = round(backoff_remaining, 4)
            return merged

        fingerprint = self._fingerprint(state, plan)
        if cached and cached.get("fingerprint") == fingerprint:
            age = _seconds_since(cached.get("helius_last_enriched_at"))
            if age is not None and age <= self.token_ttl_seconds:
                prepared = self._prepare_cached_payload(cached, "cache_hit")
                control_changed = False
                if self._maybe_record_wallet_memory_from_payload(
                    mint=mint,
                    state=state,
                    payload=prepared,
                    control=control,
                ):
                    control["wallet_memory_recorded"] = True
                    control["wallet_memory_recorded_at"] = _now()
                    control_changed = True
                if self._maybe_record_creator_entity_from_payload(
                    mint=mint,
                    state=state,
                    payload=prepared,
                    control=control,
                ):
                    control["creator_entity_recorded"] = True
                    control["creator_entity_recorded_at"] = _now()
                    control_changed = True
                if control_changed:
                    self._save_control(mint, control)
                merged.update(prepared)
                return merged

        if cached:
            age = _seconds_since(cached.get("helius_last_enriched_at"))
            if (
                age is not None
                and age < plan["cooldown_seconds"]
                and not plan.get("force")
                and not self._should_bypass_cooldown_for_material_change(
                    cached=cached,
                    state=state,
                    plan=plan,
                    age_seconds=float(age),
                )
            ):
                prepared = self._prepare_cached_payload(cached, "cache_throttled")
                control_changed = False
                if self._maybe_record_wallet_memory_from_payload(
                    mint=mint,
                    state=state,
                    payload=prepared,
                    control=control,
                ):
                    control["wallet_memory_recorded"] = True
                    control["wallet_memory_recorded_at"] = _now()
                    control_changed = True
                if self._maybe_record_creator_entity_from_payload(
                    mint=mint,
                    state=state,
                    payload=prepared,
                    control=control,
                ):
                    control["creator_entity_recorded"] = True
                    control["creator_entity_recorded_at"] = _now()
                    control_changed = True
                if control_changed:
                    self._save_control(mint, control)
                merged.update(prepared)
                return merged

        payload = self.enrich_token_state(
            state,
            tier=plan["tier"],
            max_wallets=int(plan["max_wallets"]),
            time_budget_seconds=time_budget_seconds,
        )

        if not payload:
            merged["helius_enrichment_status"] = "no_enrichment_result"
            merged["helius_last_error"] = self.client.last_error
            merged["helius_cached_error"] = None
            self._save_control(
                mint,
                {
                    **control,
                    "last_attempt_at": _now(),
                    "last_error": self.client.last_error,
                },
            )
            return merged

        payload = {
            "fingerprint": fingerprint,
            **payload,
            "helius_enrichment_tier": plan["tier"],
            "helius_trigger_reason": plan["reason"],
            "helius_wallet_target": int(plan["max_wallets"]),
            "helius_local_score_hint": float(
                plan.get("local_score", hint.get("local_score", 0.0)) or 0.0
            ),
        }

        self._save_token_cache(mint, payload)

        control_update = {
            **control,
            "last_attempt_at": _now(),
            "last_tier": plan["tier"],
            "last_error": payload.get("helius_last_error"),
            "last_profile_depth_bucket": payload.get("helius_profile_depth_bucket"),
        }

        if self._maybe_record_wallet_memory_from_payload(
            mint=mint,
            state=state,
            payload=payload,
            control=control_update,
        ):
            control_update["wallet_memory_recorded"] = True
            control_update["wallet_memory_recorded_at"] = _now()

        if self._maybe_record_creator_entity_from_payload(
            mint=mint,
            state=state,
            payload=payload,
            control=control_update,
        ):
            control_update["creator_entity_recorded"] = True
            control_update["creator_entity_recorded_at"] = _now()

        status = str(payload.get("helius_enrichment_status") or "")
        if status in {
            "light_enriched",
            "deep_enriched",
            "light_partial",
            "deep_partial",
            "creator_only",
            "cache_hit",
            "cache_throttled",
        }:
            control_update["last_success_at"] = (
                payload.get("helius_last_enriched_at") or _now()
            )
            control_update.pop("timeout_backoff_until", None)
        elif self._is_timeout_like(payload.get("helius_last_error")) or payload.get(
            "helius_time_budget_exhausted"
        ):
            control_update["timeout_backoff_until"] = datetime.fromtimestamp(
                time.time() + TIMEOUT_BACKOFF_SECONDS,
                tz=timezone.utc,
            ).isoformat()

        self._save_control(mint, control_update)

        merged.update(payload)
        merged["helius_enrichment_status"] = payload.get(
            "helius_enrichment_status",
            "enriched",
        )
        merged["helius_last_error"] = payload.get("helius_last_error")
        return merged

    def enrich_token_state(
        self,
        state: dict[str, Any],
        *,
        tier: str,
        max_wallets: int,
        time_budget_seconds: float | None = None,
    ) -> dict[str, Any]:
        self.client.last_error = None

        creator_wallet = str(state.get("creator_wallet") or "").strip() or None
        participant_wallets = self._select_participant_wallets(
            state,
            max_wallets=max_wallets,
        )

        profiles: list[WalletProfile] = []
        attempted_wallets: list[str] = []
        wallet_attempt_details: list[dict[str, Any]] = []
        budget_exhausted = False
        deadline = (
            time.monotonic() + float(time_budget_seconds)
            if time_budget_seconds
            else None
        )

        for wallet_index, address in enumerate(participant_wallets):
            remaining = self._remaining_budget(deadline)
            if remaining is not None and remaining < MIN_ENRICHMENT_BUDGET_SECONDS:
                budget_exhausted = True
                break

            request_plan = self._wallet_request_plan(
                tier=tier,
                wallet_index=wallet_index,
                remaining_budget_seconds=remaining,
            )

            attempted_wallets.append(address)
            profile = self.enrich_wallet(
                address,
                deadline=deadline,
                oldest_request_cap_seconds=float(request_plan["oldest_cap_seconds"]),
                recent_request_cap_seconds=float(request_plan["recent_cap_seconds"]),
                recent_tx_limit=int(request_plan["recent_tx_limit"]),
            )

            wallet_attempt_details.append(
                {
                    "address": address,
                    "wallet_index": wallet_index,
                    "oldest_cap_seconds": round(
                        float(request_plan["oldest_cap_seconds"]), 4
                    ),
                    "recent_cap_seconds": round(
                        float(request_plan["recent_cap_seconds"]), 4
                    ),
                    "recent_tx_limit": int(request_plan["recent_tx_limit"]),
                    "profiled": bool(profile),
                    "error": None if profile is not None else self.client.last_error,
                }
            )

            if profile is not None:
                profiles.append(profile)

        creator_profile = None
        creator_attempted = False
        creator_expected = bool(creator_wallet and tier == "deep")
        if creator_expected and creator_wallet:
            remaining = self._remaining_budget(deadline)
            if remaining is None or remaining >= MIN_CREATOR_ENRICHMENT_BUDGET_SECONDS:
                creator_attempted = True
                creator_profile = self.enrich_wallet(
                    creator_wallet,
                    deadline=deadline,
                    oldest_request_cap_seconds=CREATOR_OLDEST_CAP_SECONDS,
                    recent_request_cap_seconds=CREATOR_RECENT_CAP_SECONDS,
                    recent_tx_limit=CREATOR_TX_LIMIT,
                )
            else:
                budget_exhausted = True

        cohort_summary = self._aggregate_profiles(
            profiles,
            creator_profile=creator_profile,
        )

        requested_wallet_count = len(participant_wallets)
        completed_wallet_count = len(profiles)
        profile_completion_ratio = _safe_div(
            completed_wallet_count,
            requested_wallet_count,
            0.0,
        )
        partial_enrichment = self._is_partial_enrichment(
            tier=tier,
            requested_wallet_count=requested_wallet_count,
            completed_wallet_count=completed_wallet_count,
            creator_expected=creator_expected,
            creator_attempted=creator_attempted,
            creator_profile=creator_profile,
            budget_exhausted=budget_exhausted,
            last_error=self.client.last_error,
        )

        status = self._derive_status(
            tier=tier,
            requested_wallet_count=requested_wallet_count,
            completed_wallet_count=completed_wallet_count,
            creator_profile=creator_profile,
            partial_enrichment=partial_enrichment,
            budget_exhausted=budget_exhausted,
            last_error=self.client.last_error,
        )

        return {
            "helius_last_enriched_at": _now(),
            "helius_enrichment_status": status,
            "helius_last_error": self.client.last_error,
            "helius_cached_error": None,
            "helius_wallet_cohort_summary": cohort_summary,
            "helius_creator_profile": (
                creator_profile.to_dict() if creator_profile else {}
            ),
            "helius_selected_wallets": participant_wallets,
            "helius_attempted_wallets": attempted_wallets,
            "helius_wallet_attempt_details": wallet_attempt_details,
            "helius_selected_wallet_count": len(participant_wallets),
            "helius_profiled_wallet_count": len(profiles),
            "helius_requested_wallet_count": requested_wallet_count,
            "helius_completed_wallet_count": completed_wallet_count,
            "helius_profile_completion_ratio": round(profile_completion_ratio, 4),
            "helius_profile_depth_bucket": self._profile_depth_bucket(
                completed_wallet_count
            ),
            "helius_partial_enrichment": partial_enrichment,
            "helius_creator_attempted": creator_attempted,
            "helius_creator_profiled": bool(creator_profile),
            "helius_time_budget_seconds": float(time_budget_seconds or 0.0),
            "helius_time_budget_exhausted": budget_exhausted,
        }

    def enrich_wallet(
        self,
        address: str | None,
        *,
        deadline: float | None = None,
        oldest_request_cap_seconds: float = LIGHT_PRIMARY_OLDEST_CAP_SECONDS,
        recent_request_cap_seconds: float = LIGHT_PRIMARY_RECENT_CAP_SECONDS,
        recent_tx_limit: int = LIGHT_PRIMARY_TX_LIMIT,
    ) -> WalletProfile | None:
        address = str(address or "").strip()
        if not address or not self.enabled:
            return None

        cached = self._load_wallet_profile(address)
        if cached is not None:
            age = _seconds_since(cached.get("last_enriched_at"))
            if age is not None and age <= self.wallet_ttl_seconds:
                return WalletProfile(**cached)

        oldest_timeout = self._request_timeout(
            deadline,
            cap=oldest_request_cap_seconds,
        )
        if oldest_timeout is None:
            self.client.last_error = "budget_exhausted"
            return None

        oldest = self.client.get_transactions_for_address(
            address,
            limit=1,
            sort_order="asc",
            transaction_details="signatures",
            timeout_seconds=oldest_timeout,
        )
        oldest_rows = oldest.get("data", []) if isinstance(oldest, dict) else []
        oldest_ts = oldest_rows[0].get("blockTime") if oldest_rows else None

        recent_timeout = self._request_timeout(
            deadline,
            cap=recent_request_cap_seconds,
        )
        if recent_timeout is None:
            self.client.last_error = "budget_exhausted"
            return None

        recent_txs = (
            self.client.get_enhanced_transactions_by_address(
                address,
                limit=max(int(recent_tx_limit), 1),
                timeout_seconds=recent_timeout,
            )
            or []
        )

        if self.client.last_error and not recent_txs and oldest_ts is None:
            return None

        if not recent_txs and oldest_ts is None:
            return None

        profile = self._build_wallet_profile(address, recent_txs, oldest_ts)
        self._save_wallet_profile(profile)
        self.client.last_error = None
        return profile

    def _build_wallet_profile(
        self,
        address: str,
        recent_txs: list[dict[str, Any]],
        oldest_block_time: int | float | None,
    ) -> WalletProfile:
        tx_sample_count = len(recent_txs)
        type_counter: Counter[str] = Counter()
        source_counter: Counter[str] = Counter()
        mint_set: set[str] = set()
        inbound_funders: list[str] = []
        active_days: set[str] = set()

        for tx in recent_txs:
            tx_type = str(tx.get("type") or "UNKNOWN").upper()
            source = str(tx.get("source") or "UNKNOWN").upper()
            type_counter[tx_type] += 1
            source_counter[source] += 1

            ts = tx.get("timestamp")
            if ts:
                try:
                    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                    active_days.add(dt.date().isoformat())
                except Exception:
                    pass

            for transfer in tx.get("tokenTransfers", []) or []:
                mint = transfer.get("mint")
                if mint:
                    mint_set.add(str(mint))

            for native_transfer in tx.get("nativeTransfers", []) or []:
                to_user = native_transfer.get("toUserAccount")
                from_user = native_transfer.get("fromUserAccount")
                amount = float(native_transfer.get("amount", 0.0) or 0.0)
                if str(to_user) == address and amount > 0 and from_user:
                    inbound_funders.append(str(from_user))

        top_funder = None
        top_funder_share = 0.0
        funder_counter = Counter(inbound_funders)
        if funder_counter:
            top_funder, top_count = funder_counter.most_common(1)[0]
            top_funder_share = _safe_div(top_count, len(inbound_funders), 0.0)

        if oldest_block_time:
            try:
                oldest_dt = datetime.fromtimestamp(
                    float(oldest_block_time),
                    tz=timezone.utc,
                )
                wallet_age_days = max(
                    (datetime.now(timezone.utc) - oldest_dt).total_seconds() / 86400.0,
                    0.0,
                )
            except Exception:
                wallet_age_days = 0.0
        else:
            wallet_age_days = 0.0

        swap_ratio = _safe_div(
            sum(count for t, count in type_counter.items() if "SWAP" in t),
            tx_sample_count,
            0.0,
        )
        transfer_ratio = _safe_div(
            type_counter.get("TRANSFER", 0),
            tx_sample_count,
            0.0,
        )
        unknown_ratio = _safe_div(
            type_counter.get("UNKNOWN", 0),
            tx_sample_count,
            0.0,
        )
        recent_active_days = len(active_days)
        distinct_sources = len(source_counter)
        distinct_mints = len(mint_set)

        probable_fresh_wallet = wallet_age_days <= 14 and tx_sample_count <= 12
        probable_sniper_wallet = (
            wallet_age_days <= 21
            and tx_sample_count >= 8
            and swap_ratio >= 0.45
            and distinct_mints >= 4
        )
        probable_recycled_wallet = (
            wallet_age_days >= 45
            and tx_sample_count >= 15
            and swap_ratio >= 0.35
            and distinct_sources <= 3
        )
        probable_high_velocity_wallet = tx_sample_count >= 20 and recent_active_days <= 2

        return WalletProfile(
            address=address,
            last_enriched_at=_now(),
            tx_sample_count=tx_sample_count,
            wallet_age_days=round(wallet_age_days, 4),
            recent_active_days=recent_active_days,
            swap_tx_ratio=round(swap_ratio, 4),
            transfer_tx_ratio=round(transfer_ratio, 4),
            unknown_tx_ratio=round(unknown_ratio, 4),
            distinct_sources=distinct_sources,
            distinct_mints=distinct_mints,
            inbound_funding_count=len(inbound_funders),
            funding_wallet_count=len(funder_counter),
            top_funder=top_funder,
            top_funder_share=round(top_funder_share, 4),
            probable_fresh_wallet=probable_fresh_wallet,
            probable_sniper_wallet=probable_sniper_wallet,
            probable_recycled_wallet=probable_recycled_wallet,
            probable_high_velocity_wallet=probable_high_velocity_wallet,
        )

    def _aggregate_profiles(
        self,
        profiles: list[WalletProfile],
        *,
        creator_profile: WalletProfile | None = None,
    ) -> dict[str, Any]:
        if not profiles:
            return {
                "profile_count": 0,
                "avg_wallet_age_days": 0.0,
                "median_wallet_age_days": 0.0,
                "fresh_wallet_share": 0.0,
                "sniper_wallet_share": 0.0,
                "recycled_wallet_share": 0.0,
                "high_velocity_wallet_share": 0.0,
                "funding_diversity_score": 0.0,
                "top_funder_concentration_score": 0.0,
                "creator_shared_funder_score": 0.0,
                "creator_wallet_age_days": (
                    round(float(creator_profile.wallet_age_days), 4)
                    if creator_profile
                    else 0.0
                ),
                "creator_probable_fresh_wallet": (
                    bool(creator_profile.probable_fresh_wallet)
                    if creator_profile
                    else False
                ),
                "creator_probable_sniper_wallet": (
                    bool(creator_profile.probable_sniper_wallet)
                    if creator_profile
                    else False
                ),
                "creator_probable_recycled_wallet": (
                    bool(creator_profile.probable_recycled_wallet)
                    if creator_profile
                    else False
                ),
                "profile_depth_bucket": "none",
                "profile_completion_confidence": 0.0,
                "cohort_quality_score": 0.0,
            }

        ages = [float(profile.wallet_age_days) for profile in profiles]
        funders = [profile.top_funder for profile in profiles if profile.top_funder]
        funder_counter = Counter(funders)

        fresh_share = _safe_div(
            sum(1 for profile in profiles if profile.probable_fresh_wallet),
            len(profiles),
            0.0,
        )
        sniper_share = _safe_div(
            sum(1 for profile in profiles if profile.probable_sniper_wallet),
            len(profiles),
            0.0,
        )
        recycled_share = _safe_div(
            sum(1 for profile in profiles if profile.probable_recycled_wallet),
            len(profiles),
            0.0,
        )
        high_velocity_share = _safe_div(
            sum(1 for profile in profiles if profile.probable_high_velocity_wallet),
            len(profiles),
            0.0,
        )

        avg_age = sum(ages) / len(ages)
        median_age = median(ages) if ages else 0.0
        funding_diversity = _clamp(_safe_div(len(funder_counter), len(profiles), 0.0))
        top_funder_concentration = (
            _safe_div(funder_counter.most_common(1)[0][1], len(profiles), 0.0)
            if funder_counter
            else 0.0
        )

        creator_shared_funder_score = 0.0
        if creator_profile and creator_profile.top_funder:
            creator_shared_funder_score = _safe_div(
                sum(
                    1
                    for profile in profiles
                    if profile.top_funder == creator_profile.top_funder
                ),
                len(profiles),
                0.0,
            )

        age_balance_score = 1.0 - _clamp(abs(avg_age - 21.0) / 45.0)
        cohort_quality_score = (
            age_balance_score * 0.18
            + (1.0 - fresh_share) * 0.10
            + (1.0 - recycled_share) * 0.24
            + (1.0 - sniper_share) * 0.22
            + funding_diversity * 0.16
            + (1.0 - top_funder_concentration) * 0.10
        )

        if creator_profile and creator_profile.probable_sniper_wallet:
            cohort_quality_score *= 0.88
        if creator_profile and creator_profile.probable_recycled_wallet:
            cohort_quality_score *= 0.90
        if creator_shared_funder_score >= 0.45:
            cohort_quality_score *= 0.85

        depth_confidence_score = _clamp(_safe_div(len(profiles), 5.0, 0.0))
        cohort_quality_score *= 0.70 + (0.30 * depth_confidence_score)

        return {
            "profile_count": len(profiles),
            "avg_wallet_age_days": round(avg_age, 4),
            "median_wallet_age_days": round(median_age, 4),
            "fresh_wallet_share": round(fresh_share, 4),
            "sniper_wallet_share": round(sniper_share, 4),
            "recycled_wallet_share": round(recycled_share, 4),
            "high_velocity_wallet_share": round(high_velocity_share, 4),
            "funding_diversity_score": round(funding_diversity, 4),
            "top_funder_concentration_score": round(top_funder_concentration, 4),
            "creator_shared_funder_score": round(creator_shared_funder_score, 4),
            "creator_wallet_age_days": (
                round(float(creator_profile.wallet_age_days), 4)
                if creator_profile
                else 0.0
            ),
            "creator_probable_fresh_wallet": (
                bool(creator_profile.probable_fresh_wallet)
                if creator_profile
                else False
            ),
            "creator_probable_sniper_wallet": (
                bool(creator_profile.probable_sniper_wallet)
                if creator_profile
                else False
            ),
            "creator_probable_recycled_wallet": (
                bool(creator_profile.probable_recycled_wallet)
                if creator_profile
                else False
            ),
            "profile_depth_bucket": self._profile_depth_bucket(len(profiles)),
            "profile_completion_confidence": round(depth_confidence_score, 4),
            "cohort_quality_score": round(_clamp(cohort_quality_score), 4),
        }

    def _should_bypass_cooldown_for_material_change(
        self,
        *,
        cached: dict[str, Any],
        state: dict[str, Any],
        plan: dict[str, Any],
        age_seconds: float,
    ) -> bool:
        if bool(plan.get("force", False)):
            return True

        plan_tier = str(plan.get("tier") or "none")
        if plan_tier == "none":
            return False

        cached_tier = str(cached.get("helius_enrichment_tier") or "none")
        cached_status = str(cached.get("helius_enrichment_status") or "")
        current_score = float(
            plan.get("local_score", 0.0)
            or state.get("composite_score", 0.0)
            or 0.0
        )
        cached_score = float(
            cached.get("composite_score", cached.get("local_score", 0.0)) or 0.0
        )
        score_jump = current_score - cached_score

        current_mcap = float(
            state.get("current_market_cap_sol", 0.0)
            or state.get("market_cap_sol", 0.0)
            or 0.0
        )
        cached_mcap = float(
            cached.get("current_market_cap_sol", 0.0)
            or cached.get("market_cap_sol", 0.0)
            or 0.0
        )
        mcap_ratio = (
            current_mcap / max(cached_mcap, 1.0)
            if cached_mcap > 0.0
            else 1.0
        )

        partial_status = cached_status in {"light_partial", "deep_partial"}
        partial_payload = bool(cached.get("helius_partial_enrichment", False))
        time_budget_exhausted = bool(cached.get("helius_time_budget_exhausted", False))

        if (
            plan_tier == "deep"
            and cached_tier != "deep"
            and age_seconds >= 15.0
            and current_score >= 90.0
        ):
            return True

        if (
            (partial_status or partial_payload or time_budget_exhausted)
            and age_seconds >= 20.0
            and (
                (current_score >= 110.0 and score_jump >= 8.0)
                or (current_mcap >= 60.0 and mcap_ratio >= 1.20)
            )
        ):
            return True

        if (
            cached_status == "cache_throttled"
            and age_seconds >= 30.0
            and current_score >= 130.0
            and mcap_ratio >= 1.15
        ):
            return True

        return False

    def _resolve_plan(self, state: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
        tier = str(hint.get("tier") or "none")

        if tier == "deep":
            return {
                "tier": "deep",
                "reason": hint.get("reason") or "deep_signal",
                "max_wallets": int(
                    hint.get("max_wallets", self.deep_wallets) or self.deep_wallets
                ),
                "cooldown_seconds": int(
                    hint.get("cooldown_seconds", MIN_SECONDS_BETWEEN_DEEP_ENRICHES)
                    or MIN_SECONDS_BETWEEN_DEEP_ENRICHES
                ),
                "force": bool(hint.get("force", False)),
                "local_score": float(hint.get("local_score", 0.0) or 0.0),
            }

        if tier == "light":
            return {
                "tier": "light",
                "reason": hint.get("reason") or "shortlist_signal",
                "max_wallets": int(
                    hint.get("max_wallets", self.light_wallets) or self.light_wallets
                ),
                "cooldown_seconds": int(
                    hint.get("cooldown_seconds", MIN_SECONDS_BETWEEN_LIGHT_ENRICHES)
                    or MIN_SECONDS_BETWEEN_LIGHT_ENRICHES
                ),
                "force": bool(hint.get("force", False)),
                "local_score": float(hint.get("local_score", 0.0) or 0.0),
            }

        if self._token_qualifies(state):
            return {
                "tier": "light",
                "reason": "fallback_qualified",
                "max_wallets": self.light_wallets,
                "cooldown_seconds": MIN_SECONDS_BETWEEN_LIGHT_ENRICHES,
                "force": False,
                "local_score": float(hint.get("local_score", 0.0) or 0.0),
            }

        return {
            "tier": "none",
            "reason": "not_qualified",
            "max_wallets": 0,
            "cooldown_seconds": 0,
            "force": False,
            "local_score": float(hint.get("local_score", 0.0) or 0.0),
        }

    def _token_qualifies(self, state: dict[str, Any]) -> bool:
        current_mcap = float(
            state.get("current_market_cap_sol", 0.0)
            or state.get("market_cap_sol", 0.0)
            or 0.0
        )
        trades_1m = int(state.get("trades_last_1m", 0) or 0)
        trades_5m = int(state.get("trades_last_5m", 0) or 0)
        unique_buyers = int(
            state.get("unique_buyers_last_5m", 0)
            or state.get("unique_buyers", 0)
            or 0
        )
        buys_5m = int(state.get("buys_last_5m", 0) or 0)
        status = str(state.get("status") or "")
        return (
            current_mcap >= MIN_MC_FOR_ENRICHMENT
            or trades_1m >= 3
            or trades_5m >= 8
            or buys_5m >= 5
            or unique_buyers >= 3
            or status in {"watching", "reawakened", "escalated"}
        )

    def _fingerprint(self, state: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
        participant_wallets = self._select_participant_wallets(
            state,
            max_wallets=int(
                plan.get("max_wallets", self.light_wallets) or self.light_wallets
            ),
        )
        current_mcap = float(
            state.get("current_market_cap_sol", 0.0)
            or state.get("market_cap_sol", 0.0)
            or 0.0
        )
        mcap_bucket = int(current_mcap // 10)
        return {
            "tier": plan.get("tier"),
            "status": str(state.get("status") or ""),
            "trades_1m": int(state.get("trades_last_1m", 0) or 0),
            "trades_5m_bucket": int((int(state.get("trades_last_5m", 0) or 0)) // 2),
            "unique_buyers_5m_bucket": int(
                (int(state.get("unique_buyers_last_5m", 0) or 0)) // 2
            ),
            "market_cap_bucket": mcap_bucket,
            "participant_wallets": participant_wallets,
            "creator_wallet": state.get("creator_wallet"),
        }

    def _select_participant_wallets(
        self,
        state: dict[str, Any],
        *,
        max_wallets: int,
    ) -> list[str]:
        recent_trades = list(state.get("recent_trades") or [])
        trader_counts: Counter[str] = Counter()
        buy_sizes: Counter[str] = Counter()
        meaningful_first_buys: list[str] = []
        all_first_buys: list[str] = []
        seen_meaningful: set[str] = set()
        seen_buys: set[str] = set()

        for trade in recent_trades:
            wallet = str(trade.get("trader_wallet") or "").strip()
            if not wallet:
                continue

            trader_counts[wallet] += 1

            if str(trade.get("tx_type") or "").lower() != "buy":
                continue

            effective_sol = float(
                trade.get("effective_sol_amount", trade.get("sol_amount", 0.0)) or 0.0
            )
            buy_sizes[wallet] += effective_sol

            if wallet not in seen_buys:
                all_first_buys.append(wallet)
                seen_buys.add(wallet)

            if (
                effective_sol >= MEANINGFUL_BUY_SOL_THRESHOLD
                and wallet not in seen_meaningful
            ):
                meaningful_first_buys.append(wallet)
                seen_meaningful.add(wallet)

        ranked_by_size = [wallet for wallet, _ in buy_sizes.most_common()]
        ranked_by_repeat = [
            wallet for wallet, count in trader_counts.most_common() if count >= 2
        ]
        ranked_all = [wallet for wallet, _ in trader_counts.most_common()]

        ordered: list[str] = []
        for group in (
            meaningful_first_buys[:3],
            all_first_buys[:3],
            ranked_by_size[:4],
            ranked_by_repeat[:2],
            all_first_buys[3:5],
            ranked_all,
        ):
            for wallet in group:
                if wallet not in ordered:
                    ordered.append(wallet)
                if len(ordered) >= max_wallets:
                    return ordered[:max_wallets]
        return ordered[:max_wallets]

    def _wallet_cache_path(self, address: str) -> Path:
        return WALLET_CACHE_DIR / f"{address}.json"

    def _token_cache_path(self, mint: str) -> Path:
        return TOKEN_CACHE_DIR / f"{mint}.json"

    def _control_path(self, mint: str) -> Path:
        return TOKEN_CONTROL_DIR / f"{mint}.json"

    def _load_wallet_profile(self, address: str) -> dict[str, Any] | None:
        path = self._wallet_cache_path(address)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _save_wallet_profile(self, profile: WalletProfile) -> None:
        path = self._wallet_cache_path(profile.address)
        path.write_text(
            json.dumps(profile.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _load_token_cache(self, mint: str) -> dict[str, Any] | None:
        path = self._token_cache_path(mint)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _save_token_cache(self, mint: str, payload: dict[str, Any]) -> None:
        path = self._token_cache_path(mint)
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _load_control(self, mint: str) -> dict[str, Any] | None:
        path = self._control_path(mint)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _save_control(self, mint: str, payload: dict[str, Any]) -> None:
        path = self._control_path(mint)
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    @staticmethod
    def _remaining_budget(deadline: float | None) -> float | None:
        if deadline is None:
            return None
        return max(deadline - time.monotonic(), 0.0)

    def _request_timeout(self, deadline: float | None, *, cap: float) -> float | None:
        remaining = self._remaining_budget(deadline)
        if remaining is None:
            return min(float(self.client.timeout_seconds), cap)
        if remaining < MIN_ENRICHMENT_BUDGET_SECONDS:
            return None
        return max(min(float(self.client.timeout_seconds), remaining, cap), 0.2)

    def _wallet_request_plan(
        self,
        *,
        tier: str,
        wallet_index: int,
        remaining_budget_seconds: float | None,
    ) -> dict[str, Any]:
        if tier == "deep":
            if wallet_index == 0:
                oldest_cap = DEEP_PRIMARY_OLDEST_CAP_SECONDS
                recent_cap = DEEP_PRIMARY_RECENT_CAP_SECONDS
                tx_limit = DEEP_PRIMARY_TX_LIMIT
            elif wallet_index == 1:
                oldest_cap = DEEP_FOLLOWUP_OLDEST_CAP_SECONDS
                recent_cap = DEEP_FOLLOWUP_RECENT_CAP_SECONDS
                tx_limit = DEEP_FOLLOWUP_TX_LIMIT
            else:
                oldest_cap = DEEP_LATE_OLDEST_CAP_SECONDS
                recent_cap = DEEP_LATE_RECENT_CAP_SECONDS
                tx_limit = DEEP_LATE_TX_LIMIT
        else:
            if wallet_index == 0:
                oldest_cap = LIGHT_PRIMARY_OLDEST_CAP_SECONDS
                recent_cap = LIGHT_PRIMARY_RECENT_CAP_SECONDS
                tx_limit = LIGHT_PRIMARY_TX_LIMIT
            else:
                oldest_cap = LIGHT_FOLLOWUP_OLDEST_CAP_SECONDS
                recent_cap = LIGHT_FOLLOWUP_RECENT_CAP_SECONDS
                tx_limit = LIGHT_FOLLOWUP_TX_LIMIT

        if remaining_budget_seconds is not None:
            if remaining_budget_seconds < 1.20:
                oldest_cap = min(oldest_cap, 0.35)
                recent_cap = min(recent_cap, 0.55)
                tx_limit = min(tx_limit, 6)
            elif remaining_budget_seconds < 1.80:
                oldest_cap = min(oldest_cap, 0.45)
                recent_cap = min(recent_cap, 0.70)
                tx_limit = min(tx_limit, 8)

        return {
            "oldest_cap_seconds": oldest_cap,
            "recent_cap_seconds": recent_cap,
            "recent_tx_limit": tx_limit,
        }

    def _maybe_record_creator_entity_from_payload(
        self,
        *,
        mint: str,
        state: dict[str, Any],
        payload: dict[str, Any],
        control: dict[str, Any],
    ) -> bool:
        if bool(control.get("creator_entity_recorded", False)):
            existing_entity_key = str(payload.get("creator_entity_key") or "").strip()
            return bool(existing_entity_key)

        status = str(payload.get("helius_enrichment_status") or "")
        allowed_statuses = {
            "light_enriched",
            "deep_enriched",
            "light_partial",
            "deep_partial",
            "creator_only",
            "cache_hit",
            "cache_throttled",
            "timeout_backoff",
        }
        if status not in allowed_statuses:
            return False

        engine = _get_creator_entity_intelligence_engine()
        if engine is None:
            return False

        creator_wallet = str(
            state.get("creator_wallet")
            or payload.get("creator_wallet")
            or ""
        ).strip() or None
        creator_profile = payload.get("helius_creator_profile", {}) or {}
        first_hop_funder = str(
            payload.get("wallet_memory_funder_wallet")
            or creator_profile.get("top_funder")
            or ""
        ).strip() or None

        if not creator_wallet and not first_hop_funder:
            return False

        try:
            entity_key = engine.record_creator_launch(
                mint=mint,
                creator_wallet=creator_wallet,
                first_hop_funder=first_hop_funder,
            )
        except Exception:
            return False

        if entity_key:
            payload["creator_entity_key"] = entity_key
            return True
        return False

    def _maybe_record_wallet_memory_from_payload(
        self,
        *,
        mint: str,
        state: dict[str, Any],
        payload: dict[str, Any],
        control: dict[str, Any],
    ) -> bool:
        if bool(control.get("wallet_memory_recorded", False)):
            return False

        status = str(payload.get("helius_enrichment_status") or "")
        allowed_statuses = {
            "light_enriched",
            "deep_enriched",
            "light_partial",
            "deep_partial",
            "creator_only",
            "cache_hit",
            "cache_throttled",
            "timeout_backoff",
        }
        if status not in allowed_statuses:
            return False

        participant_wallets = [
            str(wallet).strip()
            for wallet in list(payload.get("helius_selected_wallets") or [])
            if str(wallet).strip()
        ]
        if not participant_wallets:
            return False

        engine = _get_wallet_intelligence_engine()
        if engine is None:
            return False

        creator_wallet = str(state.get("creator_wallet") or "").strip() or None
        creator_profile = payload.get("helius_creator_profile", {}) or {}
        funder_wallet = str(creator_profile.get("top_funder") or "").strip() or None

        try:
            engine.record_token_cohort(
                mint=mint,
                participant_wallets=participant_wallets,
                creator_wallet=creator_wallet,
                funder_wallet=funder_wallet,
            )
            return True
        except Exception:
            return False

    @staticmethod
    def _is_timeout_like(value: Any) -> bool:
        value_text = str(value or "").lower()
        return "timed out" in value_text or value_text == "budget_exhausted"

    @staticmethod
    def _profile_depth_bucket(count: int) -> str:
        if count <= 0:
            return "none"
        if count == 1:
            return "1"
        if 2 <= count <= 4:
            return "2_4"
        return "5_plus"

    def _prepare_cached_payload(
        self,
        cached: dict[str, Any],
        cache_status: str,
    ) -> dict[str, Any]:
        prepared = dict(cached)
        previous_error = prepared.get("helius_last_error")
        prepared["helius_cached_error"] = previous_error
        prepared["helius_last_error"] = None
        prepared["helius_enrichment_status"] = cache_status
        prepared.setdefault(
            "helius_profile_depth_bucket",
            self._profile_depth_bucket(
                int(prepared.get("helius_completed_wallet_count", 0) or 0)
            ),
        )
        prepared.setdefault(
            "helius_partial_enrichment",
            bool(prepared.get("helius_completed_wallet_count", 0))
            and str(prepared.get("helius_enrichment_tier", "none")) in {"light", "deep"}
            and str(cached.get("helius_enrichment_status", "")).endswith("_partial"),
        )
        prepared.setdefault(
            "helius_profile_completion_ratio",
            round(
                _safe_div(
                    float(prepared.get("helius_completed_wallet_count", 0) or 0.0),
                    float(prepared.get("helius_requested_wallet_count", 0) or 0.0),
                    0.0,
                ),
                4,
            ),
        )
        prepared.setdefault("helius_wallet_attempt_details", [])
        return prepared

    def _is_partial_enrichment(
        self,
        *,
        tier: str,
        requested_wallet_count: int,
        completed_wallet_count: int,
        creator_expected: bool,
        creator_attempted: bool,
        creator_profile: WalletProfile | None,
        budget_exhausted: bool,
        last_error: Any,
    ) -> bool:
        if tier not in {"light", "deep"}:
            return False
        if budget_exhausted:
            return True
        if self._is_timeout_like(last_error):
            return True
        if completed_wallet_count < requested_wallet_count:
            return True
        if creator_expected and creator_attempted and creator_profile is None:
            return True
        return False

    def _derive_status(
        self,
        *,
        tier: str,
        requested_wallet_count: int,
        completed_wallet_count: int,
        creator_profile: WalletProfile | None,
        partial_enrichment: bool,
        budget_exhausted: bool,
        last_error: Any,
    ) -> str:
        if completed_wallet_count > 0:
            if partial_enrichment:
                return f"{tier}_partial"
            return f"{tier}_enriched"

        if creator_profile is not None:
            return "creator_only"

        if budget_exhausted:
            return "budget_exhausted"

        if last_error:
            return "failed"

        if requested_wallet_count > 0:
            return "attempted_no_profiles"

        return "unqualified"
