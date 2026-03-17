from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from research.creator_entity_registry import CreatorEntityRegistry


class CreatorEntityIntelligenceEngine:
    """
    Connects creator/funder launch memory and closed paper-trade outcomes to the
    long-term creator entity registry.
    """

    def __init__(self) -> None:
        self.registry = CreatorEntityRegistry()

    def record_creator_launch(
        self,
        *,
        mint: str,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
        exchange_touch_label: str | None = None,
        funding_amount_sol: float | None = None,
        seconds_from_funding_to_launch: float | None = None,
    ) -> str | None:
        entity_key = self.registry.record_creator_launch(
            mint=mint,
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
            exchange_touch_label=exchange_touch_label,
            funding_amount_sol=funding_amount_sol,
            seconds_from_funding_to_launch=seconds_from_funding_to_launch,
        )
        if entity_key:
            self.registry.save()
        return entity_key

    def record_closed_paper_trade(
        self,
        *,
        mint: str,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
        pnl_pct: float,
        max_pnl_pct: float | None = None,
        min_pnl_pct: float | None = None,
        exit_reason: str | None = None,
        resolved_at: str | None = None,
    ) -> str | None:
        resolved_ts: float | None = None
        if resolved_at:
            try:
                dt = datetime.fromisoformat(str(resolved_at).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                resolved_ts = dt.timestamp()
            except Exception:
                resolved_ts = None

        entity_key = self.registry.record_closed_trade_outcome(
            mint=mint,
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
            pnl_pct=pnl_pct,
            max_pnl_pct=max_pnl_pct,
            min_pnl_pct=min_pnl_pct,
            exit_reason=exit_reason,
            resolved_at=resolved_ts,
        )
        if entity_key:
            self.registry.save()
        return entity_key

    def entity_features(
        self,
        *,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
    ) -> dict[str, Any]:
        return self.registry.entity_features(
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
        )

    def resolve_entity_quality(
        self,
        *,
        creator_wallet: str | None = None,
        first_hop_funder: str | None = None,
    ) -> float:
        return self.registry.resolve_entity_quality(
            creator_wallet=creator_wallet,
            first_hop_funder=first_hop_funder,
        )

    def top_entities(self, limit: int = 20):
        return self.registry.top_entities(limit)

    def suspicious_entities(self, limit: int = 20):
        return self.registry.suspicious_entities(limit)
