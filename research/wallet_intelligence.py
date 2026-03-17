from __future__ import annotations

from datetime import datetime, timezone

from research.wallet_registry import WalletRegistry


class WalletIntelligenceEngine:
    """
    Connects wallet enrichment and paper-trade outcomes to the long-term wallet registry.
    """

    def __init__(self) -> None:
        self.registry = WalletRegistry()

    def record_token_cohort(
        self,
        *,
        mint: str,
        participant_wallets: list[str],
        creator_wallet: str | None = None,
        funder_wallet: str | None = None,
    ) -> bool:
        touched = False
        for position, wallet in enumerate(participant_wallets, start=1):
            changed = self.registry.record_token_participation(
                wallet,
                mint=mint,
                entry_position=position,
                creator_wallet=creator_wallet,
                funder_wallet=funder_wallet,
            )
            touched = touched or changed

        if touched:
            self.registry.save()
        return touched

    def record_token_outcome(
        self,
        *,
        participant_wallets: list[str],
        final_score: float,
        mint: str | None = None,
    ) -> bool:
        changed = self.registry.record_token_outcome(
            participant_wallets,
            final_score=final_score,
            mint=mint,
        )
        if changed:
            self.registry.save()
        return changed

    def record_closed_paper_trade(
        self,
        *,
        mint: str,
        participant_wallets: list[str],
        pnl_pct: float,
        max_pnl_pct: float | None = None,
        min_pnl_pct: float | None = None,
        exit_reason: str | None = None,
        resolved_at: str | None = None,
    ) -> bool:
        resolved_ts: float | None = None
        if resolved_at:
            try:
                dt = datetime.fromisoformat(str(resolved_at).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                resolved_ts = dt.timestamp()
            except Exception:
                resolved_ts = None

        changed = self.registry.record_closed_trade_outcome(
            participant_wallets,
            mint=mint,
            pnl_pct=pnl_pct,
            max_pnl_pct=max_pnl_pct,
            min_pnl_pct=min_pnl_pct,
            exit_reason=exit_reason,
            resolved_at=resolved_ts,
        )
        if changed:
            self.registry.save()
        return changed

    def compute_cohort_quality(self, wallets: list[str]) -> float:
        return self.registry.cohort_quality(wallets)

    def wallet_quality(self, wallet: str) -> float:
        return self.registry.wallet_quality(wallet)

    def top_wallets(self, limit: int = 20):
        return self.registry.top_wallets(limit)

    def suspicious_wallets(self, limit: int = 20):
        return self.registry.suspicious_wallets(limit)