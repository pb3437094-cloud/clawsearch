from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REGISTRY_DIR = Path("data/research")
REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

WALLET_REGISTRY_PATH = REGISTRY_DIR / "wallet_registry.json"

MIN_WIN_SCORE = 105.0
MIN_LOSS_SCORE = 70.0

PAPER_WIN_PNL_PCT = 10.0
PAPER_BIG_WIN_PNL_PCT = 25.0
PAPER_WIN_MAX_PNL_PCT = 20.0
PAPER_BIG_WIN_MAX_PNL_PCT = 45.0
PAPER_LOSS_PNL_PCT = -12.0
PAPER_BIG_LOSS_PNL_PCT = -25.0
PAPER_LOSS_MAX_PNL_CAP = 12.0
PAPER_BIG_LOSS_MAX_PNL_CAP = 8.0


@dataclass
class WalletRecord:
    wallet: str
    tokens_seen: int = 0
    winning_tokens: int = 0
    losing_tokens: int = 0
    neutral_tokens: int = 0

    early_entries: int = 0
    late_entries: int = 0
    avg_entry_position: float = 0.0

    paper_trade_count: int = 0
    paper_trade_wins: int = 0
    paper_trade_losses: int = 0
    paper_trade_neutral: int = 0
    paper_trade_big_wins: int = 0
    paper_trade_big_losses: int = 0
    cumulative_realized_pnl_pct: float = 0.0
    cumulative_max_pnl_pct: float = 0.0
    cumulative_min_pnl_pct: float = 0.0
    avg_realized_pnl_pct: float = 0.0
    avg_max_pnl_pct: float = 0.0
    avg_min_pnl_pct: float = 0.0

    associated_creators: list[str] | None = None
    associated_funders: list[str] | None = None

    last_seen: float = 0.0
    last_outcome_at: float = 0.0

    bootstrap_quality_score: float = 0.5
    paper_trade_quality_score: float = 0.5
    confidence_score: float = 0.0
    quality_score: float = 0.5

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["associated_creators"] = data["associated_creators"] or []
        data["associated_funders"] = data["associated_funders"] or []
        for key in (
            "avg_entry_position",
            "cumulative_realized_pnl_pct",
            "cumulative_max_pnl_pct",
            "cumulative_min_pnl_pct",
            "avg_realized_pnl_pct",
            "avg_max_pnl_pct",
            "avg_min_pnl_pct",
            "bootstrap_quality_score",
            "paper_trade_quality_score",
            "confidence_score",
            "quality_score",
        ):
            data[key] = round(float(data.get(key, 0.0) or 0.0), 6)
        return data


class WalletRegistry:
    def __init__(self) -> None:
        self._wallets: dict[str, WalletRecord] = {}
        self._recorded_token_cohorts: dict[str, dict[str, Any]] = {}
        self._recorded_token_outcomes: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not WALLET_REGISTRY_PATH.exists():
            return

        try:
            raw = json.loads(WALLET_REGISTRY_PATH.read_text(encoding="utf-8"))
        except Exception:
            return

        wallets_payload: dict[str, Any]
        if isinstance(raw, dict) and "wallets" in raw:
            wallets_payload = dict(raw.get("wallets") or {})
            self._recorded_token_cohorts = {
                str(mint): dict(payload or {})
                for mint, payload in dict(raw.get("recorded_token_cohorts") or {}).items()
            }
            self._recorded_token_outcomes = {
                str(mint): dict(payload or {})
                for mint, payload in dict(raw.get("recorded_token_outcomes") or {}).items()
            }
        elif isinstance(raw, dict):
            wallets_payload = raw
        else:
            wallets_payload = {}

        for wallet, payload in wallets_payload.items():
            if not isinstance(payload, dict):
                continue
            merged = {"wallet": wallet, **payload}
            merged["associated_creators"] = list(merged.get("associated_creators") or [])
            merged["associated_funders"] = list(merged.get("associated_funders") or [])
            try:
                self._wallets[str(wallet)] = WalletRecord(**merged)
            except TypeError:
                allowed = {field_name for field_name in WalletRecord.__dataclass_fields__.keys()}
                filtered = {key: value for key, value in merged.items() if key in allowed}
                filtered.setdefault("wallet", wallet)
                self._wallets[str(wallet)] = WalletRecord(**filtered)

    def save(self) -> None:
        payload = {
            "wallets": {
                wallet: record.to_dict()
                for wallet, record in self._wallets.items()
            },
            "recorded_token_cohorts": self._recorded_token_cohorts,
            "recorded_token_outcomes": self._recorded_token_outcomes,
            "saved_at": time.time(),
        }
        WALLET_REGISTRY_PATH.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def get_wallet(self, wallet: str) -> WalletRecord:
        wallet = str(wallet or "").strip()
        if not wallet:
            raise ValueError("wallet is required")
        if wallet not in self._wallets:
            self._wallets[wallet] = WalletRecord(wallet=wallet)
        return self._wallets[wallet]

    @staticmethod
    def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
        denominator = float(denominator or 0.0)
        if abs(denominator) < 1e-9:
            return default
        return float(numerator or 0.0) / denominator

    @staticmethod
    def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
        return max(lower, min(upper, float(value)))

    @staticmethod
    def _normalize_centered(value: float, scale: float) -> float:
        if scale <= 0:
            return 0.0
        return max(-1.0, min(1.0, float(value or 0.0) / float(scale)))

    def record_token_participation(
        self,
        wallet: str,
        *,
        entry_position: int,
        mint: str | None = None,
        creator_wallet: str | None = None,
        funder_wallet: str | None = None,
    ) -> bool:
        wallet = str(wallet or "").strip()
        mint = str(mint or "").strip()
        if not wallet:
            return False

        cohort_payload = self._recorded_token_cohorts.setdefault(mint, {"wallets": []}) if mint else None
        if cohort_payload is not None:
            known_wallets = cohort_payload.setdefault("wallets", [])
            if wallet in known_wallets:
                return False
            known_wallets.append(wallet)
            cohort_payload["recorded_at"] = time.time()

        record = self.get_wallet(wallet)
        record.tokens_seen += 1
        record.last_seen = time.time()

        if int(entry_position or 0) <= 5:
            record.early_entries += 1
        else:
            record.late_entries += 1

        n = max(record.tokens_seen, 1)
        prev_avg = float(record.avg_entry_position or 0.0)
        record.avg_entry_position = ((prev_avg * (n - 1)) + float(entry_position or 0)) / n

        if creator_wallet:
            creator_wallet = str(creator_wallet).strip()
            if creator_wallet:
                if record.associated_creators is None:
                    record.associated_creators = []
                if creator_wallet not in record.associated_creators:
                    record.associated_creators.append(creator_wallet)

        if funder_wallet:
            funder_wallet = str(funder_wallet).strip()
            if funder_wallet:
                if record.associated_funders is None:
                    record.associated_funders = []
                if funder_wallet not in record.associated_funders:
                    record.associated_funders.append(funder_wallet)

        self._recalculate_scores([wallet])
        return True

    def record_token_outcome(
        self,
        wallets: list[str],
        *,
        final_score: float,
        mint: str | None = None,
    ) -> bool:
        mint = str(mint or "").strip()
        outcome_key = mint or f"bootstrap::{len(self._recorded_token_outcomes)}::{int(time.time())}"
        if mint and outcome_key in self._recorded_token_outcomes:
            return False

        touched_wallets: list[str] = []
        score = float(final_score or 0.0)
        for wallet in wallets:
            wallet = str(wallet or "").strip()
            if not wallet:
                continue
            record = self.get_wallet(wallet)
            if score >= MIN_WIN_SCORE:
                record.winning_tokens += 1
            elif score <= MIN_LOSS_SCORE:
                record.losing_tokens += 1
            else:
                record.neutral_tokens += 1
            record.last_outcome_at = time.time()
            touched_wallets.append(wallet)

        if not touched_wallets:
            return False

        self._recorded_token_outcomes[outcome_key] = {
            "type": "bootstrap_score",
            "final_score": round(score, 4),
            "wallet_count": len(touched_wallets),
            "recorded_at": time.time(),
        }
        self._recalculate_scores(touched_wallets)
        return True

    def record_closed_trade_outcome(
        self,
        wallets: list[str],
        *,
        mint: str,
        pnl_pct: float,
        max_pnl_pct: float | None = None,
        min_pnl_pct: float | None = None,
        exit_reason: str | None = None,
        resolved_at: float | None = None,
    ) -> bool:
        mint = str(mint or "").strip()
        if not mint:
            return False
        if mint in self._recorded_token_outcomes:
            return False

        resolved_at = float(resolved_at or time.time())
        pnl_pct = float(pnl_pct or 0.0)
        max_pnl_pct = float(max_pnl_pct if max_pnl_pct is not None else pnl_pct)
        min_pnl_pct = float(min_pnl_pct if min_pnl_pct is not None else pnl_pct)

        is_win = pnl_pct >= PAPER_WIN_PNL_PCT or max_pnl_pct >= PAPER_WIN_MAX_PNL_PCT
        is_big_win = pnl_pct >= PAPER_BIG_WIN_PNL_PCT or max_pnl_pct >= PAPER_BIG_WIN_MAX_PNL_PCT
        is_loss = pnl_pct <= PAPER_LOSS_PNL_PCT and max_pnl_pct < PAPER_LOSS_MAX_PNL_CAP
        is_big_loss = pnl_pct <= PAPER_BIG_LOSS_PNL_PCT and max_pnl_pct < PAPER_BIG_LOSS_MAX_PNL_CAP

        touched_wallets: list[str] = []
        for wallet in wallets:
            wallet = str(wallet or "").strip()
            if not wallet:
                continue
            record = self.get_wallet(wallet)
            record.paper_trade_count += 1
            record.cumulative_realized_pnl_pct += pnl_pct
            record.cumulative_max_pnl_pct += max_pnl_pct
            record.cumulative_min_pnl_pct += min_pnl_pct
            count = max(record.paper_trade_count, 1)
            record.avg_realized_pnl_pct = record.cumulative_realized_pnl_pct / count
            record.avg_max_pnl_pct = record.cumulative_max_pnl_pct / count
            record.avg_min_pnl_pct = record.cumulative_min_pnl_pct / count

            if is_big_win:
                record.paper_trade_big_wins += 1
            if is_big_loss:
                record.paper_trade_big_losses += 1

            if is_win:
                record.paper_trade_wins += 1
            elif is_loss:
                record.paper_trade_losses += 1
            else:
                record.paper_trade_neutral += 1

            record.last_outcome_at = resolved_at
            touched_wallets.append(wallet)

        if not touched_wallets:
            return False

        self._recorded_token_outcomes[mint] = {
            "type": "closed_paper_trade",
            "wallets": touched_wallets,
            "pnl_pct": round(pnl_pct, 4),
            "max_pnl_pct": round(max_pnl_pct, 4),
            "min_pnl_pct": round(min_pnl_pct, 4),
            "exit_reason": str(exit_reason or ""),
            "recorded_at": resolved_at,
        }
        self._recalculate_scores(touched_wallets)
        return True

    def _bootstrap_quality(self, record: WalletRecord) -> float:
        total = max(record.tokens_seen, 1)
        win_rate = self._safe_div(record.winning_tokens, total, 0.0)
        loss_rate = self._safe_div(record.losing_tokens, total, 0.0)
        neutral_rate = self._safe_div(record.neutral_tokens, total, 0.0)
        early_rate = self._safe_div(record.early_entries, total, 0.0)
        entry_advantage = self._clamp((6.0 - float(record.avg_entry_position or 6.0)) / 5.0)

        centered = (
            (win_rate - loss_rate) * 0.55
            + (early_rate - 0.40) * 0.20
            + (neutral_rate - 0.30) * 0.05
            + (entry_advantage - 0.30) * 0.20
        )
        return self._clamp(0.5 + centered * 0.45)

    def _paper_trade_quality(self, record: WalletRecord) -> float:
        if record.paper_trade_count <= 0:
            return 0.5

        trade_count = max(record.paper_trade_count, 1)
        win_balance = self._safe_div(
            record.paper_trade_wins - record.paper_trade_losses,
            trade_count,
            0.0,
        )
        big_balance = self._safe_div(
            record.paper_trade_big_wins - record.paper_trade_big_losses,
            trade_count,
            0.0,
        )
        normalized_realized = self._normalize_centered(record.avg_realized_pnl_pct, 22.0)
        normalized_max = self._normalize_centered(record.avg_max_pnl_pct, 38.0)
        normalized_drawdown = self._normalize_centered(record.avg_min_pnl_pct, 28.0)

        centered = (
            normalized_realized * 0.45
            + normalized_max * 0.25
            + win_balance * 0.15
            + big_balance * 0.10
            + normalized_drawdown * 0.05
        )
        return self._clamp(0.5 + centered * 0.40)

    def _confidence_score(self, record: WalletRecord) -> float:
        bootstrap_confidence = self._clamp(self._safe_div(record.tokens_seen, 12.0, 0.0))
        paper_confidence = self._clamp(self._safe_div(record.paper_trade_count, 6.0, 0.0))
        return self._clamp(bootstrap_confidence * 0.30 + paper_confidence * 0.70)

    def _recalculate_scores(self, wallets: list[str]) -> None:
        for wallet in wallets:
            wallet = str(wallet or "").strip()
            if not wallet:
                continue
            record = self.get_wallet(wallet)
            record.bootstrap_quality_score = self._bootstrap_quality(record)
            record.paper_trade_quality_score = self._paper_trade_quality(record)
            record.confidence_score = self._confidence_score(record)

            if record.paper_trade_count > 0:
                mixed_quality = (
                    record.paper_trade_quality_score * 0.72
                    + record.bootstrap_quality_score * 0.28
                )
            else:
                mixed_quality = record.bootstrap_quality_score

            record.quality_score = round(
                0.5 + ((mixed_quality - 0.5) * record.confidence_score),
                4,
            )

    def wallet_quality(self, wallet: str) -> float:
        wallet = str(wallet or "").strip()
        if not wallet or wallet not in self._wallets:
            return 0.5
        return float(self._wallets[wallet].quality_score)

    def cohort_quality(self, wallets: list[str]) -> float:
        cleaned = [str(wallet or "").strip() for wallet in wallets if str(wallet or "").strip()]
        if not cleaned:
            return 0.5
        scores = [self.wallet_quality(wallet) for wallet in cleaned]
        return sum(scores) / len(scores)

    def top_wallets(self, limit: int = 20) -> list[WalletRecord]:
        return sorted(
            self._wallets.values(),
            key=lambda wallet: (wallet.quality_score, wallet.paper_trade_count, wallet.tokens_seen),
            reverse=True,
        )[:limit]

    def suspicious_wallets(self, limit: int = 20) -> list[WalletRecord]:
        return sorted(
            self._wallets.values(),
            key=lambda wallet: (wallet.quality_score, -(wallet.paper_trade_count + wallet.tokens_seen)),
        )[:limit]